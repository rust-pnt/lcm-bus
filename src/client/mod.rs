//! A connection to a bus, on blocking sockets and a reader thread.
//!
//! One module for each of the three, because each brings its own sockets,
//! its own reader, and its own ways of going wrong.

mod logfile;
mod multicast;
mod relay;
// A relay matches the patterns its clients send, and those are Java regular
// expressions.
// Without an engine for one, a relay takes a pattern for the name of a channel.
// A client that asked for `/pntos/.*` then gets silence.
// That is a bus that carries the wrong thing, so this says what it needs when it
// is built.
#[cfg(feature = "patterns")]
mod serve;

use alloc::vec::Vec;
use core::time::Duration;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::panic;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LockResult, Mutex, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use socket2::Socket;

use crate::bus::{BadPattern, Filter, Subscriptions, escaped};
use crate::url::{BadUrl, BusUrl, LogMode};
use crate::wire::{Frame, FrameRef, WireError, log, tcpq, udpm};

/// `SO_RCVBUF` when the URL gives no other number.
/// It holds some of the largest fragmented messages in LCM.
/// Linux limits it to `net.core.rmem_max`.
const RECV_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// A relay that accepts the connection and then says nothing must not hold
/// the caller, who has no [`Client`] and so cannot stop it.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a udpm reader waits before it looks at `running` again, and so
/// what `close` and `Drop` cost.
const READ_TIMEOUT: Duration = Duration::from_millis(20);
/// How long `close` reads what a relay is still sending, so that the socket
/// is closed with nothing unread, and what the kernel holds can go out.
const LINGER: Duration = Duration::from_millis(250);
/// A read that gives nothing back in this long is a receive queue with
/// nothing in it, which is what `close` reads until.
const EMPTY_ENOUGH: Duration = Duration::from_millis(20);

fn ignore_poison<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(PoisonError::into_inner)
}

/// One message, and what the bus knew about it.
///
/// The frame is owned. A borrow keeps a copy from a handler that reads only
/// a header, but a handler that holds a message has to copy it anyway, and
/// the filter runs before the copy, so a message this copies is one the
/// caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Delivery {
    pub frame: Frame,
    /// The time in the log on a replay, and the time it arrived otherwise,
    /// in microseconds since the Unix epoch. [`Client::publish_at`] writes
    /// this one back, so a log can go through a reader and a writer and keep
    /// its times.
    pub timestamp: i64,
    /// Where it came from, and what that bus knows about it.
    pub origin: Origin,
}

/// The bus a message came off, and what it carries that the others do not.
///
/// Three `Option` fields let a reader forget one and put `None` where a
/// value belongs. Here a variant cannot be built without what it names.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Origin {
    /// The peer that sent it, and the number that peer gave it.
    Multicast { peer: SocketAddr, sequence: u32 },
    /// A relay puts no sender and no number on the wire.
    Relay,
    /// A log numbers its events from zero.
    Log { number: i64 },
}

/// Why a client stopped.
#[derive(Debug)]
#[non_exhaustive]
pub enum Stop {
    /// A replay reached the end of its log. This is not a fault.
    EndOfLog,
    /// A replay reached the end of its log in the middle of an event, which
    /// is what a recorder that was killed leaves. The events before the cut
    /// are the ones it gave out, and the part event is dropped: what the
    /// head of it names is not all there, and the bytes that are there are
    /// the payload a publisher sent.
    TornLog,
    /// A handler panicked, and took the reader with it.
    Panicked,
    /// The relay closed the connection.
    Closed,
    /// A socket or file error on the reader.
    Io(io::Error),
    /// Bytes on a stream that no frame can follow.
    Wire(WireError),
}

impl core::fmt::Display for Stop {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::EndOfLog => f.write_str("the log ended"),
            Self::TornLog => f.write_str("the log ended in the middle of an event"),
            Self::Panicked => f.write_str("the handler panicked"),
            Self::Closed => f.write_str("the relay closed the connection"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Wire(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Stop {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Wire(e) => Some(e),
            Self::EndOfLog | Self::TornLog | Self::Panicked | Self::Closed => None,
        }
    }
}

/// What a client needs to receive. A publisher has none of it.
struct Receiving {
    subscriptions: Arc<RwLock<Subscriptions>>,
    handler: Arc<dyn DeliveryHandler>,
}

impl Receiving {
    /// A publisher keeps an empty set, so `stats` and `subscribe` still work
    /// and nothing reads it.
    fn subscriptions(receiving: &Option<Self>) -> Arc<RwLock<Subscriptions>> {
        match receiving {
            Some(receiving) => receiving.subscriptions.clone(),
            None => Arc::new(RwLock::new(Subscriptions::new())),
        }
    }
}

/// The reader thread does the work of `on_delivery`, so a slow handler holds
/// up the socket and the kernel drops what arrives meanwhile. Give slow work
/// a thread of its own: [`Client::connect_channel`] is that thread.
///
/// A handler that holds its client back holds a ring that no end drops, so
/// hold a [`std::sync::Weak`] to it.
pub trait DeliveryHandler: Send + Sync {
    /// One message a subscription matched.
    fn on_delivery(&self, delivery: Delivery);

    /// The reader stopped, and [`Client::is_connected`] is now incorrect.
    /// LCM opens a relay connection again by itself.
    /// This client does not, so the caller decides the retry policy, and
    /// [`Stop`] is what that decision needs.
    fn on_stop(&self, cause: Stop) {
        let _ = cause;
    }
}

/// The messages a [`Client`] took off a bus, and the count of the ones it
/// did not give on.
///
/// A bus does not wait for a reader. A handler that does slow work holds the
/// socket while the kernel drops what arrives, so the usual first handler
/// anyone writes hands the message to a channel — and the `unwrap` on that
/// send is on the reader thread, where a panic stops the client. This is
/// that handler, and it counts the drop where the panic was.
#[derive(Debug)]
pub struct Deliveries {
    from: std::sync::mpsc::Receiver<Delivery>,
    dropped: Arc<AtomicU64>,
}

impl Deliveries {
    /// The messages the channel was too full to take. A slow reader costs
    /// these, and a bus that says nothing about them is a bus that lies.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The next message, waiting for one. This gives an error when the
    /// client has stopped and the ones it took are gone.
    pub fn recv(&self) -> Result<Delivery, std::sync::mpsc::RecvError> {
        self.from.recv()
    }

    /// The next message, waiting up to `timeout`.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Delivery, std::sync::mpsc::RecvTimeoutError> {
        self.from.recv_timeout(timeout)
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&self) -> Result<Delivery, std::sync::mpsc::TryRecvError> {
        self.from.try_recv()
    }
}

impl Iterator for Deliveries {
    type Item = Delivery;

    fn next(&mut self) -> Option<Delivery> {
        self.from.recv().ok()
    }
}

/// A handler that only hands the message on.
#[derive(Debug)]
struct ToChannel {
    to: std::sync::mpsc::SyncSender<Delivery>,
    dropped: Arc<AtomicU64>,
}

impl DeliveryHandler for ToChannel {
    fn on_delivery(&self, delivery: Delivery) {
        // A full channel, or a reader that has gone. The reader thread
        // stops for no such thing: it has a socket to get back to.
        if self.to.try_send(delivery).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// So a caller can give the handler it shares elsewhere.
impl<H: DeliveryHandler + ?Sized> DeliveryHandler for Arc<H> {
    fn on_delivery(&self, delivery: Delivery) {
        (**self).on_delivery(delivery)
    }

    fn on_stop(&self, cause: Stop) {
        (**self).on_stop(cause)
    }
}

impl<F: Fn(Delivery) + Send + Sync> DeliveryHandler for F {
    fn on_delivery(&self, delivery: Delivery) {
        self(delivery)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
/// A snapshot of a client's counters. See [`Client::stats`].
#[non_exhaustive]
pub struct Stats {
    /// Datagrams on udpm, frames from a relay, events from a log.
    pub received: u64,
    /// Messages a subscription matched and the handler was given.
    pub delivered: u64,
    /// Datagrams this client cannot read, and fragments that did not agree.
    /// A bad sender is counted here and not reported.
    pub discarded: u64,
    /// Messages with fragments this client waits for.
    pub in_flight: u64,
    /// Messages this client dropped before they were whole, to keep to the
    /// budget its reassembler keeps. A bus that loses them says nothing
    /// else about it.
    pub evicted: u64,
    /// `fancy_regex` stops at a backtrack limit, and the pattern that reaches
    /// it matches nothing.
    pub pattern_failures: u64,
    /// Messages a relay client took and never put on the wire: the ones
    /// still waiting when the connection ended, and the one a write was in
    /// the middle of. `publish` gives these back `Ok`, because it hands a
    /// message to the writer thread and does not wait for the relay.
    /// [`Client::flush`] waits for them.
    pub unsent: u64,
}

#[derive(Default, Debug)]
struct Counters {
    received: AtomicU64,
    delivered: AtomicU64,
    discarded: AtomicU64,
    in_flight: AtomicU64,
    evicted: AtomicU64,
}

impl Counters {
    fn received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    fn delivered(&self) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
    }

    fn discarded(&self) {
        self.discarded.fetch_add(1, Ordering::Relaxed);
    }
}

/// Why opening a bus, or publishing or subscribing on one, did not work.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// The URL did not parse.
    Url(BadUrl),
    /// A socket or a file did not open, connect, or write.
    Io(io::Error),
    /// The far end is not an LCM relay, or not a version this crate speaks.
    Handshake(WireError),
    /// A subscription pattern the regex engine refused.
    Pattern(BadPattern),
    /// A read or a write of a frame failed.
    Wire(WireError),
    /// A log open to read cannot publish.
    ReadOnly,
    /// A publisher takes no messages, so it takes no subscriptions.
    PublishOnly,
    /// A log to replay is a file. A read of a FIFO, a device or a socket has
    /// no end, and nothing takes it back: `close` waits for the reader
    /// thread, and the reader thread waits in that read.
    NotAFile,
    /// The connection is closed.
    Closed,
    /// Only `tcpq://` has a relay to serve.
    /// A group and a log have no client that dials them.
    NotARelay,
}

impl core::fmt::Display for ClientError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Url(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Handshake(e) => write!(f, "handshake failed: {e}"),
            Self::Pattern(e) => write!(f, "{e}"),
            Self::Wire(e) => write!(f, "{e}"),
            Self::ReadOnly => f.write_str("a log open to read cannot publish"),
            Self::PublishOnly => f.write_str("a publisher takes no subscriptions"),
            Self::NotAFile => f.write_str("a log to replay is a file"),
            Self::Closed => f.write_str("the connection is closed"),
            Self::NotARelay => f.write_str("only a tcpq:// bus has a relay to serve"),
        }
    }
}

impl std::error::Error for ClientError {
    /// `Display` also gives the cause. This is for a caller that wants the
    /// [`io::Error`] itself.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Url(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Handshake(e) | Self::Wire(e) => Some(e),
            Self::Pattern(e) => Some(e),
            Self::ReadOnly
            | Self::PublishOnly
            | Self::NotAFile
            | Self::Closed
            | Self::NotARelay => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<BadUrl> for ClientError {
    fn from(e: BadUrl) -> Self {
        Self::Url(e)
    }
}

impl From<WireError> for ClientError {
    fn from(e: WireError) -> Self {
        Self::Wire(e)
    }
}

impl From<BadPattern> for ClientError {
    fn from(e: BadPattern) -> Self {
        Self::Pattern(e)
    }
}

/// The reader thread asks this on each message, and `subscribe` can add a
/// pattern while it does.
impl Filter for Arc<RwLock<Subscriptions>> {
    fn matches(&self, channel: &str) -> bool {
        ignore_poison(self.read()).matches(channel)
    }
}

#[cfg_attr(feature = "patterns", doc = "```no_run")]
#[cfg_attr(not(feature = "patterns"), doc = "```ignore")]
/// # use lcm_bus::{Client, Delivery, Subscriptions};
/// let mut subscriptions = Subscriptions::new();
/// subscriptions.add("/example/.*")?;
///
/// let print = |d: Delivery| println!("{} {} bytes", d.frame.channel, d.frame.payload.len());
/// let client = Client::connect("udpm://239.255.76.67:7667", subscriptions, print)?;
/// client.publish("/example/one", &[1, 2, 3])?;
/// # Ok::<(), lcm_bus::ClientError>(())
/// ```
#[derive(Debug)]
pub struct Client {
    transport: Transport,
    /// A publisher has no reader thread and no use for a pattern.
    receives: bool,
    /// The transport can still take a message. `close` clears this, and so
    /// does a reader that stops where the two share one socket.
    writable: Arc<AtomicBool>,
    subscriptions: Arc<RwLock<Subscriptions>>,
    counters: Arc<Counters>,
    running: Arc<AtomicBool>,
    reader: Mutex<Option<JoinHandle<()>>>,
    /// One change to the subscriptions at a time, so two threads cannot
    /// both find a rule and both tell the relay to drop it. Not the
    /// subscriptions lock itself, which a delivery reads.
    changing: Mutex<()>,
}

#[derive(Debug)]
enum Transport {
    Udpm {
        /// Unbound, as LCM leaves its send socket. This is a `socket2` socket
        /// because `std` has no scatter-gather send.
        send: Socket,
        /// What the kernel gave, which is not always what was asked for.
        /// A publisher listens on nothing and has none.
        recv_buffer: Option<usize>,
        destination: socket2::SockAddr,
        sequence: AtomicU32,
        short_max: usize,
    },
    Tcpq {
        stream: TcpStream,
        /// A publish puts its frame here, and the writer thread takes it. So
        /// the thread that publishes is not the thread that waits for the
        /// relay, and a handler that publishes from `on_delivery` does not
        /// give the reader thread away.
        outbox: Arc<relay::Outbox>,
        writer: Mutex<Option<JoinHandle<()>>>,
    },
    Log {
        writer: Mutex<LogWriter>,
    },
    /// A relay this process serves, rather than one it dialled.
    #[cfg(feature = "patterns")]
    Serve {
        served: Arc<serve::Served>,
        bound: std::net::SocketAddr,
        accept: Mutex<Option<JoinHandle<()>>>,
    },
    /// A replay publishes nothing, so it needs no transport.
    Replay,
}

/// One lock for the file and the counter, so that the events of a log keep
/// their sequence when two threads publish.
/// A player seeks by a binary division of the timestamps, which must increase.
#[derive(Debug)]
struct LogWriter {
    /// LCM writes a log through buffered stdio, and `close` flushes.
    file: std::io::BufWriter<std::fs::File>,
    /// A log numbers from zero, in append mode too.
    events: i64,
}

impl Client {
    /// The deepest channel [`Client::connect_channel`] will make.
    ///
    /// A channel takes the room for its whole depth as it is made, and the
    /// failure for asking too much is an abort and not a `Result`. A reader
    /// a million messages behind is gone, and [`Deliveries::dropped`] says
    /// so.
    pub const DEEPEST_CHANNEL: usize = 1 << 20;

    /// Open the bus a URL names, and give each matched message to `handler`.
    pub fn connect(
        url: &str,
        subscriptions: Subscriptions,
        handler: impl DeliveryHandler + 'static,
    ) -> Result<Self, ClientError> {
        Self::open(BusUrl::parse(url)?, subscriptions, handler)
    }

    /// As [`Client::connect`], from a [`BusUrl`] already parsed.
    pub fn open(
        url: BusUrl,
        subscriptions: Subscriptions,
        handler: impl DeliveryHandler + 'static,
    ) -> Result<Self, ClientError> {
        Self::start(
            url,
            Some(Receiving {
                subscriptions: Arc::new(RwLock::new(subscriptions)),
                handler: Arc::new(handler),
            }),
        )
    }

    /// Serve a `tcpq://` relay, rather than dial one.
    ///
    /// A relay is how LCM reaches a consumer that multicast does not: one behind NAT,
    /// on a routed subnet, or in a container whose network carries no multicast.
    /// An LCM client of any language dials this, so the reach is the choice of a
    /// deployment and not of a rewrite.
    ///
    /// The URL is the address to bind.
    /// `tcpq://:7700` listens on every address of this host, and `tcpq://10.0.0.5:7700`
    /// on that one alone.
    ///
    /// `subscriptions` is what `handler` receives.
    /// It is **not** what the relay carries: a relay carries what its clients ask for.
    /// Thus a process that publishes onto one and wants none of the traffic back serves
    /// it with an empty [`Subscriptions`].
    ///
    /// ```no_run
    /// # use lcm_bus::{Client, Delivery, Subscriptions};
    /// let relay = Client::serve(
    ///     "tcpq://:7700",
    ///     Subscriptions::new(),
    ///     |_: Delivery| {},
    /// )?;
    /// relay.publish("/pntos/gnss", &[1, 2, 3])?;
    /// # Ok::<(), lcm_bus::ClientError>(())
    /// ```
    #[cfg(feature = "patterns")]
    pub fn serve(
        url: &str,
        subscriptions: Subscriptions,
        handler: impl DeliveryHandler + 'static,
    ) -> Result<Self, ClientError> {
        let BusUrl::Tcpq(relay) = BusUrl::parse(url)? else {
            return Err(ClientError::NotARelay);
        };
        Self::serve_tcpq(
            &serve::bind_address(&relay),
            Some(Receiving {
                subscriptions: Arc::new(RwLock::new(subscriptions)),
                handler: Arc::new(handler),
            }),
        )
    }

    /// The clients connected to a relay this process serves, and zero for every other
    /// kind of bus.
    #[must_use]
    pub fn peers(&self) -> usize {
        match &self.transport {
            #[cfg(feature = "patterns")]
            Transport::Serve { served, .. } => served.peers(),
            _ => 0,
        }
    }

    /// The patterns a served relay matches for its clients, over all of them, and zero
    /// for every other kind of bus.
    ///
    /// The patterns of a client arrive after its connection does, so this rises after
    /// [`Client::peers`] does.
    /// A publisher that must not lose its first message needs a signal of readiness of
    /// its own, and the module note says why a count is not one.
    #[must_use]
    pub fn peer_patterns(&self) -> usize {
        match &self.transport {
            #[cfg(feature = "patterns")]
            Transport::Serve { served, .. } => served.peer_patterns(),
            _ => 0,
        }
    }

    /// Connections a served relay ended because the greeting was not this protocol.
    ///
    /// A port scan, a health check that opens a socket and closes it, and a client of
    /// a protocol this does not speak all count here.
    /// A number that climbs beside a `peers` of zero says something reaches the port
    /// and is not an LCM client.
    #[must_use]
    pub fn refused_peers(&self) -> u64 {
        match &self.transport {
            #[cfg(feature = "patterns")]
            Transport::Serve { served, .. } => served.refused(),
            _ => 0,
        }
    }

    /// The address a served relay listens on.
    ///
    /// A port of zero in the URL binds one the kernel chose, and this is how a caller
    /// learns which.
    #[must_use]
    pub fn bound(&self) -> Option<std::net::SocketAddr> {
        match &self.transport {
            #[cfg(feature = "patterns")]
            Transport::Serve { bound, .. } => Some(*bound),
            _ => None,
        }
    }

    /// A bus, and the messages off it, without writing a handler.
    ///
    #[cfg_attr(feature = "patterns", doc = "```no_run")]
    #[cfg_attr(not(feature = "patterns"), doc = "```ignore")]
    /// # use lcm_bus::{Client, Subscriptions};
    /// let mut subscriptions = Subscriptions::new();
    /// subscriptions.add("/example/.*")?;
    ///
    /// let (client, deliveries) = Client::connect_channel(
    ///     "udpm://239.255.76.67:7667", subscriptions, 1024)?;
    /// for delivery in deliveries {
    ///     println!("{}", delivery.frame.channel);
    /// }
    /// # Ok::<(), lcm_bus::ClientError>(())
    /// ```
    ///
    /// `depth` is the messages to hold for a reader that is behind. The bus
    /// goes on without it, so the ones above that are counted by
    /// [`Deliveries::dropped`] and not kept. A `depth` above
    /// [`Client::DEEPEST_CHANNEL`] is that many: the channel takes the room
    /// for it when it is made, and one large enough ends the program where
    /// the memory for it is not there.
    pub fn connect_channel(
        url: &str,
        subscriptions: Subscriptions,
        depth: usize,
    ) -> Result<(Self, Deliveries), ClientError> {
        // A channel of zero is a rendezvous, where `try_send` gives up unless
        // a reader waits on it at that moment. `depth` says how many to hold.
        let (to, from) = std::sync::mpsc::sync_channel(depth.clamp(1, Self::DEEPEST_CHANNEL));
        let dropped = Arc::new(AtomicU64::new(0));
        let client = Self::connect(
            url,
            subscriptions,
            ToChannel {
                to,
                dropped: dropped.clone(),
            },
        )?;
        Ok((client, Deliveries { from, dropped }))
    }

    /// A bus this client only publishes to.
    ///
    /// Subscriptions and a handler have no part in publishing, so this asks
    /// for one or the other, and it starts no reader thread. A client that
    /// subscribes to nothing otherwise takes each datagram off the group,
    /// decodes it, and drops it.
    ///
    /// A log open to read cannot publish, and gives [`ClientError::ReadOnly`].
    pub fn publisher(url: &str) -> Result<Self, ClientError> {
        Self::open_publisher(BusUrl::parse(url)?)
    }

    /// Open a bus that only publishes: no handler, and no subscriptions.
    pub fn open_publisher(url: BusUrl) -> Result<Self, ClientError> {
        if let BusUrl::File(file) = &url
            && file.mode == LogMode::Read
        {
            return Err(ClientError::ReadOnly);
        }
        Self::start(url, None)
    }

    fn start(url: BusUrl, receiving: Option<Receiving>) -> Result<Self, ClientError> {
        match url {
            BusUrl::Udpm(bus) => Self::open_udpm(bus, receiving),
            BusUrl::Tcpq(relay) => Self::open_tcpq(&alloc::format!("{relay}"), receiving),
            BusUrl::File(file) => Self::open_log(file, receiving),
        }
    }

    /// The bytes the kernel gave this bus for arriving datagrams.
    ///
    /// A kernel reads `SO_RCVBUF` as a wish. Linux holds it to
    /// `net.core.rmem_max` and then gives back two times what it kept, one
    /// part for the datagrams and one for its own record of them. Asking for
    /// 8 MiB against an `rmem_max` of 7 500 000 gives 15 000 000 here.
    ///
    /// The loss of one datagram is the loss of a full fragmented message, so
    /// a caller with messages going missing looks here first.
    pub fn recv_buffer_size(&self) -> Option<usize> {
        match &self.transport {
            Transport::Udpm { recv_buffer, .. } => *recv_buffer,
            _ => None,
        }
    }

    /// Puts what a log writer holds in its buffer on the disk, and waits for
    /// what was published to a relay to reach the wire.
    ///
    /// "On the wire" is what the kernel took, which is as far as a writer of
    /// a socket sees: a peer that goes takes what the kernel still held, and
    /// this says nothing of it.
    ///
    /// On `tcpq://` an `Err` of `TimedOut` says the frames are not there
    /// yet, and not that they never will be; [`Stats::unsent`] counts what
    /// was given up on. On a log, a publish that gave back `Ok` can fail
    /// here, and the writer stops as it stops for a publish that fails.
    pub fn flush(&self) -> Result<(), ClientError> {
        match &self.transport {
            Transport::Log { writer } => {
                use std::io::Write;
                if let Err(e) = ignore_poison(writer.lock()).file.flush() {
                    self.writable.store(false, Ordering::Relaxed);
                    return Err(ClientError::Io(e));
                }
            }
            Transport::Tcpq { outbox, .. } => {
                let deadline = Instant::now() + relay::LONGEST_WAIT;
                Self::refused(outbox.flushed(deadline))?;
            }
            // A relay hands each frame to the outbox of one peer, and one client that
            // stopped its reads must not hold a flush of the others.
            // What a peer did not take is the loss of that peer, and it is counted
            // there.
            #[cfg(feature = "patterns")]
            Transport::Serve { .. } => {}
            Transport::Udpm { .. } | Transport::Replay => {}
        }
        Ok(())
    }

    /// A replay of a log takes no messages. Every other bus does.
    #[must_use]
    pub fn can_publish(&self) -> bool {
        !matches!(self.transport, Transport::Replay)
    }

    /// On `udpm://` and on a log this waits for the write.
    ///
    /// On `tcpq://` it waits for the writer thread to take the message, and
    /// not for the relay to have it. So `Ok` says the message is on its way
    /// and in sequence, and not that it arrived:
    ///
    /// - [`Client::flush`] waits for what was published to reach the wire.
    /// - [`Stats::unsent`] counts what was taken and never went.
    /// - [`Client::is_connected`] is `false` once the sending half is gone.
    ///
    /// The wait here is for room, where the relay is slower than the
    /// messages in front of this one, and it is a minute whatever the length
    /// of the message. A subscription waits in a line of its own, so a
    /// handler that subscribes is not held by a message of megabytes.
    /// `close` from any thread ends the wait.
    pub fn publish(&self, channel: &str, payload: &[u8]) -> Result<(), ClientError> {
        self.write(channel, payload, None)
    }

    /// Publish with the time to record against it.
    ///
    /// Only a log holds a time; the others send the same bytes as
    /// [`Client::publish`]. Giving back a [`Delivery::timestamp`] writes a
    /// log that keeps the times of the one it came from.
    pub fn publish_at(
        &self,
        channel: &str,
        payload: &[u8],
        timestamp: i64,
    ) -> Result<(), ClientError> {
        self.write(channel, payload, Some(timestamp))
    }

    fn write(
        &self,
        channel: &str,
        payload: &[u8],
        timestamp: Option<i64>,
    ) -> Result<(), ClientError> {
        if !self.writable.load(Ordering::Relaxed) {
            return Err(ClientError::Closed);
        }
        // Each encoder holds a name to the limit of its own wire: 63 bytes
        // on a bus, and 999 in a log. A test here for the smaller of the two
        // would drop an event that a log legally holds and this crate's own
        // `log::encode` writes.
        let frame = FrameRef { channel, payload };

        match &self.transport {
            Transport::Udpm {
                send,
                destination,
                sequence,
                short_max,
                ..
            } => {
                // Two threads that publish together interleave their datagrams.
                // The sequence number keeps the two messages apart.
                let seq = sequence.fetch_add(1, Ordering::Relaxed);
                // The head and the payload go to the kernel as they are, so
                // a large message is not copied on the way out.
                for (head, body) in udpm::fragments(seq, frame, *short_max)? {
                    let parts = [io::IoSlice::new(head.as_ref()), io::IoSlice::new(body)];
                    send.send_to_vectored(&parts, destination)?;
                }
            }
            Transport::Tcpq { outbox, .. } => {
                self.to_relay(outbox, tcpq::publish(frame)?)?;
            }
            #[cfg(feature = "patterns")]
            Transport::Serve { served, .. } => {
                serve::publish_from_local(served, frame)?;
            }
            Transport::Log { writer } => {
                use std::io::Write;
                let mut writer = ignore_poison(writer.lock());
                // Tested before the lock as well, and a write that failed
                // while this one waited leaves part of an event in the log.
                if !self.writable.load(Ordering::Relaxed) {
                    return Err(ClientError::Closed);
                }
                let event = log::Event {
                    number: writer.events,
                    // A time of its own comes from in the lock, so two
                    // threads that publish together write times in the
                    // sequence they write the events.
                    timestamp: timestamp.unwrap_or_else(now_micros),
                    frame,
                };
                let bytes = log::encode(event)?;
                // A write that fails can leave a part of the event in the
                // log, and the event behind it would go into the middle of
                // that one. So a write that fails takes the writer with it,
                // as a write to a relay does.
                if let Err(e) = writer.file.write_all(&bytes) {
                    self.writable.store(false, Ordering::Relaxed);
                    return Err(ClientError::Io(e));
                }
                // The count follows a write that worked.
                writer.events += 1;
            }
            Transport::Replay => return Err(ClientError::ReadOnly),
        }
        Ok(())
    }

    /// One more channel, by the name it has.
    ///
    /// The rule is read, then the relay is told, then the set takes it, so a
    /// name this refuses never reaches the relay and an `Err` leaves the set
    /// as it was. On `tcpq://` "told" is the writer thread holding the
    /// frame, and not the relay having it.
    pub fn subscribe_name(&self, channel: &str) -> Result<(), ClientError> {
        self.must_receive()?;
        let checked = Subscriptions::check_name(channel)?;
        let _one_at_a_time = ignore_poison(self.changing.lock());
        self.tell_relay(&tcpq::subscribe(&escaped(channel)))?;
        ignore_poison(self.subscriptions.write()).push(checked);
        Ok(())
    }

    /// One more pattern.
    ///
    /// Read, told, taken, as in [`Client::subscribe_name`]. A pattern that
    /// does not compile never reaches the relay, which reads one with a
    /// different engine and has its own view of what compiles.
    #[cfg(feature = "patterns")]
    pub fn subscribe(&self, pattern: &str) -> Result<(), ClientError> {
        self.must_receive()?;
        let checked = Subscriptions::check(pattern)?;
        let _one_at_a_time = ignore_poison(self.changing.lock());
        self.tell_relay(&tcpq::subscribe(pattern))?;
        ignore_poison(self.subscriptions.write()).push(checked);
        Ok(())
    }

    /// This is `false` when the client did not have the subscription.
    ///
    /// The relay hears before the set changes, so an `Err` leaves the rule
    /// where it was.
    #[must_use = "this says whether the client had the subscription"]
    pub fn unsubscribe(&self, subscription: &str) -> Result<bool, ClientError> {
        self.must_receive()?;
        let _one_at_a_time = ignore_poison(self.changing.lock());
        let held = ignore_poison(self.subscriptions.read()).find(subscription);
        let Some(held) = held else { return Ok(false) };
        self.tell_relay(&tcpq::unsubscribe(&held))?;
        Ok(ignore_poison(self.subscriptions.write())
            .remove(subscription)
            .is_some())
    }

    /// Hands one message to the writer thread, waiting only for room.
    ///
    /// The messages of one thread keep their sequence, because a thread puts
    /// its second one in after its first gives back. Two threads publishing
    /// at once are owed no sequence between them, and a subscription keeps
    /// none with either.
    fn to_relay(&self, outbox: &relay::Outbox, frame: Vec<u8>) -> Result<(), ClientError> {
        // Tested in `write` as well. A publish that waited for room can come
        // through to a connection another one ended in the meantime.
        if !self.writable.load(Ordering::Relaxed) {
            return Err(ClientError::Closed);
        }
        Self::refused(outbox.put_message(frame, Instant::now() + relay::LONGEST_WAIT))
    }

    /// A subscription frame, which goes in a line of its own so that a
    /// handler that subscribes does not wait on a message of megabytes.
    fn tell_relay_frame(&self, outbox: &relay::Outbox, frame: Vec<u8>) -> Result<(), ClientError> {
        if !self.writable.load(Ordering::Relaxed) {
            return Err(ClientError::Closed);
        }
        let deadline = Instant::now() + relay::LONGEST_CONTROL_WAIT;
        Self::refused(outbox.put_control(frame, deadline))
    }

    fn refused(answer: Result<(), relay::Refused>) -> Result<(), ClientError> {
        match answer {
            Ok(()) => Ok(()),
            // The room, or the wire, never came: the relay is taking what is
            // in front of this slower than the deadline gives it.
            Err(relay::Refused::TooSlow) => {
                Err(ClientError::Io(io::Error::from(io::ErrorKind::TimedOut)))
            }
            Err(relay::Refused::Gone) => Err(ClientError::Closed),
        }
    }

    /// A publisher takes no messages, so a pattern given to one would sit in
    /// a set nothing reads. On `tcpq://` it is worse than useless: the relay
    /// would send what the pattern names, no reader would take it, and
    /// the socket would fill and hold the relay.
    fn must_receive(&self) -> Result<(), ClientError> {
        match self.receives {
            true => Ok(()),
            false => Err(ClientError::PublishOnly),
        }
    }

    fn tell_relay(&self, bytes: &[u8]) -> Result<(), ClientError> {
        if !self.writable.load(Ordering::Relaxed) {
            return Err(ClientError::Closed);
        }
        if let Transport::Tcpq { outbox, .. } = &self.transport {
            self.tell_relay_frame(outbox, bytes.to_vec())?;
        }
        Ok(())
    }

    /// This waits for the reader thread, unless it runs on that thread: a
    /// handler that closes its own client cannot wait for itself.
    ///
    /// An `Err` is the last flush of a log writer, which is where the bytes
    /// of the events it took can go missing. `Drop` calls this and has
    /// nowhere to put the answer.
    ///
    /// On `tcpq://` this reads what the relay sent and drops it, for up to a
    /// quarter of a second, because a socket closed with bytes unread is
    /// closed with a reset and a reset throws away what the kernel was still
    /// sending. A relay still sending as this runs can land bytes between
    /// that read and the close, and the reset then takes what the kernel had
    /// not sent — messages past [`Stats::unsent`], which no writer of a
    /// socket can name. A caller that must know its last messages arrived
    /// asks the far end.
    pub fn close(&self) -> io::Result<()> {
        use std::io::Read;

        self.running.store(false, Ordering::Relaxed);
        self.writable.store(false, Ordering::Relaxed);
        let mut flushed = Ok(());

        match &self.transport {
            // Linux takes `SO_RCVTIMEO` when the read starts, so a shorter
            // one here cannot move a read that waits now.
            Transport::Udpm { .. } => {}
            Transport::Tcpq {
                stream,
                outbox,
                writer,
            } => {
                // The frames still waiting never go, and the one the writer
                // has in hand with them. `flush` is how a caller waits for
                // them; `Stats::unsent` is how it learns that it did not.
                outbox.shut();
                // The sending half only, so the bytes the kernel already
                // holds still go and the relay is told there are no more.
                // This also ends the write the writer is in, so the wait
                // below is for one write and not for a relay.
                let _ = stream.shutdown(std::net::Shutdown::Write);
                let writer = ignore_poison(writer.lock()).take();
                if let Some(writer) = writer {
                    let _ = writer.join();
                }
                // A socket closed with bytes unread is closed with a reset,
                // and a reset throws away what the kernel was still sending.
                // A read that gives nothing back is an empty queue, which is
                // the answer this wants; `LINGER` only bounds looking.
                //
                // The reading half is not shut, because shutting it makes
                // the kernel answer what arrives after with that same reset,
                // and for as long as the socket is open — which is not until
                // here but until the last of the client and its threads go.
                let leaving = Instant::now() + LINGER;
                let _ = stream.set_read_timeout(Some(EMPTY_ENOUGH));
                let mut leftover = [0u8; 16 * 1024];
                while Instant::now() < leaving {
                    match (&*stream).read(&mut leftover) {
                        // The end of the stream, or nothing more waiting.
                        Ok(0) => break,
                        Ok(_) => {}
                        // A signal is not an empty queue.
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) if is_timeout(&e) => break,
                        Err(_) => break,
                    }
                }
                // What the kernel still holds is left to it: a close with
                // nothing unread goes on delivering. A linger would wait for
                // that and then send the very reset this is here to stop.
            }
            Transport::Log { writer } => {
                use std::io::Write;
                flushed = ignore_poison(writer.lock()).file.flush();
            }
            // The relay shuts each connection it holds, and waits for the threads that
            // read and write them.
            // A client of it reads the end of its stream and decides for itself whether
            // to dial again.
            #[cfg(feature = "patterns")]
            Transport::Serve { served, accept, .. } => {
                served.shut();
                let accept = ignore_poison(accept.lock()).take();
                if let Some(accept) = accept {
                    let _ = accept.join();
                }
            }
            // The log reader looks at `running` between events.
            Transport::Replay => {}
        }

        // A thread that waits for itself panics, so a handler closing its
        // own client leaves the handle where it is — taking it would also
        // take it from the `close` behind it, which can wait.
        let reader = {
            let mut held = ignore_poison(self.reader.lock());
            let ours = held
                .as_ref()
                .is_some_and(|reader| reader.thread().id() == std::thread::current().id());
            match ours {
                true => None,
                false => held.take(),
            }
            // And out of the lock before the wait, which the handler wants.
        };
        if let Some(reader) = reader {
            let _ = reader.join();
        }
        flushed
    }

    /// A client is connected while its reader runs and its sending half is
    /// sound. A write to a relay that fails takes that half for good, and a
    /// caller that watches this to know when to make a new connection has to
    /// see that.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.running.load(Ordering::Relaxed) && self.writable.load(Ordering::Relaxed)
    }

    /// A snapshot of what this client has taken off the bus and given on.
    pub fn stats(&self) -> Stats {
        let counters = &self.counters;
        // Out of the subscriptions lock before the outbox one. Every other
        // path takes them the other way about, and a temporary in the fields
        // below would hold this one until the whole of `Stats` is built.
        let pattern_failures = ignore_poison(self.subscriptions.read()).failures();
        Stats {
            received: counters.received.load(Ordering::Relaxed),
            delivered: counters.delivered.load(Ordering::Relaxed),
            discarded: counters.discarded.load(Ordering::Relaxed),
            in_flight: counters.in_flight.load(Ordering::Relaxed),
            evicted: counters.evicted.load(Ordering::Relaxed),
            pattern_failures,
            // The outbox keeps this, because it is the one that knows which
            // frames it let in and which of those went.
            unsent: match &self.transport {
                Transport::Tcpq { outbox, .. } => outbox.lost_messages(),
                _ => 0,
            },
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // A `Drop` has nowhere to put a flush that did not work, so a caller
        // that wants to know calls `close` itself.
        let _ = self.close();
    }
}

/// A reader reports its own end, on the way out and on the way down.
///
/// A handler that panics unwinds past every line after it, so the report has
/// to be a `Drop`, or the thread goes and `is_connected` says yes for good.
struct ReaderExit<'a> {
    running: &'a AtomicBool,
    /// The sending half, when a reader that stops takes it too. On `udpm://`
    /// the two sockets are separate, so a reader that stops leaves a
    /// publisher with everything it had.
    writable: Option<&'a AtomicBool>,
    handler: &'a dyn DeliveryHandler,
    cause: Option<Stop>,
}

impl Drop for ReaderExit<'_> {
    fn drop(&mut self) {
        if let Some(writable) = self.writable {
            writable.store(false, Ordering::Relaxed);
        }
        // `close` clears `running` first, so the swap tells a stop this
        // client asked for from one it did not. A panic during a close is
        // one it did not ask for, and the swap alone would say nothing.
        let panicked = std::thread::panicking();
        if self.running.swap(false, Ordering::Relaxed) || panicked {
            let cause = self.cause.take().unwrap_or(Stop::Panicked);
            // `on_stop` is the caller's code, and this `Drop` is on the way
            // down from a panic in `on_delivery`. A panic that meets that
            // one ends the process. One handler reaches it with one bug: the
            // first panic poisons a lock, and the report of it is the
            // `lock().unwrap()` everyone writes.
            //
            // The handler is `Sync`, so what it holds at a panic is what any
            // two threads of the caller's own can hold.
            let handler = self.handler;
            let report = panic::AssertUnwindSafe(move || handler.on_stop(cause));
            let _ = panic::catch_unwind(report);
        }
    }
}

/// Microseconds since the Unix epoch, for the timestamp of a written event.
/// Pacing takes [`Instant`], which a step of the wall clock cannot move.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_micros() as i64)
}

/// This gives `false` when `close` asks this thread to stop before `target`.
fn wait_until(target: Instant, running: &AtomicBool) -> bool {
    const SLICE: Duration = Duration::from_millis(100);
    loop {
        let remaining = target.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        if !running.load(Ordering::Relaxed) {
            return false;
        }
        std::thread::sleep(remaining.min(SLICE));
    }
}

fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

/// A datagram that no peer took draws an ICMP reply, and Linux gives that
/// reply to the next `recv_from` on the socket. The socket stays good, so
/// a bus that runs for weeks must not end on one. LCM also goes on.
fn is_transient(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
    )
}
