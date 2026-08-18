//! The relay, as the side that listens.
//! [`super::relay`] is the side that dials.
//!
//! A client that publishes hears its own message where its own patterns match it, as it
//! does on a `udpm://` bus through loopback.
//! Its patterns arrive after its connection, so a message published between the two
//! reaches it not at all: LCM's own relay reads the same frames in the same sequence.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::bus::ReadBuffer;
use crate::wire::{Decoded, MAX_MESSAGE_LEN, WireError, tcpq};

use super::relay::{LONGEST_WAIT, Outbox};
use super::{
    Client, ClientError, Counters, Delivery, DeliveryHandler, Origin, Receiving, Stop,
    Subscriptions, Transport, ignore_poison, is_timeout, now_micros,
};

/// Nothing interrupts a listener that blocks in `accept`, so this one polls.
/// It is also how long `close` waits for the accept thread.
const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// `SO_RCVTIMEO`, so how long `close` waits for one reader.
const READER_POLL: Duration = Duration::from_millis(100);

/// A connection that sends nothing holds a thread, and a peer that speaks this
/// protocol writes its eight bytes at once.
const GREETING_WAIT: Duration = Duration::from_secs(10);

const FRAME_MAX: usize = 4 + 4 + tcpq::CHANNEL_READ_MAX + 4 + MAX_MESSAGE_LEN;

struct Peer {
    id: u64,
    subscriptions: RwLock<Subscriptions>,
    /// One for each peer, so a client that stops reading holds up its own connection
    /// and not the publisher.
    outbox: Arc<Outbox>,
    /// A shut on this frees the reader thread that waits in a `read`.
    stream: TcpStream,
    writable: Arc<AtomicBool>,
}

pub(super) struct Served {
    peers: Mutex<Vec<Arc<Peer>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    next_id: AtomicU64,
    running: Arc<AtomicBool>,
    /// The process that serves the relay, which is a participant like any client.
    local: Option<Receiving>,
    counters: Arc<Counters>,
    /// Over the whole life of the relay, where `peers` is what it holds now.
    accepted: AtomicU64,
    /// Greetings that were not this protocol.
    refused: AtomicU64,
}

impl Served {
    /// `encoded` is the frame already written, because one write serves every peer.
    fn dispatch(&self, channel: &str, payload: &[u8], encoded: &[u8]) {
        let peers = ignore_poison(self.peers.lock()).clone();
        for peer in peers {
            if !ignore_poison(peer.subscriptions.read()).matches(channel) {
                continue;
            }
            // One client that stopped its reads must not hold the thread that publishes.
            // A frame that reaches the deadline costs that client the message.
            let deadline = Instant::now() + LONGEST_WAIT;
            let _ = peer.outbox.put_message(encoded.to_vec(), deadline);
        }

        let Some(local) = self.local.as_ref() else {
            return;
        };
        self.counters.received();
        if !ignore_poison(local.subscriptions.read()).matches(channel) {
            self.counters.discarded();
            return;
        }
        self.counters.delivered();
        local.handler.on_delivery(Delivery {
            frame: crate::wire::FrameRef { channel, payload }.to_frame(),
            origin: Origin::Relay,
            timestamp: now_micros(),
        });
    }

    fn drop_peer(&self, id: u64) {
        let mut peers = ignore_poison(self.peers.lock());
        if let Some(at) = peers.iter().position(|peer| peer.id == id) {
            let peer = peers.remove(at);
            drop(peers);
            peer.outbox.shut();
            let _ = peer.stream.shutdown(std::net::Shutdown::Both);
        }
    }

    pub(super) fn peers(&self) -> usize {
        ignore_poison(self.peers.lock()).len()
    }

    pub(super) fn peer_patterns(&self) -> usize {
        ignore_poison(self.peers.lock())
            .iter()
            .map(|peer| {
                ignore_poison(peer.subscriptions.read())
                    .for_a_relay()
                    .count()
            })
            .sum()
    }

    pub(super) fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    pub(super) fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    pub(super) fn shut(&self) {
        self.running.store(false, Ordering::Relaxed);
        let peers = core::mem::take(&mut *ignore_poison(self.peers.lock()));
        for peer in peers {
            peer.outbox.shut();
            let _ = peer.stream.shutdown(std::net::Shutdown::Both);
        }
        let threads = core::mem::take(&mut *ignore_poison(self.threads.lock()));
        for thread in threads {
            let _ = thread.join();
        }
    }
}

impl Client {
    /// Serve a `tcpq://` relay, rather than dial one.
    ///
    /// `address` is the address to bind.
    /// `tcpq://:7700` and `tcpq://0.0.0.0:7700` each listen on every address of this
    /// host, and a named one listens on that address alone.
    ///
    /// `subscriptions` is what the *local* handler receives, and not what the relay
    /// carries.
    /// A relay carries what its clients ask for.
    /// Thus a process that publishes onto one and wants none of the traffic back serves
    /// it with an empty set.
    pub(super) fn serve_tcpq(
        address: &str,
        receiving: Option<Receiving>,
    ) -> Result<Self, ClientError> {
        let subscriptions = Receiving::subscriptions(&receiving);
        let listener = TcpListener::bind(address).map_err(ClientError::Io)?;
        // Thus the accept loop can look at `running`.
        listener.set_nonblocking(true).map_err(ClientError::Io)?;
        let bound = listener.local_addr().map_err(ClientError::Io)?;

        let running = Arc::new(AtomicBool::new(true));
        let counters = Arc::new(Counters::default());
        let served = Arc::new(Served {
            peers: Mutex::new(Vec::new()),
            threads: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            running: running.clone(),
            local: receiving,
            counters: counters.clone(),
            accepted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
        });

        let accept = {
            let served = served.clone();
            std::thread::Builder::new()
                .name(format!("lcm-relay-{bound}"))
                .spawn(move || accept_loop(listener, &served))
                .map_err(ClientError::Io)?
        };

        Ok(Client {
            transport: Transport::Serve {
                served,
                bound,
                accept: Mutex::new(Some(accept)),
            },
            receives: true,
            writable: Arc::new(AtomicBool::new(true)),
            subscriptions,
            counters,
            running,
            reader: Mutex::new(None),
            changing: Mutex::new(()),
        })
    }
}

fn accept_loop(listener: TcpListener, served: &Arc<Served>) {
    while served.running.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _address)) => {
                if let Err(e) = start_peer(served, stream) {
                    // That connection alone, and the relay goes on listening.
                    let _ = e;
                    served.refused.fetch_add(1, Ordering::Relaxed);
                }
            }
            // The listener does not block, so this is the usual answer.
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(ACCEPT_POLL);
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            // The listener itself is gone, so nothing more can arrive.
            Err(_) => return,
        }
    }
}

fn start_peer(served: &Arc<Served>, stream: TcpStream) -> io::Result<()> {
    // A connection is a socket of its own, and this one blocks with timeouts.
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(GREETING_WAIT))?;
    stream.set_write_timeout(Some(LONGEST_WAIT))?;

    // The relay greets first, as LCM's own does: the client reads eight bytes
    // before it writes its own.
    {
        use std::io::Write;
        (&stream).write_all(&tcpq::server_handshake())?;
    }
    let mut greeting = [0u8; 8];
    {
        use std::io::Read;
        (&stream).read_exact(&mut greeting)?;
    }
    if tcpq::check_client_handshake(&greeting).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the greeting is not that of an LCM relay client",
        ));
    }

    stream.set_read_timeout(Some(READER_POLL))?;

    let id = served.next_id.fetch_add(1, Ordering::Relaxed);
    let outbox = Arc::new(Outbox::new());
    let writable = Arc::new(AtomicBool::new(true));
    let peer = Arc::new(Peer {
        id,
        subscriptions: RwLock::new(Subscriptions::new()),
        outbox: outbox.clone(),
        stream: stream.try_clone()?,
        writable: writable.clone(),
    });

    let writer = {
        let served = served.clone();
        let stream = stream.try_clone()?;
        let peer = peer.clone();
        std::thread::Builder::new()
            .name(format!("lcm-relay-w{id}"))
            .spawn(move || {
                // A torn write belongs to this connection alone.
                let torn = Mutex::new(None);
                super::relay::tcpq_writer(
                    &stream,
                    &peer.outbox,
                    &peer.writable,
                    &served.running,
                    &torn,
                );
            })?
    };

    let reader = {
        let served = served.clone();
        let peer = peer.clone();
        std::thread::Builder::new()
            .name(format!("lcm-relay-r{id}"))
            .spawn(move || {
                peer_reader(stream, &peer, &served);
                served.drop_peer(peer.id);
            })?
    };

    ignore_poison(served.peers.lock()).push(peer);
    {
        let mut threads = ignore_poison(served.threads.lock());
        threads.push(writer);
        threads.push(reader);
    }
    served.accepted.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn peer_reader(mut stream: TcpStream, peer: &Peer, served: &Served) -> Option<Stop> {
    let mut pending = ReadBuffer::with_limits(16 * 1024, FRAME_MAX);

    while served.running.load(Ordering::Relaxed) {
        match pending.fill_from(&mut stream) {
            Ok(0) => return Some(Stop::Closed),
            Ok(_) => {}
            Err(e) if is_timeout(&e) => continue,
            Err(e) => return Some(Stop::Io(e)),
        }

        while served.running.load(Ordering::Relaxed) {
            let (request, used) = match tcpq::decode_request(pending.unread()) {
                Ok(Decoded::Item(request, used)) => (request, used),
                Ok(Decoded::Need(bytes)) => {
                    // A client that announces more than this is one to drop.
                    if !pending.reserve(bytes) {
                        return Some(Stop::Wire(WireError::MessageTooLarge(bytes)));
                    }
                    break;
                }
                // A known length, so a step over it costs the request and not the
                // connection.
                Ok(Decoded::Skip(bytes)) => {
                    pending.consume(bytes);
                    continue;
                }
                // Nothing after a frame of unknown length can be found.
                Err(e) => return Some(Stop::Wire(e)),
            };

            match request {
                tcpq::Request::Subscribe(pattern) => {
                    // LCM's relay reads a Java regex, so a correct client can name a
                    // pattern this engine refuses.
                    // That costs the pattern and not the connection.
                    let _ = ignore_poison(peer.subscriptions.write()).add(pattern);
                }
                tcpq::Request::Unsubscribe(pattern) => {
                    ignore_poison(peer.subscriptions.write()).remove(pattern);
                }
                tcpq::Request::Publish(frame) => {
                    // Once, and the fan-out gives each peer the same bytes.
                    let Ok(encoded) = tcpq::publish(frame) else {
                        // The decoder holds a frame to these same limits, so nothing
                        // reaches here.
                        pending.consume(used);
                        continue;
                    };
                    served.dispatch(frame.channel, frame.payload, &encoded);
                }
            }
            pending.consume(used);
        }
    }
    None
}

impl core::fmt::Debug for Served {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Served")
            .field("peers", &self.peers())
            .field("accepted", &self.accepted())
            .finish()
    }
}

/// The publish of the relay itself: the local participant puts a message on the bus.
pub(super) fn publish_from_local(
    served: &Served,
    frame: crate::wire::FrameRef<'_>,
) -> Result<(), ClientError> {
    let encoded = tcpq::publish(frame).map_err(ClientError::Wire)?;
    served.dispatch(frame.channel, frame.payload, &encoded);
    Ok(())
}

/// The address to bind, from a `tcpq://` URL.
///
/// [`BusUrl::parse`](crate::BusUrl::parse) fills an empty host with the address LCM's
/// own relay dials.
/// A relay binds and does not dial, so an empty host here is every address of this
/// machine and not that one.
pub(super) fn bind_address(relay: &crate::url::Relay) -> String {
    match relay.host.as_str() {
        // That default is a loopback address, and a bind on it serves this host alone.
        host if host == crate::url::DEFAULT_TCPQ_HOST => format!("0.0.0.0:{}", relay.port),
        host => format!("{host}:{}", relay.port).to_string(),
    }
}
