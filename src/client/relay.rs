//! A relay, reached over TCP.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::time::Duration;
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Instant;

use socket2::Socket;

use crate::bus::ReadBuffer;
use crate::wire::{Decoded, MAX_MESSAGE_LEN, WireError, tcpq};

use super::CONNECT_TIMEOUT;
use super::{
    Client, ClientError, Counters, Delivery, DeliveryHandler, Origin, ReaderExit, Receiving, Stop,
    Subscriptions, Transport, ignore_poison, is_timeout, now_micros,
};

/// The largest frame this reader will hold.
///
/// A ceiling of `MAX_MESSAGE_LEN` alone would refuse a message of exactly
/// that size, which is one this crate sends and one a C peer sends.
const TCPQ_FRAME_MAX: usize = 4 + 4 + tcpq::CHANNEL_READ_MAX + 4 + MAX_MESSAGE_LEN;

/// How long the writer holds one frame that is getting nowhere.
///
/// A write says a byte moved only where the kernel frees a part of the send
/// buffer, so this is a rate — that part of the buffer over this time — and
/// both ends of it belong to the platform. Minutes and not seconds, because a
/// link under whatever the rate is that day loses the connection, and nothing
/// waits behind this write but the outbox.
const STALL_TIMEOUT: Duration = Duration::from_secs(900);

/// How long a publish waits for room in the outbox, and a flush waits for the
/// wire. Reaching it costs the message and not the connection.
pub(super) const LONGEST_WAIT: Duration = Duration::from_secs(60);

/// How long a subscription waits for room in its line.
///
/// Shorter than [`LONGEST_WAIT`]: a handler subscribes from the reader
/// thread, and that thread has messages to take off the bus.
pub(super) const LONGEST_CONTROL_WAIT: Duration = Duration::from_secs(10);

/// `SO_SNDTIMEO`. A poll and not an answer: [`STALL_TIMEOUT`] decides, and it
/// can only decide between calls.
const WRITE_POLL: Duration = Duration::from_millis(500);

/// `SO_RCVTIMEO`. How often a reader looks at `running`, and so how long
/// `close` waits for it.
const READER_POLL: Duration = Duration::from_millis(100);

/// The bytes of messages the writer thread holds on their way out.
///
/// A frame is a message, so a bound with room for a whole one would refuse
/// what LCM carries. A frame goes where the outbox is *at* this, which is one
/// test whatever its length: without that, a large message waits for an empty
/// outbox that small ones never leave it, and the small ones wait behind it.
///
/// The frame on the wire is not on this, so an outbox holds this and two
/// messages. Counting it would hold every publish for as long as one large
/// message takes to write.
const OUTBOX_BYTES: usize = 4 * 1024 * 1024;

/// What a frame costs beside its bytes: its handle in the ring, and the block
/// its bytes are in. A budget of bytes alone holds hundreds of thousands of
/// the smallest frame.
const FRAME_COST: usize = size_of::<Vec<u8>>() + 2 * size_of::<usize>();

/// What a ring keeps once it empties. It holds the room it grew to otherwise.
const OUTBOX_KEEPS: usize = 64;

/// The bytes of subscription frames the writer thread holds.
///
/// A line of their own, because the subscription that matters most comes from
/// a handler on the reader thread and must not wait on a message of megabytes.
const OUTBOX_CONTROL_BYTES: usize = 1024 * 1024;

/// Which line a frame is in.
///
/// The writer takes control first, so the two do not finish in the sequence
/// they were let in, and one count for both would answer a flush with the
/// wrong frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lane {
    Message,
    Control,
}

/// What one line holds, and what has gone through it.
#[derive(Debug, Default)]
struct Sent {
    /// Frames let in, and of those the ones that reached the wire and the
    /// ones thrown away.
    taken: u64,
    wrote: u64,
    lost: u64,
}

/// The frames of one client on their way to the relay.
#[derive(Debug)]
struct Queue {
    /// From `publish`, in the sequence they were let in.
    messages: VecDeque<Vec<u8>>,
    /// From `subscribe` and `unsubscribe`. The writer takes these first.
    control: VecDeque<Vec<u8>>,
    /// The bytes in each line. The frame the writer holds is on neither:
    /// see [`OUTBOX_BYTES`].
    waiting: usize,
    control_waiting: usize,
    /// The line the frame the writer holds came from. `done` and `abandon`
    /// each take it, so a frame is counted once and not twice.
    in_hand: Option<Lane>,
    /// The publishes waiting for room, oldest first. Without an order, room
    /// goes to whoever wakes first and the largest message never gets any.
    line: VecDeque<u64>,
    next_ticket: u64,
    /// One count for each line. An empty line is not a line that went, so
    /// `lost` is what keeps `flushed` from reading one as the other.
    sent: Sent,
    control_sent: Sent,
    open: bool,
}

impl Queue {
    /// A ring that grew for a busy moment keeps that room, so this gives it
    /// back once the ring is empty.
    fn give_back_the_room(&mut self) {
        if self.messages.is_empty() && self.messages.capacity() > OUTBOX_KEEPS {
            self.messages.shrink_to(OUTBOX_KEEPS);
        }
        if self.control.is_empty() && self.control.capacity() > OUTBOX_KEEPS {
            self.control.shrink_to(OUTBOX_KEEPS);
        }
    }

    fn of(&mut self, lane: Lane) -> &mut Sent {
        match lane {
            Lane::Message => &mut self.sent,
            Lane::Control => &mut self.control_sent,
        }
    }
}

/// The frames between a publisher and the writer thread.
#[derive(Debug)]
pub(super) struct Outbox {
    queue: Mutex<Queue>,
    /// Bytes left a line, or a write finished, or the queue closed.
    room: Condvar,
    /// Bytes arrived, or the queue closed.
    filled: Condvar,
}

/// Why a frame did not go in, or a flush did not finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refused {
    /// The room, or the wire, never came inside the deadline.
    TooSlow,
    /// The connection ended.
    Gone,
}

impl Outbox {
    pub(super) fn new() -> Self {
        Self {
            queue: Mutex::new(Queue {
                messages: VecDeque::new(),
                control: VecDeque::new(),
                waiting: 0,
                control_waiting: 0,
                in_hand: None,
                line: VecDeque::new(),
                next_ticket: 0,
                sent: Sent::default(),
                control_sent: Sent::default(),
                open: true,
            }),
            room: Condvar::new(),
            filled: Condvar::new(),
        }
    }

    /// Puts one message in, waiting for room until `deadline`. Room goes to
    /// whoever has waited longest.
    pub(super) fn put_message(&self, frame: Vec<u8>, deadline: Instant) -> Result<(), Refused> {
        let mut queue = ignore_poison(self.queue.lock());
        let ticket = queue.next_ticket;
        queue.next_ticket += 1;
        queue.line.push_back(ticket);

        let refused = loop {
            if !queue.open {
                break Refused::Gone;
            }
            let room = queue.waiting <= OUTBOX_BYTES;
            if room && queue.line.front() == Some(&ticket) {
                queue.line.pop_front();
                queue.waiting += frame.len() + FRAME_COST;
                queue.messages.push_back(frame);
                queue.sent.taken += 1;
                self.filled.notify_one();
                self.room.notify_all();
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break Refused::TooSlow;
            }
            queue = ignore_poison(self.room.wait_timeout(queue, left)).0;
        };

        // Out of the line, or the frames behind it wait on a place nobody
        // is waiting in.
        queue.line.retain(|held| *held != ticket);
        self.room.notify_all();
        Err(refused)
    }

    /// Puts one subscription frame in. These have a line of their own.
    pub(super) fn put_control(&self, frame: Vec<u8>, deadline: Instant) -> Result<(), Refused> {
        let mut queue = ignore_poison(self.queue.lock());
        loop {
            if !queue.open {
                return Err(Refused::Gone);
            }
            if queue.control_waiting <= OUTBOX_CONTROL_BYTES {
                queue.control_waiting += frame.len() + FRAME_COST;
                queue.control.push_back(frame);
                queue.control_sent.taken += 1;
                self.filled.notify_one();
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Refused::TooSlow);
            }
            queue = ignore_poison(self.room.wait_timeout(queue, left)).0;
        }
    }

    /// The next frame, waiting up to `patience`. Subscriptions go first: one
    /// of them can be holding a reader thread.
    fn take(&self, patience: Duration) -> Option<(Lane, Vec<u8>)> {
        let mut queue = ignore_poison(self.queue.lock());
        loop {
            let took = match queue.control.pop_front() {
                Some(frame) => {
                    queue.control_waiting -= frame.len() + FRAME_COST;
                    Some((Lane::Control, frame))
                }
                None => queue
                    .messages
                    .pop_front()
                    .map(|frame| (Lane::Message, frame)),
            };
            if let Some((lane, frame)) = took {
                queue.in_hand = Some(lane);
                if lane == Lane::Message {
                    queue.waiting -= frame.len() + FRAME_COST;
                }
                queue.give_back_the_room();
                self.room.notify_all();
                return Some((lane, frame));
            }
            if !queue.open {
                return None;
            }
            let (held, timed) = ignore_poison(self.filled.wait_timeout(queue, patience));
            queue = held;
            if timed.timed_out() {
                return None;
            }
        }
    }

    /// The writer finished with the frame it took, and `went` says whether
    /// it reached the wire. An `abandon` that got there first took it.
    fn done(&self, went: bool) {
        let mut queue = ignore_poison(self.queue.lock());
        let Some(lane) = queue.in_hand.take() else {
            return;
        };
        let sent = queue.of(lane);
        match went {
            true => sent.wrote += 1,
            false => sent.lost += 1,
        }
        self.room.notify_all();
    }

    /// The messages let in that never went.
    pub(super) fn lost_messages(&self) -> u64 {
        ignore_poison(self.queue.lock()).sent.lost
    }

    /// Waits until every frame let in before now is on the wire.
    ///
    /// Counted, and not a wait for an empty queue: with one more thread
    /// publishing, a queue is never empty. Each line is counted on its own,
    /// because the writer takes them out of the sequence they went in.
    pub(super) fn flushed(&self, deadline: Instant) -> Result<(), Refused> {
        let mut queue = ignore_poison(self.queue.lock());
        let (wanted, wanted_control) = (queue.sent.taken, queue.control_sent.taken);
        loop {
            if queue.sent.wrote >= wanted && queue.control_sent.wrote >= wanted_control {
                return Ok(());
            }
            // A count the wire will never reach.
            if queue.sent.lost > 0 || queue.control_sent.lost > 0 || !queue.open {
                return Err(Refused::Gone);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Refused::TooSlow);
            }
            queue = ignore_poison(self.room.wait_timeout(queue, left)).0;
        }
    }

    /// Closes the queue, and counts the frames in the lines as lost.
    ///
    /// Not the frame the writer holds: `close` runs this while that write
    /// can still finish, and counting it would call a message that arrived
    /// in full one that never went. [`Outbox::abandon`] counts that one.
    pub(super) fn shut(&self) {
        let mut queue = ignore_poison(self.queue.lock());
        queue.open = false;
        let (messages, control) = (queue.messages.len() as u64, queue.control.len() as u64);
        queue.messages.clear();
        queue.control.clear();
        queue.waiting = 0;
        queue.control_waiting = 0;
        queue.give_back_the_room();
        queue.sent.lost += messages;
        queue.control_sent.lost += control;
        self.room.notify_all();
        self.filled.notify_all();
    }

    /// Counts the frame the writer holds as lost, where it holds one. `done`
    /// takes it on every way out of a write, so this is for a panic.
    pub(super) fn abandon(&self) {
        let mut queue = ignore_poison(self.queue.lock());
        if let Some(lane) = queue.in_hand.take() {
            queue.of(lane).lost += 1;
        }
        self.room.notify_all();
    }
}

/// Puts the frames of one client on the wire, in the sequence they were put
/// in.
///
/// A write that does not finish leaves part of a frame on the stream, and
/// the frame behind it would go into the middle of that one. So a write that
/// fails ends the connection, and the socket with it, so that the handler
/// hears one `Stop` for both halves. Every way out counts what the queue
/// still held.
fn tcpq_writer(
    stream: &TcpStream,
    outbox: &Outbox,
    writable: &AtomicBool,
    running: &AtomicBool,
    torn: &Mutex<Option<io::Error>>,
) {
    use std::io::Write;

    /// The reader has `ReaderExit` for this. Without the same here, a panic
    /// would leave the queue open with `running` and `writable` set, so
    /// `is_connected` would say yes for good and every publish would give
    /// back `Ok` and go nowhere.
    struct WriterExit<'a> {
        outbox: &'a Outbox,
        writable: &'a AtomicBool,
    }

    impl Drop for WriterExit<'_> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                self.writable.store(false, Ordering::Relaxed);
            }
            self.outbox.shut();
            self.outbox.abandon();
        }
    }

    let _exit = WriterExit { outbox, writable };

    while running.load(Ordering::Relaxed) {
        let Some((_, frame)) = outbox.take(WRITE_POLL) else {
            continue;
        };
        let mut moved = Instant::now();

        let mut rest = &frame[..];
        let failed = loop {
            if rest.is_empty() {
                break None;
            }
            // The reader can stop inside one frame, and a frame can take
            // minutes.
            if !running.load(Ordering::Relaxed) {
                break Some(io::Error::from(io::ErrorKind::Interrupted));
            }
            let wrote = match (&*stream).write(rest) {
                Ok(0) => Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(bytes) => Ok(bytes),
                // A poll, not an answer: `STALL_TIMEOUT` decides.
                Err(e) if is_timeout(&e) => Ok(0),
                Err(e) => Err(e),
            };
            match wrote {
                Ok(0) => {}
                Ok(bytes) => {
                    rest = &rest[bytes..];
                    moved = Instant::now();
                }
                Err(e) => break Some(e),
            }
            if !rest.is_empty() && moved.elapsed() >= STALL_TIMEOUT {
                break Some(io::Error::from(io::ErrorKind::TimedOut));
            }
        };
        outbox.done(failed.is_none());

        if let Some(e) = failed {
            writable.store(false, Ordering::Relaxed);
            // The reader reads the shutdown below as a relay that closed.
            // A peer that went is not a link that is too slow, and a caller
            // deciding on a new connection needs the two apart.
            *ignore_poison(torn.lock()) = Some(e);
            // A stream holding half a frame carries nothing more either way.
            let _ = stream.shutdown(std::net::Shutdown::Both);
            break;
        }
    }
}

/// Writes the whole of `bytes` before `deadline`, or gives up on it.
///
/// The opening subscriptions go on the thread of whoever called `connect`,
/// who has no client yet and so nothing to stop them with. The writer thread
/// needs no such bound: only the outbox waits behind it.
fn write_by(stream: &TcpStream, bytes: &[u8], deadline: Instant) -> io::Result<()> {
    use std::io::Write;

    let mut rest = bytes;
    while !rest.is_empty() {
        if Instant::now() >= deadline {
            return Err(io::Error::from(io::ErrorKind::TimedOut));
        }
        match (&*stream).write(rest) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(bytes) => rest = &rest[bytes..],
            // The socket timeout is the poll that gives this loop the thread
            // back so it can look at the deadline.
            Err(e) if is_timeout(&e) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Reads the whole of `into` before `deadline`, or gives up on it.
///
/// `read_exact` bounds no more than `write_all` does: a socket timeout bounds
/// one call, and a call that moves one byte starts it again.
fn read_by(stream: &mut TcpStream, into: &mut [u8], deadline: Instant) -> io::Result<()> {
    use std::io::Read;

    let mut rest = into;
    while !rest.is_empty() {
        if Instant::now() >= deadline {
            return Err(io::Error::from(io::ErrorKind::TimedOut));
        }
        match stream.read(rest) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(bytes) => rest = &mut rest[bytes..],
            Err(e) if is_timeout(&e) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// The addresses a name gives, and the deadline for the rest of the open.
///
/// A resolver is not a relay: it is slow for its own reasons, and time taken
/// from the relay's makes a slow resolver read as a relay that is not there.
fn addresses_of(address: &str) -> Result<(Vec<SocketAddr>, Instant), io::Error> {
    let found: Vec<SocketAddr> = address.to_socket_addrs()?.collect();
    if found.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            alloc::format!("`{address}` gives no address"),
        ));
    }
    Ok((found, Instant::now() + CONNECT_TIMEOUT))
}

/// Tries each address the name gave, holding all of them to `deadline`.
///
/// Each gets its share of what is left, and not the whole of it: a first
/// address that answers nothing would otherwise spend the deadline alone,
/// and a relay that is up and listening could not be reached.
fn connect_within(
    address: &str,
    found: &[SocketAddr],
    deadline: Instant,
) -> Result<TcpStream, io::Error> {
    let mut refusal = None;
    for (number, candidate) in found.iter().enumerate() {
        let left = deadline.saturating_duration_since(Instant::now());
        let share = left / (found.len() - number) as u32;
        if share.is_zero() {
            break;
        }
        match TcpStream::connect_timeout(candidate, share) {
            Ok(stream) => return Ok(stream),
            Err(e) => refusal = Some(e),
        }
    }
    Err(refusal.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            alloc::format!("`{address}` answered from no address in time"),
        )
    }))
}

impl Client {
    pub(super) fn open_tcpq(
        address: &str,
        receiving: Option<Receiving>,
    ) -> Result<Self, ClientError> {
        let subscriptions = Receiving::subscriptions(&receiving);
        // One deadline for the whole open: the addresses, the handshake and
        // the opening subscriptions all wait on the relay.
        let (found, opening) = addresses_of(address)?;
        let stream = connect_within(address, &found, opening)?;
        // Low latency is worth more than large writes here.
        stream.set_nodelay(true)?;
        // Without this a partition leaves `is_connected` saying yes for good.
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(30))
            .with_interval(Duration::from_secs(10));
        let socket = Socket::from(stream);
        let _ = socket.set_tcp_keepalive(&keepalive);
        let mut stream: TcpStream = socket.into();

        // Polls, so the loops below hold the deadline and a call that moves
        // one byte cannot start it again.
        stream.set_read_timeout(Some(WRITE_POLL))?;
        stream.set_write_timeout(Some(WRITE_POLL))?;
        write_by(&stream, &tcpq::handshake(), opening)?;
        let mut reply = [0u8; 8];
        read_by(&mut stream, &mut reply, opening)?;
        tcpq::check_handshake(&reply).map_err(ClientError::Handshake)?;
        // A read that waits for ever is a reader that stops only where
        // something shuts the socket under it, and shutting the reading half
        // is what makes the kernel answer what comes after with a reset.
        stream.set_read_timeout(Some(READER_POLL))?;

        // The relay matches on these, so it takes traffic off the wire. This
        // client keeps the set to refuse a bad pattern and for `unsubscribe`.
        for pattern in ignore_poison(subscriptions.read()).for_a_relay() {
            write_by(&stream, &tcpq::subscribe(&pattern), opening)?;
        }

        let running = Arc::new(AtomicBool::new(true));
        let writable = Arc::new(AtomicBool::new(true));
        let counters = Arc::new(Counters::default());
        let outbox = Arc::new(Outbox::new());
        // What the writer stopped for, where the reader would otherwise read
        // the writer's own `shutdown` as a relay that closed.
        let torn = Arc::new(Mutex::new(None));
        let mut reader = None;

        let writer = std::thread::Builder::new()
            .name("lcm-tcpq-w".into())
            .spawn({
                let stream = stream.try_clone()?;
                let outbox = outbox.clone();
                let writable = writable.clone();
                let running = running.clone();
                let torn = torn.clone();
                move || tcpq_writer(&stream, &outbox, &writable, &running, &torn)
            })
            .map_err(ClientError::Io)?;

        // The writer is running, and holds the only handle on `running` that
        // stops it. An `Err` from here would leave it and its socket for the
        // life of the program.
        let started = |receiving: Receiving| -> Result<JoinHandle<()>, ClientError> {
            let handler = receiving.handler;
            std::thread::Builder::new()
                .name("lcm-tcpq".into())
                .spawn({
                    let stream = stream.try_clone()?;
                    let subscriptions = subscriptions.clone();
                    let running = running.clone();
                    let writable = writable.clone();
                    let counters = counters.clone();
                    let torn = torn.clone();
                    move || {
                        let mut exit = ReaderExit {
                            running: &running,
                            writable: Some(&writable),
                            handler: &*handler,
                            cause: Some(Stop::Panicked),
                        };
                        exit.cause = tcpq_reader(
                            stream,
                            &subscriptions,
                            &*handler,
                            &counters,
                            &running,
                            &torn,
                        );
                    }
                })
                .map_err(ClientError::Io)
        };

        if let Some(receiving) = receiving {
            match started(receiving) {
                Ok(started) => reader = Some(started),
                Err(e) => {
                    running.store(false, Ordering::Relaxed);
                    outbox.shut();
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    let _ = writer.join();
                    return Err(e);
                }
            }
        }

        Ok(Self {
            receives: reader.is_some(),
            transport: Transport::Tcpq {
                stream,
                outbox,
                writer: Mutex::new(Some(writer)),
            },
            subscriptions,
            counters,
            running,
            writable,
            reader: Mutex::new(reader),
            changing: Mutex::new(()),
        })
    }
}

pub(super) fn tcpq_reader(
    mut stream: TcpStream,
    subscriptions: &RwLock<Subscriptions>,
    handler: &dyn DeliveryHandler,
    counters: &Counters,
    running: &AtomicBool,
    torn: &Mutex<Option<io::Error>>,
) -> Option<Stop> {
    let mut pending = ReadBuffer::with_limits(16 * 1024, TCPQ_FRAME_MAX);

    while running.load(Ordering::Relaxed) {
        match pending.fill_from(&mut stream) {
            // The writer shuts this socket where a write of its own tore,
            // and the end of the stream then reads as a relay that closed.
            // What it was is on `torn`, and it is not the same answer for a
            // caller deciding whether to make the connection again.
            Ok(0) => {
                return Some(match ignore_poison(torn.lock()).take() {
                    Some(e) => Stop::Io(e),
                    None => Stop::Closed,
                });
            }
            Ok(_) => {}
            Err(e) if is_timeout(&e) => continue,
            Err(e) => return Some(Stop::Io(e)),
        }

        // One fill holds many frames, and each one goes to the handler,
        // whose time is the caller's own. `close` waits for this thread, so
        // without this test it waits for every frame that is already here.
        while running.load(Ordering::Relaxed) {
            let decoded = match tcpq::decode(pending.unread()) {
                Ok(Decoded::Item(frame, used)) => Ok((frame.to_frame(), used)),
                Ok(other) => Err(other),
                // After a framing error, this client cannot find the next one.
                Err(e) => return Some(Stop::Wire(e)),
            };
            let (frame, used) = match decoded {
                Ok(item) => item,
                // A frame with a length this client will not hold is a peer
                // it will not follow.
                Err(Decoded::Need(bytes)) => {
                    if !pending.reserve(bytes) {
                        return Some(Stop::Wire(WireError::MessageTooLarge(bytes)));
                    }
                    break;
                }
                // A name no encoder here would write. The frame has a length,
                // so this costs the message and not the connection.
                Err(Decoded::Skip(bytes)) => {
                    counters.received();
                    counters.discarded();
                    pending.consume(bytes);
                    continue;
                }
                Err(Decoded::Item(..)) => unreachable!("taken above"),
            };
            pending.consume(used);
            counters.received();

            // The relay matched these too, with `java.util.regex` and not
            // with the engine here, and a pattern the two read differently
            // can quietly change which channels come. `unsubscribe` also
            // gives back before the messages on the wire do. LCM matches
            // here too, for the same reasons.
            let wanted = ignore_poison(subscriptions.read()).matches(&frame.channel);
            if !wanted {
                counters.discarded();
                continue;
            }
            counters.delivered();
            // A relay sends no time of its own, so this is the arrival.
            handler.on_delivery(Delivery {
                frame,
                timestamp: now_micros(),
                origin: Origin::Relay,
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A name can give more than one address, and the first can be one that
    /// answers nothing: an AAAA record on a host with no route for it, a
    /// stale A record, a firewall that drops what it is sent. One deadline
    /// for the whole open must not mean the first address spends all of it,
    /// or a relay that is up and listening cannot be reached at all.
    #[test]
    fn every_address_a_name_gives_is_tried() {
        use std::net::TcpListener;

        // One that answers nothing. A documentation address is not routed,
        // so what is sent to it goes nowhere and nothing comes back — which
        // is the case this is about, and not a refusal.
        let deaf: SocketAddr = "192.0.2.1:9".parse().unwrap();
        if TcpStream::connect_timeout(&deaf, Duration::from_millis(300)).is_ok() {
            // This address answers here, so there is nothing to test.
            return;
        }

        // And one that answers at once.
        let good = TcpListener::bind("127.0.0.1:0").expect("bind");
        let good_at = good.local_addr().unwrap();

        let began = Instant::now();
        let reached = connect_within("two.addresses", &[deaf, good_at], began + CONNECT_TIMEOUT);
        let took = began.elapsed();

        assert!(
            reached.is_ok(),
            "the second address was never tried: {:?} after {took:?}",
            reached.err()
        );
        assert!(took < CONNECT_TIMEOUT, "and it took {took:?}");
    }

    /// The writer takes subscriptions before messages, so the two lines do
    /// not go in the sequence they were let in. One count for both then lets
    /// a subscription that went stand for a message that did not: a `flush`
    /// waiting on a publish answers because a subscription behind it reached
    /// the wire first.
    ///
    /// That loses messages quietly, because `close` throws away what is
    /// left. Publish, flush, close is the ordinary shape, and it would report
    /// success for a message that never went.
    #[test]
    fn a_subscription_does_not_answer_a_flush_that_waits_on_a_message() {
        use std::sync::atomic::AtomicBool;

        let outbox = Arc::new(Outbox::new());
        let far = Instant::now() + Duration::from_secs(60);
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();

        // A flush from here is waiting on that message, and on nothing else.
        let answered = Arc::new(AtomicBool::new(false));
        let flushing = std::thread::spawn({
            let outbox = outbox.clone();
            let answered = answered.clone();
            move || {
                let flushed = outbox.flushed(Instant::now() + Duration::from_secs(30));
                answered.store(true, Ordering::SeqCst);
                flushed
            }
        });
        // Long enough for it to have read the counts it waits for.
        std::thread::sleep(Duration::from_millis(100));

        // A subscription, let in after that and on the wire before it.
        outbox.put_control(alloc::vec![1u8; 4], far).unwrap();
        let (lane, frame) = outbox.take(Duration::ZERO).unwrap();
        assert_eq!(lane, Lane::Control, "the writer takes subscriptions first");
        assert_eq!(frame.len(), 4);
        outbox.done(true);

        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !answered.load(Ordering::SeqCst),
            "the flush answered on a subscription, and its message is still here"
        );

        // And the message itself answers it.
        let (lane, _) = outbox.take(Duration::ZERO).unwrap();
        assert_eq!(lane, Lane::Message);
        outbox.done(true);
        assert_eq!(flushing.join().unwrap(), Ok(()));
    }

    /// A subscription that never went is not a message a caller lost.
    #[test]
    fn unsent_counts_messages_and_not_subscriptions() {
        let outbox = Outbox::new();
        let far = Instant::now() + Duration::from_secs(60);
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();
        for _ in 0..5 {
            outbox.put_control(alloc::vec![1u8; 4], far).unwrap();
        }
        outbox.shut();
        assert_eq!(outbox.lost_messages(), 1);
    }

    /// A message above the budget waits for an outbox at the budget, and not
    /// for an empty one.
    ///
    /// An empty one is what a run of small messages never lets it have, so
    /// it waits for good — and with the line keeping the sequence, every
    /// small message behind it waits for good as well. One test for every
    /// frame, whatever its length, is what closes both.
    #[test]
    fn a_large_message_does_not_wait_for_an_empty_outbox() {
        let outbox = Outbox::new();
        let far = Instant::now() + Duration::from_secs(60);

        // A little in the outbox, and most of the budget free.
        outbox.put_message(alloc::vec![7u8; 1024], far).unwrap();

        // No free room could ever hold this one, and it goes in all the same.
        assert_eq!(
            outbox.put_message(alloc::vec![7u8; OUTBOX_BYTES + 1], Instant::now()),
            Ok(()),
            "a large message waited on an outbox that a small one keeps full"
        );

        // The outbox is over its budget now, so the next one waits — for the
        // one being written, and not for good.
        assert_eq!(
            outbox.put_message(alloc::vec![7u8; 8], Instant::now()),
            Err(Refused::TooSlow)
        );
        for _ in 0..2 {
            outbox.take(Duration::ZERO).unwrap();
            outbox.done(true);
        }
        assert_eq!(
            outbox.put_message(alloc::vec![7u8; 8], Instant::now()),
            Ok(())
        );
    }

    /// A frame costs its handle in the ring as well as its bytes, and the
    /// smallest a publish makes is thirteen bytes. A budget of bytes alone
    /// would take three hundred thousand of them.
    #[test]
    fn the_budget_counts_the_ring_and_not_the_bytes_alone() {
        let outbox = Outbox::new();
        let now = Instant::now();
        let mut frames = 0;
        while outbox.put_message(alloc::vec![7u8; 13], now).is_ok() {
            frames += 1;
            assert!(frames < 200_000, "a budget of bytes alone would take more");
        }
        assert!(
            frames < OUTBOX_BYTES / (13 + FRAME_COST) + 2,
            "{frames} frames of thirteen bytes on a {OUTBOX_BYTES}-byte budget"
        );

        // And the ring gives its room back once it empties.
        let grew = ignore_poison(outbox.queue.lock()).messages.capacity();
        assert!(grew > OUTBOX_KEEPS, "the ring grew to {grew}");
        outbox.shut();
        assert_eq!(outbox.lost_messages(), frames as u64);
        let kept = ignore_poison(outbox.queue.lock()).messages.capacity();
        assert!(kept <= OUTBOX_KEEPS, "the ring kept room for {kept} frames");
    }

    /// The frame the writer holds is in no line, so a `shut` that counted
    /// only the lines would say nothing about it. Both `shut` and `done`
    /// account for it, and it must be the one or the other: `close` shuts
    /// the queue while the writer is in the middle of a write, and a message
    /// counted twice is one a caller never sent.
    #[test]
    fn the_frame_in_hand_is_counted_once_whichever_way_the_writer_stops() {
        let far = Instant::now() + Duration::from_secs(60);

        // The writer stops with the frame in hand, as it does on a panic.
        let outbox = Outbox::new();
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        outbox.shut();
        outbox.abandon();
        assert_eq!(outbox.lost_messages(), 1, "the frame in hand went nowhere");

        // A close shuts the queue, and then the write fails and the writer
        // finishes with the same frame.
        let outbox = Outbox::new();
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        outbox.shut();
        outbox.done(false);
        outbox.abandon();
        assert_eq!(outbox.lost_messages(), 1, "and it is counted once");

        // A close shuts the queue while the writer is inside the last write
        // of a frame, and that write then finishes. The frame is on the
        // wire: a caller that goes by `unsent` would send it a second time.
        let outbox = Outbox::new();
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        outbox.shut();
        outbox.done(true);
        outbox.abandon();
        assert_eq!(
            outbox.lost_messages(),
            0,
            "a message that reached the relay in full is counted in `unsent`"
        );

        // A write that tears, and then the queue closes behind it.
        let outbox = Outbox::new();
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        outbox.done(false);
        outbox.shut();
        outbox.abandon();
        assert_eq!(outbox.lost_messages(), 1);

        // And a frame that went is not counted at all.
        let outbox = Outbox::new();
        outbox.put_message(alloc::vec![7u8; 8], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        outbox.done(true);
        outbox.shut();
        outbox.abandon();
        assert_eq!(outbox.lost_messages(), 0);
    }

    /// A `shut` taken mid-frame leaves nothing on the books.
    #[test]
    fn a_shut_leaves_the_budget_where_it_found_it() {
        let outbox = Outbox::new();
        let far = Instant::now() + Duration::from_secs(60);
        outbox.put_message(alloc::vec![7u8; 4096], far).unwrap();
        outbox.put_message(alloc::vec![7u8; 4096], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        outbox.shut();

        let queue = ignore_poison(outbox.queue.lock());
        assert_eq!(queue.waiting, 0);
        assert_eq!(queue.control_waiting, 0);
    }

    /// The frame the writer holds is not on the budget, so a message on the
    /// wire does not stop the next one being let in.
    ///
    /// Counting it would: a message above the budget holds the whole outbox
    /// for as long as it takes to write, and every publish behind it waits
    /// its own deadline and then fails. On a slow link that is hours of a
    /// client that says it is connected and takes nothing.
    #[test]
    fn a_message_on_the_wire_does_not_hold_the_outbox() {
        let outbox = Outbox::new();
        let far = Instant::now() + Duration::from_secs(60);

        // One the budget cannot hold, and the writer takes it.
        outbox
            .put_message(alloc::vec![7u8; OUTBOX_BYTES + 1], far)
            .unwrap();
        let (lane, _) = outbox.take(Duration::ZERO).unwrap();
        assert_eq!(lane, Lane::Message);

        // The next one goes in while that is still being written.
        assert_eq!(
            outbox.put_message(alloc::vec![7u8; 8], Instant::now()),
            Ok(()),
            "a publish waited on a message that was already on the wire"
        );
        // And a subscription in hand does not hold one either.
        outbox.done(true);
        outbox.put_control(alloc::vec![1u8; 4], far).unwrap();
        outbox.take(Duration::ZERO).unwrap();
        assert_eq!(
            outbox.put_message(alloc::vec![7u8; OUTBOX_BYTES + 1], Instant::now()),
            Ok(())
        );
    }
}
