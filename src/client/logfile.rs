//! A `.lcmlog` file, read or written.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::bus::{Pace, ReadBuffer};
use crate::url::{LogFile, LogMode};
use crate::wire::{Decoded, MAX_MESSAGE_LEN, WireError, log};

use super::{
    Client, ClientError, Counters, Delivery, DeliveryHandler, LogWriter, Origin, ReaderExit,
    Receiving, Stop, Subscriptions, Transport, ignore_poison, is_timeout, wait_until,
};

/// The largest event this reader will hold, on the same reasoning as
/// `TCPQ_FRAME_MAX`.
const LOG_EVENT_MAX: usize = log::HEADER_LEN + log::CHANNEL_MAX + MAX_MESSAGE_LEN;

/// `O_NONBLOCK` where the platform has it.
///
/// The open of a FIFO waits: one to read waits for a writer, and one to
/// write waits for a reader. Nothing here takes such a wait back, and it is
/// on the caller's own thread. A file waits for neither, and a read of one
/// does not wait either, so this changes nothing for a log.
#[cfg(unix)]
fn without_a_wait(open: &mut std::fs::OpenOptions) -> &mut std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    open.custom_flags(libc::O_NONBLOCK)
}

#[cfg(not(unix))]
fn without_a_wait(open: &mut std::fs::OpenOptions) -> &mut std::fs::OpenOptions {
    open
}

/// Opens a log to replay, and gives back a handle only for a file.
///
/// A test of the path and an open of the path reach two different objects
/// where something moves one between the two. So this asks the handle what
/// it holds, and not the name it asked for.
fn open_to_replay(path: &str) -> Result<std::fs::File, ClientError> {
    let open = without_a_wait(std::fs::OpenOptions::new().read(true)).open(path)?;
    if !open.metadata()?.is_file() {
        return Err(ClientError::NotAFile);
    }
    Ok(open)
}

/// Puts the read at the first event of the log at or after `timestamp`.
///
/// A log is a chain: the head of an event holds the full length of it, so
/// this walks that chain and reads the heads alone.
///
/// A jump to the middle and a look for a sync word from there is faster, and
/// is what LCM does, and is not safe: a payload holds the bytes a publisher
/// selected, so a jump that stops in one reads that payload as a log and
/// gives out events no publisher sent. No count of what follows tells the
/// two apart, because the payload holds those events as well.
///
/// A length in a head can lie, so the reader starts at the last head this
/// read and not where a step went. That puts it on the event that lied, and
/// it drops that one and finds the next sync word.
fn seek_before(file: &mut std::io::BufReader<std::fs::File>, timestamp: i64, running: &AtomicBool) {
    use std::io::{Read, Seek, SeekFrom};

    // Where the walk is, and where the reader is to start.
    let (mut at, mut from) = (0u64, 0u64);
    if file.rewind().is_ok() {
        let mut head = [0u8; log::HEADER_LEN];
        // The walk reads every head in front of the start, and a long log
        // holds many. `close` waits for this thread, so the walk looks at
        // `running` as the loop below it does.
        while running.load(Ordering::Relaxed) && file.read_exact(&mut head).is_ok() {
            let Ok(Some(peeked)) = log::peek(&head) else {
                break;
            };
            from = at;
            if peeked.timestamp >= timestamp {
                break;
            }
            // The head is read, and the rest of the event is stepped over.
            let rest = (peeked.len - log::HEADER_LEN) as i64;
            let (Ok(()), Some(next)) =
                (file.seek_relative(rest), at.checked_add(peeked.len as u64))
            else {
                break;
            };
            at = next;
        }
    }
    let _ = file.seek(SeekFrom::Start(from));
}

/// The timestamps set the rate.
pub(super) fn log_reader(
    file: std::fs::File,
    log_file: &LogFile,
    subscriptions: &RwLock<Subscriptions>,
    handler: &dyn DeliveryHandler,
    counters: &Counters,
    running: &AtomicBool,
) -> Option<Stop> {
    let mut file = std::io::BufReader::new(file);
    // A walk that stops short costs time and not events: the filter below
    // drops each event before the start.
    if let Some(start) = log_file.replay.start_timestamp {
        seek_before(&mut file, start, running);
    }

    let mut pending = ReadBuffer::with_limits(64 * 1024, LOG_EVENT_MAX);
    // The event of the last turn, taken out of the buffer at the start of the
    // next one so that the borrow lives as long as the event does.
    let mut decoded = 0;
    let mut pace = Pace::new(log_file.replay.speed);
    // The clock time this reader gave to the event before this one.
    let mut anchor: Option<Instant> = None;

    while running.load(Ordering::Relaxed) {
        pending.consume(decoded);
        decoded = 0;

        let event = match log::decode(pending.unread()) {
            Ok(Decoded::Item(event, used)) => {
                decoded = used;
                counters.received();
                event
            }
            // A name no encoder here would write, on an event whose length
            // the sync word behind it agrees with. One of those costs
            // itself and nothing behind it.
            Ok(Decoded::Skip(bytes)) => {
                counters.received();
                counters.discarded();
                decoded = bytes;
                continue;
            }
            // An event of a length this reader will not hold is a log it
            // will not follow.
            Ok(Decoded::Need(bytes)) => {
                if !pending.reserve(bytes) {
                    return Some(Stop::Wire(WireError::MessageTooLarge(bytes)));
                }
                match pending.fill_from(&mut file) {
                    // The head read, so what is left is that event cut short
                    // in the payload it names — a publisher's bytes, holding
                    // whatever sync word a publisher put there. So the tail
                    // is dropped, and said to be dropped.
                    Ok(0) => {
                        if pending.unread().is_empty() {
                            return Some(Stop::EndOfLog);
                        }
                        counters.discarded();
                        return Some(Stop::TornLog);
                    }
                    Ok(_) => {}
                    Err(e) if is_timeout(&e) => {}
                    Err(e) => return Some(Stop::Io(e)),
                }
                continue;
            }
            // A log reader slides to the next sync word.
            // With none here, keep the bytes that can hold the start of one.
            Err(_) => {
                counters.discarded();
                let unread = pending.unread().len();
                decoded = match pending.unread().get(1..).and_then(log::resync) {
                    Some(offset) => 1 + offset,
                    None => unread.saturating_sub(3),
                };
                continue;
            }
        };

        if log_file
            .replay
            .start_timestamp
            .is_some_and(|start| event.timestamp < start)
        {
            continue;
        }

        // The hold comes from the times in the log; the clock it goes on is
        // this reader's. They add up from the first event, so a slow turn
        // does not push the ones after it more and more out of time.
        let hold = pace.hold(event.timestamp);
        let target = match anchor {
            // `Instant` is a counter on some platforms, and adding to one
            // can go above what that counter holds.
            Some(at) => at.checked_add(hold).unwrap_or(at),
            None => Instant::now(),
        };
        anchor = Some(target);
        if !wait_until(target, running) {
            return None;
        }

        // Taken and given back before the handler runs, because a handler
        // can go to `subscribe` and its write lock.
        let wanted = ignore_poison(subscriptions.read()).matches(event.frame.channel);
        if wanted {
            counters.delivered();
            // The time the log holds, and not the time it is read.
            handler.on_delivery(Delivery {
                frame: event.frame.to_frame(),
                timestamp: event.timestamp,
                origin: Origin::Log {
                    number: event.number,
                },
            });
        }
    }
    None
}

impl Client {
    pub(super) fn open_log(
        file: LogFile,
        receiving: Option<Receiving>,
    ) -> Result<Self, ClientError> {
        let running = Arc::new(AtomicBool::new(true));
        let writable = Arc::new(AtomicBool::new(true));
        let counters = Arc::new(Counters::default());
        let subscriptions = Receiving::subscriptions(&receiving);

        let (transport, reader) = match (file.mode, receiving) {
            // A replay with no handler gives its events to nobody, and
            // `open_publisher` refuses one before it gets here.
            (LogMode::Read, Some(receiving)) => {
                let handler = receiving.handler;
                // A read of a FIFO, a device or a socket has no end, and
                // nothing here takes it back: `close` waits for the reader
                // thread, and that thread waits in the read.
                let open = open_to_replay(&file.path)?;
                let reader = std::thread::Builder::new()
                    .name("lcm-log".into())
                    .spawn({
                        let running = running.clone();
                        let subscriptions = subscriptions.clone();
                        let counters = counters.clone();
                        move || {
                            let mut exit = ReaderExit {
                                running: &running,
                                writable: None,
                                handler: &*handler,
                                cause: Some(Stop::Panicked),
                            };
                            exit.cause = log_reader(
                                open,
                                &file,
                                &subscriptions,
                                &*handler,
                                &counters,
                                &running,
                            );
                        }
                    })
                    .map_err(ClientError::Io)?;
                (Transport::Replay, Some(reader))
            }
            (LogMode::Read, None) => return Err(ClientError::ReadOnly),
            (mode, _) => {
                // The open of a FIFO to write waits for a reader, on the
                // thread of whoever asked for this client.
                let open = without_a_wait(
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(mode == LogMode::Write)
                        .append(mode == LogMode::Append),
                )
                .open(&file.path)?;
                // A log is a file to write as well as to read. `O_NONBLOCK`
                // is on the open file and not on the open call, so a write
                // to a FIFO that is full would give `EAGAIN` in place of
                // waiting, and the writer would stop for a reader that is a
                // moment behind. A write to a file waits for none of that.
                if !open.metadata()?.is_file() {
                    return Err(ClientError::NotAFile);
                }
                let transport = Transport::Log {
                    writer: Mutex::new(LogWriter {
                        file: std::io::BufWriter::new(open),
                        events: 0,
                    }),
                };
                (transport, None)
            }
        };

        Ok(Self {
            transport,
            receives: reader.is_some(),
            subscriptions,
            counters,
            running,
            writable,
            reader: Mutex::new(reader),
            changing: Mutex::new(()),
        })
    }
}
