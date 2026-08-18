//! Log files, written and replayed through a `Client`.
// A subscription here is a pattern, so an engine for one is necessary.
#![cfg(all(feature = "std", feature = "patterns"))]

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use lcm_bus::wire::{Decoded, FrameRef, log};
use lcm_bus::{
    BusUrl, Client, ClientError, Delivery, DeliveryHandler, Frame, LogFile, LogMode, Replay, Speed,
    Stop, Subscriptions,
};

#[derive(Default)]
struct Collector {
    frames: Mutex<Vec<Frame>>,
    stopped: Mutex<Vec<String>>,
    arrived: Condvar,
}

impl Collector {
    fn wait_for(&self, n: usize, timeout: Duration) -> Vec<Frame> {
        let deadline = Instant::now() + timeout;
        let mut frames = self.frames.lock().unwrap();
        while frames.len() < n {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, _) = self.arrived.wait_timeout(frames, remaining).unwrap();
            frames = next;
        }
        frames.clone()
    }
}

impl DeliveryHandler for Collector {
    fn on_delivery(&self, delivery: Delivery) {
        let frame = delivery.frame;
        self.frames.lock().unwrap().push(frame);
        self.arrived.notify_all();
    }
    fn on_stop(&self, cause: Stop) {
        let reason = &format!("{cause}");
        self.stopped.lock().unwrap().push(reason.to_owned());
    }
}

fn log_path(name: &str) -> String {
    let dir = std::env::temp_dir().join("lcm-logfile-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

fn subscriptions(patterns: &[&str]) -> Subscriptions {
    let mut subs = Subscriptions::new();
    for pattern in patterns {
        subs.add(pattern).unwrap();
    }
    subs
}

fn replay(path: &str, speed: Speed, start: Option<i64>) -> BusUrl {
    BusUrl::File(LogFile {
        path: path.to_owned(),
        mode: LogMode::Read,
        replay: Replay {
            speed,
            start_timestamp: start,
        },
    })
}

/// Put `events` in a log, in the LCM layout.
fn write_log(path: &str, events: &[(i64, &str, &[u8])]) {
    let mut bytes = Vec::new();
    for (number, &(timestamp, channel, payload)) in events.iter().enumerate() {
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number: number as i64,
                timestamp,
                frame: FrameRef { channel, payload },
            })
            .unwrap(),
        );
    }
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn a_log_written_by_this_client_replays_through_it() {
    let path = log_path("round-trip.lcmlog");
    let _ = std::fs::remove_file(&path);

    let writer = Client::connect(
        &format!("file://{path}?mode=w"),
        Subscriptions::new(),
        Arc::new(Collector::default()),
    )
    .expect("open for writing");
    writer.publish("/example/one", &[1, 2, 3]).unwrap();
    writer.publish("/example/two", &[4, 5]).unwrap();
    writer.close().unwrap();

    let collector = Arc::new(Collector::default());
    let reader = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .expect("open for reading");

    let frames = collector.wait_for(2, Duration::from_secs(5));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].channel, "/example/one");
    assert_eq!(frames[0].payload, vec![1, 2, 3]);
    assert_eq!(frames[1].channel, "/example/two");
    assert_eq!(frames[1].payload, vec![4, 5]);
    drop(reader);

    // And the file is the format the LCM tools read.
    let bytes = std::fs::read(&path).unwrap();
    let (first, consumed) = log::decode(&bytes).unwrap().item().unwrap();
    assert_eq!(first.number, 0, "LCM numbers the events of a log from zero");
    let (second, _) = log::decode(&bytes[consumed..]).unwrap().item().unwrap();
    assert_eq!(second.number, 1);
    assert!(second.timestamp >= first.timestamp);
}

#[test]
fn a_replay_keeps_to_the_subscriptions() {
    let path = log_path("filtered.lcmlog");
    write_log(
        &path,
        &[
            (10, "/keep/one", &[1]),
            (20, "/drop/two", &[2]),
            (30, "/keep/three", &[3]),
        ],
    );

    let collector = Arc::new(Collector::default());
    let _client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&["/keep/.*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(2, Duration::from_secs(5));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].channel, "/keep/one");
    assert_eq!(frames[1].channel, "/keep/three");
}

#[test]
fn a_replay_can_start_part_way_through() {
    let path = log_path("start.lcmlog");
    write_log(
        &path,
        &[(100, "/a", &[1]), (200, "/b", &[2]), (300, "/c", &[3])],
    );

    let collector = Arc::new(Collector::default());
    let _client = Client::open(
        replay(&path, Speed::Unthrottled, Some(200)),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(2, Duration::from_secs(5));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].channel, "/b");
    assert_eq!(frames[1].channel, "/c");
}

/// `speed` divides the time between two events, as LCM does. Below 1 a
/// replay takes longer than the log, and above 1 it takes less.
#[test]
fn speed_sets_the_rate_of_a_replay() {
    let path = log_path("speed.lcmlog");
    write_log(&path, &[(0, "/a", &[1]), (400_000, "/b", &[2])]);

    let replay_at = |speed: Speed| {
        let collector = Arc::new(Collector::default());
        let started = Instant::now();
        let _client = Client::open(
            replay(&path, speed, None),
            subscriptions(&[".*"]),
            collector.clone(),
        )
        .unwrap();
        let frames = collector.wait_for(2, Duration::from_secs(10));
        assert_eq!(frames.len(), 2, "both events, in the end");
        started.elapsed()
    };

    let slow = replay_at(Speed::Rate(0.5));
    assert!(
        slow >= Duration::from_millis(700),
        "half speed took {slow:?}"
    );
    let fast = replay_at(Speed::Rate(4.0));
    assert!(
        fast < Duration::from_millis(300),
        "four times took {fast:?}"
    );
    assert!(fast >= Duration::from_millis(80), "and not none of it");
}

#[test]
fn the_end_of_a_log_stops_the_client() {
    let path = log_path("end.lcmlog");
    write_log(&path, &[(10, "/a", &[1])]);

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    for _ in 0..500 {
        if !client.is_connected() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!client.is_connected());
    assert_eq!(collector.stopped.lock().unwrap().len(), 1);
    // A replay takes no message, before the end of its log and after it. The
    // reason does not become `Closed` until the caller closes it.
    assert!(matches!(
        client.publish("/x", &[1]),
        Err(ClientError::ReadOnly)
    ));

    client.close().unwrap();
    assert!(matches!(
        client.publish("/x", &[1]),
        Err(ClientError::Closed)
    ));
}

#[test]
fn a_replay_refuses_to_publish() {
    let path = log_path("read-only.lcmlog");
    write_log(&path, &[(10, "/a", &[1])]);

    let client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        Subscriptions::new(),
        Arc::new(Collector::default()),
    )
    .unwrap();

    // Before the reader reaches the end of the log, this is `ReadOnly`.
    let err = client.publish("/x", &[1]).unwrap_err();
    assert!(
        matches!(err, ClientError::ReadOnly | ClientError::Closed),
        "{err}"
    );
}

/// A sync word, or the end of the log, sits behind each event, so damage
/// costs the event in front of it as well as itself: nothing confirms that
/// one, and a length that reads as longer than it is would otherwise take
/// the events behind it for a payload and give them back as one. LCM refuses
/// the same event, and then stops; this reader slides to the next sync word
/// and goes on.
#[test]
fn a_replay_gets_past_a_torn_event() {
    let path = log_path("torn.lcmlog");
    let mut bytes = log::encode(log::Event {
        number: 0,
        timestamp: 10,
        frame: FrameRef {
            channel: "/a",
            payload: &[1],
        },
    })
    .unwrap();
    // Bytes that are not an event, where the next one starts.
    bytes.extend_from_slice(&[0xff; 17]);
    bytes.extend_from_slice(
        &log::encode(log::Event {
            number: 1,
            timestamp: 20,
            frame: FrameRef {
                channel: "/b",
                payload: &[2],
            },
        })
        .unwrap(),
    );
    std::fs::write(&path, bytes).unwrap();

    let collector = Arc::new(Collector::default());
    let _client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(2, Duration::from_secs(2));
    assert_eq!(frames.len(), 1, "the damage cost the event in front of it");
    assert_eq!(
        frames[0].channel, "/b",
        "and the reader found the one behind"
    );
}

/// Put an event in a log with a name this crate cannot write: a name is a
/// `&str`, and these bytes are not text.
fn event_with_a_raw_name(number: i64, timestamp: i64, name: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&log::MAGIC.to_be_bytes());
    bytes.extend_from_slice(&number.to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(&(name.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(payload);
    bytes
}

/// A name in a log is a length and then that many bytes, so a log holds a
/// name that is not text and a name with a NUL in it. C reads a name with
/// `strlen` and takes both, and this crate takes neither.
///
/// The event still has a length, and the sync word behind it agrees with
/// that length. So the event costs itself and nothing else. A reader that
/// looks for the next sync word instead finds the first one in the payload
/// of the event it just refused, and from there it reads that payload as a
/// log: events on channels no publisher used, with times a payload holds.
#[test]
fn a_name_this_crate_cannot_read_costs_its_own_event_only() {
    let mut forged = Vec::new();
    for (number, channel) in [(0, "/forged/one"), (1, "/forged/two")] {
        forged.extend_from_slice(
            &log::encode(log::Event {
                number,
                timestamp: 1_000_000 + number,
                frame: FrameRef {
                    channel,
                    payload: b"no publisher sent this",
                },
            })
            .unwrap(),
        );
    }

    for (what, name) in [("not text", &[0xffu8][..]), ("a NUL in it", &b"a\0b"[..])] {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number: 0,
                timestamp: 100,
                frame: FrameRef {
                    channel: "/real/first",
                    payload: b"first",
                },
            })
            .unwrap(),
        );
        bytes.extend_from_slice(&event_with_a_raw_name(1, 200, name, &forged));
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number: 2,
                timestamp: 300,
                frame: FrameRef {
                    channel: "/real/last",
                    payload: b"last",
                },
            })
            .unwrap(),
        );

        let path = log_path("a-name-that-is-not-text.lcmlog");
        std::fs::write(&path, bytes).unwrap();

        let collector = Arc::new(Collector::default());
        let client = Client::open(
            replay(&path, Speed::Unthrottled, None),
            subscriptions(&[".*"]),
            collector.clone(),
        )
        .unwrap();

        collector.wait_for(2, Duration::from_secs(5));
        // Give a reader that takes too much the time to show it.
        std::thread::sleep(Duration::from_millis(100));
        let frames = collector.frames.lock().unwrap().clone();
        drop(client);

        let channels: Vec<&str> = frames.iter().map(|f| f.channel.as_str()).collect();
        assert_eq!(
            channels,
            ["/real/first", "/real/last"],
            "a name with {what} costs its own event, and no other"
        );
    }
}

/// A log that stops in the middle of an event is what a recorder that was
/// killed leaves: the head of the last event is there, and the payload it
/// names is cut short. Those payload bytes are a publisher's, and a reader
/// that looks in them for the next sync word finds what the publisher put
/// there. It then gives out that payload as events.
///
/// The head of the event read, so the bytes behind it are its payload and
/// nothing else. The reader drops them, and says it dropped them.
#[test]
fn a_log_cut_in_the_middle_of_an_event_gives_out_none_of_it() {
    let mut forged = Vec::new();
    for number in 0..8i64 {
        forged.extend_from_slice(
            &log::encode(log::Event {
                number: 4242 + number,
                timestamp: 1_700_000_000_000_000 + number,
                frame: FrameRef {
                    channel: "/forged/waypoint",
                    payload: b"no publisher sent this",
                },
            })
            .unwrap(),
        );
    }

    let path = log_path("cut-in-the-middle.lcmlog");
    let writer = Client::connect(
        &format!("file://{path}?mode=w"),
        Subscriptions::new(),
        Arc::new(Collector::default()),
    )
    .unwrap();
    writer.publish("/real/one", b"first").unwrap();
    writer.publish("/real/two", b"second").unwrap();
    // The payload is what a publisher chose, and this crate does not read it.
    writer.publish("/camera/raw", &forged).unwrap();
    writer.publish("/real/three", b"last").unwrap();
    writer.close().unwrap();

    // The recorder was killed part way through the large event.
    let whole = std::fs::metadata(&path).unwrap().len();
    let cut = whole - forged.len() as u64 / 2;
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(cut)
        .unwrap();

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    collector.wait_for(2, Duration::from_secs(5));
    std::thread::sleep(Duration::from_millis(100));
    let frames = collector.frames.lock().unwrap().clone();
    let stats = client.stats();
    drop(client);

    let channels: Vec<&str> = frames.iter().map(|f| f.channel.as_str()).collect();
    assert_eq!(
        channels,
        ["/real/one", "/real/two"],
        "the events the log holds whole, and nothing out of a payload"
    );
    assert!(
        stats.discarded > 0,
        "and the reader said it dropped the tail"
    );
}

/// A length can read as longer than the log itself. The event it names never
/// arrives, however much the reader reads, and the end of the file is the
/// answer. The head of that event read, so what is behind it is the payload
/// it names: the reader drops it, and counts it, and does not look in it.
#[test]
fn a_length_longer_than_the_log_is_dropped_and_counted() {
    let mut bytes = Vec::new();
    for (number, channel) in [(0, "/real/one"), (1, "/real/two"), (2, "/real/three")] {
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number,
                timestamp: (number + 1) * 1000,
                frame: FrameRef {
                    channel,
                    payload: &[7],
                },
            })
            .unwrap(),
        );
    }
    // The payload length of the first event, from 1 to more than the log.
    let at = log::HEADER_LEN - 4;
    bytes[at..at + 4].copy_from_slice(&(1u32 << 20).to_be_bytes());

    let path = log_path("a-length-longer-than-the-log.lcmlog");
    std::fs::write(&path, bytes).unwrap();

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(200));
    let frames = collector.frames.lock().unwrap().clone();
    let stats = client.stats();
    let stopped = collector.stopped.lock().unwrap().clone();
    drop(client);

    // The length names the rest of the log as one payload, and a reader
    // cannot tell that from a recorder that was killed there. It takes the
    // safer of the two: what a payload holds is never an event.
    assert!(frames.is_empty(), "and no event out of a payload");
    assert!(stats.discarded > 0, "the reader said it dropped the tail");
    assert_eq!(stopped, ["the log ended in the middle of an event"]);
}

/// A length that reads as longer than the event is takes the events behind
/// it for a payload. Without a test of what sits behind an event, a reader
/// hands back a payload nobody wrote and loses the events it ate.
#[test]
fn a_length_that_lies_does_not_eat_the_events_behind_it() {
    let mut bytes = Vec::new();
    for (number, channel) in [(0, "/one"), (1, "/two"), (2, "/three"), (3, "/four")] {
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number,
                timestamp: (number + 1) * 1000,
                frame: FrameRef {
                    channel,
                    payload: &[7; 8],
                },
            })
            .unwrap(),
        );
    }
    // The payload length of the second event, from 8 to 60.
    let second = log::HEADER_LEN + 4 + 8;
    let at = second + log::HEADER_LEN - 4;
    bytes[at..at + 4].copy_from_slice(&60u32.to_be_bytes());

    let path = log_path("lying-length.lcmlog");
    std::fs::write(&path, bytes).unwrap();

    let collector = Arc::new(Collector::default());
    let _client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(3, Duration::from_secs(2));
    let channels: Vec<&str> = frames.iter().map(|f| f.channel.as_str()).collect();
    assert_eq!(
        channels,
        ["/one", "/three", "/four"],
        "the event that lied is gone, and the ones behind it are not"
    );
    assert!(
        frames.iter().all(|f| f.payload == [7; 8]),
        "and no payload was made up"
    );
}

/// LCM numbers each event, and a player seeks by a binary division of the
/// timestamps.
/// Two threads that publish together must not break one of the two.
#[test]
fn two_publishers_keep_a_log_in_order() {
    let path = log_path("concurrent.lcmlog");
    let _ = std::fs::remove_file(&path);

    let client = Arc::new(
        Client::connect(
            &format!("file://{path}?mode=w"),
            Subscriptions::new(),
            Arc::new(Collector::default()),
        )
        .expect("open to write"),
    );

    std::thread::scope(|s| {
        for thread in 0..8u8 {
            let client = client.clone();
            s.spawn(move || {
                for _ in 0..200 {
                    client.publish("/c", &[thread]).unwrap();
                }
            });
        }
    });
    client.close().unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let (mut at, mut count, mut previous) = (0usize, 0i64, i64::MIN);
    while let Ok(Decoded::Item(event, used)) = log::decode(&bytes[at..]) {
        assert_eq!(event.number, count, "LCM numbers events 0, 1, 2 and on");
        assert!(event.timestamp >= previous, "and the times do not go back");
        previous = event.timestamp;
        count += 1;
        at += used;
    }
    assert_eq!(count, 1_600, "and every publish reached the log");
}

/// Append mode keeps what is there and writes after it.
/// LCM numbers from zero in this mode too.
#[test]
fn append_mode_writes_after_what_is_there() {
    let path = log_path("append.lcmlog");
    write_log(&path, &[(10, "/old", &[1])]);

    let client = Client::connect(
        &format!("file://{path}?mode=a"),
        Subscriptions::new(),
        Arc::new(Collector::default()),
    )
    .expect("open to append");
    client.publish("/new", &[2]).unwrap();
    client.close().unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let (first, used) = log::decode(&bytes).unwrap().item().unwrap();
    assert_eq!(first.frame.channel, "/old", "the first event is still here");
    assert_eq!(first.number, 0);

    let (second, _) = log::decode(&bytes[used..]).unwrap().item().unwrap();
    assert_eq!(second.frame.channel, "/new");
    assert_eq!(second.number, 0, "LCM numbers from zero in append mode");
}

/// The walk to a late `start_timestamp` reads the head of each event before
/// it, and no more of one. `Stats::received` counts the events this client
/// decoded, so a low count shows that the walk decoded none of them. The
/// events delivered are the correct ones.
#[test]
fn a_late_start_decodes_none_of_the_events_it_steps_over() {
    const EVENTS: i64 = 30_000;
    const START: i64 = 25_000_000;

    let path = log_path("seek.lcmlog");
    let payload = [7u8; 32];
    let mut bytes = Vec::new();
    for number in 0..EVENTS {
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number,
                timestamp: number * 1_000,
                frame: FrameRef {
                    channel: "/seek",
                    payload: &payload,
                },
            })
            .unwrap(),
        );
    }
    assert!(
        bytes.len() > 1_000_000,
        "long enough to make a seek worth it"
    );
    std::fs::write(&path, bytes).unwrap();

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, Some(START)),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let want = (EVENTS - START / 1_000) as usize;
    let frames = collector.wait_for(want, Duration::from_secs(10));
    assert_eq!(frames.len(), want, "every event at or after the start");
    assert_eq!(frames[0].channel, "/seek");

    let stats = client.stats();
    assert_eq!(stats.delivered as usize, want);
    assert!(
        stats.received < EVENTS as u64 / 2,
        "the walk stepped over most of the log, and decoded {} of {EVENTS}",
        stats.received
    );
}

/// The URL parser turns `speed=0` into `Speed::Unthrottled`, but a caller
/// building a `Replay` by hand can write `Rate(0.0)`. A division by it
/// gives a wait with no end.
#[test]
fn a_rate_of_zero_does_not_stop_a_replay() {
    let path = log_path("zero-rate.lcmlog");
    write_log(&path, &[(0, "/a", &[1]), (5_000_000, "/b", &[2])]);

    let collector = Arc::new(Collector::default());
    let started = Instant::now();
    let _client = Client::open(
        BusUrl::File(LogFile {
            path: path.clone(),
            mode: LogMode::Read,
            replay: Replay {
                speed: Speed::Rate(0.0),
                start_timestamp: None,
            },
        }),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(2, Duration::from_secs(3));
    assert_eq!(frames.len(), 2, "and not one, five seconds apart");
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// A start time is a point to start at. It does not change which events the
/// log holds. So a replay from a start time gives what a replay of all of the
/// log gives, less the events before that time. A log with a length that lies
/// is where the two can come apart: the walk to the start time goes by the
/// length, and a read from the start of the log does not.
#[test]
fn a_start_time_gives_what_a_full_replay_gives() {
    let mut bytes = Vec::new();
    for (number, channel) in [(0, "/one"), (1, "/two"), (2, "/three"), (3, "/four")] {
        bytes.extend_from_slice(
            &log::encode(log::Event {
                number,
                timestamp: (number + 1) * 1000,
                frame: FrameRef {
                    channel,
                    payload: &[7; 8],
                },
            })
            .unwrap(),
        );
    }
    // The payload length of the second event, from 8 to 60: over the third
    // event, and into the fourth.
    let at = (log::HEADER_LEN + 4 + 8) + log::HEADER_LEN - 4;
    bytes[at..at + 4].copy_from_slice(&60u32.to_be_bytes());

    let path = log_path("lying-length-with-a-start.lcmlog");
    std::fs::write(&path, bytes).unwrap();

    let channels_from = |start: Option<i64>, want: usize| {
        let collector = Arc::new(Collector::default());
        let client = Client::open(
            replay(&path, Speed::Unthrottled, start),
            subscriptions(&[".*"]),
            collector.clone(),
        )
        .unwrap();
        collector.wait_for(want, Duration::from_secs(5));
        // Give a reader that takes too much the time to show it.
        std::thread::sleep(Duration::from_millis(100));
        let names: Vec<String> = collector
            .frames
            .lock()
            .unwrap()
            .iter()
            .map(|f| f.channel.clone())
            .collect();
        drop(client);
        names
    };

    // The second event lied, so a replay of all of the log drops it and
    // finds the two behind it.
    assert_eq!(channels_from(None, 3), ["/one", "/three", "/four"]);
    // The third event is at 3000, so a start there drops the first two.
    assert_eq!(channels_from(Some(3_000), 2), ["/three", "/four"]);
}

/// A payload is bytes a publisher selects, and a log holds them as they
/// came. So a payload can hold what looks like a piece of a log: sync words,
/// heads, and lengths that agree with each other.
///
/// A read from the start of the log never sees these, because it steps from
/// each event to the one after it. A start at a point must stay on that same
/// chain. If it does not, it reads a payload as a log, and gives out events
/// that no publisher sent.
#[test]
fn a_payload_that_looks_like_a_log_is_not_read_as_one() {
    const START: i64 = 2_000;

    // Events on a channel no publisher used, in the payload of one a
    // publisher sent. Each event ends where the next starts, so each agrees
    // with what follows it, and their times increase as a log's do.
    let mut forged = Vec::new();
    for number in 0..4_000i64 {
        forged.extend_from_slice(
            &log::encode(log::Event {
                number,
                timestamp: number,
                frame: FrameRef {
                    channel: "/forged",
                    payload: b"nobody sent this",
                },
            })
            .unwrap(),
        );
    }

    let path = log_path("payload-that-looks-like-a-log.lcmlog");
    write_log(
        &path,
        &[
            (1_000, "/real", &[1]),
            (1_500, "/real", &forged),
            (99_000, "/real", &[2]),
        ],
    );

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, Some(START)),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    // One event of this log is at or after the start: the last one.
    collector.wait_for(1, Duration::from_secs(10));
    // Give a wrong reader the time to give out more than that.
    std::thread::sleep(Duration::from_millis(200));
    let frames = collector.frames.lock().unwrap().clone();
    drop(client);

    let forged: Vec<_> = frames.iter().filter(|f| f.channel == "/forged").collect();
    assert!(
        forged.is_empty(),
        "the replay gave out {} events from inside a payload",
        forged.len()
    );
    assert_eq!(
        frames.len(),
        1,
        "and the one real event at or after {START}"
    );
    assert_eq!(frames[0].payload, vec![2]);
}

/// The times of a log increase, because a recorder writes each event as it
/// comes. A log made another way does not have to. The start time gives the
/// same events either way: the ones at or after it.
#[test]
fn a_start_time_holds_when_the_times_do_not_increase() {
    let path = log_path("times-out-of-order.lcmlog");
    write_log(
        &path,
        &[
            (1_000, "/early", &[1]),
            (9_000, "/late", &[2]),
            (2_000, "/early", &[3]),
            (8_000, "/late", &[4]),
            (3_000, "/early", &[5]),
        ],
    );

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, Some(5_000)),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    collector.wait_for(2, Duration::from_secs(5));
    // Give a reader that takes too much the time to show it.
    std::thread::sleep(Duration::from_millis(100));
    let frames = collector.frames.lock().unwrap().clone();
    drop(client);

    let channels: Vec<&str> = frames.iter().map(|f| f.channel.as_str()).collect();
    assert_eq!(channels, ["/late", "/late"], "the events at or after 5000");
    assert_eq!(frames[0].payload, vec![2]);
    assert_eq!(frames[1].payload, vec![4]);
}

/// A read of a FIFO waits for a writer, and a read of one that is open waits
/// for bytes. Nothing in this crate takes such a read back: `close` waits for
/// the reader thread, and that thread waits in the read. So `close`, and the
/// `Drop` that calls it, never give up.
#[cfg(unix)]
#[test]
fn a_replay_of_a_thing_that_is_not_a_file_is_refused() {
    let path = log_path("a-fifo.lcmlog");
    let _ = std::fs::remove_file(&path);
    let made = std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .expect("run mkfifo");
    assert!(made.success(), "make a FIFO to replay");

    // No reader and no writer are on this FIFO, so an open that waits waits
    // for good. Both of these have to give an answer.
    let began = Instant::now();
    let opened = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        |_: Delivery| {},
    );
    assert!(
        matches!(opened, Err(ClientError::NotAFile)),
        "a FIFO is not a log to replay"
    );

    // A log writer keeps what it holds, and only has to answer.
    let writing = Client::publisher(&format!("file://{path}?mode=w"));
    assert!(writing.is_err(), "and a FIFO takes no log either");
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "and neither open waited for the other end of the FIFO"
    );

    let _ = std::fs::remove_file(&path);

    // A device is not a file either, and this one opens where a FIFO does
    // not: the answer has to come from the handle and not from the path.
    let opened = Client::open(
        replay("/dev/zero", Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        |_: Delivery| {},
    );
    assert!(
        matches!(opened, Err(ClientError::NotAFile)),
        "a character device is not a log to replay"
    );
}

/// A panic in `on_stop` meets the unwind of a panic in `on_delivery`, which
/// is a panic in a destructor while a panic unwinds: that ends the process,
/// and every other bus in it. One handler reaches it with one bug — the
/// first panic poisons a lock, and the report of it is the `lock().unwrap()`
/// everyone writes.
///
/// Without the guard this does not fail. It ends the test program with
/// `SIGABRT` and takes the other tests with it.
#[test]
fn a_handler_that_panics_twice_does_not_end_the_process() {
    struct TwoPanics {
        held: Mutex<u32>,
    }

    impl DeliveryHandler for TwoPanics {
        fn on_delivery(&self, delivery: Delivery) {
            let mut count = self.held.lock().unwrap();
            *count += 1;
            // A length a payload from a bus is free to have.
            let _ = delivery.frame.payload[0..4];
        }
        fn on_stop(&self, _: Stop) {
            // The lock the panic above poisoned.
            let _ = *self.held.lock().unwrap();
        }
    }

    let path = log_path("two-panics.lcmlog");
    write_log(&path, &[(10, "/short", &[1, 2])]);

    let client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        Arc::new(TwoPanics {
            held: Mutex::new(0),
        }),
    )
    .unwrap();

    for _ in 0..500 {
        if !client.is_connected() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !client.is_connected(),
        "the handler took the reader with it"
    );
    client.close().unwrap();
}

/// A log is a file to write as well as to read.
///
/// The open of one does not wait, which for a FIFO needs `O_NONBLOCK`. That
/// flag is on the open file and not on the open call, so it stays for every
/// write: a write to a FIFO whose reader is a moment behind would give
/// `EAGAIN` in place of waiting, and the writer stops for good on a reader
/// that was about to take it. A file waits for none of that, so the answer
/// is to write to files.
#[cfg(unix)]
#[test]
fn a_log_writer_takes_a_file_and_not_a_device() {
    assert!(matches!(
        Client::publisher("file:///dev/full?mode=w"),
        Err(ClientError::NotAFile)
    ));
    assert!(matches!(
        Client::publisher("file:///dev/null?mode=w"),
        Err(ClientError::NotAFile)
    ));

    let path = log_path("an-ordinary-log.lcmlog");
    let _ = std::fs::remove_file(&path);
    let writer = Client::publisher(&format!("file://{path}?mode=w")).unwrap();
    writer.publish("/real", &[1]).unwrap();
    assert!(writer.is_connected());
    writer.close().unwrap();
}

/// `close` waits for the reader thread, so that a caller can take down what
/// its handler reaches once `close` gives back. A handler that closes its
/// own client cannot wait for the thread it runs on, and it must not take
/// that wait away from the `close` behind it either: one that finds nothing
/// to wait for gives back while the handler is still in the middle.
#[test]
fn a_close_waits_for_a_handler_that_closed_the_client_itself() {
    use std::sync::Weak;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct SelfCloser {
        client: Mutex<Option<Weak<Client>>>,
        closed_itself: Arc<AtomicBool>,
        left: Arc<AtomicBool>,
    }

    impl DeliveryHandler for SelfCloser {
        fn on_delivery(&self, _: Delivery) {
            let held = self.client.lock().unwrap().clone();
            if let Some(client) = held.and_then(|weak| weak.upgrade()) {
                let _ = client.close();
            }
            self.closed_itself.store(true, Ordering::SeqCst);
            // Long enough for the close behind this one to give back early.
            std::thread::sleep(Duration::from_millis(500));
            self.left.store(true, Ordering::SeqCst);
        }
    }

    let path = log_path("self-close-then-close.lcmlog");
    write_log(&path, &[(1000, "/a", &[1])]);

    let closed_itself = Arc::new(AtomicBool::new(false));
    let left = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(SelfCloser {
        client: Mutex::new(None),
        closed_itself: closed_itself.clone(),
        left: left.clone(),
    });
    let client = Arc::new(
        Client::open(
            replay(&path, Speed::Unthrottled, None),
            subscriptions(&[".*"]),
            handler.clone(),
        )
        .unwrap(),
    );
    *handler.client.lock().unwrap() = Some(Arc::downgrade(&client));

    let deadline = Instant::now() + Duration::from_secs(5);
    while !closed_itself.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(closed_itself.load(Ordering::SeqCst), "the handler ran");

    client.close().unwrap();
    assert!(
        left.load(Ordering::SeqCst),
        "close gave back while the handler was still in the middle"
    );
}

/// A replay says so before a publish fails, not only after.
#[test]
fn a_replay_says_it_takes_no_messages() {
    let path = log_path("cannot-publish.lcmlog");
    write_log(&path, &[(10, "/a", &[1])]);

    let replaying = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        |_: Delivery| {},
    )
    .unwrap();
    assert!(!replaying.can_publish());
    assert!(matches!(
        replaying.publish("/a", &[1]),
        Err(ClientError::ReadOnly)
    ));

    let writing = Client::open(
        BusUrl::File(LogFile {
            path: log_path("can-publish.lcmlog"),
            mode: LogMode::Write,
            replay: Replay::default(),
        }),
        Subscriptions::new(),
        |_: Delivery| {},
    )
    .unwrap();
    assert!(writing.can_publish());
}

/// A log of a bus holds the time of the event and not the time of the write.
/// Without that, a log read and written again comes out re-timed, and its
/// replay no longer holds to what happened.
#[test]
fn a_log_keeps_its_times_through_a_program() {
    let source = log_path("relog-in.lcmlog");
    write_log(&source, &[(1_000_000, "/a", &[1]), (1_400_000, "/b", &[2])]);

    let copy = log_path("relog-out.lcmlog");
    let seen = Arc::new(Mutex::new(Vec::new()));

    {
        let writer = Client::open(
            BusUrl::File(LogFile {
                path: copy.clone(),
                mode: LogMode::Write,
                replay: Replay::default(),
            }),
            Subscriptions::new(),
            |_: Delivery| {},
        )
        .unwrap();

        let recorded = seen.clone();
        let _replay = Client::open(
            replay(&source, Speed::Unthrottled, None),
            subscriptions(&[".*"]),
            move |d: Delivery| {
                recorded.lock().unwrap().push(d.timestamp);
                writer
                    .publish_at(&d.frame.channel, &d.frame.payload, d.timestamp)
                    .unwrap();
            },
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline && seen.lock().unwrap().len() < 2 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    assert_eq!(*seen.lock().unwrap(), vec![1_000_000, 1_400_000]);

    let bytes = std::fs::read(&copy).unwrap();
    let mut at = 0;
    let mut times = Vec::new();
    while let Decoded::Item(event, used) = log::decode(&bytes[at..]).unwrap() {
        times.push((event.frame.channel.to_owned(), event.timestamp));
        at += used;
    }
    assert_eq!(
        times,
        [("/a".to_owned(), 1_000_000), ("/b".to_owned(), 1_400_000)],
        "the copy holds the times of the original"
    );
}

/// `on_delivery` is the caller's code on this crate's thread, and it can panic.
/// Without a report on the way down, the reader goes and every means this
/// crate gives of noticing says all is well.
#[test]
fn a_handler_that_panics_stops_the_client_and_says_so() {
    struct Panicker {
        cause: Mutex<Option<String>>,
    }

    impl DeliveryHandler for Panicker {
        fn on_delivery(&self, delivery: Delivery) {
            panic!("a handler fault on {}", delivery.frame.channel);
        }

        fn on_stop(&self, cause: Stop) {
            *self.cause.lock().unwrap() = Some(format!("{cause}"));
        }
    }

    let path = log_path("panicking-handler.lcmlog");
    write_log(&path, &[(10, "/a", &[1])]);

    let handler = Arc::new(Panicker {
        cause: Mutex::new(None),
    });
    let _client = Client::open(
        replay(&path, Speed::Unthrottled, None),
        subscriptions(&[".*"]),
        handler.clone(),
    )
    .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && handler.cause.lock().unwrap().is_none() {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        handler.cause.lock().unwrap().as_deref(),
        Some("the handler panicked")
    );
    assert!(!_client.is_connected());
}

/// A payload is bytes this crate does not read, so a payload holds the sync
/// word. A reader that takes the first of those can stop in one, read a time
/// out of it, and go on from there. A time that reads as a low one takes it
/// beyond each correct event, and the replay then delivers nothing at all and
/// says nothing about it.
#[test]
fn a_sync_word_inside_a_payload_does_not_lose_the_seek() {
    // A full head that is not one: the sync word, a number, a low time, and
    // two lengths that go through each test a correct one goes through.
    let mut decoy = vec![0u8; 256 * 1024];
    for chunk in decoy.chunks_mut(64) {
        chunk[..4].copy_from_slice(&0xEDA1_DA01u32.to_be_bytes());
        chunk[4..12].copy_from_slice(&7i64.to_be_bytes());
        chunk[12..20].copy_from_slice(&1i64.to_be_bytes());
        chunk[20..24].copy_from_slice(&4u32.to_be_bytes());
        chunk[24..28].copy_from_slice(&8u32.to_be_bytes());
    }

    let path = log_path("sync-word-in-a-payload.lcmlog");
    write_log(
        &path,
        &[
            (1_000_000, "/a", &[1]),
            (2_000_000, "/b", &[2]),
            (3_000_000, "/c", &decoy),
        ],
    );

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        replay(&path, Speed::Unthrottled, Some(1_500_000)),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(2, Duration::from_secs(5));
    let channels: Vec<&str> = frames.iter().map(|f| f.channel.as_str()).collect();
    assert_eq!(channels, ["/b", "/c"], "the events at or after the start");
    assert_eq!(
        client.stats().discarded,
        0,
        "and the reader took no event that is not one"
    );
}

/// An LCM message is often an image or a point cloud, so an event is often
/// larger than each buffer a reader keeps. The walk to the start reads the
/// heads and steps over the payloads. The size of a payload thus costs it
/// nothing, and no event is too large to step over.
#[test]
fn a_seek_steps_over_the_payloads_it_passes() {
    let payload = vec![7u8; 96 * 1024];
    let events: Vec<(i64, &str, &[u8])> = (1..=40)
        .map(|i| (i as i64 * 1000, "/big", &payload[..]))
        .collect();

    let path = log_path("large-events.lcmlog");
    write_log(&path, &events);

    let collector = Arc::new(Collector::default());
    let client = Client::open(
        // The last quarter of the log.
        replay(&path, Speed::Unthrottled, Some(30_000)),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .unwrap();

    let frames = collector.wait_for(11, Duration::from_secs(10));
    assert_eq!(frames.len(), 11, "the events at or after the start");
    assert!(
        client.stats().received < 20,
        "and it read {} of 40 events to find them",
        client.stats().received
    );
}

/// A handler that closes its own client is a supported thing to write, and
/// so is closing from elsewhere. The two at the same time must not have
/// each waiting on the other: `close` from elsewhere waits for the reader
/// thread, and the handler on that thread wants what the waiter holds.
#[test]
fn two_closes_at_once_do_not_wait_on_each_other() {
    use std::sync::Weak;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Closer {
        client: Mutex<Option<Weak<Client>>>,
        inside: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    impl DeliveryHandler for Closer {
        fn on_delivery(&self, _: Delivery) {
            self.inside.store(true, Ordering::SeqCst);
            // Sufficient for the other close to get to its wait.
            std::thread::sleep(Duration::from_millis(200));
            let held = self.client.lock().unwrap().clone();
            if let Some(client) = held.and_then(|weak| weak.upgrade()) {
                let _ = client.close();
            }
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    let path = log_path("two-closes.lcmlog");
    write_log(&path, &[(1000, "/a", &[1])]);

    let inside = Arc::new(AtomicBool::new(false));
    let closed = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(Closer {
        client: Mutex::new(None),
        inside: inside.clone(),
        closed: closed.clone(),
    });
    let client = Arc::new(
        Client::open(
            replay(&path, Speed::Unthrottled, None),
            subscriptions(&[".*"]),
            handler.clone(),
        )
        .unwrap(),
    );
    *handler.client.lock().unwrap() = Some(Arc::downgrade(&client));

    while !inside.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(5));
    }
    let outer = Arc::new(AtomicBool::new(false));
    std::thread::spawn({
        let client = client.clone();
        let outer = outer.clone();
        move || {
            let _ = client.close();
            outer.store(true, Ordering::SeqCst);
        }
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline
        && !(closed.load(Ordering::SeqCst) && outer.load(Ordering::SeqCst))
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        closed.load(Ordering::SeqCst),
        "the handler's close came back"
    );
    assert!(outer.load(Ordering::SeqCst), "and so did the other one");
}
