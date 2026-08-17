//! Sockets.
//! Multicast is unavailable in many containers, and a test that cannot
//! connect there stops without an error.
// A subscription here is a pattern, so an engine for one is necessary.
#![cfg(all(feature = "std", feature = "patterns"))]

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use lcm_bus::{Client, Delivery, DeliveryHandler, Frame, Stop, Subscriptions};

#[derive(Default)]
struct Collector {
    frames: Mutex<Vec<Frame>>,
    arrived: Condvar,
}

impl Collector {
    /// At the timeout, this gives the frames that came.
    fn wait_for(&self, n: usize, timeout: Duration) -> Vec<Frame> {
        let deadline = std::time::Instant::now() + timeout;
        let mut frames = self.frames.lock().unwrap();
        while frames.len() < n {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
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
}

/// One port for each test, because these tests are parallel on one group.
fn multicast_url(port: u16) -> String {
    format!("udpm://239.255.76.99:{port}?ttl=0")
}

fn subscriptions(patterns: &[&str]) -> Subscriptions {
    let mut subs = Subscriptions::new();
    for pattern in patterns {
        subs.add(pattern).unwrap();
    }
    subs
}

/// A container with no multicast skips the udpm tests, which then go green,
/// and the full socket path stays untested with nothing to say so. CI sets
/// `LCM_REQUIRE_MULTICAST` and gets a failure.
fn require_multicast(reason: &str) {
    assert!(
        std::env::var_os("LCM_REQUIRE_MULTICAST").is_none(),
        "multicast is unavailable and LCM_REQUIRE_MULTICAST is set: {reason}"
    );
    eprintln!("SKIPPED: multicast unavailable here ({reason})");
}

fn try_connect(
    port: u16,
    patterns: &[&str],
    collector: Arc<dyn DeliveryHandler>,
) -> Option<Client> {
    match Client::connect(&multicast_url(port), subscriptions(patterns), collector) {
        Ok(client) => Some(client),
        Err(e) => {
            require_multicast(&format!("{e}"));
            None
        }
    }
}

#[test]
fn a_message_published_to_multicast_comes_back() {
    let collector = Arc::new(Collector::default());
    let Some(client) = try_connect(17_671, &[".*"], collector.clone()) else {
        return;
    };

    client.publish("/example/one", &[1, 2, 3, 4]).unwrap();

    let frames = collector.wait_for(1, Duration::from_secs(2));
    assert_eq!(frames.len(), 1, "multicast loopback must hear ourselves");
    assert_eq!(frames[0].channel, "/example/one");
    assert_eq!(frames[0].payload, vec![1, 2, 3, 4]);
}

/// More than a hundred datagrams, so UDP loss is likely.
/// The subject is the reassembler, so the test publishes again.
/// Ten empty tries is not loss.
#[test]
fn a_fragmented_message_arrives_whole() {
    let collector = Arc::new(Collector::default());
    let Some(client) = try_connect(17_672, &[".*"], collector.clone()) else {
        return;
    };

    // More than one datagram on all platforms.
    let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();

    let mut frames = Vec::new();
    for attempt in 0..10 {
        client.publish("/example/big", &payload).unwrap();
        frames = collector.wait_for(1, Duration::from_secs(2));
        if !frames.is_empty() {
            break;
        }
        eprintln!("attempt {attempt}: fragments lost, publishing again");
    }

    assert!(
        !frames.is_empty(),
        "ten attempts and not one full message: that is not UDP loss"
    );
    assert_eq!(frames[0].channel, "/example/big");
    assert_eq!(frames[0].payload.len(), payload.len());
    assert_eq!(frames[0].payload, payload, "and byte-identical");
}

/// LCM leaves its send socket unbound, so a publisher's source port is its
/// own and not the bus port.
/// The key holds that port, and tells two senders on one host apart.
/// These two clients number their messages from zero.
#[test]
fn two_clients_on_one_host_send_from_different_ports() {
    use lcm_bus::wire::udpm;
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

    const PORT: u16 = 17_679;
    let group = Ipv4Addr::new(239, 255, 76, 99);

    // One more reader on the bus, to see where each datagram came from.
    let observer = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
    observer.set_reuse_address(true).unwrap();
    #[cfg(unix)]
    observer.set_reuse_port(true).unwrap();
    if observer
        .bind(&SocketAddrV4::new(group, PORT).into())
        .and_then(|()| observer.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED))
        .is_err()
    {
        require_multicast("the observer cannot join the group");
        return;
    }
    observer
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let observer: UdpSocket = observer.into();

    let Some(first) = try_connect(PORT, &[], Arc::new(Collector::default())) else {
        return;
    };
    let Some(second) = try_connect(PORT, &[], Arc::new(Collector::default())) else {
        return;
    };
    first.publish("/port/first", &[1]).unwrap();
    second.publish("/port/second", &[2]).unwrap();

    let mut ports = std::collections::HashMap::new();
    let mut buffer = vec![0u8; 65_536];
    for _ in 0..16 {
        let Ok((n, from)) = observer.recv_from(&mut buffer) else {
            break;
        };
        if let Ok(udpm::Datagram::Whole { frame, .. }) = udpm::decode(&buffer[..n]) {
            ports.insert(frame.channel.to_owned(), from.port());
        }
        if ports.len() == 2 {
            break;
        }
    }

    let (Some(&a), Some(&b)) = (ports.get("/port/first"), ports.get("/port/second")) else {
        require_multicast(&format!("the observer heard {ports:?}"));
        return;
    };
    assert_ne!(a, PORT, "LCM does not publish from the bus port");
    assert_ne!(b, PORT);
    assert_ne!(
        a, b,
        "and two clients on one host do not share a source port"
    );
}

#[test]
fn subscriptions_filter_what_is_delivered() {
    let collector = Arc::new(Collector::default());
    let Some(client) = try_connect(17_674, &["/keep/.*"], collector.clone()) else {
        return;
    };

    client.publish("/drop/one", &[1]).unwrap();
    client.publish("/keep/two", &[2]).unwrap();

    let frames = collector.wait_for(1, Duration::from_secs(2));
    assert_eq!(frames.len(), 1, "only the subscribed channel");
    assert_eq!(frames[0].channel, "/keep/two");
}

#[test]
fn a_later_subscription_takes_effect() {
    let collector = Arc::new(Collector::default());
    let Some(client) = try_connect(17_676, &["/first/.*"], collector.clone()) else {
        return;
    };

    client.subscribe("/second/.*").unwrap();
    client.publish("/second/x", &[1]).unwrap();
    assert_eq!(collector.wait_for(1, Duration::from_secs(2)).len(), 1);

    assert!(client.unsubscribe("/second/.*").unwrap());
    assert!(!client.unsubscribe("/second/.*").unwrap(), "gone already");

    client.publish("/second/x", &[2]).unwrap();
    client.publish("/first/x", &[3]).unwrap();

    let frames = collector.wait_for(2, Duration::from_secs(2));
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[1].channel, "/first/x", "and not /second/x");
}

#[test]
fn publishing_after_close_is_refused() {
    let collector = Arc::new(Collector::default());
    let Some(client) = try_connect(17_675, &[".*"], collector) else {
        return;
    };

    client.close().unwrap();
    assert!(!client.is_connected());
    assert!(
        client.publish("/test/x", &[1]).is_err(),
        "a closed client must say so and not pretend"
    );
}

#[test]
fn the_relay_handshake_and_framing_are_what_a_server_expects() {
    use lcm_bus::wire::{Frame as WireFrame, tcpq};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");

        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        assert_eq!(&opening[..4], &tcpq::MAGIC_CLIENT.to_be_bytes());
        assert_eq!(&opening[4..], &tcpq::PROTOCOL_VERSION.to_be_bytes());

        // Reply as a relay does.
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();

        let mut subscribe = [0u8; 8];
        stream.read_exact(&mut subscribe).expect("subscribe");
        assert_eq!(&subscribe[..4], &tcpq::MESSAGE_TYPE_SUBSCRIBE.to_be_bytes());
        let pattern_len = u32::from_be_bytes(subscribe[4..].try_into().unwrap()) as usize;
        let mut pattern = vec![0u8; pattern_len];
        stream.read_exact(&mut pattern).unwrap();
        assert_eq!(pattern, b"/example/.*");

        // Push a message, as a relay does.
        stream
            .write_all(
                &tcpq::publish(
                    WireFrame {
                        channel: "/example/one".to_owned(),
                        payload: vec![9, 8, 7],
                    }
                    .view(),
                )
                .unwrap(),
            )
            .unwrap();

        let mut unsubscribe = [0u8; 8];
        stream.read_exact(&mut unsubscribe).expect("unsubscribe");
        assert_eq!(
            &unsubscribe[..4],
            &tcpq::MESSAGE_TYPE_UNSUBSCRIBE.to_be_bytes()
        );
    });

    let collector = Arc::new(Collector::default());
    let client = Client::connect(
        &format!("tcpq://{address}"),
        subscriptions(&["/example/.*"]),
        collector.clone(),
    )
    .expect("connect to the stand-in relay");

    let frames = collector.wait_for(1, Duration::from_secs(2));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].channel, "/example/one");
    assert_eq!(frames[0].payload, vec![9, 8, 7]);

    assert!(client.unsubscribe("/example/.*").unwrap());
    server.join().unwrap();
    client.close().unwrap();
}

#[derive(Default)]
struct Disconnects(Mutex<Vec<String>>);

impl DeliveryHandler for Disconnects {
    fn on_delivery(&self, _: Delivery) {}
    fn on_stop(&self, cause: Stop) {
        let reason = &format!("{cause}");
        self.0.lock().unwrap().push(reason.to_owned());
    }
}

/// A relay that goes away stops the client for good: `is_connected` says so,
/// a `publish` after it gives `Closed`, and the handler hears one cause.
#[test]
fn a_relay_that_closes_stops_the_client() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
    });

    let seen = Arc::new(Disconnects::default());
    let client = Client::connect(
        &format!("tcpq://{address}"),
        Subscriptions::new(),
        seen.clone(),
    )
    .expect("connect");
    server.join().unwrap();

    for _ in 0..200 {
        if !client.is_connected() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(!client.is_connected(), "the relay closed the connection");
    assert!(
        matches!(
            client.publish("/x", &[1]),
            Err(lcm_bus::ClientError::Closed)
        ),
        "and a publish says so, and does not write to a dead socket"
    );
    assert_eq!(
        seen.0.lock().unwrap().len(),
        1,
        "one stop, and one report of it"
    );

    // `close` must not report a connection that stopped by itself.
    client.close().unwrap();
    assert_eq!(seen.0.lock().unwrap().len(), 1);
}

#[test]
fn a_server_that_is_not_a_relay_is_rejected() {
    use std::io::Write;
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            // A reply that is not from an LCM relay.
            let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n");
        }
    });

    let err = match Client::connect(
        &format!("tcpq://{address}"),
        Subscriptions::new(),
        Arc::new(Collector::default()),
    ) {
        Err(e) => e,
        Ok(_) => panic!("a web server is not an LCM relay"),
    };

    assert!(err.to_string().contains("handshake"), "{err}");
    server.join().unwrap();
}

/// One bad sender must not stop the receiver, and `Client::stats` must say so.
#[test]
fn a_bad_datagram_is_counted_and_does_not_stop_the_bus() {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

    const PORT: u16 = 17_680;
    let group = Ipv4Addr::new(239, 255, 76, 99);

    let collector = Arc::new(Collector::default());
    let Some(client) = try_connect(PORT, &[".*"], collector.clone()) else {
        return;
    };

    let junk = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
    junk.set_multicast_loop_v4(true).unwrap();
    junk.set_multicast_ttl_v4(0).unwrap();
    let junk: UdpSocket = junk.into();
    let destination = SocketAddrV4::new(group, PORT);
    for _ in 0..5 {
        junk.send_to(b"not an LCM datagram", destination).unwrap();
    }

    client.publish("/after/junk", &[1]).unwrap();
    let frames = collector.wait_for(1, Duration::from_secs(2));
    assert_eq!(frames.len(), 1, "the receiver kept going");
    assert_eq!(frames[0].channel, "/after/junk");

    let stats = client.stats();
    assert!(stats.received >= 2, "{stats:?}");
    assert_eq!(stats.delivered, 1);
    assert!(
        stats.discarded >= 1,
        "the bad datagrams are counted: {stats:?}"
    );
    assert_eq!(stats.in_flight, 0, "and nothing is half-assembled");
}

/// A relay that stops to read blocks a `publish` when the socket is full.
/// `close` must not wait behind that write.
#[test]
fn close_ends_a_publish_that_the_relay_stopped_reading() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let (release, held) = mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        let _ = held.recv();
    });

    let client = Arc::new(
        Client::connect(
            &format!("tcpq://{address}"),
            Subscriptions::new(),
            Arc::new(Disconnects::default()),
        )
        .expect("connect"),
    );

    let publisher = std::thread::spawn({
        let client = Arc::clone(&client);
        move || while client.publish("/bus/big", &[0u8; 1 << 16]).is_ok() {}
    });
    std::thread::sleep(Duration::from_millis(200));

    let (closed, returned) = mpsc::channel();
    std::thread::spawn({
        let client = Arc::clone(&client);
        move || {
            client.close().unwrap();
            let _ = closed.send(());
        }
    });
    let in_time = returned.recv_timeout(Duration::from_secs(5)).is_ok();

    let _ = release.send(());
    publisher.join().unwrap();
    server.join().unwrap();
    assert!(in_time, "close waited for a write that cannot finish");
}

/// A caller that wants the `io::Error` gets it, and does not read the text.
#[test]
fn a_connection_error_keeps_the_io_error() {
    use std::error::Error;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();
    drop(listener);

    let refused = lcm_bus::Client::connect(
        &format!("tcpq://{address}"),
        Subscriptions::new(),
        Arc::new(Disconnects::default()),
    )
    .expect_err("nothing listens on that port");

    let cause = refused.source().expect("the cause is kept");
    let io = cause
        .downcast_ref::<std::io::Error>()
        .expect("and it is the io::Error");
    assert_eq!(io.kind(), std::io::ErrorKind::ConnectionRefused);
}

/// A relay that accepts the connection and then says nothing must not hold the
/// caller: there is no `Client`, so nothing can stop it.
#[test]
fn a_silent_relay_does_not_hold_connect() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();
    let silent = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        std::thread::sleep(Duration::from_secs(30));
        drop(stream);
    });

    let started = std::time::Instant::now();
    let result = Client::connect(
        &format!("tcpq://{address}"),
        Subscriptions::new(),
        Arc::new(Disconnects::default()),
    );
    let waited = started.elapsed();

    assert!(result.is_err(), "the handshake never finished");
    assert!(
        waited < Duration::from_secs(20),
        "it gave up after {waited:?}"
    );
    drop(silent);
}

/// `close` waits for the reader, which waits on a socket. A `Drop` that holds
/// the caller for a fifth of a second is a surprise on a shutdown path.
#[test]
fn close_does_not_hold_the_caller_on_udpm() {
    let Some(client) = try_connect(17_699, &[".*"], Arc::new(Collector::default())) else {
        return;
    };
    // Long after the reader entered its read, which is the slow condition.
    std::thread::sleep(Duration::from_millis(300));

    let started = std::time::Instant::now();
    client.close().unwrap();
    let waited = started.elapsed();
    assert!(waited < Duration::from_millis(100), "close took {waited:?}");
}

/// The most load-bearing property of the socket setup: two clients on one
/// group and port each get the full bus. This rests on `SO_REUSEPORT`.
#[test]
fn two_clients_on_one_group_both_hear_everything() {
    let first = Arc::new(Collector::default());
    let second = Arc::new(Collector::default());
    let Some(sender) = try_connect(17_691, &[".*"], first.clone()) else {
        return;
    };
    let Some(_listener) = try_connect(17_691, &[".*"], second.clone()) else {
        return;
    };

    for i in 0..10u8 {
        sender.publish("/bus/all", &[i]).unwrap();
    }

    let heard = |c: &Collector| {
        let mut payloads: Vec<u8> = c
            .wait_for(10, Duration::from_secs(2))
            .iter()
            .map(|f| f.payload[0])
            .collect();
        payloads.sort_unstable();
        payloads
    };
    let expected: Vec<u8> = (0..10).collect();
    assert_eq!(heard(&first), expected, "the sender hears its own messages");
    assert_eq!(heard(&second), expected, "and so does the other client");
}

/// A relay writes when it wants to, so one frame can come in two reads and
/// two frames in one. The reader collects, and a torn tail is not a frame.
#[test]
fn a_frame_split_across_two_reads_is_still_one_frame() {
    use lcm_bus::wire::{Frame as WireFrame, tcpq};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();

        let one = tcpq::publish(
            WireFrame {
                channel: "/split/one".to_owned(),
                payload: vec![1; 3_000],
            }
            .view(),
        )
        .unwrap();
        let two = tcpq::publish(
            WireFrame {
                channel: "/split/two".to_owned(),
                payload: vec![2; 40],
            }
            .view(),
        )
        .unwrap();

        // One frame in two writes, far apart.
        stream.write_all(&one[..17]).unwrap();
        std::thread::sleep(Duration::from_millis(80));
        stream.write_all(&one[17..]).unwrap();
        // Then two frames in one write, and a tail that is no frame at all.
        let mut rest = two.clone();
        rest.extend_from_slice(&two);
        rest.extend_from_slice(&two[..9]);
        stream.write_all(&rest).unwrap();
        stream
    });

    let collector = Arc::new(Collector::default());
    let client = Client::connect(
        &format!("tcpq://{address}"),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .expect("connect");

    let frames = collector.wait_for(3, Duration::from_secs(3));
    assert_eq!(frames.len(), 3, "the torn tail is not a fourth frame");
    assert_eq!(frames[0].channel, "/split/one");
    assert_eq!(
        frames[0].payload,
        vec![1; 3_000],
        "reassembled across reads"
    );
    assert_eq!(frames[1].channel, "/split/two");
    assert_eq!(frames[2].channel, "/split/two");

    client.close().unwrap();
    drop(server.join().expect("server"));
}

/// A `Client` goes in an `Arc` and gets shared. A field that is not `Send`,
/// or not `Sync`, breaks all callers, and nothing here says so.
const _: fn() = || {
    fn shareable<T: Send + Sync>() {}
    shareable::<Client>();
    shareable::<Subscriptions>();
};

/// `Drop` calls `close`, so a caller that closes and then drops closes again.
#[test]
fn closing_a_second_time_is_harmless() {
    let Some(client) = try_connect(17_692, &[".*"], Arc::new(Collector::default())) else {
        return;
    };
    client.close().unwrap();
    client.close().unwrap();
    assert!(!client.is_connected());
    assert!(matches!(
        client.publish("/bus/x", &[1]),
        Err(lcm_bus::ClientError::Closed)
    ));
    drop(client);
}

/// One byte more than a datagram holds turns one datagram into fragments.
/// A mistake on one side of the limit or the other drops the message.
#[test]
fn a_payload_on_each_side_of_the_datagram_limit_arrives() {
    use lcm_bus::wire::udpm;

    const SHORT_MAX: usize = 600;
    let collector = Arc::new(Collector::default());
    let url = format!("udpm://239.255.76.99:17693?ttl=0&short_max={SHORT_MAX}");
    let client = match Client::connect(&url, subscriptions(&[".*"]), collector.clone()) {
        Ok(client) => client,
        Err(e) => {
            require_multicast(&format!("{e}"));
            return;
        }
    };

    // The channel name and its NUL use part of the first datagram.
    let head = "/edge/x".len() + 1;
    let sizes = [SHORT_MAX - head - 1, SHORT_MAX - head, SHORT_MAX - head + 1];
    for size in sizes {
        client.publish("/edge/x", &vec![0xC3; size]).unwrap();
    }

    let frames = collector.wait_for(3, Duration::from_secs(3));
    let lengths: Vec<usize> = frames.iter().map(|f| f.payload.len()).collect();
    assert_eq!(
        lengths, sizes,
        "one on each side of the limit, and the limit"
    );
    assert!(frames.iter().all(|f| f.payload.iter().all(|b| *b == 0xC3)));

    // Two full datagrams, then two fragments: the last message crossed.
    let stats = client.stats();
    assert_eq!(stats.delivered, 3);
    assert_eq!(stats.received, 4, "the last one went as fragments");
    assert_eq!(udpm::fragment_max(SHORT_MAX), 588);
}

/// A handler that holds a `Weak<Client>` and closes it is a natural thing to
/// write. It runs on the reader thread, and a thread that joins itself panics.
#[test]
fn a_handler_can_close_its_own_client() {
    use std::sync::Weak;

    #[derive(Default)]
    struct Closer {
        client: Mutex<Option<Weak<Client>>>,
        closed: Mutex<bool>,
    }

    impl DeliveryHandler for Closer {
        fn on_delivery(&self, _: Delivery) {
            let held = self.client.lock().unwrap().clone();
            if let Some(client) = held.and_then(|weak| weak.upgrade()) {
                client.close().unwrap();
                *self.closed.lock().unwrap() = true;
            }
        }
    }

    let handler = Arc::new(Closer::default());
    let Some(client) = try_connect(17_695, &[".*"], handler.clone()) else {
        return;
    };
    let client = Arc::new(client);
    *handler.client.lock().unwrap() = Some(Arc::downgrade(&client));

    client.publish("/close/me", &[1]).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline && !*handler.closed.lock().unwrap() {
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(*handler.closed.lock().unwrap(), "close came back");
    assert!(!client.is_connected());
}

/// A client that subscribes to nothing still takes each datagram off the
/// group and decodes it. A publisher asks for no subscriptions and no
/// handler, binds no port, joins no group, and runs no reader thread.
#[test]
fn a_publisher_does_not_read_the_bus() {
    const PORT: u16 = 17_698;
    let heard = Arc::new(Collector::default());
    let Some(listener) = try_connect(PORT, &[".*"], heard.clone()) else {
        return;
    };

    let publisher = Client::publisher(&multicast_url(PORT)).expect("a publisher needs no group");
    for i in 0..20u8 {
        publisher.publish("/publish/only", &[i]).unwrap();
    }

    assert_eq!(
        heard.wait_for(20, Duration::from_secs(2)).len(),
        20,
        "the messages reach a client that wants them"
    );
    let stats = publisher.stats();
    assert_eq!(stats.received, 0, "and the publisher read none of them");
    assert_eq!(stats.delivered, 0);
    assert_eq!(publisher.recv_buffer_size(), None, "it listens on nothing");
    assert!(publisher.can_publish());

    drop(listener);
}

/// A publisher has no reader. A pattern given to one would sit in a set that
/// nothing reads, and on a relay it would name traffic that no reader
/// takes, which fills the socket and holds the relay.
#[test]
fn a_publisher_takes_no_subscriptions() {
    let publisher = Client::publisher(&multicast_url(17_688)).expect("a publisher");
    assert!(matches!(
        publisher.subscribe("/anything/.*"),
        Err(lcm_bus::ClientError::PublishOnly)
    ));
    assert!(matches!(
        publisher.unsubscribe("/anything/.*"),
        Err(lcm_bus::ClientError::PublishOnly)
    ));

    // A client that receives still takes them.
    let Some(client) = try_connect(17_689, &[], Arc::new(Collector::default())) else {
        return;
    };
    assert!(client.subscribe("/anything/.*").is_ok());
}

/// A log open to read takes no messages, so there is nothing to publish to.
#[test]
fn a_replay_is_not_a_publisher() {
    let path = std::env::temp_dir().join("lcm-bus-not-a-publisher.lcmlog");
    std::fs::write(&path, []).unwrap();
    let url = format!("file://{}?mode=r", path.display());
    assert!(matches!(
        Client::publisher(&url),
        Err(lcm_bus::ClientError::ReadOnly)
    ));
}

/// C and Java relay whatever a publisher gives them, and neither holds it to
/// the 63 bytes of LCM. So a name this crate will not take arrives in the
/// normal course of things, and it must cost the message and not the bus.
#[test]
fn a_relay_frame_this_crate_refuses_does_not_end_the_connection() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn raw(channel: &str, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&tcpq::MESSAGE_TYPE_PUBLISH.to_be_bytes());
        out.extend_from_slice(&(channel.len() as u32).to_be_bytes());
        out.extend_from_slice(channel.as_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();
    let relay = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        stream.write_all(&raw(&"c".repeat(64), b"x")).unwrap();
        stream.write_all(&raw("", b"y")).unwrap();
        stream.write_all(&raw("/good/one", b"z")).unwrap();
        std::thread::sleep(Duration::from_secs(2));
    });

    let collector = Arc::new(Collector::default());
    let client = Client::connect(
        &format!("tcpq://{address}"),
        subscriptions(&[".*"]),
        collector.clone(),
    )
    .expect("connect");

    let frames = collector.wait_for(1, Duration::from_secs(3));
    assert_eq!(frames.len(), 1, "the one frame this crate takes");
    assert_eq!(frames[0].channel, "/good/one");
    assert!(client.is_connected(), "and the connection is still up");
    assert_eq!(client.stats().discarded, 2, "the other two were counted");

    client.close().unwrap();
    drop(relay);
}

/// The channel takes what a handler takes, so a caller writes no
/// handler and puts no `unwrap` on the reader thread. A reader that falls
/// behind costs messages, and the count of them is there to be read.
#[test]
fn a_channel_delivers_and_counts_what_it_could_not_hold() {
    let (client, deliveries) =
        match Client::connect_channel(&multicast_url(17_687), subscriptions(&[".*"]), 64) {
            Ok(pair) => pair,
            Err(e) => return require_multicast(&format!("{e}")),
        };

    for i in 0..10u8 {
        client.publish("/channel/one", &[i]).unwrap();
    }

    let mut got = Vec::new();
    while got.len() < 10 {
        match deliveries.recv_timeout(Duration::from_secs(2)) {
            Ok(delivery) => got.push(delivery.frame.payload[0]),
            Err(_) => break,
        }
    }
    assert_eq!(got, (0..10).collect::<Vec<u8>>());
    assert_eq!(deliveries.dropped(), 0, "nothing was lost at this rate");

    // A channel of one, and nobody taking from it.
    let (slow, behind) =
        match Client::connect_channel(&multicast_url(17_686), subscriptions(&[".*"]), 1) {
            Ok(pair) => pair,
            Err(e) => return require_multicast(&format!("{e}")),
        };
    for i in 0..50u8 {
        slow.publish("/channel/two", &[i]).unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline && behind.dropped() == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        behind.dropped() > 0,
        "and the ones it could not hold say so"
    );
}

/// A udpm bus sends on one socket and receives on a second, as LCM does. So
/// reader that stops takes nothing from a publisher, and one fault in the
/// caller's handler must not quietly end the sending half of a healthy bus.
#[test]
fn a_reader_that_stops_leaves_udpm_publishing() {
    #[derive(Default)]
    struct Bad(Mutex<Option<String>>);

    impl DeliveryHandler for Bad {
        fn on_delivery(&self, _: Delivery) {
            panic!("a handler fault");
        }

        fn on_stop(&self, cause: Stop) {
            *self.0.lock().unwrap() = Some(format!("{cause}"));
        }
    }

    let handler = Arc::new(Bad::default());
    let Some(client) = try_connect(17_685, &[".*"], handler.clone()) else {
        return;
    };

    client.publish("/stop/one", &[1]).expect("before the fault");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline && handler.0.lock().unwrap().is_none() {
        std::thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(
        handler.0.lock().unwrap().as_deref(),
        Some("the handler panicked")
    );
    assert!(!client.is_connected(), "the reader is gone");
    client
        .publish("/stop/two", &[2])
        .expect("and publishing is not");

    client.close().unwrap();
    assert!(matches!(
        client.publish("/stop/three", &[3]),
        Err(lcm_bus::ClientError::Closed)
    ));
}

/// A publish hands its frame to the writer thread, so `Ok` says the frame is
/// queued and not that it is on the wire. A frame queued behind one that the
/// relay never took does not go: the writer ends the connection where a
/// write is torn, because a frame that followed a part frame would go into
/// the middle of it and the relay could frame neither.
///
/// What must not happen is that the frame goes quietly. `Stats::unsent`
/// counts what was taken and never went, and a publish after the connection
/// ends says `Closed` rather than taking one more.
#[test]
fn a_frame_that_never_went_is_counted_and_not_lost() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let (release, held) = mpsc::channel::<()>();
    let relay = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        // And from here it takes nothing off the socket.
        let _ = held.recv();
    });

    let client = Arc::new(
        Client::connect(
            &format!("tcpq://{address}"),
            Subscriptions::new(),
            Arc::new(Disconnects::default()),
        )
        .expect("connect"),
    );

    // Larger than the socket will hold, so it waits on a relay that is not
    // listening, and holds the lock while it waits.
    let first = std::thread::spawn({
        let client = Arc::clone(&client);
        move || client.publish("/wreck/first", &vec![0u8; 8 << 20])
    });
    std::thread::sleep(Duration::from_millis(300));

    // This gets past the test in `write` and then waits for the lock.
    let second = std::thread::spawn({
        let client = Arc::clone(&client);
        move || client.publish("/wreck/second", &[1, 2, 3])
    });
    std::thread::sleep(Duration::from_millis(300));

    // The sending half goes while both are in flight.
    let _ = client.close();

    let _ = second.join().unwrap();
    let _ = first.join().unwrap();
    assert!(!client.is_connected());

    let stats = client.stats();
    assert!(
        stats.unsent > 0,
        "a frame the relay never took went without a count of it: {stats:?}"
    );
    // And the connection takes nothing more.
    assert!(matches!(
        client.publish("/wreck/third", &[1]),
        Err(lcm_bus::ClientError::Closed)
    ));

    let _ = release.send(());
    let _ = relay.join();
}

/// A subscription is two things that have to agree: the rule this client
/// keeps, and the rule the relay keeps for it. A pattern the client refuses
/// must not reach the relay first — the relay would then hold one that
/// nothing here can name, so nothing here can take it back.
///
/// And the two steps are one step. Two threads that both find a rule and
/// both tell the relay to drop it leave the relay holding it once more than
/// this client believes, which is the same rule nothing can name.
#[test]
fn what_the_relay_holds_is_what_the_client_holds() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    // Every subscribe and unsubscribe the relay hears, in order.
    let heard = Arc::new(Mutex::new(Vec::<(u32, String)>::new()));
    let relay = std::thread::spawn({
        let heard = Arc::clone(&heard);
        move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut opening = [0u8; 8];
            stream.read_exact(&mut opening).expect("handshake");
            stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
            stream
                .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
                .unwrap();
            let mut rest = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => rest.extend_from_slice(&chunk[..n]),
                }
                // Type, then a length-prefixed pattern.
                while rest.len() >= 8 {
                    let kind = u32::from_be_bytes(rest[0..4].try_into().unwrap());
                    let len = u32::from_be_bytes(rest[4..8].try_into().unwrap()) as usize;
                    if rest.len() < 8 + len {
                        break;
                    }
                    let text = String::from_utf8_lossy(&rest[8..8 + len]).into_owned();
                    heard.lock().unwrap().push((kind, text));
                    rest.drain(..8 + len);
                }
            }
        }
    });

    let client = Arc::new(
        Client::connect(
            &format!("tcpq://{address}"),
            Subscriptions::new(),
            Arc::new(Disconnects::default()),
        )
        .expect("connect"),
    );

    // A pattern this client refuses never reaches the relay.
    for refused in ["[unclosed", "(?P<bad"] {
        assert!(client.subscribe(refused).is_err(), "{refused}");
    }
    for refused in ["", "with\0nul"] {
        assert!(client.subscribe_name(refused).is_err(), "{refused:?}");
    }

    // Many threads changing the same few rules at once.
    let names = ["/one", "/two", "/three", "/four", "/five", "/six"];
    let mut hands = Vec::new();
    for thread in 0..8 {
        let client = Arc::clone(&client);
        hands.push(std::thread::spawn(move || {
            for turn in 0..300 {
                let name = names[(thread + turn) % names.len()];
                if turn % 2 == 0 {
                    let _ = client.subscribe_name(name);
                } else {
                    let _ = client.unsubscribe(name);
                }
            }
        }));
    }
    for hand in hands {
        hand.join().unwrap();
    }

    // Take every rule this client still holds, so the relay should end with
    // none of them.
    for name in names {
        while client.unsubscribe(name).unwrap_or(false) {}
    }
    std::thread::sleep(Duration::from_millis(300));
    let _ = client.close();
    let _ = relay.join();

    let heard = heard.lock().unwrap();
    // The texts above, and not the shape of them: an empty pattern and a
    // pattern with a NUL in it both compile, so `subscribe` takes both and
    // the relay hears both. Neither names a channel a bus can carry, and
    // `unsubscribe` names them back, so the two sets still agree. It is the
    // name door that refuses them, and this is what it refused.
    let refused = ["[unclosed", "(?P<bad", "", "with\0nul"];
    assert!(
        !heard
            .iter()
            .any(|(_, text)| refused.contains(&text.as_str())),
        "a rule this client refused reached the relay: {heard:?}"
    );

    let mut held: std::collections::BTreeMap<&str, i64> = std::collections::BTreeMap::new();
    for (kind, text) in heard.iter() {
        let step = if *kind == 2 { 1 } else { -1 };
        *held.entry(text.as_str()).or_default() += step;
    }
    let left: Vec<_> = held.iter().filter(|(_, count)| **count != 0).collect();
    assert!(
        left.is_empty(),
        "the relay was left holding rules this client cannot name: {left:?}"
    );
}

/// The opening subscriptions go out with `write_all`, which reads a socket
/// timeout as the answer and not as the poll the writes after it read it as.
/// A set large enough to fill the socket then meets a relay that has not
/// read yet, and the connection is lost before it is made.
#[test]
fn a_large_subscription_set_survives_a_relay_that_pauses() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    // A small receive buffer, so the patterns fill the socket rather than
    // going into the kernel's. Without that the write never has to wait and
    // the timeout on it never decides anything.
    let held = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    held.set_recv_buffer_size(4096).unwrap();
    held.bind(
        &"127.0.0.1:0"
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .into(),
    )
    .unwrap();
    held.listen(1).unwrap();
    let listener: TcpListener = held.into();
    let address = listener.local_addr().unwrap();

    let relay = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        // Long enough that a poll-sized timeout gives up, and short enough
        // that a whole one does not.
        std::thread::sleep(Duration::from_secs(3));
        let mut sink = [0u8; 64 * 1024];
        while stream.read(&mut sink).map(|n| n > 0).unwrap_or(false) {}
    });

    // More bytes of patterns than a socket will hold.
    let mut subscriptions = Subscriptions::new();
    for number in 0..8192 {
        let long = "x".repeat(400);
        subscriptions
            .add(&format!("/bus/{number}/{long}/.*"))
            .unwrap();
    }

    let client = Client::connect(
        &format!("tcpq://{address}"),
        subscriptions,
        Arc::new(Disconnects::default()),
    );
    assert!(
        client.is_ok(),
        "a relay that reads its subscriptions late is still a relay: {:?}",
        client.err().map(|e| e.to_string())
    );
    let _ = client.unwrap().close();
    let _ = relay.join();
}

/// `on_delivery` runs on the reader thread. A handler that publishes or
/// subscribes from it used to give that thread to the relay: the write took
/// the lock that puts writes in sequence, and the client took nothing off
/// the bus until the relay had the whole message. On a relay that stopped
/// reading, that is the write deadline — a minute of a bus going nowhere,
/// with `is_connected` saying yes the whole time.
///
/// The writer thread is what takes that wait. A publish hands its frame over
/// and gives back, so the reader goes on reading.
#[test]
fn a_handler_that_subscribes_does_not_stop_the_bus() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    /// Counts deliveries, and changes a subscription on each one.
    struct Busy {
        client: Mutex<Option<std::sync::Weak<Client>>>,
        seen: AtomicU64,
        worst: Mutex<Duration>,
        last: Mutex<Option<std::time::Instant>>,
    }

    impl DeliveryHandler for Busy {
        fn on_delivery(&self, _: Delivery) {
            let now = std::time::Instant::now();
            let mut last = self.last.lock().unwrap();
            if let Some(before) = last.replace(now) {
                let gap = now.duration_since(before);
                let mut worst = self.worst.lock().unwrap();
                if gap > *worst {
                    *worst = gap;
                }
            }
            drop(last);
            self.seen.fetch_add(1, Ordering::Relaxed);

            let held = self.client.lock().unwrap().clone();
            if let Some(client) = held.and_then(|weak| weak.upgrade()) {
                // The rule is held, so this reaches the relay.
                let _ = client.unsubscribe("/bus/.*");
                let _ = client.subscribe("/bus/.*");
            }
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let (release, held) = mpsc::channel::<()>();
    let relay = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        // It sends steadily and reads nothing at all.
        let frame = tcpq::publish(lcm_bus::wire::FrameRef {
            channel: "/bus/tick",
            payload: &[7; 64],
        })
        .unwrap();
        while stream.write_all(&frame).is_ok() {
            if held.try_recv().is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let handler = Arc::new(Busy {
        client: Mutex::new(None),
        seen: AtomicU64::new(0),
        worst: Mutex::new(Duration::ZERO),
        last: Mutex::new(None),
    });
    let client = Arc::new(
        Client::connect(
            &format!("tcpq://{address}"),
            subscriptions(&["/bus/.*"]),
            handler.clone(),
        )
        .expect("connect"),
    );
    *handler.client.lock().unwrap() = Some(Arc::downgrade(&client));

    // Two messages the relay will never take, on other threads: one ends up
    // in the writer's hand and one in the queue, so the queue holds more
    // than the whole budget. A small frame that had to fit inside that
    // budget would wait its full deadline, which is what the line of its own
    // is for.
    let big: Vec<_> = (0..2)
        .map(|number| {
            std::thread::spawn({
                let client = Arc::clone(&client);
                move || client.publish(&format!("/bus/big{number}"), &vec![0u8; 8 << 20])
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(5));
    let seen = handler.seen.load(Ordering::Relaxed);
    let worst = *handler.worst.lock().unwrap();

    let _ = release.send(());
    let _ = client.close();
    for held in big {
        let _ = held.join();
    }
    let _ = relay.join();

    assert!(
        seen > 100,
        "the bus gave {seen} messages in five seconds while one publish waited"
    );
    assert!(
        worst < Duration::from_secs(2),
        "the longest a delivery waited on the publish was {worst:?}"
    );
}

/// A relay stub that handshakes and then does what `after` says.
fn stub_relay(
    after: impl FnOnce(std::net::TcpStream) + Send + 'static,
) -> (String, std::thread::JoinHandle<()>) {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap().to_string();
    let held = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        after(stream);
    });
    (address, held)
}

/// `flush` is how a caller waits for what it published to be on the wire, so
/// it is the one call that must not say yes about frames that were thrown
/// away. An empty outbox is not an outbox that went.
///
/// The count has to hold wherever the writer stops, and it stops two ways:
/// its own write tears, or the reader stops first and takes `running` with
/// it. The second is the ordinary one — a relay that restarts — and a count
/// taken only on the first reads zero for it.
#[test]
fn a_flush_does_not_say_yes_about_frames_that_never_went() {
    use std::sync::mpsc;

    for what in ["the relay went", "a close"] {
        let (release, held) = mpsc::channel::<()>();
        let (address, relay) = stub_relay(move |stream| {
            // It reads nothing, and then it stops talking without going: the
            // reader sees the end of the stream and stops, while the writer
            // is only slow and has frames in hand. That is the writer's
            // other way out, and the ordinary one.
            std::thread::sleep(Duration::from_millis(400));
            let _ = stream.shutdown(std::net::Shutdown::Write);
            let _ = held.recv();
        });

        let client = Client::connect(
            &format!("tcpq://{address}"),
            Subscriptions::new(),
            Arc::new(Disconnects::default()),
        )
        .expect("connect");

        // More than the relay will ever take.
        for _ in 0..8 {
            let _ = client.publish("/never", &vec![0u8; 2 << 20]);
        }

        if what == "a close" {
            let _ = client.close();
        } else {
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while client.is_connected() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(!client.is_connected(), "{what}: the client is still up");
        }

        let stats = client.stats();
        assert!(stats.unsent > 0, "{what}: nothing counted, {stats:?}");
        let flushed = client.flush();
        assert!(
            flushed.is_err(),
            "{what}: flush said {flushed:?} about {} frames that never went",
            stats.unsent
        );

        let _ = release.send(());
        let _ = client.close();
        let _ = relay.join();
    }
}

/// `flush` waits for the frames of its caller and not for an empty queue.
/// With one more thread publishing, a queue is never empty, and a wait for
/// one never ends: a supervisor that flushes to test a connection would tear
/// down a healthy one every time.
#[test]
fn a_flush_does_not_wait_on_another_thread_that_keeps_publishing() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let (release, held) = mpsc::channel::<()>();
    let (address, relay) = stub_relay(move |mut stream| {
        use std::io::Read;
        // Slow enough that the outbox always holds something, so a wait for
        // an empty one is a wait that never ends.
        let mut sink = vec![0u8; 64 * 1024];
        loop {
            std::thread::sleep(Duration::from_millis(20));
            if held.try_recv().is_ok() || !stream.read(&mut sink).map(|n| n > 0).unwrap_or(false) {
                return;
            }
        }
    });

    let client = Arc::new(
        Client::connect(
            &format!("tcpq://{address}"),
            Subscriptions::new(),
            Arc::new(Disconnects::default()),
        )
        .expect("connect"),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let busy = std::thread::spawn({
        let client = Arc::clone(&client);
        let stop = Arc::clone(&stop);
        move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = client.publish("/steady", &vec![0u8; 256 * 1024]);
            }
        }
    });

    std::thread::sleep(Duration::from_millis(300));
    let began = std::time::Instant::now();
    let flushed = client.flush();
    let took = began.elapsed();

    stop.store(true, Ordering::Relaxed);
    let _ = busy.join();
    let _ = release.send(());
    let _ = client.close();
    let _ = relay.join();

    assert!(
        flushed.is_ok(),
        "flush on a healthy relay said {flushed:?} after {took:?}"
    );
    assert!(took < Duration::from_secs(10), "and it took {took:?}");
}

/// Room in the outbox goes to the publish that has waited longest.
///
/// Without that, room goes to whoever wakes first, and a message larger than
/// the whole budget waits for an outbox that is empty while the small ones
/// behind it keep it full. A point cloud beside 100 Hz telemetry on one
/// client then never goes at all, on a relay with nothing wrong with it.
#[test]
fn a_large_message_is_not_held_out_by_small_ones() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;

    let (release, held) = mpsc::channel::<()>();
    let (address, relay) = stub_relay(move |mut stream| {
        use std::io::Read;
        // Slow enough that the outbox stays full, and honest.
        let mut sink = vec![0u8; 256 * 1024];
        loop {
            std::thread::sleep(Duration::from_millis(5));
            if held.try_recv().is_ok() || !stream.read(&mut sink).map(|n| n > 0).unwrap_or(false) {
                return;
            }
        }
    });

    let client = Arc::new(
        Client::connect(
            &format!("tcpq://{address}"),
            Subscriptions::new(),
            Arc::new(Disconnects::default()),
        )
        .expect("connect"),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let small = Arc::new(AtomicU64::new(0));
    let busy = std::thread::spawn({
        let client = Arc::clone(&client);
        let stop = Arc::clone(&stop);
        let small = Arc::clone(&small);
        move || {
            while !stop.load(Ordering::Relaxed) {
                if client.publish("/small", &vec![0u8; 64 * 1024]).is_ok() {
                    small.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    std::thread::sleep(Duration::from_millis(300));

    // Larger than the whole budget, so it can only go on its own.
    let began = std::time::Instant::now();
    let large = client.publish("/large", &vec![0u8; 8 << 20]);
    let took = began.elapsed();

    stop.store(true, Ordering::Relaxed);
    let _ = busy.join();
    let _ = release.send(());
    let _ = client.close();
    let _ = relay.join();

    assert!(
        large.is_ok(),
        "the large message said {large:?} after {took:?}, while {} small ones went",
        small.load(Ordering::Relaxed)
    );
    assert!(took < Duration::from_secs(30), "and it waited {took:?}");
}

/// A relay that is slow is not a relay that is gone, and what tells the two
/// apart is whether bytes move — not how many. A bound on the whole of a
/// message makes the length of the message a rate the relay has to keep, and
/// what is lost where it fires is the connection and not the message.
#[test]
fn a_slow_relay_keeps_its_connection_however_large_the_message() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    // The receive window is left as the kernel gives it, so the client's own
    // send buffer fills and stays full. That is the state a deadline on a
    // write that says nothing gets wrong: bytes move on the wire the whole
    // time and none of them makes a write give back.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let (release, held) = mpsc::channel::<()>();
    let relay = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        stream.read_exact(&mut opening).expect("handshake");
        stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
        stream
            .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
            .unwrap();
        // 204 bytes every 50 ms: four kilobytes a second, slow and steady
        // and honest, taken in small reads as a slow link delivers them.
        // Under what the message below needs to go inside any bound on the
        // whole of it, and under the rate a deadline on a write that says
        // nothing asks a relay to keep.
        let mut sink = vec![0u8; 204];
        let mut read = 0usize;
        while held.try_recv().is_err() {
            std::thread::sleep(Duration::from_millis(250));
            match stream.read(&mut sink) {
                Ok(0) | Err(_) => return read,
                Ok(n) => read += n,
            }
        }
        read
    });

    let stopped = Arc::new(Disconnects::default());
    let client = Client::connect(
        &format!("tcpq://{address}"),
        Subscriptions::new(),
        stopped.clone(),
    )
    .expect("connect");

    // Larger than any socket buffer will take off the writer's hands, so
    // the writer is waiting on the relay for the whole of this and not on
    // the kernel. 32 MiB at four kilobytes a second is two hours of wire,
    // and a bound on the whole of it caps out at a minute.
    client.publish("/large", &vec![0u8; 32 << 20]).unwrap();

    // Long enough for such a bound to have fired.
    std::thread::sleep(Duration::from_secs(65));
    let connected = client.is_connected();
    let stops = stopped.0.lock().unwrap().clone();

    let _ = release.send(());
    let _ = client.close();
    let _ = relay.join();

    assert!(
        connected && stops.is_empty(),
        "a relay taking 4 KiB a second lost the connection: {stops:?}"
    );
}

/// `connect` is one wait, and `CONNECT_TIMEOUT` is how long.
///
/// Everything in it waits on the relay — the addresses a name gives, the
/// handshake, the opening subscriptions — and all of it is on the thread of
/// whoever called it, who has no client to stop it with. `read_exact` and
/// `write_all` bound none of that: a socket timeout bounds one call, and a
/// call that moves one byte starts it again, so a relay that answers a byte
/// at a time is paid a fresh timeout for each of them.
#[test]
fn a_relay_that_answers_slowly_does_not_hold_connect_for_ever() {
    use lcm_bus::wire::tcpq;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().unwrap();

    let relay = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut opening = [0u8; 8];
        let _ = stream.read_exact(&mut opening);
        // Eight bytes of answer, one every three seconds. Each one used to
        // buy the whole handshake timeout again.
        let mut reply = tcpq::MAGIC_SERVER.to_be_bytes().to_vec();
        reply.extend_from_slice(&tcpq::PROTOCOL_VERSION.to_be_bytes());
        for byte in reply {
            std::thread::sleep(Duration::from_secs(3));
            if stream.write_all(&[byte]).is_err() {
                return;
            }
        }
        std::thread::sleep(Duration::from_secs(30));
    });

    let began = std::time::Instant::now();
    let opened = Client::connect(
        &format!("tcpq://{address}"),
        Subscriptions::new(),
        Arc::new(Disconnects::default()),
    );
    let took = began.elapsed();
    drop(opened);
    let _ = relay.join();

    // Eight bytes at three seconds each is 24 s of answer against a ten
    // second budget, so this is the budget and not the answer.
    assert!(
        took < Duration::from_secs(20),
        "connect waited {took:?} on a relay answering one byte at a time"
    );
}

/// A socket closed with bytes unread is closed with a reset, and a reset
/// throws away what the kernel was still sending.
///
/// Those are messages `publish` took, that `flush` said had gone, and that
/// `Stats::unsent` does not count — the one thing this crate says cannot
/// happen. One frame from the relay is enough to arm it, and every client
/// that subscribes has unread bytes at `close`, because the reader stops
/// between frames while the relay goes on sending.
#[test]
fn a_close_does_not_throw_away_what_the_kernel_is_still_sending() {
    use lcm_bus::wire::{FrameRef, tcpq};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    // A relay that is quiet, and one that said a frame. A relay that is
    // still sending as the close runs is the case this cannot win: its
    // bytes arrive between the last read and the close of the socket, and
    // no writer of a socket can see which messages the reset then took.
    for chattering in [0u8, 1] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().unwrap();

        let (read_back, counted) = mpsc::channel::<usize>();
        let relay = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut opening = [0u8; 8];
            stream.read_exact(&mut opening).expect("handshake");
            stream.write_all(&tcpq::MAGIC_SERVER.to_be_bytes()).unwrap();
            stream
                .write_all(&tcpq::PROTOCOL_VERSION.to_be_bytes())
                .unwrap();
            let said = tcpq::publish(FrameRef {
                channel: "/from/relay",
                payload: &[7; 8],
            })
            .unwrap();
            // One frame is all it takes to leave bytes unread. Talking the
            // whole way through is the case where a queue this drains is
            // never empty, and the client must still not throw away what it
            // was sending.
            let talking = false;
            if chattering >= 1 {
                stream.write_all(&said).unwrap();
            }
            // It reads nothing until the client has published and closed,
            // so everything is in the kernel when the close happens.
            let until = std::time::Instant::now() + Duration::from_millis(700);
            while std::time::Instant::now() < until {
                if talking && stream.write_all(&said).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            let mut took = 0usize;
            let mut sink = vec![0u8; 64 * 1024];
            while let Ok(n) = stream.read(&mut sink) {
                if n == 0 {
                    break;
                }
                took += n;
            }
            let _ = read_back.send(took);
        });

        // A publisher has no reader thread, so what the relay sends stays
        // unread in the kernel — which is what arms the reset. A client that
        // subscribes has the same bytes unread whenever its reader is
        // between frames.
        let client = Client::publisher(&format!("tcpq://{address}")).expect("connect");

        let each = 64 * 1024;
        let mut sent = 0usize;
        for _ in 0..16 {
            client.publish("/to/relay", &vec![0u8; each]).unwrap();
            sent += 1;
        }
        let _ = client.flush();
        let unsent = client.stats().unsent;
        // A relay that never stops talking keeps the queue this drains from
        // being empty, so the close is a reset whatever it does. What must
        // not happen is that it is quiet about it.
        client.close().unwrap();
        // The reset is at the close of the socket itself, which is here.
        drop(client);

        let took = counted.recv_timeout(Duration::from_secs(10)).unwrap_or(0);
        let _ = relay.join();

        // Every frame is the head and the payload, and the relay reads the
        // stream to its end, so what arrived is countable in whole frames.
        let whole = took / (each + 4 + 4 + "/to/relay".len() + 4);
        assert_eq!(
            whole as u64 + unsent,
            sent as u64,
            "chattering {chattering}: {sent} published, {whole} arrived, {unsent} counted"
        );
    }
}
