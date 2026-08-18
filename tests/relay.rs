//! The relay, served here and dialled here.
//!
//! TCP on the loopback address, so unlike the multicast tests these run
//! everywhere and skip nowhere.
// A relay reads patterns, so an engine for one is necessary.
#![cfg(all(feature = "std", feature = "patterns"))]

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use lcm_bus::{Client, Delivery, DeliveryHandler, Frame, Subscriptions};

const PATIENCE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Collector {
    frames: Mutex<Vec<Frame>>,
    arrived: Condvar,
}

impl Collector {
    /// At the timeout, this gives the frames that came.
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

    fn seen(&self) -> Vec<Frame> {
        self.frames.lock().unwrap().clone()
    }
}

impl DeliveryHandler for Collector {
    fn on_delivery(&self, delivery: Delivery) {
        self.frames.lock().unwrap().push(delivery.frame);
        self.arrived.notify_all();
    }
}

fn subscriptions(patterns: &[&str]) -> Subscriptions {
    let mut subs = Subscriptions::new();
    for pattern in patterns {
        subs.add(pattern).unwrap();
    }
    subs
}

/// A port the kernel chose, so these tests are parallel.
fn relay(local: Subscriptions, handler: Arc<Collector>) -> (Client, String) {
    let relay = Client::serve("tcpq://127.0.0.1:0", local, handler).expect("the relay binds");
    let bound = relay.bound().expect("a served relay is bound");
    (relay, format!("tcpq://{bound}"))
}

/// `connect` gives back on the greeting, and the relay records the peer after it.
fn wait_for_peers(relay: &Client, n: usize) {
    let deadline = Instant::now() + PATIENCE;
    while relay.peers() != n && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(relay.peers(), n, "the relay should have {n} client(s)");
}

/// The patterns arrive behind the greeting, and a publish between the two reaches
/// nobody.
fn wait_for_patterns(relay: &Client, n: usize) {
    let deadline = Instant::now() + PATIENCE;
    while relay.peer_patterns() < n && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        relay.peer_patterns(),
        n,
        "the relay should match {n} pattern(s)"
    );
}

/// A consumer on another machine reads what the relay publishes.
#[test]
fn a_client_reads_what_the_relay_publishes() {
    let (relay, url) = relay(Subscriptions::new(), Arc::new(Collector::default()));

    let heard = Arc::new(Collector::default());
    let consumer =
        Client::connect(&url, subscriptions(&[".*"]), heard.clone()).expect("the client dials");
    wait_for_peers(&relay, 1);
    wait_for_patterns(&relay, 1);

    relay
        .publish("/pntos/gnss", &[1, 2, 3, 4])
        .expect("publish");

    let frames = heard.wait_for(1, PATIENCE);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].channel, "/pntos/gnss");
    assert_eq!(frames[0].payload, vec![1, 2, 3, 4]);

    let _ = consumer.close();
    let _ = relay.close();
}

/// Several consumers read one stream.
#[test]
fn every_client_that_matches_gets_the_message() {
    let (relay, url) = relay(Subscriptions::new(), Arc::new(Collector::default()));

    let first = Arc::new(Collector::default());
    let second = Arc::new(Collector::default());
    let a = Client::connect(&url, subscriptions(&[".*"]), first.clone()).expect("dial");
    let b = Client::connect(&url, subscriptions(&[".*"]), second.clone()).expect("dial");
    wait_for_peers(&relay, 2);
    wait_for_patterns(&relay, 2);

    relay.publish("/pntos/gnss", &[7]).expect("publish");

    assert_eq!(first.wait_for(1, PATIENCE).len(), 1);
    assert_eq!(second.wait_for(1, PATIENCE).len(), 1);

    let _ = a.close();
    let _ = b.close();
    let _ = relay.close();
}

/// A consumer that wants one channel takes one channel off the wire.
#[test]
fn a_client_gets_only_what_it_subscribed_to() {
    let (relay, url) = relay(Subscriptions::new(), Arc::new(Collector::default()));

    let heard = Arc::new(Collector::default());
    let consumer =
        Client::connect(&url, subscriptions(&["/pntos/gnss"]), heard.clone()).expect("dial");
    wait_for_peers(&relay, 1);
    wait_for_patterns(&relay, 1);

    relay.publish("/pntos/imu", &[1]).expect("publish");
    relay.publish("/pntos/gnss", &[2]).expect("publish");

    let frames = heard.wait_for(1, PATIENCE);
    assert_eq!(frames.len(), 1, "the imu channel was not subscribed to");
    assert_eq!(frames[0].channel, "/pntos/gnss");

    let _ = consumer.close();
    let _ = relay.close();
}

/// A client publishes and the relay carries it: a bus, and not a broadcast.
#[test]
fn the_relay_carries_what_a_client_publishes() {
    let local = Arc::new(Collector::default());
    let (relay, url) = relay(subscriptions(&[".*"]), local.clone());

    let other = Arc::new(Collector::default());
    let a = Client::connect(&url, Subscriptions::new(), Arc::new(Collector::default()))
        .expect("the publisher dials");
    let b =
        Client::connect(&url, subscriptions(&[".*"]), other.clone()).expect("the consumer dials");
    wait_for_peers(&relay, 2);
    // Only the consumer subscribes, and the publish must not race its pattern.
    wait_for_patterns(&relay, 1);

    a.publish("/from/a", &[42]).expect("publish");

    let frames = other.wait_for(1, PATIENCE);
    assert_eq!(frames.len(), 1, "the other client");
    assert_eq!(frames[0].channel, "/from/a");

    let frames = local.wait_for(1, PATIENCE);
    assert_eq!(frames.len(), 1, "the process serving the relay");
    assert_eq!(frames[0].payload, vec![42]);

    let _ = a.close();
    let _ = b.close();
    let _ = relay.close();
}

/// An empty set carries traffic for the clients and hands the local handler nothing.
/// A system that copies its own traffic onto the bus needs that, or the copy comes
/// back and the loop has no end.
#[test]
fn a_relay_that_subscribes_to_nothing_hears_nothing() {
    let local = Arc::new(Collector::default());
    let (relay, url) = relay(Subscriptions::new(), local.clone());

    let heard = Arc::new(Collector::default());
    let consumer = Client::connect(&url, subscriptions(&[".*"]), heard.clone()).expect("dial");
    wait_for_peers(&relay, 1);
    wait_for_patterns(&relay, 1);

    relay.publish("/pntos/gnss", &[1]).expect("its own");
    consumer
        .publish("/from/consumer", &[2])
        .expect("and one back");

    // The consumer hears its own publish and the relay's, as on a udpm bus.
    assert_eq!(heard.wait_for(2, PATIENCE).len(), 2);
    assert!(
        local.seen().is_empty(),
        "the relay subscribed to nothing: {:?}",
        local.seen()
    );

    let _ = consumer.close();
    let _ = relay.close();
}

/// A client that goes leaves no peer behind, and the relay keeps serving.
#[test]
fn a_client_that_leaves_is_forgotten() {
    let (relay, url) = relay(Subscriptions::new(), Arc::new(Collector::default()));

    let first = Client::connect(&url, subscriptions(&[".*"]), Arc::new(Collector::default()))
        .expect("dial");
    wait_for_peers(&relay, 1);
    let _ = first.close();
    wait_for_peers(&relay, 0);

    let heard = Arc::new(Collector::default());
    let second = Client::connect(&url, subscriptions(&[".*"]), heard.clone()).expect("dial again");
    wait_for_peers(&relay, 1);
    wait_for_patterns(&relay, 1);
    relay.publish("/still/here", &[1]).expect("publish");
    assert_eq!(heard.wait_for(1, PATIENCE).len(), 1);

    let _ = second.close();
    let _ = relay.close();
}

/// Something that is not an LCM client costs its connection and not the relay.
#[test]
fn a_greeting_this_does_not_speak_ends_that_connection_alone() {
    use std::io::Write;

    let (relay, url) = relay(Subscriptions::new(), Arc::new(Collector::default()));
    let address = relay.bound().expect("bound");

    {
        let mut stray = std::net::TcpStream::connect(address).expect("connect");
        // Not either magic number.
        stray.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0]).ok();
        stray.flush().ok();
    }

    let deadline = Instant::now() + PATIENCE;
    while relay.refused_peers() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(relay.refused_peers(), 1);

    let heard = Arc::new(Collector::default());
    let consumer = Client::connect(&url, subscriptions(&[".*"]), heard.clone())
        .expect("the relay is still serving");
    wait_for_peers(&relay, 1);
    wait_for_patterns(&relay, 1);
    relay.publish("/still/here", &[1]).expect("publish");
    assert_eq!(heard.wait_for(1, PATIENCE).len(), 1);

    let _ = consumer.close();
    let _ = relay.close();
}

/// A relay that closed takes no more clients.
#[test]
fn a_closed_relay_takes_no_more_clients() {
    let (relay, url) = relay(Subscriptions::new(), Arc::new(Collector::default()));
    let address = relay.bound().expect("bound");
    let _ = relay.close();

    // The listener is dropped with the accept thread, so the port refuses.
    let deadline = Instant::now() + PATIENCE;
    loop {
        match Client::connect(&url, Subscriptions::new(), Arc::new(Collector::default())) {
            Err(_) => break,
            Ok(client) => {
                let _ = client.close();
                assert!(
                    Instant::now() < deadline,
                    "{address} still accepts after close"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

/// Only `tcpq://` has a relay to serve.
#[test]
fn a_group_and_a_log_have_no_relay() {
    for url in ["udpm://239.255.76.67:7667", "file:///tmp/x.lcmlog"] {
        assert!(
            Client::serve(url, Subscriptions::new(), Arc::new(Collector::default())).is_err(),
            "{url}"
        );
    }
}

/// A URL that states a host binds that host, and one that states none binds every
/// address.
///
/// `BusUrl::parse` fills an empty host with the address a client dials, so the parsed
/// host is `127.0.0.1` whether the URL stated it or stated nothing, and cannot decide
/// this.
#[test]
fn a_stated_host_is_the_host_that_is_bound() {
    let heard = Arc::new(Collector::default());
    let relay =
        Client::serve("tcpq://127.0.0.1:0", Subscriptions::new(), heard).expect("loopback binds");
    let bound = relay.bound().expect("bound");
    assert_eq!(
        bound.ip().to_string(),
        "127.0.0.1",
        "a URL that states loopback must not bind every address"
    );
    let _ = relay.close();

    let heard = Arc::new(Collector::default());
    let every = Client::serve("tcpq://:0", Subscriptions::new(), heard).expect("every address");
    assert_eq!(
        every.bound().expect("bound").ip().to_string(),
        "0.0.0.0",
        "a URL that states no host binds every address"
    );
    let _ = every.close();
}
