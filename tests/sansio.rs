//! The receive path with no socket under it.
//! A runtime this crate does not know about brings the bytes.

use lcm_bus::bus::{Everything, Filter, MulticastReceiver, ReadBuffer};
use lcm_bus::wire::{Decoded, tcpq, udpm};
use lcm_bus::{Frame, FrameRef};

/// Each datagram is behind its length as a big-endian `u32`.
fn datagrams(capture: &[u8]) -> Vec<&[u8]> {
    let mut at = 0;
    let mut out = Vec::new();
    while at + 4 <= capture.len() {
        let len = u32::from_be_bytes(capture[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        out.push(&capture[at..at + len]);
        at += len;
    }
    out
}

/// A source is whatever tells the senders apart, and a socket address is only
/// the one a socket gives. Here it is the index of a serial line.
#[test]
fn a_receiver_takes_a_key_that_is_not_an_address() {
    let capture = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/reference/udpm.bin"
    ))
    .expect("the fixture is in the repository");

    let mut receiver: MulticastReceiver<Everything, u8> = MulticastReceiver::new(Everything);
    let mut messages = Vec::new();
    for datagram in datagrams(&capture) {
        if let Some(whole) = receiver
            .on_datagram(3, datagram)
            .expect("the reference wrote it")
        {
            messages.push(whole);
        }
    }

    let named: Vec<(&str, usize)> = messages
        .iter()
        .map(|m| (m.frame.channel.as_str(), m.frame.payload.len()))
        .collect();
    assert_eq!(
        named,
        [
            ("/ref/short", 5),
            ("/ref/whole10k", 10_000),
            ("/ref/fragmented", 200_000),
        ]
    );
    assert_eq!(receiver.in_flight(), 0);
}

struct OneChannel(&'static str);

impl Filter for OneChannel {
    fn matches(&self, channel: &str) -> bool {
        channel == self.0
    }
}

/// A message no filter wants gives nothing back. One that arrives whole
/// costs no copy of its payload; one in fragments is put together first,
/// because only fragment zero carries the channel name.
#[test]
fn a_receiver_drops_what_its_filter_does_not_want() {
    let mut receiver: MulticastReceiver<OneChannel, u8> =
        MulticastReceiver::new(OneChannel("/keep/this"));

    let wanted = lcm_bus::wire::udpm::encode(
        1,
        FrameRef {
            channel: "/keep/this",
            payload: &[1, 2, 3],
        },
        1400,
    )
    .unwrap();
    let unwanted = lcm_bus::wire::udpm::encode(
        2,
        FrameRef {
            channel: "/drop/this",
            payload: &[4; 9_000],
        },
        1400,
    )
    .unwrap();

    let unwanted_whole = lcm_bus::wire::udpm::encode(
        3,
        FrameRef {
            channel: "/drop/this",
            payload: &[5, 6],
        },
        1400,
    )
    .unwrap();

    for datagram in unwanted.iter().chain(&unwanted_whole) {
        assert_eq!(receiver.on_datagram(1, datagram).unwrap(), None);
    }
    assert!(unwanted.len() > 1, "the large one had to fragment");
    assert_eq!(unwanted_whole.len(), 1, "and the small one had not");
    assert_eq!(receiver.in_flight(), 0, "nothing is held after either");

    let mut got = None;
    for datagram in &wanted {
        got = receiver.on_datagram(1, datagram).unwrap().or(got);
    }
    let got = got.expect("the wanted message");
    assert_eq!(
        got.frame,
        Frame {
            channel: "/keep/this".to_owned(),
            payload: vec![1, 2, 3],
        }
    );
    assert_eq!(got.sequence, 1);
    assert!(!got.reassembled, "it fitted in one datagram");
}

/// `ReadBuffer` takes bytes from anywhere, so a caller with no `std::io::Read`
/// can still find the frames in a stream.
#[test]
fn a_stream_can_be_framed_without_an_io_trait() {
    let mut wire = Vec::new();
    for i in 0..3u8 {
        wire.extend_from_slice(
            &tcpq::publish(FrameRef {
                channel: "/stream/one",
                payload: &[i; 700],
            })
            .unwrap(),
        );
    }

    let mut pending = ReadBuffer::new(64);
    let mut sent = 0;
    let mut frames = Vec::new();
    while sent < wire.len() || !pending.unread().is_empty() {
        // Seventeen bytes at a time, so a frame lands over many of them.
        let spare = pending.spare();
        let take = spare.len().min(17).min(wire.len() - sent);
        spare[..take].copy_from_slice(&wire[sent..sent + take]);
        pending.filled(take);
        sent += take;

        while let Decoded::Item(frame, used) = tcpq::decode(pending.unread()).unwrap() {
            let frame = frame.to_frame();
            pending.consume(used);
            frames.push(frame);
        }
        if take == 0 {
            break;
        }
    }

    assert_eq!(frames.len(), 3);
    for (i, frame) in frames.iter().enumerate() {
        assert_eq!(frame.channel, "/stream/one");
        assert_eq!(frame.payload, vec![i as u8; 700]);
    }
}

/// The send half needs no socket either: `fragments` gives the head and a
/// slice of the caller's payload, and copies nothing.
#[test]
fn a_message_can_be_framed_for_sending_without_a_copy() {
    use lcm_bus::wire::udpm;

    let payload: Vec<u8> = (0..50_000u32).map(|i| i as u8).collect();
    let frame = FrameRef {
        channel: "/send/one",
        payload: &payload,
    };

    let pieces = udpm::fragments(7, frame, 1400).unwrap();
    assert_eq!(
        pieces.len(),
        pieces.clone().count(),
        "the count is known up front"
    );

    let mut sent = Vec::new();
    for (head, body) in pieces {
        // A caller writes these two, in this order, to whatever it has.
        let mut datagram = head.as_ref().to_vec();
        datagram.extend_from_slice(body);
        sent.push(datagram);
    }
    assert_eq!(sent, udpm::encode(7, frame, 1400).unwrap());

    // And what comes out the other end is the message that went in.
    let mut receiver: MulticastReceiver<Everything, u8> = MulticastReceiver::new(Everything);
    let mut whole = None;
    for datagram in &sent {
        whole = receiver.on_datagram(1, datagram).unwrap().or(whole);
    }
    let whole = whole.expect("the message completes");
    assert_eq!(whole.frame.channel, "/send/one");
    assert_eq!(whole.frame.payload, payload);
    assert_eq!(whole.sequence, 7, "the number its sender gave it");
    assert!(whole.reassembled, "and it took more than one datagram");
}

/// The matcher this crate ships is the one a caller with its own bus wants,
/// and it was reachable only through a lock that needs `std`.
#[test]
fn the_subscriptions_of_this_crate_are_a_filter() {
    use lcm_bus::Subscriptions;

    let mut subscriptions = Subscriptions::new();
    subscriptions.add_name("/keep/this").unwrap();

    let mut receiver: MulticastReceiver<Subscriptions, u8> = MulticastReceiver::new(subscriptions);

    let wanted = udpm::encode(
        1,
        FrameRef {
            channel: "/keep/this",
            payload: &[1, 2, 3],
        },
        1400,
    )
    .unwrap();
    let unwanted = udpm::encode(
        2,
        FrameRef {
            channel: "/drop/this",
            payload: &[4],
        },
        1400,
    )
    .unwrap();

    assert_eq!(receiver.on_datagram(1, &unwanted[0]).unwrap(), None);
    let got = receiver
        .on_datagram(1, &wanted[0])
        .unwrap()
        .expect("wanted");
    assert_eq!(got.frame.channel, "/keep/this");
}
