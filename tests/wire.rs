//! LCM framing. These tests compare the bytes with the published layouts.

use std::net::SocketAddr;

use lcm_bus::wire::{
    Decoded, Frame, FrameRef, MAX_CHANNEL_LEN, MAX_MESSAGE_LEN, WireError, log, tcpq, udpm,
};

fn frame(channel: &str, payload: Vec<u8>) -> Frame {
    Frame {
        channel: channel.to_owned(),
        payload,
    }
}

fn peer(host: u8) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, host], 7667))
}

/// One message, one source, and the fragments in sequence.
fn assemble(datagrams: &[Vec<u8>]) -> Option<Frame> {
    let mut reassembler = udpm::Reassembler::new();
    let mut done = None;
    for datagram in datagrams {
        let udpm::Datagram::Fragment(f) = udpm::decode(datagram).unwrap() else {
            panic!("expected a fragment")
        };
        if let Some(frame) = reassembler.feed(peer(1), f).unwrap() {
            done = Some(frame);
        }
    }
    assert_eq!(reassembler.in_flight(), 0, "nothing left behind");
    done
}

/// The LCM datagram limits. The platform changes two of them.
#[test]
fn the_datagram_limits_are_the_ones_lcm_uses() {
    assert_eq!(udpm::SHORT_MESSAGE_MAX_APPLE, 1435);
    assert_eq!(udpm::fragment_max(udpm::SHORT_MESSAGE_MAX_APPLE), 1423);
    assert_eq!(udpm::MAX_FRAGMENT_BUFFERS, 1000);
    assert_eq!(udpm::MAX_FRAGMENT_BYTES, 1 << 24);

    if !cfg!(target_os = "macos") {
        assert_eq!(udpm::SHORT_MESSAGE_MAX, 65_499);
        assert_eq!(udpm::fragment_max(udpm::SHORT_MESSAGE_MAX), 65_487);
    }
}

/// The bytes LCM writes for a short message.
#[test]
fn a_short_message_has_the_documented_layout() {
    let encoded = udpm::encode(7, frame("POSE", vec![1, 2, 3]).view(), 65_499).unwrap();
    assert_eq!(encoded.len(), 1, "it fits in one datagram");

    assert_eq!(
        encoded[0],
        vec![
            0x4c, 0x43, 0x30, 0x32, // "LC02"
            0, 0, 0, 7, // sequence
            b'P', b'O', b'S', b'E', 0, // channel, NUL-terminated
            1, 2, 3, // payload
        ]
    );
}

#[test]
fn a_short_message_round_trips() {
    let original = frame("/example/one", vec![9; 200]);
    let encoded = udpm::encode(42, original.view(), 65_499).unwrap();

    match udpm::decode(&encoded[0]).unwrap() {
        udpm::Datagram::Whole { sequence, frame } => {
            assert_eq!(sequence, 42);
            assert_eq!(frame.to_frame(), original);
        }
        other => panic!("expected a whole message, got {other:?}"),
    }
}

#[test]
fn an_empty_payload_is_legal() {
    let encoded = udpm::encode(1, frame("EMPTY", vec![]).view(), 65_499).unwrap();
    match udpm::decode(&encoded[0]).unwrap() {
        udpm::Datagram::Whole { frame, .. } => {
            assert_eq!(frame.channel, "EMPTY");
            assert!(frame.payload.is_empty());
        }
        other => panic!("got {other:?}"),
    }
}

/// A message of `short_max` bytes goes in one datagram.
/// One more byte goes in fragments.
#[test]
fn the_short_message_limit_counts_the_channel_name() {
    let head = "/bus/x".len() + 1;
    let fits = frame("/bus/x", vec![0; 500 - head]);
    let one_over = frame("/bus/x", vec![0; 500 - head + 1]);

    assert_eq!(udpm::encode(1, fits.view(), 500).unwrap().len(), 1);
    assert!(udpm::encode(1, one_over.view(), 500).unwrap().len() > 1);
}

#[test]
fn a_foreign_datagram_is_rejected_by_its_magic() {
    let mut bytes = udpm::encode(1, frame("C", vec![1]).view(), 65_499).unwrap()[0].clone();
    bytes[0..4].copy_from_slice(b"XXXX");
    assert!(matches!(udpm::decode(&bytes), Err(WireError::BadMagic(_))));
}

#[test]
fn a_truncated_datagram_is_rejected() {
    let bytes = udpm::encode(1, frame("C", vec![1, 2, 3]).view(), 65_499).unwrap()[0].clone();
    for cut in 0..8 {
        assert!(
            udpm::decode(&bytes[..cut]).is_err(),
            "a {cut}-byte datagram must not decode"
        );
    }
}

#[test]
fn an_unterminated_channel_is_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&udpm::MAGIC_SHORT.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(b"no terminator here");
    assert_eq!(udpm::decode(&bytes), Err(WireError::BadChannel));
}

/// The LCM channel name limit, which udpm applies when it writes a name and
/// when it reads one.
#[test]
fn a_channel_name_above_the_limit_is_rejected() {
    let long = "c".repeat(MAX_CHANNEL_LEN + 1);
    assert_eq!(
        udpm::encode(1, frame(&long, vec![1]).view(), 65_499),
        Err(WireError::ChannelTooLong(MAX_CHANNEL_LEN + 1))
    );

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&udpm::MAGIC_SHORT.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(long.as_bytes());
    bytes.push(0);
    assert_eq!(
        udpm::decode(&bytes),
        Err(WireError::ChannelTooLong(MAX_CHANNEL_LEN + 1))
    );

    let at_limit = "c".repeat(MAX_CHANNEL_LEN);
    assert!(udpm::encode(1, frame(&at_limit, vec![1]).view(), 65_499).is_ok());
}

/// A NUL in the name makes it short on the wire.
#[test]
fn a_channel_name_with_a_nul_is_rejected() {
    assert_eq!(
        udpm::encode(1, frame("a\0b", vec![1]).view(), 65_499),
        Err(WireError::BadChannel)
    );
}

/// LCM writes `payload_size / fragment_size + !!(payload_size % fragment_size)`
/// fragments, where `payload_size` counts the channel name.
#[test]
fn the_number_of_fragments_is_the_one_lcm_computes() {
    let head = "/bus/x".len() + 1;
    for short_max in [100, 400, 1_000, udpm::SHORT_MESSAGE_MAX_APPLE] {
        for len in [1, 2_000, 100_000] {
            let datagrams =
                udpm::encode(1, frame("/bus/x", vec![0; len]).view(), short_max).unwrap();
            let expected = if head + len <= short_max {
                1
            } else {
                (head + len).div_ceil(udpm::fragment_max(short_max))
            };
            assert_eq!(datagrams.len(), expected, "{short_max} bytes, {len} bytes");
        }
    }
}

/// The fragment count is 16 bits, and LCM rejects more.
#[test]
fn a_message_of_too_many_fragments_is_rejected() {
    let head = "/bus/x".len() + 1;
    let payload = vec![0u8; 6_000_000];
    let expected = (head + payload.len()).div_ceil(udpm::fragment_max(100));
    assert!(expected > u16::MAX as usize);

    assert_eq!(
        udpm::encode(1, frame("/bus/x", payload).view(), 100),
        Err(WireError::TooManyFragments(expected))
    );
}

/// A name that one transport refuses and one more accepts lets a recorder
/// write an event that can never go back on a bus.
#[test]
fn all_encoders_refuse_the_same_channel_names() {
    let log_event = |channel: &str| {
        log::encode(log::Event {
            number: 0,
            timestamp: 0,
            frame: frame(channel, vec![1]).view(),
        })
    };
    for bad in ["", "with\0nul"] {
        let held = frame(bad, vec![1]);
        assert!(udpm::encode(1, held.view(), 1400).is_err(), "udpm {bad:?}");
        assert!(tcpq::publish(held.view()).is_err(), "tcpq {bad:?}");
        assert!(log_event(bad).is_err(), "log {bad:?}");
    }

    // A log holds a longer name than a datagram does, and says so.
    let long = "/".repeat(MAX_CHANNEL_LEN + 1);
    let held = frame(&long, vec![1]);
    assert!(udpm::encode(1, held.view(), 1400).is_err());
    assert!(tcpq::publish(held.view()).is_err());
    assert!(log_event(&long).is_ok());
}

/// The table of where each fragment wrote holds what came, and not room for
/// each fragment a sender says there will be. So it is kept in sequence and
/// searched, and a duplicate index, an index out of sequence, and an index
/// at each end must all go where they belong.
#[test]
fn fragments_out_of_order_and_repeated_still_make_one_message() {
    let mut reassembler = udpm::Reassembler::new();
    let payload: Vec<u8> = (0..40u8).collect();
    let count = 8u16;
    let each = 5usize;

    // Backwards, with each one sent two times.
    let mut done = None;
    for index in (0..count).rev() {
        for _ in 0..2 {
            let at = index as usize * each;
            let got = reassembler
                .feed(
                    peer(1),
                    udpm::Fragment {
                        sequence: 1,
                        total: payload.len() as u32,
                        offset: at as u32,
                        index,
                        count,
                        channel: (index == 0).then_some("/bus/x"),
                        payload: &payload[at..at + each],
                    },
                )
                .expect("consistent");
            done = got.or(done);
        }
    }

    let done = done.expect("the message completes");
    assert_eq!(done.channel, "/bus/x");
    assert_eq!(done.payload, payload, "each byte where its sender put it");
    // The message went when it completed, so the second send of the fragment
    // that completed it is the start of one more.
    assert_eq!(reassembler.in_flight(), 1);
}

/// The budget counts the bytes that came, so a length a sender claims and never sends
/// costs nothing and pushes nothing else out.
/// A budget that charged the claim instead let one datagram evict every message a bus
/// was putting together.
#[test]
fn a_claim_takes_none_of_the_fragment_budget() {
    let mut reassembler = udpm::Reassembler::new();
    for sequence in 0..500u32 {
        let _ = reassembler.feed(
            peer(1),
            udpm::Fragment {
                sequence,
                total: udpm::MAX_FRAGMENT_BYTES as u32,
                offset: 0,
                index: 0,
                count: 2,
                channel: Some("/bus/x"),
                payload: &[1],
            },
        );
    }
    assert_eq!(
        reassembler.in_flight(),
        500,
        "500 claims of the whole budget, and 500 bytes between them"
    );
}

/// The bytes that did come are held to the budget, so a sender that sends
/// them loses the messages it sent first.
#[test]
fn the_bytes_that_came_are_held_to_the_budget() {
    let mut reassembler = udpm::Reassembler::new();
    let each = 64 * 1024;
    let payload = vec![7u8; each];
    let enough = udpm::MAX_FRAGMENT_BYTES / each + 8;

    for sequence in 0..enough as u32 {
        let _ = reassembler.feed(
            peer(1),
            udpm::Fragment {
                sequence,
                total: each as u32 * 2,
                offset: 0,
                index: 0,
                count: 2,
                channel: Some("/bus/x"),
                payload: &payload,
            },
        );
    }
    assert!(
        reassembler.in_flight() * each <= udpm::MAX_FRAGMENT_BYTES,
        "{} messages of {each} bytes is above the budget",
        reassembler.in_flight()
    );
    assert!(reassembler.in_flight() > 0, "and some are kept");
}

/// A fragment that holds the channel name and nothing else moves no payload.
#[test]
fn a_datagram_that_cannot_hold_the_channel_and_a_byte_is_rejected() {
    assert_eq!(
        udpm::encode(1, frame("/bus/x", vec![0; 100]).view(), 19),
        Err(WireError::DatagramTooSmall {
            short_max: 19,
            minimum: 20
        })
    );
    // One more byte of datagram is one byte of payload in each fragment.
    assert!(udpm::encode(1, frame("/bus/x", vec![0; 100]).view(), 20).is_ok());
}

#[test]
fn a_large_message_fragments_and_reassembles() {
    let payload: Vec<u8> = (0..5_000u32).map(|i| (i % 251) as u8).collect();
    let original = frame("/example/big", payload);

    // A small limit, so the message divides into fragments.
    let datagrams = udpm::encode(11, original.view(), 1_000).unwrap();
    assert!(datagrams.len() > 4, "it must make some fragments");
    assert_eq!(assemble(&datagrams), Some(original));
}

#[test]
fn fragments_may_arrive_in_any_order() {
    let payload: Vec<u8> = (0..3_000u32).map(|i| i as u8).collect();
    let original = frame("/bus/big", payload);
    let mut datagrams = udpm::encode(3, original.view(), 500).unwrap();
    datagrams.reverse();

    assert_eq!(assemble(&datagrams), Some(original));
}

/// Multicast can send a datagram two times. The reassembler must count it one time.
#[test]
fn a_duplicated_fragment_does_not_complete_a_message_early() {
    let original = frame("/bus/dup", vec![7; 2_000]);
    let datagrams = udpm::encode(5, original.view(), 400).unwrap();
    assert!(datagrams.len() >= 5);

    let mut reassembler = udpm::Reassembler::new();

    for datagram in &datagrams[..datagrams.len() - 1] {
        for _ in 0..2 {
            let udpm::Datagram::Fragment(f) = udpm::decode(datagram).unwrap() else {
                panic!("expected a fragment")
            };
            assert_eq!(reassembler.feed(peer(1), f).unwrap(), None, "not yet whole");
        }
    }

    let udpm::Datagram::Fragment(f) = udpm::decode(datagrams.last().unwrap()).unwrap() else {
        panic!("expected a fragment")
    };
    assert_eq!(reassembler.feed(peer(1), f).unwrap(), Some(original));
}

#[test]
fn messages_in_flight_together_do_not_corrupt_each_other() {
    let a = frame("/bus/a", vec![0xAA; 1_500]);
    let b = frame("/bus/b", vec![0xBB; 1_500]);
    let da = udpm::encode(1, a.view(), 400).unwrap();
    let db = udpm::encode(2, b.view(), 400).unwrap();

    let mut reassembler = udpm::Reassembler::new();
    let mut done = Vec::new();

    for (x, y) in da.iter().zip(db.iter()) {
        for datagram in [x, y] {
            if let udpm::Datagram::Fragment(f) = udpm::decode(datagram).unwrap()
                && let Some(frame) = reassembler.feed(peer(1), f).unwrap()
            {
                done.push(frame);
            }
        }
    }

    assert_eq!(done.len(), 2);
    assert!(done.contains(&a));
    assert!(done.contains(&b));
}

/// All LCM peers number from zero, so two on one group share a sequence number.
/// The key also holds the source.
#[test]
fn two_peers_that_share_a_sequence_number_do_not_collide() {
    let a = frame("/bus/a", vec![0xAA; 1_500]);
    let b = frame("/bus/b", vec![0xBB; 1_500]);
    let da = udpm::encode(0, a.view(), 400).unwrap();
    let db = udpm::encode(0, b.view(), 400).unwrap();

    let mut reassembler = udpm::Reassembler::new();
    let mut done = Vec::new();

    for (x, y) in da.iter().zip(db.iter()) {
        for (source, datagram) in [(peer(1), x), (peer(2), y)] {
            if let udpm::Datagram::Fragment(f) = udpm::decode(datagram).unwrap()
                && let Some(frame) = reassembler.feed(source, f).unwrap()
            {
                done.push(frame);
            }
        }
    }

    assert_eq!(done.len(), 2, "one message from each peer");
    assert!(done.contains(&a));
    assert!(done.contains(&b));
}

#[test]
fn abandoned_messages_are_evicted() {
    let mut reassembler = udpm::Reassembler::new();

    for sequence in 0..(udpm::MAX_FRAGMENT_BUFFERS as u32 + 50) {
        let datagrams = udpm::encode(sequence, frame("/bus/x", vec![1; 900]).view(), 400).unwrap();
        if let udpm::Datagram::Fragment(f) = udpm::decode(&datagrams[0]).unwrap() {
            reassembler.feed(peer(1), f).unwrap();
        }
    }

    assert!(
        reassembler.in_flight() <= udpm::MAX_FRAGMENT_BUFFERS,
        "held {} partial messages, the limit is {}",
        reassembler.in_flight(),
        udpm::MAX_FRAGMENT_BUFFERS
    );
}

/// The reassembler must not write a fragment that goes after the end of its message.
#[test]
fn a_fragment_outside_its_message_is_rejected() {
    let mut reassembler = udpm::Reassembler::new();
    let bad = udpm::Fragment {
        sequence: 1,
        total: 100,
        offset: 90,
        index: 0,
        count: 2,
        channel: Some("/bus/x"),
        payload: &[0; 50], // 90 + 50 > 100
    };
    assert_eq!(
        reassembler.feed(peer(1), bad),
        Err(WireError::InconsistentFragment(1))
    );
}

#[test]
fn fragments_disagreeing_about_a_message_are_rejected() {
    let mut reassembler = udpm::Reassembler::new();
    let first = udpm::Fragment {
        sequence: 9,
        total: 100,
        offset: 0,
        index: 0,
        count: 2,
        channel: Some("/bus/x"),
        payload: &[0; 50],
    };
    assert_eq!(reassembler.feed(peer(1), first).unwrap(), None);

    // Same sequence number, different shape.
    let second = udpm::Fragment {
        sequence: 9,
        total: 200,
        offset: 50,
        index: 1,
        count: 2,
        channel: None,
        payload: &[0; 50],
    };
    assert_eq!(
        reassembler.feed(peer(1), second),
        Err(WireError::InconsistentFragment(9))
    );
    assert_eq!(reassembler.in_flight(), 0, "LCM removes what it had");
}

/// A fragment holds one byte or more.
/// A message of `n` bytes has no more than `n` fragments.
/// A source that gives a larger number gets a `seen` array too large.
#[test]
fn more_fragments_than_bytes_is_rejected() {
    let mut reassembler = udpm::Reassembler::new();
    let lie = udpm::Fragment {
        sequence: 1,
        total: 100,
        offset: 0,
        index: 0,
        count: u16::MAX,
        channel: Some("/bus/x"),
        payload: &[0; 4],
    };
    assert_eq!(
        reassembler.feed(peer(1), lie),
        Err(WireError::InconsistentFragment(1))
    );
    assert_eq!(reassembler.in_flight(), 0, "and nothing is held");
}

/// Only fragment zero holds the channel name.
/// Without it the reassembler has no frame to give.
#[test]
fn a_message_missing_its_first_fragment_is_rejected() {
    let mut reassembler = udpm::Reassembler::new();
    for index in 1..=2u16 {
        let f = udpm::Fragment {
            sequence: 4,
            total: 30,
            offset: (index as u32 - 1) * 15,
            index,
            count: 3,
            channel: None,
            payload: &[0; 15],
        };
        reassembler.feed(peer(1), f).unwrap();
    }

    // All the fragments are here, but none gave a channel name.
    let f = udpm::Fragment {
        sequence: 4,
        total: 30,
        offset: 0,
        index: 0,
        count: 3,
        channel: None,
        payload: &[],
    };
    assert_eq!(
        reassembler.feed(peer(1), f),
        Err(WireError::InconsistentFragment(4))
    );
}

/// The LCM message limit, which the decoder and the reassembler apply
/// before they make a buffer.
#[test]
fn a_message_above_the_size_limit_is_rejected_before_allocating() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&udpm::MAGIC_LONG.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes()); // sequence
    bytes.extend_from_slice(&u32::MAX.to_be_bytes()); // msg_size
    bytes.extend_from_slice(&0u32.to_be_bytes()); // offset
    bytes.extend_from_slice(&0u16.to_be_bytes()); // index
    bytes.extend_from_slice(&1u16.to_be_bytes()); // count

    assert_eq!(
        udpm::decode(&bytes),
        Err(WireError::MessageTooLarge(u32::MAX as usize))
    );

    let huge = udpm::Fragment {
        sequence: 1,
        total: u32::MAX,
        offset: 0,
        index: 0,
        count: 1,
        channel: Some("/bus/x"),
        payload: &[0; 4],
    };
    assert_eq!(
        udpm::Reassembler::new().feed(peer(1), huge),
        Err(WireError::MessageTooLarge(u32::MAX as usize))
    );
}

#[test]
fn the_handshake_matches_the_relay_protocol() {
    assert_eq!(
        tcpq::handshake(),
        vec![0x28, 0x76, 0x17, 0xfb, 0x00, 0x00, 0x01, 0x00]
    );

    let mut reply = Vec::new();
    reply.extend_from_slice(&tcpq::MAGIC_SERVER.to_be_bytes());
    reply.extend_from_slice(&tcpq::PROTOCOL_VERSION.to_be_bytes());
    assert_eq!(tcpq::check_handshake(&reply).unwrap(), 0x0100);
}

/// LCM reads the version of the server and does not compare it.
#[test]
fn a_relay_of_another_version_is_accepted() {
    let mut reply = Vec::new();
    reply.extend_from_slice(&tcpq::MAGIC_SERVER.to_be_bytes());
    reply.extend_from_slice(&0x0200u32.to_be_bytes());
    assert_eq!(tcpq::check_handshake(&reply).unwrap(), 0x0200);
}

#[test]
fn a_server_that_is_not_a_relay_is_rejected() {
    let mut reply = Vec::new();
    reply.extend_from_slice(b"HTTP");
    reply.extend_from_slice(&0u32.to_be_bytes());
    assert!(matches!(
        tcpq::check_handshake(&reply),
        Err(WireError::BadMagic(_))
    ));
}

#[test]
fn a_publish_frame_round_trips() {
    let original = frame("/example/one", vec![1, 2, 3, 4]);
    let bytes = tcpq::publish(original.view()).unwrap();

    let (decoded, consumed) = tcpq::decode(&bytes).unwrap().item().expect("a whole frame");
    assert_eq!(decoded.to_frame(), original);
    assert_eq!(consumed, bytes.len(), "the frame is exactly this long");
}

/// A prefix is not a frame, and the answer says how many bytes would make
/// it one. A reader that only heard "not yet" has to guess, and guesses by
/// doubling whatever it holds.
#[test]
fn a_partial_frame_says_how_much_more_it_needs() {
    let bytes = tcpq::publish(frame("/bus/x", vec![7; 100]).view()).unwrap();
    for cut in 0..bytes.len() {
        let Decoded::Need(needs) = tcpq::decode(&bytes[..cut]).unwrap() else {
            panic!("a {cut}-byte prefix is not whole");
        };
        assert!(needs > cut, "a {cut}-byte prefix needs more than it has");
        assert!(needs <= bytes.len(), "and never more than the frame");
    }
    assert_eq!(
        tcpq::decode(&bytes).unwrap(),
        Decoded::Item(frame("/bus/x", vec![7; 100]).view(), bytes.len())
    );
}

/// A relay that sends a name this crate will not publish costs one message.
/// C and Java relay whatever a publisher gives them, so a 64-byte name from
/// each peer on the relay reaches here in the normal course of things.
#[test]
fn a_frame_this_crate_will_not_take_is_stepped_over() {
    let mut stream = Vec::new();
    let long = "/".repeat(MAX_CHANNEL_LEN + 1);
    stream.extend_from_slice(&tcpq::MESSAGE_TYPE_PUBLISH.to_be_bytes());
    stream.extend_from_slice(&(long.len() as u32).to_be_bytes());
    stream.extend_from_slice(long.as_bytes());
    stream.extend_from_slice(&3u32.to_be_bytes());
    stream.extend_from_slice(&[1, 2, 3]);
    let over = stream.len();
    let good = frame("/bus/x", vec![9]);
    stream.extend_from_slice(&tcpq::publish(good.view()).unwrap());

    assert_eq!(tcpq::decode(&stream).unwrap(), Decoded::Skip(over));
    let (next, _) = tcpq::decode(&stream[over..]).unwrap().item().unwrap();
    assert_eq!(next.to_frame(), good, "the frame after it still reads");

    // A name this long is no name at all, and the stream cannot be followed.
    let mut absurd = Vec::new();
    absurd.extend_from_slice(&tcpq::MESSAGE_TYPE_PUBLISH.to_be_bytes());
    absurd.extend_from_slice(&(tcpq::CHANNEL_READ_MAX as u32 + 1).to_be_bytes());
    assert_eq!(
        tcpq::decode(&absurd),
        Err(WireError::ChannelTooLong(tcpq::CHANNEL_READ_MAX + 1))
    );
}

#[test]
fn frames_are_read_one_at_a_time_from_a_stream() {
    let a = frame("/bus/a", vec![1]);
    let b = frame("/bus/b", vec![2, 3]);

    let mut stream = tcpq::publish(a.view()).unwrap();
    stream.extend_from_slice(&tcpq::publish(b.view()).unwrap());

    let (first, consumed) = tcpq::decode(&stream).unwrap().item().unwrap();
    assert_eq!(first.to_frame(), a);

    let (second, _) = tcpq::decode(&stream[consumed..]).unwrap().item().unwrap();
    assert_eq!(second.to_frame(), b);
}

#[test]
fn a_subscription_carries_its_pattern() {
    let pattern = "/example/.*";
    let bytes = tcpq::subscribe(pattern);
    assert_eq!(&bytes[..4], &tcpq::MESSAGE_TYPE_SUBSCRIBE.to_be_bytes());
    assert_eq!(&bytes[4..8], &(pattern.len() as u32).to_be_bytes());
    assert_eq!(&bytes[8..], pattern.as_bytes());

    let bytes = tcpq::unsubscribe("x");
    assert_eq!(&bytes[..4], &tcpq::MESSAGE_TYPE_UNSUBSCRIBE.to_be_bytes());
}

/// A relay sends a publish frame and nothing else.
/// After a frame of a different type, this client cannot find the next one.
#[test]
fn a_frame_that_is_not_a_publish_is_rejected() {
    let bytes = tcpq::subscribe("/bus/x");
    assert_eq!(
        tcpq::decode(&bytes),
        Err(WireError::UnknownFrameType(tcpq::MESSAGE_TYPE_SUBSCRIBE))
    );
}

/// A stream decoder reads a length before the bytes it counts, so a length it
/// takes is a length the reader then waits for. A name bounded by the largest
/// message there is would have the reader hold 256 MiB for a channel name.
#[test]
fn an_absurd_length_from_a_relay_is_rejected() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&tcpq::MESSAGE_TYPE_PUBLISH.to_be_bytes());
    bytes.extend_from_slice(&(200u32 * 1024 * 1024).to_be_bytes());
    bytes.extend_from_slice(&[b'x'; 40]);
    assert_eq!(
        tcpq::decode(&bytes),
        Err(WireError::ChannelTooLong(200 * 1024 * 1024)),
        "and not `Ok(None)`, which asks the reader for 200 MiB more"
    );

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&tcpq::MESSAGE_TYPE_PUBLISH.to_be_bytes());
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        tcpq::decode(&bytes),
        Err(WireError::ChannelTooLong(u32::MAX as usize))
    );

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&tcpq::MESSAGE_TYPE_PUBLISH.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.push(b'x');
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        tcpq::decode(&bytes),
        Err(WireError::MessageTooLarge(u32::MAX as usize))
    );
}

// --- log files ---------------------------------------------------------------

fn event<'a>(number: i64, timestamp: i64, channel: &'a str, payload: &'a [u8]) -> log::Event<'a> {
    log::Event {
        number,
        timestamp,
        frame: FrameRef { channel, payload },
    }
}

/// The published header: a sync word, an event number, a timestamp, and the
/// two lengths, in 28 big-endian bytes.
#[test]
fn a_log_event_has_the_documented_layout() {
    let bytes = log::encode(event(7, 0x0102_0304_0506_0708, "AB", &[9, 9])).unwrap();
    assert_eq!(
        bytes,
        vec![
            0xed, 0xa1, 0xda, 0x01, // sync word
            0, 0, 0, 0, 0, 0, 0, 7, // event number
            1, 2, 3, 4, 5, 6, 7, 8, // timestamp
            0, 0, 0, 2, // channel length
            0, 0, 0, 2, // data length
            b'A', b'B', // channel, with no terminator
            9, 9, // data
        ]
    );
    assert_eq!(bytes.len(), log::HEADER_LEN + 4);
}

#[test]
fn a_log_event_round_trips() {
    let original = event(3, 1_700_000_000_000_000, "/example/one", &[1, 2, 3]);
    let bytes = log::encode(original).unwrap();

    let (decoded, consumed) = log::decode(&bytes).unwrap().item().expect("a whole event");
    assert_eq!(decoded, original);
    assert_eq!(consumed, bytes.len());
}

#[test]
fn a_partial_log_event_says_how_much_more_it_needs() {
    let bytes = log::encode(event(1, 2, "/bus/x", &[7; 40])).unwrap();
    for cut in 4..bytes.len() {
        let Decoded::Need(needs) = log::decode(&bytes[..cut]).unwrap() else {
            panic!("{cut} bytes is not a whole event");
        };
        assert!(
            needs > cut && needs <= bytes.len(),
            "{cut} bytes needs {needs}"
        );
    }
    assert!(log::decode(&bytes).unwrap().item().is_some());
}

/// A log channel name is 1 to 999 bytes, which is not the udpm limit.
#[test]
fn a_log_channel_name_keeps_to_its_own_limits() {
    assert_eq!(
        log::encode(event(1, 2, "", &[])),
        Err(WireError::BadChannel)
    );

    let long = "c".repeat(1_000);
    assert_eq!(
        log::encode(event(1, 2, &long, &[])),
        Err(WireError::ChannelTooLong(1_000))
    );
    // 999 is legal here, where udpm stops at 63.
    let at_limit = "c".repeat(999);
    assert!(log::encode(event(1, 2, &at_limit, &[])).is_ok());
    assert!(at_limit.len() > MAX_CHANNEL_LEN);
}

/// A negative length on the wire reads here as a large number, and a limit
/// catches it.
#[test]
fn a_negative_length_in_a_log_is_rejected() {
    let mut bytes = log::encode(event(1, 2, "/bus/x", &[1])).unwrap();
    bytes[24..28].copy_from_slice(&(-1i32).to_be_bytes());
    assert_eq!(
        log::decode(&bytes),
        Err(WireError::MessageTooLarge(u32::MAX as usize))
    );
}

/// The budget is for every message together, and a limit on one message is
/// the limit of the protocol. This crate's own encoder writes up to that, so
/// a smaller limit here would refuse what it sends.
#[test]
fn one_message_may_be_larger_than_the_budget_for_all_of_them() {
    let mut reassembler = udpm::Reassembler::new();
    let claim = |total: u32| udpm::Fragment {
        sequence: 1,
        total,
        offset: 0,
        index: 0,
        count: 2,
        channel: Some("/bus/x"),
        payload: &[0; 8],
    };

    let over_the_budget = udpm::MAX_FRAGMENT_BYTES as u32 + 1;
    assert!(reassembler.feed(peer(1), claim(over_the_budget)).is_ok());

    let over_the_protocol = MAX_MESSAGE_LEN as u32 + 1;
    assert_eq!(
        reassembler.feed(peer(2), claim(over_the_protocol)),
        Err(WireError::MessageTooLarge(over_the_protocol as usize))
    );
}

/// An index for all fragments does not give a sender to all bytes. Two
/// fragments on one range keep a second range at zero, and a zero is not data.
#[test]
fn fragments_that_do_not_cover_the_message_are_rejected() {
    let mut reassembler = udpm::Reassembler::new();
    let mut last = Ok(None);
    for index in 0..3u16 {
        last = reassembler.feed(
            peer(1),
            udpm::Fragment {
                sequence: 1,
                total: 30,
                offset: 0,
                index,
                count: 3,
                channel: if index == 0 { Some("/bus/x") } else { None },
                payload: &[0xAA; 10],
            },
        );
    }
    assert_eq!(last, Err(WireError::InconsistentFragment(1)));
    assert_eq!(reassembler.in_flight(), 0);
}

/// A 32-bit `usize` wraps on `offset + payload`, and the low result then gets
/// through a test against the length of the message.
#[test]
fn an_offset_that_wraps_a_32_bit_usize_is_rejected() {
    let mut reassembler = udpm::Reassembler::new();
    let wrapping = udpm::Fragment {
        sequence: 1,
        total: 100,
        offset: u32::MAX - 5,
        index: 0,
        count: 2,
        channel: Some("/bus/x"),
        payload: &[0; 10],
    };
    assert_eq!(
        reassembler.feed(peer(1), wrapping),
        Err(WireError::InconsistentFragment(1))
    );
}

/// A log reader slides to the next sync word, so a torn event does not cost
/// the ones after it.
#[test]
fn a_torn_log_resyncs_on_the_next_event() {
    let good = log::encode(event(2, 20, "/bus/b", &[2])).unwrap();
    let mut stream = vec![0xde, 0xad, 0xbe, 0xef, 0xed, 0xa1, 0x00];
    stream.extend_from_slice(&good);

    assert!(log::decode(&stream).is_err(), "the head is not an event");
    let at = log::resync(&stream).expect("a sync word is in there");
    let (decoded, _) = log::decode(&stream[at..]).unwrap().item().unwrap();
    assert_eq!(decoded.frame.channel, "/bus/b");

    assert_eq!(log::resync(&[0; 32]), None);
}

/// A sender picks the length of a message and the sequence its fragments
/// come in. The two must not cost this more than the sender paid: a claim
/// of 16 MiB from one 23-byte datagram, and fragments that come backwards,
/// were each a means to buy much work with few bytes.
#[test]
fn a_sender_pays_for_the_work_it_makes() {
    let mut reassembler = udpm::Reassembler::new();

    // One datagram, claiming the largest message there can be.
    let started = std::time::Instant::now();
    for sequence in 0..2_000u32 {
        let _ = reassembler.feed(
            peer(1),
            udpm::Fragment {
                sequence,
                total: udpm::MAX_FRAGMENT_BYTES as u32,
                offset: 0,
                index: 0,
                count: 2,
                channel: Some("/bus/x"),
                payload: &[1],
            },
        );
    }
    let claims = started.elapsed();
    assert!(
        claims < std::time::Duration::from_millis(500),
        "2000 claims of 16 MiB took {claims:?}"
    );

    // The same message two times, forwards and backwards.
    let count = 4_096u16;
    let each = 8usize;
    let payload = vec![7u8; count as usize * each];
    let feed = |reassembler: &mut udpm::Reassembler, sequence: u32, backwards: bool| {
        let order: Vec<u16> = match backwards {
            true => (0..count).rev().collect(),
            false => (0..count).collect(),
        };
        let started = std::time::Instant::now();
        let mut done = None;
        for index in order {
            let at = index as usize * each;
            done = reassembler
                .feed(
                    peer(2),
                    udpm::Fragment {
                        sequence,
                        total: payload.len() as u32,
                        offset: at as u32,
                        index,
                        count,
                        channel: (index == 0).then_some("/bus/x"),
                        payload: &payload[at..at + each],
                    },
                )
                .expect("consistent")
                .or(done);
        }
        (done.expect("completes"), started.elapsed())
    };

    let (forwards, quick) = feed(&mut reassembler, 1, false);
    let (backwards, slow) = feed(&mut reassembler, 2, true);
    assert_eq!(forwards.payload, payload);
    assert_eq!(
        backwards.payload, payload,
        "and the same message either way"
    );
    assert!(
        slow < quick * 8,
        "backwards took {slow:?} against {quick:?} forwards"
    );
}

/// A subscription names a channel, and a name no channel can have matches
/// nothing whatever it is compared against. A log holds the longest names
/// there are, so that is the limit a subscription keeps.
#[test]
fn a_subscription_by_name_refuses_a_name_no_channel_has() {
    let mut subscriptions = lcm_bus::Subscriptions::new();

    assert!(subscriptions.add_name("").is_err(), "empty");
    assert!(subscriptions.add_name("with\0nul").is_err(), "a NUL in it");
    assert!(
        subscriptions
            .add_name(&"c".repeat(log::CHANNEL_MAX + 1))
            .is_err(),
        "longer than a log holds"
    );

    // A name a log holds and a bus does not is still a name.
    let in_a_log = "c".repeat(MAX_CHANNEL_LEN + 1);
    assert!(subscriptions.add_name(&in_a_log).is_ok());
    assert!(subscriptions.matches(&in_a_log));
    assert!(!subscriptions.matches(""));
}

/// A fragment with no payload is a legal datagram, and one that costs no
/// bytes takes no part of a budget counted in bytes and keeps a place in the
/// table of what came all the same. A thousand messages of those places is
/// gigabytes a budget of 16 MiB says nothing about.
#[test]
fn a_fragment_with_no_payload_still_takes_part_of_the_budget() {
    let mut reassembler = udpm::Reassembler::new();
    for message in 0..200u32 {
        for index in 1..2_000u16 {
            let _ = reassembler.feed(
                peer((message % 250) as u8 + 1),
                udpm::Fragment {
                    sequence: message,
                    total: udpm::MAX_FRAGMENT_BYTES as u32,
                    offset: 0,
                    index,
                    count: u16::MAX,
                    channel: None,
                    payload: &[],
                },
            );
        }
    }

    // Sixteen bytes for a place is below what one really takes, so this is
    // the floor of what the budget has to have kept back.
    let places = reassembler.in_flight() * 1_999;
    assert!(
        places * 16 <= udpm::MAX_FRAGMENT_BYTES,
        "{} messages of 1999 places each is above the budget",
        reassembler.in_flight()
    );
    assert!(reassembler.in_flight() > 0, "and some are kept");
}

/// A message says how long it is, and an end that takes room for that before
/// the bytes are known lets one 23-byte datagram get 256 MiB — which a host
/// with a limit on its address space answers by stopping everything.
#[test]
fn a_completion_takes_room_for_the_bytes_that_came() {
    let mut reassembler = udpm::Reassembler::new();
    // `count` of one completes on the first fragment.
    let claim = udpm::Fragment {
        sequence: 1,
        total: MAX_MESSAGE_LEN as u32 - 1,
        offset: 0,
        index: 0,
        count: 1,
        channel: Some("/bus/x"),
        payload: b"A",
    };
    assert_eq!(
        reassembler.feed(peer(1), claim),
        Err(WireError::InconsistentFragment(1)),
        "one byte is not a message of 256 MiB"
    );
    assert_eq!(reassembler.in_flight(), 0);
}

/// A message larger than the budget is the oldest of the one message there
/// is, so a reassembler that drops the oldest to make room drops the message
/// it is building — on each fragment above the budget, for good, with no
/// error and nothing delivered. The budget is for all of them together and
/// was never a limit on one.
#[test]
fn a_message_larger_than_the_budget_does_not_evict_itself() {
    let over = udpm::MAX_FRAGMENT_BYTES + 1;
    let payload = vec![7u8; over];
    let frame = FrameRef {
        channel: "/bus/big",
        payload: &payload,
    };

    let mut reassembler = udpm::Reassembler::new();
    let mut done = None;
    for datagram in udpm::encode(1, frame, 1400).unwrap() {
        let Ok(udpm::Datagram::Fragment(fragment)) = udpm::decode(&datagram) else {
            panic!("what encode wrote");
        };
        done = reassembler
            .feed(peer(1), fragment)
            .expect("consistent")
            .or(done);
    }

    let done = done.expect("a message alone is not one to drop");
    assert_eq!(done.payload.len(), over);
    assert_eq!(reassembler.in_flight(), 0);
    assert_eq!(reassembler.evicted(), 0, "and nothing was dropped");
}

/// A log gives the length of a channel name, so a NUL can be in one, and so
/// can a byte that is not text. C reads that name with `strlen` and stops at
/// the NUL, so the two do not agree on which channel the event is for.
///
/// `Skip` and not `Err`: the length of the event is known, and the sync word
/// behind it agrees. An `Err` tells the reader the chain is lost, and the
/// reader then looks for the next sync word — which is in this event's own
/// payload, where a publisher put it.
#[test]
fn a_log_channel_name_this_crate_cannot_read_is_a_skip() {
    let event = |channel: &[u8], payload: &[u8]| {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0xEDA1_DA01u32.to_be_bytes());
        bytes.extend_from_slice(&0i64.to_be_bytes());
        bytes.extend_from_slice(&0i64.to_be_bytes());
        bytes.extend_from_slice(&(channel.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(channel);
        bytes.extend_from_slice(payload);
        bytes
    };

    let with_a_nul = event(b"/good\0evil", &[7]);
    assert_eq!(
        log::decode(&with_a_nul),
        Ok(Decoded::Skip(with_a_nul.len()))
    );

    let not_text = event(&[0xff, 0xfe], &[7]);
    assert_eq!(log::decode(&not_text), Ok(Decoded::Skip(not_text.len())));

    // And the length it gives back is the event, so the reader takes up on
    // the one behind it.
    let mut two = event(b"/good\0evil", &[7]);
    let good = log::encode(log::Event {
        number: 1,
        timestamp: 5,
        frame: FrameRef {
            channel: "/behind",
            payload: &[9],
        },
    })
    .unwrap();
    let skip = two.len();
    two.extend_from_slice(&good);
    assert_eq!(log::decode(&two), Ok(Decoded::Skip(skip)));
    let (event, _) = log::decode(&two[skip..]).unwrap().item().unwrap();
    assert_eq!(event.frame.channel, "/behind");
}
