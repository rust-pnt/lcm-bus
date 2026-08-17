//! Random bytes and almost correct bytes, through all the decoders.
//! A panic on the reader thread stops a bus.
//! A decoder must give an error and not one.
#![cfg(feature = "std")]

extern crate alloc;

use alloc::string::String;

use std::net::SocketAddr;

use lcm_bus::wire::{FrameRef, log, tcpq, udpm};

/// xorshift64*, so a test that stops here repeats with the same seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

fn peer(host: u8) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, host], 7667))
}

fn feed(bytes: &[u8], reassembler: &mut udpm::Reassembler) {
    if let Ok(udpm::Datagram::Fragment(f)) = udpm::decode(bytes) {
        let _ = reassembler.feed(peer(1), f);
    }
    let _ = tcpq::decode(bytes);
    let _ = log::decode(bytes);
    let _ = log::resync(bytes);
}

#[test]
fn random_bytes_never_panic_a_decoder() {
    let mut rng = Rng(0x1234_5678_9abc_def0);
    let mut reassembler = udpm::Reassembler::new();
    for _ in 0..200_000 {
        let len = rng.below(80);
        let bytes = rng.bytes(len);
        feed(&bytes, &mut reassembler);
    }
}

/// Correct frames with one byte changed get much deeper than random bytes.
#[test]
fn mutated_frames_never_panic_a_decoder() {
    let mut rng = Rng(0xfeed_face_dead_beef);
    let mut reassembler = udpm::Reassembler::new();

    let payload = vec![7u8; 3_000];
    let frame = FrameRef {
        channel: "/bus/x",
        payload: &payload,
    };
    let mut seeds: Vec<Vec<u8>> = udpm::encode(9, frame, 500).unwrap();
    seeds.extend(udpm::encode(9, frame, 65_499).unwrap());
    seeds.push(tcpq::publish(frame).unwrap());
    seeds.push(tcpq::subscribe("/bus/.*"));
    seeds.push(
        log::encode(log::Event {
            number: 3,
            timestamp: 1_700_000_000_000_000,
            frame,
        })
        .unwrap(),
    );

    for _ in 0..200_000 {
        let mut bytes = seeds[rng.below(seeds.len())].clone();
        for _ in 0..1 + rng.below(4) {
            if bytes.is_empty() {
                break;
            }
            let at = rng.below(bytes.len());
            bytes[at] = rng.next() as u8;
        }
        if rng.below(4) == 0 {
            bytes.truncate(rng.below(bytes.len().max(1)));
        }
        feed(&bytes, &mut reassembler);
    }
}

/// A fragmented message must come back byte for byte, whatever the sequence
/// of its datagrams. A reassembler that makes up a byte fails here.
#[test]
fn a_fragmented_message_survives_any_arrival_order() {
    let mut rng = Rng(0x0bad_c0de_0dd1_e5a1);
    let mut reassembler = udpm::Reassembler::new();

    for round in 0..2_000u32 {
        let len = 1 + rng.below(60_000);
        let payload = rng.bytes(len);
        let channel = alloc::format!("/bus/{}", rng.below(1_000));
        let short_max = 64 + rng.below(2_000);
        let frame = FrameRef {
            channel: &channel,
            payload: &payload,
        };

        let mut datagrams = udpm::encode(round, frame, short_max).expect("encodable");
        for i in (1..datagrams.len()).rev() {
            datagrams.swap(i, rng.below(i + 1));
        }

        let mut whole = None;
        for datagram in &datagrams {
            match udpm::decode(datagram).expect("what encode wrote") {
                udpm::Datagram::Whole { frame, .. } => whole = Some(frame.to_frame()),
                udpm::Datagram::Fragment(f) => {
                    if let Some(done) = reassembler.feed(peer(1), f).expect("consistent") {
                        whole = Some(done);
                    }
                }
            }
        }

        let whole = whole.expect("the message completes");
        assert_eq!(whole.channel, channel);
        assert_eq!(whole.payload, payload, "round {round}");
        assert_eq!(reassembler.in_flight(), 0, "nothing is left over");
    }
}

/// Each message a sender sends whole comes back whole.
///
/// The other tests here say that nothing panics and that nothing is made up.
/// This says the thing a bus is for: what goes in comes out. Two rounds of
/// making the reassembler cheaper broke this very thing and nothing caught
/// it, because a message never delivered raises no error and no count.
///
/// So: many senders together, each with a message of its own length, their
/// fragments shuffled together and some of them sent two times. Nothing can
/// come back changed, and nothing can go missing without the reassembler
/// saying it dropped something. A message alone, of whatever length, has to
/// come back with nothing dropped at all.
#[test]
fn what_a_sender_sends_whole_arrives_whole() {
    let mut rng = Rng(0x5eed_1234_5eed_1234);

    for round in 0..200u32 {
        let senders = 1 + rng.below(6);
        let mut reassembler = udpm::Reassembler::new();
        let mut sent: Vec<(String, Vec<u8>)> = Vec::new();
        let mut datagrams: Vec<(SocketAddr, Vec<u8>)> = Vec::new();

        for sender in 0..senders {
            // Lengths on each side of the datagram limit.
            let len = match rng.below(8) {
                0..=2 => 1 + rng.below(4_000),
                _ => 1 + rng.below(200_000),
            };
            let channel = alloc::format!("/bus/{sender}");
            let payload = rng.bytes(len);
            let frame = FrameRef {
                channel: &channel,
                payload: &payload,
            };
            let short_max = 64 + rng.below(2_000);
            // A large message and a small datagram want more fragments than
            // the count field holds, and the encoder refuses that. It is not
            // what this test is for.
            let Ok(pieces) = udpm::encode(round, frame, short_max) else {
                continue;
            };
            for datagram in pieces {
                // A network sends a datagram two times now and then.
                let copies = 1 + usize::from(rng.below(8) == 0);
                for _ in 0..copies {
                    datagrams.push((peer(sender as u8 + 1), datagram.clone()));
                }
            }
            sent.push((channel, payload));
        }

        for i in (1..datagrams.len()).rev() {
            datagrams.swap(i, rng.below(i + 1));
        }

        let mut got: Vec<(String, Vec<u8>)> = Vec::new();
        for (from, datagram) in &datagrams {
            match udpm::decode(datagram).expect("what encode wrote") {
                udpm::Datagram::Whole { frame, .. } => {
                    let frame = frame.to_frame();
                    got.push((frame.channel, frame.payload));
                }
                udpm::Datagram::Fragment(fragment) => {
                    if let Some(whole) = reassembler.feed(*from, fragment).expect("consistent") {
                        got.push((whole.channel, whole.payload));
                    }
                }
            }
        }

        // What came is what its sender sent, byte for byte.
        for (channel, payload) in &got {
            let (_, was) = sent
                .iter()
                .find(|(name, _)| name == channel)
                .unwrap_or_else(|| panic!("round {round}: {channel} was never sent"));
            assert_eq!(payload, was, "round {round}: {channel} came back changed");
        }

        // A message that one datagram holds has no fragment number to tell
        // it apart, so a second copy is delivered a second time, as it is in
        // LCM. Count the channels that came, and not what came.
        let arrived: std::collections::HashSet<&str> =
            got.iter().map(|(channel, _)| channel.as_str()).collect();

        // And nothing goes missing without the reassembler saying so.
        let lost = sent.len() - arrived.len();
        assert!(
            lost == 0 || reassembler.evicted() > 0,
            "round {round}: {lost} of {} messages went missing and none were dropped",
            sent.len()
        );
    }
}

/// A message alone on a bus comes back, of whatever length. The budget is
/// for all messages together, so with one message there is nothing to select
/// between and nothing to drop.
#[test]
fn a_message_alone_arrives_at_any_size() {
    let mut rng = Rng(0x0a10_0a10_0a10_0a10);

    for round in 0..40u32 {
        let len = match round % 4 {
            0 => 1 + rng.below(2_000),
            1 => 1 + rng.below(500_000),
            2 => udpm::MAX_FRAGMENT_BYTES - 1 - rng.below(4_096),
            _ => udpm::MAX_FRAGMENT_BYTES + 1 + rng.below(4_096),
        };
        let payload = rng.bytes(len);
        let frame = FrameRef {
            channel: "/bus/alone",
            payload: &payload,
        };

        let mut reassembler = udpm::Reassembler::new();
        let mut datagrams = udpm::encode(round, frame, 1_400).expect("encodable");
        for i in (1..datagrams.len()).rev() {
            datagrams.swap(i, rng.below(i + 1));
        }

        let mut got = None;
        for datagram in &datagrams {
            match udpm::decode(datagram).expect("what encode wrote") {
                udpm::Datagram::Whole { frame, .. } => got = Some(frame.to_frame()),
                udpm::Datagram::Fragment(fragment) => {
                    got = reassembler
                        .feed(peer(1), fragment)
                        .expect("consistent")
                        .or(got);
                }
            }
        }

        let got = got.unwrap_or_else(|| panic!("round {round}: {len} bytes never came"));
        assert_eq!(
            got.payload, payload,
            "round {round}: {len} bytes came back changed"
        );
        assert_eq!(reassembler.evicted(), 0, "round {round}: nothing to drop");
        assert_eq!(reassembler.in_flight(), 0);
    }
}
