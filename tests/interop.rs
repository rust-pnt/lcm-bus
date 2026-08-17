//! Bytes that `lcm-java` 1.5.1 wrote, read by this crate.
//!
//! Every other test holds bytes derived from the specification, so a mistake
//! in how that was read can agree with itself. These came off a bus and
//! out of a log that the reference implementation drove, and they are kept
//! as they are: a golden vector made again when it is wanted is one that
//! agrees with whatever makes it.
//!
//! Each payload is `i % 251` at index `i`.
#![cfg(feature = "std")]

use std::net::SocketAddr;

use lcm_bus::wire::{Decoded, log, udpm};

fn ramp(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn reference(name: &str) -> Vec<u8> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/reference/");
    std::fs::read(format!("{path}{name}")).expect("the bytes are in the repository")
}

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

#[test]
fn datagrams_from_the_reference_implementation_decode() {
    let capture = reference("udpm.bin");
    let source = SocketAddr::from(([127, 0, 0, 1], 43_210));
    let mut reassembler = udpm::Reassembler::new();
    let mut messages = Vec::new();

    for datagram in datagrams(&capture) {
        match udpm::decode(datagram).expect("the reference wrote it") {
            udpm::Datagram::Whole { frame, .. } => messages.push(frame.to_frame()),
            udpm::Datagram::Fragment(fragment) => {
                if let Some(whole) = reassembler.feed(source, fragment).expect("consistent") {
                    messages.push(whole);
                }
            }
        }
    }

    let named: Vec<(&str, usize)> = messages
        .iter()
        .map(|m| (m.channel.as_str(), m.payload.len()))
        .collect();
    assert_eq!(
        named,
        [
            ("/ref/short", 5),
            // Java fragments above 64 000 bytes and C above about 1400, so
            // this one is one datagram here and many from a C peer.
            ("/ref/whole10k", 10_000),
            ("/ref/fragmented", 200_000),
        ]
    );
    for message in &messages {
        assert_eq!(
            message.payload,
            ramp(message.payload.len()),
            "{}",
            message.channel
        );
    }
    assert_eq!(reassembler.in_flight(), 0);
}

#[test]
fn a_log_from_the_reference_implementation_reads() {
    let bytes = reference("ref.lcmlog");
    let mut at = 0;
    let mut events = Vec::new();
    while let Decoded::Item(event, used) =
        log::decode(&bytes[at..]).expect("the reference wrote it")
    {
        events.push((
            event.frame.channel.to_owned(),
            event.number,
            event.frame.payload.to_vec(),
        ));
        at += used;
    }

    assert_eq!(at, bytes.len(), "the whole file is events");
    let named: Vec<(&str, i64, usize)> = events
        .iter()
        .map(|(c, n, p)| (c.as_str(), *n, p.len()))
        .collect();
    // `LogFileProvider.publish` writes zero for each event, and its own
    // documentation asks for a sequence. `lcm-logger` and this crate number
    // them, so a reader must not take the number as one of a sequence.
    assert_eq!(named, [("/ref/one", 0, 3), ("/ref/two", 0, 5_000)]);
    for (channel, _, payload) in &events {
        assert_eq!(payload, &ramp(payload.len()), "{channel}");
    }
}
