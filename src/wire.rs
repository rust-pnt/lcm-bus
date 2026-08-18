//! LCM framing.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::net::SocketAddr;

/// A message: the channel it is on, and the bytes on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub channel: String,
    pub payload: Vec<u8>,
}

/// A [`Frame`] that borrows the buffer it was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameRef<'a> {
    pub channel: &'a str,
    pub payload: &'a [u8],
}

impl FrameRef<'_> {
    pub fn to_frame(&self) -> Frame {
        Frame {
            channel: String::from(self.channel),
            payload: Vec::from(self.payload),
        }
    }
}

impl Frame {
    pub fn view(&self) -> FrameRef<'_> {
        FrameRef {
            channel: &self.channel,
            payload: &self.payload,
        }
    }
}

impl From<FrameRef<'_>> for Frame {
    fn from(frame: FrameRef<'_>) -> Self {
        frame.to_frame()
    }
}

impl<'a> From<&'a Frame> for FrameRef<'a> {
    fn from(frame: &'a Frame) -> Self {
        frame.view()
    }
}

/// The LCM limit on a channel name. tcpq gives the length as a field and
/// applies no limit.
pub const MAX_CHANNEL_LEN: usize = 63;

/// The LCM limit on a message.
pub const MAX_MESSAGE_LEN: usize = 1 << 28;

/// Why a read or a write of a frame failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WireError {
    /// The buffer stops in the middle of a field.
    Truncated { needed: usize, remaining: usize },
    /// Not an LCM datagram, or a version this crate does not speak.
    BadMagic(u32),
    /// A channel name with no terminator, with a NUL in it, or one that is not UTF-8.
    BadChannel,
    /// Longer than [`MAX_CHANNEL_LEN`] on udpm, or than [`MAX_MESSAGE_LEN`] on tcpq.
    /// The decoder rejects it before it makes a buffer.
    ChannelTooLong(usize),
    /// A payload above [`MAX_MESSAGE_LEN`].
    MessageTooLarge(usize),
    /// One datagram cannot hold the channel name and a payload byte.
    DatagramTooSmall { short_max: usize, minimum: usize },
    /// More fragments than the 16-bit field numbers.
    TooManyFragments(usize),
    /// A fragment that does not agree with the others in its message.
    InconsistentFragment(u32),
    /// A relay frame of a type this client does not read.
    UnknownFrameType(u32),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { needed, remaining } => {
                write!(f, "needed {needed} more bytes, {remaining} remain")
            }
            Self::BadMagic(magic) => write!(f, "{magic:#010x} is not an LCM magic number"),
            Self::BadChannel => {
                f.write_str("an empty channel name, or one with bytes a name cannot hold")
            }
            Self::ChannelTooLong(len) => write!(f, "a channel name of {len} bytes is too long"),
            Self::MessageTooLarge(len) => write!(f, "a message of {len} bytes is above the limit"),
            Self::DatagramTooSmall { short_max, minimum } => write!(
                f,
                "a datagram limit of {short_max} bytes is too small for this channel, \
                 and the minimum is {minimum}"
            ),
            Self::TooManyFragments(count) => write!(f, "{count} fragments is above the limit"),
            Self::InconsistentFragment(sequence) => {
                write!(f, "the fragments of message {sequence} do not agree")
            }
            Self::UnknownFrameType(kind) => write!(f, "{kind} is not a relay frame type"),
        }
    }
}

impl core::error::Error for WireError {}

/// What one read of a stream found.
///
/// A stream decoder is asked the same question again and again, so it answers
/// with what to do next and not only with what it read. `Ok(None)` says none
/// of that: not how many bytes to wait for, and not that a frame is one this
/// decoder will not take although the caller can step over it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded<T> {
    /// An item, and the bytes of the stream it used.
    Item(T, usize),
    /// Not an item yet. The stream must hold this many bytes before a
    /// second read can say anything else.
    Need(usize),
    /// A whole frame this decoder will not give back. Its length is known, so
    /// a caller steps over it and keeps the stream.
    Skip(usize),
}

impl<T> Decoded<T> {
    /// The item and the bytes it used, for a caller that wants only those.
    #[must_use]
    pub fn item(self) -> Option<(T, usize)> {
        match self {
            Self::Item(item, used) => Some((item, used)),
            Self::Need(_) | Self::Skip(_) => None,
        }
    }
}

/// LCM takes all channel names through `lcm_publish`, so one rule holds for
/// all of them. `max` differs: a log holds a longer name than a datagram.
pub fn check_channel(channel: &str, max: usize) -> Result<(), WireError> {
    let bytes = channel.as_bytes();
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(WireError::BadChannel);
    }
    if bytes.len() > max {
        return Err(WireError::ChannelTooLong(bytes.len()));
    }
    Ok(())
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let remaining = self.data.len() - self.pos;
        if remaining < n {
            return Err(WireError::Truncated {
                needed: n,
                remaining,
            });
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u16(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64, WireError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn channel(&mut self) -> Result<&'a str, WireError> {
        let rest = &self.data[self.pos..];
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or(WireError::BadChannel)?;
        // C names a channel with `strlen` and tests no lower limit, so C
        // delivers an empty name. No encoder here writes one, and a message
        // this crate cannot publish is not one it gives back.
        if end == 0 {
            return Err(WireError::BadChannel);
        }
        if end > MAX_CHANNEL_LEN {
            return Err(WireError::ChannelTooLong(end));
        }
        let name = core::str::from_utf8(&rest[..end]).map_err(|_| WireError::BadChannel)?;
        self.pos += end + 1;
        Ok(name)
    }

    fn rest(&mut self) -> &'a [u8] {
        let out = &self.data[self.pos..];
        self.pos = self.data.len();
        out
    }
}

struct Writer(Vec<u8>);

impl Writer {
    fn with_capacity(n: usize) -> Self {
        Self(Vec::with_capacity(n))
    }

    fn bytes(mut self, bytes: &[u8]) -> Self {
        self.0.extend_from_slice(bytes);
        self
    }

    fn u32(self, value: u32) -> Self {
        self.bytes(&value.to_be_bytes())
    }

    fn i64(self, value: i64) -> Self {
        self.bytes(&value.to_be_bytes())
    }

    fn length_prefixed(self, bytes: &[u8]) -> Self {
        self.u32(bytes.len() as u32).bytes(bytes)
    }

    fn finish(self) -> Vec<u8> {
        self.0
    }
}

/// The UDP multicast wire format: a short message in one datagram, or a
/// larger one in fragments a [`udpm::Reassembler`] puts back together.
pub mod udpm {
    use super::*;

    /// `"LC02"` — a full message in one datagram.
    pub const MAGIC_SHORT: u32 = 0x4c43_3032;
    /// `"LC03"` — one fragment of a larger message.
    pub const MAGIC_LONG: u32 = 0x4c43_3033;

    const HEADER_SHORT: usize = 8;
    const HEADER_LONG: usize = 20;

    /// Give this to [`encode`] when the peer runs macOS and this host does not.
    pub const SHORT_MESSAGE_MAX_APPLE: usize = 1435;

    /// The payload of one datagram: the channel name, its NUL, and the message.
    pub const SHORT_MESSAGE_MAX: usize = if cfg!(target_os = "macos") {
        SHORT_MESSAGE_MAX_APPLE
    } else {
        65_499
    };

    /// A fragment header is longer than a short one, so a fragment holds less.
    const HEADER_GAP: usize = HEADER_LONG - HEADER_SHORT;

    /// LCM keeps a fragment and a short message to one datagram length.
    pub const fn fragment_max(short_max: usize) -> usize {
        if short_max > HEADER_GAP {
            short_max - HEADER_GAP
        } else {
            1
        }
    }

    /// The messages a [`Reassembler`] holds at one time.
    pub const MAX_FRAGMENT_BUFFERS: usize = 1000;
    /// The bytes a [`Reassembler`] holds across every message it is putting
    /// back together.
    ///
    /// This counts what has come, not what a sender says will come, so a
    /// length claimed and never sent takes none of it. One message alone can
    /// go above this, as it can in LCM, because a budget for all of them
    /// cannot be a limit on one of them.
    pub const MAX_FRAGMENT_BYTES: usize = 1 << 24;

    /// What one datagram holds: a whole message, or one fragment of one.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Datagram<'a> {
        Whole { sequence: u32, frame: FrameRef<'a> },
        Fragment(Fragment<'a>),
    }

    /// One fragment of a message, for a [`Reassembler`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Fragment<'a> {
        pub sequence: u32,
        /// The payload length of the full message.
        pub total: u32,
        /// The position of this fragment in that message.
        pub offset: u32,
        pub index: u16,
        pub count: u16,
        /// Only fragment zero has this.
        pub channel: Option<&'a str>,
        pub payload: &'a [u8],
    }

    /// Read the datagram at the start of `data`.
    pub fn decode(data: &[u8]) -> Result<Datagram<'_>, WireError> {
        let mut r = Reader::new(data);

        match r.u32()? {
            MAGIC_SHORT => Ok(Datagram::Whole {
                sequence: r.u32()?,
                frame: FrameRef {
                    channel: r.channel()?,
                    payload: r.rest(),
                },
            }),
            MAGIC_LONG => {
                let sequence = r.u32()?;
                let total = r.u32()?;
                let offset = r.u32()?;
                let index = r.u16()?;
                let count = r.u16()?;

                if total as usize > MAX_MESSAGE_LEN {
                    return Err(WireError::MessageTooLarge(total as usize));
                }

                let channel = if index == 0 { Some(r.channel()?) } else { None };

                Ok(Datagram::Fragment(Fragment {
                    sequence,
                    total,
                    offset,
                    index,
                    count,
                    channel,
                    payload: r.rest(),
                }))
            }
            other => Err(WireError::BadMagic(other)),
        }
    }

    /// A payload of `short_max` bytes, channel name included, goes in one datagram.
    /// A larger one becomes fragments.
    /// The most a datagram puts before the payload: the long header, the
    /// longest channel name, and its NUL.
    pub const HEAD_MAX: usize = HEADER_LONG + MAX_CHANNEL_LEN + 1;

    /// What goes before the payload of one datagram. This holds no
    /// allocation, so a caller can write it and the payload together with
    /// one scatter-gather send.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Head {
        bytes: [u8; HEAD_MAX],
        len: u8,
    }

    impl AsRef<[u8]> for Head {
        fn as_ref(&self) -> &[u8] {
            &self.bytes[..self.len as usize]
        }
    }

    /// The datagrams of one message, as a head and a slice of the payload
    /// the caller still owns.
    #[derive(Debug, Clone)]
    pub struct Fragments<'a> {
        sequence: u32,
        channel: &'a [u8],
        payload: &'a [u8],
        /// The payload bytes of fragment zero, which shares with the channel.
        first: usize,
        /// The payload bytes of each fragment after it.
        limit: usize,
        count: usize,
        index: usize,
    }

    impl Fragments<'_> {
        fn start(&self, index: usize) -> usize {
            match index {
                0 => 0,
                _ => self.first + (index - 1) * self.limit,
            }
        }
    }

    impl<'a> Iterator for Fragments<'a> {
        type Item = (Head, &'a [u8]);

        fn next(&mut self) -> Option<Self::Item> {
            if self.index >= self.count {
                return None;
            }
            let index = self.index;
            self.index += 1;

            let mut head = Head {
                bytes: [0; HEAD_MAX],
                len: 0,
            };
            let mut put = |bytes: &[u8]| {
                let at = head.len as usize;
                head.bytes[at..at + bytes.len()].copy_from_slice(bytes);
                head.len += bytes.len() as u8;
            };

            if self.count == 1 {
                put(&MAGIC_SHORT.to_be_bytes());
                put(&self.sequence.to_be_bytes());
                put(self.channel);
                put(&[0]);
                return Some((head, self.payload));
            }

            put(&MAGIC_LONG.to_be_bytes());
            put(&self.sequence.to_be_bytes());
            put(&(self.payload.len() as u32).to_be_bytes());
            put(&(self.start(index) as u32).to_be_bytes());
            put(&(index as u16).to_be_bytes());
            put(&(self.count as u16).to_be_bytes());
            if index == 0 {
                put(self.channel);
                put(&[0]);
            }

            let end = self.start(index + 1).min(self.payload.len());
            Some((head, &self.payload[self.start(index)..end]))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let left = self.count - self.index;
            (left, Some(left))
        }
    }

    impl ExactSizeIterator for Fragments<'_> {}

    /// The datagrams of one message, each one whole.
    /// [`fragments`] gives the same bytes with no allocation.
    pub fn encode(
        sequence: u32,
        frame: FrameRef<'_>,
        short_max: usize,
    ) -> Result<Vec<Vec<u8>>, WireError> {
        Ok(fragments(sequence, frame, short_max)?
            .map(|(head, body)| [head.as_ref(), body].concat())
            .collect())
    }

    /// The datagrams of one message, without a copy of the payload.
    ///
    /// A message that fits in one datagram gives one head and the payload
    /// whole. A larger one gives the channel name in the head of fragment
    /// zero, as LCM does.
    pub fn fragments(
        sequence: u32,
        frame: FrameRef<'_>,
        short_max: usize,
    ) -> Result<Fragments<'_>, WireError> {
        super::check_channel(frame.channel, MAX_CHANNEL_LEN)?;
        let channel = frame.channel.as_bytes();
        if frame.payload.len() > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(frame.payload.len()));
        }

        let head = channel.len() + 1;
        let whole = Fragments {
            sequence,
            channel,
            payload: frame.payload,
            first: 0,
            limit: 0,
            count: 1,
            index: 0,
        };
        if head + frame.payload.len() <= short_max {
            return Ok(whole);
        }

        // The channel name takes part of fragment zero and leaves `first`.
        let limit = fragment_max(short_max);
        let Some(first) = limit.checked_sub(head).filter(|first| *first > 0) else {
            return Err(WireError::DatagramTooSmall {
                short_max,
                minimum: head + 1 + HEADER_GAP,
            });
        };
        let count = (head + frame.payload.len()).div_ceil(limit);
        if count > u16::MAX as usize {
            return Err(WireError::TooManyFragments(count));
        }

        Ok(Fragments {
            first,
            limit,
            count,
            ..whole
        })
    }

    /// The key of a message is its source and its sequence number.
    /// All the peers on one group number their messages from zero.
    ///
    /// `K` names a source. A socket gives [`SocketAddr`], and a bus on
    /// something else gives whatever tells its senders apart.
    #[derive(Debug)]
    pub struct Reassembler<K = SocketAddr> {
        partial: BTreeMap<(K, u32), Partial>,
        /// The keys by `touched`. `clock` counts up and never repeats, so this
        /// holds one entry for each message and its first is the one to drop.
        by_age: BTreeMap<u64, (K, u32)>,
        held: usize,
        clock: u64,
        evicted: u64,
    }

    impl<K: Ord> Default for Reassembler<K> {
        fn default() -> Self {
            Self {
                partial: BTreeMap::new(),
                by_age: BTreeMap::new(),
                held: 0,
                clock: 0,
                evicted: 0,
            }
        }
    }

    /// One fragment that came: where it belongs in the message, and where
    /// its bytes sit in the payload gathered so far.
    #[derive(Debug, Clone, Copy)]
    struct Piece {
        start: u32,
        at: u32,
        len: u32,
    }

    /// What remembering one fragment costs, beyond its bytes.
    ///
    /// A fragment with no payload at all is a legal datagram, and one that
    /// costs no bytes takes no part of the budget and keeps a place in the
    /// table all the same. A thousand messages of 65 534 such places is two
    /// gigabytes that a budget of 16 MiB says nothing about.
    const PIECE_COST: usize = size_of::<(u16, Piece)>() + 2 * size_of::<usize>();

    #[derive(Debug)]
    struct Partial {
        channel: Option<String>,
        total: u32,
        count: u16,
        /// The payload bytes of the fragments that came, in the sequence
        /// they came in.
        ///
        /// A sender picks the length of a message, and a buffer of that
        /// length has to be cleared before anything can be written into a
        /// part of it. So one 23-byte datagram claiming 16 MiB cost 16 MiB
        /// of writing, and the fragment behind it never came. Nothing goes
        /// in here that a sender did not send, so nothing is cleared.
        arrived: Vec<u8>,
        /// The fragments that came, by index. A sender picks `count`, so a
        /// table with room for each of them is `count` units of work for one
        /// datagram; and a table kept in sequence is that much work again
        /// for each fragment that comes out of sequence.
        pieces: BTreeMap<u16, Piece>,
        touched: u64,
    }

    impl<K: Ord + Clone> Reassembler<K> {
        pub fn new() -> Self {
            Self::default()
        }

        /// This gives the message when the fragment completes it.
        pub fn feed(
            &mut self,
            source: K,
            fragment: Fragment<'_>,
        ) -> Result<Option<Frame>, WireError> {
            // The fuzzers push hundreds of thousands of fragments, so this
            // holds each path that adds to one map and not to the other.
            debug_assert_eq!(self.by_age.len(), self.partial.len());

            let key = (source, fragment.sequence);
            let inconsistent = WireError::InconsistentFragment(fragment.sequence);

            // The limit of the protocol, which is the limit this crate's own
            // encoder keeps. Nothing is put aside for a length a sender
            // claims, so a claim on its own costs nothing and needs no
            // smaller limit of its own.
            if fragment.total as usize > MAX_MESSAGE_LEN {
                return Err(WireError::MessageTooLarge(fragment.total as usize));
            }
            if fragment.count == 0 || fragment.index >= fragment.count {
                return Err(inconsistent);
            }
            // A fragment holds one byte or more.
            // Without this test, a `seen` array is larger than the message it names.
            if fragment.count as usize > (fragment.total as usize).max(1) {
                return Err(inconsistent);
            }
            // A 32-bit `usize` wraps on this sum, and the low result gets
            // through the next test.
            if (fragment.offset as usize)
                .checked_add(fragment.payload.len())
                .is_none_or(|end| end > fragment.total as usize)
            {
                return Err(inconsistent);
            }

            // A sequence number with a different shape is a different message.
            // LCM removes the fragments it has.
            let disagrees = self
                .partial
                .get(&key)
                .is_some_and(|p| p.total != fragment.total || p.count != fragment.count);
            if disagrees {
                self.remove(&key);
                return Err(inconsistent);
            }

            self.clock += 1;
            let now = self.clock;
            if let Some(entry) = self.partial.get(&key) {
                self.by_age.remove(&entry.touched);
            }
            self.by_age.insert(now, key.clone());
            // Before the bytes of this fragment go anywhere, put what is
            // held back within the budget. Nothing is dropped in the middle
            // of a write like this, and the budget goes above itself by one
            // datagram and no more.
            self.make_space(&key, fragment.payload.len());
            let entry = match self.partial.get_mut(&key) {
                Some(entry) => entry,
                None => self.partial.entry(key.clone()).or_insert(Partial {
                    channel: None,
                    total: fragment.total,
                    count: fragment.count,
                    arrived: Vec::new(),
                    pieces: BTreeMap::new(),
                    touched: now,
                }),
            };
            entry.touched = now;

            // Fragment zero carries the name, and a duplicate of it must no
            // more rewrite the name than it adds to the count.
            if let Some(channel) = fragment.channel {
                entry.channel.get_or_insert_with(|| String::from(channel));
            }
            if let alloc::collections::btree_map::Entry::Vacant(slot) =
                entry.pieces.entry(fragment.index)
            {
                // The lengths of the fragments of a message add up to the
                // length of the message, so more than that is not one.
                if entry.arrived.len() + fragment.payload.len() > entry.total as usize {
                    return Err(inconsistent);
                }
                slot.insert(Piece {
                    start: fragment.offset,
                    at: entry.arrived.len() as u32,
                    len: fragment.payload.len() as u32,
                });
                entry.arrived.extend_from_slice(fragment.payload);
                self.held += fragment.payload.len() + PIECE_COST;
            }
            if entry.pieces.len() < entry.count as usize {
                return Ok(None);
            }

            let partial = self.remove(&key).expect("just matched");
            // A count of indexes is not a count of bytes: fragments that
            // overlap fill it and leave a hole. Each span here is one a
            // fragment wrote, and they have to cover the message exactly.
            // Room is taken for what came, and not for what a sender
            // claimed.
            if partial.arrived.len() != partial.total as usize {
                return Err(inconsistent);
            }
            let mut pieces: Vec<Piece> = partial.pieces.into_values().collect();
            pieces.sort_unstable_by_key(|piece| piece.start);
            let mut payload = Vec::with_capacity(partial.arrived.len());
            for piece in pieces {
                if piece.start as usize != payload.len() {
                    return Err(inconsistent);
                }
                let at = piece.at as usize;
                payload.extend_from_slice(&partial.arrived[at..at + piece.len as usize]);
            }
            if payload.len() != partial.total as usize {
                return Err(inconsistent);
            }
            // Only fragment zero holds the channel name.
            let Some(channel) = partial.channel else {
                return Err(inconsistent);
            };
            Ok(Some(Frame { channel, payload }))
        }

        fn remove(&mut self, key: &(K, u32)) -> Option<Partial> {
            let partial = self.partial.remove(key)?;
            self.by_age.remove(&partial.touched);
            self.held = self
                .held
                .saturating_sub(partial.arrived.len() + partial.pieces.len() * PIECE_COST);
            Some(partial)
        }

        /// LCM drops the message that waited longest for a fragment.
        ///
        /// Never the one being fed. A message larger than the budget is the
        /// oldest of the one message there is, so it dropped itself on each
        /// fragment above the budget and was never put together at all: no
        /// error, no count, and nothing delivered.
        fn make_space(&mut self, keep: &(K, u32), incoming: usize) {
            while self.partial.len() >= MAX_FRAGMENT_BUFFERS
                || (self.held + incoming > MAX_FRAGMENT_BYTES && !self.partial.is_empty())
            {
                let oldest = self.by_age.values().find(|key| *key != keep).cloned();
                let Some(oldest) = oldest else { break };
                self.remove(&oldest);
                self.evicted += 1;
            }
        }

        /// The messages this dropped to keep to its budget. A bus that
        /// loses them says nothing else about it.
        pub fn evicted(&self) -> u64 {
            self.evicted
        }

        /// How many messages are part-way put together.
        pub fn in_flight(&self) -> usize {
            self.partial.len()
        }
    }
}

/// The relay framing: a client subscribes and publishes, and the relay sends back the
/// messages that match.
/// This crate is both sides.
pub mod tcpq {
    use super::*;

    pub const MAGIC_SERVER: u32 = 0x2876_17fa;
    pub const MAGIC_CLIENT: u32 = 0x2876_17fb;
    pub const PROTOCOL_VERSION: u32 = 0x0100;

    pub const MESSAGE_TYPE_PUBLISH: u32 = 1;
    pub const MESSAGE_TYPE_SUBSCRIBE: u32 = 2;
    pub const MESSAGE_TYPE_UNSUBSCRIBE: u32 = 3;

    /// The bytes a client sends a relay when it connects.
    pub fn handshake() -> Vec<u8> {
        Writer::with_capacity(8)
            .u32(MAGIC_CLIENT)
            .u32(PROTOCOL_VERSION)
            .finish()
    }

    /// LCM reads the version of the server and does not compare it.
    pub fn check_handshake(reply: &[u8]) -> Result<u32, WireError> {
        let mut r = Reader::new(reply);
        let magic = r.u32()?;
        let version = r.u32()?;
        if magic != MAGIC_SERVER {
            return Err(WireError::BadMagic(magic));
        }
        Ok(version)
    }

    /// The bytes a relay sends a client that it accepted.
    pub fn server_handshake() -> Vec<u8> {
        Writer::with_capacity(8)
            .u32(MAGIC_SERVER)
            .u32(PROTOCOL_VERSION)
            .finish()
    }

    /// The greeting of a client, as a relay reads it.
    ///
    /// The version comes back and nothing compares it, which is what LCM does with the
    /// other direction.
    /// One protocol version exists.
    pub fn check_client_handshake(greeting: &[u8]) -> Result<u32, WireError> {
        let mut r = Reader::new(greeting);
        let magic = r.u32()?;
        let version = r.u32()?;
        if magic != MAGIC_CLIENT {
            return Err(WireError::BadMagic(magic));
        }
        Ok(version)
    }

    /// What a client sends a relay.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Request<'a> {
        /// A message for the relay to send on to every client that matches it.
        Publish(FrameRef<'a>),
        /// A pattern to match.
        /// The relay reads it as a Java regex against the whole channel name.
        Subscribe(&'a str),
        /// The relay removes the first record equal to this.
        Unsubscribe(&'a str),
    }

    /// The relay reads `pattern` as a Java regex and matches the full name.
    pub fn subscribe(pattern: &str) -> Vec<u8> {
        request(MESSAGE_TYPE_SUBSCRIBE, pattern)
    }

    /// The relay removes the first record equal to `pattern`.
    pub fn unsubscribe(pattern: &str) -> Vec<u8> {
        request(MESSAGE_TYPE_UNSUBSCRIBE, pattern)
    }

    fn request(kind: u32, pattern: &str) -> Vec<u8> {
        Writer::with_capacity(8 + pattern.len())
            .u32(kind)
            .length_prefixed(pattern.as_bytes())
            .finish()
    }

    /// The bytes of a publish frame, for a relay to send on.
    pub fn publish(frame: FrameRef<'_>) -> Result<Vec<u8>, WireError> {
        super::check_channel(frame.channel, MAX_CHANNEL_LEN)?;
        let channel = frame.channel.as_bytes();
        if frame.payload.len() > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(frame.payload.len()));
        }
        Ok(
            Writer::with_capacity(12 + channel.len() + frame.payload.len())
                .u32(MESSAGE_TYPE_PUBLISH)
                .length_prefixed(channel)
                .length_prefixed(frame.payload)
                .finish(),
        )
    }

    /// The longest channel name this reads off a relay before it gives up on
    /// the stream.
    ///
    /// LCM names are [`MAX_CHANNEL_LEN`] bytes, and neither the C relay nor
    /// the Java one holds a publisher to that. A longer name is a message
    /// this crate will not take, but it is still a frame with a length, and
    /// stepping over it costs the message and not the connection. A name
    /// above this is not a name, and the stream is lost.
    pub const CHANNEL_READ_MAX: usize = 1024;

    /// Read one frame.
    pub fn decode(data: &[u8]) -> Result<Decoded<FrameRef<'_>>, WireError> {
        // The type of the frame, and the length of the channel name.
        const HEAD: usize = 8;
        if data.len() < HEAD {
            return Ok(Decoded::Need(HEAD));
        }
        let mut r = Reader::new(data);
        let kind = r.u32()?;
        // A relay sends a publish frame and nothing else. This client
        // knows no length for a frame of a different type, so the stream
        // ends here.
        if kind != MESSAGE_TYPE_PUBLISH {
            return Err(WireError::UnknownFrameType(kind));
        }

        let channel_len = r.u32()? as usize;
        if channel_len > CHANNEL_READ_MAX {
            return Err(WireError::ChannelTooLong(channel_len));
        }
        // The length of the payload is behind the name, so the length of the
        // frame is not known until the name is here.
        let head = HEAD + channel_len + 4;
        if data.len() < head {
            return Ok(Decoded::Need(head));
        }
        let channel = r.take(channel_len)?;
        let payload_len = r.u32()? as usize;
        // A relay is a peer this client dialled, so its messages keep the
        // limit of the protocol. A udpm group takes anyone, and holds to the
        // smaller `MAX_FRAGMENT_BYTES`.
        if payload_len > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(payload_len));
        }
        let frame = head + payload_len;
        if data.len() < frame {
            return Ok(Decoded::Need(frame));
        }

        // A name no encoder here would write, on a frame whose length is
        // known. One of those costs itself and nothing after it.
        let Ok(channel) = core::str::from_utf8(channel) else {
            return Ok(Decoded::Skip(frame));
        };
        if super::check_channel(channel, MAX_CHANNEL_LEN).is_err() {
            return Ok(Decoded::Skip(frame));
        }
        let payload = r.take(payload_len)?;
        Ok(Decoded::Item(FrameRef { channel, payload }, frame))
    }

    /// Read one request of a client, as a relay reads it.
    ///
    /// This is the mirror of [`decode`], which reads the one frame type that travels
    /// the other way.
    /// A relay reads three.
    pub fn decode_request(data: &[u8]) -> Result<Decoded<Request<'_>>, WireError> {
        // The type of the frame, and the length of the first field.
        const HEAD: usize = 8;
        if data.len() < HEAD {
            return Ok(Decoded::Need(HEAD));
        }
        let mut r = Reader::new(data);
        let kind = r.u32()?;
        if !matches!(
            kind,
            MESSAGE_TYPE_PUBLISH | MESSAGE_TYPE_SUBSCRIBE | MESSAGE_TYPE_UNSUBSCRIBE
        ) {
            // Nothing gives the length of a frame of a type this does not read, so
            // nothing after it can be found.
            // The stream ends here.
            return Err(WireError::UnknownFrameType(kind));
        }

        let first_len = r.u32()? as usize;
        if first_len > CHANNEL_READ_MAX {
            return Err(WireError::ChannelTooLong(first_len));
        }

        // A subscription is the type and one string.
        // A publish carries a payload behind the name, so nothing gives the length of
        // the frame until the name is here.
        if kind != MESSAGE_TYPE_PUBLISH {
            let frame = HEAD + first_len;
            if data.len() < frame {
                return Ok(Decoded::Need(frame));
            }
            let pattern = r.take(first_len)?;
            // A pattern that is not text is a whole frame of known length, so a step
            // over it costs the request and not the stream.
            let Ok(pattern) = core::str::from_utf8(pattern) else {
                return Ok(Decoded::Skip(frame));
            };
            let request = if kind == MESSAGE_TYPE_SUBSCRIBE {
                Request::Subscribe(pattern)
            } else {
                Request::Unsubscribe(pattern)
            };
            return Ok(Decoded::Item(request, frame));
        }

        let head = HEAD + first_len + 4;
        if data.len() < head {
            return Ok(Decoded::Need(head));
        }
        let channel = r.take(first_len)?;
        let payload_len = r.u32()? as usize;
        if payload_len > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(payload_len));
        }
        let frame = head + payload_len;
        if data.len() < frame {
            return Ok(Decoded::Need(frame));
        }
        let Ok(channel) = core::str::from_utf8(channel) else {
            return Ok(Decoded::Skip(frame));
        };
        if super::check_channel(channel, MAX_CHANNEL_LEN).is_err() {
            return Ok(Decoded::Skip(frame));
        }
        let payload = r.take(payload_len)?;
        Ok(Decoded::Item(
            Request::Publish(FrameRef { channel, payload }),
            frame,
        ))
    }
}

/// The `.lcmlog` event format: a sequence of events, each a sync word, a
/// number, a timestamp, and a message.
pub mod log {
    use super::*;

    /// The LCM sync word at the start of each event.
    pub const MAGIC: u32 = 0xeda1_da01;

    /// The sync word, the event number, the timestamp, and the two lengths.
    pub const HEADER_LEN: usize = 28;

    /// A log channel name is 1 to 999 bytes, which is not the udpm limit.
    /// The LCM limit on a channel name in a log, which is not the one on a
    /// bus.
    pub const CHANNEL_MAX: usize = 999;

    /// One event of a log: a message, and when it came.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Event<'a> {
        /// LCM numbers the events of one log from zero.
        pub number: i64,
        /// When the message came, in microseconds since the Unix epoch.
        pub timestamp: i64,
        pub frame: FrameRef<'a>,
    }

    /// The bytes of one event, to write to a log.
    pub fn encode(event: Event<'_>) -> Result<Vec<u8>, WireError> {
        super::check_channel(event.frame.channel, CHANNEL_MAX)?;
        let channel = event.frame.channel.as_bytes();
        let payload = event.frame.payload;
        if payload.len() > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(payload.len()));
        }

        Ok(
            Writer::with_capacity(HEADER_LEN + channel.len() + payload.len())
                .u32(MAGIC)
                .i64(event.number)
                .i64(event.timestamp)
                .u32(channel.len() as u32)
                .u32(payload.len() as u32)
                .bytes(channel)
                .bytes(payload)
                .finish(),
        )
    }

    /// Read the event at the start of `data`.
    /// An `Err` is a torn event, and a reader slides to the next sync word.
    ///
    /// C reads a length as `int32` and refuses one below zero, and no more,
    /// so C reads an event of 2 GiB. This holds a payload to
    /// [`MAX_MESSAGE_LEN`], which each bus here holds it to also, and so
    /// refuses a log that only a writer of the format itself makes.
    pub fn decode(data: &[u8]) -> Result<Decoded<Event<'_>>, WireError> {
        // One test for the fixed head, so the reads below cannot come up
        // short and each one can be read as itself.
        if data.len() < HEADER_LEN {
            return Ok(Decoded::Need(HEADER_LEN));
        }
        let mut r = Reader::new(data);

        let magic = r.u32()?;
        if magic != MAGIC {
            return Err(WireError::BadMagic(magic));
        }
        let number = r.i64()?;
        let timestamp = r.i64()?;
        let channel_len = r.u32()?;
        let payload_len = r.u32()?;

        // The two lengths are signed on the wire, so a negative one reads
        // here as a large number and breaks a limit.
        let channel_len = channel_len as usize;
        if channel_len == 0 {
            return Err(WireError::BadChannel);
        }
        if channel_len > CHANNEL_MAX {
            return Err(WireError::ChannelTooLong(channel_len));
        }
        let payload_len = payload_len as usize;
        if payload_len > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(payload_len));
        }

        let event = HEADER_LEN + channel_len + payload_len;
        if data.len() < event {
            return Ok(Decoded::Need(event));
        }
        // A sync word, or the end of the log, sits behind an event. Without
        // this test a length that is wrong takes the events behind it for a
        // payload and gives them back as one. LCM makes the same test.
        //
        // `data` that stops on the end of an event cannot say which of the
        // two it is, and takes the event. A length wrong there costs that
        // event, and `resync` finds the ones behind it.
        let behind = &data[event..];
        if behind.len() >= 4 && behind[..4] != MAGIC.to_be_bytes() {
            return Err(WireError::BadMagic(u32::from_be_bytes(
                behind[..4].try_into().expect("4 bytes"),
            )));
        }
        // A log gives the length of a name, so a name that is not text, or
        // has a NUL in it, is in one. C reads a name with `strlen` and takes
        // both.
        //
        // `Skip` and not `Err`: the sync word behind the event agrees with
        // the length, so the event costs itself alone. An `Err` sends the
        // reader looking for the next sync word, and the first it finds is
        // in this event's own payload.
        let channel = r.take(channel_len)?;
        let Ok(channel) = core::str::from_utf8(channel) else {
            return Ok(Decoded::Skip(event));
        };
        if super::check_channel(channel, CHANNEL_MAX).is_err() {
            return Ok(Decoded::Skip(event));
        }
        let payload = r.take(payload_len)?;

        Ok(Decoded::Item(
            Event {
                number,
                timestamp,
                frame: FrameRef { channel, payload },
            },
            event,
        ))
    }

    /// A reader looks for a point in a log with this, and needs no payload.
    /// What the head of an event says, without reading its payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Peek {
        pub timestamp: i64,
        /// The bytes of the whole event: this head, and the rest of it.
        pub len: usize,
    }

    /// Read the head of the event at the start of `data`.
    ///
    /// The head is [`HEADER_LEN`] bytes and holds the length of the event, so
    /// a caller that wants to know where an event ends reads no payload to
    /// find out. That is what lets an event larger than each buffer here be
    /// checked as cheaply as a small one.
    pub fn peek(data: &[u8]) -> Result<Option<Peek>, WireError> {
        if data.len() < HEADER_LEN {
            return Ok(None);
        }
        let mut r = Reader::new(data);
        let magic = r.u32()?;
        if magic != MAGIC {
            return Err(WireError::BadMagic(magic));
        }
        let _number = r.i64()?;
        let timestamp = r.i64()?;
        let channel_len = r.u32()? as usize;
        let payload_len = r.u32()? as usize;
        if channel_len == 0 {
            return Err(WireError::BadChannel);
        }
        if channel_len > CHANNEL_MAX {
            return Err(WireError::ChannelTooLong(channel_len));
        }
        if payload_len > MAX_MESSAGE_LEN {
            return Err(WireError::MessageTooLarge(payload_len));
        }
        Ok(Some(Peek {
            timestamp,
            len: HEADER_LEN + channel_len + payload_len,
        }))
    }

    /// A reader that meets a bad event uses this to find the one after it.
    pub fn resync(data: &[u8]) -> Option<usize> {
        let magic = MAGIC.to_be_bytes();
        data.windows(magic.len()).position(|w| w == magic)
    }
}

#[cfg(test)]
mod tcpq_server_tests {
    use super::tcpq::*;
    use super::{Decoded, FrameRef};

    /// The two greetings are the two magic numbers, and each side refuses the other's.
    /// A relay that answered its own greeting would look like a client to the peer it
    /// accepted.
    #[test]
    fn each_side_refuses_the_greeting_of_its_own_kind() {
        assert!(check_client_handshake(&handshake()).is_ok());
        assert!(check_handshake(&server_handshake()).is_ok());
        assert!(check_client_handshake(&server_handshake()).is_err());
        assert!(check_handshake(&handshake()).is_err());
    }

    #[test]
    fn a_relay_reads_the_subscription_a_client_wrote() {
        let bytes = subscribe("/pntos/.*");
        assert_eq!(
            decode_request(&bytes).unwrap(),
            Decoded::Item(Request::Subscribe("/pntos/.*"), bytes.len())
        );

        let bytes = unsubscribe("/pntos/.*");
        assert_eq!(
            decode_request(&bytes).unwrap(),
            Decoded::Item(Request::Unsubscribe("/pntos/.*"), bytes.len())
        );
    }

    #[test]
    fn a_relay_reads_the_publish_a_client_wrote() {
        let frame = FrameRef {
            channel: "/ublox",
            payload: &[1, 2, 3, 4],
        };
        let bytes = publish(frame).unwrap();
        assert_eq!(
            decode_request(&bytes).unwrap(),
            Decoded::Item(Request::Publish(frame), bytes.len())
        );
    }

    /// A stream gives what it gives.
    /// The decoder asks for the whole frame and takes none of it until all of it is
    /// there.
    #[test]
    fn a_partial_frame_asks_for_the_rest() {
        let bytes = publish(FrameRef {
            channel: "/ublox",
            payload: &[9; 64],
        })
        .unwrap();
        for n in 0..bytes.len() {
            match decode_request(&bytes[..n]).unwrap() {
                Decoded::Need(needs) => assert!(needs > n, "{n} bytes asked for {needs}"),
                other => panic!("{n} of {} bytes gave {other:?}", bytes.len()),
            }
        }
    }

    /// A frame this relay cannot measure ends the stream, because nothing can find
    /// what is behind it.
    #[test]
    fn a_frame_of_an_unknown_type_ends_the_stream() {
        let mut bytes = subscribe("/x");
        bytes[3] = 99;
        assert!(decode_request(&bytes).is_err());
    }

    /// Two requests in one read, which is what a client sends where it subscribes to
    /// several patterns at once.
    #[test]
    fn the_length_of_a_request_finds_the_next_one() {
        let mut stream = subscribe("/a");
        let second = subscribe("/b");
        stream.extend_from_slice(&second);

        let Decoded::Item(first, used) = decode_request(&stream).unwrap() else {
            panic!("the first request");
        };
        assert_eq!(first, Request::Subscribe("/a"));
        assert_eq!(
            decode_request(&stream[used..]).unwrap(),
            Decoded::Item(Request::Subscribe("/b"), second.len())
        );
    }
}
