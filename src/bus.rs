//! The protocol above framing, with no socket in it.
//!
//! [`crate::wire`] reads and writes bytes. This reads a bus: it puts
//! fragments back together, drops what no pattern wants, and holds the bytes
//! of a stream that do not yet make a frame. A caller brings the bytes from
//! wherever it likes, so an async runtime or a serial link needs none of
//! [`crate::client`].

use alloc::borrow::Cow;
use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::net::SocketAddr;
#[cfg(feature = "patterns")]
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

#[cfg(feature = "patterns")]
use fancy_regex::{Regex, RegexBuilder};

use crate::url::Speed;
use crate::wire::{Frame, WireError, udpm};

/// A subscription pattern the regex engine refuses. Compiling one is
/// not an operation of a client, so it does not give a
/// [`ClientError`](crate::ClientError).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BadPattern {
    pub pattern: String,
    pub reason: String,
}

impl fmt::Display for BadPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a usable subscription pattern: {}",
            self.pattern, self.reason
        )
    }
}

impl core::error::Error for BadPattern {}

/// A set with no pattern matches nothing, as LCM delivers nothing to a client
/// that subscribes to nothing.
///
/// A pattern goes in a group and is anchored at each end, so `a|b` matches
/// `a` or `b` and nothing more. **C LCM anchors as `^a|b$`, where the same
/// pattern means "starts with `a`, or ends with `b`".** So one string can
/// select different channels here and in a C peer. `lcm-java` agrees with
/// this crate, and so does a relay, which is Java itself.
///
/// `fancy_regex` takes a negative lookahead, and `regex` rejects one.
#[derive(Debug, Default)]
pub struct Subscriptions {
    rules: Vec<Rule>,
    /// `fancy_regex` gives up on a pattern that backtracks too far, and a
    /// name cannot do that.
    #[cfg(feature = "patterns")]
    failures: AtomicU64,
}

/// A name, as a relay reads one: a Java regex where each character of the
/// name stands for itself.
pub(crate) fn escaped(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if r"\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// What one subscription selects.
#[derive(Debug)]
enum Rule {
    /// One channel, by the name it has.
    Name(String),
    /// The channels a pattern matches.
    #[cfg(feature = "patterns")]
    Pattern { text: String, compiled: Regex },
}

/// A rule a set will take, held between the check and the change.
pub(crate) struct Checked(Rule);

impl Rule {
    /// What the caller gave, which is what `remove` names.
    fn text(&self) -> &str {
        match self {
            Self::Name(name) => name,
            #[cfg(feature = "patterns")]
            Self::Pattern { text, .. } => text,
        }
    }

    /// What a relay is given. A relay reads it as a Java regex, so a name
    /// goes with each of its characters made to stand for itself.
    fn for_a_relay(&self) -> Cow<'_, str> {
        match self {
            Self::Name(name) => Cow::Owned(escaped(name)),
            #[cfg(feature = "patterns")]
            Self::Pattern { text, .. } => Cow::Borrowed(text),
        }
    }
}

impl Subscriptions {
    /// An empty set, which matches nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// One channel, by the name it has.
    ///
    /// An LCM channel name often holds a `.`, which a pattern would take for
    /// "one of anything". This is here whether or not `patterns` is. The
    /// longest name is a log's, at [`crate::wire::log::CHANNEL_MAX`].
    pub fn add_name(&mut self, channel: &str) -> Result<(), BadPattern> {
        self.push(Self::check_name(channel)?);
        Ok(())
    }

    /// A rule that is built and read, and not in a set yet.
    ///
    /// A client that tells a relay before it changes its own set needs the
    /// two apart. A name this set will not take must not go on the wire
    /// first: the relay would hold a rule that nothing here can name, and so
    /// nothing here can take it back.
    pub(crate) fn check_name(channel: &str) -> Result<Checked, BadPattern> {
        crate::wire::check_channel(channel, crate::wire::log::CHANNEL_MAX).map_err(|e| {
            BadPattern {
                pattern: channel.to_owned(),
                reason: alloc::format!("{e}"),
            }
        })?;
        Ok(Checked(Rule::Name(channel.to_owned())))
    }

    pub(crate) fn push(&mut self, checked: Checked) {
        self.rules.push(checked.0);
    }

    /// The channels a pattern matches.
    ///
    /// The pattern goes in a group and is anchored at each end. Without the
    /// `patterns` feature this method is not here, so a crate that wanted a
    /// pattern says so when it is built and not when it runs.
    #[cfg(feature = "patterns")]
    pub fn add(&mut self, pattern: &str) -> Result<(), BadPattern> {
        self.push(Self::check(pattern)?);
        Ok(())
    }

    /// A pattern that compiles, and is not in a set yet. See
    /// [`Subscriptions::check_name`] for why the two are apart.
    #[cfg(feature = "patterns")]
    pub(crate) fn check(pattern: &str) -> Result<Checked, BadPattern> {
        let anchored = alloc::format!("^(?:{pattern})$");
        let compiled = RegexBuilder::new(&anchored)
            .backtrack_limit(BACKTRACK_LIMIT)
            .build()
            .map_err(|e| BadPattern {
                pattern: pattern.to_owned(),
                reason: alloc::format!("{e}"),
            })?;
        Ok(Checked(Rule::Pattern {
            text: pattern.to_owned(),
            compiled,
        }))
    }

    /// What a relay was told for the first subscription equal to this one,
    /// without taking it out of the set.
    ///
    /// A caller that tells the relay before it changes its own set needs the
    /// one without the other: a set that changed and a relay that was not
    /// told do not agree, and neither do the other way about.
    #[must_use]
    pub fn find(&self, subscription: &str) -> Option<String> {
        let found = self.rules.iter().find(|r| r.text() == subscription)?;
        Some(found.for_a_relay().into_owned())
    }

    /// Removes the first subscription equal to this one, and gives back what
    /// a relay was told for it, so the relay can be told to stop.
    pub fn remove(&mut self, subscription: &str) -> Option<String> {
        let found = self.rules.iter().position(|r| r.text() == subscription)?;
        let told = self.rules[found].for_a_relay().into_owned();
        self.rules.remove(found);
        Some(told)
    }

    /// Whether any subscription selects `channel`.
    pub fn matches(&self, channel: &str) -> bool {
        self.rules.iter().any(|rule| match rule {
            Rule::Name(name) => name == channel,
            #[cfg(feature = "patterns")]
            Rule::Pattern { compiled, .. } => match compiled.is_match(channel) {
                Ok(matched) => matched,
                Err(_) => {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    false
                }
            },
        })
    }

    /// The patterns that gave up. Without the `patterns` feature there are
    /// none of those, so there are none of these.
    pub fn failures(&self) -> u64 {
        #[cfg(feature = "patterns")]
        return self.failures.load(Ordering::Relaxed);
        #[cfg(not(feature = "patterns"))]
        0
    }

    /// What a relay is given for each subscription. A relay reads these as
    /// Java regular expressions, so a name comes back with each of its
    /// characters made to stand for itself.
    pub fn for_a_relay(&self) -> impl Iterator<Item = Cow<'_, str>> {
        self.rules.iter().map(Rule::for_a_relay)
    }
}

/// The channels a [`MulticastReceiver`] gives back.
pub trait Filter {
    /// Whether the channel is one to deliver.
    fn matches(&self, channel: &str) -> bool;
}

/// All of them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Everything;

/// The steps one match can take before `fancy_regex` gives up on it.
///
/// A channel name comes off the wire, and a pattern with a lookaround and a
/// quantifier takes a step for each reading of that name it could try — on
/// the thread that reads the socket. A name is short, so a pattern that
/// means anything for one is done long before this.
#[cfg(feature = "patterns")]
const BACKTRACK_LIMIT: usize = 10_000;

/// The set this crate ships. A caller with a bus of its own needs the
/// matcher as much as it needs the reassembler.
impl Filter for Subscriptions {
    fn matches(&self, channel: &str) -> bool {
        Subscriptions::matches(self, channel)
    }
}

/// So a borrowed filter is one too, and a caller can keep its own.
impl<F: Filter + ?Sized> Filter for &F {
    fn matches(&self, channel: &str) -> bool {
        (**self).matches(channel)
    }
}

impl Filter for Everything {
    fn matches(&self, _channel: &str) -> bool {
        true
    }
}

/// A message off a udpm bus, and what its datagrams said about it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Received {
    pub frame: Frame,
    /// The number its sender gave it. Each peer numbers from zero, so a
    /// number missing here is a loss from the peer this came from, and says
    /// nothing about the others.
    pub sequence: u32,
    /// One datagram held it, or many did.
    pub reassembled: bool,
}

/// A udpm bus, from datagrams to messages.
///
/// `K` names the sender of a datagram. Two peers can use one sequence number
/// at one time, so the key of a message is the peer and the number together.
#[derive(Debug)]
pub struct MulticastReceiver<F, K = SocketAddr> {
    reassembler: udpm::Reassembler<K>,
    filter: F,
}

impl<F: Filter, K: Ord + Clone> MulticastReceiver<F, K> {
    pub fn new(filter: F) -> Self {
        Self {
            reassembler: udpm::Reassembler::new(),
            filter,
        }
    }

    /// The message that `data` completes, if `filter` wants its channel.
    ///
    /// `Ok(None)` says there is nothing for the caller: the datagram was a
    /// fragment of a message that is still short of one, or it completed a
    /// message on a channel that no pattern matches.
    ///
    /// An `Err` is one datagram this receiver cannot read. A bus carries
    /// senders that this crate did not write, so a caller counts these and
    /// goes on to the next datagram.
    pub fn on_datagram(&mut self, from: K, data: &[u8]) -> Result<Option<Received>, WireError> {
        match udpm::decode(data)? {
            udpm::Datagram::Whole { sequence, frame } => {
                Ok(self.filter.matches(frame.channel).then(|| Received {
                    frame: frame.to_frame(),
                    sequence,
                    reassembled: false,
                }))
            }
            udpm::Datagram::Fragment(fragment) => {
                let sequence = fragment.sequence;
                let Some(frame) = self.reassembler.feed(from, fragment)? else {
                    return Ok(None);
                };
                Ok(self.filter.matches(&frame.channel).then_some(Received {
                    frame,
                    sequence,
                    reassembled: true,
                }))
            }
        }
    }

    /// The messages this receiver holds fragments of.
    pub fn in_flight(&self) -> usize {
        self.reassembler.in_flight()
    }

    /// The messages it dropped before they were whole, to keep to its budget.
    pub fn evicted(&self) -> u64 {
        self.reassembler.evicted()
    }

    pub fn filter(&self) -> &F {
        &self.filter
    }
}

/// The time to hold between two events of a log, so a replay goes at the
/// rate of the recording.
///
/// This holds no clock: it gives a length of time, and the caller waits in
/// the way it waits. [`crate::client`] sleeps a thread; a runtime with a
/// timer arms one.
///
/// ```
/// # use lcm_bus::bus::Pace;
/// # use lcm_bus::Speed;
/// # use core::time::Duration;
/// let mut pace = Pace::new(Speed::Rate(2.0));
/// assert_eq!(pace.hold(1_000_000), Duration::ZERO, "nothing to wait for yet");
/// assert_eq!(pace.hold(1_400_000), Duration::from_micros(200_000), "at two times");
///
/// // A time a peer chose, and not a time.
/// assert_eq!(pace.hold(i64::MAX), lcm_bus::bus::LONGEST_HOLD);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Pace {
    speed: Speed,
    previous: Option<i64>,
}

/// The longest a replay holds between two events.
///
/// A time in a log is a number a peer chose, and the difference of two of
/// them reaches 292 000 years. A replay that takes such a hold delivers one
/// event and then waits for good, with nothing to say it is waiting. A log
/// where two events are more than this apart is a log with a time in it
/// that is not a time.
pub const LONGEST_HOLD: Duration = Duration::from_secs(60 * 60);

impl Pace {
    pub fn new(speed: Speed) -> Self {
        Self {
            speed,
            previous: None,
        }
    }

    /// How long to hold before the event at `timestamp`.
    ///
    /// The first event holds for nothing, and so does one that a log
    /// recorded out of sequence. [`Speed::Unthrottled`], a rate of zero, and
    /// a rate that is not a number all give [`Duration::ZERO`]. No hold is
    /// longer than [`LONGEST_HOLD`].
    pub fn hold(&mut self, timestamp: i64) -> Duration {
        let was = self.previous.replace(timestamp);
        match (was, self.speed) {
            // Zero, a rate below zero, and a rate that is not a number are
            // none of them a rate to divide by.
            (Some(was), Speed::Rate(rate)) if rate > 0.0 => {
                // Two times a log holds are two numbers a peer chose, and
                // what is between them is not always a number.
                let gap = timestamp.checked_sub(was).unwrap_or(0).max(0);
                Duration::from_micros((gap as f64 / rate) as u64).min(LONGEST_HOLD)
            }
            _ => Duration::ZERO,
        }
    }
}

/// The bytes read off a stream and not decoded.
///
/// `tcpq://` and a log file both hold a frame that one read does not finish,
/// and both meet two frames in one read. This keeps what is left over.
///
#[cfg_attr(feature = "std", doc = "```")]
#[cfg_attr(not(feature = "std"), doc = "```ignore")]
/// # use lcm_bus::bus::ReadBuffer;
/// # use lcm_bus::wire::tcpq;
/// # use lcm_bus::wire::Decoded;
/// let mut pending = ReadBuffer::new(4096);
/// # let mut socket: &[u8] = &tcpq::publish(lcm_bus::FrameRef {
/// #     channel: "/example/one", payload: &[1, 2, 3] }).unwrap();
/// pending.fill_from(&mut socket)?;
/// loop {
///     match tcpq::decode(pending.unread())? {
///         Decoded::Item(frame, used) => {
///             println!("{} {} bytes", frame.channel, frame.payload.len());
///             pending.consume(used);
///         }
///         // Read `bytes` more, or stop if this caller will not hold them.
///         Decoded::Need(bytes) => { let _ = pending.reserve(bytes); break }
///         // A frame this crate will not take, stepped over.
///         Decoded::Skip(bytes) => pending.consume(bytes),
///     }
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ReadBuffer {
    buffer: Vec<u8>,
    start: usize,
    end: usize,
    floor: usize,
    ceiling: usize,
    /// The room [`ReadBuffer::reserve`] took and no read has used. Without
    /// it, an empty buffer goes back to its floor and undoes the reserve.
    wanted: usize,
}

impl core::fmt::Debug for ReadBuffer {
    /// The bytes are a stream, so this gives their measures and not them.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReadBuffer")
            .field("unread", &(self.end - self.start))
            .field("held", &self.buffer.len())
            .field("floor", &self.floor)
            .field("ceiling", &self.ceiling)
            .finish()
    }
}

impl ReadBuffer {
    /// `floor` is the length to read into, and the length to come back to
    /// after a frame that needed more. The ceiling is the largest message the
    /// protocol has.
    pub fn new(floor: usize) -> Self {
        Self::with_limits(floor, crate::wire::MAX_MESSAGE_LEN)
    }

    /// `ceiling` is the most this will ever hold. A frame above it is one
    /// this caller will not take, which is a decision and not an allocation:
    /// [`ReadBuffer::reserve`] says no and the caller ends the stream, in place
    /// of a buffer that follows whatever length a peer claims.
    pub fn with_limits(floor: usize, ceiling: usize) -> Self {
        let floor = floor.max(1);
        Self {
            buffer: alloc::vec![0u8; floor],
            start: 0,
            end: 0,
            floor,
            ceiling: ceiling.max(floor),
            wanted: 0,
        }
    }

    /// Room for `bytes` of unread stream, or `false` when that is above the
    /// ceiling.
    ///
    /// This takes no room. `bytes` is a length a peer claimed, and the bytes
    /// behind it can fail to come, so room is bought by what arrives: the
    /// buffer doubles where the unread bytes fill it. A reader asks this
    /// once for each read, so room taken here is room a peer buys by
    /// claiming it.
    #[must_use]
    pub fn reserve(&mut self, bytes: usize) -> bool {
        if bytes > self.ceiling {
            return false;
        }
        self.wanted = bytes;
        self.make_room();
        true
    }

    /// The bytes a decoder has not taken.
    pub fn unread(&self) -> &[u8] {
        &self.buffer[self.start..self.end]
    }

    /// Give this what a decoder took. What it was holding room for has come
    /// and gone, so the room can go too.
    ///
    /// Nothing taken is nothing to give up, so a caller that calls this with
    /// zero at the head of its loop keeps the room it asked for.
    pub fn consume(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.start = (self.start + bytes).min(self.end);
        self.wanted = 0;
    }

    /// Where to put the next bytes. Give [`ReadBuffer::filled`] how many
    /// went in.
    pub fn spare(&mut self) -> &mut [u8] {
        self.make_room();
        &mut self.buffer[self.end..]
    }

    /// How many of the [`ReadBuffer::spare`] bytes a read wrote.
    pub fn filled(&mut self, bytes: usize) {
        self.end = (self.end + bytes).min(self.buffer.len());
    }

    /// A read goes into the spare part, so a frame is not copied two times.
    #[cfg(feature = "std")]
    pub fn fill_from(&mut self, source: &mut impl std::io::Read) -> std::io::Result<usize> {
        let spare = self.spare();
        if spare.is_empty() {
            // A read of nothing into nothing reads as a closed stream, and
            // this is a full buffer.
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "the unread bytes are at the ceiling of this buffer",
            ));
        }
        let read = source.read(spare)?;
        self.filled(read);
        Ok(read)
    }

    /// The unread bytes move down when most of the buffer is behind them, so
    /// a stream of small frames does not move them on each read. A buffer
    /// that one large frame grew goes back to its floor when it empties.
    fn make_room(&mut self) {
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
            let keep = self.floor.max(self.wanted);
            if self.buffer.len() > keep {
                self.buffer = alloc::vec![0u8; keep];
            }
            return;
        }
        if self.start * 2 >= self.buffer.len() || self.end == self.buffer.len() {
            self.buffer.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        if self.end == self.buffer.len() && self.buffer.len() < self.ceiling {
            let want = (self.buffer.len() * 2).min(self.ceiling);
            self.buffer.resize(want, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "patterns")]
    fn subscriptions(patterns: &[&str]) -> Subscriptions {
        let mut subs = Subscriptions::new();
        for pattern in patterns {
            subs.add(pattern).unwrap();
        }
        subs
    }

    #[cfg(feature = "patterns")]
    #[test]
    fn patterns_match_the_whole_channel_name() {
        let subs = subscriptions(&["/example/.*"]);
        assert!(subs.matches("/example/one"));
        assert!(!subs.matches("/other/example/one"), "anchored");
        assert!(!subs.matches("/other"));
    }

    /// A negative lookahead, which the `regex` crate cannot compile.
    #[cfg(feature = "patterns")]
    #[test]
    fn a_negative_lookahead_compiles() {
        let subs = subscriptions(&["((?!private).)*"]);
        assert!(subs.matches("/example/one"));
        assert!(!subs.matches("private_channel"));
    }

    #[cfg(feature = "patterns")]
    #[test]
    fn several_patterns_are_a_union() {
        let subs = subscriptions(&["A.*", "B.*"]);
        assert!(subs.matches("ABC"));
        assert!(subs.matches("BCD"));
        assert!(!subs.matches("CDE"));
    }

    #[cfg(feature = "patterns")]
    #[test]
    fn an_alternation_is_anchored_on_both_sides() {
        let subs = subscriptions(&["a|b"]);
        assert!(subs.matches("a"));
        assert!(subs.matches("b"));
        assert!(!subs.matches("xax"));
    }

    #[cfg(feature = "patterns")]
    #[cfg(feature = "std")]
    #[test]
    fn an_invalid_pattern_says_so() {
        let mut subs = Subscriptions::new();
        let err = subs.add("[unclosed").unwrap_err();
        assert_eq!(err.pattern, "[unclosed");
        assert!(!err.reason.is_empty(), "{err}");
        // And it reaches a caller who works in `ClientError`.
        assert!(matches!(
            crate::ClientError::from(err),
            crate::ClientError::Pattern(_)
        ));
    }

    #[cfg(feature = "patterns")]
    #[test]
    fn a_pattern_goes_away_when_it_is_removed() {
        let mut subs = subscriptions(&["A.*", "B.*"]);
        assert!(subs.remove("A.*").is_some());
        assert!(subs.remove("A.*").is_none(), "one record for one add");
        assert!(!subs.matches("ABC"));
        assert!(subs.matches("BCD"));
    }

    /// A pattern with a lookaround and a nested quantifier can go above the
    /// backtrack limit of `fancy_regex`, on a name shorter than LCM permits.
    /// A channel name comes off the wire, so what one match can cost is what
    /// a peer can make this crate spend on the thread that reads the socket.
    #[cfg(feature = "patterns")]
    #[test]
    fn a_pattern_that_gives_up_matches_nothing_and_says_so() {
        let subs = subscriptions(&["(?!zzz)(a|a?)+c"]);
        assert!(!subs.matches(&"a".repeat(30)));
        assert_eq!(subs.failures(), 1);

        assert!(!subs.matches("/example/one"), "a name it can decide");
        assert_eq!(subs.failures(), 1, "and that one is not a failure");

        // The limit is what keeps the cost of one name off the reader.
        let longest = "a".repeat(crate::wire::MAX_CHANNEL_LEN);
        let started = std::time::Instant::now();
        for _ in 0..20 {
            let _ = subs.matches(&longest);
        }
        let each = started.elapsed() / 20;
        assert!(
            each < core::time::Duration::from_millis(5),
            "one name cost {each:?} on the thread that reads the socket"
        );
    }

    /// A name is a name whatever the features, and an LCM channel name often
    /// holds a `.`, which a pattern reads as one of anything.
    #[test]
    fn a_name_matches_the_channel_of_that_name_and_no_other() {
        let mut subs = Subscriptions::new();
        subs.add_name("SENSOR.GPS").unwrap();
        assert!(subs.matches("SENSOR.GPS"));
        assert!(
            !subs.matches("SENSORxGPS"),
            "and not one that reads like it"
        );

        // A relay reads what it is given as a Java regex, so a name goes
        // with each of its characters made to stand for itself.
        let told: Vec<String> = subs.for_a_relay().map(|c| c.into_owned()).collect();
        assert_eq!(told, [r"SENSOR\.GPS"]);
        assert_eq!(subs.remove("SENSOR.GPS").as_deref(), Some(r"SENSOR\.GPS"));
    }

    /// A pattern reads `.` as one of anything, which is LCM's own behaviour
    /// and the reason `add_name` is beside this.
    #[cfg(feature = "patterns")]
    #[test]
    fn a_pattern_reads_a_dot_as_a_pattern_does() {
        let subs = subscriptions(&["SENSOR.GPS"]);
        assert!(subs.matches("SENSOR.GPS"));
        assert!(subs.matches("SENSORxGPS"));
    }

    #[test]
    fn no_subscriptions_means_nothing() {
        assert!(!Subscriptions::new().matches("/any/channel"));
    }

    /// Bytes go in through the part `spare` gives, which is what a caller
    /// with no `std::io::Read` has.
    fn push(pending: &mut ReadBuffer, bytes: &[u8]) -> usize {
        let spare = pending.spare();
        let take = spare.len().min(bytes.len());
        spare[..take].copy_from_slice(&bytes[..take]);
        pending.filled(take);
        take
    }

    /// The unread bytes must survive each move down and each growth.
    #[test]
    fn pending_keeps_the_bytes_it_has_not_given_back() {
        let mut pending = ReadBuffer::new(8);
        let source: Vec<u8> = (0u8..=255).collect();
        let mut sent = 0;
        let mut taken = Vec::new();

        while sent < source.len() || !pending.unread().is_empty() {
            sent += push(&mut pending, &source[sent..]);
            // Three at a time, so the start and the end are rarely aligned.
            let take = pending.unread().len().min(3);
            taken.extend_from_slice(&pending.unread()[..take]);
            pending.consume(take);
        }
        assert_eq!(taken, source);
    }

    /// A frame above the ceiling is one this caller will not take. Saying so
    /// is a decision; growing to hold whatever length a peer claims is not.
    #[test]
    fn a_read_buffer_refuses_to_grow_past_its_ceiling() {
        let mut buffer = ReadBuffer::with_limits(64, 4_096);
        assert!(buffer.reserve(4_096), "up to the ceiling");
        assert!(!buffer.reserve(4_097), "and not past it");

        // The room comes in steps, so a claim on its own buys one of them.
        let mut sent = 0;
        let source = alloc::vec![1u8; 5_000];
        while sent < 4_096 {
            assert!(buffer.reserve(4_096));
            let took = push(&mut buffer, &source[sent..]);
            assert!(took > 0, "each turn takes more");
            sent += took;
        }
        assert_eq!(buffer.unread().len(), 4_096);
        assert!(buffer.spare().is_empty(), "and it grows no further");
    }

    /// A length a peer claims is not memory a peer has sent. A reader asks
    /// for the room one time on each read, so a claim made again on each
    /// turn must not buy room again on each turn.
    #[test]
    fn a_claim_does_not_buy_the_room_it_names_however_often_it_is_made() {
        let mut buffer = ReadBuffer::with_limits(64, 256 * 1024 * 1024);
        let one = alloc::vec![1u8];

        for turn in 0..24 {
            assert!(buffer.reserve(256 * 1024 * 1024));
            push(&mut buffer, &one);
            let room = buffer.unread().len() + buffer.spare().len();
            assert!(
                room <= 256,
                "turn {turn} of one byte took {room} bytes of room"
            );
        }
    }

    /// A client that read one large frame must not hold the room for it.
    #[test]
    fn pending_gives_back_the_room_a_large_frame_took() {
        let mut pending = ReadBuffer::new(16);
        let large = alloc::vec![7u8; 4_000];

        let mut sent = 0;
        while sent < large.len() {
            sent += push(&mut pending, &large[sent..]);
        }
        assert_eq!(pending.unread().len(), 4_000, "it grew to hold the frame");
        assert!(pending.buffer.len() >= 4_000);

        pending.consume(4_000);
        assert_eq!(pending.spare().len(), 16, "and went back to its floor");
    }
}
