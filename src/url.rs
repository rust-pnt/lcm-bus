//! Bus addresses, in the URL forms that LCM uses.

use alloc::string::{String, ToString};
use core::fmt;
use core::net::Ipv4Addr;

/// The group and port LCM uses when a `udpm://` URL gives none.
pub const DEFAULT_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 76, 67);
pub const DEFAULT_UDPM_PORT: u16 = 7667;

/// A datagram holds the long header, a channel name, its NUL, and one byte
/// of payload. Below this no message can be sent at all.
pub const MIN_SHORT_MAX: usize = 22;
/// The payload of one IPv4 datagram.
pub const MAX_UDP_PAYLOAD: usize = 65_507;
/// `short_max` counts the bytes after the short header, so the largest one
/// a datagram holds is that much less than the datagram itself. Above this a
/// small message goes and a large one fails at the socket.
pub const MAX_SHORT_MAX: usize = MAX_UDP_PAYLOAD - 8;

/// The address LCM uses when a `tcpq://` URL gives none.
pub const DEFAULT_TCPQ_HOST: &str = "127.0.0.1";
pub const DEFAULT_TCPQ_PORT: u16 = 7700;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
/// A bus to open, parsed from a URL. See [`BusUrl::parse`].
pub enum BusUrl {
    /// `udpm://group:port[?ttl=n&recv_buf_size=n&interface=a.b.c.d&short_max=n]`
    Udpm(Multicast),
    /// `tcpq://host:port` — a relay.
    Tcpq(Relay),
    /// `file://path[?mode=r|w|a&speed=n&start_timestamp=n]` — a log file.
    File(LogFile),
}

/// A relay to connect to. LCM keeps no default host, and this uses the one
/// its own relay listens on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relay {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for Relay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// A log file, in the LCM log format. `path` and `mode` say which file and
/// what to do with it; [`Replay`] says how to read it.
#[derive(Debug, Clone, PartialEq)]
pub struct LogFile {
    pub path: String,
    pub mode: LogMode,
    pub replay: Replay,
}

/// How to read a log. A writer takes no notice of these.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Replay {
    pub speed: Speed,
    /// Start at this time, in microseconds since the Unix epoch.
    pub start_timestamp: Option<i64>,
}

/// The rate of a replay against the times in the log.
///
/// C reads a rate at or below zero as fast as it goes. Only `0` says that
/// here, and a rate below zero is a URL this refuses, because a rate below
/// zero is more of a mistake than a wish.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Speed {
    /// `2.0` is two times as fast as the log was recorded.
    Rate(f64),
    /// As fast as the reader goes, which LCM writes as `speed=0`.
    Unthrottled,
}

impl Default for Speed {
    fn default() -> Self {
        Self::Rate(1.0)
    }
}

impl fmt::Display for Speed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rate(rate) => write!(f, "{rate}"),
            Self::Unthrottled => f.write_str("0"),
        }
    }
}

/// What a `file://` bus does with the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogMode {
    /// Replay the log to the handler. This is the LCM default.
    Read,
    /// Write each published message. This first makes the file empty.
    Write,
    /// Write each published message at the end of the file.
    Append,
}

/// A multicast bus. [`Default`] gives the LCM defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multicast {
    pub group: Ipv4Addr,
    pub port: u16,
    /// The multicast hop limit.
    /// At the LCM default of 0 a datagram stays on this host.
    pub ttl: u8,
    /// `SO_RCVBUF`.
    /// The loss of one datagram is the loss of the full fragmented message,
    /// so LCM takes a large buffer.
    pub recv_buf_size: Option<usize>,
    /// The interface to join the group on and to send on. This is not LCM syntax.
    /// Policy routing that sends `239.0.0.0/8` to a different interface needs this.
    pub interface: Option<Ipv4Addr>,
    /// The datagram payload limit. This is not LCM syntax.
    /// Set 1435 to speak to a macOS peer from a host that is not macOS.
    pub short_max: Option<usize>,
}

impl Default for Multicast {
    fn default() -> Self {
        Self {
            group: DEFAULT_GROUP,
            port: DEFAULT_UDPM_PORT,
            ttl: 0,
            recv_buf_size: None,
            interface: None,
            short_max: None,
        }
    }
}

/// A URL that did not parse, and what was wrong with it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BadUrl {
    pub url: String,
    pub problem: UrlProblem,
}

/// The fault in a URL. A caller that only prints one uses [`fmt::Display`],
/// and one that acts on the difference matches on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum UrlProblem {
    Scheme,
    Group,
    /// A group that is not in `224.0.0.0/4`. LCM sends to it and no peer
    /// hears it.
    NotMulticast,
    Port,
    Ttl,
    RecvBufSize,
    Interface,
    ShortMax,
    /// A `file://` URL with nothing after it.
    NoPath,
    Mode,
    Speed,
    StartTimestamp,
}

impl fmt::Display for UrlProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Scheme => "the scheme is not udpm://, tcpq:// or file://",
            Self::Group => "the group is not an IPv4 address",
            Self::NotMulticast => "the group is not a multicast address",
            Self::Port => "the port is not a number",
            Self::Ttl => "the ttl is not a number from 0 to 255",
            Self::RecvBufSize => "the recv_buf_size is not a number of bytes",
            Self::Interface => "the interface is not an IPv4 address",
            Self::ShortMax => "the short_max is not a datagram length",
            Self::NoPath => "there is no path",
            Self::Mode => "the mode is not r, w or a",
            Self::Speed => "the speed is not a rate above zero",
            Self::StartTimestamp => "the start_timestamp is not a number of microseconds",
        })
    }
}

impl fmt::Display for BadUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a usable bus URL: {}",
            self.url, self.problem
        )
    }
}

impl core::error::Error for BadUrl {}

impl BadUrl {
    fn new(url: &str, problem: UrlProblem) -> Self {
        Self {
            url: url.to_string(),
            problem,
        }
    }
}

fn param<T: core::str::FromStr>(value: &str, url: &str, problem: UrlProblem) -> Result<T, BadUrl> {
    value.parse().map_err(|_| BadUrl::new(url, problem))
}

/// The URL this came from, so a caller can record the bus it reached and
/// read that record back.
impl fmt::Display for BusUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcpq(relay) => write!(f, "tcpq://{relay}"),
            Self::Udpm(udpm) => {
                write!(f, "udpm://{}:{}?ttl={}", udpm.group, udpm.port, udpm.ttl)?;
                if let Some(bytes) = udpm.recv_buf_size {
                    write!(f, "&recv_buf_size={bytes}")?;
                }
                if let Some(address) = udpm.interface {
                    write!(f, "&interface={address}")?;
                }
                if let Some(bytes) = udpm.short_max {
                    write!(f, "&short_max={bytes}")?;
                }
                Ok(())
            }
            Self::File(log) => {
                let mode = match log.mode {
                    LogMode::Read => 'r',
                    LogMode::Write => 'w',
                    LogMode::Append => 'a',
                };
                write!(
                    f,
                    "file://{}?mode={mode}&speed={}",
                    log.path, log.replay.speed
                )?;
                if let Some(at) = log.replay.start_timestamp {
                    write!(f, "&start_timestamp={at}")?;
                }
                Ok(())
            }
        }
    }
}

impl core::str::FromStr for BusUrl {
    type Err = BadUrl;

    fn from_str(url: &str) -> Result<Self, BadUrl> {
        Self::parse(url)
    }
}

impl BusUrl {
    /// Read a `udpm://`, `tcpq://` or `file://` URL.
    pub fn parse(url: &str) -> Result<Self, BadUrl> {
        let bad = |problem: UrlProblem| BadUrl::new(url, problem);

        if let Some(rest) = url.strip_prefix("tcpq://") {
            // The last colon, so an IPv6 host keeps its own.
            let (host, port) = match rest.rsplit_once(':') {
                Some((host, port)) => (host, param(port, url, UrlProblem::Port)?),
                None => (rest, DEFAULT_TCPQ_PORT),
            };
            return Ok(Self::Tcpq(Relay {
                host: match host {
                    "" => DEFAULT_TCPQ_HOST.to_string(),
                    host => host.to_string(),
                },
                port,
            }));
        }

        if let Some(rest) = url.strip_prefix("file://") {
            return Self::parse_file(rest, url);
        }

        let Some(rest) = url.strip_prefix("udpm://") else {
            return Err(bad(UrlProblem::Scheme));
        };
        let (address, query) = match rest.split_once('?') {
            Some((address, query)) => (address, Some(query)),
            None => (rest, None),
        };

        let mut udpm = Multicast::default();
        if !address.is_empty() {
            let (group, port) = match address.split_once(':') {
                Some((group, port)) => (group, Some(port)),
                None => (address, None),
            };
            udpm.group = param(group, url, UrlProblem::Group)?;
            // LCM sends to a unicast group and no peer hears it.
            // It gives no error, so this parser does.
            if !udpm.group.is_multicast() {
                return Err(bad(UrlProblem::NotMulticast));
            }
            if let Some(port) = port {
                udpm.port = param(port, url, UrlProblem::Port)?;
            }
        }

        for pair in query.into_iter().flat_map(|q| q.split('&')) {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            match key {
                "ttl" => udpm.ttl = param(value, url, UrlProblem::Ttl)?,
                "recv_buf_size" => {
                    udpm.recv_buf_size = Some(param(value, url, UrlProblem::RecvBufSize)?)
                }
                "interface" => udpm.interface = Some(param(value, url, UrlProblem::Interface)?),
                "short_max" => {
                    // Below the header and its channel name no payload byte
                    // fits, and above the datagram of IPv4 none is sent.
                    let bytes: usize = param(value, url, UrlProblem::ShortMax)?;
                    if !(MIN_SHORT_MAX..=MAX_SHORT_MAX).contains(&bytes) {
                        return Err(bad(UrlProblem::ShortMax));
                    }
                    udpm.short_max = Some(bytes);
                }
                // LCM has more parameters. This parser ignores an unknown one.
                _ => {}
            }
        }

        Ok(Self::Udpm(udpm))
    }

    fn parse_file(rest: &str, url: &str) -> Result<Self, BadUrl> {
        let (path, query) = match rest.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (rest, None),
        };
        if path.is_empty() {
            return Err(BadUrl::new(url, UrlProblem::NoPath));
        }

        let mut log = LogFile {
            path: path.to_string(),
            mode: LogMode::Read,
            replay: Replay::default(),
        };
        for pair in query.into_iter().flat_map(|q| q.split('&')) {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            match key {
                "mode" => {
                    log.mode = match value {
                        "r" => LogMode::Read,
                        "w" => LogMode::Write,
                        "a" => LogMode::Append,
                        _ => return Err(BadUrl::new(url, UrlProblem::Mode)),
                    }
                }
                "speed" => {
                    // A rate that is not a number, or is below zero, is not
                    // a rate. `-0.0` is below zero and reads as `0.0`, which
                    // is the one value that means the opposite of slow.
                    let rate: f64 = param(value, url, UrlProblem::Speed)?;
                    if rate.is_nan() || rate.is_sign_negative() {
                        return Err(BadUrl::new(url, UrlProblem::Speed));
                    }
                    // `0` is LCM for as fast as the reader goes, and a rate
                    // too small for an `f64` to hold reads as `0` as well.
                    log.replay.speed = match rate {
                        0.0 => Speed::Unthrottled,
                        rate => Speed::Rate(rate),
                    };
                }
                "start_timestamp" => {
                    log.replay.start_timestamp =
                        Some(param(value, url, UrlProblem::StartTimestamp)?)
                }
                _ => {}
            }
        }

        Ok(Self::File(log))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn udpm(url: &str) -> Multicast {
        match BusUrl::parse(url).unwrap() {
            BusUrl::Udpm(udpm) => udpm,
            other => panic!("expected udpm, got {other:?}"),
        }
    }

    #[test]
    fn lcms_default_multicast_address_parses() {
        assert_eq!(
            udpm("udpm://239.255.76.67:7667?ttl=0"),
            Multicast {
                group: Ipv4Addr::new(239, 255, 76, 67),
                port: 7667,
                ..Multicast::default()
            }
        );
    }

    /// LCM fills in the group and the port when the URL gives no address.
    #[test]
    fn an_empty_udpm_url_is_the_lcm_default_bus() {
        assert_eq!(udpm("udpm://"), Multicast::default());
        assert_eq!(udpm("udpm://239.255.76.67").port, DEFAULT_UDPM_PORT);
        assert_eq!(udpm("udpm://?ttl=1").group, DEFAULT_GROUP);
    }

    #[test]
    fn every_parameter_is_read() {
        let url =
            "udpm://239.255.76.67:7667?ttl=1&interface=10.0.0.5&short_max=1435&recv_buf_size=4096";
        assert_eq!(
            udpm(url),
            Multicast {
                group: Ipv4Addr::new(239, 255, 76, 67),
                port: 7667,
                ttl: 1,
                recv_buf_size: Some(4096),
                interface: Some(Ipv4Addr::new(10, 0, 0, 5)),
                short_max: Some(1435),
            }
        );
    }

    /// What `Display` writes must parse back to the same bus.
    #[test]
    fn a_url_goes_out_and_comes_back() {
        for url in [
            "tcpq://",
            "tcpq://10.0.0.1:7701",
            "udpm://",
            "udpm://239.255.76.67:7667?ttl=3",
            "udpm://239.255.76.67:7667?ttl=1&recv_buf_size=4096&interface=10.0.0.2&short_max=1435",
            "file:///tmp/a.lcmlog",
            "file:///tmp/a.lcmlog?mode=a&speed=0.5&start_timestamp=1700000000",
        ] {
            let parsed = BusUrl::parse(url).expect(url);
            let written = alloc::format!("{parsed}");
            assert_eq!(
                BusUrl::parse(&written).expect(&written),
                parsed,
                "{url} wrote {written}"
            );
        }
    }

    #[test]
    fn unknown_parameters_are_ignored() {
        assert!(BusUrl::parse("udpm://239.255.76.67:7667?transmit_only=true").is_ok());
    }

    #[test]
    fn a_relay_gets_lcms_default_address() {
        for (url, host, port) in [
            ("tcpq://", DEFAULT_TCPQ_HOST, DEFAULT_TCPQ_PORT),
            ("tcpq://localhost", "localhost", 7700),
            ("tcpq://10.0.0.1:7701", "10.0.0.1", 7701),
        ] {
            assert_eq!(
                BusUrl::parse(url).unwrap(),
                BusUrl::Tcpq(Relay {
                    host: host.to_string(),
                    port
                })
            );
        }
    }

    fn file(url: &str) -> LogFile {
        match BusUrl::parse(url).unwrap() {
            BusUrl::File(file) => file,
            other => panic!("expected a log, got {other:?}"),
        }
    }

    /// LCM reads a log at speed 1 by default.
    #[test]
    fn a_log_reads_by_default() {
        assert_eq!(
            file("file:///tmp/run.lcmlog"),
            LogFile {
                path: "/tmp/run.lcmlog".to_string(),
                mode: LogMode::Read,
                replay: Replay::default(),
            }
        );
        assert_eq!(file("file://run.lcmlog").path, "run.lcmlog", "relative too");
    }

    #[test]
    fn a_log_reads_its_parameters() {
        let log = file("file:///tmp/a?mode=a&speed=2.5&start_timestamp=1700000000000000");
        assert_eq!(log.mode, LogMode::Append);
        assert_eq!(log.replay.speed, Speed::Rate(2.5));
        assert_eq!(log.replay.start_timestamp, Some(1_700_000_000_000_000));
        assert_eq!(file("file:///tmp/a?mode=w").mode, LogMode::Write);
    }

    #[test]
    fn malformed_urls_say_what_is_wrong() {
        for (url, expected) in [
            ("http://example.com", UrlProblem::Scheme),
            ("file://", UrlProblem::NoPath),
            ("file:///tmp/a?mode=x", UrlProblem::Mode),
            ("file:///tmp/a?speed=fast", UrlProblem::Speed),
            ("udpm://10.0.0.1:7667", UrlProblem::NotMulticast),
            ("udpm://not-an-address:1", UrlProblem::Group),
            ("udpm://239.255.76.67:abc", UrlProblem::Port),
            ("udpm://239.255.76.67?ttl=256", UrlProblem::Ttl),
        ] {
            assert_eq!(BusUrl::parse(url).unwrap_err().problem, expected, "{url}");
        }
    }

    /// These parse as numbers and then make each publish fail, or each
    /// replay wait for a time that never comes.
    #[test]
    fn numbers_outside_their_range_are_refused_at_the_url() {
        for url in [
            "udpm://239.255.76.67?short_max=0",
            "udpm://239.255.76.67?short_max=21",
            "udpm://239.255.76.67?short_max=65500",
            "udpm://239.255.76.67?short_max=65508",
            "udpm://239.255.76.67?short_max=18446744073709551615",
        ] {
            assert_eq!(
                BusUrl::parse(url).unwrap_err().problem,
                UrlProblem::ShortMax,
                "{url}"
            );
        }
        for url in [
            "file:///tmp/a?speed=NaN",
            "file:///tmp/a?speed=-1",
            "file:///tmp/a?speed=-inf",
        ] {
            assert_eq!(
                BusUrl::parse(url).unwrap_err().problem,
                UrlProblem::Speed,
                "{url}"
            );
        }

        // `0` is LCM for as fast as the reader goes, and `inf` is a rate.
        assert_eq!(
            file("file:///tmp/a?speed=0").replay.speed,
            Speed::Unthrottled
        );
        assert!(BusUrl::parse("file:///tmp/a?speed=inf").is_ok());
        assert!(BusUrl::parse("udpm://239.255.76.67?short_max=22").is_ok());
        assert!(BusUrl::parse("udpm://239.255.76.67?short_max=65499").is_ok());
    }
}
