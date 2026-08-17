#![cfg_attr(
    all(doc, feature = "std", feature = "patterns"),
    doc = include_str!("../README.md")
)]
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(any(test, feature = "std"))]
extern crate std;

pub mod bus;
#[cfg(feature = "std")]
pub mod client;
pub mod url;
pub mod wire;

pub use bus::{BadPattern, Subscriptions};
#[cfg(feature = "std")]
pub use client::{Client, ClientError, Deliveries, Delivery, DeliveryHandler, Origin, Stats, Stop};
pub use url::{BadUrl, BusUrl, LogFile, LogMode, Multicast, Relay, Replay, Speed, UrlProblem};
pub use wire::{Frame, FrameRef, WireError};
