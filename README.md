# lcm-bus

A pure-Rust [LCM](https://lcm-proj.github.io/) bus. `udpm://` is multicast,
`tcpq://` goes through a relay, and `file://` is a `.lcmlog` event log. A
payload is bytes this crate does not read.

`#![no_std]` with `alloc`, and `#![forbid(unsafe_code)]`.

```rust,no_run
use lcm_bus::{Client, Delivery, Subscriptions};

fn main() -> Result<(), lcm_bus::ClientError> {
    let mut subscriptions = Subscriptions::new();
    subscriptions.add("/example/.*")?;

    let client = Client::connect(
        "udpm://239.255.76.67:7667",
        subscriptions,
        |delivery: Delivery| println!("{}", delivery.frame.channel),
    )?;
    client.publish("/example/one", &[1, 2, 3])?;
    Ok(())
}
```

## Layers

`client` is the sockets and a reader thread. `bus` is the same protocol with
no socket and no thread — give it the bytes from an async runtime or a serial
link of your own. `wire` is the framing under both.

## Features

`std` and `patterns` are on by default.

- `patterns` gives `Subscriptions` a regex; without it a subscription is one
  channel by name. A relay matches on its own side, so `tcpq://` wants this
  only for the local filter.
- `std` gives the socket-and-thread `client`. Without it, `wire` and `bus`
  are the whole crate.
