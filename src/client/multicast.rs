//! A udpm bus: two sockets, and the datagrams that come in on one of them.

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use socket2::{Domain, Protocol, Socket, Type};

use crate::bus::MulticastReceiver;
use crate::url::Multicast;
use crate::wire::udpm;

use super::{
    Client, ClientError, Counters, Delivery, DeliveryHandler, Origin, ReaderExit, Receiving, Stop,
    Subscriptions, Transport, is_timeout, now_micros,
};
use super::{READ_TIMEOUT, RECV_BUFFER_BYTES, is_transient};

impl Client {
    pub(super) fn open_udpm(
        bus: Multicast,
        receiving: Option<Receiving>,
    ) -> Result<Self, ClientError> {
        let local = bus.interface.unwrap_or(Ipv4Addr::UNSPECIFIED);

        // A publisher binds no port and joins no group.
        let listening = match &receiving {
            Some(_) => {
                let receive = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
                // LCM peers frequently share a port on one host.
                receive.set_reuse_address(true)?;
                #[cfg(unix)]
                receive.set_reuse_port(true)?;

                // On some platforms `SO_RCVBUF` must come before `bind`.
                // A kernel that rejects it is not an error.
                let _ =
                    receive.set_recv_buffer_size(bus.recv_buf_size.unwrap_or(RECV_BUFFER_BYTES));
                let recv_buffer = receive.recv_buffer_size().unwrap_or(0);

                // LCM binds to the group, so the kernel drops the traffic of
                // a different group on this port.
                // Windows does not accept that.
                let bind_to = if cfg!(windows) {
                    Ipv4Addr::UNSPECIFIED
                } else {
                    bus.group
                };
                receive.bind(&SocketAddrV4::new(bind_to, bus.port).into())?;
                receive.join_multicast_v4(&bus.group, &local)?;
                Some((UdpSocket::from(receive), recv_buffer))
            }
            None => None,
        };

        // A second socket sends, and LCM leaves it unbound.
        // The source port is then this process alone, which is what lets a
        // peer tell two senders on one host apart, because the key holds it.
        let send = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        send.set_multicast_ttl_v4(bus.ttl.into())?;
        // Hear our own traffic. LCM does the same.
        send.set_multicast_loop_v4(true)?;
        // LCM joins the group on the two sockets. On Linux the membership
        // of the receive socket alone carries the loopback traffic, and LCM
        // itself lets this one fail.
        let _ = send.join_multicast_v4(&bus.group, &local);
        if bus.interface.is_some() {
            send.set_multicast_if_v4(&local)?;
        }

        let running = Arc::new(AtomicBool::new(true));
        let writable = Arc::new(AtomicBool::new(true));
        let counters = Arc::new(Counters::default());
        let subscriptions = Receiving::subscriptions(&receiving);
        let mut recv_buffer = None;
        let mut reader = None;

        if let (Some(receiving), Some((receive, bytes))) = (receiving, listening) {
            recv_buffer = Some(bytes);
            let handler = receiving.handler;
            reader = Some(
                std::thread::Builder::new()
                    .name("lcm-udpm".into())
                    .spawn({
                        let subscriptions = subscriptions.clone();
                        let running = running.clone();
                        let counters = counters.clone();
                        move || {
                            let mut exit = ReaderExit {
                                running: &running,
                                writable: None,
                                handler: &*handler,
                                cause: Some(Stop::Panicked),
                            };
                            exit.cause = udpm_reader(
                                &receive,
                                subscriptions,
                                &*handler,
                                &counters,
                                &running,
                            );
                        }
                    })
                    .map_err(ClientError::Io)?,
            );
        }

        Ok(Self {
            receives: reader.is_some(),
            transport: Transport::Udpm {
                send,
                recv_buffer,
                destination: SocketAddrV4::new(bus.group, bus.port).into(),
                sequence: AtomicU32::new(0),
                short_max: bus.short_max.unwrap_or(udpm::SHORT_MESSAGE_MAX),
            },
            subscriptions,
            counters,
            running,
            writable,
            reader: Mutex::new(reader),
            changing: Mutex::new(()),
        })
    }
}

/// Read until `close` asks this thread to stop. A different stop gives its cause.
pub(super) fn udpm_reader(
    socket: &UdpSocket,
    subscriptions: Arc<RwLock<Subscriptions>>,
    handler: &dyn DeliveryHandler,
    counters: &Counters,
    running: &AtomicBool,
) -> Option<Stop> {
    // The read timeout is what lets `close` stop this loop.
    let _ = socket.set_read_timeout(Some(READ_TIMEOUT));

    let mut receiver = MulticastReceiver::new(subscriptions);
    let mut buffer = alloc::vec![0u8; 65_536];

    while running.load(Ordering::Relaxed) {
        let (received, source) = match socket.recv_from(&mut buffer) {
            Ok(from) => from,
            Err(e) if is_timeout(&e) => continue,
            Err(e) if is_transient(&e) => {
                counters.discarded();
                continue;
            }
            Err(e) => return Some(Stop::Io(e)),
        };

        counters.received();
        // One bad sender on a shared bus must not stop the receiver.
        match receiver.on_datagram(source, &buffer[..received]) {
            Ok(Some(received)) => {
                counters.delivered();
                handler.on_delivery(Delivery {
                    frame: received.frame,
                    timestamp: now_micros(),
                    origin: Origin::Multicast {
                        peer: source,
                        sequence: received.sequence,
                    },
                });
            }
            Ok(None) => {}
            Err(_) => counters.discarded(),
        }
        counters
            .in_flight
            .store(receiver.in_flight() as u64, Ordering::Relaxed);
        counters
            .evicted
            .store(receiver.evicted(), Ordering::Relaxed);
    }
    None
}
