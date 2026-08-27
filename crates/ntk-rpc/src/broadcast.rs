//! Broadcast/datagram transport over UDP, per-NIC — the Rust replacement
//! for zcd's `datagram_net_listen`/`send_datagram_net`
//! (`research/impl/vala/pth-tasklet/tasklet_blocking_sockets.vala:181-241`,
//! research/notes/02-vala-services-daemon.md §1).

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use ntk_proto::v1::{Auth, CallerContext, Envelope, MethodCall, ProtocolVersion, TypedValue};
use prost::Message;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::error::RpcError;

/// A per-NIC UDP broadcast endpoint. Unframed — one packet is one
/// `Envelope`, no reassembly — matching zcd's datagram semantics;
/// `max_packet_size` must fit in one UDP datagram (zcd's own limit is
/// 60000 bytes, `research/impl/vala/zcd/listeners.vala:318`).
#[derive(Debug)]
pub struct UdpBroadcaster {
    socket: UdpSocket,
    max_packet_size: usize,
    broadcast_addr: SocketAddr,
}

impl UdpBroadcaster {
    /// Binds a UDP socket for broadcast on `port`. Always sets
    /// `SO_BROADCAST` and `SO_REUSEADDR`; when `device` is given, also sets
    /// Linux's `SO_BINDTODEVICE` to restrict the socket to that NIC (this
    /// requires `CAP_NET_RAW` — pass `None` for a loopback/test socket with
    /// no device restriction).
    pub fn bind(device: Option<&str>, port: u16, max_packet_size: usize) -> Result<Self, RpcError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_broadcast(true)?;
        if let Some(device) = device {
            socket.bind_device(Some(device.as_bytes()))?;
        }
        socket.set_nonblocking(true)?;
        let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));
        socket.bind(&bind_addr.into())?;
        let std_socket: std::net::UdpSocket = socket.into();
        let socket = UdpSocket::from_std(std_socket)?;
        Ok(Self {
            socket,
            max_packet_size,
            broadcast_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, port)),
        })
    }

    /// The bound local address (useful when `port = 0` picks an ephemeral
    /// port, e.g. in tests).
    pub fn local_addr(&self) -> Result<SocketAddr, RpcError> {
        Ok(self.socket.local_addr()?)
    }

    /// Broadcasts a `BroadcastRequest` to 255.255.255.255 on the bound
    /// port. `auth` is attached to the outbound `Envelope`
    /// (`ntk_proto::v1::Envelope::with_auth`) when present — this crate's callers decide
    /// whether/how to sign; `None` sends today's unauthenticated envelope unchanged.
    pub async fn send_broadcast_request(
        &self,
        packet_id: u64,
        caller: CallerContext,
        broadcast_id: TypedValue,
        send_ack: bool,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> Result<(), RpcError> {
        let mut envelope = Envelope::broadcast_request(
            ProtocolVersion::CURRENT,
            packet_id,
            caller,
            broadcast_id,
            send_ack,
            call,
        );
        if let Some(auth) = auth {
            envelope = envelope.with_auth(auth);
        }
        self.send_to(&envelope, self.broadcast_addr).await
    }

    /// Sends a `BroadcastAck` directly back to `to`. Upstream resends this
    /// 3x at random 10-200ms as a best-effort measure (notes/02 §1);
    /// retransmission is the caller's responsibility — this sends exactly
    /// one packet.
    pub async fn send_ack(
        &self,
        packet_id: u64,
        src_nic: TypedValue,
        to: SocketAddr,
    ) -> Result<(), RpcError> {
        let envelope = Envelope::broadcast_ack(ProtocolVersion::CURRENT, packet_id, src_nic);
        self.send_to(&envelope, to).await
    }

    async fn send_to(&self, envelope: &Envelope, to: SocketAddr) -> Result<(), RpcError> {
        let mut buf = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut buf)?;
        if buf.len() > self.max_packet_size {
            return Err(RpcError::FrameTooLarge {
                size: buf.len(),
                max: self.max_packet_size,
            });
        }
        self.socket.send_to(&buf, to).await?;
        Ok(())
    }

    /// Receives and decodes the next datagram — dispatching on whether it
    /// turns out to hold a `BroadcastRequest` or a `BroadcastAck` is the
    /// caller's responsibility, mirroring how zcd's listener hands the
    /// parsed packet to whichever of `request`/`ack` applies.
    pub async fn recv(&self) -> Result<(Envelope, SocketAddr), RpcError> {
        let mut buf = vec![0u8; self.max_packet_size];
        let (len, from) = self.socket.recv_from(&mut buf).await?;
        let envelope = Envelope::decode(&buf[..len])?;
        Ok((envelope, from))
    }
}
