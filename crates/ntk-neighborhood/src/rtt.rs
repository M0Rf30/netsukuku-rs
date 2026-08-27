//! [`IcmpRttProbe`]: the production [`RttProbe`] (`INeighborhoodNetworkInterface::measure_rtt`,
//! `research/impl/vala/neighborhood/api.vala:33`). Upstream leaves the concrete measurement
//! mechanism to the deployment -- this is the first *production* implementation in this port; the
//! only other [`RttProbe`] in the workspace, [`crate::nic::FixedRttProbe`], is a test double and
//! was (incorrectly) the one wired into `ntkd`'s real startup path -- see the fix at that call
//! site, `crates/ntkd/src/node/transport.rs`.
//!
//! # Why `spawn_blocking`, not an async ICMP crate or `AsyncFd`
//! This runs at most once per exported arc per ~29s
//! ([`crate::timing::NeighborhoodTiming::arc_monitor_interval`]'s production default) -- far below
//! any threadpool-starvation threshold. Registering the raw socket with
//! `tokio::io::unix::AsyncFd` would add real complexity (readiness-driven send/recv,
//! cancellation-safe retries across `.await` points) for a cadence a single blocking OS thread
//! with `SO_RCVTIMEO` handles just as correctly, with far less code and no new dependency.

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::pin::Pin;
use std::time::{Duration, Instant};

use socket2::{Domain, Protocol, Socket, Type};

use crate::nic::RttProbe;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const ICMP_ECHO_REQUEST: u8 = 8;
const ICMP_ECHO_REPLY: u8 = 0;

/// Total budget for one probe (socket setup + one echo request + one matching reply).
/// Comfortably inside the ~28-30s per-arc monitor cadence this runs on (module doc).
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Real [`RttProbe`] over ICMPv4 echo. See the module doc for why this exists and why it is
/// implemented with a blocking socket under `spawn_blocking` rather than an async I/O
/// registration.
#[derive(Debug, Clone, Copy, Default)]
pub struct IcmpRttProbe;

impl RttProbe for IcmpRttProbe {
    fn measure_rtt<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
        peer_addr: &'a str,
    ) -> BoxFuture<'a, Option<u64>> {
        let my_dev = my_dev.to_owned();
        let my_addr = my_addr.to_owned();
        let peer_addr = peer_addr.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || ping_once(&my_dev, &my_addr, &peer_addr))
                .await
                .unwrap_or(None)
        })
    }
}

/// One blocking ICMP echo request/reply round trip, bound to `dev`/`my_addr`. `None` on any
/// error or timeout -- [`RttProbe::measure_rtt`]'s documented contract, mirroring upstream's
/// `rtt == -1` (`neighborhood.vala:253-259`). Never panics/unwraps on anything read off the
/// network -- both addresses and every received byte are peer-influenced.
fn ping_once(dev: &str, my_addr: &str, peer_addr: &str) -> Option<u64> {
    let my_ip: Ipv4Addr = my_addr.parse().ok()?;
    let peer_ip: Ipv4Addr = peer_addr.parse().ok()?;

    let (socket, is_raw) = open_icmp_socket()?;
    socket.bind_device(Some(dev.as_bytes())).ok()?;
    socket
        .bind(&SocketAddr::V4(SocketAddrV4::new(my_ip, 0)).into())
        .ok()?;

    // A "ping" (`SOCK_DGRAM`) socket has Linux overwrite the ICMP identifier with the socket's
    // own bound local port on every send, and demultiplexes replies by that same port (kernel
    // commit c319b4d76b9e, "Message identifiers ... are interpreted as local ports"); a raw
    // socket does neither, so this crate must pick its own identifier there and verify id+seq
    // itself against every packet the raw socket sees (including its own outbound request,
    // which a raw ICMP socket normally observes too).
    let ident: u16 = if is_raw {
        (RandomState::new().build_hasher().finish() & 0xffff) as u16
    } else {
        socket.local_addr().ok()?.as_socket_ipv4()?.port()
    };
    let sequence: u16 = 1;

    let std_socket: UdpSocket = socket.into();
    std_socket.set_read_timeout(Some(PROBE_TIMEOUT)).ok()?;
    let sent_at = Instant::now();
    std_socket
        .send_to(
            &build_echo_request(ident, sequence),
            SocketAddr::V4(SocketAddrV4::new(peer_ip, 0)),
        )
        .ok()?;

    let mut buf = [0u8; 128];
    loop {
        let elapsed = sent_at.elapsed();
        if elapsed >= PROBE_TIMEOUT {
            return None;
        }
        std_socket
            .set_read_timeout(Some(PROBE_TIMEOUT - elapsed))
            .ok()?;
        let (n, from) = std_socket.recv_from(&mut buf).ok()?;
        let SocketAddr::V4(from) = from else {
            continue;
        };
        if *from.ip() != peer_ip {
            continue;
        }
        let Some(reply) = parse_icmp(&buf[..n], is_raw) else {
            continue;
        };
        if reply.kind != ICMP_ECHO_REPLY || reply.id != ident || reply.sequence != sequence {
            continue;
        }
        return Some(sent_at.elapsed().as_micros() as u64);
    }
}

/// Opens an ICMP socket, preferring the unprivileged "ping" socket (`SOCK_DGRAM`, gated by
/// Linux's `ping_group_range`) and falling back to a raw socket (`SOCK_RAW`, requires
/// `CAP_NET_RAW`) when the former is refused. Returns `(socket, is_raw)`; `None` when neither
/// could be opened at all, which [`ping_once`] surfaces as its documented `None`.
fn open_icmp_socket() -> Option<(Socket, bool)> {
    if let Ok(socket) = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::ICMPV4)) {
        return Some((socket, false));
    }
    Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4))
        .ok()
        .map(|socket| (socket, true))
}

struct IcmpReply {
    kind: u8,
    id: u16,
    sequence: u16,
}

/// Parses one ICMP message out of a received datagram. `is_raw` selects whether the datagram
/// still carries its IPv4 header (`SOCK_RAW` sockets always include it on receive, regardless of
/// `IP_HDRINCL`) or not (`SOCK_DGRAM` ping sockets deliver the ICMP message alone -- "data sent
/// and received include ICMP headers", not IP ones, same kernel commit as [`open_icmp_socket`]'s
/// doc). Malformed/short input returns `None` rather than panicking; this is wire data from
/// whatever host happened to reply.
fn parse_icmp(buf: &[u8], is_raw: bool) -> Option<IcmpReply> {
    let icmp = if is_raw {
        let ihl = usize::from(*buf.first()? & 0x0f) * 4;
        buf.get(ihl..)?
    } else {
        buf
    };
    if icmp.len() < 8 {
        return None;
    }
    Some(IcmpReply {
        kind: icmp[0],
        id: u16::from_be_bytes([icmp[4], icmp[5]]),
        sequence: u16::from_be_bytes([icmp[6], icmp[7]]),
    })
}

/// Builds an 8-byte ICMP echo request with a correct RFC 1071 checksum. No payload -- this probe
/// only needs round-trip timing, not payload contents.
fn build_echo_request(ident: u16, sequence: u16) -> [u8; 8] {
    let mut packet = [0u8; 8];
    packet[0] = ICMP_ECHO_REQUEST;
    packet[4..6].copy_from_slice(&ident.to_be_bytes());
    packet[6..8].copy_from_slice(&sequence.to_be_bytes());
    let checksum = internet_checksum(&packet);
    packet[2..4].copy_from_slice(&checksum.to_be_bytes());
    packet
}

/// RFC 1071 Internet checksum: one's-complement sum of 16-bit words, carries folded back in,
/// then complemented. The same algorithm ICMP, IP, TCP, and UDP all share.
fn internet_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let (chunks, remainder) = data.as_chunks::<2>();
    for chunk in chunks {
        sum += u32::from(u16::from_be_bytes(*chunk));
    }
    if let [last] = *remainder {
        sum += u32::from(last) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

#[cfg(test)]
mod tests {
    use super::{build_echo_request, internet_checksum, parse_icmp};

    /// RFC 1071's defining property: summing a correctly-checksummed message, checksum field
    /// included, always yields zero.
    #[test]
    fn internet_checksum_of_a_correctly_checksummed_packet_is_zero() {
        let packet = build_echo_request(0x1234, 7);
        assert_eq!(internet_checksum(&packet), 0);
    }

    #[test]
    fn build_echo_request_encodes_type_id_and_sequence() {
        let packet = build_echo_request(0xabcd, 42);
        assert_eq!(packet[0], super::ICMP_ECHO_REQUEST);
        assert_eq!(packet[1], 0, "code must be 0 for an echo request");
        assert_eq!(u16::from_be_bytes([packet[4], packet[5]]), 0xabcd);
        assert_eq!(u16::from_be_bytes([packet[6], packet[7]]), 42);
    }

    #[test]
    fn parse_icmp_reads_a_dgram_reply_with_no_ip_header() {
        let reply = build_echo_reply(99, 3);
        let parsed = parse_icmp(&reply, false).expect("well-formed reply must parse");
        assert_eq!(parsed.kind, super::ICMP_ECHO_REPLY);
        assert_eq!(parsed.id, 99);
        assert_eq!(parsed.sequence, 3);
    }

    #[test]
    fn parse_icmp_skips_a_raw_sockets_ip_header() {
        let icmp = build_echo_reply(99, 3);
        let mut raw = vec![
            0x45u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]; // 20-byte IHL=5 IPv4 header
        raw.extend_from_slice(&icmp);
        let parsed = parse_icmp(&raw, true).expect("well-formed raw reply must parse");
        assert_eq!(parsed.id, 99);
        assert_eq!(parsed.sequence, 3);
    }

    #[test]
    fn parse_icmp_rejects_truncated_input_instead_of_panicking() {
        assert!(parse_icmp(&[], false).is_none());
        assert!(parse_icmp(&[8, 0, 0], false).is_none());
        assert!(parse_icmp(&[0x45], true).is_none());
    }

    fn build_echo_reply(id: u16, sequence: u16) -> [u8; 8] {
        let mut packet = build_echo_request(id, sequence);
        packet[0] = super::ICMP_ECHO_REPLY;
        packet[2] = 0;
        packet[3] = 0;
        let checksum = internet_checksum(&packet);
        packet[2..4].copy_from_slice(&checksum.to_be_bytes());
        packet
    }

    /// Requires either `CAP_NET_RAW` or membership in `ping_group_range` -- not guaranteed in an
    /// unprivileged CI sandbox, so this stays `#[ignore]`d per this repo's convention (every
    /// other privilege-gated test in the workspace does the same). Run explicitly with:
    /// `cargo test -p ntk-neighborhood --lib -- --ignored icmp_probe_measures_a_real_loopback_round_trip`
    #[tokio::test]
    #[ignore = "requires CAP_NET_RAW or ping_group_range membership for an ICMP socket"]
    async fn icmp_probe_measures_a_real_loopback_round_trip() {
        use crate::nic::RttProbe;

        let probe = super::IcmpRttProbe;
        let rtt = probe.measure_rtt("lo", "127.0.0.1", "127.0.0.1").await;
        assert!(
            rtt.is_some(),
            "a loopback ping must succeed when privileged"
        );
    }
}
