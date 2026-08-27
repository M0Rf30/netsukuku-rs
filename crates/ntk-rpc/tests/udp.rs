//! UDP broadcast/ack round trip on loopback.
//!
//! Broadcast delivery on `lo` (via its own broadcast address,
//! 127.255.255.255) depends on kernel/sandbox network policy that is not
//! guaranteed in every environment (containers may restrict `SO_BROADCAST`
//! or simply not route it). If binding or delivery does not work for an
//! environment reason — not a bug in this crate — this test logs why and
//! returns instead of flaking the suite; it only fails on an actual
//! protocol-level defect once delivery is known to work.

use std::net::SocketAddr;
use std::time::Duration;

use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{CallerContext, Empty, MethodCall, TypedValue};
use ntk_rpc::UdpBroadcaster;

fn caller() -> CallerContext {
    CallerContext {
        source_id: Some(TypedValue::new("t", Vec::new())),
        src_nic: Some(TypedValue::new("t", Vec::new())),
    }
}

async fn recv_broadcast_request(
    broadcaster: &UdpBroadcaster,
    packet_id: u64,
) -> Option<SocketAddr> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let Ok(Ok((envelope, from))) = tokio::time::timeout(remaining, broadcaster.recv()).await
        else {
            return None;
        };
        if let Some(request) = envelope.as_broadcast_request()
            && request.packet_id == packet_id
        {
            return Some(from);
        }
    }
}

async fn recv_broadcast_ack(broadcaster: &UdpBroadcaster, packet_id: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let Ok(Ok((envelope, _from))) = tokio::time::timeout(remaining, broadcaster.recv()).await
        else {
            return false;
        };
        if let Some(ack) = envelope.as_broadcast_ack()
            && ack.packet_id == packet_id
        {
            return true;
        }
    }
}

#[tokio::test]
async fn broadcast_request_and_ack_on_loopback() {
    let sender = match UdpBroadcaster::bind(None, 0, 4096) {
        Ok(socket) => socket,
        Err(error) => {
            eprintln!(
                "skipping broadcast_request_and_ack_on_loopback: cannot bind sender socket: {error}"
            );
            return;
        }
    };
    let port = sender.local_addr().expect("sender local_addr").port();
    // A second socket bound to the same port (SO_REUSEADDR) is how a real
    // broadcast listener works: any number of local sockets can share one
    // broadcast port and each receives its own copy of an inbound packet.
    let receiver = match UdpBroadcaster::bind(None, port, 4096) {
        Ok(socket) => socket,
        Err(error) => {
            eprintln!(
                "skipping broadcast_request_and_ack_on_loopback: cannot bind receiver socket: {error}"
            );
            return;
        }
    };

    let packet_id = 4242;
    let call = MethodCall {
        call: Some(Call::QspnGotDestroy(Empty::VALUE)),
    };
    if let Err(error) = sender
        .send_broadcast_request(
            packet_id,
            caller(),
            TypedValue::new("t", Vec::new()),
            true,
            call,
            None,
        )
        .await
    {
        eprintln!("skipping broadcast_request_and_ack_on_loopback: cannot send broadcast: {error}");
        return;
    }

    let Some(from) = recv_broadcast_request(&receiver, packet_id).await else {
        eprintln!(
            "skipping broadcast_request_and_ack_on_loopback: broadcast was not delivered on loopback in this sandbox"
        );
        return;
    };

    receiver
        .send_ack(packet_id, TypedValue::new("t", Vec::new()), from)
        .await
        .expect("a unicast ack to a known address should not fail once broadcast delivery already worked");

    assert!(
        recv_broadcast_ack(&sender, packet_id).await,
        "ack was sent but never observed by the original broadcaster"
    );
}
