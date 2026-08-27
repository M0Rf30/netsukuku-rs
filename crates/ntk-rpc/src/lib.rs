//! Transport and dispatch for the netsukuku-rs inter-node RPC protocol —
//! the Rust replacement for zcd (research/notes/02-vala-services-daemon.md
//! §1). Builds on [`ntk_proto`]'s wire types with:
//!
//! - length-delimited framing for unicast TCP calls ([`EnvelopeCodec`],
//!   [`TcpRpcClient`], [`TcpServer`]);
//! - per-NIC UDP broadcast/ack ([`UdpBroadcaster`]);
//! - an object-safe [`RpcClient`] seam with a real and a fake
//!   implementation ([`TcpRpcClient`], [`FakeRpcClient`]);
//! - the local-vs-remote error distinction from notes/02 §1, made explicit
//!   in [`RpcError`].
//!
//! Depends on `ntk-proto` only — no other sibling crate.

mod broadcast;
mod client;
mod codec;
mod error;
mod fake;
mod server;

pub use broadcast::UdpBroadcaster;
pub use client::{RpcClient, TcpRpcClient};
pub use codec::EnvelopeCodec;
pub use error::RpcError;
pub use fake::{FailureFactory, FakeRpcClient};
pub use server::{FnHandler, RpcHandler, TcpServer};
