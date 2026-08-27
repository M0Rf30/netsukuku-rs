//! Generic distributed-service substrate (DHT-over-topology) that `ntk-coordinator` and
//! `ntk-andna` register on (`research/notes/02-vala-services-daemon.md` §3; RFC 0014,
//! "P2P over Netsukuku").
//!
//! - [`tuple`] — [`TupleNode`]/[`TupleGNode`] position tuples and the Chord-like
//!   `dist`/`approximate` geometry: the key→g-node mapping (RFC 0014 §2).
//! - [`hashnode`] — [`hash_to_tuple`], the hashing half of that mapping.
//! - [`routing`] — [`Handle::contact_peer`], [`Handle::replicate`] (RFC 0014 §2.2 step 5's
//!   31-node redundancy rule), and the server-side `forward_msg` hop-by-hop counterpart.
//! - [`participation`]/[`gossip`] — participation-map flood-gossip and its `retrieved_below_level`
//!   freshness rule.
//! - [`service`] — the [`PeerService`] registration trait and [`ServiceId`].
//! - [`config`] — [`Config`], every timing/redundancy constant, injectable.
//! - [`actor`] — the single-owner [`Manager`] actor and its [`Handle`].
//! - [`stub`]/[`wire`] — the outbound [`PeersStub`] seam and its real ([`RpcPeersStub`]) and
//!   test (`ntk_rpc::FakeRpcClient`-backed) transports.
//! - [`handler`] — [`PeersRpcHandler`], the inbound dispatch for the 12
//!   `MethodCall::peers_*` methods.
//!
//! **Scope**: models an always-fully-hooked node (no virtual/mid-migration addressing, no
//! guest/host network-entry bootstrap) — see [`actor::Manager::new`]'s doc comment. Coordinator,
//! ANDNA, and QSPN internals are out of scope; this crate is only the substrate they build on.

mod actor;
mod config;
mod error;
mod gossip;
mod handler;
mod hashnode;
mod origin_auth;
mod participation;
mod routing;
mod service;
mod stub;
mod tuple;
mod wire;

/// Generated protobuf types for this module's own payloads (`proto/peerservices.proto`, package
/// `ntk.peerservices.v1`). These travel inside `ntk_proto::v1::TypedValue` payloads; nothing
/// outside [`wire`](self::wire) constructs them directly.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.peerservices.v1.rs"));
}

pub use actor::{Event, Handle, Manager, Snapshot};
pub use config::Config;
pub use error::Error;
pub use handler::PeersRpcHandler;
pub use hashnode::hash_to_tuple;
pub use participation::{
    ParticipantMap, ParticipantSet, fold_to_my_granularity, is_fresher, produce_below_level,
};
pub use routing::ContactPeerError;
pub use service::{ExecError, PeerService, Refusal, ServiceId};
pub use stub::{GetRequestError, PeerMessageForwarder, PeersStub, RoutingEnv, StubCallError};
pub use tuple::{GNodeRelation, TupleGNode, TupleNode, approximate, contains, dist};
pub use wire::RpcPeersStub;
