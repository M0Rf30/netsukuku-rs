//! Coordinator: the reserved-position allocator and per-level election
//! (`research/notes/01-vala-core-routing.md` §7). Not a separate network protocol — implemented
//! entirely **as** a [`ntk_peerservices::PeerService`] over PeerServices' fixed-keys DHT
//! (upstream `coordinator/Makefile.am:39,45`, the only core module with a real library
//! dependency).
//!
//! - [`domain`] — [`Booking`], [`GnodeMemory`] (the fixed-keys record itself),
//!   [`Reservation`]/[`ReserveError`], [`Snapshot`], [`Event`], [`HandOff`] (the migration
//!   hand-off protocol), [`PropagationArgs`].
//! - [`traits`] — the capabilities Coordinator needs from the rest of the daemon, declared as
//!   its own traits rather than a dependency on `ntk-qspn`/`ntk-hooking`: [`CoordinatorMap`],
//!   the four enter-protocol handlers bundled in [`EnterHandlers`], [`PropagationHandler`], and
//!   the outbound seam [`CoordinatorStub`]/[`CoordinatorStubFactory`].
//! - [`config`] — [`Config`], every timing/redundancy constant, injectable.
//! - [`actor`] — the single-owner [`Manager`] actor and its [`Handle`] (the servant role: runs
//!   the fixed-keys database and this node's own propagation orchestration).
//! - [`client`] — [`CoordinatorClient`], the DHT-routed proxy to whichever node is currently
//!   elected servant for a level.
//! - [`service`] — [`CoordinatorService`], the [`ntk_peerservices::PeerService`] registration.
//! - [`handler`] — [`CoordinatorRpcHandler`], the inbound dispatch for the 5
//!   `MethodCall::coordinator_execute_*` methods.
//! - [`fake`] — [`FakeCoordinatorStubFactory`], the in-memory test double for the outbound seam;
//!   [`RpcCoordinatorStub`] is the real-transport counterpart.
//!
//! **Election, not an invented algorithm**: per level `l`, "the Coordinator" is whichever node
//! PeerServices' DHT resolves `perfect_tuple(k) = [0,0,...,0]` (`l` zeros) to — i.e. the
//! position-0 (eldest) node inside the g-node at level `l`. This is a DHT-hash lookup, not a
//! running leader-election protocol; see `CoordinatorClient`'s target-resolution doc comment.

mod actor;
mod client;
mod config;
mod domain;
mod error;
mod fake;
mod handler;
mod service;
mod traits;
mod wire;

/// Generated protobuf types for this module's own payloads (`proto/coordinator.proto`, package
/// `ntk.coordinator.v1`). These travel inside `ntk_proto::v1::TypedValue` payloads; nothing
/// outside [`wire`](self::wire) constructs them directly.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.coordinator.v1.rs"));
}

pub use actor::{Handle, Manager};
pub use client::{CoordinatorClient, ProxyError};
pub use config::Config;
pub use domain::{
    Booking, Event, GnodeMemory, HandOff, PropagationArgs, Reservation, ReserveError, Snapshot,
};
pub use error::Error;
pub use fake::{FakeCoordinatorStubFactory, direct_stub};
pub use handler::CoordinatorRpcHandler;
pub use service::{CoordinatorService, SERVICE_ID};
pub use traits::{
    AbortEnterHandler, BeginEnterHandler, CompletedEnterHandler, CoordinatorMap, CoordinatorStub,
    CoordinatorStubFactory, EnterHandlers, EvaluateEnterHandler, PropagationHandler,
};
pub use wire::RpcCoordinatorStub;
