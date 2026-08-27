//! Link/arc discovery and liveness monitoring — the Rust port of Vala's
//! `neighborhood/` module (`research/impl/vala/neighborhood/`,
//! `research/notes/01-vala-core-routing.md` §4).
//!
//! Scope: the UDP-broadcast 3-way discovery handshake (`here_i_am` /
//! `request_arc` / `can_you_export`), the resulting [`Arc`] lifecycle state
//! machine, TCP `nop()` liveness probing, EMA-smoothed link cost with
//! hysteresis publication, and which local NICs participate (via
//! `ntk-netlink`'s [`ntk_netlink::TopologyQuery`]). Explicitly out of scope:
//! QSPN map logic, identities/multi-identity addressing, and hooking — this
//! crate exposes only what upstream's `INeighborhoodArc`/`NeighborhoodManager`
//! surface exposes to those modules.
//!
//! # Module surface
//! - [`v1`] — `prost`-generated wire types for `proto/neighborhood.proto`.
//! - [`NodeId`], [`NicRef`] — domain counterparts of the wire types, with
//!   validating [`TryFrom`] conversions back from the wire.
//! - [`Arc`], [`ArcState`] — one discovered/negotiated/established link and
//!   its lifecycle.
//! - [`cost`] — the pure EMA-smoothing/hysteresis functions ([`cost::ema_step`],
//!   [`cost::exceeds_hysteresis`]).
//! - [`NeighborhoodTiming`] — every interval this crate waits on, injectable
//!   so tests never sleep upstream's real 28-30s/60s constants.
//! - [`LocalNic`], [`RttProbe`], [`IpRouteManager`] — the caller-injected
//!   seams mirroring upstream's `INeighborhoodNetworkInterface`/
//!   `INeighborhoodIPRouteManager`. [`IcmpRttProbe`] is the production
//!   [`RttProbe`]; [`FixedRttProbe`] is a test double only.
//! - [`NeighborhoodStubFactory`], [`FakeIpRouteManager`] — the outbound-call
//!   seam and its non-privileged fake, mirroring `INeighborhoodStubFactory`.
//! - [`Manager`], [`Handle`], [`Event`] — the actor, its cheap-clone handle,
//!   and the broadcast event stream.
//! - [`NeighborhoodRpcHandler`] — the inbound [`ntk_rpc::RpcHandler`] for the
//!   5 `MethodCall` arms this module owns.

mod arc;
mod cost_model;
mod error;
mod handler;
mod interface_state;
mod manager;
mod nic;
mod node_id;
mod rtt;
mod stub;
mod timing;
mod wire;

/// `prost`-generated wire types for `proto/neighborhood.proto` (package
/// `ntk.neighborhood.v1`). See that file for the design rationale.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.neighborhood.v1.rs"));
}

pub use arc::{Arc, ArcState};

/// Pure link-cost math ([`cost::ema_step`], [`cost::exceeds_hysteresis`]) —
/// the only public surface of the private `cost_model` module. Only the
/// functions are re-exported, not the module itself (its `#[cfg(test)]`
/// table is an implementation detail, not part of this crate's API).
pub mod cost {
    pub use crate::cost_model::{ema_step, exceeds_hysteresis};
}
pub use error::NeighborhoodError;
pub use handler::NeighborhoodRpcHandler;
pub use manager::{Event, Handle, Manager, NeighborhoodConfig};
pub use nic::{
    FakeIpRouteManager, FixedRttProbe, IpRouteManager, IpRouteOperation, LocalNic, RttProbe,
};
pub use node_id::NodeId;
pub use rtt::IcmpRttProbe;
pub use stub::{BroadcastRpcClient, NeighborhoodStubFactory, serve_broadcast};
pub use timing::NeighborhoodTiming;
pub use wire::NicRef;
