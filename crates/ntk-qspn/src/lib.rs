//! QSPN v2: the netsukuku-rs routing core.
//!
//! Ports `research/impl/vala/qspn/` (NORMATIVE upstream source) faithfully,
//! with one deliberate scope cut documented throughout this crate:
//!
//! - **Arc identity is decoupled from Neighborhood.** Upstream's `IQspnArc`
//!   bundles cost, equality and caller-resolution into one object
//!   (`research/impl/vala/qspn/api.vala:114-119`); this crate only ever
//!   carries an opaque [`ArcId`] plus a cost the caller supplies, and
//!   delegates "which arc did this RPC call arrive on" to an injectable
//!   [`rpc::ArcResolver`] — see [`arc`] for the full rationale.
//!
//! Everything else — ETP revision, path admission, disjoint-path selection,
//! fingerprint/split handling, flooding, the `enter_net` migration/
//! bootstrap-phase lifecycle, and the `make_connectivity`/`exit_network`/
//! `check_connectivity` connectivity-identity lifecycle — is a complete,
//! faithful port. This crate still never installs routes itself (the daemon
//! does, via `ntk-netlink`, off the snapshot this crate publishes) and never
//! models `prepare_destroy`/`destroy`'s broadcast identity-teardown
//! (`qspn.vala:2450-2505`, see [`manager::QspnHandle::check_connectivity`]'s
//! docs for why).
//!
//! # Layout
//! - [`state::QspnState`] — owned protocol state, `update_map`/
//!   `update_clusters` (`qspn.vala:1334-2115`), the `enter_net` constructor
//!   ([`state::QspnState::new_entering`], `qspn.vala:223-355`), and the
//!   connectivity lifecycle ([`state::QspnState::make_connectivity`]/
//!   [`state::QspnState::exit_network`]/[`state::QspnState::check_connectivity`],
//!   `qspn.vala:2226-2448`).
//! - [`revise_etp`] — ETP grouping/acyclic-check/implicit-withdrawal
//!   (`qspn.vala:1074-1232`).
//! - [`flood`] — outgoing ETP construction and the `ignore_outside` pruning
//!   pass (`etp_message.vala`).
//! - [`manager`] — the actor ([`manager::spawn`]/[`manager::spawn_entering`],
//!   [`manager::QspnHandle`]).
//! - [`stub`]/[`fake`] — the outbound RPC seam and its in-memory fake.
//! - [`rpc`] — inbound [`ntk_rpc::RpcHandler`] for the 4 `qspn_*` methods.
//! - [`wire`] — `proto/qspn.proto` <-> domain type conversions.

mod arc;
mod config;
mod error;
mod events;
mod fake;
mod flood;
mod manager;
mod mch_ratio;
mod path;
mod revise;
mod rpc;
mod snapshot;
mod state;
mod stub;
mod validate;
mod wire;

pub use arc::{ArcId, ArcIdSource, DefaultArcIdSource};
pub use config::{FixedThreshold, MchRatioTable, OverlapWeights, QspnConfig, ThresholdCalculator};
pub use error::QspnError;
pub use events::QspnEvent;
pub use fake::FakeQspnStubFactory;
pub use manager::{QspnHandle, spawn, spawn_entering};
pub use mch_ratio::mch_ratio;
pub use path::{Destination, EtpMessage, EtpPath, Hop, NodePath, RoutePath};
pub use revise::{RevisedEtp, revise_etp};
pub use rpc::{ArcResolver, QspnRpcHandler};
pub use snapshot::{RouteEntry, RouteSnapshot};
pub use state::{ArcEntry, ExitNetworkOutcome, InternalArc, MakeConnectivityOutcome, QspnState};
pub use stub::{MissingArcHandler, QspnStub, QspnStubFactory};
pub use validate::{check_incoming_message, check_outgoing_message};
pub use wire::{
    ETP_MESSAGE_TAG, NADDR_TAG, decode_etp_message, decode_naddr, encode_etp_message, encode_naddr,
};
