//! Multi-identity per node (`research/impl/vala/identities/`,
//! `research/notes/01-vala-core-routing.md` §5).
//!
//! Identities exist solely to support **live g-node migration during
//! hooking**: to move a g-node to a new position without breaking its
//! internal connectivity, the node that is the g-node's single point of
//! contact with the outside forks into two identities — the *old* identity
//! keeps the external arcs alive as a connectivity-only fork while the
//! *new* identity takes over the internal presence and re-hooks at the new
//! position. This crate models exactly that: the identity registry
//! ([`Registry`]/[`IdentityRecord`]), the arc-to-identity-arc mapping
//! ([`ArcId`]/[`IdentityArc`]), the duplication/migration handshake behind
//! `match_duplication` ([`Handle::prepare_migration`]/[`Handle::migrate`]),
//! and the deterministic pseudo-address/pseudo-device naming rules
//! ([`pseudo`]) the daemon hands to `ntk-netlink`.
//!
//! Explicit non-goals: neighborhood discovery internals, the hooking state
//! machine, and any netlink/kernel side effect — those are composed by the
//! daemon, out of this crate's dependency graph. Protocol state lives in a
//! single-owner actor task reachable only through [`Handle`]
//! (`research/notes/06-rust-stack.md` §Concurrency); [`Handle::watch`]
//! publishes read-only snapshots and [`Handle::subscribe`] streams events —
//! neither takes a lock over live state.

mod actor;
mod arc;
mod error;
mod events;
mod identity;
mod migration;
pub mod pseudo;
mod registry;
mod rpc_handler;
mod snapshot;
mod stub;
pub mod wire;

pub use arc::{ArcId, ArcInfo, IdentityArc, IdentityArcChange};
pub use error::Error;
pub use events::IdentityEvent;
pub use identity::{IdentityId, IdentityRecord, IdentityStatus};
pub use migration::{DuplicationData, MigrationDeviceInfo, MigrationId};
pub use registry::Registry;
pub use rpc_handler::IdentityRpcHandler;
pub use snapshot::IdentitySnapshot;
pub use stub::IdentityStubFactory;

pub use actor::Handle;
