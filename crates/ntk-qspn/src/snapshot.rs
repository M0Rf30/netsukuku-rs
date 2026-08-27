//! The exported per-level route set: an immutable snapshot published via
//! `tokio::sync::watch` after each processed command, for the daemon to hand
//! to `ntk-netlink` (`research/notes/06-rust-stack.md` §Concurrency,
//! "read-mostly consumers ... cheap concurrent snapshots"). This crate never
//! installs routes itself.

use ntk_common::HCoord;

use crate::path::RoutePath;

/// Every currently-admitted, elder-gated path to one destination
/// (`QspnManager::get_paths_to`, `research/impl/vala/qspn/qspn.vala:2151-2180`),
/// ascending cost.
#[derive(Clone, Debug, PartialEq)]
pub struct RouteEntry {
    pub destination: HCoord,
    pub paths: Vec<RoutePath>,
}

/// Immutable snapshot of every known destination at every level.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct RouteSnapshot {
    /// Index 0 = level 0, ... up to `topology.levels() - 1`.
    pub levels: Vec<Vec<RouteEntry>>,
}
