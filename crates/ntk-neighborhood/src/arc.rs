//! [`Arc`]/[`ArcState`]: one discovered link to a neighbor and its
//! lifecycle (`NeighborhoodRealArc`, `INeighborhoodArc`,
//! `research/impl/vala/neighborhood/structs.vala`,
//! `research/impl/vala/neighborhood/api.vala:38-45`).

use ntk_common::Cost;

use crate::node_id::NodeId;

/// Lifecycle state of an [`Arc`]. Upstream's real state machine is more
/// compressed than this — `NeighborhoodRealArc` only ever tracks `exported`
/// (bool) and `available` (`cost.is_some()`) — but every transition this
/// enum names corresponds to an observable point in
/// `neighborhood.vala:363-558`:
///
/// - [`ArcState::Discovered`] — registered via `here_i_am` (or as the
///   not-yet-negotiated side effect of receiving `request_arc`), not yet
///   exported (`:412-419`).
/// - [`ArcState::Requested`] — a `request_arc`/`can_you_export` capacity
///   negotiation is in flight for this arc (`:513-528`).
/// - [`ArcState::Established`] — both sides agreed to export
///   (`arc.exported = true`, `:531,552`); the periodic `nop`/cost monitor
///   is running. Note this is reached *before* a cost is ever measured —
///   [`Arc::cost`] stays `None` until the monitor's first successful tick,
///   matching upstream's `arc_added` signal firing at first RTT measurement
///   rather than at export (`:264-271`, `research/notes/01-vala-core-routing.md`
///   §4 point 5).
/// - [`ArcState::Removed`] — terminal, set by `remove_my_arc`
///   (`:306-350`) immediately before the entry is dropped; a
///   [`crate::Event::ArcRemoved`] carries the arc in this state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArcState {
    /// Known, not yet exported.
    Discovered,
    /// Capacity negotiation (`can_you_export`) is in flight.
    Requested,
    /// Exported (qspn-visible) and monitored for liveness/cost.
    Established,
    /// Torn down.
    Removed,
}

/// A discovered/negotiated/established link to one neighbor, on one local
/// NIC (`NeighborhoodRealArc`, `research/impl/vala/neighborhood/structs.vala:23-87`).
///
/// # Identity and duplicate suppression
/// This crate keys arcs by [`Arc::neighbour_mac`] alone. Upstream maintains
/// six parallel indices (`arcs_by_itsmac`/`arcs_by_itsll`/`arcs_by_itsnodeid`
/// and per-`my_dev` variants, `neighborhood.vala:56-61`) to dedup on four
/// collision rules (`:397-410`); those rules jointly enforce that a given
/// `neighbour_mac` maps to exactly one `(neighbour_id, neighbour_nic_addr,
/// my_dev)` tuple at a time (no two neighbors share a MAC, a MAC's
/// linklocal is fixed, one NIC pairing per remote MAC, one dev per remote
/// node-id). Given that invariant, `neighbour_mac` alone is a sufficient
/// map key — this crate keeps one `HashMap<String, _>` (see
/// `crate::manager::Manager`) and re-derives the four collision checks as
/// filters over its values (`Manager::find_collision`) instead of
/// maintaining six separate indices, which is behavior-equivalent at the
/// scale of a routing daemon's neighbor count and considerably simpler.
#[derive(Debug, Clone, PartialEq)]
pub struct Arc {
    /// The neighbor's random per-identity discovery id.
    pub neighbour_id: NodeId,
    /// The neighbor's MAC address — this arc's identity key.
    pub neighbour_mac: String,
    /// The neighbor's fixed linklocal address.
    pub neighbour_nic_addr: String,
    /// Our own local NIC (`dev` name) this arc runs over.
    pub my_dev: String,
    /// Lifecycle state.
    pub state: ArcState,
    /// The last *published* link cost, `None` until the arc's monitor
    /// completes its first successful RTT measurement (see [`ArcState`]
    /// doc). Smoothing/hysteresis math lives in [`crate::cost`].
    pub cost: Option<Cost>,
}

impl Arc {
    /// This arc's identity key (see the struct doc's "Identity and
    /// duplicate suppression" section) — always equal to
    /// [`Arc::neighbour_mac`].
    #[must_use]
    pub fn key(&self) -> &str {
        &self.neighbour_mac
    }

    pub(crate) fn new(
        neighbour_id: NodeId,
        neighbour_mac: String,
        neighbour_nic_addr: String,
        my_dev: String,
    ) -> Self {
        Self {
            neighbour_id,
            neighbour_mac,
            neighbour_nic_addr,
            my_dev,
            state: ArcState::Discovered,
            cost: None,
        }
    }
}
