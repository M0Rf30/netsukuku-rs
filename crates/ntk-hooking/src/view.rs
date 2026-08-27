//! `IHookingMapPaths` (`research/impl/vala/hooking/api.vala:23-50`), inverted
//! into a trait this crate declares rather than a dependency on `ntk-qspn`:
//! the read-only view onto this identity's current position/topology/map
//! that the composition root (`ntkd`, phase 4) supplies, backed by the real
//! `ntk-qspn` state. Per the batch contract, `ntk-hooking` MUST NOT depend
//! on `ntk-qspn`/`ntk-identities`/`ntk-neighborhood`/`ntk-coordinator`.
//!
//! Every accessor here is synchronous: upstream's `IHookingMapPaths` never
//! throws or blocks (it reads an in-memory map), so there is no async
//! seam to model.

use ntk_common::{HCoord, Topology};

/// One adjacent g-node found via [`QspnView::adjacent_to_my_gnode`] —
/// `IPairHCoordInt` (`api.vala:52-57`): the coordinate of the neighboring
/// g-node plus the real position, inside *my* current border g-node, that a
/// migrating chain would record as the new head of its
/// `previous_migrating_gnode` (`structs.vala:216-226`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdjacentGNode {
    pub hc: HCoord,
    pub border_real_pos: u32,
}

/// Read-only view onto this identity's topology, position, and map —
/// `IHookingMapPaths` (`api.vala:23-50`). The daemon implements this by
/// delegating to a real `ntk_qspn::QspnHandle` (or, for `retrieve_network_data`'s
/// `n_nodes`/`is_bootstrapped`, whichever aggregate the composition root
/// tracks); every method here must be answerable from already-known local
/// state, matching upstream's non-throwing, non-blocking getters.
pub trait QspnView: Send + Sync {
    /// The network shape this identity currently believes in
    /// (`get_levels`/`get_gsize`, `api.vala:29-30`).
    fn topology(&self) -> &Topology;

    /// This identity's own network id (`get_network_id`, `api.vala:26`).
    fn network_id(&self) -> i64;

    /// Estimated total node count of my own network
    /// (`get_n_nodes`, `api.vala:34`).
    fn n_nodes(&self) -> u64;

    /// My own position at `level` (`get_my_pos`, `api.vala:37`).
    fn my_pos(&self, level: usize) -> u32;

    /// My own eldership at `level` (`get_my_eldership`, `api.vala:38`).
    fn my_eldership(&self, level: usize) -> i32;

    /// The lowest level at which this identity's own subnet is not a single
    /// leaf (`get_subnetlevel`, `api.vala:39`) — `first_host_lvl` in
    /// [`crate::search::find_shortest_mig`] is never allowed below this.
    fn subnetlevel(&self) -> usize;

    /// Acceptable "good enough" slack in host levels above the requested
    /// one (`get_epsilon`, `api.vala:31`) — `ok_host_lvl = lvl + epsilon`
    /// (`hooking.vala:522-524`).
    fn epsilon(&self, level: usize) -> usize;

    /// The eldership of the g-node at `(level, pos)` in my current map
    /// (`get_eldership`, `api.vala:43`).
    fn eldership(&self, level: usize, pos: u32) -> i32;

    /// Every g-node at `level_adjacent_gnodes` adjacent to my own g-node at
    /// `level_my_gnode` (`adjacent_to_my_gnode`, `api.vala:44`) — the
    /// candidate set [`crate::search::execute_search`] offers back to the
    /// BFS for further exploration.
    fn adjacent_to_my_gnode(
        &self,
        level_adjacent_gnodes: usize,
        level_my_gnode: usize,
    ) -> Vec<AdjacentGNode>;

    /// Whether this identity has completed its own QSPN bootstrap
    /// (`NotBootstrappedError` guard on every hooking wire method,
    /// `hooking.vala:500,521`).
    fn is_bootstrapped(&self) -> bool;

    /// Records that `pos` at `level` is currently known to belong to a *different* network
    /// than mine (`crate::arc::run_arc_handler`'s own "another network" discovery,
    /// `arc_handler.vala:124-129`'s negative case). [`Self::n_nodes`]'s own doc names the
    /// hazard this exists to let an implementer guard against: this trait has no dependency on
    /// `ntk-qspn`'s own address space, so a not-yet-merged foreign neighbor can otherwise get
    /// silently counted as if it were already one of my own members the moment the underlying
    /// arc becomes reachable — long before hooking resolves anything. Default no-op: only an
    /// implementer backed by real, mutable per-position state needs to track this.
    ///
    /// `(level, pos)` is a bare numeric coordinate, not a node identity: two independent,
    /// not-yet-merged networks assign their own positions from the same range, so a call here
    /// about one (genuinely foreign) peer can name the identical `(level, pos)` as a real
    /// sibling elsewhere in *my* own network. An implementer MUST let a prior or later
    /// [`Self::note_same_network`] call for the same `(level, pos)` win regardless of call
    /// order — see `ntkd::node::adapters::NetworkInfo`'s own doc for the real-kernel run this
    /// was found from.
    fn note_foreign(&self, _level: usize, _pos: u32) {}

    /// The inverse of [`Self::note_foreign`]: `pos` at `level` is now confirmed, by this
    /// identity's own arc negotiation, to be part of *my own* network. Default no-op, matching
    /// [`Self::note_foreign`]. This confirmation is a fact about my own network's structure and
    /// MUST be sticky: an implementer must never let a later, unrelated [`Self::note_foreign`]
    /// call that merely happens to name the same `(level, pos)` (see that method's own doc)
    /// revert it.
    fn note_same_network(&self, _level: usize, _pos: u32) {}
}
