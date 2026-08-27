//! The exported hooking state: an immutable snapshot published via
//! `tokio::sync::watch` after each processed command
//! (`research/notes/06-rust-stack.md` §Concurrency).

use std::collections::BTreeMap;

use ntk_common::Naddr;

use crate::arc::ArcId;
use crate::domain::EntryData;

/// Which phase one arc's handler tasklet is currently in — a coarse,
/// externally-observable projection of `ArcHandler.add_arc_tasklet`'s
/// control flow (`research/impl/vala/hooking/arc_handler.vala:91-358`).
/// Upstream has no equivalent explicit state enum (the tasklet's program
/// counter *is* its state); this is this crate's own addition, needed to
/// satisfy the watch-snapshot requirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArcPhase {
    /// Just registered; `retrieve_network_data` has not yet succeeded.
    Discovering,
    /// The peer's `network_id` matches mine — the arc contributes nothing
    /// further to hooking (`arc_handler.vala:124-129`).
    SameNetwork,
    /// The peer's network has an incompatible topology (`gsizes` mismatch)
    /// — permanently inert (`arc_handler.vala:130-149`).
    IncompatibleTopology,
    /// I am (or the peer is) a connectivity identity — permanently inert
    /// (`arc_handler.vala:95-99,114-116`).
    Connectivity,
    /// The merge-direction heuristic decided to wait before redoing the
    /// whole loop from start (`arc_handler.vala:209-214`).
    Waiting,
    /// `evaluate_enter` is in flight (`arc_handler.vala:224-248`).
    Evaluating,
    /// The begin/search/complete loop is in flight for `ask_lvl`
    /// (`arc_handler.vala:250-334`).
    Entering { ask_lvl: usize },
    /// This arc's handler successfully drove an entry to completion at
    /// `ask_lvl` (`arc_handler.vala:336-357`) and returned.
    Entered { ask_lvl: usize },
    /// A transport/deserialize failure terminated this arc's handler
    /// (`signal_and_exit`, `arc_handler.vala:73-79`).
    Failed,
}

/// The resolved new-address chain this identity is entering at, plus the
/// materialized [`Naddr`] when enough information is present to build one.
///
/// Upstream's `EntryData` (`serializables.vala:451-509`) only ever carries
/// positions from `ask_lvl` upward — everything *below* `ask_lvl` is this
/// identity's own pre-existing internal subtree, known to `ntk-identities`/
/// `ntk-qspn`, not to Hooking. `naddr` is therefore only `Some` when
/// `entry_data.pos` happens to already span every topology level (the
/// common case of a single, previously unhooked node joining at `ask_lvl ==
/// 0`); otherwise the daemon must combine `entry_data` with its own
/// retained lower-level positions to materialize the full address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChosenAddress {
    pub entry_data: EntryData,
    pub naddr: Option<Naddr>,
}

/// Immutable snapshot of every arc's phase, whether this identity is
/// currently hooked, and (once hooked via a join rather than `create_net`)
/// the resolved [`ChosenAddress`].
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct HookingSnapshot {
    pub arcs: BTreeMap<ArcId, ArcPhase>,
    pub hooked: bool,
    pub chosen: Option<ChosenAddress>,
}
