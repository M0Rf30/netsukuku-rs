//! Owned protocol state — the sole owner is the actor task
//! (`research/notes/06-rust-stack.md` §Concurrency: single-owner actor +
//! message passing, never `Arc<RwLock<_>>`). This module ports
//! `research/impl/vala/qspn/qspn.vala`'s `update_map`
//! (`qspn.vala:1334-1816`), `update_clusters` (`qspn.vala:1954-2074`) and
//! `my_gnode_neighbors` (`qspn.vala:2076-2115`) as methods on [`QspnState`],
//! plus the `enter_net`/migration lifecycle: [`QspnState::new_entering`]
//! (`qspn.vala:223-355`), the bootstrap-phase gates on
//! [`QspnState::my_eldership`]/[`QspnState::eldership`]/[`QspnState::snapshot`]
//! (`qspn.vala:2117-2207`), and [`QspnState::make_connectivity`]/
//! [`QspnState::exit_network`]/[`QspnState::check_connectivity`]
//! (`qspn.vala:2226-2448`).
//!
//! `is_null_eldership` (upstream's virtual-position flag,
//! `qspn.vala:1962-1966,2010-2014`) is derived in [`QspnState::update_clusters`]
//! from [`Naddr::is_virtual_at`]: a g-node whose own position at the child
//! level is virtual (reserved but not yet placed, `pos >= gsize(level)`)
//! can never win [`Fingerprint::construct`]'s champion race as a real member
//! would, which is recorded into the resulting fingerprint's
//! `elderships_seed` trail — the value [`Fingerprint::elder_seed`] arbitrates
//! a split/merge on — rather than that fingerprint's own plain per-level
//! `eldership` (always this node's next scheduled claim, real or not). For a
//! `create_net` identity every position is real (`Naddr::new` rejects
//! virtual positions), so this is always `false` there, exactly the phase-1
//! behavior; an `enter_net` identity's virtual levels are where it actually
//! varies.

use std::collections::{BTreeMap, HashMap, HashSet};

use ntk_common::{Cost, Fingerprint, HCoord, Naddr};

use crate::arc::ArcId;
use crate::config::QspnConfig;
use crate::error::QspnError;
use crate::events::QspnEvent;
use crate::path::{
    Destination, EtpPath, NodePath, prepare_for_sending, to_route_path, winning_fingerprint,
};
use crate::snapshot::{RouteEntry, RouteSnapshot};

/// This node's record of one of its own arcs: the peer's advertised cost
/// (upstream re-reads `arc.i_qspn_get_cost()` live off the `IQspnArc` object;
/// here it is stored, updated explicitly by `set_arc_cost`) and the peer's
/// address at this arc, learned from its first ETP (`arc_to_naddr`,
/// `qspn.vala:100`; `None` until the first ETP arrives, matching upstream's
/// `arc_to_naddr[arc] = null` at `arc_add` time, `qspn.vala:734`).
#[derive(Clone, Debug, PartialEq)]
pub struct ArcEntry {
    pub cost: Cost,
    pub peer_naddr: Option<Naddr>,
}

/// Key identifying a [`Fingerprint`] by origin identity (`i_qspn_equals`'s
/// notion of identity — same `(level, id)` — not by structural equality of
/// its full eldership trail). Used only to dedupe the split-signal debounce
/// set ([`QspnState::pending_gnode_split`]), mirroring upstream's
/// `PairFingerprints` (`qspn.vala:36-51`).
type FpKey = (usize, Vec<u8>);

fn fp_key(fp: &Fingerprint<Vec<u8>>) -> FpKey {
    (fp.level(), fp.id().clone())
}

/// One destination's admitted-set diff, as produced by [`QspnState::update_map`]
/// for a single destination (`qspn.vala:1399-1815`'s per-`d` loop body).
struct OneDestOutcome {
    all_paths_set: Vec<EtpPath>,
    first_detection_split: bool,
    events: Vec<QspnEvent>,
    split_signals: Vec<SplitSignal>,
}

/// A debounced split-signal candidate: `fp` (not the eldest) may need to
/// migrate away from `destination`, once
/// [`crate::ThresholdCalculator::calculate_threshold`] elapses without the
/// fork healing (`qspn.vala:1775-1811`).
#[derive(Clone, Debug)]
pub struct SplitSignal {
    pub destination: HCoord,
    pub fp_eldest: Fingerprint<Vec<u8>>,
    pub fp: Fingerprint<Vec<u8>>,
    pub bp_eldest: NodePath,
    pub bp: NodePath,
}

/// The full result of one [`QspnState::update_map`] call
/// (`qspn.vala:1334-1349`'s three `out` parameters, plus the events upstream
/// emits as GObject signals).
#[derive(Default, Debug)]
pub struct UpdateMapOutcome {
    /// Paths that changed in the map and must be forwarded to other arcs in
    /// a new ETP (`all_paths_set`).
    pub all_paths_set: Vec<EtpPath>,
    /// G-nodes to re-flood for first detection of a split (`b_set`).
    pub b_set: Vec<HCoord>,
    /// Consumer-facing events, in emission order.
    pub events: Vec<QspnEvent>,
    /// Debounced split candidates to schedule (`signal_split` tasklets).
    pub split_signals: Vec<SplitSignal>,
}

/// Result of [`QspnState::remove_arc`]'s synchronous phase
/// (`qspn.vala:913-987`): the arc and every path through it are gone
/// immediately; `dead_paths` feeds the subsequent `update_map` call the same
/// way upstream's `tasklet_arc_remove` merges `paths_to_add_to_all_paths`
/// into `all_paths_set` (`qspn.vala:1053`).
#[derive(Default, Debug)]
pub struct RemoveArcOutcome {
    pub events: Vec<QspnEvent>,
    pub dead_paths: Vec<EtpPath>,
}

/// Bootstrap-phase state (`bootstrap_complete`/`guest_gnode_level`/
/// `host_gnode_level`, `qspn.vala:109-111`). `Complete` is `create_net`'s
/// permanent state (`qspn.vala:202-203`); `InProgress` is `enter_net`'s
/// starting state until [`QspnState::exit_bootstrap`] fires
/// (`exit_bootstrap_phase`, `qspn.vala:568-573`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Bootstrap {
    Complete,
    InProgress {
        guest_gnode_level: usize,
        host_gnode_level: usize,
    },
}

/// One arc surviving migration from the previous identity into this newly
/// constructed identity's internal (`level < guest_gnode_level`) portion —
/// `internal_arc_set`/`internal_arc_prev_arc_set`/`internal_arc_peer_naddr_set`
/// zipped into one triple (`qspn.vala:224-226,253-283`). The composition
/// layer (hooking, out of this crate's scope) decides which of the previous
/// identity's arcs survive and under what new local [`ArcId`].
#[derive(Clone, Debug)]
pub struct InternalArc {
    pub previous_arc: ArcId,
    pub new_arc: ArcId,
    pub peer_naddr: Naddr,
    pub cost: Cost,
}

/// Owned protocol state for one identity. Never wrapped in a lock — the
/// actor task is this type's sole owner.
#[derive(Debug)]
pub struct QspnState {
    my_naddr: Naddr,
    /// Index 0 = level 0 (this node's own leaf fingerprint), length =
    /// `levels + 1` (`qspn.vala:102,182-194`).
    my_fingerprints: Vec<Fingerprint<Vec<u8>>>,
    /// Same indexing as `my_fingerprints` (`qspn.vala:103,186-193`).
    my_nodes_inside: Vec<u32>,
    arcs: BTreeMap<ArcId, ArcEntry>,
    /// Index by level, then by position (`destinations`, `qspn.vala:119`).
    destinations: Vec<HashMap<u32, Destination>>,
    config: QspnConfig,
    pending_gnode_split: HashSet<(FpKey, FpKey)>,
    bootstrap: Bootstrap,
    /// `0` for a main identity (`is_main_identity`, `qspn.vala:154-158`).
    connectivity_from_level: usize,
    connectivity_to_level: usize,
}

impl QspnState {
    /// Shared fingerprint-chain init for both constructors
    /// (`qspn.vala:182-194,331-343`): level 0 is `my_fingerprint` itself;
    /// each level above aggregates with no known siblings yet
    /// (`is_null_eldership=false` here regardless of `my_naddr` — this
    /// placeholder chain is always superseded by a real
    /// [`QspnState::update_clusters`] climb once there is a map to fold in,
    /// for `enter_net` immediately at construction, `qspn.vala:345`).
    fn initial_fingerprint_chain(
        my_fingerprint: Fingerprint<Vec<u8>>,
        levels: usize,
    ) -> (Vec<Fingerprint<Vec<u8>>>, Vec<u32>) {
        let mut my_fingerprints = Vec::with_capacity(levels + 1);
        let mut my_nodes_inside = Vec::with_capacity(levels + 1);
        my_fingerprints.push(my_fingerprint);
        my_nodes_inside.push(1);
        for l in 1..=levels {
            let next = my_fingerprints[l - 1]
                .construct(&[], false)
                .expect("fingerprint must carry >= levels pending eldership entries");
            my_fingerprints.push(next);
            my_nodes_inside.push(my_nodes_inside[l - 1]);
        }
        (my_fingerprints, my_nodes_inside)
    }

    /// Builds the state for a `create_net`-rooted identity
    /// (`qspn.vala:161-219`): a single-node network, immediately
    /// bootstrap-complete, no arcs, no known destinations.
    ///
    /// `my_fingerprint` MUST carry at least `topology.levels()` entries in
    /// its `pending_elderships` trail (`Fingerprint::new`'s third argument) —
    /// one is consumed per [`QspnState::update_clusters`] climb from level 0
    /// up to the top level.
    #[must_use]
    pub fn new(my_naddr: Naddr, my_fingerprint: Fingerprint<Vec<u8>>, config: QspnConfig) -> Self {
        let levels = my_naddr.topology().levels();
        let (my_fingerprints, my_nodes_inside) =
            Self::initial_fingerprint_chain(my_fingerprint, levels);
        Self {
            my_naddr,
            my_fingerprints,
            my_nodes_inside,
            arcs: BTreeMap::new(),
            destinations: vec![HashMap::new(); levels],
            config,
            pending_gnode_split: HashSet::new(),
            bootstrap: Bootstrap::Complete,
            connectivity_from_level: 0,
            connectivity_to_level: 0,
        }
    }

    /// Builds the state for an `enter_net`-rooted identity
    /// (`qspn.vala:223-355`): hooking into an existing network at a
    /// (possibly virtual) `my_naddr`, carrying over `internal_arcs` — and the
    /// portion of `previous_destinations` reachable through them, at levels
    /// `< guest_gnode_level` — from the identity this one supersedes, plus a
    /// set of brand-new `external_arcs` into the host g-node
    /// (`host_gnode_level`). `connectivity` is the previous identity's own
    /// `(connectivity_from_level, connectivity_to_level)`, inherited and
    /// clamped to `guest_gnode_level` exactly as upstream does
    /// (`qspn.vala:239-242`); pass `(0, 0)` for an ordinary (non-connectivity)
    /// migration.
    ///
    /// Deviation from upstream: the reference `enter_net`/`migration`
    /// constructors also take a `ChangeFingerprintDelegate` applied to every
    /// imported path's fingerprint (`qspn.vala:230,313,340-343`,
    /// `Destination.copy`, `destinations.vala:148-169`). This crate's actor
    /// model has no live "previous identity" object to read from mid-ctor —
    /// the composition layer already extracts `previous_destinations` as
    /// plain data beforehand — and nothing in [`Fingerprint`]'s pure
    /// id+eldership-chain model requires a migration-time rewrite (it embeds
    /// no address). A future composition layer that discovers it needs one
    /// can transform `previous_destinations` itself before calling this
    /// constructor ([`Destination`]/[`NodePath`] are both public with public
    /// fields), without a crate change here.
    ///
    /// # Panics
    /// If `connectivity_from_level > guest_gnode_level`,
    /// `host_gnode_level > my_naddr.topology().levels()`, or
    /// `guest_gnode_level >= host_gnode_level` (`qspn.vala:241,302-303`'s
    /// asserts).
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from the [`QspnState::update_clusters`]
    /// climb performed at construction time to fold in `previous_destinations`
    /// (`qspn.vala:345`).
    #[allow(clippy::too_many_arguments)]
    pub fn new_entering(
        my_naddr: Naddr,
        my_fingerprint: Fingerprint<Vec<u8>>,
        config: QspnConfig,
        internal_arcs: &[InternalArc],
        external_arcs: &[(ArcId, Cost)],
        guest_gnode_level: usize,
        host_gnode_level: usize,
        connectivity: (usize, usize),
        previous_destinations: &[HashMap<u32, Destination>],
    ) -> Result<Self, QspnError> {
        let levels = my_naddr.topology().levels();
        assert!(host_gnode_level <= levels, "qspn.vala:302");
        assert!(guest_gnode_level < host_gnode_level, "qspn.vala:303");
        let (connectivity_from_level, mut connectivity_to_level) = connectivity;
        assert!(
            connectivity_from_level < guest_gnode_level + 1,
            "qspn.vala:241"
        );
        if connectivity_to_level > guest_gnode_level {
            connectivity_to_level = guest_gnode_level;
        }

        let mut arcs = BTreeMap::new();
        let mut arc_remap = HashMap::with_capacity(internal_arcs.len());
        for a in internal_arcs {
            arcs.insert(
                a.new_arc,
                ArcEntry {
                    cost: a.cost,
                    peer_naddr: Some(a.peer_naddr.clone()),
                },
            );
            arc_remap.insert(a.previous_arc, a.new_arc);
        }
        for &(id, cost) in external_arcs {
            arcs.insert(
                id,
                ArcEntry {
                    cost,
                    peer_naddr: None,
                },
            );
        }

        // Import the previous identity's map at levels < guest_gnode_level,
        // remapping each path's owning arc and dropping whatever didn't
        // survive the migration (qspn.vala:308-330).
        let mut destinations = vec![HashMap::new(); levels];
        for (l, level_map) in previous_destinations
            .iter()
            .enumerate()
            .take(guest_gnode_level)
        {
            for (&pos, d) in level_map {
                let paths: Vec<NodePath> = d
                    .paths
                    .iter()
                    .filter_map(|np| {
                        let new_arc = *arc_remap.get(&np.arc)?;
                        let mut path = np.path.clone();
                        path.arcs[0] = new_arc;
                        Some(NodePath {
                            arc: new_arc,
                            path,
                            exposed: np.exposed,
                        })
                    })
                    .collect();
                if !paths.is_empty() {
                    destinations[l].insert(
                        pos,
                        Destination {
                            coord: d.coord,
                            paths,
                        },
                    );
                }
            }
        }

        let (my_fingerprints, my_nodes_inside) =
            Self::initial_fingerprint_chain(my_fingerprint, levels);

        let mut state = Self {
            my_naddr,
            my_fingerprints,
            my_nodes_inside,
            arcs,
            destinations,
            config,
            pending_gnode_split: HashSet::new(),
            bootstrap: Bootstrap::InProgress {
                guest_gnode_level,
                host_gnode_level,
            },
            connectivity_from_level,
            connectivity_to_level,
        };
        state.update_clusters()?;
        Ok(state)
    }

    #[must_use]
    pub fn levels(&self) -> usize {
        self.my_naddr.topology().levels()
    }

    #[must_use]
    pub fn my_naddr(&self) -> &Naddr {
        &self.my_naddr
    }

    #[must_use]
    pub fn config(&self) -> &QspnConfig {
        &self.config
    }

    /// True once this identity has fully hooked (`is_bootstrap_complete`,
    /// `qspn.vala:2204-2207`). Always `true` for a `create_net` identity.
    #[must_use]
    pub fn is_bootstrap_complete(&self) -> bool {
        matches!(self.bootstrap, Bootstrap::Complete)
    }

    /// The highest level this identity may currently publish/query
    /// (`guest_gnode_level`, `qspn.vala:110`): `levels()` once bootstrap is
    /// complete, otherwise the level [`QspnState::new_entering`] was given.
    #[must_use]
    pub fn guest_gnode_level(&self) -> usize {
        match self.bootstrap {
            Bootstrap::Complete => self.levels(),
            Bootstrap::InProgress {
                guest_gnode_level, ..
            } => guest_gnode_level,
        }
    }

    /// The g-node level this identity is hooking into (`host_gnode_level`,
    /// `qspn.vala:111`), or `None` once bootstrap is complete (upstream
    /// keeps the field but it is no longer meaningful past
    /// `exit_bootstrap_phase`, `qspn.vala:571-572`).
    #[must_use]
    pub fn host_gnode_level(&self) -> Option<usize> {
        match self.bootstrap {
            Bootstrap::Complete => None,
            Bootstrap::InProgress {
                host_gnode_level, ..
            } => Some(host_gnode_level),
        }
    }

    /// `exit_bootstrap_phase`'s state transition (`qspn.vala:568-573`),
    /// minus the outbound `qspn_bootstrap_complete` signal and full-ETP
    /// republish, which are the actor's job (outbound I/O — see module
    /// docs). A no-op if already complete.
    pub fn exit_bootstrap(&mut self) {
        self.bootstrap = Bootstrap::Complete;
    }

    /// `is_main_identity` (`qspn.vala:154-158`).
    #[must_use]
    pub fn is_main_identity(&self) -> bool {
        self.connectivity_from_level == 0
    }

    /// This identity's connectivity span, `(connectivity_from_level,
    /// connectivity_to_level)` — `(0, 0)` for a main identity.
    #[must_use]
    pub fn connectivity_range(&self) -> (usize, usize) {
        (self.connectivity_from_level, self.connectivity_to_level)
    }

    /// This node's own fingerprint at `level` (`get_fingerprint`,
    /// `qspn.vala:2194-2200`).
    #[must_use]
    pub fn fingerprint(&self, level: usize) -> Option<&Fingerprint<Vec<u8>>> {
        self.my_fingerprints.get(level)
    }

    /// This node's own eldership claim at `level` — the value
    /// `ntk_hooking::view::QspnView::my_eldership` needs
    /// (`research/impl/vala/hooking/api.vala:38`, `get_my_eldership`, backed
    /// by `get_fingerprint`, `qspn.vala:2194-2200`). `None` if `level` is out
    /// of range for this identity (mirrors [`Self::fingerprint`]'s domain) or
    /// this identity has not yet bootstrapped that high
    /// (`level > guest_gnode_level()` — upstream instead throws
    /// `QspnBootstrapInProgressError`, `qspn.vala:2196-2198`; this method has
    /// no `Result` channel, and "not yet known" already collapses to `None`
    /// for the out-of-range case, so bootstrap-in-progress folds into the
    /// same answer rather than adding one). `Some(None)` is the virtual/
    /// null-eldership case (`FingerprintParts::eldership`, upstream's `-1`
    /// `is_null_eldership` sentinel).
    #[must_use]
    pub fn my_eldership(&self, level: usize) -> Option<Option<u32>> {
        if level > self.guest_gnode_level() {
            return None;
        }
        self.fingerprint(level).map(|fp| fp.to_parts().eldership)
    }

    /// This node's own `nodes_inside` estimate at `level` (`get_nodes_inside`,
    /// `qspn.vala:2184-2190`).
    #[must_use]
    pub fn nodes_inside_at(&self, level: usize) -> Option<u32> {
        self.my_nodes_inside.get(level).copied()
    }

    /// Current arcs (`current_arcs`, `qspn.vala:2211-2216`).
    pub fn arcs(&self) -> impl Iterator<Item = ArcId> + '_ {
        self.arcs.keys().copied()
    }

    /// True if `arc` is currently one of this node's arcs.
    #[must_use]
    pub fn contains_arc(&self, arc: ArcId) -> bool {
        self.arcs.contains_key(&arc)
    }

    /// The [`Destination`] record at `(level, pos)`, if known.
    #[must_use]
    pub fn destination(&self, level: usize, pos: u32) -> Option<&Destination> {
        self.destinations.get(level).and_then(|m| m.get(&pos))
    }

    /// The eldership of the g-node currently known at `(level, pos)`: the
    /// own-eldership claim of that destination's elder-seed-winning
    /// fingerprint, exactly the value [`Destination::evaluate`] (used
    /// identically by [`Self::update_clusters`]/[`Self::exposed_paths`])
    /// selects — the value `ntk_hooking::view::QspnView::eldership` needs
    /// (`research/impl/vala/hooking/api.vala:43`, `get_eldership`) for the
    /// merge tiebreak (`research/impl/vala/hooking/arc_handler.vala:150-214`).
    /// `Ok(None)` if no destination is currently known at `(level, pos)`;
    /// `Ok(Some(None))` is the virtual/null-eldership case
    /// (`FingerprintParts::eldership`).
    ///
    /// # Errors
    /// [`QspnError::BootstrapInProgress`] if `level` is a structurally valid
    /// level (`level < levels()`) but `level >= guest_gnode_level()`
    /// (`is_known_destination`/`get_paths_to`'s gate, matching upstream's own
    /// precondition that `level < levels()` already holds,
    /// `qspn.vala:2132-2137,2154-2155`); a structurally out-of-range `level`
    /// is instead `Ok(None)`, same as an unknown position. Propagates
    /// [`ntk_common::Error`] from [`Fingerprint::elder_seed`] (via
    /// [`Destination::evaluate`]).
    pub fn eldership(&self, level: usize, pos: u32) -> Result<Option<Option<u32>>, QspnError> {
        if level < self.levels() && level >= self.guest_gnode_level() {
            return Err(QspnError::BootstrapInProgress);
        }
        let Some(dest) = self.destination(level, pos) else {
            return Ok(None);
        };
        let (fp, _, _) = dest.evaluate(|a| self.arc_cost(a))?;
        Ok(Some(fp.to_parts().eldership))
    }

    /// The peer address learned for `arc`, if any (`get_naddr_for_arc`,
    /// `qspn.vala:2220-2224`).
    #[must_use]
    pub fn peer_naddr(&self, arc: ArcId) -> Option<&Naddr> {
        self.arcs.get(&arc).and_then(|e| e.peer_naddr.as_ref())
    }

    /// Live cost of `arc`, or [`Cost::Dead`] if `arc` is unknown (defensive
    /// fallback: a removed/unknown arc behaves as unreachable rather than
    /// panicking).
    #[must_use]
    pub fn arc_cost(&self, arc: ArcId) -> Cost {
        self.arcs.get(&arc).map_or(Cost::Dead, |e| e.cost)
    }

    /// Registers a brand-new arc at `cost`, with no known peer address yet
    /// (`arc_to_naddr[arc] = null`, `qspn.vala:734`).
    pub fn add_arc(&mut self, id: ArcId, cost: Cost) {
        self.arcs.insert(
            id,
            ArcEntry {
                cost,
                peer_naddr: None,
            },
        );
    }

    /// `arc_is_changed`'s bookkeeping half (`qspn.vala:821-852`): updates the
    /// stored cost. Every `NodePath` through this arc picks the new cost up
    /// automatically on its next [`crate::NodePath::total_cost`] call — there
    /// is no per-path cost to fix up, unlike upstream's arc-object swap.
    pub fn set_arc_cost(&mut self, arc: ArcId, cost: Cost) {
        if let Some(e) = self.arcs.get_mut(&arc) {
            e.cost = cost;
        }
    }

    /// Records `new` as `arc`'s peer address and returns the previous value —
    /// the bookkeeping upstream does inline at the top of `revise_etp`
    /// (`qspn.vala:1079-1080`), factored out here so [`crate::revise_etp`]
    /// stays a pure function.
    pub fn record_peer_naddr(&mut self, arc: ArcId, new: Naddr) -> Option<Naddr> {
        self.arcs
            .get_mut(&arc)
            .and_then(|e| e.peer_naddr.replace(new))
    }

    /// `m_a_set` (`qspn.vala:1185-1196`): every currently-known path whose
    /// first (innermost) hop arc is `arc` — the set [`crate::revise_etp`]
    /// diffs a full ETP against for implicit withdrawal.
    #[must_use]
    pub fn paths_via_arc0(&self, arc: ArcId) -> Vec<NodePath> {
        self.destinations
            .iter()
            .flat_map(|level_map| level_map.values())
            .flat_map(|d| d.paths.iter())
            .filter(|np| np.path.arcs.first() == Some(&arc))
            .cloned()
            .collect()
    }

    /// Every currently-admitted path at `level`, across all destinations —
    /// used by `prepare_full_etp` to enumerate this node's whole map
    /// (`etp_message.vala:219-230`).
    pub fn all_paths_at(&self, level: usize) -> impl Iterator<Item = &NodePath> {
        self.destinations
            .get(level)
            .into_iter()
            .flat_map(|m| m.values())
            .flat_map(|d| d.paths.iter())
    }

    /// `arc_remove`'s synchronous phase (`qspn.vala:913-987`): drops the arc
    /// and every path through it immediately, returning the
    /// `destination_removed`/`path_removed` events and the dead-cost paths to
    /// merge into the caller's subsequent `update_map` forward
    /// (`paths_to_add_to_all_paths`, `qspn.vala:946,993,1053`).
    pub fn remove_arc(&mut self, removed_arc: ArcId) -> RemoveArcOutcome {
        if self.arcs.remove(&removed_arc).is_none() {
            return RemoveArcOutcome::default();
        }
        let mut events = Vec::new();
        let mut dead_paths = Vec::new();
        for level_map in &mut self.destinations {
            let mut to_unset = Vec::new();
            for (&pos, d) in level_map.iter_mut() {
                let mut i = 0;
                while i < d.paths.len() {
                    if d.paths[i].arc == removed_arc {
                        let np = d.paths.remove(i);
                        events.push(QspnEvent::PathRemoved(to_route_path(&np, Cost::Dead)));
                        let mut p = prepare_for_sending(&np, Cost::Dead);
                        p.cost = Cost::Dead;
                        dead_paths.push(p);
                    } else {
                        i += 1;
                    }
                }
                if d.paths.is_empty() {
                    to_unset.push(pos);
                }
            }
            for pos in to_unset {
                if let Some(d) = level_map.remove(&pos) {
                    events.push(QspnEvent::DestinationRemoved(d.coord));
                }
            }
        }
        RemoveArcOutcome { events, dead_paths }
    }

    /// `my_gnode_neighbors` (`qspn.vala:2076-2115`): every level-`i` g-node
    /// this node can reach as the first hop-at-level-`i` of some known
    /// deeper destination.
    fn my_gnode_neighbors(&self, i: usize) -> Vec<HCoord> {
        let levels = self.levels();
        let mut y_set: Vec<HCoord> = Vec::new();
        for l in i..levels {
            for x in self.destinations[l].values() {
                for np in &x.paths {
                    let mut y = np.path.hops[0];
                    if y.level == i {
                        if !y_set.contains(&y) {
                            y_set.push(y);
                        }
                        continue;
                    }
                    for idx in 1..np.path.hops.len() {
                        let y_prev = np.path.hops[idx - 1];
                        y = np.path.hops[idx];
                        if y.level == i && y_prev.level < i {
                            if !y_set.contains(&y) {
                                y_set.push(y);
                            }
                            break;
                        }
                    }
                }
            }
        }
        y_set
    }

    /// `get_paths_to` (`qspn.vala:2151-2180`): every currently-admitted path
    /// to `d`, filtered to the elder-winning fingerprint at levels above 0.
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from [`Fingerprint::elder_seed`].
    pub fn exposed_paths(&self, d: HCoord) -> Result<Vec<NodePath>, QspnError> {
        let Some(dest) = self.destinations.get(d.level).and_then(|m| m.get(&d.pos)) else {
            return Ok(Vec::new());
        };
        if dest.paths.is_empty() {
            return Ok(Vec::new());
        }
        if d.level == 0 {
            return Ok(dest.paths.clone());
        }
        let (valid_fp, _, _) = dest.evaluate(|a| self.arc_cost(a))?;
        Ok(dest
            .paths
            .iter()
            .filter(|p| p.path.fingerprint.same_branch(&valid_fp))
            .cloned()
            .collect())
    }

    /// The exported route-set snapshot (`get_known_destinations` +
    /// `get_paths_to` combined over every level, `qspn.vala:2117-2180`).
    /// Levels at/above [`Self::guest_gnode_level`] are omitted while
    /// bootstrapping — upstream throws `QspnBootstrapInProgressError` for
    /// each of them individually (`qspn.vala:2122-2123,2154-2155`); an
    /// aggregate snapshot has no per-level error channel, so this crate
    /// instead publishes nothing for a level it cannot yet vouch for, the
    /// same "an entering identity does not publish what it must not"
    /// contract from a different angle.
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from [`Fingerprint::elder_seed`].
    pub fn snapshot(&self) -> Result<RouteSnapshot, QspnError> {
        let arc_cost = |a: ArcId| self.arc_cost(a);
        let guest_gnode_level = self.guest_gnode_level();
        let mut levels = Vec::with_capacity(self.levels());
        for (level, level_map) in self.destinations.iter().enumerate() {
            if level >= guest_gnode_level {
                levels.push(Vec::new());
                continue;
            }
            let mut entries = Vec::new();
            for d in level_map.values() {
                let exposed = self.exposed_paths(d.coord)?;
                if exposed.is_empty() {
                    continue;
                }
                let mut paths: Vec<_> = exposed
                    .iter()
                    .map(|np| to_route_path(np, arc_cost(np.arc)))
                    .collect();
                paths.sort_by_key(|p| p.cost);
                entries.push(RouteEntry {
                    destination: d.coord,
                    paths,
                });
            }
            levels.push(entries);
        }
        Ok(RouteSnapshot { levels })
    }

    /// `update_clusters` (`qspn.vala:1954-2074`): recomputes
    /// `my_fingerprints`/`my_nodes_inside` bottom-up from the current
    /// destination map. The two upstream code paths (level 1 special-cased,
    /// levels above folded generically) differ only in how a level-0
    /// destination's `nodes_inside` contributes: upstream hardcodes `+= 1`
    /// per level-0 destination (a level-0 destination is by definition one
    /// real node) rather than trusting that destination's own
    /// `nodes_inside` field; every other level sums the destination's own
    /// evaluated `nodes_inside`. That is the only asymmetry, so this port
    /// folds both into one loop via `child_level == 0`.
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from [`Fingerprint::elder_seed`]/
    /// [`Fingerprint::construct`].
    pub fn update_clusters(&mut self) -> Result<Vec<QspnEvent>, QspnError> {
        let levels = self.levels();
        let mut events = Vec::new();
        for i in 1..=levels {
            let child_level = i - 1;
            let mut fp_set = Vec::new();
            let mut nn_tot: u64 = 0;
            for d in self.destinations[child_level].values() {
                let (fp, nn, _) = d.evaluate(|a| self.arc_cost(a))?;
                fp_set.push(fp);
                nn_tot += if child_level == 0 { 1 } else { u64::from(nn) };
            }
            // is_null_eldership: my own membership at child_level is virtual
            // (a reserved-but-unplaced slot). This feeds `construct`'s
            // champion race, recorded into the new fingerprint's
            // `elderships_seed` trail (used by `elder_seed` for split/merge
            // arbitration) — a virtual member's claim there, not this
            // fingerprint's own plain per-level `eldership` value, which is
            // always this node's next scheduled claim regardless of
            // virtuality (`qspn.vala:1962-1966,2010-2014`).
            let is_null_eldership = self.my_naddr.is_virtual_at(child_level).unwrap_or(false);
            let new_fp = self.my_fingerprints[child_level].construct(&fp_set, is_null_eldership)?;
            if !new_fp.identity_eq(&self.my_fingerprints[i]) {
                events.push(QspnEvent::ChangedFingerprint(i));
            }
            self.my_fingerprints[i] = new_fp;
            let new_nn = u32::try_from(u64::from(self.my_nodes_inside[child_level]) + nn_tot)
                .unwrap_or(u32::MAX);
            if new_nn != self.my_nodes_inside[i] {
                events.push(QspnEvent::ChangedNodesInside(i));
            }
            self.my_nodes_inside[i] = new_nn;
        }
        Ok(events)
    }

    /// `update_map` (`qspn.vala:1334-1816`): merges freshly revised paths
    /// (`q_set`, from [`crate::revise_etp`]) into the destination map.
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from [`Fingerprint::elder_seed`].
    pub fn update_map(
        &mut self,
        q_set: &[NodePath],
        a_changed: Option<ArcId>,
    ) -> Result<UpdateMapOutcome, QspnError> {
        let levels = self.levels();
        let z_set: Vec<Vec<HCoord>> = (0..levels).map(|i| self.my_gnode_neighbors(i)).collect();
        let arc_costs: HashMap<ArcId, Cost> =
            self.arcs.iter().map(|(&id, e)| (id, e.cost)).collect();
        let arc_cost = |a: ArcId| arc_costs.get(&a).copied().unwrap_or(Cost::Dead);

        // Group q_set by destination (qspn.vala:1350-1359).
        let mut q_by_dest: HashMap<HCoord, Vec<NodePath>> = HashMap::new();
        for np in q_set {
            let d = *np.path.hops.last().expect("path always has >= 1 hop");
            q_by_dest.entry(d).or_default().push(np.clone());
        }

        // Sort destinations: ascending level, then ascending hop-count at
        // that level of each destination's cheapest candidate
        // (qspn.vala:1360-1398).
        let mut sorted_keys: Vec<HCoord> = q_by_dest.keys().copied().collect();
        sorted_keys.sort_by(|&d0, &d1| {
            if d0.level != d1.level {
                return d0.level.cmp(&d1.level);
            }
            let l = d0.level;
            let hops_in_l = |d: HCoord| -> usize {
                let qd = &q_by_dest[&d];
                let best = qd
                    .iter()
                    .min_by_key(|np| np.total_cost(arc_cost(np.arc)))
                    .expect("non-empty group");
                best.path.hops.iter().filter(|h| h.level == l).count()
            };
            hops_in_l(d0).cmp(&hops_in_l(d1))
        });

        let mut all_paths_set = Vec::new();
        let mut b_set: Vec<HCoord> = Vec::new();
        let mut events = Vec::new();
        let mut split_signals = Vec::new();
        for d in sorted_keys {
            let outcome =
                self.update_map_one_destination(&q_by_dest[&d], d, a_changed, &z_set, &arc_costs)?;
            all_paths_set.extend(outcome.all_paths_set);
            if outcome.first_detection_split && !b_set.contains(&d) {
                b_set.push(d);
            }
            events.extend(outcome.events);
            split_signals.extend(outcome.split_signals);
        }
        Ok(UpdateMapOutcome {
            all_paths_set,
            b_set,
            events,
            split_signals,
        })
    }

    /// One destination's admission pass (`qspn.vala:1399-1815`'s per-`d` loop
    /// body).
    fn update_map_one_destination(
        &mut self,
        qd_set_orig: &[NodePath],
        d: HCoord,
        a_changed: Option<ArcId>,
        z_set: &[Vec<HCoord>],
        arc_costs: &HashMap<ArcId, Cost>,
    ) -> Result<OneDestOutcome, QspnError> {
        let arc_cost = |a: ArcId| arc_costs.get(&a).copied().unwrap_or(Cost::Dead);
        let overlap = self.config.overlap_weights;
        let max_paths = self.config.max_paths;
        let tol = self.config.nodes_inside_tolerance;

        let mut qd_set: Vec<NodePath> = qd_set_orig.to_vec();
        let md_set: Vec<NodePath> = self.destinations[d.level]
            .get(&d.pos)
            .map(|dd| dd.paths.clone())
            .unwrap_or_default();

        // f1: fingerprints already known for d before this update
        // (qspn.vala:1408-1412). Deduped by `same_branch`, not bare
        // `identity_eq`: two tied, differently-identified members of the
        // *same* g-node (see `same_branch`'s docs) must collapse to one
        // entry here too, or a branch that only changes which tied member's
        // fingerprint object currently represents it looks like a brand new
        // fingerprint next round and spuriously re-triggers
        // `first_detection_split` below.
        let mut f1: Vec<Fingerprint<Vec<u8>>> = Vec::new();
        if d.level > 0 {
            for np in &md_set {
                if !f1.iter().any(|f| f.same_branch(&np.path.fingerprint)) {
                    f1.push(np.path.fingerprint.clone());
                }
            }
        }

        // Merge md_set with qd_set into od_set/vd_set (qspn.vala:1416-1454).
        let mut od_set: Vec<NodePath> = Vec::new();
        let mut vd_set: Vec<NodePath> = Vec::new();
        for p1 in &md_set {
            let match_idx = qd_set
                .iter()
                .position(|p_test| p_test.hops_arcs_equal(&p1.path));
            match match_idx {
                Some(idx) => {
                    let p2 = qd_set.remove(idx);
                    let fp_changed = !p1.path.fingerprint.identity_eq(&p2.path.fingerprint);
                    let cost_variation = important_variation(p1.path.cost, p2.path.cost);
                    let ni_changed = (f64::from(p1.path.nodes_inside) * (1.0 + tol)
                        < f64::from(p2.path.nodes_inside))
                        || (f64::from(p1.path.nodes_inside) * (1.0 - tol)
                            > f64::from(p2.path.nodes_inside));
                    if fp_changed || cost_variation || ni_changed {
                        od_set.push(p2.clone());
                        vd_set.push(p2);
                    } else {
                        od_set.push(p1.clone());
                        if a_changed == Some(p1.arc) {
                            vd_set.push(p1.clone());
                        }
                    }
                }
                None => {
                    od_set.push(p1.clone());
                    if a_changed == Some(p1.arc) {
                        vd_set.push(p1.clone());
                    }
                }
            }
        }
        od_set.extend(qd_set);

        // Sort ascending cost, then drop candidates through a still-unknown
        // intermediate hop (qspn.vala:1455-1486).
        od_set.sort_by_key(|np| np.total_cost(arc_cost(np.arc)));
        let mut num_nodes_inside: HashMap<HCoord, u32> = HashMap::new();
        let mut od_i = 0;
        while od_i < od_set.len() {
            let hops = od_set[od_i].path.hops.clone();
            let mut to_remove = false;
            for h in &hops[..hops.len().saturating_sub(1)] {
                if let Some(dest) = self.destinations[h.level].get(&h.pos) {
                    let (_, nn, _) = dest.evaluate(|a| self.arc_cost(a))?;
                    num_nodes_inside.insert(*h, nn);
                } else {
                    to_remove = true;
                    break;
                }
            }
            if to_remove {
                od_set.remove(od_i);
            } else {
                od_i += 1;
            }
        }

        // vnd: g-nodes I reach via a direct arc; z1d: my own siblings at every
        // level below d (qspn.vala:1487-1502).
        let mut vnd: Vec<HCoord> = Vec::new();
        for e in self.arcs.values() {
            if let Some(peer) = &e.peer_naddr
                && let Some(v) = self.my_naddr.hcoord(peer).map_err(QspnError::Common)?
                && !vnd.contains(&v)
            {
                vnd.push(v);
            }
        }
        let mut z1d: Vec<HCoord> = Vec::new();
        for zi in z_set.iter().take(d.level) {
            z1d.extend(zi.iter().copied());
        }

        // Size/gateway-adaptive overlap tolerance (qspn.vala:1503-1521).
        let mut mch = self.config.max_common_hops_ratio;
        if let Some(dest_entry) = self.destinations[d.level].get(&d.pos) {
            let (_, size, _) = dest_entry.evaluate(|a| self.arc_cost(a))?;
            let exposed = self.exposed_paths(d)?;
            let mut avail_arcs: Vec<ArcId> = Vec::new();
            for p in &exposed {
                if !avail_arcs.contains(&p.arc) {
                    avail_arcs.push(p.arc);
                }
            }
            mch = crate::mch_ratio::mch_ratio(
                self.config.max_common_hops_ratio,
                &self.config.mch_ratio_table,
                size,
                avail_arcs.len() as u32,
            );
        }

        // Disjoint-path admission (qspn.vala:1522-1621).
        let mut fd: Vec<Fingerprint<Vec<u8>>> = Vec::new();
        let mut rd: Vec<NodePath> = Vec::new();
        for p1 in &od_set {
            if p1.path.cost.is_dead() {
                break;
            }
            let mut mandatory = false;
            if !fd.iter().any(|f| f.identity_eq(&p1.path.fingerprint)) {
                mandatory = true;
                fd.push(p1.path.fingerprint.clone());
            }
            let mut g_i = 0;
            while g_i < vnd.len() {
                if !p1.path.hops.contains(&vnd[g_i]) {
                    vnd.remove(g_i);
                    mandatory = true;
                } else {
                    g_i += 1;
                }
            }
            let mut g_i = 0;
            while g_i < z1d.len() {
                if p1.path.hops.contains(&z1d[g_i]) {
                    z1d.remove(g_i);
                    mandatory = true;
                } else {
                    g_i += 1;
                }
            }
            if mandatory {
                rd.push(p1.clone());
                continue;
            }
            if rd.len() >= max_paths {
                continue;
            }
            let mut insert = true;
            for p2 in &rd {
                let mut total_hops = 0.0f64;
                let mut common_hops = 0.0f64;
                for g2_i in 0..p2.path.hops.len().saturating_sub(1) {
                    let g2 = p2.path.hops[g2_i];
                    let arc_in_g2 = p2.path.arcs[g2_i];
                    let arc_out_g2 = p2.path.arcs[g2_i + 1];
                    let nn = num_nodes_inside.get(&g2).copied().unwrap_or(0);
                    let n_nodes = (overlap.intermediate_coeff * f64::from(nn).sqrt()).floor();
                    total_hops += n_nodes;
                    if p1.path.hops.contains(&g2) {
                        if p1.path.arcs.contains(&arc_in_g2) {
                            if p1.path.arcs.contains(&arc_out_g2) {
                                common_hops += n_nodes;
                            } else {
                                common_hops += (0.5 * n_nodes).ceil();
                            }
                        } else if p1.path.arcs.contains(&arc_out_g2) {
                            common_hops += (0.5 * n_nodes).ceil();
                        }
                    }
                }
                if d.level > 0
                    && let Some(dest_entry) = self.destinations[d.level].get(&d.pos)
                {
                    let (_, nn_d, _) = dest_entry.evaluate(|a| self.arc_cost(a))?;
                    let mut n_nodes = (overlap.destination_coeff * f64::from(nn_d).sqrt()).floor();
                    if n_nodes > 0.0 {
                        n_nodes -= overlap.destination_offset;
                    }
                    if n_nodes > 0.0 {
                        let arc_in_d = p2.path.arcs[p2.path.hops.len() - 1];
                        total_hops += n_nodes;
                        if p1.path.arcs.contains(&arc_in_d) {
                            common_hops += n_nodes;
                        }
                    }
                }
                if total_hops > 0.0 && common_hops / total_hops > mch {
                    insert = false;
                    break;
                }
            }
            if insert {
                rd.push(p1.clone());
            }
        }
        let mut od_set = rd;

        // Winning fingerprint of the *updated* set (qspn.vala:1623-1640).
        let valid_fp_d: Option<Fingerprint<Vec<u8>>> = if d.level > 0 {
            winning_fingerprint(&od_set)?
        } else {
            None
        };

        let mut sd: Vec<QspnEvent> = Vec::new();
        let mut all_paths_set: Vec<EtpPath> = Vec::new();

        // New/kept candidates not previously in md_set (qspn.vala:1642-1661).
        for p in od_set.iter_mut() {
            let already_known = md_set.iter().any(|m| m.hops_arcs_equal(&p.path));
            if already_known {
                continue;
            }
            all_paths_set.push(prepare_for_sending(p, arc_cost(p.arc)));
            if d.level == 0 {
                sd.push(QspnEvent::PathAdded(to_route_path(p, arc_cost(p.arc))));
            } else if p
                .path
                .fingerprint
                .same_branch(valid_fp_d.as_ref().expect("d.level > 0"))
            {
                sd.push(QspnEvent::PathAdded(to_route_path(p, arc_cost(p.arc))));
                p.exposed = true;
            }
        }

        // Existing paths: removed, changed, or untouched (qspn.vala:1662-1717).
        for p in &md_set {
            let fp_d_p = p.path.fingerprint.clone();
            match od_set.iter().position(|o| o.hops_arcs_equal(&p.path)) {
                None => {
                    let mut pp = prepare_for_sending(p, arc_cost(p.arc));
                    pp.cost = Cost::Dead;
                    all_paths_set.push(pp);
                    if d.level == 0 || p.exposed {
                        sd.push(QspnEvent::PathRemoved(to_route_path(p, arc_cost(p.arc))));
                    }
                }
                Some(idx) => {
                    let is_changed = vd_set.iter().any(|v| v.hops_arcs_equal(&p.path));
                    if !is_changed {
                        continue;
                    }
                    all_paths_set
                        .push(prepare_for_sending(&od_set[idx], arc_cost(od_set[idx].arc)));
                    if d.level == 0 {
                        sd.push(QspnEvent::PathChanged(to_route_path(
                            &od_set[idx],
                            arc_cost(od_set[idx].arc),
                        )));
                    } else if p.exposed {
                        // Compares the *old* path's fingerprint against the
                        // new winner, not the replacement's — literal
                        // upstream behavior (qspn.vala:1696-1705), preserved
                        // as-is even though it looks asymmetric.
                        if fp_d_p.same_branch(valid_fp_d.as_ref().expect("d.level > 0")) {
                            sd.push(QspnEvent::PathChanged(to_route_path(
                                &od_set[idx],
                                arc_cost(od_set[idx].arc),
                            )));
                            od_set[idx].exposed = true;
                        } else {
                            sd.push(QspnEvent::PathRemoved(to_route_path(p, arc_cost(p.arc))));
                        }
                    } else if fp_d_p.same_branch(valid_fp_d.as_ref().expect("d.level > 0")) {
                        sd.push(QspnEvent::PathAdded(to_route_path(
                            &od_set[idx],
                            arc_cost(od_set[idx].arc),
                        )));
                        od_set[idx].exposed = true;
                    }
                }
            }
        }

        if md_set.is_empty() && !od_set.is_empty() {
            sd.insert(0, QspnEvent::DestinationAdded(d));
        }
        if !md_set.is_empty() && od_set.is_empty() {
            sd.push(QspnEvent::DestinationRemoved(d));
        }

        // Commit to memory (qspn.vala:1727-1736).
        if od_set.is_empty() {
            self.destinations[d.level].remove(&d.pos);
        } else {
            self.destinations[d.level].insert(
                d.pos,
                Destination {
                    coord: d,
                    paths: od_set,
                },
            );
        }

        // Fingerprint-split check (qspn.vala:1751-1814). Deduped by
        // `same_branch`, not bare `identity_eq`: two tied,
        // differently-identified fingerprints from real members of the
        // *same* g-node are the ordinary outcome of a densely-connected
        // g-node with more than one direct gateway into it (e.g. a K4 mesh
        // where this destination is reached via two disjoint arcs, one to
        // each of two eldership-tied siblings) — not a fork. Only
        // fingerprints `same_branch` cannot reconcile land in different
        // `f2` entries, so only a genuine, orderable disagreement between
        // two distinct g-nodes ever reaches the `split_signals` below.
        let mut first_detection_split = false;
        let mut split_signals = Vec::new();
        if d.level > 0
            && let Some(dest_now) = self.destinations[d.level].get(&d.pos)
        {
            let mut f2: Vec<Fingerprint<Vec<u8>>> = Vec::new();
            for np in &dest_now.paths {
                if !f2.iter().any(|f| f.same_branch(&np.path.fingerprint)) {
                    f2.push(np.path.fingerprint.clone());
                }
            }
            if f2.len() > 1 {
                if f2.iter().any(|fp| !f1.iter().any(|f| f.same_branch(fp))) {
                    first_detection_split = true;
                }
                // Indistinguishable (tied, differently-identified) fingerprints
                // never propagate a fatal error here either — see
                // `winning_fingerprint`'s docs for why that outcome is
                // ordinary between two real members of the same g-node. The
                // fold just keeps its current `fp_eldest` deterministically.
                let mut fp_eldest = f2[0].clone();
                for fp in &f2[1..] {
                    match fp.elder_seed(&fp_eldest) {
                        Ok(true) => fp_eldest = fp.clone(),
                        Ok(false) | Err(ntk_common::Error::IndistinguishableFingerprints) => {}
                        Err(e) => return Err(QspnError::Common(e)),
                    }
                }
                let cheapest_with = |fp: &Fingerprint<Vec<u8>>| -> NodePath {
                    dest_now
                        .paths
                        .iter()
                        .filter(|np| np.path.fingerprint.same_branch(fp))
                        .min_by_key(|np| np.total_cost(arc_cost(np.arc)))
                        .cloned()
                        .expect("fp came from this destination's own path set")
                };
                let bp_eldest = cheapest_with(&fp_eldest);
                for fp in f2.iter().filter(|fp| !fp.same_branch(&fp_eldest)) {
                    let bp = cheapest_with(fp);
                    split_signals.push(SplitSignal {
                        destination: d,
                        fp_eldest: fp_eldest.clone(),
                        fp: fp.clone(),
                        bp_eldest: bp_eldest.clone(),
                        bp,
                    });
                }
            }
        }

        Ok(OneDestOutcome {
            all_paths_set,
            first_detection_split,
            events: sd,
            split_signals,
        })
    }

    /// `signal_split`'s dedup gate (`qspn.vala:1842-1844,1851`):
    /// `pending_gnode_split` prevents scheduling a second debounce timer for
    /// a fingerprint pair already awaiting one. Returns `true` (and records
    /// the pair) the first time this exact `(fp_eldest, fp)` pair is seen;
    /// callers should schedule the debounce timer only on `true`, and MUST
    /// call [`Self::clear_pending_split`] when the timer fires.
    pub fn begin_pending_split(
        &mut self,
        fp_eldest: &Fingerprint<Vec<u8>>,
        fp: &Fingerprint<Vec<u8>>,
    ) -> bool {
        self.pending_gnode_split
            .insert((fp_key(fp_eldest), fp_key(fp)))
    }

    /// Clears a pending split pair once its debounce timer has fired
    /// (`qspn.vala:1851`).
    pub fn clear_pending_split(
        &mut self,
        fp_eldest: &Fingerprint<Vec<u8>>,
        fp: &Fingerprint<Vec<u8>>,
    ) {
        self.pending_gnode_split
            .remove(&(fp_key(fp_eldest), fp_key(fp)));
    }

    /// Post-debounce re-check: which of this node's direct-neighbor arcs to
    /// `d` should now actually emit `GnodeSplitted` for `fp`
    /// (`signal_split`'s re-check after its wait, `qspn.vala:1852-1883`).
    /// Empty if the eldest fingerprint is no longer present at `d` (the fork
    /// healed) or `d` is not (or no longer) a direct-neighbor gnode.
    #[must_use]
    pub fn split_still_live(
        &self,
        d: HCoord,
        fp_eldest: &Fingerprint<Vec<u8>>,
        fp: &Fingerprint<Vec<u8>>,
    ) -> Vec<ArcId> {
        let Some(dest) = self.destinations.get(d.level).and_then(|m| m.get(&d.pos)) else {
            return Vec::new();
        };
        if !dest
            .paths
            .iter()
            .any(|np| np.path.fingerprint.identity_eq(fp_eldest))
        {
            return Vec::new();
        }
        let mut hits = Vec::new();
        for (&arc, entry) in &self.arcs {
            let Some(peer) = &entry.peer_naddr else {
                continue;
            };
            let Ok(Some(v)) = self.my_naddr.hcoord(peer) else {
                continue;
            };
            if v != d {
                continue;
            }
            if let Some(np) = dest.paths.iter().find(|np| np.arc == arc)
                && np.path.fingerprint.identity_eq(fp)
            {
                hits.push(arc);
            }
        }
        hits
    }

    /// `make_connectivity` (`qspn.vala:2226-2263`): turns this identity into
    /// a *connectivity* one spanning `[connectivity_from_level,
    /// connectivity_to_level]`, used mid-migration to keep this g-node's
    /// external routing alive while a successor identity re-hooks at the new
    /// position. Rewrites `my_naddr` (via `update_naddr`) and every internal
    /// arc's peer address with the same delegate, moves this identity's own
    /// position at `connectivity_from_level - 1` from real to virtual, and
    /// recomputes clusters.
    ///
    /// Deviation from upstream: `publish_connectivity`'s delayed void-ETP
    /// announcement to outer arcs (`qspn.vala:2255-2262`, `etp_publish.vala:
    /// 110-144`) is outbound I/O, so it is the actor's job (see module docs
    /// on why no `QspnState` method performs I/O), driven off
    /// [`MakeConnectivityOutcome::old_position`].
    ///
    /// # Panics
    /// If `connectivity_from_level > connectivity_to_level`,
    /// `connectivity_to_level > levels()`, `connectivity_from_level == 0`
    /// (`qspn.vala:2232-2234`'s asserts), or this identity's own position at
    /// `connectivity_from_level - 1` is not currently real (`qspn.vala:2236`).
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from the [`Self::update_clusters`]
    /// climb.
    pub fn make_connectivity(
        &mut self,
        connectivity_from_level: usize,
        connectivity_to_level: usize,
        update_naddr: impl Fn(&Naddr) -> Naddr,
    ) -> Result<MakeConnectivityOutcome, QspnError> {
        assert!(connectivity_from_level <= connectivity_to_level);
        assert!(connectivity_to_level <= self.levels());
        assert!(connectivity_from_level > 0);
        let old_lvl = connectivity_from_level - 1;
        let old_pos = self.my_naddr.pos(old_lvl).expect("old_lvl < levels");
        assert_eq!(
            self.my_naddr.is_virtual_at(old_lvl),
            Some(false),
            "make_connectivity requires a currently-real position (qspn.vala:2236)"
        );

        let internal_arcs: Vec<ArcId> = self
            .arcs
            .iter()
            .filter_map(|(&id, e)| {
                let peer = e.peer_naddr.as_ref()?;
                let lvl = self.my_naddr.hcoord(peer).ok().flatten()?.level;
                (lvl < old_lvl).then_some(id)
            })
            .collect();

        self.my_naddr = update_naddr(&self.my_naddr);
        for arc in &internal_arcs {
            if let Some(entry) = self.arcs.get_mut(arc)
                && let Some(peer) = entry.peer_naddr.take()
            {
                entry.peer_naddr = Some(update_naddr(&peer));
            }
        }
        self.connectivity_from_level = connectivity_from_level;
        self.connectivity_to_level = connectivity_to_level;
        debug_assert_eq!(
            self.my_naddr.is_virtual_at(old_lvl),
            Some(true),
            "make_connectivity must leave the old position virtual (qspn.vala:2251)"
        );

        let events = self.update_clusters()?;
        Ok(MakeConnectivityOutcome {
            old_position: HCoord::new(old_lvl, old_pos),
            events,
        })
    }

    /// Shared by [`Self::exit_network`]: drops every destination (and its
    /// paths) at/above `lvl`, emitting `PathRemoved`/`DestinationRemoved`
    /// for each (`qspn.vala:2283-2296`).
    fn strip_destinations_at_or_above(&mut self, lvl: usize, events: &mut Vec<QspnEvent>) {
        let top = self.levels();
        for level_map in &mut self.destinations[lvl.min(top)..top] {
            for (_, d) in level_map.drain() {
                for np in &d.paths {
                    events.push(QspnEvent::PathRemoved(to_route_path(np, Cost::Dead)));
                }
                events.push(QspnEvent::DestinationRemoved(d.coord));
            }
        }
    }

    /// `exit_network(lvl)` (`qspn.vala:2280-2334`): drops every destination
    /// and path at/above `lvl`, removes every arc whose peer belongs to a
    /// g-node at/above `lvl`, and recomputes clusters.
    ///
    /// Deviation from upstream: the reference implementation strips the map
    /// twice (`qspn.vala:2283-2296,2320-2334`) — once before removing the
    /// departing arcs, once after, "just to be sure" — a defensive measure
    /// against upstream's own cooperative-tasklet interleaving, where a
    /// signal handler could in principle touch the map between the two
    /// steps. Nothing can run between them in this actor's single-threaded
    /// command loop, so the second pass is a provable no-op here (every
    /// destination it would touch, [`Self::remove_arc`] already removed as a
    /// side effect of dropping the departing arcs' paths); this port keeps
    /// one pass. Also omitted: upstream sends every *surviving* arc a
    /// heads-up full ETP before actually removing the departing ones
    /// (`qspn.vala:2308-2313`) — outbound I/O, the actor's job (see module
    /// docs), using [`ExitNetworkOutcome::removed_arcs`] to know which arcs
    /// were dropped and therefore which arcs are the survivors to notify.
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from the [`Self::update_clusters`]
    /// climbs.
    pub fn exit_network(&mut self, lvl: usize) -> Result<ExitNetworkOutcome, QspnError> {
        let mut events = Vec::new();
        self.strip_destinations_at_or_above(lvl, &mut events);
        events.extend(self.update_clusters()?);

        let removed_arcs: Vec<ArcId> = self
            .arcs
            .iter()
            .filter_map(|(&id, e)| {
                let peer = e.peer_naddr.as_ref()?;
                let arc_lvl = self.my_naddr.hcoord(peer).ok().flatten()?.level;
                (arc_lvl >= lvl).then_some(id)
            })
            .collect();
        for &arc in &removed_arcs {
            let removal = self.remove_arc(arc);
            events.extend(removal.events);
            events.push(QspnEvent::ArcRemoved {
                arc,
                bad_link: false,
            });
        }

        events.extend(self.update_clusters()?);
        Ok(ExitNetworkOutcome {
            events,
            removed_arcs,
        })
    }

    /// `check_connectivity` (`qspn.vala:2371-2448`): true if removing this
    /// connectivity identity would not disconnect any g-node it currently
    /// bridges — the daemon's own precondition before it may retire this
    /// identity. Read-only; no state mutation, no I/O.
    ///
    /// # Panics
    /// If called on a main identity (`connectivity_from_level == 0`), or if
    /// `connectivity_to_level < connectivity_from_level` or `>
    /// levels()` — both are this identity's own construction invariants
    /// (`qspn.vala:2375-2377`'s asserts), never a caller input.
    #[must_use]
    pub fn check_connectivity(&self) -> bool {
        assert!(!self.is_main_identity());
        assert!(self.connectivity_to_level >= self.connectivity_from_level);
        assert!(self.connectivity_to_level <= self.levels());
        let mut i = self.connectivity_from_level - 1;
        let j = self.connectivity_to_level;
        loop {
            if j <= i {
                return true;
            }
            if !self.destinations[i].is_empty() {
                break;
            }
            i += 1;
        }

        let x_set: Vec<&Destination> = (i..j).flat_map(|l| self.destinations[l].values()).collect();
        let mut y_set: Vec<HCoord> = Vec::new();
        for x in &x_set {
            for np in &x.paths {
                let mut y = np.path.hops[0];
                if y.level == i {
                    if !y_set.contains(&y) {
                        y_set.push(y);
                    }
                    continue;
                }
                for idx in 1..np.path.hops.len() {
                    let y_prev = np.path.hops[idx - 1];
                    y = np.path.hops[idx];
                    if y.level == i && y_prev.level < i {
                        if !y_set.contains(&y) {
                            y_set.push(y);
                        }
                        break;
                    }
                }
            }
        }

        for x in &x_set {
            for &y in &y_set {
                if x.coord != y {
                    let path_found = x.paths.iter().any(|np| np.path.hops.contains(&y));
                    if !path_found {
                        return false;
                    }
                }
            }
        }
        true
    }
}

/// [`QspnState::make_connectivity`]'s result: the identity's own former
/// (now-vacated) position, which the actor uses to build the delayed
/// `publish_connectivity` void-ETP announcement (`qspn.vala:2255-2262`).
#[derive(Clone, Debug)]
pub struct MakeConnectivityOutcome {
    pub old_position: HCoord,
    pub events: Vec<QspnEvent>,
}

/// [`QspnState::exit_network`]'s result: `removed_arcs` are the arcs whose
/// peer belonged to a g-node at/above the exited level — the actor uses this
/// to know which of the *other* current arcs are the survivors that still
/// need a heads-up full ETP (`qspn.vala:2308-2313`).
#[derive(Clone, Debug, Default)]
pub struct ExitNetworkOutcome {
    pub events: Vec<QspnEvent>,
    pub removed_arcs: Vec<ArcId>,
}

/// `i_qspn_important_variation` for this crate's [`Cost`]: `Dead`/`Null`
/// transitions are always significant; two `Finite` costs differ
/// significantly whenever they differ at all (`qspn.vala/api.vala:66-70,97-101`
/// — the sole reference implementations both treat *any* numeric change as
/// "important", deferring real hysteresis to a future `Cost` implementation).
fn important_variation(old: Cost, new: Cost) -> bool {
    old != new
}

#[cfg(test)]
mod eldership_tests {
    use ntk_common::{FingerprintParts, Topology};

    use super::*;

    fn test_naddr(levels: usize) -> Naddr {
        let topology = Topology::new(vec![4u32; levels]).expect("valid topology");
        Naddr::new(topology, vec![0u32; levels]).expect("valid address")
    }

    fn test_state(levels: usize, eldership: u32, pending: &[u32]) -> QspnState {
        let fp = Fingerprint::new(vec![1u8], eldership, pending.to_vec());
        QspnState::new(test_naddr(levels), fp, QspnConfig::default())
    }

    fn leaf_path(fp: Fingerprint<Vec<u8>>, coord: HCoord, arc: u32) -> NodePath {
        NodePath::new(
            ArcId::from(arc),
            EtpPath {
                hops: vec![coord],
                arcs: vec![ArcId::from(arc)],
                cost: Cost::Finite(1),
                fingerprint: fp,
                nodes_inside: 1,
                ignore_outside: vec![false; coord.level + 1],
            },
        )
    }

    #[test]
    fn my_eldership_climbs_the_fingerprint_chain_and_stops_at_the_top() {
        // level0 eldership=5; construct() consumes pending[0]=7 for level1's
        // own claim, then pending[1]=9 for level2's.
        let state = test_state(2, 5, &[7, 9]);
        assert_eq!(state.my_eldership(0), Some(Some(5)));
        assert_eq!(state.my_eldership(1), Some(Some(7)));
        assert_eq!(state.my_eldership(2), Some(Some(9)));
        // One past the top: fingerprint(3) does not exist.
        assert_eq!(state.my_eldership(3), None);
    }

    #[test]
    fn destination_eldership_agrees_with_winning_fingerprint() {
        let mut state = test_state(2, 0, &[0, 0]);
        let d = HCoord::new(1, 2);
        // Elder-seed favors the lower elderships_seed entry: fp_a (seed 1)
        // outranks fp_b (seed 2), so fp_a's *own* eldership (100) must win,
        // never fp_b's (200).
        let fp_a = Fingerprint::new(vec![10u8], 1, vec![100u32])
            .construct(&[], false)
            .unwrap();
        let fp_b = Fingerprint::new(vec![20u8], 2, vec![200u32])
            .construct(&[], false)
            .unwrap();
        let paths = vec![leaf_path(fp_a.clone(), d, 1), leaf_path(fp_b, d, 2)];
        let winner = winning_fingerprint(&paths).unwrap().unwrap();
        assert!(winner.identity_eq(&fp_a));
        state.destinations[1].insert(d.pos, Destination { coord: d, paths });
        assert_eq!(
            state.eldership(1, 2).unwrap(),
            Some(winner.to_parts().eldership)
        );
        assert_eq!(state.eldership(1, 2).unwrap(), Some(Some(100)));
    }

    #[test]
    fn unknown_destination_and_level_are_none_not_virtual() {
        let state = test_state(1, 0, &[0]);
        // No destination has ever been recorded at this position.
        assert_eq!(state.eldership(0, 3).unwrap(), None);
        // Level out of range entirely.
        assert_eq!(state.eldership(5, 0).unwrap(), None);
        assert_eq!(state.my_eldership(5), None);
    }

    #[test]
    fn virtual_eldership_surfaces_as_none_not_zero() {
        let mut state = test_state(2, 0, &[0, 0]);
        let d = HCoord::new(1, 4);
        let virtual_fp = Fingerprint::from_parts(FingerprintParts {
            id: vec![9u8],
            level: 1,
            eldership: None,
            pending_elderships: vec![],
            elderships_seed: vec![Some(3)],
        })
        .unwrap();
        let paths = vec![leaf_path(virtual_fp, d, 1)];
        state.destinations[1].insert(d.pos, Destination { coord: d, paths });
        // Some(None): a destination is known, but its winning fingerprint's
        // own eldership is the virtual/null-eldership case — never 0.
        assert_eq!(state.eldership(1, 4).unwrap(), Some(None));
    }
}

#[cfg(test)]
mod migration_tests {
    use std::collections::HashMap;

    use ntk_common::Topology;

    use super::*;

    fn topology2() -> Topology {
        Topology::new(vec![4u32, 4u32]).expect("valid topology")
    }

    fn fp2(id: u8, eldership: u32) -> Fingerprint<Vec<u8>> {
        Fingerprint::new(vec![id], eldership, vec![0u32, 0u32])
    }

    fn empty_previous(levels: usize) -> Vec<HashMap<u32, Destination>> {
        vec![HashMap::new(); levels]
    }

    /// An `enter_net` identity does not publish a level it is still
    /// bootstrapping at ("bootstrap-phase gating"): a level-1 destination
    /// already present in the map (as if learned via an internal arc) is
    /// invisible via [`QspnState::snapshot`]/[`QspnState::eldership`] while
    /// `guest_gnode_level == 1`, and visible the moment bootstrap exits.
    #[test]
    fn entering_identity_gates_publication_until_bootstrap_exits() {
        let naddr = Naddr::new(topology2(), [0, 0]).expect("valid address");
        let mut entering = QspnState::new_entering(
            naddr,
            fp2(1, 0),
            QspnConfig::default(),
            &[],
            &[],
            1,
            2,
            (0, 0),
            &empty_previous(2),
        )
        .expect("valid enter_net construction");
        assert!(!entering.is_bootstrap_complete());
        assert_eq!(entering.guest_gnode_level(), 1);

        let d = HCoord::new(1, 2);
        let dest_fp = Fingerprint::new(vec![9u8], 5, vec![100u32])
            .construct(&[], false)
            .expect("valid construct");
        let np = NodePath::new(
            ArcId::from(1u32),
            EtpPath {
                hops: vec![d],
                arcs: vec![ArcId::from(1u32)],
                cost: Cost::Finite(1),
                fingerprint: dest_fp,
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        entering.update_map(&[np], None).expect("valid update_map");

        let snap = entering.snapshot().expect("valid snapshot");
        assert!(
            snap.levels[1].is_empty(),
            "level 1 must not be published while guest_gnode_level == 1"
        );
        assert_eq!(entering.my_eldership(2), None);
        assert!(matches!(
            entering.eldership(1, 2),
            Err(QspnError::BootstrapInProgress)
        ));

        entering.exit_bootstrap();
        assert!(entering.is_bootstrap_complete());
        let snap = entering.snapshot().expect("valid snapshot");
        assert!(
            !snap.levels[1].is_empty(),
            "level 1 must be published once bootstrap has exited"
        );
        assert!(
            entering
                .eldership(1, 2)
                .expect("known destination")
                .is_some()
        );
    }

    /// A g-node whose position is virtual can never carry a real *elder-seed*
    /// champion claim one level up; the identical setup with the same slot
    /// resolved to a real position does — `update_clusters`'s
    /// `is_null_eldership` (`qspn.vala:1962-1966,2010-2014`) feeds
    /// [`Fingerprint::construct`]'s champion race, which records into
    /// `elderships_seed` (used by [`Fingerprint::elder_seed`] for split/merge
    /// arbitration), not the fingerprint's own plain per-level `eldership`
    /// value (that field is always this node's next scheduled claim,
    /// regardless of virtuality — see `ntk_common::fingerprint`'s own
    /// `virtual_eldership_wins_unconditionally_over_real_siblings` test for
    /// the same distinction). This is the "virtual position resolving to a
    /// real one" transition the elder-seed trail must reflect once hooking
    /// finalizes a slot. In the real protocol this transition is realized by
    /// hooking constructing a *successor* identity once the position
    /// resolves (this crate's actor model has no in-place `Naddr` mutation,
    /// see [`QspnState::new_entering`]'s docs), so this test builds both
    /// identities explicitly rather than mutating one in place.
    #[test]
    fn virtual_position_yields_null_elder_seed_claim_real_position_resolves_it() {
        let topology = topology2();
        let virtual_naddr =
            Naddr::new_allowing_virtual(topology.clone(), [0, 10]).expect("valid address");
        let mut virtual_identity = QspnState::new_entering(
            virtual_naddr,
            fp2(1, 0),
            QspnConfig::default(),
            &[],
            &[],
            1,
            2,
            (0, 0),
            &empty_previous(2),
        )
        .expect("valid enter_net construction");
        virtual_identity.exit_bootstrap();
        let virtual_seed = virtual_identity
            .fingerprint(2)
            .expect("level 2 fingerprint exists")
            .to_parts()
            .elderships_seed;
        assert_eq!(
            virtual_seed[0], None,
            "a virtual level-1 position's champion claim must be null"
        );

        let real_naddr = Naddr::new(topology, [0, 2]).expect("valid address");
        let mut resolved_identity = QspnState::new_entering(
            real_naddr,
            fp2(1, 0),
            QspnConfig::default(),
            &[],
            &[],
            1,
            2,
            (0, 0),
            &empty_previous(2),
        )
        .expect("valid enter_net construction");
        resolved_identity.exit_bootstrap();
        let resolved_seed = resolved_identity
            .fingerprint(2)
            .expect("level 2 fingerprint exists")
            .to_parts()
            .elderships_seed;
        assert_eq!(
            resolved_seed[0],
            Some(0),
            "the same slot, resolved to a real position, must carry a real champion claim"
        );
    }

    /// `new_entering`'s arc remap (`qspn.vala:253-283,308-330`): a path
    /// through a surviving internal arc is remapped to the new local
    /// [`ArcId`]; a path through an arc that did not survive migration is
    /// dropped, and a destination left with no paths is dropped entirely.
    #[test]
    fn internal_arc_remap_drops_unmapped_paths_and_empty_destinations() {
        let old_arc = ArcId::from(100u32);
        let dying_arc = ArcId::from(200u32);
        let new_arc = ArcId::from(1u32);
        let peer_naddr = Naddr::new(topology2(), [1, 0]).expect("valid address");

        let survives = NodePath::new(
            old_arc,
            EtpPath {
                hops: vec![HCoord::new(0, 1)],
                arcs: vec![old_arc],
                cost: Cost::Finite(1),
                fingerprint: Fingerprint::new(vec![9u8], 5, vec![0u32]),
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        let dies = NodePath::new(
            dying_arc,
            EtpPath {
                hops: vec![HCoord::new(0, 2)],
                arcs: vec![dying_arc],
                cost: Cost::Finite(1),
                fingerprint: Fingerprint::new(vec![8u8], 5, vec![0u32]),
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        let mut previous_level0 = HashMap::new();
        previous_level0.insert(
            1u32,
            Destination {
                coord: HCoord::new(0, 1),
                paths: vec![survives],
            },
        );
        previous_level0.insert(
            2u32,
            Destination {
                coord: HCoord::new(0, 2),
                paths: vec![dies],
            },
        );
        let previous_destinations = vec![previous_level0, HashMap::new()];

        let internal_arcs = [InternalArc {
            previous_arc: old_arc,
            new_arc,
            peer_naddr,
            cost: Cost::Finite(1),
        }];
        let entering = QspnState::new_entering(
            Naddr::new(topology2(), [0, 0]).expect("valid address"),
            fp2(1, 0),
            QspnConfig::default(),
            &internal_arcs,
            &[],
            1,
            2,
            (0, 0),
            &previous_destinations,
        )
        .expect("valid enter_net construction");

        let d1 = entering
            .destination(0, 1)
            .expect("surviving destination imported");
        assert_eq!(d1.paths.len(), 1);
        assert_eq!(
            d1.paths[0].arc, new_arc,
            "path must be remapped to the new arc id"
        );
        assert_eq!(d1.paths[0].path.arcs[0], new_arc);
        assert!(
            entering.destination(0, 2).is_none(),
            "a destination reachable only via an arc that did not survive must be dropped"
        );
    }

    /// `make_connectivity` turns a real position virtual and flips
    /// `is_main_identity`; `check_connectivity` then confirms this
    /// connectivity identity (which bridges no destinations at all) may
    /// safely be retired, and `exit_network` drops its remaining arc/map —
    /// creation and retirement of a connectivity identity.
    #[test]
    fn connectivity_identity_created_checked_and_retired() {
        let naddr = Naddr::new(topology2(), [0, 1]).expect("valid address");
        let mut state = QspnState::new(naddr, fp2(1, 0), QspnConfig::default());
        let arc = ArcId::from(1u32);
        state.add_arc(arc, Cost::Finite(1));
        assert!(state.is_main_identity());

        let outcome = state
            .make_connectivity(1, 1, |old| {
                Naddr::new_allowing_virtual(
                    old.topology().clone(),
                    [10, old.pos(1).expect("level 1 exists")],
                )
                .expect("valid virtual address")
            })
            .expect("valid make_connectivity call");
        assert!(!state.is_main_identity());
        assert_eq!(state.connectivity_range(), (1, 1));
        assert_eq!(outcome.old_position, HCoord::new(0, 0));
        assert_eq!(state.my_naddr().is_virtual_at(0), Some(true));

        // Peer diverges at level 0 from the now-virtual `my_naddr`, so it is
        // at/above the exited level — the remaining external arc.
        let peer = Naddr::new(topology2(), [1, 1]).expect("valid address");
        state.record_peer_naddr(arc, peer);

        // No destination depends on this bridge, so retiring it is safe.
        assert!(state.check_connectivity());

        let exit = state.exit_network(0).expect("valid exit_network call");
        assert_eq!(exit.removed_arcs, vec![arc]);
        assert_eq!(state.arcs().count(), 0);
    }
}
