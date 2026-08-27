//! The dependency-inverted wiring: real implementations of every trait `ntk-hooking`,
//! `ntk-peerservices`, and `ntk-coordinator` declare instead of depending on a sibling
//! protocol crate directly. This is the heart of the composition — see each `impl` below for
//! which upstream capability it plays.
//!
//! # Routing model
//! Every "reach some other node" seam in this module ([`RoutingEnvAdapter::gateway`],
//! [`RoutingEnvAdapter::dial`], [`CoordinatorStubFactoryAdapter`]'s neighbor stubs) resolves to
//! **my own already-connected direct neighbor's** [`ntk_rpc::RpcClient`] — never a fresh
//! point-to-point socket to an arbitrary address. This matches this codebase's own established
//! design (`ntk_hooking::routing`'s module doc: "resolves the destination's g-node directly...
//! and lets each hop's own handler recurse the same way") rather than modeling raw IP
//! reachability once kernel routes exist: a message bound for a distant node is forwarded
//! hop-by-hop through each intermediate node's own `RpcHandler`, which runs the identical
//! resolution one hop further, exactly like `ntk-peerservices`' own `forward_msg`.
//!
//! # QSPN eldership
//! [`ntk_hooking::QspnView::my_eldership`]/`eldership` are synchronous ("every method here must
//! be answerable from already-known local state", per that trait's own doc), but the real
//! source of truth (`QspnHandle::my_eldership`/`eldership`, both `async fn(..) -> Result<Option<Option<u32>>,
//! QspnError>`) is not. `EldershipCache` bridges the two: a background task refreshes it on
//! every [`ntk_qspn::QspnEvent`], and [`QspnViewAdapter`]'s sync methods only ever read the
//! cache. Mapping `Result<Option<Option<u32>>, _>` to the plain `i32` `QspnView` demands, without
//! collapsing "virtual/null claim" and "unknown" into the same value: a real claim `n` maps to
//! `n` itself, `FingerprintParts`'s virtual/null case (`Ok(Some(None))`) maps to `-1`
//! (matching this codebase's own established `-1` "not yet known" sentinel convention, e.g.
//! `ntk_hooking::TupleGNode::eldership`), and "unknown" (`Ok(None)`, an actor error, or simply
//! not cached yet) maps to `i32::MAX` — deliberately distinct from `-1` and chosen so an unknown
//! claim never wins a numeric "lowest claim is most senior" tiebreak by accident.
//!
//! # Fixed: `fp_id` now scopes to `network_id`, not a per-g-node fingerprint
//! [`ntk_coordinator::CoordinatorMap::fp_id`] is upstream's per-g-node fingerprint —
//! `ntk_coordinator::actor`'s own `check_propagation` (`coord.vala:424-440`'s two "not my
//! g-node" guards) rejects a propagated `prepare_enter`/`finish_enter`/etc. unless *both* the
//! sender's recorded `positions` *and* `fp_id` match the receiving node's own current ones.
//!
//! Three stand-ins were tried here in turn. A fixed `0` baseline let two coincidentally-co-
//! positioned *unrelated* networks pass the guard by accident — confirmed by direct
//! reproduction of `crates/ntkd/tests/mesh.rs`'s `two_star_groups_merge_into_one_network` at
//! `816d52c`: node `a0` (`NodeId(601)`) and node `a2` (`NodeId(603)`) both hash to level-0
//! position `3` under `crate::node::lifecycle::derive_initial_position`'s own algorithm
//! (topology `gsizes=[8]`), so `a2`'s own `check_propagation` matched `a0`'s propagated
//! `positions=[3]`/`fp_id=0` against its *own* `my_pos(0)==3`/`fp_id(0)==0` and wrongly accepted
//! `a0`'s resolved position as its own.
//!
//! A `network_id`-derived-but-still-per-process-counter baseline closed that but drifted: two
//! real siblings that had processed a *different number* of qspn recomputes (an everyday
//! asymmetry — resolving a migration path teaches the negotiating member about the destination
//! network before its still-uninvolved siblings ever see it) reported different `fp_id`s
//! despite being the identical g-node the guard exists to recognize. Confirmed live via
//! `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit`: the entrant that resolves the
//! merge always finished one extra local counter bump ahead of a still-passive sibling by the
//! time it called `finish_enter`, so `check_propagation` on the receiving end rejected every
//! fan-out — permanently: the counter never re-converges on its own.
//!
//! Switching the source to [`ntk_qspn::QspnHandle::fingerprint_id`] (the real, content-derived
//! champion id `QspnState::update_clusters` settles on, upstream's own
//! `research/impl/vala/ntkd/coordinator_helpers.vala:167-175` projection) closed the *permanent*
//! drift — the value depends on the g-node's actual composition, not on local event counts, so
//! it eventually agrees for two real siblings
//! (`ntk_qspn::manager`'s own `fingerprint_id_agrees_across_members_of_the_same_gnode` pins
//! this). But "eventually" is the residual gap: a real-kernel run of the same scenario still
//! stranded a sibling (`a1`) because `check_propagation` never retries a rejection —
//! `handle_execute_finish_enter` fans a `propagation_id` out exactly once, and a sibling whose
//! own qspn view has not yet finished re-converging its *local* champion computation at the
//! instant the fan-out arrives rejects it for good, with no second delivery to catch up on.
//! Upstream's own doc on this exact value (`fingerprint_id`'s "Same-g-node agreement is NOT
//! unconditional" section) already concedes real siblings can transiently disagree here — that
//! residual risk is upstream-faithful, but this daemon's own compressed real-kernel merge
//! timing hits it often enough to matter, and there is no upstream retry path to fall back on
//! either (`check_propagation`'s one-shot dedup is upstream's own design, `coord.vala:424-440`).
//!
//! **The fix**: `fp_id` now returns [`NetworkInfo::network_id`] directly — the plain identity of
//! the network this identity currently belongs to, which every real sibling shares
//! *unconditionally* and *immediately* (no qspn convergence involved at all) for exactly as long
//! as `check_propagation`'s guard ever needs it: every one of the five propagation kinds this
//! guard covers only ever fans out within the sender's *own current* g-node/network, so sender
//! and receiver share `network_id` by construction at propagation time, migrated or not. Combined
//! with the existing `positions` check (which still disambiguates *which* g-node within a shared
//! network this propagation names), this is strictly at least as safe as the fingerprint ever
//! was for the "two coincidentally-co-positioned unrelated networks" guard this exists for — two
//! *different* networks matching on both a full position suffix and a randomly-drawn 63-bit
//! `network_id` is the same vanishing-probability class fp_id relied on — while adding no
//! convergence dependency and no per-process counter to ever drift. This is a deliberate
//! divergence from upstream's own fingerprint-keyed guard, not a faithfulness gap: `network_id`
//! is this daemon's own concept with no 1:1 upstream analogue, chosen because it closes a defect
//! this port's own compressed real-kernel timing exposes and upstream's slower-cadence
//! deployment apparently does not.
//!
//! The id-generation race independently confirmed and fixed in
//! `crates/ntk-hooking/src/idgen.rs` (a load-then-store, not a single atomic step, letting two
//! genuinely concurrent callers mint the identical `reserve_request_id`/`evaluate_enter_id`) is
//! a real, separate defect worth its own fix, but is not what reproduces the coincidental-
//! collision test's failure: `a2` there never made a `reserve` call of its own at all.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use ntk_common::{HCoord, Naddr, Topology};
use ntk_hooking::{
    CoordinatorClient as HookingCoordinatorClient, CoordinatorError, EvaluateEnterRequest,
    FinishEnterData, FinishMigrationData, HookingHandle, MergeArbitrationRequest,
    Reservation as HookingReservation, merge_tiebreak,
};
use ntk_peerservices::{PeersStub, RoutingEnv, TupleNode};
use ntk_proto::v1::TypedValue;
use ntk_qspn::{ArcId as QspnArcId, QspnHandle, RouteSnapshot};

use crate::node::codec;
use crate::node::peers::PeerLinks;
use crate::node::registry::{LinkId, LinkRegistry};

/// This identity's own network-scoped facts `ntk-qspn` does not track on the daemon's behalf:
/// just the network id it joined/founded. See the module doc's "Fixed: `fp_id`" section for
/// where the per-g-node fingerprint identity lives now (`FingerprintCache`, not here).
#[derive(Debug)]
pub struct NetworkInfo {
    network_id: AtomicI64,
    bootstrapped: std::sync::atomic::AtomicBool,
    /// `(level, pos)` pairs currently known to belong to a *different*, not-yet-merged network
    /// — see [`ntk_hooking::QspnView::note_foreign`]'s doc for why this exists: `ntk_qspn` maps
    /// a foreign arc's peer into its own destination set the moment the arc is reachable, with
    /// no notion of network boundaries, so [`estimate_n_nodes`] and
    /// [`CoordinatorMapAdapter::free_positions`] must filter this set out themselves.
    ///
    /// **Only ever authoritative when [`Self::same_network`] disagrees in `foreign`'s favor**:
    /// `(level, pos)` is a bare numeric coordinate, not a node identity, and two independent,
    /// not-yet-merged networks each assign their own positions from the same small range —
    /// `crates/ntkd/tests/mesh.rs`'s `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit`
    /// found this live: two real 3-member trios, each numbering its own level-0 members
    /// `{0,1,2}` independently, so a member's own external arc negotiations into the *other*
    /// trio call [`Self::note_foreign`] at exactly the numeric positions its *own real
    /// siblings* occupy. See [`Self::is_foreign`]'s doc for how [`Self::same_network`] resolves
    /// the conflict.
    foreign: Mutex<HashSet<(usize, u32)>>,
    /// `(level, pos)` pairs *confirmed*, via this identity's own arc negotiation
    /// ([`ntk_hooking::QspnView::note_same_network`]), to be part of *my own* network. Sticky:
    /// once confirmed, never revisited by a later (or earlier — order is not guaranteed, arc
    /// handlers race independently) [`Self::note_foreign`] call that merely happens to name the
    /// identical numeric position from an unrelated, genuinely foreign peer. See
    /// [`Self::is_foreign`]'s doc.
    same_network: Mutex<HashSet<(usize, u32)>>,
}

impl NetworkInfo {
    #[must_use]
    pub fn new(_levels: usize, initial_network_id: i64) -> Self {
        Self {
            network_id: AtomicI64::new(initial_network_id),
            bootstrapped: std::sync::atomic::AtomicBool::new(false),
            foreign: Mutex::new(HashSet::new()),
            same_network: Mutex::new(HashSet::new()),
        }
    }

    pub fn set_network_id(&self, id: i64) {
        self.network_id.store(id, Ordering::Relaxed);
    }

    #[must_use]
    pub fn network_id(&self) -> i64 {
        self.network_id.load(Ordering::Relaxed)
    }

    /// See [`ntk_hooking::QspnView::note_foreign`]'s doc, and [`Self::is_foreign`]'s for why
    /// this alone is never enough to conclude `pos` at `level` is actually foreign.
    pub fn note_foreign(&self, level: usize, pos: u32) {
        self.foreign
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((level, pos));
    }

    /// See [`ntk_hooking::QspnView::note_same_network`]'s doc. Sticky: recorded in
    /// `Self::same_network`, never merely subtracted from `Self::foreign` — a later
    /// (arc-handler ordering is not guaranteed) [`Self::note_foreign`] call naming the same
    /// `(level, pos)` from an unrelated foreign peer must not re-poison a position this
    /// identity's own negotiation already confirmed.
    pub fn note_same_network(&self, level: usize, pos: u32) {
        self.same_network
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((level, pos));
    }

    /// `pos` at `level` is foreign iff *some* arc reported it foreign and *no* arc has ever
    /// confirmed it as my own network — `Self::same_network` always wins, regardless of call
    /// order, over a merely coincidental numeric collision with an unrelated foreign peer (see
    /// `Self::foreign`'s own doc for the real-kernel run that found this). A position this
    /// identity's own arc negotiation confirmed is a *fact* about my own network's structure;
    /// nothing an unrelated foreign peer reports can outweigh it.
    #[must_use]
    pub fn is_foreign(&self, level: usize, pos: u32) -> bool {
        if self
            .same_network
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&(level, pos))
        {
            return false;
        }
        self.foreign
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&(level, pos))
    }

    /// Every `(level, pos)` this identity currently believes is foreign — [`Self::is_foreign`]'s
    /// own set logic, enumerated rather than asked about one position at a time. Feeds
    /// `foreign_exclusions`, which turns these into the
    /// [`ntk_peerservices::TupleGNode`] exclusion list every `ntk_coordinator::CoordinatorClient`
    /// DHT round trip now seeds — see [`ntk_coordinator::CoordinatorClient::reserve`]'s own doc
    /// for the misroute this closes: without it, `target_for`'s elect-key (matched by raw
    /// position alone) can resolve to a physically reachable but logically foreign node that
    /// merely happens to claim the same numeric position as this identity's own network.
    #[must_use]
    pub fn foreign_positions(&self) -> Vec<(usize, u32)> {
        let same_network = self.same_network.lock().unwrap_or_else(|e| e.into_inner());
        self.foreign
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|entry| !same_network.contains(entry))
            .copied()
            .collect()
    }

    /// Latches [`ntk_qspn::QspnEvent::BootstrapComplete`] — see [`Self::is_bootstrapped`].
    pub fn set_bootstrapped(&self) {
        self.bootstrapped.store(true, Ordering::Relaxed);
    }

    #[must_use]
    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped.load(Ordering::Relaxed)
    }
}

/// A cached async eldership result: `None` while not yet queried, mirroring
/// `QspnHandle::my_eldership`/`eldership`'s own `Option<Option<u32>>` shape (`Some(None)` is a
/// real virtual/null claim, `Some(Some(n))` a real claim `n`).
type EldershipSlot = Option<Option<u32>>;

/// Bridges `QspnHandle`'s async `my_eldership`/`eldership` to `QspnView`'s sync surface — see
/// the module doc's "QSPN eldership" section for the mapping. `None` (not yet queried) reads
/// the same as the qspn-reported "unknown" case.
#[derive(Debug, Default)]
struct EldershipCache {
    my: Mutex<HashMap<usize, EldershipSlot>>,
    foreign: Mutex<HashMap<(usize, u32), EldershipSlot>>,
}

/// `Ok(Some(Some(n)))` -> `n`; `Ok(Some(None))` (virtual/null claim) -> `-1`; anything else
/// (`Ok(None)`, an actor error, or not cached yet) -> `i32::MAX` ("unknown").
fn map_eldership(v: Option<Option<u32>>) -> i32 {
    match v {
        Some(Some(n)) => i32::try_from(n).unwrap_or(i32::MAX - 1),
        Some(None) => -1,
        None => i32::MAX,
    }
}

impl EldershipCache {
    fn my(&self, level: usize) -> i32 {
        map_eldership(
            self.my
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&level)
                .copied()
                .flatten(),
        )
    }

    fn foreign(&self, level: usize, pos: u32) -> i32 {
        map_eldership(
            self.foreign
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&(level, pos))
                .copied()
                .flatten(),
        )
    }

    fn set_my(&self, level: usize, value: Option<Option<u32>>) {
        self.my
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(level, value);
    }

    fn set_foreign(&self, level: usize, pos: u32, value: Option<Option<u32>>) {
        self.foreign
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((level, pos), value);
    }
}

/// Refreshes every level's own eldership plus every currently-known destination's, then repeats
/// on every subsequent [`ntk_qspn::QspnEvent`] until `cancel` fires.
async fn run_eldership_cache(
    qspn: QspnHandle,
    cache: Arc<EldershipCache>,
    cancel: tokio_util::sync::CancellationToken,
) {
    let levels = qspn.my_naddr().topology().levels();
    let mut events = qspn.subscribe_events();
    loop {
        for level in 0..levels {
            cache.set_my(level, qspn.my_eldership(level).await.unwrap_or(None));
        }
        for (level, entries) in qspn.snapshot().levels.iter().enumerate() {
            for entry in entries {
                let pos = entry.destination.pos;
                cache.set_foreign(level, pos, qspn.eldership(level, pos).await.unwrap_or(None));
            }
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            event = events.recv() => {
                if event.is_err() && matches!(event, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
                    return;
                }
            }
        }
    }
}

/// Estimate of the total node count reachable at/above `level`: this node's own already-
/// aggregated `level` population plus every sibling destination's reported `nodes_inside` at
/// `level`. Real data (`RoutePath::nodes_inside`), not fabricated — see
/// [`ntk_hooking::QspnView::n_nodes`]'s and [`ntk_coordinator::CoordinatorMap::n_nodes`]'s doc
/// comments, neither of which `ntk-qspn` computes directly.
///
/// **The bug this recursion fixes**: a level-0 destination is exactly one real node, but a
/// destination at any higher level is itself a whole sibling g-node, and "myself" at that same
/// higher level is, symmetrically, my *own* whole g-node — not one node either. An earlier
/// version hardcoded `+ 1` for "myself" at every level, correct only for a single-level
/// topology (where level 0 *is* the top level). Under a real multi-level topology
/// (`crates/ntkd/tests/mesh.rs`'s `UNIT_MERGE_GSIZES = [4, 2]`) this silently froze a node's own
/// reported size at `1` forever, however many further members its own g-node had already
/// absorbed via lower-level migrations — those siblings are level-0 destinations, invisible to
/// a query at the top level, whose only contribution was ever meant to flow through "myself"'s
/// own recursively-aggregated count. Confirmed live: real-kernel
/// `two_level_gnode_migrates_as_a_unit_into_merged_network` instrumentation showed a node with
/// two already-migrated-in siblings still reporting `my_n_nodes=Some(1)` on every ask, forever
/// declining or mis-tiebreaking merges its real size should have decided differently. The fix
/// mirrors `ntk_qspn::state::update_clusters`'s own bottom-up formula
/// (`my_nodes_inside[i] = my_nodes_inside[child_level] + Σ destinations[child_level]`, level 0
/// hardcoded to 1 both there and here) — recursing down to level 0 instead of assuming "myself"
/// is always a single node.
///
/// Excludes any destination [`NetworkInfo::is_foreign`] currently flags, at *every* recursed
/// level, not only the one first asked about: `ntk_qspn` maps a reachable arc's peer into its
/// own destination set the instant the arc exists, with no notion of network boundaries, and a
/// foreign arc can surface at any level, so a not-yet-merged foreign neighbor would otherwise
/// inflate this identity's own reported size — feeding a false, unstable value straight into
/// `ntk_hooking::merge::merge_tiebreak` (reproduced by this daemon's own concurrent-merge test
/// coverage: both sides of a tied merge transiently over-counting themselves via the other's
/// still-foreign members, breaking the tiebreak's antisymmetry).
fn estimate_n_nodes(snapshot: &RouteSnapshot, net: Option<&NetworkInfo>, level: usize) -> u64 {
    let mut contributions: Vec<(u32, u32)> = Vec::new();
    let mut foreign_skipped: Vec<(u32, u32)> = Vec::new();
    let mut counted: u64 = 0;
    for entry in snapshot.levels.get(level).into_iter().flatten() {
        let is_foreign = match net {
            Some(n) => n.is_foreign(entry.destination.level, entry.destination.pos),
            None => false,
        };
        let Some(path) = entry.paths.first() else {
            continue;
        };
        if is_foreign {
            foreign_skipped.push((entry.destination.pos, path.nodes_inside));
            continue;
        }
        contributions.push((entry.destination.pos, path.nodes_inside));
        // A level-0 destination is by definition exactly one real node — hardcoded rather than
        // trusted, matching `update_clusters`'s own asymmetry (every node's `nodes_inside[0]`
        // is always `1` anyway, so this changes nothing for a well-behaved peer).
        counted += if level == 0 {
            1
        } else {
            u64::from(path.nodes_inside)
        };
    }
    // "Myself" at `level` is my own already-aggregated `level - 1` population, recursively —
    // not a flat `1` except at the level-0 base case.
    let mine = if level == 0 {
        1
    } else {
        estimate_n_nodes(snapshot, net, level - 1)
    };
    let total = counted + mine;
    tracing::debug!(
        level,
        ?contributions,
        ?foreign_skipped,
        mine,
        total,
        "migration-instrumentation: estimate_n_nodes"
    );
    total
}

/// The best local arc toward `hc`: the first (cheapest) admitted [`ntk_qspn::RoutePath`]'s own
/// `arc` field, resolved through the [`LinkRegistry`] to the [`LinkId`] whose connection carries
/// it — see the module doc's "Routing model".
fn first_hop_link(
    snapshot: &RouteSnapshot,
    registry: &LinkRegistry,
    hc: HCoord,
    skip: Option<QspnArcId>,
) -> Option<LinkId> {
    let entry = snapshot
        .levels
        .get(hc.level)?
        .iter()
        .find(|e| e.destination == hc)?;
    entry
        .paths
        .iter()
        .find(|p| Some(p.arc) != skip)
        .and_then(|p| registry.link_of_qspn_arc(p.arc))
}

// ---------------------------------------------------------------------------
// ntk_hooking::QspnView
// ---------------------------------------------------------------------------

/// Implements [`ntk_hooking::QspnView`] over the real [`QspnHandle`].
#[derive(Debug)]
pub struct QspnViewAdapter {
    pub qspn: QspnHandle,
    pub net: Arc<NetworkInfo>,
    /// Per-level migration search radius (`hooking_epsilon`,
    /// `research/impl/vala/ntkd/configuration.vala:49-63`): smallest count of levels whose
    /// cumulative bit-width reaches 5 bits, computed once from the topology at construction.
    pub epsilon: usize,
    eldership: Arc<EldershipCache>,
}

impl QspnViewAdapter {
    /// Builds the adapter and spawns its background eldership-cache refresher (see the module
    /// doc's "QSPN eldership" section) as a child of `cancel`.
    #[must_use]
    pub fn spawn(
        qspn: QspnHandle,
        net: Arc<NetworkInfo>,
        tasks: &mut tokio::task::JoinSet<()>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        let epsilon = hooking_epsilon(qspn.my_naddr().topology());
        let eldership = Arc::new(EldershipCache::default());
        tasks.spawn(run_eldership_cache(qspn.clone(), eldership.clone(), cancel));
        Self {
            qspn,
            net,
            epsilon,
            eldership,
        }
    }
}

/// `hooking_epsilon` (`research/impl/vala/ntkd/configuration.vala:49-63`): the smallest number
/// of levels whose cumulative `ceil(log2(gsize))` bit width reaches 5 bits.
fn hooking_epsilon(topology: &Topology) -> usize {
    let mut bits = 0u32;
    for (count, gsize) in topology.gsizes().iter().enumerate() {
        bits += 32 - gsize.saturating_sub(1).leading_zeros();
        if bits >= 5 {
            return count + 1;
        }
    }
    topology.levels()
}

impl ntk_hooking::QspnView for QspnViewAdapter {
    fn topology(&self) -> &Topology {
        self.qspn.my_naddr().topology()
    }

    fn network_id(&self) -> i64 {
        self.net.network_id()
    }

    fn n_nodes(&self) -> u64 {
        estimate_n_nodes(
            &self.qspn.snapshot(),
            Some(&self.net),
            self.topology().levels().saturating_sub(1),
        )
    }

    fn my_pos(&self, level: usize) -> u32 {
        self.qspn.my_naddr().pos(level).unwrap_or(0)
    }

    fn my_eldership(&self, level: usize) -> i32 {
        self.eldership.my(level)
    }

    /// No subnet-boundary/NAT feature exists in this daemon (batch contract: `subnetlevel`
    /// deliberately out of scope) — `0` is the correct value for "no subnetting", not a stub.
    fn subnetlevel(&self) -> usize {
        0
    }

    fn epsilon(&self, _level: usize) -> usize {
        self.epsilon
    }

    fn eldership(&self, level: usize, pos: u32) -> i32 {
        self.eldership.foreign(level, pos)
    }

    fn adjacent_to_my_gnode(
        &self,
        level_adjacent_gnodes: usize,
        level_my_gnode: usize,
    ) -> Vec<ntk_hooking::AdjacentGNode> {
        let snapshot = self.qspn.snapshot();
        let my_naddr = self.qspn.my_naddr();
        let Some(entries) = snapshot.levels.get(level_adjacent_gnodes) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter_map(|entry| {
                let hc = entry.destination;
                if my_naddr.is_inside(hc).unwrap_or(false) {
                    return None;
                }
                let border_real_pos = my_naddr.pos(level_my_gnode)?;
                Some(ntk_hooking::AdjacentGNode {
                    hc,
                    border_real_pos,
                })
            })
            .collect()
    }

    fn is_bootstrapped(&self) -> bool {
        // Latched by `crate::node::lifecycle` on the first observed
        // `QspnEvent::BootstrapComplete` — see `NetworkInfo::set_bootstrapped`.
        self.net.is_bootstrapped()
    }

    fn note_foreign(&self, level: usize, pos: u32) {
        self.net.note_foreign(level, pos);
    }

    fn note_same_network(&self, level: usize, pos: u32) {
        self.net.note_same_network(level, pos);
    }
}

// ---------------------------------------------------------------------------
// ntk_hooking::CoordinatorClient (the asker)
// ---------------------------------------------------------------------------

/// Implements [`ntk_hooking::CoordinatorClient`]: DHT round trips
/// ([`ntk_coordinator::CoordinatorClient`]) for `evaluate_enter`/`begin_enter`/
/// `completed_enter`/`abort_enter`/`reserve`/`delete_reserve`/`n_nodes`, and local-instance
/// propagation fanout ([`ntk_coordinator::Handle`]) for `prepare_migration`/
/// `finish_migration`/`prepare_enter`/`finish_enter` — see `ntk_hooking::coordinator`'s own
/// module doc for why these are two different transports, not one.
#[derive(Debug)]
pub struct CoordinatorClientAdapter {
    pub dht: ntk_coordinator::CoordinatorClient,
    pub local: ntk_coordinator::Handle,
    /// This identity's own qspn handle and network-scoped facts — used only to compute
    /// `Self::foreign_exclusions` before every `self.dht` DHT round trip. See
    /// [`ntk_coordinator::CoordinatorClient::reserve`]'s own doc for why this is required, not
    /// an optional hardening: without it, `target_for`'s elect-key can resolve to a physically
    /// reachable but logically foreign node.
    pub qspn: QspnHandle,
    pub net: Arc<NetworkInfo>,
    /// Local shortcut for [`Self::decide_merge`]'s own recently-decided verdicts, keyed by
    /// `neighbor_network_id` — avoids a Coordinator round trip for every ask while a verdict
    /// is still within [`Self::merge_decision_ttl`] (from asking itself or from the shared
    /// [`ntk_coordinator::CoordinatorClient::hooking_memory`] the elected Coordinator holds).
    /// Never trusted past that TTL — see [`Self::decide_merge`]'s own doc for why a verdict
    /// with no expiry at all was the actual defect, not merely an optimization detail.
    merge_decisions: Mutex<HashMap<i64, (bool, Instant)>>,
    /// How long a persisted `decide_merge` verdict (local or shared) may be trusted before it
    /// must be recomputed against live inputs. Reuses
    /// `crate::node::services::coordinator_config`'s own `n_nodes_cache_ttl` at this daemon's
    /// one call site — both bound the identical question ("is this size-based judgment still
    /// fresh enough to trust"), so tying them to the same duration is deliberate.
    merge_decision_ttl: Duration,
}

impl CoordinatorClientAdapter {
    #[must_use]
    pub fn new(
        dht: ntk_coordinator::CoordinatorClient,
        local: ntk_coordinator::Handle,
        qspn: QspnHandle,
        net: Arc<NetworkInfo>,
        merge_decision_ttl: Duration,
    ) -> Self {
        Self {
            dht,
            local,
            qspn,
            net,
            merge_decisions: Mutex::new(HashMap::new()),
            merge_decision_ttl,
        }
    }

    /// See [`Self::qspn`]'s/[`Self::net`]'s own doc, and
    /// [`ntk_coordinator::CoordinatorClient::reserve`]'s for why every DHT round trip below
    /// passes this.
    fn foreign_exclusions(&self) -> Vec<ntk_peerservices::TupleGNode> {
        foreign_exclusions(&self.qspn, &self.net)
    }
}

/// [`CoordinatorClientAdapter::foreign_exclusions`]'s own logic, factored out so
/// [`EnterArbiter`]'s replicated-record round trips (`self.dht.hooking_memory`/
/// `set_hooking_memory` at `top = levels()`, exactly [`CoordinatorClientAdapter::decide_merge`]'s
/// own target) can seed the identical exclusion list without a second identity's worth of
/// `qspn`/`net` fields.
fn foreign_exclusions(qspn: &QspnHandle, net: &NetworkInfo) -> Vec<ntk_peerservices::TupleGNode> {
    let topology = qspn.my_naddr().topology().clone();
    net.foreign_positions()
        .into_iter()
        .filter_map(|(level, pos)| {
            ntk_peerservices::TupleGNode::new(topology.clone(), level + 1, vec![pos]).ok()
        })
        .collect()
}

fn proxy_err(e: ntk_coordinator::ProxyError) -> CoordinatorError {
    CoordinatorError::Unreachable(e.to_string())
}

impl HookingCoordinatorClient for CoordinatorClientAdapter {
    fn n_nodes(&self) -> BoxFuture<'_, u64> {
        Box::pin(async move {
            self.dht
                .get_n_nodes(&self.foreign_exclusions())
                .await
                .unwrap_or(1)
        })
    }

    /// `api.vala:63`'s own comment on `ICoordinator.evaluate_enter` is explicit: "This is going
    /// to be proxied to the coordinator of **the whole network**: lvl=levels" —
    /// `hooking_helpers.vala:245` (`identity_data.coord_mgr.evaluate_enter(levels, ...)`) always
    /// targets `CoordinatorKey(levels)`, never a level derived from the request payload itself
    /// (`req.min_lvl` only ever travels *inside* `data`, consulted by the election algorithm on
    /// the servant side — [`ntk_coordinator::EvaluateEnterHandler::evaluate_enter`] — not by DHT routing here).
    /// A prior version of this method mistakenly used `req.min_lvl + 1` as the DHT target: for
    /// this daemon's own `crate::node::adapters::QspnViewAdapter::subnetlevel` (always `0`,
    /// "no subnetting") that coincidentally equalled `levels` only for a single-level topology,
    /// masking the bug until a `gsizes = [8]` two-node merge test exposed it.
    fn evaluate_enter(
        &self,
        req: EvaluateEnterRequest,
    ) -> BoxFuture<'_, Result<usize, CoordinatorError>> {
        Box::pin(async move {
            let top = self.local.topology().levels();
            let data = codec::encode_evaluate_enter_request(&req);
            let reply = self
                .dht
                .evaluate_enter(top, data)
                .await
                .map_err(proxy_err)?;
            let decoded = codec::decode_evaluate_enter_answer(&reply);
            tracing::info!(
                network_id = req.network_id,
                evaluate_enter_id = req.evaluate_enter_id,
                min_lvl = req.min_lvl,
                ?decoded,
                "hooking client: evaluate_enter round trip"
            );
            match decoded {
                Ok(codec::EvaluateEnterAnswer::Accepted { chosen_lvl }) => Ok(chosen_lvl),
                Ok(codec::EvaluateEnterAnswer::AskAgain) => Err(CoordinatorError::AskAgain),
                Ok(codec::EvaluateEnterAnswer::IgnoreNetwork) => {
                    Err(CoordinatorError::IgnoreNetwork)
                }
                Err(e) => Err(CoordinatorError::Unreachable(e.to_string())),
            }
        })
    }

    /// `begin_enter`/`completed_enter`/`abort_enter` route to `CoordinatorKey(lvl)` directly in
    /// upstream (`peer_service.vala:218,243,268,293`: `new CoordinatorKey(lvl)`, no offset) —
    /// unlike [`Self::reserve`], `lvl` here can legitimately be `0` (`arc_handler.vala:303`'s own
    /// "network is full at level 0" branch), which upstream represents as `CoordinatorKey(0)`,
    /// `perfect_tuple` = **zero** zeros = the empty tuple `ntk_peerservices::Handle::contact_peer`
    /// already documents as "route to myself" (`tuple::approximate`'s `valid_levels == 0`
    /// branch). `ntk_coordinator::CoordinatorClient::target_for` has no way to express that
    /// degenerate target (`top` must be `1..=levels`), so this keeps `lvl + 1` here — routing to
    /// `CoordinatorKey(1)` ("coordinator of my own level-0 g-node") instead of literally myself.
    /// This is not merely a tolerated approximation: `lvl == 0` is reachable exactly when
    /// [`ntk_coordinator::EvaluateEnterHandler::evaluate_enter`] echoed back a `req.min_lvl` of `0`, and
    /// `QspnViewAdapter::subnetlevel` is unconditionally `0` in this daemon (see its own doc), so
    /// every reachable `lvl == 0` call already means "no other member exists anywhere in my own
    /// hierarchy" — `CoordinatorKey(1)`'s DHT lookup falls back to exactly the same node (myself,
    /// `tuple::approximate`'s own unconditional "me" fallback) that `CoordinatorKey(0)` would
    /// have targeted directly, so the two are observably identical for every value `lvl` can
    /// actually take here today.
    fn begin_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
        Box::pin(async move {
            self.dht
                .begin_enter(lvl + 1, codec::encode_unit())
                .await
                .map(drop)
                .map_err(proxy_err)
        })
    }

    /// See [`Self::begin_enter`]'s doc comment.
    fn completed_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
        Box::pin(async move {
            self.dht
                .completed_enter(lvl + 1, codec::encode_unit())
                .await
                .map(drop)
                .map_err(proxy_err)
        })
    }

    /// See [`Self::begin_enter`]'s doc comment.
    fn abort_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
        Box::pin(async move {
            self.dht
                .abort_enter(lvl + 1, codec::encode_unit())
                .await
                .map(drop)
                .map_err(proxy_err)
        })
    }

    /// `ICoordinator.reserve(host_lvl, ...)` forwards `host_lvl` straight into
    /// `CoordinatorManager.reserve`/`CoordinatorKey(host_lvl)` in upstream
    /// (`hooking_helpers.vala:319-327`: `identity_data.coord_mgr.reserve(host_lvl, ...)`, no
    /// offset) — `host_lvl` here is already the same 1-indexed "top" `execute_search` computes
    /// via `visiting_gnode.level(levels)` (`hooking.vala:166`/`crate::search::execute_search`),
    /// ranging `1..=levels` by construction (`first_host_lvl = max(lvl + 1, subnetlevel + 1)` is
    /// always `>= 1`). A prior version of this method added a spurious `+ 1`, shifting every
    /// reservation one level too deep — for a single-level topology that made the *very first*
    /// attempt (`top = 2`) exceed `levels` and fail outright with
    /// [`ntk_coordinator::ProxyError::InvalidTop`], which `execute_search` (indistinguishable
    /// from an ordinary "no coordinator at this level" answer) treated as "climb higher" until
    /// exhausting `max_host_lvl` — the exact `NoMigrationPathFound` this method's callers saw.
    fn reserve(
        &self,
        host_lvl: usize,
        reserve_request_id: i32,
    ) -> BoxFuture<'_, Result<HookingReservation, CoordinatorError>> {
        Box::pin(async move {
            let exclude = self.foreign_exclusions();
            let outcome = self
                .dht
                .reserve(host_lvl, i64::from(reserve_request_id), &exclude)
                .await;
            tracing::info!(
                host_lvl,
                reserve_request_id,
                ?exclude,
                ?outcome,
                "coordinator: reserve outcome"
            );
            match outcome {
                Ok(Some(r)) => Ok(HookingReservation {
                    pos: r.new_pos,
                    eldership: i32::try_from(r.new_eldership).unwrap_or(i32::MAX),
                }),
                Ok(None) => Err(CoordinatorError::NoCoordinatorForLevel),
                Err(e) => Err(proxy_err(e)),
            }
        })
    }

    /// See [`Self::reserve`]'s doc comment.
    fn delete_reserve(&self, host_lvl: usize, reserve_request_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = self
                .dht
                .delete_reserve(
                    host_lvl,
                    i64::from(reserve_request_id),
                    &self.foreign_exclusions(),
                )
                .await;
        })
    }

    fn prepare_migration(&self, lvl: usize, migration_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.local
                .prepare_migration(lvl, codec::encode_migration_id(migration_id))
                .await;
        })
    }

    fn finish_migration(&self, lvl: usize, data: FinishMigrationData) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.local
                .finish_migration(lvl, codec::encode_finish_migration_data(&data))
                .await;
        })
    }

    fn prepare_enter(&self, lvl: usize, enter_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tracing::info!(
                lvl,
                enter_id,
                "migration-instrumentation: prepare_enter propagating"
            );
            self.local
                .prepare_enter(lvl, codec::encode_enter_id(enter_id))
                .await;
        })
    }

    /// See [`ntk_hooking::CoordinatorClient::decide_merge`]'s own doc for why this must be
    /// collective rather than each arc handler recomputing [`ntk_hooking::merge_tiebreak`]
    /// locally: a real multi-member merge with that per-arc recomputation produced
    /// `a_rehooked=2 b_rehooked=3` — members of the *same* g-node reaching opposite
    /// conclusions.
    ///
    /// Reuses per [`MergeArbitrationRequest::neighbor_network_id`] for at most
    /// `Self::merge_decision_ttl`, first checking this process's own cache, then the
    /// g-node's elected Coordinator's shared
    /// [`ntk_coordinator::CoordinatorClient::hooking_memory`] at `top = levels()` — the same
    /// DHT target [`Self::n_nodes`] already uses for "the whole network" — so every member,
    /// on every node, asking about the same `neighbor_network_id` within that window is routed
    /// to the identical physical servant and reads back the *same* answer rather than
    /// recomputing its own, even from a differently-timed local sample.
    ///
    /// # Why a verdict cannot be trusted forever
    /// An earlier version of this method memoized every verdict permanently, with no expiry at
    /// either layer. That produced a real six-node merge where three members of one g-node
    /// (whose own size was genuinely growing as the merge progressed) reached three different,
    /// each individually stale, verdicts about the same neighbor network: whichever asked
    /// earliest cached a verdict computed from a smaller `n_nodes` than the ones who asked
    /// later, and — since nothing ever invalidated it — kept following that stale verdict for
    /// the rest of the episode, splitting one g-node's migration into "some members moved, some
    /// never did". [`Self::n_nodes`] is itself already freshly re-fetched (bounded by
    /// `ntk_coordinator::Config::n_nodes_cache_ttl`) on every miss below, but that freshness is
    /// wasted if the *derived* verdict is then remembered forever regardless — bounding this
    /// method's own cache to the same order of TTL is what actually lets a later ask notice a
    /// real size change on either side and recompute, rather than replaying a decision made
    /// from numbers that no longer hold. This is also what lets an arc that aborted an entry
    /// (`crate::arc`'s "target network changed during entry" redo) re-decide from live data on
    /// its next attempt instead of replaying whatever this same key resolved to before the
    /// abort.
    ///
    /// The persist step is a best-effort read-modify-write, not a compare-and-swap — the
    /// shared memory has no atomic update primitive — so two concurrent first-askers for the
    /// same `neighbor_network_id` can each compute (deterministically, from the same
    /// authoritative `n_nodes`) and one write can clobber the other's; both compute the same
    /// verdict in the overwhelmingly common case (no concurrent membership change), and either
    /// write leaves the memory holding *a* valid, freshly-timestamped verdict every later asker
    /// converges on until the TTL next expires.
    fn decide_merge(&self, req: MergeArbitrationRequest) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            let now = Instant::now();
            if let Some(&(cached, decided_at)) = self
                .merge_decisions
                .lock()
                .expect("merge_decisions mutex poisoned")
                .get(&req.neighbor_network_id)
                && now.saturating_duration_since(decided_at) < self.merge_decision_ttl
            {
                tracing::info!(
                    my_network_id = req.my_network_id,
                    neighbor_network_id = req.neighbor_network_id,
                    neighbor_n_nodes = req.neighbor_n_nodes,
                    decision = cached,
                    cached = true,
                    "migration-instrumentation: decide_merge"
                );
                return cached;
            }

            // One read serves both the "someone already decided, and it is still fresh" check
            // and, on a miss/expiry, the base to merge this process's own fresh verdict into
            // before writing back. Pruning every expired entry here (not just this ask's key)
            // keeps the shared record from growing forever with verdicts nobody has asked
            // about in a long time, and guarantees a later ask for any of those keys recomputes
            // rather than resurrecting a decision made from long-gone inputs.
            let top = self.local.topology().levels();
            let now_ms = codec::now_millis();
            let ttl_ms = u64::try_from(self.merge_decision_ttl.as_millis()).unwrap_or(u64::MAX);
            let exclude = self.foreign_exclusions();
            let mut mem = match self.dht.hooking_memory(top, &exclude).await {
                Ok(Some(tv)) => codec::decode_hooking_memory(&tv).unwrap_or_default(),
                _ => codec::HookingMemory::default(),
            };
            mem.merge_decisions
                .retain(|_, &mut (_, decided_at_ms)| now_ms.saturating_sub(decided_at_ms) < ttl_ms);

            let (decision, my_n_nodes) =
                if let Some(&(decision, _)) = mem.merge_decisions.get(&req.neighbor_network_id) {
                    (decision, None)
                } else {
                    let my_n_nodes = self.n_nodes().await;
                    let decision = merge_tiebreak(
                        my_n_nodes,
                        req.neighbor_n_nodes,
                        req.my_network_id,
                        req.neighbor_network_id,
                    );
                    mem.merge_decisions
                        .insert(req.neighbor_network_id, (decision, now_ms));
                    let _ = self
                        .dht
                        .set_hooking_memory(top, Some(codec::encode_hooking_memory(&mem)), &exclude)
                        .await;
                    (decision, Some(my_n_nodes))
                };

            tracing::info!(
                my_network_id = req.my_network_id,
                neighbor_network_id = req.neighbor_network_id,
                neighbor_n_nodes = req.neighbor_n_nodes,
                ?my_n_nodes,
                decision,
                cached = false,
                "migration-instrumentation: decide_merge"
            );
            self.merge_decisions
                .lock()
                .expect("merge_decisions mutex poisoned")
                .insert(req.neighbor_network_id, (decision, now));
            decision
        })
    }

    fn finish_enter(&self, lvl: usize, data: FinishEnterData) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tracing::info!(
                lvl,
                enter_id = data.enter_id,
                entry_network_id = data.entry_data.network_id,
                entry_pos = ?data.entry_data.pos,
                "migration-instrumentation: finish_enter propagating"
            );
            self.local
                .finish_enter(lvl, codec::encode_finish_enter_data(&data))
                .await;
        })
    }
}

// ---------------------------------------------------------------------------
// ntk_coordinator's server-side traits (the answerer) — delegate into Hooking
// ---------------------------------------------------------------------------

/// Implements [`ntk_coordinator::CoordinatorMap`] over the real [`QspnHandle`] plus
/// [`NetworkInfo`] — see the module doc's "Fixed: `fp_id`" section for why [`ntk_coordinator::CoordinatorMap::fp_id`]
/// returns [`NetworkInfo::network_id`] rather than a per-g-node fingerprint.
#[derive(Debug)]
pub struct CoordinatorMapAdapter {
    pub qspn: QspnHandle,
    pub net: Arc<NetworkInfo>,
}

impl ntk_coordinator::CoordinatorMap for CoordinatorMapAdapter {
    fn n_nodes(&self) -> u64 {
        estimate_n_nodes(
            &self.qspn.snapshot(),
            Some(&self.net),
            self.qspn.my_naddr().topology().levels().saturating_sub(1),
        )
    }

    fn free_positions(&self, level: usize) -> Vec<u32> {
        let topology = self.qspn.my_naddr().topology();
        let Some(gsize) = topology.gsize(level) else {
            return Vec::new();
        };
        let snapshot = self.qspn.snapshot();
        let my_pos = self.qspn.my_naddr().pos(level).unwrap_or(0);
        let raw_destinations: Vec<(usize, u32, bool)> = snapshot
            .levels
            .get(level)
            .into_iter()
            .flatten()
            .map(|e| {
                (
                    e.destination.level,
                    e.destination.pos,
                    self.net.is_foreign(e.destination.level, e.destination.pos),
                )
            })
            .collect();
        let mut occupied: std::collections::HashSet<u32> = snapshot
            .levels
            .get(level)
            .into_iter()
            .flatten()
            .filter(|e| !self.net.is_foreign(e.destination.level, e.destination.pos))
            .map(|e| e.destination.pos)
            .collect();
        occupied.insert(my_pos);
        let free: Vec<u32> = (0..gsize).filter(|p| !occupied.contains(p)).collect();
        tracing::info!(
            level,
            gsize,
            my_pos,
            network_id = self.net.network_id(),
            ?raw_destinations,
            ?occupied,
            ?free,
            "coordinator: free_positions"
        );
        free
    }

    fn can_reserve(&self, level: usize) -> bool {
        !self.free_positions(level).is_empty()
    }

    fn my_pos(&self, level: usize) -> u32 {
        self.qspn.my_naddr().pos(level).unwrap_or(0)
    }

    /// See the module doc's "Fixed: `fp_id`" section: this identity's `network_id` is stable
    /// and shared by every real sibling unconditionally, unlike a per-level fingerprint that
    /// must reconverge — `level` carries no separate value here, position already disambiguates
    /// which g-node within a shared network a propagation names.
    fn fp_id(&self, _level: usize) -> i64 {
        self.net.network_id()
    }
}

/// This daemon's own arbitration for the network-wide "which arc handler proceeds first"
/// election (`ntk_coordinator::EvaluateEnterHandler` and friends) — genuinely absent from any
/// ported crate (`ntk_hooking::coordinator`'s own doc: the server-side election machinery
/// belongs wherever the daemon wires it, not to either crate).
///
/// # The bug this fixes: a second entrant landing in a fresh, sibling slot
/// A prior version cleared its whole per-level record in *both* `completed_enter` and
/// `abort_enter`. Upstream keeps these as two genuinely separate pieces of memory
/// (`research/impl/vala/hooking/proxy_coord.vala`): `execute_completed_enter`/
/// `execute_abort_enter` (`:412-420`, `:444-452`) touch only `begin_enter_timeout` — a
/// re-entrancy guard for `begin_enter` itself — and *never* the election state
/// (`evaluate_enter_status`/`evaluate_enter_evaluation_list`/`evaluate_enter_elected`) that
/// `execute_evaluate_enter` (`:88-340`) owns entirely on its own. That election state is what
/// upstream uses to grant at most **one** real placement per network-wide entry: every other
/// concurrent or later ask for the *same* `network_id` is answered `IgnoreNetworkError`
/// (`:314-337`, the `NOTIFIED` case) rather than a fresh, independent `Accepted`. Clearing the
/// Rust port's equivalent slot the instant the elected member called `completed_enter` — well
/// before its own `finish_enter` propagation could reach anyone, including this very servant if
/// it is itself a g-node member — reopened that slot immediately: a second member of the same
/// still-migrating g-node, asking moments later, was granted its own independent `Accepted` at
/// the identical level and went on to `reserve` its own, different real position on the target
/// — the concurrent-placement defect this struct now closes.
///
/// # Design
/// The first ask at a free level is granted `Accepted` immediately and remembered as
/// `elected` — kept alive across `completed_enter` (which no longer touches it at all, matching
/// upstream) and released only by `abort_enter` (the elected candidate itself giving up) or
/// after [`ELECTED_TTL`] with no `abort_enter`, a bounded self-heal for the case propagation
/// never lands. A different id asking about the *same* `network_id` while `elected` is live is
/// refused with `IgnoreNetwork` — upstream's `NOTIFIED` outcome — instead of independently
/// accepted; a different `network_id` is told to ask again later rather than starting a second,
/// concurrent election at the same level.
#[derive(Debug, Default)]
struct LevelState {
    /// `(network_id, evaluate_enter_id, granted_at)` of this level's currently-granted
    /// election, if any, as *this process* last observed it — either because it granted the
    /// election itself, or because it adopted the Coordinator-replicated record (see
    /// [`EnterArbiter::decide`]'s own doc) on an earlier ask. `None` means this process has no
    /// opinion yet: the next ask must consult the replicated record before granting anything.
    elected: Option<(i64, i32, Instant)>,
}

/// How long a granted election is trusted before a fresh one may start for the same
/// `(level, network_id)` — bounds the same question `booking_ttl` bounds at the *target*'s own
/// Coordinator (`ntk_coordinator::Config::default().booking_ttl`, 60s): if the elected
/// candidate's whole episode (through `finish_enter` propagation reaching every sibling)
/// overruns this, the target's own reservation will have already expired too, so nothing is
/// lost by letting a fresh election start here as well. Also bounds how long the
/// Coordinator-*replicated* record (see [`EnterArbiter`]'s own doc) can block a fresh election
/// if the member that granted it dies mid-episode: nothing runs a `Drop` on a crashed process,
/// but every reader compares `granted_at_millis` against this same bound, so any surviving
/// replica (or the same member, restarted) treats the slot as free once it elapses — the
/// abandoned-election reclamation story this struct's own doc promises.
const ELECTED_TTL: Duration = Duration::from_millis(60_000);

/// This daemon's own arbitration for the network-wide "which arc handler proceeds first"
/// election (`ntk_coordinator::EvaluateEnterHandler` and friends) — genuinely absent from any
/// ported crate (`ntk_hooking::coordinator`'s own doc: the server-side election machinery
/// belongs wherever the daemon wires it, not to either crate).
///
/// # The bug this fixes: a second entrant landing in a fresh, sibling slot
/// A prior version cleared its whole per-level record in *both* `completed_enter` and
/// `abort_enter`. Upstream keeps these as two genuinely separate pieces of memory
/// (`research/impl/vala/hooking/proxy_coord.vala`): `execute_completed_enter`/
/// `execute_abort_enter` (`:412-420`, `:444-452`) touch only `begin_enter_timeout` — a
/// re-entrancy guard for `begin_enter` itself — and *never* the election state
/// (`evaluate_enter_status`/`evaluate_enter_evaluation_list`/`evaluate_enter_elected`) that
/// `execute_evaluate_enter` (`:88-340`) owns entirely on its own. That election state is what
/// upstream uses to grant at most **one** real placement per network-wide entry: every other
/// concurrent or later ask for the *same* `network_id` is answered `IgnoreNetworkError`
/// (`:314-337`, the `NOTIFIED` case) rather than a fresh, independent `Accepted`.
///
/// # The second bug this fixes: the election was never shared across the target g-node's own
/// # members
/// The above kept exactly one election alive *per process*. It was never replicated across the
/// target g-node's own members, each of which runs its own instance of this struct. Different
/// callers' `contact_peer` resolutions can legitimately land on different physical members —
/// ordinary eventual-consistency skew during an active merge, not a routing bug (this crate's
/// module doc) — so two members could each independently run a full successful
/// `evaluate_enter -> Accepted -> reserve -> finish_enter` episode for the *same* merge,
/// splitting the entering group between them (confirmed live: two members `t=51.163s` and
/// `t=56.251s` apart, ~5 seconds, each fanning `finish_enter` out to a different subset of the
/// same entering trio). `Self::decide` now consults and persists into the Coordinator's own
/// replicated `hooking_memory` record (`CoordGnodeMemory.hooking_memory`,
/// `research/impl/vala/coordinator/serializables.vala:182-201`) — the same replicated store
/// `reserve_list` already lives in, reached through the exact same `hooking_memory`/
/// `set_hooking_memory` round trip [`CoordinatorClientAdapter::decide_merge`] already uses for
/// its own merge verdicts (`top = levels()`) — mirroring upstream's own `HookingMemory.
/// evaluate_enter_status`/`evaluate_enter_elected` fields living inside that same opaque
/// per-network memory (`research/impl/vala/hooking/serializables.vala:301-323`,
/// `research/impl/vala/hooking/proxy_coord.vala:88-340`'s state machine). A second member that
/// finds an election already recorded there defers to it — `IgnoreNetwork` for another asker
/// of the *same* `network_id` (upstream's own `NOTIFIED` outcome), `AskAgain` for a genuinely
/// different one — and forwards nothing further itself, exactly like upstream's own
/// already-`NOTIFIED` branch: the elected candidate's own `finish_enter` propagation (unchanged
/// by this fix) is what actually reaches every member of the *entering* g-node, this struct
/// only ever decided who that elected candidate is.
///
/// # Design
/// The first ask at a free level is granted `Accepted` immediately: checked first against this
/// process's own local memory (no network round trip — the common case of the same candidate
/// re-asking, or a second local caller for a level this process itself already decided), then,
/// on a local miss, against the Coordinator's replicated record before granting anything new.
/// Kept alive across `completed_enter` (which no longer touches it at all, matching upstream)
/// and released — both locally and in the replicated record — only by `abort_enter` (the
/// elected candidate itself giving up) or after `ELECTED_TTL` with no `abort_enter`. A
/// different id asking about the *same* `network_id` while `elected` is live is refused with
/// `IgnoreNetwork`; a different `network_id` is told to ask again later rather than starting a
/// second, concurrent election at the same level.
///
/// The replicated write is a best-effort read-modify-write, not a compare-and-swap — this
/// memory has no such primitive (see [`CoordinatorClientAdapter::decide_merge`]'s own doc) — so
/// two genuinely concurrent first-askers landing on two different physical members can still
/// each compute `Accepted` and one write can clobber the other's. That window is bounded by one
/// DHT round trip, not by the whole multi-second enter episode the reported bug reproduced: the
/// local fast path above means every *later* ask on either process (including the very next ask
/// on the process that lost the race) converges on whichever write landed last.
#[derive(Debug, Default)]
pub struct EnterArbiter {
    /// One lock for every level, not one per level — mirrors upstream's own single
    /// `_lock_hooking_memory` (`research/impl/vala/hooking/proxy_coord.vala:41,301-304`), which
    /// brackets every `get_hooking_memory`/mutate/`set_hooking_memory` sequence regardless of
    /// which level it concerns. A `tokio::sync::Mutex`, not `std::sync::Mutex`, and held across
    /// the network round trip below by design: two concurrent *local* asks (even for different
    /// levels) must not each observe the replicated record as free and both grant an election
    /// before either writes back.
    levels: tokio::sync::Mutex<HashMap<usize, LevelState>>,
}

impl EnterArbiter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs one `evaluate_enter` ask against `level`'s own state — see this struct's own doc.
    /// `service`/`top` reach *this node's own* local `hooking_memory` record directly —
    /// [`ntk_coordinator::CoordinatorService::hooking_memory_locally`]/
    /// `set_hooking_memory_locally`, not a DHT round trip: this method only ever runs inside
    /// the `EvaluateEnterRequest` handler for `CoordinatorKey(top)`, i.e. this node is *already*
    /// that key's resolved servant for whichever caller reached it — exactly like
    /// `reserve`/`get_n_nodes`'s own handlers reading `self.handle` directly, never re-routing
    /// through `contact_peer` a second time. See [`EnterHandlersAdapter::evaluate_enter`]'s call
    /// site for `top`.
    async fn decide(
        &self,
        service: &ntk_coordinator::CoordinatorService,
        top: usize,
        level: usize,
        network_id: i64,
        evaluate_enter_id: i32,
    ) -> codec::EvaluateEnterAnswer {
        let mut levels = self.levels.lock().await;
        let st = levels.entry(level).or_default();
        if let Some((_, _, granted_at)) = st.elected
            && granted_at.elapsed() >= ELECTED_TTL
        {
            st.elected = None;
        }
        // Fast, purely local path: no round trip at all for the overwhelmingly common case of
        // the same candidate re-asking, or a second local caller for a level this process
        // itself already decided (either by granting it or by adopting the shared record
        // below on an earlier ask).
        match st.elected {
            Some((elected_network, _, _)) if elected_network != network_id => {
                return codec::EvaluateEnterAnswer::AskAgain;
            }
            Some((_, elected_id, _)) if elected_id == evaluate_enter_id => {
                return codec::EvaluateEnterAnswer::Accepted { chosen_lvl: level };
            }
            Some(_) => return codec::EvaluateEnterAnswer::IgnoreNetwork,
            None => {}
        }

        // No local record: a *different* physical member of this same g-node may already have
        // decided (see this struct's own doc, "Cross-process" section) — consult the
        // Coordinator's own replicated record before granting anything new. Still a network
        // round trip in effect (the record was written by, and replicated from, whichever
        // member actually decided first), but a *passive read* of this node's own already-
        // replicated copy, not a fresh `contact_peer` call — replication delivers it here on
        // its own schedule, the same way it already delivers `reserve_list` updates.
        let now_ms = codec::now_millis();
        let ttl_ms = u64::try_from(ELECTED_TTL.as_millis()).unwrap_or(u64::MAX);
        let mut mem = match service.hooking_memory_locally(top).await {
            Some(tv) => codec::decode_hooking_memory(&tv).unwrap_or_default(),
            None => codec::HookingMemory::default(),
        };
        let shared = mem
            .elections
            .get(&level)
            .filter(|e| now_ms.saturating_sub(e.granted_at_millis) < ttl_ms)
            .copied();

        let answer = match shared {
            Some(e) if e.network_id != network_id => codec::EvaluateEnterAnswer::AskAgain,
            Some(e) if e.evaluate_enter_id == evaluate_enter_id => {
                codec::EvaluateEnterAnswer::Accepted { chosen_lvl: level }
            }
            Some(_) => codec::EvaluateEnterAnswer::IgnoreNetwork,
            None => {
                mem.elections.insert(
                    level,
                    codec::ElectionRecord {
                        network_id,
                        evaluate_enter_id,
                        granted_at_millis: now_ms,
                    },
                );
                service
                    .set_hooking_memory_locally(top, Some(codec::encode_hooking_memory(&mem)))
                    .await;
                codec::EvaluateEnterAnswer::Accepted { chosen_lvl: level }
            }
        };

        if matches!(answer, codec::EvaluateEnterAnswer::Accepted { .. }) {
            st.elected = Some((network_id, evaluate_enter_id, Instant::now()));
        }
        answer
    }

    /// The elected candidate at `level` gave up (no migration path, target changed under it, or
    /// a sibling's own propagation already carried the g-node there) — release the lease
    /// immediately, both locally and in the Coordinator's own replicated record (which then
    /// fans the clear out to other replicas on its own schedule, same as [`Self::decide`]'s own
    /// write), instead of waiting out [`ELECTED_TTL`].
    async fn release(
        &self,
        service: &ntk_coordinator::CoordinatorService,
        top: usize,
        level: usize,
    ) {
        {
            let mut levels = self.levels.lock().await;
            levels.entry(level).or_default().elected = None;
        }
        if let Some(tv) = service.hooking_memory_locally(top).await
            && let Ok(mut mem) = codec::decode_hooking_memory(&tv)
            && mem.elections.remove(&level).is_some()
        {
            service
                .set_hooking_memory_locally(top, Some(codec::encode_hooking_memory(&mem)))
                .await;
        }
    }
}

/// Implements every `ntk_coordinator` enter-protocol handler trait by decoding/encoding this
/// daemon's own [`codec`] payloads and running [`EnterArbiter`].
#[derive(Debug)]
pub struct EnterHandlersAdapter {
    pub arbiter: Arc<EnterArbiter>,
    /// This g-node's own live topology/positions — needed to compute `Self::chosen_lvl`
    /// rather than always echoing back the caller's own `min_lvl` (always `0` in this daemon,
    /// `QspnViewAdapter::subnetlevel`'s own doc). A fresh instance is built for every identity
    /// generation (`crate::node::services::spawn`, never carried across a `rehook`), so this
    /// is always the *current* generation's own qspn — never the staleness
    /// [`ntk_hooking::HookingHandle`]'s own carried-across-generations view has.
    pub qspn: ntk_qspn::QspnHandle,
    pub net: Arc<NetworkInfo>,
    /// This identity's own [`ntk_coordinator::CoordinatorService`] — reached directly (never
    /// through `contact_peer`) for [`EnterArbiter`]'s replicated election record; see
    /// `EnterArbiter::decide`'s own doc for why. A `watch` channel because
    /// `CoordinatorService` is constructed *after* [`ntk_coordinator::Manager::new`], which
    /// this adapter is itself a constructor argument of (same cycle
    /// [`PropagationHandlerAdapter::hooking`]'s own doc explains for Hooking/Coordinator).
    pub coordinator_service:
        tokio::sync::watch::Receiver<Option<Arc<ntk_coordinator::CoordinatorService>>>,
}

async fn wait_for_coordinator_service(
    rx: &tokio::sync::watch::Receiver<Option<Arc<ntk_coordinator::CoordinatorService>>>,
) -> Arc<ntk_coordinator::CoordinatorService> {
    let mut rx = rx.clone();
    rx.wait_for(Option::is_some)
        .await
        .expect("coordinator service sender is never dropped before the daemon shuts down")
        .clone()
        .expect("wait_for guarantees Some")
}

impl EnterHandlersAdapter {
    /// `execute_evaluate_enter`'s own "first evaluation" climb
    /// (`research/impl/vala/hooking/proxy_coord.vala:104-121`): starting at level 0 (this
    /// daemon's `QspnViewAdapter::subnetlevel` is unconditionally `0`, "no subnetting"), a real,
    /// already-merged sibling known at level `i` means the whole subtree through level `i` must
    /// move as one unit when this g-node enters a new network — the entry level bumps to at
    /// least `i + 1`. Never climbs to the topology's own top level: entering *the whole
    /// network* is a different, unreachable case here.
    ///
    /// This is the piece that makes the destination of a merge collective rather than each
    /// member independently negotiating its own entry (this crate's module doc, "Coordinated
    /// multi-member migration"): once this returns `>= 1`, [`ntk_hooking::arc::run_arc_handler`]
    /// propagates one shared target to the whole g-node instead of only the negotiating member
    /// adopting it.
    ///
    /// Filters out a not-yet-merged foreign neighbor accidentally visible via qspn before
    /// hooking resolves anything (`self.net.is_foreign`, the same guard
    /// [`CoordinatorMapAdapter::free_positions`] applies) — otherwise a live merge negotiation
    /// in progress at level `i` would make `i`'s own still-foreign peer look like an
    /// already-merged sibling and spuriously bump the level.
    fn chosen_lvl(&self) -> usize {
        chosen_lvl_from_snapshot(
            &self.qspn.snapshot(),
            &self.net,
            self.qspn.my_naddr().topology().levels(),
        )
    }
}

/// [`EnterHandlersAdapter::chosen_lvl`]'s pure logic, split out so it is testable against a
/// crafted [`RouteSnapshot`]/[`NetworkInfo`] without a live [`QspnHandle`] — same split as
/// [`estimate_n_nodes`]'s own doc explains.
fn chosen_lvl_from_snapshot(snapshot: &RouteSnapshot, net: &NetworkInfo, levels: usize) -> usize {
    let mut max_lvl = 0;
    for i in 0..levels.saturating_sub(1) {
        let has_real_sibling = snapshot
            .levels
            .get(i)
            .into_iter()
            .flatten()
            .any(|e| !net.is_foreign(e.destination.level, e.destination.pos));
        if has_real_sibling {
            max_lvl = i + 1;
        }
    }
    max_lvl
}

impl ntk_coordinator::EvaluateEnterHandler for EnterHandlersAdapter {
    /// Keys [`EnterArbiter`] by `Self::chosen_lvl` — the same logical entry level echoed back
    /// as `chosen_lvl` and later passed to `completed_enter`/`abort_enter` as `lvl` (their own
    /// handlers below key by `top.saturating_sub(1)`, and their `top` is always `lvl + 1`, so
    /// their effective key is that same `lvl`) — not the caller's own `req.min_lvl` (always `0`
    /// in this daemon, `QspnViewAdapter::subnetlevel`'s own doc), which would key every g-node's
    /// own entry identically regardless of how many real members it actually has.
    ///
    /// # Bug this fixes: every entry was evaluated as if solitary
    /// Used to echo `req.min_lvl` straight back as `chosen_lvl`, always `0` — so every merge
    /// negotiation, regardless of how many real members its own g-node had, was told to enter
    /// individually rather than as a unit: `Self::chosen_lvl`'s doc names this the actual
    /// mechanism this daemon needed to make a coordinated g-node migration's destination
    /// collective (this crate's module doc, "Coordinated multi-member migration"). Also
    /// resolves a related, previously separate key-mismatch bug: keying by the DHT *routing*
    /// level (always `topology().levels()`, `api.vala:63`'s "proxied to the coordinator of the
    /// whole network") let `evaluate_enter` accept an id under a level that
    /// `completed_enter`/`abort_enter` (keyed by the *negotiated* level) never released for any
    /// topology deeper than one level — `Self::chosen_lvl` and this method now agree on
    /// exactly the same value.
    fn evaluate_enter(
        &self,
        _top: usize,
        data: TypedValue,
        _client_tuple: &[u32],
    ) -> BoxFuture<'_, TypedValue> {
        Box::pin(async move {
            let answer = match codec::decode_evaluate_enter_request(&data) {
                Ok(req) => {
                    let level = self.chosen_lvl();
                    let service = wait_for_coordinator_service(&self.coordinator_service).await;
                    let top = self.qspn.my_naddr().topology().levels();
                    let answer = self
                        .arbiter
                        .decide(&service, top, level, req.network_id, req.evaluate_enter_id)
                        .await;
                    tracing::info!(
                        level,
                        network_id = req.network_id,
                        evaluate_enter_id = req.evaluate_enter_id,
                        ?answer,
                        "coordinator: evaluate_enter"
                    );
                    answer
                }
                Err(_) => codec::EvaluateEnterAnswer::IgnoreNetwork,
            };
            codec::encode_evaluate_enter_answer(answer)
        })
    }
}

impl ntk_coordinator::BeginEnterHandler for EnterHandlersAdapter {
    fn begin_enter(
        &self,
        _top: usize,
        _data: TypedValue,
        _client_tuple: &[u32],
    ) -> BoxFuture<'_, TypedValue> {
        Box::pin(async move { codec::encode_unit() })
    }
}

impl ntk_coordinator::CompletedEnterHandler for EnterHandlersAdapter {
    /// Deliberately does **not** touch [`EnterArbiter`]'s own election state — see that
    /// struct's own doc for why releasing it here (the prior bug) let a second, independent
    /// election proceed before this completed episode's own `finish_enter` propagation had
    /// reached anyone. Upstream's `execute_completed_enter`
    /// (`research/impl/vala/hooking/proxy_coord.vala:412-420`) only ever clears a *different*,
    /// unrelated re-entrancy guard the same way.
    fn completed_enter(
        &self,
        top: usize,
        _data: TypedValue,
        _client_tuple: &[u32],
    ) -> BoxFuture<'_, TypedValue> {
        let level = top.saturating_sub(1);
        Box::pin(async move {
            tracing::info!(level, "coordinator: completed_enter");
            codec::encode_unit()
        })
    }
}

impl ntk_coordinator::AbortEnterHandler for EnterHandlersAdapter {
    fn abort_enter(
        &self,
        top: usize,
        _data: TypedValue,
        _client_tuple: &[u32],
    ) -> BoxFuture<'_, TypedValue> {
        let level = top.saturating_sub(1);
        Box::pin(async move {
            let service = wait_for_coordinator_service(&self.coordinator_service).await;
            let coord_top = self.qspn.my_naddr().topology().levels();
            self.arbiter.release(&service, coord_top, level).await;
            codec::encode_unit()
        })
    }
}

/// Implements [`ntk_coordinator::PropagationHandler`] by decoding this daemon's own payloads
/// and applying them to the local [`HookingHandle`]. `we_have_splitted` has no corresponding
/// `HookingHandle` notification (this daemon's Hooking wiring never triggers it — QSPN's own
/// `GnodeSplitted` event, not a Coordinator propagation, is what this daemon reacts to for
/// splits, see `crate::node::lifecycle`), so it is a documented no-op rather than a fabricated
/// call into an unrelated method.
/// `hooking` is set once, right after `ntk_hooking::spawn` returns — [`ntk_coordinator::Manager::new`]
/// needs this adapter *before* that handle exists (Hooking's own constructor needs the
/// already-running Coordinator), so every method awaits the `watch` channel becoming `Some`
/// rather than taking the handle up front (`tokio::sync::OnceCell` has no async wait; a `watch`
/// does).
#[derive(Debug)]
pub struct PropagationHandlerAdapter {
    pub hooking: tokio::sync::watch::Receiver<Option<HookingHandle>>,
}

async fn wait_for_hooking(
    rx: &tokio::sync::watch::Receiver<Option<HookingHandle>>,
) -> HookingHandle {
    let mut rx = rx.clone();
    rx.wait_for(Option::is_some)
        .await
        .expect("hooking handle sender is never dropped before the daemon shuts down")
        .clone()
        .expect("wait_for guarantees Some")
}

impl ntk_coordinator::PropagationHandler for PropagationHandlerAdapter {
    fn prepare_migration(&self, _level: usize, data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let hooking = wait_for_hooking(&self.hooking).await;
            if let Ok(migration_id) = codec::decode_migration_id(&data) {
                hooking.notify_prepare_migration(migration_id);
            }
        })
    }

    fn finish_migration(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let hooking = wait_for_hooking(&self.hooking).await;
            if let Ok(fmd) = codec::decode_finish_migration_data(&data) {
                hooking.notify_finish_migration(level, fmd);
            }
        })
    }

    fn prepare_enter(&self, _level: usize, data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let hooking = wait_for_hooking(&self.hooking).await;
            if let Ok(enter_id) = codec::decode_enter_id(&data) {
                tracing::info!(
                    enter_id,
                    "migration-instrumentation: prepare_enter propagation applied locally"
                );
                hooking.notify_prepare_enter(enter_id);
            }
        })
    }

    fn finish_enter(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let hooking = wait_for_hooking(&self.hooking).await;
            if let Ok(fed) = codec::decode_finish_enter_data(&data) {
                tracing::info!(
                    level,
                    entry_network_id = fed.entry_data.network_id,
                    entry_pos = ?fed.entry_data.pos,
                    "migration-instrumentation: finish_enter propagation applied locally"
                );
                hooking.notify_finish_enter(level, fed);
            }
        })
    }

    fn we_have_splitted(&self, level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tracing::debug!(
                level,
                "coordinator we_have_splitted propagation: not modeled by this daemon's Hooking wiring, ignored"
            );
        })
    }
}

/// Implements [`ntk_coordinator::CoordinatorStubFactory`]/[`ntk_coordinator::CoordinatorStub`]
/// over [`PeerLinks`] — see the module doc's "Routing model".
#[derive(Debug)]
pub struct CoordinatorStubFactoryAdapter {
    pub links: Arc<PeerLinks>,
}

impl ntk_coordinator::CoordinatorStubFactory for CoordinatorStubFactoryAdapter {
    fn stub_for_each_neighbor(&self) -> Vec<Arc<dyn ntk_coordinator::CoordinatorStub>> {
        self.links
            .all()
            .into_iter()
            .map(|(_, client)| {
                Arc::new(ntk_coordinator::RpcCoordinatorStub::new(client))
                    as Arc<dyn ntk_coordinator::CoordinatorStub>
            })
            .collect()
    }

    fn stub_for_all_neighbors(&self) -> Arc<dyn ntk_coordinator::CoordinatorStub> {
        Arc::new(AllNeighborsCoordinatorStub {
            links: self.links.clone(),
        })
    }
}

struct AllNeighborsCoordinatorStub {
    links: Arc<PeerLinks>,
}

macro_rules! fan_out {
    ($self:ident, $method:ident, $args:ident) => {
        Box::pin(async move {
            for (_, client) in $self.links.all() {
                let stub = ntk_coordinator::RpcCoordinatorStub::new(client);
                let _ = stub.$method($args.clone()).await;
            }
            Ok(())
        })
    };
}

impl ntk_coordinator::CoordinatorStub for AllNeighborsCoordinatorStub {
    fn execute_prepare_migration(
        &self,
        args: ntk_coordinator::PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        fan_out!(self, execute_prepare_migration, args)
    }

    fn execute_finish_migration(
        &self,
        args: ntk_coordinator::PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        fan_out!(self, execute_finish_migration, args)
    }

    fn execute_prepare_enter(
        &self,
        args: ntk_coordinator::PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        fan_out!(self, execute_prepare_enter, args)
    }

    fn execute_finish_enter(
        &self,
        args: ntk_coordinator::PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        fan_out!(self, execute_finish_enter, args)
    }

    fn execute_we_have_splitted(
        &self,
        args: ntk_coordinator::PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        fan_out!(self, execute_we_have_splitted, args)
    }
}

// ---------------------------------------------------------------------------
// ntk_peerservices::RoutingEnv
// ---------------------------------------------------------------------------

/// Implements [`RoutingEnv`] over the real [`QspnHandle`] plus [`LinkRegistry`]/[`PeerLinks`] —
/// see the module doc's "Routing model".
#[derive(Debug)]
pub struct RoutingEnvAdapter {
    pub qspn: QspnHandle,
    pub registry: Arc<LinkRegistry>,
    pub links: Arc<PeerLinks>,
}

impl RoutingEnvAdapter {
    fn stub_toward(&self, hc: HCoord, skip: Option<QspnArcId>) -> Option<Arc<dyn PeersStub>> {
        let snapshot = self.qspn.snapshot();
        let link = first_hop_link(&snapshot, &self.registry, hc, skip)?;
        let client = self.links.get(link)?;
        Some(Arc::new(ntk_peerservices::RpcPeersStub::new(
            client,
            self.qspn.my_naddr().topology().clone(),
        )))
    }

    /// Recovers the [`LinkId`] `stub` was resolved from, by matching its underlying
    /// `Arc<dyn ntk_rpc::RpcClient>` (stable per link in [`PeerLinks`]) against the live
    /// connection pool — see `gateway`'s own doc for why this, not stub identity, is the
    /// actionable signal a fresh-per-call [`ntk_peerservices::RpcPeersStub`] can offer.
    fn link_of_stub(&self, stub: &Arc<dyn PeersStub>) -> Option<LinkId> {
        let rpc_stub = stub
            .as_any()
            .downcast_ref::<ntk_peerservices::RpcPeersStub>()?;
        self.links
            .all()
            .into_iter()
            .find(|(_, client)| Arc::ptr_eq(client, rpc_stub.client()))
            .map(|(id, _)| id)
    }
}

impl RoutingEnv for RoutingEnvAdapter {
    fn gnode_exists(&self, hc: HCoord) -> bool {
        if self.qspn.my_naddr().is_inside(hc).unwrap_or(false) {
            return true;
        }
        self.qspn
            .snapshot()
            .levels
            .get(hc.level)
            .is_some_and(|entries| entries.iter().any(|e| e.destination == hc))
    }

    fn gateway(
        &self,
        hc: HCoord,
        failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>> {
        // `failed` names a previously-returned stub whose gateway just failed a real call:
        // exclude the exact arc it was resolved from and retry the next-best admitted path
        // towards `hc`, matching `RoutingEnv::gateway`'s own doc ("optionally avoiding
        // `failed`") and upstream's real `i_peers_gateway`
        // (`research/impl/vala/ntkd/peers_helpers.vala:72-135`), whose own `failed` handling
        // exists for exactly this: handing the caller a *different* candidate, not the same
        // dead one again. Upstream additionally tears down the underlying neighborhood arc
        // outright (`neighborhood_mgr.remove_my_arc`, `:76-92`) — a destructive,
        // connectivity-wide side effect this adapter deliberately does not reproduce: one
        // failed `PeersStub` call is not by itself evidence the whole arc (shared by every
        // other module routed over it: qspn, identities, hooking, coordinator) is dead, only
        // that this one routing attempt should not retry it. `None` once no admitted path
        // remains excluding `failed` — the caller (`ntk_peerservices::Handle::try_forward`)
        // treats that identically to "no gateway at all" and gives up on this target.
        let skip = failed
            .and_then(|f| self.link_of_stub(f))
            .and_then(|link| self.registry.qspn_arc_of(link));
        self.stub_toward(hc, skip)
    }

    /// `n` is often a *partial* tuple — `top()` less than this topology's full depth — built by
    /// `ntk_peerservices::tuple::make_tuple_node` inside a shared-ancestor g-node scope: every
    /// node inside that scope, this receiver included, shares the identical position at every
    /// level `>= n.top()` (`make_tuple_node`'s own doc: levels above the overridden one "keep my
    /// own position" — the *constructing* node's, which by the scope invariant is also this
    /// node's). `dial_target` does that widening and is the exact production analogue of
    /// upstream's own partial-tuple contract: `i_peers_get_tcp_inside`'s doc records "`positions[0]`
    /// is `pos[0]` ... of level positions.size" (`research/impl/vala/peerservices/peers.vala:90-93`),
    /// and `get_stub_main_identity_unicast_inside_gnode`'s real implementation
    /// (`research/impl/vala/ntkd/rpc/stub_factory.vala:69-85`) receives exactly such a prefix —
    /// "levels from 0 to the level just below the common g-node's level"
    /// (`research/impl/vala/documentation/ita/DemoneNTKD/RPC.md:495-509`) — and pads the rest.
    /// [`crate::node::adapters`]'s own already-proven-correct test-only twin,
    /// `ntk-andna/tests/multi_node.rs`'s `FakeEnv::dial`, does the identical
    /// `n.positions() ++ my_full()[n.top()..]` widening.
    fn dial(&self, n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
        let my_naddr = self.qspn.my_naddr();
        let target = dial_target(my_naddr, n)?;
        let hc = my_naddr.hcoord(&target).ok()??;
        self.stub_toward(hc, None)
    }

    fn nodes_in_my_group(&self, level: usize) -> usize {
        estimate_n_nodes(&self.qspn.snapshot(), None, level) as usize
    }

    fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
        self.links
            .all()
            .into_iter()
            .map(|(_, client)| {
                Arc::new(ntk_peerservices::RpcPeersStub::new(
                    client,
                    self.qspn.my_naddr().topology().clone(),
                )) as Arc<dyn PeersStub>
            })
            .collect()
    }
}

/// [`RoutingEnvAdapter::dial`]'s pure widening/refusal logic, split out — like
/// [`chosen_lvl_from_snapshot`]/[`estimate_n_nodes`] — so it is unit-testable without a live
/// [`QspnHandle`]. Widens `n` (levels `0..n.top()`) to a full [`Naddr`] by filling levels
/// `n.top()..topology.levels()` from `my_naddr`'s own position — see [`RoutingEnvAdapter::dial`]'s
/// own doc for why that is the correct value, not an arbitrary placeholder. Refuses a tuple
/// naming zero levels (identifies no real target) or more levels than `my_naddr`'s topology has
/// (`n.top() > topology.levels()`; unreachable for a `TupleNode` built against *this* topology —
/// `TupleNode::new` already rejects that — so only a tuple minted against a foreign, larger
/// topology can trigger it) without ever indexing out of bounds. A tuple built against a
/// foreign topology whose positions are in range for *it* but out of range for `my_naddr`'s own
/// topology is refused too, via [`Naddr::new`]'s own [`ntk_common::Error::PositionOutOfRange`].
fn dial_target(my_naddr: &Naddr, n: &TupleNode) -> Option<Naddr> {
    let topology = my_naddr.topology();
    if n.top() == 0 || n.top() > topology.levels() {
        return None;
    }
    let mut positions = n.positions().to_vec();
    positions.extend_from_slice(&my_naddr.positions()[n.top()..]);
    Naddr::new(topology.clone(), positions).ok()
}

/// Implements [`ntk_qspn::ArcResolver`] by resolving the caller's own stable Neighborhood id
/// (embedded in every outbound qspn call's `CallerContext.src_nic`,
/// `crate::node::stubs::RpcQspnStub`) against this node's own registry — see
/// `crate::node::registry::encode_caller_id`'s doc for why this must go through the registry's
/// id lookup rather than trusting a peer-minted [`LinkId`] directly.
#[derive(Debug)]
pub struct QspnArcResolverAdapter {
    pub registry: Arc<LinkRegistry>,
}

impl ntk_qspn::ArcResolver for QspnArcResolverAdapter {
    fn resolve(&self, caller: &ntk_proto::v1::CallerContext) -> Option<QspnArcId> {
        let link = self.registry.link_for_caller(caller.src_nic.as_ref()?)?;
        self.registry.qspn_arc_of(link)
    }
}

#[cfg(test)]
mod estimate_n_nodes_tests {
    use super::{NetworkInfo, estimate_n_nodes};
    use ntk_common::Cost;
    use ntk_qspn::{ArcId, RouteEntry, RoutePath, RouteSnapshot};

    fn route_path(nodes_inside: u32) -> RoutePath {
        RoutePath {
            arc: ArcId::from(0),
            hops: Vec::new(),
            cost: Cost::Finite(1),
            nodes_inside,
        }
    }

    fn entry(level: usize, pos: u32, nodes_inside: u32) -> RouteEntry {
        RouteEntry {
            destination: ntk_common::HCoord::new(level, pos),
            paths: vec![route_path(nodes_inside)],
        }
    }

    #[test]
    fn a_solitary_node_counts_only_itself() {
        let snapshot = RouteSnapshot {
            levels: vec![vec![]],
        };
        assert_eq!(estimate_n_nodes(&snapshot, None, 0), 1);
    }

    #[test]
    fn level_zero_destinations_always_count_as_one_real_node_each() {
        // Even a (malicious or buggy) peer claiming `nodes_inside=99` at level 0 must still
        // count as exactly one real node — a level-0 destination is one node by definition.
        let snapshot = RouteSnapshot {
            levels: vec![vec![entry(0, 1, 99), entry(0, 2, 99)]],
        };
        assert_eq!(estimate_n_nodes(&snapshot, None, 0), 3);
    }

    /// Pins the fix: a node whose own g-node has already absorbed further members via
    /// lower-level migrations must have those members flow through its own recursively
    /// aggregated count, not vanish behind a flat `+1` for "myself" — the exact shape
    /// `two_level_gnode_migrates_as_a_unit_into_merged_network` hit live (a node with two
    /// already-migrated-in siblings still reporting `n_nodes=1` on every ask).
    #[test]
    fn own_level_zero_siblings_are_reflected_in_a_higher_level_count() {
        let snapshot = RouteSnapshot {
            levels: vec![
                // My own g-node has already absorbed 2 further members: 1 (myself) + 2 = 3.
                vec![entry(0, 1, 1), entry(0, 2, 1)],
                // One sibling g-node at the top level, reporting a real size of 3.
                vec![entry(1, 1, 3)],
            ],
        };
        // Correct: my own 3-member g-node + the sibling's 3 = 6.
        assert_eq!(estimate_n_nodes(&snapshot, None, 1), 6);
    }

    #[test]
    fn a_foreign_peer_is_excluded_at_every_recursed_level() {
        let net = NetworkInfo::new(2, 1);
        net.note_foreign(0, 2); // a not-yet-merged foreign peer at level 0
        net.note_foreign(1, 1); // and another at level 1
        let snapshot = RouteSnapshot {
            levels: vec![vec![entry(0, 1, 1), entry(0, 2, 1)], vec![entry(1, 1, 3)]],
        };
        // My own g-node: only the non-foreign level-0 sibling counts (1 + 1 = 2); the foreign
        // level-1 sibling is excluded entirely.
        assert_eq!(estimate_n_nodes(&snapshot, Some(&net), 1), 2);
    }
}

/// Pins [`RoutingEnvAdapter::dial`]'s fix via its pure [`dial_target`] core: before this fix,
/// `dial` hard-required `n.top() == topology.levels()`, so a single-hop, level-0 forward's
/// `n.top() == 1` tuple (`ntk_peerservices::routing::Handle::contact_peer`'s own
/// `PeerMessageForwarder::n`, `x.level + 1` levels — always `1` in a 2-node network) was
/// *always* refused; reproduced live by `crates/ntkd/tests/andna_e2e.rs`. A genuinely malformed
/// tuple must still be refused both before and after.
#[cfg(test)]
mod dial_target_tests {
    use ntk_common::{Naddr, Topology};

    use super::{TupleNode, dial_target};

    fn topology() -> Topology {
        Topology::new([4, 2, 2, 2]).unwrap()
    }

    /// The exact shape `andna_e2e`'s node A hits: a single-hop, level-0 forward names only the
    /// leaf level (`top() == 1`), one level short of this topology's depth of 4.
    #[test]
    fn a_single_hop_partial_tuple_widens_to_the_right_full_address() {
        let my_naddr = Naddr::new(topology(), vec![2, 1, 0, 1]).unwrap();
        let n = TupleNode::new(topology(), vec![3]).unwrap();

        let target = dial_target(&my_naddr, &n).expect("a valid top()=1 tuple must resolve");
        // Level 0 comes from `n`; levels 1..4 are filled from my own position, exactly as
        // `make_tuple_node`'s scope invariant guarantees they already agree.
        assert_eq!(target.positions(), &[3, 1, 0, 1]);
    }

    /// The already-correct case: a tuple already scoped to the full topology depth needs no
    /// widening at all and must still resolve (this was the only case the old, over-strict
    /// `n.top() != topology.levels()` guard ever accepted).
    #[test]
    fn a_full_depth_tuple_still_resolves_unchanged() {
        let my_naddr = Naddr::new(topology(), vec![2, 1, 0, 1]).unwrap();
        let n = TupleNode::new(topology(), vec![3, 1, 0, 1]).unwrap();

        let target = dial_target(&my_naddr, &n).expect("a full-depth tuple must resolve");
        assert_eq!(target.positions(), &[3, 1, 0, 1]);
    }

    /// An empty tuple identifies no real target and must be refused, not silently widened into
    /// "dial myself".
    #[test]
    fn an_empty_tuple_is_refused() {
        let my_naddr = Naddr::new(topology(), vec![2, 1, 0, 1]).unwrap();
        let n = TupleNode::new(topology(), Vec::new()).unwrap();
        assert_eq!(dial_target(&my_naddr, &n), None);
    }

    /// A tuple minted against a foreign, deeper topology names more levels than this topology
    /// has — refused before ever indexing `my_naddr`'s own (shorter) position slice.
    #[test]
    fn a_tuple_naming_more_levels_than_my_topology_has_is_refused() {
        let my_naddr = Naddr::new(topology(), vec![2, 1, 0, 1]).unwrap();
        let foreign = Topology::new([4, 2, 2, 2, 2]).unwrap();
        let n = TupleNode::new(foreign, vec![3, 1, 0, 1, 0]).unwrap();
        assert_eq!(dial_target(&my_naddr, &n), None);
    }

    /// A tuple minted against a foreign topology whose level-0 g-node is larger than this
    /// topology's own names a position out of range here — refused via `Naddr::new`, not
    /// silently truncated/clamped into a wrong-but-valid address.
    #[test]
    fn a_position_out_of_range_for_my_topology_is_refused() {
        let my_naddr = Naddr::new(topology(), vec![2, 1, 0, 1]).unwrap();
        let foreign = Topology::new([8, 2, 2, 2]).unwrap();
        let n = TupleNode::new(foreign, vec![5]).unwrap();
        assert_eq!(dial_target(&my_naddr, &n), None);
    }
}

/// Pins the fix for the real-kernel `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit`
/// finding: two independently-numbered g-nodes assign level-0 positions from the same small
/// range, so a member's own external arc negotiations into a foreign network call
/// `NetworkInfo::note_foreign` at positions its own real siblings also occupy. Before this fix,
/// whichever of `note_foreign`/`note_same_network` last touched a given `(level, pos)` won —
/// arc-handler ordering is not guaranteed, so a real sibling could be misclassified foreign,
/// undercounting `chosen_lvl_from_snapshot` and routing a member of a genuinely migrating
/// g-node down the individual `ask_lvl == 0` path instead of the collective one.
#[cfg(test)]
mod sibling_position_collision_tests {
    use super::{NetworkInfo, chosen_lvl_from_snapshot};
    use ntk_common::Cost;
    use ntk_qspn::{ArcId, RouteEntry, RoutePath, RouteSnapshot};

    fn entry(level: usize, pos: u32) -> RouteEntry {
        RouteEntry {
            destination: ntk_common::HCoord::new(level, pos),
            paths: vec![RoutePath {
                arc: ArcId::from(0),
                hops: Vec::new(),
                cost: Cost::Finite(1),
                nodes_inside: 1,
            }],
        }
    }

    /// A same-network confirmation, once recorded, must win regardless of whether the
    /// colliding `note_foreign` call for the identical numeric position happened before or
    /// after it.
    #[test]
    fn same_network_confirmation_survives_a_colliding_foreign_report_either_order() {
        let confirmed_then_foreign = NetworkInfo::new(1, 1);
        confirmed_then_foreign.note_same_network(0, 1);
        confirmed_then_foreign.note_foreign(0, 1);
        assert!(!confirmed_then_foreign.is_foreign(0, 1));

        let foreign_then_confirmed = NetworkInfo::new(1, 1);
        foreign_then_confirmed.note_foreign(0, 1);
        foreign_then_confirmed.note_same_network(0, 1);
        assert!(!foreign_then_confirmed.is_foreign(0, 1));
    }

    /// The full scenario `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` hit live:
    /// `a1`, a member of a real 3-member trio (`a0`,`a1`,`a2`, level-0 positions `{0,1,2}`),
    /// merging into another trio (`b0`,`b1`,`b2`) that independently numbered its own level-0
    /// members `{0,1,2}` too. `a1`'s own real siblings are confirmed via `note_same_network`;
    /// its external arc negotiations into `b`'s group call `note_foreign` at the identical
    /// numeric positions. `chosen_lvl_from_snapshot` must still see `a0`/`a2` as real siblings
    /// and return `1` (the collective path), not `0` (the individual path this bug routed
    /// every trio member but the first into).
    #[test]
    fn a_trio_member_with_positions_colliding_a_foreign_trio_still_gets_the_collective_level() {
        let net = NetworkInfo::new(2, 91_000_001);
        // a1's own real siblings, confirmed via internal arc negotiation.
        net.note_same_network(0, 0); // a0
        net.note_same_network(0, 2); // a2
        // a1's external arc negotiations into b's group — same numeric positions, unrelated
        // foreign peers, fired after the confirmations above (ordering the bug depended on).
        net.note_foreign(0, 0); // b0
        net.note_foreign(0, 1); // b1
        net.note_foreign(0, 2); // b2

        let snapshot = RouteSnapshot {
            levels: vec![vec![entry(0, 0), entry(0, 1), entry(0, 2)]],
        };
        assert_eq!(chosen_lvl_from_snapshot(&snapshot, &net, 2), 1);
    }

    proptest::proptest! {
        /// However many siblings and colliding-position foreign peers exist, and in whichever
        /// order their `note_*` calls interleave, a position ever confirmed same-network is
        /// never later read as foreign.
        #[test]
        fn confirmed_positions_never_read_foreign_regardless_of_interleaving(
            confirmed in proptest::collection::hash_set(0u32..8, 0..8),
            foreign in proptest::collection::hash_set(0u32..8, 0..8),
            confirm_first in proptest::bool::ANY,
        ) {
            let net = NetworkInfo::new(1, 1);
            if confirm_first {
                for &p in &confirmed { net.note_same_network(0, p); }
                for &p in &foreign { net.note_foreign(0, p); }
            } else {
                for &p in &foreign { net.note_foreign(0, p); }
                for &p in &confirmed { net.note_same_network(0, p); }
            }
            for &p in &confirmed {
                proptest::prop_assert!(!net.is_foreign(0, p));
            }
        }
    }
}

/// Pins the concurrent-placement fix: [`EnterArbiter`] must grant at most one real election per
/// `(level, network_id)` regardless of how many members of the same g-node ask, and regardless
/// of `completed_enter` — only `abort_enter` (or [`ELECTED_TTL`] expiring) may open the level
/// back up — and the cross-process fix: two *distinct* `EnterArbiter` instances sharing the
/// same Coordinator-replicated record converge on exactly one election too. See
/// [`EnterArbiter`]'s own doc for the upstream citations this reproduces/fixes.
#[cfg(test)]
mod enter_arbiter_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::BoxFuture;
    use ntk_common::{HCoord, Naddr, Topology};
    use ntk_coordinator::{
        AbortEnterHandler, BeginEnterHandler, CompletedEnterHandler, Config as CoordConfig,
        CoordinatorMap, CoordinatorService, EnterHandlers, EvaluateEnterHandler,
        FakeCoordinatorStubFactory, Manager as CoordinatorManager, PropagationHandler,
    };
    use ntk_peerservices::{
        Config as PeersConfig, Manager as PeersManager, PeersStub, RoutingEnv, TupleNode,
    };
    use ntk_proto::v1::TypedValue;
    use tokio_util::sync::CancellationToken;

    use super::{ELECTED_TTL, EnterArbiter};
    use crate::node::codec::{self, EvaluateEnterAnswer};

    #[derive(Debug, Clone)]
    struct TestMap;
    impl CoordinatorMap for TestMap {
        fn n_nodes(&self) -> u64 {
            1
        }
        fn free_positions(&self, _level: usize) -> Vec<u32> {
            vec![0, 1, 2, 3]
        }
        fn can_reserve(&self, _level: usize) -> bool {
            true
        }
        fn my_pos(&self, _level: usize) -> u32 {
            0
        }
        fn fp_id(&self, _level: usize) -> i64 {
            0
        }
    }

    /// `evaluate_enter`/`begin_enter`/`completed_enter`/`abort_enter` are never exercised
    /// through the real `CoordinatorService::exec` by these tests (they exercise
    /// [`EnterArbiter::decide`]/[`EnterArbiter::release`] directly) — only wired to satisfy
    /// `Manager::new`.
    struct NoopHandlers;
    impl EvaluateEnterHandler for NoopHandlers {
        fn evaluate_enter<'a>(
            &'a self,
            _top: usize,
            data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async move { data })
        }
    }
    impl BeginEnterHandler for NoopHandlers {
        fn begin_enter<'a>(
            &'a self,
            _top: usize,
            data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async move { data })
        }
    }
    impl CompletedEnterHandler for NoopHandlers {
        fn completed_enter<'a>(
            &'a self,
            _top: usize,
            data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async move { data })
        }
    }
    impl AbortEnterHandler for NoopHandlers {
        fn abort_enter<'a>(
            &'a self,
            _top: usize,
            data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async move { data })
        }
    }

    fn noop_enter_handlers() -> EnterHandlers {
        let h = Arc::new(NoopHandlers);
        EnterHandlers {
            evaluate_enter: h.clone(),
            begin_enter: h.clone(),
            completed_enter: h.clone(),
            abort_enter: h,
        }
    }

    struct NoopPropagationHandler;
    impl PropagationHandler for NoopPropagationHandler {
        fn prepare_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn finish_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn prepare_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn finish_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn we_have_splitted(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    struct SingleNodeEnv;
    impl RoutingEnv for SingleNodeEnv {
        fn gnode_exists(&self, hc: HCoord) -> bool {
            hc.level == 0 && hc.pos == 0
        }
        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            None
        }
        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            None
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            1
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    /// A real single-node PeerServices + Coordinator stack — the same harness shape
    /// `ntk-coordinator/tests/reserve.rs` uses for the same purpose — so [`EnterArbiter::decide`]
    /// exercises the real `Handle`/replication plumbing behind
    /// [`CoordinatorService::hooking_memory_locally`]/`set_hooking_memory_locally`, not a
    /// hand-rolled stand-in. Every test uses `top = 1` (this topology's only level, i.e.
    /// `levels()`), matching [`super::EnterHandlersAdapter`]'s own production target.
    async fn build_service() -> (Arc<CoordinatorService>, CancellationToken) {
        let topology = Topology::new([4]).unwrap();
        let my_addr = Naddr::new(topology.clone(), vec![0u32; topology.levels()]).unwrap();
        let (peers_manager, peers) = PeersManager::new(
            topology.clone(),
            my_addr,
            Arc::new(SingleNodeEnv),
            PeersConfig::default(),
            topology.levels(),
        );
        let cancel = CancellationToken::new();
        tokio::spawn(peers_manager.run(cancel.child_token()));

        let (coordinator_manager, coordinator) = CoordinatorManager::new(
            topology.clone(),
            Arc::new(TestMap),
            Arc::new(FakeCoordinatorStubFactory::new(Vec::new())),
            Arc::new(NoopPropagationHandler),
            noop_enter_handlers(),
            CoordConfig::default(),
            None,
        );
        tokio::spawn(coordinator_manager.run(cancel.child_token()));
        (
            Arc::new(CoordinatorService::new(coordinator, peers)),
            cancel,
        )
    }

    /// Before this fix, `EnterArbiter::in_flight` was cleared by `completed_enter`, so a
    /// *second* member of the same still-migrating g-node — asking with a different
    /// `evaluate_enter_id` for the identical `network_id` right after the first member
    /// completed — was granted its own, independent `Accepted`, going on to reserve a fresh
    /// sibling slot on the target instead of being told to fall back and observe `SameNetwork`.
    /// This reproduces that scenario against the current code and pins the fix: the second
    /// entrant must be refused (`IgnoreNetwork`), not accepted again.
    #[tokio::test]
    async fn a_second_concurrent_entrant_for_the_same_network_is_refused_not_reelected() {
        let (service, _cancel) = build_service().await;
        let arbiter = EnterArbiter::new();
        let (level, network_id, top) = (1, 42, 1);

        // Entrant 1 is elected.
        assert_eq!(
            arbiter.decide(&service, top, level, network_id, 1).await,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
        // Entrant 1 completes its own episode — must NOT reopen the level.
        // (completed_enter's real handler no longer calls anything on the arbiter at all;
        // there being nothing to call here *is* the fix.)

        // Entrant 2, a different member of the same g-node asking about the same target
        // network, must be refused rather than independently elected.
        assert_eq!(
            arbiter.decide(&service, top, level, network_id, 2).await,
            EvaluateEnterAnswer::IgnoreNetwork
        );
        // Entrant 1 itself retrying (idempotent) still sees its own grant.
        assert_eq!(
            arbiter.decide(&service, top, level, network_id, 1).await,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
    }

    /// The invariant generalized: for any number of concurrent members of one g-node asking
    /// about the same target, exactly one is ever elected.
    #[tokio::test]
    async fn exactly_one_of_any_number_of_concurrent_entrants_is_elected() {
        let (service, _cancel) = build_service().await;
        let arbiter = EnterArbiter::new();
        let (level, network_id, top) = (0, 7, 1);
        let mut accepted = 0;
        for id in 1..=8 {
            if arbiter.decide(&service, top, level, network_id, id).await
                == (EvaluateEnterAnswer::Accepted { chosen_lvl: level })
            {
                accepted += 1;
            }
        }
        assert_eq!(accepted, 1, "exactly one candidate must ever be elected");
    }

    /// A concurrent ask for a genuinely *different* target network must not be starved forever
    /// by an unrelated election in progress at the same level — it is told to ask again rather
    /// than folded into (or refused because of) the other election.
    #[tokio::test]
    async fn a_different_target_network_is_told_to_ask_again_not_refused() {
        let (service, _cancel) = build_service().await;
        let arbiter = EnterArbiter::new();
        let (level, top) = (0, 1);
        assert_eq!(
            arbiter.decide(&service, top, level, 1, 1).await,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
        assert_eq!(
            arbiter.decide(&service, top, level, 2, 2).await,
            EvaluateEnterAnswer::AskAgain
        );
    }

    /// `abort_enter` (the elected candidate itself giving up) must release the level
    /// immediately, both locally and in the Coordinator's own replicated record, so a fresh
    /// election can proceed without waiting out [`ELECTED_TTL`].
    #[tokio::test]
    async fn abort_releases_the_level_for_a_fresh_election() {
        let (service, _cancel) = build_service().await;
        let arbiter = EnterArbiter::new();
        let (level, top) = (0, 1);
        assert_eq!(
            arbiter.decide(&service, top, level, 1, 1).await,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
        arbiter.release(&service, top, level).await;
        // A different id, even for the same network, is now freely (re-)elected — which also
        // pins that `release` cleared the *replicated* record, not only this process's own
        // local cache: a stale replicated record with a different `evaluate_enter_id` would
        // otherwise still answer `IgnoreNetwork` here.
        assert_eq!(
            arbiter.decide(&service, top, level, 1, 2).await,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
    }

    /// Self-heals if `finish_enter` propagation never lands and nobody ever calls
    /// `abort_enter` either — including when the member that granted the election is simply
    /// gone (crashed, no `Drop` ever runs on a killed process): once [`ELECTED_TTL`] has
    /// elapsed *in the replicated record itself*, a fresh election is granted instead of
    /// refusing forever. Seeds the shared record directly (bypassing `decide`) so the test
    /// controls the record's own timestamp without a real sleep.
    #[tokio::test]
    async fn an_expired_replicated_election_self_heals_without_an_abort() {
        let (service, _cancel) = build_service().await;
        let arbiter = EnterArbiter::new();
        let (level, top) = (0, 1);

        let stale_millis = codec::now_millis().saturating_sub(
            u64::try_from((ELECTED_TTL + Duration::from_millis(1)).as_millis()).unwrap(),
        );
        let mut mem = codec::HookingMemory::default();
        mem.elections.insert(
            level,
            codec::ElectionRecord {
                network_id: 1,
                evaluate_enter_id: 1,
                granted_at_millis: stale_millis,
            },
        );
        service
            .set_hooking_memory_locally(top, Some(codec::encode_hooking_memory(&mem)))
            .await;

        assert_eq!(
            arbiter.decide(&service, top, level, 1, 2).await,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
    }

    /// Pins the cross-process fix directly: two *distinct* `EnterArbiter` instances — standing
    /// in for two distinct physical members of the same target g-node, each independently
    /// asked to serve an `evaluate_enter` for the same `(level, network_id)` — converge on
    /// exactly one election when they share the same Coordinator record, exactly this daemon's
    /// real production wiring (each member's own `EnterHandlersAdapter` reaches *its own*
    /// node's `CoordinatorService`, and the underlying `hooking_memory` record is replicated
    /// across members by [`CoordinatorService::set_hooking_memory_locally`]'s own fanout).
    #[tokio::test]
    async fn two_members_sharing_the_coordinators_record_converge_on_one_election() {
        let (service, _cancel) = build_service().await;
        let member_a = EnterArbiter::new();
        let member_b = EnterArbiter::new();
        let (level, network_id, top) = (1, 99, 1);

        // Member A is asked first — some caller's own `contact_peer` resolution landed on it.
        let answer_a = member_a.decide(&service, top, level, network_id, 1).await;
        // Member B is asked moments later about the *same* merge, but a *different* caller's
        // resolution landed on B instead — the exact eventual-consistency skew this struct's
        // own doc describes (and the real trace captured, ~5s apart). B must observe A's
        // already-recorded election and defer, not run its own.
        let answer_b = member_b.decide(&service, top, level, network_id, 2).await;

        assert_eq!(
            answer_a,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
        assert_eq!(answer_b, EvaluateEnterAnswer::IgnoreNetwork);
    }

    /// The failure mode the fix above closes, reproduced directly: without a *shared* backing
    /// record (two independent Coordinator stores — the pre-fix shape, since a purely local
    /// `EnterArbiter` never consulted anything shared at all), two distinct `EnterArbiter`s
    /// each grant their own, independent `Accepted` for the identical `(level, network_id)` —
    /// the split-election defect the real trace captured (two physical servants, each fanning
    /// `finish_enter` to a different subset of the same entering trio).
    #[tokio::test]
    async fn two_members_with_independent_records_would_each_elect_a_different_member() {
        let (service_a, _cancel_a) = build_service().await;
        let (service_b, _cancel_b) = build_service().await;
        let member_a = EnterArbiter::new();
        let member_b = EnterArbiter::new();
        let (level, network_id, top) = (1, 99, 1);

        let answer_a = member_a.decide(&service_a, top, level, network_id, 1).await;
        let answer_b = member_b.decide(&service_b, top, level, network_id, 2).await;

        assert_eq!(
            answer_a,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level }
        );
        assert_eq!(
            answer_b,
            EvaluateEnterAnswer::Accepted { chosen_lvl: level },
            "two independent records reproduce the pre-fix split: both members grant their own election"
        );
    }
}

/// Pins the `fp_id` fix: see the module doc's "Fixed: `fp_id`" section for the full story
/// (`check_propagation` silently dropping a real sibling's collective `finish_enter` because
/// `fp_id` used to be a per-g-node fingerprint that either drifted via a local counter or
/// needed qspn convergence this daemon's own compressed merge timing outruns).
#[cfg(test)]
mod coordinator_map_fp_id_tests {
    use super::NetworkInfo;

    /// The bug this fixes: two real siblings sharing one pre-formed `network_id` (e.g.
    /// `NodeInputs::preformed`) must compute the *same* `fp_id`
    /// ([`super::CoordinatorMapAdapter::fp_id`] is a one-line delegation to
    /// [`NetworkInfo::network_id`], exercised directly here) before either has migrated, or
    /// `check_propagation` on the receiving side rejects the sender's collective `finish_enter`
    /// fan-out outright — and must do so *immediately*, with no dependency on either side's own
    /// qspn convergence state: `NetworkInfo` has no qspn handle at all to converge.
    #[test]
    fn two_identities_sharing_a_network_id_compute_the_same_fp_id() {
        let a = NetworkInfo::new(2, 91_000_001);
        let b = NetworkInfo::new(2, 91_000_001);
        assert_eq!(a.network_id(), b.network_id());
    }

    /// The original bug this must not reopen: two coincidentally-co-positioned but *unrelated*
    /// networks (independently random `network_id`s) must still diverge.
    #[test]
    fn two_identities_with_different_network_ids_diverge() {
        let a = NetworkInfo::new(2, 91_000_001);
        let b = NetworkInfo::new(2, 91_000_002);
        assert_ne!(a.network_id(), b.network_id());
    }

    /// A migration (`NetworkInfo::set_network_id`) must be reflected *immediately* — no `Drop`,
    /// no background task, no event to wait for — otherwise a just-migrated identity keeps
    /// announcing its old network's `fp_id` inside its new one for however long a background
    /// refresh would otherwise lag.
    #[test]
    fn set_network_id_is_reflected_immediately() {
        let a = NetworkInfo::new(2, 91_000_001);
        let before = a.network_id();
        a.set_network_id(91_000_002);
        assert_ne!(before, a.network_id());
        assert_eq!(a.network_id(), 91_000_002);
    }
}
