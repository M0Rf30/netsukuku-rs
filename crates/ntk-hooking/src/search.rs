//! Network-discovery / g-node-splitting / position-choice: `execute_search`
//! (`research/impl/vala/hooking/hooking.vala:156-228`), `execute_explore`
//! (`:230-235`), `execute_delete_reserve` (`:237-242`), `execute_mig`
//! (`:244-274`), and the migration-path BFS `find_shortest_mig`
//! (`:326-464`) plus `execute_shortest_mig`/`get_migs`
//! (`:466-490`, `structs.vala:187-237`).
//!
//! The BFS is decoupled from the wire/transport via [`SearchRouter`]: each
//! step asks "what does the g-node I'm currently visiting look like, and
//! what does reserving inside it resolve to" without knowing whether that
//! answer came from a local call or a multi-hop routed RPC
//! (`message_routing.vala`, ported in [`crate::routing`]). This lets the
//! BFS algorithm itself be unit-tested against a hand-built oracle instead
//! of a live multi-node mesh.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::future::BoxFuture;
use thiserror::Error;

use crate::coordinator::CoordinatorClient;
use crate::domain::{
    MigOp, PairTupleGNodeInt, PathHop, RequestPacket, TupleGNode, i_am_inside, make_tuple_from_hc,
    make_tuple_from_level, positions_equal, tuple_has_virtual_pos,
};
use crate::view::QspnView;

/// A migration-path routing hop failed (peer unreachable, or timed out
/// waiting for a correlated response) — the Rust replacement for
/// `MessageRouting.SearchMigrationPathError`/`ExploreGNodeError`
/// (`message_routing.vala:25-31`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("migration-path routing hop failed")]
pub struct RoutingError;

/// `execute_search`'s reservation outcome for one visited g-node
/// (`hooking.vala:156-228`'s `out` parameters, minus `min_host_lvl` which
/// [`execute_search`] returns as its own return-position field so callers
/// don't have to thread it separately).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchStepResult {
    pub min_host_lvl: usize,
    pub final_host_lvl: usize,
    pub real_new_pos: Option<u32>,
    pub real_new_eldership: Option<i32>,
    pub set_adjacent: Vec<PairTupleGNodeInt>,
    pub new_conn_vir_pos: Option<u32>,
    pub new_eldership: Option<i32>,
}

/// One step of the distributed BFS: ask whoever hosts `path_hops`'s final
/// hop to run [`execute_search`]/[`execute_explore`] locally and report
/// back — `MessageRouting.send_search_request`/`send_explore_request`
/// (`message_routing.vala:93-164,487-545`) collapsed to their essential
/// request/response shape. A real implementation ([`crate::MessageRouting`]) picks
/// "run locally" vs. "route over the wire" based on whether the caller is
/// already inside the target g-node; a test oracle can just answer directly
/// from a hand-built topology.
pub trait SearchRouter: Send + Sync {
    fn send_search_request(
        &self,
        path_hops: Vec<PathHop>,
        max_host_lvl: usize,
        reserve_request_id: i32,
    ) -> BoxFuture<'_, Result<SearchStepResult, RoutingError>>;

    fn send_explore_request(
        &self,
        path_hops: Vec<PathHop>,
        requested_lvl: usize,
    ) -> BoxFuture<'_, Result<TupleGNode, RoutingError>>;

    /// Fire-and-forget — `send_delete_reserve_request`
    /// (`message_routing.vala:739-772`) has no response upstream either.
    fn send_delete_reserve_request(&self, dest_gnode: TupleGNode, reserve_request_id: i32);

    fn send_mig_request(&self, packet: RequestPacket) -> BoxFuture<'_, Result<(), RoutingError>>;
}

/// `execute_search` (`hooking.vala:156-228`): attempts to reserve a real
/// (non-virtual) position for `visiting_gnode` inside this node's own
/// hierarchy, climbing host levels until either a real slot is found or
/// `max_host_lvl` is exhausted.
///
/// Returns `None` for every case upstream answers by dropping the message
/// entirely or (functionally equivalently, see this module's doc comment)
/// by returning a response [`find_shortest_mig`] would discard anyway:
/// I am not inside `visiting_gnode`, I am myself virtual
/// (`tasklet.exit_tasklet(null)`, `:164-165`), or no host level up to
/// `max_host_lvl` has a reachable coordinator at all
/// (`:185-189`, upstream returns `min_host_lvl > max_host_lvl` with every
/// other field left at its `-1`/`null` default — [`find_shortest_mig`]
/// treats that identically to no response).
pub async fn execute_search(
    view: &dyn QspnView,
    coord: &dyn CoordinatorClient,
    visiting_gnode: &TupleGNode,
    max_host_lvl: usize,
    reserve_request_id: i32,
) -> Option<SearchStepResult> {
    if !i_am_inside(visiting_gnode, view) {
        return None;
    }
    if tuple_has_virtual_pos(&make_tuple_from_level(0, view), view) {
        return None;
    }

    let levels = view.topology().levels();
    let gsize = |lvl: usize| view.topology().gsize(lvl - 1).unwrap_or(0);

    let mut min_host_lvl = visiting_gnode.level(levels);
    let (pos, eldership) = loop {
        if min_host_lvl > max_host_lvl {
            return None;
        }
        match coord.reserve(min_host_lvl, reserve_request_id).await {
            Ok(r) => break (r.pos, r.eldership),
            Err(_) => {
                min_host_lvl += 1;
            }
        }
    };

    let mut final_host_lvl = min_host_lvl;
    if pos < gsize(final_host_lvl) {
        return Some(SearchStepResult {
            min_host_lvl,
            final_host_lvl,
            real_new_pos: Some(pos),
            real_new_eldership: Some(eldership),
            set_adjacent: adjacency_set(view, min_host_lvl),
            new_conn_vir_pos: None,
            new_eldership: None,
        });
    }

    let new_conn_vir_pos = pos;
    let new_eldership = eldership;
    final_host_lvl += 1;
    let mut real_new_pos = None;
    let mut real_new_eldership = None;
    while final_host_lvl <= max_host_lvl {
        // `assert_not_reached()` on a `CoordReserveError` here
        // (`hooking.vala:202-206`): upstream assumes any host level above
        // one that already answered once will always answer again. This
        // crate keeps that assumption explicit via `expect` rather than
        // silently swallowing a state upstream declares impossible.
        let r = coord
            .reserve(final_host_lvl, reserve_request_id)
            .await
            .expect(
                "a host level above an already-successful reservation must not fail to reserve \
                 (hooking.vala:202-206 asserts this unreachable)",
            );
        if r.pos < gsize(final_host_lvl) {
            real_new_pos = Some(r.pos);
            real_new_eldership = Some(r.eldership);
            break;
        }
        final_host_lvl += 1;
    }

    Some(SearchStepResult {
        min_host_lvl,
        final_host_lvl,
        real_new_pos,
        real_new_eldership,
        set_adjacent: adjacency_set(view, min_host_lvl),
        new_conn_vir_pos: Some(new_conn_vir_pos),
        new_eldership: Some(new_eldership),
    })
}

fn adjacency_set(view: &dyn QspnView, min_host_lvl: usize) -> Vec<PairTupleGNodeInt> {
    let levels = view.topology().levels();
    (min_host_lvl..levels)
        .flat_map(|i| view.adjacent_to_my_gnode(i, min_host_lvl))
        .map(|adj| PairTupleGNodeInt {
            gnode: make_tuple_from_hc(adj.hc, view),
            border_real_pos: adj.border_real_pos,
        })
        .collect()
}

/// `execute_explore` (`hooking.vala:230-235`): reports my own tuple at
/// `requested_lvl`.
#[must_use]
pub fn execute_explore(requested_lvl: usize, view: &dyn QspnView) -> TupleGNode {
    make_tuple_from_level(requested_lvl, view)
}

/// `execute_delete_reserve` (`hooking.vala:237-242`).
pub async fn execute_delete_reserve(
    coord: &dyn CoordinatorClient,
    view: &dyn QspnView,
    dest_gnode: &TupleGNode,
    reserve_request_id: i32,
) {
    let lvl = dest_gnode.level(view.topology().levels());
    coord.delete_reserve(lvl, reserve_request_id).await;
}

/// `execute_mig` (`hooking.vala:244-274`): applies a routed
/// `PREPARE_MIGRATION`/`FINISH_MIGRATION` packet by asking the Coordinator
/// to propagate it to every member of the migrating g-node.
///
/// Upstream never sets `migration_data.network_id` for a `FinishMigration`
/// packet (`hooking.vala:256-266` constructs a bare `new EntryData()` and
/// only assigns `pos`/`elderships`) — a migration happens entirely within
/// one already-known network, so the field is meaningless here; this port
/// keeps that exact omission (`0`) rather than inventing a value upstream
/// itself never provides.
pub async fn execute_mig(
    coord: &dyn CoordinatorClient,
    view: &dyn QspnView,
    packet: &RequestPacket,
) {
    let lvl = packet.dest.level(view.topology().levels());
    match packet.operation {
        MigOp::PrepareMigration => coord.prepare_migration(lvl, packet.migration_id).await,
        MigOp::FinishMigration => {
            let mut pos = packet.host_gnode.pos.clone();
            let mut elderships = packet.host_gnode.eldership.clone();
            pos.insert(0, u32::try_from(packet.real_new_pos).unwrap_or(0));
            elderships.insert(0, packet.real_new_eldership);
            let migration_data = crate::domain::EntryData {
                network_id: 0,
                pos,
                elderships,
            };
            let data = crate::domain::FinishMigrationData {
                migration_id: packet.migration_id,
                migration_data,
                go_connectivity_position: u32::try_from(packet.conn_gnode_pos).unwrap_or(0),
            };
            coord.finish_migration(lvl, data).await;
        }
    }
}

/// One step of the BFS tree — `SolutionStep` (`structs.vala:23-56`).
#[derive(Clone, Debug)]
struct SolutionStep {
    visiting_gnode: TupleGNode,
    previous_migrating_gnode: Option<TupleGNode>,
    previous_gnode_new_conn_vir_pos: Option<u32>,
    previous_gnode_new_eldership: Option<i32>,
    parent: Option<Arc<SolutionStep>>,
}

impl SolutionStep {
    fn distance(&self) -> usize {
        let mut d = 0;
        let mut cur = self.parent.as_deref();
        while let Some(p) = cur {
            d += 1;
            cur = p.parent.as_deref();
        }
        d
    }
}

/// One accepted migration-path solution — `Solution` (`structs.vala:58-72`).
#[derive(Clone, Debug)]
pub struct MigrationSolution {
    leaf: Arc<SolutionStep>,
    pub final_host_lvl: usize,
    pub real_new_pos: u32,
    pub real_new_eldership: i32,
}

impl MigrationSolution {
    #[must_use]
    pub fn distance(&self) -> usize {
        self.leaf.distance()
    }

    /// The dest g-node a rejected (non-chosen) solution's reservation
    /// should be released against — `hooking.vala:536-539`.
    #[must_use]
    pub fn cleanup_target(&self, levels: usize) -> TupleGNode {
        self.leaf
            .visiting_gnode
            .truncate_to_level(self.final_host_lvl, levels)
    }

    /// Resolves this solution into the final `EntryData` —
    /// `search_migration_path`'s two tail branches
    /// (`hooking.vala:542-577`): a distance-0 solution needs no migration
    /// chain walk; otherwise walks `self.leaf`'s parent chain up to the BFS
    /// root, combining the root's own known address with the outermost
    /// migration hop's freshly assigned position/eldership.
    ///
    /// # Panics
    /// If called on a distance-0 solution whose `real_new_pos`/
    /// `real_new_eldership` somehow bypass this module's own invariant (see
    /// [`find_shortest_mig`]'s doc comment) — cannot happen through the
    /// public API.
    #[must_use]
    pub fn resolve_entry_data(&self, view: &dyn QspnView) -> crate::domain::EntryData {
        let network_id = view.network_id();
        if self.distance() == 0 {
            let host_gnode = make_tuple_from_level(self.final_host_lvl, view);
            let mut pos = host_gnode.pos;
            let mut elderships = host_gnode.eldership;
            pos.insert(0, self.real_new_pos);
            elderships.insert(0, self.real_new_eldership);
            tracing::info!(
                network_id,
                final_host_lvl = self.final_host_lvl,
                real_new_pos = self.real_new_pos,
                ?pos,
                distance = 0,
                "hooking: resolve_entry_data (distance-0 branch)"
            );
            return crate::domain::EntryData {
                network_id,
                pos,
                elderships,
            };
        }
        let mut root = self
            .leaf
            .parent
            .clone()
            .expect("distance() > 0 implies leaf has a parent");
        let mut second = self.leaf.clone();
        while let Some(parent) = root.parent.clone() {
            root = parent;
            second = second
                .parent
                .clone()
                .expect("root/second walk stays in sync");
        }
        let prev = second
            .previous_migrating_gnode
            .as_ref()
            .expect("every non-root SolutionStep carries a previous_migrating_gnode");
        let mut pos = root.visiting_gnode.pos.clone();
        let mut elderships = root.visiting_gnode.eldership.clone();
        pos.insert(0, prev.pos[0]);
        elderships.insert(0, second.previous_gnode_new_eldership.unwrap_or(-1));
        tracing::info!(
            network_id,
            final_host_lvl = self.final_host_lvl,
            real_new_pos = self.real_new_pos,
            ?pos,
            distance = self.distance(),
            "hooking: resolve_entry_data (walked branch)"
        );
        crate::domain::EntryData {
            network_id,
            pos,
            elderships,
        }
    }
}

fn get_path_hops(current: &Arc<SolutionStep>) -> Vec<PathHop> {
    let mut hops = Vec::new();
    let mut cur = Some(current.clone());
    while let Some(step) = cur {
        hops.insert(
            0,
            PathHop {
                visiting_gnode: step.visiting_gnode.clone(),
                previous_migrating_gnode: step.previous_migrating_gnode.clone(),
            },
        );
        cur = step.parent.clone();
    }
    hops
}

/// `find_shortest_mig` (`hooking.vala:326-464`): BFS over candidate host
/// g-nodes, starting at `first_host_lvl`, looking for a real position no
/// deeper than `ok_host_lvl` (accepted immediately) or otherwise the
/// shallowest reachable one.
///
/// **Deviation from upstream**: `hooking.vala:381-390,397-401` records a
/// [`MigrationSolution`] even when `execute_search` could not resolve a
/// real position at all (leaving `real_new_pos`/`real_new_eldership` at
/// their `-1` sentinel) — a latent defect that would otherwise propagate a
/// nonsensical negative position into the final `EntryData`. This port only
/// ever records a solution when both fields are genuinely resolved, and — in
/// the `final_host_lvl <= ok_host_lvl` "early accept" branch specifically —
/// continues the BFS instead of returning early when that step turned out
/// to have no real position after all, since terminating the whole search
/// on a single unresolved hop would incorrectly hide a solution reachable
/// via a different branch.
pub async fn find_shortest_mig(
    view: &dyn QspnView,
    router: &dyn SearchRouter,
    reserve_request_id: i32,
    first_host_lvl: usize,
    ok_host_lvl: usize,
) -> Vec<MigrationSolution> {
    let levels = view.topology().levels();
    let subnetlevel = view.subnetlevel();
    let first_host_lvl = first_host_lvl.max(subnetlevel + 1);
    let ok_host_lvl = ok_host_lvl.max(first_host_lvl);

    let root_gnode = make_tuple_from_level(first_host_lvl, view);
    let mut max_host_lvl = levels;
    let mut solutions = Vec::new();
    let mut prev_sol_distance: Option<usize> = None;

    let mut visited: Vec<TupleGNode> = vec![root_gnode.clone()];
    let mut queue: VecDeque<Arc<SolutionStep>> = VecDeque::new();
    queue.push_back(Arc::new(SolutionStep {
        visiting_gnode: root_gnode,
        previous_migrating_gnode: None,
        previous_gnode_new_conn_vir_pos: None,
        previous_gnode_new_eldership: None,
        parent: None,
    }));

    while let Some(mut current) = queue.pop_front() {
        let distance = current.distance();
        if let Some(prev) = prev_sol_distance
            && prev + 5 <= distance
            && (prev as f64) * 1.3 <= distance as f64
        {
            break;
        }

        let path_hops = get_path_hops(&current);
        let step = match router
            .send_search_request(path_hops, max_host_lvl, reserve_request_id)
            .await
        {
            Ok(step) => step,
            Err(_) => {
                visited.retain(|t| !positions_equal(t, &current.visiting_gnode));
                continue;
            }
        };
        if step.min_host_lvl > levels || step.min_host_lvl > max_host_lvl {
            continue;
        }

        current = Arc::new(SolutionStep {
            visiting_gnode: current
                .visiting_gnode
                .truncate_to_level(step.min_host_lvl, levels),
            previous_migrating_gnode: current.previous_migrating_gnode.clone(),
            previous_gnode_new_conn_vir_pos: current.previous_gnode_new_conn_vir_pos,
            previous_gnode_new_eldership: current.previous_gnode_new_eldership,
            parent: current.parent.clone(),
        });

        let resolved = step
            .real_new_pos
            .zip(step.real_new_eldership)
            .map(|(pos, eldership)| MigrationSolution {
                leaf: current.clone(),
                final_host_lvl: step.final_host_lvl,
                real_new_pos: pos,
                real_new_eldership: eldership,
            });

        if step.final_host_lvl <= ok_host_lvl {
            if let Some(sol) = resolved {
                solutions.push(sol);
                return solutions;
            }
            // See this function's doc comment: no real position resolved at
            // an early-accept-eligible step — keep searching instead of
            // returning an empty/incomplete result.
        } else if step.min_host_lvl == step.final_host_lvl {
            if let Some(sol) = resolved {
                prev_sol_distance = Some(sol.distance());
                max_host_lvl = step.final_host_lvl - 1;
                solutions.push(sol);
            }
            continue;
        } else if step.final_host_lvl <= max_host_lvl
            && let Some(sol) = resolved
        {
            prev_sol_distance = Some(sol.distance());
            max_host_lvl = step.final_host_lvl - 1;
            solutions.push(sol);
        }

        for adj in &step.set_adjacent {
            let mut n = adj.gnode.clone();
            if n.level(levels) > step.min_host_lvl {
                let mut explore_hops = get_path_hops(&current);
                explore_hops.push(PathHop {
                    visiting_gnode: n.clone(),
                    previous_migrating_gnode: None,
                });
                match router
                    .send_explore_request(explore_hops, step.min_host_lvl)
                    .await
                {
                    Ok(resolved_n) => n = resolved_n,
                    Err(_) => continue,
                }
            }
            if n.level(levels) != step.min_host_lvl {
                continue;
            }
            if tuple_has_virtual_pos(&n, view) {
                continue;
            }
            if visited.iter().any(|t| positions_equal(t, &n)) {
                continue;
            }

            let mut in_prev_step = false;
            let mut prev_step = Some(current.clone());
            while let Some(step_ref) = prev_step {
                let bigger = step_ref
                    .visiting_gnode
                    .truncate_to_level(step.min_host_lvl, levels);
                if positions_equal(&bigger, &n) {
                    in_prev_step = true;
                    break;
                }
                prev_step = step_ref.parent.clone();
            }
            if in_prev_step {
                continue;
            }

            visited.push(n.clone());
            let mut previous_migrating_gnode = current.visiting_gnode.clone();
            previous_migrating_gnode.pos.insert(0, adj.border_real_pos);
            previous_migrating_gnode.eldership.insert(0, -1);
            queue.push_back(Arc::new(SolutionStep {
                visiting_gnode: n,
                previous_migrating_gnode: Some(previous_migrating_gnode),
                previous_gnode_new_conn_vir_pos: step.new_conn_vir_pos,
                previous_gnode_new_eldership: step.new_eldership,
                parent: Some(current.clone()),
            }));
        }
    }
    solutions
}

/// One resolved migration hop — `MigData` (`structs.vala:187-197`).
#[derive(Clone, Debug)]
struct MigData {
    migration_id: i32,
    mig_gnode: TupleGNode,
    conn_gnode_pos: i32,
    prev_mig_gnode_new_eldership: i32,
    host_gnode: TupleGNode,
    mig_gnode_new_pos: Option<u32>,
    final_mig_gnode_new_pos: Option<u32>,
    final_mig_gnode_new_eldership: Option<i32>,
}

/// `get_migs` (`structs.vala:199-237`): walks `sol.leaf`'s parent chain
/// (farthest hop first, root excluded) building one [`MigData`] per hop.
///
/// # Panics
/// If `sol.leaf` is the BFS root (`sol.leaf.parent.is_none()`) — matches
/// upstream's own `assert(sol.leaf.parent != null)`; callers only reach
/// `get_migs` when `sol.distance() > 0` (see `search_migration_path`'s
/// direct-access short-circuit, [`crate::rpc`]).
fn get_migs(sol: &MigrationSolution, levels: usize) -> Vec<MigData> {
    assert!(
        sol.leaf.parent.is_some(),
        "get_migs called on a distance-0 solution"
    );
    let mut migs = Vec::new();
    let mut last = true;
    let mut ss_prev: Option<Arc<SolutionStep>> = None;
    let mut ss = sol.leaf.clone();
    while let Some(parent) = ss.parent.clone() {
        let mut host_gnode = ss.visiting_gnode.clone();
        if last {
            let toremove = host_gnode.pos.len() - (levels - sol.final_host_lvl);
            host_gnode.pos.drain(0..toremove);
            host_gnode.eldership.drain(0..toremove);
        }
        let mig_gnode_new_pos = ss_prev
            .as_ref()
            .and_then(|p| p.previous_migrating_gnode.as_ref())
            .and_then(|t| t.pos.first().copied());
        migs.insert(
            0,
            MigData {
                migration_id: crate::idgen::next_i32(),
                mig_gnode: ss
                    .previous_migrating_gnode
                    .clone()
                    .expect("every non-root SolutionStep carries a previous_migrating_gnode"),
                conn_gnode_pos: ss.previous_gnode_new_conn_vir_pos.map_or(-1, |p| p as i32),
                prev_mig_gnode_new_eldership: ss.previous_gnode_new_eldership.unwrap_or(-1),
                host_gnode,
                mig_gnode_new_pos,
                final_mig_gnode_new_pos: last.then_some(sol.real_new_pos),
                final_mig_gnode_new_eldership: last.then_some(sol.real_new_eldership),
            },
        );
        last = false;
        ss_prev = Some(ss.clone());
        ss = parent;
    }
    migs
}

/// `pkt_id`/`src` are left at their placeholder default here — the
/// [`SearchRouter`] implementation fills them in only when a hop actually
/// needs to cross the wire (`send_mig_request`, `message_routing.vala:819-823`
/// sets them the same way, only for the remote case).
fn build_request_packet_prepare(mig: &MigData) -> RequestPacket {
    RequestPacket {
        pkt_id: 0,
        dest: mig.mig_gnode.clone(),
        src: TupleGNode::default(),
        operation: MigOp::PrepareMigration,
        migration_id: mig.migration_id,
        conn_gnode_pos: -1,
        host_gnode: TupleGNode::default(),
        real_new_pos: -1,
        real_new_eldership: -1,
    }
}

fn build_request_packet_finish(mig: &MigData, mig_next: Option<&MigData>) -> RequestPacket {
    let (real_new_pos, real_new_eldership) = match mig_next {
        None => (
            i32::try_from(
                mig.final_mig_gnode_new_pos
                    .expect("the farthest migration hop always resolves a final position"),
            )
            .unwrap_or(-1),
            mig.final_mig_gnode_new_eldership
                .expect("the farthest migration hop always resolves a final eldership"),
        ),
        Some(next) => (
            mig.mig_gnode_new_pos.map_or(-1, |p| p as i32),
            next.prev_mig_gnode_new_eldership,
        ),
    };
    RequestPacket {
        pkt_id: 0,
        dest: mig.mig_gnode.clone(),
        src: TupleGNode::default(),
        operation: MigOp::FinishMigration,
        migration_id: mig.migration_id,
        conn_gnode_pos: mig.conn_gnode_pos,
        host_gnode: mig.host_gnode.clone(),
        real_new_pos,
        real_new_eldership,
    }
}

/// `execute_shortest_mig` (`hooking.vala:466-490`): sends
/// `PREPARE_MIGRATION` to every hop farthest-first, then `FINISH_MIGRATION`
/// starting from the farthest, propagating each hop's real new
/// position/eldership inward.
///
/// # Errors
/// [`RoutingError`] if any hop is unreachable — `MigrationPathExecuteFailureError`.
pub async fn execute_shortest_mig(
    view: &dyn QspnView,
    router: &dyn SearchRouter,
    sol: &MigrationSolution,
) -> Result<(), RoutingError> {
    let levels = view.topology().levels();
    let migs = get_migs(sol, levels);

    for mig in migs.iter().rev() {
        router
            .send_mig_request(build_request_packet_prepare(mig))
            .await?;
    }
    let farthest = migs
        .last()
        .expect("get_migs never returns an empty list for distance > 0");
    router
        .send_mig_request(build_request_packet_finish(farthest, None))
        .await?;
    for i in (0..migs.len().saturating_sub(1)).rev() {
        router
            .send_mig_request(build_request_packet_finish(&migs[i], Some(&migs[i + 1])))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    /// A hand-built oracle: every g-node's `execute_search`/`execute_explore`
    /// response is a fixed, precomputed table entry keyed by position
    /// vector — no coordinator/QSPN traits, no RPC transport, exercising the
    /// BFS algorithm alone.
    #[derive(Default)]
    struct Oracle {
        search: StdHashMap<Vec<u32>, SearchStepResult>,
        explore: StdHashMap<(Vec<u32>, usize), TupleGNode>,
        unreachable: Vec<Vec<u32>>,
    }

    impl SearchRouter for Oracle {
        fn send_search_request(
            &self,
            path_hops: Vec<PathHop>,
            _max_host_lvl: usize,
            _reserve_request_id: i32,
        ) -> BoxFuture<'_, Result<SearchStepResult, RoutingError>> {
            let target = path_hops.last().unwrap().visiting_gnode.pos.clone();
            Box::pin(async move {
                if self.unreachable.contains(&target) {
                    return Err(RoutingError);
                }
                self.search.get(&target).cloned().ok_or(RoutingError)
            })
        }

        fn send_explore_request(
            &self,
            path_hops: Vec<PathHop>,
            requested_lvl: usize,
        ) -> BoxFuture<'_, Result<TupleGNode, RoutingError>> {
            let target = path_hops.last().unwrap().visiting_gnode.pos.clone();
            Box::pin(async move {
                self.explore
                    .get(&(target, requested_lvl))
                    .cloned()
                    .ok_or(RoutingError)
            })
        }

        fn send_delete_reserve_request(&self, _dest_gnode: TupleGNode, _reserve_request_id: i32) {}

        fn send_mig_request(
            &self,
            _packet: RequestPacket,
        ) -> BoxFuture<'_, Result<(), RoutingError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct FixedView {
        topology: ntk_common::Topology,
        my_pos: Vec<u32>,
        subnetlevel: usize,
    }
    impl QspnView for FixedView {
        fn topology(&self) -> &ntk_common::Topology {
            &self.topology
        }
        fn network_id(&self) -> i64 {
            0
        }
        fn n_nodes(&self) -> u64 {
            1
        }
        fn my_pos(&self, level: usize) -> u32 {
            self.my_pos[level]
        }
        fn my_eldership(&self, _level: usize) -> i32 {
            0
        }
        fn subnetlevel(&self) -> usize {
            self.subnetlevel
        }
        fn epsilon(&self, _level: usize) -> usize {
            0
        }
        fn eldership(&self, _level: usize, _pos: u32) -> i32 {
            0
        }
        fn adjacent_to_my_gnode(
            &self,
            _level_adjacent_gnodes: usize,
            _level_my_gnode: usize,
        ) -> Vec<crate::view::AdjacentGNode> {
            Vec::new()
        }
        fn is_bootstrapped(&self) -> bool {
            true
        }
    }

    fn view_at(my_pos: Vec<u32>) -> FixedView {
        FixedView {
            topology: ntk_common::Topology::new([4, 4, 4]).unwrap(),
            my_pos,
            subnetlevel: 0,
        }
    }

    #[tokio::test]
    async fn direct_hit_at_first_host_level_needs_no_further_exploration() {
        // levels=3, gsizes=[4,4,4]; I am [pos0=1,pos1=2,pos2=3].
        // Joining at ask_lvl=0 => first_host_lvl=1, root gnode = my tuple
        // from level 1 = pos [2,3] (levels 1,2).
        let view = view_at(vec![1, 2, 3]);
        let root_pos = vec![2, 3];
        let mut oracle = Oracle::default();
        oracle.search.insert(
            root_pos.clone(),
            SearchStepResult {
                min_host_lvl: 1,
                final_host_lvl: 1,
                real_new_pos: Some(1),
                real_new_eldership: Some(7),
                set_adjacent: Vec::new(),
                new_conn_vir_pos: None,
                new_eldership: None,
            },
        );

        let solutions = find_shortest_mig(&view, &oracle, 42, 1, 1).await;
        assert_eq!(solutions.len(), 1);
        assert_eq!(solutions[0].distance(), 0);
        assert_eq!(solutions[0].final_host_lvl, 1);
        assert_eq!(solutions[0].real_new_pos, 1);
        assert_eq!(solutions[0].real_new_eldership, 7);
    }

    #[tokio::test]
    async fn climbs_to_an_adjacent_gnode_when_root_is_full() {
        let view = view_at(vec![1, 2, 3]);
        let root_pos = vec![2, 3];
        let adjacent = TupleGNode::new(vec![0, 3], vec![0, 0]);
        let mut oracle = Oracle::default();
        // Root: virtual only (gsize exhausted at level 1), must explore adjacent.
        oracle.search.insert(
            root_pos.clone(),
            SearchStepResult {
                min_host_lvl: 1,
                final_host_lvl: 2,
                real_new_pos: None,
                real_new_eldership: None,
                set_adjacent: vec![PairTupleGNodeInt {
                    gnode: adjacent.clone(),
                    border_real_pos: 5,
                }],
                new_conn_vir_pos: Some(4),
                new_eldership: Some(11),
            },
        );
        oracle.search.insert(
            adjacent.pos.clone(),
            SearchStepResult {
                min_host_lvl: 1,
                final_host_lvl: 1,
                real_new_pos: Some(2),
                real_new_eldership: Some(3),
                set_adjacent: Vec::new(),
                new_conn_vir_pos: None,
                new_eldership: None,
            },
        );

        let solutions = find_shortest_mig(&view, &oracle, 1, 1, 1).await;
        assert_eq!(solutions.len(), 1);
        assert_eq!(solutions[0].distance(), 1);
        assert_eq!(solutions[0].final_host_lvl, 1);
        assert_eq!(solutions[0].real_new_pos, 2);
    }

    #[tokio::test]
    async fn no_solution_when_every_candidate_is_unreachable() {
        let view = view_at(vec![1, 2, 3]);
        let mut oracle = Oracle::default();
        oracle.unreachable.push(vec![2, 3]);

        let solutions = find_shortest_mig(&view, &oracle, 1, 1, 1).await;
        assert!(solutions.is_empty());
    }

    #[tokio::test]
    async fn no_solution_when_root_never_resolves_a_real_position() {
        let view = view_at(vec![1, 2, 3]);
        let mut oracle = Oracle::default();
        oracle.search.insert(
            vec![2, 3],
            SearchStepResult {
                min_host_lvl: 1,
                final_host_lvl: 4,
                real_new_pos: None,
                real_new_eldership: None,
                set_adjacent: Vec::new(),
                new_conn_vir_pos: None,
                new_eldership: None,
            },
        );
        let solutions = find_shortest_mig(&view, &oracle, 1, 1, 1).await;
        assert!(solutions.is_empty());
    }

    // ---------------------------------------------------------------------------------------
    // Regression: concurrent real entrants into the same g-node must never collide on a
    // position — the invariant `crates/ntk-hooking/src/idgen.rs`'s data race broke, and
    // `crates/ntkd/tests/mesh.rs`'s `two_star_groups_merge_into_one_network` caught in the
    // real daemon (`crates/ntk-coordinator/src/actor.rs::reserve_enter`, idempotent-by-
    // `reserve_request_id`, is deliberately indistinguishable from a genuine retry when two
    // different entrants collide on that id).
    // ---------------------------------------------------------------------------------------

    /// Runs `n` concurrent entrants, each minting its own `reserve_request_id` via the crate's
    /// real (not scripted) [`crate::idgen::next_i32`] on its own `tokio::spawn`'d task, all
    /// asking the same [`FakeCoordinatorClient`] to reserve a position in the same one-level,
    /// `gsize = 8` g-node (the topology shape of `crates/ntkd/tests/mesh.rs`'s `MERGE_GSIZES`)
    /// — the exact concurrency shape of several arc handlers (or inbound
    /// `search_migration_path` RPCs) racing on a real multi-threaded Tokio runtime. Returns
    /// every granted real position; panics if `n` exceeds the g-node's 8 free slots.
    async fn run_concurrent_entrants(n: usize) -> Vec<u32> {
        let view: Arc<dyn QspnView> = Arc::new(FixedView {
            topology: ntk_common::Topology::new([8]).unwrap(),
            my_pos: vec![0],
            subnetlevel: 0,
        });
        let coord: Arc<dyn CoordinatorClient> =
            Arc::new(crate::fake::FakeCoordinatorClient::new(1));
        // The root/topmost g-node: an empty `pos` names `level(levels) == levels == 1`, so
        // `i_am_inside` holds trivially (every identity is inside the whole network).
        let visiting_gnode = TupleGNode::new(Vec::new(), Vec::new());

        let mut tasks = Vec::with_capacity(n);
        for _ in 0..n {
            let view = Arc::clone(&view);
            let coord = Arc::clone(&coord);
            let visiting_gnode = visiting_gnode.clone();
            tasks.push(tokio::spawn(async move {
                let reserve_request_id = crate::idgen::next_i32();
                execute_search(
                    view.as_ref(),
                    coord.as_ref(),
                    &visiting_gnode,
                    1,
                    reserve_request_id,
                )
                .await
                .expect("a free slot exists for every entrant up to gsize")
                .real_new_pos
                .expect("gsize=8 with at most 8 entrants always resolves a real position")
            }));
        }
        let mut positions = Vec::with_capacity(n);
        for t in tasks {
            positions.push(t.await.expect("entrant task panicked"));
        }
        positions
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_entrants_never_collide_on_a_position() {
        let positions = run_concurrent_entrants(8).await;
        let unique: std::collections::HashSet<u32> = positions.iter().copied().collect();
        assert_eq!(
            unique.len(),
            positions.len(),
            "{} of {} concurrent entrants into the same g-node were granted a duplicate \
             position: {positions:?} — exactly the deadlock `crates/ntkd/tests/mesh.rs`'s \
             `two_star_groups_merge_into_one_network` caught in the real daemon",
            positions.len() - unique.len(),
            positions.len(),
        );
    }

    proptest::proptest! {
        /// The same invariant, over the number of concurrent entrants (bounded by the g-node's
        /// own 8 free slots — beyond that, running out of real positions is the *expected*,
        /// different outcome this test isn't about).
        #[test]
        fn concurrent_entrants_up_to_gsize_never_collide(n in 2usize..=8usize) {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(n)
                .enable_all()
                .build()
                .expect("build a multi-thread runtime for real concurrent entrants");
            let positions = rt.block_on(run_concurrent_entrants(n));
            let unique: std::collections::HashSet<u32> = positions.iter().copied().collect();
            proptest::prop_assert_eq!(unique.len(), positions.len());
        }
    }
}
