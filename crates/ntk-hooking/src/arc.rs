//! `ArcId` plus the per-arc state machine — `ArcHandler.add_arc_tasklet`
//! (`research/impl/vala/hooking/arc_handler.vala:62-359`): one independent
//! task per identity-arc, driven purely by that arc's peer. Each arc's task
//! sequentially awaits its own outbound calls; this is safe precisely
//! because it is *not* the Manager actor's own command loop (see
//! `research/notes/06-rust-stack.md` §Concurrency and
//! `crate::manager`'s module docs) — N independent arc tasks never block
//! each other or the central actor.

use ntk_proto::v1::ErrorDomain;
use ntk_rpc::RpcError;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::HookingConfig;
use crate::coordinator::{CoordinatorClient, CoordinatorError, MergeArbitrationRequest};
use crate::domain::{EvaluateEnterRequest, FinishEnterData};
use crate::events::HookingEvent;
use crate::manager::HookingHandle;
use crate::merge::{MergeDecision, merge_direction};
use crate::snapshot::ArcPhase;
use crate::stub::{HookingStub, HookingStubFactory};
use crate::view::QspnView;
use std::sync::Arc;

/// Opaque identifier for one identity-arc — the Rust replacement for
/// `IIdentityArc` object identity (`api.vala:86-89`). Minted and owned by
/// whoever composes Hooking with Neighborhood/Identities (out of this
/// crate's scope); this crate only ever treats it as an opaque key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArcId(pub u64);

/// Bound on quick retries for [`CoordinatorError::Unreachable`] from
/// `evaluate_enter` specifically. Upstream aborts the whole arc handler on
/// `CoordProxyError`/`UnknownResultError` outright (`arc_handler.vala:230-235`)
/// because by the time that error crosses upstream's own RPC layer, it has
/// already exhausted transport-level retries and genuinely means "broken".
/// This port's `Unreachable` can also fire for a purely local reason
/// upstream never has to model: `evaluate_enter` is the very first DHT
/// round trip after a brand-new arc is discovered, and the outbound route
/// to the target g-node depends on `ntk-qspn`'s own destination table
/// having processed that arc — a convergence step that can still be one
/// tick behind the arc-handler's own, much faster, direct link-level
/// `retrieve_network_data` exchange (real-kernel trace: `contact_peer`
/// returns "no candidate" ~1ms after `add_arc`, before the next SPF
/// recompute registers the neighbor as a destination). A handful of quick
/// retries bridges exactly that narrow window without weakening upstream's
/// eventual-abort semantics for a target that is genuinely unreachable.
const EVALUATE_ENTER_UNREACHABLE_RETRIES: u32 = 5;

/// Everything one arc's handler task needs, bundled so [`crate::manager`]
/// can spawn one of these per arc without a long parameter list.
#[derive(Clone)]
pub(crate) struct ArcHandlerCtx {
    pub view: Arc<dyn QspnView>,
    pub coord: Arc<dyn CoordinatorClient>,
    pub stubs: Arc<dyn HookingStubFactory>,
    pub config: HookingConfig,
    pub handle: HookingHandle,
}

/// RAII release for [`HookingHandle::try_begin_commit`]'s exclusive slot: dropped on every
/// exit from the commit phase (success, error, or a `continue 'outer` redo), so a stuck or
/// aborted commit never permanently blocks this identity's other arcs.
struct CommitGuard<'a>(&'a HookingHandle);

impl Drop for CommitGuard<'_> {
    fn drop(&mut self) {
        self.0.end_commit();
    }
}

/// RAII release for an accepted `evaluate_enter` slot (`EnterArbiter`'s in-flight map, keyed
/// by level — `crates/ntkd/src/node/adapters.rs`): created the moment `evaluate_enter`
/// answers `Accepted`, normally consumed by the first successful [`Self::complete`]/
/// [`Self::abort`] call for the level it holds. Every *other* exit from here on — an early
/// `return` on a proxy error, a `continue 'outer` redo, or (the case no call site can cover)
/// this whole task being aborted/dropped mid-episode when its owning actor's `JoinSet` tears
/// down (`crate::manager::Actor::run`'s own shutdown path calls `JoinSet::shutdown`, which
/// aborts any arc task still parked inside a plain RPC `.await`, not only one that happens to
/// be checking cancellation) — drops this guard instead. `Drop` cannot itself await the
/// network round trip a release needs, so it hands the release to a detached best-effort
/// task; the arbiter's own release handlers are idempotent (`remove` on a map), so retrying
/// is safe even when a manual call already succeeded or never reached the server at all.
struct EnterGuard {
    coord: Arc<dyn CoordinatorClient>,
    level: usize,
    resolved: bool,
}

impl EnterGuard {
    fn new(coord: Arc<dyn CoordinatorClient>, level: usize) -> Self {
        Self {
            coord,
            level,
            resolved: false,
        }
    }

    /// Marks this guard resolved iff `level` is the one it holds and the release actually
    /// succeeded — a failed call leaves it armed so `Drop`'s fallback retries; a call for a
    /// *different* level (the search loop's own post-decrement `abort_enter` at a level this
    /// guard never held) is passed straight through without touching the guard's state.
    fn note_released(&mut self, level: usize, ok: bool) {
        if ok && level == self.level {
            self.resolved = true;
        }
    }

    async fn complete(&mut self, level: usize) -> Result<(), CoordinatorError> {
        let result = self.coord.completed_enter(level).await;
        self.note_released(level, result.is_ok());
        result
    }

    async fn abort(&mut self, level: usize) -> Result<(), CoordinatorError> {
        let result = self.coord.abort_enter(level).await;
        self.note_released(level, result.is_ok());
        result
    }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let coord = self.coord.clone();
        let level = self.level;
        tokio::spawn(async move {
            let _ = coord.abort_enter(level).await;
        });
    }
}

/// `i_am_real` (`arc_handler.vala:54-60`): true iff every one of my own
/// positions is non-virtual — i.e. I am a genuine (not connectivity)
/// identity.
fn i_am_real(view: &dyn QspnView) -> bool {
    (0..view.topology().levels()).all(|i| view.my_pos(i) < view.topology().gsize(i).unwrap_or(0))
}

/// Sleeps `d`, returning `true` if `cancel` fired first (in which case the
/// caller must stop, not continue the protocol).
async fn sleep_or_cancelled(d: std::time::Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(d) => false,
    }
}

fn remote_domain(err: &RpcError) -> Option<ErrorDomain> {
    match err {
        RpcError::Remote(e) => ErrorDomain::try_from(e.domain).ok(),
        _ => None,
    }
}

/// `ArcHandler.add_arc_tasklet` (`arc_handler.vala:91-358`).
pub(crate) async fn run_arc_handler(ctx: ArcHandlerCtx, arc: ArcId, cancel: CancellationToken) {
    let stub: Arc<dyn HookingStub> = ctx.stubs.arc_stub(arc);

    'outer: loop {
        if cancel.is_cancelled() {
            return;
        }

        // (1) connectivity identities never hook (arc_handler.vala:95-99).
        if !i_am_real(ctx.view.as_ref()) {
            ctx.handle.set_arc_phase(arc, ArcPhase::Connectivity);
            return;
        }

        // (2) retrieve_network_data(false) (arc_handler.vala:100-149).
        ctx.handle.set_arc_phase(arc, ArcPhase::Discovering);
        let network_data = loop {
            match stub.retrieve_network_data(false).await {
                Ok(nd) => break nd,
                Err(e) => match remote_domain(&e) {
                    Some(ErrorDomain::NotBootstrapped) => {
                        if sleep_or_cancelled(ctx.config.not_bootstrapped_retry, &cancel).await {
                            return;
                        }
                        continue;
                    }
                    Some(ErrorDomain::HookingNotPrincipal) => {
                        ctx.handle.set_arc_phase(arc, ArcPhase::Connectivity);
                        return;
                    }
                    _ => {
                        warn!(?arc, error = %e, "hooking arc: bad arc on retrieve_network_data");
                        ctx.handle.emit(HookingEvent::FailingArc(arc));
                        ctx.handle.set_arc_phase(arc, ArcPhase::Failed);
                        return;
                    }
                },
            }
        };

        let levels = ctx.view.topology().levels();

        // (3) same network -> done (arc_handler.vala:124-129).
        if network_data.network_id == ctx.view.network_id() {
            for (level, &pos) in network_data.neighbor_pos.iter().enumerate().take(levels) {
                ctx.view.note_same_network(level, pos);
            }
            ctx.handle.emit(HookingEvent::SameNetwork(arc));
            ctx.handle.set_arc_phase(arc, ArcPhase::SameNetwork);
            return;
        }

        // incompatible topology -> terminate (arc_handler.vala:130-149).
        let my_gsizes: Vec<u32> = (0..levels)
            .map(|i| ctx.view.topology().gsize(i).unwrap_or(0))
            .collect();
        if network_data.gsizes != my_gsizes {
            ctx.handle
                .set_arc_phase(arc, ArcPhase::IncompatibleTopology);
            return;
        }

        // A not-yet-merged foreign neighbor is otherwise silently counted by `n_nodes()`/
        // `free_positions()` as if it were already one of my own members the moment the
        // underlying QSPN arc becomes reachable — `ntk_qspn` has no notion of network
        // boundaries, so it happily maps a foreign peer's own position into its destination set
        // regardless of whether hooking has resolved anything (a real, reproduced size
        // inflation, not a hypothetical: this crate's own concurrent-arc test coverage showed
        // it feeding straight into `crate::merge::merge_tiebreak` and making the tiebreak
        // decision itself unstable). Record it as foreign *before* any size/capacity read below
        // ever consults it.
        for (level, &pos) in network_data.neighbor_pos.iter().enumerate().take(levels) {
            ctx.view.note_foreign(level, pos);
        }
        ctx.handle.emit(HookingEvent::AnotherNetwork {
            arc,
            network_id: network_data.network_id,
        });

        // (4) merge-direction heuristic (arc_handler.vala:150-214).
        let mut network_data = network_data;
        let proceed = match merge_direction(ctx.view.n_nodes(), network_data.neighbor_n_nodes) {
            MergeDecision::Proceed => true,
            MergeDecision::Wait => false,
            MergeDecision::AskCoordinator => {
                match stub.retrieve_network_data(true).await {
                    Ok(nd) => {
                        network_data = nd;
                    }
                    Err(e) => match remote_domain(&e) {
                        Some(ErrorDomain::HookingNotPrincipal) => {
                            ctx.handle.set_arc_phase(arc, ArcPhase::Connectivity);
                            return;
                        }
                        _ => {
                            warn!(?arc, error = %e, "hooking arc: bad arc on authoritative retrieve_network_data");
                            ctx.handle.emit(HookingEvent::FailingArc(arc));
                            ctx.handle.set_arc_phase(arc, ArcPhase::Failed);
                            return;
                        }
                    },
                }
                // Routed through the Coordinator (`CoordinatorClient::decide_merge`'s own
                // docs) so this decision is made once per target network and shared, for a
                // bounded freshness window, by every member of my own g-node, instead of each
                // arc recomputing `merge_tiebreak` against its own, potentially
                // differently-timed, sampling of `n_nodes()`.
                ctx.coord
                    .decide_merge(MergeArbitrationRequest {
                        my_network_id: ctx.view.network_id(),
                        neighbor_network_id: network_data.network_id,
                        neighbor_n_nodes: network_data.neighbor_n_nodes,
                    })
                    .await
            }
        };
        if !proceed {
            ctx.handle.set_arc_phase(arc, ArcPhase::Waiting);
            if sleep_or_cancelled(ctx.config.merge_reject_wait, &cancel).await {
                return;
            }
            continue 'outer;
        }

        // Serialize this identity's own commit attempts across its own concurrently-
        // negotiating arcs (`HookingHandle::try_begin_commit`'s doc): without this, two arcs
        // independently deciding `proceed=true` against two *different* foreign networks would
        // each reserve a real position on their own target concurrently, and whichever lost the
        // resulting race to `finish_enter` would leave its own reservation to leak until the
        // Coordinator's booking TTL reclaims it — with enough concurrent arcs (this crate's own
        // concurrent-arc test coverage routinely has 4-5 per identity) that starves the target's
        // whole address space faster than TTLs can free it. Gating *before* any reservation is
        // ever made, not just before the final local commit, is what actually avoids this: at
        // most one reservation per identity is ever outstanding. Losing this race is a decisive
        // "someone else got there first" — back off exactly like the `Wait` branch above; by the
        // time this arc redoes the whole decision, `SameNetwork` will fire if this identity
        // itself already committed elsewhere in the meantime.
        if !ctx.handle.try_begin_commit().await {
            ctx.handle.set_arc_phase(arc, ArcPhase::Waiting);
            if sleep_or_cancelled(ctx.config.merge_reject_wait, &cancel).await {
                return;
            }
            continue 'outer;
        }
        let _commit_guard = CommitGuard(&ctx.handle);

        // (5) network-wide evaluation (arc_handler.vala:216-248).
        ctx.handle.set_arc_phase(arc, ArcPhase::Evaluating);
        let evaluate_req = EvaluateEnterRequest {
            network_id: network_data.network_id,
            neighbor_pos: network_data.neighbor_pos.clone(),
            neighbor_min_lvl: network_data.neighbor_min_level,
            min_lvl: ctx.view.subnetlevel(),
            evaluate_enter_id: crate::idgen::next_i32(),
        };
        let mut unreachable_retries = 0u32;
        let ask_lvl = loop {
            match ctx.coord.evaluate_enter(evaluate_req.clone()).await {
                Ok(lvl) => break lvl,
                Err(CoordinatorError::AskAgain) => {
                    info!(
                        ?arc,
                        network_id = evaluate_req.network_id,
                        evaluate_enter_id = evaluate_req.evaluate_enter_id,
                        "hooking arc: evaluate_enter -> AskAgain, retrying same id"
                    );
                    if sleep_or_cancelled(ctx.config.ask_again_wait(ctx.view.n_nodes()), &cancel)
                        .await
                    {
                        return;
                    }
                    continue;
                }
                Err(CoordinatorError::IgnoreNetwork) => {
                    info!(
                        ?arc,
                        network_id = evaluate_req.network_id,
                        evaluate_enter_id = evaluate_req.evaluate_enter_id,
                        "hooking arc: evaluate_enter -> IgnoreNetwork, redoing from start with a fresh id"
                    );
                    if sleep_or_cancelled(ctx.config.restart_wait(ctx.view.n_nodes()), &cancel)
                        .await
                    {
                        return;
                    }
                    continue 'outer;
                }
                Err(CoordinatorError::Unreachable(_))
                    if unreachable_retries < EVALUATE_ENTER_UNREACHABLE_RETRIES =>
                {
                    unreachable_retries += 1;
                    info!(
                        ?arc,
                        attempt = unreachable_retries,
                        "hooking arc: evaluate_enter transiently unreachable (qspn route not yet \
                         converged), retrying briefly"
                    );
                    if sleep_or_cancelled(ctx.config.not_bootstrapped_retry, &cancel).await {
                        return;
                    }
                    continue;
                }
                Err(e) => {
                    warn!(?arc, error = %e, "hooking arc: evaluate_enter proxy error, aborting");
                    return;
                }
            }
        };

        // RAII release for the slot `evaluate_enter` just granted — see `EnterGuard`'s own
        // doc: every exit from here on, including this task being aborted mid-episode, drops
        // it and guarantees at least one `abort_enter` attempt even if no call site below
        // ever runs.
        let mut enter_guard = EnterGuard::new(ctx.coord.clone(), ask_lvl);

        // (6) begin/search loop (arc_handler.vala:250-334).
        let mut ask_lvl = ask_lvl;
        let entry_data = 'begin: loop {
            ctx.handle
                .set_arc_phase(arc, ArcPhase::Entering { ask_lvl });
            match ctx.coord.begin_enter(ask_lvl).await {
                Ok(()) => {}
                Err(CoordinatorError::AlreadyEntering) => {
                    if sleep_or_cancelled(ctx.config.restart_wait(ctx.view.n_nodes()), &cancel)
                        .await
                    {
                        return;
                    }
                    continue 'outer;
                }
                Err(e) => {
                    warn!(?arc, error = %e, "hooking arc: begin_enter proxy error, aborting");
                    return;
                }
            }

            loop {
                match stub.search_migration_path(ask_lvl).await {
                    Ok(entry) => break 'begin entry,
                    Err(e) => match remote_domain(&e) {
                        Some(ErrorDomain::NoMigrationPathFound) => {
                            if let Err(err) = enter_guard.abort(ask_lvl).await {
                                warn!(?arc, error = %err, "hooking arc: abort_enter proxy error, aborting");
                                return;
                            }
                            if ask_lvl == 0 {
                                warn!(
                                    ?arc,
                                    "hooking arc: no migration path for a single node in a brand-new network"
                                );
                                if sleep_or_cancelled(
                                    ctx.config.restart_wait(ctx.view.n_nodes()),
                                    &cancel,
                                )
                                .await
                                {
                                    return;
                                }
                                continue 'outer;
                            }
                            ask_lvl -= 1;
                            continue 'begin;
                        }
                        Some(ErrorDomain::MigrationPathExecuteFailure) => continue,
                        _ => {
                            warn!(?arc, error = %e, "hooking arc: bad arc on search_migration_path");
                            ctx.handle.emit(HookingEvent::FailingArc(arc));
                            ctx.handle.set_arc_phase(arc, ArcPhase::Failed);
                            return;
                        }
                    },
                }
            }
        };

        // The target network's own identity is not a stable transaction boundary: the
        // responder answering `search_migration_path` reports its *current*
        // `view.network_id()` (`crate::search::resolve_entry_data`), which can have changed
        // since `network_data.network_id` was captured (this same target merging into a THIRD,
        // still-larger network while our own negotiation was in flight — a real race in this
        // crate's own concurrent-arc test coverage, not a hypothetical). Blindly committing to
        // `entry_data` in that case adopts a position/eldership chain resolved against a
        // network identity we never actually evaluated or agreed to enter — exactly the
        // mechanism that let two symmetric merges each conclude they were the loser. Treat a
        // mismatch as "the target changed under us": release the reservation and redo the
        // whole decision from scratch against fresh data.
        if entry_data.network_id != network_data.network_id {
            warn!(
                ?arc,
                decided_network_id = network_data.network_id,
                entry_network_id = entry_data.network_id,
                "hooking arc: target network changed during entry, aborting and redoing from start"
            );
            if let Err(err) = enter_guard.abort(ask_lvl).await {
                warn!(?arc, error = %err, "hooking arc: abort_enter proxy error, aborting");
                return;
            }
            if sleep_or_cancelled(ctx.config.restart_wait(ctx.view.n_nodes()), &cancel).await {
                return;
            }
            continue 'outer;
        }

        // At `ask_lvl >= 1` this g-node has other members; while this arc's own
        // `search_migration_path` round trip was in flight, a sibling's own negotiation may
        // already have carried the whole g-node into this exact target network (this crate's
        // collective-destination propagation: `ntkd::node::lifecycle`'s `DoFinishEnter` handler
        // combines a propagated `finish_enter`'s target with each member's own retained
        // lower-level position and applies it immediately, for every member, not only the one
        // that negotiated). The coordinator's per-(g-node, level) exclusivity bounds how many
        // members can be *mid*-negotiation at once; it does not forbid a second, later
        // negotiation from completing after an earlier one already resolved the g-node — this
        // state machine relies on that elsewhere (repeatable migration). Completing this one
        // anyway would not be wrong (the resulting position is data, and a mismatched one would
        // just drive one more, self-correcting migration, exactly as an unrelated later merge
        // would), but it is a needless second `completed_enter`/reserve round trip for a g-node
        // that has already arrived — so abort instead and let the outer loop redo from
        // `retrieve_network_data`, which now observes `SameNetwork` and terminates cleanly.
        // Gated to `ask_lvl >= 1`: a level-0 g-node has exactly one member (this identity), so
        // nothing else could ever have carried it anywhere while this arc was busy, and this
        // branch must never engage there (see this crate's own `ask_lvl == 0` scope note).
        if ask_lvl >= 1 && ctx.view.network_id() == entry_data.network_id {
            debug!(
                ?arc,
                ask_lvl,
                network_id = entry_data.network_id,
                "hooking arc: this g-node already entered the target network via a sibling's \
                 propagation while this arc negotiated its own entry, aborting the redundant one"
            );
            if let Err(err) = enter_guard.abort(ask_lvl).await {
                warn!(?arc, error = %err, "hooking arc: abort_enter proxy error, aborting");
                return;
            }
            if sleep_or_cancelled(ctx.config.restart_wait(ctx.view.n_nodes()), &cancel).await {
                return;
            }
            continue 'outer;
        }

        // entry_data obtained: tell the coordinator we completed entry.
        if let Err(e) = enter_guard.complete(ask_lvl).await {
            warn!(?arc, error = %e, "hooking arc: completed_enter proxy error, aborting");
            return;
        }

        // propagate prepare_enter/finish_enter to my own current g-node
        // (arc_handler.vala:349-357) — infallible from Hooking's own point
        // of view (see CoordinatorClient::prepare_enter's docs).
        let enter_id = crate::idgen::next_i32_at_least(1);
        ctx.coord.prepare_enter(ask_lvl, enter_id).await;
        let gsize_at_ask_lvl = ctx.view.topology().gsize(ask_lvl).unwrap_or(0);
        let go_connectivity_position = crate::idgen::next_u32_at_least(gsize_at_ask_lvl);
        let finish_enter_data = FinishEnterData {
            enter_id,
            entry_data: entry_data.clone(),
            go_connectivity_position,
        };
        ctx.coord.finish_enter(ask_lvl, finish_enter_data).await;

        ctx.handle.mark_entered(arc, ask_lvl, entry_data);
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HookingConfig;
    use crate::coordinator::Reservation;
    use crate::domain::{EntryData, FinishMigrationData, NetworkData};
    use crate::fake::{FakeHookingStubFactory, FakeQspnView, ScriptedHookingStub};
    use crate::manager::{HookingOrigin, spawn};
    use futures::future::BoxFuture;
    use ntk_common::Topology;
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard, PoisonError};
    use std::time::Duration;

    fn topo() -> Topology {
        Topology::new([4]).expect("valid topology")
    }

    async fn wait_for(mut check: impl FnMut() -> bool, max_rounds: usize) -> bool {
        for _ in 0..max_rounds {
            if check() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        check()
    }

    /// Like [`wait_for`] but actually lets wall-clock time pass each round — required when the
    /// condition depends on a real `tokio::time::sleep` elsewhere (e.g. a retry backoff)
    /// completing, which bare `yield_now` polling never gives a chance to elapse.
    async fn wait_for_real(mut check: impl FnMut() -> bool, max_rounds: usize) -> bool {
        for _ in 0..max_rounds {
            if check() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        check()
    }

    fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A minimal [`CoordinatorClient`] reproducing `EnterArbiter`'s exact server-side
    /// semantics (`crates/ntkd/src/node/adapters.rs`): at most one `evaluate_enter_id` in
    /// flight per level; a second, different id at the same level gets `AskAgain`; the slot
    /// is released only by an explicit `completed_enter`/`abort_enter` at that level.
    /// `begin_enter` never resolves, so a test can abort the caller while it is parked there
    /// — reproducing a task dropped mid-episode deterministically, without racing a timer.
    #[derive(Default)]
    struct ArbiterClient {
        in_flight: Mutex<HashMap<usize, i32>>,
    }

    impl ArbiterClient {
        fn new() -> Self {
            Self::default()
        }
    }

    impl CoordinatorClient for ArbiterClient {
        fn n_nodes(&self) -> BoxFuture<'_, u64> {
            Box::pin(async { 1 })
        }

        fn evaluate_enter(
            &self,
            req: EvaluateEnterRequest,
        ) -> BoxFuture<'_, Result<usize, CoordinatorError>> {
            Box::pin(async move {
                let level = req.min_lvl;
                let mut in_flight = lock(&self.in_flight);
                match in_flight.get(&level) {
                    Some(existing) if *existing != req.evaluate_enter_id => {
                        Err(CoordinatorError::AskAgain)
                    }
                    _ => {
                        in_flight.insert(level, req.evaluate_enter_id);
                        Ok(level)
                    }
                }
            })
        }

        fn begin_enter(&self, _lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
            Box::pin(std::future::pending())
        }

        fn completed_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
            Box::pin(async move {
                lock(&self.in_flight).remove(&lvl);
                Ok(())
            })
        }

        fn abort_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
            Box::pin(async move {
                lock(&self.in_flight).remove(&lvl);
                Ok(())
            })
        }

        fn reserve(
            &self,
            _host_lvl: usize,
            _reserve_request_id: i32,
        ) -> BoxFuture<'_, Result<Reservation, CoordinatorError>> {
            Box::pin(async { unreachable!("not exercised by this test") })
        }

        fn delete_reserve(&self, _host_lvl: usize, _reserve_request_id: i32) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }

        fn prepare_migration(&self, _lvl: usize, _migration_id: i32) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }

        fn finish_migration(&self, _lvl: usize, _data: FinishMigrationData) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }

        fn prepare_enter(&self, _lvl: usize, _enter_id: i32) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }

        fn finish_enter(&self, _lvl: usize, _data: FinishEnterData) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    /// Pins the leak this batch's context describes: an accepted `evaluate_enter` episode
    /// abandoned by its own task being aborted mid-flight — exactly what
    /// `crate::manager::Actor::run`'s shutdown path (`JoinSet::shutdown`) does to any arc task
    /// still parked inside a plain RPC `.await`, per that method's own doc comment — must not
    /// permanently starve the next `evaluate_enter` at the same level. Without `EnterGuard`
    /// this fails: the abandoned id is never released, so the retry gets `AskAgain` forever.
    #[tokio::test]
    async fn aborted_episode_releases_the_evaluate_enter_slot() {
        let coord = Arc::new(ArbiterClient::new());
        let coord_dyn: Arc<dyn CoordinatorClient> = coord.clone();
        let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
        let stubs = Arc::new(FakeHookingStubFactory::new());
        let arc_id = ArcId(1);
        stubs.register_arc(
            arc_id,
            Arc::new(ScriptedHookingStub::new(
                |_ask_coord| {
                    Ok(NetworkData {
                        network_id: 999,
                        neighbor_n_nodes: 100,
                        neighbor_min_level: 0,
                        gsizes: vec![4],
                        neighbor_pos: vec![0],
                    })
                },
                |_lvl| unreachable!("begin_enter never resolves in this test"),
            )),
        );
        let stubs: Arc<dyn HookingStubFactory> = stubs;
        let cancel = CancellationToken::new();

        let (handle, _actor) = spawn(
            HookingOrigin::Joining,
            view,
            coord_dyn,
            stubs,
            HookingConfig::default(),
            cancel.clone(),
        );

        handle.add_arc(arc_id).await.expect("add_arc succeeds");
        assert!(
            wait_for(
                || handle.snapshot().arcs.get(&arc_id) == Some(&ArcPhase::Entering { ask_lvl: 0 }),
                10_000,
            )
            .await,
            "evaluate_enter must be Accepted and the episode parked in begin_enter"
        );

        // Tear the generation down without ever calling `completed_enter`/`abort_enter`:
        // `Actor::run`'s shutdown path aborts the arc task while it sits inside
        // `begin_enter().await`, dropping it mid-episode.
        cancel.cancel();
        // Let the actor process the cancellation, abort the arc task, and let `EnterGuard`'s
        // own detached cleanup task (spawned from `Drop`) run to completion.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let retry = coord
            .evaluate_enter(EvaluateEnterRequest {
                network_id: 999,
                neighbor_pos: vec![0],
                neighbor_min_lvl: 0,
                min_lvl: 0,
                evaluate_enter_id: 424_242,
            })
            .await;
        assert_eq!(
            retry,
            Ok(0),
            "the abandoned episode's slot must be released, not leaked forever"
        );
    }

    /// A wrapper over [`FakeQspnView`] whose `network_id` is independently mutable after
    /// construction — needed to simulate, deterministically and without racing a real timer, a
    /// sibling's own propagated `finish_enter` landing (`crate::manager::HookingHandle::
    /// notify_finish_enter`, applied in `ntkd::node::lifecycle::on_hooking_event`) while this
    /// arc's own `search_migration_path` round trip is still in flight.
    #[derive(Debug)]
    struct DynamicNetworkIdView {
        inner: FakeQspnView,
        network_id: std::sync::atomic::AtomicI64,
    }

    impl QspnView for DynamicNetworkIdView {
        fn topology(&self) -> &Topology {
            self.inner.topology()
        }
        fn network_id(&self) -> i64 {
            self.network_id.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn n_nodes(&self) -> u64 {
            self.inner.n_nodes()
        }
        fn my_pos(&self, level: usize) -> u32 {
            self.inner.my_pos(level)
        }
        fn my_eldership(&self, level: usize) -> i32 {
            self.inner.my_eldership(level)
        }
        fn subnetlevel(&self) -> usize {
            self.inner.subnetlevel()
        }
        fn epsilon(&self, level: usize) -> usize {
            self.inner.epsilon(level)
        }
        fn eldership(&self, level: usize, pos: u32) -> i32 {
            self.inner.eldership(level, pos)
        }
        fn adjacent_to_my_gnode(&self, a: usize, b: usize) -> Vec<crate::view::AdjacentGNode> {
            self.inner.adjacent_to_my_gnode(a, b)
        }
        fn is_bootstrapped(&self) -> bool {
            self.inner.is_bootstrapped()
        }
    }

    fn two_level_topo() -> Topology {
        Topology::new([4, 2]).expect("valid topology")
    }

    /// The defining "yield" case (batch context's step 3): a member's own arc is mid-negotiation
    /// — its `search_migration_path` round trip already in flight — when a sibling's own
    /// propagated `finish_enter` lands this identity in the target network first. Resolved here
    /// as "abort the now-redundant reservation, not complete it": `completed_enter`/`finish_enter`
    /// must never fire, `abort_enter` must, and the arc must settle at `ArcPhase::SameNetwork`
    /// on its very next pass through `retrieve_network_data` — converging, not wedging or
    /// double-committing a second, competing position for the same g-node.
    #[tokio::test]
    async fn arc_yields_when_its_own_gnode_already_entered_the_target_via_a_sibling() {
        let topo = two_level_topo();
        let mut inner = FakeQspnView::new(topo.clone(), vec![0, 0]);
        inner.network_id = 100; // this identity's own (pre-merge) network
        inner.n_nodes = 3; // this g-node's own 3 members
        inner.subnetlevel = 1; // forces `evaluate_enter`'s min_lvl, hence ask_lvl, to 1
        let view = Arc::new(DynamicNetworkIdView {
            inner,
            network_id: std::sync::atomic::AtomicI64::new(100),
        });
        let network_id_cell = Arc::clone(&view);
        let view: Arc<dyn QspnView> = view;

        let coord = Arc::new(crate::fake::FakeCoordinatorClient::new(3));
        let coord_dyn: Arc<dyn CoordinatorClient> = coord.clone();

        let stubs = Arc::new(FakeHookingStubFactory::new());
        let arc_id = ArcId(1);
        stubs.register_arc(
            arc_id,
            Arc::new(ScriptedHookingStub::new(
                |_ask_coord| {
                    Ok(NetworkData {
                        network_id: 200,       // the target network
                        neighbor_n_nodes: 100, // > 10x my 3 nodes: proceed unconditionally
                        neighbor_min_level: 0,
                        gsizes: vec![4, 2],
                        neighbor_pos: vec![0, 0],
                    })
                },
                move |_lvl| {
                    // By the time this identity's own `search_migration_path` round trip
                    // resolves, a sibling's own propagated `finish_enter` has already carried
                    // this g-node into the target network — exactly what
                    // `ntkd::node::lifecycle::on_hooking_event`'s `DoFinishEnter` handling
                    // does for every member, not only the one that negotiated.
                    network_id_cell
                        .network_id
                        .store(200, std::sync::atomic::Ordering::SeqCst);
                    Ok(EntryData {
                        network_id: 200,
                        pos: vec![1],
                        elderships: vec![0],
                    })
                },
            )),
        );
        let stubs: Arc<dyn HookingStubFactory> = stubs;
        let cancel = CancellationToken::new();
        let config = HookingConfig {
            not_bootstrapped_retry: Duration::from_millis(5),
            merge_reject_wait: Duration::from_millis(5),
            global_timeout: Arc::new(|_| Duration::from_millis(5)),
            ask_again_divisor: 1,
            restart_multiplier: 1,
            routing_response_timeout: Duration::from_millis(200),
        };

        let (handle, _actor) = spawn(
            HookingOrigin::Joining,
            view,
            coord_dyn,
            stubs,
            config,
            cancel.clone(),
        );
        handle.add_arc(arc_id).await.expect("add_arc succeeds");

        let settled = wait_for(
            || {
                matches!(
                    handle.snapshot().arcs.get(&arc_id),
                    Some(ArcPhase::SameNetwork)
                )
            },
            10_000,
        )
        .await;
        assert!(
            settled,
            "arc did not converge to SameNetwork: {:?}",
            handle.snapshot().arcs.get(&arc_id)
        );

        let calls = coord.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("abort_enter(1)")),
            "expected an abort_enter(1) call, got: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("completed_enter")),
            "the redundant negotiation must never call completed_enter: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("finish_enter")),
            "the redundant negotiation must never propagate its own finish_enter: {calls:?}"
        );
    }

    /// The mirror case: a member whose own g-node has *not* yet entered the target (it "missed"
    /// — or simply never received — a sibling's propagation) must still converge through its own
    /// negotiation rather than wedge. This is the negative case for the check above: with
    /// `network_id` never mutated to the target, the new `ask_lvl >= 1` guard must not fire, and
    /// the arc must complete its entry exactly as it did before that guard existed.
    #[tokio::test]
    async fn arc_with_no_propagation_still_completes_its_own_entry() {
        let topo = two_level_topo();
        let mut view = FakeQspnView::new(topo, vec![0, 0]);
        view.network_id = 100;
        view.n_nodes = 3;
        view.subnetlevel = 1;
        let view: Arc<dyn QspnView> = Arc::new(view);

        let coord = Arc::new(crate::fake::FakeCoordinatorClient::new(3));
        let coord_dyn: Arc<dyn CoordinatorClient> = coord.clone();

        let stubs = Arc::new(FakeHookingStubFactory::new());
        let arc_id = ArcId(1);
        stubs.register_arc(
            arc_id,
            Arc::new(ScriptedHookingStub::new(
                |_ask_coord| {
                    Ok(NetworkData {
                        network_id: 200,
                        neighbor_n_nodes: 100,
                        neighbor_min_level: 0,
                        gsizes: vec![4, 2],
                        neighbor_pos: vec![0, 0],
                    })
                },
                |_lvl| {
                    Ok(EntryData {
                        network_id: 200,
                        pos: vec![1],
                        elderships: vec![0],
                    })
                },
            )),
        );
        let stubs: Arc<dyn HookingStubFactory> = stubs;
        let cancel = CancellationToken::new();
        let config = HookingConfig {
            not_bootstrapped_retry: Duration::from_millis(5),
            merge_reject_wait: Duration::from_millis(5),
            global_timeout: Arc::new(|_| Duration::from_millis(5)),
            ask_again_divisor: 1,
            restart_multiplier: 1,
            routing_response_timeout: Duration::from_millis(200),
        };

        let (handle, _actor) = spawn(
            HookingOrigin::Joining,
            view,
            coord_dyn,
            stubs,
            config,
            cancel.clone(),
        );
        handle.add_arc(arc_id).await.expect("add_arc succeeds");

        let settled = wait_for(
            || {
                matches!(
                    handle.snapshot().arcs.get(&arc_id),
                    Some(ArcPhase::Entered { ask_lvl: 1 })
                )
            },
            10_000,
        )
        .await;
        assert!(
            settled,
            "arc did not converge to Entered: {:?}",
            handle.snapshot().arcs.get(&arc_id)
        );

        let calls = coord.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("completed_enter(1)")),
            "an un-propagated member must still complete its own negotiation: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("finish_enter(1)")),
            "an un-propagated member must still propagate its own finish_enter: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("abort_enter")),
            "no sibling propagation landed, so nothing should be aborted: {calls:?}"
        );
    }

    /// `ask_lvl == 0` must never engage the yield check above, even in the one case that could
    /// otherwise coincidentally trigger it: this identity's `network_id` happening to already
    /// equal the target's by the time `search_migration_path` resolves. A level-0 g-node has
    /// exactly one member, so this can only ever be coincidence, never a sibling's propagation —
    /// the gate is `ask_lvl >= 1`, not a `network_id` heuristic alone, precisely so this case is
    /// unaffected: `completed_enter`/`finish_enter` must still fire, `abort_enter` must not.
    #[tokio::test]
    async fn level_zero_completes_even_if_network_id_happens_to_already_match_target() {
        let topo = Topology::new([4]).expect("valid topology");
        let mut inner = FakeQspnView::new(topo, vec![0]);
        inner.network_id = 100;
        inner.subnetlevel = 0; // forces ask_lvl == 0
        let view = Arc::new(DynamicNetworkIdView {
            inner,
            network_id: std::sync::atomic::AtomicI64::new(100),
        });
        let network_id_cell = Arc::clone(&view);
        let view: Arc<dyn QspnView> = view;

        let coord = Arc::new(crate::fake::FakeCoordinatorClient::new(1));
        let coord_dyn: Arc<dyn CoordinatorClient> = coord.clone();

        let stubs = Arc::new(FakeHookingStubFactory::new());
        let arc_id = ArcId(1);
        stubs.register_arc(
            arc_id,
            Arc::new(ScriptedHookingStub::new(
                |_ask_coord| {
                    Ok(NetworkData {
                        network_id: 200,
                        neighbor_n_nodes: 100,
                        neighbor_min_level: 0,
                        gsizes: vec![4],
                        neighbor_pos: vec![0],
                    })
                },
                move |_lvl| {
                    // Coincidence, not a propagation: nothing else could have moved a level-0
                    // (single-member) g-node's `network_id` out from under it.
                    network_id_cell
                        .network_id
                        .store(200, std::sync::atomic::Ordering::SeqCst);
                    Ok(EntryData {
                        network_id: 200,
                        pos: vec![0],
                        elderships: vec![0],
                    })
                },
            )),
        );
        let stubs: Arc<dyn HookingStubFactory> = stubs;
        let cancel = CancellationToken::new();
        let config = HookingConfig {
            not_bootstrapped_retry: Duration::from_millis(5),
            merge_reject_wait: Duration::from_millis(5),
            global_timeout: Arc::new(|_| Duration::from_millis(5)),
            ask_again_divisor: 1,
            restart_multiplier: 1,
            routing_response_timeout: Duration::from_millis(200),
        };

        let (handle, _actor) = spawn(
            HookingOrigin::Joining,
            view,
            coord_dyn,
            stubs,
            config,
            cancel.clone(),
        );
        handle.add_arc(arc_id).await.expect("add_arc succeeds");

        let settled = wait_for(
            || {
                matches!(
                    handle.snapshot().arcs.get(&arc_id),
                    Some(ArcPhase::Entered { ask_lvl: 0 })
                )
            },
            10_000,
        )
        .await;
        assert!(
            settled,
            "arc did not converge to Entered: {:?}",
            handle.snapshot().arcs.get(&arc_id)
        );

        let calls = coord.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("completed_enter(0)")),
            "ask_lvl == 0 must complete exactly as before this feature existed: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("finish_enter(0)")),
            "ask_lvl == 0 must still propagate its own finish_enter: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("abort_enter")),
            "the ask_lvl >= 1 gate must not engage at ask_lvl == 0: {calls:?}"
        );
    }

    /// The regression this batch actually fixes (not `check_propagation`/`fp_id`, which a
    /// real-kernel trace showed was never on the failing path — see this crate's own history
    /// for the corrected diagnosis). `evaluate_enter` is the very first DHT round trip after a
    /// brand-new arc is discovered; on real hardware its outbound route can still be one
    /// `ntk-qspn` tick behind the arc handler's own faster, direct-link
    /// `retrieve_network_data` exchange, so `contact_peer` transiently answers "no candidate"
    /// (`CoordinatorError::Unreachable`) even though the target is genuinely reachable. Fewer
    /// than [`EVALUATE_ENTER_UNREACHABLE_RETRIES`] such failures must not abort the arc
    /// handler — before this fix, any `Unreachable` here was fatal (`return` on the very first
    /// one), which is the bug this test fails against.
    #[tokio::test]
    async fn evaluate_enter_survives_a_few_transient_unreachable_errors() {
        let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
        let coord = Arc::new(crate::fake::FakeCoordinatorClient::new(1));
        for _ in 0..EVALUATE_ENTER_UNREACHABLE_RETRIES {
            coord.queue_evaluate_enter(Err(CoordinatorError::Unreachable(
                "no candidate".to_string(),
            )));
        }
        let coord_dyn: Arc<dyn CoordinatorClient> = coord.clone();

        let stubs = Arc::new(FakeHookingStubFactory::new());
        let arc_id = ArcId(1);
        stubs.register_arc(
            arc_id,
            Arc::new(ScriptedHookingStub::new(
                |_ask_coord| {
                    Ok(NetworkData {
                        network_id: 200,
                        neighbor_n_nodes: 100,
                        neighbor_min_level: 0,
                        gsizes: vec![4],
                        neighbor_pos: vec![0],
                    })
                },
                |_lvl| {
                    Ok(EntryData {
                        network_id: 200,
                        pos: vec![1],
                        elderships: vec![0],
                    })
                },
            )),
        );
        let stubs: Arc<dyn HookingStubFactory> = stubs;
        let cancel = CancellationToken::new();
        let config = HookingConfig {
            not_bootstrapped_retry: Duration::from_millis(5),
            merge_reject_wait: Duration::from_millis(5),
            global_timeout: Arc::new(|_| Duration::from_millis(5)),
            ask_again_divisor: 1,
            restart_multiplier: 1,
            routing_response_timeout: Duration::from_millis(200),
        };

        let (handle, _actor) = spawn(
            HookingOrigin::Joining,
            view,
            coord_dyn,
            stubs,
            config,
            cancel.clone(),
        );
        handle.add_arc(arc_id).await.expect("add_arc succeeds");

        let settled = wait_for_real(
            || {
                matches!(
                    handle.snapshot().arcs.get(&arc_id),
                    Some(ArcPhase::Entered { ask_lvl: 0 })
                )
            },
            2_000,
        )
        .await;
        assert!(
            settled,
            "evaluate_enter must recover from a bounded run of transient Unreachable errors \
             instead of aborting: {:?}",
            handle.snapshot().arcs.get(&arc_id)
        );

        let calls = coord.calls();
        let attempts = calls.iter().filter(|c| *c == "evaluate_enter").count();
        assert_eq!(
            attempts,
            EVALUATE_ENTER_UNREACHABLE_RETRIES as usize + 1,
            "every queued failure must be retried exactly once each, then the next call \
             succeeds: {calls:?}"
        );
    }

    /// The other half of the same pin: a target that stays genuinely unreachable past the
    /// retry budget must still make the arc handler give up, exactly like upstream's own
    /// `CoordProxyError`/`UnknownResultError` handling (`arc_handler.vala:230-235`) — the retry
    /// added above must not become an unbounded, upstream-diverging loop.
    #[tokio::test]
    async fn evaluate_enter_gives_up_once_the_unreachable_retry_budget_is_exhausted() {
        let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
        let coord = Arc::new(crate::fake::FakeCoordinatorClient::new(1));
        for _ in 0..=EVALUATE_ENTER_UNREACHABLE_RETRIES {
            coord.queue_evaluate_enter(Err(CoordinatorError::Unreachable(
                "no candidate".to_string(),
            )));
        }
        let coord_dyn: Arc<dyn CoordinatorClient> = coord.clone();

        let stubs = Arc::new(FakeHookingStubFactory::new());
        let arc_id = ArcId(1);
        stubs.register_arc(
            arc_id,
            Arc::new(ScriptedHookingStub::new(
                |_ask_coord| {
                    Ok(NetworkData {
                        network_id: 200,
                        neighbor_n_nodes: 100,
                        neighbor_min_level: 0,
                        gsizes: vec![4],
                        neighbor_pos: vec![0],
                    })
                },
                |_lvl| unreachable!("evaluate_enter never succeeds in this test"),
            )),
        );
        let stubs: Arc<dyn HookingStubFactory> = stubs;
        let cancel = CancellationToken::new();
        let config = HookingConfig {
            not_bootstrapped_retry: Duration::from_millis(5),
            merge_reject_wait: Duration::from_millis(5),
            global_timeout: Arc::new(|_| Duration::from_millis(5)),
            ask_again_divisor: 1,
            restart_multiplier: 1,
            routing_response_timeout: Duration::from_millis(200),
        };

        let (handle, _actor) = spawn(
            HookingOrigin::Joining,
            view,
            coord_dyn,
            stubs,
            config,
            cancel.clone(),
        );
        handle.add_arc(arc_id).await.expect("add_arc succeeds");

        let exhausted = wait_for_real(
            || {
                coord
                    .calls()
                    .iter()
                    .filter(|c| *c == "evaluate_enter")
                    .count()
                    == EVALUATE_ENTER_UNREACHABLE_RETRIES as usize + 1
            },
            2_000,
        )
        .await;
        assert!(
            exhausted,
            "expected the retry budget to be spent: {:?}",
            coord.calls()
        );

        // Give the (already-aborted) task a moment it should never use, then confirm it never
        // reached `Entered` and never attempted a further call past the budget.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !matches!(
                handle.snapshot().arcs.get(&arc_id),
                Some(ArcPhase::Entered { .. })
            ),
            "a target unreachable past the retry budget must not converge: {:?}",
            handle.snapshot().arcs.get(&arc_id)
        );
        let calls = coord.calls();
        assert_eq!(
            calls.iter().filter(|c| *c == "evaluate_enter").count(),
            EVALUATE_ENTER_UNREACHABLE_RETRIES as usize + 1,
            "the handler must not retry past its bound: {calls:?}"
        );
    }
}
