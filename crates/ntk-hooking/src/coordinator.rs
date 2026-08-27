//! `ICoordinator` (`research/impl/vala/hooking/api.vala:59-81`), inverted
//! into a trait this crate declares rather than a dependency on
//! `ntk-coordinator`. Every method here is a *client-side* outbound call:
//! `evaluate_enter`/`begin_enter`/`completed_enter`/`abort_enter`/`reserve`/
//! `delete_reserve` are DHT round-trips to whichever node PeerServices
//! elects servant for the target level
//! (`research/notes/01-vala-core-routing.md` §7); `prepare_migration`/
//! `finish_migration`/`prepare_enter`/`finish_enter` ask the Coordinator to
//! flood a local-propagation event to every member of the g-node at `lvl`
//! (`research/notes/01` §7, "not DHT calls ... broadcast ... to
//! `get_stub_for_each_neighbor`/`get_stub_for_all_neighbors`").
//!
//! Upstream's `HookingManager.evaluate_enter`/`begin_enter`/`completed_enter`/
//! `abort_enter` (`hooking.vala:286-304`) are the *server*-side election
//! algorithm (`ProxyCoord.execute_evaluate_enter` and friends,
//! `research/impl/vala/hooking/proxy_coord.vala`), run by whichever node is
//! elected coordinator servant for a level — including, at `lvl == 0`, by
//! calling straight back into that very node's own Hooking module
//! (`proxy_coord.vala:345-349`). Since `ntk-coordinator` must not depend on
//! `ntk-hooking` either (batch contract: "Neither depends on the other, nor
//! on hooking"), that server-side election machinery belongs to
//! `ntk-coordinator` itself, not here — this trait only ever needs to be the
//! *asker*.

use futures::future::BoxFuture;
use thiserror::Error;

use crate::domain::{EvaluateEnterRequest, FinishEnterData, FinishMigrationData};
use crate::merge::merge_tiebreak;

/// A freshly reserved position — `coord.reserve`'s `out` parameters
/// (`api.vala:76`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub pos: u32,
    pub eldership: i32,
}

/// Everything [`merge_tiebreak`] needs, packaged as a single Coordinator-mediated request
/// (`arc_handler.vala:183-208`'s "ask coord" tiebreak inputs). Passing the whole decision
/// through one call, rather than each arc handler collecting the inputs itself, is what lets
/// [`CoordinatorClient::decide_merge`] be answered once per target and shared *for a bounded
/// freshness window* — see that method's own doc for why the window must be bounded, not
/// eternal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergeArbitrationRequest {
    /// My own network id, as seen by the asking arc (`map_paths.get_network_id()`).
    pub my_network_id: i64,
    /// The candidate neighbor network's id, as freshly reported by
    /// `stub.retrieve_network_data(true)`.
    pub neighbor_network_id: i64,
    /// The candidate neighbor network's authoritative node count, from the same call.
    pub neighbor_n_nodes: u64,
}

/// Everything a [`CoordinatorClient`] call can fail with. Not every variant
/// is reachable from every method — each method's doc comment says which
/// upstream `errordomain` it stands in for.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CoordinatorError {
    /// `CoordProxyError`/`UnknownResultError` (`api.vala:64,69-71,83`): the
    /// proxy round-trip to the elected servant failed outright (transport
    /// failure, or a reply of an unexpected shape). Every
    /// [`CoordinatorClient`] method can fail this way.
    #[error("coordinator proxy unreachable or returned an unexpected result: {0}")]
    Unreachable(String),

    /// `ProxyCoord.AskAgainError` (`proxy_coord.vala:27`): `evaluate_enter`
    /// only — the network-wide election is still pending; retry after
    /// `HookingConfig::ask_again_wait`.
    #[error("evaluate_enter: election pending, ask again")]
    AskAgain,

    /// `ProxyCoord.IgnoreNetworkError` (`proxy_coord.vala:28`):
    /// `evaluate_enter` only — this network evaluation lost or expired;
    /// abandon it and redo the whole arc-handler loop from start.
    #[error("evaluate_enter: this network evaluation was abandoned")]
    IgnoreNetwork,

    /// `ProxyCoord.AlreadyEnteringError` (`proxy_coord.vala:30`):
    /// `begin_enter` only — another entry is already in progress for this
    /// g-node.
    #[error("begin_enter: another entry is already in progress")]
    AlreadyEntering,

    /// `CoordReserveError` (`api.vala:84`): `reserve` only — no coordinator
    /// is presently reachable/elected for this host level; the caller
    /// should try the next level up (`hooking.vala:176-184`).
    #[error("reserve: no coordinator reachable for this host level")]
    NoCoordinatorForLevel,
}

/// Client-side seam onto the (per-level elected) Coordinator —
/// `ICoordinator` (`api.vala:59-81`). Implemented by the `ntkd` composition
/// root over the real `ntk-coordinator`/`ntk-peerservices` crates; a fake
/// implementation lives in [`crate::fake::FakeCoordinatorClient`].
pub trait CoordinatorClient: Send + Sync {
    /// `get_n_nodes` (`api.vala:61`): the coordinator's authoritative node
    /// count for my network (used to break a near-tie merge decision,
    /// `arc_handler.vala:183`).
    fn n_nodes(&self) -> BoxFuture<'_, u64>;

    /// `evaluate_enter` (`api.vala:64`): network-wide arbitration of which
    /// arc handler gets to proceed first and at which level.
    ///
    /// # Errors
    /// [`CoordinatorError::AskAgain`], [`CoordinatorError::IgnoreNetwork`],
    /// or [`CoordinatorError::Unreachable`].
    fn evaluate_enter(
        &self,
        req: EvaluateEnterRequest,
    ) -> BoxFuture<'_, Result<usize, CoordinatorError>>;

    /// `begin_enter` (`api.vala:69`): claims the right to attempt entry at
    /// g-node level `lvl`.
    ///
    /// # Errors
    /// [`CoordinatorError::AlreadyEntering`] or [`CoordinatorError::Unreachable`].
    fn begin_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>>;

    /// `completed_enter` (`api.vala:70`): releases the `begin_enter` claim
    /// after a successful (or abandoned-but-final) entry attempt.
    ///
    /// # Errors
    /// [`CoordinatorError::Unreachable`].
    fn completed_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>>;

    /// `abort_enter` (`api.vala:71`): releases the `begin_enter` claim after
    /// a failed migration-path search, so a retry at a different level can
    /// proceed (`arc_handler.vala:291-303`).
    ///
    /// # Errors
    /// [`CoordinatorError::Unreachable`].
    fn abort_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>>;

    /// `reserve` (`api.vala:76`): reserves a position for the g-node
    /// hosted at `host_lvl`, idempotent by `reserve_request_id` (safe to
    /// retry with the same id, `research/notes/01` §7).
    ///
    /// # Errors
    /// [`CoordinatorError::NoCoordinatorForLevel`] — try `host_lvl + 1`
    /// (`hooking.vala:176-184`).
    fn reserve(
        &self,
        host_lvl: usize,
        reserve_request_id: i32,
    ) -> BoxFuture<'_, Result<Reservation, CoordinatorError>>;

    /// `delete_reserve` (`api.vala:77`): releases a reservation that turned
    /// out not to be the chosen solution (`hooking.vala:531-541`). Best
    /// effort — upstream's own call site never inspects a failure either.
    fn delete_reserve(&self, host_lvl: usize, reserve_request_id: i32) -> BoxFuture<'_, ()>;

    /// `prepare_migration` (`api.vala:79`): floods "prepare to migrate
    /// `migration_id`" to every member of the g-node at `lvl`. Infallible
    /// from Hooking's point of view — `ICoordinator.prepare_migration`
    /// declares no `throws` clause (`api.vala:79`); any propagation
    /// failure is the Coordinator's own internal concern.
    fn prepare_migration(&self, lvl: usize, migration_id: i32) -> BoxFuture<'_, ()>;

    /// `finish_migration` (`api.vala:80`): floods the resolved migration
    /// data to every member of the g-node at `lvl`. Infallible — see
    /// [`Self::prepare_migration`]'s docs.
    fn finish_migration(&self, lvl: usize, data: FinishMigrationData) -> BoxFuture<'_, ()>;

    /// `prepare_enter` (`api.vala:73`): floods "prepare to admit `enter_id`"
    /// to every member of my current g-node at `lvl` — this call blocks,
    /// upstream-style, until every member has completed
    /// (`propagation_coord.vala:46-52`). Infallible — see
    /// [`Self::prepare_migration`]'s docs.
    fn prepare_enter(&self, lvl: usize, enter_id: i32) -> BoxFuture<'_, ()>;

    /// Routes [`merge_tiebreak`]'s "ask coord" decision through the Coordinator so it is made
    /// **once** per target network and shared by every member of my own g-node, instead of
    /// each arc handler recomputing it against its own, potentially differently-timed, sampling
    /// of [`Self::n_nodes`]/`retrieve_network_data`. A real six-node two-group merge with that
    /// per-arc recomputation produced `a_rehooked=2 b_rehooked=3` — members of the *same*
    /// g-node reaching opposite conclusions about which side should migrate. A conforming
    /// implementation memoizes its answer per [`MergeArbitrationRequest::neighbor_network_id`]
    /// for a bounded freshness window: an ask within that window — from any arc, on any
    /// member — gets the identical cached verdict, which is what makes members *follow*
    /// instead of *re-decide* against their own, possibly out-of-sync, sample. That memoization
    /// must still expire: a verdict computed once and never revisited again correctly models
    /// "the g-node has decided" only for as long as the inputs it was computed from still
    /// hold, and either side's real size can change mid-episode (that is precisely what a
    /// merge does). A second real multi-member merge, fixed by bounding this memoization's
    /// lifetime, showed the failure mode of an *unbounded* cache directly: some members of one
    /// g-node asked (and cached a verdict) before their side's own count had caught up, and,
    /// since nothing ever invalidated it, never reconsidered — splitting one g-node's migration
    /// into "some members moved, some never did". A late asker or a retry after a failed round
    /// trip is still naturally robust either way (pull, not push: there is nothing to have
    /// missed), it just also gets a fresh recompute once its own ask falls outside the window.
    ///
    /// Default implementation preserves the pre-existing, non-collective behavior — it
    /// recomputes [`merge_tiebreak`] locally against [`Self::n_nodes`] on every call — so an
    /// implementor that has not wired up real cross-member sharing yet keeps compiling and
    /// behaving exactly as before (the same additive-default discipline
    /// [`crate::view::QspnView::note_foreign`] established).
    fn decide_merge(&self, req: MergeArbitrationRequest) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            let my_n_nodes = self.n_nodes().await;
            merge_tiebreak(
                my_n_nodes,
                req.neighbor_n_nodes,
                req.my_network_id,
                req.neighbor_network_id,
            )
        })
    }

    /// `finish_enter` (`api.vala:74`): floods the resolved entry data to
    /// every member of my current g-node at `lvl`. Infallible — see
    /// [`Self::prepare_migration`]'s docs.
    fn finish_enter(&self, lvl: usize, data: FinishEnterData) -> BoxFuture<'_, ()>;
}
