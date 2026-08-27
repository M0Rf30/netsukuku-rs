//! Capabilities Coordinator needs from the rest of the daemon, declared as this crate's own
//! traits rather than a dependency on `ntk-qspn`/`ntk-hooking` (mirrors how `ntk-peerservices`
//! declares [`ntk_peerservices::RoutingEnv`] instead of depending on Neighborhood/QSPN/Hooking).
//! The `ntkd` composition root (phase 4) implements these by delegating to the real crates.

use futures::future::BoxFuture;
use ntk_proto::v1::TypedValue;

use crate::domain::PropagationArgs;

/// Everything Coordinator needs to know about my own g-node topology to run the fixed-keys
/// database (`ICoordinatorMap`, `research/impl/vala/coordinator/api.vala:23-30`). Every method
/// is 0-indexed, matching `ntk_common::Topology`'s own convention — callers translate from a
/// 1-indexed `top` (`fk_database.vala:505,539` pass `lvl - 1`) where upstream does the same.
pub trait CoordinatorMap: Send + Sync {
    /// Network-wide node count (`get_n_nodes`, `api.vala:25`).
    fn n_nodes(&self) -> u64;
    /// Real (non-virtual) positions at `level` not currently occupied (`get_free_pos`,
    /// `api.vala:26`).
    fn free_positions(&self, level: usize) -> Vec<u32>;
    /// Whether a reservation can be served at `level` right now (`can_reserve`, `api.vala:27`).
    fn can_reserve(&self, level: usize) -> bool;
    /// My own position at `level` (`get_my_pos`, `api.vala:28`).
    fn my_pos(&self, level: usize) -> u32;
    /// My own g-node's fingerprint id at `level`, used to detect a stale propagation
    /// (`get_fp_id`, `api.vala:29`).
    fn fp_id(&self, level: usize) -> i64;
}

/// `HandlingImpossibleError` (`research/impl/vala/coordinator/api.vala:36-38`) has no recovery
/// path upstream — the request handler `tasklet.exit_tasklet()`s rather than answer
/// (`fk_database.vala:447-448` and its three repeats). Since aborting the whole actor task on
/// untrusted-but-routine input is not an option here, and upstream's own comment treats reaching
/// this branch as a should-never-happen protocol bug rather than a modeled outcome, this crate's
/// four enter-protocol handler traits are **deliberately infallible**: the implementor (Hooking)
/// is responsible for never being asked something it cannot answer, exactly as upstream's own
/// abort-on-violation semantics already assume.
pub trait EvaluateEnterHandler: Send + Sync {
    /// `IEvaluateEnterHandler.evaluate_enter` (`api.vala:42`). `top` is the same 1-indexed
    /// `CoordinatorKey` level the DHT request targeted; `client_tuple` is the requester's
    /// position, scoped to `top`.
    fn evaluate_enter<'a>(
        &'a self,
        top: usize,
        data: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue>;
}

/// `IBeginEnterHandler.begin_enter` (`api.vala:47`). See [`EvaluateEnterHandler`]'s doc comment
/// for the infallibility rationale.
pub trait BeginEnterHandler: Send + Sync {
    fn begin_enter<'a>(
        &'a self,
        top: usize,
        data: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue>;
}

/// `ICompletedEnterHandler.completed_enter` (`api.vala:52`). See [`EvaluateEnterHandler`]'s doc
/// comment for the infallibility rationale.
pub trait CompletedEnterHandler: Send + Sync {
    fn completed_enter<'a>(
        &'a self,
        top: usize,
        data: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue>;
}

/// `IAbortEnterHandler.abort_enter` (`api.vala:57`). See [`EvaluateEnterHandler`]'s doc comment
/// for the infallibility rationale.
pub trait AbortEnterHandler: Send + Sync {
    fn abort_enter<'a>(
        &'a self,
        top: usize,
        data: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue>;
}

/// Bundles the four enter-protocol handlers a [`crate::Manager`] dispatches into
/// (`CoordinatorManager`'s four constructor handler params, `coord.vala:93-103`).
pub struct EnterHandlers {
    pub evaluate_enter: std::sync::Arc<dyn EvaluateEnterHandler>,
    pub begin_enter: std::sync::Arc<dyn BeginEnterHandler>,
    pub completed_enter: std::sync::Arc<dyn CompletedEnterHandler>,
    pub abort_enter: std::sync::Arc<dyn AbortEnterHandler>,
}

impl std::fmt::Debug for EnterHandlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnterHandlers").finish_non_exhaustive()
    }
}

/// `IPropagationHandler` (`research/impl/vala/coordinator/api.vala:60-67`): the local effect of
/// a deduplicated propagation, applied by Hooking after fanout. Fire-and-forget, mirroring the
/// upstream `void`-returning interface.
pub trait PropagationHandler: Send + Sync {
    fn prepare_migration(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()>;
    fn finish_migration(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()>;
    fn prepare_enter(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()>;
    fn finish_enter(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()>;
    fn we_have_splitted(&self, level: usize, data: TypedValue) -> BoxFuture<'_, ()>;
}

/// The 5 outbound `CoordinatorManager.execute_*` calls (`ICoordinatorManagerStub`,
/// `research/impl/vala/coordinator/api.vala:69-73` names the factory; the stub interface itself
/// is `ntkdrpc`'s generated `coordinator_manager` skeleton, `research/notes/01-vala-core-
/// routing.md` §2). Implemented once against the real transport
/// ([`crate::wire::RpcCoordinatorStub`]) and once over an in-memory fake for tests.
pub trait CoordinatorStub: Send + Sync {
    fn execute_prepare_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>>;
    fn execute_finish_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>>;
    fn execute_prepare_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>>;
    fn execute_finish_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>>;
    fn execute_we_have_splitted(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>>;
}

/// `IStubFactory` (`research/impl/vala/coordinator/api.vala:69-73`): the substitutability seam
/// between Coordinator's propagation fanout and whatever transport/neighbor-discovery
/// (Neighborhood) actually delivers it — Coordinator has no dependency on `ntk-neighborhood`,
/// so this is its own capability trait, exactly like `ntk_peerservices::RoutingEnv::neighbors`.
pub trait CoordinatorStubFactory: Send + Sync {
    /// One stub per direct neighbor, each delivered to individually
    /// (`get_stub_for_each_neighbor`, used by `prepare_migration`/`prepare_enter`).
    fn stub_for_each_neighbor(&self) -> Vec<std::sync::Arc<dyn CoordinatorStub>>;
    /// One stub representing every neighbor at once, delivered to as a single reliable-
    /// broadcast group (`get_stub_for_all_neighbors`, used by `finish_migration`/`finish_enter`/
    /// `we_have_splitted`).
    fn stub_for_all_neighbors(&self) -> std::sync::Arc<dyn CoordinatorStub>;
}
