//! The single-owner actor holding the fixed-keys database state (`research/notes/06-rust-
//! stack.md` §Concurrency): every level's [`GnodeMemory`], and the propagation dedup set. Every
//! other module in this crate reaches this state only through [`Handle`] — never directly.
//!
//! Outbound work (stub fanout, calling into the injected Hooking handlers) never happens inside
//! [`State::handle`] (the actor's own command loop) — it lives in [`Handle`]'s own async methods,
//! which run on whichever task called them, exactly mirroring `ntk_peerservices::actor::Handle`'s
//! `contact_peer`/`forward_msg` split. This is what keeps the actor loop from ever awaiting an
//! outbound RPC (the deadlock class `ntk-qspn` found and fixed the same way).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ntk_common::Topology;
use ntk_proto::v1::TypedValue;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::domain::{
    Booking, Event, GnodeMemory, HandOff, PropagationArgs, Reservation, ReserveError, Snapshot,
};
use crate::traits::{CoordinatorMap, CoordinatorStubFactory, EnterHandlers, PropagationHandler};

/// Commands [`Manager`] processes. Every read/write of the fixed-keys database goes through one
/// of these; nothing outside [`actor`](self) ever locks or shares that state directly.
enum Cmd {
    NumberOfNodes {
        reply: oneshot::Sender<u64>,
    },
    ReserveEnter {
        top: usize,
        request_id: i64,
        reply: oneshot::Sender<Result<Reservation, ReserveError>>,
    },
    DeleteReserveEnter {
        top: usize,
        request_id: i64,
        reply: oneshot::Sender<()>,
    },
    GetHookingMemory {
        top: usize,
        reply: oneshot::Sender<Option<TypedValue>>,
    },
    SetHookingMemory {
        top: usize,
        data: Option<TypedValue>,
        reply: oneshot::Sender<()>,
    },
    ApplyReplica {
        top: usize,
        memory: GnodeMemory,
        reply: oneshot::Sender<()>,
    },
    CheckPropagation {
        positions: Vec<u32>,
        fp_id: i64,
        propagation_id: i32,
        level: usize,
        reply: oneshot::Sender<bool>,
    },
    ExpirePropagation {
        propagation_id: i32,
    },
    NextPropagationId {
        reply: oneshot::Sender<i32>,
    },
    HandOff {
        reply: oneshot::Sender<HandOff>,
    },
    InvalidateNNodes,
}

struct State {
    topology: Topology,
    map: Arc<dyn CoordinatorMap>,
    config: Config,
    /// Keyed by `top` (`CoordinatorKey.lvl`, `1..=levels`).
    memory: BTreeMap<usize, GnodeMemory>,
    recent_propagations: BTreeSet<i32>,
    next_propagation_id: i32,
    snapshot_tx: watch::Sender<Snapshot>,
    events_tx: broadcast::Sender<Event>,
}

impl State {
    fn publish_snapshot(&self) {
        self.snapshot_tx.send_replace(Arc::new(self.memory.clone()));
    }

    /// `fk_database.vala:502-573`'s reserve protocol: idempotent by `request_id`, real-position
    /// preference with a virtual fallback, monotonic eldership.
    fn reserve_enter(&mut self, top: usize, request_id: i64) -> Result<Reservation, ReserveError> {
        if top < 1 || top > self.topology.levels() {
            return Err(ReserveError::TopOutOfRange(top));
        }
        let level = top - 1;
        if !self.map.can_reserve(level) {
            return Err(ReserveError::CannotReserve(top));
        }
        let now = Instant::now();
        let ttl = self.config.booking_ttl;
        let mem = self
            .memory
            .get_mut(&top)
            .expect("every top in 1..=levels is pre-populated at construction");
        // Purge expired bookings first (fk_database.vala:509-522).
        mem.reserve_list.retain(|b| b.expires_at > now);

        if let Some(existing) = mem
            .reserve_list
            .iter_mut()
            .find(|b| b.reserve_request_id == request_id)
        {
            existing.expires_at = now + ttl;
            let reservation = Reservation {
                new_pos: existing.new_pos,
                new_eldership: existing.new_eldership,
            };
            self.publish_snapshot();
            let _ = self.events_tx.send(Event::Reserved { top, reservation });
            return Ok(reservation);
        }

        let taken: BTreeSet<u32> = mem.reserve_list.iter().map(|b| b.new_pos).collect();
        let free = self.map.free_positions(level);
        let new_pos = free
            .iter()
            .copied()
            .find(|p| !taken.contains(p))
            .unwrap_or_else(|| {
                mem.max_virtual_pos += 1;
                mem.max_virtual_pos
            });
        tracing::info!(
            top,
            level,
            request_id,
            my_pos = self.map.my_pos(level),
            ?free,
            ?taken,
            new_pos,
            "coordinator: reserve_enter granted"
        );
        mem.max_eldership += 1;
        let reservation = Reservation {
            new_pos,
            new_eldership: mem.max_eldership,
        };
        mem.reserve_list.push(Booking {
            reserve_request_id: request_id,
            new_pos,
            new_eldership: mem.max_eldership,
            expires_at: now + ttl,
        });
        self.publish_snapshot();
        let _ = self.events_tx.send(Event::Reserved { top, reservation });
        Ok(reservation)
    }

    fn delete_reserve_enter(&mut self, top: usize, request_id: i64) {
        if let Some(mem) = self.memory.get_mut(&top) {
            mem.reserve_list
                .retain(|b| b.reserve_request_id != request_id);
        }
        self.publish_snapshot();
        let _ = self.events_tx.send(Event::ReserveDeleted {
            top,
            reserve_request_id: request_id,
        });
    }

    /// `NumberOfNodesRequest` always targets `CoordinatorKey(levels)` — the whole network
    /// (`fk_database.vala:118-119`).
    fn number_of_nodes(&mut self) -> u64 {
        let top = self.topology.levels();
        let now = Instant::now();
        let cache_ttl = self.config.n_nodes_cache_ttl;
        let map = self.map.clone();
        let mem = self
            .memory
            .get_mut(&top)
            .expect("every top in 1..=levels is pre-populated at construction");
        let n = match mem.n_nodes {
            Some((n, expiry)) if expiry > now => n,
            _ => map.n_nodes(),
        };
        mem.n_nodes = Some((n, now + cache_ttl));
        self.publish_snapshot();
        n
    }

    /// This identity's own g-node membership just changed (a member entered, migrated, or the
    /// network split) — the cached whole-network `get_n_nodes` answer is stale immediately, not
    /// merely after `Config::n_nodes_cache_ttl`. See [`Handle::invalidate_n_nodes`]'s doc for why
    /// this diverges from upstream's pure-TTL cache.
    fn invalidate_n_nodes(&mut self) {
        let top = self.topology.levels();
        if let Some(mem) = self.memory.get_mut(&top) {
            mem.n_nodes = None;
        }
        self.publish_snapshot();
    }

    fn get_hooking_memory(&self, top: usize) -> Option<TypedValue> {
        self.memory.get(&top).and_then(|m| m.hooking_memory.clone())
    }

    fn set_hooking_memory(&mut self, top: usize, data: Option<TypedValue>) {
        if let Some(mem) = self.memory.get_mut(&top) {
            mem.hooking_memory = data;
        }
        self.publish_snapshot();
        let _ = self.events_tx.send(Event::HookingMemoryChanged { top });
    }

    /// Replaces every field of `top`'s record from an incoming `ReplicaRequest`
    /// (`fk_database.vala:597-601`) **except** the local `n_nodes` cache: a replica is a
    /// snapshot as of when the sender scheduled the (fire-and-forget) replicate fanout, and
    /// `request_all_replicas_in_tasklet`'s first hop routes to the closest node for the target
    /// tuple — typically the very node that just answered the request and may have since
    /// invalidated its own cache on a membership change. Letting a since-superseded snapshot
    /// overwrite that strictly newer local knowledge would silently resurrect the stale count
    /// [`Handle::invalidate_n_nodes`] exists to avoid. Upstream carries `n_nodes`/
    /// `n_nodes_timeout` inside the same serialized record and applies it just as wholesale
    /// (`serializables.vala:187-189`, `fk_database.vala:598-600`) — this diverges from it because
    /// the cache it would clobber didn't exist there either.
    fn apply_replica(&mut self, top: usize, memory: GnodeMemory) {
        let n_nodes = self.memory.get(&top).and_then(|m| m.n_nodes);
        self.memory.insert(top, GnodeMemory { n_nodes, ..memory });
        self.publish_snapshot();
    }

    /// `CoordinatorManager.check_propagation` (`coord.vala:424-440`): dedup by `propagation_id`,
    /// then confirm the tuple/`fp_id` still name *my* current g-node at `level`.
    fn check_propagation(
        &mut self,
        positions: &[u32],
        fp_id: i64,
        propagation_id: i32,
        level: usize,
    ) -> bool {
        if self.recent_propagations.contains(&propagation_id) {
            tracing::info!(
                propagation_id,
                "coordinator: check_propagation -> already seen, dropping"
            );
            return false;
        }
        if level >= self.topology.levels() || positions.len() != self.topology.levels() - level {
            tracing::info!(
                propagation_id,
                level,
                topology_levels = self.topology.levels(),
                positions_len = positions.len(),
                "coordinator: check_propagation -> level/positions length mismatch, dropping"
            );
            return false;
        }
        for (i, &p) in positions.iter().enumerate() {
            let mine = self.map.my_pos(level + i);
            if p != mine {
                tracing::info!(
                    propagation_id,
                    level,
                    i,
                    their_pos = p,
                    my_pos = mine,
                    "coordinator: check_propagation -> position mismatch, dropping"
                );
                return false;
            }
        }
        let my_fp_id = self.map.fp_id(level);
        if fp_id != my_fp_id {
            tracing::info!(
                propagation_id,
                level,
                their_fp_id = fp_id,
                my_fp_id,
                "coordinator: check_propagation -> fp_id mismatch, dropping"
            );
            return false;
        }
        self.recent_propagations.insert(propagation_id);
        tracing::info!(
            propagation_id,
            level,
            "coordinator: check_propagation -> accepted"
        );
        true
    }

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::NumberOfNodes { reply } => {
                let _ = reply.send(self.number_of_nodes());
            }
            Cmd::ReserveEnter {
                top,
                request_id,
                reply,
            } => {
                let _ = reply.send(self.reserve_enter(top, request_id));
            }
            Cmd::DeleteReserveEnter {
                top,
                request_id,
                reply,
            } => {
                self.delete_reserve_enter(top, request_id);
                let _ = reply.send(());
            }
            Cmd::GetHookingMemory { top, reply } => {
                let _ = reply.send(self.get_hooking_memory(top));
            }
            Cmd::SetHookingMemory { top, data, reply } => {
                self.set_hooking_memory(top, data);
                let _ = reply.send(());
            }
            Cmd::ApplyReplica { top, memory, reply } => {
                self.apply_replica(top, memory);
                let _ = reply.send(());
            }
            Cmd::CheckPropagation {
                positions,
                fp_id,
                propagation_id,
                level,
                reply,
            } => {
                let _ =
                    reply.send(self.check_propagation(&positions, fp_id, propagation_id, level));
            }
            Cmd::ExpirePropagation { propagation_id } => {
                self.recent_propagations.remove(&propagation_id);
            }
            Cmd::NextPropagationId { reply } => {
                let id = self.next_propagation_id;
                self.next_propagation_id = self.next_propagation_id.wrapping_add(1);
                let _ = reply.send(id);
            }
            Cmd::HandOff { reply } => {
                let _ = reply.send(HandOff(self.memory.clone()));
            }
            Cmd::InvalidateNNodes => {
                self.invalidate_n_nodes();
            }
        }
    }
}

/// The single-owner actor. Spawn with [`Manager::run`]; interact only through the [`Handle`] it
/// returns.
pub struct Manager {
    state: State,
    cmd_rx: mpsc::Receiver<Cmd>,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager").finish_non_exhaustive()
    }
}

impl Manager {
    /// Builds a `Manager` and its [`Handle`] for a node running the Coordinator servant role
    /// (`CoordinatorManager`/`CoordService`, `research/impl/vala/coordinator/coord.vala:93-146`).
    ///
    /// `handoff`, if given, is a retiring identity's exported [`HandOff`]
    /// (`Handle::hand_off`) — the coordinator hand-off protocol
    /// (`coord.vala:142-146`). Every level not present in `handoff` starts fresh
    /// (`CoordService.new_coordgnodememory`).
    #[must_use]
    pub fn new(
        topology: Topology,
        map: Arc<dyn CoordinatorMap>,
        stub_factory: Arc<dyn CoordinatorStubFactory>,
        propagation_handler: Arc<dyn PropagationHandler>,
        enter_handlers: EnterHandlers,
        config: Config,
        handoff: Option<HandOff>,
    ) -> (Self, Handle) {
        let levels = topology.levels();
        let mut memory = BTreeMap::new();
        for top in 1..=levels {
            let gsize = topology.gsize(top - 1).expect("top - 1 < levels");
            let mem = handoff
                .as_ref()
                .and_then(|h| h.0.get(&top).cloned())
                .unwrap_or_else(|| GnodeMemory::fresh(gsize));
            memory.insert(top, mem);
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(memory.clone()));
        let (events_tx, _events_rx) = broadcast::channel(256);
        let state = State {
            topology: topology.clone(),
            map: map.clone(),
            config,
            memory,
            recent_propagations: BTreeSet::new(),
            next_propagation_id: 1,
            snapshot_tx,
            events_tx: events_tx.clone(),
        };
        let handle = Handle {
            topology,
            map,
            stub_factory,
            propagation_handler,
            enter_handlers: Arc::new(enter_handlers),
            config,
            cmd_tx,
            snapshot_rx,
            events_tx,
        };
        (Self { state, cmd_rx }, handle)
    }

    /// Runs the actor loop until `cancel` fires or every [`Handle`] is dropped.
    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.state.handle(cmd),
                        None => return,
                    }
                }
            }
        }
    }
}

/// Cheap-clone handle to a running [`Manager`]. The only way to interact with it.
#[derive(Clone)]
pub struct Handle {
    topology: Topology,
    map: Arc<dyn CoordinatorMap>,
    stub_factory: Arc<dyn CoordinatorStubFactory>,
    propagation_handler: Arc<dyn PropagationHandler>,
    enter_handlers: Arc<EnterHandlers>,
    config: Config,
    cmd_tx: mpsc::Sender<Cmd>,
    snapshot_rx: watch::Receiver<Snapshot>,
    events_tx: broadcast::Sender<Event>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("topology", &self.topology)
            .finish_non_exhaustive()
    }
}

impl Handle {
    /// Sends `f`'s command and awaits its reply. Returns `None` once the actor has already shut
    /// down (cancelled): `Manager::run` returning drops `cmd_rx` and every in-flight reply
    /// `oneshot::Sender` along with `State`, so a `Handle` call racing (or simply arriving
    /// after) that shutdown finds a closed channel — an ordinary, expected outcome of
    /// cancellation, not a bug, so callers get `None` to fold into their own "nothing/unknown/
    /// give up cleanly" case rather than a panic (mirrors `ntk_peerservices::actor::Handle::call`).
    async fn call<T>(&self, f: impl FnOnce(oneshot::Sender<T>) -> Cmd) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(f(tx)).await.ok()?;
        rx.await.ok()
    }

    /// Sends `cmd` without awaiting a reply. A closed channel means the actor already shut down
    /// — the same ordinary outcome [`Handle::call`] documents — so it is silently dropped rather
    /// than treated as an error.
    async fn cast(&self, cmd: Cmd) {
        let _ = self.cmd_tx.send(cmd).await;
    }

    /// The [`Topology`] this Coordinator instance is running on.
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// The injectable [`Config`] this instance was constructed with.
    #[must_use]
    pub fn config(&self) -> Config {
        self.config
    }

    /// A read-only, always-current snapshot of the local fixed-keys database.
    #[must_use]
    pub fn snapshot(&self) -> watch::Receiver<Snapshot> {
        self.snapshot_rx.clone()
    }

    /// The record for `top` in the last published [`crate::Snapshot`], with no actor round trip
    /// (backed by the same `watch` channel [`Handle::snapshot`] reads).
    #[must_use]
    pub(crate) fn memory_snapshot(&self, top: usize) -> Option<GnodeMemory> {
        self.snapshot_rx.borrow().get(&top).cloned()
    }

    /// Subscribes to fixed-keys-database change events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events_tx.subscribe()
    }

    /// Exports every level's current record, for handing off to the `Manager` a migrating
    /// identity spawns next (`Manager::new`'s `handoff` parameter). Falls back to an empty
    /// hand-off if the actor already shut down — the migrating identity then just starts every
    /// level fresh, exactly as if no prior generation existed to hand off from.
    pub async fn hand_off(&self) -> HandOff {
        self.call(|reply| Cmd::HandOff { reply })
            .await
            .unwrap_or_default()
    }

    // -- fk_database.vala request surface, servant-side --

    /// `None` if the actor already shut down.
    pub(crate) async fn number_of_nodes(&self) -> Option<u64> {
        self.call(|reply| Cmd::NumberOfNodes { reply }).await
    }

    /// `None` if the actor already shut down.
    pub(crate) async fn reserve_enter(
        &self,
        top: usize,
        request_id: i64,
    ) -> Option<Result<Reservation, ReserveError>> {
        self.call(|reply| Cmd::ReserveEnter {
            top,
            request_id,
            reply,
        })
        .await
    }

    pub(crate) async fn delete_reserve_enter(&self, top: usize, request_id: i64) {
        self.call(|reply| Cmd::DeleteReserveEnter {
            top,
            request_id,
            reply,
        })
        .await;
    }

    /// Actor shutdown and "nothing recorded at this level" both collapse to `None`.
    pub(crate) async fn hooking_memory(&self, top: usize) -> Option<TypedValue> {
        self.call(|reply| Cmd::GetHookingMemory { top, reply })
            .await
            .flatten()
    }

    pub(crate) async fn set_hooking_memory(&self, top: usize, data: Option<TypedValue>) {
        self.call(|reply| Cmd::SetHookingMemory { top, data, reply })
            .await;
    }

    pub(crate) async fn apply_replica(&self, top: usize, memory: GnodeMemory) {
        self.call(|reply| Cmd::ApplyReplica { top, memory, reply })
            .await;
    }

    pub(crate) async fn evaluate_enter(
        &self,
        top: usize,
        data: TypedValue,
        client_tuple: &[u32],
    ) -> TypedValue {
        self.enter_handlers
            .evaluate_enter
            .evaluate_enter(top, data, client_tuple)
            .await
    }

    pub(crate) async fn begin_enter(
        &self,
        top: usize,
        data: TypedValue,
        client_tuple: &[u32],
    ) -> TypedValue {
        self.enter_handlers
            .begin_enter
            .begin_enter(top, data, client_tuple)
            .await
    }

    pub(crate) async fn completed_enter(
        &self,
        top: usize,
        data: TypedValue,
        client_tuple: &[u32],
    ) -> TypedValue {
        self.enter_handlers
            .completed_enter
            .completed_enter(top, data, client_tuple)
            .await
    }

    pub(crate) async fn abort_enter(
        &self,
        top: usize,
        data: TypedValue,
        client_tuple: &[u32],
    ) -> TypedValue {
        self.enter_handlers
            .abort_enter
            .abort_enter(top, data, client_tuple)
            .await
    }

    // -- propagation --

    /// `None` if the actor already shut down.
    async fn next_propagation_id(&self) -> Option<i32> {
        self.call(|reply| Cmd::NextPropagationId { reply }).await
    }

    /// Actor shutdown and "not a live propagation" both collapse to `false` — either way the
    /// caller has nothing to apply or fan further.
    async fn check_propagation(&self, args: &PropagationArgs) -> bool {
        self.call(|reply| Cmd::CheckPropagation {
            positions: args.positions.clone(),
            fp_id: args.fp_id,
            propagation_id: args.propagation_id,
            level: args.level,
            reply,
        })
        .await
        .unwrap_or(false)
    }

    /// Drops the cached whole-network `get_n_nodes` answer so the very next ask sees the new
    /// size immediately. Upstream caches `get_n_nodes` on a pure TTL with no invalidation path
    /// at all (`research/impl/vala/coordinator/fk_database.vala:426-436`, `msec_n_nodes`,
    /// `peer_service.vala:29`); this crate diverges from it because the merge-direction decision
    /// `n_nodes` feeds (`ntk-hooking`'s `merge_tiebreak`) is irreversible — a stale,
    /// pre-absorption count picks the wrong direction and nothing re-evaluates it afterwards.
    /// Called on every confirmed membership-changing propagation
    /// (`finish_enter`/`finish_migration`/`we_have_splitted`, both self-initiated and the
    /// server-received `handle_execute_*` counterparts) — never on `prepare_*`, which is only a
    /// notice, not yet a committed change.
    async fn invalidate_n_nodes(&self) {
        self.cast(Cmd::InvalidateNNodes).await;
    }

    fn schedule_propagation_cleanup(&self, propagation_id: i32) {
        let handle = self.clone();
        let retention = self.config.propagation_retention;
        tokio::spawn(async move {
            tokio::time::sleep(retention).await;
            handle.cast(Cmd::ExpirePropagation { propagation_id }).await;
        });
    }

    /// Builds a fresh propagation envelope for `level` (`CoordinatorManager.prepare_propagation`,
    /// `coord.vala:229-237`). Deviates from upstream's random `propagation_id`
    /// (`PRNGen.int_range`) with a per-actor monotonic counter: dedup only needs uniqueness
    /// among *my own* concurrently-live propagations, which a counter guarantees deterministically
    /// without an RNG dependency (this crate's dependency list has none).
    ///
    /// `None` if the actor already shut down — every caller below gives up cleanly rather than
    /// fan out a propagation nothing will ever locally apply.
    async fn prepare_propagation(&self, level: usize, data: TypedValue) -> Option<PropagationArgs> {
        let positions = (level..self.topology.levels())
            .map(|l| self.map.my_pos(l))
            .collect();
        let fp_id = self.map.fp_id(level);
        let propagation_id = self.next_propagation_id().await?;
        Some(PropagationArgs {
            positions,
            fp_id,
            propagation_id,
            level,
            data,
        })
    }

    /// `prepare_migration`: fan out to each neighbor, then apply locally inline
    /// (`coord.vala:261-281`).
    pub async fn prepare_migration(&self, level: usize, data: TypedValue) {
        let Some(args) = self.prepare_propagation(level, data).await else {
            return;
        };
        for stub in self.stub_factory.stub_for_each_neighbor() {
            let _ = stub.execute_prepare_migration(args.clone()).await;
        }
        self.propagation_handler
            .prepare_migration(args.level, args.data.clone())
            .await;
        self.schedule_propagation_cleanup(args.propagation_id);
    }

    /// `finish_migration`: fan out to the all-neighbors group, then apply locally in a spawned
    /// task — upstream does not make the caller wait for local completion (`coord.vala:283-320`).
    /// That task can legitimately outlive the actor generation it belongs to (this generation's
    /// own teardown racing its own `finish_migration` fanout); it is intentionally fire-and-forget
    /// and cancellation-safe — every call it makes back into `handle` folds a closed channel into
    /// an ordinary no-op instead of panicking (see `Handle::call`/`Handle::cast`).
    pub async fn finish_migration(&self, level: usize, data: TypedValue) {
        let Some(args) = self.prepare_propagation(level, data).await else {
            return;
        };
        self.invalidate_n_nodes().await;
        let _ = self
            .stub_factory
            .stub_for_all_neighbors()
            .execute_finish_migration(args.clone())
            .await;
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .propagation_handler
                .finish_migration(args.level, args.data.clone())
                .await;
            handle.schedule_propagation_cleanup(args.propagation_id);
        });
    }

    /// `prepare_enter` (`coord.vala:322-342`), same shape as [`Handle::prepare_migration`].
    pub async fn prepare_enter(&self, level: usize, data: TypedValue) {
        let Some(args) = self.prepare_propagation(level, data).await else {
            return;
        };
        for stub in self.stub_factory.stub_for_each_neighbor() {
            let _ = stub.execute_prepare_enter(args.clone()).await;
        }
        self.propagation_handler
            .prepare_enter(args.level, args.data.clone())
            .await;
        self.schedule_propagation_cleanup(args.propagation_id);
    }

    /// `finish_enter` (`coord.vala:344-381`), same shape as [`Handle::finish_migration`] — the
    /// spawned task's callback into `handle.schedule_propagation_cleanup` is the exact site the
    /// original panic-on-shutdown defect fired at; see that method's doc comment for why racing
    /// this generation's own teardown is now an ordinary, safe outcome rather than a bug.
    pub async fn finish_enter(&self, level: usize, data: TypedValue) {
        let Some(args) = self.prepare_propagation(level, data).await else {
            return;
        };
        self.invalidate_n_nodes().await;
        let _ = self
            .stub_factory
            .stub_for_all_neighbors()
            .execute_finish_enter(args.clone())
            .await;
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .propagation_handler
                .finish_enter(args.level, args.data.clone())
                .await;
            handle.schedule_propagation_cleanup(args.propagation_id);
        });
    }

    /// `we_have_splitted` (`coord.vala:383-420`), same shape as [`Handle::finish_migration`].
    pub async fn we_have_splitted(&self, level: usize, data: TypedValue) {
        let Some(args) = self.prepare_propagation(level, data).await else {
            return;
        };
        self.invalidate_n_nodes().await;
        let _ = self
            .stub_factory
            .stub_for_all_neighbors()
            .execute_we_have_splitted(args.clone())
            .await;
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .propagation_handler
                .we_have_splitted(args.level, args.data.clone())
                .await;
            handle.schedule_propagation_cleanup(args.propagation_id);
        });
    }

    /// Server-received counterpart of [`Handle::prepare_migration`]
    /// (`CoordinatorManager.execute_prepare_migration`, `coord.vala:442-465`).
    pub(crate) async fn handle_execute_prepare_migration(&self, args: PropagationArgs) {
        if !self.check_propagation(&args).await {
            return;
        }
        for stub in self.stub_factory.stub_for_each_neighbor() {
            let _ = stub.execute_prepare_migration(args.clone()).await;
        }
        self.propagation_handler
            .prepare_migration(args.level, args.data.clone())
            .await;
        self.schedule_propagation_cleanup(args.propagation_id);
    }

    /// Server-received counterpart of [`Handle::finish_migration`] (`coord.vala:467-486`).
    pub(crate) async fn handle_execute_finish_migration(&self, args: PropagationArgs) {
        if !self.check_propagation(&args).await {
            return;
        }
        self.invalidate_n_nodes().await;
        let _ = self
            .stub_factory
            .stub_for_all_neighbors()
            .execute_finish_migration(args.clone())
            .await;
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .propagation_handler
                .finish_migration(args.level, args.data.clone())
                .await;
            handle.schedule_propagation_cleanup(args.propagation_id);
        });
    }

    /// Server-received counterpart of [`Handle::prepare_enter`] (`coord.vala:488-511`).
    pub(crate) async fn handle_execute_prepare_enter(&self, args: PropagationArgs) {
        if !self.check_propagation(&args).await {
            return;
        }
        for stub in self.stub_factory.stub_for_each_neighbor() {
            let _ = stub.execute_prepare_enter(args.clone()).await;
        }
        self.propagation_handler
            .prepare_enter(args.level, args.data.clone())
            .await;
        self.schedule_propagation_cleanup(args.propagation_id);
    }

    /// Server-received counterpart of [`Handle::finish_enter`] (`coord.vala:513-532`).
    pub(crate) async fn handle_execute_finish_enter(&self, args: PropagationArgs) {
        if !self.check_propagation(&args).await {
            return;
        }
        self.invalidate_n_nodes().await;
        let _ = self
            .stub_factory
            .stub_for_all_neighbors()
            .execute_finish_enter(args.clone())
            .await;
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .propagation_handler
                .finish_enter(args.level, args.data.clone())
                .await;
            handle.schedule_propagation_cleanup(args.propagation_id);
        });
    }

    /// Server-received counterpart of [`Handle::we_have_splitted`] (`coord.vala:534-552`).
    pub(crate) async fn handle_execute_we_have_splitted(&self, args: PropagationArgs) {
        if !self.check_propagation(&args).await {
            return;
        }
        self.invalidate_n_nodes().await;
        let _ = self
            .stub_factory
            .stub_for_all_neighbors()
            .execute_we_have_splitted(args.clone())
            .await;
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .propagation_handler
                .we_have_splitted(args.level, args.data.clone())
                .await;
            handle.schedule_propagation_cleanup(args.propagation_id);
        });
    }
}
