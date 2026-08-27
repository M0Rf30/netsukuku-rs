//! [`CoordinatorService`]: the [`ntk_peerservices::PeerService`] registration that runs the
//! fixed-keys database as a DHT-hash-based election over PeerServices (`research/notes/01-vala-
//! core-routing.md` §7; `CoordService`, `research/impl/vala/coordinator/peer_service.vala:25-91`).

use std::fmt;

use futures::future::BoxFuture;
use ntk_peerservices::{ExecError, PeerService, Refusal, ServiceId};
use ntk_proto::v1::TypedValue;

use crate::actor::Handle;
use crate::domain::ReserveError;
use crate::wire::{RequestBody, ResponseBody, pack_response, unpack_request};

/// `coordinator_p_id = 1` (`research/impl/vala/coordinator/peer_service.vala:27`).
pub const SERVICE_ID: u16 = 1;

fn malformed(message: impl Into<String>) -> ExecError {
    // Only ever reached on a decode failure — every real caller goes through this crate's own
    // `CoordinatorClient`/`wire` module, which never produces a malformed `TypedValue`. Modeled
    // as a routing-level refuse-at-level-0 rather than a panic, since `exec` is reachable from
    // untrusted network input.
    ExecError::Refuse(Refusal {
        level: 0,
        message: message.into(),
    })
}

/// The [`PeerService`] registration that puts a [`Handle`] on the PeerServices substrate.
/// Mandatory (`is_optional() == false`), matching upstream's `base(coordinator_p_id, false)` —
/// every node implicitly participates, no gossip flood needed.
pub struct CoordinatorService {
    service_id: ServiceId,
    handle: Handle,
    peers: ntk_peerservices::Handle,
}

impl fmt::Debug for CoordinatorService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinatorService").finish_non_exhaustive()
    }
}

impl CoordinatorService {
    /// Registers this node as a Coordinator servant candidate. `peers` is used only to replicate
    /// a fixed-keys write to [`crate::Config::replica_fanout`] other nodes after it lands here
    /// (`request_all_replicas_in_tasklet`, `fk_database.vala:688-715`) — routing to the elected
    /// servant itself is `peers`' own `contact_peer`, invoked by whichever node calls
    /// [`crate::CoordinatorClient`].
    #[must_use]
    pub fn new(handle: Handle, peers: ntk_peerservices::Handle) -> Self {
        Self {
            service_id: ServiceId::new(SERVICE_ID),
            handle,
            peers,
        }
    }

    /// Fire-and-forget replication of the current record at `top` to
    /// [`crate::Config::replica_fanout`] other nodes (`request_all_replicas_in_tasklet`,
    /// `research/impl/vala/coordinator/fk_database.vala:688-715`).
    ///
    /// `top` is bounds-checked again here, immediately before `vec![0u32; top]`, even though
    /// every peer-reachable entry point that can reach `spawn_replicate`
    /// (`RequestBody::Replica`/`RequestBody::SetHookingMemory`) already validates it in
    /// `wire::unpack_request`: `self.handle.memory_snapshot(top)` above reads whatever is
    /// currently *stored*, and `apply_replica` (`actor.rs`) inserts into that store keyed by
    /// whatever `top` a `Replica` message carried at the time it was applied — a value that
    /// could predate this fix (a stale on-disk/in-memory record from before an upgrade, or a
    /// hand-off from a not-yet-patched peer during a rolling deploy). `vec![0u32; top]` is a
    /// function argument, evaluated eagerly *before* `TupleNode::new` ever runs its own
    /// `pos.len() > topology.levels()` check (`ntk-peerservices/src/tuple.rs`), so relying on
    /// that callee-side check cannot help: the allocation has already happened by the time
    /// `TupleNode::new` gets a chance to reject it. The only way to make this call site safe is
    /// to check `top` *before* constructing the `vec!` argument, not to reorder around
    /// `TupleNode::new` (which cannot see `top` until after the `Vec` it would reject already
    /// exists).
    fn spawn_replicate(&self, top: usize) {
        let Some(memory) = self.handle.memory_snapshot(top) else {
            return;
        };
        let levels = self.peers.topology().levels();
        if top < 1 || top > levels {
            tracing::warn!(
                top,
                levels,
                "coordinator: spawn_replicate refusing out-of-range top (stale/poisoned record)"
            );
            return;
        }
        let Ok(target) =
            ntk_peerservices::TupleNode::new(self.peers.topology().clone(), vec![0u32; top])
        else {
            return;
        };
        let peers = self.peers.clone();
        let service_id = self.service_id;
        let config = self.handle.config();
        let request = crate::wire::pack_request(&RequestBody::Replica { top, memory });
        tokio::spawn(async move {
            let _ = peers
                .replicate(
                    service_id,
                    target,
                    request,
                    config.replica_timeout,
                    config.replica_fanout,
                )
                .await;
        });
    }

    /// Reads this node's own local `hooking_memory` record for `top` — no DHT round trip:
    /// callers of this method are, by construction, already running as this key's elected
    /// servant (either because `exec` below is answering the DHT request that resolved *to*
    /// this node, or because a caller reached this same node's own [`Handle`] directly, e.g.
    /// [`ntkd`]'s `EnterArbiter`, which only ever runs inside the `EvaluateEnterRequest`
    /// handler for this exact `CoordinatorKey`).
    pub async fn hooking_memory_locally(&self, top: usize) -> Option<TypedValue> {
        self.handle.hooking_memory(top).await
    }

    /// Writes `data` to this node's own local `hooking_memory` record for `top` and replicates
    /// it — the same effect `exec`'s own `SetHookingMemoryRequest` handling has, factored out
    /// so a caller that is *already* this key's elected servant (see
    /// [`Self::hooking_memory_locally`]'s own doc) can skip the redundant DHT round trip a
    /// fresh `SetHookingMemoryRequest` through `contact_peer` would otherwise cost: that extra
    /// round trip, multiplied across every concurrent asker during a real multi-member merge,
    /// was measured adding enough latency to make unrelated `contact_peer` attempts time out
    /// and permanently abort (`ntk_hooking::arc::run_arc_handler`'s own "no participants"
    /// catch-all), not a merely theoretical concern.
    pub async fn set_hooking_memory_locally(&self, top: usize, data: Option<TypedValue>) {
        self.handle.set_hooking_memory(top, data).await;
        self.spawn_replicate(top);
    }
}

impl PeerService for CoordinatorService {
    fn service_id(&self) -> ServiceId {
        self.service_id
    }

    fn is_optional(&self) -> bool {
        false
    }

    fn exec<'a>(
        &'a self,
        request: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move {
            let body = unpack_request(&request, self.handle.topology().levels())
                .map_err(|e| malformed(e.to_string()))?;
            let response = match body {
                RequestBody::NumberOfNodes => {
                    let n = self
                        .handle
                        .number_of_nodes()
                        .await
                        .ok_or(ExecError::RedoFromStart)?;
                    self.spawn_replicate(self.handle.topology().levels());
                    ResponseBody::NumberOfNodes(n)
                }
                RequestBody::EvaluateEnter { top, data } => ResponseBody::EvaluateEnter(
                    self.handle.evaluate_enter(top, data, client_tuple).await,
                ),
                RequestBody::BeginEnter { top, data } => {
                    ResponseBody::BeginEnter(self.handle.begin_enter(top, data, client_tuple).await)
                }
                RequestBody::CompletedEnter { top, data } => ResponseBody::CompletedEnter(
                    self.handle.completed_enter(top, data, client_tuple).await,
                ),
                RequestBody::AbortEnter { top, data } => {
                    ResponseBody::AbortEnter(self.handle.abort_enter(top, data, client_tuple).await)
                }
                RequestBody::GetHookingMemory { top } => {
                    ResponseBody::GetHookingMemory(self.hooking_memory_locally(top).await)
                }
                RequestBody::SetHookingMemory { top, data } => {
                    // Upstream's identical-looking guard (`fk_database.vala:487-493`,
                    // `if (! client_tuple.is_empty) { ... tasklet.exit_tasklet(); }`) is
                    // enforceable only under upstream's own calling convention:
                    // `CoordinatorManager.set_hooking_memory` (`coord.vala:190-197`) requires
                    // its caller to have *already* verified `am_i_servant_for(k)` — i.e. that
                    // it already **is** the elected node for this key — before ever calling
                    // `CoordClient.set_hooking_memory`. Given that precondition, that client's
                    // own `contact_peer` for the same key always self-loops (zero hops), so
                    // `client_tuple` is empty for every legitimate call by construction; a
                    // non-empty `client_tuple` can then only mean the request was forwarded
                    // from a caller that skipped the precondition.
                    //
                    // This port has no such precondition: every caller of
                    // `CoordinatorClient::set_hooking_memory` (`get_n_nodes`/`reserve`/
                    // `delete_reserve`/`hooking_memory` share this — none of the other three
                    // guard `client_tuple` either) simply calls it and lets `contact_peer`'s own
                    // target-key resolution route to whichever node is elected, possibly via
                    // `forward_msg` hops when the caller itself is not that node — the ordinary,
                    // expected shape for any other member of the caller's own network asking
                    // its elected Coordinator to persist shared state (`decide_merge`'s memo).
                    // By the time `exec` runs here at all, `contact_peer`/`forward_msg` have
                    // *already* established this node is the resolved target for `top` — the
                    // property upstream's check was a redundant proxy for under its own
                    // narrower convention — regardless of `client_tuple`, so refusing on it
                    // here only rejects legitimate multi-hop writes without guarding anything
                    // this port's routing model doesn't already guarantee.
                    self.set_hooking_memory_locally(top, data).await;
                    ResponseBody::SetHookingMemory
                }
                RequestBody::ReserveEnter {
                    top,
                    reserve_request_id,
                } => match self.handle.reserve_enter(top, reserve_request_id).await {
                    Some(Ok(reservation)) => {
                        self.spawn_replicate(top);
                        ResponseBody::ReserveEnter(Some(reservation))
                    }
                    Some(Err(ReserveError::TopOutOfRange(_) | ReserveError::CannotReserve(_))) => {
                        ResponseBody::ReserveEnter(None)
                    }
                    // The actor already shut down (this generation is tearing down mid-request)
                    // — ask the caller to restart from scratch rather than answer with a made-up
                    // reservation.
                    None => return Err(ExecError::RedoFromStart),
                },
                RequestBody::DeleteReserveEnter {
                    top,
                    reserve_request_id,
                } => {
                    self.handle
                        .delete_reserve_enter(top, reserve_request_id)
                        .await;
                    self.spawn_replicate(top);
                    ResponseBody::DeleteReserveEnter
                }
                RequestBody::Replica { top, memory } => {
                    self.handle.apply_replica(top, memory).await;
                    ResponseBody::Replica
                }
            };
            Ok(pack_response(&response))
        })
    }
}
