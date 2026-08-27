//! Participation-map flood-gossip: registering a service, propagating a new `set_participant`
//! fact, and merging/forwarding a neighbor's `give_participant_maps` snapshot
//! (`research/impl/vala/peerservices/map_handler.vala`).

use std::sync::Arc;
use std::time::Duration;

use ntk_common::HCoord;

use crate::actor::Handle;
use crate::participation::ParticipantSet;
use crate::service::{PeerService, ServiceId};
use crate::tuple::{GNodeRelation, TupleGNode, convert_tuple_gnode, make_tuple_gnode};

/// How long a just-published `set_participant` fact is remembered to suppress re-flooding a
/// duplicate (`RecentPublishedListRemoveTasklet`, `research/impl/vala/peerservices/
/// map_handler.vala:414-429`).
const RECENT_PUBLISHED_TTL: Duration = Duration::from_secs(60);

async fn flood_set_participant(handle: &Handle, p_id: ServiceId, gn: &TupleGNode) {
    for neighbor in handle.env.neighbors() {
        // Best-effort: upstream ignores a failed gossip hop too, only signaling it
        // (`map_handler.vala:159-183,283-301`) — that signal depends on Neighborhood arc
        // identity this crate does not have (see `RoutingEnv::neighbors`'s doc comment).
        let _ = neighbor.set_participant(p_id, gn.clone()).await;
    }
}

/// Re-floods every locally-registered optional service's participation fact — the periodic
/// insurance repeat of [`Handle::register`]'s initial flood, driven by the owning
/// [`crate::actor::Manager`] every [`crate::config::Config::participation_reannounce_interval`]
/// when that field is set (`participate_tasklet`,
/// `research/impl/vala/peerservices/map_handler.vala:331-362`). Reuses
/// [`flood_set_participant`] rather than a second gossip mechanism, so a periodic re-announce is
/// wire-identical to the reactive one `register` already sends. A no-op if this node has no
/// locally-registered optional services (or the actor has already shut down).
pub(crate) async fn reannounce_participation(handle: &Handle) {
    let services = handle.my_optional_services().await;
    if services.is_empty() {
        return;
    }
    let gn = make_tuple_gnode(
        handle.topology(),
        handle.my_pos().positions(),
        HCoord::new(0, handle.my_pos().positions()[0]),
        handle.topology().levels(),
    );
    for p_id in services {
        flood_set_participant(handle, p_id, &gn).await;
    }
}

impl Handle {
    /// Registers `service`, auto-participating and flooding that fact once if it is optional
    /// (`PeersManager.register`, `research/impl/vala/peerservices/peers.vala:397-406`;
    /// `participate`, `peers.vala:309-313`).
    ///
    /// **Periodic re-announce**: upstream also runs this same flood forever afterward as
    /// insurance against a lost delivery — 5 times every 5 minutes, then randomly every 1-2 days
    /// (`participate_tasklet`, `research/impl/vala/peerservices/map_handler.vala:331-362`). This
    /// crate models that as a single fixed cadence instead of upstream's exact schedule: set
    /// [`crate::Config::participation_reannounce_interval`] and the owning
    /// [`crate::actor::Manager`] repeats this flood (via [`reannounce_participation`]) for every
    /// locally-registered optional service every such interval. Left `None` (the default), no
    /// re-announce ever fires, matching this crate's original hand-off behavior.
    pub async fn register(&self, service: Arc<dyn PeerService>) {
        let p_id = service.service_id();
        let optional = service.is_optional();
        self.register_cmd(service).await;
        if optional {
            let gn = make_tuple_gnode(
                self.topology(),
                self.my_pos().positions(),
                HCoord::new(0, self.my_pos().positions()[0]),
                self.topology().levels(),
            );
            flood_set_participant(self, p_id, &gn).await;
        }
    }

    /// Re-floods every locally-registered optional service's participation fact right now — the
    /// event-driven counterpart to [`crate::Config::participation_reannounce_interval`]'s
    /// periodic timer, for a caller that needs to re-drive it on demand rather than wait for the
    /// next tick. Exists because [`Self::register`]'s own flood is a one-shot fired wherever the
    /// caller first constructs this `Handle`'s owner — for `ntkd` (`ntkd::node::services::spawn`)
    /// that is at process boot, before any neighbor is reachable, so it always reaches zero
    /// neighbors; every other g-node keeps treating this node's optional services as
    /// non-participant (`crate::actor`'s `non_participant_gnodes` gate) until either this is
    /// called or the periodic re-announce's first tick, up to
    /// [`crate::Config::participation_reannounce_interval`] later. `ntkd` calls this once this
    /// node actually gains a reachable neighbor (`ntkd::node::lifecycle`'s own qspn/neighborhood
    /// event handling).
    pub async fn reannounce_participation(&self) {
        reannounce_participation(self).await;
    }

    /// Applies and re-floods an inbound `set_participant` fact, deduplicating recent republishes
    /// (`MapHandler.set_participant`, `research/impl/vala/peerservices/map_handler.vala:383-418`).
    pub(crate) async fn handle_set_participant(&self, p_id: ServiceId, tuple: TupleGNode) {
        let (relation, at) = convert_tuple_gnode(self.my_pos().positions(), &tuple);
        if relation == GNodeRelation::Mine {
            return;
        }
        let Some(at) = self.apply_participant(p_id, at).await else {
            return;
        };
        let gn = make_tuple_gnode(
            self.topology(),
            self.my_pos().positions(),
            at,
            self.topology().levels(),
        );
        flood_set_participant(self, p_id, &gn).await;

        let handle = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(RECENT_PUBLISHED_TTL).await;
            handle.expire_recently_published(at).await;
        });
    }

    /// Applies and re-forwards an inbound `give_participant_maps` snapshot if it is fresher than
    /// what I already know (`MapHandler.give_participant_maps`/`copy_and_forward`,
    /// `research/impl/vala/peerservices/map_handler.vala:238-301`).
    pub(crate) async fn handle_give_participant_maps(&self, maps: ParticipantSet) {
        if let Some(forward) = self.apply_participant_set(maps).await {
            for neighbor in self.env.neighbors() {
                let _ = neighbor.give_participant_maps(forward.clone()).await;
            }
        }
    }
}

/// Pins the fix for `ntkd::node::lifecycle`'s "gains its first arc" trigger: a node that
/// registers an optional service before it has any neighbor (`ntkd::node::services::spawn`'s
/// own boot-time `andna.register_services()`, before any arc exists) must have its peers stop
/// treating that g-node as a non-participant once it actually gains a neighbor and re-announces
/// — not stay a non-participant for the rest of `Config::participation_reannounce_interval`'s
/// own steady-state cadence (5 minutes in production, `ntkd::node::services::peers_config`'s own
/// doc).
#[cfg(test)]
mod reannounce_participation_tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    use futures::future::BoxFuture;
    use ntk_common::{HCoord, Naddr, Topology};
    use ntk_proto::v1::TypedValue;
    use tokio_util::sync::CancellationToken;

    use super::Handle;
    use crate::actor::Manager;
    use crate::config::Config;
    use crate::participation::ParticipantSet;
    use crate::service::{ExecError, PeerService, ServiceId};
    use crate::stub::{
        GetRequestError, PeerMessageForwarder, PeersStub, RoutingEnv, StubCallError,
    };
    use crate::tuple::{TupleGNode, TupleNode};

    struct DummyOptionalService(ServiceId);
    impl PeerService for DummyOptionalService {
        fn service_id(&self) -> ServiceId {
            self.0
        }
        fn is_optional(&self) -> bool {
            true
        }
        fn exec<'a>(
            &'a self,
            request: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
            Box::pin(async move { Ok(request) })
        }
    }

    /// Delivers only `set_participant` straight into `target` (in-process, no wire encoding);
    /// every other call this test never exercises panics loudly instead of silently no-op-ing.
    struct BridgeStub {
        target: Handle,
    }

    impl PeersStub for BridgeStub {
        fn forward_peer_message(
            &self,
            _msg: PeerMessageForwarder,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn get_request(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> BoxFuture<'_, Result<TypedValue, GetRequestError>> {
            unreachable!("not exercised by this test")
        }
        fn set_response(
            &self,
            _msg_id: i32,
            _response: TypedValue,
            _respondant: TupleNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_refuse_message(
            &self,
            _msg_id: i32,
            _refusal: crate::service::Refusal,
            _respondant: TupleNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_redo_from_start(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_next_destination(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_failure(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_non_participant(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_missing_optional_maps(
            &self,
            _msg_id: i32,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_participant(
            &self,
            p_id: ServiceId,
            tuple: TupleGNode,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            let target = self.target.clone();
            Box::pin(async move {
                target.handle_set_participant(p_id, tuple).await;
                Ok(())
            })
        }
        fn give_participant_maps(
            &self,
            _maps: ParticipantSet,
        ) -> BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn ask_participant_maps(&self) -> BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// A [`RoutingEnv`] whose `neighbors()` reflects a slot that starts empty (no arc yet) and
    /// is filled once the test simulates gaining one; everything else this test never exercises.
    struct SlotEnv {
        neighbor: Mutex<Option<Arc<dyn PeersStub>>>,
    }
    impl RoutingEnv for SlotEnv {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            true
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
            self.neighbor
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .into_iter()
                .collect()
        }
    }

    #[tokio::test]
    async fn gaining_an_arc_after_boot_reannounces_participation_to_the_new_neighbor() {
        let topology = Topology::new([2, 2]).unwrap();
        let pos_a = Naddr::new(topology.clone(), vec![0, 0]).unwrap();
        let pos_b = Naddr::new(topology.clone(), vec![1, 0]).unwrap();
        let cancel = CancellationToken::new();

        let env_b: Arc<dyn RoutingEnv> = Arc::new(SlotEnv {
            neighbor: Mutex::new(None),
        });
        let (manager_b, handle_b) = Manager::new(
            topology.clone(),
            pos_b,
            env_b,
            Config::default(),
            topology.levels(),
        );
        let manager_b_task = tokio::spawn(manager_b.run(cancel.child_token()));

        let env_a_slot = Arc::new(SlotEnv {
            neighbor: Mutex::new(None),
        });
        let env_a: Arc<dyn RoutingEnv> = env_a_slot.clone();
        let (manager_a, handle_a) = Manager::new(
            topology.clone(),
            pos_a,
            env_a,
            Config::default(),
            topology.levels(),
        );
        let manager_a_task = tokio::spawn(manager_a.run(cancel.child_token()));

        let p_id = ServiceId::new(77);

        // Boot-time registration, exactly `ntkd::node::services::spawn`'s own ordering: before
        // any neighbor exists. `register`'s own flood reaches nobody.
        handle_a
            .register(Arc::new(DummyOptionalService(p_id)))
            .await;

        // Node B has no reason yet to treat node A's g-node as a participant.
        assert_eq!(
            handle_b.gnode_participates(p_id, topology.levels()).await,
            Some(false),
            "before any arc/flood, node B must not yet know node A participates"
        );

        // Node A "gains its first arc": a real stub reaching node B becomes reachable.
        *env_a_slot
            .neighbor
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(BridgeStub {
            target: handle_b.clone(),
        }) as Arc<dyn PeersStub>);

        // The fix under test: re-drive the flood now that a neighbor exists, exactly what
        // `ntkd::node::lifecycle`'s `on_neighborhood_event` now does on `ArcAdded`.
        handle_a.reannounce_participation().await;

        assert_eq!(
            handle_b.gnode_participates(p_id, topology.levels()).await,
            Some(true),
            "after gaining an arc and re-announcing, node B must now see node A's g-node as a \
             participant"
        );

        cancel.cancel();
        manager_a_task.await.unwrap();
        manager_b_task.await.unwrap();
    }
}
