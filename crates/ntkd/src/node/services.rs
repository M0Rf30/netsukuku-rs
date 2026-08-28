//! Wires the PeerServices substrate, Coordinator, ANDNA, and Hooking together for one identity —
//! the piece that actually registers `CoordinatorService`/`AndnaService`/`CounterService` on the
//! one `ntk_peerservices::Manager`, and resolves the Hooking<->Coordinator initialization cycle
//! (Hooking's constructor needs a `CoordinatorClient`; Coordinator's `PropagationHandler` needs
//! a `HookingHandle` — see [`crate::node::adapters::PropagationHandlerAdapter`]'s doc comment for how).

use std::sync::Arc;

use ntk_common::{Naddr, Topology};
use ntk_hooking::{HookingConfig, HookingHandle, HookingOrigin};
use ntk_peerservices::RoutingEnv;
use ntk_qspn::QspnHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::node::adapters::{
    CoordinatorClientAdapter, CoordinatorMapAdapter, CoordinatorStubFactoryAdapter, EnterArbiter,
    EnterHandlersAdapter, NetworkInfo, PropagationHandlerAdapter, QspnViewAdapter,
    RoutingEnvAdapter,
};
use crate::node::peers::PeerLinks;
use crate::node::registry::LinkRegistry;
use crate::node::stubs::HookingStubFactoryAdapter;

/// Where this generation's [`HookingHandle`] comes from — see `crate::node::lifecycle`'s
/// "Negotiated re-address" module doc section.
///
/// A negotiated re-address (`rehook`) fires only once hooking's own per-arc merge protocol has
/// *already* resolved this identity's entry (`HookingEvent::DoFinishEnter`) — there is nothing
/// left for a second `HookingHandle` to do: rebuilding it would only restart a merge negotiation
/// that already succeeded. So a rehook always carries the *same*, already-resolved handle over
/// into the new generation ([`Self::Carried`]); a *fresh* actor ([`Self::Fresh`]) is only ever
/// needed once, for this identity's very first (always `CreateNet`) generation.
#[derive(Debug)]
pub enum HookingProvenance {
    Fresh(HookingOrigin),
    Carried(HookingHandle),
}

/// Every service handle the rest of the daemon (lifecycle, status) needs after this identity's
/// full stack is up.
#[derive(Debug)]
pub struct Services {
    pub peers: ntk_peerservices::Handle,
    pub coordinator: ntk_coordinator::Handle,
    pub coordinator_client: ntk_coordinator::CoordinatorClient,
    pub andna: ntk_andna::Handle,
    pub hooking: HookingHandle,
    pub qspn_view: Arc<QspnViewAdapter>,
}

/// This daemon's own `PeerServices::Config`, upstream defaults plus one opt-in: a periodic
/// participation re-announce (`Config::participation_reannounce_interval`,
/// `research/impl/vala/peerservices/map_handler.vala:331-362`). Left disabled that field
/// changes nothing for any existing caller/test; this daemon turns it on because it is the
/// first caller for which the fact it insures against — a `set_participant` flood lost to a
/// dead/flapping arc — is actually observable: `ntk-neighborhood`'s own liveness detector
/// (failed `nop()`, `research/notes/01-vala-core-routing.md` §4 point 5) can tear an arc down
/// mid-flood, and unlike upstream's tiered "5 times every 5 minutes, then every 1-2 days"
/// schedule this crate models one fixed cadence (see that field's own doc). Five minutes
/// matches upstream's own initial burst interval — frequent enough to recover a lost flood
/// well before `ArcPhase`/route churn would otherwise mask it, cheap enough (one no-op flood
/// per locally-registered optional service when nothing changed) to run forever.
#[must_use]
fn peers_config(require_auth: bool) -> ntk_peerservices::Config {
    ntk_peerservices::Config {
        participation_reannounce_interval: Some(std::time::Duration::from_secs(300)),
        require_auth,
        ..ntk_peerservices::Config::default()
    }
}

/// This daemon's own [`ntk_coordinator::Config`]: upstream defaults except
/// [`ntk_coordinator::Config::n_nodes_cache_ttl`]. Upstream's own 20s default
/// (`CoordService.msec_stat`, `research/impl/vala/coordinator/peer_service.vala:29`) means a
/// g-node that just absorbed a new member can keep reporting its *pre-absorption* size to
/// `merge_tiebreak` for up to 20 more seconds — long enough, in a real multi-member merge, for
/// that stale count to make an already-larger g-node lose a tiebreak it should have won.
/// Confirmed by direct reproduction (`ntkd::node::negotiation_tests`, a 3-solitary-peer
/// convergence race) and by real-kernel capture of `two_star_groups_merge_into_one_network`:
/// a 2-member group's own `decide_merge` against a 1-member neighbor read back a cached `1`
/// for its *own* size milliseconds after its second member had already joined, tiebreaking on
/// `network_id` instead of size and sending the larger group into the smaller one. A short TTL
/// keeps the same crash-recovery cache (a servant restart still doesn't recompute on every
/// single ask) while actually reflecting membership changes on the timescale hooking's own
/// per-arc negotiation runs on, not the timescale a human administrator would notice a stale
/// dashboard count.
#[must_use]
pub(crate) fn coordinator_config() -> ntk_coordinator::Config {
    ntk_coordinator::Config {
        n_nodes_cache_ttl: std::time::Duration::from_millis(200),
        ..ntk_coordinator::Config::default()
    }
}

/// This daemon's own [`HookingConfig`]: upstream defaults except
/// [`HookingConfig::merge_reject_wait`]. Upstream's own flat 10-minute value
/// (`arc_handler.vala:212`, `tasklet.ms_wait(600000); // 10 minutes`) is not scaled by
/// [`HookingConfig::global_timeout`] the way every *other* arc-handler backoff is — every
/// other retry in this same state machine (`ask_again_wait`/`restart_wait`) resolves to
/// seconds for the small networks this daemon's own test suite (and any network under
/// [`HookingConfig::global_timeout`]'s `< 5`-node band) actually runs, so a merge decision that
/// resolves to "wait" would otherwise freeze that one arc's whole state machine for two orders
/// of magnitude longer than every neighboring backoff before it ever reconsiders — upstream's
/// own doc comment on [`HookingConfig::global_timeout`] already flags this whole ladder as
/// provisional tuning, not a protocol invariant. [`HookingConfig::restart_wait`] at this
/// daemon's actual (small) network sizes is exactly the cadence a "try again" decision should
/// run at, so this reuses that same figure.
#[must_use]
fn hooking_config() -> HookingConfig {
    let base = HookingConfig::default();
    let merge_reject_wait = base.restart_wait(1);
    HookingConfig {
        merge_reject_wait,
        ..base
    }
}

/// Spawns PeerServices, Coordinator, ANDNA, and Hooking for one identity, and registers every
/// `PeerService`. `tasks` reaps every spawned actor; each gets its own child of `cancel`.
#[allow(clippy::too_many_arguments)]
pub async fn spawn(
    topology: Topology,
    my_naddr: Naddr,
    hooking: HookingProvenance,
    qspn: QspnHandle,
    registry: Arc<LinkRegistry>,
    links: Arc<PeerLinks>,
    net: Arc<NetworkInfo>,
    tasks: &mut JoinSet<()>,
    cancel: &CancellationToken,
    // This node's RPC-identity signing key (`NtkdConfig::node_key_path`), if configured —
    // opts this generation's `ntk_peerservices::Handle` into signing its own outbound
    // origin-auth requests (`ntk_peerservices::Handle::with_signing_key`). `None` leaves
    // every outbound `PeerMessageForwarder::auth` unset, exactly today's wire shape.
    signing_key: Option<ed25519_dalek::SigningKey>,
    // `NtkdConfig::require_auth` — gates this generation's servant-side origin-auth
    // enforcement (`ntk_peerservices::Config::require_auth`).
    require_auth: bool,
    // A retiring generation's exported Coordinator state (`ntk_coordinator::Handle::hand_off`),
    // threaded into `ntk_coordinator::Manager::new`'s `handoff` parameter — the hand-off protocol
    // at `coord.vala:142-146`. `None` on a first boot, where there is nothing to inherit; `Some`
    // on a rehook, so per-level eldership and reservation state carries across the migration
    // instead of every level restarting from `GnodeMemory::fresh`.
    coordinator_handoff: Option<ntk_coordinator::HandOff>,
) -> Services {
    // A negotiated re-address (`HookingProvenance::Carried`) starts this generation's
    // participation knowledge empty rather than trivially-complete — `ntk_peerservices::Manager::
    // new`'s own doc calls this "a node still bootstrapping it from elsewhere" and explicitly
    // leaves the bootstrap sequencing to whichever crate wires Hooking, i.e. here (see the
    // live `ask_participant_maps` seed below).
    let joining = matches!(&hooking, HookingProvenance::Carried(_));
    let retrieved_below_level = if joining { 0 } else { topology.levels() };

    // -- PeerServices --
    let routing_env: Arc<dyn RoutingEnv> = Arc::new(RoutingEnvAdapter {
        qspn: qspn.clone(),
        registry: registry.clone(),
        links: links.clone(),
    });
    let (peers_manager, peers) = ntk_peerservices::Manager::new(
        topology.clone(),
        my_naddr.clone(),
        routing_env.clone(),
        peers_config(require_auth),
        retrieved_below_level,
    );
    let peers = match signing_key {
        Some(key) => peers.with_signing_key(key),
        None => peers,
    };
    tasks.spawn(peers_manager.run(cancel.child_token()));

    // Constructed here (needs only `peers` + config, `ntk_coordinator::CoordinatorClient::new`'s
    // own signature) rather than after `Manager::new` below, so `HookingCoordinatorClient`
    // adapters that need it can be built before `Manager::new` runs.
    let coordinator_client =
        ntk_coordinator::CoordinatorClient::new(peers.clone(), coordinator_config());

    // -- Coordinator (needs a not-yet-existing HookingHandle; see PropagationHandlerAdapter) --
    let coordinator_map = Arc::new(CoordinatorMapAdapter {
        qspn: qspn.clone(),
        net: net.clone(),
    });
    let coordinator_stub_factory = Arc::new(CoordinatorStubFactoryAdapter {
        links: links.clone(),
    });
    let (hooking_tx, hooking_rx) = tokio::sync::watch::channel(match &hooking {
        HookingProvenance::Carried(existing) => Some(existing.clone()),
        HookingProvenance::Fresh(_) => None,
    });
    let propagation_handler = Arc::new(PropagationHandlerAdapter {
        hooking: hooking_rx,
    });
    // `EnterHandlersAdapter` needs its own node's `CoordinatorService` for `EnterArbiter`'s
    // replicated-election record (`EnterArbiter::decide`'s own doc: read/write the *local*
    // record directly, no DHT round trip) — but `CoordinatorService` is constructed *after*
    // `Manager::new` below, which `EnterHandlersAdapter` is itself wired into as a constructor
    // argument. Same resolution as `PropagationHandlerAdapter::hooking`'s own cycle: a `watch`
    // channel, filled in once the real value exists.
    let (coordinator_service_tx, coordinator_service_rx) =
        tokio::sync::watch::channel(None::<Arc<ntk_coordinator::CoordinatorService>>);
    let enter_arbiter = Arc::new(EnterArbiter::new());
    let enter_adapter = Arc::new(EnterHandlersAdapter {
        arbiter: enter_arbiter,
        qspn: qspn.clone(),
        net: net.clone(),
        coordinator_service: coordinator_service_rx,
    });
    let enter_handlers = ntk_coordinator::EnterHandlers {
        evaluate_enter: enter_adapter.clone(),
        begin_enter: enter_adapter.clone(),
        completed_enter: enter_adapter.clone(),
        abort_enter: enter_adapter,
    };
    let (coordinator_manager, coordinator) = ntk_coordinator::Manager::new(
        topology.clone(),
        coordinator_map,
        coordinator_stub_factory,
        propagation_handler,
        enter_handlers,
        coordinator_config(),
        coordinator_handoff,
    );
    tasks.spawn(coordinator_manager.run(cancel.child_token()));

    let coordinator_service = Arc::new(ntk_coordinator::CoordinatorService::new(
        coordinator.clone(),
        peers.clone(),
    ));
    let _ = coordinator_service_tx.send(Some(coordinator_service.clone()));
    peers.register(coordinator_service).await;

    // -- Hooking: a fresh actor for this identity's very first generation, or the same
    // identity's already-resolved handle carried over from a negotiated re-address (see
    // `HookingProvenance`'s doc) --
    let qspn_view = Arc::new(QspnViewAdapter::spawn(
        qspn.clone(),
        net.clone(),
        tasks,
        cancel.child_token(),
    ));
    let hooking = match hooking {
        HookingProvenance::Fresh(origin) => {
            let coordinator_client_adapter = Arc::new(CoordinatorClientAdapter::new(
                coordinator_client.clone(),
                coordinator.clone(),
                qspn.clone(),
                net.clone(),
                // Same TTL as this identity's own `n_nodes` cache (`coordinator_config`'s own
                // doc): `decide_merge` bounds an identical question, "is this size-based
                // judgment still fresh enough to trust", so a stale merge verdict cannot
                // outlive a stale `n_nodes` reading would have.
                coordinator_config().n_nodes_cache_ttl,
            ));
            let hooking_stub_factory = Arc::new(HookingStubFactoryAdapter {
                qspn: qspn.clone(),
                links: links.clone(),
                registry: registry.clone(),
            });
            let (h, hooking_join) = ntk_hooking::spawn(
                origin,
                qspn_view.clone() as Arc<dyn ntk_hooking::QspnView>,
                coordinator_client_adapter,
                hooking_stub_factory,
                hooking_config(),
                cancel.child_token(),
            );
            tasks.spawn(async move {
                let _ = hooking_join.await;
            });
            let _ = hooking_tx.send(Some(h.clone()));
            h
        }
        HookingProvenance::Carried(existing) => existing,
    };

    // -- ANDNA (its `AndnaSubstrate` is already implemented for `ntk_peerservices::Handle`) --
    let substrate = Arc::new(peers.clone()) as Arc<dyn ntk_andna::AndnaSubstrate>;
    let (andna_manager, andna) = ntk_andna::Manager::new(substrate, ntk_andna::Config::default());
    tasks.spawn(andna_manager.run(cancel.child_token()));
    andna.register_services().await;
    // Nothing else in `ntk-andna` ever calls `purge_expired` on its own (that method's own
    // doc) — without this, expired hostnames/reservations pile up forever behind
    // `Config::max_hosted_records`/`max_counter_registrants`. A plain interval loop over the
    // actor's own `mpsc` channel, not an outbound RPC, so it carries none of this daemon's
    // "never await an outbound call inline" constraint.
    tasks.spawn(ntk_andna::run_expiry_reclaimer(
        andna.clone(),
        cancel.child_token(),
    ));

    // -- Bootstrap this generation's participation knowledge from whichever neighbors are
    // already known: `Handle::register` (above) already reactively floods *my own* freshly
    // registered services, so the only remaining gap for a joining generation is *learning*
    // about participants that were already in the network before I joined
    // (`ntk_peerservices::Manager::new`'s own "bootstrap sequencing" scope note). A live RPC
    // round trip never runs inline in the steady-state loop (this daemon's own concurrency
    // rule) — it is spawned as its own task instead.
    if joining {
        let peers_for_seed = peers.clone();
        tasks.spawn(async move {
            for neighbor in routing_env.neighbors() {
                match neighbor.ask_participant_maps().await {
                    Ok(incoming) => {
                        peers_for_seed.apply_participant_set(incoming).await;
                    }
                    Err(err) => tracing::debug!(
                        %err,
                        "peerservices: ask_participant_maps failed while bootstrapping a joined generation's participation knowledge"
                    ),
                }
            }
        });
    }

    Services {
        peers,
        coordinator,
        coordinator_client,
        andna,
        hooking,
        qspn_view,
    }
}
