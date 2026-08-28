//! The single-owner actor holding all mutable PeerServices state
//! (`research/notes/06-rust-stack.md` §Concurrency): the registered services, the participation
//! maps, and the routing layer's in-flight `waiting_answer_map`
//! (`research/impl/vala/peerservices/message_routing.vala:78-105,119`). Every other module in
//! this crate reaches this state only through [`Handle`] — never directly.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ntk_common::{HCoord, Naddr, Topology};
use ntk_proto::v1::TypedValue;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::participation::{self, ParticipantMap, ParticipantSet};
use crate::service::{ExecError, PeerService, Refusal, ServiceId};
use crate::stub::RoutingEnv;
use crate::tuple::TupleGNode;

/// An asynchronous outcome of a `contact_peer` attempt, delivered to the waiting caller as
/// upstream delivers it over `WaitingAnswer.ch`
/// (`research/impl/vala/peerservices/message_routing.vala:78-105,975-1183`).
#[derive(Clone, Debug)]
pub(crate) enum RouteEvent {
    /// The search has progressed to a new (deeper) minimum target — `set_next_destination`.
    NextDestination(TupleGNode),
    /// Routing failed at this g-node; exclude it and retry — `set_failure`.
    Failure(TupleGNode),
    /// This g-node is known not to participate; exclude it and retry — `set_non_participant`.
    NonParticipant(TupleGNode),
    /// The servant is missing participation maps it needs to answer authoritatively —
    /// `set_missing_optional_maps`.
    MissingOptionalMaps,
    /// A servant has been found and is fetching the request — `get_request`'s caller.
    RespondantNode(crate::tuple::TupleNode),
    /// The servant answered — `set_response`.
    Response(TypedValue),
    /// The servant refused — `set_refuse_message`.
    Refuse(Refusal),
    /// The servant asks for a full restart — `set_redo_from_start`.
    RedoFromStart,
}

struct WaitingAnswer {
    min_target: TupleGNode,
    respondant_node: Option<crate::tuple::TupleNode>,
    /// The request this search is for, served back via `get_request`. `None` for a probe with
    /// no real request (`check_non_participation`, `message_routing.vala:574-661`).
    request: Option<TypedValue>,
    tx: mpsc::UnboundedSender<RouteEvent>,
}

/// True if `respondant` names the same node as `wa.respondant_node` — the mismatch guard every
/// `set_*` handler applies before accepting a call
/// (`research/impl/vala/peerservices/message_routing.vala:1008-1021` and its repeats).
fn respondant_matches(wa: &WaitingAnswer, respondant: &crate::tuple::TupleNode) -> bool {
    wa.respondant_node
        .as_ref()
        .is_some_and(|r| r.positions() == respondant.positions())
}

/// True if `tuple` is not shallower than the currently recorded `min_target` — the monotonicity
/// guard `set_next_destination`/`set_failure`/`set_non_participant` each apply
/// (`message_routing.vala:1106-1117,1131-1142,1156-1167`). `strictly` selects
/// `set_next_destination`'s stricter "`>=` old_k is rejected" rule vs the other two's "`>` old_k
/// is rejected" rule.
fn is_deeper_or_equal(wa: &WaitingAnswer, tuple: &TupleGNode, strictly: bool) -> bool {
    if wa.min_target.top() != tuple.top() {
        return false;
    }
    let old_k = wa.min_target.level();
    let new_k = tuple.level();
    if strictly {
        new_k < old_k
    } else {
        new_k <= old_k
    }
}

/// Commands the [`Manager`] actor processes. Every read of mutable PeerServices state — the
/// service registry, participation maps, or in-flight routing state — goes through one of
/// these; nothing outside [`actor`](self) ever locks or shares that state directly.
enum Cmd {
    Register {
        service: Arc<dyn PeerService>,
    },
    LookupService {
        p_id: ServiceId,
        reply: oneshot::Sender<Option<Arc<dyn PeerService>>>,
    },
    IsServiceOptional {
        p_id: ServiceId,
        reply: oneshot::Sender<bool>,
    },
    NonParticipantGnodes {
        p_id: ServiceId,
        target_levels: usize,
        reply: oneshot::Sender<Vec<HCoord>>,
    },
    GnodeParticipates {
        p_id: ServiceId,
        level: usize,
        reply: oneshot::Sender<bool>,
    },
    NextMsgId {
        reply: oneshot::Sender<i32>,
    },
    RegisterWaiting {
        msg_id: i32,
        min_target: TupleGNode,
        request: Option<TypedValue>,
        reply: oneshot::Sender<mpsc::UnboundedReceiver<RouteEvent>>,
    },
    UnregisterWaiting {
        msg_id: i32,
    },
    GetRequest {
        msg_id: i32,
        respondant: crate::tuple::TupleNode,
        reply: oneshot::Sender<Result<TypedValue, GetRequestOutcome>>,
    },
    SetResponse {
        msg_id: i32,
        response: TypedValue,
        respondant: crate::tuple::TupleNode,
    },
    SetRefuseMessage {
        msg_id: i32,
        refusal: Refusal,
        respondant: crate::tuple::TupleNode,
    },
    SetRedoFromStart {
        msg_id: i32,
        respondant: crate::tuple::TupleNode,
    },
    SetNextDestination {
        msg_id: i32,
        tuple: TupleGNode,
    },
    SetFailure {
        msg_id: i32,
        tuple: TupleGNode,
    },
    SetNonParticipant {
        msg_id: i32,
        tuple: TupleGNode,
    },
    SetMissingOptionalMaps {
        msg_id: i32,
    },
    /// Applies a flooded `set_participant` fact if it is new, returning the fact to re-flood
    /// (`MapHandler.set_participant`, `research/impl/vala/peerservices/map_handler.vala:383-418`).
    ApplyParticipant {
        p_id: ServiceId,
        at: HCoord,
        reply: oneshot::Sender<Option<HCoord>>,
    },
    ExpireRecentlyPublished {
        at: HCoord,
    },
    /// The locally-registered optional services this node currently participates in
    /// (`State.my_services`) — read by the periodic participation re-announce
    /// (`crate::gossip::reannounce_participation`).
    MyOptionalServices {
        reply: oneshot::Sender<Vec<ServiceId>>,
    },
    AskParticipantMaps {
        reply: oneshot::Sender<ParticipantSet>,
    },
    /// Applies a neighbor's `give_participant_maps` snapshot if it is fresher, returning the
    /// re-shaped snapshot to forward onward (`MapHandler.give_participant_maps`/
    /// `copy_and_forward`, `map_handler.vala:238-301`).
    ApplyParticipantSet {
        incoming: ParticipantSet,
        reply: oneshot::Sender<Option<ParticipantSet>>,
    },
    /// Servant-side origin-auth replay check (`crate::actor::Handle::verify_origin`) — the only
    /// mutable state origin-auth needs, so this is the one round trip verification costs beyond
    /// the signature check itself.
    ObserveOriginSequence {
        signer: ed25519_dalek::VerifyingKey,
        sequence: u64,
        reply: oneshot::Sender<Result<(), ntk_proto::auth::AuthError>>,
    },
}

/// Outcome of a `get_request` lookup, mirroring the two upstream domain errors
/// (`message_routing.vala:975-997`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GetRequestOutcome {
    UnknownMessage,
    InvalidRequest,
}

/// A read-only snapshot of participation knowledge, published on every change
/// (`tokio::sync::watch`, per this crate's actor-model constraints).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Every level below this one is known accurately (`PeersManager.maps_retrieved_below_level`,
    /// `research/impl/vala/peerservices/peers.vala:149-153`).
    pub retrieved_below_level: usize,
    /// Known participants per service.
    pub participants: BTreeMap<ServiceId, ParticipantMap>,
}

/// A participation-change notification, published on a [`tokio::sync::broadcast`] stream in
/// place of upstream's GObject signals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A g-node became known to participate in a service (`add_participant`, `peers.vala:325-332`).
    ParticipantAdded { p_id: ServiceId, at: HCoord },
    /// A g-node was confirmed not to participate in a service
    /// (`MessageRouting.gnode_is_not_participating`, `peers.vala:125,263-266`).
    ParticipantRemoved { p_id: ServiceId, at: HCoord },
}

struct State {
    topology: Topology,
    my_pos: Naddr,
    config: Config,
    services: BTreeMap<ServiceId, Arc<dyn PeerService>>,
    my_services: BTreeSet<ServiceId>,
    participant_set: BTreeMap<ServiceId, ParticipantMap>,
    /// Services for which [`Config::max_participants_per_service`] has already engaged and
    /// logged its one-time `warn` (`State::add_participant`'s own doc).
    capacity_warned: BTreeSet<ServiceId>,
    retrieved_below_level: usize,
    recent_published: BTreeSet<HCoord>,
    waiting: BTreeMap<i32, WaitingAnswer>,
    next_msg_id: i32,
    snapshot_tx: watch::Sender<Snapshot>,
    events_tx: broadcast::Sender<Event>,
    /// Servant-side origin-auth replay guard (`Cmd::ObserveOriginSequence`), keyed by the
    /// signer key `ntk_proto::auth::verify` returns — bounded (`SequenceGuard::new`'s own doc)
    /// since `signer` is peer-supplied.
    origin_replay: ntk_proto::auth::SequenceGuard,
}

impl State {
    fn publish_snapshot(&self) {
        self.snapshot_tx.send_replace(Snapshot {
            retrieved_below_level: self.retrieved_below_level,
            participants: self.participant_set.clone(),
        });
    }

    /// `get_non_participant_gnodes`, `research/impl/vala/peerservices/peers.vala:446-472`.
    ///
    /// **Deviation, deliberate**: the literal upstream algorithm never special-cases "myself" —
    /// `get_all_gnodes_up_to_lvl` always includes my own coordinate as a scan candidate, and
    /// `participant_maps` never records it either (`add_participant`'s own "ignore myself" guard,
    /// `peers.vala:325-332`, since gossip only ever teaches me about *other* g-nodes) — so a
    /// literal port would always list myself as a non-participant of any optional service,
    /// including one I have registered locally, contradicting `my_gnode_participates`'s own
    /// `services.has_key(p_id)` self-check three lines below in the same file
    /// (`peers.vala:432-434`). This method applies that same self-check here too, so a node
    /// that registers a service can actually be routed to as its own servant.
    fn non_participant_gnodes(&self, p_id: ServiceId, target_levels: usize) -> Vec<HCoord> {
        let optional = self.services.get(&p_id).is_none_or(|s| s.is_optional());
        if !optional {
            return Vec::new();
        }
        let map = self.participant_set.get(&p_id);
        let registered_locally = self.services.contains_key(&p_id);
        let myself = HCoord::new(0, self.my_pos.positions()[0]);
        all_gnodes_up_to_lvl(&self.topology, self.my_pos.positions(), target_levels)
            .into_iter()
            .filter(|lp| map.is_none_or(|m| !m.contains(*lp)))
            .filter(|&lp| !(registered_locally && lp == myself))
            .collect()
    }

    /// `my_gnode_participates`, `peers.vala:430-444`.
    fn gnode_participates(&self, p_id: ServiceId, level: usize) -> bool {
        if self.services.contains_key(&p_id) {
            return true;
        }
        self.participant_set
            .get(&p_id)
            .is_some_and(|m| m.participants().any(|g| g.level < level))
    }

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Register { service } => {
                let p_id = service.service_id();
                let optional = service.is_optional();
                self.services.insert(p_id, service);
                if optional {
                    self.my_services.insert(p_id);
                }
            }
            Cmd::LookupService { p_id, reply } => {
                let _ = reply.send(self.services.get(&p_id).cloned());
            }
            Cmd::IsServiceOptional { p_id, reply } => {
                let _ = reply.send(self.services.get(&p_id).is_none_or(|s| s.is_optional()));
            }
            Cmd::NonParticipantGnodes {
                p_id,
                target_levels,
                reply,
            } => {
                let _ = reply.send(self.non_participant_gnodes(p_id, target_levels));
            }
            Cmd::GnodeParticipates { p_id, level, reply } => {
                let _ = reply.send(self.gnode_participates(p_id, level));
            }
            Cmd::NextMsgId { reply } => {
                let id = self.next_msg_id;
                self.next_msg_id = self.next_msg_id.wrapping_add(1);
                let _ = reply.send(id);
            }
            Cmd::RegisterWaiting {
                msg_id,
                min_target,
                request,
                reply,
            } => {
                let (tx, rx) = mpsc::unbounded_channel();
                self.waiting.insert(
                    msg_id,
                    WaitingAnswer {
                        min_target,
                        respondant_node: None,
                        request,
                        tx,
                    },
                );
                let _ = reply.send(rx);
            }
            Cmd::UnregisterWaiting { msg_id } => {
                self.waiting.remove(&msg_id);
            }
            Cmd::GetRequest {
                msg_id,
                respondant,
                reply,
            } => {
                let outcome = (|| {
                    let wa = self
                        .waiting
                        .get_mut(&msg_id)
                        .ok_or(GetRequestOutcome::UnknownMessage)?;
                    if wa.min_target.top() != respondant.top() {
                        return Err(GetRequestOutcome::InvalidRequest);
                    }
                    wa.respondant_node = Some(respondant.clone());
                    let _ = wa.tx.send(RouteEvent::RespondantNode(respondant));
                    wa.request.clone().ok_or(GetRequestOutcome::UnknownMessage)
                })();
                let _ = reply.send(outcome);
            }
            Cmd::SetResponse {
                msg_id,
                response,
                respondant,
            } => {
                if let Some(wa) = self.waiting.get(&msg_id)
                    && respondant_matches(wa, &respondant)
                {
                    let _ = wa.tx.send(RouteEvent::Response(response));
                }
            }
            Cmd::SetRefuseMessage {
                msg_id,
                refusal,
                respondant,
            } => {
                if let Some(wa) = self.waiting.get(&msg_id)
                    && respondant_matches(wa, &respondant)
                {
                    let _ = wa.tx.send(RouteEvent::Refuse(refusal));
                }
            }
            Cmd::SetRedoFromStart { msg_id, respondant } => {
                if let Some(wa) = self.waiting.get(&msg_id)
                    && respondant_matches(wa, &respondant)
                {
                    let _ = wa.tx.send(RouteEvent::RedoFromStart);
                }
            }
            Cmd::SetNextDestination { msg_id, tuple } => {
                if let Some(wa) = self.waiting.get_mut(&msg_id)
                    && is_deeper_or_equal(wa, &tuple, true)
                {
                    wa.min_target = tuple.clone();
                    let _ = wa.tx.send(RouteEvent::NextDestination(tuple));
                }
            }
            Cmd::SetFailure { msg_id, tuple } => {
                if let Some(wa) = self.waiting.get(&msg_id)
                    && is_deeper_or_equal(wa, &tuple, false)
                {
                    let _ = wa.tx.send(RouteEvent::Failure(tuple));
                }
            }
            Cmd::SetNonParticipant { msg_id, tuple } => {
                if let Some(wa) = self.waiting.get(&msg_id)
                    && is_deeper_or_equal(wa, &tuple, false)
                {
                    let _ = wa.tx.send(RouteEvent::NonParticipant(tuple));
                }
            }
            Cmd::SetMissingOptionalMaps { msg_id } => {
                if let Some(wa) = self.waiting.get(&msg_id) {
                    let _ = wa.tx.send(RouteEvent::MissingOptionalMaps);
                }
            }
            Cmd::ApplyParticipant { p_id, at, reply } => {
                if self.recent_published.contains(&at) {
                    let _ = reply.send(None);
                    return;
                }
                self.recent_published.insert(at);
                self.add_participant(p_id, at);
                let _ = reply.send(Some(at));
            }
            Cmd::ExpireRecentlyPublished { at } => {
                self.recent_published.remove(&at);
            }
            Cmd::MyOptionalServices { reply } => {
                let _ = reply.send(self.my_services.iter().copied().collect());
            }
            Cmd::AskParticipantMaps { reply } => {
                let _ = reply.send(self.produce_maps_copy());
            }
            Cmd::ApplyParticipantSet { incoming, reply } => {
                if !participation::is_fresher(self.retrieved_below_level, &incoming) {
                    let _ = reply.send(None);
                    return;
                }
                let folded = participation::fold_to_my_granularity(
                    self.my_pos.positions(),
                    self.topology.levels(),
                    incoming,
                );
                for (&p_id, map) in &folded.participant_set {
                    for hc in map.participants() {
                        if hc.level >= self.retrieved_below_level
                            && hc.level < folded.retrieved_below_level
                        {
                            self.add_participant(p_id, hc);
                        }
                    }
                }
                self.retrieved_below_level = folded.retrieved_below_level;
                self.publish_snapshot();
                let forward = participation::produce_below_level(
                    &self.participant_set,
                    self.my_pos.positions(),
                    self.retrieved_below_level,
                );
                let _ = reply.send(Some(forward));
            }
            Cmd::ObserveOriginSequence {
                signer,
                sequence,
                reply,
            } => {
                let _ = reply.send(self.origin_replay.observe(signer, sequence));
            }
        }
    }

    /// `add_participant`, `research/impl/vala/peerservices/peers.vala:325-332`.
    ///
    /// **Deviation, deliberate**: caps the map at [`Config::max_participants_per_service`],
    /// refusing only a brand-new fact once full rather than evicting an existing one — see that
    /// field's own doc for why an evict-existing policy would corrupt routing. Logs a `warn`
    /// the first time this engages for `p_id`.
    fn add_participant(&mut self, p_id: ServiceId, h: HCoord) {
        if self.my_pos.pos(h.level) == Some(h.pos) {
            return; // ignore myself
        }
        let map = self.participant_set.entry(p_id).or_default();
        if !map.contains(h) && map.len() >= self.config.max_participants_per_service {
            if self.capacity_warned.insert(p_id) {
                tracing::warn!(
                    ?p_id,
                    cap = self.config.max_participants_per_service,
                    "participant map at capacity: refusing new participant facts for this \
                     service until restart; routing view of unseen participants is now \
                     incomplete"
                );
            }
            return;
        }
        if map.insert(h) {
            self.publish_snapshot();
            let _ = self.events_tx.send(Event::ParticipantAdded { p_id, at: h });
        }
    }

    /// `produce_maps_copy`, `research/impl/vala/peerservices/peers.vala:343-359`.
    fn produce_maps_copy(&self) -> ParticipantSet {
        let mut out = self.participant_set.clone();
        for &p_id in &self.my_services {
            out.entry(p_id)
                .or_default()
                .insert(HCoord::new(0, self.my_pos.positions()[0]));
        }
        participation::produce_below_level(
            &out,
            self.my_pos.positions(),
            self.retrieved_below_level,
        )
    }
}

/// `get_all_gnodes_up_to_lvl`, `research/impl/vala/peerservices/peers.vala:474-489`: every
/// g-node visible in my topology inside my g-node at `lvl`, including myself.
pub(crate) fn all_gnodes_up_to_lvl(topology: &Topology, my_pos: &[u32], lvl: usize) -> Vec<HCoord> {
    let mut ret = Vec::new();
    for (level, &my_p) in my_pos.iter().enumerate().take(lvl) {
        let gsize = topology
            .gsize(level)
            .expect("level < lvl <= topology.levels()");
        for p in 0..gsize {
            if my_p != p {
                ret.push(HCoord::new(level, p));
            }
        }
    }
    ret.push(HCoord::new(0, my_pos[0]));
    ret
}

/// The single-owner actor. Spawn with [`Manager::run`]; interact only through the [`Handle`] it
/// returns.
pub struct Manager {
    state: State,
    cmd_rx: mpsc::Receiver<Cmd>,
    handle: Handle,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager").finish_non_exhaustive()
    }
}

impl Manager {
    /// Builds a `Manager` and its [`Handle`]. `retrieved_below_level` seeds the participation
    /// gossip freshness marker (`PeersManager.maps_retrieved_below_level`,
    /// `research/impl/vala/peerservices/peers.vala:149-153`); pass `topology.levels()` for a
    /// node that already has (or trivially has, on a brand-new network) complete participation
    /// knowledge — upstream's `create_net` case
    /// (`research/impl/vala/peerservices/map_handler.vala:130-134`) — or a lower value for a
    /// node still bootstrapping it from elsewhere.
    ///
    /// **Scope note**: upstream's constructor also supports `enter_net` — joining an existing
    /// network as a guest at a `guest_gnode_level` below `host_gnode_level`, bootstrapping
    /// participation knowledge from an old identity plus a live `ask_participant_maps` fetch
    /// (`map_handler.vala:136-236`). That flow depends on Hooking's migration machinery, which
    /// this crate does not have as a dependency; this crate models only the mechanics
    /// (a seedable freshness marker, plus [`Handle::apply_participant_set`]'s merge), not the
    /// bootstrap sequencing itself — that is for whichever future crate wires Hooking.
    #[must_use]
    pub fn new(
        topology: Topology,
        my_pos: Naddr,
        env: Arc<dyn RoutingEnv>,
        config: Config,
        retrieved_below_level: usize,
    ) -> (Self, Handle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (snapshot_tx, snapshot_rx) = watch::channel(Snapshot::default());
        let (events_tx, _events_rx) = broadcast::channel(256);
        let state = State {
            topology: topology.clone(),
            my_pos: my_pos.clone(),
            config,
            services: BTreeMap::new(),
            my_services: BTreeSet::new(),
            participant_set: BTreeMap::new(),
            capacity_warned: BTreeSet::new(),
            retrieved_below_level,
            recent_published: BTreeSet::new(),
            waiting: BTreeMap::new(),
            next_msg_id: 0,
            snapshot_tx,
            events_tx: events_tx.clone(),
            origin_replay: ntk_proto::auth::SequenceGuard::new(),
        };
        let handle = Handle {
            topology,
            my_pos,
            env,
            config,
            cmd_tx,
            snapshot_rx,
            events_tx,
            signing_key: None,
            next_sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        let manager = Self {
            state,
            cmd_rx,
            handle: handle.clone(),
        };
        (manager, handle)
    }

    /// Runs the actor loop until `cancel` fires or every [`Handle`] is dropped. When
    /// [`Config::participation_reannounce_interval`] is set, also drives the periodic
    /// participation re-announce (`crate::gossip::reannounce_participation`) in its own task —
    /// never awaiting that outbound gossip inline in this command loop — spawned into a private
    /// [`JoinSet`] and stopped via a child of `cancel` so it is guaranteed to have stopped,
    /// panic surfaced rather than swallowed, before this method returns on either shutdown path
    /// (explicit `cancel`, or every [`Handle`] dropped).
    pub async fn run(mut self, cancel: CancellationToken) {
        let mut background = JoinSet::new();
        let periodic_cancel = cancel.child_token();
        if let Some(interval) = self.handle.config.participation_reannounce_interval {
            let handle = self.handle.clone();
            let stop = periodic_cancel.clone();
            background.spawn(async move {
                loop {
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(interval) => {
                            crate::gossip::reannounce_participation(&handle).await;
                        }
                    }
                }
            });
        }
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.state.handle(cmd),
                        None => break,
                    }
                }
            }
        }
        periodic_cancel.cancel();
        while let Some(res) = background.join_next().await {
            if let Err(err) = res
                && err.is_panic()
            {
                std::panic::resume_unwind(err.into_panic());
            }
        }
    }
}

/// Cheap-clone handle to a running [`Manager`]. The only way to interact with it.
#[derive(Clone)]
pub struct Handle {
    pub(crate) topology: Topology,
    pub(crate) my_pos: Naddr,
    pub(crate) env: Arc<dyn RoutingEnv>,
    pub(crate) config: Config,
    cmd_tx: mpsc::Sender<Cmd>,
    snapshot_rx: watch::Receiver<Snapshot>,
    events_tx: broadcast::Sender<Event>,
    /// This identity's origin-auth signing key, if configured (`Handle::with_signing_key`).
    /// `None` (the default every existing caller/test still gets) leaves every outbound
    /// `contact_peer` request's `PeerMessageForwarder::auth` unset.
    signing_key: Option<Arc<ed25519_dalek::SigningKey>>,
    /// Strictly-increasing per-signer sequence counter for this identity's own outbound
    /// origin-auth signatures (`ntk_proto::auth::sign`'s `sequence`). Shared (not re-created)
    /// across every `Handle::clone()` so concurrent `contact_peer` callers never reuse a
    /// sequence — `ntk_proto::auth::SequenceGuard`'s replay check at the servant depends on it.
    next_sequence: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("topology", &self.topology)
            .field("my_pos", &self.my_pos)
            .finish_non_exhaustive()
    }
}

impl Handle {
    /// Sends `f`'s command and awaits its reply. Returns `None` once the actor has already shut
    /// down (cancelled): `Manager::run` returning drops `cmd_rx` and every in-flight
    /// `WaitingAnswer`/reply `oneshot::Sender` along with `State`, so a `Handle` call racing (or
    /// simply arriving after) that shutdown finds a closed channel — an ordinary, expected
    /// outcome of cancellation, not a bug, so callers get `None` to handle as "no answer is
    /// coming" rather than a panic.
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

    /// True once the actor's own `Manager::run` has returned (only ever from cancellation —
    /// `Manager::run`'s own doc), dropping its `cmd_rx` and closing this channel — a lock-free,
    /// side-effect-free check of the same closed-channel signal [`Handle::call`]/[`Handle::cast`]
    /// already treat as ordinary shutdown, used by [`crate::routing`]'s gateway-retry loop to
    /// stop retrying instead of working through its full bound against a `Manager` that is no
    /// longer running.
    pub(crate) fn is_shut_down(&self) -> bool {
        self.cmd_tx.is_closed()
    }

    /// The [`Topology`] this substrate is running on.
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// My own address in this topology.
    #[must_use]
    pub fn my_pos(&self) -> &Naddr {
        &self.my_pos
    }

    /// Opts this identity's outbound `contact_peer` requests into origin-auth signing —
    /// [`Config::require_auth`]'s companion on the originator side. Not a [`Manager::new`]
    /// constructor parameter: every existing caller/test that never calls this keeps producing
    /// byte-for-byte the pre-auth `PeerMessageForwarder` shape (`auth` unset).
    #[must_use]
    pub fn with_signing_key(mut self, signing_key: ed25519_dalek::SigningKey) -> Self {
        self.signing_key = Some(Arc::new(signing_key));
        self
    }

    /// Registers `service`. Auto-participation flooding for optional services lives in
    /// [`crate::gossip`] (`Handle::register`'s actual definition).
    pub(crate) async fn register_cmd(&self, service: Arc<dyn PeerService>) {
        self.cast(Cmd::Register { service }).await;
    }

    /// A read-only, always-current snapshot of participation knowledge.
    #[must_use]
    pub fn snapshot(&self) -> watch::Receiver<Snapshot> {
        self.snapshot_rx.clone()
    }

    /// The current `retrieved_below_level` without an actor round trip (backed by the same
    /// `watch` channel [`Handle::snapshot`] reads).
    pub(crate) fn snapshot_retrieved_below_level(&self) -> usize {
        self.snapshot_rx.borrow().retrieved_below_level
    }

    /// Subscribes to participation-change events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events_tx.subscribe()
    }

    /// `None` if the actor already shut down.
    pub(crate) async fn is_service_optional(&self, p_id: ServiceId) -> Option<bool> {
        self.call(|reply| Cmd::IsServiceOptional { p_id, reply })
            .await
    }

    /// `None` if the actor already shut down.
    pub(crate) async fn non_participant_gnodes(
        &self,
        p_id: ServiceId,
        target_levels: usize,
    ) -> Option<Vec<HCoord>> {
        self.call(|reply| Cmd::NonParticipantGnodes {
            p_id,
            target_levels,
            reply,
        })
        .await
    }

    /// `None` if the actor already shut down.
    pub(crate) async fn gnode_participates(&self, p_id: ServiceId, level: usize) -> Option<bool> {
        self.call(|reply| Cmd::GnodeParticipates { p_id, level, reply })
            .await
    }

    /// Actor shutdown and "not registered" both collapse to `None` — a caller either way just
    /// has no service to invoke.
    pub(crate) async fn lookup_service(&self, p_id: ServiceId) -> Option<Arc<dyn PeerService>> {
        self.call(|reply| Cmd::LookupService { p_id, reply })
            .await
            .flatten()
    }

    /// Executes `request` against a locally-registered service, mirroring `exec_service`
    /// (`research/impl/vala/peerservices/peers.vala:258-262`). Returns `None` if `p_id` is not
    /// registered locally — upstream `assert()`s this can't happen (`services.has_key(p_id)`);
    /// since reaching this point can be driven by network input (routing decided *some* node,
    /// possibly this one, is the best candidate for a service it never actually registered),
    /// this crate treats it as a possible, not fatal, outcome instead of panicking on untrusted
    /// input.
    pub(crate) async fn exec_local(
        &self,
        p_id: ServiceId,
        request: TypedValue,
        client_tuple: &[u32],
    ) -> Option<Result<TypedValue, ExecError>> {
        let service = self.lookup_service(p_id).await?;
        Some(service.exec(request, client_tuple).await)
    }

    /// Signs this attempt's origin assertion if [`Handle::with_signing_key`] configured a
    /// signing key — `None` (the default) leaves `PeerMessageForwarder::auth` unset, exactly
    /// today's unauthenticated wire shape. A fresh signature (and sequence) every call, never
    /// cached across `contact_peer` retries: each retry can reach a different candidate/relay
    /// path, so each gets its own proof.
    pub(crate) fn sign_origin(
        &self,
        client_tuple: &[u32],
        p_id: ServiceId,
        request: &TypedValue,
    ) -> Option<ntk_proto::v1::Auth> {
        let key = self.signing_key.as_deref()?;
        let sequence = self
            .next_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let payload = crate::origin_auth::origin_signing_payload(client_tuple, p_id, request);
        Some(ntk_proto::auth::sign(
            key,
            sequence,
            crate::origin_auth::ORIGIN_AUTH_METHOD,
            &payload,
        ))
    }

    /// Verifies `auth` as the true originator's signature over `(client_tuple, p_id, request)`
    /// — the servant-side half of origin-auth (`crate::routing::Handle::forward_msg`'s
    /// self-loop, after fetching `request`, never at a relay hop). A no-op success when
    /// [`Config::require_auth`] is `false`: enforcement is entirely opt-in.
    ///
    /// # Errors
    /// [`crate::origin_auth::OriginAuthError::Missing`] if `require_auth` is set but `auth` is
    /// `None`; [`crate::origin_auth::OriginAuthError::Auth`] if the signature or its replay
    /// sequence doesn't check out; [`crate::origin_auth::OriginAuthError::ActorShutDown`] if the
    /// actor already shut down mid-check.
    pub(crate) async fn verify_origin(
        &self,
        auth: Option<&ntk_proto::v1::Auth>,
        client_tuple: &[u32],
        p_id: ServiceId,
        request: &TypedValue,
    ) -> Result<(), crate::origin_auth::OriginAuthError> {
        // `require_auth` is the global, interop-driven default. A service may still demand a
        // verified origin for a specific request — see `PeerService::requires_origin_auth` for
        // why that is per-request and why the global default cannot simply be flipped.
        if !self.config.require_auth {
            let demanded = match self.lookup_service(p_id).await {
                Some(service) => service.requires_origin_auth(request),
                // No such service registered here: nothing to enforce on behalf of. The request
                // fails later on its own merits rather than as an auth error.
                None => false,
            };
            if !demanded {
                return Ok(());
            }
        }
        let auth = auth.ok_or(crate::origin_auth::OriginAuthError::Missing)?;
        let payload = crate::origin_auth::origin_signing_payload(client_tuple, p_id, request);
        let signer =
            ntk_proto::auth::verify(auth, crate::origin_auth::ORIGIN_AUTH_METHOD, &payload)?;
        self.call(|reply| Cmd::ObserveOriginSequence {
            signer,
            sequence: auth.sequence,
            reply,
        })
        .await
        .ok_or(crate::origin_auth::OriginAuthError::ActorShutDown)??;
        Ok(())
    }

    /// `None` if the actor already shut down.
    pub(crate) async fn next_msg_id(&self) -> Option<i32> {
        self.call(|reply| Cmd::NextMsgId { reply }).await
    }

    /// `None` if the actor already shut down.
    pub(crate) async fn register_waiting(
        &self,
        msg_id: i32,
        min_target: TupleGNode,
        request: Option<TypedValue>,
    ) -> Option<mpsc::UnboundedReceiver<RouteEvent>> {
        self.call(|reply| Cmd::RegisterWaiting {
            msg_id,
            min_target,
            request,
            reply,
        })
        .await
    }

    pub(crate) async fn unregister_waiting(&self, msg_id: i32) {
        self.cast(Cmd::UnregisterWaiting { msg_id }).await;
    }

    /// An actor already shut down can't know about any `msg_id`, so that case folds into the
    /// same [`GetRequestOutcome::UnknownMessage`] a live actor would answer with.
    pub(crate) async fn get_request(
        &self,
        msg_id: i32,
        respondant: crate::tuple::TupleNode,
    ) -> Result<TypedValue, GetRequestOutcome> {
        self.call(|reply| Cmd::GetRequest {
            msg_id,
            respondant,
            reply,
        })
        .await
        .unwrap_or(Err(GetRequestOutcome::UnknownMessage))
    }

    pub(crate) async fn set_response(
        &self,
        msg_id: i32,
        response: TypedValue,
        respondant: crate::tuple::TupleNode,
    ) {
        self.cast(Cmd::SetResponse {
            msg_id,
            response,
            respondant,
        })
        .await;
    }

    pub(crate) async fn set_refuse_message(
        &self,
        msg_id: i32,
        refusal: Refusal,
        respondant: crate::tuple::TupleNode,
    ) {
        self.cast(Cmd::SetRefuseMessage {
            msg_id,
            refusal,
            respondant,
        })
        .await;
    }

    pub(crate) async fn set_redo_from_start(
        &self,
        msg_id: i32,
        respondant: crate::tuple::TupleNode,
    ) {
        self.cast(Cmd::SetRedoFromStart { msg_id, respondant })
            .await;
    }

    pub(crate) async fn set_next_destination(&self, msg_id: i32, tuple: TupleGNode) {
        self.cast(Cmd::SetNextDestination { msg_id, tuple }).await;
    }

    pub(crate) async fn set_failure(&self, msg_id: i32, tuple: TupleGNode) {
        self.cast(Cmd::SetFailure { msg_id, tuple }).await;
    }

    pub(crate) async fn set_non_participant(&self, msg_id: i32, tuple: TupleGNode) {
        self.cast(Cmd::SetNonParticipant { msg_id, tuple }).await;
    }

    pub(crate) async fn set_missing_optional_maps(&self, msg_id: i32) {
        self.cast(Cmd::SetMissingOptionalMaps { msg_id }).await;
    }

    /// Applies a flooded `set_participant` fact, returning `Some(at)` if it was new and should
    /// be re-flooded to my own neighbors (`MapHandler.set_participant`,
    /// `research/impl/vala/peerservices/map_handler.vala:383-418`) — the caller is responsible
    /// for scheduling the matching 60-second `recent_published` expiry via
    /// [`Handle::expire_recently_published`].
    /// Actor shutdown and "nothing to re-flood" both collapse to `None`.
    pub(crate) async fn apply_participant(&self, p_id: ServiceId, at: HCoord) -> Option<HCoord> {
        self.call(|reply| Cmd::ApplyParticipant { p_id, at, reply })
            .await
            .flatten()
    }

    pub(crate) async fn expire_recently_published(&self, at: HCoord) {
        self.cast(Cmd::ExpireRecentlyPublished { at }).await;
    }

    /// Falls back to an empty (but valid) set if the actor already shut down — a caller reading
    /// this only ever forwards it onward, so "I know of no participants" is a harmless answer.
    pub(crate) async fn ask_participant_maps(&self) -> ParticipantSet {
        self.call(|reply| Cmd::AskParticipantMaps { reply })
            .await
            .unwrap_or_else(|| ParticipantSet {
                retrieved_below_level: 0,
                my_pos: self.my_pos.positions().to_vec(),
                participant_set: BTreeMap::new(),
            })
    }

    /// The locally-registered optional services this node currently participates in, read by
    /// [`crate::gossip::reannounce_participation`]. Empty if the actor already shut down.
    pub(crate) async fn my_optional_services(&self) -> Vec<ServiceId> {
        self.call(|reply| Cmd::MyOptionalServices { reply })
            .await
            .unwrap_or_default()
    }

    /// Applies a neighbor's (or a bootstrapping node's) participation snapshot if it is fresher,
    /// returning the re-shaped snapshot to forward onward to my own neighbors
    /// (`MapHandler.give_participant_maps`/`copy_and_forward`,
    /// `research/impl/vala/peerservices/map_handler.vala:238-301`). `pub` (not `pub(crate)`) so
    /// whichever crate wires Hooking's `enter_net` bootstrap fetch (see [`Manager::new`]'s scope
    /// note) can feed a fetched snapshot in directly.
    /// Actor shutdown and "not fresher, nothing to forward" both collapse to `None`.
    pub async fn apply_participant_set(&self, incoming: ParticipantSet) -> Option<ParticipantSet> {
        self.call(|reply| Cmd::ApplyParticipantSet { incoming, reply })
            .await
            .flatten()
    }
}

#[cfg(test)]
mod capacity_tests {
    use std::sync::Arc;

    use ntk_common::{HCoord, Naddr, Topology};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::config::Config;
    use crate::service::ServiceId;
    use crate::stub::{PeersStub, RoutingEnv};

    struct NoopEnv;

    impl RoutingEnv for NoopEnv {
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
        fn dial(&self, _n: &crate::tuple::TupleNode) -> Option<Arc<dyn PeersStub>> {
            None
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            1
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    /// Pins the fix: once a service's participant map is at
    /// [`Config::max_participants_per_service`], a brand-new fact is refused instead of evicting
    /// an existing one — the cap engages deterministically without dropping data routing needs.
    #[tokio::test]
    async fn participant_map_refuses_new_facts_at_capacity_but_never_evicts_existing_ones() {
        let topology = Topology::new([50]).unwrap();
        let my_pos = Naddr::new(topology.clone(), vec![0]).unwrap();
        let env: Arc<dyn RoutingEnv> = Arc::new(NoopEnv);
        let config = Config {
            max_participants_per_service: 5,
            ..Config::default()
        };
        let (manager, handle) =
            Manager::new(topology.clone(), my_pos, env, config, topology.levels());
        let cancel = CancellationToken::new();
        let manager_task = tokio::spawn(manager.run(cancel.child_token()));

        let p_id = ServiceId::new(1);
        let first_five: Vec<HCoord> = (1..=5).map(|p| HCoord::new(0, p)).collect();
        for &h in &first_five {
            handle.apply_participant(p_id, h).await;
        }
        let snapshot = handle.snapshot().borrow().clone();
        let map = snapshot
            .participants
            .get(&p_id)
            .expect("five facts were applied");
        assert_eq!(map.len(), 5, "the map must hold exactly the configured cap");
        for &h in &first_five {
            assert!(
                map.contains(h),
                "every fact applied under the cap must be recorded"
            );
        }

        // A brand-new fact beyond capacity is refused, not swapped in for an older one.
        let overflow = HCoord::new(0, 6);
        handle.apply_participant(p_id, overflow).await;
        let snapshot = handle.snapshot().borrow().clone();
        let map = snapshot.participants.get(&p_id).unwrap();
        assert_eq!(
            map.len(),
            5,
            "capacity must not grow past the configured limit"
        );
        assert!(
            !map.contains(overflow),
            "the fact that arrived once the map was full must be refused"
        );
        for &h in &first_five {
            assert!(
                map.contains(h),
                "no already-known participant may be evicted to make room for a new one"
            );
        }

        cancel.cancel();
        manager_task.await.unwrap();
    }
}
