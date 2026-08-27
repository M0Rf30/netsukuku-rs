//! The QSPN actor: single-owner protocol state fed by an `mpsc` command
//! queue with `oneshot` replies (`research/notes/06-rust-stack.md`
//! §Concurrency) — the Rust replacement for `QspnManager`'s public method
//! surface (`research/impl/vala/qspn/qspn.vala:66-2799`), including the
//! `enter_net` migration/bootstrap-phase machinery ([`spawn_entering`],
//! `qspn.vala:223-355,500-627`) and the connectivity-identity lifecycle
//! ([`QspnHandle::make_connectivity`]/[`QspnHandle::exit_network`]/
//! [`QspnHandle::check_connectivity`], `qspn.vala:2226-2448`).
//! `prepare_destroy`/`destroy`'s broadcast teardown (`qspn.vala:2450-2505`)
//! remains out of scope — see [`QspnHandle::check_connectivity`]'s docs.
//!
//! **Outbound network I/O never blocks the command loop.** Upstream gets
//! this for free from `pth-tasklet`'s cooperative scheduler: an
//! `ArcAddTasklet` blocked on `retrieve_full_etp` simply yields, and another
//! tasklet (e.g. servicing an inbound `get_full_etp` skeleton call from the
//! very peer being fetched from) runs in the meantime
//! (`research/notes/06-rust-stack.md` §Concurrency). A single serial `mpsc`
//! loop has no equivalent free lunch: if a command handler awaited a fetch
//! inline, two peers racing simultaneous `add_arc` calls would deadlock each
//! other (each blocks its own loop waiting on the other's reply, so neither
//! can service the other's inbound request). Every outbound call here is
//! therefore spawned onto [`Actor::timers`] as an independent task that
//! reports its result back as a new [`Command`]; the command loop itself
//! only ever does local state mutation and never awaits a peer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ntk_common::{Cost, Fingerprint, HCoord, Naddr};
use ntk_rpc::RpcError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::arc::{ArcId, ArcIdSource};
use crate::config::{QspnConfig, ThresholdCalculator};
use crate::error::QspnError;
use crate::events::QspnEvent;
use crate::flood;
use crate::path::{Destination, EtpMessage, EtpPath, NodePath, to_route_path};
use crate::revise::revise_etp;
use crate::snapshot::RouteSnapshot;
use crate::state::{InternalArc, QspnState, SplitSignal};
use crate::stub::{MissingArcHandler, QspnStubFactory};
use crate::validate::check_incoming_message;

/// Commands the actor processes serially off its `mpsc` queue. A mutating
/// command's `reply` fires as soon as the resulting *state* mutation is
/// complete; any outbound network call the command also triggers is spawned
/// separately (see module docs) and never gates the reply or the next
/// command.
enum Command {
    AddArc {
        cost: Cost,
        reply: oneshot::Sender<ArcId>,
    },
    ArcAddFetched {
        arc: ArcId,
        result: Result<EtpMessage, RpcError>,
    },
    ArcChanged {
        arc: ArcId,
        cost: Cost,
        reply: oneshot::Sender<Result<(), QspnError>>,
    },
    RemoveArc {
        arc: ArcId,
        reply: oneshot::Sender<Result<(), QspnError>>,
    },
    GatherComplete {
        a_changed: Option<ArcId>,
        extra_dead_paths: Vec<EtpPath>,
        results: Vec<(ArcId, Result<EtpMessage, RpcError>)>,
    },
    CurrentArcs {
        reply: oneshot::Sender<Vec<ArcId>>,
    },
    PeerNaddr {
        arc: ArcId,
        reply: oneshot::Sender<Option<Naddr>>,
    },
    MyEldership {
        level: usize,
        reply: oneshot::Sender<Option<Option<u32>>>,
    },
    Eldership {
        level: usize,
        pos: u32,
        reply: oneshot::Sender<Result<Option<Option<u32>>, QspnError>>,
    },
    /// The per-level g-node identity fingerprint's champion `id` after this
    /// node's own [`QspnState::update_clusters`] aggregation at `level` — the
    /// value upstream's production `CoordinatorManagerMap.get_fp_id` reads
    /// (`(Fingerprint) qspn_mgr.get_fingerprint(lvl); return fp.id;`,
    /// `research/impl/vala/ntkd/coordinator_helpers.vala:167-175`). See
    /// [`QspnHandle::fingerprint_id`] for the full semantics this stands in
    /// for on the wire.
    FingerprintId {
        level: usize,
        reply: oneshot::Sender<Option<Vec<u8>>>,
    },
    GetFullEtp {
        arc: ArcId,
        requesting_address: Naddr,
        reply: oneshot::Sender<Result<EtpMessage, QspnError>>,
    },
    SendEtp {
        arc: ArcId,
        etp: EtpMessage,
        is_full: bool,
        reply: oneshot::Sender<Result<(), QspnError>>,
    },
    GotPrepareDestroy {
        reply: oneshot::Sender<()>,
    },
    GotDestroy {
        arc: ArcId,
        reply: oneshot::Sender<()>,
    },
    ResendEtp {
        arc: ArcId,
        etp: EtpMessage,
        is_full: bool,
    },
    ResendFailed {
        arc: ArcId,
    },
    PeriodicFullEtp,
    FirstDetectionSplitFire(Vec<HCoord>),
    SplitTimerFire {
        destination: HCoord,
        fp_eldest: Fingerprint<Vec<u8>>,
        fp: Fingerprint<Vec<u8>>,
    },
    BootstrapEtpFetched {
        arc: ArcId,
        result: Result<EtpMessage, RpcError>,
    },
    BootstrapTimeout,
    ExitBootstrapGathered {
        results: Vec<(ArcId, Result<EtpMessage, RpcError>)>,
    },
    IsBootstrapComplete {
        reply: oneshot::Sender<bool>,
    },
    CurrentNaddr {
        reply: oneshot::Sender<Naddr>,
    },
    MakeConnectivity {
        from: usize,
        to: usize,
        update_naddr: Box<dyn Fn(&Naddr) -> Naddr + Send>,
        reply: oneshot::Sender<Result<(), QspnError>>,
    },
    PublishConnectivity {
        old_position: HCoord,
    },
    ExitNetwork {
        level: usize,
        reply: oneshot::Sender<Result<(), QspnError>>,
    },
    CheckConnectivity {
        reply: oneshot::Sender<bool>,
    },
    /// Fires once `config.arc_gather_debounce` has elapsed since the most
    /// recent arc-flap-triggered gather — see
    /// [`Actor::handle_arc_gather_window_elapsed`].
    ArcGatherWindowElapsed,
}

/// Cheap-clone handle to a running QSPN actor — the only way to interact
/// with it. Mirrors `QspnManager`'s public method surface
/// (`qspn.vala:686-2224`), including [`Self::make_connectivity`]/
/// [`Self::exit_network`]; `prepare_destroy`/`destroy`'s broadcast teardown
/// remains out of scope (`qspn.vala:2450-2505`, see [`Self::check_connectivity`]).
#[derive(Clone)]
pub struct QspnHandle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    snapshot_rx: watch::Receiver<Arc<RouteSnapshot>>,
    events_tx: broadcast::Sender<QspnEvent>,
    my_naddr: Naddr,
}

impl std::fmt::Debug for QspnHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QspnHandle")
            .field("my_naddr", &self.my_naddr)
            .finish_non_exhaustive()
    }
}

async fn call<T>(
    tx: &mpsc::UnboundedSender<Command>,
    build: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> Result<T, QspnError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(build(reply_tx)).map_err(|_| QspnError::ActorGone)?;
    reply_rx.await.map_err(|_| QspnError::ActorGone)
}

impl QspnHandle {
    /// This identity's own address — immutable for the life of the actor
    /// (`create_net` never changes `my_naddr`; only `make_connectivity`,
    /// out of scope, does).
    #[must_use]
    pub fn my_naddr(&self) -> &Naddr {
        &self.my_naddr
    }

    /// The current route-set snapshot (`get_known_destinations` +
    /// `get_paths_to` combined, `qspn.vala:2117-2180`).
    #[must_use]
    pub fn snapshot(&self) -> Arc<RouteSnapshot> {
        self.snapshot_rx.borrow().clone()
    }

    /// A live handle onto the snapshot channel, for a consumer that wants to
    /// `wait_for`/`changed()` on updates rather than polling
    /// [`Self::snapshot`].
    #[must_use]
    pub fn watch_snapshot(&self) -> watch::Receiver<Arc<RouteSnapshot>> {
        self.snapshot_rx.clone()
    }

    /// Subscribes to this identity's event stream (`QspnManager`'s GObject
    /// signals, `qspn.vala:122-147`, as a `broadcast` stream — see
    /// [`QspnEvent`]).
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<QspnEvent> {
        self.events_tx.subscribe()
    }

    /// `arc_add` (`qspn.vala:696-798`): registers a brand-new arc at `cost`
    /// and fetches its peer's full ETP in the background.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`] if the actor task has stopped.
    pub async fn add_arc(&self, cost: Cost) -> Result<ArcId, QspnError> {
        call(&self.cmd_tx, |reply| Command::AddArc { cost, reply }).await
    }

    /// `arc_is_changed` (`qspn.vala:800-911`): updates `arc`'s cost and
    /// re-gathers full ETPs from every arc, in the background, to
    /// re-evaluate paths.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`], or whatever the actor itself returns.
    pub async fn arc_changed(&self, arc: ArcId, cost: Cost) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::ArcChanged {
            arc,
            cost,
            reply,
        })
        .await?
    }

    /// `arc_remove` (`qspn.vala:913-1070`).
    ///
    /// # Errors
    /// [`QspnError::ActorGone`], or whatever the actor itself returns.
    pub async fn remove_arc(&self, arc: ArcId) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::RemoveArc { arc, reply }).await?
    }

    /// `current_arcs` (`qspn.vala:2211-2216`).
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn current_arcs(&self) -> Result<Vec<ArcId>, QspnError> {
        call(&self.cmd_tx, |reply| Command::CurrentArcs { reply }).await
    }

    /// `get_naddr_for_arc` (`qspn.vala:2220-2224`).
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn peer_naddr(&self, arc: ArcId) -> Result<Option<Naddr>, QspnError> {
        call(&self.cmd_tx, |reply| Command::PeerNaddr { arc, reply }).await
    }

    /// This node's own eldership claim at `level`
    /// (`ntk_hooking::view::QspnView::my_eldership`,
    /// `research/impl/vala/hooking/api.vala:38`), via
    /// [`QspnState::my_eldership`]. Answered entirely from local state — no
    /// outbound call.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn my_eldership(&self, level: usize) -> Result<Option<Option<u32>>, QspnError> {
        call(&self.cmd_tx, |reply| Command::MyEldership { level, reply }).await
    }

    /// The eldership of the g-node currently known at `(level, pos)`
    /// (`ntk_hooking::view::QspnView::eldership`,
    /// `research/impl/vala/hooking/api.vala:43`), via
    /// [`QspnState::eldership`]. Answered entirely from local state — no
    /// outbound call.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`], or whatever [`QspnState::eldership`] itself
    /// returns.
    pub async fn eldership(
        &self,
        level: usize,
        pos: u32,
    ) -> Result<Option<Option<u32>>, QspnError> {
        call(&self.cmd_tx, |reply| Command::Eldership {
            level,
            pos,
            reply,
        })
        .await?
    }

    /// This node's own per-level g-node identity fingerprint, projected down
    /// to the one field a `fp_id`-shaped consumer needs: the champion `id`
    /// [`QspnState::update_clusters`] settled on for `level` the last time it
    /// ran, via [`QspnState::fingerprint`]. Answered entirely from local
    /// state — no outbound call.
    ///
    /// This exists so a caller outside this crate (`ntk_coordinator`'s
    /// `CoordinatorMap::fp_id`, `research/impl/vala/coordinator/api.vala:29`)
    /// can read the *real* fingerprint this identity was constructed with
    /// (see [`spawn`]/[`spawn_entering`]'s `my_fingerprint` parameter)
    /// instead of maintaining a parallel notion of g-node identity beside it.
    /// `None` if `level` is out of range for this identity (mirrors
    /// [`Self::my_eldership`]'s domain check).
    ///
    /// # Why the champion `id`, not the whole fingerprint or its seed trail
    /// Upstream's own production wiring does exactly this projection:
    /// `CoordinatorManagerMap.get_fp_id` reads `((Fingerprint)
    /// qspn_mgr.get_fingerprint(lvl)).id` verbatim
    /// (`research/impl/vala/ntkd/coordinator_helpers.vala:167-175`) — there
    /// is no more faithful choice than porting that same projection. This
    /// crate's `Id = Vec<u8>` (a random value in this daemon, see
    /// `ntkd::node::lifecycle::random_fingerprint_id`) rather than upstream's
    /// `int64`, so a caller reducing this to a fixed-width `i64` should do so
    /// the same way that id was produced (e.g. `i64::from_be_bytes` on an
    /// 8-byte id) rather than reinterpreting arbitrary-length bytes.
    ///
    /// # Same-g-node agreement is NOT unconditional
    /// [`Fingerprint::construct`]'s champion race is order-dependent on a
    /// genuine eldership *tie* (its own docs, and the
    /// `virtual_eldership_wins_unconditionally_over_real_siblings` /
    /// `elder_seed_indistinguishable_when_seeds_fully_match` tests): each
    /// tied member's own fingerprint starts as its local "current" champion
    /// and only a *later-processed* sibling can depose it, so for exactly
    /// two really-tied members each one names the *other* as champion —
    /// different `id`s even though [`Fingerprint::elder_seed`] cannot order
    /// them ([`Fingerprint::same_branch`]). Two members of the very same
    /// stable g-node therefore agree on this value whenever their eldership
    /// claims are pairwise distinct (the ordinary case — claims are randomly
    /// assigned per node), but a coincidental tie can make this value
    /// disagree for that one aggregation, which only ever fails a
    /// `ntk_coordinator` `check_propagation` guard *closed* (the
    /// propagation is dropped as "not my g-node", never misapplied to the
    /// wrong one) — no worse than upstream's own untied assumption
    /// (`assert_not_reached()` on a tie, `serializables.vala:260`), and
    /// strictly safer.
    ///
    /// # Virtual positions never agree, by design
    /// While this node's own position at `level - 1` is virtual
    /// (`Naddr::is_virtual_at`, reserved but not yet placed),
    /// [`QspnState::update_clusters`] aggregates with `is_null_eldership =
    /// true`: [`Fingerprint::construct`]'s champion race starts at `self`
    /// with a virtual (`-1`) claim, and a virtual claim can never be
    /// outranked by *any* sibling, real or virtual (`elder_claim_outranks`'s
    /// docs). This node's fingerprint at that level therefore always
    /// champions itself — its own persistent id — regardless of who else is
    /// aggregated in, so distinct not-yet-placed members necessarily
    /// disagree here. That is intentional, not a gap: it mirrors the
    /// existing "route installation stays suppressed while a position is
    /// virtual" rule — a `check_propagation`/`fp_id` guard is equally not
    /// meant to treat virtual, not-yet-real members as already the same
    /// settled g-node.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn fingerprint_id(&self, level: usize) -> Result<Option<Vec<u8>>, QspnError> {
        call(&self.cmd_tx, |reply| Command::FingerprintId {
            level,
            reply,
        })
        .await
    }

    /// Inbound `get_full_etp` (`qspn_manager.get_full_etp`, skeleton at
    /// `qspn.vala:2540-2606`): `arc` is the caller's arc, already resolved
    /// from its `CallerContext` by an [`crate::rpc::ArcResolver`] — this
    /// handle never does that resolution itself (see the crate root docs on
    /// why arc identity is decoupled from Neighborhood). Answered entirely
    /// from local state — no outbound call, so it is always serviced
    /// promptly even while other commands have background fetches in flight.
    ///
    /// # Errors
    /// [`QspnError::NotAnArc`] if `arc` is not one of this node's arcs;
    /// [`QspnError::ActorGone`].
    pub async fn handle_get_full_etp(
        &self,
        arc: ArcId,
        requesting_address: Naddr,
    ) -> Result<EtpMessage, QspnError> {
        call(&self.cmd_tx, |reply| Command::GetFullEtp {
            arc,
            requesting_address,
            reply,
        })
        .await?
    }

    /// Inbound `send_etp` (skeleton at `qspn.vala:2608-2751`). Replies as
    /// soon as this node's own map is updated; forwarding to other arcs (if
    /// needed) happens in the background afterward.
    ///
    /// # Errors
    /// [`QspnError::NotAnArc`]; [`QspnError::ActorGone`].
    pub async fn handle_send_etp(
        &self,
        arc: ArcId,
        etp: EtpMessage,
        is_full: bool,
    ) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::SendEtp {
            arc,
            etp,
            is_full,
            reply,
        })
        .await?
    }

    /// Inbound `got_prepare_destroy` (`qspn.vala:2753-2764`): always a
    /// no-op here — upstream propagates this only to a *connectivity*
    /// identity and starts its self-removal countdown
    /// (`prepare_destroy`/`destroy`'s broadcast teardown, `qspn.vala:
    /// 2450-2505`), which remains out of scope (see
    /// [`Self::check_connectivity`]'s docs); a main identity's own handling
    /// is upstream's `if (is_main_identity) tasklet.exit_tasklet(null);`
    /// early return, which this always matches.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn handle_got_prepare_destroy(&self) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::GotPrepareDestroy { reply }).await
    }

    /// Inbound `got_destroy` (`qspn.vala:2766-2792`): the peer on `arc` is
    /// going away — remove it.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn handle_got_destroy(&self, arc: ArcId) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::GotDestroy { arc, reply }).await
    }

    /// The actor's own live address. Differs from [`Self::my_naddr`] (a
    /// construction-time snapshot, kept for existing callers — see that
    /// method's docs) in that [`Self::make_connectivity`] mutates the
    /// actor's internal address; this queries it live instead of the cached
    /// copy.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn current_naddr(&self) -> Result<Naddr, QspnError> {
        call(&self.cmd_tx, |reply| Command::CurrentNaddr { reply }).await
    }

    /// Whether this identity has finished hooking (`is_bootstrap_complete`,
    /// `qspn.vala:2204-2207`) — always `true` for a `create_net` identity;
    /// for an `enter_net` identity ([`spawn_entering`]), the composition
    /// root's signal that it may take over routing. [`QspnEvent::BootstrapComplete`]
    /// fires the same transition as an event, for a consumer that would
    /// rather subscribe than poll.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn is_bootstrap_complete(&self) -> Result<bool, QspnError> {
        call(&self.cmd_tx, |reply| Command::IsBootstrapComplete { reply }).await
    }

    /// `make_connectivity` (`qspn.vala:2226-2263`): turns this identity into
    /// a *connectivity* identity spanning `[from, to]`, keeping this
    /// g-node's external arcs alive while a successor identity
    /// ([`spawn_entering`]) re-hooks at the new position.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`], or whatever [`crate::QspnState::make_connectivity`]
    /// itself returns (e.g. on a malformed `from`/`to`, as a panic — see that
    /// method's docs).
    pub async fn make_connectivity(
        &self,
        from: usize,
        to: usize,
        update_naddr: impl Fn(&Naddr) -> Naddr + Send + 'static,
    ) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::MakeConnectivity {
            from,
            to,
            update_naddr: Box::new(update_naddr),
            reply,
        })
        .await?
    }

    /// `exit_network(lvl)` (`qspn.vala:2280-2334`): drops every destination
    /// and arc at/above `lvl` — a connectivity identity's own retirement
    /// step, once [`Self::check_connectivity`] confirms it is safe.
    ///
    /// # Errors
    /// [`QspnError::ActorGone`], or whatever [`crate::QspnState::exit_network`]
    /// itself returns.
    pub async fn exit_network(&self, level: usize) -> Result<(), QspnError> {
        call(&self.cmd_tx, |reply| Command::ExitNetwork { level, reply }).await?
    }

    /// `check_connectivity` (`qspn.vala:2371-2448`): true if this
    /// connectivity identity may be retired without disconnecting any
    /// g-node it currently bridges. `prepare_destroy`/`destroy`'s broadcast
    /// teardown that would normally follow (`qspn.vala:2450-2505`) is out of
    /// scope for this crate; the composition root instead tears the actor
    /// down itself once this returns `true` (e.g. via [`Self::exit_network`]
    /// plus dropping the handle/cancelling its token).
    ///
    /// # Errors
    /// [`QspnError::ActorGone`].
    pub async fn check_connectivity(&self) -> Result<bool, QspnError> {
        call(&self.cmd_tx, |reply| Command::CheckConnectivity { reply }).await
    }
}

/// Spawns the QSPN actor for a `create_net`-rooted identity. Returns a
/// cheap-clone [`QspnHandle`] and the actor's own `JoinHandle`; the caller's
/// `JoinSet` should reap the latter (`research/notes/06-rust-stack.md`
/// §Concurrency). `cancel` is this actor's own child token — dropping the
/// handle does not stop the actor; cancelling `cancel` does.
///
/// `my_fingerprint` MUST carry at least `my_naddr.topology().levels()`
/// entries in its `pending_elderships` trail (see [`QspnState::new`]).
#[must_use]
pub fn spawn(
    my_naddr: Naddr,
    my_fingerprint: Fingerprint<Vec<u8>>,
    config: QspnConfig,
    stub_factory: Arc<dyn QspnStubFactory>,
    threshold_calculator: Arc<dyn ThresholdCalculator>,
    arc_id_source: Arc<dyn ArcIdSource>,
    cancel: CancellationToken,
) -> (QspnHandle, tokio::task::JoinHandle<()>) {
    let state = QspnState::new(my_naddr.clone(), my_fingerprint, config);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (events_tx, _) = broadcast::channel(256);
    let initial_snapshot = state.snapshot().unwrap_or_default();
    let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(initial_snapshot));

    let actor = Actor {
        state,
        stub_factory,
        threshold_calculator,
        arc_id_source,
        events_tx: events_tx.clone(),
        snapshot_tx,
        cmd_tx: cmd_tx.clone(),
        timers: JoinSet::new(),
        arc_gather_window: None,
    };
    let join = tokio::spawn(actor.run(cmd_rx, cancel));

    (
        QspnHandle {
            cmd_tx,
            snapshot_rx,
            events_tx,
            my_naddr,
        },
        join,
    )
}

/// Spawns the QSPN actor for an `enter_net`-rooted identity: hooking into an
/// existing network at a (possibly virtual) `my_naddr`. See
/// [`QspnState::new_entering`] for the full parameter contract (every
/// parameter here maps onto it 1:1); the remaining parameters mirror
/// [`spawn`]'s.
///
/// Unlike [`spawn`], the returned actor starts in the bootstrap phase
/// (`qspn.vala:352-354,500-566`): it fetches a full ETP from each of
/// `external_arcs`, exits bootstrap on the first qualifying answer — sender
/// divergence level in `[guest_gnode_level, host_gnode_level)` — or after
/// `config.bootstrap_fallback_max_wait` with no qualifying answer, whichever
/// comes first ([`QspnEvent::BootstrapComplete`] fires then;
/// [`QspnHandle::is_bootstrap_complete`] polls the same transition).
///
/// Deviation from upstream: `bootstrap_phase` fetches its queued arcs one at
/// a time, exiting as soon as the first qualifying ETP arrives
/// (`qspn.vala:506-554`) — an artifact of its cooperative-tasklet scheduler,
/// not an observable protocol invariant (the *set* of arcs tried and the
/// exit condition are what matters). This port fetches every external arc
/// concurrently and reacts to whichever qualifying answer arrives first,
/// which converges at least as fast and never asks an arc twice.
///
/// # Errors
/// Propagates whatever [`QspnState::new_entering`] itself returns.
#[must_use = "propagates a Result; discarding it silently ignores a construction error"]
#[allow(clippy::too_many_arguments)]
pub fn spawn_entering(
    my_naddr: Naddr,
    my_fingerprint: Fingerprint<Vec<u8>>,
    config: QspnConfig,
    stub_factory: Arc<dyn QspnStubFactory>,
    threshold_calculator: Arc<dyn ThresholdCalculator>,
    arc_id_source: Arc<dyn ArcIdSource>,
    internal_arcs: Vec<InternalArc>,
    external_arcs: Vec<(ArcId, Cost)>,
    guest_gnode_level: usize,
    host_gnode_level: usize,
    connectivity: (usize, usize),
    previous_destinations: Vec<HashMap<u32, Destination>>,
    cancel: CancellationToken,
) -> Result<(QspnHandle, tokio::task::JoinHandle<()>), QspnError> {
    let state = QspnState::new_entering(
        my_naddr.clone(),
        my_fingerprint,
        config,
        &internal_arcs,
        &external_arcs,
        guest_gnode_level,
        host_gnode_level,
        connectivity,
        &previous_destinations,
    )?;
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (events_tx, _) = broadcast::channel(256);
    let initial_snapshot = state.snapshot().unwrap_or_default();
    let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(initial_snapshot));

    let actor = Actor {
        state,
        stub_factory,
        threshold_calculator,
        arc_id_source,
        events_tx: events_tx.clone(),
        snapshot_tx,
        cmd_tx: cmd_tx.clone(),
        timers: JoinSet::new(),
        arc_gather_window: None,
    };
    let bootstrap_arcs: Vec<ArcId> = external_arcs.into_iter().map(|(id, _)| id).collect();
    let join = tokio::spawn(actor.run_entering(cmd_rx, bootstrap_arcs, cancel));

    Ok((
        QspnHandle {
            cmd_tx,
            snapshot_rx,
            events_tx,
            my_naddr,
        },
        join,
    ))
}

/// Outcome of ingesting one already-arrived ETP from a single arc
/// (`revise_etp` -> `update_map` -> `update_clusters`, shared by
/// `handle_arc_add_fetched` and the `send_etp` skeleton,
/// `qspn.vala:759-797,2709-2733`).
struct Ingested {
    hops: Vec<HCoord>,
    all_paths_set: Vec<EtpPath>,
    changed_my_gnodes: bool,
}

/// Debounce state for [`Actor::request_arc_gather`] (arc-flap coalescing —
/// see [`crate::QspnConfig::arc_gather_debounce`]'s doc for the full
/// rationale).
struct PendingArcGather {
    /// The most recent arc to report a change during this window; the
    /// trailing catch-up gather (if any) uses this as its `a_changed`.
    latest_arc: ArcId,
    /// Whether an `ArcChanged` has landed since this window's own gather
    /// last fired — set by [`Actor::request_arc_gather`] on a suppressed
    /// change, consumed (and, if set, acted on) by
    /// [`Actor::handle_arc_gather_window_elapsed`].
    trailing_owed: bool,
}

struct Actor {
    state: QspnState,
    stub_factory: Arc<dyn QspnStubFactory>,
    threshold_calculator: Arc<dyn ThresholdCalculator>,
    arc_id_source: Arc<dyn ArcIdSource>,
    events_tx: broadcast::Sender<QspnEvent>,
    snapshot_tx: watch::Sender<Arc<RouteSnapshot>>,
    cmd_tx: mpsc::UnboundedSender<Command>,
    /// Every background task this actor has spawned — outbound network
    /// calls (see module docs) and debounce/periodic timers alike. Reaped
    /// here, its owner, never left dangling.
    timers: JoinSet<()>,
    /// `Some` while an arc-flap gather debounce window is open; see
    /// [`Actor::request_arc_gather`].
    arc_gather_window: Option<PendingArcGather>,
}

/// The periodical full-ETP re-publish loop (`periodical_update`,
/// `qspn.vala:673-684`), started once bootstrap completes for either
/// constructor: [`Actor::spawn_bootstrap_and_periodic`] chains straight into
/// it (`create_net` is complete from the start); [`Actor::do_exit_bootstrap`]
/// spawns it fresh once an `enter_net` identity actually finishes hooking.
async fn periodic_full_etp_loop(
    cmd_tx: mpsc::UnboundedSender<Command>,
    interval: Duration,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(interval) => {
                if cmd_tx.send(Command::PeriodicFullEtp).is_err() {
                    return;
                }
            }
        }
    }
}

impl Actor {
    async fn run(
        mut self,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
        cancel: CancellationToken,
    ) {
        self.spawn_bootstrap_and_periodic(cancel.clone());
        self.run_loop(&mut cmd_rx, &cancel).await;
    }

    /// Entry point for an `enter_net`-rooted identity ([`spawn_entering`]):
    /// starts the bootstrap-phase fetches instead of firing
    /// [`QspnEvent::BootstrapComplete`] immediately.
    async fn run_entering(
        mut self,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
        bootstrap_arcs: Vec<ArcId>,
        cancel: CancellationToken,
    ) {
        self.spawn_bootstrap_entering(bootstrap_arcs, cancel.clone());
        self.run_loop(&mut cmd_rx, &cancel).await;
    }

    async fn run_loop(
        &mut self,
        cmd_rx: &mut mpsc::UnboundedReceiver<Command>,
        cancel: &CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle(cmd).await,
                        None => break,
                    }
                }
                Some(_) = self.timers.join_next(), if !self.timers.is_empty() => {}
            }
        }
        self.timers.shutdown().await;
    }

    fn emit(&self, event: QspnEvent) {
        let _ = self.events_tx.send(event);
    }

    fn emit_all(&self, events: Vec<QspnEvent>) {
        for e in events {
            self.emit(e);
        }
    }

    fn publish_snapshot(&self) {
        match self.state.snapshot() {
            Ok(s) => {
                let _ = self.snapshot_tx.send(Arc::new(s));
            }
            Err(e) => warn!(?e, "failed to build route snapshot"),
        }
    }

    /// Applies `set_ignore_outside_for_sending` to every path about to go on
    /// the wire (`finalize_paths`, `qspn.vala:1817-1820`), applied lazily
    /// right before an `all_paths_set` is actually wrapped into an outgoing
    /// message rather than unconditionally after every `update_map` call —
    /// equivalent, since `all_paths_set` has no other consumer.
    fn finalize_for_sending(&self, mut paths: Vec<EtpPath>) -> Vec<EtpPath> {
        for p in &mut paths {
            flood::set_ignore_outside_for_sending(&self.state, p);
        }
        paths
    }

    // -- Outbound I/O, always spawned, never awaited inline (see module docs) --

    /// Fetches a full ETP from `arc` in the background and reports it back
    /// as [`Command::ArcAddFetched`].
    fn spawn_fetch_for_new_arc(&mut self, arc: ArcId) {
        let stub = self.stub_factory.tcp(arc);
        let my_naddr = self.state.my_naddr().clone();
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            let result = stub.get_full_etp(my_naddr).await;
            let _ = cmd_tx.send(Command::ArcAddFetched { arc, result });
        });
    }

    /// `gather_full_etp_set` (`qspn.vala:860-889,1017-1046`): fetches a full
    /// ETP from every current arc in parallel, in the background, and
    /// reports the whole batch back as one [`Command::GatherComplete`].
    fn spawn_gather(&mut self, a_changed: Option<ArcId>, extra_dead_paths: Vec<EtpPath>) {
        let arcs: Vec<ArcId> = self.state.arcs().collect();
        let my_naddr = self.state.my_naddr().clone();
        let stub_factory = self.stub_factory.clone();
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            let fetches = arcs.into_iter().map(|arc| {
                let stub = stub_factory.tcp(arc);
                let my_naddr = my_naddr.clone();
                async move { (arc, stub.get_full_etp(my_naddr).await) }
            });
            let results = futures::future::join_all(fetches).await;
            let _ = cmd_tx.send(Command::GatherComplete {
                a_changed,
                extra_dead_paths,
                results,
            });
        });
    }

    /// Rate-limits arc-flap-triggered gathers to at most one dispatch per
    /// `config.arc_gather_debounce` — see [`crate::QspnConfig::arc_gather_debounce`]'s
    /// doc for the full rationale. A change while no window is open (the
    /// ordinary single-change case) gathers immediately, exactly as before
    /// this fix. A change that lands while a window from an earlier gather
    /// is still open only updates [`PendingArcGather::latest_arc`]/
    /// `trailing_owed`; [`Self::handle_arc_gather_window_elapsed`] fires
    /// exactly one trailing gather for it once the window closes. A burst
    /// therefore produces at most two gathers (the immediate one plus one
    /// bounded catch-up), never one per change — and the arc's cost itself
    /// is always recorded synchronously by the caller regardless
    /// ([`Self::handle_arc_changed`]), so a suppressed gather never drops
    /// data, only defers *when* the next full re-admission pass runs.
    fn request_arc_gather(&mut self, arc: ArcId) {
        match &mut self.arc_gather_window {
            None => {
                self.spawn_gather(Some(arc), Vec::new());
                self.arc_gather_window = Some(PendingArcGather {
                    latest_arc: arc,
                    trailing_owed: false,
                });
                self.spawn_arc_gather_window();
            }
            Some(pending) => {
                pending.latest_arc = arc;
                pending.trailing_owed = true;
            }
        }
    }

    fn spawn_arc_gather_window(&mut self) {
        let interval = self.state.config().arc_gather_debounce;
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            tokio::time::sleep(interval).await;
            let _ = cmd_tx.send(Command::ArcGatherWindowElapsed);
        });
    }

    /// The debounce window opened by [`Self::request_arc_gather`] closed:
    /// fire the one trailing gather it owes, if any, and re-arm a fresh
    /// window for it so continuous flapping still rate-limits to one
    /// gather per window rather than resuming one-per-change.
    fn handle_arc_gather_window_elapsed(&mut self) {
        let Some(pending) = self.arc_gather_window.take() else {
            return;
        };
        if pending.trailing_owed {
            self.spawn_gather(Some(pending.latest_arc), Vec::new());
            self.arc_gather_window = Some(PendingArcGather {
                latest_arc: pending.latest_arc,
                trailing_owed: false,
            });
            self.spawn_arc_gather_window();
        }
    }

    /// Fire-and-forget unicast send (`send_etp_uni`, `etp_publish.vala:49-80`,
    /// stripped of its synchronous failure handling since nothing here
    /// blocks on the outcome — a failed send is indistinguishable from a
    /// slow one until the next gather/periodic cycle notices the arc is
    /// unreachable).
    fn spawn_unicast(&mut self, arc: ArcId, msg: EtpMessage, is_full: bool) {
        let stub = self.stub_factory.tcp(arc);
        self.timers.spawn(async move {
            let _ = stub.send_etp(msg, is_full).await;
        });
    }

    /// `send_etp_multi` (`etp_publish.vala:24-46`): broadcast to `arcs`,
    /// with reliable-unicast-resend on a missing ack (`MissingArcSendEtp`,
    /// `missing_arcs.vala:24-40`), fire-and-forget.
    fn spawn_broadcast(&mut self, arcs: Vec<ArcId>, msg: EtpMessage, is_full: bool) {
        if arcs.is_empty() {
            return;
        }
        let missing = Arc::new(ResendOnMissing {
            cmd_tx: self.cmd_tx.clone(),
            etp: msg.clone(),
            is_full,
        });
        let stub = self.stub_factory.broadcast(&arcs, Some(missing));
        self.timers.spawn(async move {
            if let Err(e) = stub.send_etp(msg, is_full).await {
                warn!(?e, "send_etp_multi: broadcast failed");
            }
        });
    }

    fn spawn_broadcast_all(&mut self, msg: EtpMessage, is_full: bool) {
        let arcs: Vec<ArcId> = self.state.arcs().collect();
        self.spawn_broadcast(arcs, msg, is_full);
    }

    fn spawn_broadcast_all_but(&mut self, msg: EtpMessage, is_full: bool, except: ArcId) {
        let arcs: Vec<ArcId> = self.state.arcs().filter(|&a| a != except).collect();
        self.spawn_broadcast(arcs, msg, is_full);
    }

    // -- Timers --

    fn spawn_bootstrap_and_periodic(&mut self, cancel: CancellationToken) {
        let signal_delay = self.state.config().bootstrap_signal_delay;
        let interval = self.state.config().periodic_full_etp_interval;
        let cmd_tx = self.cmd_tx.clone();
        let events_tx = self.events_tx.clone();
        let loop_cancel = cancel.clone();
        self.timers.spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(signal_delay) => {}
            }
            // qspn_bootstrap_complete (qspn.vala:206-219): create_net is
            // always bootstrap-complete, so this fires unconditionally,
            // shortly after the actor starts.
            let _ = events_tx.send(QspnEvent::BootstrapComplete);
            // on_bootstrap_complete starts the periodical-update loop
            // (qspn.vala:658-684).
            periodic_full_etp_loop(cmd_tx, interval, loop_cancel).await;
        });
    }

    /// `bootstrap_phase`'s initial fetch wave (`qspn.vala:500-521`): fetches
    /// a full ETP from every `bootstrap_arcs` (the brand-new external arcs
    /// [`spawn_entering`] was given), plus the fallback
    /// `bootstrap_fallback_max_wait` timer (`qspn.vala:556-565`) that forces
    /// bootstrap to exit even with no qualifying answer. See
    /// [`spawn_entering`]'s docs for why this fetches concurrently rather
    /// than one arc at a time.
    fn spawn_bootstrap_entering(&mut self, bootstrap_arcs: Vec<ArcId>, cancel: CancellationToken) {
        self.spawn_bootstrap_fetches(bootstrap_arcs);
        let max_wait = self.state.config().bootstrap_fallback_max_wait;
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => {}
                () = tokio::time::sleep(max_wait) => {
                    let _ = cmd_tx.send(Command::BootstrapTimeout);
                }
            }
        });
    }

    /// The per-arc half of [`Self::spawn_bootstrap_entering`], reusable for
    /// an arc registered *after* the initial wave (`arc_add` during
    /// bootstrap, `qspn.vala:737-742`) without spawning a second fallback
    /// timer.
    fn spawn_bootstrap_fetches(&mut self, arcs: Vec<ArcId>) {
        let my_naddr = self.state.my_naddr().clone();
        for arc in arcs {
            let stub = self.stub_factory.tcp(arc);
            let my_naddr = my_naddr.clone();
            let cmd_tx = self.cmd_tx.clone();
            self.timers.spawn(async move {
                let result = stub.get_full_etp(my_naddr).await;
                let _ = cmd_tx.send(Command::BootstrapEtpFetched { arc, result });
            });
        }
    }

    /// The exit-bootstrap "process all arcs" wave (`qspn.vala:574-622`):
    /// concurrently re-fetches a full ETP from every current arc, reporting
    /// the whole batch back as one [`Command::ExitBootstrapGathered`].
    fn spawn_exit_bootstrap_gather(&mut self) {
        let arcs: Vec<ArcId> = self.state.arcs().collect();
        let my_naddr = self.state.my_naddr().clone();
        let stub_factory = self.stub_factory.clone();
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            let fetches = arcs.into_iter().map(|arc| {
                let stub = stub_factory.tcp(arc);
                let my_naddr = my_naddr.clone();
                async move { (arc, stub.get_full_etp(my_naddr).await) }
            });
            let results = futures::future::join_all(fetches).await;
            let _ = cmd_tx.send(Command::ExitBootstrapGathered { results });
        });
    }

    fn spawn_first_detection_split(&mut self, b_set: Vec<HCoord>, cancel: CancellationToken) {
        let delay = self.state.config().first_detection_split_delay;
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => {}
                () = tokio::time::sleep(delay) => {
                    let _ = cmd_tx.send(Command::FirstDetectionSplitFire(b_set));
                }
            }
        });
    }

    /// `signal_split` (`qspn.vala:1835-1884`): dedup, compute the debounce
    /// threshold *now* (from the paths as known at `update_map` time,
    /// matching upstream calling `calculate_threshold` before the wait, not
    /// after), then schedule the delayed re-check.
    fn spawn_split_signals(&mut self, signals: Vec<SplitSignal>, cancel: CancellationToken) {
        for s in signals {
            if !self.state.begin_pending_split(&s.fp_eldest, &s.fp) {
                continue;
            }
            let bp_eldest_route = to_route_path(&s.bp_eldest, self.state.arc_cost(s.bp_eldest.arc));
            let bp_route = to_route_path(&s.bp, self.state.arc_cost(s.bp.arc));
            let threshold = self
                .threshold_calculator
                .calculate_threshold(&bp_eldest_route, &bp_route);
            let cmd_tx = self.cmd_tx.clone();
            let cancel = cancel.clone();
            let SplitSignal {
                destination,
                fp_eldest,
                fp,
                ..
            } = s;
            self.timers.spawn(async move {
                tokio::select! {
                    () = cancel.cancelled() => {}
                    () = tokio::time::sleep(threshold) => {
                        let _ = cmd_tx.send(Command::SplitTimerFire { destination, fp_eldest, fp });
                    }
                }
            });
        }
    }

    // -- Command dispatch --

    async fn handle(&mut self, cmd: Command) {
        match cmd {
            Command::AddArc { cost, reply } => self.handle_add_arc(cost, reply),
            Command::ArcAddFetched { arc, result } => self.handle_arc_add_fetched(arc, result),
            Command::ArcChanged { arc, cost, reply } => self.handle_arc_changed(arc, cost, reply),
            Command::RemoveArc { arc, reply } => self.handle_remove_arc(arc, reply),
            Command::GatherComplete {
                a_changed,
                extra_dead_paths,
                results,
            } => self.handle_gather_complete(a_changed, extra_dead_paths, results),
            Command::CurrentArcs { reply } => {
                let _ = reply.send(self.state.arcs().collect());
            }
            Command::PeerNaddr { arc, reply } => {
                let _ = reply.send(self.state.peer_naddr(arc).cloned());
            }
            Command::MyEldership { level, reply } => {
                let _ = reply.send(self.state.my_eldership(level));
            }
            Command::Eldership { level, pos, reply } => {
                let _ = reply.send(self.state.eldership(level, pos));
            }
            Command::FingerprintId { level, reply } => {
                let _ = reply.send(self.state.fingerprint(level).map(|fp| fp.id().clone()));
            }
            Command::GetFullEtp {
                arc,
                requesting_address,
                reply,
            } => {
                let _ = reply.send(self.handle_inbound_get_full_etp(arc, requesting_address));
            }
            Command::SendEtp {
                arc,
                etp,
                is_full,
                reply,
            } => {
                let result = self.handle_inbound_send_etp(arc, etp, is_full);
                let _ = reply.send(result);
            }
            Command::GotPrepareDestroy { reply } => {
                // Always a no-op — see `QspnHandle::handle_got_prepare_destroy`'s
                // docs on why (`qspn.vala:2755-2756`).
                let _ = reply.send(());
            }
            Command::GotDestroy { arc, reply } => {
                self.do_remove_arc(arc, false);
                let _ = reply.send(());
            }
            Command::ResendEtp { arc, etp, is_full } => {
                if !self.state.contains_arc(arc) {
                    return;
                }
                let stub = self.stub_factory.tcp(arc);
                let cmd_tx = self.cmd_tx.clone();
                self.timers.spawn(async move {
                    if stub.send_etp(etp, is_full).await.is_err() {
                        let _ = cmd_tx.send(Command::ResendFailed { arc });
                    }
                });
            }
            Command::ResendFailed { arc } => {
                warn!(?arc, "reliable unicast resend failed");
                self.do_remove_arc(arc, true);
            }
            Command::PeriodicFullEtp => self.handle_periodic_full_etp(),
            Command::BootstrapEtpFetched { arc, result } => {
                self.handle_bootstrap_etp_fetched(arc, result);
            }
            Command::BootstrapTimeout => self.handle_bootstrap_timeout(),
            Command::ExitBootstrapGathered { results } => {
                self.handle_exit_bootstrap_gathered(results);
            }
            Command::IsBootstrapComplete { reply } => {
                let _ = reply.send(self.state.is_bootstrap_complete());
            }
            Command::CurrentNaddr { reply } => {
                let _ = reply.send(self.state.my_naddr().clone());
            }
            Command::MakeConnectivity {
                from,
                to,
                update_naddr,
                reply,
            } => self.handle_make_connectivity(from, to, update_naddr, reply),
            Command::PublishConnectivity { old_position } => {
                self.handle_publish_connectivity(old_position);
            }
            Command::ExitNetwork { level, reply } => self.handle_exit_network(level, reply),
            Command::CheckConnectivity { reply } => {
                let _ = reply.send(self.state.check_connectivity());
            }
            Command::FirstDetectionSplitFire(b_set) => self.handle_first_detection_split(b_set),
            Command::SplitTimerFire {
                destination,
                fp_eldest,
                fp,
            } => {
                self.handle_split_timer_fire(destination, fp_eldest, fp);
            }
            Command::ArcGatherWindowElapsed => self.handle_arc_gather_window_elapsed(),
        }
    }

    /// `arc_add`'s synchronous half (`qspn.vala:696-742`): allocate and
    /// register the arc, reply, then kick off the peer fetch in the
    /// background (see module docs).
    fn handle_add_arc(&mut self, cost: Cost, reply: oneshot::Sender<ArcId>) {
        let id = loop {
            let candidate = ArcId::from(self.arc_id_source.next());
            if !self.state.contains_arc(candidate) {
                break candidate;
            }
        };
        self.state.add_arc(id, cost);
        let _ = reply.send(id);
        // No `publish_snapshot()` here (audit-flagged churn, deliberate
        // deviation from the "publish after every mutation" shape the other
        // command handlers use): a bare arc registration has no peer address
        // and no admitted path yet, so it cannot change `RouteSnapshot`'s
        // content (`QspnState::snapshot` only ever walks `destinations`).
        // The first mutation that can actually change it is the peer fetch
        // this method kicks off next, via `handle_arc_add_fetched` /
        // `handle_gather_complete`, both of which already gate their own
        // publish on whether anything was admitted.
        if self.state.is_bootstrap_complete() {
            self.spawn_fetch_for_new_arc(id);
        } else {
            // `arc_add` during bootstrap (`qspn.vala:737-742`): the new arc
            // is just another bootstrap-exit candidate, not the normal
            // forward-on-fetch flow (which only applies once hooked).
            self.spawn_bootstrap_fetches(vec![id]);
        }
    }

    /// `tasklet_arc_add`'s post-fetch half (`qspn.vala:744-797`).
    fn handle_arc_add_fetched(&mut self, arc: ArcId, result: Result<EtpMessage, RpcError>) {
        if !self.state.contains_arc(arc) {
            return; // removed (e.g. by a flap) while the fetch was in flight
        }
        let etp = match result {
            Ok(etp) => etp,
            Err(e) => {
                warn!(?e, ?arc, "arc_add: get_full_etp failed");
                self.do_remove_arc(arc, true);
                return;
            }
        };
        let Some(ingested) = self.ingest_incoming_etp(arc, etp, true) else {
            return;
        };
        // Publish only if this ingest actually admitted/changed a path or
        // this node's own g-node membership — an unchanged re-ingest (e.g.
        // the peer's full ETP restating what this node already knew) would
        // otherwise rebuild and republish an identical snapshot, waking
        // `ntkd`'s route-diff observer for nothing.
        let changed = !ingested.all_paths_set.is_empty() || ingested.changed_my_gnodes;
        if changed {
            self.publish_snapshot();
        }
        if changed && self.state.arcs().count() > 1 {
            let paths = self.finalize_for_sending(ingested.all_paths_set);
            let msg = flood::prepare_new_etp(&self.state, paths, ingested.hops);
            self.spawn_broadcast_all_but(msg, false, arc);
        }
        // Always send a full ETP back to the new arc (qspn.vala:794-796).
        let full = flood::prepare_full_etp(&self.state);
        self.spawn_unicast(arc, full, true);
    }

    /// `arc_is_changed`'s synchronous half (`qspn.vala:821-859`).
    fn handle_arc_changed(
        &mut self,
        arc: ArcId,
        cost: Cost,
        reply: oneshot::Sender<Result<(), QspnError>>,
    ) {
        if !self.state.contains_arc(arc) {
            let _ = reply.send(Err(QspnError::UnknownArc));
            return;
        }
        self.state.set_arc_cost(arc, cost);
        let _ = reply.send(Ok(()));
        // `arc_is_changed` during bootstrap does nothing beyond recording
        // the new cost (`qspn.vala:854-857`) — no re-gather until hooked.
        if self.state.is_bootstrap_complete() {
            self.request_arc_gather(arc);
        }
    }

    /// `arc_remove`'s synchronous half (`qspn.vala:913-995`). During
    /// bootstrap this is a real no-op (`qspn.vala:932-937`): upstream only
    /// dequeues the arc from its bootstrap-candidate set, which this port
    /// has no separate list for (see [`spawn_entering`]'s docs) — the arc
    /// stays registered until bootstrap exits and an ordinary post-bootstrap
    /// `remove_arc` can run.
    fn handle_remove_arc(&mut self, arc: ArcId, reply: oneshot::Sender<Result<(), QspnError>>) {
        if !self.state.contains_arc(arc) {
            let _ = reply.send(Err(QspnError::UnknownArc));
            return;
        }
        if !self.state.is_bootstrap_complete() {
            let _ = reply.send(Ok(()));
            return;
        }
        let removal = self.state.remove_arc(arc);
        // A removal that took zero paths with it (e.g. an arc dropped right
        // after `add_arc`, before any fetch ever landed) cannot have changed
        // `RouteSnapshot`'s content — see `handle_add_arc`'s doc for why a
        // bare arc mutation alone never does.
        let changed = !removal.events.is_empty();
        self.emit_all(removal.events);
        if changed {
            self.publish_snapshot();
        }
        let _ = reply.send(Ok(()));
        self.spawn_gather(None, removal.dead_paths);
    }

    /// `revise_etp` + `update_map` + `update_clusters` over a gathered batch
    /// (`qspn.vala:860-902,1017-1060`), then forward if anything changed.
    fn handle_gather_complete(
        &mut self,
        a_changed: Option<ArcId>,
        extra_dead_paths: Vec<EtpPath>,
        results: Vec<(ArcId, Result<EtpMessage, RpcError>)>,
    ) {
        let mut q: Vec<NodePath> = Vec::new();
        for (arc, result) in results {
            if !self.state.contains_arc(arc) {
                continue; // removed since the gather was kicked off
            }
            let etp = match result {
                Ok(etp) => etp,
                Err(e) => {
                    warn!(?e, ?arc, "gather_full_etp_set: arc failed");
                    self.do_remove_arc(arc, true);
                    continue;
                }
            };
            if !check_incoming_message(&etp, self.state.my_naddr()) {
                warn!(?arc, "gather_full_etp_set: check_incoming_message failed");
                self.do_remove_arc(arc, false);
                continue;
            }
            let old_peer = self.state.record_peer_naddr(arc, etp.node_address.clone());
            let existing = self.state.paths_via_arc0(arc);
            match revise_etp(
                self.state.my_naddr(),
                etp,
                arc,
                old_peer.as_ref(),
                true,
                &existing,
            ) {
                Ok(revised) => q.extend(revised.paths),
                Err(e) => warn!(?e, ?arc, "revise_etp rejected gathered ETP"),
            }
        }

        let outcome = match self.state.update_map(&q, a_changed) {
            Ok(o) => o,
            Err(e) => {
                warn!(?e, "update_map failed");
                return;
            }
        };
        self.emit_all(outcome.events);
        if !outcome.b_set.is_empty() {
            self.spawn_first_detection_split(outcome.b_set, CancellationToken::new());
        }
        self.spawn_split_signals(outcome.split_signals, CancellationToken::new());
        let cluster_events = self.state.update_clusters().unwrap_or_default();
        let changed_my_gnodes = !cluster_events.is_empty();
        self.emit_all(cluster_events);

        let mut all_paths_set = outcome.all_paths_set;
        all_paths_set.extend(extra_dead_paths);
        let changed = !all_paths_set.is_empty() || changed_my_gnodes;
        if changed {
            self.publish_snapshot();
        }
        if changed && self.state.arcs().count() > 0 {
            let paths = self.finalize_for_sending(all_paths_set);
            let msg = flood::prepare_new_etp(&self.state, paths, Vec::new());
            self.spawn_broadcast_all(msg, false);
        }
    }

    /// Shared ingest for a single arc's already-arrived ETP (`revise_etp` ->
    /// `update_map` -> emit -> `update_clusters` -> emit), used by
    /// `handle_arc_add_fetched` and the `send_etp` skeleton. Purely local —
    /// no outbound call — so it never needs to be spawned. Returns `None` if
    /// the message was rejected: malformed input removes the arc; an
    /// acyclic message is just ignored (matching upstream's differing
    /// treatment of the two cases, `qspn.vala:2660-2669` vs `2712-2720`).
    fn ingest_incoming_etp(
        &mut self,
        arc: ArcId,
        etp: EtpMessage,
        is_full: bool,
    ) -> Option<Ingested> {
        if !check_incoming_message(&etp, self.state.my_naddr()) {
            warn!(?arc, "check_incoming_message failed");
            self.do_remove_arc(arc, false);
            return None;
        }
        let old_peer = self.state.record_peer_naddr(arc, etp.node_address.clone());
        let existing = self.state.paths_via_arc0(arc);
        let revised = match revise_etp(
            self.state.my_naddr(),
            etp,
            arc,
            old_peer.as_ref(),
            is_full,
            &existing,
        ) {
            Ok(r) => r,
            Err(QspnError::Acyclic) => {
                warn!(?arc, "revise_etp: cyclic ETP dropped");
                return None;
            }
            Err(e) => {
                warn!(?arc, ?e, "revise_etp failed");
                return None;
            }
        };
        let outcome = match self.state.update_map(&revised.paths, None) {
            Ok(o) => o,
            Err(e) => {
                warn!(?e, "update_map failed");
                return None;
            }
        };
        self.emit_all(outcome.events);
        if !outcome.b_set.is_empty() {
            self.spawn_first_detection_split(outcome.b_set, CancellationToken::new());
        }
        self.spawn_split_signals(outcome.split_signals, CancellationToken::new());
        let cluster_events = self.state.update_clusters().unwrap_or_default();
        let changed_my_gnodes = !cluster_events.is_empty();
        self.emit_all(cluster_events);
        Some(Ingested {
            hops: revised.hops,
            all_paths_set: outcome.all_paths_set,
            changed_my_gnodes,
        })
    }

    /// Inbound `get_full_etp` skeleton (`qspn.vala:2540-2606`) — purely
    /// local, no outbound call.
    fn handle_inbound_get_full_etp(
        &self,
        arc: ArcId,
        requesting_address: Naddr,
    ) -> Result<EtpMessage, QspnError> {
        if !self.state.is_bootstrap_complete() {
            return Err(QspnError::BootstrapInProgress);
        }
        if !self.state.contains_arc(arc) {
            return Err(QspnError::NotAnArc);
        }
        let b = self
            .state
            .my_naddr()
            .hcoord(&requesting_address)
            .map_err(QspnError::Common)?
            .ok_or(QspnError::EtpFromSelf)?;
        let mut paths = Vec::new();
        for level in b.level..self.state.levels() {
            for np in self.state.all_paths_at(level) {
                if np.path.hops.contains(&b) {
                    continue;
                }
                paths.push(crate::path::prepare_for_sending(
                    np,
                    self.state.arc_cost(np.arc),
                ));
            }
        }
        let paths = self.finalize_for_sending(paths);
        Ok(flood::prepare_new_etp(&self.state, paths, Vec::new()))
    }

    /// Inbound `send_etp` skeleton (`qspn.vala:2608-2751`): replies as soon
    /// as the local map is updated; forwarding is spawned separately.
    fn handle_inbound_send_etp(
        &mut self,
        arc: ArcId,
        etp: EtpMessage,
        is_full: bool,
    ) -> Result<(), QspnError> {
        if !self.state.contains_arc(arc) {
            return Err(QspnError::NotAnArc);
        }
        let Some(ingested) = self.ingest_incoming_etp(arc, etp, is_full) else {
            return Ok(());
        };
        let changed = !ingested.all_paths_set.is_empty() || ingested.changed_my_gnodes;
        if changed {
            self.publish_snapshot();
        }
        if changed && self.state.arcs().count() > 1 {
            let paths = self.finalize_for_sending(ingested.all_paths_set);
            let msg = flood::prepare_new_etp(&self.state, paths, ingested.hops);
            self.spawn_broadcast_all_but(msg, false, arc);
        }
        Ok(())
    }

    /// `periodical_update` (`qspn.vala:673-684`).
    fn handle_periodic_full_etp(&mut self) {
        if self.state.arcs().count() == 0 {
            return;
        }
        let msg = flood::prepare_full_etp(&self.state);
        self.spawn_broadcast_all(msg, true);
    }

    /// `start_flood_first_detection_split` (`qspn.vala:1930-1949`).
    fn handle_first_detection_split(&mut self, b_set: Vec<HCoord>) {
        let mut paths = Vec::new();
        for g in b_set {
            if let Some(d) = self.state.destination(g.level, g.pos) {
                for np in &d.paths {
                    paths.push(crate::path::prepare_for_sending(
                        np,
                        self.state.arc_cost(np.arc),
                    ));
                }
            }
        }
        if paths.is_empty() {
            return;
        }
        let paths = self.finalize_for_sending(paths);
        let msg = flood::prepare_new_etp(&self.state, paths, Vec::new());
        self.spawn_broadcast_all(msg, false);
    }

    /// `signal_split`'s post-wait half (`qspn.vala:1851-1883`).
    fn handle_split_timer_fire(
        &mut self,
        destination: HCoord,
        fp_eldest: Fingerprint<Vec<u8>>,
        fp: Fingerprint<Vec<u8>>,
    ) {
        self.state.clear_pending_split(&fp_eldest, &fp);
        for arc in self.state.split_still_live(destination, &fp_eldest, &fp) {
            self.emit(QspnEvent::GnodeSplitted {
                arc,
                destination,
                fingerprint: fp.clone(),
            });
        }
    }

    /// `arc_remove` triggered internally after a failed call
    /// (`send_etp_uni`/`retrieve_full_etp` failure paths, e.g.
    /// `qspn.vala:753-757,58-62`). Purely local.
    fn do_remove_arc(&mut self, arc: ArcId, bad_link: bool) {
        let removal = self.state.remove_arc(arc);
        // See `handle_remove_arc`'s doc: no admitted path removed means
        // `RouteSnapshot` cannot have changed.
        let changed = !removal.events.is_empty();
        self.emit_all(removal.events);
        self.emit(QspnEvent::ArcRemoved { arc, bad_link });
        if changed {
            self.publish_snapshot();
        }
    }

    /// `bootstrap_phase`'s per-arc acceptance test (`qspn.vala:522-554`): a
    /// fetched ETP qualifies this node to exit bootstrap iff its sender's
    /// divergence level falls in `[guest_gnode_level, host_gnode_level)` —
    /// inside the g-node being hooked into, but not already known to be
    /// inside this node's own (still-forming) g-node.
    ///
    /// Deviation from upstream: this reuses [`Self::ingest_incoming_etp`],
    /// which runs [`check_incoming_message`] first (`qspn.vala:506-554`
    /// itself does not) — a strictly safer, never-rejects-a-legitimate-ETP
    /// extra check, kept for the same reason [`spawn_entering`]'s docs give
    /// for fetching concurrently: reusing the already-tested ingest path
    /// beats a second hand-rolled one.
    fn handle_bootstrap_etp_fetched(&mut self, arc: ArcId, result: Result<EtpMessage, RpcError>) {
        if self.state.is_bootstrap_complete() || !self.state.contains_arc(arc) {
            return;
        }
        let etp = match result {
            Ok(etp) => etp,
            Err(e) => {
                warn!(?e, ?arc, "bootstrap: get_full_etp failed");
                self.do_remove_arc(arc, true);
                return;
            }
        };
        let Ok(Some(sender)) = self.state.my_naddr().hcoord(&etp.node_address) else {
            return;
        };
        let guest = self.state.guest_gnode_level();
        let host = self
            .state
            .host_gnode_level()
            .expect("is_bootstrap_complete() checked above");
        if sender.level < guest || sender.level >= host {
            return;
        }
        // Ingest (no forward — qspn.vala:531 "No forward is needed") then exit — see
        // `do_exit_bootstrap`'s own doc for why the watch-channel publish belongs there, not
        // here, even though this call site is the one racing a caller against it.
        let _ = self.ingest_incoming_etp(arc, etp, true);
        self.do_exit_bootstrap();
    }

    /// The fallback max-wait timer fired with no qualifying ETP yet
    /// (`qspn.vala:556-565`): force bootstrap to exit anyway.
    fn handle_bootstrap_timeout(&mut self) {
        if !self.state.is_bootstrap_complete() {
            self.do_exit_bootstrap();
        }
    }

    /// `exit_bootstrap_phase`'s state transition (`qspn.vala:568-573`):
    /// flips to bootstrap-complete, emits [`QspnEvent::BootstrapComplete`],
    /// starts the periodic full-ETP loop (`on_bootstrap_complete`,
    /// `qspn.vala:658-664`), and kicks off the "process all arcs" re-fetch
    /// (`qspn.vala:574-622`).
    ///
    /// Publishes the watch-channel snapshot *synchronously, right here* —
    /// not left for the exit-bootstrap re-fetch's own `publish_snapshot`
    /// (`handle_exit_bootstrap_gathered`) to eventually do. Two things
    /// change the moment [`QspnState::exit_bootstrap`] runs:
    /// [`QspnState::guest_gnode_level`] widens from the in-progress value to
    /// the full topology, which is what makes `state.snapshot()` start
    /// including the higher levels a qualifying bootstrap-fetch ETP
    /// (`handle_bootstrap_etp_fetched`) already ingested into `self.state`
    /// moments earlier but could not yet publish (the same gate suppressed
    /// it). Any caller polling [`QspnHandle::is_bootstrap_complete`] —
    /// `ntkd::node::lifecycle::migrate`'s bootstrap wait loop, which re-diffs
    /// kernel routes against [`QspnHandle::snapshot`] the instant it
    /// observes `true` — races this exact actor for that widened view. The
    /// exit-bootstrap-gather re-fetch this method spawns is the only other
    /// path that would otherwise publish it, asynchronously, and — since it
    /// re-ingests data an already-qualifying arc already holds — with *no
    /// new events* to signal a retry once it lands (`update_map`'s outcome
    /// is empty for unchanged data). Publishing here, before that spawn,
    /// guarantees the watch channel is never stale relative to
    /// `is_bootstrap_complete()` for any caller that observes the flag flip
    /// no earlier than this same command's completion — regression-tested
    /// by `bootstrap_publish_tests::
    /// qualifying_bootstrap_etp_publishes_snapshot_before_flipping_complete`.
    fn do_exit_bootstrap(&mut self) {
        self.state.exit_bootstrap();
        self.publish_snapshot();
        self.emit(QspnEvent::BootstrapComplete);
        let interval = self.state.config().periodic_full_etp_interval;
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(periodic_full_etp_loop(
            cmd_tx,
            interval,
            CancellationToken::new(),
        ));
        self.spawn_exit_bootstrap_gather();
    }

    /// `exit_bootstrap_phase`'s per-arc ingest (`qspn.vala:578-622`, no
    /// forward) followed by the unconditional `publish_full_etp`
    /// (`qspn.vala:624`, `etp_publish.vala:83-108`) and the delayed
    /// [`QspnEvent::PresenceNotified`] (`qspn.vala:625-626`).
    fn handle_exit_bootstrap_gathered(
        &mut self,
        results: Vec<(ArcId, Result<EtpMessage, RpcError>)>,
    ) {
        let mut changed = false;
        for (arc, result) in results {
            if !self.state.contains_arc(arc) {
                continue;
            }
            match result {
                Ok(etp) => {
                    if let Some(ingested) = self.ingest_incoming_etp(arc, etp, true) {
                        changed |= !ingested.all_paths_set.is_empty() || ingested.changed_my_gnodes;
                    }
                }
                Err(e) => {
                    warn!(?e, ?arc, "exit_bootstrap: get_full_etp failed");
                    // `do_remove_arc` already gates its own publish on
                    // whether it actually dropped an admitted path.
                    self.do_remove_arc(arc, true);
                }
            }
        }
        // `do_exit_bootstrap` already published synchronously the instant
        // `is_bootstrap_complete()` flipped true (see that method's doc for
        // why it must not wait for this re-fetch). This re-ingest can still
        // teach the map something new for every *other* arc that wasn't the
        // one qualifying arc, so publish again only if it actually did.
        if changed {
            self.publish_snapshot();
        }
        if self.state.arcs().count() > 0 {
            let msg = flood::prepare_full_etp(&self.state);
            self.spawn_broadcast_all(msg, true);
        }
        let delay = self.state.config().presence_notified_delay;
        let events_tx = self.events_tx.clone();
        self.timers.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = events_tx.send(QspnEvent::PresenceNotified);
        });
    }

    /// `make_connectivity`'s synchronous phase (`qspn.vala:2228-2262`):
    /// mutates state, replies, then schedules the delayed
    /// `publish_connectivity` void-ETP announcement (outbound I/O — see
    /// module docs).
    fn handle_make_connectivity(
        &mut self,
        from: usize,
        to: usize,
        update_naddr: Box<dyn Fn(&Naddr) -> Naddr + Send>,
        reply: oneshot::Sender<Result<(), QspnError>>,
    ) {
        let outcome = match self.state.make_connectivity(from, to, |n| update_naddr(n)) {
            Ok(o) => o,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        // Gate on whether `update_clusters` actually produced an event —
        // see `handle_add_arc`'s doc: `make_connectivity` itself only
        // rewrites `my_naddr`/arc peer addresses/connectivity range, none of
        // which `QspnState::snapshot` reads; only a fingerprint/nodes-inside
        // change (or a future consumer of those fields) could matter.
        let changed = !outcome.events.is_empty();
        self.emit_all(outcome.events);
        if changed {
            self.publish_snapshot();
        }
        let _ = reply.send(Ok(()));

        let delay = self.state.config().publish_connectivity_delay;
        let old_position = outcome.old_position;
        let cmd_tx = self.cmd_tx.clone();
        self.timers.spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = cmd_tx.send(Command::PublishConnectivity { old_position });
        });
    }

    /// `publish_connectivity` (`etp_publish.vala:110-144`): a void ETP
    /// (empty path list, `hops = [old_position]`) to every arc outside the
    /// vacated g-node, so neighbors withdraw the now-obsolete gateway
    /// without waiting for full-ETP GC. Re-evaluates the outer-arc set
    /// against *live* state, matching upstream firing this off a delayed
    /// tasklet rather than precomputing it at `make_connectivity` time.
    fn handle_publish_connectivity(&mut self, old_position: HCoord) {
        let outer_arcs = self.outer_arcs_after_connectivity(old_position);
        let msg = flood::prepare_new_etp(&self.state, Vec::new(), vec![old_position]);
        self.spawn_broadcast(outer_arcs, msg, false);
    }

    /// `publish_connectivity`'s outer-arc selection (`etp_publish.vala:
    /// 117-126`): every arc whose peer is known and either shares
    /// `old_position`'s level but a different position, or lies at a level
    /// above it.
    fn outer_arcs_after_connectivity(&self, old_position: HCoord) -> Vec<ArcId> {
        self.state
            .arcs()
            .filter(|&arc| {
                let Some(peer) = self.state.peer_naddr(arc) else {
                    return false;
                };
                let Ok(Some(h)) = self.state.my_naddr().hcoord(peer) else {
                    return false;
                };
                (h.level == old_position.level && h.pos != old_position.pos)
                    || h.level > old_position.level
            })
            .collect()
    }

    /// `exit_network(lvl)`'s synchronous phase plus the survivors' heads-up
    /// full ETP (`qspn.vala:2280-2313`).
    ///
    /// Deviation from upstream: the heads-up full ETP is built and broadcast
    /// *after* [`QspnState::exit_network`] has already removed the departing
    /// arcs and stripped their destinations (upstream sends it just before
    /// removing them, `qspn.vala:2308-2313`), so it never advertises a path
    /// through an arc about to disappear; delivery also gets this crate's
    /// usual missing-arc-retry instead of upstream's bare unicast. Both are
    /// strict improvements over the reference ordering, not behavior changes
    /// a consumer could observe as wrong.
    fn handle_exit_network(&mut self, level: usize, reply: oneshot::Sender<Result<(), QspnError>>) {
        let outcome = match self.state.exit_network(level) {
            Ok(o) => o,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let changed = !outcome.events.is_empty();
        self.emit_all(outcome.events);
        if changed {
            self.publish_snapshot();
        }
        let _ = reply.send(Ok(()));

        if self.state.arcs().count() > 0 {
            let msg = flood::prepare_full_etp(&self.state);
            self.spawn_broadcast_all(msg, true);
        }
    }
}

/// `MissingArcSendEtp` (`research/impl/vala/qspn/missing_arcs.vala:24-40`):
/// on a missed broadcast ack, resend reliably via unicast — routed back
/// through the actor's own command queue so the resend is spawned uniformly
/// like every other outbound call, rather than reaching into the transport
/// from inside this synchronous callback.
struct ResendOnMissing {
    cmd_tx: mpsc::UnboundedSender<Command>,
    etp: EtpMessage,
    is_full: bool,
}

impl MissingArcHandler for ResendOnMissing {
    fn missing(&self, arc: ArcId) {
        let _ = self.cmd_tx.send(Command::ResendEtp {
            arc,
            etp: self.etp.clone(),
            is_full: self.is_full,
        });
    }
}

#[cfg(test)]
mod eldership_tests {
    use ntk_common::Topology;

    use super::*;
    use crate::arc::DefaultArcIdSource;
    use crate::config::FixedThreshold;
    use crate::fake::FakeQspnStubFactory;

    /// Builds a bare `Actor` (no spawned task, no other node) directly
    /// mutable in the test — lets a test drive [`Actor::handle`] and read
    /// [`Actor::state`] side by side against the very same live state.
    fn test_actor(levels: usize, eldership: u32, pending: &[u32]) -> Actor {
        let topology = Topology::new(vec![4u32; levels]).expect("valid topology");
        let naddr = Naddr::new(topology, vec![0u32; levels]).expect("valid address");
        let fp = Fingerprint::new(vec![1u8], eldership, pending.to_vec());
        let state = QspnState::new(naddr, fp, QspnConfig::default());
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(16);
        let snapshot_tx = watch::channel(Arc::new(state.snapshot().unwrap_or_default())).0;
        Actor {
            state,
            stub_factory: Arc::new(FakeQspnStubFactory::new()),
            threshold_calculator: Arc::new(FixedThreshold(std::time::Duration::from_millis(1))),
            arc_id_source: Arc::new(DefaultArcIdSource::default()),
            events_tx,
            snapshot_tx,
            cmd_tx,
            timers: JoinSet::new(),
            arc_gather_window: None,
        }
    }

    /// The `MyEldership` command path must answer with exactly what
    /// [`QspnState::my_eldership`] itself returns for the same live state,
    /// for every level including one past the top.
    #[tokio::test]
    async fn my_eldership_command_matches_direct_state_read() {
        let mut actor = test_actor(2, 5, &[7, 9]);
        for level in [0usize, 1, 2, 3] {
            let expected = actor.state.my_eldership(level);
            let (tx, rx) = oneshot::channel();
            actor
                .handle(Command::MyEldership { level, reply: tx })
                .await;
            assert_eq!(rx.await.unwrap(), expected);
        }
    }

    /// The `Eldership` command path must answer with exactly what
    /// [`QspnState::eldership`] itself returns for a real, admitted
    /// destination, for both a known and an unknown position.
    #[tokio::test]
    async fn eldership_command_matches_direct_state_read() {
        let mut actor = test_actor(2, 0, &[0, 0]);
        let d = HCoord::new(1, 2);
        let fp = Fingerprint::new(vec![10u8], 1, vec![100u32])
            .construct(&[], false)
            .unwrap();
        let np = NodePath::new(
            ArcId::from(1u32),
            EtpPath {
                hops: vec![d],
                arcs: vec![ArcId::from(1u32)],
                cost: Cost::Finite(1),
                fingerprint: fp,
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        actor.state.update_map(&[np], None).unwrap();

        for pos in [2u32, 3u32] {
            let expected = actor.state.eldership(1, pos);
            let (tx, rx) = oneshot::channel();
            actor
                .handle(Command::Eldership {
                    level: 1,
                    pos,
                    reply: tx,
                })
                .await;
            assert_eq!(rx.await.unwrap(), expected);
        }
        // Sanity: the known position actually resolved to a real value, so
        // the two prior asserts were not trivially None == None.
        assert_eq!(actor.state.eldership(1, 2).unwrap(), Some(Some(100)));
    }

    /// The `FingerprintId` command path must answer with exactly what
    /// [`Fingerprint::id`] carries in the live state, for every level
    /// including one past the top.
    #[tokio::test]
    async fn fingerprint_id_command_matches_direct_state_read() {
        let mut actor = test_actor(2, 5, &[7, 9]);
        for level in [0usize, 1, 2, 3] {
            let expected = actor.state.fingerprint(level).map(|fp| fp.id().clone());
            let (tx, rx) = oneshot::channel();
            actor
                .handle(Command::FingerprintId { level, reply: tx })
                .await;
            assert_eq!(rx.await.unwrap(), expected);
        }
    }

    /// A more senior (lower-eldership) sibling joining the aggregation must
    /// change the champion `id` at the level above — the value actually
    /// tracks g-node *identity* rather than being pinned to this node's own
    /// id forever.
    #[tokio::test]
    async fn fingerprint_id_changes_when_a_more_senior_sibling_joins() {
        let mut actor = test_actor(2, 5, &[7, 9]);
        let before = actor.state.fingerprint(1).map(|fp| fp.id().clone());
        assert_eq!(before, Some(vec![1u8]), "champions itself: no siblings yet");

        let sibling = NodePath::new(
            ArcId::from(1u32),
            EtpPath {
                hops: vec![HCoord::new(0, 2)],
                arcs: vec![ArcId::from(1u32)],
                cost: Cost::Finite(1),
                // Eldership 1 outranks this node's own claim of 5 (lower is
                // more senior).
                fingerprint: Fingerprint::new(vec![99u8], 1, vec![100u32]),
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        actor.state.update_map(&[sibling], None).unwrap();
        actor.state.update_clusters().unwrap();

        let after = actor.state.fingerprint(1).map(|fp| fp.id().clone());
        assert_eq!(after, Some(vec![99u8]));
        assert_ne!(before, after);

        let (tx, rx) = oneshot::channel();
        actor
            .handle(Command::FingerprintId {
                level: 1,
                reply: tx,
            })
            .await;
        assert_eq!(rx.await.unwrap(), after);
    }

    /// Two members of the very same g-node, each aggregating from its own
    /// local view, must land on the same champion `id` at the level above —
    /// this is what makes the value a g-node identity rather than a
    /// per-node one. Node A (pos 0, eldership 5) and node B (pos 2,
    /// eldership 1) each see the other as a level-0 sibling; both must
    /// settle on B (the more senior, lower claim) as champion.
    #[test]
    fn fingerprint_id_agrees_across_members_of_the_same_gnode() {
        let topology = Topology::new(vec![4u32; 2]).expect("valid topology");

        let mut a = QspnState::new(
            Naddr::new(topology.clone(), vec![0, 0]).expect("valid address"),
            Fingerprint::new(vec![1u8], 5, vec![7, 9]),
            QspnConfig::default(),
        );
        let b_as_seen_by_a = NodePath::new(
            ArcId::from(1u32),
            EtpPath {
                hops: vec![HCoord::new(0, 2)],
                arcs: vec![ArcId::from(1u32)],
                cost: Cost::Finite(1),
                fingerprint: Fingerprint::new(vec![99u8], 1, vec![100u32]),
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        a.update_map(&[b_as_seen_by_a], None).unwrap();
        a.update_clusters().unwrap();

        let mut b = QspnState::new(
            Naddr::new(topology, vec![2, 0]).expect("valid address"),
            Fingerprint::new(vec![99u8], 1, vec![7, 9]),
            QspnConfig::default(),
        );
        let a_as_seen_by_b = NodePath::new(
            ArcId::from(1u32),
            EtpPath {
                hops: vec![HCoord::new(0, 0)],
                arcs: vec![ArcId::from(1u32)],
                cost: Cost::Finite(1),
                fingerprint: Fingerprint::new(vec![1u8], 5, vec![100u32]),
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        b.update_map(&[a_as_seen_by_b], None).unwrap();
        b.update_clusters().unwrap();

        let id_from_a = a.fingerprint(1).map(|fp| fp.id().clone());
        let id_from_b = b.fingerprint(1).map(|fp| fp.id().clone());
        assert_eq!(id_from_a, Some(vec![99u8]));
        assert_eq!(id_from_a, id_from_b);
    }

    /// While this node's own position at level 0 is virtual (reserved, not
    /// yet placed), it must always champion itself at level 1 — even a real
    /// sibling with a far more senior claim must not win, per
    /// `elder_claim_outranks`'s "once current is virtual it can never be
    /// outranked" rule. Distinct not-yet-placed members therefore disagree
    /// on this value by design, mirroring "route installation stays
    /// suppressed while a position is virtual".
    #[test]
    fn fingerprint_id_champions_itself_while_position_is_virtual() {
        let topology = Topology::new(vec![4u32; 2]).expect("valid topology");
        // Position 10 at level 0 is virtual: gsize(0) == 4.
        let naddr =
            Naddr::new_allowing_virtual(topology, vec![10, 0]).expect("valid virtual address");
        let mut state = QspnState::new(
            naddr,
            Fingerprint::new(vec![7u8], 5, vec![7, 9]),
            QspnConfig::default(),
        );

        let sibling = NodePath::new(
            ArcId::from(1u32),
            EtpPath {
                hops: vec![HCoord::new(0, 2)],
                arcs: vec![ArcId::from(1u32)],
                cost: Cost::Finite(1),
                // Eldership 0: as senior a real claim as this topology
                // allows, yet still must not depose a virtual champion.
                fingerprint: Fingerprint::new(vec![99u8], 0, vec![100u32]),
                nodes_inside: 1,
                ignore_outside: vec![false; 2],
            },
        );
        state.update_map(&[sibling], None).unwrap();
        state.update_clusters().unwrap();

        assert_eq!(
            state.fingerprint(1).map(|fp| fp.id().clone()),
            Some(vec![7u8]),
            "a virtual own-position must always champion itself"
        );
    }
}

/// Regression coverage for the "loser's route never comes back after
/// rehook re-attached the arc" bug (`ntkd::node::lifecycle::migrate`'s
/// bootstrap wait loop polls [`QspnHandle::is_bootstrap_complete`] and,
/// the instant it observes `true`, immediately re-diffs kernel routes
/// against [`QspnHandle::snapshot`] — see that function's own doc).
#[cfg(test)]
mod bootstrap_publish_tests {
    use ntk_common::Topology;

    use super::*;
    use crate::arc::DefaultArcIdSource;
    use crate::config::FixedThreshold;
    use crate::fake::FakeQspnStubFactory;
    use crate::flood::prepare_full_etp;

    /// A bare entering [`Actor`] (no spawned task), mirroring
    /// `eldership_tests::test_actor` but via [`QspnState::new_entering`] with
    /// zero arcs at construction — exactly `ntkd::node::lifecycle::migrate`'s
    /// own `reattach_known_arcs` shape: the entering identity starts with no
    /// arcs, and every known arc is `add_arc`'d in afterward.
    fn entering_actor(
        guest_gnode_level: usize,
        host_gnode_level: usize,
    ) -> (Actor, Naddr, watch::Receiver<Arc<RouteSnapshot>>) {
        let topology = Topology::new(vec![4u32; 2]).expect("valid topology");
        let naddr = Naddr::new(topology, vec![0u32; 2]).expect("valid address");
        let fp = Fingerprint::new(vec![1u8], 0, vec![0u32; 2]);
        let state = QspnState::new_entering(
            naddr.clone(),
            fp,
            QspnConfig::default(),
            &[],
            &[],
            guest_gnode_level,
            host_gnode_level,
            (0, 0),
            &[],
        )
        .expect("valid entering state");
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(16);
        // `watch::Sender::send` silently no-ops (never applies the value) once its receiver
        // count drops to zero — the receiver half must stay alive for `publish_snapshot`'s
        // sends to actually land, exactly like the real `QspnHandle::snapshot_rx` does for the
        // whole life of a real actor.
        let (snapshot_tx, snapshot_rx) =
            watch::channel(Arc::new(state.snapshot().unwrap_or_default()));
        (
            Actor {
                state,
                stub_factory: Arc::new(FakeQspnStubFactory::new()),
                threshold_calculator: Arc::new(FixedThreshold(Duration::from_millis(1))),
                arc_id_source: Arc::new(DefaultArcIdSource::default()),
                events_tx,
                snapshot_tx,
                cmd_tx,
                timers: JoinSet::new(),
                arc_gather_window: None,
            },
            naddr,
            snapshot_rx,
        )
    }

    /// The moment a qualifying bootstrap-fetch ETP flips
    /// `is_bootstrap_complete()` true, the watch-channel snapshot
    /// [`QspnHandle::snapshot`] reads must already carry the destination
    /// that same ETP just taught this node — not just `self.state`.
    ///
    /// Before the fix: [`Actor::handle_bootstrap_etp_fetched`] ingests the
    /// qualifying ETP (mutating `self.state`, emitting events) and then
    /// calls [`Actor::do_exit_bootstrap`], both without ever calling
    /// [`Actor::publish_snapshot`] — the watch channel is left exactly as it
    /// was before this command ran. The *only* thing that eventually
    /// publishes it is the redundant re-fetch `do_exit_bootstrap` itself
    /// kicks off (`spawn_exit_bootstrap_gather` -> `handle_exit_bootstrap_gathered`),
    /// which is asynchronous, unavoidably real async work — and, since it
    /// re-ingests data this same arc already holds, produces *no new events*
    /// (`update_map`'s outcome is empty), so nothing tells an event-driven
    /// consumer to re-diff once it does land. Any caller that observes
    /// `is_bootstrap_complete() == true` and immediately reads `snapshot()`
    /// in that same window — exactly `migrate`'s bootstrap wait loop — can
    /// therefore see a destination-less snapshot and never get a second
    /// chance to retry, permanently skipping the kernel route install.
    #[tokio::test]
    async fn qualifying_bootstrap_etp_publishes_snapshot_before_flipping_complete() {
        let (mut actor, my_naddr, snapshot_rx) = entering_actor(1, 2);
        let arc = ArcId::from(1u32);
        actor.state.add_arc(arc, Cost::Finite(10));

        // A peer diverging from `my_naddr` at exactly level 1 — inside
        // `[guest_gnode_level, host_gnode_level) = [1, 2)`, so its full ETP
        // qualifies this node to exit bootstrap on the spot.
        let peer_naddr = Naddr::new(my_naddr.topology().clone(), vec![0u32, 1u32]).unwrap();
        let peer_fp = Fingerprint::new(vec![2u8], 0, vec![0u32; 2]);
        let peer_state = QspnState::new(peer_naddr, peer_fp, QspnConfig::default());
        let etp = prepare_full_etp(&peer_state);

        assert!(!actor.state.is_bootstrap_complete());
        actor
            .handle(Command::BootstrapEtpFetched {
                arc,
                result: Ok(etp),
            })
            .await;

        assert!(
            actor.state.is_bootstrap_complete(),
            "a same-arc, divergence-level-1 ETP must qualify bootstrap exit"
        );
        let published = snapshot_rx.borrow().clone();
        assert!(
            published.levels[1].iter().any(|e| e.destination.pos == 1),
            "the watch-channel snapshot must already carry the peer's g-node the instant \
             is_bootstrap_complete() can first observe true, not only once the redundant \
             exit-bootstrap re-fetch eventually completes: {published:?}"
        );
    }
}

/// Regression coverage for the watch-channel churn the audit flagged: a
/// mutation that cannot possibly change [`RouteSnapshot`]'s content must not
/// rebuild-and-send one (`Actor::publish_snapshot`'s call sites), but a
/// mutation that genuinely does must never be suppressed — `ntkd`'s
/// route-diff observer only re-diffs when it sees a *new* value on the
/// channel.
#[cfg(test)]
mod publish_gating_tests {
    use ntk_common::Topology;

    use super::*;
    use crate::arc::DefaultArcIdSource;
    use crate::config::FixedThreshold;
    use crate::fake::FakeQspnStubFactory;
    use crate::flood::prepare_full_etp;

    /// Mirrors `bootstrap_publish_tests::entering_actor`, but for an
    /// already-bootstrap-complete (`create_net`-rooted) identity, matching
    /// [`QspnState::new`]'s own default.
    fn bare_actor(levels: usize) -> (Actor, Naddr, watch::Receiver<Arc<RouteSnapshot>>) {
        let topology = Topology::new(vec![4u32; levels]).expect("valid topology");
        let naddr = Naddr::new(topology, vec![0u32; levels]).expect("valid address");
        let fp = Fingerprint::new(vec![1u8], 0, vec![0u32; levels]);
        let state = QspnState::new(naddr.clone(), fp, QspnConfig::default());
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(16);
        let (snapshot_tx, snapshot_rx) =
            watch::channel(Arc::new(state.snapshot().unwrap_or_default()));
        (
            Actor {
                state,
                stub_factory: Arc::new(FakeQspnStubFactory::new()),
                threshold_calculator: Arc::new(FixedThreshold(Duration::from_millis(1))),
                arc_id_source: Arc::new(DefaultArcIdSource::default()),
                events_tx,
                snapshot_tx,
                cmd_tx,
                timers: JoinSet::new(),
                arc_gather_window: None,
            },
            naddr,
            snapshot_rx,
        )
    }

    /// A bare arc registration carries no peer address and no admitted path
    /// yet, so it cannot change `RouteSnapshot`'s content —
    /// `handle_add_arc` must not publish for it.
    #[tokio::test]
    async fn adding_a_bare_arc_does_not_publish_a_snapshot() {
        let (mut actor, _my_naddr, mut snapshot_rx) = bare_actor(2);
        snapshot_rx.borrow_and_update();

        let (tx, rx) = oneshot::channel();
        actor
            .handle(Command::AddArc {
                cost: Cost::Finite(10),
                reply: tx,
            })
            .await;
        rx.await.unwrap();

        assert!(
            !snapshot_rx.has_changed().unwrap(),
            "a bare arc registration must not publish a snapshot"
        );
    }

    /// A qualifying peer ETP that actually admits a new destination must
    /// publish a fresh, up-to-date snapshot.
    #[tokio::test]
    async fn a_real_map_change_publishes_a_fresh_snapshot() {
        let (mut actor, my_naddr, mut snapshot_rx) = bare_actor(2);
        let arc = ArcId::from(1u32);
        actor.state.add_arc(arc, Cost::Finite(10));
        snapshot_rx.borrow_and_update();

        let peer_naddr = Naddr::new(my_naddr.topology().clone(), vec![0u32, 1u32]).unwrap();
        let peer_fp = Fingerprint::new(vec![2u8], 0, vec![0u32; 2]);
        let peer_state = QspnState::new(peer_naddr, peer_fp, QspnConfig::default());
        let etp = prepare_full_etp(&peer_state);

        actor
            .handle(Command::ArcAddFetched {
                arc,
                result: Ok(etp),
            })
            .await;

        assert!(
            snapshot_rx.has_changed().unwrap(),
            "an ETP that admits a new destination must publish a fresh snapshot"
        );
        let published = snapshot_rx.borrow_and_update().clone();
        assert!(
            published.levels[1].iter().any(|e| e.destination.pos == 1),
            "the published snapshot must actually carry the peer's g-node: {published:?}"
        );
    }
}

/// Regression coverage for the arc-flap fan-out the audit flagged:
/// [`Actor::request_arc_gather`] must coalesce a burst of `ArcChanged`
/// commands into far fewer than one gather per change, while never delaying
/// a genuinely isolated change.
#[cfg(test)]
mod arc_gather_debounce_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::future::BoxFuture;
    use ntk_common::Topology;

    use super::*;
    use crate::arc::DefaultArcIdSource;
    use crate::config::FixedThreshold;
    use crate::flood::prepare_full_etp;
    use crate::stub::QspnStub;

    /// Counts `get_full_etp` calls — one per arc a gather actually fetched
    /// — so a test can tell how many *gathers*
    /// [`Actor::request_arc_gather`] dispatched without needing a real peer
    /// on the other end of the link. Answers with a real (always the same,
    /// cloned) peer ETP rather than an error, so a fetch never trips
    /// `handle_gather_complete`'s failed-arc removal path and the arc stays
    /// registered across every gather a test drives.
    struct CountingStub {
        fetches: AtomicUsize,
        etp: EtpMessage,
    }

    impl QspnStub for CountingStub {
        fn get_full_etp(
            &self,
            _requesting_address: Naddr,
        ) -> BoxFuture<'_, Result<EtpMessage, RpcError>> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(self.etp.clone()) })
        }
        fn send_etp(
            &self,
            _etp: EtpMessage,
            _is_full: bool,
        ) -> BoxFuture<'_, Result<(), RpcError>> {
            Box::pin(async { Ok(()) })
        }
        fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
            Box::pin(async { Ok(()) })
        }
        fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct CountingStubFactory(Arc<CountingStub>);

    impl QspnStubFactory for CountingStubFactory {
        fn broadcast(
            &self,
            _arcs: &[ArcId],
            _missing: Option<Arc<dyn MissingArcHandler>>,
        ) -> Arc<dyn QspnStub> {
            self.0.clone()
        }
        fn tcp(&self, _arc: ArcId) -> Arc<dyn QspnStub> {
            self.0.clone()
        }
    }

    fn debounced_actor(
        debounce: Duration,
    ) -> (
        Actor,
        Arc<CountingStub>,
        ArcId,
        mpsc::UnboundedReceiver<Command>,
    ) {
        let topology = Topology::new(vec![4u32; 1]).expect("valid topology");
        let naddr = Naddr::new(topology.clone(), vec![0u32]).expect("valid address");
        let fp = Fingerprint::new(vec![1u8], 0, vec![0u32]);
        let config = QspnConfig {
            arc_gather_debounce: debounce,
            ..QspnConfig::default()
        };
        let mut state = QspnState::new(naddr, fp, config);
        let arc = ArcId::from(1u32);
        state.add_arc(arc, Cost::Finite(10));

        // A well-formed peer ETP every fetch answers with — just needs to
        // pass `check_incoming_message` so a fetch never looks like a
        // failed/misbehaving arc.
        let peer_naddr = Naddr::new(topology, vec![1u32]).expect("valid peer address");
        let peer_fp = Fingerprint::new(vec![2u8], 0, vec![0u32]);
        let peer_state = QspnState::new(peer_naddr, peer_fp, QspnConfig::default());
        let etp = prepare_full_etp(&peer_state);

        let stub = Arc::new(CountingStub {
            fetches: AtomicUsize::new(0),
            etp,
        });
        let stub_factory = Arc::new(CountingStubFactory(stub.clone()));
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(16);
        let snapshot_tx = watch::channel(Arc::new(state.snapshot().unwrap_or_default())).0;
        let actor = Actor {
            state,
            stub_factory,
            threshold_calculator: Arc::new(FixedThreshold(Duration::from_millis(1))),
            arc_id_source: Arc::new(DefaultArcIdSource::default()),
            events_tx,
            snapshot_tx,
            cmd_tx,
            timers: JoinSet::new(),
            arc_gather_window: None,
        };
        (actor, stub, arc, cmd_rx)
    }

    async fn send_arc_changed(actor: &mut Actor, arc: ArcId, cost: Cost) {
        let (tx, rx) = oneshot::channel();
        actor
            .handle(Command::ArcChanged {
                arc,
                cost,
                reply: tx,
            })
            .await;
        rx.await.unwrap().unwrap();
    }

    /// Processes every command a just-completed gather posted back
    /// (`Command::GatherComplete`), exactly like the real run loop's
    /// `select!` would before looking at anything else — so a later,
    /// targeted `cmd_rx.try_recv()` for a *specific* command (e.g. the
    /// debounce window's own) is never shadowed by one still sitting ahead
    /// of it in the queue.
    async fn drain_pending_commands(
        actor: &mut Actor,
        cmd_rx: &mut mpsc::UnboundedReceiver<Command>,
    ) {
        while let Ok(cmd) = cmd_rx.try_recv() {
            actor.handle(cmd).await;
        }
    }

    /// Fully drains every currently-ready timer task and the commands each
    /// one's completion posts, until both are quiescent -- for checkpoints
    /// where a test wants "everything that can happen without further
    /// virtual time elapsing has happened" rather than needing to reason
    /// about the exact interleaving of gather/broadcast/debounce tasks.
    async fn settle(actor: &mut Actor, cmd_rx: &mut mpsc::UnboundedReceiver<Command>) {
        loop {
            drain_pending_commands(actor, cmd_rx).await;
            if actor.timers.is_empty() {
                break;
            }
            actor.timers.join_next().await;
        }
    }

    /// A lone arc change with no debounce window already open must gather
    /// immediately — no `tokio::time::advance` is needed to observe it.
    #[tokio::test(start_paused = true)]
    async fn single_isolated_arc_change_gathers_immediately_without_waiting_for_the_debounce_window()
     {
        let (mut actor, stub, arc, _cmd_rx) = debounced_actor(Duration::from_millis(200));

        send_arc_changed(&mut actor, arc, Cost::Finite(20)).await;
        assert_eq!(
            stub.fetches.load(Ordering::SeqCst),
            0,
            "the gather task is spawned, not yet polled"
        );

        actor.timers.join_next().await;
        assert_eq!(
            stub.fetches.load(Ordering::SeqCst),
            1,
            "an isolated change must gather promptly, without waiting out the debounce window"
        );
    }

    /// A burst of rapid arc changes — none separated by any elapsed virtual
    /// time — must coalesce into exactly one dispatched gather, not one per
    /// change, and must remember the rest as a still-owed trailing gather
    /// rather than silently dropping them.
    #[tokio::test(start_paused = true)]
    async fn burst_of_rapid_arc_changes_coalesces_to_one_gather() {
        let (mut actor, stub, arc, _cmd_rx) = debounced_actor(Duration::from_millis(200));

        for cost in [10u64, 20, 30, 40, 50] {
            send_arc_changed(&mut actor, arc, Cost::Finite(cost)).await;
        }

        // Resolve exactly the one task that is actually ready right now —
        // the leading gather. The debounce window's own sleep task is still
        // pending (no virtual time has elapsed), so this cannot also drain
        // it.
        actor.timers.join_next().await;
        assert_eq!(
            stub.fetches.load(Ordering::SeqCst),
            1,
            "a burst of 5 rapid arc changes must produce one gather, not 5"
        );
        let pending = actor
            .arc_gather_window
            .as_ref()
            .expect("a debounce window must still be open");
        assert!(
            pending.trailing_owed,
            "the 4 changes suppressed during the window must be remembered, not dropped"
        );
    }

    /// Once the debounce window closes, exactly one trailing gather catches
    /// up whatever was suppressed during it — bounding how stale admission
    /// can get after a burst, rather than leaving it stale until some
    /// unrelated event happens to trigger the next gather.
    #[tokio::test(start_paused = true)]
    async fn debounce_window_closing_fires_exactly_one_trailing_gather() {
        let debounce = Duration::from_millis(200);
        let (mut actor, stub, arc, mut cmd_rx) = debounced_actor(debounce);

        for cost in [10u64, 20, 30] {
            send_arc_changed(&mut actor, arc, Cost::Finite(cost)).await;
        }
        actor.timers.join_next().await; // the leading gather
        assert_eq!(stub.fetches.load(Ordering::SeqCst), 1);
        drain_pending_commands(&mut actor, &mut cmd_rx).await;

        tokio::time::advance(debounce).await;
        settle(&mut actor, &mut cmd_rx).await;
        assert_eq!(
            stub.fetches.load(Ordering::SeqCst),
            2,
            "exactly one bounded trailing gather must catch up the changes the window \
             suppressed, never zero (permanently stale) and never one per suppressed change"
        );
    }
}
