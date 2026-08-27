//! The actor: [`Manager`] owns every mutable piece of protocol state behind
//! one `mpsc` command queue (`research/notes/06-rust-stack.md` §Concurrency
//! — no `Arc<RwLock<_>>` over protocol state); [`Handle`] is the
//! cheap-clone, `Send + Sync` way everything else talks to it; [`Event`] is
//! the `broadcast` stream of arc-added/removed/cost-changed notifications.

use std::collections::{HashMap, HashSet};
use std::sync::Arc as StdArc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use ed25519_dalek::{SigningKey, VerifyingKey};
use ntk_common::Cost;
use ntk_netlink::NetlinkError;
use ntk_proto::auth::SequenceGuard;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value as ResponseValue;
use ntk_proto::v1::{Empty, MethodCall, NeighborhoodArcArgs, RemoteError, ResponsePayload};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::arc::{Arc as NeighborArc, ArcState};
use crate::cost_model as cost;
use crate::error::NeighborhoodError;
use crate::interface_state::{InterfaceState, resolve_by_name};
use crate::nic::{IpRouteManager, LocalNic, RttProbe};
use crate::node_id::NodeId;
use crate::stub::NeighborhoodStubFactory;
use crate::timing::NeighborhoodTiming;
use crate::wire::{self, NicRef};

/// One arc-set change consumers can subscribe to via [`Handle::subscribe`]
/// (upstream's `arc_added`/`arc_removing`+`arc_removed`/`arc_changed`
/// signals, `neighborhood.vala:89-99`, collapsed to the three the batch
/// contract asks for). [`Event::ArcAdded`] fires at first successful cost
/// measurement, not at export — see [`ArcState::Established`]'s doc.
#[derive(Debug, Clone)]
pub enum Event {
    /// A new arc became visible (cost known) for the first time.
    ArcAdded(NeighborArc),
    /// A previously-visible arc was torn down.
    ArcRemoved(NeighborArc),
    /// A visible arc's published cost changed (hysteresis gate cleared).
    ArcCostChanged(NeighborArc),
}

/// Configuration to [`Manager::spawn`]. Generic over `K` so callers stay
/// generic over [`crate::interface_state::InterfaceState`] rather than a
/// concrete implementation (`ntk-netlink`'s own convention,
/// `research/notes/06-rust-stack.md` "Trait boundary is load-bearing for
/// simulation coverage") — `K` is `ntk_netlink::RealNetlink` in production,
/// `ntk_netlink::FakeNetlink` in tests.
pub struct NeighborhoodConfig<K> {
    /// This node's discovery id. Use [`NodeId::generate`] unless a test
    /// needs a fixed value.
    pub my_id: NodeId,
    /// Per-node cap on *exported* arcs (`neighborhood.vala:514,549`).
    pub max_arcs: usize,
    /// Interface-state source — see the module doc comment.
    pub kernel: K,
    /// Outbound-call seam (`INeighborhoodStubFactory`).
    pub stub_factory: StdArc<dyn NeighborhoodStubFactory>,
    /// OS network-stack seam (`INeighborhoodIPRouteManager`).
    pub ip_route_manager: StdArc<dyn IpRouteManager>,
    /// Best-effort RTT probe (`INeighborhoodNetworkInterface::measure_rtt`).
    pub rtt_probe: StdArc<dyn RttProbe>,
    /// Injectable wait intervals.
    pub timing: NeighborhoodTiming,
    /// Picks a fresh linklocal address for a newly-monitored NIC —
    /// upstream's caller-supplied `NewLinklocalAddress` delegate
    /// (`api.vala:23`); address-allocation policy is explicitly not this
    /// crate's concern, matching upstream.
    pub new_linklocal_address: Box<dyn FnMut() -> String + Send>,
    /// Signs outbound neighbourhood calls (`here_i_am`/`request_arc`/`can_you_export`/
    /// `remove_arc`/`nop`) with this node's RPC-identity key (`ntk_proto::auth::sign`) when
    /// set. `None` (the default) leaves outbound traffic unsigned — the vanilla-reference
    /// behaviour this crate must stay bit-identical to when auth is not configured at all.
    /// Deliberately a *different* key than ANDNA's own signing key
    /// (`crates/ntkd/src/kernel/config.rs`'s `NtkdConfig::node_key_path` vs `andna_key_path`
    /// doc): this one proves transport identity, not hostname ownership.
    pub signing_key: Option<SigningKey>,
    /// Reject inbound neighbourhood calls that carry no valid, verified `ntk_proto::auth`
    /// block — see [`Manager::authenticate`]'s doc for exactly what this gates. Defaults to
    /// `false` (matching [`crate::NeighborhoodConfig::signing_key`]'s own default), the only
    /// setting interoperable with an unmodified/unauthenticated peer.
    pub require_auth: bool,
}

impl<K: std::fmt::Debug> std::fmt::Debug for NeighborhoodConfig<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeighborhoodConfig")
            .field("my_id", &self.my_id)
            .field("max_arcs", &self.max_arcs)
            .field("kernel", &self.kernel)
            .field("stub_factory", &self.stub_factory)
            .field("ip_route_manager", &self.ip_route_manager)
            .field("rtt_probe", &self.rtt_probe)
            .field("timing", &self.timing)
            .field("new_linklocal_address", &"<closure>")
            .field("signing_key_present", &self.signing_key.is_some())
            .field("require_auth", &self.require_auth)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct NicState {
    mac: String,
    local_address: String,
    radar_cancel: CancellationToken,
}

struct ArcEntry {
    arc: NeighborArc,
    monitor_cancel: CancellationToken,
    /// When this entry was inserted — [`Manager::reap_stale_pending`]'s clock; see
    /// [`PENDING_ARC_TTL`]'s doc. `tokio::time::Instant`, not `std::time::Instant`, so tests
    /// can advance it deterministically under `tokio::time::pause`.
    created_at: tokio::time::Instant,
    /// Consecutive [`MonitorOutcome::NoRtt`] results seen while [`NeighborArc::cost`] is still
    /// `None` — [`Manager::handle_monitor_result`]'s clock for [`NO_RTT_FALLBACK_THRESHOLD`].
    /// Never consulted once `cost` is `Some` (see that method's own doc for why), so it is not
    /// reset on a later `NoRtt` — there is no later `NoRtt` that reads it.
    consecutive_no_rtt: u32,
    /// This arc's pinned sender identity, once a message on it has verified — `None` until
    /// then. Mirrors `ntk_andna::record::HostedRecord::owner_key`'s pinning contract ("a
    /// renewal MUST be signed by this same key", `crates/ntk-andna/src/record.rs:166`): once
    /// set, [`Manager::authenticate`] rejects any later message claiming this arc's identity
    /// under a *different* verified key — closing the impersonation defect a bare "the caller
    /// knows the right `(id, mac, nic_addr)` triple" check can't, since those fields are
    /// peer-supplied and unauthenticated on their own. Never set from an unauthenticated
    /// message: only [`Manager::authenticate`]'s `Some((key, sequence))` branch ever writes
    /// this.
    verified_key: Option<VerifyingKey>,
}

enum Command {
    StartMonitor {
        nic: LocalNic,
        reply: oneshot::Sender<Result<(), NeighborhoodError>>,
    },
    StopMonitor {
        dev: String,
        reply: oneshot::Sender<()>,
    },
    SyncInterfaces {
        reply: oneshot::Sender<Vec<String>>,
    },
    HereIAm {
        received_on_dev: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
        reply: oneshot::Sender<Result<ResponsePayload, RemoteError>>,
    },
    RequestArc {
        received_on_dev: String,
        dest_id: NodeId,
        dest_mac: String,
        dest_nic_addr: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
        reply: oneshot::Sender<Result<ResponsePayload, RemoteError>>,
    },
    CanYouExport {
        caller_id: NodeId,
        caller_mac: String,
        caller_nic_addr: String,
        peer_can_export: bool,
        verified: Option<(VerifyingKey, u64)>,
        reply: oneshot::Sender<Result<ResponsePayload, RemoteError>>,
    },
    RemoveArc {
        received_on_dev: String,
        dest_id: NodeId,
        dest_mac: String,
        dest_nic_addr: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
        reply: oneshot::Sender<Result<ResponsePayload, RemoteError>>,
    },
    Nop {
        caller_id: NodeId,
        caller_mac: String,
        caller_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
        reply: oneshot::Sender<Result<ResponsePayload, RemoteError>>,
    },
    MonitorResult {
        key: String,
        outcome: MonitorOutcome,
    },
    /// Fed back by the spawned task `handle_request_arc` hands the outbound
    /// `can_you_export` call to (see that method's doc for why it can't be
    /// awaited inline in this command loop). Never sent by a `Handle` method.
    RequestArcNegotiated {
        mac: String,
        can_i: bool,
        can_you: bool,
    },
}

enum MonitorOutcome {
    NopFailed,
    NoRtt,
    FirstSample(u64),
    Sample(u64),
}

/// Cheap-clone, `Send + Sync` handle to a running [`Manager`] — the only
/// way to interact with it (per the module surface contract). Cloning
/// shares the same underlying actor.
#[derive(Clone)]
pub struct Handle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    snapshot_rx: watch::Receiver<Vec<NeighborArc>>,
    events_tx: broadcast::Sender<Event>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}

impl Handle {
    /// A live snapshot of the current arc set (upstream's `current_arcs()`,
    /// `neighborhood.vala:297-302`, minus the `available` filter — this
    /// snapshot includes every lifecycle state, not just established ones).
    #[must_use]
    pub fn snapshot(&self) -> watch::Receiver<Vec<NeighborArc>> {
        self.snapshot_rx.clone()
    }

    /// Subscribes to the [`Event`] stream.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events_tx.subscribe()
    }

    /// Starts monitoring `nic` (`start_monitor`, `neighborhood.vala:106-134`).
    ///
    /// # Errors
    /// See [`NeighborhoodError`]: unknown/down interface, already
    /// monitored, a netlink failure, or the actor having stopped.
    pub async fn start_monitor(&self, nic: LocalNic) -> Result<(), NeighborhoodError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StartMonitor { nic, reply })
            .map_err(|_| NeighborhoodError::ActorGone)?;
        rx.await.map_err(|_| NeighborhoodError::ActorGone)?
    }

    /// Stops monitoring `dev`, tearing down every arc on it
    /// (`stop_monitor`, `neighborhood.vala:136-162`).
    ///
    /// # Errors
    /// Returns [`NeighborhoodError::ActorGone`] if the actor has stopped.
    pub async fn stop_monitor(&self, dev: impl Into<String>) -> Result<(), NeighborhoodError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StopMonitor {
                dev: dev.into(),
                reply,
            })
            .map_err(|_| NeighborhoodError::ActorGone)?;
        rx.await.map_err(|_| NeighborhoodError::ActorGone)
    }

    /// Re-queries interface state and stops monitoring any NIC that is no
    /// longer up (or no longer exists); returns the `dev`s stopped. Not run
    /// on an internal timer — call this reactively, e.g. from a netlink
    /// link-change watcher, per this crate's "which local interfaces
    /// participate" responsibility.
    ///
    /// # Errors
    /// Returns [`NeighborhoodError::ActorGone`] if the actor has stopped.
    pub async fn sync_interfaces(&self) -> Result<Vec<String>, NeighborhoodError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SyncInterfaces { reply })
            .map_err(|_| NeighborhoodError::ActorGone)?;
        rx.await.map_err(|_| NeighborhoodError::ActorGone)
    }

    pub(crate) async fn here_i_am(
        &self,
        received_on_dev: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        self.dispatch(|reply| Command::HereIAm {
            received_on_dev,
            sender_id,
            sender_mac,
            sender_nic_addr,
            verified,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_arc(
        &self,
        received_on_dev: String,
        dest_id: NodeId,
        dest_mac: String,
        dest_nic_addr: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        self.dispatch(|reply| Command::RequestArc {
            received_on_dev,
            dest_id,
            dest_mac,
            dest_nic_addr,
            sender_id,
            sender_mac,
            sender_nic_addr,
            verified,
            reply,
        })
        .await
    }

    pub(crate) async fn can_you_export(
        &self,
        caller_id: NodeId,
        caller_mac: String,
        caller_nic_addr: String,
        peer_can_export: bool,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        self.dispatch(|reply| Command::CanYouExport {
            caller_id,
            caller_mac,
            caller_nic_addr,
            peer_can_export,
            verified,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_arc(
        &self,
        received_on_dev: String,
        dest_id: NodeId,
        dest_mac: String,
        dest_nic_addr: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        self.dispatch(|reply| Command::RemoveArc {
            received_on_dev,
            dest_id,
            dest_mac,
            dest_nic_addr,
            sender_id,
            sender_mac,
            sender_nic_addr,
            verified,
            reply,
        })
        .await
    }

    pub(crate) async fn nop(
        &self,
        caller_id: NodeId,
        caller_mac: String,
        caller_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        // Upstream's skeleton body is empty (`neighborhood.vala:619-621`) and
        // caller identity genuinely doesn't matter *there*: `nop` only needs
        // to prove the connection is alive. Over a medium where a live TCP
        // connection can outlive the arc's own agreement on both ends (see
        // `Manager::handle_nop`'s doc), that's not enough — this departs
        // from upstream by making `nop` additionally confirm the callee
        // still has *this caller's* arc, so a one-sided belief self-heals
        // via the existing liveness-probe cadence instead of persisting
        // forever.
        self.dispatch(|reply| Command::Nop {
            caller_id,
            caller_mac,
            caller_nic_addr,
            verified,
            reply,
        })
        .await
    }

    async fn dispatch(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<ResponsePayload, RemoteError>>) -> Command,
    ) -> Result<ResponsePayload, RemoteError> {
        let (reply, rx) = oneshot::channel();
        if self.cmd_tx.send(build(reply)).is_err() {
            return Err(wire::malformed(
                "neighborhood manager actor is no longer running",
            ));
        }
        rx.await.unwrap_or_else(|_| {
            Err(wire::malformed(
                "neighborhood manager actor is no longer running",
            ))
        })
    }
}

/// The actor itself. Never accessed directly outside this module — obtain a
/// [`Handle`] via [`Manager::spawn`].
pub struct Manager<K> {
    my_id: NodeId,
    max_arcs: usize,
    kernel: K,
    stub_factory: StdArc<dyn NeighborhoodStubFactory>,
    ip_route_manager: StdArc<dyn IpRouteManager>,
    rtt_probe: StdArc<dyn RttProbe>,
    timing: NeighborhoodTiming,
    new_linklocal_address: Box<dyn FnMut() -> String + Send>,
    nics: HashMap<String, NicState>,
    disabling: HashSet<String>,
    arcs: HashMap<String, ArcEntry>,
    self_tx: mpsc::UnboundedSender<Command>,
    snapshot_tx: watch::Sender<Vec<NeighborArc>>,
    events_tx: broadcast::Sender<Event>,
    tasks: JoinSet<()>,
    /// This node's outbound signing identity — see [`NeighborhoodConfig::signing_key`]'s doc.
    /// `StdArc`-wrapped so it can be cheaply cloned into the background tasks that also sign
    /// outbound calls independently of the command loop (`RadarContext`/`ArcMonitorContext`).
    signing_key: Option<StdArc<SigningKey>>,
    /// The single monotonic sequence counter every signed outbound call (actor-inline or
    /// background-task) draws its `ntk_proto::v1::Auth::sequence` from — see
    /// [`wire::sign_call`]'s doc for why it must be shared rather than per-call-site. `StdArc`
    /// for the same reason as [`Manager::signing_key`].
    sequence_counter: StdArc<AtomicU64>,
    /// Bounded per-node replay guard for *inbound* auth — see [`Manager::authenticate`].
    sequence_guard: SequenceGuard,
    /// See [`NeighborhoodConfig::require_auth`]'s doc.
    require_auth: bool,
}

impl<K> std::fmt::Debug for Manager<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager").finish_non_exhaustive()
    }
}

/// Multiplier applied to [`NeighborhoodConfig::max_arcs`] to bound the number of
/// *pending* (`Discovered` + `Requested`, i.e. not yet [`ArcState::Established`])
/// arcs [`Manager::handle_here_i_am`] and [`Manager::handle_request_arc`] will ever
/// create. Closes a resource-exhaustion defect in both: an unauthenticated UDP
/// broadcast could synthesise unlimited distinct `(sender_mac, sender_nic_addr,
/// sender_id)` triples, each one an unbounded [`Manager::arcs`] insert, a real
/// kernel neighbour-table row (`IpRouteManager::add_neighbor`), and an outbound
/// dial to an attacker-chosen address — none of it gated by `max_arcs`, which only
/// ever bounds *exported* arcs (see that field's own doc).
///
/// # Upstream has no such bound: documented divergence, not a port
/// `neighborhood.vala:363-433` (`here_i_am`) creates a `NeighborhoodRealArc`, calls
/// `ip_mgr.add_neighbor`, and broadcasts `request_arc` — all unconditionally, no
/// capacity check anywhere on that path. `max_arcs` (`:514`, `:549`) is consulted
/// only inside `request_arc`/`can_you_export`, and only to gate `arc.exported =
/// true` (`exported_arcs.size < max_arcs`) — never to gate creating the
/// not-yet-exported `NeighborhoodRealArc` itself, nor the `add_neighbor` call that
/// comes with it (`:412-420`, `:501-510`). That omission is safe under upstream's
/// implicit trust model: `here_i_am`/`request_arc` are only ever reachable via a
/// broadcast that arrived on a physical NIC
/// (`query_caller_info.is_from_broadcast`), so upstream is trusting that only a
/// genuine physical neighbour can put such a frame on the wire in the first place.
/// This project's own 802.11 tier (`crates/ntkd/tests/wireless.rs`,
/// `mac80211_hwsim`) breaks that assumption: an open-air segment lets any attacker
/// within radio range inject the identical broadcast, no genuine physical adjacency
/// required. This bound is therefore a deliberate divergence from upstream, exactly
/// like `HCoord.pos`'s (`crates/ntk-peerservices/src/wire.rs`'s
/// `forwarder_from_wire`) — upstream has no upper-bound check on that field either,
/// for the analogous reason that its one decode site assumed the value already
/// safe.
///
/// # Why a multiplier of `max_arcs`, not a new config field
/// A dedicated cap would have to be threaded through every [`NeighborhoodConfig`]
/// construction site across the workspace, including ones outside this crate's
/// remit. Scaling the existing, already-configurable `max_arcs` keeps the bound
/// tunable together with the export cap it is paired with, at no new surface.
/// `4` gives real deployments generous headroom: every physically-present
/// neighbour beyond `max_arcs` legitimately stays `Discovered`/`Requested` forever
/// by upstream's own design (`can_i = exported_arcs.size < max_arcs` never
/// promotes them once the export cap is full) — a lower multiplier would misfire
/// on that ordinary case, not just on an attack. At the production default
/// (`crates/ntkd/src/node/transport.rs`'s `MAX_ARCS = 64`) this is 256 pending
/// slots: comfortably above any real single-segment neighbour count, and small
/// enough — a few hundred `HashMap` entries and kernel neighbour rows — to be a
/// non-issue even fully occupied by an attacker.
const PENDING_ARC_MULTIPLIER: usize = 4;

/// How long an [`ArcState::Discovered`] arc may sit unresolved before
/// [`Manager::reap_stale_pending`] reclaims its slot (and the kernel neighbour
/// entry that came with it). Without this, [`PENDING_ARC_MULTIPLIER`]'s cap only
/// *delays* exhaustion: one attacker broadcast able to fill every pending slot
/// once would then hold them forever — neither upstream nor this crate has any
/// other reap mechanism for a non-exported arc ([`Manager::export_arc`]'s
/// monitor/confirmation tasks only start once `Established`; [`Manager::stop_monitor`]
/// and the `remove_arc` RPC are the only other removal paths, neither triggered by
/// mere inactivity) — permanently blocking every subsequent legitimate neighbour
/// from being tracked at all.
///
/// # Only `Discovered`, not `Requested`
/// A `Requested` arc has an outbound `can_you_export` call already in flight
/// (`Manager::handle_request_arc`'s spawned task), bounded by
/// `TcpDialer::call_timeout` (10s in production,
/// `crates/ntkd/src/node/lifecycle.rs`) plus the OS's own bounded TCP connect
/// retry limit — it always resolves back to `Discovered` or forward to
/// `Established` on its own (`Manager::handle_request_arc_negotiated`), so reaping
/// it here would risk racing a call about to succeed. An arc created by
/// [`Manager::handle_here_i_am`] never even reaches `Requested` on its own (that
/// transition happens only on the *receiving* side of an inbound `request_arc`) —
/// a fabricated peer that never replies leaves its arc stuck in `Discovered`
/// specifically, which is exactly what this reaps.
///
/// # A fixed duration, not a multiple of `NeighborhoodTiming::radar_interval`
/// Deliberately not derived from `radar_interval` the way [`PENDING_ARC_MULTIPLIER`]
/// derives from `max_arcs`: negotiation latency has no relationship to how often
/// *this* node re-broadcasts its own `here_i_am`, and this crate's tests exercise
/// `radar_interval` values from 1ms (`crates/ntkd/src/node/negotiation_tests.rs`)
/// to the real 60s default — tying the reap TTL to it would make reaping either
/// fire mid-negotiation in fast tests or never fire in slow ones. Three minutes is
/// comfortably above the 10s production call-timeout (so it never reaps a
/// negotiation that is merely running slow) and short enough that a squatted slot
/// recovers within minutes, not indefinitely. Tests exercise this deterministically
/// via `tokio::time::pause`/`advance`, never a real sleep.
const PENDING_ARC_TTL: Duration = Duration::from_secs(180);

/// How many consecutive [`MonitorOutcome::NoRtt`] results [`Manager::handle_monitor_result`]
/// tolerates, for an arc whose [`crate::Arc::cost`] has *never* been set, before it publishes
/// [`NO_RTT_FALLBACK_COST_US`] anyway so the arc still reaches [`ArcState::Established`]'s
/// outward-visible [`Event::ArcAdded`] instead of staying invisible to `qspn` forever.
///
/// # The regression this closes
/// Before this, `MonitorOutcome::NoRtt => {}` did nothing and [`Event::ArcAdded`] fired only on
/// [`MonitorOutcome::FirstSample`] — so a real [`crate::RttProbe`] that can fail (ICMP filtered,
/// missing `CAP_NET_RAW`/`ping_group_range`) would leave a physically-live arc permanently
/// un-exported to routing, with no error and no log line explaining why. Upstream has the
/// identical gap (`neighborhood.vala:253-259`'s `rtt == -1` branch only logs a `warning` and
/// "maintain[s] the arc" — it never synthesizes a cost), which is a non-issue for upstream only
/// because its own `measure_rtt` implementation is not part of the vendored reference and is
/// assumed to essentially always succeed in practice. This crate's own probe genuinely can fail
/// (no privilege, ICMP filtered), so silently never exporting the arc is not acceptable here —
/// this is a deliberate deviation, not a port.
///
/// # Why a private constant, not a new [`NeighborhoodConfig`]/[`NeighborhoodTiming`] field
/// Same reasoning as [`PENDING_ARC_MULTIPLIER`]'s own doc above: `NeighborhoodTiming` is built via
/// full struct-literal syntax (no `..Default::default()`) at every call site across the
/// workspace, several of them in `ntkd` outside this fix's remit — adding a required field would
/// break every one of them. A named, documented constant is exactly as tunable at its one
/// definition site and adds no such surface.
const NO_RTT_FALLBACK_THRESHOLD: u32 = 3;

/// The fallback [`ntk_common::Cost::Finite`] magnitude (microseconds, matching that type's own
/// unit) [`NO_RTT_FALLBACK_THRESHOLD`] publishes. Deliberately coarse: `1_000_000`us (1s) is on
/// the order of one failed probe's own timeout budget, not a real measured RTT (this crate's
/// production `arc_monitor_interval` default is a 28-30**s** tick, i.e. real link-cost samples
/// are orders of magnitude smaller) — high enough that a fallback-metric arc never quietly
/// outcompetes a link with a genuine measurement, low enough that it is still selected over no
/// route at all.
const NO_RTT_FALLBACK_COST_US: u64 = 1_000_000;

impl<K> Manager<K>
where
    K: InterfaceState + 'static,
{
    /// Spawns the actor task and returns a [`Handle`] to it plus the
    /// `JoinHandle` for its own supervising task (per the concurrency
    /// contract: the caller's `JoinSet` reaps this).
    #[must_use]
    pub fn spawn(
        config: NeighborhoodConfig<K>,
        cancel: CancellationToken,
    ) -> (Handle, tokio::task::JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (snapshot_tx, snapshot_rx) = watch::channel(Vec::new());
        let (events_tx, _) = broadcast::channel(64);
        let manager = Self {
            my_id: config.my_id,
            max_arcs: config.max_arcs,
            kernel: config.kernel,
            stub_factory: config.stub_factory,
            ip_route_manager: config.ip_route_manager,
            rtt_probe: config.rtt_probe,
            timing: config.timing,
            new_linklocal_address: config.new_linklocal_address,
            nics: HashMap::new(),
            disabling: HashSet::new(),
            arcs: HashMap::new(),
            self_tx: cmd_tx.clone(),
            snapshot_tx,
            events_tx: events_tx.clone(),
            tasks: JoinSet::new(),
            signing_key: config.signing_key.map(StdArc::new),
            sequence_counter: StdArc::new(AtomicU64::new(0)),
            sequence_guard: SequenceGuard::new(),
            require_auth: config.require_auth,
        };
        let handle = Handle {
            cmd_tx,
            snapshot_rx,
            events_tx,
        };
        let join = tokio::spawn(manager.run(cmd_rx, cancel));
        (handle, join)
    }

    async fn run(
        mut self,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                Some(result) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "ntk-neighborhood: a spawned task panicked");
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => break,
                    }
                }
            }
        }
        for nic in self.nics.values() {
            nic.radar_cancel.cancel();
        }
        for entry in self.arcs.values() {
            entry.monitor_cancel.cancel();
        }
        while self.tasks.join_next().await.is_some() {}
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::StartMonitor { nic, reply } => {
                let result = self.start_monitor(nic).await;
                let _ = reply.send(result);
            }
            Command::StopMonitor { dev, reply } => {
                self.stop_monitor(&dev).await;
                let _ = reply.send(());
            }
            Command::SyncInterfaces { reply } => {
                let stopped = self.sync_interfaces().await;
                let _ = reply.send(stopped);
            }
            Command::HereIAm {
                received_on_dev,
                sender_id,
                sender_mac,
                sender_nic_addr,
                verified,
                reply,
            } => {
                self.handle_here_i_am(
                    received_on_dev,
                    sender_id,
                    sender_mac,
                    sender_nic_addr,
                    verified,
                )
                .await;
                let _ = reply.send(Ok(wire::empty_response()));
            }
            Command::RequestArc {
                received_on_dev,
                dest_id,
                dest_mac,
                dest_nic_addr,
                sender_id,
                sender_mac,
                sender_nic_addr,
                verified,
                reply,
            } => {
                self.handle_request_arc(
                    received_on_dev,
                    dest_id,
                    dest_mac,
                    dest_nic_addr,
                    sender_id,
                    sender_mac,
                    sender_nic_addr,
                    verified,
                )
                .await;
                let _ = reply.send(Ok(wire::empty_response()));
            }
            Command::CanYouExport {
                caller_id,
                caller_mac,
                caller_nic_addr,
                peer_can_export,
                verified,
                reply,
            } => {
                let result = self.handle_can_you_export(
                    caller_id,
                    caller_mac,
                    caller_nic_addr,
                    peer_can_export,
                    verified,
                );
                let _ = reply.send(result);
            }
            Command::RemoveArc {
                received_on_dev,
                dest_id,
                dest_mac,
                dest_nic_addr,
                sender_id,
                sender_mac,
                sender_nic_addr,
                verified,
                reply,
            } => {
                self.handle_remove_arc(
                    received_on_dev,
                    dest_id,
                    dest_mac,
                    dest_nic_addr,
                    sender_id,
                    sender_mac,
                    sender_nic_addr,
                    verified,
                )
                .await;
                let _ = reply.send(Ok(wire::empty_response()));
            }
            Command::Nop {
                caller_id,
                caller_mac,
                caller_nic_addr,
                verified,
                reply,
            } => {
                let result = self.handle_nop(caller_id, caller_mac, caller_nic_addr, verified);
                let _ = reply.send(result);
            }
            Command::MonitorResult { key, outcome } => {
                self.handle_monitor_result(key, outcome).await
            }
            Command::RequestArcNegotiated {
                mac,
                can_i,
                can_you,
            } => {
                self.handle_request_arc_negotiated(&mac, can_i, can_you);
            }
        }
    }

    /// Stateful half of inbound sender authentication for a message about `mac`'s arc — the
    /// counterpart to [`wire::verify_auth`]'s stateless signature check, which every one of
    /// this crate's 5 inbound handlers already ran before calling this (`crate::handler`).
    ///
    /// - `verified: None` (no `Auth` on the wire, or a peer that has never heard of this
    ///   scheme) is accepted unless [`Manager::require_auth`] is set, *or* `mac` already has a
    ///   pinned key on file: once any peer has proven ownership of an arc identity with a real
    ///   signature, an attacker with no key at all must not be able to silently reclaim that
    ///   same identity just because `require_auth` happens to be off — that would leave the
    ///   pin toothless against the exact impersonation this change exists to close. A `mac`
    ///   that has never been pinned stays fully permissive, matching today's behaviour exactly
    ///   when auth is not configured at all (no peer this node talks to has ever signed
    ///   anything, so no `mac` is ever pinned in the first place).
    /// - `verified: Some((key, sequence))` is replay-checked via [`Manager::sequence_guard`]
    ///   (bounded per-node, not per-arc — see that field's/[`SequenceGuard`]'s own doc for why
    ///   one shared guard is correct here), then pinned against [`ArcEntry::verified_key`]: a
    ///   `mac` with no entry yet, or an entry not yet pinned, accepts `key` as-is (the caller
    ///   is responsible for actually recording it — this only pins an *existing* entry, since
    ///   `here_i_am`/`request_arc`'s fresh-arc path calls this before the entry is inserted);
    ///   a `mac` already pinned to a *different* key is rejected outright — the impersonation
    ///   defect this whole change closes.
    ///
    /// # Errors
    /// A [`RemoteError`] when the message must be rejected: `require_auth` unmet, an
    /// already-pinned arc contacted with no auth at all, a replayed/stale sequence, or a pin
    /// mismatch.
    fn authenticate(
        &mut self,
        mac: &str,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<(), RemoteError> {
        let Some((key, sequence)) = verified else {
            if self.require_auth {
                return Err(wire::malformed(
                    "require_auth is enabled and this call carried no valid Auth",
                ));
            }
            if self.arcs.get(mac).is_some_and(|e| e.verified_key.is_some()) {
                return Err(wire::malformed(
                    "this arc's identity has a pinned signer key; an unauthenticated message cannot act on it",
                ));
            }
            return Ok(());
        };
        self.sequence_guard
            .observe(key, sequence)
            .map_err(|error| wire::malformed(format!("auth replay rejected: {error}")))?;
        match self.arcs.get_mut(mac).map(|entry| &mut entry.verified_key) {
            Some(Some(pinned)) if *pinned != key => Err(wire::malformed(
                "auth: signer key does not match this arc's previously verified key",
            )),
            Some(slot @ None) => {
                *slot = Some(key);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// The four dedup/collision rules gating `here_i_am`/`request_arc`/
    /// `remove_arc` (`neighborhood.vala:397-410`) — see [`NeighborArc`]'s
    /// doc comment for why one map keyed by MAC plus these filters replaces
    /// upstream's six indices.
    fn find_collision(
        &self,
        its_mac: &str,
        its_nic_addr: &str,
        its_id: NodeId,
        my_dev: &str,
    ) -> bool {
        self.arcs.values().any(|e| {
            let a = &e.arc;
            (a.neighbour_mac == its_mac && a.neighbour_id != its_id)
                || (a.neighbour_mac == its_mac && a.neighbour_nic_addr != its_nic_addr)
                || (a.neighbour_mac == its_mac && a.my_dev != my_dev)
                || (a.neighbour_id == its_id && a.my_dev == my_dev && a.neighbour_mac != its_mac)
        })
    }

    fn exported_count(&self) -> usize {
        self.arcs
            .values()
            .filter(|e| e.arc.state == ArcState::Established)
            .count()
    }

    /// Count of arcs not yet [`ArcState::Established`] — [`PENDING_ARC_MULTIPLIER`]'s
    /// gate. See that constant's doc for what this bounds and why.
    fn pending_count(&self) -> usize {
        self.arcs
            .values()
            .filter(|e| e.arc.state != ArcState::Established)
            .count()
    }

    fn pending_arc_cap(&self) -> usize {
        self.max_arcs.saturating_mul(PENDING_ARC_MULTIPLIER)
    }

    /// Reclaims [`ArcState::Discovered`] arcs that exceeded [`PENDING_ARC_TTL`]
    /// unresolved — see that constant's doc for the reasoning (why `Discovered`
    /// only, why a fixed duration). Called before
    /// [`Manager::pending_arc_cap`] is checked so a stale, attacker-squatted slot
    /// can never permanently starve a legitimate peer's turn.
    async fn reap_stale_pending(&mut self) {
        let now = tokio::time::Instant::now();
        let stale: Vec<String> = self
            .arcs
            .iter()
            .filter(|(_, entry)| {
                entry.arc.state == ArcState::Discovered
                    && now.saturating_duration_since(entry.created_at) >= PENDING_ARC_TTL
            })
            .map(|(mac, _)| mac.clone())
            .collect();
        for mac in stale {
            tracing::debug!(
                my_id = ?self.my_id, mac,
                "ntk-neighborhood: reaping a Discovered arc that exceeded PENDING_ARC_TTL unresolved"
            );
            self.remove_my_arc(&mac, false).await;
        }
    }

    fn publish_snapshot(&self) {
        let snapshot: Vec<NeighborArc> = self.arcs.values().map(|e| e.arc.clone()).collect();
        let _ = self.snapshot_tx.send(snapshot);
    }

    async fn start_monitor(&mut self, nic: LocalNic) -> Result<(), NeighborhoodError> {
        if self.nics.contains_key(&nic.dev) {
            return Err(NeighborhoodError::AlreadyMonitored(nic.dev));
        }
        let link = resolve_by_name(&self.kernel, &nic.dev)
            .await
            .map_err(|error| match error {
                NetlinkError::InterfaceNotFound(_) => {
                    NeighborhoodError::UnknownInterface(nic.dev.clone())
                }
                other => NeighborhoodError::Netlink(other),
            })?;
        if !link.is_up {
            return Err(NeighborhoodError::InterfaceDown(nic.dev));
        }

        let local_address = (self.new_linklocal_address)();
        self.ip_route_manager
            .add_address(&nic.dev, &local_address)
            .await?;

        let cancel = CancellationToken::new();
        self.nics.insert(
            nic.dev.clone(),
            NicState {
                mac: nic.mac.clone(),
                local_address: local_address.clone(),
                radar_cancel: cancel.clone(),
            },
        );

        let ctx = RadarContext {
            dev: nic.dev,
            nic_ref: NicRef {
                mac: nic.mac,
                nic_addr: local_address,
            },
            my_id: self.my_id,
            stub_factory: self.stub_factory.clone(),
            timing: self.timing.clone(),
            signing_key: self.signing_key.clone(),
            sequence_counter: self.sequence_counter.clone(),
        };
        self.tasks.spawn(run_radar(ctx, cancel));
        Ok(())
    }

    async fn stop_monitor(&mut self, dev: &str) {
        if !self.nics.contains_key(dev) {
            return;
        }
        self.disabling.insert(dev.to_owned());
        let macs: Vec<String> = self
            .arcs
            .iter()
            .filter(|(_, e)| e.arc.my_dev == dev)
            .map(|(key, _)| key.clone())
            .collect();
        for mac in macs {
            self.remove_my_arc(&mac, true).await;
        }
        if let Some(nic) = self.nics.remove(dev) {
            nic.radar_cancel.cancel();
            if let Err(error) = self
                .ip_route_manager
                .remove_address(dev, &nic.local_address)
                .await
            {
                tracing::warn!(%error, dev, "ntk-neighborhood: remove_address failed");
            }
        }
        self.disabling.remove(dev);
    }

    async fn sync_interfaces(&mut self) -> Vec<String> {
        let links = match self.kernel.list_links().await {
            Ok(links) => links,
            Err(error) => {
                tracing::warn!(%error, "ntk-neighborhood: list_links failed during interface sync");
                return Vec::new();
            }
        };
        let up: HashSet<&str> = links
            .iter()
            .filter(|l| l.is_up)
            .map(|l| l.name.as_str())
            .collect();
        let stale: Vec<String> = self
            .nics
            .keys()
            .filter(|dev| !up.contains(dev.as_str()))
            .cloned()
            .collect();
        for dev in &stale {
            self.stop_monitor(dev).await;
        }
        stale
    }

    /// `here_i_am` skeleton (`neighborhood.vala:363-433`). Bounded by
    /// [`PENDING_ARC_MULTIPLIER`]/[`PENDING_ARC_TTL`] — see those constants' docs for the
    /// resource-exhaustion defect this closes: an unauthenticated broadcast could otherwise
    /// grow [`Manager::arcs`], kernel neighbour state, and outbound `request_arc` dials
    /// without limit.
    async fn handle_here_i_am(
        &mut self,
        received_on_dev: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) {
        if sender_id == self.my_id {
            return;
        }
        let Some(nic) = self.nics.get(&received_on_dev) else {
            return;
        };
        let my_nic_ref = NicRef {
            mac: nic.mac.clone(),
            nic_addr: nic.local_address.clone(),
        };
        if self.disabling.contains(&received_on_dev) {
            return;
        }
        self.reap_stale_pending().await;
        if self.find_collision(&sender_mac, &sender_nic_addr, sender_id, &received_on_dev) {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev,
                "ntk-neighborhood: here_i_am dropped -- find_collision"
            );
            return;
        }
        if self.arcs.contains_key(&sender_mac) {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev,
                "ntk-neighborhood: here_i_am dropped -- arc already known (dedup)"
            );
            return;
        }
        if self.pending_count() >= self.pending_arc_cap() {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev,
                cap = self.pending_arc_cap(),
                "ntk-neighborhood: here_i_am dropped -- pending arc cap reached"
            );
            return;
        }
        if let Err(error) = self.authenticate(&sender_mac, verified) {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev, reason = %error.message,
                "ntk-neighborhood: here_i_am dropped -- auth rejected"
            );
            return;
        }

        tracing::debug!(
            my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev, sender_id = ?sender_id,
            "ntk-neighborhood: here_i_am created a Discovered arc, sending request_arc"
        );
        self.arcs.insert(
            sender_mac.clone(),
            ArcEntry {
                arc: NeighborArc::new(
                    sender_id,
                    sender_mac.clone(),
                    sender_nic_addr.clone(),
                    received_on_dev.clone(),
                ),
                monitor_cancel: CancellationToken::new(),
                created_at: tokio::time::Instant::now(),
                consecutive_no_rtt: 0,
                verified_key: verified.map(|(key, _)| key),
            },
        );
        self.publish_snapshot();

        if let Err(error) = self
            .ip_route_manager
            .add_neighbor(&received_on_dev, &my_nic_ref.nic_addr, &sender_nic_addr)
            .await
        {
            tracing::warn!(%error, "ntk-neighborhood: add_neighbor failed for here_i_am");
        }

        let caller = wire::caller_context(self.my_id, &my_nic_ref);
        let call = MethodCall {
            call: Some(Call::NeighborhoodRequestArc(NeighborhoodArcArgs {
                your_id: Some(sender_id.to_typed_value()),
                your_mac: sender_mac,
                your_nic_addr: sender_nic_addr,
                my_id: Some(self.my_id.to_typed_value()),
                my_mac: my_nic_ref.mac,
                my_nic_addr: my_nic_ref.nic_addr,
            })),
        };
        let auth = wire::sign_call(
            self.signing_key.as_deref(),
            &self.sequence_counter,
            wire::METHOD_REQUEST_ARC,
            &call,
        );
        let broadcaster = self.stub_factory.broadcast(&received_on_dev);
        if let Err(error) = broadcaster
            .notify_authenticated(caller, wire::default_identity_marker(), call, auth)
            .await
        {
            tracing::debug!(%error, "ntk-neighborhood: request_arc broadcast failed");
        }
    }

    /// `request_arc` skeleton (`neighborhood.vala:435-536`). Also reachable via an
    /// unauthenticated broadcast (dest fields peer-supplied) and bounded by
    /// [`PENDING_ARC_MULTIPLIER`]/[`PENDING_ARC_TTL`] the same way
    /// [`Manager::handle_here_i_am`] is — see those constants' docs.
    ///
    /// # Why the outbound `can_you_export` call is spawned, not awaited here
    /// This method runs inside the actor's own command loop (`handle_command`,
    /// `&mut self`) — awaiting a peer's reply here would block that loop, and with
    /// it every *other* peer's command (most importantly a different neighbour's
    /// `here_i_am`), for as long as this peer takes to answer. Confirmed against a
    /// real kernel: a relay discovering two neighbours in close succession stalled
    /// the second one's entire negotiation for the first one's whole dial+call
    /// latency, because this call used to be a plain `.await` right here — the
    /// exact "never await an outbound RPC inside a command loop" anti-pattern
    /// this project's own rules forbid (already fixed once in `ntk-qspn::manager`
    /// for the identical shape, see that module's doc comment). Fixed the same
    /// way `export_arc`'s `run_arc_monitor` already does it: spawn the call onto
    /// `self.tasks`, and feed its outcome back in as [`Command::RequestArcNegotiated`]
    /// — the command loop itself still only ever does local state mutation.
    #[allow(clippy::too_many_arguments)]
    async fn handle_request_arc(
        &mut self,
        received_on_dev: String,
        dest_id: NodeId,
        dest_mac: String,
        dest_nic_addr: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) {
        tracing::debug!(
            my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev, sender_id = ?sender_id,
            "ntk-neighborhood: request_arc received"
        );
        if sender_id == self.my_id {
            return;
        }
        let Some(nic) = self.nics.get(&received_on_dev) else {
            return;
        };
        if self.disabling.contains(&received_on_dev) {
            return;
        }
        let (my_mac, my_addr) = (nic.mac.clone(), nic.local_address.clone());
        if dest_id != self.my_id || dest_mac != my_mac || dest_nic_addr != my_addr {
            return;
        }
        self.reap_stale_pending().await;
        if self.find_collision(&sender_mac, &sender_nic_addr, sender_id, &received_on_dev) {
            return;
        }
        if let Err(error) = self.authenticate(&sender_mac, verified) {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev, reason = %error.message,
                "ntk-neighborhood: request_arc dropped -- auth rejected"
            );
            return;
        }

        let already_exported = self
            .arcs
            .get(&sender_mac)
            .is_some_and(|e| e.arc.state == ArcState::Established);
        if already_exported {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac,
                "ntk-neighborhood: request_arc dropped -- already exported here (dest never told)"
            );
            return;
        }

        if !self.arcs.contains_key(&sender_mac) {
            if self.pending_count() >= self.pending_arc_cap() {
                tracing::debug!(
                    my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev,
                    cap = self.pending_arc_cap(),
                    "ntk-neighborhood: request_arc dropped -- pending arc cap reached"
                );
                return;
            }
            self.arcs.insert(
                sender_mac.clone(),
                ArcEntry {
                    arc: NeighborArc::new(
                        sender_id,
                        sender_mac.clone(),
                        sender_nic_addr.clone(),
                        received_on_dev.clone(),
                    ),
                    monitor_cancel: CancellationToken::new(),
                    created_at: tokio::time::Instant::now(),
                    consecutive_no_rtt: 0,
                    verified_key: verified.map(|(key, _)| key),
                },
            );
            if let Err(error) = self
                .ip_route_manager
                .add_neighbor(&received_on_dev, &my_addr, &sender_nic_addr)
                .await
            {
                tracing::warn!(%error, "ntk-neighborhood: add_neighbor failed for request_arc");
            }
        }
        if let Some(entry) = self.arcs.get_mut(&sender_mac) {
            entry.arc.state = ArcState::Requested;
        }
        self.publish_snapshot();

        let can_i = self.exported_count() < self.max_arcs;
        let Some(arc_snapshot) = self.arcs.get(&sender_mac).map(|e| e.arc.clone()) else {
            return;
        };
        let unicast = self.stub_factory.unicast(&arc_snapshot);
        let caller = wire::caller_context(
            self.my_id,
            &NicRef {
                mac: my_mac,
                nic_addr: my_addr,
            },
        );
        let commands = self.self_tx.clone();
        let signing_key = self.signing_key.clone();
        let sequence_counter = self.sequence_counter.clone();
        tracing::debug!(
            mac = %arc_snapshot.neighbour_mac, can_i,
            "ntk-neighborhood: request_arc dispatching outbound can_you_export"
        );
        self.tasks.spawn(async move {
            let call = MethodCall {
                call: Some(Call::NeighborhoodCanYouExport(can_i)),
            };
            let auth = wire::sign_call(
                signing_key.as_deref(),
                &sequence_counter,
                wire::METHOD_CAN_YOU_EXPORT,
                &call,
            );
            let reply = unicast
                .call_authenticated(caller, wire::default_identity_marker(), call, auth)
                .await;
            let can_you = matches!(
                reply,
                Ok(ResponsePayload {
                    value: Some(ResponseValue::Boolean(true)),
                })
            );
            let _ = commands.send(Command::RequestArcNegotiated {
                mac: sender_mac,
                can_i,
                can_you,
            });
        });
    }

    /// Applies the outcome [`handle_request_arc`]'s spawned `can_you_export` call
    /// reported back — see that method's doc for why the call itself can't
    /// finish inline. `mac`'s entry may have since been removed (e.g. a
    /// concurrent `remove_arc`); `export_arc` and the `get_mut` below already
    /// tolerate that by no-op'ing, matching this code's pre-spawn behavior.
    ///
    /// # Why the downgrade branch also checks current state
    /// [`Manager::export_arc`]'s own doc explains why this callback and
    /// [`Manager::handle_can_you_export`]'s inbound path legitimately race
    /// for the same arc: discovery is symmetric, so both this node's
    /// outbound negotiation *and* the peer's outbound negotiation can be in
    /// flight for one arc at once. The success branch above already
    /// tolerates the *faster* direction winning first (`export_arc`'s own
    /// `if entry.arc.state == ArcState::Established { return; }` guard) —
    /// this branch has to tolerate the same race on the *failing* side: if
    /// the other direction already reached [`ArcState::Established`] by the
    /// time this (slower, losing) negotiation resolves negatively, blindly
    /// downgrading back to [`ArcState::Discovered`] here would silently
    /// break a perfectly healthy arc — its [`run_arc_monitor`] task is
    /// untouched by this method and keeps running regardless, so nothing
    /// else would ever notice the mismatch between the arc's real state and
    /// its published one until [`Manager::handle_nop`] started checking
    /// this same field (a real regression this exact guard fixes, found via
    /// `real_netns_two_daemons_negotiate_a_shared_network` stressed 12+
    /// times after that change).
    fn handle_request_arc_negotiated(&mut self, mac: &str, can_i: bool, can_you: bool) {
        tracing::debug!(
            mac,
            can_i,
            can_you,
            "ntk-neighborhood: outbound can_you_export negotiation resolved"
        );
        if can_i && can_you {
            self.export_arc(mac);
        } else if let Some(entry) = self.arcs.get_mut(mac)
            && entry.arc.state != ArcState::Established
        {
            // Negotiation finished without export: no longer "in flight".
            entry.arc.state = ArcState::Discovered;
            self.publish_snapshot();
        }
    }

    /// `can_you_export` skeleton (`neighborhood.vala:538-558`). Resolves
    /// the arc from `CallerContext` rather than transport introspection —
    /// see `crate::wire`'s module doc comment.
    fn handle_can_you_export(
        &mut self,
        caller_id: NodeId,
        caller_mac: String,
        caller_nic_addr: String,
        peer_can_export: bool,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        let Some(entry) = self.arcs.get(&caller_mac) else {
            return Err(wire::malformed(
                "can_you_export: no known arc for this caller",
            ));
        };
        if entry.arc.neighbour_id != caller_id || entry.arc.neighbour_nic_addr != caller_nic_addr {
            return Err(wire::malformed(
                "can_you_export: caller identity does not match the known arc",
            ));
        }
        self.authenticate(&caller_mac, verified)?;
        let Some(entry) = self.arcs.get(&caller_mac) else {
            return Err(wire::malformed(
                "can_you_export: no known arc for this caller",
            ));
        };
        if entry.arc.state == ArcState::Established {
            return Ok(wire::boolean_response(true));
        }
        let can_i = self.exported_count() < self.max_arcs;
        let will_export = can_i && peer_can_export;
        tracing::debug!(
            mac = %caller_mac, can_i, peer_can_export, will_export,
            "ntk-neighborhood: inbound can_you_export"
        );
        if will_export {
            self.export_arc(&caller_mac);
        }
        Ok(wire::boolean_response(can_i))
    }

    /// `nop` skeleton (`neighborhood.vala:619-621`), made caller-aware —
    /// upstream's own body is empty (proves only that the connection is
    /// alive), which this crate ported faithfully until real 802.11 IBSS
    /// testing exposed the gap it leaves: [`Manager::run_arc_monitor`]'s
    /// periodic `nop` is the *only* liveness check a one-sided arc ever
    /// gets, and a bare "the socket answered" check can't detect that the
    /// callee no longer has this arc at all — only that its process is
    /// still up. Measured against a real kernel (two `ntkd` daemons over
    /// `mac80211_hwsim`): one side's arc-monitor `nop` call failed and tore
    /// its own arc down (correct, matching `remove_my_arc`'s existing
    /// `is_still_usable=false` no-broadcast contract); the peer, whose own
    /// reciprocal `nop` kept succeeding against this crate's old
    /// unconditional-`Ok` handler, never learned and stayed `Established`
    /// for the rest of the run. Neither `here_i_am` (`Manager::find_collision`'s
    /// sibling dedup guard, `arcs.contains_key`) nor `request_arc`
    /// (`already_exported`) ever revisits a peer believed already-exported,
    /// so that belief was permanent. Checking caller identity here the same
    /// way [`Manager::handle_can_you_export`] already does closes the loop:
    /// the still-`Established` side's very next `nop` tick (at most one
    /// [`NeighborhoodTiming::arc_monitor_interval`] later) now fails too,
    /// tears its own arc down via the existing [`MonitorOutcome::NopFailed`]
    /// path, and the ordinary `here_i_am`/`request_arc` handshake can run
    /// again from a clean slate on both sides.
    ///
    /// # Why this also requires [`ArcState::Established`], not just presence
    /// A bare "some entry for this caller exists" check is not enough: this
    /// node's own radar keeps discovering the caller independently of the
    /// monitor above (`Manager::handle_here_i_am` runs on every NIC
    /// forever, regardless of arc state), so a fresh, still-negotiating
    /// [`ArcState::Discovered`]/[`ArcState::Requested`] entry can appear for
    /// the very same caller mid-teardown. Accepting that entry as "the
    /// caller's belief is confirmed" would race the two protocols and mask
    /// exactly the staleness this method exists to detect — confirmed by a
    /// flaky regression run of this method's own pinning test before this
    /// state check was added. [`Arc::cost`] is *not* a safe substitute for
    /// this check even though it seems more churn-resistant: a fresh arc's
    /// very first `nop` probe (this side calling the peer) races the peer's
    /// own not-yet-run first monitor tick, which is the only thing that
    /// ever sets *its* cost — checking cost here would reject that
    /// perfectly healthy, brand-new arc (measured: it broke even this
    /// crate's own `full_handshake_establishes_arc_then_dead_nop_removes_it`
    /// test).
    fn handle_nop(
        &mut self,
        caller_id: NodeId,
        caller_mac: String,
        caller_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) -> Result<ResponsePayload, RemoteError> {
        tracing::debug!(
            my_id = ?self.my_id, caller_mac = %caller_mac, caller_id = ?caller_id,
            "ntk-neighborhood: nop entered"
        );
        let Some(entry) = self.arcs.get(&caller_mac) else {
            tracing::debug!(
                my_id = ?self.my_id, caller_mac = %caller_mac, caller_id = ?caller_id,
                "ntk-neighborhood: nop rejected -- no arc for this caller"
            );
            return Err(wire::malformed("nop: no known arc for this caller"));
        };
        if entry.arc.neighbour_id != caller_id || entry.arc.neighbour_nic_addr != caller_nic_addr {
            tracing::debug!(
                my_id = ?self.my_id, caller_mac = %caller_mac,
                caller_id = ?caller_id, caller_nic_addr = %caller_nic_addr,
                known_id = ?entry.arc.neighbour_id, known_nic_addr = %entry.arc.neighbour_nic_addr,
                "ntk-neighborhood: nop rejected -- caller identity mismatch"
            );
            return Err(wire::malformed(
                "nop: caller identity does not match the known arc",
            ));
        }
        if entry.arc.state != ArcState::Established {
            tracing::debug!(
                my_id = ?self.my_id, caller_mac = %caller_mac, state = ?entry.arc.state,
                "ntk-neighborhood: nop rejected -- arc not established here"
            );
            return Err(wire::malformed(
                "nop: caller's arc is not (yet) established here",
            ));
        }
        self.authenticate(&caller_mac, verified)?;
        tracing::debug!(
            my_id = ?self.my_id, caller_mac = %caller_mac,
            "ntk-neighborhood: nop accepted"
        );
        Ok(wire::empty_response())
    }

    /// `remove_arc` skeleton (`neighborhood.vala:560-617`).
    #[allow(clippy::too_many_arguments)]
    async fn handle_remove_arc(
        &mut self,
        received_on_dev: String,
        dest_id: NodeId,
        dest_mac: String,
        dest_nic_addr: String,
        sender_id: NodeId,
        sender_mac: String,
        sender_nic_addr: String,
        verified: Option<(VerifyingKey, u64)>,
    ) {
        if sender_id == self.my_id {
            return;
        }
        let Some(nic) = self.nics.get(&received_on_dev) else {
            return;
        };
        let (my_mac, my_addr) = (nic.mac.clone(), nic.local_address.clone());
        if dest_id != self.my_id || dest_mac != my_mac || dest_nic_addr != my_addr {
            return;
        }
        if self.find_collision(&sender_mac, &sender_nic_addr, sender_id, &received_on_dev) {
            return;
        }
        let matches_dev = self
            .arcs
            .get(&sender_mac)
            .is_some_and(|e| e.arc.my_dev == received_on_dev);
        if !matches_dev {
            return;
        }
        if let Err(error) = self.authenticate(&sender_mac, verified) {
            tracing::debug!(
                my_id = ?self.my_id, mac = %sender_mac, dev = %received_on_dev, reason = %error.message,
                "ntk-neighborhood: remove_arc dropped -- auth rejected"
            );
            return;
        }
        self.remove_my_arc(&sender_mac, false).await;
    }

    /// `remove_my_arc` (`neighborhood.vala:306-350`).
    async fn remove_my_arc(&mut self, mac: &str, is_still_usable: bool) {
        let Some(entry) = self.arcs.remove(mac) else {
            return;
        };
        tracing::debug!(
            my_id = ?self.my_id,
            mac,
            is_still_usable,
            had_cost = entry.arc.cost.is_some(),
            state = ?entry.arc.state,
            "ntk-neighborhood: removing arc"
        );
        entry.monitor_cancel.cancel();
        let nic = self.nics.get(&entry.arc.my_dev).cloned();

        if let Some(nic) = &nic
            && let Err(error) = self
                .ip_route_manager
                .remove_neighbor(
                    &entry.arc.my_dev,
                    &nic.local_address,
                    &entry.arc.neighbour_nic_addr,
                )
                .await
        {
            tracing::warn!(%error, "ntk-neighborhood: remove_neighbor failed");
        }

        if is_still_usable && let Some(nic) = &nic {
            let caller = wire::caller_context(
                self.my_id,
                &NicRef {
                    mac: nic.mac.clone(),
                    nic_addr: nic.local_address.clone(),
                },
            );
            let call = MethodCall {
                call: Some(Call::NeighborhoodRemoveArc(NeighborhoodArcArgs {
                    your_id: Some(entry.arc.neighbour_id.to_typed_value()),
                    your_mac: entry.arc.neighbour_mac.clone(),
                    your_nic_addr: entry.arc.neighbour_nic_addr.clone(),
                    my_id: Some(self.my_id.to_typed_value()),
                    my_mac: nic.mac.clone(),
                    my_nic_addr: nic.local_address.clone(),
                })),
            };
            let auth = wire::sign_call(
                self.signing_key.as_deref(),
                &self.sequence_counter,
                wire::METHOD_REMOVE_ARC,
                &call,
            );
            let broadcaster = self.stub_factory.broadcast(&entry.arc.my_dev);
            let _ = broadcaster
                .notify_authenticated(caller, wire::default_identity_marker(), call, auth)
                .await;
        }

        let had_been_announced = entry.arc.cost.is_some();
        let mut removed_arc = entry.arc;
        removed_arc.state = ArcState::Removed;
        self.publish_snapshot();
        if had_been_announced {
            let _ = self.events_tx.send(Event::ArcRemoved(removed_arc));
        }
    }

    /// Marks `mac`'s arc [`ArcState::Established`] and starts its [`run_arc_monitor`] and
    /// [`run_arc_confirmation`] tasks.
    ///
    /// # Idempotent: its two callers legitimately race
    /// [`Manager::handle_can_you_export`] (the peer's own outbound negotiation completing,
    /// inbound to us) and [`Manager::handle_request_arc_negotiated`] (this node's own
    /// outbound negotiation completing) can each independently decide to export the very
    /// same neighbour — discovery is symmetric (`handle_request_arc`'s own doc: both sides
    /// broadcast `here_i_am` and both may issue `request_arc`), so both paths legitimately
    /// run for one arc, not just in theory.
    ///
    /// # Regression this fixes: fix #2 (spawning `can_you_export`) reopened this exact race
    /// Before `handle_request_arc`'s own outbound `can_you_export` call moved off the
    /// command loop onto `self.tasks` (see that method's doc), its inline `.await`
    /// accidentally *serialized* the two paths above: awaiting blocked the whole actor for
    /// the call's duration, so the peer's inbound `can_you_export` could never arrive
    /// mid-negotiation. Spawning it correctly fixed the inline-await anti-pattern but
    /// reopened this window — confirmed against a real kernel (two-NIC relay): one
    /// neighbour got exported twice, each call spawning its own [`run_arc_monitor`], each
    /// independently reporting its own first `MonitorOutcome::FirstSample` — i.e. two
    /// [`Event::ArcAdded`]s, and downstream in `ntkd`, two `ArcId`s registered in qspn for
    /// one physical arc (the "peer never usefully answers" / "arc never measures a cost"
    /// symptom class). Upstream's own `request_arc`/`can_you_export` pair has the identical
    /// gap — `can_you_export` guards `if (arc.exported) return true`
    /// (`neighborhood.vala:547`), but `request_arc`'s own post-call export
    /// (`neighborhood.vala:529-535`) does not — so matching upstream would not have caught
    /// this; the guard below applies that same check symmetrically to both paths instead.
    ///
    /// **Invariant:** at most one live [`run_arc_monitor`]/[`run_arc_confirmation`] pair per
    /// arc, for as long as the arc stays [`ArcState::Established`] — the property that, one
    /// layer up, guarantees `ntkd` never registers more than one `ArcId` per neighbour.
    fn export_arc(&mut self, mac: &str) {
        let Some(entry) = self.arcs.get(mac) else {
            return;
        };
        if entry.arc.state == ArcState::Established {
            return;
        }
        let Some(caller_nic) = self.nics.get(&entry.arc.my_dev).map(|nic| NicRef {
            mac: nic.mac.clone(),
            nic_addr: nic.local_address.clone(),
        }) else {
            return;
        };
        let entry = self.arcs.get_mut(mac).expect("checked present above");
        entry.arc.state = ArcState::Established;
        // The guard above makes this a fresh, never-yet-cancelled token in practice; cancel
        // it anyway so this line can never silently leak a live monitor task regardless of
        // how a future caller sequence reaches it.
        entry.monitor_cancel.cancel();
        let cancel = CancellationToken::new();
        entry.monitor_cancel = cancel.clone();
        let ctx = ArcMonitorContext {
            key: mac.to_owned(),
            arc: entry.arc.clone(),
            rtt_probe: self.rtt_probe.clone(),
            stub_factory: self.stub_factory.clone(),
            caller_nic,
            my_id: self.my_id,
            timing: self.timing.clone(),
            commands: self.self_tx.clone(),
            signing_key: self.signing_key.clone(),
            sequence_counter: self.sequence_counter.clone(),
        };
        tracing::debug!(
            my_id = ?self.my_id, mac,
            "ntk-neighborhood: export_arc spawning run_arc_monitor + run_arc_confirmation"
        );
        self.tasks
            .spawn(run_arc_monitor(ctx.clone(), cancel.clone()));
        self.tasks.spawn(run_arc_confirmation(ctx, cancel));
        self.publish_snapshot();
    }

    /// See [`NO_RTT_FALLBACK_THRESHOLD`]'s doc for the `NoRtt` regression this closes.
    async fn handle_monitor_result(&mut self, key: String, outcome: MonitorOutcome) {
        match outcome {
            MonitorOutcome::NopFailed => self.remove_my_arc(&key, false).await,
            MonitorOutcome::NoRtt => {
                // An arc that already has a published cost keeps today's behaviour exactly:
                // nothing moves, nothing is emitted (pinned by
                // `no_rtt_changes_nothing_and_emits_nothing`). Only a never-yet-published arc
                // counts failures toward the fallback.
                if let Some(entry) = self.arcs.get_mut(&key)
                    && entry.arc.cost.is_none()
                {
                    entry.consecutive_no_rtt += 1;
                    if entry.consecutive_no_rtt >= NO_RTT_FALLBACK_THRESHOLD {
                        entry.arc.cost = Some(Cost::Finite(NO_RTT_FALLBACK_COST_US));
                        let snapshot = entry.arc.clone();
                        tracing::warn!(
                            my_id = ?self.my_id, mac = %key, dev = %snapshot.my_dev,
                            attempts = NO_RTT_FALLBACK_THRESHOLD,
                            fallback_cost_us = NO_RTT_FALLBACK_COST_US,
                            "ntk-neighborhood: RTT probe never succeeded for this device; \
                             publishing a fallback cost so the arc still establishes -- routing \
                             is running on a fallback metric here, not a measured one"
                        );
                        self.publish_snapshot();
                        let _ = self.events_tx.send(Event::ArcAdded(snapshot));
                    }
                }
            }
            MonitorOutcome::FirstSample(rtt) => {
                if let Some(entry) = self.arcs.get_mut(&key) {
                    entry.arc.cost = Some(Cost::Finite(rtt));
                    let snapshot = entry.arc.clone();
                    self.publish_snapshot();
                    let _ = self.events_tx.send(Event::ArcAdded(snapshot));
                }
            }
            MonitorOutcome::Sample(smoothed) => {
                if let Some(entry) = self.arcs.get_mut(&key)
                    && let Some(Cost::Finite(published)) = entry.arc.cost
                    && cost::exceeds_hysteresis(published, smoothed)
                {
                    entry.arc.cost = Some(Cost::Finite(smoothed));
                    let snapshot = entry.arc.clone();
                    self.publish_snapshot();
                    let _ = self.events_tx.send(Event::ArcCostChanged(snapshot));
                }
            }
        }
    }
}

struct RadarContext {
    dev: String,
    nic_ref: NicRef,
    my_id: NodeId,
    stub_factory: StdArc<dyn NeighborhoodStubFactory>,
    timing: NeighborhoodTiming,
    signing_key: Option<StdArc<SigningKey>>,
    sequence_counter: StdArc<AtomicU64>,
}

/// `MonitorRunTasklet` (`neighborhood.vala:166-190`): periodically
/// broadcasts `here_i_am` on one NIC.
async fn run_radar(ctx: RadarContext, cancel: CancellationToken) {
    loop {
        let caller = wire::caller_context(ctx.my_id, &ctx.nic_ref);
        let call = MethodCall {
            call: Some(Call::NeighborhoodHereIAm(
                ntk_proto::v1::NeighborhoodHereIAmArgs {
                    my_id: Some(ctx.my_id.to_typed_value()),
                    my_mac: ctx.nic_ref.mac.clone(),
                    my_nic_addr: ctx.nic_ref.nic_addr.clone(),
                },
            )),
        };
        let auth = wire::sign_call(
            ctx.signing_key.as_deref(),
            &ctx.sequence_counter,
            wire::METHOD_HERE_I_AM,
            &call,
        );
        let broadcaster = ctx.stub_factory.broadcast(&ctx.dev);
        if let Err(error) = broadcaster
            .notify_authenticated(caller, wire::default_identity_marker(), call, auth)
            .await
        {
            tracing::debug!(%error, dev = %ctx.dev, "ntk-neighborhood: here_i_am broadcast failed");
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(ctx.timing.radar_interval) => {}
        }
    }
}

#[derive(Clone)]
struct ArcMonitorContext {
    key: String,
    arc: NeighborArc,
    rtt_probe: StdArc<dyn RttProbe>,
    stub_factory: StdArc<dyn NeighborhoodStubFactory>,
    caller_nic: NicRef,
    my_id: NodeId,
    timing: NeighborhoodTiming,
    commands: mpsc::UnboundedSender<Command>,
    signing_key: Option<StdArc<SigningKey>>,
    sequence_counter: StdArc<AtomicU64>,
}

/// `ArcMonitorRunTasklet` (`neighborhood.vala:210-293`): for one exported
/// arc, waits a random interval, measures RTT (best-effort), then probes
/// liveness with `nop` — a *failed* `nop`, not RTT, is the dead-arc
/// detector (`neighborhood.vala:246-251`, `research/notes/01` §4 point 5).
/// `nop` here stays a fire-and-forget [`RpcClient::notify`], matching
/// upstream exactly — see [`run_arc_confirmation`]'s doc for why the
/// caller-aware check ([`Manager::handle_nop`]) deliberately lives in a
/// *separate* task rather than folded into this one's own `nop` tick.
async fn run_arc_monitor(ctx: ArcMonitorContext, cancel: CancellationToken) {
    let mut smoothed: Option<u64> = None;
    loop {
        let wait = ctx.timing.next_arc_monitor_wait();
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(wait) => {}
        }

        let measured = ctx
            .rtt_probe
            .measure_rtt(
                &ctx.arc.my_dev,
                &ctx.caller_nic.nic_addr,
                &ctx.arc.neighbour_nic_addr,
            )
            .await;

        let unicast = ctx.stub_factory.unicast(&ctx.arc);
        let caller = wire::caller_context(ctx.my_id, &ctx.caller_nic);
        let nop_call = MethodCall {
            call: Some(Call::NeighborhoodNop(Empty::VALUE)),
        };
        let auth = wire::sign_call(
            ctx.signing_key.as_deref(),
            &ctx.sequence_counter,
            wire::METHOD_NOP,
            &nop_call,
        );
        let nop_result = unicast
            .notify_authenticated(caller, wire::default_identity_marker(), nop_call, auth)
            .await;
        if let Err(error) = &nop_result {
            tracing::debug!(
                mac = %ctx.key, %error,
                "ntk-neighborhood: arc monitor's nop failed, arc will be torn down"
            );
        }
        if nop_result.is_err() {
            let _ = ctx.commands.send(Command::MonitorResult {
                key: ctx.key.clone(),
                outcome: MonitorOutcome::NopFailed,
            });
            return;
        }

        let Some(rtt) = measured else {
            let _ = ctx.commands.send(Command::MonitorResult {
                key: ctx.key.clone(),
                outcome: MonitorOutcome::NoRtt,
            });
            continue;
        };
        let outcome = match smoothed {
            None => {
                smoothed = Some(rtt);
                MonitorOutcome::FirstSample(rtt)
            }
            Some(prev) => {
                let next = cost::ema_step(prev, rtt);
                smoothed = Some(next);
                MonitorOutcome::Sample(next)
            }
        };
        let _ = ctx.commands.send(Command::MonitorResult {
            key: ctx.key.clone(),
            outcome,
        });
    }
}

/// Independent companion to [`run_arc_monitor`], spawned alongside it (same lifetime, same
/// cancellation) by [`Manager::export_arc`]: on its own cadence, sends `nop` as an
/// [`RpcClient::call`] instead of a `notify`, so [`Manager::handle_nop`]'s caller-aware
/// rejection (see that method's doc for the asymmetric-arc bug it closes) can actually reach
/// back here as an `Err` — a `notify` carries no reply channel at all.
///
/// # Why this isn't just `run_arc_monitor`'s own `nop` tick, upgraded to a `call`
/// Confirmed against a real kernel (`crates/ntkd/tests/multi_node.rs`'s
/// `real_netns_two_daemons_negotiate_a_shared_network`, stressed 12+ times): promoting
/// `run_arc_monitor`'s own `nop` from `notify` to `call` regressed that scenario even with
/// [`Manager::handle_nop`] left unconditionally accepting every caller — i.e. the round-trip
/// wait itself, nothing about what it checks, was the problem. `run_arc_monitor`'s loop cadence
/// (`NeighborhoodTiming::arc_monitor_interval`, sub-millisecond in that test's own injected
/// timing) drives real, timing-sensitive downstream work (cost sampling feeding
/// `Event::ArcCostChanged`, and transitively qspn/hooking convergence); gating its next tick on
/// a full round-trip reply measurably perturbed that. This task's own cadence has no such
/// downstream dependents, so it can afford the round-trip its job actually requires.
async fn run_arc_confirmation(ctx: ArcMonitorContext, cancel: CancellationToken) {
    let mut ticks: u64 = 0;
    loop {
        let wait = ctx.timing.next_arc_monitor_wait();
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(wait) => {}
        }

        ticks += 1;
        let unicast = ctx.stub_factory.unicast(&ctx.arc);
        let caller = wire::caller_context(ctx.my_id, &ctx.caller_nic);
        let nop_call = MethodCall {
            call: Some(Call::NeighborhoodNop(Empty::VALUE)),
        };
        let auth = wire::sign_call(
            ctx.signing_key.as_deref(),
            &ctx.sequence_counter,
            wire::METHOD_NOP,
            &nop_call,
        );
        tracing::debug!(
            my_id = ?ctx.my_id, mac = %ctx.key, ticks,
            "ntk-neighborhood: arc confirmation nop tick firing"
        );
        let result = unicast
            .call_authenticated(caller, wire::default_identity_marker(), nop_call, auth)
            .await;
        match &result {
            Ok(_) => tracing::debug!(
                my_id = ?ctx.my_id, mac = %ctx.key, ticks,
                "ntk-neighborhood: arc confirmation's nop accepted"
            ),
            Err(error) => tracing::debug!(
                my_id = ?ctx.my_id, mac = %ctx.key, ticks, %error, is_remote = error.is_remote(),
                "ntk-neighborhood: arc confirmation's nop was rejected, arc will be torn down"
            ),
        }
        if result.is_err() {
            let _ = ctx.commands.send(Command::MonitorResult {
                key: ctx.key.clone(),
                outcome: MonitorOutcome::NopFailed,
            });
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use ntk_netlink::{FakeNetlink, LinkInfo};
    use ntk_proto::v1::{Auth, CallerContext, TypedValue};
    use ntk_rpc::{FakeRpcClient, FnHandler, RpcClient, RpcError};

    use super::*;
    use crate::handler::NeighborhoodRpcHandler;
    use crate::nic::{FakeIpRouteManager, FixedRttProbe, IpRouteOperation};

    fn fast_timing() -> NeighborhoodTiming {
        NeighborhoodTiming {
            radar_interval: Duration::from_millis(20),
            arc_monitor_interval: (Duration::from_millis(1), Duration::from_millis(2)),
        }
    }

    fn nic_state(mac: &str, addr: &str) -> NicState {
        NicState {
            mac: mac.to_owned(),
            local_address: addr.to_owned(),
            radar_cancel: CancellationToken::new(),
        }
    }

    fn arc_entry(
        neighbour_id: NodeId,
        mac: &str,
        addr: &str,
        dev: &str,
        state: ArcState,
        cost: Option<Cost>,
    ) -> ArcEntry {
        ArcEntry {
            arc: NeighborArc {
                neighbour_id,
                neighbour_mac: mac.to_owned(),
                neighbour_nic_addr: addr.to_owned(),
                my_dev: dev.to_owned(),
                state,
                cost,
            },
            monitor_cancel: CancellationToken::new(),
            created_at: tokio::time::Instant::now(),
            consecutive_no_rtt: 0,
            verified_key: None,
        }
    }

    /// A stub factory with no peer wired up: `broadcast` swallows every
    /// call, `unicast` always answers `can_you_export` with a fixed
    /// boolean. Used for [`Manager`]-internal unit tests that drive one
    /// side of the protocol directly and never need a real peer.
    #[derive(Debug)]
    struct NullStubFactory {
        can_export: bool,
    }

    impl NeighborhoodStubFactory for NullStubFactory {
        fn broadcast(&self, _dev: &str) -> StdArc<dyn RpcClient> {
            StdArc::new(FakeRpcClient::new(StdArc::new(FnHandler(
                |_caller: CallerContext,
                 _uid: TypedValue,
                 _call: MethodCall,
                 _auth: Option<Auth>| async move {
                    Ok::<_, RemoteError>(wire::empty_response())
                },
            ))))
        }

        fn unicast(&self, _arc: &NeighborArc) -> StdArc<dyn RpcClient> {
            let can_export = self.can_export;
            StdArc::new(FakeRpcClient::new(StdArc::new(FnHandler(
                move |_caller: CallerContext,
                      _uid: TypedValue,
                      _call: MethodCall,
                      _auth: Option<Auth>| async move {
                    Ok::<_, RemoteError>(wire::boolean_response(can_export))
                },
            ))))
        }
    }

    /// Records how many times each outbound seam was reached — for asserting the
    /// pending-arc cap gates the outbound dial itself, not just the map insert.
    /// `broadcast` mirrors [`NullStubFactory`]'s always-succeeds `notify`;
    /// `unicast` always answers `can_you_export` with `true`.
    #[derive(Debug, Default)]
    struct CountingStubFactory {
        broadcasts: AtomicUsize,
        unicasts: AtomicUsize,
    }

    impl NeighborhoodStubFactory for CountingStubFactory {
        fn broadcast(&self, _dev: &str) -> StdArc<dyn RpcClient> {
            self.broadcasts.fetch_add(1, Ordering::SeqCst);
            StdArc::new(FakeRpcClient::new(StdArc::new(FnHandler(
                |_caller: CallerContext,
                 _uid: TypedValue,
                 _call: MethodCall,
                 _auth: Option<Auth>| async move {
                    Ok::<_, RemoteError>(wire::empty_response())
                },
            ))))
        }

        fn unicast(&self, _arc: &NeighborArc) -> StdArc<dyn RpcClient> {
            self.unicasts.fetch_add(1, Ordering::SeqCst);
            StdArc::new(FakeRpcClient::new(StdArc::new(FnHandler(
                |_caller: CallerContext,
                 _uid: TypedValue,
                 _call: MethodCall,
                 _auth: Option<Auth>| async move {
                    Ok::<_, RemoteError>(wire::boolean_response(true))
                },
            ))))
        }
    }

    fn test_manager(
        my_id: NodeId,
        can_export: bool,
    ) -> (Manager<FakeNetlink>, mpsc::UnboundedReceiver<Command>) {
        let (self_tx, self_rx) = mpsc::unbounded_channel();
        let (snapshot_tx, _snapshot_rx) = watch::channel(Vec::new());
        let (events_tx, _) = broadcast::channel(64);
        let mgr = Manager {
            my_id,
            max_arcs: 8,
            kernel: FakeNetlink::new(),
            stub_factory: StdArc::new(NullStubFactory { can_export }),
            ip_route_manager: StdArc::new(FakeIpRouteManager::new()),
            rtt_probe: StdArc::new(FixedRttProbe(Some(10))),
            timing: fast_timing(),
            new_linklocal_address: Box::new(|| "10.0.0.1".to_owned()),
            nics: HashMap::new(),
            disabling: HashSet::new(),
            arcs: HashMap::new(),
            self_tx,
            snapshot_tx,
            events_tx,
            tasks: JoinSet::new(),
            signing_key: None,
            sequence_counter: StdArc::new(AtomicU64::new(0)),
            sequence_guard: SequenceGuard::new(),
            require_auth: false,
        };
        (mgr, self_rx)
    }

    /// Drives one self-directed task `mgr` spawned (e.g. `handle_request_arc`'s
    /// negotiation, spawned onto `mgr.tasks` rather than awaited inline — see that
    /// method's doc) to completion and applies the [`Command`] it feeds back through
    /// `self_rx`, via the real dispatch (`handle_command`) — the same two steps
    /// `Manager::run`'s own `tokio::select!` performs, spelled out for tests that
    /// drive `Manager` directly instead of through the actor loop.
    async fn drain_one_task(
        mgr: &mut Manager<FakeNetlink>,
        self_rx: &mut mpsc::UnboundedReceiver<Command>,
    ) {
        mgr.tasks
            .join_next()
            .await
            .expect("a task was spawned")
            .expect("spawned task panicked");
        let cmd = self_rx
            .try_recv()
            .expect("the spawned task should have fed a command back");
        mgr.handle_command(cmd).await;
    }

    // ---- find_collision: the four dedup rules -----------------------------

    #[test]
    fn find_collision_flags_all_four_rules_and_nothing_else() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.arcs.insert(
            "peer-mac".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "peer-mac",
                "10.0.0.9",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );

        // Same MAC, different node id -> collision.
        assert!(mgr.find_collision("peer-mac", "10.0.0.9", NodeId::from_raw(3).unwrap(), "eth0"));
        // Same MAC, different linklocal -> collision.
        assert!(mgr.find_collision(
            "peer-mac",
            "10.0.0.10",
            NodeId::from_raw(2).unwrap(),
            "eth0"
        ));
        // Same MAC, different my_dev -> collision.
        assert!(mgr.find_collision("peer-mac", "10.0.0.9", NodeId::from_raw(2).unwrap(), "eth1"));
        // Same node id + same dev, different MAC -> collision.
        assert!(mgr.find_collision(
            "other-mac",
            "10.0.0.11",
            NodeId::from_raw(2).unwrap(),
            "eth0"
        ));
        // Exact match (the "I already have this arc" case) is not a collision.
        assert!(!mgr.find_collision("peer-mac", "10.0.0.9", NodeId::from_raw(2).unwrap(), "eth0"));
        // Unrelated neighbour -> not a collision.
        assert!(!mgr.find_collision(
            "unrelated-mac",
            "10.0.0.20",
            NodeId::from_raw(9).unwrap(),
            "eth2"
        ));
    }

    // ---- here_i_am: Discovered creation + duplicate suppression -----------

    #[tokio::test]
    async fn here_i_am_creates_discovered_arc() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert_eq!(mgr.arcs.len(), 1);
        let arc = &mgr.arcs["bb:bb"].arc;
        assert_eq!(arc.state, ArcState::Discovered);
        assert_eq!(arc.neighbour_id, peer_id);
        assert!(arc.cost.is_none());
    }

    #[tokio::test]
    async fn here_i_am_duplicate_is_suppressed() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;
        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert_eq!(
            mgr.arcs.len(),
            1,
            "a repeated here_i_am for the same neighbour must not create a second arc"
        );
    }

    #[tokio::test]
    async fn here_i_am_rejects_mac_claimed_by_two_node_ids() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            NodeId::from_raw(2).unwrap(),
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;
        // A different node id claiming the same MAC must be ignored.
        mgr.handle_here_i_am(
            "eth0".to_owned(),
            NodeId::from_raw(3).unwrap(),
            "bb:bb".to_owned(),
            "10.0.0.3".to_owned(),
            None,
        )
        .await;

        assert_eq!(mgr.arcs.len(), 1);
        assert_eq!(
            mgr.arcs["bb:bb"].arc.neighbour_id,
            NodeId::from_raw(2).unwrap()
        );
    }

    #[tokio::test]
    async fn here_i_am_ignores_self() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            my_id,
            "aa:aa".to_owned(),
            "10.0.0.1".to_owned(),
            None,
        )
        .await;

        assert!(mgr.arcs.is_empty());
    }

    // ---- pending-arc cap: the unauthenticated-broadcast resource-exhaustion fix --

    /// Reproduces the defect [`PENDING_ARC_MULTIPLIER`] closes. Confirmed failing before
    /// this change: with the cap check removed, driving `cap + 1` distinct synthetic
    /// `here_i_am` triples through [`Manager::handle_here_i_am`] left `mgr.arcs.len() == 9`,
    /// `ip_route_manager`'s `AddNeighbor` count at 9, and `stub_factory`'s broadcast count at
    /// 9 — one per triple, unbounded. After the fix all three stop at `pending_arc_cap()`
    /// (`max_arcs = 2` -> `8`), and the 9th triple triggers neither a kernel neighbour entry
    /// nor an outbound `request_arc` dial.
    #[tokio::test]
    async fn here_i_am_from_unlimited_distinct_peers_is_bounded_by_pending_arc_cap() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.max_arcs = 2; // pending_arc_cap() == 8
        let stub_factory = StdArc::new(CountingStubFactory::default());
        mgr.stub_factory = stub_factory.clone();
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        mgr.ip_route_manager = ip_route_manager.clone();
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        let cap = mgr.pending_arc_cap();
        assert_eq!(cap, 8);

        for i in 0..(cap as i32 + 1) {
            mgr.handle_here_i_am(
                "eth0".to_owned(),
                NodeId::from_raw(100 + i).unwrap(),
                format!("mac-{i}"),
                format!("10.0.1.{i}"),
                None,
            )
            .await;
        }

        assert_eq!(
            mgr.arcs.len(),
            cap,
            "the (cap+1)th distinct peer must not grow the arc map past the pending cap"
        );
        let add_neighbor_count = ip_route_manager
            .operations()
            .iter()
            .filter(|op| matches!(op, IpRouteOperation::AddNeighbor { .. }))
            .count();
        assert_eq!(
            add_neighbor_count, cap,
            "the (cap+1)th peer must not install a kernel neighbour entry"
        );
        assert_eq!(
            stub_factory.broadcasts.load(Ordering::SeqCst),
            cap,
            "the (cap+1)th peer must not trigger an outbound request_arc broadcast"
        );
    }

    /// [`Manager::handle_request_arc`] creates arcs from the same kind of
    /// unauthenticated broadcast (`dest_id`/`dest_mac`/`dest_nic_addr` are peer-supplied)
    /// and independently calls `add_neighbor` plus spawns an outbound `can_you_export`
    /// dial — this pins that its own creation path is equally bounded, not just
    /// `here_i_am`'s. Confirmed failing before this change the same way: `cap + 1` distinct
    /// triples produced `cap + 1` arcs, `AddNeighbor` calls and unicast dials.
    #[tokio::test]
    async fn request_arc_from_unlimited_distinct_peers_is_also_bounded_by_pending_arc_cap() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.max_arcs = 2; // pending_arc_cap() == 8
        let stub_factory = StdArc::new(CountingStubFactory::default());
        mgr.stub_factory = stub_factory.clone();
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        mgr.ip_route_manager = ip_route_manager.clone();
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        let cap = mgr.pending_arc_cap();
        for i in 0..(cap as i32 + 1) {
            mgr.handle_request_arc(
                "eth0".to_owned(),
                my_id,
                "aa:aa".to_owned(),
                "10.0.0.1".to_owned(),
                NodeId::from_raw(100 + i).unwrap(),
                format!("mac-{i}"),
                format!("10.0.1.{i}"),
                None,
            )
            .await;
        }

        assert_eq!(mgr.arcs.len(), cap);
        let add_neighbor_count = ip_route_manager
            .operations()
            .iter()
            .filter(|op| matches!(op, IpRouteOperation::AddNeighbor { .. }))
            .count();
        assert_eq!(add_neighbor_count, cap);
        assert_eq!(
            stub_factory.unicasts.load(Ordering::SeqCst),
            cap,
            "the (cap+1)th peer must not trigger an outbound can_you_export dial"
        );
    }

    /// A real, already-`Established` neighbour must never be displaced or refused because of
    /// `Discovered` churn from unrelated (possibly hostile) peers — refuse-new, never evict,
    /// so an attacker filling the pending cap can only ever block *new* arcs, not tear down
    /// one that already completed the handshake.
    #[tokio::test]
    async fn established_arc_survives_discovered_churn_at_the_pending_cap() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.max_arcs = 2; // pending_arc_cap() == 8
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let established_peer = NodeId::from_raw(999).unwrap();
        mgr.arcs.insert(
            "established-mac".to_owned(),
            arc_entry(
                established_peer,
                "established-mac",
                "10.0.9.9",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(7)),
            ),
        );

        let cap = mgr.pending_arc_cap();
        // Flood well past the pending cap with distinct fake peers.
        for i in 0..(cap as i32 + 5) {
            mgr.handle_here_i_am(
                "eth0".to_owned(),
                NodeId::from_raw(100 + i).unwrap(),
                format!("mac-{i}"),
                format!("10.0.1.{i}"),
                None,
            )
            .await;
        }

        let established = &mgr.arcs["established-mac"].arc;
        assert_eq!(established.state, ArcState::Established);
        assert_eq!(established.neighbour_id, established_peer);
        assert_eq!(established.cost, Some(Cost::Finite(7)));
        assert_eq!(
            mgr.arcs.len(),
            1 + cap,
            "the established arc plus exactly pending_arc_cap() Discovered arcs -- \
             Established never counts against, or is evicted by, the pending cap"
        );
    }

    /// No regression: a real neighbour must still establish normally when the node is
    /// nowhere near [`Manager::pending_arc_cap`] -- exercised alongside a handful of
    /// unrelated, already-pending arcs to prove the new gate does not false-positive under
    /// ordinary partial load.
    #[tokio::test]
    async fn legitimate_peer_still_establishes_normally_far_below_the_pending_cap() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, mut self_rx) = test_manager(my_id, true);
        mgr.max_arcs = 8; // pending_arc_cap() == 32
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        for i in 0..5i32 {
            mgr.arcs.insert(
                format!("other-{i}"),
                arc_entry(
                    NodeId::from_raw(200 + i).unwrap(),
                    &format!("other-{i}"),
                    &format!("10.0.2.{i}"),
                    "eth0",
                    ArcState::Discovered,
                    None,
                ),
            );
        }
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_request_arc(
            "eth0".to_owned(),
            my_id,
            "aa:aa".to_owned(),
            "10.0.0.1".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Requested);
        drain_one_task(&mut mgr, &mut self_rx).await;

        assert_eq!(
            mgr.arcs["bb:bb"].arc.state,
            ArcState::Established,
            "a real peer must still establish normally when nowhere near the pending cap"
        );
    }

    /// Verifies the [`PENDING_ARC_TTL`] reap this fix adds -- without it, a stale
    /// `Discovered` arc (a peer that broadcasts once and never completes negotiation) would
    /// occupy its pending slot indefinitely, so once an attacker filled the cap once, no
    /// legitimate new peer could ever be tracked again. Uses `tokio::time::pause`/`advance`,
    /// never a real sleep.
    #[tokio::test]
    async fn stale_discovered_arc_is_reaped_after_pending_arc_ttl_freeing_its_slot() {
        tokio::time::pause();
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.max_arcs = 1; // pending_arc_cap() == 4
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        let cap = mgr.pending_arc_cap();
        for i in 0..(cap as i32) {
            mgr.handle_here_i_am(
                "eth0".to_owned(),
                NodeId::from_raw(100 + i).unwrap(),
                format!("mac-{i}"),
                format!("10.0.1.{i}"),
                None,
            )
            .await;
        }
        assert_eq!(
            mgr.arcs.len(),
            cap,
            "the cap is fully occupied by stale peers"
        );

        // A brand-new peer is refused while every occupying slot is still fresh.
        mgr.handle_here_i_am(
            "eth0".to_owned(),
            NodeId::from_raw(999).unwrap(),
            "late-comer".to_owned(),
            "10.0.9.9".to_owned(),
            None,
        )
        .await;
        assert_eq!(mgr.arcs.len(), cap);
        assert!(!mgr.arcs.contains_key("late-comer"));

        // Age every stale Discovered entry past PENDING_ARC_TTL.
        tokio::time::advance(PENDING_ARC_TTL + Duration::from_millis(1)).await;

        // The late-comer's own here_i_am triggers reap_stale_pending, which must reclaim
        // every stale slot before the cap is checked -- so this one is now accepted.
        mgr.handle_here_i_am(
            "eth0".to_owned(),
            NodeId::from_raw(999).unwrap(),
            "late-comer".to_owned(),
            "10.0.9.9".to_owned(),
            None,
        )
        .await;

        assert!(
            mgr.arcs.contains_key("late-comer"),
            "a legitimate peer must be accepted once the stale slots it was competing \
             against have aged past PENDING_ARC_TTL"
        );
        assert_eq!(
            mgr.arcs.len(),
            1,
            "reap_stale_pending must have cleared every expired Discovered arc, leaving \
             only the freshly-accepted late-comer"
        );
    }

    // ---- request_arc / can_you_export: negotiation + illegal transitions --

    #[tokio::test]
    async fn request_arc_negotiation_reaches_established() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, mut self_rx) = test_manager(my_id, true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_request_arc(
            "eth0".to_owned(),
            my_id,
            "aa:aa".to_owned(),
            "10.0.0.1".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert_eq!(
            mgr.arcs["bb:bb"].arc.state,
            ArcState::Requested,
            "negotiation is in flight, not resolved inline (see handle_request_arc's doc)"
        );
        drain_one_task(&mut mgr, &mut self_rx).await;

        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Established);
    }

    /// The regression [`Manager::export_arc`]'s doc pins: discovery is symmetric, so this
    /// node's own outbound negotiation ([`Manager::handle_request_arc_negotiated`]) and the
    /// peer's outbound negotiation arriving inbound ([`Manager::handle_can_you_export`]) can
    /// both resolve for the same neighbour. Exactly one must ever export — one arc, one
    /// monitor/confirmation task pair, one [`Event::ArcAdded`] — never two.
    #[tokio::test]
    async fn symmetric_discovery_race_exports_exactly_once() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, mut self_rx) = test_manager(my_id, true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();
        let mut events = mgr.events_tx.subscribe();

        // This node's own outbound negotiation starts (Requested) and spawns its
        // `can_you_export` call — deliberately left un-drained below, so it is still
        // in-flight when the peer's side of the race resolves first.
        mgr.handle_request_arc(
            "eth0".to_owned(),
            my_id,
            "aa:aa".to_owned(),
            "10.0.0.1".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Requested);
        assert_eq!(
            mgr.tasks.len(),
            1,
            "one in-flight can_you_export negotiation task"
        );

        // The peer's own symmetric outbound negotiation reaches me first and decides to
        // export, independently of my own still-in-flight negotiation above.
        let response = mgr
            .handle_can_you_export(
                peer_id,
                "bb:bb".to_owned(),
                "10.0.0.2".to_owned(),
                true,
                None,
            )
            .expect("known arc, matching identity");
        assert!(matches!(
            response,
            ResponsePayload {
                value: Some(ResponseValue::Boolean(true)),
            }
        ));
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Established);
        assert_eq!(
            mgr.tasks.len(),
            3,
            "the still-in-flight negotiation task plus the monitor and confirmation tasks it \
             just spawned"
        );

        // My own negotiation now completes and reports back -- the exact race from
        // export_arc's doc. It must not export a second time.
        drain_one_task(&mut mgr, &mut self_rx).await;

        assert_eq!(mgr.arcs.len(), 1);
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Established);
        assert_eq!(
            mgr.tasks.len(),
            2,
            "the negotiation task's completion must not spawn a second monitor/confirmation \
             pair for an arc the peer's side of the race already established"
        );

        // Exactly one ArcAdded must ever reach subscribers for this neighbour -- never two.
        // `run_arc_monitor` loops forever (only `cancel`/`NopFailed` end it), so read its
        // first fed-back command straight off the channel rather than joining the task.
        let cmd = self_rx
            .recv()
            .await
            .expect("the monitor task fed back its first sample");
        mgr.handle_command(cmd).await;
        assert!(matches!(
            events.try_recv().expect("ArcAdded must fire once"),
            Event::ArcAdded(_)
        ));
        assert!(
            events.try_recv().is_err(),
            "a second ArcAdded would mean a second monitor task was live for one neighbour"
        );
    }

    /// Companion to [`symmetric_discovery_race_exports_exactly_once`], on the *losing* side of
    /// the same race: this node's own outbound negotiation resolves negatively (its RPC call
    /// failed, so `can_you=false`) *after* the peer's side of the same symmetric race already
    /// exported the arc via the faster, inbound path. Losing must never un-export a healthy
    /// arc — see [`Manager::handle_request_arc_negotiated`]'s own doc for the regression this
    /// pins (found stressing `real_netns_two_daemons_negotiate_a_shared_network` 12+ times
    /// after [`Manager::handle_nop`] started trusting this state).
    #[tokio::test]
    async fn losing_negotiation_does_not_downgrade_an_already_established_arc() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_request_arc(
            "eth0".to_owned(),
            my_id,
            "aa:aa".to_owned(),
            "10.0.0.1".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Requested);

        // The peer's own symmetric outbound negotiation reaches me first and exports,
        // independently of my own still-in-flight negotiation below.
        mgr.handle_can_you_export(
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            true,
            None,
        )
        .expect("known arc, matching identity");
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Established);

        // My own negotiation now resolves negatively — it must not un-export the arc the
        // peer's side of the race already established.
        mgr.handle_request_arc_negotiated("bb:bb", true, false);

        assert_eq!(
            mgr.arcs["bb:bb"].arc.state,
            ArcState::Established,
            "a losing negotiation must never downgrade an arc already established by the \
             other direction of the same symmetric race"
        );
    }

    #[tokio::test]
    async fn request_arc_for_a_different_destination_is_ignored() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        // dest_id does not match this node -> the message is not for us.
        mgr.handle_request_arc(
            "eth0".to_owned(),
            NodeId::from_raw(99).unwrap(),
            "cc:cc".to_owned(),
            "10.0.0.9".to_owned(),
            NodeId::from_raw(2).unwrap(),
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert!(mgr.arcs.is_empty());
    }

    #[tokio::test]
    async fn request_arc_does_not_reopen_an_established_arc() {
        let my_id = NodeId::from_raw(1).unwrap();
        let (mut mgr, _self_rx) = test_manager(my_id, true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                peer_id,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(5)),
            ),
        );

        mgr.handle_request_arc(
            "eth0".to_owned(),
            my_id,
            "aa:aa".to_owned(),
            "10.0.0.1".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        // Re-requesting an already-exported arc is a no-op: an illegal
        // transition back to Requested/Discovered is rejected.
        assert_eq!(mgr.arcs["bb:bb"].arc.state, ArcState::Established);
        assert_eq!(mgr.arcs.len(), 1);
    }

    #[test]
    fn can_you_export_rejects_unknown_caller() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let result = mgr.handle_can_you_export(
            NodeId::from_raw(5).unwrap(),
            "zz:zz".to_owned(),
            "10.0.0.9".to_owned(),
            true,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn can_you_export_rejects_identity_mismatch_for_known_mac() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let real_peer = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                real_peer,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );
        // Same MAC key, but a different claimed node id than the arc
        // records -> rejected, never silently trusted.
        let result = mgr.handle_can_you_export(
            NodeId::from_raw(99).unwrap(),
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            true,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn can_you_export_already_established_short_circuits_true() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let peer_id = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                peer_id,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(1)),
            ),
        );
        let response = mgr
            .handle_can_you_export(
                peer_id,
                "bb:bb".to_owned(),
                "10.0.0.2".to_owned(),
                false,
                None,
            )
            .unwrap();
        assert!(matches!(
            response,
            ResponsePayload {
                value: Some(ResponseValue::Boolean(true)),
            }
        ));
    }

    // ---- remove_my_arc: Removed transition + event gating -----------------

    #[tokio::test]
    async fn remove_my_arc_drops_entry_and_emits_event_only_if_announced() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        mgr.arcs.insert(
            "never-announced".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "never-announced",
                "10.0.0.2",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );
        let mut events = mgr.events_tx.subscribe();
        mgr.remove_my_arc("never-announced", false).await;
        assert!(!mgr.arcs.contains_key("never-announced"));
        assert!(
            events.try_recv().is_err(),
            "an arc that never reached a cost measurement must not emit ArcRemoved"
        );

        mgr.arcs.insert(
            "announced".to_owned(),
            arc_entry(
                NodeId::from_raw(3).unwrap(),
                "announced",
                "10.0.0.3",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(7)),
            ),
        );
        mgr.remove_my_arc("announced", false).await;
        assert!(!mgr.arcs.contains_key("announced"));
        let event = events.try_recv().expect("ArcRemoved expected");
        assert!(matches!(event, Event::ArcRemoved(arc) if arc.state == ArcState::Removed));
    }

    // ---- start_monitor: unknown/down/duplicate refusals are side-effect-free ----

    #[tokio::test]
    async fn start_monitor_rejects_duplicate_device_with_no_side_effects() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "eth0".to_owned(),
            is_up: true,
        }]);
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        mgr.ip_route_manager = ip_route_manager.clone();

        mgr.start_monitor(LocalNic {
            dev: "eth0".to_owned(),
            mac: "aa:aa".to_owned(),
        })
        .await
        .expect("the first start_monitor for a fresh device must succeed");
        assert_eq!(
            mgr.tasks.len(),
            1,
            "the first start_monitor must spawn exactly one radar task"
        );
        assert_eq!(ip_route_manager.operations().len(), 1);

        let result = mgr
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "aa:aa".to_owned(),
            })
            .await;

        match result {
            Err(NeighborhoodError::AlreadyMonitored(dev)) => assert_eq!(dev, "eth0"),
            other => panic!("expected AlreadyMonitored, got {other:?}"),
        }
        assert_eq!(
            mgr.tasks.len(),
            1,
            "a refused duplicate start_monitor must not spawn a second radar task"
        );
        assert_eq!(
            ip_route_manager.operations().len(),
            1,
            "a refused duplicate start_monitor must not add a second link-local address"
        );
    }

    #[tokio::test]
    async fn start_monitor_rejects_unknown_device() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.kernel = FakeNetlink::new(); // no links seeded at all

        let result = mgr
            .start_monitor(LocalNic {
                dev: "eth9".to_owned(),
                mac: "aa:aa".to_owned(),
            })
            .await;

        match result {
            Err(NeighborhoodError::UnknownInterface(dev)) => assert_eq!(dev, "eth9"),
            other => panic!(
                "expected UnknownInterface (mapped from NetlinkError::InterfaceNotFound), got {other:?}"
            ),
        }
        assert!(mgr.nics.is_empty());
    }

    #[tokio::test]
    async fn start_monitor_rejects_down_device_before_touching_ip_route_manager() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "eth0".to_owned(),
            is_up: false,
        }]);
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        mgr.ip_route_manager = ip_route_manager.clone();

        let result = mgr
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "aa:aa".to_owned(),
            })
            .await;

        match result {
            Err(NeighborhoodError::InterfaceDown(dev)) => assert_eq!(dev, "eth0"),
            other => panic!("expected InterfaceDown, got {other:?}"),
        }
        assert!(
            ip_route_manager.operations().is_empty(),
            "a down interface must be rejected before add_address is ever called: {:?}",
            ip_route_manager.operations()
        );
        assert!(
            mgr.tasks.is_empty(),
            "a down interface must never spawn a radar task"
        );
        assert!(mgr.nics.is_empty());
    }

    // ---- stop_monitor: dev-scoped teardown + the mid-stop disabling guard --

    #[tokio::test]
    async fn stop_monitor_removes_only_arcs_on_that_device_and_cancels_its_radar() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        mgr.ip_route_manager = ip_route_manager.clone();

        let eth0_radar = CancellationToken::new();
        mgr.nics.insert(
            "eth0".to_owned(),
            NicState {
                mac: "aa:aa".to_owned(),
                local_address: "10.0.0.1".to_owned(),
                radar_cancel: eth0_radar.clone(),
            },
        );
        let eth1_radar = CancellationToken::new();
        mgr.nics.insert(
            "eth1".to_owned(),
            NicState {
                mac: "cc:cc".to_owned(),
                local_address: "10.0.0.5".to_owned(),
                radar_cancel: eth1_radar.clone(),
            },
        );
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(10)),
            ),
        );
        mgr.arcs.insert(
            "dd:dd".to_owned(),
            arc_entry(
                NodeId::from_raw(3).unwrap(),
                "dd:dd",
                "10.0.0.3",
                "eth1",
                ArcState::Established,
                Some(Cost::Finite(20)),
            ),
        );

        mgr.stop_monitor("eth0").await;

        assert!(!mgr.nics.contains_key("eth0"));
        assert!(
            mgr.nics.contains_key("eth1"),
            "a second monitored NIC must survive stopping the first"
        );
        assert!(
            eth0_radar.is_cancelled(),
            "stop_monitor must cancel the stopped device's radar"
        );
        assert!(
            !eth1_radar.is_cancelled(),
            "stop_monitor must not touch a surviving NIC's radar"
        );

        assert!(
            !mgr.arcs.contains_key("bb:bb"),
            "an arc on the stopped device must be removed"
        );
        assert!(
            mgr.arcs.contains_key("dd:dd"),
            "an arc on a different, still-monitored device must survive"
        );

        let ops = ip_route_manager.operations();
        assert!(
            ops.iter().any(
                |op| matches!(op, IpRouteOperation::RemoveAddress { dev, .. } if dev == "eth0")
            ),
            "the stopped device's address must be removed: {ops:?}"
        );
        assert!(
            !ops.iter().any(
                |op| matches!(op, IpRouteOperation::RemoveAddress { dev, .. } if dev == "eth1")
            ),
            "a surviving device's address must never be touched: {ops:?}"
        );
    }

    #[tokio::test]
    async fn here_i_am_is_dropped_while_its_device_is_mid_stop() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        // Construct the mid-stop state deliberately: the actor processes one command at a
        // time, so `stop_monitor` and an inbound `here_i_am` never genuinely race in
        // production -- but `handle_here_i_am`'s own guard must still honour `disabling`
        // whenever it is set.
        mgr.disabling.insert("eth0".to_owned());

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            NodeId::from_raw(2).unwrap(),
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert!(
            mgr.arcs.is_empty(),
            "a here_i_am for a device mid-stop must be dropped, not create an arc"
        );
    }

    #[tokio::test]
    async fn stop_monitor_clears_its_disabling_entry_so_it_does_not_leak() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));

        mgr.stop_monitor("eth0").await;

        assert!(
            !mgr.disabling.contains("eth0"),
            "stop_monitor must remove its own disabling entry once done, not leak it"
        );

        // A subsequent here_i_am on a freshly re-monitored device is no longer dropped.
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        mgr.handle_here_i_am(
            "eth0".to_owned(),
            NodeId::from_raw(2).unwrap(),
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;
        assert_eq!(
            mgr.arcs.len(),
            1,
            "the disabling guard must not outlive stop_monitor"
        );
    }

    // ---- sync_interfaces: stale-NIC teardown, and the list_links-failure guard --

    #[tokio::test]
    async fn sync_interfaces_stops_exactly_the_monitored_devices_that_are_no_longer_up() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.kernel = FakeNetlink::with_links(vec![
            LinkInfo {
                index: 1,
                name: "eth0".to_owned(),
                is_up: true,
            },
            LinkInfo {
                index: 2,
                name: "eth1".to_owned(),
                is_up: false,
            },
            // eth2 is intentionally absent: gone from the kernel entirely, same as down.
        ]);
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        mgr.ip_route_manager = ip_route_manager.clone();
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        mgr.nics
            .insert("eth1".to_owned(), nic_state("bb:bb", "10.0.0.2"));
        mgr.nics
            .insert("eth2".to_owned(), nic_state("cc:cc", "10.0.0.3"));

        let mut stopped = mgr.sync_interfaces().await;
        stopped.sort();

        assert_eq!(stopped, vec!["eth1".to_owned(), "eth2".to_owned()]);
        assert!(
            mgr.nics.contains_key("eth0"),
            "an interface still up must remain monitored"
        );
        assert!(!mgr.nics.contains_key("eth1"));
        assert!(!mgr.nics.contains_key("eth2"));
    }

    /// The highest-value test in this file: a transient `list_links` failure must never be
    /// read as "every interface disappeared". Uses a dedicated [`InterfaceState`] double
    /// (not [`FakeNetlink`], which has no error-injection seam) so `list_links` fails while
    /// everything else about the manager stays ordinary.
    #[tokio::test]
    async fn sync_interfaces_stops_nothing_when_list_links_fails() {
        #[derive(Debug, Default)]
        struct FailingListLinksKernel;

        impl InterfaceState for FailingListLinksKernel {
            fn list_links(&self) -> BoxFuture<'_, Result<Vec<LinkInfo>, NetlinkError>> {
                Box::pin(async {
                    Err(NetlinkError::Connect(std::io::Error::other(
                        "simulated transient netlink failure",
                    )))
                })
            }
        }

        let (self_tx, _self_rx) = mpsc::unbounded_channel();
        let (snapshot_tx, _snapshot_rx) = watch::channel(Vec::new());
        let (events_tx, _) = broadcast::channel(64);
        let ip_route_manager = StdArc::new(FakeIpRouteManager::new());
        let mut mgr = Manager {
            my_id: NodeId::from_raw(1).unwrap(),
            max_arcs: 8,
            kernel: FailingListLinksKernel,
            stub_factory: StdArc::new(NullStubFactory { can_export: true }),
            ip_route_manager: ip_route_manager.clone(),
            rtt_probe: StdArc::new(FixedRttProbe(Some(10))),
            timing: fast_timing(),
            new_linklocal_address: Box::new(|| "10.0.0.1".to_owned()),
            nics: HashMap::new(),
            disabling: HashSet::new(),
            arcs: HashMap::new(),
            self_tx,
            snapshot_tx,
            events_tx,
            tasks: JoinSet::new(),
            signing_key: None,
            sequence_counter: StdArc::new(AtomicU64::new(0)),
            sequence_guard: SequenceGuard::new(),
            require_auth: false,
        };
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        mgr.nics
            .insert("eth1".to_owned(), nic_state("bb:bb", "10.0.0.2"));

        let stopped = mgr.sync_interfaces().await;

        assert!(
            stopped.is_empty(),
            "a list_links failure must stop nothing, got {stopped:?}"
        );
        assert_eq!(
            mgr.nics.len(),
            2,
            "a list_links failure must leave every monitored NIC in place"
        );
        assert!(
            ip_route_manager.operations().is_empty(),
            "a list_links failure must never touch the IP route manager: {:?}",
            ip_route_manager.operations()
        );
    }

    // ---- handle_monitor_result: the cost-publication path ------------------

    #[tokio::test]
    async fn first_sample_sets_cost_and_publishes_arc_added() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                None,
            ),
        );
        let mut events = mgr.events_tx.subscribe();

        mgr.handle_monitor_result("bb:bb".to_owned(), MonitorOutcome::FirstSample(42))
            .await;

        assert_eq!(mgr.arcs["bb:bb"].arc.cost, Some(Cost::Finite(42)));
        let event = events
            .try_recv()
            .expect("FirstSample must publish an event");
        assert!(matches!(event, Event::ArcAdded(arc) if arc.cost == Some(Cost::Finite(42))));
    }

    /// Pins the *integration*, not the pure hysteresis math -- `cost_model.rs`'s own
    /// `hysteresis_table`/`hysteresis_suppresses_sub_threshold_drift` tests already cover the
    /// boundary of [`cost::exceeds_hysteresis`] itself. This checks that
    /// [`Manager::handle_monitor_result`] actually consults that gate for
    /// [`MonitorOutcome::Sample`] rather than publishing every sample unconditionally.
    #[tokio::test]
    async fn sample_republishes_only_past_the_hysteresis_boundary() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(1000)),
            ),
        );
        let mut events = mgr.events_tx.subscribe();

        // Within [500, 2000]: must not move the published cost or emit anything.
        mgr.handle_monitor_result("bb:bb".to_owned(), MonitorOutcome::Sample(1500))
            .await;
        assert_eq!(
            mgr.arcs["bb:bb"].arc.cost,
            Some(Cost::Finite(1000)),
            "a sample within the hysteresis band must not move the published cost"
        );
        assert!(
            events.try_recv().is_err(),
            "a sample within the hysteresis band must not publish ArcCostChanged"
        );

        // Past 2x the published value: must republish.
        mgr.handle_monitor_result("bb:bb".to_owned(), MonitorOutcome::Sample(2500))
            .await;
        assert_eq!(mgr.arcs["bb:bb"].arc.cost, Some(Cost::Finite(2500)));
        let event = events
            .try_recv()
            .expect("a sample past the hysteresis boundary must publish ArcCostChanged");
        assert!(
            matches!(event, Event::ArcCostChanged(arc) if arc.cost == Some(Cost::Finite(2500)))
        );
    }

    #[tokio::test]
    async fn no_rtt_changes_nothing_and_emits_nothing() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                Some(Cost::Finite(1000)),
            ),
        );
        let mut events = mgr.events_tx.subscribe();

        mgr.handle_monitor_result("bb:bb".to_owned(), MonitorOutcome::NoRtt)
            .await;

        assert_eq!(mgr.arcs["bb:bb"].arc.cost, Some(Cost::Finite(1000)));
        assert!(
            events.try_recv().is_err(),
            "NoRtt must never publish an event"
        );
    }

    #[tokio::test]
    async fn no_rtt_never_exports_the_arc_before_the_fallback_threshold() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                None,
            ),
        );
        let mut events = mgr.events_tx.subscribe();

        for _ in 0..NO_RTT_FALLBACK_THRESHOLD - 1 {
            mgr.handle_monitor_result("bb:bb".to_owned(), MonitorOutcome::NoRtt)
                .await;
        }

        assert_eq!(mgr.arcs["bb:bb"].arc.cost, None);
        assert!(
            events.try_recv().is_err(),
            "a probe that has not yet failed NO_RTT_FALLBACK_THRESHOLD times must not publish"
        );
    }

    /// Pins the regression fix: without a real RTT ever succeeding, the arc must still reach
    /// `Event::ArcAdded` -- otherwise a physically-live neighbour whose probe is filtered/
    /// unprivileged would never be exported to routing at all. See `NO_RTT_FALLBACK_THRESHOLD`'s
    /// own doc for the full defect.
    #[tokio::test]
    async fn no_rtt_publishes_a_fallback_cost_and_arc_added_once_the_probe_never_succeeds() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                NodeId::from_raw(2).unwrap(),
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Established,
                None,
            ),
        );
        let mut events = mgr.events_tx.subscribe();

        for _ in 0..NO_RTT_FALLBACK_THRESHOLD {
            mgr.handle_monitor_result("bb:bb".to_owned(), MonitorOutcome::NoRtt)
                .await;
        }

        assert_eq!(
            mgr.arcs["bb:bb"].arc.cost,
            Some(Cost::Finite(NO_RTT_FALLBACK_COST_US)),
            "an arc whose RTT probe never once succeeds must still get a published cost"
        );
        let event = events
            .try_recv()
            .expect("reaching the fallback threshold must publish ArcAdded");
        assert!(matches!(
            event,
            Event::ArcAdded(arc) if arc.cost == Some(Cost::Finite(NO_RTT_FALLBACK_COST_US))
        ));
    }

    // ---- full actor-level handshake, using FakeRpcClient + FakeNetlink ----

    /// Wraps a [`FakeRpcClient`] so `notify()` truly fire-and-forgets: the
    /// handler dispatch is spawned onto its own task instead of awaited
    /// inline. A real UDP broadcast (`stub::BroadcastRpcClient::notify`)
    /// returns as soon as the local socket write completes, never waiting
    /// on the receiving actor to finish processing; `FakeRpcClient::notify`
    /// awaits the handler in place instead, which is fine for a single
    /// outbound call but deadlocks a bidirectional two-actor simulation:
    /// actor X's broadcast, issued from inside its own command loop
    /// (`Manager::handle_here_i_am`/`handle_request_arc`/`remove_my_arc`),
    /// would otherwise keep that loop busy for the entire round trip of
    /// any reply-driven call the peer makes back to X while handling it.
    #[derive(Debug)]
    struct FireAndForget(FakeRpcClient);

    impl RpcClient for FireAndForget {
        fn call<'a>(
            &'a self,
            caller: CallerContext,
            unicast_id: TypedValue,
            call: MethodCall,
        ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
            self.0.call(caller, unicast_id, call)
        }

        fn notify<'a>(
            &'a self,
            caller: CallerContext,
            unicast_id: TypedValue,
            call: MethodCall,
        ) -> BoxFuture<'a, Result<(), RpcError>> {
            let client = self.0.clone();
            tokio::spawn(async move {
                let _ = client.notify(caller, unicast_id, call).await;
            });
            Box::pin(async { Ok(()) })
        }
    }

    /// Routes every neighborhood call to a fixed peer [`Handle`] — the peer
    /// handle is filled in with [`OnceLock::set`] once both sides of a
    /// simulated 2-node link have been spawned ([`Manager::spawn`] needs the
    /// config before either [`Handle`] exists). `alive` toggles the unicast
    /// channel dead to simulate a failing `nop` liveness probe.
    #[derive(Debug)]
    struct PeerStubFactory {
        peer: StdArc<OnceLock<Handle>>,
        peer_dev: String,
        alive: StdArc<AtomicBool>,
    }

    impl NeighborhoodStubFactory for PeerStubFactory {
        fn broadcast(&self, _dev: &str) -> StdArc<dyn RpcClient> {
            let peer = self.peer.get().expect("peer handle set before use").clone();
            StdArc::new(FireAndForget(FakeRpcClient::new(StdArc::new(
                NeighborhoodRpcHandler::for_broadcast(peer, self.peer_dev.clone()),
            ))))
        }

        fn unicast(&self, _arc: &NeighborArc) -> StdArc<dyn RpcClient> {
            let peer = self.peer.get().expect("peer handle set before use").clone();
            let client = FakeRpcClient::new(StdArc::new(NeighborhoodRpcHandler::for_unicast(peer)));
            if self.alive.load(Ordering::SeqCst) {
                StdArc::new(client)
            } else {
                StdArc::new(client.with_failure(|| RpcError::ConnectionClosed))
            }
        }
    }

    #[tokio::test]
    async fn full_handshake_establishes_arc_then_dead_nop_removes_it() {
        let timing = fast_timing();
        let x_id = NodeId::from_raw(101).unwrap();
        let y_id = NodeId::from_raw(202).unwrap();

        let peer_of_x: StdArc<OnceLock<Handle>> = StdArc::new(OnceLock::new());
        let peer_of_y: StdArc<OnceLock<Handle>> = StdArc::new(OnceLock::new());
        let x_to_y_alive = StdArc::new(AtomicBool::new(true));
        let y_to_x_alive = StdArc::new(AtomicBool::new(true));

        let x_config = NeighborhoodConfig {
            my_id: x_id,
            max_arcs: 8,
            kernel: FakeNetlink::with_links(vec![LinkInfo {
                index: 1,
                name: "eth0".to_owned(),
                is_up: true,
            }]),
            stub_factory: StdArc::new(PeerStubFactory {
                peer: peer_of_x.clone(),
                peer_dev: "eth0".to_owned(),
                alive: x_to_y_alive.clone(),
            }),
            ip_route_manager: StdArc::new(FakeIpRouteManager::new()),
            rtt_probe: StdArc::new(FixedRttProbe(Some(20))),
            timing: timing.clone(),
            new_linklocal_address: Box::new(|| "10.0.0.1".to_owned()),
            signing_key: None,
            require_auth: false,
        };
        let y_config = NeighborhoodConfig {
            my_id: y_id,
            max_arcs: 8,
            kernel: FakeNetlink::with_links(vec![LinkInfo {
                index: 1,
                name: "eth0".to_owned(),
                is_up: true,
            }]),
            stub_factory: StdArc::new(PeerStubFactory {
                peer: peer_of_y.clone(),
                peer_dev: "eth0".to_owned(),
                alive: y_to_x_alive.clone(),
            }),
            ip_route_manager: StdArc::new(FakeIpRouteManager::new()),
            rtt_probe: StdArc::new(FixedRttProbe(Some(15))),
            timing: timing.clone(),
            new_linklocal_address: Box::new(|| "10.0.0.2".to_owned()),
            signing_key: None,
            require_auth: false,
        };

        let cancel = CancellationToken::new();
        let (x_handle, x_join) = Manager::spawn(x_config, cancel.clone());
        let (y_handle, y_join) = Manager::spawn(y_config, cancel.clone());
        peer_of_x.set(y_handle.clone()).expect("set once");
        peer_of_y.set(x_handle.clone()).expect("set once");

        let mut x_events = x_handle.subscribe();

        x_handle
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "aa:aa".to_owned(),
            })
            .await
            .unwrap();
        y_handle
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "bb:bb".to_owned(),
            })
            .await
            .unwrap();

        let added = tokio::time::timeout(Duration::from_secs(2), x_events.recv())
            .await
            .expect("timed out waiting for ArcAdded")
            .expect("events channel closed");
        assert!(matches!(added, Event::ArcAdded(_)));

        let snapshot = x_handle.snapshot().borrow().clone();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].state, ArcState::Established);

        // X can no longer reach Y: the next failed `nop` — not an RTT
        // threshold — must tear the arc down.
        x_to_y_alive.store(false, Ordering::SeqCst);
        let removed = tokio::time::timeout(Duration::from_secs(2), x_events.recv())
            .await
            .expect("timed out waiting for ArcRemoved")
            .expect("events channel closed");
        assert!(matches!(removed, Event::ArcRemoved(_)));

        cancel.cancel();
        let _ = x_join.await;
        let _ = y_join.await;
    }

    /// Regression pin for the asymmetric-arc defect measured over real
    /// 802.11 IBSS (`crates/ntkd/tests/wireless.rs`'s
    /// `real_hwsim_broadcast_reliability_across_ten_runs`): X's own outbound
    /// connection to Y dies and X's `run_arc_monitor` correctly tears X's
    /// side down (`is_still_usable=false`, matching upstream's "a failed
    /// nop immediately removes the arc") — but Y's transport to X is still
    /// healthy, so *before* [`Manager::handle_nop`] became caller-aware, Y's
    /// own `nop` ticks kept getting an unconditional `Ok` back and Y stayed
    /// `Established` forever: no timeout, no retry, nothing ever told Y its
    /// belief was stale. Runs at this crate's real, unshortened
    /// [`NeighborhoodTiming::default`] cadence (~28-30s arc-monitor, 60s
    /// radar) under a paused clock (`tokio::time::pause`), so the assertions
    /// below exercise the actual production interval, not a scaled-down
    /// stand-in, while the test itself still completes in milliseconds of
    /// wall-clock time.
    #[tokio::test]
    async fn a_one_sided_established_arc_self_heals_via_the_next_nop_tick() {
        tokio::time::pause();
        let timing = NeighborhoodTiming::default();
        let x_id = NodeId::from_raw(111).unwrap();
        let y_id = NodeId::from_raw(222).unwrap();

        let peer_of_x: StdArc<OnceLock<Handle>> = StdArc::new(OnceLock::new());
        let peer_of_y: StdArc<OnceLock<Handle>> = StdArc::new(OnceLock::new());
        let x_to_y_alive = StdArc::new(AtomicBool::new(true));
        let y_to_x_alive = StdArc::new(AtomicBool::new(true));

        let x_config = NeighborhoodConfig {
            my_id: x_id,
            max_arcs: 8,
            kernel: FakeNetlink::with_links(vec![LinkInfo {
                index: 1,
                name: "eth0".to_owned(),
                is_up: true,
            }]),
            stub_factory: StdArc::new(PeerStubFactory {
                peer: peer_of_x.clone(),
                peer_dev: "eth0".to_owned(),
                alive: x_to_y_alive.clone(),
            }),
            ip_route_manager: StdArc::new(FakeIpRouteManager::new()),
            rtt_probe: StdArc::new(FixedRttProbe(Some(20))),
            timing: timing.clone(),
            new_linklocal_address: Box::new(|| "10.0.2.1".to_owned()),
            signing_key: None,
            require_auth: false,
        };
        let y_config = NeighborhoodConfig {
            my_id: y_id,
            max_arcs: 8,
            kernel: FakeNetlink::with_links(vec![LinkInfo {
                index: 1,
                name: "eth0".to_owned(),
                is_up: true,
            }]),
            stub_factory: StdArc::new(PeerStubFactory {
                peer: peer_of_y.clone(),
                peer_dev: "eth0".to_owned(),
                alive: y_to_x_alive.clone(),
            }),
            ip_route_manager: StdArc::new(FakeIpRouteManager::new()),
            rtt_probe: StdArc::new(FixedRttProbe(Some(15))),
            timing: timing.clone(),
            new_linklocal_address: Box::new(|| "10.0.2.2".to_owned()),
            signing_key: None,
            require_auth: false,
        };

        let cancel = CancellationToken::new();
        let (x_handle, x_join) = Manager::spawn(x_config, cancel.clone());
        let (y_handle, y_join) = Manager::spawn(y_config, cancel.clone());
        peer_of_x.set(y_handle.clone()).expect("set once");
        peer_of_y.set(x_handle.clone()).expect("set once");

        let mut x_events = x_handle.subscribe();
        let mut y_events = y_handle.subscribe();

        x_handle
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "aa:aa".to_owned(),
            })
            .await
            .unwrap();
        y_handle
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "bb:bb".to_owned(),
            })
            .await
            .unwrap();

        let budget = Duration::from_secs(300);

        let x_added = tokio::time::timeout(budget, x_events.recv())
            .await
            .expect("x: timed out waiting for the initial ArcAdded")
            .expect("events channel closed");
        assert!(matches!(x_added, Event::ArcAdded(_)));
        let y_added = tokio::time::timeout(budget, y_events.recv())
            .await
            .expect("y: timed out waiting for the initial ArcAdded")
            .expect("events channel closed");
        assert!(matches!(y_added, Event::ArcAdded(_)));

        // X's own outbound connection to Y dies. X's next `nop` tick fails at the transport
        // layer and correctly tears X's own side down without notifying Y — matching upstream's
        // "a failed nop immediately removes the arc" and this crate's own `is_still_usable=false`
        // no-broadcast contract.
        x_to_y_alive.store(false, Ordering::SeqCst);
        let x_removed = tokio::time::timeout(budget, x_events.recv())
            .await
            .expect("x: timed out waiting for its own ArcRemoved")
            .expect("events channel closed");
        assert!(matches!(x_removed, Event::ArcRemoved(_)));

        // Y's transport to X was never touched (`y_to_x_alive` stays true) — only X's *belief*
        // changed. Y's own next `nop` tick must still fail, because X no longer recognizes Y as
        // a caller it has an arc for — this is the fix under test, not a transport failure.
        let y_removed = tokio::time::timeout(budget, y_events.recv())
            .await
            .expect("y: a one-sided Established arc persisted indefinitely instead of self-healing")
            .expect("events channel closed");
        assert!(matches!(y_removed, Event::ArcRemoved(_)));

        // Both sides now agree the arc is gone, so the ordinary handshake can retry from a clean
        // slate and reconverge on the very next radar cycle.
        x_to_y_alive.store(true, Ordering::SeqCst);
        let x_readded = tokio::time::timeout(budget, x_events.recv())
            .await
            .expect("x: the handshake never retried after both sides cleared their belief")
            .expect("events channel closed");
        assert!(matches!(x_readded, Event::ArcAdded(_)));

        cancel.cancel();
        let _ = x_join.await;
        let _ = y_join.await;
    }

    /// Pins the daemon-side half of the "one-sided arc" investigation
    /// (`crates/ntkd/tests/wireless.rs`'s real-802.11 commit history): a real trial's trace
    /// signature was `arc confirmation nop tick firing ... nop was rejected ... removing arc
    /// ... error=connection closed is_remote=false` — [`run_arc_confirmation`] itself, exactly
    /// where this test operates, is the layer that observed and reacted to it. Tracing that
    /// investigation to its root cause (not the mechanism this crate first suspected) showed
    /// the daemon side was never the bug: the wireless *fixture* tore one peer's transport down
    /// mid-exchange without a rendezvous, and this call site's response — treat a failed `nop`,
    /// local or remote, as "this arc is dead", tear it down, stop — was already correct
    /// (upstream's own "a failed nop immediately removes the arc" contract, matching
    /// `full_handshake_establishes_arc_then_dead_nop_removes_it` above). The production fix
    /// landed in the fixture's teardown rendezvous, not here. This test proves the daemon-side
    /// conclusion directly, using [`FakeRpcClient::with_failure_at`] to script exactly the
    /// single transient failure a mid-exchange transport death produces, instead of asserting
    /// it from a distance: the arc is torn down promptly on that one failure (not wedged
    /// `Established` forever waiting on a dead link), the confirmation task itself exits rather
    /// than looping or retrying (nothing left parked to ever un-wedge), and the same client
    /// proves the failure was genuinely transient — the very next call over it succeeds.
    #[tokio::test]
    async fn run_arc_confirmation_reports_a_single_transient_local_error_and_then_exits() {
        #[derive(Debug)]
        struct UnicastOnlyStubFactory {
            client: FakeRpcClient,
        }
        impl NeighborhoodStubFactory for UnicastOnlyStubFactory {
            fn broadcast(&self, _dev: &str) -> StdArc<dyn RpcClient> {
                unreachable!("run_arc_confirmation never broadcasts")
            }
            fn unicast(&self, _arc: &NeighborArc) -> StdArc<dyn RpcClient> {
                // Same client handed out every tick: the fault schedule (and its call counter)
                // is scoped to the whole simulated link, matching a real `unicast()` backed by
                // one persistent connection, not reset per call.
                StdArc::new(self.client.clone())
            }
        }

        let client =
            FakeRpcClient::new(StdArc::new(FnHandler(
                |_caller: CallerContext,
                 _uid: TypedValue,
                 _call: MethodCall,
                 _auth: Option<Auth>| async move {
                    Ok::<_, RemoteError>(wire::empty_response())
                },
            )))
            .with_failure_at(1, || RpcError::ConnectionClosed);

        let stub_factory: StdArc<dyn NeighborhoodStubFactory> =
            StdArc::new(UnicastOnlyStubFactory {
                client: client.clone(),
            });
        let arc = NeighborArc {
            neighbour_id: NodeId::from_raw(9).unwrap(),
            neighbour_mac: "cc:cc".to_owned(),
            neighbour_nic_addr: "10.9.9.9".to_owned(),
            my_dev: "eth0".to_owned(),
            state: ArcState::Established,
            cost: Some(Cost::Finite(10)),
        };
        let (commands, mut rx) = mpsc::unbounded_channel();
        let ctx = ArcMonitorContext {
            key: arc.key().to_owned(),
            arc,
            rtt_probe: StdArc::new(FixedRttProbe(Some(10))),
            stub_factory,
            caller_nic: NicRef {
                mac: "aa:aa".to_owned(),
                nic_addr: "10.0.0.1".to_owned(),
            },
            my_id: NodeId::from_raw(1).unwrap(),
            timing: fast_timing(),
            commands,
            signing_key: None,
            sequence_counter: StdArc::new(AtomicU64::new(0)),
        };
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_arc_confirmation(ctx, cancel));

        let reported = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("the transient failure must be reported promptly, not left unwedged forever")
            .expect("commands channel closed");
        match reported {
            Command::MonitorResult { key, outcome } => {
                assert_eq!(key, "cc:cc");
                assert!(
                    matches!(outcome, MonitorOutcome::NopFailed),
                    "a failed nop -- remote or local -- must report NopFailed, tearing the arc \
                     down, not any other outcome"
                );
            }
            _ => panic!("expected exactly one MonitorResult{{NopFailed}}"),
        }

        // Not wedged: the task that just reported the failure must actually finish, not stay
        // parked retrying or waiting on a link that already failed once.
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("run_arc_confirmation must exit after reporting NopFailed, not hang")
            .expect("run_arc_confirmation task panicked");

        // Nothing else was ever reported -- exactly one failure, exactly one exit.
        assert!(
            rx.try_recv().is_err(),
            "run_arc_confirmation must not report anything past its own single exit"
        );

        // The fault was genuinely transient (scoped to call #1 alone, per `with_failure_at`):
        // the same link, asked again, now succeeds -- proving there is nothing left wedged at
        // the transport level either.
        let caller = wire::caller_context(
            NodeId::from_raw(1).unwrap(),
            &NicRef {
                mac: "aa:aa".to_owned(),
                nic_addr: "10.0.0.1".to_owned(),
            },
        );
        let recovered = client
            .call(
                caller,
                wire::default_identity_marker(),
                MethodCall {
                    call: Some(Call::NeighborhoodNop(Empty::VALUE)),
                },
            )
            .await;
        assert!(
            recovered.is_ok(),
            "the injected fault must not outlive its scripted single call: {recovered:?}"
        );
    }

    // ---- regression: a slow peer's negotiation must not block a different peer -----------

    /// A stub factory whose `unicast` calls take `latency` to resolve — models a peer that is
    /// real and will eventually reply, but slowly (e.g. mid connection-retry) — and signals
    /// `unicast_started` the moment `unicast()` is invoked, synchronously, from inside the
    /// actor's own command handling (the same point in both the buggy and fixed code, giving
    /// the test a reliable "the actor has now started negotiating with this peer" signal
    /// independent of whether that negotiation then blocks the actor or not).
    #[derive(Debug)]
    struct SlowUnicastStubFactory {
        latency: Duration,
        unicast_started: StdArc<tokio::sync::Notify>,
    }

    impl NeighborhoodStubFactory for SlowUnicastStubFactory {
        fn broadcast(&self, _dev: &str) -> StdArc<dyn RpcClient> {
            StdArc::new(FakeRpcClient::new(StdArc::new(FnHandler(
                |_caller: CallerContext,
                 _uid: TypedValue,
                 _call: MethodCall,
                 _auth: Option<Auth>| async move {
                    Ok::<_, RemoteError>(wire::empty_response())
                },
            ))))
        }

        fn unicast(&self, _arc: &NeighborArc) -> StdArc<dyn RpcClient> {
            self.unicast_started.notify_one();
            StdArc::new(
                FakeRpcClient::new(StdArc::new(FnHandler(
                    |_caller: CallerContext,
                     _uid: TypedValue,
                     _call: MethodCall,
                     _auth: Option<Auth>| async move {
                        Ok::<_, RemoteError>(wire::boolean_response(true))
                    },
                )))
                .with_latency(self.latency),
            )
        }
    }

    /// Pins the exact regression this module's `handle_request_arc` doc describes: before that
    /// fix, awaiting the outbound `can_you_export` call inline inside the actor's own command
    /// loop meant one peer's slow reply stalled *every other peer's* commands too — including a
    /// different, perfectly responsive peer's `here_i_am`, which never even reached the point of
    /// creating an arc until the slow peer's negotiation finally timed out or resolved.
    ///
    /// Runs under `start_paused = true`: peer A's `can_you_export` reply is 30 (virtual)
    /// seconds away, but peer B's `here_i_am` must still complete in a small fraction of that —
    /// proving B was never queued behind A in the first place, not merely that the test waited
    /// long enough for both.
    #[tokio::test(start_paused = true)]
    async fn a_slow_peers_negotiation_never_delays_a_different_peers_here_i_am() {
        let my_id = NodeId::from_raw(1).unwrap();
        let unicast_started = StdArc::new(tokio::sync::Notify::new());
        let config = NeighborhoodConfig {
            my_id,
            max_arcs: 8,
            kernel: FakeNetlink::with_links(vec![LinkInfo {
                index: 1,
                name: "eth0".to_owned(),
                is_up: true,
            }]),
            stub_factory: StdArc::new(SlowUnicastStubFactory {
                latency: Duration::from_secs(30),
                unicast_started: unicast_started.clone(),
            }),
            ip_route_manager: StdArc::new(FakeIpRouteManager::new()),
            rtt_probe: StdArc::new(FixedRttProbe(Some(10))),
            timing: fast_timing(),
            new_linklocal_address: Box::new(|| "10.0.0.1".to_owned()),
            signing_key: None,
            require_auth: false,
        };
        let cancel = CancellationToken::new();
        let (handle, join) = Manager::spawn(config, cancel.clone());
        handle
            .start_monitor(LocalNic {
                dev: "eth0".to_owned(),
                mac: "aa:aa".to_owned(),
            })
            .await
            .unwrap();

        // Peer A's request_arc kicks off a negotiation whose can_you_export reply is 30s away.
        // Dispatched on its own task and never awaited directly: this test only needs it to
        // *start*, never to finish.
        let handle_for_a = handle.clone();
        let a_task = tokio::spawn(async move {
            handle_for_a
                .request_arc(
                    "eth0".to_owned(),
                    my_id,
                    "aa:aa".to_owned(),
                    "10.0.0.1".to_owned(),
                    NodeId::from_raw(2).unwrap(),
                    "peer-a".to_owned(),
                    "10.0.0.2".to_owned(),
                    None,
                )
                .await
        });

        // Wait until the actor has actually started negotiating with A — the exact point the
        // pre-fix code would then block the whole actor on for the next 30s.
        unicast_started.notified().await;

        let started = tokio::time::Instant::now();
        handle
            .here_i_am(
                "eth0".to_owned(),
                NodeId::from_raw(3).unwrap(),
                "peer-b".to_owned(),
                "10.0.0.3".to_owned(),
                None,
            )
            .await
            .expect("here_i_am from a different, responsive peer");
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "peer B's here_i_am took {elapsed:?} of (paused) time to process — a slow peer A's \
             can_you_export negotiation must never delay a different peer's here_i_am"
        );
        let arcs = handle.snapshot().borrow().clone();
        assert!(
            arcs.iter()
                .any(|arc| arc.neighbour_mac == "peer-b" && arc.state == ArcState::Discovered),
            "peer B's arc should have been created promptly: {arcs:?}"
        );

        cancel.cancel();
        let _ = a_task.await;
        let _ = join.await;
    }

    // ---------------------------------------------------------------------
    // Sender authentication (`Manager::authenticate`, `ArcEntry::verified_key`)
    // ---------------------------------------------------------------------

    fn auth_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[tokio::test]
    async fn here_i_am_with_verified_auth_pins_the_signers_key() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();
        let verifying_key = auth_key(9).verifying_key();

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            Some((verifying_key, 1)),
        )
        .await;

        assert_eq!(mgr.arcs["bb:bb"].verified_key, Some(verifying_key));
    }

    #[test]
    fn a_second_message_signed_by_a_different_key_is_rejected() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let peer_id = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                peer_id,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );
        let key1 = auth_key(1).verifying_key();
        let key2 = auth_key(2).verifying_key();
        assert_ne!(key1, key2);
        mgr.arcs.get_mut("bb:bb").unwrap().verified_key = Some(key1);

        let result = mgr.handle_can_you_export(
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            false,
            Some((key2, 1)),
        );

        assert!(result.is_err(), "a different key must be rejected");
        assert_eq!(
            mgr.arcs["bb:bb"].verified_key,
            Some(key1),
            "the pinned key must not change on a rejected message"
        );
    }

    #[test]
    fn an_unauthenticated_message_is_rejected_once_the_arc_is_pinned() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let peer_id = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                peer_id,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );
        mgr.arcs.get_mut("bb:bb").unwrap().verified_key = Some(auth_key(1).verifying_key());

        // require_auth stays off, but a bare, unsigned message must still be unable to act on
        // an identity a real signature already claimed -- otherwise the pin is toothless
        // against exactly the impersonation this feature exists to close.
        let result = mgr.handle_can_you_export(
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            false,
            None,
        );

        assert!(result.is_err());
    }

    #[test]
    fn a_message_signed_by_the_same_pinned_key_is_accepted() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        let peer_id = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                peer_id,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );
        let key = auth_key(3).verifying_key();
        mgr.arcs.get_mut("bb:bb").unwrap().verified_key = Some(key);

        let result = mgr.handle_can_you_export(
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            false,
            Some((key, 1)),
        );

        assert!(result.is_ok(), "the same pinned key must be accepted");
    }

    #[test]
    fn a_replayed_sequence_is_rejected() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), false);
        let peer_id = NodeId::from_raw(2).unwrap();
        mgr.arcs.insert(
            "bb:bb".to_owned(),
            arc_entry(
                peer_id,
                "bb:bb",
                "10.0.0.2",
                "eth0",
                ArcState::Discovered,
                None,
            ),
        );
        let key = auth_key(4).verifying_key();

        mgr.handle_can_you_export(
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            false,
            Some((key, 5)),
        )
        .expect("the first message at sequence 5 must be accepted");

        let replayed = mgr.handle_can_you_export(
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            false,
            Some((key, 5)),
        );
        assert!(
            replayed.is_err(),
            "a verbatim replay of an already-seen sequence must be rejected"
        );
    }

    #[tokio::test]
    async fn require_auth_rejects_here_i_am_with_no_valid_auth() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.require_auth = true;
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert!(
            mgr.arcs.is_empty(),
            "require_auth must refuse to establish an arc with no valid Auth"
        );
    }

    #[tokio::test]
    async fn require_auth_admits_here_i_am_with_valid_auth() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        mgr.require_auth = true;
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();
        let verifying_key = auth_key(6).verifying_key();

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            Some((verifying_key, 1)),
        )
        .await;

        assert_eq!(
            mgr.arcs.len(),
            1,
            "a validly-authenticated peer must still establish an arc"
        );
        assert_eq!(mgr.arcs["bb:bb"].verified_key, Some(verifying_key));
    }

    /// Pins the hard constraint this whole feature must never violate: with no signing key
    /// configured and `require_auth` at its default (`false`), a today-shaped handshake
    /// behaves exactly as it did before this feature existed — no key is ever pinned. Paired
    /// with `wire::tests::sign_call_with_no_signing_key_produces_no_auth_...`, which pins the
    /// outbound half: no `Auth` block is ever attached to any call either.
    #[tokio::test]
    async fn auth_disabled_by_default_handshake_pins_no_key() {
        let (mut mgr, _self_rx) = test_manager(NodeId::from_raw(1).unwrap(), true);
        assert!(mgr.signing_key.is_none());
        assert!(!mgr.require_auth);
        mgr.nics
            .insert("eth0".to_owned(), nic_state("aa:aa", "10.0.0.1"));
        let peer_id = NodeId::from_raw(2).unwrap();

        mgr.handle_here_i_am(
            "eth0".to_owned(),
            peer_id,
            "bb:bb".to_owned(),
            "10.0.0.2".to_owned(),
            None,
        )
        .await;

        assert_eq!(mgr.arcs.len(), 1);
        assert_eq!(
            mgr.arcs["bb:bb"].verified_key, None,
            "an unauthenticated handshake must never pin a key"
        );
    }
}
