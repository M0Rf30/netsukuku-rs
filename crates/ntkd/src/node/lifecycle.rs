//! Startup sequence and steady-state event loop for one Netsukuku node: first identity,
//! hooking, qspn bootstrap, peerservices/coordinator/andna participation, route installation —
//! then the loop upstream's own `startup.vala` never wrote (`// TODO continue`,
//! `research/notes/02-vala-services-daemon.md` §5): reacting to arc up/down/cost-change, qspn
//! route-snapshot changes, and hooking's migration notifications for as long as the daemon runs.
//!
//! Neighborhood discovery and its transport (UDP broadcasters, and this identity's own TCP
//! listener/dispatcher) are the caller's responsibility (`crate::node::transport` for
//! production, the test harness for `tests/multi_node.rs`) — `ntk_neighborhood::Manager<K>`'s
//! generic bound is sealed to `RealNetlink`/`FakeNetlink` (`ntk_neighborhood::interface_state`
//! is a private module), so it can only ever be spawned at a call site where the concrete
//! backend is statically known, never from generic code here.
//!
//! # Negotiated re-address (create-net always, then merge)
//! Every identity bootstraps [`ntk_hooking::HookingOrigin::CreateNet`] — its own trivial
//! network-of-one, at its own random `network_id`, at its own position: an explicit
//! [`NodeInputs::initial_position`] (multi-node test harnesses only) is authoritative and never
//! renegotiated; production (`initial_position` `None`, `SteadyStateCtx::negotiated`) instead
//! derives one via `derive_initial_position`, salted with this identity's own stable
//! [`ntk_neighborhood::NodeId`] — the same reasoning as every other cross-node-unique value in
//! this daemon (`linklocal_allocator`/`synthetic_mac`'s docs): two independently started
//! daemons get different `my_id`s and so, overwhelmingly likely, distinct starting positions.
//! Two nodes nonetheless colliding (or simply meeting, at any distinct positions) is exactly
//! what `ntk-hooking`'s own per-arc merge protocol (wired below, `on_neighborhood_event`'s
//! `ArcAdded` arm calling [`ntk_hooking::HookingHandle::add_arc`]) exists to resolve.
//!
//! **Why not `HookingOrigin::Joining`.** An earlier version of this bootstrap started production
//! identities `Joining` (unhooked) at the all-zero address, on the theory that `mark_entered`'s
//! snapshot update — hooking's own record of "I resolved a real entry" — would then fire for
//! real. Two problems, confirmed by direct reproduction: (1) `HookingOrigin::CreateNet`'s
//! `mark_entered` guard (`if !self.snapshot.hooked`) is intentional and correct — a `CreateNet`
//! identity's `chosen`/`hooked` snapshot fields are permanent from construction, matching
//! upstream's own "`create_net` is always immediately bootstrap-complete" (`qspn.vala:206-219`);
//! a losing `create_net` identity's *real* re-addressing path is the identity-migration
//! machinery this daemon already documents as out of scope below, not `mark_entered`. So
//! `Joining` was never actually reachable in production without that migration machinery
//! anyway. (2) Two `Joining` identities both starting at the *same* all-zero address broke
//! `ntk-qspn`'s own ETP exchange outright (`"ETP claims to originate from my own address"`) the
//! moment they discovered each other — a self-inflicted address collision, not a library defect
//! (confirmed once every identity instead gets its own `derive_initial_position`-derived,
//! near-certainly-distinct address: real per-arc merge negotiation completes end to end).
//!
//! **The real trigger: `HookingEvent::DoFinishEnter`.** `ntk_hooking::arc::run_arc_handler`'s
//! own arc-handler task calls `ctx.coord.finish_enter(...)` immediately before `mark_entered`
//! when *this* identity's own merge negotiation resolves it as the guest that must enter a
//! bigger network (`arc_handler.vala:336-357`, "propagate prepare_enter/finish_enter to my own
//! current g-node") — Coordinator propagates that back to this same process
//! (`crate::node::adapters::PropagationHandlerAdapter`), surfacing as
//! [`HookingEvent::DoFinishEnter`] on hooking's *event* stream (unconditional — unlike
//! `mark_entered`'s snapshot update, nothing gates this on `hooked`). `rehook` reacts to it:
//! `entry_data.pos` covers levels `[guest_gnode_level, topology.levels())` only — the levels
//! the negotiation actually resolved (`ntk_hooking::ChosenAddress`'s own doc). At
//! `guest_gnode_level == 0` (the single-previously-unhooked-node case this daemon's own
//! bootstrap always produces) that happens to be the whole address, since a level-0 g-node has
//! exactly one member and nothing below it to retain. `rehook` combines it with this
//! identity's own currently-held positions at levels below `guest_gnode_level` (see "Coordinated
//! multi-member migration" below) before discarding the old position and rebuilding every OTHER
//! generation-scoped actor (qspn, peerservices/coordinator/andna, the installed kernel routes)
//! directly at the combined one, then re-attaches every already-known neighborhood arc to the
//! fresh qspn. Hooking itself is *not* rebuilt — it already resolved this identity's entry
//! correctly on its own, and rebuilding it would only ever restart a merge negotiation that
//! already succeeded (`crate::node::services::HookingProvenance`'s doc). `ntk_identities::Handle`
//! is not rebuilt either — [`ntk_identities::Handle::set_naddr`] updates it in place, its own
//! documented seam for exactly this ("the daemon set it once hooking resolves a position").
//!
//! `rehook` runs any number of times over the process's life — a member's own g-node forms
//! once, then may merge into successively bigger networks, each merge driving another
//! `DoFinishEnter`/`rehook` cycle for every member ("Coordinated multi-member migration" below).
//! There is accordingly no one-shot latch: `SteadyStateCtx::migration_in_progress` only ever
//! blocks a second `DoFinishEnter` from starting while an earlier one's synchronous
//! teardown/rebuild is still in flight — never a legitimate repeat migration once that work has
//! completed — and `rehook` separately drops a `DoFinishEnter` naming the position this
//! identity already holds (a stale re-delivery of an already-applied propagation), so the
//! invariant is: **at most one migration in flight per identity, serialized; a completed
//! migration is idempotent against its own stale replay.** This still applies only to a
//! negotiated identity (`SteadyStateCtx::negotiated`) — an explicit test position
//! (`tests/multi_node.rs`'s `spawn_real_node`/`spawn_node`) is authoritative and therefore never
//! a candidate, exactly as already documented on [`NodeInputs::initial_position`].
//!
//! **`NodeInputs::preformed`: pre-formed, not frozen.** A multi-node test harness can also give
//! a node [`NodeInputs::preformed`] — a real position *and* the `network_id` its co-members
//! already share, standing in for several nodes whose g-node a real Coordinator already formed
//! before this process existed. Unlike `initial_position`, `preformed` leaves `negotiated`
//! `true`: `negotiated` is computed from `initial_position.is_none()` alone, so a `preformed`
//! identity — like the production default — can still `migrate` into a bigger network later.
//! The two are mutually exclusive; [`run`] rejects both being `Some`. An earlier attempt tried
//! to isolate a multi-member merge with `initial_position` alone, at a shared level-1 coordinate
//! across several nodes; it failed even though every node visibly shared that coordinate to
//! `ntk-qspn`, because `network_id` was still `random_i64` per node (invisible to
//! `merge_direction`/`merge_tiebreak`, which read only `network_id` and `n_nodes`, never
//! positions) *and* `initial_position: Some(_)` freezes `negotiated` regardless —
//! `crates/ntkd/tests/mesh.rs`'s own
//! `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` doc keeps the full account of
//! what that produced across three real-kernel runs before `preformed` existed to fix it.
//!
//! # Coordinated multi-member migration: one destination, not one negotiation each
//! A g-node with more than one member (`guest_gnode_level >= 1`) must move as a unit — the same
//! shared new position at and above `guest_gnode_level`, for every member — not as however many
//! members happen to independently notice a foreign neighbor and negotiate their own entry.
//! Upstream's own model has the g-node's Coordinator decide once and *propagate the target*, not
//! merely the fact that something happened (`research/impl/vala/hooking/arc_handler.vala:349-357`,
//! `research/impl/vala/hooking/propagation_coord.vala:74-86`); `ntk-coordinator`'s propagation
//! wire shape already carries the full negotiated `EntryData` end to end
//! (`ntk_hooking::FinishEnterData::entry_data`, `crate::node::adapters::CoordinatorClientAdapter`,
//! `crate::node::codec::encode_finish_enter_data`) — the only piece that was missing was *using*
//! it as such at the receiving end, which `on_hooking_event`'s `DoFinishEnter` arm now does:
//! every member, negotiator and silent sibling alike, combines the identical propagated upper
//! levels with its own distinct, unaffected lower levels and calls `rehook` — so a g-node's
//! members always end up sharing the new upper position while keeping their own separate
//! identity below it, regardless of which one of them actually ran the negotiation.
//!
//! **Gated to `guest_gnode_level >= 1` implicitly, not by a level check.** A level-0 g-node has
//! exactly one member, so `entry_data.pos` already spans the whole address there and the "combine
//! with retained lower levels" step degenerates to concatenating an empty prefix — byte-for-byte
//! the same address `rehook` would have built before this section existed. `guest_gnode_level
//! == 0` behavior is therefore unchanged, not specially preserved.
//!
//! **A member already mid-negotiation when the propagation lands is let finish, not aborted.**
//! `ntk_hooking::arc::run_arc_handler` gains one extra check, gated to `ask_lvl >= 1`
//! (unreachable at `ask_lvl == 0` for the same one-member reason above): if this identity's own
//! network id already matches the target by the time its own `search_migration_path` resolves —
//! a sibling's propagation landed first — it aborts its own now-redundant reservation instead of
//! completing it. Earlier stages (a still-in-flight `evaluate_enter`/`begin_enter`/
//! `search_migration_path` round trip) are deliberately *not* raced against incoming propagation:
//! the coordinator's own per-(g-node, level) exclusivity already bounds how many members can be
//! mid-negotiation at once, doing so would need the arc-handler's sequential RPC chain to become
//! cancellation-aware at points it never has been, and this state machine already tolerates a
//! second, later negotiation resolving a g-node that has already arrived (`migration_in_progress`
//! plus [`GenerationHandles::migrations`] above, not a one-shot latch) — a second, different
//! `DoFinishEnter` for the same g-node just drives one more, self-correcting `rehook`, exactly
//! like an unrelated later merge would.
//!
//! # Scope boundary: no true concurrent fork, no third-network re-fork
//! `rehook` always does the same simple thing regardless of how many times it runs: fork the
//! identity, tear down the previous generation, rebuild at the combined position. It never
//! models upstream's *connectivity identity* (`make_connectivity`/`check_connectivity`,
//! `identities.vala:441-577`) — a bridge kept alive so a still-migrating g-node's external arcs
//! stay reachable throughout, with the guest re-hooking concurrently rather than after a
//! synchronous teardown. `migrate`'s own "Why not a true concurrent fork" section gives the two
//! structural reasons this daemon cannot do that today regardless of `guest_gnode_level` (one
//! live dispatcher target per process; never two identities simultaneously reachable, so
//! either bridging call would violate its own concurrent-fork precondition before
//! `guest_gnode_level` is even relevant — see `migrate`'s own doc for the detail).
//! `ntk_hooking::HookingEvent::DoPrepareMigration`/`DoFinishMigration` are wired to
//! [`ntk_identities::Handle::prepare_migration`]/`migrate` — real, working identity-registry
//! bookkeeping — but this daemon does not spin up a second full protocol stack for the identity
//! `migrate` resolves: `ntk-qspn`'s own scope note says it "never models `enter_net`". A fully
//! faithful port would spawn that second complete stack the moment `migrate` returns its id — a
//! substantial addition kept out of this pass and reported rather than half-built.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::{Fingerprint, Naddr, Topology};
use ntk_hooking::HookingEvent;
use ntk_identities::{ArcInfo, MigrationId};
use ntk_netlink::Interface;
use ntk_qspn::QspnEvent;
use ntk_rpc::RpcClient;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::kernel::config::NtkdConfig;
use crate::kernel::routes::RouteInstaller;
use crate::node::adapters::{CoordinatorClientAdapter, NetworkInfo, QspnArcResolverAdapter};
use crate::node::dispatch::{Dispatcher, IdentityStack};
use crate::node::kernel_handle::{KernelHandle, SendNetlink};
use crate::node::peers::PeerLinks;
use crate::node::registry::LinkRegistry;
use crate::node::services::{self, HookingProvenance};
use crate::node::stubs::{
    HookingStubFactoryAdapter, IdentityStubFactoryAdapter, QspnStubFactoryAdapter,
};

/// Resolves a neighbor's linklocal address string into an outbound [`RpcClient`], shared by
/// every module's stub factory (`crate::node::peers`'s module doc). Production dials a real
/// TCP connection; tests substitute a pre-wired directory (no sockets, `FakeRpcClient`).
pub trait Dialer: Send + Sync {
    fn dial(&self, addr: &str, port: u16) -> BoxFuture<'_, Option<Arc<dyn RpcClient>>>;

    /// Like [`Self::dial`], but binds the outbound socket to `device` first when given — see
    /// [`ntk_rpc::TcpRpcClient::connect_via`]'s doc for why: a `169.254.0.0/16` destination
    /// does not disambiguate an outbound dial across 2+ monitored NICs by address alone, so a
    /// relay node must nail down the egress NIC itself. Default forwards to [`Self::dial`],
    /// ignoring `device` — correct for fakes/test dialers with no real NIC to bind to.
    fn dial_via(
        &self,
        addr: &str,
        port: u16,
        device: Option<&str>,
    ) -> BoxFuture<'_, Option<Arc<dyn RpcClient>>> {
        let _ = device;
        self.dial(addr, port)
    }
}

/// Real-transport [`Dialer`]: connects a fresh [`ntk_rpc::TcpRpcClient`] per neighbor.
#[derive(Debug)]
pub struct TcpDialer {
    pub max_frame_length: usize,
    pub call_timeout: Duration,
}

impl Default for TcpDialer {
    fn default() -> Self {
        Self {
            max_frame_length: 1 << 20,
            call_timeout: Duration::from_secs(10),
        }
    }
}

impl Dialer for TcpDialer {
    fn dial(&self, addr: &str, port: u16) -> BoxFuture<'_, Option<Arc<dyn RpcClient>>> {
        self.dial_via(addr, port, None)
    }

    fn dial_via(
        &self,
        addr: &str,
        port: u16,
        device: Option<&str>,
    ) -> BoxFuture<'_, Option<Arc<dyn RpcClient>>> {
        let addr = addr.to_owned();
        let device = device.map(str::to_owned);
        Box::pin(async move {
            let socket_addr: std::net::SocketAddr = format!("{addr}:{port}").parse().ok()?;
            match ntk_rpc::TcpRpcClient::connect_via(
                socket_addr,
                device.as_deref(),
                self.max_frame_length,
                self.call_timeout,
            )
            .await
            {
                Ok(client) => Some(Arc::new(client) as Arc<dyn RpcClient>),
                Err(error) => {
                    tracing::debug!(
                        %socket_addr, device = ?device, %error,
                        "ntkd: outbound dial attempt failed"
                    );
                    None
                }
            }
        })
    }
}

/// RFC 3927 (`169.254.0.0/16`) linklocal address allocator for newly-monitored NICs —
/// `NeighborhoodConfig::new_linklocal_address`. Called once per process, salted with this
/// identity's own stable [`ntk_neighborhood::NodeId`] (production: `crate::node::transport::start`;
/// tests: `tests/multi_node.rs`'s `spawn_real_node` — both already have `my_id` in scope). The
/// returned closure is then called once per NIC as `ntk_neighborhood::Manager` begins monitoring
/// it, in the order NICs are enabled.
///
/// # Bug this fixes
/// Used to be a `fetch_add` counter starting at a fixed `1` in every process, so two freshly
/// started `ntkd` processes each monitoring their first NIC both self-assigned `169.254.0.1` —
/// confirmed by a minimal two-namespace veth reproduction outside this codebase: two *equal*
/// `/16` addresses on the two ends of one link fail to exchange broadcast (the kernel drops an
/// inbound packet claiming a local address as martian); two *distinct* `/16` addresses succeed.
///
/// # Why `my_id`, not a dedicated salt
/// An earlier version of this function could not take `my_id` as a parameter —
/// `NeighborhoodConfig::new_linklocal_address`'s field type (`Box<dyn FnMut() -> String + Send>`)
/// takes no arguments, and both call sites used to invoke `linklocal_allocator()` itself with no
/// arguments before wiring the result into `NeighborhoodConfig` — so it minted its own *fresh*
/// [`ntk_neighborhood::NodeId`] via [`ntk_neighborhood::NodeId::generate`] purely as a private
/// salt. That worked (a second random id is just as good a salt as the real one), but left every
/// node carrying two independent process-lifetime identities where one would do, when the real
/// `my_id` — the value `synthetic_mac` and every wire arc resolution already key off — was always
/// available at both call sites once threaded through this function's signature instead:
/// - two independently started daemons (the production case this bug broke) get different
///   `my_id`s and so, overwhelmingly likely, different addresses for their first NIC;
/// - every NIC on the *same* node gets a provably distinct address (see `derive_linklocal`'s
///   doc: NICs are enumerated as an exact permutation of the valid address space, not hashed
///   independently);
/// - the same `(my_id, index)` pair — i.e. the same NIC, re-derived within one process's
///   lifetime — always yields the same address, since `my_id` is constant for the process and
///   `index` only ever increases.
///
/// # Residual collision risk
/// This is a hash-based pseudo-random pick, not RFC 3927's own probe-and-defend (ARP) collision
/// resolution — implementing that is out of scope here. Two daemons' *first* NIC collide only
/// if their `my_id`s hash into the same starting slot of the `LINKLOCAL_SPACE`-sized valid
/// range: roughly a 1-in-65,024 chance for any specific pair, growing with the number of daemons
/// starting simultaneously on one broadcast domain (birthday bound: ~1% at ~36 concurrent
/// daemons). Acceptable for this daemon's actual deployment shape — small numbers of
/// directly-adjacent peers per link, not broadcast domains with dozens of strangers
/// self-assigning at once — and no worse than what RFC 3927 itself tolerates before its own
/// probe-and-defend step would run.
#[must_use]
pub fn linklocal_allocator(my_id: ntk_neighborhood::NodeId) -> Box<dyn FnMut() -> String + Send> {
    let mut index: u32 = 0;
    Box::new(move || {
        let addr = derive_linklocal(my_id, index);
        index += 1;
        addr.to_string()
    })
}

/// Number of valid, non-reserved addresses in `169.254.0.0/16`: RFC 3927 §2.1 reserves the
/// first and last `/24` of the block (`169.254.0.0/24`, `169.254.255.0/24`) "for future use",
/// leaving the third octet ranging over `1..=254` (254 values) and the fourth over the full
/// `0..=255` (256 values).
const LINKLOCAL_VALID_THIRDS: u32 = 254;
const LINKLOCAL_FOURTHS: u32 = 256;
const LINKLOCAL_SPACE: u32 = LINKLOCAL_VALID_THIRDS * LINKLOCAL_FOURTHS;

/// Derives the `index`-th linklocal address a node identified by `salt` self-assigns, confined
/// to the reserved-block-excluded `169.254.1.0`-`169.254.254.255` range (see
/// [`LINKLOCAL_SPACE`]'s doc).
///
/// Hashes `salt` with `DefaultHasher` (a fixed algorithm/seed, like `synthetic_mac`'s hash — not
/// `RandomState`, which reseeds every process) to pick this node's *starting* slot in the
/// `LINKLOCAL_SPACE`-sized valid address space, then walks `index` slots forward from there,
/// wrapping. That walk is `index -> slot modulo a fixed space`, so every `index` in
/// `0..LINKLOCAL_SPACE` maps to a distinct slot — i.e. distinct NICs on the same node
/// (`linklocal_allocator` calls this with `index` `0, 1, 2, ...`) are *structurally* guaranteed
/// distinct addresses, not merely hashed-and-hoped. Only the *starting* slot depends on `salt`,
/// so two different salts landing on the same start is the sole source of residual collision
/// risk ([`linklocal_allocator`]'s doc quantifies it).
fn derive_linklocal(salt: ntk_neighborhood::NodeId, index: u32) -> Ipv4Addr {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    salt.hash(&mut hasher);
    let start = (hasher.finish() % u64::from(LINKLOCAL_SPACE)) as u32;
    let slot = (start + index) % LINKLOCAL_SPACE;
    let third = (slot / LINKLOCAL_FOURTHS) as u8 + 1; // 1..=254, excludes the two reserved /24s
    let fourth = (slot % LINKLOCAL_FOURTHS) as u8; // 0..=255
    Ipv4Addr::new(169, 254, third, fourth)
}

/// Derives this identity's own trivial network-of-one position when the caller supplies none
/// (production; see [`NodeInputs::initial_position`]'s doc): one [`NodeId`]-salted position per
/// topology level, each within `0..gsize(level)` — the same hash-based pseudo-random pick as
/// [`derive_linklocal`], for the identical reason (two independently started daemons get
/// different `my_id`s and so, overwhelmingly likely, different starting positions).
///
/// Two nodes nonetheless landing on the *same* position (or, at any shared level, positions
/// naming the same or an adjacent g-node) is not a failure this function needs to prevent — it
/// is exactly the case `ntk-hooking`'s own merge negotiation (`merge_direction`/`merge_tiebreak`,
/// `arc_handler.vala:150-214`) already exists to resolve between two solitary, create_net-rooted
/// networks that happen to meet (module doc's "Negotiated re-address" section).
fn derive_initial_position(my_id: ntk_neighborhood::NodeId, topology: &Topology) -> Vec<u32> {
    use std::hash::{Hash, Hasher};
    (0..topology.levels())
        .map(|level| {
            let gsize = topology.gsize(level).unwrap_or(1).max(1);
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            my_id.hash(&mut hasher);
            level.hash(&mut hasher);
            (hasher.finish() % u64::from(gsize)) as u32
        })
        .collect()
}

/// A [`Naddr::new_allowing_virtual`] placeholder — position `gsize(level)` at every level, the
/// smallest value [`Naddr::is_virtual_at`] always calls out of range — for the entering
/// generation's [`RouteInstaller`] to hold for the whole bootstrap-wait window
/// ([`migrate`]'s own doc). This is a bookkeeping sentinel, unrelated to the actual negotiated
/// target: [`Naddr`]'s own type doc defines "virtual" purely as `pos >= gsize(level)`, so the
/// real, already-Coordinator-resolved position (always `< gsize`) cannot itself be marked
/// virtual — the daemon must hold a genuinely different placeholder value, then
/// [`RouteInstaller::realize`] to the real one once bootstrap confirms.
fn virtual_placeholder(topology: &Topology) -> Naddr {
    let pos: Vec<u32> = (0..topology.levels())
        .map(|level| topology.gsize(level).unwrap_or(1))
        .collect();
    Naddr::new_allowing_virtual(topology.clone(), pos)
        .expect("gsize(level) at every level has exactly topology.levels() entries")
}

/// A harness-only way to start a node already a member of a real, shared network — see
/// [`NodeInputs::preformed`]'s doc for the full distinction from [`NodeInputs::initial_position`].
#[derive(Clone, Debug)]
pub struct PreformedNetwork {
    /// Seeds [`NetworkInfo::new`] directly, instead of `random_i64` — shared by every other
    /// member of the same pre-formed g-node so their mutual arcs resolve
    /// [`ntk_hooking::QspnView::note_same_network`], not `note_foreign`.
    pub network_id: i64,
    /// This identity's position at each topology level, innermost first — seeds [`Naddr`]
    /// exactly like [`NodeInputs::initial_position`] does.
    pub position: Vec<u32>,
}

/// Everything [`run`] needs from its caller.
pub struct NodeInputs<K> {
    pub config: NtkdConfig,
    /// Already spawned by the caller against the concrete netlink backend (see the module doc).
    pub neighborhood: ntk_neighborhood::Handle,
    pub registry: Arc<LinkRegistry>,
    pub links: Arc<PeerLinks>,
    /// Shared with the caller (e.g. a test asserting on `FakeNetlink::operations()`); wrapped in
    /// [`KernelHandle`] for [`RouteInstaller`].
    pub routing_kernel: Arc<K>,
    pub dialer: Arc<dyn Dialer>,
    /// This identity's position at each topology level, innermost first. `None` (production
    /// default, see [`crate::node::transport::start`]) derives a per-[`ntk_neighborhood::NodeId`] position via
    /// `derive_initial_position` — this identity is still its own network-of-one at that
    /// position (`ntk_hooking::HookingOrigin::CreateNet`, its own random `network_id`), exactly
    /// like upstream's `create_net`; the module doc's "Negotiated re-address" section covers
    /// what happens when it then discovers a bigger network. A multi-node test harness composing
    /// several [`run`] calls against one shared [`ntk_netlink::FakeNetlink`]/topology instead
    /// gives each node a distinct, authoritative `Some(pos)` that is never renegotiated — this
    /// is a *freeze* knob, unconditionally clearing `negotiated`. Mutually exclusive with
    /// [`NodeInputs::preformed`] (a position that stays negotiable); [`run`] rejects both being
    /// `Some`.
    pub initial_position: Option<Vec<u32>>,
    /// This identity starts already a member of a real, shared network rather than its own
    /// network-of-one: `preformed.position` seeds [`Naddr`] exactly like `initial_position`
    /// does, but `preformed.network_id` seeds [`NetworkInfo`] instead of `random_i64`, and —
    /// the entire point of this field — `negotiated` stays `true` (it is computed from
    /// `initial_position.is_none()` alone, unaffected by this field), so `migrate` is never
    /// blocked the way it is for `initial_position`. Modeled after a real g-node that formed
    /// before this process existed: several nodes sharing this same `network_id` at a shared
    /// upper-level position, with distinct lower-level positions, is what makes them one
    /// pre-formed g-node rather than several coincidentally-adjacent ones. Mutually exclusive
    /// with `initial_position`; [`run`] rejects both being `Some`. See the module doc's
    /// "`NodeInputs::preformed`: pre-formed, not frozen" section for why a bare
    /// `initial_position` cannot express this.
    pub preformed: Option<PreformedNetwork>,
    /// This identity's own stable Neighborhood discovery id — must be exactly the same
    /// [`ntk_neighborhood::NodeId`] the caller passed as `NeighborhoodConfig::my_id` when
    /// spawning `neighborhood`. See `crate::node::registry::encode_caller_id`'s doc for why
    /// this (not a peer-decodable [`crate::node::registry::LinkId`]) is what outbound qspn calls embed.
    pub my_id: ntk_neighborhood::NodeId,
}

impl<K: std::fmt::Debug> std::fmt::Debug for NodeInputs<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeInputs")
            .field("config", &self.config)
            .field("neighborhood", &self.neighborhood)
            .field("registry", &self.registry)
            .field("links", &self.links)
            .field("routing_kernel", &self.routing_kernel)
            .field("dialer", &"<dyn Dialer>")
            .field("initial_position", &self.initial_position)
            .field("preformed", &self.preformed)
            .field("my_id", &self.my_id)
            .finish()
    }
}

/// The four identity-generation-scoped handles that change together on every `rehook`: torn
/// down and rebuilt as a unit, exactly like `Generation`'s own bundle (`identities`/`hooking`
/// are excluded for the same reason `Generation` excludes/carries them — see the module doc's
/// "Negotiated re-address" section and [`crate::node::services::HookingProvenance`]'s doc).
///
/// # Bug this fixes
/// `RunningNode` used to hold `qspn`/`peers`/`coordinator`/`andna` as plain fields, captured
/// once from the *first* (always-trivial) generation and never updated — unlike
/// `route_installer` below, which already solves this identical staleness problem for kernel
/// state via `Arc<Mutex<_>>`. Once a negotiated identity actually rehooks, those four plain
/// fields kept pointing at the old generation's now-cancelled actors forever: the status server
/// (`crate::node::status::report`) would report frozen route/participation/reservation/hostname
/// counts, and any test holding a `RunningNode` reference could never observe the post-rehook
/// state. A `watch::Receiver` here, updated by `rehook` via `send_replace` (module doc), keeps
/// every reader current.
///
/// # `rehooked`: why a dedicated flag, not "did the position change"
/// The obvious external proxy for "did this identity rehook" — comparing
/// `qspn.my_naddr().positions()` before and after — is unsound: `rehook` can legitimately
/// negotiate a position that numerically coincides with this identity's own pre-migration
/// position (the Coordinator reserves whatever slot is free in the *other* network, which has
/// no relationship to the slot this identity happened to hold before). Confirmed by a captured
/// stress-test failure of `tests/multi_node.rs`'s
/// `real_netns_two_daemons_negotiate_a_shared_network`: the guest's own trace log showed the
/// full `AnotherNetwork` -> `finish_enter` -> `rehook` sequence complete successfully, yet its
/// reserved position (`[0]`) happened to equal its own discarded starting position (`[0]`,
/// deterministic from that test's hardcoded `NodeId`), so a position-comparison heuristic
/// reported "not rehooked" for an identity that, per the daemon's own internal state,
/// definitely had. `rehooked` instead mirrors whether [`Self::migrations`] is nonzero — the
/// authoritative count `rehook` itself maintains — so external observers never have to
/// (unsoundly) reconstruct it. `rehook` is not one-shot (module doc's "Coordinated multi-member
/// migration" section), so `rehooked` means "has migrated at least once", not "did the one
/// allowed migration happen" — existing readers of this field (`tests/multi_node.rs`,
/// `tests/wireless.rs`, `tests/netns`) only ever test it for `true`, which this meaning
/// preserves exactly.
#[derive(Clone, Debug)]
pub struct GenerationHandles {
    pub qspn: ntk_qspn::QspnHandle,
    pub peers: ntk_peerservices::Handle,
    pub coordinator: ntk_coordinator::Handle,
    pub andna: ntk_andna::Handle,
    /// See this struct's own doc's `rehooked` section. `true` once [`Self::migrations`] is
    /// nonzero.
    pub rehooked: bool,
    /// How many times this identity has successfully migrated so far. `0` for the initial
    /// generation [`run`] constructs.
    pub migrations: u32,
}

impl GenerationHandles {
    fn from_generation<K>(generation: &Generation<K>, migrations: u32) -> Self {
        Self {
            qspn: generation.qspn.clone(),
            peers: generation.peers.clone(),
            coordinator: generation.coordinator.clone(),
            andna: generation.andna.clone(),
            rehooked: migrations > 0,
            migrations,
        }
    }
}

/// Every long-lived handle the supervisor/status server need once startup completes.
#[derive(Debug)]
pub struct RunningNode<K> {
    /// See [`GenerationHandles`]'s doc for why this is a `watch::Receiver`, not four plain
    /// fields.
    pub generation: watch::Receiver<GenerationHandles>,
    pub identities: ntk_identities::Handle,
    pub neighborhood: ntk_neighborhood::Handle,
    pub hooking: ntk_hooking::HookingHandle,
    pub registry: Arc<LinkRegistry>,
    pub route_table: u32,
    pub route_installer: Arc<tokio::sync::Mutex<RouteInstaller<KernelHandle<K>>>>,
    /// Shared with the caller (`NodeInputs::routing_kernel`) — reused for
    /// [`ntk_netlink::cleanup`] at shutdown.
    pub kernel: Arc<K>,
    pub net: Arc<NetworkInfo>,
}

/// [`run`]'s full result: the running actors plus the inbound [`Dispatcher`] the caller binds
/// to real (`TcpServer`/`UdpBroadcaster`) or fake (`FakeRpcClient`) transport.
#[derive(Debug)]
pub struct StartedNode<K> {
    pub running: RunningNode<K>,
    pub dispatcher: Arc<Dispatcher>,
}

/// Everything [`bootstrap_generation`] spawns for one identity generation: torn down and
/// rebuilt as a unit by [`rehook`] on a negotiated re-address. `identities` is deliberately not
/// part of this bundle — see the module doc's "Negotiated re-address" section.
struct Generation<K> {
    qspn: ntk_qspn::QspnHandle,
    /// Subscribed the instant [`ntk_qspn::spawn`] returns, inside [`bootstrap_generation`] —
    /// see that function's doc for why this exact timing is load-bearing, not incidental.
    qspn_events: broadcast::Receiver<QspnEvent>,
    hooking: ntk_hooking::HookingHandle,
    peers: ntk_peerservices::Handle,
    coordinator: ntk_coordinator::Handle,
    andna: ntk_andna::Handle,
    dispatch: IdentityStack,
    route_installer: RouteInstaller<KernelHandle<K>>,
}

/// Which QSPN actor variant [`bootstrap_generation`] should spawn — a fresh `create_net` root
/// ([`run`], this identity's very first generation) or a real entering identity taking over from
/// a superseded generation ([`migrate`], `ntk_qspn::spawn_entering`). `guest_gnode_level`/
/// `host_gnode_level` come straight from [`HookingEvent::DoFinishEnter`]/[`Topology::levels`] —
/// see [`migrate`]'s own doc for why `internal_arcs`/`previous_destinations` are always empty in
/// this daemon's reachable scope, regardless of `guest_gnode_level`.
#[derive(Clone, Copy)]
enum QspnOrigin {
    CreateNet,
    Entering {
        guest_gnode_level: usize,
        host_gnode_level: usize,
    },
}

/// Spawns qspn, peerservices/coordinator/andna/hooking (via [`services::spawn`]), and this
/// identity's kernel routes, all positioned at `my_naddr` — the common bootstrap both [`run`]
/// (the initial, always-trivial generation) and [`rehook`] (a later negotiated re-address) need.
/// Every actor spawned here is a child of `cancel` and reaped into `tasks`.
///
/// # Bug this fixes: a lost-event race on `QspnEvent::BootstrapComplete`
/// `ntk_qspn`'s actor unconditionally fires `BootstrapComplete` `bootstrap_signal_delay`
/// (default 1ms) after its own task is first polled — `create_net` is always immediately
/// bootstrap-complete (`ntk_qspn`'s own module doc), so nothing external gates it. A
/// `tokio::sync::broadcast` channel only delivers an event to receivers that already existed
/// at send time; a receiver created afterward starts empty and can never see it. This function
/// used to let its caller subscribe (`run_steady_state`'s own `ctx.qspn.subscribe_events()`,
/// called only once that task itself got scheduled and run) — under CPU contention, that first
/// poll can easily land more than 1ms after `ntk_qspn::spawn` returns, so the actor's
/// self-timer can fire, find zero receivers, and silently drop `BootstrapComplete` forever.
/// `NetworkInfo::is_bootstrapped()` then never latches, `QspnViewAdapter::is_bootstrapped()`
/// stays `false` permanently, and every peer's `retrieve_network_data`/`search_migration_path`
/// call against this identity fails `NotBootstrapped` for the rest of the process's life —
/// silently (`ntk_hooking::arc::run_arc_handler`'s own `NotBootstrapped` retry loop logs
/// nothing), which is exactly what a stress run of `tests/multi_node.rs`'s
/// `real_netns_two_daemons_negotiate_a_shared_network` captured: neither side ever logged past
/// its own initial arc discovery, for the full 25s deadline, with heavier parallel load
/// correlating with a higher failure rate — the signature of a scheduling-delay-sensitive lost
/// wakeup, not a slow-but-eventually-successful retry. Fixed by subscribing synchronously in
/// this function, immediately after `ntk_qspn::spawn` returns and before this task's next
/// `.await` point: `spawn` itself never polls the actor it hands back (a fresh `tokio::spawn`
/// only enqueues it), so no event can possibly fire before this line runs — see
/// [`Generation::qspn_events`]'s own doc. [`rehook`] carries the identical risk (it also calls
/// `bootstrap_generation`, then used to subscribe only after several more `.await` points,
/// including a real netlink round trip in `installer.install_identity()`) and is fixed the same
/// way, by reading `Generation::qspn_events` instead of re-subscribing.
///
/// # Errors
/// Propagates [`crate::kernel::routes::RouteError`] from installing this identity's address/
/// rule.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_generation<K>(
    topology: Topology,
    my_naddr: Naddr,
    origin: QspnOrigin,
    hooking: HookingProvenance,
    net: Arc<NetworkInfo>,
    registry: Arc<LinkRegistry>,
    links: Arc<PeerLinks>,
    my_id: ntk_neighborhood::NodeId,
    kernel: KernelHandle<K>,
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
    // This node's RPC-identity signing key and `require_auth` flag — threaded straight through
    // to `services::spawn` (`ntk_peerservices::Handle::with_signing_key`/`Config::require_auth`).
    // Reloaded fresh at every generation (including a `rehook`) rather than persisted across
    // them: `andna_key::load_or_generate` is idempotent against the same path, and a rehook is
    // rare enough that the extra file read is immaterial.
    signing_key: Option<ed25519_dalek::SigningKey>,
    require_auth: bool,
) -> anyhow::Result<Generation<K>>
where
    K: SendNetlink + 'static,
{
    let levels = topology.levels();
    // The route installer's own naddr, decoupled from qspn's: qspn's `my_naddr` is always real
    // (spawn_entering has no later "make it real" hook this daemon's scope can use — see
    // `migrate`'s doc), but kernel state for an *entering* generation stays suppressed
    // (`RouteInstaller::install_identity`/`apply`'s own virtual no-op) until its bootstrap
    // confirms — see [`virtual_placeholder`]'s doc.
    let route_naddr = match origin {
        QspnOrigin::CreateNet => my_naddr.clone(),
        QspnOrigin::Entering { .. } => virtual_placeholder(&topology),
    };

    let my_fingerprint = Fingerprint::new(random_fingerprint_id(), 0, vec![0u32; levels]);
    let qspn_stub_factory = Arc::new(QspnStubFactoryAdapter {
        links: links.clone(),
        registry: registry.clone(),
        my_id,
    });
    let (qspn, qspn_join) = match origin {
        QspnOrigin::CreateNet => ntk_qspn::spawn(
            my_naddr.clone(),
            my_fingerprint,
            ntk_qspn::QspnConfig::default(),
            qspn_stub_factory,
            Arc::new(ntk_qspn::FixedThreshold::default()),
            Arc::new(ntk_qspn::DefaultArcIdSource::default()),
            cancel.child_token(),
        ),
        QspnOrigin::Entering {
            guest_gnode_level,
            host_gnode_level,
        } => ntk_qspn::spawn_entering(
            my_naddr.clone(),
            my_fingerprint,
            ntk_qspn::QspnConfig::default(),
            qspn_stub_factory,
            Arc::new(ntk_qspn::FixedThreshold::default()),
            Arc::new(ntk_qspn::DefaultArcIdSource::default()),
            Vec::new(),
            Vec::new(),
            guest_gnode_level,
            host_gnode_level,
            (0, 0),
            Vec::new(),
            cancel.child_token(),
        )?,
    };
    // Load-bearing ordering — see this function's own doc's "Bug this fixes" section: this
    // MUST be the very next statement after `ntk_qspn::spawn` returns, before any `.await`
    // point in this task, so no `QspnEvent` (in particular the actor's own near-immediate
    // `BootstrapComplete`) can possibly fire before this receiver exists.
    let qspn_events = qspn.subscribe_events();
    tasks.spawn(async move {
        let _ = qspn_join.await;
    });

    let svc = services::spawn(
        topology,
        my_naddr.clone(),
        hooking,
        qspn.clone(),
        registry.clone(),
        links.clone(),
        net.clone(),
        tasks,
        &cancel,
        signing_key,
        require_auth,
    )
    .await;

    let table = ntk_netlink::DEFAULT_MAIN_TABLE_ID;
    let rule_priority = ntk_netlink::DEFAULT_MAIN_RULE_PRIORITY;
    let mut installer = RouteInstaller::new(kernel, route_naddr, table, rule_priority);
    installer.install_identity().await?;

    let arc_resolver = Arc::new(QspnArcResolverAdapter {
        registry: registry.clone(),
    });
    let qspn_cfg = ntk_qspn::QspnConfig::default();
    let qspn_rpc = ntk_qspn::QspnRpcHandler::new(
        qspn.clone(),
        arc_resolver,
        qspn_cfg.arc_timeout,
        qspn_cfg.caller_arc_poll_interval,
    );
    let peers_rpc = ntk_peerservices::PeersRpcHandler::new(svc.peers.clone());
    let coordinator_rpc = ntk_coordinator::CoordinatorRpcHandler::new(svc.coordinator.clone());
    let hooking_stub_factory = Arc::new(HookingStubFactoryAdapter {
        qspn: qspn.clone(),
        links: links.clone(),
        registry,
    });
    let coord_client_for_router = Arc::new(CoordinatorClientAdapter::new(
        svc.coordinator_client.clone(),
        svc.coordinator.clone(),
        qspn.clone(),
        net,
        services::coordinator_config().n_nodes_cache_ttl,
    ));
    let view_for_router = svc.qspn_view.clone() as Arc<dyn ntk_hooking::QspnView>;
    let router = Arc::new(ntk_hooking::MessageRouting::new(
        view_for_router.clone(),
        coord_client_for_router.clone(),
        hooking_stub_factory,
        ntk_hooking::HookingConfig::default().routing_response_timeout,
    ));
    let hooking_rpc =
        ntk_hooking::HookingRpcHandler::new(view_for_router, coord_client_for_router, router);

    Ok(Generation {
        qspn,
        qspn_events,
        hooking: svc.hooking,
        peers: svc.peers,
        coordinator: svc.coordinator,
        andna: svc.andna,
        dispatch: IdentityStack {
            qspn: qspn_rpc,
            peers: peers_rpc,
            coordinator: coordinator_rpc,
            hooking: hooking_rpc,
        },
        route_installer: installer,
    })
}

/// Runs the full startup sequence and spawns the steady-state loop into `tasks`. Every actor
/// this function spawns is a child of `cancel`.
///
/// # Errors
/// Propagates [`crate::kernel::preflight::PreflightError`] and [`crate::kernel::routes::RouteError`].
pub async fn run<K>(
    inputs: NodeInputs<K>,
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
) -> anyhow::Result<StartedNode<K>>
where
    K: SendNetlink + 'static,
{
    let NodeInputs {
        config,
        neighborhood,
        registry,
        links,
        routing_kernel,
        dialer,
        initial_position,
        preformed,
        my_id,
    } = inputs;
    anyhow::ensure!(
        initial_position.is_none() || preformed.is_none(),
        "NodeInputs::initial_position and NodeInputs::preformed are mutually exclusive: \
         initial_position={initial_position:?} preformed={preformed:?}"
    );
    // Must happen before this function returns: both call sites (`crate::node::transport::start`,
    // `tests/multi_node.rs`'s `spawn_real_node`) spawn every task that could ever *receive* an
    // inbound message (`TcpServer::serve`, `ntk_neighborhood::serve_broadcast`) only after `run`
    // completes, so no arc can reach a state that needs dialing (`crate::node::stubs`'s
    // lazy-dialing `unicast`) before this line has run.
    links.set_port(config.port());
    let signing_key = config
        .node_key_path()
        .map(crate::node::andna_key::load_or_generate)
        .transpose()?;
    let require_auth = config.require_auth();
    let kernel = KernelHandle(routing_kernel.clone());
    crate::kernel::preflight::check(&kernel).await?;

    let topology = config.topology()?;
    let levels = topology.levels();

    // Every node bootstraps as its own trivial network-of-one, at a distinct position unless the
    // caller supplied one — either an authoritative, never-renegotiated `initial_position` or a
    // `preformed` position/`network_id` pair that stays negotiable (`NodeInputs::preformed`'s
    // doc explains the distinction; the `ensure!` above already rejected both being set). Always
    // `CreateNet`: every node is its own network-of-one from the start, exactly like upstream's
    // own `create_net`; `negotiated` (an explicit `initial_position` is authoritative and never
    // renegotiated; `preformed`, like the production default, stays negotiable) instead gates
    // [`rehook`], triggered later by `HookingEvent::DoFinishEnter`.
    let negotiated = initial_position.is_none();
    let origin = ntk_hooking::HookingOrigin::CreateNet;
    let position = match (initial_position, &preformed) {
        (Some(pos), _) => pos,
        (None, Some(preformed)) => preformed.position.clone(),
        (None, None) => derive_initial_position(my_id, &topology),
    };
    let my_naddr = Naddr::new(topology.clone(), position)?;
    let network_id = preformed.map_or_else(random_i64, |preformed| preformed.network_id);
    let net = Arc::new(NetworkInfo::new(levels, network_id));

    // -- Identities (survives every future `rehook` — see the module doc) --
    let identity_stub_factory = Arc::new(IdentityStubFactoryAdapter {
        links: links.clone(),
        registry: registry.clone(),
    });
    let (identities, identities_join) = ntk_identities::Handle::spawn(
        Some(my_naddr.clone()),
        identity_stub_factory.clone(),
        cancel.child_token(),
    );
    tasks.spawn(async move {
        let _ = identities_join.await;
    });

    // -- qspn / peerservices / coordinator / andna / hooking / routes --
    let generation_cancel = cancel.child_token();
    let generation = bootstrap_generation(
        topology.clone(),
        my_naddr.clone(),
        QspnOrigin::CreateNet,
        HookingProvenance::Fresh(origin),
        net.clone(),
        registry.clone(),
        links.clone(),
        my_id,
        kernel,
        tasks,
        generation_cancel.clone(),
        signing_key.clone(),
        require_auth,
    )
    .await?;
    let table = ntk_netlink::DEFAULT_MAIN_TABLE_ID;
    let generation_handles = GenerationHandles::from_generation(&generation, 0);

    // -- Inbound dispatch --
    let identity_rpc =
        ntk_identities::IdentityRpcHandler::new(identities.clone(), identity_stub_factory);
    let neighborhood_rpc =
        ntk_neighborhood::NeighborhoodRpcHandler::for_unicast(neighborhood.clone());
    let dispatcher = Arc::new(Dispatcher::new(
        neighborhood_rpc,
        identity_rpc,
        generation.dispatch,
    ));

    let route_installer = Arc::new(tokio::sync::Mutex::new(generation.route_installer));

    // -- Steady-state event loop --
    let net_for_running = net.clone();
    let (generation_handles_tx, generation_handles_rx) = watch::channel(generation_handles);
    tasks.spawn(run_steady_state(
        SteadyStateCtx {
            config_port: config.port(),
            topology,
            my_id,
            qspn: generation.qspn.clone(),
            identities: identities.clone(),
            neighborhood: neighborhood.clone(),
            hooking: generation.hooking.clone(),
            peers: generation.peers.clone(),
            registry: registry.clone(),
            links,
            dialer,
            net,
            route_installer: route_installer.clone(),
            kernel: routing_kernel.clone(),
            dispatcher: dispatcher.clone(),
            generation_handles_tx,
            negotiated,
            migration_in_progress: false,
            migrations: 0,
            generation_cancel,
            generation_tasks: JoinSet::new(),
            signing_key,
            require_auth,
        },
        generation.qspn_events,
        cancel,
    ));

    Ok(StartedNode {
        running: RunningNode {
            generation: generation_handles_rx,
            identities,
            neighborhood,
            hooking: generation.hooking,
            registry,
            route_table: table,
            route_installer,
            kernel: routing_kernel,
            net: net_for_running,
        },
        dispatcher,
    })
}

fn random_i64() -> i64 {
    let raw = RandomState::new().build_hasher().finish();
    (raw & 0x7fff_ffff_ffff_ffff).max(1) as i64
}

fn random_fingerprint_id() -> Vec<u8> {
    RandomState::new()
        .build_hasher()
        .finish()
        .to_be_bytes()
        .to_vec()
}

/// A fresh, process-random [`MigrationId`] for one [`migrate`] call — the same
/// `RandomState`-backed technique as [`random_i64`]/[`random_fingerprint_id`], narrowed to
/// [`MigrationId`]'s own `1..=i32::MAX` positive range (`ntk_identities::MigrationId`'s doc).
fn random_migration_id() -> MigrationId {
    let raw = RandomState::new().build_hasher().finish() as i32;
    MigrationId(raw.checked_abs().unwrap_or(i32::MAX).max(1))
}

struct SteadyStateCtx<K> {
    config_port: u16,
    topology: Topology,
    my_id: ntk_neighborhood::NodeId,
    qspn: ntk_qspn::QspnHandle,
    identities: ntk_identities::Handle,
    neighborhood: ntk_neighborhood::Handle,
    hooking: ntk_hooking::HookingHandle,
    /// This generation's `ntk_peerservices::Handle` — needed to re-drive
    /// [`ntk_peerservices::Handle::reannounce_participation`] once this identity actually gains
    /// a reachable neighbor (`on_neighborhood_event`'s `ArcAdded` arm / [`reattach_known_arcs`]);
    /// see those call sites' own doc for why the boot-time flood alone is not enough.
    peers: ntk_peerservices::Handle,
    registry: Arc<LinkRegistry>,
    links: Arc<PeerLinks>,
    dialer: Arc<dyn Dialer>,
    net: Arc<NetworkInfo>,
    route_installer: Arc<tokio::sync::Mutex<RouteInstaller<KernelHandle<K>>>>,
    kernel: Arc<K>,
    dispatcher: Arc<Dispatcher>,
    /// Publishes the current generation's `qspn`/`peers`/`coordinator`/`andna` handles for
    /// [`RunningNode::generation`]'s readers — see [`GenerationHandles`]'s doc.
    generation_handles_tx: watch::Sender<GenerationHandles>,
    negotiated: bool,
    /// Guards [`rehook`] against a second `DoFinishEnter` starting while an earlier one's
    /// synchronous teardown/rebuild is still running — never against a legitimate *repeat*
    /// migration. See the module doc's "Coordinated multi-member migration" section.
    migration_in_progress: bool,
    /// How many times this identity has migrated so far — see [`GenerationHandles::migrations`].
    migrations: u32,
    generation_cancel: CancellationToken,
    generation_tasks: JoinSet<()>,
    /// This node's RPC-identity signing key, if configured — reused by [`migrate`]'s own
    /// `bootstrap_generation` call so a negotiated re-address keeps signing outbound
    /// origin-auth requests with the same identity.
    signing_key: Option<ed25519_dalek::SigningKey>,
    require_auth: bool,
}

/// Reacts to arc up/down/cost-change, qspn route-snapshot changes, and hooking's migration
/// notifications for as long as `cancel` is live — the loop upstream's own `startup.vala` never
/// wrote (module doc).
async fn run_steady_state<K>(
    mut ctx: SteadyStateCtx<K>,
    mut qspn_events: broadcast::Receiver<QspnEvent>,
    cancel: CancellationToken,
) where
    K: SendNetlink + 'static,
{
    let mut neighborhood_events = ctx.neighborhood.subscribe();
    let mut hooking_events = ctx.hooking.subscribe_events();

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            event = neighborhood_events.recv() => {
                match event {
                    Ok(ev) => on_neighborhood_event(&ctx, ev).await,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
            event = qspn_events.recv() => {
                match event {
                    Ok(ev) => on_qspn_event(&ctx, ev).await,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => on_route_snapshot_changed(&ctx).await,
                }
            }
            event = hooking_events.recv() => {
                match event {
                    Ok(ev) => {
                        if let Some(new_qspn_events) = on_hooking_event(&mut ctx, &cancel, ev).await
                        {
                            qspn_events = new_qspn_events;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        }
    }
    // Mirrors `node::supervisor::run`'s own shutdown drain for whichever generation is current:
    // the very first generation's actors are reaped through the caller's own `tasks` `JoinSet`
    // (passed into `bootstrap_generation` from `run`), but a later negotiated generation's
    // actors (`rehook`) live only in `generation_tasks` — nothing else ever drains them.
    while ctx.generation_tasks.join_next().await.is_some() {}
}

/// Re-registers every currently-known neighborhood arc against a freshly (re)spawned qspn actor
/// and route installer — needed after [`migrate`] respawns qspn with zero registered arcs of
/// its own. `ntk_identities`/`ntk_hooking` need no equivalent step: neither is rebuilt (module
/// doc), so their own arc registrations from [`on_neighborhood_event`] are still exactly right.
///
/// Also re-drives this rehooked generation's own [`ntk_peerservices::Handle::reannounce_participation`]
/// once, if at least one arc was reattached — the negotiated generation's `peers` (`ntkd::node::
/// services::spawn`'s `retrieved_below_level = 0` seed) starts as empty as a fresh boot's did,
/// and this is its own equivalent "just gained reachable neighbors" moment. See
/// [`on_neighborhood_event`]'s `ArcAdded` arm for why this is spawned rather than awaited inline.
async fn reattach_known_arcs<K>(
    ctx: &SteadyStateCtx<K>,
    installer: &mut RouteInstaller<KernelHandle<K>>,
) where
    K: SendNetlink + 'static,
{
    let arcs = ctx.neighborhood.snapshot().borrow().clone();
    let mut reattached_any = false;
    for arc in arcs {
        let Some(cost) = arc.cost else { continue };
        let link =
            ctx.registry
                .link_for_neighbour(arc.neighbour_id, &arc.neighbour_mac, &arc.my_dev);
        let qspn_arc = match ctx.qspn.add_arc(cost).await {
            Ok(qspn_arc) => qspn_arc,
            Err(err) => {
                tracing::warn!(?link, %err, "qspn: re-attaching a known arc to the negotiated generation failed");
                continue;
            }
        };
        ctx.registry.set_qspn_arc(link, qspn_arc);
        if let Ok(via) = arc.neighbour_nic_addr.parse() {
            installer.set_arc_endpoint(qspn_arc, via, Interface::name(&arc.my_dev));
        }
        reattached_any = true;
    }
    if reattached_any {
        let peers = ctx.peers.clone();
        tokio::spawn(async move {
            peers.reannounce_participation().await;
        });
    }
}

/// Combines a propagated `finish_enter`'s target — `entry_pos`, covering levels
/// `[guest_gnode_level, topology.levels())`, the levels the negotiation actually resolved — with
/// this identity's own retained `current_positions` below `guest_gnode_level` into the full
/// address a migrating g-node's members end up sharing at the upper levels while keeping
/// distinct at the lower ones (module doc's "Coordinated multi-member migration" section).
///
/// At `guest_gnode_level == 0` this degenerates to `entry_pos` verbatim (an empty prefix),
/// byte-for-byte the address [`on_hooking_event`]'s `DoFinishEnter` arm built before this
/// function existed — the single-member case needs no combination.
///
/// `None` if `guest_gnode_level` exceeds `current_positions`' own length, or the combined
/// vector is not a valid [`Naddr`] for `topology` (wrong total length, or a position outside
/// its level's g-node size) — logged and skipped by the caller, never faked.
#[must_use]
fn combine_entry_naddr(
    topology: &Topology,
    current_positions: &[u32],
    guest_gnode_level: usize,
    entry_pos: &[u32],
) -> Option<Naddr> {
    let prefix = current_positions.get(..guest_gnode_level)?;
    let mut pos = prefix.to_vec();
    pos.extend_from_slice(entry_pos);
    Naddr::new(topology.clone(), pos).ok()
}

/// Whether an incoming `DoFinishEnter` proposing `(new_naddr, target_network_id)` is a stale
/// re-delivery of a migration this identity has *already* completed — true only when **both**
/// the position and the network id already match the identity's current state. Position alone
/// is never enough: `Naddr` carries no network identity (`ntk_common::Naddr` is just
/// `topology`+`pos`), and two distinct, still-unmerged networks landing on the same numeric
/// position is ordinary, expected input this negotiation exists to resolve
/// (`derive_initial_position`'s own doc). A real-kernel run of
/// `two_level_gnode_migrates_as_a_unit_into_merged_network` hit exactly this: an identity's own
/// untouched network-of-one position coincided with a completely different target network's
/// freshly reserved position, a position-only check treated that coincidence as "already done",
/// and the negotiated migration was silently dropped forever.
#[must_use]
fn is_stale_finish_enter_replay(
    current_naddr: &Naddr,
    current_network_id: i64,
    new_naddr: &Naddr,
    target_network_id: i64,
) -> bool {
    current_naddr == new_naddr && current_network_id == target_network_id
}

/// Reacts to a real merge negotiation resolving this identity's entry into a bigger network —
/// see the module doc's "Negotiated re-address" section for why the trigger is
/// [`HookingEvent::DoFinishEnter`], not hooking's own `chosen`/`hooked` snapshot fields (those
/// are frozen at construction for `HookingOrigin::CreateNet`, which every identity now
/// bootstraps as), and the module doc's "Coordinated multi-member migration" section for why
/// this runs any number of times over the process's life rather than once.
///
/// Tears down the previous generation's tasks and kernel routes before installing the new
/// position's — never a double-install, never a leak. Guarded against re-entrancy
/// (`ctx.migration_in_progress`) and against a stale `DoFinishEnter` re-proposing the position
/// this identity already holds in the same network (`ctx.qspn.my_naddr() == new_naddr` *and*
/// `ctx.net.network_id() == target_network_id` — position alone is not enough, see [`migrate`]'s
/// own "stale replay" doc) — otherwise runs every time
/// a negotiated identity (`ctx.negotiated`) is handed a newly resolved position.
///
/// # Real entering bootstrap, not a cold rebuild
/// The successor generation is a real [`ntk_qspn::spawn_entering`] actor, gated on its own
/// bootstrap (a qualifying peer ETP, or the configured fallback timer) before this function
/// treats it as hooked — not an instantly-"complete" `create_net` rebuild. `guest_gnode_level`
/// comes straight from the triggering `DoFinishEnter`; `host_gnode_level` is always
/// `ctx.topology.levels()` (upstream's "coordinator of the whole network" scale,
/// `api.vala:63`). `internal_arcs`/`previous_destinations` are always empty in this daemon's
/// reachable scope regardless of `guest_gnode_level`: this daemon rebuilds a migrating
/// identity's qspn state from scratch and lets [`reattach_known_arcs`] rediscover connectivity
/// through the normal protocol rather than carrying internal arc/destination state across the
/// fork (module doc's "Coordinated multi-member migration" section names this a real, accepted
/// gap — slower reconvergence, not incorrect convergence). `external_arcs` starts empty too;
/// [`reattach_known_arcs`] (called immediately after
/// spawn, before the dispatcher swap below) registers every currently-known physical arc via
/// [`ntk_qspn::QspnHandle::add_arc`], which itself triggers this entering actor's own
/// still-in-bootstrap fetch path for each one (`ntk_qspn::manager`'s `handle_add_arc`: `arc_add`
/// during bootstrap is "just another bootstrap-exit candidate" — no separate wiring needed here).
///
/// # Bug this fixes: waiting on bootstrap before swapping the dispatcher deafened this identity
/// An earlier version of this function waited for [`ntk_qspn::QspnHandle::is_bootstrap_complete`]
/// *before* calling [`Dispatcher::replace_identity_stack`], on the theory that only a fully
/// bootstrapped generation should ever become the live inbound target. Reproduced against the
/// real-kernel `two_star_groups_merge_into_one_network`/`real_netns_two_daemons_negotiate_a_shared_network`
/// scenarios, that ordering is self-defeating: the *previous* generation is already cancelled and
/// drained by this point (this function's own "tear down the previous generation" step, above),
/// so until the dispatcher swaps, every inbound RPC for this identity — including the peer's own
/// unsolicited ETP pushes, exactly the signal bootstrap is waiting on — is routed to a dead
/// actor and dropped, and the wait can only ever resolve via the fallback timer, over and over,
/// on every single migration. There is no "two live generations" hazard in swapping early: the
/// old generation is already retired by the time the new one is merely constructed (not yet
/// hooked), so exactly one generation is ever the live inbound target regardless of its own
/// bootstrap status. Swapping immediately after construction — before waiting — lets the
/// dispatcher actually deliver the qualifying ETP that ends the wait.
///
/// # Why not a true concurrent fork
/// Upstream's own model keeps a superseded identity alive as a *connectivity* bridge
/// (`ntk_qspn::QspnHandle::make_connectivity`/`check_connectivity`) while its successor
/// independently re-hooks, both simultaneously reachable. Two things in this daemon's current
/// shape make that impossible, not merely unimplemented:
/// (1) [`Dispatcher::replace_identity_stack`] swaps the *whole* [`IdentityStack`] — this daemon
/// has exactly one live inbound dispatch target per process, never two — so there is no way to
/// keep `old_id`'s protocol stack independently answering RPCs while `new_id`'s bootstraps; and
/// (2) `make_connectivity`/`check_connectivity` themselves assert `connectivity_from_level > 0`
/// (a real bridge holds internal structure *below* the level that's moving) — since this
/// daemon's fork never keeps two identities simultaneously live regardless of `guest_gnode_level`
/// (point (1) above already rules that out unconditionally), calling either would violate their
/// own concurrent-fork precondition before `guest_gnode_level` is even relevant.
/// The closest correct thing given those two facts: use `ntk_identities`' fork purely for
/// identity-*registry* bookkeeping (which `IdentityId` is "main", a virtual-then-real `naddr`
/// for `is_hooked()`), retiring `old_id` synchronously the instant this function's own
/// synchronous protocol handoff (unchanged from before this fork existed) completes — never a
/// window where two identities are simultaneously reachable, because only one ever is.
async fn migrate<K>(
    ctx: &mut SteadyStateCtx<K>,
    cancel: &CancellationToken,
    guest_gnode_level: usize,
    new_naddr: Naddr,
    target_network_id: i64,
) -> Option<broadcast::Receiver<QspnEvent>>
where
    K: SendNetlink + 'static,
{
    if !ctx.negotiated || ctx.migration_in_progress {
        return None;
    }
    // Position equality alone is not idempotency: `Naddr` carries no network identity
    // (`ntk_common::Naddr`'s own fields are just `topology`+`pos`), and
    // `derive_initial_position`'s own doc names small-topology position collisions between
    // *distinct* networks as expected, ordinary input this negotiation must handle — exactly
    // what happened live in a real-kernel `two_level_gnode_migrates_as_a_unit_into_merged_network`
    // run: this identity's own untouched network-of-one position coincided numerically with a
    // completely different target network's freshly reserved position, this check fired on
    // that coincidence alone, and the migration this identity had just negotiated was silently
    // dropped forever — while `on_hooking_event`'s caller-side bookkeeping (previously done
    // unconditionally before ever calling this function) had already told every future arc
    // handler this identity belonged to the target network, wedging it: every later arc saw
    // `SameNetwork` against its own now-wrong `network_id()` and exited without retrying.
    // Requiring the target's `network_id` to *also* already match (see
    // [`is_stale_finish_enter_replay`]) is what actually makes this "did this identity really
    // already complete this exact migration", not "do the numbers happen to coincide".
    if is_stale_finish_enter_replay(
        ctx.qspn.my_naddr(),
        ctx.net.network_id(),
        &new_naddr,
        target_network_id,
    ) {
        tracing::debug!(
            ?new_naddr,
            target_network_id,
            "hooking: finish_enter re-proposed the position already held, ignoring stale replay"
        );
        return None;
    }
    let host_gnode_level = ctx.topology.levels();
    if guest_gnode_level >= host_gnode_level {
        tracing::warn!(
            guest_gnode_level,
            host_gnode_level,
            "hooking: finish_enter's guest_gnode_level doesn't span every topology level, \
             staying at the current position (not modeled by this daemon)"
        );
        return None;
    }
    ctx.migration_in_progress = true;
    // Adopt the merged network's id now that this migration is actually committing (moved here
    // from `on_hooking_event`'s caller, which used to set this unconditionally before knowing
    // whether this function would proceed past the guards above — see this function's own
    // "stale replay" doc for the real, reproduced bug that ordering caused) —
    // `QspnViewAdapter::network_id()` reads this, and every subsequent arc handler's
    // same-network/another-network comparison (ntk-hooking's arc_handler) depends on it being
    // current, not the network-of-one id this identity bootstrapped with.
    ctx.net.set_network_id(target_network_id);
    tracing::info!(
        ?new_naddr,
        guest_gnode_level,
        target_network_id,
        "hooking: migrating"
    );

    // -- Fork the current main identity via ntk-identities' real migration machinery, before
    // touching any running generation: `old_id` becomes a connectivity-status fork, `new_id`
    // takes over the main-identity role and is set to a virtual placeholder position until this
    // migration's own bootstrap confirms it real, below. See this function's own "Why not a
    // true concurrent fork" section for what this fork does and does not buy this daemon. --
    let old_id = ctx.identities.main_id();
    let migration_id = random_migration_id();
    if let Err(err) = ctx.identities.prepare_migration(migration_id, old_id).await {
        tracing::warn!(%err, "identities: prepare_migration failed, staying at the current position");
        ctx.migration_in_progress = false;
        return None;
    }
    // `devices` is always empty in this daemon's scope: this daemon runs one physical protocol
    // stack per identity generation, never upstream's per-device pseudo-device/namespace
    // duplication (`ntk_identities::pseudo`'s own doc: "the daemon calls these to know what to
    // name things before calling ntk-netlink to actually create them" — this daemon never does).
    // Every arc's duplication attempt therefore reports `broken` and no peer notification goes
    // out (`ntk_identities::actor::run_migration_duplication`) — a real, reported gap (this
    // function's "Why not a true concurrent fork" section), bounded by `old_id` being retired
    // synchronously, below, the moment this migration's own bootstrap confirms.
    let new_id = match ctx
        .identities
        .migrate(migration_id, old_id, HashMap::new())
        .await
    {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(%err, "identities: migrate failed, staying at the current position");
            ctx.migration_in_progress = false;
            return None;
        }
    };
    if let Err(err) = ctx
        .identities
        .set_naddr(new_id, Some(virtual_placeholder(&ctx.topology)))
        .await
    {
        tracing::warn!(%err, "identities: set_naddr(virtual) on the migrating identity failed");
    }

    // -- tear down the previous generation: cancel its tasks, wait for them, then remove
    // exactly the kernel state it installed — before anything about the new generation touches
    // the kernel, so there is never a moment with both generations' routes installed at once. --
    ctx.generation_cancel.cancel();
    while ctx.generation_tasks.join_next().await.is_some() {}
    if let Err(err) = ctx.route_installer.lock().await.teardown().await {
        tracing::warn!(%err, "route installer: teardown of the previous generation's kernel state failed");
    }

    // -- bootstrap the successor as a real entering identity, positioned at the negotiated
    // address --
    let new_generation_cancel = cancel.child_token();
    let mut new_tasks = JoinSet::new();
    let generation = match bootstrap_generation(
        ctx.topology.clone(),
        new_naddr.clone(),
        QspnOrigin::Entering {
            guest_gnode_level,
            host_gnode_level,
        },
        HookingProvenance::Carried(ctx.hooking.clone()),
        ctx.net.clone(),
        ctx.registry.clone(),
        ctx.links.clone(),
        ctx.my_id,
        KernelHandle(ctx.kernel.clone()),
        &mut new_tasks,
        new_generation_cancel.clone(),
        ctx.signing_key.clone(),
        ctx.require_auth,
    )
    .await
    {
        Ok(generation) => generation,
        Err(err) => {
            tracing::error!(%err, "migrate: bootstrapping the entering generation failed, aborting");
            // The previous generation's actors/kernel state are already torn down above (this
            // was already true before the identity fork existed); reverting the fork here at
            // least leaves the identity registry itself consistent (`old_id` regains Main
            // status, `new_id` is gone) rather than stuck mid-fork with no running protocol
            // stack for either identity — a real, defined-but-degraded state, not a wedge:
            // the next resolved `DoFinishEnter` (hooking's own retry backoff) tries again from
            // `old_id`, exactly as it would have before this identity ever forked.
            if let Err(abort_err) = ctx.identities.abort_migration(old_id, new_id).await {
                tracing::warn!(%abort_err, "identities: abort_migration after a failed entering-generation bootstrap failed");
            }
            ctx.migration_in_progress = false;
            return None;
        }
    };
    ctx.generation_cancel = new_generation_cancel;
    ctx.generation_tasks = new_tasks;

    let new_handles = GenerationHandles::from_generation(&generation, ctx.migrations + 1);
    ctx.qspn = generation.qspn;
    // Captured synchronously inside `bootstrap_generation`, immediately after this generation's
    // `ntk_qspn::spawn_entering` — see that function's doc's "Bug this fixes" section. Subscribing
    // here instead (after `install_identity`'s real netlink round trip above, among other
    // `.await` points) is exactly the race that section documents: this generation's own
    // `BootstrapComplete` could already have fired and been dropped by the time this line runs.
    let new_qspn_events = generation.qspn_events;
    ctx.hooking = generation.hooking;
    ctx.peers = generation.peers;
    let mut new_installer = generation.route_installer;
    reattach_known_arcs(ctx, &mut new_installer).await;
    *ctx.route_installer.lock().await = new_installer;
    // Swap the dispatcher — and everything else external observers read — before waiting on
    // bootstrap: see this function's own "Bug this fixes" doc section for why waiting first
    // deafens this identity to the exact signal it is waiting for.
    ctx.generation_handles_tx.send_replace(new_handles);
    ctx.dispatcher
        .replace_identity_stack(generation.dispatch)
        .await;

    // Wait for the successor's own bootstrap to confirm real connectivity (a qualifying peer
    // ETP, or `QspnConfig::bootstrap_fallback_max_wait`'s fallback) before treating it as hooked.
    // An actor error breaks out the same as a confirmed completion: this identity is as hooked
    // as it is ever going to get either way.
    loop {
        match ctx.qspn.is_bootstrap_complete().await {
            Ok(true) | Err(_) => break,
            Ok(false) => tokio::time::sleep(MIGRATION_POLL_INTERVAL).await,
        }
    }

    // -- Bootstrap confirmed: realize real kernel state (suppressed until now, see
    // `virtual_placeholder`'s doc), then finalize the identity fork. --
    {
        let mut installer = ctx.route_installer.lock().await;
        installer.realize(new_naddr.clone());
        if let Err(err) = installer.install_identity().await {
            tracing::warn!(%err, "route installer: install_identity after migrate failed");
        }
    }
    on_route_snapshot_changed(ctx).await;

    if let Err(err) = ctx.identities.set_naddr(new_id, Some(new_naddr)).await {
        tracing::warn!(%err, "identities: set_naddr(real) after migrate failed");
    }
    // Retire the superseded connectivity fork now that the successor is confirmed — see this
    // function's own "Why not a true concurrent fork" section for why this is synchronous
    // rather than gated on `ntk_qspn::QspnHandle::check_connectivity` (upstream's own retirement
    // gate for a real bridge, inapplicable here).
    if let Err(err) = ctx.identities.remove_identity(old_id).await {
        tracing::warn!(%err, "identities: remove_identity for the superseded connectivity fork failed");
    }

    ctx.migrations += 1;
    ctx.migration_in_progress = false;
    Some(new_qspn_events)
}

/// Poll interval while [`migrate`] waits for the entering successor's own bootstrap to
/// complete — frequent enough not to meaningfully delay the migration, cheap enough to run for
/// the whole (bounded, `QspnConfig::bootstrap_fallback_max_wait`) wait.
const MIGRATION_POLL_INTERVAL: Duration = Duration::from_millis(20);

async fn on_neighborhood_event<K>(ctx: &SteadyStateCtx<K>, event: ntk_neighborhood::Event)
where
    K: SendNetlink + 'static,
{
    match event {
        ntk_neighborhood::Event::ArcAdded(arc) => {
            let link =
                ctx.registry
                    .link_for_neighbour(arc.neighbour_id, &arc.neighbour_mac, &arc.my_dev);
            if ctx.links.get(link).is_none()
                && let Some(client) = ctx
                    .dialer
                    .dial_via(&arc.neighbour_nic_addr, ctx.config_port, Some(&arc.my_dev))
                    .await
            {
                ctx.links.insert(link, client);
            }
            let Some(cost) = arc.cost else { return };
            let add_arc_result = ctx.qspn.add_arc(cost).await;
            tracing::debug!(?link, ?cost, mac = %arc.neighbour_mac, ok = add_arc_result.is_ok(), "qspn: add_arc for newly discovered arc");
            let qspn_arc = match add_arc_result {
                Ok(qspn_arc) => qspn_arc,
                Err(err) => {
                    tracing::warn!(?link, mac = %arc.neighbour_mac, %err, "qspn: add_arc failed, arc will not be routed");
                    return;
                }
            };
            ctx.registry.set_qspn_arc(link, qspn_arc);
            if let Ok(via) = arc.neighbour_nic_addr.parse() {
                ctx.route_installer.lock().await.set_arc_endpoint(
                    qspn_arc,
                    via,
                    Interface::name(&arc.my_dev),
                );
            }
            let _ = ctx
                .identities
                .add_arc(
                    link.identities(),
                    ArcInfo {
                        dev: arc.my_dev.clone(),
                        peer_mac: arc.neighbour_mac.clone(),
                        peer_linklocal: arc.neighbour_nic_addr.clone(),
                    },
                )
                .await;
            let _ = ctx.hooking.add_arc(link.hooking()).await;
            // Re-drives `ntk_peerservices::Handle::register`'s own boot-time flood
            // (`ntkd::node::services::spawn`), which always found zero neighbors: this is the
            // first moment this arc's peer is actually reachable
            // (`ctx.links`/`ctx.registry`/`ctx.qspn` all just updated above), so every other
            // g-node stops treating this node's optional services as non-participant far sooner
            // than `Config::participation_reannounce_interval`'s own steady-state cadence
            // (`services::peers_config`'s own doc). Spawned, never awaited inline: this fans out
            // a real outbound RPC per known neighbor (`ntk_peerservices::gossip::
            // flood_set_participant`), and this function runs inside `run_steady_state`'s own
            // command loop.
            let peers = ctx.peers.clone();
            tokio::spawn(async move {
                peers.reannounce_participation().await;
            });
            on_route_snapshot_changed(ctx).await;
        }
        ntk_neighborhood::Event::ArcCostChanged(arc) => {
            let Some(link) = ctx.registry.link_for_dev_and_mac(&arc.neighbour_mac) else {
                return;
            };
            let Some(qspn_arc) = ctx.registry.qspn_arc_of(link) else {
                return;
            };
            let Some(cost) = arc.cost else { return };
            let _ = ctx.qspn.arc_changed(qspn_arc, cost).await;
            on_route_snapshot_changed(ctx).await;
        }
        ntk_neighborhood::Event::ArcRemoved(arc) => {
            if let Some(entry) = ctx.registry.remove(&arc.neighbour_mac) {
                if let Some(qspn_arc) = entry.qspn_arc {
                    let _ = ctx.qspn.remove_arc(qspn_arc).await;
                    ctx.route_installer
                        .lock()
                        .await
                        .clear_arc_endpoint(qspn_arc);
                }
                let _ = ctx.identities.remove_arc(entry.id.identities()).await;
                let _ = ctx.hooking.remove_arc(entry.id.hooking()).await;
                ctx.links.remove(entry.id);
            }
            on_route_snapshot_changed(ctx).await;
        }
    }
}

async fn on_qspn_event<K>(ctx: &SteadyStateCtx<K>, event: QspnEvent)
where
    K: SendNetlink + 'static,
{
    match event {
        QspnEvent::BootstrapComplete => ctx.net.set_bootstrapped(),
        QspnEvent::DestinationAdded(_)
        | QspnEvent::DestinationRemoved(_)
        | QspnEvent::PathAdded(_)
        | QspnEvent::PathChanged(_)
        | QspnEvent::PathRemoved(_) => on_route_snapshot_changed(ctx).await,
        QspnEvent::GnodeSplitted { destination, .. } => {
            tracing::info!(
                ?destination,
                "qspn: g-node split observed (not further modeled by this daemon)"
            );
        }
        // `ChangedFingerprint` no longer drives any daemon-owned state: the real fingerprint
        // (`ntkd::node::adapters::FingerprintCache`) refreshes itself from
        // `QspnHandle::fingerprint_id` on *every* qspn event, not just this one — see that
        // module's own "Fixed: `fp_id`" doc section.
        QspnEvent::PresenceNotified
        | QspnEvent::ArcRemoved { .. }
        | QspnEvent::ChangedFingerprint(_)
        | QspnEvent::ChangedNodesInside(_) => {}
    }
}

async fn on_route_snapshot_changed<K>(ctx: &SteadyStateCtx<K>)
where
    K: SendNetlink + 'static,
{
    let snapshot = ctx.qspn.snapshot();
    let mut installer = ctx.route_installer.lock().await;
    if let Err(err) = installer.apply(&snapshot).await {
        tracing::warn!(%err, "route installer: apply failed");
    }
}

async fn on_hooking_event<K>(
    ctx: &mut SteadyStateCtx<K>,
    cancel: &CancellationToken,
    event: HookingEvent,
) -> Option<broadcast::Receiver<QspnEvent>>
where
    K: SendNetlink + 'static,
{
    match event {
        HookingEvent::FailingArc(arc) => tracing::warn!(?arc, "hooking: arc failing"),
        HookingEvent::SameNetwork(arc) => tracing::debug!(?arc, "hooking: peer on same network"),
        HookingEvent::AnotherNetwork { arc, network_id } => {
            tracing::info!(
                ?arc,
                network_id,
                "hooking: peer belongs to another network, merge evaluation started"
            );
        }
        HookingEvent::DoPrepareEnter { enter_id } => {
            tracing::debug!(enter_id, "hooking: prepare_enter propagated");
        }
        HookingEvent::DoFinishEnter {
            guest_gnode_level,
            data,
        } => {
            // Network-id adoption used to happen right here, unconditionally, before it was
            // known whether `migrate` would actually proceed past its own guards — see
            // `migrate`'s own "stale replay" doc for the real bug that caused (a position
            // coincidence with an unrelated network wedging this identity while every other arc
            // handler was already told the migration had happened). `migrate` now owns setting
            // `ctx.net`'s network id itself, only once it has confirmed this is a real,
            // non-duplicate migration.
            tracing::debug!(
                guest_gnode_level,
                network_id = data.entry_data.network_id,
                "hooking: finish_enter resolved, requesting migration"
            );
            // `data.entry_data.pos` covers levels `[guest_gnode_level, topology.levels())` only
            // — the levels this negotiation actually resolved (`ChosenAddress`'s own doc:
            // "spans every topology level" happens to describe exactly the `guest_gnode_level
            // == 0` case, a level-0 g-node having exactly one member so there is nothing below
            // it to retain). Levels below `guest_gnode_level` are this identity's own retained
            // internal structure — a merge at `guest_gnode_level` moves the whole g-node, not
            // any one member's place inside it, so every member combines the identical
            // propagated upper levels with its own distinct lower levels: the collective half
            // of coordinated g-node migration (every member ends up sharing the new upper
            // position while keeping its own separate identity below it).
            let current = ctx.qspn.my_naddr().positions();
            let new_naddr = combine_entry_naddr(
                &ctx.topology,
                current,
                guest_gnode_level,
                &data.entry_data.pos,
            );
            match new_naddr {
                Some(new_naddr) => {
                    return migrate(
                        ctx,
                        cancel,
                        guest_gnode_level,
                        new_naddr,
                        data.entry_data.network_id,
                    )
                    .await;
                }
                None => tracing::warn!(
                    guest_gnode_level,
                    entry_pos = ?data.entry_data.pos,
                    current_pos = ?current,
                    "hooking: finish_enter's position doesn't combine into a valid address, staying at the current position"
                ),
            }
        }
        HookingEvent::DoPrepareMigration { migration_id } => {
            let old_id = ctx.identities.main_id();
            let _ = ctx
                .identities
                .prepare_migration(ntk_identities::MigrationId(migration_id), old_id)
                .await;
        }
        HookingEvent::DoFinishMigration { .. } => {
            // See the module doc's scope-boundary note: identity-registry bookkeeping only,
            // no second full protocol stack for the resolved new identity in this pass.
            tracing::info!(
                "hooking: finish_migration notified (identity re-hooking not modeled by this daemon)"
            );
        }
    }
    None
}

/// Deterministic synthetic MAC for `dev` **on this node**, since `ntk_netlink::LinkInfo` carries
/// no hardware address (`ntk-netlink`'s public surface has no accessor for it).
///
/// Folds in `my_id` (this process's [`ntk_neighborhood::NodeId`]) alongside `dev`: hashing the
/// device name alone is not enough, because device names are chosen locally and collide across
/// nodes as a matter of course (every node calling its first NIC `eth0` is the common case, not
/// an edge case) — two nodes would then broadcast the identical "own MAC" on the wire, which
/// `ntk_neighborhood`'s collision detector can only tell apart by `NodeId` in the first place
/// (`Manager::find_collision`), defeating the point of a synthetic address. Mixing in `my_id`
/// makes the result distinct per `(NodeId, dev)` pair while staying deterministic and stable for
/// the same pair across the process's lifetime — hashed with `DefaultHasher` (a fixed
/// algorithm/seed), not `RandomState` (reseeded every process).
///
/// The output's first octet always has bit 1 (`0x02`, locally-administered) set and bit 0
/// (`0x01`, multicast/group) cleared, per IEEE 802's MAC bit layout, so the result is always a
/// valid locally-administered *unicast* address regardless of the hash bits landing there —
/// it never collides with a real vendor-assigned (globally-administered) MAC and is never
/// mistaken for a multicast/broadcast address.
#[must_use]
pub fn synthetic_mac(dev: &str, my_id: ntk_neighborhood::NodeId) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    my_id.hash(&mut hasher);
    dev.hash(&mut hasher);
    let b = hasher.finish().to_be_bytes();
    let first = (b[0] & 0xfc) | 0x02;
    format!(
        "{first:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        b[1], b[2], b[3], b[4], b[5]
    )
}

#[cfg(test)]
mod synthetic_mac_tests {
    use super::synthetic_mac;
    use ntk_neighborhood::NodeId;

    fn assert_locally_administered_unicast(mac: &str) {
        let first = u8::from_str_radix(mac.split(':').next().unwrap(), 16).unwrap();
        assert_eq!(
            first & 0x02,
            0x02,
            "{mac}: locally-administered bit not set"
        );
        assert_eq!(first & 0x01, 0, "{mac}: multicast bit not clear");
        assert_eq!(mac.split(':').count(), 6, "{mac}: not 6 octets");
    }

    #[test]
    fn different_node_ids_diverge_on_the_same_device_name() {
        // The exact production case that was broken: two nodes both naming an interface
        // `eth0` must not broadcast the same synthetic MAC.
        let a = synthetic_mac("eth0", NodeId::from_raw(1).unwrap());
        let b = synthetic_mac("eth0", NodeId::from_raw(2).unwrap());
        assert_ne!(a, b);
    }

    #[test]
    fn same_node_and_device_is_deterministic() {
        let id = NodeId::from_raw(42).unwrap();
        assert_eq!(synthetic_mac("eth0", id), synthetic_mac("eth0", id));
    }

    #[test]
    fn different_devices_on_the_same_node_diverge() {
        let id = NodeId::from_raw(7).unwrap();
        assert_ne!(synthetic_mac("eth0", id), synthetic_mac("eth1", id));
    }

    #[test]
    fn result_is_always_a_valid_locally_administered_unicast_mac() {
        for raw in [1, 2, 3, 42, i32::MAX] {
            for dev in ["eth0", "wlan0", "enp0s3"] {
                assert_locally_administered_unicast(&synthetic_mac(
                    dev,
                    NodeId::from_raw(raw).unwrap(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod linklocal_allocator_tests {
    use super::{LINKLOCAL_SPACE, derive_linklocal};
    use ntk_neighborhood::NodeId;
    use std::net::Ipv4Addr;

    fn assert_valid_linklocal(addr: Ipv4Addr) {
        let o = addr.octets();
        assert_eq!(&o[0..2], [169, 254], "{addr}: not inside 169.254.0.0/16");
        assert_ne!(o[2], 0, "{addr}: inside reserved 169.254.0.0/24");
        assert_ne!(o[2], 255, "{addr}: inside reserved 169.254.255.0/24");
    }

    #[test]
    fn different_node_ids_diverge_on_the_same_nic_index() {
        // The exact production case that was broken: two freshly started daemons' first
        // (index 0) NIC must not self-assign the same address.
        let a = derive_linklocal(NodeId::from_raw(1).unwrap(), 0);
        let b = derive_linklocal(NodeId::from_raw(2).unwrap(), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn same_salt_and_index_is_deterministic() {
        let id = NodeId::from_raw(42).unwrap();
        assert_eq!(derive_linklocal(id, 3), derive_linklocal(id, 3));
    }

    #[test]
    fn different_indices_on_the_same_node_diverge() {
        // Multiple NICs on one node must still get distinct addresses.
        let id = NodeId::from_raw(7).unwrap();
        assert_ne!(derive_linklocal(id, 0), derive_linklocal(id, 1));
        assert_ne!(derive_linklocal(id, 0), derive_linklocal(id, 41));
    }

    #[test]
    fn every_generated_address_stays_inside_the_valid_linklocal_range() {
        for raw in [1, 2, 3, 42, i32::MAX] {
            let id = NodeId::from_raw(raw).unwrap();
            for index in [0, 1, 2, 100, LINKLOCAL_SPACE - 1] {
                assert_valid_linklocal(derive_linklocal(id, index));
            }
        }
    }
}

#[cfg(test)]
mod combine_entry_naddr_tests {
    use super::combine_entry_naddr;
    use ntk_common::Topology;

    fn topo() -> Topology {
        Topology::new([4, 2]).expect("valid topology")
    }

    /// `guest_gnode_level == 0`: the single-member case must combine to exactly `entry_pos`,
    /// byte-for-byte what [`on_hooking_event`]'s `DoFinishEnter` arm built before this function
    /// existed — the module doc's "gated implicitly, not by a level check" claim.
    #[test]
    fn level_zero_is_the_entry_pos_verbatim_regardless_of_current_position() {
        for current in [[0, 0], [3, 1], [2, 0]] {
            let naddr = combine_entry_naddr(&topo(), &current, 0, &[1, 0])
                .expect("full-length entry_pos at level 0 is always valid");
            assert_eq!(naddr.positions(), [1, 0]);
        }
    }

    /// The defining property: three siblings with three different retained level-0 positions,
    /// all handed the identical propagated level-1 target, must end up sharing that exact upper
    /// position while keeping their own separate identity below it — never collapsing onto one
    /// another, never drifting onto three different upper positions.
    #[test]
    fn three_members_with_distinct_lower_positions_share_one_propagated_upper_position() {
        let entry_pos = [1]; // the negotiated new level-1 position, shared by the whole g-node
        let members = [
            combine_entry_naddr(&topo(), &[0, 0], 1, &entry_pos).expect("member 0 combines"),
            combine_entry_naddr(&topo(), &[1, 0], 1, &entry_pos).expect("member 1 combines"),
            combine_entry_naddr(&topo(), &[2, 0], 1, &entry_pos).expect("member 2 combines"),
        ];
        // Shared upper level (index 1): every member adopted the identical propagated target.
        assert!(members.iter().all(|n| n.positions()[1] == 1));
        // Distinct lower level (index 0): every member kept its own separate identity.
        let level0: std::collections::HashSet<u32> =
            members.iter().map(|n| n.positions()[0]).collect();
        assert_eq!(level0, std::collections::HashSet::from([0, 1, 2]));
        // And every member landed at an overall distinct address.
        let distinct: std::collections::HashSet<&[u32]> =
            members.iter().map(ntk_common::Naddr::positions).collect();
        assert_eq!(distinct.len(), 3);
    }

    /// `guest_gnode_level` at or beyond how many positions this identity actually holds cannot
    /// be sliced into a prefix — must report "not combinable", never panic or silently truncate.
    #[test]
    fn out_of_range_guest_gnode_level_is_rejected_not_panicked() {
        assert!(combine_entry_naddr(&topo(), &[0, 0], 3, &[]).is_none());
    }

    /// A combined vector whose length doesn't match the topology (a malformed or
    /// unexpectedly-shaped propagation) must be rejected, never truncated or padded.
    #[test]
    fn wrong_total_length_is_rejected() {
        assert!(combine_entry_naddr(&topo(), &[0, 0], 1, &[1, 0]).is_none());
        assert!(combine_entry_naddr(&topo(), &[0, 0], 1, &[]).is_none());
    }

    /// A propagated position outside its level's g-node size is invalid data, not a valid
    /// (if unusual) address — [`ntk_common::Naddr::new`]'s own validation, exercised through
    /// this combinator.
    #[test]
    fn out_of_bounds_position_is_rejected() {
        // gsize(1) == 2, so position 5 at level 1 is out of range.
        assert!(combine_entry_naddr(&topo(), &[0, 0], 1, &[5]).is_none());
    }
}

#[cfg(test)]
mod is_stale_finish_enter_replay_tests {
    use super::is_stale_finish_enter_replay;
    use ntk_common::{Naddr, Topology};

    fn topo() -> Topology {
        Topology::new([4, 2]).expect("valid topology")
    }

    fn naddr(pos: [u32; 2]) -> Naddr {
        Naddr::new(topo(), pos.to_vec()).expect("valid address")
    }

    /// The genuine idempotency case: same position, same network — a real re-delivery of an
    /// already-applied propagation, which must be dropped.
    #[test]
    fn same_position_and_network_is_stale() {
        assert!(is_stale_finish_enter_replay(
            &naddr([1, 0]),
            42,
            &naddr([1, 0]),
            42
        ));
    }

    /// The regression this pins: two *distinct* networks whose positions numerically coincide
    /// — real-kernel evidence (`migrate`'s own doc) — must never be treated as a stale replay.
    /// Position-only comparison previously silently dropped a fully negotiated migration here.
    #[test]
    fn same_position_different_network_is_not_stale() {
        assert!(!is_stale_finish_enter_replay(
            &naddr([1, 0]),
            42,
            &naddr([1, 0]),
            99
        ));
    }

    /// The symmetric case: same network already adopted, but a different (newer) position —
    /// e.g. a second, later merge for the same g-node — must also proceed, not be dropped.
    #[test]
    fn different_position_same_network_is_not_stale() {
        assert!(!is_stale_finish_enter_replay(
            &naddr([1, 0]),
            42,
            &naddr([2, 0]),
            42
        ));
    }

    /// Neither position nor network match: an ordinary, fresh migration.
    #[test]
    fn different_position_and_network_is_not_stale() {
        assert!(!is_stale_finish_enter_replay(
            &naddr([1, 0]),
            42,
            &naddr([2, 0]),
            99
        ));
    }
}
