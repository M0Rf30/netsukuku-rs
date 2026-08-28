//! Shared real-kernel N-node mesh fixture for `tests/mesh.rs`, generalizing the two-node
//! technique proven in `tests/multi_node.rs` (see that file's own "Scenario 3" doc comments for
//! the full rationale, which this module does not repeat): one dedicated `std::thread` per
//! network namespace, unshared once and then driving its own `current_thread` tokio runtime for
//! its whole life; raw `rtnetlink` for the two link-identity/state primitives `ntk_netlink`
//! deliberately has no API for; a rendezvous before any namespace is reclaimed; assertions read
//! back through an independent [`ntk_netlink::RealNetlink`] connection, never the daemon's own
//! snapshot.
//!
//! # Radio-agnostic by construction
//! [`NamespaceWorker`] (the pinned-thread-per-namespace primitive), [`spawn_node`] (the real
//! `ntkd` composition), and [`observe`]/[`NodeReport`] (the independent-`RealNetlink` report)
//! know nothing about *how* a node's devices got wired up — that is entirely the caller's job,
//! performed on the un-unshared coordinator thread before calling [`NamespaceWorker::fd`]/
//! [`NamespaceWorker::signal_moved`]. This module's own answer, [`Segment`]/[`wire`], realises a
//! shared broadcast domain as a Linux bridge with one veth per member. `tests/wireless.rs`
//! substitutes `mac80211_hwsim` radios for that same slot without touching anything else here.

use std::collections::HashMap;
use std::future::Future;
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::TryStreamExt;
use ntk_common::Cost;
use ntk_neighborhood::{
    Arc as NeighborArc, FixedRttProbe, LocalNic, NeighborhoodConfig, NeighborhoodRpcHandler,
    NeighborhoodTiming, NodeId,
};
use ntk_netlink::{AddressEntry, AddressTable, RouteSpec, RouteTable};
use ntkd::kernel::config::NtkdConfig;
use ntkd::node::lifecycle::{self, NodeInputs, PreformedNetwork, StartedNode};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

/// The fixed RTT every scenario's [`FixedRttProbe`] reports, chosen (not zero) so a converged
/// path's cost accumulates hop-by-hop and is therefore a real, distinguishing assertion —
/// matching the proven per-hop value `tests/multi_node.rs`'s own `chain_converges_...` scenario
/// uses over `FakeNetlink`, now over a real kernel/transport.
pub const RTT_MS: u64 = 10;

/// Budget for [`teardown`]'s own [`ntkd::node::supervisor::drain_tasks`] call — the same value
/// production shutdown uses (`ntkd::node::supervisor::SHUTDOWN_DRAIN_TIMEOUT`'s own doc: one
/// `HookingConfig::default()` restart-from-start backoff floor plus margin), since a test
/// identity's actors back off on the identical schedule as a production one.
pub const TEARDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Margin [`NamespaceWorker::join`] callers add on top of a scenario's own worst-case internal
/// budget (its `wait_until`/barrier timeouts, already accounted for in the scenario's own
/// constants) to size the *outer* join timeout: [`TEARDOWN_DRAIN_TIMEOUT`] plus
/// `ntkd::node::supervisor::ABORT_REAP_WINDOW`-scale slack for `teardown` itself, plus flat
/// scheduling margin — this file's own established convention (every barrier/rendezvous timeout
/// here already adds a flat margin over its own predicate's budget).
pub const JOIN_MARGIN: Duration = Duration::from_secs(60);

// -------------------------------------------------------------------------------------------
// Raw rtnetlink link plumbing — the two primitives `ntk_netlink` has no API for (link identity/
// creation/state), plus bridge/veth wiring. See `tests/multi_node.rs`'s own "Scenario 3" doc
// comment for why nothing here shells out to `ip`/`bridge`.
// -------------------------------------------------------------------------------------------
pub mod link {
    use super::{Context, RawFd, TryStreamExt};

    /// Resolves `name` to its current kernel `ifindex` via a raw link dump.
    pub async fn index(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<u32> {
        handle
            .link()
            .get()
            .match_name(name.to_owned())
            .execute()
            .try_next()
            .await
            .with_context(|| format!("resolving link {name:?}"))?
            .map(|link| link.header.index)
            .ok_or_else(|| anyhow::anyhow!("link {name:?} not found"))
    }

    /// `ip link set <name> up`, natively (see `tests/multi_node.rs`'s `bring_link_up` doc for why
    /// `LinkUnspec::change`, not `set`). Returns the link's resolved `ifindex`.
    pub async fn up(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<u32> {
        let index = self::index(handle, name).await?;
        handle
            .link()
            .change(rtnetlink::LinkUnspec::new_with_index(index).up().build())
            .execute()
            .await
            .with_context(|| format!("bringing {name:?} up"))?;
        Ok(index)
    }

    /// `ip link set <name> down`.
    pub async fn down(handle: &rtnetlink::Handle, index: u32) -> anyhow::Result<()> {
        handle
            .link()
            .change(rtnetlink::LinkUnspec::new_with_index(index).down().build())
            .execute()
            .await
            .with_context(|| format!("bringing ifindex {index} down"))
    }

    /// `ip link add <a> type veth peer name <b>`.
    pub async fn create_veth(handle: &rtnetlink::Handle, a: &str, b: &str) -> anyhow::Result<()> {
        handle
            .link()
            .add(rtnetlink::LinkVeth::new(a, b).build())
            .execute()
            .await
            .with_context(|| format!("creating veth pair {a:?}/{b:?}"))
    }

    /// `ip link add <name> type bridge` (created already up, matching `LinkBridge::new`'s own
    /// contract). Returns the bridge's resolved `ifindex`.
    pub async fn create_bridge(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<u32> {
        handle
            .link()
            .add(rtnetlink::LinkBridge::new(name).build())
            .execute()
            .await
            .with_context(|| format!("creating bridge {name:?}"))?;
        self::index(handle, name).await
    }

    /// `ip link set <port> master <bridge>` plus bringing the port up — a bridge port carries no
    /// traffic administratively down.
    pub async fn attach_to_bridge(
        handle: &rtnetlink::Handle,
        port: &str,
        bridge_index: u32,
    ) -> anyhow::Result<u32> {
        let port_index = self::index(handle, port).await?;
        handle
            .link()
            .change(
                rtnetlink::LinkUnspec::new_with_index(port_index)
                    .controller(bridge_index)
                    .up()
                    .build(),
            )
            .execute()
            .await
            .with_context(|| format!("attaching {port:?} to bridge ifindex {bridge_index}"))?;
        Ok(port_index)
    }

    /// Moves `name` into the network namespace identified by `fd` (`IFLA_NET_NS_FD`).
    pub async fn move_to_ns(
        handle: &rtnetlink::Handle,
        name: &str,
        fd: RawFd,
    ) -> anyhow::Result<()> {
        let index = self::index(handle, name).await?;
        handle
            .link()
            .change(
                rtnetlink::LinkUnspec::new_with_index(index)
                    .setns_by_fd(fd)
                    .build(),
            )
            .execute()
            .await
            .with_context(|| format!("moving {name:?} into namespace"))
    }

    /// `ip link delete <ifindex>` — deletes a link outright: for a veth, both ends of the pair
    /// disappear together (standard veth-pair semantics); for a bridge, the bridge device
    /// itself is removed (its ports are detached, not deleted).
    pub async fn delete(handle: &rtnetlink::Handle, index: u32) -> anyhow::Result<()> {
        handle
            .link()
            .del(index)
            .execute()
            .await
            .with_context(|| format!("deleting link ifindex {index}"))
    }
}

/// Opens a real `NETLINK_ROUTE` connection and returns both the handle and an owned
/// [`tokio::task::JoinHandle`] for its background I/O driver task, instead of spawning it and
/// discarding the handle — a bare `tokio::spawn`-and-forget lets a driver panic pass silently
/// (`AGENTS.md`'s own documented case for why an unjoined task is a defect class, not a style
/// nit) and gives nothing a caller can wait on for deterministic shutdown.
///
/// Dropping every clone of the returned `Handle` closes the driver's own request channel, which
/// lets its future finish on its own (checked directly by this module's own
/// `root_connection_driver_exits_once_its_handle_is_dropped` test) — so the returned
/// `JoinHandle` is genuinely joinable, not decorative: [`teardown_mesh`] drops the handle, then
/// awaits this join, bounded, exactly like every other task this harness manages.
pub fn root_handle_with_driver() -> anyhow::Result<(rtnetlink::Handle, tokio::task::JoinHandle<()>)>
{
    let (connection, handle, _) = rtnetlink::new_connection().context("rtnetlink connection")?;
    Ok((handle, tokio::spawn(connection)))
}

/// Opens a real `NETLINK_ROUTE` connection and spawns its background I/O driver onto the
/// caller's current runtime — the coordinator's own handle, used for every link-plumbing call
/// that must run in the root (un-unshared) test namespace: creating bridges/veths, moving veth
/// ends into node namespaces, and later severing/healing a segment.
///
/// A thin, driver-discarding wrapper over [`root_handle_with_driver`] for callers whose own
/// connection is already scoped to a single throwaway per-test-function runtime: dropping a
/// `tokio::Runtime` cancels every task spawned on it, this driver included, so nothing here
/// outlives that runtime's own life regardless of whether its `JoinHandle` was kept. A
/// multi-scenario-per-process suite can't rely on "this test's own runtime" as a concept, so
/// [`teardown_mesh`] uses [`root_handle_with_driver`] directly instead of this wrapper.
///
/// `#[allow(dead_code)]`: this module is compiled separately into every integration-test binary
/// that declares it, so a helper only some of them use is genuinely dead in the others.
/// `wireless.rs` and `andna_e2e.rs` call this; `mesh.rs` uses [`root_handle_with_driver`]
/// throughout, so the `mesh` target alone would fail `-D dead-code` without this.
#[allow(dead_code)]
pub fn root_handle() -> anyhow::Result<rtnetlink::Handle> {
    Ok(root_handle_with_driver()?.0)
}

// -------------------------------------------------------------------------------------------
// Segments: named broadcast domains realised as a bridge + one veth per member.
// -------------------------------------------------------------------------------------------

/// One member of a [`Segment`]: node `node` (an index into the caller's own node list) attaches
/// via a device it will call `dev` inside its own namespace.
#[derive(Clone, Copy)]
pub struct Member {
    pub node: usize,
    pub dev: &'static str,
}

/// A named broadcast domain: one Linux bridge, living in the coordinator's own (root) network
/// namespace, with one veth pair per member — a bridge is a faithful stand-in for a shared
/// medium (this module's own doc comment explains why this exact slot is what `tests/
/// wireless.rs` swaps out). Keep `name` short: the root-side port for member `i` is named
/// `"{name}p{i}"`, and Linux interface names are capped at 15 characters.
pub struct Segment {
    pub name: &'static str,
    pub members: Vec<Member>,
}

/// A fully-wired set of segments: every bridge is up, every member's veth end has been moved
/// into that member's namespace (left down — each node's own worker brings its own devices up,
/// matching `tests/multi_node.rs`'s convention), and every segment's root-side port indices
/// (plus each segment's own bridge `ifindex`) are recorded for later [`sever`]/[`link_bridges`]/
/// [`WiredMesh::teardown`].
pub struct WiredMesh {
    ports: HashMap<&'static str, Vec<u32>>,
    bridges: HashMap<&'static str, u32>,
    /// Root-namespace-only veth pairs [`link_bridges`] created. Unlike a member port (whose
    /// node-side end lives inside a [`NamespaceWorker`]'s own netns and is destroyed
    /// automatically — peer included, standard veth-pair semantics — the moment that namespace
    /// is reclaimed), an uplink's *both* ends live in the coordinator's own namespace for the
    /// whole test and have no namespace teardown to ride along with; one ifindex per uplink is
    /// enough since deleting either end of a veth pair deletes both. Drained by
    /// [`WiredMesh::teardown`].
    uplinks: Vec<u32>,
}

/// Wires every segment in `segments`: for each member, creates a veth pair (node-side `dev`,
/// root-side `"{seg}p{i}"`), attaches the root side to that segment's bridge, and moves the
/// node side into `node_fds[member.node]`. Call once, from the coordinator (root-namespace)
/// thread, after every [`NamespaceWorker::fd`] has been collected and before any
/// [`NamespaceWorker::signal_moved`].
pub async fn wire(
    handle: &rtnetlink::Handle,
    segments: &[Segment],
    node_fds: &[RawFd],
) -> anyhow::Result<WiredMesh> {
    let mut ports = HashMap::new();
    let mut bridges = HashMap::new();
    for seg in segments {
        let bridge_name = format!("br-{}", seg.name);
        let bridge_index = link::create_bridge(handle, &bridge_name)
            .await
            .with_context(|| format!("segment {:?}: create bridge", seg.name))?;
        let mut port_indices = Vec::with_capacity(seg.members.len());
        for (i, member) in seg.members.iter().enumerate() {
            let root_port = format!("{}p{i}", seg.name);
            link::create_veth(handle, member.dev, &root_port)
                .await
                .with_context(|| format!("segment {:?}: create veth for member {i}", seg.name))?;
            let port_index = link::attach_to_bridge(handle, &root_port, bridge_index)
                .await
                .with_context(|| format!("segment {:?}: attach member {i}", seg.name))?;
            link::move_to_ns(handle, member.dev, node_fds[member.node])
                .await
                .with_context(|| format!("segment {:?}: move member {i} in", seg.name))?;
            port_indices.push(port_index);
        }
        ports.insert(seg.name, port_indices);
        bridges.insert(seg.name, bridge_index);
    }
    Ok(WiredMesh {
        ports,
        bridges,
        uplinks: Vec::new(),
    })
}

impl WiredMesh {
    /// Severs a segment (or a [`Self::link_bridges`] uplink, named the same way) by bringing
    /// every one of its root-side ports down — equivalent to a switch/cable failure:
    /// administratively down, so no frame is forwarded in either direction, without touching
    /// (or risking a shared-deletion cascade on) any veth pair.
    pub async fn sever(&self, handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<()> {
        let ports = self
            .ports
            .get(name)
            .with_context(|| format!("unknown segment {name:?}"))?;
        for &index in ports {
            link::down(handle, index)
                .await
                .with_context(|| format!("segment {name:?}: sever port ifindex {index}"))?;
        }
        Ok(())
    }

    /// Merges two segments' broadcast domains into one by attaching a veth pair directly
    /// between their two bridges — an L2 uplink belonging to neither node, radio-agnostic in
    /// the same sense as [`wire`] itself (this crate's own answer to "join two domains without
    /// giving any node a second NIC"). `name` becomes a severable pseudo-segment: [`Self::sever`]
    /// brings this exact uplink down, splitting the two domains apart again without touching
    /// either side's own member ports.
    pub async fn link_bridges(
        &mut self,
        handle: &rtnetlink::Handle,
        a: &str,
        b: &str,
        name: &'static str,
    ) -> anyhow::Result<()> {
        let bridge_a = *self
            .bridges
            .get(a)
            .with_context(|| format!("unknown segment {a:?}"))?;
        let bridge_b = *self
            .bridges
            .get(b)
            .with_context(|| format!("unknown segment {b:?}"))?;
        let end_a = format!("{name}a");
        let end_b = format!("{name}b");
        link::create_veth(handle, &end_a, &end_b)
            .await
            .with_context(|| format!("uplink {name:?}: create veth"))?;
        let index_a = link::attach_to_bridge(handle, &end_a, bridge_a)
            .await
            .with_context(|| format!("uplink {name:?}: attach to {a:?}"))?;
        let index_b = link::attach_to_bridge(handle, &end_b, bridge_b)
            .await
            .with_context(|| format!("uplink {name:?}: attach to {b:?}"))?;
        self.ports.insert(name, vec![index_a, index_b]);
        self.uplinks.push(index_a);
        Ok(())
    }

    /// Explicit, deterministic teardown for everything [`wire`]/[`Self::link_bridges`] created
    /// directly in the coordinator's own (root) namespace: every uplink veth, plus every bridge
    /// device. Matches this module's own call-it-explicitly-not-`Drop` discipline
    /// ([`teardown`]'s own doc: `Drop` cannot `.await`, and deleting a link is a netlink round
    /// trip). A per-member port veth needs no entry here — this struct's own `uplinks` field
    /// doc explains why — so call this only after every one of this mesh's own
    /// [`NamespaceWorker::join`] calls has already returned, which is exactly when that becomes
    /// true. Logs (never panics on) an individual deletion failure, matching [`teardown`]'s own
    /// best-effort convention for cleanup-time errors: by the time this runs, the scenario's
    /// actual assertions have already had their chance to fail loudly.
    pub async fn teardown(&self, handle: &rtnetlink::Handle) {
        for &index in &self.uplinks {
            if let Err(err) = link::delete(handle, index).await {
                tracing::warn!(%err, index, "WiredMesh::teardown: uplink veth");
            }
        }
        for (&name, &index) in &self.bridges {
            if let Err(err) = link::delete(handle, index).await {
                tracing::warn!(%err, %name, index, "WiredMesh::teardown: bridge");
            }
        }
    }
}

/// Budget for [`teardown_mesh`]'s own join of the root connection driver, after dropping the
/// `rtnetlink::Handle` that was its only reason to keep running: closing that channel is
/// immediate, and everything before it ([`WiredMesh::teardown`]'s own bridge/uplink deletions)
/// is a handful of point-to-point netlink round trips — generous, not tight, margin over both.
pub const ROOT_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Bundled, bounded teardown for a mesh coordinator's own root-namespace resources: every
/// bridge/uplink [`wire`]/[`WiredMesh::link_bridges`] created, plus the
/// [`root_handle_with_driver`] connection driver, whose lifetime otherwise outlives the
/// `rtnetlink::Handle` used to build them. Call once, from the coordinator thread, only after
/// every one of this mesh's own [`NamespaceWorker::join`] calls has already returned — see
/// [`WiredMesh::teardown`]'s own doc for why that ordering matters. A driver that panics, or
/// fails to notice its handle was dropped within [`ROOT_TEARDOWN_TIMEOUT`], fails the calling
/// test outright, matching every other bounded join in this module
/// ([`NamespaceWorker::join`]'s own convention) instead of letting a wedged or panicking
/// background task go unnoticed the way a bare `tokio::spawn` would.
pub async fn teardown_mesh(mesh: WiredMesh, handle: rtnetlink::Handle, driver: JoinHandle<()>) {
    mesh.teardown(&handle).await;
    drop(handle);
    match tokio::time::timeout(ROOT_TEARDOWN_TIMEOUT, driver).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("root rtnetlink connection driver panicked: {err}"),
        Err(_) => panic!(
            "root rtnetlink connection driver did not exit within {ROOT_TEARDOWN_TIMEOUT:?} of \
             its handle being dropped — every WiredMesh bridge/uplink is still deleted, but the \
             driver task itself would otherwise leak for the rest of this process"
        ),
    }
}

// -------------------------------------------------------------------------------------------
// NamespaceWorker: one namespace's entire life, on its own dedicated OS thread.
// -------------------------------------------------------------------------------------------

/// One namespace's entire life, on its own dedicated `std::thread` — see this module's own doc
/// comment for why (one `unshare`, one `current_thread` runtime, for the thread's whole life).
/// The coordinator drives it through exactly three steps: [`Self::fd`] (move links in),
/// [`Self::signal_moved`] (release the worker to proceed), [`Self::join`] (collect the result).
pub struct NamespaceWorker<T> {
    label: String,
    thread: std::thread::JoinHandle<()>,
    fd_rx: std::sync::mpsc::Receiver<RawFd>,
    moved_tx: std::sync::mpsc::Sender<()>,
    result_rx: tokio::sync::oneshot::Receiver<anyhow::Result<T>>,
}

impl<T: Send + 'static> NamespaceWorker<T> {
    /// Spawns the thread: `unshare(CLONE_NEWNET)`, hand the coordinator this namespace's fd,
    /// wait for the coordinator's go-ahead, then build a `current_thread` runtime and drive
    /// `body` to completion on it — entirely on this one pinned thread, for the reasons
    /// `tests/multi_node.rs`'s own "Scenario 3" doc comment documents in full.
    pub fn spawn<F, Fut>(label: impl Into<String>, body: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        let label = label.into();
        let (fd_tx, fd_rx) = std::sync::mpsc::channel();
        let (moved_tx, moved_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let thread_label = label.clone();
        let thread = std::thread::Builder::new()
            .name(thread_label.clone())
            .spawn(move || {
                let outcome = (|| -> anyhow::Result<T> {
                    use std::os::fd::AsRawFd;

                    nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET).map_err(|errno| {
                        anyhow::anyhow!("{thread_label}: unshare(CLONE_NEWNET): {errno}")
                    })?;
                    // Held until this closure returns — see `tests/multi_node.rs`'s
                    // `run_namespace_worker` doc for why this fd must stay open for both the
                    // coordinator's `setns_by_fd` call and this namespace's own lifetime.
                    let ns_file = std::fs::File::open("/proc/thread-self/ns/net")
                        .with_context(|| format!("{thread_label}: open own netns fd"))?;
                    fd_tx.send(ns_file.as_raw_fd()).map_err(|_| {
                        anyhow::anyhow!("{thread_label}: coordinator dropped fd channel")
                    })?;
                    moved_rx.recv().map_err(|_| {
                        anyhow::anyhow!("{thread_label}: coordinator dropped moved signal")
                    })?;

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .with_context(|| format!("{thread_label}: build current-thread runtime"))?;
                    let result = rt.block_on(body());
                    drop(ns_file);
                    result
                })();
                let _ = result_tx.send(outcome);
            })
            .expect("spawn namespace worker thread");
        Self {
            label,
            thread,
            fd_rx,
            moved_tx,
            result_rx,
        }
    }

    /// Blocks (briefly — the worker sends within microseconds of starting, and nothing else is
    /// scheduled on the coordinator's runtime at this point, matching `tests/multi_node.rs`'s own
    /// call site) until this namespace's own netns fd is available.
    pub fn fd(&self) -> RawFd {
        self.fd_rx
            .recv()
            .unwrap_or_else(|_| panic!("{}: worker dropped fd channel", self.label))
    }

    /// Releases the worker to build its runtime and start `body` — call once every link this
    /// namespace needs has been moved in.
    pub fn signal_moved(&self) {
        let _ = self.moved_tx.send(());
    }

    /// Awaits `body`'s result and joins the underlying thread, bounded by `timeout`.
    ///
    /// A worker whose own single-threaded runtime is captured by a task that never yields
    /// control back to it (`teardown`'s own doc: a real-kernel capture of
    /// `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` found exactly this in
    /// `ntk_peerservices::routing::Handle::relay`) can never finish `body()` — `result_rx`
    /// would then wait forever, since nothing on that captured thread can ever send on it, and
    /// `teardown`'s own internal bound (running *inside* `body()`, on the very thread that's
    /// wedged) cannot rescue it either. This method's `timeout` is the *outer*, only-still-live
    /// backstop: it runs on the coordinator's own (separate) runtime, so it keeps working
    /// regardless of what's happened to the worker's thread.
    ///
    /// On timeout, the underlying `std::thread::JoinHandle` is simply dropped (never
    /// `std::thread::JoinHandle::join`-ed, which would block just as unboundedly) rather than
    /// waited on further — the thread may keep running in the background, but the OS reclaims it
    /// at process exit regardless, and the caller gets a fast, clear per-node failure instead of
    /// a whole-test hang measured in minutes.
    pub async fn join(self, timeout: Duration) -> anyhow::Result<T> {
        let Self {
            label,
            thread,
            fd_rx: _,
            moved_tx: _,
            result_rx,
        } = self;
        match tokio::time::timeout(timeout, result_rx).await {
            Ok(Ok(result)) => {
                thread
                    .join()
                    .unwrap_or_else(|e| panic!("{label}: worker thread panicked: {e:?}"));
                result
            }
            Ok(Err(_)) => Err(anyhow::anyhow!("{label}: result channel")),
            Err(_) => Err(anyhow::anyhow!(
                "{label}: did not finish within {timeout:?} — its own single-threaded runtime is \
                 likely wedged by a task that never yields control back to it (see this \
                 method's own doc); abandoning its thread rather than hanging the whole test"
            )),
        }
    }
}

/// Joins every worker unconditionally, even once an earlier one has already failed — never
/// short-circuits partway through the list.
///
/// A caller that instead panics inside its own join loop (this suite's original shape, on every
/// scenario) abandons every `NamespaceWorker` from that point on without ever awaiting it: the
/// underlying OS thread and its `current_thread` tokio runtime (radar/arc-monitor polling, UDP
/// broadcast, TCP server) keep running for up to that worker's own timeout budget — tens to
/// hundreds of seconds — while `cargo test`, with the panic already unwound, has moved on to the
/// *next* test in the same process. That is real, unbounded-looking cross-test CPU/network
/// contention self-inflicted by the harness, not the daemon: confirmed live by
/// `partition_clean_severance_drops_exactly_the_unreachable_destinations`, whose own first
/// `ensure!` (this file's `severance_worker_body`) fails on every observed run — via the old
/// per-worker-panic loop, its three still-converged siblings' threads (each polling every
/// 20-200ms) ran to completion in the background, unjoined, well past this scenario's own test
/// function returning.
///
/// Joining every worker first — and only then letting the caller decide whether to panic, always
/// after [`teardown_mesh`] has run — bounds every worker's lifetime to its own declared timeout
/// and guarantees the root-namespace mesh (bridges/veths) this scenario created is reclaimed
/// before the test function returns, pass or fail.
pub async fn join_all<T: Send + 'static>(
    workers: Vec<NamespaceWorker<T>>,
    timeout: Duration,
) -> Vec<anyhow::Result<T>> {
    let mut results = Vec::with_capacity(workers.len());
    for w in workers {
        results.push(w.join(timeout).await);
    }
    results
}

// -------------------------------------------------------------------------------------------
// Node composition + report — run inside a `NamespaceWorker`'s `body`.
// -------------------------------------------------------------------------------------------

/// Brings `lo` and every one of `devs` up (freshly created namespaces start with only a down
/// `lo`, and `ntk_neighborhood::Handle::start_monitor` refuses a down interface) using a fresh
/// rtnetlink connection scoped to the calling (already-`unshare`d) thread's namespace. Returns
/// each device's resolved `ifindex`, keyed by name — needed later to identify which of a node's
/// several linklocal addresses belongs to which device (see [`NodeReport::linklocal`]).
pub async fn bring_up_devs(devs: &[&str]) -> anyhow::Result<HashMap<String, u32>> {
    let (connection, handle, _) = rtnetlink::new_connection().context("rtnetlink connection")?;
    tokio::spawn(connection);
    link::up(&handle, "lo").await.context("bring lo up")?;
    let mut indices = HashMap::with_capacity(devs.len());
    for &dev in devs {
        let index = link::up(&handle, dev)
            .await
            .with_context(|| format!("bring {dev:?} up"))?;
        indices.insert(dev.to_owned(), index);
    }
    Ok(indices)
}

/// Composes one real `ntkd` node against the real kernel over every device in `devs` — the same
/// production wiring `tests/multi_node.rs`'s `spawn_real_node` uses (`RealNetlink`, a real
/// `UdpBroadcaster`/`TcpServer`, `NeighborhoodStubFactoryAdapter`, `RealIpRouteManager`),
/// generalized to an arbitrary device list, topology, and (per that file's own documented
/// distinction) either an authoritative test position, a [`PreformedNetwork`] (a position
/// *and* a shared `network_id`, still negotiable — `NodeInputs::preformed`'s own doc explains
/// the distinction), or both `None` for the real negotiated path. `initial_position` and
/// `preformed` are mutually exclusive, exactly as `NodeInputs` itself requires.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_node(
    my_id: NodeId,
    initial_position: Option<Vec<u32>>,
    preformed: Option<PreformedNetwork>,
    gsizes: &[u32],
    devs: &[&str],
    port: u16,
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
) -> anyhow::Result<StartedNode<ntk_netlink::RealNetlink>> {
    use ntk_rpc::{TcpServer, UdpBroadcaster};
    use ntkd::node::ip_route::RealIpRouteManager;
    use ntkd::node::lifecycle::{TcpDialer, linklocal_allocator, synthetic_mac};
    use ntkd::node::peers::PeerLinks;
    use ntkd::node::registry::LinkRegistry;
    use ntkd::node::stubs::NeighborhoodStubFactoryAdapter;

    let gsizes_toml = gsizes
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let nics_toml = devs
        .iter()
        .map(|d| format!("{d:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let config = NtkdConfig::from_str(&format!(
        "gsizes = [{gsizes_toml}]\nnics = [{nics_toml}]\nport = {port}\n"
    ))?;

    let registry = Arc::new(LinkRegistry::new());
    let links = Arc::new(PeerLinks::new());

    let mut broadcasters = HashMap::new();
    for &dev in devs {
        let broadcaster = Arc::new(UdpBroadcaster::bind(Some(dev), port, 1 << 16)?);
        broadcasters.insert(dev.to_owned(), broadcaster);
    }

    let neighborhood_config = NeighborhoodConfig {
        my_id,
        max_arcs: 64,
        kernel: ntk_netlink::RealNetlink::new()?,
        stub_factory: Arc::new(NeighborhoodStubFactoryAdapter {
            broadcasters: broadcasters.clone(),
            links: links.clone(),
            registry: registry.clone(),
        }),
        ip_route_manager: Arc::new(RealIpRouteManager {
            kernel: ntk_netlink::RealNetlink::new()?,
        }),
        rtt_probe: Arc::new(FixedRttProbe(Some(RTT_MS))),
        timing: NeighborhoodTiming {
            radar_interval: Duration::from_millis(200),
            arc_monitor_interval: (Duration::from_millis(20), Duration::from_millis(40)),
        },
        new_linklocal_address: linklocal_allocator(my_id),
        signing_key: None,
        require_auth: false,
    };
    let (neighborhood, neighborhood_join) =
        ntk_neighborhood::Manager::spawn(neighborhood_config, cancel.child_token());
    tasks.spawn(async move {
        let _ = neighborhood_join.await;
    });

    for &dev in devs {
        neighborhood
            .start_monitor(LocalNic {
                dev: dev.to_owned(),
                mac: synthetic_mac(dev, my_id),
            })
            .await?;
    }

    let routing_kernel = Arc::new(ntk_netlink::RealNetlink::new()?);
    let started = lifecycle::run(
        NodeInputs {
            config,
            neighborhood: neighborhood.clone(),
            registry,
            links,
            routing_kernel,
            dialer: Arc::new(TcpDialer::default()),
            initial_position,
            preformed,
            my_id,
        },
        tasks,
        cancel.clone(),
    )
    .await?;

    let server = TcpServer::bind(format!("0.0.0.0:{port}").parse()?, 1 << 20).await?;
    let dispatcher = started.dispatcher.clone();
    let server_cancel = cancel.child_token();
    tasks.spawn(async move {
        server.serve(dispatcher, server_cancel).await;
    });

    for (dev, broadcaster) in broadcasters {
        let handler = Arc::new(NeighborhoodRpcHandler::for_broadcast(
            neighborhood.clone(),
            dev,
        ));
        let broadcast_cancel = cancel.child_token();
        tasks.spawn(async move {
            ntk_neighborhood::serve_broadcast(broadcaster, handler, broadcast_cancel).await;
        });
    }

    Ok(started)
}

/// Graceful per-identity teardown, mirroring `supervisor::run`'s own shutdown sequence — see
/// `tests/multi_node.rs`'s `namespace_body` doc comment for why this is worth doing even though
/// the namespace itself is reclaimed once the worker thread exits regardless.
///
/// Bounded via [`ntkd::node::supervisor::drain_tasks`] — see that function's own doc for why an
/// unbounded `while tasks.join_next().await.is_some() {}` here previously let one wedged actor
/// (a real-kernel capture of `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` caught
/// the exact mechanism: `ntk_peerservices::routing::Handle::relay`'s zero-backoff gateway retry
/// loop, permanently starving this identity's own single-threaded runtime) hang this call for
/// 300-460s instead of returning within a bounded budget. Returns the number of tasks that
/// never joined (0 on a clean teardown), so callers/tests can assert on it.
pub async fn teardown(
    started: &StartedNode<ntk_netlink::RealNetlink>,
    cancel: CancellationToken,
    tasks: &mut JoinSet<()>,
) -> usize {
    cancel.cancel();
    let outstanding = ntkd::node::supervisor::drain_tasks(tasks, TEARDOWN_DRAIN_TIMEOUT).await;
    if let Err(err) = started
        .running
        .route_installer
        .lock()
        .await
        .teardown()
        .await
    {
        tracing::warn!(%err, "{}: route teardown failed", "namespace");
    }
    outstanding
}

/// One node's real, converged state, read back through a [`ntk_netlink::RealNetlink`] connection
/// independent of the one the daemon itself writes through — `tests/multi_node.rs`'s own "trust
/// the kernel, not just in-process state" discipline, generalized to N nodes.
#[derive(Debug)]
pub struct NodeReport {
    pub label: String,
    /// Mirrors `ntkd::node::lifecycle::GenerationHandles::rehooked` verbatim — see that field's
    /// own doc for why this (not a position-delta inference) is the only sound signal.
    pub rehooked: bool,
    pub naddr_positions: Vec<u32>,
    pub route_table: u32,
    pub routes: Vec<RouteSpec>,
    pub addresses: Vec<AddressEntry>,
    /// This node's own device name -> `ifindex` map, from [`bring_up_devs`] — see
    /// [`Self::linklocal`].
    pub dev_index: HashMap<String, u32>,
    /// This node's own [`ntk_neighborhood::Handle::snapshot`] at report time — diagnostic, and
    /// the only place a per-NIC arc-completion pin (see [`Self::arc_cost`]) can be made: kernel
    /// routes are per-destination, not per-local-NIC.
    pub arcs: Vec<NeighborArc>,
}

impl NodeReport {
    /// This node's own RFC 3927 linklocal address on `dev` (the gateway a neighbor sharing
    /// `dev`'s segment would route through to reach this node), if any.
    #[must_use]
    pub fn linklocal(&self, dev: &str) -> Option<Ipv4Addr> {
        let index = *self.dev_index.get(dev)?;
        self.addresses
            .iter()
            .find(|a| {
                a.interface_index == index
                    && a.network.prefix_len() == 16
                    && a.network.address().octets()[0] == 169
                    && a.network.address().octets()[1] == 254
            })
            .map(|a| a.network.address())
    }

    /// The published cost of the arc running over this node's own `dev`, if that arc has
    /// completed at least one successful measurement — the direct pin for "this specific NIC's
    /// arc actually came up", independent of whatever destinations/routes it produced.
    #[must_use]
    pub fn arc_cost(&self, dev: &str) -> Option<Cost> {
        self.arcs.iter().find(|a| a.my_dev == dev)?.cost
    }
}

/// Builds a [`NodeReport`] for `started`, via a fresh, independent `RealNetlink` connection.
pub async fn observe(
    label: &str,
    started: &StartedNode<ntk_netlink::RealNetlink>,
    dev_index: HashMap<String, u32>,
) -> anyhow::Result<NodeReport> {
    let observer = ntk_netlink::RealNetlink::new()
        .with_context(|| format!("{label}: observer RealNetlink"))?;
    let generation = started.running.generation.borrow().clone();
    let routes = observer
        .list_routes(Some(started.running.route_table))
        .await
        .with_context(|| format!("{label}: list_routes"))?;
    let addresses = observer
        .list_addresses(None)
        .await
        .with_context(|| format!("{label}: list_addresses"))?;
    let arcs = started.running.neighborhood.snapshot().borrow().clone();
    Ok(NodeReport {
        label: label.to_owned(),
        rehooked: generation.rehooked,
        naddr_positions: generation.qspn.my_naddr().positions().to_vec(),
        route_table: started.running.route_table,
        routes,
        addresses,
        dev_index,
        arcs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`root_handle_with_driver`]'s own load-bearing assumption, checked directly: the
    /// connection driver task exits promptly once every `rtnetlink::Handle` referencing it is
    /// dropped, rather than running forever. Without this, [`teardown_mesh`]'s own join would
    /// hang every scenario that calls it instead of tearing anything down. Needs no namespace or
    /// `CAP_NET_ADMIN`: opening a `NETLINK_ROUTE` socket doesn't require it, only mutating link
    /// state does.
    #[tokio::test]
    async fn root_connection_driver_exits_once_its_handle_is_dropped() {
        let (handle, driver) = root_handle_with_driver().expect("open rtnetlink connection");
        drop(handle);
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("connection driver did not exit within 2s of its handle being dropped")
            .expect("connection driver task panicked");
    }
}
