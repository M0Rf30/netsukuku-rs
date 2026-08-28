//! Real-kernel N-node mesh scenarios (Rung 2): multi-hop forwarding, a two-g-node merge, a
//! partition's debounced split signal, and a level-1 destination's CIDR arithmetic — all driven
//! over the shared fixture in `tests/netns/mod.rs`. See that module's own doc comment for the
//! namespace/segment machinery; see `tests/multi_node.rs`'s "Scenario 3" doc comment for why
//! this project never shells out to `ip`/`bridge` for any of it.
//!
//! # Deadlines
//! Every scenario here uses real (not paused) time over real sockets, so every deadline below is
//! derived, not guessed:
//! - `ntk_hooking::HookingConfig::default()`'s own restart-from-start backoff is
//!   `global_timeout(n) * restart_multiplier` = `1000ms * 20` = **20s** for any network under 5
//!   members (`hooking/src/config.rs`) — the "~20s hooking retry cycle" every deadline below
//!   budgets at least one of.
//! - `ntkd::node::lifecycle`'s real-daemon split debounce is hardcoded
//!   `ntk_qspn::FixedThreshold::default()` = **10s** (`ntk-qspn/src/config.rs`), not configurable
//!   via `NodeInputs` — the partition scenario's own deadline budgets this on top of the 20s
//!   figure above.
//! - Ordinary neighborhood/qspn convergence (arc discovery, ETP flooding) settles in well under
//!   a second at this fixture's `radar_interval`/`arc_monitor_interval` (200ms / 20-40ms); the
//!   20s (and, for partition, +10s) protocol-level floors dominate every budget below, so each
//!   deadline is that floor plus a flat 10-20s margin for scheduling/CI jitter — comfortably
//!   under this batch's 90s-per-scenario cap.
//!
//! # Running
//! Each scenario needs the same capability as this crate's existing privileged suite (the
//! equivalent of `CAP_NET_ADMIN` over a set of network namespaces it owns) and is `#[ignore]`d
//! by default; see each scenario's own doc comment for its exact invocation.

mod netns;

use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;

use ntk_common::{Cost, HCoord, Naddr, Topology};
use ntk_neighborhood::NodeId;
use ntk_netlink::{Interface, Nexthop, RouteSpec, RouteTarget};
use ntkd::kernel::addressing;
use ntkd::node::lifecycle::PreformedNetwork;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use netns::{Member, NamespaceWorker, NodeReport, Segment};

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_thread_names(true)
        .try_init();
}

/// Polls `check` (real, unpaused sleeps) until it returns `true` or `timeout` elapses. Returns
/// the final result of `check` either way, so callers get an honest pass/fail rather than a bare
/// bool that hides which predicate failed.
async fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return check();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn cost_at(snapshot: &ntk_qspn::RouteSnapshot, level: usize, pos: u32) -> Option<Cost> {
    snapshot
        .levels
        .get(level)?
        .iter()
        .find(|e| e.destination == HCoord::new(level, pos))?
        .paths
        .first()
        .map(|p| p.cost)
}

// =============================================================================================
// Scenario: multi-hop chain, 4 nodes on 3 separate segments
// =============================================================================================

const CHAIN_GSIZES: [u32; 1] = [4];
const CHAIN_PORT: u16 = 27310;
/// 20s hooking-restart floor (module doc) + 20s margin for 4-node real-socket convergence.
const CHAIN_TIMEOUT: Duration = Duration::from_secs(40);

fn chain_topology() -> Topology {
    Topology::new(CHAIN_GSIZES).unwrap()
}

fn chain_devs(idx: u32) -> Vec<&'static str> {
    match idx {
        0 | 3 => vec!["eth0"],
        1 | 2 => vec!["eth0", "eth1"],
        _ => unreachable!("chain has exactly 4 nodes"),
    }
}

/// The device `node` uses on its own side of the direct link toward `next_hop` — fixed by this
/// scenario's own segment wiring (`s0`: 0-1, `s1`: 1-2, `s2`: 2-3).
fn chain_dev_toward(node: u32, next_hop: u32) -> &'static str {
    match (node, next_hop) {
        (0, 1) | (1, 0) | (2, 1) | (3, 2) => "eth0",
        (1, 2) | (2, 3) => "eth1",
        _ => unreachable!("not a chain edge: {node} -> {next_hop}"),
    }
}

fn chain_next_hop(from: u32, to: u32) -> u32 {
    if to > from { from + 1 } else { from - 1 }
}

async fn chain_worker_body(
    idx: u32,
    devs: Vec<&'static str>,
    barrier: Arc<Barrier>,
) -> anyhow::Result<NodeReport> {
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        NodeId::from_raw(301 + idx as i32).unwrap(),
        Some(vec![idx]),
        None,
        &CHAIN_GSIZES,
        &devs,
        CHAIN_PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await?;

    let converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            (0..4u32).all(|j| {
                j == idx || {
                    let hops = u64::from((idx as i32 - j as i32).unsigned_abs());
                    cost_at(&snapshot, 0, j) == Some(Cost::Finite(hops * netns::RTT_MS))
                }
            })
        },
        CHAIN_TIMEOUT,
    )
    .await;
    anyhow::ensure!(
        converged,
        "node{idx}: did not converge to the exact expected hop-by-hop cost set in time: {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    let report = netns::observe(&format!("node{idx}"), &started, dev_index).await?;
    // Bounded: if a *sibling* node's own convergence `ensure!` above failed, it never reaches
    // this barrier, and an unbounded wait here would leave this (converged) node's thread
    // running forever in the background — well past this test's own coordinator having already
    // panicked and `cargo test` having moved on to the next one (see `netns::join_all`'s own
    // doc for the cross-test corruption this class of bug causes).
    let _ = tokio::time::timeout(CHAIN_TIMEOUT + Duration::from_secs(5), barrier.wait()).await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(report)
}

/// A 4-node chain (0 — 1 — 2 — 3), each link its own real bridged-veth broadcast segment,
/// converges over the real daemon core + real kernel to the *exact* route set multi-hop
/// forwarding implies: every node reaches every other, gatewayed through the correct direct
/// neighbour (never a more-distant node), cost accumulating one [`netns::RTT_MS`] per hop —
/// the first real-kernel proof of this daemon's multi-hop forwarding (every prior real-kernel
/// test, `tests/multi_node.rs`'s Scenario 3, is exactly one hop).
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored chain_of_four_converges_to_exact_multi_hop_routes
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn chain_of_four_converges_to_exact_multi_hop_routes() {
    init_tracing();
    let barrier = Arc::new(Barrier::new(4));
    let workers: Vec<NamespaceWorker<NodeReport>> = (0..4u32)
        .map(|i| {
            let b = barrier.clone();
            let devs = chain_devs(i);
            NamespaceWorker::spawn(format!("chain-node{i}"), move || {
                chain_worker_body(i, devs, b)
            })
        })
        .collect();

    let fds: Vec<RawFd> = workers.iter().map(NamespaceWorker::fd).collect();
    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![
        Segment {
            name: "s0",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth0",
                },
                Member {
                    node: 1,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "s1",
            members: vec![
                Member {
                    node: 1,
                    dev: "eth1",
                },
                Member {
                    node: 2,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "s2",
            members: vec![
                Member {
                    node: 2,
                    dev: "eth1",
                },
                Member {
                    node: 3,
                    dev: "eth0",
                },
            ],
        },
    ];
    let mesh = netns::wire(&root, &segments, &fds)
        .await
        .expect("wire chain segments");
    for w in &workers {
        w.signal_moved();
    }

    // Join every worker unconditionally, teardown the mesh, and only then panic on any
    // failure — see `netns::join_all`'s own doc for why panicking mid-loop (this scenario's old
    // shape) leaked still-running worker threads into whichever test ran next.
    let results = netns::join_all(workers, CHAIN_TIMEOUT + netns::JOIN_MARGIN).await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    let reports: Vec<NodeReport> = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| r.unwrap_or_else(|e| panic!("chain worker {i} failed: {e:?}")))
        .collect();

    let topo = chain_topology();

    // Explicit pin for the exact defect this rung found and reported (a 2-NIC relay node's
    // *second* NIC never completing an arc at all, because the kernel's route table can only
    // ever pick one interface for the whole shared `169.254.0.0/16` prefix): both of node1's and
    // node2's NICs must have an independently completed, costed arc — not merely "some route
    // eventually appeared", which the per-destination assertions below already cover.
    for (idx, devs) in [(1u32, ["eth0", "eth1"]), (2u32, ["eth0", "eth1"])] {
        let report = &reports[idx as usize];
        for dev in devs {
            assert!(
                report.arc_cost(dev).is_some(),
                "node{idx}'s NIC {dev:?} never completed an arc (no published cost) — the \
                 relay node must have a working arc on *every* configured NIC, not just the \
                 first: {:#?}",
                report.arcs
            );
        }
    }

    for i in 0..4u32 {
        let report = &reports[i as usize];
        assert_eq!(
            report.routes.len(),
            3,
            "node{i} must have exactly the 3 converged destinations installed, nothing stale: {:#?}",
            report.routes
        );
        for j in 0..4u32 {
            if i == j {
                continue;
            }
            let next_hop = chain_next_hop(i, j);
            let dev = chain_dev_toward(i, next_hop);
            let via = reports[next_hop as usize]
                .linklocal(chain_dev_toward(next_hop, i))
                .unwrap_or_else(|| {
                    panic!("node{next_hop} has no linklocal address on its link toward node{i}")
                });
            let expected = RouteSpec {
                destination: addressing::gnode_destination(
                    &Naddr::new(topo.clone(), vec![i]).unwrap(),
                    HCoord::new(0, j),
                )
                .unwrap(),
                table: report.route_table,
                target: RouteTarget::Gateway {
                    via,
                    dev: Interface::Index(*report.dev_index.get(dev).unwrap()),
                    src: Some(
                        addressing::host_address(&Naddr::new(topo.clone(), vec![i]).unwrap())
                            .unwrap()
                            .address(),
                    ),
                },
            };
            assert!(
                report.routes.contains(&expected),
                "node{i}'s route to node{j} (via node{next_hop}) is not exactly {expected:?}: {:#?}",
                report.routes
            );
        }
    }
}

// =============================================================================================
// Scenario: level-1 destination CIDR arithmetic touches a real kernel table
// =============================================================================================

const LEVEL1_GSIZES: [u32; 2] = [2, 2];
const LEVEL1_PORT: u16 = 27320;
const LEVEL1_TIMEOUT: Duration = Duration::from_secs(40);

/// `p0` sits alone in level-1 slot 0; `p1`/`p2` are the two level-0 siblings of slot 1. All
/// three share one flat broadcast segment — single NIC each, no bridging trick needed: unlike
/// hop count, `gnode_destination`'s CIDR *level* is a pure function of `Naddr` structure (which
/// level-1 slot a destination shares with the caller), so `p0` seeing `p1`/`p2` as direct,
/// disjoint 1-hop neighbours still exercises a genuine level-1 aggregate route from `p0` — as a
/// real `Multipath` (two disjoint equal-cost paths to the same destination), which is a richer,
/// not weaker, real-kernel proof than an indirect one.
fn level1_position(idx: u32) -> Vec<u32> {
    match idx {
        0 => vec![0, 0],
        1 => vec![0, 1],
        2 => vec![1, 1],
        _ => unreachable!("level1 mesh has exactly 3 nodes"),
    }
}

async fn level1_worker_body(idx: u32, barrier: Arc<Barrier>) -> anyhow::Result<NodeReport> {
    let devs = ["eth0"];
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        NodeId::from_raw(401 + idx as i32).unwrap(),
        Some(level1_position(idx)),
        None,
        &LEVEL1_GSIZES,
        &devs,
        LEVEL1_PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await?;

    let converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            match idx {
                // p0: two disjoint 1-hop paths (via p1, via p2) to the level-1 slot-1
                // aggregate — both must be admitted before this node's kernel route reflects
                // the real multipath set.
                0 => snapshot
                    .levels
                    .get(1)
                    .and_then(|level| level.iter().find(|e| e.destination == HCoord::new(1, 1)))
                    .is_some_and(|e| {
                        e.paths.len() == 2
                            && e.paths
                                .iter()
                                .all(|p| p.cost == Cost::Finite(netns::RTT_MS))
                    }),
                // p1/p2: one sibling (level-0) plus one level-1 route to p0's solitary slot.
                1 => {
                    cost_at(&snapshot, 0, 1) == Some(Cost::Finite(netns::RTT_MS))
                        && cost_at(&snapshot, 1, 0) == Some(Cost::Finite(netns::RTT_MS))
                }
                2 => {
                    cost_at(&snapshot, 0, 0) == Some(Cost::Finite(netns::RTT_MS))
                        && cost_at(&snapshot, 1, 0) == Some(Cost::Finite(netns::RTT_MS))
                }
                _ => unreachable!(),
            }
        },
        LEVEL1_TIMEOUT,
    )
    .await;
    anyhow::ensure!(
        converged,
        "node{idx}: did not converge to the expected level-0/level-1 cost set in time: {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    // DIAG (per Main's request): does the duplicated-`via` Multipath come from qspn admitting
    // two paths sharing one first-hop `ArcId` (protocol-level, legitimate), or from the route
    // installer emitting one nexthop per admitted *path* instead of per distinct gateway
    // (installer-level collapsing bug)? Logs every admitted path's `ArcId` per destination.
    if idx == 1 || idx == 2 {
        let snapshot = started.running.generation.borrow().qspn.snapshot();
        for level in 0..2 {
            if let Some(entries) = snapshot.levels.get(level) {
                for entry in entries {
                    let arc_ids: Vec<_> = entry.paths.iter().map(|p| p.arc).collect();
                    eprintln!(
                        "DIAG node{idx} dest={:?} admitted_path_arcs={arc_ids:?}",
                        entry.destination
                    );
                }
            }
        }
    }

    let report = netns::observe(&format!("node{idx}"), &started, dev_index).await?;
    // Bounded — see `chain_worker_body`'s identical fix for why an unbounded wait here would
    // leave this node's thread running forever if a sibling's own convergence `ensure!` failed.
    let _ = tokio::time::timeout(LEVEL1_TIMEOUT + Duration::from_secs(5), barrier.wait()).await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(report)
}

/// One level-1 g-node with two level-0 members (`p1`,`p2`) and one solitary neighbouring g-node
/// (`p0`), all on one real broadcast segment, converge to real kernel routes exercising
/// `ntkd::kernel::addressing::gnode_destination`'s CIDR arithmetic at `level == 1` — including a
/// genuine `Multipath` route (two disjoint, equal-cost paths) from `p0`, the first real-kernel
/// exercise of both `update_clusters` (`ntk-qspn`) and multipath route installation
/// (`ntkd::kernel::routes`) together.
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored level1_destination_installs_correct_cidr_route
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn level1_destination_installs_correct_cidr_route() {
    init_tracing();
    let barrier = Arc::new(Barrier::new(3));
    let workers: Vec<NamespaceWorker<NodeReport>> = (0..3u32)
        .map(|i| {
            let b = barrier.clone();
            NamespaceWorker::spawn(format!("level1-node{i}"), move || level1_worker_body(i, b))
        })
        .collect();

    let fds: Vec<RawFd> = workers.iter().map(NamespaceWorker::fd).collect();
    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![Segment {
        name: "seg",
        members: vec![
            Member {
                node: 0,
                dev: "eth0",
            },
            Member {
                node: 1,
                dev: "eth0",
            },
            Member {
                node: 2,
                dev: "eth0",
            },
        ],
    }];
    let mesh = netns::wire(&root, &segments, &fds)
        .await
        .expect("wire level1 segment");
    for w in &workers {
        w.signal_moved();
    }

    // Join every worker unconditionally, teardown the mesh, and only then panic — see
    // `netns::join_all`'s own doc.
    let results = netns::join_all(workers, LEVEL1_TIMEOUT + netns::JOIN_MARGIN).await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    let reports: Vec<NodeReport> = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| r.unwrap_or_else(|e| panic!("level1 worker {i} failed: {e:?}")))
        .collect();

    let topo = Topology::new(LEVEL1_GSIZES).unwrap();
    let naddr = |idx: u32| Naddr::new(topo.clone(), level1_position(idx)).unwrap();
    let gateway = |idx: u32| Nexthop {
        via: reports[idx as usize]
            .linklocal("eth0")
            .unwrap_or_else(|| panic!("node{idx} has no linklocal address on eth0")),
        dev: Interface::Index(*reports[idx as usize].dev_index.get("eth0").unwrap()),
        weight: 255,
    };

    // Every nexthop in a route (Gateway or Multipath) must name a *distinct* real neighbour on
    // this segment — a nexthop set can never legitimately repeat a gateway, and a single-path
    // destination can never legitimately be Multipath at all. Whether a longer, second-arc path
    // (e.g. p1's 2-hop route to p0 via p2) is *also* admitted alongside the direct one-hop path
    // is `mch_ratio`'s call, not something this test pins — only the distinctness invariant is.
    let assert_valid_nexthops = |report: &NodeReport, idx: u32, hc: HCoord, neighbours: &[u32]| {
        let expected_dest = addressing::gnode_destination(&naddr(idx), hc).unwrap();
        let route = report
            .routes
            .iter()
            .find(|r| r.destination == expected_dest)
            .unwrap_or_else(|| panic!("node{idx} has no route to {hc:?}: {:#?}", report.routes));
        let vias: Vec<Ipv4Addr> = match &route.target {
            RouteTarget::Gateway { via, .. } => vec![*via],
            RouteTarget::Multipath(nexthops) => nexthops.iter().map(|n| n.via).collect(),
            other => panic!("node{idx}'s route to {hc:?} is not routable: {other:?}"),
        };
        let unique: std::collections::HashSet<_> = vias.iter().collect();
        assert_eq!(
            unique.len(),
            vias.len(),
            "node{idx}'s route to {hc:?} repeats a gateway — a nexthop set can never legitimately \
             name the same neighbour twice: {vias:?}"
        );
        let allowed: Vec<Ipv4Addr> = neighbours.iter().map(|&n| gateway(n).via).collect();
        for via in &vias {
            assert!(
                allowed.contains(via),
                "node{idx}'s route to {hc:?} names {via}, not one of this segment's real \
                 neighbours {allowed:?}"
            );
        }
    };
    assert_valid_nexthops(&reports[1], 1, HCoord::new(0, 1), &[2]);
    assert_valid_nexthops(&reports[1], 1, HCoord::new(1, 0), &[0, 2]);
    assert_valid_nexthops(&reports[2], 2, HCoord::new(0, 0), &[1]);
    assert_valid_nexthops(&reports[2], 2, HCoord::new(1, 0), &[0, 1]);

    // p0: one route to the level-1 slot-1 aggregate, as Multipath naming both p1 and p2 —
    // exactly the topology's two disjoint one-hop neighbours, no duplicates.
    assert_eq!(reports[0].routes.len(), 1, "{:#?}", reports[0].routes);
    let route = &reports[0].routes[0];
    let expected_dest = addressing::gnode_destination(&naddr(0), HCoord::new(1, 1)).unwrap();
    assert_eq!(route.destination, expected_dest);
    assert_eq!(route.table, reports[0].route_table);
    let RouteTarget::Multipath(nexthops) = &route.target else {
        panic!(
            "p0's route to the level-1 slot-1 aggregate must be Multipath (two disjoint equal-cost \
             paths via p1 and p2), got {:?}",
            route.target
        );
    };
    let vias: std::collections::HashSet<_> = nexthops.iter().map(|n| n.via).collect();
    assert_eq!(
        vias.len(),
        nexthops.len(),
        "p0's level-1 route repeats a gateway: {nexthops:?}"
    );
    assert_eq!(
        vias,
        std::collections::HashSet::from([gateway(1).via, gateway(2).via]),
        "p0's level-1 route must name exactly p1 and p2, its two disjoint one-hop neighbours: \
         {nexthops:?}"
    );
}

// =============================================================================================
// Scenario: partition + the debounced split signal
// =============================================================================================

const PARTITION_GSIZES: [u32; 2] = [2, 2];
const PARTITION_PORT: u16 = 27330;
/// `ntk_qspn`'s hardcoded 10s split debounce (module doc) + 20s hooking-restart floor + margin.
const PARTITION_TIMEOUT: Duration = Duration::from_secs(60);

// A genuine `GnodeSplitted` witness needs *disjoint* paths to both post-partition fragments —
// otherwise a severed link is observed as plain withdrawal (`DestinationRemoved`/`PathRemoved`),
// never a fork. That redundancy is a graph-theoretic precondition, not a topology choice: no
// bridge-only (single-NIC-per-node) topology can supply it, because any additional inter-bridge
// link merely merges the two domains into one broadcast domain (and would create an L2 loop) —
// it can never give one witness two *independent* paths into the same still-distinguishable
// far side. Two scenarios below reflect this split honestly rather than papering over it:
// `partition_clean_severance_drops_exactly_the_unreachable_destinations` proves the (single-NIC,
// unblocked) reachability-loss half; `partition_signals_split_only_after_the_documented_debounce`
// proves the (multi-NIC witness, currently blocked on the same dial defect `mesh.rs`'s chain
// scenario reports) split-signal half — left red on purpose, not weakened to pass.

const SEVERANCE_GSIZES: [u32; 2] = [2, 2];
const SEVERANCE_PORT: u16 = 27335;
/// 20s hooking-restart floor (module doc) + 20s margin — no split debounce involved here.
const SEVERANCE_TIMEOUT: Duration = Duration::from_secs(40);

/// `q0`,`q1` (level-1 slot 0) and `q2`,`q3` (level-1 slot 1), each pair on its own single-NIC
/// flat segment, the two segments joined by one [`netns::WiredMesh::link_bridges`] L2 uplink —
/// no node needs a second NIC. Every node sees every other directly once bridged (a flat merged
/// domain), so severing the uplink is a clean, total loss of the *other* slot's aggregate
/// destination, nothing partial: the textbook case of QSPN's implicit-withdrawal rule reaching a
/// real kernel for the first time.
fn severance_position(idx: u32) -> Vec<u32> {
    match idx {
        0 => vec![0, 0],
        1 => vec![1, 0],
        2 => vec![0, 1],
        3 => vec![1, 1],
        _ => unreachable!("severance mesh has exactly 4 nodes"),
    }
}

async fn severance_worker_body(
    idx: u32,
    sever_barrier: Arc<Barrier>,
    done_barrier: Arc<Barrier>,
) -> anyhow::Result<NodeReport> {
    let devs = ["eth0"];
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        NodeId::from_raw(511 + idx as i32).unwrap(),
        Some(severance_position(idx)),
        None,
        &SEVERANCE_GSIZES,
        &devs,
        SEVERANCE_PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await?;

    let sibling_pos0 = if idx.is_multiple_of(2) {
        idx + 1
    } else {
        idx - 1
    };
    let other_slot = 1 - severance_position(idx)[1];

    let converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            // Exactly 2 *disjoint, cheapest* paths (via node2 and via node3, each cost
            // `RTT_MS`) is the real invariant here — not "exactly 2 admitted paths total".
            // `slot0`/`slot1` are bridged into one flat broadcast domain before severing, so
            // this node's sibling (also directly reachable) has its own direct arc into the
            // other slot too; QSPN's own multipath admission (`ntk-qspn`'s
            // `update_map_one_destination`) legitimately keeps that longer, costlier
            // via-sibling path (`2 * RTT_MS`) alongside the two direct ones instead of
            // discarding it — confirmed via a real-kernel run (`MergeDiag`, `research/notes/
            // 01-vala-core-routing.md`'s own account of upstream's overlap-tolerant multipath
            // admission). Filtering to the cheapest-cost paths recovers the two-disjoint-direct-
            // path invariant this test actually means to check, without assuming away a real,
            // legitimate third path.
            let two_disjoint_paths_to_other_slot = snapshot
                .levels
                .get(1)
                .and_then(|level| {
                    level
                        .iter()
                        .find(|e| e.destination == HCoord::new(1, other_slot))
                })
                .is_some_and(|e| {
                    e.paths
                        .iter()
                        .filter(|p| p.cost == Cost::Finite(netns::RTT_MS))
                        .count()
                        == 2
                });
            cost_at(&snapshot, 0, sibling_pos0) == Some(Cost::Finite(netns::RTT_MS))
                && two_disjoint_paths_to_other_slot
        },
        SEVERANCE_TIMEOUT,
    )
    .await;
    anyhow::ensure!(
        converged,
        "node{idx}: did not converge to its sibling plus both disjoint paths to the other slot: \
         {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    // Bounded — see `netns::join_all`'s own doc for the cross-test corruption an unbounded wait
    // here causes: this scenario's own first `ensure!` above fails on every observed run, and an
    // unbounded wait would leave every *other*, still-converged node's thread running forever.
    let _ = tokio::time::timeout(
        SEVERANCE_TIMEOUT + Duration::from_secs(5),
        sever_barrier.wait(),
    )
    .await;

    // Clean severance: the whole other-slot aggregate destination disappears (both disjoint
    // paths ran over the one uplink that just went down), while the sibling route — never
    // touched — survives unchanged.
    let dropped_exactly_the_unreachable_destination = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            let other_slot_gone = snapshot.levels.get(1).is_none_or(|level| {
                !level
                    .iter()
                    .any(|e| e.destination == HCoord::new(1, other_slot))
            });
            cost_at(&snapshot, 0, sibling_pos0) == Some(Cost::Finite(netns::RTT_MS))
                && other_slot_gone
        },
        Duration::from_secs(20),
    )
    .await;
    anyhow::ensure!(
        dropped_exactly_the_unreachable_destination,
        "node{idx}: did not drop exactly the unreachable other-slot destination within 20s: {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    let report = netns::observe(&format!("node{idx}"), &started, dev_index).await?;

    // Hold every node up until all four have observed. `netns::teardown` cancels this node's
    // root token, which closes its `TcpServer` (`ntkd::node::transport`'s `server_cancel` is a
    // child of the supervisor's root token, not of a generation token), and that drops every
    // peer's shared connection to it. Without this barrier the first node to finish observing
    // tears itself down while the others are still watching, so their *sibling* route vanishes
    // for a reason the scenario never intended to test — which is exactly how this test used to
    // fail: node1 must watch a route *disappear*, which is inherently slower than watching one
    // arrive, so it was reliably the node still waiting when its sibling exited. Bounded like
    // `sever_barrier` above so one node's failed `ensure!` cannot hang the rest.
    let _ = tokio::time::timeout(
        SEVERANCE_TIMEOUT + Duration::from_secs(5),
        done_barrier.wait(),
    )
    .await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(report)
}

/// A converged 4-node, 2-slot network (`{q0,q1}` in level-1 slot 0, `{q2,q3}` in slot 1, joined
/// by one bridge-to-bridge L2 uplink) is partitioned by severing that uplink: each node keeps
/// its own sibling route untouched and drops *exactly* the other slot's now-unreachable
/// aggregate destination — QSPN's implicit-withdrawal rule meeting a real kernel for the first
/// time. See the constraint comment above this scenario for why this is the reachability-loss
/// half of "partition", not the `GnodeSplitted` half (that one needs a multi-NIC witness — see
/// `partition_signals_split_only_after_the_documented_debounce` below).
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored partition_clean_severance_drops_exactly_the_unreachable_destinations
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn partition_clean_severance_drops_exactly_the_unreachable_destinations() {
    init_tracing();
    // Sized 5: the 4 node workers plus this coordinator, so the sever only runs once every node
    // has confirmed convergence.
    let sever_barrier = Arc::new(Barrier::new(5));
    // Also sized 5, and for the mirror-image reason: no node may tear itself down until all four
    // have finished observing the post-sever state. See `severance_worker_body`'s own comment.
    let done_barrier = Arc::new(Barrier::new(5));
    let workers: Vec<NamespaceWorker<NodeReport>> = (0..4u32)
        .map(|i| {
            let b = sever_barrier.clone();
            let d = done_barrier.clone();
            NamespaceWorker::spawn(format!("severance-node{i}"), move || {
                severance_worker_body(i, b, d)
            })
        })
        .collect();
    let fds: Vec<RawFd> = workers.iter().map(NamespaceWorker::fd).collect();

    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![
        Segment {
            name: "slot0",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth0",
                },
                Member {
                    node: 1,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "slot1",
            members: vec![
                Member {
                    node: 2,
                    dev: "eth0",
                },
                Member {
                    node: 3,
                    dev: "eth0",
                },
            ],
        },
    ];
    let mut mesh = netns::wire(&root, &segments, &fds)
        .await
        .expect("wire severance segments");
    mesh.link_bridges(&root, "slot0", "slot1", "sevup")
        .await
        .expect("uplink the two slots' bridges");
    for w in &workers {
        w.signal_moved();
    }

    // Join the sever rendezvous as the 5th party, bounded: if any node's own convergence
    // `ensure!` failed, it never reaches this barrier, and an unbounded wait here would hang
    // the whole test instead of surfacing that node's real error via `w.join()` below.
    let _ = tokio::time::timeout(
        SEVERANCE_TIMEOUT + Duration::from_secs(5),
        sever_barrier.wait(),
    )
    .await;
    mesh.sever(&root, "sevup").await.expect("sever the uplink");

    // Join the post-observation rendezvous as the 5th party, same bounded discipline: this is
    // what keeps all four nodes alive until every one of them has observed the post-sever state,
    // so no node's `TcpServer` closes under a peer that is still watching.
    let _ = tokio::time::timeout(
        SEVERANCE_TIMEOUT + Duration::from_secs(25),
        done_barrier.wait(),
    )
    .await;

    // Join every worker unconditionally, teardown the mesh, and only then panic — see
    // `netns::join_all`'s own doc.
    let results = netns::join_all(
        workers,
        SEVERANCE_TIMEOUT + Duration::from_secs(60) + netns::JOIN_MARGIN,
    )
    .await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    for (i, r) in results.into_iter().enumerate() {
        r.unwrap_or_else(|e| panic!("severance worker {i} failed: {e:?}"));
    }
}

/// A triangle inside one level-1 g-node: `a` sits alone in slot 0 (`[0,0]`); `b1`/`b2` are the
/// two level-0 siblings of slot 1 (`[0,1]`/`[1,1]`), directly linked to each other *and* each
/// independently linked to `a` — mirroring `ntk-qspn`'s own proven
/// `partition_signals_split_only_after_debounce` fixture-shape (`crates/ntk-qspn/tests/
/// convergence.rs`), now over a real kernel: severing the `b1`-`b2` link is a genuine partition
/// (`b1` and `b2` can no longer agree on slot 1's identity), observed by `a` — the only node with
/// an unbroken arc to each independent survivor.
fn partition_position(label: &str) -> Vec<u32> {
    match label {
        "a" => vec![0, 0],
        "b1" => vec![0, 1],
        "b2" => vec![1, 1],
        _ => unreachable!(),
    }
}

enum PartitionReport {
    A {
        report: NodeReport,
        saw_split_before_debounce: bool,
        saw_split_after_debounce: bool,
    },
    B(NodeReport),
}

async fn partition_worker_a(
    sever_barrier: Arc<Barrier>,
    done_barrier: Arc<Barrier>,
) -> anyhow::Result<PartitionReport> {
    let devs = vec!["eth0", "eth1"];
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        NodeId::from_raw(501).unwrap(),
        Some(partition_position("a")),
        None,
        &PARTITION_GSIZES,
        &devs,
        PARTITION_PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await?;

    // Converge: both arcs up, single agreed fingerprint for slot 1 (b1 and b2 still see each
    // other directly, so no split yet).
    let converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            cost_at(&snapshot, 1, 1) == Some(Cost::Finite(netns::RTT_MS))
        },
        PARTITION_TIMEOUT,
    )
    .await;
    anyhow::ensure!(converged, "a: did not converge to slot 1 before partition");

    let mut events = started.running.generation.borrow().qspn.subscribe_events();

    // Rendezvous with the coordinator (which is itself a party of this barrier) so the sever
    // only happens once every side has converged; unlike `tests/multi_node.rs`'s raw veth pairs,
    // this fixture's bridge+veth segments give every namespace an independent link into the
    // persistent root namespace, so no shared-deletion hazard gates this rendezvous — it exists
    // purely to pin down the sever instant for the debounce-timing assertion below.
    // Bounded — see `netns::join_all`'s own doc: if a *sibling* worker's own convergence
    // `ensure!` failed before reaching this barrier, an unbounded wait here would leave this
    // node's thread running forever past this test's own conclusion.
    let _ = tokio::time::timeout(
        PARTITION_TIMEOUT + Duration::from_secs(5),
        sever_barrier.wait(),
    )
    .await;

    let debounce_check_deadline = tokio::time::Instant::now() + Duration::from_secs(9);
    let mut saw_split_before_debounce = false;
    while tokio::time::Instant::now() < debounce_check_deadline {
        if let Ok(Ok(ntk_qspn::QspnEvent::GnodeSplitted { .. })) =
            tokio::time::timeout(Duration::from_millis(200), events.recv()).await
        {
            saw_split_before_debounce = true;
        }
    }

    let mut saw_split_after_debounce = false;
    let after_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < after_deadline && !saw_split_after_debounce {
        if let Ok(Ok(ntk_qspn::QspnEvent::GnodeSplitted { .. })) =
            tokio::time::timeout(Duration::from_millis(200), events.recv()).await
        {
            saw_split_after_debounce = true;
        }
    }

    let report = netns::observe("a", &started, dev_index).await?;
    // Bounded for the same reason: a sibling's own post-sever `ensure!` (worker b's) failing
    // before reaching this barrier must not strand this node's thread forever.
    let _ = tokio::time::timeout(Duration::from_secs(30), done_barrier.wait()).await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(PartitionReport::A {
        report,
        saw_split_before_debounce,
        saw_split_after_debounce,
    })
}

async fn partition_worker_b(
    label: &'static str,
    my_id: NodeId,
    other_pos0: u32,
    sever_barrier: Arc<Barrier>,
    done_barrier: Arc<Barrier>,
) -> anyhow::Result<PartitionReport> {
    let devs = vec!["eth0", "eth1"];
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        Some(partition_position(label)),
        None,
        &PARTITION_GSIZES,
        &devs,
        PARTITION_PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await?;

    // Two routes before the sever: the sibling (level-0, direct) and `a`'s own solitary group
    // (level-1 slot 0, also direct — `a` is alone there).
    let converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            cost_at(&snapshot, 0, other_pos0) == Some(Cost::Finite(netns::RTT_MS))
                && cost_at(&snapshot, 1, 0) == Some(Cost::Finite(netns::RTT_MS))
        },
        PARTITION_TIMEOUT,
    )
    .await;
    anyhow::ensure!(
        converged,
        "{label}: did not converge to its sibling and to a's group before partition"
    );

    // Bounded — same reason as `partition_worker_a`'s identical fix.
    let _ = tokio::time::timeout(
        PARTITION_TIMEOUT + Duration::from_secs(5),
        sever_barrier.wait(),
    )
    .await;

    // The sibling route must disappear (real reachability loss) — bounded well under the split
    // debounce, since ordinary arc removal is gated by `arc_monitor_interval` (20-40ms), not by
    // `FixedThreshold`. The route to `a`'s group is untouched: that link was never severed.
    let lost = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            cost_at(&snapshot, 0, other_pos0).is_none()
                && cost_at(&snapshot, 1, 0) == Some(Cost::Finite(netns::RTT_MS))
        },
        Duration::from_secs(20),
    )
    .await;
    anyhow::ensure!(
        lost,
        "{label}: did not drop exactly its severed-sibling route within 20s: {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    let report = netns::observe(label, &started, dev_index).await?;
    // Bounded — same reason as `partition_worker_a`'s identical fix.
    let _ = tokio::time::timeout(Duration::from_secs(30), done_barrier.wait()).await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(PartitionReport::B(report))
}

/// A converged triangle (`a`-`b1`, `a`-`b2`, `b1`-`b2`) is partitioned by severing the `b1`-`b2`
/// segment: `a` (still directly linked to both) observes exactly the documented debounced split
/// signal (`GnodeSplitted`, gated by `ntk_qspn`'s hardcoded 10s `FixedThreshold`) — not before,
/// but shortly after — while `b1`/`b2` each drop exactly their now-unreachable mutual-sibling
/// route, keeping their unrelated route to `a`'s group untouched.
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored partition_signals_split_only_after_the_documented_debounce
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn partition_signals_split_only_after_the_documented_debounce() {
    init_tracing();
    // Sized 4: the 3 node workers plus this coordinator thread, so the sever below only ever
    // runs once every side has actually converged (see `partition_worker_a`'s own doc comment).
    let sever_barrier = Arc::new(Barrier::new(4));
    // Sized 3: node workers only, released once every side has taken its post-sever report.
    let done_barrier = Arc::new(Barrier::new(3));

    let worker_a = NamespaceWorker::spawn("partition-a", {
        let (sb, db) = (sever_barrier.clone(), done_barrier.clone());
        move || partition_worker_a(sb, db)
    });
    let worker_b1 = NamespaceWorker::spawn("partition-b1", {
        let (sb, db) = (sever_barrier.clone(), done_barrier.clone());
        move || partition_worker_b("b1", NodeId::from_raw(502).unwrap(), 1, sb, db)
    });
    let worker_b2 = NamespaceWorker::spawn("partition-b2", {
        let (sb, db) = (sever_barrier.clone(), done_barrier.clone());
        move || partition_worker_b("b2", NodeId::from_raw(503).unwrap(), 0, sb, db)
    });

    let fd_a = worker_a.fd();
    let fd_b1 = worker_b1.fd();
    let fd_b2 = worker_b2.fd();

    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![
        Segment {
            name: "ab1",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth0",
                },
                Member {
                    node: 1,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "ab2",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth1",
                },
                Member {
                    node: 2,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "b1b2",
            members: vec![
                Member {
                    node: 1,
                    dev: "eth1",
                },
                Member {
                    node: 2,
                    dev: "eth1",
                },
            ],
        },
    ];
    let mesh = netns::wire(&root, &segments, &[fd_a, fd_b1, fd_b2])
        .await
        .expect("wire partition triangle");
    worker_a.signal_moved();
    worker_b1.signal_moved();
    worker_b2.signal_moved();

    // Join the sever rendezvous as the 4th party, bounded for the same reason as the severance
    // scenario above.
    let _ = tokio::time::timeout(
        PARTITION_TIMEOUT + Duration::from_secs(5),
        sever_barrier.wait(),
    )
    .await;
    mesh.sever(&root, "b1b2").await.expect("sever b1b2 segment");

    // Join every worker unconditionally, teardown the mesh, and only then panic — see
    // `netns::join_all`'s own doc. Order is preserved: index 0 = a, 1 = b1, 2 = b2.
    let mut results = netns::join_all(
        vec![worker_a, worker_b1, worker_b2],
        PARTITION_TIMEOUT + Duration::from_secs(60) + netns::JOIN_MARGIN,
    )
    .await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    let report_b2 = results
        .pop()
        .unwrap()
        .unwrap_or_else(|e| panic!("worker b2 failed: {e:?}"));
    let report_b1 = results
        .pop()
        .unwrap()
        .unwrap_or_else(|e| panic!("worker b1 failed: {e:?}"));
    let report_a = results
        .pop()
        .unwrap()
        .unwrap_or_else(|e| panic!("worker a failed: {e:?}"));

    let PartitionReport::A {
        report: report_a,
        saw_split_before_debounce,
        saw_split_after_debounce,
    } = report_a
    else {
        panic!("worker a returned the wrong report variant");
    };
    assert!(
        !saw_split_before_debounce,
        "GnodeSplitted fired before the documented 10s debounce elapsed"
    );
    assert!(
        saw_split_after_debounce,
        "GnodeSplitted did not fire within 20s after the 10s debounce elapsed"
    );

    let topo = Topology::new(PARTITION_GSIZES).unwrap();
    for (report, label, a_dev, other_pos0) in [
        (report_b1, "b1", "eth0", 1u32),
        (report_b2, "b2", "eth1", 0u32),
    ] {
        let PartitionReport::B(report) = report else {
            panic!("worker b returned the wrong report variant");
        };
        let naddr = Naddr::new(topo.clone(), partition_position(label)).unwrap();
        let via = report_a
            .linklocal(a_dev)
            .unwrap_or_else(|| panic!("a has no linklocal address on {a_dev}"));
        let expected = RouteSpec {
            destination: addressing::gnode_destination(&naddr, HCoord::new(1, 0)).unwrap(),
            table: report.route_table,
            target: RouteTarget::Gateway {
                via,
                dev: Interface::Index(*report.dev_index.get("eth0").unwrap()),
                src: Some(addressing::host_address(&naddr).unwrap().address()),
            },
        };
        assert_eq!(
            report.routes,
            vec![expected],
            "{label}: must have dropped exactly its severed route to node at pos0={other_pos0}, \
             keeping only its route to a's group: {:#?}",
            report.routes
        );
    }
}

// =============================================================================================
// Scenario: two g-nodes negotiate and merge
// =============================================================================================

const MERGE_GSIZES: [u32; 1] = [8];
const MERGE_PORT: u16 = 27340;
/// Own-group formation (the star hub negotiates with its 2 spokes) needs its own hooking-restart
/// floor before any cross-group path exists at all — 20s (module doc). Real-kernel runs of this
/// scenario measured a 3-way *simultaneous* star (all three nodes discover each other and start
/// negotiating in the same instant, unlike a sequential 1-then-1 join) needing several such
/// floors: `ntk_coordinator`'s own `evaluate_enter` serializes to one in-flight id per level
/// (`AskAgain` — "a different id is already in flight"), and a peer whose own arc-handler
/// already resolved against a neighbor that has since migrated only re-evaluates once that
/// neighbor's real `ntk_identities` duplication handshake notifies it (`on_identity_event`),
/// which itself waits on the migrating peer's own negotiation to finish first — measured up to
/// ~65s for the slowest of three simultaneously-negotiating members in real-kernel runs.
/// Budgeted generously above that measurement rather than tightly to it.
const GROUP_CONVERGE_TIMEOUT: Duration = Duration::from_secs(90);
/// Once the uplink joins two independently-converged groups, the merge negotiation itself has no
/// fixed predicate (the outcome is a real negotiation, not known ahead of time) — the same
/// budget as [`GROUP_CONVERGE_TIMEOUT`], for the identical reason (three members of the losing
/// group each negotiate their own entry into the winning network at once, and may each need
/// several retry cycles before every arc-handler on both sides has re-evaluated).
const MERGE_TIMEOUT: Duration = Duration::from_secs(90);
/// Bundled [`MergeScenarioConfig`] for this scenario's own call into [`merge_worker_body`].
const MERGE_CONFIG: MergeScenarioConfig = MergeScenarioConfig {
    gsizes: &MERGE_GSIZES,
    port: MERGE_PORT,
    group_converge_timeout: GROUP_CONVERGE_TIMEOUT,
    merge_timeout: MERGE_TIMEOUT,
};

/// One node's position/network-id observation, captured both after this node's own group
/// converges internally (before the inter-group uplink forms) and after the merge completes —
/// see `two_star_groups_merge_into_one_network`'s own doc for why comparing these two snapshots,
/// not `NodeReport::rehooked`, is the property this scenario actually claims.
#[derive(Debug)]
struct MergeReport {
    report: NodeReport,
    pre_network_id: i64,
    pre_positions: Vec<u32>,
    post_network_id: i64,
}

/// Per-scenario knobs [`merge_worker_body`] needs beyond identity/barriers — bundled so the
/// function stays under clippy's argument-count lint instead of growing a ninth positional
/// parameter for [`two_level_gnode_migrates_as_a_unit_into_merged_network`].
#[derive(Clone, Copy)]
struct MergeScenarioConfig {
    gsizes: &'static [u32],
    port: u16,
    group_converge_timeout: Duration,
    merge_timeout: Duration,
}

async fn merge_worker_body(
    label: &'static str,
    my_id: NodeId,
    config: MergeScenarioConfig,
    converge_barrier: Arc<Barrier>,
    barrier: Arc<Barrier>,
) -> anyhow::Result<MergeReport> {
    let devs = ["eth0"];
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        None,
        None,
        config.gsizes,
        &devs,
        config.port,
        &mut tasks,
        cancel.clone(),
    )
    .await?;
    // Each node must reach its own group's canonical 3-node identity — see *both* group siblings
    // as real, hooking-admitted destinations — before the coordinator ever creates the
    // inter-group uplink. Deliberately reachability, not direct-hop cost: intra-group formation
    // with `initial_position: None` is itself a real negotiated hooking sequence (a star — one
    // hub, two spokes admitted sequentially, exactly per this scenario's own doc comment), so a
    // hub sees one spoke directly and the other spoke only via that first spoke until/unless a
    // separate physical arc also forms; requiring both at cost `RTT_MS` mistook that legitimate
    // star shape for non-convergence and starved the barrier for the full timeout on every run.
    // See this scenario's own doc comment for why this staging (not a sleep) is load-bearing.
    // Checking level 0's own admitted-sibling count (not any higher level) generalizes unchanged
    // across both the single-level ([`MERGE_GSIZES`]) and two-level ([`UNIT_MERGE_GSIZES`])
    // callers: "this node's own g-node has admitted its other 2 members" is the same predicate
    // whether that g-node is the whole (one-level) network or the innermost level of a taller one.
    let group_converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            snapshot
                .levels
                .first()
                .is_some_and(|level| level.len() == 2)
        },
        config.group_converge_timeout,
    )
    .await;
    anyhow::ensure!(
        group_converged,
        "{label}: did not converge to its own group's two siblings before the uplink: {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    // Snapshot this node's own pre-uplink (network_id, positions) — the "before" half of the
    // property this scenario actually claims (see the final assertion's own doc comment below).
    let pre_network_id = started.running.net.network_id();
    let pre_positions = started
        .running
        .generation
        .borrow()
        .qspn
        .my_naddr()
        .positions()
        .to_vec();

    // Bounded, for the same reason severance's sever rendezvous is (see its coordinator's
    // comment): this barrier has one party per node plus the coordinator, so if any *other*
    // worker fails its own convergence `ensure!` above, an unbounded wait here would hang the
    // whole test instead of surfacing that node's real error via `w.join()` below.
    let _ = tokio::time::timeout(
        config.group_converge_timeout + Duration::from_secs(5),
        converge_barrier.wait(),
    )
    .await;

    // No fixed convergence predicate is possible for the cross-group merge itself (every node's
    // final position is a real negotiation outcome, not known ahead of time) — just run out the
    // deadline, exactly like `tests/multi_node.rs`'s own `negotiation_namespace_body`.
    tokio::time::sleep(config.merge_timeout).await;

    let report = netns::observe(label, &started, dev_index).await?;
    let post_network_id = started.running.net.network_id();
    // Bounded, for the same reason the severance scenario's rendezvous is (see its coordinator's
    // comment): this barrier has one party per node, so if any *other* worker fails its own
    // `?`/`ensure!` before reaching here, an unbounded wait would block every remaining worker
    // forever — turning one node's reportable error into a whole-suite hang. Timing out here lets
    // each worker finish and surface its own result through `w.join()`.
    let _ = tokio::time::timeout(Duration::from_secs(30), barrier.wait()).await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(MergeReport {
        report,
        pre_network_id,
        pre_positions,
        post_network_id,
    })
}

/// Two groups of 3 single-NIC nodes each (`a0`,`a1`,`a2` on one flat segment; `b0`,`b1`,`b2` on
/// another) form independently — the coordinator wires both segments but withholds the
/// bridge-to-bridge L2 uplink ([`netns::WiredMesh::link_bridges`] — no node needs a second NIC)
/// until every node has confirmed it sees its own two group siblings — and only then does the
/// uplink join the two broadcast domains into one: real `ntk_hooking` merge negotiation
/// (`initial_position: None` on every node, the production path) must resolve the two groups
/// into one, with exactly one side's nodes adopting new, Coordinator-reserved positions —
/// generalizing `tests/multi_node.rs`'s own proven 2-node
/// `real_netns_two_daemons_negotiate_a_shared_network` to 6 nodes, which that file's own module
/// doc names as a documented scope boundary ("identity migration during an *already-established*
/// network's merge" — `crate::node::lifecycle`'s module doc).
///
/// # This is not a g-node migration test — `MERGE_GSIZES` has one level
/// A single-level topology has no g-node spanning more than one member: `a0`/`a1`/`a2` are three
/// peers negotiated into one shared `network_id`, each *individually* its own level-0 g-node —
/// there is no shared higher-level position for them to move as a unit. Earlier revisions of this
/// scenario asserted "exactly one group's positions changed" and "every member of the losing
/// group changed" as if group membership were itself a protocol concept here; it is not, at this
/// topology. Those assertions could not fail for the reason they claimed to check (a group
/// "migrating"), only for the coincidence of `a0..a2`'s three independent re-hooks landing on the
/// same side — so they are corrected below to the property a one-level topology actually holds:
/// all six nodes converge to one `network_id`, each at a distinct position.
/// [`two_level_gnode_migrates_as_a_unit_into_merged_network`] below is this rung's real test of
/// genuine multi-member g-node migration, over a topology (`UNIT_MERGE_GSIZES`, two levels) that
/// can actually hold that property.
///
/// # The staging is load-bearing, not incidental
/// Wiring both segments' bridges through the uplink *before* any daemon starts (this scenario's
/// original shape) means all six nodes see all five peers from their very first broadcast — no
/// group ever forms its own canonical identity, so what actually gets exercised is a six-node
/// flat bootstrap that happens to converge to one network, not a merge of two pre-existing
/// negotiated stars. That distinction is not cosmetic: now that `ntk_hooking::CoordinatorClient::
/// decide_merge` routes the merge verdict through a g-node's own Coordinator (a g-node decides
/// once, members follow), a node asking for a merge verdict before its own group has a canonical
/// identity is a different, harder scenario than the one this test's name claims to exercise. Do
/// not "simplify" this back to a single `link_bridges` call before `signal_moved` — it silently
/// changes what the test proves.
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored two_star_groups_merge_into_one_network
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn two_star_groups_merge_into_one_network() {
    init_tracing();
    // 6 node workers + this coordinator, so the uplink is created only once every node has
    // confirmed its own group's convergence — see the scenario's own doc comment for why.
    let converge_barrier = Arc::new(Barrier::new(7));
    let barrier = Arc::new(Barrier::new(6));

    let node = |idx: usize| -> (&'static str, NodeId) {
        match idx {
            0 => ("a0", NodeId::from_raw(601).unwrap()),
            1 => ("a1", NodeId::from_raw(602).unwrap()),
            2 => ("a2", NodeId::from_raw(603).unwrap()),
            3 => ("b0", NodeId::from_raw(604).unwrap()),
            4 => ("b1", NodeId::from_raw(605).unwrap()),
            5 => ("b2", NodeId::from_raw(606).unwrap()),
            _ => unreachable!(),
        }
    };

    let workers: Vec<NamespaceWorker<MergeReport>> = (0..6)
        .map(|i| {
            let (label, id) = node(i);
            let cb = converge_barrier.clone();
            let b = barrier.clone();
            NamespaceWorker::spawn(label, move || {
                merge_worker_body(label, id, MERGE_CONFIG, cb, b)
            })
        })
        .collect();
    let fds: Vec<RawFd> = workers.iter().map(NamespaceWorker::fd).collect();

    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![
        Segment {
            name: "grpa",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth0",
                },
                Member {
                    node: 1,
                    dev: "eth0",
                },
                Member {
                    node: 2,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "grpb",
            members: vec![
                Member {
                    node: 3,
                    dev: "eth0",
                },
                Member {
                    node: 4,
                    dev: "eth0",
                },
                Member {
                    node: 5,
                    dev: "eth0",
                },
            ],
        },
    ];
    let mut mesh = netns::wire(&root, &segments, &fds)
        .await
        .expect("wire merge topology");
    // Deliberately no `link_bridges` here yet: the two groups must reach their own canonical
    // identity in isolation before any inter-group path exists — see the scenario's own doc
    // comment for why this ordering is load-bearing.
    for w in &workers {
        w.signal_moved();
    }

    // Join the convergence rendezvous as the 7th party, bounded exactly like severance's sever
    // rendezvous: if any node's own group-convergence `ensure!` failed, it never reaches this
    // barrier, and an unbounded wait here would hang the whole test instead of surfacing that
    // node's real error via `w.join()` below.
    let _ = tokio::time::timeout(
        GROUP_CONVERGE_TIMEOUT + Duration::from_secs(5),
        converge_barrier.wait(),
    )
    .await;

    mesh.link_bridges(&root, "grpa", "grpb", "abup")
        .await
        .expect("uplink the two groups' bridges");

    // Join every worker unconditionally, teardown the mesh, and only then panic — see
    // `netns::join_all`'s own doc.
    let results = netns::join_all(
        workers,
        MERGE_TIMEOUT + GROUP_CONVERGE_TIMEOUT + netns::JOIN_MARGIN,
    )
    .await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    let reports: Vec<MergeReport> = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| r.unwrap_or_else(|e| panic!("merge worker {i} failed: {e:?}")))
        .collect();

    for r in &reports {
        eprintln!(
            "{}: rehooked={} pre_network_id={} pre_positions={:?} post_network_id={} \
             post_positions={:?} routes={:#?}",
            r.report.label,
            r.report.rehooked,
            r.pre_network_id,
            r.pre_positions,
            r.post_network_id,
            r.report.naddr_positions,
            r.report.routes
        );
    }

    // The property this single-level topology can actually express (see this fn's own doc
    // comment, "This is not a g-node migration test"): all six nodes converge to one shared
    // `network_id`, each at a distinct position. It cannot express "exactly one group's nodes
    // moved" or "the losing group migrated as a whole unit" — those are multi-level-g-node
    // properties, and `MERGE_GSIZES` has one level, so no g-node here spans more than one member
    // to migrate as a unit. `two_level_gnode_migrates_as_a_unit_into_merged_network` below tests
    // that property for real, over a topology that can hold it.
    //
    // `NodeReport::rehooked` (`ntkd::node::lifecycle::GenerationHandles::rehooked`) cannot
    // express even this weaker property: it is one bit, set the first time an identity ever
    // migrates away from its own trivial starting position, so it is already `true` for at least
    // two members of *each* group by the time that group's own 3-node star finishes forming —
    // before the inter-group uplink even exists. Comparing each node's own pre-uplink and
    // post-merge `(network_id, positions)` is the signal that isolates the actual cross-group
    // merge event from that noise.
    let post_network_ids: std::collections::HashSet<i64> =
        reports.iter().map(|r| r.post_network_id).collect();
    assert_eq!(
        post_network_ids.len(),
        1,
        "all six nodes must share one network_id after the merge: {:?}",
        reports
            .iter()
            .map(|r| (r.report.label.clone(), r.post_network_id))
            .collect::<Vec<_>>()
    );

    let post_positions: std::collections::HashSet<Vec<u32>> = reports
        .iter()
        .map(|r| r.report.naddr_positions.clone())
        .collect();
    assert_eq!(
        post_positions.len(),
        reports.len(),
        "all six nodes must land at distinct positions in the merged network: {:?}",
        reports
            .iter()
            .map(|r| (&r.report.label, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );

    // Whichever nodes actually re-hooked to reach the shared network must have real cross-seam
    // routes afterward (evidence the merge actually completed, not just that a position moved).
    // "Moved" replaces "losing group" here on purpose: with no g-node spanning multiple members,
    // there is no group to lose as a unit, only individual re-hooks.
    for r in reports
        .iter()
        .filter(|r| r.pre_positions != r.report.naddr_positions)
    {
        assert!(
            !r.report.routes.is_empty(),
            "{} moved but has no real kernel routes after the merge: {:#?}",
            r.report.label,
            r.report.routes
        );
    }
}

// =============================================================================================
// Scenario: a genuine multi-member g-node migrates as a unit across a merge
// =============================================================================================

const UNIT_MERGE_GSIZES: [u32; 2] = [4, 2];
const UNIT_MERGE_PORT: u16 = 27350;
/// Real-kernel evidence (this batch's own runs, not a guess) shows this budget was too tight:
/// unlike the single-level star, a 3-member *level-0* g-node under a taller topology can form in
/// two rounds, not one — two members pair up first (an ordinary star, same cost as
/// [`GROUP_CONVERGE_TIMEOUT`]), then that pair's own g-node must migrate *again* to absorb the
/// third, which additionally has to propagate the new position to the already-joined sibling
/// before it, not just the mover, is done. Measured completing at ~104s elapsed in one run
/// (a1 finishing at T+9.5s, its sibling a2 only following at T+104s) and not at all within 90s+5s
/// in two others — budgeted at roughly double [`GROUP_CONVERGE_TIMEOUT`] for that second round,
/// not tightened.
const UNIT_GROUP_CONVERGE_TIMEOUT: Duration = Duration::from_secs(180);
/// Same reasoning as [`UNIT_GROUP_CONVERGE_TIMEOUT`]'s own widening: the cross-g-node merge here
/// can require the identical two-round shape (a pairwise merge, then a second migration to bring
/// in the rest of the losing g-node), so it gets the same budget, not [`MERGE_TIMEOUT`]'s.
const UNIT_MERGE_TIMEOUT: Duration = Duration::from_secs(180);
/// Bundled [`MergeScenarioConfig`] for this scenario's own call into [`merge_worker_body`].
const UNIT_MERGE_CONFIG: MergeScenarioConfig = MergeScenarioConfig {
    gsizes: &UNIT_MERGE_GSIZES,
    port: UNIT_MERGE_PORT,
    group_converge_timeout: UNIT_GROUP_CONVERGE_TIMEOUT,
    merge_timeout: UNIT_MERGE_TIMEOUT,
};

/// Two independent 3-member level-1 g-nodes (`a0`,`a1`,`a2` on one flat segment; `b0`,`b1`,`b2`
/// on another), each formed over `UNIT_MERGE_GSIZES` = `[4, 2]` — a *two*-level topology, unlike
/// [`two_star_groups_merge_into_one_network`]'s single-level `MERGE_GSIZES` — so each trio
/// genuinely shares one level-1 position (`gsize(0) == 4` comfortably holds 3 distinct level-0
/// siblings under it) before the merge, and the merge itself is a real g-node moving as a unit,
/// not three individually-coincidental re-hooks. Staged identically to the single-level scenario
/// and for the same load-bearing reason (see that scenario's own "The staging is load-bearing"
/// section, which applies here unchanged): both trios reach their own canonical two-level
/// identity in isolation, confirmed via the barrier below, before
/// [`netns::WiredMesh::link_bridges`] ever joins the two broadcast domains.
///
/// # What "parity" means here, and what real-kernel runs actually found
/// After the merge: all six nodes share one `network_id`; the losing g-node's members all moved
/// into the winner's g-node (the *same* new shared level-1 position, checked directly, not just
/// "some new position each"); and every member still holds a distinct position. Real-kernel runs
/// of this exact scenario found and pinned a genuine defect, now fixed:
/// [`crate::node::adapters::EnterHandlersAdapter::evaluate_enter`] keyed its one-in-flight-per-
/// level arbiter by the DHT *routing* level (always `topology().levels() - 1`), while
/// `completed_enter`/`abort_enter` keyed the same map by the *negotiated* level — the two
/// coincide only for a one-level topology, so this scenario (and only this scenario, being the
/// first two-level real-kernel g-node test) deterministically wedged every retry after a
/// `target network changed during entry, aborting and redoing from start` abort into permanent
/// `AskAgain`. See that method's own doc comment for the fix and its reasoning. Separately (not a
/// defect, an empirical timing fact — see [`UNIT_GROUP_CONVERGE_TIMEOUT`]'s own doc), forming a
/// >2-member level-0 g-node under a taller topology can take a genuine second propagation round,
/// which is why this scenario's own timeouts are double the single-level scenario's.
///
/// # Current status: blocked before the merge is ever reached
/// Both HEAD and its parent commit fail this scenario at the *same* assertion and line —
/// `did not converge to its own group's two siblings before the uplink` (this file's own
/// `merge_worker_body`, the `anyhow::ensure!` a few lines above its "pre-uplink" barrier) —
/// meaning this scenario currently never gets past pre-uplink group *formation*, let alone
/// exercises the merge this doc section describes. That failure is unrelated to any
/// collective-destination change: it reproduces identically on the parent commit. See
/// [`isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit`] below for this rung's isolated
/// proof of the merge alone, over an identical topology, with formation removed from the
/// critical path.
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored two_level_gnode_migrates_as_a_unit_into_merged_network
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn two_level_gnode_migrates_as_a_unit_into_merged_network() {
    init_tracing();
    // 6 node workers + this coordinator, same rendezvous shape as the single-level scenario.
    let converge_barrier = Arc::new(Barrier::new(7));
    let barrier = Arc::new(Barrier::new(6));

    let node = |idx: usize| -> (&'static str, NodeId) {
        match idx {
            0 => ("a0", NodeId::from_raw(701).unwrap()),
            1 => ("a1", NodeId::from_raw(702).unwrap()),
            2 => ("a2", NodeId::from_raw(703).unwrap()),
            3 => ("b0", NodeId::from_raw(704).unwrap()),
            4 => ("b1", NodeId::from_raw(705).unwrap()),
            5 => ("b2", NodeId::from_raw(706).unwrap()),
            _ => unreachable!(),
        }
    };

    let workers: Vec<NamespaceWorker<MergeReport>> = (0..6)
        .map(|i| {
            let (label, id) = node(i);
            let cb = converge_barrier.clone();
            let b = barrier.clone();
            NamespaceWorker::spawn(label, move || {
                merge_worker_body(label, id, UNIT_MERGE_CONFIG, cb, b)
            })
        })
        .collect();
    let fds: Vec<RawFd> = workers.iter().map(NamespaceWorker::fd).collect();

    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![
        Segment {
            name: "ugrpa",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth0",
                },
                Member {
                    node: 1,
                    dev: "eth0",
                },
                Member {
                    node: 2,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "ugrpb",
            members: vec![
                Member {
                    node: 3,
                    dev: "eth0",
                },
                Member {
                    node: 4,
                    dev: "eth0",
                },
                Member {
                    node: 5,
                    dev: "eth0",
                },
            ],
        },
    ];
    let mut mesh = netns::wire(&root, &segments, &fds)
        .await
        .expect("wire unit-migration topology");
    // Deliberately no `link_bridges` here yet — see the scenario's own doc comment.
    for w in &workers {
        w.signal_moved();
    }

    let _ = tokio::time::timeout(
        UNIT_GROUP_CONVERGE_TIMEOUT + Duration::from_secs(5),
        converge_barrier.wait(),
    )
    .await;

    mesh.link_bridges(&root, "ugrpa", "ugrpb", "uabup")
        .await
        .expect("uplink the two g-nodes' bridges");

    // Join every worker unconditionally, teardown the mesh, and only then panic — see
    // `netns::join_all`'s own doc.
    let results = netns::join_all(
        workers,
        UNIT_MERGE_TIMEOUT + UNIT_GROUP_CONVERGE_TIMEOUT + netns::JOIN_MARGIN,
    )
    .await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    let reports: Vec<MergeReport> = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| r.unwrap_or_else(|e| panic!("unit-migration worker {i} failed: {e:?}")))
        .collect();

    for r in &reports {
        eprintln!(
            "{}: pre_network_id={} pre_positions={:?} post_network_id={} post_positions={:?} \
             routes={:#?}",
            r.report.label,
            r.pre_network_id,
            r.pre_positions,
            r.post_network_id,
            r.report.naddr_positions,
            r.report.routes
        );
    }

    let a_labels = ["a0", "a1", "a2"];
    let b_labels = ["b0", "b1", "b2"];

    // Sanity precondition, not the property under test: each trio genuinely converged to share
    // one level-1 g-node before the uplink ever existed (index 1 of `pre_positions`, the outer
    // of `UNIT_MERGE_GSIZES`'s two levels). If this does not hold, the scenario never reached the
    // two-level starting condition its name claims to exercise.
    for labels in [&a_labels[..], &b_labels[..]] {
        let level1: std::collections::HashSet<u32> = reports
            .iter()
            .filter(|r| labels.contains(&r.report.label.as_str()))
            .map(|r| r.pre_positions[1])
            .collect();
        assert_eq!(
            level1.len(),
            1,
            "{labels:?} must share one level-1 g-node before the uplink: {:?}",
            reports
                .iter()
                .filter(|r| labels.contains(&r.report.label.as_str()))
                .map(|r| (&r.report.label, &r.pre_positions))
                .collect::<Vec<_>>()
        );
    }

    // The property this two-level topology can express that the single-level scenario above
    // cannot (see that scenario's own "This is not a g-node migration test" section): after the
    // merge every node shares one `network_id`, the losing g-node's members all moved into the
    // winner's g-node as a unit, and every member still holds a distinct position.
    let post_network_ids: std::collections::HashSet<i64> =
        reports.iter().map(|r| r.post_network_id).collect();
    assert_eq!(
        post_network_ids.len(),
        1,
        "all six nodes must share one network_id after the merge: {:?}",
        reports
            .iter()
            .map(|r| (r.report.label.clone(), r.post_network_id))
            .collect::<Vec<_>>()
    );

    let moved = |labels: &[&str]| -> usize {
        reports
            .iter()
            .filter(|r| {
                labels.contains(&r.report.label.as_str())
                    && r.pre_positions != r.report.naddr_positions
            })
            .count()
    };
    let a_moved = moved(&a_labels);
    let b_moved = moved(&b_labels);
    assert!(
        (a_moved == 0) != (b_moved == 0),
        "exactly one g-node's members should migrate for the merge, never both or neither: \
         a_moved={a_moved} b_moved={b_moved}"
    );
    let (losing_labels, winning_labels): (&[&str], &[&str]) = if a_moved > 0 {
        (&a_labels, &b_labels)
    } else {
        (&b_labels, &a_labels)
    };
    assert_eq!(
        moved(losing_labels),
        losing_labels.len(),
        "the losing g-node must migrate as a whole unit, not partially: {:?}",
        reports
            .iter()
            .filter(|r| losing_labels.contains(&r.report.label.as_str()))
            .map(|r| (&r.report.label, &r.pre_positions, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        moved(winning_labels),
        0,
        "the winning g-node must keep its own positions untouched by the merge: {:?}",
        reports
            .iter()
            .filter(|r| winning_labels.contains(&r.report.label.as_str()))
            .map(|r| (&r.report.label, &r.pre_positions, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );

    // Genuine unit migration, not three independent re-hooks that happen to land in the same
    // network by coincidence: the losing g-node's members must all share the *same* new level-1
    // position as the (unmoved) winning g-node.
    let winner_level1: std::collections::HashSet<u32> = reports
        .iter()
        .filter(|r| winning_labels.contains(&r.report.label.as_str()))
        .map(|r| r.report.naddr_positions[1])
        .collect();
    assert_eq!(
        winner_level1.len(),
        1,
        "the winning g-node must keep one shared level-1 position: {:?}",
        reports
            .iter()
            .filter(|r| winning_labels.contains(&r.report.label.as_str()))
            .map(|r| (&r.report.label, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );
    let winner_level1 = *winner_level1
        .iter()
        .next()
        .expect("checked len() == 1 above");
    for label in losing_labels {
        let report = reports
            .iter()
            .find(|r| r.report.label == *label)
            .unwrap_or_else(|| panic!("missing report for {label}"));
        assert_eq!(
            report.report.naddr_positions[1], winner_level1,
            "{label} (losing g-node) must share the winner's level-1 position after migrating \
             as a unit: {:?}",
            report.report.naddr_positions
        );
    }

    // Every member still holds a distinct position after the merge — the shared g-node's members
    // remain individually addressable, not collapsed onto one another.
    let post_positions: std::collections::HashSet<Vec<u32>> = reports
        .iter()
        .map(|r| r.report.naddr_positions.clone())
        .collect();
    assert_eq!(
        post_positions.len(),
        reports.len(),
        "all six nodes must land at distinct positions in the merged network: {:?}",
        reports
            .iter()
            .map(|r| (&r.report.label, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );

    // The losing g-node's members must all have real cross-seam routes after the merge (evidence
    // the merge actually completed, not just that positions changed).
    for label in losing_labels {
        let report = reports
            .iter()
            .find(|r| r.report.label == *label)
            .unwrap_or_else(|| panic!("missing report for {label}"));
        assert!(
            !report.report.routes.is_empty(),
            "{label} (losing g-node) has no real kernel routes after the merge: {:#?}",
            report.report.routes
        );
    }
}

// =============================================================================================
// Scenario: the merge alone, with formation removed from the critical path
// =============================================================================================

const ISOLATED_MERGE_PORT: u16 = 27360;
/// No hooking negotiation happens for the pre-uplink starting state here (see this scenario's
/// own doc's "Isolating formation" section) — only ordinary QSPN/neighborhood convergence gates
/// the barrier below, and the module doc already establishes that settles "well under a second"
/// at this fixture's intervals. Budgeted at 20s: the same flat-margin convention this file
/// already uses for its other two no-hooking-needed scenarios ([`CHAIN_TIMEOUT`],
/// [`LEVEL1_TIMEOUT`], both 40s over a smaller topology; 20s here is still a wide multiple of
/// the sub-second floor it budgets).
const ISOLATED_GROUP_CONVERGE_TIMEOUT: Duration = Duration::from_secs(20);
/// Derived from the merge's own cost alone, now that formation no longer shares this budget
/// (see this scenario's own doc): one hooking restart-from-start floor is 20s (module doc). Up
/// to all 3 of the losing g-node's members can serialize through the Coordinator's single
/// in-flight-id-per-level arbiter within one round (the reasoning [`MERGE_TIMEOUT`]'s own doc
/// already gives for 3 simultaneous negotiators) — 3 * 20s = 60s. The same two-round shape
/// [`UNIT_GROUP_CONVERGE_TIMEOUT`]'s own doc measured for multi-member *formation* (a pairwise
/// round, then a second round for the last member to follow and propagate to its
/// already-migrated sibling) can recur for a multi-member *merge* too, doubling that to 120s.
/// +20s flat scheduling/CI margin (this file's own convention) = 140s.
const ISOLATED_MERGE_TIMEOUT: Duration = Duration::from_secs(140);
/// Bundled [`MergeScenarioConfig`] for this scenario's own call into
/// [`isolated_merge_worker_body`]; reuses [`UNIT_MERGE_GSIZES`] — the identical topology
/// [`two_level_gnode_migrates_as_a_unit_into_merged_network`] uses — so this isolates *only* the
/// formation cost, not the topology under test.
const ISOLATED_MERGE_CONFIG: MergeScenarioConfig = MergeScenarioConfig {
    gsizes: &UNIT_MERGE_GSIZES,
    port: ISOLATED_MERGE_PORT,
    group_converge_timeout: ISOLATED_GROUP_CONVERGE_TIMEOUT,
    merge_timeout: ISOLATED_MERGE_TIMEOUT,
};

/// `a0`/`a1`/`a2`'s and `b0`/`b1`/`b2`'s explicit starting `Naddr` positions under
/// [`UNIT_MERGE_GSIZES`] (`[4, 2]`): level-0 (`gsize(0) == 4`) comfortably holds 3 distinct
/// siblings per trio; level-1 (`gsize(1) == 2`) gives each trio its own slot, `a` at 0 and `b`
/// at 1 — the same shape [`two_level_gnode_migrates_as_a_unit_into_merged_network`]'s trios
/// negotiate their way into via real (slow) hooking; here it is one half of [`isolated_preformed`]
/// ([`NodeInputs::preformed`]'s own `position` field — see that function's doc for the other
/// half this position alone could never express).
fn isolated_position(idx: u32) -> Vec<u32> {
    match idx {
        0 => vec![0, 0],
        1 => vec![1, 0],
        2 => vec![2, 0],
        3 => vec![0, 1],
        4 => vec![1, 1],
        5 => vec![2, 1],
        _ => unreachable!("isolated-merge mesh has exactly 6 nodes"),
    }
}

/// The `network_id` [`isolated_preformed`] shares across each trio: `a0`/`a1`/`a2` (idx `0..=2`)
/// get one value, `b0`/`b1`/`b2` (idx `3..=5`) a distinct one. This is the piece a bare
/// `initial_position` could never express (see this scenario's own "What three real-kernel runs
/// actually found" section): without it, every node's `network_id` stays `random_i64()`-per-node
/// regardless of position, so a trio sharing a level-1 coordinate is invisible to
/// `ntk_hooking::merge::merge_direction`/`merge_tiebreak`, which read only `network_id` and
/// `n_nodes`, never positions.
fn isolated_network_id(idx: u32) -> i64 {
    match idx {
        0..=2 => 91_000_001,
        3..=5 => 91_000_002,
        _ => unreachable!("isolated-merge mesh has exactly 6 nodes"),
    }
}

/// [`NodeInputs::preformed`] for member `idx`: [`isolated_position`]'s explicit position paired
/// with [`isolated_network_id`]'s trio-shared id — together, the two pieces a real Coordinator
/// would have already resolved for a g-node that formed before this test's processes existed.
/// Passed as `preformed` (not `initial_position`) to [`netns::spawn_node`], so `negotiated`
/// stays `true` and the trio can still merge once bridged (`NodeInputs::preformed`'s own doc).
fn isolated_preformed(idx: u32) -> PreformedNetwork {
    PreformedNetwork {
        network_id: isolated_network_id(idx),
        position: isolated_position(idx),
    }
}

/// [`merge_worker_body`]'s own shape, minus the pre-uplink negotiated-formation wait:
/// [`isolated_preformed`] ([`NodeInputs::preformed`]'s own doc) means each trio already looks,
/// to both QSPN *and* `ntk-hooking`, like one converged, already-networked g-node the instant
/// its arcs come up — no hooking star, no per-member retry cycles, nothing formation's own cost
/// was ever spent on, and (unlike a bare `initial_position`) no six-way `network_id` chaos once
/// the uplink joins the two trios. Not a copy-paste of `merge_worker_body` with an added
/// parameter: that function is [`two_level_gnode_migrates_as_a_unit_into_merged_network`]'s and
/// [`two_star_groups_merge_into_one_network`]'s own proven, unmodified code path, and this
/// scenario's whole point is to change nothing about *their* behavior while adding a third,
/// differently-staged one.
async fn isolated_merge_worker_body(
    label: &'static str,
    my_id: NodeId,
    preformed: PreformedNetwork,
    config: MergeScenarioConfig,
    converge_barrier: Arc<Barrier>,
    barrier: Arc<Barrier>,
) -> anyhow::Result<MergeReport> {
    let devs = ["eth0"];
    let dev_index = netns::bring_up_devs(&devs).await?;
    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        None,
        Some(preformed),
        config.gsizes,
        &devs,
        config.port,
        &mut tasks,
        cancel.clone(),
    )
    .await?;

    // Plain QSPN/neighborhood convergence only — no hooking negotiation, since the position is
    // already authoritative (see this function's own doc).
    let group_converged = wait_until(
        || {
            let snapshot = started.running.generation.borrow().qspn.snapshot();
            snapshot
                .levels
                .first()
                .is_some_and(|level| level.len() == 2)
        },
        config.group_converge_timeout,
    )
    .await;
    anyhow::ensure!(
        group_converged,
        "{label}: did not converge to its own group's two siblings before the uplink: {:#?}",
        started.running.generation.borrow().qspn.snapshot()
    );

    // Snapshot this node's own pre-uplink (network_id, positions) — same "before" half of the
    // property [`merge_worker_body`] captures, and for the identical reason.
    let pre_network_id = started.running.net.network_id();
    let pre_positions = started
        .running
        .generation
        .borrow()
        .qspn
        .my_naddr()
        .positions()
        .to_vec();

    // Bounded for the same reason `merge_worker_body`'s own rendezvous is: an unbounded wait
    // here would hang the whole test on one other worker's convergence failure instead of
    // surfacing it via that worker's own `w.join()`.
    let _ = tokio::time::timeout(
        config.group_converge_timeout + Duration::from_secs(5),
        converge_barrier.wait(),
    )
    .await;

    // No fixed convergence predicate is possible for the cross-group merge itself — same
    // reasoning as `merge_worker_body`'s own identical sleep.
    tokio::time::sleep(config.merge_timeout).await;

    let report = netns::observe(label, &started, dev_index).await?;
    let post_network_id = started.running.net.network_id();
    let _ = tokio::time::timeout(Duration::from_secs(30), barrier.wait()).await;
    netns::teardown(&started, cancel, &mut tasks).await;
    Ok(MergeReport {
        report,
        pre_network_id,
        pre_positions,
        post_network_id,
    })
}

/// Two independent 3-member level-1 g-nodes, identical in every respect to
/// [`two_level_gnode_migrates_as_a_unit_into_merged_network`] (same [`UNIT_MERGE_GSIZES`], same
/// staged uplink) except one: every node starts already a member of its own trio's real, shared
/// network ([`isolated_preformed`], [`NodeInputs::preformed`]) instead of negotiating one from
/// scratch, so pre-uplink group formation — that scenario's own cost, per its "Current status"
/// doc section — is no longer the thing this scenario's deadline is spent waiting on.
///
/// # What three real-kernel runs actually found: `initial_position` cannot isolate this merge
/// It does not work, and three real-kernel runs (below) show precisely why, deterministically,
/// not as a timing fluke. `network_id` is *not* derived from `initial_position` —
/// `crate::node::lifecycle::run` always assigns it `random_i64()`, unconditionally, regardless
/// of whether the position itself came from the caller or from negotiation. So all six of this
/// scenario's nodes still bootstrap as six independent single-node networks that must still
/// resolve their `network_id`s via the exact same real `ntk_hooking` arc-handler dance
/// [`two_level_gnode_migrates_as_a_unit_into_merged_network`] pays for — sharing a level-1
/// coordinate is invisible to `merge_direction`/`merge_tiebreak`, which only ever compare
/// `network_id` and `n_nodes`. Concretely, this reproduces
/// [`two_star_groups_merge_into_one_network`]'s own documented anti-pattern ("no group ever
/// forms its own canonical identity") at six-way scale instead of skipping it.
///
/// Compounding that: `initial_position: Some(_)` also sets `SteadyStateCtx::negotiated = false`
/// (`crate::node::lifecycle`'s module doc, "Negotiated re-address"), which unconditionally
/// blocks `migrate` (`if !ctx.negotiated { return None; }`, that function's own entry) — so none
/// of the six-way chaos above can ever land as an actual position change, in any run, for any
/// of the six nodes, regardless of what `network_id` itself does. Runs 1-3 below were taken
/// against a `migrate` that additionally had `on_hooking_event`'s `DoFinishEnter` arm call
/// `ctx.net.set_network_id(..)` *unconditionally*, *before* the `negotiated` guard — a real,
/// separately production-reachable ordering defect (not test-only: `migrate`'s other early
/// returns, e.g. `ctx.migration_in_progress`, hit the identical gap) that let a node's reported
/// `network_id` advance past its actual position/routes, and that let six independently-racing
/// arbitrations land on anywhere from 1 to 6 distinct final `network_id`s depending on timing.
/// That ordering has since been fixed (`migrate` now sets `ctx.net`'s id itself, only after its
/// own guards) by concurrent work on `crate::node::lifecycle` this same batch; run 4 is the
/// first taken after that fix landed, and is fully deterministic: every node's `network_id`
/// *and* position are untouched by the merge, because `migrate` now never runs far enough to
/// touch either.
///
/// Four real-kernel runs, each `140.2`-`140.6`s (this scenario's own [`ISOLATED_MERGE_TIMEOUT`]
/// budget, run out in full — not a timeout-induced abort):
///
/// | run | distinct post-merge `network_id`s | positions changed |
/// |---|---|---|
/// | 1 (pre-fix) | 1 (all six converged) | 0 of 6 |
/// | 2 (pre-fix) | 2 ({a0,a2,b0}, {a1,b1,b2}) | 0 of 6 |
/// | 3 (pre-fix) | 6 (no convergence at all) | 0 of 6 |
/// | 4 (post-fix) | 6 (no convergence at all, deterministic) | 0 of 6 |
///
/// # The fix: `NodeInputs::preformed`
/// The missing capability was never "start at a shared position" — `initial_position` already
/// did that — it was "start already sharing a `network_id`", which nothing on the `NodeInputs`
/// surface could express before this batch (see this module's own "Negotiated re-address"
/// section's "`NodeInputs::preformed`: pre-formed, not frozen" for the exact distinction).
/// [`isolated_preformed`] now supplies both pieces — [`isolated_position`]'s coordinate *and*
/// [`isolated_network_id`]'s trio-shared id — passed as `preformed`, never `initial_position`,
/// so `negotiated` stays `true` and each trio's internal arcs resolve
/// `ntk_hooking::QspnView::note_same_network` instead of racing six independent `network_id`
/// arbitrations the moment they come up. Confirmed live: `decide_merge` sees
/// `neighbor_n_nodes=3`/`my_n_nodes=Some(3)` for the *whole* trio before any negotiation starts
/// — the six-way chaos the "What three real-kernel runs actually found" section documents is
/// gone.
///
/// This does isolate the merge — and the merge alone now fails, genuinely, every time it reaches
/// a clean outcome. Run 1 (140.28s, the only run whose `netns::teardown` returned within any
/// bounded wait — see the second finding below) completed with all six nodes correctly
/// converging to one `network_id` (`91000002`), but not as one unit: `a0` (the negotiator,
/// `ask_lvl=1`, the collective destination) landed at `[0, 0]` (level-1 slot 0, numerically
/// unchanged by coincidence — the same kind [`GenerationHandles::rehooked`]'s own doc already
/// documents); `a1` and `a2` never received or followed that collective destination — each
/// independently ran its *own* `ask_lvl=0` `evaluate_enter`/`reserve` round trip into `b`'s
/// g-node, and `CoordinatorMapAdapter::free_positions` handed *both* of them `new_pos: 1` at
/// `host_lvl: 1`, so both landed at the identical final `Naddr` `[1, 1]` — a real
/// duplicate-position bug, not a test artifact. (The assertion below actually fires one line
/// earlier, on the "moved as a whole unit" count — `a0`'s coincidentally-unchanged position
/// makes it count as "2 moved" instead of "3 moved" — but the duplicate `[1, 1]` is the more
/// serious finding underneath it.) Three further runs reproduce the identical shape (independent
/// `ask_lvl=0` entrants, never the propagated collective destination) with different colliding
/// positions each time — in one, `a1`/`a2` each landed exactly on `b1`'s/`b2`'s own
/// already-installed address instead of each other's. Full evidence, and the handoff, went to
/// `PlacementFix` (`crates/ntkd/src/node/adapters.rs`'s `CoordinatorMapAdapter::free_positions`,
/// `ntk-hooking`/`ntk-coordinator`) via `hub`.
///
/// **A second, separate finding**: `netns::teardown` after the fixed 140s observation sleep does
/// not reliably return. Only the least-churny run (1) completed within any bounded wait; three
/// further runs each printed nothing at all for 300-460s after the sleep ended, still with
/// `ntk_hooking::arc::run_arc_handler`'s own "target network changed during entry, aborting and
/// redoing from start" cycle and reverse (b-into-a) merge attempts still active at the moment of
/// cancellation. Not chased further here — outside this task's scope
/// (`crates/ntkd/src/node/adapters.rs`, `ntk-hooking`) — but flagged to `PlacementFix` since a
/// fix that stops the independent-entrant race might also stop the churn that seems to trigger
/// it.
///
/// # Running
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test mesh -- --ignored isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit
/// ```
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit() {
    init_tracing();
    let converge_barrier = Arc::new(Barrier::new(7));
    let barrier = Arc::new(Barrier::new(6));

    let node = |idx: usize| -> (&'static str, NodeId) {
        match idx {
            0 => ("a0", NodeId::from_raw(801).unwrap()),
            1 => ("a1", NodeId::from_raw(802).unwrap()),
            2 => ("a2", NodeId::from_raw(803).unwrap()),
            3 => ("b0", NodeId::from_raw(804).unwrap()),
            4 => ("b1", NodeId::from_raw(805).unwrap()),
            5 => ("b2", NodeId::from_raw(806).unwrap()),
            _ => unreachable!(),
        }
    };

    let workers: Vec<NamespaceWorker<MergeReport>> = (0..6)
        .map(|i| {
            let (label, id) = node(i);
            let cb = converge_barrier.clone();
            let b = barrier.clone();
            NamespaceWorker::spawn(label, move || {
                isolated_merge_worker_body(
                    label,
                    id,
                    isolated_preformed(i as u32),
                    ISOLATED_MERGE_CONFIG,
                    cb,
                    b,
                )
            })
        })
        .collect();
    let fds: Vec<RawFd> = workers.iter().map(NamespaceWorker::fd).collect();

    let (root, root_driver) = netns::root_handle_with_driver().expect("root rtnetlink handle");
    let segments = vec![
        Segment {
            name: "igrpa",
            members: vec![
                Member {
                    node: 0,
                    dev: "eth0",
                },
                Member {
                    node: 1,
                    dev: "eth0",
                },
                Member {
                    node: 2,
                    dev: "eth0",
                },
            ],
        },
        Segment {
            name: "igrpb",
            members: vec![
                Member {
                    node: 3,
                    dev: "eth0",
                },
                Member {
                    node: 4,
                    dev: "eth0",
                },
                Member {
                    node: 5,
                    dev: "eth0",
                },
            ],
        },
    ];
    let mut mesh = netns::wire(&root, &segments, &fds)
        .await
        .expect("wire isolated-merge topology");
    // Deliberately no `link_bridges` here yet — see `two_level_gnode_migrates_as_a_unit_into_
    // merged_network`'s own "The staging is load-bearing" section, which applies here unchanged
    // even though each trio's own formation is now instant rather than negotiated: the merge
    // itself must still start from two separately-addressable domains, not one flat bootstrap.
    for w in &workers {
        w.signal_moved();
    }

    let _ = tokio::time::timeout(
        ISOLATED_GROUP_CONVERGE_TIMEOUT + Duration::from_secs(5),
        converge_barrier.wait(),
    )
    .await;

    mesh.link_bridges(&root, "igrpa", "igrpb", "iabup")
        .await
        .expect("uplink the two g-nodes' bridges");

    // Join every worker unconditionally, teardown the mesh, and only then panic — see
    // `netns::join_all`'s own doc.
    let results = netns::join_all(
        workers,
        ISOLATED_MERGE_TIMEOUT + ISOLATED_GROUP_CONVERGE_TIMEOUT + netns::JOIN_MARGIN,
    )
    .await;
    netns::teardown_mesh(mesh, root, root_driver).await;
    let reports: Vec<MergeReport> = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| r.unwrap_or_else(|e| panic!("isolated-merge worker {i} failed: {e:?}")))
        .collect();

    for r in &reports {
        eprintln!(
            "{}: pre_network_id={} pre_positions={:?} post_network_id={} post_positions={:?} \
             routes={:#?}",
            r.report.label,
            r.pre_network_id,
            r.pre_positions,
            r.post_network_id,
            r.report.naddr_positions,
            r.report.routes
        );
    }

    let a_labels = ["a0", "a1", "a2"];
    let b_labels = ["b0", "b1", "b2"];

    // Sanity precondition, not the property under test: each trio's asserted starting position
    // and shared `network_id` really do make it one pre-formed g-node, instantly, before the
    // uplink existed — this is what a bare `initial_position` could never establish (see this
    // scenario's own "What three real-kernel runs actually found" section).
    for labels in [&a_labels[..], &b_labels[..]] {
        let level1: std::collections::HashSet<u32> = reports
            .iter()
            .filter(|r| labels.contains(&r.report.label.as_str()))
            .map(|r| r.pre_positions[1])
            .collect();
        assert_eq!(
            level1.len(),
            1,
            "{labels:?} must share one level-1 g-node before the uplink: {:?}",
            reports
                .iter()
                .filter(|r| labels.contains(&r.report.label.as_str()))
                .map(|r| (&r.report.label, &r.pre_positions))
                .collect::<Vec<_>>()
        );
        let pre_network_ids: std::collections::HashSet<i64> = reports
            .iter()
            .filter(|r| labels.contains(&r.report.label.as_str()))
            .map(|r| r.pre_network_id)
            .collect();
        assert_eq!(
            pre_network_ids.len(),
            1,
            "{labels:?} must already share one network_id before the uplink — the piece a bare \
             `initial_position` could never establish: {:?}",
            reports
                .iter()
                .filter(|r| labels.contains(&r.report.label.as_str()))
                .map(|r| (&r.report.label, r.pre_network_id))
                .collect::<Vec<_>>()
        );
    }

    // The property this scenario isolates: after the merge, all six nodes share one
    // `network_id` — with formation no longer competing for the deadline above, this is now a
    // pure signal of whether the cross-group merge itself completed.
    let post_network_ids: std::collections::HashSet<i64> =
        reports.iter().map(|r| r.post_network_id).collect();
    assert_eq!(
        post_network_ids.len(),
        1,
        "all six nodes must share one network_id after the merge: {:?}",
        reports
            .iter()
            .map(|r| (r.report.label.clone(), r.post_network_id))
            .collect::<Vec<_>>()
    );

    // Which trio migrated is read from `network_id` adoption, never from a bare position diff.
    //
    // The previous `pre_positions != naddr_positions` heuristic could not express this property
    // under this topology and produced false negatives on genuinely correct merges. With
    // `UNIT_MERGE_GSIZES == [4, 2]` level 1 has exactly two slots, `a` preformed at 0 and `b` at
    // 1 ([`isolated_position`]). When the loser migrates into the winner's network it must take
    // the one level-1 slot the winner does not hold — which is *numerically its own former slot*,
    // the only one left. So a correct unit migration leaves every position identical and changes
    // only the network the trio belongs to, and the diff heuristic read that as "nobody moved".
    // `network_id` is the unambiguous signal: the losing trio adopts the winner's, the winner's
    // own never changes.
    let adopted = |labels: &[&str]| -> usize {
        reports
            .iter()
            .filter(|r| {
                labels.contains(&r.report.label.as_str()) && r.pre_network_id != r.post_network_id
            })
            .count()
    };
    let a_adopted = adopted(&a_labels);
    let b_adopted = adopted(&b_labels);
    assert!(
        (a_adopted == 0) != (b_adopted == 0),
        "exactly one g-node must adopt the other's network_id, never both or neither: \
         a_adopted={a_adopted} b_adopted={b_adopted}"
    );
    let (losing_labels, winning_labels): (&[&str], &[&str]) = if a_adopted > 0 {
        (&a_labels, &b_labels)
    } else {
        (&b_labels, &a_labels)
    };
    assert_eq!(
        adopted(losing_labels),
        losing_labels.len(),
        "the losing g-node must migrate as a whole unit, not partially — every member adopts the \
         winner's network_id: {:?}",
        reports
            .iter()
            .filter(|r| losing_labels.contains(&r.report.label.as_str()))
            .map(|r| (
                &r.report.label,
                r.pre_network_id,
                r.post_network_id,
                &r.pre_positions,
                &r.report.naddr_positions
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        adopted(winning_labels),
        0,
        "the winning g-node must keep its own network_id untouched by the merge: {:?}",
        reports
            .iter()
            .filter(|r| winning_labels.contains(&r.report.label.as_str()))
            .map(|r| (&r.report.label, r.pre_network_id, r.post_network_id))
            .collect::<Vec<_>>()
    );

    // Genuine *unit* migration: the losing g-node's members all share one level-1 position, and
    // that position is distinct from the winner's — a g-node moving intact keeps its internal
    // structure and occupies a slot of its own. It does not dissolve into the winner's g-node:
    // under `gsize(0) == 4` a single level-1 slot cannot even hold all six nodes, so demanding a
    // shared slot here would be unsatisfiable as well as wrong.
    let shared_level1 = |labels: &[&str]| -> u32 {
        let level1: std::collections::HashSet<u32> = reports
            .iter()
            .filter(|r| labels.contains(&r.report.label.as_str()))
            .map(|r| r.report.naddr_positions[1])
            .collect();
        assert_eq!(
            level1.len(),
            1,
            "{labels:?} must share exactly one level-1 position after the merge: {:?}",
            reports
                .iter()
                .filter(|r| labels.contains(&r.report.label.as_str()))
                .map(|r| (&r.report.label, &r.report.naddr_positions))
                .collect::<Vec<_>>()
        );
        *level1.iter().next().expect("checked len() == 1 above")
    };
    let winner_level1 = shared_level1(winning_labels);
    let loser_level1 = shared_level1(losing_labels);
    assert_ne!(
        loser_level1,
        winner_level1,
        "the migrated g-node must occupy its own level-1 slot, not the winner's: {:?}",
        reports
            .iter()
            .map(|r| (&r.report.label, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );

    // A g-node migrating intact preserves its members' level-0 positions: the unit moves, its
    // internal addressing does not get renegotiated member by member.
    for report in &reports {
        assert_eq!(
            report.report.naddr_positions[0], report.pre_positions[0],
            "{} must keep its level-0 position through a unit migration: {:?} -> {:?}",
            report.report.label, report.pre_positions, report.report.naddr_positions
        );
    }

    // Every level-0 position within each g-node — and every full position across all six —
    // stays distinct: the shared g-node's members remain individually addressable, never
    // collapsed onto one another.
    let post_positions: std::collections::HashSet<Vec<u32>> = reports
        .iter()
        .map(|r| r.report.naddr_positions.clone())
        .collect();
    assert_eq!(
        post_positions.len(),
        reports.len(),
        "all six nodes must land at distinct positions in the merged network: {:?}",
        reports
            .iter()
            .map(|r| (&r.report.label, &r.report.naddr_positions))
            .collect::<Vec<_>>()
    );

    // The losing g-node's members must all have real cross-seam kernel routes after the merge —
    // evidence the merge actually completed, not just that positions/network_id changed.
    for label in losing_labels {
        let report = reports
            .iter()
            .find(|r| r.report.label == *label)
            .unwrap_or_else(|| panic!("missing report for {label}"));
        assert!(
            !report.report.routes.is_empty(),
            "{label} (losing g-node) has no real kernel routes after the merge: {:#?}",
            report.report.routes
        );
    }
}
