//! Multi-node convergence tests over the in-memory
//! [`ntk_qspn::FakeQspnStubFactory`]: a small g-node topology converges to
//! the expected route set, an arc flap converges back, and a partition
//! produces the split signal only after the documented debounce.

mod support;

use std::time::Duration;

use ntk_common::{Cost, HCoord};
use ntk_qspn::QspnEvent;
use support::{Node, fast_config, link, naddr, settle, topology, wait_for};

fn cost_to(snapshot: &ntk_qspn::RouteSnapshot, level: usize, pos: u32) -> Option<Cost> {
    snapshot
        .levels
        .get(level)?
        .iter()
        .find(|e| e.destination == HCoord::new(level, pos))?
        .paths
        .first()
        .map(|p| p.cost)
}

/// A 3-node chain A-B-C converges so each node learns the other two, with
/// costs accumulating hop-by-hop.
#[tokio::test]
async fn chain_topology_converges_to_expected_route_set() {
    let topo = topology();
    let a = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());
    let b = Node::spawn(naddr(&topo, [1, 0]), 2, fast_config());
    let c = Node::spawn(naddr(&topo, [2, 0]), 3, fast_config());

    link(&a, &b, Cost::Finite(10)).await;
    link(&b, &c, Cost::Finite(10)).await;

    let ok = wait_for(
        || {
            cost_to(&a.handle.snapshot(), 0, 1) == Some(Cost::Finite(10))
                && cost_to(&a.handle.snapshot(), 0, 2) == Some(Cost::Finite(20))
                && cost_to(&c.handle.snapshot(), 0, 1) == Some(Cost::Finite(10))
                && cost_to(&c.handle.snapshot(), 0, 0) == Some(Cost::Finite(20))
                && cost_to(&b.handle.snapshot(), 0, 0) == Some(Cost::Finite(10))
                && cost_to(&b.handle.snapshot(), 0, 2) == Some(Cost::Finite(10))
        },
        200,
    )
    .await;
    assert!(
        ok,
        "chain did not converge: a={:?} b={:?} c={:?}",
        a.handle.snapshot(),
        b.handle.snapshot(),
        c.handle.snapshot()
    );
}

/// An arc flap (remove then re-add) converges back to the same route set.
#[tokio::test]
async fn arc_flap_reconverges() {
    let topo = topology();
    let a = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());
    let b = Node::spawn(naddr(&topo, [1, 0]), 2, fast_config());
    let c = Node::spawn(naddr(&topo, [2, 0]), 3, fast_config());

    link(&a, &b, Cost::Finite(10)).await;
    let (bc_on_b, _bc_on_c) = link(&b, &c, Cost::Finite(10)).await;

    assert!(
        wait_for(
            || cost_to(&a.handle.snapshot(), 0, 2) == Some(Cost::Finite(20)),
            200
        )
        .await,
        "initial convergence failed"
    );

    // Flap: remove B<->C, confirm C drops out of A's map, then re-add and
    // confirm it reconverges to the same cost.
    b.handle.remove_arc(bc_on_b).await.unwrap();
    b.factory.disconnect(bc_on_b);
    assert!(
        wait_for(|| cost_to(&a.handle.snapshot(), 0, 2).is_none(), 200).await,
        "C did not withdraw after arc removal: {:?}",
        a.handle.snapshot()
    );

    link(&b, &c, Cost::Finite(10)).await;
    assert!(
        wait_for(
            || cost_to(&a.handle.snapshot(), 0, 2) == Some(Cost::Finite(20)),
            200
        )
        .await,
        "did not reconverge after flap: {:?}",
        a.handle.snapshot()
    );
}

/// The middle node's two arcs both survive `arc_is_changed`'s full re-gather
/// (`qspn.vala:800-911`, `handle_gather_complete`/`spawn_gather`), which
/// fetches a *fresh* full ETP from every one of its arcs and merges all of
/// them into a single `update_map` call — the multi-arc-full-ETPs-in-one-
/// batch shape `chain_topology_converges_to_expected_route_set` never
/// exercises (its two arcs each only ever go through the single-arc
/// `handle_arc_add_fetched` path, one at a time, fully settled before the
/// next starts). This is the shape that surfaced the `ntkd` multi-node
/// integration defect (session `QspnWithdrawFix`); `ntk-qspn` itself proved
/// innocent (see `tests/implicit_withdrawal.rs`'s cross-arc test) but this
/// pins the multi-arc-gather code path regardless, since nothing else did.
#[tokio::test]
async fn middle_node_survives_multi_arc_regather_after_convergence() {
    let topo = topology();
    let a = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());
    let b = Node::spawn(naddr(&topo, [1, 0]), 2, fast_config());
    let c = Node::spawn(naddr(&topo, [2, 0]), 3, fast_config());

    let (_ab_on_a, ab_on_b) = link(&a, &b, Cost::Finite(10)).await;
    let (bc_on_b, _bc_on_c) = link(&b, &c, Cost::Finite(20)).await;

    assert!(
        wait_for(
            || cost_to(&b.handle.snapshot(), 0, 0) == Some(Cost::Finite(10))
                && cost_to(&b.handle.snapshot(), 0, 2) == Some(Cost::Finite(20))
                && cost_to(&a.handle.snapshot(), 0, 2) == Some(Cost::Finite(30))
                && cost_to(&c.handle.snapshot(), 0, 0) == Some(Cost::Finite(30)),
            200
        )
        .await,
        "initial convergence failed: b={:?}",
        b.handle.snapshot()
    );

    // Re-report both arcs' costs unchanged, exactly as neighborhood's own
    // radar does on every measurement tick — each call re-gathers a full
    // ETP from *every* one of b's arcs and merges them in one `update_map`.
    b.handle
        .arc_changed(ab_on_b, Cost::Finite(10))
        .await
        .unwrap();
    settle().await;
    b.handle
        .arc_changed(bc_on_b, Cost::Finite(20))
        .await
        .unwrap();
    settle().await;

    assert!(
        wait_for(
            || cost_to(&b.handle.snapshot(), 0, 0) == Some(Cost::Finite(10))
                && cost_to(&b.handle.snapshot(), 0, 2) == Some(Cost::Finite(20)),
            200
        )
        .await,
        "b lost a direct neighbor after a multi-arc regather: {:?}",
        b.handle.snapshot()
    );
}

/// A "partition" — two isolated nodes independently claiming to be the sole
/// occupant of the same outer g-node — produces `GnodeSplitted` only after
/// the configured debounce threshold, never immediately on first detection.
#[tokio::test(start_paused = true)]
async fn partition_signals_split_only_after_debounce() {
    let topo = topology();
    let threshold = Duration::from_millis(200);
    // A sits alone in level-1 g-node 0; b1/b2 both sit in level-1 g-node 1
    // but never talk to each other, so they each independently believe
    // they're g-node 1's only member — the split condition.
    let a = Node::spawn_with(naddr(&topo, [0, 0]), 1, 0, threshold, fast_config());
    let b1 = Node::spawn_with(naddr(&topo, [0, 1]), 2, 0, threshold, fast_config());
    let b2 = Node::spawn_with(naddr(&topo, [1, 1]), 3, 1, threshold, fast_config());

    link(&a, &b1, Cost::Finite(5)).await;
    settle().await;
    // Only one fingerprint known so far for g-node (level 1, pos 1): no split yet.
    let mut events = a.handle.subscribe_events();
    link(&a, &b2, Cost::Finite(5)).await;
    settle().await;

    // The split is detected immediately (b_set / first-detection reflood),
    // but GnodeSplitted must NOT fire before the debounce elapses.
    let mut saw_split = false;
    while let Ok(ev) = events.try_recv() {
        if matches!(ev, QspnEvent::GnodeSplitted { .. }) {
            saw_split = true;
        }
    }
    assert!(
        !saw_split,
        "GnodeSplitted fired before the debounce threshold elapsed"
    );

    tokio::time::advance(threshold - Duration::from_millis(1)).await;
    settle().await;
    let mut saw_split = false;
    while let Ok(ev) = events.try_recv() {
        if matches!(ev, QspnEvent::GnodeSplitted { .. }) {
            saw_split = true;
        }
    }
    assert!(
        !saw_split,
        "GnodeSplitted fired 1ms before the debounce threshold"
    );

    tokio::time::advance(Duration::from_millis(2)).await;
    settle().await;
    let mut saw_split = false;
    while let Ok(ev) = events.try_recv() {
        if matches!(ev, QspnEvent::GnodeSplitted { .. }) {
            saw_split = true;
        }
    }
    assert!(
        saw_split,
        "GnodeSplitted did not fire after the debounce threshold elapsed"
    );
}
