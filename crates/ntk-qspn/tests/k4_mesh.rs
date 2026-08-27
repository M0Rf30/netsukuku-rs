//! Full K4 mesh: two level-0 sibling pairs (`q0`,`q1` in level-1 slot 0;
//! `q2`,`q3` in level-1 slot 1), every pair directly adjacent — the in-memory
//! analogue of `ntkd/tests/mesh.rs`'s
//! `partition_clean_severance_drops_exactly_the_unreachable_destinations`
//! real-kernel scenario, where a bridged L2 uplink merges both segments into
//! one flat broadcast domain and every node ends up with a direct arc to
//! every other node.
//!
//! `q2` and `q3` are tied at the default eldership (0): each one's own
//! `Fingerprint::construct` names the *other* as champion for their shared
//! level-1 g-node (`Fingerprint::construct`'s docs), so every node that
//! reaches both of them independently — every other node here, since this
//! is a full K4 — legitimately admits two differently-identified but
//! `same_branch`-equal fingerprints for that one destination. Before the
//! fix, `QspnState::update_map_one_destination`'s fingerprint-split check
//! deduped by bare `Fingerprint::identity_eq` instead of `same_branch`, so
//! it saw those two admitted fingerprints as *two different g-nodes in
//! conflict* and fired a spurious `QspnEvent::GnodeSplitted` for an ordinary
//! tied g-node that never split at all — reproduced below as `q0`/`q1` (the
//! other slot's own tied members) each still seeing a false split for
//! q2/q3. That false signal is exactly the kind of thing a real daemon's
//! hooking layer treats as "this network needs renegotiation", which is the
//! most likely trigger for the level-0-sibling-goes-missing symptom the
//! real-kernel severance scenario reports (a hooking rehook tears down and
//! rebuilds the whole QSPN generation) — `ntk-qspn` itself never loses the
//! sibling route in this topology (see the second assertion below), so the
//! loss has to come from a consumer reacting to this crate's own false
//! alarm.
//!
//! After the fix, the same topology never emits `GnodeSplitted` for q2/q3
//! at all, while `partition_signals_split_only_after_debounce` (a genuine
//! fork) still does.

mod support;

use ntk_common::{Cost, HCoord, Topology};
use ntk_qspn::QspnEvent;
use support::{Node, fast_config, link, naddr, wait_for};

fn cost_to(snapshot: &ntk_qspn::RouteSnapshot, level: usize, pos: u32) -> Vec<Cost> {
    snapshot
        .levels
        .get(level)
        .into_iter()
        .flat_map(|entries| entries.iter())
        .find(|e| e.destination == HCoord::new(level, pos))
        .map(|e| e.paths.iter().map(|p| p.cost).collect())
        .unwrap_or_default()
}

fn saw_gnode_splitted(rx: &mut tokio::sync::broadcast::Receiver<QspnEvent>) -> bool {
    let mut saw = false;
    while let Ok(e) = rx.try_recv() {
        if matches!(e, QspnEvent::GnodeSplitted { .. }) {
            saw = true;
        }
    }
    saw
}

/// Each node must learn its own level-0 sibling (one direct path) and the
/// other slot's level-1 aggregate: the two disjoint direct paths, one per
/// far member, at cost 10 each, *plus* a third, more expensive (cost 20)
/// backup path routed through its own sibling — mandatory admission for any
/// path that "reaches a new sibling g-node" (`research/notes/
/// 01-vala-core-routing.md` §3, `qspn.vala:1487-1502`'s `z1d`), regardless
/// of whether its fingerprint is already otherwise covered. This is
/// intentional multipath redundancy through every known gateway, not a bug
/// — a real K4 mesh is exactly the topology that makes it visible.
#[tokio::test]
async fn k4_mesh_converges_to_sibling_plus_disjoint_far_slot_with_no_false_split() {
    let topo = Topology::new([2, 2]).expect("valid topology");
    let q0 = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());
    let q1 = Node::spawn(naddr(&topo, [1, 0]), 2, fast_config());
    let q2 = Node::spawn(naddr(&topo, [0, 1]), 3, fast_config());
    let q3 = Node::spawn(naddr(&topo, [1, 1]), 4, fast_config());

    let mut q0_events = q0.handle.subscribe_events();
    let mut q1_events = q1.handle.subscribe_events();

    // Full K4: every pair directly adjacent, as a bridged flat L2 domain
    // gives every node a direct arc to every other node. Established
    // concurrently, mirroring near-simultaneous broadcast discovery in the
    // real kernel scenario — the timing that actually puts both of q2/q3's
    // tied fingerprints in front of `update_map_one_destination` in one
    // call (a fully sequential, one-link-settles-before-the-next
    // formation order can dodge that co-occurrence by accident).
    tokio::join!(
        link(&q0, &q1, Cost::Finite(10)),
        link(&q0, &q2, Cost::Finite(10)),
        link(&q0, &q3, Cost::Finite(10)),
        link(&q1, &q2, Cost::Finite(10)),
        link(&q1, &q3, Cost::Finite(10)),
        link(&q2, &q3, Cost::Finite(10)),
    );

    let far_slot = vec![Cost::Finite(10), Cost::Finite(10), Cost::Finite(20)];
    let ok = wait_for(
        || {
            cost_to(&q0.handle.snapshot(), 0, 1) == vec![Cost::Finite(10)]
                && cost_to(&q1.handle.snapshot(), 0, 0) == vec![Cost::Finite(10)]
                && cost_to(&q2.handle.snapshot(), 0, 1) == vec![Cost::Finite(10)]
                && cost_to(&q3.handle.snapshot(), 0, 0) == vec![Cost::Finite(10)]
                && cost_to(&q0.handle.snapshot(), 1, 1) == far_slot
                && cost_to(&q1.handle.snapshot(), 1, 1) == far_slot
                && cost_to(&q2.handle.snapshot(), 1, 0) == far_slot
                && cost_to(&q3.handle.snapshot(), 1, 0) == far_slot
        },
        200,
    )
    .await;

    assert!(
        ok,
        "K4 mesh did not converge:\nq0={:#?}\nq1={:#?}\nq2={:#?}\nq3={:#?}",
        q0.handle.snapshot(),
        q1.handle.snapshot(),
        q2.handle.snapshot(),
        q3.handle.snapshot()
    );

    // `Node::spawn`'s split-signal debounce is a real 20ms timer (not
    // `start_paused`), so give a fired-but-pending `SplitTimerFire` real
    // wall-clock time to land before checking for it — `wait_for`'s own
    // polling loop returns as soon as the route set above is correct,
    // which is well under 20ms and would otherwise race a genuine (buggy)
    // split signal that just hasn't fired yet.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert!(
        !saw_gnode_splitted(&mut q0_events),
        "q0 saw a false GnodeSplitted for q2/q3's ordinary tied eldership"
    );
    assert!(
        !saw_gnode_splitted(&mut q1_events),
        "q1 saw a false GnodeSplitted for q2/q3's ordinary tied eldership"
    );
}
