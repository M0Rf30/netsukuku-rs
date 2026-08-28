//! Outbound `destroy` (`research/impl/vala/qspn/qspn.vala:2481-2505`): a retiring identity tells
//! its neighbours, and each one's implicit withdrawal retracts whatever was only reachable
//! through it.
//!
//! This is the half of the migration gap that *is* portable without the connectivity identity
//! (`qspn.vala:2226-2505`, absent here — see `QspnHandle::check_connectivity`'s docs). Upstream's
//! `destroy` is explicitly not connectivity-only: its own doc says "connectivity or not" and its
//! `connectivity_from_level` "could be also 0" (`qspn.vala:2479-2484`), which makes every arc an
//! outer arc for a main identity. The receiving side (`got_destroy` -> `arc_remove`) was already
//! ported and wired; only the announcement was missing, so a peer kept routing to a position its
//! owner had left until its own liveness probe happened to reap the arc.

mod support;

use ntk_common::{Cost, Topology};
use support::{Node, fast_config, link, naddr, wait_for};

/// Two nodes in different level-1 g-nodes: `b` learns `a`'s g-node through their single arc, so
/// that destination has no substitute. When `a` retires, a correct withdrawal drops it outright,
/// which makes the assertion unambiguous rather than a cost comparison.
///
/// Two is the right size here, not a simplification: the claim is that the announcement itself
/// triggers withdrawal. Multi-hop relaying is a different property, already covered by
/// `convergence.rs`/`triangle.rs`, and `gsizes = [2, 2]` has only two level-1 slots anyway — a
/// third distinct g-node to chain through does not exist.
#[tokio::test]
async fn a_retiring_identity_makes_its_neighbour_withdraw_it() {
    let topo = Topology::new([2, 2]).expect("valid topology");
    let a = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());
    let b = Node::spawn(naddr(&topo, [0, 1]), 2, fast_config());

    link(&a, &b, Cost::Finite(10)).await;

    let converged = wait_for(
        || cost_to(&b.handle.snapshot(), 1, 0) == vec![Cost::Finite(10)],
        200,
    )
    .await;
    assert!(
        converged,
        "the pair never converged, so the withdrawal below would prove nothing: b={:?}",
        b.handle.snapshot()
    );

    a.handle
        .announce_destroy()
        .await
        .expect("announcing retirement must reach a live actor");

    let withdrawn = wait_for(|| cost_to(&b.handle.snapshot(), 1, 0).is_empty(), 200).await;
    assert!(
        withdrawn,
        "a retirement must retract the departing node's destination, not leave it to age out on \
         the liveness probe: b={:?}",
        b.handle.snapshot()
    );
}

/// A node with no arcs has nobody to tell. It must succeed rather than error, because `migrate`
/// calls this unconditionally and a lone node re-addressing is an ordinary case, not a failure.
#[tokio::test]
async fn announcing_retirement_with_no_arcs_is_a_success_not_an_error() {
    let topo = Topology::new([2, 2]).expect("valid topology");
    let lone = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());

    lone.handle
        .announce_destroy()
        .await
        .expect("no arcs is not a failure");
}

fn cost_to(snapshot: &ntk_qspn::RouteSnapshot, level: usize, pos: u32) -> Vec<Cost> {
    let Some(entries) = snapshot.levels.get(level) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|e| e.destination == ntk_common::HCoord::new(level, pos))
        .flat_map(|e| e.paths.iter().map(|p| p.cost))
        .collect()
}
