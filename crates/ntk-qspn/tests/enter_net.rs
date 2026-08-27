//! `enter_net`-rooted identities driven through a real (in-memory) actor,
//! via [`ntk_qspn::spawn_entering`]: bootstrap-phase gating, driving
//! bootstrap to completion once a qualifying peer connects, and observing
//! the resulting hook via [`ntk_qspn::QspnHandle::is_bootstrap_complete`]/
//! [`ntk_qspn::QspnEvent::BootstrapComplete`].

mod support;

use ntk_common::Cost;
use ntk_qspn::QspnEvent;

use support::{Node, fast_config, link, naddr, settle, topology};

/// An entering identity starts bootstrapping, does not yet expose the
/// g-node it is hooking into, and exits bootstrap — firing
/// [`QspnEvent::BootstrapComplete`] and becoming
/// [`ntk_qspn::QspnHandle::is_bootstrap_complete`] — the moment a qualifying
/// peer's ETP arrives (`qspn.vala:522-573`).
#[tokio::test]
async fn entering_identity_exits_bootstrap_once_a_qualifying_peer_connects() {
    let topo = topology();
    let config = fast_config();
    // `a` hooks into level 1 (guest=1, host=2): its own g-node (level 0) is
    // already resolved, level 1 is what bootstrap must confirm.
    let a = Node::spawn_entering(naddr(&topo, [0, 0]), 1, 1, 2, config.clone());
    // `b` is an established peer one level-1 slot over — its ETP's
    // divergence from `a` is exactly level 1, the window `a` is waiting on.
    let b = Node::spawn(naddr(&topo, [0, 1]), 2, config);

    assert!(
        !a.handle.is_bootstrap_complete().await.unwrap(),
        "must start in bootstrap"
    );
    let mut events = a.handle.subscribe_events();

    link(&a, &b, Cost::Finite(10)).await;

    let mut hooked = false;
    for _ in 0..64 {
        if a.handle.is_bootstrap_complete().await.unwrap() {
            hooked = true;
            break;
        }
        settle().await;
    }
    assert!(
        hooked,
        "bootstrap must complete once the qualifying peer's ETP arrives"
    );

    let mut saw_complete = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, QspnEvent::BootstrapComplete) {
            saw_complete = true;
        }
    }
    assert!(
        saw_complete,
        "QspnEvent::BootstrapComplete must fire on the same transition"
    );

    // Now hooked: the peer's g-node at level 1 must be a visible route.
    let snapshot = a.handle.snapshot();
    assert!(
        snapshot.levels[1]
            .iter()
            .any(|entry| entry.destination.pos == 1),
        "the host g-node must be a published destination once hooked"
    );
}

/// A fallback timeout forces bootstrap to exit even with no qualifying
/// answer at all (`qspn.vala:556-565`), so an entering identity with no
/// peers is never stuck forever.
#[tokio::test(start_paused = true)]
async fn entering_identity_exits_bootstrap_on_fallback_timeout_with_no_peers() {
    let topo = topology();
    let mut config = fast_config();
    config.bootstrap_fallback_max_wait = std::time::Duration::from_millis(50);
    let a = Node::spawn_entering(naddr(&topo, [0, 0]), 1, 1, 2, config);

    assert!(!a.handle.is_bootstrap_complete().await.unwrap());
    tokio::time::advance(std::time::Duration::from_millis(60)).await;
    settle().await;
    assert!(
        a.handle.is_bootstrap_complete().await.unwrap(),
        "bootstrap must exit via the fallback timeout with no peers at all"
    );
}
