//! Collective-merge decision tests.
//!
//! `CoordinatorClient::decide_merge` (`src/coordinator.rs`) routes the
//! `merge_direction`/`merge_tiebreak` "ask coordinator" tiebreak
//! (`arc_handler.vala:183-208`) through a single, per-target-network memoized answer instead of
//! each arc handler recomputing it locally. This is the fix for a real six-node two-group merge
//! that produced `a_rehooked=2 b_rehooked=3` — members of the *same* g-node disagreeing about
//! which side should migrate.
//!
//! Two levels of test:
//! - Unit-level: `decide_merge` called directly against a shared [`FakeCoordinatorClient`] —
//!   proves the memoization itself (a later ask with a *different* local reading for the same
//!   target still gets the first-computed answer, which is what makes every member "follow").
//! - Integration-level: real [`spawn`]ed actors, one per simulated group member, sharing one
//!   [`FakeCoordinatorClient`] per group (modeling "my own g-node's elected Coordinator") —
//!   proves the end-to-end property the real scenario violated: of two equal-sized groups,
//!   exactly one migrates, and every one of its members does.

use std::sync::Arc;
use std::time::Duration;

use ntk_common::Topology;
use ntk_hooking::FakeQspnView;
use ntk_hooking::{
    ArcId, ArcPhase, CoordinatorClient, EntryData, FakeCoordinatorClient, FakeHookingStubFactory,
    HookingConfig, HookingHandle, HookingOrigin, HookingStubFactory, MergeArbitrationRequest,
    NetworkData, QspnView, ScriptedHookingStub, spawn,
};
use tokio_util::sync::CancellationToken;

async fn settle() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

async fn wait_for(mut check: impl FnMut() -> bool, max_rounds: usize) -> bool {
    for _ in 0..max_rounds {
        if check() {
            return true;
        }
        settle().await;
    }
    check()
}

/// Every timer shortened to keep tests fast while still exercising real (paused, injected) time
/// — mirrors `tests/actor_lifecycle.rs`'s own `fast_config`.
fn fast_config() -> HookingConfig {
    HookingConfig {
        not_bootstrapped_retry: Duration::from_millis(10),
        merge_reject_wait: Duration::from_millis(10),
        global_timeout: Arc::new(|_| Duration::from_millis(10)),
        ask_again_divisor: 1,
        restart_multiplier: 1,
        routing_response_timeout: Duration::from_millis(200),
    }
}

fn topo() -> Topology {
    Topology::new([8]).expect("valid topology")
}

// ---------------------------------------------------------------------------
// Unit-level: decide_merge is decided once and shared.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn decide_merge_memoizes_despite_a_later_differing_local_reading() {
    let coord = FakeCoordinatorClient::new(3);
    let first = coord
        .decide_merge(MergeArbitrationRequest {
            my_network_id: 100,
            neighbor_network_id: 200,
            neighbor_n_nodes: 3,
        })
        .await;
    assert!(first, "the smaller network id proceeds into the larger one");

    // A later ask about the same target (a different member's arc, or the same arc retrying)
    // samples a wildly different neighbor size — on its own this would flip
    // `merge_tiebreak`'s answer (`merge_tiebreak(3, 1, 100, 200) == false`) — but the target
    // network id is unchanged, so the memoized verdict must still come back, not a fresh
    // recomputation.
    let skewed = coord
        .decide_merge(MergeArbitrationRequest {
            my_network_id: 100,
            neighbor_network_id: 200,
            neighbor_n_nodes: 1,
        })
        .await;
    assert_eq!(
        skewed, first,
        "every member asking about the same target network must get the same verdict"
    );
}

#[tokio::test]
async fn decide_merge_antisymmetric_verdict_is_also_memoized() {
    let coord = FakeCoordinatorClient::new(3);
    let first = coord
        .decide_merge(MergeArbitrationRequest {
            my_network_id: 200,
            neighbor_network_id: 100,
            neighbor_n_nodes: 3,
        })
        .await;
    assert!(!first, "the larger network id waits instead of proceeding");

    let skewed = coord
        .decide_merge(MergeArbitrationRequest {
            my_network_id: 200,
            neighbor_network_id: 100,
            neighbor_n_nodes: 5, // alone would flip merge_tiebreak(3,5,200,100) to true
        })
        .await;
    assert_eq!(
        skewed, first,
        "the mirror case is just as sticky once decided"
    );
}

// ---------------------------------------------------------------------------
// Integration-level: two equal-sized groups meet; exactly one migrates, in full.
// ---------------------------------------------------------------------------

struct GroupMember {
    handle: HookingHandle,
    arc: ArcId,
    cancel: CancellationToken,
}

/// Spawns one simulated group member at `member_pos` inside its own (size-3) group, with one
/// arc into a peer reporting `peer_network_id`/`peer_n_nodes`, sharing `coord` (this group's
/// elected Coordinator) with its groupmates.
fn spawn_member(
    my_network_id: i64,
    member_pos: u32,
    coord: Arc<FakeCoordinatorClient>,
    peer_network_id: i64,
    peer_n_nodes: u64,
) -> GroupMember {
    let mut view = FakeQspnView::new(topo(), vec![member_pos]);
    view.network_id = my_network_id;
    view.n_nodes = 3;
    let view: Arc<dyn QspnView> = Arc::new(view);
    let coord: Arc<dyn CoordinatorClient> = coord;

    let stubs = Arc::new(FakeHookingStubFactory::new());
    let arc = ArcId(u64::from(member_pos) + 1);
    let entered_pos = member_pos + 10; // distinct from the peer's own positions
    let stub = Arc::new(ScriptedHookingStub::new(
        move |_ask_coord| {
            Ok(NetworkData {
                network_id: peer_network_id,
                neighbor_n_nodes: peer_n_nodes,
                neighbor_min_level: 0,
                gsizes: vec![8],
                neighbor_pos: vec![0],
            })
        },
        move |_lvl| {
            Ok(EntryData {
                network_id: peer_network_id,
                pos: vec![entered_pos],
                elderships: vec![0],
            })
        },
    ));
    stubs.register_arc(arc, stub);
    let stubs: Arc<dyn HookingStubFactory> = stubs;
    let cancel = CancellationToken::new();

    let (handle, _actor) = spawn(
        HookingOrigin::Joining,
        view,
        coord,
        stubs,
        fast_config(),
        cancel.clone(),
    );
    GroupMember {
        handle,
        arc,
        cancel,
    }
}

/// Runs the "two equal (3-node) groups meet" scenario with group A at `a_network_id` and group
/// B at `b_network_id`, and asserts exactly the group with the smaller network id fully migrates
/// (`ArcPhase::Entered`, `hooked == true`) while every member of the other group waits — the
/// property the real scenario violated (`a_rehooked=2 b_rehooked=3`: both groups partially
/// migrating instead of exactly one, fully).
async fn assert_smaller_network_id_group_migrates(a_network_id: i64, b_network_id: i64) {
    let coord_a = Arc::new(FakeCoordinatorClient::new(3));
    let coord_b = Arc::new(FakeCoordinatorClient::new(3));

    let a_members: Vec<GroupMember> = (0..3)
        .map(|i| spawn_member(a_network_id, i, coord_a.clone(), b_network_id, 3))
        .collect();
    let b_members: Vec<GroupMember> = (0..3)
        .map(|i| spawn_member(b_network_id, i, coord_b.clone(), a_network_id, 3))
        .collect();

    for m in a_members.iter().chain(b_members.iter()) {
        m.handle.add_arc(m.arc).await.expect("add_arc succeeds");
    }

    let (migrating, waiting): (&[GroupMember], &[GroupMember]) = if a_network_id < b_network_id {
        (&a_members, &b_members)
    } else {
        (&b_members, &a_members)
    };

    for m in migrating {
        assert!(
            wait_for(|| m.handle.snapshot().hooked, 1000).await,
            "every member of the smaller-network-id group must migrate"
        );
        assert!(matches!(
            m.handle.snapshot().arcs.get(&m.arc),
            Some(ArcPhase::Entered { .. })
        ));
    }

    // Give the waiting side ample opportunity to (wrongly) proceed too before asserting it
    // never does — several `merge_reject_wait` redo-from-start cycles.
    settle().await;
    tokio::time::advance(Duration::from_millis(200)).await;
    settle().await;
    for m in waiting {
        assert!(
            !m.handle.snapshot().hooked,
            "the larger-network-id group must never also migrate"
        );
    }

    for m in migrating.iter().chain(waiting.iter()) {
        m.cancel.cancel();
    }
}

#[tokio::test(start_paused = true)]
async fn two_equal_sized_groups_meeting_produce_exactly_one_migrating_group() {
    assert_smaller_network_id_group_migrates(100, 200).await;
}

#[tokio::test(start_paused = true)]
async fn two_equal_sized_groups_meeting_mirror_case_with_roles_swapped() {
    // Antisymmetry under test: swapping which side has the smaller id swaps who migrates.
    assert_smaller_network_id_group_migrates(300, 150).await;
}

// ---------------------------------------------------------------------------
// A late-asking / previously-unreachable member still converges (pull, not push: there is
// nothing to have "missed" — every arc gets the same answer whenever it happens to ask).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_late_asker_gets_the_same_already_decided_verdict() {
    let coord = FakeCoordinatorClient::new(3);
    let earlier = coord
        .decide_merge(MergeArbitrationRequest {
            my_network_id: 100,
            neighbor_network_id: 200,
            neighbor_n_nodes: 3,
        })
        .await;

    // A member added to the g-node long after the first decision (e.g. a new arc coming up, or
    // one whose earlier attempt never reached the coordinator) asks for the first time here.
    let late = coord
        .decide_merge(MergeArbitrationRequest {
            my_network_id: 100,
            neighbor_network_id: 200,
            neighbor_n_nodes: 3,
        })
        .await;
    assert_eq!(
        late, earlier,
        "a late asker converges on the already-decided verdict instead of wedging or diverging"
    );
}
