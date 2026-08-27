//! Actor-level tests driving a [`ntk_hooking::HookingHandle`] through the
//! full state machine over in-memory fakes: `create_net` (immediately
//! hooked), joining an existing (larger) network through one arc, a
//! `NotBootstrappedError` retry recovering once the peer becomes ready, and
//! a hard remote failure (timeout) settling the arc into a well-defined
//! terminal state instead of wedging.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ntk_common::{HCoord, Topology};
use ntk_hooking::{
    ArcId, ArcPhase, CoordinatorClient, EntryData, FakeCoordinatorClient, FakeHookingStubFactory,
    FakeQspnView, HookingConfig, HookingEvent, HookingOrigin, HookingRpcHandler,
    HookingStubFactory, MessageRouting, NetworkData, QspnView, ScriptedHookingStub, spawn,
};
use ntk_proto::v1::{ErrorDomain, RemoteError};
use ntk_rpc::RpcError;
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

/// Every timer shortened to keep tests fast while still exercising real
/// (paused, injected) time.
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
    Topology::new([4]).expect("valid topology")
}

#[tokio::test(start_paused = true)]
async fn create_net_is_immediately_hooked() {
    let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
    let coord: Arc<dyn CoordinatorClient> = Arc::new(FakeCoordinatorClient::new(1));
    let stubs: Arc<dyn HookingStubFactory> = Arc::new(FakeHookingStubFactory::new());
    let cancel = CancellationToken::new();

    let (handle, _actor) = spawn(
        HookingOrigin::CreateNet,
        view,
        coord,
        stubs,
        fast_config(),
        cancel.clone(),
    );

    let snap = handle.snapshot();
    assert!(
        snap.hooked,
        "create_net is hooked from the very first snapshot"
    );
    let chosen = snap.chosen.expect("create_net always resolves an address");
    assert_eq!(chosen.entry_data.pos, vec![0]);
    let naddr = chosen
        .naddr
        .expect("single-level create_net always resolves a full Naddr");
    assert_eq!(naddr.positions(), &[0]);

    cancel.cancel();
}

#[tokio::test(start_paused = true)]
async fn join_existing_network_drives_unhooked_node_to_hooked() {
    let peer_handler = spawn_peer_handler_for_join();

    let join_view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
    let join_coord: Arc<dyn CoordinatorClient> = Arc::new(FakeCoordinatorClient::new(1));
    let join_stubs = Arc::new(FakeHookingStubFactory::new());
    let arc = ArcId(1);
    join_stubs.register_peer(arc, HCoord::new(0, 0), peer_handler);
    let join_stubs: Arc<dyn HookingStubFactory> = join_stubs;
    let cancel = CancellationToken::new();

    let (handle, _actor) = spawn(
        HookingOrigin::Joining,
        join_view,
        join_coord,
        join_stubs,
        fast_config(),
        cancel.clone(),
    );

    assert!(!handle.snapshot().hooked, "Joining starts unhooked");

    handle.add_arc(arc).await.expect("add_arc succeeds");
    assert!(
        wait_for(|| handle.snapshot().hooked, 500).await,
        "the arc handler must eventually drive this identity to hooked"
    );

    let snap = handle.snapshot();
    let chosen = snap
        .chosen
        .expect("hooked implies a resolved chosen address");
    assert_eq!(
        chosen.entry_data.pos,
        vec![1],
        "merged into the peer's network at position 1"
    );
    let naddr = chosen
        .naddr
        .expect("single-level EntryData always resolves a full Naddr");
    assert_eq!(naddr.positions(), &[1]);
    assert_eq!(snap.arcs.get(&arc), Some(&ArcPhase::Entered { ask_lvl: 0 }));

    cancel.cancel();
}

/// `HookingHandle::try_begin_commit`'s `migrated` flag exists to keep this identity's own
/// concurrently-negotiating arcs from racing `finish_enter` (see that method's own doc comment)
/// — it must NOT also permanently refuse every migration after the first. A member that has
/// already migrated once must still be able to follow its g-node into a later, separate merge
/// (the daemon's own `SteadyStateCtx` dropped its equivalent one-shot `rehooked` latch for
/// exactly this reason, replacing it with `migration_in_progress` + a `migrations` counter).
#[tokio::test(start_paused = true)]
async fn a_second_migration_is_not_permanently_blocked_by_an_earlier_one() {
    let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
    let coord: Arc<dyn CoordinatorClient> = Arc::new(FakeCoordinatorClient::new(1));
    let stubs = Arc::new(FakeHookingStubFactory::new());

    // Two distinct foreign networks, each self-reportedly >10x my own size — `merge_direction`
    // proceeds unconditionally for both, with no Coordinator arbitration to complicate things.
    let arc1 = ArcId(1);
    stubs.register_arc(
        arc1,
        Arc::new(ScriptedHookingStub::new(
            |_ask_coord| {
                Ok(NetworkData {
                    network_id: 2,
                    neighbor_n_nodes: 100,
                    neighbor_min_level: 0,
                    gsizes: vec![4],
                    neighbor_pos: vec![0],
                })
            },
            |_lvl| {
                Ok(EntryData {
                    network_id: 2,
                    pos: vec![1],
                    elderships: vec![0],
                })
            },
        )),
    );
    let arc2 = ArcId(2);
    stubs.register_arc(
        arc2,
        Arc::new(ScriptedHookingStub::new(
            |_ask_coord| {
                Ok(NetworkData {
                    network_id: 3,
                    neighbor_n_nodes: 100,
                    neighbor_min_level: 0,
                    gsizes: vec![4],
                    neighbor_pos: vec![0],
                })
            },
            |_lvl| {
                Ok(EntryData {
                    network_id: 3,
                    pos: vec![1],
                    elderships: vec![0],
                })
            },
        )),
    );
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

    handle.add_arc(arc1).await.expect("add_arc succeeds");
    assert!(
        wait_for(
            || handle.snapshot().arcs.get(&arc1) == Some(&ArcPhase::Entered { ask_lvl: 0 }),
            500
        )
        .await,
        "the first arc must complete its own migration"
    );

    // A later arc discovering a THIRD, different network must still be able to migrate — this
    // is the exact scenario `HookingHandle::try_begin_commit`'s `migrated` flag once refused
    // forever, since `Command::MarkEntered` set it once and nothing ever cleared it.
    handle.add_arc(arc2).await.expect("add_arc succeeds");
    assert!(
        wait_for(
            || handle.snapshot().arcs.get(&arc2) == Some(&ArcPhase::Entered { ask_lvl: 0 }),
            500
        )
        .await,
        "a later migration must not be permanently refused by an earlier, already-completed one"
    );

    cancel.cancel();
}

/// Builds a peer node representing an already-larger, already-hooked
/// network (`network_id = 2`, `n_nodes = 100`, occupying position 0), whose
/// wire surface is a real [`HookingRpcHandler`] — the migration-path
/// search (`find_shortest_mig`/`execute_search`) genuinely runs, resolving
/// the joining node into position 1.
fn spawn_peer_handler_for_join() -> Arc<HookingRpcHandler> {
    let mut view = FakeQspnView::new(topo(), vec![0]);
    view.network_id = 2;
    view.n_nodes = 100;
    let view: Arc<dyn QspnView> = Arc::new(view);
    let coord = Arc::new(FakeCoordinatorClient::new(100));
    // Position 0 is already taken by the peer itself; the next free slot
    // for a joining node is 1.
    coord.set_next_pos(1, 1);
    let coord: Arc<dyn CoordinatorClient> = coord;
    let stubs: Arc<dyn HookingStubFactory> = Arc::new(FakeHookingStubFactory::new());
    let router = Arc::new(MessageRouting::new(
        view.clone(),
        coord.clone(),
        stubs,
        Duration::from_millis(200),
    ));
    Arc::new(HookingRpcHandler::new(view, coord, router))
}

#[tokio::test(start_paused = true)]
async fn not_bootstrapped_retry_recovers_once_the_peer_is_ready() {
    let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
    let coord: Arc<dyn CoordinatorClient> = Arc::new(FakeCoordinatorClient::new(1));
    let stubs = Arc::new(FakeHookingStubFactory::new());
    let arc = ArcId(7);

    let calls = Arc::new(AtomicU32::new(0));
    let calls_for_closure = calls.clone();
    let stub = Arc::new(ScriptedHookingStub::new(
        move |_ask_coord| {
            if calls_for_closure.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(RpcError::Remote(RemoteError {
                    domain: ErrorDomain::NotBootstrapped as i32,
                    message: "still hooking myself".into(),
                }))
            } else {
                Ok(ntk_hooking::NetworkData {
                    network_id: 1, // same as mine -> arc settles as SameNetwork
                    neighbor_n_nodes: 1,
                    neighbor_min_level: 0,
                    gsizes: vec![4],
                    neighbor_pos: vec![0],
                })
            }
        },
        |_lvl| unreachable!("same_network never reaches search_migration_path"),
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
    let mut events = handle.subscribe_events();

    handle.add_arc(arc).await.expect("add_arc succeeds");
    // The first attempt fails with NotBootstrapped and the handler sleeps
    // before retrying — advance paused time past that wait instead of
    // sleeping the test for real.
    settle().await;
    tokio::time::advance(Duration::from_millis(50)).await;

    assert!(
        wait_for(
            || matches!(
                handle.snapshot().arcs.get(&arc),
                Some(ArcPhase::SameNetwork)
            ),
            500
        )
        .await,
        "the retry must eventually observe the peer's now-ready NetworkData"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "exactly one retry after the first NotBootstrapped"
    );
    assert!(matches!(events.try_recv(), Ok(HookingEvent::SameNetwork(a)) if a == arc));

    cancel.cancel();
}

#[tokio::test(start_paused = true)]
async fn far_side_timeout_settles_into_a_well_defined_failed_state() {
    let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(topo(), vec![0]));
    let coord: Arc<dyn CoordinatorClient> = Arc::new(FakeCoordinatorClient::new(1));
    let stubs = Arc::new(FakeHookingStubFactory::new());
    let arc = ArcId(9);
    stubs.register_arc(arc, Arc::new(ScriptedHookingStub::always_times_out()));
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
    let mut events = handle.subscribe_events();

    handle.add_arc(arc).await.expect("add_arc succeeds");
    assert!(
        wait_for(
            || matches!(handle.snapshot().arcs.get(&arc), Some(ArcPhase::Failed)),
            500
        )
        .await,
        "a hard remote failure must settle into Failed, not wedge in Discovering forever"
    );
    assert!(matches!(events.try_recv(), Ok(HookingEvent::FailingArc(a)) if a == arc));
    assert!(
        !handle.snapshot().hooked,
        "a single failed arc must not falsely report hooked"
    );

    // The actor keeps serving other commands after an arc fails — not
    // wedged: the naturally-completed arc handler already cleaned up its
    // own tracking, so a subsequent remove_arc is a prompt, well-defined
    // UnknownArc rather than a hang or a stale success.
    assert_eq!(
        handle.remove_arc(arc).await,
        Err(ntk_hooking::HookingError::UnknownArc)
    );

    cancel.cancel();
}
