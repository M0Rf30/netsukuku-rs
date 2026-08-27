//! Integration coverage for the duplication/migration handshake, driven
//! through `ntk_rpc::FakeRpcClient` with injectable time
//! (`tokio::time::pause`/`advance`) instead of real sleeps.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ntk_common::{Naddr, Topology};
use ntk_identities::wire::{duplication_data_from_typed_value, identity_id_to_typed_value};
use ntk_identities::{
    ArcId, ArcInfo, Error, Handle, IdentityArcChange, IdentityEvent, IdentityId,
    IdentityRpcHandler, IdentityStatus, IdentityStubFactory, MigrationDeviceInfo, MigrationId,
};
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value;
use ntk_proto::v1::{
    Auth, CallerContext, Empty, IdentityMatchDuplicationArgs, MethodCall, ResponsePayload,
    TypedValue,
};
use ntk_rpc::{FakeRpcClient, FnHandler, RpcClient, RpcHandler};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn caller_context() -> CallerContext {
    CallerContext {
        source_id: None,
        src_nic: None,
    }
}

fn arc_info(peer_mac: &str, peer_linklocal: &str) -> ArcInfo {
    ArcInfo {
        dev: "eth0".to_owned(),
        peer_mac: peer_mac.to_owned(),
        peer_linklocal: peer_linklocal.to_owned(),
    }
}

fn devices(mac: &str, linklocal: &str) -> HashMap<String, MigrationDeviceInfo> {
    let mut map = HashMap::new();
    map.insert(
        "eth0".to_owned(),
        MigrationDeviceInfo {
            old_id_new_mac: mac.to_owned(),
            old_id_new_linklocal: linklocal.to_owned(),
        },
    );
    map
}

/// Routes every outbound call for `arc` to a lazily-bound target handler.
/// Two in-process peers each need the other's [`IdentityRpcHandler`] to
/// build their own stub factory, but that handler in turn needs the peer's
/// already-spawned [`Handle`] — this indirection breaks the construction
/// cycle.
struct PeerStubFactory {
    arc: ArcId,
    target: OnceLock<Arc<dyn RpcHandler>>,
}

impl PeerStubFactory {
    fn new(arc: ArcId) -> Arc<Self> {
        Arc::new(Self {
            arc,
            target: OnceLock::new(),
        })
    }

    fn bind(&self, handler: Arc<dyn RpcHandler>) {
        assert!(self.target.set(handler).is_ok(), "bind called once");
    }
}

impl IdentityStubFactory for PeerStubFactory {
    fn stub(&self, _arc: ArcId) -> Arc<dyn RpcClient> {
        let handler = self
            .target
            .get()
            .expect("peer bound before first call")
            .clone();
        Arc::new(FakeRpcClient::new(handler))
    }

    fn arc_for_caller(&self, _caller: &CallerContext) -> Option<ArcId> {
        Some(self.arc)
    }
}

/// Two peers, A and B, wired to a single shared arc, each having already
/// learned the other's main identity via `get_peer_main_id`.
async fn two_peers() -> (Handle, Handle, ArcId) {
    let arc = ArcId(1);
    let a_stub = PeerStubFactory::new(arc);
    let b_stub = PeerStubFactory::new(arc);
    let cancel = CancellationToken::new();

    let (a_handle, _a_join) = Handle::spawn(None, a_stub.clone(), cancel.clone());
    let (b_handle, _b_join) = Handle::spawn(None, b_stub.clone(), cancel.clone());

    let a_rpc: Arc<dyn RpcHandler> =
        Arc::new(IdentityRpcHandler::new(a_handle.clone(), a_stub.clone()));
    let b_rpc: Arc<dyn RpcHandler> =
        Arc::new(IdentityRpcHandler::new(b_handle.clone(), b_stub.clone()));
    a_stub.bind(b_rpc);
    b_stub.bind(a_rpc);

    a_handle
        .add_arc(arc, arc_info("bb:bb:bb:bb:bb:bb", "fe80::b"))
        .await
        .expect("a learns b's main id");
    b_handle
        .add_arc(arc, arc_info("aa:aa:aa:aa:aa:aa", "fe80::a"))
        .await
        .expect("b learns a's main id");

    (a_handle, b_handle, arc)
}

async fn collect_events(
    rx: &mut broadcast::Receiver<IdentityEvent>,
    count: usize,
) -> Vec<IdentityEvent> {
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("event stream stayed open");
        events.push(event);
    }
    events
}

fn find_event<'a>(
    events: &'a [IdentityEvent],
    what: &str,
    pred: impl Fn(&IdentityEvent) -> bool,
) -> &'a IdentityEvent {
    events
        .iter()
        .find(|e| pred(e))
        .unwrap_or_else(|| panic!("did not observe {what} among {events:?}"))
}

/// The full handshake between two independent actors, driven end to end
/// through `FakeRpcClient`: A migrates while B is not, so B's peer-side
/// `match_duplication` answers `null` and reactively runs `neighbour_migrated`
/// (`identities.vala:862-907`).
#[tokio::test]
async fn asymmetric_migration_duplicates_and_notifies_the_peer() {
    let (a, b, arc) = two_peers().await;
    let a_main = a.main_id();
    let b_main = b.main_id();
    let mut b_events = b.subscribe();

    let migration = MigrationId(1);
    a.prepare_migration(migration, a_main)
        .await
        .expect("prepare");
    let a_new = a
        .migrate(migration, a_main, devices("aa:aa:aa:aa:aa:a1", "fe80::a1"))
        .await
        .expect("migrate");

    let snap = a.snapshot();
    assert_eq!(snap.main_id, a_new);
    assert_eq!(
        snap.identities[&a_main].status,
        IdentityStatus::Connectivity
    );
    assert_eq!(snap.identities[&a_new].status, IdentityStatus::Main);

    // B never registered a pending migration for A's identity, so it takes
    // the "unmatched" path and reactively patches its own bookkeeping:
    // exactly the `Changed` (old identity-arc) and `Added` (new
    // identity-arc) pair from `on_neighbour_migrated`.
    let events = collect_events(&mut b_events, 2).await;

    // `identities.vala:889-893`: the *new* identity-arc gets the old
    // (original, real) mac/linklocal, and the *old* identity-arc gets
    // patched to the new pseudo mac/linklocal — this is the exact ordering
    // this port must preserve.
    let changed = find_event(
        &events,
        "old identity-arc patched to the new pseudo address",
        |e| {
            matches!(
                e,
                IdentityEvent::IdentityArc {
                    arc: got_arc,
                    identity,
                    change: IdentityArcChange::Changed { peer_id, only_neighbour_migrated: true, .. },
                } if *got_arc == arc && *identity == b_main && *peer_id == a_main
            )
        },
    );
    let IdentityEvent::IdentityArc {
        change:
            IdentityArcChange::Changed {
                peer_mac,
                peer_linklocal,
                ..
            },
        ..
    } = changed
    else {
        unreachable!()
    };
    assert_eq!(peer_mac, "aa:aa:aa:aa:aa:a1");
    assert_eq!(peer_linklocal, "fe80::a1");

    let added = find_event(&events, "new identity-arc for A's migrated id", |e| {
        matches!(
            e,
            IdentityEvent::IdentityArc {
                arc: got_arc,
                identity,
                change: IdentityArcChange::Added { peer_id, .. },
            } if *got_arc == arc && *identity == b_main && *peer_id == a_new
        )
    });
    let IdentityEvent::IdentityArc {
        change:
            IdentityArcChange::Added {
                peer_mac,
                peer_linklocal,
                ..
            },
        ..
    } = added
    else {
        unreachable!()
    };
    assert_eq!(peer_mac, "aa:aa:aa:aa:aa:aa");
    assert_eq!(peer_linklocal, "fe80::a");
}

/// Both peers migrate under the same `migration_id` at once — the symmetric
/// case upstream's peer-side `match_duplication` busy-wait exists for
/// (`identities.vala:852-861`, notes/01 §5). Each side's outbound call
/// should find the other's pending migration and receive real duplication
/// data back, not `null`.
#[tokio::test]
async fn symmetric_migration_matches_both_sides() {
    let (a, b, arc) = two_peers().await;
    let a_main = a.main_id();
    let b_main = b.main_id();
    let mut a_events = a.subscribe();
    let mut b_events = b.subscribe();

    let migration = MigrationId(5);
    a.prepare_migration(migration, a_main)
        .await
        .expect("a prepares");
    b.prepare_migration(migration, b_main)
        .await
        .expect("b prepares");

    let (a_result, b_result) = tokio::join!(
        a.migrate(migration, a_main, devices("aa:aa:aa:aa:aa:a2", "fe80::a2")),
        b.migrate(migration, b_main, devices("bb:bb:bb:bb:bb:b2", "fe80::b2")),
    );
    let a_new = a_result.expect("a migrates");
    let b_new = b_result.expect("b migrates");

    // Each side publishes: `IdentityAdded`, then (matched) `Added` +
    // `Changed` for the one identity-arc, then `IdentityDuplicated`.
    let a_events = collect_events(&mut a_events, 4).await;
    let b_events = collect_events(&mut b_events, 4).await;

    // A matched: it learned B's *new* mac/linklocal for the old identity-arc.
    let a_changed = find_event(
        &a_events,
        "a's old identity-arc matched b's new address",
        |e| {
            matches!(
                e,
                IdentityEvent::IdentityArc { arc: got_arc, identity, change: IdentityArcChange::Changed { peer_id, .. } }
                    if *got_arc == arc && *identity == a_main && *peer_id == b_main
            )
        },
    );
    let IdentityEvent::IdentityArc {
        change:
            IdentityArcChange::Changed {
                peer_mac,
                peer_linklocal,
                ..
            },
        ..
    } = a_changed
    else {
        unreachable!()
    };
    assert_eq!(peer_mac, "bb:bb:bb:bb:bb:b2");
    assert_eq!(peer_linklocal, "fe80::b2");

    find_event(
        &a_events,
        "a's new identity-arc points at b's new id",
        |e| {
            matches!(
                e,
                IdentityEvent::IdentityArc { arc: got_arc, identity, change: IdentityArcChange::Added { peer_id, .. } }
                    if *got_arc == arc && *identity == a_new && *peer_id == b_new
            )
        },
    );

    // Symmetric on B's side.
    let b_changed = find_event(
        &b_events,
        "b's old identity-arc matched a's new address",
        |e| {
            matches!(
                e,
                IdentityEvent::IdentityArc { arc: got_arc, identity, change: IdentityArcChange::Changed { peer_id, .. } }
                    if *got_arc == arc && *identity == b_main && *peer_id == a_main
            )
        },
    );
    let IdentityEvent::IdentityArc {
        change:
            IdentityArcChange::Changed {
                peer_mac,
                peer_linklocal,
                ..
            },
        ..
    } = b_changed
    else {
        unreachable!()
    };
    assert_eq!(peer_mac, "aa:aa:aa:aa:aa:a2");
    assert_eq!(peer_linklocal, "fe80::a2");

    find_event(
        &b_events,
        "b's new identity-arc points at a's new id",
        |e| {
            matches!(
                e,
                IdentityEvent::IdentityArc { arc: got_arc, identity, change: IdentityArcChange::Added { peer_id, .. } }
                    if *got_arc == arc && *identity == b_new && *peer_id == a_new
            )
        },
    );
}

/// A stub factory that answers `get_peer_main_id` with a fixed id and
/// nothing else — enough to bootstrap one arc via `add_arc` without a
/// second real peer actor.
struct FixedStubFactory {
    arc: ArcId,
    peer_main_id: IdentityId,
}

impl IdentityStubFactory for FixedStubFactory {
    fn stub(&self, _arc: ArcId) -> Arc<dyn RpcClient> {
        let peer_main_id = self.peer_main_id;
        let handler: Arc<dyn RpcHandler> = Arc::new(FnHandler(
            move |_caller: CallerContext,
                  _unicast: TypedValue,
                  call: MethodCall,
                  _auth: Option<Auth>| {
                let response = match call.call {
                    Some(Call::IdentityGetPeerMainId(_)) => ResponsePayload {
                        value: Some(Value::Typed(identity_id_to_typed_value(peer_main_id))),
                    },
                    _ => ResponsePayload {
                        value: Some(Value::Empty(Empty::VALUE)),
                    },
                };
                async move { Ok(response) }
            },
        ));
        Arc::new(FakeRpcClient::new(handler))
    }

    fn arc_for_caller(&self, _caller: &CallerContext) -> Option<ArcId> {
        Some(self.arc)
    }
}

fn build_match_duplication_call(migration: MigrationId, target_old_id: IdentityId) -> MethodCall {
    MethodCall {
        call: Some(Call::IdentityMatchDuplication(
            IdentityMatchDuplicationArgs {
                migration_id: migration.0,
                peer_id: Some(identity_id_to_typed_value(target_old_id)),
                old_id: Some(identity_id_to_typed_value(IdentityId::from_raw(1001))),
                new_id: Some(identity_id_to_typed_value(IdentityId::from_raw(1002))),
                old_id_new_mac: "cc:cc:cc:cc:cc:cc".to_owned(),
                old_id_new_linklocal: "fe80::c".to_owned(),
            },
        )),
    }
}

/// The busy-wait's replacement is bounded exactly by the pending migration's
/// 600s cleanup deadline: if the cleanup wins the race,
/// `match_duplication` must resolve — not hang — and answer as unmatched
/// (`identities.vala:854`'s never-returning `while` loop is what this test
/// proves this port does *not* reproduce).
#[tokio::test(start_paused = true)]
async fn match_duplication_is_rejected_once_the_cleanup_timer_wins() {
    let arc = ArcId(9);
    let stub: Arc<dyn IdentityStubFactory> = Arc::new(FixedStubFactory {
        arc,
        peer_main_id: IdentityId::from_raw(1),
    });
    let cancel = CancellationToken::new();
    let (b, _join) = Handle::spawn(None, stub.clone(), cancel.clone());
    let b_main = b.main_id();
    let rpc = Arc::new(IdentityRpcHandler::new(b.clone(), stub.clone()));

    let migration = MigrationId(42);
    b.prepare_migration(migration, b_main)
        .await
        .expect("prepare");

    let call = build_match_duplication_call(migration, b_main);
    let call_task = tokio::spawn(async move {
        rpc.handle(caller_context(), TypedValue::default(), call, None)
            .await
    });

    // Give the spawned call every chance to reach its bounded wait before
    // the clock jumps past the cleanup deadline.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(601)).await;

    let response = tokio::time::timeout(Duration::from_secs(5), call_task)
        .await
        .expect("call task did not hang")
        .expect("task joins")
        .expect("handler returns Ok, not a RemoteError");
    assert_eq!(
        response,
        ResponsePayload {
            value: Some(Value::Empty(Empty::VALUE))
        }
    );
}

/// The mirror case: the peer's `migrate` completes — flipping `ready` —
/// before the 600s deadline, so the bounded wait resolves to a match with
/// real duplication data instead of timing out.
#[tokio::test(start_paused = true)]
async fn match_duplication_succeeds_when_migrate_completes_just_in_time() {
    let arc = ArcId(9);
    let stub: Arc<dyn IdentityStubFactory> = Arc::new(FixedStubFactory {
        arc,
        peer_main_id: IdentityId::from_raw(1),
    });
    let cancel = CancellationToken::new();
    let (b, _join) = Handle::spawn(None, stub.clone(), cancel.clone());
    let b_main = b.main_id();
    let rpc = Arc::new(IdentityRpcHandler::new(b.clone(), stub.clone()));
    b.add_arc(arc, arc_info("cc:cc:cc:cc:cc:cc", "fe80::c"))
        .await
        .expect("add_arc");

    let migration = MigrationId(43);
    b.prepare_migration(migration, b_main)
        .await
        .expect("prepare");

    let call = build_match_duplication_call(migration, b_main);
    let call_task = tokio::spawn(async move {
        rpc.handle(caller_context(), TypedValue::default(), call, None)
            .await
    });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // Still well inside the 600s window when `migrate` finally runs.
    tokio::time::advance(Duration::from_secs(599)).await;
    let b_new = b
        .migrate(migration, b_main, devices("dd:dd:dd:dd:dd:dd", "fe80::d"))
        .await
        .expect("migrate completes just in time");

    let response = tokio::time::timeout(Duration::from_secs(5), call_task)
        .await
        .expect("call task did not hang")
        .expect("task joins")
        .expect("handler returns Ok");
    match response.value {
        Some(Value::Typed(tv)) => {
            let data = duplication_data_from_typed_value(&tv).expect("valid DuplicationData");
            assert_eq!(data.peer_new_id, b_new);
            assert_eq!(data.peer_old_id_new_mac, "dd:dd:dd:dd:dd:dd");
            assert_eq!(data.peer_old_id_new_linklocal, "fe80::d");
        }
        other => panic!("expected a present DuplicationData, got {other:?}"),
    }
}

/// The full composition-root-facing cycle: fork, negotiate a virtual
/// position for the successor, resolve it to a real one once hooking
/// finishes, then retire the connectivity fork — and confirm the arc it
/// held is never left orphaned once it is gone.
#[tokio::test]
async fn migration_hooks_then_retires_the_connectivity_fork() {
    let (a, b, arc) = two_peers().await;
    let _ = b;
    let a_main = a.main_id();

    let migration = MigrationId(7);
    a.prepare_migration(migration, a_main)
        .await
        .expect("prepare");
    let a_new = a
        .migrate(migration, a_main, devices("aa:aa:aa:aa:aa:a3", "fe80::a3"))
        .await
        .expect("migrate");

    // Immediately after the fork, both the connectivity fork and the
    // successor still reach the arc — that overlap is the entire point of
    // keeping a connectivity identity alive during migration.
    let ownership = a.arc_ownership().await.expect("arc ownership");
    assert_eq!(ownership.get(&a_main), Some(&vec![arc]));
    assert_eq!(ownership.get(&a_new), Some(&vec![arc]));

    let topology = Topology::new([4, 4]).expect("topology");
    let virtual_naddr =
        Naddr::new_allowing_virtual(topology.clone(), [10, 10]).expect("virtual naddr");
    assert!(virtual_naddr.is_virtual());
    a.set_naddr(a_new, Some(virtual_naddr))
        .await
        .expect("set virtual naddr");
    assert!(!a.snapshot().identities[&a_new].is_hooked());

    let real_naddr = Naddr::new(topology, [1, 2]).expect("real naddr");
    a.set_naddr(a_new, Some(real_naddr))
        .await
        .expect("set real naddr");
    assert!(a.snapshot().identities[&a_new].is_hooked());

    // The successor is fully hooked: retire the connectivity fork.
    a.remove_identity(a_main).await.expect("retire old_id");

    let snap = a.snapshot();
    assert_eq!(snap.main_id, a_new);
    assert!(
        !snap.identities.contains_key(&a_main),
        "dismissed identity must be unreachable"
    );
    assert_eq!(
        snap.identities.len(),
        1,
        "no duplicate or leftover identities"
    );

    let ownership = a.arc_ownership().await.expect("arc ownership after retire");
    assert_eq!(ownership.get(&a_new), Some(&vec![arc]));
    assert!(
        !ownership.contains_key(&a_main),
        "arc must not be left orphaned under the retired identity"
    );
}

/// The cleanup deadline can race the *initiating* side too, not just an
/// inbound `match_duplication` (see
/// `match_duplication_is_rejected_once_the_cleanup_timer_wins` above): if
/// `Handle::migrate` is never called before `prepare_migration`'s own
/// cleanup fires, the pending migration is gone and `migrate` must fail
/// cleanly instead of silently reusing — or wedging on — a stale
/// registration (`prepare_add_identity`'s tasklet,
/// `identities.vala:304,415-417`).
#[tokio::test(start_paused = true)]
async fn migrate_after_the_local_cleanup_deadline_is_rejected_cleanly() {
    let arc = ArcId(3);
    let stub: Arc<dyn IdentityStubFactory> = Arc::new(FixedStubFactory {
        arc,
        peer_main_id: IdentityId::from_raw(1),
    });
    let cancel = CancellationToken::new();
    let (b, _join) = Handle::spawn(None, stub, cancel);
    let b_main = b.main_id();

    let migration = MigrationId(77);
    b.prepare_migration(migration, b_main)
        .await
        .expect("prepare");

    tokio::time::advance(Duration::from_secs(601)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    let err = b
        .migrate(migration, b_main, devices("ee:ee:ee:ee:ee:ee", "fe80::e"))
        .await
        .expect_err("migrate must reject a migration whose cleanup already fired");
    assert!(matches!(
        err,
        Error::UnknownMigration { migration_id, old_id }
            if migration_id == migration && old_id == b_main
    ));

    // No wedge, no partial fork: the identity is exactly as it was.
    let snap = b.snapshot();
    assert_eq!(snap.main_id, b_main);
    assert_eq!(snap.identities.len(), 1);
    assert_eq!(snap.identities[&b_main].status, IdentityStatus::Main);
}

/// The third undefined failure path: the successor never finishes hooking.
/// `abort_migration` must dismiss it and hand the main-identity role back
/// to `old_id`, leaving no duplicate ids, no reachable trace of the failed
/// successor, and no arcs orphaned under it.
#[tokio::test]
async fn abort_migration_reverts_a_successor_that_never_hooks() {
    let (a, b, arc) = two_peers().await;
    let _ = b;
    let a_main = a.main_id();

    let migration = MigrationId(99);
    a.prepare_migration(migration, a_main)
        .await
        .expect("prepare");
    let a_new = a
        .migrate(migration, a_main, devices("aa:aa:aa:aa:aa:a4", "fe80::a4"))
        .await
        .expect("migrate");
    assert_eq!(a.main_id(), a_new);

    // The successor never gets a real position — hooking gives up.
    let mut events = a.subscribe();
    a.abort_migration(a_main, a_new)
        .await
        .expect("abort a stuck migration");

    let snap = a.snapshot();
    assert_eq!(snap.main_id, a_main, "main-identity role reverts to old_id");
    assert_eq!(
        snap.identities[&a_main].status,
        IdentityStatus::Main,
        "old_id regains its pre-migration status"
    );
    assert!(
        !snap.identities.contains_key(&a_new),
        "the failed successor must be unreachable"
    );
    assert_eq!(snap.identities.len(), 1, "no duplicate identities remain");

    let ownership = a.arc_ownership().await.expect("arc ownership after abort");
    assert_eq!(ownership.get(&a_main), Some(&vec![arc]));
    assert!(
        !ownership.contains_key(&a_new),
        "the dismissed successor must not retain the arc"
    );

    let published = collect_events(&mut events, 2).await;
    find_event(
        &published,
        "MigrationAborted for the stuck successor",
        |e| {
            matches!(
                e,
                IdentityEvent::MigrationAborted { old_id, new_id }
                    if *old_id == a_main && *new_id == a_new
            )
        },
    );
}
