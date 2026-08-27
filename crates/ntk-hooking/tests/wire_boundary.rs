//! Wire-boundary DoS regression: `HookingSearchMigrationPath`'s `lvl` is a
//! bare peer-supplied `i32` (`ntk.proto`'s `hooking_search_migration_path`
//! field, arg: `lvl`), dispatched straight to a remote peer's
//! [`HookingRpcHandler`] (`crates/ntkd/src/node/dispatch.rs`). Before the
//! fix, `lvl >= topology.levels()` reached
//! `HookingRpcHandler::search_migration_path` -> `find_shortest_mig` ->
//! `make_tuple_from_level`'s `Vec::with_capacity(levels - l)` and panicked
//! with "attempt to subtract with overflow" (captured verbatim while
//! reproducing this issue, debug profile) — a remotely triggerable crash
//! (or, in release, an underflowed huge capacity request) on every request
//! this crate's own tests were run under. After the fix, the same call
//! returns a clean [`ErrorDomain::Deserialize`] `Err`, no panic.

use std::sync::Arc;
use std::time::Duration;

use ntk_common::Topology;
use ntk_hooking::{
    CoordinatorClient, FakeCoordinatorClient, FakeHookingStubFactory, FakeQspnView,
    HookingRpcHandler, HookingStubFactory, MessageRouting, QspnView,
};
use ntk_proto::v1::{CallerContext, ErrorDomain, MethodCall, TypedValue, method_call};
use ntk_rpc::RpcHandler;

fn handler_with_levels(levels: u32) -> HookingRpcHandler {
    let gsizes: Vec<u32> = std::iter::repeat_n(4, levels as usize).collect();
    let view: Arc<dyn QspnView> = Arc::new(FakeQspnView::new(
        Topology::new(gsizes).unwrap(),
        vec![0; levels as usize],
    ));
    let coord: Arc<dyn CoordinatorClient> = Arc::new(FakeCoordinatorClient::new(1));
    let stubs: Arc<dyn HookingStubFactory> = Arc::new(FakeHookingStubFactory::new());
    let router = Arc::new(MessageRouting::new(
        view.clone(),
        coord.clone(),
        stubs,
        Duration::from_millis(200),
    ));
    HookingRpcHandler::new(view, coord, router)
}

async fn call_search_migration_path(
    handler: &HookingRpcHandler,
    lvl: i32,
) -> Result<ntk_proto::v1::ResponsePayload, ntk_proto::v1::RemoteError> {
    let call = MethodCall {
        call: Some(method_call::Call::HookingSearchMigrationPath(lvl)),
    };
    handler
        .handle(CallerContext::default(), TypedValue::default(), call, None)
        .await
}

/// A peer requesting a level at or beyond this identity's own topology depth
/// must be rejected cleanly, not crash the servant. This is the exact entry
/// point a remote peer uses (`Call::HookingSearchMigrationPath`, dispatched
/// at `crates/ntkd/src/node/dispatch.rs:133`).
#[tokio::test]
async fn hostile_level_at_topology_depth_is_a_clean_error() {
    let handler = handler_with_levels(1);

    // `levels == 1`, so `lvl == 1` is already one past the last real level.
    let err = call_search_migration_path(&handler, 1)
        .await
        .expect_err("lvl >= levels must be rejected, not panic or succeed");
    assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
}

/// The same rejection holds for a maximally hostile value, not just the
/// off-by-one boundary — this is the value that reproduced the underflow
/// panic pre-fix.
#[tokio::test]
async fn hostile_level_i32_max_is_a_clean_error() {
    let handler = handler_with_levels(4);

    let err = call_search_migration_path(&handler, i32::MAX)
        .await
        .expect_err("an absurdly large lvl must be rejected, not panic");
    assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
}

/// The pre-existing "negative level" rejection (`i32` -> `usize` conversion
/// failure) must still work unchanged.
#[tokio::test]
async fn negative_level_is_still_a_clean_error() {
    let handler = handler_with_levels(4);

    let err = call_search_migration_path(&handler, -1)
        .await
        .expect_err("a negative lvl must be rejected");
    assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
}

/// `lvl == levels - 1` (the deepest legal request, `first_host_lvl ==
/// levels`) must still be accepted by the validation itself — it must not
/// be rejected as out-of-range.
///
/// Deliberately does not assume whether this fake topology yields a
/// migration path: either outcome proves the point, and pinning one would
/// assert unrelated fake-harness behaviour rather than the boundary check
/// this test exists for. What must never happen is an
/// [`ErrorDomain::Deserialize`], which is what the out-of-range rejection
/// returns.
#[tokio::test]
async fn deepest_legal_level_is_not_rejected_as_out_of_range() {
    let handler = handler_with_levels(4);

    match call_search_migration_path(&handler, 3).await {
        Ok(_) => {}
        Err(err) => assert_ne!(
            err.domain,
            ErrorDomain::Deserialize as i32,
            "a legal boundary level must not be rejected as out-of-range: {err:?}"
        ),
    }
}
