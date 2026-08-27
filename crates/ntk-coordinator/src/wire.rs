//! Wire encoding: conversions between this crate's domain types and its own generated
//! `ntk.coordinator.v1` protobuf messages (`crate::v1`), plus [`RpcCoordinatorStub`] — the
//! [`CoordinatorStub`] implementation that adapts a real `ntk_rpc::RpcClient` (or
//! `ntk_rpc::FakeRpcClient`, for tests) to this crate's typed outbound-call surface.

use std::fmt;
use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{CallerContext, CoordinatorExecuteArgs, MethodCall, TypedValue};
use ntk_rpc::{RpcClient, RpcError};
use tokio::time::{Duration, Instant};

use crate::domain::{Booking, GnodeMemory, PropagationArgs, Reservation};
use crate::error::Error;
use crate::traits::CoordinatorStub;
use crate::v1 as wire;

const TAG_TUPLE: &str = "coordinator.PropagationTuple";
const TAG_REQUEST: &str = "coordinator.CoordinatorRequest";
const TAG_RESPONSE: &str = "coordinator.CoordinatorResponse";

fn level_from_wire(v: i32) -> Result<usize, Error> {
    usize::try_from(v).map_err(|_| Error::LevelOutOfRange(i64::from(v)))
}

/// `top` is not a self-describing quantity on the wire — only the caller (here, `exec`, which
/// knows `self.handle.topology().levels()`) can say whether a given value is in range.
///
/// Applied to **every** `top`-carrying request arm, not only the two on the originally-reported
/// attack path (`Body::Replica` then `Body::SetHookingMemory`, which together allocated
/// `vec![0u32; top]`). All nine name the same kind of quantity — a level bound scoping this
/// coordinator's own database — so validating a subset would leave a split with no principled
/// basis, and this codebase has repeatedly been bitten by exactly that: a check correct for one
/// case and silently absent for its structural twin.
///
/// Mirrors upstream's `CoordDatabaseDescriptor.is_valid_key`
/// (`research/impl/vala/coordinator/fk_database.vala:47-55`), which every write to the fixed-keys
/// database is gated on (`is_valid_record`, `fk_database.vala:80-83`, called from the generic
/// `databases.vala` write path). Unlike `HCoord.pos`, upstream *does* have an upper-bound check
/// here — this port's flatter per-message RPC dispatch (no generic `DatabaseHandler` layer) had
/// simply never wired the equivalent guard in, so this is a faithfulness gap rather than a
/// deliberate divergence.
fn validate_top(top: usize, levels: usize) -> Result<usize, Error> {
    if top < 1 || top > levels {
        return Err(Error::InvalidTop { top, levels });
    }
    Ok(top)
}

// ---------------------------------------------------------------------------
// PropagationArgs <-> CoordinatorExecuteArgs (ntk-proto's own wire message)
// ---------------------------------------------------------------------------

pub(crate) fn pack_propagation_args(args: &PropagationArgs) -> CoordinatorExecuteArgs {
    let tuple = wire::PropagationTuple {
        positions: args.positions.clone(),
    };
    CoordinatorExecuteArgs {
        tuple: Some(typed_value(TAG_TUPLE, &tuple)),
        fp_id: args.fp_id,
        propagation_id: args.propagation_id,
        lvl: i32::try_from(args.level).unwrap_or(i32::MAX),
        data: Some(args.data.clone()),
    }
}

pub(crate) fn unpack_propagation_args(
    w: &CoordinatorExecuteArgs,
) -> Result<PropagationArgs, Error> {
    let tuple_tv = w
        .tuple
        .as_ref()
        .ok_or(Error::MissingField("CoordinatorExecuteArgs.tuple"))?;
    let tuple: wire::PropagationTuple = from_typed_value(tuple_tv, TAG_TUPLE)?;
    let data = w
        .data
        .clone()
        .ok_or(Error::MissingField("CoordinatorExecuteArgs.data"))?;
    Ok(PropagationArgs {
        positions: tuple.positions,
        fp_id: w.fp_id,
        propagation_id: w.propagation_id,
        level: level_from_wire(w.lvl)?,
        data,
    })
}

// ---------------------------------------------------------------------------
// Booking / GnodeMemory <-> wire, relative-TTL encoding (SerTimer.msec_ttl analogue)
// ---------------------------------------------------------------------------

fn duration_remaining_ms(deadline: Instant, now: Instant) -> i64 {
    i64::try_from(deadline.saturating_duration_since(now).as_millis()).unwrap_or(i64::MAX)
}

fn booking_to_wire(b: &Booking, now: Instant) -> wire::Booking {
    wire::Booking {
        reserve_request_id: b.reserve_request_id,
        new_pos: b.new_pos,
        new_eldership: b.new_eldership,
        timeout_remaining_ms: duration_remaining_ms(b.expires_at, now),
    }
}

fn booking_from_wire(w: &wire::Booking, now: Instant) -> Booking {
    let remaining_ms = u64::try_from(w.timeout_remaining_ms).unwrap_or(0);
    Booking {
        reserve_request_id: w.reserve_request_id,
        new_pos: w.new_pos,
        new_eldership: w.new_eldership,
        expires_at: now + Duration::from_millis(remaining_ms),
    }
}

pub(crate) fn gnode_memory_to_wire(m: &GnodeMemory) -> wire::GnodeMemory {
    let now = Instant::now();
    wire::GnodeMemory {
        reserve_list: m
            .reserve_list
            .iter()
            .map(|b| booking_to_wire(b, now))
            .collect(),
        max_virtual_pos: m.max_virtual_pos,
        max_eldership: m.max_eldership,
        n_nodes: m.n_nodes.map(|(n, _)| n),
        n_nodes_timeout_remaining_ms: m
            .n_nodes
            .map(|(_, expiry)| duration_remaining_ms(expiry, now)),
        hooking_memory: m.hooking_memory.clone(),
    }
}

pub(crate) fn gnode_memory_from_wire(w: &wire::GnodeMemory) -> GnodeMemory {
    let now = Instant::now();
    let n_nodes = w.n_nodes.map(|n| {
        let remaining_ms = u64::try_from(w.n_nodes_timeout_remaining_ms.unwrap_or(0)).unwrap_or(0);
        (n, now + Duration::from_millis(remaining_ms))
    });
    GnodeMemory {
        reserve_list: w
            .reserve_list
            .iter()
            .map(|b| booking_from_wire(b, now))
            .collect(),
        max_virtual_pos: w.max_virtual_pos,
        max_eldership: w.max_eldership,
        n_nodes,
        hooking_memory: w.hooking_memory.clone(),
    }
}

// ---------------------------------------------------------------------------
// Fixed-keys DB request/response envelope, carried opaquely inside PeerService::exec
// ---------------------------------------------------------------------------

/// The 10 `fk_database.vala` request kinds, decoded off the wire (`crate::service`'s
/// `PeerService::exec` input).
#[derive(Debug, Clone)]
pub(crate) enum RequestBody {
    NumberOfNodes,
    EvaluateEnter {
        top: usize,
        data: TypedValue,
    },
    BeginEnter {
        top: usize,
        data: TypedValue,
    },
    CompletedEnter {
        top: usize,
        data: TypedValue,
    },
    AbortEnter {
        top: usize,
        data: TypedValue,
    },
    GetHookingMemory {
        top: usize,
    },
    SetHookingMemory {
        top: usize,
        data: Option<TypedValue>,
    },
    ReserveEnter {
        top: usize,
        reserve_request_id: i64,
    },
    DeleteReserveEnter {
        top: usize,
        reserve_request_id: i64,
    },
    Replica {
        top: usize,
        memory: GnodeMemory,
    },
}

/// The matching 10 response kinds (`crate::service`'s `PeerService::exec` success output).
#[derive(Debug, Clone)]
pub(crate) enum ResponseBody {
    NumberOfNodes(u64),
    EvaluateEnter(TypedValue),
    BeginEnter(TypedValue),
    CompletedEnter(TypedValue),
    AbortEnter(TypedValue),
    GetHookingMemory(Option<TypedValue>),
    SetHookingMemory,
    ReserveEnter(Option<Reservation>),
    DeleteReserveEnter,
    Replica,
}

pub(crate) fn pack_request(body: &RequestBody) -> TypedValue {
    use wire::coordinator_request::Body;
    let body = match body {
        RequestBody::NumberOfNodes => Body::NumberOfNodes(wire::NumberOfNodesRequest {}),
        RequestBody::EvaluateEnter { top, data } => {
            Body::EvaluateEnter(wire::EvaluateEnterRequest {
                top: *top as u32,
                data: Some(data.clone()),
            })
        }
        RequestBody::BeginEnter { top, data } => Body::BeginEnter(wire::BeginEnterRequest {
            top: *top as u32,
            data: Some(data.clone()),
        }),
        RequestBody::CompletedEnter { top, data } => {
            Body::CompletedEnter(wire::CompletedEnterRequest {
                top: *top as u32,
                data: Some(data.clone()),
            })
        }
        RequestBody::AbortEnter { top, data } => Body::AbortEnter(wire::AbortEnterRequest {
            top: *top as u32,
            data: Some(data.clone()),
        }),
        RequestBody::GetHookingMemory { top } => {
            Body::GetHookingMemory(wire::GetHookingMemoryRequest { top: *top as u32 })
        }
        RequestBody::SetHookingMemory { top, data } => {
            Body::SetHookingMemory(wire::SetHookingMemoryRequest {
                top: *top as u32,
                data: data.clone(),
            })
        }
        RequestBody::ReserveEnter {
            top,
            reserve_request_id,
        } => Body::ReserveEnter(wire::ReserveEnterRequest {
            top: *top as u32,
            reserve_request_id: *reserve_request_id,
        }),
        RequestBody::DeleteReserveEnter {
            top,
            reserve_request_id,
        } => Body::DeleteReserveEnter(wire::DeleteReserveEnterRequest {
            top: *top as u32,
            reserve_request_id: *reserve_request_id,
        }),
        RequestBody::Replica { top, memory } => Body::Replica(wire::ReplicaRequest {
            top: *top as u32,
            memory: Some(gnode_memory_to_wire(memory)),
        }),
    };
    typed_value(TAG_REQUEST, &wire::CoordinatorRequest { body: Some(body) })
}

pub(crate) fn unpack_request(tv: &TypedValue, levels: usize) -> Result<RequestBody, Error> {
    use wire::coordinator_request::Body;
    let w: wire::CoordinatorRequest = from_typed_value(tv, TAG_REQUEST)?;
    let body = w
        .body
        .ok_or(Error::MissingField("CoordinatorRequest.body"))?;
    Ok(match body {
        Body::NumberOfNodes(_) => RequestBody::NumberOfNodes,
        Body::EvaluateEnter(r) => RequestBody::EvaluateEnter {
            top: validate_top(r.top as usize, levels)?,
            data: r
                .data
                .ok_or(Error::MissingField("EvaluateEnterRequest.data"))?,
        },
        Body::BeginEnter(r) => RequestBody::BeginEnter {
            top: validate_top(r.top as usize, levels)?,
            data: r
                .data
                .ok_or(Error::MissingField("BeginEnterRequest.data"))?,
        },
        Body::CompletedEnter(r) => RequestBody::CompletedEnter {
            top: validate_top(r.top as usize, levels)?,
            data: r
                .data
                .ok_or(Error::MissingField("CompletedEnterRequest.data"))?,
        },
        Body::AbortEnter(r) => RequestBody::AbortEnter {
            top: validate_top(r.top as usize, levels)?,
            data: r
                .data
                .ok_or(Error::MissingField("AbortEnterRequest.data"))?,
        },
        Body::GetHookingMemory(r) => RequestBody::GetHookingMemory {
            top: validate_top(r.top as usize, levels)?,
        },
        Body::SetHookingMemory(r) => RequestBody::SetHookingMemory {
            top: validate_top(r.top as usize, levels)?,
            data: r.data,
        },
        Body::ReserveEnter(r) => RequestBody::ReserveEnter {
            top: validate_top(r.top as usize, levels)?,
            reserve_request_id: r.reserve_request_id,
        },
        Body::DeleteReserveEnter(r) => RequestBody::DeleteReserveEnter {
            top: validate_top(r.top as usize, levels)?,
            reserve_request_id: r.reserve_request_id,
        },
        Body::Replica(r) => RequestBody::Replica {
            top: validate_top(r.top as usize, levels)?,
            memory: gnode_memory_from_wire(
                &r.memory
                    .ok_or(Error::MissingField("ReplicaRequest.memory"))?,
            ),
        },
    })
}

pub(crate) fn pack_response(body: &ResponseBody) -> TypedValue {
    use wire::coordinator_response::Body;
    let body = match body {
        ResponseBody::NumberOfNodes(n) => {
            Body::NumberOfNodes(wire::NumberOfNodesResponse { n_nodes: *n })
        }
        ResponseBody::EvaluateEnter(data) => Body::EvaluateEnter(wire::EvaluateEnterResponse {
            data: Some(data.clone()),
        }),
        ResponseBody::BeginEnter(data) => Body::BeginEnter(wire::BeginEnterResponse {
            data: Some(data.clone()),
        }),
        ResponseBody::CompletedEnter(data) => Body::CompletedEnter(wire::CompletedEnterResponse {
            data: Some(data.clone()),
        }),
        ResponseBody::AbortEnter(data) => Body::AbortEnter(wire::AbortEnterResponse {
            data: Some(data.clone()),
        }),
        ResponseBody::GetHookingMemory(data) => {
            Body::GetHookingMemory(wire::GetHookingMemoryResponse { data: data.clone() })
        }
        ResponseBody::SetHookingMemory => Body::SetHookingMemory(wire::SetHookingMemoryResponse {}),
        ResponseBody::ReserveEnter(reservation) => Body::ReserveEnter(wire::ReserveEnterResponse {
            reservation: reservation.map(|r| wire::Reservation {
                new_pos: r.new_pos,
                new_eldership: r.new_eldership,
            }),
        }),
        ResponseBody::DeleteReserveEnter => {
            Body::DeleteReserveEnter(wire::DeleteReserveEnterResponse {})
        }
        ResponseBody::Replica => Body::Replica(wire::ReplicaResponse {}),
    };
    typed_value(
        TAG_RESPONSE,
        &wire::CoordinatorResponse { body: Some(body) },
    )
}

pub(crate) fn unpack_response(tv: &TypedValue) -> Result<ResponseBody, Error> {
    use wire::coordinator_response::Body;
    let w: wire::CoordinatorResponse = from_typed_value(tv, TAG_RESPONSE)?;
    let body = w
        .body
        .ok_or(Error::MissingField("CoordinatorResponse.body"))?;
    Ok(match body {
        Body::NumberOfNodes(r) => ResponseBody::NumberOfNodes(r.n_nodes),
        Body::EvaluateEnter(r) => ResponseBody::EvaluateEnter(
            r.data
                .ok_or(Error::MissingField("EvaluateEnterResponse.data"))?,
        ),
        Body::BeginEnter(r) => ResponseBody::BeginEnter(
            r.data
                .ok_or(Error::MissingField("BeginEnterResponse.data"))?,
        ),
        Body::CompletedEnter(r) => ResponseBody::CompletedEnter(
            r.data
                .ok_or(Error::MissingField("CompletedEnterResponse.data"))?,
        ),
        Body::AbortEnter(r) => ResponseBody::AbortEnter(
            r.data
                .ok_or(Error::MissingField("AbortEnterResponse.data"))?,
        ),
        Body::GetHookingMemory(r) => ResponseBody::GetHookingMemory(r.data),
        Body::SetHookingMemory(_) => ResponseBody::SetHookingMemory,
        Body::ReserveEnter(r) => ResponseBody::ReserveEnter(r.reservation.map(|rr| Reservation {
            new_pos: rr.new_pos,
            new_eldership: rr.new_eldership,
        })),
        Body::DeleteReserveEnter(_) => ResponseBody::DeleteReserveEnter,
        Body::Replica(_) => ResponseBody::Replica,
    })
}

// ---------------------------------------------------------------------------
// RpcCoordinatorStub: real transport implementation of CoordinatorStub
// ---------------------------------------------------------------------------

fn stub_err(e: RpcError) -> ntk_peerservices::StubCallError {
    ntk_peerservices::StubCallError(e.to_string())
}

/// Adapts an [`ntk_rpc::RpcClient`] (real `TcpRpcClient` or, for tests, `FakeRpcClient`) into
/// this crate's [`CoordinatorStub`] surface: builds the `MethodCall` arm for each of the 5
/// `CoordinatorManager.execute_*` methods (`research/impl/vala/coordinator/coord.vala:442-553`).
pub struct RpcCoordinatorStub {
    client: Arc<dyn RpcClient>,
}

impl fmt::Debug for RpcCoordinatorStub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcCoordinatorStub").finish_non_exhaustive()
    }
}

impl RpcCoordinatorStub {
    /// Wraps `client`.
    #[must_use]
    pub fn new(client: Arc<dyn RpcClient>) -> Self {
        Self { client }
    }

    /// Identity/NIC addressing is out of this crate's scope (no `ntk-identities`/
    /// `ntk-neighborhood` dependency) — every call carries an empty [`CallerContext`].
    fn caller() -> CallerContext {
        CallerContext {
            source_id: None,
            src_nic: None,
        }
    }

    fn unicast_id() -> TypedValue {
        TypedValue::new(String::new(), Vec::new())
    }

    /// Every `execute_*` method is `void` upstream — fire-and-forget, matching how
    /// `PeersStub::forward_peer_message` &c. use `notify` rather than `call`.
    fn notify(&self, call: Call) -> BoxFuture<'_, Result<(), RpcError>> {
        self.client.notify(
            Self::caller(),
            Self::unicast_id(),
            MethodCall { call: Some(call) },
        )
    }
}

impl CoordinatorStub for RpcCoordinatorStub {
    fn execute_prepare_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        let call = Call::CoordinatorExecutePrepareMigration(pack_propagation_args(&args));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn execute_finish_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        let call = Call::CoordinatorExecuteFinishMigration(pack_propagation_args(&args));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn execute_prepare_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        let call = Call::CoordinatorExecutePrepareEnter(pack_propagation_args(&args));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn execute_finish_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        let call = Call::CoordinatorExecuteFinishEnter(pack_propagation_args(&args));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn execute_we_have_splitted(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), ntk_peerservices::StubCallError>> {
        let call = Call::CoordinatorExecuteWeHaveSplitted(pack_propagation_args(&args));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }
}

#[cfg(test)]
mod validate_top_tests {
    use super::*;

    /// Every `top`-carrying request arm, paired with a builder for its wire body. Kept exhaustive
    /// on purpose: the defect this pins was two validated arms beside seven unvalidated ones, so a
    /// new arm added without a guard should show up here as an obvious omission.
    fn hostile_bodies(top: u32) -> Vec<(&'static str, wire::coordinator_request::Body)> {
        use wire::coordinator_request::Body;
        let data = || Some(TypedValue::default());
        vec![
            (
                "EvaluateEnter",
                Body::EvaluateEnter(wire::EvaluateEnterRequest { top, data: data() }),
            ),
            (
                "BeginEnter",
                Body::BeginEnter(wire::BeginEnterRequest { top, data: data() }),
            ),
            (
                "CompletedEnter",
                Body::CompletedEnter(wire::CompletedEnterRequest { top, data: data() }),
            ),
            (
                "AbortEnter",
                Body::AbortEnter(wire::AbortEnterRequest { top, data: data() }),
            ),
            (
                "GetHookingMemory",
                Body::GetHookingMemory(wire::GetHookingMemoryRequest { top }),
            ),
            (
                "SetHookingMemory",
                Body::SetHookingMemory(wire::SetHookingMemoryRequest { top, data: data() }),
            ),
            (
                "ReserveEnter",
                Body::ReserveEnter(wire::ReserveEnterRequest {
                    top,
                    reserve_request_id: 1,
                }),
            ),
            (
                "DeleteReserveEnter",
                Body::DeleteReserveEnter(wire::DeleteReserveEnterRequest {
                    top,
                    reserve_request_id: 1,
                }),
            ),
            (
                "Replica",
                Body::Replica(wire::ReplicaRequest {
                    top,
                    memory: Some(wire::GnodeMemory::default()),
                }),
            ),
        ]
    }

    fn request(body: wire::coordinator_request::Body) -> TypedValue {
        typed_value(TAG_REQUEST, &wire::CoordinatorRequest { body: Some(body) })
    }

    /// `top = u32::MAX` is the value that drove `vec![0u32; top]` in `spawn_replicate` before the
    /// fix. Every arm must refuse it at decode, before any sizing or storing happens.
    #[test]
    fn a_hostile_top_is_refused_at_every_request_arm() {
        for (name, body) in hostile_bodies(u32::MAX) {
            let err = unpack_request(&request(body), 4)
                .expect_err("{name}: u32::MAX top must be refused at decode");
            assert!(
                matches!(err, Error::InvalidTop { .. }),
                "{name}: expected InvalidTop, got {err:?}"
            );
        }
    }

    /// `top == 0` names no level at all. Refused for the same reason a too-large one is: `top` is
    /// consumed as a span length (`vec![0u32; top]`, `TupleNode` arity), and a zero-length span is
    /// not a meaningful coordinator key.
    #[test]
    fn a_zero_top_is_refused_at_every_request_arm() {
        for (name, body) in hostile_bodies(0) {
            let err = unpack_request(&request(body), 4)
                .expect_err("{name}: a zero top must be refused at decode");
            assert!(
                matches!(err, Error::InvalidTop { .. }),
                "{name}: expected InvalidTop, got {err:?}"
            );
        }
    }

    /// The whole legal range must still decode — a guard that also rejected valid traffic would be
    /// a worse defect than the one it replaced.
    #[test]
    fn every_in_range_top_still_decodes_at_every_request_arm() {
        let levels = 4;
        for top in 1..=u32::try_from(levels).unwrap() {
            for (name, body) in hostile_bodies(top) {
                unpack_request(&request(body), levels)
                    .unwrap_or_else(|err| panic!("{name}: top {top} must decode, got {err:?}"));
            }
        }
    }

    #[test]
    fn validate_top_boundaries() {
        assert!(validate_top(1, 4).is_ok());
        assert!(validate_top(4, 4).is_ok());
        assert!(matches!(
            validate_top(5, 4),
            Err(Error::InvalidTop { top: 5, levels: 4 })
        ));
        assert!(matches!(
            validate_top(0, 4),
            Err(Error::InvalidTop { top: 0, levels: 4 })
        ));
    }
}
