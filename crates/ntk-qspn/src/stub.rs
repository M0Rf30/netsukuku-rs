//! The outbound RPC seam: `IQspnStubFactory`/`IQspnManagerStub`
//! (`research/impl/vala/qspn/api.vala:104,141-158`,
//! `research/impl/vala/ntkdrpc/addr_stub.vala:40-43`) — QSPN's sole coupling
//! to a real transport. Implemented once for `ntk-rpc`'s real transport (by
//! whichever crate composes QSPN with Neighborhood/RPC — out of this crate's
//! scope) and once for [`crate::fake::FakeQspnStubFactory`], the
//! substitutability seam `research/notes/06-rust-stack.md` calls out by name.

use futures::future::BoxFuture;
use ntk_common::Naddr;
use ntk_rpc::RpcError;

use crate::arc::ArcId;
use crate::path::EtpMessage;

/// Retry hook for a missing broadcast ack: `IQspnMissingArcHandler`
/// (`research/impl/vala/qspn/api.vala:141-144`). If an arc's ack doesn't
/// arrive, the caller falls back to a reliable unicast resend to just that
/// arc (`MissingArcSendEtp`, `research/impl/vala/qspn/missing_arcs.vala:24-40`).
pub trait MissingArcHandler: Send + Sync {
    fn missing(&self, arc: ArcId);
}

/// Builds outbound stubs, keyed by arc-set (broadcast) or a single arc (TCP)
/// — `IQspnStubFactory` (`api.vala:146-158`).
pub trait QspnStubFactory: Send + Sync {
    /// One stub addressing every arc in `arcs` via broadcast, retrying
    /// through `missing` on a missed ack (`i_qspn_get_broadcast`,
    /// `api.vala:148-152`). An empty `arcs` is legal (a void stub that never
    /// actually sends, matching upstream's `QspnManagerStubVoid`,
    /// `research/impl/vala/ntkd/qspn_helpers.vala:42`).
    fn broadcast(
        &self,
        arcs: &[ArcId],
        missing: Option<std::sync::Arc<dyn MissingArcHandler>>,
    ) -> std::sync::Arc<dyn QspnStub>;

    /// One stub addressing exactly `arc` over a reliable unicast transport
    /// (`i_qspn_get_tcp`, `api.vala:153-157`).
    fn tcp(&self, arc: ArcId) -> std::sync::Arc<dyn QspnStub>;
}

/// The four outbound calls one stub can make — `IQspnManagerStub`
/// (`research/impl/vala/ntkdrpc/addr_stub.vala:40-43`, plus
/// `got_prepare_destroy`/`got_destroy` from the same interface elsewhere in
/// that file) — the exact method surface `ntk_proto::v1::MethodCall`'s
/// `qspn_*` arms carry on the wire.
///
/// Errors are [`ntk_rpc::RpcError`] directly: it already distinguishes local
/// transport failure from [`ntk_rpc::RpcError::Remote`] (the wire-carried
/// `QspnNotAcceptedError`/`QspnBootstrapInProgressError`), the same split
/// upstream's `StubError` vs. domain `errordomain`s draws.
pub trait QspnStub: Send + Sync {
    fn get_full_etp(
        &self,
        requesting_address: Naddr,
    ) -> BoxFuture<'_, Result<EtpMessage, RpcError>>;
    fn send_etp(&self, etp: EtpMessage, is_full: bool) -> BoxFuture<'_, Result<(), RpcError>>;
    fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>>;
    fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>>;
}
