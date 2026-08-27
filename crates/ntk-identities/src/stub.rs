//! The outbound-call seam substituted by tests.

use std::sync::Arc;

use ntk_proto::v1::CallerContext;
use ntk_rpc::RpcClient;

use crate::arc::ArcId;

/// Outbound-call seam analogous to upstream's `IIdmgmtStubFactory`
/// (`identities/identities.vala:38-42`): resolves a neighborhood-level arc
/// to an RPC client for this module's three methods, and the reverse
/// mapping from an inbound call's `CallerContext` back to the arc it
/// arrived on (`IIdmgmtStubFactory::get_arc`, `:41`). A trait so tests
/// substitute an in-memory [`ntk_rpc::FakeRpcClient`] for the real
/// transport, and so this crate never depends on `ntk-neighborhood`'s
/// concrete arc type — [`ArcId`] is an opaque handle the daemon assigns.
pub trait IdentityStubFactory: Send + Sync {
    /// The outbound RPC client for `arc`.
    fn stub(&self, arc: ArcId) -> Arc<dyn RpcClient>;

    /// Resolves which arc an inbound call arrived on, from its
    /// `CallerContext` (`IIdmgmtStubFactory::get_arc`,
    /// `identities.vala:41`). `None` if the context does not correspond to
    /// a known arc.
    fn arc_for_caller(&self, caller: &CallerContext) -> Option<ArcId>;
}
