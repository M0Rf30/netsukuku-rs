//! [`PeerLinks`]: the one outbound-connection table every module's stub factory shares.
//!
//! `ntk_proto::v1::MethodCall` is one enum spanning every module (neighborhood/identities/
//! qspn/peers/coordinator/hooking), so a single [`ntk_rpc::RpcClient`] per neighbor already
//! carries all of them — there is no need for a separate TCP connection per module. This table
//! is that one shared pool, keyed by [`LinkId`], populated once a neighborhood arc is
//! established and consulted by every outbound stub factory in [`crate::node::stubs`] and
//! every [`ntk_peerservices::RoutingEnv`]/[`ntk_coordinator::CoordinatorStubFactory`] lookup in
//! [`crate::node::adapters`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ntk_rpc::RpcClient;

use crate::node::registry::LinkId;

#[derive(Default)]
pub struct PeerLinks {
    clients: Mutex<HashMap<LinkId, Arc<dyn RpcClient>>>,
    /// This identity's own TCP listen/dial port (`NtkdConfig::port`), set once by
    /// `crate::node::lifecycle::run` before any inbound message can possibly be processed (see
    /// that call site's doc for why that ordering is guaranteed) — read by
    /// `crate::node::stubs`'s lazy-dialing [`ntk_neighborhood::NeighborhoodStubFactory::unicast`]
    /// implementation, the one stub factory that must be able to open a *brand-new* connection
    /// rather than only ever look one up (see that impl's doc comment for why).
    port: OnceLock<u16>,
}

impl std::fmt::Debug for PeerLinks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerLinks").finish_non_exhaustive()
    }
}

impl PeerLinks {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records this identity's own port for [`crate::node::stubs`]'s lazy-dialing `unicast`
    /// stub to dial *other* neighbours on. Idempotent (a `OnceLock`): only the first call
    /// (`crate::node::lifecycle::run`'s own startup call) has any effect.
    pub fn set_port(&self, port: u16) {
        let _ = self.port.set(port);
    }

    #[must_use]
    pub fn port(&self) -> Option<u16> {
        self.port.get().copied()
    }

    pub fn insert(&self, link: LinkId, client: Arc<dyn RpcClient>) {
        self.clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(link, client);
    }

    pub fn remove(&self, link: LinkId) {
        self.clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&link);
    }

    #[must_use]
    pub fn get(&self, link: LinkId) -> Option<Arc<dyn RpcClient>> {
        self.clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&link)
            .cloned()
    }

    #[must_use]
    pub fn all(&self) -> Vec<(LinkId, Arc<dyn RpcClient>)> {
        self.clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, c)| (*id, c.clone()))
            .collect()
    }
}
