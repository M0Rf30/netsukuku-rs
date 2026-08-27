//! The outbound seam: what [`crate::actor::Handle`] needs from the PeerServices substrate
//! (`ntk-peerservices`), and an in-memory [`FakeSubstrate`] for tests that don't need a real
//! multi-node network.
//!
//! **Shape note**: every other phase-2/3 module's outbound seam (`NeighborhoodStubFactory`,
//! `IdentityStubFactory`, `QspnStubFactory`, ...) abstracts "how do I reach one specific remote
//! node over the wire". ANDNA is built as two `PeerService`s *registered on* `ntk-peerservices`
//! (RFC 0014), which already owns that abstraction one layer down (`PeersStub`/`RoutingEnv`,
//! with `RpcPeersStub`+`FakeRpcClient`/`RpcPeersStub`+real-`TcpRpcClient` variants). This crate's
//! own seam is one level up: "how do I reach the *substrate*" — `contact_peer`/`replicate`/
//! `register`, hash-routed rather than node-addressed. [`RealSubstrate`] wraps a live
//! `ntk_peerservices::Handle`; [`FakeSubstrate`] loops requests straight into locally-registered
//! `PeerService`s, for single-node crypto/TTL/cap unit tests that don't need real DHT routing.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::{Naddr, Topology};
use ntk_peerservices::{ContactPeerError, PeerService, ServiceId, TupleNode};
use ntk_proto::v1::TypedValue;

/// What [`crate::actor::Handle`]'s top-level `register`/`resolve`/`renew` operations need from
/// the PeerServices substrate.
pub trait AndnaSubstrate: Send + Sync {
    fn topology(&self) -> &Topology;
    fn my_pos(&self) -> &Naddr;

    /// Registers a `PeerService` (this crate's `AndnaService`/`CounterService`) to receive
    /// inbound requests.
    fn register(&self, service: Arc<dyn PeerService>) -> BoxFuture<'_, ()>;

    /// Routes `request` to whichever node is closest to `target` (RFC 0014 §2's `h(k) =
    /// H(h'(k))`), dropping the respondant's address — this crate never needs it.
    fn contact_peer(
        &self,
        p_id: ServiceId,
        target: TupleNode,
        request: TypedValue,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<TypedValue, ContactPeerError>>;

    /// RFC 0014 §2.2 step 5's redundancy rule: sends `request` to `q` distinct nodes closest to
    /// `target`, returning every reply that arrived.
    fn replicate(
        &self,
        p_id: ServiceId,
        target: TupleNode,
        request: TypedValue,
        timeout: Duration,
        q: u32,
    ) -> BoxFuture<'_, Vec<TypedValue>>;
}

/// The real substrate: a live `ntk_peerservices::Handle`.
impl AndnaSubstrate for ntk_peerservices::Handle {
    fn topology(&self) -> &Topology {
        ntk_peerservices::Handle::topology(self)
    }

    fn my_pos(&self) -> &Naddr {
        ntk_peerservices::Handle::my_pos(self)
    }

    fn register(&self, service: Arc<dyn PeerService>) -> BoxFuture<'_, ()> {
        Box::pin(async move { ntk_peerservices::Handle::register(self, service).await })
    }

    fn contact_peer(
        &self,
        p_id: ServiceId,
        target: TupleNode,
        request: TypedValue,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<TypedValue, ContactPeerError>> {
        Box::pin(async move {
            ntk_peerservices::Handle::contact_peer(
                self,
                p_id,
                Some(target),
                request,
                timeout,
                None,
                Vec::new(),
            )
            .await
            .map(|(response, _respondant)| response)
        })
    }

    fn replicate(
        &self,
        p_id: ServiceId,
        target: TupleNode,
        request: TypedValue,
        timeout: Duration,
        q: u32,
    ) -> BoxFuture<'_, Vec<TypedValue>> {
        Box::pin(async move {
            ntk_peerservices::Handle::replicate(self, p_id, target, request, timeout, q)
                .await
                .into_iter()
                .map(|(response, _respondant)| response)
                .collect()
        })
    }
}

/// A single-node, no-network substrate: `contact_peer`/`replicate` call straight into whichever
/// locally-held service matches `p_id`, with an empty `client_tuple` (as `ntk-peerservices`
/// itself does for its own "target is myself" fast path). Useful for testing this crate's
/// signature/TTL/cap/collision logic without spinning up a real multi-node network — the
/// multi-node behavior itself (routing convergence, replication across distinct nodes) is
/// exercised separately, against the real substrate, in `tests/multi_node.rs`.
pub struct FakeSubstrate {
    topology: Topology,
    my_pos: Naddr,
    andna: Arc<dyn PeerService>,
    counter: Arc<dyn PeerService>,
}

impl std::fmt::Debug for FakeSubstrate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeSubstrate").finish_non_exhaustive()
    }
}

impl FakeSubstrate {
    /// Wires a fixed pair of locally-registered services behind `topology`/`my_pos`.
    #[must_use]
    pub fn new(
        topology: Topology,
        my_pos: Naddr,
        andna: Arc<dyn PeerService>,
        counter: Arc<dyn PeerService>,
    ) -> Self {
        Self {
            topology,
            my_pos,
            andna,
            counter,
        }
    }

    fn service_for(&self, p_id: ServiceId) -> &Arc<dyn PeerService> {
        if p_id == self.andna.service_id() {
            &self.andna
        } else {
            &self.counter
        }
    }
}

impl AndnaSubstrate for FakeSubstrate {
    fn topology(&self) -> &Topology {
        &self.topology
    }

    fn my_pos(&self) -> &Naddr {
        &self.my_pos
    }

    fn register(&self, _service: Arc<dyn PeerService>) -> BoxFuture<'_, ()> {
        // The fake is wired to fixed services at construction; nothing to do.
        Box::pin(async {})
    }

    fn contact_peer(
        &self,
        p_id: ServiceId,
        _target: TupleNode,
        request: TypedValue,
        _timeout: Duration,
    ) -> BoxFuture<'_, Result<TypedValue, ContactPeerError>> {
        let service = self.service_for(p_id).clone();
        Box::pin(async move {
            service
                .exec(request, &[])
                .await
                .map_err(|_| ContactPeerError::NoParticipants)
        })
    }

    fn replicate(
        &self,
        p_id: ServiceId,
        target: TupleNode,
        request: TypedValue,
        timeout: Duration,
        _q: u32,
    ) -> BoxFuture<'_, Vec<TypedValue>> {
        Box::pin(async move {
            match self.contact_peer(p_id, target, request, timeout).await {
                Ok(response) => vec![response],
                Err(_) => Vec::new(),
            }
        })
    }
}
