//! In-memory [`QspnStubFactory`]/[`QspnStub`] for tests/simulation — the fake
//! half of the outbound stub substitutability seam
//! (`research/notes/06-rust-stack.md` §"Where Rust traits replace...",
//! mirroring [`ntk_rpc::FakeRpcClient`]'s role for the transport layer).
//! Routes calls directly to a registered peer [`QspnHandle`]; each side of a
//! simulated link independently registers its own [`ArcId`] for it, matching
//! how two real nodes each mint their own local `arc_id` for the same
//! physical arc (`research/impl/vala/qspn/qspn.vala:727-731`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use ntk_common::Naddr;
use ntk_rpc::RpcError;

use crate::arc::ArcId;
use crate::manager::QspnHandle;
use crate::path::EtpMessage;
use crate::rpc::qspn_error_to_remote;
use crate::stub::{MissingArcHandler, QspnStub, QspnStubFactory};

#[derive(Clone)]
struct PeerLink {
    peer: QspnHandle,
    /// The peer's own `ArcId` for the link that reaches this node — what the
    /// peer's `handle_*` methods expect as their `arc` parameter.
    peer_arc: ArcId,
}

/// In-memory stub factory: `my_arc -> PeerLink` on this node's side of each
/// simulated link.
#[derive(Default)]
pub struct FakeQspnStubFactory {
    peers: Mutex<HashMap<ArcId, PeerLink>>,
}

impl std::fmt::Debug for FakeQspnStubFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeQspnStubFactory")
            .finish_non_exhaustive()
    }
}

impl FakeQspnStubFactory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `my_arc` as reaching `peer`, whose own arc for this same
    /// link is `peer_arc`. Call once per side of a simulated link (i.e.
    /// twice per link, once from each node's factory).
    pub fn connect(&self, my_arc: ArcId, peer: QspnHandle, peer_arc: ArcId) {
        self.peers
            .lock()
            .expect("not poisoned")
            .insert(my_arc, PeerLink { peer, peer_arc });
    }

    /// Removes `my_arc`'s peer mapping, simulating that link going away
    /// (an arc flap or partition).
    pub fn disconnect(&self, my_arc: ArcId) {
        self.peers.lock().expect("not poisoned").remove(&my_arc);
    }

    fn link(&self, arc: ArcId) -> Option<PeerLink> {
        self.peers.lock().expect("not poisoned").get(&arc).cloned()
    }
}

struct FakeStub {
    link: Option<PeerLink>,
}

impl QspnStub for FakeStub {
    fn get_full_etp(
        &self,
        requesting_address: Naddr,
    ) -> BoxFuture<'_, Result<EtpMessage, RpcError>> {
        let link = self.link.clone();
        Box::pin(async move {
            let link = link.ok_or(RpcError::ConnectionClosed)?;
            link.peer
                .handle_get_full_etp(link.peer_arc, requesting_address)
                .await
                .map_err(|e| RpcError::Remote(qspn_error_to_remote(e)))
        })
    }

    fn send_etp(&self, etp: EtpMessage, is_full: bool) -> BoxFuture<'_, Result<(), RpcError>> {
        let link = self.link.clone();
        Box::pin(async move {
            let link = link.ok_or(RpcError::ConnectionClosed)?;
            link.peer
                .handle_send_etp(link.peer_arc, etp, is_full)
                .await
                .map_err(|e| RpcError::Remote(qspn_error_to_remote(e)))
        })
    }

    fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        let link = self.link.clone();
        Box::pin(async move {
            let link = link.ok_or(RpcError::ConnectionClosed)?;
            link.peer
                .handle_got_prepare_destroy()
                .await
                .map_err(|e| RpcError::Remote(qspn_error_to_remote(e)))
        })
    }

    fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        let link = self.link.clone();
        Box::pin(async move {
            let link = link.ok_or(RpcError::ConnectionClosed)?;
            link.peer
                .handle_got_destroy(link.peer_arc)
                .await
                .map_err(|e| RpcError::Remote(qspn_error_to_remote(e)))
        })
    }
}

struct FakeBroadcastStub {
    links: Vec<(ArcId, Option<PeerLink>)>,
    missing: Option<Arc<dyn MissingArcHandler>>,
}

impl QspnStub for FakeBroadcastStub {
    fn get_full_etp(
        &self,
        _requesting_address: Naddr,
    ) -> BoxFuture<'_, Result<EtpMessage, RpcError>> {
        // Upstream never calls get_full_etp on a broadcast stub
        // (api.vala:148-152 vs 153-157).
        Box::pin(async { Err(RpcError::ConnectionClosed) })
    }

    fn send_etp(&self, etp: EtpMessage, is_full: bool) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            for (arc, link) in &self.links {
                match link {
                    None => {
                        if let Some(missing) = &self.missing {
                            missing.missing(*arc);
                        }
                    }
                    Some(link) => {
                        if link
                            .peer
                            .handle_send_etp(link.peer_arc, etp.clone(), is_full)
                            .await
                            .is_err()
                            && let Some(missing) = &self.missing
                        {
                            missing.missing(*arc);
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            for (_, link) in &self.links {
                if let Some(link) = link {
                    let _ = link.peer.handle_got_prepare_destroy().await;
                }
            }
            Ok(())
        })
    }

    fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            for (arc, link) in &self.links {
                if let Some(link) = link {
                    let _ = link.peer.handle_got_destroy(link.peer_arc).await;
                } else if let Some(missing) = &self.missing {
                    missing.missing(*arc);
                }
            }
            Ok(())
        })
    }
}

impl QspnStubFactory for FakeQspnStubFactory {
    fn broadcast(
        &self,
        arcs: &[ArcId],
        missing: Option<Arc<dyn MissingArcHandler>>,
    ) -> Arc<dyn QspnStub> {
        let links = arcs.iter().map(|&a| (a, self.link(a))).collect();
        Arc::new(FakeBroadcastStub { links, missing })
    }

    fn tcp(&self, arc: ArcId) -> Arc<dyn QspnStub> {
        Arc::new(FakeStub {
            link: self.link(arc),
        })
    }
}
