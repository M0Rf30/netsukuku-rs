//! Outbound stub factories: the real-transport half of every module's `StubFactory` seam, built
//! over [`PeerLinks`] (one shared [`RpcClient`] per neighbor — see that module's doc comment for
//! why one connection carries every module's calls).
//!
//! `ntk-peerservices` and `ntk-coordinator` already ship their own real-transport stub
//! (`RpcPeersStub`, `RpcCoordinatorStub`); `ntk-qspn` and `ntk-hooking` do not (only their
//! `Fake*` doubles exist), so this module hand-writes `QspnStub`/`HookingStub` over the shared
//! `RpcClient`, encoding/decoding with each crate's already-exported `wire` helpers.

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_common::Naddr;
use ntk_hooking::{
    DeleteReservationRequest, EntryData, ExploreGNodeRequest, ExploreGNodeResponse, HookingStub,
    HookingStubFactory, NetworkData, RequestPacket, ResponsePacket, SearchMigrationPathErrorPkt,
    SearchMigrationPathRequest, SearchMigrationPathResponse,
};
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value as RespValue;
use ntk_proto::v1::{CallerContext, MethodCall, TypedValue};
use ntk_qspn::{ArcId as QspnArcId, EtpMessage, MissingArcHandler, QspnStub, QspnStubFactory};
use ntk_rpc::{RpcClient, RpcError};

use crate::node::peers::PeerLinks;
use crate::node::registry::{LinkId, LinkRegistry};

fn empty_caller() -> CallerContext {
    CallerContext {
        source_id: None,
        src_nic: None,
    }
}

/// Builds the outbound `CallerContext` for a call over an arc, embedding this identity's own
/// stable Neighborhood id — see `crate::node::registry::encode_caller_id`'s doc for why.
fn caller_for_id(id: ntk_neighborhood::NodeId) -> CallerContext {
    CallerContext {
        source_id: None,
        src_nic: Some(crate::node::registry::encode_caller_id(id)),
    }
}

fn unicast_id() -> TypedValue {
    TypedValue::new(String::new(), Vec::new())
}

fn malformed(msg: impl Into<String>) -> RpcError {
    RpcError::Malformed(msg.into())
}

// ---------------------------------------------------------------------------
// Neighborhood
// ---------------------------------------------------------------------------

/// Implements [`ntk_neighborhood::NeighborhoodStubFactory`]: `broadcast(dev)` wraps this NIC's
/// bound [`ntk_rpc::UdpBroadcaster`] (via [`ntk_neighborhood::BroadcastRpcClient`]); `unicast(arc)`
/// returns a [`LazyLinkClient`] over the arc's [`LinkId`] — see that type's doc comment for why
/// this stub factory, alone among this daemon's stub factories, must be able to *open* a
/// connection to a brand-new neighbour rather than only ever look one up in [`PeerLinks`].
#[derive(Debug)]
pub struct NeighborhoodStubFactoryAdapter {
    pub broadcasters: std::collections::HashMap<String, Arc<ntk_rpc::UdpBroadcaster>>,
    pub links: Arc<PeerLinks>,
    pub registry: Arc<LinkRegistry>,
}

impl ntk_neighborhood::NeighborhoodStubFactory for NeighborhoodStubFactoryAdapter {
    fn broadcast(&self, dev: &str) -> Arc<dyn RpcClient> {
        let broadcaster = self
            .broadcasters
            .get(dev)
            .unwrap_or_else(|| panic!("no UDP broadcaster bound for nic {dev}"))
            .clone();
        Arc::new(ntk_neighborhood::BroadcastRpcClient::new(broadcaster))
    }

    fn unicast(&self, arc: &ntk_neighborhood::Arc) -> Arc<dyn RpcClient> {
        let link =
            self.registry
                .link_for_neighbour(arc.neighbour_id, &arc.neighbour_mac, &arc.my_dev);
        Arc::new(LazyLinkClient {
            link,
            addr: arc.neighbour_nic_addr.clone(),
            dev: arc.my_dev.clone(),
            links: self.links.clone(),
        })
    }
}

/// An [`RpcClient`] that resolves to the real per-neighbour connection lazily, on first use —
/// dialing [`LazyLinkClient::addr`], bound to [`LazyLinkClient::dev`] (see
/// [`ntk_rpc::TcpRpcClient::connect_via`]'s doc for why an unscoped dial is unsafe with 2+
/// monitored NICs), via [`TcpDialer`] and caching the result in [`PeerLinks`] if none exists
/// yet, so every other module's stub factory (`crate::node::peers`'s module doc) reuses the
/// same device-bound connection from then on.
///
/// # Why `unicast` can't just look [`PeerLinks`] up
/// Every *other* seam in this file assumes something else populates [`PeerLinks`] first — a
/// reasonable assumption when that something is this daemon's own steady-state loop reacting to
/// `ntk_neighborhood::Event::ArcAdded` (`crate::node::lifecycle::on_neighborhood_event`). But
/// `NeighborhoodStubFactory::unicast` is the very call `ArcAdded` itself depends on: that event
/// fires only "at first successful cost measurement" (its own doc comment), and cost measurement
/// only ever runs after the arc monitor's `nop` unicast call has already returned `Ok`
/// (`ntk_neighborhood::manager`'s arc-monitor loop). Waiting for `ArcAdded` to populate the very
/// connection this call itself must succeed to produce is circular — the first connection to a
/// brand-new neighbour has to be opened here, on demand, or the arc can never leave `Requested`
/// (confirmed against a real kernel: this was the third real bug blocking two-node discovery,
/// found only after fixing the linklocal `/16` prefix and per-node address distinctness).
///
/// Safe to dial on demand here: see [`crate::node::peers::PeerLinks::set_port`]'s call site
/// (`crate::node::lifecycle::run`) for why this identity's own port is guaranteed already
/// recorded by the time any inbound message — the only thing that can ever cause `unicast` to be
/// invoked in the first place — could possibly be processed.
struct LazyLinkClient {
    link: LinkId,
    addr: String,
    /// This identity's own NIC (`Arc::my_dev`) the arc runs over — the egress device
    /// [`crate::node::lifecycle::Dialer::dial_via`] binds the outbound socket to, so a relay
    /// monitoring 2+ NICs dials each neighbour off the correct one instead of whichever NIC the
    /// kernel's route table happens to prefer for the shared `169.254.0.0/16` prefix.
    dev: String,
    links: Arc<PeerLinks>,
}

/// How long [`LazyLinkClient::resolve`] retries a failed first dial before conceding the
/// neighbour is genuinely unreachable, and how often it retries in between.
///
/// # Bug this fixes
/// `ntk_neighborhood::manager::run_arc_monitor`'s arc-monitor task gives up on an arc
/// *permanently* the first time its `nop()` unicast `notify()` call returns `Err` (no retry —
/// there is no next tick). Confirmed against a real kernel (4-node chain, two adjacent 2-NIC
/// relay nodes): each side composes its own identity independently and in parallel, so nothing
/// orders "my own `TcpServer` is already `accept()`-ing" before "my neighbour's arc-monitor
/// fires its first `nop()` at me" — a real, if narrow, window where the very first dial hits
/// `ECONNREFUSED` purely because the callee hasn't reached `TcpServer::bind`/`.serve()` yet, not
/// because the neighbour is actually unreachable. Before this fix that single transient refusal
/// permanently starved that arc of a cost, with a *directional* symptom: this identity's own
/// outbound `nop()` to that neighbour would never succeed, while the neighbour's own `nop()`
/// back (dialing a `TcpServer` that *had* come up by then) succeeded fine — so exactly one side
/// of the arc could still resolve the other as a caller, and the side that couldn't ever measure
/// a cost, and both sides' subsequent qspn `get_full_etp` fetches over that arc simply timed out
/// (10s, `QspnConfig::arc_timeout`), never a wire-level error. Retrying the dial itself here, for
/// a window comfortably wider than realistic startup skew between two independently-composed
/// identities, absorbs that race before it ever reaches `run_arc_monitor`'s one-shot check.
///
/// `LAZY_LINK_DIAL_ATTEMPT_TIMEOUT` bounds each individual dial, not just the overall budget:
/// a callee whose interface is administratively up but whose `TcpServer` isn't listening yet
/// does not always answer with an immediate `ECONNREFUSED` — an in-namespace veth peer with no
/// listener can also leave a `SYN` unanswered until the kernel's own (multi-second to
/// multi-minute) retransmit timeout, which would otherwise consume the *entire* retry budget on
/// a single hung attempt and defeat the retry entirely.
const LAZY_LINK_DIAL_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
const LAZY_LINK_DIAL_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const LAZY_LINK_DIAL_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(300);

impl LazyLinkClient {
    async fn resolve(&self) -> Result<Arc<dyn RpcClient>, RpcError> {
        if let Some(client) = self.links.get(self.link) {
            return Ok(client);
        }
        let port = self.links.port().ok_or_else(|| {
            malformed("PeerLinks::set_port was never called before a neighbour needed dialing")
        })?;
        let dialer = crate::node::lifecycle::TcpDialer::default();
        let deadline = tokio::time::Instant::now() + LAZY_LINK_DIAL_RETRY_BUDGET;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let outcome = tokio::time::timeout(
                LAZY_LINK_DIAL_ATTEMPT_TIMEOUT,
                crate::node::lifecycle::Dialer::dial_via(
                    &dialer,
                    &self.addr,
                    port,
                    Some(&self.dev),
                ),
            )
            .await;
            match &outcome {
                Err(_elapsed) => tracing::debug!(
                    addr = %self.addr, dev = %self.dev, attempt,
                    "ntkd: dial attempt timed out (no response within the per-attempt budget)"
                ),
                Ok(None) => tracing::debug!(
                    addr = %self.addr, dev = %self.dev, attempt,
                    "ntkd: dial attempt returned promptly but failed"
                ),
                Ok(Some(_)) => {}
            }
            if let Some(client) = outcome.ok().flatten() {
                self.links.insert(self.link, client.clone());
                return Ok(client);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RpcError::ConnectionClosed);
            }
            tokio::time::sleep(LAZY_LINK_DIAL_RETRY_INTERVAL).await;
        }
    }
}

impl RpcClient for LazyLinkClient {
    fn call<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<ntk_proto::v1::ResponsePayload, RpcError>> {
        Box::pin(async move { self.resolve().await?.call(caller, unicast_id, call).await })
    }

    fn notify<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        Box::pin(async move { self.resolve().await?.notify(caller, unicast_id, call).await })
    }

    /// Overridden (rather than left at [`RpcClient`]'s default, auth-dropping forward) so
    /// `ntk-neighborhood`'s hop-auth signing actually reaches the wire on this daemon's real
    /// per-neighbour connection — the only production [`RpcClient`] neighbourhood's
    /// `NeighborhoodStubFactory::unicast` resolves to (`NeighborhoodStubFactoryAdapter`, below).
    fn call_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<ntk_proto::v1::ResponsePayload, RpcError>> {
        Box::pin(async move {
            self.resolve()
                .await?
                .call_authenticated(caller, unicast_id, call, auth)
                .await
        })
    }

    /// See [`Self::call_authenticated`]'s doc.
    fn notify_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        Box::pin(async move {
            self.resolve()
                .await?
                .notify_authenticated(caller, unicast_id, call, auth)
                .await
        })
    }
}

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

/// Implements [`ntk_identities::IdentityStubFactory`] over [`PeerLinks`]/[`LinkRegistry`].
#[derive(Debug)]
pub struct IdentityStubFactoryAdapter {
    pub links: Arc<PeerLinks>,
    pub registry: Arc<LinkRegistry>,
}

impl ntk_identities::IdentityStubFactory for IdentityStubFactoryAdapter {
    fn stub(&self, arc: ntk_identities::ArcId) -> Arc<dyn RpcClient> {
        self.links
            .get(LinkId(arc.0))
            .unwrap_or_else(|| panic!("no outbound connection for identity arc {arc:?}"))
    }

    fn arc_for_caller(&self, caller: &CallerContext) -> Option<ntk_identities::ArcId> {
        self.registry
            .link_for_caller(caller.src_nic.as_ref()?)
            .map(LinkId::identities)
    }
}

// ---------------------------------------------------------------------------
// QSPN
// ---------------------------------------------------------------------------

/// A single-arc [`QspnStub`] over a shared [`RpcClient`], with this identity's own stable
/// Neighborhood id embedded in every `CallerContext` so the peer's [`ntk_qspn::ArcResolver`]
/// can resolve it back to *its own* [`LinkId`] — see `crate::node::registry::encode_caller_id`'s
/// doc for why the [`LinkId`] itself is never sent.
struct RpcQspnStub {
    client: Arc<dyn RpcClient>,
    my_id: ntk_neighborhood::NodeId,
}

impl QspnStub for RpcQspnStub {
    fn get_full_etp(
        &self,
        requesting_address: Naddr,
    ) -> BoxFuture<'_, Result<EtpMessage, RpcError>> {
        Box::pin(async move {
            let arg = ntk_qspn::encode_naddr(&requesting_address);
            let payload = self
                .client
                .call(
                    caller_for_id(self.my_id),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::QspnGetFullEtp(arg)),
                    },
                )
                .await?;
            match payload.value {
                Some(RespValue::Typed(tv)) => {
                    ntk_qspn::decode_etp_message(&tv).map_err(|e| malformed(e.to_string()))
                }
                _ => Err(malformed("qspn_get_full_etp: expected a typed ETP reply")),
            }
        })
    }

    fn send_etp(&self, etp: EtpMessage, is_full: bool) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            let args = ntk_proto::v1::QspnSendEtpArgs {
                etp: Some(ntk_qspn::encode_etp_message(&etp)),
                is_full,
            };
            self.client
                .call(
                    caller_for_id(self.my_id),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::QspnSendEtp(args)),
                    },
                )
                .await?;
            Ok(())
        })
    }

    fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            self.client
                .notify(
                    caller_for_id(self.my_id),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::QspnGotPrepareDestroy(ntk_proto::v1::Empty::VALUE)),
                    },
                )
                .await
        })
    }

    fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            self.client
                .notify(
                    caller_for_id(self.my_id),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::QspnGotDestroy(ntk_proto::v1::Empty::VALUE)),
                    },
                )
                .await
        })
    }
}

/// A stub addressing several arcs via `notify` on each, in place of a true reliable broadcast
/// transport: this daemon runs QSPN entirely over per-neighbor TCP connections (see the module
/// doc comment), so "broadcast" here is fan-out over that same set. Never fails outright — a
/// per-arc failure reports through `missing`, matching [`ntk_qspn::MissingArcHandler`]'s role.
struct FanOutQspnStub {
    targets: Vec<(QspnArcId, LazyQspnStub)>,
    missing: Option<Arc<dyn MissingArcHandler>>,
}

impl QspnStub for FanOutQspnStub {
    fn get_full_etp(
        &self,
        _requesting_address: Naddr,
    ) -> BoxFuture<'_, Result<EtpMessage, RpcError>> {
        Box::pin(async move {
            Err(malformed(
                "get_full_etp is never sent over a broadcast stub",
            ))
        })
    }

    fn send_etp(&self, etp: EtpMessage, is_full: bool) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            for (arc, target) in &self.targets {
                if target.send_etp(etp.clone(), is_full).await.is_err()
                    && let Some(missing) = &self.missing
                {
                    missing.missing(*arc);
                }
            }
            Ok(())
        })
    }

    fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            for (_, target) in &self.targets {
                let _ = target.got_prepare_destroy().await;
            }
            Ok(())
        })
    }

    fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            for (_, target) in &self.targets {
                let _ = target.got_destroy().await;
            }
            Ok(())
        })
    }
}

/// How long [`LazyQspnStub`] retries a not-yet-registered arc before conceding it is genuinely
/// unreachable, and how often it polls in between. `ntk_qspn::Manager::handle_add_arc` mints an
/// arc id, replies to the caller, and spawns that arc's first full-ETP fetch *before yielding* —
/// nothing guarantees the caller's own continuation (which is what records this arc's
/// [`LinkId`]/[`RpcClient`] in [`LinkRegistry`]/[`PeerLinks`]) has run by the time that fetch
/// task's stub method is actually invoked. Generous relative to one uncontended scheduling gap,
/// negligible relative to any real round trip.
const LAZY_STUB_RETRY_BUDGET: std::time::Duration = std::time::Duration::from_millis(200);
const LAZY_STUB_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(2);

/// Implements [`QspnStubFactory`] over [`PeerLinks`]/[`LinkRegistry`].
#[derive(Debug)]
pub struct QspnStubFactoryAdapter {
    pub links: Arc<PeerLinks>,
    pub registry: Arc<LinkRegistry>,
    /// This identity's own stable Neighborhood id — see
    /// `crate::node::registry::encode_caller_id`'s doc.
    pub my_id: ntk_neighborhood::NodeId,
}

impl QspnStubFactoryAdapter {
    fn lazy_stub(&self, arc: QspnArcId) -> LazyQspnStub {
        LazyQspnStub {
            registry: self.registry.clone(),
            links: self.links.clone(),
            arc,
            my_id: self.my_id,
        }
    }
}

impl QspnStubFactory for QspnStubFactoryAdapter {
    fn broadcast(
        &self,
        arcs: &[QspnArcId],
        missing: Option<Arc<dyn MissingArcHandler>>,
    ) -> Arc<dyn QspnStub> {
        let targets = arcs.iter().map(|a| (*a, self.lazy_stub(*a))).collect();
        Arc::new(FanOutQspnStub { targets, missing })
    }

    fn tcp(&self, arc: QspnArcId) -> Arc<dyn QspnStub> {
        Arc::new(self.lazy_stub(arc))
    }
}

/// A [`QspnStub`] for one arc, resolved to a real [`RpcQspnStub`] lazily — and, within
/// [`LAZY_STUB_RETRY_BUDGET`], retried — on first use rather than eagerly at construction time.
/// See [`LAZY_STUB_RETRY_BUDGET`]'s doc comment for why an eager, one-shot lookup is wrong: it
/// would permanently discard a genuinely just-established arc as unreachable purely because of
/// scheduling order, not because the arc is actually unreachable.
struct LazyQspnStub {
    registry: Arc<LinkRegistry>,
    links: Arc<PeerLinks>,
    arc: QspnArcId,
    my_id: ntk_neighborhood::NodeId,
}

impl LazyQspnStub {
    async fn resolve(&self) -> Option<RpcQspnStub> {
        let deadline = tokio::time::Instant::now() + LAZY_STUB_RETRY_BUDGET;
        loop {
            if let Some(link) = self.registry.link_of_qspn_arc(self.arc)
                && let Some(client) = self.links.get(link)
            {
                return Some(RpcQspnStub {
                    client,
                    my_id: self.my_id,
                });
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(LAZY_STUB_RETRY_INTERVAL).await;
        }
    }
}

impl QspnStub for LazyQspnStub {
    fn get_full_etp(
        &self,
        requesting_address: Naddr,
    ) -> BoxFuture<'_, Result<EtpMessage, RpcError>> {
        Box::pin(async move {
            match self.resolve().await {
                Some(stub) => stub.get_full_etp(requesting_address).await,
                None => Err(RpcError::ConnectionClosed),
            }
        })
    }

    fn send_etp(&self, etp: EtpMessage, is_full: bool) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            match self.resolve().await {
                Some(stub) => stub.send_etp(etp, is_full).await,
                None => Err(RpcError::ConnectionClosed),
            }
        })
    }

    fn got_prepare_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            match self.resolve().await {
                Some(stub) => stub.got_prepare_destroy().await,
                None => Err(RpcError::ConnectionClosed),
            }
        })
    }

    fn got_destroy(&self) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move {
            match self.resolve().await {
                Some(stub) => stub.got_destroy().await,
                None => Err(RpcError::ConnectionClosed),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Hooking
// ---------------------------------------------------------------------------

/// A [`HookingStub`] over a shared [`RpcClient`] — hand-written since `ntk-hooking` exports no
/// real-transport stub, only `Fake*` doubles (see this module's doc comment).
struct RpcHookingStub {
    client: Arc<dyn RpcClient>,
}

macro_rules! notify_call {
    ($self:ident, $variant:ident, $encoded:expr) => {
        Box::pin(async move {
            $self
                .client
                .notify(
                    empty_caller(),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::$variant($encoded)),
                    },
                )
                .await
        })
    };
}

impl HookingStub for RpcHookingStub {
    fn retrieve_network_data(
        &self,
        ask_coord: bool,
    ) -> BoxFuture<'_, Result<NetworkData, RpcError>> {
        Box::pin(async move {
            let payload = self
                .client
                .call(
                    empty_caller(),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::HookingRetrieveNetworkData(ask_coord)),
                    },
                )
                .await?;
            match payload.value {
                Some(RespValue::Typed(tv)) => {
                    ntk_hooking::decode_network_data(&tv).map_err(|e| malformed(e.to_string()))
                }
                _ => Err(malformed("retrieve_network_data: expected a typed reply")),
            }
        })
    }

    fn search_migration_path(&self, lvl: usize) -> BoxFuture<'_, Result<EntryData, RpcError>> {
        Box::pin(async move {
            let lvl_i32 = i32::try_from(lvl).unwrap_or(i32::MAX);
            let payload = self
                .client
                .call(
                    empty_caller(),
                    unicast_id(),
                    MethodCall {
                        call: Some(Call::HookingSearchMigrationPath(lvl_i32)),
                    },
                )
                .await?;
            let result = match payload.value {
                Some(RespValue::Typed(tv)) => {
                    ntk_hooking::decode_entry_data(&tv).map_err(|e| malformed(e.to_string()))
                }
                _ => Err(malformed("search_migration_path: expected a typed reply")),
            };
            match &result {
                Ok(entry) => tracing::info!(
                    ask_lvl = lvl,
                    target_network_id = entry.network_id,
                    target_pos = ?entry.pos,
                    "migration-instrumentation: search_migration_path resolved"
                ),
                Err(err) => tracing::info!(
                    ask_lvl = lvl,
                    %err,
                    "migration-instrumentation: search_migration_path failed"
                ),
            }
            result
        })
    }

    fn route_search_request(
        &self,
        req: SearchMigrationPathRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteSearchRequest,
            ntk_hooking::encode_search_request(&req)
        )
    }

    fn route_search_error(
        &self,
        pkt: SearchMigrationPathErrorPkt,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteSearchError,
            ntk_hooking::encode_search_error(&pkt)
        )
    }

    fn route_search_response(
        &self,
        resp: SearchMigrationPathResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteSearchResponse,
            ntk_hooking::encode_search_response(&resp)
        )
    }

    fn route_explore_request(
        &self,
        req: ExploreGNodeRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteExploreRequest,
            ntk_hooking::encode_explore_request(&req)
        )
    }

    fn route_explore_response(
        &self,
        resp: ExploreGNodeResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteExploreResponse,
            ntk_hooking::encode_explore_response(&resp)
        )
    }

    fn route_delete_reserve_request(
        &self,
        req: DeleteReservationRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteDeleteReserveRequest,
            ntk_hooking::encode_delete_reserve_request(&req)
        )
    }

    fn route_mig_request(&self, req: RequestPacket) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteMigRequest,
            ntk_hooking::encode_mig_request(&req)
        )
    }

    fn route_mig_response(&self, resp: ResponsePacket) -> BoxFuture<'_, Result<(), RpcError>> {
        notify_call!(
            self,
            HookingRouteMigResponse,
            ntk_hooking::encode_mig_response(&resp)
        )
    }
}

/// Implements [`HookingStubFactory`]: `arc_stub` resolves a specific identity-arc's connection;
/// `gateway_stub` resolves the best next hop toward `hc` — see the module doc's routing model
/// in `crate::node::adapters`.
#[derive(Debug)]
pub struct HookingStubFactoryAdapter {
    pub qspn: ntk_qspn::QspnHandle,
    pub links: Arc<PeerLinks>,
    pub registry: Arc<LinkRegistry>,
}

impl HookingStubFactory for HookingStubFactoryAdapter {
    fn arc_stub(&self, arc: ntk_hooking::ArcId) -> Arc<dyn HookingStub> {
        self.links.get(LinkId(arc.0)).map_or_else(
            || Arc::new(UnreachableHookingStub) as Arc<dyn HookingStub>,
            |client| Arc::new(RpcHookingStub { client }) as Arc<dyn HookingStub>,
        )
    }

    fn gateway_stub(&self, hc: ntk_common::HCoord) -> Option<Arc<dyn HookingStub>> {
        let snapshot = self.qspn.snapshot();
        let entry = snapshot
            .levels
            .get(hc.level)?
            .iter()
            .find(|e| e.destination == hc)?;
        let arc = entry.paths.first()?.arc;
        let link = self.registry.link_of_qspn_arc(arc)?;
        let client = self.links.get(link)?;
        Some(Arc::new(RpcHookingStub { client }))
    }
}

struct UnreachableHookingStub;

impl HookingStub for UnreachableHookingStub {
    fn retrieve_network_data(
        &self,
        _ask_coord: bool,
    ) -> BoxFuture<'_, Result<NetworkData, RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn search_migration_path(&self, _lvl: usize) -> BoxFuture<'_, Result<EntryData, RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_search_request(
        &self,
        _req: SearchMigrationPathRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_search_error(
        &self,
        _pkt: SearchMigrationPathErrorPkt,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_search_response(
        &self,
        _resp: SearchMigrationPathResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_explore_request(
        &self,
        _req: ExploreGNodeRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_explore_response(
        &self,
        _resp: ExploreGNodeResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_delete_reserve_request(
        &self,
        _req: DeleteReservationRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_mig_request(&self, _req: RequestPacket) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
    fn route_mig_response(&self, _resp: ResponsePacket) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async move { Err(RpcError::ConnectionClosed) })
    }
}
