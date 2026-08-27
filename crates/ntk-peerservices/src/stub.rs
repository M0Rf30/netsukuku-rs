//! The outbound-call seam ([`PeersStub`]) and the environment a [`crate::actor::Manager`] needs
//! injected from outside (topology visibility, gateway/neighbor lookup — [`RoutingEnv`]):
//! upstream's `IPeersManagerStub`/`IPeersMapPaths`/`IPeersNeighborsFactory`
//! (`research/impl/vala/peerservices/peers.vala:71-107`), the seam that lets
//! `contact_peer`/`forward_msg` be tested against an in-memory fake instead of real sockets.

use std::any::Any;
use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_common::HCoord;
use ntk_proto::v1::{Auth, TypedValue};

use crate::participation::ParticipantSet;
use crate::service::{Refusal, ServiceId};
use crate::tuple::{TupleGNode, TupleNode};

/// Hop-by-hop routing envelope for `contact_peer`'s recursive forwarding — the domain analogue
/// of the wire `PeerMessageForwarder` message
/// (`research/impl/vala/peerservices/serializables.vala:209-360`).
#[derive(Clone, Debug)]
pub struct PeerMessageForwarder {
    /// The scope (levels 0..inside_level) this whole search is restricted to.
    pub inside_level: usize,
    /// The originator's own address, scoped to `n.top()` (used to route replies back to it).
    pub n: TupleNode,
    /// The originator's target position, scoped to `lvl` levels (absent once routing has
    /// narrowed to level 0 — nothing left to disambiguate below it).
    pub x_macron: Option<TupleNode>,
    /// The level of the next hop's target g-node.
    pub lvl: usize,
    /// The position of the next hop's target g-node at `lvl`.
    pub pos: u32,
    /// Which service this request is for.
    pub p_id: ServiceId,
    /// Correlates asynchronous replies (`get_request`/`set_response`/...) back to this search.
    pub msg_id: i32,
    /// G-nodes to exclude from routing decisions from here on (a `refuse`'s level-scoped
    /// exclusion, propagated forward).
    pub exclude_tuple_list: Vec<TupleGNode>,
    /// G-nodes already known not to participate, propagated forward so downstream hops don't
    /// re-discover the same negative fact.
    pub non_participant_tuple_list: Vec<TupleGNode>,
    /// The true originator's signature over its claimed `n` (client_tuple), `p_id`, and (once
    /// fetched via `get_request`) the request payload — `None` when the originator has no
    /// configured signing key (the vanilla-reference default). Relays forward this opaquely,
    /// verbatim, and never verify it; only the servant does, once, in `Handle::forward_msg`'s
    /// self-loop branch, after fetching the actual request — see `crate::routing::origin_auth`.
    pub auth: Option<Auth>,
}

/// Local, non-wire-carried outcome of an outbound PeersStub call — the client-side analogue of
/// upstream's `StubError`/`DeserializeError`, which `message_routing.vala` only ever branches on
/// as "did the call fail at all", never on a specific variant
/// (`research/impl/vala/peerservices/message_routing.vala:404-413`).
#[derive(Debug, thiserror::Error)]
#[error("peers stub call failed: {0}")]
pub struct StubCallError(pub String);

/// Outcome of an outbound `get_request` call: the two upstream domain errors
/// (`PeersUnknownMessageError`/`PeersInvalidRequest`, `research/impl/vala/peerservices/
/// message_routing.vala:975-997`) kept distinguishable, plus a plain call failure.
#[derive(Debug, thiserror::Error)]
pub enum GetRequestError {
    /// The servant does not recognize `msg_id` (`PeersUnknownMessageError`).
    #[error("unknown msg_id")]
    UnknownMessage,
    /// `respondant` is not scoped to the same g-node of research as the original request
    /// (`PeersInvalidRequest`).
    #[error("respondant not in the same g-node of research")]
    InvalidRequest,
    /// The call itself failed (transport/timeout).
    #[error(transparent)]
    Call(#[from] StubCallError),
}

/// The 12 outbound PeerServices RPC calls — the Rust analogue of the per-(root,medium) stub
/// factories `zcd`'s `rpcdesign` codegen would have produced
/// (`research/notes/02-vala-services-daemon.md` §1). Implemented once against the real
/// transport (`RpcPeersStub` over `ntk_rpc::TcpRpcClient`) and once over
/// `ntk_rpc::FakeRpcClient` for tests, both going through identical wire encoding.
pub trait PeersStub: Send + Sync {
    /// `forward_peer_message` (`interfaces.rpcidl:18`) — fire-and-forget.
    fn forward_peer_message(
        &self,
        msg: PeerMessageForwarder,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `get_request` (`interfaces.rpcidl:19`) — fetches the actual request payload once routing
    /// has reached the servant.
    fn get_request(
        &self,
        msg_id: i32,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<TypedValue, GetRequestError>>;
    /// `set_response` (`interfaces.rpcidl:20`) — fire-and-forget.
    fn set_response(
        &self,
        msg_id: i32,
        response: TypedValue,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_refuse_message` (`interfaces.rpcidl:21`) — fire-and-forget.
    fn set_refuse_message(
        &self,
        msg_id: i32,
        refusal: Refusal,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_redo_from_start` (`interfaces.rpcidl:22`) — fire-and-forget.
    fn set_redo_from_start(
        &self,
        msg_id: i32,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_next_destination` (`interfaces.rpcidl:23`) — fire-and-forget.
    fn set_next_destination(
        &self,
        msg_id: i32,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_failure` (`interfaces.rpcidl:24`) — fire-and-forget.
    fn set_failure(
        &self,
        msg_id: i32,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_non_participant` (`interfaces.rpcidl:25`) — fire-and-forget.
    fn set_non_participant(
        &self,
        msg_id: i32,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_missing_optional_maps` (`interfaces.rpcidl:26`) — fire-and-forget.
    fn set_missing_optional_maps(&self, msg_id: i32) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `set_participant` (`interfaces.rpcidl:27`) — fire-and-forget.
    fn set_participant(
        &self,
        p_id: ServiceId,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `give_participant_maps` (`interfaces.rpcidl:28`) — fire-and-forget.
    fn give_participant_maps(
        &self,
        maps: ParticipantSet,
    ) -> BoxFuture<'_, Result<(), StubCallError>>;
    /// `ask_participant_maps` (`interfaces.rpcidl:29`) — request/response.
    fn ask_participant_maps(&self) -> BoxFuture<'_, Result<ParticipantSet, StubCallError>>;

    /// Downcast support so a [`RoutingEnv`] implementation can recognize a previously-returned
    /// stub it handed back as `failed` and recover whatever adapter-specific routing identity
    /// (arc, link, connection) it was built from — e.g. [`crate::wire::RpcPeersStub`]'s own
    /// `client()` accessor, matched by `Arc::ptr_eq` against a live connection pool. Every
    /// implementor returns `self`; there is no useful default for a trait object.
    fn as_any(&self) -> &dyn Any;
}

/// Everything a [`crate::actor::Manager`] needs from the rest of the daemon: topology
/// visibility and neighbor/gateway lookup. Upstream splits this across `IPeersMapPaths`,
/// `IPeersBackStubFactory`, and `IPeersNeighborsFactory` (`peers.vala:71-107`); those depend on
/// Neighborhood/QSPN/Hooking knowledge this crate does not own, so — exactly like upstream
/// injects them as constructor parameters — a [`RoutingEnv`] implementation is supplied by
/// whichever crate wires a live `Manager` together.
pub trait RoutingEnv: Send + Sync {
    /// True if the g-node named by `hc` is known to exist (`i_peers_exists`, `peers.vala:77`).
    fn gnode_exists(&self, hc: HCoord) -> bool;

    /// The best gateway stub towards `hc`, optionally avoiding `failed` (`i_peers_gateway`,
    /// `peers.vala:78-82`). `None` means routing cannot proceed towards `hc` right now.
    fn gateway(
        &self,
        hc: HCoord,
        failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>>;

    /// A stub reaching node `n` directly, e.g. to deliver a reply back to the originator of a
    /// search (`i_peers_get_tcp_inside`, `peers.vala:92-93`).
    fn dial(&self, n: &TupleNode) -> Option<Arc<dyn PeersStub>>;

    /// How many nodes are known to live in my g-node at `level` — used to size routing timeouts
    /// (`i_peers_get_nodes_in_my_group`, `peers.vala:76`).
    fn nodes_in_my_group(&self, level: usize) -> usize;

    /// Direct-arc neighbors to flood a participation-map/gossip update to. Upstream's
    /// `IPeersNeighborsFactory.i_peers_get_broadcast` additionally threads a missing-arc
    /// callback through Neighborhood's liveness tracking (`peers.vala:96-107`); that tracking is
    /// out of this crate's scope (no `ntk-neighborhood` dependency), so failures here are simply
    /// best-effort-ignored by the caller, matching upstream's own "ignore, just emit a signal"
    /// handling for a failed gossip hop (`map_handler.vala:159-183,283-301`).
    fn neighbors(&self) -> Vec<Arc<dyn PeersStub>>;
}
