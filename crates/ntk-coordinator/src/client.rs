//! [`CoordinatorClient`]: the DHT-routed proxy to whichever node is currently elected servant
//! for a level (`CoordClient`, `research/impl/vala/coordinator/peer_service.vala:115-314`) —
//! the client half `ntk-hooking`'s own `trait CoordinatorClient` is implemented against in
//! `ntkd` (phase 4). [`crate::Handle`] is the *servant*-side counterpart run by whichever node
//! the DHT elects.

use ntk_common::Topology;
use ntk_peerservices::{ContactPeerError, ServiceId, TupleGNode, TupleNode};
use ntk_proto::v1::TypedValue;
use thiserror::Error;

use crate::domain::Reservation;
use crate::error::Error as WireError;
use crate::service::SERVICE_ID;
use crate::wire::{RequestBody, ResponseBody, pack_request, unpack_response};

/// Everything that can go wrong proxying a Coordinator operation to its elected servant.
#[derive(Debug, Error)]
pub enum ProxyError {
    /// `top` (a `CoordinatorKey` level) is not `1..=topology.levels()`.
    #[error("top {top} is out of range for a topology with {levels} levels")]
    InvalidTop { top: usize, levels: usize },
    /// Routing to the elected servant failed entirely (`ProxyError.GENERIC`,
    /// `research/impl/vala/coordinator/peer_service.vala:209-214`).
    #[error("routing to the elected coordinator failed: {0}")]
    Routing(#[from] ContactPeerError),
    /// The servant's response did not decode as the expected type — "This should happen when
    /// another node is malicious or bugged" (`peer_service.vala:182-184` and its five repeats).
    #[error("malformed response from the elected coordinator: {0}")]
    MalformedResponse(#[from] WireError),
    /// The servant answered a different (well-formed) response kind than the one this call
    /// expected.
    #[error("unexpected response kind from the elected coordinator")]
    UnexpectedResponseKind,
}

/// DHT-routed proxy to the Coordinator elected for each level of this topology
/// (`CoordinatorManager`'s public facade over `CoordClient`,
/// `research/impl/vala/coordinator/coord.vala:153-225`). Cheap to construct; holds only a
/// cloned `ntk_peerservices::Handle` plus this crate's own [`crate::Config`] (for its DHT
/// round-trip timeouts).
#[derive(Clone, Debug)]
pub struct CoordinatorClient {
    peers: ntk_peerservices::Handle,
    service_id: ServiceId,
    config: crate::config::Config,
}

impl CoordinatorClient {
    /// Wraps `peers` (this node's PeerServices [`ntk_peerservices::Handle`]) to talk to whichever
    /// node it elects as Coordinator.
    #[must_use]
    pub fn new(peers: ntk_peerservices::Handle, config: crate::config::Config) -> Self {
        Self {
            peers,
            service_id: ServiceId::new(SERVICE_ID),
            config,
        }
    }
    fn topology(&self) -> &Topology {
        self.peers.topology()
    }

    /// The DHT target for `CoordinatorKey(top)`: `perfect_tuple(k) = [0,0,...,0]` (`top` zeros)
    /// — position 0 (the eldest node) inside the g-node at level `top`
    /// (`research/notes/01-vala-core-routing.md` §7; `coordinator/peer_service.vala:158-166`).
    /// Implemented **exactly** as upstream: this is a DHT-hash-based election, not an invented
    /// leader-election algorithm.
    fn target_for(&self, top: usize) -> Result<TupleNode, ProxyError> {
        let levels = self.topology().levels();
        if top < 1 || top > levels {
            return Err(ProxyError::InvalidTop { top, levels });
        }
        let target = TupleNode::new(self.topology().clone(), vec![0u32; top])
            .expect("top in 1..=levels is always a valid TupleNode span");
        tracing::debug!(top, ?target, "TRACE target_for: elect-key");
        Ok(target)
    }

    /// Own-network round trip: `exclude` seeds [`ntk_peerservices::Handle::contact_peer`]'s own
    /// `seed_exclude_tuple_list` with every g-node this node already knows is foreign to its own
    /// network (`CoordinatorClientAdapter::foreign_exclusions`,
    /// `ntk_hooking::QspnView::note_foreign`/`note_same_network`). Used by every method that
    /// asks about *this node's own* network — [`Self::get_n_nodes`], [`Self::reserve`],
    /// [`Self::delete_reserve`], [`Self::hooking_memory`], [`Self::set_hooking_memory`] — see
    /// [`Self::reserve`]'s own doc for why `reserve`/`delete_reserve` belong here rather than
    /// with [`Self::call_entering`]. Without `exclude`, [`Self::target_for`]'s elect-key
    /// (`[0,0,...,0]`, matched by raw position alone, with no notion of network identity) could
    /// resolve to a *physically reachable but logically foreign* node that merely happens to
    /// claim that same numeric position, instead of this node's own real Coordinator.
    async fn call(
        &self,
        top: usize,
        request: RequestBody,
        timeout: std::time::Duration,
        exclude: &[TupleGNode],
    ) -> Result<ResponseBody, ProxyError> {
        let target = self.target_for(top)?;
        let (response, _respondant) = self
            .peers
            .contact_peer(
                self.service_id,
                Some(target),
                pack_request(&request),
                timeout,
                None,
                exclude.to_vec(),
            )
            .await?;
        Ok(unpack_response(&response)?)
    }

    /// Entering-a-host-network round trip: excludes *this node's own* g-node
    /// (`ntk_peerservices::Handle::contact_peer`'s `exclude_my_gnode`, levels `0..top-1`, plus
    /// this node itself unconditionally — see `all_gnodes_up_to_lvl`'s own doc) rather than any
    /// foreign one. Used by every method that, by construction, always asks a *different*
    /// (candidate host) network's Coordinator — `evaluate_enter`, `begin_enter`,
    /// `completed_enter`, `abort_enter`: every real caller (`ntk_hooking::arc::run_arc_handler`)
    /// invokes these directly, only while itself negotiating entry into a candidate neighbor,
    /// never about this node's own network. This is *not* [`Self::reserve`]/
    /// [`Self::delete_reserve`]'s own role even though those two are also part of the entry
    /// protocol — see that method's own doc for why they stay on [`Self::call`] instead.
    ///
    /// This replaces an earlier, too-blunt fix that instead excluded *every currently-known-
    /// foreign* g-node here (via [`Self::call`]'s own `exclude`) for all six entry-protocol
    /// methods including `reserve`/`delete_reserve`: that closed the real self-loop (confirmed
    /// live in `crates/ntkd/tests/mesh.rs`'s
    /// `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` — the entering g-node's own
    /// first negotiator shared address `[0, 0]` with the target network's own elect-key, so its
    /// `evaluate_enter` calls answered *themselves*) but also excluded the very host network
    /// `evaluate_enter`/`begin_enter`/`completed_enter`/`abort_enter` exist to reach, blocking
    /// every other, non-coincidentally-addressed member from ever completing its own
    /// negotiation — and, for `reserve`/`delete_reserve` specifically, was never the right tool
    /// at all: those two run as the *servant* granting a slot from its own network, which must
    /// stay reachable via `exclude`'s original foreign-only semantics, never self-excluded.
    /// Excluding only this node's own g-node here fixes the self-loop without blocking
    /// legitimate contact with a genuinely foreign host: [`target_for`]'s elect-key names *some*
    /// position-0 g-node reachable in this node's own map, and once every candidate inside this
    /// node's own hierarchy is excluded, the only candidates left are foreign ones — exactly the
    /// host these four calls exist to reach.
    ///
    /// `CoordinatorKey(0)` is **not** expressible and must not be attempted: `is_valid_key`
    /// (`fk_database.vala:47-55`, mirrored in [`crate::actor`]'s `reserve_enter`) accepts only
    /// `1..=levels`, so a servant reached with `top == 0` answers
    /// `top 0 is out of range for a topology with N levels`. Routing `top == 0` to `x_macron =
    /// None` ("route to myself") was tried and reverted: it delivers the request but the servant
    /// rejects it, and it regressed the single-level
    /// `discovering_a_peer_joins_and_adopts_the_negotiated_position`. Callers clamp instead —
    /// see `CoordinatorClientAdapter::begin_enter` in `ntkd`.
    async fn call_entering(
        &self,
        top: usize,
        request: RequestBody,
        timeout: std::time::Duration,
    ) -> Result<ResponseBody, ProxyError> {
        let target = self.target_for(top)?;
        // `Some(0)`, NOT `Some(top - 1)`. `all_gnodes_up_to_lvl` excludes every g-node that is
        // *not* mine below the level it is given, and `ntk_peerservices::tuple::approximate`
        // independently skips every g-node that *is* mine — so any `lvl >= 1` excludes the entire
        // searchable space below it, the prospective host included, and `contact_peer` fails
        // `NoParticipants`. `Some(0)` makes that helper return exactly `[HCoord(0, my_pos[0])]`,
        // which is the "do not self-answer" suppression this call actually wants, and is what
        // `Some(top - 1)` already degenerated to for a single-level topology — the only shape the
        // negotiation tests covered, which is why the overshoot read as correct. Verified: this
        // alone takes a multi-level guest's `evaluate_enter` from `NoParticipants` to success.
        let exclude_my_gnode = Some(0);
        let (response, _respondant) = self
            .peers
            .contact_peer(
                self.service_id,
                Some(target),
                pack_request(&request),
                timeout,
                exclude_my_gnode,
                Vec::new(),
            )
            .await?;
        Ok(unpack_response(&response)?)
    }

    /// `get_n_nodes` (`peer_service.vala:173-207`): always targets the whole network
    /// (`CoordinatorKey(levels)`), i.e. *this node's own* network — see `call`'s own doc
    /// for `exclude`.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn get_n_nodes(&self, exclude: &[TupleGNode]) -> Result<u64, ProxyError> {
        let top = self.topology().levels();
        match self
            .call(
                top,
                RequestBody::NumberOfNodes,
                self.config.write_timeout,
                exclude,
            )
            .await?
        {
            ResponseBody::NumberOfNodes(n) => Ok(n),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `evaluate_enter` (`peer_service.vala:216-239`) — always targets a candidate host network;
    /// see `call_entering`'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn evaluate_enter(
        &self,
        top: usize,
        data: TypedValue,
    ) -> Result<TypedValue, ProxyError> {
        match self
            .call_entering(
                top,
                RequestBody::EvaluateEnter { top, data },
                self.config.hooking_timeout,
            )
            .await?
        {
            ResponseBody::EvaluateEnter(data) => Ok(data),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `begin_enter` (`peer_service.vala:241-264`) — see `call_entering`'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn begin_enter(
        &self,
        top: usize,
        data: TypedValue,
    ) -> Result<TypedValue, ProxyError> {
        match self
            .call_entering(
                top,
                RequestBody::BeginEnter { top, data },
                self.config.hooking_timeout,
            )
            .await?
        {
            ResponseBody::BeginEnter(data) => Ok(data),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `completed_enter` (`peer_service.vala:266-289`) — see `call_entering`'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn completed_enter(
        &self,
        top: usize,
        data: TypedValue,
    ) -> Result<TypedValue, ProxyError> {
        match self
            .call_entering(
                top,
                RequestBody::CompletedEnter { top, data },
                self.config.hooking_timeout,
            )
            .await?
        {
            ResponseBody::CompletedEnter(data) => Ok(data),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `abort_enter` (`peer_service.vala:291-314`) — see `call_entering`'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn abort_enter(
        &self,
        top: usize,
        data: TypedValue,
    ) -> Result<TypedValue, ProxyError> {
        match self
            .call_entering(
                top,
                RequestBody::AbortEnter { top, data },
                self.config.hooking_timeout,
            )
            .await?
        {
            ResponseBody::AbortEnter(data) => Ok(data),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `reserve` (`coord.vala:201-211`): idempotent by `reserve_request_id`. `None` mirrors
    /// upstream's `ReserveEnterErrorResponse` — a normal "cannot reserve here right now" answer.
    ///
    /// Unlike [`Self::evaluate_enter`]/[`Self::begin_enter`]/etc., this targets *this node's
    /// own* network, not a candidate host: every real caller
    /// (`ntk_hooking::search::execute_search`, reached only via `ntk_hooking::rpc`'s
    /// `search_migration_path` server handler and `ntk_hooking::routing`'s hop-forwarding, never
    /// directly from `ntk_hooking::arc`) runs as the *servant* granting a slot from its own
    /// hierarchy to whichever guest asked — the guest itself never calls this. See `call`'s
    /// own doc for `exclude`.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn reserve(
        &self,
        top: usize,
        reserve_request_id: i64,
        exclude: &[TupleGNode],
    ) -> Result<Option<Reservation>, ProxyError> {
        match self
            .call(
                top,
                RequestBody::ReserveEnter {
                    top,
                    reserve_request_id,
                },
                self.config.write_timeout,
                exclude,
            )
            .await?
        {
            ResponseBody::ReserveEnter(r) => Ok(r),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `delete_reserve` (`coord.vala:213-217`) — see [`Self::reserve`]'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn delete_reserve(
        &self,
        top: usize,
        reserve_request_id: i64,
        exclude: &[TupleGNode],
    ) -> Result<(), ProxyError> {
        self.call(
            top,
            RequestBody::DeleteReserveEnter {
                top,
                reserve_request_id,
            },
            self.config.write_timeout,
            exclude,
        )
        .await?;
        Ok(())
    }

    /// `get_hooking_memory` (`coord.vala:181-188`) — targets *this node's own* network; see
    /// `call`'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn hooking_memory(
        &self,
        top: usize,
        exclude: &[TupleGNode],
    ) -> Result<Option<TypedValue>, ProxyError> {
        match self
            .call(
                top,
                RequestBody::GetHookingMemory { top },
                self.config.read_timeout,
                exclude,
            )
            .await?
        {
            ResponseBody::GetHookingMemory(data) => Ok(data),
            _ => Err(ProxyError::UnexpectedResponseKind),
        }
    }

    /// `set_hooking_memory` (`coord.vala:190-197`) — see [`Self::hooking_memory`]'s own doc.
    ///
    /// # Errors
    /// See [`ProxyError`].
    pub async fn set_hooking_memory(
        &self,
        top: usize,
        data: Option<TypedValue>,
        exclude: &[TupleGNode],
    ) -> Result<(), ProxyError> {
        self.call(
            top,
            RequestBody::SetHookingMemory { top, data },
            self.config.write_timeout,
            exclude,
        )
        .await?;
        Ok(())
    }
}
