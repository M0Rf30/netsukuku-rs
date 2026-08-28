//! The service-registration surface: [`ServiceId`], [`PeerService`], and the structured
//! refuse/redo-from-start outcomes a service's request handler can raise.

use futures::future::BoxFuture;
use ntk_proto::v1::TypedValue;

use crate::error::Error;

/// A registered service's numeric identifier. RFC 0014 §2, Definition 2.2: "The Netsukuku
/// network can host up to 2^16 different P2P services. Each registered service has a unique
/// identification number called PID." Upstream's own corpus assigns no canonical registry for
/// these ids (`research/notes/02-vala-services-daemon.md` §3, open question 2: "`p_id` has no
/// canonical registry in this corpus") — concrete ids belong to whichever crate registers a
/// service (`ntk-coordinator`, `ntk-andna`), not this one. This type only enforces the RFC's PID
/// space bound; the wire (`PeersSetParticipantArgs.p_id` &c., `ntk-proto/proto/ntk.proto`) still
/// carries it as `int32` for protobuf-native varint encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceId(u16);

impl ServiceId {
    /// Wraps `id` as a [`ServiceId`].
    #[must_use]
    pub fn new(id: u16) -> Self {
        Self(id)
    }

    /// The raw numeric id.
    #[must_use]
    pub fn get(self) -> u16 {
        self.0
    }
}

impl From<ServiceId> for i32 {
    fn from(id: ServiceId) -> Self {
        i32::from(id.0)
    }
}

impl TryFrom<i32> for ServiceId {
    type Error = Error;

    /// # Errors
    /// [`Error::ServiceIdOutOfRange`] if `value` is negative or exceeds `u16::MAX` (RFC 0014
    /// §2, Definition 2.2's PID space).
    fn try_from(value: i32) -> Result<Self, Error> {
        u16::try_from(value)
            .map(Self)
            .map_err(|_| Error::ServiceIdOutOfRange(value.cast_unsigned()))
    }
}

/// A servant's structured refusal to execute a request — the level-scoped exclusion signal from
/// `contact_peer`'s routing state machine (`research/notes/02-vala-services-daemon.md` §3
/// "refuse/redo semantics"; upstream `PeersRefuseExecutionError`,
/// `research/impl/vala/peerservices/peers.vala:57-61`).
///
/// **Deviation from upstream, deliberate**: upstream smuggles the refusal level through
/// formatted exception-message text (`"...level=$(e_lvl)"`,
/// `extract_level_from_refuse_execution_message`,
/// `research/impl/vala/peerservices/message_routing.vala:65-76`) — notes/02 §3 calls this out as
/// a wire-compat wart, not a contract to preserve. `ntk-proto`'s `PeersSetRefuseMessageArgs`
/// already carries `e_lvl` as its own `int32` wire field (`ntk-proto/proto/ntk.proto`), so this
/// type keeps it a first-class struct field end to end instead of re-parsing text out of
/// `message`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// The g-node level that should be excluded from further routing attempts for this request.
    pub level: usize,
    /// A human-readable detail. Upstream additionally distinguishes
    /// `WRITE_OUT_OF_MEMORY`/`READ_NOT_FOUND_NOT_EXHAUSTIVE`/`GENERIC` sub-reasons
    /// (`peers.vala:57-61`) — those are specific to the TTL/fixed-keys distributed-database
    /// algorithms (`databases.vala`), which are out of this crate's scope (Coordinator/ANDNA
    /// non-goals); a concrete [`PeerService`] that needs such a taxonomy encodes it into this
    /// string at its own boundary.
    pub message: String,
}

/// The two ways a [`PeerService`] can decline to answer normally, kept as distinct enum
/// variants rather than upstream's two separate `errordomain`s
/// (`PeersRefuseExecutionError`/`PeersRedoFromStartError`, `peers.vala:57-69`) so callers match
/// exhaustively. A third failure mode, plain call timeout, is **not** a variant here: it is the
/// absence of any message from the peer, not a message the peer sent, so it is represented at
/// the routing layer instead (`crate::routing::ContactPeerError`) via `tokio::time::timeout`,
/// never mixed into this type.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// The servant declines to execute the request for now, excluding the given level from
    /// further routing attempts.
    #[error("refused at level {}: {}", .0.level, .0.message)]
    Refuse(Refusal),
    /// The servant's own address computation is stale (e.g. a network split/merge invalidated
    /// it); the *client* should restart `contact_peer` from scratch
    /// (`PeersRedoFromStartError`, `peers.vala:67-69`).
    #[error("servant requests a full restart of contact_peer")]
    RedoFromStart,
}

/// A distributed service registered on the PeerServices substrate (`PeerService`,
/// `research/impl/vala/peerservices/peers.vala:774-787`). `ntk-coordinator` and `ntk-andna`
/// implement this to run atop the DHT-over-topology substrate this crate provides.
pub trait PeerService: Send + Sync {
    /// This service's registered id.
    fn service_id(&self) -> ServiceId;

    /// Whether this service is optional (not every g-node need participate) or mandatory (every
    /// node participates implicitly). Mirrors `PeerService.p_is_optional`
    /// (`peers.vala:777,405`): registering a mandatory service does not `participate()` it
    /// (mandatory participation is model-wide, not gossiped), while an optional service
    /// auto-participates on registration.
    fn is_optional(&self) -> bool;

    /// Executes `request` on behalf of `client_tuple` (the requesting node's position, scoped to
    /// this request's target level — `PeerService.exec`, `peers.vala:784-786`). `request`/the
    /// success payload are opaque [`TypedValue`]s: this substrate does not interpret a service's
    /// request/response schema, exactly as `ntk_proto::v1::TypedValue` stands in for the ~20
    /// upstream marker interfaces it replaces.
    fn exec<'a>(
        &'a self,
        request: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>>;

    /// Whether this service demands a verified origin signature for `request`, regardless of
    /// [`crate::Config::require_auth`].
    ///
    /// `require_auth` defaults to `false` because the wire `Auth` field is optional upstream:
    /// adding it is compatible, *enforcing* it is not, so a node that enforced globally could
    /// not talk to an unmodified Vala peer. That reasoning is sound for the services Vala
    /// actually implements — but not for one it does not, and not for a request whose whole
    /// security model rests on knowing who asked.
    ///
    /// So a service may opt individual requests in. Granularity is per-request, not per-service,
    /// because a single service legitimately mixes both: ANDNA's records service answers
    /// registrations (which must be attributable) and resolutions (which need not be) behind one
    /// `exec`. Vanilla draws the line in exactly the same place — the C daemon verifies a
    /// signature on a registration (`andna.c:829-841`) and on the counter check
    /// (`andna.c:1181-1191`), and never on a lookup (`andna.c:1604-1609`).
    ///
    /// Defaults to `false`: a service that says nothing keeps today's behaviour exactly.
    fn requires_origin_auth(&self, _request: &TypedValue) -> bool {
        false
    }
}
