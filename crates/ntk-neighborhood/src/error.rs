//! [`NeighborhoodError`]: this crate's single error type, covering wire
//! decode failures, local API misuse, and the local-only failures a caller
//! can hit driving [`crate::Handle`] — never a wire-carried outcome (that
//! is [`ntk_proto::v1::RemoteError`], produced only inside
//! [`crate::NeighborhoodRpcHandler`]).

use ntk_proto::domain::DomainDecodeError;
use ntk_proto::v1::{ErrorDomain, RemoteError};

/// Everything that can go wrong in this crate outside of a wire-carried RPC
/// outcome.
#[derive(Debug, thiserror::Error)]
pub enum NeighborhoodError {
    /// [`crate::Handle::start_monitor`] named an interface
    /// [`ntk_netlink::TopologyQuery::list_links`] does not know about.
    #[error("no such local interface: {0}")]
    UnknownInterface(String),

    /// [`crate::Handle::start_monitor`] named an interface that exists but
    /// is administratively down (`IFF_UP` unset) — upstream's
    /// `start_monitor` (`neighborhood.vala:106-134`) has no equivalent
    /// check since the caller was trusted to hand over a live NIC; this
    /// crate performs interface discovery itself (constraint: "which local
    /// interfaces participate" via `ntk-netlink`), so it enforces the
    /// invariant upstream's caller used to guarantee.
    #[error("local interface {0} is administratively down")]
    InterfaceDown(String),

    /// [`crate::Handle::start_monitor`] was called twice for the same
    /// `dev` without an intervening `stop_monitor`
    /// (`neighborhood.vala:110-116`'s `assert(present.dev != dev)`).
    #[error("local interface {0} is already monitored")]
    AlreadyMonitored(String),

    /// A netlink query failed.
    #[error(transparent)]
    Netlink(#[from] ntk_netlink::NetlinkError),

    /// A `TypedValue` payload was not this crate's own wire type, was
    /// tagged for it but failed to decode, or a required field was unset.
    #[error(transparent)]
    Wire(#[from] DomainDecodeError),

    /// A decoded wire value violated this crate's own domain invariant
    /// (e.g. a non-positive [`crate::NodeId`], an empty MAC/address in a
    /// [`crate::NicRef`]) — distinct from [`NeighborhoodError::Wire`]
    /// because the bytes decoded fine, the *value* is what upstream would
    /// never have produced.
    #[error("malformed neighborhood payload: {0}")]
    MalformedWire(String),

    /// The actor task behind a [`crate::Handle`] has stopped (its `mpsc`
    /// receiver, or the matching `oneshot` sender, was dropped).
    #[error("neighborhood manager actor is no longer running")]
    ActorGone,
}

impl From<NeighborhoodError> for RemoteError {
    /// Every [`NeighborhoodError`] that can occur while producing a
    /// [`crate::NeighborhoodRpcHandler`] response is a malformed-request
    /// condition from the peer's point of view — mirrors
    /// `research/impl/vala/ntkdrpc/api.vala`'s "an error domain the
    /// receiver doesn't recognize at all becomes `DeserializeError`" rule
    /// (`research/notes/02-vala-services-daemon.md` §1), reused here for
    /// every local decode/validation failure this crate can hit while
    /// serving a call.
    fn from(error: NeighborhoodError) -> Self {
        RemoteError {
            domain: ErrorDomain::Deserialize as i32,
            message: error.to_string(),
        }
    }
}
