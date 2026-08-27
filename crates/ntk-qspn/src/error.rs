//! Crate-wide error type.

use thiserror::Error;

/// Everything that can go wrong inside the QSPN actor or its pure helpers.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum QspnError {
    /// `AcyclicError` (`research/impl/vala/qspn/qspn.vala:57-59`): the ETP's
    /// own hop list has looped back through this node's own g-node position.
    /// (A single looping *path* inside an otherwise-fine message is instead
    /// silently dropped, `qspn.vala:1132-1153` — not surfaced as an error.)
    #[error("ETP looped back through my own g-node")]
    Acyclic,

    /// An ETP claimed to originate from this node's own address —
    /// `check_incoming_message`'s "MUST NOT be the same as mine" guard
    /// (`research/impl/vala/qspn/etp_message.vala:127-139`).
    #[error("ETP claims to originate from my own address")]
    EtpFromSelf,

    /// `check_incoming_message`/`check_outgoing_message`/`check_any_message`
    /// rejected a malformed ETP (`research/impl/vala/qspn/etp_message.vala:125-191`).
    #[error("malformed ETP: {0}")]
    MalformedEtp(&'static str),

    /// `QspnBootstrapInProgressError` (`research/impl/vala/ntkdrpc/interfaces.vala`,
    /// wire `ErrorDomain::QspnBootstrapInProgress`): the caller asked this
    /// identity for something it cannot yet answer because it has not
    /// finished hooking at that level (`get_full_etp`,
    /// `is_known_destination`/`get_paths_to`/`get_fingerprint`/
    /// `get_nodes_inside`, `qspn.vala:2122-2123,2136-2137,2154-2155,
    /// 2187-2188,2197-2198,2545`).
    #[error("still in bootstrap phase")]
    BootstrapInProgress,

    /// `QspnNotAcceptedError` (`research/impl/vala/ntkdrpc/interfaces.vala`,
    /// notes/01 §2): the RPC caller did not resolve to one of this node's
    /// known arcs within `QspnConfig::arc_timeout`.
    #[error("caller is not a known arc")]
    NotAnArc,

    /// The referenced [`crate::ArcId`] is not (or is no longer) one of this
    /// node's arcs.
    #[error("unknown arc")]
    UnknownArc,

    /// An [`ntk_common`] invariant that validated ETP input is expected to
    /// always uphold did not hold (e.g. `Fingerprint::elder_seed` called on
    /// two paths to the same destination aggregated to different levels).
    /// Reported rather than panicking, matching `ntk_common::Error`'s own
    /// choice for the equivalent upstream `assert_not_reached()` sites.
    #[error("protocol invariant violated: {0}")]
    Common(#[from] ntk_common::Error),

    /// Wire decoding rejected a `qspn.EtpMessage`/`qspn.EtpPath` — either
    /// `ntk-proto`'s shared domain codec rejected an embedded
    /// `Naddr`/`HCoord`/`Fingerprint`/`Cost`, or the `TypedValue` carrying it
    /// had the wrong `type_tag`.
    #[error("wire decode error: {0}")]
    Domain(#[from] ntk_proto::domain::DomainDecodeError),

    /// The QSPN actor task is no longer running (its command channel closed).
    #[error("qspn actor is no longer running")]
    ActorGone,
}
