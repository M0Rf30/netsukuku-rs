//! The crate-wide error type.

use crate::arc::ArcId;
use crate::identity::IdentityId;
use crate::migration::MigrationId;

/// Everything that can go wrong operating on this crate's identity
/// registry, identity-arcs, or migration handshake.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown identity {0:?}")]
    UnknownIdentity(IdentityId),

    #[error("identity {0:?} already exists")]
    DuplicateIdentity(IdentityId),

    #[error("the main identity cannot be removed")]
    CannotRemoveMainIdentity,

    #[error("arc {0:?} is not registered")]
    UnknownArc(ArcId),

    #[error("arc {0:?} is already registered")]
    DuplicateArc(ArcId),

    #[error("migration {migration_id:?} for identity {old_id:?} is already pending")]
    DuplicateMigration {
        migration_id: MigrationId,
        old_id: IdentityId,
    },

    #[error("no pending migration {migration_id:?} for identity {old_id:?}")]
    UnknownMigration {
        migration_id: MigrationId,
        old_id: IdentityId,
    },

    /// A decoded wire message was missing a field this implementation
    /// requires.
    #[error("wire message missing required field {0}")]
    MissingField(&'static str),

    /// A decoded wire message had a shape this implementation does not
    /// expect for the call in progress (e.g. a `bool` `ResponsePayload`
    /// where a `TypedValue` was required).
    #[error("unexpected wire response shape: {0}")]
    UnexpectedResponse(&'static str),

    /// A `TypedValue` failed to decode as, or was tagged for, a different
    /// type than expected.
    #[error("wire decode error: {0}")]
    Decode(#[from] ntk_proto::domain::DomainDecodeError),

    #[error("outbound rpc failed: {0}")]
    Rpc(#[from] ntk_rpc::RpcError),

    /// The identity-manager actor task has stopped (e.g. its
    /// [`crate::Handle`] outlived a cancellation).
    #[error("the identity-manager actor is no longer running")]
    ActorGone,
}
