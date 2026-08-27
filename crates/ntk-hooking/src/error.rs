//! Crate-wide error type for [`crate::manager::HookingHandle`]'s own API
//! surface (as opposed to [`crate::coordinator::CoordinatorError`]/
//! [`ntk_rpc::RpcError`], which are the outbound-call error types).

use thiserror::Error;

/// Everything that can go wrong calling into a running Hooking actor.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HookingError {
    /// The Hooking actor task is no longer running (its command channel
    /// closed).
    #[error("hooking actor is no longer running")]
    ActorGone,

    /// `add_arc` called for an [`crate::arc::ArcId`] that is already
    /// registered (`ArcHandler.add_arc`, `arc_handler.vala:62-71`, silently
    /// ignores a duplicate — this crate reports it instead so a caller
    /// notices a composition bug).
    #[error("arc is already registered")]
    ArcAlreadyRegistered,

    /// `remove_arc` called for an [`crate::arc::ArcId`] that is not
    /// registered.
    #[error("arc is not registered")]
    UnknownArc,
}
