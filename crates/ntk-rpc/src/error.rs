//! The local-vs-remote error distinction from
//! research/notes/02-vala-services-daemon.md §1
//! (`research/impl/vala/ntkdrpc/api.vala:23-32`): upstream's `StubError` is
//! local-only and never serialized; `DeserializeError` is the wire-carried
//! call outcome. [`RpcError`] keeps that split visible in the Rust type
//! system: every variant is local except [`RpcError::Remote`].

use std::io;

use ntk_proto::VersionMismatch;
use ntk_proto::v1::RemoteError;

/// Everything that can go wrong issuing or serving an RPC call.
///
/// Only [`RpcError::Remote`] reflects a wire-carried outcome (a peer's
/// `Response.error`, itself the wire form of `DeserializeError` and the
/// eight upstream domain errors). Every other variant is local-only —
/// upstream's `StubError` equivalent — and never crosses the wire.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// Transport-level I/O failure (connect/accept/read/write), including
    /// `tokio_util`'s frame-too-large rejection on decode.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// A frame could not be decoded as a valid `Envelope`.
    #[error("decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    /// A message could not be encoded. Only occurs if a caller-supplied
    /// buffer was undersized; this crate always sizes its own buffers via
    /// `Message::encoded_len`, so in practice this indicates a `prost` bug.
    #[error("encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    /// An encoded envelope exceeded the transport's configured maximum
    /// frame/packet size.
    #[error("frame of {size} bytes exceeds the {max}-byte limit")]
    FrameTooLarge { size: usize, max: usize },

    /// The peer announced an incompatible `ProtocolVersion`
    /// ([`ntk_proto::v1::ProtocolVersion`]).
    #[error(transparent)]
    VersionMismatch(#[from] VersionMismatch),

    /// A decoded `Envelope`/`Request`/`Response` was missing a field this
    /// implementation requires (e.g. `Response.outcome` unset).
    #[error("malformed envelope: {0}")]
    Malformed(String),

    /// No `Response` arrived within the call's deadline.
    #[error("call timed out")]
    Timeout,

    /// The connection closed (locally or by the peer) while a call was
    /// outstanding, or before a `notify` could be sent.
    #[error("connection closed")]
    ConnectionClosed,

    /// The peer's `Response.error` — the one variant that is a wire-carried
    /// outcome rather than a local failure.
    #[error("remote error: {0:?}")]
    Remote(RemoteError),
}

impl RpcError {
    /// True only for [`RpcError::Remote`] (the wire-carried call outcome);
    /// false for every local-only failure mode.
    #[must_use]
    pub fn is_remote(&self) -> bool {
        matches!(self, RpcError::Remote(_))
    }
}
