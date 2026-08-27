//! Hand-written glue codegen cannot produce: envelope construction /
//! inspection helpers and a [`ProtocolVersion`] compatibility check.

use crate::v1::envelope::Body;
use crate::v1::response::Outcome;
use crate::v1::{
    Auth, BroadcastAck, BroadcastRequest, CallerContext, Empty, Envelope, MethodCall,
    ProtocolVersion, RemoteError, Request, Response, ResponsePayload, TypedValue,
};
use core::fmt;

impl ProtocolVersion {
    /// The protocol version this build of `ntk-proto` implements.
    pub const CURRENT: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };

    /// Whether a peer announcing `other` can be decoded by this build.
    ///
    /// Protobuf field-number-based evolution means an additive (`minor`)
    /// change on either side is always safely decodable: new oneof arms and
    /// fields are simply unknown to the older peer and are skipped on
    /// decode. Only a `major` mismatch is a hard incompatibility (a
    /// schema-breaking change — a field's number, type, or semantics
    /// changed).
    #[must_use]
    pub fn is_compatible_with(&self, other: &ProtocolVersion) -> bool {
        self.major == other.major
    }
}

/// A peer announced a [`ProtocolVersion`] whose `major` component does not
/// match [`ProtocolVersion::CURRENT`]; the two ends cannot safely talk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionMismatch {
    pub ours: ProtocolVersion,
    pub theirs: ProtocolVersion,
}

impl fmt::Display for VersionMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "incompatible protocol version: ours {}.{}, theirs {}.{}",
            self.ours.major, self.ours.minor, self.theirs.major, self.theirs.minor
        )
    }
}

impl std::error::Error for VersionMismatch {}

impl TypedValue {
    /// Wraps an already-encoded payload with the tag identifying its
    /// concrete producing type. `type_tag` should be stable across versions
    /// of that type (e.g. a fully qualified Rust type name, or a short
    /// registry key owned by the phase-2 crate that defines it) — this
    /// crate never interprets it, only carries it.
    pub fn new(type_tag: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        TypedValue {
            type_tag: type_tag.into(),
            payload: payload.into(),
        }
    }
}

impl Empty {
    /// The single value of this zero-field message, for call sites that
    /// build a [`MethodCall`]/[`ResponsePayload`] for a void method without
    /// writing out an empty-struct literal.
    pub const VALUE: Empty = Empty {};
}

impl Envelope {
    fn with_body(version: ProtocolVersion, body: Body) -> Self {
        Envelope {
            version: Some(version),
            body: Some(body),
            auth: None,
        }
    }

    /// Builds a unicast [`Request`] envelope.
    pub fn request(
        version: ProtocolVersion,
        correlation_id: u64,
        caller: CallerContext,
        unicast_id: TypedValue,
        wait_reply: bool,
        call: MethodCall,
    ) -> Self {
        Self::with_body(
            version,
            Body::Request(Request {
                correlation_id,
                caller: Some(caller),
                unicast_id: Some(unicast_id),
                wait_reply,
                call: Some(call),
            }),
        )
    }

    /// Builds a successful [`Response`] envelope.
    pub fn response_ok(
        version: ProtocolVersion,
        correlation_id: u64,
        payload: ResponsePayload,
    ) -> Self {
        Self::with_body(
            version,
            Body::Response(Response {
                correlation_id,
                outcome: Some(Outcome::Payload(payload)),
            }),
        )
    }

    /// Builds a failed [`Response`] envelope carrying a [`RemoteError`].
    pub fn response_err(version: ProtocolVersion, correlation_id: u64, error: RemoteError) -> Self {
        Self::with_body(
            version,
            Body::Response(Response {
                correlation_id,
                outcome: Some(Outcome::Error(error)),
            }),
        )
    }

    /// Builds a [`BroadcastRequest`] envelope.
    pub fn broadcast_request(
        version: ProtocolVersion,
        packet_id: u64,
        caller: CallerContext,
        broadcast_id: TypedValue,
        send_ack: bool,
        call: MethodCall,
    ) -> Self {
        Self::with_body(
            version,
            Body::BroadcastRequest(BroadcastRequest {
                packet_id,
                caller: Some(caller),
                broadcast_id: Some(broadcast_id),
                send_ack,
                call: Some(call),
            }),
        )
    }

    /// Builds a [`BroadcastAck`] envelope.
    pub fn broadcast_ack(version: ProtocolVersion, packet_id: u64, src_nic: TypedValue) -> Self {
        Self::with_body(
            version,
            Body::BroadcastAck(BroadcastAck {
                packet_id,
                src_nic: Some(src_nic),
            }),
        )
    }

    /// Returns the inner [`Request`], if this envelope carries one.
    #[must_use]
    pub fn as_request(&self) -> Option<&Request> {
        match &self.body {
            Some(Body::Request(r)) => Some(r),
            _ => None,
        }
    }

    /// Returns the inner [`Response`], if this envelope carries one.
    #[must_use]
    pub fn as_response(&self) -> Option<&Response> {
        match &self.body {
            Some(Body::Response(r)) => Some(r),
            _ => None,
        }
    }

    /// Returns the inner [`BroadcastRequest`], if this envelope carries one.
    #[must_use]
    pub fn as_broadcast_request(&self) -> Option<&BroadcastRequest> {
        match &self.body {
            Some(Body::BroadcastRequest(r)) => Some(r),
            _ => None,
        }
    }

    /// Returns the inner [`BroadcastAck`], if this envelope carries one.
    #[must_use]
    pub fn as_broadcast_ack(&self) -> Option<&BroadcastAck> {
        match &self.body {
            Some(Body::BroadcastAck(a)) => Some(a),
            _ => None,
        }
    }

    /// Checks `self.version` (if present) against [`ProtocolVersion::CURRENT`].
    ///
    /// Returns `Ok(())` for a missing version field too: an absent field is
    /// a malformed-envelope concern for the caller's decoder, not a version
    /// mismatch.
    pub fn check_version(&self) -> Result<(), VersionMismatch> {
        match &self.version {
            Some(theirs) if !ProtocolVersion::CURRENT.is_compatible_with(theirs) => {
                Err(VersionMismatch {
                    ours: ProtocolVersion::CURRENT,
                    theirs: *theirs,
                })
            }
            _ => Ok(()),
        }
    }

    /// Attaches sender authentication produced by [`crate::auth::sign`] to
    /// this envelope. Optional by construction: an envelope with no `auth`
    /// call still encodes/decodes exactly as it did before this field
    /// existed (see the `envelope_without_auth_is_wire_compatible` test).
    #[must_use]
    pub fn with_auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Returns this envelope's sender authentication, if any was attached.
    #[must_use]
    pub fn auth(&self) -> Option<&Auth> {
        self.auth.as_ref()
    }
}
