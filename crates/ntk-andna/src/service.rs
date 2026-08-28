//! [`AndnaService`]/[`CounterService`]: the two `PeerService` registrations RFC 0014 describes
//! ANDNA as ("two peer-to-peer services, Andna and Counter",
//! `research/notes/02-vala-services-daemon.md` §4).
//!
//! **Shape note (deviation from the (a)-(f) module template, explained)**: the template's
//! inbound seam (e) is "an `RpcHandler` dispatching this module's `ntk_proto::v1::MethodCall`
//! arms". Upstream's own `ntkdrpc` interface declares **no** `AndnaManager` module at all
//! (`research/notes/02-vala-services-daemon.md` §4) — there are no `andna_*` `MethodCall` arms to
//! dispatch. ANDNA's inbound path instead *is* [`PeerService::exec`]: every request arrives as an
//! opaque `TypedValue` already routed here by `ntk-peerservices`' generic DHT substrate, and
//! `exec` dispatches on the payload's own `type_tag` — this is the direct analogue of an
//! `RpcHandler`, realized as a `PeerService` because that is exactly the substrate RFC 0014
//! prescribes a registered service run on.

use futures::future::BoxFuture;
use ntk_peerservices::{ExecError, PeerService, Refusal, ServiceId};
use ntk_proto::v1::TypedValue;

use crate::actor::{Handle, unix_now};
use crate::wire;

/// RFC 0014 §2, Definition 2.2's PID space has no canonical registry
/// (`research/notes/02-vala-services-daemon.md` §3) — concrete ids belong to whichever crate
/// registers a service. This crate picks two arbitrary, mutually distinct ids.
#[must_use]
pub fn andna_service_id() -> ServiceId {
    ServiceId::new(900)
}

/// See [`andna_service_id`]'s doc comment — the Counter service's own id.
#[must_use]
pub fn counter_service_id() -> ServiceId {
    ServiceId::new(901)
}

/// ANDNA has no notion of "try a different node" for a malformed request — the DHT already
/// routed correctly; a bad payload is a definitive, application-level problem, not something the
/// substrate should route elsewhere. `ExecError` has no third "just reject" variant, so this
/// borrows `Refuse` at level 0 (the narrowest possible exclusion) purely to carry the message
/// back to the caller; `crate::actor::Handle::register`/`resolve` never retry on an `ExecError`
/// from this crate's own services for exactly that reason.
fn malformed(message: impl Into<String>) -> ExecError {
    ExecError::Refuse(Refusal {
        level: 0,
        message: message.into(),
    })
}

/// The `Andna` service: the hostname hash-node/backup role (register/resolve).
pub struct AndnaService {
    handle: Handle,
}

impl std::fmt::Debug for AndnaService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AndnaService").finish_non_exhaustive()
    }
}

impl AndnaService {
    /// Wraps `handle`, the actor this service dispatches every `exec` call onto.
    #[must_use]
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl PeerService for AndnaService {
    fn service_id(&self) -> ServiceId {
        andna_service_id()
    }

    /// Optional: not every g-node need hold ANDNA records, matching upstream's own hash-node/
    /// backup-gnode model (a subset of the network, not every node).
    fn is_optional(&self) -> bool {
        true
    }

    fn exec<'a>(
        &'a self,
        request: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move {
            match request.type_tag.as_str() {
                "andna.RegisterRequest" => {
                    let req = wire::unpack_register_request(&request)
                        .map_err(|e| malformed(e.to_string()))?;
                    let now = unix_now();
                    let outcome = self.handle.handle_register(req, now).await;
                    let reply = match &outcome {
                        Ok(o) => Ok(*o),
                        Err(e) => Err(e),
                    };
                    Ok(wire::pack_register_reply(reply))
                }
                "andna.ResolveRequest" => {
                    let (hostname, service) = wire::unpack_resolve_request(&request)
                        .map_err(|e| malformed(e.to_string()))?;
                    let now = unix_now();
                    let records = self.handle.handle_resolve(hostname, service, now).await;
                    Ok(wire::pack_resolve_reply(&records))
                }
                other => {
                    tracing::warn!(
                        type_tag = other,
                        "ntk-andna: andna service got unknown request type_tag"
                    );
                    Err(malformed(format!(
                        "andna: unknown request type_tag {other:?}"
                    )))
                }
            }
        })
    }

    /// Registration is a write that claims a name, so it must be attributable regardless of
    /// `require_auth`; resolution is a read and stays open. That is exactly where vanilla draws
    /// the line: the C daemon verifies a signature on a registration request
    /// (`research/impl/c/netsukuku/src/andna.c:829-841`, rejecting with `E_INVALID_SIGNATURE`)
    /// and never on a lookup (`andna.c:1604-1609`).
    ///
    /// Enforcing here costs no interoperability, unlike flipping `require_auth` globally: Vala
    /// has no ANDNA to interoperate with at all — `research/impl/vala/andna/andna.vala` is 36
    /// lines, its `serializables.vala` is empty, and `ntkdrpc` carries no ANDNA method — so
    /// there is no unmodified upstream ANDNA peer that this could lock out.
    fn requires_origin_auth(&self, request: &TypedValue) -> bool {
        requires_verified_origin(&request.type_tag)
    }
}

/// The `Counter` service (NTK_RFC 0007): per-registrant hostname-count capping.
pub struct CounterService {
    handle: Handle,
}

impl std::fmt::Debug for CounterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CounterService").finish_non_exhaustive()
    }
}

impl CounterService {
    /// Wraps `handle`, the actor this service dispatches every `exec` call onto.
    #[must_use]
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl PeerService for CounterService {
    fn service_id(&self) -> ServiceId {
        counter_service_id()
    }

    fn is_optional(&self) -> bool {
        true
    }

    fn exec<'a>(
        &'a self,
        request: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move {
            match request.type_tag.as_str() {
                "andna.CounterRequest" => {
                    let hash = wire::unpack_counter_request(&request)
                        .map_err(|e| malformed(e.to_string()))?;
                    let now = unix_now();
                    let outcome = self
                        .handle
                        .handle_counter_reserve(client_tuple.to_vec(), hash, now)
                        .await;
                    let reply = match &outcome {
                        Ok(n) => Ok(*n),
                        Err(e) => Err(e),
                    };
                    Ok(wire::pack_counter_reply(reply))
                }
                other => {
                    tracing::warn!(
                        type_tag = other,
                        "ntk-andna: counter service got unknown request type_tag"
                    );
                    Err(malformed(format!(
                        "counter: unknown request type_tag {other:?}"
                    )))
                }
            }
        })
    }

    /// The anti-Sybil cap (NTK_RFC 0007) is only a cap if the requester cannot be forged: it
    /// keys reservations by `client_tuple`, so an unattributable request makes the whole
    /// mechanism decorative. Vanilla verifies a signature on the counter check too, separately
    /// from the registration itself (`research/impl/c/netsukuku/src/andna.c:1181-1191`), and
    /// keys its own record on the verified key (`counter_c_add(&rfrom, req->pubkey)`,
    /// `andna.c:1235`). See [`AndnaService::requires_origin_auth`] for why enforcing this
    /// regardless of `require_auth` costs no interoperability.
    fn requires_origin_auth(&self, request: &TypedValue) -> bool {
        requires_verified_origin(&request.type_tag)
    }
}

/// The request tags that must be attributable regardless of
/// [`ntk_peerservices::Config::require_auth`].
///
/// One list rather than one per service, so vanilla's line appears in exactly one place: the C
/// daemon verifies a signature on a registration (`research/impl/c/netsukuku/src/andna.c:829-841`,
/// rejecting with `E_INVALID_SIGNATURE`) and again on the counter check (`andna.c:1181-1191`),
/// and never on a lookup (`andna.c:1604-1609`).
fn requires_verified_origin(type_tag: &str) -> bool {
    matches!(type_tag, "andna.RegisterRequest" | "andna.CounterRequest")
}

#[cfg(test)]
mod tests {
    use super::requires_verified_origin;

    /// Pins the port to vanilla's three answers. The distinction matters because
    /// `AndnaService::exec` mixes a write and a read behind one entry point, which is why
    /// `PeerService::requires_origin_auth` is per-request rather than per-service.
    #[test]
    fn only_the_write_requests_demand_a_verified_origin() {
        assert!(
            requires_verified_origin("andna.RegisterRequest"),
            "a registration claims a name and must be attributable"
        );
        assert!(
            requires_verified_origin("andna.CounterRequest"),
            "the anti-Sybil cap keys on client_tuple, so it is only a cap if the origin is real"
        );
        assert!(
            !requires_verified_origin("andna.ResolveRequest"),
            "a lookup must stay open — vanilla does not sign one, and requiring it here would \
             lock out any unauthenticated resolver, including ntkd's own andna-resolve"
        );
    }

    /// An unknown tag must not inherit enforcement: `exec` already rejects it as malformed, and
    /// reporting it as an auth failure instead would misattribute the cause.
    #[test]
    fn an_unknown_request_tag_demands_nothing() {
        assert!(!requires_verified_origin("andna.NotAThing"));
        assert!(!requires_verified_origin(""));
    }
}
