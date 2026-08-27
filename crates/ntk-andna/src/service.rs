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
}
