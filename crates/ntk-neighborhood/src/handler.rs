//! [`NeighborhoodRpcHandler`]: the inbound [`RpcHandler`] for this module's
//! 5 `MethodCall` arms (`here_i_am`/`request_arc`/`can_you_export`/
//! `remove_arc`/`nop`).

use futures::future::BoxFuture;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{Auth, CallerContext, MethodCall, RemoteError, ResponsePayload, TypedValue};
use ntk_rpc::RpcHandler;
use prost::Message;

use crate::manager::Handle;
use crate::node_id::NodeId;
use crate::wire::{self, NicRef};

/// One instance per local listening endpoint.
///
/// `here_i_am`/`request_arc`/`remove_arc` all need to know which of *our
/// own* NICs a broadcast arrived on (upstream resolves this from
/// `CallerInfo`'s `Listener`, `ntkdrpc/caller_info.vala:23-134`); since
/// `ntk_rpc::RpcHandler::handle` carries no transport-level information at
/// all, this crate resolves it structurally instead, by scoping one handler
/// per NIC's broadcast receiver ([`NeighborhoodRpcHandler::for_broadcast`]).
/// `can_you_export`/`nop` never need it (their upstream argument list
/// carries no `NeighborhoodHereIAmArgs`-shaped identity fields to begin
/// with, relying entirely on `CallerContext` — see `crate::wire`'s module
/// doc comment), so [`NeighborhoodRpcHandler::for_unicast`] serves them
/// without a bound device.
#[derive(Debug, Clone)]
pub struct NeighborhoodRpcHandler {
    handle: Handle,
    bound_dev: Option<String>,
}

impl NeighborhoodRpcHandler {
    /// A handler scoped to broadcast traffic received on `dev`.
    #[must_use]
    pub fn for_broadcast(handle: Handle, dev: impl Into<String>) -> Self {
        Self {
            handle,
            bound_dev: Some(dev.into()),
        }
    }

    /// A handler for the shared TCP-unicast listener (`can_you_export`/`nop`).
    #[must_use]
    pub fn for_unicast(handle: Handle) -> Self {
        Self {
            handle,
            bound_dev: None,
        }
    }

    fn received_on_dev(&self) -> Result<String, RemoteError> {
        self.bound_dev
            .clone()
            .ok_or_else(|| wire::malformed("this method requires a broadcast-scoped handler"))
    }
}

fn decode_id(tv: Option<&TypedValue>, field: &str) -> Result<NodeId, RemoteError> {
    let tv = tv.ok_or_else(|| wire::malformed(format!("{field} unset")))?;
    NodeId::from_typed_value(tv).map_err(RemoteError::from)
}

fn caller_identity(caller: &CallerContext) -> Result<(NodeId, NicRef), RemoteError> {
    let source_id = caller
        .source_id
        .as_ref()
        .ok_or_else(|| wire::malformed("CallerContext.source_id unset"))?;
    let src_nic = caller
        .src_nic
        .as_ref()
        .ok_or_else(|| wire::malformed("CallerContext.src_nic unset"))?;
    let id = NodeId::from_typed_value(source_id).map_err(RemoteError::from)?;
    let nic = NicRef::from_typed_value(src_nic).map_err(RemoteError::from)?;
    Ok((id, nic))
}

impl RpcHandler for NeighborhoodRpcHandler {
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        _unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            // The canonical signed payload is the *whole* `MethodCall` (oneof discriminant
            // included), captured before it's unwrapped below — see `wire::sign_call`'s doc
            // for why the sender signs the identical encoding.
            let payload = call.encode_to_vec();
            let call = call
                .call
                .ok_or_else(|| wire::malformed("MethodCall.call unset"))?;
            match call {
                Call::NeighborhoodHereIAm(args) => {
                    let dev = self.received_on_dev()?;
                    let sender_id = decode_id(args.my_id.as_ref(), "here_i_am: my_id")?;
                    let verified =
                        wire::verify_auth(auth.as_ref(), wire::METHOD_HERE_I_AM, &payload)?;
                    self.handle
                        .here_i_am(dev, sender_id, args.my_mac, args.my_nic_addr, verified)
                        .await
                }
                Call::NeighborhoodRequestArc(args) => {
                    let dev = self.received_on_dev()?;
                    let dest_id = decode_id(args.your_id.as_ref(), "request_arc: your_id")?;
                    let sender_id = decode_id(args.my_id.as_ref(), "request_arc: my_id")?;
                    let verified =
                        wire::verify_auth(auth.as_ref(), wire::METHOD_REQUEST_ARC, &payload)?;
                    self.handle
                        .request_arc(
                            dev,
                            dest_id,
                            args.your_mac,
                            args.your_nic_addr,
                            sender_id,
                            args.my_mac,
                            args.my_nic_addr,
                            verified,
                        )
                        .await
                }
                Call::NeighborhoodCanYouExport(peer_can_export) => {
                    let (caller_id, nic) = caller_identity(&caller)?;
                    let verified =
                        wire::verify_auth(auth.as_ref(), wire::METHOD_CAN_YOU_EXPORT, &payload)?;
                    self.handle
                        .can_you_export(caller_id, nic.mac, nic.nic_addr, peer_can_export, verified)
                        .await
                }
                Call::NeighborhoodRemoveArc(args) => {
                    let dev = self.received_on_dev()?;
                    let dest_id = decode_id(args.your_id.as_ref(), "remove_arc: your_id")?;
                    let sender_id = decode_id(args.my_id.as_ref(), "remove_arc: my_id")?;
                    let verified =
                        wire::verify_auth(auth.as_ref(), wire::METHOD_REMOVE_ARC, &payload)?;
                    self.handle
                        .remove_arc(
                            dev,
                            dest_id,
                            args.your_mac,
                            args.your_nic_addr,
                            sender_id,
                            args.my_mac,
                            args.my_nic_addr,
                            verified,
                        )
                        .await
                }
                Call::NeighborhoodNop(_) => {
                    let (caller_id, nic) = caller_identity(&caller)?;
                    let verified = wire::verify_auth(auth.as_ref(), wire::METHOD_NOP, &payload)?;
                    self.handle
                        .nop(caller_id, nic.mac, nic.nic_addr, verified)
                        .await
                }
                other => Err(wire::malformed(format!(
                    "{other:?} is not a neighborhood method"
                ))),
            }
        })
    }
}
