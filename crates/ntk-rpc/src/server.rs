//! Server side: [`RpcHandler`] decodes/routes/encodes one call at a time;
//! [`TcpServer`] is the listener task shaped for the actor model — each
//! connection owns its socket, is cancellable via a `CancellationToken`,
//! and shares no mutable state with any other connection
//! (research/notes/06-rust-stack.md §Concurrency).

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use ntk_proto::v1::envelope::Body;
use ntk_proto::v1::{
    Auth, CallerContext, Envelope, ErrorDomain, MethodCall, RemoteError, Request, ResponsePayload,
    TypedValue,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::codec::EnvelopeCodec;

/// Server-side dispatch seam: decodes a [`MethodCall`] and produces its
/// [`ResponsePayload`] outcome, or a [`RemoteError`] — the wire-carried
/// failure channel from `research/notes/02-vala-services-daemon.md` §1.
/// One handler instance, shared via `Arc`, serves every connection a
/// [`TcpServer`] accepts.
pub trait RpcHandler: Send + Sync {
    /// `auth` is the inbound `Envelope`'s optional sender-authentication block
    /// (`ntk_proto::v1::Envelope::auth`), already separated from `Request`/`BroadcastRequest`
    /// by the caller (`dispatch`/`crate::UdpBroadcaster`'s consumers) since `Auth` lives on the
    /// envelope, not inside either body variant. `None` when the peer sent none — this trait
    /// has no opinion on whether that's acceptable; each implementor decides.
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>>;
}

/// Adapts a plain async closure into an [`RpcHandler`], so tests and small
/// services can pass a closure instead of implementing the trait.
pub struct FnHandler<F>(pub F);

impl<F> std::fmt::Debug for FnHandler<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FnHandler").finish_non_exhaustive()
    }
}

impl<F, Fut> RpcHandler for FnHandler<F>
where
    F: Fn(CallerContext, TypedValue, MethodCall, Option<Auth>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ResponsePayload, RemoteError>> + Send + 'static,
{
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin((self.0)(caller, unicast_id, call, auth))
    }
}

fn malformed(message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: ErrorDomain::Deserialize as i32,
        message: message.into(),
    }
}

async fn dispatch(
    handler: &dyn RpcHandler,
    request: Request,
    auth: Option<Auth>,
) -> Result<ResponsePayload, RemoteError> {
    let caller = request
        .caller
        .ok_or_else(|| malformed("Request.caller unset"))?;
    let unicast_id = request
        .unicast_id
        .ok_or_else(|| malformed("Request.unicast_id unset"))?;
    let call = request
        .call
        .ok_or_else(|| malformed("Request.call unset"))?;
    handler.handle(caller, unicast_id, call, auth).await
}

/// A TCP listener dispatching every accepted connection to a shared
/// [`RpcHandler`]. Each connection reads `Envelope`s concurrently —
/// multiple in-flight `Request`s per connection, matched to their
/// `Response` only by `correlation_id`, so handling order does not matter —
/// and writes replies back through one per-connection writer task, so the
/// socket's write half is never touched from more than one task at a time.
#[derive(Debug)]
pub struct TcpServer {
    listener: TcpListener,
    max_frame_length: usize,
}

impl TcpServer {
    /// Binds a listening socket. `max_frame_length` bounds every frame this
    /// server reads or writes.
    pub async fn bind(addr: SocketAddr, max_frame_length: usize) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            max_frame_length,
        })
    }

    /// The bound local address (useful when `addr`'s port was 0).
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections until `cancel` fires, dispatching each to
    /// `handler`. Cancellation propagates to every connection via a child
    /// token; this method returns only once they have all wound down.
    pub async fn serve(self, handler: Arc<dyn RpcHandler>, cancel: CancellationToken) {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let handler = handler.clone();
                            let conn_cancel = cancel.child_token();
                            connections.spawn(serve_connection(stream, self.max_frame_length, handler, conn_cancel));
                        }
                        Err(error) => tracing::warn!(%error, "ntk-rpc: tcp accept failed"),
                    }
                }
            }
        }
        while connections.join_next().await.is_some() {}
    }
}

async fn serve_connection(
    stream: TcpStream,
    max_frame_length: usize,
    handler: Arc<dyn RpcHandler>,
    cancel: CancellationToken,
) {
    let framed = Framed::new(stream, EnvelopeCodec::new(max_frame_length));
    let (mut sink, mut stream) = framed.split();
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Envelope>();

    let writer = tokio::spawn(async move {
        while let Some(envelope) = write_rx.recv().await {
            if sink.send(envelope).await.is_err() {
                break;
            }
        }
    });

    let mut inflight: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Cancellation closes this connection for every module multiplexed over it,
                // not just whichever one prompted the cancel. Logged because a peer sees only
                // an anonymous EOF, so an arc dying for no visible reason is indistinguishable
                // from a network fault without this line.
                tracing::debug!(
                    inflight = inflight.len(),
                    "ntk-rpc: server connection cancelled, closing"
                );
                inflight.abort_all();
                break;
            }
            Some(_) = inflight.join_next(), if !inflight.is_empty() => {}
            frame = stream.next() => {
                match frame {
                    None => {
                        tracing::debug!("ntk-rpc: server connection closed by peer (EOF)");
                        break;
                    }
                    Some(Err(err)) => {
                        // A decode failure kills the whole shared connection, so name it:
                        // every module's calls to this peer die with it.
                        tracing::debug!(error = %err, "ntk-rpc: server connection read error, closing");
                        break;
                    }
                    Some(Ok(envelope)) => {
                        if let Err(mismatch) = envelope.check_version() {
                            tracing::warn!(%mismatch, "ntk-rpc: rejecting envelope with incompatible protocol version");
                            continue;
                        }
                        let Some(version) = envelope.version else { continue };
                        let auth = envelope.auth;
                        // BroadcastRequest/BroadcastAck never arrive on a
                        // stream connection in this design (they are UDP-only,
                        // see `crate::UdpBroadcaster`) and are ignored here.
                        let Some(Body::Request(request)) = envelope.body else { continue };
                        let correlation_id = request.correlation_id;
                        let wait_reply = request.wait_reply;
                        let handler = handler.clone();
                        let write_tx = write_tx.clone();
                        inflight.spawn(async move {
                            let outcome = dispatch(handler.as_ref(), request, auth).await;
                            if wait_reply {
                                let response = match outcome {
                                    Ok(payload) => Envelope::response_ok(version, correlation_id, payload),
                                    Err(error) => Envelope::response_err(version, correlation_id, error),
                                };
                                let _ = write_tx.send(response);
                            }
                        });
                    }
                }
            }
        }
    }
    drop(write_tx);
    let _ = writer.await;
    while inflight.join_next().await.is_some() {}
}
