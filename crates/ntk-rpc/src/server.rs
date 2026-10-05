// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Server side: [`RpcHandler`] decodes/routes/encodes one call at a time;
//! [`TcpServer`] is the listener task shaped for the actor model — each
//! connection owns its socket, is cancellable via a `CancellationToken`,
//! and shares no mutable state with any other connection
//! (research/notes/06-rust-stack.md §Concurrency).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use ntk_proto::v1::envelope::Body;
use ntk_proto::v1::{
    Auth, CallerContext, Envelope, ErrorDomain, MethodCall, ProtocolVersion, RemoteError, Request,
    ResponsePayload, TypedValue,
};
use prost::Message;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
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
    // A peer that never sets `unicast_id` predates the field; the documented compat rule treats
    // that as the default (MainIdentity) id, never as a malformed request.
    let unicast_id = request.unicast_id.unwrap_or_default();
    let call = request
        .call
        .ok_or_else(|| malformed("Request.call unset"))?;
    handler.handle(caller, unicast_id, call, auth).await
}

/// Resource bounds for a [`TcpServer`]. Every peer-reachable queue or task set is capped so a
/// single misbehaving host on the link cannot grow the daemon's memory or task count without
/// limit.
#[derive(Debug, Clone, Copy)]
pub struct ServerLimits {
    /// Maximum simultaneously open connections; further accepts wait for a slot.
    pub max_connections: usize,
    /// Maximum requests being handled concurrently per connection; reading pauses at the cap.
    pub max_inflight: usize,
    /// Capacity of the per-connection reply queue; handlers wait when the peer is not reading.
    pub reply_queue: usize,
    /// A connection with nothing in flight and no frame received for this long is closed.
    pub idle_timeout: Duration,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            max_inflight: 64,
            reply_queue: 64,
            idle_timeout: Duration::from_secs(300),
        }
    }
}

/// Pause after a failed `accept()` so fd exhaustion does not spin the listener.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

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
    limits: ServerLimits,
}

impl TcpServer {
    /// Binds a listening socket. `max_frame_length` bounds every frame this
    /// server reads or writes.
    pub async fn bind(addr: SocketAddr, max_frame_length: usize) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            max_frame_length,
            limits: ServerLimits::default(),
        })
    }

    /// Overrides the default [`ServerLimits`].
    #[must_use]
    pub fn with_limits(mut self, limits: ServerLimits) -> Self {
        self.limits = limits;
        self
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
        let slots = Arc::new(Semaphore::new(self.limits.max_connections.max(1)));
        loop {
            let permit = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => continue,
                permit = slots.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_closed) => break,
                },
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let _ = stream.set_nodelay(true);
                            let handler = handler.clone();
                            let conn_cancel = cancel.child_token();
                            let max_frame_length = self.max_frame_length;
                            let limits = self.limits;
                            connections.spawn(async move {
                                serve_connection(stream, max_frame_length, limits, handler, conn_cancel).await;
                                drop(permit);
                            });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "ntk-rpc: tcp accept failed");
                            tokio::select! {
                                _ = cancel.cancelled() => break,
                                () = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => {}
                            }
                        }
                    }
                }
            }
        }
        while connections.join_next().await.is_some() {}
    }
}

/// Correlation id of a `Request` envelope that wants a reply, if it is one.
fn reply_wanted(envelope: &Envelope) -> Option<u64> {
    match &envelope.body {
        Some(Body::Request(request)) if request.wait_reply => Some(request.correlation_id),
        _ => None,
    }
}

/// Replaces a response too large for `max_frame_length` with a small error response for the
/// same correlation id, so one oversize reply neither kills the writer nor leaves the caller
/// waiting until its timeout.
fn fit_frame(envelope: Envelope, max_frame_length: usize) -> Envelope {
    if envelope.encoded_len() <= max_frame_length {
        return envelope;
    }
    let correlation_id = match &envelope.body {
        Some(Body::Response(response)) => response.correlation_id,
        _ => return envelope,
    };
    tracing::warn!(
        size = envelope.encoded_len(),
        max = max_frame_length,
        "ntk-rpc: response exceeds the frame limit, replying with an error instead"
    );
    Envelope::response_err(
        ProtocolVersion::CURRENT,
        correlation_id,
        RemoteError {
            domain: ErrorDomain::Deserialize as i32,
            message: "response exceeds the maximum frame size".to_owned(),
        },
    )
}

async fn serve_connection(
    stream: TcpStream,
    max_frame_length: usize,
    limits: ServerLimits,
    handler: Arc<dyn RpcHandler>,
    cancel: CancellationToken,
) {
    let framed = Framed::new(stream, EnvelopeCodec::new(max_frame_length));
    let (mut sink, mut stream) = framed.split();
    let (write_tx, mut write_rx) = mpsc::channel::<Envelope>(limits.reply_queue.max(1));
    let writer_dead = CancellationToken::new();

    let writer = tokio::spawn({
        let writer_dead = writer_dead.clone();
        async move {
            while let Some(envelope) = write_rx.recv().await {
                if sink
                    .send(fit_frame(envelope, max_frame_length))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            // A dead writer means no reply can ever reach the peer: end the connection
            // rather than leaving it half-open.
            writer_dead.cancel();
        }
    });

    let max_inflight = limits.max_inflight.max(1);
    let mut inflight: JoinSet<()> = JoinSet::new();
    let idle = tokio::time::sleep(limits.idle_timeout);
    tokio::pin!(idle);
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
            _ = writer_dead.cancelled() => {
                tracing::debug!("ntk-rpc: server connection writer failed, closing");
                inflight.abort_all();
                break;
            }
            Some(joined) = inflight.join_next(), if !inflight.is_empty() => {
                if let Err(error) = joined
                    && error.is_panic()
                {
                    tracing::error!("ntk-rpc: request handler panicked");
                }
                idle.as_mut().reset(tokio::time::Instant::now() + limits.idle_timeout);
            }
            () = &mut idle, if inflight.is_empty() => {
                tracing::debug!("ntk-rpc: server connection idle, closing");
                break;
            }
            frame = stream.next(), if inflight.len() < max_inflight => {
                idle.as_mut().reset(tokio::time::Instant::now() + limits.idle_timeout);
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
                        let mismatch = envelope.check_version().err();
                        let version = envelope.version;
                        let Some(version) = version.filter(|_| mismatch.is_none()) else {
                            match &mismatch {
                                Some(mismatch) => tracing::warn!(%mismatch, "ntk-rpc: rejecting envelope with incompatible protocol version"),
                                None => tracing::warn!("ntk-rpc: rejecting envelope without a protocol version"),
                            }
                            // Answer with our own version so the caller sees a diagnosable
                            // error instead of waiting out its timeout.
                            if let Some(correlation_id) = reply_wanted(&envelope) {
                                let response = Envelope::response_err(
                                    ProtocolVersion::CURRENT,
                                    correlation_id,
                                    malformed("incompatible or missing protocol version"),
                                );
                                if write_tx.send(response).await.is_err() {
                                    break;
                                }
                            }
                            continue;
                        };
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
                                let _ = write_tx.send(response).await;
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
