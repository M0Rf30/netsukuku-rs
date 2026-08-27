//! [`RpcClient`]: the async, object-safe substitutability seam that
//! upstream's `IQspnStubFactory`-style per-(root,medium) stub factories
//! played (research/notes/06-rust-stack.md §"Where Rust traits replace...").
//! [`TcpRpcClient`] is the real transport implementation; the in-memory fake
//! lives in [`crate::fake`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use ntk_proto::v1::envelope::Body;
use ntk_proto::v1::response::Outcome;
use ntk_proto::v1::{
    Auth, CallerContext, Envelope, MethodCall, ProtocolVersion, Response, ResponsePayload,
    TypedValue,
};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::codec::EnvelopeCodec;
use crate::error::RpcError;

/// Async, object-safe seam for issuing RPC calls — implemented once for the
/// real transport ([`TcpRpcClient`]) and once for an in-memory fake
/// ([`crate::FakeRpcClient`]) used in tests/simulation.
///
/// Hand-written as boxed futures (rather than `async fn`) so the trait
/// stays object-safe (`Arc<dyn RpcClient>`) without an `async-trait`-style
/// macro dependency.
pub trait RpcClient: Send + Sync {
    /// Issues a unicast call and waits for its outcome (`wait_reply = true`
    /// on the wire). Resolves to [`RpcError::Remote`] iff the peer replied
    /// with an error; every other `Err` variant is local.
    fn call<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>>;

    /// Issues a fire-and-forget call (`wait_reply = false` on the wire).
    /// Never awaits a `Response` — mirrors how upstream's `wait_reply =
    /// false` makes any return value unobservable to the caller
    /// (`ntkdrpc/api.vala:23-27`).
    fn notify<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<(), RpcError>>;

    /// [`Self::call`], plus an optional `ntk_proto::v1::Auth` block attached to the outbound
    /// `Envelope` (`ntk_proto::v1::Envelope::with_auth`) — `ntk-neighborhood`'s hop-auth signing
    /// seam. Defaults to ignoring `auth` and forwarding to [`Self::call`] so every existing
    /// implementor (qspn/hooking/identities' stubs, every module's `Fake*`) keeps compiling
    /// unchanged; only transports that actually attach `Auth` to the wire
    /// ([`TcpRpcClient`], [`crate::FakeRpcClient`]) override it.
    fn call_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
        let _ = auth;
        self.call(caller, unicast_id, call)
    }

    /// [`Self::notify`]'s authenticated counterpart — see [`Self::call_authenticated`]'s doc.
    fn notify_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        let _ = auth;
        self.notify(caller, unicast_id, call)
    }
}

enum ClientCmd {
    Call {
        envelope: Envelope,
        reply: oneshot::Sender<Result<Response, RpcError>>,
    },
    Notify {
        envelope: Envelope,
        reply: oneshot::Sender<Result<(), RpcError>>,
    },
}

/// Real [`RpcClient`] transport: one TCP connection, multiplexing
/// concurrent calls by `Request.correlation_id`.
///
/// Internally a single-owner actor task holds the connection halves and the
/// pending-call table — deliberately not an `Arc<Mutex<_>>` over that state
/// (research/notes/06-rust-stack.md §Concurrency). Public methods send a
/// command to the actor over an `mpsc` channel and await a `oneshot` reply.
#[derive(Debug)]
pub struct TcpRpcClient {
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    next_id: AtomicU64,
    call_timeout: Duration,
}

impl TcpRpcClient {
    /// Connects to `addr` and spawns the connection-owning actor task.
    /// `max_frame_length` bounds every frame in both directions;
    /// `call_timeout` bounds how long [`RpcClient::call`] waits for a
    /// `Response`. Equivalent to [`Self::connect_via`] with `device: None`.
    pub async fn connect(
        addr: SocketAddr,
        max_frame_length: usize,
        call_timeout: Duration,
    ) -> Result<Self, RpcError> {
        Self::connect_via(addr, None, max_frame_length, call_timeout).await
    }

    /// Connects to `addr`, first restricting the outbound socket to `device` via Linux's
    /// `SO_BINDTODEVICE` when given — the TCP-client counterpart of
    /// [`crate::UdpBroadcaster::bind`]'s existing per-NIC binding, and for the same reason:
    /// `169.254.0.0/16` link-local addresses (RFC 3927) are per-link by definition, so with 2+
    /// monitored NICs sharing that one prefix the kernel's normal route lookup cannot
    /// disambiguate an outbound dial by destination address alone — whichever route was added
    /// first wins, regardless of which NIC the peer actually lives on. Binding the *socket* to
    /// the already-known-correct egress NIC sidesteps that ambiguity instead of depending on
    /// route order. Requires `CAP_NET_RAW`, same as `UdpBroadcaster::bind`; `device: None`
    /// behaves exactly like the pre-existing `connect` (no device restriction).
    pub async fn connect_via(
        addr: SocketAddr,
        device: Option<&str>,
        max_frame_length: usize,
        call_timeout: Duration,
    ) -> Result<Self, RpcError> {
        let stream = match device {
            None => TcpStream::connect(addr).await?,
            Some(device) => {
                let socket = match addr {
                    SocketAddr::V4(_) => TcpSocket::new_v4(),
                    SocketAddr::V6(_) => TcpSocket::new_v6(),
                }?;
                socket.bind_device(Some(device.as_bytes()))?;
                socket.connect(addr).await?
            }
        };
        Ok(Self::from_stream(stream, max_frame_length, call_timeout))
    }

    /// Wraps an already-connected [`TcpStream`] — used by tests that set up
    /// the connection themselves (e.g. against a [`crate::TcpServer`] bound
    /// to an ephemeral port).
    #[must_use]
    pub fn from_stream(stream: TcpStream, max_frame_length: usize, call_timeout: Duration) -> Self {
        let framed = Framed::new(stream, EnvelopeCodec::new(max_frame_length));
        let (sink, stream) = framed.split();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        tokio::spawn(run_actor(sink, stream, cmd_rx));
        Self {
            cmd_tx,
            next_id: AtomicU64::new(1),
            call_timeout,
        }
    }

    fn next_correlation_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

async fn run_actor(
    mut sink: SplitSink<Framed<TcpStream, EnvelopeCodec>, Envelope>,
    mut stream: SplitStream<Framed<TcpStream, EnvelopeCodec>>,
    mut cmd_rx: mpsc::UnboundedReceiver<ClientCmd>,
) {
    let mut pending: HashMap<u64, oneshot::Sender<Result<Response, RpcError>>> = HashMap::new();
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    None => break,
                    Some(ClientCmd::Call { envelope, reply }) => {
                        let correlation_id = envelope.as_request().map(|r| r.correlation_id).unwrap_or_default();
                        match sink.send(envelope).await {
                            Ok(()) => {
                                pending.insert(correlation_id, reply);
                            }
                            Err(err) => {
                                let _ = reply.send(Err(err));
                            }
                        }
                    }
                    Some(ClientCmd::Notify { envelope, reply }) => {
                        let result = sink.send(envelope).await;
                        let _ = reply.send(result);
                    }
                }
            }
            frame = stream.next() => {
                match frame {
                    None => {
                        for (_, reply) in pending.drain() {
                            let _ = reply.send(Err(RpcError::ConnectionClosed));
                        }
                        break;
                    }
                    Some(Err(err)) => {
                        tracing::warn!(error = %err, "ntk-rpc: client connection error");
                        for (_, reply) in pending.drain() {
                            let _ = reply.send(Err(RpcError::ConnectionClosed));
                        }
                        break;
                    }
                    Some(Ok(envelope)) => {
                        if let Err(mismatch) = envelope.check_version() {
                            tracing::warn!(%mismatch, "ntk-rpc: dropping envelope with incompatible protocol version");
                            continue;
                        }
                        if let Some(Body::Response(response)) = envelope.body
                            && let Some(reply) = pending.remove(&response.correlation_id)
                        {
                            let _ = reply.send(Ok(response));
                        }
                    }
                }
            }
        }
    }
}

impl RpcClient for TcpRpcClient {
    fn call<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
        self.call_authenticated(caller, unicast_id, call, None)
    }

    fn notify<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        self.notify_authenticated(caller, unicast_id, call, None)
    }

    fn call_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
        Box::pin(async move {
            let correlation_id = self.next_correlation_id();
            let mut envelope = Envelope::request(
                ProtocolVersion::CURRENT,
                correlation_id,
                caller,
                unicast_id,
                true,
                call,
            );
            if let Some(auth) = auth {
                envelope = envelope.with_auth(auth);
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            self.cmd_tx
                .send(ClientCmd::Call {
                    envelope,
                    reply: reply_tx,
                })
                .map_err(|_| RpcError::ConnectionClosed)?;
            let response = match tokio::time::timeout(self.call_timeout, reply_rx).await {
                Err(_elapsed) => return Err(RpcError::Timeout),
                Ok(Err(_recv_error)) => return Err(RpcError::ConnectionClosed),
                Ok(Ok(result)) => result?,
            };
            match response.outcome {
                Some(Outcome::Payload(payload)) => Ok(payload),
                Some(Outcome::Error(error)) => Err(RpcError::Remote(error)),
                None => Err(RpcError::Malformed("Response.outcome unset".to_owned())),
            }
        })
    }

    fn notify_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        Box::pin(async move {
            let correlation_id = self.next_correlation_id();
            let mut envelope = Envelope::request(
                ProtocolVersion::CURRENT,
                correlation_id,
                caller,
                unicast_id,
                false,
                call,
            );
            if let Some(auth) = auth {
                envelope = envelope.with_auth(auth);
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            self.cmd_tx
                .send(ClientCmd::Notify {
                    envelope,
                    reply: reply_tx,
                })
                .map_err(|_| RpcError::ConnectionClosed)?;
            reply_rx.await.map_err(|_| RpcError::ConnectionClosed)?
        })
    }
}
