# ntk-rpc

Transport and dispatch for the netsukuku-rs inter-node RPC protocol — the
Rust replacement for zcd.

## Where this sits

`ntk-rpc` depends on `ntk-proto` only, and on nothing else in the workspace.
Every module crate (`ntk-neighborhood`, `ntk-qspn`, `ntk-identities`,
`ntk-peerservices`, `ntk-hooking`, `ntk-coordinator`, `ntk-andna`) depends on
it for the same two seams: an `RpcClient` to make outbound calls and an
`RpcHandler` to answer inbound ones. `ntkd`, the composition root, owns the
actual `TcpServer`/`UdpBroadcaster` instances and routes decoded calls to
each module's handler.

Transport shape: one persistent TCP connection per peer, multiplexing
concurrent calls by `Request.correlation_id` so handling order never
matters, plus one UDP broadcast/ack socket per participating NIC for the
discovery-time broadcast methods.

## What it provides

- [`RpcClient`] — an async, object-safe trait for issuing calls
  (`Arc<dyn RpcClient>`, hand-written as boxed futures rather than an
  `async-trait` macro). [`TcpRpcClient`] is the real implementation;
  [`FakeRpcClient`] is an in-memory double with configurable latency and
  fault injection, used throughout the workspace's in-process multi-node
  tests.
- [`RpcHandler`] — the inbound seam: decode a `MethodCall`, produce a
  `ResponsePayload` or a `RemoteError`. [`FnHandler`] adapts a plain async
  closure into one, for tests and small services.
- [`TcpServer`] — a listener that accepts connections and dispatches each to
  a shared `RpcHandler`, one task per connection, cancellable via a
  `CancellationToken`.
- [`UdpBroadcaster`] — per-NIC broadcast/ack, unframed (one packet, one
  `Envelope`), used for the discovery-time broadcast methods.
- [`EnvelopeCodec`] — the length-delimited framing codec shared by client and
  server.
- [`RpcError`] — the local/remote error split: exactly one variant
  (`RpcError::Remote`) crosses the wire; everything else (I/O failure,
  timeout, decode failure) is local-only and never leaks peer-controlled
  detail back onto the network.

**The wire protocol is cleartext.** Nothing in this crate encrypts or
authenticates a frame. Optional per-message ed25519 authentication lives in
`ntk-proto::auth` (sign/verify against `(method, payload, sequence)`, with
replay protection via a bounded sequence-number table) and is carried as an
`Option<Auth>` on `Envelope`/dispatch — but it is off by default
(`require_auth` defaults to `false`); a deployment that needs it must opt in
explicitly at the call site.

## Usage

```rust,no_run
use std::sync::Arc;
use std::time::Duration;

use ntk_proto::v1::{CallerContext, MethodCall, TypedValue};
use ntk_rpc::{FnHandler, RpcClient, TcpRpcClient, TcpServer};
use tokio_util::sync::CancellationToken;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let addr = "127.0.0.1:0".parse().unwrap();
let server = TcpServer::bind(addr, 1 << 20).await?;
let local_addr = server.local_addr()?;
let cancel = CancellationToken::new();

let handler = Arc::new(FnHandler(
    |_caller: CallerContext, _unicast_id: TypedValue, _call: MethodCall, _auth| async move {
        unimplemented!("route `_call` to the owning module's actor")
    },
));
tokio::spawn(server.serve(handler, cancel.clone()));

let client = TcpRpcClient::connect(local_addr, 1 << 20, Duration::from_secs(5)).await?;
// client.call(caller_context, unicast_id, method_call).await?;
# Ok(())
# }
```

## License

GPL-3.0-or-later. Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs)
workspace.
