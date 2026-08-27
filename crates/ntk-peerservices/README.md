# ntk-peerservices

Generic distributed-service substrate for Netsukuku — DHT-over-hierarchy, per NTK_RFC 0014
("P2P over Netsukuku").

## Where this sits

Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs) workspace, a Rust
reimplementation of the Netsukuku mesh-routing protocol. This crate depends only on
`ntk-common` (topology/address types), `ntk-proto` (wire schema), and `ntk-rpc` (transport).
Two sibling crates register services on top of it: `ntk-coordinator` (position allocation and
election) and `ntk-andna` (distributed hostnames). The composition root, `ntkd`, wires the
outbound seam to real neighborhood/QSPN/hooking state; nothing in this crate talks to the
kernel or to sockets directly.

## What makes this not Kademlia

A conventional DHT (Chord, Kademlia) hashes a key into a *flat* keyspace and routes there with
its own overlay topology — k-buckets, finger tables, a second address space layered over the
network's real one. This crate does the opposite: [`hash_to_tuple`] maps a key onto a
**position inside the network's existing hierarchical address space** (the same g-node/level
structure QSPN already routes on), and the request travels there over ordinary hierarchical
routing. There is no second keyspace and no separate overlay to keep consistent with reality —
the DHT's routing table *is* the topology.

[`approximate`] is the actual `h(k) = H(h'(k))` mapping from RFC 0014 §2: given a target tuple,
it returns whichever known g-node is closest by [`dist`], a Chord-like (but asymmetric,
non-metric-symmetric) circular distance over the mixed-radix position tuple.

## What it provides

- [`PeerService`] — the trait a module implements to register on the substrate. A service
  declares whether it's optional (gossiped participation) or mandatory (every node implicitly
  participates), and answers requests via `exec(request, client_tuple)`.
- [`Handle::contact_peer`] — routes an opaque request to whichever node is closest to a target
  tuple, following refusals and redo-from-start restarts until a servant answers or routing is
  exhausted.
- [`Handle::replicate`] — RFC 0014 §2.2 step 5's redundancy rule: after the primary hash-node
  accepts a write, replicate it to `q` more of the closest nodes so any can take over if the
  primary is lost.
- [`Handle::register`] — registers a `PeerService` with the local actor.

This crate is a substrate, not an application: nothing here is useful stood up alone. A caller
registers a `PeerService` and drives it through `contact_peer`/`replicate`; see `ntk-coordinator`
or `ntk-andna` for what a real service built on this substrate looks like.

## Usage sketch

```rust,ignore
use ntk_peerservices::{ExecError, PeerService, ServiceId};
use ntk_proto::v1::TypedValue;
use futures::future::BoxFuture;

struct MyService;

impl PeerService for MyService {
    fn service_id(&self) -> ServiceId {
        ServiceId::new(950)
    }
    fn is_optional(&self) -> bool {
        true
    }
    fn exec<'a>(
        &'a self,
        request: TypedValue,
        client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move { Ok(request) }) // echo, for illustration
    }
}

// `handle: ntk_peerservices::Handle` comes from `Manager::new`, wired by `ntkd`.
async fn register(handle: &ntk_peerservices::Handle) {
    handle.register(std::sync::Arc::new(MyService)).await;
}
```

## License

GPL-3.0-or-later. Part of [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs).
