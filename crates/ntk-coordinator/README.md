# ntk-coordinator

Coordinator: per-level reserved-position allocator and DHT-hash-based election for Netsukuku,
implemented as a `PeerService` over `ntk-peerservices`.

## Where this sits

Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs) workspace. This crate depends
on `ntk-common`, `ntk-proto`, `ntk-rpc`, and `ntk-peerservices`, whose DHT substrate it registers
on. It is not called directly by application code — `ntk-hooking` declares its own
`CoordinatorClient` trait for what it needs during network join/merge, and `ntkd` (the
composition root) implements that trait against this crate's real `CoordinatorClient`. Coordinator
itself declares no dependency on `ntk-hooking` or `ntk-qspn`; the capabilities it needs from the
rest of the daemon come back as its own traits (`CoordinatorMap`, `EnterHandlers`,
`PropagationHandler`) that `ntkd` implements.

## Why a coordinator exists at all

In Netsukuku, a node's address *is* its position in the hierarchical g-node tree — there is no
separate identifier layered on top the way an IP address sits apart from a DHCP lease. That means
joining a g-node, or migrating between two, requires someone to hand out a free position without
two nodes colliding on the same one. This crate is that allocator: per level, exactly one node —
whichever the DHT resolves the fixed key `perfect_tuple(k) = [0,0,...,0]` (`top` zeros) to, i.e.
position 0, the eldest node in that g-node — runs the fixed-keys reservation database for that
level. This is a DHT-hash lookup, not an invented leader-election protocol: the same election
mechanism `ntk-peerservices`' `approximate()` already provides for any service.

## What it provides

- [`CoordinatorService`] — the `PeerService` registration; mandatory (every node implicitly
  participates, no gossip needed), matching one Coordinator servant per level.
- [`Handle`] (the servant side) — runs the fixed-keys database ([`GnodeMemory`], keyed by level)
  for whichever level this node happens to be elected servant of, and serves reservations.
- [`CoordinatorClient`] (the client side) — the DHT-routed proxy any node uses to reach whatever
  node is *currently* elected servant for a level, without needing to know who that is in
  advance. `reserve()` is its main entry point, returning a [`Reservation`] (new position, new
  eldership) or a [`ReserveError`].
- [`EnterHandlers`] / [`PropagationHandler`] / [`CoordinatorMap`] — the trait seam Hooking's
  enter/reserve/migrate decisions are serialized through; `ntkd` implements these against the
  real Hooking and QSPN state.

Coordinator is not usefully standalone: it exists to serialize position allocation for whatever
drives network join and migration, which in this workspace is `ntk-hooking` via `ntkd`'s
adapters.

## Usage sketch

```rust,ignore
// From the client side — reserving a position at level `top`, once `ntkd` has wired a
// `CoordinatorClient` from this node's `ntk_peerservices::Handle`:
let client = ntk_coordinator::CoordinatorClient::new(peers_handle, config);
let reservation = client.reserve(top, reserve_request_id, &[]).await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The servant side ([`Manager`]/[`Handle`]) and the `EnterHandlers`/`PropagationHandler` trait
implementations are composed by `ntkd`; this crate does not run standalone.

## License

GPL-3.0-or-later. Part of [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs).
