# ntk-hooking

Bootstrap/join protocol for [`netsukuku-rs`](https://github.com/M0Rf30/netsukuku-rs):
per-arc network-merge negotiation, migration-path search, and g-node split
handling — the logic that runs when a node first joins a Netsukuku mesh, or
when two independently-formed networks meet at a new arc and must merge into
one. It is one of the twelve workspace crates composed by
[`ntkd`](https://crates.io/crates/ntkd), the only binary in the project.

## Where it sits

`ntk-hooking` deliberately depends on **no sibling protocol crate** — not
`ntk-qspn`, `ntk-identities`, `ntk-neighborhood`, or `ntk-coordinator`. Instead
it declares the two capabilities it needs as its own traits:

- [`QspnView`] — a read-only, synchronous view onto this node's current
  topology, position, and map.
- [`CoordinatorClient`] — the outbound seam onto the (per-level elected)
  Coordinator: DHT-mediated position reservation and evaluate/begin/complete/
  abort-enter election, plus local-propagation calls for migration.

`ntkd`, the composition root, implements both traits by delegating to the real
`ntk-qspn` and `ntk-coordinator` crates. That inversion is what keeps
`ntk-hooking` acyclic in the dependency graph and independently testable
against fakes ([`FakeQspnView`], [`FakeCoordinatorClient`]) rather than a live
network.

## What it provides

- [`manager::spawn`] / [`HookingHandle`] — the actor: spawn it with a
  [`QspnView`], a [`CoordinatorClient`], a [`HookingStubFactory`] (outbound RPC),
  and an [`HookingOrigin`] (create a new network vs. join an existing one).
  The handle exposes `snapshot()` (arcs, hooked state, chosen address) and a
  `broadcast` stream of [`HookingEvent`]s.
- [`HookingRpcHandler`] — the inbound `ntk_rpc::RpcHandler` for the protocol's
  10 wire methods.
- [`search`] — the pure algorithms: `execute_search`/`execute_explore`/
  `execute_delete_reserve`/`execute_mig`, and the `find_shortest_mig` BFS over
  migration paths, reachable independently of the actor via [`SearchRouter`].
- [`merge`] — the pure size-based merge-direction heuristic
  ([`merge_direction`], [`merge_tiebreak`]).

Because this crate never touches a live network by itself, it is only really
meaningful composed by `ntkd` (see `crates/ntkd/src/node/adapters.rs`, where
the traits above meet the real `ntk-qspn`/`ntk-coordinator` state) or exercised
directly against its exported `Fake*` test doubles, as below.

```rust
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use ntk_common::Topology;
use ntk_hooking::{
    FakeCoordinatorClient, FakeHookingStubFactory, FakeQspnView, HookingConfig, HookingOrigin,
};

let topology = Topology::new([16, 16]).unwrap();
let view = Arc::new(FakeQspnView::new(topology, vec![0, 0]));
let coord = Arc::new(FakeCoordinatorClient::default());
let stubs = Arc::new(FakeHookingStubFactory::default());
let (handle, _join) = ntk_hooking::spawn(
    HookingOrigin::CreateNet,
    view,
    coord,
    stubs,
    HookingConfig::default(),
    CancellationToken::new(),
);
let _snapshot = handle.snapshot();
```

Against real crates, `ntkd`'s `adapters.rs` is the reference for what the
`view`/`coord`/`stubs` arguments look like in production.

## License

GPL-3.0-or-later. Source and issue tracker:
<https://github.com/M0Rf30/netsukuku-rs>.
