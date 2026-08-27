# ntk-qspn

QSPN v2 — the netsukuku-rs routing core: ETP propagation, disjoint-path
admission, implicit withdrawal, and split/merge detection via fingerprints.

## Where this sits

`ntk-qspn` depends only on `ntk-common` (topology/addressing types) and
`ntk-rpc` (transport traits for its outbound stub and inbound handler). It is
one of the twelve workspace crates; the composition root, `ntkd`, drives it
directly and subscribes to the route snapshots it publishes, diffing them
into real kernel routes via `ntk-netlink`. It does not depend on, or know
about, `ntk-neighborhood`, `ntk-hooking`, or any other module crate — the
QSPN protocol only needs arc costs and an RPC seam, both supplied by the
caller.

The single most interesting property of this crate: routing state is
**O(gsize × levels), not O(n)**. The hierarchical g-node structure is what
makes that bound hold — a node never keeps a full picture of every peer in
the network, only a fingerprint per level of the hierarchy it belongs to.

## What it provides

- [`QspnState`] — the owned protocol state: `update_map`/`update_clusters`,
  the `enter_net` bootstrap constructor, and the connectivity lifecycle
  (`make_connectivity`/`exit_network`/`check_connectivity`).
- [`manager::spawn`]/[`manager::spawn_entering`] — start the QSPN actor,
  returning a cheap-clone [`QspnHandle`] plus its `JoinHandle`. `spawn` is
  for a freshly created network identity; `spawn_entering` bootstraps into an
  existing one by fetching full ETPs from a set of external arcs.
- [`revise_etp`] — the highest-risk single function in the crate: turns one
  received ETP into this node's own admitted paths, including the *implicit*
  withdrawal of paths the sender silently stopped repeating.
- [`RouteSnapshot`]/[`RouteEntry`] — the immutable, `watch`-published view a
  caller diffs into real routes.
- [`QspnRpcHandler`] — the inbound `ntk_rpc::RpcHandler` for the 4 `qspn_*`
  wire methods; [`QspnStubFactory`]/[`FakeQspnStubFactory`] — the outbound
  seam and its in-memory test double.

`src/{revise,state}.rs` hold the algorithms this crate is riskiest to get
wrong, and they carry the weight of it: a dedicated `proptest` suite
(`tests/proptest_invariants.rs`) fuzzes acyclic-path admission, cost
monotonicity, implicit-withdrawal scoping, and fingerprint-ordering
independence, alongside multi-node convergence and split/merge integration
tests.

## Usage

This crate is not meaningfully standalone — `QspnState`/`QspnHandle` only do
anything useful wired to real arcs and a real transport, which is `ntkd`'s
job. One piece that *is* self-contained and safe to reach for directly is
the pure disjoint-path admission ratio:

```rust
use ntk_qspn::{mch_ratio, QspnConfig};

let config = QspnConfig::default();
// Tolerance for a destination reachable through 3 distinct gateways,
// spanning 40 nodes.
let ratio = mch_ratio(config.max_common_hops_ratio, &config.mch_ratio_table, 40, 3);
assert!(ratio > 0.0 && ratio <= config.max_common_hops_ratio);
```

For the actor itself, see `manager::spawn`'s signature and `ntkd`'s
`crates/ntkd/src/node/adapters.rs` for how a real `QspnStubFactory` is
assembled.

## License

GPL-3.0-or-later. Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs)
workspace.
