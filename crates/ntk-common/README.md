# ntk-common

Shared domain vocabulary for [`netsukuku-rs`](https://github.com/M0Rf30/netsukuku-rs), a Rust
reimplementation of the Netsukuku mesh-networking protocol suite (QSPN v2 routing, Hooking,
Coordinator, PeerServices, ANDNA). This is the foundation crate: it depends on no sibling
`ntk-*` crate and carries no protocol logic — no RPC, no transport, no netlink — only the types
every other crate in the workspace builds on. `ntk-proto`'s wire codec converts these types
to and from the wire; every protocol-module crate (`ntk-qspn`, `ntk-hooking`, `ntk-coordinator`,
`ntk-peerservices`, `ntk-andna`) and the `ntkd` daemon build on top of that.

## What it provides

- **`Topology`** — the shape of a Netsukuku address hierarchy: how many levels it has and each
  level's g-node size (`gsize(i)`). Netsukuku's addressing is not a flat address space: a node's
  address is a *position in a nested tree*, and `Topology` is that tree's shape, shared (cheaply,
  via `Arc`) by every address built against it.
- **`Naddr`** — a hierarchical address: one position per level of a bound `Topology`. Also
  models *virtual* positions (`pos >= gsize(level)`), which name a reserved-but-not-yet-placed
  slot for a g-node mid-migration.
- **`HCoord`** — a bare `(level, position)` coordinate, used to name "the g-node reached via
  this hop" or "the g-node a destination lives in relative to me" without a full address.
- **`Cost`** — the QSPN path-cost metric: `Null` (zero/identity), `Finite(u64)` (an ordinary
  additive cost), or `Dead` (absorbing "unreachable"), ordered `Null < Finite(_) < Dead`.
- **`Fingerprint<Id>`** — a g-node's identity fingerprint plus the eldership bookkeeping QSPN
  uses to decide which branch of a network split is authoritative. Generic over an opaque
  identity type `Id`; a level-0 fingerprint names a single real node, and
  `Fingerprint::construct` aggregates one level's siblings into the next level's fingerprint.

All types are `Clone + Debug`, most are `Eq + Hash`, and every constructor validates its inputs
(no unchecked, potentially-invalid value is ever exposed) — that validation is exactly what
`ntk-proto`'s wire codec re-runs on every value decoded from an untrusted peer.

## Hierarchical addressing in short

A `Topology` with `gsizes = [16, 16, 256]` describes a three-level hierarchy: 16 nodes per
innermost g-node, 16 of those g-nodes per level-1 g-node, 256 level-1 g-nodes per level-2 g-node.
An `Naddr` built against that topology carries one position per level (`[3, 7, 12]`, say) —
"node 3 within g-node 7 within g-node 12" — rather than a single flat integer. This generalizes
the legacy fixed `MAXGROUPNODE = 256` scheme to an arbitrary per-level size.

## Example

```rust
use ntk_common::{Cost, HCoord, Naddr, Topology};

fn main() -> Result<(), ntk_common::Error> {
    // A three-level hierarchy: 16 nodes per g-node, 16 g-nodes per level-1
    // g-node, 256 level-1 g-nodes per level-2 g-node.
    let topology = Topology::new([16, 16, 256])?;

    let me = Naddr::new(topology.clone(), [3, 7, 12])?;
    let dest = Naddr::new(topology, [9, 7, 12])?;

    // Both addresses share their level-1 and level-2 positions; the highest
    // level at which they diverge is level 0, where `dest` sits at position 9.
    assert_eq!(me.hcoord(&dest)?, Some(HCoord::new(0, 9)));

    // Cost sentinels order as Null < Finite(_) < Dead.
    assert!(Cost::Null < Cost::Finite(1) && Cost::Finite(u64::MAX) < Cost::Dead);
    Ok(())
}
```

## License

GPL-3.0-or-later. Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs) workspace.
