# ntk-identities

Multi-identity-per-node support for
[`netsukuku-rs`](https://github.com/M0Rf30/netsukuku-rs): the identity
registry, the arc-to-identity-arc mapping, and the duplication handshake that
underlies *live* g-node migration during hooking. One of the twelve workspace
crates composed by [`ntkd`](https://crates.io/crates/ntkd), the only binary in
the project; it depends only on `ntk-common`, `ntk-proto`, and `ntk-rpc`.

## Why identities exist

To move a g-node to a new position in the topology without breaking its
internal connectivity, the node that is that g-node's single point of contact
with the outside forks into two identities: the *old* identity keeps the
external arcs alive as a connectivity-only fork while the *new* identity takes
over the internal presence and re-hooks at the new position. This crate models
exactly that fork — not neighbour discovery, not the hooking state machine
itself (that's [`ntk-hooking`](https://crates.io/crates/ntk-hooking)), and no
netlink/kernel side effect (that's composed by `ntkd`).

## What it provides

- [`Registry`] / [`IdentityRecord`] — the set of identities a node currently
  holds, one designated `main_id`, each with its own address and status.
- [`ArcId`] / [`IdentityArc`] — the mapping from a physical arc to its
  per-identity view.
- [`Handle`] — the actor entry point. `Handle::spawn` starts the identity
  manager (seeded with a freshly generated main identity); `Handle::watch`
  publishes read-only [`IdentitySnapshot`]s and `Handle::subscribe` streams
  [`IdentityEvent`]s. [`Handle::prepare_migration`] and [`Handle::migrate`]
  drive the duplication handshake — matching a peer's `match_duplication`,
  producing the [`DuplicationData`] the new identity needs.
- [`pseudo`] — deterministic pseudo-address and pseudo-device naming rules the
  daemon hands to `ntk-netlink` when materializing the old identity's fork as
  a real (pseudo-)interface.
- [`IdentityRpcHandler`] — the inbound `ntk_rpc::RpcHandler` for the identity
  module's wire methods.

## Current state of migration

The duplication handshake is implemented, and `ntkd` genuinely drives it:
`ntk_hooking::HookingEvent::DoPrepareMigration`/`DoFinishMigration` are wired
to `Handle::prepare_migration`/`Handle::migrate` in `ntkd`'s lifecycle code,
and that is real identity-registry bookkeeping, not a stub. What is *not*
wired up yet is the second full protocol stack that should spin up for the
identity `migrate` resolves — a fully faithful port would start that second
stack (QSPN, neighbourhood, etc. bound to the new identity) the moment
`migrate` returns its id. `ntk-qspn`'s own scope note documents that it never
models `enter_net`, which is the underlying gap. In short: the handshake
itself is complete and exercised; running two live protocol stacks per node
during a migration window is not yet done.

This crate is only meaningful composed by `ntkd` — see
`crates/ntkd/src/node/lifecycle.rs` for how `Handle` is spawned and driven
against real hooking events, and `Handle::set_naddr` for how the daemon
updates it in place once hooking resolves a position, rather than rebuilding
it.

## License

GPL-3.0-or-later. Source and issue tracker:
<https://github.com/M0Rf30/netsukuku-rs>.
