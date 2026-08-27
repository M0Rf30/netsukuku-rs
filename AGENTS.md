# Repository Guidelines

## Project Overview

Rust reimplementation of the Netsukuku mesh-networking protocol suite: QSPN v2 routing, Hooking
(network join/merge), Coordinator (position reservation), PeerServices (DHT substrate), ANDNA
(distributed hostnames). 12 crates, ~44k lines of source.

**The normative upstream is Luca Dionisi's Vala rewrite (2017-2020)**, vendored at
`research/impl/vala/`. The legacy C daemon (`research/impl/c/netsukuku`) is *not* the reference —
its `doc/` tree has one commit ever (2013) and its recent activity is build/CI only. Porting from
the C tree reintroduces superseded designs (QSPN v1, fixed `MAXGROUPNODE=256` addressing, the
pre-split Radar/Hook model). Evidence and the full divergence list: `research/notes/03-specs-and-rfcs.md`.

Netsukuku is an **L3 routing protocol, not a TUN overlay**: the daemon owns real kernel routing
tables. Do not add a TUN device; do not shell out to `ip`/`iptables`/`sysctl` anywhere, including
in tests — replacing upstream's subprocess calls with native netlink is a core purpose of this port.

## Architecture & Data Flow

Three foundation crates, nine protocol/module crates, one composition root:

```
ntk-common ──────────────────────────────► everyone      (Topology, Naddr, HCoord, Cost, Fingerprint)
ntk-proto  ──► ntk-rpc, every module crate               (wire schema + domain codec)
ntk-rpc    ──► every module crate                        (TCP unicast, UDP broadcast, client/handler traits)
ntk-netlink ─► ntkd                                      (address/route/rule/link traits; no sibling deps)
ntk-peerservices ──► ntk-coordinator, ntk-andna
ntkd ──► all eleven                                      (the ONLY composition root)
```

`ntk-hooking` deliberately depends on **no** sibling protocol crate. It declares what it needs as
its own traits (`QspnView`, `CoordinatorClient` in `crates/ntk-hooking/src/{view,coordinator}.rs`),
which `ntkd` implements against the real crates in `crates/ntkd/src/node/adapters.rs`. Preserve that
inversion — a direct dependency would create a cycle and make the crate untestable.

One inbound request end to end:

1. `ntk_rpc::TcpServer` decodes an `Envelope` (`crates/ntk-rpc/src/server.rs`).
2. `crates/ntkd/src/node/dispatch.rs` routes the `ntk_proto::v1::MethodCall` arm to the owning
   module's `RpcHandler` (neighborhood 5 methods, identity 3, qspn 4, peers 12, coordinator 5,
   hooking 10). ANDNA has no `MethodCall` arms by design — it is reached via `PeerService::exec`.
3. The handler sends a command into that module's actor `mpsc` and awaits a `oneshot` reply.
4. The actor mutates its own state and republishes an immutable snapshot over `watch`; observers get
   `broadcast` events.
5. `ntkd` subscribes to `ntk_qspn::RouteSnapshot`, diffs it, and issues only the deltas through
   `ntk-netlink` (`crates/ntkd/src/kernel/routes.rs`).

## Key Directories

| Path | Purpose |
|---|---|
| `crates/ntk-common/` | Shared domain types. No dependencies. Start here to understand addressing. |
| `crates/ntk-proto/` | `proto/ntk.proto` (39-method surface) + `proto/domain.proto` (shared types) + the revalidating wire↔domain codec. |
| `crates/ntk-netlink/` | The kernel seam: four traits, `RealNetlink`, and `FakeNetlink` with an ordered operation log. |
| `crates/ntkd/src/kernel/` | Machine-facing half: config, capability preflight, NIP→IPv4 addressing, route installation. |
| `crates/ntkd/src/node/` | Protocol-facing half: CLI, supervisor, transport, dispatch, adapters, lifecycle. |
| `research/notes/` | Committed institutional memory, ~1600 lines. Read before designing anything. |
| `research/impl/`, `specs/`, `papers/`, `related/` | Vendored upstream corpus — **gitignored**, present locally, regenerable per `research/README.md`. |

Doc comments cite upstream as `research/impl/vala/<path>:<line>`. Those paths resolve on this
machine but **not in a fresh clone** — re-clone per `research/README.md` before chasing one.

Route a question to the right note: `01` QSPN/neighborhood/identities/hooking/coordinator internals ·
`02` zcd wire protocol, PeerServices, daemon/kernel wiring · `03` which spec is normative, RFC
adoption · `04` papers and formal grounding · `05` Yggdrasil/cjdns/Babel comparison · `06` crate
verdicts and the concurrency strategy.

## Development Commands

```bash
cargo build --workspace --all-targets
cargo test  --workspace                              # 527 pass, 24 ignored (privilege-gated)
cargo clippy --workspace --all-targets -- -D warnings # must stay at zero warnings
cargo fmt --all --check
cargo run -p ntkd -- --help                          # subcommands: run, status
cargo bench --workspace                              # criterion; see `crates/*/benches`
```

Privileged tier — real netlink and real network namespaces, rootless via user namespaces:

```bash
unshare --net --map-root-user -- sh -c 'ip link set lo up; cargo test -p ntk-netlink -- --ignored'
unshare --net --map-root-user -- cargo test -p ntkd --test multi_node -- --ignored
```

No task runner, no Makefile, no scripts directory — raw `cargo` plus those `unshare` one-liners is
the whole interface. When touching a single crate use `-p <crate>`: a workspace-wide command fails
while any sibling is mid-edit.

## Code Conventions & Common Patterns

**The actor pattern is the central convention.** Each module crate owns one actor: a private
command enum consumed off an `mpsc` queue, `oneshot` reply channels, an immutable snapshot published
via `tokio::sync::watch`, events via `broadcast`, a child `CancellationToken`, and a `JoinSet` that
reaps its own tasks. Canonical example: `crates/ntk-qspn/src/manager.rs`.

Three hard rules, each learned from a real bug — do not relax them:

- **Never `Arc<RwLock<_>>` over protocol state.** Upstream's lock-free code is only sound under
  pth-tasklet's cooperative single-thread scheduler; a shape-for-shape port onto multi-threaded
  tokio reintroduces races (`research/notes/06-rust-stack.md`, §Concurrency).
- **Never await an outbound RPC inside a command loop.** Spawn it into the actor's `JoinSet` and
  feed the result back as a new command. Two peers racing `add_arc` deadlocked exactly this way;
  the rationale is at the top of `crates/ntk-qspn/src/manager.rs`.
- **Subscribe to a `broadcast` channel synchronously, before any `await`.** `ntk-qspn` fires
  `QspnEvent::BootstrapComplete` ~1 ms after first poll; a late subscriber misses it permanently and
  silently (`crates/ntkd/src/node/lifecycle.rs`).

**Dependency injection is trait-per-capability, with a real and a fake implementation.** Outbound
calls go through a stub-factory trait (upstream's `IXxxStubFactory`); kernel access goes through the
`ntk-netlink` traits. Real impls talk to `ntk_rpc::TcpRpcClient`/`RealNetlink`; fakes live in each
crate's `fake.rs` (except `ntk-neighborhood`, whose double sits in `src/nic.rs`). Never make a
module depend on a concrete transport or on `RealNetlink`.

**Errors:** `thiserror` enums per library crate, `anyhow` only at the `ntkd` binary boundary. In
`ntk-rpc` exactly one variant crosses the wire (`RpcError::Remote`); everything else is local-only.
Library code does not panic on protocol input — upstream `assert_not_reached()` sites became `Err`
variants. Return an error, never `unwrap` a peer's data.

**Anything that must be unique across nodes derives from `ntk_neighborhood::NodeId.`** Three
separate bugs came from treating a node-local counter as globally meaningful: a `LinkId` shipped in
`CallerContext.src_nic` and decoded against the *receiver's* registry, a MAC hashed from the device
name alone, and a link-local allocator starting at a fixed `1`. Derive from `NodeId`; do not invent
a second per-process identity.

**Naming:** `actor.rs`/`manager.rs` (the actor), `stub.rs` (outbound seam), `handler.rs`/`rpc.rs`
(inbound), `wire.rs` (proto↔domain), `fake.rs` (test doubles). Types follow `*Handle`, `*Manager`,
`*StubFactory`, `*RpcHandler`, `Fake*`. Cheap-clone `*Handle` is the only public way to reach an
actor.

**Record deliberate deviations from upstream in the doc comment itself**, next to the citation —
several upstream behaviours were intentionally *not* copied (a 600 s busy-wait, a `-1`-sentinel
solution in `find_shortest_mig`, regex-scraping `ip` output for cleanup).

**Leave unfinished capability out of the API rather than stubbing it.** Workspace lints deny
`clippy::todo` and `clippy::unimplemented`; there are no placeholder functions to imitate.

## Important Files

| File | Why it matters |
|---|---|
| `crates/ntkd/src/node/lifecycle.rs` | Bootstrap, the steady-state loop, and `rehook()`. The daemon's brain; largest file in `ntkd`. |
| `crates/ntkd/src/node/adapters.rs` | Where `ntk-hooking`'s inverted traits meet the real crates. Two level-arithmetic bugs lived here. |
| `crates/ntkd/src/node/dispatch.rs` | The single inbound routing table for all 39 methods. |
| `crates/ntk-qspn/src/{revise,state}.rs` | Implicit withdrawal and `update_map` — the highest-risk algorithms in the project. |
| `crates/ntk-proto/proto/domain.proto` | Shared wire types; module protos import it rather than redeclaring. |
| `crates/ntkd/tests/multi_node.rs` | Both real-kernel scenarios, with root-cause history in the doc comments. Read those before editing. |
| `Cargo.toml` (root) | `members = ["crates/*"]`, all dependency versions, all lints. |

## Runtime/Tooling Preferences

- rustc/cargo **1.98.0** (manifests pin `rust-version = 1.97`), edition 2024, resolver 3, Linux-only.
- **Every crate dependency must be `<name>.workspace = true`.** Versions live only in the root
  `[workspace.dependencies]`. Adding a dependency is a root-manifest decision, not a per-crate one.
- Workspace lints: `unsafe_code = "deny"`, `clippy::{todo,unimplemented,dbg_macro} = "deny"`,
  `missing_debug_implementations = "warn"`.
- Approved stack: `tokio` + `tokio-util` (`CancellationToken`, `JoinSet`) · `rtnetlink` +
  `netlink-packet-route` + `socket2` + `nix` · `prost` + `protox` · `serde` + `toml` · `thiserror` /
  `anyhow` · `tracing` · `clap` · `ed25519-dalek` + `blake3` · `proptest` · `criterion` (benches).
- **Deliberately absent:** `async-trait` (traits are hand-written boxed-future and dyn-compatible),
  any TUN crate, any NAT/nftables crate, layered config crates. `rand` is used only by `ntk-andna`
  and `ntkd`. Note `research/notes/06-rust-stack.md` also proposes `tun-rs`, `snow`, and an
  `ntk-crypto` crate — all three were consciously dropped; the manifests are authoritative.
- Codegen: eight crates have a `build.rs` using `protox` (pure Rust). **No system `protoc` is
  required** — do not add one. Module protos live at `crates/<crate>/proto/<module>.proto` in package
  `ntk.<module>.v1`, and reference shared types via `extern_path` to `::ntk_proto::domain::v1`
  instead of regenerating them. Two recorded gotchas: `protox` needs `.include_imports(true)`, and
  prost-build 0.14 panics on a *singular* extern-mapped message field (`andna.proto` uses `repeated`
  as the workaround).
- `ntkd` has both `[lib]` and `[[bin]]`, plus a `test-util` feature: integration tests link it as an
  ordinary dependency and so never see plain `#[cfg(test)]` items.

## Testing & QA

Four tiers:

1. **Unit** — inline `#[cfg(test)]` modules, the bulk of the suite.
2. **Property** — `proptest`, concentrated in `crates/ntk-qspn/tests/proptest_invariants.rs`
   (acyclic paths, cost monotonicity, implicit-withdrawal scoping, fingerprint ordering).
3. **In-process multi-node** — real actor code over `FakeRpcClient` + `FakeNetlink` and, in `ntkd`,
   an in-memory `Medium` that emulates the broadcast domain.
4. **Real-kernel**, 24 `#[ignore]`d tests: `crates/ntk-netlink/tests/real_netlink.rs`,
   `crates/ntkd/tests/multi_node.rs` (daemons in separate namespaces over a veth pair, built
   natively with `nix::sched::unshare` + `rtnetlink`, never `ip netns`), the `mac80211_hwsim`
   wireless fixtures, and one ICMP probe test needing a usable ping socket. Each documents its
   exact invocation.

Conventions:

- Use `tokio::time::pause`/`advance`; never sleep out a real protocol cadence (neighborhood's
  liveness probe is ~28-30 s, hooking backoffs are ~20 s).
- Assert on `FakeNetlink::operations()` — the ordered operation log — rather than on internal state.
  Prove that an unchanged snapshot issues *zero* operations, not merely few.
- **Join spawned tasks.** A panic in an unjoined task is silently swallowed; that hid a real
  `ntk-peerservices` shutdown bug for a full phase.
- `#[ignore]` is reserved for privilege-gated tests, each documenting its exact invocation. The
  default `cargo test` run must never require privileges.
- Test names read as sentences, e.g. `inbound_caller_never_resolves_to_a_different_peers_arc_on_link_id_collision`.
- Stress-check anything touching the real-kernel tier: run it 12+ times in parallel mode. Two
  distinct flakes were found and fixed that way.

**Never weaken an assertion to reach green.** When a test legitimately cannot pass, the convention
here is to leave it red or `#[ignore]`d with the evidence in its doc comment and escalate the
diagnosis — that discipline is how four production bugs (a `/32` link-local killing broadcast, a
duplicated link-local address, a wedged arc-dial cycle, a `broadcast` subscription race) were found
rather than papered over.

`ntk-neighborhood` has no `tests/` directory — all of its coverage is inline, and its
`src/manager.rs` (~1.7k lines) is the largest file in the repo. Both are known gaps, not a pattern
to copy.

CI (`.github/workflows/ci.yml`) runs the four standard commands plus a separate privileged job. That
job uses `sudo unshare --net`, **not** `--map-root-user`, because GitHub's ubuntu-24.04 image
restricts unprivileged user namespaces via AppArmor. That CI path has not yet been observed to pass
— if the privileged job fails on a first run, start there rather than assuming a code regression.
