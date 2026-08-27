# 06 — Rust stack proposal for netsukuku-rs

Open this for the Rust side of the fence: which crates were picked and why, the single-owner-actor
concurrency verdict, and the testing strategy — all argued from what the Vala tasklet model assumes
that multi-threaded tokio does not.

Toolchain: rustc/cargo 1.97.1, Linux target. Research-only; no Cargo.toml or Rust source in this batch.
All crate versions/dates/downloads below are `max_stable_version` snapshots from `crates.io` API
(`GET /api/v1/crates/<name>`), fetched 2026-08-23. Re-verify immediately before pinning in Cargo.toml —
several crates here (bincode 3.x, ed25519-dalek/x25519-dalek 3.x) are recent major bumps.

## Vala module inventory (skim only — decomposition, not algorithms)

Dependency lines below are literal, from each module's `README` (`depends on: …`):

| Vala module | Depends on | Role |
|---|---|---|
| `tasklet-system` | (none) | abstract cooperative-tasklet API (`tasklet-system/README:3`) |
| `pth-tasklet` | GNU Pth | concrete tasklet impl on GNU Pth, single OS thread (`pth-tasklet/README:3`) |
| `ntkd-common` | (none) | shared base types, "each module has no dependency on each other" except this (`ntkd-common/README:3-4`) |
| `ntkdrpc` | tasklet-system | RPC interfaces + stub/skeleton codegen surface (`ntkdrpc/README:3-6`) |
| `zcd` | gobject, gee, json-glib, tasklet-system, pth-tasklet | transport: per-NIC unicast (stream) + broadcast/multicast (datagram) dispatch, JSON wire (`zcd/README:3-16`, `zcd/configure.ac:20-29`) |
| `neighborhood` | tasklet-system, ntkd-common, ntkdrpc | arc/link discovery over NICs (`neighborhood/README:3-4`) |
| `identities` | tasklet-system, ntkd-common, ntkdrpc | pseudonym/identity lifecycle, NIP allocation (`identities/README:3-4`) |
| `qspn` | tasklet-system, ntkd-common, ntkdrpc | routing core: ETP publish/retrieve, fingerprint, destinations table (`qspn/README:3-4`) |
| `hooking` | tasklet-system, ntkd-common, ntkdrpc | bootstrap/join sequence, arc handler (`hooking/README:3-4`) |
| `peerservices` | tasklet-system, ntkd-common, ntkdrpc | generic p2p distributed-service substrate (`peerservices/README:3-7`) |
| `coordinator` | …, peerservices | g-node coordinator election, `fk_database` (`coordinator/README:3-4`) |
| `andna` | ntkd-common, tasklet-system, ntkdrpc, pth-tasklet, **peers** (peerservices) | hostname registration/resolution service (`andna/configure.ac:25-34`) |
| `ntkd` | git submodules: qspn, identities, neighborhood, coordinator, andna, hooking | daemon binary; `rpc/module_stubs.vala`, `mainloop.vala`, `startup.vala`, `commander.vala`, `cleaning/` (`ntkd/.gitmodules:1-18`) |

Key finding for the transport layer: `ntkd/identity_ip_commands.vala:36-56` configures addressing and NAT by
**shelling out** to `ip address add …` / `iptables -t nat …` via a `Commander` subprocess wrapper — not native
netlink syscalls. This is a concrete, fixable weakness the Rust port should not reproduce (§ workspace, `ntk-netlink`).

Key finding for the concurrency model: `qspn/qspn.vala` has **zero** explicit `Mutex`/lock use despite mutating
shared collections (`destinations`, `my_fingerprints`, `my_arcs`) from many entry points. This is safe only because
`pth-tasklet` (`pth-tasklet/notes.developer`, `libwrappth.vala`) wraps GNU Pth: cooperative scheduling on a single
OS thread, no preemption between explicit yield points. Every `QspnManager` method is an implicit critical section.
This drives the concurrency-strategy decision below.

## Crate research by area

Legend: **date** = latest stable release date (crates.io `updated_at` for that version); **dl** = total downloads /
recent (~90d) downloads, a maintenance/adoption proxy, not a design signal by itself.

### Async runtime + structured concurrency / cancellation

| crate | version | license | date | dl (total/recent) | verdict |
|---|---|---|---|---|---|
| tokio | 1.53.1 | MIT | 2026-07-20 | 899.9M / 209.4M | **primary**. Features: `rt-multi-thread, net, time, sync, macros, signal, io-util`. |
| tokio-util | 0.7.19 | MIT | 2026-07-21 | 726.9M / 154.4M | **primary** — `sync::CancellationToken` for cancellation trees, `codec` for framing. |
| futures | 0.3.34 | MIT/Apache-2.0 | 2026-08-11 | 732.9M / 159.7M | **primary** — `select!`, `FuturesUnordered` combinators tokio doesn't ship. |
| async-std | 1.13.2 | Apache-2.0/MIT | 2025-08-15 | 89.6M / 9.6M | reject — crate's own description: "Deprecated in favor of `smol`". |
| smol | 2.0.2 | Apache-2.0/MIT | 2024-09-07 | 21.1M / 3.9M | runner-up, reject as primary — netlink/TUN ecosystem below assumes tokio; no built-in structured-task-group equivalent to `JoinSet`. |
| async-scoped | 0.9.0 | Apache-2.0/MIT | 2024-01-25 | 13.8M / 3.3M | reject — 2yr stale; scoped non-`'static` spawn historically flagged for soundness edge cases; superseded by owned `JoinSet`. |
| moro | 0.4.0 | MIT/Apache-2.0 | 2022-06-04 | 12.9k / 988 | reject — abandoned experiment (nursery pattern), negligible adoption. |
| futures-concurrency | 7.7.1 | MIT/Apache-2.0 | 2026-01-18 | 9.4M / 3.6M | optional `join!`/`race!` sugar; not required, `tokio::task::JoinSet` covers the actual need. |

**Structured concurrency verdict**: no dedicated crate. `tokio::task::JoinSet` (built into tokio) is the
one supervisor per composition root (an identity's actor set, or the whole `ntkd` binary); each spawned task
receives a child `tokio_util::sync::CancellationToken` derived from its parent's token. This directly replaces
`pth-tasklet`'s tasklet-group abort semantics (see `hooking/api.vala` "remove_identity" style teardown) without
adding a dependency — every abandoned "structured concurrency" crate above (moro, async-scoped) is stale or dead.

### Netlink, raw sockets, TUN/TAP, discovery, interface enumeration

| crate | version | license | date | dl (total/recent) | verdict |
|---|---|---|---|---|---|
| rtnetlink | 0.23.0 | MIT | 2026-08-18 | 24.6M / 4.1M | **primary** — async route/link/addr/rule manipulation + `RTMGRP_*` multicast-group change notifications. |
| netlink-packet-route | 0.33.0 | MIT | 2026-08-18 | 33.2M / 7.8M | transitive (rtnetlink's message types), **primary**. |
| netlink-sys | 0.9.0 | MIT | 2026-08-18 | 34.6M / 8.3M | transitive (tokio-integrated netlink socket), **primary**. |
| neli | 0.7.4 | BSD-3-Clause | 2026-01-28 | 39.1M / 10.0M | runner-up, reject as primary — lower-level message building; would mean reimplementing route/addr convenience calls rtnetlink already provides. |
| socket2 | 0.6.5 | MIT/Apache-2.0 | 2026-07-13 | 1128.2M / 275.5M | **primary** — `SO_BINDTODEVICE`, `IP_MULTICAST_IF`/`IPV6_MULTICAST_IF`, `SO_BROADCAST`, `SO_REUSEADDR` for the per-NIC broadcast/multicast dispatcher that replaces `zcd`. |
| nix | 0.31.3 | MIT | 2026-05-11 | 741.3M / 164.5M | **primary** — `unshare`/`setns` (netns test harness), signal handling, ioctl fallback. |
| tun-rs | 2.8.8 | Apache-2.0 | 2026-07-21 | 454.2k / 212.2k | **primary** — cross-platform TUN/TAP, native async (tokio) feature, actively released. |
| tun | 0.8.14 | WTFPL | 2026-07-21 | 2.45M / 505.6k | runner-up, reject as primary — WTFPL is an unusual license to mix into a GPLv3-lineage project's dependency tree; no native tokio integration. |
| tun2 | 4.0.0 | WTFPL | 2024-10-27 | 346.0k / 55.0k | reject — stale since 2024, same WTFPL concern, lower adoption. |
| if-addrs | 0.15.0 | MIT/BSD-3-Clause | 2026-02-08 | 25.6M / 5.4M | reject as separate dep — redundant `getifaddrs(3)` code path; use rtnetlink's own `GetLink`/`GetAddress` so there is one source of truth for interface state. |
| if-watch | 3.2.2 | MIT/Apache-2.0 | 2026-03-03 | 17.1M / 2.3M | reject — redundant with rtnetlink's own link/addr multicast-group subscription; its cross-platform abstraction buys nothing on a Linux-only target. |
| pnet | 0.35.0 | MIT/Apache-2.0 | 2024-05-30 | 19.3M / 5.8M | reject — raw L2 frame crafting not needed (transport is UDP/TCP, not raw Ethernet); also 2yr+ stale. |
| mdns-sd | 0.21.0 | Apache-2.0/MIT | 2026-08-12 | 4.4M / 1.8M | reject — implements DNS-SD/mDNS (RFC 6762/6763) semantics; Netsukuku's own hook/ETP discovery datagrams are a different, incompatible format — would add weight with no protocol fit. |
| multicast-socket | 0.3.3 | MIT/Apache-2.0 | 2024-04-27 | 23.9k / 554 | reject — near-zero adoption (23.9k downloads), stale since 2024; socket2 trivially replicates its single-socket-multi-iface trick. |

### RPC wire format (tradeoff verdict for a versioned, multi-year P2P protocol)

| crate | version | license | date | dl (total/recent) | verdict |
|---|---|---|---|---|---|
| serde | 1.0.229 | MIT/Apache-2.0 | 2026-07-18 | 1298.4M / 276.6M | **primary** — (de)serialization trait layer under whichever format(s) are chosen. |
| prost | 0.14.4 | Apache-2.0 | 2026-06-07 | 545.6M / 124.8M | **primary for inter-node wire RPC**. |
| prost-build | 0.14.4 | Apache-2.0 | 2026-06-07 | 336.8M / 69.1M | **primary** — build-time `.proto` → Rust codegen. |
| protox | 0.9.1 | MIT/Apache-2.0 | 2025-12-02 | 13.8M / 3.0M | **primary** — pure-Rust `protoc` replacement for `prost-build`; no system C `protoc` binary needed, matters for embedded/router cross-compiles (OpenWrt-class targets). |
| postcard | 1.1.3 | MIT/Apache-2.0 | 2025-07-24 | 54.6M / 20.6M | **secondary** — serde-native, `no_std`-friendly, compact but *not* self-describing; use only for same-binary-version serialization (local persisted state), never wire RPC between independently-upgraded nodes. |
| bincode | 3.0.0 | MIT | 2025-12-16 | 299.8M / 57.3M | reject for wire RPC — non-self-describing, no schema evolution; **v3 is a brand-new breaking rewrite (released 2025-12)**, own `Encode`/`Decode` traits replacing the old serde-only API, ecosystem/tutorials still mostly on 1.x/2.x — immaturity trap. |
| rmp-serde | 1.3.1 | MIT | 2025-12-23 | 122.4M / 24.2M | runner-up — self-describing MessagePack, tolerates unknown-field skip, easy migration story; reject as primary only because it lacks protobuf's explicit field-tag versioning discipline; keep as fallback if protobuf build tooling proves too heavy for a constrained target. |
| capnp | 0.27.0 | MIT | 2026-08-02 | 13.9M / 2.3M | reject — zero-copy is attractive but IDL toolchain + far smaller Rust ecosystem footprint (13.9M vs prost's 545.6M downloads) don't justify the complexity here. |
| flatbuffers | 25.12.19 | Apache-2.0 | 2025-12-19 | 94.5M / 19.8M | reject — same rationale as capnp; optimized for zero-copy random-access reads, which RPC request/response messages don't need. |

**Verdict**: `prost` + `protox` (pure-Rust build pipeline, no system `protoc`) for the inter-node RPC envelope
(method id/name + versioned payload). This directly replaces `ntkdrpc`'s hand-rolled JSON stub/skeleton codegen
(`ntkdrpc/interfaces.vala`, `ntkdrpc/common_helpers.vala`) and `zcd`'s `json-glib` wire format
(`zcd/json_handling.vala`, `zcd/rpcdesign_common_helpers.vala`) — neither of which carries any schema-versioning
discipline at all (plain JSON objects, "hope the other end still has the same fields"). Protobuf's field-number
based forward/backward compatibility is a real requirement for Netsukuku specifically, because unlike a
company's microservice fleet, independently-run mesh nodes are *never* upgraded in lockstep. `postcard`
remains for purely-local serialization where both ends are always the same binary.

### Crypto / identity

| crate | version | license | date | dl (total/recent) | verdict |
|---|---|---|---|---|---|
| ed25519-dalek | 3.0.0 | BSD-3-Clause | 2026-07-06 | 193.2M / 51.2M | **primary** — node identity keypair, signing (identity/pseudo-address authentication). |
| x25519-dalek | 3.0.0 | BSD-3-Clause | 2026-07-06 | 67.2M / 16.4M | **primary** — ephemeral ECDH for the Noise handshake. |
| snow | 0.10.0 | Apache-2.0/MIT | 2025-07-19 | 25.7M / 3.7M | **primary** — Noise Protocol Framework transport encryption + mutual auth *without a CA*, matching Netsukuku's self-sovereign identity/pseudo-address model far better than X.509. |
| blake3 | 1.8.7 | CC0-1.0/Apache-2.0(+LLVM-exception) | 2026-08-20 | 167.7M / 39.4M | **primary** — fast, modern fingerprint/hash primitive (replaces whatever ad hoc hash the Vala `IQspnFingerprint` implementers used; algorithm choice itself is out of this note's scope). |
| noise-protocol | 0.2.1 | Unlicense | 2026-03-11 | 244.6k / 53.9k | reject — far lower adoption/maintenance signal than `snow` (244.6k vs 25.7M downloads). |
| rustls | 0.23.43 | Apache-2.0/ISC/MIT | 2026-07-29 | 852.1M / 186.9M | **deferred, not core** — only relevant if a TLS-terminated bootstrap/web gateway is added later; core mesh RPC uses Noise via `snow`, not CA-based TLS. |
| ring | 0.17.14 | Apache-2.0 AND ISC | 2025-03-11 | 687.6M / 148.4M | not directly needed; only a transitive dep if `rustls` is later adopted. |

Note: `ed25519-dalek` and `x25519-dalek` both jumped to `3.0.0` the same release day (2026-07-06) — almost
certainly a coordinated `dalek-cryptography` workspace bump `[INFERENCE — inferred from identical release
dates, not independently confirmed via a changelog]`. Verify `curve25519-dalek`/`signature` trait version
alignment (and `snow`'s own `x25519-dalek` pin, if any) before committing versions in Cargo.toml.

### Errors, tracing, metrics, config, CLI

| crate | version | license | date | dl (total/recent) | verdict |
|---|---|---|---|---|---|
| thiserror | 2.0.20 | MIT/Apache-2.0 | 2026-08-08 | 1348.9M / 331.6M | **primary** — per-crate error enums (library crates: `ntk-qspn`, `ntk-hooking`, …). |
| anyhow | 1.0.104 | MIT/Apache-2.0 | 2026-07-18 | 893.1M / 197.0M | **primary** — top-level `ntkd` binary error handling / `main() -> anyhow::Result<()>`. |
| tracing | 0.1.44 | MIT | 2025-12-18 | 788.8M / 176.8M | **primary**. |
| tracing-subscriber | 0.3.23 | MIT | 2026-03-13 | 562.5M / 134.9M | **primary**. |
| metrics | 0.24.6 | MIT | 2026-05-13 | 101.1M / 18.3M | **primary** — facade macros, keeps `ntk-*` library crates exporter-agnostic. |
| metrics-exporter-prometheus | 0.18.3 | MIT AND Apache-2.0 | 2026-04-30 | 42.9M / 11.5M | **primary** — Prometheus exporter wired only in the `ntkd` binary. |
| prometheus | 0.14.0 | Apache-2.0 | 2025-03-27 | 137.4M / 25.3M | reject as primary — couples metric *definition* directly to the Prometheus client instead of the facade; would leak an exporter choice into every library crate. |
| toml | 1.1.4+spec-1.1.0 | MIT/Apache-2.0 | 2026-07-28 | 837.9M / 196.7M | **primary**, paired with `serde` — one `ntkd.toml`. |
| figment | 0.10.19 | MIT/Apache-2.0 | 2024-05-17 | 35.1M / 8.6M | reject — 2yr+ stale; layered multi-source config machinery solves a problem this daemon doesn't have. |
| config | 0.15.25 | MIT/Apache-2.0 | 2026-06-26 | 107.9M / 18.3M | reject — same rationale as `figment`. `ntkd/configuration.vala:31-68` shows the actual config surface is a small, hard-coded/local struct (gsize/level topology + NIC name list), not a layered env/profile system. |
| clap | 4.6.6 | MIT/Apache-2.0 | 2026-08-06 | 1065.7M / 219.1M | **primary** — derive API for CLI flags (`--config`, `--dev`, `--netns`, log level). |

### Testing / simulation

| crate | version | license | date | dl (total/recent) | verdict |
|---|---|---|---|---|---|
| proptest | 1.11.0 | MIT/Apache-2.0 | 2026-03-24 | 172.7M / 44.5M | **primary** — pure algorithmic invariants: NIP encode/decode round-trip, fingerprint-merge idempotency, ETP TTL/hop-count bounds, PeerServices version-vector merge idempotency. |
| turmoil | 0.7.2 | MIT | 2026-04-24 | 15.9M / 0.5M | **REJECTED 2026-08-26, not adopted** — investigated against the real code and unusable here: no `TcpSocket`, no `bind_device`, no `UdpSocket::from_std`, no concept of more than one NIC per host, all of which `ntk-rpc` requires. `SO_BINDTODEVICE` is load-bearing (link-local `169.254/16` is per-link by definition), so a simulator that cannot represent a NIC cannot host the tests that matter most. See "Why `turmoil` was rejected". |
| madsim | 0.2.34 | Apache-2.0 | 2025-10-11 | 6.4M / 0.7M | **REJECTED 2026-08-26, not adopted** — whole-runtime `[patch]`-based tokio replacement across twelve crates, still cannot cover netlink. Declined even though turmoil's failure technically fired its revisit trigger: the determinism it buys (arbitrary interleaving replay) is not the determinism this codebase needed. |
| loom | 0.7.2 | MIT | 2024-04-23 | 60.1M / 11.2M | optional/defensive — only relevant if hand-rolled unsafe/lock-free code appears; the concurrency strategy below avoids that by design (single-owner actors, no shared mutable state), so not a day-1 dependency. |
| netns-rs | 0.2.0 | Apache-2.0 | 2026-03-24 | 869.0k / 167.9k | reject — thin wrapper around `unshare(2)`/`setns(2)`; `nix` is already a hard dependency (sockets/TUN), so drive namespaces directly through `nix::sched` instead of adding a second crate for the same two syscalls. |

## Proposed workspace layout

One crate per Vala module family, `ntk-` prefixed, plus two crates with no direct Vala analogue
(`ntk-netlink`, `ntk-crypto`) that factor out cross-cutting concerns the Vala tree left inline or shelled out to `ip`/`iptables`:

```
netsukuku-rs/
  crates/
    ntk-common/        # ~ ntkd-common: NIP, level/gsize params, shared error types
    ntk-crypto/         # NEW: ed25519-dalek + x25519-dalek + snow + blake3 behind a small
                         #      identity/handshake trait — isolates crypto-crate choice
    ntk-proto/          # generated prost types + wire envelope (replaces ntkdrpc's
                         #      interfaces.vala/stub/skeleton codegen)
    ntk-rpc/            # ~ zcd: transport — per-NIC unicast (TCP) + broadcast/multicast
                         #      (UDP via socket2), trait-based method dispatch registry
    ntk-netlink/         # NEW: rtnetlink + socket2 + nix wrapped as RouteTable/TunDevice
                         #      traits — replaces ip/iptables subprocess calls
                         #      (ntkd/identity_ip_commands.vala:36-56)
    ntk-neighborhood/    # ~ neighborhood: arc/link discovery
    ntk-identities/      # ~ identities: pseudonym/identity lifecycle, NIP allocation
    ntk-qspn/            # ~ qspn: routing core
    ntk-hooking/         # ~ hooking: bootstrap/join sequence
    ntk-peerservices/    # ~ peerservices: generic p2p consensus substrate
    ntk-coordinator/     # ~ coordinator: depends on ntk-peerservices
    ntk-andna/           # ~ andna: depends on ntk-peerservices
  bin/
    ntkd/               # ~ ntkd: binary — tokio runtime, clap CLI, toml config,
                         #      tracing/metrics init, JoinSet supervisor + CancellationToken tree
```

Dependency direction (arrows = "depends on"), generalizing the literal `depends on:` lines each Vala
README stated:

```
ntk-common  ──────────────────────────────────────────────► (everyone)
ntk-crypto  ──► ntk-rpc, ntk-hooking, ntk-identities
ntk-netlink ──► ntk-neighborhood, ntkd (bin)
ntk-proto   ──► ntk-rpc
ntk-rpc     ──► ntk-neighborhood, ntk-identities, ntk-qspn, ntk-hooking, ntk-peerservices
ntk-neighborhood ──► ntk-identities, ntk-hooking
ntk-identities   ──► ntk-qspn, ntk-hooking, ntk-coordinator, ntk-andna
ntk-qspn         ──► ntk-hooking, ntk-coordinator, ntkd
ntk-peerservices ──► ntk-coordinator, ntk-andna
ntk-hooking, ntk-coordinator, ntk-andna ──► ntkd (bin, composes all identities)
```

This mirrors the original topology exactly: `qspn`/`identities`/`neighborhood`/`hooking`/`peerservices` are
siblings depending only on shared base libraries; `coordinator` and `andna` are the only modules that further
depend on `peerservices`; `ntkd` is the sole composition root (previously via git submodules, `ntkd/.gitmodules:1-18`).

**Where Rust traits replace Vala's interface/stub-skeleton indirection**: every Vala module exposed an
`api.vala`/`interfaces.vala` (e.g. `hooking/api.vala`, `coordinator/api.vala`) plus a matching stub/skeleton
pair generated by `zcd`'s `rpcdesign.vala` purely so that a module could call another module's methods
*as if* they were remote — even when both were compiled into the same `ntkd` binary. In Rust this collapses to
two, deliberately different, mechanisms:
1. **In-process calls** between crates linked into the same binary are plain trait objects / async fn calls —
   no codegen, no serialization, no "as if remote" ceremony.
2. **Actually-remote calls** (peer daemon to peer daemon) go through `ntk-rpc`'s real transport and `ntk-proto`'s
   real wire format — this is the only place serialization belongs, because it is the only place data actually
   crosses a process/machine boundary.
The trait boundary that *does* carry over 1:1 is `IQspnStubFactory`-style factories (e.g. `qspn/qspn.vala:104`,
`164`) becoming a `trait RpcClient` implemented once for the real `ntk-rpc` transport and once for an
in-memory fake used by unit/turmoil tests — same purpose (testability / substitutability), no codegen needed
to achieve it in Rust.

## Concurrency / ownership strategy for QSPN routing state

**Decision: single-owner actor task per identity + message passing (mpsc command queue + oneshot replies),
not `Arc<RwLock<QspnState>>`.**

Rationale, grounded in the Vala source:
- `qspn/qspn.vala` mutates shared collections (`destinations: ArrayList<HashMap<int, Destination>>`,
  `my_fingerprints`, `my_arcs`, `arc_to_naddr`) from many call sites (ETP retrieve, arc up/down, hook
  completion, `on_bootstrap_complete`) with **no explicit locking anywhere in the file** — confirmed by
  grep, zero `Mutex`/`lock(` hits.
- This is safe in the original only because `pth-tasklet` (`pth-tasklet/README:3`, `libwrappth.vala`) is a
  cooperative scheduler on a single OS thread: a tasklet runs to its next explicit yield point uninterrupted,
  so every `QspnManager` method body is an implicit critical section for free.
- Porting this to tokio's real multi-threaded work-stealing scheduler with `Arc<RwLock<QspnState>>` would
  silently reintroduce exactly the races the cooperative model prevented (e.g. an ETP handler iterating
  `destinations[lvl]` while a concurrent arc-down handler mutates the same map) unless the lock were held for
  entire multi-step method bodies — which defeats `RwLock`'s point and reintroduces cross-module lock-ordering
  hazards at the `qspn`/`hooking`/`coordinator` boundary.
- A single task processing commands off an `mpsc` queue serially reproduces the same "one logical thread of
  control mutates this object" invariant pth gave for free, while the rest of the daemon (RPC listeners,
  netlink watcher, other identities' actors) still runs in real parallel on tokio's thread pool. It also gives
  a natural home for the per-identity `CancellationToken`: `qspn.vala`'s `remove_identity` signal becomes
  "cancel this actor's token, `JoinSet` reaps it."
- Where read-mostly consumers need cheap concurrent snapshots (e.g. an introspection/metrics endpoint reading
  the routing table), the owning actor publishes an immutable snapshot via `tokio::sync::watch::channel` after
  each processed command — no extra crate (`arc-swap` etc.) needed, `tokio::sync::watch` already ships in the
  chosen `tokio` feature set.

One actor instance exists per **identity** (Netsukuku nodes can host several simultaneous pseudo-addresses,
per the `identities` module), each with its own `destinations`/`my_fingerprints` state, its own child
`CancellationToken`, and its own entry in the top-level `JoinSet` owned by `ntkd`'s supervisor.

## Deterministic-simulation testing plan

> **Superseded 2026-08-26 by implementation experience. `turmoil` was investigated against the real code and
> REJECTED; `madsim` was rejected with it. Items 1, 2 and 5 below are the original proposal, kept because the
> reasoning is still instructive, but they are NOT what the port does. Read
> "What replaced it" at the end of this section before acting on any of this.**

1. ~~**Protocol-level simulation (primary tool: `turmoil`).**~~ Multi-node scenarios — qspn convergence after an
   arc flap, the full hooking join sequence, peerservices consensus under partition, andna registration races —
   run the real `ntk-qspn`/`ntk-hooking`/`ntk-peerservices` actor code against turmoil's simulated
   `TcpListener`/`UdpSocket`, with injected latency/jitter/partitions, seed-reproducible for regression tests
   on any failure turmoil finds.
2. **Trait boundary is load-bearing for simulation coverage.** `ntk-netlink` exposes its kernel traits and the
   in-process tiers substitute an in-memory fake (no real kernel calls) so the qspn/hooking/peerservices logic
   under test never touches real netlink. **This item held up completely** and is the single most valuable
   prediction in this note — the boundary did exist from the first commit of `ntk-netlink`, and every
   in-process multi-node test depends on it. (The `TunDevice` half is void: there is no TUN device, per
   `03-specs-and-rfcs.md` and AGENTS.md.)
3. **`proptest`** covers pure, non-networked invariants inside each crate. Held up; concentrated in
   `crates/ntk-qspn/tests/proptest_invariants.rs`.
4. **Real-kernel integration harness (netns, not simulated).** Held up, and grew beyond what was predicted:
   real rtnetlink route tables across veth pairs, plus a `mac80211_hwsim` 802.11 tier the note never
   anticipated. Built with `nix::sched::unshare(CLONE_NEWNET)` + `rtnetlink`, never `ip netns`. Gated behind
   `#[ignore]`. This tier found the large majority of real defects.
5. ~~**`madsim` deferred.**~~ Its `[patch]`-based whole-runtime tokio replacement is heavier to roll out across a
   multi-crate workspace than turmoil's incremental per-crate adoption, and it still can't simulate netlink
   (tier 4 is unavoidable either way) — revisit only if turmoil's fault-injection depth proves insufficient.

### Why `turmoil` was rejected

turmoil 0.7.2 has no `TcpSocket`, no `bind_device`, no `UdpSocket::from_std`, and no concept of more than one
NIC per host. `ntk-rpc` needs all of them:

- `crates/ntk-rpc/src/client.rs` dials with `TcpSocket::new_v4()` + `socket.bind_device(..)` when a device is
  given, and its public `from_stream` takes a concrete `tokio::net::TcpStream`.
- `crates/ntk-rpc/src/broadcast.rs` builds a `socket2::Socket`, sets `SO_BROADCAST`/`SO_REUSEADDR` and
  optionally `SO_BINDTODEVICE`, then converts it with `UdpSocket::from_std`.

`SO_BINDTODEVICE` is not incidental — it is load-bearing protocol behaviour. Netsukuku addresses peers on
`169.254.0.0/16` link-local, which is per-link **by definition** (RFC 3927), so with two or more monitored NICs
sharing that one prefix the kernel's ordinary route lookup cannot disambiguate a peer. That was a real,
diagnosed bug; per-device binding fixed it, and `crates/ntkd/tests/multi_nic_relay.rs` exists to keep it fixed.
A simulator that cannot represent a NIC cannot host the tests that matter most here.

`madsim` was rejected for the reason item 5 already gave — a whole-runtime `[patch]` across twelve crates that
still cannot cover netlink. Note the trigger in item 5 ("revisit only if turmoil's fault-injection depth proves
insufficient") technically fired, since turmoil turned out unusable rather than merely shallow. It was still
declined, deliberately: see the next section for why the determinism it would buy is not the determinism this
codebase needed.

### What replaced it: deterministic fault injection, layered by defect class

The premise behind item 1 was that reproducing timing races needs a deterministic *scheduler*. Implementation
experience says otherwise. Every hard-to-reproduce defect in this port was ordinary logic mishandling a specific
observable **fault**; the race only decided whether that fault arrived. So the port injects faults
deterministically instead of controlling interleaving, using the seams that already exist:

| defect class | tool | example it caught |
|---|---|---|
| transport / RPC faults | scriptable faults on `FakeRpcClient` (`with_failure_at`, `with_failures_for`) | one transient `Unreachable` permanently killing an arc handler |
| ordering invariants | synchronous actor-level invariant tests | a `watch` snapshot published *after* the flag observers read, leaving a route uninstalled |
| multi-member / routing faults | routing control on `ntkd`'s in-memory `Medium` | an enter-election split across two g-node members |
| real kernel / NIC behaviour | the real-kernel tier (tier 4 above) | `/32` link-local killing broadcast; per-NIC dial; IBSS cells that cannot merge |

The ordering-invariant row is the one that refutes item 1's premise most directly. A race where an observer must
wake *between* a flag flip and a publish looks like it demands scheduler control, and does not: assert instead
that the flag cannot be observed before the data, and the interleaving becomes harmless. See
`qualifying_bootstrap_etp_publishes_snapshot_before_flipping_complete` in `crates/ntk-qspn/src/manager.rs` —
fully deterministic, no scheduler control, and it fails against the pre-fix code.

What this deliberately does **not** buy: a seed that replays an arbitrary interleaving. Gating anything touching
the real-kernel tier still means running it 12–20+ times, and that convention stands — one regression in this
lineage surfaced only at run 12 of a 12-run stress.

## IPv6: investigated, declined (2026-08-27)

`crates/ntkd/src/kernel/addressing.rs` packs a NIP into `10.0.0.0/8` (kind 0/global only, 24 usable
bits after the fixed `10` octet). Whether that module should grow an IPv6 counterpart was investigated
directly against the normative source (`AGENTS.md`'s Vala-is-normative rule, not the legacy C tree) and
declined:

- The Vala tree (`research/impl/vala/`) contains only `ipv4_compute.vala` — copies across `proof/`
  (including its own `testsuites/ipv4compute/`), `ntkd/` (and its `tester_01`–`tester_05` subtrees),
  `system-ntkd/`, and the `sys-ntkd-*` trees, all the same module. `research/impl/vala/**/ipv6_compute.vala`
  matches nothing — there is no vala-era IPv6 address-computation counterpart anywhere in the corpus.
- Every IPv6 mention that *does* exist in the vendored corpus is legacy C-era material, not vala-era design:
  - `documentation/old_doc/main_doc/ntk_rfc/Ntk_andna_and_dns:409-410` — an ANDNA record-length aside ("4 in
    IPv4, 16 in IPv6"), not an addressing scheme.
  - `documentation/old_doc/misc/Ntk_scalability:151` — a QSPN v1 worst-case map-size table, already
    superseded on every count by `qspn.pdf`/QSPN v2 per this note's sibling, `03-specs-and-rfcs.md:59`.
  - `documentation/old_doc/manuals/ntkd:33-35` — the **C daemon's own man page**, which flags its `-6,
    --ipv6` flag as "still experimental" even there.
- Practical consequence for this crate: `total_bits` (`addressing.rs`) enforces a hard 24-bit packing
  budget and returns `AddressingError::TopologyTooWide` rather than silently mispacking a topology that
  does not fit — a deep or wide `gsizes[]` configuration is rejected outright, the same discipline an
  IPv6-shaped budget would need. Nothing in either corpus describes what that budget or its bit layout
  should be for IPv6.

Building it anyway would mean inventing new protocol design under the `netsukuku-rs` name, not porting the
normative source — declined for that reason, matching the same normative-source discipline
`AGENTS.md:9-13` already states for the C tree generally. Revisit only if a vala-era `ipv6_compute.vala`
equivalent surfaces from a source not yet vendored here.

## Open questions / risks for the Rust port

1. `protox`'s proto3 feature coverage (oneof, optional, well-known types) must be checked against whatever
   `.proto` schema style the RPC-message design ultimately needs, before committing to the pure-Rust
   `prost`+`protox` build pipeline over a system-`protoc` based one.
2. `bincode` 3.0.0 was released 2025-12-16 — under a year old at scan time, a breaking rewrite of the crate's
   own API. Re-check crates.io immediately before implementation for further breaking point-releases or a
   yank; `postcard` is the safer internal-serialization fallback regardless.
3. `ed25519-dalek` and `x25519-dalek` both jumped to `3.0.0` on the same day — verify the full
   `dalek-cryptography` workspace (`curve25519-dalek`, `signature` trait crate) and `snow`'s own dependency pin
   resolve without conflict before pinning versions; a coordinated major-version bump is exactly when a
   downstream crate (`snow`) can lag.
4. `snow`'s Noise pattern choice (XX/IK/NK/etc.) must be tied to Netsukuku's identity/pseudo-address model —
   that decision belongs to whichever note defines the hooking handshake, not this stack note.
5. `ntkd/identity_ip_commands.vala:53-56` uses `iptables -t nat` SNAT/NETMAP rules for the anonymizing-address
   feature. No native-Rust nftables/iptables-netlink crate (e.g. `rustables`) was evaluated in this pass —
   needs a dedicated look before the anonymization/subnet-level feature is ported; naive rtnetlink alone does
   not cover NAT table manipulation.
6. Re-check `metrics-exporter-prometheus` (0.18.3, 2026-04-30) and `prometheus` (0.14.0, 2025-03-27) release
   cadence closer to implementation time — both show longer gaps between releases than `tokio`/`serde`-tier
   crates; not a rejection, just a freshness flag.
7. License mix: `blake3`'s CC0-1.0/Apache-2.0(+LLVM-exception) options, `ed25519-dalek`/`x25519-dalek`'s
   BSD-3-Clause, and `tun` family's WTFPL (rejected above partly for this reason) all need reconciling against
   whatever license `netsukuku-rs` itself picks, given the Vala/C lineage is GPLv3 — that decision belongs to
   whoever owns licensing, not this note.
8. This note did not evaluate a QUIC-based transport (e.g. `quinn`) as an alternative to raw TCP+Noise for
   unicast RPC; if wide-area (non-link-local) bridging becomes a real requirement, QUIC's built-in
   multiplexing/0-RTT could change the `ntk-rpc` transport verdict — flagged, not decided, here.
