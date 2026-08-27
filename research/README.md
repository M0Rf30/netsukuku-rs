# netsukuku-rs — research corpus

  - Close the world, txEn eht nepO -

--

This is the notebook, not the net. Everything a Rust module cites by `path:line`
lives under here, kept read-only so the citation stays true a year from now.
Only `notes/` and this file are ours; the rest — `impl/`, `related/`, `papers/`,
`specs/` — is vendored third-party material, gitignored, and regenerable:
`impl/` and `related/` from the clone list below, `specs/` by copying the doc
trees out of those clones, `papers/` from the URLs in `notes/04-bibliography.md`.
Every note cites upstream by `path:line`, so the notes stay readable with the
corpus gone.

## Layout

| Path | Content |
|---|---|
| `notes/01-vala-core-routing.md` | QSPN v2, Neighborhood, Identities, Hooking, Coordinator, RPC surface (480 lines) |
| `notes/02-vala-services-daemon.md` | zcd wire protocol, tasklets, PeerServices DHT, ANDNA status, kernel side effects, test harnesses (411) |
| `notes/03-specs-and-rfcs.md` | Document inventory, NTK_RFC enumeration, legacy-vs-current divergence, normative recommendation (154) |
| `notes/04-bibliography.md` | Papers retrieved + dead ends (97) |
| `notes/05-related-art.md` | Yggdrasil/ironwood, cjdns, Babel, BATMAN-adv, OLSRv2, Kademlia; karkinos post-mortem (168) |
| `notes/06-rust-stack.md` | Latest-stable crate table, workspace layout, concurrency + simulation plan (296) |
| `specs/` | 106 verbatim spec/doc artifacts (`vala-doc--*` = 85, `c-doc--*` = 21) |
| `papers/` | 17 files, 3.3 MB — 4 arXiv preprints, 5 website-edition PDFs, the 2010 Cambridge thesis (x2 hosts), RFC 0011-0014, DART (ToN 2006) |
| `impl/vala/` | 18 clones, lukisi (Luca Dionisi) Vala rewrite 2017-2020 — **normative implementation** |
| `impl/c/netsukuku` | Netsukuku/netsukuku "revived" Npv7 C daemon, HEAD 886a24a — historical only |
| `related/` | karkinos (prior Rust attempt), yggdrasil-go, cjdns, vg/netsukuku (contains pyntk + simulator), rfc8966.txt |

## What is actually current

Three documentation strata (`notes/03`): Alpt-era monolithic draft (2005-09) → split PDFs
(topology / QSPN v2 / ANDNA / inetntk, 2007-09) → **lukisi `ita/` module design docs (2017-2020),
which are normative**. The C "revived" repo's 199 commits touch `doc/` exactly once (2013 import);
all 2025 activity is build/CI — it keeps legacy Npv7_HT compiling and tracks no newer spec. Read
oldest to newest and you watch the same net get re-drawn twice; the drift between drafts is itself
a finding, cross-checked below.

Address model diverged: legacy = raw IP split at fixed `MAXGROUPNODE=256` per level; current =
`Naddr{pos[], sizes[]}` with per-level arbitrary `gsize(i)`, a separate `Fingerprint` for
identity/age, and virtual positions during migration. (`NIP` is our shorthand — the string does not
exist upstream.)

Legacy monolith's Radar + Hook/Unhook + ad-hoc ANDNA hash-node placement is now a clean 3-way split:
`neighborhood` + `hooking`/`coordinator` + `peerservices`.

## Load-bearing findings

These are the claims the Rust port is built on. Doubt one, go re-read the citation before you doubt
the code.

- **QSPN v2 has no withdraw message.** Dead paths are inferred from absence in a full ETP
  (`qspn.vala:1074-1232`). `update_map` (`:1334-1816`) is the highest-risk algorithm: disjoint-path
  admission via a size/gateway-adaptive max-common-hops ratio plus elder-fingerprint gating.
- **Arc liveness is TCP `nop()` every 28-30 s**, not RTT; cost uses asymmetric EMA with 2x hysteresis.
- **Identities exist only for live g-node migration**: node forks a connectivity identity (keeps
  external arcs via pseudodevices/netns) while the new identity re-hooks.
- **Coordinator is a PeerService**, elected by DHT hash (`perfect_tuple` = zeros → position-0/eldest),
  the only core module with a real library dependency.
- **ANDNA is unimplemented in Vala**: `andna.vala` is a 13-line stub, `serializables.vala` is 0 bytes,
  no `AndnaManager` in the RPC IDL. Its design must come from the C impl + RFC 0009/0014. RFC 0014 is
  the only formal spec of the generic DHT layer and states ANDNA is two instances of it.
- **Daemon wiring is unfinished upstream**: `ntkd/startup.vala` ends at `// TODO continue`; no
  steady-state loop for new NICs/arcs. IPv6 absent everywhere. Kernel state is driven by shelling out
  to `ip(8)`/`iptables(8)`/`sysctl(8)`.
- **Netsukuku is an L3 routing protocol, not a TUN overlay** — unlike Yggdrasil/cjdns it owns the real
  routing tables (needs `CONFIG_IP_MULTIPLE_TABLES`, `IP_ROUTE_MULTIPATH`). Explicit design fork to decide.
- **karkinos (the only prior Rust attempt) is empty**: one commit, a 23-line bitflags stub, zero
  networking. `vg/netsukuku`'s vendored `pyntk` (with `ntk/sim` simulator) is far better prior art.
- **No peer-reviewed analysis of QSPN/ANDNA exists** — only the 2010 Cambridge thesis (formal proofs,
  Preemptive/Last-Minute balancing rules, O(√N) migration bound) and DART (ToN 2006) as its sibling.
- Four RFCs live on the website but absent from the C checkout: 0011 Carciofo (anonymity), 0012 Net
  Split, 0013 Caustic Routing, plus Viphilama Static — all incomplete, post-MVP.

## Implementation baseline (`notes/06`)

Verified latest stable: tokio 1.53.1, tokio-util 0.7.19, rtnetlink 0.23.0, tun-rs 2.8.8, prost+protox
for wire RPC, postcard for internal, serde+toml for config. Rejected: bincode 3.0.0 (fresh breaking
rewrite, non-self-describing), tun/tun2 (license/abandoned), figment/config (over-spec'd for a
one-struct config surface).

Concurrency verdict: **single-owner actor task per identity + mpsc/oneshot**, not
`Arc<RwLock<QspnState>>`. Grounding: `qspn.vala` uses zero locks, safe only because pth-tasklet is a
cooperative single-threaded M:1 scheduler; a direct port onto multi-threaded tokio reintroduces races.
`tokio::sync::watch` publishes read-only route snapshots.

Testing: `turmoil` for seed-reproducible protocol-level simulation, `proptest` for pure invariants,
real netns+veth harness for netlink/TUN. Upstream's `sys-ntkd-*` pattern (AF_UNIX-emulated medium +
`FakeCommandDispatcher` asserting would-be `ip` argv against golden output) is worth replicating.
