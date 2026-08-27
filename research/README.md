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

## Clone list

Confirmed working 2026-08-27. Shallow (`--depth 1`) is enough for everything except the C
daemon: it's pinned to a historical commit, and a shallow clone only guarantees *some* recent
commit, not that specific one (it happens to equal current HEAD today — no drift found this
pass — but that's not guaranteed to stay true, since the repo is still active). Everything
else can restore in seconds; the C daemon's full history is the only part worth the ~2 minutes
it takes (124 MB vs. 21 MB for all 18 vala clones combined).

`impl/vala/` — flat, one directory per repo, named after the repo:

```sh
for r in documentation ntkd ntkd-common qspn neighborhood peerservices hooking proof \
         coordinator andna zcd ntkdrpc pth-tasklet identities tasklet-system system-ntkd \
         sys-ntkd-test1 sys-ntkd-alone; do
  git clone --depth 1 "https://github.com/lukisi/$r" "impl/vala/$r"
done
```

18 repos, matching the Layout count above. `lukisi` has 25 public repos total; the 7 excluded
are `ntkd-snapcraft`/`qspnclient-snapcraft` (packaging, not source) and 5 unrelated personal
projects (`wardrobe`, `eagle-rdp-note`, `iotalib-vala`, `allstingycookies`, `woloo-note`).

`impl/c/netsukuku` — full clone, then pin:

```sh
git clone https://github.com/Netsukuku/netsukuku impl/c/netsukuku
git -C impl/c/netsukuku checkout 886a24a
```

`related/`:

```sh
git clone --depth 1 https://github.com/d0p1s4m4/karkinos related/karkinos
git clone --depth 1 https://github.com/yggdrasil-network/yggdrasil-go related/yggdrasil-go
git clone --depth 1 https://github.com/cjdelisle/cjdns related/cjdns
git clone --depth 1 https://github.com/vg/netsukuku related/netsukuku
```

Plus `related/rfc8966.txt`, fetched verbatim from `https://www.rfc-editor.org/rfc/rfc8966.txt`
(not a clone — a single-file text/plain GET).

`specs/` is not a clone either — it's 106 files copied out of `impl/vala/documentation` and
`impl/c/netsukuku/doc` once those two exist, named `vala-doc--<path>` / `c-doc--<path>` with
every `/` in the origin path turned into `-` and the extension kept (a handful of origin-path
collisions get a manual disambiguating suffix, e.g. `-index` vs. `-overview` for two different
`old_doc/main_doc/netsukuku*` files) — see `notes/03-specs-and-rfcs.md`'s document table for
the exact file-by-file mapping, which is the actual source of truth this was regenerated from.

`papers/` (17 files, 3.3 MB) come from the URLs in `notes/04-bibliography.md`, not from a repo:
4 arXiv PDFs (`arxiv.org/pdf/<id>`, ids `0705.0815`/`0817`/`0819`/`0820`), 5 website-edition
PDFs (`netsukuku.freaknet.org/doc/main_doc/{netsukuku,qspn,topology,andna,inetntk}.pdf` — the
dynamic PHP frontend is dead, static paths still resolve), RFC 0011/0012/0013 + the unnumbered
Viphilama-Static companion (`lab.dyne.org/Ntk_{carciofo,net_split,caustic_routing}` wiki pages,
still live via `?action=raw`; Viphilama-Static and RFC 0014 are just copies of the vala tree's
own `old_doc/main_doc/ntk_rfc/{Ntk_viphilama_static,Ntk_p2p_over_ntk.pdf}` — both already in
`impl/vala/`, not actually website-only despite this note's own bibliography claiming so), the
2010 Cambridge thesis from both hosts (`archive.org` item
`scalable_mesh_networks_and_the_address_space_balancing_problem-andrea_lo_pumo`, 647,116 bytes;
the author's own `lab.dyne.org/Netsukuku_Tesi2010` wiki upload via
`?action=AttachFile&do=get&target=scalable-mesh-networks.pdf`, 658,039 bytes — same content,
different PDF export pass, not a revision), and DART (`cs.ucr.edu/~krish/dart_ton_2006.pdf`).

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
