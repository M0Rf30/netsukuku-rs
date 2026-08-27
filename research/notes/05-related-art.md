# Related Art: Comparable Systems and Prior Rust Attempts

Open this to place Netsukuku next to the mesh-routing systems it gets compared to — Yggdrasil,
cjdns, Babel, BATMAN-adv, Kademlia — and to read the post-mortem on karkinos, the one prior Rust
attempt.

Scope: systems solving the same problem class as Netsukuku (self-configuring
mesh L3 routing, decentralized addressing, name resolution without central
authority) plus every known prior attempt to implement Netsukuku in Rust.
Clones under `research/related/`: `karkinos/`, `yggdrasil-go/`, `cjdns/`,
`netsukuku/` (vg's fork). `rfc8966.txt` (Babel) fetched inline. BATMAN-adv,
OLSRv2, Kademlia/libp2p-kad studied via upstream docs/RFCs, not cloned.

## Comparison table

| System | Addressing | Routing algorithm | Per-node state | Mobility | Name service | Crypto/identity |
|---|---|---|---|---|---|---|
| **Netsukuku** (Vala/QSPN v2) | Hierarchical NIP, position in nested g-node tree (level, gsize) | QSPN: flooding ETPs build per-level topology maps; hierarchical path composition across g-nodes | O(gsize × levels) map, not O(n); collapses inner g-node detail at higher levels | Designed for ad hoc wireless; hooking protocol re-derives position on join | ANDNA: hash(hostname) → hierarchical `hash_node` walk constrained to g-node levels, replicated per-service PeerServices DB | Node identity = numeric NodeID; no mandatory PKI in QSPN core (pseudo-identities decoupled from crypto keys) |
| **karkinos** (Rust, d0p1s4m4) | Same NIP concept (unimplemented) | Unimplemented — only `bitflags` map-flags stub | none | n/a | none | none |
| **yggdrasil-go** | IPv6 in `0200::/7`, deterministically derived from ed25519 pubkey (leading-1-run compression) | Ironwood: greedy routing over spanning-tree embedding (treespace distance); bloom-filter multicast lookups for key→location, no DHT since v0.5 | O(1)-ish per peer (CRDT tree ancestry + bloom filter per link); lookups cost grows with subtree size (false-positive tradeoff) | Designed for Internet-wide overlay, peers reconnect, tree self-heals | None built-in (apps resolve pubkeys themselves) | ed25519 keypair = identity = address; nacl/box (X25519/XSalsa20/Poly1305) session encryption w/ ratcheting |
| **cjdns** | IPv6 `fc00::/8`-like, SHA-512(pubkey) truncated | Source-routed "switch" labels (stacked variable-width interface directors) for the data plane; Kademlia-style XOR-metric DHT search to discover routes, spliced into labels | DHT routing table (Kademlia buckets) + switch forwarding table per interface | Ad hoc capable but historically weaker convergence at scale (motivated Yggdrasil fork) | None built-in; NameCoin/other external systems proposed historically | Curve25519 keypair = identity; CryptoAuth (NaCl-based) hop-by-hop + end-to-end sessions |
| **vg/netsukuku** (archived C fork, 2009–2013) | Netsukuku NIP (same as C mainline) | Npv7 QSPN v1 (C) + `pyntk` — independent Python re-implementation incl. `ntk/sim` network simulator | Same as C mainline | Same as C mainline | ANDNA (C) | Same as C mainline (RSA-based node signatures in Npv7) |
| **Babel** (RFC 8966) | Any (IPv4/IPv6 prefixes, protocol-agnostic) | Distance-vector Bellman-Ford + sequenced routes + feasibility condition (loop-free without full topology) | O(neighbors × prefixes); route + source tables | Explicit design target: wireless/mobile links, fast convergence on link change | None | None (Babel-DTLS/Babel-MAC optional per RFC 8967) |
| **BATMAN-adv** | L2 (MAC-based), OGM-driven originator table | Per-originator OGM flooding w/ sequence-number sliding window + path TQ (transitive quality) product; distance-vector, no explicit topology map | O(originators × routers-per-originator); Originator List + Router List | Core design goal (mobile ad hoc, layer-2) | None (operates as an Ethernet bridge; DNS irrelevant at L2) | None in base protocol |
| **OLSRv2** (RFC 7181) | IP-based | Proactive link-state; MPR (multipoint relay) selection reduces flooding + advertised topology to a subset of links, Dijkstra over reduced topology | O(neighbors) Link/2-Hop Sets + O(MPR-selectors) Topology Set — smaller than full link-state | Ad hoc/MANET origin, tunable timers | None | None in base spec (integrity/confidentiality noted as external, RFC 7183 has TLV signing) |
| **Kademlia / libp2p-kad** | Flat keyspace, `PeerId`/key = XOR-metric coordinate | Iterative closest-peer lookup, k-buckets (`K_VALUE`, `ALPHA_VALUE` parallelism) | O(k × log n) buckets; provider/record stores | N/A (overlay, not physical mesh) | Analogue to ANDNA: `put_record`/`get_record`, provider records keyed by content hash | Identity = keypair-derived `PeerId`; transport security handled by libp2p (Noise/TLS), not kad itself |


## Kernel integration: TUN overlay vs. real routing tables (per system)

Netsukuku (Vala/C mainline) is the outlier among everything reviewed here:
it does **not** create a TUN/TAP overlay device at all. `IpCommands`
(`research/impl/vala/documentation/ita/Assemblaggio/CasoBanale.md:605-607`,
"vengono dati i vari comandi `ip` che servono a inizializzare o aggiornare le
tabelle di routing") issues real `ip route`-equivalent commands against the
node's live interfaces, and `IpCommands.map_update`
(`CasoBanale.md:631-632`) pushes QSPN map changes straight into the kernel
routing table. The C mainline's kernel prerequisites confirm this at the
`.config` level: `CONFIG_IP_ADVANCED_ROUTER`, `CONFIG_IP_MULTIPLE_TABLES`,
`CONFIG_IP_ROUTE_MULTIPATH`, `CONFIG_NETFILTER`
(`research/impl/c/netsukuku/README.md:177-181`) — i.e. Netsukuku *is* the
L3 routing table on participating interfaces, using multiple kernel routing
tables and multipath for its hierarchical paths, not an app-owned overlay
network. This matches its original design goal: replace the OSI L3 routing
layer outright on physical links, not tunnel over an existing IP network.

Every mesh-overlay system reviewed here instead **creates its own virtual
address space behind a TUN device** and never touches the host's default
routing table for the physical interfaces it peers over: Yggdrasil
(`src/tun/tun_linux.go` etc., §yggdrasil-go above), cjdns
(`doc/notes/cjdns-core.md:20-36`, "TUN support is a critical requirement"),
and — per Ironwood's own network package — the same PacketConn abstraction
wraps a TUN device rather than reprogramming kernel routes. BATMAN-adv is
the exception in the other direction: it runs at layer 2, presenting a
virtual Ethernet bridge interface (`batadv0`) instead of TUN/L3, so IP
addressing and routing above it are unaffected — closer in spirit to
Netsukuku's "be the network" stance than to the TUN-overlay group, but at
L2 instead of L3.

**Design fork this implies for netsukuku-rs**: if the Rust port targets
ad hoc physical mesh links as the original design intended, direct kernel
routing-table manipulation (Linux: rtnetlink, `RTM_NEWROUTE`/multipath
nexthops, possibly multiple tables via `ip rule`) is the faithful port of
`IpCommands`. If instead an easier bring-up path is wanted (running over
the Internet as an overlay, akin to Viphilama in karkinos's vendored RFC
0010, or for testing without root/`CAP_NET_ADMIN` on real interfaces), a
TUN-backed mode following the Yggdrasil/cjdns pattern is the safer
precedent. These are not mutually exclusive — Yggdrasil supports a "no
TUN" mode too (`yggdrasil-network.github.io/faq.html`, "Can I run an
Yggdrasil router without a TUN interface? Yes... `IfName` = `none`") — but
the *default* kernel-integration strategy is a decision the routing-core
design phase must make explicitly rather than copy from whichever example
is read first.

## karkinos post-mortem (d0p1s4m4/karkinos)

- Repo: `karkinos/` — GitHub metadata: 7 stars, 1 fork, not archived, "Updated 2025-09-01" (repo-level metadata touch, e.g. topics/README), but **actual last commit is `4230f21` on 2023-01-11** (`git log -1`, single commit in shallow clone — full history not available but this is HEAD).
- README (`karkinos/README.md:10`): "Karkinos is an implementation of the Netsukuku Protocol (Npv7_HT) in rust... aims to be fully compatible with Netsukuku C." Author states "⚠️ I'm not a rust developer" (`README.md:13`).
- Cargo workspace (`karkinos/Cargo.toml:3-6`) declares 3 crates: `karkinosd` (daemon), `karkinos-cli` (CLI/admin client), `qspn` (routing lib).
- **Actual implementation state — essentially nothing beyond scaffolding**:
  - `crates/qspn/src/lib.rs` (23 lines): a `bitflags!` enum mirroring the legacy C code's map/QSPN status flags (`MAP_ME`, `MAP_GNODE`, `QSPN_OPENED`, etc.) and `MAX_GROUP_NODE: u16 = 1<<8`, plus two empty stub fns `open()`/`close()` and a placeholder `add()` unit test. No packet types, no map structure, no ETP handling, no networking.
  - `crates/karkinosd/src/main.rs`: `clap`-based CLI arg parsing (`-i` interfaces, `-4`/`-6`, `--restricted`, `--daemonize`, `--pid`) and `tracing` logger init. Prints a startup banner and **exits** — no socket, no event loop, no packet processing.
  - `crates/karkinos-cli/src/main.rs`: parses `--server`/`--user`/`--password`/`--config` and does nothing with them (`let _cli = Args::parse();`).
  - `docs/viphilama.md`: reproduction of NTK_RFC 0010 (Viphilama, internet-overlay hybrid layer) — documentation only, not implementation-relevant to core QSPN/routing.
- Dependencies chosen: `bitflags 1.3.2`, `clap 4.0.10` (derive), `tracing`/`tracing-subscriber`. Idiomatic, unremarkable choices; no async runtime, no networking crate, no serialization crate present — confirming no actual I/O was ever written.
- **Why it stalled [INFERENCE]**: single-commit history, author self-identifies as non-Rust-developer, zero packet/socket/state-machine code after the initial scaffold commit. This is a bootstrap that never got past `cargo new` + flag-copying; there is nothing to port or avoid architecturally — it never reached a design that could fail. The only reusable artifact is the flag-naming convention mirrored from the C code (useful as a naming cross-reference, not as design guidance).
- Contrast with `research/related/netsukuku` (vg's fork) which *does* contain a full second independent reimplementation attempt (Python, `pyntk/`, with `ntk/sim` — a network simulator and a `test/` suite) — a far more complete prior-art artifact than karkinos, just not in Rust.

## yggdrasil-go — closest live production analogue

- Repo `yggdrasil-go/` (Go, commit `422836e`, 2026-06-19). README (`yggdrasil-go/README.md:7-11`): "early-stage implementation of a fully end-to-end encrypted IPv6 network... self-arranging... works over IPv4 or IPv6."
- **Architecture split**: `yggdrasil-go` itself (`src/core`, `src/tun`, `src/admin`, `src/multicast`, `src/config`) is a thin host-integration shell — TUN device, admin JSON-RPC API, multicast peer discovery, TLS/QUIC/TCP/WS transports (`go.mod:6-23`). All actual routing logic lives in a **separate library**, `github.com/Arceliar/ironwood` (`core.go:12-14`, imported as `iwe`/`iwn`/`iwt`). This is a strong architectural precedent: separate the routing/forwarding core (a `PacketConn`-like abstraction keyed by pubkey) from OS integration (TUN, admin, discovery).
- **Addressing** (`yggdrasil-go/src/address/address.go:9-100`, read in full): address = fixed 1-byte prefix (`0x02`) + 1 byte encoding "number of leading 1-bits in the bitwise-inverted pubkey" + truncated remainder of the inverted pubkey, bit-packed to 128 bits. This front-loads entropy so that addresses with many leading zero-runs (rare, "vanity" keys) get shorter usable prefixes — a self-certifying, PKI-free address derivation. `/64` **Subnet** variant sets one flag bit for LAN-behind-a-node routing. `GetKey()` reverses the encoding to recover the (partial, for lookup) pubkey from an address.
- **Routing** (per ironwood README, fetched via GitHub): as of the version vendored by current yggdrasil-go, routing is **not DHT-based**. It is greedy routing over a spanning-tree metric-space embedding (each node's ancestry path acts as its coordinate); loss of a path triggers a "path broken" notification back to sender; if the sender doesn't know the destination's tree location, it does a **multicast bloom-filter lookup** flooded over the spanning tree (ARP/NDP-like), not a Kademlia DHT. The README explicitly documents *why* they moved away from a DHT (used in Yggdrasil v0.4.x): O(n) worst-case convergence when merging two networks, hard-state DHT security/memory tradeoffs. Concrete numbers given: 8192-bit bloom filter, 8 hash functions, false-positive onset ~200 nodes in a subtree for a 1M-node network.
- **Root node**: no special privilege beyond occasionally appearing in worst-case paths (yggdrasil-network.github.io/faq.html, "Is there any benefit to being the root node? No.").
- **Scalability claim**: designed for "potentially global scale" per FAQ; explicitly created as a reaction to observed cjdns performance/scaling issues at the time (FAQ "Why Yggdrasil?").
- **Kernel integration**: TUN/TAP per-platform (`src/tun/tun_linux.go` etc.), MTU tuned to reduce syscalls (up to 65535, explained in FAQ), IPv6 packet rewrite in `src/ipv6rwc`.
- **Verdict — borrow**: the core/shell separation (routing library independent of TUN/admin/config); crypto-derived self-certifying addressing; explicit non-DHT lookup rationale (worth re-reading before choosing PeerServices' hash-node walk as-is, since it shares some DHT-like tradeoffs). **Avoid**: Yggdrasil is a flat single-level overlay — it has no concept of Netsukuku's g-node hierarchy/levels, so its addressing/routing code is not directly portable, only its engineering patterns are.

## cjdns — historical inspiration for Yggdrasil, differs in mechanism

- Repo `cjdns/` (commit `f5902ac`, 2025-12-10). Docs-only review per task scope (`doc/Whitepaper.md`, `doc/notes/*`); C internals not read.
- **Addressing**: IPv6 derived from a hash of an ed25519-style pubkey (Whitepaper intro + `doc/notes/cjdns-core.md:11` mentions key-generation/XOR-metric reimplementations by community).
- **Routing = two mechanisms layered**:
  1. **Switch** (`Whitepaper.md:494-552`): source routing via a "Route Label" — a stack of variable-width "Directors" (per-hop outgoing-interface indices), with an **Encoding Scheme** each node advertises so peers can *extend*/*truncate* Director widths when splicing routes together (`Whitepaper.md:437-490`). This is a compact strict-source-routing scheme, conceptually similar to MPLS label stacks.
  2. **Router / DHT** (`Whitepaper.md:553-589`): a **Kademlia-derived** system. Nodes search for routes using the XOR metric ("address space distance" = addresses XOR'd, rotated 64 bits, as big-endian integer, `Whitepaper.md:571-577`), splicing discovered route-segments to build a full source route. Routers **only** ever return closer-XOR-distance nodes with routes not doubling back down the querying interface (`Whitepaper.md:558-565`) — direct Kademlia lookup-convergence rules.
- **Kernel integration**: TUN device (`doc/notes/cjdns-core.md:20-36`, doc explicitly names this "a critical requirement").
- **Scalability claim**: Whitepaper frames itself as fixing Internet-scale routing-table growth (`Whitepaper.md:112-135`) by using self-certifying flat addresses instead of provider-allocated aggregatable blocks — but per Yggdrasil's own FAQ, this XOR/DHT approach hit real-world performance/scaling issues that motivated the Yggdrasil fork.
- **Crypto/identity**: CryptoAuth (NaCl/Curve25519-based) sessions, both hop-by-hop (switch-level, "outer"/point-to-point) and end-to-end.
- **Verdict — borrow**: the *idea* of a compact, self-describing variable-width label stack for physical-hop source routes (useful if Netsukuku's per-level route composition needs a wire-compact representation across many g-node levels — cjdns's Encoding Scheme negotiation solves exactly the "labels must remain compatible when spliced across differently-sized routers" problem, which is analogous to composing NIP paths across g-nodes of different `gsize`). **Avoid**: the Kademlia/XOR-metric DHT for topology discovery — Netsukuku already has a purpose-built hierarchical alternative (QSPN maps + PeerServices hash_node) that avoids DHT's O(n) network-merge cost by construction; grafting a XOR DHT on top would be a second, redundant addressing space.

## vg/netsukuku — archived community fork, diff vs Netsukuku/netsukuku mainline

- Repo `netsukuku/` (commit `12402be`, 2009-11-16). GitHub metadata: 24 stars, 13 forks, **archived**, homepage `netsukuku.org` (vs. mainline's `netsukuku.freaknet.org`).
- Structural diff vs. already-cloned `research/impl/c/netsukuku` (mainline, HEAD `886a24a`, 2025-06-12, active): mainline has **no `pyntk/`, no `openwrt/`** directory; vg's fork has both plus the original C `src/` (`netsukuku/README:60-115` — build via SCons, deps on `libgmp`/`zlib`/`openssl`).
- `pyntk/` is a **from-scratch Python reimplementation** with its own `ntk/core`, `ntk/network`, `ntk/lib`, `ntk/sim` (network *simulator*, distinct from the live daemon) and a substantial `test/` directory (`test_map.py`, `test_p2p.py`, `test_network_route.py`, `test_microrpc.py`, etc.) — i.e., a second, independently-tested prior implementation of Netsukuku's core algorithms, in a different language, with a simulation harness for testing routing/map logic without real network hardware.
- **Verdict**: not a fork with *different protocol intent* — it is the same Npv7/ANDNA C protocol, but preserves a community-contributed Python reimplementation + test/simulation infrastructure that the mainline dropped. **Borrow**: the idea of a network *simulator* (`ntk/sim`) as a test harness — building an equivalent in-process simulated-topology test harness for `netsukuku-rs` (many virtual nodes in one process, fake links, deterministic packet delivery) would let QSPN/hooking/ANDNA algorithms be tested without real interfaces, exactly as `pyntk`'s test suite does. **Avoid**: don't treat this repo as an authoritative spec source — it is older (2009-2013) and less complete than the Vala rewrite already cloned in `research/impl/vala`.

## Babel (RFC 8966) — distance-vector loop-avoidance, feasibility conditions

Read in full (`rfc8966.txt`, 3060 lines).

- Core mechanism: Bellman-Ford distance-vector + **sequenced routes** (DSDV-derived) to solve transient-loop starvation (`rfc8966.txt:280-441`). A route is a quintuple `(prefix, plen, router-id, seqno, metric)`.
- **Feasibility condition** (`rfc8966.txt:951-1004`): an update is only accepted if `seqno' > seqno` or (`seqno'==seqno` and `metric' < metric`) relative to the *feasibility distance* recorded in a **source table** (keyed by `(prefix, plen, router-id)`, independent from the per-neighbor route table). This guarantees loop-freedom without needing full topology knowledge — cheaper than link-state, more robust than naive distance-vector.
- **Route/source table separation** (`rfc8966.txt:715-751`): source table holds `(seqno, metric)` feasibility distance per source; route table holds one entry per `(prefix, plen, neighbor)` with its own timer.
- **Seqno requests** (`rfc8966.txt:427-441`, `1300-1343`): explicit hop-by-hop request forwarded to the origin to force a route to become feasible again, bounded by a `Hop Count` TTL-like field — avoids waiting for periodic reannouncement.
- Metric is a pluggable monotonic function `M(c,m)`; RECOMMENDED default additive (`rfc8966.txt:1006-1034`); reference implementation uses ETX for wireless (`rfc8966.txt:2611-2624`).
- **Verdict — borrow**: feasibility-condition-via-sequence-numbers is a much simpler loop-avoidance primitive than Netsukuku's current QSPN flooding-with-fingerprint-dedup, worth comparing against for any lower-level fallback/link-cost routing netsukuku-rs might need at level 0 (within-gnode) links. **Differs from Netsukuku**: Babel is flat distance-vector with no hierarchy/addressing scheme of its own (it is protocol-agnostic, routes arbitrary prefixes) — it solves a different half of the problem (loop-free metric propagation) than QSPN's hierarchical map-building; not a substitute for QSPN, potentially a model for one layer of it.

## BATMAN-adv — OGM flooding, per-neighbor router ranking

- Distance-vector at layer 2: every node floods a periodic **OGM** (Originator Message) with an incrementing sequence number and TQ (Transmission Quality, 0..1 mapped to a byte) (open-mesh.org/doc/batman-adv/OGM.html §4.1).
- Receivers compute **path TQ** = incoming OGM's TQ × best link TQ to the sending neighbor, using a sliding window over recently-seen sequence numbers per neighbor (§Definitions "sliding window").
- Router selection ("Router Ranking", §5) keeps a list of *all* potential next-hops per originator (loop-free by sequence-number ordering, §4.2.2) and picks a **Selected Router**; rebroadcasts only occur when the selected router info changes, bounding OGM amplification.
- No explicit topology map is ever built — pure per-originator distance-vector, "squared amount of overhead in worst case" acknowledged by the design doc itself.
- **Verdict — avoid** as a routing algorithm for netsukuku-rs (no hierarchy, doesn't scale past a single flat mesh — the doc itself flags O(n²) worst case), but the **OGM sliding-window duplicate/out-of-order detection** and the split of link-quality estimation into a separate protocol (ELP, mentioned in OGM.html §1) are useful references for netsukuku-rs's own neighbor/link-quality tracking at level 0.

## OLSRv2 (RFC 7181) — MPR flooding/topology reduction

- Proactive link-state, but reduces both **flooding cost** and **advertised topology size** via **Multipoint Relays (MPRs)**: each router selects a subset of symmetric 1-hop neighbors that covers all its symmetric 2-hop neighbors (RFC 7181 §1, abstract). Two MPR sets: "flooding MPRs" (who rebroadcasts control traffic) and "routing MPRs" (who must declare link-state for correctness) — can coincide under hop-count metric.
- Builds on NHDP (RFC 6130, neighbor discovery) and the Generalized MANET Packet/Message Format (RFC 5444) — a layered protocol family rather than one monolith.
- Table-of-contents (fetched) shows the full information-base model: Local/Interface/Neighbor/Topology/Received-Message information bases, each with defined tuple lifetimes — a much more formally specified state machine than Netsukuku's Vala docs currently describe QSPN's map state.
- **Verdict — borrow**: the MPR concept (subset of neighbors responsible for rebroadcast) is directly analogous to reducing ETP flooding cost in QSPN; worth flagging to the routing-design phase as a possible optimization for level-0 ETP flood suppression. **Differs from Netsukuku**: OLSRv2 has no hierarchy — MPR reduction operates within one flat MANET; QSPN's g-node hierarchy already bounds flooding scope by design, so MPR would be a level-0-only optimization, not a substitute for the hierarchy itself.

## Kademlia / libp2p-kad — analogue for ANDNA/PeerServices' `hash_node`

- Netsukuku's PeerServices computes, for service `p` and key `k`, `hash_node(k) = α_t(H_t(h_p(k)))` — a hash mapped into a **tuple of g-node coordinates**, i.e. the lookup target is *always* expressed as a position in the existing hierarchical address space, and the request is routed there using ordinary QSPN-derived paths, optionally scoped to a sub-g-node (`research/impl/vala/documentation/ita/ModuloPeers/AnalisiFunzionale.md:46-61`, `AlgoritmiInstradamento.md:354-356` — read via grep, evidence lines quoted above). This is structurally different from Kademlia: there is one global hierarchical address space and the hash just picks a point in it; there is no separate DHT keyspace or k-bucket structure.
- **libp2p-kad** (`docs.rs/libp2p-kad` 0.48.0, read in full module index): `Behaviour` type implementing iterative closest-peer lookups; `K_VALUE`/`ALPHA_VALUE` constants (bucket size / lookup parallelism, standard Kademlia parameters); `Record`/`ProviderRecord` for DHT-style key→value and key→providers; ships as a `NetworkBehaviour` for `rust-libp2p`'s swarm, i.e. designed to be composed with other libp2p protocols (identify, noise, yamux) rather than used standalone.
- **Verdict — differs, do not adopt wholesale**: netsukuku-rs should **not** replace PeerServices' hierarchical `hash_node` walk with a flat Kademlia DHT — doing so would duplicate the addressing hierarchy with a second, incompatible keyspace and lose the "confine search to my g-node" locality optimization that hierarchy gives for free. **Borrow selectively**: `libp2p-kad`'s `Record`/`ProviderRecord`/`Quorum`/replication-and-caching machinery (`Caching`, `Quorum`, TTL/republish logic) is a mature reference for the *storage and consistency* half of a PeerServices-like key/value database (Netsukuku's own docs already describe TTL databases and quorum-like replication at `research/impl/vala/documentation/ita/ModuloPeers/DatabaseTTL.md`), even though the *routing* half (how you find the node) must stay hierarchical.

## Reusable building blocks — adopt, don't invent

| Need | Adopt from ecosystem instead of hand-rolling | Rationale |
|---|---|---|
| TUN/TAP device creation & packet I/O | Existing Rust TUN crates (cross-platform tun/tap wrappers) | Every reviewed system (cjdns, Yggdrasil) treats this as a solved, unglamorous platform layer; Yggdrasil's own per-OS `tun_*.go` files show how much boilerplate this needs — don't hand-roll ioctl/netlink TUN setup. |
| Netlink route/interface manipulation (Linux) | Rust netlink crates (rtnetlink-family) | Yggdrasil depends on `github.com/vishvananda/netlink` (`go.mod:15`) rather than shelling out to `ip route` — same rationale applies to netsukuku-rs kernel route injection. |
| Ed25519/X25519 keys, AEAD sessions | `ed25519-dalek`/`x25519-dalek` + a NaCl-box-equivalent (XSalsa20-Poly1305) or a maintained AEAD (ChaCha20-Poly1305) crate | Yggdrasil (ed25519 + nacl/box) and cjdns (CryptoAuth/Curve25519) both derive identity directly from a keypair and use well-known primitives, not bespoke crypto — same pattern is safe to copy for Netsukuku identities/pseudo-addresses. |
| Async runtime / event loop for daemon | `tokio` (already the de facto standard; matches the "Rust 1.97" toolchain target) | All reviewed Go/C daemons are event-driven around sockets+timers; karkinos's failure to even reach this stage underscores it's the first real design decision to get right. |
| Bloom filters (if adopting Yggdrasil-style lookup as an optimization) | `bloomfilter`/`fastbloom`-class crates | Ironwood's own bloom-filter lookup is a well-specified, parameterizable technique (8192 bits, 8 hash fns) — reuse an audited bitset/bloom crate rather than reimplementing hashing. |
| DHT/record-store patterns for PeerServices-like KV needs | Borrow API shape from `libp2p-kad`'s `Record`/`ProviderRecord`/`Quorum`/`Caching` (not the crate itself, since its routing layer is flat Kademlia) | Gives a tested vocabulary (quorum levels, write-back caching, provider records) for Netsukuku's own TTL/quorum database semantics without adopting its XOR-metric routing. |
| Bincode/CBOR-style compact wire encoding for ETPs/labels | `bincode` or `postcard` (no-alloc friendly) for internal wire formats | cjdns's Encoding-Scheme-negotiated variable-width Director stacks show hand-rolled bit-packing is achievable but fiddly; a `postcard`/custom-bitpack hybrid is more maintainable in Rust than replicating cjdns's exact scheme unless byte-for-byte savings are proven necessary. |
| Config file format | `hjson`-style human-friendly config (Yggdrasil uses `hjson-go`) or plain TOML/JSON via `serde` | Matches Rust ecosystem norms (`serde` + `toml`) better than JSON-with-comments hacks cjdns uses. |

(No versions pinned per task contract — RustStack task owns dependency versioning.)

## Open questions / risks for the Rust port

1. **Lookup strategy for PeerServices/ANDNA at scale**: Netsukuku's `hash_node` walk assumes a live, consistent hierarchical map (QSPN). Yggdrasil's own history (DHT → bloom-filter-over-tree) shows that *even a working global network* found hard-state and soft-state DHT approaches individually unattractive at scale. Should netsukuku-rs's design phase re-validate PeerServices' current replication/quorum model against these documented failure modes before implementation, or is the existing Vala spec considered final? Needs a decision from the specs-synthesis phase, not from this note.
2. **Level-0 (intra-gnode) flooding cost**: QSPN's flooding within a level, absent a Netsukuku-native MPR-equivalent, could hit BATMAN-adv/OLSRv1's well-documented O(n²) worst case at the *lowest* level before hierarchy kicks in. Whether to borrow an MPR-style reduction for level-0 ETP flooding is an open design question for the routing-core task, not resolved by this note.
3. **cjdns-style variable-width label stacks vs. current Vala route representation**: unclear whether the existing Vala/QSPN `HCoord`/tuple-based route representation is already wire-compact enough, or whether a cjdns-style negotiated variable-width encoding would meaningfully shrink ETP/route messages at high `levels` counts — needs a wire-format sizing analysis once the ETP format is fixed (out of scope here, depends on 01/03 notes).
4. **No live third-party Rust Netsukuku implementation exists to benchmark against or diff behavior with** — karkinos is not usable as a reference implementation for interop testing or performance baselines; netsukuku-rs is effectively greenfield with no Rust prior art beyond a discarded scaffold.
5. **TUN/netlink crate selection is deferred to RustStack's note (06)** — this note only asserts *that* such crates should be used, not which; avoid duplicate/conflicting crate picks between notes 05 and 06.
