# netsukuku-rs

			- Close the world, txEn eht nepO -

--

A Rust reimplementation of [Netsukuku](https://github.com/Netsukuku/netsukuku)'s **current**
protocol stack: QSPN v2 routing, Neighborhood discovery, Identities, Hooking (network-merge
negotiation), Coordinator, the generic PeerServices DHT substrate, and ANDNA (distributed hostname
service).

> **Canonical home: [github.com/M0Rf30/netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs)** —
> issues, pull requests, CI, and the crates.io release all live there. A read-only mirror is
> published at [codeberg.org/M0Rf30/netsukuku-rs](https://codeberg.org/M0Rf30/netsukuku-rs) for
> anyone who prefers a libre forge; it is a Forgejo *pull* mirror, so it tracks GitHub
> automatically and accepts no commits of its own.

## 1. The old wired

The Internet you are reading this on has a centre. Addresses come from a registry, names come from
a hierarchy of servers rooted in a handful of organizations, and a route exists because someone
with the authority to announce it did so. Take the registries and the roots away and the network
does not degrade — it stops meaning anything.

Netsukuku starts from the opposite premise: a routing protocol in which every node computes its own
address, resolves names peer-to-peer, and builds its own routes out of nothing but its neighbours,
with no server, no registry, and no root anywhere in the design. This repository is a from-scratch
Rust implementation of that protocol as it stands today, not as it stood in 2005.

"Current" is a deliberate word choice. Netsukuku has two upstream lineages and only one is a live
protocol spec:

- The **C daemon** (`netsukuku/netsukuku`) still implements the original 2005-2007 Npv7_HT design —
  fixed 256-way g-node fanout, combined "Radar" link-probing + "Hook & Unhook" merge logic, ad-hoc
  ANDNA hash-node placement, IP addressing/NAT done by shelling out to `ip`(8)/`iptables`(8). Its
  `doc/` tree has exactly one commit since 2013; all activity since is build/CI maintenance. It is a
  legacy compatibility target, not a spec source.
- The **Vala rewrite** (2017-2020, lukisi) is the modular respecification that actually changed the
  protocol: QSPN v2 (Extended Tracer Packets, per-level `gsize(i)` instead of fixed 256), a 3-way
  split of the old radar+hook logic into Neighborhood / Hooking / Coordinator, and PeerServices as a
  generic hierarchical DHT that ANDNA (and Coordinator) sit on top of instead of hand-rolled
  placement logic.

This repository ports the Vala design, not the C one. The archaeology backing that decision —
document-stratum dating, RFC-by-RFC adoption status, line-cited divergences — is in
[`research/notes/03-specs-and-rfcs.md`](research/notes/03-specs-and-rfcs.md).

## Crate map

Twelve crates, dependency arrows point from dependent to dependency:

```
ntk-common  (Topology, Naddr, HCoord, Cost, Fingerprint — no deps on any sibling crate)
ntk-proto   (39-method wire schema + domain codec, prost/protox)                 -> ntk-common
ntk-rpc     (RpcClient/RpcHandler, TcpRpcClient, FakeRpcClient, TcpServer,
             UdpBroadcaster)                                                     -> ntk-proto
ntk-netlink (native netlink: RealNetlink, FakeNetlink, TableAllocator, detect,
             cleanup — no sibling deps, speaks std types only)
ntk-neighborhood (arc discovery, liveness, EMA cost + hysteresis)        -> ntk-common, ntk-proto,
                                                                              ntk-rpc, ntk-netlink
ntk-identities   (identity registry, duplication/migration handshake)   -> ntk-common, ntk-proto,
                                                                              ntk-rpc
ntk-qspn         (QSPN v2: ETP propagation, RouteSnapshot, eldership)    -> ntk-common, ntk-proto,
                                                                              ntk-rpc
ntk-peerservices (hierarchical DHT over the Naddr space, NTK_RFC 0014)   -> ntk-common, ntk-proto,
                                                                              ntk-rpc
ntk-hooking      (join/merge state machine, find_shortest_mig)          -> ntk-common, ntk-proto,
                                                                              ntk-rpc
                 (deliberately NOT ntk-qspn/identities/neighborhood/coordinator —
                  it defines its own QspnView/CoordinatorClient traits and the daemon
                  implements them, so hooking has no compile-time dependency on the
                  concrete routing/coordination stack)
ntk-coordinator  (CoordinatorService: PeerService, position reservation) -> ntk-common, ntk-proto,
                                                                              ntk-rpc, ntk-peerservices
ntk-andna        (hostname service, ed25519 ownership, two PeerServices
                  instances per RFC 0014)                                -> ntk-common, ntk-proto,
                                                                              ntk-rpc, ntk-peerservices
ntkd             (composition root: binary + lib, wires all eleven crates
                  into a running node)                                   -> all of the above
```

Exact dependencies (including feature flags) live in each crate's `Cargo.toml`; the above is the
shape, not a substitute for reading them.

## 2. Build, test, run

Toolchain: `rust-version = "1.97"` (workspace floor); developed and CI-verified against `1.98.0`.

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

`ntk-proto` and every module crate that defines RPC messages generate their prost code from
`.proto` sources **at build time** via `protox` (a pure-Rust protoc). No system `protoc` install is
required anywhere.

### Privileged test tier

Two suites are `#[ignore]`d by default because they mutate real kernel network state, and are not
part of the `cargo test --workspace` run above:

```sh
unshare --net --map-root-user -- \
  sh -c 'ip link set lo up; cargo test -p ntk-netlink -- --ignored'

unshare --net --map-root-user -- \
  cargo test -p ntkd --test multi_node -- --ignored real_netns
```

The first exercises `RealNetlink` (address/route/rule add-list-remove round trips, kernel capability
detection) inside a fresh, unprivileged network namespace. The second is the highest-value test in
the repository: it starts two real `ntkd` daemons in two real network namespaces joined by a veth
pair, lets them discover each other, form an arc, and negotiate into a shared network, then reads the
installed kernel routes back through an *independent* `RealNetlink` connection to confirm the routes
are real, not merely computed in memory. Both run rootlessly via `unshare --map-root-user` (an
unprivileged user+network namespace) on a Linux host that allows unprivileged user namespaces; see
`.github/workflows/ci.yml` for why CI runs the privileged tier under `sudo` instead (GitHub-hosted
runners restrict unprivileged user namespace creation by default).

### Running a node

```sh
cargo run -p ntkd -- run --config <path/to/config.toml> [--nic eth0 ...] [--log-level info]
cargo run -p ntkd -- status [--socket /tmp/ntkd.sock]
```

See `ntkd::kernel::config::NtkdConfig` for the config file shape (topology `gsizes`, `nics`, port).

--

## 3. Load-bearing design decisions

- **L3 via netlink, never a TUN overlay.** Netsukuku is a real routing protocol: it owns kernel
  routing tables (multiple tables + policy rules via `ntk-netlink`'s `TableAllocator`), not an
  overlay network tunneled through a virtual interface. There is no TUN device anywhere in the
  workspace and no subprocess calls to `ip`/`iptables`/`sysctl` — `ntk-netlink` speaks netlink
  directly (`rtnetlink`) even in tests (`FakeNetlink` records the same operations `RealNetlink` would
  issue, so tests assert real intent, not real kernel state).
- **Single-owner actors, never `Arc<RwLock<_>>` over protocol state.** Each protocol module (QSPN,
  Neighborhood, Hooking, PeerServices, Identities, ANDNA, Coordinator) runs as one task owning its
  state exclusively; other code talks to it over `mpsc` command channels with `oneshot` replies,
  reads consistent point-in-time state via `tokio::sync::watch`, and observes state transitions via
  `broadcast` events. An outbound RPC is never awaited from inside a command loop — blocking the
  actor on a peer's response has caused two confirmed deadlocks in this codebase (`ntk-qspn`, and
  `ntkd`'s `LazyLinkClient`) and is treated as a standing defect class, not a one-off bug.
- **PeerServices is the substrate, ANDNA is a client of it.** Rather than ANDNA hand-rolling its own
  hash-node placement and anti-abuse bookkeeping (as the legacy C/RFC-0007 design does), ANDNA is
  built as two `PeerService` instances registered on the generic `ntk-peerservices` hierarchical DHT
  (NTK_RFC 0014). Coordinator is built the same way. This is a direct port of the Vala-era
  generalization, not a Rust-specific choice.
- **No transport crypto; authentication is opt-in at the application layer.** `ntk-rpc`'s
  `TcpRpcClient`/`TcpServer` speak the wire protocol in the clear — there is no TLS/Noise layer
  anywhere in the stack. What exists instead is ed25519 signing in `ntk-proto::auth`, over a
  domain-separated, length-framed canonical encoding: ANDNA hostname ownership, per-arc peer
  identity (pinned on first contact, as ANDNA pins a hostname's owner key), and a request's
  origin assertion, verified once at the servant rather than at every relay because verification
  costs ~26.8 µs against a 503 ns unauthenticated round trip. All of it is off by default
  (`require_auth = false`), so the default build stays a faithful reference: the `Auth` field is
  an optional protobuf field, which makes carrying it wire-compatible and only *enforcing* it a
  break.
- Anything that must be unique across the whole network — not just locally — derives from
  `ntk_neighborhood::NodeId`. Three separate bugs in this codebase came from treating a node-local
  value (an arc id, a link id) as if it were globally meaningful; `NodeId` is the one type that
  actually is.

## 4. What is deliberately not implemented

**On parity.** The normative baseline for this port is Luca Dionisi's Vala rewrite, not the 2005
C daemon and not the full NTK_RFC wishlist. Measured against that baseline the port is
essentially complete, and the list below deliberately mixes three different kinds of absence, so
read the reason rather than counting the bullets:

- *Not in the normative upstream either* — IPv6 (never implemented in any Netsukuku), and the
  unported Alpt-era RFC ideas: IGS (0003), bandwidth cost (0002), the counter-gnode pubkey fix
  (0007), Viphilama (0010), Carciofo (0011), Net Split (0012), Local ANDNA (0015).
  `research/notes/03-specs-and-rfcs.md` records that none of these has a found vala-era
  replacement. ANDNA is the inverse case: upstream's is a 13-line stub, so this port is *ahead*
  of the reference there rather than behind it.
- *Present in the legacy C daemon, deliberately dropped* — everything reachable only through
  `iptables`. That follows from the no-subprocess rule, which is a purpose of this port, not an
  omission from it.
- *A real gap against the reference* — exactly one: the second protocol stack after a g-node
  migration.

- **iptables/NAT, and everything downstream of it**: the anonymizing address kind and `subnetlevel`
  (autonomous-subnet/NAT boundary) from the legacy addressing scheme. The legacy daemon implements
  both via `iptables -t nat` SNAT/NETMAP rules; porting them would mean either shelling out (rejected
  design choice, see above) or adopting a native nftables/netlink NAT crate, which has not been
  evaluated. `ntk-netlink`'s address/route/rule types have no NAT concept.
- **IPv6.** Every address type in the workspace (`ntk-netlink::Ipv4Net`, the netlink `RouteSpec`/
  `Nexthop`/`AddressEntry` types, `NETSUKUKU_ADDRESS_SPACE = 10.0.0.0/8`) is IPv4-only. The legacy C
  daemon never implemented IPv6 either; nothing in this port revisits that.
- **The second protocol stack after a g-node migration.** The migration itself is wired and does
  run: `ntk_hooking::HookingEvent::DoPrepareMigration`/`DoFinishMigration` drive
  `ntk_identities::Handle::prepare_migration`/`migrate`, and `ntkd`'s own lifecycle calls them
  (`crates/ntkd/src/node/lifecycle.rs`). What is missing is narrower: the daemon does not spin up
  a second full protocol stack for the identity `migrate` resolves, because it keeps one live
  dispatcher target per process and so can never have two identities simultaneously reachable.
  A fully faithful port would spawn that second stack the moment `migrate` returns its id; the
  reasoning is recorded at `crates/ntkd/src/node/lifecycle.rs:137-149`, kept out of this pass and
  reported rather than half-built.
- **ANDNA is a reconstruction, not a port.** Upstream's own Vala source, `andna.vala`, is a 13-line
  stub (`AndnaManager.init` and nothing else — no registration, no resolution, no counter/anti-abuse
  logic); `serializables.vala` for the module is empty. `ntk-andna`'s design is derived from the
  legacy C implementation (`andna.c`/`andna_cache.c`/`snsd_cache.c`) plus NTK_RFC 0009 (SNSD) and
  NTK_RFC 0014 (the DHT substrate), because there is no working Vala-era ANDNA to port from.

Q: Does it really work?
A: ^_^

## 5. Research corpus

[`research/README.md`](research/README.md) indexes the reference material this port was built
against. `research/notes/` (our own synthesis, with `path:line` citations into the vendored sources)
is committed. The vendored upstream clones themselves — `research/impl/` (the Vala and C trees),
`research/related/`, `research/papers/`, `research/specs/` — are gitignored: they are large,
third-party, and regenerable from the clone list and bibliography in `research/notes/`, not source
material this project owns.

Two notes worth reading first: `research/notes/01-vala-core-routing.md` (§3 QSPN, §6-7 Hooking and
Coordinator) and `research/notes/02-vala-services-daemon.md` (§3 PeerServices, §5 the daemon).

--

## Credits

The protocol this repository implements is not ours. It was designed by Andrea Lo Pumo (AlpT) and
the community around the **Freaknet Medialab**, Catania, Italy, <www.freaknet.org>, together with
the **Poetry Hacklab**. The modular respecification this port actually follows — QSPN v2,
Neighborhood / Hooking / Coordinator, PeerServices as a generic substrate — is Luca Dionisi's
(lukisi) Vala rewrite; it is the normative source for this codebase, cited throughout
`research/notes/`.

This is a Rust implementation of their protocol, tested and built independently; any defect in it
is ours, not theirs.

## License

GPL-3.0-or-later, matching the upstream Netsukuku lineage. See [`LICENSE`](LICENSE).
