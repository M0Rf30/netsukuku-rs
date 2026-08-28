# Changelog

All notable changes to this project are documented here. Versions follow [Semantic
Versioning](https://semver.org/spec/v2.0.0.html); the twelve `ntk-*` crates and `ntkd` are
released in lockstep, so they always share a version even when only some of them changed.

## [Unreleased]

### Changed

- **`ntkd andna-resolve` now returns the routable IPv4, not only `Naddr` notation.** A resolve
  reported its target as a hierarchical position (`2/4.0/2.1/2.0/2`) — correct, and useless to a
  caller, who wants something it can connect to. Each entry in `AndnaResolveReply::addresses` is
  now a `ResolvedTarget { target, ipv4 }`, where `ipv4` is the `/32`
  `crate::kernel::addressing::host_address` computes for an `SnsdTarget::Address`. The CLI prints
  `10.0.0.27  (2/4.0/2.1/2.0/2)`.
  `ipv4` is `None` for an `SnsdTarget::Alias` (a hostname has no position of its own) and for an
  address whose topology does not fit the 24 bits under the fixed `10` octet. The two are
  deliberately not distinguished: a client's action is the same in both — fall back to `target`.
  The `Naddr` here was decoded from the wire, so the second case is not this node's invariant to
  assume even though it cannot arise for a peer sharing this node's `gsizes`.
  This changes the control socket's TOML reply shape: `addresses` was an array of strings and is
  now an array of tables. Local-only — the status socket is not a peer-facing wire protocol, so no
  deployed node is affected.

### Fixed

- **`call_entering` excluded the entire searchable space on any multi-level topology.**
  `ntk_coordinator::CoordinatorClient::call_entering` passed `exclude_my_gnode = Some(top - 1)`.
  `all_gnodes_up_to_lvl` (`ntk-peerservices/src/actor.rs:515-528`) excludes every g-node that is
  *not* mine below the level it is given, while `tuple::approximate` independently skips every
  g-node that *is* mine — so any `lvl >= 1` excluded everything below it, the prospective host
  included, and `contact_peer` failed `NoParticipants`. `Some(top - 1)` degenerates to `Some(0)`
  exactly when `levels == 1`, which is why single-level `gsizes = [8]` was unaffected and this
  went unnoticed. Now `Some(0)` unconditionally, which is the "do not self-answer" suppression
  the call actually wants. Verified: this alone takes a multi-level guest's `evaluate_enter` from
  `NoParticipants` to success. Upstream has no `call_entering` at all — every coordinator call
  uses `call()` with no exclusion (`peers.vala:820-861`) — so `Some(0)` is also the minimum
  deviation needed to keep the self-loop fix this method was introduced for.

- **The enter protocol built the illegal `CoordinatorKey(0)`.** `begin_enter`/`completed_enter`/
  `abort_enter` sent `lvl + 1`, and briefly a `lvl.max(1)` clamp. `is_valid_key` accepts only
  `1..=levels` (`fk_database.vala:47-55`, mirrored in `ntk_coordinator`'s `reserve_enter`), so a
  servant reached with `top == 0` answers `top 0 is out of range for a topology with N levels`.
  Upstream never builds that key: `proxy_coord.vala:342-355,389-396,422-429` short-circuit
  `lvl == 0` to the *local* manager, skipping the coordinator entirely. All three now do the
  same via `ntk_coordinator::Handle::{begin,completed,abort}_enter` (previously `pub(crate)`,
  now `pub` — they are upstream's `mgr.*`), and pass `lvl + 1` on the DHT path, which is this
  port's actual contract: the handlers recover `level = top.saturating_sub(1)`.
  `completed_enter`'s local handler is not a no-op — it drives `EnterArbiter::complete`, which
  must release the level so a different network can enter later — so its bypass invokes the
  handler rather than merely returning.

- **Two virgin daemons now merge into one network on a multi-level topology.** The three fixes
  above, in combination. `[4, 2, 2, 2]` is the topology `contrib/systemd/ntkd.toml` ships, so
  before this a packaged two-node install formed an arc, installed routes, and looked healthy
  while permanently remaining *two* networks. Verified in a live two-namespace run: the pair goes
  from `10.0.0.10` + `10.0.0.30` with `/28` g-node routes between them to `10.0.0.18` +
  `10.0.0.16` with `/32` host routes inside one g-node, the guest reaching
  `hooking: Entered { ask_lvl: 0 }` and both nodes reporting a coordinator reservation.
  Covered by `two_virgin_daemons_merge_into_one_network_on_a_multi_level_topology`
  (`crates/ntkd/src/node/negotiation_tests.rs`), which is no longer `#[ignore]`d; its doc comment
  keeps the full history, including two rejected fixes so they are not retried. Only single-level
  `gsizes = [8]` negotiation was ever covered, which is why none of this surfaced earlier.

- **Cross-node ANDNA resolve is deterministic again.** Resolving a hostname from a node other
  than the registrant returned records only intermittently. That was a symptom of the merge
  failure above, not an ANDNA defect: two unmerged networks have disjoint hash-node placement, so
  which node answered depended on the run. Two scout claims were checked and refuted rather than
  acted on — that `RoutingEnvAdapter::gnode_exists` cannot see an unmerged peer's outer g-nodes
  (a working `evaluate_enter` requires that it can), and that upstream's `max_lvl` is
  "1-indexed, never 0" (`proxy_coord.vala:104` seeds it from `subnetlevel`, unconditionally `0`
  here, so `chosen_lvl = 0` is correct and shifting that index would have regressed the
  single-level path).

## [0.1.7]

One correctness fix in 0.1.6's groundwork, and the mig-01 plan corrected after tracing it properly.
No behaviour change for a single-identity node, which is every node today.

### Fixed

- **The identity dispatch map was keyed on the node, not on an identity.** `secondary` used
  `ntk_neighborhood::NodeId` — `NeighborhoodConfig::my_id`, one value for the process's whole life
  — so a connectivity fork and the successor it bridges for would have shared it and collided,
  which defeats the only purpose the map has. Keyed on `ntk_identities::IdentityId` now, which the
  registry mints per identity and `Handle::migrate` returns for the successor. Upstream draws the
  same line: `IdentityAwareUnicastID` carries an identities-level id and `get_identity_skeleton`
  matches it against each local identity (`skeleton_factory.vala:284-291`), while its `src_nic`
  equivalent stays per-arc.
  The main id is also read live from the registry rather than copied at construction, which is a
  second correctness fix in the same area: unlike the node id, the main `IdentityId` *changes* on
  every migration, so a cached copy would have named a retired identity from the first rehook on.
  Nothing on the wire changes — no caller sends an `IdentityAware` `unicast_id`, so redefining that
  payload cannot affect a deployed peer.

### Documentation

- **The recorded mig-01 plan was wrong, and is corrected.** A previous note claimed the migration
  blackout was purely `migrate`'s ordering. Reordering is necessary but not sufficient: the
  successor converges by receiving its peers' ETPs, so it must be reachable while it bootstraps, so
  a peer must name it in `unicast_id`, so that peer must know its `IdentityId`. Peers learn that
  from the identity-arc duplication protocol — and `ntk_identities::Handle::migrate` is called with
  an empty devices map (`crates/ntkd/src/node/lifecycle.rs:1418`), so `run_migration_duplication`
  computes `broken = devdata.is_none()` for every arc, reports them all broken, and removes them.
  No peer is ever told the successor exists. The map is empty because it describes per-identity
  *pseudodevices*, which `ntk_identities::pseudo` names and nothing creates.
  So the floor under mig-01 is per-identity pseudodevices — real per-identity L2/L3 presence on a
  shared NIC, over `ntk-netlink` link creation. Doing the reordering alone would keep the bridge
  serving while leaving the successor unable to converge, which is *worse* than today, since the
  current synchronous teardown at least guarantees the successor is the only identity its peers can
  reach. Recorded in `lifecycle.rs` and `README.md` §6 so a plausible-looking partial fix is not
  attempted twice.

## [0.1.6]

Groundwork for the connectivity identity (mig-01). No behaviour change for a single-identity node,
which is every node today — the point is that a second identity is now *possible*.

### Added

- **A second local identity is now addressable.** `lifecycle.rs`'s own scope note named the two
  blockers: "one live dispatcher target per process; never two identities simultaneously
  reachable". Both are gone.
  The wire already carried the selector and this port ignored it. `unicast_id` — upstream's
  `IUnicastID` — names the *destination* identity:
  `research/impl/vala/ntkd/rpc/skeleton_factory.vala`
  dispatches on it at `:192-236`, and `get_identity_skeleton` matches it against each local
  identity's own id at `:284-291`. `ntk.proto`'s `CallerContext` comment has said all along that a
  node can host several identities sharing one NIC. `stubs.rs` sent an empty value; `dispatch.rs`
  never read it.
  `unicast_id` now has upstream's three variants (`serializables.vala:405-492`) and the dispatcher
  resolves them: identity-aware to that identity's stack, main/empty/absent to the main stack,
  whole-node to the node-level handlers. An identity-aware id naming an identity this node does not
  host is **rejected**, not quietly served from main — answering out of the wrong map is the defect
  this work exists to prevent.
- **Two identities can hold kernel routing state at once.** Every generation claimed table 251, so
  a bridge would have fought the main identity for it. `bootstrap_generation` now takes the table
  and rule priority as parameters, and one `TableAllocator` lives in `SteadyStateCtx` for the
  process's life. The main identity keeps 251 and its rule priority unchanged — upstream's
  `ntk.conf` fixes that id. Releasing a table whose routes are still installed is rejected rather
  than leaving `cleanup`'s ownership model believing it is free.

### Compatibility

- An empty or absent `unicast_id` still reaches the main identity, unchanged. Every peer released
  before this sends one, and an unmodified upstream node sends its own encoding; that path has its
  own test asserting the real dispatcher returns the main stack, not merely that the decision
  function agrees.
- Outbound calls deliberately still send `main_identity` rather than naming a destination.
  Naming one was tried and regressed real-kernel convergence from 4 passed/3 failed to 2/5: the
  only id available at a stub is the sender's own `LinkRegistry` record of the peer, not provably
  the value the peer matches on — the failure family `AGENTS.md` records, where an id minted on one
  side is decoded against the other side's registry. A destination worth naming only exists once
  the bridge does, and then the caller knows which identity it means. `mesh.rs` is back to 4/3.

## [0.1.5]

A partial answer to the migration gap, and the packaged unit and config move into this repository.

### Added

- **A departing node now tells its neighbours instead of letting them time it out.**
  `QspnHandle::announce_destroy` ports upstream's `destroy`
  (`research/impl/vala/qspn/qspn.vala:2481-2505`): every neighbour is told the identity is going
  away, drops the arc, and lets implicit withdrawal retract whatever was only reachable through
  it. Graceful shutdown calls it, so peers reconverge immediately rather than each waiting out a
  ~28-30s liveness probe. The receiving half (`got_destroy` → `arc_remove`) was already ported and
  wired; only the announcement was missing.
  It is deliberately **not** called on migration, which is a finding rather than an omission. With
  one identity per process, the arcs the successor must enter through are the same ones the
  announcement tells peers to drop; wiring it there leaves the entering generation with nothing to
  bootstrap against, so `is_bootstrap_complete` never fires and kernel installation stays
  suppressed. Upstream can announce mid-migration only because a connectivity identity
  (`qspn.vala:2226-2505`) still serves the old position. `README.md` §6 records both halves.
- `contrib/systemd/` — the systemd unit and default config, with a README documenting every
  capability the unit grants and why. Previously these lived only in the Arch packaging
  repository, where they drifted: the unit shipped without `CAP_NET_BIND_SERVICE` for three
  releases, so no packaged install could bind the default port 269. Both files encode facts this
  tree defines — the capability set follows from where the daemon binds, and the config comments
  quote the daemon's own error text — so they belong beside the code. The Arch packages now install
  them from the release instead of carrying copies.

### Fixed

- **A config parse failure did not say which file failed.** `missing field \`nics\`` named the
  field but not the path, and the daemon is normally started by a unit the operator never typed a
  path into. Both load failures now carry it: `failed to load config /etc/ntkd/ntkd.toml: ...
  missing field \`nics\``. This matters because the packaged config ships `nics` unset on purpose —
  there is no safe default, since one naming an interface that happens to exist would quietly mesh
  over the operator's uplink, and `eth0` is the worst candidate precisely because it often does
  exist (containers, or any host booted with `net.ifnames=0`). An empty list was the alternative
  and is worse: it parses, starts, and meshes over nothing.

## [0.1.4]

One security fix, from the last open finding of 0.1.3's parity audit. Its premise turned out to be
backwards: the behaviour was justified as matching vanilla, and vanilla does the opposite.

### Fixed

- **ANDNA's anti-Sybil cap was decorative under the default configuration.** The Counter service
  caps hostname reservations per registrant (NTK_RFC 0007) by keying them to the requester's
  `client_tuple`, which `crates/ntk-andna/src/counter.rs:12-14` calls "not a self-declared,
  spoofable payload field". That holds only when the request's origin is verified, and
  `verify_origin` returned `Ok` unconditionally whenever `require_auth` was `false` — the default.
  So on a default node the cap was enforced against precisely the self-declared value it claims
  not to be.
  `require_auth`'s default is correct and unchanged: the wire `Auth` field is optional upstream, so
  *enforcing* it globally is what breaks interoperability, not carrying it. But that reasoning
  never applied to ANDNA, which has no upstream to interoperate with —
  `research/impl/vala/andna/andna.vala` is 36 lines, its `serializables.vala` is empty, and
  `ntkdrpc` declares no ANDNA method. Meanwhile vanilla, the C implementation this port
  reconstructs ANDNA from, verifies a signature on every registration
  (`research/impl/c/netsukuku/src/andna.c:829-841`, rejecting with `E_INVALID_SIGNATURE`) and again
  on the counter check (`:1181-1191`), with no toggle at all.
  `PeerService::requires_origin_auth` now lets a service demand a verified origin per request —
  per request, not per service, because `AndnaService` answers both a registration and a resolution
  behind one `exec`. ANDNA opts in the two writes. Resolution stays open, which is where vanilla
  draws the line too (`andna.c:1604-1609` verifies nothing on a lookup) and what keeps `ntkd`'s own
  `andna-resolve` working. A service that does not override it behaves exactly as before.
  **`node_key_path` is now required to register a hostname on another node.** A name a node
  hash-owns locally still registers without one: the substrate's local path never crosses the wire,
  so such a request is provably attributable. Resolution never needs a key. Both shipped
  `ntkd.toml` copies say so.

### Changed

- The logo wordmark reads `netsukuku-rs`, not `netsukuku`.

## [0.1.3]

Fixes real bugs, unlike 0.1.2. Three defects found while diagnosing a restart-looping
`ntkd.service` on Arch, all tracing back to one root event: the packaged config shipped a
placeholder `nics = ["eth0"]`, and the host had no `eth0`. The systemd unit and capability
changes below live in the Arch packaging repo (`PKGBUILD`), not here — nothing here adds a unit
file. A separate four-domain audit before tagging, unrelated to that restart-loop, found two more
real protocol defects and one broken research-tooling promise; those are below too.

### Fixed

- **A missing network interface produced an unattributable error.** `SO_BINDTODEVICE` against a
  nonexistent `eth0` returned ENODEV, which surfaced only as `ntkd: error: i/o error: No such
  device (os error 19)` — naming neither the interface nor the cause. `kernel::preflight` now
  checks interface *existence* (a down link is legitimate, a missing one is permanent) before any
  socket bind, in `node/transport.rs::start`. The same failure now reads: `configured interface
  "eth0" does not exist; available interfaces: enp0s31f6, lo, tailscale0, wlan0 — fix `nics` in
  the ntkd config`.
- **A host already using `10.0.0.0/8` collided with Netsukuku's address space silently.** That
  range is the whole space this daemon routes in, and nothing checked whether something else on
  the host was already in it — Docker's default bridge, a corporate VPN, WireGuard, Tailscale.
  Startup now lists every conflicting host address by name and ifindex. It warns rather than
  refuses: an overlap is a genuine hazard, but a 10/8 address on an idle interface is no reason to
  refuse to route, and failing would break working deployments to prevent a hypothetical one. The
  collision is no longer silent; ntkd still does not narrow the space it claims.
- **The Coordinator's migration hand-off was implemented and never used.**
  `ntk_coordinator::Manager::new` takes a `handoff: Option<HandOff>` and `Handle::hand_off`
  exports a generation's state for exactly that purpose — the protocol at `coord.vala:142-146`,
  covered by its own test (`crates/ntk-coordinator/tests/reserve.rs:393`). But `ntkd`'s only call
  site passed `None`, so every level's eldership and reservation state restarted from
  `GnodeMemory::fresh` on every rehook. `migrate` now exports the outgoing generation's state and
  hands it to its successor. Captured *before* the generation is cancelled, because `hand_off` on
  a dead actor silently returns an empty hand-off — which would have looked like it worked while
  changing nothing.
- **A permanently misconfigured host restarted forever.** `Restart=on-failure` with
  `RestartSec=3` never tripped systemd's default rate limit (5 starts/10s — 3s spacing only fits
  about 4), so the ENODEV above respawned indefinitely; the observed restart counter reached 69.
  The unit now sets `StartLimitIntervalSec=60` and `StartLimitBurst=5` (packaging only).
- **The daemon could not bind its own default port.** `port = 269` is a privileged port, and under
  `DynamicUser=yes` `AmbientCapabilities` is an explicit allow-list that named only
  `CAP_NET_ADMIN CAP_NET_RAW`. Bind failed with `i/o error: Permission denied (os error 13)`.
  Confirmed by varying only the port: 269 fails at bind with EPERM, 26900 binds and proceeds to
  netlink. `CAP_NET_BIND_SERVICE` is now in both `AmbientCapabilities` and
  `CapabilityBoundingSet` (packaging), and a shared `describe_bind_failure` helper in
  `node/transport.rs` — used by both the UDP and TCP bind sites — now reports: `failed to bind UDP
  broadcast socket on "enp0s31f6" port 269: Permission denied (os error 13) — ports below 1024 are
  privileged; grant CAP_NET_BIND_SERVICE (AmbientCapabilities in the systemd unit) or set a port
  >= 1024 in the ntkd config`.

Bind failures are diagnosed by interpreting the errno at the bind site, not by a `port < 1024`
preflight check: `net.ipv4.ip_unprivileged_port_start` can be lowered below 1024, so a static
pre-check could refuse a bind the kernel would in fact have allowed.

- **A netlink failure mid-batch could wedge routing at the same destination forever.**
  `RouteInstaller::apply` (`crates/ntkd/src/kernel/routes.rs`) recorded `self.applied` only after
  an entire batch of route mutations succeeded; a single `add_route`/`change_route` failure
  partway through a batch left `self.applied` stale against what the kernel already held. The
  next `apply()` re-diffed from that stale record, re-issued the identical mutation, hit
  `AlreadyExists` (`RealNetlink::add_route` is a plain `.add()`), and failed the same way forever
  — every destination ordered after the failing one in the diff never applied again, and one that
  vanished from a later snapshot before being recorded leaked its kernel route permanently.
  `self.applied` is now updated one mutation at a time, immediately after each kernel call
  succeeds, so a partial failure records exactly what landed and the next call retries only the
  remainder.
- **A still-hooking node could inject premature routing state into the network.**
  `handle_inbound_send_etp` (`crates/ntk-qspn/src/manager.rs`) had no bootstrap-phase gate: every
  inbound ETP was ingested and flooded onward regardless of whether this node had itself finished
  hooking, unlike upstream's `qspn.vala:2671-2707`, which drops an ETP from outside the hooking
  g-node outright and holds — never forwards — one with no path yet at the host g-node's level.
  That gate is now ported: only an ETP that actually qualifies clears bootstrap and
  ingests/forwards; the rest are ignored or held exactly as upstream does.
- **The research corpus had no documented way to regenerate.** `research/README.md` claimed the
  vendored, gitignored `research/impl/`/`research/related/` trees were "regenerable from the
  clone list" while the file contained no clone list — a fresh clone had no way to restore the
  corpus every doc comment in the workspace cites by `path:line`. `research/README.md` now
  carries the verified clone list for all 18 `impl/vala/` repos, the pinned `impl/c/netsukuku`
  checkout, and `related/`.

### Known broken

- **3 of the 7 real-kernel `mesh.rs` tests still fail.** This release took the suite from 2 passed
  / 5 failed to **4 passed / 3 failed** (serially, ~830s, under
  `unshare --net --map-root-user -- cargo test -p ntkd --test mesh -- --ignored
  --test-threads=1`) by fixing five root causes, listed above.
  `chain_of_four_converges_to_exact_multi_hop_routes`,
  `level1_destination_installs_correct_cidr_route` and
  `partition_signals_split_only_after_the_documented_debounce` now pass.
  All three remaining failures share ONE cause: the migration gap this release documents. Nothing
  announces that an identity has retired, so peers keep routing to a position its owner has left.
  Both merge scenarios keep a sibling's stale pre-migration position; the severance scenario keeps
  a level-1 route to the now-unreachable other slot across the re-hook the partition triggers.
  Upstream announces retirement via the connectivity identity (`qspn.vala:2226-2505`) that this
  port does not implement, so these three cannot go green without that work. QSPN's implicit
  withdrawal is not at fault — reviewed line-by-line against `qspn.vala:1074-1232` and `:1334-1816`
  and found faithful.
  Two attributions of the severance failure were investigated and disproven along the way, noted so
  they are not re-run: `RpcError::is_remote()` is `matches!(self, Remote(_))`, so `ConnectionClosed`
  reads `false` for a peer-initiated EOF as well and never distinguished local from remote; and the
  shared-per-neighbour connection multiplexing is not implicated — new `ntk-rpc` debug logging
  showed a server-side cancellation from a node that finished observing early and tore itself down,
  because the scenario had a barrier before the sever and none after it.
  Position collisions were investigated and ruled out: negotiated positions are collision-free by
  construction (`crates/ntk-coordinator/src/actor.rs:94`) and colliding bootstrap positions are
  resolved by arc retry — both verified in live traces, despite `NodeId(601)`/`NodeId(603)` and
  `NodeId(601)`/`NodeId(604)` genuinely colliding under `Topology([8])`.
  These failures predate this release, verified by re-running with its fixes stashed out. They are
  invisible in CI because the entire privileged tier is `#[ignore]`d and that job has never been
  observed to pass. Left red rather than weakened, per the project's convention; `README.md` §6
  records the same finding so a reader cannot mistake the green default run for a working mesh.

## [0.1.2]

Fixes what 0.1.1 shipped wrong. No Rust code changed: `git diff v0.1.1..v0.1.2 -- 'crates/**/*.rs'`
is empty.

### Fixed

- **Every crates.io page rendered empty.** No README, no keywords, no categories on any of the
  twelve crates — `GET /crates/<name>/0.1.1/readme` returned 403 across the board. The cause is
  that `readme` resolves relative to a crate's own directory, so a path to the workspace-root
  README is simply never packaged. Each crate now carries its own `README.md`, plus keywords and
  categories chosen per crate for discovery rather than a shared generic set.
- The container release never pushed: the build job ran `docker/login-action` *after*
  `build-push-action`, so buildx authenticated anonymously and GHCR answered 403. The image built
  correctly on both architectures — it was purely step order, and it surfaced only on the first
  tag build to reach the push path.
- The static-linking guard rejected a valid binary. It grepped `file` output for
  "statically linked" and a correct static PIE is described as "static-pie linked" — wording that
  varies by `file` version and architecture, which is why the aarch64 leg passed while x86_64
  failed on the same kind of binary. It now tests for an ELF `INTERP` segment: a static binary has
  none, a dynamic one always has exactly one naming its loader.

### Added

- `homepage` on all twelve crates.

### Note on the `ntkd` docs page

`ntkd` declares a `[lib]` and a `[[bin]]` both named `ntkd`, so rustdoc documents the library and
drops the binary — `docs.rs/ntkd/latest/src/ntkd/main.rs.html` is a 404 while `lib.rs.html` is not.
Nothing is lost: `src/main.rs` is six lines delegating to `ntkd::node::main()`. But it does mean
the rendered docs show a library API and no CLI, which is why the new `ntkd` README documents the
command line. The full source remains browsable at `docs.rs/crate/ntkd/<version>/source/`.

## [0.1.1]

**No Rust code changed in this release.** The only changes under `crates/` are doc comments and
the twelve manifests switching to workspace version inheritance; every `.rs` change is a comment.
The version exists so the workspace stays in lockstep and so a tag can carry the first binary and
container artifacts, which 0.1.0 never produced.

### Added

- Distribution packaging: Arch Linux (source and `-bin`), an OpenWrt package with a procd init
  script and UCI config, an OCI container image, and static `x86_64`/`aarch64` musl binaries
  attached to the GitHub release.
- `[profile.dist]` for shipped binaries — fat LTO, `codegen-units = 1`, `opt-level = "z"`,
  `panic = "abort"`, stripped. Measured on x86_64 for `ntkd`: **10.44 MiB → 3.90 MiB**, a 63%
  reduction. `release` is deliberately left conventional so `cargo install` and Arch's
  `options=(!lto)` behave as expected.
- `CHANGELOG.md` (this file).

### Fixed

- **181 dead links in the crate documentation.** Every one rendered on docs.rs: 128 links pointed
  at private items (which silently degrade to plain text), 41 were unresolved — including prose
  such as `pos[0]` that rustdoc was parsing as a link — and 3 were redundant or ambiguous targets.
  `rustdoc::{broken_intra_doc_links, private_intra_doc_links, redundant_explicit_links}` are now
  denied workspace-wide and `cargo doc` runs in CI, so they cannot return.
- The release workflow published only 5 of 12 crates on 0.1.0 and could not resume. crates.io
  rate-limits *new* crate creation to a burst of five, then one per ten minutes, so a twelve-crate
  first publish cannot complete in one pass. Publishing is now per crate, skips versions already
  on the registry, and treats `429` as wait-and-retry — which makes a half-finished release
  re-runnable instead of requiring a version bump.
- The publish order was derived by parsing `cargo publish --dry-run` output, which matched nothing
  in CI because the workflow sets `CARGO_TERM_COLOR: always` and cargo wrapped the marker in ANSI
  escapes. Colour is now forced off for that step and residual escapes are stripped.

### Changed

- Crate versions are inherited from `[workspace.package]` rather than repeated in twelve
  manifests, so a release touches one file instead of twenty-three.
- `README.md` §4 no longer presents three unrelated kinds of absence as one deficit list. Measured
  against the normative Vala rewrite the port is essentially complete; what is missing is either
  absent from upstream too (IPv6, the unported Alpt-era RFC ideas), deliberately dropped
  (everything reachable only through `iptables`, which the no-subprocess rule excludes by design),
  or — in exactly one case — a real gap: the second protocol stack after a g-node migration.
- Two README claims were corrected. Live g-node migration is **not** unwired — `ntkd`'s lifecycle
  does drive `prepare_migration`/`migrate`; only the second protocol stack is missing. And "no
  transport crypto", while still true of the transport, no longer described the whole picture.

## [0.1.0]

First release: a Rust implementation of the current Netsukuku protocol stack — QSPN v2 routing,
Neighborhood discovery, Identities, Hooking, Coordinator, PeerServices, and ANDNA — ported from
Luca Dionisi's Vala rewrite rather than the 2005 C daemon. Twelve crates, ~44k lines, 527 tests.

An L3 routing protocol, not a TUN overlay: the daemon owns real kernel routing tables and reaches
them over native netlink, never by shelling out to `ip`.

Only five of the twelve crates reached crates.io under this version, for the rate-limit reason
described under 0.1.1. Use 0.1.2 instead.

[0.1.7]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/M0Rf30/netsukuku-rs/releases/tag/v0.1.0
