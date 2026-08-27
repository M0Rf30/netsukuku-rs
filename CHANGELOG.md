# Changelog

All notable changes to this project are documented here. Versions follow [Semantic
Versioning](https://semver.org/spec/v2.0.0.html); the twelve `ntk-*` crates and `ntkd` are
released in lockstep, so they always share a version even when only some of them changed.

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

- **5 of the 7 real-kernel `mesh.rs` tests fail, and this release does not fix them.** Run
  serially under `unshare --net --map-root-user -- cargo test -p ntkd --test mesh -- --ignored
  --test-threads=1`, the suite reports 2 passed / 5 failed in ~500s. Four fail deterministically —
  `partition_clean_severance_drops_exactly_the_unreachable_destinations`,
  `partition_signals_split_only_after_the_documented_debounce`,
  `two_level_gnode_migrates_as_a_unit_into_merged_network`,
  `two_star_groups_merge_into_one_network` — covering partition detection, g-node migration and
  network merge. Two more (`chain_of_four_converges_to_exact_multi_hop_routes`,
  `level1_destination_installs_correct_cidr_route`) swap pass/fail between identical runs, so they
  are flaky rather than broken. The failures predate this release: re-running the suite with
  0.1.3's two fixes stashed out produces the same 2/5 split, with the same two flaky tests
  swapping. They are invisible in CI because the entire privileged tier is `#[ignore]`d and that
  job has never been observed to pass. Left red and undiagnosed rather than weakened, per the
  project's convention; `README.md` §6 records the same finding so a reader cannot mistake the
  green default run for working multi-hop routing.

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

[0.1.3]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/M0Rf30/netsukuku-rs/releases/tag/v0.1.0
