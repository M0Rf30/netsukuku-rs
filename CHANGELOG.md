# Changelog

All notable changes to this project are documented here. Versions follow [Semantic
Versioning](https://semver.org/spec/v2.0.0.html); the twelve `ntk-*` crates and `ntkd` are
released in lockstep, so they always share a version even when only some of them changed.

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

[0.1.2]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/M0Rf30/netsukuku-rs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/M0Rf30/netsukuku-rs/releases/tag/v0.1.0
