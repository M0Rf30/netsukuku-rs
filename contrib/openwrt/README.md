# netsukuku-rs on OpenWrt

`net/netsukuku-rs/` is a standard OpenWrt package for `ntkd`, the Netsukuku L3 mesh routing
daemon. It is not part of any OpenWrt feed by default — add this directory as a custom feed.

## Adding it to a feed

Add a line to your OpenWrt buildroot's `feeds.conf` (or `feeds.conf.default`), pointing at this
directory or a checkout of it:

```
src-link netsukuku-rs /path/to/netsukuku-rs/contrib/openwrt
```

(`src-link` symlinks a local path; use `src-git https://github.com/M0Rf30/netsukuku-rs.git;main`
with a subdirectory pointer instead if the buildroot can't see a local checkout.)

Then, from the OpenWrt buildroot root:

```
./scripts/feeds update netsukuku-rs
./scripts/feeds install netsukuku-rs
```

It then appears in `make menuconfig` under `Network ---> netsukuku-rs`, with a `Configuration`
submenu for the UPX-compression toggle described below.

## Cross-compilation

The package builds with OpenWrt's own Rust toolchain (`lang/rust/rust-package.mk` in the
`packages` feed) targeting whatever architecture is selected in `menuconfig` — no separate
cross-compiler setup is needed beyond the ordinary OpenWrt build prerequisites. Codegen
(`ntk-proto`) is pure-Rust `protox`; **no host `protoc` is required or should be added** as a
build dependency.

## Binary size

`ntkd` is built with the workspace's `dist` Cargo profile (fat LTO, `codegen-units = 1`,
`opt-level = "z"`, `panic = "abort"`, stripped), then optionally compressed with
`upx --best --lzma` (menuconfig option `NETSUKUKU_RS_UPX`, on by default). Measured on x86_64:

| variant                                    | size        |
| ------------------------------------------ | ----------- |
| `release` baseline (`lto = "thin"`)        | 10.44 MiB   |
| `dist` profile (fat LTO + `opt-level="z"` + `panic="abort"`, stripped) | 3.90 MiB |
| `dist` + `upx --best --lzma`               | **1.21 MiB** (startup ~69 ms) |

UPX is applied on OpenWrt only, never for the Arch packages, downloadable release binaries, or
container images built elsewhere in this repository: OpenWrt targets are flash-constrained and
`ntkd` is a long-running daemon that pays the one-time decompression cost once, not per request.
Everywhere else a UPX image can't be demand-paged, so the whole decompressed binary stays
resident instead of only the pages actually touched — not a good trade when flash isn't scarce.

**What was actually measured above is x86_64 only** (the environment this package was authored
in has no OpenWrt target toolchain or hardware available). The `dist`-profile numbers should
carry over to any target rustc/LLVM supports, since they come from generic codegen flags, not
anything x86_64-specific; the `upx --best --lzma` ratio is `[INFERENCE]` for other
architectures — UPX's achievable compression ratio and even its support for a given target's
instruction set both vary by architecture (some targets, e.g. some MIPS/RISC-V variants, are not
supported by every UPX release at all). Nothing in this package Makefile, init script, or UCI
config has been built or run against a real OpenWrt image or device; verify on your actual target
before deploying.
