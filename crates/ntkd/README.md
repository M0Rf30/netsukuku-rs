# ntkd

The Netsukuku routing daemon — the binary you actually run, and the only composition
root of the [`netsukuku-rs`](https://github.com/M0Rf30/netsukuku-rs) workspace.

Netsukuku is an **L3 mesh routing protocol**, not a VPN or a TUN overlay: `ntkd`
programs a node's real kernel routing tables over netlink, using QSPN v2 for
route discovery, a bootstrap/join (Hooking) protocol to enter or merge networks,
and PeerServices/Coordinator/ANDNA for DHT-backed position reservation and
distributed hostnames. `ntkd` wires all eleven library crates (`ntk-common`
through `ntk-andna`) together; every other crate in the workspace is a
dependency of this one.

## Installing

- `cargo install ntkd`
- Arch Linux packages (`netsukuku-rs`, built from source, and `netsukuku-rs-bin`)
  are maintained at [M0Rf30/PKGBUILD](https://github.com/M0Rf30/PKGBUILD).
- An OpenWrt package lives in this repo under `contrib/openwrt/net/netsukuku-rs/`.
- A multi-arch container image is published to `ghcr.io/m0rf30/netsukuku-rs` on
  tagged releases (see `.github/workflows/container.yml` and the repo's
  `Containerfile`).

## Running it

```text
ntkd run --config <PATH> [--nic <IFACE>]... [--log-level <LEVEL>] [--status-socket <PATH>]
ntkd status [--socket <PATH>]
ntkd andna-register <HOSTNAME> [--socket <PATH>]
ntkd andna-resolve <HOSTNAME> [--socket <PATH>]
```

- `run` starts the daemon in the foreground. `--nic` is repeatable and, when
  given, overrides the config file's `nics` list entirely. `--log-level` is one
  of `error`/`warn`/`info`/`debug`/`trace`. `--status-socket` sets the unix
  socket path the other subcommands talk to.
- `status`, `andna-register`, and `andna-resolve` are thin clients that connect
  to a running daemon's status socket (default `/tmp/ntkd.sock`).

Programming real routes requires `CAP_NET_ADMIN`. Measuring real neighbour RTT
(the ICMP probe behind route cost) additionally benefits from `CAP_NET_RAW`, or
membership in Linux's `ping_group_range`; without either, arcs stay usable but
their liveness probe can't produce a cost sample.

### Minimal config

```toml
# Per-level g-node sizes, index 0 innermost (see ntk_common::Topology::new).
gsizes = [16, 16]
# Interfaces to monitor for neighbours.
nics = ["eth0"]
# TCP/UDP port every RPC transport binds to.
port = 269

# Optional. Omitting andna_key_path refuses ANDNA registration outright rather
# than silently disabling it.
# andna_key_path = "/etc/ntkd/andna.key"

# Optional. Signs outbound RPCs; a separate key from andna_key_path so rotating
# a compromised transport key doesn't forfeit registered hostnames.
# node_key_path = "/etc/ntkd/node.key"

# Rejects inbound RPCs without a valid auth block. Defaults to false — the only
# setting interoperable with an unmodified upstream Vala node — and requires
# node_key_path when enabled.
# require_auth = false
```

See `crates/ntkd/src/kernel/config.rs` for the authoritative field list.

## Notes on the docs.rs page

docs.rs renders `ntkd`'s library API, not this CLI: the crate declares both a
`[lib]` and a `[[bin]]` named `ntkd`, and rustdoc documents the library,
dropping the binary target. `src/main.rs` is six lines delegating into
`ntkd::node::main()` — this README is the only place the CLI itself is
documented.

## License

GPL-3.0-or-later. Source and issue tracker:
<https://github.com/M0Rf30/netsukuku-rs>.
