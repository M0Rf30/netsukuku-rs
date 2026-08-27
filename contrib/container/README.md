# Container image

`Containerfile` (repo root) builds a `FROM scratch` OCI image around a single fully-static
(musl) `ntkd` binary, compiled with the `dist` profile from the root `Cargo.toml` (fat LTO,
`opt-level = "z"`, `panic = "abort"`, stripped). Buildable with either podman or docker; CI
(`.github/workflows/container.yml`) publishes multi-arch (`linux/amd64`, `linux/arm64`) images to
`ghcr.io/m0rf30/netsukuku-rs` on every `v*` tag.

UPX compression is **not** applied to this image (unlike the OpenWrt package). A UPX-packed
binary decompresses itself entirely into memory on startup and can't be demand-paged from its
container layer afterward — a bad trade when registry/disk space isn't the scarce resource, which
it isn't for a container host the way it is for OpenWrt flash.

## Why this daemon needs more than the default sandbox

`ntkd` is an L3 routing daemon, not a TUN overlay or a userspace network stack: it reads real
neighbours off real NICs and installs real routes into the kernel routing table over netlink
(`crates/ntk-netlink`). Concretely that means:

- **`--cap-add=NET_ADMIN`** — required. Every route/address/rule mutation goes through netlink
  calls that need `CAP_NET_ADMIN`; without it, `ntkd` starts and then fails (or silently does
  nothing useful) the first time it tries to touch the routing table.
- **`--cap-add=NET_RAW`** — recommended, not required. The neighbourhood module's RTT probe uses
  an unprivileged ICMP datagram socket when the kernel allows it, and falls back to a raw ICMP
  socket (gated by `CAP_NET_RAW`) otherwise. Lacking both, it just uses a fixed fallback cost
  instead of a measured one — degraded, not broken.
- **`--network host`** — required for the daemon to do anything useful. In Docker/Podman's
  default bridge network, `ntkd`'s `--nic`/config interface names refer to the container's
  private veth pair, not the host's real interfaces. The container will start, bind its RPC port,
  and *look* healthy while routing nothing at all — the daemon has no visibility into whatever
  network you actually wanted it to join. Host networking is what gives it that visibility.

Running this image without all three is a common enough mistake to call out explicitly: you will
get a process that appears to work and does nothing.

## Build

```sh
podman build -t ntkd:0.1.0 \
  --build-arg NTKD_VERSION=0.1.0 \
  --build-arg NTKD_REVISION="$(git rev-parse --short HEAD)" \
  -f Containerfile .
# or: docker build ...
```

`NTKD_VERSION`/`NTKD_REVISION` only feed the `org.opencontainers.image.version`/`.revision`
labels; the binary itself is unaffected. Cross-arch builds (e.g. building `linux/arm64` on an
`amd64` host) need `--platform linux/arm64` plus QEMU user-mode emulation (`buildx`); the release
workflow avoids that cost entirely by building each architecture on a native runner instead — see
the comment in `.github/workflows/container.yml`.

## Run

```sh
podman run -d --name ntkd \
  --cap-add=NET_ADMIN --cap-add=NET_RAW \
  --network host \
  -v /path/to/ntkd.toml:/etc/ntkd/config.toml:ro \
  ghcr.io/m0rf30/netsukuku-rs:0.1.0
# or: docker run ... (identical flags)
```

The image ships no config (there is no sane default topology to bake in) — mount one at
`/etc/ntkd/config.toml`, or override the command entirely, e.g.:

```sh
podman run --rm --cap-add=NET_ADMIN --network host \
  -v /path/to/ntkd.toml:/etc/ntkd.toml:ro \
  ghcr.io/m0rf30/netsukuku-rs:0.1.0 \
  run --config /etc/ntkd.toml --nic wlan0 --log-level debug
```

`ntkd status`/`andna-register`/`andna-resolve` talk to a running daemon over its unix status
socket (`/tmp/ntkd.sock` by default), so run them with `podman exec`/`docker exec` against the
same container rather than as separate `run` invocations.

## Image size

Measured locally on x86_64 with `podman build` + `podman images`:

<!-- SIZE_PLACEHOLDER -->

For comparison: the bare `dist`-profile `ntkd` binary alone (stripped, no UPX) is 3.90 MiB; the
image adds only its `LICENSE` copy and OCI metadata on top of `scratch`.
