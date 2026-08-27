# syntax=docker/dockerfile:1
#
# OCI image for `ntkd`, the Netsukuku routing daemon. Buildable with both podman and docker.
#
# ntkd is an L3 routing protocol, not a TUN overlay or a userspace network stack: the daemon
# installs and mutates REAL kernel routes over netlink (crates/ntk-netlink). It needs
# CAP_NET_ADMIN inside the container for route/address/rule management, and benefits from
# CAP_NET_RAW (the neighbourhood RTT probe uses an unprivileged ICMP datagram socket when the
# kernel allows it and falls back to a raw ICMP socket -- gated by CAP_NET_RAW -- otherwise;
# lacking both, it just uses a fixed fallback cost instead of a measured RTT).
#
# Running this image in Docker/Podman's DEFAULT bridge network sandbox is close to useless: the
# daemon starts, binds its RPC port, and looks healthy, but the `--nic`/config interface names
# refer to the HOST's interfaces, not the container's private veth, so it has nothing to route
# for. `--network host` plus `--cap-add=NET_ADMIN --cap-add=NET_RAW` is what makes this an
# actual routing node rather than a sandboxed demo. Full invocation and rationale:
# contrib/container/README.md.

ARG RUST_VERSION=1.98
ARG DEBIAN_CODENAME=bookworm

########################################################################
# Builder: pure-Rust codegen (protox -- no `protoc` package, ever), static musl output built
# with the size-optimized `dist` profile from the root Cargo.toml (fat LTO, opt-level "z",
# panic=abort, stripped; ~63% smaller than a plain `release` build on x86_64).
########################################################################
FROM docker.io/library/rust:${RUST_VERSION}-${DEBIAN_CODENAME} AS builder

# BuildKit/Buildah populate TARGETARCH automatically from --platform (or the host platform on a
# plain single-arch build) -- no manual --build-arg needed.
ARG TARGETARCH

# `musl-tools` ships musl-gcc for the IMAGE's own architecture (Debian carries a separate
# package build per arch). Building each platform natively (see .github/workflows/container.yml)
# rather than cross-compiling under QEMU means this one `apt-get install` always resolves to the
# right linker with no cross-toolchain setup.
RUN apt-get update \
    && apt-get install --no-install-recommends -y musl-tools \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETARCH}" in \
      amd64) rust_target=x86_64-unknown-linux-musl ;; \
      arm64) rust_target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && echo "${rust_target}" >/rust_target \
    && rustup target add "${rust_target}"

WORKDIR /src
COPY . .

# musl targets default to fully static (crt-static) linking, so no extra RUSTFLAGS are needed
# to get a binary with zero dynamic dependencies -- `ldd` on the result reports "not a dynamic
# executable".
RUN rust_target="$(cat /rust_target)" \
    && cargo build --profile dist --target "${rust_target}" -p ntkd \
    && install -Dm755 "target/${rust_target}/dist/ntkd" /out/usr/local/bin/ntkd \
    && install -Dm644 LICENSE /out/LICENSE

# `FROM scratch` starts with no filesystem at all, so a writable /tmp for the default
# `--status-socket /tmp/ntkd.sock` has to be materialized here and copied in explicitly. World
#-writable + sticky, same as a normal /tmp, so it works regardless of which UID `ntkd` runs as.
RUN mkdir -p /out/tmp && chmod 1777 /out/tmp

########################################################################
# Final. `scratch` is the right base, not a minimal distro image, once each usual reason for a
# fuller base is checked against what this daemon actually does:
#   - CA certificates: not needed. `ntk-rpc` is a cleartext protobuf wire protocol end to end --
#     there is no TLS anywhere in this stack (see AGENTS.md / root Cargo.toml dependency list).
#   - /etc/resolv.conf + NSS: not needed. Peers are named by NIC + Netsukuku address (ANDNA is
#     this project's OWN hostname system, not host DNS); nothing here calls getaddrinfo.
#   - /etc/passwd: not needed to run non-root. `ntkd` never resolves a UID to a username
#     (grepped: no getpwuid/dirs::home_dir/etc.), and container runtimes accept a bare numeric
#     `USER` with no passwd entry -- Linux capability and file-permission checks are UID-based,
#     not name-based.
#   - /tmp: needed, for the default status socket -- handled above, not a reason to add a base.
# Net result: static binary + LICENSE + one writable directory, nothing else.
########################################################################
FROM scratch

ARG NTKD_VERSION=0.1.0
ARG NTKD_REVISION=unknown

LABEL org.opencontainers.image.title="ntkd" \
      org.opencontainers.image.description="Netsukuku mesh-routing daemon (QSPN v2, Hooking, Coordinator, PeerServices, ANDNA)" \
      org.opencontainers.image.source="https://github.com/M0Rf30/netsukuku-rs" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      org.opencontainers.image.version="${NTKD_VERSION}" \
      org.opencontainers.image.revision="${NTKD_REVISION}"

COPY --from=builder /out/ /

# Fixed, unallocated-in-/etc/passwd numeric UID:GID (the common "nonroot" convention popularized
# by Google's distroless images). CAP_NET_ADMIN/CAP_NET_RAW granted via `--cap-add` at `run`
# time apply to the container's initial process regardless of its UID -- Linux capabilities are
# independent of the setuid/root model, so this does not need to run as UID 0 to manage routes.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/ntkd"]
# No config ships in the image (there is no sane default topology to bake in). Mount one at
# this path, or override the whole command -- see contrib/container/README.md.
CMD ["run", "--config", "/etc/ntkd/config.toml"]
