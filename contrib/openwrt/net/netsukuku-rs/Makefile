include $(TOPDIR)/rules.mk

PKG_NAME:=netsukuku-rs
PKG_VERSION:=0.1.1
PKG_RELEASE:=1

# All 12 workspace crates are published on crates.io at 0.1.1, but `ntkd` is a workspace member
# with path deps on its 11 siblings plus a checked-in root Cargo.lock (`cargo install --locked`
# needs both) — a crates.io tarball of `ntkd` alone has neither, so this builds straight from the
# tagged git commit instead of a release tarball.
#
# OpenWrt's download.pl hashes the tarball *it* produces from the git checkout, which depends on
# tooling/timestamps this sandbox cannot reproduce byte-for-byte; PKG_MIRROR_HASH:=skip is the
# documented escape hatch for that (see include/download.mk's literal "skip" check) rather than a
# fabricated value — do not replace it with an invented hash.
PKG_SOURCE_URL:=https://github.com/M0Rf30/netsukuku-rs.git
PKG_SOURCE_PROTO:=git
PKG_SOURCE_VERSION:=v$(PKG_VERSION)
PKG_MIRROR_HASH:=skip

PKG_MAINTAINER:=M0Rf30
PKG_LICENSE:=GPL-3.0-or-later
PKG_LICENSE_FILES:=LICENSE

PKG_BUILD_DEPENDS:=rust/host
PKG_BUILD_PARALLEL:=1

# Tracked so opkg rebuilds this package when the UPX menuconfig toggle changes, not just when
# package selection changes.
PKG_CONFIG_DEPENDS:= \
	CONFIG_PACKAGE_netsukuku-rs \
	CONFIG_NETSUKUKU_RS_UPX

# Codegen (`ntk-proto`) is pure-Rust `protox`, not a `protoc` binding — do NOT add a
# `protobuf/host` (or any `+protobuf-c`) build/runtime dependency here; there is nothing for it
# to satisfy and it would just be dead weight.
include $(INCLUDE_DIR)/package.mk
include $(TOPDIR)/feeds/packages/lang/rust/rust-package.mk

# rust-package.mk defaults CARGO_PKG_PROFILE to plain "release" (or "dev" under
# CONFIG_DEBUG); override it to the workspace's `dist` profile (fat LTO, codegen-units=1,
# opt-level="z", panic="abort", stripped — see root Cargo.toml), measured at 3.90 MiB for
# `ntkd` vs 10.44 MiB for `release`. `cargo install --root` always places the binary under
# <root>/bin/ regardless of profile name, so nothing downstream needs to change.
CARGO_PKG_PROFILE:=dist

# `ntkd` is a workspace member at crates/ntkd, not the repository root.
define Build/Compile
	$(call Build/Compile/Cargo,crates/ntkd)
endef

define Package/netsukuku-rs
  SECTION:=net
  CATEGORY:=Network
  TITLE:=Netsukuku L3 mesh routing daemon
  URL:=https://github.com/M0Rf30/netsukuku-rs
  DEPENDS:=$(RUST_ARCH_DEPENDS)
  MENU:=1
endef

define Package/netsukuku-rs/config
	source "$(SOURCE)/Config.in"
endef

define Package/netsukuku-rs/description
  Netsukuku is a peer-to-peer, self-organizing mesh routing protocol with no central
  authority. This daemon (`ntkd`) is a full Rust reimplementation of the original Vala
  reference: it builds and maintains real Linux kernel routes over netlink (rtnetlink route,
  address, and rule tables) as the mesh topology changes, rather than tunnelling traffic
  through a userspace TUN/overlay device.
endef

define Package/netsukuku-rs/install
	$(INSTALL_DIR) $(1)/usr/sbin
	$(INSTALL_BIN) $(PKG_INSTALL_DIR)/bin/ntkd $(1)/usr/sbin/ntkd
	$(INSTALL_DIR) $(1)/etc/init.d
	$(INSTALL_BIN) ./files/netsukuku-rs.init $(1)/etc/init.d/netsukuku-rs
	$(INSTALL_DIR) $(1)/etc/config
	$(INSTALL_CONF) ./files/netsukuku-rs.config $(1)/etc/config/netsukuku-rs
ifneq ($(CONFIG_NETSUKUKU_RS_UPX),)
	# UPX is applied only here (OpenWrt), never for Arch packages, release binaries, or
	# container images: flash is the binding constraint on OpenWrt targets and ntkd is a
	# long-running daemon, so the ~69ms one-time --lzma decompression startup cost (measured
	# 3.90 MiB -> 1.21 MiB) is amortised to nothing. Everywhere else, a UPX image can't be
	# demand-paged, so the whole decompressed binary stays resident instead of only the pages
	# actually touched — a bad trade when flash isn't the scarce resource. Guarded so a build
	# host without `upx` still produces a working (merely larger) package.
	upx --best --lzma $(1)/usr/sbin/ntkd || true
endif
endef

define Package/netsukuku-rs/conffiles
/etc/config/netsukuku-rs
endef

$(eval $(call BuildPackage,netsukuku-rs))
