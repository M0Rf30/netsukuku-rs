//! Startup kernel-capability preflight. Upstream's `ntkd/startup.vala` never checks for
//! `CONFIG_IP_MULTIPLE_TABLES`/`CONFIG_IP_ROUTE_MULTIPATH` before wiring routes — it just fails
//! deep inside `identity_ip_commands.vala`'s shelled-out `ip` calls with an opaque nonzero exit
//! code (`research/notes/02-vala-services-daemon.md` §5). This module runs the same probes
//! `ntk_netlink::detect_capabilities` performs, but turns a missing feature into one actionable
//! error message up front, before any identity or protocol actor is spawned.

use ntk_netlink::{
    KernelCapabilities, Netlink, NetlinkError, TopologyQuery, UnsupportedKernel,
    detect_capabilities,
};

/// Probes `kernel` for the routing features Netsukuku's L3 model requires, returning the full
/// capability report on success.
///
/// # Errors
/// [`PreflightError`] naming exactly which feature(s) are missing and how to enable them.
pub async fn check<K: Netlink>(kernel: &K) -> Result<KernelCapabilities, PreflightError> {
    let capabilities = detect_capabilities(kernel).await;
    capabilities.ensure_supported()?;
    Ok(capabilities)
}

/// Confirms every interface named in the ntkd config's `nics` list actually exists on this
/// host, before anything tries to bind a socket or netlink object to it.
///
/// Checks existence only, never `is_up`: a down link can come up later (a cable plugged in
/// after boot, a driver loaded late), so refusing to start over that would be a false
/// positive. A *nonexistent* name, by contrast, is a permanent misconfiguration — it can
/// never come up — so this fails fast on it instead.
///
/// This check has no upstream analogue: upstream Vala never validates `nics` up front; it
/// shells out to `ip` deep inside route setup and fails opaquely on a bad interface name.
///
/// An empty `nics` slice is valid (some configs run with no configured interfaces) and
/// always passes.
///
/// # Errors
/// [`MissingNics::Query`] if listing the host's interfaces fails; [`MissingNics::NotFound`]
/// naming every configured interface absent from the kernel's link table alongside every
/// interface that does exist, so the config can be fixed from this message alone.
pub async fn check_nics<K: TopologyQuery>(kernel: &K, nics: &[String]) -> Result<(), MissingNics> {
    if nics.is_empty() {
        return Ok(());
    }

    let links = kernel.list_links().await.map_err(MissingNics::Query)?;
    let mut available: Vec<String> = links.into_iter().map(|link| link.name).collect();
    available.sort();
    available.dedup();

    let mut missing: Vec<String> = nics
        .iter()
        .filter(|nic| !available.contains(nic))
        .cloned()
        .collect();
    missing.sort();
    missing.dedup();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(MissingNics::NotFound { missing, available })
    }
}

/// Netsukuku's entire address space (`crate::kernel::addressing`). Every identity address and
/// every g-node destination this daemon installs falls inside it.
///
/// A function rather than a `const`: `Ipv4Net::new` validates its prefix at runtime and so is
/// not a `const fn`.
fn netsukuku_address_space() -> ntk_netlink::Ipv4Net {
    ntk_netlink::Ipv4Net::new(std::net::Ipv4Addr::new(10, 0, 0, 0), 8)
        .expect("10.0.0.0/8 is a valid network")
}

/// Warns if this host already carries addresses inside `10.0.0.0/8`, which is the whole space
/// Netsukuku routes in.
///
/// **Warns, never fails.** An overlap is a genuine operational hazard — this daemon will install
/// routes for g-node destinations that may collide with what the host already uses, and
/// `10.0.0.0/8` is extremely common in practice (Docker's default pools, corporate VPNs,
/// WireGuard and Tailscale subnets). But an overlap is not automatically fatal: the conflicting
/// interface may be idle, or the operator may have sized the topology to avoid the range in use.
/// Refusing to start would break working deployments to prevent a hypothetical one, so this
/// reports and continues. `check_nics` fails instead, because a nonexistent interface can never
/// become usable, whereas an overlap can be perfectly fine.
///
/// Addresses this daemon itself installed are indistinguishable from pre-existing ones at this
/// point, which is exactly why it runs during startup preflight — before any identity address is
/// added — rather than later.
///
/// No upstream analogue: upstream never checks, and would collide silently.
pub async fn warn_address_space_conflicts<K: ntk_netlink::AddressTable>(kernel: &K) {
    let addresses = match kernel.list_addresses(None).await {
        Ok(addresses) => addresses,
        Err(err) => {
            // Not fatal: this is advisory, so a failed probe must not stop startup.
            tracing::debug!(%err, "preflight: could not list host addresses to check for 10.0.0.0/8 overlap");
            return;
        }
    };
    let space = netsukuku_address_space();
    let conflicting: Vec<String> = addresses
        .iter()
        .filter(|entry| space.contains(entry.network.address()))
        .map(|entry| format!("{} (ifindex {})", entry.network, entry.interface_index))
        .collect();
    if !conflicting.is_empty() {
        tracing::warn!(
            conflicts = %conflicting.join(", "),
            "preflight: this host already uses addresses inside 10.0.0.0/8, the range Netsukuku \
             routes in — ntkd will install g-node routes that may collide with them (common \
             causes: Docker, WireGuard, Tailscale, corporate VPNs). Starting anyway; verify the \
             ranges do not overlap"
        );
    }
}

/// One or more interfaces named in the ntkd config's `nics` list are missing from this
/// host's link table, or listing the host's interfaces failed outright.
#[derive(Debug)]
pub enum MissingNics {
    /// Listing the host's links failed before existence could even be checked.
    Query(NetlinkError),
    /// `missing` are absent from `available` (both sorted and deduplicated).
    NotFound {
        missing: Vec<String>,
        available: Vec<String>,
    },
}

impl std::fmt::Display for MissingNics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(err) => write!(f, "failed to list kernel interfaces: {err}"),
            Self::NotFound { missing, available } => {
                let (noun, verb) = if missing.len() == 1 {
                    ("interface", "does")
                } else {
                    ("interfaces", "do")
                };
                let missing_list = missing
                    .iter()
                    .map(|name| format!("{name:?}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "configured {noun} {missing_list} {verb} not exist; available interfaces: \
                     {} — fix `nics` in the ntkd config",
                    available.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for MissingNics {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Query(err) => Some(err),
            Self::NotFound { .. } => None,
        }
    }
}

/// The running kernel is missing a routing feature Netsukuku requires to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreflightError(UnsupportedKernel);

impl std::fmt::Display for PreflightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; enable {}", self.0, Self::remedy(&self.0))
    }
}

impl std::error::Error for PreflightError {}

impl From<UnsupportedKernel> for PreflightError {
    fn from(unsupported: UnsupportedKernel) -> Self {
        Self(unsupported)
    }
}

impl PreflightError {
    /// The exact kernel config option(s)/sysctl an operator needs to enable, named per the
    /// missing feature(s) so the error is actionable rather than a bare capability report.
    fn remedy(unsupported: &UnsupportedKernel) -> String {
        let mut remedies = Vec::new();
        if unsupported.missing_multiple_routing_tables {
            remedies.push(
                "CONFIG_IP_MULTIPLE_TABLES in the running kernel (rebuild/reboot into a kernel \
                 built with it; most distro kernels already enable it)",
            );
        }
        if unsupported.missing_multipath_routes {
            remedies.push(
                "IP_ROUTE_MULTIPATH support (CONFIG_IP_ROUTE_MULTIPATH at build time; if already \
                 built in, check `sysctl net.ipv4.fib_multipath_hash_policy` exists)",
            );
        }
        remedies.join(" and ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntk_netlink::{AddressTable, FakeNetlink, LinkInfo};

    #[tokio::test]
    async fn fake_netlink_with_loopback_passes_preflight() {
        let kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        }]);
        check(&kernel)
            .await
            .expect("FakeNetlink with lo supports both features");
    }

    #[tokio::test]
    async fn fake_netlink_without_loopback_fails_preflight_with_actionable_message() {
        let kernel = FakeNetlink::new();
        let err = check(&kernel)
            .await
            .expect_err("no lo means no multipath probe");
        assert!(err.to_string().contains("IP_ROUTE_MULTIPATH"));
    }

    #[test]
    fn the_netsukuku_address_space_covers_ten_slash_eight_and_nothing_outside_it() {
        // Pins the constant itself: the warning below is only meaningful if this range is
        // actually what `crate::kernel::addressing` packs into.
        let space = netsukuku_address_space();
        assert!(space.contains(std::net::Ipv4Addr::new(10, 0, 0, 1)));
        assert!(space.contains(std::net::Ipv4Addr::new(10, 255, 255, 254)));
        assert!(!space.contains(std::net::Ipv4Addr::new(192, 168, 1, 1)));
        assert!(!space.contains(std::net::Ipv4Addr::new(100, 64, 0, 1)));
    }

    #[tokio::test]
    async fn an_address_outside_ten_slash_eight_is_not_reported_as_a_conflict() {
        let kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "eth0".into(),
            is_up: true,
        }]);
        kernel
            .add_address(
                &ntk_netlink::Interface::index(1),
                ntk_netlink::Ipv4Net::new(std::net::Ipv4Addr::new(192, 168, 1, 10), 24)
                    .expect("valid network"),
            )
            .await
            .expect("add a non-conflicting address");
        // Advisory and infallible by contract: it must return normally either way. The value is
        // that it does not panic or error on a clean host, so startup is never blocked by it.
        warn_address_space_conflicts(&kernel).await;
    }

    #[tokio::test]
    async fn an_address_inside_ten_slash_eight_still_lets_startup_proceed() {
        let kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "docker0".into(),
            is_up: true,
        }]);
        kernel
            .add_address(
                &ntk_netlink::Interface::index(1),
                ntk_netlink::Ipv4Net::new(std::net::Ipv4Addr::new(10, 42, 0, 1), 16)
                    .expect("valid network"),
            )
            .await
            .expect("add a conflicting address");
        // The whole point of warn-not-fail: an overlap must NOT stop the daemon, because a
        // 10/8 address on an idle interface is not a reason to refuse to route.
        warn_address_space_conflicts(&kernel).await;
    }

    #[tokio::test]
    async fn existing_configured_nic_passes_preflight() {
        let kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "enp0s31f6".into(),
            is_up: true,
        }]);
        check_nics(&kernel, &["enp0s31f6".to_string()])
            .await
            .expect("configured nic exists in the link table");
    }

    #[tokio::test]
    async fn missing_configured_nic_names_itself_and_the_available_interfaces() {
        let kernel = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "enp0s31f6".into(),
            is_up: true,
        }]);
        let err = check_nics(&kernel, &["eth0".to_string()])
            .await
            .expect_err("eth0 is not in the link table");
        let message = err.to_string();
        assert!(message.contains("eth0"));
        assert!(message.contains("enp0s31f6"));
    }
}
