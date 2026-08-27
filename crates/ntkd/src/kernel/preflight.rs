//! Startup kernel-capability preflight. Upstream's `ntkd/startup.vala` never checks for
//! `CONFIG_IP_MULTIPLE_TABLES`/`CONFIG_IP_ROUTE_MULTIPATH` before wiring routes — it just fails
//! deep inside `identity_ip_commands.vala`'s shelled-out `ip` calls with an opaque nonzero exit
//! code (`research/notes/02-vala-services-daemon.md` §5). This module runs the same probes
//! `ntk_netlink::detect_capabilities` performs, but turns a missing feature into one actionable
//! error message up front, before any identity or protocol actor is spawned.

use ntk_netlink::{KernelCapabilities, Netlink, UnsupportedKernel, detect_capabilities};

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
    use ntk_netlink::{FakeNetlink, LinkInfo};

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
}
