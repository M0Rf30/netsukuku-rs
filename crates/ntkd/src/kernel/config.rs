//! On-disk daemon configuration.
//!
//! Upstream's `ntkd` has no config file at all — its topology and NIC list are hard-wired
//! directly into `configuration()` (`research/impl/vala/ntkd/configuration.vala:31-67`, the
//! literal `foreach (int _g_exp in new int[]{2,1,1,1})` and `foreach (string dev in interfaces)`)
//! and its table/rule numbering lives in the static `research/impl/vala/system-ntkd/ntk.conf`.
//! This module is the minimal, single-source TOML equivalent of both: one small struct (per-level
//! g-node sizes plus the NIC list and TCP port), not a layered multi-source config system —
//! matching the scope `research/notes/06-rust-stack.md` calls for.

use std::path::Path;

use ntk_common::Topology;

/// The daemon's on-disk configuration: hierarchy shape, monitored interfaces, and RPC port.
///
/// Mirrors upstream's `configuration()` output (`naddr`'s topology, `devs`) plus the one port
/// number every `ntk-rpc` transport (`TcpServer`, `UdpBroadcaster`) needs to bind
/// (upstream has none — it is a Vala convention baked into `system_ntkd.vala`'s test harness,
/// not a real deployment concern; here it is an explicit, required config field instead).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct NtkdConfig {
    /// Per-level g-node sizes, index 0 innermost — see [`ntk_common::Topology::new`].
    gsizes: Vec<u32>,
    /// Names of the network interfaces to monitor for neighbours
    /// (`configuration.vala:65-67`'s `devs`).
    nics: Vec<String>,
    /// TCP/UDP port every RPC transport binds to.
    port: u16,
    /// Path to this node's persisted ANDNA ed25519 signing-key seed
    /// (`crate::node::andna_key::load_or_generate`). `None` (the default, and the value when
    /// this key is absent from the config file) refuses ANDNA registration outright rather than
    /// silently disabling it or guessing a global path — see `crate::node::status::serve`.
    #[serde(default)]
    andna_key_path: Option<std::path::PathBuf>,
    /// Path to this node's persisted RPC-identity ed25519 signing-key seed, used to sign
    /// outbound calls (`ntk_proto::auth`). Deliberately NOT the same key as
    /// [`NtkdConfig::andna_key_path`]: that one proves hostname ownership, so rotating a
    /// compromised transport key would otherwise forfeit every hostname this node registered.
    /// `None` (the default) leaves outbound traffic unsigned — the vanilla-reference behaviour.
    #[serde(default)]
    node_key_path: Option<std::path::PathBuf>,
    /// Reject inbound RPCs that carry no valid `ntk_proto::auth` block.
    ///
    /// Defaults to `false`, which is the only setting interoperable with an unmodified upstream
    /// Vala node: the `Auth` field is an optional protobuf field, so adding it is wire-compatible
    /// and only *enforcing* it is not. Enabling this requires [`NtkdConfig::node_key_path`],
    /// since a node that demands authentication must be able to authenticate itself.
    #[serde(default)]
    require_auth: bool,
}

impl NtkdConfig {
    /// Reads and parses the config file at `path`.
    ///
    /// Every error names `path`. A parse failure otherwise reports only the offending field —
    /// `missing field \`nics\`` — with no indication of *which* file to edit, and the packaged
    /// config deliberately ships `nics` commented out (no interface can be a safe default: one
    /// that happens to exist would silently mesh over the operator's uplink), so that message is
    /// the first thing a new operator sees.
    ///
    /// # Errors
    /// [`ConfigError::Io`] if `path` cannot be read; [`ConfigError::Parse`] wrapping any
    /// parse/validation failure from [`NtkdConfig::from_str`].
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.display().to_string(),
            source: Box::new(source),
        })
    }

    /// Parses `text` as TOML and validates its topology.
    ///
    /// Named to mirror the batch contract's fixed signature, not [`std::str::FromStr`] — no
    /// trait impl exists for this type since a bare `parse()` would hide which error type is
    /// returned.
    ///
    /// # Errors
    /// [`ConfigError::Toml`] on malformed TOML; [`ConfigError::Topology`] if `gsizes` does not
    /// describe a valid [`Topology`] (e.g. empty, or a zero-sized level).
    #[allow(
        clippy::should_implement_trait,
        reason = "contracted method name/signature, not a FromStr impl"
    )]
    pub fn from_str(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text)?;
        config.topology()?;
        if config.require_auth && config.node_key_path.is_none() {
            return Err(ConfigError::AuthWithoutKey);
        }
        Ok(config)
    }

    /// Validates and builds the [`Topology`] described by `NtkdConfig::gsizes`.
    ///
    /// # Errors
    /// [`ConfigError::Topology`] — see [`Topology::new`] for the exact validation rules.
    pub fn topology(&self) -> Result<Topology, ConfigError> {
        Topology::new(self.gsizes.iter().copied()).map_err(ConfigError::Topology)
    }

    /// Names of the network interfaces to monitor for neighbours.
    pub fn nics(&self) -> &[String] {
        &self.nics
    }

    /// The TCP/UDP port every RPC transport binds to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Path to this node's persisted ANDNA signing-key seed, if configured.
    pub fn andna_key_path(&self) -> Option<&Path> {
        self.andna_key_path.as_deref()
    }

    /// Path to this node's persisted RPC-identity signing-key seed, if configured.
    pub fn node_key_path(&self) -> Option<&Path> {
        self.node_key_path.as_deref()
    }

    /// Whether inbound RPCs lacking a valid authentication block must be rejected.
    pub fn require_auth(&self) -> bool {
        self.require_auth
    }
}

/// Everything that can go wrong loading an [`NtkdConfig`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read. Carries the path, since the daemon is normally
    /// started by a unit file the operator did not type the path into.
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    /// The config at `path` parsed as TOML but was rejected. Wraps the underlying reason so a
    /// bare `missing field \`nics\`` still says which file to edit.
    #[error("failed to load config {path}: {source}")]
    Parse {
        path: String,
        source: Box<ConfigError>,
    },
    /// The config text is not valid TOML, or is missing/mistypes a required field.
    #[error("failed to parse config: {0}")]
    Toml(#[from] toml::de::Error),
    /// `gsizes` does not describe a valid topology.
    #[error("invalid topology: {0}")]
    Topology(#[from] ntk_common::Error),
    /// `require_auth` was set without a `node_key_path`. Refused rather than defaulted: a node
    /// that rejects unauthenticated peers but cannot sign its own calls would be unreachable in
    /// both directions, which is a silent, hard-to-diagnose outage.
    #[error("require_auth is set but no node_key_path is configured")]
    AuthWithoutKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
        gsizes = [4, 2, 2, 2]
        nics = ["eth0", "eth1"]
        port = 269
    "#;

    #[test]
    fn round_trips_valid_toml() {
        let config = NtkdConfig::from_str(VALID).expect("valid config parses");
        assert_eq!(config.nics(), ["eth0", "eth1"]);
        assert_eq!(config.port(), 269);
        assert_eq!(config.topology().expect("valid topology").levels(), 4);
    }

    #[test]
    fn andna_key_path_defaults_to_none_when_absent() {
        let config = NtkdConfig::from_str(VALID).expect("valid config parses");
        assert_eq!(config.andna_key_path(), None);
    }

    #[test]
    fn andna_key_path_parses_when_present() {
        let text = r#"
            gsizes = [4, 2, 2, 2]
            nics = ["eth0", "eth1"]
            port = 269
            andna_key_path = "/etc/ntkd/andna.key"
        "#;
        let config = NtkdConfig::from_str(text).expect("valid config parses");
        assert_eq!(
            config.andna_key_path(),
            Some(Path::new("/etc/ntkd/andna.key"))
        );
    }

    /// The vanilla-reference default: authentication is opt-in, so a config that never mentions
    /// it must leave the node unsigned and non-enforcing.
    #[test]
    fn auth_is_off_and_keyless_by_default() {
        let config = NtkdConfig::from_str(VALID).expect("valid config parses");
        assert_eq!(config.node_key_path(), None);
        assert!(!config.require_auth());
    }

    #[test]
    fn require_auth_without_a_node_key_is_refused() {
        let text = r#"
            gsizes = [4, 2, 2, 2]
            nics = ["eth0", "eth1"]
            port = 269
            require_auth = true
        "#;
        assert!(matches!(
            NtkdConfig::from_str(text),
            Err(ConfigError::AuthWithoutKey)
        ));
    }

    #[test]
    fn require_auth_with_a_node_key_parses() {
        let text = r#"
            gsizes = [4, 2, 2, 2]
            nics = ["eth0", "eth1"]
            port = 269
            require_auth = true
            node_key_path = "/etc/ntkd/node.key"
        "#;
        let config = NtkdConfig::from_str(text).expect("valid config parses");
        assert!(config.require_auth());
        assert_eq!(
            config.node_key_path(),
            Some(Path::new("/etc/ntkd/node.key"))
        );
    }

    /// The RPC identity and the ANDNA owner key are deliberately distinct: rotating a
    /// compromised transport key must not forfeit the hostnames this node registered.
    #[test]
    fn node_key_and_andna_key_are_independent() {
        let text = r#"
            gsizes = [4, 2, 2, 2]
            nics = ["eth0", "eth1"]
            port = 269
            node_key_path = "/etc/ntkd/node.key"
            andna_key_path = "/etc/ntkd/andna.key"
        "#;
        let config = NtkdConfig::from_str(text).expect("valid config parses");
        assert_ne!(config.node_key_path(), config.andna_key_path());
    }

    #[test]
    fn rejects_zero_gsize_as_config_error_not_panic() {
        let text = r#"
            gsizes = [4, 0, 2]
            nics = ["eth0"]
            port = 269
        "#;
        let err = NtkdConfig::from_str(text).expect_err("zero gsize is invalid");
        assert!(matches!(
            err,
            ConfigError::Topology(ntk_common::Error::ZeroGsize { level: 1 })
        ));
    }

    #[test]
    fn rejects_malformed_toml() {
        let err = NtkdConfig::from_str("not valid toml {{{").expect_err("malformed toml");
        assert!(matches!(err, ConfigError::Toml(_)));
    }
}
