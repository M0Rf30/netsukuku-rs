// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Introspection and ANDNA control: a unix-socket server the running daemon exposes, and the
//! `ntkd status`/`andna-register`/`andna-resolve` clients that query it. Wire format is `toml`
//! (already a workspace dependency, matching `crate::node::codec`'s own choice for other
//! ntkd-internal payloads): the client sends one request line — `"status\n"`,
//! `"andna-register <hostname>\n"`, or `"andna-resolve <hostname>\n"` — the server replies with
//! a TOML document and closes its write half, and the client reads to EOF. An unrecognized
//! request line, or any failure handling a recognized one, replies with [`ErrorReply`] instead
//! of panicking or closing silently.

use std::net::Ipv4Addr;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;

use ntk_andna::{Hostname, RegisterOutcome, RegisterRequest, SnsdTarget};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

use crate::kernel::addressing;
use crate::node::adapters::NetworkInfo;
use crate::node::andna_key;
use crate::node::kernel_handle::SendNetlink;
use crate::node::lifecycle::RunningNode;

#[derive(Serialize, Deserialize, Debug)]
pub struct ArcStatus {
    pub mac: String,
    pub dev: String,
    pub state: String,
    pub cost: Option<String>,
    /// Whether this arc has been admitted to qspn as a routable [`ntk_qspn::ArcId`] yet
    /// (`crate::node::registry::LinkEntry::qspn_arc`) — `false` covers everything from "just
    /// discovered, cost not settled" through "hooking still evaluating the merge".
    pub qspn_linked: bool,
    /// This arc's hooking protocol phase (`ntk_hooking::snapshot::ArcPhase`), if hooking has
    /// registered it yet.
    pub hooking_phase: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusReport {
    pub main_identity: u64,
    pub identity_count: usize,
    pub arcs: Vec<ArcStatus>,
    pub route_destinations: usize,
    pub bootstrapped: bool,
    /// Whether this identity has completed hooking (`ntk_hooking::HookingSnapshot::hooked`) —
    /// distinct from `bootstrapped` (qspn's own "at least one route computed" latch): an
    /// identity is qspn-bootstrapped as a trivial network-of-one before it ever hooks into a
    /// bigger network.
    pub hooked: bool,
    /// The kernel routing table this identity's routes live in.
    pub route_table: u32,
    /// Links the registry has ever minted a [`crate::node::registry::LinkId`] for, including
    /// any not currently present in `arcs` (e.g. removed at the neighborhood layer a beat
    /// before the registry entry is purged).
    pub known_links: usize,
    /// Total g-nodes known (across every peerservices-registered service) to participate in
    /// that service.
    pub peer_participants: usize,
    /// In-flight coordinator reservations summed across every level this identity holds
    /// fixed-keys memory for.
    pub coordinator_reservations: usize,
    /// Hostnames this identity holds under andna's `Andna` hash-node/replica role.
    pub andna_hosted_hostnames: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AndnaRegisterReply {
    pub hostname: String,
    pub outcome: String,
    pub expires_unix: Option<u64>,
}

/// One resolved SNSD record: what ANDNA stored, plus — for an address target — the
/// `10.0.0.0/8` IPv4 that record actually routes as, which is the only form a caller can
/// connect to or hand to `ping`.
#[derive(Serialize, Deserialize, Debug)]
pub struct ResolvedTarget {
    /// The stored target verbatim: an [`SnsdTarget::Address`]'s hierarchical `Naddr` notation,
    /// or an [`SnsdTarget::Alias`]'s hostname.
    pub target: String,
    /// The `/32` host address [`addressing::host_address`] computes for an
    /// [`SnsdTarget::Address`] target.
    ///
    /// `None` in two distinct cases, deliberately not distinguished here because a client's
    /// action is the same in both (fall back to `target`): an [`SnsdTarget::Alias`], which names
    /// a hostname rather than a position and so has no address of its own; or an address whose
    /// topology does not fit the 24 bits under the fixed `10` octet. The latter cannot arise for
    /// a peer sharing this node's `gsizes` — but the `Naddr` here was decoded from the wire, so
    /// it is not this node's invariant to assume.
    pub ipv4: Option<Ipv4Addr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AndnaResolveReply {
    pub hostname: String,
    pub addresses: Vec<ResolvedTarget>,
    /// Always `None`: neither [`ntk_andna::Handle::resolve`] nor its wire reply carries a
    /// per-record TTL back to the caller (only the hash-node's own hosted record knows
    /// `expires_at`) — present so a client can tell "known absent" from "not yet supported" if
    /// that ever changes.
    pub ttl_secs: Option<u64>,
}

/// A request the server could not satisfy: an unrecognized request line, an invalid hostname,
/// an unconfigured or invalid ANDNA key, or a failed ANDNA call. A client distinguishes success
/// from failure by which struct the TOML reply parses as.
#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorReply {
    pub error: String,
}

/// Builds a fresh [`StatusReport`] from the running node's own live state — no caching, always
/// current as of the moment of the query.
pub fn report<K>(node: &RunningNode<K>, bootstrapped: bool) -> StatusReport {
    let identity_snapshot = node.identities.snapshot();
    let hooking_snapshot = node.hooking.snapshot();
    let known_links = node.registry.all().len();
    let arcs = node
        .neighborhood
        .snapshot()
        .borrow()
        .iter()
        .map(|arc| {
            let entry = node
                .registry
                .link_for_dev_and_mac(&arc.neighbour_mac)
                .and_then(|link| node.registry.entry(link));
            // The registry's own copy is canonical once minted (recorded synchronously on
            // first discovery, `crate::node::lifecycle::on_neighborhood_event`); the
            // neighborhood-reported value is only a fallback for the narrow race before that.
            let (mac, dev) = entry.as_ref().map_or_else(
                || (arc.neighbour_mac.clone(), arc.my_dev.clone()),
                |e| (e.mac.clone(), e.dev.clone()),
            );
            ArcStatus {
                mac,
                dev,
                state: format!("{:?}", arc.state),
                cost: arc.cost.map(|c| format!("{c}")),
                qspn_linked: entry.as_ref().is_some_and(|e| e.qspn_arc.is_some()),
                hooking_phase: entry
                    .and_then(|e| hooking_snapshot.arcs.get(&e.id.hooking()).cloned())
                    .map(|phase| format!("{phase:?}")),
            }
        })
        .collect();
    let generation = node.generation.borrow();
    let route_destinations = generation.qspn.snapshot().levels.iter().map(Vec::len).sum();
    let peer_participants = generation
        .peers
        .snapshot()
        .borrow()
        .participants
        .values()
        .map(|m| m.participants().count())
        .sum();
    let coordinator_reservations = generation
        .coordinator
        .snapshot()
        .borrow()
        .values()
        .map(|memory| memory.reserve_list.len())
        .sum();
    let andna_hosted_hostnames = generation.andna.snapshot().borrow().hosted.len();
    StatusReport {
        main_identity: identity_snapshot.main_id.into_raw(),
        identity_count: identity_snapshot.identities.len(),
        arcs,
        route_destinations,
        bootstrapped,
        hooked: hooking_snapshot.hooked,
        route_table: node.route_table,
        known_links,
        peer_participants,
        coordinator_reservations,
        andna_hosted_hostnames,
    }
}

/// File name of the control socket inside the runtime directory.
const SOCKET_FILE_NAME: &str = "ntkd.sock";

/// Runtime directory used when systemd's `RuntimeDirectory=` is not in effect.
const FALLBACK_RUNTIME_DIR: &str = "/run/ntkd";

/// Longest request line the server reads: the longest legitimate line is
/// `andna-register <hostname>`, and a hostname is at most a few hundred bytes.
const MAX_REQUEST_LINE_BYTES: u64 = 4096;

/// How much of an oversized request the server discards after refusing it.
const OVERSIZED_DRAIN_BYTES: u64 = 256 * 1024;

/// How long a client gets to deliver its request line before the server gives up on it.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The control socket path used by `run`/`status`/`andna-*` when `--status-socket`/`--socket`
/// is not given: `$RUNTIME_DIRECTORY/ntkd.sock` (the first entry, when systemd passes several
/// colon-separated directories), else `/run/ntkd/ntkd.sock`. Never a world-writable directory
/// such as `/tmp`, where another local user could pre-create or squat the path.
#[must_use]
pub fn default_socket_path() -> PathBuf {
    socket_path_in(std::env::var_os("RUNTIME_DIRECTORY").as_deref())
}

fn socket_path_in(runtime_directory: Option<&std::ffi::OsStr>) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    let dir = runtime_directory
        .and_then(|value| value.as_bytes().split(|&b| b == b':').next())
        .filter(|first| !first.is_empty())
        .map_or_else(
            || PathBuf::from(FALLBACK_RUNTIME_DIR),
            |first| PathBuf::from(std::ffi::OsStr::from_bytes(first)),
        );
    dir.join(SOCKET_FILE_NAME)
}

/// Makes `socket_path` free to bind: creates its parent directory (mode 0700) when missing,
/// removes a stale socket left behind by a crashed daemon, and refuses to touch anything that is
/// not a dead socket.
///
/// # Errors
/// A live daemon is already listening on `socket_path`, `socket_path` exists but is not a
/// socket, or the filesystem refuses the directory creation/removal.
fn prepare_socket_path(socket_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = socket_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
            .with_context(|| {
                format!("creating the status socket directory {}", parent.display())
            })?;
    }
    let metadata = match std::fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("inspecting {}", socket_path.display()));
        }
    };
    anyhow::ensure!(
        metadata.file_type().is_socket(),
        "{} exists and is not a socket; refusing to remove it",
        socket_path.display()
    );
    if std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
        anyhow::bail!(
            "another ntkd is already listening on {}; refusing to steal its socket",
            socket_path.display()
        );
    }
    std::fs::remove_file(socket_path)
        .with_context(|| format!("removing the stale socket {}", socket_path.display()))
}

/// Binds `socket_path` with mode 0600 and no window in which it is reachable with looser
/// permissions or replaceable by a racing daemon: binds under a private temporary name,
/// restricts the mode, then hard-links it into place (which fails if `socket_path` appeared in
/// the meantime).
fn bind_private(socket_path: &Path) -> anyhow::Result<UnixListener> {
    let mut staging = socket_path.as_os_str().to_owned();
    staging.push(format!(".{}.tmp", std::process::id()));
    let staging = PathBuf::from(staging);
    let _ = std::fs::remove_file(&staging);
    let listener = UnixListener::bind(&staging)
        .with_context(|| format!("binding the status socket {}", staging.display()))?;
    let published = std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restricting {} to mode 0600", staging.display()))
        .and_then(|()| {
            std::fs::hard_link(&staging, socket_path).with_context(|| {
                format!(
                    "publishing the status socket at {} (is another ntkd running?)",
                    socket_path.display()
                )
            })
        });
    let _ = std::fs::remove_file(&staging);
    published?;
    Ok(listener)
}

/// Serves [`StatusReport`]s and ANDNA register/resolve requests over a unix socket at
/// `socket_path` until `cancel` fires. `andna_key_path` is passed straight through to every
/// connection's `andna-register` handling, unchanged — see
/// [`crate::node::andna_key::load_or_generate`].
///
/// The socket is created with mode 0600. A stale socket from a crashed daemon is replaced, but a
/// socket another daemon is still listening on, or any non-socket file, is never removed.
///
/// # Errors
/// The socket path cannot be prepared or bound; the message names the path.
pub async fn serve<K>(
    socket_path: std::path::PathBuf,
    node: Arc<RunningNode<K>>,
    net: Arc<NetworkInfo>,
    andna_key_path: Option<std::path::PathBuf>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    K: SendNetlink + 'static,
{
    prepare_socket_path(&socket_path)?;
    let listener = bind_private(&socket_path)?;
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                let _ = std::fs::remove_file(&socket_path);
                return Ok(());
            }
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let node = node.clone();
                let net = net.clone();
                let andna_key_path = andna_key_path.clone();
                tokio::spawn(async move {
                    if let Err(err) =
                        handle_connection(stream, &node, &net, andna_key_path.as_deref()).await
                    {
                        tracing::debug!(error = %format!("{err:#}"), "status: connection failed");
                    }
                });
            }
        }
    }
}

async fn handle_connection<K>(
    stream: UnixStream,
    node: &RunningNode<K>,
    net: &NetworkInfo,
    andna_key_path: Option<&Path>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    // Bounded in both size and time: an unauthenticated peer on the socket must not be able to
    // make the daemon buffer an unbounded line or hold the connection open forever.
    let read = tokio::time::timeout(
        REQUEST_READ_TIMEOUT,
        (&mut reader)
            .take(MAX_REQUEST_LINE_BYTES)
            .read_line(&mut line),
    )
    .await;
    let Ok(read) = read else {
        anyhow::bail!("timed out waiting for a request line");
    };
    let read = u64::try_from(read?).unwrap_or(u64::MAX);
    if read >= MAX_REQUEST_LINE_BYTES && !line.ends_with('\n') {
        let text = toml::to_string(&ErrorReply {
            error: format!("request line exceeds {MAX_REQUEST_LINE_BYTES} bytes"),
        })?;
        write_half.write_all(text.as_bytes()).await?;
        write_half.shutdown().await?;
        // Discard what the client already sent (bounded): closing with unread input would reset
        // the connection and could destroy the reply before the client reads it.
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::io::copy(
                &mut (&mut reader).take(OVERSIZED_DRAIN_BYTES),
                &mut tokio::io::sink(),
            ),
        )
        .await;
        return Ok(());
    }
    let request = line.trim();
    let (command, arg) = request
        .split_once(' ')
        .map_or((request, ""), |(c, a)| (c, a.trim()));
    let text = match (command, arg) {
        ("status", "") => toml::to_string(&report(node, net.is_bootstrapped())),
        ("andna-register", hostname) if !hostname.is_empty() => {
            match andna_register(node, andna_key_path, hostname).await {
                Ok(reply) => toml::to_string(&reply),
                Err(error) => toml::to_string(&ErrorReply { error }),
            }
        }
        ("andna-resolve", hostname) if !hostname.is_empty() => {
            match andna_resolve(node, hostname).await {
                Ok(reply) => toml::to_string(&reply),
                Err(error) => toml::to_string(&ErrorReply { error }),
            }
        }
        _ => toml::to_string(&ErrorReply {
            error: format!("unrecognized request line: {request:?}"),
        }),
    };
    if let Ok(text) = text {
        write_half.write_all(text.as_bytes()).await?;
    }
    write_half.shutdown().await?;
    Ok(())
}

/// Signs and submits a registration for `hostname`, using the daemon's persisted ANDNA key
/// (loaded or generated at `andna_key_path`) and this identity's current address as the owner
/// — the `andna-register` request line's handler.
async fn andna_register<K>(
    node: &RunningNode<K>,
    andna_key_path: Option<&Path>,
    hostname: &str,
) -> Result<AndnaRegisterReply, String> {
    let hostname = Hostname::new(hostname).map_err(|err| err.to_string())?;
    let key_path = andna_key_path.ok_or_else(|| {
        "andna_key_path is not configured; ANDNA registration is refused".to_string()
    })?;
    let signing_key = andna_key::load_or_generate(key_path).map_err(|err| err.to_string())?;
    let (andna, owner_naddr) = {
        let generation = node.generation.borrow();
        (generation.andna.clone(), generation.qspn.my_naddr().clone())
    };
    let now = unix_now();
    let req = RegisterRequest::sign(
        &signing_key,
        hostname.clone(),
        owner_naddr,
        now,
        now,
        ntk_andna::ZERO_DEFAULT_PRIORITY,
        ntk_andna::ZERO_DEFAULT_WEIGHT,
        Vec::new(),
    )
    .map_err(|err| err.to_string())?;
    let outcome = andna.register(req).await.map_err(|err| err.to_string())?;
    let (outcome_name, expires_at) = match outcome {
        RegisterOutcome::Registered { expires_at } => ("registered", expires_at),
        RegisterOutcome::Renewed { expires_at } => ("renewed", expires_at),
    };
    Ok(AndnaRegisterReply {
        hostname: hostname.to_string(),
        outcome: outcome_name.to_string(),
        expires_unix: Some(expires_at),
    })
}

/// Resolves `hostname`'s zero (service-0) SNSD record via ANDNA — the `andna-resolve` request
/// line's handler.
async fn andna_resolve<K>(
    node: &RunningNode<K>,
    hostname: &str,
) -> Result<AndnaResolveReply, String> {
    let hostname = Hostname::new(hostname).map_err(|err| err.to_string())?;
    let andna = node.generation.borrow().andna.clone();
    let records = andna
        .resolve(&hostname, ntk_andna::ZERO_SERVICE)
        .await
        .map_err(|err| err.to_string())?;
    let addresses = records
        .into_iter()
        .map(|record| match record.target {
            SnsdTarget::Address(naddr) => ResolvedTarget {
                target: naddr.to_string(),
                ipv4: addressing::host_address(&naddr)
                    .ok()
                    .map(|net| net.address()),
            },
            SnsdTarget::Alias(alias) => ResolvedTarget {
                target: alias.to_string(),
                ipv4: None,
            },
        })
        .collect();
    Ok(AndnaResolveReply {
        hostname: hostname.to_string(),
        addresses,
        ttl_secs: None,
    })
}

/// Current unix time in seconds, used as [`RegisterRequest::sequence`] for CLI-driven
/// registrations: the daemon keeps no per-hostname sequence state of its own (that bookkeeping
/// lives at the ANDNA hash-node, not the registrant), and wall-clock seconds strictly increase
/// across any two calls this line-at-a-time control socket can actually issue back to back.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The `ntkd status` client: connects to `socket_path`, requests a report, and returns it
/// parsed.
///
/// # Errors
/// Any I/O failure connecting to or reading from `socket_path`, or a malformed reply.
pub async fn query(socket_path: &std::path::Path) -> anyhow::Result<StatusReport> {
    parse_reply(&request_line(socket_path, "status\n").await?)
}

/// The `ntkd andna-register` client: connects to `socket_path`, requests registration of
/// `hostname`, and returns the parsed reply.
///
/// # Errors
/// Any I/O failure connecting to or reading from `socket_path`; a malformed reply; or the
/// daemon's own [`ErrorReply`] text (e.g. an invalid hostname or an unconfigured ANDNA key),
/// surfaced as `Err` rather than left for the caller to detect by reply shape.
pub async fn register_hostname(
    socket_path: &std::path::Path,
    hostname: &str,
) -> anyhow::Result<AndnaRegisterReply> {
    parse_reply(&request_line(socket_path, &format!("andna-register {hostname}\n")).await?)
}

/// The `ntkd andna-resolve` client: connects to `socket_path`, requests resolution of
/// `hostname`, and returns the parsed reply.
///
/// # Errors
/// Same as [`register_hostname`].
pub async fn resolve_hostname(
    socket_path: &std::path::Path,
    hostname: &str,
) -> anyhow::Result<AndnaResolveReply> {
    parse_reply(&request_line(socket_path, &format!("andna-resolve {hostname}\n")).await?)
}

/// Sends `line` to `socket_path` and returns the reply text read to EOF — the shared transport
/// for every client function in this module.
async fn request_line(socket_path: &std::path::Path, line: &str) -> anyhow::Result<String> {
    let stream = UnixStream::connect(socket_path).await.with_context(|| {
        format!(
            "connecting to the ntkd status socket {} (is the daemon running?)",
            socket_path.display()
        )
    })?;
    let (read_half, mut write_half) = stream.into_split();
    write_half.write_all(line.as_bytes()).await?;
    write_half.shutdown().await?;
    let mut text = String::new();
    BufReader::new(read_half).read_to_string(&mut text).await?;
    Ok(text)
}

/// Parses `text` as `T`; on failure, tries it as [`ErrorReply`] and surfaces its message instead
/// — the same "which struct parses" success/failure distinction the wire contract describes.
fn parse_reply<T: serde::de::DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    match toml::from_str::<T>(text) {
        Ok(reply) => Ok(reply),
        Err(parse_err) => match toml::from_str::<ErrorReply>(text) {
            Ok(err) => anyhow::bail!(err.error),
            Err(_) => Err(parse_err.into()),
        },
    }
}

#[cfg(test)]
mod andna_socket_tests {
    use std::time::Duration;

    use futures::future::BoxFuture;
    use ntk_neighborhood::{
        Arc as NeighborArc, FakeIpRouteManager, FixedRttProbe, NeighborhoodConfig,
        NeighborhoodStubFactory, NeighborhoodTiming, NodeId,
    };
    use ntk_netlink::{FakeNetlink, LinkInfo};
    use ntk_rpc::RpcClient;
    use tokio::task::JoinSet;

    use super::*;
    use crate::kernel::config::NtkdConfig;
    use crate::node::lifecycle::{self, Dialer, NodeInputs};
    use crate::node::peers::PeerLinks;
    use crate::node::registry::LinkRegistry;

    /// Never actually invoked: the single test node below monitors no NICs, so it never forms
    /// an arc and never has anything to broadcast/unicast over.
    #[derive(Debug)]
    struct UnreachableStubFactory;

    impl NeighborhoodStubFactory for UnreachableStubFactory {
        fn broadcast(&self, _dev: &str) -> Arc<dyn RpcClient> {
            unreachable!("test node has no nics to broadcast on")
        }

        fn unicast(&self, _arc: &NeighborArc) -> Arc<dyn RpcClient> {
            unreachable!("test node has no arcs to unicast to")
        }
    }

    /// Never actually invoked: a single-node, single-slot topology (`gsizes = [1]`) always
    /// routes ANDNA's peerservices calls back to this same node, which is registered as the
    /// `Andna`/`Counter` service locally — no outbound dial is ever needed.
    #[derive(Debug)]
    struct UnreachableDialer;

    impl Dialer for UnreachableDialer {
        fn dial(&self, _addr: &str, _port: u16) -> BoxFuture<'_, Option<Arc<dyn RpcClient>>> {
            Box::pin(async { None })
        }
    }

    /// Boots one real, isolated node — network-of-one, no NICs, `gsizes = [1]` so every
    /// peerservices hash target resolves to this node's own single possible position — over
    /// `FakeNetlink`, exactly the same [`lifecycle::run`] production path `crate::node::
    /// transport::start` drives, just without any real kernel/socket I/O.
    async fn single_node() -> (Arc<RunningNode<FakeNetlink>>, Arc<NetworkInfo>, JoinSet<()>) {
        let links = vec![LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        }];
        let neighborhood_kernel = FakeNetlink::with_links(links.clone());
        let routing_kernel = Arc::new(FakeNetlink::with_links(links));
        let my_id = NodeId::from_raw(1).unwrap();
        let neighborhood_config = NeighborhoodConfig {
            my_id,
            max_arcs: 8,
            kernel: neighborhood_kernel,
            stub_factory: Arc::new(UnreachableStubFactory),
            ip_route_manager: Arc::new(FakeIpRouteManager::new()),
            rtt_probe: Arc::new(FixedRttProbe(Some(10))),
            timing: NeighborhoodTiming {
                radar_interval: Duration::from_millis(50),
                arc_monitor_interval: (Duration::from_millis(20), Duration::from_millis(40)),
            },
            new_linklocal_address: Box::new(|| "169.254.1.1".to_owned()),
            signing_key: None,
            require_auth: false,
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut tasks = JoinSet::new();
        let (neighborhood, neighborhood_join) =
            ntk_neighborhood::Manager::spawn(neighborhood_config, cancel.child_token());
        tasks.spawn(async move {
            let _ = neighborhood_join.await;
        });

        let config = NtkdConfig::from_str("gsizes = [1]\nnics = []\nport = 269\n")
            .expect("valid test config");
        let started = lifecycle::run(
            NodeInputs {
                config,
                neighborhood,
                registry: Arc::new(LinkRegistry::new()),
                links: Arc::new(PeerLinks::new()),
                routing_kernel,
                dialer: Arc::new(UnreachableDialer),
                initial_position: None,
                preformed: None,
                my_id,
            },
            &mut tasks,
            cancel.child_token(),
        )
        .await
        .expect("single-node lifecycle::run");
        let net = started.running.net.clone();
        (Arc::new(started.running), net, tasks)
    }

    fn key_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "ntkd-status-test-andna-key-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn an_unrecognized_request_line_yields_an_error_reply_not_a_panic() {
        let (node, net, _tasks) = single_node().await;
        let socket_path = std::env::temp_dir().join(format!(
            "ntkd-status-test-unknown-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, &node, &net, None).await.unwrap();
        });
        let client = UnixStream::connect(&socket_path).await.unwrap();
        let (client_read, mut client_write) = client.into_split();
        client_write
            .write_all(b"not-a-real-request\n")
            .await
            .unwrap();
        client_write.shutdown().await.unwrap();
        let mut text = String::new();
        BufReader::new(client_read)
            .read_to_string(&mut text)
            .await
            .unwrap();
        server.await.unwrap();
        let reply: ErrorReply = toml::from_str(&text).expect("unknown request yields ErrorReply");
        assert!(reply.error.contains("unrecognized"));
        let _ = std::fs::remove_file(&socket_path);
    }

    #[tokio::test]
    async fn register_then_resolve_round_trips_over_the_control_socket() {
        let (node, net, _tasks) = single_node().await;
        let socket_path = std::env::temp_dir().join(format!(
            "ntkd-status-test-andna-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&socket_path);
        let key = key_path();
        let cancel = tokio_util::sync::CancellationToken::new();
        let server_cancel = cancel.child_token();
        let server = tokio::spawn(serve(
            socket_path.clone(),
            node,
            net,
            Some(key.clone()),
            server_cancel,
        ));

        // Give `serve` a beat to bind before the client dials.
        for _ in 0..100 {
            if UnixStream::connect(&socket_path).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let register = register_hostname(&socket_path, "angelica")
            .await
            .expect("registration succeeds");
        assert_eq!(register.hostname, "angelica");
        assert_eq!(register.outcome, "registered");
        assert!(register.expires_unix.is_some());

        let resolve = resolve_hostname(&socket_path, "angelica")
            .await
            .expect("resolve succeeds");
        assert_eq!(resolve.hostname, "angelica");
        assert_eq!(resolve.addresses.len(), 1);
        // The whole point of `ResolvedTarget::ipv4`: an address target resolves to something a
        // caller can actually connect to, not only to hierarchical `Naddr` notation. This node's
        // `gsizes = [1]` leaves a single valid position, so its `/32` host address is 10.0.0.0.
        assert_eq!(
            resolve.addresses[0].ipv4,
            Some(Ipv4Addr::new(10, 0, 0, 0)),
            "address target must carry its routable IPv4"
        );
        assert!(!resolve.addresses[0].target.is_empty());

        cancel.cancel();
        let _ = server.await;
        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&key);
    }

    #[tokio::test]
    async fn an_oversized_request_line_is_refused_with_an_error_reply() {
        let (node, net, _tasks) = single_node().await;
        let socket_path = unique_socket_path("oversized");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, &node, &net, None).await.unwrap();
        });
        let client = UnixStream::connect(&socket_path).await.unwrap();
        let (client_read, mut client_write) = client.into_split();
        client_write
            .write_all(&vec![b'a'; 64 * 1024])
            .await
            .unwrap();
        client_write.shutdown().await.unwrap();
        let mut text = String::new();
        BufReader::new(client_read)
            .read_to_string(&mut text)
            .await
            .unwrap();
        server.await.unwrap();
        let reply: ErrorReply = toml::from_str(&text).expect("oversized request yields ErrorReply");
        assert!(reply.error.contains("exceeds"), "{}", reply.error);
        let _ = std::fs::remove_file(&socket_path);
    }

    fn unique_socket_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "ntkd-status-test-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn the_bound_status_socket_is_private_to_its_owner() {
        let socket_path = unique_socket_path("mode");
        prepare_socket_path(&socket_path).unwrap();
        let _listener = bind_private(&socket_path).unwrap();
        let mode = std::fs::metadata(&socket_path)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_file(&socket_path);
    }

    #[tokio::test]
    async fn a_live_status_socket_is_never_stolen_by_a_second_daemon() {
        let socket_path = unique_socket_path("live");
        prepare_socket_path(&socket_path).unwrap();
        let _live = bind_private(&socket_path).unwrap();
        let err = prepare_socket_path(&socket_path).unwrap_err();
        assert!(format!("{err:#}").contains("already listening"), "{err:#}");
        assert!(
            UnixStream::connect(&socket_path).await.is_ok(),
            "the live daemon's socket must survive the refused takeover"
        );
        let _ = std::fs::remove_file(&socket_path);
    }

    #[tokio::test]
    async fn a_stale_status_socket_from_a_crashed_daemon_is_replaced() {
        let socket_path = unique_socket_path("stale");
        {
            let _crashed = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        }
        assert!(
            socket_path.exists(),
            "dropping a listener leaves the file behind"
        );
        prepare_socket_path(&socket_path).unwrap();
        let _fresh = bind_private(&socket_path).unwrap();
        assert!(UnixStream::connect(&socket_path).await.is_ok());
        let _ = std::fs::remove_file(&socket_path);
    }

    #[test]
    fn a_regular_file_at_the_status_socket_path_is_not_removed() {
        let path = unique_socket_path("regular");
        std::fs::write(&path, b"precious").unwrap();
        let err = prepare_socket_path(&path).unwrap_err();
        assert!(format!("{err:#}").contains("not a socket"), "{err:#}");
        assert_eq!(std::fs::read(&path).unwrap(), b"precious");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_default_status_socket_lives_in_the_runtime_directory_not_tmp() {
        use std::ffi::OsStr;
        assert_eq!(
            socket_path_in(Some(OsStr::new("/run/custom:/run/other"))),
            PathBuf::from("/run/custom/ntkd.sock")
        );
        assert_eq!(socket_path_in(None), PathBuf::from("/run/ntkd/ntkd.sock"));
        assert_eq!(
            socket_path_in(Some(OsStr::new(""))),
            PathBuf::from("/run/ntkd/ntkd.sock")
        );
    }

    #[tokio::test]
    async fn a_status_client_error_names_the_socket_path() {
        let socket_path = unique_socket_path("missing");
        let err = query(&socket_path).await.unwrap_err();
        assert!(
            format!("{err:#}").contains(&socket_path.display().to_string()),
            "{err:#}"
        );
    }
}
