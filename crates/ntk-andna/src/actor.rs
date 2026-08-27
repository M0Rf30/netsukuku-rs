//! The single-owner actor holding this node's ANDNA state, and [`Handle`], the only interaction
//! path — both this crate's own [`crate::service::AndnaService`]/[`crate::service::CounterService`]
//! (inbound, from the network) and top-level `register`/`resolve`/`renew` callers (outbound, to
//! the network) go through it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ntk_common::Topology;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::counter::{CounterCache, CounterRejected};
use crate::error::Error;
use crate::hostname::{Hostname, HostnameHash};
use crate::record::{Cache, HostedRecord, RegisterOutcome, RegisterRejected, RegisterRequest};
use crate::route::counter_route_key;
use crate::service::{AndnaService, CounterService, andna_service_id, counter_service_id};
use crate::snsd::SnsdRecord;
use crate::substrate::AndnaSubstrate;
use crate::wire;

/// Current unix time in seconds — the only wall-clock read in this crate. Every domain function
/// ([`crate::record::Cache::register`], [`crate::counter::CounterCache::try_reserve`], ...) takes
/// `now` as an explicit parameter instead of reading a clock itself, so tests inject arbitrary
/// times directly rather than needing `tokio::time::pause`/`advance`.
pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Everything that can go wrong in a top-level [`Handle::register`]/[`Handle::resolve`]/
/// [`Handle::renew`] call.
#[derive(Debug, thiserror::Error)]
pub enum AndnaError {
    /// The caller-constructed request's own signature didn't verify — checked locally before
    /// any network round trip.
    #[error("signature verification failed")]
    InvalidSignature,
    /// The substrate could not route the request to any node.
    #[error("routing failed: {0}")]
    Routing(#[from] ntk_peerservices::ContactPeerError),
    /// A reply from the network failed to decode.
    #[error("wire decode error: {0}")]
    Decode(#[from] Error),
    /// The Counter service declined to reserve capacity for this hostname.
    #[error("counter service denied capacity: {0}")]
    CounterDenied(String),
    /// The Andna hash-node declined the registration.
    #[error("hash-node rejected registration: {0}")]
    Rejected(String),
}

/// A read-only snapshot of the records this node currently holds — both as the `Andna` hash-node
/// role (`hosted`) and the `Counter` role (`counters`, live reservation count per registrant
/// position) — published on every change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Hostnames held under the `Andna` hash-node/replica role.
    pub hosted: BTreeMap<Hostname, HostedRecord>,
    /// Live reservation count per registrant position, held under the `Counter` role.
    pub counters: BTreeMap<Vec<u32>, usize>,
}

/// Notifications published on a [`broadcast`] stream in place of upstream's GObject signals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Registered { hostname: Hostname, expires_at: u64 },
    Renewed { hostname: Hostname, expires_at: u64 },
    Expired { hostname: Hostname },
}

enum Cmd {
    Register {
        req: Box<RegisterRequest>,
        now: u64,
        reply: oneshot::Sender<Result<RegisterOutcome, RegisterRejected>>,
    },
    Resolve {
        hostname: Hostname,
        service: u16,
        now: u64,
        reply: oneshot::Sender<Vec<SnsdRecord>>,
    },
    CounterReserve {
        registrant: Vec<u32>,
        hash: HostnameHash,
        now: u64,
        reply: oneshot::Sender<Result<usize, CounterRejected>>,
    },
    PurgeExpired {
        now: u64,
    },
}

struct State {
    cache: Cache,
    counters: CounterCache,
    config: Config,
    snapshot_tx: watch::Sender<Snapshot>,
    events_tx: broadcast::Sender<Event>,
}

impl State {
    fn publish_snapshot(&self, now: u64) {
        let hosted = self.cache.records().clone();
        let counters = self.counters.snapshot(now);
        let _ = self.snapshot_tx.send(Snapshot { hosted, counters });
    }

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Register { req, now, reply } => {
                let outcome = self.cache.register(
                    &req,
                    now,
                    self.config.name_ttl,
                    self.config.min_renewal_interval,
                    self.config.max_snsd_records_per_service,
                    self.config.max_snsd_records_total,
                    self.config.max_hosted_records,
                );
                match &outcome {
                    Ok(applied) => {
                        let event = match *applied {
                            RegisterOutcome::Registered { expires_at } => Event::Registered {
                                hostname: req.hostname.clone(),
                                expires_at,
                            },
                            RegisterOutcome::Renewed { expires_at } => Event::Renewed {
                                hostname: req.hostname.clone(),
                                expires_at,
                            },
                        };
                        let _ = self.events_tx.send(event);
                        self.publish_snapshot(now);
                    }
                    Err(rejected) => {
                        tracing::debug!(
                            hostname = %req.hostname,
                            %rejected,
                            "ntk-andna: registration rejected"
                        );
                    }
                }
                let _ = reply.send(outcome);
            }
            Cmd::Resolve {
                hostname,
                service,
                now,
                reply,
            } => {
                let mut rng = rand::rng();
                let records = self.cache.resolve(&hostname, service, now, &mut rng);
                let _ = reply.send(records);
            }
            Cmd::CounterReserve {
                registrant,
                hash,
                now,
                reply,
            } => {
                let outcome = self.counters.try_reserve(
                    &registrant,
                    hash,
                    now,
                    self.config.name_ttl,
                    self.config.max_hostnames_per_registrant,
                    self.config.max_counter_registrants,
                );
                if outcome.is_ok() {
                    self.publish_snapshot(now);
                }
                let _ = reply.send(outcome);
            }
            Cmd::PurgeExpired { now } => {
                let expired = self.cache.purge_expired(now);
                self.counters.purge_expired(now);
                for hostname in expired {
                    let _ = self.events_tx.send(Event::Expired { hostname });
                }
                self.publish_snapshot(now);
            }
        }
    }
}

/// The single-owner actor. Spawn with [`Manager::run`]; interact only through the [`Handle`] it
/// returns.
pub struct Manager {
    state: State,
    cmd_rx: mpsc::Receiver<Cmd>,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager").finish_non_exhaustive()
    }
}

impl Manager {
    /// Builds a `Manager` and its [`Handle`], bound to `substrate` (the PeerServices instance
    /// this node's `Andna`/`Counter` services run on and whose `contact_peer`/`replicate` the
    /// `Handle`'s own `register`/`resolve`/`renew` use).
    #[must_use]
    pub fn new(substrate: Arc<dyn AndnaSubstrate>, config: Config) -> (Self, Handle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let (snapshot_tx, snapshot_rx) = watch::channel(Snapshot::default());
        let (events_tx, _events_rx) = broadcast::channel(256);
        let state = State {
            cache: Cache::new(),
            counters: CounterCache::new(),
            config,
            snapshot_tx,
            events_tx: events_tx.clone(),
        };
        let handle = Handle {
            config,
            cmd_tx,
            snapshot_rx,
            events_tx,
            substrate,
        };
        (Self { state, cmd_rx }, handle)
    }

    /// Runs the actor loop until `cancel` fires or every [`Handle`] is dropped. `State::handle`
    /// never awaits, so this loop can never violate the "never await an outbound RPC inside the
    /// command loop" rule — the only outbound calls this crate makes
    /// ([`Handle::register`]/[`Handle::resolve`]/[`Handle::renew`]) run in the caller's own task,
    /// never inside this loop.
    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(cmd) => self.state.handle(cmd),
                    None => return,
                },
            }
        }
    }
}

/// Cheap-clone handle to a running [`Manager`]. The only way to interact with it.
#[derive(Clone)]
pub struct Handle {
    config: Config,
    cmd_tx: mpsc::Sender<Cmd>,
    snapshot_rx: watch::Receiver<Snapshot>,
    events_tx: broadcast::Sender<Event>,
    substrate: Arc<dyn AndnaSubstrate>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}

impl Handle {
    async fn call<T>(&self, f: impl FnOnce(oneshot::Sender<T>) -> Cmd) -> T {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(f(tx))
            .await
            .expect("actor task is alive for the Handle's lifetime");
        rx.await
            .expect("actor never drops a reply sender without replying")
    }

    /// The [`Topology`] the underlying substrate runs on.
    #[must_use]
    pub fn topology(&self) -> &Topology {
        self.substrate.topology()
    }

    /// A read-only, always-current snapshot of the records this node holds.
    #[must_use]
    pub fn snapshot(&self) -> watch::Receiver<Snapshot> {
        self.snapshot_rx.clone()
    }

    /// Subscribes to registration/renewal/expiry events.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events_tx.subscribe()
    }

    /// Drops every expired hostname/reservation, publishing an [`Event::Expired`] per hostname
    /// freed. Lazy expiry already makes this optional for *correctness* ([`Cache::resolve`] and
    /// [`Cache::register`] both check expiry on access) — it exists to reclaim memory, which
    /// matters because [`Config::max_hosted_records`]/[`Config::max_counter_registrants`] only
    /// stay meaningful caps if something actually calls this on a live daemon; see
    /// [`run_expiry_reclaimer`], the driver this crate ships for that purpose.
    pub async fn purge_expired(&self, now: u64) {
        self.cmd_tx
            .send(Cmd::PurgeExpired { now })
            .await
            .expect("actor task is alive for the Handle's lifetime");
    }

    /// This node's current [`Config`], as constructed at [`Manager::new`] — read-only, for
    /// callers (e.g. [`run_expiry_reclaimer`]) that need a value from it without reaching into
    /// the actor.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// Executes an inbound registration/renewal against this node's local Andna cache — called
    /// by [`crate::service::AndnaService::exec`], never directly by network-facing code.
    pub(crate) async fn handle_register(
        &self,
        req: RegisterRequest,
        now: u64,
    ) -> Result<RegisterOutcome, RegisterRejected> {
        self.call(|reply| Cmd::Register {
            req: Box::new(req),
            now,
            reply,
        })
        .await
    }

    /// Executes an inbound resolve against this node's local Andna cache — called by
    /// [`crate::service::AndnaService::exec`].
    pub(crate) async fn handle_resolve(
        &self,
        hostname: Hostname,
        service: u16,
        now: u64,
    ) -> Vec<SnsdRecord> {
        self.call(|reply| Cmd::Resolve {
            hostname,
            service,
            now,
            reply,
        })
        .await
    }

    /// Executes an inbound reservation against this node's local Counter cache — called by
    /// [`crate::service::CounterService::exec`].
    pub(crate) async fn handle_counter_reserve(
        &self,
        registrant: Vec<u32>,
        hash: HostnameHash,
        now: u64,
    ) -> Result<usize, CounterRejected> {
        self.call(|reply| Cmd::CounterReserve {
            registrant,
            hash,
            now,
            reply,
        })
        .await
    }

    /// Registers this node's `Andna`/`Counter` services on the substrate — call once per node
    /// before any [`Handle::register`]/[`Handle::resolve`] call is expected to reach it.
    pub async fn register_services(&self) {
        self.substrate
            .register(Arc::new(AndnaService::new(self.clone())))
            .await;
        self.substrate
            .register(Arc::new(CounterService::new(self.clone())))
            .await;
    }

    /// Registers or renews `req.hostname`: reserves capacity at the Counter service keyed by
    /// `req.owner_naddr` (RFC 0007), then replicates the registration to
    /// [`Config::replication_factor`] nodes closest to the hostname's hash target (RFC 0014
    /// §2.2 step 5). The first replica's reply is authoritative for the caller; the rest exist
    /// purely for failover, not consensus.
    ///
    /// # Errors
    /// [`AndnaError::InvalidSignature`] if `req` doesn't verify against its own `owner_key`
    /// (checked locally, before any network call); [`AndnaError::Routing`] if no node could be
    /// reached; [`AndnaError::CounterDenied`] if the registrant's 256-hostname cap is exhausted;
    /// [`AndnaError::Rejected`] if the hash-node declined the registration (collision, stale
    /// sequence, ...).
    pub async fn register(&self, req: RegisterRequest) -> Result<RegisterOutcome, AndnaError> {
        req.verify().map_err(|_| AndnaError::InvalidSignature)?;
        let topology = self.substrate.topology().clone();

        let counter_target =
            ntk_peerservices::hash_to_tuple(&topology, counter_route_key(&req.owner_naddr));
        let counter_reply = self
            .substrate
            .contact_peer(
                counter_service_id(),
                counter_target,
                wire::pack_counter_request(req.hostname.hash()),
                self.config.call_timeout,
            )
            .await?;
        if let Err(reason) = wire::unpack_counter_reply(&counter_reply)? {
            return Err(AndnaError::CounterDenied(reason));
        }

        let andna_target =
            ntk_peerservices::hash_to_tuple(&topology, req.hostname.hash().route_key());
        let replies = self
            .substrate
            .replicate(
                andna_service_id(),
                andna_target,
                wire::pack_register_request(&req),
                self.config.call_timeout,
                self.config.replication_factor,
            )
            .await;
        let first = replies.first().ok_or(AndnaError::Routing(
            ntk_peerservices::ContactPeerError::NoParticipants,
        ))?;
        wire::unpack_register_reply(first)?.map_err(AndnaError::Rejected)
    }

    /// An ANDNA renewal *is* a registration whose `sequence` strictly increases past the stored
    /// record's (`crate::record`'s module doc comment) — this is a thin alias for
    /// [`Handle::register`], kept as a separate name for call-site clarity per this crate's
    /// contracted `register`/`resolve`/`renew` surface.
    pub async fn renew(&self, req: RegisterRequest) -> Result<RegisterOutcome, AndnaError> {
        self.register(req).await
    }

    /// Resolves `hostname` for `service` via the Andna hash-node closest to its hash target.
    ///
    /// # Errors
    /// [`AndnaError::Routing`] if no node could be reached.
    pub async fn resolve(
        &self,
        hostname: &Hostname,
        service: u16,
    ) -> Result<Vec<SnsdRecord>, AndnaError> {
        let target =
            ntk_peerservices::hash_to_tuple(self.substrate.topology(), hostname.hash().route_key());
        let reply = self
            .substrate
            .contact_peer(
                andna_service_id(),
                target,
                wire::pack_resolve_request(hostname, service),
                self.config.call_timeout,
            )
            .await?;
        Ok(wire::unpack_resolve_reply(&reply)?)
    }
}
/// Drives [`Handle::purge_expired`] on [`Config::expiry_purge_interval`] for as long as `cancel`
/// stays alive — nothing else in this crate ever calls `purge_expired` on its own (that method's
/// own doc), so without a driver like this expired hostnames/reservations are never reclaimed
/// and the capacity caps this crate enforces can fill up permanently with an attacker's
/// already-expired garbage. Callers (`ntkd::node::services::spawn`) `tasks.spawn` this directly,
/// the same convention as [`Manager::run`], with their own [`CancellationToken`] child.
///
/// This is a plain interval loop, not the single-owner actor's own command loop — it only ever
/// sends into [`Handle`]'s existing `mpsc` channel (never a raw outbound RPC), so it carries none
/// of that loop's own "never await an outbound call" constraint.
pub async fn run_expiry_reclaimer(handle: Handle, cancel: CancellationToken) {
    let mut ticker = time::interval(handle.config.expiry_purge_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = ticker.tick() => handle.purge_expired(unix_now()).await,
        }
    }
}
