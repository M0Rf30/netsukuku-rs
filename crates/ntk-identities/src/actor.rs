//! The identity-manager actor: single-owner protocol state
//! (`research/notes/06-rust-stack.md` §Concurrency), fed by an `mpsc`
//! command queue, publishing read-only snapshots via `tokio::sync::watch`
//! and events via `tokio::sync::broadcast`. [`Handle`] is the only way to
//! reach it.
//!
//! Ports `IdentityManager` (`identities/identities.vala:60-928`) minus
//! everything this crate excludes by design: real-NIC/`handled_nics`
//! bookkeeping and kernel operations (`IIdmgmtNetnsManager`) are the
//! daemon's job via `ntk-netlink` (see [`crate::pseudo`]); the concrete
//! neighborhood arc type is opaque ([`ArcId`]/[`ArcInfo`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use ntk_common::Naddr;
use ntk_proto::v1::{
    CallerContext, Empty, IdentityMatchDuplicationArgs, IdentityNotifyIdentityArcRemovedArgs,
    MethodCall, ResponsePayload, TypedValue, method_call, response_payload,
};
use ntk_rpc::RpcClient;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::arc::{ArcId, ArcInfo, IdentityArc, IdentityArcChange};
use crate::error::Error;
use crate::events::IdentityEvent;
use crate::identity::{IdentityId, IdentityRecord, IdentityStatus};
use crate::migration::{DuplicationData, MigrationDeviceInfo, MigrationId};
use crate::registry::Registry;
use crate::snapshot::IdentitySnapshot;
use crate::stub::IdentityStubFactory;
use crate::wire::{
    duplication_data_from_typed_value, identity_id_from_typed_value, identity_id_to_typed_value,
};

/// Cleanup deadline for a pending migration that is never followed up by
/// [`Handle::migrate`] (`prepare_add_identity`'s tasklet,
/// `identities.vala:304,415-417`: `tasklet.ms_wait(600000)`).
const MIGRATION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(600);

/// Shared time budget for the whole duplication pass across every arc
/// (`Timer mintime = new Timer(10000)`, `identities.vala:493`).
const MIGRATION_MIN_BUDGET: Duration = Duration::from_secs(10);

/// Floor timeout for one `match_duplication` call once `MIGRATION_MIN_BUDGET`
/// is exhausted (`new Timer(3000)`, `identities.vala:532`).
const MIGRATION_CALL_FLOOR: Duration = Duration::from_secs(3);

/// Timeout for one best-effort `notify_identity_arc_removed` call
/// (`Timer(500)`, `identities.vala:674,705`).
const NOTIFY_TIMEOUT: Duration = Duration::from_millis(500);

/// A migration this node knows about but has not (yet) executed —
/// upstream's `MigrationData` (`identities.vala:997-1009`).
///
/// **Substitution note**: upstream's peer-side `match_duplication` busy-waits
/// on `migration_data.ready` (`while (!ready) tasklet.ms_wait(50)`,
/// `identities.vala:854`) with no bound other than the unrelated 600s
/// cleanup racing in a different tasklet — if `add_identity`/[`Handle::migrate`]
/// never arrives, that busy-wait never returns. This port replaces it with
/// `ready_tx`/`ready_rx` (a [`watch::channel`]) plus `deadline`: callers
/// `tokio::time::timeout_at(deadline, ready_rx.wait_for(...))`, so the wait
/// is always bounded by exactly the cleanup deadline upstream's own comment
/// says it should race against (research/notes/01 §5's "Open questions").
/// Dropping this record (on cleanup) drops `ready_tx`, which makes any
/// in-flight `wait_for` observe a closed channel — the same "reject" outcome
/// as a timeout.
struct PendingMigration {
    ready_tx: watch::Sender<bool>,
    ready_rx: watch::Receiver<bool>,
    deadline: Instant,
    new_id: Option<IdentityId>,
    devices: HashMap<String, MigrationDeviceInfo>,
}

/// What [`Cmd::LookupPendingMigration`] hands back to the inbound
/// `match_duplication` handler: enough to bounded-wait for readiness
/// without holding the actor's state.
pub(crate) struct PendingLookup {
    pub(crate) ready: watch::Receiver<bool>,
    pub(crate) deadline: Instant,
}

/// Outcome of one `match_duplication` call made while duplicating an
/// identity-arc during [`Handle::migrate`].
pub(crate) struct IdentityArcDuplication {
    peer_id: IdentityId,
    dup_data: Option<DuplicationData>,
}

/// Outcome of duplicating every identity-arc on one arc.
pub(crate) struct ArcDuplicationOutcome {
    arc: ArcId,
    /// True once any identity-arc on this arc failed to duplicate
    /// (`arc_is_broken`, `identities.vala:498,511,546,567`) — the whole arc
    /// is then removed.
    broken: bool,
    identity_arcs: Vec<IdentityArcDuplication>,
}

pub(crate) enum Cmd {
    AddArc {
        arc: ArcId,
        info: ArcInfo,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    ApplyArcAdd {
        arc: ArcId,
        peer_main_id: Result<IdentityId, Error>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    RemoveArc {
        arc: ArcId,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    PrepareMigration {
        migration_id: MigrationId,
        old_id: IdentityId,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Migrate {
        migration_id: MigrationId,
        old_id: IdentityId,
        devices: HashMap<String, MigrationDeviceInfo>,
        reply: oneshot::Sender<Result<IdentityId, Error>>,
    },
    ApplyMigrationDuplication {
        migration_id: MigrationId,
        old_id: IdentityId,
        new_id: IdentityId,
        outcomes: Vec<ArcDuplicationOutcome>,
        reply: oneshot::Sender<Result<IdentityId, Error>>,
    },
    CleanupMigration {
        migration_id: MigrationId,
        old_id: IdentityId,
    },
    RemoveIdentity {
        id: IdentityId,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    AbortMigration {
        old_id: IdentityId,
        new_id: IdentityId,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    ArcOwnership {
        reply: oneshot::Sender<BTreeMap<IdentityId, Vec<ArcId>>>,
    },
    SetNaddr {
        id: IdentityId,
        naddr: Option<Naddr>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    LookupPendingMigration {
        migration_id: MigrationId,
        old_id: IdentityId,
        reply: oneshot::Sender<Option<PendingLookup>>,
    },
    FetchDuplicationData {
        migration_id: MigrationId,
        old_id: IdentityId,
        arc: ArcId,
        reply: oneshot::Sender<Result<DuplicationData, Error>>,
    },
    NeighbourMigrated {
        my_id: IdentityId,
        my_peer_old_id: IdentityId,
        my_peer_new_id: IdentityId,
        my_peer_old_id_new_mac: String,
        my_peer_old_id_new_linklocal: String,
        arc: ArcId,
    },
    NotifyIdentityArcRemoved {
        my_id: IdentityId,
        peer_id: IdentityId,
        arc: ArcId,
    },
}

struct State {
    registry: Registry,
    arcs: HashMap<ArcId, ArcInfo>,
    identity_arcs: HashMap<(IdentityId, ArcId), Vec<IdentityArc>>,
    pending_migrations: HashMap<(MigrationId, IdentityId), PendingMigration>,
    stub_factory: Arc<dyn IdentityStubFactory>,
    self_tx: mpsc::Sender<Cmd>,
    snapshot_tx: watch::Sender<IdentitySnapshot>,
    events_tx: broadcast::Sender<IdentityEvent>,
    /// Lets background tasks that would otherwise outlive this actor's own shutdown (the
    /// 600s `prepare_migration` cleanup timer, [`State::on_prepare_migration`]) race their own
    /// wait against cancellation instead of blocking [`run`]'s post-loop `background.join_next`
    /// drain for up to that whole duration on every process shutdown.
    cancel: CancellationToken,
}

impl State {
    fn publish_event(&self, event: IdentityEvent) {
        let _ = self.events_tx.send(event);
    }

    fn publish_snapshot(&self) {
        let _ = self.snapshot_tx.send(self.registry.snapshot());
    }

    /// Tears down one arc and every identity's identity-arcs over it —
    /// upstream's `remove_arc` (`identities.vala:318-339`), triggered both
    /// by [`Handle::remove_arc`] and by an arc going bad mid-communication
    /// (`RemoveArcTasklet`, `identities.vala:305-316`). Upstream delays this
    /// by a tasklet-yield (`ms_wait(10)`) purely so the cooperative
    /// scheduler unwinds the failing call first; under the actor model this
    /// command is already the only code touching `self`, so no delay is
    /// needed.
    fn remove_arc(&mut self, arc: ArcId) -> bool {
        if self.arcs.remove(&arc).is_none() {
            return false;
        }
        let ids: Vec<IdentityId> = self.registry.ids().collect();
        for id in ids {
            if let Some(list) = self.identity_arcs.remove(&(id, arc)) {
                for ia in list {
                    self.publish_event(IdentityEvent::IdentityArc {
                        arc,
                        identity: id,
                        change: IdentityArcChange::Removing {
                            peer_id: ia.peer_id,
                        },
                    });
                    self.publish_event(IdentityEvent::IdentityArc {
                        arc,
                        identity: id,
                        change: IdentityArcChange::Removed {
                            peer_id: ia.peer_id,
                        },
                    });
                }
            }
        }
        self.publish_event(IdentityEvent::ArcRemoved { arc });
        true
    }

    fn handle(&mut self, cmd: Cmd, background: &mut JoinSet<()>) {
        match cmd {
            Cmd::AddArc { arc, info, reply } => self.on_add_arc(arc, info, reply, background),
            Cmd::ApplyArcAdd {
                arc,
                peer_main_id,
                reply,
            } => {
                self.on_apply_arc_add(arc, peer_main_id, reply);
            }
            Cmd::RemoveArc { arc, reply } => {
                let result = if self.remove_arc(arc) {
                    Ok(())
                } else {
                    Err(Error::UnknownArc(arc))
                };
                let _ = reply.send(result);
            }
            Cmd::PrepareMigration {
                migration_id,
                old_id,
                reply,
            } => {
                self.on_prepare_migration(migration_id, old_id, reply, background);
            }
            Cmd::Migrate {
                migration_id,
                old_id,
                devices,
                reply,
            } => {
                self.on_migrate(migration_id, old_id, devices, reply, background);
            }
            Cmd::ApplyMigrationDuplication {
                migration_id,
                old_id,
                new_id,
                outcomes,
                reply,
            } => self.on_apply_migration_duplication(migration_id, old_id, new_id, outcomes, reply),
            Cmd::CleanupMigration {
                migration_id,
                old_id,
            } => {
                self.pending_migrations.remove(&(migration_id, old_id));
            }
            Cmd::RemoveIdentity { id, reply } => self.on_remove_identity(id, reply, background),
            Cmd::AbortMigration {
                old_id,
                new_id,
                reply,
            } => self.on_abort_migration(old_id, new_id, reply, background),
            Cmd::ArcOwnership { reply } => {
                let _ = reply.send(self.arc_ownership());
            }
            Cmd::SetNaddr { id, naddr, reply } => {
                let result = self.registry.set_naddr(id, naddr);
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            Cmd::LookupPendingMigration {
                migration_id,
                old_id,
                reply,
            } => {
                let lookup = self
                    .pending_migrations
                    .get(&(migration_id, old_id))
                    .map(|p| PendingLookup {
                        ready: p.ready_rx.clone(),
                        deadline: p.deadline,
                    });
                let _ = reply.send(lookup);
            }
            Cmd::FetchDuplicationData {
                migration_id,
                old_id,
                arc,
                reply,
            } => {
                let _ = reply.send(self.fetch_duplication_data(migration_id, old_id, arc));
            }
            Cmd::NeighbourMigrated {
                my_id,
                my_peer_old_id,
                my_peer_new_id,
                my_peer_old_id_new_mac,
                my_peer_old_id_new_linklocal,
                arc,
            } => self.on_neighbour_migrated(
                my_id,
                my_peer_old_id,
                my_peer_new_id,
                my_peer_old_id_new_mac,
                my_peer_old_id_new_linklocal,
                arc,
            ),
            Cmd::NotifyIdentityArcRemoved {
                my_id,
                peer_id,
                arc,
            } => {
                self.on_notify_identity_arc_removed(my_id, peer_id, arc);
            }
        }
    }

    fn on_add_arc(
        &mut self,
        arc: ArcId,
        info: ArcInfo,
        reply: oneshot::Sender<Result<(), Error>>,
        background: &mut JoinSet<()>,
    ) {
        if self.arcs.contains_key(&arc) {
            let _ = reply.send(Err(Error::DuplicateArc(arc)));
            return;
        }
        self.arcs.insert(arc, info);
        for id in self.registry.ids().collect::<Vec<_>>() {
            self.identity_arcs.entry((id, arc)).or_default();
        }
        let stub = self.stub_factory.stub(arc);
        let main_id = self.registry.main_id();
        let cmd_tx = self.self_tx.clone();
        background.spawn(async move {
            let peer_main_id = resolve_peer_main_id(stub.as_ref(), main_id).await;
            let _ = cmd_tx
                .send(Cmd::ApplyArcAdd {
                    arc,
                    peer_main_id,
                    reply,
                })
                .await;
        });
    }

    fn on_apply_arc_add(
        &mut self,
        arc: ArcId,
        peer_main_id: Result<IdentityId, Error>,
        reply: oneshot::Sender<Result<(), Error>>,
    ) {
        match peer_main_id {
            Ok(peer_id) => {
                let Some(info) = self.arcs.get(&arc).cloned() else {
                    let _ = reply.send(Err(Error::UnknownArc(arc)));
                    return;
                };
                let main_id = self.registry.main_id();
                self.identity_arcs
                    .entry((main_id, arc))
                    .or_default()
                    .push(IdentityArc {
                        peer_id,
                        peer_mac: info.peer_mac.clone(),
                        peer_linklocal: info.peer_linklocal.clone(),
                    });
                self.publish_event(IdentityEvent::IdentityArc {
                    arc,
                    identity: main_id,
                    change: IdentityArcChange::Added {
                        peer_id,
                        peer_mac: info.peer_mac,
                        peer_linklocal: info.peer_linklocal,
                    },
                });
                let _ = reply.send(Ok(()));
            }
            Err(err) => {
                self.remove_arc(arc);
                let _ = reply.send(Err(err));
            }
        }
    }

    fn on_prepare_migration(
        &mut self,
        migration_id: MigrationId,
        old_id: IdentityId,
        reply: oneshot::Sender<Result<(), Error>>,
        background: &mut JoinSet<()>,
    ) {
        if self.registry.get(old_id).is_none() {
            let _ = reply.send(Err(Error::UnknownIdentity(old_id)));
            return;
        }
        let key = (migration_id, old_id);
        if self.pending_migrations.contains_key(&key) {
            let _ = reply.send(Err(Error::DuplicateMigration {
                migration_id,
                old_id,
            }));
            return;
        }
        let (ready_tx, ready_rx) = watch::channel(false);
        let deadline = Instant::now() + MIGRATION_CLEANUP_TIMEOUT;
        self.pending_migrations.insert(
            key,
            PendingMigration {
                ready_tx,
                ready_rx,
                deadline,
                new_id: None,
                devices: HashMap::new(),
            },
        );
        let cmd_tx = self.self_tx.clone();
        let cancel = self.cancel.clone();
        background.spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep_until(deadline) => {}
            }
            let _ = cmd_tx
                .send(Cmd::CleanupMigration {
                    migration_id,
                    old_id,
                })
                .await;
        });
        let _ = reply.send(Ok(()));
    }

    fn on_migrate(
        &mut self,
        migration_id: MigrationId,
        old_id: IdentityId,
        devices: HashMap<String, MigrationDeviceInfo>,
        reply: oneshot::Sender<Result<IdentityId, Error>>,
        background: &mut JoinSet<()>,
    ) {
        let key = (migration_id, old_id);
        if !self.pending_migrations.contains_key(&key) {
            let _ = reply.send(Err(Error::UnknownMigration {
                migration_id,
                old_id,
            }));
            return;
        }
        let Some(old_record) = self.registry.get(old_id).cloned() else {
            let _ = reply.send(Err(Error::UnknownIdentity(old_id)));
            return;
        };

        let new_id = self.registry.fresh_id();
        let new_record = IdentityRecord {
            id: new_id,
            naddr: None,
            status: old_record.status,
        };
        // Cannot fail: `fresh_id` guarantees no collision.
        let _ = self.registry.insert(new_record);
        // The old identity becomes (or remains) the connectivity fork that
        // keeps the migrated g-node's external arcs alive; the new identity
        // inherits whatever role the old one had (`identities.vala:441-464`
        // plus this crate's status model, see `IdentityStatus` doc).
        let _ = self
            .registry
            .set_status(old_id, IdentityStatus::Connectivity);
        if self.registry.main_id() == old_id {
            self.registry.reassign_main(new_id);
        }

        let pending = self
            .pending_migrations
            .get_mut(&key)
            .expect("checked above");
        pending.new_id = Some(new_id);
        pending.devices = devices.clone();
        let _ = pending.ready_tx.send(true);

        self.publish_event(IdentityEvent::IdentityAdded {
            id: new_id,
            migration_id: Some(migration_id),
        });
        self.publish_snapshot();

        let arcs_snapshot: Vec<(ArcId, ArcInfo)> =
            self.arcs.iter().map(|(a, i)| (*a, i.clone())).collect();
        let mut per_arc = Vec::new();
        for (arc, info) in arcs_snapshot {
            if let Some(list) = self.identity_arcs.get(&(old_id, arc))
                && !list.is_empty()
            {
                per_arc.push((arc, info, list.clone()));
            }
            self.identity_arcs.entry((new_id, arc)).or_default();
        }

        let stub_factory = self.stub_factory.clone();
        let cmd_tx = self.self_tx.clone();
        background.spawn(async move {
            let outcomes = run_migration_duplication(
                stub_factory.as_ref(),
                old_id,
                new_id,
                migration_id,
                per_arc,
                devices,
            )
            .await;
            let _ = cmd_tx
                .send(Cmd::ApplyMigrationDuplication {
                    migration_id,
                    old_id,
                    new_id,
                    outcomes,
                    reply,
                })
                .await;
        });
    }

    fn on_apply_migration_duplication(
        &mut self,
        migration_id: MigrationId,
        old_id: IdentityId,
        new_id: IdentityId,
        outcomes: Vec<ArcDuplicationOutcome>,
        reply: oneshot::Sender<Result<IdentityId, Error>>,
    ) {
        let mut broken_arcs = Vec::new();
        for outcome in outcomes {
            let arc = outcome.arc;
            for ia in outcome.identity_arcs {
                if let Some(list) = self.identity_arcs.get_mut(&(old_id, arc))
                    && let (Some(w0), Some(dup)) = (
                        list.iter_mut().find(|w| w.peer_id == ia.peer_id),
                        ia.dup_data.as_ref(),
                    )
                {
                    w0.peer_mac = dup.peer_old_id_new_mac.clone();
                    w0.peer_linklocal = dup.peer_old_id_new_linklocal.clone();
                }
                let w0_now = self
                    .identity_arcs
                    .get(&(old_id, arc))
                    .and_then(|l| l.iter().find(|w| w.peer_id == ia.peer_id).cloned());

                let w1_peer_id = ia.dup_data.as_ref().map_or(ia.peer_id, |d| d.peer_new_id);
                let (peer_mac, peer_linklocal) = w0_now
                    .map(|w| (w.peer_mac, w.peer_linklocal))
                    .unwrap_or_default();
                self.identity_arcs
                    .entry((new_id, arc))
                    .or_default()
                    .push(IdentityArc {
                        peer_id: w1_peer_id,
                        peer_mac: peer_mac.clone(),
                        peer_linklocal: peer_linklocal.clone(),
                    });
                self.publish_event(IdentityEvent::IdentityArc {
                    arc,
                    identity: new_id,
                    change: IdentityArcChange::Added {
                        peer_id: w1_peer_id,
                        peer_mac,
                        peer_linklocal,
                    },
                });
                if let Some(dup) = ia.dup_data {
                    self.publish_event(IdentityEvent::IdentityArc {
                        arc,
                        identity: old_id,
                        change: IdentityArcChange::Changed {
                            peer_id: ia.peer_id,
                            peer_mac: dup.peer_old_id_new_mac,
                            peer_linklocal: dup.peer_old_id_new_linklocal,
                            only_neighbour_migrated: false,
                        },
                    });
                }
            }
            if outcome.broken {
                broken_arcs.push(arc);
            }
        }
        for arc in broken_arcs {
            self.remove_arc(arc);
        }
        self.publish_event(IdentityEvent::IdentityDuplicated {
            migration_id,
            old_id,
            new_id,
        });
        self.publish_snapshot();
        let _ = reply.send(Ok(new_id));
    }

    fn fetch_duplication_data(
        &self,
        migration_id: MigrationId,
        old_id: IdentityId,
        arc: ArcId,
    ) -> Result<DuplicationData, Error> {
        let pending = self.pending_migrations.get(&(migration_id, old_id)).ok_or(
            Error::UnknownMigration {
                migration_id,
                old_id,
            },
        )?;
        let new_id = pending.new_id.ok_or(Error::UnknownMigration {
            migration_id,
            old_id,
        })?;
        let dev = &self.arcs.get(&arc).ok_or(Error::UnknownArc(arc))?.dev;
        let devdata = pending
            .devices
            .get(dev)
            .ok_or(Error::MissingField("MigrationDeviceInfo for arc's device"))?;
        Ok(DuplicationData {
            peer_new_id: new_id,
            peer_old_id_new_mac: devdata.old_id_new_mac.clone(),
            peer_old_id_new_linklocal: devdata.old_id_new_linklocal.clone(),
        })
    }

    fn on_neighbour_migrated(
        &mut self,
        my_id: IdentityId,
        my_peer_old_id: IdentityId,
        my_peer_new_id: IdentityId,
        my_peer_old_id_new_mac: String,
        my_peer_old_id_new_linklocal: String,
        arc: ArcId,
    ) {
        let Some(list) = self.identity_arcs.get_mut(&(my_id, arc)) else {
            return;
        };
        let Some(w0) = list.iter_mut().find(|w| w.peer_id == my_peer_old_id) else {
            return;
        };
        let original_mac = w0.peer_mac.clone();
        let original_linklocal = w0.peer_linklocal.clone();
        w0.peer_mac = my_peer_old_id_new_mac.clone();
        w0.peer_linklocal = my_peer_old_id_new_linklocal.clone();
        let new_ia = IdentityArc {
            peer_id: my_peer_new_id,
            peer_mac: original_mac,
            peer_linklocal: original_linklocal,
        };
        self.identity_arcs
            .entry((my_id, arc))
            .or_default()
            .push(new_ia.clone());
        self.publish_event(IdentityEvent::IdentityArc {
            arc,
            identity: my_id,
            change: IdentityArcChange::Changed {
                peer_id: my_peer_old_id,
                peer_mac: my_peer_old_id_new_mac,
                peer_linklocal: my_peer_old_id_new_linklocal,
                only_neighbour_migrated: true,
            },
        });
        self.publish_event(IdentityEvent::IdentityArc {
            arc,
            identity: my_id,
            change: IdentityArcChange::Added {
                peer_id: my_peer_new_id,
                peer_mac: new_ia.peer_mac,
                peer_linklocal: new_ia.peer_linklocal,
            },
        });
    }

    fn on_notify_identity_arc_removed(
        &mut self,
        my_id: IdentityId,
        peer_id: IdentityId,
        arc: ArcId,
    ) {
        let Some(list) = self.identity_arcs.get_mut(&(my_id, arc)) else {
            return;
        };
        let Some(pos) = list.iter().position(|w| w.peer_id == peer_id) else {
            return;
        };
        list.remove(pos);
        self.publish_event(IdentityEvent::IdentityArc {
            arc,
            identity: my_id,
            change: IdentityArcChange::Removing { peer_id },
        });
        self.publish_event(IdentityEvent::IdentityArc {
            arc,
            identity: my_id,
            change: IdentityArcChange::Removed { peer_id },
        });
    }

    /// Tears down `id`'s identity-arcs, removes it from the registry, and
    /// fires off best-effort peer notification — the shared tail of
    /// [`Cmd::RemoveIdentity`] and [`Cmd::AbortMigration`]. Callers are
    /// responsible for any main-identity guard: this unconditionally
    /// removes `id`, which must already be known-present and non-main.
    fn dismiss_identity(&mut self, id: IdentityId, background: &mut JoinSet<()>) {
        let mut notify_targets: Vec<(ArcId, IdentityId)> = Vec::new();
        for arc in self.arcs.keys().copied().collect::<Vec<_>>() {
            if let Some(list) = self.identity_arcs.remove(&(id, arc)) {
                notify_targets.extend(list.into_iter().map(|ia| (arc, ia.peer_id)));
            }
        }
        // Cannot fail: callers already validated `id` is present and
        // non-main (`AbortMigration` reassigns main away from `id` first).
        let _ = self.registry.dismiss(id);
        self.publish_event(IdentityEvent::IdentityDismissed { id });

        let stub_factory = self.stub_factory.clone();
        background.spawn(async move {
            notify_peers_arc_removed(stub_factory.as_ref(), id, notify_targets).await;
        });
    }

    fn on_remove_identity(
        &mut self,
        id: IdentityId,
        reply: oneshot::Sender<Result<(), Error>>,
        background: &mut JoinSet<()>,
    ) {
        if id == self.registry.main_id() {
            let _ = reply.send(Err(Error::CannotRemoveMainIdentity));
            return;
        }
        if self.registry.get(id).is_none() {
            let _ = reply.send(Err(Error::UnknownIdentity(id)));
            return;
        }
        self.dismiss_identity(id, background);
        self.publish_snapshot();
        let _ = reply.send(Ok(()));
    }

    /// Reverts a migration whose successor never finished hooking.
    /// Upstream has no equivalent at this layer — hooking/coordinator-level
    /// abort (`abort_enter`, `research/notes/01-vala-core-routing.md` §7,
    /// citing `coord.vala:434`) sits above this crate's non-goals — but
    /// this crate must still be able to undo *its own* bookkeeping so the
    /// composition root has a well-defined recovery instead of leaving the
    /// g-node's main identity pinned to a position that will never
    /// resolve: `new_id` is dismissed and `old_id` regains whatever status
    /// — including the main-identity role — it held immediately before
    /// [`Self::on_migrate`] forked it.
    fn on_abort_migration(
        &mut self,
        old_id: IdentityId,
        new_id: IdentityId,
        reply: oneshot::Sender<Result<(), Error>>,
        background: &mut JoinSet<()>,
    ) {
        let Some(new_record) = self.registry.get(new_id).cloned() else {
            let _ = reply.send(Err(Error::UnknownIdentity(new_id)));
            return;
        };
        if self.registry.get(old_id).is_none() {
            let _ = reply.send(Err(Error::UnknownIdentity(old_id)));
            return;
        }
        if self.registry.main_id() == new_id {
            self.registry.reassign_main(old_id);
        }
        // `new_id` inherited `old_id`'s pre-migration status at fork time
        // (`on_migrate`); restoring it now undoes that fork's demotion to
        // `Connectivity`.
        let _ = self.registry.set_status(old_id, new_record.status);
        self.dismiss_identity(new_id, background);
        self.publish_event(IdentityEvent::MigrationAborted { old_id, new_id });
        self.publish_snapshot();
        let _ = reply.send(Ok(()));
    }

    /// Which arcs each identity currently has at least one live
    /// identity-arc on (`identity_arcs`, `identities.vala:129,182-215`).
    fn arc_ownership(&self) -> BTreeMap<IdentityId, Vec<ArcId>> {
        let mut owned: BTreeMap<IdentityId, Vec<ArcId>> = BTreeMap::new();
        for (&(id, arc), list) in &self.identity_arcs {
            if !list.is_empty() {
                owned.entry(id).or_default().push(arc);
            }
        }
        for arcs in owned.values_mut() {
            arcs.sort_unstable();
        }
        owned
    }
}

/// `get_peer_main_id()` (`identities.vala:284-296,820-825`).
async fn resolve_peer_main_id(
    stub: &dyn RpcClient,
    local_id: IdentityId,
) -> Result<IdentityId, Error> {
    let call = MethodCall {
        call: Some(method_call::Call::IdentityGetPeerMainId(Empty {})),
    };
    let payload = stub
        .call(caller_context(local_id), TypedValue::default(), call)
        .await?;
    match payload.value {
        Some(response_payload::Value::Typed(tv)) => Ok(identity_id_from_typed_value(&tv)?),
        _ => Err(Error::UnexpectedResponse(
            "get_peer_main_id: expected TypedValue",
        )),
    }
}

/// Duplicates every identity-arc of `old_id` across every arc it has one on
/// (`add_identity`'s duplication loop, `identities.vala:493-576`),
/// sequentially, sharing one shrinking time budget across all calls
/// (`mintime`/`Timer(3000)` floor, `identities.vala:493,528-532`).
async fn run_migration_duplication(
    stub_factory: &dyn IdentityStubFactory,
    old_id: IdentityId,
    new_id: IdentityId,
    migration_id: MigrationId,
    arcs: Vec<(ArcId, ArcInfo, Vec<IdentityArc>)>,
    devices: HashMap<String, MigrationDeviceInfo>,
) -> Vec<ArcDuplicationOutcome> {
    let mintime_deadline = tokio::time::Instant::now() + MIGRATION_MIN_BUDGET;
    let mut results = Vec::with_capacity(arcs.len());
    for (arc, info, identity_arcs) in arcs {
        let devdata = devices.get(&info.dev);
        let mut broken = devdata.is_none();
        let mut outcomes = Vec::with_capacity(identity_arcs.len());
        for w0 in identity_arcs {
            let dup_data = match (&broken, devdata) {
                (false, Some(devdata)) => {
                    let budget = call_budget(mintime_deadline);
                    let stub = stub_factory.stub(arc);
                    let call = build_match_duplication_call(
                        migration_id,
                        w0.peer_id,
                        old_id,
                        new_id,
                        devdata,
                    );
                    let outcome = tokio::time::timeout(
                        budget,
                        stub.call(caller_context(old_id), TypedValue::default(), call),
                    )
                    .await;
                    match outcome {
                        Ok(Ok(payload)) => match decode_duplication_response(&payload) {
                            Ok(value) => value,
                            Err(_) => {
                                broken = true;
                                None
                            }
                        },
                        _ => {
                            broken = true;
                            None
                        }
                    }
                }
                _ => None,
            };
            outcomes.push(IdentityArcDuplication {
                peer_id: w0.peer_id,
                dup_data,
            });
        }
        results.push(ArcDuplicationOutcome {
            arc,
            broken,
            identity_arcs: outcomes,
        });
    }
    results
}

/// Timeout budget for the next `match_duplication` call: whatever remains
/// of the shared 10s window, floored at a fresh 3s
/// (`if (mintime.get_remaining() > 3000) time = mintime; else time = new
/// Timer(3000);`, `identities.vala:530-532`).
fn call_budget(mintime_deadline: tokio::time::Instant) -> Duration {
    let remaining = mintime_deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining > MIGRATION_CALL_FLOOR {
        remaining
    } else {
        MIGRATION_CALL_FLOOR
    }
}

fn build_match_duplication_call(
    migration_id: MigrationId,
    peer_id: IdentityId,
    old_id: IdentityId,
    new_id: IdentityId,
    devdata: &MigrationDeviceInfo,
) -> MethodCall {
    MethodCall {
        call: Some(method_call::Call::IdentityMatchDuplication(
            IdentityMatchDuplicationArgs {
                migration_id: migration_id.0,
                peer_id: Some(identity_id_to_typed_value(peer_id)),
                old_id: Some(identity_id_to_typed_value(old_id)),
                new_id: Some(identity_id_to_typed_value(new_id)),
                old_id_new_mac: devdata.old_id_new_mac.clone(),
                old_id_new_linklocal: devdata.old_id_new_linklocal.clone(),
            },
        )),
    }
}

/// `match_duplication`'s nullable `IDuplicationData?` return: `empty` is
/// `null`, `typed` is present (see `ResponsePayload`'s doc comment in
/// `ntk-proto/proto/ntk.proto`).
fn decode_duplication_response(
    payload: &ResponsePayload,
) -> Result<Option<DuplicationData>, Error> {
    match &payload.value {
        Some(response_payload::Value::Empty(_)) | None => Ok(None),
        Some(response_payload::Value::Typed(tv)) => {
            Ok(Some(duplication_data_from_typed_value(tv)?))
        }
        Some(response_payload::Value::Boolean(_)) => Err(Error::UnexpectedResponse(
            "match_duplication: unexpected bool response",
        )),
    }
}

/// Best-effort `notify_identity_arc_removed` fan-out
/// (`remove_identity`, `identities.vala:685-730`): sequential per arc,
/// stopping early on that arc's first failure (mirrors the `break` in the
/// upstream per-arc loop), never surfaced to the caller of
/// [`Handle::remove_identity`] — upstream's own version has no error path
/// either.
async fn notify_peers_arc_removed(
    stub_factory: &dyn IdentityStubFactory,
    my_id: IdentityId,
    targets: Vec<(ArcId, IdentityId)>,
) {
    let mut broken_arcs: HashSet<ArcId> = HashSet::new();
    for (arc, peer_id) in targets {
        if broken_arcs.contains(&arc) {
            continue;
        }
        let stub = stub_factory.stub(arc);
        let call = MethodCall {
            call: Some(method_call::Call::IdentityNotifyIdentityArcRemoved(
                IdentityNotifyIdentityArcRemovedArgs {
                    peer_id: Some(identity_id_to_typed_value(peer_id)),
                    my_id: Some(identity_id_to_typed_value(my_id)),
                },
            )),
        };
        let outcome = tokio::time::timeout(
            NOTIFY_TIMEOUT,
            stub.call(caller_context(my_id), TypedValue::default(), call),
        )
        .await;
        if !matches!(outcome, Ok(Ok(_))) {
            broken_arcs.insert(arc);
        }
    }
}

/// `CallerContext` for an outbound identity-manager call. None of this
/// module's three RPC methods dispatch on `source_id`/`src_nic` upstream —
/// `get_peer_main_id` ignores its caller entirely, and `match_duplication`/
/// `notify_identity_arc_removed` resolve identity from explicit message
/// arguments instead (`identities.vala:780-796,820-876`); only the arc is
/// resolved from the caller, via [`IdentityStubFactory::arc_for_caller`] on
/// the *receiving* side. `source_id` is still filled in honestly for wire
/// hygiene; `src_nic` is left unset — this crate has no physical/pseudo-NIC
/// representation to put there.
fn caller_context(local_id: IdentityId) -> CallerContext {
    CallerContext {
        source_id: Some(identity_id_to_typed_value(local_id)),
        src_nic: None,
    }
}

async fn run(mut state: State, mut cmd_rx: mpsc::Receiver<Cmd>, cancel: CancellationToken) {
    let mut background: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            maybe_cmd = cmd_rx.recv() => {
                match maybe_cmd {
                    Some(cmd) => state.handle(cmd, &mut background),
                    None => break,
                }
            }
            Some(_) = background.join_next(), if !background.is_empty() => {}
        }
    }
    while background.join_next().await.is_some() {}
}

/// Cheap-clone handle to a running identity-manager actor — the only way
/// calling code interacts with identity state
/// (`research/notes/06-rust-stack.md` §Concurrency).
#[derive(Clone, Debug)]
pub struct Handle {
    cmd_tx: mpsc::Sender<Cmd>,
    snapshot: watch::Receiver<IdentitySnapshot>,
    events: broadcast::Sender<IdentityEvent>,
}

impl Handle {
    /// Spawns the identity-manager actor, seeded with a freshly generated
    /// main identity (the effect of `IdentityManager`'s constructor,
    /// `identities.vala:76-110`, minus the real-NIC bookkeeping this
    /// crate's non-goals exclude). `initial_naddr` is the main identity's
    /// resolved network position if already known, else `None`.
    ///
    /// Returns the [`Handle`] plus the actor's `JoinHandle`; per
    /// `research/notes/06-rust-stack.md` §Concurrency the caller is
    /// expected to reap it from its own `JoinSet`, cancelling via `cancel`.
    #[must_use]
    pub fn spawn(
        initial_naddr: Option<Naddr>,
        stub_factory: Arc<dyn IdentityStubFactory>,
        cancel: CancellationToken,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let main_id = IdentityId::generate();
        let registry = Registry::new(main_id, initial_naddr);
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (snapshot_tx, snapshot_rx) = watch::channel(registry.snapshot());
        let (events_tx, _) = broadcast::channel(256);
        let state = State {
            registry,
            arcs: HashMap::new(),
            identity_arcs: HashMap::new(),
            pending_migrations: HashMap::new(),
            stub_factory,
            self_tx: cmd_tx.clone(),
            snapshot_tx,
            events_tx: events_tx.clone(),
            cancel: cancel.clone(),
        };
        let join = tokio::spawn(run(state, cmd_rx, cancel));
        (
            Self {
                cmd_tx,
                snapshot: snapshot_rx,
                events: events_tx,
            },
            join,
        )
    }

    /// The current main identity (`get_main_id`, `identities.vala:344-347`).
    #[must_use]
    pub fn main_id(&self) -> IdentityId {
        self.snapshot.borrow().main_id
    }

    /// A consistent point-in-time view of the registry.
    #[must_use]
    pub fn snapshot(&self) -> IdentitySnapshot {
        self.snapshot.borrow().clone()
    }

    /// A live handle to the registry snapshot stream.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<IdentitySnapshot> {
        self.snapshot.clone()
    }

    /// Subscribes to this actor's event stream (`identities.vala:771-775`'s
    /// signals, as a stream).
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<IdentityEvent> {
        self.events.subscribe()
    }

    async fn call<T>(&self, build: impl FnOnce(oneshot::Sender<T>) -> Cmd) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(build(tx))
            .await
            .map_err(|_| Error::ActorGone)?;
        rx.await.map_err(|_| Error::ActorGone)
    }

    /// Registers a newly discovered neighborhood arc and resolves its
    /// peer's main identity (`add_arc`, `identities.vala:274-304`).
    pub async fn add_arc(&self, arc: ArcId, info: ArcInfo) -> Result<(), Error> {
        self.call(|reply| Cmd::AddArc { arc, info, reply }).await?
    }

    /// Tears down `arc` and every identity's identity-arcs over it
    /// (`remove_arc`, `identities.vala:318-339`).
    pub async fn remove_arc(&self, arc: ArcId) -> Result<(), Error> {
        self.call(|reply| Cmd::RemoveArc { arc, reply }).await?
    }

    /// Phase 1 of a migration: registers the pending duplication and starts
    /// its 600s cleanup race (`prepare_add_identity`,
    /// `identities.vala:399-414`).
    pub async fn prepare_migration(
        &self,
        migration_id: MigrationId,
        old_id: IdentityId,
    ) -> Result<(), Error> {
        self.call(|reply| Cmd::PrepareMigration {
            migration_id,
            old_id,
            reply,
        })
        .await?
    }

    /// Phase 2 of a migration: allocates the new identity, hands the
    /// main-identity role across if `old_id` held it, and duplicates every
    /// identity-arc `old_id` has, matching peers via `match_duplication`
    /// (`add_identity`, `identities.vala:441-577`). `devices` is the mac and
    /// linklocal address the caller's `ntk-netlink` step already assigned
    /// the old identity's pseudo-device on each real device it handles,
    /// named per [`crate::pseudo`].
    ///
    /// Resolves only once every arc has been duplicated (or given up on) —
    /// matching upstream's blocking `add_identity` return — while the actor
    /// itself stays responsive to other commands throughout, since the
    /// outbound RPC round trips run in a background task, not inline in the
    /// command loop.
    pub async fn migrate(
        &self,
        migration_id: MigrationId,
        old_id: IdentityId,
        devices: HashMap<String, MigrationDeviceInfo>,
    ) -> Result<IdentityId, Error> {
        self.call(|reply| Cmd::Migrate {
            migration_id,
            old_id,
            devices,
            reply,
        })
        .await?
    }

    /// Decommissions a non-main identity (`remove_identity`,
    /// `identities.vala:685-730`); best-effort peer notification happens in
    /// the background and never affects this call's outcome.
    pub async fn remove_identity(&self, id: IdentityId) -> Result<(), Error> {
        self.call(|reply| Cmd::RemoveIdentity { id, reply }).await?
    }

    /// Reverts an in-flight migration whose successor never finished
    /// hooking: `new_id` — the identity [`Handle::migrate`] returned — is
    /// dismissed, and `old_id` regains whatever status, including the
    /// main-identity role, it held immediately before the fork. Deciding
    /// *when* a successor has given up on hooking belongs to the
    /// composition root's hooking/qspn layers (an explicit non-goal here);
    /// this only makes that decision safe to act on.
    ///
    /// # Errors
    /// [`Error::UnknownIdentity`] if either `old_id` or `new_id` is not
    /// currently registered.
    pub async fn abort_migration(
        &self,
        old_id: IdentityId,
        new_id: IdentityId,
    ) -> Result<(), Error> {
        self.call(|reply| Cmd::AbortMigration {
            old_id,
            new_id,
            reply,
        })
        .await?
    }

    /// Which arcs each currently-registered identity has at least one live
    /// identity-arc on (`identity_arcs`, `identities.vala:129,182-215`) —
    /// e.g. so the composition root can confirm a connectivity fork has no
    /// live identity-arcs left before retiring it via
    /// [`Handle::remove_identity`]. An identity absent from the map has no
    /// live identity-arc on any arc.
    pub async fn arc_ownership(&self) -> Result<BTreeMap<IdentityId, Vec<ArcId>>, Error> {
        self.call(|reply| Cmd::ArcOwnership { reply }).await
    }

    /// Records `id`'s resolved network position (or clears it). This crate
    /// never computes this itself — hooking/qspn are non-goals — callers
    /// (the daemon) set it once hooking resolves a position.
    pub async fn set_naddr(&self, id: IdentityId, naddr: Option<Naddr>) -> Result<(), Error> {
        self.call(|reply| Cmd::SetNaddr { id, naddr, reply })
            .await?
    }

    pub(crate) async fn lookup_pending_migration(
        &self,
        migration_id: MigrationId,
        old_id: IdentityId,
    ) -> Option<PendingLookup> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(Cmd::LookupPendingMigration {
                migration_id,
                old_id,
                reply: tx,
            })
            .await
            .is_err()
        {
            return None;
        }
        rx.await.ok().flatten()
    }

    pub(crate) async fn fetch_duplication_data(
        &self,
        migration_id: MigrationId,
        old_id: IdentityId,
        arc: ArcId,
    ) -> Result<DuplicationData, Error> {
        self.call(|reply| Cmd::FetchDuplicationData {
            migration_id,
            old_id,
            arc,
            reply,
        })
        .await?
    }

    pub(crate) async fn neighbour_migrated(
        &self,
        my_id: IdentityId,
        my_peer_old_id: IdentityId,
        my_peer_new_id: IdentityId,
        my_peer_old_id_new_mac: String,
        my_peer_old_id_new_linklocal: String,
        arc: ArcId,
    ) {
        let _ = self
            .cmd_tx
            .send(Cmd::NeighbourMigrated {
                my_id,
                my_peer_old_id,
                my_peer_new_id,
                my_peer_old_id_new_mac,
                my_peer_old_id_new_linklocal,
                arc,
            })
            .await;
    }

    pub(crate) async fn notify_identity_arc_removed_inbound(
        &self,
        my_id: IdentityId,
        peer_id: IdentityId,
        arc: ArcId,
    ) {
        let _ = self
            .cmd_tx
            .send(Cmd::NotifyIdentityArcRemoved {
                my_id,
                peer_id,
                arc,
            })
            .await;
    }
}
