//! The Hooking actor: single-owner protocol state (per-arc phase, hooked
//! status, chosen address) fed by an `mpsc` command queue with `oneshot`
//! replies (`research/notes/06-rust-stack.md` §Concurrency) — the Rust
//! replacement for `HookingManager`'s public method surface
//! (`research/impl/vala/hooking/hooking.vala:60-661`).
//!
//! **Outbound network I/O never runs inside this actor's command loop.**
//! Each arc gets its own independent task ([`crate::arc::run_arc_handler`]),
//! spawned once at `add_arc` and tracked in [`Actor::arc_tasks`] — mirroring
//! upstream's one-tasklet-per-arc model
//! (`research/impl/vala/hooking/arc_handler.vala:62-71`). That task
//! sequentially awaits its own outbound RPC/Coordinator calls; it reports
//! phase transitions back to this actor as fire-and-forget [`Command`]s,
//! which this actor applies as pure, non-blocking state mutations. The
//! command loop itself therefore never awaits a peer — exactly the
//! discipline `ntk-qspn` adopted after a real deadlock was found and fixed
//! that way (see this crate's parent task notes).
//!
//! Inbound `search_migration_path` handling
//! ([`crate::search::find_shortest_mig`], dispatched from
//! [`crate::rpc::HookingRpcHandler`]) similarly never touches this actor:
//! it is pure computation over [`crate::view::QspnView`] plus outbound
//! [`crate::coordinator::CoordinatorClient`]/[`crate::stub::HookingStubFactory`]
//! calls, so it simply runs on the RPC server's own per-call task.

use std::collections::HashMap;
use std::sync::Arc;

use ntk_common::Naddr;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::arc::{ArcHandlerCtx, ArcId, run_arc_handler};
use crate::config::HookingConfig;
use crate::coordinator::CoordinatorClient;
use crate::domain::{EntryData, FinishEnterData, FinishMigrationData};
use crate::error::HookingError;
use crate::events::HookingEvent;
use crate::snapshot::{ArcPhase, ChosenAddress, HookingSnapshot};
use crate::stub::HookingStubFactory;
use crate::view::QspnView;

/// Whether this identity is the root of a brand-new network or is trying to
/// join an existing one. Upstream ties "am I hooked" to QSPN's own
/// bootstrap-complete state (`create_net` is always immediately
/// bootstrap-complete, `qspn.vala:206-219`); this crate surfaces the same
/// distinction directly on its own snapshot instead, since Hooking has no
/// dependency on `ntk-qspn` to ask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HookingOrigin {
    /// This identity is the root of a brand-new network — immediately
    /// hooked, at the trivial all-zero position.
    CreateNet,
    /// This identity has arcs to discover and merge with; starts unhooked.
    Joining,
}

enum Command {
    AddArc {
        arc: ArcId,
        reply: oneshot::Sender<Result<(), HookingError>>,
    },
    RemoveArc {
        arc: ArcId,
        reply: oneshot::Sender<Result<(), HookingError>>,
    },
    SetArcPhase {
        arc: ArcId,
        phase: ArcPhase,
    },
    MarkEntered {
        arc: ArcId,
        ask_lvl: usize,
        entry_data: EntryData,
    },
    /// See [`HookingHandle::try_begin_commit`]'s doc.
    TryBeginCommit {
        reply: oneshot::Sender<bool>,
    },
    /// See [`HookingHandle::end_commit`]'s doc.
    EndCommit,
}

/// Cheap-clone handle to a running Hooking actor — the only way to interact
/// with it. Mirrors `HookingManager`'s public method surface.
#[derive(Clone)]
pub struct HookingHandle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    snapshot_rx: watch::Receiver<HookingSnapshot>,
    events_tx: broadcast::Sender<HookingEvent>,
}

impl std::fmt::Debug for HookingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookingHandle").finish_non_exhaustive()
    }
}

async fn call<T>(
    tx: &mpsc::UnboundedSender<Command>,
    build: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> Result<T, HookingError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(build(reply_tx))
        .map_err(|_| HookingError::ActorGone)?;
    reply_rx.await.map_err(|_| HookingError::ActorGone)
}

impl HookingHandle {
    /// The current state snapshot (`arcs`/`hooked`/`chosen`).
    #[must_use]
    pub fn snapshot(&self) -> HookingSnapshot {
        self.snapshot_rx.borrow().clone()
    }

    /// A live handle onto the snapshot channel, for a consumer that wants
    /// to `wait_for`/`changed()` on updates rather than polling
    /// [`Self::snapshot`].
    #[must_use]
    pub fn watch_snapshot(&self) -> watch::Receiver<HookingSnapshot> {
        self.snapshot_rx.clone()
    }

    /// Subscribes to this identity's event stream (`HookingManager`'s
    /// GObject signals, `hooking.vala:112-122`, as a `broadcast` stream —
    /// see [`HookingEvent`]).
    #[must_use]
    pub fn subscribe_events(&self) -> broadcast::Receiver<HookingEvent> {
        self.events_tx.subscribe()
    }

    /// `ArcHandler.add_arc` (`arc_handler.vala:62-71`): registers a new
    /// identity-arc and spawns its handler task.
    ///
    /// # Errors
    /// [`HookingError::ArcAlreadyRegistered`] if `arc` is already tracked;
    /// [`HookingError::ActorGone`] if the actor task has stopped.
    pub async fn add_arc(&self, arc: ArcId) -> Result<(), HookingError> {
        call(&self.cmd_tx, |reply| Command::AddArc { arc, reply }).await?
    }

    /// `ArcHandler.remove_arc` (`arc_handler.vala:361-367`): cancels and
    /// unregisters an identity-arc's handler task.
    ///
    /// # Errors
    /// [`HookingError::UnknownArc`] if `arc` is not tracked;
    /// [`HookingError::ActorGone`].
    pub async fn remove_arc(&self, arc: ArcId) -> Result<(), HookingError> {
        call(&self.cmd_tx, |reply| Command::RemoveArc { arc, reply }).await?
    }

    /// `execute_propagate_prepare_enter` -> `mgr.do_prepare_enter`
    /// (`propagation_coord.vala:54-64`, `hooking.vala:115`): the composition
    /// root calls this when the real Coordinator module has propagated a
    /// `prepare_enter` to this identity's g-node. Purely relays the event —
    /// upstream's own signal carries only `enter_id`, no level.
    pub fn notify_prepare_enter(&self, enter_id: i32) {
        let _ = self
            .events_tx
            .send(HookingEvent::DoPrepareEnter { enter_id });
    }

    /// `execute_propagate_finish_enter` -> `mgr.do_finish_enter`
    /// (`propagation_coord.vala:74-86`, `hooking.vala:116-118`).
    pub fn notify_finish_enter(&self, guest_gnode_level: usize, data: FinishEnterData) {
        let _ = self.events_tx.send(HookingEvent::DoFinishEnter {
            guest_gnode_level,
            data,
        });
    }

    /// `execute_propagate_prepare_migration` -> `mgr.do_prepare_migration`
    /// (`propagation_coord.vala:96-106`, `hooking.vala:119`).
    pub fn notify_prepare_migration(&self, migration_id: i32) {
        let _ = self
            .events_tx
            .send(HookingEvent::DoPrepareMigration { migration_id });
    }

    /// `execute_propagate_finish_migration` -> `mgr.do_finish_migration`
    /// (`propagation_coord.vala:116-128`, `hooking.vala:120-122`).
    pub fn notify_finish_migration(&self, guest_gnode_level: usize, data: FinishMigrationData) {
        let _ = self.events_tx.send(HookingEvent::DoFinishMigration {
            guest_gnode_level,
            data,
        });
    }

    // -- crate-internal: used by the per-arc handler task --

    pub(crate) fn emit(&self, event: HookingEvent) {
        let _ = self.events_tx.send(event);
    }

    pub(crate) fn set_arc_phase(&self, arc: ArcId, phase: ArcPhase) {
        let _ = self.cmd_tx.send(Command::SetArcPhase { arc, phase });
    }

    pub(crate) fn mark_entered(&self, arc: ArcId, ask_lvl: usize, entry_data: EntryData) {
        let _ = self.cmd_tx.send(Command::MarkEntered {
            arc,
            ask_lvl,
            entry_data,
        });
    }

    /// Serializes this identity's own "network-wide evaluation" phase
    /// (`arc_handler.vala:216-357`) across its own concurrently-negotiating arcs: grants at
    /// most one arc at a time the right to proceed past the merge decision into
    /// `evaluate_enter`/.../`finish_enter`. Exclusive only for the duration of one migration
    /// episode (`self.committing`, held from a granted call here until the matching
    /// [`Self::end_commit`]) — NOT a permanent one-shot latch. An identity that has already
    /// completed a migration must still be able to follow its own g-node into a *later*,
    /// separate merge; the daemon's own `SteadyStateCtx` dropped its equivalent one-shot
    /// `rehooked` latch for exactly this reason, replacing it with `migration_in_progress` plus
    /// a `migrations` counter. Without this exclusion, two of this identity's own arcs
    /// independently discovering "another network" against two *different* foreign networks (a
    /// real scenario, not hypothetical — see `crate::arc`'s module doc) could both reach
    /// `finish_enter` concurrently: whichever wins is arbitrary, and the loser's own commit
    /// races an already-in-flight one it has no way to know about.
    pub(crate) async fn try_begin_commit(&self) -> bool {
        call(&self.cmd_tx, |reply| Command::TryBeginCommit { reply })
            .await
            .unwrap_or(false)
    }

    /// Releases the exclusive slot [`Self::try_begin_commit`] granted — called on every exit
    /// from the commit phase, success or failure alike, by `crate::arc::CommitGuard`'s `Drop`.
    pub(crate) fn end_commit(&self) {
        let _ = self.cmd_tx.send(Command::EndCommit);
    }
}

struct Actor {
    view: Arc<dyn QspnView>,
    coord: Arc<dyn CoordinatorClient>,
    stubs: Arc<dyn HookingStubFactory>,
    config: HookingConfig,
    handle: HookingHandle,
    cancel: CancellationToken,
    snapshot: HookingSnapshot,
    snapshot_tx: watch::Sender<HookingSnapshot>,
    arc_cancels: HashMap<ArcId, CancellationToken>,
    arc_tasks: JoinSet<ArcId>,
    /// See [`HookingHandle::try_begin_commit`]'s doc: exclusive for one migration episode only,
    /// released by the matching [`HookingHandle::end_commit`] — not a permanent latch.
    committing: bool,
}

impl Actor {
    async fn run(mut self, mut cmd_rx: mpsc::UnboundedReceiver<Command>) {
        loop {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle(cmd),
                        None => break,
                    }
                }
                Some(joined) = self.arc_tasks.join_next(), if !self.arc_tasks.is_empty() => {
                    match joined {
                        Ok(arc) => { self.arc_cancels.remove(&arc); }
                        Err(e) => warn!(error = %e, "hooking arc handler task panicked"),
                    }
                }
            }
        }
        for token in self.arc_cancels.values() {
            token.cancel();
        }
        self.arc_tasks.shutdown().await;
    }

    fn publish(&self) {
        let _ = self.snapshot_tx.send(self.snapshot.clone());
    }

    fn handle(&mut self, cmd: Command) {
        match cmd {
            Command::AddArc { arc, reply } => {
                let result = self.handle_add_arc(arc);
                let _ = reply.send(result);
            }
            Command::RemoveArc { arc, reply } => {
                let result = self.handle_remove_arc(arc);
                let _ = reply.send(result);
            }
            Command::SetArcPhase { arc, phase } => {
                self.snapshot.arcs.insert(arc, phase);
                self.publish();
            }
            Command::MarkEntered {
                arc,
                ask_lvl,
                entry_data,
            } => {
                self.snapshot
                    .arcs
                    .insert(arc, ArcPhase::Entered { ask_lvl });
                if !self.snapshot.hooked {
                    self.snapshot.hooked = true;
                    let naddr = build_naddr(self.view.topology(), &entry_data);
                    self.snapshot.chosen = Some(ChosenAddress { entry_data, naddr });
                }
                self.publish();
            }
            Command::TryBeginCommit { reply } => {
                let granted = !self.committing;
                if granted {
                    self.committing = true;
                }
                let _ = reply.send(granted);
            }
            Command::EndCommit => {
                self.committing = false;
            }
        }
    }

    fn handle_add_arc(&mut self, arc: ArcId) -> Result<(), HookingError> {
        if self.arc_cancels.contains_key(&arc) {
            return Err(HookingError::ArcAlreadyRegistered);
        }
        let cancel = self.cancel.child_token();
        let ctx = ArcHandlerCtx {
            view: self.view.clone(),
            coord: self.coord.clone(),
            stubs: self.stubs.clone(),
            config: self.config.clone(),
            handle: self.handle.clone(),
        };
        let task_cancel = cancel.clone();
        self.arc_tasks.spawn(async move {
            run_arc_handler(ctx, arc, task_cancel).await;
            arc
        });
        self.arc_cancels.insert(arc, cancel);
        self.snapshot.arcs.insert(arc, ArcPhase::Discovering);
        self.publish();
        Ok(())
    }

    fn handle_remove_arc(&mut self, arc: ArcId) -> Result<(), HookingError> {
        let Some(cancel) = self.arc_cancels.remove(&arc) else {
            return Err(HookingError::UnknownArc);
        };
        cancel.cancel();
        self.snapshot.arcs.remove(&arc);
        self.publish();
        Ok(())
    }
}

/// Materializes a full [`Naddr`] from `entry_data.pos` when it happens to
/// already span every topology level (see [`ChosenAddress`]'s docs).
fn build_naddr(topology: &ntk_common::Topology, entry_data: &EntryData) -> Option<Naddr> {
    if entry_data.pos.len() != topology.levels() {
        return None;
    }
    Naddr::new(topology.clone(), entry_data.pos.iter().copied()).ok()
}

/// Spawns the Hooking actor. Returns a cheap-clone [`HookingHandle`] and the
/// actor's own `JoinHandle`; the caller's `JoinSet` should reap the latter
/// (`research/notes/06-rust-stack.md` §Concurrency). `cancel` is this
/// actor's own child token — dropping the handle does not stop the actor;
/// cancelling `cancel` does (and, transitively, every per-arc child task).
#[must_use]
pub fn spawn(
    origin: HookingOrigin,
    view: Arc<dyn QspnView>,
    coord: Arc<dyn CoordinatorClient>,
    stubs: Arc<dyn HookingStubFactory>,
    config: HookingConfig,
    cancel: CancellationToken,
) -> (HookingHandle, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (events_tx, _) = broadcast::channel(256);

    let mut initial = HookingSnapshot::default();
    if origin == HookingOrigin::CreateNet {
        let topology = view.topology().clone();
        let levels = topology.levels();
        let entry_data = EntryData {
            network_id: view.network_id(),
            pos: vec![0; levels],
            elderships: vec![0; levels],
        };
        let naddr = build_naddr(&topology, &entry_data);
        initial.hooked = true;
        initial.chosen = Some(ChosenAddress { entry_data, naddr });
    }
    let (snapshot_tx, snapshot_rx) = watch::channel(initial.clone());

    let handle = HookingHandle {
        cmd_tx: cmd_tx.clone(),
        snapshot_rx,
        events_tx: events_tx.clone(),
    };

    let actor = Actor {
        view,
        coord,
        stubs,
        config,
        handle: handle.clone(),
        cancel: cancel.clone(),
        snapshot: initial,
        snapshot_tx,
        arc_cancels: HashMap::new(),
        arc_tasks: JoinSet::new(),
        committing: false,
    };
    let join = tokio::spawn(actor.run(cmd_rx));

    (handle, join)
}
