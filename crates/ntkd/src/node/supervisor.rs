// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Production entry point: CLI dispatch, the root [`tokio::task::JoinSet`]/[`CancellationToken`],
//! graceful shutdown on SIGINT/SIGTERM, and the final [`ntk_netlink::cleanup`] safety net plus
//! its [`crate::node::ip_route::cleanup_neighbor_routes`] counterpart (see that function's doc
//! for why the neighbor on-link route table needs one of its own).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ntk_netlink::TableAllocator;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::kernel::config::NtkdConfig;
use crate::node::{status, transport};

/// Budget for [`drain_tasks`]'s cooperative wait during shutdown: one
/// `HookingConfig::default()` restart-from-start backoff floor (20s, `crate::node::services`'s
/// own doc) plus a flat 10s scheduling margin — long enough for every actor's own retry/backoff
/// cycle to notice `root_cancel` between attempts, not so long that a genuinely wedged actor
/// (see [`drain_tasks`]'s own doc) turns a shutdown request into a multi-minute hang.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Extra window [`drain_tasks`] gives already-aborted tasks to actually finish: real, not
/// pathological, reasons exist for a task to still be running right after
/// [`JoinSet::abort_all`] (it genuinely has a live `.await` suspension point and simply hasn't
/// been repolled yet) — worth a brief further wait rather than reporting it outstanding
/// needlessly. A task that never yields at all (this function's own doc) cannot be recovered by
/// any amount of waiting, so this stays short: [`drain_tasks`]'s own worst-case bound is
/// `timeout + ABORT_REAP_WINDOW`, never unbounded.
const ABORT_REAP_WINDOW: Duration = Duration::from_secs(2);

/// Drains `tasks`, bounded by `timeout`: every spawned actor is expected to observe
/// cancellation and exit promptly once its own [`CancellationToken`] fires, but a task that
/// never yields control back to its runtime at all — a synchronous retry loop with no real
/// `.await` suspension point, not merely a slow one — can never be *cooperatively* cancelled;
/// no amount of extra waiting recovers it. Confirmed live, not hypothetical: a real-kernel
/// capture of `crates/ntkd/tests/mesh.rs`'s
/// `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` caught exactly this via `gdb` —
/// `ntk_peerservices::routing::Handle::relay`'s own gateway retry loop (`routing.rs`), paired
/// with `crate::node::adapters::RoutingEnvAdapter::gateway` ignoring its `failed` exclusion
/// parameter and always re-resolving the identical (broken) candidate, spinning with zero
/// backoff and zero syscalls, permanently starving that identity's single-threaded runtime.
///
/// Past `timeout` this stops waiting cooperatively, calls [`JoinSet::abort_all`] (best-effort:
/// it only takes effect the next time an aborted task's own poll actually yields, which the
/// pathological case above never does), then gives the `JoinSet` `ABORT_REAP_WINDOW` to reap
/// whatever actually responds, and returns — reporting how many tasks never joined rather than
/// hanging indefinitely. Every 5s of cooperative waiting also logs the still-outstanding count,
/// so a merely slow (not wedged) shutdown is observable while in progress, not only diagnosed
/// after the fact.
///
/// Returns the number of tasks that never joined (0 on a clean drain).
pub async fn drain_tasks(tasks: &mut JoinSet<()>, timeout: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let tick = (deadline - tokio::time::Instant::now()).min(Duration::from_secs(5));
        tokio::select! {
            joined = tasks.join_next() => match joined {
                None => return 0,
                Some(result) => {
                    log_join_result(result);
                }
            },
            () = tokio::time::sleep(tick) => {
                tracing::warn!(remaining = tasks.len(), "shutdown: still draining tasks");
            }
        }
    }
    if tasks.is_empty() {
        return 0;
    }
    tracing::error!(
        outstanding = tasks.len(),
        ?timeout,
        "shutdown: drain timed out with tasks still running; aborting the rest"
    );
    tasks.abort_all();
    let _ = tokio::time::timeout(ABORT_REAP_WINDOW, async {
        while let Some(result) = tasks.join_next().await {
            log_join_result(result);
        }
    })
    .await;
    tasks.len()
}

/// Logs a panicked task loudly; a cancelled (aborted) task is expected during shutdown.
/// Returns `true` when the task panicked.
fn log_join_result(result: Result<(), tokio::task::JoinError>) -> bool {
    match result {
        Ok(()) => false,
        Err(err) if err.is_panic() => {
            tracing::error!(%err, "a daemon task panicked");
            true
        }
        Err(_) => false,
    }
}

/// Removes every piece of kernel state this daemon (or a crashed predecessor) could have left:
/// the generic [`ntk_netlink::cleanup`] sweep plus the neighbor on-link route table.
/// Best-effort and idempotent, so it runs both before the first install (a crash-restart would
/// otherwise hit `EEXIST` on the fixed identity rule/address) and on every exit path.
async fn cleanup_kernel(kernel: &ntk_netlink::RealNetlink, nics: &[String], when: &str) {
    let table_allocator: TableAllocator<()> = TableAllocator::new();
    match ntk_netlink::cleanup(
        kernel,
        &table_allocator,
        &nics
            .iter()
            .map(ntk_netlink::Interface::name)
            .collect::<Vec<_>>(),
    )
    .await
    {
        Ok(report) if !report.is_empty() => {
            tracing::info!(?report, when, "cleanup removed leftover kernel state");
        }
        Ok(_) => {}
        Err(err) => tracing::warn!(%err, when, "kernel cleanup failed"),
    }
    if let Err(err) = crate::node::ip_route::cleanup_neighbor_routes(kernel).await {
        tracing::warn!(%err, when, "neighbor on-link route cleanup failed");
    }
}

/// Entry point for `ntkd run`.
///
/// # Errors
/// Config load/parse failure, any transport bind failure, or a startup error from
/// [`crate::node::lifecycle::run`].
pub async fn run(
    config_path: PathBuf,
    cli_nics: Vec<String>,
    log_level: &str,
    status_socket: PathBuf,
) -> anyhow::Result<()> {
    init_tracing(log_level);

    let config = NtkdConfig::load(&config_path)?;
    let andna_key_path = config.andna_key_path().map(std::path::Path::to_path_buf);
    let nics = if cli_nics.is_empty() {
        config.nics().to_vec()
    } else {
        cli_nics
    };

    let root_cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Startup sweep: a crash (or SIGKILL) skips the shutdown cleanup below, and the fixed
    // identity rule/address would then fail `install_identity` with EEXIST on every restart.
    let startup_kernel = ntk_netlink::RealNetlink::new()?;
    cleanup_kernel(&startup_kernel, &nics, "startup").await;

    let started = match transport::start(config, &nics, &mut tasks, root_cancel.child_token()).await
    {
        Ok(started) => started,
        Err(err) => {
            // `transport::start` may have installed kernel state (identity address, rules)
            // before failing; leaving it would wedge the next start.
            root_cancel.cancel();
            let outstanding = drain_tasks(&mut tasks, SHUTDOWN_DRAIN_TIMEOUT).await;
            if outstanding > 0 {
                tracing::error!(outstanding, "startup failure: some actors never stopped");
            }
            cleanup_kernel(&startup_kernel, &nics, "startup failure").await;
            return Err(err);
        }
    };
    let running = Arc::new(started.running);
    let net = running.net.clone();
    let kernel = running.kernel.clone();
    let running_for_status = running.clone();

    let status_cancel = root_cancel.child_token();
    tasks.spawn(async move {
        if let Err(err) = status::serve(
            status_socket,
            running_for_status,
            net,
            andna_key_path,
            status_cancel,
        )
        .await
        {
            tracing::warn!(%err, "status server exited");
        }
    });

    let mut failure: Option<anyhow::Error> = None;
    {
        let signal = wait_for_shutdown_signal();
        tokio::pin!(signal);
        loop {
            tokio::select! {
                biased;
                () = &mut signal => {
                    tracing::info!("shutdown signal received, cancelling every actor");
                    break;
                }
                joined = tasks.join_next() => match joined {
                    // Every actor is expected to run until `root_cancel` fires, so any
                    // completion now is abnormal; a panic leaves a live-looking daemon that no
                    // longer routes, so it is fatal.
                    Some(result) => {
                        if log_join_result(result) {
                            failure = Some(anyhow::anyhow!("a daemon task panicked; shutting down"));
                            break;
                        }
                        tracing::warn!("a daemon task exited before shutdown");
                    }
                    None => {
                        failure = Some(anyhow::anyhow!("every daemon task exited; shutting down"));
                        break;
                    }
                },
            }
        }
    }

    // Tell the neighbours before the actors die — upstream's `destroy`
    // (`research/impl/vala/qspn/qspn.vala:2481-2505`). Each peer removes the arc to this node and
    // lets implicit withdrawal retract whatever was only reachable through it, so the network
    // reconverges now instead of waiting out every peer's ~28-30s liveness probe.
    //
    // Here rather than in `migrate`: this is the one case where the identity really is going
    // away. A migration reuses the same physical arcs, so announcing there tells peers to drop
    // the arcs the successor still needs — see the long comment at the `destroy` site in
    // `crate::node::lifecycle`'s `migrate` for the failure that proved it.
    //
    // Before `root_cancel`, because `announce_destroy` awaits the broadcast and the actor is
    // gone once cancelled. Best-effort: a peer that cannot be reached is one already reaping the
    // arc itself, and shutdown must not hang on it.
    let shutdown_qspn = running.generation.borrow().qspn.clone();
    if let Err(err) = shutdown_qspn.announce_destroy().await {
        tracing::warn!(%err, "qspn: announcing this node's departure failed");
    }

    root_cancel.cancel();
    let outstanding = drain_tasks(&mut tasks, SHUTDOWN_DRAIN_TIMEOUT).await;
    if outstanding > 0 {
        tracing::error!(
            outstanding,
            "graceful shutdown incomplete: some actors never stopped"
        );
    }

    // Graceful per-identity teardown first — removes exactly what this run installed — then the
    // OS-level crash-recovery sweep as a final safety net for anything it missed.
    if let Err(err) = running.route_installer.lock().await.teardown().await {
        tracing::warn!(%err, "graceful route teardown failed");
    }
    cleanup_kernel(kernel.as_ref(), &nics, "shutdown").await;

    failure.map_or(Ok(()), Err)
}

/// Entry point for `ntkd status`.
///
/// # Errors
/// Any I/O failure connecting to `socket`.
pub async fn status(socket: PathBuf) -> anyhow::Result<()> {
    let report = status::query(&socket).await?;
    println!("main identity:            {}", report.main_identity);
    println!("identities:               {}", report.identity_count);
    println!("hooked:                   {}", report.hooked);
    println!("bootstrapped:             {}", report.bootstrapped);
    println!("route table:              {}", report.route_table);
    println!("route destinations:       {}", report.route_destinations);
    println!("known links:              {}", report.known_links);
    println!("peer participants:        {}", report.peer_participants);
    println!(
        "coordinator reservations: {}",
        report.coordinator_reservations
    );
    println!(
        "andna hosted hostnames:   {}",
        report.andna_hosted_hostnames
    );
    println!("arcs:");
    for arc in &report.arcs {
        println!(
            "  {} on {} — {} (cost: {}, qspn: {}, hooking: {})",
            arc.mac,
            arc.dev,
            arc.state,
            arc.cost.as_deref().unwrap_or("unknown"),
            if arc.qspn_linked { "linked" } else { "pending" },
            arc.hooking_phase.as_deref().unwrap_or("unknown"),
        );
    }
    Ok(())
}

/// Entry point for `ntkd andna-register`.
///
/// # Errors
/// Any I/O failure connecting to `socket`, or the daemon's own reported error (an invalid
/// hostname, an unconfigured/invalid ANDNA key, or a registration failure).
pub async fn andna_register(socket: PathBuf, hostname: String) -> anyhow::Result<()> {
    let reply = status::register_hostname(&socket, &hostname).await?;
    println!("hostname:     {}", reply.hostname);
    println!("outcome:      {}", reply.outcome);
    match reply.expires_unix {
        Some(t) => println!("expires_unix: {t}"),
        None => println!("expires_unix: (none)"),
    }
    Ok(())
}

/// Entry point for `ntkd andna-resolve`.
///
/// # Errors
/// Any I/O failure connecting to `socket`, or the daemon's own reported error (an invalid
/// hostname or a resolution failure).
pub async fn andna_resolve(socket: PathBuf, hostname: String) -> anyhow::Result<()> {
    let reply = status::resolve_hostname(&socket, &hostname).await?;
    println!("hostname: {}", reply.hostname);
    println!("addresses:");
    for entry in &reply.addresses {
        match entry.ipv4 {
            Some(ip) => println!("  {ip}  ({})", entry.target),
            None => println!("  {}", entry.target),
        }
    }
    match reply.ttl_secs {
        Some(t) => println!("ttl_secs: {t}"),
        None => println!("ttl_secs: (none)"),
    }
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn init_tracing(log_level: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[cfg(test)]
mod drain_tasks_tests {
    use super::drain_tasks;
    use std::time::Duration;
    use tokio::task::JoinSet;

    /// The happy path: every task actually observes cancellation (here, simply finishes on its
    /// own) well inside the budget, so `drain_tasks` returns `0` outstanding without ever
    /// reaching its own timeout/abort fallback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drains_cleanly_when_every_task_finishes() {
        let mut tasks = JoinSet::new();
        for _ in 0..4 {
            tasks.spawn(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
            });
        }
        let start = tokio::time::Instant::now();
        let outstanding = drain_tasks(&mut tasks, Duration::from_secs(5)).await;
        assert_eq!(outstanding, 0);
        assert!(tasks.is_empty());
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "a clean drain must not wait anywhere near its own timeout, took {:?}",
            start.elapsed()
        );
    }

    /// Pins this module's own doc: a task with no real `.await` suspension point at all —
    /// exactly the shape a real-kernel capture of `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit`
    /// found in `ntk_peerservices::routing::Handle::relay` (`drain_tasks`'s own doc) — can never
    /// be cooperatively cancelled. `drain_tasks`'s own worst-case bound is
    /// `timeout + ABORT_REAP_WINDOW` (its own doc) regardless of how long the wedged task
    /// actually runs — so the spinning task here is wall-clock-bounded to 5s, comfortably past
    /// that bound (200ms + 2s), so it cannot possibly join before `drain_tasks` gives up on it.
    /// The 5s cap itself is purely so this *test process*'s own runtime shutdown (which still
    /// waits for every worker thread to go idle — `drain_tasks`'s abort-and-move-on strategy has
    /// no bearing on that) can eventually complete; a truly-infinite spin would hang the test
    /// binary itself, not just prove the point already established below.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_promptly_when_a_task_never_yields() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            let stop_at = tokio::time::Instant::now() + Duration::from_secs(5);
            while tokio::time::Instant::now() < stop_at {
                std::hint::spin_loop();
            }
        });
        let start = tokio::time::Instant::now();
        let outstanding = drain_tasks(&mut tasks, Duration::from_millis(200)).await;
        assert_eq!(
            outstanding, 1,
            "the never-yielding task must be reported as outstanding"
        );
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "drain_tasks must bound its wait to timeout + ABORT_REAP_WINDOW even when a task \
             never yields, took {:?}",
            start.elapsed()
        );
    }
}

#[cfg(test)]
mod panic_supervision_tests {
    use super::{drain_tasks, log_join_result};
    use std::time::Duration;
    use tokio::task::JoinSet;

    /// A panicking actor must be reported as a panic (so the supervisor shuts the daemon down
    /// instead of leaving a live-looking process), while a clean exit and an abort are not.
    #[tokio::test]
    async fn a_panicked_task_is_reported_as_a_panic_but_clean_exits_and_aborts_are_not() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async { panic!("actor blew up") });
        let panicked = tasks.join_next().await.expect("one task");
        assert!(log_join_result(panicked), "a panic must be flagged");

        tasks.spawn(async {});
        let clean = tasks.join_next().await.expect("one task");
        assert!(!log_join_result(clean), "a clean exit is not a panic");

        let handle = tasks.spawn(std::future::pending::<()>());
        handle.abort();
        let aborted = tasks.join_next().await.expect("one task");
        assert!(!log_join_result(aborted), "an abort is not a panic");
    }

    /// Draining must keep going past a panicked task rather than bailing out on it.
    #[tokio::test]
    async fn draining_continues_past_a_panicked_task() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async { panic!("actor blew up") });
        tasks.spawn(async { tokio::time::sleep(Duration::from_millis(10)).await });
        assert_eq!(drain_tasks(&mut tasks, Duration::from_secs(5)).await, 0);
        assert!(tasks.is_empty());
    }
}
