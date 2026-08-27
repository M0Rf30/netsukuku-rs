//! The `ntkd` node composition: CLI, supervisor, transport wiring, and steady-state loop.

pub mod adapters;
pub mod andna_key;
pub mod cli;
pub mod codec;
pub mod dispatch;
pub mod ip_route;
pub mod kernel_handle;
pub mod lifecycle;
#[cfg(test)]
mod negotiation_tests;
pub mod peers;
pub mod registry;
pub mod services;
pub mod status;
pub mod stubs;
pub mod supervisor;
pub mod transport;

pub fn main() {
    use clap::Parser;
    let cli = cli::Cli::parse();
    let runtime = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
    let result = runtime.block_on(async move {
        match cli.command {
            cli::Command::Run {
                config,
                nics,
                log_level,
                status_socket,
            } => {
                let socket =
                    status_socket.unwrap_or_else(|| std::path::PathBuf::from("/tmp/ntkd.sock"));
                supervisor::run(config, nics, &log_level, socket).await
            }
            cli::Command::Status { socket } => supervisor::status(socket).await,
            cli::Command::AndnaRegister { hostname, socket } => {
                supervisor::andna_register(socket, hostname).await
            }
            cli::Command::AndnaResolve { hostname, socket } => {
                supervisor::andna_resolve(socket, hostname).await
            }
        }
    });
    if let Err(err) = result {
        // Every error type in this workspace already embeds its own cause's `Display` text in
        // its own message (`#[error("context: {0}")]` + `#[from]`, this codebase's established
        // convention — see e.g. `crate::kernel::config::ConfigError`) — so plain `{err}` already
        // shows the full, readable chain once. Anyhow's alternate `{:#}` additionally walks
        // `Error::source()`, which re-prints the same already-embedded text a second time.
        eprintln!("ntkd: error: {err}");
        std::process::exit(1);
    }
}
