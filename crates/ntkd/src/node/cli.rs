//! `ntkd` command-line surface.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "ntkd", version, about = "The Netsukuku routing daemon")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Runs the daemon in the foreground.
    Run {
        /// Path to the TOML config file (see `kernel::config::NtkdConfig`).
        #[arg(long)]
        config: PathBuf,
        /// Interface to monitor for neighbours; repeatable. Overrides the config file's `nics`
        /// list entirely when given.
        #[arg(long = "nic")]
        nics: Vec<String>,
        /// Log verbosity: `error`, `warn`, `info`, `debug`, or `trace`.
        #[arg(long, default_value = "info")]
        log_level: String,
        /// Unix socket path the `status` subcommand connects to.
        #[arg(long)]
        status_socket: Option<PathBuf>,
    },
    /// Queries a running daemon's status over its unix socket.
    Status {
        /// Unix socket path a running daemon's `run --status-socket` is listening on.
        #[arg(long, default_value = "/tmp/ntkd.sock")]
        socket: PathBuf,
    },
    /// Registers or renews a hostname against a running daemon's ANDNA.
    AndnaRegister {
        /// The hostname to register or renew.
        hostname: String,
        /// Unix socket path a running daemon's `run --status-socket` is listening on.
        #[arg(long, default_value = "/tmp/ntkd.sock")]
        socket: PathBuf,
    },
    /// Resolves a hostname against a running daemon's ANDNA.
    AndnaResolve {
        /// The hostname to resolve.
        hostname: String,
        /// Unix socket path a running daemon's `run --status-socket` is listening on.
        #[arg(long, default_value = "/tmp/ntkd.sock")]
        socket: PathBuf,
    },
}
