// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

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
                let socket = status_socket.unwrap_or_else(status::default_socket_path);
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
        eprintln!("ntkd: error: {}", render_error_chain(&err));
        std::process::exit(1);
    }
}

/// Renders `err` and its whole cause chain on one line (`outer: cause: root`).
///
/// Plain `{err}` prints only the outermost layer, which for an `anyhow::Context` wrapper is a
/// bare "connecting to the socket /run/ntkd.sock" with the actual `ENOENT`/`EACCES` dropped;
/// anyhow's `{err:#}` prints every layer, but several error types in this workspace
/// (`ConfigError`'s `#[error("...: {source}")]`) already embed their cause's text *and* expose
/// it through `source()`, so `{err:#}` would print that text twice. A cause whose text the
/// message so far already contains is therefore skipped.
pub(crate) fn render_error_chain(err: &anyhow::Error) -> String {
    let mut rendered = err.to_string();
    for cause in err.chain().skip(1) {
        let text = cause.to_string();
        if !rendered.contains(&text) {
            rendered.push_str(": ");
            rendered.push_str(&text);
        }
    }
    rendered
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::render_error_chain;

    #[test]
    fn error_chain_shows_the_io_cause_under_a_context_message() {
        let err = std::fs::metadata("/nonexistent/ntkd.sock")
            .context("connecting to the ntkd status socket /nonexistent/ntkd.sock")
            .unwrap_err();
        let rendered = render_error_chain(&err);
        assert!(
            rendered.starts_with("connecting to the ntkd status socket /nonexistent/ntkd.sock: "),
            "{rendered}"
        );
        assert!(rendered.contains("No such file or directory"), "{rendered}");
    }

    #[test]
    fn error_chain_does_not_repeat_a_cause_the_message_already_embeds() {
        let missing =
            crate::kernel::config::NtkdConfig::load(std::path::Path::new("/nonexistent/ntkd.toml"))
                .unwrap_err();
        let err = anyhow::Error::from(missing);
        let rendered = render_error_chain(&err);
        assert_eq!(
            rendered.matches("No such file or directory").count(),
            1,
            "{rendered}"
        );
        assert!(rendered.contains("/nonexistent/ntkd.toml"), "{rendered}");
    }
}
