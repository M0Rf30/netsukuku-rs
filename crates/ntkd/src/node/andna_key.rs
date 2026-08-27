//! Persistence for the daemon's ANDNA ed25519 signing key (RFC 0014 registrant identity).
//!
//! ANDNA has TTL/renewal semantics, so a registered hostname must survive a daemon restart —
//! the key therefore lives on disk at a caller-chosen path, not generated fresh per process.
//! [`load_or_generate`] never accepts a world-readable key file, and this module never logs the
//! seed or the resulting [`SigningKey`].

use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use rand::RngExt;

/// Permission bits [`load_or_generate`] refuses to exceed: owner read/write only.
const MAX_MODE: u32 = 0o600;

/// Everything that can go wrong loading or generating the ANDNA signing key.
#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    /// The key file exists but its permissions are wider than `MAX_MODE`.
    #[error("key file {path} has permissions {mode:o}, wider than the required 0600")]
    TooPermissive { path: PathBuf, mode: u32 },
    /// The key file's contents are not exactly 32 bytes.
    #[error("key file {path} holds {len} bytes, expected a 32-byte seed")]
    WrongLength { path: PathBuf, len: usize },
    /// Reading the existing key file, or its metadata, failed.
    #[error("failed to read key file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Writing a freshly generated key file failed.
    #[error("failed to write key file {path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Loads the 32-byte ed25519 seed at `path`, generating and persisting (mode `0600`) a fresh
/// one if `path` doesn't exist yet. Idempotent: a second call against the same `path` returns
/// the identical key. Refuses, without touching the file, a key file whose current on-disk
/// permissions are wider than `0600`.
///
/// # Errors
/// See [`KeyError`]'s variants.
pub fn load_or_generate(path: &Path) -> Result<SigningKey, KeyError> {
    match std::fs::metadata(path) {
        Ok(meta) => read_existing(path, meta.permissions().mode()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => generate_new(path),
        Err(source) => Err(KeyError::Read {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn read_existing(path: &Path, mode: u32) -> Result<SigningKey, KeyError> {
    let mode = mode & 0o777;
    if mode & !MAX_MODE != 0 {
        return Err(KeyError::TooPermissive {
            path: path.to_path_buf(),
            mode,
        });
    }
    let bytes = std::fs::read(path).map_err(|source| KeyError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let seed: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| KeyError::WrongLength {
            path: path.to_path_buf(),
            len: bytes.len(),
        })?;
    Ok(SigningKey::from_bytes(&seed))
}

fn generate_new(path: &Path) -> Result<SigningKey, KeyError> {
    use std::io::Write;

    let mut seed = [0u8; 32];
    rand::rng().fill(&mut seed);

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(MAX_MODE)
        .open(path)
        .map_err(|source| KeyError::Write {
            path: path.to_path_buf(),
            source,
        })?;
    // `mode()` above is subject to the process umask; set it explicitly so the on-disk
    // permissions are exactly `0600` regardless.
    file.set_permissions(std::fs::Permissions::from_mode(MAX_MODE))
        .and_then(|()| file.write_all(&seed))
        .map_err(|source| KeyError::Write {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(SigningKey::from_bytes(&seed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_then_loads_the_identical_seed() {
        let dir = tempfile();
        let path = dir.join("andna.key");
        let first = load_or_generate(&path).expect("generates a fresh key");
        let second = load_or_generate(&path).expect("loads the same key back");
        assert_eq!(first.to_bytes(), second.to_bytes());
        cleanup(&dir);
    }

    #[test]
    fn created_file_is_mode_0600() {
        let dir = tempfile();
        let path = dir.join("andna.key");
        load_or_generate(&path).expect("generates a fresh key");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        cleanup(&dir);
    }

    #[test]
    fn refuses_a_world_readable_key_file() {
        let dir = tempfile();
        let path = dir.join("andna.key");
        std::fs::write(&path, [1u8; 32]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = load_or_generate(&path).expect_err("wide permissions must be refused");
        assert!(matches!(err, KeyError::TooPermissive { mode: 0o644, .. }));
        cleanup(&dir);
    }

    #[test]
    fn accepts_an_owner_only_readonly_key_file() {
        let dir = tempfile();
        let path = dir.join("andna.key");
        std::fs::write(&path, [2u8; 32]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
        load_or_generate(&path).expect("0400 is within the 0600 ceiling");
        cleanup(&dir);
    }

    fn tempfile() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ntkd-andna-key-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }
}
