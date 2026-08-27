//! [`Hostname`]: a validated, case-folded ANDNA name, and its `blake3` route key.

use std::fmt;

use crate::error::Error;

/// `ANDNA_MAX_HNAME_LEN` (512, null terminator included,
/// `research/impl/c/netsukuku/src/andna_cache.h:34`) minus the terminator this Rust type has no
/// use for.
const MAX_LEN: usize = 511;

/// A flat-namespace, bounded-length, case-insensitive alphanumeric ANDNA name
/// (`research/notes/02-vala-services-daemon.md` §4, citing
/// `documentation/ita/DemoneNTKD/RisoluzioneNomi.md`). Always stored lower-cased: ANDNA has no
/// concept of case, so two names differing only in case are the same name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hostname(String);

impl Hostname {
    /// Validates and case-folds `raw`.
    ///
    /// # Errors
    /// [`Error::EmptyHostname`], [`Error::HostnameTooLong`], or [`Error::InvalidHostnameChar`].
    pub fn new(raw: &str) -> Result<Self, Error> {
        if raw.is_empty() {
            return Err(Error::EmptyHostname);
        }
        if raw.len() > MAX_LEN {
            return Err(Error::HostnameTooLong {
                len: raw.len(),
                max: MAX_LEN,
            });
        }
        if !raw.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::InvalidHostnameChar);
        }
        Ok(Self(raw.to_ascii_lowercase()))
    }

    /// The canonical (lower-cased) name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// This name's `blake3` hash — the input to [`ntk_peerservices::hash_to_tuple`] that places
    /// it on the DHT (RFC 0014 §2, Definition 2.3's `h: KEY -> IP`). `blake3` replaces upstream's
    /// legacy MD5+FNV1_32 (`andna_hash`, `research/impl/c/netsukuku/src/andna.c:309-328`) per
    /// this crate's assignment ("blake3 for name hashing").
    #[must_use]
    pub fn hash(&self) -> HostnameHash {
        HostnameHash(*blake3::hash(self.0.as_bytes()).as_bytes())
    }
}

impl fmt::Display for Hostname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A [`Hostname`]'s `blake3` hash, both as opaque bytes (for the Counter service's per-registrant
/// reservation set) and as a [`ntk_peerservices`] DHT route key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostnameHash([u8; 32]);

impl HostnameHash {
    /// Wraps a raw 32-byte hash (e.g. one decoded off the wire).
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw hash bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Reduces this hash to the `u128` [`ntk_peerservices::hash_to_tuple`] expects — its low 16
    /// bytes, exactly the "low 128 bits of an MD5/SHA output" example that function's own doc
    /// comment names.
    #[must_use]
    pub fn route_key(&self) -> u128 {
        let mut low16 = [0u8; 16];
        low16.copy_from_slice(&self.0[..16]);
        u128::from_le_bytes(low16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_folds_to_lowercase() {
        assert_eq!(Hostname::new("Angelica").unwrap().as_str(), "angelica");
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(Hostname::new(""), Err(Error::EmptyHostname)));
    }

    #[test]
    fn rejects_non_alnum() {
        assert!(matches!(
            Hostname::new("a-b"),
            Err(Error::InvalidHostnameChar)
        ));
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(MAX_LEN + 1);
        assert!(matches!(
            Hostname::new(&long),
            Err(Error::HostnameTooLong { .. })
        ));
    }

    #[test]
    fn accepts_max_len() {
        let ok = "a".repeat(MAX_LEN);
        assert!(Hostname::new(&ok).is_ok());
    }

    #[test]
    fn hash_is_deterministic_and_case_insensitive() {
        let a = Hostname::new("depausceve").unwrap();
        let b = Hostname::new("DePauSceve").unwrap();
        assert_eq!(a.hash(), b.hash());
    }

    #[test]
    fn different_names_hash_differently() {
        let a = Hostname::new("angelica").unwrap();
        let b = Hostname::new("frenzu").unwrap();
        assert_ne!(a.hash(), b.hash());
    }

    proptest::proptest! {
        #[test]
        fn canonicalization_is_idempotent(s in "[a-zA-Z0-9]{1,64}") {
            let once = Hostname::new(&s).unwrap();
            let twice = Hostname::new(once.as_str()).unwrap();
            proptest::prop_assert_eq!(once, twice);
        }

        #[test]
        fn route_key_is_deterministic(s in "[a-zA-Z0-9]{1,64}") {
            let h = Hostname::new(&s).unwrap();
            proptest::prop_assert_eq!(h.hash().route_key(), h.hash().route_key());
        }
    }
}
