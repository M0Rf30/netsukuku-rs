// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Error types for kernel-state manipulation.

use crate::types::Interface;

/// Failure manipulating or querying kernel routing state.
#[derive(Debug, thiserror::Error)]
pub enum NetlinkError {
    /// Opening the netlink socket itself failed (see [`crate::RealNetlink::new`]).
    #[error("failed to open netlink connection: {0}")]
    Connect(#[source] std::io::Error),

    /// The kernel rejected a request with an errno this crate has no dedicated variant for
    /// (`rtnetlink`'s own error). `EEXIST`, `ENOENT`/`ESRCH` and `EPERM`/`EACCES` are mapped to
    /// [`NetlinkError::AlreadyExists`], [`NetlinkError::NotFound`] and
    /// [`NetlinkError::PermissionDenied`] by the `From<rtnetlink::Error>` conversion.
    #[error(transparent)]
    Netlink(rtnetlink::Error),

    /// The kernel refused the request for lack of privilege (`EPERM`/`EACCES`, typically a
    /// missing `CAP_NET_ADMIN`).
    #[error("permission denied by the kernel: {0}")]
    PermissionDenied(String),

    /// `interface` does not exist in the kernel's link table.
    #[error("interface {0:?} not found")]
    InterfaceNotFound(Interface),

    /// An IPv4 prefix length outside `0..=32`.
    #[error("invalid IPv4 prefix length {0} (must be 0..=32)")]
    InvalidPrefixLength(u8),

    /// The caller tried to mutate a kernel-reserved table (`unspec`/`default`/
    /// `main`/`local`) through [`crate::RouteTable`] or [`crate::RuleTable`].
    /// Netsukuku must never touch these (design decision, `research/README.md`
    /// "Netsukuku is an L3 routing protocol, not a TUN overlay").
    #[error(
        "table {0} is a kernel-reserved table (unspec/default/main/local) and cannot be used by Netsukuku"
    )]
    ReservedTable(u32),

    /// The kernel rejected the request's arguments (`EINVAL`), e.g. a route prefix with host
    /// bits set. Only [`crate::FakeNetlink`] constructs this today, mirroring the kernel.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// No kernel object matched the given key (used by [`crate::FakeNetlink`]
    /// to mirror the kernel's `ENOENT`/`ESRCH` on deleting something that
    /// isn't there).
    #[error("no matching kernel object: {0}")]
    NotFound(String),

    /// The requested kernel object already exists (used by
    /// [`crate::FakeNetlink`] to mirror the kernel's `EEXIST` on adding
    /// something that is already there).
    #[error("kernel object already exists: {0}")]
    AlreadyExists(String),
}

const EPERM: i32 = 1;
const ENOENT: i32 = 2;
const ESRCH: i32 = 3;
const EACCES: i32 = 13;
const EEXIST: i32 = 17;

impl From<rtnetlink::Error> for NetlinkError {
    /// Maps the kernel's errno onto the typed variants so callers can tell "already there"
    /// from a genuine failure on a real kernel exactly as they can on `FakeNetlink`.
    fn from(error: rtnetlink::Error) -> Self {
        let errno = match &error {
            rtnetlink::Error::NetlinkError(message) => {
                Some(message.raw_code().abs()).filter(|code| *code != 0)
            }
            _ => None,
        };
        match errno {
            Some(EEXIST) => Self::AlreadyExists(error.to_string()),
            Some(ENOENT | ESRCH) => Self::NotFound(error.to_string()),
            Some(EPERM | EACCES) => Self::PermissionDenied(error.to_string()),
            _ => Self::Netlink(error),
        }
    }
}

impl NetlinkError {
    /// Whether the kernel (or fake) reported the object as already existing (`EEXIST`).
    pub fn is_already_exists(&self) -> bool {
        matches!(self, Self::AlreadyExists(_))
    }

    /// Whether the kernel (or fake) reported the object as absent (`ENOENT`/`ESRCH`).
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }

    /// Whether the kernel refused for lack of privilege (`EPERM`/`EACCES`).
    pub fn is_permission_denied(&self) -> bool {
        matches!(self, Self::PermissionDenied(_))
    }
}

#[cfg(test)]
mod tests {
    use rtnetlink::packet_core::{ErrorBuffer, ErrorMessage, Parseable};

    use super::*;

    fn kernel_error(errno: i32) -> rtnetlink::Error {
        let bytes = (-errno).to_ne_bytes();
        let buffer = ErrorBuffer::new_checked(&bytes).expect("a bare error code is a valid buffer");
        rtnetlink::Error::NetlinkError(ErrorMessage::parse(&buffer).expect("error message parses"))
    }

    #[test]
    fn kernel_errnos_map_to_typed_variants() {
        assert!(NetlinkError::from(kernel_error(EEXIST)).is_already_exists());
        assert!(NetlinkError::from(kernel_error(ESRCH)).is_not_found());
        assert!(NetlinkError::from(kernel_error(ENOENT)).is_not_found());
        assert!(NetlinkError::from(kernel_error(EPERM)).is_permission_denied());
        assert!(NetlinkError::from(kernel_error(EACCES)).is_permission_denied());
    }

    #[test]
    fn unrecognised_errnos_stay_generic() {
        let error = NetlinkError::from(kernel_error(22));
        assert!(matches!(error, NetlinkError::Netlink(_)));
    }
}
