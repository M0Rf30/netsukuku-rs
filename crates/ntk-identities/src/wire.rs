//! Wire encoding for this module's payload types
//! (`proto/identities.proto`), and the `TypedValue` glue built on
//! `ntk_proto::domain`'s generic `typed_value`/`from_typed_value` helpers
//! (the shared codec every phase-2 module uses — see `ntk-proto`'s
//! `domain` module doc).

use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::TypedValue;

use crate::error::Error;
use crate::identity::IdentityId;
use crate::migration::DuplicationData;

/// Generated protobuf types for `ntk.identities.v1`
/// (`proto/identities.proto`).
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.identities.v1.rs"));
}

const IDENTITY_ID_TAG: &str = "identities.IdentityId";
const DUPLICATION_DATA_TAG: &str = "identities.DuplicationData";

/// Encodes `id` as the `TypedValue` wire form of the opaque `IIdentityID`
/// marker interface (`ntkdrpc/interfaces.vala:125-127`), used throughout
/// `MethodCall`'s identity-manager arms
/// (`IdentityMatchDuplicationArgs.peer_id`/`old_id`/`new_id`,
/// `IdentityNotifyIdentityArcRemovedArgs.peer_id`/`my_id`).
#[must_use]
pub fn identity_id_to_typed_value(id: IdentityId) -> TypedValue {
    typed_value(
        IDENTITY_ID_TAG,
        &v1::IdentityId {
            value: id.into_raw(),
        },
    )
}

/// Decodes a `TypedValue` produced by [`identity_id_to_typed_value`].
///
/// # Errors
/// [`Error::Decode`] if `tv` does not decode as `ntk.identities.v1.IdentityId`
/// tagged `"identities.IdentityId"`.
pub fn identity_id_from_typed_value(tv: &TypedValue) -> Result<IdentityId, Error> {
    let msg = from_typed_value::<v1::IdentityId>(tv, IDENTITY_ID_TAG)?;
    Ok(IdentityId::from_raw(msg.value))
}

/// Encodes `data` as the `TypedValue` wire form of the opaque
/// `IDuplicationData` marker interface (`ntkdrpc/interfaces.vala:121-123`).
#[must_use]
pub fn duplication_data_to_typed_value(data: &DuplicationData) -> TypedValue {
    typed_value(
        DUPLICATION_DATA_TAG,
        &v1::DuplicationData {
            peer_new_id: Some(v1::IdentityId {
                value: data.peer_new_id.into_raw(),
            }),
            peer_old_id_new_mac: data.peer_old_id_new_mac.clone(),
            peer_old_id_new_linklocal: data.peer_old_id_new_linklocal.clone(),
        },
    )
}

/// Decodes a `TypedValue` produced by [`duplication_data_to_typed_value`].
///
/// # Errors
/// [`Error::Decode`] if `tv` does not decode as
/// `ntk.identities.v1.DuplicationData` tagged `"identities.DuplicationData"`;
/// [`Error::MissingField`] if `peer_new_id` is unset.
pub fn duplication_data_from_typed_value(tv: &TypedValue) -> Result<DuplicationData, Error> {
    let msg = from_typed_value::<v1::DuplicationData>(tv, DUPLICATION_DATA_TAG)?;
    let peer_new_id = msg
        .peer_new_id
        .ok_or(Error::MissingField("DuplicationData.peer_new_id"))?;
    Ok(DuplicationData {
        peer_new_id: IdentityId::from_raw(peer_new_id.value),
        peer_old_id_new_mac: msg.peer_old_id_new_mac,
        peer_old_id_new_linklocal: msg.peer_old_id_new_linklocal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_id_round_trips() {
        let id = IdentityId::from_raw(42);
        let tv = identity_id_to_typed_value(id);
        assert_eq!(identity_id_from_typed_value(&tv).unwrap(), id);
    }

    #[test]
    fn duplication_data_round_trips() {
        let data = DuplicationData {
            peer_new_id: IdentityId::from_raw(7),
            peer_old_id_new_mac: "aa:bb:cc:dd:ee:ff".to_owned(),
            peer_old_id_new_linklocal: "fe80::1".to_owned(),
        };
        let tv = duplication_data_to_typed_value(&data);
        assert_eq!(duplication_data_from_typed_value(&tv).unwrap(), data);
    }

    #[test]
    fn wrong_type_tag_is_rejected() {
        let id = IdentityId::from_raw(1);
        let tv = identity_id_to_typed_value(id);
        assert!(duplication_data_from_typed_value(&tv).is_err());
    }
}
