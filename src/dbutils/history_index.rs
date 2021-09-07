use crate::{changeset::HistoryKind, models::*};
use bytes::Bytes;
use static_bytes::{BufMut, BytesMut};

pub fn composite_key_without_incarnation<K: HistoryKind>(key: &K::Key) -> Bytes<'_> {
    if key.as_ref().len() == ADDRESS_LENGTH + KECCAK_LENGTH + INCARNATION_LENGTH {
        let mut b = BytesMut::with_capacity(ADDRESS_LENGTH + KECCAK_LENGTH);
        b.put(&key.as_ref()[..ADDRESS_LENGTH]);
        b.put(&key.as_ref()[ADDRESS_LENGTH + INCARNATION_LENGTH..]);
        return b.freeze().into();
    }

    key.as_ref().into()
}
