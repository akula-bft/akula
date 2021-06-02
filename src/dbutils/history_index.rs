use bytes::Bytes;
use static_bytes::{BufMut, BytesMut};

use crate::{changeset::HistoryKind, common};

pub fn composite_key_without_incarnation<K: HistoryKind>(key: &K::Key) -> Bytes<'_> {
    if key.as_ref().len()
        == common::ADDRESS_LENGTH + common::HASH_LENGTH + common::INCARNATION_LENGTH
    {
        let mut b = BytesMut::with_capacity(common::ADDRESS_LENGTH + common::HASH_LENGTH);
        b.put(&key.as_ref()[..common::ADDRESS_LENGTH]);
        b.put(&key.as_ref()[common::ADDRESS_LENGTH + common::INCARNATION_LENGTH..]);
        return b.freeze().into();
    }

    key.as_ref().into()
}
