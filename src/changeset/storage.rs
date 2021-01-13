use super::*;
use crate::{common, CursorDupSort};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::pin::Pin;

/* Hashed changesets (key is a hash of common.Address) */

impl ChangeSet {
    pub fn new_storage() -> Self {
        Self {
            changes: vec![],
            key_len: common::HASH_LENGTH + common::HASH_LENGTH + common::INCARNATION_LENGTH,
        }
    }
}

pub fn encode_storage(
    block_n: u64,
    s: ChangeSet,
) -> Pin<Box<dyn Iterator<Item = (Bytes, Bytes)> + Send>> {
    Box::pin(encode_storage_2(block_n, s, common::HASH_LENGTH))
}

pub struct StorageChangeSet<C: CursorDupSort> {
    pub c: C,
}

#[async_trait]
impl<C: CursorDupSort> Walker2 for StorageChangeSet<C> {
    fn walk<'w>(
        &'w mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'w, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk(&mut self.c, from, to, common::HASH_LENGTH)
    }

    fn walk_reverse<'w>(
        &'w mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'w, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk_reverse(&mut self.c, from, to, common::HASH_LENGTH)
    }

    async fn find(&mut self, block_number: u64, k: &[u8]) -> anyhow::Result<Option<Bytes>> {
        find_without_incarnation_in_storage_changeset_2(
            &mut self.c,
            block_number,
            common::HASH_LENGTH,
            &k[..common::HASH_LENGTH],
            &k[common::HASH_LENGTH..],
        )
        .await
    }
}

impl<C: CursorDupSort> StorageChangeSet<C> {
    pub async fn find_with_incarnation(
        &mut self,
        block_number: u64,
        k: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        find_in_storage_changeset_2(&mut self.c, block_number, common::HASH_LENGTH, k).await
    }

    pub async fn find_without_incarnation(
        &mut self,
        block_number: u64,
        addr_hash_to_find: &[u8],
        key_hash_to_find: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        find_without_incarnation_in_storage_changeset_2(
            &mut self.c,
            block_number,
            common::HASH_LENGTH,
            addr_hash_to_find,
            key_hash_to_find,
        )
        .await
    }
}

/* Plain changesets (key is a common.Address) */

impl ChangeSet {
    pub fn new_storage_plain() -> Self {
        Self {
            changes: vec![],
            key_len: common::ADDRESS_LENGTH + common::HASH_LENGTH + common::INCARNATION_LENGTH,
        }
    }
}

pub fn encode_storage_plain(
    block_n: u64,
    s: ChangeSet,
) -> Pin<Box<dyn Iterator<Item = (Bytes, Bytes)> + Send>> {
    Box::pin(encode_storage_2(block_n, s, common::ADDRESS_LENGTH))
}

pub struct StorageChangeSetPlain<C: CursorDupSort> {
    pub c: C,
}

#[async_trait]
impl<C: CursorDupSort> Walker2 for StorageChangeSetPlain<C> {
    fn walk<'w>(
        &'w mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'w, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk(&mut self.c, from, to, common::ADDRESS_LENGTH)
    }

    fn walk_reverse<'w>(
        &'w mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'w, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk_reverse(&mut self.c, from, to, common::ADDRESS_LENGTH)
    }

    async fn find(&mut self, block_number: u64, k: &[u8]) -> anyhow::Result<Option<Bytes>> {
        find_without_incarnation_in_storage_changeset_2(
            &mut self.c,
            block_number,
            common::ADDRESS_LENGTH,
            &k[..common::ADDRESS_LENGTH],
            &k[common::ADDRESS_LENGTH..],
        )
        .await
    }
}

impl<C: CursorDupSort> StorageChangeSetPlain<C> {
    pub async fn find_with_incarnation(
        &mut self,
        block_number: u64,
        k: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        find_in_storage_changeset_2(&mut self.c, block_number, common::ADDRESS_LENGTH, k).await
    }

    pub async fn find_without_incarnation(
        &mut self,
        block_number: u64,
        addr_hash_to_find: &[u8],
        key_hash_to_find: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        find_without_incarnation_in_storage_changeset_2(
            &mut self.c,
            block_number,
            common::ADDRESS_LENGTH,
            addr_hash_to_find,
            key_hash_to_find,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_data_at_index<
        KeyGen: Fn(common::Address, common::Incarnation, common::Hash) -> Bytes,
    >(
        i: usize,
        j: usize,
        inc: common::Incarnation,
        generator: &KeyGen,
    ) -> Bytes {
        let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abBc{}", i)
            .parse()
            .unwrap();
        let key = common::hash_data(format!("key{}", j).as_bytes());
        (generator)(address, inc, key)
    }

    fn do_test_encoding_storage_new<
        KeyGen: Fn(common::Address, common::Incarnation, common::Hash) -> Bytes,
        IncarnationGen: Fn() -> common::Incarnation,
        ValueGen: Fn(usize) -> Bytes,
        NewFunc: Fn() -> ChangeSet,
    >(
        key_gen: &KeyGen,
        incarnation_generator: &IncarnationGen,
        value_generator: &ValueGen,
        new_func: &NewFunc,
        encode_func: &Encoder,
        decode_func: &Decoder,
    ) {
        let f = move |num_of_elements, num_of_keys| {
            let mut ch = (new_func)();

            for i in 0..num_of_elements {
                let inc = (incarnation_generator)();
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, inc, key_gen);
                    let val = (value_generator)(j);
                    ch.insert(key, val).unwrap();
                }
            }

            let mut ch2 = (new_func)();

            for (k, v) in (encode_func)(0, ch.clone()) {
                let (_, k, v) = (decode_func)(k, v);
                ch2.insert(k, v).unwrap();
            }

            assert_eq!(ch, ch2)
        };
    }
}
