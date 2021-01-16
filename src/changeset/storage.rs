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

pub fn encode_storage(block_n: u64, s: ChangeSet) -> impl Iterator<Item = (Bytes, Bytes)> {
    encode_storage_2(block_n, s, common::HASH_LENGTH)
}

pub struct StorageChangeSet<C: CursorDupSort> {
    pub c: C,
}

#[async_trait]
impl<C: CursorDupSort> Walker2 for StorageChangeSet<C> {
    fn walk(&mut self, from: u64, to: u64) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
        super::storage_utils::walk(&mut self.c, from, to, common::HASH_LENGTH)
    }

    fn walk_reverse(
        &mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
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
    use crate::dbutils;

    use super::*;

    const NUM_OF_CHANGES: &[usize] = &[1, 3, 10, 100];

    const fn default_incarnation() -> common::Incarnation {
        1
    }

    fn random_incarnation() -> common::Incarnation {
        rand::random()
    }

    fn hash_value_generator(j: usize) -> Bytes {
        Bytes::copy_from_slice(common::hash_data(format!("val{}", j).as_bytes()).as_bytes())
    }

    fn empty_value_generator(j: usize) -> Bytes {
        Bytes::new()
    }

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

    fn hash_key_generator(
        address: common::Address,
        inc: common::Incarnation,
        key: common::Hash,
    ) -> Bytes {
        Bytes::copy_from_slice(&dbutils::generate_composite_storage_key(
            common::hash_data(address.as_bytes()),
            inc,
            key,
        ))
    }

    fn plain_key_generator(
        address: common::Address,
        inc: common::Incarnation,
        key: common::Hash,
    ) -> Bytes {
        Bytes::copy_from_slice(&dbutils::plain_generate_composite_storage_key(
            address, inc, key,
        ))
    }

    #[test]
    fn encoding_storage_new_with_random_incarnation_hashed() {
        do_test_encoding_storage_new::<buckets::StorageChangeSet, _, _, _>(
            &hash_key_generator,
            &random_incarnation,
            &hash_value_generator,
        )
    }

    fn do_test_encoding_storage_new<
        Bucket: ChangeSetBucket,
        KeyGen: Fn(common::Address, common::Incarnation, common::Hash) -> Bytes,
        IncarnationGen: Fn() -> common::Incarnation,
        ValueGen: Fn(usize) -> Bytes,
    >(
        key_gen: &KeyGen,
        incarnation_generator: &IncarnationGen,
        value_generator: &ValueGen,
    ) {
        let f = move |num_of_elements, num_of_keys| {
            let mut ch = Bucket::make_changeset();

            for i in 0..num_of_elements {
                let inc = (incarnation_generator)();
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, inc, key_gen);
                    let val = (value_generator)(j);
                    ch.insert(key, val).unwrap();
                }
            }

            let mut ch2 = Bucket::make_changeset();

            for (k, v) in Bucket::encode(0, ch.clone()) {
                let (_, k, v) = Bucket::decode(k, v);
                ch2.insert(k, v).unwrap();
            }

            assert_eq!(ch, ch2)
        };

        for &v in NUM_OF_CHANGES {
            run_test(f, v, 1)
        }

        for &v in NUM_OF_CHANGES {
            run_test(f, v, 5);
        }

        run_test(f, 10, 10);
        run_test(f, 50, 1000);
        run_test(f, 100, 1000);
    }

    fn run_test<F: Fn(usize, usize)>(f: F, elements: usize, keys: usize) {
        println!("elements: {}, keys: {}", elements, keys);
        (f)(elements, keys);
    }
}
