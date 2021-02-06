use super::*;
use crate::{common, CursorDupSort};
use async_trait::async_trait;
use bytes::Bytes;

pub struct StorageChangeSetPlain<'cur, C: CursorDupSort> {
    pub c: &'cur mut C,
}

#[async_trait(?Send)]
impl<'cur, C: 'cur + CursorDupSort> Walker for StorageChangeSetPlain<'cur, C> {
    type Key = [u8; common::ADDRESS_LENGTH + common::HASH_LENGTH + common::INCARNATION_LENGTH];
    type WalkStream<'w> = impl WalkStream<Self::Key>;

    fn walk(&mut self, from: u64, to: u64) -> Self::WalkStream<'_> {
        super::storage_utils::walk::<C, _, _>(
            &mut self.c,
            |db_key, db_value| {
                let (b, k1, v) = from_storage_db_format(db_key, db_value);
                let mut k =
                    [0; common::ADDRESS_LENGTH + common::HASH_LENGTH + common::INCARNATION_LENGTH];
                k[..].copy_from_slice(&k1[..]);

                (b, k, v)
            },
            from,
            to,
        )
    }

    async fn find(&mut self, block_number: u64, k: &Self::Key) -> anyhow::Result<Option<Bytes>> {
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

impl<'cur, C: 'cur + CursorDupSort> StorageChangeSetPlain<'cur, C> {
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
        address_to_find: &[u8],
        key_to_find: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        find_without_incarnation_in_storage_changeset_2(
            &mut self.c,
            block_number,
            common::ADDRESS_LENGTH,
            address_to_find,
            key_to_find,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbutils;
    use std::fmt::Debug;

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
        Key,
        KeyGen: Fn(common::Address, common::Incarnation, common::Hash) -> Key,
    >(
        i: usize,
        j: usize,
        inc: common::Incarnation,
        generator: &KeyGen,
    ) -> Key {
        let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abB{:02x}", i)
            .parse()
            .unwrap();
        let key = common::hash_data(format!("key{}", j).as_bytes());
        (generator)(address, inc, key)
    }

    fn hash_key_generator(
        address: common::Address,
        inc: common::Incarnation,
        key: common::Hash,
    ) -> dbutils::CompositeStorageKey {
        dbutils::generate_composite_storage_key(common::hash_data(address.as_bytes()), inc, key)
    }

    fn plain_key_generator(
        address: common::Address,
        inc: common::Incarnation,
        key: common::Hash,
    ) -> dbutils::PlainCompositeStorageKey {
        dbutils::plain_generate_composite_storage_key(address, inc, key)
    }

    #[test]
    fn encoding_storage_new_with_random_incarnation() {
        do_test_encoding_storage_new::<buckets::PlainStorageChangeSet, _, _, _, _>(
            &plain_key_generator,
            &random_incarnation,
            &hash_value_generator,
        )
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation() {
        do_test_encoding_storage_new::<buckets::PlainStorageChangeSet, _, _, _, _>(
            &plain_key_generator,
            &default_incarnation,
            &hash_value_generator,
        )
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation_and_empty_value() {
        do_test_encoding_storage_new::<buckets::PlainStorageChangeSet, _, _, _, _>(
            &plain_key_generator,
            &default_incarnation,
            &empty_value_generator,
        )
    }

    fn do_test_encoding_storage_new<
        Bucket: ChangeSetBucket<Key = Key>,
        Key: ChangeKey,
        KeyGen: Fn(common::Address, common::Incarnation, common::Hash) -> Key,
        IncarnationGen: Fn() -> common::Incarnation,
        ValueGen: Fn(usize) -> Bytes,
    >(
        key_gen: &KeyGen,
        incarnation_generator: &IncarnationGen,
        value_generator: &ValueGen,
    ) {
        let f = move |num_of_elements, num_of_keys| {
            let mut ch = ChangeSet::new();

            for i in 0..num_of_elements {
                let inc = (incarnation_generator)();
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, inc, key_gen);
                    let val = (value_generator)(j);
                    ch.insert(Change::new(key, val));
                }
            }

            let mut ch2 = ChangeSet::new();

            for (k, v) in Bucket::encode(0, &ch) {
                let (_, k, v) = Bucket::decode(k, v);
                ch2.insert(Change::new(k, v));
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
