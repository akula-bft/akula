use super::*;
use crate::{common, CursorDupSort};
use bytes::Bytes;

impl tables::PlainStorageChangeSet {
    pub async fn find_with_incarnation<'tx, Txn, C>(
        c: &mut C,
        block_number: u64,
        k: &[u8],
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        Txn: Transaction<'tx>,
        C: CursorDupSort<'tx, Txn, tables::PlainStorageChangeSet>,
    {
        find_in_storage_changeset_2(c, block_number, k).await
    }

    pub async fn find_without_incarnation<'tx, Txn, C>(
        c: &mut C,
        block_number: u64,
        address_to_find: &[u8],
        key_to_find: &[u8],
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        Txn: Transaction<'tx>,
        C: CursorDupSort<'tx, Txn, tables::PlainStorageChangeSet>,
    {
        find_without_incarnation_in_storage_changeset_2(
            c,
            block_number,
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

    const NUM_OF_CHANGES: &[usize] = &[1, 3, 10, 100];

    const fn default_incarnation() -> common::Incarnation {
        1
    }

    fn random_incarnation() -> common::Incarnation {
        rand::random()
    }

    fn hash_value_generator(j: usize) -> Bytes<'static> {
        common::hash_data(format!("val{}", j).as_bytes())
            .as_bytes()
            .to_vec()
            .into()
    }

    fn empty_value_generator(j: usize) -> Bytes<'static> {
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
        do_test_encoding_storage_new::<tables::PlainStorageChangeSet, _, _, _, _>(
            &plain_key_generator,
            &random_incarnation,
            &hash_value_generator,
        )
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation() {
        do_test_encoding_storage_new::<tables::PlainStorageChangeSet, _, _, _, _>(
            &plain_key_generator,
            &default_incarnation,
            &hash_value_generator,
        )
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation_and_empty_value() {
        do_test_encoding_storage_new::<tables::PlainStorageChangeSet, _, _, _, _>(
            &plain_key_generator,
            &default_incarnation,
            &empty_value_generator,
        )
    }

    fn do_test_encoding_storage_new<
        Table: ChangeSetTable<Key = Key>,
        Key: ChangeKey,
        KeyGen: Fn(common::Address, common::Incarnation, common::Hash) -> Key,
        IncarnationGen: Fn() -> common::Incarnation,
        ValueGen: Fn(usize) -> Bytes<'static>,
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

            for (k, v) in Table::encode(0, &ch) {
                let (_, k, v) = Table::decode(k, v);
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
