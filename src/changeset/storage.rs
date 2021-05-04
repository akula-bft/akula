use super::*;
use crate::CursorDupSort;
use bytes::Bytes;

impl tables::PlainStorageChangeSet {
    pub async fn find_with_incarnation<'tx, C>(
        c: &mut C,
        block_number: u64,
        k: &[u8],
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        C: CursorDupSort<'tx, tables::PlainStorageChangeSet>,
    {
        find_in_storage_changeset_2(c, block_number, k).await
    }

    pub async fn find_without_incarnation<'tx, C>(
        c: &mut C,
        block_number: u64,
        address_to_find: &[u8],
        key_to_find: &[u8],
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        C: CursorDupSort<'tx, tables::PlainStorageChangeSet>,
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
    use crate::{
        dbutils,
        kv::traits::{MutableCursor, MutableKV, MutableTransaction},
    };
    use ethereum_types::Address;
    use hex_literal::hex;

    type Table = tables::PlainStorageChangeSet;

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

    fn empty_value_generator(_: usize) -> Bytes<'static> {
        Bytes::new()
    }

    fn get_test_data_at_index(
        i: usize,
        j: usize,
        inc: common::Incarnation,
    ) -> dbutils::PlainCompositeStorageKey {
        let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abB{:02x}", i)
            .parse()
            .unwrap();
        let key = common::hash_data(format!("key{}", j).as_bytes());
        plain_key_generator(address, inc, key)
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
        do_test_encoding_storage_new(&random_incarnation, &hash_value_generator)
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation() {
        do_test_encoding_storage_new(&default_incarnation, &hash_value_generator)
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation_and_empty_value() {
        do_test_encoding_storage_new(&default_incarnation, &empty_value_generator)
    }

    fn do_test_encoding_storage_new<
        IncarnationGen: Fn() -> common::Incarnation,
        ValueGen: Fn(usize) -> Bytes<'static>,
    >(
        incarnation_generator: &IncarnationGen,
        value_generator: &ValueGen,
    ) {
        let f = move |num_of_elements, num_of_keys| {
            let mut ch = ChangeSet::new();

            for i in 0..num_of_elements {
                let inc = (incarnation_generator)();
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, inc);
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

    #[tokio::test]
    async fn multiple_incarnations_of_the_same_contract() {
        let env = crate::kv::new_mem_database().unwrap();
        let tx = env.begin_mutable().await.unwrap();

        type CsTable = tables::PlainStorageChangeSet;

        let mut cs = tx.mutable_cursor_dupsort::<CsTable>().await.unwrap();

        let contract_a = Address::from(hex!("6f0e0cdac6c716a00bd8db4d0eee4f2bfccf8e6a"));
        let contract_b = Address::from(hex!("c5acb79c258108f288288bc26f7820d06f45f08c"));
        let contract_c = Address::from(hex!("1cbdd8336800dc3fe27daf5fb5188f0502ac1fc7"));
        let contract_d = Address::from(hex!("d88eba4c93123372a9f67215f80477bc3644e6ab"));

        let key1 = common::Hash::from(hex!(
            "a4e69cebbf4f8f3a1c6e493a6983d8a5879d22057a7c73b00e105d7c7e21efbc"
        ));
        let key2 = common::Hash::from(hex!(
            "0bece5a88f7b038f806dbef77c0b462506e4b566c5be7dd44e8e2fc7b1f6a99c"
        ));
        let key3 = common::Hash::from(hex!(
            "0000000000000000000000000000000000000000000000000000000000000001"
        ));
        let key4 = common::Hash::from(hex!(
            "4fdf6c1878d2469b49684effe69db8689d88a4f1695055538501ff197bc9e30e"
        ));
        let key5 = common::Hash::from(hex!(
            "aa2703c3ae5d0024b2c3ab77e5200bb2a8eb39a140fad01e89a495d73760297c"
        ));
        let key6 = common::Hash::from(hex!(
            "000000000000000000000000000000000000000000000000000000000000df77"
        ));
        let key7 = common::Hash::from(hex!(
            "0000000000000000000000000000000000000000000000000000000000000000"
        ));

        let val1 = hex!("33bf0d0c348a2ef1b3a12b6a535e1e25a56d3624e45603e469626d80fd78c762");
        let val2 = hex!("0000000000000000000000000000000000000000000000000000000000000459");
        let val3 = hex!("0000000000000000000000000000002506e4b566c5be7dd44e8e2fc7b1f6a99c");
        let val4 = hex!("207a386cdf40716455365db189633e822d3a7598558901f2255e64cb5e424714");
        let val5 = hex!("0000000000000000000000000000000000000000000000000000000000000000");
        let val6 = hex!("ec89478783348038046b42cc126a3c4e351977b5f4cf5e3c4f4d8385adbf8046");

        let ch = vec![
            (
                dbutils::plain_generate_composite_storage_key(contract_a, 2, key1),
                Bytes::from(&val1),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_a, 1, key5),
                Bytes::from(&val5),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_a, 2, key6),
                Bytes::from(&val6),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_b, 1, key2),
                Bytes::from(&val2),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_b, 1, key3),
                Bytes::from(&val3),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_c, 5, key4),
                Bytes::from(&val4),
            ),
        ]
        .into_iter()
        .map(|(k, v)| Change::new(k, v))
        .collect();

        let mut c = tx.mutable_cursor_dupsort::<CsTable>().await.unwrap();
        for (k, v) in CsTable::encode(1, &ch) {
            c.put(&k, &v).await.unwrap()
        }

        assert_eq!(
            CsTable::find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_a, 2, key1),
            )
            .await
            .unwrap()
            .unwrap(),
            val1
        );

        assert_eq!(
            CsTable::find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_b, 1, key3)
            )
            .await
            .unwrap()
            .unwrap(),
            val3
        );

        assert_eq!(
            CsTable::find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_a, 1, key5)
            )
            .await
            .unwrap()
            .unwrap(),
            val5
        );

        assert_eq!(
            CsTable::find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_a, 1, key1)
            )
            .await
            .unwrap(),
            None
        );

        assert_eq!(
            CsTable::find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_d, 2, key1)
            )
            .await
            .unwrap(),
            None
        );

        assert_eq!(
            CsTable::find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_b, 1, key7)
            )
            .await
            .unwrap(),
            None
        );
    }
}
