use super::*;
use crate::{
    kv::tables::{StorageChange, StorageChangeKey},
    CursorDupSort,
};
use ethereum_types::*;

#[async_trait]
impl HistoryKind for StorageHistory {
    type Key = (Address, Incarnation, H256);
    type Value = H256;
    type IndexChunkKey = (Address, H256);
    type IndexTable = tables::StorageHistory;
    type ChangeSetTable = tables::StorageChangeSet;
    type EncodedStream<'cs> = impl EncodedStream<'cs, Self::ChangeSetTable>;

    fn index_chunk_key((address, _, location): Self::Key) -> Self::IndexChunkKey {
        (address, location)
    }
    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: BlockNumber,
        (address, incarnation, location): Self::Key,
    ) -> anyhow::Result<Option<Self::Value>>
    where
        C: CursorDupSort<'tx, Self::ChangeSetTable>,
    {
        if let Some((_, v)) = cursor
            .seek_both_range(
                StorageChangeKey {
                    block_number,
                    address,
                    incarnation,
                },
                location,
            )
            .await?
        {
            if v.location == location {
                return Ok(Some(v.value.0));
            }
        }

        Ok(None)
    }

    fn encode(block_number: BlockNumber, changes: &ChangeSet<Self>) -> Self::EncodedStream<'_> {
        changes
            .iter()
            .map(move |&((address, incarnation, location), value)| {
                (
                    StorageChangeKey {
                        block_number,
                        address,
                        incarnation,
                    },
                    StorageChange {
                        location,
                        value: value.into(),
                    },
                )
            })
    }

    fn decode(
        StorageChangeKey {
            block_number,
            address,
            incarnation,
        }: <Self::ChangeSetTable as Table>::Key,
        StorageChange { location, value }: <Self::ChangeSetTable as Table>::Value,
    ) -> (BlockNumber, Change<Self::Key, Self::Value>) {
        (block_number, ((address, incarnation, location), value.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::*,
        kv::{self, traits::*},
    };
    use ethereum_types::{Address, H256};
    use hex_literal::hex;
    use std::future::Future;

    const NUM_OF_CHANGES: &[usize] = &[1, 3, 10, 100];

    fn hash_value_generator(j: usize) -> H256 {
        keccak256(format!("val{}", j).as_bytes())
    }

    fn empty_value_generator(_: usize) -> H256 {
        H256::zero()
    }

    fn get_test_data_at_index(
        i: usize,
        j: usize,
        incarnation: Incarnation,
    ) -> <StorageHistory as HistoryKind>::Key {
        let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abB{:02x}", i)
            .parse()
            .unwrap();
        let key = keccak256(format!("key{}", j));
        (address, incarnation, key)
    }

    #[test]
    fn encoding_storage_new_with_random_incarnation() {
        do_test_encoding_storage_new(|| Incarnation(rand::random()), hash_value_generator)
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation() {
        do_test_encoding_storage_new(|| DEFAULT_INCARNATION, hash_value_generator)
    }

    #[test]
    fn encoding_storage_new_with_default_incarnation_and_empty_value() {
        do_test_encoding_storage_new(|| DEFAULT_INCARNATION, empty_value_generator)
    }

    fn do_test_encoding_storage_new(
        incarnation_generator: impl Fn() -> Incarnation,
        value_generator: impl Fn(usize) -> H256,
    ) {
        let f = move |num_of_elements, num_of_keys| {
            let mut ch = StorageChangeSet::new();

            for i in 0..num_of_elements {
                let inc = (incarnation_generator)();
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, inc);
                    let val = (value_generator)(j);
                    ch.insert((key, val));
                }
            }

            let mut ch2 = StorageChangeSet::new();

            for (k, v) in StorageHistory::encode(0.into(), &ch) {
                let (_, change) = StorageHistory::decode(k, v);
                ch2.insert(change);
            }

            assert_eq!(ch, ch2)
        };

        for &v in NUM_OF_CHANGES {
            run_test(&f, v, 1)
        }

        for &v in NUM_OF_CHANGES {
            run_test(&f, v, 5);
        }

        run_test(&f, 10, 10);
        run_test(&f, 50, 1000);
        run_test(&f, 100, 1000);
    }

    #[tokio::test]
    async fn encoding_storage_new_without_not_default_incarnation_walk() {
        let f = |num_of_elements, num_of_keys| {
            let mut ch = StorageChangeSet::new();

            for i in 0..num_of_elements {
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, DEFAULT_INCARNATION);
                    let val = hash_value_generator(j);
                    ch.insert((key, val));
                }
            }

            for ((_, transformed), original) in StorageHistory::encode(0.into(), &ch)
                .map(|(k, v)| StorageHistory::decode(k, v))
                .zip(&ch)
            {
                assert_eq!(transformed, *original);
            }
        };

        for &v in NUM_OF_CHANGES {
            run_test(&f, v, 1)
        }

        for &v in NUM_OF_CHANGES {
            run_test(&f, v, 5);
        }

        run_test(&f, 50, 1000);
        run_test(&f, 5, 1000);
    }

    #[tokio::test]
    async fn encoding_storage_new_without_not_default_incarnation_find() {
        let db = kv::new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        let tx = &tx;
        let f = |num_of_elements, num_of_keys| async move {
            let mut ch = StorageChangeSet::new();

            for i in 0..num_of_elements {
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, DEFAULT_INCARNATION);
                    let val = hash_value_generator(j);
                    ch.insert((key, val));
                }
            }

            let mut c = tx
                .mutable_cursor_dupsort(&tables::StorageChangeSet)
                .await
                .unwrap();

            for (k, v) in StorageHistory::encode(1.into(), &ch) {
                c.put((k, v)).await.unwrap()
            }

            for v in ch {
                assert_eq!(
                    v.1,
                    StorageHistory::find(&mut c, 1.into(), v.0)
                        .await
                        .unwrap()
                        .unwrap()
                )
            }

            let mut c = tx
                .mutable_cursor_dupsort(&tables::StorageChangeSet)
                .await
                .unwrap();

            while c.first().await.unwrap().is_some() {
                c.delete_current().await.unwrap();
            }
        };

        for &v in NUM_OF_CHANGES[..NUM_OF_CHANGES.len() - 2].iter() {
            run_test_async(f, v, 1).await;
        }

        for &v in NUM_OF_CHANGES[..NUM_OF_CHANGES.len() - 2].iter() {
            run_test_async(f, v, 5).await;
        }

        run_test_async(f, 50, 1000).await;
        run_test_async(f, 100, 1000).await;
    }

    fn run_test<F: Fn(usize, usize)>(f: F, elements: usize, keys: usize) {
        println!("elements: {}, keys: {}", elements, keys);
        (f)(elements, keys);
    }

    async fn run_test_async<F: Fn(usize, usize) -> Fut, Fut: Future<Output = ()>>(
        f: F,
        elements: usize,
        keys: usize,
    ) {
        println!("elements: {}, keys: {}", elements, keys);
        (f)(elements, keys).await;
    }

    #[tokio::test]
    async fn multiple_incarnations_of_the_same_contract() {
        let env = crate::kv::new_mem_database().unwrap();
        let tx = env.begin_mutable().await.unwrap();

        let mut cs = tx
            .mutable_cursor_dupsort(&tables::StorageChangeSet)
            .await
            .unwrap();

        let contract_a = Address::from(hex!("6f0e0cdac6c716a00bd8db4d0eee4f2bfccf8e6a"));
        let contract_b = Address::from(hex!("c5acb79c258108f288288bc26f7820d06f45f08c"));
        let contract_c = Address::from(hex!("1cbdd8336800dc3fe27daf5fb5188f0502ac1fc7"));
        let contract_d = Address::from(hex!("d88eba4c93123372a9f67215f80477bc3644e6ab"));

        let key1 = H256::from(hex!(
            "a4e69cebbf4f8f3a1c6e493a6983d8a5879d22057a7c73b00e105d7c7e21efbc"
        ));
        let key2 = H256::from(hex!(
            "0bece5a88f7b038f806dbef77c0b462506e4b566c5be7dd44e8e2fc7b1f6a99c"
        ));
        let key3 = H256::from(hex!(
            "0000000000000000000000000000000000000000000000000000000000000001"
        ));
        let key4 = H256::from(hex!(
            "4fdf6c1878d2469b49684effe69db8689d88a4f1695055538501ff197bc9e30e"
        ));
        let key5 = H256::from(hex!(
            "aa2703c3ae5d0024b2c3ab77e5200bb2a8eb39a140fad01e89a495d73760297c"
        ));
        let key6 = H256::from(hex!(
            "000000000000000000000000000000000000000000000000000000000000df77"
        ));
        let key7 = H256::from(hex!(
            "0000000000000000000000000000000000000000000000000000000000000000"
        ));

        let val1 = hex!("33bf0d0c348a2ef1b3a12b6a535e1e25a56d3624e45603e469626d80fd78c762").into();
        let val2 = hex!("0000000000000000000000000000000000000000000000000000000000000459").into();
        let val3 = hex!("0000000000000000000000000000002506e4b566c5be7dd44e8e2fc7b1f6a99c").into();
        let val4 = hex!("207a386cdf40716455365db189633e822d3a7598558901f2255e64cb5e424714").into();
        let val5 = hex!("0000000000000000000000000000000000000000000000000000000000000000").into();
        let val6 = hex!("ec89478783348038046b42cc126a3c4e351977b5f4cf5e3c4f4d8385adbf8046").into();

        let ch = vec![
            ((contract_a, 2.into(), key1), val1),
            ((contract_a, 1.into(), key5), val5),
            ((contract_a, 2.into(), key6), val6),
            ((contract_b, 1.into(), key2), val2),
            ((contract_b, 1.into(), key3), val3),
            ((contract_c, 5.into(), key4), val4),
        ]
        .into_iter()
        .collect::<ChangeSet<StorageHistory>>();

        let mut c = tx
            .mutable_cursor_dupsort(&tables::StorageChangeSet)
            .await
            .unwrap();
        for (k, v) in StorageHistory::encode(1.into(), &ch) {
            c.put((k, v)).await.unwrap()
        }

        assert_eq!(
            StorageHistory::find(&mut cs, 1.into(), (contract_a, 2.into(), key1))
                .await
                .unwrap()
                .unwrap(),
            val1
        );

        assert_eq!(
            StorageHistory::find(&mut cs, 1.into(), (contract_b, 1.into(), key3))
                .await
                .unwrap()
                .unwrap(),
            val3
        );

        assert_eq!(
            StorageHistory::find(&mut cs, 1.into(), (contract_a, 1.into(), key5))
                .await
                .unwrap()
                .unwrap(),
            val5
        );

        assert_eq!(
            StorageHistory::find(&mut cs, 1.into(), (contract_a, 1.into(), key1))
                .await
                .unwrap(),
            None
        );

        assert_eq!(
            StorageHistory::find(&mut cs, 1.into(), (contract_d, 2.into(), key1))
                .await
                .unwrap(),
            None
        );

        assert_eq!(
            StorageHistory::find(&mut cs, 1.into(), (contract_b, 1.into(), key7))
                .await
                .unwrap(),
            None
        );
    }
}
