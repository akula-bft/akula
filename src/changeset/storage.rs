use super::*;
use crate::CursorDupSort;
use bytes::Bytes;
use std::io::Write;

#[async_trait]
impl HistoryKind for StorageHistory {
    type Key = [u8; ADDRESS_LENGTH + INCARNATION_LENGTH + KECCAK_LENGTH];
    type IndexChunkKey = [u8; ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH];
    type IndexTable = tables::StorageHistory;
    type ChangeSetTable = tables::StorageChangeSet;
    type EncodedStream<'tx: 'cs, 'cs> = impl EncodedStream<'tx, 'cs>;

    fn index_chunk_key(key: Self::Key, block_number: BlockNumber) -> Self::IndexChunkKey {
        let mut v = [0; ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH];
        v[..ADDRESS_LENGTH].copy_from_slice(&key[..ADDRESS_LENGTH]);
        v[ADDRESS_LENGTH..ADDRESS_LENGTH + KECCAK_LENGTH]
            .copy_from_slice(&key[ADDRESS_LENGTH + INCARNATION_LENGTH..]);
        v[ADDRESS_LENGTH + KECCAK_LENGTH..].copy_from_slice(&block_number.to_be_bytes());
        v
    }
    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: BlockNumber,
        k: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        C: CursorDupSort<'tx, Self::ChangeSetTable>,
    {
        do_search_2(
            cursor,
            block_number,
            Address::from_slice(&k[..ADDRESS_LENGTH]),
            &k[ADDRESS_LENGTH..],
            0.into(),
        )
        .await
    }

    fn encode<'cs, 'tx: 'cs>(
        block_number: BlockNumber,
        s: &'cs ChangeSet<'tx, Self>,
    ) -> Self::EncodedStream<'tx, 'cs> {
        s.iter().map(move |cs| {
            const KEY_PART: usize = ADDRESS_LENGTH + INCARNATION_LENGTH;

            let mut new_k = vec![0; BLOCK_NUMBER_LENGTH + KEY_PART];
            new_k[..BLOCK_NUMBER_LENGTH]
                .copy_from_slice(&dbutils::encode_block_number(block_number));
            new_k[BLOCK_NUMBER_LENGTH..].copy_from_slice(&cs.key[..KEY_PART]);

            let mut new_v = vec![0; KECCAK_LENGTH + cs.value.len()];
            new_v[..KECCAK_LENGTH].copy_from_slice(&cs.key[KEY_PART..]);
            new_v[KECCAK_LENGTH..].copy_from_slice(&cs.value[..]);

            (new_k.into(), new_v.into())
        })
    }

    fn decode<'tx>(
        db_key: Bytes<'tx>,
        mut db_value: Bytes<'tx>,
    ) -> (BlockNumber, Change<'tx, Self::Key>) {
        let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, BLOCK_NUMBER_LENGTH)).into();

        let mut k = [0; ADDRESS_LENGTH + INCARNATION_LENGTH + KECCAK_LENGTH];
        let db_key = &db_key[BLOCK_NUMBER_LENGTH..]; // remove block_n bytes

        k[..db_key.len()].copy_from_slice(db_key);
        k[db_key.len()..].copy_from_slice(&db_value[..KECCAK_LENGTH]);

        let v = db_value.split_off(KECCAK_LENGTH);

        (block_n, Change::new(k, v))
    }
}

pub async fn find_with_incarnation<'tx, C>(
    c: &mut C,
    block_number: impl Into<BlockNumber>,
    k: &[u8],
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    C: CursorDupSort<'tx, tables::StorageChangeSet>,
{
    do_search_2(
        c,
        block_number.into(),
        Address::from_slice(&k[..ADDRESS_LENGTH]),
        &k[ADDRESS_LENGTH + INCARNATION_LENGTH
            ..ADDRESS_LENGTH + INCARNATION_LENGTH + KECCAK_LENGTH],
        u64::from_be_bytes(*array_ref!(&k[ADDRESS_LENGTH..], 0, INCARNATION_LENGTH)).into(),
    )
    .await
}

#[allow(dead_code)]
pub async fn find_without_incarnation<'tx, C>(
    c: &mut C,
    block_number: impl Into<BlockNumber>,
    address_to_find: Address,
    key_to_find: &[u8],
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    C: CursorDupSort<'tx, tables::StorageChangeSet>,
{
    do_search_2(
        c,
        block_number.into(),
        address_to_find,
        key_to_find,
        0.into(),
    )
    .await
}

pub async fn do_search_2<'tx, C>(
    c: &mut C,
    block_number: BlockNumber,
    address_to_find: Address,
    key_bytes_to_find: &[u8],
    incarnation: Incarnation,
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    C: CursorDupSort<'tx, tables::StorageChangeSet>,
{
    if incarnation.0 == 0 {
        let mut seek = [0; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH];
        seek[..BLOCK_NUMBER_LENGTH].copy_from_slice(&block_number.db_key());
        seek[BLOCK_NUMBER_LENGTH..].copy_from_slice(address_to_find.as_bytes());
        let mut b = c.seek(&seek).await?;
        while let Some((k, v)) = b {
            let (_, change) = StorageHistory::decode(k, v);
            if !change.key.starts_with(address_to_find.as_bytes()) {
                break;
            }

            let st_hash = &change.key[ADDRESS_LENGTH + INCARNATION_LENGTH..];
            if st_hash == key_bytes_to_find {
                return Ok(Some(change.value));
            }

            b = c.next().await?
        }

        return Ok(None);
    }

    let mut seek = vec![0; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH];
    seek[..BLOCK_NUMBER_LENGTH].copy_from_slice(&block_number.to_be_bytes());
    seek[BLOCK_NUMBER_LENGTH..]
        .as_mut()
        .write(address_to_find.as_bytes())
        .unwrap();
    seek[BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH..].copy_from_slice(&incarnation.to_be_bytes());

    if let Some(v) = c.seek_both_range(&seek, key_bytes_to_find).await? {
        if v.starts_with(key_bytes_to_find) {
            let (_, change) = StorageHistory::decode(seek.into(), v);

            return Ok(Some(change.value));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::*,
        dbutils,
        kv::traits::{MutableCursor, MutableKV, MutableTransaction},
    };
    use ethereum_types::{Address, H256};
    use hex_literal::hex;
    use std::future::Future;

    const NUM_OF_CHANGES: &[usize] = &[1, 3, 10, 100];

    fn hash_value_generator(j: usize) -> Bytes<'static> {
        keccak256(format!("val{}", j).as_bytes()).0.to_vec().into()
    }

    fn empty_value_generator(_: usize) -> Bytes<'static> {
        Bytes::new()
    }

    fn get_test_data_at_index(
        i: usize,
        j: usize,
        inc: Incarnation,
    ) -> dbutils::PlainCompositeStorageKey {
        let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abB{:02x}", i)
            .parse()
            .unwrap();
        let key = keccak256(format!("key{}", j));
        dbutils::plain_generate_composite_storage_key(address, inc, key)
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
        value_generator: impl Fn(usize) -> Bytes<'static>,
    ) {
        let f = move |num_of_elements, num_of_keys| {
            let mut ch = StorageChangeSet::new();

            for i in 0..num_of_elements {
                let inc = (incarnation_generator)();
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, inc);
                    let val = (value_generator)(j);
                    ch.insert(Change::new(key, val));
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
                    ch.insert(Change::new(key, val));
                }
            }

            for ((_, transformed), original) in StorageHistory::encode(0.into(), &ch)
                .map(|(k, v)| StorageHistory::decode(k, v))
                .zip(&ch)
            {
                assert_eq!(transformed.key, original.key);
                assert_eq!(transformed.value, original.value);
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
                    ch.insert(Change::new(key, val));
                }
            }

            let mut c = tx
                .mutable_cursor_dupsort(&tables::StorageChangeSet)
                .await
                .unwrap();

            for (k, v) in StorageHistory::encode(1.into(), &ch) {
                c.put(&k, &v).await.unwrap()
            }

            for v in ch {
                assert_eq!(
                    v.value,
                    find_with_incarnation(&mut c, 1, &v.key)
                        .await
                        .unwrap()
                        .unwrap()
                        .to_vec()
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

    #[tokio::test]
    async fn encoding_storage_new_without_not_default_incarnation_find_without_incarnation() {
        let db = kv::new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        let tx = &tx;
        let f = |num_of_elements, num_of_keys| async move {
            let mut ch = StorageChangeSet::new();

            for i in 0..num_of_elements {
                for j in 0..num_of_keys {
                    let key = get_test_data_at_index(i, j, DEFAULT_INCARNATION);
                    let val = hash_value_generator(j);
                    ch.insert(Change::new(key, val));
                }
            }

            let mut c = tx
                .mutable_cursor_dupsort(&tables::StorageChangeSet)
                .await
                .unwrap();

            for (k, v) in StorageHistory::encode(1.into(), &ch) {
                c.put(&k, &v).await.unwrap()
            }

            for v in ch {
                let (addr, _, key) = dbutils::plain_parse_composite_storage_key(&v.key);
                let value = find_without_incarnation(&mut c, 1, addr, key.as_bytes())
                    .await
                    .unwrap()
                    .unwrap();

                assert_eq!(v.value, value)
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

        let val1 = hex!("33bf0d0c348a2ef1b3a12b6a535e1e25a56d3624e45603e469626d80fd78c762");
        let val2 = hex!("0000000000000000000000000000000000000000000000000000000000000459");
        let val3 = hex!("0000000000000000000000000000002506e4b566c5be7dd44e8e2fc7b1f6a99c");
        let val4 = hex!("207a386cdf40716455365db189633e822d3a7598558901f2255e64cb5e424714");
        let val5 = hex!("0000000000000000000000000000000000000000000000000000000000000000");
        let val6 = hex!("ec89478783348038046b42cc126a3c4e351977b5f4cf5e3c4f4d8385adbf8046");

        let ch = vec![
            (
                dbutils::plain_generate_composite_storage_key(contract_a, 2.into(), key1),
                Bytes::from(&val1),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_a, 1.into(), key5),
                Bytes::from(&val5),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_a, 2.into(), key6),
                Bytes::from(&val6),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_b, 1.into(), key2),
                Bytes::from(&val2),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_b, 1.into(), key3),
                Bytes::from(&val3),
            ),
            (
                dbutils::plain_generate_composite_storage_key(contract_c, 5.into(), key4),
                Bytes::from(&val4),
            ),
        ]
        .into_iter()
        .map(|(k, v)| Change::new(k, v))
        .collect();

        let mut c = tx
            .mutable_cursor_dupsort(&tables::StorageChangeSet)
            .await
            .unwrap();
        for (k, v) in StorageHistory::encode(1.into(), &ch) {
            c.put(&k, &v).await.unwrap()
        }

        assert_eq!(
            find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_a, 2.into(), key1),
            )
            .await
            .unwrap()
            .unwrap(),
            val1
        );

        assert_eq!(
            find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_b, 1.into(), key3)
            )
            .await
            .unwrap()
            .unwrap(),
            val3
        );

        assert_eq!(
            find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_a, 1.into(), key5)
            )
            .await
            .unwrap()
            .unwrap(),
            val5
        );

        assert_eq!(
            find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_a, 1.into(), key1)
            )
            .await
            .unwrap(),
            None
        );

        assert_eq!(
            find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_d, 2.into(), key1)
            )
            .await
            .unwrap(),
            None
        );

        assert_eq!(
            find_with_incarnation(
                &mut cs,
                1,
                &dbutils::plain_generate_composite_storage_key(contract_b, 1.into(), key7)
            )
            .await
            .unwrap(),
            None
        );
    }
}
