use crate::{
    changeset::*,
    kv::{tables::BitmapKey, *},
    models::*,
    read_account_storage, Cursor, Transaction,
};
use ethereum_types::*;

pub async fn get_account_data_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    timestamp: BlockNumber,
) -> anyhow::Result<Option<EncodedAccount>> {
    if let Some(v) = find_data_by_history(tx, address, timestamp).await? {
        return Ok(Some(v));
    }

    tx.get(&tables::PlainState, tables::PlainStateKey::Account(address))
        .await
        .map(|opt| opt.map(From::from))
}

pub async fn get_storage_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    location: H256,
    block_number: impl Into<BlockNumber>,
) -> anyhow::Result<Option<H256>> {
    if let Some(v) =
        find_storage_by_history(tx, address, incarnation, location, block_number.into()).await?
    {
        return Ok(Some(v));
    }

    read_account_storage(tx, address, incarnation, location).await
}

pub async fn find_data_by_history<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    block_number: BlockNumber,
) -> anyhow::Result<Option<EncodedAccount>> {
    let mut ch = tx.cursor(&tables::AccountHistory).await?;
    if let Some((k, v)) = ch
        .seek(BitmapKey {
            inner: address,
            block_number,
        })
        .await?
    {
        if k.inner == address {
            let change_set_block = v.iter().find(|n| *n >= *block_number);

            let data = {
                if let Some(change_set_block) = change_set_block {
                    let data = {
                        let mut c = tx.cursor_dup_sort(&tables::AccountChangeSet).await?;
                        AccountHistory::find(&mut c, BlockNumber(change_set_block), address).await?
                    };

                    if let Some(data) = data {
                        data
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            };

            //restore codehash
            if let Some(mut acc) = Account::decode_for_storage(&*data)? {
                if acc.incarnation.0 > 0 && acc.code_hash == EMPTY_HASH {
                    if let Some(code_hash) = tx
                        .get(&tables::PlainCodeHash, (address, acc.incarnation))
                        .await?
                    {
                        acc.code_hash = code_hash;
                    }

                    let data = acc.encode_for_storage(false);

                    return Ok(Some(data));
                }
            }

            return Ok(Some(data));
        }
    }

    Ok(None)
}

pub async fn find_storage_by_history<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    location: H256,
    timestamp: BlockNumber,
) -> anyhow::Result<Option<H256>> {
    let mut ch = tx.cursor(&tables::StorageHistory).await?;
    if let Some((k, v)) = ch
        .seek(BitmapKey {
            inner: (address, location),
            block_number: timestamp,
        })
        .await?
    {
        if k.inner.0 != address || k.inner.1 != location {
            return Ok(None);
        }
        let change_set_block = v.iter().find(|n| *n >= *timestamp);

        let data = {
            if let Some(change_set_block) = change_set_block {
                let data = {
                    let mut c = tx.cursor_dup_sort(&tables::StorageChangeSet).await?;
                    StorageHistory::find(
                        &mut c,
                        change_set_block.into(),
                        (address, incarnation, location),
                    )
                    .await?
                };

                if let Some(data) = data {
                    data
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        };

        return Ok(Some(data));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bitmapdb, crypto,
        kv::{
            tables::{StorageChangeKey, StorageChangeSeekKey},
            traits::{ttw, MutableKV},
        },
        state::database::*,
        MutableTransaction,
    };
    use pin_utils::pin_mut;
    use std::collections::HashMap;
    use tokio_stream::StreamExt;

    fn random_account() -> (Account, Address) {
        let acc = Account {
            balance: rand::random::<u64>().into(),
            ..Default::default()
        };
        let addr = crypto::pubkey_to_address(&crypto::to_pubkey(&crypto::generate_key()));

        (acc, addr)
    }

    #[tokio::test]
    async fn mutation_delete_timestamp() {
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        let mut block_writer = PlainStateWriter::new(&tx, 1);

        let empty_account = Account::default();

        let mut addrs = vec![];
        for _ in 0..10 {
            let (acc, addr) = random_account();

            block_writer
                .update_account_data(addr, &empty_account, &acc)
                .await
                .unwrap();
            addrs.push(addr);
        }

        block_writer.write_changesets().await.unwrap();
        block_writer.write_history().await.unwrap();

        let mut cursor = tx.cursor(&tables::AccountChangeSet).await.unwrap();
        let s = cursor.walk(None);

        pin_mut!(s);

        let mut i = 0;
        while s.try_next().await.unwrap().is_some() {
            i += 1;
        }

        assert_eq!(addrs.len(), i);

        let index = bitmapdb::get(&tx, &tables::AccountHistory, addrs[0], 0, u64::MAX)
            .await
            .unwrap();

        assert_eq!(index.iter().next(), Some(1));

        let mut cursor = tx.cursor(&tables::StorageChangeSet).await.unwrap();
        let bn = BlockNumber(1);
        let s = cursor
            .walk(Some(StorageChangeSeekKey::Block(bn)))
            .take_while(ttw(|(StorageChangeKey { block_number, .. }, _)| {
                *block_number == bn
            }));

        pin_mut!(s);

        let mut count = 0;
        while s.try_next().await.unwrap().is_some() {
            count += 1;
        }

        assert_eq!(count, 0, "changeset must be deleted");

        assert_eq!(
            tx.get(
                &tables::AccountHistory,
                BitmapKey {
                    inner: addrs[0],
                    block_number: bn
                }
            )
            .await
            .unwrap(),
            None,
            "account must be deleted"
        );
    }

    #[tokio::test]
    async fn mutation_commit() {
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        let num_of_accounts = 5;
        let num_of_state_keys = 5;

        let (addrs, acc_state, acc_state_storage, acc_history, acc_history_state_storage) =
            generate_accounts_with_storage_and_history(
                &mut PlainStateWriter::new(&tx, 2),
                num_of_accounts,
                num_of_state_keys,
            )
            .await;

        let mut plain_state = tx.cursor(&tables::PlainState).await.unwrap();

        for i in 0..num_of_accounts {
            let a = addrs[i];
            let acc = read_account_data(&tx, a).await.unwrap().unwrap();

            assert_eq!(acc_state[i], acc);

            let index = bitmapdb::get(&tx, &tables::AccountHistory, a, 0, u64::MAX)
                .await
                .unwrap();

            assert_eq!(index.iter().next().unwrap(), 2);
            assert_eq!(index.len(), 1);

            let res_account_storage = plain_state
                .walk(Some(tables::PlainStateSeekKey::StorageWithIncarnation(
                    a,
                    acc.incarnation,
                )))
                .take_while(ttw(|fv: &tables::PlainStateFusedValue| {
                    if let tables::PlainStateFusedValue::Storage {
                        address,
                        incarnation,
                        ..
                    } = fv
                    {
                        if *address == a && *incarnation == acc.incarnation {
                            return true;
                        }
                    }

                    false
                }))
                .fold(HashMap::new(), |mut accum, res| {
                    let (_, _, location, value) = res.unwrap().as_storage().unwrap();
                    accum.insert(location, value);
                    accum
                })
                .await;

            assert_eq!(res_account_storage, acc_state_storage[i]);

            for (&key, &v) in &acc_history_state_storage[i] {
                assert_eq!(
                    v,
                    get_storage_as_of(&tx, a, acc.incarnation, key, 1)
                        .await
                        .unwrap()
                        .unwrap()
                );
            }
        }

        let bn = BlockNumber(2);
        let changeset_in_db = tx
            .cursor(&tables::AccountChangeSet)
            .await
            .unwrap()
            .walk(Some(bn))
            .take_while(ttw(|(key, _)| *key == bn))
            .map(|res| {
                let (k, v) = res.unwrap();
                AccountHistory::decode(k, v).1
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<AccountChangeSet>();

        let mut expected_changeset = AccountChangeSet::new();
        for i in 0..num_of_accounts {
            let b = acc_history[i].encode_for_storage(true);
            expected_changeset.insert((addrs[i], b));
        }

        assert_eq!(changeset_in_db, expected_changeset);

        let bn = BlockNumber(2);
        let cs = tx
            .cursor(&tables::StorageChangeSet)
            .await
            .unwrap()
            .walk(Some(StorageChangeSeekKey::Block(bn)))
            .take_while(ttw(|(StorageChangeKey { block_number, .. }, _)| {
                *block_number == bn
            }))
            .map(|res| {
                let (k, v) = res.unwrap();
                StorageHistory::decode(k, v).1
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<StorageChangeSet>();

        assert_eq!(cs.len(), num_of_accounts * num_of_state_keys);

        let mut expected_changeset = StorageChangeSet::new();

        for (i, &address) in addrs.iter().enumerate() {
            for j in 0..num_of_state_keys {
                let key = H256::from_slice(&hex::decode(format!("{:0>64}", i * 100 + j)).unwrap());
                let value = H256::from_low_u64_be(10 + j as u64);

                expected_changeset.insert(((address, acc_history[i].incarnation, key), value));
            }
        }

        assert_eq!(cs, expected_changeset);
    }

    async fn generate_accounts_with_storage_and_history<
        'db: 'tx,
        'tx,
        Tx: MutableTransaction<'db>,
    >(
        block_writer: &mut PlainStateWriter<'db, 'tx, Tx>,
        num_of_accounts: usize,
        num_of_state_keys: usize,
    ) -> (
        Vec<Address>,
        Vec<Account>,
        Vec<HashMap<H256, H256>>,
        Vec<Account>,
        Vec<HashMap<H256, H256>>,
    ) {
        let mut addrs = vec![];
        let mut acc_state = vec![];
        let mut acc_state_storage = vec![];
        let mut acc_history = vec![];
        let mut acc_history_state_storage = vec![];

        for i in 0..num_of_accounts {
            let (acc_history_e, addrs_e) = random_account();
            acc_history.push(acc_history_e);
            addrs.push(addrs_e);

            acc_history[i].balance = 100.into();
            acc_history[i].code_hash =
                H256::from_slice(&hex::decode(format!("{:0>64}", 10 + i)).unwrap());
            // acc_history[i].root = Some(Hash::from_slice(
            //     &hex::decode(format!("{:0>64}", 10 + i)).unwrap(),
            // ));
            acc_history[i].incarnation = Incarnation(i as u64 + 1);

            acc_state.push(acc_history[i].clone());
            acc_state[i].nonce += 1;
            acc_state[i].balance = 200.into();

            acc_state_storage.push(HashMap::new());
            acc_history_state_storage.push(HashMap::new());
            for j in 0..num_of_state_keys {
                let key = H256::from_slice(&hex::decode(format!("{:0>64}", i * 100 + j)).unwrap());
                let new_value = H256::from_low_u64_be(j as u64);
                if !new_value.is_zero() {
                    // Empty value is not considered to be present
                    acc_state_storage[i].insert(key, new_value);
                }

                let value = H256::from_low_u64_be(10 + j as u64);
                acc_history_state_storage[i].insert(key, value);
                block_writer
                    .write_account_storage(
                        addrs[i],
                        acc_history[i].incarnation,
                        key,
                        value,
                        new_value,
                    )
                    .await
                    .unwrap();
            }
            block_writer
                .update_account_data(addrs[i], &acc_history[i] /* original */, &acc_state[i])
                .await
                .unwrap();
        }
        block_writer.write_changesets().await.unwrap();
        block_writer.write_history().await.unwrap();

        (
            addrs,
            acc_state,
            acc_state_storage,
            acc_history,
            acc_history_state_storage,
        )
    }
}
