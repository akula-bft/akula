use crate::{changeset::*, common, dbutils, dbutils::*, kv::*, models::*, Cursor, Transaction};
use arrayref::array_ref;
use bytes::Bytes;
use common::{Hash, Incarnation};
use ethereum_types::{Address, H256};
use roaring::RoaringTreemap;

pub async fn get_account_data_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    if let Some(v) = find_data_by_history(tx, address, timestamp).await? {
        return Ok(Some(v));
    }

    tx.get(&tables::PlainState, address.as_fixed_bytes()).await
}

pub async fn get_storage_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    key: Hash,
    block_number: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let key = plain_generate_composite_storage_key(address, incarnation, key);
    if let Some(v) = find_storage_by_history(tx, key, block_number).await? {
        return Ok(Some(v));
    }

    tx.get(&tables::PlainState, &key).await
}

pub async fn find_data_by_history<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: common::Address,
    block_number: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let mut ch = tx.cursor(&tables::AccountHistory).await?;
    if let Some((k, v)) = ch
        .seek(&AccountHistory::index_chunk_key(address, block_number))
        .await?
    {
        if k.starts_with(address.as_fixed_bytes()) {
            let change_set_block = RoaringTreemap::deserialize_from(&*v)?
                .into_iter()
                .find(|n| *n >= block_number);

            let data = {
                if let Some(change_set_block) = change_set_block {
                    let data = {
                        let mut c = tx.cursor_dup_sort(&tables::AccountChangeSet).await?;
                        AccountHistory::find(&mut c, change_set_block, &address).await?
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
                if acc.incarnation > 0 && acc.code_hash.is_none() {
                    if let Some(code_hash) = tx
                        .get(
                            &tables::PlainCodeHash,
                            &dbutils::plain_generate_storage_prefix(address, acc.incarnation),
                        )
                        .await?
                    {
                        acc.code_hash = Some(H256(*array_ref![&*code_hash, 0, 32]));
                    }

                    let mut data = vec![0; acc.encoding_length_for_storage()];
                    acc.encode_for_storage(&mut data);

                    return Ok(Some(data.into()));
                }
            }

            return Ok(Some(data));
        }
    }

    Ok(None)
}

pub async fn find_storage_by_history<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    key: PlainCompositeStorageKey,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let mut ch = tx.cursor(&tables::StorageHistory).await?;
    if let Some((k, v)) = ch
        .seek(&StorageHistory::index_chunk_key(key, timestamp))
        .await?
    {
        if k[..common::ADDRESS_LENGTH] != key[..common::ADDRESS_LENGTH]
            || k[common::ADDRESS_LENGTH..common::ADDRESS_LENGTH + common::HASH_LENGTH]
                != key[common::ADDRESS_LENGTH + common::INCARNATION_LENGTH..]
        {
            return Ok(None);
        }
        let change_set_block = RoaringTreemap::deserialize_from(&*v)?
            .into_iter()
            .find(|n| *n >= timestamp);

        let data = {
            if let Some(change_set_block) = change_set_block {
                let data = {
                    let mut c = tx.cursor_dup_sort(&tables::StorageChangeSet).await?;
                    find_storage_with_incarnation(&mut c, change_set_block, &key).await?
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
