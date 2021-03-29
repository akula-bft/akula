use crate::{changeset::*, common, dbutils, dbutils::*, models::*, txutil, Cursor, Transaction};
use arrayref::array_ref;
use bytes::Bytes;
use common::{Hash, Incarnation, ADDRESS_LENGTH};
use ethereum_types::{Address, H256};
use roaring::RoaringTreemap;

pub async fn get_account_data_as_of<'tx, Tx: Transaction<'tx>>(
    tx: &'tx Tx,
    address: Address,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let key = address.to_fixed_bytes();
    if let Some(v) = find_data_by_history(tx, &key, timestamp).await? {
        return Ok(Some(v));
    }

    txutil::get_one::<_, tables::PlainState>(tx, &key).await
}

pub async fn get_storage_as_of<'tx, Tx: Transaction<'tx>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    key: Hash,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let key = plain_generate_composite_storage_key(address, incarnation, key);
    if let Some(v) = find_storage_by_history(tx, &key, timestamp).await? {
        return Ok(Some(v));
    }

    txutil::get_one::<_, tables::PlainState>(tx, &key).await
}

pub async fn find_data_by_history<'tx, Tx: Transaction<'tx>>(
    tx: &'tx Tx,
    key: &[u8; ADDRESS_LENGTH],
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let mut ch = tx.cursor::<tables::AccountsHistory>().await?;
    if let Some((k, v)) = ch.seek(&index_chunk_key(key, timestamp)).await? {
        if k.starts_with(key) {
            let change_set_block = RoaringTreemap::deserialize_from(&*v)?
                .into_iter()
                .find(|n| *n >= timestamp);

            let data = {
                if let Some(change_set_block) = change_set_block {
                    let data = {
                        type B = tables::PlainAccountChangeSet;
                        let mut c = tx.cursor_dup_sort::<B>().await?;
                        B::find(&mut c, change_set_block, key).await?
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
                if acc.incarnation > 0 && acc.is_empty_code_hash() {
                    if let Some(code_hash) = txutil::get_one::<_, tables::PlainContractCode>(
                        tx,
                        &dbutils::plain_generate_storage_prefix(key, acc.incarnation),
                    )
                    .await?
                    {
                        acc.code_hash = H256(*array_ref![&*code_hash, 0, 32]);
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

pub async fn find_storage_by_history<'tx, Tx: Transaction<'tx>>(
    tx: &'tx Tx,
    key: &PlainCompositeStorageKey,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let mut ch = tx.cursor::<tables::StorageHistory>().await?;
    if let Some((k, v)) = ch.seek(&index_chunk_key(key, timestamp)).await? {
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
                    type B = tables::PlainStorageChangeSet;
                    let mut c = tx.cursor_dup_sort::<B>().await?;
                    B::find_with_incarnation(&mut c, change_set_block, key).await?
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
}
