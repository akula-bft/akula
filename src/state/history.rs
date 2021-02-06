use crate::{changeset::*, common, dbutils, dbutils::*, models::*, Cursor, Transaction};
use arrayref::array_ref;
use bytes::Bytes;
use common::{Hash, Incarnation, ADDRESS_LENGTH};
use ethereum_types::{Address, H256};
use roaring::RoaringTreemap;

pub async fn get_account_data_as_of<Tx: Transaction>(
    tx: &Tx,
    address: Address,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'static>>> {
    let key = address.to_fixed_bytes();
    if let Some(v) = find_data_by_history(tx, &key, timestamp).await? {
        return Ok(Some(v));
    }

    let v = tx.get_one(buckets::PlainState::DB_NAME, &key).await?;

    if v.is_empty() {
        return Ok(None);
    }

    Ok(Some(v))
}

pub async fn get_storage_as_of<Tx: Transaction>(
    tx: &Tx,
    address: Address,
    incarnation: Incarnation,
    key: Hash,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'static>>> {
    let key = plain_generate_composite_storage_key(address, incarnation, key);
    if let Some(v) = find_storage_by_history(tx, &key, timestamp).await? {
        return Ok(Some(v));
    }

    let v = tx.get_one(buckets::PlainState::DB_NAME, &key).await?;

    if v.is_empty() {
        return Ok(None);
    }

    Ok(Some(v))
}

pub async fn find_data_by_history<Tx: Transaction>(
    tx: &Tx,
    key: &[u8; ADDRESS_LENGTH],
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'static>>> {
    let mut ch = tx.cursor(buckets::AccountsHistory::DB_NAME).await?;
    let (k, v) = ch.seek(&index_chunk_key(key, timestamp)).await?;

    if k.is_empty() {
        return Ok(None);
    }

    if !k.starts_with(key) {
        return Ok(None);
    }
    let change_set_block = RoaringTreemap::deserialize_from(&*v)?
        .into_iter()
        .find(|n| *n >= timestamp);

    let data = {
        if let Some(change_set_block) = change_set_block {
            let data = {
                type B = buckets::PlainAccountChangeSet;
                let mut c = tx.cursor_dup_sort(B::DB_NAME).await?;
                B::walker_adapter(&mut c)
                    .find(change_set_block, key)
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

    //restore codehash
    if let Some(mut acc) = Account::decode_for_storage(&*data)? {
        if acc.incarnation > 0 && acc.is_empty_code_hash() {
            let code_hash = tx
                .get_one(
                    buckets::PlainContractCode.as_ref(),
                    &dbutils::plain_generate_storage_prefix(key, acc.incarnation),
                )
                .await?;

            if !code_hash.is_empty() {
                acc.code_hash = H256(*array_ref![&*code_hash, 0, 32]);
            }

            let mut data = vec![0; acc.encoding_length_for_storage()];
            acc.encode_for_storage(&mut data);

            return Ok(Some(data.into()));
        }
    }

    Ok(Some(data))
}

pub async fn find_storage_by_history<Tx: Transaction>(
    tx: &Tx,
    key: &PlainCompositeStorageKey,
    timestamp: u64,
) -> anyhow::Result<Option<Bytes<'static>>> {
    let mut ch = tx.cursor(buckets::StorageHistory::DB_NAME).await?;
    let (k, v) = ch.seek(&index_chunk_key(key, timestamp)).await?;

    if k.is_empty() {
        return Ok(None);
    }

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
                type B = buckets::PlainStorageChangeSet;
                let mut c = tx.cursor_dup_sort(B::DB_NAME).await?;
                B::walker_adapter(&mut c)
                    .find_with_incarnation(change_set_block, key)
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

    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;
}
