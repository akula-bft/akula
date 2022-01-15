use crate::{
    kv::{tables, traits::*},
    models::*,
    u256_to_h256,
};
use bytes::Bytes;

pub async fn seek_storage_key<'tx, C: CursorDupSort<'tx, tables::Storage>>(
    cur: &mut C,
    address: Address,
    location: U256,
) -> anyhow::Result<Option<U256>> {
    let location = u256_to_h256(location);
    if let Some((l, v)) = cur.seek_both_range(address, location).await? {
        if l == location {
            return Ok(Some(v));
        }
    }

    Ok(None)
}

pub async fn upsert_storage_value<'tx, C>(
    cur: &mut C,
    address: Address,
    location: U256,
    value: U256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, tables::Storage>,
{
    if seek_storage_key(cur, address, location).await?.is_some() {
        cur.delete_current().await?;
    }

    if value != 0 {
        cur.upsert(address, (u256_to_h256(location), value)).await?;
    }

    Ok(())
}

pub async fn seek_hashed_storage_key<'tx, C: CursorDupSort<'tx, tables::HashedStorage>>(
    cur: &mut C,
    hashed_address: H256,
    hashed_location: H256,
) -> anyhow::Result<Option<U256>> {
    Ok(cur
        .seek_both_range(hashed_address, hashed_location)
        .await?
        .filter(|&(l, _)| l == hashed_location)
        .map(|(_, v)| v))
}

pub async fn upsert_hashed_storage_value<'tx, C>(
    cur: &mut C,
    hashed_address: H256,
    hashed_location: H256,
    value: U256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, tables::HashedStorage>,
{
    if seek_hashed_storage_key(cur, hashed_address, hashed_location)
        .await?
        .is_some()
    {
        cur.delete_current().await?;
    }

    if value != 0 {
        cur.upsert(hashed_address, (hashed_location, value)).await?;
    }

    Ok(())
}

pub async fn read_account_storage<'db, Tx: Transaction<'db>>(
    tx: &Tx,
    address: Address,
    location: H256,
) -> anyhow::Result<Option<U256>> {
    Ok(tx
        .cursor_dup_sort(tables::Storage)
        .await?
        .seek_both_range(address, location)
        .await?
        .filter(|&(l, _)| l == location)
        .map(|(_, v)| v))
}

pub async fn read_account_code<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    _: Address,
    code_hash: H256,
) -> anyhow::Result<Option<Bytes>> {
    tx.get(tables::Code, code_hash).await
}

pub async fn read_account_code_size<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    code_hash: H256,
) -> anyhow::Result<usize> {
    Ok(read_account_code(tx, address, code_hash)
        .await?
        .map(|code| code.len())
        .unwrap_or(0))
}
