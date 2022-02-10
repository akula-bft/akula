use crate::{
    kv::{mdbx::*, tables},
    models::*,
    u256_to_h256,
};

pub fn seek_storage_key<K>(
    cur: &mut MdbxCursor<'_, K, tables::Storage>,
    address: Address,
    location: U256,
) -> anyhow::Result<Option<U256>>
where
    K: TransactionKind,
{
    let location = u256_to_h256(location);
    if let Some((l, v)) = cur.seek_both_range(address, location)? {
        if l == location {
            return Ok(Some(v));
        }
    }

    Ok(None)
}

pub fn upsert_storage_value(
    cur: &mut MdbxCursor<'_, RW, tables::Storage>,
    address: Address,
    location: U256,
    value: U256,
) -> anyhow::Result<()> {
    if seek_storage_key(cur, address, location)?.is_some() {
        cur.delete_current()?;
    }

    if value != 0 {
        cur.upsert(address, (u256_to_h256(location), value))?;
    }

    Ok(())
}

pub fn seek_hashed_storage_key<K>(
    cur: &mut MdbxCursor<'_, K, tables::HashedStorage>,
    hashed_address: H256,
    hashed_location: H256,
) -> anyhow::Result<Option<U256>>
where
    K: TransactionKind,
{
    Ok(cur
        .seek_both_range(hashed_address, hashed_location)?
        .filter(|&(l, _)| l == hashed_location)
        .map(|(_, v)| v))
}

pub fn upsert_hashed_storage_value(
    cur: &mut MdbxCursor<'_, RW, tables::HashedStorage>,
    hashed_address: H256,
    hashed_location: H256,
    value: U256,
) -> anyhow::Result<()> {
    if seek_hashed_storage_key(cur, hashed_address, hashed_location)?.is_some() {
        cur.delete_current()?;
    }

    if value != 0 {
        cur.upsert(hashed_address, (hashed_location, value))?;
    }

    Ok(())
}
