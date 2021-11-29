use crate::{
    h256_to_u256,
    kv::tables::{self, PlainStateFusedValue},
    models::*,
    u256_to_h256, CursorDupSort, MutableCursorDupSort, Transaction,
};
use bytes::Bytes;
use ethereum_types::*;

pub async fn seek_storage_key<'tx, C: CursorDupSort<'tx, tables::PlainState>>(
    cur: &mut C,
    address: Address,
    incarnation: Incarnation,
    location: U256,
) -> anyhow::Result<Option<U256>> {
    let location = u256_to_h256(location);
    if let Some(v) = cur
        .seek_both_range(
            tables::PlainStateKey::Storage(address, incarnation),
            location,
        )
        .await?
    {
        if let Some((a, inc, l, v)) = v.as_storage() {
            if a == address && inc == incarnation && l == location {
                return Ok(Some(h256_to_u256(v)));
            }
        }
    }

    Ok(None)
}

pub async fn upsert_storage_value<'tx, C>(
    cur: &mut C,
    address: Address,
    incarnation: Incarnation,
    location: U256,
    value: U256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, tables::PlainState>,
{
    if seek_storage_key(cur, address, incarnation, location)
        .await?
        .is_some()
    {
        cur.delete_current().await?;
    }

    if !value.is_zero() {
        cur.upsert(PlainStateFusedValue::Storage {
            address,
            incarnation,
            location: u256_to_h256(location),
            value: u256_to_h256(value),
        })
        .await?;
    }

    Ok(())
}

pub async fn seek_hashed_storage_key<'tx, C: CursorDupSort<'tx, tables::HashedStorage>>(
    cur: &mut C,
    hashed_address: H256,
    incarnation: Incarnation,
    hashed_location: H256,
) -> anyhow::Result<Option<H256>> {
    if let Some(((a, inc), (l, v))) = cur
        .seek_both_range((hashed_address, incarnation), hashed_location)
        .await?
    {
        if a == hashed_address && inc == incarnation && l == hashed_location {
            return Ok(Some(*v));
        }
    }

    Ok(None)
}

pub async fn upsert_hashed_storage_value<'tx, C>(
    cur: &mut C,
    hashed_address: H256,
    incarnation: Incarnation,
    hashed_location: H256,
    value: H256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, tables::HashedStorage>,
{
    if seek_hashed_storage_key(cur, hashed_address, incarnation, hashed_location)
        .await?
        .is_some()
    {
        cur.delete_current().await?;
    }

    if !value.is_zero() {
        cur.upsert((
            (hashed_address, incarnation),
            (hashed_location, value.into()),
        ))
        .await?;
    }

    Ok(())
}

pub async fn read_account_data<'db, Tx: Transaction<'db>>(
    tx: &Tx,
    address: Address,
) -> anyhow::Result<Option<Account>> {
    if let Some(encoded) = tx
        .get(&tables::PlainState, tables::PlainStateKey::Account(address))
        .await?
    {
        return Account::decode_for_storage(&*encoded);
    }

    Ok(None)
}

pub async fn read_account_storage<'db, Tx: Transaction<'db>>(
    tx: &Tx,
    address: Address,
    incarnation: Incarnation,
    location: H256,
) -> anyhow::Result<Option<H256>> {
    if let Some(v) = tx
        .cursor_dup_sort(&tables::PlainState)
        .await?
        .seek_both_range(
            tables::PlainStateKey::Storage(address, incarnation),
            location,
        )
        .await?
    {
        if let Some((a, inc, l, v)) = v.as_storage() {
            if a == address && inc == incarnation && l == location {
                return Ok(Some(v));
            }
        }
    }

    Ok(None)
}

pub async fn read_account_code<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    _: Address,
    _: Incarnation,
    code_hash: H256,
) -> anyhow::Result<Option<Bytes>> {
    tx.get(&tables::Code, code_hash).await
}

pub async fn read_account_code_size<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    code_hash: H256,
) -> anyhow::Result<usize> {
    Ok(read_account_code(tx, address, incarnation, code_hash)
        .await?
        .map(|code| code.len())
        .unwrap_or(0))
}

pub async fn read_previous_incarnation<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
) -> anyhow::Result<Option<Incarnation>> {
    Ok(tx.get(&tables::IncarnationMap, address).await?)
}
