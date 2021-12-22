use crate::{
    kv::{
        tables::BitmapKey,
        traits::{Transaction, *},
        *,
    },
    models::*,
    read_account_storage,
};
use croaring::Treemap as RoaringTreemap;
use ethereum_types::*;

pub async fn get_account_data_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    timestamp: BlockNumber,
) -> anyhow::Result<Option<Account>> {
    if let Some(v) = find_account_by_history(tx, address, timestamp).await? {
        return Ok(v);
    }

    tx.get(tables::Account, address).await
}

pub async fn get_storage_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    location: H256,
    block_number: impl Into<BlockNumber>,
) -> anyhow::Result<Option<U256>> {
    if let Some(v) = find_storage_by_history(tx, address, location, block_number.into()).await? {
        return Ok(Some(v));
    }

    read_account_storage(tx, address, location).await
}

async fn find_next_block_in_history_index<'db: 'tx, 'tx, Tx: Transaction<'db>, K, H>(
    tx: &'tx Tx,
    table: H,
    needle: K,
    block_number: BlockNumber,
) -> anyhow::Result<Option<BlockNumber>>
where
    H: Table<Key = BitmapKey<K>, Value = RoaringTreemap, SeekKey = BitmapKey<K>>,
    BitmapKey<K>: TableObject,
    K: Copy + PartialEq,
{
    let mut ch = tx.cursor(table).await?;
    if let Some((index_key, change_blocks)) = ch
        .seek(BitmapKey {
            inner: needle,
            block_number,
        })
        .await?
    {
        if index_key.inner == needle {
            return Ok(change_blocks
                .iter()
                .find(|&change_block| *block_number < change_block)
                .map(BlockNumber));
        }
    }

    Ok(None)
}

pub async fn find_account_by_history<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address_to_find: Address,
    block_number: BlockNumber,
) -> anyhow::Result<Option<Option<Account>>> {
    if let Some(block_number) =
        find_next_block_in_history_index(tx, tables::AccountHistory, address_to_find, block_number)
            .await?
    {
        if let Some(tables::AccountChange { address, account }) = tx
            .cursor_dup_sort(tables::AccountChangeSet)
            .await?
            .seek_both_range(block_number, address_to_find)
            .await?
        {
            if address == address_to_find {
                return Ok(Some(account));
            }
        }
    }

    Ok(None)
}

pub async fn find_storage_by_history<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    location_to_find: H256,
    block_number: BlockNumber,
) -> anyhow::Result<Option<U256>> {
    if let Some(block_number) = find_next_block_in_history_index(
        tx,
        tables::StorageHistory,
        (address, location_to_find),
        block_number,
    )
    .await?
    {
        if let Some(tables::StorageChange { location, value }) = tx
            .cursor_dup_sort(tables::StorageChangeSet)
            .await?
            .seek_both_range(
                tables::StorageChangeKey {
                    block_number,
                    address,
                },
                location_to_find,
            )
            .await?
        {
            if location == location_to_find {
                return Ok(Some(value));
            }
        }
    }

    Ok(None)
}
