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

    tx.get(&tables::Account, address)
        .await
        .map(|opt| opt.map(From::from))
}

pub async fn get_storage_as_of<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    location: H256,
    block_number: impl Into<BlockNumber>,
) -> anyhow::Result<Option<U256>> {
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
) -> anyhow::Result<Option<U256>> {
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
