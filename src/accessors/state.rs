use crate::{
    kv::{tables, traits::*},
    models::*,
};

pub mod account {
    use super::*;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        address_to_find: Address,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<Option<Account>> {
        if let Some(block_number) = block_number {
            if let Some(block_number) = super::history_index::find_next_block(
                tx,
                tables::AccountHistory,
                address_to_find,
                block_number,
            )
            .await?
            {
                if let Some(tables::AccountChange { address, account }) = tx
                    .cursor_dup_sort(tables::AccountChangeSet)
                    .await?
                    .seek_both_range(block_number, address_to_find)
                    .await?
                {
                    if address == address_to_find {
                        return Ok(account);
                    }
                }
            }
        }

        tx.get(tables::Account, address_to_find).await
    }
}

pub mod storage {
    use super::*;
    use crate::u256_to_h256;

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        address: Address,
        location_to_find: U256,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<U256> {
        let location_to_find = u256_to_h256(location_to_find);
        if let Some(block_number) = block_number {
            if let Some(block_number) = super::history_index::find_next_block(
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
                        return Ok(value);
                    }
                }
            }
        }

        Ok(crate::read_account_storage(tx, address, location_to_find)
            .await?
            .unwrap_or_default())
    }
}

pub mod history_index {
    use super::*;
    use crate::kv::tables::BitmapKey;
    use croaring::Treemap as RoaringTreemap;

    pub async fn find_next_block<'db: 'tx, 'tx, Tx: Transaction<'db>, K, H>(
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
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        h256_to_u256,
        kv::{new_mem_database, tables},
    };
    use hex_literal::hex;

    #[tokio::test]
    async fn read_storage() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address = hex!("b000000000000000000000000000000000000008").into();

        let loc1 = hex!("000000000000000000000000000000000000a000000000000000000000000037").into();
        let loc2 = hex!("0000000000000000000000000000000000000000000000000000000000000000").into();
        let loc3 = hex!("ff00000000000000000000000000000000000000000000000000000000000017").into();
        let loc4: H256 =
            hex!("00000000000000000000000000000000000000000000000000000000000f3128").into();

        let val1 = 0xc9b131a4_u128.into();
        let val2 = 0x5666856076ebaf477f07_u128.into();
        let val3 = h256_to_u256(H256(hex!(
            "4400000000000000000000000000000000000000000000000000000000000000"
        )));

        txn.set(tables::Storage, address, (loc1, val1))
            .await
            .unwrap();
        txn.set(tables::Storage, address, (loc2, val2))
            .await
            .unwrap();
        txn.set(tables::Storage, address, (loc3, val3))
            .await
            .unwrap();

        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc1), None)
                .await
                .unwrap(),
            val1
        );
        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc2), None)
                .await
                .unwrap(),
            val2
        );
        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc3), None)
                .await
                .unwrap(),
            val3
        );
        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc4), None)
                .await
                .unwrap(),
            0.as_u256()
        );
    }
}
