use crate::{
    kv::{mdbx::*, tables, traits::*},
    models::*,
};

pub mod account {
    use super::*;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        address_to_find: Address,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<Option<Account>> {
        if let Some(block_number) = block_number {
            if let Some(block_number) = super::history_index::find_next_block(
                tx,
                tables::AccountHistory,
                address_to_find,
                block_number,
            )? {
                return Ok(tx
                    .cursor(tables::AccountChangeSet)?
                    .find_account(block_number, address_to_find)?
                    .flatten());
            }
        }

        tx.get(tables::Account, address_to_find)
    }

    impl<'tx, K: TransactionKind> MdbxCursor<'tx, K, tables::AccountChangeSet> {
        pub fn find_account(
            &mut self,
            block_number: BlockNumber,
            address_to_find: Address,
        ) -> anyhow::Result<Option<Option<Account>>> {
            if let Some(tables::AccountChange { address, account }) =
                self.seek_both_range(block_number, address_to_find)?
            {
                if address == address_to_find {
                    return Ok(Some(account));
                }
            }

            Ok(None)
        }
    }
}

pub mod storage {
    use super::*;
    use crate::u256_to_h256;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
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
            )? {
                if let Some(tables::StorageChange { location, value }) =
                    tx.cursor(tables::StorageChangeSet)?.seek_both_range(
                        tables::StorageChangeKey {
                            block_number,
                            address,
                        },
                        location_to_find,
                    )?
                {
                    if location == location_to_find {
                        return Ok(value);
                    }
                }
            }
        }

        Ok(tx
            .cursor(tables::Storage)?
            .seek_both_range(address, location_to_find)?
            .filter(|&(l, _)| l == location_to_find)
            .map(|(_, v)| v)
            .unwrap_or(U256::ZERO))
    }
}

pub mod history_index {
    use super::*;
    use crate::kv::{mdbx::MdbxTransaction, tables::BitmapKey};
    use croaring::Treemap as RoaringTreemap;

    pub fn find_next_block<'db: 'tx, 'tx, K, TK, E, H>(
        tx: &'tx MdbxTransaction<'db, TK, E>,
        table: H,
        needle: K,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<BlockNumber>>
    where
        H: Table<Key = BitmapKey<K>, Value = RoaringTreemap, SeekKey = BitmapKey<K>>,
        BitmapKey<K>: TableObject,
        K: Copy + PartialEq,
        TK: TransactionKind,
        E: EnvironmentKind,
    {
        let mut ch = tx.cursor(table)?;
        if let Some((index_key, change_blocks)) = ch.seek(BitmapKey {
            inner: needle,
            block_number,
        })? {
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

    #[test]
    fn read_storage() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().unwrap();

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

        txn.set(tables::Storage, address, (loc1, val1)).unwrap();
        txn.set(tables::Storage, address, (loc2, val2)).unwrap();
        txn.set(tables::Storage, address, (loc3, val3)).unwrap();

        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc1), None).unwrap(),
            val1
        );
        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc2), None).unwrap(),
            val2
        );
        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc3), None).unwrap(),
            val3
        );
        assert_eq!(
            super::storage::read(&txn, address, h256_to_u256(loc4), None).unwrap(),
            0.as_u256()
        );
    }
}
