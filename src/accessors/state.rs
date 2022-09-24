use crate::{
    kv::{mdbx::*, tables, traits::*},
    models::*,
};

pub mod account {
    use super::*;
    use crate::kv::tables::BitmapKey;
    use anyhow::format_err;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        address_to_find: Address,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<Option<Account>> {
        let changeset_block = if let Some(block_number) = block_number {
            super::history_index::find_next_block(
                tx,
                tables::AccountHistory,
                address_to_find,
                block_number,
            )?
        } else {
            None
        };

        read_inner(tx, address_to_find, changeset_block)
    }

    fn read_inner<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        address: Address,
        changeset_block: Option<BlockNumber>,
    ) -> anyhow::Result<Option<Account>> {
        if let Some(block_number) = changeset_block {
            Ok(tx
                .cursor(tables::AccountChangeSet)?
                .find_account(block_number, address)?
                .ok_or_else(|| format_err!("changeset does not contain account"))?)
        } else {
            tx.get(tables::Account, address)
        }
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

    pub fn walk<'db, 'tx, K: TransactionKind, E: EnvironmentKind>(
        tx: &'tx MdbxTransaction<'db, K, E>,
        offset: Option<Address>,
        block: Option<BlockNumber>,
    ) -> impl Iterator<Item = anyhow::Result<(Address, Account)>> + 'tx
    where
        'db: 'tx,
    {
        TryGenIter::from(move || {
            if let Some(block_number) = block {
                // Traverse history index and add to set if non-zero at our block

                let mut index = tx
                    .cursor(tables::AccountHistory)?
                    .walk(offset.map(|offset| BitmapKey {
                        inner: offset,
                        block_number: BlockNumber(0),
                    }));

                let mut last_entry = None;

                while let Some((
                    BitmapKey {
                        inner: address,
                        block_number: mut max_bitmap_block,
                    },
                    mut change_blocks,
                )) = index.next().transpose()?
                {
                    if last_entry == Some(address) {
                        continue;
                    }

                    last_entry = Some(address);

                    if *block_number
                        < change_blocks
                            .minimum()
                            .ok_or_else(|| format_err!("Index chunk should not be empty"))?
                    {
                        continue;
                    }

                    while block_number >= max_bitmap_block {
                        (
                            BitmapKey {
                                inner: _,
                                block_number: max_bitmap_block,
                            },
                            change_blocks,
                        ) = index
                            .next()
                            .transpose()?
                            .ok_or_else(|| format_err!("Unexpected end of history index"))?;
                    }

                    let v = crate::accessors::state::account::read_inner(
                        tx,
                        address,
                        change_blocks
                            .iter()
                            .find(|&change_block| *block_number < change_block)
                            .map(BlockNumber),
                    )?;

                    if let Some(account) = v {
                        yield (address, account);
                    }
                }
            } else {
                // Simply traverse the current state
                let mut walker = tx.cursor(tables::Account)?.walk(offset);

                while let Some(v) = walker.next().transpose()? {
                    yield v;
                }
            }

            Ok(())
        })
    }
}

pub mod storage {
    use super::*;
    use crate::{kv::tables::BitmapKey, u256_to_h256};
    use anyhow::format_err;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        address: Address,
        location_to_find: U256,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<U256> {
        let location_to_find = u256_to_h256(location_to_find);

        let changeset_block = if let Some(block_number) = block_number {
            super::history_index::find_next_block(
                tx,
                tables::StorageHistory,
                (address, location_to_find),
                block_number,
            )?
        } else {
            None
        };

        read_inner(tx, address, location_to_find, changeset_block)
    }

    fn read_inner<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        address: Address,
        location_to_find: H256,
        changeset_block: Option<BlockNumber>,
    ) -> anyhow::Result<U256> {
        if let Some(block_number) = changeset_block {
            let tables::StorageChange { location, value } = tx
                .cursor(tables::StorageChangeSet)?
                .seek_both_range(
                    tables::StorageChangeKey {
                        block_number,
                        address,
                    },
                    location_to_find,
                )?
                .ok_or_else(|| format_err!("changeset does not contain storage entry"))?;
            anyhow::ensure!(location == location_to_find);
            Ok(value)
        } else {
            Ok(tx
                .cursor(tables::Storage)?
                .seek_both_range(address, location_to_find)?
                .filter(|&(l, _)| l == location_to_find)
                .map(|(_, v)| v)
                .unwrap_or(U256::ZERO))
        }
    }

    pub fn walk<'db, 'tx, K: TransactionKind, E: EnvironmentKind>(
        tx: &'tx MdbxTransaction<'db, K, E>,
        searched_address: Address,
        offset: Option<H256>,
        block: Option<BlockNumber>,
    ) -> impl Iterator<Item = anyhow::Result<(H256, U256)>> + 'tx
    where
        'db: 'tx,
    {
        TryGenIter::from(move || {
            if let Some(block_number) = block {
                // Traverse history index and add to set if non-zero at our block

                let mut index = tx.cursor(tables::StorageHistory)?.walk(Some(BitmapKey {
                    inner: (searched_address, H256::zero()),
                    block_number: BlockNumber(0),
                }));

                let mut last_entry = None;

                while let Some((
                    BitmapKey {
                        inner: (address, slot),
                        block_number: mut max_bitmap_block,
                    },
                    mut change_blocks,
                )) = index.next().transpose()?
                {
                    if address != searched_address {
                        break;
                    }

                    if last_entry == Some((address, slot)) {
                        continue;
                    }

                    last_entry = Some((address, slot));

                    if *block_number
                        < change_blocks
                            .minimum()
                            .ok_or_else(|| format_err!("Index chunk should not be empty"))?
                    {
                        continue;
                    }

                    while block_number >= max_bitmap_block {
                        (
                            BitmapKey {
                                inner: _,
                                block_number: max_bitmap_block,
                            },
                            change_blocks,
                        ) = index
                            .next()
                            .transpose()?
                            .ok_or_else(|| format_err!("Unexpected end of history index"))?;
                    }

                    let v = crate::accessors::state::storage::read_inner(
                        tx,
                        address,
                        slot,
                        change_blocks
                            .iter()
                            .find(|&change_block| *block_number < change_block)
                            .map(BlockNumber),
                    )?;

                    if v != U256::ZERO {
                        yield (slot, v);
                    }
                }
            } else {
                // Simply traverse the current state
                let mut walker = tx
                    .cursor(tables::Storage)?
                    .walk_dup(searched_address, offset);

                while let Some(v) = walker.next().transpose()? {
                    yield v;
                }
            }

            Ok(())
        })
    }
}

pub mod code {
    use super::*;
    use anyhow::format_err;
    use bytes::Bytes;

    pub fn read<K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, K, E>,
        code_hash: H256,
    ) -> anyhow::Result<Bytes> {
        if code_hash == EMPTY_HASH {
            Ok(Bytes::new())
        } else {
            Ok(tx
                .get(tables::Code, code_hash)?
                .ok_or_else(|| format_err!("code expected but not found"))?)
        }
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
            block_number: block_number + 1,
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
        kv::{
            new_mem_chaindata,
            tables::{self, BitmapKey},
        },
    };
    use hex_literal::hex;

    #[test]
    fn read_storage() {
        let db = new_mem_chaindata().unwrap();
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

    #[test]
    fn find_next_block() {
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();

        let address = hex!("b000000000000000000000000000000000000008").into();

        txn.set(
            tables::AccountHistory,
            BitmapKey {
                inner: address,
                block_number: 20.into(),
            },
            [10, 20].into_iter().collect(),
        )
        .unwrap();

        txn.set(
            tables::AccountHistory,
            BitmapKey {
                inner: address,
                block_number: 50.into(),
            },
            [30, 40, 50].into_iter().collect(),
        )
        .unwrap();

        txn.set(
            tables::AccountHistory,
            BitmapKey {
                inner: address,
                block_number: u64::MAX.into(),
            },
            [60, 70, 80].into_iter().collect(),
        )
        .unwrap();

        for (block, next_block) in [
            (0, Some(10)),
            (5, Some(10)),
            (10, Some(20)),
            (15, Some(20)),
            (20, Some(30)),
            (25, Some(30)),
            (30, Some(40)),
            (35, Some(40)),
            (40, Some(50)),
            (45, Some(50)),
            (50, Some(60)),
            (55, Some(60)),
            (60, Some(70)),
            (65, Some(70)),
            (70, Some(80)),
            (75, Some(80)),
            (80, None),
            (85, None),
            (90, None),
        ] {
            assert_eq!(
                super::history_index::find_next_block(
                    &txn,
                    tables::AccountHistory,
                    address,
                    block.into()
                )
                .unwrap(),
                next_block.map(BlockNumber)
            );
        }
    }
}
