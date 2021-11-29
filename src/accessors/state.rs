use crate::{kv::tables, models::*, Transaction};
use ethereum_types::*;

pub mod storage {
    use super::*;
    use crate::{h256_to_u256, u256_to_h256};

    pub async fn read<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        address: Address,
        incarnation: Incarnation,
        location: U256,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<U256> {
        let location = u256_to_h256(location);
        if let Some(block_number) = block_number {
            return Ok(crate::find_storage_by_history(
                tx,
                address,
                incarnation,
                location,
                block_number,
            )
            .await?
            .map(h256_to_u256)
            .unwrap_or_default());
        }

        Ok(
            crate::read_account_storage(tx, address, incarnation, location)
                .await?
                .map(h256_to_u256)
                .unwrap_or_default(),
        )
    }
}

pub async fn read_previous_incarnation<'db, Tx: Transaction<'db>>(
    txn: &Tx,
    address: Address,
    block_num: Option<BlockNumber>,
) -> anyhow::Result<Option<Incarnation>> {
    if block_num.is_some() {
        // TODO
        return Ok(None);
    }

    txn.get(&tables::IncarnationMap, address).await
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        h256_to_u256,
        kv::{
            tables::{self, PlainStateFusedValue},
            traits::{MutableKV, MutableTransaction},
        },
        new_mem_database, DEFAULT_INCARNATION,
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

        let val1 = H256(hex!(
            "00000000000000000000000000000000000000000000000000000000c9b131a4"
        ));
        let val2 = H256(hex!(
            "000000000000000000000000000000000000000000005666856076ebaf477f07"
        ));
        let val3 = H256(hex!(
            "4400000000000000000000000000000000000000000000000000000000000000"
        ));

        txn.set(
            &tables::PlainState,
            PlainStateFusedValue::Storage {
                address,
                location: loc1,
                incarnation: DEFAULT_INCARNATION,
                value: val1,
            },
        )
        .await
        .unwrap();
        txn.set(
            &tables::PlainState,
            PlainStateFusedValue::Storage {
                address,
                location: loc2,
                incarnation: DEFAULT_INCARNATION,
                value: val2,
            },
        )
        .await
        .unwrap();
        txn.set(
            &tables::PlainState,
            PlainStateFusedValue::Storage {
                address,
                location: loc3,
                incarnation: DEFAULT_INCARNATION,
                value: val3,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            super::storage::read(&txn, address, DEFAULT_INCARNATION, h256_to_u256(loc1), None)
                .await
                .unwrap(),
            h256_to_u256(val1)
        );
        assert_eq!(
            super::storage::read(&txn, address, DEFAULT_INCARNATION, h256_to_u256(loc2), None)
                .await
                .unwrap(),
            h256_to_u256(val2)
        );
        assert_eq!(
            super::storage::read(&txn, address, DEFAULT_INCARNATION, h256_to_u256(loc3), None)
                .await
                .unwrap(),
            h256_to_u256(val3)
        );
        assert_eq!(
            super::storage::read(&txn, address, DEFAULT_INCARNATION, h256_to_u256(loc4), None)
                .await
                .unwrap(),
            0.into()
        );
    }
}
