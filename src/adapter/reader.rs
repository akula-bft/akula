use crate::{common, dbutils, models::*, tables::*, Transaction};
use bytes::Bytes;
use dbutils::plain_generate_composite_storage_key;
use ethereum_types::Address;

pub struct StateReader<'tx, Tx: Transaction<'tx> + ?Sized> {
    block_nr: u64,
    tx: &'tx Tx,
}

impl<'tx, Tx: Transaction<'tx> + ?Sized> StateReader<'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_nr: u64) -> Self {
        Self { block_nr, tx }
    }

    pub async fn read_account_data(&mut self, address: Address) -> anyhow::Result<Option<Account>> {
        if let Some(enc) =
            crate::state::get_account_data_as_of(self.tx, address, self.block_nr + 1).await?
        {
            return Account::decode_for_storage(&enc);
        }

        Ok(None)
    }

    pub async fn read_account_storage(
        &mut self,
        address: Address,
        incarnation: common::Incarnation,
        key: common::Hash,
    ) -> anyhow::Result<Option<Bytes<'tx>>> {
        let composite_key = plain_generate_composite_storage_key(address, incarnation, key);
        self.tx.get_one::<PlainState>(&composite_key).await
    }
}
