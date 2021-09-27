use crate::{
    kv::{traits::CursorDupSort, *},
    models::*,
    Transaction,
};
use ethereum_types::{Address, H256};
use std::marker::PhantomData;

pub struct StateReader<'db: 'tx, 'tx, Tx: Transaction<'db> + ?Sized> {
    block_number: BlockNumber,
    tx: &'tx Tx,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: Transaction<'db> + ?Sized> StateReader<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_number: BlockNumber) -> Self {
        Self {
            block_number,
            tx,
            _marker: PhantomData,
        }
    }

    pub async fn read_account_data(&mut self, address: Address) -> anyhow::Result<Option<Account>> {
        if let Some(enc) =
            crate::state::get_account_data_as_of(self.tx, address, self.block_number + 1).await?
        {
            return Account::decode_for_storage(&enc);
        }

        Ok(None)
    }

    pub async fn read_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        location: H256,
    ) -> anyhow::Result<Option<H256>> {
        self.tx
            .cursor_dup_sort(&tables::PlainState)
            .await?
            .seek_both_range(
                tables::PlainStateKey::Storage(address, incarnation),
                location,
            )
            .await
            .map(|opt| opt.map(|v| v.as_storage().unwrap().3))
    }
}
