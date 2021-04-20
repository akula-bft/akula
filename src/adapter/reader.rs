use crate::{changeset::*, dbutils::*, models::*, txutil, Cursor, Transaction};
use anyhow::bail;
use ethereum_types::Address;

pub struct StateReader<'tx, Tx: Transaction<'tx> + ?Sized> {
    block_nr: u64,
    tx: &'tx Tx,
}

impl<'tx, Tx: Transaction<'tx> + ?Sized> StateReader<'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_nr: u64) -> Self {
        Self { block_nr, tx }
    }

    pub fn read_account_data(&mut self, address: Address) -> anyhow::Result<Option<Account>> {
        bail!("TODO")
    }
}
