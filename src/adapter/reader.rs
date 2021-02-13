use crate::{changeset::*, common, dbutils::*, models::*, txutil, Cursor, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum::Header;
use ethereum_types::{Address, H256, U256};
use futures::stream::LocalBoxStream;
use std::collections::{HashMap, HashSet};
use tokio::pin;
use tokio_stream::Stream;
use tracing::*;

pub struct StateReader<'tx, Tx: ?Sized> {
    account_reads: HashSet<Address>,
    storage_reads: HashMap<Address, HashSet<H256>>,
    code_reads: HashSet<Address>,
    block_nr: u64,
    tx: &'tx Tx,
    // storage: HashMap<Address, LLRB>,
}

impl<'tx, Tx: ?Sized> StateReader<'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_nr: u64) -> Self {
        Self {
            block_nr,
            tx,

            account_reads: Default::default(),
            storage_reads: Default::default(),
            code_reads: Default::default(),
        }
    }

    pub fn read_account_data(&mut self, address: Address) -> anyhow::Result<Option<Account>> {
        self.account_reads.insert(address);

        bail!("TODO")
    }
}
