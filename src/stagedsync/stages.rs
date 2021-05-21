use std::fmt::Display;

use crate::{common, dbutils::*, tables, MutableTransaction, Transaction};
use anyhow::Context;
use arrayref::array_ref;
use tracing::*;

#[derive(Clone, Copy, Debug)]
pub struct SyncStage(pub &'static str);

pub const HEADERS: SyncStage = SyncStage("Headers");
pub const BLOCK_HASHES: SyncStage = SyncStage("BlockHashes");
pub const BODIES: SyncStage = SyncStage("Bodies");
pub const SENDERS: SyncStage = SyncStage("Senders");
pub const EXECUTION: SyncStage = SyncStage("Execution");
pub const INTERMEDIATE_HASHES: SyncStage = SyncStage("IntermediateHashes");
pub const HASH_STATE: SyncStage = SyncStage("HashState");
pub const ACCOUNT_HISTORY_INDEX: SyncStage = SyncStage("AccountHistoryIndex");
pub const STORAGE_HISTORY_INDEX: SyncStage = SyncStage("StorageHistoryIndex");
pub const LOG_INDEX: SyncStage = SyncStage("LogIndex");
pub const CALL_TRACES: SyncStage = SyncStage("CallTraces");
pub const TX_LOOKUP: SyncStage = SyncStage("TxLookup");
pub const TX_POOL: SyncStage = SyncStage("TxPool");
pub const FINISH: SyncStage = SyncStage("Finish");

impl AsRef<str> for SyncStage {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl AsRef<[u8]> for SyncStage {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Display for SyncStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl SyncStage {
    #[instrument]
    async fn get<'db, Tx: Transaction<'db>, T: Table>(
        &self,
        tx: &Tx,
    ) -> anyhow::Result<Option<u64>> {
        if let Some(b) = tx.get::<T>(self.as_ref()).await? {
            return Ok(Some(u64::from_be_bytes(*array_ref![
                b.get(0..common::BLOCK_NUMBER_LENGTH)
                    .context("failed to read block number from bytes")?,
                0,
                common::BLOCK_NUMBER_LENGTH
            ])));
        }

        Ok(None)
    }

    #[instrument]
    async fn save<'db, RwTx: MutableTransaction<'db>, T: Table>(
        &self,
        tx: &RwTx,
        block: u64,
    ) -> anyhow::Result<()> {
        tx.set::<T>(self.as_ref(), &block.to_be_bytes()).await
    }

    #[instrument]
    pub async fn get_progress<'db, Tx: Transaction<'db>>(
        &self,
        tx: &Tx,
    ) -> anyhow::Result<Option<u64>> {
        self.get::<Tx, tables::SyncStageProgress>(tx).await
    }

    #[instrument]
    pub async fn save_progress<'db, RwTx: MutableTransaction<'db>>(
        &self,
        tx: &RwTx,
        block: u64,
    ) -> anyhow::Result<()> {
        self.save::<RwTx, tables::SyncStageProgress>(tx, block)
            .await
    }

    #[instrument]
    pub async fn get_unwind<'db, Tx: Transaction<'db>>(
        &self,
        tx: &Tx,
    ) -> anyhow::Result<Option<u64>> {
        self.get::<Tx, tables::SyncStageUnwind>(tx).await
    }

    #[instrument]
    pub async fn save_unwind<'db, RwTx: MutableTransaction<'db>>(
        &self,
        tx: &RwTx,
        block: u64,
    ) -> anyhow::Result<()> {
        self.save::<RwTx, tables::SyncStageUnwind>(tx, block).await
    }
}
