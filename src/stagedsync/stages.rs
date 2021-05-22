use crate::{common, kv::*, MutableTransaction, Transaction};
use anyhow::Context;
use arrayref::array_ref;
use std::fmt::Display;
use tracing::*;

#[derive(Clone, Copy, Debug)]
pub struct StageId(pub &'static str);

pub const HEADERS: StageId = StageId("Headers");
pub const BLOCK_HASHES: StageId = StageId("BlockHashes");
pub const BODIES: StageId = StageId("Bodies");
pub const SENDERS: StageId = StageId("Senders");
pub const EXECUTION: StageId = StageId("Execution");
pub const INTERMEDIATE_HASHES: StageId = StageId("IntermediateHashes");
pub const HASH_STATE: StageId = StageId("HashState");
pub const ACCOUNT_HISTORY_INDEX: StageId = StageId("AccountHistoryIndex");
pub const STORAGE_HISTORY_INDEX: StageId = StageId("StorageHistoryIndex");
pub const LOG_INDEX: StageId = StageId("LogIndex");
pub const CALL_TRACES: StageId = StageId("CallTraces");
pub const TX_LOOKUP: StageId = StageId("TxLookup");
pub const TX_POOL: StageId = StageId("TxPool");
pub const FINISH: StageId = StageId("Finish");

impl AsRef<str> for StageId {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl AsRef<[u8]> for StageId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StageId {
    #[instrument]
    async fn get<'db, Tx: Transaction<'db>, T: Table>(
        &self,
        tx: &Tx,
        table: &T,
    ) -> anyhow::Result<Option<u64>> {
        if let Some(b) = tx.get(table, self.as_ref()).await? {
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
        table: &T,
        block: u64,
    ) -> anyhow::Result<()> {
        tx.set(table, self.as_ref(), &block.to_be_bytes()).await
    }

    #[instrument]
    pub async fn get_progress<'db, Tx: Transaction<'db>>(
        &self,
        tx: &Tx,
    ) -> anyhow::Result<Option<u64>> {
        self.get(tx, &tables::SyncStage).await
    }

    #[instrument]
    pub async fn save_progress<'db, RwTx: MutableTransaction<'db>>(
        &self,
        tx: &RwTx,
        block: u64,
    ) -> anyhow::Result<()> {
        self.save(tx, &tables::SyncStage, block).await
    }

    #[instrument]
    pub async fn get_unwind<'db, Tx: Transaction<'db>>(
        &self,
        tx: &Tx,
    ) -> anyhow::Result<Option<u64>> {
        self.get(tx, &tables::SyncStageUnwind).await
    }

    #[instrument]
    pub async fn save_unwind<'db, RwTx: MutableTransaction<'db>>(
        &self,
        tx: &RwTx,
        block: u64,
    ) -> anyhow::Result<()> {
        self.save(tx, &tables::SyncStageUnwind, block).await
    }
}
