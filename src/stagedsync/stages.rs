use crate::{
    kv::{tables, traits::*},
    models::*,
};
use std::fmt::Display;
use tracing::*;

#[derive(Clone, Copy, Debug)]
pub struct StageId(pub &'static str);

pub const HEADERS: StageId = StageId("Headers");
pub const BLOCK_HASHES: StageId = StageId("BlockHashes");
pub const BODIES: StageId = StageId("Bodies");
pub const SENDERS: StageId = StageId("Senders");
pub const CUMULATIVE_INDEX: StageId = StageId("CumulativeIndex");
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
    pub async fn get_progress<'db, Tx: Transaction<'db>>(
        &self,
        tx: &Tx,
    ) -> anyhow::Result<Option<BlockNumber>> {
        tx.get(tables::SyncStage, *self).await
    }

    #[instrument]
    pub async fn save_progress<'db, RwTx: MutableTransaction<'db>>(
        &self,
        tx: &RwTx,
        block: BlockNumber,
    ) -> anyhow::Result<()> {
        tx.set(tables::SyncStage, *self, block).await
    }
}
