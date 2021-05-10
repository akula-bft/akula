use crate::{common, dbutils::*, tables, MutableTransaction, Transaction};
use anyhow::Context;
use arrayref::array_ref;
use tracing::*;

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
