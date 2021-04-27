use crate::{common, dbutils::*, tables, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use ethereum::Header;
use ethereum_types::H256;
use tracing::*;

pub async fn get_stage_progress<'tx, Tx: Transaction<'tx>>(
    tx: &'tx Tx,
    stage: SyncStage,
) -> anyhow::Result<Option<u64>> {
    trace!("Reading stage {:?} progress", stage);

    if let Some(b) = tx
        .get_one::<tables::SyncStageProgress>(stage.as_ref())
        .await?
    {
        return Ok(Some(u64::from_be_bytes(*array_ref![
            b.get(0..common::BLOCK_NUMBER_LENGTH)
                .context("failed to read block number from bytes")?,
            0,
            common::BLOCK_NUMBER_LENGTH
        ])));
    }

    Ok(None)
}
