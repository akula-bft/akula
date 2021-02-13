use crate::{buckets, common, dbutils::*, txutil, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use ethereum::Header;
use ethereum_types::H256;
use tracing::*;

pub async fn get_stage_progress<Tx: Transaction>(
    tx: &Tx,
    stage: SyncStage,
) -> anyhow::Result<Option<u64>> {
    trace!("Reading stage {:?} progress", stage);

    let b = txutil::get_one::<_, buckets::SyncStageProgress>(tx, stage.as_ref()).await?;

    if b.is_empty() {
        return Ok(None);
    }

    Ok(Some(u64::from_be_bytes(*array_ref![
        b.get(0..common::BLOCK_NUMBER_LENGTH)
            .context("failed to read block number from bytes")?,
        0,
        common::BLOCK_NUMBER_LENGTH
    ])))
}
