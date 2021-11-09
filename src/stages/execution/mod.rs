use crate::{
    accessors,
    consensus::engine_factory,
    execution::{analysis_cache::AnalysisCache, processor::ExecutionProcessor},
    kv::tables,
    models::*,
    stagedsync::{
        format_duration,
        stage::{ExecOutput, Stage, StageInput},
        stages::EXECUTION,
    },
    Buffer, Cursor, MutableTransaction,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::time::{Duration, Instant};
use tracing::*;

#[derive(Debug)]
pub struct Execution {
    pub batch_size: u64,
    pub commit_every: Option<Duration>,
    pub prune_from: BlockNumber,
}

#[allow(clippy::too_many_arguments)]
async fn execute_batch_of_blocks<'db, Tx: MutableTransaction<'db>>(
    tx: &Tx,
    chain_config: ChainConfig,
    max_block: BlockNumber,
    batch_size: u64,
    commit_every: Option<Duration>,
    starting_block: BlockNumber,
    first_started_at: (Instant, Option<BlockNumber>),
    prune_from: BlockNumber,
) -> anyhow::Result<BlockNumber> {
    let mut buffer = Buffer::new(tx, prune_from, None);
    let mut consensus_engine = engine_factory(chain_config.clone())?;
    let mut analysis_cache = AnalysisCache::default();

    let mut block_number = starting_block;
    let mut gas_since_start = 0;
    let mut gas_since_last_message = 0;
    let batch_started_at = Instant::now();
    let first_started_at_gas = tx
        .get(
            &tables::CumulativeIndex,
            first_started_at.1.unwrap_or(BlockNumber(0)),
        )
        .await?
        .unwrap()
        .gas;
    let mut last_message = Instant::now();
    let mut printed_at_least_once = false;
    loop {
        let block_hash = accessors::chain::canonical_hash::read(tx, block_number)
            .await?
            .ok_or_else(|| anyhow!("No canonical hash found for block {}", block_number))?;
        let header = accessors::chain::header::read(tx, block_hash, block_number)
            .await?
            .ok_or_else(|| anyhow!("Header not found: {}/{:?}", block_number, block_hash))?
            .into();
        let block = accessors::chain::block_body::read_with_senders(tx, block_hash, block_number)
            .await?
            .ok_or_else(|| anyhow!("Block body not found: {}/{:?}", block_number, block_hash))?;

        ExecutionProcessor::new(
            &mut buffer,
            &mut analysis_cache,
            &mut *consensus_engine,
            &header,
            &block,
            &chain_config,
        )
        .execute_and_write_block()
        .await
        .with_context(|| {
            format!(
                "Failed to execute block #{} ({:?})",
                block_number, block_hash
            )
        })?;

        gas_since_start += header.gas_used;
        gas_since_last_message += header.gas_used;

        let now = Instant::now();

        let stage_complete = block_number == max_block;

        let end_of_batch = stage_complete
            || gas_since_start > batch_size
            || commit_every
                .map(|commit_every| now - batch_started_at > commit_every)
                .unwrap_or(false);

        let elapsed = now - last_message;
        if elapsed > Duration::from_secs(30) || (end_of_batch && !printed_at_least_once) {
            let current_total_gas = tx
                .get(&tables::CumulativeIndex, block_number)
                .await?
                .unwrap()
                .gas;

            let total_gas = tx
                .cursor(&tables::CumulativeIndex)
                .await?
                .last()
                .await?
                .unwrap()
                .1
                .gas;
            let mgas_sec = gas_since_last_message as f64
                / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                / 1_000_000f64;
            info!(
                "Executed block {}, Mgas/sec: {:.2}{}",
                block_number,
                mgas_sec,
                if stage_complete {
                    String::new()
                } else {
                    format!(
                        ", progress: {:.2}%, {} remaining",
                        (current_total_gas as f64 / total_gas as f64) * 100_f64,
                        format_duration(
                            (now - first_started_at.0)
                                * ((total_gas - current_total_gas) as f64
                                    / (current_total_gas - first_started_at_gas) as f64)
                                    as u32
                        )
                    )
                }
            );
            printed_at_least_once = true;
            last_message = now;
            gas_since_last_message = 0;
        }

        if end_of_batch {
            break;
        }

        block_number.0 += 1;
    }

    buffer.write_to_db().await?;

    Ok(block_number)
}

#[async_trait]
impl<'db, RwTx: MutableTransaction<'db>> Stage<'db, RwTx> for Execution {
    fn id(&self) -> crate::StageId {
        EXECUTION
    }

    fn description(&self) -> &'static str {
        "Execution of blocks through EVM"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let _ = tx;

        let genesis_hash = tx
            .get(&tables::CanonicalHeader, BlockNumber(0))
            .await?
            .ok_or_else(|| anyhow!("Genesis block absent"))?;
        let chain_config = tx
            .get(&tables::Config, genesis_hash)
            .await?
            .ok_or_else(|| anyhow!("No chain config for genesis block {:?}", genesis_hash))?;

        let starting_block = input.stage_progress.unwrap_or_default() + 1;
        let max_block = input
            .previous_stage.ok_or_else(|| anyhow!("Execution stage cannot be executed first, but no previous stage progress specified"))?.1;

        Ok(if max_block > starting_block {
            let executed_to = execute_batch_of_blocks(
                tx,
                chain_config,
                max_block,
                self.batch_size,
                self.commit_every,
                starting_block,
                input.first_started_at,
                self.prune_from,
            )
            .await?;

            let done = executed_to == max_block;

            ExecOutput::Progress {
                stage_progress: executed_to,
                done,
                must_commit: true,
            }
        } else {
            ExecOutput::Progress {
                stage_progress: max_block,
                done: true,
                must_commit: false,
            }
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: crate::stagedsync::stage::UnwindInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let _ = tx;
        let _ = input;
        todo!()
    }
}
