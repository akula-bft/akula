use crate::{
    accessors,
    consensus::engine_factory,
    execution::processor::ExecutionProcessor,
    kv::tables,
    models::*,
    stagedsync::{
        stage::{ExecOutput, Stage, StageInput},
        stages::EXECUTION,
    },
    state::State,
    Buffer, MutableTransaction,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::time::{Duration, Instant};
use tracing::*;

#[derive(Debug)]
pub struct Execution {
    pub batch_size: usize,
    pub prune_from: BlockNumber,
}

async fn execute_batch_of_blocks<'db, Tx: MutableTransaction<'db>>(
    tx: &Tx,
    chain_config: ChainConfig,
    max_block: BlockNumber,
    batch_size: usize,
    starting_block: BlockNumber,
    prune_from: BlockNumber,
) -> anyhow::Result<BlockNumber> {
    let mut buffer = Buffer::new(tx, prune_from, None);
    let mut consensus_engine = engine_factory(chain_config.clone())?;

    let mut block_number = starting_block;
    let mut gas_since_last_message = 0;
    let mut last_message = Instant::now();
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

        let receipts = ExecutionProcessor::new(
            &mut buffer,
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

        gas_since_last_message += header.gas_used;

        let now = Instant::now();
        let elapsed = now - last_message;
        if elapsed > Duration::from_secs(5) {
            let mgas_sec = gas_since_last_message as f64
                / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                / 1_000_000f64;
            info!("Executed block {}, Mgas/sec: {}", block_number, mgas_sec);
            last_message = now;
            gas_since_last_message = 0;
        }

        // TODO: implement pruning
        buffer.insert_receipts(block_number, receipts).await?;

        if block_number == max_block
            || block_number.0 - starting_block.0 == u64::try_from(batch_size)?
        {
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
                starting_block,
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
