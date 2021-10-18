use crate::{
    accessors,
    consensus::engine_factory,
    execution::processor::ExecutionProcessor,
    kv::tables,
    models::*,
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    state::State,
    Buffer, MutableTransaction,
};
use anyhow::anyhow;
use async_trait::async_trait;
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
        .await?;

        if *block_number % 250 == 0 {
            info!("Executed block {}", block_number);
        }

        // TODO: implement pruning
        buffer.insert_receipts(block_number, receipts).await?;

        if block_number == max_block
            || starting_block.0 - block_number.0 == u64::try_from(batch_size)?
        {
            break;
        }

        block_number.0 += 1;
    }

    buffer.write_to_db().await?;

    Ok(max_block)
}

#[async_trait]
impl<'db, RwTx: MutableTransaction<'db>> Stage<'db, RwTx> for Execution {
    fn id(&self) -> crate::StageId {
        todo!()
    }

    fn description(&self) -> &'static str {
        todo!()
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

        Ok(ExecOutput::Progress {
            stage_progress: executed_to,
            done,
            must_commit: true,
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
