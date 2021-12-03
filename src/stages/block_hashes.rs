use crate::{
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::{tables, traits::*},
    models::*,
    stagedsync::stage::*,
    StageId,
};
use async_trait::async_trait;
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

#[derive(Debug)]
pub struct BlockHashes;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for BlockHashes
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("BlockHashes")
    }

    fn description(&self) -> &'static str {
        "Generating BlockHashes => BlockNumber Mapping"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        let mut bodies_cursor = tx.mutable_cursor(&tables::CanonicalHeader).await?;
        let mut blockhashes_cursor = tx.mutable_cursor(&tables::HeaderNumber.erased()).await?;

        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);
        let walker = bodies_cursor.walk(Some(highest_block + 1));
        pin!(walker);

        while let Some((block_number, block_hash)) = walker.try_next().await? {
            if block_number.0 % 50_000 == 0 {
                info!("Processing block {}", block_number);
            }
            // BlockBody Key is block_number + hash, so we just separate and collect
            collector.collect(Entry::new(block_hash, block_number));

            highest_block = block_number;
        }
        collector.load(&mut blockhashes_cursor).await?;
        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done: true,
            must_commit: highest_block > original_highest_block,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: crate::stagedsync::stage::UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut header_number_cur = tx.mutable_cursor(&tables::HeaderNumber).await?;
        let mut body_cur = tx.mutable_cursor(&tables::CanonicalHeader).await?;

        let mut walker = body_cur.walk_back(None);

        while let Some((block_num, block_hash)) = walker.try_next().await? {
            if block_num > input.unwind_to {
                if header_number_cur.seek(block_hash).await?.is_some() {
                    header_number_cur.delete_current().await?;
                }
            } else {
                break;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
            must_commit: true,
        })
    }
}
