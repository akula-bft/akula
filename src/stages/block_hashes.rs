use crate::{
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::{tables, traits::TableEncode},
    models::*,
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    Cursor, MutableTransaction, StageId,
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
        let past_progress = input.stage_progress.unwrap_or(BlockNumber(0));

        let mut bodies_cursor = tx.mutable_cursor(&tables::BlockBody).await?;
        let mut blockhashes_cursor = tx.mutable_cursor(&tables::HeaderNumber.erased()).await?;
        let processed = BlockNumber(0);

        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);
        let walker = bodies_cursor.walk(Some(past_progress));
        pin!(walker);

        while let Some(((block_number, block_hash), _)) = walker.try_next().await? {
            // BlockBody Key is block_number + hash, so we just separate and collect
            collector.collect(Entry {
                key: block_hash.encode().to_vec(),
                value: block_number.encode().to_vec(),
                id: 0, // Irrelevant here, could be anything
            });
        }
        collector.load(&mut blockhashes_cursor, None).await?;
        info!("Processed");
        Ok(ExecOutput::Progress {
            stage_progress: processed,
            done: false,
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
