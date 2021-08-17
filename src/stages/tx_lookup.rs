use crate::{
    common::hash_data,
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::tables,
    models::BodyForStorage,
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    txdb, MutableTransaction, StageId,
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
        StageId("TxLookup")
    }

    fn description(&self) -> &'static str {
        "Generating TransactionHash => BlockNumber Mapping"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let past_progress = input.stage_progress.unwrap_or(0);

        let mut bodies_cursor = tx.mutable_cursor(&tables::BlockBody).await?;
        let mut tx_hash_cursor = tx.mutable_cursor(&tables::TxLookup).await?;

        let start_key = past_progress.to_be_bytes();
        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);
        let walker = txdb::walk(&mut bodies_cursor, &start_key, 0);
        pin!(walker);

        while let Some((block_key, ref body)) = walker.try_next().await? {
            let (block_number, block_hash) = (&block_key[0..8], &block_key[8..]);

            let body = rlp::decode::<BodyForStorage>(body)?;
            for ref uncle in body.uncles {
                let bytes = rlp::encode(uncle);
                let hashed_tx_data = hash_data(bytes.as_ref());
                collector.collect(Entry {
                    key: hashed_tx_data.as_bytes().to_vec(),
                    value: [block_number, block_hash].concat(),
                    id: 0, // ?
                });
            }
        }

        collector.load(&mut tx_hash_cursor, None).await?;
        info!("Processed");
        Ok(ExecOutput::Progress {
            stage_progress: 0, // ?
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
