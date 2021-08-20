use crate::{
    common::hash_data,
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::tables,
    models::BodyForStorage,
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    txdb, Cursor, MutableTransaction, StageId,
};
use anyhow::anyhow;
use async_trait::async_trait;
use ethereum_types::U64;
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

    async fn execute<'tx>(
        &self,
        tx: &'tx mut RwTx,
        _input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let mut bodies_cursor = tx.mutable_cursor(&tables::BlockBody).await?;
        let mut tx_hash_cursor = tx.mutable_cursor(&tables::TxLookup).await?;

        let mut block_txs_cursor = tx.cursor(&tables::BlockTransaction).await?;

        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

        let mut start_block_number = [0; 8];
        let (_, last_processed_block_number) = tx
            .mutable_cursor(&tables::TxLookup)
            .await?
            .last()
            .await?
            .ok_or_else(|| anyhow!("TxLookup is empty"))?;

        (U64::from_big_endian(last_processed_block_number.as_ref()) + 1)
            .to_big_endian(&mut start_block_number);

        let walker_block_body = txdb::walk(&mut bodies_cursor, &start_block_number, 0);
        pin!(walker_block_body);

        while let Some((block_body_key, ref block_body_value)) =
            walker_block_body.try_next().await?
        {
            let block_number = block_body_key[..8]
                .iter()
                .cloned()
                // remove trailing zeros
                .skip_while(|x| *x == 0)
                .collect::<Vec<_>>();
            let body_rpl = rlp::decode::<BodyForStorage>(block_body_value)?;
            let (tx_count, tx_base_id) = (body_rpl.tx_amount, body_rpl.base_tx_id);
            let tx_base_id_as_bytes = tx_base_id.to_be_bytes();

            let walker_block_txs = txdb::walk(&mut block_txs_cursor, &tx_base_id_as_bytes, 0);
            pin!(walker_block_txs);

            let mut num_txs = 1;

            while let Some((_tx_key, ref tx_value)) = walker_block_txs.try_next().await? {
                if num_txs > tx_count {
                    break;
                }

                let hashed_tx_data = hash_data(tx_value);
                collector.collect(Entry {
                    key: hashed_tx_data.as_bytes().to_vec(),
                    value: block_number.clone(),
                    id: 0, // ?
                });
                num_txs += 1;
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
