use crate::{
    consensus::Consensus,
    kv::{mdbx::MdbxTransaction, tables},
    models::{
        Block, BlockBody, BlockNumber, BodyForStorage, TxIndex, EMPTY_LIST_HASH, EMPTY_ROOT, H256,
    },
    p2p::{node::Node, types::Message},
    stagedsync::{
        stage::{ExecOutput, Stage, StageError, StageInput, UnwindInput, UnwindOutput},
        stages::BODIES,
    },
    StageId,
};
use async_trait::async_trait;
use hashbrown::HashMap;
use mdbx::{EnvironmentKind, RW};
use rayon::iter::{ParallelDrainRange, ParallelIterator};
use std::{sync::Arc, time::Duration};
use tokio_stream::StreamExt;
use tracing::*;

const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct BodyDownload {
    /// P2P Interface.
    pub node: Arc<Node>,
    /// Consensus engine used.
    pub consensus: Arc<dyn Consensus>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for BodyDownload
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        BODIES
    }

    async fn execute<'tx>(
        &mut self,
        txn: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let prev_progress = input.stage_progress.map(|v| v + 1u8).unwrap_or_default();
        let starting_block = prev_progress;
        let target = input.previous_stage.map(|(_, v)| v).unwrap();

        self.download_bodies(txn, starting_block, target).await?;

        Ok(ExecOutput::Progress {
            stage_progress: target,
            done: true,
            reached_tip: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        txn: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut block_body_cur = txn.cursor(tables::BlockBody)?;
        let mut block_tx_cur = txn.cursor(tables::BlockTransaction)?;

        while let Some(((number, _), body)) = block_body_cur.last()? {
            if number <= input.unwind_to {
                break;
            }

            block_body_cur.delete_current()?;
            let mut deleted = 0;
            while deleted < body.tx_amount {
                let to_delete = body.base_tx_id + deleted;
                if block_tx_cur.seek_exact(to_delete)?.is_some() {
                    block_tx_cur.delete_current()?;
                }

                deleted += 1;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

impl BodyDownload {
    const STEP_UPPER_BOUND: usize = 1 << 15;
    const REQUEST_UPPER_BOUND: usize = 1 << 6;

    async fn download_bodies<E: EnvironmentKind>(
        &self,
        txn: &mut MdbxTransaction<'_, RW, E>,
        mut starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<()> {
        let mut requests = Self::prepare_requests(txn, starting_block, target)?;

        let mut stream = self.node.stream_bodies().await;
        let mut ticker = tokio::time::interval(REQUEST_INTERVAL);

        let mut verified = HashMap::with_capacity(requests.len());
        let mut unverified_buf = Vec::with_capacity(Self::STEP_UPPER_BOUND);

        while !requests.is_empty() {
            tokio::select! {
                Some(msg) = stream.next() => {
                    if let Message::BlockBodies(bodies) = msg.msg {
                        unverified_buf.extend(bodies.bodies);
                    }
                }
                _ = ticker.tick() => {
                    let r = requests
                        .iter()
                        .map(|(_, (_, h))| *h)
                        .take(Self::STEP_UPPER_BOUND).collect::<Vec<_>>();

                    self.node
                        .clone()
                        .send_many_body_requests(r.chunks(Self::REQUEST_UPPER_BOUND).map(ToOwned::to_owned))
                        .await?;

                    if unverified_buf.len() >= Self::STEP_UPPER_BOUND
                        || unverified_buf.len() >= requests.len()
                    {
                        for (key, value) in unverified_buf
                            .par_drain(..)
                            .map(|body| ((body.ommers_hash(), body.transactions_root()), body))
                            .collect::<Vec<_>>()
                        {
                            if let Some((number, hash)) = requests.remove(&key) {
                                verified.insert(number, (hash, value));
                            }
                        }
                    }
                },
            }
        }

        info!("Saving {} block bodies", verified.len());

        let mut cursor = txn.cursor(tables::BlockBody)?;
        let mut header_cur = txn.cursor(tables::Header)?;
        let mut block_tx_cursor = txn.cursor(tables::BlockTransaction)?;
        let mut hash_cur = txn.cursor(tables::CanonicalHeader)?;
        let mut base_tx_id = cursor
            .last()?
            .map(|((_, _), body)| *body.base_tx_id + body.tx_amount)
            .unwrap();

        // Skipping genesis block, because it's already inserted.
        if *starting_block == 0 {
            starting_block += 1u8
        };
        for block_number in starting_block..=target {
            let (hash, body) = verified.remove(&block_number).unwrap_or_else(|| {
                let (_, hash) = hash_cur.seek_exact(block_number).unwrap().unwrap();
                (hash, BlockBody::default())
            });

            let block = Block {
                header: header_cur
                    .seek_exact((block_number, hash))
                    .unwrap()
                    .unwrap()
                    .1,
                transactions: body.transactions,
                ommers: body.ommers,
            };

            self.consensus.pre_validate_block(&block, txn)?;

            cursor.append(
                (block_number, hash),
                BodyForStorage {
                    base_tx_id: TxIndex(base_tx_id),
                    tx_amount: block.transactions.len() as u64,
                    uncles: block.ommers,
                },
            )?;

            for transaction in block.transactions {
                block_tx_cursor.append(TxIndex(base_tx_id), transaction)?;
                base_tx_id += 1;
            }
        }

        Ok(())
    }

    fn prepare_requests<E: EnvironmentKind>(
        txn: &mut MdbxTransaction<'_, RW, E>,
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<HashMap<(H256, H256), (BlockNumber, H256)>> {
        assert!(target > starting_block);

        let cap = (target.0 - starting_block.0) as usize;
        let mut map = HashMap::with_capacity(cap);

        let mut canonical_cursor = txn
            .cursor(tables::CanonicalHeader)?
            .walk(Some(starting_block));
        let mut header_cursor = txn.cursor(tables::Header)?;

        while let Some(Ok((block_number, hash))) = canonical_cursor.next() {
            let (_, header) = header_cursor.seek_exact((block_number, hash))?.unwrap();
            if header.ommers_hash == EMPTY_LIST_HASH && header.transactions_root == EMPTY_ROOT {
                continue;
            }

            map.insert(
                (header.ommers_hash, header.transactions_root),
                (block_number, hash),
            );
        }

        Ok(map)
    }
}
