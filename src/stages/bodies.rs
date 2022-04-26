use crate::{
    consensus::Consensus,
    kv::{mdbx::MdbxTransaction, tables},
    models::*,
    p2p::{peer::*, types::Message},
    stagedsync::{stage::*, stages::BODIES},
    StageId,
};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use hashbrown::HashMap;
use mdbx::{EnvironmentKind, RW};
use parking_lot::RwLock;
use rayon::iter::{ParallelDrainRange, ParallelIterator};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_stream::StreamExt;
use tracing::*;

const REQUEST_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct BodyDownload {
    consensus: Arc<dyn Consensus>,
    /// Peer is a interface for interacting with p2p.
    peer: Arc<Peer>,
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
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let prev_progress = input.stage_progress.map(|v| v + 1u8).unwrap_or_default();
        let starting_block = prev_progress;
        let target = input.previous_stage.map(|(_, v)| v).unwrap();

        let mut stream = self.peer.recv_bodies().await?;
        self.collect_bodies(&mut stream, txn, starting_block, target)
            .await?;

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
    pub fn new(consensus: Arc<dyn Consensus>, peer: Arc<Peer>) -> anyhow::Result<Self> {
        Ok(Self { consensus, peer })
    }

    async fn collect_bodies<E: EnvironmentKind>(
        &mut self,
        stream: &mut InboundStream,
        txn: &mut MdbxTransaction<'_, RW, E>,
        mut starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<()> {
        let requests = Arc::new(RwLock::new(self.prepare_requests(
            txn,
            starting_block,
            target,
        )?));
        let done = Arc::new(AtomicBool::new(false));
        let handler = self.peer.clone();

        tokio::spawn({
            let done = done.clone();
            let handler = handler.clone();
            let requests = requests.clone();
            async move {
                while !done.load(Ordering::SeqCst) {
                    let left_requests = requests
                        .read()
                        .iter()
                        .map(|(_, (_, hash))| *hash)
                        .collect::<Vec<_>>();
                    info!("Sending {} block bodies requests", left_requests.len());
                    if left_requests.is_empty() {
                        done.store(true, Ordering::SeqCst);
                        break;
                    }

                    if left_requests.len() < 64 {
                        while !done.load(Ordering::SeqCst) {
                            let _ = handler.send_body_request(left_requests.clone()).await;
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        break;
                    } else {
                        let _ = left_requests
                            .chunks(64)
                            .map(ToOwned::to_owned)
                            .map(|chunk| {
                                let handler = handler.clone();
                                tokio::spawn(async move { handler.send_body_request(chunk).await })
                            })
                            .collect::<FuturesUnordered<_>>()
                            .map(|_| ())
                            .collect::<()>()
                            .await;
                    }

                    // Check if we're done before we go sleep.
                    if done.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::time::sleep(REQUEST_INTERVAL).await;
                }
            }
            .instrument(span!(Level::DEBUG, "body downloader requester"))
        });

        let mut bodies = {
            let done = done.clone();
            let requests = requests.clone();
            let mut bodies = HashMap::with_capacity(requests.read().len());
            while !done.load(Ordering::SeqCst) {
                let requests_length = requests.read().len();
                if requests_length == 0 {
                    done.store(true, Ordering::SeqCst);
                    break;
                };
                // No floting point version of: requests_length * 1.25 / 64.
                let batch_size = match requests_length * 5 / 256 {
                    v if v <= 2 => 8,
                    v => v,
                };

                let mut pending_bodies = stream
                    .filter_map(|msg| match msg.msg {
                        Message::BlockBodies(msg) => Some(msg.bodies),
                        _ => None,
                    })
                    .take(batch_size)
                    .collect::<Vec<_>>()
                    .await;

                let mut requests = requests.write();
                for (key, value) in pending_bodies
                    .par_drain(..)
                    .flatten()
                    .map(|body| ((body.ommers_hash(), body.transactions_root()), body))
                    .collect::<Vec<_>>()
                {
                    if let Some((number, hash)) = requests.remove(&key) {
                        bodies.insert(number, (hash, value));
                    }
                }
                if requests.is_empty() {
                    done.store(true, Ordering::SeqCst);
                    break;
                }
                drop(requests);
            }
            bodies
        };

        info!("Saving {} block bodies", bodies.len());

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
            let (hash, body) = bodies.remove(&block_number).unwrap_or_else(|| {
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

    pub fn prepare_requests<E: EnvironmentKind>(
        &mut self,
        txn: &mut MdbxTransaction<'_, RW, E>,
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<HashMap<(H256, H256), (BlockNumber, H256)>> {
        Ok(txn
            .cursor(tables::CanonicalHeader)?
            .walk(Some(starting_block))
            .filter_map(Result::ok)
            .filter_map(Option::Some)
            .map_while(|(number, hash)| {
                (number <= target).then(|| {
                    let header = txn.get(tables::Header, (number, hash)).unwrap().unwrap();
                    (
                        (header.ommers_hash, header.transactions_root),
                        (number, hash),
                    )
                })
            })
            .filter(|&((ommers_hash, transactions_root), _)| {
                !(ommers_hash == EMPTY_LIST_HASH && transactions_root == EMPTY_ROOT)
            })
            .collect::<HashMap<_, _>>())
    }
}
