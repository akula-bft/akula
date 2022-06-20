use crate::{
    consensus::{Consensus, DuoError},
    kv::{mdbx::MdbxTransaction, tables, traits::ttw},
    models::*,
    p2p::{
        node::{Node, NodeStream},
        types::{BlockBodies, Message},
    },
    stagedsync::{stage::*, stages::BODIES},
    StageId, TaskGuard,
};
use anyhow::format_err;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use hashbrown::HashMap;
use mdbx::{EnvironmentKind, RW};
use parking_lot::{Mutex, RwLock};
use rand::prelude::*;
use rayon::iter::{ParallelDrainRange, ParallelIterator};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::select;
use tokio_stream::StreamExt;
use tracing::*;

const STAGE_UPPER_BOUND: usize = 90_000;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct BodyDownload {
    /// Node is a interface for interacting with p2p.
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
        let prev_progress = input.stage_progress.unwrap_or_default();
        let prev_stage_progress = input
            .previous_stage
            .map(|(_, v)| v)
            .ok_or_else(|| format_err!("cannot be first stage"))?;

        let (target, done) = if prev_stage_progress > prev_progress + STAGE_UPPER_BOUND {
            (prev_progress + STAGE_UPPER_BOUND, false)
        } else {
            (prev_stage_progress, true)
        };

        if target > prev_progress {
            info!("Downloading blocks up to {target}");
            let starting_block = prev_progress + 1;

            let mut stream = self.node.stream_bodies().await;
            self.download_bodies(&mut stream, txn, starting_block, target, done)
                .await?;
        }

        Ok(ExecOutput::Progress {
            stage_progress: target,
            done,
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

#[derive(Default)]
pub struct PendingResponses {
    inner: HashSet<u64>,
}

impl PendingResponses {
    pub fn get_id(&mut self) -> u64 {
        loop {
            let id = rand::thread_rng().gen::<u64>();

            if self.inner.insert(id) {
                return id;
            }
        }
    }

    pub fn remove(&mut self, request_id: u64) -> bool {
        self.inner.remove(&request_id)
    }

    pub fn count(&self) -> usize {
        self.inner.len()
    }

    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

impl BodyDownload {
    async fn download_bodies<E: EnvironmentKind>(
        &mut self,
        stream: &mut NodeStream,
        txn: &mut MdbxTransaction<'_, RW, E>,
        starting_block: BlockNumber,
        target: BlockNumber,
        will_reach_tip: bool,
    ) -> Result<(), StageError> {
        let requests = Arc::new(RwLock::new(Self::prepare_requests(
            txn,
            starting_block,
            target,
        )?));
        let pending_responses = Arc::new(Mutex::new(PendingResponses::default()));
        let handler = self.node.clone();

        let mut bodies = {
            let _requester_task = TaskGuard(tokio::spawn({
                let handler = handler.clone();
                let requests = requests.clone();
                let pending_responses = pending_responses.clone();

                async move {
                    loop {
                        let left_requests = requests
                            .read()
                            .values()
                            .copied()
                            .collect::<Vec<_>>();

                        if left_requests.is_empty() {
                            break;
                        }
                        let chunk = 16;

                        let mut will_reach_tip_this_cycle = false;

                        let mut total_chunks_this_cycle = 32 * handler.total_peers().await;
                        let left_chunks = (left_requests.len() + chunk - 1) / chunk;
                        if left_chunks < total_chunks_this_cycle {
                            total_chunks_this_cycle = left_chunks;
                            will_reach_tip_this_cycle = true;
                        }

                        info!(
                            "Sending {total_chunks_this_cycle} requests for {} out of {} block bodies",
                            std::cmp::min(total_chunks_this_cycle * chunk, left_requests.len()),
                            left_requests.len()
                        );

                        left_requests
                            .chunks(chunk)
                            .take(total_chunks_this_cycle)
                            .map(|chunk| {
                                let handler = handler.clone();
                                let pending_responses = pending_responses.clone();
                                async move {
                                    let _ = tokio::time::timeout(
                                        REQUEST_INTERVAL,
                                        async move {
                                            let request_id = pending_responses.lock().get_id();
                                            if handler.send_block_request(request_id, chunk, will_reach_tip_this_cycle).await.is_none() {
                                                pending_responses.lock().remove(request_id);
                                            } else {
                                                debug!("Sent block request with id {request_id}");
                                            }
                                        }
                                    )
                                    .await;
                                }
                            })
                            .collect::<FuturesUnordered<_>>()
                            .map(|_| ())
                            .collect::<()>()
                            .await;

                        for _ in 0..(REQUEST_INTERVAL.as_secs() as usize * 2) {
                            let current_pending_responses = pending_responses.lock().count();
                            let next_cycle_threshold = total_chunks_this_cycle / 5;
                            if current_pending_responses < next_cycle_threshold {
                                break;
                            }

                            trace!("Not enough blocks received for next request cycle ({current_pending_responses} >= {next_cycle_threshold})");

                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }

                        pending_responses.lock().clear();
                    }
                }
                .instrument(span!(Level::DEBUG, "body downloader requester"))
            }));

            let mut bodies = HashMap::with_capacity(requests.read().len());
            loop {
                let requests_length = requests.read().len();
                if requests_length == 0 {
                    break;
                };
                let batch_size = match requests_length * 5 / 256 {
                    v if v <= 2 => 8,
                    v => v,
                };

                let mut pending_bodies = Vec::with_capacity(batch_size);

                let mut skip_wait = false;
                if will_reach_tip {
                    for (
                        _,
                        (
                            _,
                            _,
                            Block {
                                transactions,
                                ommers,
                                ..
                            },
                        ),
                    ) in handler.block_cache.lock().drain()
                    {
                        pending_bodies.push(vec![BlockBody {
                            transactions,
                            ommers,
                        }]);
                        skip_wait = true;
                    }
                }

                if !skip_wait {
                    let s = stream.filter_map(|msg| match msg.msg {
                        Message::BlockBodies(msg) => Some(msg),
                        _ => None,
                    });
                    tokio::pin!(s);

                    let receive_window = tokio::time::sleep(Duration::from_secs(2));
                    tokio::pin!(receive_window);

                    loop {
                        select! {
                            res = s.next() => {
                                if let Some(BlockBodies { request_id, bodies }) = res {
                                    let mut pending_responses = pending_responses.lock();
                                    if pending_responses.remove(request_id) {
                                        debug!("Accepted block bodies with id {request_id}");
                                        pending_bodies.push(bodies);

                                        if pending_responses.count() == 0 {
                                            break;
                                        }
                                    } else {
                                        debug!("Ignoring block response with unknown id: {request_id}");
                                    }
                                } else {
                                    break;
                                }
                            }
                            _ = &mut receive_window => break,
                        }
                    }
                }

                let tmp = pending_bodies
                    .par_drain(..)
                    .flatten()
                    .map(|body| ((body.ommers_hash(), body.transactions_root()), body))
                    .collect::<Vec<_>>();

                let mut downloaded = 0;
                let mut requests = requests.write();
                for (key, value) in tmp {
                    if let Some((number, hash)) = requests.remove(&key) {
                        bodies.insert(number, (hash, value));
                        downloaded += 1;
                    } else {
                        debug!("Block {key:?} was not requested, ignored");
                    }
                }

                if downloaded > 0 {
                    info!("Downloaded {downloaded} block bodies");
                }
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

            self.consensus
                .pre_validate_block(&block, txn)
                .map_err(|e| match e {
                    DuoError::Validation(error) => StageError::Validation {
                        block: block.header.number,
                        error,
                    },
                    DuoError::Internal(error) => StageError::Internal(error),
                })?;

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
        let cap = match target.0.saturating_sub(starting_block.0) + 1 {
            0 => return Ok(HashMap::new()),
            cap => cap as usize,
        };
        let mut map = HashMap::with_capacity(cap);

        let mut canonical_cursor = txn
            .cursor(tables::CanonicalHeader)?
            .walk(Some(starting_block))
            .take_while(ttw(|&(block_number, _)| block_number <= target));
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
