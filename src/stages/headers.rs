#![allow(unreachable_code)]

use crate::{
    consensus::Consensus,
    kv::{mdbx::*, tables},
    models::{BlockHeader, BlockNumber, H256},
    p2p::{
        collections::ForkChoiceGraph,
        node::Node,
        types::{BlockHeaders, BlockId, HeaderRequest, Message, Status},
    },
    stagedsync::{stage::*, stages::HEADERS},
    TaskGuard,
};
use anyhow::format_err;
use async_trait::async_trait;
use dashmap::DashMap;
use ethereum_types::H512;
use parking_lot::Mutex;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::*;

const HEADERS_UPPER_BOUND: usize = 1 << 10;

const STAGE_UPPER_BOUND: BlockNumber = BlockNumber(90_000);
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct HeaderDownload {
    pub node: Arc<Node>,
    pub consensus: Arc<dyn Consensus>,
    pub max_block: BlockNumber,
    pub graph: Arc<Mutex<ForkChoiceGraph>>,
    pub increment: Option<BlockNumber>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for HeaderDownload
where
    E: EnvironmentKind,
{
    fn id(&self) -> crate::StageId {
        HEADERS
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
        if prev_progress != 0 {
            self.update_head(txn, prev_progress).await?;
        }

        let prev_progress_hash = txn
            .get(tables::CanonicalHeader, prev_progress)?
            .ok_or_else(|| {
                StageError::Internal(format_err!("no canonical hash for block #{prev_progress}"))
            })?;

        let starting_block: BlockNumber = prev_progress + 1;
        let mut chain_tip = self.node.chain_tip.clone();
        let current_chain_tip = loop {
            let _ = chain_tip.changed().await;
            let (n, _) = *chain_tip.borrow();
            if n >= starting_block {
                break n;
            }
        };

        debug!("Chain tip={}", current_chain_tip);

        let max_increment = self
            .increment
            .map(|v| std::cmp::min(v, STAGE_UPPER_BOUND))
            .unwrap_or(STAGE_UPPER_BOUND);
        let (mut target_block, mut reached_tip) =
            if starting_block + max_increment > current_chain_tip {
                (current_chain_tip, true)
            } else {
                (starting_block + max_increment, false)
            };
        if target_block >= self.max_block {
            target_block = self.max_block;
            reached_tip = true;
        }

        info!(
            "Target block for download: {target_block}{}",
            if reached_tip { ", will reach tip" } else { "" }
        );

        let headers_cap = (target_block.0 - starting_block.0 + 1) as usize;
        let mut headers = Vec::<(H256, BlockHeader)>::with_capacity(headers_cap);

        while headers.len() < headers_cap {
            let starting_block = if let Some((_, last_buffered_header)) = headers.last() {
                last_buffered_header.number + 1
            } else {
                starting_block
            };

            info!("Download session {starting_block} to {target_block}");

            if let Some(mut downloaded) =
                self.download_headers(starting_block, target_block).await?
            {
                // Check that downloaded headers attach to present chain
                if let Some((_, first_downloaded)) = downloaded.first() {
                    if let Some((_, last_buffered)) = headers.last() {
                        if last_buffered.hash() != first_downloaded.parent_hash {
                            // Does not attach to buffered chain, just pop last header and download again
                            headers.pop();
                            continue;
                        }
                    } else if prev_progress_hash != first_downloaded.parent_hash {
                        // Does not attach to chain in database, unwind and start over
                        return Ok(ExecOutput::Unwind {
                            unwind_to: BlockNumber(prev_progress.saturating_sub(1)),
                        });
                    }
                }

                headers.append(&mut downloaded);
            } else {
                return Ok(ExecOutput::Unwind {
                    unwind_to: BlockNumber(prev_progress.saturating_sub(1)),
                });
            }
        }
        let mut stage_progress = prev_progress;

        let mut cursor_header_number = txn.cursor(tables::HeaderNumber)?;
        let mut cursor_header = txn.cursor(tables::Header)?;
        let mut cursor_canonical = txn.cursor(tables::CanonicalHeader)?;
        let mut cursor_td = txn.cursor(tables::HeadersTotalDifficulty)?;
        let mut td = cursor_td.last()?.map(|((_, _), v)| v).unwrap();

        for (hash, header) in headers {
            if header.number == 0 {
                continue;
            }
            if header.number > self.max_block {
                break;
            }

            let block_number = header.number;
            td += header.difficulty;

            cursor_header_number.put(hash, block_number)?;
            cursor_header.put((block_number, hash), header)?;
            cursor_canonical.put(block_number, hash)?;
            cursor_td.put((block_number, hash), td)?;

            stage_progress = block_number;
        }

        Ok(ExecOutput::Progress {
            stage_progress,
            done: self.increment.is_some() || reached_tip,
            reached_tip,
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
        self.graph.lock().clear();
        let mut cur = txn.cursor(tables::CanonicalHeader)?;

        if let Some(bad_block) = input.bad_block {
            if let Some((_, hash)) = cur.seek_exact(bad_block)? {
                self.node.mark_bad_block(hash);
            }
        }

        let mut stage_progress = BlockNumber(0);
        while let Some((number, _)) = cur.last()? {
            if number <= input.unwind_to {
                stage_progress = number;
                break;
            }

            cur.delete_current()?;
        }

        Ok(UnwindOutput { stage_progress })
    }
}

impl HeaderDownload {
    const BACK_OFF: Duration = Duration::from_secs(5);

    pub async fn download_headers(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<Option<Vec<(H256, BlockHeader)>>> {
        let requests = Arc::new(self.prepare_requests(start, end));

        info!(
            "Will download {} headers over {} requests",
            end - start + 1,
            requests.len()
        );

        let mut stream = self.node.stream_headers().await;
        let is_bounded = |block_number: BlockNumber| block_number >= start && block_number <= end;

        {
            let mut tasks = Vec::new();

            let _g = TaskGuard(tokio::task::spawn({
                let node = self.node.clone();
                let requests = requests.clone();

                async move {
                    loop {
                        let reqs = requests
                            .iter()
                            .map(|entry_ref| *entry_ref.value())
                            .collect::<Vec<_>>();
                        node.clone().send_many_header_requests(reqs).await;
                        tokio::time::sleep(Self::BACK_OFF).await;
                    }
                }
            }));

            while !requests.is_empty() {
                if let Some(msg) = stream.next().await {
                    let peer_id = msg.peer_id;

                    if let Message::BlockHeaders(inner) = msg.msg {
                        if inner.headers.is_empty() {
                            continue;
                        }

                        if is_bounded(inner.headers[0].number) {
                            tasks.push(TaskGuard(tokio::task::spawn({
                                let (node, requests, graph, peer_id) = (
                                    self.node.clone(),
                                    requests.clone(),
                                    self.graph.clone(),
                                    peer_id,
                                );

                                async move {
                                    Self::handle_response(node, requests, graph, peer_id, inner)
                                        .await
                                }
                            })));
                        }
                    }
                }
            }
        }

        let took = Instant::now();

        let mut graph = self.graph.lock();
        let tail = if let Some(v) = graph.chain_head() {
            v
        } else {
            info!("Difficulty graph failure, will unwind");
            return Ok(None);
        };
        let mut headers = graph.backtrack(&tail);

        info!(
            "Built canonical chain with={} headers, elapsed={:?}",
            headers.len(),
            took.elapsed()
        );

        let cur_size = headers.len();
        let took = Instant::now();

        self.verify_seal(&mut headers);

        if cur_size == headers.len() {
            info!(
                "Seal verification took={:?} all headers are valid.",
                took.elapsed()
            );
        } else {
            info!(
                "Seal verification took={:?} {} headers are invalidated.",
                took.elapsed(),
                cur_size - headers.len()
            );
        }

        Ok(Some(headers))
    }

    async fn handle_response(
        node: Arc<Node>,
        requests: Arc<DashMap<BlockNumber, HeaderRequest>>,
        graph: Arc<Mutex<ForkChoiceGraph>>,
        peer_id: H512,
        response: BlockHeaders,
    ) {
        let cur_size = response.headers.len();
        debug!("Handling response from {peer_id} with {cur_size} headers");
        let headers = Self::check_headers(&node, response.headers);
        if cur_size != headers.len() {
            node.penalize_peer(peer_id).await;
        } else {
            let key = headers[0].1.number;
            let last_hash = headers[headers.len() - 1].0;

            let mut graph = graph.lock();

            if let dashmap::mapref::entry::Entry::Occupied(entry) = requests.entry(key) {
                let limit = entry.get().limit as usize;

                if headers.len() == limit {
                    entry.remove();
                    graph.extend(headers);
                }
            } else if !graph.contains(last_hash) {
                graph.extend(headers);
            }
        }
    }

    async fn update_head<'tx, E: EnvironmentKind>(
        &self,
        txn: &'tx mut MdbxTransaction<'_, RW, E>,
        height: BlockNumber,
    ) -> anyhow::Result<()> {
        let hash = txn.get(tables::CanonicalHeader, height)?.unwrap();
        let td = txn
            .get(tables::HeadersTotalDifficulty, (height, hash))?
            .unwrap();
        let status = Status::new(height, hash, td);
        self.node.update_chain_head(Some(status)).await;
        Ok(())
    }

    fn prepare_requests(
        &self,
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> DashMap<BlockNumber, HeaderRequest> {
        (starting_block..=target)
            .step_by(HEADERS_UPPER_BOUND)
            .map(|start| {
                let limit = std::cmp::max(
                    std::cmp::min(*target - *start, HEADERS_UPPER_BOUND as u64),
                    1,
                );

                let request = HeaderRequest {
                    start: BlockId::Number(start),
                    limit,
                    ..Default::default()
                };

                (start, request)
            })
            .collect()
    }

    #[inline]
    fn check_headers(node: &Node, headers: Vec<BlockHeader>) -> Vec<(H256, BlockHeader)> {
        let mut headers = headers
            .into_iter()
            .map(|h| (h.hash(), h))
            .collect::<Vec<_>>();

        if let Some(valid_till) =
            headers
                .iter()
                .skip(1)
                .enumerate()
                .position(|(i, (hash, header))| {
                    header.parent_hash != headers[i].0
                        || header.number != headers[i].1.number + 1u8
                        || node.bad_blocks.contains(hash)
                })
        {
            headers.truncate(valid_till);
        }
        headers
    }

    fn verify_seal(&self, headers: &mut Vec<(H256, BlockHeader)>) {
        let valid_till = AtomicUsize::new(0);

        headers.par_iter().enumerate().skip(1).for_each(|(i, _)| {
            if self
                .consensus
                .validate_block_header(&headers[i].1, &headers[i - 1].1, false)
                .is_err()
            {
                let mut value = valid_till.load(Ordering::SeqCst);
                while i < value {
                    if valid_till.compare_exchange(value, i, Ordering::SeqCst, Ordering::SeqCst)
                        == Ok(value)
                    {
                        break;
                    } else {
                        value = valid_till.load(Ordering::SeqCst);
                    }
                }
            }
        });

        let valid_till = valid_till.load(Ordering::SeqCst);
        if valid_till != 0 {
            headers.truncate(valid_till);
        }
    }
}
