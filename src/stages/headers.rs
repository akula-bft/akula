#![allow(unreachable_code)]

use crate::{
    consensus::Consensus,
    kv::{mdbx::*, tables},
    models::{BlockHeader, BlockNumber, H256},
    p2p::{
        collections::Graph,
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

const STAGE_UPPER_BOUND: usize = 3 << 15;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct HeaderDownload {
    pub node: Arc<Node>,
    pub consensus: Arc<dyn Consensus>,
    pub requests: Arc<DashMap<BlockNumber, HeaderRequest>>,
    pub max_block: BlockNumber,
    pub graph: Arc<Mutex<Graph>>,
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

        let mut starting_block: BlockNumber = prev_progress + 1;
        let current_chain_tip = loop {
            let n = self.node.chain_tip.read().0;
            if n > starting_block {
                break n;
            }
            tokio::time::sleep(Self::BACK_OFF).await;
        };

        debug!("Chain tip={}", current_chain_tip);

        let (mut target_block, mut reached_tip) =
            if starting_block + STAGE_UPPER_BOUND > current_chain_tip {
                (current_chain_tip, true)
            } else {
                (starting_block + STAGE_UPPER_BOUND, false)
            };
        if target_block >= self.max_block {
            target_block = self.max_block;
            reached_tip = true;
        }

        let headers_cap = (target_block.0 - starting_block.0) as usize;
        let mut headers = Vec::<(H256, BlockHeader)>::with_capacity(headers_cap);

        while headers.len() < headers_cap {
            if !headers.is_empty() {
                starting_block = headers.last().map(|(_, h)| h).unwrap().number;
            }

            headers.extend(self.download_headers(starting_block, target_block).await?);
            if let Some((_, h)) = headers.first() {
                if h.parent_hash != prev_progress_hash {
                    return Ok(ExecOutput::Unwind {
                        unwind_to: BlockNumber(prev_progress.saturating_sub(1)),
                    });
                }
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
            done: true,
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
    ) -> anyhow::Result<Vec<(H256, BlockHeader)>> {
        self.prepare_requests(start, end);

        let mut stream = self.node.stream_headers().await;
        let is_bounded = |block_number: BlockNumber| block_number >= start && block_number <= end;

        {
            let mut tasks = Vec::new();

            let _g = TaskGuard(tokio::task::spawn({
                let node = self.node.clone();
                let requests = self.requests.clone();

                async move {
                    loop {
                        let reqs = requests
                            .iter()
                            .map(|entry_ref| *entry_ref.value())
                            .collect::<Vec<_>>();
                        node.clone().send_many_header_requests(reqs).await?;
                        tokio::time::sleep(Self::BACK_OFF).await;
                    }

                    Ok::<_, anyhow::Error>(())
                }
            }));

            while !self.requests.is_empty() {
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
                                    self.requests.clone(),
                                    self.graph.clone(),
                                    peer_id,
                                );

                                async move {
                                    Self::handle_response(node, requests, graph, peer_id, inner)
                                        .await?;
                                    Ok::<_, anyhow::Error>(())
                                }
                            })));
                        }
                    }
                }
            }
        }

        let took = Instant::now();

        let mut graph = self.graph.lock();
        let tail = graph.dfs().expect("unreachable");
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

        Ok(headers)
    }

    async fn handle_response(
        node: Arc<Node>,
        requests: Arc<DashMap<BlockNumber, HeaderRequest>>,
        graph: Arc<Mutex<Graph>>,
        peer_id: H512,
        response: BlockHeaders,
    ) -> anyhow::Result<()> {
        let cur_size = response.headers.len();
        let headers = Self::check_headers(&node, response.headers);
        if cur_size != headers.len() {
            node.penalize_peer(peer_id).await?;
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

        Ok(())
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
        self.node.update_chain_head(Some(status)).await?;

        Ok(())
    }
}

impl HeaderDownload {
    fn prepare_requests(&self, starting_block: BlockNumber, target: BlockNumber) {
        assert!(starting_block < target);

        self.requests.clear();

        for start in (starting_block..target).step_by(HEADERS_UPPER_BOUND) {
            let limit = if start + HEADERS_UPPER_BOUND < target {
                HEADERS_UPPER_BOUND as u64
            } else {
                *target - *start
            };

            let request = HeaderRequest {
                start: BlockId::Number(start),
                limit,
                ..Default::default()
            };

            self.requests.insert(start, request);
        }
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
