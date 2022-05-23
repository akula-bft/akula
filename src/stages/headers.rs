use crate::{
    consensus::Consensus,
    kv::{mdbx::*, tables},
    models::{BlockHeader, BlockNumber, H256},
    p2p::{
        collections::Graph,
        node::Node,
        types::{HeaderRequest, Message, Status},
    },
    stagedsync::{stage::*, stages::HEADERS},
    TaskGuard,
};
use anyhow::format_err;
use async_trait::async_trait;
use hashbrown::{HashMap, HashSet};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

use super::STAGE_UPPER_BOUND;

const HEADERS_UPPER_BOUND: usize = 1 << 10;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct HeaderDownload {
    pub node: Arc<Node>,
    pub consensus: Arc<dyn Consensus>,
    pub max_block: BlockNumber,
    pub graph: Graph,
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
        let mut headers = Vec::with_capacity(headers_cap);

        while headers.len() < headers_cap {
            if !headers.is_empty() {}

            headers.extend(
                self.download_headers(starting_block, target_block + 1)
                    .await?,
            );
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
        todo!()
    }
}

impl HeaderDownload {
    const BACK_OFF: Duration = Duration::from_secs(2);
    const ANCHOR_THRESHOLD: usize = 8;

    async fn put_anchors(
        &mut self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<HashMap<H256, BlockHeader>> {
        let limit = (*end - *start) / HEADERS_UPPER_BOUND as u64 + 1;
        if limit > HEADERS_UPPER_BOUND as u64 {
            anyhow::bail!("too many headers to download")
        }

        let anchor_request = HeaderRequest {
            start: start.into(),
            limit,
            skip: HEADERS_UPPER_BOUND as u64 - 1,
            reverse: false,
        };

        let mut threshold = 0;
        let mut anchors = HashMap::with_capacity(limit as usize);
        let stream = self.node.stream_headers().await.timeout(Self::BACK_OFF);
        pin!(stream);

        'outer_loop: while threshold < Self::ANCHOR_THRESHOLD {
            match stream.try_next().await {
                Ok(Some(msg)) => {
                    if let Message::BlockHeaders(inner) = msg.msg {
                        let mut cur = start;

                        if inner.headers.is_empty() {
                            self.node.penalize_peer(msg.peer_id).await?;
                            continue 'outer_loop;
                        }

                        debug!(
                            "got={} anchors, anchors={}, current threshold={}",
                            inner.headers.len(),
                            anchors.len(),
                            threshold
                        );
                        for header in inner.headers {
                            if header.number != cur && limit > 1 {
                                continue 'outer_loop;
                            }
                            cur += HEADERS_UPPER_BOUND;
                            let hash = header.hash();

                            trace!("Anchor={}|{:?}", header.number, hash);

                            anchors.insert(hash, header);
                        }
                        threshold += 1;
                    }
                }
                _ => {
                    let _ = self.node.send_header_request(anchor_request).await?;
                }
            }
        }

        debug!(
            "Prepared={} anchors. Starting header downloader...",
            anchors.len()
        );

        Ok(anchors)
    }

    pub async fn download_headers(
        &mut self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<Vec<(H256, BlockHeader)>> {
        let anchors = self.put_anchors(start, end).await?;
        let mut requests = HashMap::<BlockNumber, HeaderRequest>::with_capacity(anchors.len());

        for (hash, anchor) in anchors {
            let request = HeaderRequest {
                start: hash.into(),
                ..Default::default()
            };

            requests.insert(anchor.number, request);
            self.graph.insert(anchor);
        }

        let stream = self.node.stream_headers().await.timeout(Self::BACK_OFF);
        pin!(stream);

        while !requests
            .iter()
            .all(|(k, _)| self.graph.contains(*k) || *k == start)
        {
            match stream.try_next().await {
                Ok(Some(msg)) => match msg.msg {
                    crate::p2p::types::Message::BlockHeaders(inner) => {
                        if inner.headers.is_empty() {
                            self.node.penalize_peer(msg.peer_id).await?;
                            continue;
                        }

                        if requests.contains_key(&(inner.headers[0].number.into())) {
                            debug!(
                                "Received={} headers, Graph={}",
                                inner.headers.len(),
                                self.graph.len()
                            );
                            self.graph.extend(inner.headers);
                        }
                    }
                    _ => continue,
                },
                _ => {
                    self.node
                        .clone()
                        .send_many_header_requests(
                            requests
                                .clone()
                                .into_iter()
                                .filter(|&(n, _)| !self.graph.contains(n))
                                .map(|(_, v)| v),
                        )
                        .await?;
                }
            }
        }

        let tail = self.graph.dfs().expect("unreachable");
        let mut headers = self.graph.backtrack(&tail);
        self.verify_seal(&mut headers);

        Ok(headers)
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

        if let Some(index) = self.node.position_bad_block(headers.iter().map(|(h, _)| h)) {
            headers.truncate(index);
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
        self.node.update_chain_head(Some(status)).await?;

        Ok(())
    }
}
