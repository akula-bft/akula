use crate::{
    consensus::Consensus,
    kv::{mdbx::MdbxTransaction, tables},
    models::{BlockHeader, BlockNumber, H256},
    p2p::{
        node::Node,
        types::{BlockId, HeaderRequest, Message, Status},
    },
    stagedsync::{
        stage::{ExecOutput, Stage, StageError, StageInput, UnwindInput, UnwindOutput},
        stages::HEADERS,
    },
};

use async_trait::async_trait;
use hashbrown::HashMap;
use mdbx::{EnvironmentKind, RW};
use rayon::{
    iter::{
        IndexedParallelIterator, IntoParallelRefIterator, ParallelDrainRange, ParallelIterator,
    },
    slice::ParallelSliceMut,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_stream::StreamExt;
use tracing::*;

const HEADERS_UPPER_BOUND: usize = 1 << 10;
const STAGE_UPPER_BOUND: usize = 3 << 15;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct HeaderDownload {
    pub node: Arc<Node>,
    /// Consensus engine used.
    pub consensus: Arc<dyn Consensus>,
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
        let prev_progress = input.stage_progress.map(|v| v + 1u8).unwrap_or_default();
        if prev_progress != 0 {
            self.update_head(txn, prev_progress).await?;
        }
        let mut starting_block = prev_progress;
        let current_chain_tip = loop {
            let n = self.node.chain_tip.read().0;
            if n > starting_block {
                break n;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        };

        let (target_block, reached_tip) =
            if starting_block + STAGE_UPPER_BOUND as u64 > current_chain_tip {
                (current_chain_tip, true)
            } else {
                (starting_block + STAGE_UPPER_BOUND as u64, false)
            };

        let headers_cap = (target_block.0 - starting_block.0) as usize;
        let mut headers: Vec<(H256, BlockHeader)> = Vec::with_capacity(headers_cap);

        while headers.len() < headers_cap {
            if !headers.is_empty() {
                starting_block = headers.last().unwrap().1.number;
            }

            let requests = Self::prepare_requests(starting_block, target_block);
            headers.extend(self.download_headers(requests).await?);
        }

        Self::insert(txn, headers)?;

        Ok(ExecOutput::Progress {
            stage_progress: target_block - 1u8,
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
        let mut cur = txn.cursor(tables::CanonicalHeader)?;

        if let Some(bad_block) = input.bad_block {
            if let Some((_, hash)) = cur.seek_exact(bad_block)? {
                self.node.mark_bad_block(hash);
            }
        }

        while let Some((number, _)) = cur.last()? {
            if number <= input.unwind_to {
                break;
            }

            cur.delete_current()?;
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

impl HeaderDownload {
    pub async fn download_headers(
        &self,
        mut requests: HashMap<BlockNumber, HeaderRequest>,
    ) -> anyhow::Result<Vec<(H256, BlockHeader)>> {
        let mut headers = Vec::with_capacity(requests.len() * HEADERS_UPPER_BOUND);
        let mut ticker = tokio::time::interval(REQUEST_INTERVAL);

        let mut stream = self.node.stream_headers().await;

        while !requests.is_empty() {
            let mut message_processed = false;

            tokio::select! {
                Some(msg) = stream.next() => {
                    let peer_id = msg.peer_id;
                    if let Message::BlockHeaders(msg) = msg.msg {
                        if msg.headers.is_empty() {
                            continue;
                        }
                        if msg.headers.len() > HEADERS_UPPER_BOUND {
                            self.node.penalize_peer(peer_id).await?;
                            continue;
                        }
                        let key = &msg.headers[0].number;
                        if !requests.contains_key(key) {
                            continue;
                        }
                        let limit = requests.get(key).unwrap().limit as usize;
                        if msg.headers.len() != limit {
                            continue;
                        }

                        headers.extend_from_slice(&msg.headers);
                        requests.remove(key);

                        info!("Downloaded {} block headers: {}->{}", msg.headers.len(), **key, **key + msg.headers.len() as u64);
                        message_processed = true;
                    }
                }
                _ = ticker.tick() => {}
            }
            if !message_processed {
                let r = requests.values().copied();
                self.node.clone().send_many_header_requests(r).await?;
            }
        }

        Ok(self.calculate_and_verify(headers))
    }

    fn insert<E: EnvironmentKind>(
        txn: &mut MdbxTransaction<'_, RW, E>,
        headers: Vec<(H256, BlockHeader)>,
    ) -> anyhow::Result<()> {
        let mut cursor_header_number = txn.cursor(tables::HeaderNumber)?;
        let mut cursor_header = txn.cursor(tables::Header)?;
        let mut cursor_canonical = txn.cursor(tables::CanonicalHeader)?;
        let mut cursor_td = txn.cursor(tables::HeadersTotalDifficulty)?;
        let mut td = cursor_td.last()?.map(|((_, _), v)| v).unwrap();

        for (hash, header) in headers {
            if header.number == 0 {
                continue;
            }

            let block_number = header.number;
            td += header.difficulty;

            cursor_header_number.put(hash, block_number)?;
            cursor_header.put((block_number, hash), header)?;
            cursor_canonical.put(block_number, hash)?;
            cursor_td.put((block_number, hash), td)?;
        }

        Ok(())
    }

    fn calculate_and_verify(&self, mut headers: Vec<BlockHeader>) -> Vec<(H256, BlockHeader)> {
        headers.par_sort_unstable_by_key(|header| header.number);
        let valid_till = AtomicUsize::new(0);

        headers.par_iter().enumerate().skip(1).for_each(|(i, _)| {
            if self
                .consensus
                .validate_block_header(&headers[i], &headers[i - 1], false)
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

        let mut block_headers = headers
            .par_drain(..)
            .map(|header| (header.hash(), header))
            .collect::<Vec<_>>();

        if let Some(index) = self
            .node
            .position_bad_block(block_headers.iter().map(|(h, _)| h))
        {
            block_headers.truncate(index);
        }

        block_headers
    }

    fn prepare_requests(
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> HashMap<BlockNumber, HeaderRequest> {
        assert!(starting_block < target);

        let cap = (target.0 - starting_block.0) as usize / HEADERS_UPPER_BOUND;
        let mut requests = HashMap::with_capacity(cap + 1);
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
            requests.insert(start, request);
        }
        requests
    }

    async fn update_head<'tx, E: EnvironmentKind>(
        &self,
        txn: &'tx mut MdbxTransaction<'_, RW, E>,
        height: BlockNumber,
    ) -> anyhow::Result<()> {
        let hash = txn.get(tables::CanonicalHeader, height - 1u8)?.unwrap();
        let td = txn
            .get(tables::HeadersTotalDifficulty, (height - 1u8, hash))?
            .unwrap();
        let status = Status::new(height, hash, td);
        self.node.update_chain_head(Some(status)).await?;

        Ok(())
    }
}
