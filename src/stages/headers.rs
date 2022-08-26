#![allow(unreachable_code)]

use crate::{
    consensus::{engine_factory, fork_choice_graph::ForkChoiceGraph, Consensus, DuoError, ForkChoiceMode},
    kv::{mdbx::*, tables},
    models::{BlockHeader, BlockNumber, H256},
    p2p::{
        node::{Node, NodeStream},
        types::{BlockHeaders, BlockId, HeaderRequest, Message, Status},
    },
    stagedsync::stage::*,
    StageId, TaskGuard,
};
use anyhow::format_err;
use async_trait::async_trait;
use dashmap::DashMap;
use ethereum_types::H512;
use parking_lot::Mutex;
use rand::prelude::*;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::BTreeMap,
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

pub const HEADERS: StageId = StageId("Headers");

#[derive(Debug)]
pub struct HeaderDownload {
    pub node: Arc<Node>,
    pub consensus: Arc<dyn Consensus>,
    pub max_block: BlockNumber,
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
        self.update_head(txn, prev_progress).await?;

        let mut stage_progress = prev_progress;
        let mut reached_tip = true;

        if prev_progress < self.max_block {
            let prev_progress_hash = txn
                .get(tables::CanonicalHeader, prev_progress)?
                .ok_or_else(|| {
                    StageError::Internal(format_err!(
                        "no canonical hash for block #{prev_progress}"
                    ))
                })?;
            let prev_progress_header = txn
                .get(tables::Header, (prev_progress, prev_progress_hash))?
                .ok_or_else(|| {
                    StageError::Internal(format_err!(
                        "no canonical header for block #{prev_progress}:{prev_progress_hash:?}"
                    ))
                })?;

            let headers;
            (headers, reached_tip) = match self.consensus.fork_choice_mode() {
                ForkChoiceMode::External(mut chain_tip_watch) => {
                    // Reverse download mode

                    let prev_progress_block = txn
                        .get(tables::Header, (prev_progress, prev_progress_hash))?
                        .ok_or_else(|| {
                            StageError::Internal(format_err!(
                                "no header for block #{prev_progress}"
                            ))
                        })?;

                    info!("Awaiting chain tip...");

                    let (chain_tip_hash, chain_finalized_hash) = loop {
                        let _ = chain_tip_watch.changed().await;

                        let fork_choice = *chain_tip_watch.borrow();
                        if !fork_choice.head_block.is_zero()
                            && fork_choice.head_block != prev_progress_hash
                        {
                            break (fork_choice.head_block, fork_choice.finalized_block);
                        }
                    };
                    let _ = chain_finalized_hash;

                    info!("Received chain tip hash: {chain_tip_hash}, starting_download");

                    let mut stream = self.node.stream_headers().await;

                    match self
                        .reverse_download_linear(
                            &mut stream,
                            prev_progress_hash,
                            &prev_progress_block,
                            chain_tip_hash,
                            chain_finalized_hash,
                        )
                        .await
                    {
                        LinearDownloadResult::Done(buffered_headers) => (
                            Box::new(buffered_headers.into_values())
                                as Box<dyn Iterator<Item = (H256, BlockHeader)> + Send>,
                            true,
                        ),
                        LinearDownloadResult::DoesNotAttach => {
                            return Ok(ExecOutput::Unwind {
                                unwind_to: prev_progress
                                    .checked_sub(1)
                                    .ok_or_else(|| format_err!("Attempting to reorg past genesis"))?
                                    .into(),
                            })
                        }
                        LinearDownloadResult::NoResponse => {
                            return Ok(ExecOutput::Progress {
                                stage_progress: prev_progress,
                                done: false,
                                reached_tip: false,
                            });
                        }
                    }
                }
                ForkChoiceMode::Difficulty(fork_choice_graph) => {
                    // Forward download mode
                    let mut chain_tip = self.node.chain_tip.clone();
                    let current_chain_tip = loop {
                        let _ = chain_tip.changed().await;
                        let (n, _) = *chain_tip.borrow();
                        if n > prev_progress {
                            break n;
                        }
                    };

                    debug!("Chain tip={}", current_chain_tip);

                    let (mut target_block, mut reached_tip) = Self::forward_set_target_block(
                        prev_progress,
                        self.increment,
                        current_chain_tip,
                    );

                    let starting_block: BlockNumber = prev_progress + 1;

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
                        let starting_block = if let Some((_, last_buffered_header)) = headers.last()
                        {
                            last_buffered_header.number + 1
                        } else {
                            starting_block
                        };

                        info!("Download session {starting_block} to {target_block}");

                        if let Some(mut downloaded) = self
                            .download_headers(
                                txn,
                                fork_choice_graph.clone(),
                                &prev_progress_header,
                                starting_block,
                                target_block,
                            )
                            .await?
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

                    (
                        Box::new(headers.into_iter())
                            as Box<dyn Iterator<Item = (H256, BlockHeader)> + Send>,
                        reached_tip,
                    )
                }
            };

            let mut cursor_header_number = txn.cursor(tables::HeaderNumber)?;
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
                cursor_canonical.put(block_number, hash)?;
                cursor_td.put((block_number, hash), td)?;

                stage_progress = block_number;
            }
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
        if let ForkChoiceMode::Difficulty(graph) = self.consensus.fork_choice_mode() {
            graph.lock().clear();
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum LinearDownloadResult {
    Done(BTreeMap<BlockNumber, (H256, BlockHeader)>),
    DoesNotAttach,
    NoResponse,
}

impl HeaderDownload {
    const BACK_OFF: Duration = Duration::from_secs(5);

    async fn reverse_download_linear(
        &self,
        stream: &mut NodeStream,
        head: H256,
        head_block: &BlockHeader,
        chain_tip: H256,
        finalized: H256,
    ) -> LinearDownloadResult {
        let mut buffered_headers = BTreeMap::<BlockNumber, (H256, BlockHeader)>::new();
        let mut remaining_attempts = Some(10_usize);
        loop {
            let limit = 1024;
            let (request_start_block, request_start) = buffered_headers
                .iter()
                .next()
                .map(|(_, (_, block))| (Some(block.number), block.parent_hash))
                .unwrap_or((None, chain_tip));

            info!(
                "Requesting {limit} headers ending at {}/{request_start:?}",
                if let Some(block_number) = request_start_block {
                    format!("#{block_number}")
                } else {
                    "the tip".to_string()
                }
            );

            let mut success = true;
            let sent_request_id = rand::thread_rng().gen();
            if let Ok(sent) = tokio::time::timeout(
                Duration::from_secs(5),
                self.node.send_header_request(
                    Some(sent_request_id),
                    HeaderRequest {
                        start: BlockId::Hash(request_start),
                        limit,
                        reverse: true,
                        ..Default::default()
                    },
                    request_start_block,
                ),
            )
            .await
            {
                let timeout = tokio::time::sleep(Duration::from_secs(5));
                tokio::pin!(timeout);

                let headers = loop {
                    tokio::select! {
                        msg = stream.next() => {
                            if let Some(msg) = msg {
                                if sent.contains(&(msg.sentry_id, msg.peer_id)) {
                                    if let Message::BlockHeaders(BlockHeaders { request_id, headers }) = msg.msg.clone() {
                                        if sent_request_id == request_id && !headers.is_empty() {
                                            info!("Received {} headers from peer {}/{}", headers.len(), msg.sentry_id, msg.peer_id);

                                            break Some(headers);
                                        }
                                    }
                                }
                                debug!("Ignoring unsolicited message from {}/{}: {:?}", msg.sentry_id, msg.peer_id, msg);
                            }
                        }
                        _ = &mut timeout => {
                            break None;
                        }
                    }
                };

                if let Some(mut headers) = headers {
                    headers.sort_unstable_by_key(|h| h.number);

                    let num_headers = headers.len();

                    for header in headers.into_iter().rev() {
                        let header_hash = header.hash();

                        debug!("Checking header #{}/{header_hash:?}", header.number);

                        if head == header_hash {
                            return LinearDownloadResult::Done(buffered_headers);
                        } else if head_block.number == header.number {
                            return LinearDownloadResult::DoesNotAttach;
                        }

                        if let Some((_, (_, earliest_block))) = buffered_headers.iter().next() {
                            if earliest_block.number.0 - 1 != header.number.0
                                && earliest_block.parent_hash != header_hash
                            {
                                break;
                            }

                            self.consensus
                                .validate_block_header(earliest_block, &header, true)
                                .map_err(|e| match e {
                                    DuoError::Validation(error) => StageError::Validation {
                                        block: header.number,
                                        error,
                                    },
                                    DuoError::Internal(error) => StageError::Internal(error),
                                })
                                .unwrap();
                        } else if chain_tip != header.hash() {
                            break;
                        }

                        if header_hash == finalized {
                            remaining_attempts = None;
                        }

                        buffered_headers.insert(header.number, (header_hash, header));
                    }

                    info!(
                        "Buffered {} (+{num_headers}) headers",
                        buffered_headers.len()
                    );
                } else {
                    success = false;
                }
            } else {
                success = false;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }

            if !success {
                if let Some(remaining_attempts) = &mut remaining_attempts {
                    if let Some(attempts) = remaining_attempts.checked_sub(1) {
                        *remaining_attempts = attempts;
                    } else {
                        return LinearDownloadResult::NoResponse;
                    }
                }
            }
        }
    }

    pub async fn download_headers<'tx, 'db, E: EnvironmentKind>(
        &self,
        txn: &'tx mut MdbxTransaction<'db, RW, E>,
        fork_choice_graph: Arc<Mutex<ForkChoiceGraph>>,
        prev_progress_header: &BlockHeader,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<Option<Vec<(H256, BlockHeader)>>> {
        let requests = Arc::new(Self::prepare_requests(start, end));
        let peer_map = Arc::new(DashMap::new());

        let chain_config = txn
            .get(tables::Config, ())?
            .ok_or_else(|| format_err!("No chain specification set"))?;
        let consensus_engine = engine_factory(None, chain_config.clone(), None)?;
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

                        info!(
                            "Received {} headers from peer {peer_id}",
                            inner.headers.len()
                        );

                        if is_bounded(inner.headers[0].number) {
                            tasks.push(TaskGuard(tokio::task::spawn({
                                let node = self.node.clone();
                                let requests = requests.clone();
                                let graph = fork_choice_graph.clone();
                                let peer_map = peer_map.clone();

                                async move {
                                    Self::handle_response(
                                        node, requests, graph, peer_map, peer_id, inner,
                                    )
                                    .await
                                }
                            })));
                        }
                    }
                }
            }
        }

        let took = Instant::now();
        let mut headers = {
            let mut graph = fork_choice_graph.lock();
            let tail = if let Some(v) = graph.chain_head() {
                v
            } else {
                info!("Difficulty graph failure, will unwind");
                return Ok(None);
            };
            graph.backtrack(&tail)
        };

        if let Some(first) = headers.first() {
            if prev_progress_header.hash() != first.1.parent_hash {
                return Ok(None);
            }
        }

        info!(
            "Built canonical chain with={} headers, elapsed={:?}",
            headers.len(),
            took.elapsed()
        );

        let cur_size = headers.len();
        let took = Instant::now();

        if let Err((last_valid, invalid_hash)) =
            self.validate_sequentially(consensus_engine, txn, prev_progress_header, &headers)
        {
            headers.truncate(last_valid);

            if let Some(peer_id) = peer_map.get(&invalid_hash).map(|e| *e) {
                self.node.penalize_peer(peer_id).await;
            }
        }

        if self.consensus.needs_parallel_validation() {
            if let Err((last_valid, invalid_hash)) = self.validate_parallel(&headers) {
                headers.truncate(last_valid);

                if let Some(peer_id) = peer_map.get(&invalid_hash).map(|e| *e) {
                    self.node.penalize_peer(peer_id).await;
                }
            }
        }

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
        peer_map: Arc<DashMap<H256, H512>>,
        peer_id: H512,
        response: BlockHeaders,
    ) {
        let cur_size = response.headers.len();
        debug!("Handling response from {peer_id} with {cur_size} headers");

        match Self::check_contiguous(response.headers) {
            Ok(headers) => {
                let key = headers[0].1.number;
                let last_hash = headers[headers.len() - 1].0;

                let mut graph = graph.lock();

                if let dashmap::mapref::entry::Entry::Occupied(entry) = requests.entry(key) {
                    let limit = entry.get().limit as usize;

                    if headers.len() == limit {
                        entry.remove();

                        for (hash, header) in headers {
                            graph.insert_with_hash(hash, header);
                            peer_map.insert(hash, peer_id);
                        }
                    }
                } else if !graph.contains(last_hash) {
                    for (hash, header) in headers {
                        graph.insert_with_hash(hash, header);
                        peer_map.insert(hash, peer_id);
                    }
                }
            }
            Err(()) => {
                warn!("Rejected discontiguous header segment from {peer_id}");
                node.penalize_peer(peer_id).await
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

    fn forward_set_target_block(
        prev_progress: BlockNumber,
        increment: Option<BlockNumber>,
        chain_tip: BlockNumber,
    ) -> (BlockNumber, bool) {
        let max_increment = std::cmp::max(
            BlockNumber(1),
            increment
                .map(|v| std::cmp::min(v, STAGE_UPPER_BOUND))
                .unwrap_or(STAGE_UPPER_BOUND),
        );
        let max_incremented_from_start = BlockNumber(prev_progress.0 + max_increment.0);

        if max_incremented_from_start > chain_tip {
            (chain_tip, true)
        } else {
            (max_incremented_from_start, false)
        }
    }

    fn prepare_requests(
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> DashMap<BlockNumber, HeaderRequest> {
        (starting_block..=target)
            .step_by(HEADERS_UPPER_BOUND)
            .map(|start| {
                let limit = std::cmp::min(*target - *start + 1, HEADERS_UPPER_BOUND as u64);

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
    fn check_contiguous(headers: Vec<BlockHeader>) -> Result<Vec<(H256, BlockHeader)>, ()> {
        let headers = headers
            .into_iter()
            .map(|h| (h.hash(), h))
            .collect::<Vec<_>>();
        if headers.iter().skip(1).enumerate().any(|(i, (_, header))| {
            header.parent_hash != headers[i].0 || header.number != headers[i].1.number + 1u8
        }) {
            return Err(());
        }

        Ok(headers)
    }

    fn validate_sequentially<'tx, 'db, 'a, E: EnvironmentKind>(
        &self,
        mut engine: Box<dyn Consensus>,
        txn: &'tx mut MdbxTransaction<'db, RW, E>,
        mut parent_header: &'a BlockHeader,
        headers: &'a [(H256, BlockHeader)],
    ) -> Result<(), (usize, H256)> {

        let mut cursor_header = txn.cursor(tables::Header)
            .map_err(|e| {
                warn!("validate_sequentially, but txn open err: ({e:?})");
                (0, H256::zero())
            })?;
        for (i, (hash, header)) in headers.iter().enumerate() {
            let parent_hash = parent_header.hash();
            if header.parent_hash != parent_hash || header.number != parent_header.number + 1_u8 {
                warn!("Rejected bad block header ({hash:?}) because it doesn't attach to parent ({parent_hash:?}): {header:?} => {parent_header:?}");
                return Err((i.saturating_sub(1), *hash));
            }

            if let Err(e) = engine.snapshot(txn, BlockNumber(header.number.0-1), header.parent_hash) {
                warn!("Rejected bad block header ({hash:?}) when create snap err: {e:?}");
                return Err((i.saturating_sub(1), *hash));
            }
            if let Err(e) = self
                .consensus
                .validate_block_header(header, parent_header, false)
            {
                warn!("Rejected bad block header ({hash:?}) for reason {e:?}: {header:?}");
                return Err((i.saturating_sub(1), *hash));
            }
            parent_header = header;
            cursor_header.put((header.number, headers[i].0), header.clone())
                .map_err(|e| {
                    warn!("Rejected bad block header ({hash:?}) because txn put header err: ({e:?})");
                    (i.saturating_sub(1), *hash)
                })?;
        }

        Ok(())
    }

    fn validate_parallel(&self, headers: &[(H256, BlockHeader)]) -> Result<(), (usize, H256)> {
        let valid_till = AtomicUsize::new(0);

        headers.par_iter().enumerate().for_each(|(i, (_, header))| {
            if self.consensus.validate_header_parallel(header).is_err() {
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
            Err((valid_till - 1, headers[valid_till].0))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_requests() {
        for (from, to, requests) in [
            (1000, 1000, vec![(1000, 1)]),
            (1000, 3047, vec![(1000, 1024), (2024, 1024)]),
            (1000, 3048, vec![(1000, 1024), (2024, 1024), (3048, 1)]),
            (
                1000,
                5000,
                vec![(1000, 1024), (2024, 1024), (3048, 1024), (4072, 929)],
            ),
        ] {
            assert_eq!(
                HeaderDownload::prepare_requests(from.into(), to.into())
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
                requests
                    .into_iter()
                    .map(|(start, limit)| {
                        let start = BlockNumber(start);
                        (
                            start,
                            HeaderRequest {
                                start: BlockId::Number(start),
                                limit,
                                ..Default::default()
                            },
                        )
                    })
                    .collect()
            )
        }
    }

    #[test]
    fn forward_set_target_block() {
        for ((prev_progress, increment, chain_tip), (expected_target, expected_reached_tip)) in [
            ((10_000, Some(1_000_000), 2_000_000), (100_000, false)),
            ((10_000, Some(10_000), 2_000_000), (20_000, false)),
            ((10_000, Some(10_000), 15_000), (15_000, true)),
            ((10_000, None, 2_000_000), (100_000, false)),
            ((10_000, None, 30_000), (30_000, true)),
        ] {
            assert_eq!(
                HeaderDownload::forward_set_target_block(
                    BlockNumber(prev_progress),
                    increment.map(BlockNumber),
                    BlockNumber(chain_tip)
                ),
                (BlockNumber(expected_target), expected_reached_tip)
            );
        }
    }
}
