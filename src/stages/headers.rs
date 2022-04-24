use crate::{
    consensus::Consensus,
    kv::{mdbx::*, tables},
    models::{BlockHeader, BlockNumber, ChainConfig, H256},
    p2p::{peer::*, types::*},
    stagedsync::{stage::*, stages::HEADERS},
};
use async_trait::async_trait;
use futures::{future, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::{HashMap, HashSet};
use hashlink::{LinkedHashSet, LruCache};
use rayon::prelude::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::{debug, info};

#[derive(Debug)]
pub struct HeaderDownload {
    /// Consensus engine used
    consensus: Arc<dyn Consensus>,
    /// Peer is an interface for interacting with P2P network.
    pub peer: Arc<Peer>,
    /// Height is a HeaderDownloader highest seen height.
    height: BlockNumber,
    /// Marker is a HeaderDownloader highest seen hash.
    marker: H256,
    /// seen_announces is a set of seen announced blocks/headers.
    seen_announces: LruCache<H256, ()>,
    /// parents_table is a mapping from block hash to its parent hash.
    parents_table: LruCache<H256, H256>,
    /// block_table is a mapping from block hash to its block number and optionally total difficulty.
    blocks_table: LruCache<H256, (BlockNumber, Option<u128>)>,
    pending_requests: HashMap<BlockNumber, HeaderRequest>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for HeaderDownload
where
    E: EnvironmentKind,
{
    #[inline]
    fn id(&self) -> crate::StageId {
        HEADERS
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
        match prev_progress {
            BlockNumber(0) => {
                self.peer
                    .update_head(
                        BlockNumber(0),
                        self.peer.chain_config.genesis_hash,
                        self.peer.chain_config.chain_spec.genesis.seal.difficulty(),
                    )
                    .await?;
            }
            height => {
                let hash = txn
                    .get(tables::CanonicalHeader, height - 1u8)
                    .unwrap()
                    .unwrap();
                let td = txn
                    .get(tables::HeadersTotalDifficulty, (height - 1u8, hash))
                    .unwrap()
                    .unwrap();
                self.peer.update_head(height - 1u8, hash, td).await?;
            }
        };

        let mut starting_block = prev_progress;
        let mut target = txn
            .cursor(tables::LastHeader)?
            .last()?
            .map(|(_, (v, _))| v)
            .unwrap();
        let do_lookup = target - starting_block <= BlockNumber(BATCH_SIZE as u64);
        match do_lookup {
            true => {
                let mut stream = self.peer.recv().await?;
                target = match self.evaluate_chain_tip(&mut stream).await? {
                    (value, _) if value - starting_block <= BlockNumber(BATCH_SIZE as u64) => value,
                    last_header => {
                        txn.set(tables::LastHeader, Default::default(), last_header)?;
                        starting_block + BlockNumber(BATCH_SIZE as u64)
                    }
                }
            }
            false => target = starting_block + BlockNumber(BATCH_SIZE as u64),
        }

        let mut stream = self.peer.recv_headers().await?;

        let alloc = *(target - starting_block) as usize;
        let mut headers = Vec::<BlockHeader>::with_capacity(alloc);
        while headers.len() < alloc {
            if !headers.is_empty() {
                (starting_block, _) = headers
                    .last()
                    .map(|header| (header.number, header.hash()))
                    .unwrap();
            }
            headers.extend(
                self.collect_headers(&mut stream, starting_block, target)
                    .await?,
            );
        }

        insert(txn, &mut headers)?;
        Ok(ExecOutput::Progress {
            stage_progress: target - 1u8,
            done: true,
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
    pub fn new<T, E>(
        conn: T,
        consensus: Arc<dyn Consensus>,
        chain_config: ChainConfig,
        txn: MdbxTransaction<'_, RO, E>,
    ) -> anyhow::Result<Self>
    where
        T: Into<SentryPool>,
        E: EnvironmentKind,
    {
        let (height, hash) = txn
            .cursor(tables::CanonicalHeader)?
            .last()?
            .unwrap_or((BlockNumber(0), chain_config.genesis_hash));
        let total_difficulty = txn
            .get(tables::HeadersTotalDifficulty, (height, hash))?
            .unwrap_or_else(|| chain_config.chain_spec.genesis.seal.difficulty())
            .to_be_bytes()
            .into();
        Ok(Self {
            consensus,
            peer: Arc::new(Peer::new(
                conn,
                chain_config,
                Status {
                    height,
                    hash,
                    total_difficulty,
                },
            )),
            height,
            marker: hash,
            seen_announces: LruCache::new(128),
            parents_table: LruCache::new(8),
            blocks_table: LruCache::new(128),
            pending_requests: Default::default(),
        })
    }

    #[inline(always)]
    fn prevalidate_block_headers(&self, headers: &[BlockHeader]) -> bool {
        !headers.is_empty()
            && self.pending_requests.contains_key(&headers[0].number)
            && (headers.len() == CHUNK_SIZE
                || headers.len()
                    == self.pending_requests.get(&headers[0].number).unwrap().limit as usize)
            && (headers.last().unwrap().number + 1u8 == headers[0].number + CHUNK_SIZE
                || headers.last().unwrap().number + 1u8
                    == headers[0].number
                        + self.pending_requests.get(&headers[0].number).unwrap().limit)
    }

    fn prepare_requests(&mut self, starting_block: BlockNumber, target: BlockNumber) {
        self.pending_requests = (starting_block..target)
            .step_by(CHUNK_SIZE)
            .map(|block_number| {
                debug!(
                    "Preparing header requests for {}->{}",
                    block_number,
                    block_number + CHUNK_SIZE
                );
                (
                    block_number,
                    HeaderRequest {
                        start: block_number.into(),
                        limit: (block_number + CHUNK_SIZE < target)
                            .then(|| CHUNK_SIZE as u64)
                            .unwrap_or(*target - *block_number),
                        ..Default::default()
                    },
                )
            })
            .collect::<HashMap<_, _>>();
    }
    async fn collect_headers(
        &mut self,
        stream: &mut InboundStream,
        starting_block: BlockNumber,
        target: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        self.prepare_requests(starting_block, target);

        let cap = (*target - *starting_block) as usize;
        let mut headers = Vec::<BlockHeader>::with_capacity(cap);

        let mut ticker = tokio::time::interval(Duration::from_secs(15));

        while !self.pending_requests.is_empty() {
            let mut message_processed = false;
            tokio::select! {
                msg = stream.next() => {
                    if let Some(msg) = msg {
                        message_processed = true;
                        if let Message::BlockHeaders(msg) = msg.msg {
                            if self.prevalidate_block_headers(&msg.headers) {
                                let _v = self.pending_requests.remove(&msg.headers[0].number);
                                debug_assert!(_v.is_some());
                                info!(
                                    "Received block headers for {}->{}, requests left={}",
                                    msg.headers[0].number,
                                    msg.headers.last().unwrap().number,
                                    self.pending_requests.len()
                                );
                                headers.extend(msg.headers.into_iter());
                            }
                        }
                    }
                }
                _ = ticker.tick() => {}
            }

            if !message_processed {
                self.pending_requests
                    .values()
                    .cloned()
                    .into_iter()
                    .map(|request| {
                        let peer = self.peer.clone();
                        tokio::spawn(async move { peer.send_header_request(request).await })
                    })
                    .collect::<FuturesUnordered<_>>()
                    .for_each(|_| async {})
                    .await;
            }
        }

        let valid_till = self.verify_chunks(&mut headers);

        if valid_till > 0 {
            headers.truncate(valid_till - 1);
        };

        Ok(headers)
    }

    async fn evaluate_chain_tip(
        &mut self,
        stream: &mut InboundStream,
    ) -> anyhow::Result<(BlockNumber, H256)> {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));

        let prevalidate_block_hashes = |s: &mut Self, hashes: &[BlockHashAndNumber]| -> bool {
            !hashes.is_empty()
                && !s.seen_announces.contains_key(&hashes.last().unwrap().hash)
                && hashes.last().unwrap().number >= s.height
        };

        loop {
            match future::select(stream.next().fuse(), ticker.tick().boxed().fuse()).await {
                future::Either::Left((Some(msg), _)) => match msg.msg {
                    Message::NewBlockHashes(msg) if prevalidate_block_hashes(self, &msg.0) => {
                        let block = msg.0.last().unwrap();
                        self.seen_announces.insert(block.hash, ());
                        self.peer
                            .send_header_request(HeaderRequest {
                                start: block.hash.into(),
                                limit: 1,
                                ..Default::default()
                            })
                            .await?;
                    }
                    Message::BlockHeaders(msg) => {
                        if msg.headers.len() == 1 && msg.headers[0].number > self.peer.last_ping() {
                            let header = &msg.headers[0];
                            let hash = header.hash();

                            self.parents_table.insert(hash, header.parent_hash);
                            self.blocks_table.insert(hash, (header.number, None));
                            self.peer
                                .send_header_request(HeaderRequest {
                                    start: hash.into(),
                                    limit: 1,
                                    skip: 1,
                                    ..Default::default()
                                })
                                .await?;
                            if self.height < header.number {
                                self.height = header.number;
                            }
                        }
                    }
                    Message::NewBlock(msg) => {
                        let (hash, number, parent_hash) = (
                            msg.block.header.hash(),
                            msg.block.header.number,
                            msg.block.header.parent_hash,
                        );
                        self.parents_table.insert(hash, parent_hash);
                        self.blocks_table
                            .insert(hash, (number, Some(msg.total_difficulty)));
                        self.peer
                            .send_header_request(HeaderRequest {
                                start: hash.into(),
                                limit: 1,
                                skip: 1,
                                ..Default::default()
                            })
                            .await?;
                        if self.height < number {
                            self.height = number;
                        }
                    }
                    _ => continue,
                },
                _ => {
                    self.peer.send_ping().await?;
                    if let Some((number, hash)) = self.try_find_tip().await {
                        self.height = number;
                        self.marker = hash;
                        return Ok((number, hash));
                    }

                    self.parents_table
                        .clone()
                        .into_iter()
                        .flat_map(|(key, value)| {
                            (1..3)
                                .map(|skip| {
                                    info!("Requesting block headers for {:?}->{:?}", key, value);
                                    let peer = self.peer.clone();
                                    tokio::task::spawn(async move {
                                        let _ = peer
                                            .send_header_request(HeaderRequest {
                                                start: key.into(),
                                                limit: 1,
                                                skip,
                                                ..Default::default()
                                            })
                                            .await;
                                        let _ = peer
                                            .send_header_request(HeaderRequest {
                                                start: value.into(),
                                                limit: 1,
                                                skip,
                                                ..Default::default()
                                            })
                                            .await;
                                    })
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<FuturesUnordered<_>>()
                        .for_each(|_| async {})
                        .await;
                }
            }
        }
    }

    async fn try_find_tip(&mut self) -> Option<(BlockNumber, H256)> {
        let possible_tips = self
            .parents_table
            .clone()
            .into_iter()
            .map(|(k, _)| k)
            .collect::<HashSet<_>>();
        let mut longest_chain = LinkedHashSet::new();
        for tip in &possible_tips {
            let mut path = LinkedHashSet::new();
            path.insert(*tip);

            let mut current = *tip;
            while let Some(v) = self.parents_table.get(&current) {
                current = *v;
                path.insert(current);
            }

            if path.len() > longest_chain.len() {
                longest_chain = path;
            }
        }

        possible_tips
            .into_iter()
            .map(|hash| {
                let peer = self.peer.clone();
                tokio::spawn(async move {
                    let _ = peer
                        .send_header_request(HeaderRequest {
                            start: hash.into(),
                            limit: 1,
                            skip: 1,
                            ..Default::default()
                        })
                        .await;
                })
            })
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;

        if longest_chain.len() > 1 {
            let tip = longest_chain.pop_front().unwrap();
            let number = self.blocks_table.get(&tip).unwrap().0;
            self.parents_table.clear();
            Some((number, tip))
        } else {
            None
        }
    }

    fn verify_chunks(&self, headers: &mut [BlockHeader]) -> usize {
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

        valid_till.load(Ordering::SeqCst)
    }
}

fn insert<E: EnvironmentKind>(
    txn: &mut MdbxTransaction<'_, RW, E>,
    headers: &mut Vec<BlockHeader>,
) -> anyhow::Result<()> {
    let mut cursor_header_number = txn.cursor(tables::HeaderNumber)?;
    let mut cursor_header = txn.cursor(tables::Header)?;
    let mut cursor_canonical = txn.cursor(tables::CanonicalHeader)?;
    let mut cursor_td = txn.cursor(tables::HeadersTotalDifficulty)?;
    let mut td = cursor_td.last()?.map(|((_, _), v)| v).unwrap();

    let mut block_headers = Vec::<(H256, BlockHeader)>::with_capacity(headers.len());
    headers
        .par_drain(..)
        .map(|header| (header.hash(), header))
        .collect_into_vec(&mut block_headers);

    for (hash, header) in block_headers {
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
