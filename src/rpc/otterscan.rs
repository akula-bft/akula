use super::helpers;
use crate::{
    accessors::chain,
    consensus::{engine_factory, FinalizationChange},
    execution::{
        analysis_cache::AnalysisCache,
        processor::{execute_transaction, ExecutionProcessor},
        tracer::{CallKind, MessageKind, NoopTracer, Tracer},
    },
    kv::{
        mdbx::*,
        tables::{self, BitmapKey},
        traits::*,
        MdbxWithDirHandle,
    },
    models::*,
    Buffer, IntraBlockState,
};
use anyhow::format_err;
use async_trait::async_trait;
use bytes::Bytes;
use croaring::Treemap;
use ethereum_jsonrpc::{
    types, BlockData, BlockDetails, BlockTransactions, ContractCreatorData, InternalOperation,
    Issuance, OperationType, OtterscanApiServer, ReceiptWithTimestamp, TraceEntry,
    TransactionsWithReceipts,
};
use jsonrpsee::core::RpcResult;
use std::{cmp::Ordering, sync::Arc};
use tokio::pin;

pub struct OtterscanApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
}

fn get_block_details_inner<K, E>(
    tx: &MdbxTransaction<'_, K, E>,
    block_id: impl Into<types::BlockId>,
    include_txs: bool,
) -> RpcResult<Option<BlockDetails>>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    if let Some(block) = helpers::construct_block(tx, block_id, include_txs, None)? {
        let block_number = block.number.unwrap().as_u64().into();
        let block_hash = block.hash.unwrap();

        let header = tx.get(tables::Header, (block_number, block_hash))?.unwrap();
        let ommers = tx
            .get(tables::BlockBody, (block_number, block_hash))?
            .unwrap()
            .uncles;

        let chainspec = tx
            .get(tables::Config, ())?
            .ok_or_else(|| format_err!("no chainspec found"))?;
        let buffer = Buffer::new(tx, None);
        let finalization_changes =
            engine_factory(None, chainspec, None)?.finalize(&header, &ommers, None, &buffer)?;

        let mut block_reward = U256::ZERO;
        let mut uncle_reward = U256::ZERO;

        for change in finalization_changes {
            match change {
                FinalizationChange::Reward { amount, ommer, .. } => {
                    *if ommer {
                        &mut uncle_reward
                    } else {
                        &mut block_reward
                    } += amount;
                }
            }
        }
        let issuance = block_reward + uncle_reward;

        let mut details = BlockDetails {
            block: BlockData {
                transaction_count: block.transactions.len() as u64,
                inner: block,
            },
            issuance: Issuance {
                block_reward,
                uncle_reward,
                issuance,
            },
            total_fees: U256::ZERO,
        };

        if !include_txs {
            details.block.inner.transactions.clear();
        }
        return Ok(Some(details));
    }

    Ok(None)
}

fn search_trace_block<K, E>(
    txn: &MdbxTransaction<'_, K, E>,
    addr: Address,
    chain_spec: &ChainSpec,
    block_number: BlockNumber,
) -> anyhow::Result<TransactionsWithReceipts>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    #[derive(Debug, Default)]
    struct TouchTracer {
        addr: Address,
        touched: bool,
    }

    impl TouchTracer {
        fn new(addr: Address) -> Self {
            Self {
                addr,
                touched: false,
            }
        }

        fn touched(&self) -> bool {
            self.touched
        }
    }

    impl Tracer for TouchTracer {
        fn capture_start(
            &mut self,
            _: u16,
            _: Address,
            _: Address,
            real_sender: Address,
            code_address: Address,
            _: MessageKind,
            _: Bytes,
            _: u64,
            _: U256,
        ) {
            self.touched |= self.addr == real_sender;
            self.touched |= self.addr == code_address;
        }
    }

    let mut buffer = Buffer::new(txn, Some(BlockNumber(block_number.0 - 1)));
    let mut state = IntraBlockState::new(&mut buffer);

    let block_spec = chain_spec.collect_block_spec(block_number);
    let mut analysis_cache = AnalysisCache::default();

    let mut prev_cumulative_gas_used = 0;
    let mut cumulative_gas_used = 0;

    let block_hash = chain::canonical_hash::read(txn, block_number)?
        .ok_or_else(|| format_err!("no canonical hash for block #{block_number}"))?;
    let header: BlockHeader = chain::header::read(txn, block_hash, block_number)?
        .ok_or_else(|| format_err!("no header for block #{block_number}"))?;
    let senders = chain::tx_sender::read(txn, block_hash, block_number)?;
    let messages = chain::block_body::read_without_senders(txn, block_hash, block_number)?
        .ok_or_else(|| format_err!("where's block body"))?
        .transactions;

    for (&address, &balance) in &block_spec.balance_changes {
        state.set_balance(address, balance)?;
    }

    let mut results = TransactionsWithReceipts {
        txs: Vec::new(),
        receipts: Vec::new(),
        first_page: false,
        last_page: false,
    };

    let engine = engine_factory(None, chain_spec.clone(), None)?;
    let beneficiary = engine.get_beneficiary(&header);
    for (transaction_index, (transaction, sender)) in messages.into_iter().zip(senders).enumerate()
    {
        let mut tracer = TouchTracer::new(addr);
        let receipt = execute_transaction(
            &mut state,
            &block_spec,
            &header,
            &mut tracer,
            &mut analysis_cache,
            &mut cumulative_gas_used,
            &transaction.message,
            sender,
            beneficiary,
        )?
        .1;

        if tracer.touched() {
            let gas_used = U64::from(cumulative_gas_used - prev_cumulative_gas_used);

            let transaction_hash = transaction.hash();

            let logs = receipt
                .logs
                .iter()
                .enumerate()
                .map(|(i, log)| types::TransactionLog {
                    log_index: Some(U64::from(i)),
                    transaction_index: Some(U64::from(transaction_index)),
                    transaction_hash: Some(transaction_hash),
                    block_hash: Some(block_hash),
                    block_number: Some(U64::from(block_number.0)),
                    address: log.address,
                    data: log.data.clone().into(),
                    topics: log.topics.clone(),
                })
                .collect::<Vec<_>>();

            let block_number = U64::from(block_number.0);

            let receipt = ReceiptWithTimestamp {
                base: types::TransactionReceipt {
                    transaction_hash,
                    transaction_index: U64::from(transaction_index),
                    block_hash,
                    block_number,
                    from: sender,
                    to: transaction.action().into_address(),
                    cumulative_gas_used: receipt.cumulative_gas_used.into(),
                    gas_used,
                    contract_address: if let TransactionAction::Create =
                        transaction.message.action()
                    {
                        Some(crate::execution::address::create_address(
                            sender,
                            transaction.nonce(),
                        ))
                    } else {
                        None
                    },
                    logs,
                    logs_bloom: receipt.bloom,
                    status: if receipt.success {
                        U64::from(1_u16)
                    } else {
                        U64::zero()
                    },
                },
                timestamp: header.timestamp.into(),
            };

            let transaction = helpers::new_jsonrpc_tx(
                transaction,
                sender,
                Some(transaction_index as u64),
                Some(block_hash),
                Some(block_number.as_u64().into()),
            );

            results.txs.push(transaction);
            results.receipts.push(receipt);
        }

        prev_cumulative_gas_used = cumulative_gas_used;
    }

    Ok(results)
}

fn trace_blocks<K, E>(
    txn: &MdbxTransaction<'_, K, E>,
    addr: Address,
    chain_config: &ChainSpec,
    page_size: usize,
    result_count: usize,
    call_from_to_provider: &mut impl Iterator<Item = anyhow::Result<BlockNumber>>,
) -> anyhow::Result<(Vec<TransactionsWithReceipts>, bool)>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    // Estimate the common case of user address having at most 1 interaction/block and
    // trace N := remaining page matches as number of blocks to trace concurrently.
    // TODO: this is not optimimal for big contract addresses; implement some better heuristics.
    let est_blocks_to_trace = page_size - result_count;
    let mut results = Vec::with_capacity(est_blocks_to_trace);
    let mut has_more = true;

    for _ in 0..est_blocks_to_trace {
        if let Some(block) = call_from_to_provider.next().transpose()? {
            results.push(search_trace_block(txn, addr, chain_config, block)?);
        } else {
            has_more = false;
        }
    }

    Ok((results, has_more))
}

pub fn try_merge<'a, T, E>(
    mut it1: impl Iterator<Item = Result<T, E>> + 'a,
    mut it2: impl Iterator<Item = Result<T, E>> + 'a,
    forward: bool,
) -> impl Iterator<Item = Result<T, E>> + 'a
where
    T: Copy + Ord,
    E: 'a,
{
    TryGenIter::from(move || {
        let mut a = it1.next().transpose()?;
        let mut b = it2.next().transpose()?;

        loop {
            match (a, b) {
                (None, None) => {
                    break;
                }
                (Some(e), None) => {
                    yield e;
                    a = it1.next().transpose()?;
                }
                (None, Some(e)) => {
                    yield e;
                    b = it2.next().transpose()?;
                }
                (Some(a_e), Some(b_e)) => match a_e.cmp(&b_e) {
                    Ordering::Less => {
                        if forward {
                            yield a_e;
                            a = it1.next().transpose()?;
                        } else {
                            yield b_e;
                            b = it2.next().transpose()?;
                        }
                    }
                    Ordering::Equal => {
                        yield a_e;
                        a = it1.next().transpose()?;
                        b = it2.next().transpose()?;
                    }
                    Ordering::Greater => {
                        if forward {
                            yield b_e;
                            b = it2.next().transpose()?;
                        } else {
                            yield a_e;
                            a = it1.next().transpose()?;
                        }
                    }
                },
            }
        }

        Ok(())
    })
}

#[async_trait]
impl<DB> OtterscanApiServer for OtterscanApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn get_api_level(&self) -> RpcResult<u8> {
        Ok(8)
    }
    async fn get_internal_operations(&self, hash: H256) -> RpcResult<Vec<InternalOperation>> {
        #[derive(Debug, Default)]
        struct OperationsTracer {
            results: Vec<InternalOperation>,
        }

        impl Tracer for OperationsTracer {
            fn capture_start(
                &mut self,
                depth: u16,
                from: Address,
                to: Address,
                _: Address,
                _: Address,
                call_type: MessageKind,
                _: Bytes,
                _: u64,
                value: U256,
            ) {
                if depth > 0 {
                    match call_type {
                        MessageKind::Create { salt } => {
                            self.results.push(InternalOperation {
                                op_type: if salt.is_some() {
                                    OperationType::Create2
                                } else {
                                    OperationType::Create
                                },
                                from,
                                to,
                                value,
                            });
                        }
                        MessageKind::Call { call_kind, .. } => {
                            if matches!(call_kind, CallKind::Call) && value > 0 {
                                self.results.push(InternalOperation {
                                    op_type: OperationType::Transfer,
                                    from,
                                    to,
                                    value,
                                });
                            }
                        }
                    }
                }
            }
            fn capture_self_destruct(
                &mut self,
                caller: Address,
                beneficiary: Address,
                balance: U256,
            ) {
                self.results.push(InternalOperation {
                    op_type: OperationType::SelfDestruct,
                    from: caller,
                    to: beneficiary,
                    value: balance,
                })
            }
        }

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            if let Some(block_number) = chain::tl::read(&txn, hash)? {
                let block_hash = chain::canonical_hash::read(&txn, block_number)?
                    .ok_or_else(|| format_err!("no canonical header for block #{block_number:?}"))?;
                let header = chain::header::read(&txn, block_hash, block_number)?.ok_or_else(|| {
                    format_err!("header not found for block #{block_number}/{block_hash}")
                })?;
                let block_body = chain::block_body::read_with_senders(&txn, block_hash, block_number)?
                    .ok_or_else(|| {
                        format_err!("body not found for block #{block_number}/{block_hash}")
                    })?;
                let chain_spec = chain::chain_config::read(&txn)?
                    .ok_or_else(|| format_err!("chain specification not found"))?;

                // Prepare the execution context.
                let mut buffer = Buffer::new(&txn, Some(BlockNumber(block_number.0 - 1)));

                let block_execution_spec = chain_spec.collect_block_spec(block_number);
                let mut engine = engine_factory(None, chain_spec.clone(), None)?;
                let mut analysis_cache = AnalysisCache::default();
                let mut tracer = NoopTracer;

                let mut processor = ExecutionProcessor::new(
                    &mut buffer,
                    &mut tracer,
                    &mut analysis_cache,
                    &mut *engine,
                    &header,
                    &block_body,
                    &block_execution_spec,
                    &chain_spec,
                );

                let transaction_index = chain::block_body::read_without_senders(&txn, block_hash, block_number)?.ok_or_else(|| format_err!("where's block body"))?.transactions
                    .into_iter()
                    .enumerate()
                    .find(|(_, tx)| tx.hash() == hash)
                    .ok_or_else(|| format_err!("transaction {hash} not found in block #{block_number}/{block_hash} despite lookup index"))?.0;

                processor.execute_block_no_post_validation_while(|i, _| i < transaction_index)?;

                let tx = block_body.transactions.get(transaction_index).ok_or_else(|| format_err!("block #{block_number}/{block_hash} too short: tx #{transaction_index} not in body"))?;
                let mut operations_tracer = OperationsTracer::default();
                processor.set_tracer(&mut operations_tracer);
                processor.execute_transaction(&tx.message, tx.sender)?;

                return Ok(operations_tracer.results);
            }

            Ok(vec![])
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn search_transactions_before(
        &self,
        addr: Address,
        block_num: u64,
        page_size: usize,
    ) -> RpcResult<TransactionsWithReceipts> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let dbtx = db.begin()?;

            let call_from_cursor = dbtx.cursor(tables::CallFromIndex)?;
            let call_to_cursor = dbtx.cursor(tables::CallToIndex)?;

            let chain_config = dbtx
                .get(tables::Config, ())?
                .ok_or_else(|| format_err!("chain spec not found"))?;

            let (start_block, first_page) = if let Some(b) = block_num.checked_sub(1) {
                // Internal search code considers block_num [including], so adjust the value
                (Some(b.into()), false)
            } else {
                (None, true)
            };

            // Initialize search cursors at the first shard >= desired block number
            let mut call_from_to_provider = try_merge(
                call_from_cursor.walk_chunks_back(addr, start_block),
                call_to_cursor.walk_chunks_back(addr, start_block),
                false,
            );

            let mut txs = Vec::with_capacity(page_size);
            let mut receipts = Vec::with_capacity(page_size);

            let mut result_count = 0;
            let mut has_more = true;
            loop {
                if result_count >= page_size || !has_more {
                    break;
                }

                let results;
                (results, has_more) = trace_blocks(
                    &dbtx,
                    addr,
                    &chain_config,
                    page_size,
                    result_count,
                    &mut call_from_to_provider,
                )?;

                for r in results {
                    result_count += r.txs.len();
                    for tx in r.txs.into_iter().rev() {
                        txs.push(tx)
                    }
                    for receipt in r.receipts.into_iter().rev() {
                        receipts.push(receipt);
                    }

                    if result_count >= page_size {
                        break;
                    }
                }
            }

            Ok(TransactionsWithReceipts {
                txs,
                receipts,
                first_page,
                last_page: !has_more,
            })
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn search_transactions_after(
        &self,
        addr: Address,
        block_num: u64,
        page_size: usize,
    ) -> RpcResult<TransactionsWithReceipts> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let dbtx = db.begin()?;

            let call_from_cursor = dbtx.cursor(tables::CallFromIndex)?;
            let call_to_cursor = dbtx.cursor(tables::CallToIndex)?;

            let chain_config = dbtx
                .get(tables::Config, ())?
                .ok_or_else(|| format_err!("chain spec not found"))?;

            let (start_block, last_page) = if block_num > 0 {
                // Internal search code considers block_num [including], so adjust the value
                (Some((block_num + 1).into()), false)
            } else {
                (None, true)
            };

            // Initialize search cursors at the first shard >= desired block number
            let mut call_from_to_provider = try_merge(
                call_from_cursor.walk_chunks(addr, start_block),
                call_to_cursor.walk_chunks(addr, start_block),
                true,
            );

            let mut txs = Vec::with_capacity(page_size);
            let mut receipts = Vec::with_capacity(page_size);

            let mut result_count = 0;
            let mut has_more = true;
            loop {
                if result_count >= page_size || !has_more {
                    break;
                }

                let results;
                (results, has_more) = trace_blocks(
                    &dbtx,
                    addr,
                    &chain_config,
                    page_size,
                    result_count,
                    &mut call_from_to_provider,
                )?;

                for mut r in results {
                    result_count += r.txs.len();
                    txs.append(&mut r.txs);
                    receipts.append(&mut r.receipts);

                    if result_count >= page_size {
                        break;
                    }
                }
            }

            txs.reverse();
            receipts.reverse();

            Ok(TransactionsWithReceipts {
                txs,
                receipts,
                first_page: !has_more,
                last_page,
            })
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn get_block_details(&self, number: u64) -> RpcResult<Option<BlockDetails>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            get_block_details_inner(&txn, types::BlockNumber::Number(number.into()), false)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn get_block_details_by_hash(&self, hash: H256) -> RpcResult<Option<BlockDetails>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            get_block_details_inner(&txn, hash, false)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn get_block_transactions(
        &self,
        number: u64,
        page_number: usize,
        page_size: usize,
    ) -> RpcResult<Option<BlockTransactions>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            if let Some(mut block_details) =
                get_block_details_inner(&txn, types::BlockNumber::Number(number.into()), true)?
            {
                let page_end = block_details
                    .block
                    .inner
                    .transactions
                    .len()
                    .saturating_sub(page_number * page_size);
                let page_start = page_end.saturating_sub(page_size);

                block_details.block.inner.transactions = block_details
                    .block
                    .inner
                    .transactions
                    .get(page_start..page_end)
                    .map(|v| v.to_vec())
                    .unwrap_or_default();

                return Ok(Some(BlockTransactions {
                    receipts: helpers::get_receipts(&txn, number.into())?,
                    fullblock: block_details.block.inner,
                }));
            }

            Ok(None)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn has_code(&self, address: Address, block_id: types::BlockId) -> RpcResult<bool> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            if let Some((block_number, _)) = helpers::resolve_block_id(&txn, block_id)? {
                if let Some(account) =
                    crate::accessors::state::account::read(&txn, address, Some(block_number))?
                {
                    return Ok(account.code_hash != EMPTY_HASH);
                }
            }

            Ok(false)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn trace_transaction(&self, hash: H256) -> RpcResult<Vec<TraceEntry>> {
        let _ = hash;
        Ok(vec![])
    }
    async fn get_transaction_error(&self, hash: H256) -> RpcResult<types::Bytes> {
        let _ = hash;
        Ok(types::Bytes::default())
    }
    async fn get_transaction_by_sender_and_nonce(
        &self,
        addr: Address,
        nonce: u64,
    ) -> RpcResult<Option<H256>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            let acc_history_cursor = txn.cursor(tables::AccountHistory)?;
            let mut acc_change_cursor = txn.cursor(tables::AccountChangeSet)?;

            // Locate the chunk where the nonce happens
            let mut max_bl_prev_chunk = BlockNumber(0);
            let mut bitmap = Treemap::default();
            let mut acc = None;

            let walker = acc_history_cursor.walk(Some(BitmapKey {
                inner: addr,
                block_number: 0.into(),
            }));
            pin!(walker);

            while let Some((BitmapKey { inner, .. }, b)) = walker.next().transpose()? {
                if inner != addr {
                    break;
                }

                bitmap = b;

                // Inspect block changeset
                let max_bl = bitmap.maximum().unwrap_or(0);

                if let Some(a) = acc_change_cursor
                    .find_account(max_bl.into(), addr)?
                    .ok_or_else(|| format_err!("account not found"))?
                {
                    if a.nonce > nonce {
                        acc = Some(a);
                        break;
                    }
                }

                max_bl_prev_chunk = BlockNumber(max_bl);
            }

            if acc.is_none() {
                // Check plain state
                if let Some(a) = txn.get(tables::Account, addr)? {
                    // Nonce changed in plain state, so it means the last block of last chunk
                    // contains the actual nonce change
                    if a.nonce > nonce {
                        acc = Some(a);
                    }
                }
            }

            Ok(if acc.is_some() {
                // Locate the exact block inside chunk when the nonce changed
                let blocks = bitmap.iter().collect::<Vec<_>>();
                let mut idx = 0;
                for (i, block) in blocks.iter().enumerate() {
                    // Locate the block changeset
                    if let Some(acc) = acc_change_cursor
                        .find_account((*block).into(), addr)?
                        .ok_or_else(|| format_err!("account not found"))?
                    {
                        // Since the state contains the nonce BEFORE the block changes, we look for
                        // the block when the nonce changed to be > the desired once, which means the
                        // previous history block contains the actual change; it may contain multiple
                        // nonce changes.
                        if acc.nonce > nonce {
                            idx = i;
                            break;
                        }
                    }
                }

                // Since the changeset contains the state BEFORE the change, we inspect
                // the block before the one we found; if it is the first block inside the chunk,
                // we use the last block from prev chunk
                let nonce_block = if idx > 0 {
                    BlockNumber(blocks[idx - 1])
                } else {
                    max_bl_prev_chunk
                };

                let hash = crate::accessors::chain::canonical_hash::read(&txn, nonce_block)?
                    .ok_or_else(|| format_err!("canonical hash not found for block {nonce_block}"))?;
                let txs =
                    crate::accessors::chain::block_body::read_without_senders(&txn, hash, nonce_block)?
                        .ok_or_else(|| format_err!("body not found for block {nonce_block}"))?
                        .transactions;

                Some({
                    txs
                        .into_iter()
                        .find(|t| t.nonce() == nonce)
                        .ok_or_else(|| format_err!("body for block #{nonce_block} does not contain tx for {addr}/#{nonce} despite indications for otherwise"))?
                        .hash()
                })
            } else {
                None
            })
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn get_contract_creator(&self, addr: Address) -> RpcResult<Option<ContractCreatorData>> {
        let _ = addr;
        Ok(None)
    }
}
