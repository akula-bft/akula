use super::helpers;
use crate::{
    accessors::{chain, state},
    consensus::engine_factory,
    execution::{
        analysis_cache::AnalysisCache, evmglue, processor::ExecutionProcessor, tracer::NoopTracer,
    },
    kv::{mdbx::*, tables, MdbxWithDirHandle},
    models::*,
    stages::{self, FINISH},
    Buffer, IntraBlockState,
};
use anyhow::format_err;
use async_trait::async_trait;
use ethereum_jsonrpc::{
    types::{self, TransactionLog},
    EthApiServer, LogFilter, SyncStatus,
};
use jsonrpsee::core::RpcResult;
use std::{collections::HashSet, sync::Arc};

pub struct EthApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
    pub call_gas_limit: u64,
}

fn filter_log(
    log: &Log,
    addresses: &Option<HashSet<H160>>,
    topics: &Option<Vec<Option<HashSet<H256>>>>,
) -> bool {
    if let Some(addresses) = addresses.as_ref() {
        if !addresses.is_empty() && !addresses.contains(&log.address) {
            return false;
        }
    }

    if let Some(topic_filters) = topics.as_ref() {
        if topic_filters.len() > log.topics.len() {
            return false;
        }

        for (topic_filter, topic) in topic_filters.iter().zip(&log.topics) {
            if let Some(topic_filter) = topic_filter {
                if !topic_filter.is_empty() && !topic_filter.contains(topic) {
                    return false;
                }
            }
        }
    }

    true
}

#[async_trait]
impl<DB> EthApiServer for EthApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn block_number(&self) -> RpcResult<U64> {
        Ok(U64::from(
            self.db
                .begin()?
                .get(tables::SyncStage, FINISH)?
                .unwrap_or(BlockNumber(0))
                .0,
        ))
    }

    async fn get_logs(&self, filter: LogFilter) -> RpcResult<Vec<TransactionLog>> {
        let db = self.db.clone();

        let (logtx, mut logrx) = tokio::sync::mpsc::channel(1);

        let addresses = filter
            .address
            .map(|addresses| addresses.0.into_iter().collect::<HashSet<_>>());

        let topics = filter.topics.map(|topics| {
            // Reverse, cut the dead tail, reverse again
            let mut topics = topics
                .into_iter()
                .rev()
                .skip_while(|v| {
                    if let Some(topic_filter) = v {
                        if !topic_filter.0.is_empty() {
                            return false;
                        }
                    }
                    true
                })
                .map(|topic_filter| {
                    topic_filter.map(|topics| topics.0.into_iter().collect::<HashSet<_>>())
                })
                .collect::<Vec<_>>();

            topics.reverse();

            topics
        });

        tokio::task::spawn_blocking(move || {
            let f = {
                let logtx = logtx.clone();
                move || {
                    let txn = db.begin()?;

                    let block_range = match filter.block_filter {
                        Some(filter) => match filter {
                            ethereum_jsonrpc::BlockFilter::Exact { block_hash } => {
                                if let Some((number, _)) = helpers::resolve_block_id(
                                    &txn,
                                    types::BlockId::Hash(block_hash),
                                )? {
                                    number.0..=number.0
                                } else {
                                    return Ok(());
                                }
                            }
                            ethereum_jsonrpc::BlockFilter::Bounded {
                                from_block,
                                to_block,
                            } => {
                                let from = helpers::resolve_block_number(
                                    &txn,
                                    from_block.unwrap_or(types::BlockNumber::Latest),
                                )?;
                                let to = helpers::resolve_block_number(
                                    &txn,
                                    to_block.unwrap_or(types::BlockNumber::Latest),
                                )?;

                                from.0..=to.0
                            }
                        },
                        None => {
                            let latest =
                                helpers::resolve_block_number(&txn, types::BlockNumber::Latest)?;
                            latest.0..=latest.0
                        }
                    };

                    for block_number in block_range {
                        let block_number = BlockNumber(block_number);
                        let block_hash =
                            crate::accessors::chain::canonical_hash::read(&txn, block_number)?
                                .ok_or_else(|| {
                                    format_err!("no canonical hash for block #{block_number}")
                                })?;

                        let header = chain::header::read(&txn, block_hash, block_number)?
                            .ok_or_else(|| {
                                format_err!(
                                    "header not found for block #{block_number}/{block_hash}"
                                )
                            })?;
                        let block_body =
                            chain::block_body::read_with_senders(&txn, block_hash, block_number)?
                                .ok_or_else(|| {
                                format_err!("body not found for block #{block_number}/{block_hash}")
                            })?;
                        let txhashes = block_body
                            .transactions
                            .iter()
                            .map(|tx| tx.hash())
                            .collect::<Vec<_>>();
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

                        let receipts = processor.execute_block_no_post_validation()?;

                        for ((transaction_index, receipt), txhash) in
                            receipts.into_iter().enumerate().zip(txhashes)
                        {
                            for (i, log) in receipt.logs.into_iter().enumerate() {
                                if filter_log(&log, &addresses, &topics) {
                                    let sent = logtx
                                        .blocking_send(Ok(types::TransactionLog {
                                            log_index: Some(U64::from(i)),
                                            transaction_index: Some(U64::from(transaction_index)),
                                            transaction_hash: Some(txhash),
                                            block_hash: Some(block_hash),
                                            block_number: Some(U64::from(block_number.0)),
                                            address: log.address,
                                            data: log.data.clone().into(),
                                            topics: log.topics.clone(),
                                        }))
                                        .is_ok();
                                    if !sent {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }

                    Ok(())
                }
            };
            if let Err::<_, anyhow::Error>(e) = (f)() {
                let _ = logtx.blocking_send(Err(e));
            }
        });

        let mut out = vec![];
        while let Some(v) = logrx.recv().await.transpose()? {
            out.push(v);
        }
        Ok(out)
    }

    async fn chain_id(&self) -> RpcResult<U64> {
        Ok(chain::chain_config::read(&self.db.begin()?)?
            .ok_or_else(|| format_err!("chain specification not found"))?
            .params
            .chain_id
            .0
            .into())
    }

    async fn call(
        &self,
        call_data: types::MessageCall,
        block_number: types::BlockNumber,
    ) -> RpcResult<types::Bytes> {
        let db = self.db.clone();
        let call_gas_limit = self.call_gas_limit;

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            let (block_number, block_hash) = helpers::resolve_block_id(&txn, block_number)?
                .ok_or_else(|| format_err!("failed to resolve block {block_number:?}"))?;

            let chain_id = txn
                .get(tables::Config, ())?
                .ok_or_else(|| format_err!("chain spec not found"))?
                .params
                .chain_id;

            let header = chain::header::read(&txn, block_hash, block_number)?
                .ok_or_else(|| format_err!("Header not found for #{block_number}/{block_hash}"))?;

            let mut buffer = Buffer::new(&txn, Some(block_number));

            let (sender, message) = helpers::convert_message_call(
                &buffer,
                chain_id,
                call_data,
                &header,
                U256::ZERO,
                Some(call_gas_limit),
            )?;

            let mut state = IntraBlockState::new(&mut buffer);

            let mut analysis_cache = AnalysisCache::default();
            let chain_spec = chain::chain_config::read(&txn)?
                .ok_or_else(|| format_err!("no chainspec found"))?;
            let block_spec = chain_spec.collect_block_spec(block_number);

            let mut tracer = NoopTracer;

            let beneficiary = engine_factory(None, chain_spec, None)?.get_beneficiary(&header);
            Ok(evmglue::execute(
                &mut state,
                &mut tracer,
                &mut analysis_cache,
                &header,
                &block_spec,
                &message,
                sender,
                beneficiary,
                message.gas_limit(),
            )?
            .output_data
            .into())
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn estimate_gas(
        &self,
        call_data: types::MessageCall,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            let (block_number, hash) = helpers::resolve_block_id(&txn, block_number)?
                .ok_or_else(|| format_err!("failed to resolve block {block_number:?}"))?;
            let chain_id = txn
                .get(tables::Config, ())?
                .ok_or_else(|| format_err!("chain spec not found"))?
                .params
                .chain_id;
            let header = chain::header::read(&txn, hash, block_number)?
                .ok_or_else(|| format_err!("no header found for block #{block_number}/{hash}"))?;
            let mut buffer = Buffer::new(&txn, Some(block_number));

            let (sender, message) = helpers::convert_message_call(
                &buffer,
                chain_id,
                call_data,
                &header,
                U256::ZERO,
                None,
            )?;

            let mut state = IntraBlockState::new(&mut buffer);

            let mut cache = AnalysisCache::default();
            let chain_spec = chain::chain_config::read(&txn)?
                .ok_or_else(|| format_err!("no chainspec found"))?;
            let block_spec = chain_spec.collect_block_spec(block_number);
            let mut tracer = NoopTracer;
            let gas_limit = header.gas_limit;

            let beneficiary = engine_factory(None, chain_spec, None)?.get_beneficiary(&header);
            Ok(U64::from(
                gas_limit as i64
                    - evmglue::execute(
                        &mut state,
                        &mut tracer,
                        &mut cache,
                        &header,
                        &block_spec,
                        &message,
                        sender,
                        beneficiary,
                        gas_limit,
                    )?
                    .gas_left,
            ))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_balance(
        &self,
        address: Address,
        block_number: types::BlockNumber,
    ) -> RpcResult<U256> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            Ok(state::account::read(
                &txn,
                address,
                Some(helpers::resolve_block_number(&txn, block_number)?),
            )?
            .map(|acc| acc.balance)
            .unwrap_or(U256::ZERO))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_block_by_hash(
        &self,
        hash: H256,
        include_txs: bool,
    ) -> RpcResult<Option<types::Block>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            Ok(helpers::construct_block(&txn, hash, include_txs, None)?)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn get_block_by_number(
        &self,
        block_number: types::BlockNumber,
        include_txs: bool,
    ) -> RpcResult<Option<types::Block>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(helpers::construct_block(
                &txn,
                block_number,
                include_txs,
                None,
            )?)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
    async fn get_transaction_by_hash(&self, hash: H256) -> RpcResult<Option<types::Tx>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            if let Some(block_number) = chain::tl::read(&txn, hash)? {
                let block_hash = chain::canonical_hash::read(&txn, block_number)?
                    .ok_or_else(|| format_err!("canonical hash for block #{block_number} not found"))?;
                let (index, transaction) = chain::block_body::read_without_senders(
                    &txn,
                    block_hash,
                    block_number,
                )?.ok_or_else(|| format_err!("body not found for block #{block_number}/{block_hash}"))?
                .transactions
                .into_iter()
                .enumerate()
                .find(|(_, tx)| tx.hash() == hash)
                .ok_or_else(|| {
                    format_err!(
                        "tx with hash {hash} is not found in block #{block_number}/{block_hash} - tx lookup index invalid?"
                    )
                })?;
                let senders = chain::tx_sender::read(&txn, block_hash, block_number)?;
                let sender = *senders
                    .get(index)
                    .ok_or_else(|| format_err!("senders to short: {index} vs len {}", senders.len()))?;
                return Ok(Some(types::Tx::Transaction(Box::new(
                    helpers::new_jsonrpc_tx(transaction, sender, Some(index as u64), Some(block_hash), Some(block_number)),
                ))));
            }

            Ok(None)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_block_transaction_count_by_hash(&self, hash: H256) -> RpcResult<U64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            let block_number = chain::header_number::read(&txn, hash)?
                .ok_or_else(|| format_err!("no header number for hash {hash} found"))?;
            Ok(U64::from(
                chain::block_body::read_without_senders(&txn, hash, block_number)?
                    .ok_or_else(|| format_err!("no body found for block #{block_number}/{hash}"))?
                    .transactions
                    .len(),
            ))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_block_transaction_count_by_number(
        &self,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            let (block_number, block_hash) = helpers::resolve_block_id(&txn, block_number)?
                .ok_or_else(|| format_err!("failed to resolve block {block_number:?}"))?;
            Ok(
                chain::block_body::read_without_senders(&txn, block_hash, block_number)?
                    .ok_or_else(|| {
                        format_err!("body not found for block #{block_number}/{block_hash}")
                    })?
                    .transactions
                    .len()
                    .into(),
            )
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_code(
        &self,
        address: Address,
        block_number: types::BlockNumber,
    ) -> RpcResult<types::Bytes> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            let block_number = helpers::resolve_block_number(&txn, block_number)?;
            Ok(
                if let Some(account) = state::account::read(&txn, address, Some(block_number))? {
                    txn.get(tables::Code, account.code_hash)?
                        .ok_or_else(|| {
                            format_err!("failed to find code for code hash {}", account.code_hash)
                        })?
                        .into()
                } else {
                    Default::default()
                },
            )
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_storage_at(
        &self,
        address: Address,
        key: U256,
        block_number: types::BlockNumber,
    ) -> RpcResult<U256> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(state::storage::read(
                &txn,
                address,
                key,
                Some(helpers::resolve_block_number(&txn, block_number)?),
            )?)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_transaction_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<Option<types::Tx>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(
                helpers::construct_block(&txn, block_hash, true, None)?.and_then(|mut block| {
                    let index = index.as_usize();
                    if index < block.transactions.len() {
                        Some(block.transactions.remove(index))
                    } else {
                        None
                    }
                }),
            )
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_transaction_by_block_number_and_index(
        &self,
        block_number: types::BlockNumber,
        index: U64,
    ) -> RpcResult<Option<types::Tx>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(
                helpers::construct_block(&txn, block_number, true, None)?.and_then(|mut block| {
                    let index = index.as_usize();
                    if index < block.transactions.len() {
                        Some(block.transactions.remove(index))
                    } else {
                        None
                    }
                }),
            )
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_transaction_count(
        &self,
        address: Address,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(state::account::read(
                &txn,
                address,
                Some(helpers::resolve_block_number(&txn, block_number)?),
            )?
            .map(|account| account.nonce)
            .unwrap_or(0)
            .into())
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_transaction_receipt(
        &self,
        hash: H256,
    ) -> RpcResult<Option<types::TransactionReceipt>> {
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

                let receipts =
                    processor.execute_block_no_post_validation_while(|i, _| i <= transaction_index)?;

                let transaction = &block_body.transactions[transaction_index];
                let receipt = receipts.get(transaction_index).unwrap();
                let gas_used = U64::from(
                    receipt.cumulative_gas_used
                        - transaction_index
                            .checked_sub(1)
                            .and_then(|last_index| receipts.get(last_index))
                            .map(|receipt| receipt.cumulative_gas_used)
                            .unwrap_or(0),
                );
                let logs = receipt
                    .logs
                    .iter()
                    .enumerate()
                    .map(|(i, log)| types::TransactionLog {
                        log_index: Some(U64::from(i)),
                        transaction_index: Some(U64::from(transaction_index)),
                        transaction_hash: Some(transaction.hash()),
                        block_hash: Some(block_hash),
                        block_number: Some(U64::from(block_number.0)),
                        address: log.address,
                        data: log.data.clone().into(),
                        topics: log.topics.clone(),
                    })
                    .collect::<Vec<_>>();

                return Ok(Some(types::TransactionReceipt {
                    transaction_hash: hash,
                    transaction_index: U64::from(transaction_index),
                    block_hash,
                    block_number: U64::from(block_number.0),
                    from: transaction.sender,
                    to: transaction.message.action().into_address(),
                    cumulative_gas_used: receipt.cumulative_gas_used.into(),
                    gas_used,
                    contract_address: if let TransactionAction::Create = transaction.message.action() {
                        Some(crate::execution::address::create_address(
                            transaction.sender,
                            transaction.message.nonce(),
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
                }));
            }

            Ok(None)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_uncle_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<Option<types::Block>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(helpers::construct_block(
                &txn,
                block_hash,
                false,
                Some(index),
            )?)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_uncle_by_block_number_and_index(
        &self,
        block_number: types::BlockNumber,
        index: U64,
    ) -> RpcResult<Option<types::Block>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            Ok(helpers::construct_block(
                &txn,
                block_number,
                false,
                Some(index),
            )?)
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_uncle_count_by_block_hash(&self, block_hash: H256) -> RpcResult<U64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            let (block_number, block_hash) = helpers::resolve_block_id(&txn, block_hash)?
                .ok_or_else(|| format_err!("failed to resolve block {block_hash}"))?;
            Ok(U64::from(
                chain::storage_body::read(&txn, block_hash, block_number)?
                    .map(|body| body.uncles.len())
                    .unwrap_or(0),
            ))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn get_uncle_count_by_block_number(
        &self,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;
            let (block_number, block_hash) = helpers::resolve_block_id(&txn, block_number)?
                .ok_or_else(|| format_err!("failed to resolve block #{block_number:?}"))?;
            Ok(U64::from(
                chain::storage_body::read(&txn, block_hash, block_number)?
                    .map(|body| body.uncles.len())
                    .unwrap_or(0),
            ))
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }

    async fn syncing(&self) -> RpcResult<SyncStatus> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let txn = db.begin()?;

            let highest_block = stages::HEADERS
                .get_progress(&txn)?
                .unwrap_or(BlockNumber(0));
            let current_block = stages::FINISH.get_progress(&txn)?.unwrap_or(BlockNumber(0));

            Ok(if current_block > 0 && current_block >= highest_block {
                // Sync completed
                SyncStatus::NotSyncing
            } else {
                SyncStatus::Syncing {
                    highest_block: highest_block.0.into(),
                    current_block: current_block.0.into(),
                }
            })
        })
        .await
        .unwrap_or_else(helpers::joinerror_to_result)
    }
}
