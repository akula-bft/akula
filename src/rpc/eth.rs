use crate::{
    consensus::engine_factory,
    execution::{analysis_cache::AnalysisCache, evm, processor::ExecutionProcessor},
    kv::{
        tables,
        traits::{Transaction, KV},
    },
    models::*,
    res::chainspec::MAINNET,
    stagedsync::stages::FINISH,
    Buffer, InMemoryState, IntraBlockState,
};
use async_trait::async_trait;
use ethereum_jsonrpc::{
    common::{Balance, CallData, ReturnData},
    EthApiServer,
};
use jsonrpsee::core::RpcResult;
use std::sync::Arc;

pub struct EthApiServerImpl<DB>
where
    DB: KV,
{
    pub db: Arc<DB>,
}

#[async_trait]
impl<DB> EthApiServer for EthApiServerImpl<DB>
where
    DB: KV,
{
    async fn block_number(&self) -> RpcResult<U64> {
        Ok(U64::from(
            self.db
                .begin()
                .await?
                .get(tables::SyncStage, FINISH)
                .await?
                .unwrap_or(BlockNumber(0))
                .0,
        ))
    }

    async fn call(
        &self,
        call_data: CallData,
        block_number: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::ReturnData> {
        let tx = &self.db.begin().await?;

        let block_number = BlockNumber(block_number.as_u64());
        let block_hash = crate::accessors::chain::canonical_hash::read(tx, block_number)
            .await?
            .unwrap();

        let header = crate::accessors::chain::header::read(tx, block_hash, block_number)
            .await?
            .unwrap();

        let mut state = Buffer::new(tx, BlockNumber(0), Some(block_number));
        let mut analysis_cache = AnalysisCache::default();
        let block_spec = MAINNET.collect_block_spec(block_number);

        let value = call_data.value.unwrap_or(U256::ZERO);

        let input = call_data.data.unwrap_or_default();

        let sender = call_data.from.unwrap_or_else(Address::zero);

        let msg_with_sender = MessageWithSender {
            message: Message::Legacy {
                chain_id: Some(ChainId(1)),
                nonce: 0,
                gas_price: Default::default(),
                gas_limit: 0,
                action: TransactionAction::Call(call_data.to),
                value,
                input,
            },
            sender,
        };

        let gas = call_data.gas.map(|v| v.as_u64()).unwrap_or(100_000_000);

        Ok(ReturnData(
            evm::execute(
                &mut IntraBlockState::new(&mut state),
                None,
                &mut analysis_cache,
                &PartialHeader::from(header.clone()),
                &block_spec,
                &msg_with_sender,
                gas,
            )
            .await?
            .output_data,
        ))
    }

    async fn estimate_gas(&self, call_data: CallData, block_number: U64) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        let block_number = BlockNumber(block_number.as_u64());
        let hash = txn
            .get(tables::CanonicalHeader, block_number)
            .await?
            .unwrap();
        let header = txn
            .get(tables::Header, (block_number, hash))
            .await?
            .unwrap();
        let tx = MessageWithSender {
            message: Message::Legacy {
                chain_id: None,
                nonce: 0,
                gas_price: call_data
                    .gas_price
                    .map(|v| v.as_u64().as_u256())
                    .unwrap_or(U256::ZERO),
                gas_limit: call_data
                    .gas
                    .map(|gas| gas.as_u64())
                    .unwrap_or_else(|| header.gas_limit),
                action: TransactionAction::Call(call_data.to),
                value: call_data.value.unwrap_or(U256::ZERO),
                input: call_data.data.unwrap_or_default(),
            },
            sender: call_data.from.unwrap_or_else(Address::zero),
        };
        let mut db = InMemoryState::default();
        let mut state = IntraBlockState::new(&mut db);
        let mut cache = AnalysisCache::default();
        let spec = txn
            .get(
                tables::Config,
                txn.get(tables::CanonicalHeader, BlockNumber(0))
                    .await?
                    .unwrap(),
            )
            .await?
            .unwrap()
            .collect_block_spec(block_number);
        Ok(U64::from(
            50_000_000
                - evm::execute(
                    &mut state,
                    None,
                    &mut cache,
                    &PartialHeader::from(header),
                    &spec,
                    &tx,
                    50_000_000,
                )
                .await?
                .gas_left,
        ))
    }

    async fn get_balance(&self, address: Address, block_number: U64) -> RpcResult<Balance> {
        Ok(crate::accessors::state::account::read(
            &self.db.begin().await?,
            address,
            Some(BlockNumber(block_number.as_u64())),
        )
        .await?
        .map(|acc| acc.balance)
        .unwrap_or(U256::ZERO))
    }

    async fn get_block_by_hash(
        &self,
        hash: H256,
        full_tx_obj: bool,
    ) -> RpcResult<ethereum_jsonrpc::common::Block> {
        let txn = self.db.begin().await?;
        let block_number = crate::accessors::chain::header_number::read(&txn, hash)
            .await?
            .unwrap();
        let header = crate::accessors::chain::header::read(&txn, hash, block_number)
            .await?
            .unwrap();
        let body =
            crate::accessors::chain::block_body::read_without_senders(&txn, hash, block_number)
                .await?
                .unwrap();
        let transactions: Vec<ethereum_jsonrpc::common::Transaction> = match full_tx_obj {
            true => {
                let senders =
                    crate::accessors::chain::tx_sender::read(&txn, hash, block_number).await?;
                body.transactions
                    .into_iter()
                    .zip(senders)
                    .enumerate()
                    .map(|(index, (tx, sender))| {
                        ethereum_jsonrpc::common::Transaction::Full(Box::new(
                            ethereum_jsonrpc::common::Tx {
                                block_number: Some(U64::from(block_number.0)),
                                block_hash: Some(hash),
                                from: sender,
                                gas: U64::from(tx.message.gas_limit()),
                                gas_price: match tx.message {
                                    Message::Legacy { gas_price, .. } => gas_price,
                                    Message::EIP2930 { gas_price, .. } => gas_price,
                                    Message::EIP1559 {
                                        max_fee_per_gas, ..
                                    } => max_fee_per_gas,
                                },
                                hash: tx.message.hash(),
                                input: tx.message.input().clone(),
                                nonce: U64::from(tx.message.nonce()),
                                to: match tx.message.action() {
                                    TransactionAction::Call(to) => Some(to),
                                    TransactionAction::Create => None,
                                },
                                transaction_index: Some(U64::from(index as u64)),
                                value: tx.message.value(),
                                v: U64::from(tx.v()),
                                r: tx.r(),
                                s: tx.s(),
                            },
                        ))
                    })
                    .collect()
            }
            false => body
                .transactions
                .into_iter()
                .map(|tx| ethereum_jsonrpc::common::Transaction::Partial(tx.message.hash()))
                .collect(),
        };

        let td = crate::accessors::chain::td::read(&txn, hash, block_number)
            .await?
            .unwrap();

        Ok(ethereum_jsonrpc::common::Block {
            number: Some(U64::from(block_number.0)),
            hash: Some(hash),
            parent_hash: header.parent_hash,
            nonce: Some(header.nonce),
            sha3_uncles: header.ommers_hash,
            logs_bloom: Some(header.logs_bloom),
            transactions_root: header.transactions_root,
            state_root: header.state_root,
            receipts_root: header.receipts_root,
            miner: header.beneficiary,
            difficulty: header.difficulty,
            total_difficulty: td,
            extra_data: header.extra_data,
            size: U64::zero(),
            gas_limit: U64::from(header.gas_limit),
            gas_used: U64::from(header.gas_used),
            timestamp: U64::from(header.timestamp),
            transactions,
            uncles: body.ommers.into_iter().map(|uncle| uncle.hash()).collect(),
        })
    }
    async fn get_block_by_number(
        &self,
        block_number: U64,
        full_tx_obj: bool,
    ) -> RpcResult<ethereum_jsonrpc::common::Block> {
        let txn = self.db.begin().await?;
        let block_number = BlockNumber(block_number.as_u64());
        let hash = crate::accessors::chain::canonical_hash::read(&txn, block_number)
            .await?
            .unwrap();
        let header = crate::accessors::chain::header::read(&txn, hash, block_number)
            .await?
            .unwrap();
        let body =
            crate::accessors::chain::block_body::read_without_senders(&txn, hash, block_number)
                .await?
                .unwrap();
        let transactions: Vec<ethereum_jsonrpc::common::Transaction> = match full_tx_obj {
            true => {
                let senders =
                    crate::accessors::chain::tx_sender::read(&txn, hash, block_number).await?;
                body.transactions
                    .into_iter()
                    .zip(senders)
                    .enumerate()
                    .map(|(index, (tx, sender))| {
                        ethereum_jsonrpc::common::Transaction::Full(Box::new(
                            ethereum_jsonrpc::common::Tx {
                                block_number: Some(U64::from(block_number.0)),
                                block_hash: Some(hash),
                                from: sender,
                                gas: U64::from(tx.message.gas_limit()),
                                gas_price: match tx.message {
                                    Message::Legacy { gas_price, .. } => gas_price,
                                    Message::EIP2930 { gas_price, .. } => gas_price,
                                    Message::EIP1559 {
                                        max_fee_per_gas, ..
                                    } => max_fee_per_gas,
                                },
                                hash: tx.message.hash(),
                                input: tx.message.input().clone(),
                                nonce: U64::from(tx.message.nonce()),
                                to: match tx.message.action() {
                                    TransactionAction::Call(to) => Some(to),
                                    TransactionAction::Create => None,
                                },
                                transaction_index: Some(U64::from(index as u64)),
                                value: tx.message.value(),
                                v: U64::from(tx.v()),
                                r: tx.r(),
                                s: tx.s(),
                            },
                        ))
                    })
                    .collect()
            }
            false => body
                .transactions
                .into_iter()
                .map(|tx| ethereum_jsonrpc::common::Transaction::Partial(tx.message.hash()))
                .collect(),
        };

        let td = crate::accessors::chain::td::read(&txn, hash, block_number)
            .await?
            .unwrap();

        Ok(ethereum_jsonrpc::common::Block {
            number: Some(U64::from(block_number.0)),
            hash: Some(hash),
            parent_hash: header.parent_hash,
            nonce: Some(header.nonce),
            sha3_uncles: header.ommers_hash,
            logs_bloom: Some(header.logs_bloom),
            transactions_root: header.transactions_root,
            state_root: header.state_root,
            receipts_root: header.receipts_root,
            miner: header.beneficiary,
            difficulty: header.difficulty,
            total_difficulty: td,
            extra_data: header.extra_data,
            size: U64::zero(),
            gas_limit: U64::from(header.gas_limit),
            gas_used: U64::from(header.gas_used),
            timestamp: U64::from(header.timestamp),
            transactions,
            uncles: body.ommers.into_iter().map(|uncle| uncle.hash()).collect(),
        })
    }

    async fn get_block_tx_count_by_hash(&self, hash: H256) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        Ok(U64::from(
            crate::accessors::chain::block_body::read_without_senders(
                &txn,
                hash,
                crate::accessors::chain::header_number::read(&txn, hash)
                    .await?
                    .unwrap(),
            )
            .await?
            .unwrap()
            .transactions
            .len(),
        ))
    }

    async fn get_block_tx_count_by_number(&self, block_number: U64) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        Ok(U64::from(
            crate::accessors::chain::block_body::read_without_senders(
                &txn,
                crate::accessors::chain::canonical_hash::read(
                    &txn,
                    BlockNumber(block_number.as_u64()),
                )
                .await?
                .unwrap(),
                BlockNumber(block_number.as_u64()),
            )
            .await?
            .unwrap()
            .transactions
            .len(),
        ))
    }

    async fn get_code(
        &self,
        address: Address,
        block_number: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::Code> {
        let txn = self.db.begin().await?;
        let account = crate::accessors::state::account::read(
            &txn,
            address,
            Some(BlockNumber(block_number.as_u64())),
        )
        .await?
        .unwrap();

        Ok(ethereum_jsonrpc::common::Code(
            txn.get(tables::Code, account.code_hash).await?.unwrap(),
        ))
    }

    async fn get_storage_at(
        &self,
        address: Address,
        key: U256,
        block_number: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::StorageData> {
        Ok(crate::accessors::state::storage::read(
            &self.db.begin().await?,
            address,
            key,
            Some(BlockNumber(block_number.as_u64())),
        )
        .await?)
    }

    async fn get_tx_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::Tx> {
        let txn = self.db.begin().await?;
        let block_number = crate::accessors::chain::header_number::read(&txn, block_hash)
            .await?
            .unwrap();
        Ok(crate::accessors::chain::block_body::read_without_senders(
            &txn,
            block_hash,
            block_number,
        )
        .await?
        .unwrap()
        .transactions
        .into_iter()
        .nth(index.as_usize())
        .map(|tx| ethereum_jsonrpc::common::Tx {
            block_hash: Some(block_hash),
            block_number: Some(U64::from(block_number.0)),
            from: Address::zero(),
            gas: U64::from(tx.message.gas_limit()),
            gas_price: match tx.message {
                Message::Legacy { gas_price, .. } => gas_price,
                Message::EIP2930 { gas_price, .. } => gas_price,
                Message::EIP1559 {
                    max_fee_per_gas, ..
                } => max_fee_per_gas,
            },
            hash: tx.message.hash(),
            input: tx.message.input().clone(),
            nonce: U64::from(tx.message.nonce()),
            to: match tx.message.action() {
                TransactionAction::Call(to) => Some(to),
                _ => None,
            },
            transaction_index: Some(index),
            value: tx.message.value(),
            v: U64::from(tx.v()),
            r: tx.r(),
            s: tx.s(),
        })
        .unwrap())
    }

    async fn get_tx_by_block_number_and_index(
        &self,
        block_number: U64,
        index: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::Tx> {
        let txn = self.db.begin().await?;
        let block_hash =
            crate::accessors::chain::canonical_hash::read(&txn, BlockNumber(block_number.as_u64()))
                .await?
                .unwrap();
        Ok(crate::accessors::chain::block_body::read_without_senders(
            &txn,
            block_hash,
            BlockNumber(block_number.as_u64()),
        )
        .await?
        .unwrap()
        .transactions
        .into_iter()
        .nth(index.as_usize())
        .map(|tx| ethereum_jsonrpc::common::Tx {
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            from: Address::zero(),
            gas: U64::from(tx.message.gas_limit()),
            gas_price: match tx.message {
                Message::Legacy { gas_price, .. } => gas_price,
                Message::EIP2930 { gas_price, .. } => gas_price,
                Message::EIP1559 {
                    max_fee_per_gas, ..
                } => max_fee_per_gas,
            },
            hash: tx.message.hash(),
            input: tx.message.input().clone(),
            nonce: U64::from(tx.message.nonce()),
            to: match tx.message.action() {
                TransactionAction::Call(to) => Some(to),
                _ => None,
            },
            transaction_index: Some(index),
            value: tx.message.value(),
            v: U64::from(tx.v()),
            r: tx.r(),
            s: tx.s(),
        })
        .unwrap())
    }

    async fn get_transaction_count(&self, address: Address, block_number: U64) -> RpcResult<U64> {
        Ok(U64::from(
            crate::accessors::state::account::read(
                &self.db.begin().await?,
                address,
                Some(BlockNumber(block_number.as_u64())),
            )
            .await?
            .unwrap()
            .nonce,
        ))
    }

    async fn get_transaction_receipt(
        &self,
        hash: H256,
    ) -> RpcResult<ethereum_jsonrpc::common::TxReceipt> {
        let txn = self.db.begin().await?;
        let block_number = crate::accessors::chain::tl::read(&txn, hash)
            .await?
            .unwrap();
        let block_hash = crate::accessors::chain::canonical_hash::read(&txn, block_number)
            .await?
            .unwrap();
        let header = PartialHeader::from(
            crate::accessors::chain::header::read(&txn, block_hash, block_number)
                .await?
                .unwrap(),
        );
        let block_body =
            crate::accessors::chain::block_body::read_with_senders(&txn, block_hash, block_number)
                .await?
                .unwrap();
        let chain_config = txn
            .get(
                tables::Config,
                txn.get(tables::CanonicalHeader, BlockNumber(0))
                    .await?
                    .unwrap(),
            )
            .await?
            .unwrap();
        let block_spec = chain_config.collect_block_spec(block_number);

        // Prepare the execution context.
        let mut buffer = Buffer::new(&txn, BlockNumber(0), Some(BlockNumber(block_number.0 - 1)));
        let mut engine = engine_factory(chain_config.clone())?;
        let mut analysis_cache = AnalysisCache::default();
        let mut processor = ExecutionProcessor::new(
            &mut buffer,
            None,
            &mut analysis_cache,
            &mut *engine,
            &header,
            &block_body,
            &block_spec,
        );

        let receipts = processor.execute_block_no_post_validation().await?;
        let transaction_index = block_body
            .transactions
            .iter()
            .position(|tx| tx.message.hash() == hash)
            .unwrap();
        let transaction = block_body.transactions.get(transaction_index).unwrap();
        let receipt = receipts.get(transaction_index).unwrap();
        let gas_used = match transaction_index {
            0 => U64::from(receipt.cumulative_gas_used),
            _ => U64::from(
                receipt.cumulative_gas_used
                    - receipts
                        .get(transaction_index - 1)
                        .unwrap()
                        .cumulative_gas_used,
            ),
        };
        let logs = receipt
            .logs
            .iter()
            .enumerate()
            .map(|(i, log)| ethereum_jsonrpc::common::TxLog {
                log_index: Some(U64::from(i)),
                transaction_index: Some(U64::from(transaction_index)),
                transaction_hash: Some(transaction.message.hash()),
                block_hash: Some(block_hash),
                block_number: Some(U64::from(block_number.0)),
                address: log.clone().address,
                data: log.clone().data,
                topics: log.clone().topics,
            })
            .collect::<Vec<_>>();

        Ok(ethereum_jsonrpc::common::TxReceipt {
            transaction_hash: hash,
            transaction_index: U64::from(transaction_index),
            block_hash,
            block_number: U64::from(block_number.0),
            from: transaction.sender,
            to: match transaction.message.action() {
                TransactionAction::Call(to) => Some(to),
                _ => None,
            },
            cumulative_gas_used: U64::from(receipt.cumulative_gas_used),
            gas_used,
            contract_address: match transaction.message.action() {
                TransactionAction::Create => Some(crate::execution::address::create_address(
                    transaction.sender,
                    transaction.message.nonce(),
                )),
                _ => None,
            },
            logs,
            logs_bloom: receipt.bloom,
            status: if receipt.success {
                U64::from(1)
            } else {
                U64::zero()
            },
        })
    }

    async fn get_uncle_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::Block> {
        let txn = self.db.begin().await?;
        let block_number = crate::accessors::chain::header_number::read(&txn, block_hash)
            .await?
            .unwrap();
        let uncle_header = crate::accessors::chain::storage_body::read(
            &txn,
            crate::accessors::chain::canonical_hash::read(&txn, BlockNumber(block_number.0))
                .await?
                .unwrap(),
            block_number,
        )
        .await?
        .unwrap()
        .uncles
        .into_iter()
        .nth(index.as_usize())
        .unwrap();

        let body = crate::accessors::chain::block_body::read_without_senders(
            &txn,
            uncle_header.hash(),
            block_number,
        )
        .await?
        .unwrap();
        let senders =
            crate::accessors::chain::tx_sender::read(&txn, uncle_header.hash(), block_number)
                .await?;

        let transactions = body
            .transactions
            .into_iter()
            .zip(senders)
            .enumerate()
            .map(|(index, (tx, sender))| {
                ethereum_jsonrpc::common::Transaction::Full(Box::new(
                    ethereum_jsonrpc::common::Tx {
                        block_number: Some(U64::from(block_number.0)),
                        block_hash: Some(uncle_header.hash()),
                        from: sender,
                        gas: U64::from(tx.message.gas_limit()),
                        gas_price: match tx.message {
                            Message::Legacy { gas_price, .. } => gas_price,
                            Message::EIP2930 { gas_price, .. } => gas_price,
                            Message::EIP1559 {
                                max_fee_per_gas, ..
                            } => max_fee_per_gas,
                        },
                        hash: tx.message.hash(),
                        input: tx.message.input().clone(),
                        nonce: U64::from(tx.message.nonce()),
                        to: match tx.message.action() {
                            TransactionAction::Call(to) => Some(to),
                            TransactionAction::Create => None,
                        },
                        transaction_index: Some(U64::from(index as u64)),
                        value: tx.message.value(),
                        v: U64::from(tx.v()),
                        r: tx.r(),
                        s: tx.s(),
                    },
                ))
            })
            .collect();

        let td = crate::accessors::chain::td::read(&txn, uncle_header.hash(), block_number)
            .await?
            .unwrap();

        Ok(ethereum_jsonrpc::common::Block {
            number: Some(U64::from(block_number.0)),
            hash: Some(uncle_header.hash()),
            parent_hash: uncle_header.parent_hash,
            nonce: Some(uncle_header.nonce),
            sha3_uncles: uncle_header.ommers_hash,
            logs_bloom: Some(uncle_header.logs_bloom),
            transactions_root: uncle_header.transactions_root,
            state_root: uncle_header.state_root,
            receipts_root: uncle_header.receipts_root,
            miner: uncle_header.beneficiary,
            difficulty: uncle_header.difficulty,
            total_difficulty: td,
            extra_data: uncle_header.extra_data,
            size: U64::zero(),
            gas_limit: U64::from(uncle_header.gas_limit),
            gas_used: U64::from(uncle_header.gas_used),
            timestamp: U64::from(uncle_header.timestamp),
            transactions,
            uncles: body.ommers.into_iter().map(|uncle| uncle.hash()).collect(),
        })
    }

    async fn get_uncle_by_block_number_and_index(
        &self,
        block_number: U64,
        index: U64,
    ) -> RpcResult<ethereum_jsonrpc::common::Block> {
        let txn = self.db.begin().await?;
        let uncle_header = crate::accessors::chain::storage_body::read(
            &txn,
            crate::accessors::chain::canonical_hash::read(&txn, BlockNumber(block_number.as_u64()))
                .await?
                .unwrap(),
            BlockNumber(block_number.as_u64()),
        )
        .await?
        .unwrap()
        .uncles
        .into_iter()
        .nth(index.as_usize())
        .unwrap();

        let body = crate::accessors::chain::block_body::read_without_senders(
            &txn,
            uncle_header.hash(),
            BlockNumber(block_number.as_u64()),
        )
        .await?
        .unwrap();
        let senders = crate::accessors::chain::tx_sender::read(
            &txn,
            uncle_header.hash(),
            BlockNumber(block_number.as_u64()),
        )
        .await?;

        let transactions = body
            .transactions
            .into_iter()
            .zip(senders)
            .enumerate()
            .map(|(index, (tx, sender))| {
                ethereum_jsonrpc::common::Transaction::Full(Box::new(
                    ethereum_jsonrpc::common::Tx {
                        block_number: Some(block_number),
                        block_hash: Some(uncle_header.hash()),
                        from: sender,
                        gas: U64::from(tx.message.gas_limit()),
                        gas_price: match tx.message {
                            Message::Legacy { gas_price, .. } => gas_price,
                            Message::EIP2930 { gas_price, .. } => gas_price,
                            Message::EIP1559 {
                                max_fee_per_gas, ..
                            } => max_fee_per_gas,
                        },
                        hash: tx.message.hash(),
                        input: tx.message.input().clone(),
                        nonce: U64::from(tx.message.nonce()),
                        to: match tx.message.action() {
                            TransactionAction::Call(to) => Some(to),
                            TransactionAction::Create => None,
                        },
                        transaction_index: Some(U64::from(index as u64)),
                        value: tx.message.value(),
                        v: U64::from(tx.v()),
                        r: tx.r(),
                        s: tx.s(),
                    },
                ))
            })
            .collect();
        let td = crate::accessors::chain::td::read(
            &txn,
            uncle_header.hash(),
            BlockNumber(block_number.as_u64()),
        )
        .await?
        .unwrap();

        Ok(ethereum_jsonrpc::common::Block {
            number: Some(block_number),
            hash: Some(uncle_header.hash()),
            parent_hash: uncle_header.parent_hash,
            nonce: Some(uncle_header.nonce),
            sha3_uncles: uncle_header.ommers_hash,
            logs_bloom: Some(uncle_header.logs_bloom),
            transactions_root: uncle_header.transactions_root,
            state_root: uncle_header.state_root,
            receipts_root: uncle_header.receipts_root,
            miner: uncle_header.beneficiary,
            difficulty: uncle_header.difficulty,
            total_difficulty: td,
            extra_data: uncle_header.extra_data,
            size: U64::zero(),
            gas_limit: U64::from(uncle_header.gas_limit),
            gas_used: U64::from(uncle_header.gas_used),
            timestamp: U64::from(uncle_header.timestamp),
            transactions,
            uncles: body.ommers.into_iter().map(|uncle| uncle.hash()).collect(),
        })
    }

    async fn get_uncle_count_by_block_hash(&self, block_hash: H256) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        Ok(U64::from(
            crate::accessors::chain::storage_body::read(
                &txn,
                block_hash,
                crate::accessors::chain::header_number::read(&txn, block_hash)
                    .await?
                    .unwrap(),
            )
            .await?
            .unwrap()
            .uncles
            .len(),
        ))
    }

    async fn get_uncle_count_by_block_number(&self, block_number: U64) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        Ok(U64::from(
            crate::accessors::chain::storage_body::read(
                &txn,
                crate::accessors::chain::canonical_hash::read(
                    &txn,
                    BlockNumber(block_number.as_u64()),
                )
                .await?
                .unwrap(),
                BlockNumber(block_number.as_u64()),
            )
            .await?
            .unwrap()
            .uncles
            .len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        genesis::initialize_genesis,
        kv::{new_mem_database, tables, traits::*},
        models::{Account, BlockNumber, ETHER},
        stagedsync::stages::FINISH,
    };
    use ethereum_jsonrpc::EthApiClient;
    use ethereum_types::{H160, U64};
    use hex_literal::hex;
    use jsonrpsee::{
        http_client::HttpClientBuilder,
        http_server::{HttpServerBuilder, HttpServerHandle},
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    // We'll need it later in our tests.
    const DEADBEEF: Address = H160(hex!("00000000000000000000000000000000deadbeef"));

    async fn init_mock_chain() -> anyhow::Result<Arc<impl MutableKV>> {
        use super::*;
        let mock_db = Arc::new(new_mem_database().unwrap());
        let txn = mock_db.begin_mutable().await.unwrap();
        assert!(initialize_genesis(
            &txn,
            &TempDir::new().unwrap(),
            crate::res::chainspec::MAINNET.clone()
        )
        .await
        .unwrap());
        assert!(txn
            .set(
                tables::Account,
                DEADBEEF,
                Account {
                    balance: ETHER.as_u256(),
                    ..Default::default()
                },
            )
            .await
            .is_ok());
        assert!(txn.commit().await.is_ok());

        Ok(mock_db)
    }

    async fn start_server(db: Arc<impl MutableKV>) -> anyhow::Result<(u16, HttpServerHandle)> {
        let server = HttpServerBuilder::default().build("localhost:0")?;
        let port = server.local_addr()?.port();
        let _server_handle = server.start(EthApiServerImpl { db }.into_rpc())?;
        Ok((port, _server_handle))
    }

    #[tokio::test]
    async fn test_block_number() {
        let db = init_mock_chain().await.unwrap();
        let (port, _handle) = start_server(db.clone()).await.unwrap();

        let client = HttpClientBuilder::default()
            .build(format!("http://localhost:{}", port))
            .unwrap();
        let block_number = client.block_number().await.unwrap();
        assert_eq!(block_number, U64::from(0));

        let txn = db.begin_mutable().await.unwrap();
        txn.set(tables::SyncStage, FINISH, BlockNumber(0xff))
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let block_number = client.block_number().await.unwrap();
        assert_eq!(block_number, U64::from(0xff));
    }

    #[tokio::test]
    async fn test_get_balance() {
        let db = init_mock_chain().await.unwrap();
        let (port, _handle) = start_server(db.clone()).await.unwrap();

        let client = HttpClientBuilder::default()
            .build(format!("http://localhost:{}", port))
            .unwrap();
        let block_number = client.block_number().await.unwrap();
        let balance = client.get_balance(DEADBEEF, block_number).await.unwrap();
        assert_eq!(balance, ETHER);

        let txn = db.begin_mutable().await.unwrap();
        txn.set(
            tables::Account,
            DEADBEEF,
            Account {
                balance: ETHER.as_u256() * 100,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let balance = client.get_balance(DEADBEEF, block_number).await.unwrap();
        assert_eq!(balance, ETHER.as_u256() * 100);
    }
}
