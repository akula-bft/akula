use crate::{
    accessors::{chain, state},
    consensus::engine_factory,
    execution::{analysis_cache::AnalysisCache, evm, processor::ExecutionProcessor, tracer::NoopTracer},
    kv::tables,
    execution::evmglue,
    kv::mdbx::{
        MdbxTransaction,
        MdbxEnvironment,
        EnvironmentKind,
    },
    models::*,
    res::chainspec::MAINNET,
    stagedsync::stages::FINISH,
    Buffer, InMemoryState, IntraBlockState,
};

use async_trait::async_trait;
use ethereum_jsonrpc::{types, EthApiServer};
use jsonrpsee::core::RpcResult;
use std::sync::Arc;

use super::helpers;

pub struct EthApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxEnvironment<SE>>,
}

#[async_trait]
impl<DB> EthApiServer for EthApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn block_number(&self) ->  RpcResult<U64> {
        Ok(U64::from(
            self.db
                .begin()?
                .get(tables::SyncStage, FINISH)?
                .unwrap_or(BlockNumber(0))
                .0,
        ))
    }

    async fn call(
        &self,
        call_data: types::MessageCall,
        block_number: types::BlockNumber,
    ) -> RpcResult<types::Bytes> {
        let txn = self.db.begin()?;

        let block_number = helpers::get_block_number(&txn, block_number)?;
        let block_hash = chain::canonical_hash::read(&txn, block_number)?;

        let header = chain::header::read(&txn, block_hash, block_number)?;

        let mut state = Buffer::new(&txn, BlockNumber(0), Some(block_number));
        let mut analysis_cache = AnalysisCache::default();
        let block_spec = MAINNET.collect_block_spec(block_number);

        let input = call_data.data.unwrap_or_default().into();
        let sender = call_data.from.unwrap_or_else(Address::zero);
        let value = call_data.value.unwrap_or_default();

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
        let mut tracer = NoopTracer;

        Ok(evmglue::execute(
                &mut IntraBlockState::new(&mut state),
                &mut tracer,
                &mut analysis_cache,
                &PartialHeader::from(header.clone()),
                &block_spec,
                &msg_with_sender,
                gas,
            )?
            .output_data
            .into()
        )
    }

    async fn estimate_gas(
        &self,
        call_data: types::MessageCall,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let txn = self.db.begin()?;
        let block_number = helpers::get_block_number(&txn, block_number)?;
        let hash = txn
            .get(tables::CanonicalHeader, block_number)?
            .unwrap();
        let header = txn
            .get(tables::Header, (block_number, hash))?
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
                input: call_data.data.unwrap_or_default().into(),
            },
            sender: call_data.from.unwrap_or_else(Address::zero),
        };
        let mut db = InMemoryState::default();
        let mut state = IntraBlockState::new(&mut db);
        let mut cache = AnalysisCache::default();
        let block_spec = MAINNET.collect_block_spec(block_number);
        let mut tracer = NoopTracer;
        Ok(U64::from(
            50_000_000
                - evmglue::execute(
                    &mut state,
                    &mut tracer,
                    &mut cache,
                    &PartialHeader::from(header),
                    &block_spec,
                    &tx,
                    50_000_000,
                )?.gas_left,
        ))
    }

    async fn get_balance(
        &self,
        address: Address,
        block_number: types::BlockNumber,
    ) -> RpcResult<U256> {
        let txn = self.db.begin()?;

        Ok(state::account::read(
            &txn,
            address,
            Some(helpers::get_block_number(&txn, block_number)?),
        )?
        .map(|acc| acc.balance)
        .unwrap_or(U256::ZERO))
    }

    async fn get_block_by_hash(&self, hash: H256, include_txs: bool) -> RpcResult<types::Block> {
        let txn = self.db.begin()?;
        Ok(helpers::construct_block(&txn, hash.into(), Some(include_txs), None)?)
    }
    async fn get_block_by_number(
        &self,
        block_number: types::BlockNumber,
        include_txs: bool,
    ) -> RpcResult<types::Block> {
        Ok(helpers::construct_block(
            &self.db.begin()?,
            block_number.into(),
            Some(include_txs),
            None,
        )?)
    }
    async fn get_transaction(&self, hash: H256) -> RpcResult<Option<types::Tx>> {
        let txn = self.db.begin()?;
        let block_number = match chain::tl::read(&txn, hash)? {
            Some(tl) => tl,
            None => return Ok(None),
        };
        let block_hash = chain::canonical_hash::read(&txn, block_number)?;
        let (index, transaction) =
            chain::block_body::read_without_senders(&txn, block_hash, block_number)?
                .unwrap()
                .transactions
                .into_iter()
                .enumerate()
                .find(|(_, tx)| tx.hash() == hash)
                .unwrap();
        let sender = chain::tx_sender::read(&txn, block_hash, block_number)?
            .into_iter()
            .nth(index)
            .unwrap();
        Ok(Some(types::Tx::Transaction(Box::new(types::Transaction{
            hash,
            nonce: transaction.nonce().into(),
            block_hash: Some(block_hash),
            block_number: Some(block_number.0.into()),
            from: sender,
            gas: transaction.gas_limit().into(),
            gas_price: match transaction.message {
                Message::Legacy { gas_price, .. } => gas_price,
                Message::EIP2930 { gas_price, .. } => gas_price,
                Message::EIP1559 {
                    max_fee_per_gas, ..
                } => max_fee_per_gas,
            },
            input: transaction.input().clone().into(),
            to: match transaction.action() {
                TransactionAction::Call(to) => Some(to),
                TransactionAction::Create => None,
            },
            transaction_index: Some(U64::from(index)),
            value: transaction.value(),
            v: transaction.v().into(),
            r: transaction.r(),
            s: transaction.s(),
        }))))
    }

    async fn get_block_transaction_count_by_hash(&self, hash: H256) -> RpcResult<U64> {
        let txn = self.db.begin()?;
        Ok(U64::from(
            chain::block_body::read_without_senders(
                &txn,
                hash,
                chain::header_number::read(&txn, hash)?,
            )?
            .unwrap()
            .transactions
            .len(),
        ))
    }

    async fn get_block_transaction_count_by_number(
        &self,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let txn = self.db.begin()?;
        let block_number = helpers::get_block_number(&txn, block_number)?;
        Ok(chain::block_body::read_without_senders(
            &txn,
            chain::canonical_hash::read(&txn, block_number)?,
            block_number
        )?
        .unwrap()
        .transactions
        .len()
        .into())
    }

    async fn get_code(
        &self,
        address: Address,
        block_number: types::BlockNumber,
    ) -> RpcResult<types::Bytes> {
        let txn = self.db.begin()?;
        let block_number = helpers::get_block_number(&txn, block_number)?;
        let account = state::account::read(&txn, address, Some(block_number))?
            .unwrap();

        Ok(txn
            .get(tables::Code, account.code_hash)?
            .unwrap()
            .into())
    }

    async fn get_storage_at(
        &self,
        address: Address,
        key: U256,
        block_number: types::BlockNumber,
    ) -> RpcResult<H256> {
        let txn = self.db.begin()?;
        Ok(state::storage::read(
            &txn,
            address,
            key,
            Some(helpers::get_block_number(&txn, block_number)?)
        )?)
    }

    async fn get_transaction_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<Option<types::Tx>> {
        Ok(helpers::construct_block(
            &self.db.begin()?,
            block_hash.into(),
            None,
            Some(index),
        )?
        .transactions
        .into_iter()
        .nth(index.as_usize()))
    }

    async fn get_transaction_by_block_number_and_index(
        &self,
        block_number: types::BlockNumber,
        index: U64,
    ) -> RpcResult<Option<types::Tx>> {
        Ok(
            helpers::construct_block(&self.db.begin()?, block_number.into(), None, None)?
                .transactions
                .into_iter()
                .nth(index.as_usize()),
        )
    }

    async fn get_transaction_count(
        &self,
        address: Address,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let txn = self.db.begin()?;
        Ok(state::account::read(
            &txn,
            address,
            Some(helpers::get_block_number(&txn, block_number)?),
        )?
        .unwrap()
        .nonce
        .into())
    }

    async fn get_transaction_receipt(
        &self,
        hash: H256,
    ) -> RpcResult<Option<types::TransactionReceipt>> {
        let txn = self.db.begin()?;
        let block_number = chain::tl::read(&txn, hash)?.unwrap();
        let block_hash = chain::canonical_hash::read(&txn, block_number)?;
        let header = PartialHeader::from(
            chain::header::read(&txn, block_hash, block_number)?
        );
        let block_body = chain::block_body::read_with_senders(&txn, block_hash, block_number)?
            .unwrap();
        let block_spec = MAINNET.collect_block_spec(block_number);

        // Prepare the execution context.
        let mut buffer = Buffer::new(&txn, BlockNumber(0), Some(BlockNumber(block_number.0 - 1)));
        let mut engine = engine_factory(MAINNET.clone()).unwrap();
        let mut analysis_cache = AnalysisCache::default();
        let mut tracer = NoopTracer;
        let mut processor = ExecutionProcessor::new(
            &mut buffer,
            &mut tracer,
            &mut analysis_cache,
            &mut *engine,
            &header,
            &block_body,
            &block_spec,
        );

        let receipts = processor.execute_block_no_post_validation()?;
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
            .map(|(i, log)| types::TransactionLog {
                log_index: Some(U64::from(i)),
                transaction_index: Some(U64::from(transaction_index)),
                transaction_hash: Some(transaction.message.hash()),
                block_hash: Some(block_hash),
                block_number: Some(U64::from(block_number.0)),
                address: log.clone().address,
                data: log.clone().data.into(),
                topics: log.clone().topics,
            })
            .collect::<Vec<_>>();

        Ok(Some(types::TransactionReceipt {
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
                U64::from(1 as u16)
            } else {
                U64::zero()
            },
        }))
    }

    async fn get_uncle_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<Option<types::Block>> {
        Ok(Some(
            helpers::construct_block(
                &self.db.begin()?,
                block_hash.into(),
                None,
                Some(index),
            )?,
        ))
    }

    async fn get_uncle_by_block_number_and_index(
        &self,
        block_number: types::BlockNumber,
        index: U64,
    ) -> RpcResult<Option<types::Block>> {
        Ok(Some(
            helpers::construct_block(
                &self.db.begin()?,
                block_number.into(),
                None,
                Some(index),
            )?,
        ))
    }

    async fn get_uncle_count_by_block_hash(&self, block_hash: H256) -> RpcResult<U64> {
        let txn = self.db.begin()?;
        Ok(U64::from(
            chain::storage_body::read(
                &txn,
                block_hash,
                chain::header_number::read(&txn, block_hash)?,
            )?
            .unwrap()
            .uncles
            .len(),
        ))
    }

    async fn get_uncle_count_by_block_number(
        &self,
        block_number: types::BlockNumber,
    ) -> RpcResult<U64> {
        let txn = self.db.begin()?;
        let block_number = helpers::get_block_number(&txn, block_number)?;
        Ok(U64::from(
            chain::storage_body::read(
                &txn,
                chain::canonical_hash::read(&txn, block_number)
                    .unwrap(),
                block_number,
            )?
            .unwrap()
            .uncles
            .len(),
        ))
    }
}

/*
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

    async fn init_mock_chain() -> anyhow::Result<Arc<impl EnvironmentKind>> {
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

    async fn start_server(db: Arc<impl EnvironmentKind>) -> anyhow::Result<(u16, HttpServerHandle)> {
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
        let balance = client
            .get_balance(DEADBEEF, block_number.into())
            .await
            .unwrap();
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

        let balance = client
            .get_balance(DEADBEEF, block_number.into())
            .await
            .unwrap();
        assert_eq!(balance, ETHER.as_u256() * 100);
    }
}*/