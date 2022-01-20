use crate::{
    accessors,
    kv::{
        tables,
        traits::{Transaction, KV},
    },
    models::{Block, BlockHeader, BlockNumber, MessageWithSender, PartialHeader, Receipt},
    stagedsync::stages::FINISH,
    u256_to_h256,
};
use async_trait::async_trait;
use ethereum_types::{Address, H256, U256, U64};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use std::sync::Arc;

// #[derive(Serialize, Deserialize)]
// struct Message {
//     from: Address,
//     to: Address,
//     value: U256,
//     data: Vec<u8>,
// }

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<BlockNumber>;
    #[method(name = "getTransactionCount")]
    async fn get_transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U256>;
    #[method(name = "getBalance")]
    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256>;
    #[method(name = "getBlockByHash")]
    async fn get_block_by_hash(&self, hash: H256, include_txs: bool) -> RpcResult<Block>;
    #[method(name = "getBlockByNumber")]
    async fn get_block_by_number(
        &self,
        block_number: BlockNumber,
        include_txs: bool,
    ) -> RpcResult<Block>;
    #[method(name = "getBlockTransactionCountByHash")]
    async fn get_block_transaction_count_by_hash(&self, hash: H256) -> RpcResult<U64>;
    #[method(name = "getBlockTransactionCountByNumber")]
    async fn get_block_transaction_count_by_number(
        &self,
        block_number: BlockNumber,
    ) -> RpcResult<U64>;
    #[method(name = "getTransactionByHash")]
    async fn get_transaction_by_hash(&self, hash: H256) -> RpcResult<MessageWithSender>;
    #[method(name = "getTransactionByBlockHashAndIndex")]
    async fn get_transaction_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<MessageWithSender>;
    #[method(name = "getTransactionByBlockNumberAndIndex")]
    async fn get_transaction_by_block_number_and_index(
        &self,
        block_number: BlockNumber,
        index: U64,
    ) -> RpcResult<MessageWithSender>;
    #[method(name = "getTransactionReceipt")]
    async fn get_transaction_receipt(&self, hash: H256) -> RpcResult<Receipt>;
    #[method(name = "getCode")]
    async fn get_code(&self, address: Address, block_number: BlockNumber) -> RpcResult<H256>;
    #[method(name = "getStorageAt")]
    async fn get_storage_at(
        &self,
        address: Address,
        index: U256,
        block_number: BlockNumber,
    ) -> RpcResult<H256>;
    // #[method(name = "call")]
    // async fn call(&self, transaction: Message, block_number: BlockNumber) -> RpcResult<Receipt>;
}
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
    async fn block_number(&self) -> RpcResult<BlockNumber> {
        Ok(self
            .db
            .begin()
            .await?
            .get(tables::SyncStage, FINISH)
            .await?
            .unwrap_or(BlockNumber(0)))
    }

    async fn get_transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockNumber>,
    ) -> RpcResult<U256> {
        Ok(U256::from(
            accessors::state::account::read(&self.db.begin().await?, address, block_number)
                .await?
                .unwrap_or_default()
                .nonce,
        ))
    }

    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256> {
        Ok(
            accessors::state::account::read(&self.db.begin().await?, address, Some(block_number))
                .await?
                .map(|acc| acc.balance)
                .unwrap_or_else(U256::zero),
        )
    }

    async fn get_block_by_hash(&self, hash: H256, include_txs: bool) -> RpcResult<Block> {
        let txn = self.db.begin().await?;
        let number = accessors::chain::header_number::read(&txn, hash)
            .await?
            .unwrap_or(BlockNumber(0));
        let header = PartialHeader::from(
            accessors::chain::header::read(&txn, hash, number)
                .await?
                .unwrap_or_else(BlockHeader::empty),
        );

        if include_txs {
            let body = accessors::chain::block_body::read_without_senders(&txn, hash, number)
                .await?
                .unwrap();
            Ok(Block::new(header, body.transactions, body.ommers))
        } else {
            Ok(Block::new(header, vec![], vec![]))
        }
    }

    async fn get_block_by_number(
        &self,
        number: BlockNumber,
        include_txs: bool,
    ) -> RpcResult<Block> {
        let txn = self.db.begin().await?;
        let hash = accessors::chain::canonical_hash::read(&txn, number)
            .await?
            .unwrap();
        let header = PartialHeader::from(
            accessors::chain::header::read(&txn, hash, number)
                .await?
                .unwrap(),
        );
        if include_txs {
            let body = accessors::chain::block_body::read_without_senders(&txn, hash, number)
                .await?
                .unwrap();
            Ok(Block::new(header, body.transactions, body.ommers))
        } else {
            Ok(Block::new(header, vec![], vec![]))
        }
    }

    async fn get_block_transaction_count_by_hash(&self, hash: H256) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        Ok(U64::from(
            accessors::chain::block_body::read_without_senders(
                &txn,
                hash,
                accessors::chain::header_number::read(&txn, hash)
                    .await?
                    .unwrap_or(BlockNumber(0)),
            )
            .await?
            .unwrap()
            .transactions
            .len() as u64,
        ))
    }

    async fn get_block_transaction_count_by_number(&self, number: BlockNumber) -> RpcResult<U64> {
        let txn = self.db.begin().await?;
        Ok(U64::from(
            accessors::chain::block_body::read_without_senders(
                &txn,
                accessors::chain::canonical_hash::read(&txn, number)
                    .await?
                    .unwrap(),
                number,
            )
            .await?
            .unwrap()
            .transactions
            .len() as u64,
        ))
    }

    async fn get_transaction_by_hash(&self, hash: H256) -> RpcResult<MessageWithSender> {
        let txn = self.db.begin().await?;
        let number = accessors::chain::tl::read(&txn, hash).await?.unwrap();
        Ok(accessors::chain::block_body::read_with_senders(
            &txn,
            accessors::chain::canonical_hash::read(&txn, number)
                .await?
                .unwrap(),
            number,
        )
        .await?
        .unwrap()
        .transactions
        .into_iter()
        .find(|tx| tx.hash() == hash)
        .unwrap())
    }

    async fn get_transaction_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: U64,
    ) -> RpcResult<MessageWithSender> {
        let txn = self.db.begin().await?;
        Ok(accessors::chain::block_body::read_with_senders(
            &txn,
            block_hash,
            accessors::chain::header_number::read(&txn, block_hash)
                .await?
                .unwrap_or(BlockNumber(0)),
        )
        .await?
        .unwrap()
        .transactions
        .into_iter()
        .nth(index.as_u64() as usize)
        .unwrap())
    }

    async fn get_transaction_by_block_number_and_index(
        &self,
        block_number: BlockNumber,
        index: U64,
    ) -> RpcResult<MessageWithSender> {
        let txn = self.db.begin().await?;
        Ok(accessors::chain::block_body::read_with_senders(
            &txn,
            accessors::chain::canonical_hash::read(&txn, block_number)
                .await?
                .unwrap(),
            block_number,
        )
        .await?
        .unwrap()
        .transactions
        .into_iter()
        .nth(index.as_u64() as usize)
        .unwrap())
    }
    async fn get_transaction_receipt(&self, _hash: H256) -> RpcResult<Receipt> {
        todo!()
    }

    async fn get_code(&self, address: Address, block_number: BlockNumber) -> RpcResult<H256> {
        Ok(
            accessors::state::account::read(&self.db.begin().await?, address, Some(block_number))
                .await?
                .unwrap()
                .code_hash,
        )
    }

    async fn get_storage_at(
        &self,
        address: Address,
        position: U256,
        block_number: BlockNumber,
    ) -> RpcResult<H256> {
        Ok(u256_to_h256(
            accessors::state::storage::read(
                &self.db.begin().await?,
                address,
                position,
                Some(block_number),
            )
            .await?,
        ))
    }

    // async fn call(
    //     &self,
    //     transaction: Message,
    //     block_number: BlockNumber,
    // ) -> RpcResult<Receipt> {
    //     let txn = self.db.begin().await?;
    //     let hash = accessors::chain::canonical_hash::read(&txn, block_number)
    //         .await?
    //         .unwrap();
    //     let header = PartialHeader::from(
    //         accessors::chain::header::read(&txn, hash, block_number)
    //             .await?
    //             .unwrap(),
    //     );
    //     let block = accessors::chain::block_body::read_with_senders(&txn, hash, block_number)
    //         .await?
    //         .unwrap();
    //     let mut state = InMemoryState::default();
    //     let mut cache = AnalysisCache::default();
    //     let mut engine = engine_factory(MAINNET.clone()).unwrap();
    //     let spec = MAINNET.collect_block_spec(header.number);
    //     let mut processor = ExecutionProcessor::new(
    //         &mut state,
    //         None,
    //         &mut cache,
    //         &mut *engine,
    //         &header,
    //         &block,
    //         &spec,
    //     );
    //     Ok(processor.execute_transaction(&MessageWithSender { message: , sender: transaction.from }).await?)
    // }
}
