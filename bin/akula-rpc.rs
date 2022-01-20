use akula::{
    binutil::AkulaDataDir,
    kv::{tables, traits::*},
    models::*,
    stagedsync::stages::*,
};
use async_trait::async_trait;
use clap::Parser;
use ethereum_types::{Address, H256, U256, U64};
use jsonrpsee::{core::RpcResult, http_server::HttpServerBuilder, proc_macros::rpc};
use std::{future::pending, net::SocketAddr, sync::Arc};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[clap(name = "Akula RPC", about = "RPC server for Akula")]
pub struct Opt {
    #[clap(long)]
    pub datadir: AkulaDataDir,

    #[clap(long)]
    pub listen_address: SocketAddr,
}

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
    // #[method(name = "getLogs")]
    // async fn get_logs(&self, filter: Filter) -> RpcResult<Vec<Log>>;
}
pub struct EthApiServerImpl<DB>
where
    DB: KV,
{
    db: Arc<DB>,
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
            akula::accessors::state::account::read(&self.db.begin().await?, address, block_number)
                .await?
                .unwrap_or_default()
                .nonce,
        ))
    }

    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256> {
        Ok(akula::accessors::state::account::read(
            &self.db.begin().await?,
            address,
            Some(block_number),
        )
        .await?
        .map(|acc| acc.balance)
        .unwrap_or_else(U256::zero))
    }

    async fn get_block_by_hash(&self, hash: H256, include_txs: bool) -> RpcResult<Block> {
        let txn = self.db.begin().await?;
        let number = akula::accessors::chain::header_number::read(&txn, hash)
            .await?
            .unwrap_or(BlockNumber(0));
        let header = PartialHeader::from(
            akula::accessors::chain::header::read(&txn, hash, number)
                .await?
                .unwrap_or_else(BlockHeader::empty),
        );

        if include_txs {
            let body =
                akula::accessors::chain::block_body::read_without_senders(&txn, hash, number)
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
        let hash = akula::accessors::chain::canonical_hash::read(&txn, number)
            .await?
            .unwrap();
        let header = PartialHeader::from(
            akula::accessors::chain::header::read(&txn, hash, number)
                .await?
                .unwrap(),
        );
        if include_txs {
            let body =
                akula::accessors::chain::block_body::read_without_senders(&txn, hash, number)
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
            akula::accessors::chain::block_body::read_without_senders(
                &txn,
                hash,
                akula::accessors::chain::header_number::read(&txn, hash)
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
            akula::accessors::chain::block_body::read_without_senders(
                &txn,
                akula::accessors::chain::canonical_hash::read(&txn, number)
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
        let number = akula::accessors::chain::tl::read(&txn, hash)
            .await?
            .unwrap();
        Ok(akula::accessors::chain::block_body::read_with_senders(
            &txn,
            akula::accessors::chain::canonical_hash::read(&txn, number)
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
        Ok(akula::accessors::chain::block_body::read_with_senders(
            &txn,
            block_hash,
            akula::accessors::chain::header_number::read(&txn, block_hash)
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
        Ok(akula::accessors::chain::block_body::read_with_senders(
            &txn,
            akula::accessors::chain::canonical_hash::read(&txn, block_number)
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info,rpc=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    let db = Arc::new(akula::kv::mdbx::Environment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &opt.datadir,
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?);

    let server = HttpServerBuilder::default().build(opt.listen_address)?;
    let _server_handle = server.start(EthApiServerImpl { db }.into_rpc())?;

    pending().await
}
