use akula::{
    accessors::{chain::*, state::account},
    binutil::AkulaDataDir,
    kv::{tables, traits::*},
    models::*,
    stagedsync::stages::*,
};
use async_trait::async_trait;
use clap::Parser;
use ethereum_types::{Address, U256, U64, *};
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
    #[method(name = "getBalance")]
    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256>;
    #[method(name = "getBlockByHash")]
    async fn get_block_by_hash(
        &self,
        block_hash: H256,
        full_tx_obj: bool,
    ) -> RpcResult<jsonrpc::common::Block>;
    #[method(name = "getBlockTransactionCountByHash")]
    async fn get_block_tx_count_by_hash(&self, block_hash: H256) -> RpcResult<U64>;
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
        Ok(FINISH
            .get_progress(&self.db.begin().await?)
            .await?
            .unwrap_or(BlockNumber(0)))
    }

    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256> {
        Ok(
            account::read(&self.db.begin().await?, address, Some(block_number))
                .await?
                .map(|acc| acc.balance)
                .unwrap_or_else(U256::zero),
        )
    }

    async fn get_block_by_hash(
        &self,
        block_hash: H256,
        full_tx_obj: bool,
    ) -> RpcResult<jsonrpc::common::Block> {
        let block_number = header_number::read(&self.db.begin().await?, block_hash)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        let msgs = block_body.transactions;
        let index = 0;
        let mut msg_hashes: Vec<jsonrpc::common::Transaction> = Vec::new();
        while index < msgs.len() {
            match full_tx_obj {
                false => msg_hashes.push(jsonrpc::common::Transaction::Partial(msgs[index].hash())),
                true => {
                    let v = YParityAndChainId {
                        odd_y_parity: msgs[index].signature.odd_y_parity(),
                        chain_id: msgs[index].chain_id(),
                    };

                    let to = match msgs[index].action() {
                        TransactionAction::Call(to) => Some(to),
                        TransactionAction::Create => None,
                    };

                    msg_hashes.push(jsonrpc::common::Transaction::Full(Box::new(
                        jsonrpc::common::Tx {
                            block_hash: Some(block_hash),
                            block_number: Some(U64::from(block_number.0)),
                            from: msgs[index].recover_sender().unwrap(),
                            gas: U64::from(msgs[index].gas_limit()),
                            gas_price: msgs[index].max_fee_per_gas(),
                            hash: msgs[index].hash(),
                            input: msgs[index].input().clone(),
                            nonce: U64::from(msgs[index].nonce()),
                            to,
                            transaction_index: Some(U64::from(index)),
                            value: msgs[index].value(),
                            v: U64::from(v.v()),
                            r: msgs[index].r(),
                            s: msgs[index].s(),
                        },
                    )))
                }
            }
        }

        let total_difficulty = td::read(&self.db.begin().await?, block_hash, block_number)
            .await?
            .unwrap();

        let size = rlp::encode(
            &(block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap()),
        )
        .len();

        let ommers = block_body.ommers;
        let mut ommer_hashes: Vec<H256> = Vec::new();
        for ommer in ommers.iter() {
            ommer_hashes.push(ommer.hash());
        }

        let header = header::read(&self.db.begin().await?, block_hash, block_number)
            .await?
            .unwrap();

        Ok(jsonrpc::common::Block {
            number: Some(U64::from(block_number.0)),
            hash: Some(block_hash),
            parent_hash: header.parent_hash,
            nonce: Some(header.nonce),
            sha3_uncles: header.ommers_hash,
            logs_bloom: Some(header.logs_bloom),
            transactions_root: header.transactions_root,
            state_root: header.state_root,
            receipts_root: header.receipts_root,
            miner: header.beneficiary,
            difficulty: header.difficulty,
            total_difficulty,
            extra_data: header.extra_data,
            size: U64::from(size),
            gas_limit: U64::from(header.gas_limit),
            gas_used: U64::from(header.gas_used),
            timestamp: U64::from(header.timestamp),
            transactions: msg_hashes,
            uncles: ommer_hashes,
        })
    }

    async fn get_block_tx_count_by_hash(&self, block_hash: H256) -> RpcResult<U64> {
        let block_number = header_number::read(&self.db.begin().await?, block_hash)
            .await?
            .unwrap();

        let block_body = storage_body::read(&self.db.begin().await?, block_hash, block_number)
            .await?
            .unwrap();

        Ok(block_body.tx_amount.into())
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
