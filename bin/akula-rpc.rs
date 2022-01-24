use akula::{
    accessors::{
        chain::*,
        state::{account, storage},
    },
    binutil::AkulaDataDir,
    kv::{tables, traits::*},
    models::*,
    stagedsync::stages::*,
};
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use ethereum_types::{Address, U256, U64, *};
use jsonrpc::common::{StoragePos, TxIndex, UncleIndex};
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
    #[method(name = "getBlockByNumber")]
    async fn get_block_by_number(
        &self,
        block_number: BlockNumber,
        full_tx_obj: bool,
    ) -> RpcResult<jsonrpc::common::Block>;
    #[method(name = "getBlockTransactionCountByHash")]
    async fn get_block_tx_count_by_hash(&self, block_hash: H256) -> RpcResult<U64>;
    #[method(name = "getBlockTransactionCountByNumber")]
    async fn get_block_tx_count_by_number(&self, block_number: BlockNumber) -> RpcResult<U64>;
    #[method(name = "getCode")]
    async fn get_code(&self, address: Address, block_number: BlockNumber) -> RpcResult<Bytes>;
    #[method(name = "getStorageAt")]
    async fn get_storage_at(
        &self,
        address: Address,
        storage_pos: StoragePos,
        block_number: BlockNumber,
    ) -> RpcResult<U256>;
    #[method(name = "getTransactionByBlockHashAndIndex")]
    async fn get_tx_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: TxIndex,
    ) -> RpcResult<jsonrpc::common::Tx>;
    #[method(name = "getTransactionByBlockNumberAndIndex")]
    async fn get_tx_by_block_number_and_index(
        &self,
        block_number: BlockNumber,
        index: TxIndex,
    ) -> RpcResult<jsonrpc::common::Tx>;
    #[method(name = "getUncleByBlockHashAndIndex")]
    async fn get_uncle_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: UncleIndex,
    ) -> RpcResult<jsonrpc::common::Block>;
    #[method(name = "getUncleByBlockNumberAndIndex")]
    async fn get_uncle_by_block_number_and_index(
        &self,
        block_number: BlockNumber,
        index: UncleIndex,
    ) -> RpcResult<jsonrpc::common::Block>;
    #[method(name = "getUncleCountByBlockHash")]
    async fn get_uncle_count_by_block_hash(&self, block_hash: H256) -> RpcResult<U64>;
    #[method(name = "getUncleCountByBlockNumber")]
    async fn get_uncle_count_by_block_number(&self, block_number: BlockNumber) -> RpcResult<U64>;
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
        let mut index = 0;
        let mut msg_hashes: Vec<jsonrpc::common::Transaction> = Vec::new();
        while index < msgs.len() {
            match full_tx_obj {
                false => msg_hashes.push(jsonrpc::common::Transaction::Partial(msgs[index].hash())),
                true => {
                    let tx =
                        json_tx::assemble_tx(block_hash, block_number, &msgs[index], index).await;
                    msg_hashes.push(jsonrpc::common::Transaction::Full(Box::new(tx)));
                }
            }
            index = index + 1;
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

    async fn get_block_by_number(
        &self,
        block_number: BlockNumber,
        full_tx_obj: bool,
    ) -> RpcResult<jsonrpc::common::Block> {
        let block_hash = canonical_hash::read(&self.db.begin().await?, block_number)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        let msgs = block_body.transactions;
        let mut index = 0;
        let mut msg_hashes: Vec<jsonrpc::common::Transaction> = Vec::new();
        while index < msgs.len() {
            match full_tx_obj {
                false => msg_hashes.push(jsonrpc::common::Transaction::Partial(msgs[index].hash())),
                true => {
                    let tx =
                        json_tx::assemble_tx(block_hash, block_number, &msgs[index], index).await;
                    msg_hashes.push(jsonrpc::common::Transaction::Full(Box::new(tx)));
                }
            }
            index = index + 1;
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

    async fn get_block_tx_count_by_number(&self, block_number: BlockNumber) -> RpcResult<U64> {
        let block_hash = canonical_hash::read(&self.db.begin().await?, block_number)
            .await?
            .unwrap();

        let block_body = storage_body::read(&self.db.begin().await?, block_hash, block_number)
            .await?
            .unwrap();

        Ok(block_body.tx_amount.into())
    }

    async fn get_code(&self, address: Address, block_number: BlockNumber) -> RpcResult<Bytes> {
        let code_hash = account::read(&self.db.begin().await?, address, Some(block_number))
            .await?
            .unwrap()
            .code_hash;

        Ok(
            akula::read_account_code(&self.db.begin().await?, Address::from([1; 20]), code_hash)
                .await?
                .unwrap(),
        )
    }

    async fn get_storage_at(
        &self,
        address: Address,
        storage_pos: StoragePos,
        block_number: BlockNumber,
    ) -> RpcResult<U256> {
        Ok(storage::read(
            &self.db.begin().await?,
            address,
            storage_pos,
            Some(block_number),
        )
        .await?)
    }

    async fn get_tx_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: TxIndex,
    ) -> RpcResult<jsonrpc::common::Tx> {
        let block_number = header_number::read(&self.db.begin().await?, block_hash)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        let i = index.as_u64() as usize;
        let msgs = block_body.transactions;
        Ok(json_tx::assemble_tx(block_hash, block_number, &msgs[i], i).await)
    }

    async fn get_tx_by_block_number_and_index(
        &self,
        block_number: BlockNumber,
        index: TxIndex,
    ) -> RpcResult<jsonrpc::common::Tx> {
        let block_hash = canonical_hash::read(&self.db.begin().await?, block_number)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        let i = index.as_u64() as usize;
        let msgs = block_body.transactions;
        Ok(json_tx::assemble_tx(block_hash, block_number, &msgs[i], i).await)
    }

    async fn get_uncle_by_block_hash_and_index(
        &self,
        block_hash: H256,
        index: UncleIndex,
    ) -> RpcResult<jsonrpc::common::Block> {
        let block_number = header_number::read(&self.db.begin().await?, block_hash)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        let ommer_hash = block_body.ommers[index.as_u64() as usize].hash();

        let ommer_block_number = header_number::read(&self.db.begin().await?, ommer_hash)
            .await?
            .unwrap();

        let ommer = block_body::read_without_senders(
            &self.db.begin().await?,
            ommer_hash,
            ommer_block_number,
        )
        .await?
        .unwrap();

        let msgs = ommer.transactions;
        let mut index = 0;
        let mut msg_hashes: Vec<jsonrpc::common::Transaction> = Vec::new();
        while index < msgs.len() {
            let tx = json_tx::assemble_tx(block_hash, block_number, &msgs[index], index).await;
            msg_hashes.push(jsonrpc::common::Transaction::Full(Box::new(tx)));
            index = index + 1;
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

        let ommers = ommer.ommers;
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

    async fn get_uncle_by_block_number_and_index(
        &self,
        block_number: BlockNumber,
        index: UncleIndex,
    ) -> RpcResult<jsonrpc::common::Block> {
        let block_hash = canonical_hash::read(&self.db.begin().await?, block_number)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        let ommer_hash = block_body.ommers[index.as_u64() as usize].hash();

        let ommer_block_number = header_number::read(&self.db.begin().await?, ommer_hash)
            .await?
            .unwrap();

        let ommer = block_body::read_without_senders(
            &self.db.begin().await?,
            ommer_hash,
            ommer_block_number,
        )
        .await?
        .unwrap();

        let msgs = ommer.transactions;
        let mut index = 0;
        let mut msg_hashes: Vec<jsonrpc::common::Transaction> = Vec::new();
        while index < msgs.len() {
            let tx = json_tx::assemble_tx(block_hash, block_number, &msgs[index], index).await;
            msg_hashes.push(jsonrpc::common::Transaction::Full(Box::new(tx)));
            index = index + 1;
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

        let ommers = ommer.ommers;
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

    async fn get_uncle_count_by_block_hash(&self, block_hash: H256) -> RpcResult<U64> {
        let block_number = header_number::read(&self.db.begin().await?, block_hash)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        Ok(block_body.ommers.len().into())
    }

    async fn get_uncle_count_by_block_number(&self, block_number: BlockNumber) -> RpcResult<U64> {
        let block_hash = canonical_hash::read(&self.db.begin().await?, block_number)
            .await?
            .unwrap();

        let block_body =
            block_body::read_without_senders(&self.db.begin().await?, block_hash, block_number)
                .await?
                .unwrap();

        Ok(block_body.ommers.len().into())
    }
}

pub mod json_tx {
    use super::*;

    pub async fn assemble_tx(
        block_hash: H256,
        block_number: BlockNumber,
        msg: &MessageWithSignature,
        index: usize,
    ) -> jsonrpc::common::Tx {
        let v = YParityAndChainId {
            odd_y_parity: msg.signature.odd_y_parity(),
            chain_id: msg.chain_id(),
        };

        let to = match msg.action() {
            TransactionAction::Call(to) => Some(to),
            TransactionAction::Create => None,
        };

        jsonrpc::common::Tx {
            block_hash: Some(block_hash),
            block_number: Some(U64::from(block_number.0)),
            from: msg.recover_sender().unwrap(),
            gas: U64::from(msg.gas_limit()),
            gas_price: msg.max_fee_per_gas(),
            hash: msg.hash(),
            input: msg.input().clone(),
            nonce: U64::from(msg.nonce()),
            to,
            transaction_index: Some(U64::from(index)),
            value: msg.value(),
            v: U64::from(v.v()),
            r: msg.r(),
            s: msg.s(),
        }
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
