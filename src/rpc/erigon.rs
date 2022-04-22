use super::helpers;
use crate::{
    kv::{mdbx::*, MdbxWithDirHandle},
    models::*,
};
use async_trait::async_trait;
use ethereum_jsonrpc::{types, ErigonApiServer};
use jsonrpsee::core::RpcResult;
use std::sync::Arc;

pub struct ErigonApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
}

#[async_trait]
impl<DB> ErigonApiServer for ErigonApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn get_header_by_number(&self, block_number: u64) -> RpcResult<Option<types::Header>> {
        let tx = self.db.begin()?;

        if let Some((block_number, hash)) =
            helpers::resolve_block_id(&tx, types::BlockNumber::Number(block_number.into()))?
        {
            if let Some(BlockHeader {
                parent_hash,
                ommers_hash,
                beneficiary,
                state_root,
                transactions_root,
                receipts_root,
                logs_bloom,
                difficulty,
                number,
                gas_limit,
                gas_used,
                timestamp,
                extra_data,
                mix_hash,
                nonce,
                base_fee_per_gas,
            }) = crate::accessors::chain::header::read(&tx, hash, block_number)?
            {
                return Ok(Some(types::Header {
                    parent_hash,
                    sha3_uncles: ommers_hash,
                    miner: beneficiary,
                    state_root,
                    transactions_root,
                    receipts_root,
                    logs_bloom,
                    difficulty,
                    number: number.0.into(),
                    gas_limit: gas_limit.into(),
                    gas_used: gas_used.into(),
                    timestamp: timestamp.into(),
                    extra_data: extra_data.into(),
                    mix_hash,
                    nonce,
                    base_fee_per_gas,
                }));
            }
        }

        Ok(None)
    }
}
