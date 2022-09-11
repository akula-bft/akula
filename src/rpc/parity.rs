use super::helpers;
use crate::{
    kv::{mdbx::*, MdbxWithDirHandle},
    models::*,
};
use async_trait::async_trait;
use ethereum_jsonrpc::{types, ParityApiServer};
use jsonrpsee::core::RpcResult;
use std::{collections::BTreeSet, num::NonZeroUsize, sync::Arc};

pub struct ParityApiServerImpl<SE>
where
    SE: EnvironmentKind,
{
    pub db: Arc<MdbxWithDirHandle<SE>>,
}

#[async_trait]
impl<DB> ParityApiServer for ParityApiServerImpl<DB>
where
    DB: EnvironmentKind,
{
    async fn list_storage_keys(
        &self,
        address: Address,
        number_of_slots: NonZeroUsize,
        offset: Option<H256>,
        block_id: Option<types::BlockId>,
    ) -> RpcResult<Option<BTreeSet<H256>>> {
        let tx = self.db.begin()?;

        let block = match block_id {
            None
            | Some(types::BlockId::Number(types::BlockNumber::Latest))
            | Some(types::BlockId::Number(types::BlockNumber::Pending)) => None,
            Some(block_id) => {
                if let Some((block_number, _)) = helpers::resolve_block_id(&tx, block_id)? {
                    Some(block_number)
                } else {
                    return Ok(None);
                }
            }
        };

        Ok(Some(
            crate::accessors::state::storage::walk(&tx, address, offset, block)
                .take(number_of_slots.get())
                .map(|res| res.map(|(slot, _)| slot))
                .collect::<anyhow::Result<_>>()?,
        ))
    }
}
