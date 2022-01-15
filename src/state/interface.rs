use crate::models::*;
use async_trait::async_trait;
use auto_impl::auto_impl;
use bytes::Bytes;
use std::fmt::Debug;

#[async_trait]
#[auto_impl(&mut, Box)]
pub trait State: Debug + Send + Sync {
    async fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>>;

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes>;

    async fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256>;

    async fn erase_storage(&mut self, address: Address) -> anyhow::Result<()>;

    async fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>>;

    async fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>>;

    async fn total_difficulty(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>>;

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: BlockNumber);

    fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    );

    async fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()>;

    async fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    ) -> anyhow::Result<()>;
}
