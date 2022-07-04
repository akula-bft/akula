use crate::models::*;
use auto_impl::auto_impl;
use bytes::Bytes;
use std::fmt::Debug;

#[auto_impl(&mut, &, Box)]
pub trait BlockState: Debug + Send + Sync {
    fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>>;

    fn read_parent_header(&self, header: &BlockHeader) -> anyhow::Result<Option<BlockHeader>> {
        if let Some(parent_number) = header.number.0.checked_sub(1) {
            return self.read_header(parent_number.into(), header.parent_hash);
        }

        Ok(None)
    }

    fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>>;
}

#[auto_impl(&mut, Box)]
pub trait State: BlockState {
    fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>>;

    fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes>;

    fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256>;

    fn erase_storage(&mut self, address: Address) -> anyhow::Result<()>;

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

    fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()>;

    fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    ) -> anyhow::Result<()>;
}
