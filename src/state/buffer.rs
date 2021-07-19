use crate::models::*;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_types::{Address, H256, U256};

#[async_trait]
pub trait State<'storage>: Send + Sync {
    // Readers

    async fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>>;

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes<'storage>>;

    async fn read_storage(
        &self,
        address: Address,
        incarnation: u64,
        location: H256,
    ) -> anyhow::Result<H256>;

    // Previous non-zero incarnation of an account; 0 if none exists.
    async fn previous_incarnation(&self, address: Address) -> anyhow::Result<u64>;

    async fn read_header(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>>;

    async fn read_body(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>>;

    async fn read_body_with_senders(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>>;

    async fn total_difficulty(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>>;

    async fn state_root_hash(&self) -> anyhow::Result<H256>;

    async fn current_canonical_block(&self) -> anyhow::Result<u64>;

    async fn canonical_hash(&self, block_number: u64) -> anyhow::Result<Option<H256>>;

    async fn insert_block(&mut self, block: Block, hash: H256) -> anyhow::Result<()>;

    async fn canonize_block(&mut self, block_number: u64, block_hash: H256) -> anyhow::Result<()>;

    async fn decanonize_block(&mut self, block_number: u64) -> anyhow::Result<()>;

    async fn insert_receipts(
        &mut self,
        block_number: u64,
        receipts: &[Receipt],
    ) -> anyhow::Result<()>;

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: u64);

    async fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) -> anyhow::Result<()>;

    async fn update_account_code(
        &mut self,
        address: Address,
        incarnation: u64,
        code_hash: H256,
        code: Bytes<'storage>,
    ) -> anyhow::Result<()>;

    async fn update_storage(
        &mut self,
        address: Address,
        incarnation: u64,
        location: H256,
        initial: H256,
        current: H256,
    ) -> anyhow::Result<()>;

    async fn unwind_state_changes(&mut self, block_number: u64) -> anyhow::Result<()>;
}
