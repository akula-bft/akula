use async_trait::async_trait;
use bytes::Bytes;

use crate::{common, models::Account};

#[async_trait]
pub trait StateReader {
    async fn read_account_data(&self, address: common::Address) -> anyhow::Result<Option<Account>>;
    async fn read_account_storage(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        key: Option<common::Hash>,
    ) -> anyhow::Result<Option<&[u8]>>;
    async fn read_account_code(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
    ) -> anyhow::Result<Option<&[u8]>>;
    async fn read_account_code_size(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
    ) -> anyhow::Result<Option<&[u8]>>;
    async fn read_account_incarnation(address: common::Address) -> anyhow::Result<Option<u64>>;
}

#[async_trait]
pub trait StateWriter {
    async fn update_account_data(
        &mut self,
        address: common::Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()>;
    async fn update_account_code(
        &mut self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
        code: &[u8],
    ) -> anyhow::Result<()>;
    async fn delete_account(
        &mut self,
        address: common::Address,
        original: &Account,
    ) -> anyhow::Result<()>;
    async fn write_account_storage(
        &mut self,
        address: common::Address,
        incarnation: common::Incarnation,
        key: Option<common::Hash>,
        original: Option<common::Value>,
        value: Option<common::Value>,
    ) -> anyhow::Result<()>;
    async fn create_contract(&mut self, address: common::Address) -> anyhow::Result<()>;
}

#[async_trait]
pub trait WriterWithChangesets: StateWriter {
    async fn write_changesets(&mut self) -> anyhow::Result<()>;
    async fn write_history(&mut self) -> anyhow::Result<()>;
}

#[derive(Clone, Default, Debug)]
pub struct Noop;

#[async_trait]
impl StateWriter for Noop {
    async fn update_account_data(
        &mut self,
        address: ethereum_types::Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_account_code(
        &mut self,
        address: ethereum_types::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
        code: &[u8],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_account(
        &mut self,
        address: ethereum_types::Address,
        original: &Account,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        address: ethereum_types::Address,
        incarnation: common::Incarnation,
        key: Option<common::Hash>,
        original: Option<common::Value>,
        value: Option<common::Value>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_contract(&mut self, address: ethereum_types::Address) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl WriterWithChangesets for Noop {
    async fn write_changesets(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_history(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
