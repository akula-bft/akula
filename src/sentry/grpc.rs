use super::eth::{EthMessageId, EthProtocolVersion};
use anyhow::bail;
use ethereum_interfaces::sentry;
use std::convert::TryFrom;

impl From<EthMessageId> for sentry::MessageId {
    fn from(id: EthMessageId) -> Self {
        match id {
            EthMessageId::Status => Self::Status66,
            EthMessageId::NewBlockHashes => Self::NewBlockHashes66,
            EthMessageId::Transactions => Self::Transactions66,
            EthMessageId::GetBlockHeaders => Self::GetBlockHeaders66,
            EthMessageId::BlockHeaders => Self::BlockHeaders66,
            EthMessageId::GetBlockBodies => Self::GetBlockBodies66,
            EthMessageId::BlockBodies => Self::BlockBodies66,
            EthMessageId::NewBlock => Self::NewBlock66,
            EthMessageId::NewPooledTransactionHashes => Self::NewPooledTransactionHashes66,
            EthMessageId::GetPooledTransactions => Self::GetPooledTransactions66,
            EthMessageId::PooledTransactions => Self::PooledTransactions66,
            EthMessageId::GetNodeData => Self::GetNodeData66,
            EthMessageId::NodeData => Self::NodeData66,
            EthMessageId::GetReceipts => Self::GetReceipts66,
            EthMessageId::Receipts => Self::Receipts66,
        }
    }
}

impl TryFrom<sentry::MessageId> for EthMessageId {
    type Error = anyhow::Error;

    fn try_from(id: sentry::MessageId) -> Result<Self, Self::Error> {
        Ok(match id {
            sentry::MessageId::NewBlockHashes66 => Self::NewBlockHashes,
            sentry::MessageId::NewBlock66 => Self::NewBlock,
            sentry::MessageId::Transactions66 => Self::Transactions,
            sentry::MessageId::NewPooledTransactionHashes66 => Self::NewPooledTransactionHashes,
            sentry::MessageId::GetBlockHeaders66 => Self::GetBlockHeaders,
            sentry::MessageId::GetBlockBodies66 => Self::GetBlockBodies,
            sentry::MessageId::GetNodeData66 => Self::GetNodeData,
            sentry::MessageId::GetReceipts66 => Self::GetReceipts,
            sentry::MessageId::GetPooledTransactions66 => Self::GetPooledTransactions,
            sentry::MessageId::BlockHeaders66 => Self::BlockHeaders,
            sentry::MessageId::BlockBodies66 => Self::BlockBodies,
            sentry::MessageId::NodeData66 => Self::NodeData,
            sentry::MessageId::Receipts66 => Self::Receipts,
            sentry::MessageId::PooledTransactions66 => Self::PooledTransactions,
            other => bail!("Unsupported message id: {:?}", other),
        })
    }
}

impl From<EthProtocolVersion> for sentry::Protocol {
    fn from(version: EthProtocolVersion) -> Self {
        match version {
            EthProtocolVersion::Eth65 => Self::Eth65,
            EthProtocolVersion::Eth66 => Self::Eth66,
        }
    }
}
