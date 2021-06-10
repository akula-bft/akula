use crate::downloader::{
    messages::*,
    sentry_client::{EthMessageId, Message},
};

pub fn decode_rlp_message(
    id: EthMessageId,
    message_bytes: &[u8],
) -> anyhow::Result<Box<dyn Message>> {
    let message: Box<dyn Message> = match id {
        // EthMessageId::NewBlockHashes => Box::new(rlp::decode::<NewBlockHashesMessage>(message_bytes)?),
        // EthMessageId::Transactions => Box::new(rlp::decode::<TransactionsMessage>(message_bytes)?),
        EthMessageId::GetBlockHeaders => {
            Box::new(rlp::decode::<GetBlockHeadersMessage>(message_bytes)?)
        }
        // EthMessageId::BlockHeaders => Box::new(rlp::decode::<BlockHeadersMessage>(message_bytes)?),
        // EthMessageId::GetBlockBodies => Box::new(rlp::decode::<GetBlockBodiesMessage>(message_bytes)?),
        // EthMessageId::BlockBodies => Box::new(rlp::decode::<BlockBodiesMessage>(message_bytes)?),
        // EthMessageId::NewBlock => Box::new(rlp::decode::<NewBlockMessage>(message_bytes)?),
        // EthMessageId::NewPooledTransactionHashes => Box::new(rlp::decode::<NewPooledTransactionHashesMessage>(message_bytes)?),
        // EthMessageId::GetPooledTransactions => Box::new(rlp::decode::<GetPooledTransactionsMessage>(message_bytes)?),
        // EthMessageId::PooledTransactions => Box::new(rlp::decode::<PooledTransactionsMessage>(message_bytes)?),
        // EthMessageId::GetNodeData => Box::new(rlp::decode::<GetNodeDataMessage>(message_bytes)?),
        // EthMessageId::NodeData => Box::new(rlp::decode::<NodeDataMessage>(message_bytes)?),
        // EthMessageId::GetReceipts => Box::new(rlp::decode::<GetReceiptsMessage>(message_bytes)?),
        // EthMessageId::Receipts => Box::new(rlp::decode::<ReceiptsMessage>(message_bytes)?),
        _ => anyhow::bail!("decode_rlp_message: unsupported message {:?}", id),
    };
    Ok(message)
}
