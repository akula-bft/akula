use crate::downloader::{block_id::BlockId, sentry_client};
use rlp_derive;
use sentry_client::EthMessageId;

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable)]
pub struct GetBlockHeadersMessage {
    pub request_id: u64,
    pub start_block: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: bool,
}

macro_rules! message_id_impl {
    ($id:ident, $message_type:ident) => {
        impl sentry_client::Identifiable for $message_type {
            fn id(&self) -> EthMessageId {
                EthMessageId::$id
            }
        }
        impl sentry_client::Message for $message_type {}
    };
}

message_id_impl!(GetBlockHeaders, GetBlockHeadersMessage);
