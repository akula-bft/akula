use crate::{
    models::{BlockHeader, BlockNumber, H256},
    p2p::types::BlockId,
};
use fastrlp::*;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct HeaderRequest {
    pub start: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: bool,
}

impl const Default for HeaderRequest {
    #[inline(always)]
    fn default() -> Self {
        HeaderRequest {
            start: BlockId::Number(BlockNumber(0)),
            limit: 1024,
            skip: 0,
            reverse: false,
        }
    }
}

pub struct Announce {
    pub hash: H256,
    pub number: BlockNumber,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeaders {
    pub request_id: u64,
    pub params: GetBlockHeadersParams,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeadersParams {
    pub start: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: u8,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockHeaders {
    pub request_id: u64,
    pub headers: Vec<BlockHeader>,
}
