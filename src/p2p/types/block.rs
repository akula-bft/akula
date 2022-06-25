use crate::models::{Block, BlockNumber, H256};
use fastrlp::*;

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct GetBlockBodies {
    pub request_id: u64,
    pub hashes: Vec<H256>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum BlockId {
    Hash(H256),
    Number(BlockNumber),
}

impl From<BlockNumber> for BlockId {
    #[inline(always)]
    fn from(number: BlockNumber) -> Self {
        BlockId::Number(number)
    }
}

impl From<H256> for BlockId {
    #[inline(always)]
    fn from(hash: H256) -> Self {
        BlockId::Hash(hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct NewBlockHashes(pub Vec<BlockHashAndNumber>);

impl NewBlockHashes {
    #[inline(always)]
    pub fn new(block_hashes: Vec<(H256, BlockNumber)>) -> Self {
        Self(
            block_hashes
                .into_iter()
                .map(|(hash, number)| BlockHashAndNumber { hash, number })
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable)]
pub struct BlockHashAndNumber {
    pub hash: H256,
    pub number: BlockNumber,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NewBlock {
    pub block: Block,
    pub total_difficulty: u128,
}
