use crate::models;
use ethereum_types::{H256, U256};

#[derive(Clone, Debug)]
pub struct BlockHeader {
    pub header: models::BlockHeader,
}

impl BlockHeader {
    pub fn difficulty(&self) -> U256 {
        self.header.difficulty
    }
    pub fn number(&self) -> models::BlockNumber {
        self.header.number
    }
    pub fn ommers_hash(&self) -> H256 {
        self.header.ommers_hash
    }
    pub fn parent_hash(&self) -> H256 {
        self.header.parent_hash
    }
    pub fn timestamp(&self) -> u64 {
        self.header.timestamp
    }
    pub fn hash(&self) -> H256 {
        self.header.hash()
    }
}

impl From<models::BlockHeader> for BlockHeader {
    fn from(header: models::BlockHeader) -> Self {
        Self { header }
    }
}
