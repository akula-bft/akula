use crate::{crypto::keccak256, models};
use bytes::BytesMut;
use ethereum_types::{H256, U256};

#[derive(Clone, Debug)]
pub struct BlockHeader {
    pub header: models::BlockHeader,
    rlp_repr_cached: Option<BytesMut>,
    hash_cached: Option<H256>,
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

    fn rlp_repr_compute(&self) -> BytesMut {
        rlp::encode(&self.header)
    }

    pub fn rlp_repr_prepare(&mut self) {
        self.rlp_repr_cached = Some(self.rlp_repr_compute());
    }

    pub fn rlp_repr(&self) -> BytesMut {
        self.rlp_repr_cached
            .clone()
            .unwrap_or_else(|| self.rlp_repr_compute())
    }

    fn hash_compute(rlp_repr: &BytesMut) -> H256 {
        keccak256(rlp_repr.as_ref())
    }

    pub fn hash_prepare(&mut self) {
        if self.rlp_repr_cached.is_none() {
            self.rlp_repr_prepare();
        }
        // Not calling self.rlp_repr(), because it causes an extra clone,
        // but we just need a ref here.
        let rlp_repr_cached = self.rlp_repr_cached.as_ref().unwrap();
        self.hash_cached = Some(Self::hash_compute(rlp_repr_cached))
    }

    pub fn hash(&self) -> H256 {
        self.hash_cached
            .unwrap_or_else(|| Self::hash_compute(&self.rlp_repr()))
    }
}

impl From<models::BlockHeader> for BlockHeader {
    fn from(header: models::BlockHeader) -> Self {
        Self {
            header,
            rlp_repr_cached: None,
            hash_cached: None,
        }
    }
}
