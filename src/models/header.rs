use super::*;
use crate::crypto::*;
use bytes::{Bytes, BytesMut};
use fastrlp::*;
use parity_scale_codec::*;

#[derive(Clone, Debug, PartialEq, Eq, Default, Encode, Decode)]
/// Ethereum block header definition.
pub struct BlockHeader {
    pub parent_hash: H256,
    pub ommers_hash: H256,
    pub beneficiary: H160,
    pub state_root: H256,
    pub transactions_root: H256,
    pub receipts_root: H256,
    pub logs_bloom: Bloom,
    pub difficulty: U256,
    pub number: BlockNumber,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes,
    pub mix_hash: H256,
    pub nonce: H64,
    pub base_fee_per_gas: Option<U256>,
}

impl BlockHeader {
    fn rlp_header(&self) -> Header {
        let mut rlp_head = Header {
            list: true,
            payload_length: 0,
        };

        rlp_head.payload_length += KECCAK_LENGTH + 1; // parent_hash
        rlp_head.payload_length += KECCAK_LENGTH + 1; // ommers_hash
        rlp_head.payload_length += ADDRESS_LENGTH + 1; // beneficiary
        rlp_head.payload_length += KECCAK_LENGTH + 1; // state_root
        rlp_head.payload_length += KECCAK_LENGTH + 1; // transactions_root
        rlp_head.payload_length += KECCAK_LENGTH + 1; // receipts_root
        rlp_head.payload_length += BLOOM_BYTE_LENGTH + length_of_length(BLOOM_BYTE_LENGTH); // logs_bloom
        rlp_head.payload_length += self.difficulty.length(); // difficulty
        rlp_head.payload_length += self.number.length(); // block height
        rlp_head.payload_length += self.gas_limit.length(); // gas_limit
        rlp_head.payload_length += self.gas_used.length(); // gas_used
        rlp_head.payload_length += self.timestamp.length(); // timestamp
        rlp_head.payload_length += self.extra_data.length(); // extra_data

        rlp_head.payload_length += KECCAK_LENGTH + 1; // mix_hash
        rlp_head.payload_length += 8 + 1; // nonce

        if let Some(base_fee_per_gas) = self.base_fee_per_gas {
            rlp_head.payload_length += base_fee_per_gas.length();
        }

        rlp_head
    }

    fn rlp_header_with_chain_id(&self) -> Header {
        let mut rlp_head = Header {
            list: true,
            payload_length: 0,
        };

        rlp_head.payload_length += 1; // chain_id
        rlp_head.payload_length += KECCAK_LENGTH + 1; // parent_hash
        rlp_head.payload_length += KECCAK_LENGTH + 1; // ommers_hash
        rlp_head.payload_length += ADDRESS_LENGTH + 1; // beneficiary
        rlp_head.payload_length += KECCAK_LENGTH + 1; // state_root
        rlp_head.payload_length += KECCAK_LENGTH + 1; // transactions_root
        rlp_head.payload_length += KECCAK_LENGTH + 1; // receipts_root
        rlp_head.payload_length += BLOOM_BYTE_LENGTH + length_of_length(BLOOM_BYTE_LENGTH); // logs_bloom
        rlp_head.payload_length += self.difficulty.length(); // difficulty
        rlp_head.payload_length += self.number.length(); // block height
        rlp_head.payload_length += self.gas_limit.length(); // gas_limit
        rlp_head.payload_length += self.gas_used.length(); // gas_used
        rlp_head.payload_length += self.timestamp.length(); // timestamp
        rlp_head.payload_length += self.extra_data.length(); // extra_data

        rlp_head.payload_length += KECCAK_LENGTH + 1; // mix_hash
        rlp_head.payload_length += 8 + 1; // nonce

        if let Some(base_fee_per_gas) = self.base_fee_per_gas {
            rlp_head.payload_length += base_fee_per_gas.length();
        }

        rlp_head
    }
}

impl Encodable for BlockHeader {
    fn encode(&self, out: &mut dyn BufMut) {
        self.rlp_header().encode(out);
        Encodable::encode(&self.parent_hash, out);
        Encodable::encode(&self.ommers_hash, out);
        Encodable::encode(&self.beneficiary, out);
        Encodable::encode(&self.state_root, out);
        Encodable::encode(&self.transactions_root, out);
        Encodable::encode(&self.receipts_root, out);
        Encodable::encode(&self.logs_bloom, out);
        Encodable::encode(&self.difficulty, out);
        Encodable::encode(&self.number, out);
        Encodable::encode(&self.gas_limit, out);
        Encodable::encode(&self.gas_used, out);
        Encodable::encode(&self.timestamp, out);
        Encodable::encode(&self.extra_data, out);
        Encodable::encode(&self.mix_hash, out);
        Encodable::encode(&self.nonce, out);
        if let Some(base_fee_per_gas) = self.base_fee_per_gas {
            Encodable::encode(&base_fee_per_gas, out);
        }
    }
    fn length(&self) -> usize {
        let rlp_head = self.rlp_header();
        length_of_length(rlp_head.payload_length) + rlp_head.payload_length
    }
}

impl Decodable for BlockHeader {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let rlp_head = Header::decode(buf)?;
        if !rlp_head.list {
            return Err(DecodeError::UnexpectedString);
        }
        let leftover = buf.len() - rlp_head.payload_length;
        let parent_hash = Decodable::decode(buf)?;
        let ommers_hash = Decodable::decode(buf)?;
        let beneficiary = Decodable::decode(buf)?;
        let state_root = Decodable::decode(buf)?;
        let transactions_root = Decodable::decode(buf)?;
        let receipts_root = Decodable::decode(buf)?;
        let logs_bloom = Decodable::decode(buf)?;
        let difficulty = Decodable::decode(buf)?;
        let number = Decodable::decode(buf)?;
        let gas_limit = Decodable::decode(buf)?;
        let gas_used = Decodable::decode(buf)?;
        let timestamp = Decodable::decode(buf)?;
        let extra_data = Decodable::decode(buf)?;
        let mix_hash = Decodable::decode(buf)?;
        let nonce = Decodable::decode(buf)?;
        let base_fee_per_gas = if buf.len() > leftover {
            Some(Decodable::decode(buf)?)
        } else {
            None
        };

        Ok(Self {
            parent_hash,
            ommers_hash,
            beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            logs_bloom,
            difficulty,
            number,
            gas_limit,
            gas_used,
            timestamp,
            extra_data,
            mix_hash,
            nonce,
            base_fee_per_gas,
        })
    }
}

impl BlockHeader {
    #[must_use]
    pub fn new(partial_header: PartialHeader, ommers_hash: H256, transactions_root: H256) -> Self {
        Self {
            parent_hash: partial_header.parent_hash,
            ommers_hash,
            beneficiary: partial_header.beneficiary,
            state_root: partial_header.state_root,
            transactions_root,
            receipts_root: partial_header.receipts_root,
            logs_bloom: partial_header.logs_bloom,
            difficulty: partial_header.difficulty,
            number: partial_header.number,
            gas_limit: partial_header.gas_limit,
            gas_used: partial_header.gas_used,
            timestamp: partial_header.timestamp,
            extra_data: partial_header.extra_data,
            mix_hash: partial_header.mix_hash,
            nonce: partial_header.nonce,
            base_fee_per_gas: partial_header.base_fee_per_gas,
        }
    }

    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self {
            parent_hash: H256::zero(),
            ommers_hash: H256::zero(),
            beneficiary: Address::zero(),
            state_root: H256::zero(),
            transactions_root: H256::zero(),
            receipts_root: H256::zero(),
            logs_bloom: Bloom::zero(),
            difficulty: U256::ZERO,
            number: BlockNumber(0),
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: Bytes::new(),
            mix_hash: H256::zero(),
            nonce: H64::zero(),
            base_fee_per_gas: None,
        }
    }

    #[must_use]
    pub fn hash(&self) -> H256 {
        let mut out = BytesMut::new();
        Encodable::encode(self, &mut out);
        keccak256(&out[..])
    }

    #[must_use]
    pub fn hash_with_chain_id(&self, chain_id: u64) -> H256 {
        let mut out = BytesMut::new();
        self.encode_with_chain_id(&mut out, chain_id);
        keccak256(&out[..])
    }

    fn encode_with_chain_id(&self, out: &mut dyn BufMut, chain_id :u64) {
        self.rlp_header_with_chain_id().encode(out);
        Encodable::encode(&chain_id, out);
        Encodable::encode(&self.parent_hash, out);
        Encodable::encode(&self.ommers_hash, out);
        Encodable::encode(&self.beneficiary, out);
        Encodable::encode(&self.state_root, out);
        Encodable::encode(&self.transactions_root, out);
        Encodable::encode(&self.receipts_root, out);
        Encodable::encode(&self.logs_bloom, out);
        Encodable::encode(&self.difficulty, out);
        Encodable::encode(&self.number, out);
        Encodable::encode(&self.gas_limit, out);
        Encodable::encode(&self.gas_used, out);
        Encodable::encode(&self.timestamp, out);
        Encodable::encode(&self.extra_data, out);
        Encodable::encode(&self.mix_hash, out);
        Encodable::encode(&self.nonce, out);
        if let Some(base_fee_per_gas) = self.base_fee_per_gas {
            Encodable::encode(&base_fee_per_gas, out);
        }
    }
    #[must_use]
    pub fn truncated_hash(&self) -> H256 {
        struct TruncatedHeader {
            parent_hash: H256,
            ommers_hash: H256,
            beneficiary: H160,
            state_root: H256,
            transactions_root: H256,
            receipts_root: H256,
            logs_bloom: Bloom,
            difficulty: U256,
            number: BlockNumber,
            gas_limit: u64,
            gas_used: u64,
            timestamp: u64,
            extra_data: Bytes,
            base_fee_per_gas: Option<U256>,
        }

        impl TruncatedHeader {
            fn rlp_header(&self) -> Header {
                let mut rlp_head = Header {
                    list: false,
                    payload_length: 0,
                };

                rlp_head.payload_length += KECCAK_LENGTH + 1; // parent_hash
                rlp_head.payload_length += KECCAK_LENGTH + 1; // ommers_hash
                rlp_head.payload_length += ADDRESS_LENGTH + 1; // beneficiary
                rlp_head.payload_length += KECCAK_LENGTH + 1; // state_root
                rlp_head.payload_length += KECCAK_LENGTH + 1; // transactions_root
                rlp_head.payload_length += KECCAK_LENGTH + 1; // receipts_root
                rlp_head.payload_length += BLOOM_BYTE_LENGTH + length_of_length(BLOOM_BYTE_LENGTH); // logs_bloom
                rlp_head.payload_length += self.difficulty.length(); // difficulty
                rlp_head.payload_length += self.number.length(); // block height
                rlp_head.payload_length += self.gas_limit.length(); // gas_limit
                rlp_head.payload_length += self.gas_used.length(); // gas_used
                rlp_head.payload_length += self.timestamp.length(); // timestamp
                rlp_head.payload_length += self.extra_data.length(); // extra_data

                if let Some(base_fee_per_gas) = self.base_fee_per_gas {
                    rlp_head.payload_length += base_fee_per_gas.length();
                }

                rlp_head
            }
        }

        impl Encodable for TruncatedHeader {
            fn encode(&self, out: &mut dyn BufMut) {
                self.rlp_header().encode(out);
                Encodable::encode(&self.parent_hash, out);
                Encodable::encode(&self.ommers_hash, out);
                Encodable::encode(&self.beneficiary, out);
                Encodable::encode(&self.state_root, out);
                Encodable::encode(&self.transactions_root, out);
                Encodable::encode(&self.receipts_root, out);
                Encodable::encode(&self.logs_bloom, out);
                Encodable::encode(&self.difficulty, out);
                Encodable::encode(&self.number, out);
                Encodable::encode(&self.gas_limit, out);
                Encodable::encode(&self.gas_used, out);
                Encodable::encode(&self.timestamp, out);
                Encodable::encode(&self.extra_data, out);
                if let Some(base_fee_per_gas) = self.base_fee_per_gas {
                    Encodable::encode(&base_fee_per_gas, out);
                }
            }
            fn length(&self) -> usize {
                let rlp_head = self.rlp_header();
                length_of_length(rlp_head.payload_length) + rlp_head.payload_length
            }
        }

        let mut buffer = BytesMut::new();

        TruncatedHeader {
            parent_hash: self.parent_hash,
            ommers_hash: self.ommers_hash,
            beneficiary: self.beneficiary,
            state_root: self.state_root,
            transactions_root: self.transactions_root,
            receipts_root: self.receipts_root,
            logs_bloom: self.logs_bloom,
            difficulty: self.difficulty,
            number: self.number,
            gas_limit: self.gas_limit,
            gas_used: self.gas_used,
            timestamp: self.timestamp,
            extra_data: self.extra_data.clone(),
            base_fee_per_gas: self.base_fee_per_gas,
        }
        .encode(&mut buffer);

        keccak256(&buffer[..])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Partial header definition without ommers hash and transactions root.
pub struct PartialHeader {
    pub parent_hash: H256,
    pub beneficiary: H160,
    pub state_root: H256,
    pub receipts_root: H256,
    pub logs_bloom: Bloom,
    pub difficulty: U256,
    pub number: BlockNumber,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes,
    pub mix_hash: H256,
    pub nonce: H64,
    pub base_fee_per_gas: Option<U256>,
}

impl From<BlockHeader> for PartialHeader {
    fn from(header: BlockHeader) -> Self {
        Self {
            parent_hash: header.parent_hash,
            beneficiary: header.beneficiary,
            state_root: header.state_root,
            receipts_root: header.receipts_root,
            logs_bloom: header.logs_bloom,
            difficulty: header.difficulty,
            number: header.number,
            gas_limit: header.gas_limit,
            gas_used: header.gas_used,
            timestamp: header.timestamp,
            extra_data: header.extra_data,
            mix_hash: header.mix_hash,
            nonce: header.nonce,
            base_fee_per_gas: header.base_fee_per_gas,
        }
    }
}

impl PartialHeader {
    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self {
            parent_hash: H256::zero(),
            beneficiary: Address::zero(),
            state_root: H256::zero(),
            receipts_root: H256::zero(),
            logs_bloom: Bloom::zero(),
            difficulty: U256::ZERO,
            number: BlockNumber(0),
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: Bytes::new(),
            mix_hash: H256::zero(),
            nonce: H64::zero(),
            base_fee_per_gas: None,
        }
    }
}
