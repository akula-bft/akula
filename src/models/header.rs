use super::{util::*, *};
use crate::crypto::*;
use anyhow::{bail, format_err};
use bytes::{Buf, Bytes, BytesMut};
use fastrlp::*;
use modular_bitfield::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
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

#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
struct HeaderFlags {
    ommers_hash: bool,
    transactions_root: bool,
    receipts_root: bool,
    logs_bloom: bool,
    mix_hash: bool,
    nonce: bool,

    difficulty_len: B5,
    block_number_len: B3,
    gas_limit_len: B3,
    gas_used_len: B3,
    timestamp_len: B3,
    base_fee_per_gas_len: B5,

    #[skip]
    unused: B4,
}

impl BlockHeader {
    pub fn compact_encode(&self) -> Vec<u8> {
        let mut buffer = vec![];

        let mut flags = HeaderFlags::default();
        if self.ommers_hash != EMPTY_LIST_HASH {
            flags.set_ommers_hash(true);
        }
        if self.transactions_root != EMPTY_ROOT {
            flags.set_transactions_root(true);
        }
        if self.receipts_root != EMPTY_ROOT {
            flags.set_receipts_root(true);
        }
        if !self.logs_bloom.is_zero() {
            flags.set_logs_bloom(true);
        }

        let difficulty_encoded = variable_to_compact(self.difficulty);
        flags.set_difficulty_len(difficulty_encoded.len() as u8);

        let block_number_encoded = variable_to_compact(self.number.0);
        flags.set_block_number_len(block_number_encoded.len() as u8);

        let gas_limit_encoded = variable_to_compact(self.gas_limit);
        flags.set_gas_limit_len(gas_limit_encoded.len() as u8);

        let gas_used_encoded = variable_to_compact(self.gas_used);
        flags.set_gas_used_len(gas_used_encoded.len() as u8);

        let timestamp_encoded = variable_to_compact(self.timestamp);
        flags.set_timestamp_len(timestamp_encoded.len() as u8);

        let base_fee_per_gas_encoded =
            variable_to_compact(self.base_fee_per_gas.unwrap_or(U256::ZERO));
        flags.set_base_fee_per_gas_len(base_fee_per_gas_encoded.len() as u8);

        if !self.mix_hash.is_zero() {
            flags.set_mix_hash(true);
        }
        if !self.nonce.is_zero() {
            flags.set_nonce(true);
        }

        let fs = flags.into_bytes();
        buffer.extend_from_slice(&fs[..]);

        buffer.extend_from_slice(&self.parent_hash[..]);
        if flags.ommers_hash() {
            buffer.extend_from_slice(&self.ommers_hash[..]);
        }
        buffer.extend_from_slice(&self.beneficiary[..]);
        buffer.extend_from_slice(&self.state_root[..]);
        if flags.transactions_root() {
            buffer.extend_from_slice(&self.transactions_root[..]);
        }
        if flags.receipts_root() {
            buffer.extend_from_slice(&self.receipts_root[..]);
        }
        if flags.logs_bloom() {
            buffer.extend_from_slice(&self.logs_bloom[..]);
        }

        buffer.extend_from_slice(&difficulty_encoded[..]);
        buffer.extend_from_slice(&block_number_encoded[..]);
        buffer.extend_from_slice(&gas_limit_encoded[..]);
        buffer.extend_from_slice(&gas_used_encoded[..]);
        buffer.extend_from_slice(&timestamp_encoded[..]);
        buffer.extend_from_slice(&base_fee_per_gas_encoded[..]);

        if flags.mix_hash() {
            buffer.extend_from_slice(&self.mix_hash[..]);
        }

        if flags.nonce() {
            buffer.extend_from_slice(&self.nonce[..]);
        }

        buffer.extend_from_slice(&self.extra_data);

        buffer
    }

    pub fn compact_decode(mut buf: &[u8]) -> anyhow::Result<Self> {
        if buf.len() < 4 {
            bail!("input too short");
        }

        let flags =
            HeaderFlags::from_bytes([buf.get_u8(), buf.get_u8(), buf.get_u8(), buf.get_u8()]);

        let parent_hash;
        (parent_hash, buf) = h256_from_compact(buf)?;

        let mut ommers_hash = EMPTY_LIST_HASH;
        if flags.ommers_hash() {
            (ommers_hash, buf) = h256_from_compact(buf)?;
        }

        let beneficiary;
        (beneficiary, buf) = h160_from_compact(buf)?;

        let state_root;
        (state_root, buf) = h256_from_compact(buf)?;

        let mut transactions_root = EMPTY_ROOT;
        if flags.transactions_root() {
            (transactions_root, buf) = h256_from_compact(buf)?;
        }

        let mut receipts_root = EMPTY_ROOT;
        if flags.receipts_root() {
            (receipts_root, buf) = h256_from_compact(buf)?;
        }

        let mut logs_bloom = Bloom::zero();
        if flags.logs_bloom() {
            fn bloom_from_compact(mut buf: &[u8]) -> anyhow::Result<(Bloom, &[u8])> {
                let v = Bloom::from_slice(
                    buf.get(..256)
                        .ok_or_else(|| format_err!("input too short"))?,
                );
                buf.advance(256);
                Ok((v, buf))
            }

            (logs_bloom, buf) = bloom_from_compact(buf)?;
        }

        let difficulty;
        (difficulty, buf) = variable_from_compact(buf, flags.difficulty_len())?;

        let number;
        (number, buf) = variable_from_compact(buf, flags.block_number_len())?;
        let number = BlockNumber(number);

        let gas_limit;
        (gas_limit, buf) = variable_from_compact(buf, flags.gas_limit_len())?;

        let gas_used;
        (gas_used, buf) = variable_from_compact(buf, flags.gas_used_len())?;

        let timestamp;
        (timestamp, buf) = variable_from_compact(buf, flags.timestamp_len())?;

        let base_fee_per_gas: U256;
        (base_fee_per_gas, buf) = variable_from_compact(buf, flags.base_fee_per_gas_len())?;
        let base_fee_per_gas = if base_fee_per_gas == 0 {
            None
        } else {
            Some(base_fee_per_gas)
        };

        let mut mix_hash = H256::zero();
        if flags.mix_hash() {
            (mix_hash, buf) = h256_from_compact(buf)?;
        }

        let mut nonce = H64::zero();
        if flags.nonce() {
            fn h64_from_compact(mut buf: &[u8]) -> anyhow::Result<(H64, &[u8])> {
                let v =
                    H64::from_slice(buf.get(..8).ok_or_else(|| format_err!("input too short"))?);
                buf.advance(8);
                Ok((v, buf))
            }

            (nonce, buf) = h64_from_compact(buf)?;
        }

        let extra_data = buf[..].to_vec().into();

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
