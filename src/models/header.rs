use bytes::Bytes;
use ethereum_types::*;
use rlp::*;
use sha3::*;

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub number: u64,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes<'static>,
    pub mix_hash: H256,
    pub nonce: H64,
    pub base_fee_per_gas: Option<U256>,
}

impl Encodable for BlockHeader {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list({
            if self.base_fee_per_gas.is_some() {
                16
            } else {
                15
            }
        });
        s.append(&self.parent_hash);
        s.append(&self.ommers_hash);
        s.append(&self.beneficiary);
        s.append(&self.state_root);
        s.append(&self.transactions_root);
        s.append(&self.receipts_root);
        s.append(&self.logs_bloom);
        s.append(&self.difficulty);
        s.append(&self.number);
        s.append(&self.gas_limit);
        s.append(&self.gas_used);
        s.append(&self.timestamp);
        s.append(&self.extra_data.as_ref());
        s.append(&self.mix_hash);
        s.append(&self.nonce);
        if let Some(base_fee_per_gas) = self.base_fee_per_gas {
            s.append(&base_fee_per_gas);
        }
    }
}

impl Decodable for BlockHeader {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        let mut rlp = rlp.iter();
        let parent_hash = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let ommers_hash = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let beneficiary = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let state_root = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let transactions_root = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let receipts_root = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let logs_bloom = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let difficulty = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let number = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let gas_limit = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let gas_used = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let timestamp = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let extra_data = rlp
            .next()
            .ok_or(DecoderError::RlpInvalidLength)?
            .as_val::<Vec<u8>>()?
            .into();
        let mix_hash = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let nonce = rlp.next().ok_or(DecoderError::RlpInvalidLength)?.as_val()?;
        let base_fee_per_gas = rlp.next().map(|rlp| rlp.as_val()).transpose()?;

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
            difficulty: U256::zero(),
            number: 0,
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
        H256::from_slice(Keccak256::digest(&rlp::encode(self)).as_slice())
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
    pub number: u64,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes<'static>,
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
