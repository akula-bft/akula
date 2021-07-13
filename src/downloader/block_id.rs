use ethereum_types::H256;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum BlockId {
    Hash(H256),
    Number(u64),
}

impl rlp::Decodable for BlockId {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        if rlp.size() == 32 {
            Ok(Self::Hash(rlp.as_val()?))
        } else {
            Ok(Self::Number(rlp.as_val()?))
        }
    }
}

impl rlp::Encodable for BlockId {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        match self {
            Self::Hash(v) => rlp::Encodable::rlp_append(v, s),
            Self::Number(v) => rlp::Encodable::rlp_append(v, s),
        }
    }
}
