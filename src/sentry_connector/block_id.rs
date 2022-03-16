use crate::models::*;
use fastrlp::*;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum BlockId {
    Hash(H256),
    Number(BlockNumber),
}

impl Decodable for BlockId {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let header = Header::decode(&mut &**buf)?;

        if header.payload_length == 32 {
            H256::decode(buf).map(Self::Hash)
        } else {
            BlockNumber::decode(buf).map(Self::Number)
        }
    }
}

impl Encodable for BlockId {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Hash(hash) => hash.encode(out),
            Self::Number(number) => number.encode(out),
        }
    }

    fn length(&self) -> usize {
        match self {
            BlockId::Hash(hash) => hash.length(),
            BlockId::Number(num) => num.length(),
        }
    }
}
