use crate::{
    models::{BlockNumber, H256},
    p2p::types::{BlockId, Message},
};
use fastrlp::*;

impl Decodable for BlockId {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        if buf.len() == 32 {
            Ok(BlockId::Hash(<H256 as Decodable>::decode(buf)?))
        } else {
            debug_assert!(buf.len() <= 8);
            Ok(BlockId::Number(<BlockNumber as Decodable>::decode(buf)?))
        }
    }
}

impl Encodable for BlockId {
    fn encode(&self, out: &mut dyn BufMut) {
        match *self {
            BlockId::Hash(ref hash) => hash.encode(out),
            BlockId::Number(ref number) => number.encode(out),
        }
    }
}

impl Encodable for Message {
    fn encode(&self, out: &mut dyn BufMut) {
        match *self {
            Message::NewBlockHashes(ref value) => value.encode(out),
            Message::GetBlockHeaders(ref value) => value.encode(out),
            Message::GetBlockBodies(ref value) => value.encode(out),
            Message::BlockBodies(ref value) => value.encode(out),
            Message::BlockHeaders(ref value) => value.encode(out),
            Message::NewBlock(ref value) => value.encode(out),
            Message::NewPooledTransactionHashes(ref value) => value.encode(out),
        }
    }
}
