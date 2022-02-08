use super::*;
use crate::crypto::*;
use bytes::{BufMut, Bytes, BytesMut};
use rlp::{DecoderError, Encodable, RlpStream};
use rlp_derive::RlpDecodable;
use serde::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub tx_type: TxType,
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub bloom: Bloom,
    pub logs: Vec<Log>,
}

impl Receipt {
    pub fn new(tx_type: TxType, success: bool, cumulative_gas_used: u64, logs: Vec<Log>) -> Self {
        let bloom = logs_bloom(&logs);
        Self {
            tx_type,
            success,
            cumulative_gas_used,
            bloom,
            logs,
        }
    }

    fn encode_inner(&self, s: &mut RlpStream, standalone: bool) {
        match self.tx_type {
            TxType::Legacy => {
                let l = s.begin_list(4);
                l.append(&self.success);
                l.append(&self.cumulative_gas_used);
                l.append(&self.bloom);
                l.append_list(&self.logs);
            }
            TxType::EIP2930 | TxType::EIP1559 => {
                let mut b = BytesMut::with_capacity(1);
                b.put_u8(self.tx_type as u8);
                let mut l = RlpStream::new_list_with_buffer(b, 4);
                l.append(&self.success);
                l.append(&self.cumulative_gas_used);
                l.append(&self.bloom);
                l.append_list(&self.logs);
                if standalone {
                    s.append_raw(&*l.out().freeze(), 1);
                } else {
                    s.append(&l.out());
                }
            }
        }
    }

    fn decode_inner(rlp: &rlp::Rlp, is_legacy: bool) -> Result<Self, DecoderError> {
        Ok(match is_legacy {
            true => {
                let inner = UntypedReceipt::decode(rlp)?;
                inner.into_receipt(TxType::Legacy)
            }
            false => {
                let tx_type = u8::decode(rlp)?;
                let inner = UntypedReceipt::decode(rlp)?;
                inner.into_receipt(tx_type.try_into()?)
            }
        })
    }
}

impl TrieEncode for Receipt {
    fn trie_encode(&self) -> Bytes {
        let mut s = RlpStream::new();
        self.encode_inner(&mut s, true);
        s.out().freeze()
    }
}

impl Encodable for Receipt {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        self.encode_inner(s, false);
    }
}

impl Decodable for Receipt {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        // FIXME: This check may be incorrect
        let is_legacy = !rlp.is_data();
        Self::decode_inner(rlp, is_legacy)
    }
}

#[derive(RlpDecodable)]
struct UntypedReceipt {
    pub success: bool,
    pub cumulative_gas_used: u64,
    pub bloom: Bloom,
    pub logs: Vec<Log>,
}

impl UntypedReceipt {
    fn into_receipt(self, tx_type: TxType) -> Receipt {
        Receipt {
            tx_type,
            success: self.success,
            cumulative_gas_used: self.cumulative_gas_used,
            bloom: self.bloom,
            logs: self.logs,
        }
    }
}
