use super::*;
use crate::trie::*;
use bytes::{Buf, BufMut};
use fastrlp::*;
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

    fn rlp_header(&self) -> fastrlp::Header {
        let mut h = fastrlp::Header {
            list: true,
            payload_length: 0,
        };

        h.payload_length += Encodable::length(&self.success);
        h.payload_length += Encodable::length(&self.cumulative_gas_used);
        h.payload_length += Encodable::length(&self.bloom);
        h.payload_length += Encodable::length(&self.logs);

        h
    }

    fn encode_inner(&self, out: &mut dyn BufMut, rlp_head: Header) {
        if !matches!(self.tx_type, TxType::Legacy) {
            out.put_u8(self.tx_type as u8);
        }

        rlp_head.encode(out);
        Encodable::encode(&self.success, out);
        Encodable::encode(&self.cumulative_gas_used, out);
        Encodable::encode(&self.bloom, out);
        Encodable::encode(&self.logs, out);
    }
}

impl Encodable for Receipt {
    fn length(&self) -> usize {
        let rlp_head = self.rlp_header();
        let rlp_len = length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
        if matches!(self.tx_type, TxType::Legacy) {
            rlp_len
        } else {
            // EIP-2718 objects are wrapped into byte array in containing RLP
            length_of_length(rlp_len + 1) + rlp_len + 1
        }
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let rlp_head = self.rlp_header();

        if !matches!(self.tx_type, TxType::Legacy) {
            let rlp_len = length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
            Header {
                list: false,
                payload_length: rlp_len + 1,
            }
            .encode(out);

            out.put_u8(self.tx_type as u8);
        }

        self.encode_inner(out, rlp_head);
    }
}

impl TrieEncode for Receipt {
    fn trie_encode(&self, buf: &mut dyn BufMut) {
        self.encode_inner(buf, self.rlp_header())
    }
}

impl Decodable for Receipt {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        fn base_decode(buf: &mut &[u8], tx_type: TxType) -> Result<Receipt, DecodeError> {
            let receipt = Receipt {
                tx_type,
                success: Decodable::decode(buf)?,
                cumulative_gas_used: Decodable::decode(buf)?,
                bloom: Decodable::decode(buf)?,
                logs: Decodable::decode(buf)?,
            };

            Ok(receipt)
        }

        fn eip2718_decode(buf: &mut &[u8], tx_type: TxType) -> Result<Receipt, DecodeError> {
            let h = Header::decode(buf)?;

            if !h.list {
                return Err(DecodeError::UnexpectedString);
            }

            base_decode(buf, tx_type)
        }

        let rlp_head = Header::decode(buf)?;

        Ok(if rlp_head.list {
            let started_len = buf.len();
            let this = base_decode(buf, TxType::Legacy)?;

            let consumed = started_len - buf.len();
            if consumed != rlp_head.payload_length {
                return Err(fastrlp::DecodeError::ListLengthMismatch {
                    expected: rlp_head.payload_length,
                    got: consumed,
                });
            }

            this
        } else if rlp_head.payload_length == 0 {
            return Err(DecodeError::InputTooShort);
        } else {
            if buf.is_empty() {
                return Err(DecodeError::InputTooShort);
            }

            let tx_type = TxType::try_from(buf.get_u8())?;

            if tx_type != TxType::EIP2930 && tx_type != TxType::EIP1559 {
                return Err(DecodeError::Custom("Unsupported transaction type"));
            }

            let this = eip2718_decode(buf, tx_type)?;

            if !buf.is_empty() {
                return Err(DecodeError::ListLengthMismatch {
                    expected: 0,
                    got: buf.len(),
                });
            }

            this
        })
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
