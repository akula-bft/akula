use super::*;
use ethereum_types::Bloom;
use rlp::{Decodable, Encodable};

#[derive(Clone, Debug, PartialEq)]
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
}

impl Encodable for Receipt {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        let l = if self.tx_type > TxType::Legacy {
            let l = s.begin_list(5);
            l.append(&(self.tx_type as u8));
            l
        } else {
            s.begin_list(4)
        };
        l.append(&self.success);
        l.append(&self.cumulative_gas_used);
        l.append(&self.bloom);
        l.append_list(&self.logs);
    }
}

impl Decodable for Receipt {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        let num = rlp.item_count()?;
        let mut it = rlp.iter();

        let tx_type = match if num > 4 {
            it.next()
                .ok_or(rlp::DecoderError::RlpInvalidLength)?
                .as_val::<u8>()?
        } else {
            0
        } {
            0 => TxType::Legacy,
            1 => TxType::EIP2930,
            2 => TxType::EIP1559,
            _ => return Err(rlp::DecoderError::Custom("invalid tx type")),
        };

        let success = it
            .next()
            .ok_or(rlp::DecoderError::RlpInvalidLength)?
            .as_val()?;
        let cumulative_gas_used = it
            .next()
            .ok_or(rlp::DecoderError::RlpInvalidLength)?
            .as_val()?;
        let bloom = it
            .next()
            .ok_or(rlp::DecoderError::RlpInvalidLength)?
            .as_val()?;
        let logs = it
            .next()
            .ok_or(rlp::DecoderError::RlpInvalidLength)?
            .as_list()?;

        Ok(Self {
            tx_type,
            success,
            cumulative_gas_used,
            bloom,
            logs,
        })
    }
}
