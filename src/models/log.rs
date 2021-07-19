use bytes::Bytes;
use ethereum_types::*;
use rlp::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<H256>,
    pub data: Bytes<'static>,
}

impl Encodable for Log {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(3);
        s.append(&self.address);
        s.append_list(&self.topics);
        s.append(&self.data.as_ref());
    }
}

impl Decodable for Log {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        let mut rlp = rlp.iter();
        let address = rlp
            .next()
            .ok_or(DecoderError::RlpIncorrectListLen)?
            .as_val()?;
        let topics = rlp
            .next()
            .ok_or(DecoderError::RlpIncorrectListLen)?
            .as_list()?;
        let data = rlp
            .next()
            .ok_or(DecoderError::RlpIncorrectListLen)?
            .as_val::<Vec<u8>>()?
            .into();

        Ok(Self {
            address,
            topics,
            data,
        })
    }
}
