use super::*;
use bytes::Bytes;
use fastrlp::*;
use parity_scale_codec::*;
use serde::*;

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, RlpEncodable, RlpDecodable,
)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<H256>,
    pub data: Bytes,
}
