use super::*;
use bytes::Bytes;
use fastrlp::*;
use serde::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<H256>,
    pub data: Bytes,
}
