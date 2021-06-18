use ethereum::Header;
use rlp_derive::{RlpDecodable, RlpEncodable};

#[derive(RlpDecodable, RlpEncodable, Debug, PartialEq)]
pub struct BodyForStorage {
    pub base_tx_id: u64,
    pub tx_amount: u32,
    pub uncles: Vec<Header>,
}
