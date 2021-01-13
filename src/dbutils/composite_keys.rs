use super::*;
use crate::common;
use ethereum_types::H256;
use std::mem::size_of;

const NUMBER_HASH_COMPOSITE_LEN: usize = common::BLOCK_NUMBER_LENGTH + common::HASH_LENGTH;

pub type NumberHashCompositeKey = [u8; NUMBER_HASH_COMPOSITE_LEN];
pub type HeaderHashKey = [u8; size_of::<u64>() + 1];
pub type HeaderTDKey = [u8; NUMBER_HASH_COMPOSITE_LEN + 1];

#[allow(non_upper_case_globals)]
pub const encode_block_number: fn(u64) -> [u8; 8] = u64::to_be_bytes;

pub fn number_hash_composite_key(number: u64, hash: H256) -> NumberHashCompositeKey {
    let mut v: NumberHashCompositeKey = [0; NUMBER_HASH_COMPOSITE_LEN];

    v[..common::BLOCK_NUMBER_LENGTH].copy_from_slice(&encode_block_number(number));
    v[common::BLOCK_NUMBER_LENGTH..].copy_from_slice(&hash.to_fixed_bytes());

    v
}

pub fn header_hash_key(block: u64) -> HeaderHashKey {
    let mut v: HeaderHashKey = Default::default();

    v[..common::BLOCK_NUMBER_LENGTH].copy_from_slice(&encode_block_number(block));
    v[common::BLOCK_NUMBER_LENGTH..].copy_from_slice(HEADER_HASH_SUFFIX.as_bytes());

    v
}

pub fn header_td_key(number: u64, hash: H256) -> HeaderTDKey {
    let mut v: HeaderTDKey = [0; NUMBER_HASH_COMPOSITE_LEN + 1];

    v[..NUMBER_HASH_COMPOSITE_LEN].copy_from_slice(&number_hash_composite_key(number, hash));
    v[NUMBER_HASH_COMPOSITE_LEN..].copy_from_slice(HEADER_TD_SUFFIX.as_bytes());

    v
}
