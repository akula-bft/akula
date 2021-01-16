use super::*;
use crate::common;
use ethereum_types::H256;
use std::{io::Write, mem::size_of};

const NUMBER_HASH_COMPOSITE_LEN: usize = common::BLOCK_NUMBER_LENGTH + common::HASH_LENGTH;

const COMPOSITE_STORAGE_KEY_LENGTH: usize = STORAGE_PREFIX_LENGTH + common::HASH_LENGTH;
const PLAIN_COMPOSITE_STORAGE_KEY_LENGTH: usize =
    common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH;
const STORAGE_PREFIX_LENGTH: usize = common::HASH_LENGTH + common::INCARNATION_LENGTH;

pub type NumberHashCompositeKey = [u8; NUMBER_HASH_COMPOSITE_LEN];
pub type HeaderHashKey = [u8; size_of::<u64>() + 1];
pub type HeaderTDKey = [u8; NUMBER_HASH_COMPOSITE_LEN + 1];

pub type CompositeStorageKey = [u8; COMPOSITE_STORAGE_KEY_LENGTH];
pub type PlainCompositeStorageKey = [u8; PLAIN_COMPOSITE_STORAGE_KEY_LENGTH];
pub type StoragePrefix = [u8; STORAGE_PREFIX_LENGTH];

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

/// AddrHash + incarnation + KeyHash
/// For contract storage
pub fn generate_composite_storage_key(
    address_hash: common::Hash,
    incarnation: common::Incarnation,
    seckey: common::Hash,
) -> CompositeStorageKey {
    let mut composite_key = [0; COMPOSITE_STORAGE_KEY_LENGTH];
    composite_key[..STORAGE_PREFIX_LENGTH]
        .copy_from_slice(&generate_storage_prefix(address_hash, incarnation));
    composite_key[STORAGE_PREFIX_LENGTH..].copy_from_slice(seckey.as_bytes());
    composite_key
}

/// AddrHash + incarnation + KeyHash
/// For contract storage (for plain state)
pub fn plain_generate_composite_storage_key(
    address: common::Address,
    incarnation: common::Incarnation,
    key: common::Hash,
) -> PlainCompositeStorageKey {
    let mut composite_key = [0; PLAIN_COMPOSITE_STORAGE_KEY_LENGTH];
    let mut w = composite_key.as_mut();
    w.write_all(address.as_bytes()).unwrap();
    w.write_all(&incarnation.to_be_bytes()).unwrap();
    w.write_all(key.as_bytes()).unwrap();

    composite_key
}

/// address hash + incarnation prefix
pub fn generate_storage_prefix(
    address_hash: common::Hash,
    incarnation: common::Incarnation,
) -> StoragePrefix {
    let mut prefix = [0; STORAGE_PREFIX_LENGTH];
    prefix[..common::HASH_LENGTH].copy_from_slice(address_hash.as_bytes());
    prefix[common::HASH_LENGTH..].copy_from_slice(&incarnation.to_be_bytes());
    prefix
}
