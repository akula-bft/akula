use super::*;
use crate::common;
use common::HASH_LENGTH;
use ethereum_types::H256;
use futures::io::copy;
use std::{io::Write, mem::size_of};

pub const NUMBER_HASH_COMPOSITE_LEN: usize = common::BLOCK_NUMBER_LENGTH + common::HASH_LENGTH;

pub const COMPOSITE_STORAGE_KEY_LENGTH: usize = STORAGE_PREFIX_LENGTH + common::HASH_LENGTH;
pub const PLAIN_COMPOSITE_STORAGE_KEY_LENGTH: usize =
    common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH;
pub const STORAGE_PREFIX_LENGTH: usize = common::HASH_LENGTH + common::INCARNATION_LENGTH;
pub const PLAIN_STORAGE_PREFIX_LENGTH: usize = common::ADDRESS_LENGTH + common::INCARNATION_LENGTH;

pub type NumberHashCompositeKey = [u8; NUMBER_HASH_COMPOSITE_LEN];
pub type HeaderHashKey = [u8; size_of::<u64>() + 1];
pub type HeaderTDKey = [u8; NUMBER_HASH_COMPOSITE_LEN + 1];

pub type CompositeStorageKey = [u8; COMPOSITE_STORAGE_KEY_LENGTH];
pub type PlainCompositeStorageKey = [u8; PLAIN_COMPOSITE_STORAGE_KEY_LENGTH];
pub type StoragePrefix = [u8; STORAGE_PREFIX_LENGTH];
pub type PlainStoragePrefix = [u8; PLAIN_STORAGE_PREFIX_LENGTH];

#[allow(non_upper_case_globals)]
pub const encode_block_number: fn(u64) -> [u8; 8] = u64::to_be_bytes;

pub fn number_hash_composite_key(number: u64, hash: H256) -> NumberHashCompositeKey {
    let mut v: NumberHashCompositeKey = [0; NUMBER_HASH_COMPOSITE_LEN];

    v[..common::BLOCK_NUMBER_LENGTH].copy_from_slice(&encode_block_number(number));
    v[common::BLOCK_NUMBER_LENGTH..].copy_from_slice(&hash.to_fixed_bytes());

    v
}

pub fn is_header_key(k: &[u8]) -> bool {
    let l = common::BLOCK_NUMBER_LENGTH + common::HASH_LENGTH;

    if k.len() != l {
        return false;
    }

    !is_header_hash_key(k) && !is_header_td_key(k)
}

pub fn header_td_key(number: u64, hash: H256) -> HeaderTDKey {
    let mut v: HeaderTDKey = [0; NUMBER_HASH_COMPOSITE_LEN + 1];

    v[..NUMBER_HASH_COMPOSITE_LEN].copy_from_slice(&number_hash_composite_key(number, hash));
    v[NUMBER_HASH_COMPOSITE_LEN..].copy_from_slice(HEADER_TD_SUFFIX.as_bytes());

    v
}

pub fn is_header_td_key(k: &[u8]) -> bool {
    let l = common::BLOCK_NUMBER_LENGTH + common::HASH_LENGTH + 1;
    k.len() == l && &k[l - 1..] == HEADER_TD_SUFFIX.as_bytes()
}

pub fn header_hash_key(block: u64) -> HeaderHashKey {
    let mut v: HeaderHashKey = Default::default();

    v[..common::BLOCK_NUMBER_LENGTH].copy_from_slice(&encode_block_number(block));
    v[common::BLOCK_NUMBER_LENGTH..].copy_from_slice(HEADER_HASH_SUFFIX.as_bytes());

    v
}

pub fn is_header_hash_key(k: &[u8]) -> bool {
    let l = common::BLOCK_NUMBER_LENGTH + 1;
    k.len() == l && &k[l - 1..] == HEADER_HASH_SUFFIX.as_bytes()
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

/// address hash + incarnation prefix (for plain state)
pub fn plain_generate_storage_prefix(
    address: &[u8],
    incarnation: common::Incarnation,
) -> PlainStoragePrefix {
    let mut prefix = PlainStoragePrefix::default();
    prefix[..common::ADDRESS_LENGTH].copy_from_slice(address);
    prefix[common::ADDRESS_LENGTH..].copy_from_slice(&incarnation.to_be_bytes());
    prefix
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn header_type_detection() {
        // good input
        let header_hash_key = hex!("00000000000000006e");
        assert!(!is_header_key(&header_hash_key));
        assert!(!is_header_td_key(&header_hash_key));
        assert!(is_header_hash_key(&header_hash_key));

        let header_key = hex!(
            "0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd"
        );
        assert!(is_header_key(&header_key));
        assert!(!is_header_td_key(&header_key));
        assert!(!is_header_hash_key(&header_key));

        let header_td_key = hex!(
            "0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd74"
        );
        assert!(!is_header_key(&header_td_key));
        assert!(is_header_td_key(&header_td_key));
        assert!(!is_header_hash_key(&header_td_key));

        // bad input
        let empty_key = hex!("");
        assert!(!is_header_key(&empty_key));
        assert!(!is_header_td_key(&empty_key));
        assert!(!is_header_hash_key(&empty_key));

        let too_long_key = hex!("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd");
        assert!(!is_header_key(&too_long_key));
        assert!(!is_header_td_key(&too_long_key));
        assert!(!is_header_hash_key(&too_long_key));
    }
}
