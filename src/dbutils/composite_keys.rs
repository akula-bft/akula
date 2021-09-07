#![allow(dead_code)]
use crate::models::*;
use arrayref::array_ref;
use ethereum_types::H256;
use std::io::Write;

pub const HEADER_KEY_LEN: usize = BLOCK_NUMBER_LENGTH + KECCAK_LENGTH;

pub const COMPOSITE_STORAGE_KEY_LENGTH: usize = STORAGE_PREFIX_LENGTH + KECCAK_LENGTH;
pub const PLAIN_COMPOSITE_STORAGE_KEY_LENGTH: usize =
    ADDRESS_LENGTH + INCARNATION_LENGTH + KECCAK_LENGTH;
pub const STORAGE_PREFIX_LENGTH: usize = KECCAK_LENGTH + INCARNATION_LENGTH;
pub const PLAIN_STORAGE_PREFIX_LENGTH: usize = ADDRESS_LENGTH + INCARNATION_LENGTH;

pub type HeaderKey = [u8; HEADER_KEY_LEN];

pub type CompositeStorageKey = [u8; COMPOSITE_STORAGE_KEY_LENGTH];
pub type PlainCompositeStorageKey = [u8; PLAIN_COMPOSITE_STORAGE_KEY_LENGTH];
pub type StoragePrefix = [u8; STORAGE_PREFIX_LENGTH];
pub type PlainStoragePrefix = [u8; PLAIN_STORAGE_PREFIX_LENGTH];

#[allow(non_upper_case_globals)]
pub const encode_block_number: fn(u64) -> [u8; 8] = u64::to_be_bytes;

pub fn header_key(number: u64, hash: H256) -> HeaderKey {
    let mut v: HeaderKey = [0; HEADER_KEY_LEN];

    v[..BLOCK_NUMBER_LENGTH].copy_from_slice(&encode_block_number(number));
    v[BLOCK_NUMBER_LENGTH..].copy_from_slice(&hash.to_fixed_bytes());

    v
}

/// AddrHash + incarnation + KeyHash
/// For contract storage
pub fn generate_composite_storage_key(
    address_hash: H256,
    incarnation: Incarnation,
    seckey: H256,
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
    address: Address,
    incarnation: Incarnation,
    key: H256,
) -> PlainCompositeStorageKey {
    let mut composite_key = [0; PLAIN_COMPOSITE_STORAGE_KEY_LENGTH];
    let mut w = composite_key.as_mut();
    w.write_all(address.as_bytes()).unwrap();
    w.write_all(&incarnation.to_be_bytes()).unwrap();
    w.write_all(key.as_bytes()).unwrap();

    composite_key
}

pub fn plain_parse_composite_storage_key(
    composite_key: &PlainCompositeStorageKey,
) -> (Address, Incarnation, H256) {
    const PREFIX_LEN: usize = ADDRESS_LENGTH + INCARNATION_LENGTH;

    let (addr, inc) = plain_parse_storage_prefix(&composite_key[..PREFIX_LEN]);
    (
        addr,
        inc,
        H256::from_slice(&composite_key[PREFIX_LEN..PREFIX_LEN + KECCAK_LENGTH]),
    )
}

/// address hash + incarnation prefix
pub fn generate_storage_prefix(address_hash: H256, incarnation: Incarnation) -> StoragePrefix {
    let mut prefix = [0; STORAGE_PREFIX_LENGTH];
    prefix[..KECCAK_LENGTH].copy_from_slice(address_hash.as_bytes());
    prefix[KECCAK_LENGTH..].copy_from_slice(&incarnation.to_be_bytes());
    prefix
}

/// address hash + incarnation prefix (for plain state)
pub fn plain_generate_storage_prefix(
    address: Address,
    incarnation: Incarnation,
) -> PlainStoragePrefix {
    let mut prefix = PlainStoragePrefix::default();
    prefix[..ADDRESS_LENGTH].copy_from_slice(address.as_bytes());
    prefix[ADDRESS_LENGTH..].copy_from_slice(&incarnation.to_be_bytes());
    prefix
}

pub fn plain_parse_storage_prefix(prefix: &[u8]) -> (Address, u64) {
    (
        Address::from_slice(&prefix[..ADDRESS_LENGTH]),
        u64::from_be_bytes(*array_ref!(
            prefix[ADDRESS_LENGTH..ADDRESS_LENGTH + INCARNATION_LENGTH],
            0,
            8
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn plain_storage_prefix() {
        let expected_addr = hex!("5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").into();
        let expected_incarnation = 999_000_999_u64;

        let prefix = plain_generate_storage_prefix(expected_addr, expected_incarnation);

        let (addr, incarnation) = plain_parse_storage_prefix(&prefix);

        assert_eq!(expected_addr, addr, "address should be extracted");
        assert_eq!(
            expected_incarnation, incarnation,
            "incarnation should be extracted"
        );
    }

    #[test]
    fn plain_composite_storage_key() {
        let expected_addr = hex!("5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").into();
        let expected_incarnation = 999_000_999_u64;
        let expected_key =
            hex!("58833f949125129fb8c6c93d2c6003c5bab7c0b116d695f4ca137b1debf4e472").into();

        let composite_key =
            plain_generate_composite_storage_key(expected_addr, expected_incarnation, expected_key);

        let (addr, incarnation, key) = plain_parse_composite_storage_key(&composite_key);

        assert_eq!(expected_addr, addr, "address should be extracted");
        assert_eq!(
            expected_incarnation, incarnation,
            "incarnation should be extracted"
        );
        assert_eq!(expected_key, key, "key should be extracted");
    }
}
