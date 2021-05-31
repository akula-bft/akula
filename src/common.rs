use ethereum_types::{H256, U256};
use hex_literal::hex;
use sha3::{Digest, Keccak256};
use std::mem::size_of;

pub use ethereum_types::Address;
pub type Hash = H256;
pub type Incarnation = u64;
pub type Value = U256;

pub const HASH_LENGTH: usize = Hash::len_bytes();
pub const ADDRESS_LENGTH: usize = Address::len_bytes();
pub const BLOCK_NUMBER_LENGTH: usize = size_of::<u64>();
pub const INCARNATION_LENGTH: usize = size_of::<u64>();

pub fn hash_data(data: &[u8]) -> Hash {
    Hash::from_slice(&Keccak256::digest(data)[..])
}

pub const EMPTY_ROOT: H256 = H256(hex!(
    "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
));
