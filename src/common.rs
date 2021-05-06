use ethereum_types::{H256, U256};
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
