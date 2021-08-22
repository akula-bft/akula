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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockNumber(pub u64);

impl BlockNumber {
    pub fn db_key(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

pub fn hash_data(data: impl AsRef<[u8]>) -> Hash {
    Hash::from_slice(&Keccak256::digest(data.as_ref())[..])
}

#[allow(non_upper_case_globals)]
pub const value_to_bytes: fn(Value) -> [u8; 32] = From::<Value>::from;

// Keccak-256 hash of an empty string, KEC("").
pub const EMPTY_HASH: H256 = H256(hex!(
    "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
));

// Root hash of an empty trie.
pub const EMPTY_ROOT: H256 = H256(hex!(
    "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
));

pub const GIGA: u64 = 1_000_000_000; // = 10^9
pub const ETHER: u64 = GIGA * GIGA; // = 10^18
