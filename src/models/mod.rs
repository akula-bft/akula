mod account;
mod block;
mod bloom;
mod chainspec;
mod config;
mod header;
mod log;
mod receipt;
mod transaction;

pub use self::{
    account::*, block::*, bloom::*, config::*, header::*, log::*, receipt::*, transaction::*,
};

use derive_more::{Deref, DerefMut, Display, From};
use ethereum_types::{H256, U256};
use hex_literal::hex;
use once_cell::sync::Lazy;
use rlp::{Decodable, Encodable};
use serde::{Deserialize, Serialize};
use std::{iter::Step, mem::size_of, ops::Add};

pub use ethereum_types::Address;
pub type Incarnation = u64;

pub const KECCAK_LENGTH: usize = H256::len_bytes();
pub const ADDRESS_LENGTH: usize = Address::len_bytes();
pub const BLOCK_NUMBER_LENGTH: usize = size_of::<u64>();
pub const INCARNATION_LENGTH: usize = size_of::<u64>();

#[derive(
    Clone,
    Copy,
    Debug,
    Deref,
    DerefMut,
    Default,
    Display,
    PartialEq,
    Eq,
    From,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct BlockNumber(pub u64);

impl Encodable for BlockNumber {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        self.0.rlp_append(s)
    }
}

impl Decodable for BlockNumber {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        u64::decode(rlp).map(Self)
    }
}

impl Add<u64> for BlockNumber {
    type Output = BlockNumber;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Step for BlockNumber {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        u64::steps_between(&start.0, &end.0)
    }

    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        u64::forward_checked(start.0, count).map(Self)
    }

    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        u64::backward_checked(start.0, count).map(Self)
    }
}

impl BlockNumber {
    pub fn db_key(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

#[allow(non_upper_case_globals)]
pub const value_to_bytes: fn(U256) -> [u8; 32] = From::<U256>::from;

// Keccak-256 hash of an empty string, KEC("").
pub const EMPTY_HASH: H256 = H256(hex!(
    "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
));

// Keccak-256 hash of the RLP of an empty list, KEC("\xc0").
pub const EMPTY_LIST_HASH: H256 = H256(hex!(
    "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
));

// Root hash of an empty trie.
pub const EMPTY_ROOT: H256 = H256(hex!(
    "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
));

pub const GIGA: u64 = 1_000_000_000; // = 10^9
pub static ETHER: Lazy<U256> = Lazy::new(|| U256::from(GIGA * GIGA)); // = 10^18
