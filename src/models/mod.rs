mod account;
mod block;
mod bloom;
mod chainspec;
mod header;
mod log;
mod receipt;
mod transaction;

pub use self::{
    account::*, block::*, bloom::*, chainspec::*, header::*, log::*, receipt::*, transaction::*,
};

use derive_more::*;
use hex_literal::hex;
use rlp::{Decodable, Encodable};
use serde::{Deserialize, Serialize};
use std::{iter::Step, mem::size_of, ops::Add};

pub use ethereum_types::{Address, Bloom, H160, H256, H512, H64, U512, U64};
pub use ethnum::*;

pub const KECCAK_LENGTH: usize = H256::len_bytes();
pub const ADDRESS_LENGTH: usize = Address::len_bytes();
pub const BLOCK_NUMBER_LENGTH: usize = size_of::<u64>();

macro_rules! u64_wrapper {
    ($ty:ident) => {
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
            FromStr,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $ty(pub u64);

        impl ::parity_scale_codec::WrapperTypeEncode for $ty {}
        impl ::parity_scale_codec::EncodeLike for $ty {}
        impl ::parity_scale_codec::EncodeLike<u64> for $ty {}
        impl ::parity_scale_codec::EncodeLike<$ty> for u64 {}

        impl ::parity_scale_codec::WrapperTypeDecode for $ty {
            type Wrapped = u64;
        }

        impl From<::parity_scale_codec::Compact<$ty>> for $ty {
            fn from(x: ::parity_scale_codec::Compact<$ty>) -> $ty {
                x.0
            }
        }

        impl ::parity_scale_codec::CompactAs for $ty {
            type As = u64;

            fn encode_as(&self) -> &Self::As {
                &self.0
            }

            fn decode_from(v: Self::As) -> Result<Self, ::parity_scale_codec::Error> {
                Ok(Self(v))
            }
        }

        impl Encodable for $ty {
            fn rlp_append(&self, s: &mut rlp::RlpStream) {
                self.0.rlp_append(s)
            }
        }

        impl Decodable for $ty {
            fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
                <u64 as Decodable>::decode(rlp).map(Self)
            }
        }

        impl Add<u64> for $ty {
            type Output = Self;

            fn add(self, rhs: u64) -> Self::Output {
                Self(self.0 + rhs)
            }
        }

        impl Step for $ty {
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
    };
}

u64_wrapper!(BlockNumber);
u64_wrapper!(ChainId);
u64_wrapper!(NetworkId);
u64_wrapper!(TxIndex);

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
pub const ETHER: u128 = 1_000_000_000_000_000_000; // = 10^18
