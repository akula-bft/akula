mod account;
mod block;
mod bloom;
mod chainspec;
mod config;
mod header;
mod log;
mod receipt;
mod revision;
mod transaction;

pub use self::{
    account::*, block::*, bloom::*, chainspec::*, config::*, header::*, log::*, receipt::*,
    revision::*, transaction::*,
};

use derive_more::*;
use fastrlp::*;
use hex_literal::hex;
use serde::{Deserialize, Serialize};
use std::{
    iter::Step,
    mem::size_of,
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, RemAssign, Sub, SubAssign},
};

pub use ethereum_types::{Address, Bloom, H128, H160, H256, H512, H64, U512, U64};
pub use ethnum::*;

pub const KECCAK_LENGTH: usize = H256::len_bytes();
pub const ADDRESS_LENGTH: usize = Address::len_bytes();
pub const BLOCK_NUMBER_LENGTH: usize = size_of::<u64>();

macro_rules! impl_ops {
    ($type:ty, $other:ty) => {
        impl Add<$other> for $type {
            type Output = Self;
            #[inline(always)]
            fn add(self, other: $other) -> Self {
                Self(
                    self.0
                        + u64::try_from(other)
                            .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() }),
                )
            }
        }
        impl Sub<$other> for $type {
            type Output = Self;
            #[inline(always)]
            fn sub(self, other: $other) -> Self {
                Self(
                    self.0
                        - u64::try_from(other)
                            .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() }),
                )
            }
        }
        impl Mul<$other> for $type {
            type Output = Self;
            #[inline(always)]
            fn mul(self, other: $other) -> Self {
                Self(
                    self.0
                        * u64::try_from(other)
                            .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() }),
                )
            }
        }
        impl Div<$other> for $type {
            type Output = Self;
            #[inline(always)]
            fn div(self, other: $other) -> Self {
                Self(
                    self.0
                        / u64::try_from(other)
                            .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() }),
                )
            }
        }
        impl Rem<$other> for $type {
            type Output = Self;
            #[inline(always)]
            fn rem(self, other: $other) -> Self {
                Self(
                    self.0
                        % u64::try_from(other)
                            .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() }),
                )
            }
        }
        impl AddAssign<$other> for $type {
            #[inline(always)]
            fn add_assign(&mut self, other: $other) {
                self.0 += u64::try_from(other)
                    .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() });
            }
        }
        impl SubAssign<$other> for $type {
            #[inline(always)]
            fn sub_assign(&mut self, other: $other) {
                self.0 -= u64::try_from(other)
                    .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() });
            }
        }
        impl MulAssign<$other> for $type {
            #[inline(always)]
            fn mul_assign(&mut self, other: $other) {
                self.0 *= u64::try_from(other)
                    .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() });
            }
        }
        impl DivAssign<$other> for $type {
            #[inline(always)]
            fn div_assign(&mut self, other: $other) {
                self.0 /= u64::try_from(other)
                    .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() });
            }
        }
        impl RemAssign<$other> for $type {
            #[inline(always)]
            fn rem_assign(&mut self, other: $other) {
                self.0 %= u64::try_from(other)
                    .unwrap_or_else(|_| unsafe { std::hint::unreachable_unchecked() });
            }
        }
    };
}

macro_rules! impl_from {
    ($type:ty, $other:ty) => {
        impl const From<$type> for $other {
            #[inline(always)]
            fn from(x: $type) -> $other {
                x.0 as $other
            }
        }
    };
}

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
            Eq,
            From,
            FromStr,
            PartialEq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
            RlpEncodableWrapper,
            RlpDecodableWrapper,
            RlpMaxEncodedLen,
        )]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $ty(pub u64);

        impl const ::parity_scale_codec::WrapperTypeEncode for $ty {}
        impl const ::parity_scale_codec::EncodeLike for $ty {}
        impl const ::parity_scale_codec::EncodeLike<u64> for $ty {}
        impl const ::parity_scale_codec::EncodeLike<$ty> for u64 {}
        impl const ::parity_scale_codec::WrapperTypeDecode for $ty {
            type Wrapped = u64;
        }
        impl const From<::parity_scale_codec::Compact<$ty>> for $ty {
            #[inline(always)]
            fn from(x: ::parity_scale_codec::Compact<$ty>) -> $ty {
                x.0
            }
        }
        impl const ::parity_scale_codec::CompactAs for $ty {
            type As = u64;
            #[inline(always)]
            fn encode_as(&self) -> &Self::As {
                &self.0
            }
            #[inline(always)]
            fn decode_from(v: Self::As) -> Result<Self, ::parity_scale_codec::Error> {
                Ok(Self(v))
            }
        }
        impl PartialOrd<usize> for $ty {
            #[inline(always)]
            fn partial_cmp(&self, other: &usize) -> Option<std::cmp::Ordering> {
                self.0.partial_cmp(&(*other as u64))
            }
        }
        impl const PartialEq<usize> for $ty {
            #[inline(always)]
            fn eq(&self, other: &usize) -> bool {
                self.0 == *other as u64
            }
        }
        impl Add<i32> for $ty {
            type Output = Self;
            #[inline(always)]
            fn add(self, other: i32) -> Self {
                Self(self.0 + u64::try_from(other).unwrap())
            }
        }

        impl_from!($ty, u64);
        impl_from!($ty, usize);

        impl_ops!($ty, u8);
        impl_ops!($ty, u64);
        impl_ops!($ty, usize);
        impl_ops!($ty, $ty);

        impl Step for $ty {
            #[inline(always)]
            fn steps_between(start: &Self, end: &Self) -> Option<usize> {
                u64::steps_between(&start.0, &end.0)
            }
            #[inline(always)]
            fn forward_checked(start: Self, count: usize) -> Option<Self> {
                u64::forward_checked(start.0, count).map(Self)
            }
            #[inline(always)]
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
