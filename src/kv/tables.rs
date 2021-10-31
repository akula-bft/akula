use super::*;
use crate::{models::*, zeroless_view, StageId};
use anyhow::bail;
use arrayref::array_ref;
use arrayvec::ArrayVec;
use bincode::Options;
use bytes::Bytes;
use derive_more::*;
use ethereum_types::*;
use maplit::hashmap;
use modular_bitfield::prelude::*;
use once_cell::sync::Lazy;
use roaring::RoaringTreemap;
use serde::{Deserialize, *};
use std::{collections::HashMap, fmt::Display, sync::Arc};

#[derive(Debug)]
pub struct ErasedTable<T>(pub T)
where
    T: Table;

impl<T> Table for ErasedTable<T>
where
    T: Table,
{
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type SeekKey = Vec<u8>;
    type FusedValue = (Self::Key, Self::Value);

    fn db_name(&self) -> string::String<StaticBytes> {
        self.0.db_name()
    }

    fn fuse_values(key: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
        Ok((key, value))
    }

    fn split_fused((key, value): Self::FusedValue) -> (Self::Key, Self::Value) {
        (key, value)
    }
}

impl<T> ErasedTable<T>
where
    T: Table,
{
    pub fn encode_key(object: T::Key) -> <<T as Table>::Key as TableEncode>::Encoded {
        object.encode()
    }

    pub fn decode_key(input: &[u8]) -> anyhow::Result<T::Key>
    where
        <T as Table>::Key: TableDecode,
    {
        T::Key::decode(input)
    }

    pub fn encode_value(object: T::Value) -> <<T as Table>::Value as TableEncode>::Encoded {
        object.encode()
    }

    pub fn decode_value(input: &[u8]) -> anyhow::Result<T::Value> {
        T::Value::decode(input)
    }

    pub fn encode_seek_key(object: T::SeekKey) -> <<T as Table>::SeekKey as TableEncode>::Encoded {
        object.encode()
    }
}

macro_rules! decl_table {
    ($name:ident => $key:ty => $value:ty => $seek_key:ty) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl crate::kv::traits::Table for $name {
            type Key = $key;
            type SeekKey = $seek_key;
            type Value = $value;
            type FusedValue = (Self::Key, Self::Value);

            fn db_name(&self) -> string::String<bytes::Bytes> {
                unsafe {
                    string::String::from_utf8_unchecked(bytes::Bytes::from_static(
                        Self::const_db_name().as_bytes(),
                    ))
                }
            }

            fn fuse_values(key: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
                Ok((key, value))
            }

            fn split_fused((key, value): Self::FusedValue) -> (Self::Key, Self::Value) {
                (key, value)
            }
        }

        impl $name {
            pub const fn const_db_name() -> &'static str {
                stringify!($name)
            }

            pub const fn erased(self) -> ErasedTable<Self> {
                ErasedTable(self)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", Self::const_db_name())
            }
        }
    };
    ($name:ident => $key:ty => $value:ty) => {
        decl_table!($name => $key => $value => $key);
    };
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TableInfo {
    pub dup_sort: bool,
}

impl traits::TableEncode for Vec<u8> {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl traits::TableDecode for Vec<u8> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(b.to_vec())
    }
}

impl traits::TableEncode for Bytes {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl traits::TableDecode for Bytes {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(b.to_vec().into())
    }
}

#[derive(Clone, Debug, Default, Deref, DerefMut, PartialEq, Eq, PartialOrd, Ord)]
pub struct VariableVec<const LEN: usize> {
    pub inner: ArrayVec<u8, LEN>,
}

impl<const LEN: usize> FromIterator<u8> for VariableVec<LEN> {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self {
            inner: ArrayVec::from_iter(iter),
        }
    }
}

impl<const LEN: usize> AsRef<[u8]> for VariableVec<LEN> {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl<const LEN: usize> traits::TableEncode for VariableVec<LEN> {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl<const LEN: usize> traits::TableDecode for VariableVec<LEN> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let mut out = Self::default();
        out.try_extend_from_slice(b)?;
        Ok(out)
    }
}

impl<const LEN: usize> From<VariableVec<LEN>> for Vec<u8> {
    fn from(v: VariableVec<LEN>) -> Self {
        v.to_vec()
    }
}

#[derive(Clone, Debug)]
pub struct InvalidLength<const EXPECTED: usize> {
    pub got: usize,
}

impl<const EXPECTED: usize> Display for InvalidLength<EXPECTED> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid length: {} != {}", EXPECTED, self.got)
    }
}

impl<const EXPECTED: usize> std::error::Error for InvalidLength<EXPECTED> {}

#[derive(Clone, Debug)]
pub struct TooShort<const MINIMUM: usize> {
    pub got: usize,
}

impl<const MINIMUM: usize> Display for TooShort<MINIMUM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Too short: {} < {}", self.got, MINIMUM)
    }
}

impl<const MINIMUM: usize> std::error::Error for TooShort<MINIMUM> {}

#[derive(Clone, Debug)]
pub struct TooLong<const MAXIMUM: usize> {
    pub got: usize,
}
impl<const MAXIMUM: usize> Display for TooLong<MAXIMUM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Too long: {} > {}", self.got, MAXIMUM)
    }
}

impl<const MAXIMUM: usize> std::error::Error for TooLong<MAXIMUM> {}

macro_rules! u64_table_object {
    ($ty:ident) => {
        impl TableEncode for $ty {
            type Encoded = [u8; 8];

            fn encode(self) -> Self::Encoded {
                self.to_be_bytes()
            }
        }

        impl TableDecode for $ty {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                match b.len() {
                    8 => Ok(u64::from_be_bytes(*array_ref!(&*b, 0, 8)).into()),
                    other => Err(InvalidLength::<8> { got: other }.into()),
                }
            }
        }
    };
}

u64_table_object!(u64);
u64_table_object!(BlockNumber);
u64_table_object!(Incarnation);
u64_table_object!(TxIndex);

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
pub struct TruncateStart<T>(pub T);

impl<T, const LEN: usize> TableEncode for TruncateStart<T>
where
    T: TableEncode<Encoded = [u8; LEN]>,
{
    type Encoded = VariableVec<LEN>;

    fn encode(self) -> Self::Encoded {
        let arr = self.0.encode();

        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(zeroless_view(&arr)).unwrap();
        out
    }
}

impl<T, const LEN: usize> TableDecode for TruncateStart<T>
where
    T: TableEncode<Encoded = [u8; LEN]> + TableDecode,
{
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > LEN {
            return Err(TooLong::<LEN> { got: b.len() }.into());
        }

        let mut arr = [0; LEN];
        arr[LEN - b.len()..].copy_from_slice(b);
        T::decode(&arr).map(Self)
    }
}

macro_rules! bincode_table_object {
    ($ty:ty) => {
        impl TableEncode for $ty {
            type Encoded = Vec<u8>;

            fn encode(self) -> Self::Encoded {
                bincode::DefaultOptions::new().serialize(&self).unwrap()
            }
        }

        impl TableDecode for $ty {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                Ok(bincode::DefaultOptions::new().deserialize(b)?)
            }
        }
    };
}

bincode_table_object!(U256);
bincode_table_object!(BodyForStorage);
bincode_table_object!(BlockHeader);
bincode_table_object!(Transaction);

macro_rules! json_table_object {
    ($ty:ident) => {
        impl TableEncode for $ty {
            type Encoded = Vec<u8>;

            fn encode(self) -> Self::Encoded {
                serde_json::to_vec(&self).unwrap()
            }
        }

        impl TableDecode for $ty {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                Ok(serde_json::from_slice(b)?)
            }
        }
    };
}

json_table_object!(ChainConfig);

impl TableEncode for Address {
    type Encoded = [u8; ADDRESS_LENGTH];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableDecode for Address {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        match b.len() {
            ADDRESS_LENGTH => Ok(Address::from_slice(&*b)),
            other => Err(InvalidLength::<ADDRESS_LENGTH> { got: other }.into()),
        }
    }
}

impl TableEncode for H256 {
    type Encoded = [u8; KECCAK_LENGTH];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableDecode for H256
where
    InvalidLength<KECCAK_LENGTH>: 'static,
{
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        match b.len() {
            KECCAK_LENGTH => Ok(H256::from_slice(&*b)),
            other => Err(InvalidLength::<KECCAK_LENGTH> { got: other }.into()),
        }
    }
}

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
    Into,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ZerolessH256(pub H256);

impl TableEncode for ZerolessH256 {
    type Encoded = VariableVec<KECCAK_LENGTH>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(zeroless_view(&self.0)).unwrap();
        out
    }
}

impl TableDecode for ZerolessH256 {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > KECCAK_LENGTH {
            bail!("too long: {} > {}", b.len(), KECCAK_LENGTH);
        }

        Ok(H256::from_uint(&U256::from_big_endian(b)).into())
    }
}

impl TableEncode for (H256, ZerolessH256) {
    type Encoded = VariableVec<{ KECCAK_LENGTH + KECCAK_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.0.encode()).unwrap();
        out.try_extend_from_slice(zeroless_view(&(self.1).0))
            .unwrap();
        out
    }
}

impl TableDecode for (H256, ZerolessH256) {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > KECCAK_LENGTH + KECCAK_LENGTH {
            return Err(TooLong::<{ KECCAK_LENGTH + KECCAK_LENGTH }> { got: b.len() }.into());
        }

        if b.len() < KECCAK_LENGTH {
            return Err(TooShort::<{ KECCAK_LENGTH }> { got: b.len() }.into());
        }

        let (location, value) = b.split_at(KECCAK_LENGTH);

        Ok((H256::decode(location)?, ZerolessH256::decode(value)?))
    }
}

impl TableEncode for RoaringTreemap {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        let mut out = vec![];
        self.serialize_into(&mut out).unwrap();
        out
    }
}

impl TableDecode for RoaringTreemap {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(RoaringTreemap::deserialize_from(b)?)
    }
}

#[derive(Debug)]
pub struct BitmapKey<K> {
    pub inner: K,
    pub block_number: BlockNumber,
}

impl TableEncode for BitmapKey<Address> {
    type Encoded = [u8; ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH];
        out[..ADDRESS_LENGTH].copy_from_slice(&self.inner.encode());
        out[ADDRESS_LENGTH..].copy_from_slice(&self.block_number.encode());
        out
    }
}

impl TableDecode for BitmapKey<Address> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH {
            return Err(
                InvalidLength::<{ ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH }> { got: b.len() }.into(),
            );
        }

        Ok(Self {
            inner: Address::decode(&b[..ADDRESS_LENGTH])?,
            block_number: BlockNumber::decode(&b[ADDRESS_LENGTH..])?,
        })
    }
}

impl TableEncode for BitmapKey<(Address, H256)> {
    type Encoded = [u8; ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH];
        out[..ADDRESS_LENGTH].copy_from_slice(&self.inner.0.encode());
        out[ADDRESS_LENGTH..ADDRESS_LENGTH + KECCAK_LENGTH].copy_from_slice(&self.inner.1.encode());
        out[ADDRESS_LENGTH + KECCAK_LENGTH..].copy_from_slice(&self.block_number.encode());
        out
    }
}

impl TableDecode for BitmapKey<(Address, H256)> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH {
            return Err(
                InvalidLength::<{ ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH }> {
                    got: b.len(),
                }
                .into(),
            );
        }

        Ok(Self {
            inner: (
                Address::decode(&b[..ADDRESS_LENGTH])?,
                H256::decode(&b[ADDRESS_LENGTH..ADDRESS_LENGTH + KECCAK_LENGTH])?,
            ),
            block_number: BlockNumber::decode(&b[ADDRESS_LENGTH + KECCAK_LENGTH..])?,
        })
    }
}

impl TableEncode for StageId {
    type Encoded = &'static str;

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl<A, B, const A_LEN: usize, const B_LEN: usize> TableEncode for (A, B)
where
    A: TableObject<Encoded = [u8; A_LEN]>,
    B: TableObject<Encoded = [u8; B_LEN]>,
{
    type Encoded = VariableVec<256>;

    fn encode(self) -> Self::Encoded {
        let mut v = Self::Encoded::default();
        v.try_extend_from_slice(&self.0.encode()).unwrap();
        v.try_extend_from_slice(&self.1.encode()).unwrap();
        v
    }
}

impl<A, B, const A_LEN: usize, const B_LEN: usize> TableDecode for (A, B)
where
    A: TableObject<Encoded = [u8; A_LEN]>,
    B: TableObject<Encoded = [u8; B_LEN]>,
{
    fn decode(v: &[u8]) -> anyhow::Result<Self> {
        if v.len() != A_LEN + B_LEN {
            bail!("Invalid len: {} != {} + {}", v.len(), A_LEN, B_LEN);
        }
        Ok((
            A::decode(&v[..A_LEN]).unwrap(),
            B::decode(&v[A_LEN..]).unwrap(),
        ))
    }
}

impl DupSort for PlainState {
    type SeekBothKey = H256;
}
impl DupSort for AccountChangeSet {
    type SeekBothKey = Address;
}
impl DupSort for StorageChangeSet {
    type SeekBothKey = H256;
}
impl DupSort for HashedStorage {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for CallTraceSet {
    type SeekBothKey = Vec<u8>;
}

pub type AccountChangeKey = BlockNumber;

#[derive(Clone, Debug, PartialEq)]
pub struct AccountChange {
    pub address: Address,
    pub account: EncodedAccount,
}

impl TableEncode for AccountChange {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + MAX_ACCOUNT_LEN }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.address.encode()).unwrap();
        out.try_extend_from_slice(&self.account).unwrap();
        out
    }
}

impl TableDecode for AccountChange {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < ADDRESS_LENGTH + 1 {
            return Err(TooShort::<{ ADDRESS_LENGTH + 1 }> { got: b.len() }.into());
        }

        Ok(Self {
            address: Address::decode(&b[..ADDRESS_LENGTH])?,
            account: EncodedAccount::decode(&b[ADDRESS_LENGTH..])?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageChangeKey {
    pub block_number: BlockNumber,
    pub address: Address,
    pub incarnation: Incarnation,
}

impl TableEncode for StorageChangeKey {
    type Encoded = [u8; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH];
        out[..BLOCK_NUMBER_LENGTH].copy_from_slice(&self.block_number.encode());
        out[BLOCK_NUMBER_LENGTH..BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH]
            .copy_from_slice(&self.address.encode());
        out[BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH..].copy_from_slice(&self.incarnation.encode());
        out
    }
}

impl TableDecode for StorageChangeKey {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH {
            return Err(InvalidLength::<
                { BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH },
            > {
                got: b.len(),
            }
            .into());
        }

        Ok(Self {
            block_number: BlockNumber::decode(&b[..BLOCK_NUMBER_LENGTH])?,
            address: Address::decode(
                &b[BLOCK_NUMBER_LENGTH..BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH],
            )?,
            incarnation: Incarnation::decode(&b[BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH..])?,
        })
    }
}

pub enum StorageChangeSeekKey {
    Block(BlockNumber),
    BlockAndAddress(BlockNumber, Address),
    Full(StorageChangeKey),
}

impl TableEncode for StorageChangeSeekKey {
    type Encoded = VariableVec<{ BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        match self {
            StorageChangeSeekKey::Block(block) => {
                out.try_extend_from_slice(&block.encode()).unwrap();
            }
            StorageChangeSeekKey::BlockAndAddress(block, address) => {
                out.try_extend_from_slice(&block.encode()).unwrap();
                out.try_extend_from_slice(&address.encode()).unwrap();
            }
            StorageChangeSeekKey::Full(key) => {
                out.try_extend_from_slice(&key.encode()).unwrap();
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageChange {
    pub location: H256,
    pub value: H256,
}

impl TableEncode for StorageChange {
    type Encoded = VariableVec<{ KECCAK_LENGTH + KECCAK_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.location.encode()).unwrap();
        out.try_extend_from_slice(&ZerolessH256(self.value).encode())
            .unwrap();
        out
    }
}

impl TableDecode for StorageChange {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < KECCAK_LENGTH {
            return Err(TooShort::<KECCAK_LENGTH> { got: b.len() }.into());
        }

        Ok(Self {
            location: H256::decode(&b[..KECCAK_LENGTH])?,
            value: ZerolessH256::decode(&b[KECCAK_LENGTH..])?.0,
        })
    }
}

pub type HeaderKey = (BlockNumber, H256);

#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
struct CallTraceSetFlags {
    flag_from: bool,
    flag_to: bool,
    #[skip]
    unused: B6,
}

#[derive(Clone, Copy, Debug)]
pub struct CallTraceSetEntry {
    address: Address,
    from: bool,
    to: bool,
}

impl TableEncode for CallTraceSetEntry {
    type Encoded = [u8; ADDRESS_LENGTH + 1];

    fn encode(self) -> Self::Encoded {
        let mut v = [0; ADDRESS_LENGTH + 1];
        v[..ADDRESS_LENGTH].copy_from_slice(&self.address.encode());

        let mut field_set = CallTraceSetFlags::default();
        field_set.set_flag_from(self.from);
        field_set.set_flag_to(self.to);
        v[ADDRESS_LENGTH] = field_set.into_bytes()[0];

        v
    }
}

impl TableDecode for CallTraceSetEntry {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != ADDRESS_LENGTH + 1 {
            return Err(InvalidLength::<{ ADDRESS_LENGTH + 1 }> { got: b.len() }.into());
        }

        let field_set = CallTraceSetFlags::from_bytes([b[ADDRESS_LENGTH]]);
        Ok(Self {
            address: Address::decode(&b[..ADDRESS_LENGTH])?,
            from: field_set.flag_from(),
            to: field_set.flag_to(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PlainStateKey {
    Account(Address),
    Storage(Address, Incarnation),
}

impl TableEncode for PlainStateKey {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + INCARNATION_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        match self {
            PlainStateKey::Account(address) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
            }
            PlainStateKey::Storage(address, incarnation) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
                out.try_extend_from_slice(&incarnation.encode()).unwrap();
            }
        }
        out
    }
}

impl TableDecode for PlainStateKey {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        const STORAGE_KEY_LEN: usize = ADDRESS_LENGTH + INCARNATION_LENGTH;
        Ok(match b.len() {
            ADDRESS_LENGTH => Self::Account(Address::decode(b)?),
            STORAGE_KEY_LEN => Self::Storage(
                Address::decode(&b[..ADDRESS_LENGTH])?,
                Incarnation::decode(&b[ADDRESS_LENGTH..])?,
            ),
            _ => bail!(
                "invalid length: expected one of [{}, {}], got {}",
                ADDRESS_LENGTH,
                STORAGE_KEY_LEN,
                b.len()
            ),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PlainStateSeekKey {
    Account(Address),
    StorageAllIncarnations(Address),
    StorageWithIncarnation(Address, Incarnation),
}

impl TableEncode for PlainStateSeekKey {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + INCARNATION_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        match self {
            Self::Account(address) | Self::StorageAllIncarnations(address) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
            }
            Self::StorageWithIncarnation(address, incarnation) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
                out.try_extend_from_slice(&incarnation.encode()).unwrap();
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlainStateFusedValue {
    Account {
        address: Address,
        account: EncodedAccount,
    },
    Storage {
        address: Address,
        incarnation: Incarnation,
        location: H256,
        value: H256,
    },
}

impl PlainStateFusedValue {
    pub fn as_account(&self) -> Option<(Address, EncodedAccount)> {
        if let PlainStateFusedValue::Account { address, account } = self {
            Some((*address, account.clone()))
        } else {
            None
        }
    }

    pub fn as_storage(&self) -> Option<(Address, Incarnation, H256, H256)> {
        if let Self::Storage {
            address,
            incarnation,
            location,
            value,
        } = self
        {
            Some((*address, *incarnation, *location, *value))
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PlainState;

impl Table for PlainState {
    type Key = PlainStateKey;
    type Value = VariableVec<MAX_ACCOUNT_LEN>;
    type SeekKey = PlainStateSeekKey;
    type FusedValue = PlainStateFusedValue;

    fn db_name(&self) -> string::String<bytes::Bytes> {
        unsafe {
            string::String::from_utf8_unchecked(bytes::Bytes::from_static(
                Self::const_db_name().as_bytes(),
            ))
        }
    }

    fn fuse_values(key: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
        Ok(match key {
            PlainStateKey::Account(address) => {
                if value.len() > MAX_ACCOUNT_LEN {
                    return Err(InvalidLength::<MAX_ACCOUNT_LEN> { got: value.len() }.into());
                }

                PlainStateFusedValue::Account {
                    address,
                    account: value,
                }
            }
            PlainStateKey::Storage(address, incarnation) => {
                if value.len() > KECCAK_LENGTH + KECCAK_LENGTH {
                    return Err(
                        TooLong::<{ KECCAK_LENGTH + KECCAK_LENGTH }> { got: value.len() }.into(),
                    );
                }

                PlainStateFusedValue::Storage {
                    address,
                    incarnation,
                    location: H256::decode(&value[..KECCAK_LENGTH])?,
                    value: ZerolessH256::decode(&value[KECCAK_LENGTH..])?.0,
                }
            }
        })
    }

    fn split_fused(fv: Self::FusedValue) -> (Self::Key, Self::Value) {
        match fv {
            PlainStateFusedValue::Account { address, account } => {
                (PlainStateKey::Account(address), account)
            }
            PlainStateFusedValue::Storage {
                address,
                incarnation,
                location,
                value,
            } => {
                let mut v = Self::Value::default();
                v.try_extend_from_slice(&location.encode()).unwrap();
                v.try_extend_from_slice(&ZerolessH256(value).encode())
                    .unwrap();
                (PlainStateKey::Storage(address, incarnation), v)
            }
        }
    }
}

impl PlainState {
    pub const fn const_db_name() -> &'static str {
        "PlainState"
    }

    pub const fn erased(self) -> ErasedTable<Self> {
        ErasedTable(self)
    }
}

impl std::fmt::Display for PlainState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Self::const_db_name())
    }
}

decl_table!(PlainCodeHash => (Address, Incarnation) => H256);
decl_table!(AccountChangeSet => AccountChangeKey => AccountChange);
decl_table!(StorageChangeSet => StorageChangeKey => StorageChange => StorageChangeSeekKey);
decl_table!(HashedAccount => H256 => Vec<u8>);
decl_table!(HashedStorage => (H256, Incarnation) => (H256, ZerolessH256));
decl_table!(AccountHistory => BitmapKey<Address> => RoaringTreemap);
decl_table!(StorageHistory => BitmapKey<(Address, H256)> => RoaringTreemap);
decl_table!(Code => H256 => Bytes);
decl_table!(HashedCodeHash => (H256, Incarnation) => H256);
decl_table!(IncarnationMap => Address => Incarnation);
decl_table!(TrieAccount => Vec<u8> => Vec<u8>);
decl_table!(TrieStorage => Vec<u8> => Vec<u8>);
decl_table!(DbInfo => Vec<u8> => Vec<u8>);
decl_table!(SnapshotInfo => Vec<u8> => Vec<u8>);
decl_table!(BittorrentInfo => Vec<u8> => Vec<u8>);
decl_table!(HeaderNumber => H256 => BlockNumber);
decl_table!(CanonicalHeader => BlockNumber => H256);
decl_table!(Header => HeaderKey => BlockHeader => BlockNumber);
decl_table!(HeadersTotalDifficulty => HeaderKey => U256);
decl_table!(BlockBody => HeaderKey => BodyForStorage => BlockNumber);
decl_table!(BlockTransaction => TxIndex => Transaction);
decl_table!(LogTopicIndex => Vec<u8> => RoaringTreemap);
decl_table!(LogAddressIndex => Vec<u8> => RoaringTreemap);
decl_table!(CallTraceSet => BlockNumber => CallTraceSetEntry);
decl_table!(CallFromIndex => Vec<u8> => RoaringTreemap);
decl_table!(CallToIndex => Vec<u8> => RoaringTreemap);
decl_table!(BlockTransactionLookup => H256 => TruncateStart<BlockNumber>);
decl_table!(Config => H256 => ChainConfig);
decl_table!(SyncStage => StageId => BlockNumber);
decl_table!(TxSender => TxIndex => Address);
decl_table!(LastBlock => Vec<u8> => Vec<u8>);
decl_table!(Migration => Vec<u8> => Vec<u8>);
decl_table!(Sequence => Vec<u8> => Vec<u8>);
decl_table!(LastHeader => VariableVec<0> => H256);
decl_table!(Issuance => Vec<u8> => Vec<u8>);

pub type DatabaseChart = Arc<HashMap<&'static str, TableInfo>>;

pub static CHAINDATA_TABLES: Lazy<Arc<HashMap<&'static str, TableInfo>>> = Lazy::new(|| {
    Arc::new(hashmap! {
        PlainState::const_db_name() => TableInfo {
            dup_sort: true,
        },
        PlainCodeHash::const_db_name() => TableInfo::default(),
        AccountChangeSet::const_db_name() => TableInfo {
            dup_sort: true,
        },
        StorageChangeSet::const_db_name() => TableInfo {
            dup_sort: true,
        },
        HashedAccount::const_db_name() => TableInfo::default(),
        HashedStorage::const_db_name() => TableInfo {
            dup_sort: true,
        },
        AccountHistory::const_db_name() => TableInfo::default(),
        StorageHistory::const_db_name() => TableInfo::default(),
        Code::const_db_name() => TableInfo::default(),
        HashedCodeHash::const_db_name() => TableInfo::default(),
        IncarnationMap::const_db_name() => TableInfo::default(),
        TrieAccount::const_db_name() => TableInfo::default(),
        TrieStorage::const_db_name() => TableInfo::default(),
        DbInfo::const_db_name() => TableInfo::default(),
        SnapshotInfo::const_db_name() => TableInfo::default(),
        BittorrentInfo::const_db_name() => TableInfo::default(),
        HeaderNumber::const_db_name() => TableInfo::default(),
        CanonicalHeader::const_db_name() => TableInfo::default(),
        Header::const_db_name() => TableInfo::default(),
        HeadersTotalDifficulty::const_db_name() => TableInfo::default(),
        BlockBody::const_db_name() => TableInfo::default(),
        BlockTransaction::const_db_name() => TableInfo::default(),
        LogTopicIndex::const_db_name() => TableInfo::default(),
        LogAddressIndex::const_db_name() => TableInfo::default(),
        CallTraceSet::const_db_name() => TableInfo {
            dup_sort: true,
        },
        CallFromIndex::const_db_name() => TableInfo::default(),
        CallToIndex::const_db_name() => TableInfo::default(),
        BlockTransactionLookup::const_db_name() => TableInfo::default(),
        Config::const_db_name() => TableInfo::default(),
        SyncStage::const_db_name() => TableInfo::default(),
        TxSender::const_db_name() => TableInfo::default(),
        LastBlock::const_db_name() => TableInfo::default(),
        Migration::const_db_name() => TableInfo::default(),
        Sequence::const_db_name() => TableInfo::default(),
        LastHeader::const_db_name() => TableInfo::default(),
        Issuance::const_db_name() => TableInfo::default(),
    })
});
