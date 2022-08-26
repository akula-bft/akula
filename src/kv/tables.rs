use super::*;
use crate::{models::*, zeroless_view, StageId};
use anyhow::{bail, format_err};
use arrayref::array_ref;
use arrayvec::ArrayVec;
use bytes::Bytes;
use croaring::{treemap::NativeSerializer, Treemap as RoaringTreemap};
use derive_more::*;
use modular_bitfield::prelude::*;
use once_cell::sync::Lazy;
use serde::{Deserialize, *};
use std::{collections::BTreeMap, fmt::Display, sync::Arc};

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

    fn db_name(&self) -> string::String<Bytes> {
        self.0.db_name()
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

#[macro_export]
macro_rules! decl_table {
    ($name:ident => $key:ty => $value:ty => $seek_key:ty) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl $crate::kv::traits::Table for $name {
            type Key = $key;
            type SeekKey = $seek_key;
            type Value = $value;

            fn db_name(&self) -> string::String<bytes::Bytes> {
                unsafe {
                    string::String::from_utf8_unchecked(bytes::Bytes::from_static(
                        Self::const_db_name().as_bytes(),
                    ))
                }
            }
        }

        impl $name {
            pub const fn const_db_name() -> &'static str {
                stringify!($name)
            }

            pub const fn erased(self) -> $crate::kv::tables::ErasedTable<Self> {
                $crate::kv::tables::ErasedTable(self)
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

/// Table encoding impls
impl traits::TableEncode for () {
    type Encoded = [u8; 0];

    fn encode(self) -> Self::Encoded {
        []
    }
}

impl traits::TableDecode for () {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if !b.is_empty() {
            return Err(TooLong::<0> { got: b.len() }.into());
        }

        Ok(())
    }
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

macro_rules! scale_table_object {
    ($ty:ty) => {
        impl TableEncode for $ty {
            type Encoded = Vec<u8>;

            fn encode(self) -> Self::Encoded {
                ::parity_scale_codec::Encode::encode(&self)
            }
        }

        impl TableDecode for $ty {
            fn decode(mut b: &[u8]) -> anyhow::Result<Self> {
                Ok(<Self as ::parity_scale_codec::Decode>::decode(&mut b)?)
            }
        }
    };
}

scale_table_object!(BodyForStorage);
scale_table_object!(BlockHeader);
scale_table_object!(MessageWithSignature);

macro_rules! ron_table_object {
    ($ty:ident) => {
        impl TableEncode for $ty {
            type Encoded = String;

            fn encode(self) -> Self::Encoded {
                ron::to_string(&self).unwrap()
            }
        }

        impl TableDecode for $ty {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                Ok(ron::from_str(std::str::from_utf8(b)?)?)
            }
        }
    };
}

ron_table_object!(ChainSpec);

impl TableEncode for Address {
    type Encoded = [u8; ADDRESS_LENGTH];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableDecode for Address {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        match b.len() {
            ADDRESS_LENGTH => Ok(Address::from_slice(b)),
            other => Err(InvalidLength::<ADDRESS_LENGTH> { got: other }.into()),
        }
    }
}

impl TableEncode for Vec<Address> {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        let mut v = Vec::with_capacity(self.len() * ADDRESS_LENGTH);
        for addr in self {
            v.extend_from_slice(&addr.encode());
        }

        v
    }
}

impl TableDecode for Vec<Address> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() % ADDRESS_LENGTH != 0 {
            bail!("Slice len should be divisible by {}", ADDRESS_LENGTH);
        }

        let mut v = Vec::with_capacity(b.len() / ADDRESS_LENGTH);
        for i in 0..b.len() / ADDRESS_LENGTH {
            let offset = i * ADDRESS_LENGTH;
            v.push(TableDecode::decode(&b[offset..offset + ADDRESS_LENGTH])?);
        }

        Ok(v)
    }
}

impl TableEncode for Vec<H256> {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        let mut v = Vec::with_capacity(self.len() * KECCAK_LENGTH);
        for addr in self {
            v.extend_from_slice(&addr.encode());
        }

        v
    }
}

impl TableDecode for Vec<H256> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() % KECCAK_LENGTH != 0 {
            bail!("Slice len should be divisible by {}", KECCAK_LENGTH);
        }

        let mut v = Vec::with_capacity(b.len() / KECCAK_LENGTH);
        for i in 0..b.len() / KECCAK_LENGTH {
            let offset = i * KECCAK_LENGTH;
            v.push(TableDecode::decode(&b[offset..offset + KECCAK_LENGTH])?);
        }

        Ok(v)
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
            KECCAK_LENGTH => Ok(H256::from_slice(b)),
            other => Err(InvalidLength::<KECCAK_LENGTH> { got: other }.into()),
        }
    }
}

impl TableEncode for U256 {
    type Encoded = VariableVec<KECCAK_LENGTH>;

    fn encode(self) -> Self::Encoded {
        self.to_be_bytes()
            .into_iter()
            .skip_while(|&v| v == 0)
            .collect()
    }
}

impl TableDecode for U256 {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > KECCAK_LENGTH {
            return Err(TooLong::<KECCAK_LENGTH> { got: b.len() }.into());
        }
        let mut v = [0; 32];
        v[KECCAK_LENGTH - b.len()..].copy_from_slice(b);
        Ok(Self::from_be_bytes(v))
    }
}

impl TableEncode for (H256, U256) {
    type Encoded = VariableVec<{ KECCAK_LENGTH + KECCAK_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.0.encode()).unwrap();
        out.try_extend_from_slice(&self.1.encode()).unwrap();
        out
    }
}

impl TableDecode for (H256, U256) {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > KECCAK_LENGTH + KECCAK_LENGTH {
            return Err(TooLong::<{ KECCAK_LENGTH + KECCAK_LENGTH }> { got: b.len() }.into());
        }

        if b.len() < KECCAK_LENGTH {
            return Err(TooShort::<{ KECCAK_LENGTH }> { got: b.len() }.into());
        }

        let (location, value) = b.split_at(KECCAK_LENGTH);

        Ok((H256::decode(location)?, U256::decode(value)?))
    }
}

impl TableEncode for RoaringTreemap {
    type Encoded = Vec<u8>;

    fn encode(mut self) -> Self::Encoded {
        self.run_optimize();
        self.serialize().unwrap()
    }
}

impl TableDecode for RoaringTreemap {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(RoaringTreemap::deserialize(b)?)
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

impl DupSort for Storage {
    type SeekBothKey = H256;
}
impl DupSort for AccountChangeSet {
    type SeekBothKey = Address;
}
impl DupSort for StorageChangeSet {
    type SeekBothKey = H256;
}
impl DupSort for HashedStorage {
    type SeekBothKey = H256;
}
impl DupSort for CallTraceSet {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for LogAddressesByBlock {
    type SeekBothKey = Address;
}
impl DupSort for LogTopicsByBlock {
    type SeekBothKey = H256;
}

pub type AccountChangeKey = BlockNumber;

impl TableEncode for crate::models::Account {
    type Encoded = EncodedAccount;

    fn encode(self) -> Self::Encoded {
        self.encode_for_storage()
    }
}

impl TableDecode for crate::models::Account {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        crate::models::Account::decode_for_storage(b)?.ok_or_else(|| format_err!("cannot be empty"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountChange {
    pub address: Address,
    pub account: Option<crate::models::Account>,
}

impl TableEncode for AccountChange {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + MAX_ACCOUNT_LEN }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.address.encode()).unwrap();
        if let Some(account) = self.account {
            out.try_extend_from_slice(&account.encode()).unwrap();
        }
        out
    }
}

impl TableDecode for AccountChange {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < ADDRESS_LENGTH {
            return Err(TooShort::<{ ADDRESS_LENGTH }> { got: b.len() }.into());
        }

        Ok(Self {
            address: TableDecode::decode(&b[..ADDRESS_LENGTH])?,
            account: if b.len() > ADDRESS_LENGTH {
                Some(TableDecode::decode(&b[ADDRESS_LENGTH..])?)
            } else {
                None
            },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageChangeKey {
    pub block_number: BlockNumber,
    pub address: Address,
}

impl TableEncode for StorageChangeKey {
    type Encoded = [u8; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH];
        out[..BLOCK_NUMBER_LENGTH].copy_from_slice(&self.block_number.encode());
        out[BLOCK_NUMBER_LENGTH..].copy_from_slice(&self.address.encode());
        out
    }
}

impl TableDecode for StorageChangeKey {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH {
            return Err(
                InvalidLength::<{ BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH }> { got: b.len() }.into(),
            );
        }

        Ok(Self {
            block_number: BlockNumber::decode(&b[..BLOCK_NUMBER_LENGTH])?,
            address: Address::decode(
                &b[BLOCK_NUMBER_LENGTH..BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH],
            )?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageChange {
    pub location: H256,
    pub value: U256,
}

impl TableEncode for StorageChange {
    type Encoded = VariableVec<{ KECCAK_LENGTH + KECCAK_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.location.encode()).unwrap();
        out.try_extend_from_slice(&self.value.encode()).unwrap();
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
            value: U256::decode(&b[KECCAK_LENGTH..])?,
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
    pub address: Address,
    pub from: bool,
    pub to: bool,
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

decl_table!(Account => Address => crate::models::Account);
decl_table!(Storage => Address => (H256, U256));
decl_table!(AccountChangeSet => AccountChangeKey => AccountChange);
decl_table!(StorageChangeSet => StorageChangeKey => StorageChange => BlockNumber);
decl_table!(HashedAccount => H256 => crate::models::Account);
decl_table!(HashedStorage => H256 => (H256, U256));
decl_table!(AccountHistory => BitmapKey<Address> => RoaringTreemap);
decl_table!(StorageHistory => BitmapKey<(Address, H256)> => RoaringTreemap);
decl_table!(Code => H256 => Bytes);
decl_table!(TrieAccount => Vec<u8> => Vec<u8>);
decl_table!(TrieStorage => Vec<u8> => Vec<u8>);
decl_table!(HeaderNumber => H256 => BlockNumber);
decl_table!(CanonicalHeader => BlockNumber => H256);
decl_table!(Header => HeaderKey => BlockHeader => BlockNumber);
decl_table!(HeadersTotalDifficulty => HeaderKey => U256);
decl_table!(BlockBody => HeaderKey => BodyForStorage => BlockNumber);
decl_table!(BlockTransaction => TxIndex => MessageWithSignature);
decl_table!(TotalGas => BlockNumber => u64);
decl_table!(TotalTx => BlockNumber => u64);
decl_table!(LogAddressIndex => Address => RoaringTreemap);
decl_table!(LogAddressesByBlock => BlockNumber => Address);
decl_table!(LogTopicIndex => H256 => RoaringTreemap);
decl_table!(LogTopicsByBlock => BlockNumber => H256);
decl_table!(CallTraceSet => BlockNumber => CallTraceSetEntry);
decl_table!(CallFromIndex => BitmapKey<Address> => RoaringTreemap);
decl_table!(CallToIndex => BitmapKey<Address> => RoaringTreemap);
decl_table!(BlockTransactionLookup => H256 => TruncateStart<BlockNumber>);
decl_table!(Config => () => ChainSpec);
decl_table!(SyncStage => StageId => BlockNumber);
decl_table!(PruneProgress => StageId => BlockNumber);
decl_table!(TxSender => HeaderKey => Vec<Address>);
decl_table!(LastHeader => () => HeaderKey);
decl_table!(Issuance => Vec<u8> => Vec<u8>);
decl_table!(Version => () => u64);
decl_table!(ColParliaSnapshot => H256 => Vec<u8>);

pub type DatabaseChart = BTreeMap<&'static str, TableInfo>;

macro_rules! table_entry {
    ($t:ty) => {
        (
            <$t>::const_db_name(),
            TableInfo {
                dup_sort: impls::impls!($t: DupSort),
            },
        )
    };
}

pub static CHAINDATA_TABLES: Lazy<Arc<DatabaseChart>> = Lazy::new(|| {
    Arc::new(
        [
            table_entry!(Account),
            table_entry!(Storage),
            table_entry!(AccountChangeSet),
            table_entry!(StorageChangeSet),
            table_entry!(HashedAccount),
            table_entry!(HashedStorage),
            table_entry!(AccountHistory),
            table_entry!(StorageHistory),
            table_entry!(Code),
            table_entry!(TrieAccount),
            table_entry!(TrieStorage),
            table_entry!(HeaderNumber),
            table_entry!(CanonicalHeader),
            table_entry!(Header),
            table_entry!(HeadersTotalDifficulty),
            table_entry!(BlockBody),
            table_entry!(BlockTransaction),
            table_entry!(TotalGas),
            table_entry!(TotalTx),
            table_entry!(LogAddressIndex),
            table_entry!(LogAddressesByBlock),
            table_entry!(LogTopicIndex),
            table_entry!(LogTopicsByBlock),
            table_entry!(CallTraceSet),
            table_entry!(CallFromIndex),
            table_entry!(CallToIndex),
            table_entry!(BlockTransactionLookup),
            table_entry!(Config),
            table_entry!(SyncStage),
            table_entry!(PruneProgress),
            table_entry!(TxSender),
            table_entry!(LastHeader),
            table_entry!(Issuance),
            table_entry!(Version),
            table_entry!(ColParliaSnapshot),
        ]
        .into_iter()
        .collect(),
    )
});

pub static SENTRY_TABLES: Lazy<Arc<DatabaseChart>> =
    Lazy::new(|| Arc::new([].into_iter().collect()));

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn u256() {
        for (fixture, expected) in [
            (U256::ZERO, vec![]),
            (
                U256::from(0xDEADBEEFBAADCAFE_u128),
                hex!("DEADBEEFBAADCAFE").to_vec(),
            ),
            (U256::MAX, hex!("FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF FFFF").to_vec()),
        ] {
            assert_eq!(fixture.encode().to_vec(), expected);
            assert_eq!(U256::decode(&expected).unwrap(), fixture);
        }
    }

    #[test]
    fn table_meta() {
        assert!(!CHAINDATA_TABLES[tables::Account::const_db_name()].dup_sort);
        assert!(CHAINDATA_TABLES[tables::Storage::const_db_name()].dup_sort);
    }
}
