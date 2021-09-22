use super::*;
use crate::models::*;
use arrayref::array_ref;
use ethereum_types::*;
use maplit::hashmap;
use once_cell::sync::Lazy;
use serde::Deserialize;
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

    fn db_name(&self) -> string::String<StaticBytes> {
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

    pub fn decode_key(
        input: &[u8],
    ) -> Result<T::Key, <<T as Table>::Key as TableDecode>::DecodeError> {
        T::Key::decode(input)
    }

    pub fn encode_value(object: T::Value) -> <<T as Table>::Value as TableEncode>::Encoded {
        object.encode()
    }

    pub fn decode_value(
        input: &[u8],
    ) -> Result<T::Value, <<T as Table>::Value as TableDecode>::DecodeError> {
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

            fn db_name(&self) -> string::String<static_bytes::Bytes> {
                unsafe {
                    string::String::from_utf8_unchecked(static_bytes::Bytes::from_static(
                        Self::const_db_name().as_bytes(),
                    ))
                }
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
    type DecodeError = !;

    fn decode(b: &[u8]) -> Result<Self, Self::DecodeError> {
        Ok(b.to_vec())
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

macro_rules! u64_table_object {
    ($ty:ident) => {
        impl TableDecode for $ty
        where
            InvalidLength<8>: 'static,
        {
            type DecodeError = InvalidLength<8>;

            fn decode(b: &[u8]) -> Result<Self, Self::DecodeError> {
                match b.len() {
                    8 => Ok(Self(u64::from_be_bytes(*array_ref!(&*b, 0, 8)))),
                    other => Err(InvalidLength { got: other }),
                }
            }
        }

        impl TableEncode for $ty {
            type Encoded = [u8; 8];

            fn encode(self) -> Self::Encoded {
                self.0.to_be_bytes()
            }
        }
    };
}

u64_table_object!(BlockNumber);
u64_table_object!(Incarnation);
u64_table_object!(TxIndex);

impl TableEncode for Address {
    type Encoded = [u8; ADDRESS_LENGTH];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableDecode for Address
where
    InvalidLength<ADDRESS_LENGTH>: 'static,
{
    type DecodeError = InvalidLength<ADDRESS_LENGTH>;

    fn decode(b: &[u8]) -> Result<Self, Self::DecodeError> {
        match b.len() {
            ADDRESS_LENGTH => Ok(Address::from_slice(&*b)),
            other => Err(InvalidLength { got: other }),
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
    type DecodeError = InvalidLength<KECCAK_LENGTH>;

    fn decode(b: &[u8]) -> Result<Self, Self::DecodeError> {
        match b.len() {
            KECCAK_LENGTH => Ok(H256::from_slice(&*b)),
            other => Err(InvalidLength { got: other }),
        }
    }
}

impl DupSort for PlainState {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for AccountChangeSet {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for StorageChangeSet {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for HashedStorage {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for CallTraceSet {
    type SeekBothKey = Vec<u8>;
}

decl_table!(PlainState => Vec<u8> => Vec<u8>);
decl_table!(PlainCodeHash => Vec<u8> => H256);
decl_table!(AccountChangeSet => Vec<u8> => Vec<u8>);
decl_table!(StorageChangeSet => Vec<u8> => Vec<u8>);
decl_table!(HashedAccount => H256 => Vec<u8>);
decl_table!(HashedStorage => Vec<u8> => Vec<u8>);
decl_table!(AccountHistory => Vec<u8> => Vec<u8>);
decl_table!(StorageHistory => Vec<u8> => Vec<u8>);
decl_table!(Code => H256 => Vec<u8>);
decl_table!(HashedCodeHash => Vec<u8> => Vec<u8>);
decl_table!(IncarnationMap => Vec<u8> => Vec<u8>);
decl_table!(TEVMCode => H256 => Vec<u8>);
decl_table!(TrieAccount => Vec<u8> => Vec<u8>);
decl_table!(TrieStorage => Vec<u8> => Vec<u8>);
decl_table!(DbInfo => Vec<u8> => Vec<u8>);
decl_table!(SnapshotInfo => Vec<u8> => Vec<u8>);
decl_table!(BittorrentInfo => Vec<u8> => Vec<u8>);
decl_table!(HeaderNumber => H256 => BlockNumber);
decl_table!(CanonicalHeader => BlockNumber => H256);
decl_table!(Header => Vec<u8> => Vec<u8>);
decl_table!(HeadersTotalDifficulty => Vec<u8> => Vec<u8>);
decl_table!(BlockBody => Vec<u8> => Vec<u8>);
decl_table!(BlockTransaction => Vec<u8> => Vec<u8>);
decl_table!(Receipt => Vec<u8> => Vec<u8>);
decl_table!(TransactionLog => Vec<u8> => Vec<u8>);
decl_table!(LogTopicIndex => Vec<u8> => Vec<u8>);
decl_table!(LogAddressIndex => Vec<u8> => Vec<u8>);
decl_table!(CallTraceSet => Vec<u8> => Vec<u8>);
decl_table!(CallFromIndex => Vec<u8> => Vec<u8>);
decl_table!(CallToIndex => Vec<u8> => Vec<u8>);
decl_table!(BlockTransactionLookup => H256 => Vec<u8>);
decl_table!(Config => H256 => Vec<u8>);
decl_table!(SyncStage => Vec<u8> => BlockNumber);
decl_table!(CliqueSeparate => Vec<u8> => Vec<u8>);
decl_table!(CliqueSnapshot => Vec<u8> => Vec<u8>);
decl_table!(CliqueLastSnapshot => Vec<u8> => Vec<u8>);
decl_table!(TxSender => TxIndex => Address);
decl_table!(LastBlock => Vec<u8> => Vec<u8>);
decl_table!(Migration => Vec<u8> => Vec<u8>);
decl_table!(Sequence => Vec<u8> => Vec<u8>);
decl_table!(LastHeader => Vec<u8> => Vec<u8>);
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
        TEVMCode::const_db_name() => TableInfo::default(),
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
        Receipt::const_db_name() => TableInfo::default(),
        TransactionLog::const_db_name() => TableInfo::default(),
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
        CliqueSeparate::const_db_name() => TableInfo::default(),
        CliqueSnapshot::const_db_name() => TableInfo::default(),
        CliqueLastSnapshot::const_db_name() => TableInfo::default(),
        TxSender::const_db_name() => TableInfo::default(),
        LastBlock::const_db_name() => TableInfo::default(),
        Migration::const_db_name() => TableInfo::default(),
        Sequence::const_db_name() => TableInfo::default(),
        LastHeader::const_db_name() => TableInfo::default(),
        Issuance::const_db_name() => TableInfo::default(),
    })
});
