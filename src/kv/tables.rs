use super::*;
use maplit::hashmap;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};

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
            const fn const_db_name() -> &'static str {
                stringify!($name)
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
decl_table!(PlainCodeHash => Vec<u8> => Vec<u8>);
decl_table!(AccountChangeSet => Vec<u8> => Vec<u8>);
decl_table!(StorageChangeSet => Vec<u8> => Vec<u8>);
decl_table!(HashedAccount => Vec<u8> => Vec<u8>);
decl_table!(HashedStorage => Vec<u8> => Vec<u8>);
decl_table!(AccountHistory => Vec<u8> => Vec<u8>);
decl_table!(StorageHistory => Vec<u8> => Vec<u8>);
decl_table!(Code => Vec<u8> => Vec<u8>);
decl_table!(HashedCodeHash => Vec<u8> => Vec<u8>);
decl_table!(IncarnationMap => Vec<u8> => Vec<u8>);
decl_table!(TEVMCode => Vec<u8> => Vec<u8>);
decl_table!(TrieAccount => Vec<u8> => Vec<u8>);
decl_table!(TrieStorage => Vec<u8> => Vec<u8>);
decl_table!(DbInfo => Vec<u8> => Vec<u8>);
decl_table!(SnapshotInfo => Vec<u8> => Vec<u8>);
decl_table!(BittorrentInfo => Vec<u8> => Vec<u8>);
decl_table!(HeaderNumber => Vec<u8> => Vec<u8>);
decl_table!(CanonicalHeader => Vec<u8> => Vec<u8>);
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
decl_table!(BlockTransactionLookup => Vec<u8> => Vec<u8>);
decl_table!(Config => Vec<u8> => Vec<u8>);
decl_table!(SyncStage => Vec<u8> => Vec<u8>);
decl_table!(CliqueSeparate => Vec<u8> => Vec<u8>);
decl_table!(CliqueSnapshot => Vec<u8> => Vec<u8>);
decl_table!(CliqueLastSnapshot => Vec<u8> => Vec<u8>);
decl_table!(TxSender => Vec<u8> => Vec<u8>);
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
