use maplit::hashmap;
use std::collections::HashMap;

pub trait Bucket {
    const DB_NAME: &'static str;
}

macro_rules! decl_bucket {
    ($name:ident, $db_name:expr) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $name;

        impl $crate::dbutils::Bucket for $name {
            const DB_NAME: &'static str = $db_name;
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                <Self as $crate::dbutils::Bucket>::DB_NAME
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", <Self as $crate::dbutils::Bucket>::DB_NAME)
            }
        }
    };
}

pub mod buckets {
    decl_bucket!(PlainState, "PLAIN-CST2");
    decl_bucket!(PlainContractCode, "PLAIN-contractCode");
    decl_bucket!(PlainAccountChangeSet, "PLAIN-ACS");
    decl_bucket!(PlainStorageChangeSet, "PLAIN-SCS");
    decl_bucket!(CurrentState, "CST2");
    decl_bucket!(AccountsHistory, "hAT");
    decl_bucket!(StorageHistory, "hST");
    decl_bucket!(Code, "CODE");
    decl_bucket!(ContractCode, "contractCode");
    decl_bucket!(IncarnationMap, "incarnationMap");
    decl_bucket!(IntermediateTrieHash, "iTh2");
    decl_bucket!(DatabaseInfo, "DBINFO");
    decl_bucket!(SnapshotInfo, "SNINFO");
}

pub type BucketFlags = u8;
pub type DBI = u8;
pub type CustomComparator = &'static str;

#[derive(Clone, Copy, Debug)]
pub enum SyncStage {
    Headers,
    BlockHashes,
    Bodies,
    Senders,
    Execution,
    IntermediateHashes,
    HashState,
    AccountHistoryIndex,
    StorageHistoryIndex,
    LogIndex,
    CallTraces,
    TxLookup,
    TxPool,
    Finish,
}

impl AsRef<[u8]> for SyncStage {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Headers => "Headers",
            Self::BlockHashes => "BlockHashes",
            Self::Bodies => "Bodies",
            Self::Senders => "Senders",
            Self::Execution => "Execution",
            Self::IntermediateHashes => "IntermediateHashes",
            Self::HashState => "HashState",
            Self::AccountHistoryIndex => "AccountHistoryIndex",
            Self::StorageHistoryIndex => "StorageHistoryIndex",
            Self::LogIndex => "LogIndex",
            Self::CallTraces => "CallTraces",
            Self::TxLookup => "TxLookup",
            Self::TxPool => "TxPool",
            Self::Finish => "Finish",
        }
        .as_bytes()
    }
}

pub enum BucketFlag {
    Default = 0x00,
    ReverseKey = 0x02,
    DupSort = 0x04,
    IntegerKey = 0x08,
    DupFixed = 0x10,
    IntegerDup = 0x20,
    ReverseDup = 0x40,
}

// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
pub const HEADER_PREFIX: &str = "h"; // block_num_u64 + hash -> header
pub const HEADER_TD_SUFFIX: &str = "t"; // block_num_u64 + hash + headerTDSuffix -> td
pub const HEADER_HASH_SUFFIX: &str = "n"; // block_num_u64 + headerHashSuffix -> hash
pub const HEADER_NUMBER_PREFIX: &str = "H"; // headerNumberPrefix + hash -> num (uint64 big endian)

pub const BLOCK_BODY_PREFIX: &str = "b"; // block_num_u64 + hash -> block body
pub const ETH_TX: &str = "eth_tx"; // tbl_sequence_u64 -> rlp(tx)
pub const BLOCK_RECEIPTS_PREFIX: &str = "r"; // block_num_u64 + hash -> block receipts
pub const LOG: &str = "log"; // block_num_u64 + hash -> block receipts

pub const CONFIG_PREFIX: &str = "ethereum-config-";

pub const SYNC_STAGE_PROGRESS: &str = "SSP2";

#[derive(Clone, Copy, Default)]
pub struct BucketConfigItem {
    pub flags: BucketFlags,
    // AutoDupSortKeysConversion - enables some keys transformation - to change db layout without changing app code.
    // Use it wisely - it helps to do experiments with DB format faster, but better reduce amount of Magic in app.
    // If good DB format found, push app code to accept this format and then disable this property.
    pub auto_dup_sort_keys_conversion: bool,
    pub is_deprecated: bool,
    pub dbi: DBI,
    // DupFromLen - if user provide key of this length, then next transformation applied:
    // v = append(k[DupToLen:], v...)
    // k = k[:DupToLen]
    // And opposite at retrieval
    // Works only if AutoDupSortKeysConversion enabled
    pub dup_from_len: u8,
    pub dup_to_len: u8,
    pub dup_fixed_size: u8,
    pub custom_comparator: CustomComparator,
    pub custom_dup_comparator: CustomComparator,
}

pub fn buckets_configs() -> HashMap<&'static str, BucketConfigItem> {
    hashmap! {
        "CurrentStateBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            auto_dup_sort_keys_conversion: true,
            dup_from_len: 72,
            dup_to_len: 40,
            ..Default::default()
        },
        "PlainAccountChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "PlainStorageChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "AccountChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "StorageChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "PlainStateBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            auto_dup_sort_keys_conversion: true,
            dup_from_len: 60,
            dup_to_len: 28,
            ..Default::default()
        },
        "IntermediateTrieHashBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            custom_dup_comparator: "dup_cmp_suffix32",
            ..Default::default()
        },
    }
}
