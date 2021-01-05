use ethereum_types::H256;
use maplit::hashmap;
use std::{collections::HashMap, fmt::Display, mem::size_of};

pub type BlockNumber = u64;
pub type Incarnation = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Bucket {
    PlainState,
    PlainContractCode,
    PlainAccountChangeSet,
    PlainStorageChangeSet,
    CurrentState,
    AccountsHistory,
    StorageHistory,
    Code,
    ContractCode,
    IncarnationMap,
    AccountChangeSet,
    StorageChangeSet,
    IntermediateTrieHash,
    DatabaseInfo,
    SnapshotInfo,
}

impl AsRef<str> for Bucket {
    fn as_ref(&self) -> &str {
        match self {
            Self::PlainState => "PLAIN-CST2",
            Self::PlainContractCode => "PLAIN-contractCode",
            Self::PlainAccountChangeSet => "PLAIN-ACS",
            Self::PlainStorageChangeSet => "PLAIN-SCS",
            Self::CurrentState => "CST2",
            Self::AccountsHistory => "hAT",
            Self::StorageHistory => "hST",
            Self::Code => "CODE",
            Self::ContractCode => "contractCode",
            Self::IncarnationMap => "incarnationMap",
            Self::AccountChangeSet => "ACS",
            Self::StorageChangeSet => "SCS",
            Self::IntermediateTrieHash => "iTh2",
            Self::DatabaseInfo => "DBINFO",
            Self::SnapshotInfo => "SNINFO",
        }
    }
}

impl Display for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

pub type BucketFlags = u8;
pub type DBI = u8;
pub type CustomComparator = &'static str;

const NUMBER_HASH_COMPOSITE_LEN: usize = size_of::<u64>() + H256::len_bytes();

pub type NumberHashCompositeKey = [u8; NUMBER_HASH_COMPOSITE_LEN];
pub type HeaderHashKey = [u8; size_of::<u64>() + 1];
pub type HeaderTDKey = [u8; NUMBER_HASH_COMPOSITE_LEN + 1];

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

pub fn number_hash_composite_key(number: u64, hash: H256) -> NumberHashCompositeKey {
    let mut v: NumberHashCompositeKey = [0; NUMBER_HASH_COMPOSITE_LEN];

    const SEPARATOR: usize = size_of::<u64>();

    v[..SEPARATOR].copy_from_slice(&number.to_be_bytes());
    v[SEPARATOR..].copy_from_slice(&hash.to_fixed_bytes());

    v
}

pub fn header_hash_key(block: u64) -> HeaderHashKey {
    let mut v: HeaderHashKey = Default::default();

    const SEPARATOR: usize = size_of::<u64>();

    v[..SEPARATOR].copy_from_slice(&block.to_be_bytes());
    v[SEPARATOR..].copy_from_slice(HEADER_HASH_SUFFIX.as_bytes());

    v
}

pub fn header_td_key(number: u64, hash: H256) -> HeaderTDKey {
    let mut v: HeaderTDKey = [0; NUMBER_HASH_COMPOSITE_LEN + 1];

    v[..NUMBER_HASH_COMPOSITE_LEN].copy_from_slice(&number_hash_composite_key(number, hash));
    v[NUMBER_HASH_COMPOSITE_LEN..].copy_from_slice(HEADER_TD_SUFFIX.as_bytes());

    v
}

pub fn bytes_mask(fixed_bits: u64) -> (u64, u8) {
    let fixed_bytes = (fixed_bits + 7) / 8;
    let shift_bits = fixed_bits & 7;
    let mut mask = 0xff;
    if shift_bits != 0 {
        mask = 0xff << (8 - shift_bits);
    }
    (fixed_bytes, mask)
}

pub fn change_set_by_index_bucket(storage: bool) -> (Bucket, usize) {
    if storage {
        (Bucket::PlainStorageChangeSet, 60)
    } else {
        (Bucket::PlainAccountChangeSet, 20)
    }
}
