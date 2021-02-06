use maplit::hashmap;
use std::collections::HashMap;

pub trait Bucket {
    const DB_NAME: &'static str;
}

pub trait DupSort {
    const AUTO_KEYS_CONVERSION: bool = false;
    const CUSTOM_DUP_COMPARATOR: Option<&'static str> = None;
    const DUP_FROM_LEN: Option<usize> = None;
    const DUP_TO_LEN: Option<usize> = None;
}

pub trait DupFixed {}

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
    use super::DupSort;

    decl_bucket!(CurrentState, "CST2");
    decl_bucket!(AccountsHistory, "hAT");
    decl_bucket!(StorageHistory, "hST");
    decl_bucket!(Code, "CODE");
    decl_bucket!(ContractCode, "contractCode");
    decl_bucket!(IntermediateTrieHash, "iTh2");
    decl_bucket!(DatabaseVersion, "DatabaseVersion");
    decl_bucket!(Header, "h"); // block_num_u64 + hash -> header
    decl_bucket!(HeaderNumber, "H"); // hash -> num (uint64 big endian)
    decl_bucket!(BlockBody, "b"); // block_num_u64 + hash -> block body
    decl_bucket!(BlockReceipts, "r"); // block_num_u64 + hash -> block receipts
    decl_bucket!(TxLookup, "l");
    decl_bucket!(BloomBits, "B");
    decl_bucket!(Preimage, "secure-key-"); // hash -> preimage
    decl_bucket!(Config, "ethereum-config-"); // config prefix for the db
    decl_bucket!(BloomBitsIndex, "iB");
    decl_bucket!(DatabaseInfo, "DBINFO");
    decl_bucket!(IncarnationMap, "incarnationMap");
    decl_bucket!(Clique, "clique-");
    decl_bucket!(SyncStageProgress, "SSP2");
    decl_bucket!(SyncStageUnwind, "SSU2");
    decl_bucket!(PlainState, "PLAIN-CST2");
    decl_bucket!(PlainContractCode, "PLAIN-contractCode");
    decl_bucket!(PlainAccountChangeSet, "PLAIN-ACS");
    decl_bucket!(PlainStorageChangeSet, "PLAIN-SCS");
    decl_bucket!(Senders, "txSenders");
    decl_bucket!(FastTrieProgress, "TrieSync");
    decl_bucket!(HeadBlock, "LastBlock");
    decl_bucket!(HeadFastBlock, "LastFast");
    decl_bucket!(HeadHeader, "LastHeader");
    decl_bucket!(Migrations, "migrations");
    decl_bucket!(LogTopicIndex, "log_topic_index");
    decl_bucket!(LogAddressIndex, "log_address_index");
    decl_bucket!(SnapshotInfo, "SNINFO");
    decl_bucket!(HeadersSnapshotInfoBucket, "hSNINFO");
    decl_bucket!(BodiesSnapshotInfoBucket, "bSNINFO");
    decl_bucket!(StateSnapshotInfoBucket, "sSNINFO");
    decl_bucket!(CallFromIndex, "call_from_index");
    decl_bucket!(CallToIndex, "call_to_index");
    decl_bucket!(Log, "log"); // block_num_u64 + hash -> block receipts
    decl_bucket!(Sequence, "sequence");
    decl_bucket!(EthTx, "eth_tx"); // tbl_sequence_u64 -> rlp(tx)

    impl DupSort for CurrentState {
        const AUTO_KEYS_CONVERSION: bool = true;
        const DUP_FROM_LEN: Option<usize> = Some(72);
        const DUP_TO_LEN: Option<usize> = Some(40);
    }
    impl DupSort for PlainAccountChangeSet {}
    impl DupSort for PlainStorageChangeSet {}
    impl DupSort for PlainState {
        const AUTO_KEYS_CONVERSION: bool = true;
        const DUP_FROM_LEN: Option<usize> = Some(60);
        const DUP_TO_LEN: Option<usize> = Some(28);
    }
    impl DupSort for IntermediateTrieHash {
        const CUSTOM_DUP_COMPARATOR: Option<&'static str> = Some("dup_cmp_suffix32");
    }
}

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
pub const HEADER_TD_SUFFIX: &str = "t"; // block_num_u64 + hash + headerTDSuffix -> td
pub const HEADER_HASH_SUFFIX: &str = "n"; // block_num_u64 + headerHashSuffix -> hash
