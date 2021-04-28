use maplit::hashmap;
use std::collections::HashMap;

pub trait Table: 'static {
    const DB_NAME: &'static str;
}

pub trait DupSort: Table {}

macro_rules! decl_tables {
    (($count:expr, $all_tables:expr) => /* nothing left */) => {
        const COUNT: usize = $count;

        fn all_tables() -> impl Iterator<Item = &'static str> {
            $all_tables.split(' ')
        }
    };
    (($count:expr, $all_tables:expr) => $name:ident => $db_name:expr, $($tail:tt)*) => {
        decl_tables!(($count, const_format::concatcp!($all_tables, " ", $db_name)) => $($tail)*);

        #[derive(Clone, Copy, Debug)]
        pub struct $name;

        impl $crate::dbutils::Table for $name {
            const DB_NAME: &'static str = $db_name;
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                <Self as $crate::dbutils::Table>::DB_NAME
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", <Self as $crate::dbutils::Table>::DB_NAME)
            }
        }
    };
    ($name:ident => $db_name:expr, $($tail:tt)*) => {
        decl_tables!((0, "") => $name => $db_name, $($tail)*);
    }
}

pub mod tables {
    use std::collections::HashMap;

    use once_cell::sync::Lazy;

    use crate::Table;

    use super::DupSort;

    decl_tables!(
        AccountsHistory => "hAT",
        StorageHistory => "hST",
        Code => "CODE",
        ContractCode => "contractCode",
        DatabaseVersion => "DatabaseVersion",
        HeaderNumber => "H", // hash -> num (uint64 big endian)
        BlockBody => "b", // block_num_u64 + hash -> block body
        BlockReceipts => "r", // block_num_u64 + hash -> block receipts
        TxLookup => "l",
        BloomBits => "B",
        Preimage => "secure-key-", // hash -> preimage
        Config => "ethereum-config-", // config prefix for the db
        BloomBitsIndex => "iB",
        DatabaseInfo => "DBINFO",
        IncarnationMap => "incarnationMap",
        Clique => "clique-",
        SyncStageProgress => "SSP2",
        SyncStageUnwind => "SSU2",
        PlainState => "PLAIN-CST2",
        PlainContractCode => "PLAIN-contractCode",
        PlainAccountChangeSet => "PLAIN-ACS",
        PlainStorageChangeSet => "PLAIN-SCS",
        Senders => "txSenders",
        FastTrieProgress => "TrieSync",
        HeadBlock => "LastBlock",
        HeadFastBlock => "LastFast",
        HeadHeader => "LastHeader",
        LogTopicIndex => "log_topic_index",
        LogAddressIndex => "log_address_index",
        SnapshotInfo => "SNINFO",
        HeadersSnapshotInfo => "hSNINFO",
        BodiesSnapshotInfo => "bSNINFO",
        StateSnapshotInfo => "sSNINFO",
        CallFromIndex => "call_from_index",
        CallToIndex => "call_to_index",
        Log => "log", // block_num_u64 + hash -> block receipts
        Sequence => "sequence",
        EthTx => "eth_tx", // tbl_sequence_u64 -> rlp(tx)
        TrieOfAccounts => "trie_account",
        TrieOfStorage => "trie_storage",
        HashedAccounts => "hashed_accounts",
        HashedStorage => "hashed_storage",
        HeaderCanonical => "canonical_headers",
        Headers => "headers",
        HeaderTD => "header_to_td",
    );

    impl DupSort for HashedStorage {}
    impl DupSort for PlainAccountChangeSet {}
    impl DupSort for PlainStorageChangeSet {}
    impl DupSort for PlainState {}

    pub const DUPSORT_TABLES: &[&str] = &[
        HashedStorage::DB_NAME,
        PlainAccountChangeSet::DB_NAME,
        PlainStorageChangeSet::DB_NAME,
        PlainState::DB_NAME,
    ];

    pub static TABLE_MAP: Lazy<HashMap<&'static str, bool>> = Lazy::new(|| {
        let mut v = HashMap::with_capacity(COUNT);
        for table in all_tables() {
            v.insert(table, false);
        }

        for table in DUPSORT_TABLES {
            v.insert(table, true);
        }

        v
    });
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

// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
pub const HEADER_TD_SUFFIX: &str = "t"; // block_num_u64 + hash + headerTDSuffix -> td
pub const HEADER_HASH_SUFFIX: &str = "n"; // block_num_u64 + headerHashSuffix -> hash
