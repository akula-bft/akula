use core::fmt::Debug;

pub trait Table: Debug + 'static {
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

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", stringify!($name))
            }
        }
    };
    ($name:ident => $db_name:expr, $($tail:tt)*) => {
        decl_tables!((0, "") => $name => $db_name, $($tail)*);
    }
}

pub mod tables {
    use super::*;
    use crate::Table;
    use once_cell::sync::Lazy;
    use std::collections::HashMap;

    decl_tables!(
        AccountsHistory => "hAT",
        StorageHistory => "hST",
        Code => "CODE",
        ContractCode => "contractCode",
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
        CliqueSeparate => "clique-snapshots-",
        CliqueLastSnapshot => "lastSnap",
        CliqueSnapshot => "snap",
        SyncStageProgress => "SSP2",
        SyncStageUnwind => "SSU2",
        PlainState => "PLAIN-CST2",
        PlainContractCode => "PLAIN-contractCode",
        AccountChangeSet => "PLAIN-ACS",
        StorageChangeSet => "PLAIN-SCS",
        Senders => "txSenders",
        HeadBlock => "LastBlock",
        HeadHeader => "LastHeader",
        Migrations => "migrations",
        LogTopicIndex => "log_topic_index",
        LogAddressIndex => "log_address_index",
        SnapshotInfo => "SNINFO",
        HeadersSnapshotInfo => "hSNINFO",
        BodiesSnapshotInfo => "bSNINFO",
        StateSnapshotInfo => "sSNINFO",
        CallTraceSet => "call_trace_set",
        CallFromIndex => "call_from_index",
        CallToIndex => "call_to_index",
        Log => "log", // block_num_u64 + hash -> block receipts
        Sequence => "sequence",
        EthTx => "eth_tx", // tbl_sequence_u64 -> rlp(tx)
        TrieOfAccounts => "trie_account",
        TrieOfStorage => "trie_storage",
        HashedAccounts => "hashed_accounts",
        HashedStorage => "hashed_storage",
        BittorrentInfoBucket => "BTINFO",
        HeaderCanonical => "canonical_headers",
        Headers => "headers",
        HeaderTD => "header_to_td",
    );

    impl DupSort for HashedStorage {}
    impl DupSort for AccountChangeSet {}
    impl DupSort for StorageChangeSet {}
    impl DupSort for PlainState {}
    impl DupSort for CallTraceSet {}

    pub const DUPSORT_TABLES: &[&str] = &[
        HashedStorage::DB_NAME,
        AccountChangeSet::DB_NAME,
        StorageChangeSet::DB_NAME,
        PlainState::DB_NAME,
        CallTraceSet::DB_NAME,
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

    pub struct AutoDupSort {
        pub from: usize,
        pub to: usize,
    }

    pub static AUTO_DUP_SORT: Lazy<HashMap<&'static str, AutoDupSort>> = Lazy::new(|| {
        let mut v = HashMap::new();
        v.insert(HashedStorage::DB_NAME, AutoDupSort { from: 72, to: 40 });
        v.insert(PlainState::DB_NAME, AutoDupSort { from: 60, to: 28 });
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
