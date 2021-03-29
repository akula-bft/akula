use maplit::hashmap;
use std::collections::HashMap;

pub trait Table: 'static {
    const DB_NAME: &'static str;
}

pub trait DupSort: Table {}

macro_rules! decl_table {
    ($name:ident, $db_name:expr) => {
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
}

pub mod tables {
    use super::DupSort;

    decl_table!(AccountsHistory, "hAT");
    decl_table!(StorageHistory, "hST");
    decl_table!(Code, "CODE");
    decl_table!(ContractCode, "contractCode");
    decl_table!(DatabaseVersion, "DatabaseVersion");
    decl_table!(HeaderNumber, "H"); // hash -> num (uint64 big endian)
    decl_table!(BlockBody, "b"); // block_num_u64 + hash -> block body
    decl_table!(BlockReceipts, "r"); // block_num_u64 + hash -> block receipts
    decl_table!(TxLookup, "l");
    decl_table!(BloomBits, "B");
    decl_table!(Preimage, "secure-key-"); // hash -> preimage
    decl_table!(Config, "ethereum-config-"); // config prefix for the db
    decl_table!(BloomBitsIndex, "iB");
    decl_table!(DatabaseInfo, "DBINFO");
    decl_table!(IncarnationMap, "incarnationMap");
    decl_table!(Clique, "clique-");
    decl_table!(SyncStageProgress, "SSP2");
    decl_table!(SyncStageUnwind, "SSU2");
    decl_table!(PlainState, "PLAIN-CST2");
    decl_table!(PlainContractCode, "PLAIN-contractCode");
    decl_table!(PlainAccountChangeSet, "PLAIN-ACS");
    decl_table!(PlainStorageChangeSet, "PLAIN-SCS");
    decl_table!(Senders, "txSenders");
    decl_table!(FastTrieProgress, "TrieSync");
    decl_table!(HeadBlock, "LastBlock");
    decl_table!(HeadFastBlock, "LastFast");
    decl_table!(HeadHeader, "LastHeader");
    decl_table!(LogTopicIndex, "log_topic_index");
    decl_table!(LogAddressIndex, "log_address_index");
    decl_table!(SnapshotInfo, "SNINFO");
    decl_table!(HeadersSnapshotInfo, "hSNINFO");
    decl_table!(BodiesSnapshotInfo, "bSNINFO");
    decl_table!(StateSnapshotInfo, "sSNINFO");
    decl_table!(CallFromIndex, "call_from_index");
    decl_table!(CallToIndex, "call_to_index");
    decl_table!(Log, "log"); // block_num_u64 + hash -> block receipts
    decl_table!(Sequence, "sequence");
    decl_table!(EthTx, "eth_tx"); // tbl_sequence_u64 -> rlp(tx)
    decl_table!(TrieOfAccounts, "trie_account");
    decl_table!(TrieOfStorage, "trie_storage");
    decl_table!(HashedAccounts, "hashed_accounts");
    decl_table!(HashedStorage, "hashed_storage");
    decl_table!(HeaderCanonical, "canonical_headers");
    decl_table!(Headers, "headers");
    decl_table!(HeaderTD, "header_to_td");

    impl DupSort for HashedStorage {}
    impl DupSort for PlainAccountChangeSet {}
    impl DupSort for PlainStorageChangeSet {}
    impl DupSort for PlainState {}
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
