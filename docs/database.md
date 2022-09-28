# Akula database schema

This is the Akula database schema.

Data is stored in a MDBX database which is sorted by key.

Keys and values are converted to bytes by `TableEncode` and decoded by `TableDecode`.

Each key may have multiple values which, for example, may correspond to different
block heights and transactions.

See:

[src/kv/tables.rs](../src/kv/tables.rs)


## Account

mapping: `Address => Account`

The current account value for an address.

```
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: H256, // hash of the bytecode
}
```

Accessor: `accessors::state::account`

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/stages/history_index.rs](../src/stages/history_index.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/accessors/state.rs](../src/accessors/state.rs)

[src/kv/tables.rs](../src/kv/tables.rs)

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## Storage

mapping: `Address => (H256, U256)`

For a given address this table represents a list of storage locations and values.

Accessor: `accessors::state::storage`

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/state/database.rs](../src/state/database.rs)

[src/stages/history_index.rs](../src/stages/history_index.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/accessors/state.rs](../src/accessors/state.rs)

[src/kv/tables.rs](../src/kv/tables.rs)

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## AccountChangeSet

mapping: `AccountChangeKey => AccountChange`

For a given block this table represents a list of changes to accounts as
a result of executing transactions.

Accessor: `accessors::state::account`

```
pub type AccountChangeKey = BlockNumber;

pub struct AccountChange {
    pub address: Address,
    pub account: Option<crate::models::Account>,
}
```

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/stages/history_index.rs](../src/stages/history_index.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/accessors/state.rs](../src/accessors/state.rs)

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## StorageChangeSet

mapping: `StorageChangeKey => StorageChange`

For a given block this table represents a list of storage location changes.

Sorted by BlockNumber.

Accessor: `accessors::state::storage`

```
pub struct StorageChangeKey {
    pub block_number: BlockNumber,
    pub address: Address,
}

pub struct StorageChange {
    pub location: H256,
    pub value: U256,
}
```

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/stages/history_index.rs](../src/stages/history_index.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/accessors/state.rs](../src/accessors/state.rs)

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## HashedAccount

mapping: `H256 => Account`

For a hashed address this table holds the corresponding account information.

```
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: H256, // hash of the bytecode
}
```

See:

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## HashedStorage

mapping: `H256 => (H256, U256)`

For a hashed address this table holds the corresponding account storage keys
and values.

See:

[src/state/database.rs](../src/state/database.rs)

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## AccountHistory

mapping: `BitmapKey<Address> => RoaringTreemap`

*todo:* What does the key and bitmap represent?

Accessor: `accessors::state::account`

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/stages/history_index.rs](../src/stages/history_index.rs)

[src/accessors/state.rs](../src/accessors/state.rs)


## StorageHistory

mapping: `BitmapKey<(Address, H256)> => RoaringTreemap`

*todo:* What does the key and bitmap represent?

Accessor: `accessors::state::storage`

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/stages/history_index.rs](../src/stages/history_index.rs)

[src/accessors/state.rs](../src/accessors/state.rs)


## Code

mapping: `H256 => Bytes`

For a code hash what is the bytecode?

Accessor: `accessors::state::code`

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/rpc/eth.rs](../src/rpc/eth.rs)

[src/accessors/state.rs](../src/accessors/state.rs)


## TrieAccount

mapping: `Vec<u8> => Vec<u8>`

Used in the hashing process.

See:

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## TrieStorage

mapping: `Vec<u8> => Vec<u8>`

Used in the hashing process.

See:

[src/trie/intermediate_hashes.rs](../src/trie/intermediate_hashes.rs)


## HeaderNumber

mapping: `H256 => BlockNumber`

For a particular block hash, what is the block number.

Accessor: `accessors::chain::header_number`

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/rpc/mod.rs](../src/rpc/mod.rs)

[src/stages/block_hashes.rs](../src/stages/block_hashes.rs)

[src/stages/headers.rs](../src/stages/headers.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)

[src/p2p/node/stash.rs](../src/p2p/node/stash.rs)

[src/etl/collector.rs](../src/etl/collector.rs)


## CanonicalHeader

mapping: `BlockNumber => H256`

For a given block number, what is the hash?

Accessor: `accessors::chain::canonical_hash`

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/rpc/mod.rs](../src/rpc/mod.rs)

[src/stages/bodies.rs](../src/stages/bodies.rs)

[src/stages/total_gas_index.rs](../src/stages/total_gas_index.rs)

[src/stages/interhashes.rs](../src/stages/interhashes.rs)

[src/stages/sender_recovery.rs](../src/stages/sender_recovery.rs)

[src/stages/total_tx_index.rs](../src/stages/total_tx_index.rs)

[src/stages/block_hashes.rs](../src/stages/block_hashes.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/headers.rs](../src/stages/headers.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)

[src/p2p/node/stash.rs](../src/p2p/node/stash.rs)


## Header

mapping: `HeaderKey => BlockHeader`

Sorted by BlockNumber

```
pub type HeaderKey = (BlockNumber, H256);

pub struct BlockHeader {
    pub parent_hash: H256,
    pub ommers_hash: H256,
    pub beneficiary: H160,
    pub state_root: H256,
    pub transactions_root: H256,
    pub receipts_root: H256,
    pub logs_bloom: Bloom,
    pub difficulty: U256,
    pub number: BlockNumber,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes,
    pub mix_hash: H256,
    pub nonce: H64,
    pub base_fee_per_gas: Option<U256>,
}
```

Accessor: `accessors::chain::header`

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/state/buffer.rs](../src/state/buffer.rs)

[src/rpc/mod.rs](../src/rpc/mod.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/consensus/clique/mod.rs](../src/consensus/clique/mod.rs)

[src/stages/bodies.rs](../src/stages/bodies.rs)

[src/stages/total_gas_index.rs](../src/stages/total_gas_index.rs)

[src/stages/interhashes.rs](../src/stages/interhashes.rs)

[src/stages/block_hashes.rs](../src/stages/block_hashes.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/headers.rs](../src/stages/headers.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)

[src/p2p/node/stash.rs](../src/p2p/node/stash.rs)

[src/etl/collector.rs](../src/etl/collector.rs)


## HeadersTotalDifficulty

mapping: `HeaderKey => U256`

Accessor: `accessors::chain::td`

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/stages/headers.rs](../src/stages/headers.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)


## BlockBody

mapping: `HeaderKey => BodyForStorage`

Sorted by BlockNumber

Accessor: `accessors::chain::storage_body`

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/rpc/mod.rs](../src/rpc/mod.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/stages/bodies.rs](../src/stages/bodies.rs)

[src/stages/sender_recovery.rs](../src/stages/sender_recovery.rs)

[src/stages/total_tx_index.rs](../src/stages/total_tx_index.rs)

[src/stages/tx_lookup.rs](../src/stages/tx_lookup.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)


## BlockTransaction

mapping: `TxIndex => MessageWithSignature`

Accessor: `accessors::chain::tx`

```
struct TxIndex(u64);

pub struct MessageWithSignature {
    pub message: Message,
    pub signature: MessageSignature,
}
```

See:

[src/stages/bodies.rs](../src/stages/bodies.rs)

[src/stages/sender_recovery.rs](../src/stages/sender_recovery.rs)

[src/stages/tx_lookup.rs](../src/stages/tx_lookup.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)


## TotalGas

mapping: `BlockNumber => u64`

For a particular block number, what is the total gas.

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/stages/total_gas_index.rs](../src/stages/total_gas_index.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/hashstate.rs](../src/stages/hashstate.rs)

[src/stages/stage_util.rs](../src/stages/stage_util.rs)


## TotalTx

mapping: `BlockNumber => u64`

How many transactions are in this block?

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/stages/sender_recovery.rs](../src/stages/sender_recovery.rs)

[src/stages/total_tx_index.rs](../src/stages/total_tx_index.rs)


## LogAddressIndex

mapping: `Address => RoaringTreemap`

## LogAddressesByBlock

mapping: `BlockNumber => Address`

For a block number, which addresses emit logs.

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/stages/execution.rs](../src/stages/execution.rs)


## LogTopicIndex

mapping: `H256 => RoaringTreemap`

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/stages/execution.rs](../src/stages/execution.rs)


## LogTopicsByBlock

mapping: `BlockNumber => H256`

*todo* presumably a list of log topics indexd by block.

See:

[src/state/buffer.rs](../src/state/buffer.rs)

[src/stages/execution.rs](../src/stages/execution.rs)


## CallTraceSet

mapping: `BlockNumber => CallTraceSetEntry`

```
pub struct CallTraceSetEntry {
    pub address: Address,
    pub from: bool,
    pub to: bool,
}
```

Records calls for tracing.

See:

[src/stages/execution.rs](../src/stages/execution.rs)

[src/stages/call_trace_index.rs](../src/stages/call_trace_index.rs)


## CallFromIndex

mapping: `BitmapKey<Address> => RoaringTreemap`

Sparse bitmap of blocks with 'from' addresses.

See:

[src/rpc/trace.rs](../src/rpc/trace.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/stages/call_trace_index.rs](../src/stages/call_trace_index.rs)


## CallToIndex

mapping: `BitmapKey<Address> => RoaringTreemap`

Sparse bitmap of blocks with 'to' addresses.

See:

[src/rpc/trace.rs](../src/rpc/trace.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/stages/call_trace_index.rs](../src/stages/call_trace_index.rs)


## BlockTransactionLookup

mapping: `H256 => TruncateStart<BlockNumber>`

```
// Removes zeros from an encoding.
pub struct TruncateStart<T>(pub T);
```

Accessor: `accessors::chain::tl`

See:

[src/stages/tx_lookup.rs](../src/stages/tx_lookup.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)


## Config

mapping: `() => ChainSpec`

Single entry for chain-specific information.

```
pub struct ChainSpec {
    pub name: String,
    pub consensus: ConsensusParams,
    pub upgrades: Upgrades,
    pub params: Params,
    pub genesis: Genesis,
    pub contracts: BTreeMap<BlockNumber, HashMap<Address, Contract>>,
    pub balances: BTreeMap<BlockNumber, HashMap<Address, U256>>,
    pub p2p: P2PParams,
}
```

See:

[src/state/genesis.rs](../src/state/genesis.rs)

[src/rpc/trace.rs](../src/rpc/trace.rs)

[src/rpc/otterscan.rs](../src/rpc/otterscan.rs)

[src/rpc/eth.rs](../src/rpc/eth.rs)

[src/stages/execution.rs](../src/stages/execution.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)


## SyncStage

mapping: `StageId => BlockNumber`

What is the progress for each sync stage.

```
pub struct StageId(pub &'static str);

pub const BODIES: StageId = StageId("Bodies");
pub const TOTAL_GAS_INDEX: StageId = StageId("TotalGasIndex");
pub const ACCOUNT_HISTORY_INDEX: StageId = StageId("AccountHistoryIndex");
pub const STORAGE_HISTORY_INDEX: StageId = StageId("StorageHistoryIndex");
pub const INTERMEDIATE_HASHES: StageId = StageId("IntermediateHashes");
pub const SENDERS: StageId = StageId("SenderRecovery");
pub const TOTAL_TX_INDEX: StageId = StageId("TotalTxIndex");
pub const BLOCK_HASHES: StageId = StageId("BlockHashes");
pub const EXECUTION: StageId = StageId("Execution");
pub const HEADERS: StageId = StageId("Headers");
pub const FINISH: StageId = StageId("Finish");
pub const TX_LOOKUP: StageId = StageId("TxLookup");
pub const CALL_TRACES: StageId = StageId("CallTraces");
pub const HASH_STATE: StageId = StageId("HashState");
```

See:

[src/rpc/mod.rs](../src/rpc/mod.rs)

[src/rpc/eth.rs](../src/rpc/eth.rs)

[src/stagedsync/stage.rs](../src/stagedsync/stage.rs)


## PruneProgress

mapping: `StageId => BlockNumber`

What is the progress for the prune process.

See:

[src/rpc/mod.rs](../src/rpc/mod.rs)

[src/stagedsync/stage.rs](../src/stagedsync/stage.rs)


## TxSender

mapping: `HeaderKey => Vec<Address>`

Accessor: `accessors::chain::tx_sender`

Used by `sender_recovery.rs`

See:

[src/stages/sender_recovery.rs](../src/stages/sender_recovery.rs)

[src/accessors/chain.rs](../src/accessors/chain.rs)


## LastHeader

mapping: `() => HeaderKey`

The last block in the chain. (Currently always 0).

See:

[src/state/genesis.rs](../src/state/genesis.rs)


## Issuance

mapping: `Vec<u8> => Vec<u8>`

Not used at present. Possibly for Otterscan.

```
pub struct Issuance {
    pub block_reward: U256,
    pub uncle_reward: U256,
    pub issuance: U256,
}
```

## Version

mapping: `() => u64`

The version of the database. Used for future migrations.

See:

[src/state/database_version.rs](../src/state/database_version.rs)


