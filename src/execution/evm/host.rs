use crate::execution::{
    evm::EvmSubMemory,
    tracer::{NoopTracer, Tracer},
};

use super::{
    common::{InterpreterMessage, Output},
    CreateMessage, StatusCode,
};
use bytes::Bytes;
use ethereum_types::Address;
use ethnum::U256;

/// State access status (EIP-2929).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessStatus {
    Cold,
    Warm,
}

impl Default for AccessStatus {
    fn default() -> Self {
        Self::Cold
    }
}

#[derive(Clone, Copy, Debug)]
pub enum StorageStatus {
    /// The value of a storage item has been left unchanged: 0 -> 0 and X -> X.
    Unchanged,
    /// The value of a storage item has been modified: X -> Y.
    Modified,
    /// A storage item has been modified after being modified before: X -> Y -> Z.
    ModifiedAgain,
    /// A new storage item has been added: 0 -> X.
    Added,
    /// A storage item has been deleted: X -> 0.
    Deleted,
}

/// The transaction and block data for execution.
#[derive(Clone, Debug)]
pub struct TxContext {
    /// The transaction gas price.
    pub tx_gas_price: U256,
    /// The transaction origin account.
    pub tx_origin: Address,
    /// The miner of the block.
    pub block_coinbase: Address,
    /// The block number.
    pub block_number: u64,
    /// The block timestamp.
    pub block_timestamp: u64,
    /// The block gas limit.
    pub block_gas_limit: u64,
    /// The block difficulty.
    pub block_difficulty: U256,
    /// The blockchain's ChainID.
    pub chain_id: U256,
    /// The block base fee per gas (EIP-1559, EIP-3198).
    pub block_base_fee: U256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call<'a> {
    Call(&'a InterpreterMessage),
    Create(&'a CreateMessage),
}

/// Abstraction that exposes host context to EVM.
pub trait Host {
    fn trace_instructions(&self) -> bool {
        false
    }
    fn tracer(&mut self, mut f: impl FnMut(&mut dyn Tracer)) {
        (f)(&mut NoopTracer)
    }
    /// Check if an account exists.
    fn account_exists(&mut self, address: Address) -> bool;
    /// Get value of a storage key.
    ///
    /// Returns `Ok(U256::zero())` if does not exist.
    fn get_storage(&mut self, address: Address, key: U256) -> U256;
    /// Set value of a storage key.
    fn set_storage(&mut self, address: Address, key: U256, value: U256) -> StorageStatus;
    /// Get balance of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_balance(&mut self, address: Address) -> U256;
    /// Get code size of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_size(&mut self, address: Address) -> U256;
    /// Get code hash of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_hash(&mut self, address: Address) -> U256;
    /// Copy code of an account.
    ///
    /// Returns `Ok(0)` if offset is invalid.
    fn copy_code(&mut self, address: Address, offset: usize, buffer: &mut [u8]) -> usize;
    /// Self-destruct account.
    fn selfdestruct(&mut self, address: Address, beneficiary: Address);
    /// Call to another account.
    fn call(&mut self, msg: Call, mem: EvmSubMemory) -> Output;
    /// Retrieve transaction context.
    fn get_tx_context(&mut self) -> Result<TxContext, StatusCode>;
    /// Get block hash.
    ///
    /// Returns `Ok(U256::zero())` if block does not exist.
    fn get_block_hash(&mut self, block_number: u64) -> U256;
    /// Emit a log.
    fn emit_log(&mut self, address: Address, data: Bytes, topics: &[U256]);
    /// Mark account as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_account(&mut self, address: Address) -> AccessStatus;
    /// Mark storage key as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_storage(&mut self, address: Address, key: U256) -> AccessStatus;
}
