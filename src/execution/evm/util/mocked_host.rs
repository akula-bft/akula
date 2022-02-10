use crate::execution::evm::{host::*, *};
use bytes::Bytes;
use ethereum_types::*;
use ethnum::U256;
use hex_literal::hex;
use parking_lot::Mutex;
use std::{cmp::min, collections::HashMap};

/// LOG record.
#[derive(Clone, Debug, PartialEq)]
pub struct LogRecord {
    /// The address of the account which created the log.
    pub creator: Address,

    /// The data attached to the log.
    pub data: Bytes,

    /// The log topics.
    pub topics: Vec<U256>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SelfdestructRecord {
    /// The address of the account which has self-destructed.
    pub selfdestructed: Address,

    /// The address of the beneficiary account.
    pub beneficiary: Address,
}

#[derive(Clone, Debug, Default)]
pub struct StorageValue {
    pub value: U256,
    pub dirty: bool,
    pub access_status: AccessStatus,
}

#[derive(Clone, Debug, Default)]
pub struct Account {
    /// The account nonce.
    pub nonce: u64,
    /// The account code.
    pub code: Bytes,
    /// The code hash. Can be a value not related to the actual code.
    pub code_hash: U256,
    /// The account balance.
    pub balance: U256,
    /// The account storage map.
    pub storage: HashMap<U256, StorageValue>,
}

const MAX_RECORDED_ACCOUNT_ACCESSES: usize = 200;
const MAX_RECORDED_CALLS: usize = 100;

#[derive(Clone, Debug, Default)]
pub struct Records {
    /// The copy of call inputs for the recorded_calls record.
    pub call_inputs: Vec<Bytes>,

    pub blockhashes: Vec<u64>,
    pub account_accesses: Vec<Address>,
    pub calls: Vec<InterpreterMessage>,
    pub logs: Vec<LogRecord>,
    pub selfdestructs: Vec<SelfdestructRecord>,
}

#[derive(Debug)]
pub struct MockedHost {
    pub accounts: HashMap<Address, Account>,
    pub tx_context: TxContext,
    pub block_hash: U256,
    pub call_result: Output,
    pub recorded: Mutex<Records>,
}

impl Clone for MockedHost {
    fn clone(&self) -> Self {
        Self {
            accounts: self.accounts.clone(),
            tx_context: self.tx_context.clone(),
            block_hash: self.block_hash,
            call_result: self.call_result.clone(),
            recorded: Mutex::new(self.recorded.lock().clone()),
        }
    }
}

impl Default for MockedHost {
    fn default() -> Self {
        Self {
            accounts: Default::default(),
            tx_context: TxContext {
                tx_gas_price: U256::ZERO,
                tx_origin: Address::zero(),
                block_coinbase: Address::zero(),
                block_number: 0,
                block_timestamp: 0,
                block_gas_limit: 0,
                block_difficulty: U256::ZERO,
                chain_id: U256::ZERO,
                block_base_fee: U256::ZERO,
            },
            block_hash: U256::ZERO,
            call_result: Output {
                status_code: StatusCode::Success,
                gas_left: 0,
                output_data: Bytes::new(),
                create_address: Some(Address::zero()),
            },
            recorded: Default::default(),
        }
    }
}

impl Records {
    fn record_account_access(&mut self, address: Address) {
        if self.account_accesses.len() < MAX_RECORDED_ACCOUNT_ACCESSES {
            self.account_accesses.push(address)
        }
    }
}

impl Host for MockedHost {
    fn account_exists(&self, address: ethereum_types::Address) -> bool {
        self.recorded.lock().record_account_access(address);
        self.accounts.contains_key(&address)
    }

    fn get_storage(&self, address: ethereum_types::Address, key: U256) -> U256 {
        self.recorded.lock().record_account_access(address);

        self.accounts
            .get(&address)
            .and_then(|account| account.storage.get(&key).map(|value| value.value))
            .unwrap_or(U256::ZERO)
    }

    fn set_storage(
        &mut self,
        address: ethereum_types::Address,
        key: U256,
        value: U256,
    ) -> StorageStatus {
        self.recorded.lock().record_account_access(address);

        // Get the reference to the old value.
        // This will create the account in case it was not present.
        // This is convenient for unit testing and standalone EVM execution to preserve the
        // storage values after the execution terminates.
        let old = self
            .accounts
            .entry(address)
            .or_default()
            .storage
            .entry(key)
            .or_default();

        // Follow https://eips.ethereum.org/EIPS/eip-1283 specification.
        // WARNING! This is not complete implementation as refund is not handled here.

        if old.value == value {
            return StorageStatus::Unchanged;
        }

        let status = if !old.dirty {
            old.dirty = true;
            if old.value == 0 {
                StorageStatus::Added
            } else if value != 0 {
                StorageStatus::Modified
            } else {
                StorageStatus::Deleted
            }
        } else {
            StorageStatus::ModifiedAgain
        };

        old.value = value;

        status
    }

    fn get_balance(&self, address: ethereum_types::Address) -> ethnum::U256 {
        self.recorded.lock().record_account_access(address);

        self.accounts
            .get(&address)
            .map(|acc| acc.balance)
            .unwrap_or(U256::ZERO)
    }

    fn get_code_size(&self, address: ethereum_types::Address) -> ethnum::U256 {
        self.recorded.lock().record_account_access(address);

        self.accounts
            .get(&address)
            .map(|acc| u128::try_from(acc.code.len()).unwrap().into())
            .unwrap_or(U256::ZERO)
    }

    fn get_code_hash(&self, address: ethereum_types::Address) -> U256 {
        self.recorded.lock().record_account_access(address);

        self.accounts
            .get(&address)
            .map(|acc| acc.code_hash)
            .unwrap_or(U256::ZERO)
    }

    fn copy_code(&self, address: Address, code_offset: usize, buffer: &mut [u8]) -> usize {
        self.recorded.lock().record_account_access(address);

        self.accounts
            .get(&address)
            .map(|acc| {
                let code = &acc.code;

                if code_offset >= code.len() {
                    return 0;
                }

                let n = min(buffer.len(), code.len() - code_offset);

                buffer[..n].copy_from_slice(&code[code_offset..code_offset + n]);

                n
            })
            .unwrap_or(0)
    }

    fn selfdestruct(
        &mut self,
        address: ethereum_types::Address,
        beneficiary: ethereum_types::Address,
    ) {
        let mut r = self.recorded.lock();

        r.record_account_access(address);
        r.selfdestructs.push(SelfdestructRecord {
            selfdestructed: address,
            beneficiary,
        });
    }

    fn call(&mut self, msg: &InterpreterMessage) -> Output {
        let mut r = self.recorded.lock();

        r.record_account_access(msg.recipient);

        if r.calls.len() < MAX_RECORDED_CALLS {
            r.calls.push(msg.clone());
            let call_msg = msg;
            if !call_msg.input_data.is_empty() {
                r.call_inputs.push(call_msg.input_data.clone());
            }
        }
        self.call_result.clone()
    }

    fn get_tx_context(&self) -> TxContext {
        self.tx_context.clone()
    }

    fn get_block_hash(&self, block_number: u64) -> U256 {
        self.recorded.lock().blockhashes.push(block_number);
        self.block_hash
    }

    fn emit_log(&mut self, address: ethereum_types::Address, data: &[u8], topics: &[U256]) {
        self.recorded.lock().logs.push(LogRecord {
            creator: address,
            data: data.to_vec().into(),
            topics: topics.to_vec(),
        });
    }

    fn access_account(&mut self, address: ethereum_types::Address) -> AccessStatus {
        let mut r = self.recorded.lock();

        // Check if the address have been already accessed.
        let already_accessed = r.account_accesses.iter().any(|&a| a == address);

        r.record_account_access(address);

        if address.0 >= hex!("0000000000000000000000000000000000000001")
            && address.0 <= hex!("0000000000000000000000000000000000000009")
        {
            return AccessStatus::Warm;
        }

        if already_accessed {
            AccessStatus::Warm
        } else {
            AccessStatus::Cold
        }
    }

    fn access_storage(&mut self, address: ethereum_types::Address, key: U256) -> AccessStatus {
        let value = self
            .accounts
            .entry(address)
            .or_default()
            .storage
            .entry(key)
            .or_default();
        let access_status = value.access_status;
        value.access_status = AccessStatus::Warm;
        access_status
    }
}
