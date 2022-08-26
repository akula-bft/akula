// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of OpenEthereum.

// OpenEthereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// OpenEthereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with OpenEthereum.  If not, see <http://www.gnu.org/licenses/>.

//! Implementation of the Parlia POSA Engine.
#![allow(missing_docs)]
mod contract_upgrade;
mod params;
mod snapshot;
pub mod util;

use block::ExecutedBlock;
use client::{BlockId, EngineClient};
use engines::parlia::contract_upgrade::upgrade_build_in_system_contract;
use engines::{
    parlia::{
        params::ParliaParams,
        snapshot::Snapshot,
        util::{is_system_transaction, recover_creator},
    },
    Engine, EngineError,
};
use error::{BlockError, Error};
use ethabi::FunctionOutputDecoder;
use ethereum_types::{Address, H256, U256};
use hash::KECCAK_EMPTY_LIST_RLP;
use kvdb::KeyValueDB;
use lru_cache::LruCache;
use machine::{Call, EthereumMachine};
use parking_lot::RwLock;
use std::{
    collections::BTreeSet,
    ops::{Add, Mul},
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};
use types::{
    header::{ExtendedHeader, Header},
    transaction::{Action, Transaction},
};
use unexpected::{Mismatch, OutOfBounds};

// Protocol constants
/// Fixed number of extra-data prefix bytes reserved for signer vanity
pub const VANITY_LENGTH: usize = 32;
/// Fixed number of extra-data suffix bytes reserved for signer signature
pub const SIGNATURE_LENGTH: usize = 65;
/// Address length of signer
pub const ADDRESS_LENGTH: usize = 20;
/// Difficulty for INTURN block
pub const DIFF_INTURN: U256 = U256([2, 0, 0, 0]);
/// Difficulty for NOTURN block
pub const DIFF_NOTURN: U256 = U256([1, 0, 0, 0]);
/// Default value for mixhash
pub const NULL_MIXHASH: H256 = H256([0; 32]);
/// Default value for uncles hash
pub const NULL_UNCLES_HASH: H256 = KECCAK_EMPTY_LIST_RLP;
/// Default noturn block wiggle factor defined in spec.
pub const SIGNING_DELAY_NOTURN_MS: u64 = 500;
/// How many snapshot to cache in the memory.
pub const SNAP_CACHE_NUM: usize = 512;
/// Number of blocks after which to save the snapshot to the database
pub const CHECKPOINT_INTERVAL: u64 = 1024;
/// Percentage to system reward.
pub const SYSTEM_REWARD_PERCENT: usize = 4;

const MAX_SYSTEM_REWARD: &str = "56bc75e2d63100000";
const INIT_TX_NUM: usize = 7;

use_contract!(validator_ins, "res/contracts/bsc_validators.json");
use_contract!(slash_ins, "res/contracts/bsc_slash.json");

/// Parlia Engine implementation
pub struct Parlia {
    chain_id: u64,
    epoch: u64,
    period: u64,
    client: RwLock<Option<Weak<dyn EngineClient>>>,
    machine: EthereumMachine,
    recent_snaps: RwLock<LruCache<H256, Snapshot>>,
    db: RwLock<Option<Arc<dyn KeyValueDB>>>,
}
impl Parlia {
    /// new parlia engine
    pub fn new(
        params: ParliaParams,
        machine: EthereumMachine,
        chain_id: u64,
    ) -> Result<Arc<Self>, Error> {
        let engine = Parlia {
            chain_id,
            machine,
            client: Default::default(),
            db: Default::default(),
            epoch: params.epoch,
            period: params.period,
            recent_snaps: RwLock::new(LruCache::new(SNAP_CACHE_NUM)),
        };
        let engine = Arc::new(engine);
        Ok(engine)
    }

    fn snapshot(&self, mut block_number: u64, mut block_hash: H256) -> Result<Snapshot, Error> {
        let mut snap_by_hash = self.recent_snaps.write();
        let mut headers = vec![];
        let mut snap: Snapshot;
        match self.client.read().as_ref().and_then(move |w| w.upgrade()) {
            None => Err(EngineError::RequiresClient)?,
            Some(c) => {
                loop {
                    if let Some(new_snap) = snap_by_hash.get_mut(&block_hash) {
                        snap = new_snap.clone();
                        break;
                    }
                    if block_number % CHECKPOINT_INTERVAL == 0 {
                        if let Some(new_snap) = Snapshot::load(
                            Arc::clone(&self.db.read().as_ref().unwrap()),
                            &block_hash,
                        ) {
                            snap = new_snap;
                            break;
                        }
                    }
                    if block_number == 0 {
                        match c.block_header(BlockId::Number(0)) {
                            None => Err(EngineError::ParliaUnContinuousHeader)?,
                            Some(genesis) => {
                                let hash = genesis.hash();
                                let validator_bytes = &genesis.extra_data()[VANITY_LENGTH
                                    ..(genesis.extra_data().len() - SIGNATURE_LENGTH)];
                                let validators = snapshot::parse_validators(validator_bytes)?;
                                snap = Snapshot::new(validators, 0, hash, self.epoch);
                                snap.store(Arc::clone(&self.db.write().as_ref().unwrap()));
                                break;
                            }
                        }
                    }
                    if let Some(header) = c.block_header(BlockId::Hash(block_hash)) {
                        headers.push(header.decode()?);
                        block_number -= 1;
                        block_hash = header.parent_hash();
                    } else {
                        Err(EngineError::ParliaUnContinuousHeader)?
                    }
                }
                for h in headers.iter().rev() {
                    snap = snap.apply(c.clone(), h, &self.chain_id)?;
                }
                snap_by_hash.insert(snap.hash.clone(), snap.clone());
                if snap.number % CHECKPOINT_INTERVAL == 0 {
                    snap.store(Arc::clone(&self.db.write().as_ref().unwrap()));
                }
                return Ok(snap);
            }
        }
    }

    fn default_caller(&self, id: BlockId) -> Box<Call> {
        let client = self.client.read().clone();
        Box::new(move |addr, data| {
            client
                .as_ref()
                .and_then(Weak::upgrade)
                .ok_or_else(|| "No client!".into())
                .and_then(|c| match c.as_full_client() {
                    Some(c) => c.call_contract(id, addr, data),
                    None => Err("No full client!".into()),
                })
                .map(|out| (out, Vec::new()))
        })
    }

    fn upgrade_build_in_system_contract(&self, _block: &mut ExecutedBlock) {
        upgrade_build_in_system_contract(self.machine.params(), _block);
    }
}

/// whether it is a parlia engine
pub fn is_parlia(engine: &str) -> bool {
    engine == "Parlia"
}

impl Engine<EthereumMachine> for Parlia {
    fn name(&self) -> &str {
        "Parlia"
    }

    fn machine(&self) -> &EthereumMachine {
        &self.machine
    }

    fn on_new_block(
        &self,
        _block: &mut ExecutedBlock,
        _epoch_begin: bool,
        _ancestry: &mut dyn Iterator<Item = ExtendedHeader>,
    ) -> Result<(), Error> {
        self.upgrade_build_in_system_contract(_block);
        Ok(())
    }

    fn on_close_block(&self, _block: &mut ExecutedBlock) -> Result<(), Error> {
        let txs = &_block.transactions;
        let header = &_block.header;

        if header.number() % self.epoch == 0 {
            let caller = self.default_caller(BlockId::Hash(*header.parent_hash()));
            let (input, decoder) = validator_ins::functions::get_validators::call();
            let addresses = caller(util::VALIDATOR_CONTRACT.clone(), input)
                .and_then(|x| decoder.decode(&x.0).map_err(|e| e.to_string()))?;

            let mut suppose_vals = BTreeSet::new();
            for a in addresses {
                suppose_vals.insert(Address::from(a));
            }
            let validator_bytes =
                &header.extra_data()[VANITY_LENGTH..(header.extra_data().len() - SIGNATURE_LENGTH)];
            let header_vals = snapshot::parse_validators(validator_bytes)?;
            if header_vals != suppose_vals {
                Err(EngineError::ParliaCheckpointMismatchValidators)?
            }
        }
        let mut expect_system_txs = vec![];
        let mut actual_system_txs = vec![];
        let mut have_sys_reward = false;
        for tx in txs {
            if is_system_transaction(tx, header.author()) {
                actual_system_txs.push(tx);
            }
        }
        if header.number() == 1 {
            // skip first 7 init transaction
            actual_system_txs = actual_system_txs[INIT_TX_NUM..].to_owned();
        }
        for tx in actual_system_txs.iter() {
            if tx.action == Action::Call(util::SYSTEM_REWARD_CONTRACT.clone()) {
                have_sys_reward = true;
            }
        }
        if *_block.header.difficulty() != DIFF_INTURN {
            let snap = self.snapshot(header.number() - 1, *header.parent_hash())?;
            let suppose_val = snap.suppose_validator();
            let mut signed_recently = false;
            for (_, recent) in snap.recents {
                if recent == suppose_val {
                    signed_recently = true;
                    break;
                }
            }
            if !signed_recently {
                let slash_tx_data: Vec<u8> = slash_ins::functions::slash::encode_input(suppose_val);
                let slash_tx = Transaction {
                    nonce: Default::default(),
                    gas_price: 0.into(),
                    gas: (std::u64::MAX / 2).into(),
                    value: 0.into(),
                    action: Action::Call(util::SLASH_CONTRACT.clone()),
                    data: slash_tx_data,
                };
                expect_system_txs.push(slash_tx);
            }
        }
        let mut reward: U256 = 0.into();
        for i in 0..txs.len() {
            let tx = txs.get(i).unwrap();
            let r = _block.receipts.get(i).unwrap();
            if i == 0 {
                reward = reward.add(tx.gas_price.mul(r.gas_used));
            } else {
                let last_used = _block.receipts.get(i - 1).unwrap().gas_used;
                reward = reward.add(tx.gas_price.mul(r.gas_used - last_used));
            }
        }
        if reward > 0.into() {
            let sys_reward = reward >> SYSTEM_REWARD_PERCENT;
            if sys_reward > 0.into() {
                let sys_hold = _block.state.balance(&util::SYSTEM_REWARD_CONTRACT).unwrap();
                if !have_sys_reward && sys_hold < MAX_SYSTEM_REWARD.into() {
                    Err(EngineError::ParliaSystemTxMismatch)?
                }
                if have_sys_reward {
                    let sys_reward_tx = Transaction {
                        nonce: Default::default(),
                        gas_price: 0.into(),
                        gas: (std::u64::MAX / 2).into(),
                        value: sys_reward,
                        action: Action::Call(util::SYSTEM_REWARD_CONTRACT.clone()),
                        data: vec![],
                    };
                    expect_system_txs.push(sys_reward_tx);
                    reward -= sys_reward;
                }
            }
            let validator_dis_data =
                validator_ins::functions::deposit::encode_input(*header.author());
            let validator_dis_tx = Transaction {
                nonce: Default::default(),
                gas_price: 0.into(),
                gas: (std::u64::MAX / 2).into(),
                value: reward,
                action: Action::Call(util::VALIDATOR_CONTRACT.clone()),
                data: validator_dis_data,
            };
            expect_system_txs.push(validator_dis_tx);
        }

        if actual_system_txs.len() != expect_system_txs.len() {
            Err(EngineError::ParliaSystemTxMismatch)?
        }
        let system_tx_num = expect_system_txs.len();
        for i in 0..system_tx_num {
            let expect_tx = expect_system_txs.get(i).unwrap();
            let system_tx = actual_system_txs.get(i).unwrap();
            if system_tx.gas != expect_tx.gas
                || system_tx.gas_price != expect_tx.gas_price
                || system_tx.value != expect_tx.value
                || system_tx.data != expect_tx.data
                || system_tx.action != expect_tx.action
            {
                Err(EngineError::ParliaSystemTxMismatch)?
            }
        }
        Ok(())
    }

    fn verify_local_seal(&self, _header: &Header) -> Result<(), Error> {
        Ok(())
    }

    fn seal_fields(&self, _header: &Header) -> usize {
        2
    }

    fn verify_block_basic(&self, header: &Header) -> Result<(), Error> {
        let num = header.number();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header_time = header.timestamp();
        if header_time > now {
            Err(BlockError::InvalidTimestamp(OutOfBounds {
                max: Some(SystemTime::UNIX_EPOCH.add(Duration::from_secs(now))),
                min: None,
                found: SystemTime::UNIX_EPOCH.add(Duration::from_secs(header_time)),
            }))?
        }
        let extra_data_len = header.extra_data().len();

        if extra_data_len < VANITY_LENGTH {
            Err(EngineError::ParliaMissingVanity)?
        }

        if extra_data_len < VANITY_LENGTH + SIGNATURE_LENGTH {
            Err(EngineError::ParliaMissingSignature)?
        }

        let signers_bytes = extra_data_len - VANITY_LENGTH - SIGNATURE_LENGTH;
        let is_epoch = num % self.epoch == 0;
        if !is_epoch && signers_bytes != 0 {
            Err(EngineError::ParliaInvalidValidatorsExtra)?
        }
        if is_epoch && signers_bytes % ADDRESS_LENGTH != 0 {
            Err(EngineError::ParliaInvalidValidatorsExtra)?
        }
        let seal_fields = header.decode_seal::<Vec<_>>()?;
        if seal_fields.len() != 2 {
            Err(BlockError::InvalidSealArity(Mismatch {
                expected: 2,
                found: seal_fields.len(),
            }))?
        }

        let mixhash: H256 = seal_fields[0].into();
        if mixhash != NULL_MIXHASH {
            Err(BlockError::MismatchedH256SealElement(Mismatch {
                expected: NULL_MIXHASH,
                found: mixhash,
            }))?
        }
        // Ensure that the block doesn't contain any uncles which are meaningless in PoA
        if *header.uncles_hash() != NULL_UNCLES_HASH {
            Err(BlockError::InvalidUnclesHash(Mismatch {
                expected: NULL_UNCLES_HASH,
                found: *header.uncles_hash(),
            }))?
        }

        // Ensure that the block's difficulty is meaningful (may not be correct at this point)
        if *header.difficulty() != DIFF_INTURN && *header.difficulty() != DIFF_NOTURN {
            Err(BlockError::DifficultyOutOfBounds(OutOfBounds {
                min: Some(DIFF_NOTURN),
                max: Some(DIFF_INTURN),
                found: *header.difficulty(),
            }))?
        }
        if header.gas_used() > header.gas_limit() {
            Err(BlockError::TooMuchGasUsed(OutOfBounds {
                max: Some(*header.gas_limit()),
                min: None,
                found: *header.gas_used(),
            }))?
        }
        let cap = U256::from(0x7fffffffffffffff_u64);
        if *header.gas_limit() > cap {
            Err(BlockError::InvalidGasLimit(OutOfBounds {
                min: None,
                max: Some(cap),
                found: *header.gas_limit(),
            }))?
        }
        Ok(())
    }

    fn verify_block_family(&self, header: &Header, parent: &Header) -> Result<(), Error> {
        let num = header.number();
        let snap = self.snapshot(parent.number(), parent.hash())?;
        if header.timestamp()
            < parent.timestamp() + self.period + snap.back_off_time(header.author())
        {
            Err(EngineError::ParliaFutureBlock)?
        }
        let signer = recover_creator(header, &self.chain_id)?;
        if signer != *header.author() {
            Err(EngineError::ParliaAuthorMismatch)?
        }
        if !snap.validators.contains(&signer) {
            Err(EngineError::ParliaUnauthorizedValidator)?
        }
        for (seen, recent) in snap.recents.iter() {
            if *recent == signer {
                // Signer is among recents, only fail if the current block doesn't shift it out
                let limit = (snap.validators.len() / 2 + 1) as u64;
                if *seen > num - limit {
                    Err(EngineError::ParliaRecentlySigned)?
                }
            }
        }
        let is_inturn = snap.inturn(&signer);
        if is_inturn && *header.difficulty() != DIFF_INTURN {
            Err(BlockError::InvalidDifficulty(Mismatch {
                expected: DIFF_INTURN,
                found: *header.difficulty(),
            }))?
        } else if !is_inturn && *header.difficulty() != DIFF_NOTURN {
            Err(BlockError::InvalidDifficulty(Mismatch {
                expected: DIFF_NOTURN,
                found: *header.difficulty(),
            }))?
        }
        Ok(())
    }

    fn genesis_epoch_data(&self, genesis: &Header, _call: &Call) -> Result<Vec<u8>, String> {
        let hash = genesis.hash();
        let validator_bytes =
            &genesis.extra_data()[VANITY_LENGTH..(genesis.extra_data().len() - SIGNATURE_LENGTH)];
        let validators =
            snapshot::parse_validators(validator_bytes).map_err(|e| format!("{}", e))?;
        let snap = Snapshot::new(validators, 0, hash, self.epoch);
        snap.store(Arc::clone(&self.db.write().as_ref().unwrap()));
        Ok(Vec::new())
    }

    fn register_client(&self, client: Weak<dyn EngineClient>) {
        *self.client.write() = Some(client.clone());
    }

    fn register_db(&self, db: Arc<dyn KeyValueDB>) {
        *self.db.write() = Some(db.clone());
    }

    fn is_timestamp_valid(&self, header_timestamp: u64, parent_timestamp: u64) -> bool {
        header_timestamp >= parent_timestamp.saturating_add(self.period)
    }

    fn fork_choice(&self, new: &ExtendedHeader, current: &ExtendedHeader) -> super::ForkChoice {
        super::total_difficulty_fork_choice(new, current)
    }

    fn executive_author(&self, header: &Header) -> Result<Address, Error> {
        recover_creator(header, &self.chain_id)
    }
}
