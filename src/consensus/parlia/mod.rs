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

//! Implementation of the œ POSA Engine.
#![allow(missing_docs)]
mod snapshot;
pub mod util;
mod contract_upgrade;

use super::{*, base::ConsensusEngineBase};
use std::{io::Read, str};
use crate::{execution::{
    analysis_cache::AnalysisCache,
    evmglue,
    tracer::{CallKind, MessageKind, NoopTracer, Tracer},}, rpc::eth};

use crate::{
    accessors::chain,
    Buffer,
    consensus::{
        parlia::{
            snapshot::Snapshot,
            util::{is_system_transaction, recover_creator},
        },
    },
    HeaderReader,
    kv::{mdbx::*, MdbxWithDirHandle},
    models::*,
};
use anyhow::format_err;
use ethabi::FunctionOutputDecoder;
use ethereum_types::{Address, H256};
use lru_cache::LruCache;
use parking_lot::RwLock;
use std::{
    collections::BTreeSet,
    ops::{Add, Mul},
    str::FromStr, time::{Duration, SystemTime},
};
use std::ops::Deref;
use TransactionAction;
use bytes::{Buf, Bytes};
use byteorder::{ByteOrder, LittleEndian};
use ethereum_types::{H160, U256};
use futures::future::err;
use crate::consensus::DuoError::Validation;
use tracing::*;
use crate::kv::tables;

/// Type alias for a function we can make calls through synchronously.
/// Returns the call result and state proof for each call.
pub type Call<'a> = dyn Fn(Address, Vec<u8>) -> Result<(Vec<u8>, Vec<Vec<u8>>), String> + 'a;

// Protocol constants
/// Fixed number of extra-data prefix bytes reserved for signer vanity
pub const VANITY_LENGTH: usize = 32;
/// Fixed number of extra-data suffix bytes reserved for signer signature
pub const SIGNATURE_LENGTH: usize = 65;
/// Address length of signer
pub const ADDRESS_LENGTH: usize = 20;
/// Difficulty for INTURN block
pub const DIFF_INTURN: ethnum::U256 =ethnum::U256([2, 0]);
/// Difficulty for NOTURN block
pub const DIFF_NOTURN: ethnum::U256 = ethnum::U256([1, 0]);
/// Default value for mixhash
pub const NULL_MIXHASH: H256 = H256([0; 32]);
/// Default value for uncles hash
pub const NULL_UNCLES_HASH :H256 = H256([0x1d, 0xcc, 0x4d, 0xe8, 0xde, 0xc7, 0x5d, 0x7a, 0xab, 0x85, 0xb5, 0x67, 0xb6, 0xcc, 0xd4, 0x1a, 0xd3, 0x12, 0x45, 0x1b, 0x94, 0x8a, 0x74, 0x13, 0xf0, 0xa1, 0x42, 0xfd, 0x40, 0xd4, 0x93, 0x47] );
/// Default noturn block wiggle factor defined in spec.
pub const SIGNING_DELAY_NOTURN_MS: u64 = 500;
/// How many snapshot to cache in the memory.
pub const SNAP_CACHE_NUM: usize = 2048;
/// Number of blocks after which to save the snapshot to the database
pub const CHECKPOINT_INTERVAL: u64 = 1024;
/// Percentage to system reward.
pub const SYSTEM_REWARD_PERCENT: usize = 4;

const MAX_SYSTEM_REWARD: &str = "0x56bc75e2d63100000";
const INIT_TX_NUM: usize = 7;

use_contract!(validator_ins, "src/contracts/bsc_validators.json");
use_contract!(slash_ins, "src/contracts/bsc_slash.json");

/// Parlia Engine implementation
#[derive(Debug)]
pub struct Parlia {
    chain_spec: ChainSpec,
    chain_id: ChainId,
    epoch: u64,
    period: u64,
    recent_snaps: RwLock<LruCache<H256, Snapshot>>,
    fork_choice_graph: Arc<Mutex<ForkChoiceGraph>>,
}
impl Parlia {
    /// new parlia engine
    pub fn new(
        chain_id: ChainId,
        chain_spec: ChainSpec,
        epoch: u64,
        period: u64,) -> Self {
        Self {
            chain_spec,
            chain_id,
            epoch,
            period,
            recent_snaps: RwLock::new(LruCache::new(SNAP_CACHE_NUM)),
            fork_choice_graph: Arc::new(Mutex::new(Default::default())),
        }
    }
}

/// whether it is a parlia engine
pub fn is_parlia(engine: &str) -> bool {
    engine == "Parlia"
}

impl Consensus for Parlia {
    fn name(&self) -> &str {
        "Parlia"
    }
    
    fn fork_choice_mode(&self) -> ForkChoiceMode {
        ForkChoiceMode::Difficulty(self.fork_choice_graph.clone())
    }

    fn pre_validate_block(&self, block: &Block, state: &dyn BlockReader) -> Result<(), DuoError>{
        Ok(())
    }

    fn finalize(
        &self,
        header: &BlockHeader,
        ommers: &[BlockHeader],
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        Ok(Vec::default())
    }

    fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        with_future_timestamp_check: bool,
    ) -> Result<(), DuoError> {
        let num = header.number;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header_time = header.timestamp;
        if header_time > now {
           return Err(Validation(ValidationError::WrongHeaderTime {
               now,
               got: header_time
           }));
        }
        let extra_data_len = header.extra_data.len();

        if extra_data_len < VANITY_LENGTH {
            return Err(Validation(ValidationError::WrongHeaderExtraLen{
                expected: VANITY_LENGTH,
                got: extra_data_len
            }));
        }

        if extra_data_len < VANITY_LENGTH + SIGNATURE_LENGTH {
            return Err(Validation(ValidationError::WrongHeaderExtraLen{
                expected: VANITY_LENGTH + SIGNATURE_LENGTH,
                got: extra_data_len
            }));
        }

        let signers_bytes = extra_data_len - VANITY_LENGTH - SIGNATURE_LENGTH;
        let is_epoch = num % self.epoch == 0;
        if !is_epoch && signers_bytes != 0 {
            return Err(Validation(ValidationError::WrongHeaderExtraSignersLen{
                expected: 0,
                got: signers_bytes
            }));
        }
        if is_epoch && signers_bytes % ADDRESS_LENGTH != 0 {
            return Err(Validation(ValidationError::WrongHeaderExtraSignersLen{
                expected: 0,
                got: signers_bytes % ADDRESS_LENGTH
            }));
        }

        // Ensure that the block doesn't contain any uncles which are meaningless in PoA
        if header.ommers_hash != NULL_UNCLES_HASH {
            return Err(ValidationError::NotAnOmmer.into());
        }

        // Ensure that the block's difficulty is meaningful (may not be correct at this point)
        if header.difficulty != DIFF_INTURN && header.difficulty != DIFF_NOTURN {
            return Err(ValidationError::WrongDifficulty.into());
        }
        if header.gas_used > header.gas_limit {
            return Err(ValidationError::InvalidGasLimit.into());
        }
        let cap = ethnum::U256::from(0x7fffffffffffffff_u64);
        if header.gas_limit.as_u256() > cap {
            return Err(ValidationError::InvalidGasLimit.into());
        }
        let snap = self.query_snap(parent.number.0, parent.hash())?;
        if header.timestamp
            < parent.timestamp + self.period + snap.back_off_time(&header.beneficiary)
        {
            return Err(ValidationError::InvalidTimestamp {
                parent: parent.timestamp,
                current: header.timestamp
            }.into());
        }
        let signer = recover_creator(header, self.chain_id)?;
        if signer != header.beneficiary {
            return Err(ValidationError::WrongHeaderSigner {
                number: header.number,
                expected: header.beneficiary,
                got: signer
            }.into());
        }
        if !snap.validators.contains(&signer) {
            return Err(ValidationError::SignerUnauthorized{ number: header.number, signer }.into());
        }
        for (seen, recent) in snap.recents.iter() {
            if *recent == signer {
                // Signer is among recents, only fail if the current block doesn't shift it out
                let limit = (snap.validators.len() / 2 + 1) as u64;
                if *seen > num.0 - limit {
                    return Err(ValidationError::SignerOverLimit{ signer }.into());
                }
            }
        }
        let is_inturn = snap.inturn(&signer);
        if is_inturn && header.difficulty != DIFF_INTURN {
            return Err(ValidationError::WrongDifficulty.into());
        } else if !is_inturn && header.difficulty != DIFF_NOTURN {
            return Err(ValidationError::WrongDifficulty.into());
        }
        Ok(())
    }

    fn parlia(&mut self) -> Option<&mut Parlia> {
        Some(self)
    }

}

impl Parlia {
    // TODO tmp parlia real finalize move outside, because trait's inner generic problem fix next time
    pub fn outer_finalize<'r, S>(
        &mut self,
        header: &BlockHeader,
        ommers: &[BlockHeader],
        state: &mut IntraBlockState<'r, S>,
        transactionsOp: Option<&Vec<MessageWithSender>>,
        receiptsOp: Option<&Vec<Receipt>>,
    ) -> anyhow::Result<Vec<FinalizationChange>, DuoError>
        where S: StateReader+HeaderReader,
    {
        if transactionsOp.is_none() {
            return Err(Validation(ValidationError::NoneTransactions));
        }
        if receiptsOp.is_none() {
            return Err(Validation(ValidationError::NoneReceipts));
        }
        let transactions = transactionsOp.unwrap();
        let receipts = receiptsOp.unwrap();
        if header.number % self.epoch == 0 {
            let (input, decoder) = validator_ins::functions::get_validators::call();
            let mut input_byte= Bytes::from(input);
            let message = Message::Legacy {
                chain_id: Some(self.chain_id),
                nonce: header.nonce.to_low_u64_be(),
                gas_price: ethnum::U256::ZERO,
                gas_limit: 50000000,
                action: TransactionAction::Call(util::VALIDATOR_CONTRACT.clone()),
                value:  ethnum::U256::ZERO,
                input: input_byte,
            };

            let mut analysis_cache = AnalysisCache::default();
            let mut tracer = NoopTracer;
            let block_spec = self.chain_spec.collect_block_spec(header.number);
            let res = evmglue::execute(
                state,
                &mut tracer,
                &mut analysis_cache,
                &header,
                &block_spec,
                &message,
                *util::VALIDATOR_CONTRACT,
                *util::VALIDATOR_CONTRACT,
                message.gas_limit(),
            )?;
            let dec_res = decoder.decode(res.output_data.chunk());
            if let Ok(addresses) = dec_res {
                let mut suppose_vals = BTreeSet::new();
                for addr in addresses {
                    suppose_vals.insert(Address::from(addr));
                }

                let validator_bytes =
                    &header.extra_data[VANITY_LENGTH..(header.extra_data.len() - SIGNATURE_LENGTH)];
                let header_vals = snapshot::parse_validators(validator_bytes)?;
                if header_vals != suppose_vals {
                    return Err(Validation(ValidationError::EpochChgWrongValidators{
                        expect: suppose_vals,
                        got: header_vals
                    }));
                }
            } else {
                error!("decoder err block {}, res {:?}", header.number, dec_res);
                return Err(Validation(ValidationError::EpochChgCallErr));
            }
        }

        // calculate reward
        let mut expect_system_txs: Vec<Message>= vec![];
        let mut actual_system_txs: Vec<&MessageWithSender> = vec![];
        let mut have_sys_reward = false;
        for tx in transactions.iter() {
            if is_system_transaction(&tx.message, &tx.sender, &header.beneficiary) {
                actual_system_txs.push(tx);
            }
        }
        if header.number == 1 {
            // skip first 7 init transaction
            actual_system_txs = actual_system_txs[INIT_TX_NUM..].to_owned();
        }
        for tx in actual_system_txs.iter() {
            if tx.message.action() == TransactionAction::Call(util::SYSTEM_REWARD_CONTRACT.clone()) {
                have_sys_reward = true;
            }
        }
        if header.difficulty != DIFF_INTURN {
            let snap = self.query_snap(header.number.0 - 1, header.parent_hash)?;
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
                let mut input_byte= Bytes::from(slash_tx_data);
                let slash_tx = Message::Legacy {
                    chain_id: Some(self.chain_id),
                    nonce: Default::default(),
                    gas_price: ethnum::U256::new(0),
                    gas_limit: (std::u64::MAX / 2).into(),
                    value: ethnum::U256::new(0),
                    action: TransactionAction::Call(util::SLASH_CONTRACT.clone()),
                    input: input_byte,
                };
                expect_system_txs.push(slash_tx);
            }
        }
        let mut reward: ethnum::U256 = ethnum::U256::ZERO;
        for i in 0..transactions.len() {
            let tx = transactions.get(i).unwrap();
            let r = receipts.get(i).unwrap();
            if i == 0 {
                reward = reward + ethnum::U256::new(tx.message.max_fee_per_gas().as_u256().mul(r.cumulative_gas_used.as_u256()).as_u128());
            } else {
                let last_used = receipts.get(i - 1).unwrap().cumulative_gas_used;
                reward = reward + ethnum::U256::new(tx.message.max_fee_per_gas().as_u256().mul((r.cumulative_gas_used - last_used).as_u256()).as_u128());
            }
        }
        if reward > ethnum::U256::new(0) {
            let sys_reward = reward >> SYSTEM_REWARD_PERCENT;
            if sys_reward > ethnum::U256::new(0) {
                let sys_hold = state.get_balance(*util::SYSTEM_REWARD_CONTRACT).unwrap();
                let max_system_reward = ethnum::U256::from_str_hex(MAX_SYSTEM_REWARD.into()).unwrap();
                if !have_sys_reward && sys_hold < max_system_reward{
                    return Err(ValidationError::SystemTxWrongSystemReward {
                        expect: max_system_reward,
                        got: sys_hold,
                    }.into());
                }
                if have_sys_reward {
                    let sys_reward_tx = Message::Legacy {
                        chain_id:Some(self.chain_id),
                        nonce: Default::default(),
                        gas_price: ethnum::U256::new(0),
                        gas_limit: (std::u64::MAX / 2).into(),
                        value: sys_reward,
                        action: TransactionAction::Call(util::SYSTEM_REWARD_CONTRACT.clone()),
                        input: Bytes::new(),
                    };
                    expect_system_txs.push(sys_reward_tx);
                    reward -= sys_reward;
                }
            }
            let validator_dis_data =
                validator_ins::functions::deposit::encode_input(header.beneficiary);
            let mut input_byte= Bytes::from(validator_dis_data);
            let validator_dis_tx = Message::Legacy {
                chain_id: Some(self.chain_id),
                nonce: Default::default(),
                gas_price: ethnum::U256::new(0),
                gas_limit: (std::u64::MAX / 2).into(),
                value: reward,
                action: TransactionAction::Call(util::VALIDATOR_CONTRACT.clone()),
                input: input_byte,
            };
            expect_system_txs.push(validator_dis_tx);
        }

        if actual_system_txs.len() != expect_system_txs.len() {
            debug!("expect_system_txs {:?}", expect_system_txs);
            return Err(ValidationError::SystemTxWrongCount {
                expect: expect_system_txs.len(),
                got: actual_system_txs.len()
            }.into());
        }
        let system_tx_num = expect_system_txs.len();
        for i in 0..system_tx_num {
            let expect_tx = expect_system_txs.get(i).unwrap();
            let system_tx = actual_system_txs.get(i).unwrap();
            if system_tx.message.max_fee_per_gas() != expect_tx.max_fee_per_gas()
                || system_tx.message.max_fee_per_gas() != expect_tx.max_fee_per_gas()
                || system_tx.message.value() != expect_tx.value()
                || system_tx.message.input() != expect_tx.input()
                || system_tx.message.action() != expect_tx.action()
            {
                return Err(ValidationError::SystemTxWrong{
                    expect: expect_tx.clone(),
                    got: system_tx.message.clone(),
                }.into());
            }
        }
        Ok(Vec::default())
    }

    // TODO tmp add block outside, because trait's inner generic problem fix next time
    pub fn new_block<'r, S>(
        &mut self,
        header: &BlockHeader,
        state: &mut IntraBlockState<'r, S>,
    ) -> anyhow::Result<(), DuoError>
        where S: StateReader+HeaderReader,
    {
        contract_upgrade::upgrade_build_in_system_contract(&self.chain_spec, &header.number, state)
    }

    pub fn query_snap(
        &self,
        mut block_number: u64,
        mut block_hash: H256,
    ) -> Result<Snapshot, DuoError>{

        let mut snap_by_hash = self.recent_snaps.write();
        if let Some(new_snap) = snap_by_hash.get_mut(&block_hash) {
            return Ok(new_snap.clone());
        }
        return Err(Validation(ValidationError::SnapNotFound {
            number: BlockNumber(block_number),
            hash: block_hash,
        }))
    }

    // snapshot retrieves the authorization snapshot at a given point in time.
    pub fn snapshot<E>(
        &mut self,
        txn: &MdbxTransaction<'_, RW, E>,
        mut block_number: BlockNumber,
        mut block_hash: H256,
    ) -> anyhow::Result<Snapshot, DuoError>
        where E: EnvironmentKind
    {
        debug!("snapshot header {}", block_number);
        let mut snap_by_hash = self.recent_snaps.write();
        let mut headers = Vec::new();
        let mut snap: Snapshot;

        loop {
            debug!("snap loop header {} {:?}", block_number, block_hash);
            if let Some(new_snap) = snap_by_hash.get_mut(&block_hash) {
                snap = new_snap.clone();
                break;
            }
            if block_number % CHECKPOINT_INTERVAL == 0 {
                if let Some(new_snap) = Snapshot::load(txn, block_hash)? {
                    snap = new_snap;
                    info!("snap find from db {} {:?}", block_number, block_hash);
                    break;
                }
            }
            if block_number == 0 {
                let header = txn.read_header(block_number, block_hash)?;
                if header.is_none(){
                    return Err(Validation(ValidationError::UnknownHeader{
                        number: block_number, hash: block_hash
                    }));
                }
                let genesis = header.unwrap();
                let validator_bytes =
                    &genesis.extra_data[VANITY_LENGTH..(genesis.extra_data.len() - SIGNATURE_LENGTH)];
                let validators = snapshot::parse_validators(validator_bytes)?;
                snap = Snapshot::new(validators, block_number.0, block_hash, self.epoch);
                break;
            }
            if let Some(header) = txn.read_header(block_number, block_hash)? {
                block_hash = header.parent_hash;
                block_number = BlockNumber(header.number.0 - 1);
                headers.push(header);
            } else {
                return Err(Validation(ValidationError::UnknownHeader{
                    number: block_number, hash: block_hash
                }));
            }
        }
        for h in headers.iter().rev() {
            snap = snap.apply(txn, h, self.chain_id)?;
        }

        debug!("snap insert {} {:?}", snap.number, snap.hash);
        snap_by_hash.insert(snap.hash, snap.clone());
        if snap.number % CHECKPOINT_INTERVAL == 0 {
            snap.store(txn)?;
        }
        return Ok(snap);
    }
}
