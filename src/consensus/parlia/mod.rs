//! Implementation of the BSC's POSA Engine.
#![allow(missing_docs)]
mod snapshot;
mod contract_upgrade;
mod util;
mod state;
pub use state::{ParliaNewBlockState};
pub use snapshot::{SnapRW};
pub use util::{is_system_transaction, SYSTEM_ACCOUNT};

use super::*;
use crate::{
    execution::{
        analysis_cache::AnalysisCache,
        evmglue,
        tracer::NoopTracer,
    },
};
use std::str;

use crate::{
    consensus::{
        parlia::{
            snapshot::{Snapshot},
        },
        ValidationError, ParliaError
    },
    crypto::go_rng::{RngSource, Shuffle},
    models::*, HeaderReader,
};
use bytes::{Buf, Bytes};
use ethabi::FunctionOutputDecoder;
use ethereum_types::{Address, H256};
use lru_cache::LruCache;
use parking_lot::RwLock;
use std::{
    collections::BTreeSet,
    time::{SystemTime},
};
use ethabi_contract::use_contract;
use tracing::*;
use TransactionAction;

/// Fixed number of extra-data prefix bytes reserved for signer vanity
pub const VANITY_LENGTH: usize = 32;
/// Fixed number of extra-data suffix bytes reserved for signer signature
pub const SIGNATURE_LENGTH: usize = 65;
/// Address length of signer
pub const ADDRESS_LENGTH: usize = 20;
/// Difficulty for INTURN block
pub const DIFF_INTURN: U256 = U256([2, 0]);
/// Difficulty for NOTURN block
pub const DIFF_NOTURN: U256 = U256([1, 0]);
/// Default value for mixhash
pub const NULL_MIXHASH: H256 = H256([0; 32]);
/// Default value for uncles hash
pub const NULL_UNCLES_HASH: H256 = H256([
    0x1d, 0xcc, 0x4d, 0xe8, 0xde, 0xc7, 0x5d, 0x7a, 0xab, 0x85, 0xb5, 0x67, 0xb6, 0xcc, 0xd4, 0x1a,
    0xd3, 0x12, 0x45, 0x1b, 0x94, 0x8a, 0x74, 0x13, 0xf0, 0xa1, 0x42, 0xfd, 0x40, 0xd4, 0x93, 0x47,
]);
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

use_contract!(validator_ins, "src/consensus/parlia/contracts/bsc_validators.json");
use_contract!(slash_ins, "src/consensus/parlia/contracts/bsc_slash.json");

/// Parlia Engine implementation
#[derive(Debug)]
pub struct Parlia {
    chain_spec: ChainSpec,
    chain_id: ChainId,
    epoch: u64,
    period: u64,
    recent_snaps: RwLock<LruCache<H256, Snapshot>>,
    fork_choice_graph: Arc<Mutex<ForkChoiceGraph>>,
    new_block_state: ParliaNewBlockState,
}
impl Parlia {
    /// new parlia engine
    pub fn new(chain_id: ChainId, chain_spec: ChainSpec, epoch: u64, period: u64) -> Self {
        Self {
            chain_spec,
            chain_id,
            epoch,
            period,
            recent_snaps: RwLock::new(LruCache::new(SNAP_CACHE_NUM)),
            fork_choice_graph: Arc::new(Mutex::new(Default::default())),
            new_block_state: ParliaNewBlockState::new(None),
        }
    }

    /// check if extra len is correct
    fn check_header_extra_len(&self, header: &BlockHeader) -> anyhow::Result<(), DuoError> {
        let extra_data_len = header.extra_data.len();

        if extra_data_len < VANITY_LENGTH {
            return Err(ParliaError::WrongHeaderExtraLen {
                expected: VANITY_LENGTH,
                got: extra_data_len,
            }.into());
        }

        if extra_data_len < VANITY_LENGTH + SIGNATURE_LENGTH {
            return Err(ParliaError::WrongHeaderExtraLen {
                expected: VANITY_LENGTH + SIGNATURE_LENGTH,
                got: extra_data_len,
            }.into());
        }

        let signers_bytes = extra_data_len - VANITY_LENGTH - SIGNATURE_LENGTH;
        let epoch_chg = header.number.0 % self.epoch == 0;
        if !epoch_chg && signers_bytes != 0 {
            return Err(ParliaError::WrongHeaderExtraSignersLen {
                expected: 0,
                got: signers_bytes,
            }.into());
        }
        if epoch_chg && signers_bytes % ADDRESS_LENGTH != 0 {
            return Err(ParliaError::WrongHeaderExtraSignersLen {
                expected: 0,
                got: signers_bytes % ADDRESS_LENGTH,
            }.into());
        }

        Ok(())
    }
}

impl Consensus for Parlia {

    fn fork_choice_mode(&self) -> ForkChoiceMode {
        ForkChoiceMode::Difficulty(self.fork_choice_graph.clone())
    }

    fn pre_validate_block(&self, _block: &Block, _state: &dyn BlockReader) -> Result<(), DuoError> {
        Ok(())
    }

    fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        _with_future_timestamp_check: bool,
    ) -> Result<(), DuoError> {
        let block_number = header.number;
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
        let header_timestamp = header.timestamp;
        if header_timestamp > timestamp {
            return Err(ParliaError::WrongHeaderTime {
                now: timestamp,
                got: header_timestamp,
            }.into());
        }

        self.check_header_extra_len(header)?;
        // Ensure that the block with no uncles
        if header.ommers_hash != NULL_UNCLES_HASH {
            return Err(ValidationError::NotAnOmmer.into());
        }

        // Ensure that the block's difficulty is DIFF_INTURN or DIFF_NOTURN
        if header.difficulty != DIFF_INTURN && header.difficulty != DIFF_NOTURN {
            return Err(ValidationError::WrongDifficulty.into());
        }
        if header.gas_used > header.gas_limit {
            return Err(ValidationError::InvalidGasLimit.into());
        }
        if header.gas_limit.as_u256() > *util::MAX_GAS_LIMIT_CAP {
            return Err(ValidationError::InvalidGasLimit.into());
        }
        let snap = self.query_snap(parent.number.0, parent.hash())?;
        self.verify_block_time_for_ramanujan_fork(&snap, header, parent)?;

        let proposer = util::recover_creator(header, self.chain_id)?;
        if proposer != header.beneficiary {
            return Err(ParliaError::WrongHeaderSigner {
                number: header.number,
                expected: header.beneficiary,
                got: proposer,
            }.into());
        }
        if !snap.validators.contains(&proposer) {
            return Err(ParliaError::SignerUnauthorized {
                number: header.number,
                signer: proposer,
            }.into());
        }
        for (seen, recent) in snap.recent_proposers.iter() {
            if *recent == proposer {
                // Signer is among recent_proposers, only fail if the current block doesn't shift it out
                let limit = (snap.validators.len() / 2 + 1) as u64;
                if *seen > block_number.0 - limit {
                    return Err(ParliaError::SignerOverLimit { signer: proposer }.into());
                }
            }
        }
        let inturn_proposer = snap.inturn(&proposer);
        if inturn_proposer && header.difficulty != DIFF_INTURN {
            return Err(ValidationError::WrongDifficulty.into());
        } else if !inturn_proposer && header.difficulty != DIFF_NOTURN {
            return Err(ValidationError::WrongDifficulty.into());
        }
        Ok(())
    }

    /// parlia's finalize not effect any state, must set transaction and ConsensusFinalizeState in sync
    fn finalize(
        &self,
        header: &BlockHeader,
        _ommers: &[BlockHeader],
        transactions: Option<&Vec<MessageWithSender>>,
        state: &dyn StateReader,
    ) -> anyhow::Result<Vec<FinalizationChange>> {

        // check epoch validators chg correctly
        if self.new_block_state.parsed_validators() && header.number % self.epoch == 0 {
            let expect_validators = self.new_block_state.get_validators()
                .ok_or_else(|| ParliaError::CacheValidatorsUnknown)?;

            let actual_validators = util::parse_epoch_validators(&header.extra_data[VANITY_LENGTH..(header.extra_data.len() - SIGNATURE_LENGTH)])?;

            debug!("epoch validators check {}, {}:{}", header.number, actual_validators.len(), expect_validators.len());
            if actual_validators != *expect_validators {
                return Err(ParliaError::EpochChgWrongValidators {
                    expect: expect_validators.clone(),
                    got: actual_validators,
                }.into());
            }
        }

        // if set transactions, check systemTxs and reward if correct
        // must set transactions in sync
        if let Some(transactions) = transactions {
            let mut system_txs: Vec<&MessageWithSender> = transactions.iter()
                .filter(|tx| is_system_transaction(&tx.message, &tx.sender, &header.beneficiary))
                .collect();
            if header.number == 1 {
                // skip block=1, first 7 init system transactions
                system_txs = system_txs[INIT_TX_NUM..].to_vec();
            }

            let mut expect_txs = Vec::new();
            if header.difficulty != DIFF_INTURN {
                debug!("check in turn {}", header.number);
                let snap = self.query_snap(header.number.0 - 1, header.parent_hash)?;
                let proposer = snap.suppose_validator();
                let had_proposed = snap.recent_proposers.iter().find(|(_, v)| **v == proposer)
                    .map(|_| true).unwrap_or(false);

                if !had_proposed {
                    let slash_data: Vec<u8> = slash_ins::functions::slash::encode_input(proposer);
                    expect_txs.push(Message::Legacy {
                        chain_id: Some(self.chain_id),
                        nonce: Default::default(),
                        gas_price: U256::ZERO,
                        gas_limit: (std::u64::MAX / 2).into(),
                        value: U256::ZERO,
                        action: TransactionAction::Call(*util::SLASH_CONTRACT),
                        input: Bytes::from(slash_data),
                    });
                }
            }

            let mut total_reward = state.read_account(*util::SYSTEM_ACCOUNT)?
                .and_then(|a| Some(a.balance) )
                .unwrap_or(U256::ZERO);
            let sys_reward_collected = state.read_account(*util::SYSTEM_REWARD_CONTRACT)?
                .and_then(|a| Some(a.balance) )
                .unwrap_or(U256::ZERO);

            if total_reward > U256::ZERO {
                // check if contribute to SYSTEM_REWARD_CONTRACT
                let to_sys_reward = total_reward >> SYSTEM_REWARD_PERCENT;
                let max_reward = U256::from_str_hex(MAX_SYSTEM_REWARD)?;
                if to_sys_reward > U256::ZERO && sys_reward_collected < max_reward {
                    expect_txs.push(Message::Legacy {
                        chain_id: Some(self.chain_id),
                        nonce: Default::default(),
                        gas_price: U256::ZERO,
                        gas_limit: (std::u64::MAX / 2).into(),
                        value: to_sys_reward,
                        action: TransactionAction::Call(util::SYSTEM_REWARD_CONTRACT.clone()),
                        input: Bytes::new(),
                    });
                    total_reward -= to_sys_reward;
                    debug!("SYSTEM_REWARD_CONTRACT, block {}, reward {}", header.number, to_sys_reward);
                }

                // left reward contribute to VALIDATOR_CONTRACT
                debug!("VALIDATOR_CONTRACT, block {}, reward {}", header.number, total_reward);
                let input_data = validator_ins::functions::deposit::encode_input(header.beneficiary);
                expect_txs.push(Message::Legacy {
                    chain_id: Some(self.chain_id),
                    nonce: Default::default(),
                    gas_price: U256::ZERO,
                    gas_limit: (std::u64::MAX / 2).into(),
                    value: total_reward,
                    action: TransactionAction::Call(*util::VALIDATOR_CONTRACT),
                    input: Bytes::from(input_data),
                });
            }

            if system_txs.len() != expect_txs.len() {
                return Err(ParliaError::SystemTxWrongCount {
                    expect: expect_txs.len(),
                    got: system_txs.len(),
                }.into());
            }
            for (i, expect) in expect_txs.iter().enumerate() {
                let actual = system_txs.get(i).unwrap();
                if !util::is_similar_tx(expect, &actual.message) {
                    return Err(ParliaError::SystemTxWrong {
                        expect: expect.clone(),
                        got: actual.message.clone(),
                    }.into());
                }
            }
        }
        Ok(Vec::new())
    }

    fn new_block(
        &mut self,
        _header: &BlockHeader,
        state: ConsensusNewBlockState
    ) -> Result<(), DuoError> {
        if let ConsensusNewBlockState::Parlia(state) = state {
            self.new_block_state = state;
            return Ok(());
        }
        Err(ParliaError::WrongConsensusParam.into())
    }

    fn snapshot(
        &mut self,
        db: &dyn SnapRW,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<(), DuoError> {
        let mut snap_cache = self.recent_snaps.write();

        let mut block_number = block_number;
        let mut block_hash = block_hash;
        let mut skip_headers = Vec::new();

        let mut snap: Snapshot;
        loop {
            if let Some(cached) = snap_cache.get_mut(&block_hash) {
                snap = cached.clone();
                break;
            }
            if block_number % CHECKPOINT_INTERVAL == 0 {
                if let Some(cached) = db.read_snap(block_hash)? {
                    debug!("snap find from db {} {:?}", block_number, block_hash);
                    snap = cached;
                    break;
                }
            }
            if block_number == 0 {
                let header = db.read_header(block_number, block_hash)?
                    .ok_or_else(|| ParliaError::UnknownHeader {
                        number: block_number,
                        hash: block_hash,
                    })?;
                let validators = util::parse_epoch_validators(&header.extra_data[VANITY_LENGTH..(header.extra_data.len() - SIGNATURE_LENGTH)])?;
                snap = Snapshot::new(validators, block_number.0, block_hash, self.epoch);
                break;
            }
            let header = db.read_header(block_number, block_hash)?
                .ok_or_else(|| ParliaError::UnknownHeader {
                    number: block_number,
                    hash: block_hash,
                })?;
            block_hash = header.parent_hash;
            block_number = BlockNumber(header.number.0 - 1);
            skip_headers.push(header);
        }
        for h in skip_headers.iter().rev() {
            snap = snap.apply(db, h, self.chain_id)?;
        }

        snap_cache.insert(snap.block_hash, snap.clone());
        if snap.block_number % CHECKPOINT_INTERVAL == 0 {
            debug!("snap save {} {:?}", snap.block_number, snap.block_hash);
            db.write_snap(&snap)?;
        }
        return Ok(());
    }
}

impl Parlia {

    fn query_snap(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> Result<Snapshot, DuoError> {
        let mut snap_by_hash = self.recent_snaps.write();
        if let Some(new_snap) = snap_by_hash.get_mut(&block_hash) {
            return Ok(new_snap.clone());
        }
        return Err(ParliaError::SnapNotFound {
            number: BlockNumber(block_number),
            hash: block_hash,
        }.into());
    }

    fn verify_block_time_for_ramanujan_fork(
        &self,
        snap: &Snapshot,
        header: &BlockHeader,
        parent: &BlockHeader,
    ) -> anyhow::Result<(), DuoError> {
        if self.chain_spec.is_ramanujan(&header.number) {
            if header.timestamp
                < parent.timestamp + self.period + back_off_time(snap, &header.beneficiary)
            {
                return Err(ValidationError::InvalidTimestamp {
                    parent: parent.timestamp,
                    current: header.timestamp,
                }.into());
            }
        }
        Ok(())
    }
}

pub fn parse_parlia_new_block_state<'r, S>(
    chain_spec: &ChainSpec,
    header: &BlockHeader,
    state: &mut IntraBlockState<'r, S>,
) -> anyhow::Result<ParliaNewBlockState>
    where
        S: StateReader + HeaderReader,
{
    debug!("new_block {} {:?}", header.number, header.hash());
    let (_period, epoch) = match chain_spec.consensus.seal_verification {
        SealVerificationParams::Parlia{ period, epoch,} => {
            (period, epoch)
        },
        _ => {
            return Err(ParliaError::WrongConsensusParam.into());
        }
    };
    contract_upgrade::upgrade_build_in_system_contract(chain_spec, &header.number, state)?;
    // cache before executed, then validate epoch
    if header.number % epoch == 0 {
        let parent_header = state.db().read_parent_header(header)?
            .ok_or_else(|| ParliaError::UnknownHeader {
                number: BlockNumber(header.number.0-1),
                hash: header.parent_hash
            })?;
        return Ok(ParliaNewBlockState::new(Some(query_validators(chain_spec, &parent_header, state)?)));
    }
    Ok(ParliaNewBlockState::new(None))
}

/// query_validators query validators from VALIDATOR_CONTRACT
fn query_validators<'r, S>(
    chain_spec: &ChainSpec,
    header: &BlockHeader,
    state: &mut IntraBlockState<'r, S>,
) -> anyhow::Result<Vec<Address>, DuoError>
    where
        S: StateReader + HeaderReader,
{
    let input_bytes = Bytes::from(if chain_spec.is_euler(&header.number) {
        let (input, _) = validator_ins::functions::get_mining_validators::call();
        input
    } else {
        let (input, _) = validator_ins::functions::get_validators::call();
        input
    });

    let message = Message::Legacy {
        chain_id: Some(chain_spec.params.chain_id),
        nonce: header.nonce.to_low_u64_be(),
        gas_price: U256::ZERO,
        gas_limit: 50000000,
        action: TransactionAction::Call(util::VALIDATOR_CONTRACT.clone()),
        value: U256::ZERO,
        input: input_bytes,
    };

    let mut analysis_cache = AnalysisCache::default();
    let mut tracer = NoopTracer;
    let block_spec = chain_spec.collect_block_spec(header.number);
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

    let validator_addrs = if chain_spec.is_euler(&header.number) {
        let (_, decoder) = validator_ins::functions::get_mining_validators::call();
        decoder.decode(res.output_data.chunk())
    } else {
        let (_, decoder) = validator_ins::functions::get_validators::call();
        decoder.decode(res.output_data.chunk())
    }?;

    let mut validators = BTreeSet::new();
    for addr in validator_addrs {
        validators.insert(Address::from(addr));
    }
    Ok(validators.into_iter().collect())
}

fn back_off_time(snap: &Snapshot, val: &Address) -> u64 {
    if snap.inturn(val) {
        return 0;
    } else {
        let idx = snap.index_of(val);
        if idx < 0 {
            // The backOffTime does not matter when a validator is not authorized.
            return 0;
        }
        let mut rng = RngSource::new(snap.block_number as i64);
        let mut y = Vec::new();
        let n = snap.validators.len();
        for i in 0..n {
            y.insert(i, i as u64);
        }
        y.shuffle(&mut rng);
        y[idx as usize]
    }
}
