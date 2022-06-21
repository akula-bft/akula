use crate::{
    consensus::{
        CliqueError, Consensus, ConsensusEngineBase, ConsensusState, DuoError, FinalizationChange,
        ValidationError,
    },
    kv::{
        mdbx::{MdbxCursor, MdbxTransaction},
        tables,
    },
    models::{Block, BlockHeader, BlockNumber, ChainId, Genesis, Revision, Seal},
    BlockState,
};
use anyhow::bail;
use bytes::Bytes;
use ethereum_types::Address;
use ethnum::U256;
use mdbx::{EnvironmentKind, TransactionKind};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message as SecpMessage, SECP256K1,
};
use sha3::{Digest, Keccak256};
use std::{collections::BTreeMap, sync::Mutex, time::Duration, unreachable};

const EPOCH_LENGTH: usize = 30000;
const BLOCK_PERIOD: u64 = 15;
const EXTRA_VANITY: usize = 32;
const EXTRA_SEAL: usize = 65;
const NONCE_AUTH: u64 = 0xffffffffffffffff;
const NONCE_DROP: u64 = 0x0000000000000000;
const DIFF_NOTURN: U256 = U256::ONE;
const DIFF_INTURN: U256 = U256::new(2);

fn recover_signer(header: &BlockHeader) -> Result<Address, anyhow::Error> {
    let signature_offset = header.extra_data.len() - EXTRA_SEAL;

    let sig = &header.extra_data[signature_offset..signature_offset + 64];
    let rec = RecoveryId::from_i32(header.extra_data[signature_offset + 64] as i32)?;
    let signature = RecoverableSignature::from_compact(sig, rec)?;

    let mut sig_hash_header = header.clone();
    sig_hash_header.extra_data = Bytes::copy_from_slice(&header.extra_data[..signature_offset]);
    let message = &SecpMessage::from_slice(sig_hash_header.hash().as_bytes())?;

    let public = &SECP256K1.recover_ecdsa(message, &signature)?;
    let address_slice = &Keccak256::digest(&public.serialize_uncompressed()[1..])[12..];

    Ok(Address::from_slice(address_slice))
}

#[derive(Clone, Debug, PartialEq)]
struct Vote {
    beneficiary: Address,
    authorize: bool,
}

impl Vote {
    fn new(beneficiary: Address, authorize: bool) -> Self {
        Self {
            beneficiary,
            authorize,
        }
    }

    fn from_data(beneficiary: Address, nonce: u64) -> Result<Option<Self>, DuoError> {
        if beneficiary.is_zero() {
            Ok(None)
        } else {
            let authorize = match nonce {
                NONCE_AUTH => true,
                NONCE_DROP => false,
                _ => return Err(CliqueError::WrongNonce { nonce }.into()),
            };
            Ok(Some(Vote::new(beneficiary, authorize)))
        }
    }
}

#[derive(Debug)]
struct Votes {
    votes: BTreeMap<Address, BTreeMap<Address, bool>>,
    threshold: usize,
}

impl Votes {
    fn new(threshold: usize) -> Self {
        Self {
            votes: BTreeMap::new(),
            threshold,
        }
    }

    fn tally(&mut self, address: Address, vote: &Vote) -> bool {
        self.votes
            .entry(address)
            .or_insert_with(BTreeMap::new)
            .insert(vote.beneficiary, vote.authorize);

        let count = self
            .votes
            .values()
            .filter(|v| v.get(&vote.beneficiary) == Some(&vote.authorize))
            .count();

        count >= self.threshold
    }

    fn clear(&mut self) {
        self.votes.clear();
    }

    fn clear_votes_for(&mut self, beneficiary: &Address) {
        for signer_votes in self.votes.values_mut() {
            signer_votes.remove(beneficiary);
        }
    }

    fn set_threshold(&mut self, new_threshold: usize) {
        self.threshold = new_threshold;
    }
}

#[derive(Debug)]
struct Signers(Vec<Address>);

impl Signers {
    fn new() -> Self {
        Self(vec![])
    }

    fn count(&self) -> usize {
        self.0.len()
    }

    fn limit(&self) -> usize {
        self.count() / 2 + 1
    }

    fn find(&self, address: Address) -> Option<usize> {
        self.0.binary_search(&address).ok()
    }

    fn compare_checkpoint(&self, checkpoint: &Vec<Address>) -> bool {
        &self.0 == checkpoint
    }

    fn insert(&mut self, new_signer: Address) {
        if let Err(index) = self.0.binary_search(&new_signer) {
            self.0.insert(index, new_signer);
        }
    }

    fn remove(&mut self, former_signer: Address) {
        if let Ok(index) = self.0.binary_search(&former_signer) {
            self.0.remove(index);
        }
    }
}

fn parse_checkpoint(extra_data: &[u8]) -> Result<Vec<Address>, DuoError> {
    let addresses_length = extra_data.len() as isize - (EXTRA_VANITY + EXTRA_SEAL) as isize;

    if addresses_length < 0 || addresses_length % 20 != 0 {
        return Err(CliqueError::WrongExtraData.into());
    };

    let mut addresses = vec![];

    for offset in (EXTRA_VANITY..(EXTRA_VANITY + addresses_length as usize)).step_by(20) {
        let next_address = Address::from_slice(&extra_data[offset..offset + 20]);
        addresses.push(next_address);
    }

    for index in 1..addresses.len() {
        if addresses[index - 1].ge(&addresses[index]) {
            return Err(CliqueError::InvalidCheckpoint.into());
        }
    }

    Ok(addresses)
}

#[derive(Debug)]
struct CliqueBlock {
    signer: Address,
    vote: Option<Vote>,
    number: BlockNumber,
    checkpoint: Vec<Address>,
    vanity: Vec<u8>,
    in_turn: bool,
    timestamp: u64,
}

impl CliqueBlock {
    fn is_epoch(&self) -> bool {
        self.number % EPOCH_LENGTH == 0
    }

    fn parse_extra_data(extra_data: &[u8]) -> Result<(Vec<Address>, Vec<u8>, Vec<u8>), DuoError> {
        let addresses = parse_checkpoint(extra_data)?;
        let vanity = extra_data[..EXTRA_VANITY].to_vec();
        let signature = extra_data[(EXTRA_VANITY + 20usize * addresses.len())..].to_vec();

        Ok((addresses, vanity, signature))
    }

    fn from_header(header: &BlockHeader) -> Result<Self, DuoError> {
        let in_turn = match header.difficulty {
            DIFF_INTURN => true,
            DIFF_NOTURN => false,
            _ => {
                return Err(ValidationError::WrongDifficulty.into());
            }
        };

        let (checkpoint, vanity, _) = CliqueBlock::parse_extra_data(&header.extra_data)?;

        let vote = Vote::from_data(header.beneficiary, header.nonce.to_low_u64_be())?;
        let signer = recover_signer(header)?;

        Ok(CliqueBlock {
            signer,
            vote,
            number: header.number,
            checkpoint,
            vanity,
            in_turn,
            timestamp: header.timestamp,
        })
    }
}

#[derive(Debug)]
struct History(BTreeMap<Address, BlockNumber>);

impl History {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    fn find(&self, address: Address) -> Option<&BlockNumber> {
        self.0.get(&address)
    }

    fn insert(&mut self, address: Address, block_number: BlockNumber) {
        self.0.insert(address, block_number);
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

#[derive(Debug)]
pub struct CliqueState {
    signers: Signers,
    history: History,
    votes: Votes,
    epoch: u64,
}

impl CliqueState {
    fn new(epoch: u64) -> Self {
        Self {
            signers: Signers::new(),
            history: History::new(),
            votes: Votes::new(0),
            epoch,
        }
    }

    fn set_signers(&mut self, signers: Vec<Address>) {
        self.signers = Signers(signers);
        self.votes.set_threshold(self.signers.limit());
    }

    fn is_epoch(&self, number: BlockNumber) -> bool {
        number.0 % self.epoch == 0
    }

    fn validate(&mut self, block: &CliqueBlock) -> Result<(), DuoError> {
        let candidate = block.signer;

        let index = match self.signers.find(candidate) {
            Some(i) => i,
            None => {
                return Err(CliqueError::UnknownSigner { signer: candidate }.into());
            }
        };

        let in_turn = block.number % self.signers.count() == index;
        if in_turn ^ block.in_turn {
            return Err(ValidationError::WrongDifficulty.into());
        }

        if let Some(last_signed_block) = self.history.find(candidate) {
            let previous = block.number.0 - last_signed_block.0;
            if (previous as usize) < self.signers.limit() {
                return Err(CliqueError::SignedRecently { signer: candidate }.into());
            }
        }

        if self.is_epoch(block.number) {
            if block.vote.is_some() {
                return Err(CliqueError::VoteInEpochBlock.into());
            }
            if !self.signers.compare_checkpoint(&block.checkpoint) {
                return Err(CliqueError::CheckpointMismatch {
                    expected: self.signers.0.clone(),
                    got: block.checkpoint.clone(),
                }
                .into());
            }
        } else if !block.checkpoint.is_empty() {
            return Err(CliqueError::CheckpointInNonEpochBlock.into());
        }

        Ok(())
    }

    fn apply_non_genesis_block(&mut self, block: &CliqueBlock) {
        self.history.insert(block.signer, block.number);

        if self.is_epoch(block.number) {
            self.votes.clear();
        } else if let Some(ref vote) = block.vote {
            let accepted = self.votes.tally(block.signer, vote);

            if accepted {
                if vote.authorize {
                    self.signers.insert(vote.beneficiary);
                } else {
                    self.signers.remove(vote.beneficiary);
                }

                self.votes.clear_votes_for(&vote.beneficiary);
                self.votes.set_threshold(self.signers.limit());
            }
        }
    }

    fn apply_genesis_block(&mut self, genesis: &CliqueBlock) {
        for signer in &genesis.checkpoint {
            self.signers.insert(*signer);
        }
        self.votes.set_threshold(self.signers.limit());
    }

    fn finalize(&mut self, block: CliqueBlock) -> anyhow::Result<()> {
        if block.number == 0 {
            self.apply_genesis_block(&block);
        } else {
            self.apply_non_genesis_block(&block);
        }

        Ok(())
    }
}

fn get_header<K: TransactionKind>(
    cursor: &mut MdbxCursor<'_, K, tables::Header>,
    height: BlockNumber,
) -> anyhow::Result<BlockHeader> {
    Ok(match cursor.seek(height)? {
        Some(((found_height, _), header)) if found_height == height => header,
        _ => bail!("Last epoch header missing from database."),
    })
}

pub fn recover_signers_from_epoch_block<T: TransactionKind, E: EnvironmentKind>(
    tx: &MdbxTransaction<'_, T, E>,
    current_epoch: BlockNumber,
) -> anyhow::Result<Vec<Address>> {
    let mut cursor = tx.cursor(tables::Header)?;
    let epoch_header = get_header(&mut cursor, current_epoch)?;
    Ok(parse_checkpoint(epoch_header.extra_data.as_ref())?)
}

pub fn fast_forward_within_epoch<T: TransactionKind, E: EnvironmentKind>(
    state: &mut CliqueState,
    tx: &MdbxTransaction<'_, T, E>,
    latest_epoch: BlockNumber,
    starting_block: BlockNumber,
) -> anyhow::Result<()> {
    let mut cursor = tx.cursor(tables::Header)?;

    for height in latest_epoch + 1..starting_block {
        state.finalize(CliqueBlock::from_header(&get_header(&mut cursor, height)?)?)?;
    }

    Ok(())
}

pub fn recover_clique_state<T: TransactionKind, E: EnvironmentKind>(
    tx: &MdbxTransaction<'_, T, E>,
    genesis: &Genesis,
    epoch: u64,
    starting_block: BlockNumber,
) -> anyhow::Result<CliqueState> {
    let mut state = CliqueState::new(epoch);

    let blocks_into_epoch = starting_block % epoch;
    let latest_epoch = starting_block - blocks_into_epoch;

    let begin_of_epoch_signers = if latest_epoch == 0 {
        if let Seal::Clique {
            vanity: _,
            score: _,
            signers,
        } = &genesis.seal
        {
            signers.clone()
        } else {
            unreachable!("This should only be called if consensus algorithm is Clique.");
        }
    } else {
        recover_signers_from_epoch_block(tx, latest_epoch)?
    };

    state.set_signers(begin_of_epoch_signers);

    if blocks_into_epoch > 0 {
        fast_forward_within_epoch(&mut state, tx, latest_epoch, starting_block)?;
    }

    Ok(state)
}

#[derive(Debug)]
pub struct Clique {
    base: ConsensusEngineBase,
    state: Mutex<CliqueState>,
    period: u64,
}

impl Clique {
    pub(crate) fn new(
        chain_id: ChainId,
        eip1559_block: Option<BlockNumber>,
        period: Duration,
        epoch: u64,
        initial_signers: Vec<Address>,
    ) -> Self {
        let mut state = CliqueState::new(epoch);
        state.set_signers(initial_signers);
        Self {
            base: ConsensusEngineBase::new(chain_id, eip1559_block, None, 5000, false),
            state: Mutex::new(state),
            period: period.as_secs(),
        }
    }
}

impl Consensus for Clique {
    fn pre_validate_block(&self, block: &Block, state: &dyn BlockState) -> Result<(), DuoError> {
        self.base.pre_validate_block(block, state)?;
        Ok(())
    }

    fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        with_future_timestamp_check: bool,
    ) -> Result<(), DuoError> {
        self.base
            .validate_block_header(header, parent, with_future_timestamp_check)?;

        if header.timestamp - parent.timestamp < self.period {
            return Err(ValidationError::InvalidTimestamp {
                parent: parent.timestamp,
                current: header.timestamp,
            }
            .into());
        };

        Ok(())
    }

    fn finalize(
        &self,
        block: &BlockHeader,
        _ommers: &[BlockHeader],
        _revision: Revision,
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        let clique_block = CliqueBlock::from_header(block)?;

        let mut state = self.state.lock().unwrap();

        state.validate(&clique_block)?;
        state.finalize(clique_block)?;

        Ok(vec![])
    }

    fn set_state(&mut self, state: ConsensusState) {
        if let ConsensusState::Clique(state) = state {
            self.state = Mutex::new(state);
        } else {
            unreachable!("Expected clique ConsensusState.");
        }
    }

    fn get_beneficiary(&self, header: &BlockHeader) -> Address {
        recover_signer(header).unwrap()
    }
}
