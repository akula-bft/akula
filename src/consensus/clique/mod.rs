pub mod state;
pub use state::CliqueState;

use crate::{
    consensus::{
        fork_choice_graph::ForkChoiceGraph, state::CliqueBlock, CliqueError, Consensus,
        ConsensusEngineBase, ConsensusState, DuoError, FinalizationChange, ForkChoiceMode,
        ValidationError,
    },
    kv::{
        mdbx::{MdbxCursor, MdbxTransaction},
        tables,
    },
    models::{Block, BlockHeader, BlockNumber, ChainConfig, ChainId, ChainSpec, Seal, MessageWithSender},
    BlockReader,StateReader
};
use anyhow::bail;
use bytes::Bytes;
use ethereum_types::Address;
use mdbx::{EnvironmentKind, TransactionKind};
use parking_lot::Mutex;
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message as SecpMessage, SECP256K1,
};
use sha3::{Digest, Keccak256};
use std::{sync::Arc, time::Duration, unreachable};

const EXTRA_VANITY: usize = 32;
const EXTRA_SEAL: usize = 65;

pub fn recover_signer(header: &BlockHeader) -> Result<Address, anyhow::Error> {
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

fn get_header<K: TransactionKind>(
    cursor: &mut MdbxCursor<'_, K, tables::Header>,
    height: BlockNumber,
) -> anyhow::Result<BlockHeader> {
    Ok(match cursor.seek(height)? {
        Some(((found_height, _), header)) if found_height == height => header,
        _ => bail!("Header for block {} missing from database.", height),
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
        state.finalize(CliqueBlock::from_header(&get_header(&mut cursor, height)?)?);
    }

    Ok(())
}

pub fn recover_clique_state<T: TransactionKind, E: EnvironmentKind>(
    tx: &MdbxTransaction<'_, T, E>,
    chain_spec: &ChainSpec,
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
        } = &chain_spec.genesis.seal
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

    if starting_block > 1 {
        let mut cursor = tx.cursor(tables::Header)?;
        let header = get_header(&mut cursor, starting_block - BlockNumber(1))?;
        state.set_block_hash(header.hash());
    } else {
        let config = ChainConfig::from(chain_spec.clone());
        state.set_block_hash(config.genesis_hash);
    };

    Ok(state)
}

#[derive(Debug)]
pub struct Clique {
    base: ConsensusEngineBase,
    state: Mutex<CliqueState>,
    period: u64,
    fork_choice_graph: Arc<Mutex<ForkChoiceGraph>>,
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
            base: ConsensusEngineBase::new(chain_id, eip1559_block, None),
            state: Mutex::new(state),
            period: period.as_secs(),
            fork_choice_graph: Arc::new(Mutex::new(Default::default())),
        }
    }
}

impl Consensus for Clique {

    fn pre_validate_block(&self, block: &Block, state: &dyn BlockReader) -> Result<(), DuoError> {
        if !block.ommers.is_empty() {
            return Err(ValidationError::TooManyOmmers.into());
        }

        self.base.pre_validate_block(block)?;

        if state.read_parent_header(&block.header)?.is_none() {
            return Err(ValidationError::UnknownParent {
                number: block.header.number,
                parent_hash: block.header.parent_hash,
            }
            .into());
        }

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
        _transactions: Option<&Vec<MessageWithSender>>,
        _state: &dyn StateReader,
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        let clique_block = CliqueBlock::from_header(block)?;

        let mut state = self.state.lock();

        state
            .validate(&clique_block, false)
            .map_err(DuoError::Validation)?;
        state.finalize(clique_block);

        state.set_block_hash(block.hash());

        Ok(vec![])
    }

    fn set_state(&mut self, state: ConsensusState) {
        if let ConsensusState::Clique(state) = state {
            self.state = Mutex::new(state);
        } else {
            unreachable!("Expected clique ConsensusState.");
        }
    }

    fn is_state_valid(&self, next_header: &BlockHeader) -> bool {
        self.state.lock().match_block_hash(next_header.parent_hash)
    }

    fn get_beneficiary(&self, header: &BlockHeader) -> Address {
        recover_signer(header).unwrap()
    }

    fn fork_choice_mode(&self) -> ForkChoiceMode {
        ForkChoiceMode::Difficulty(self.fork_choice_graph.clone())
    }
}
