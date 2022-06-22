pub mod state;

use crate::{
    BlockState,
    consensus::{
        CliqueError, Consensus, ConsensusEngineBase, ConsensusState, DuoError, FinalizationChange,
        ValidationError,
    },
    kv::{
        mdbx::{MdbxCursor, MdbxTransaction},
        tables,
    },
    models::{Block, BlockHeader, BlockNumber, ChainId, Genesis, Revision, Seal},
};
use anyhow::bail;
use bytes::Bytes;
use ethereum_types::Address;
use mdbx::{EnvironmentKind, TransactionKind};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message as SecpMessage, SECP256K1,
};
use sha3::{Digest, Keccak256};
use std::{sync::Mutex, time::Duration, unreachable};
use state::CliqueState;
use crate::consensus::state::CliqueBlock;

const EXTRA_VANITY: usize = 32;
const EXTRA_SEAL: usize = 65;

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
