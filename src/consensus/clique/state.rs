use crate::{
    consensus::{clique, clique::EXTRA_VANITY, CliqueError, DuoError, ValidationError},
    models::{BlockHeader, BlockNumber},
};
use ethereum_types::Address;
use ethnum::U256;
use primitive_types::H256;
use std::collections::BTreeMap;

const NONCE_AUTH: u64 = 0xffffffffffffffff;
const NONCE_DROP: u64 = 0x0000000000000000;
const DIFF_NOTURN: U256 = U256::ONE;
const DIFF_INTURN: U256 = U256::new(2);

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

#[derive(Debug)]
pub struct CliqueBlock {
    signer: Address,
    vote: Option<Vote>,
    number: BlockNumber,
    checkpoint: Vec<Address>,
    vanity: Vec<u8>,
    in_turn: bool,
    timestamp: u64,
}

impl CliqueBlock {
    fn parse_extra_data(extra_data: &[u8]) -> Result<(Vec<Address>, Vec<u8>, Vec<u8>), DuoError> {
        let addresses = clique::parse_checkpoint(extra_data)?;
        let vanity = extra_data[..EXTRA_VANITY].to_vec();
        let signature = extra_data[(EXTRA_VANITY + 20usize * addresses.len())..].to_vec();

        Ok((addresses, vanity, signature))
    }

    pub(crate) fn from_header(header: &BlockHeader) -> Result<Self, DuoError> {
        let in_turn = match header.difficulty {
            DIFF_INTURN => true,
            DIFF_NOTURN => false,
            _ => {
                return Err(ValidationError::WrongDifficulty.into());
            }
        };

        let (checkpoint, vanity, _) = CliqueBlock::parse_extra_data(&header.extra_data)?;

        let vote = Vote::from_data(header.beneficiary, header.nonce.to_low_u64_be())?;
        let signer = clique::recover_signer(header)?;

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
    block_hash: Option<H256>,
    epoch: u64,
}

impl CliqueState {
    pub(crate) fn new(epoch: u64) -> Self {
        Self {
            signers: Signers::new(),
            history: History::new(),
            votes: Votes::new(0),
            block_hash: None,
            epoch,
        }
    }

    pub(crate) fn match_block_hash(&self, hash: H256) -> bool {
        match self.block_hash {
            Some(expected_hash) => expected_hash == hash,
            None => false,
        }
    }

    pub(crate) fn set_block_hash(&mut self, hash: H256) {
        self.block_hash = Some(hash);
    }

    pub(crate) fn set_signers(&mut self, signers: Vec<Address>) {
        self.signers = Signers(signers);
        self.votes.set_threshold(self.signers.limit());
    }

    fn is_epoch(&self, number: BlockNumber) -> bool {
        number.0 % self.epoch == 0
    }

    pub(crate) fn validate(&mut self, block: &CliqueBlock) -> Result<(), DuoError> {
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
                return Err(CliqueError::SignedRecently {
                    signer: candidate,
                    current: block.number,
                    last: *last_signed_block,
                    limit: self.signers.limit() as u64,
                }
                .into());
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

    pub(crate) fn finalize(&mut self, block: CliqueBlock) {
        self.history.insert(block.signer, block.number);

        if self.is_epoch(block.number) {
            self.votes.clear();
        } else if let Some(ref vote) = &block.vote {
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
}
