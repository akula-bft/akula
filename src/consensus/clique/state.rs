use crate::{
    consensus::{clique, clique::EXTRA_VANITY, CliqueError, DuoError, ValidationError},
    models::{BlockHeader, BlockNumber},
};
use ethereum_types::Address;
use ethnum::U256;
use primitive_types::H256;
use std::collections::BTreeMap;
use tracing::*;

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

    fn tally(&mut self, address: Address, vote: &Vote) -> Option<bool> {
        self.votes
            .entry(address)
            .or_insert_with(BTreeMap::new)
            .insert(vote.beneficiary, vote.authorize);

        let mut votes_auth = 0;
        let mut votes_deauth = 0;

        for signer_votes in self.votes.values_mut() {
            if let Some(vote) = signer_votes.get(&vote.beneficiary) {
                if *vote {
                    votes_auth += 1;
                } else {
                    votes_deauth += 1;
                }
            }
        }

        if votes_auth >= self.threshold {
            Some(true)
        } else if votes_deauth >= self.threshold {
            Some(false)
        } else {
            None
        }
    }

    fn clear(&mut self) {
        self.votes.clear();
    }

    fn clear_votes_by(&mut self, signer: Address) {
        self.votes.remove(&signer);
    }

    fn clear_votes_for(&mut self, beneficiary: Address) {
        for signer_votes in self.votes.values_mut() {
            signer_votes.remove(&beneficiary);
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

    fn compare_checkpoint(&self, checkpoint: &[Address]) -> bool {
        self.0 == checkpoint
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

    fn remove(&mut self, address: Address) {
        self.0.remove(&address);
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

    pub(crate) fn validate(
        &mut self,
        block: &CliqueBlock,
        skip_in_turn_check: bool,
    ) -> Result<(), ValidationError> {
        let candidate = block.signer;

        let index = match self.signers.find(candidate) {
            Some(i) => i,
            None => {
                return Err(CliqueError::UnknownSigner { signer: candidate }.into());
            }
        };

        if !skip_in_turn_check {
            let in_turn = block.number % self.signers.count() == index;
            if in_turn ^ block.in_turn {
                return Err(ValidationError::WrongDifficulty);
            }
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
            if let Some(authorize) = self.votes.tally(block.signer, vote) {
                if authorize {
                    trace!("Adding signer {}", vote.beneficiary);
                    self.signers.insert(vote.beneficiary);
                } else {
                    trace!("Removing signer {}", vote.beneficiary);
                    self.signers.remove(vote.beneficiary);
                    self.history.remove(vote.beneficiary);
                    self.votes.clear_votes_by(vote.beneficiary);
                }

                self.votes.clear_votes_for(vote.beneficiary);
                self.votes.set_threshold(self.signers.limit());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_types::H160;
    use hex_literal::hex;

    #[test]
    fn eip225_test_vectors() {
        const A: Address = H160(hex!("A000000000000000000000000000000000000000"));
        const B: Address = H160(hex!("B000000000000000000000000000000000000000"));
        const C: Address = H160(hex!("C000000000000000000000000000000000000000"));
        const D: Address = H160(hex!("D000000000000000000000000000000000000000"));
        const E: Address = H160(hex!("E000000000000000000000000000000000000000"));
        const F: Address = H160(hex!("F000000000000000000000000000000000000000"));

        #[derive(Clone, Debug, PartialEq, Eq)]
        enum AuthMeta {
            Inert,
            VoteMeta(Address, bool),
            CheckpointMeta(Vec<Address>),
        }
        use AuthMeta::*;

        /// Define the various voting scenarios to test
        struct Test {
            description: &'static str,
            epoch: u64,
            signers: Vec<Address>,
            blocks: Vec<(Address, AuthMeta)>,
            results: Result<Vec<Address>, ValidationError>,
        }

        impl Default for Test {
            fn default() -> Self {
                Self {
                    description: "",
                    epoch: 30_000,
                    signers: Default::default(),
                    blocks: Default::default(),
                    results: Ok(vec![]),
                }
            }
        }

        for Test {
            description,
            epoch,
            signers,
            blocks,
            results,
        } in [
            Test {
                description: "Single signer, no votes cast",
                signers: vec![A],
                blocks: vec![(A, Inert)],
                results: Ok(vec![A]),
                ..Default::default()
            },
            Test {
                description: "Single signer, voting to add two others (only accept first, second needs 2 votes)",
                signers: vec![A],
                blocks: vec![(A, VoteMeta(B, true)), (B, Inert), (A, VoteMeta(C, true))],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Two signers, voting to add three others (only accept first two, third needs 3 votes already)",
                signers: vec![A, B],
                blocks: vec![
                    (A, VoteMeta(C, true)),
                    (B, VoteMeta(C, true)),
                    (A, VoteMeta(D, true)),
                    (B, VoteMeta(D, true)),
                    (C, Inert),
                    (A, VoteMeta(E, true)),
                    (B, VoteMeta(E, true)),
                ],
                results: Ok(vec![A, B, C, D]),
                ..Default::default()
            },
            Test {
                description: "Single signer, dropping itself (weird, but one less cornercase by explicitly allowing this)",
                signers: vec![A],
                blocks: vec![(A, VoteMeta(A, false))],
                results: Ok(vec![]),
                ..Default::default()
            },
            Test {
                description: "Two signers, actually needing mutual consent to drop either of them (not fulfilled)",
                signers: vec![A, B],
                blocks: vec![(A, VoteMeta(B, false))],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Two signers, actually needing mutual consent to drop either of them (fulfilled)",
                signers: vec![A, B],
                blocks: vec![(A, VoteMeta(B, false)), (B, VoteMeta(B, false))],
                results: Ok(vec![A]),
                ..Default::default()
            },
            Test {
                description: "Three signers, two of them deciding to drop the third",
                signers: vec![A, B, C],
                blocks: vec![(A, VoteMeta(C, false)), (B, VoteMeta(C, false))],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Four signers, consensus of two not being enough to drop anyone",
                signers: vec![A, B, C, D],
                blocks: vec![(A, VoteMeta(C, false)), (B, VoteMeta(C, false))],
                results: Ok(vec![A, B, C, D]),
                ..Default::default()
            },
            Test {
                description: "Four signers, consensus of three already being enough to drop someone",
                signers: vec![A, B, C, D],
                blocks: vec![
                    (A, VoteMeta(D, false)),
                    (B, VoteMeta(D, false)),
                    (C, VoteMeta(D, false)),
                ],
                results: Ok(vec![A, B, C]),
                ..Default::default()
            },
            Test {
                description: "Authorizations are counted once per signer per target",
                signers: vec![A, B],
                blocks: vec![
                    (A, VoteMeta(C, true)),
                    (B, Inert),
                    (A, VoteMeta(C, true)),
                    (B, Inert),
                    (A, VoteMeta(C, true)),
                ],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Authorizing multiple accounts concurrently is permitted",
                signers: vec![A, B],
                blocks: vec![
                    (A, VoteMeta(C, true)),
                    (B, Inert),
                    (A, VoteMeta(D, true)),
                    (B, Inert),
                    (A, Inert),
                    (B, VoteMeta(D, true)),
                    (A, Inert),
                    (B, VoteMeta(C, true)),
                ],
                results: Ok(vec![A, B, C, D]),
                ..Default::default()
            },
            Test {
                description: "Deauthorizations are counted once per signer per target",
                signers: vec![A, B],
                blocks:  vec![
                    (A, VoteMeta(B, false)),
                    (B, Inert),
                    (A, VoteMeta(B, false)),
                    (B, Inert),
                    (A, VoteMeta(B, false)),
                ],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Deauthorizing multiple accounts concurrently is permitted",
                signers: vec![A, B, C, D],
                blocks:  vec![
                    (A, VoteMeta(C, false)),
                    (B, Inert),
                    (C, Inert),
                    (A, VoteMeta(D, false)),
                    (B, Inert),
                    (C, Inert),
                    (A, Inert),
                    (B, VoteMeta(D, false)),
                    (C, VoteMeta(D, false)),
                    (A, Inert),
                    (B, VoteMeta(C, false)),
                ],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Votes from deauthorized signers are discarded immediately (deauth votes)",
                signers: vec![A, B, C],
                blocks:  vec![
                    (C, VoteMeta(B, false)),
                    (A, VoteMeta(C, false)),
                    (B, VoteMeta(C, false)),
                    (A, VoteMeta(B, false)),
                ],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Votes from deauthorized signers are discarded immediately (auth votes)",
                signers: vec![A, B, C],
                blocks:  vec![
                    (C, VoteMeta(D, true)),
                    (A, VoteMeta(C, false)),
                    (B, VoteMeta(C, false)),
                    (A, VoteMeta(D, true)),
                ],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Cascading changes are not allowed, only the account being voted on may change",
                signers: vec![A, B, C, D],
                blocks:  vec![
                    (A, VoteMeta(C, false)),
                    (B, Inert),
                    (C, Inert),
                    (A, VoteMeta(D, false)),
                    (B, VoteMeta(C, false)),
                    (C, Inert),
                    (A, Inert),
                    (B, VoteMeta(D, false)),
                    (C, VoteMeta(D, false)),
                ],
                results: Ok(vec![A, B, C]),
                ..Default::default()
            },
            Test {
                description: "Changes reaching consensus out of bounds (via a deauth) execute on touch",
                signers: vec![A, B, C, D],
                blocks:  vec![
                    (A, VoteMeta(C, false)),
                    (B, Inert),
                    (C, Inert),
                    (A, VoteMeta(D, false)),
                    (B, VoteMeta(C, false)),
                    (C, Inert),
                    (A, Inert),
                    (B, VoteMeta(D, false)),
                    (C, VoteMeta(D, false)),
                    (A, Inert),
                    (C, VoteMeta(C, true)),
                ],
                results: Ok(vec![A, B]),
                ..Default::default()
            },
            Test {
                description: "Changes reaching consensus out of bounds (via a deauth) may go out of consensus on first touch",
                signers: vec![A, B, C, D],
                blocks:  vec![
                    (A, VoteMeta(C, false)),
                    (B, Inert),
                    (C, Inert),
                    (A, VoteMeta(D, false)),
                    (B, VoteMeta(C, false)),
                    (C, Inert),
                    (A, Inert),
                    (B, VoteMeta(D, false)),
                    (C, VoteMeta(D, false)),
                    (A, Inert),
                    (B, VoteMeta(C, true)),
                ],
                results: Ok(vec![A, B, C]),
                ..Default::default()
            },
            Test {
                description: "Ensure that pending votes don't survive authorization status changes. This corner case can only appear if a signer is quickly added, removed and then readded (or the inverse), while one of the original voters dropped. If a past vote is left cached in the system somewhere, this will interfere with the final signer outcome.",
                signers: vec![A, B, C, D, E],
                blocks:  vec![
                    (A, VoteMeta(F, true)), // Authorize F, 3 votes needed
                    (B, VoteMeta(F, true)),
                    (C, VoteMeta(F, true)),
                    (D, VoteMeta(F, false)), // Deauthorize F, 4 votes needed (leave A's previous vote "unchanged")
                    (E, VoteMeta(F, false)),
                    (B, VoteMeta(F, false)),
                    (C, VoteMeta(F, false)),
                    (D, VoteMeta(F, true)), // Almost authorize F, 2/3 votes needed
                    (E, VoteMeta(F, true)),
                    (B, VoteMeta(A, false)), // Deauthorize A, 3 votes needed
                    (C, VoteMeta(A, false)),
                    (D, VoteMeta(A, false)),
                    (B, VoteMeta(F, true)), // Finish authorizing F, 3/3 votes needed
                ],
                results: Ok(vec![B, C, D, E, F]),
                ..Default::default()
            },
            Test {
                description: "Epoch transitions reset all votes to allow chain checkpointing",
                epoch:   3,
                signers: vec![A, B],
                blocks:  vec![
                    (A, VoteMeta(C, true)),
                    (B, Inert),
                    (A, CheckpointMeta(vec![A, B])),
                    (B, VoteMeta(C, true)),
                ],
                results: Ok(vec![A, B]),
            },
            Test {
                description: "An unauthorized signer should not be able to sign blocks",
                signers: vec![A],
                blocks:  vec![
                    (B, Inert),
                ],
                results: Err(ValidationError::CliqueError(CliqueError::UnknownSigner { signer: B })),
                ..Default::default()
            },
            Test {
                description: "An authorized signer that signed recenty should not be able to sign again",
                signers: vec![A, B],
                blocks: vec![
                    (A, Inert),
                    (A, Inert),
                ],
                results: Err(ValidationError::CliqueError(CliqueError::SignedRecently { current: 2.into(), last: 1.into(), signer: A, limit: 2 })),
                ..Default::default()
            },
            Test {
                description: "Recent signatures should not reset on checkpoint blocks imported in a batch",
                epoch:   3,
                signers: vec![A, B, C],
                blocks:  vec![
                    (A, Inert),
                    (B, Inert),
                    (A, CheckpointMeta(vec![A, B, C])),
                    (A, Inert),
                ],
                results: Err(ValidationError::CliqueError(CliqueError::SignedRecently { current: 4.into(), last: 3.into(), signer: A, limit: 2 })),
            },
        ] {
            println!("{description}");

            let res: Result<_, ValidationError> = (move || {
                let mut state = CliqueState::new(epoch);

                state.set_signers(signers);

                for (number, (signer, auth_meta)) in blocks.into_iter().enumerate() {
                    let (vote, checkpoint) = match auth_meta {
                        VoteMeta(vote, auth) => (Some((vote, auth)), None),
                        CheckpointMeta(checkpoint) => (None, Some(checkpoint)),
                        Inert => (None, None),
                    };

                    let block = CliqueBlock {
                        signer,
                        vote: vote.map(|(beneficiary, authorize)| Vote {
                            beneficiary,
                            authorize,
                        }),
                        number: (number as u64 + 1).into(),
                        checkpoint: checkpoint.unwrap_or_default(),
                        vanity: vec![],
                        in_turn: false,
                        timestamp: 0,
                    };
                    state.validate(&block, true)?;
                    state.finalize(block);
                }

                Ok(state.signers.0.clone())
            })();

            assert_eq!(res, results);
        }
    }
}
