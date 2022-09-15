mod base;
mod beacon;
mod blockchain;
mod clique;
pub mod fork_choice_graph;
mod parlia;

use self::fork_choice_graph::ForkChoiceGraph;
pub use self::{base::*, beacon::*, blockchain::*, clique::*, parlia::*};
use crate::{
    kv::{mdbx::*, MdbxWithDirHandle},
    models::*,
    BlockReader, HeaderReader,
    state::{IntraBlockState, StateReader}
};
use anyhow::bail;
use derive_more::{Display, From};
use mdbx::{EnvironmentKind, TransactionKind};
use parking_lot::Mutex;
use std::{
    fmt::{Debug, Display},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use std::time::{SystemTimeError};
use tokio::sync::watch;

#[derive(Debug)]
pub enum FinalizationChange {
    Reward {
        address: Address,
        amount: U256,
        ommer: bool,
    },
}

pub enum ConsensusState {
    Stateless,
    Clique(CliqueState),
}

impl ConsensusState {
    pub(crate) fn recover<T: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'_, T, E>,
        chainspec: &ChainSpec,
        starting_block: BlockNumber,
    ) -> anyhow::Result<ConsensusState> {
        Ok(match chainspec.consensus.seal_verification {
            SealVerificationParams::Clique { period: _, epoch } => {
                ConsensusState::Clique(recover_clique_state(tx, chainspec, epoch, starting_block)?)
            }
            SealVerificationParams::Beacon { .. } => ConsensusState::Stateless,
            SealVerificationParams::Parlia { .. } => ConsensusState::Stateless,
        })
    }
}

pub enum ConsensusNewBlockState {
    Stateless,
    Parlia(ParliaNewBlockState),
}

impl ConsensusNewBlockState {
    pub(crate) fn handle<'r, S>(
        chain_spec: &ChainSpec,
        header: &BlockHeader,
        state: &mut IntraBlockState<'r, S>,
    ) -> anyhow::Result<ConsensusNewBlockState>
        where
            S: StateReader + HeaderReader,
    {
        Ok(match chain_spec.consensus.seal_verification {
            SealVerificationParams::Parlia { .. } => {
                ConsensusNewBlockState::Parlia(parse_parlia_new_block_state(chain_spec, header, state)?)
            },
            _ => {
                ConsensusNewBlockState::Stateless
            },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalForkChoice {
    pub head_block: H256,
    pub finalized_block: H256,
}

pub enum ForkChoiceMode {
    External(watch::Receiver<ExternalForkChoice>),
    Difficulty(Arc<Mutex<ForkChoiceGraph>>),
}

pub trait Consensus: Debug + Send + Sync + 'static {

    fn fork_choice_mode(&self) -> ForkChoiceMode;

    /// Performs validation of block header & body that can be done prior to sender recovery and execution.
    /// See YP Sections 4.3.2 "Holistic Validity", 4.3.4 "Block Header Validity", and 11.1 "Ommer Validation".
    ///
    /// NOTE: Shouldn't be used for genesis block.
    fn pre_validate_block(&self, block: &Block, state: &dyn BlockReader) -> Result<(), DuoError>;

    /// See YP Section 4.3.4 "Block Header Validity".
    ///
    /// NOTE: Shouldn't be used for genesis block.
    fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        with_future_timestamp_check: bool,
    ) -> Result<(), DuoError>;

    /// Finalizes block execution by applying changes in the state of accounts or of the consensus itself
    ///
    /// NOTE: For Ethash See YP Section 11.3 "Reward Application".
    fn finalize(
        &self,
        header: &BlockHeader,
        ommers: &[BlockHeader],
        transactions: Option<&Vec<MessageWithSender>>,
        state: &dyn StateReader,
    ) -> anyhow::Result<Vec<FinalizationChange>>;

    /// See YP Section 11.3 "Reward Application".
    fn get_beneficiary(&self, header: &BlockHeader) -> Address {
        header.beneficiary
    }

    /// To be overridden for stateful consensus engines, e. g. PoA engines with a signer list.
    #[allow(unused_variables)]
    fn set_state(&mut self, state: ConsensusState) {}

    /// To be overridden for stateful consensus engines, e. g. PoA engines with a signer list.
    fn new_block(
        &mut self,
        _header: &BlockHeader,
        _state: ConsensusNewBlockState
    ) -> Result<(), DuoError> {
        Ok(())
    }

    /// To be overridden for stateful consensus engines.
    ///
    /// Should return false if the state needs to be recovered, e. g. in case of a reorg.
    #[allow(unused_variables)]
    fn is_state_valid(&self, next_header: &BlockHeader) -> bool {
        true
    }

    fn needs_parallel_validation(&self) -> bool {
        false
    }

    fn validate_header_parallel(&self, _: &BlockHeader) -> Result<(), DuoError> {
        Ok(())
    }

    /// To be overridden for consensus validators' snap.
    fn snapshot(
        &mut self,
        _db: &dyn SnapRW,
        _block_number: BlockNumber,
        _block_hash: H256,
    ) -> anyhow::Result<(), DuoError> {
        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BadTransactionError {
    SenderNoEOA {
        sender: Address,
    }, // EIP-3607: σ[S(T)]c ≠ KEC( () )
    WrongNonce {
        account: Address,
        expected: u64,
        got: u64,
    }, // Tn ≠ σ[S(T)]n
    InsufficientFunds {
        account: Address,
        available: U512,
        required: U512,
    }, // v0 > σ[S(T)]b
    BlockGasLimitExceeded {
        available: u64,
        required: u64,
    }, // Tg > BHl - l(BR)u
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CliqueError {
    UnknownSigner {
        signer: Address,
    },
    SignedRecently {
        signer: Address,
        current: BlockNumber,
        last: BlockNumber,
        limit: u64,
    },
    WrongExtraData,
    WrongNonce {
        nonce: u64,
    },
    VoteInEpochBlock,
    CheckpointInNonEpochBlock,
    InvalidCheckpoint,
    CheckpointMismatch {
        expected: Vec<Address>,
        got: Vec<Address>,
    },
}

impl Display for CliqueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParliaError {
    WrongHeaderTime {
        now: u64,
        got: u64,
    },
    WrongHeaderExtraLen {
        expected: usize,
        got: usize,
    },
    WrongHeaderExtraSignersLen {
        expected: usize,
        got: usize,
    },
    WrongHeaderSigner {
        number: BlockNumber,
        expected: Address,
        got: Address,
    },
    UnknownHeader{
        number: BlockNumber,
        hash: H256,
    },
    SignerUnauthorized{
        number: BlockNumber,
        signer: Address,
    },
    SignerOverLimit{
        signer: Address,
    },
    EpochChgWrongValidators {
        expect: Vec<Address>,
        got: Vec<Address>,
    },
    EpochChgCallErr,
    SnapFutureBlock {
        expect: BlockNumber,
        got: BlockNumber,
    },
    SnapNotFound {
        number: BlockNumber,
        hash: H256,
    },
    SystemTxWrongSystemReward {
        expect: U256,
        got: U256,
    },
    SystemTxWrongCount {
        expect: usize,
        got: usize,
    },
    SystemTxWrong {
        expect: Message,
        got: Message,
    },
    CacheValidatorsUnknown,
    WrongConsensusParam,
    UnknownAccount {
        block: BlockNumber,
        account: Address,
    },
}
impl From<ParliaError> for anyhow::Error {
    fn from(err: ParliaError) -> Self {
        DuoError::Validation(ValidationError::ParliaError(err)).into()
    }
}

impl Display for ParliaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationError {
    FutureBlock {
        now: u64,
        got: u64,
    }, // Block has a timestamp in the future

    // See [YP] Section 4.3.2 "Holistic Validity", Eq (31)
    WrongStateRoot {
        expected: H256,
        got: H256,
    }, // wrong Hr
    WrongOmmersHash {
        expected: H256,
        got: H256,
    }, // wrong Ho
    WrongHeaderNonce {
        expected: H64,
        got: H64,
    },
    WrongTransactionsRoot {
        expected: H256,
        got: H256,
    }, // wrong Ht
    WrongReceiptsRoot {
        expected: H256,
        got: H256,
    }, // wrong He
    WrongLogsBloom {
        expected: Bloom,
        got: Bloom,
    }, // wrong Hb

    // See [YP] Section 4.3.4 "Block Header Validity", Eq (50)
    UnknownParent {
        number: BlockNumber,
        parent_hash: H256,
    }, // P(H) = ∅ ∨ Hi ≠ P(H)Hi + 1
    WrongDifficulty, // Hd ≠ D(H)
    GasAboveLimit {
        used: u64,
        limit: u64,
    }, // Hg > Hl
    InvalidGasLimit, // |Hl-P(H)Hl|≥P(H)Hl/1024 ∨ Hl<5000
    InvalidTimestamp {
        parent: u64,
        current: u64,
    }, // Hs ≤ P(H)Hs
    ExtraDataTooLong, // ‖Hx‖ > 32
    WrongDaoExtraData, // see EIP-779
    WrongBaseFee {
        expected: Option<U256>,
        got: Option<U256>,
    }, // see EIP-1559
    MissingBaseFee,  // see EIP-1559
    InvalidSeal,     // Nonce or mix_hash

    // See [YP] Section 6.2 "Execution", Eq (58)
    MissingSender, // S(T) = ∅
    BadTransaction {
        index: usize,
        error: BadTransactionError,
    },
    IntrinsicGas,                 // g0 > Tg
    MaxFeeLessThanBase,           // max_fee_per_gas < base_fee_per_gas (EIP-1559)
    MaxPriorityFeeGreaterThanMax, // max_priority_fee_per_gas > max_fee_per_gas (EIP-1559)

    // See [YP] Section 11.1 "Ommer Validation", Eq (157)
    OmmerUnknownParent {
        number: BlockNumber,
        parent_hash: H256,
    }, // P(H) = ∅ ∨ Hi ≠ P(H)Hi + 1
    TooManyOmmers, // ‖BU‖ > 2
    InvalidOmmerHeader {
        inner: Box<ValidationError>,
    }, // ¬V(U)
    NotAnOmmer,    // ¬k(U, P(BH)H, 6)
    DuplicateOmmer, // not well covered by the YP actually

    // See [YP] Section 11.2 "Transaction Validation", Eq (160)
    WrongBlockGas {
        expected: u64,
        got: u64,
        transactions: Vec<(usize, u64)>,
    }, // BHg ≠ l(BR)u

    InvalidSignature, // EIP-2

    WrongChainId, // EIP-155

    UnsupportedTransactionType, // EIP-2718

    CliqueError(CliqueError),
    ParliaError(ParliaError),
}

impl From<CliqueError> for ValidationError {
    fn from(e: CliqueError) -> Self {
        Self::CliqueError(e)
    }
}

impl From<ParliaError> for ValidationError {
    fn from(e: ParliaError) -> Self {
        Self::ParliaError(e)
    }
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Display, From)]
pub enum DuoError {
    Validation(ValidationError),
    Internal(anyhow::Error),
}

impl std::error::Error for DuoError {}

impl From<CliqueError> for DuoError {
    fn from(clique_error: CliqueError) -> Self {
        DuoError::Validation(ValidationError::CliqueError(clique_error))
    }
}

impl From<ParliaError> for DuoError {
    fn from(err: ParliaError) -> Self {
        DuoError::Validation(ValidationError::ParliaError(err))
    }
}

impl From<ethabi::Error> for DuoError {
    fn from(err: ethabi::Error) -> Self {
        DuoError::Internal(anyhow::Error::from(err))
    }
}

impl From<secp256k1::Error> for DuoError {
    fn from(err: secp256k1::Error) -> Self {
        DuoError::Internal(anyhow::Error::from(err))
    }
}

impl From<SystemTimeError> for DuoError {
    fn from(err: SystemTimeError) -> Self {
        DuoError::Internal(anyhow::Error::from(err))
    }
}

pub fn pre_validate_transaction(
    txn: &Message,
    canonical_chain_id: ChainId,
    base_fee_per_gas: Option<U256>,
) -> Result<(), ValidationError> {
    if let Some(chain_id) = txn.chain_id() {
        if chain_id != canonical_chain_id {
            return Err(ValidationError::WrongChainId);
        }
    }

    if let Some(base_fee_per_gas) = base_fee_per_gas {
        if txn.max_fee_per_gas() < base_fee_per_gas {
            return Err(ValidationError::MaxFeeLessThanBase);
        }
    }

    // https://github.com/ethereum/EIPs/pull/3594
    if txn.max_priority_fee_per_gas() > txn.max_fee_per_gas() {
        return Err(ValidationError::MaxPriorityFeeGreaterThanMax);
    }

    Ok(())
}

pub fn engine_factory(
    db: Option<Arc<MdbxWithDirHandle<WriteMap>>>,
    chain_config: ChainSpec,
    listen_addr: Option<SocketAddr>,
) -> anyhow::Result<Box<dyn Consensus>> {
    Ok(match chain_config.consensus.seal_verification {
        SealVerificationParams::Parlia {
            period,
            epoch,
        } => Box::new(Parlia::new(
            chain_config.params.chain_id,
            chain_config,
            epoch,
            period,
        )),
        SealVerificationParams::Clique { period, epoch } => {
            let initial_signers = match chain_config.genesis.seal {
                Seal::Clique {
                    vanity: _,
                    score: _,
                    signers,
                } => signers,
                _ => bail!("Genesis seal does not match, expected Clique seal."),
            };
            Box::new(Clique::new(
                chain_config.params.chain_id,
                chain_config.consensus.eip1559_block,
                period,
                epoch,
                initial_signers,
            ))
        }

        SealVerificationParams::Beacon {
            terminal_total_difficulty,
            terminal_block_hash,
            terminal_block_number,
            since,
            block_reward,
            beneficiary,
        } => Box::new(BeaconConsensus::new(
            db,
            listen_addr.unwrap_or_else(|| {
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8551))
            }),
            chain_config.params.chain_id,
            chain_config.params.network_id,
            chain_config.consensus.eip1559_block,
            block_reward.into(),
            beneficiary.into(),
            terminal_total_difficulty,
            terminal_block_hash,
            terminal_block_number,
            since,
        )),
    })
}
