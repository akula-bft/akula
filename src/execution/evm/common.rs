use arrayref::array_ref;
use bytes::Bytes;
use ethereum_types::{Address, H256};
use ethnum::U256;
use strum_macros::Display;

/// Message status code.
#[must_use]
#[derive(Clone, Debug, Display, PartialEq)]
pub enum StatusCode {
    /// Execution finished with success.
    #[strum(serialize = "success")]
    Success,

    /// Generic execution failure.
    #[strum(serialize = "failure")]
    Failure,

    /// Execution terminated with REVERT opcode.
    ///
    /// In this case the amount of gas left MAY be non-zero and additional output
    /// data MAY be provided in ::evmc_result.
    #[strum(serialize = "revert")]
    Revert,

    /// The execution has run out of gas.
    #[strum(serialize = "out of gas")]
    OutOfGas,

    /// The designated INVALID instruction has been hit during execution.
    ///
    /// [EIP-141](https://github.com/ethereum/EIPs/blob/master/EIPS/eip-141.md)
    /// defines the instruction 0xfe as INVALID instruction to indicate execution
    /// abortion coming from high-level languages. This status code is reported
    /// in case this INVALID instruction has been encountered.
    #[strum(serialize = "invalid instruction")]
    InvalidInstruction,

    /// An undefined instruction has been encountered.
    #[strum(serialize = "undefined instruction")]
    UndefinedInstruction,

    /// The execution has attempted to put more items on the EVM stack
    /// than the specified limit.
    #[strum(serialize = "stack overflow")]
    StackOverflow,

    /// Execution of an opcode has required more items on the EVM stack.
    #[strum(serialize = "stack underflow")]
    StackUnderflow,

    /// Execution has violated the jump destination restrictions.
    #[strum(serialize = "bad jump destination")]
    BadJumpDestination,

    /// Tried to read outside memory bounds.
    ///
    /// An example is RETURNDATACOPY reading past the available buffer.
    #[strum(serialize = "invalid memory access")]
    InvalidMemoryAccess,

    /// Call depth has exceeded the limit (if any)
    #[strum(serialize = "call depth exceeded")]
    CallDepthExceeded,

    /// Tried to execute an operation which is restricted in static mode.
    #[strum(serialize = "static mode violation")]
    StaticModeViolation,

    /// A call to a precompiled or system contract has ended with a failure.
    ///
    /// An example: elliptic curve functions handed invalid EC points.
    #[strum(serialize = "precompile failure")]
    PrecompileFailure,

    /// Contract validation has failed.
    #[strum(serialize = "contract validation failure")]
    ContractValidationFailure,

    /// An argument to a state accessing method has a value outside of the
    /// accepted range of values.
    #[strum(serialize = "argument out of range")]
    ArgumentOutOfRange,

    /// The caller does not have enough funds for value transfer.
    #[strum(serialize = "insufficient balance")]
    InsufficientBalance,

    /// EVM implementation generic internal error.
    #[strum(serialize = "internal error")]
    InternalError(String),
}

/// The kind of call-like instruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2 { salt: U256 },
}

/// The message describing an EVM call,
/// including a zero-depth call from transaction origin.
#[derive(Clone, Debug, PartialEq)]
pub struct InterpreterMessage {
    /// The kind of the call. For zero-depth calls `CallKind::Call` SHOULD be used.
    pub kind: CallKind,

    /// Static call mode.
    pub is_static: bool,

    /// The call depth.
    pub depth: i32,

    /// The amount of gas for message execution.
    pub gas: i64,

    /// The destination (recipient) of the message.
    pub recipient: Address,

    /// The sender of the message.
    pub sender: Address,

    /// Message input data.
    pub input_data: Bytes,

    /// The amount of Ether transferred with the message.
    pub value: U256,

    /// The address of the code to be executed.
    ///
    /// May be different from the evmc_message::destination (recipient)
    /// in case of `CallKind::CallCode` or `CallKind::DelegateCall`.
    pub code_address: Address,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateMessage {
    pub salt: Option<U256>,
    pub gas: i64,
    pub depth: i32,
    pub initcode: Bytes,
    pub sender: Address,
    pub endowment: U256,
}

impl From<CreateMessage> for InterpreterMessage {
    fn from(msg: CreateMessage) -> Self {
        Self {
            kind: if let Some(salt) = msg.salt {
                CallKind::Create2 { salt }
            } else {
                CallKind::Create
            },
            is_static: false,
            depth: msg.depth,
            gas: msg.gas,
            recipient: Address::zero(),
            code_address: Address::zero(),
            sender: msg.sender,
            input_data: msg.initcode,
            value: msg.endowment,
        }
    }
}

/// Output of EVM execution.
#[derive(Clone, Debug, PartialEq)]
pub struct Output {
    /// EVM exited with this status code.
    pub status_code: StatusCode,
    /// How much gas was left after execution
    pub gas_left: i64,
    /// Output data returned.
    pub output_data: Bytes,
    /// Contract creation address.
    pub create_address: Option<Address>,
}

/// EVM execution output if no error has occurred.
#[derive(Clone, Debug, PartialEq)]
pub struct SuccessfulOutput {
    /// Indicates if revert was requested.
    pub reverted: bool,
    /// How much gas was left after execution.
    pub gas_left: i64,
    /// Output data returned.
    pub output_data: Bytes,
}

impl From<SuccessfulOutput> for Output {
    fn from(
        SuccessfulOutput {
            reverted,
            gas_left,
            output_data,
        }: SuccessfulOutput,
    ) -> Self {
        Self {
            status_code: if reverted {
                StatusCode::Revert
            } else {
                StatusCode::Success
            },
            gas_left,
            output_data,
            create_address: None,
        }
    }
}

#[inline(always)]
pub(crate) fn u256_to_address(v: U256) -> Address {
    H256(v.to_be_bytes()).into()
}

#[inline(always)]
pub(crate) fn address_to_u256(v: Address) -> U256 {
    U256::from_be_bytes(H256::from(v).0)
}

#[inline(always)]
pub(crate) fn u256_from_slice(v: &[u8]) -> U256 {
    debug_assert!(v.len() <= 32, "invalid len");
    match v.len() {
        0 => U256::ZERO,
        32 => U256::from_be_bytes(*array_ref!(v, 0, 32)),
        _ => {
            let mut padded = [0; 32];
            padded[32 - v.len()..].copy_from_slice(v);
            U256::from_be_bytes(padded)
        }
    }
}
