use self::{interrupt::*, interrupt_data::*, resume_data::*};
use super::{
    common::*,
    host::{AccessStatus, StorageStatus, TxContext},
    state::ExecutionState,
    *,
};
use arrayvec::ArrayVec;
use derive_more::From;
use enum_as_inner::EnumAsInner;
use ethereum_types::Address;
use ethnum::U256;
use std::{
    convert::Infallible,
    ops::{Generator, GeneratorState},
    pin::Pin,
};

/// Interrupts.
pub mod interrupt;
/// Data attached to interrupts.
pub mod interrupt_data;
/// Data required for resume.
pub mod resume_data;

pub(crate) type InnerCoroutine = Pin<
    Box<
        dyn Generator<
                ResumeData,
                Yield = InterruptData,
                Return = Result<SuccessfulOutput, StatusCode>,
            > + Send
            + Sync,
    >,
>;

fn resume_interrupt(mut inner: InnerCoroutine, resume_data: ResumeData) -> Interrupt {
    match inner.as_mut().resume(resume_data) {
        GeneratorState::Yielded(interrupt) => match interrupt {
            InterruptData::InstructionStart { pc, opcode, state } => Interrupt::InstructionStart {
                interrupt: InstructionStartInterrupt { inner },
                pc,
                opcode,
                state,
            },
            InterruptData::AccountExists { address } => Interrupt::AccountExists {
                interrupt: AccountExistsInterrupt { inner },
                address,
            },
            InterruptData::GetStorage { address, location } => Interrupt::GetStorage {
                interrupt: GetStorageInterrupt { inner },
                address,
                location,
            },
            InterruptData::SetStorage {
                address,
                location,
                value,
            } => Interrupt::SetStorage {
                interrupt: SetStorageInterrupt { inner },
                address,
                location,
                value,
            },
            InterruptData::GetBalance { address } => Interrupt::GetBalance {
                interrupt: GetBalanceInterrupt { inner },
                address,
            },
            InterruptData::GetCodeSize { address } => Interrupt::GetCodeSize {
                interrupt: GetCodeSizeInterrupt { inner },
                address,
            },
            InterruptData::GetCodeHash { address } => Interrupt::GetCodeHash {
                interrupt: GetCodeHashInterrupt { inner },
                address,
            },
            InterruptData::CopyCode {
                address,
                offset,
                max_size,
            } => Interrupt::CopyCode {
                interrupt: CopyCodeInterrupt { inner },
                address,
                offset,
                max_size,
            },
            InterruptData::Selfdestruct {
                address,
                beneficiary,
            } => Interrupt::Selfdestruct {
                interrupt: SelfdestructInterrupt { inner },
                address,
                beneficiary,
            },
            InterruptData::Call(call_data) => Interrupt::Call {
                interrupt: CallInterrupt { inner },
                call_data,
            },
            InterruptData::GetTxContext => Interrupt::GetTxContext {
                interrupt: GetTxContextInterrupt { inner },
            },
            InterruptData::GetBlockHash { block_number } => Interrupt::GetBlockHash {
                interrupt: GetBlockHashInterrupt { inner },
                block_number,
            },
            InterruptData::EmitLog {
                address,
                data,
                topics,
            } => Interrupt::EmitLog {
                interrupt: EmitLogInterrupt { inner },
                address,
                data,
                topics,
            },
            InterruptData::AccessAccount { address } => Interrupt::AccessAccount {
                interrupt: AccessAccountInterrupt { inner },
                address,
            },
            InterruptData::AccessStorage { address, location } => Interrupt::AccessStorage {
                interrupt: AccessStorageInterrupt { inner },
                address,
                location,
            },
        },
        GeneratorState::Complete(result) => Interrupt::Complete {
            interrupt: ExecutionComplete(inner),
            result,
        },
    }
}
