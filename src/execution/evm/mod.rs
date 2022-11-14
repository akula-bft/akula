use bytes::Bytes;
pub use common::{
    CallKind, CreateMessage, InterpreterMessage, Output, StatusCode, SuccessfulOutput,
};
pub use host::Host;
pub use interpreter::AnalyzedCode;
pub use opcode::OpCode;
pub use state::{EvmMemory, EvmStack, EvmSubMemory, ExecutionState, PageSize};

/// Maximum allowed EVM bytecode size.
pub const MAX_CODE_SIZE: usize = 0x6000;

mod common;
pub mod host;
#[macro_use]
pub mod instructions;
mod interpreter;
pub mod opcode;
mod state;
pub mod util;

#[cfg(test)]
mod tests;
