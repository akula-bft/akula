use bytes::Bytes;
pub use common::{
    CallKind, CreateMessage, InterpreterMessage, Output, StatusCode, SuccessfulOutput,
};
pub use host::Host;
pub use interpreter::AnalyzedCode;
pub use opcode::OpCode;
pub use state::{ExecutionState, Stack};

/// Maximum allowed EVM bytecode size.
pub const MAX_CODE_SIZE: usize = 0x6000;

mod common;
pub mod continuation;
pub mod host;
#[macro_use]
pub mod instructions;
mod interpreter;
pub mod opcode;
mod state;
pub mod tracing;
pub mod util;

#[cfg(test)]
mod tests;
