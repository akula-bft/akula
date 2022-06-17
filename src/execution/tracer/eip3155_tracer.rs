use super::*;
use crate::{
    execution::evm::{ExecutionState, OpCode, Output, Stack, StatusCode},
    models::*,
};
use bytes::Bytes;
use serde::Serialize;

#[derive(Serialize)]
struct ExecutionStart {
    pub depth: u16,
    pub rev: Revision,
    #[serde(rename = "static")]
    pub is_static: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstructionStart {
    pub pc: usize,
    pub op: u8,
    pub op_name: &'static str,
    pub gas: u64,
    pub stack: Stack,
    pub memory_size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutionEnd {
    pub error: Option<String>,
    pub gas: u64,
    pub gas_used: u64,
    pub output: String,
}

#[derive(Debug)]
struct TracerContext {
    message_gas: u64,
}

/// Tracer which prints to stdout.
#[derive(Debug, Default)]
pub struct StdoutTracer {
    execution_stack: Vec<TracerContext>,
}

impl Tracer for StdoutTracer {
    fn capture_start(
        &mut self,
        _: u16,
        _: Address,
        _: Address,
        _: MessageKind,
        _: Bytes,
        gas: u64,
        _: U256,
    ) {
        self.execution_stack
            .push(TracerContext { message_gas: gas });
    }

    fn capture_state(&mut self, env: &ExecutionState, pc: usize, op: OpCode, _: u64, _: u16) {
        println!(
            "{}",
            serde_json::to_string(&InstructionStart {
                pc,
                op: op.0,
                op_name: op.name(),
                gas: env.gas_left as u64,
                stack: env.stack.clone(),
                memory_size: env.memory.len()
            })
            .unwrap()
        )
    }

    fn capture_end(&mut self, _: usize, _: u64, output: &Output) {
        let context = self.execution_stack.pop().unwrap();
        let error = match output.status_code {
            StatusCode::Success => None,
            other => Some(other.to_string()),
        };
        let (gas_left, gas_used) = match output.status_code {
            StatusCode::Success | StatusCode::Revert => (
                output.gas_left as u64,
                context.message_gas - output.gas_left as u64,
            ),
            _ => (0, context.message_gas),
        };

        println!(
            "{}",
            serde_json::to_string(&ExecutionEnd {
                error,
                gas: gas_left,
                gas_used,
                output: hex::encode(&output.output_data),
            })
            .unwrap()
        )
    }
}
