use super::*;
use crate::models::*;
use serde::Serialize;

/// Passed into execution context to collect metrics.
pub trait Tracer {
    #[doc(hidden)]
    const DUMMY: bool = false;

    /// Called when execution starts.
    fn notify_execution_start(
        &mut self,
        revision: Revision,
        message: InterpreterMessage,
        code: Bytes,
    );
    /// Called on each instruction.
    fn notify_instruction_start(&mut self, pc: usize, opcode: OpCode, state: &ExecutionState);
    /// Called when execution ends.
    fn notify_execution_end(&mut self, output: &Output);
}

/// Tracer which does nothing.
pub struct NoopTracer;

impl Tracer for NoopTracer {
    const DUMMY: bool = true;

    fn notify_execution_start(&mut self, _: Revision, _: InterpreterMessage, _: Bytes) {}

    fn notify_instruction_start(&mut self, _: usize, _: OpCode, _: &ExecutionState) {}

    fn notify_execution_end(&mut self, _: &Output) {}
}

#[derive(Serialize)]
struct ExecutionStart {
    pub depth: i32,
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
    pub gas: i64,
    pub stack: Stack,
    pub memory_size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutionEnd {
    pub error: Option<String>,
    pub gas: i64,
    pub gas_used: i64,
    pub output: String,
}

struct TracerContext {
    message: InterpreterMessage,
    code: Bytes,
}

/// Tracer which prints to stdout.
#[derive(Default)]
pub struct StdoutTracer {
    execution_stack: Vec<TracerContext>,
}

impl Tracer for StdoutTracer {
    fn notify_execution_start(
        &mut self,
        revision: Revision,
        message: InterpreterMessage,
        code: Bytes,
    ) {
        println!(
            "{}",
            serde_json::to_string(&ExecutionStart {
                depth: message.depth,
                rev: revision,
                is_static: message.is_static,
            })
            .unwrap()
        );
        self.execution_stack.push(TracerContext { message, code });
    }

    fn notify_instruction_start(&mut self, pc: usize, _: OpCode, state: &ExecutionState) {
        let context = self.execution_stack.last().unwrap();
        let opcode = OpCode(context.code[pc]);
        println!(
            "{}",
            serde_json::to_string(&InstructionStart {
                pc,
                op: opcode.0,
                op_name: opcode.name(),
                gas: state.gas_left,
                stack: state.stack.clone(),
                memory_size: state.memory.len()
            })
            .unwrap()
        )
    }

    fn notify_execution_end(&mut self, output: &Output) {
        let context = self.execution_stack.pop().unwrap();
        let error = match &output.status_code {
            StatusCode::Success => None,
            other => Some(other.to_string()),
        };
        let (gas_left, gas_used) = if error.is_none() {
            (output.gas_left, context.message.gas - output.gas_left)
        } else {
            (0, context.message.gas)
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
