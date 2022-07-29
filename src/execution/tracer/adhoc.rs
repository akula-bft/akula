use super::{CodeKind, Tracer};
use crate::{execution::evm::StatusCode, models::*};
use ethereum_jsonrpc::types;

#[derive(Debug, Default)]
pub struct AdhocTracer {
    trace: Vec<types::TransactionTrace>,

    trace_addr: Vec<usize>,
    trace_stack: Vec<usize>,
    /// Whether the last capture_start was called with `precompile = true`
    precompile: bool,
}

impl AdhocTracer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_trace(self) -> Vec<types::TransactionTrace> {
        self.trace
    }
}

impl Tracer for AdhocTracer {
    fn trace_instructions(&self) -> bool {
        true
    }

    fn capture_start(
        &mut self,
        depth: u16,
        sender: ethereum_types::Address,
        recipient: ethereum_types::Address,
        real_sender: ethereum_types::Address,
        code_address: ethereum_types::Address,
        call_type: super::MessageKind,
        input: bytes::Bytes,
        mut gas: u64,
        mut value: ethnum::U256,
    ) {
        let mut precompile = false;
        if let super::MessageKind::Call {
            code_kind: CodeKind::Precompile,
            ..
        } = &call_type
        {
            precompile = true;
        }
        if precompile && depth > 0 && value == 0 {
            self.precompile = true;
            return;
        }
        if gas > 500000000 {
            gas = 500000001 - (0x8000000000000000 - gas)
        }
        let output = if let super::MessageKind::Create { .. } = &call_type {
            types::TraceOutput::Create(types::CreateOutput {
                gas_used: 0.into(),
                code: types::Bytes::default(),
                address: recipient,
            })
        } else {
            types::TraceOutput::Call(types::CallOutput {
                gas_used: 0.into(),
                output: types::Bytes::default(),
            })
        };
        if depth > 0 {
            let top_trace = &mut self.trace[*self.trace_stack.last().unwrap()];
            self.trace_addr.push(top_trace.subtraces);
            top_trace.subtraces += 1;

            if let super::MessageKind::Call { call_kind, .. } = &call_type {
                match call_kind {
                    super::CallKind::DelegateCall => match top_trace.action {
                        types::Action::Call(types::CallAction { value: v, .. }) => {
                            value = v;
                        }
                        types::Action::Create(types::CreateAction { value: v, .. }) => {
                            value = v;
                        }
                        _ => {}
                    },
                    super::CallKind::StaticCall => {
                        value = U256::ZERO;
                    }
                    _ => {}
                }
            };
        }

        let trace_address = self.trace_addr.clone();
        let action = match call_type {
            super::MessageKind::Create { .. } => types::Action::Create(types::CreateAction {
                from: sender,
                value,
                gas: gas.into(),
                init: input.into(),
            }),
            super::MessageKind::Call { call_kind, .. } => types::Action::Call(types::CallAction {
                from: real_sender,
                to: code_address,
                gas: gas.into(),
                input: input.into(),
                value,
                call_type: match call_kind {
                    super::CallKind::Call => types::CallType::Call,
                    super::CallKind::CallCode => types::CallType::CallCode,
                    super::CallKind::DelegateCall => types::CallType::DelegateCall,
                    super::CallKind::StaticCall => types::CallType::StaticCall,
                },
            }),
        };

        self.trace.push(types::TransactionTrace {
            trace_address,
            subtraces: 0,
            action,
            result: Some(types::TraceResult::Success { result: output }),
        });
        self.trace_stack.push(self.trace.len() - 1);
    }

    fn capture_end(
        &mut self,
        depth: usize,
        start_gas: u64,
        exec_output: &crate::execution::evm::Output,
    ) {
        if self.precompile {
            self.precompile = false;
            return;
        }
        let top_trace = &mut self.trace[*self.trace_stack.last().unwrap()];
        match exec_output.status_code {
            StatusCode::Success => match (&top_trace.action, &mut top_trace.result) {
                (
                    types::Action::Call(_),
                    Some(types::TraceResult::Success {
                        result: types::TraceOutput::Call(output),
                        ..
                    }),
                ) => {
                    if !exec_output.output_data.is_empty() {
                        output.output = exec_output.output_data.clone().into();
                    }
                    output.gas_used =
                        (start_gas - u64::try_from(exec_output.gas_left).unwrap()).into();
                }
                (
                    types::Action::Create(_),
                    Some(types::TraceResult::Success {
                        result: types::TraceOutput::Create(output),
                        ..
                    }),
                ) => {
                    if !exec_output.output_data.is_empty() {
                        output.code = exec_output.output_data.clone().into();
                    }
                    output.gas_used =
                        (start_gas - u64::try_from(exec_output.gas_left).unwrap()).into();
                }
                _ => {}
            },
            StatusCode::Failure => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Failure".to_string(),
                })
            }
            StatusCode::Revert => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Reverted".to_string(),
                })
            }
            StatusCode::OutOfGas => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Out of gas".to_string(),
                })
            }
            StatusCode::InvalidInstruction => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Bad instruction".to_string(),
                })
            }
            StatusCode::UndefinedInstruction => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Bad instruction".to_string(),
                })
            }
            StatusCode::StackOverflow => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Stack overflow".to_string(),
                })
            }
            StatusCode::StackUnderflow => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Stack underflow".to_string(),
                })
            }
            StatusCode::BadJumpDestination => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Bad jump destination".to_string(),
                })
            }
            StatusCode::InvalidMemoryAccess => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Invalid memory access".to_string(),
                })
            }
            StatusCode::CallDepthExceeded => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Call depth exceeded".to_string(),
                })
            }
            StatusCode::StaticModeViolation => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Mutable Call In Static Context".to_string(),
                })
            }
            StatusCode::PrecompileFailure => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Precompile failure".to_string(),
                })
            }
            StatusCode::ContractValidationFailure => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Contract validation failure".to_string(),
                })
            }
            StatusCode::ArgumentOutOfRange => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Argument out of range".to_string(),
                })
            }
            StatusCode::InsufficientBalance => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: "Insufficient balance".to_string(),
                })
            }
            StatusCode::InternalError(e) => {
                top_trace.result = Some(types::TraceResult::Error {
                    error: format!("Internal error: {e}"),
                })
            }
        }
        self.trace_stack.pop();
        if depth > 0 {
            self.trace_addr.pop();
        }
    }

    fn capture_self_destruct(
        &mut self,
        caller: ethereum_types::Address,
        beneficiary: ethereum_types::Address,
        balance: ethnum::U256,
    ) {
        let top_trace = &mut self.trace[*self.trace_stack.last().unwrap()];
        let trace_idx = top_trace.subtraces;
        top_trace.subtraces += 1;

        self.trace.push(types::TransactionTrace {
            trace_address: {
                let mut v = self.trace_addr.clone();
                v.push(trace_idx);
                v
            },
            subtraces: 0,
            action: types::Action::Selfdestruct(types::SelfdestructAction {
                address: caller,
                refund_address: beneficiary,
                balance,
            }),
            result: Some(types::TraceResult::Error {
                error: "".to_string(),
            }),
        });
    }
}
