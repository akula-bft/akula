use self::instruction_table::*;
use super::{
    common::{InterpreterMessage, *},
    instructions::{control::*, stack_manip::*, *},
    state::*,
    *,
};
use crate::models::*;
use ethnum::U256;
use std::sync::Arc;

#[inline]
fn check_requirements(
    metrics: &InstructionTableEntry,
    state: &mut ExecutionState,
) -> Result<(), StatusCode> {
    state.gas_left -= metrics.gas_cost as i64;
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }

    let stack_size = state.mem.stack().len();
    if stack_size == STACK_SIZE {
        if metrics.can_overflow_stack {
            return Err(StatusCode::StackOverflow);
        }
    } else if stack_size < metrics.stack_height_required.into() {
        return Err(StatusCode::StackUnderflow);
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub struct JumpdestMap(Arc<[bool]>);

impl JumpdestMap {
    #[inline]
    pub fn contains(&self, dst: U256) -> bool {
        dst < self.0.len() as u128 && self.0[dst.as_usize()]
    }
}

/// Code with analysis.
#[derive(Clone, Debug)]
pub struct AnalyzedCode {
    jumpdest_map: JumpdestMap,
    code: Bytes,
    padded_code: Bytes,
}

impl AnalyzedCode {
    /// Analyze code and prepare it for execution.
    pub fn analyze(code: &[u8]) -> Self {
        const JUMPDEST: u8 = OpCode::JUMPDEST.0;
        const PUSH1: u8 = OpCode::PUSH1.0;
        const PUSH32: u8 = OpCode::PUSH32.0;

        let code_len = code.len();
        let mut jumpdest_map = vec![false; code_len];

        let mut i = 0;
        while i < code_len {
            let opcode = code[i];
            i += match opcode {
                JUMPDEST => {
                    jumpdest_map[i] = true;
                    1
                }
                PUSH1..=PUSH32 => {
                    (opcode - PUSH1 + 2) as usize
                }
                _ => 1,
            }
        }

        let mut padded_code = code.to_vec();
        padded_code.resize(i + 1, OpCode::STOP.to_u8());

        let jumpdest_map = JumpdestMap(jumpdest_map.into());
        let padded_code = Bytes::from(padded_code);
        let mut code = padded_code.clone();
        code.truncate(code_len);

        Self {
            jumpdest_map,
            code,
            padded_code,
        }
    }

    /// Execute analyzed EVM bytecode using provided `Host` context.
    pub fn execute<H>(
        &self,
        host: &mut H,
        message: &InterpreterMessage,
        mem: EvmSubMemory,
        revision: Revision,
    ) -> Output
    where
        H: Host,
    {
        let mut state = ExecutionState::new(message, mem);

        macro_rules! execute_revisions {
            (
                $trace:expr, $revision:expr, $self:expr, $state:expr, $host:expr,
                $($rev:ident, )*
            ) => {
                match ($trace, $revision) {
                    $(
                        (true, Revision::$rev) => {
                            execute_message::<H, true, { Revision::$rev }>(self, &mut state, host)
                        }
                        (false, Revision::$rev) => {
                            execute_message::<H, false, { Revision::$rev }>(self, &mut state, host)
                        }
                    )*
                }
            };
        }

        let res = execute_revisions!(
            host.trace_instructions(), revision, self, &mut state, host,
            Frontier, Homestead, Tangerine, Spurious, Byzantium,
            Constantinople, Petersburg, Istanbul, Berlin, London,
            Paris,  
        );

        match res {
            Ok(output) => output.into(),
            Err(status_code) => Output {
                status_code,
                gas_left: 0,
                output_data: Bytes::new(),
                create_address: None,
            },
        }
    }
}

#[allow(clippy::needless_borrow)]
fn execute_message<H, const TRACE: bool, const REVISION: Revision>(
    s: &AnalyzedCode,
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<SuccessfulOutput, StatusCode>
where
    H: Host,
{
    let instruction_table = get_instruction_table(REVISION);

    let mut reverted = false;

    let mut pc = 0;

    loop {
        let op = OpCode(s.padded_code[pc]);

        let metrics = &instruction_table[op.to_usize()];

        if metrics.gas_cost < 0 {
            return Err(StatusCode::UndefinedInstruction);
        }

        if TRACE {
            // Do not print stop on the final STOP
            if pc < s.code.len() {
                host.tracer(|t| {
                    t.capture_state(
                        &state,
                        pc,
                        op,
                        metrics.gas_cost as u64,
                        state.message.depth as u16,
                    )
                });
            }
        }

        check_requirements(metrics, state)?;

        let stack = &mut state.mem.stack();
        match op {
            OpCode::STOP => {
                break;
            }
            OpCode::ADD => {
                arithmetic::add(stack);
            }
            OpCode::MUL => {
                arithmetic::mul(stack);
            }
            OpCode::SUB => {
                arithmetic::sub(stack);
            }
            OpCode::DIV => {
                arithmetic::div(stack);
            }
            OpCode::SDIV => {
                arithmetic::sdiv(stack);
            }
            OpCode::MOD => {
                arithmetic::modulo(stack);
            }
            OpCode::SMOD => {
                arithmetic::smod(stack);
            }
            OpCode::ADDMOD => {
                arithmetic::addmod(stack);
            }
            OpCode::MULMOD => {
                arithmetic::mulmod(stack);
            }
            OpCode::EXP => {
                arithmetic::exp::<REVISION>(state)?;
            }
            OpCode::SIGNEXTEND => {
                arithmetic::signextend(stack);
            }
            OpCode::LT => {
                boolean::lt(stack);
            }
            OpCode::GT => {
                boolean::gt(stack);
            }
            OpCode::SLT => {
                boolean::slt(stack);
            }
            OpCode::SGT => {
                boolean::sgt(stack);
            }
            OpCode::EQ => {
                boolean::eq(stack);
            }
            OpCode::ISZERO => {
                boolean::iszero(stack);
            }
            OpCode::AND => {
                boolean::and(stack);
            }
            OpCode::OR => {
                boolean::or(stack);
            }
            OpCode::XOR => {
                boolean::xor(stack);
            }
            OpCode::NOT => {
                boolean::not(stack);
            }
            OpCode::BYTE => {
                bitwise::byte(stack);
            }
            OpCode::SHL => {
                bitwise::shl(stack);
            }
            OpCode::SHR => {
                bitwise::shr(stack);
            }
            OpCode::SAR => {
                bitwise::sar(stack);
            }

            OpCode::KECCAK256 => {
                memory::keccak256(state)?;
            }
            OpCode::ADDRESS => {
                external::address(state);
            }
            OpCode::BALANCE => {
                external::balance::<_, REVISION>(state, host)?;
            }
            OpCode::CALLER => {
                external::caller(state);
            }
            OpCode::CALLVALUE => {
                external::callvalue(state);
            }
            OpCode::CALLDATALOAD => {
                calldataload(state);
            }
            OpCode::CALLDATASIZE => {
                calldatasize(state);
            }
            OpCode::CALLDATACOPY => {
                memory::calldatacopy(state)?;
            }
            OpCode::CODESIZE => {
                memory::codesize(stack, &s.code[..]);
            }
            OpCode::CODECOPY => {
                memory::codecopy(state, &s.code[..])?;
            }
            OpCode::EXTCODESIZE => {
                external::extcodesize::<_, REVISION>(state, host)?;
            }
            OpCode::EXTCODECOPY => {
                memory::extcodecopy::<_, REVISION>(state, host)?;
            }
            OpCode::RETURNDATASIZE => {
                memory::returndatasize(state);
            }
            OpCode::RETURNDATACOPY => {
                memory::returndatacopy(state)?;
            }
            OpCode::EXTCODEHASH => {
                memory::extcodehash::<_, REVISION>(state, host)?;
            }
            OpCode::BLOCKHASH => {
                external::blockhash(state, host)?;
            }
            OpCode::ORIGIN => stack
                .push(address_to_u256(host.get_tx_context()?.tx_origin)),
            OpCode::COINBASE => stack
                .push(address_to_u256(host.get_tx_context()?.block_coinbase)),
            OpCode::GASPRICE => stack.push(host.get_tx_context()?.tx_gas_price),
            OpCode::TIMESTAMP => stack
                .push(host.get_tx_context()?.block_timestamp.into()),
            OpCode::NUMBER => stack.push(host.get_tx_context()?.block_number.into()),
            OpCode::DIFFICULTY => stack.push(host.get_tx_context()?.block_difficulty),
            OpCode::GASLIMIT => stack
                .push(host.get_tx_context()?.block_gas_limit.into()),
            OpCode::CHAINID => stack.push(host.get_tx_context()?.chain_id),
            OpCode::BASEFEE => stack.push(host.get_tx_context()?.block_base_fee),
            OpCode::SELFBALANCE => {
                external::selfbalance(state, host);
            }
            OpCode::POP => pop(stack),
            OpCode::MLOAD => memory::mload(state)?,
            OpCode::MSTORE => memory::mstore(state)?,
            OpCode::MSTORE8 => memory::mstore8(state)?,
            OpCode::JUMP => {
                let dst = stack.pop();
                pc = op_jump(dst, &s.jumpdest_map)?;

                continue;
            }
            OpCode::JUMPI => {
                let dst = stack.pop();
                let b = stack.pop();
                if b != 0 {
                    pc = op_jump(dst, &s.jumpdest_map)?;
                    continue;
                }
            }
            OpCode::PC => stack.push(u128::try_from(pc).unwrap().into()),
            OpCode::MSIZE => memory::msize(state),
            OpCode::SLOAD => {
                external::sload::<_, REVISION>(state, host)?;
            }
            OpCode::SSTORE => {
                external::sstore::<_, REVISION>(state, host)?;
            }
            OpCode::GAS => stack
                .push(u128::try_from(state.gas_left).unwrap().into()),
            OpCode::JUMPDEST => {}
            OpCode::PUSH1 => {
                push1(stack, s.padded_code[pc + 1]);
                pc += 1;
            }
            OpCode::PUSH2 => pc += push::<2>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH3 => pc += push::<3>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH4 => pc += push::<4>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH5 => pc += push::<5>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH6 => pc += push::<6>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH7 => pc += push::<7>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH8 => pc += push::<8>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH9 => pc += push::<9>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH10 => pc += push::<10>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH11 => pc += push::<11>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH12 => pc += push::<12>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH13 => pc += push::<13>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH14 => pc += push::<14>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH15 => pc += push::<15>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH16 => pc += push::<16>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH17 => pc += push::<17>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH18 => pc += push::<18>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH19 => pc += push::<19>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH20 => pc += push::<20>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH21 => pc += push::<21>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH22 => pc += push::<22>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH23 => pc += push::<23>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH24 => pc += push::<24>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH25 => pc += push::<25>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH26 => pc += push::<26>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH27 => pc += push::<27>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH28 => pc += push::<28>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH29 => pc += push::<29>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH30 => pc += push::<30>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH31 => pc += push::<31>(stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH32 => {
                push32(stack, &s.padded_code[pc + 1..]);
                pc += 32;
            }

            OpCode::DUP1 => dup::<1>(stack),
            OpCode::DUP2 => dup::<2>(stack),
            OpCode::DUP3 => dup::<3>(stack),
            OpCode::DUP4 => dup::<4>(stack),
            OpCode::DUP5 => dup::<5>(stack),
            OpCode::DUP6 => dup::<6>(stack),
            OpCode::DUP7 => dup::<7>(stack),
            OpCode::DUP8 => dup::<8>(stack),
            OpCode::DUP9 => dup::<9>(stack),
            OpCode::DUP10 => dup::<10>(stack),
            OpCode::DUP11 => dup::<11>(stack),
            OpCode::DUP12 => dup::<12>(stack),
            OpCode::DUP13 => dup::<13>(stack),
            OpCode::DUP14 => dup::<14>(stack),
            OpCode::DUP15 => dup::<15>(stack),
            OpCode::DUP16 => dup::<16>(stack),

            OpCode::SWAP1 => swap::<1>(stack),
            OpCode::SWAP2 => swap::<2>(stack),
            OpCode::SWAP3 => swap::<3>(stack),
            OpCode::SWAP4 => swap::<4>(stack),
            OpCode::SWAP5 => swap::<5>(stack),
            OpCode::SWAP6 => swap::<6>(stack),
            OpCode::SWAP7 => swap::<7>(stack),
            OpCode::SWAP8 => swap::<8>(stack),
            OpCode::SWAP9 => swap::<9>(stack),
            OpCode::SWAP10 => swap::<10>(stack),
            OpCode::SWAP11 => swap::<11>(stack),
            OpCode::SWAP12 => swap::<12>(stack),
            OpCode::SWAP13 => swap::<13>(stack),
            OpCode::SWAP14 => swap::<14>(stack),
            OpCode::SWAP15 => swap::<15>(stack),
            OpCode::SWAP16 => swap::<16>(stack),

            OpCode::LOG0 => external::do_log::<_, 0>(state, host)?,
            OpCode::LOG1 => external::do_log::<_, 1>(state, host)?,
            OpCode::LOG2 => external::do_log::<_, 2>(state, host)?,
            OpCode::LOG3 => external::do_log::<_, 3>(state, host)?,
            OpCode::LOG4 => external::do_log::<_, 4>(state, host)?,
            OpCode::CREATE => call::do_create::<_, REVISION, false>(state, host)?,
            OpCode::CREATE2 => call::do_create::<_, REVISION, true>(state, host)?,
            OpCode::CALL => call::do_call::<_, REVISION, { CallKind::Call }, false>(state, host)?,
            OpCode::CALLCODE => {
                call::do_call::<_, REVISION, { CallKind::CallCode }, false>(state, host)?
            }
            OpCode::DELEGATECALL => {
                call::do_call::<_, REVISION, { CallKind::DelegateCall }, false>(state, host)?
            }
            OpCode::STATICCALL => {
                call::do_call::<_, REVISION, { CallKind::Call }, true>(state, host)?
            }
            OpCode::RETURN | OpCode::REVERT => {
                ret(state)?;
                reverted = op == OpCode::REVERT;
                break;
            }
            OpCode::INVALID => {
                return Err(StatusCode::InvalidInstruction);
            }
            OpCode::SELFDESTRUCT => {
                external::selfdestruct::<_, REVISION>(state, host)?;
                break;
            }
            other => {
                unreachable!("reached unhandled opcode: {}", other);
            }
        }

        pc += 1;
    }

    let output = SuccessfulOutput {
        reverted,
        gas_left: state.gas_left,
        output_data: state.output_data.clone(),
    };

    Ok(output)
}
