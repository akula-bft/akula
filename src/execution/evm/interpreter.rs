use self::instruction_table::*;
use super::{
    common::{InterpreterMessage, *},
    instructions::{control::*, stack_manip::*, *},
    state::*,
    *,
};
use crate::models::*;
use bitvec::{bitvec, vec::BitVec};
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

    let stack_size = state.stack().len();
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
pub struct JumpdestMap(Arc<BitVec>);

impl JumpdestMap {
    #[inline]
    pub fn contains(&self, dst: U256) -> bool {
        dst.try_into()
            .ok()
            .and_then(|pos: usize| self.0.get(pos))
            .map(|br| *br)
            .unwrap_or(false)
    }
}

// We always reserve 32 bytes for potential last opcode
// being equal to PUSH32
const PADDING: usize = 32;

/// Code with analysis.
#[derive(Clone, Debug)]
pub struct AnalyzedCode {
    jumpdest_map: JumpdestMap,
    code: Vec<u8>,
}

impl AnalyzedCode {
    /// Analyze code and prepare it for execution.
    pub fn analyze(code: &[u8]) -> Self {
        const JUMPDEST: u8 = OpCode::JUMPDEST.0;
        const PUSH1: u8 = OpCode::PUSH1.0;
        const PUSH32: u8 = OpCode::PUSH32.0;

        let code_len = code.len();
        let mut jumpdest_map = bitvec![0; code_len];

        let mut i = 0;
        while i < code_len {
            i += match code[i] {
                JUMPDEST => {
                    jumpdest_map.set(i, true);
                    1
                }
                opcode @ PUSH1..=PUSH32 => (opcode - PUSH1 + 2) as usize,
                _ => 1,
            };
        }

        // Without this assert compiler generates a bunch of unnecessary
        // code which handles `code.len() > usize::MAX - PADDING`
        assert!(code.len() <= isize::MAX as usize);
        let code = {
            let mut t = Vec::with_capacity(code.len() + PADDING);
            t.extend_from_slice(code);
            t.extend_from_slice(&[OpCode::STOP.0; PADDING]);
            t
        };

        Self {
            jumpdest_map: JumpdestMap(Arc::new(jumpdest_map)),
            code,
        }
    }

    /// Execute analyzed EVM bytecode using provided `Host` context.
    pub fn execute<H>(
        &self,
        host: &mut H,
        message: &InterpreterMessage,
        revision: Revision,
        mem: EvmSubMemory,
    ) -> Output
    where
        H: Host,
    {
        let mut state = ExecutionState::new(message, mem);

        macro_rules! execute_revisions {
            (
                $trace:expr, $revision:expr, $self:expr, $state:expr, $host:expr,
                $($rev:ident )*
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
            host.trace_instructions(),
            revision,
            self,
            &mut state,
            host,
            Frontier Homestead Tangerine Spurious Byzantium Constantinople
            Petersburg Istanbul Berlin London Paris
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

    pub fn padded_code(&self) -> &[u8] {
        &self.code
    }

    pub fn orig_code(&self) -> &[u8] {
        // This never panics since during construction we enforce that
        // length of `code` is bigger or equal to `PADDING`
        &self.code[..self.code.len() - PADDING]
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
    macro_rules! push_tx_ctx {
        ($stack:expr, $host:expr, $attr:ident) => {{
            let val = $host.get_tx_context()?.$attr;
            $stack.push(val.into());
        }};
        ($stack:expr, $host:expr, $attr:ident, addr) => {{
            let val = $host.get_tx_context()?.$attr;
            $stack.push(address_to_u256(val));
        }};
    }

    let instruction_table = get_instruction_table(REVISION);

    let mut pc = 0;
    let code = s.orig_code();
    let reverted = loop {
        let op = match code.get(pc) {
            Some(&op) => OpCode(op),
            None => break false,
        };

        let metrics = &instruction_table[op.to_usize()];

        if metrics.gas_cost < 0 {
            return Err(StatusCode::UndefinedInstruction);
        }

        if TRACE {
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

        check_requirements(metrics, state)?;

        let stack = &mut state.mem.stack();
        match op {
            OpCode::STOP => break false,
            OpCode::ADD => arithmetic::add(stack),
            OpCode::MUL => arithmetic::mul(stack),
            OpCode::SUB => arithmetic::sub(stack),
            OpCode::DIV => arithmetic::div(stack),
            OpCode::SDIV => arithmetic::sdiv(stack),
            OpCode::MOD => arithmetic::modulo(stack),
            OpCode::SMOD => arithmetic::smod(stack),
            OpCode::ADDMOD => arithmetic::addmod(stack),
            OpCode::MULMOD => arithmetic::mulmod(stack),
            OpCode::EXP => arithmetic::exp::<REVISION>(state)?,
            OpCode::SIGNEXTEND => arithmetic::signextend(stack),
            OpCode::LT => boolean::lt(stack),
            OpCode::GT => boolean::gt(stack),
            OpCode::SLT => boolean::slt(stack),
            OpCode::SGT => boolean::sgt(stack),
            OpCode::EQ => boolean::eq(stack),
            OpCode::ISZERO => boolean::iszero(stack),
            OpCode::AND => boolean::and(stack),
            OpCode::OR => boolean::or(stack),
            OpCode::XOR => boolean::xor(stack),
            OpCode::NOT => boolean::not(stack),
            OpCode::BYTE => bitwise::byte(stack),
            OpCode::SHL => bitwise::shl(stack),
            OpCode::SHR => bitwise::shr(stack),
            OpCode::SAR => bitwise::sar(stack),
            OpCode::KECCAK256 => memory::keccak256(state)?,
            OpCode::ADDRESS => external::address(state),
            OpCode::BALANCE => external::balance::<_, REVISION>(state, host)?,
            OpCode::CALLER => external::caller(state),
            OpCode::CALLVALUE => external::callvalue(state),
            OpCode::CALLDATALOAD => calldataload(state),
            OpCode::CALLDATASIZE => calldatasize(state),
            OpCode::CALLDATACOPY => memory::calldatacopy(state)?,
            OpCode::CODESIZE => memory::codesize(stack, code),
            OpCode::CODECOPY => memory::codecopy(state, code)?,
            OpCode::EXTCODESIZE => external::extcodesize::<_, REVISION>(state, host)?,
            OpCode::EXTCODECOPY => memory::extcodecopy::<_, REVISION>(state, host)?,
            OpCode::RETURNDATASIZE => memory::returndatasize(state),
            OpCode::RETURNDATACOPY => memory::returndatacopy(state)?,
            OpCode::EXTCODEHASH => memory::extcodehash::<_, REVISION>(state, host)?,
            OpCode::BLOCKHASH => external::blockhash(state, host)?,
            OpCode::ORIGIN => push_tx_ctx!(stack, host, tx_origin, addr),
            OpCode::COINBASE => push_tx_ctx!(stack, host, block_coinbase, addr),
            OpCode::GASPRICE => push_tx_ctx!(stack, host, tx_gas_price),
            OpCode::TIMESTAMP => push_tx_ctx!(stack, host, block_timestamp),
            OpCode::NUMBER => push_tx_ctx!(stack, host, block_number),
            OpCode::DIFFICULTY => push_tx_ctx!(stack, host, block_difficulty),
            OpCode::GASLIMIT => push_tx_ctx!(stack, host, block_gas_limit),
            OpCode::CHAINID => push_tx_ctx!(stack, host, chain_id),
            OpCode::BASEFEE => push_tx_ctx!(stack, host, block_base_fee),
            OpCode::SELFBALANCE => external::selfbalance(state, host),
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
            OpCode::SLOAD => external::sload::<_, REVISION>(state, host)?,
            OpCode::SSTORE => external::sstore::<_, REVISION>(state, host)?,
            OpCode::GAS => {
                let gas = u128::try_from(state.gas_left).unwrap().into();
                stack.push(gas);
            }
            OpCode::JUMPDEST => {}
            OpCode::PUSH1 => pc += push::<1>(stack, s, pc),
            OpCode::PUSH2 => pc += push::<2>(stack, s, pc),
            OpCode::PUSH3 => pc += push::<3>(stack, s, pc),
            OpCode::PUSH4 => pc += push::<4>(stack, s, pc),
            OpCode::PUSH5 => pc += push::<5>(stack, s, pc),
            OpCode::PUSH6 => pc += push::<6>(stack, s, pc),
            OpCode::PUSH7 => pc += push::<7>(stack, s, pc),
            OpCode::PUSH8 => pc += push::<8>(stack, s, pc),
            OpCode::PUSH9 => pc += push::<9>(stack, s, pc),
            OpCode::PUSH10 => pc += push::<10>(stack, s, pc),
            OpCode::PUSH11 => pc += push::<11>(stack, s, pc),
            OpCode::PUSH12 => pc += push::<12>(stack, s, pc),
            OpCode::PUSH13 => pc += push::<13>(stack, s, pc),
            OpCode::PUSH14 => pc += push::<14>(stack, s, pc),
            OpCode::PUSH15 => pc += push::<15>(stack, s, pc),
            OpCode::PUSH16 => pc += push::<16>(stack, s, pc),
            OpCode::PUSH17 => pc += push::<17>(stack, s, pc),
            OpCode::PUSH18 => pc += push::<18>(stack, s, pc),
            OpCode::PUSH19 => pc += push::<19>(stack, s, pc),
            OpCode::PUSH20 => pc += push::<20>(stack, s, pc),
            OpCode::PUSH21 => pc += push::<21>(stack, s, pc),
            OpCode::PUSH22 => pc += push::<22>(stack, s, pc),
            OpCode::PUSH23 => pc += push::<23>(stack, s, pc),
            OpCode::PUSH24 => pc += push::<24>(stack, s, pc),
            OpCode::PUSH25 => pc += push::<25>(stack, s, pc),
            OpCode::PUSH26 => pc += push::<26>(stack, s, pc),
            OpCode::PUSH27 => pc += push::<27>(stack, s, pc),
            OpCode::PUSH28 => pc += push::<28>(stack, s, pc),
            OpCode::PUSH29 => pc += push::<29>(stack, s, pc),
            OpCode::PUSH30 => pc += push::<30>(stack, s, pc),
            OpCode::PUSH31 => pc += push::<31>(stack, s, pc),
            OpCode::PUSH32 => pc += push::<32>(stack, s, pc),

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
            OpCode::RETURN => {
                ret(state)?;
                break false;
            }
            OpCode::REVERT => {
                ret(state)?;
                break true;
            }
            OpCode::INVALID => {
                return Err(StatusCode::InvalidInstruction);
            }
            OpCode::SELFDESTRUCT => {
                external::selfdestruct::<_, REVISION>(state, host)?;
                break false;
            }
            other => {
                unreachable!("reached unhandled opcode: {}", other);
            }
        }

        pc += 1;
    };

    let output = SuccessfulOutput {
        reverted,
        gas_left: state.gas_left,
        output_data: state.output_data.clone(),
    };

    Ok(output)
}
