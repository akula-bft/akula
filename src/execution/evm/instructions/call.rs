use crate::{
    execution::evm::{
        state::num_words, state::MAX_CONTEXT_DEPTH, CallKind, ExecutionState, Host, StatusCode,
    },
    models::Revision,
};

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn do_call<
    H: Host,
    const REVISION: Revision,
    const KIND: CallKind,
    const IS_STATIC: bool,
>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{
            common::u256_to_address, host::*, instructions::properties::*, InterpreterMessage,
        },
        models::*,
    };
    use std::cmp::min;

    let gas = state.stack().pop();
    let dst = u256_to_address(state.stack().pop());
    let value = if IS_STATIC || matches!(KIND, CallKind::DelegateCall) {
        U256::ZERO
    } else {
        state.stack().pop()
    };
    let has_value = value != 0;
    let input_offset = state.stack().pop();
    let input_size = state
        .stack()
        .pop()
        .try_into()
        .map_err(|_| StatusCode::OutOfGas)?;
    let output_offset = state.stack().pop();
    let output_size = state
        .stack()
        .pop()
        .try_into()
        .map_err(|_| StatusCode::OutOfGas)?;

    state.stack().push(U256::ZERO); // Assume failure.

    if REVISION >= Revision::Berlin {
        if host.access_account(dst) == AccessStatus::Cold {
            state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
            if state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }
        }
    }

    let _ = state.get_heap(output_offset, output_size)?;
    let input_data = state.get_heap(input_offset, input_size)?.to_vec().into();
    let mut msg = InterpreterMessage {
        kind: KIND,
        is_static: IS_STATIC || state.message.is_static,
        depth: state.message.depth + 1,
        recipient: if matches!(KIND, CallKind::Call) {
            dst
        } else {
            state.message.recipient
        },
        code_address: dst,
        sender: if matches!(KIND, CallKind::DelegateCall) {
            state.message.sender
        } else {
            state.message.recipient
        },
        real_sender: state.message.recipient,
        gas: i64::MAX,
        value: if matches!(KIND, CallKind::DelegateCall) {
            state.message.value
        } else {
            value
        },
        input_data,
    };

    let mut cost = if has_value { 9000 } else { 0 };

    if matches!(KIND, CallKind::Call) {
        if has_value && state.message.is_static {
            return Err(StatusCode::StaticModeViolation);
        }

        if (has_value || REVISION < Revision::Spurious) && !host.account_exists(dst) {
            cost += 25000;
        }
    }
    state.gas_left -= cost;
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }

    if gas < u128::try_from(msg.gas).unwrap() {
        msg.gas = gas.as_usize() as i64;
    }

    if REVISION >= Revision::Tangerine {
        // TODO: Always true for STATICCALL.
        msg.gas = min(msg.gas, state.gas_left - state.gas_left / 64);
    } else if msg.gas > state.gas_left {
        return Err(StatusCode::OutOfGas);
    }

    if has_value {
        msg.gas += 2300; // Add stipend.
        state.gas_left += 2300;
    }

    state.return_data.clear();

    if state.message.depth < MAX_CONTEXT_DEPTH as i32
        && !(has_value && host.get_balance(state.message.recipient) < value)
    {
        let msg_gas = msg.gas;
        let next_submem = state.mem.next_submem();
        // let _ = state.get_heap(output_offset, output_size)?;
        let result = host.call(Call::Call(&msg), next_submem);
        state.return_data = result.output_data.clone();
        *state.stack().get_mut(0) = if matches!(result.status_code, StatusCode::Success) {
            U256::ONE
        } else {
            U256::ZERO
        };

        let mem = state.get_heap(output_offset, output_size)?;
        let output_data = &result.output_data;
        if mem.len() > output_data.len() {
            mem[..output_data.len()].copy_from_slice(output_data);
        } else {
            mem.copy_from_slice(&output_data[..mem.len()])
        }

        let gas_used = msg_gas - result.gas_left;
        state.gas_left -= gas_used;
    }

    Ok(())
}

#[inline]
pub(crate) fn do_create<H: Host, const REVISION: Revision, const CREATE2: bool>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{common::*, host::*, CreateMessage},
        models::*,
    };
    use ethnum::U256;

    if state.message.is_static {
        return Err(StatusCode::StaticModeViolation);
    }

    let endowment = state.stack().pop();
    let init_code_offset = state.stack().pop();
    let init_code_size = state
        .stack()
        .pop()
        .try_into()
        .map_err(|_| StatusCode::OutOfGas)?;

    let salt = if CREATE2 {
        let salt = state.stack().pop();

        let salt_cost = 6 * num_words(init_code_size);
        state.gas_left -= salt_cost as i64;
        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }

        Some(salt)
    } else {
        None
    };

    state.stack().push(U256::ZERO);
    state.return_data.clear();

    let initcode = state
        .get_heap(init_code_offset, init_code_size)?
        .to_vec()
        .into();
    if state.message.depth < MAX_CONTEXT_DEPTH as i32
        && !(endowment != 0 && host.get_balance(state.message.recipient) < endowment)
    {
        let msg = CreateMessage {
            gas: if REVISION >= Revision::Tangerine {
                state.gas_left - state.gas_left / 64
            } else {
                state.gas_left
            },
            salt,
            initcode,
            sender: state.message.recipient,
            depth: state.message.depth + 1,
            endowment,
        };
        let msg_gas = msg.gas;
        let next_submem = state.mem.next_submem();
        let result = host.call(Call::Create(&msg), next_submem);
        state.gas_left -= msg_gas - result.gas_left;

        state.return_data = result.output_data;
        if result.status_code == StatusCode::Success {
            *state.stack().get_mut(0) =
                address_to_u256(result.create_address.expect("expected create address"));
        }
    }

    Ok(())
}
