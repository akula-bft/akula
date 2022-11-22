use crate::{
    execution::evm::{common::*, state::*, Host},
    models::Revision,
};
use sha3::{Digest, Keccak256};

#[inline]
pub(crate) fn mload(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack().pop();

    let mem = state.get_heap(index, 32)?;
    let value = u256_from_slice(mem);

    state.stack().push(value);

    Ok(())
}

#[inline]
pub(crate) fn mstore(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack().pop();
    let value = state.stack().pop();

    let mem = state.get_heap(index, 32)?;
    mem.copy_from_slice(&value.to_be_bytes());

    Ok(())
}

#[inline]
pub(crate) fn mstore8(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack().pop();
    let value = state.stack().pop();

    let value = (*value.low() as u32 & 0xff) as u8;
    let mem = state.get_heap(index, 1)?;
    mem[0] = value;

    Ok(())
}

#[inline]
pub(crate) fn msize(state: &mut ExecutionState) {
    let res = state.heap_size().into();
    state.stack().push(res);
}

#[inline(always)]
fn copy(state: &mut ExecutionState, data: &[u8]) -> Result<(), StatusCode> {
    let mem_index = state.stack().pop();
    let input_index = state.stack().pop();
    let size = state.stack().pop();

    let size = size.try_into().map_err(|_| StatusCode::OutOfGas)?;
    let copy_cost = 3 * num_words(size);
    state.gas_left -= copy_cost as i64;
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }

    let mem = state.get_heap(mem_index, size)?;
    match input_index.try_into() {
        Ok(index) if index <= data.len() => {
            let data = &data[index..];
            if size as usize <= data.len() {
                mem.copy_from_slice(&data[..size as usize]);
            } else {
                let (dst1, dst2) = mem.split_at_mut(data.len());
                dst1.copy_from_slice(data);
                dst2.fill(0);
            }
        }
        // Index either can not be converted to `usize`, or bigger than data length.
        _ => mem.fill(0),
    }

    Ok(())
}

#[inline]
pub(crate) fn calldatacopy(state: &mut ExecutionState) -> Result<(), StatusCode> {
    copy(state, &state.message.input_data)
}

#[inline]
pub(crate) fn codecopy(state: &mut ExecutionState, code: &[u8]) -> Result<(), StatusCode> {
    copy(state, code)
}

pub(crate) fn keccak256(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack().pop();
    let size = state.stack().pop();

    let size = size.try_into().map_err(|_| StatusCode::OutOfGas)?;
    let cost = 6 * num_words(size);
    state.gas_left -= cost as i64;
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }
    let data = state.get_heap(index, size)?;
    let hash = Keccak256::digest(data);
    state.stack().push(u256_from_slice(&hash));

    Ok(())
}

#[inline]
pub(crate) fn codesize(stack: &mut EvmStack, code: &[u8]) {
    stack.push(u128::try_from(code.len()).unwrap().into())
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn extcodecopy<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{
            common::*,
            host::*,
            instructions::{memory::*, properties::*},
        },
        models::*,
    };
    let addr = u256_to_address(state.stack().pop());
    let mem_index = state.stack().pop();
    let input_index = state.stack().pop();
    let size = state.stack().pop();

    let size = size.try_into().map_err(|_| StatusCode::OutOfGas)?;

    let copy_cost = 3 * num_words(size);
    state.gas_left -= copy_cost as i64;
    if REVISION >= Revision::Berlin {
        if host.access_account(addr) == AccessStatus::Cold {
            state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
        }
    }
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }

    if size > 0 {
        let mem = state.get_heap(mem_index, size)?;
        let src = input_index.try_into().unwrap_or(u32::MAX as usize);

        // TODO: remove allocation
        let mut code = vec![0; size as usize];
        let copied = host.copy_code(addr, src, &mut code[..]);
        debug_assert!(copied <= code.len());
        code.truncate(copied);

        let (dst1, dst2) = mem.split_at_mut(code.len());
        dst1.copy_from_slice(&code);
        dst2.fill(0);
    }

    Ok(())
}

#[inline]
pub(crate) fn returndatasize(state: &mut ExecutionState) {
    let res = u128::try_from(state.return_data.len()).unwrap().into();
    state.stack().push(res);
}

#[inline]
pub(crate) fn returndatacopy(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let mem_index = state.stack().pop();
    let input_index = state.stack().pop();
    let size = state
        .stack()
        .pop()
        .try_into()
        .map_err(|_| StatusCode::InvalidMemoryAccess)?;

    let data = &state.return_data;
    match input_index.try_into() {
        Ok(index) if index <= data.len() => {
            let data = &data[index..];
            if size as usize <= data.len() {
                let copy_cost = 3 * num_words(size);
                state.gas_left -= copy_cost as i64;
                if state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
                // TODO: remove allocation
                let data = data.to_vec();
                let mem = state.get_heap(mem_index, size)?;
                mem.copy_from_slice(&data[..size as usize]);
                Ok(())
            } else {
                Err(StatusCode::InvalidMemoryAccess)
            }
        }
        _ => Err(StatusCode::InvalidMemoryAccess),
    }
}

#[inline]
#[allow(clippy::collapsible_if)]
pub(crate) fn extcodehash<H: Host, const REVISION: Revision>(
    state: &mut ExecutionState,
    host: &mut H,
) -> Result<(), StatusCode> {
    use crate::{
        execution::evm::{common::*, host::*, instructions::properties::*},
        models::*,
    };

    let addr = u256_to_address(state.stack().pop());

    if REVISION >= Revision::Berlin {
        if host.access_account(addr) == AccessStatus::Cold {
            state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
            if state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }
        }
    }

    state.stack().push(host.get_code_hash(addr));

    Ok(())
}
