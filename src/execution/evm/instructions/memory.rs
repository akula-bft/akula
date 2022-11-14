use crate::{
    execution::evm::{common::*, state::*, Host},
    models::Revision,
};
use ethnum::U256;
use sha3::{Digest, Keccak256};
use std::{cmp::min, num::NonZeroUsize};

pub(crate) const MAX_BUFFER_SIZE: u128 = u32::MAX as u128;

/// The size of the EVM 256-bit word.
const WORD_SIZE: i64 = 32;

/// Returns number of words what would fit to provided number of bytes,
/// i.e. it rounds up the number bytes to number of words.
#[inline]
pub(crate) fn num_words(size_in_bytes: usize) -> i64 {
    ((size_in_bytes as i64) + (WORD_SIZE - 1)) / WORD_SIZE
}

#[inline]
pub(crate) fn mload(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();

    let region = get_memory_region_u64(state, index, NonZeroUsize::new(32).unwrap())
        .map_err(|_| StatusCode::OutOfGas)?;

    let value = u256_from_slice(&state.memory[region.offset..][..region.size.get()]);

    state.stack.push(value);

    Ok(())
}

#[inline]
pub(crate) fn mstore(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();
    let value = state.stack.pop();

    let region = get_memory_region_u64(state, index, NonZeroUsize::new(32).unwrap())
        .map_err(|_| StatusCode::OutOfGas)?;

    state.memory[region.offset..][..32].copy_from_slice(&value.to_be_bytes());

    Ok(())
}

#[inline]
pub(crate) fn mstore8(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();
    let value = state.stack.pop();

    let region = get_memory_region_u64(state, index, NonZeroUsize::new(1).unwrap())
        .map_err(|_| StatusCode::OutOfGas)?;

    let value = (*value.low() as u32 & 0xff) as u8;

    state.memory[region.offset] = value;

    Ok(())
}

#[inline]
pub(crate) fn msize(state: &mut ExecutionState) {
    let res = u64::try_from(state.memory.len()).unwrap().into();
    state.stack.push(res);
}

#[inline]
fn grow_memory(state: &mut ExecutionState, new_size: usize) -> Result<(), ()> {
    let new_words = num_words(new_size);
    let current_words = (state.memory.len() / 32) as i64;
    let new_cost = 3 * new_words + new_words * new_words / 512;
    let current_cost = 3 * current_words + current_words * current_words / 512;
    let cost = new_cost - current_cost;

    state.gas_left -= cost;

    if state.gas_left < 0 {
        return Err(());
    }

    state.memory.grow((new_words * WORD_SIZE) as usize);

    Ok(())
}

#[inline]
pub(crate) fn get_memory_region_u64(
    state: &mut ExecutionState,
    offset: U256,
    size: NonZeroUsize,
) -> Result<MemoryRegion, ()> {
    if offset > MAX_BUFFER_SIZE {
        return Err(());
    }

    let new_size = offset.as_usize() + size.get();
    let current_size = state.memory.len();
    if new_size > current_size {
        grow_memory(state, new_size)?;
    }

    Ok(MemoryRegion {
        offset: offset.as_usize(),
        size,
    })
}

pub(crate) struct MemoryRegion {
    pub offset: usize,
    pub size: NonZeroUsize,
}

#[inline]
pub(crate) fn get_memory_region(
    state: &mut ExecutionState,
    offset: U256,
    size: U256,
) -> Result<Option<MemoryRegion>, ()> {
    if size == 0 {
        return Ok(None);
    }

    if size > MAX_BUFFER_SIZE {
        return Err(());
    }

    get_memory_region_u64(state, offset, NonZeroUsize::new(size.as_usize()).unwrap()).map(Some)
}

#[inline]
pub(crate) fn calldatacopy(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let mem_index = state.stack.pop();
    let input_index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, mem_index, size).map_err(|_| StatusCode::OutOfGas)?;

    if let Some(region) = &region {
        let copy_cost = num_words(region.size.get()) * 3;
        state.gas_left -= copy_cost;
        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }

        let input_len = u128::try_from(state.message.input_data.len())
            .unwrap()
            .into();

        let src = core::cmp::min(input_len, input_index);
        let copy_size = core::cmp::min(size, input_len - src).as_usize();
        let src = src.as_usize();

        let src = &state.message.input_data[src..][..copy_size];
        let dst = &mut state.memory[region.offset..][..region.size.get()];
        let (dst1, dst2) = dst.split_at_mut(copy_size);
        dst1.copy_from_slice(src);
        dst2.fill(0);
    }

    Ok(())
}

pub(crate) fn keccak256(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, index, size).map_err(|_| StatusCode::OutOfGas)?;

    let hash = match region {
        Some(region) => {
            let w = num_words(region.size.get());
            let cost = w * 6;
            state.gas_left -= cost;
            if state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }

            let data = &state.memory[region.offset..][..region.size.get()];
            Keccak256::digest(data)
        }
        None => Keccak256::digest([]),
    };

    state.stack.push(u256_from_slice(&hash));

    Ok(())
}

#[inline]
pub(crate) fn codesize(stack: &mut Stack, code: &[u8]) {
    stack.push(u128::try_from(code.len()).unwrap().into())
}

#[inline]
pub(crate) fn codecopy(state: &mut ExecutionState, code: &[u8]) -> Result<(), StatusCode> {
    // TODO: Similar to calldatacopy().

    let mem_index = state.stack.pop();
    let input_index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, mem_index, size).map_err(|_| StatusCode::OutOfGas)?;

    if let Some(region) = region {
        let src = min(U256::from(u128::try_from(code.len()).unwrap()), input_index).as_usize();
        let copy_size = min(region.size.get(), code.len() - src);

        let copy_cost = num_words(region.size.get()) * 3;
        state.gas_left -= copy_cost;
        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }

        // TODO: Add unit tests for each combination of conditions.
        let src = &code[src..][..copy_size];
        let dst = &mut state.memory[region.offset..][..region.size.get()];
        let (dst1, dst2) = dst.split_at_mut(copy_size);
        dst1.copy_from_slice(src);
        dst2.fill(0);
    }

    Ok(())
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

    let addr = u256_to_address(state.stack.pop());
    let mem_index = state.stack.pop();
    let input_index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, mem_index, size).map_err(|_| StatusCode::OutOfGas)?;

    if let Some(region) = &region {
        let copy_cost = num_words(region.size.get()) * 3;
        state.gas_left -= copy_cost;
        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }
    }

    if REVISION >= Revision::Berlin {
        if host.access_account(addr) == AccessStatus::Cold {
            state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
            if state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }
        }
    }

    if let Some(region) = region {
        let src = min(U256::from(MAX_BUFFER_SIZE), input_index).as_usize();

        let mut code = vec![0; region.size.get()];
        let copied = host.copy_code(addr, src, &mut code[..]);
        debug_assert!(copied <= code.len());
        code.truncate(copied);

        let dst = &mut state.memory[region.offset..][..region.size.get()];
        let (dst1, dst2) = dst.split_at_mut(code.len());
        dst1.copy_from_slice(&code);
        dst2.fill(0);
    }

    Ok(())
}

#[inline]
pub(crate) fn returndatasize(state: &mut ExecutionState) {
    let res = u128::try_from(state.return_data.len()).unwrap().into();
    state.stack.push(res);
}

#[inline]
pub(crate) fn returndatacopy(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let mem_index = state.stack.pop();
    let input_index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, mem_index, size).map_err(|_| StatusCode::OutOfGas)?;

    if input_index > state.return_data.len() as u128 {
        return Err(StatusCode::InvalidMemoryAccess);
    }
    let src = input_index.as_usize();

    if src + region.as_ref().map(|r| r.size.get()).unwrap_or(0) > state.return_data.len() {
        return Err(StatusCode::InvalidMemoryAccess);
    }

    if let Some(region) = region {
        let copy_cost = num_words(region.size.get()) * 3;
        state.gas_left -= copy_cost;
        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }

        let region_size = region.size.get();
        let src = &state.return_data[src..][..region_size];
        let dst = &mut state.memory[region.offset..][..region_size];
        dst.copy_from_slice(src);
    }

    Ok(())
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

    let addr = u256_to_address(state.stack.pop());

    if REVISION >= Revision::Berlin {
        if host.access_account(addr) == AccessStatus::Cold {
            state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
            if state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }
        }
    }

    state.stack.push(host.get_code_hash(addr));

    Ok(())
}
