use crate::execution::evm::{common::*, state::*};
use ethnum::U256;
use sha3::{Digest, Keccak256};
use std::{cmp::min, num::NonZeroUsize};

pub(crate) const MAX_BUFFER_SIZE: u128 = u32::MAX as u128;

/// The size of the EVM 256-bit word.
const WORD_SIZE: i64 = 32;

/// Returns number of words what would fit to provided number of bytes,
/// i.e. it rounds up the number bytes to number of words.
#[inline(always)]
pub(crate) fn num_words(size_in_bytes: usize) -> i64 {
    ((size_in_bytes as i64) + (WORD_SIZE - 1)) / WORD_SIZE
}

#[inline(always)]
pub(crate) fn mload(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();

    let region = get_memory_region_u64(state, index, NonZeroUsize::new(32).unwrap())
        .map_err(|_| StatusCode::OutOfGas)?;

    let value = u256_from_slice(&state.memory[region.offset..region.offset + region.size.get()]);

    state.stack.push(value);

    Ok(())
}

#[inline(always)]
pub(crate) fn mstore(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();
    let value = state.stack.pop();

    let region = get_memory_region_u64(state, index, NonZeroUsize::new(32).unwrap())
        .map_err(|_| StatusCode::OutOfGas)?;

    state.memory[region.offset..region.offset + 32].copy_from_slice(&value.to_be_bytes());

    Ok(())
}

#[inline(always)]
pub(crate) fn mstore8(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();
    let value = state.stack.pop();

    let region = get_memory_region_u64(state, index, NonZeroUsize::new(1).unwrap())
        .map_err(|_| StatusCode::OutOfGas)?;

    let value = (*value.low() as u32 & 0xff) as u8;

    state.memory[region.offset] = value;

    Ok(())
}

#[inline(always)]
pub(crate) fn msize(state: &mut ExecutionState) {
    state
        .stack
        .push(u64::try_from(state.memory.len()).unwrap().into());
}

#[inline(never)]
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

#[inline(always)]
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

#[inline(always)]
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

        if copy_size > 0 {
            state.memory[region.offset..region.offset + copy_size]
                .copy_from_slice(&state.message.input_data[src..src + copy_size]);
        }

        if region.size.get() > copy_size {
            state.memory[region.offset + copy_size..region.offset + region.size.get()].fill(0);
        }
    }

    Ok(())
}

pub(crate) fn keccak256(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, index, size).map_err(|_| StatusCode::OutOfGas)?;

    state.stack.push(u256_from_slice(&*Keccak256::digest(
        if let Some(region) = region {
            let w = num_words(region.size.get());
            let cost = w * 6;
            state.gas_left -= cost;
            if state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }

            &state.memory[region.offset..region.offset + region.size.get()]
        } else {
            &[]
        },
    )));

    Ok(())
}

#[inline(always)]
pub(crate) fn codesize(stack: &mut Stack, code: &[u8]) {
    stack.push(u128::try_from(code.len()).unwrap().into())
}

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
        if copy_size > 0 {
            state.memory[region.offset..region.offset + copy_size]
                .copy_from_slice(&code[src..src + copy_size]);
        }

        if region.size.get() > copy_size {
            state.memory[region.offset + copy_size..region.offset + region.size.get()].fill(0);
        }
    }

    Ok(())
}

#[doc(hidden)]
#[macro_export]
macro_rules! extcodecopy {
    ($state:expr,$rev:expr) => {
        use core::cmp::min;
        use $crate::{
            execution::evm::{
                common::*,
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::{memory::*, properties::*},
            },
            models::*,
        };

        let addr = u256_to_address($state.stack.pop());
        let mem_index = $state.stack.pop();
        let input_index = $state.stack.pop();
        let size = $state.stack.pop();

        let region =
            get_memory_region(&mut $state, mem_index, size).map_err(|_| StatusCode::OutOfGas)?;

        if let Some(region) = &region {
            let copy_cost = num_words(region.size.get()) * 3;
            $state.gas_left -= copy_cost;
            if $state.gas_left < 0 {
                return Err(StatusCode::OutOfGas);
            }
        }

        if $rev >= Revision::Berlin {
            if ResumeData::into_access_account_status({
                yield InterruptData::AccessAccount { address: addr }
            })
            .unwrap()
            .status
                == AccessStatus::Cold
            {
                $state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
                if $state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
            }
        }

        if let Some(region) = region {
            let src = min(U256::from(MAX_BUFFER_SIZE), input_index).as_usize();

            let code = ResumeData::into_code({
                yield InterruptData::CopyCode {
                    address: addr,
                    offset: src,
                    max_size: region.size.get(),
                }
            })
            .unwrap()
            .code;

            $state.memory[region.offset..region.offset + code.len()].copy_from_slice(&code);
            if region.size.get() > code.len() {
                $state.memory[region.offset + code.len()..region.offset + region.size.get()]
                    .fill(0);
            }
        }
    };
}

pub(crate) fn returndatasize(state: &mut ExecutionState) {
    state
        .stack
        .push(u128::try_from(state.return_data.len()).unwrap().into());
}

pub(crate) fn returndatacopy(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let mem_index = state.stack.pop();
    let input_index = state.stack.pop();
    let size = state.stack.pop();

    let region = get_memory_region(state, mem_index, size).map_err(|_| StatusCode::OutOfGas)?;

    if input_index > u128::try_from(state.return_data.len()).unwrap() {
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

        state.memory[region.offset..region.offset + region.size.get()]
            .copy_from_slice(&state.return_data[src..src + region.size.get()]);
    }

    Ok(())
}

#[doc(hidden)]
#[macro_export]
macro_rules! extcodehash {
    ($state:expr,$rev:expr) => {
        use $crate::{
            execution::evm::{
                common::*,
                continuation::{interrupt_data::*, resume_data::*},
                host::*,
                instructions::properties::*,
            },
            models::*,
        };

        let addr = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            if ResumeData::into_access_account_status({
                yield InterruptData::AccessAccount { address: addr }
            })
            .unwrap()
            .status
                == AccessStatus::Cold
            {
                $state.gas_left -= i64::from(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST);
                if $state.gas_left < 0 {
                    return Err(StatusCode::OutOfGas);
                }
            }
        }

        let code_hash =
            ResumeData::into_code_hash({ yield InterruptData::GetCodeHash { address: addr } })
                .unwrap()
                .hash;
        $state.stack.push(code_hash);
    };
}
