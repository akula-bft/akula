use crate::execution::evm::{interpreter::JumpdestMap, state::ExecutionState, StatusCode};
use ethnum::U256;

#[inline]
pub(crate) fn ret(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let stack = state.mem.stack();
    let offset = *stack.get(0);
    let size = *stack.get(1);

    if let Some(region) =
        super::memory::get_memory_region(state, offset, size).map_err(|_| StatusCode::OutOfGas)?
    {
        state.output_data = state.mem.heap()[region.offset..region.offset + region.size.get()]
            .to_vec()
            .into();
    }

    Ok(())
}

#[inline]
pub(crate) fn op_jump(
    dst: U256,
    jumpdest_map: &JumpdestMap,
) -> Result<usize, StatusCode> {
    if !jumpdest_map.contains(dst) {
        return Err(StatusCode::BadJumpDestination);
    }

    Ok(dst.as_usize())
}

#[inline]
pub(crate) fn calldataload(state: &mut ExecutionState) {
    let mut stack = state.mem.stack();
    let index = stack.pop();

    let input_len = state.message.input_data.len();

    stack.push({
        if index > u128::try_from(input_len).unwrap() {
            U256::ZERO
        } else {
            let index_usize = index.as_usize();
            let end = core::cmp::min(index_usize + 32, input_len);

            let mut data = [0; 32];
            data[..end - index_usize].copy_from_slice(&state.message.input_data[index_usize..end]);

            U256::from_be_bytes(data)
        }
    });
}

#[inline]
pub(crate) fn calldatasize(state: &mut ExecutionState) {
    state.mem.stack().push(
        u128::try_from(state.message.input_data.len())
            .unwrap()
            .into(),
    );
}
