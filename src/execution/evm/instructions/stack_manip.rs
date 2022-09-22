use crate::execution::evm::{common::*, state::EvmStack};
use arrayref::array_ref;
use ethnum::U256;

#[inline]
pub(crate) fn push<const LEN: usize>(stack: &mut EvmStack, code: &[u8]) -> usize {
    stack.push(u256_from_slice(&code[..LEN]));
    LEN
}

#[inline]
pub(crate) fn push1(stack: &mut EvmStack, v: u8) {
    stack.push(v.into());
}

#[inline]
pub(crate) fn push32(stack: &mut EvmStack, code: &[u8]) {
    stack.push(U256::from_be_bytes(*array_ref!(code, 0, 32)));
}

#[inline]
pub(crate) fn dup<const HEIGHT: usize>(stack: &mut EvmStack) {
    stack.push(*stack.get(HEIGHT - 1));
}

#[inline]
pub(crate) fn swap<const HEIGHT: usize>(stack: &mut EvmStack) {
    stack.swap_top(HEIGHT);
}

#[inline]
pub(crate) fn pop(stack: &mut EvmStack) {
    stack.pop();
}
