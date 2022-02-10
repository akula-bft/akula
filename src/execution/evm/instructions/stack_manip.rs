use crate::execution::evm::{common::*, state::*};
use arrayref::array_ref;
use ethnum::U256;

#[inline(always)]
pub(crate) fn push<const LEN: usize>(stack: &mut Stack, code: &[u8]) -> usize {
    stack.push(u256_from_slice(&code[..LEN]));
    LEN
}

#[inline(always)]
pub(crate) fn push1(stack: &mut Stack, v: u8) {
    stack.push(v.into());
}

#[inline(always)]
pub(crate) fn push32(stack: &mut Stack, code: &[u8]) {
    stack.push(U256::from_be_bytes(*array_ref!(code, 0, 32)));
}

#[inline(always)]
pub(crate) fn dup<const HEIGHT: usize>(stack: &mut Stack) {
    stack.push(*stack.get(HEIGHT - 1));
}

#[inline(always)]
pub(crate) fn swap<const HEIGHT: usize>(stack: &mut Stack) {
    stack.swap_top(HEIGHT);
}

#[inline(always)]
pub(crate) fn pop(stack: &mut Stack) {
    stack.pop();
}
