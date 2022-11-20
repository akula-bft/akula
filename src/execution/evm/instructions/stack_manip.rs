use crate::execution::evm::{common::*, state::Stack, AnalyzedCode};
use arrayref::array_ref;
use ethnum::U256;

#[inline(always)]
pub(crate) fn push<const LEN: usize>(stack: &mut Stack, s: &AnalyzedCode, pc: usize) -> usize {
    let code = &s.padded_code()[pc + 1..];
    match LEN {
        1 => stack.push(code[0].into()),
        2..=31 => stack.push(u256_from_slice(&code[..LEN])),
        32 => stack.push(U256::from_be_bytes(*array_ref!(code, 0, 32))),
        _ => unreachable!(),
    }
    LEN
}

#[inline]
pub(crate) fn dup<const HEIGHT: usize>(stack: &mut Stack) {
    stack.push(*stack.get(HEIGHT - 1));
}

#[inline]
pub(crate) fn swap<const HEIGHT: usize>(stack: &mut Stack) {
    stack.swap_top(HEIGHT);
}

#[inline]
pub(crate) fn pop(stack: &mut Stack) {
    stack.pop();
}
