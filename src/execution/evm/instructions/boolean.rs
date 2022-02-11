use crate::execution::evm::state::*;
use ethnum::U256;
use i256::i256_cmp;
use std::cmp::Ordering;

#[inline]
pub(crate) fn lt(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    *b = if a.lt(b) { U256::ONE } else { U256::ZERO }
}

#[inline]
pub(crate) fn gt(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    *b = if a.gt(b) { U256::ONE } else { U256::ZERO }
}

#[inline]
pub(crate) fn slt(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    *b = if i256_cmp(a, *b) == Ordering::Less {
        U256::ONE
    } else {
        U256::ZERO
    }
}

#[inline]
pub(crate) fn sgt(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    *b = if i256_cmp(a, *b) == Ordering::Greater {
        U256::ONE
    } else {
        U256::ZERO
    }
}

#[inline]
pub(crate) fn eq(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    *b = if a.eq(b) { U256::ONE } else { U256::ZERO }
}

#[inline]
pub(crate) fn iszero(stack: &mut Stack) {
    let a = stack.get_mut(0);
    *a = if *a == 0 { U256::ONE } else { U256::ZERO }
}

#[inline]
pub(crate) fn and(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = a & *b;
}

#[inline]
pub(crate) fn or(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = a | *b;
}

#[inline]
pub(crate) fn xor(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = a ^ *b;
}

#[inline]
pub(crate) fn not(stack: &mut Stack) {
    let v = stack.get_mut(0);
    *v = !*v;
}
