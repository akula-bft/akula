use crate::execution::evm::state::Stack;
use ethnum::U256;
use i256::{Sign, I256};

pub(crate) fn byte(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.pop();

    let mut ret = U256::ZERO;

    for i in 0..=255 {
        if i < 8 && a < 32 {
            let o = a.as_u8();
            let t = 255 - (7 - i + 8 * o);
            let bit_mask = U256::ONE << t;
            let value = (b & bit_mask) >> t;
            ret = ret.overflowing_add(value << i).0;
        }
    }

    stack.push(ret)
}

#[inline(always)]
pub(crate) fn shl(stack: &mut Stack) {
    let shift = stack.pop();
    let value = stack.get_mut(0);

    if *value == 0 || shift >= 256 {
        *value = U256::ZERO;
    } else {
        *value <<= shift.as_u8()
    };
}

#[inline(always)]
pub(crate) fn shr(stack: &mut Stack) {
    let shift = stack.pop();
    let value = stack.get_mut(0);

    if *value == 0 || shift >= 256 {
        *value = U256::ZERO
    } else {
        *value >>= shift.as_u8()
    };
}

pub(crate) fn sar(stack: &mut Stack) {
    let shift = stack.pop();
    let value = I256::from(stack.pop());

    let ret = if value == I256::zero() || shift >= 256 {
        match value.0 {
            // value is 0 or >=1, pushing 0
            Sign::Plus | Sign::NoSign => U256::ZERO,
            // value is <0, pushing -1
            Sign::Minus => I256(Sign::Minus, U256::ONE).into(),
        }
    } else {
        let shift = shift.as_u8();

        match value.0 {
            Sign::Plus | Sign::NoSign => value.1 >> shift,
            Sign::Minus => {
                let shifted = ((value.1.overflowing_sub(U256::ONE).0) >> shift)
                    .overflowing_add(U256::ONE)
                    .0;
                I256(Sign::Minus, shifted).into()
            }
        }
    };

    stack.push(ret)
}
