use crate::execution::evm::state::Stack;
use ethnum::U256;
use i256::{i256_sign, two_compl, Sign};

#[inline]
pub(crate) fn byte(stack: &mut Stack) {
    let mut i = *stack.pop().low();
    let x = stack.get_mut(0);

    let x_word = if i >= 16 {
        i -= 16;
        x.low()
    } else {
        x.high()
    };

    *x = U256::from((x_word >> (120 - i * 8)) & 0xFF);
}

#[inline]
pub(crate) fn shl(stack: &mut Stack) {
    let shift = stack.pop();
    let value = stack.get_mut(0);

    if *value == 0 || shift >= 256 {
        *value = U256::ZERO;
    } else {
        *value <<= shift.as_u8()
    };
}

#[inline]
pub(crate) fn shr(stack: &mut Stack) {
    let shift = stack.pop();
    let value = stack.get_mut(0);

    if *value == 0 || shift >= 256 {
        *value = U256::ZERO
    } else {
        *value >>= shift.as_u8()
    };
}

#[inline]
pub(crate) fn sar(stack: &mut Stack) {
    let shift = stack.pop();
    let mut value = stack.pop();

    let value_sign = i256_sign::<true>(&mut value);

    stack.push(if value == U256::ZERO || shift >= 256 {
        match value_sign {
            // value is 0 or >=1, pushing 0
            Sign::Plus | Sign::Zero => U256::ZERO,
            // value is <0, pushing -1
            Sign::Minus => two_compl(U256::ONE),
        }
    } else {
        let shift = shift.as_u128();

        match value_sign {
            Sign::Plus | Sign::Zero => value >> shift,
            Sign::Minus => {
                let shifted = ((value.overflowing_sub(U256::ONE).0) >> shift)
                    .overflowing_add(U256::ONE)
                    .0;
                two_compl(shifted)
            }
        }
    });
}
