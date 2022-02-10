use crate::{
    execution::evm::{state::*, StatusCode},
    models::*,
};
use ethereum_types::U512;
use ethnum::U256;
use i256::I256;

#[inline(always)]
pub(crate) fn add(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.pop();
    stack.push(a.overflowing_add(b).0);
}

#[inline(always)]
pub(crate) fn mul(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.pop();
    stack.push(a.overflowing_mul(b).0);
}

#[inline(always)]
pub(crate) fn sub(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.pop();
    stack.push(a.overflowing_sub(b).0);
}

#[inline(always)]
pub(crate) fn div(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = if *b == 0 { U256::ZERO } else { a / *b };
}

#[inline(always)]
pub(crate) fn sdiv(stack: &mut Stack) {
    let a = I256::from(stack.pop());
    let b = I256::from(stack.pop());
    let v = a / b;
    stack.push(v.into());
}

#[inline(always)]
pub(crate) fn modulo(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = if *b == 0 { U256::ZERO } else { a % *b };
}

#[inline(always)]
pub(crate) fn smod(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    if *b == 0 {
        *b = U256::ZERO
    } else {
        let v = I256::from(a) % I256::from(*b);
        *b = v.into();
    };
}

pub(crate) fn addmod(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.pop();
    let c = stack.pop();

    let v = if c == 0 {
        U256::ZERO
    } else {
        let v = ethereum_types::U256::try_from(
            (U512::from_big_endian(&a.to_be_bytes()) + U512::from_big_endian(&b.to_be_bytes()))
                % U512::from_big_endian(&c.to_be_bytes()),
        )
        .unwrap();
        let mut arr = [0; 32];
        v.to_big_endian(&mut arr);
        U256::from_be_bytes(arr)
    };

    stack.push(v);
}

pub(crate) fn mulmod(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.pop();
    let c = stack.pop();

    let v = if c == 0 {
        U256::ZERO
    } else {
        let v = ethereum_types::U256::try_from(
            (U512::from_big_endian(&a.to_be_bytes()) * U512::from_big_endian(&b.to_be_bytes()))
                % U512::from_big_endian(&c.to_be_bytes()),
        )
        .unwrap();
        let mut arr = [0; 32];
        v.to_big_endian(&mut arr);
        U256::from_be_bytes(arr)
    };

    stack.push(v);
}

fn log2floor(value: U256) -> u64 {
    debug_assert!(value != 0);
    let mut l: u64 = 256;
    for v in [value.high(), value.low()] {
        if *v == 0 {
            l -= 128;
        } else {
            l -= v.leading_zeros() as u64;
            if l == 0 {
                return l;
            } else {
                return l - 1;
            }
        }
    }
    l
}

pub(crate) fn exp<const REVISION: Revision>(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let mut base = state.stack.pop();
    let mut power = state.stack.pop();

    if power > 0 {
        let factor = if REVISION >= Revision::Spurious {
            50
        } else {
            10
        };
        let additional_gas = factor * (log2floor(power) / 8 + 1);

        state.gas_left -= additional_gas as i64;

        if state.gas_left < 0 {
            return Err(StatusCode::OutOfGas);
        }
    }

    let mut v = U256::ONE;

    while power > 0 {
        if (power & 1) != 0 {
            v = v.overflowing_mul(base).0;
        }
        power >>= 1;
        base = base.overflowing_mul(base).0;
    }

    state.stack.push(v);

    Ok(())
}

pub(crate) fn signextend(stack: &mut Stack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    if a < 32 {
        let bit_index = (8 * a.as_u8() + 7) as u16;
        let (hi, lo) = b.into_words();
        let bit = if bit_index > 0x7f { hi } else { lo } & (1 << (bit_index % 128)) != 0;
        let mask = (U256::ONE << bit_index) - U256::ONE;
        *b = if bit { *b | !mask } else { *b & mask }
    }
}
