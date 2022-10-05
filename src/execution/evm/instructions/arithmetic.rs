use crate::{
    execution::evm::{
        state::{EvmStack, ExecutionState},
        StatusCode,
    },
    models::*,
};
use ethereum_types::U512;
use ethnum::U256;
use i256::{i256_div, i256_mod};

#[inline]
pub(crate) fn add(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.pop();
    stack.push(a.overflowing_add(b).0);
}

#[inline]
pub(crate) fn mul(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.pop();
    stack.push(a.overflowing_mul(b).0);
}

#[inline]
pub(crate) fn sub(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.pop();
    stack.push(a.overflowing_sub(b).0);
}

#[inline]
pub(crate) fn div(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = if *b == 0 { U256::ZERO } else { a / *b };
}

#[inline]
pub(crate) fn sdiv(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.pop();
    let v = i256_div(a, b);
    stack.push(v);
}

#[inline]
pub(crate) fn modulo(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.get_mut(0);
    *b = if *b == 0 { U256::ZERO } else { a % *b };
}

#[inline]
pub(crate) fn smod(stack: &mut EvmStack) {
    let a = stack.pop();
    let b = stack.get_mut(0);

    if *b == 0 {
        *b = U256::ZERO
    } else {
        *b = i256_mod(a, *b);
    };
}

#[inline]
pub(crate) fn addmod(stack: &mut EvmStack) {
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

#[inline]
pub(crate) fn mulmod(stack: &mut EvmStack) {
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

#[inline]
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

#[inline]
pub(crate) fn exp<const REVISION: Revision>(state: &mut ExecutionState) -> Result<(), StatusCode> {
    let mut base = state.stack().pop();
    let mut power = state.stack().pop();

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

    state.stack().push(v);

    Ok(())
}

#[inline]
pub(crate) fn signextend(stack: &mut EvmStack) {
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
