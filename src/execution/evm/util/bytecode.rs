use crate::execution::evm::opcode::*;
use core::iter::repeat;
use ethnum::{AsU256, U256};
use std::ops::{Add, Mul};

/// EVM bytecode builder.
#[derive(Clone, Debug, PartialEq)]
pub struct Bytecode {
    inner: Vec<u8>,
}

impl Bytecode {
    pub const fn new() -> Self {
        Self { inner: Vec::new() }
    }

    #[must_use]
    pub fn append(mut self, b: impl IntoIterator<Item = u8>) -> Self {
        self.inner.append(&mut b.into_iter().collect::<Vec<_>>());
        self
    }

    #[must_use]
    pub fn append_bc(mut self, b: impl Into<Self>) -> Self {
        self.inner.append(&mut b.into().build());
        self
    }

    #[must_use]
    pub fn repeat(mut self, n: usize) -> Self {
        self.inner = repeat(self.inner.into_iter()).take(n).flatten().collect();
        self
    }

    #[must_use]
    pub fn pushv(self, value: impl AsU256) -> Self {
        let value = value.as_u256();
        let b = value
            .to_be_bytes()
            .iter()
            .skip_while(|&&v| v == 0)
            .copied()
            .collect::<Vec<_>>();

        self.pushb(b)
    }

    #[must_use]
    pub fn pushb(mut self, b: impl IntoIterator<Item = u8>) -> Self {
        let mut b = b.into_iter().collect::<Vec<_>>();

        if b.is_empty() {
            b.push(0);
        }

        self.inner
            .extend_from_slice(&[(b.len() + OpCode::PUSH1.to_usize() - 1) as u8]);
        self.inner.append(&mut b);

        self
    }

    #[must_use]
    pub fn opcode(mut self, opcode: OpCode) -> Self {
        self.inner.push(opcode.to_u8());
        self
    }

    #[must_use]
    pub fn ret(mut self, index: impl AsU256, size: impl AsU256) -> Self {
        self = self.pushv(size);
        self = self.pushv(index);
        self = self.opcode(OpCode::RETURN);
        self
    }

    #[must_use]
    pub fn revert(mut self, index: impl AsU256, size: impl AsU256) -> Self {
        self = self.pushv(index);
        self = self.pushv(size);
        self = self.opcode(OpCode::REVERT);
        self
    }

    #[must_use]
    pub fn mstore(mut self, index: impl AsU256) -> Self {
        self = self.pushv(index);
        self = self.opcode(OpCode::MSTORE);
        self
    }

    #[must_use]
    pub fn mstore_value(mut self, index: impl AsU256, value: impl AsU256) -> Self {
        self = self.pushv(value);
        self = self.pushv(index);
        self = self.opcode(OpCode::MSTORE);
        self
    }

    #[must_use]
    pub fn mstore8(mut self, index: impl AsU256) -> Self {
        self = self.pushv(index);
        self = self.opcode(OpCode::MSTORE8);
        self
    }

    #[must_use]
    pub fn mstore8_value(mut self, index: impl AsU256, value: impl AsU256) -> Self {
        self = self.pushv(value);
        self = self.pushv(index);
        self = self.opcode(OpCode::MSTORE8);
        self
    }

    #[must_use]
    pub fn ret_top(self) -> Self {
        self.mstore(0).ret(0, 0x20)
    }

    #[must_use]
    pub fn jump(self, target: impl AsU256) -> Self {
        self.pushv(target).opcode(OpCode::JUMP)
    }

    #[must_use]
    pub fn jumpi(self, target: impl Into<Bytecode>, condition: impl Into<Bytecode>) -> Self {
        self.append(condition.into().build())
            .append(target.into().build())
            .opcode(OpCode::JUMPI)
    }

    #[must_use]
    pub fn sstore(self, index: impl AsU256, value: impl AsU256) -> Self {
        self.pushv(value).pushv(index).opcode(OpCode::SSTORE)
    }

    #[must_use]
    pub fn sload(self, index: impl AsU256) -> Self {
        self.pushv(index).opcode(OpCode::SLOAD)
    }

    pub fn build(self) -> Vec<u8> {
        self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<U256> for Bytecode {
    fn from(value: U256) -> Self {
        Self::new().pushv(value)
    }
}

impl From<OpCode> for Bytecode {
    fn from(opcode: OpCode) -> Self {
        Self::new().opcode(opcode)
    }
}

impl<const N: usize> From<[u8; N]> for Bytecode {
    fn from(inner: [u8; N]) -> Self {
        Self {
            inner: Vec::from(&inner as &[u8]),
        }
    }
}

impl From<Vec<u8>> for Bytecode {
    fn from(inner: Vec<u8>) -> Self {
        Self { inner }
    }
}

impl AsRef<[u8]> for Bytecode {
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl IntoIterator for Bytecode {
    type Item = u8;
    type IntoIter = <Vec<u8> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl Mul<Bytecode> for usize {
    type Output = Bytecode;

    fn mul(self, rhs: Bytecode) -> Self::Output {
        repeat(rhs)
            .take(self)
            .fold(Bytecode::new(), |acc, b| acc.append_bc(b))
    }
}

impl Mul<OpCode> for usize {
    type Output = Bytecode;

    fn mul(self, rhs: OpCode) -> Self::Output {
        self.mul(Bytecode::from(rhs))
    }
}

impl<T: Into<Bytecode>> Add<T> for Bytecode {
    type Output = Bytecode;

    fn add(self, rhs: T) -> Self::Output {
        self.append_bc(rhs)
    }
}

pub struct CallInstruction {
    op: OpCode,
    address: U256,
    gas: U256,
    value: U256,
    input: U256,
    input_size: U256,
    output: U256,
    output_size: U256,
}

impl CallInstruction {
    fn new(op: OpCode, address: impl AsU256) -> Self {
        Self {
            op,
            address: address.as_u256(),
            gas: 0_u128.into(),
            value: 0_u128.into(),
            input: 0_u128.into(),
            input_size: 0_u128.into(),
            output: 0_u128.into(),
            output_size: 0_u128.into(),
        }
    }

    pub fn delegatecall(address: impl AsU256) -> Self {
        Self::new(OpCode::DELEGATECALL, address)
    }

    pub fn staticcall(address: impl AsU256) -> Self {
        Self::new(OpCode::STATICCALL, address)
    }

    pub fn call(address: impl AsU256) -> Self {
        Self::new(OpCode::CALL, address)
    }

    pub fn callcode(address: impl AsU256) -> Self {
        Self::new(OpCode::CALLCODE, address)
    }

    pub fn opcode(&self) -> OpCode {
        self.op
    }

    #[must_use]
    pub fn gas(mut self, gas: u128) -> Self {
        self.gas = gas.into();
        self
    }

    #[must_use]
    pub fn value(mut self, value: impl AsU256) -> Self {
        self.value = value.as_u256();
        self
    }

    #[must_use]
    pub fn input(mut self, index: u128, size: u128) -> Self {
        self.input = index.into();
        self.input_size = size.into();
        self
    }

    #[must_use]
    pub fn output(mut self, index: u128, size: u128) -> Self {
        self.output = index.into();
        self.output_size = size.into();
        self
    }
}

impl From<CallInstruction> for Bytecode {
    fn from(call: CallInstruction) -> Self {
        let mut b = Bytecode::new()
            .pushv(call.output_size)
            .pushv(call.output)
            .pushv(call.input_size)
            .pushv(call.input);
        if call.op == OpCode::CALL || call.op == OpCode::CALLCODE {
            b = b.pushv(call.value);
        }
        b.pushv(call.address).pushv(call.gas).opcode(call.op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiply_bytecode() {
        assert_eq!(
            3 * Bytecode::new().opcode(OpCode::POP),
            Bytecode::new()
                .opcode(OpCode::POP)
                .opcode(OpCode::POP)
                .opcode(OpCode::POP)
        )
    }
}
