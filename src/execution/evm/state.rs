use super::common::InterpreterMessage;
use arrayvec::ArrayVec;
use bytes::{Bytes, BytesMut};
use derive_more::{Deref, DerefMut};
use ethnum::U256;
use getset::{Getters, MutGetters};
use serde::Serialize;

pub const STACK_SIZE: usize = 1024;

/// EVM stack.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Stack(pub ArrayVec<U256, STACK_SIZE>);

impl Stack {
    #[inline]
    pub const fn new() -> Self {
        Self(ArrayVec::new_const())
    }

    #[inline]
    const fn get_pos(&self, pos: usize) -> usize {
        self.len() - 1 - pos
    }

    #[inline]
    pub fn get(&self, pos: usize) -> &U256 {
        &self.0[self.get_pos(pos)]
    }

    #[inline]
    pub fn get_mut(&mut self, pos: usize) -> &mut U256 {
        let pos = self.get_pos(pos);
        &mut self.0[pos]
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn push(&mut self, v: U256) {
        unsafe { self.0.push_unchecked(v) }
    }

    #[inline]
    pub fn pop(&mut self) -> U256 {
        unsafe { self.0.pop_unchecked() }
    }

    #[inline]
    pub fn swap_top(&mut self, pos: usize) {
        let top = self.0.len() - 1;
        let pos = self.get_pos(pos);
        self.0.swap(top, pos);
    }
}

const PAGE_SIZE: usize = 4 * 1024;

#[derive(Clone, Debug, Deref, DerefMut)]
pub struct Memory(BytesMut);

impl Memory {
    #[inline]
    pub fn new() -> Self {
        Self(BytesMut::with_capacity(PAGE_SIZE))
    }

    #[inline]
    pub fn grow(&mut self, size: usize) {
        let cap = self.0.capacity();
        if size > cap {
            let required_pages = (size + PAGE_SIZE - 1) / PAGE_SIZE;
            self.0.reserve((PAGE_SIZE * required_pages) - self.0.len());
        }
        self.0.resize(size, 0);
    }
}

impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

/// EVM execution state.
#[derive(Clone, Debug, Getters, MutGetters)]
pub struct ExecutionState<'m> {
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) gas_left: i64,
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) stack: Stack,
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) memory: Memory,
    pub(crate) message: &'m InterpreterMessage,
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) return_data: Bytes,
    pub(crate) output_data: Bytes,
}

impl<'m> ExecutionState<'m> {
    pub fn new(message: &'m InterpreterMessage) -> Self {
        Self {
            gas_left: message.gas,
            stack: Stack::default(),
            memory: Memory::new(),
            message,
            return_data: Default::default(),
            output_data: Bytes::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack() {
        let mut stack = Stack::default();

        let items: [u128; 4] = [0xde, 0xad, 0xbe, 0xef];

        for (i, item) in items.iter().copied().enumerate() {
            stack.push(item.into());
            assert_eq!(stack.len(), i + 1);
        }

        assert_eq!(*stack.get(2), 0xad);

        assert_eq!(stack.pop(), 0xef);

        assert_eq!(*stack.get(2), 0xde);
    }

    #[test]
    fn grow() {
        let mut mem = Memory::new();
        mem.grow(PAGE_SIZE * 2 + 1);
        assert_eq!(mem.len(), PAGE_SIZE * 2 + 1);
        assert_eq!(mem.capacity(), PAGE_SIZE * 3);
    }
}
