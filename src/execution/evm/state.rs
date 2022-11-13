use super::common::InterpreterMessage;
use bytes::Bytes;
use ethnum::U256;
use getset::{Getters, MutGetters};
use std::{
    hint::unreachable_unchecked,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

pub const STACK_SIZE: usize = 1024;
pub const MAX_CONTEXT_DEPTH: usize = 1024;
const PAGE_SIZE: usize = 4 * 1024;

#[derive(Debug)]
pub struct EvmMemory {}

impl EvmMemory {
    #[inline(always)]
    pub fn new() -> Self {
        Self {}
    }

    #[inline(always)]
    pub fn get_origin(&mut self) -> EvmSubMemory {
        EvmSubMemory {
            heap: Vec::with_capacity(PAGE_SIZE),
            stack: Vec::with_capacity(STACK_SIZE),
            origin: PhantomData,
        }
    }
}

impl Default for EvmMemory {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

pub struct EvmSubMemory<'a> {
    heap: Vec<u8>,
    stack: Vec<U256>,
    origin: PhantomData<&'a mut ()>,
}

impl<'a> EvmSubMemory<'a> {
    #[inline(always)]
    pub fn next_submem(&mut self) -> Self {
        Self {
            heap: Vec::with_capacity(PAGE_SIZE),
            stack: Vec::with_capacity(STACK_SIZE),
            origin: self.origin,
        }
    }

    #[inline(always)]
    pub fn stack<'b>(&'b mut self) -> EvmStack<'b, 'a> {
        EvmStack {
            stack: &mut self.stack,
            origin: self.origin,
        }
    }

    #[inline(always)]
    pub fn heap<'b>(&'b mut self) -> EvmHeap<'b, 'a> {
        EvmHeap {
            heap: &mut self.heap,
            origin: self.origin,
        }
    }
}

/// EVM stack.
pub struct EvmStack<'a, 'b> {
    stack: &'a mut Vec<U256>,
    origin: PhantomData<&'b mut ()>,
}

impl<'a, 'b> EvmStack<'a, 'b> {
    #[inline]
    fn get_pos(&self, pos: usize) -> usize {
        self.len() - 1 - pos
    }

    #[inline]
    pub fn get(&self, pos: usize) -> &U256 {
        let pos = self.get_pos(pos);
        unsafe { self.stack.get_unchecked(pos) }
    }

    #[inline]
    pub fn get_mut(&mut self, pos: usize) -> &mut U256 {
        let pos = self.get_pos(pos);
        unsafe { self.stack.get_unchecked_mut(pos) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.stack.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn push(&mut self, v: U256) {
        // Prevent branch which resizes vector
        if self.stack.len() == self.stack.capacity() {
            debug_assert!(false);
            unsafe { unreachable_unchecked() }
        }
        self.stack.push(v)
    }

    #[inline]
    pub fn pop(&mut self) -> U256 {
        self.stack.pop().unwrap_or_else(|| unsafe {
            debug_assert!(false);
            unreachable_unchecked()
        })
    }

    #[inline]
    pub fn swap_top(&mut self, pos: usize) {
        let top = self.stack.len() - 1;
        let pos = self.get_pos(pos);
        unsafe {
            self.stack.swap_unchecked(top, pos);
        }
    }
}

pub struct EvmHeap<'a, 'b> {
    heap: &'a mut Vec<u8>,
    origin: PhantomData<&'b mut ()>,
}

impl<'a, 'b> EvmHeap<'a, 'b> {
    /// Get size of stack in `u8`s.
    #[inline(always)]
    fn size(&self) -> usize {
        self.heap.len()
    }

    #[inline(always)]
    pub fn grow(&mut self, size: usize) {
        self.heap.resize(size, 0)
    }
}

impl<'a, 'b> Deref for EvmHeap<'a, 'b> {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.heap
    }
}

impl<'a, 'b> DerefMut for EvmHeap<'a, 'b> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.heap
    }
}

/// EVM execution state.
#[derive(Getters, MutGetters)]
pub struct ExecutionState<'a> {
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) gas_left: i64,
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) mem: EvmSubMemory<'a>,
    pub(crate) message: &'a InterpreterMessage,
    #[getset(get = "pub", get_mut = "pub")]
    pub(crate) return_data: Bytes,
    pub(crate) output_data: Bytes,
}

impl<'a> ExecutionState<'a> {
    pub fn new(message: &'a InterpreterMessage, mem: EvmSubMemory<'a>) -> Self {
        Self {
            gas_left: message.gas,
            mem,
            message,
            return_data: Default::default(),
            output_data: Bytes::new(),
        }
    }

    #[inline(always)]
    pub fn stack<'b>(&'b mut self) -> EvmStack<'b, 'a> {
        self.mem.stack()
    }

    #[inline(always)]
    pub fn heap<'b>(&'b mut self) -> EvmHeap<'b, 'a> {
        self.mem.heap()
    }

    #[inline(always)]
    pub fn clone_stack_to_vec(&self) -> Vec<U256> {
        self.mem.stack.clone()
    }

    #[inline(always)]
    pub fn heap_size(&self) -> usize {
        self.mem.heap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack() {
        let mut evm_mem = EvmMemory::new();
        let mut mem = evm_mem.get_origin();
        let mut stack = mem.stack();

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
        const LEN: usize = 10_000;
        let mut evm_mem = EvmMemory::new();
        let mut mem = evm_mem.get_origin();
        let mut heap = mem.heap();
        heap.grow(LEN);
        for val in heap.iter_mut() {
            *val = 42;
        }
        assert!(heap.iter().all(|val| *val == 42));
        assert_eq!(heap.len(), LEN);
    }
}
