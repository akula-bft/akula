use super::common::InterpreterMessage;
use bumpalo::{collections::Vec, Bump};
use bytes::Bytes;
use ethnum::U256;
use getset::{Getters, MutGetters};
use std::hint::unreachable_unchecked;

/// Size of EVM stack in U256s
pub const STACK_SIZE: usize = 1024;
pub(crate) const MAX_CONTEXT_DEPTH: usize = 1024;

#[derive(Debug, Default)]
pub struct EvmMemory {
    bump: Bump,
}

impl EvmMemory {
    #[inline(always)]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn get_origin(&mut self) -> EvmSubMemory {
        EvmSubMemory::new_in(&self.bump)
    }
}

/// Note that stack grows down, while heap grows up, i.e. the following
/// conditions MUST be always true:
/// - `stack_base` >= `stack_head`
/// - `heap_base` <= `heap_head`
// WARNING: DO NOT CHANGE order of `heap` and `stack`. We need for `heap`
// to be dropped before `stack`, otherwise bumpalo will not be able
// to release memory. Drop order of fields is specified by RFC 1857.
pub struct EvmSubMemory<'a> {
    heap: Vec<'a, u8>,
    stack: Vec<'a, U256>,
    bump: &'a Bump,
}

impl<'a> EvmSubMemory<'a> {
    #[inline(always)]
    fn new_in(bump: &'a Bump) -> Self {
        // WARNING: DO NOT CHANGE order of `heap` and `stack`
        let stack = Vec::with_capacity_in(STACK_SIZE, bump);
        let heap = Vec::new_in(bump);
        EvmSubMemory { stack, heap, bump }
    }

    #[inline(always)]
    pub fn next_submem(&mut self) -> Self {
        Self::new_in(&self.bump)
    }

    #[inline(always)]
    pub fn stack<'b>(&'b mut self) -> EvmStack<'b, 'a> {
        EvmStack(&mut self.stack)
    }

    #[inline(always)]
    pub fn heap<'b>(&'b mut self) -> EvmHeap<'b, 'a> {
        EvmHeap(&mut self.heap)
    }
}

pub struct EvmStack<'a, 'b>(&'a mut Vec<'b, U256>);

impl<'a, 'b> EvmStack<'a, 'b> {
    #[inline(always)]
    fn get_pos(&self, pos: usize) -> usize {
        self.len() - 1 - pos
    }

    #[inline(always)]
    pub fn get(&self, pos: usize) -> &U256 {
        let pos = self.get_pos(pos);
        unsafe { self.0.get_unchecked(pos) }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, pos: usize) -> &mut U256 {
        let pos = self.get_pos(pos);
        unsafe { self.0.get_unchecked_mut(pos) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn push(&mut self, v: U256) {
        // Prevent branch which resizes vector
        if self.0.len() == self.0.capacity() {
            debug_assert!(false);
            unsafe { unreachable_unchecked() }
        }
        self.0.push(v)
    }

    #[inline]
    pub fn pop(&mut self) -> U256 {
        self.0.pop().unwrap_or_else(|| unsafe {
            debug_assert!(false);
            unreachable_unchecked()
        })
    }

    #[inline]
    pub fn swap_top(&mut self, pos: usize) {
        let top = self.0.len() - 1;
        let pos = self.get_pos(pos);
        unsafe {
            self.0.swap_unchecked(top, pos);
        }
    }
}

pub struct EvmHeap<'a, 'b>(&'a mut Vec<'b, u8>);

impl<'a, 'b> EvmHeap<'a, 'b> {
    /// Get size of stack in `u8`s.
    #[inline(always)]
    fn size(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn grow(&mut self, size: usize) {
        self.0.resize(size, 0)
    }
}

impl<'a, 'b> core::ops::Deref for EvmHeap<'a, 'b> {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl<'a, 'b> core::ops::DerefMut for EvmHeap<'a, 'b> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

/// EVM execution state.
// #[derive(Clone, Debug, Getters, MutGetters)]
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
    #[inline(always)]
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
    pub fn clone_stack_to_vec(&self) -> std::vec::Vec<U256> {
        self.mem.stack.to_vec()
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
