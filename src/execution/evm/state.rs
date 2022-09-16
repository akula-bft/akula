use super::common::InterpreterMessage;
use bytes::{Bytes, BytesMut};
use derive_more::{Deref, DerefMut};
use ethnum::U256;
use getset::{Getters, MutGetters};
use core::{ptr, mem};
use std::io;

/// Size of EVM stack in U256s
pub const STACK_SIZE: usize = 1024;
/// Size of EVM stack in bytes
const STACK_SIZE_BYTES: usize = mem::size_of::<U256>() * STACK_SIZE;

/// EVM stack.
//#[derive(Clone, Debug, Default, Serialize)]
#[derive(Debug)]
pub struct Stack {
    pos: usize,
    /// Pointer to memory of [U256; STACK_SIZE]
    p: *mut U256,
}

impl Stack {
    #[inline]
    pub fn new() -> Self {
        unsafe {
            // TODO: add MAP_HUGETLB for huge tables
            let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
            let mmap_res = libc::mmap(
                ptr::null_mut(),
                STACK_SIZE_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                -1,
                0,
            );
            if mmap_res == libc::MAP_FAILED {
                let err = io::Error::last_os_error();
                panic!("Failed to allocate memory for EVM stack: {err}");
            }
            Self {
                pos: 0,
                p: mmap_res.cast(),
            }
        }
    }

    #[inline(always)]
    pub fn get(&self, pos: usize) -> &U256 {
        assert!(pos < STACK_SIZE);
        unsafe { &*self.p.add(pos) }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, pos: usize) -> &mut U256 {
        assert!(pos < STACK_SIZE);
        unsafe { &mut *self.p.add(pos) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.pos
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        // println!("is_empty: {}", self.len() == 0);
        self.len() == 0
    }

    #[inline(always)]
    pub fn push(&mut self, v: U256) {
        // println!("push {}", self.pos);
        assert!(self.pos < STACK_SIZE);
        unsafe {
            let p = self.p.add(self.pos);
            ptr::write(p, v);
            self.pos += 1;
        }
    }

    #[inline(always)]
    pub fn pop(&mut self) -> U256 {
        // println!("pop {}", self.pos);
        assert_ne!(self.pos, 0);
        unsafe {
            self.pos -= 1;
            let p = self.p.add(self.pos);
            ptr::read(p)
        }
    }

    #[inline(always)]
    pub fn swap_top(&mut self, pos: usize) {
        // println!("swap {} {pos}", self.pos);
        assert!(pos < STACK_SIZE);
        assert!(self.pos - 1 < STACK_SIZE);
        unsafe {
            let p1 = self.p.add(self.pos - 1);
            let p2 = self.p.add(pos);
            ptr::swap_nonoverlapping(p1, p2, 1);
        }
    }

    #[inline(always)]
    pub fn clone_to_vec(&self) -> Vec<U256> {
        unsafe {
            core::slice::from_raw_parts(self.p, self.pos).to_vec()
        }
    }
}

impl Default for Stack {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        unsafe {
            let res = libc::munmap(self.p.cast(), STACK_SIZE_BYTES);
            if res != 0 {
                let err = io::Error::last_os_error();
                panic!("Failed to deallocate stack memory: {err}")
            }
        }
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
// #[derive(Clone, Debug, Getters, MutGetters)]
#[derive(Debug, Getters, MutGetters)]
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
            stack: Stack::new(),
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
        let mut stack = Stack::new();

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
