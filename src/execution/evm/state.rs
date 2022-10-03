use super::{common::InterpreterMessage};
use bytes::Bytes;
use ethnum::U256;
use getset::{Getters, MutGetters};
use core::{ptr, mem, slice, marker::PhantomData};
use std::io;

/// Size of EVM stack in U256s
pub const STACK_SIZE: usize = 1024;
/// Size of EVM stack in bytes
const STACK_SIZE_BYTES: usize = mem::size_of::<U256>() * STACK_SIZE;

pub(crate) const MAX_CONTEXT_DEPTH: usize = 1024;

const SUPER_STACK_SIZE: usize = STACK_SIZE * MAX_CONTEXT_DEPTH;
const SUPER_STACK_SIZE_BYTES: usize = mem::size_of::<U256>() * SUPER_STACK_SIZE;

/// Total memory size allocated for EVM.
///
/// Note that allocated pages get populated lazily, i.e. physically
/// allocated memory will be smaller.
const TOTAL_MEM_SIZE: usize = 8 * (1 << 30);

pub struct EvmMemory {
    p: *mut libc::c_void,
}

impl EvmMemory {
    #[inline]
    pub fn new() -> Self {
        unsafe {
            // TODO: add MAP_HUGETLB for huge tables
            let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
            let mmap_res = libc::mmap(
                ptr::null_mut(),
                TOTAL_MEM_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                -1,
                0,
            );
            if mmap_res == libc::MAP_FAILED {
                let err = io::Error::last_os_error();
                panic!("Failed to allocate memory for EVM stack: {err}");
            }
            Self { p: mmap_res }
        }
    }

    pub fn get_origin<'b>(&'b mut self) -> EvmSubMemory<'b> {
        let p = unsafe { self.p.add(SUPER_STACK_SIZE_BYTES).cast() };
        EvmSubMemory {
            stack_head: p,
            stack_base: p,
            heap_head: p.cast(),
            heap_base: p.cast(),
            origin: PhantomData,
        }
    }
}

impl Drop for EvmMemory {
    fn drop(&mut self) {
        println!("Super stack drop: {:?}", self.p);
        unsafe {
            let res = libc::munmap(self.p.cast(), TOTAL_MEM_SIZE);
            if res != 0 {
                let err = io::Error::last_os_error();
                panic!("Failed to deallocate stack memory: {err}")
            }
        }
    }
}

/// Note that stack grows down, while heap grows up, i.e. the following
/// conditions MUST be always true:
/// - `stack_base` >= `stack_head`
/// - `heap_base` <= `heap_head`
pub struct EvmSubMemory<'a> {
    stack_head: *mut U256,
    stack_base: *mut U256,
    heap_head: *mut u8,
    heap_base: *mut u8,
    origin: PhantomData<&'a mut ()>,  
}

impl<'a> EvmSubMemory<'a> {
    #[inline(always)]
    pub fn next_submem<'b>(&'b mut self) -> EvmSubMemory<'b> {
        let stack_ptr = self.stack_head;
        let heap_ptr = self.heap_head;
        println!("next submem: {stack_ptr:?} {heap_ptr:?}");
        Self {
            stack_head: stack_ptr,
            stack_base: stack_ptr,
            heap_head: heap_ptr,
            heap_base: heap_ptr,
            origin: self.origin,
        }
    }

    pub fn stack<'b>(&'b mut self) -> EvmStack<'a, 'b> {
        EvmStack {
            mem: self,
        }
    }

    pub fn heap<'b>(&'b mut self) -> EvmHeap<'a, 'b> {
        EvmHeap {
            mem: self,
        }
    }
}

pub struct EvmStack<'a, 'b> {
    mem: &'b mut EvmSubMemory<'a>,
}

impl<'a, 'b> EvmStack<'a, 'b> {
    #[inline(always)]
    pub fn get(&self, pos: usize) -> &U256 {
        println!("Get: {pos:?}", );
        debug_assert!(pos < self.len());
        unsafe { &*self.mem.stack_head.add(pos) }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, pos: usize) -> &mut U256 {
        println!("Get mut: {pos:?}", );
        debug_assert!(pos < self.len());
        unsafe { &mut *self.mem.stack_head.add(pos) }
    }

    /// Get size of stack in `U256`s.
    #[inline(always)]
    pub fn len(&self) -> usize {
        // TODO: use sub_ptr on stabilization
        let len = unsafe { self.mem.stack_base.offset_from(self.mem.stack_head) as usize };
        println!("Len: {len:?}");
        len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn push(&mut self, v: U256) {
        debug_assert!(self.len() < STACK_SIZE);
        let head = &mut self.mem.stack_head;
        unsafe {
            *head = head.sub(1);
            ptr::write(*head, v);
        }
    }

    #[inline(always)]
    pub fn pop(&mut self) -> U256 {
        println!("pop {:?}", self.len());
        debug_assert_ne!(self.len(), 0);
        let head = &mut self.mem.stack_head;
        unsafe {
            let val = ptr::read(*head);
            *head = head.add(1);
            val
        }
    }

    #[inline(always)]
    pub fn swap_top(&mut self, pos: usize) {
        println!("swap_top");
        debug_assert_ne!(pos, 0);
        debug_assert!(pos < self.len());
        let head = self.mem.stack_head;
        unsafe {
            ptr::swap_nonoverlapping(head, head.add(pos), 1);
        }
    }
}

const PAGE_SIZE: usize = 4 * 1024;

pub struct EvmHeap<'a, 'b> {
    mem: &'b mut EvmSubMemory<'a>, 
}

impl<'a, 'b> EvmHeap<'a, 'b> {
    /// Get size of stack in `u8`s.
    #[inline(always)]
    fn size(&self) -> usize {
        // TODO: use sub_ptr on stabilization
        let len = unsafe { self.mem.heap_head.offset_from(self.mem.heap_base) as usize };
        println!("Heap len: {len:?}");
        len
    }

    #[inline]
    pub fn grow(&mut self, size: usize) {
        let head = &mut self.mem.heap_head;
        let new_head = unsafe { self.mem.heap_base.add(size) };
        if new_head > *head {
            *head = new_head;
        }
    }
}

impl<'a, 'b> core::ops::Deref for EvmHeap<'a, 'b> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.mem.heap_base, self.size())
        }
    }
}

impl<'a, 'b> core::ops::DerefMut for EvmHeap<'a, 'b> {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.mem.heap_base, self.size())
        }
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
    pub fn new(message: &'a InterpreterMessage, mem: EvmSubMemory<'a>) -> Self {
        Self {
            gas_left: message.gas,
            mem,
            message,
            return_data: Default::default(),
            output_data: Bytes::new(),
        }
    }

    pub fn stack<'b>(&'b mut self) -> EvmStack<'a, 'b> {
        self.mem.stack()
    }

    pub fn heap<'b>(&'b mut self) -> EvmHeap<'a, 'b> {
        self.mem.heap()
    }

    pub fn clone_stack_to_vec(&self) -> Vec<U256> {
        let len = unsafe { self.mem.stack_base.offset_from(self.mem.stack_head) as usize };
        unsafe {
            slice::from_raw_parts(self.mem.stack_head, len).to_vec()
        }
    }

    pub fn heap_size(&self) -> usize {
        unsafe { self.mem.heap_head.offset_from(self.mem.heap_base) as usize }
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
