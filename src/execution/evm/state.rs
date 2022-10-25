use super::common::InterpreterMessage;
use bytes::Bytes;
use core::{marker::PhantomData, mem, ptr, slice};
use ethnum::U256;
use getset::{Getters, MutGetters};
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
/// allocated memory usually will be much smaller.
///
/// Currently transaction can use only 30M gas. Memory grow cost
/// is computed as:
/// ```
/// memory_size_word = (memory_byte_size + 31) / 32
/// memory_cost = (memory_size_word ** 2) / 512 + (3 * memory_size_word)
/// ```
///
/// Thus max memory which can be allocated by one contract can be
/// computed as:
/// ```
/// memory_byte_size = 8192 * (sqrt(9 + available_gas / 128) - 3)
/// ```
/// Meaning that by using 30M gas one contract can allocate at most
/// ~3.94 MB of memory.
///
/// But contract may spawn subcontracts for which memory grow cost is
/// computed independently. By splitting gas equally between 1024
/// subcontracts (the maximum context depth) each subcontract could
/// allocate up to ~102 KB of memory, meaning that in total at the maximum
/// depth up to ~104.21 MB of memory could be allocated.
///
/// In addition to the memory, each contract can use up to 32 KiB of stack
/// space (1024 of 256-bit words), meaning that in total at the maximum
/// depth 32 MiB of stack space could be used.
///
/// We map 1 GiB of memory for EVM for two reasons:
/// 1) To be future-proof against potential future raise of maximum gas
///    per transaction.
/// 2) To allow use of 1 GiB huge pages.
const TOTAL_MEM_SIZE: usize = 1 << 30;

#[derive(Debug)]
pub struct EvmMemory {
    p: *mut libc::c_void,
}

/// Page sizes supported by [`EvmMemory`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PageSize {
    Page4KiB,
    Page2MiB,
    Page1GiB,
}

impl EvmMemory {
    #[inline(always)]
    pub fn new() -> Self {
        Self::new_with_size(PageSize::Page4KiB)
    }

    #[inline(always)]
    pub fn new_with_size(page_size: PageSize) -> Self {
        unsafe {
            let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
            flags |= match page_size {
                PageSize::Page4KiB => 0,
                PageSize::Page2MiB => libc::MAP_HUGETLB | libc::MAP_HUGE_2MB,
                PageSize::Page1GiB => libc::MAP_HUGETLB | libc::MAP_HUGE_1GB,
            };
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

    #[inline(always)]
    pub fn get_origin(&mut self) -> EvmSubMemory {
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
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            let res = libc::munmap(self.p.cast(), TOTAL_MEM_SIZE);
            if res != 0 {
                let err = io::Error::last_os_error();
                panic!("Failed to deallocate stack memory: {err}")
            }
        }
    }
}

impl Default for EvmMemory {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: `EvmMemory` owns the mapped memory (similarly to `Box`),
// so it's safe to implement `Send` and `Sync` for it
unsafe impl Send for EvmMemory {}
unsafe impl Sync for EvmMemory {}

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
    pub fn next_submem(&mut self) -> Self {
        let stack_ptr = self.stack_head;
        let heap_ptr = self.heap_head;
        Self {
            stack_head: stack_ptr,
            stack_base: stack_ptr,
            heap_head: heap_ptr,
            heap_base: heap_ptr,
            origin: self.origin,
        }
    }

    #[inline(always)]
    pub fn stack<'b>(&'b mut self) -> EvmStack<'a, 'b> {
        EvmStack { mem: self }
    }

    #[inline(always)]
    pub fn heap<'b>(&'b mut self) -> EvmHeap<'a, 'b> {
        EvmHeap { mem: self }
    }
}

pub struct EvmStack<'a, 'b> {
    mem: &'b mut EvmSubMemory<'a>,
}

impl<'a, 'b> EvmStack<'a, 'b> {
    #[inline(always)]
    pub fn get(&self, pos: usize) -> &U256 {
        debug_assert!(pos < self.len());
        unsafe { &*self.mem.stack_head.add(pos) }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, pos: usize) -> &mut U256 {
        debug_assert!(pos < self.len());
        unsafe { &mut *self.mem.stack_head.add(pos) }
    }

    /// Get size of stack in `U256`s.
    #[inline(always)]
    pub fn len(&self) -> usize {
        // TODO: use sub_ptr on stabilization
        unsafe { self.mem.stack_base.offset_from(self.mem.stack_head) as usize }
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
        unsafe { self.mem.heap_head.offset_from(self.mem.heap_base) as usize }
    }

    #[inline(always)]
    pub fn grow(&mut self, size: usize) {
        let old_size = self.size();
        if size > old_size {
            unsafe {
                ptr::write_bytes(self.mem.heap_head, 0, size - old_size);
                self.mem.heap_head = self.mem.heap_base.add(size);
            }
        }
    }
}

impl<'a, 'b> core::ops::Deref for EvmHeap<'a, 'b> {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.mem.heap_base, self.size()) }
    }
}

impl<'a, 'b> core::ops::DerefMut for EvmHeap<'a, 'b> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.mem.heap_base, self.size()) }
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
    pub fn stack<'b>(&'b mut self) -> EvmStack<'a, 'b> {
        self.mem.stack()
    }

    #[inline(always)]
    pub fn heap<'b>(&'b mut self) -> EvmHeap<'a, 'b> {
        self.mem.heap()
    }

    #[inline(always)]
    pub fn clone_stack_to_vec(&self) -> Vec<U256> {
        let len = unsafe { self.mem.stack_base.offset_from(self.mem.stack_head) as usize };
        unsafe { slice::from_raw_parts(self.mem.stack_head, len).to_vec() }
    }

    #[inline(always)]
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
