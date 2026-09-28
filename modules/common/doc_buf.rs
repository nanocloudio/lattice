//! A buffer that holds a whole document, request or answer, shared by the
//! document modules that come in two builds: inline in a module's state
//! in the bounded build; one allocation from the module's heap arena,
//! taken at construction, in the `big` build (the `*_big` twins, whose
//! documents are past what a state holds). Either way it is a `[u8]` of
//! `N` bytes, so the code that uses it is one.
//!
//! Mounted beside the module's `abi` (reached as `super::abi`). A module
//! whose `big` build uses it declares its arena
//! (`declare_module_arena_bytes!`) as the sum of its buffers.

#[cfg(feature = "big")]
use super::abi::SyscallTable;

#[cfg(not(feature = "big"))]
#[repr(transparent)]
pub struct DocBuf<const N: usize>([u8; N]);

#[cfg(not(feature = "big"))]
impl<const N: usize> core::ops::Deref for DocBuf<N> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(not(feature = "big"))]
impl<const N: usize> core::ops::DerefMut for DocBuf<N> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

/// The big build's: `N` bytes of the module's heap, taken once at
/// construction ([`DocBuf::alloc`]); empty until then.
#[cfg(feature = "big")]
pub struct DocBuf<const N: usize>(*mut u8);

#[cfg(feature = "big")]
impl<const N: usize> DocBuf<N> {
    /// Take the buffer from the heap: `false` when the arena cannot.
    ///
    /// # Safety
    /// `sys` is the module's syscall table, during construction.
    pub unsafe fn alloc(&mut self, sys: &SyscallTable) -> bool {
        self.0 = (sys.heap_alloc)(N as u32);
        !self.0.is_null()
    }
}

#[cfg(feature = "big")]
impl<const N: usize> core::ops::Deref for DocBuf<N> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        if self.0.is_null() {
            return &[];
        }
        // SAFETY: `N` bytes of this module's heap, owned by this buffer
        // for the module's lifetime.
        unsafe { core::slice::from_raw_parts(self.0, N) }
    }
}

#[cfg(feature = "big")]
impl<const N: usize> core::ops::DerefMut for DocBuf<N> {
    fn deref_mut(&mut self) -> &mut [u8] {
        if self.0.is_null() {
            return &mut [];
        }
        // SAFETY: as `deref`; `&mut self` makes it the only view.
        unsafe { core::slice::from_raw_parts_mut(self.0, N) }
    }
}
