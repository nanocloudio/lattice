//! A buffer that holds a whole document, request or answer, shared by the
//! document modules that come in two builds: inline in a module's state
//! in the bounded build; a slice of the module's heap arena, taken at
//! construction, in the `big` build (the `*_big` twins, whose documents
//! are past what a state holds). Either way it is a `[u8]` of `N` bytes,
//! so the code that uses it is one.
//!
//! The big build takes its buffers as ONE heap allocation ([`DocArena`])
//! and carves them from it in order. The heap may be elastic, growing in
//! segments that are not contiguous, and an allocation cannot span two:
//! taken one by one, the buffers would strand each segment's tail and a
//! heap declared at their sum would run out before the last. One
//! allocation of the sum is served whole by either backing.
//!
//! Mounted beside the module's `abi` (reached as `super::abi`). A module
//! whose `big` build uses it declares its arena
//! (`declare_module_arena_bytes!`) as the sum of its buffers plus one
//! allocation's rounding.

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

/// The big build's buffers' memory: one allocation from the module's
/// heap, taken at construction, that its [`DocBuf`]s are carved from.
#[cfg(feature = "big")]
pub struct DocArena {
    next: *mut u8,
    left: usize,
}

#[cfg(feature = "big")]
impl DocArena {
    /// Take `bytes` from the heap as one allocation: `None` when the heap
    /// cannot.
    ///
    /// # Safety
    /// `sys` is the module's syscall table, during construction.
    pub unsafe fn take(sys: &SyscallTable, bytes: usize) -> Option<DocArena> {
        let len = u32::try_from(bytes).ok()?;
        let next = (sys.heap_alloc)(len);
        (!next.is_null()).then_some(DocArena { next, left: bytes })
    }
}

/// The big build's: `N` bytes of the module's heap, carved once at
/// construction ([`DocBuf::carve`]); empty until then.
#[cfg(feature = "big")]
pub struct DocBuf<const N: usize>(*mut u8);

#[cfg(feature = "big")]
impl<const N: usize> DocBuf<N> {
    /// Carve the buffer from `arena`: `false` when what is left is short
    /// (the arena was taken smaller than the buffers it serves).
    pub fn carve(&mut self, arena: &mut DocArena) -> bool {
        if arena.left < N {
            return false;
        }
        self.0 = arena.next;
        // SAFETY: `N <= left`, so the carve stays inside the allocation.
        arena.next = unsafe { arena.next.add(N) };
        arena.left -= N;
        true
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
