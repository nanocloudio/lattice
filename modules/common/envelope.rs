//! Reading `[msg_type:u8][len:u16 LE][payload]` envelopes off a channel,
//! a whole envelope at a time.
//!
//! A channel with more than one producer is fanned in by the kernel's
//! byte-stream merge, which finishes one producer's frame before another's
//! begins but, when the consumer's channel is short of room, delivers it
//! over several steps. A reader that takes the header and then the payload
//! loses a frame whose payload has only partly arrived — and reads the rest
//! as the next frame's header, so every frame after it is lost until the
//! stream happens to resynchronise. An [`Inbox`] reads nothing until the
//! whole envelope is on the channel.
//!
//! An envelope longer than the caller's buffer cannot be taken. It is
//! discarded whole — across steps when it arrives in pieces — and counted,
//! never half-read: the bytes left to discard are the one piece of state a
//! channel needs, which is why an `Inbox` is kept per input.
//!
//! No allocation and no panic path: mounted by PIC modules.

/// The envelope header: message type and payload length.
pub const ENVELOPE_HDR: usize = 3;

/// A channel an [`Inbox`] reads: what it holds can be looked at without
/// taking it.
pub trait Source {
    /// Copy up to `buf.len()` bytes from the head without consuming them:
    /// how many were copied.
    fn peek(&mut self, buf: &mut [u8]) -> usize;
    /// Take up to `buf.len()` bytes from the head: how many were taken.
    fn read(&mut self, buf: &mut [u8]) -> usize;
}

/// One input channel's reading state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Inbox {
    /// Bytes of an oversize envelope still to discard.
    discard: u32,
    /// Envelopes discarded as longer than the buffer they were read into
    /// (telemetry: a producer writing what this reader cannot take).
    pub oversize: u32,
}

impl Inbox {
    pub const fn new() -> Self {
        Self {
            discard: 0,
            oversize: 0,
        }
    }

    /// The next whole envelope, into `buf`: `(msg_type, payload_len)` with
    /// the payload at `buf[..payload_len]`, or `None` while no envelope is
    /// whole on the channel. The largest payload taken is
    /// `buf.len() - ENVELOPE_HDR`: the envelope is looked at whole, header
    /// and all, before any of it is read.
    pub fn read<S: Source>(&mut self, src: &mut S, buf: &mut [u8]) -> Option<(u8, usize)> {
        let (mt, len) = self.peek(src, buf)?;
        let total = ENVELOPE_HDR + len;
        if src.read(&mut buf[..total]) != total {
            return None;
        }
        buf.copy_within(ENVELOPE_HDR..total, 0);
        Some((mt, len))
    }

    /// The next whole envelope, looked at but left on the channel: the
    /// header and payload at `buf[..ENVELOPE_HDR + payload_len]`, and
    /// `(msg_type, payload_len)`. [`Self::take`] then consumes it.
    pub fn peek<S: Source>(&mut self, src: &mut S, buf: &mut [u8]) -> Option<(u8, usize)> {
        if buf.len() < ENVELOPE_HDR || !self.drain(src, buf) {
            return None;
        }
        loop {
            if src.peek(&mut buf[..ENVELOPE_HDR]) < ENVELOPE_HDR {
                return None;
            }
            let mt = buf[0];
            let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
            let total = ENVELOPE_HDR + len;
            if total <= buf.len() {
                return (src.peek(&mut buf[..total]) == total).then_some((mt, len));
            }
            // Too long to take: its header goes now, its payload as it
            // arrives, and the next envelope is looked at in its place.
            if src.read(&mut buf[..ENVELOPE_HDR]) != ENVELOPE_HDR {
                return None;
            }
            self.discard = len as u32;
            self.oversize = self.oversize.wrapping_add(1);
            if !self.drain(src, buf) {
                return None;
            }
        }
    }

    /// Consume the envelope [`Self::peek`] answered, `payload_len` bytes of
    /// payload. True when it was taken whole.
    pub fn take<S: Source>(&mut self, src: &mut S, payload_len: usize, buf: &mut [u8]) -> bool {
        let total = ENVELOPE_HDR + payload_len;
        total <= buf.len() && src.read(&mut buf[..total]) == total
    }

    /// Discard what is left of an oversize envelope, as much as has
    /// arrived: true once none is left.
    fn drain<S: Source>(&mut self, src: &mut S, buf: &mut [u8]) -> bool {
        while self.discard > 0 {
            let take = (self.discard as usize).min(buf.len());
            let n = src.read(&mut buf[..take]);
            if n == 0 {
                return false;
            }
            self.discard -= n.min(self.discard as usize) as u32;
        }
        true
    }
}

/// A module's channel, read through its syscalls.
pub struct Chan<'a> {
    pub sys: &'a crate::abi::SyscallTable,
    pub handle: i32,
}

impl Source for Chan<'_> {
    fn peek(&mut self, buf: &mut [u8]) -> usize {
        if self.handle < 0 {
            return 0;
        }
        // SAFETY: the module's syscall table; `buf` is writable for its
        // length.
        let n = unsafe { (self.sys.channel_peek)(self.handle, buf.as_mut_ptr(), buf.len()) };
        n.max(0) as usize
    }

    fn read(&mut self, buf: &mut [u8]) -> usize {
        if self.handle < 0 {
            return 0;
        }
        // SAFETY: as `peek`.
        let n = unsafe { (self.sys.channel_read)(self.handle, buf.as_mut_ptr(), buf.len()) };
        n.max(0) as usize
    }
}

/// [`Inbox::read`] on a module's channel `handle`.
///
/// # Safety
/// `sys` is the module's syscall table.
pub unsafe fn read_envelope(
    inbox: &mut Inbox,
    sys: &crate::abi::SyscallTable,
    handle: i32,
    buf: &mut [u8],
) -> Option<(u8, usize)> {
    inbox.read(&mut Chan { sys, handle }, buf)
}
