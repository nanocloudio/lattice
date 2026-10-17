//! A whole-value `storage.object` read that may wait: `GET`, then
//! `RANGE_GET` until the value ends, held across steps whenever the store
//! answers "not yet".
//!
//! `storage.object` lets `GET` and `RANGE_GET` answer `EAGAIN` while the
//! value is not available yet (a board's store whose device is busy with a
//! write). That is "ask again", never "absent" and never "short": a read
//! that waits decides nothing. The contract asks a consumer to hold the
//! handle and the request rather than open a new one, so a `RANGE_GET`
//! that waits keeps its handle and its offset here, and the next
//! [`HeldRead::step`] — on a later step, never in the same one — asks the
//! same range of the same handle again. A `GET` that waits opened nothing
//! and is simply asked again.
//!
//! All zero is the empty state (nothing held), so a read lives in a
//! zeroed module state as it is.
//!
//! No allocation and no panic path: mounted by PIC modules.

/// `storage.object` opcodes and the answers a read tells apart.
pub const OBJ_GET: u32 = 0x1421;
pub const OBJ_RANGE_GET: u32 = 0x1423;
pub const OBJ_CLOSE: u32 = 0x1425;
pub const EAGAIN: i32 = -11;

/// The most one `RANGE_GET` asks for (the provider may answer less).
pub const RANGE_ASK_MAX: u32 = 1048576;

/// What a read step answered.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Read {
    /// The value, whole (or as much of it as the buffer holds): its length.
    Done(usize),
    /// Decided without a value: the errno (`ENXIO` for an absent key).
    Failed(i32),
    /// Not yet: nothing may be concluded. Asked again on a later step.
    Pending,
}

/// One read in progress: the handle it holds and how much has arrived.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct HeldRead {
    held: bool,
    handle: i32,
    got: u32,
}

impl HeldRead {
    pub const fn new() -> Self {
        HeldRead {
            held: false,
            handle: 0,
            got: 0,
        }
    }

    /// A read is in progress: its handle is open, and the next step must
    /// be for the same key and buffer.
    pub fn held(&self) -> bool {
        self.held
    }

    /// Read `key` whole into `dst`, or as far as the store answers now.
    /// `call(handle, op, arg)` is the module's `provider_call`. A
    /// [`Read::Pending`] keeps what has arrived in `dst`; the caller asks
    /// again with the same `key` and `dst` on a later step.
    pub fn step<F: FnMut(i32, u32, &mut [u8]) -> i32>(
        &mut self,
        call: &mut F,
        key: &mut [u8],
        dst: &mut [u8],
    ) -> Read {
        if !self.held {
            let h = call(-1, OBJ_GET, key);
            if h == EAGAIN {
                return Read::Pending;
            }
            if h < 0 {
                return Read::Failed(h);
            }
            self.held = true;
            self.handle = h;
            self.got = 0;
        }
        loop {
            let got = self.got as usize;
            if got >= dst.len() {
                return self.finish(call, Read::Done(got));
            }
            let room = ((dst.len() - got) as u64).min(u64::from(RANGE_ASK_MAX)) as u32;
            let Some(out) = dst.get_mut(got..) else {
                return self.finish(call, Read::Done(got));
            };
            let mut arg = [0u8; 20];
            put(&mut arg, 0, &(got as u64).to_le_bytes());
            put(&mut arg, 8, &room.to_le_bytes());
            put(&mut arg, 12, &(out.as_mut_ptr() as u64).to_le_bytes());
            let n = call(self.handle, OBJ_RANGE_GET, &mut arg);
            if n == EAGAIN {
                // Held: the same range of the same handle, next step.
                return Read::Pending;
            }
            if n < 0 {
                return self.finish(call, Read::Failed(n));
            }
            if n == 0 {
                return self.finish(call, Read::Done(got));
            }
            self.got = self.got.saturating_add((n as u32).min(room));
        }
    }

    /// Let a held read go (its handle closed), whatever it had read.
    pub fn abandon<F: FnMut(i32, u32, &mut [u8]) -> i32>(&mut self, call: &mut F) {
        if self.held {
            let mut none = [0u8; 0];
            call(self.handle, OBJ_CLOSE, &mut none);
        }
        *self = HeldRead::new();
    }

    fn finish<F: FnMut(i32, u32, &mut [u8]) -> i32>(&mut self, call: &mut F, r: Read) -> Read {
        self.abandon(call);
        r
    }
}

fn put(dst: &mut [u8], at: usize, src: &[u8]) {
    for (d, s) in dst.iter_mut().skip(at).zip(src) {
        *d = *s;
    }
}
