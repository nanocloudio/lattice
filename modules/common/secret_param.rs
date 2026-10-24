//! A secret (a password) given to a module as a `blob` param, taken whole
//! or refused.
//!
//! fluxor frames a blob param self-delimiting: its total length (u16 LE)
//! ahead of the first chunk's bytes, then the rest in chunks, each chunk
//! delivered to the module as its own param entry. A secret is kept only
//! when every byte of it arrived and it fits the module's buffer; anything
//! else is refused, never cut short — a secret held as a prefix of itself
//! would admit anyone who knows only that prefix.
//!
//! Dual-target: `#[path]`-mounted by the anchors that take a password and
//! by host tests. No allocation and no panic path.

/// What a secret's param entries have assembled so far.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SecretTake {
    /// The secret's length, from its first entry's header (0: none yet).
    pub total: usize,
    /// Bytes of it taken so far.
    pub have: usize,
    /// It cannot be kept: longer than the buffer, or framed wrongly.
    pub refused: bool,
}

impl SecretTake {
    pub const fn new() -> Self {
        Self {
            total: 0,
            have: 0,
            refused: false,
        }
    }

    /// Take one param entry (`entry`) of the secret into `buf`.
    pub fn take(&mut self, buf: &mut [u8], entry: &[u8]) {
        // An empty entry is the param's default, applied before any given
        // value: no secret.
        if self.refused || entry.is_empty() {
            return;
        }
        let data = if self.total == 0 {
            let (Some(&lo), Some(&hi)) = (entry.first(), entry.get(1)) else {
                self.refused = true;
                return;
            };
            let total = usize::from(u16::from_le_bytes([lo, hi]));
            if total == 0 || total > buf.len() {
                self.refused = true;
                return;
            }
            self.total = total;
            entry.get(2..).unwrap_or(&[])
        } else {
            entry
        };
        if self.have + data.len() > self.total {
            self.refused = true;
            return;
        }
        let Some(dst) = buf.get_mut(self.have..self.have + data.len()) else {
            self.refused = true;
            return;
        };
        for (d, b) in dst.iter_mut().zip(data) {
            *d = *b;
        }
        self.have += data.len();
    }

    /// The secret's length once every byte has arrived (0: none was
    /// given), or `None` when it must be refused.
    pub fn finished(&self) -> Option<usize> {
        if self.refused || self.have != self.total {
            return None;
        }
        Some(self.total)
    }
}
