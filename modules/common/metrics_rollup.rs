//! Pure-logic counter rollup for `adapter_metrics`.
//!
//! Dual-target: PIC `adapter_metrics` mounts this; host tests run the
//! counter table directly.

#![allow(dead_code, reason = "consumed by both PIC and host paths")]

pub const MAX_COUNTERS: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Counter {
    pub protocol: u8,
    pub op: u8,
    pub _pad: [u8; 6],
    pub total: u64,
}

impl Counter {
    pub const fn empty() -> Self {
        Self {
            protocol: 0,
            op: 0,
            _pad: [0; 6],
            total: 0,
        }
    }
}

#[repr(C)]
pub struct MetricsTable {
    pub counters: [Counter; MAX_COUNTERS],
    pub len: u16,
}

impl MetricsTable {
    pub fn init(&mut self) {
        let mut i = 0;
        while i < MAX_COUNTERS {
            self.counters[i] = Counter::empty();
            i += 1;
        }
        self.len = 0;
    }
}

pub fn find_or_alloc(t: &mut MetricsTable, protocol: u8, op: u8) -> Option<usize> {
    let len = t.len as usize;
    let mut i = 0;
    while i < len {
        if t.counters[i].protocol == protocol && t.counters[i].op == op {
            return Some(i);
        }
        i += 1;
    }
    if len >= MAX_COUNTERS {
        return None;
    }
    t.counters[len] = Counter {
        protocol,
        op,
        _pad: [0; 6],
        total: 0,
    };
    t.len += 1;
    Some(len)
}

pub fn record_delta(t: &mut MetricsTable, protocol: u8, op: u8, delta: u32) -> bool {
    let Some(idx) = find_or_alloc(t, protocol, op) else {
        return false;
    };
    t.counters[idx].total = t.counters[idx].total.saturating_add(u64::from(delta));
    true
}

/// Encode `[count:u16 LE][(p,o,total) × count]` into `out`. Returns
/// bytes written.
pub fn encode_snapshot(t: &MetricsTable, out: &mut [u8]) -> Option<usize> {
    let count = t.len as usize;
    let need = 2 + count * 10;
    if need > out.len() {
        return None;
    }
    out[0] = (count & 0xFF) as u8;
    out[1] = ((count >> 8) & 0xFF) as u8;
    let mut p = 2;
    let mut i = 0;
    while i < count {
        let c = &t.counters[i];
        out[p] = c.protocol;
        out[p + 1] = c.op;
        out[p + 2..p + 10].copy_from_slice(&c.total.to_le_bytes());
        p += 10;
        i += 1;
    }
    Some(p)
}
