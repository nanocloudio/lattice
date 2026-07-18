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

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> MetricsTable {
        let mut t = MetricsTable {
            counters: [Counter::empty(); MAX_COUNTERS],
            len: 0,
        };
        t.init();
        t
    }

    #[test]
    fn first_delta_allocates_counter() {
        let mut t = fresh();
        assert!(record_delta(&mut t, 1, 2, 7));
        assert_eq!(t.len, 1);
        assert_eq!(t.counters[0].total, 7);
    }

    #[test]
    fn deltas_accumulate() {
        let mut t = fresh();
        record_delta(&mut t, 1, 2, 5);
        record_delta(&mut t, 1, 2, 10);
        assert_eq!(t.counters[0].total, 15);
    }

    #[test]
    fn distinct_keys_get_separate_counters() {
        let mut t = fresh();
        record_delta(&mut t, 1, 2, 5);
        record_delta(&mut t, 1, 3, 7);
        record_delta(&mut t, 2, 2, 11);
        assert_eq!(t.len, 3);
    }

    #[test]
    fn snapshot_encodes_all_counters() {
        let mut t = fresh();
        record_delta(&mut t, 1, 2, 100);
        record_delta(&mut t, 1, 3, 200);
        let mut buf = [0u8; 64];
        let n = encode_snapshot(&t, &mut buf).unwrap();
        assert_eq!(n, 2 + 2 * 10);
        assert_eq!(buf[0], 2);
        assert_eq!(buf[1], 0);
    }

    #[test]
    fn overflow_returns_false() {
        let mut t = fresh();
        for i in 0..MAX_COUNTERS {
            assert!(record_delta(&mut t, 0, i as u8, 1));
        }
        assert!(!record_delta(&mut t, 1, 0, 1));
    }
}
