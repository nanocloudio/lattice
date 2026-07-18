//! Pure-logic per-tenant token bucket for `quota_manager`.
//!
//! Dual-target: PIC `quota_manager` mounts this; host tests under
//! `tests/integration_phase6b.rs` exercise the bucket arithmetic.

#![allow(dead_code, reason = "consumed by both PIC and host paths")]

pub const MAX_TENANTS: usize = 64;
pub const DEFAULT_BURST: i32 = 1000;
pub const DEFAULT_RATE: i32 = 100;
pub const DEFAULT_DISCONNECT_STRIKES: u8 = 5;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TenantBucket {
    pub tenant_id: u32,
    pub tokens: i32,
    pub rate: i32,
    pub burst: i32,
    pub strikes: u8,
    pub _pad: [u8; 3],
}

impl TenantBucket {
    pub const fn empty() -> Self {
        Self {
            tenant_id: 0,
            tokens: 0,
            rate: 0,
            burst: 0,
            strikes: 0,
            _pad: [0; 3],
        }
    }
}

#[repr(C)]
pub struct QuotaTable {
    pub buckets: [TenantBucket; MAX_TENANTS],
    pub len: u16,
    pub disconnect_strikes: u8,
    pub _pad: u8,
}

impl QuotaTable {
    pub fn init(&mut self) {
        let mut i = 0;
        while i < MAX_TENANTS {
            self.buckets[i] = TenantBucket::empty();
            i += 1;
        }
        self.len = 0;
        self.disconnect_strikes = DEFAULT_DISCONNECT_STRIKES;
    }
}

pub fn find_mut(t: &mut QuotaTable, tenant_id: u32) -> Option<&mut TenantBucket> {
    let len = t.len as usize;
    let mut i = 0;
    while i < len {
        if t.buckets[i].tenant_id == tenant_id {
            return Some(&mut t.buckets[i]);
        }
        i += 1;
    }
    None
}

pub fn upsert_tenant(t: &mut QuotaTable, tenant_id: u32, rate: i32, burst: i32) -> bool {
    if let Some(b) = find_mut(t, tenant_id) {
        b.rate = rate;
        b.burst = burst;
        if b.tokens > burst {
            b.tokens = burst;
        }
        return true;
    }
    if (t.len as usize) >= MAX_TENANTS {
        return false;
    }
    let idx = t.len as usize;
    t.buckets[idx] = TenantBucket {
        tenant_id,
        tokens: burst,
        rate,
        burst,
        strikes: 0,
        _pad: [0; 3],
    };
    t.len += 1;
    true
}

/// `(allowed, disconnect)`. Disconnect fires when the strike counter
/// hits `disconnect_strikes`; a successful consume resets it.
pub fn consume(t: &mut QuotaTable, tenant_id: u32, cost: u16) -> (bool, bool) {
    if find_mut(t, tenant_id).is_none() {
        let _ = upsert_tenant(t, tenant_id, DEFAULT_RATE, DEFAULT_BURST);
    }
    let disc_thresh = t.disconnect_strikes;
    let Some(b) = find_mut(t, tenant_id) else {
        return (false, false);
    };
    if b.tokens >= i32::from(cost) {
        b.tokens -= i32::from(cost);
        b.strikes = 0;
        (true, false)
    } else {
        b.strikes = b.strikes.saturating_add(1);
        let disc = b.strikes >= disc_thresh;
        (false, disc)
    }
}

pub fn refill_all(t: &mut QuotaTable) {
    let len = t.len as usize;
    let mut i = 0;
    while i < len {
        let b = &mut t.buckets[i];
        let next = b.tokens.saturating_add(b.rate);
        b.tokens = if next > b.burst { b.burst } else { next };
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> QuotaTable {
        let mut t = QuotaTable {
            buckets: [TenantBucket::empty(); MAX_TENANTS],
            len: 0,
            disconnect_strikes: 0,
            _pad: 0,
        };
        t.init();
        t
    }

    #[test]
    fn unknown_tenant_auto_registers_and_allows() {
        let mut t = fresh();
        let (allow, disc) = consume(&mut t, 42, 1);
        assert!(allow);
        assert!(!disc);
        assert_eq!(t.len, 1);
    }

    #[test]
    fn burst_drains_then_throttles() {
        let mut t = fresh();
        upsert_tenant(&mut t, 1, 1, 10);
        for _ in 0..10 {
            assert!(consume(&mut t, 1, 1).0);
        }
        assert!(!consume(&mut t, 1, 1).0);
    }

    #[test]
    fn refill_caps_at_burst() {
        let mut t = fresh();
        upsert_tenant(&mut t, 1, 100, 10);
        for _ in 0..10 {
            consume(&mut t, 1, 1);
        }
        refill_all(&mut t);
        for _ in 0..10 {
            assert!(consume(&mut t, 1, 1).0);
        }
        assert!(!consume(&mut t, 1, 1).0);
    }

    #[test]
    fn repeated_throttles_trigger_disconnect() {
        let mut t = fresh();
        t.disconnect_strikes = 3;
        upsert_tenant(&mut t, 1, 0, 0);
        let (_, d1) = consume(&mut t, 1, 1);
        let (_, d2) = consume(&mut t, 1, 1);
        let (_, d3) = consume(&mut t, 1, 1);
        assert!(!d1);
        assert!(!d2);
        assert!(d3);
    }

    #[test]
    fn allow_resets_strike_counter() {
        let mut t = fresh();
        t.disconnect_strikes = 2;
        upsert_tenant(&mut t, 1, 5, 5);
        for _ in 0..5 {
            consume(&mut t, 1, 1);
        }
        let (_, d1) = consume(&mut t, 1, 1);
        assert!(!d1);
        refill_all(&mut t);
        let (a, _) = consume(&mut t, 1, 1);
        assert!(a);
        for _ in 0..4 {
            consume(&mut t, 1, 1);
        }
        let (_, d2) = consume(&mut t, 1, 1);
        let (_, d3) = consume(&mut t, 1, 1);
        assert!(!d2);
        assert!(d3);
    }
}
