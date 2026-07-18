//! Pure-logic principal table for `auth_manager`.
//!
//! Dual-target: the no_std PIC `auth_manager` module `#[path]`-mounts
//! this and wraps it with the envelope ABI; `tests/integration_phase6b.rs`
//! mounts it as a host module to exercise the lookup / decide logic
//! directly under `cargo test`.

#![allow(
    dead_code,
    reason = "exported surface is consumed by both PIC and host test paths"
)]

pub const MAX_PRINCIPALS: usize = 64;
pub const CRED_MAX: usize = 96;
pub const PRINCIPAL_NAME_MAX: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PrincipalEntry {
    pub protocol: u8,
    pub tenant_id: u32,
    pub cred_len: u16,
    pub cred: [u8; CRED_MAX],
    pub name_len: u8,
    pub name: [u8; PRINCIPAL_NAME_MAX],
}

impl PrincipalEntry {
    pub const fn empty() -> Self {
        Self {
            protocol: 0,
            tenant_id: 0,
            cred_len: 0,
            cred: [0; CRED_MAX],
            name_len: 0,
            name: [0; PRINCIPAL_NAME_MAX],
        }
    }
}

#[repr(C)]
pub struct AuthTable {
    pub table: [PrincipalEntry; MAX_PRINCIPALS],
    pub len: u16,
    /// When set, an unmapped credential is still allowed under the
    /// anonymous principal "anon".
    pub allow_anonymous: bool,
}

impl AuthTable {
    pub fn init(&mut self) {
        let mut i = 0;
        while i < MAX_PRINCIPALS {
            self.table[i] = PrincipalEntry::empty();
            i += 1;
        }
        self.len = 0;
        self.allow_anonymous = true;
    }
}

pub fn lookup<'a>(t: &'a AuthTable, protocol: u8, cred: &[u8]) -> Option<&'a PrincipalEntry> {
    let len = t.len as usize;
    let mut i = 0;
    while i < len {
        let e = &t.table[i];
        if e.protocol == protocol
            && (e.cred_len as usize) == cred.len()
            && e.cred[..e.cred_len as usize] == *cred
        {
            return Some(e);
        }
        i += 1;
    }
    None
}

pub fn add_principal(
    t: &mut AuthTable,
    protocol: u8,
    tenant_id: u32,
    cred: &[u8],
    name: &[u8],
) -> bool {
    if (t.len as usize) >= MAX_PRINCIPALS {
        return false;
    }
    if cred.len() > CRED_MAX || name.len() > PRINCIPAL_NAME_MAX {
        return false;
    }
    let idx = t.len as usize;
    let e = &mut t.table[idx];
    e.protocol = protocol;
    e.tenant_id = tenant_id;
    e.cred_len = cred.len() as u16;
    e.cred[..cred.len()].copy_from_slice(cred);
    e.name_len = name.len() as u8;
    e.name[..name.len()].copy_from_slice(name);
    t.len += 1;
    true
}

/// Encode an `MSG_AUTH_DECISION` payload: `[decision:1][name_len:1][name…]`.
/// Returns bytes written.
pub fn decide(t: &AuthTable, protocol: u8, cred: &[u8], out: &mut [u8]) -> Option<usize> {
    if out.len() < 2 {
        return None;
    }
    if let Some(entry) = lookup(t, protocol, cred) {
        let nl = entry.name_len as usize;
        if 2 + nl > out.len() {
            return None;
        }
        out[0] = 1;
        out[1] = entry.name_len;
        out[2..2 + nl].copy_from_slice(&entry.name[..nl]);
        return Some(2 + nl);
    }
    if t.allow_anonymous {
        let name = b"anon";
        if 2 + name.len() > out.len() {
            return None;
        }
        out[0] = 1;
        out[1] = name.len() as u8;
        out[2..2 + name.len()].copy_from_slice(name);
        return Some(2 + name.len());
    }
    out[0] = 0;
    out[1] = 0;
    Some(2)
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> AuthTable {
        let mut t = AuthTable {
            table: [PrincipalEntry::empty(); MAX_PRINCIPALS],
            len: 0,
            allow_anonymous: false,
        };
        t.init();
        t
    }

    #[test]
    fn anonymous_allowed_by_default() {
        let t = fresh();
        let mut out = [0u8; 32];
        let n = decide(&t, 1, b"foo", &mut out).unwrap();
        assert_eq!(out[0], 1);
        assert_eq!(&out[2..n], b"anon");
    }

    #[test]
    fn explicit_principal_overrides_anonymous() {
        let mut t = fresh();
        assert!(add_principal(&mut t, 1, 7, b"secret", b"alice"));
        let mut out = [0u8; 32];
        let n = decide(&t, 1, b"secret", &mut out).unwrap();
        assert_eq!(out[0], 1);
        assert_eq!(&out[2..n], b"alice");
    }

    #[test]
    fn anonymous_disabled_denies_unknown() {
        let mut t = fresh();
        t.allow_anonymous = false;
        let mut out = [0u8; 32];
        let n = decide(&t, 1, b"unknown", &mut out).unwrap();
        assert_eq!(out[0], 0);
        assert_eq!(n, 2);
    }

    #[test]
    fn lookup_distinguishes_protocols() {
        let mut t = fresh();
        t.allow_anonymous = false;
        add_principal(&mut t, 1, 7, b"tok", b"alice");
        let mut out = [0u8; 32];
        decide(&t, 2, b"tok", &mut out).unwrap();
        assert_eq!(out[0], 0);
    }

    #[test]
    fn table_overflow_returns_false() {
        let mut t = fresh();
        for i in 0..MAX_PRINCIPALS {
            let cred = std::format!("c{i}");
            assert!(add_principal(&mut t, 1, i as u32, cred.as_bytes(), b"x"));
        }
        assert!(!add_principal(&mut t, 1, 999, b"overflow", b"y"));
    }
}
