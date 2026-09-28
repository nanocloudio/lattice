//! etcd's authentication and role-based access control, as pure logic:
//! the user and role records, the in-memory cache the etcd anchor
//! checks requests against, password hashing, and permission coverage.
//!
//! Dual-target: the PIC `etcd_edge_anchor` `#[path]`-mounts it, and
//! `tests/contract_etcd_auth.rs` mounts it on the host.
//!
//! The records live in the store, in [`AUTH_KEYSPACE`], so they are
//! persisted, replicated and snapshotted with everything else; the
//! anchor reads them into an [`AuthCache`] when it starts and applies
//! each change it commits. Record keys and values:
//!
//! | key | value |
//! |---|---|
//! | `enabled` | `[1]` or `[0]` |
//! | `rev` | `[auth_revision:u64 LE]` |
//! | `u/<name>` | `[flags:u8][salt:16][hash:32][n:u8]` then `n × [len:u8][role]` |
//! | `r/<name>` | `[n:u8]` then `n × [type:u8][klen:u16][key][elen:u16][end]` |
//!
//! `flags` bit 0 is etcd's `no_password`; `hash` is PBKDF2-HMAC-SHA-256
//! of the password under `salt`. A permission's `type` is etcd's
//! `authpb.Permission.Type`: READ 0, WRITE 1, READWRITE 2.

#![allow(
    dead_code,
    reason = "exported surface is consumed by both PIC and host test paths"
)]

/// SHA-256 from the SDK, for the password hash.
#[allow(dead_code, reason = "the SDK hasher is mounted whole")]
pub mod sha {
    include!("../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
}
use sha::Sha256;

/// The keyspace etcd's auth records live in (tenant 0, database 0).
pub const AUTH_KEYSPACE: u32 = 0x800B_0001;

pub const USERS_MAX: usize = 32;
pub const ROLES_MAX: usize = 32;
/// Roles one user holds.
pub const USER_ROLES_MAX: usize = 8;
/// Permissions one role holds.
pub const PERMS_MAX: usize = 8;
pub const NAME_MAX: usize = 64;
/// A permission's key or range end.
pub const PERM_KEY_MAX: usize = 128;
/// PBKDF2 iterations for a password hash.
pub const HASH_ITERATIONS: u32 = 1024;

pub const PERM_READ: u8 = 0;
pub const PERM_WRITE: u8 = 1;
pub const PERM_READWRITE: u8 = 2;

/// What an operation needs of a key range.
pub const NEED_READ: u8 = 1;
pub const NEED_WRITE: u8 = 2;

pub const KEY_ENABLED: &[u8] = b"enabled";
pub const KEY_REV: &[u8] = b"rev";

/// The name etcd reserves for the all-powerful user and role.
pub const ROOT: &[u8] = b"root";

#[derive(Clone, Copy)]
pub struct Name {
    pub len: u8,
    pub bytes: [u8; NAME_MAX],
}

impl Name {
    pub const EMPTY: Name = Name {
        len: 0,
        bytes: [0; NAME_MAX],
    };

    pub fn from(b: &[u8]) -> Option<Name> {
        if b.is_empty() || b.len() > NAME_MAX {
            return None;
        }
        let mut n = Name::EMPTY;
        n.bytes[..b.len()].copy_from_slice(b);
        n.len = b.len() as u8;
        Some(n)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

#[derive(Clone, Copy)]
pub struct Perm {
    pub ptype: u8,
    pub klen: u8,
    pub key: [u8; PERM_KEY_MAX],
    pub elen: u8,
    pub end: [u8; PERM_KEY_MAX],
}

impl Perm {
    pub const EMPTY: Perm = Perm {
        ptype: 0,
        klen: 0,
        key: [0; PERM_KEY_MAX],
        elen: 0,
        end: [0; PERM_KEY_MAX],
    };

    pub fn new(ptype: u8, key: &[u8], end: &[u8]) -> Option<Perm> {
        if ptype > PERM_READWRITE || key.len() > PERM_KEY_MAX || end.len() > PERM_KEY_MAX {
            return None;
        }
        let mut p = Perm::EMPTY;
        p.ptype = ptype;
        p.key[..key.len()].copy_from_slice(key);
        p.klen = key.len() as u8;
        p.end[..end.len()].copy_from_slice(end);
        p.elen = end.len() as u8;
        Some(p)
    }

    pub fn key(&self) -> &[u8] {
        &self.key[..self.klen as usize]
    }

    pub fn end(&self) -> &[u8] {
        &self.end[..self.elen as usize]
    }

    fn grants(&self, need: u8) -> bool {
        match need {
            NEED_READ => self.ptype == PERM_READ || self.ptype == PERM_READWRITE,
            _ => self.ptype == PERM_WRITE || self.ptype == PERM_READWRITE,
        }
    }
}

#[derive(Clone, Copy)]
pub struct User {
    pub used: bool,
    pub name: Name,
    pub no_password: bool,
    pub salt: [u8; 16],
    pub hash: [u8; 32],
    pub nroles: u8,
    pub roles: [Name; USER_ROLES_MAX],
}

impl User {
    pub const EMPTY: User = User {
        used: false,
        name: Name::EMPTY,
        no_password: false,
        salt: [0; 16],
        hash: [0; 32],
        nroles: 0,
        roles: [Name::EMPTY; USER_ROLES_MAX],
    };

    pub fn roles(&self) -> &[Name] {
        &self.roles[..self.nroles as usize]
    }

    pub fn has_role(&self, role: &[u8]) -> bool {
        self.roles().iter().any(|r| r.as_bytes() == role)
    }

    pub fn grant(&mut self, role: &[u8]) -> bool {
        if self.has_role(role) {
            return true;
        }
        let (Some(n), true) = (Name::from(role), (self.nroles as usize) < USER_ROLES_MAX) else {
            return false;
        };
        self.roles[self.nroles as usize] = n;
        self.nroles += 1;
        true
    }

    pub fn revoke(&mut self, role: &[u8]) -> bool {
        let n = self.nroles as usize;
        let Some(i) = self.roles[..n].iter().position(|r| r.as_bytes() == role) else {
            return false;
        };
        self.roles[i] = self.roles[n - 1];
        self.nroles -= 1;
        true
    }
}

#[derive(Clone, Copy)]
pub struct Role {
    pub used: bool,
    pub name: Name,
    pub nperms: u8,
    pub perms: [Perm; PERMS_MAX],
}

impl Role {
    pub const EMPTY: Role = Role {
        used: false,
        name: Name::EMPTY,
        nperms: 0,
        perms: [Perm::EMPTY; PERMS_MAX],
    };

    pub fn perms(&self) -> &[Perm] {
        &self.perms[..self.nperms as usize]
    }

    /// Grant `p`, replacing a permission on the same range (etcd's).
    pub fn grant(&mut self, p: Perm) -> bool {
        let n = self.nperms as usize;
        if let Some(i) = self.perms[..n]
            .iter()
            .position(|q| q.key() == p.key() && q.end() == p.end())
        {
            self.perms[i] = p;
            return true;
        }
        if n == PERMS_MAX {
            return false;
        }
        self.perms[n] = p;
        self.nperms += 1;
        true
    }

    pub fn revoke(&mut self, key: &[u8], end: &[u8]) -> bool {
        let n = self.nperms as usize;
        let Some(i) = self.perms[..n]
            .iter()
            .position(|q| q.key() == key && q.end() == end)
        else {
            return false;
        };
        self.perms[i] = self.perms[n - 1];
        self.nperms -= 1;
        true
    }
}

/// The auth state the anchor checks requests against: the store's
/// records, read once at start and kept current by the changes the
/// anchor commits.
pub struct AuthCache {
    pub loaded: bool,
    pub enabled: bool,
    pub revision: u64,
    pub users: [User; USERS_MAX],
    pub roles: [Role; ROLES_MAX],
}

impl AuthCache {
    pub fn init(&mut self) {
        self.loaded = false;
        self.enabled = false;
        self.revision = 1;
        // One slot at a time: an array literal would build the whole
        // table on the stack first.
        for u in self.users.iter_mut() {
            *u = User::EMPTY;
        }
        for r in self.roles.iter_mut() {
            *r = Role::EMPTY;
        }
    }

    pub fn user(&self, name: &[u8]) -> Option<usize> {
        self.users
            .iter()
            .position(|u| u.used && u.name.as_bytes() == name)
    }

    pub fn role(&self, name: &[u8]) -> Option<usize> {
        self.roles
            .iter()
            .position(|r| r.used && r.name.as_bytes() == name)
    }

    /// A slot for a new user or role.
    pub fn free_user(&self) -> Option<usize> {
        self.users.iter().position(|u| !u.used)
    }

    pub fn free_role(&self) -> Option<usize> {
        self.roles.iter().position(|r| !r.used)
    }

    /// Apply one stored record (a load, or a change just committed). A
    /// `None` value is the record's deletion.
    pub fn apply(&mut self, key: &[u8], value: Option<&[u8]>) {
        if key == KEY_ENABLED {
            self.enabled = value.is_some_and(|v| v.first() == Some(&1));
        } else if key == KEY_REV {
            if let Some(v) = value.and_then(|v| v.get(..8)) {
                let mut b = [0u8; 8];
                b.copy_from_slice(v);
                self.revision = u64::from_le_bytes(b);
            }
        } else if let Some(name) = key.strip_prefix(b"u/") {
            let at = self.user(name).or_else(|| self.free_user());
            if let Some(i) = at {
                match value.and_then(|v| decode_user(name, v)) {
                    Some(u) => self.users[i] = u,
                    None => self.users[i] = User::EMPTY,
                }
            }
        } else if let Some(name) = key.strip_prefix(b"r/") {
            let at = self.role(name).or_else(|| self.free_role());
            if let Some(i) = at {
                match value.and_then(|v| decode_role(name, v)) {
                    Some(r) => self.roles[i] = r,
                    None => self.roles[i] = Role::EMPTY,
                }
            }
        }
    }

    /// Whether user `u` holds etcd's root role (all permissions).
    pub fn is_root(&self, u: usize) -> bool {
        self.users
            .get(u)
            .is_some_and(|u| u.used && u.has_role(ROOT))
    }

    /// Whether user `u` may `need` (READ / WRITE) the range
    /// `[key, end)` (`end` empty: the one key; `\0`: every key from
    /// `key`). The user's permissions over all their roles are taken
    /// as a union: a range two permissions cover together is covered.
    pub fn permitted(&self, u: usize, key: &[u8], end: &[u8], need: u8) -> bool {
        let Some(user) = self.users.get(u).filter(|u| u.used) else {
            return false;
        };
        if user.has_role(ROOT) {
            return true;
        }
        // Walk the range from its start: each step takes, of the
        // permissions holding the current point, the one reaching
        // furthest, and moves there.
        let mut at = [0u8; PERM_KEY_MAX + 2];
        let Some(mut alen) = copy_into(&mut at, key) else {
            return false;
        };
        loop {
            let mut reach = [0u8; PERM_KEY_MAX + 2];
            let mut rlen = 0usize;
            let mut found = false;
            for r in user.roles() {
                let Some(ri) = self.role(r.as_bytes()) else {
                    continue;
                };
                for p in self.roles[ri].perms() {
                    if !p.grants(need) || !holds(p, &at[..alen]) {
                        continue;
                    }
                    if p.end() == b"\0" {
                        return true; // reaches every key after
                    }
                    // A single-key permission reaches just past its key.
                    let mut cand = [0u8; PERM_KEY_MAX + 2];
                    let clen = if p.end().is_empty() {
                        let n = alen.min(PERM_KEY_MAX + 1);
                        cand[..n].copy_from_slice(&at[..n]);
                        n + 1 // the trailing 0 is already there
                    } else {
                        cand[..p.end().len()].copy_from_slice(p.end());
                        p.end().len()
                    };
                    if !found || cand[..clen] > reach[..rlen] {
                        reach = cand;
                        rlen = clen;
                        found = true;
                    }
                }
            }
            if !found {
                return false;
            }
            // The one key asked for is held.
            if end.is_empty() {
                return true;
            }
            if end != b"\0" && &reach[..rlen] >= end {
                return true;
            }
            if rlen > PERM_KEY_MAX + 1 {
                return false;
            }
            at = reach;
            alen = rlen;
        }
    }
}

/// Whether permission `p` holds the one key `k`.
fn holds(p: &Perm, k: &[u8]) -> bool {
    if p.end().is_empty() {
        return p.key() == k;
    }
    if p.end() == b"\0" {
        return k >= p.key();
    }
    k >= p.key() && k < p.end()
}

fn copy_into(dst: &mut [u8], src: &[u8]) -> Option<usize> {
    if src.len() > dst.len() {
        return None;
    }
    dst[..src.len()].copy_from_slice(src);
    Some(src.len())
}

// ── Records ───────────────────────────────────────────────────────────

pub fn decode_user(name: &[u8], v: &[u8]) -> Option<User> {
    let mut u = User::EMPTY;
    u.used = true;
    u.name = Name::from(name)?;
    u.no_password = v.first()? & 1 != 0;
    u.salt.copy_from_slice(v.get(1..17)?);
    u.hash.copy_from_slice(v.get(17..49)?);
    let n = *v.get(49)? as usize;
    let mut p = 50usize;
    for _ in 0..n.min(USER_ROLES_MAX) {
        let l = *v.get(p)? as usize;
        let r = v.get(p + 1..p + 1 + l)?;
        u.roles[u.nroles as usize] = Name::from(r)?;
        u.nroles += 1;
        p += 1 + l;
    }
    Some(u)
}

/// A user's record value; its length.
pub fn encode_user(u: &User, out: &mut [u8]) -> Option<usize> {
    let mut p = 0usize;
    put(out, &mut p, &[u8::from(u.no_password)])?;
    put(out, &mut p, &u.salt)?;
    put(out, &mut p, &u.hash)?;
    put(out, &mut p, &[u.nroles])?;
    for r in u.roles() {
        put(out, &mut p, &[r.len])?;
        put(out, &mut p, r.as_bytes())?;
    }
    Some(p)
}

pub fn decode_role(name: &[u8], v: &[u8]) -> Option<Role> {
    let mut r = Role::EMPTY;
    r.used = true;
    r.name = Name::from(name)?;
    let n = *v.first()? as usize;
    let mut p = 1usize;
    let rd16 = |at: usize| -> Option<usize> {
        Some(u16::from_le_bytes([*v.get(at)?, *v.get(at + 1)?]) as usize)
    };
    for _ in 0..n.min(PERMS_MAX) {
        let t = *v.get(p)?;
        let kl = rd16(p + 1)?;
        let key = v.get(p + 3..p + 3 + kl)?;
        let el = rd16(p + 3 + kl)?;
        let end = v.get(p + 5 + kl..p + 5 + kl + el)?;
        r.perms[r.nperms as usize] = Perm::new(t, key, end)?;
        r.nperms += 1;
        p += 5 + kl + el;
    }
    Some(r)
}

/// A role's record value; its length.
pub fn encode_role(r: &Role, out: &mut [u8]) -> Option<usize> {
    let mut p = 0usize;
    put(out, &mut p, &[r.nperms])?;
    for q in r.perms() {
        put(out, &mut p, &[q.ptype])?;
        put(out, &mut p, &(q.klen as u16).to_le_bytes())?;
        put(out, &mut p, q.key())?;
        put(out, &mut p, &(q.elen as u16).to_le_bytes())?;
        put(out, &mut p, q.end())?;
    }
    Some(p)
}

fn put(out: &mut [u8], p: &mut usize, b: &[u8]) -> Option<()> {
    out.get_mut(*p..*p + b.len())?.copy_from_slice(b);
    *p += b.len();
    Some(())
}

/// A record key: `prefix` then `name`.
pub fn record_key(prefix: &[u8], name: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0usize;
    put(out, &mut p, prefix)?;
    put(out, &mut p, name)?;
    Some(p)
}

// ── Passwords ─────────────────────────────────────────────────────────

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let mut h = Sha256::new();
        h.update(key);
        k[..32].copy_from_slice(&h.finalize());
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for ((i, o), k) in ipad.iter_mut().zip(opad.iter_mut()).zip(k.iter()) {
        *i ^= k;
        *o ^= k;
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    for p in parts {
        inner.update(p);
    }
    let ih = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&ih);
    outer.finalize()
}

/// PBKDF2-HMAC-SHA-256 of `password` under `salt`, one 32-byte block.
pub fn hash_password(password: &[u8], salt: &[u8; 16]) -> [u8; 32] {
    let mut u = hmac(password, &[salt, &1u32.to_be_bytes()]);
    let mut t = u;
    for _ in 1..HASH_ITERATIONS {
        u = hmac(password, &[&u]);
        for (t, u) in t.iter_mut().zip(u.iter()) {
            *t ^= u;
        }
    }
    t
}

/// Whether `password` is user `u`'s (never, for a `no_password` user).
pub fn check_password(u: &User, password: &[u8]) -> bool {
    if u.no_password {
        return false;
    }
    let h = hash_password(password, &u.salt);
    // Constant-time compare.
    let d = h
        .iter()
        .zip(u.hash.iter())
        .fold(0u8, |d, (a, b)| d | (a ^ b));
    d == 0
}
