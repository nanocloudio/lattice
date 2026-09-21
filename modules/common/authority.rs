// Dial authority for the outbound connectors.
//
// A connector names its peer once, as `host[:port]`, and dials it with a
// `CMD_CONNECT_TO` record: a literal travels as its address, a name
// travels as a name for the network provider to resolve. The text is
// parsed when the module is constructed, so a bad authority refuses
// construction with a reason, and every dial composes its record from
// the kept bytes. Like the other `modules/common` files this has no
// inner attributes and no test module: the PIC modules `#[path]`-mount
// it and the harness mounts the same file.

// The single mount of the SDK contract for this file. A module that
// needs the contract's opcodes reaches them through its own `abi`
// mount; nothing of this module's `net_proto` crosses the boundary.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/net/net_proto.rs"]
pub mod net_proto;

use net_proto::{write_connect_to, Target, SOCK_TYPE_STREAM};

/// Longest authority a connector keeps. A 63-byte label with a port,
/// or a bracketed IPv6 literal with a port, fits; a longer name is
/// refused at construction rather than truncated into a different name.
pub const AUTHORITY_MAX: usize = 64;

/// Protocol default ports, applied when an authority names none.
pub const PORT_MONGO: u16 = 27017;
pub const PORT_PG: u16 = 5432;
pub const PORT_MYSQL: u16 = 3306;
pub const PORT_CASSANDRA: u16 = 9042;
pub const PORT_REDIS: u16 = 6379;
pub const PORT_SPAN_COURIER: u16 = 7400;
pub const PORT_LATTICE_DATA: u16 = 7432;

/// One `host[:port]` as configured, plus the port it resolves to.
#[repr(C)]
pub struct Authority {
    text: [u8; AUTHORITY_MAX],
    len: u8,
    /// The text offered was longer than `AUTHORITY_MAX`; `adopt` refuses.
    overflow: u8,
    /// The authority's port or the protocol default; 0 until `adopt`.
    port: u16,
}

impl Authority {
    pub const fn empty() -> Self {
        Authority {
            text: [0; AUTHORITY_MAX],
            len: 0,
            overflow: 0,
            port: 0,
        }
    }

    pub fn clear(&mut self) {
        *self = Authority::empty();
    }

    /// Keep `text` as written. Longer than `AUTHORITY_MAX` is recorded
    /// as an overflow so `adopt` can refuse it by name.
    pub fn set(&mut self, text: &[u8]) {
        self.clear();
        if text.len() > AUTHORITY_MAX {
            self.overflow = 1;
            return;
        }
        self.text[..text.len()].copy_from_slice(text);
        self.len = text.len() as u8;
    }

    /// Parse the kept text as `host[:port]`; `default_port` applies
    /// when it names none. `false` when nothing was set, the text
    /// overflowed, or it is not an authority — the caller refuses to
    /// construct.
    pub fn adopt(&mut self, default_port: u16) -> bool {
        if self.overflow != 0 || self.len == 0 {
            return false;
        }
        let Some((_, port)) = Target::parse(self.text()) else {
            return false;
        };
        self.port = port.unwrap_or(default_port);
        true
    }

    /// Some text was offered, whether or not it fit: an optional
    /// authority that was given must still parse.
    pub fn offered(&self) -> bool {
        self.len > 0 || self.overflow != 0
    }

    /// Text was set and `adopt` accepted it.
    pub fn is_set(&self) -> bool {
        self.len > 0 && self.port != 0
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn text(&self) -> &[u8] {
        &self.text[..self.len as usize]
    }

    /// Compose the `CMD_CONNECT_TO` payload for this authority into
    /// `buf`, tagged with `tag` when given. `0` when the authority was
    /// never adopted or `buf` cannot hold the record.
    pub fn connect_record(&self, buf: &mut [u8], tag: Option<u8>) -> usize {
        if !self.is_set() {
            return 0;
        }
        let Some((target, _)) = Target::parse(self.text()) else {
            return 0;
        };
        write_connect_to(buf, SOCK_TYPE_STREAM, self.port, &target, tag)
    }
}
