//! Shared relational contracts — RFC database foundation §14.1-14.6,
//! §14.11, §21 invariants 15/18, delivery Phase 7.
//!
//! Phase 7 slice 1: the *representation* both connectors must target.
//! This file is contracts only — no SQL parser, no binder, no planner,
//! no connector, no ports, no I/O, no clocks, no module. It stands in
//! the same relationship to the future relational modules that
//! `db_ops.rs` does to the Phase-6 workers.
//!
//! §14.3 states the rule this file exists to make mechanically true:
//!
//! > Connectors may parse dialect-specific syntax, but must bind into
//! > the shared relational representation. They **must not introduce
//! > connector-specific table formats, transaction managers, indexes,
//! > catalogs of record, or persistence paths.**
//!
//! A rule like that is not enforceable by review. It is enforceable by
//! there being exactly one encoder, in one file, with golden vectors:
//! if the PostgreSQL connector and the MySQL connector both call
//! [`encode_row`] and [`encode_primary_key`], "equivalent logical
//! state for their shared SQL subset" (§26 Phase 7 acceptance) is a
//! consequence of the code rather than a hope about it.
//!
//! ## 1. Two encodings, on purpose
//!
//! Every value has **two** byte forms and they are not interchangeable:
//!
//! | | [`encode_value_ordered`] | [`encode_value_plain`] |
//! |---|---|---|
//! | position | inside a key | inside a row payload |
//! | requirement | `bytes_cmp == logical_cmp` | compactness |
//! | integers | big-endian, **sign bit flipped** | little-endian, raw |
//! | floats | big-endian, IEEE total-order transform | raw bits, LE |
//! | strings | escaped + terminated (no length prefix) | `[len][bytes]` |
//! | NaN | **rejected** | accepted |
//! | decimals | rescaled to the column's declared scale | scale carried |
//!
//! The reason for the split is that order preservation is expensive and
//! only ever needed in key position. A length prefix sorts short values
//! before long ones *by length*, which destroys lexical ordering — so
//! key-position strings must use §9.1's escape discipline instead
//! ([`internal_key`]'s `0x00 -> 0x00 0xFF`, terminator `0x00 0x01`).
//! Off the key path that cost buys nothing, so payload strings are
//! length-prefixed, integers keep their natural two's-complement bits,
//! and values with no total order (NaN) are storable because nothing
//! will ever try to sort them as bytes.
//!
//! Mixing them is a format break, not a bug you can patch later: a key
//! written with the plain encoding sorts wrong forever. So the two
//! functions are named for their position, not for their shape.
//!
//! ## 2. What this file does NOT own
//!
//! Secondary-index key encoding and unique-ownership records already
//! exist, one layer down, in `db_ops.rs` (§14, §14.4). This file does
//! not duplicate them. It produces the *index value tuple*
//! ([`encode_index_value`]) that `db_ops::IndexEntry` consumes, and
//! re-exports the db_ops surface so a connector needs one import.
//! Likewise the physical key wrapper is `internal_key`'s job:
//! [`encode_primary_key`] produces a **user key**, and the caller hands
//! it to `internal_key::encode` with the tenant/database/keyspace
//! triple. One encoder per concern, composed — never reimplemented.
//!
//! ## 3. Schema change is a machine, not a flag
//!
//! §14.5:
//!
//! > Schema changes are resumable state machines recorded through
//! > Lattice. Publication uses versioned descriptors so old and new
//! > binaries can identify compatible read and write phases.
//!
//! [`SchemaPhase`] is the classic safe sequence
//! `Absent → DeleteOnly → WriteOnly → Backfilling → Public`, and the
//! property that makes it safe is executable: [`adjacent_phases_safe`]
//! asserts that **no phase permits an index read unless the phase
//! before it already required full index maintenance**. If it did, a
//! reader on a new binary could consult an index that a writer on an
//! old binary was not yet obliged to update, and the read would miss a
//! committed row with no way to detect it. [`SchemaJob::advance`]
//! additionally refuses to move until the *slowest* node in the fleet
//! is known to be at-or-past the current phase — the fleet minimum, not
//! the local view, is the gate.
//!
//! ## 4. Fail closed, everywhere
//!
//! Unknown version, unknown type tag, unknown enum discriminant,
//! out-of-bounds length, non-ascending column ids, a decimal that would
//! lose a nonzero digit on rescale, a NaN in key position, an
//! unevaluated enforced CHECK constraint: all refusals, none of them
//! best-effort. §14.3's closing sentence is the governing style —
//! behavior that cannot be represented honestly is *rejected*, never
//! silently approximated.
//!
//! Golden vectors: `tests/contract_relational.rs`. Rows, keys, and
//! descriptors are persistent formats: if a change here makes a vector
//! fail, that change is a format break requiring the corresponding
//! version bump plus an explicit migration — never an update to the
//! vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

// Only `db_ops` is mounted. It already mounts `internal_key` (and
// `range_lifecycle` → `partition_map` → `db_context`) beneath it, so a
// second mount here would be the same file in two module paths. The
// escape/terminator constants this file needs are re-declared below
// with a documented byte-identity requirement instead.
#[path = "db_ops.rs"]
mod db_ops;

#[allow(
    unused_imports,
    reason = "re-export surface: connectors compose the relational layer with the index layer beneath it without a second #[path] include"
)]
pub use db_ops::{
    check_unique_ownership, decode_index_entry, encode_index_entry, index_prefix_for,
    index_satisfies_freshness, index_user_key, index_user_key_primary, index_user_prefix,
    DecodedIndexEntry, DecodedKey, FreshnessError, IndexEntry, IndexExactness, IndexFreshness,
    Timestamp, UniqueOutcome, UniqueOwnership, ValueKind, INDEX_ENTRY_MAX_WIRE_LEN, INDEX_ID_LEN,
    MAX_INDEX_VALUE_LEN, MAX_PRIMARY_KEY_LEN,
};

use core::cmp::Ordering;

// ══════════════════════════════════════════════════════════════════════
// 0. Bounds and format versions
// ══════════════════════════════════════════════════════════════════════

/// Version of the [`encode_row`] payload layout.
pub const ROW_FORMAT_VERSION: u16 = 1;
/// Version of the key composites ([`encode_primary_key`],
/// [`encode_index_value`]). Keys carry no inline version byte — §9.1
/// records key-format versions in manifests, not in every key — so this
/// constant is published in [`TableDescriptor::format_generation`] and
/// a bump is a full table rewrite.
pub const KEY_FORMAT_GENERATION: u16 = 1;
/// Version of the [`DatabaseDescriptor`] wire encoding.
pub const DATABASE_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the [`TableDescriptor`] wire encoding.
pub const TABLE_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the [`IndexDescriptor`] wire encoding.
pub const INDEX_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the [`SequenceDescriptor`] wire encoding.
pub const SEQUENCE_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the [`ForeignKeyDescriptor`] wire encoding.
pub const FOREIGN_KEY_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the [`SchemaJob`] wire encoding.
pub const SCHEMA_JOB_VERSION: u16 = 1;

/// Maximum bytes in an object name (database, table, column, index,
/// sequence). One byte of length prefix, so 255 is the hard ceiling;
/// 64 is the declared bound.
pub const MAX_NAME_LEN: usize = 64;
/// Maximum columns in one table.
pub const MAX_COLUMNS: usize = 64;
/// Maximum columns in one key tuple: primary key, index key, or
/// foreign key. Eight is the bound every key-shaped array here uses.
pub const MAX_KEY_COLUMNS: usize = 8;
/// Maximum bytes in one `CHAR`/`VARCHAR`/`BINARY`/`VARBINARY` value.
///
/// 120 rather than a round number because of the key budget: a single
/// text column in key position costs `1 + 2*120 + 2 = 243` bytes
/// worst-case (null marker, every byte escaped, terminator), and with
/// the 4-byte table id that is 247 — just inside
/// [`MAX_PRIMARY_KEY_LEN`] (256, itself `MAX_KEY_BOUND_LEN`, because a
/// primary key that cannot be a range bound cannot be indexed).
pub const MAX_TEXT_LEN: usize = 120;
/// Maximum decimal precision (significant digits). 38 is the largest
/// precision that fits an `i128` unscaled value.
pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// Wire width of an encoded [`LogicalType`]: tag plus two parameter
/// bytes.
pub const TYPE_WIRE_LEN: usize = 3;

/// Worst-case bytes of one *ordered* (key-position) encoded value:
/// null marker, every text byte escaped to two, component terminator.
pub const MAX_ORDERED_VALUE_LEN: usize = 1 + 2 * MAX_TEXT_LEN + 2;
/// Worst-case bytes of one *plain* (payload-position) encoded value:
/// null marker, `u16` length prefix, text bytes.
pub const MAX_PLAIN_VALUE_LEN: usize = 1 + 2 + MAX_TEXT_LEN;

/// Fixed header of a row payload: version, payload length, column
/// count.
pub const ROW_HEADER_LEN: usize = 2 + 2 + 2;
/// Worst-case bytes of one row column entry: column id, type, value
/// length, plain value.
pub const ROW_ENTRY_MAX_LEN: usize = 2 + TYPE_WIRE_LEN + 2 + MAX_PLAIN_VALUE_LEN;
/// Hard cap on an encoded row, bytes. Matches the canonical value bound
/// the KV layer accepts (`kv_store::MAX_VALUE_LEN`); a row that does
/// not fit cannot be stored, and v1 has no overflow/TOAST path, so
/// [`encode_row`] fails closed rather than splitting silently.
pub const MAX_ROW_BYTES: usize = 4096;

/// Worst-case bytes of one probe key in an [`FkProbePlan`].
pub const MAX_PROBE_KEY_LEN: usize = MAX_PRIMARY_KEY_LEN;
/// Maximum probes one foreign-key plan emits. Two, because a parent-key
/// UPDATE must probe the child index under **both** the old key value
/// (children that would be orphaned) and the new key value (rows that
/// silently become children). Every other plan emits one.
pub const MAX_FK_PROBES: usize = 2;

/// Escape sequence for a literal `0x00` inside an ordered text value.
/// Byte-identical to `internal_key::KEY_ESCAPE` by requirement, not by
/// coincidence: an ordered value becomes part of a user key, so all
/// three levels (row key composite, index composite, internal key) must
/// agree that `0x00` is the only special byte. The golden vectors pin
/// the identity.
const ESC: [u8; 2] = [0x00, 0xFF];
/// Component terminator inside an ordered text value. Byte-identical to
/// `internal_key::KEY_TERMINATOR`. Terminator (`0x01`) sorts below
/// escape (`0xFF`), which is what makes a proper prefix sort before its
/// extensions — index value `"a"` never sweeps up `"ab"`.
const TERM: [u8; 2] = [0x00, 0x01];

/// Ordered null marker. `0x00` sorts below the present marker, so
/// Lattice's declared key order is **NULLS FIRST**. A connector whose
/// dialect defaults to NULLS LAST reverses in the planner, never in the
/// encoder: the physical order is one thing for both connectors (§14.3).
pub const ORDERED_NULL: u8 = 0x00;
/// Ordered present marker; the type's body follows.
pub const ORDERED_PRESENT: u8 = 0x01;

// ══════════════════════════════════════════════════════════════════════
// 1. Logical type system (§14.2 "logical types and explicit connector
//    type mappings")
// ══════════════════════════════════════════════════════════════════════

/// The closed shared SQL type set. Closed is the point: a connector
/// that meets a dialect type outside this set maps it to one of these
/// or reports it unsupported (§14.3), and it may not extend the set on
/// its own — an extension is a version bump here, visible to both.
///
/// Wire form, [`TYPE_WIRE_LEN`] bytes:
///
/// ```text
/// [tag:u8][param_a:u8][param_b:u8]
///
/// Decimal                          a = precision (1..=38)
///                                  b = scale     (0..=precision)
/// Char/VarChar/Binary/VarBinary    (a,b) = declared length, u16 LE
/// every other tag                  a = b = 0   (anything else rejected)
/// ```
///
/// Temporal and numeric semantics are fixed here so the two connectors
/// cannot disagree about them:
///
/// - `Date` is **days** since 1970-01-01, signed.
/// - `Time` is **microseconds** since midnight, `0 .. 86_400_000_000`.
/// - `Timestamp` is microseconds since 1970-01-01T00:00:00, signed, with
///   no zone attached.
/// - `TimestampTz` is the same integer, already normalized to UTC. The
///   session time zone is connector-owned presentation state and never
///   reaches storage (§14.3).
/// - `Char` is **blank-padded to its declared length at encode time**,
///   which is the one storage form both dialects agree on; trimming on
///   output is connector semantics.
/// - `Binary` is `0x00`-padded to its declared length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalType {
    /// The type of an untyped `NULL` literal before binding. Only
    /// [`Value::Null`] inhabits it, and it may not appear in a
    /// descriptor — it exists so a binder can carry an unresolved
    /// literal through the same value machinery.
    Null,
    Boolean,
    SmallInt,
    Int,
    BigInt,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Float,
    Double,
    Char {
        length: u16,
    },
    VarChar {
        length: u16,
    },
    Binary {
        length: u16,
    },
    VarBinary {
        length: u16,
    },
    Date,
    Time,
    Timestamp,
    TimestampTz,
}

/// Type tags. Stable wire values; never renumber.
pub const TAG_NULL: u8 = 0x01;
pub const TAG_BOOLEAN: u8 = 0x02;
pub const TAG_SMALLINT: u8 = 0x03;
pub const TAG_INT: u8 = 0x04;
pub const TAG_BIGINT: u8 = 0x05;
pub const TAG_DECIMAL: u8 = 0x06;
pub const TAG_FLOAT: u8 = 0x07;
pub const TAG_DOUBLE: u8 = 0x08;
pub const TAG_CHAR: u8 = 0x09;
pub const TAG_VARCHAR: u8 = 0x0A;
pub const TAG_BINARY: u8 = 0x0B;
pub const TAG_VARBINARY: u8 = 0x0C;
pub const TAG_DATE: u8 = 0x0D;
pub const TAG_TIME: u8 = 0x0E;
pub const TAG_TIMESTAMP: u8 = 0x0F;
pub const TAG_TIMESTAMPTZ: u8 = 0x10;

/// Microseconds in a day; the exclusive upper bound of [`LogicalType::Time`].
pub const TIME_MICROS_PER_DAY: i64 = 86_400_000_000;

impl LogicalType {
    /// The wire tag. A plain `match` rather than a discriminant cast
    /// because the parameterized variants carry data.
    pub const fn tag(self) -> u8 {
        match self {
            Self::Null => TAG_NULL,
            Self::Boolean => TAG_BOOLEAN,
            Self::SmallInt => TAG_SMALLINT,
            Self::Int => TAG_INT,
            Self::BigInt => TAG_BIGINT,
            Self::Decimal { .. } => TAG_DECIMAL,
            Self::Float => TAG_FLOAT,
            Self::Double => TAG_DOUBLE,
            Self::Char { .. } => TAG_CHAR,
            Self::VarChar { .. } => TAG_VARCHAR,
            Self::Binary { .. } => TAG_BINARY,
            Self::VarBinary { .. } => TAG_VARBINARY,
            Self::Date => TAG_DATE,
            Self::Time => TAG_TIME,
            Self::Timestamp => TAG_TIMESTAMP,
            Self::TimestampTz => TAG_TIMESTAMPTZ,
        }
    }

    /// Declared length for the length-carrying types.
    pub const fn declared_length(self) -> Option<u16> {
        match self {
            Self::Char { length }
            | Self::VarChar { length }
            | Self::Binary { length }
            | Self::VarBinary { length } => Some(length),
            _ => None,
        }
    }

    /// Is this a fixed-width type whose ordered encoding is padded to
    /// the declared length? `CHAR` and `BINARY` are; the `VAR*` forms
    /// are not.
    pub const fn is_blank_padded(self) -> bool {
        matches!(self, Self::Char { .. } | Self::Binary { .. })
    }

    /// Byte-oriented types accept [`Value::Text`] (character) or
    /// [`Value::Bytes`] (binary); everything else is scalar.
    pub const fn is_character(self) -> bool {
        matches!(self, Self::Char { .. } | Self::VarChar { .. })
    }

    pub const fn is_binary(self) -> bool {
        matches!(self, Self::Binary { .. } | Self::VarBinary { .. })
    }

    /// Structural validity. Checked at construction *and* on decode, so
    /// a descriptor cannot smuggle in a `DECIMAL(0,5)` or a
    /// `VARCHAR(100000)` that later overflows a fixed buffer.
    pub const fn is_valid(self) -> bool {
        match self {
            Self::Decimal { precision, scale } => {
                precision >= 1 && precision <= MAX_DECIMAL_PRECISION && scale <= precision
            }
            Self::Char { length }
            | Self::VarChar { length }
            | Self::Binary { length }
            | Self::VarBinary { length } => length >= 1 && length as usize <= MAX_TEXT_LEN,
            _ => true,
        }
    }

    /// Encode into [`TYPE_WIRE_LEN`] bytes. Fails closed on a
    /// structurally invalid type — an invalid type must never reach the
    /// wire, where a later reader would have to guess.
    pub fn encode(self, out: &mut [u8]) -> Option<usize> {
        if out.len() < TYPE_WIRE_LEN || !self.is_valid() {
            return None;
        }
        out[0] = self.tag();
        match self {
            Self::Decimal { precision, scale } => {
                out[1] = precision;
                out[2] = scale;
            }
            Self::Char { length }
            | Self::VarChar { length }
            | Self::Binary { length }
            | Self::VarBinary { length } => {
                let b = length.to_le_bytes();
                out[1] = b[0];
                out[2] = b[1];
            }
            _ => {
                out[1] = 0;
                out[2] = 0;
            }
        }
        Some(TYPE_WIRE_LEN)
    }

    /// Parse [`TYPE_WIRE_LEN`] bytes. Fails closed on an unknown tag,
    /// on nonzero parameter bytes for a parameterless type (a future
    /// version might give them meaning, and guessing now is how formats
    /// rot), and on a structurally invalid parameterization.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < TYPE_WIRE_LEN {
            return None;
        }
        let (a, b) = (src[1], src[2]);
        let len = u16::from_le_bytes([a, b]);
        let plain = |t: Self| -> Option<Self> {
            if a == 0 && b == 0 {
                Some(t)
            } else {
                None
            }
        };
        let ty = match src[0] {
            TAG_NULL => plain(Self::Null)?,
            TAG_BOOLEAN => plain(Self::Boolean)?,
            TAG_SMALLINT => plain(Self::SmallInt)?,
            TAG_INT => plain(Self::Int)?,
            TAG_BIGINT => plain(Self::BigInt)?,
            TAG_DECIMAL => Self::Decimal {
                precision: a,
                scale: b,
            },
            TAG_FLOAT => plain(Self::Float)?,
            TAG_DOUBLE => plain(Self::Double)?,
            TAG_CHAR => Self::Char { length: len },
            TAG_VARCHAR => Self::VarChar { length: len },
            TAG_BINARY => Self::Binary { length: len },
            TAG_VARBINARY => Self::VarBinary { length: len },
            TAG_DATE => plain(Self::Date)?,
            TAG_TIME => plain(Self::Time)?,
            TAG_TIMESTAMP => plain(Self::Timestamp)?,
            TAG_TIMESTAMPTZ => plain(Self::TimestampTz)?,
            _ => return None,
        };
        if ty.is_valid() {
            Some(ty)
        } else {
            None
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// 2. Values
// ══════════════════════════════════════════════════════════════════════

/// A bound relational value.
///
/// Borrowed rather than owned for the byte-carrying variants: a `Value`
/// is a transient argument to an encoder or a view into a decoded row,
/// never a stored record, so inlining a 120-byte array into every
/// variant would cost every caller for the benefit of none. Zero alloc
/// either way.
///
/// Temporal values ride the integer variants with a declared unit —
/// `Date` is [`Value::Int`] days, `Time`/`Timestamp`/`TimestampTz` are
/// [`Value::BigInt`] microseconds. That is deliberate: they *are*
/// integers with an epoch, and giving them separate variants would
/// invite a connector to invent a second representation.
///
/// `PartialEq` is derived, so `Double(NAN) != Double(NAN)` — IEEE
/// semantics. [`logical_cmp`] is the *total* order, and it disagrees
/// with `==` on `-0.0` vs `+0.0` on purpose; see its docs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    Null,
    Boolean(bool),
    SmallInt(i16),
    /// Also `DATE` (days since 1970-01-01).
    Int(i32),
    /// Also `TIME` / `TIMESTAMP` / `TIMESTAMPTZ` (microseconds).
    BigInt(i64),
    Decimal {
        unscaled: i128,
        scale: u8,
    },
    Float(f32),
    Double(f64),
    /// `CHAR` / `VARCHAR` payload. UTF-8 is *not* validated here;
    /// collation and encoding validation are connector concerns and
    /// this layer must stay byte-exact.
    Text(&'a [u8]),
    /// `BINARY` / `VARBINARY` payload.
    Bytes(&'a [u8]),
}

impl Value<'_> {
    pub const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    /// Does this value inhabit `ty`? Checked before every encode, so a
    /// type/value mismatch is a refusal rather than a reinterpretation.
    pub fn matches_type(self, ty: LogicalType) -> bool {
        match (self, ty) {
            (Self::Null, _) => true,
            (Self::Boolean(_), LogicalType::Boolean) => true,
            (Self::SmallInt(_), LogicalType::SmallInt) => true,
            (Self::Int(_), LogicalType::Int | LogicalType::Date) => true,
            (
                Self::BigInt(_),
                LogicalType::BigInt
                | LogicalType::Time
                | LogicalType::Timestamp
                | LogicalType::TimestampTz,
            ) => true,
            (Self::Decimal { .. }, LogicalType::Decimal { .. }) => true,
            (Self::Float(_), LogicalType::Float) => true,
            (Self::Double(_), LogicalType::Double) => true,
            (Self::Text(_), t) if t.is_character() => true,
            (Self::Bytes(_), t) if t.is_binary() => true,
            _ => false,
        }
    }
}

/// `10^n` as `i128`, or `None` on overflow.
const fn pow10(n: u32) -> Option<i128> {
    if n > 38 {
        return None;
    }
    let mut acc: i128 = 1;
    let mut i = 0;
    while i < n {
        acc = match acc.checked_mul(10) {
            Some(v) => v,
            None => return None,
        };
        i += 1;
    }
    Some(acc)
}

/// Rescale a decimal to `target` scale **exactly**.
///
/// Scaling up multiplies (checked). Scaling down is permitted only when
/// every discarded digit is zero: a decimal that would lose a nonzero
/// digit is refused, never rounded. §14.3 — behavior that cannot be
/// represented honestly is rejected, not approximated. Rounding mode is
/// exactly the kind of thing PostgreSQL and MySQL disagree about, so
/// the shared layer refuses to have one.
pub fn rescale_decimal(unscaled: i128, scale: u8, target: u8) -> Option<i128> {
    match scale.cmp(&target) {
        Ordering::Equal => Some(unscaled),
        Ordering::Less => unscaled.checked_mul(pow10(u32::from(target - scale))?),
        Ordering::Greater => {
            let steps = u32::from(scale - target);
            if steps > u32::from(MAX_DECIMAL_PRECISION) {
                return None;
            }
            let neg = unscaled < 0;
            // Take the magnitude via wrapping_neg so i128::MIN — whose
            // positive is not representable — does not overflow. Its
            // magnitude is exactly the u128 bit pattern.
            let mut mag = if neg {
                (unscaled.wrapping_neg()) as u128
            } else {
                unscaled as u128
            };
            for _ in 0..steps {
                let (q, r) = divmod10_u128(mag);
                if r != 0 {
                    // Rescaling down must not lose digits: a value that
                    // does not divide exactly cannot be represented at
                    // the target scale, and rounding it would be a
                    // silent change of value that PostgreSQL and MySQL
                    // do not even agree on the direction of.
                    return None;
                }
                mag = q;
            }
            let out = i128::try_from(mag).ok()?;
            Some(if neg { -out } else { out })
        }
    }
}

/// `(n / 10, n % 10)` for a `u128`, using only 64-bit arithmetic.
///
/// Exists because a PIC module links no compiler-rt: `u128 / u128`
/// lowers to a `__udivti3` call that is simply absent, and the failure
/// is a link error in whatever module first touches a decimal key. Long
/// division over four 32-bit limbs needs nothing but `u64` divides,
/// which the hardware has.
///
/// Exact by construction: at each limb the partial dividend is
/// `rem·2³² + limb` with `rem < 10`, so it fits a `u64` and the quotient
/// digit fits a `u32`.
const fn divmod10_u128(n: u128) -> (u128, u32) {
    let mut rem: u64 = 0;
    let mut q: u128 = 0;
    let mut shift: i32 = 96;
    while shift >= 0 {
        let limb = ((n >> shift) & 0xFFFF_FFFF) as u64;
        let cur = (rem << 32) | limb;
        q |= ((cur / 10) as u128) << shift;
        rem = cur % 10;
        shift -= 32;
    }
    (q, rem as u32)
}

/// IEEE-754 `totalOrder`-compatible transform for `f64`: the returned
/// `u64`, compared as an unsigned big-endian integer, orders finite
/// values, both zeros, and both infinities the way the reals do.
///
/// Negative values have their whole bit pattern inverted (which
/// reverses the magnitude ordering that the sign-magnitude layout would
/// otherwise give); non-negative values get the sign bit set so they
/// sort above every negative. `-0.0` maps to `0x7FFF_FFFF_FFFF_FFFF`
/// and `+0.0` to `0x8000_0000_0000_0000`, so **the two zeros are
/// distinct and `-0.0` sorts first**. NaN has no place in a total order
/// the planner can rely on, so it is excluded here and rejected by
/// [`encode_value_ordered`]; NaN *is* storable in payload position.
const fn f64_order_bits(v: f64) -> Option<u64> {
    if v.is_nan() {
        return None;
    }
    let b = v.to_bits();
    Some(if b & 0x8000_0000_0000_0000 != 0 {
        !b
    } else {
        b ^ 0x8000_0000_0000_0000
    })
}

/// Inverse of [`f64_order_bits`].
const fn f64_from_order_bits(m: u64) -> f64 {
    let b = if m & 0x8000_0000_0000_0000 != 0 {
        m ^ 0x8000_0000_0000_0000
    } else {
        !m
    };
    f64::from_bits(b)
}

/// `f32` twin of [`f64_order_bits`].
const fn f32_order_bits(v: f32) -> Option<u32> {
    if v.is_nan() {
        return None;
    }
    let b = v.to_bits();
    Some(if b & 0x8000_0000 != 0 {
        !b
    } else {
        b ^ 0x8000_0000
    })
}

const fn f32_from_order_bits(m: u32) -> f32 {
    let b = if m & 0x8000_0000 != 0 {
        m ^ 0x8000_0000
    } else {
        !m
    };
    f32::from_bits(b)
}

/// The logical total order over values of the *same* type family.
///
/// This is the order [`encode_value_ordered`] claims to preserve, and
/// the property test compares the two directly. Three rules worth
/// naming:
///
/// - `NULL` sorts before every non-null value (NULLS FIRST, §1 of this
///   file's docs).
/// - `-0.0 < +0.0`. IEEE `==` says they are equal; a byte encoding
///   cannot make two distinct bit patterns equal, so the *order* is the
///   thing that gets adjusted, and it is adjusted consistently in both
///   the encoder and here.
/// - Decimals of different scales are compared after exact rescaling to
///   the larger scale. If that overflows, the values are incomparable
///   and this returns `None` — the same refusal the encoder makes.
///
/// Returns `None` for cross-family comparisons and for NaN.
pub fn logical_cmp(a: Value<'_>, b: Value<'_>) -> Option<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Null, _) => Some(Ordering::Less),
        (_, Value::Null) => Some(Ordering::Greater),
        (Value::Boolean(x), Value::Boolean(y)) => Some(x.cmp(&y)),
        (Value::SmallInt(x), Value::SmallInt(y)) => Some(x.cmp(&y)),
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(&y)),
        (Value::BigInt(x), Value::BigInt(y)) => Some(x.cmp(&y)),
        (
            Value::Decimal {
                unscaled: xu,
                scale: xs,
            },
            Value::Decimal {
                unscaled: yu,
                scale: ys,
            },
        ) => {
            let target = if xs > ys { xs } else { ys };
            let x = rescale_decimal(xu, xs, target)?;
            let y = rescale_decimal(yu, ys, target)?;
            Some(x.cmp(&y))
        }
        (Value::Float(x), Value::Float(y)) => Some(f32_order_bits(x)?.cmp(&f32_order_bits(y)?)),
        (Value::Double(x), Value::Double(y)) => Some(f64_order_bits(x)?.cmp(&f64_order_bits(y)?)),
        (Value::Text(x), Value::Text(y)) | (Value::Bytes(x), Value::Bytes(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// ══════════════════════════════════════════════════════════════════════
// 3. Ordered (key-position) value encoding
// ══════════════════════════════════════════════════════════════════════

/// Append `src` to `out` at `n`, escaping `0x00` as `0x00 0xFF`.
/// Fails closed rather than truncating — a truncated key sorts wrong
/// and there is no later check that would notice.
fn esc_into(out: &mut [u8], mut n: usize, src: &[u8]) -> Option<usize> {
    for &b in src {
        if b == 0x00 {
            if out.len() < n + 2 {
                return None;
            }
            out[n] = ESC[0];
            out[n + 1] = ESC[1];
            n += 2;
        } else {
            if out.len() < n + 1 {
                return None;
            }
            out[n] = b;
            n += 1;
        }
    }
    Some(n)
}

/// Append `count` copies of `pad`, escaping as [`esc_into`] would.
fn esc_pad(out: &mut [u8], mut n: usize, pad: u8, count: usize) -> Option<usize> {
    for _ in 0..count {
        n = esc_into(out, n, &[pad])?;
    }
    Some(n)
}

fn term_into(out: &mut [u8], n: usize) -> Option<usize> {
    if out.len() < n + 2 {
        return None;
    }
    out[n] = TERM[0];
    out[n + 1] = TERM[1];
    Some(n + 2)
}

/// Unescape one terminator-delimited component of `src` starting at
/// `i`. Returns `(bytes_written, index just past the terminator)`.
/// Fails closed on a missing terminator, an unknown escape, or a
/// component larger than `out`.
fn unesc_component(src: &[u8], mut i: usize, out: &mut [u8]) -> Option<(usize, usize)> {
    let mut k = 0usize;
    loop {
        if i + 1 >= src.len() {
            return None;
        }
        match (src[i], src[i + 1]) {
            (0x00, 0x01) => return Some((k, i + 2)),
            (0x00, 0xFF) => {
                if k >= out.len() {
                    return None;
                }
                out[k] = 0x00;
                k += 1;
                i += 2;
            }
            (0x00, _) => return None,
            (b, _) => {
                if k >= out.len() {
                    return None;
                }
                out[k] = b;
                k += 1;
                i += 1;
            }
        }
    }
}

fn put(out: &mut [u8], n: usize, src: &[u8]) -> Option<usize> {
    if out.len() < n + src.len() {
        return None;
    }
    out[n..n + src.len()].copy_from_slice(src);
    Some(n + src.len())
}

/// Encode `v` as `ty` in **key position**: the returned bytes, compared
/// bytewise against another value of the same type, give the same
/// answer as [`logical_cmp`].
///
/// Layout, after the one-byte null marker
/// ([`ORDERED_NULL`] / [`ORDERED_PRESENT`]):
///
/// ```text
/// Null           (no body; the marker is the whole encoding)
/// Boolean        [0x00|0x01]
/// SmallInt       [i16 BE with sign bit flipped]                2 bytes
/// Int, Date      [i32 BE with sign bit flipped]                4 bytes
/// BigInt,        [i64 BE with sign bit flipped]                8 bytes
///   Timestamp,
///   TimestampTz
/// Time           [u64 BE, no flip: always 0..86_400_000_000]   8 bytes
/// Decimal(p,s)   [i128 BE with sign bit flipped]              16 bytes
///                (value first rescaled EXACTLY to s)
/// Float          [f32 total-order bits, BE]                    4 bytes
/// Double         [f64 total-order bits, BE]                    8 bytes
/// Char(n)        [esc(value blank-padded to n)][0x00 0x01]
/// Binary(n)      [esc(value 0x00-padded to n)][0x00 0x01]
/// VarChar(n),    [esc(value)][0x00 0x01]
///   VarBinary(n)
/// ```
///
/// Why the sign flip: two's complement puts negative integers *above*
/// positives under unsigned byte comparison (`-1` is `0xFF..`). XORing
/// the sign bit maps the signed range onto the unsigned range
/// monotonically, so `-1` becomes `0x7F..` and sorts below `0` at
/// `0x80..`. Same trick, wider, for `i128` decimals.
///
/// Why escaping rather than a length prefix for text: a length prefix
/// sorts `"z"` before `"aa"` because 1 < 2. Escaping with a terminator
/// that sorts below every escaped byte preserves lexical order *and*
/// makes no encoded value a prefix of another, which is what lets a
/// composite key be a plain concatenation (§9.1's prefix-ambiguity
/// rule, applied one level up).
///
/// Refusals: a value that does not inhabit `ty`, a text/binary value
/// longer than the declared length or than [`MAX_TEXT_LEN`], a decimal
/// that will not rescale exactly or exceeds the declared precision, a
/// `Time` outside its day, a NaN, or an `out` too small.
pub fn encode_value_ordered(out: &mut [u8], ty: LogicalType, v: Value<'_>) -> Option<usize> {
    if !ty.is_valid() || !v.matches_type(ty) {
        return None;
    }
    if out.is_empty() {
        return None;
    }
    if v.is_null() {
        out[0] = ORDERED_NULL;
        return Some(1);
    }
    out[0] = ORDERED_PRESENT;
    let n = 1;
    match (ty, v) {
        (LogicalType::Boolean, Value::Boolean(b)) => put(out, n, &[u8::from(b)]),
        (LogicalType::SmallInt, Value::SmallInt(x)) => {
            put(out, n, &((x as u16) ^ 0x8000).to_be_bytes())
        }
        (LogicalType::Int | LogicalType::Date, Value::Int(x)) => {
            put(out, n, &((x as u32) ^ 0x8000_0000).to_be_bytes())
        }
        (
            LogicalType::BigInt | LogicalType::Timestamp | LogicalType::TimestampTz,
            Value::BigInt(x),
        ) => put(out, n, &((x as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()),
        (LogicalType::Time, Value::BigInt(x)) => {
            if !(0..TIME_MICROS_PER_DAY).contains(&x) {
                return None;
            }
            put(out, n, &(x as u64).to_be_bytes())
        }
        (LogicalType::Decimal { precision, scale }, Value::Decimal { unscaled, scale: s }) => {
            let u = rescale_decimal(unscaled, s, scale)?;
            let limit = pow10(u32::from(precision))?;
            if u >= limit || u <= -limit {
                return None;
            }
            put(out, n, &((u as u128) ^ (1u128 << 127)).to_be_bytes())
        }
        (LogicalType::Float, Value::Float(x)) => put(out, n, &f32_order_bits(x)?.to_be_bytes()),
        (LogicalType::Double, Value::Double(x)) => put(out, n, &f64_order_bits(x)?.to_be_bytes()),
        (LogicalType::Char { length } | LogicalType::VarChar { length }, Value::Text(b))
        | (LogicalType::Binary { length } | LogicalType::VarBinary { length }, Value::Bytes(b)) => {
            let declared = length as usize;
            if b.len() > declared || b.len() > MAX_TEXT_LEN {
                return None;
            }
            let mut k = esc_into(out, n, b)?;
            if ty.is_blank_padded() {
                let pad = if ty.is_character() { 0x20 } else { 0x00 };
                k = esc_pad(out, k, pad, declared - b.len())?;
            }
            term_into(out, k)
        }
        _ => None,
    }
}

/// A decoded key-position value plus the bytes it consumed.
///
/// Text and binary values borrow `out`, the caller-supplied unescape
/// buffer; scalars write nothing to it.
pub fn decode_value_ordered<'b>(
    src: &[u8],
    ty: LogicalType,
    out: &'b mut [u8],
) -> Option<(Value<'b>, usize)> {
    if !ty.is_valid() || src.is_empty() {
        return None;
    }
    match src[0] {
        ORDERED_NULL => return Some((Value::Null, 1)),
        ORDERED_PRESENT => {}
        _ => return None,
    }
    let body = src.get(1..)?;
    let fixed = |w: usize| -> Option<&[u8]> { body.get(..w) };
    match ty {
        LogicalType::Null => None, // a Null-typed column can only be NULL
        LogicalType::Boolean => match *fixed(1)?.first()? {
            0x00 => Some((Value::Boolean(false), 2)),
            0x01 => Some((Value::Boolean(true), 2)),
            _ => None,
        },
        LogicalType::SmallInt => {
            let x = u16::from_be_bytes(fixed(2)?.try_into().ok()?) ^ 0x8000;
            Some((Value::SmallInt(x as i16), 3))
        }
        LogicalType::Int | LogicalType::Date => {
            let x = u32::from_be_bytes(fixed(4)?.try_into().ok()?) ^ 0x8000_0000;
            Some((Value::Int(x as i32), 5))
        }
        LogicalType::BigInt | LogicalType::Timestamp | LogicalType::TimestampTz => {
            let x = u64::from_be_bytes(fixed(8)?.try_into().ok()?) ^ 0x8000_0000_0000_0000;
            Some((Value::BigInt(x as i64), 9))
        }
        LogicalType::Time => {
            let x = u64::from_be_bytes(fixed(8)?.try_into().ok()?);
            if x >= TIME_MICROS_PER_DAY as u64 {
                return None;
            }
            Some((Value::BigInt(x as i64), 9))
        }
        LogicalType::Decimal { precision, scale } => {
            let x = u128::from_be_bytes(fixed(16)?.try_into().ok()?) ^ (1u128 << 127);
            let u = x as i128;
            let limit = pow10(u32::from(precision))?;
            if u >= limit || u <= -limit {
                return None;
            }
            Some((Value::Decimal { unscaled: u, scale }, 17))
        }
        LogicalType::Float => {
            let m = u32::from_be_bytes(fixed(4)?.try_into().ok()?);
            Some((Value::Float(f32_from_order_bits(m)), 5))
        }
        LogicalType::Double => {
            let m = u64::from_be_bytes(fixed(8)?.try_into().ok()?);
            Some((Value::Double(f64_from_order_bits(m)), 9))
        }
        LogicalType::Char { length }
        | LogicalType::VarChar { length }
        | LogicalType::Binary { length }
        | LogicalType::VarBinary { length } => {
            let (k, end) = unesc_component(src, 1, out)?;
            if k > length as usize || k > MAX_TEXT_LEN {
                return None;
            }
            if ty.is_blank_padded() && k != length as usize {
                return None; // padded types are always exactly `length`
            }
            let bytes = out.get(..k)?;
            let v = if ty.is_character() {
                Value::Text(bytes)
            } else {
                Value::Bytes(bytes)
            };
            Some((v, end))
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// 4. Plain (payload-position) value encoding
// ══════════════════════════════════════════════════════════════════════

/// Encode `v` as `ty` in **payload position**. Nothing sorts these
/// bytes, so the encoding optimizes for size and decode cost instead of
/// order:
///
/// ```text
/// [null marker 0x00]                                       NULL
/// [0x01][body]                                             present
///
/// Boolean        [0x00|0x01]                                1
/// SmallInt       [i16 LE, raw two's complement]             2
/// Int, Date      [i32 LE, raw]                              4
/// BigInt, Time,  [i64 LE, raw]                              8
///   Timestamp,
///   TimestampTz
/// Decimal        [scale:u8][i128 LE unscaled, raw]         17
/// Float          [f32 bits LE]                              4
/// Double         [f64 bits LE]                              8
/// Char/VarChar/  [len:u16 LE][bytes]                     2 + n
///   Binary/VarBinary
/// ```
///
/// Three differences from [`encode_value_ordered`] and the reason for
/// each:
///
/// - **No sign flip.** The flip exists to make bytewise comparison
///   agree with signed comparison. Nothing compares these bytes, so the
///   flip would be pure cost, and its absence is a useful tripwire: a
///   payload accidentally used as a key sorts visibly wrong on the
///   first negative number rather than subtly wrong later.
/// - **Length prefix instead of escaping.** Cheaper to write and to
///   skip, and the "short sorts first" defect is irrelevant here. Being
///   skippable by length is what makes schema evolution work — see
///   [`encode_row`].
/// - **NaN is accepted, decimals keep their own scale.** A column may
///   legitimately hold a NaN or a value at a different scale than the
///   column declares; only *key* position needs a total order and an
///   exact scale. This is the honest place to store what the user
///   actually wrote.
pub fn encode_value_plain(out: &mut [u8], ty: LogicalType, v: Value<'_>) -> Option<usize> {
    if !ty.is_valid() || !v.matches_type(ty) || out.is_empty() {
        return None;
    }
    if v.is_null() {
        out[0] = ORDERED_NULL;
        return Some(1);
    }
    out[0] = ORDERED_PRESENT;
    let n = 1;
    match (ty, v) {
        (LogicalType::Boolean, Value::Boolean(b)) => put(out, n, &[u8::from(b)]),
        (LogicalType::SmallInt, Value::SmallInt(x)) => put(out, n, &x.to_le_bytes()),
        (LogicalType::Int | LogicalType::Date, Value::Int(x)) => put(out, n, &x.to_le_bytes()),
        (
            LogicalType::BigInt
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz,
            Value::BigInt(x),
        ) => put(out, n, &x.to_le_bytes()),
        (LogicalType::Decimal { .. }, Value::Decimal { unscaled, scale }) => {
            let k = put(out, n, &[scale])?;
            put(out, k, &unscaled.to_le_bytes())
        }
        (LogicalType::Float, Value::Float(x)) => put(out, n, &x.to_bits().to_le_bytes()),
        (LogicalType::Double, Value::Double(x)) => put(out, n, &x.to_bits().to_le_bytes()),
        (LogicalType::Char { length } | LogicalType::VarChar { length }, Value::Text(b))
        | (LogicalType::Binary { length } | LogicalType::VarBinary { length }, Value::Bytes(b)) => {
            if b.len() > length as usize || b.len() > MAX_TEXT_LEN {
                return None;
            }
            let k = put(out, n, &(b.len() as u16).to_le_bytes())?;
            put(out, k, b)
        }
        _ => None,
    }
}

/// Parse a payload-position value. Text and binary values borrow `src`
/// directly — no copy, no buffer.
pub fn decode_value_plain(src: &[u8], ty: LogicalType) -> Option<(Value<'_>, usize)> {
    if !ty.is_valid() || src.is_empty() {
        return None;
    }
    match src[0] {
        ORDERED_NULL => return Some((Value::Null, 1)),
        ORDERED_PRESENT => {}
        _ => return None,
    }
    let body = &src[1..];
    match ty {
        LogicalType::Null => None,
        LogicalType::Boolean => match *body.first()? {
            0x00 => Some((Value::Boolean(false), 2)),
            0x01 => Some((Value::Boolean(true), 2)),
            _ => None,
        },
        LogicalType::SmallInt => Some((
            Value::SmallInt(i16::from_le_bytes(body.get(..2)?.try_into().ok()?)),
            3,
        )),
        LogicalType::Int | LogicalType::Date => Some((
            Value::Int(i32::from_le_bytes(body.get(..4)?.try_into().ok()?)),
            5,
        )),
        LogicalType::BigInt
        | LogicalType::Time
        | LogicalType::Timestamp
        | LogicalType::TimestampTz => Some((
            Value::BigInt(i64::from_le_bytes(body.get(..8)?.try_into().ok()?)),
            9,
        )),
        LogicalType::Decimal { .. } => {
            let scale = *body.first()?;
            if scale > MAX_DECIMAL_PRECISION {
                return None;
            }
            let unscaled = i128::from_le_bytes(body.get(1..17)?.try_into().ok()?);
            Some((Value::Decimal { unscaled, scale }, 18))
        }
        LogicalType::Float => Some((
            Value::Float(f32::from_bits(u32::from_le_bytes(
                body.get(..4)?.try_into().ok()?,
            ))),
            5,
        )),
        LogicalType::Double => Some((
            Value::Double(f64::from_bits(u64::from_le_bytes(
                body.get(..8)?.try_into().ok()?,
            ))),
            9,
        )),
        LogicalType::Char { length }
        | LogicalType::VarChar { length }
        | LogicalType::Binary { length }
        | LogicalType::VarBinary { length } => {
            let n = usize::from(u16::from_le_bytes(body.get(..2)?.try_into().ok()?));
            if n > length as usize || n > MAX_TEXT_LEN {
                return None;
            }
            let bytes = body.get(2..2 + n)?;
            let v = if ty.is_character() {
                Value::Text(bytes)
            } else {
                Value::Bytes(bytes)
            };
            Some((v, 1 + 2 + n))
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// 5. Relational keyspaces (§14.4)
// ══════════════════════════════════════════════════════════════════════

/// §14.4's reserved keyspaces. The high bit is set on every one of
/// them: catalog-assigned user keyspaces are allocated below
/// `0x8000_0000`, so [`is_reserved_relational`] is a single bit test
/// and a foreign protocol cannot collide into relational space by
/// accident.
///
/// ```text
/// relational/catalog/<database-id>/<object-kind>/<object-id>
/// relational/table/<table-id>/<encoded-primary-key>
/// relational/index/<index-id>/<encoded-index-key>/<primary-key>
/// relational/sequence/<sequence-id>
/// relational/job/<schema-job-id>
/// relational/stats/<table-id>/<statistic-id>
/// ```
pub const KS_RELATIONAL_CATALOG: u32 = 0x8000_0001;
pub const KS_RELATIONAL_TABLE: u32 = 0x8000_0002;
pub const KS_RELATIONAL_INDEX: u32 = 0x8000_0003;
pub const KS_RELATIONAL_SEQUENCE: u32 = 0x8000_0004;
pub const KS_RELATIONAL_JOB: u32 = 0x8000_0005;
pub const KS_RELATIONAL_STATS: u32 = 0x8000_0006;

/// Is `keyspace` one of the reserved relational keyspaces?
pub const fn is_reserved_relational(keyspace: u32) -> bool {
    matches!(
        keyspace,
        KS_RELATIONAL_CATALOG
            | KS_RELATIONAL_TABLE
            | KS_RELATIONAL_INDEX
            | KS_RELATIONAL_SEQUENCE
            | KS_RELATIONAL_JOB
            | KS_RELATIONAL_STATS
    )
}

/// Catalog object kinds, the `<object-kind>` component of a catalog
/// key. Discriminants start at 1 so a zeroed buffer never decodes as a
/// kind.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    Database = 1,
    Table = 2,
    Index = 3,
    Sequence = 4,
    Constraint = 5,
    View = 6,
}

impl ObjectKind {
    pub const ALL: [Self; 6] = [
        Self::Database,
        Self::Table,
        Self::Index,
        Self::Sequence,
        Self::Constraint,
        Self::View,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Database),
            2 => Some(Self::Table),
            3 => Some(Self::Index),
            4 => Some(Self::Sequence),
            5 => Some(Self::Constraint),
            6 => Some(Self::View),
            _ => None,
        }
    }
}

/// Which model owns an object's writes (§21 invariant 18: "every
/// catalog object has one authoritative write owner; other models
/// access it only through declared views or projections").
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelOwner {
    Relational = 1,
    Document = 2,
    WideColumn = 3,
    Graph = 4,
    TimeSeries = 5,
    Search = 6,
    Vector = 7,
    /// Plain KV protocols (Redis/etcd/Memcached). Named so that a
    /// relational object owned by KV is expressible — and therefore
    /// visibly wrong — rather than unrepresentable.
    KeyValue = 8,
}

impl ModelOwner {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Relational),
            2 => Some(Self::Document),
            3 => Some(Self::WideColumn),
            4 => Some(Self::Graph),
            5 => Some(Self::TimeSeries),
            6 => Some(Self::Search),
            7 => Some(Self::Vector),
            8 => Some(Self::KeyValue),
            _ => None,
        }
    }
}

/// How a writer reached an object.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessPath {
    /// The writer addressed the object's own keyspace.
    Direct = 1,
    /// The writer went through an explicitly declared view or
    /// projection with transactional maintenance (§14.4's escape
    /// hatch).
    DeclaredProjection = 2,
}

/// Every way an access or catalog check refuses. No catch-all: a
/// refusal must name what it refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogError {
    /// §14.4: relational keyspaces are not writable through
    /// unrestricted Redis, etcd, or Memcached commands. Such a write
    /// would bypass types, indexes, constraints, and schema state.
    ForeignProtocolWrite { keyspace: u32, writer: ModelOwner },
    /// §21 invariant 18: a second model tried to write an object it
    /// does not own, without a declared projection.
    NotWriteOwner {
        owner: ModelOwner,
        writer: ModelOwner,
    },
    /// A descriptor field violated its declared bound.
    OutOfBounds,
    /// A structurally invalid descriptor: duplicate or unsorted column
    /// ids, a key column that is not in the column set, a nullable
    /// primary-key column, an empty key.
    Malformed,
    /// The index's freshness record names a different index.
    FreshnessWrongIndex { expected: u32, found: u32 },
    /// The index's freshness record claims a different exactness than
    /// the descriptor declares. §21 invariant 19 — an index cannot
    /// claim an exactness it was not built with.
    FreshnessExactnessMismatch,
    /// The delegated `db_ops` freshness check refused.
    Freshness(FreshnessError),
}

/// §14.4 + §21 invariant 18 as one executable gate.
///
/// Ordering of the clauses is the contract: the keyspace rule is
/// checked first and is unconditional, because a Redis `SET` into
/// `relational/table/...` is wrong regardless of what any descriptor
/// says. Only then does ownership matter, and only a *declared*
/// projection excuses a cross-model write.
pub fn check_object_write(
    keyspace: u32,
    owner: ModelOwner,
    writer: ModelOwner,
    via: AccessPath,
) -> Result<(), CatalogError> {
    if is_reserved_relational(keyspace)
        && writer == ModelOwner::KeyValue
        && via == AccessPath::Direct
    {
        return Err(CatalogError::ForeignProtocolWrite { keyspace, writer });
    }
    if writer != owner && via != AccessPath::DeclaredProjection {
        return Err(CatalogError::NotWriteOwner { owner, writer });
    }
    Ok(())
}

/// Length of a catalog user key: database id, object kind, object id.
pub const CATALOG_KEY_LEN: usize = 4 + 1 + 4;

/// Build the `relational/catalog/<database-id>/<object-kind>/<object-id>`
/// user key: `[database_id:u32 BE][kind:u8][object_id:u32 BE]`.
///
/// Fixed-width big-endian throughout, so the key is trivially
/// order-preserving and prefix-unambiguous, and a scan of
/// `[database_id][kind]` yields every object of one kind in id order.
/// Hand this to `internal_key::encode` with [`KS_RELATIONAL_CATALOG`].
pub fn encode_catalog_key(
    out: &mut [u8],
    database_id: u32,
    kind: ObjectKind,
    object_id: u32,
) -> Option<usize> {
    if out.len() < CATALOG_KEY_LEN {
        return None;
    }
    out[0..4].copy_from_slice(&database_id.to_be_bytes());
    out[4] = kind as u8;
    out[5..9].copy_from_slice(&object_id.to_be_bytes());
    Some(CATALOG_KEY_LEN)
}

/// Decode a catalog user key. Fails closed on a short buffer, trailing
/// bytes, or an unknown object kind.
pub fn decode_catalog_key(src: &[u8]) -> Option<(u32, ObjectKind, u32)> {
    if src.len() != CATALOG_KEY_LEN {
        return None;
    }
    Some((
        u32::from_be_bytes(src[0..4].try_into().ok()?),
        ObjectKind::from_u8(src[4])?,
        u32::from_be_bytes(src[5..9].try_into().ok()?),
    ))
}

/// Reserved keyspace for the catalog's NAME index (§14.4). Separate
/// from [`KS_RELATIONAL_CATALOG`] rather than a differently-shaped key
/// inside it, so a scan of the catalog keyspace still yields exactly
/// descriptors and nothing has to distinguish an index entry from an
/// object by inspecting its bytes.
pub const KS_RELATIONAL_CATALOG_NAME: u32 = 0x8000_0007;

/// Build the catalog name-index user key:
/// `[database_id:u32 BE][kind:u8][lower(name)…]`.
///
/// The catalog itself is keyed by object ID, because that is what every
/// reference to an object uses — a descriptor, a row key, an index
/// entry. But SQL names objects by NAME, so resolving `INSERT INTO t`
/// needs the opposite direction, and a scan of every descriptor to find
/// one name would make every statement cost the size of the catalog.
/// This index holds the missing direction; its value is the object id.
///
/// The name is LOWER-CASED into the key. SQL identifiers are
/// case-insensitive unless quoted, so `T` and `t` must resolve to one
/// object — and case-folding at the key is what makes that true of the
/// lookup itself rather than of a comparison someone has to remember to
/// perform. The consequence is deliberate: this build does not
/// distinguish a quoted `"T"` from a bare `t`. Case-sensitive quoted
/// identifiers would need the original bytes stored alongside, and
/// pretending to support them while folding the key would be worse than
/// not supporting them.
///
/// Only ASCII folds. A non-ASCII identifier can only arrive quoted, and
/// those bytes pass through unchanged.
pub fn encode_catalog_name_key(
    out: &mut [u8],
    database_id: u32,
    kind: ObjectKind,
    name: &[u8],
) -> Option<usize> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return None;
    }
    if out.len() < 5 + name.len() {
        return None;
    }
    out[0..4].copy_from_slice(&database_id.to_be_bytes());
    out[4] = kind as u8;
    for (i, b) in name.iter().enumerate() {
        out[5 + i] = b.to_ascii_lowercase();
    }
    Some(5 + name.len())
}

/// Longest catalog name-index key.
pub const CATALOG_NAME_KEY_MAX: usize = 5 + MAX_NAME_LEN;

/// `relational/sequence/<sequence-id>` user key.
pub fn encode_sequence_key(out: &mut [u8], sequence_id: u32) -> Option<usize> {
    put(out, 0, &sequence_id.to_be_bytes())
}

/// `relational/job/<schema-job-id>` user key.
pub fn encode_job_key(out: &mut [u8], job_id: u64) -> Option<usize> {
    put(out, 0, &job_id.to_be_bytes())
}

/// `relational/stats/<table-id>/<statistic-id>` user key.
pub fn encode_stats_key(out: &mut [u8], table_id: u32, statistic_id: u32) -> Option<usize> {
    let n = put(out, 0, &table_id.to_be_bytes())?;
    put(out, n, &statistic_id.to_be_bytes())
}

// ══════════════════════════════════════════════════════════════════════
// 6. Primary-key and index-value composites (§14.4)
// ══════════════════════════════════════════════════════════════════════

/// Width of the table identifier inside a primary-key composite.
pub const TABLE_ID_LEN: usize = 4;

/// Build the `relational/table/<table-id>/<encoded-primary-key>` **user
/// key**:
///
/// ```text
/// [table_id:u32 BE][ordered(v0)][ordered(v1)]...[ordered(vk-1)]
/// ```
///
/// and hand the result to `internal_key::encode` with
/// [`KS_RELATIONAL_TABLE`] — this function deliberately stops short of
/// the physical key, because `internal_key` owns the identity triple,
/// the MVCC suffix, and the outer escape (§9.1). One encoder per
/// concern.
///
/// Two properties, both load-bearing:
///
/// - **Rows of one table sort together and no other table's rows fall
///   between them**, because the fixed-width big-endian table id is a
///   strict prefix of every one of its keys.
/// - **A prefix of the key columns is a valid scan bound**, because
///   every ordered value is self-delimiting (fixed width, or escaped
///   with a terminator that sorts below every escape byte). Plain
///   concatenation is therefore unambiguous, and lexicographic byte
///   order over composites equals tuple order over
///   `(v0, v1, ..., vk-1)`. Use [`encode_primary_key_prefix`] for the
///   partial form.
///
/// `types` supplies the column types (from
/// [`TableDescriptor::primary_key_types`]); `values` must have the same
/// arity. The composite is bounded by [`MAX_PRIMARY_KEY_LEN`] — a key
/// that cannot be a range bound cannot be indexed, so exceeding it is a
/// refusal, never a truncation.
pub fn encode_primary_key(
    out: &mut [u8],
    table_id: u32,
    types: &[LogicalType],
    values: &[Value<'_>],
) -> Option<usize> {
    if types.len() != values.len() {
        return None;
    }
    encode_primary_key_prefix(out, table_id, types, values)
}

/// The partial form of [`encode_primary_key`]: encodes the first
/// `values.len()` key columns, which is exactly the scan bound for
/// "every row whose leading key columns equal these". `values.len()`
/// may be `0` (the whole-table prefix) up to `types.len()`.
pub fn encode_primary_key_prefix(
    out: &mut [u8],
    table_id: u32,
    types: &[LogicalType],
    values: &[Value<'_>],
) -> Option<usize> {
    if values.len() > types.len() || types.len() > MAX_KEY_COLUMNS {
        return None;
    }
    if out.len() < TABLE_ID_LEN {
        return None;
    }
    out[0..TABLE_ID_LEN].copy_from_slice(&table_id.to_be_bytes());
    let mut n = TABLE_ID_LEN;
    for (i, v) in values.iter().enumerate() {
        n += encode_value_ordered(out.get_mut(n..)?, types[i], *v)?;
    }
    if n > MAX_PRIMARY_KEY_LEN {
        return None;
    }
    Some(n)
}

/// The `[table_id:u32 BE]` prefix alone: the scan bound for one whole
/// table.
pub fn encode_table_prefix(out: &mut [u8], table_id: u32) -> Option<usize> {
    put(out, 0, &table_id.to_be_bytes())
}

/// Scratch bytes [`decode_primary_key`] needs per key column: one
/// full-width unescape window each, so every decoded value can borrow
/// its own disjoint slice at once.
pub const fn primary_key_scratch_len(key_columns: usize) -> usize {
    key_columns * MAX_TEXT_LEN
}

/// Decode a full primary-key composite back into values.
///
/// `text_out` is carved into one [`MAX_TEXT_LEN`] window per key column
/// — see [`primary_key_scratch_len`] — because text and binary values
/// borrow it and all `types.len()` of them must be live at once. Scalar
/// columns waste their window; the alternative is a second pass, and a
/// key is at most eight columns.
///
/// Returns `(table_id, values written)`. Fails closed on a short
/// buffer, an undersized scratch, an arity disagreement, or trailing
/// bytes — a composite that does not consume exactly its declared
/// columns is not this table's key.
pub fn decode_primary_key<'b>(
    src: &[u8],
    types: &[LogicalType],
    text_out: &'b mut [u8],
    values_out: &mut [Value<'b>],
) -> Option<(u32, usize)> {
    if src.len() < TABLE_ID_LEN
        || types.len() > values_out.len()
        || types.len() > MAX_KEY_COLUMNS
        || text_out.len() < primary_key_scratch_len(types.len())
    {
        return None;
    }
    let table_id = u32::from_be_bytes(src[0..TABLE_ID_LEN].try_into().ok()?);
    let mut n = TABLE_ID_LEN;
    let mut rest: &'b mut [u8] = text_out;
    for (i, ty) in types.iter().enumerate() {
        let (window, tail) = core::mem::take(&mut rest).split_at_mut(MAX_TEXT_LEN);
        rest = tail;
        let (v, used) = decode_value_ordered(src.get(n..)?, *ty, window)?;
        values_out[i] = v;
        n += used;
    }
    if n != src.len() {
        return None;
    }
    Some((table_id, types.len()))
}

/// Build the index-value tuple that `db_ops::IndexEntry` consumes as
/// its `index_value` component.
///
/// This is the relational layer *above* `db_ops`: `db_ops` owns
/// `index_id | index_value | primary_key` and the escape discipline
/// that keeps a scan for `"a"` from sweeping up `"ab"`; this function
/// owns turning typed columns into the `index_value` bytes. It is a
/// plain ordered composite with **no** table-id prefix, because
/// `db_ops` already prefixes the index id:
///
/// ```text
/// [ordered(c0)][ordered(c1)]...[ordered(ck-1)]
/// ```
///
/// Bounded by `db_ops::MAX_INDEX_VALUE_LEN`. NULLs are permitted (an
/// index over a nullable column indexes the NULLs, sorting first).
pub fn encode_index_value(
    out: &mut [u8],
    types: &[LogicalType],
    values: &[Value<'_>],
) -> Option<usize> {
    if types.len() != values.len() || types.len() > MAX_KEY_COLUMNS {
        return None;
    }
    let mut n = 0usize;
    for (i, v) in values.iter().enumerate() {
        n += encode_value_ordered(out.get_mut(n..)?, types[i], *v)?;
    }
    if n > MAX_INDEX_VALUE_LEN {
        return None;
    }
    Some(n)
}

// ══════════════════════════════════════════════════════════════════════
// 7. Row payload encoding (§14.4)
// ══════════════════════════════════════════════════════════════════════

/// One column of a row, on the way in or out of [`encode_row`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColumnValue<'a> {
    pub column_id: u16,
    pub ty: LogicalType,
    pub value: Value<'a>,
}

/// Encode a row payload.
///
/// ```text
/// [version:u16 LE][payload_len:u16 LE][column_count:u16 LE]
/// then column_count entries, ASCENDING column_id:
///   [column_id:u16 LE][tag:u8][param_a:u8][param_b:u8]
///   [value_len:u16 LE][plain value bytes]
/// ```
///
/// `payload_len` covers everything after the first four bytes.
///
/// The layout is built around one requirement from §14.5 — old and new
/// binaries must both be able to read a row while a schema change is in
/// flight:
///
/// - **Every entry carries its own `value_len`.** A reader that does
///   not recognise a `column_id` skips `value_len` bytes and keeps
///   going. It never has to understand the value to step over it, so a
///   column added by a newer binary is *skipped*, not misparsed — see
///   [`row_lookup`], which returns [`ColumnLookup::Absent`] for an
///   unknown id and refuses only when the caller's own column is
///   unreadable.
/// - **Every entry carries its own type.** A reader that knows a column
///   id but disagrees about its type refuses rather than reinterpreting
///   the bytes. Type-changing schema changes therefore fail loudly at
///   the row, not silently at the value.
/// - **Entries are strictly ascending by column id.** Strictly, so
///   duplicates are impossible: two entries for one column would give
///   two answers with no rule for choosing. Ascending, so a lookup can
///   stop early and a decoder can validate ordering in one pass.
/// - **NULL is an entry whose value is the one-byte null marker;
///   absent is no entry at all.** The distinction is the whole point of
///   per-column entries: `Present(Null)` means the row says this column
///   is NULL, `Absent` means the row predates the column, and a default
///   or NULL must be supplied by the reader's schema. Collapsing them
///   would make adding a column with a non-NULL default unrepresentable.
///
/// Fails closed on: a value that does not inhabit its declared type,
/// non-ascending or duplicate column ids, more than [`MAX_COLUMNS`]
/// columns, an encoding larger than [`MAX_ROW_BYTES`], or a short
/// `out`.
pub fn encode_row(out: &mut [u8], cols: &[ColumnValue<'_>]) -> Option<usize> {
    if cols.len() > MAX_COLUMNS || out.len() < ROW_HEADER_LEN {
        return None;
    }
    let mut n = ROW_HEADER_LEN;
    let mut prev: Option<u16> = None;
    for c in cols {
        if let Some(p) = prev {
            if c.column_id <= p {
                return None; // duplicate or out-of-order
            }
        }
        prev = Some(c.column_id);
        n = put(out, n, &c.column_id.to_le_bytes())?;
        n += c.ty.encode(out.get_mut(n..)?)?;
        // Reserve the length slot, encode into the tail, then backfill.
        let len_at = n;
        n = put(out, n, &0u16.to_le_bytes())?;
        let vlen = encode_value_plain(out.get_mut(n..)?, c.ty, c.value)?;
        out[len_at..len_at + 2].copy_from_slice(&(vlen as u16).to_le_bytes());
        n += vlen;
    }
    if n > MAX_ROW_BYTES {
        return None;
    }
    let payload_len = n - 4;
    if payload_len > u16::MAX as usize {
        return None;
    }
    out[0..2].copy_from_slice(&ROW_FORMAT_VERSION.to_le_bytes());
    out[2..4].copy_from_slice(&(payload_len as u16).to_le_bytes());
    out[4..6].copy_from_slice(&(cols.len() as u16).to_le_bytes());
    Some(n)
}

/// One entry of an encoded row, located but not interpreted.
///
/// `type_bytes` is the raw three-byte type; [`RowEntry::ty`] parses it
/// and returns `None` for a tag this binary does not know. That
/// separation is deliberate: the structural walk must be able to step
/// over an entry whose type it cannot parse, which is exactly what
/// forward compatibility means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowEntry {
    pub column_id: u16,
    pub type_bytes: [u8; TYPE_WIRE_LEN],
    /// Offset of the plain value bytes within the row payload.
    pub value_at: usize,
    pub value_len: usize,
}

impl RowEntry {
    pub fn ty(&self) -> Option<LogicalType> {
        LogicalType::decode(&self.type_bytes)
    }
}

/// Structural validation of a row payload. Returns the column count.
///
/// Checks the version, the declared payload length against the actual
/// buffer, every entry's length against the remaining bytes, ascending
/// strictly-increasing column ids, and that the entries consume the
/// payload exactly. Does **not** validate type tags: an unknown tag is
/// a forward-compatibility case, not a corruption case, and rejecting
/// it here would make every old binary refuse every new row.
pub fn validate_row(src: &[u8]) -> Option<usize> {
    let (count, _) = row_header(src)?;
    let mut prev: Option<u16> = None;
    let mut at = ROW_HEADER_LEN;
    for _ in 0..count {
        let e = row_entry_at(src, at)?;
        if let Some(p) = prev {
            if e.column_id <= p {
                return None;
            }
        }
        prev = Some(e.column_id);
        at = e.value_at + e.value_len;
    }
    if at != src.len() {
        return None;
    }
    Some(count)
}

/// Parse and check the row header, returning `(column_count, payload_len)`.
fn row_header(src: &[u8]) -> Option<(usize, usize)> {
    if src.len() < ROW_HEADER_LEN {
        return None;
    }
    if u16::from_le_bytes(src[0..2].try_into().ok()?) != ROW_FORMAT_VERSION {
        return None;
    }
    let payload_len = usize::from(u16::from_le_bytes(src[2..4].try_into().ok()?));
    if src.len() != 4 + payload_len {
        return None;
    }
    let count = usize::from(u16::from_le_bytes(src[4..6].try_into().ok()?));
    if count > MAX_COLUMNS {
        return None;
    }
    Some((count, payload_len))
}

/// Locate the entry beginning at byte offset `at`.
fn row_entry_at(src: &[u8], at: usize) -> Option<RowEntry> {
    let head = src.get(at..at + 2 + TYPE_WIRE_LEN + 2)?;
    let column_id = u16::from_le_bytes(head[0..2].try_into().ok()?);
    let mut type_bytes = [0u8; TYPE_WIRE_LEN];
    type_bytes.copy_from_slice(&head[2..2 + TYPE_WIRE_LEN]);
    let value_len = usize::from(u16::from_le_bytes(
        head[2 + TYPE_WIRE_LEN..].try_into().ok()?,
    ));
    if value_len > MAX_PLAIN_VALUE_LEN {
        return None;
    }
    let value_at = at + 2 + TYPE_WIRE_LEN + 2;
    if src.len() < value_at + value_len {
        return None;
    }
    Some(RowEntry {
        column_id,
        type_bytes,
        value_at,
        value_len,
    })
}

/// Result of asking a row for one column.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ColumnLookup<'a> {
    /// No entry with this column id. The row predates the column (or
    /// postdates its drop); the reader supplies the schema default.
    /// **Not** the same as `Present(Value::Null)`.
    Absent,
    /// The row carries a value for this column. `Present(Value::Null)`
    /// is a stored SQL NULL.
    Present(Value<'a>),
}

/// Read one column out of a row.
///
/// Walks entries in ascending column-id order, skipping each by its
/// declared `value_len` without interpreting it, and stops early once
/// the ids pass `column_id`. Returns:
///
/// - `Some(Absent)` — no such column in this row (schema evolution);
/// - `Some(Present(v))` — decoded, with `v` borrowing `src`;
/// - `None` — the row is malformed, or the entry for `column_id` does
///   not carry `expected` as its type, or its value will not decode.
///   Fail closed: a caller that asked for an `INT` and found a
///   `VARCHAR` gets a refusal, never a reinterpretation.
///
/// Note the asymmetry that makes forward compatibility work: an
/// unknown *type tag* on some **other** column is stepped over
/// harmlessly, because skipping needs only the length. Only the
/// requested column's type has to be understood.
pub fn row_lookup(src: &[u8], column_id: u16, expected: LogicalType) -> Option<ColumnLookup<'_>> {
    let (count, _) = row_header(src)?;
    let mut prev: Option<u16> = None;
    let mut at = ROW_HEADER_LEN;
    for _ in 0..count {
        let e = row_entry_at(src, at)?;
        if let Some(p) = prev {
            if e.column_id <= p {
                return None;
            }
        }
        prev = Some(e.column_id);
        if e.column_id == column_id {
            if e.ty()? != expected {
                return None;
            }
            let bytes = src.get(e.value_at..e.value_at + e.value_len)?;
            let (v, used) = decode_value_plain(bytes, expected)?;
            if used != e.value_len {
                return None;
            }
            return Some(ColumnLookup::Present(v));
        }
        if e.column_id > column_id {
            return Some(ColumnLookup::Absent);
        }
        at = e.value_at + e.value_len;
    }
    Some(ColumnLookup::Absent)
}

/// The `i`-th entry of a row, located structurally. For readers that
/// walk a row without a schema — the change-feed publisher, the backup
/// verifier — and for proving that an unknown column is skippable.
pub fn row_entry(src: &[u8], i: usize) -> Option<RowEntry> {
    let (count, _) = row_header(src)?;
    if i >= count {
        return None;
    }
    let mut at = ROW_HEADER_LEN;
    let mut e = row_entry_at(src, at)?;
    for _ in 0..i {
        at = e.value_at + e.value_len;
        e = row_entry_at(src, at)?;
    }
    Some(e)
}

// ══════════════════════════════════════════════════════════════════════
// 8. Catalog descriptors (§14.2, §14.5)
// ══════════════════════════════════════════════════════════════════════
//
// Every descriptor carries `catalog_revision`. That field is not
// decoration: §14.5 requires a plan to record the catalog revision it
// bound against, and requires that "before execution, stale catalog or
// routing generations cause rebind/replan rather than execution under
// mixed metadata". [`PlanBinding`] in §11 below is the consumer.
//
// Every descriptor is versioned, length-declared, and bounded, in the
// same `[version:u16][payload_len:u16][payload]` shape `db_ops` uses,
// so a decoder that meets a newer binary's descriptor refuses it rather
// than reading half of it.

/// A fixed-capacity object name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Name {
    bytes: [u8; MAX_NAME_LEN],
    len: u8,
}

impl Name {
    pub const EMPTY: Name = Name {
        bytes: [0; MAX_NAME_LEN],
        len: 0,
    };

    /// Build a name. Empty names are refused: an unnamed catalog object
    /// cannot be resolved and would be reachable only by id.
    pub fn new(s: &[u8]) -> Option<Self> {
        if s.is_empty() || s.len() > MAX_NAME_LEN {
            return None;
        }
        let mut n = Self::EMPTY;
        n.bytes[..s.len()].copy_from_slice(s);
        n.len = s.len() as u8;
        Some(n)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    pub fn wire_len(&self) -> usize {
        1 + self.len as usize
    }

    fn encode(&self, out: &mut [u8], n: usize) -> Option<usize> {
        let k = put(out, n, &[self.len])?;
        put(out, k, self.as_bytes())
    }

    fn decode(src: &[u8], n: usize) -> Option<(Self, usize)> {
        let len = usize::from(*src.get(n)?);
        if len == 0 || len > MAX_NAME_LEN {
            return None;
        }
        let bytes = src.get(n + 1..n + 1 + len)?;
        Some((Self::new(bytes)?, n + 1 + len))
    }
}

// ── DatabaseDescriptor ────────────────────────────────────────────────

/// A relational database (§14.2's "database ... catalog").
///
/// Wire layout ([`DATABASE_DESCRIPTOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [database_id:u32][catalog_revision:u64][owner:u8]
/// [name_len:u8][name...]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseDescriptor {
    pub database_id: u32,
    pub catalog_revision: u64,
    pub owner: ModelOwner,
    pub name: Name,
}

/// Fixed wire overhead of a [`DatabaseDescriptor`] (excluding the name
/// bytes).
pub const DATABASE_DESCRIPTOR_FIXED_WIRE_LEN: usize = 4 + 4 + 8 + 1 + 1;

impl DatabaseDescriptor {
    pub fn wire_len(&self) -> usize {
        DATABASE_DESCRIPTOR_FIXED_WIRE_LEN + self.name.len as usize
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&DATABASE_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.database_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.catalog_revision.to_le_bytes());
        out[16] = self.owner as u8;
        let n = self.name.encode(out, 17)?;
        debug_assert_eq!(n, total);
        Some(n)
    }

    /// Fails closed on unknown version, declared-length mismatch,
    /// truncation, trailing bytes, an unknown owner, or an empty name.
    pub fn decode(src: &[u8]) -> Option<Self> {
        let payload_len = check_header(src, DATABASE_DESCRIPTOR_VERSION)?;
        let database_id = u32::from_le_bytes(src.get(4..8)?.try_into().ok()?);
        let catalog_revision = u64::from_le_bytes(src.get(8..16)?.try_into().ok()?);
        let owner = ModelOwner::from_u8(*src.get(16)?)?;
        let (name, n) = Name::decode(src, 17)?;
        if n != 4 + payload_len {
            return None;
        }
        Some(Self {
            database_id,
            catalog_revision,
            owner,
            name,
        })
    }
}

/// Validate the shared `[version:u16][payload_len:u16]` header and
/// return the declared payload length.
fn check_header(src: &[u8], version: u16) -> Option<usize> {
    if src.len() < 4 {
        return None;
    }
    if u16::from_le_bytes(src[0..2].try_into().ok()?) != version {
        return None;
    }
    let payload_len = usize::from(u16::from_le_bytes(src[2..4].try_into().ok()?));
    if src.len() != 4 + payload_len {
        return None;
    }
    Some(payload_len)
}

// ── TableDescriptor ───────────────────────────────────────────────────

/// Column flag bits. Anything outside this mask is reserved and a
/// decoder that meets it refuses — a future flag must not read as
/// "unset" on an old binary, because "unset" for a nullability or
/// default flag is a silently wrong answer.
pub const COLUMN_FLAG_NULLABLE: u8 = 0b0000_0001;
pub const COLUMN_FLAG_DEFAULT_PRESENT: u8 = 0b0000_0010;
const COLUMN_FLAG_MASK: u8 = COLUMN_FLAG_NULLABLE | COLUMN_FLAG_DEFAULT_PRESENT;

/// One column of a table.
///
/// `default_present` is a *flag*, not a value: defaults are expressions
/// and this layer does not evaluate expressions (§14.6). It records
/// that a default exists so [`check_not_null`] can say whether an
/// absent column is recoverable, and so a connector can report the
/// column honestly in its compatibility catalog.
///
/// Wire layout, inside a [`TableDescriptor`]:
///
/// ```text
/// [column_id:u16][tag:u8][param_a:u8][param_b:u8][flags:u8]
/// [name_len:u8][name...]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnDescriptor {
    pub column_id: u16,
    pub ty: LogicalType,
    pub nullable: bool,
    pub default_present: bool,
    pub name: Name,
}

/// Fixed wire overhead of a [`ColumnDescriptor`] (excluding name bytes).
pub const COLUMN_DESCRIPTOR_FIXED_WIRE_LEN: usize = 2 + TYPE_WIRE_LEN + 1 + 1;

impl ColumnDescriptor {
    pub const EMPTY: ColumnDescriptor = ColumnDescriptor {
        column_id: 0,
        ty: LogicalType::Null,
        nullable: false,
        default_present: false,
        name: Name::EMPTY,
    };

    pub fn new(
        column_id: u16,
        ty: LogicalType,
        nullable: bool,
        default_present: bool,
        name: &[u8],
    ) -> Option<Self> {
        // `LogicalType::Null` is a binder placeholder, never a stored
        // column type: a column whose type is "unknown" has no encoding.
        if !ty.is_valid() || ty == LogicalType::Null {
            return None;
        }
        Some(Self {
            column_id,
            ty,
            nullable,
            default_present,
            name: Name::new(name)?,
        })
    }

    pub fn wire_len(&self) -> usize {
        COLUMN_DESCRIPTOR_FIXED_WIRE_LEN + self.name.len as usize
    }

    fn flags(&self) -> u8 {
        u8::from(self.nullable) | (u8::from(self.default_present) << 1)
    }

    fn encode(&self, out: &mut [u8], n: usize) -> Option<usize> {
        let mut k = put(out, n, &self.column_id.to_le_bytes())?;
        k += self.ty.encode(out.get_mut(k..)?)?;
        k = put(out, k, &[self.flags()])?;
        self.name.encode(out, k)
    }

    fn decode(src: &[u8], n: usize) -> Option<(Self, usize)> {
        let column_id = u16::from_le_bytes(src.get(n..n + 2)?.try_into().ok()?);
        let ty = LogicalType::decode(src.get(n + 2..n + 2 + TYPE_WIRE_LEN)?)?;
        if ty == LogicalType::Null {
            return None;
        }
        let flags = *src.get(n + 2 + TYPE_WIRE_LEN)?;
        if flags & !COLUMN_FLAG_MASK != 0 {
            return None;
        }
        let (name, k) = Name::decode(src, n + 3 + TYPE_WIRE_LEN)?;
        Some((
            Self {
                column_id,
                ty,
                nullable: flags & COLUMN_FLAG_NULLABLE != 0,
                default_present: flags & COLUMN_FLAG_DEFAULT_PRESENT != 0,
                name,
            },
            k,
        ))
    }
}

/// A relational table (§14.2's "table, column ... catalogs").
///
/// Wire layout ([`TABLE_DESCRIPTOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [table_id:u32][database_id:u32][catalog_revision:u64]
/// [format_generation:u16][owner:u8]
/// [name_len:u8][name...]
/// [column_count:u8][ColumnDescriptor × column_count]
/// [pk_count:u8][pk column_id:u16 × pk_count]
/// ```
///
/// `format_generation` is [`KEY_FORMAT_GENERATION`] at creation time.
/// Keys carry no inline version (§9.1 keeps key-format versions out of
/// keys), so this field is where a reader learns which key encoding a
/// table's rows were written with — and a mismatch is a full rewrite,
/// not a migration in place.
///
/// Invariants enforced by both the builder and [`decode`](Self::decode):
/// column ids strictly ascending, at least one column, at least one
/// primary-key column, every primary-key column present in the column
/// set, and **no primary-key column nullable** — a NULL in a key would
/// make two different rows share a key position and there is no honest
/// answer for which one a lookup means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableDescriptor {
    pub table_id: u32,
    pub database_id: u32,
    pub catalog_revision: u64,
    pub format_generation: u16,
    pub owner: ModelOwner,
    pub name: Name,
    columns: [ColumnDescriptor; MAX_COLUMNS],
    column_count: u8,
    pk: [u16; MAX_KEY_COLUMNS],
    pk_count: u8,
}

impl TableDescriptor {
    pub const EMPTY: TableDescriptor = TableDescriptor {
        table_id: 0,
        database_id: 0,
        catalog_revision: 0,
        format_generation: KEY_FORMAT_GENERATION,
        owner: ModelOwner::Relational,
        name: Name::EMPTY,
        columns: [ColumnDescriptor::EMPTY; MAX_COLUMNS],
        column_count: 0,
        pk: [0; MAX_KEY_COLUMNS],
        pk_count: 0,
    };

    pub fn new(
        table_id: u32,
        database_id: u32,
        catalog_revision: u64,
        name: &[u8],
    ) -> Option<Self> {
        let mut t = Self::EMPTY;
        t.table_id = table_id;
        t.database_id = database_id;
        t.catalog_revision = catalog_revision;
        t.name = Name::new(name)?;
        Some(t)
    }

    /// Append a column. Ids must be strictly ascending, which makes
    /// lookup a scan with an early exit and makes duplicates
    /// impossible.
    pub fn add_column(&mut self, col: ColumnDescriptor) -> Result<(), CatalogError> {
        if self.column_count as usize >= MAX_COLUMNS {
            return Err(CatalogError::OutOfBounds);
        }
        if self.column_count > 0
            && col.column_id <= self.columns[self.column_count as usize - 1].column_id
        {
            return Err(CatalogError::Malformed);
        }
        self.columns[self.column_count as usize] = col;
        self.column_count += 1;
        Ok(())
    }

    /// Declare the primary key. Order matters — it is the key tuple
    /// order, and therefore the physical row order.
    pub fn set_primary_key(&mut self, cols: &[u16]) -> Result<(), CatalogError> {
        if cols.is_empty() || cols.len() > MAX_KEY_COLUMNS {
            return Err(CatalogError::OutOfBounds);
        }
        for (i, id) in cols.iter().enumerate() {
            let c = self.column(*id).ok_or(CatalogError::Malformed)?;
            if c.nullable {
                return Err(CatalogError::Malformed);
            }
            self.pk[i] = *id;
        }
        self.pk_count = cols.len() as u8;
        Ok(())
    }

    pub fn columns(&self) -> &[ColumnDescriptor] {
        &self.columns[..self.column_count as usize]
    }

    pub fn primary_key(&self) -> &[u16] {
        &self.pk[..self.pk_count as usize]
    }

    pub fn column(&self, column_id: u16) -> Option<&ColumnDescriptor> {
        self.columns().iter().find(|c| c.column_id == column_id)
    }

    /// The primary key's column types, in key order — the `types`
    /// argument [`encode_primary_key`] wants.
    pub fn primary_key_types(&self, out: &mut [LogicalType]) -> Option<usize> {
        let pk = self.primary_key();
        if out.len() < pk.len() {
            return None;
        }
        for (i, id) in pk.iter().enumerate() {
            out[i] = self.column(*id)?.ty;
        }
        Some(pk.len())
    }

    /// Re-check every structural invariant. Called by
    /// [`decode`](Self::decode) so a malformed descriptor cannot be
    /// read back off disk even if it was somehow written.
    pub fn check_invariants(&self) -> Result<(), CatalogError> {
        if self.column_count == 0 || self.pk_count == 0 {
            return Err(CatalogError::Malformed);
        }
        for w in self.columns().windows(2) {
            if w[1].column_id <= w[0].column_id {
                return Err(CatalogError::Malformed);
            }
        }
        for id in self.primary_key() {
            match self.column(*id) {
                None => return Err(CatalogError::Malformed),
                Some(c) if c.nullable => return Err(CatalogError::Malformed),
                Some(_) => {}
            }
        }
        Ok(())
    }

    pub fn wire_len(&self) -> usize {
        let mut n = 4 + 4 + 4 + 8 + 2 + 1 + self.name.wire_len() + 1;
        for c in self.columns() {
            n += c.wire_len();
        }
        n + 1 + 2 * self.pk_count as usize
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        self.check_invariants().ok()?;
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&TABLE_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.table_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.database_id.to_le_bytes());
        out[12..20].copy_from_slice(&self.catalog_revision.to_le_bytes());
        out[20..22].copy_from_slice(&self.format_generation.to_le_bytes());
        out[22] = self.owner as u8;
        let mut n = self.name.encode(out, 23)?;
        n = put(out, n, &[self.column_count])?;
        for c in self.columns() {
            n = c.encode(out, n)?;
        }
        n = put(out, n, &[self.pk_count])?;
        for id in self.primary_key() {
            n = put(out, n, &id.to_le_bytes())?;
        }
        debug_assert_eq!(n, total);
        Some(n)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        let payload_len = check_header(src, TABLE_DESCRIPTOR_VERSION)?;
        let mut t = Self::EMPTY;
        t.table_id = u32::from_le_bytes(src.get(4..8)?.try_into().ok()?);
        t.database_id = u32::from_le_bytes(src.get(8..12)?.try_into().ok()?);
        t.catalog_revision = u64::from_le_bytes(src.get(12..20)?.try_into().ok()?);
        t.format_generation = u16::from_le_bytes(src.get(20..22)?.try_into().ok()?);
        t.owner = ModelOwner::from_u8(*src.get(22)?)?;
        let (name, mut n) = Name::decode(src, 23)?;
        t.name = name;
        let column_count = usize::from(*src.get(n)?);
        n += 1;
        if column_count > MAX_COLUMNS {
            return None;
        }
        for i in 0..column_count {
            let (c, k) = ColumnDescriptor::decode(src, n)?;
            t.columns[i] = c;
            n = k;
        }
        t.column_count = column_count as u8;
        let pk_count = usize::from(*src.get(n)?);
        n += 1;
        if pk_count > MAX_KEY_COLUMNS {
            return None;
        }
        for i in 0..pk_count {
            t.pk[i] = u16::from_le_bytes(src.get(n..n + 2)?.try_into().ok()?);
            n += 2;
        }
        t.pk_count = pk_count as u8;
        if n != 4 + payload_len {
            return None;
        }
        t.check_invariants().ok()?;
        Some(t)
    }
}

// ── IndexDescriptor ───────────────────────────────────────────────────

/// Index flag bits; anything else is reserved and refused on decode.
pub const INDEX_FLAG_UNIQUE: u8 = 0b0000_0001;
const INDEX_FLAG_MASK: u8 = INDEX_FLAG_UNIQUE;

/// A secondary index (§14.2's "index ... catalog").
///
/// Wire layout ([`INDEX_DESCRIPTOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [index_id:u32][table_id:u32][catalog_revision:u64]
/// [index_generation:u32][flags:u8][exactness:u8][phase:u8][direction:u8]
/// [name_len:u8][name...]
/// [key_count:u8][column_id:u16 × key_count]
/// ```
///
/// Three fields tie this descriptor to machinery that already exists:
///
/// - `index_generation` is the generation `db_ops::IndexFreshness`
///   must match (§11.3): an index built before a topology change cannot
///   answer for the span after it.
/// - `exactness` is `db_ops::IndexExactness`. A `Synchronous` index is
///   maintained inside the writing transaction and is exact at every
///   committed timestamp; a `Derived` index must advertise its lag and
///   cannot satisfy a freshness contract it has not reached (§21
///   invariant 19). [`IndexDescriptor::freshness_ok`] is the gate.
/// - `phase`/`direction` are the index's live [`SchemaPhase`]. An index
///   that is not `Public` is not readable no matter how fresh it is —
///   see [`IndexDescriptor::readable`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexDescriptor {
    pub index_id: u32,
    pub table_id: u32,
    pub catalog_revision: u64,
    pub index_generation: u32,
    pub unique: bool,
    pub exactness: IndexExactness,
    pub phase: SchemaPhase,
    pub direction: SchemaDirection,
    pub name: Name,
    key: [u16; MAX_KEY_COLUMNS],
    key_count: u8,
}

/// Fixed wire overhead of an [`IndexDescriptor`] (excluding name bytes
/// and key columns).
pub const INDEX_DESCRIPTOR_FIXED_WIRE_LEN: usize = 4 + 4 + 4 + 8 + 4 + 1 + 1 + 1 + 1 + 1 + 1;

impl IndexDescriptor {
    pub const EMPTY: IndexDescriptor = IndexDescriptor {
        index_id: 0,
        table_id: 0,
        catalog_revision: 0,
        index_generation: 0,
        unique: false,
        exactness: IndexExactness::Synchronous,
        phase: SchemaPhase::Absent,
        direction: SchemaDirection::Add,
        name: Name::EMPTY,
        key: [0; MAX_KEY_COLUMNS],
        key_count: 0,
    };

    /// Build an index descriptor over `key_columns` of `table`. Every
    /// key column must exist in the table; an index over a column the
    /// table does not have could never be maintained.
    pub fn new(
        index_id: u32,
        table: &TableDescriptor,
        name: &[u8],
        key_columns: &[u16],
        unique: bool,
        exactness: IndexExactness,
    ) -> Result<Self, CatalogError> {
        if key_columns.is_empty() || key_columns.len() > MAX_KEY_COLUMNS {
            return Err(CatalogError::OutOfBounds);
        }
        let mut d = Self::EMPTY;
        for (i, id) in key_columns.iter().enumerate() {
            if table.column(*id).is_none() {
                return Err(CatalogError::Malformed);
            }
            d.key[i] = *id;
        }
        d.key_count = key_columns.len() as u8;
        d.index_id = index_id;
        d.table_id = table.table_id;
        d.catalog_revision = table.catalog_revision;
        d.unique = unique;
        d.exactness = exactness;
        d.name = Name::new(name).ok_or(CatalogError::Malformed)?;
        Ok(d)
    }

    pub fn key_columns(&self) -> &[u16] {
        &self.key[..self.key_count as usize]
    }

    /// The index key's column types, in key order — the `types`
    /// argument [`encode_index_value`] wants.
    pub fn key_types(&self, table: &TableDescriptor, out: &mut [LogicalType]) -> Option<usize> {
        let k = self.key_columns();
        if out.len() < k.len() || table.table_id != self.table_id {
            return None;
        }
        for (i, id) in k.iter().enumerate() {
            out[i] = table.column(*id)?.ty;
        }
        Some(k.len())
    }

    /// May a query read this index? Only in [`SchemaPhase::Public`].
    /// Freshness is a *second* gate, not a substitute — see
    /// [`freshness_ok`](Self::freshness_ok).
    pub const fn readable(&self) -> bool {
        read_allowed(self.phase)
    }

    /// The §21-invariant-19 gate, delegated to `db_ops` and fenced by
    /// this descriptor's identity.
    ///
    /// Order of the clauses is the contract: identity first (a
    /// freshness record for a *different* index tells us nothing about
    /// this one and must not read as "fresh"), then the exactness
    /// claim (an index cannot claim an exactness it was not built
    /// with), then `db_ops::index_satisfies_freshness` for the
    /// generation and timestamp arithmetic.
    pub fn freshness_ok(
        &self,
        f: &IndexFreshness,
        required_timestamp: Timestamp,
    ) -> Result<(), CatalogError> {
        if f.index_id != self.index_id {
            return Err(CatalogError::FreshnessWrongIndex {
                expected: self.index_id,
                found: f.index_id,
            });
        }
        if f.exactness != self.exactness {
            return Err(CatalogError::FreshnessExactnessMismatch);
        }
        index_satisfies_freshness(f, required_timestamp, self.index_generation)
            .map_err(CatalogError::Freshness)
    }

    fn flags(&self) -> u8 {
        u8::from(self.unique)
    }

    pub fn wire_len(&self) -> usize {
        INDEX_DESCRIPTOR_FIXED_WIRE_LEN + self.name.len as usize + 2 * self.key_count as usize
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if self.key_count == 0 {
            return None;
        }
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&INDEX_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.index_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.table_id.to_le_bytes());
        out[12..20].copy_from_slice(&self.catalog_revision.to_le_bytes());
        out[20..24].copy_from_slice(&self.index_generation.to_le_bytes());
        out[24] = self.flags();
        out[25] = self.exactness as u8;
        out[26] = self.phase as u8;
        out[27] = self.direction as u8;
        let mut n = self.name.encode(out, 28)?;
        n = put(out, n, &[self.key_count])?;
        for id in self.key_columns() {
            n = put(out, n, &id.to_le_bytes())?;
        }
        debug_assert_eq!(n, total);
        Some(n)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        let payload_len = check_header(src, INDEX_DESCRIPTOR_VERSION)?;
        let mut d = Self::EMPTY;
        d.index_id = u32::from_le_bytes(src.get(4..8)?.try_into().ok()?);
        d.table_id = u32::from_le_bytes(src.get(8..12)?.try_into().ok()?);
        d.catalog_revision = u64::from_le_bytes(src.get(12..20)?.try_into().ok()?);
        d.index_generation = u32::from_le_bytes(src.get(20..24)?.try_into().ok()?);
        let flags = *src.get(24)?;
        if flags & !INDEX_FLAG_MASK != 0 {
            return None;
        }
        d.unique = flags & INDEX_FLAG_UNIQUE != 0;
        d.exactness = IndexExactness::from_u8(*src.get(25)?)?;
        d.phase = SchemaPhase::from_u8(*src.get(26)?)?;
        d.direction = SchemaDirection::from_u8(*src.get(27)?)?;
        let (name, mut n) = Name::decode(src, 28)?;
        d.name = name;
        let key_count = usize::from(*src.get(n)?);
        n += 1;
        if key_count == 0 || key_count > MAX_KEY_COLUMNS {
            return None;
        }
        for i in 0..key_count {
            d.key[i] = u16::from_le_bytes(src.get(n..n + 2)?.try_into().ok()?);
            n += 2;
        }
        d.key_count = key_count as u8;
        if n != 4 + payload_len {
            return None;
        }
        Some(d)
    }
}

// ── SequenceDescriptor ────────────────────────────────────────────────

/// Sequence flag bits.
pub const SEQUENCE_FLAG_CYCLE: u8 = 0b0000_0001;
const SEQUENCE_FLAG_MASK: u8 = SEQUENCE_FLAG_CYCLE;

/// A sequence (§14.2's "sequence ... catalog").
///
/// §14.11 assigns durable sequences to "Lattice transactional or lease
/// records" — the compute holds no authority over them, which is §21
/// invariant 15. This descriptor is the durable shape; [`sequence_next`]
/// is the pure arithmetic a transaction applies to it. Nothing here
/// caches, allocates a block, or reads a clock.
///
/// Wire layout ([`SEQUENCE_DESCRIPTOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [sequence_id:u32][database_id:u32][catalog_revision:u64]
/// [start:i64][increment:i64][min_value:i64][max_value:i64]
/// [cache:u32][flags:u8]
/// [name_len:u8][name...]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceDescriptor {
    pub sequence_id: u32,
    pub database_id: u32,
    pub catalog_revision: u64,
    pub start: i64,
    /// Never zero — a zero increment never advances and never
    /// terminates.
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    /// How many values one durable allocation may hand out. Recorded
    /// so a cached block is auditable; the cache itself lives in
    /// compute and is disposable (§14.11).
    pub cache: u32,
    pub cycle: bool,
    pub name: Name,
}

/// Fixed wire overhead of a [`SequenceDescriptor`] (excluding name bytes).
pub const SEQUENCE_DESCRIPTOR_FIXED_WIRE_LEN: usize = 4 + 4 + 4 + 8 + 8 + 8 + 8 + 8 + 4 + 1 + 1;

/// Ways sequence arithmetic refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequenceError {
    /// A zero increment, or bounds that cross.
    Malformed,
    /// The sequence ran off its bound and does not cycle. A refusal,
    /// not a wrap: silently wrapping a non-cycling sequence hands out a
    /// duplicate key.
    Exhausted,
}

impl SequenceDescriptor {
    #[allow(
        clippy::too_many_arguments,
        reason = "contract constructor mirrors the SQL sequence declaration one-to-one"
    )]
    pub fn new(
        sequence_id: u32,
        database_id: u32,
        catalog_revision: u64,
        name: &[u8],
        start: i64,
        increment: i64,
        min_value: i64,
        max_value: i64,
        cycle: bool,
        cache: u32,
    ) -> Option<Self> {
        let d = Self {
            sequence_id,
            database_id,
            catalog_revision,
            start,
            increment,
            min_value,
            max_value,
            cache,
            cycle,
            name: Name::new(name)?,
        };
        d.check_invariants().ok()?;
        Some(d)
    }

    pub fn check_invariants(&self) -> Result<(), SequenceError> {
        if self.increment == 0
            || self.min_value > self.max_value
            || self.start < self.min_value
            || self.start > self.max_value
        {
            return Err(SequenceError::Malformed);
        }
        Ok(())
    }

    pub fn wire_len(&self) -> usize {
        SEQUENCE_DESCRIPTOR_FIXED_WIRE_LEN + self.name.len as usize
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        self.check_invariants().ok()?;
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&SEQUENCE_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.sequence_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.database_id.to_le_bytes());
        out[12..20].copy_from_slice(&self.catalog_revision.to_le_bytes());
        out[20..28].copy_from_slice(&self.start.to_le_bytes());
        out[28..36].copy_from_slice(&self.increment.to_le_bytes());
        out[36..44].copy_from_slice(&self.min_value.to_le_bytes());
        out[44..52].copy_from_slice(&self.max_value.to_le_bytes());
        out[52..56].copy_from_slice(&self.cache.to_le_bytes());
        out[56] = u8::from(self.cycle);
        let n = self.name.encode(out, 57)?;
        debug_assert_eq!(n, total);
        Some(n)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        let payload_len = check_header(src, SEQUENCE_DESCRIPTOR_VERSION)?;
        let flags = *src.get(56)?;
        if flags & !SEQUENCE_FLAG_MASK != 0 {
            return None;
        }
        let (name, n) = Name::decode(src, 57)?;
        if n != 4 + payload_len {
            return None;
        }
        let d = Self {
            sequence_id: u32::from_le_bytes(src.get(4..8)?.try_into().ok()?),
            database_id: u32::from_le_bytes(src.get(8..12)?.try_into().ok()?),
            catalog_revision: u64::from_le_bytes(src.get(12..20)?.try_into().ok()?),
            start: i64::from_le_bytes(src.get(20..28)?.try_into().ok()?),
            increment: i64::from_le_bytes(src.get(28..36)?.try_into().ok()?),
            min_value: i64::from_le_bytes(src.get(36..44)?.try_into().ok()?),
            max_value: i64::from_le_bytes(src.get(44..52)?.try_into().ok()?),
            cache: u32::from_le_bytes(src.get(52..56)?.try_into().ok()?),
            cycle: flags & SEQUENCE_FLAG_CYCLE != 0,
            name,
        };
        d.check_invariants().ok()?;
        Some(d)
    }
}

/// The next value of `d` after `current`, or [`SequenceError::Exhausted`].
///
/// Pure: no clock, no cache, no I/O. The caller applies it inside a
/// Lattice transaction against the durable record, which is what makes
/// the sequence's authority live in the range rather than in the
/// compute worker (§14.11, §21 invariant 15).
///
/// Overflow of `i64` itself is treated as running off the bound, so a
/// cycling sequence wraps to its far end and a non-cycling one refuses
/// — never a wrapped negative masquerading as the next id.
pub fn sequence_next(d: &SequenceDescriptor, current: i64) -> Result<i64, SequenceError> {
    d.check_invariants()?;
    let ascending = d.increment > 0;
    match current.checked_add(d.increment) {
        Some(next) if ascending && next <= d.max_value => Ok(next),
        Some(next) if !ascending && next >= d.min_value => Ok(next),
        _ if d.cycle => Ok(if ascending { d.min_value } else { d.max_value }),
        _ => Err(SequenceError::Exhausted),
    }
}

// ══════════════════════════════════════════════════════════════════════
// 9. Schema-change state machine (§14.5)
// ══════════════════════════════════════════════════════════════════════
//
// > Schema changes are resumable state machines recorded through
// > Lattice. Publication uses versioned descriptors so old and new
// > binaries can identify compatible read and write phases. Backfill,
// > validation, cutover, and cleanup each have explicit fences and
// > retention claims.
//
// The classic safe sequence, and why each step exists:
//
// ```text
// ADD                                          reads  ins  del
// Absent       nobody knows the index            no    no   no
// DeleteOnly   writers must DELETE entries       no    no  yes
// WriteOnly    writers must INSERT and DELETE    no   yes  yes
// Backfilling  historic rows being written       no   yes  yes
// Public       queries may use the index        yes   yes  yes
//
// DROP  (the reverse; Backfilling has no role — nothing is being built)
// Public → WriteOnly → DeleteOnly → Absent
// ```
//
// The step that looks redundant, `DeleteOnly`, is the one that makes
// the whole thing safe. Two adjacent phases can be live in the fleet at
// once. If `Absent` went straight to `WriteOnly`, a node still at
// `Absent` could delete a row without removing its index entry, and a
// node at `WriteOnly` would then insert a *new* row whose key collides
// with the stale entry — for a unique index, a spurious conflict; for a
// non-unique one, an entry pointing at a row that no longer exists.
// Requiring deletes one phase before inserts means no entry can outlive
// its row.
//
// The mirror-image rule guards reads, and it is the property
// [`adjacent_phases_safe`] makes executable: **a phase may permit index
// reads only if the phase before it already required full maintenance
// (both insert and delete).** `Public`'s predecessor is `Backfilling`,
// which requires both, so a reader at `Public` cannot miss an entry a
// concurrent writer at `Backfilling` was not obliged to make.

/// Which way a schema job is moving. The phase *names* are shared; the
/// direction says which order they run in and therefore what "advance"
/// and "rollback" mean.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaDirection {
    /// `Absent → DeleteOnly → WriteOnly → Backfilling → Public`.
    Add = 1,
    /// `Public → WriteOnly → DeleteOnly → Absent`.
    Drop = 2,
}

impl SchemaDirection {
    pub const ALL: [Self; 2] = [Self::Add, Self::Drop];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Add),
            2 => Some(Self::Drop),
            _ => None,
        }
    }
}

/// The publication phase of an index (or any added/dropped schema
/// element) — §14.5's "compatible read and write phases".
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a phase.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaPhase {
    Absent = 1,
    DeleteOnly = 2,
    WriteOnly = 3,
    Backfilling = 4,
    Public = 5,
}

impl SchemaPhase {
    pub const ALL: [Self; 5] = [
        Self::Absent,
        Self::DeleteOnly,
        Self::WriteOnly,
        Self::Backfilling,
        Self::Public,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Absent),
            2 => Some(Self::DeleteOnly),
            3 => Some(Self::WriteOnly),
            4 => Some(Self::Backfilling),
            5 => Some(Self::Public),
            _ => None,
        }
    }

    /// Position along a job's own direction, `0` at its start. Advance
    /// increases rank by exactly one; rollback decreases it by one.
    /// `Backfilling` has no rank in the drop direction — nothing is
    /// being built — and [`rank`](Self::rank) returns `None` there.
    pub const fn rank(self, dir: SchemaDirection) -> Option<u8> {
        match (dir, self) {
            (SchemaDirection::Add, Self::Absent) => Some(0),
            (SchemaDirection::Add, Self::DeleteOnly) => Some(1),
            (SchemaDirection::Add, Self::WriteOnly) => Some(2),
            (SchemaDirection::Add, Self::Backfilling) => Some(3),
            (SchemaDirection::Add, Self::Public) => Some(4),
            (SchemaDirection::Drop, Self::Public) => Some(0),
            (SchemaDirection::Drop, Self::WriteOnly) => Some(1),
            (SchemaDirection::Drop, Self::DeleteOnly) => Some(2),
            (SchemaDirection::Drop, Self::Absent) => Some(3),
            (SchemaDirection::Drop, Self::Backfilling) => None,
        }
    }

    /// The phase at `rank` in `dir`, if any.
    pub const fn at_rank(dir: SchemaDirection, rank: u8) -> Option<Self> {
        match (dir, rank) {
            (SchemaDirection::Add, 0) | (SchemaDirection::Drop, 3) => Some(Self::Absent),
            (SchemaDirection::Add, 1) | (SchemaDirection::Drop, 2) => Some(Self::DeleteOnly),
            (SchemaDirection::Add, 2) | (SchemaDirection::Drop, 1) => Some(Self::WriteOnly),
            (SchemaDirection::Add, 3) => Some(Self::Backfilling),
            (SchemaDirection::Add, 4) | (SchemaDirection::Drop, 0) => Some(Self::Public),
            _ => None,
        }
    }

    /// The terminal phase of `dir`.
    pub const fn terminal(dir: SchemaDirection) -> Self {
        match dir {
            SchemaDirection::Add => Self::Public,
            SchemaDirection::Drop => Self::Absent,
        }
    }
}

/// May a query read the index at this phase? **Only `Public`.**
///
/// Not "Public or Backfilling": during backfill the historic rows have
/// not all been written, so a read would miss committed data with no
/// way to detect the miss.
pub const fn read_allowed(phase: SchemaPhase) -> bool {
    matches!(phase, SchemaPhase::Public)
}

/// Must a writer maintain the index at this phase at all?
///
/// True from `DeleteOnly` onward. `Absent` is the only phase where a
/// writer may ignore the index entirely.
pub const fn write_allowed(phase: SchemaPhase) -> bool {
    !matches!(phase, SchemaPhase::Absent)
}

/// Must a writer INSERT index entries for new rows at this phase?
pub const fn write_insert_allowed(phase: SchemaPhase) -> bool {
    matches!(
        phase,
        SchemaPhase::WriteOnly | SchemaPhase::Backfilling | SchemaPhase::Public
    )
}

/// Must a writer DELETE index entries for removed rows at this phase?
///
/// Deletes are required one phase *before* inserts. That asymmetry is
/// the reason `DeleteOnly` exists; see the section header.
pub const fn write_delete_allowed(phase: SchemaPhase) -> bool {
    !matches!(phase, SchemaPhase::Absent)
}

/// The safety property, executable.
///
/// Given two phases that may be live in the fleet at the same time
/// (`prev` and its immediate successor `next`), is the pair safe? It is
/// safe iff `next` permitting reads implies `prev` already required
/// **both** insert and delete maintenance. Any pair that violates this
/// admits a read at `next` that a concurrent writer at `prev` could
/// have missed.
///
/// This is a property over *phase pairs*, independent of the transition
/// lattice: the test enumerates all 25 ordered pairs and asserts the
/// only ones the lattice permits are safe ones.
pub const fn adjacent_phases_safe(prev: SchemaPhase, next: SchemaPhase) -> bool {
    !read_allowed(next) || (write_insert_allowed(prev) && write_delete_allowed(prev))
}

/// Ways a schema-change transition refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaError {
    /// A transition outside the direction's lattice, including any
    /// move out of a terminal phase and any skipped phase.
    IllegalTransition {
        from: SchemaPhase,
        to: SchemaPhase,
        direction: SchemaDirection,
    },
    /// §14.5: a phase may only advance once **every** node is known to
    /// be at-or-past the previous phase. `fleet_min` is the slowest
    /// node's phase; advancing while it lags would put two
    /// non-adjacent phases in the fleet at once and break the
    /// adjacent-pair safety argument.
    FleetNotConverged {
        current: SchemaPhase,
        fleet_min: SchemaPhase,
    },
    /// Entering `Backfilling` without a retention claim. §14.5 requires
    /// backfill to carry an explicit retention claim; without one the
    /// history the backfill reads can be compacted out from under it
    /// (§18, §21 invariant 13).
    BackfillWithoutRetentionClaim,
    /// Rollback was requested where there is nothing to roll back to:
    /// the job's start (nothing done) or its terminal phase (the change
    /// is published or the entries are gone — undoing it is a new job
    /// in the other direction, not a rollback).
    NoRollbackTarget {
        phase: SchemaPhase,
    },
    /// A phase that does not exist in this direction — `Backfilling`
    /// in a drop job.
    PhaseNotInDirection {
        phase: SchemaPhase,
        direction: SchemaDirection,
    },
    OutOfBounds,
}

/// Is `from → to` a legal single step in `dir`?
///
/// Exactly one step forward. No skipping (a skipped phase is precisely
/// what the fleet-convergence rule exists to prevent), no jumping
/// direction, no leaving the terminal phase.
pub fn schema_transition(
    dir: SchemaDirection,
    from: SchemaPhase,
    to: SchemaPhase,
) -> Result<(), SchemaError> {
    let (Some(a), Some(b)) = (from.rank(dir), to.rank(dir)) else {
        let phase = if from.rank(dir).is_none() { from } else { to };
        return Err(SchemaError::PhaseNotInDirection {
            phase,
            direction: dir,
        });
    };
    if b == a + 1 {
        Ok(())
    } else {
        Err(SchemaError::IllegalTransition {
            from,
            to,
            direction: dir,
        })
    }
}

/// The phase a job at `phase` rolls back to: exactly one step *back*
/// along its own direction.
///
/// `None` at both ends, and the two `None`s mean different things:
///
/// - at the job's start there is nothing to undo;
/// - at the job's terminal phase the change is complete — an added
///   index is `Public` and visible to queries, a dropped one has had
///   its entries removed. Undoing either is a **new job in the opposite
///   direction**, not a rollback, and pretending otherwise would let a
///   rollback silently un-drop entries that no longer exist.
///
/// That boundary is the rollback contract.
pub fn rollback_target(dir: SchemaDirection, phase: SchemaPhase) -> Option<SchemaPhase> {
    let r = phase.rank(dir)?;
    if r == 0 {
        return None;
    }
    if phase == SchemaPhase::terminal(dir) {
        return None;
    }
    SchemaPhase::at_rank(dir, r - 1)
}

/// A resumable schema-change job (§14.5), recorded in
/// `relational/job/<schema-job-id>`.
///
/// Wire layout ([`SCHEMA_JOB_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [job_id:u64][index_id:u32][table_id:u32]
/// [catalog_revision:u64][retention_claim_ts:u64]
/// [direction:u8][phase:u8]
/// [backfill_watermark_len:u16][backfill_watermark...]
/// ```
///
/// `backfill_watermark` is the encoded primary key the backfill has
/// completed through — the resume point. Empty means "not started".
/// Storing it as a key rather than a row count is what makes the job
/// resumable across a compute failure: a replacement worker re-reads
/// the job record and continues from the watermark (§14.11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaJob {
    pub job_id: u64,
    pub index_id: u32,
    pub table_id: u32,
    /// The catalog revision this job was planned against. A plan bound
    /// to an older revision must rebind (§14.5).
    pub catalog_revision: u64,
    /// The protected timestamp the backfill claims (§18). Zero means
    /// no claim, and entering `Backfilling` without one is refused.
    pub retention_claim_ts: Timestamp,
    pub direction: SchemaDirection,
    phase: SchemaPhase,
    watermark: [u8; MAX_PRIMARY_KEY_LEN],
    watermark_len: u16,
}

/// Fixed wire overhead of a [`SchemaJob`] (excluding the watermark).
pub const SCHEMA_JOB_FIXED_WIRE_LEN: usize = 4 + 8 + 4 + 4 + 8 + 8 + 1 + 1 + 2;

impl SchemaJob {
    pub const EMPTY: SchemaJob = SchemaJob {
        job_id: 0,
        index_id: 0,
        table_id: 0,
        catalog_revision: 0,
        retention_claim_ts: 0,
        direction: SchemaDirection::Add,
        phase: SchemaPhase::Absent,
        watermark: [0; MAX_PRIMARY_KEY_LEN],
        watermark_len: 0,
    };

    /// Begin a job at its direction's starting phase.
    pub fn begin(
        job_id: u64,
        index_id: u32,
        table_id: u32,
        catalog_revision: u64,
        direction: SchemaDirection,
    ) -> Self {
        let mut j = Self::EMPTY;
        j.job_id = job_id;
        j.index_id = index_id;
        j.table_id = table_id;
        j.catalog_revision = catalog_revision;
        j.direction = direction;
        j.phase = match direction {
            SchemaDirection::Add => SchemaPhase::Absent,
            SchemaDirection::Drop => SchemaPhase::Public,
        };
        j
    }

    pub const fn phase(&self) -> SchemaPhase {
        self.phase
    }

    pub const fn is_complete(&self) -> bool {
        matches!(
            (self.direction, self.phase),
            (SchemaDirection::Add, SchemaPhase::Public)
                | (SchemaDirection::Drop, SchemaPhase::Absent)
        )
    }

    pub fn backfill_watermark(&self) -> &[u8] {
        &self.watermark[..self.watermark_len as usize]
    }

    /// Record backfill progress. The watermark must advance: a
    /// watermark that moved backwards would re-scan rows already
    /// written and, worse, would let a resumed job claim progress it
    /// then loses.
    pub fn record_backfill_progress(&mut self, key: &[u8]) -> Result<(), SchemaError> {
        if key.len() > MAX_PRIMARY_KEY_LEN {
            return Err(SchemaError::OutOfBounds);
        }
        if key <= self.backfill_watermark() {
            return Err(SchemaError::OutOfBounds);
        }
        self.watermark[..key.len()].copy_from_slice(key);
        self.watermark_len = key.len() as u16;
        Ok(())
    }

    /// Advance one phase.
    ///
    /// `fleet_min_phase` is the *slowest* node's observed phase. The
    /// clause order is the contract:
    ///
    /// 1. the transition must be legal in this direction (no skips, no
    ///    reversals, no leaving terminal);
    /// 2. the fleet must be at-or-past the phase we are leaving —
    ///    §14.5's convergence fence, and the reason
    ///    [`adjacent_phases_safe`] only ever has to reason about
    ///    *adjacent* pairs;
    /// 3. entering `Backfilling` requires a retention claim.
    ///
    /// A refusal leaves the job untouched, so a crash mid-advance
    /// leaves a resumable job at a legal phase — never a half-advanced
    /// one.
    pub fn advance(
        &mut self,
        to: SchemaPhase,
        fleet_min_phase: SchemaPhase,
    ) -> Result<(), SchemaError> {
        schema_transition(self.direction, self.phase, to)?;
        let Some(min) = fleet_min_phase.rank(self.direction) else {
            return Err(SchemaError::PhaseNotInDirection {
                phase: fleet_min_phase,
                direction: self.direction,
            });
        };
        let cur = self
            .phase
            .rank(self.direction)
            .ok_or(SchemaError::PhaseNotInDirection {
                phase: self.phase,
                direction: self.direction,
            })?;
        if min < cur {
            return Err(SchemaError::FleetNotConverged {
                current: self.phase,
                fleet_min: fleet_min_phase,
            });
        }
        if to == SchemaPhase::Backfilling && self.retention_claim_ts == 0 {
            return Err(SchemaError::BackfillWithoutRetentionClaim);
        }
        self.phase = to;
        Ok(())
    }

    /// Roll back one phase. Refuses at both ends — see
    /// [`rollback_target`].
    pub fn rollback(&mut self) -> Result<SchemaPhase, SchemaError> {
        let to = rollback_target(self.direction, self.phase)
            .ok_or(SchemaError::NoRollbackTarget { phase: self.phase })?;
        self.phase = to;
        Ok(to)
    }

    pub fn wire_len(&self) -> usize {
        SCHEMA_JOB_FIXED_WIRE_LEN + self.watermark_len as usize
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&SCHEMA_JOB_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..12].copy_from_slice(&self.job_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.index_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.table_id.to_le_bytes());
        out[20..28].copy_from_slice(&self.catalog_revision.to_le_bytes());
        out[28..36].copy_from_slice(&self.retention_claim_ts.to_le_bytes());
        out[36] = self.direction as u8;
        out[37] = self.phase as u8;
        out[38..40].copy_from_slice(&self.watermark_len.to_le_bytes());
        let n = put(out, 40, self.backfill_watermark())?;
        debug_assert_eq!(n, total);
        Some(n)
    }

    /// Fails closed on unknown version, length mismatch, an unknown
    /// direction or phase, a phase that does not exist in its own
    /// direction, or an oversized watermark.
    pub fn decode(src: &[u8]) -> Option<Self> {
        let payload_len = check_header(src, SCHEMA_JOB_VERSION)?;
        let mut j = Self::EMPTY;
        j.job_id = u64::from_le_bytes(src.get(4..12)?.try_into().ok()?);
        j.index_id = u32::from_le_bytes(src.get(12..16)?.try_into().ok()?);
        j.table_id = u32::from_le_bytes(src.get(16..20)?.try_into().ok()?);
        j.catalog_revision = u64::from_le_bytes(src.get(20..28)?.try_into().ok()?);
        j.retention_claim_ts = u64::from_le_bytes(src.get(28..36)?.try_into().ok()?);
        j.direction = SchemaDirection::from_u8(*src.get(36)?)?;
        j.phase = SchemaPhase::from_u8(*src.get(37)?)?;
        j.phase.rank(j.direction)?;
        let wlen = usize::from(u16::from_le_bytes(src.get(38..40)?.try_into().ok()?));
        if wlen > MAX_PRIMARY_KEY_LEN || 40 + wlen != 4 + payload_len {
            return None;
        }
        j.watermark[..wlen].copy_from_slice(src.get(40..40 + wlen)?);
        j.watermark_len = wlen as u16;
        Some(j)
    }
}

// ══════════════════════════════════════════════════════════════════════
// 10. Constraints (§14.2 "unique, check, not-null, and referential
//     constraints")
// ══════════════════════════════════════════════════════════════════════
//
// All four are *pure functions over already-fetched state*. Nothing
// here probes storage. The executor fetches; this file decides. That
// split is what lets the same constraint logic run in a combined graph
// and a separated relational-compute graph and reach the same answer
// (§14.7, §21 invariant 16).

/// Every way a constraint check refuses. No catch-all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConstraintError {
    /// A `NOT NULL` column holds NULL.
    NotNull { column_id: u16 },
    /// A `NOT NULL` column has no entry in the row at all. Distinct
    /// from [`NotNull`](Self::NotNull) because the causes differ: this
    /// one means the writer never supplied the column (or its default),
    /// which is a bug in the write path, not a user error.
    MissingColumn { column_id: u16 },
    /// The row carries a different type for the column than the
    /// descriptor declares, or the row is malformed.
    RowMismatch { column_id: u16 },
    /// A different primary key already owns this unique-index slot.
    UniqueConflict { index_id: u32 },
    /// The candidate ownership presented an epoch older than the stored
    /// record: a delayed message from a superseded attempt (§21
    /// invariant 6).
    UniqueStaleEpoch {
        index_id: u32,
        current: u64,
        presented: u64,
    },
    /// The two ownership records describe different slots — a caller
    /// bug with no safe answer.
    UniqueSlotMismatch { index_id: u32 },
    /// An enforced `CHECK` was not evaluated. Fail closed: an
    /// unevaluated enforced predicate is not a pass.
    CheckNotEvaluated { check_id: u32 },
    /// An enforced `CHECK` evaluated false.
    CheckViolated { check_id: u32 },
    /// The executor returned a verdict for a predicate version this
    /// binary does not recognise (§14.6 — the expression capability's
    /// version contract must match, not merely exist).
    CheckVersionMismatch { check_id: u32, version: u16 },
    /// `MATCH FULL`: some but not all foreign-key columns are NULL.
    ForeignKeyPartialNull { fk_id: u32 },
    /// The plan would need more probes or more key bytes than the
    /// bounds allow.
    OutOfBounds,
    /// A descriptor referenced a column the table does not have.
    Malformed,
}

// ── NOT NULL ──────────────────────────────────────────────────────────

/// Check every `NOT NULL` column of `table` against an encoded row.
///
/// Defaults are applied by the writer *before* this check —
/// `default_present` on a [`ColumnDescriptor`] is metadata, not a value
/// this layer can evaluate (§14.6). So an absent non-nullable column is
/// a refusal here, deliberately: the alternative is inventing a value.
///
/// Returns the *first* violation in column order, which makes the error
/// deterministic across binaries — §21 invariant 3's spirit applied to
/// error reporting.
pub fn check_not_null(table: &TableDescriptor, row: &[u8]) -> Result<(), ConstraintError> {
    for c in table.columns() {
        if c.nullable {
            continue;
        }
        match row_lookup(row, c.column_id, c.ty) {
            None => {
                return Err(ConstraintError::RowMismatch {
                    column_id: c.column_id,
                })
            }
            Some(ColumnLookup::Absent) => {
                return Err(ConstraintError::MissingColumn {
                    column_id: c.column_id,
                })
            }
            Some(ColumnLookup::Present(Value::Null)) => {
                return Err(ConstraintError::NotNull {
                    column_id: c.column_id,
                })
            }
            Some(ColumnLookup::Present(_)) => {}
        }
    }
    Ok(())
}

// ── UNIQUE ────────────────────────────────────────────────────────────

/// The unique-constraint check, delegated to `db_ops`.
///
/// This file does **not** reimplement uniqueness: `db_ops` already owns
/// the ownership record and the conflict rule (including the one that
/// matters — a candidate with a *newer* epoch and a different primary
/// key is still a conflict, because being newer never wins a uniqueness
/// argument). All this adds is the relational framing and typed errors.
///
/// `existing` is `None` when the executor found no ownership record for
/// the slot.
pub fn check_unique(
    index: &IndexDescriptor,
    existing: Option<&UniqueOwnership>,
    candidate: &UniqueOwnership,
) -> Result<(), ConstraintError> {
    if !index.unique {
        return Ok(()); // nothing to enforce on a non-unique index
    }
    match check_unique_ownership(existing, candidate) {
        UniqueOutcome::Vacant | UniqueOutcome::OwnedBySelf => Ok(()),
        UniqueOutcome::Conflict => Err(ConstraintError::UniqueConflict {
            index_id: index.index_id,
        }),
        UniqueOutcome::StaleEpoch { current, presented } => {
            Err(ConstraintError::UniqueStaleEpoch {
                index_id: index.index_id,
                current,
                presented,
            })
        }
        UniqueOutcome::SlotMismatch => Err(ConstraintError::UniqueSlotMismatch {
            index_id: index.index_id,
        }),
    }
}

// ── CHECK ─────────────────────────────────────────────────────────────

/// A `CHECK` constraint, as a **versioned opaque predicate id**.
///
/// This layer deliberately does not evaluate expressions. §14.6 says
/// relational expressions *may* reuse a shared deterministic expression
/// capability, but "PostgreSQL/MySQL correctness must not depend on an
/// optional general-purpose language runtime" — so the shared
/// representation carries the *identity* of the predicate and the
/// version of the evaluator contract it was compiled against, and
/// nothing else. Whoever evaluates it returns a [`CheckVerdict`], and
/// [`apply_check_verdict`] folds that in fail-closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckConstraint {
    pub check_id: u32,
    pub table_id: u32,
    /// Version of the expression-evaluation contract the predicate was
    /// compiled against. A verdict produced under a different version
    /// is refused, not trusted.
    pub predicate_version: u16,
    /// Opaque identity of the compiled predicate. Never interpreted
    /// here.
    pub predicate_id: [u8; 16],
    /// Whether a violation blocks the write. A `NOT VALID` /
    /// not-yet-validated constraint is `enforced: false`.
    pub enforced: bool,
    columns: [u16; MAX_KEY_COLUMNS],
    column_count: u8,
}

/// What the executor reports back about a predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckVerdict {
    Satisfied,
    Violated,
    /// The predicate was not evaluated: no evaluator, a deadline, a
    /// resource bound. Never a pass.
    NotEvaluated {
        reason_version: u16,
    },
}

impl CheckConstraint {
    pub const EMPTY: CheckConstraint = CheckConstraint {
        check_id: 0,
        table_id: 0,
        predicate_version: 0,
        predicate_id: [0; 16],
        enforced: true,
        columns: [0; MAX_KEY_COLUMNS],
        column_count: 0,
    };

    pub fn new(
        check_id: u32,
        table: &TableDescriptor,
        predicate_version: u16,
        predicate_id: [u8; 16],
        columns: &[u16],
        enforced: bool,
    ) -> Result<Self, ConstraintError> {
        if columns.is_empty() || columns.len() > MAX_KEY_COLUMNS {
            return Err(ConstraintError::OutOfBounds);
        }
        let mut c = Self::EMPTY;
        for (i, id) in columns.iter().enumerate() {
            if table.column(*id).is_none() {
                return Err(ConstraintError::Malformed);
            }
            c.columns[i] = *id;
        }
        c.column_count = columns.len() as u8;
        c.check_id = check_id;
        c.table_id = table.table_id;
        c.predicate_version = predicate_version;
        c.predicate_id = predicate_id;
        c.enforced = enforced;
        Ok(c)
    }

    /// The columns the executor must supply to the predicate. Bounded,
    /// declared up front, so the evaluator's input set is knowable
    /// without parsing the predicate.
    pub fn columns(&self) -> &[u16] {
        &self.columns[..self.column_count as usize]
    }
}

/// Fold an executor-supplied verdict into a decision, fail-closed.
///
/// - A non-enforced constraint never blocks a write; its verdict is
///   recorded elsewhere for validation reporting.
/// - `NotEvaluated` on an enforced constraint is a **refusal**. This is
///   the whole reason the verdict is a three-valued enum rather than a
///   `bool`: a two-valued answer would force "unknown" to collapse into
///   one of the other two, and collapsing it into "satisfied" writes
///   rows that violate a declared constraint.
/// - A verdict whose `reason_version` disagrees with the predicate's
///   compiled version is refused (§14.6's version-contract clause).
pub fn apply_check_verdict(
    c: &CheckConstraint,
    verdict: CheckVerdict,
) -> Result<(), ConstraintError> {
    match verdict {
        CheckVerdict::NotEvaluated { reason_version } if reason_version != c.predicate_version => {
            Err(ConstraintError::CheckVersionMismatch {
                check_id: c.check_id,
                version: reason_version,
            })
        }
        _ if !c.enforced => Ok(()),
        CheckVerdict::Satisfied => Ok(()),
        CheckVerdict::Violated => Err(ConstraintError::CheckViolated {
            check_id: c.check_id,
        }),
        CheckVerdict::NotEvaluated { .. } => Err(ConstraintError::CheckNotEvaluated {
            check_id: c.check_id,
        }),
    }
}

// ── REFERENTIAL INTEGRITY ─────────────────────────────────────────────

/// What a foreign-key violation does to the referencing rows.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FkAction {
    NoAction = 1,
    Restrict = 2,
    Cascade = 3,
    SetNull = 4,
    SetDefault = 5,
}

impl FkAction {
    pub const ALL: [Self; 5] = [
        Self::NoAction,
        Self::Restrict,
        Self::Cascade,
        Self::SetNull,
        Self::SetDefault,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::NoAction),
            2 => Some(Self::Restrict),
            3 => Some(Self::Cascade),
            4 => Some(Self::SetNull),
            5 => Some(Self::SetDefault),
            _ => None,
        }
    }
}

/// Foreign-key flag bits.
pub const FK_FLAG_MATCH_FULL: u8 = 0b0000_0001;
const FK_FLAG_MASK: u8 = FK_FLAG_MATCH_FULL;

/// A declared referential constraint.
///
/// Wire layout ([`FOREIGN_KEY_DESCRIPTOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [fk_id:u32][child_table_id:u32][parent_table_id:u32]
/// [parent_index_id:u32][child_index_id:u32][catalog_revision:u64]
/// [flags:u8][on_delete:u8][on_update:u8][column_count:u8]
/// [child_column_id:u16 × n][parent_column_id:u16 × n]
/// ```
///
/// `parent_index_id == 0` means the parent side is the parent table's
/// **primary key**; any other value names a unique index. `child_index_id`
/// names the index used to find children of a parent value; it is `0`
/// when no such index exists, and a parent-side plan then has to
/// declare a table scan, which [`fk_parent_change_plan`] refuses to
/// hide (§14.13 bounded pushdown — an unbounded probe is not a plan).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForeignKeyDescriptor {
    pub fk_id: u32,
    pub child_table_id: u32,
    pub parent_table_id: u32,
    pub parent_index_id: u32,
    pub child_index_id: u32,
    pub catalog_revision: u64,
    /// `MATCH FULL` when true, `MATCH SIMPLE` when false. The
    /// difference only shows up on a partially-NULL key and it is a
    /// real dialect-visible difference, so it is declared rather than
    /// assumed.
    pub match_full: bool,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    child_columns: [u16; MAX_KEY_COLUMNS],
    parent_columns: [u16; MAX_KEY_COLUMNS],
    column_count: u8,
}

/// Fixed wire overhead of a [`ForeignKeyDescriptor`] (excluding columns).
pub const FOREIGN_KEY_DESCRIPTOR_FIXED_WIRE_LEN: usize = 4 + 4 + 4 + 4 + 4 + 4 + 8 + 1 + 1 + 1 + 1;

impl ForeignKeyDescriptor {
    pub const EMPTY: ForeignKeyDescriptor = ForeignKeyDescriptor {
        fk_id: 0,
        child_table_id: 0,
        parent_table_id: 0,
        parent_index_id: 0,
        child_index_id: 0,
        catalog_revision: 0,
        match_full: false,
        on_delete: FkAction::NoAction,
        on_update: FkAction::NoAction,
        child_columns: [0; MAX_KEY_COLUMNS],
        parent_columns: [0; MAX_KEY_COLUMNS],
        column_count: 0,
    };

    #[allow(
        clippy::too_many_arguments,
        reason = "contract constructor mirrors the declared FK shape one-to-one"
    )]
    pub fn new(
        fk_id: u32,
        child: &TableDescriptor,
        parent: &TableDescriptor,
        child_columns: &[u16],
        parent_columns: &[u16],
        match_full: bool,
        on_delete: FkAction,
        on_update: FkAction,
    ) -> Result<Self, ConstraintError> {
        if child_columns.len() != parent_columns.len()
            || child_columns.is_empty()
            || child_columns.len() > MAX_KEY_COLUMNS
        {
            return Err(ConstraintError::OutOfBounds);
        }
        let mut f = Self::EMPTY;
        for i in 0..child_columns.len() {
            let c = child
                .column(child_columns[i])
                .ok_or(ConstraintError::Malformed)?;
            let p = parent
                .column(parent_columns[i])
                .ok_or(ConstraintError::Malformed)?;
            // Types must match exactly. A referential constraint across
            // types would need an implicit cast, and PostgreSQL and
            // MySQL do not agree about implicit casts — so the shared
            // layer refuses rather than picking a dialect (§14.3).
            if c.ty != p.ty {
                return Err(ConstraintError::Malformed);
            }
            f.child_columns[i] = child_columns[i];
            f.parent_columns[i] = parent_columns[i];
        }
        f.column_count = child_columns.len() as u8;
        f.fk_id = fk_id;
        f.child_table_id = child.table_id;
        f.parent_table_id = parent.table_id;
        f.catalog_revision = child.catalog_revision;
        f.match_full = match_full;
        f.on_delete = on_delete;
        f.on_update = on_update;
        Ok(f)
    }

    pub fn child_columns(&self) -> &[u16] {
        &self.child_columns[..self.column_count as usize]
    }

    pub fn parent_columns(&self) -> &[u16] {
        &self.parent_columns[..self.column_count as usize]
    }

    pub fn wire_len(&self) -> usize {
        FOREIGN_KEY_DESCRIPTOR_FIXED_WIRE_LEN + 4 * self.column_count as usize
    }

    fn flags(&self) -> u8 {
        u8::from(self.match_full)
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if self.column_count == 0 {
            return None;
        }
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&FOREIGN_KEY_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.fk_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.child_table_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.parent_table_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.parent_index_id.to_le_bytes());
        out[20..24].copy_from_slice(&self.child_index_id.to_le_bytes());
        out[24..32].copy_from_slice(&self.catalog_revision.to_le_bytes());
        out[32] = self.flags();
        out[33] = self.on_delete as u8;
        out[34] = self.on_update as u8;
        out[35] = self.column_count;
        let mut n = 36;
        for id in self.child_columns() {
            n = put(out, n, &id.to_le_bytes())?;
        }
        for id in self.parent_columns() {
            n = put(out, n, &id.to_le_bytes())?;
        }
        debug_assert_eq!(n, total);
        Some(n)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        let payload_len = check_header(src, FOREIGN_KEY_DESCRIPTOR_VERSION)?;
        let mut f = Self::EMPTY;
        f.fk_id = u32::from_le_bytes(src.get(4..8)?.try_into().ok()?);
        f.child_table_id = u32::from_le_bytes(src.get(8..12)?.try_into().ok()?);
        f.parent_table_id = u32::from_le_bytes(src.get(12..16)?.try_into().ok()?);
        f.parent_index_id = u32::from_le_bytes(src.get(16..20)?.try_into().ok()?);
        f.child_index_id = u32::from_le_bytes(src.get(20..24)?.try_into().ok()?);
        f.catalog_revision = u64::from_le_bytes(src.get(24..32)?.try_into().ok()?);
        let flags = *src.get(32)?;
        if flags & !FK_FLAG_MASK != 0 {
            return None;
        }
        f.match_full = flags & FK_FLAG_MATCH_FULL != 0;
        f.on_delete = FkAction::from_u8(*src.get(33)?)?;
        f.on_update = FkAction::from_u8(*src.get(34)?)?;
        let n_cols = usize::from(*src.get(35)?);
        if n_cols == 0 || n_cols > MAX_KEY_COLUMNS {
            return None;
        }
        let mut n = 36;
        for i in 0..n_cols {
            f.child_columns[i] = u16::from_le_bytes(src.get(n..n + 2)?.try_into().ok()?);
            n += 2;
        }
        for i in 0..n_cols {
            f.parent_columns[i] = u16::from_le_bytes(src.get(n..n + 2)?.try_into().ok()?);
            n += 2;
        }
        f.column_count = n_cols as u8;
        if n != 4 + payload_len {
            return None;
        }
        Some(f)
    }
}

/// Where a probe key lives, so the executor knows which wrapper to
/// apply before touching storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeTarget {
    /// The bytes are a primary-key composite for `table_id`; wrap with
    /// `internal_key::encode` under [`KS_RELATIONAL_TABLE`].
    PrimaryKey { table_id: u32 },
    /// The bytes are an index-value tuple for `index_id`; wrap with
    /// `db_ops::index_prefix_for` under [`KS_RELATIONAL_INDEX`].
    IndexValue { index_id: u32 },
    /// No index exists to answer this probe. The executor must either
    /// refuse or plan an explicit bounded scan; this variant exists so
    /// that fact is *declared* rather than discovered at runtime
    /// (§14.13).
    UnindexedScan { table_id: u32 },
}

/// What the executor must observe for the constraint to hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "the shared `Must` prefix is the point: every variant states an obligation the executor must satisfy, and dropping it would read as a description of what was found"
)]
pub enum ProbeExpectation {
    /// Every probe key must resolve to a live row. Child insert/update.
    MustExist,
    /// No probe key may resolve to a live row. Parent delete/update
    /// under `NO ACTION` / `RESTRICT`.
    MustNotExist,
    /// Every matching row must be enumerated and acted on. Parent
    /// delete/update under `CASCADE` / `SET NULL` / `SET DEFAULT`.
    MustEnumerate,
}

/// A bounded lookup specification: exactly which keys must be probed,
/// where, and what the answer has to be.
///
/// The executor does the probing. This struct is the complete statement
/// of what it must probe — bounded by [`MAX_FK_PROBES`] keys of
/// [`MAX_PROBE_KEY_LEN`] bytes, so a referential check can never turn
/// into an unbounded fan-out (§21 invariant 14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FkProbePlan {
    pub fk_id: u32,
    /// False when NULL semantics discharge the constraint outright. The
    /// executor performs no probe and the constraint holds.
    pub required: bool,
    pub target: ProbeTarget,
    pub expectation: ProbeExpectation,
    keys: [[u8; MAX_PROBE_KEY_LEN]; MAX_FK_PROBES],
    key_lens: [u16; MAX_FK_PROBES],
    probe_count: u8,
}

impl FkProbePlan {
    const EMPTY: FkProbePlan = FkProbePlan {
        fk_id: 0,
        required: false,
        target: ProbeTarget::PrimaryKey { table_id: 0 },
        expectation: ProbeExpectation::MustExist,
        keys: [[0; MAX_PROBE_KEY_LEN]; MAX_FK_PROBES],
        key_lens: [0; MAX_FK_PROBES],
        probe_count: 0,
    };

    pub fn probe_count(&self) -> usize {
        self.probe_count as usize
    }

    pub fn probe(&self, i: usize) -> Option<&[u8]> {
        if i >= self.probe_count as usize {
            return None;
        }
        Some(&self.keys[i][..self.key_lens[i] as usize])
    }

    fn push(&mut self, key: &[u8]) -> Result<(), ConstraintError> {
        let i = self.probe_count as usize;
        if i >= MAX_FK_PROBES || key.len() > MAX_PROBE_KEY_LEN {
            return Err(ConstraintError::OutOfBounds);
        }
        self.keys[i][..key.len()].copy_from_slice(key);
        self.key_lens[i] = key.len() as u16;
        self.probe_count += 1;
        Ok(())
    }
}

/// Read the FK columns out of a child row into `values`, reporting how
/// many were NULL.
fn fk_child_values<'r>(
    row: &'r [u8],
    fk: &ForeignKeyDescriptor,
    child: &TableDescriptor,
    values: &mut [Value<'r>],
    types: &mut [LogicalType],
) -> Result<usize, ConstraintError> {
    let cols = fk.child_columns();
    if values.len() < cols.len() || types.len() < cols.len() {
        return Err(ConstraintError::OutOfBounds);
    }
    let mut nulls = 0usize;
    for (i, id) in cols.iter().enumerate() {
        let c = child.column(*id).ok_or(ConstraintError::Malformed)?;
        types[i] = c.ty;
        match row_lookup(row, *id, c.ty) {
            None => return Err(ConstraintError::RowMismatch { column_id: *id }),
            Some(ColumnLookup::Absent) => {
                return Err(ConstraintError::MissingColumn { column_id: *id })
            }
            Some(ColumnLookup::Present(v)) => {
                if v.is_null() {
                    nulls += 1;
                }
                values[i] = v;
            }
        }
    }
    Ok(nulls)
}

/// The child-side referential check: an `INSERT` or `UPDATE` on the
/// referencing table must find its parent.
///
/// NULL semantics come first, because they can discharge the constraint
/// without any probe at all:
///
/// - **all** FK columns NULL — satisfied under both `MATCH SIMPLE` and
///   `MATCH FULL`; `required` is false and no key is emitted;
/// - **some** NULL, `MATCH SIMPLE` — satisfied, no probe. This is the
///   SQL default and it is the one people are surprised by, so it is
///   spelled out here rather than left to the executor;
/// - **some** NULL, `MATCH FULL` — a violation, refused immediately.
///
/// Otherwise exactly **one** probe is emitted: the parent key composite
/// built from the child's values, which must resolve to a live row.
/// One, not one-per-column — the parent side is a single unique tuple
/// by construction, and emitting more would be the executor doing work
/// the constraint does not imply.
pub fn fk_check_plan(
    row: &[u8],
    fk: &ForeignKeyDescriptor,
    child: &TableDescriptor,
    parent: &TableDescriptor,
) -> Result<FkProbePlan, ConstraintError> {
    if child.table_id != fk.child_table_id || parent.table_id != fk.parent_table_id {
        return Err(ConstraintError::Malformed);
    }
    let mut values = [Value::Null; MAX_KEY_COLUMNS];
    let mut types = [LogicalType::Null; MAX_KEY_COLUMNS];
    let n = fk.child_columns().len();
    let nulls = fk_child_values(row, fk, child, &mut values, &mut types)?;

    let mut plan = FkProbePlan::EMPTY;
    plan.fk_id = fk.fk_id;
    plan.expectation = ProbeExpectation::MustExist;
    plan.target = if fk.parent_index_id == 0 {
        ProbeTarget::PrimaryKey {
            table_id: fk.parent_table_id,
        }
    } else {
        ProbeTarget::IndexValue {
            index_id: fk.parent_index_id,
        }
    };

    if nulls == n {
        return Ok(plan); // required stays false
    }
    if nulls > 0 {
        if fk.match_full {
            return Err(ConstraintError::ForeignKeyPartialNull { fk_id: fk.fk_id });
        }
        return Ok(plan); // MATCH SIMPLE: partially NULL discharges
    }

    // The parent columns' types are the parent's, and `new` already
    // proved them equal to the child's pairwise — so `types` is correct
    // for the parent composite too.
    let mut buf = [0u8; MAX_PROBE_KEY_LEN];
    let len = match plan.target {
        ProbeTarget::PrimaryKey { table_id } => {
            encode_primary_key(&mut buf, table_id, &types[..n], &values[..n])
                .ok_or(ConstraintError::OutOfBounds)?
        }
        ProbeTarget::IndexValue { .. } => encode_index_value(&mut buf, &types[..n], &values[..n])
            .ok_or(ConstraintError::OutOfBounds)?,
        ProbeTarget::UnindexedScan { .. } => return Err(ConstraintError::Malformed),
    };
    plan.push(&buf[..len])?;
    plan.required = true;
    Ok(plan)
}

/// The parent-side referential check: a `DELETE` or key-changing
/// `UPDATE` on the referenced table must account for its children.
///
/// `new_row` is `Some` for an update whose key columns changed, `None`
/// for a delete. An update emits **two** probes — the old key value
/// (children that would be orphaned) and the new key value (rows that
/// silently become children) — which is why [`MAX_FK_PROBES`] is two.
///
/// The expectation follows the declared action: `NO ACTION`/`RESTRICT`
/// require that no child match, everything else requires the children
/// be enumerated and acted on. `SET NULL`/`SET DEFAULT`/`CASCADE` are
/// not carried out here; the executor does the writing.
///
/// If the child has no index on the FK columns (`child_index_id == 0`)
/// the target is [`ProbeTarget::UnindexedScan`] and the plan *says so*.
/// A referential check that silently becomes a table scan is an
/// unbounded operation wearing a constraint's clothes (§14.13, §21
/// invariant 14).
pub fn fk_parent_change_plan(
    old_row: &[u8],
    new_row: Option<&[u8]>,
    fk: &ForeignKeyDescriptor,
    parent: &TableDescriptor,
) -> Result<FkProbePlan, ConstraintError> {
    if parent.table_id != fk.parent_table_id {
        return Err(ConstraintError::Malformed);
    }
    let cols = fk.parent_columns();
    let n = cols.len();
    let mut plan = FkProbePlan::EMPTY;
    plan.fk_id = fk.fk_id;
    plan.target = if fk.child_index_id == 0 {
        ProbeTarget::UnindexedScan {
            table_id: fk.child_table_id,
        }
    } else {
        ProbeTarget::IndexValue {
            index_id: fk.child_index_id,
        }
    };
    plan.expectation = match fk.on_delete {
        FkAction::NoAction | FkAction::Restrict => ProbeExpectation::MustNotExist,
        FkAction::Cascade | FkAction::SetNull | FkAction::SetDefault => {
            ProbeExpectation::MustEnumerate
        }
    };

    for row in [Some(old_row), new_row].into_iter().flatten() {
        let mut values = [Value::Null; MAX_KEY_COLUMNS];
        let mut types = [LogicalType::Null; MAX_KEY_COLUMNS];
        let mut nulls = 0usize;
        for (i, id) in cols.iter().enumerate() {
            let c = parent.column(*id).ok_or(ConstraintError::Malformed)?;
            types[i] = c.ty;
            match row_lookup(row, *id, c.ty) {
                None => return Err(ConstraintError::RowMismatch { column_id: *id }),
                Some(ColumnLookup::Absent) => {
                    return Err(ConstraintError::MissingColumn { column_id: *id })
                }
                Some(ColumnLookup::Present(v)) => {
                    if v.is_null() {
                        nulls += 1;
                    }
                    values[i] = v;
                }
            }
        }
        // A parent row with a NULL in the referenced tuple can have no
        // children under either MATCH mode: the child side would have
        // had to match NULL, which never matches.
        if nulls > 0 {
            continue;
        }
        let mut buf = [0u8; MAX_PROBE_KEY_LEN];
        let len = encode_index_value(&mut buf, &types[..n], &values[..n])
            .ok_or(ConstraintError::OutOfBounds)?;
        plan.push(&buf[..len])?;
    }
    plan.required = plan.probe_count > 0;
    Ok(plan)
}

// ══════════════════════════════════════════════════════════════════════
// 11. Plan binding and compute-disposable state (§14.5, §14.11,
//     §21 invariants 15 and 18)
// ══════════════════════════════════════════════════════════════════════

/// Isolation levels the shared layer represents. Serializable is the
/// contract Lattice offers (§13.3); the weaker levels are listed so a
/// connector can *record* what a session asked for rather than
/// silently upgrading or downgrading it.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    ReadCommitted = 1,
    RepeatableRead = 2,
    Serializable = 3,
}

impl Isolation {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::ReadCommitted),
            2 => Some(Self::RepeatableRead),
            3 => Some(Self::Serializable),
            _ => None,
        }
    }
}

/// §14.5's plan record: "a plan records its catalog revision, required
/// Lattice capabilities, consistency/isolation level, partition spans,
/// memory budget, spill allowance, and maximum result size".
///
/// This is **not** a durable record. It is compute-local and disposable
/// (§14.11), which is exactly why it is safe for a replacement worker
/// to throw it away and rebind. It carries no encode/decode for that
/// reason: giving it a wire format would invite someone to persist it,
/// and a persisted plan is a durable outcome held by compute — §21
/// invariant 15's failure mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanBinding {
    /// The catalog revision every descriptor in the plan was read at.
    pub catalog_revision: u64,
    /// The partition-map generation the routing decisions were made
    /// against (§11.3).
    pub partition_map_generation: u32,
    pub isolation: Isolation,
    /// Number of partition spans the plan touches. Bounded up front so
    /// a plan's fan-out is knowable before execution (§14.13).
    pub partition_spans: u16,
    pub memory_budget_bytes: u64,
    pub spill_allowance_bytes: u64,
    pub max_result_rows: u64,
    pub deadline_unix_ms: u64,
}

/// Why a bound plan may not execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// The catalog moved under the plan. §14.5: rebind/replan, never
    /// execution under mixed metadata.
    StaleCatalog { bound: u64, current: u64 },
    /// Routing moved under the plan (§21 invariant 6).
    StaleRoutingGeneration { bound: u32, current: u32 },
    /// The plan declares no bound on some resource. A missing bound is
    /// not "unlimited", it is "unknown", and unknown blocks (§21
    /// invariant 14).
    UnboundedResource,
}

/// §14.5's pre-execution gate.
///
/// Clause order is the contract: a stale catalog is reported before a
/// stale routing generation, because rebinding the catalog may change
/// which ranges the plan touches and therefore make the routing
/// question moot. Both are refusals, never adjustments — a plan is
/// replanned, not patched, because patching is how mixed metadata gets
/// into an execution.
pub fn check_rebind(
    plan: &PlanBinding,
    current_catalog_revision: u64,
    current_map_generation: u32,
) -> Result<(), PlanError> {
    if plan.catalog_revision != current_catalog_revision {
        return Err(PlanError::StaleCatalog {
            bound: plan.catalog_revision,
            current: current_catalog_revision,
        });
    }
    if plan.partition_map_generation != current_map_generation {
        return Err(PlanError::StaleRoutingGeneration {
            bound: plan.partition_map_generation,
            current: current_map_generation,
        });
    }
    if plan.memory_budget_bytes == 0 || plan.max_result_rows == 0 {
        return Err(PlanError::UnboundedResource);
    }
    Ok(())
}

/// §14.11's state categories.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationalState {
    RowsIndexesCatalog = 1,
    TransactionOutcome = 2,
    TransactionIntents = 3,
    SchemaAndBackfillJobs = 4,
    DurableSequencesAndLocks = 5,
    QueryPlanAndDecodedCatalog = 6,
    QueryIntermediateSpill = 7,
    ClientTransportAttachment = 8,
    MovableSessionState = 9,
}

/// §14.11's authority column.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Authority {
    LatticeRanges = 1,
    TransactionHomeRange = 2,
    ParticipantRanges = 3,
    LatticeSystemRanges = 4,
    LatticeTransactionalOrLeaseRecord = 5,
    /// Compute cache or compute-local: disposable, holds no outcome.
    ComputeDisposable = 6,
    ProtocolAnchor = 7,
    SessionWorker = 8,
}

impl RelationalState {
    pub const ALL: [Self; 9] = [
        Self::RowsIndexesCatalog,
        Self::TransactionOutcome,
        Self::TransactionIntents,
        Self::SchemaAndBackfillJobs,
        Self::DurableSequencesAndLocks,
        Self::QueryPlanAndDecodedCatalog,
        Self::QueryIntermediateSpill,
        Self::ClientTransportAttachment,
        Self::MovableSessionState,
    ];
}

/// §14.11's table, as a function. Total over [`RelationalState`], so
/// adding a state category without assigning it an authority does not
/// compile.
pub const fn authority_of(state: RelationalState) -> Authority {
    match state {
        RelationalState::RowsIndexesCatalog => Authority::LatticeRanges,
        RelationalState::TransactionOutcome => Authority::TransactionHomeRange,
        RelationalState::TransactionIntents => Authority::ParticipantRanges,
        RelationalState::SchemaAndBackfillJobs => Authority::LatticeSystemRanges,
        RelationalState::DurableSequencesAndLocks => Authority::LatticeTransactionalOrLeaseRecord,
        RelationalState::QueryPlanAndDecodedCatalog | RelationalState::QueryIntermediateSpill => {
            Authority::ComputeDisposable
        }
        RelationalState::ClientTransportAttachment => Authority::ProtocolAnchor,
        RelationalState::MovableSessionState => Authority::SessionWorker,
    }
}

/// Does this authority hold a durable database outcome or a storage
/// file?
pub const fn holds_durable_authority(a: Authority) -> bool {
    matches!(
        a,
        Authority::LatticeRanges
            | Authority::TransactionHomeRange
            | Authority::ParticipantRanges
            | Authority::LatticeSystemRanges
            | Authority::LatticeTransactionalOrLeaseRecord
    )
}

/// §21 invariant 15, executable: **relational compute holds no unique
/// durable database outcome or storage file.**
///
/// Every state category whose authority is [`Authority::ComputeDisposable`]
/// must be reconstructible, and no durable authority may live in
/// compute. The test asserts this over all of [`RelationalState::ALL`],
/// which is why the mapping above is a total function rather than a
/// lookup table with a default.
pub const fn invariant_15_holds(state: RelationalState) -> bool {
    !holds_durable_authority(authority_of(state))
        || !matches!(authority_of(state), Authority::ComputeDisposable)
}

/// What a replacement worker may do with a state category after a
/// compute failure (§14.11's closing paragraph).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComputeRecovery {
    /// Reload from Lattice and continue.
    Reload,
    /// Discard; it was disposable.
    Discard,
    /// Look up the durable outcome by identity, then resume or fail
    /// cleanly. Never decided by the client's disconnect.
    ConsultDurableOutcome,
    /// Export only if the continuity contract requires it.
    ExportIfContractRequires,
}

/// §14.11: "After compute failure, a replacement worker reloads catalog
/// and routing state, checks durable transaction status by identity,
/// resumes only explicitly resumable operations, and fails all others
/// cleanly. Client disconnect never decides a transaction outcome."
pub const fn compute_recovery(state: RelationalState) -> ComputeRecovery {
    match state {
        RelationalState::RowsIndexesCatalog
        | RelationalState::SchemaAndBackfillJobs
        | RelationalState::DurableSequencesAndLocks => ComputeRecovery::Reload,
        RelationalState::TransactionOutcome | RelationalState::TransactionIntents => {
            ComputeRecovery::ConsultDurableOutcome
        }
        RelationalState::QueryPlanAndDecodedCatalog
        | RelationalState::QueryIntermediateSpill
        | RelationalState::ClientTransportAttachment => ComputeRecovery::Discard,
        RelationalState::MovableSessionState => ComputeRecovery::ExportIfContractRequires,
    }
}
