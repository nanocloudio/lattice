//! SQL execution wire and key composition (RFC database foundation
//! §14.4, §14.5).
//!
//! Dual-target facade shared by `relational_executor` and both
//! connectors. Three concerns, all of them boundary definitions rather
//! than logic:
//!
//! 1. **KV user-key composition** — how a relational key reaches a
//!    keyspace-unaware KV path.
//! 2. **The SQL request/response envelope** — what a connector sends
//!    the executor and what comes back.
//! 3. **Typed result values** — why rows come back TYPED rather than as
//!    text.
//!
//! ## Why keys carry their keyspace inline
//!
//! `internal_key` owns a `(tenant, database, keyspace)` identity triple,
//! but the KV request envelope has no field for it: the KV path was
//! built for Redis/etcd/Memcached, which have one flat keyspace each,
//! and the worker encodes every user key at `(0, 0, 0)`.
//!
//! Rather than widen that envelope — which would touch every anchor,
//! the router, the worker and the batch format for the benefit of one
//! caller — a relational key carries its keyspace as a four-byte
//! big-endian PREFIX of the user key. The properties that matters for
//! are preserved exactly: relational keys cannot collide with a Redis
//! key (no Redis client can send a key beginning with those bytes by
//! accident, and `0x8000_00xx` sorts above ordinary ASCII keys), keys of
//! one keyspace sort together, and a keyspace prefix is a valid scan
//! bound. What is NOT preserved is tenancy — this composition pins
//! tenant and database to 0 — and that is recorded here rather than
//! implied: multi-tenant relational storage needs the envelope widened,
//! and until then a second tenant would share one namespace.
//!
//! ## Why result rows are typed, not text
//!
//! §14.3 gives each connector its own "result encoding", and the two
//! genuinely differ: PostgreSQL renders a boolean as `t`/`f`, MySQL as
//! `1`/`0`. If the executor produced text, one of those renderings would
//! have to win and the other connector would be re-parsing text to fix
//! it. So the executor returns values in the shared plain encoding with
//! their logical type, and each connector formats. Formatting is the
//! connector's job; deciding WHAT the value is, is not.

// `sql_core` is mounted HERE rather than alongside this file, and
// re-exports `relational` through it, so a consumer that needs both gets
// ONE copy of each. A `#[path]` mount is a fresh module tree every time:
// mounting `relational.rs` from two siblings produces two distinct
// `ColumnDescriptor` types that will not unify, and the error surfaces
// far from the cause. One mount point, one type.
#[path = "sql_core.rs"]
pub mod sql_core;

pub use sql_core::relational;

use relational::{ColumnDescriptor, LogicalType, TYPE_WIRE_LEN};

// ── KV user-key composition ───────────────────────────────────────────

/// Width of the keyspace prefix on a relational KV user key.
pub const KEYSPACE_PREFIX_LEN: usize = 4;

/// Compose a relational KV user key: `[keyspace:u32 BE][body…]`.
///
/// Big-endian so the prefix is order-preserving, and fixed-width so it
/// is prefix-unambiguous — both are what let `[keyspace]` and
/// `[keyspace][table_id]` be used directly as range-scan bounds.
pub fn kv_key(out: &mut [u8], keyspace: u32, body: &[u8]) -> Option<usize> {
    let need = KEYSPACE_PREFIX_LEN + body.len();
    if out.len() < need {
        return None;
    }
    out[0..KEYSPACE_PREFIX_LEN].copy_from_slice(&keyspace.to_be_bytes());
    out[KEYSPACE_PREFIX_LEN..need].copy_from_slice(body);
    Some(need)
}

/// The exclusive upper bound for a scan of every key under `prefix`.
///
/// Increments the last byte that is not `0xFF`, dropping the trailing
/// `0xFF` run. That is the immediate successor of the prefix in
/// lexicographic order, so `[prefix, successor)` is exactly the set of
/// keys the prefix covers.
///
/// Returns `None` when the prefix is all `0xFF` — which has no
/// successor, and therefore means "unbounded above". A caller must pass
/// an empty `end` in that case rather than substituting a bound, because
/// any concrete bound would be wrong in one direction or the other.
pub fn prefix_successor(prefix: &[u8], out: &mut [u8]) -> Option<usize> {
    if prefix.len() > out.len() {
        return None;
    }
    let mut n = prefix.len();
    while n > 0 {
        if prefix[n - 1] != 0xFF {
            out[..n].copy_from_slice(&prefix[..n]);
            out[n - 1] += 1;
            return Some(n);
        }
        n -= 1;
    }
    None
}

// ── Request / response envelope ───────────────────────────────────────

/// Fixed head of a `MSG_SQL_REQUEST` payload:
/// `[corr_id:u64][dialect:u8][tenant_id:u32][conn_id:u8][sql_len:u16]`.
pub const SQL_REQUEST_HEAD: usize = 8 + 1 + 4 + 1 + 2;

/// Fixed head of a `MSG_SQL_RESPONSE` payload:
/// `[corr_id:u64][conn_id:u8][outcome:u8][tag:u8][affected:u64]
///  [col_count:u16]`.
pub const SQL_RESPONSE_HEAD: usize = 8 + 1 + 1 + 1 + 8 + 2;

/// `dialect` byte values, matching `sql_core::Dialect`.
pub const DIALECT_POSTGRES: u8 = 0;
pub const DIALECT_MYSQL: u8 = 1;

/// What kind of statement completed. The connector maps this to its own
/// protocol's completion notice — `CommandComplete("INSERT 0 3")` for
/// PostgreSQL, an OK packet with an affected-rows count for MySQL. The
/// executor does not build either string, because those strings are
/// exactly the "result encoding" §14.3 gives the connector.
pub const TAG_EMPTY: u8 = 0;
pub const TAG_CREATE_TABLE: u8 = 1;
pub const TAG_DROP_TABLE: u8 = 2;
pub const TAG_INSERT: u8 = 3;
pub const TAG_SELECT: u8 = 4;
pub const TAG_BEGIN: u8 = 5;
pub const TAG_COMMIT: u8 = 6;
pub const TAG_ROLLBACK: u8 = 7;
pub const TAG_SET: u8 = 8;
pub const TAG_SHOW: u8 = 9;
pub const TAG_CREATE_INDEX: u8 = 10;
pub const TAG_DROP_INDEX: u8 = 11;
pub const TAG_DELETE: u8 = 12;
pub const TAG_UPDATE: u8 = 13;
pub const TAG_ALTER: u8 = 14;

/// `outcome` byte: 0 is success, anything else is an error code the
/// connector maps to a SQLSTATE / MySQL error number.
///
/// Codes 1..=31 mirror `sql_core::SqlError` — conditions the front end
/// detects without touching the store. Codes from 32 up are EXECUTION
/// conditions, which the front end cannot know: whether a table exists
/// is a fact about the catalog, not about the statement.
///
/// The split matters to a connector, because the two halves map to
/// different SQLSTATE classes: a front-end code is a syntax or type
/// error (class 42), an execution code is an integrity or resource
/// condition (classes 23, 53, 57). Collapsing them into one opaque
/// "error" would force every client to read the message text to find out
/// what happened.
pub const OUTCOME_OK: u8 = 0;

/// Front-end codes, one per `sql_core::SqlError` variant.
pub const ERR_TOO_LONG: u8 = 1;
pub const ERR_UNEXPECTED_END: u8 = 2;
pub const ERR_SYNTAX: u8 = 3;
pub const ERR_NAME_TOO_LONG: u8 = 4;
pub const ERR_LITERAL_TOO_LONG: u8 = 5;
pub const ERR_TOO_MANY_ITEMS: u8 = 6;
pub const ERR_UNTERMINATED_LITERAL: u8 = 7;
pub const ERR_UNKNOWN_TYPE: u8 = 8;
pub const ERR_UNKNOWN_COLUMN: u8 = 9;
pub const ERR_TYPE_MISMATCH: u8 = 10;
pub const ERR_ARITY_MISMATCH: u8 = 11;
pub const ERR_NOT_NULL: u8 = 12;
pub const ERR_MISSING_PRIMARY_KEY: u8 = 13;
pub const ERR_DUPLICATE_COLUMN: u8 = 14;
pub const ERR_UNSUPPORTED: u8 = 15;

/// Execution codes.
pub const ERR_NO_SUCH_TABLE: u8 = 32;
pub const ERR_TABLE_EXISTS: u8 = 33;
/// The KV path refused or faulted. Distinct from every other code
/// because it is the one that is not the client's fault, and a client
/// should retry it rather than rewrite its statement.
pub const ERR_STORE: u8 = 34;
/// The result set did not fit the response buffer. Reported rather than
/// truncated: a short result set is indistinguishable from a complete
/// one to the client.
pub const ERR_RESULT_TOO_LARGE: u8 = 35;
/// Too many statements in flight. A load condition, not a defect —
/// named so a client can back off rather than assume its SQL is wrong.
pub const ERR_BUSY: u8 = 36;
/// A row already exists at the primary key an INSERT computed.
pub const ERR_DUPLICATE_KEY: u8 = 37;
/// A stored descriptor or row failed to decode. A corruption signal, and
/// never confused with "absent": absent is a legitimate state, a row
/// that exists but cannot be read is not.
pub const ERR_CORRUPT: u8 = 38;
/// A statement's KV round-trip never completed within the executor's
/// watchdog deadline. The single serializing executor aborts the stuck
/// statement and reclaims its slot rather than wedging every session
/// behind one lost or never-arriving KV reply — the liveness backstop.
/// A client should retry, exactly as for `ERR_BUSY`.
pub const ERR_TIMEOUT: u8 = 39;

/// A decoded `MSG_SQL_REQUEST`.
pub struct SqlRequest<'a> {
    pub corr_id: u64,
    pub dialect: u8,
    pub tenant_id: u32,
    pub conn_id: u8,
    pub sql: &'a [u8],
}

/// Encode a `MSG_SQL_REQUEST` payload.
pub fn encode_sql_request(
    out: &mut [u8],
    corr_id: u64,
    dialect: u8,
    tenant_id: u32,
    conn_id: u8,
    sql: &[u8],
) -> Option<usize> {
    let need = SQL_REQUEST_HEAD + sql.len();
    if out.len() < need || sql.len() > u16::MAX as usize {
        return None;
    }
    out[0..8].copy_from_slice(&corr_id.to_le_bytes());
    out[8] = dialect;
    out[9..13].copy_from_slice(&tenant_id.to_le_bytes());
    out[13] = conn_id;
    out[14..16].copy_from_slice(&(sql.len() as u16).to_le_bytes());
    out[SQL_REQUEST_HEAD..need].copy_from_slice(sql);
    Some(need)
}

/// Decode a `MSG_SQL_REQUEST` payload. Fails closed on a truncated
/// body — a partially-read statement would execute something the client
/// did not send.
pub fn decode_sql_request(payload: &[u8]) -> Option<SqlRequest<'_>> {
    if payload.len() < SQL_REQUEST_HEAD {
        return None;
    }
    let sql_len = u16::from_le_bytes([payload[14], payload[15]]) as usize;
    if payload.len() < SQL_REQUEST_HEAD + sql_len {
        return None;
    }
    Some(SqlRequest {
        corr_id: u64::from_le_bytes(payload[0..8].try_into().ok()?),
        dialect: payload[8],
        tenant_id: u32::from_le_bytes(payload[9..13].try_into().ok()?),
        conn_id: payload[13],
        sql: &payload[SQL_REQUEST_HEAD..SQL_REQUEST_HEAD + sql_len],
    })
}

/// Builds a `MSG_SQL_RESPONSE` payload incrementally.
///
/// Incremental because a result set arrives from the store one scan page
/// at a time, and the executor cannot know the row count until the last
/// page. The row count is therefore backfilled at [`finish`](Self::finish)
/// rather than declared up front — the alternative would be buffering
/// every row twice or emitting a count that could still change.
pub struct ResponseBuilder<'b> {
    buf: &'b mut [u8],
    len: usize,
    row_count: u16,
    row_count_at: usize,
    /// Set once a write did not fit. Sticky, so a caller that ignores one
    /// failed append cannot end up with a response that silently omits a
    /// row: `finish` refuses outright.
    overflow: bool,
}

impl<'b> ResponseBuilder<'b> {
    /// Start a response. `columns` is empty for a non-row statement.
    pub fn new(
        buf: &'b mut [u8],
        corr_id: u64,
        conn_id: u8,
        outcome: u8,
        tag: u8,
        affected: u64,
        columns: &[&ColumnDescriptor],
    ) -> Option<Self> {
        if buf.len() < SQL_RESPONSE_HEAD || columns.len() > u16::MAX as usize {
            return None;
        }
        buf[0..8].copy_from_slice(&corr_id.to_le_bytes());
        buf[8] = conn_id;
        buf[9] = outcome;
        buf[10] = tag;
        buf[11..19].copy_from_slice(&affected.to_le_bytes());
        buf[19..21].copy_from_slice(&(columns.len() as u16).to_le_bytes());
        let mut len = SQL_RESPONSE_HEAD;
        for c in columns {
            // [type:TYPE_WIRE_LEN][name_len:u8][name…]
            let name = c.name.as_bytes();
            if len + TYPE_WIRE_LEN + 1 + name.len() > buf.len() || name.len() > u8::MAX as usize {
                return None;
            }
            len += c.ty.encode(buf.get_mut(len..)?)?;
            buf[len] = name.len() as u8;
            len += 1;
            buf[len..len + name.len()].copy_from_slice(name);
            len += name.len();
        }
        if len + 2 > buf.len() {
            return None;
        }
        let row_count_at = len;
        buf[len..len + 2].copy_from_slice(&0u16.to_le_bytes());
        len += 2;
        Some(Self {
            buf,
            len,
            row_count: 0,
            row_count_at,
            overflow: false,
        })
    }

    /// Append one already-plain-encoded value cell. `None` marks SQL
    /// NULL, which is distinct from a zero-length value — an empty
    /// string and a NULL are different answers.
    pub fn push_cell(&mut self, cell: Option<&[u8]>) -> bool {
        let payload = cell.unwrap_or(&[]);
        if self.len + 4 + payload.len() > self.buf.len() || payload.len() > u32::MAX as usize {
            self.overflow = true;
            return false;
        }
        let marker: u32 = match cell {
            None => NULL_CELL,
            Some(_) => payload.len() as u32,
        };
        self.buf[self.len..self.len + 4].copy_from_slice(&marker.to_le_bytes());
        self.len += 4;
        if cell.is_some() {
            self.buf[self.len..self.len + payload.len()].copy_from_slice(payload);
            self.len += payload.len();
        }
        true
    }

    /// Mark the end of a row. Call after the row's cells.
    pub fn end_row(&mut self) -> bool {
        if self.row_count == u16::MAX {
            self.overflow = true;
            return false;
        }
        self.row_count += 1;
        true
    }

    /// True once any append has failed. A caller checks this to stop
    /// scanning and report a bounded-result error rather than shipping a
    /// truncated result set.
    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    pub fn rows(&self) -> u16 {
        self.row_count
    }

    /// Append an already-encoded row section and finish.
    ///
    /// This exists because a result set is assembled across MANY
    /// cooperative steps: each scan page arrives in its own
    /// `module_step`, and a `ResponseBuilder` borrows its output buffer
    /// mutably so it cannot be held in module state between them. The
    /// executor therefore stages cells in a plain byte buffer as pages
    /// arrive and hands the whole section over once, here.
    ///
    /// `rows` must be exactly `count` rows of `col_count` cells in the
    /// `push_cell` encoding. It is the caller's invariant because only
    /// the caller knows the column count it staged against; a mismatch
    /// would decode as a different result set rather than fail, so the
    /// executor stages and counts in one place.
    pub fn finish_with_rows(mut self, count: u16, rows: &[u8]) -> Option<usize> {
        if self.overflow || self.row_count != 0 {
            // A mix of staged and pushed rows would double-count.
            return None;
        }
        if self.len + rows.len() > self.buf.len() {
            return None;
        }
        self.buf[self.len..self.len + rows.len()].copy_from_slice(rows);
        self.len += rows.len();
        self.row_count = count;
        self.finish()
    }

    /// Backfill the row count and return the payload length. Refuses if
    /// anything overflowed: a truncated result set is indistinguishable
    /// from a complete one to the client, so it must not be sent.
    pub fn finish(self) -> Option<usize> {
        if self.overflow {
            return None;
        }
        self.buf[self.row_count_at..self.row_count_at + 2]
            .copy_from_slice(&self.row_count.to_le_bytes());
        Some(self.len)
    }
}

/// Cell length marker meaning SQL NULL. Distinct from `0`, which is a
/// present value of zero length.
pub const NULL_CELL: u32 = 0xFFFF_FFFF;

/// A decoded response header.
pub struct ResponseHeader {
    pub corr_id: u64,
    pub conn_id: u8,
    pub outcome: u8,
    pub tag: u8,
    pub affected: u64,
    pub col_count: u16,
}

pub fn decode_response_header(payload: &[u8]) -> Option<ResponseHeader> {
    if payload.len() < SQL_RESPONSE_HEAD {
        return None;
    }
    Some(ResponseHeader {
        corr_id: u64::from_le_bytes(payload[0..8].try_into().ok()?),
        conn_id: payload[8],
        outcome: payload[9],
        tag: payload[10],
        affected: u64::from_le_bytes(payload[11..19].try_into().ok()?),
        col_count: u16::from_le_bytes([payload[19], payload[20]]),
    })
}

/// One column of a decoded response.
#[derive(Clone, Copy)]
pub struct ResponseColumn<'a> {
    pub ty: LogicalType,
    pub name: &'a [u8],
}

/// Walk a response's column descriptors, calling `f` per column, and
/// return the offset where the row section begins.
pub fn walk_response_columns<'a>(
    payload: &'a [u8],
    col_count: u16,
    mut f: impl FnMut(ResponseColumn<'a>),
) -> Option<usize> {
    let mut at = SQL_RESPONSE_HEAD;
    for _ in 0..col_count {
        let ty = LogicalType::decode(payload.get(at..at + TYPE_WIRE_LEN)?)?;
        at += TYPE_WIRE_LEN;
        let nlen = *payload.get(at)? as usize;
        at += 1;
        let name = payload.get(at..at + nlen)?;
        at += nlen;
        f(ResponseColumn { ty, name });
    }
    Some(at)
}

/// Walk a response's rows starting at `at` (the offset
/// [`walk_response_columns`] returned), calling `f` once per cell with
/// `(row_index, col_index, cell)` where `cell` is `None` for NULL.
pub fn walk_response_rows<'a>(
    payload: &'a [u8],
    at: usize,
    col_count: u16,
    mut f: impl FnMut(u16, u16, Option<&'a [u8]>),
) -> Option<u16> {
    let row_count = u16::from_le_bytes([*payload.get(at)?, *payload.get(at + 1)?]);
    let mut p = at + 2;
    for r in 0..row_count {
        for c in 0..col_count {
            let marker = u32::from_le_bytes(payload.get(p..p + 4)?.try_into().ok()?);
            p += 4;
            if marker == NULL_CELL {
                f(r, c, None);
                continue;
            }
            let n = marker as usize;
            let cell = payload.get(p..p + n)?;
            p += n;
            f(r, c, Some(cell));
        }
    }
    Some(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use relational::{TableDescriptor, Value};

    fn descr() -> TableDescriptor {
        let mut td = TableDescriptor::new(7, 0, 1, b"t").unwrap();
        td.add_column(ColumnDescriptor::new(1, LogicalType::Int, false, false, b"id").unwrap())
            .unwrap();
        td.add_column(
            ColumnDescriptor::new(2, LogicalType::VarChar { length: 32 }, true, false, b"name")
                .unwrap(),
        )
        .unwrap();
        td.set_primary_key(&[1]).unwrap();
        td
    }

    // ── Key composition ──────────────────────────────────────────────

    #[test]
    fn keyspace_prefix_is_order_preserving() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        let na = kv_key(&mut a, 1, b"zzz").unwrap();
        let nb = kv_key(&mut b, 2, b"aaa").unwrap();
        assert!(
            a[..na] < b[..nb],
            "a lower keyspace must sort below a higher one regardless of body"
        );
    }

    /// Relational keys must not collide with an ordinary client key.
    /// `0x8000_00xx` begins with a byte no ASCII key can start with.
    #[test]
    fn relational_keys_sort_above_ascii_keys() {
        let mut r = [0u8; 32];
        let n = kv_key(&mut r, relational::KS_RELATIONAL_TABLE, b"x").unwrap();
        assert!(r[0] >= 0x80, "prefix must be outside the ASCII range");
        assert!(r[..n] > *b"~~~~~~~~".as_slice());
    }

    #[test]
    fn prefix_successor_is_the_immediate_next_key() {
        let mut out = [0u8; 8];
        let n = prefix_successor(b"ab", &mut out).unwrap();
        assert_eq!(&out[..n], b"ac");
        // A trailing 0xFF run is dropped and the previous byte carries.
        let n = prefix_successor(&[b'a', 0xFF, 0xFF], &mut out).unwrap();
        assert_eq!(&out[..n], b"b");
    }

    /// An all-`0xFF` prefix has NO successor. Returning some bound
    /// anyway would either exclude keys the prefix covers or include
    /// keys it does not; the caller must use an unbounded scan instead.
    #[test]
    fn an_all_ff_prefix_has_no_successor() {
        let mut out = [0u8; 8];
        assert!(prefix_successor(&[0xFF, 0xFF], &mut out).is_none());
    }

    /// The composed prefix bounds exactly one table: a scan of
    /// `[ks][table_id]` must not reach `table_id + 1`.
    #[test]
    fn table_prefix_bounds_exactly_one_table() {
        let mut body = [0u8; 8];
        let bn = relational::encode_table_prefix(&mut body, 7).unwrap();
        let mut start = [0u8; 32];
        let sn = kv_key(&mut start, relational::KS_RELATIONAL_TABLE, &body[..bn]).unwrap();
        let mut end = [0u8; 32];
        let en = prefix_successor(&start[..sn], &mut end).unwrap();

        // A row of table 7 falls inside; a row of table 8 does not.
        for (table, inside) in [(7u32, true), (8, false), (6, false)] {
            let mut rowbody = [0u8; 64];
            let rn = relational::encode_primary_key(
                &mut rowbody,
                table,
                &[LogicalType::Int],
                &[Value::Int(1)],
            )
            .unwrap();
            let mut rowkey = [0u8; 96];
            let kn = kv_key(&mut rowkey, relational::KS_RELATIONAL_TABLE, &rowbody[..rn]).unwrap();
            let within = rowkey[..kn] >= start[..sn] && rowkey[..kn] < end[..en];
            assert_eq!(within, inside, "table {table}");
        }
    }

    // ── Request envelope ─────────────────────────────────────────────

    #[test]
    fn request_round_trips() {
        let mut buf = [0u8; 128];
        let n = encode_sql_request(&mut buf, 42, DIALECT_MYSQL, 3, 9, b"SELECT 1").unwrap();
        let r = decode_sql_request(&buf[..n]).unwrap();
        assert_eq!(r.corr_id, 42);
        assert_eq!(r.dialect, DIALECT_MYSQL);
        assert_eq!(r.tenant_id, 3);
        assert_eq!(r.conn_id, 9);
        assert_eq!(r.sql, b"SELECT 1");
    }

    /// A truncated request refuses. A partially-read statement would
    /// execute something the client did not send.
    #[test]
    fn truncated_request_refuses() {
        let mut buf = [0u8; 128];
        let n = encode_sql_request(&mut buf, 1, 0, 0, 0, b"SELECT 1").unwrap();
        for cut in 0..n {
            assert!(
                decode_sql_request(&buf[..cut]).is_none(),
                "truncation to {cut} accepted"
            );
        }
    }

    // ── Response envelope ────────────────────────────────────────────

    #[test]
    fn result_set_round_trips_with_types_and_nulls() {
        let td = descr();
        let cols: [&ColumnDescriptor; 2] = [&td.columns()[0], &td.columns()[1]];
        let mut buf = [0u8; 1024];
        let mut b = ResponseBuilder::new(&mut buf, 5, 2, OUTCOME_OK, TAG_SELECT, 0, &cols).unwrap();

        // Row 1: (1, 'ada'). Row 2: (2, NULL).
        let mut v = [0u8; 64];
        let vn = relational::encode_value_plain(&mut v, LogicalType::Int, Value::Int(1)).unwrap();
        assert!(b.push_cell(Some(&v[..vn])));
        let mut s = [0u8; 64];
        let sn = relational::encode_value_plain(
            &mut s,
            LogicalType::VarChar { length: 32 },
            Value::Text(b"ada"),
        )
        .unwrap();
        assert!(b.push_cell(Some(&s[..sn])));
        assert!(b.end_row());

        let vn2 = relational::encode_value_plain(&mut v, LogicalType::Int, Value::Int(2)).unwrap();
        assert!(b.push_cell(Some(&v[..vn2])));
        assert!(b.push_cell(None));
        assert!(b.end_row());
        let n = b.finish().unwrap();

        let h = decode_response_header(&buf[..n]).unwrap();
        assert_eq!(h.corr_id, 5);
        assert_eq!(h.conn_id, 2);
        assert_eq!(h.outcome, OUTCOME_OK);
        assert_eq!(h.tag, TAG_SELECT);
        assert_eq!(h.col_count, 2);

        let mut names: [&[u8]; 2] = [b"", b""];
        let mut types = [LogicalType::Null; 2];
        let mut i = 0;
        let rows_at = walk_response_columns(&buf[..n], h.col_count, |c| {
            names[i] = c.name;
            types[i] = c.ty;
            i += 1;
        })
        .unwrap();
        assert_eq!(names, [b"id".as_slice(), b"name".as_slice()]);
        assert_eq!(types[0], LogicalType::Int);
        assert_eq!(types[1], LogicalType::VarChar { length: 32 });

        // Cells decode back to the values that went in, and the NULL
        // stays distinguishable from an empty string.
        let mut seen: [[Option<&[u8]>; 2]; 2] = [[None; 2]; 2];
        let rc = walk_response_rows(&buf[..n], rows_at, h.col_count, |r, c, cell| {
            seen[r as usize][c as usize] = cell;
        })
        .unwrap();
        assert_eq!(rc, 2);
        assert_eq!(
            relational::decode_value_plain(seen[0][0].unwrap(), LogicalType::Int)
                .unwrap()
                .0,
            Value::Int(1)
        );
        assert_eq!(
            relational::decode_value_plain(
                seen[0][1].unwrap(),
                LogicalType::VarChar { length: 32 }
            )
            .unwrap()
            .0,
            Value::Text(b"ada")
        );
        assert_eq!(
            relational::decode_value_plain(seen[1][0].unwrap(), LogicalType::Int)
                .unwrap()
                .0,
            Value::Int(2)
        );
        assert!(seen[1][1].is_none(), "NULL must not become an empty value");
    }

    /// An empty value is NOT a NULL. Collapsing them would make
    /// `''` and `NULL` indistinguishable to the client.
    #[test]
    fn empty_and_null_cells_are_distinct() {
        let td = descr();
        let cols: [&ColumnDescriptor; 1] = [&td.columns()[1]];
        let mut buf = [0u8; 256];
        let mut b = ResponseBuilder::new(&mut buf, 1, 0, OUTCOME_OK, TAG_SELECT, 0, &cols).unwrap();
        let mut s = [0u8; 8];
        let sn = relational::encode_value_plain(
            &mut s,
            LogicalType::VarChar { length: 32 },
            Value::Text(b""),
        )
        .unwrap();
        assert!(b.push_cell(Some(&s[..sn])));
        assert!(b.end_row());
        assert!(b.push_cell(None));
        assert!(b.end_row());
        let n = b.finish().unwrap();
        let h = decode_response_header(&buf[..n]).unwrap();
        let at = walk_response_columns(&buf[..n], h.col_count, |_| {}).unwrap();
        let mut nulls = [false; 2];
        walk_response_rows(&buf[..n], at, h.col_count, |r, _, cell| {
            nulls[r as usize] = cell.is_none();
        })
        .unwrap();
        assert_eq!(nulls, [false, true]);
    }

    /// A response that overflowed its buffer REFUSES to finish. Shipping
    /// the rows that fit would give the client a truncated result set it
    /// could not distinguish from a complete one.
    #[test]
    fn an_overflowed_response_refuses_to_finish() {
        let td = descr();
        let cols: [&ColumnDescriptor; 1] = [&td.columns()[0]];
        // Room for the header and column, but not for many rows.
        let mut buf = [0u8; SQL_RESPONSE_HEAD + TYPE_WIRE_LEN + 1 + 2 + 2 + 8];
        let mut b = ResponseBuilder::new(&mut buf, 1, 0, OUTCOME_OK, TAG_SELECT, 0, &cols).unwrap();
        let mut pushed = 0;
        let cell = [0u8; 5];
        while b.push_cell(Some(&cell)) {
            b.end_row();
            pushed += 1;
            assert!(pushed < 100, "buffer never filled");
        }
        assert!(b.overflowed());
        assert!(
            b.finish().is_none(),
            "a truncated result set must not be sent"
        );
    }

    /// A staged row section decodes identically to pushed rows. The
    /// staging path exists because a scan spans many cooperative steps
    /// and the builder cannot be held across them, so the two paths must
    /// agree byte for byte.
    #[test]
    fn staged_rows_decode_like_pushed_rows() {
        let td = descr();
        let cols: [&ColumnDescriptor; 1] = [&td.columns()[0]];

        // Build the reference by pushing.
        let mut a = [0u8; 512];
        let mut ba = ResponseBuilder::new(&mut a, 1, 0, OUTCOME_OK, TAG_SELECT, 0, &cols).unwrap();
        let mut v = [0u8; 32];
        for i in 1..=3i32 {
            let vn =
                relational::encode_value_plain(&mut v, LogicalType::Int, Value::Int(i)).unwrap();
            assert!(ba.push_cell(Some(&v[..vn])));
            assert!(ba.end_row());
        }
        let na = ba.finish().unwrap();

        // Build the same thing by staging the cell bytes first.
        let mut staged = [0u8; 256];
        let mut sl = 0usize;
        for i in 1..=3i32 {
            let vn =
                relational::encode_value_plain(&mut v, LogicalType::Int, Value::Int(i)).unwrap();
            staged[sl..sl + 4].copy_from_slice(&(vn as u32).to_le_bytes());
            sl += 4;
            staged[sl..sl + vn].copy_from_slice(&v[..vn]);
            sl += vn;
        }
        let mut b = [0u8; 512];
        let bb = ResponseBuilder::new(&mut b, 1, 0, OUTCOME_OK, TAG_SELECT, 0, &cols).unwrap();
        let nb = bb.finish_with_rows(3, &staged[..sl]).unwrap();

        assert_eq!(
            a[..na],
            b[..nb],
            "staged and pushed rows must agree exactly"
        );
    }

    /// A non-row statement carries its affected count and no columns.
    #[test]
    fn command_completion_carries_affected_rows() {
        let mut buf = [0u8; 64];
        let b = ResponseBuilder::new(&mut buf, 9, 1, OUTCOME_OK, TAG_INSERT, 3, &[]).unwrap();
        let n = b.finish().unwrap();
        let h = decode_response_header(&buf[..n]).unwrap();
        assert_eq!(h.tag, TAG_INSERT);
        assert_eq!(h.affected, 3);
        assert_eq!(h.col_count, 0);
    }

    /// An error response is a header with a non-zero outcome and no
    /// rows — so a connector can always read the outcome before it has
    /// to understand anything else.
    #[test]
    fn error_responses_are_readable_from_the_header_alone() {
        let mut buf = [0u8; 64];
        let b = ResponseBuilder::new(&mut buf, 9, 1, 42, TAG_EMPTY, 0, &[]).unwrap();
        let n = b.finish().unwrap();
        let h = decode_response_header(&buf[..n]).unwrap();
        assert_ne!(h.outcome, OUTCOME_OK);
        assert_eq!(h.col_count, 0);
    }

    // ── Catalog name index ───────────────────────────────────────────

    /// SQL identifiers are case-insensitive unless quoted, so `T` and
    /// `t` must resolve to ONE object. Folding at the key is what makes
    /// that a property of the lookup rather than of a comparison someone
    /// has to remember.
    #[test]
    fn catalog_name_keys_fold_case() {
        let mut a = [0u8; 128];
        let mut b = [0u8; 128];
        let na =
            relational::encode_catalog_name_key(&mut a, 0, relational::ObjectKind::Table, b"Users")
                .unwrap();
        let nb =
            relational::encode_catalog_name_key(&mut b, 0, relational::ObjectKind::Table, b"users")
                .unwrap();
        assert_eq!(a[..na], b[..nb]);
    }

    #[test]
    fn catalog_name_keys_separate_kinds_and_databases() {
        let mut a = [0u8; 128];
        let mut b = [0u8; 128];
        let na =
            relational::encode_catalog_name_key(&mut a, 0, relational::ObjectKind::Table, b"x")
                .unwrap();
        let nb =
            relational::encode_catalog_name_key(&mut b, 0, relational::ObjectKind::Index, b"x")
                .unwrap();
        assert_ne!(a[..na], b[..nb], "a table and an index may share a name");
        let nb2 =
            relational::encode_catalog_name_key(&mut b, 1, relational::ObjectKind::Table, b"x")
                .unwrap();
        assert_ne!(a[..na], b[..nb2], "two databases may share a table name");
    }

    #[test]
    fn an_empty_or_oversized_name_is_refused() {
        let mut out = [0u8; 256];
        assert!(relational::encode_catalog_name_key(
            &mut out,
            0,
            relational::ObjectKind::Table,
            b""
        )
        .is_none());
        let long = [b'x'; relational::MAX_NAME_LEN + 1];
        assert!(relational::encode_catalog_name_key(
            &mut out,
            0,
            relational::ObjectKind::Table,
            &long
        )
        .is_none());
    }
}
