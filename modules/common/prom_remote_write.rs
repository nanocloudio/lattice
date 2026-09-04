//! Prometheus `remote_write` decoder — reads the protobuf `WriteRequest`
//! (after Snappy decompression) into labelled series and samples the anchor
//! writes to the time-series store. Pure logic, `no_std`, no allocation: a
//! hand-rolled protobuf reader (no `prost`, matching `etcd_codec`) that
//! streams one series at a time into caller scratch.
//!
//! Schema (the subset lattice reads):
//! ```proto
//! message WriteRequest { repeated TimeSeries timeseries = 1; }
//! message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//! message Label        { string name = 1; string value = 2; }
//! message Sample       { double value = 1; int64 timestamp = 2; }
//! ```
//! Labels include the reserved `__name__` carrying the metric name.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

#[path = "tsquery_core.rs"]
pub mod core;

use core::{Label, Unsupported};

/// One decoded sample: a millisecond timestamp and a value. (Prometheus
/// sends timestamps in milliseconds already.)
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Sample {
    pub ts_ms: i64,
    pub value: f64,
}

/// A streaming reader over a `WriteRequest` body. Call [`next_series`]
/// repeatedly; each call fills the caller's label and sample scratch for one
/// series and returns their counts, until the body is exhausted.
pub struct Reader<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(body: &'a [u8]) -> Self {
        Reader { body, pos: 0 }
    }

    /// Decode the next `TimeSeries`, writing its labels into `labels` and
    /// samples into `samples`. Returns `Ok(Some((n_labels, n_samples)))`, or
    /// `Ok(None)` at the end, or `Err` on a malformed body / scratch
    /// overflow. Labels are returned in wire order (not sorted); a caller
    /// deriving a series identity must sort them into canonical order
    /// first (`tsquery_core::identify` demands it).
    pub fn next_series(
        &mut self,
        labels: &mut [Label<'a>],
        samples: &mut [Sample],
    ) -> Result<Option<(usize, usize)>, Unsupported> {
        // Find the next top-level field 1 (timeseries, LEN).
        while self.pos < self.body.len() {
            let (field, wire, next) = read_tag(self.body, self.pos)?;
            self.pos = next;
            if wire != 2 {
                // Skip any non-LEN top-level field defensively.
                self.pos = skip_field(self.body, self.pos, wire)?;
                continue;
            }
            let (len, after_len) = read_varint(self.body, self.pos)?;
            let start = after_len;
            let end = start.checked_add(len).ok_or(Unsupported::Malformed)?;
            if end > self.body.len() {
                return Err(Unsupported::Malformed);
            }
            self.pos = end;
            if field != 1 {
                continue; // not a TimeSeries; skip its bytes
            }
            let (nl, ns) = decode_timeseries(&self.body[start..end], labels, samples)?;
            return Ok(Some((nl, ns)));
        }
        Ok(None)
    }
}

fn decode_timeseries<'a>(
    ts: &'a [u8],
    labels: &mut [Label<'a>],
    samples: &mut [Sample],
) -> Result<(usize, usize), Unsupported> {
    let mut nl = 0usize;
    let mut ns = 0usize;
    let mut pos = 0usize;
    while pos < ts.len() {
        let (field, wire, next) = read_tag(ts, pos)?;
        pos = next;
        if wire != 2 {
            pos = skip_field(ts, pos, wire)?;
            continue;
        }
        let (len, after_len) = read_varint(ts, pos)?;
        let start = after_len;
        let end = start.checked_add(len).ok_or(Unsupported::Malformed)?;
        if end > ts.len() {
            return Err(Unsupported::Malformed);
        }
        pos = end;
        match field {
            1 => {
                // Label.
                if nl >= labels.len() {
                    return Err(Unsupported::TooWide);
                }
                labels[nl] = decode_label(&ts[start..end])?;
                nl += 1;
            }
            2 => {
                // Sample.
                if ns >= samples.len() {
                    return Err(Unsupported::TooManyPoints);
                }
                samples[ns] = decode_sample(&ts[start..end])?;
                ns += 1;
            }
            _ => {}
        }
    }
    Ok((nl, ns))
}

fn decode_label(buf: &[u8]) -> Result<Label<'_>, Unsupported> {
    let mut name: &[u8] = &[];
    let mut value: &[u8] = &[];
    let mut pos = 0usize;
    while pos < buf.len() {
        let (field, wire, next) = read_tag(buf, pos)?;
        pos = next;
        if wire != 2 {
            pos = skip_field(buf, pos, wire)?;
            continue;
        }
        let (len, after_len) = read_varint(buf, pos)?;
        let end = after_len.checked_add(len).ok_or(Unsupported::Malformed)?;
        if end > buf.len() {
            return Err(Unsupported::Malformed);
        }
        let slice = &buf[after_len..end];
        pos = end;
        match field {
            1 => name = slice,
            2 => value = slice,
            _ => {}
        }
    }
    Ok(Label { name, value })
}

fn decode_sample(buf: &[u8]) -> Result<Sample, Unsupported> {
    let mut value = 0.0f64;
    let mut ts_ms = 0i64;
    let mut pos = 0usize;
    while pos < buf.len() {
        let (field, wire, next) = read_tag(buf, pos)?;
        pos = next;
        match (field, wire) {
            (1, 1) => {
                // double, 8 bytes LE.
                if pos + 8 > buf.len() {
                    return Err(Unsupported::Malformed);
                }
                let mut b = [0u8; 8];
                b.copy_from_slice(&buf[pos..pos + 8]);
                value = f64::from_le_bytes(b);
                pos += 8;
            }
            (2, 0) => {
                let (v, next) = read_varint(buf, pos)?;
                ts_ms = v as i64;
                pos = next;
            }
            _ => {
                pos = skip_field(buf, pos, wire)?;
            }
        }
    }
    Ok(Sample { ts_ms, value })
}

// ── protobuf primitives ───────────────────────────────────────────────

/// Read a field tag, returning (field_number, wire_type, next_index).
fn read_tag(buf: &[u8], pos: usize) -> Result<(u32, u8, usize), Unsupported> {
    let (key, next) = read_varint(buf, pos)?;
    let field = (key >> 3) as u32;
    let wire = (key & 0x07) as u8;
    Ok((field, wire, next))
}

/// Skip a field of the given wire type, returning the next index.
fn skip_field(buf: &[u8], pos: usize, wire: u8) -> Result<usize, Unsupported> {
    match wire {
        0 => Ok(read_varint(buf, pos)?.1),
        1 => pos
            .checked_add(8)
            .filter(|&e| e <= buf.len())
            .ok_or(Unsupported::Malformed),
        5 => pos
            .checked_add(4)
            .filter(|&e| e <= buf.len())
            .ok_or(Unsupported::Malformed),
        2 => {
            let (len, after) = read_varint(buf, pos)?;
            after
                .checked_add(len)
                .filter(|&e| e <= buf.len())
                .ok_or(Unsupported::Malformed)
        }
        _ => Err(Unsupported::Construct),
    }
}

fn read_varint(buf: &[u8], mut pos: usize) -> Result<(usize, usize), Unsupported> {
    let mut val: usize = 0;
    let mut shift = 0u32;
    let mut count = 0;
    while pos < buf.len() && count < 10 {
        let b = buf[pos];
        val |= ((b & 0x7f) as usize) << shift;
        pos += 1;
        count += 1;
        if b & 0x80 == 0 {
            return Ok((val, pos));
        }
        shift += 7;
    }
    Err(Unsupported::Malformed)
}
