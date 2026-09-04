//! Prometheus HTTP query-result encoder — renders the core's evaluated
//! series into the `matrix`/`vector` JSON shape a Prometheus datasource
//! (Grafana, Perses, `promtool`) consumes verbatim. Pure logic, `no_std`, no
//! allocation: it streams into a caller buffer, one series and one point at a
//! time, so the anchor never materialises the whole result.
//!
//! Shape produced:
//! ```json
//! {"status":"success","data":{"resultType":"matrix","result":[
//!   {"metric":{"__name__":"up","job":"api"},"values":[[1.500,"1"],[2.500,"1"]]}
//! ]}}
//! ```
//! Timestamps are seconds (the store keeps milliseconds); values are decimal
//! strings, per the Prometheus wire contract.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

#[path = "tsquery_core.rs"]
pub mod core;

use core::Label;

/// Streaming JSON result writer over a caller buffer. Every method returns
/// `None` on overflow; the anchor treats that as a "result too large" refusal
/// rather than emitting truncated JSON.
pub struct ResultWriter<'a> {
    buf: &'a mut [u8],
    n: usize,
    /// Whether the current `result` array already has a series (for commas).
    series_open: bool,
    /// Whether the current series' `values` array already has a point.
    point_open: bool,
}

impl<'a> ResultWriter<'a> {
    /// Begin a `matrix` (ranged) result. Use for `query_range`.
    pub fn begin_matrix(buf: &'a mut [u8]) -> Option<Self> {
        let mut w = ResultWriter {
            buf,
            n: 0,
            series_open: false,
            point_open: false,
        };
        w.put(b"{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[")?;
        Some(w)
    }

    /// Begin a `vector` (instant) result. Use for `query`.
    pub fn begin_vector(buf: &'a mut [u8]) -> Option<Self> {
        let mut w = ResultWriter {
            buf,
            n: 0,
            series_open: false,
            point_open: false,
        };
        w.put(b"{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[")?;
        Some(w)
    }

    /// Open a series with its label set. `metric_name` is rendered as the
    /// reserved `__name__` label; `labels` are the rest (any order).
    pub fn begin_series(&mut self, metric_name: &[u8], labels: &[Label<'_>]) -> Option<()> {
        if self.series_open {
            self.put(b",")?;
        }
        self.series_open = true;
        self.point_open = false;
        self.put(b"{\"metric\":{\"__name__\":")?;
        self.put_json_string(metric_name)?;
        for l in labels {
            self.put(b",")?;
            self.put_json_string(l.name)?;
            self.put(b":")?;
            self.put_json_string(l.value)?;
        }
        // This opener serves the `matrix` renderers, which emit an array of
        // points under "values"; the `vector` renderers below emit their
        // single point under "value" themselves.
        self.put(b"},\"values\":[")
    }

    /// Open an aggregated series carrying only `labels` and no `__name__` —
    /// PromQL aggregation results (`sum by (job)`) drop the metric name.
    pub fn begin_series_labels_only(&mut self, labels: &[Label<'_>]) -> Option<()> {
        if self.series_open {
            self.put(b",")?;
        }
        self.series_open = true;
        self.point_open = false;
        self.put(b"{\"metric\":{")?;
        for (k, l) in labels.iter().enumerate() {
            if k > 0 {
                self.put(b",")?;
            }
            self.put_json_string(l.name)?;
            self.put(b":")?;
            self.put_json_string(l.value)?;
        }
        self.put(b"},\"values\":[")
    }

    /// Emit one `[seconds, "value"]` point. `ts_ms` is the store's
    /// millisecond timestamp; it is rendered as seconds with millisecond
    /// precision.
    pub fn point(&mut self, ts_ms: u64, value: f64) -> Option<()> {
        if self.point_open {
            self.put(b",")?;
        }
        self.point_open = true;
        self.put(b"[")?;
        self.put_seconds(ts_ms)?;
        self.put(b",\"")?;
        self.put_f64(value)?;
        self.put(b"\"]")
    }

    /// Close the current series' `values` array and object.
    pub fn end_series(&mut self) -> Option<()> {
        self.put(b"]}")
    }

    /// Emit a complete instant-vector series: `{"metric":{__name__,labels},
    /// "value":[seconds,"v"]}`. Use inside `begin_vector`.
    pub fn vector_series(
        &mut self,
        metric_name: &[u8],
        labels: &[Label<'_>],
        ts_ms: u64,
        value: f64,
    ) -> Option<()> {
        if self.series_open {
            self.put(b",")?;
        }
        self.series_open = true;
        self.put(b"{\"metric\":{\"__name__\":")?;
        self.put_json_string(metric_name)?;
        for l in labels {
            self.put(b",")?;
            self.put_json_string(l.name)?;
            self.put(b":")?;
            self.put_json_string(l.value)?;
        }
        self.put(b"},\"value\":[")?;
        self.put_seconds(ts_ms)?;
        self.put(b",\"")?;
        self.put_f64(value)?;
        self.put(b"\"]}")
    }

    /// An aggregated instant-vector series (labels only, no `__name__`).
    pub fn vector_series_labels_only(
        &mut self,
        labels: &[Label<'_>],
        ts_ms: u64,
        value: f64,
    ) -> Option<()> {
        if self.series_open {
            self.put(b",")?;
        }
        self.series_open = true;
        self.put(b"{\"metric\":{")?;
        for (k, l) in labels.iter().enumerate() {
            if k > 0 {
                self.put(b",")?;
            }
            self.put_json_string(l.name)?;
            self.put(b":")?;
            self.put_json_string(l.value)?;
        }
        self.put(b"},\"value\":[")?;
        self.put_seconds(ts_ms)?;
        self.put(b",\"")?;
        self.put_f64(value)?;
        self.put(b"\"]}")
    }

    /// Close the result and return the encoded length.
    pub fn finish(mut self) -> Option<usize> {
        self.put(b"]}}")?;
        Some(self.n)
    }

    // ── primitives ────────────────────────────────────────────────────

    fn put(&mut self, bytes: &[u8]) -> Option<()> {
        if self.n + bytes.len() > self.buf.len() {
            return None;
        }
        self.buf[self.n..self.n + bytes.len()].copy_from_slice(bytes);
        self.n += bytes.len();
        Some(())
    }

    /// A JSON string with the mandatory escapes (`"`, `\`, and control
    /// chars). Label names/values in metrics are almost always bare, but a
    /// value can carry a quote; escape rather than emit invalid JSON.
    fn put_json_string(&mut self, s: &[u8]) -> Option<()> {
        self.put(b"\"")?;
        for &c in s {
            match c {
                b'"' => self.put(b"\\\"")?,
                b'\\' => self.put(b"\\\\")?,
                b'\n' => self.put(b"\\n")?,
                b'\r' => self.put(b"\\r")?,
                b'\t' => self.put(b"\\t")?,
                0x00..=0x1f => {
                    self.put(b"\\u00")?;
                    self.put(&[hex_digit(c >> 4), hex_digit(c & 0xf)])?;
                }
                _ => self.put(&[c])?,
            }
        }
        self.put(b"\"")
    }

    /// `ts_ms` → seconds with exactly three decimals, e.g. 1500 → `1.500`.
    fn put_seconds(&mut self, ts_ms: u64) -> Option<()> {
        let secs = ts_ms / 1000;
        let ms = ts_ms % 1000;
        self.put_u64(secs)?;
        self.put(b".")?;
        // Three-digit fractional, zero-padded.
        let d = [
            b'0' + (ms / 100) as u8,
            b'0' + ((ms / 10) % 10) as u8,
            b'0' + (ms % 10) as u8,
        ];
        self.put(&d)
    }

    fn put_u64(&mut self, v: u64) -> Option<()> {
        let mut tmp = [0u8; 20];
        let mut i = tmp.len();
        let mut n = v;
        loop {
            i -= 1;
            tmp[i] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        self.put(&tmp[i..])
    }

    /// A bounded decimal rendering of `value`: sign, integer part, and up to
    /// six fractional digits with trailing zeros trimmed. Non-finite values
    /// render as `NaN`/`+Inf`/`-Inf`, matching Prometheus.
    fn put_f64(&mut self, value: f64) -> Option<()> {
        if value.is_nan() {
            return self.put(b"NaN");
        }
        if value.is_infinite() {
            return self.put(if value > 0.0 { b"+Inf" } else { b"-Inf" });
        }
        let mut v = value;
        if v.is_sign_negative() && v != 0.0 {
            self.put(b"-")?;
            v = -v;
        }
        let int_part = v as u64;
        self.put_u64(int_part)?;
        // Fractional: scale by 1e6, round, trim.
        let frac = v - (int_part as f64);
        let mut scaled = (frac * 1_000_000.0 + 0.5) as u64;
        if scaled >= 1_000_000 {
            // Rounding carried into the integer part; re-emit is overkill for
            // metric values — clamp the fractional to its max instead.
            scaled = 999_999;
        }
        if scaled == 0 {
            return Some(());
        }
        // Build six digits, trim trailing zeros.
        let mut digits = [0u8; 6];
        let mut s = scaled;
        for k in (0..6).rev() {
            digits[k] = b'0' + (s % 10) as u8;
            s /= 10;
        }
        let mut end = 6;
        while end > 1 && digits[end - 1] == b'0' {
            end -= 1;
        }
        self.put(b".")?;
        self.put(&digits[..end])
    }
}

fn hex_digit(n: u8) -> u8 {
    if n < 10 {
        b'0' + n
    } else {
        b'a' + (n - 10)
    }
}
