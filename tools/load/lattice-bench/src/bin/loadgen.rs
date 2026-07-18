//! `lattice-loadgen` — off-DUT open-loop load generator (L1+).
//!
//! Drives the DUT over a REAL client path at a fixed offered rate using a
//! fixed-interval arrival process, sharded across worker threads, each with
//! its own latency histograms merged at the end. Three protocol modes share
//! the identical pacing / accounting / reporting machinery:
//!
//! - `--proto redis` (default): raw RESP2 over a persistent TCP connection
//!   per shard — `SET`/`GET`.
//! - `--proto memcached`: memcached TEXT (ASCII) protocol, the dialect the
//!   Lattice `memcached_stream_anchor` serves (flags always 0, exptime 0,
//!   no `noreply` — we need the `STORED` ack for latency).
//! - `--proto etcd`: gRPC over hand-rolled HTTP/2 against the Lattice
//!   `etcd_edge_anchor` — `etcdserverpb.KV/Put` and `/Range` (point get),
//!   one stream per request on a persistent connection. HPACK is
//!   static-table + raw literals, mirroring `modules/common/etcd_codec.rs`.
//!
//! Latency is **coordinated-omission corrected**: each request's latency is
//! measured from its *intended* send time, so a stalled server shows up as a
//! growing tail instead of pacing our loop. **Read and write tails are kept
//! separate** (writes traverse raft→WAL→quorum→apply; reads are
//! serializable/snapshot today) — never average them.
//!
//! offered / accepted(sent) / committed(ok) accounting: a write reply
//! (`+OK` / `STORED` / grpc-status 0) only returns after the entry is
//! applied, so `ok` writes == committed writes.
//!
//! It self-reports the harness-headroom verdict (achieved vs offered) so a run
//! the generator (not the DUT) bottlenecked is flagged HARNESS_BOUND.
//!
//! Usage:
//!   lattice-loadgen --host 127.0.0.1:6379 --rate 2000 --duration 10 \
//!       --conns 4 --read-ratio 95 --value-size 64 --keyspace 10000
//!   lattice-loadgen --proto memcached --host 127.0.0.1:11211 --rate 500
//!   lattice-loadgen --proto etcd --host 127.0.0.1:2379 --rate 500
//!
//! std-only: one OS thread per shard, raw sockets. Fine for a Pi-class
//! driver at the rates a 1 GbE link admits.

use std::io::{prelude::*, BufReader};
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use lattice_bench::{JsonObj, LatencyHist};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Proto {
    Redis,
    Memcached,
    Etcd,
}

impl Proto {
    fn label(self) -> &'static str {
        match self {
            Self::Redis => "redis-resp2",
            Self::Memcached => "memcached-text",
            Self::Etcd => "etcd-grpc",
        }
    }
}

struct Args {
    proto: Proto,
    host: String,
    rate: u64,
    duration_secs: u64,
    conns: u64,
    read_ratio: u64, // 0..=100 percent reads
    value_size: usize,
    keyspace: u64,
    key_prefix: String,
}

fn usage() -> ! {
    eprintln!(
        "lattice-loadgen --host <addr:port> --rate <req/s> [--duration N]\n\
         \x20  [--proto redis|memcached|etcd] [--conns N] [--read-ratio 0..100]\n\
         \x20  [--value-size B] [--keyspace N] [--key-prefix lk:]"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut a = Args {
        proto: Proto::Redis,
        host: String::new(),
        rate: 1000,
        duration_secs: 10,
        conns: 4,
        read_ratio: 95,
        value_size: 64,
        keyspace: 10_000,
        key_prefix: "lk:".to_string(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = || it.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--proto" => {
                a.proto = match next().as_str() {
                    "redis" => Proto::Redis,
                    "memcached" => Proto::Memcached,
                    "etcd" => Proto::Etcd,
                    other => {
                        eprintln!("unknown --proto: {other} (expected redis|memcached|etcd)");
                        usage();
                    }
                }
            }
            "--host" => a.host = next(),
            "--rate" => a.rate = next().parse().unwrap_or_else(|_| usage()),
            "--duration" => a.duration_secs = next().parse().unwrap_or_else(|_| usage()),
            "--conns" => a.conns = next().parse().unwrap_or_else(|_| usage()),
            "--read-ratio" => a.read_ratio = next().parse().unwrap_or_else(|_| usage()),
            "--value-size" => a.value_size = next().parse().unwrap_or_else(|_| usage()),
            "--keyspace" => a.keyspace = next().parse().unwrap_or_else(|_| usage()),
            "--key-prefix" => a.key_prefix = next(),
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown flag: {other}");
                usage();
            }
        }
    }
    if a.host.is_empty() || a.rate == 0 || a.conns == 0 || a.read_ratio > 100 {
        usage();
    }
    a
}

// ── redis (RESP2) ─────────────────────────────────────────────────────

/// RESP2 array request: `*N\r\n` then each arg as `$len\r\n<bytes>\r\n`.
fn encode_cmd(parts: &[&[u8]], out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
}

/// Read exactly one RESP reply. Returns Ok(true) for a valid reply (including a
/// `$-1` null-bulk GET miss), Ok(false) for a `-ERR` error reply, Err on IO.
fn read_reply<R: BufRead>(r: &mut R) -> std::io::Result<bool> {
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    match line.first().copied().unwrap_or(b'-') {
        b'+' | b':' => Ok(true),
        b'-' => Ok(false),
        b'$' => {
            // [$][len]\r\n ; len == -1 → null bulk (no data line)
            let s = std::str::from_utf8(&line[1..]).unwrap_or("").trim();
            let len: i64 = s.parse().unwrap_or(-1);
            if len >= 0 {
                let mut data = vec![0u8; len as usize + 2]; // data + CRLF
                r.read_exact(&mut data)?;
            }
            Ok(true)
        }
        b'*' => Ok(true), // arrays not used by our ops; accept
        _ => Ok(false),
    }
}

// ── memcached (TEXT / ASCII) ──────────────────────────────────────────
//
// The dialect the Lattice memcached_stream_anchor serves (see
// modules/common/memcached_codec.rs): `set <key> <flags> <exptime>
// <bytes>\r\n<data>\r\n` → `STORED\r\n`; `get <key>\r\n` →
// `VALUE <key> 0 <bytes>\r\n<data>\r\nEND\r\n` on a hit or bare
// `END\r\n` on a miss. flags/exptime are always sent as 0 (the anchor
// echoes flags=0). `noreply` is deliberately NOT used: the ack is the
// latency signal.

fn read_line<R: BufRead>(r: &mut R, line: &mut Vec<u8>) -> std::io::Result<()> {
    line.clear();
    if r.read_until(b'\n', line)? == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    Ok(())
}

/// One closed-loop memcached TEXT op (write→flush→read reply). Returns
/// Ok(true) for a valid reply (including a GET miss `END`), Ok(false)
/// for an unexpected reply (`NOT_STORED`, `SERVER_ERROR`, …), Err on IO.
fn memcached_op<R: BufRead>(
    wtr: &mut TcpStream,
    rdr: &mut R,
    cmd: &mut Vec<u8>,
    is_read: bool,
    key: &str,
    value: &[u8],
) -> std::io::Result<bool> {
    cmd.clear();
    if is_read {
        cmd.extend_from_slice(b"get ");
        cmd.extend_from_slice(key.as_bytes());
        cmd.extend_from_slice(b"\r\n");
    } else {
        cmd.extend_from_slice(format!("set {key} 0 0 {}\r\n", value.len()).as_bytes());
        cmd.extend_from_slice(value);
        cmd.extend_from_slice(b"\r\n");
    }
    wtr.write_all(cmd)?;
    wtr.flush()?;

    let mut line = Vec::new();
    read_line(rdr, &mut line)?;
    if !is_read {
        return Ok(line.starts_with(b"STORED"));
    }
    if line.starts_with(b"END") {
        return Ok(true); // miss — a valid outcome, mirrors redis null-bulk
    }
    if !line.starts_with(b"VALUE ") {
        return Ok(false);
    }
    // VALUE <key> <flags> <bytes> [<cas>]\r\n — data length is token 3.
    let nbytes: usize = {
        let text = std::str::from_utf8(&line).unwrap_or("");
        match text.split_ascii_whitespace().nth(3).map(str::parse) {
            Some(Ok(n)) => n,
            _ => return Ok(false),
        }
    };
    let mut data = vec![0u8; nbytes + 2]; // data + CRLF
    rdr.read_exact(&mut data)?;
    read_line(rdr, &mut line)?;
    Ok(line.starts_with(b"END"))
}

// ── etcd (gRPC over HTTP/2) ───────────────────────────────────────────
//
// A minimal std-only HTTP/2 gRPC client tailored to the Lattice
// etcd_edge_anchor's server (modules/common/etcd_codec.rs +
// modules/app/etcd_edge_anchor/mod.rs):
//
// - The anchor routes on `:path` only (`/etcdserverpb.KV/{Range,Put}`);
//   remaining headers are decoded and discarded, so we encode
//   `:method`/`:scheme` as static-indexed and the rest as HPACK
//   literals-without-indexing (raw, no Huffman) — the exact forms its
//   static-table-only decoder handles.
// - Unary responses are HEADERS (`:status 200`, no END_STREAM), one
//   DATA frame with the 5-byte-prefixed gRPC message, then a trailers
//   HEADERS frame with END_STREAM carrying `grpc-status` as a raw
//   literal. Success == grpc-status 0 (a Range miss is status 0 with
//   empty kvs, mirroring a redis GET miss).
// - The anchor does not track its send-side flow-control window and
//   window-updates our DATA itself, so the client needs no
//   WINDOW_UPDATE bookkeeping at these frame sizes.
// - Deployed DUT graphs can interleave an UNFRAMED `HTTP/1.1 ...` text
//   response on the same connection (the foundation h1 server answering
//   alongside the anchor — observed on the bare-metal rig; absent on a
//   freshly built local graph). Parsed naively as a frame header,
//   "HTT" is a multi-megabyte length and the client blocks until the
//   5 s socket timeout, then cascades into coordinated-omission tails.
//   The client therefore resyncs: an `HTTP/` at a frame boundary is
//   consumed as one complete h1 response (headers + Content-Length
//   body) and frame parsing resumes; any other absurd frame length
//   fails fast instead of blocking.

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const FT_DATA: u8 = 0x0;
const FT_HEADERS: u8 = 0x1;
const FT_RST_STREAM: u8 = 0x3;
const FT_SETTINGS: u8 = 0x4;
const FT_PING: u8 = 0x6;
const FT_GOAWAY: u8 = 0x7;
const H2_FLAG_END_STREAM: u8 = 0x1;
const H2_FLAG_ACK: u8 = 0x1;
const H2_FLAG_END_HEADERS: u8 = 0x4;

/// Append an RFC 7540 §4.1 9-byte frame header.
fn h2_frame_header(out: &mut Vec<u8>, len: usize, ftype: u8, flags: u8, stream_id: u32) {
    out.push((len >> 16) as u8);
    out.push((len >> 8) as u8);
    out.push(len as u8);
    out.push(ftype);
    out.push(flags);
    out.extend_from_slice(&(stream_id & 0x7FFF_FFFF).to_be_bytes());
}

/// HPACK integer (RFC 7541 §5.1) with an N-bit prefix under `pattern`.
fn hpack_write_int(out: &mut Vec<u8>, prefix_bits: u8, pattern: u8, mut v: u64) {
    let max = (1u64 << prefix_bits) - 1;
    if v < max {
        out.push(pattern | v as u8);
        return;
    }
    out.push(pattern | max as u8);
    v -= max;
    while v >= 128 {
        out.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// HPACK literal-without-indexing, new name, raw (non-Huffman) strings —
/// the mirror of `etcd_codec::hpack_encode_literal`.
fn hpack_write_literal(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.push(0x00);
    hpack_write_int(out, 7, 0x00, name.len() as u64);
    out.extend_from_slice(name);
    hpack_write_int(out, 7, 0x00, value.len() as u64);
    out.extend_from_slice(value);
}

/// HPACK integer decode; advances `i` past the whole encoded int.
fn hpack_read_int(block: &[u8], i: &mut usize, prefix_bits: u8) -> Option<u64> {
    let first = *block.get(*i)?;
    *i += 1;
    let max = (1u64 << prefix_bits) - 1;
    let mut v = u64::from(first) & max;
    if v < max {
        return Some(v);
    }
    let mut shift = 0u32;
    loop {
        let b = *block.get(*i)?;
        *i += 1;
        v = v.checked_add(u64::from(b & 0x7F) << shift)?;
        if b & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
        if shift > 28 {
            return None;
        }
    }
}

/// HPACK string literal; returns `(bytes, huffman_flag)`.
fn hpack_read_string<'a>(block: &'a [u8], i: &mut usize) -> Option<(&'a [u8], bool)> {
    let huff = block.get(*i)? & 0x80 != 0;
    let len = hpack_read_int(block, i, 7)? as usize;
    let s = block.get(*i..*i + len)?;
    *i += len;
    Some((s, huff))
}

/// Extract `grpc-status` from a trailers HPACK block. Handles the forms
/// the anchor emits (static-indexed entries + raw literals); Huffman
/// strings are skipped (the anchor never emits them).
fn grpc_status(block: &[u8]) -> Option<u64> {
    let mut i = 0usize;
    let mut status = None;
    while i < block.len() {
        let b = block[i];
        if b & 0x80 != 0 {
            hpack_read_int(block, &mut i, 7)?; // fully indexed — skip
        } else if b & 0x40 != 0 {
            // literal with incremental indexing
            let idx = hpack_read_int(block, &mut i, 6)?;
            if idx == 0 {
                hpack_read_string(block, &mut i)?;
            }
            hpack_read_string(block, &mut i)?;
        } else if b & 0x20 != 0 {
            hpack_read_int(block, &mut i, 5)?; // dynamic table size update
        } else {
            // literal without indexing / never-indexed (4-bit prefix)
            let idx = hpack_read_int(block, &mut i, 4)?;
            let name = if idx == 0 {
                Some(hpack_read_string(block, &mut i)?)
            } else {
                None
            };
            let (value, vhuff) = hpack_read_string(block, &mut i)?;
            if let Some((n, nhuff)) = name {
                if !nhuff && !vhuff && n == b"grpc-status" {
                    status = std::str::from_utf8(value).ok()?.parse().ok();
                }
            }
        }
    }
    status
}

/// Protobuf varint.
fn pb_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 128 {
        out.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Protobuf length-delimited (wire type 2) field.
fn pb_bytes_field(out: &mut Vec<u8>, field: u32, data: &[u8]) {
    pb_varint(out, (u64::from(field) << 3) | 2);
    pb_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

struct EtcdConn {
    wtr: TcpStream,
    rdr: BufReader<TcpStream>,
    next_stream_id: u32,
    frame: Vec<u8>,   // outbound request bytes (HEADERS + DATA)
    payload: Vec<u8>, // inbound frame payload scratch
}

impl EtcdConn {
    fn connect(host: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(host)?;
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let mut wtr = stream.try_clone()?;
        let rdr = BufReader::new(stream);
        // Connection preface + our (empty) SETTINGS.
        wtr.write_all(H2_PREFACE)?;
        let mut settings = Vec::with_capacity(9);
        h2_frame_header(&mut settings, 0, FT_SETTINGS, 0, 0);
        wtr.write_all(&settings)?;
        wtr.flush()?;
        Ok(Self {
            wtr,
            rdr,
            next_stream_id: 1,
            frame: Vec::with_capacity(256),
            payload: Vec::new(),
        })
    }

    /// One closed-loop gRPC unary op: Put (write) or point Range (read)
    /// on a fresh stream; blocks until the trailers arrive. Ok(true) on
    /// grpc-status 0 (including a Range miss), Ok(false) on a non-zero
    /// status or RST_STREAM, Err on IO / connection loss.
    fn do_op(&mut self, is_read: bool, key: &str, value: &[u8]) -> std::io::Result<bool> {
        let sid = self.next_stream_id;
        self.next_stream_id += 2;

        // Protobuf message: RangeRequest{key=1} or PutRequest{key=1,value=2}.
        let mut msg = Vec::with_capacity(key.len() + value.len() + 8);
        pb_bytes_field(&mut msg, 1, key.as_bytes());
        let path: &[u8] = if is_read {
            b"/etcdserverpb.KV/Range"
        } else {
            pb_bytes_field(&mut msg, 2, value);
            b"/etcdserverpb.KV/Put"
        };

        let mut block = Vec::with_capacity(80);
        block.push(0x83); // :method: POST (static index 3)
        block.push(0x86); // :scheme: http  (static index 6)
        hpack_write_literal(&mut block, b":path", path);
        hpack_write_literal(&mut block, b"content-type", b"application/grpc");
        hpack_write_literal(&mut block, b"te", b"trailers");

        let f = &mut self.frame;
        f.clear();
        h2_frame_header(f, block.len(), FT_HEADERS, H2_FLAG_END_HEADERS, sid);
        f.extend_from_slice(&block);
        // DATA: 5-byte gRPC prefix (uncompressed + u32 BE length) + message.
        h2_frame_header(f, 5 + msg.len(), FT_DATA, H2_FLAG_END_STREAM, sid);
        f.push(0);
        f.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        f.extend_from_slice(&msg);
        self.wtr.write_all(&self.frame)?;
        self.wtr.flush()?;

        self.read_response(sid)
    }

    /// Consume one unframed `HTTP/1.1 ...` text response whose first 9
    /// bytes were already read as a would-be frame header: finish the
    /// status line, drain headers noting `Content-Length`, drain the
    /// body. Leaves the reader back at an HTTP/2 frame boundary.
    fn skip_h1_response(&mut self) -> std::io::Result<()> {
        let mut line = Vec::new();
        read_line(&mut self.rdr, &mut line)?; // rest of the status line
        let mut content_len = 0usize;
        loop {
            read_line(&mut self.rdr, &mut line)?;
            if line == b"\r\n" || line == b"\n" {
                break;
            }
            let lower: Vec<u8> = line.iter().map(u8::to_ascii_lowercase).collect();
            if let Some(rest) = lower.strip_prefix(b"content-length:") {
                let digits: String = rest
                    .iter()
                    .map(|&b| b as char)
                    .filter(char::is_ascii_digit)
                    .collect();
                content_len = digits.parse().unwrap_or(0);
            }
        }
        let mut body = vec![0u8; content_len];
        self.rdr.read_exact(&mut body)?;
        Ok(())
    }

    fn read_response(&mut self, sid: u32) -> std::io::Result<bool> {
        // Larger than any frame the anchor emits (its SETTINGS cap is
        // 16 KiB); a length beyond this means we lost frame sync — fail
        // fast rather than blocking on bytes that will never arrive.
        const MAX_FRAME_LEN: usize = 1 << 20;
        loop {
            let mut hdr = [0u8; 9];
            self.rdr.read_exact(&mut hdr)?;
            if &hdr[..5] == b"HTTP/" {
                // Unframed h1 error response interleaved by the DUT
                // (see module comment) — consume it and resync.
                self.skip_h1_response()?;
                continue;
            }
            let len =
                (usize::from(hdr[0]) << 16) | (usize::from(hdr[1]) << 8) | usize::from(hdr[2]);
            if len > MAX_FRAME_LEN {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "http/2 frame sync lost",
                ));
            }
            let (ftype, flags) = (hdr[3], hdr[4]);
            let fsid = u32::from_be_bytes([hdr[5], hdr[6], hdr[7], hdr[8]]) & 0x7FFF_FFFF;
            self.payload.resize(len, 0);
            self.rdr.read_exact(&mut self.payload)?;
            match ftype {
                FT_SETTINGS if flags & H2_FLAG_ACK == 0 => {
                    let mut ack = Vec::with_capacity(9);
                    h2_frame_header(&mut ack, 0, FT_SETTINGS, H2_FLAG_ACK, 0);
                    self.wtr.write_all(&ack)?;
                    self.wtr.flush()?;
                }
                FT_PING if flags & H2_FLAG_ACK == 0 => {
                    let mut pong = Vec::with_capacity(9 + self.payload.len());
                    h2_frame_header(&mut pong, self.payload.len(), FT_PING, H2_FLAG_ACK, 0);
                    pong.extend_from_slice(&self.payload);
                    self.wtr.write_all(&pong)?;
                    self.wtr.flush()?;
                }
                FT_GOAWAY => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "server GOAWAY",
                    ));
                }
                FT_RST_STREAM if fsid == sid => return Ok(false),
                FT_HEADERS if fsid == sid && flags & H2_FLAG_END_STREAM != 0 => {
                    // Trailers (or a headers-only error reply): the
                    // grpc-status literal decides ok vs error.
                    return Ok(grpc_status(&self.payload) == Some(0));
                }
                // Response HEADERS (no END_STREAM), DATA (body ignored —
                // the trailer status is the ok/err signal), WINDOW_UPDATE,
                // SETTINGS/PING acks: nothing to do.
                _ => {}
            }
        }
    }
}

// ── per-connection client dispatch ────────────────────────────────────

enum Conn {
    Redis {
        wtr: TcpStream,
        rdr: BufReader<TcpStream>,
        cmd: Vec<u8>,
    },
    Memcached {
        wtr: TcpStream,
        rdr: BufReader<TcpStream>,
        cmd: Vec<u8>,
    },
    Etcd(Box<EtcdConn>),
}

impl Conn {
    fn connect(proto: Proto, host: &str, value_len: usize) -> std::io::Result<Self> {
        if proto == Proto::Etcd {
            return Ok(Self::Etcd(Box::new(EtcdConn::connect(host)?)));
        }
        let stream = TcpStream::connect(host)?;
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let wtr = stream.try_clone()?;
        let rdr = BufReader::new(stream);
        let cmd = Vec::with_capacity(64 + value_len);
        Ok(match proto {
            Proto::Redis => Self::Redis { wtr, rdr, cmd },
            Proto::Memcached => Self::Memcached { wtr, rdr, cmd },
            Proto::Etcd => unreachable!("handled above"),
        })
    }

    /// One closed-loop op: write→flush→read reply on this connection.
    fn do_op(&mut self, is_read: bool, key: &str, value: &[u8]) -> std::io::Result<bool> {
        match self {
            Self::Redis { wtr, rdr, cmd } => {
                if is_read {
                    encode_cmd(&[b"GET", key.as_bytes()], cmd);
                } else {
                    encode_cmd(&[b"SET", key.as_bytes(), value], cmd);
                }
                wtr.write_all(cmd)?;
                wtr.flush()?;
                read_reply(rdr)
            }
            Self::Memcached { wtr, rdr, cmd } => memcached_op(wtr, rdr, cmd, is_read, key, value),
            Self::Etcd(c) => c.do_op(is_read, key, value),
        }
    }
}

struct ShardResult {
    reads: LatencyHist,
    writes: LatencyHist,
    sent: u64,
    ok_reads: u64,
    ok_writes: u64,
    errors: u64,
}

// Each parameter is an independent, per-shard workload knob passed
// straight from argv; bundling them into a struct would add a type
// whose only purpose is to satisfy the arity lint.
#[expect(
    clippy::too_many_arguments,
    reason = "per-shard workload knobs passed verbatim from argv; a wrapper struct would add indirection without clarity"
)]
fn run_shard(
    shard: u64,
    proto: Proto,
    host: String,
    per_shard_rate: f64,
    duration: Duration,
    read_ratio: u64,
    value: Vec<u8>,
    keyspace: u64,
    key_prefix: String,
) -> ShardResult {
    let mut reads = LatencyHist::new();
    let mut writes = LatencyHist::new();
    let (mut sent, mut ok_reads, mut ok_writes, mut errors) = (0u64, 0u64, 0u64, 0u64);

    let mut conn = match Conn::connect(proto, &host, value.len()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[loadgen] shard {shard} connect {host}: {e}");
            return ShardResult {
                reads,
                writes,
                sent,
                ok_reads,
                ok_writes,
                errors: 1,
            };
        }
    };

    let interval = Duration::from_secs_f64(1.0 / per_shard_rate);
    let start = Instant::now();
    // xorshift64* seeded per shard so key selection varies without a rand dep.
    let mut rng: u64 = shard.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    let mut next_rng = || {
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    let mut i: u64 = 0;
    loop {
        let intended = start + interval.mul_f64(i as f64);
        let now = Instant::now();
        if now >= start + duration {
            break;
        }
        if intended > now {
            std::thread::sleep(intended - now);
        }
        let is_read = (next_rng() % 100) < read_ratio;
        let key_idx = next_rng() % keyspace.max(1);
        let key = format!("{key_prefix}{key_idx}");

        let ok = conn.do_op(is_read, &key, &value);
        let done = Instant::now();
        let latency = done.saturating_duration_since(intended);
        let us = latency.as_micros() as u64;
        match ok {
            Ok(true) => {
                if is_read {
                    reads.record(us);
                    ok_reads += 1;
                } else {
                    writes.record(us);
                    ok_writes += 1;
                }
            }
            Ok(false) | Err(_) => errors += 1,
        }
        sent += 1;
        i += 1;
    }
    ShardResult {
        reads,
        writes,
        sent,
        ok_reads,
        ok_writes,
        errors,
    }
}

fn tail(h: &LatencyHist) -> String {
    JsonObj::new()
        .num("count", h.count())
        .num("p50_us", h.percentile(50.0))
        .num("p99_us", h.percentile(99.0))
        .num("p999_us", h.percentile(99.9))
        .num("max_us", h.max())
        .num("mean_us", h.mean())
        .render()
}

fn main() {
    let a = parse_args();
    let per_shard = a.rate as f64 / a.conns as f64;
    let duration = Duration::from_secs(a.duration_secs);
    let value = vec![b'x'; a.value_size];

    eprintln!(
        "[loadgen] host={} offered={}/s conns={} dur={}s read_ratio={}% val={}B keyspace={}",
        a.host, a.rate, a.conns, a.duration_secs, a.read_ratio, a.value_size, a.keyspace
    );
    if a.proto != Proto::Redis {
        eprintln!("[loadgen] proto={}", a.proto.label());
    }

    let (tx, rx) = mpsc::channel();
    let wall = Instant::now();
    let mut handles = Vec::new();
    for shard in 0..a.conns {
        let (host, value, prefix, tx) = (
            a.host.clone(),
            value.clone(),
            a.key_prefix.clone(),
            tx.clone(),
        );
        let (proto, rr, ks) = (a.proto, a.read_ratio, a.keyspace);
        handles.push(std::thread::spawn(move || {
            let r = run_shard(
                shard, proto, host, per_shard, duration, rr, value, ks, prefix,
            );
            let _ = tx.send(r);
        }));
    }
    drop(tx);

    let mut reads = LatencyHist::new();
    let mut writes = LatencyHist::new();
    let (mut sent, mut ok_reads, mut ok_writes, mut errors) = (0u64, 0u64, 0u64, 0u64);
    for r in rx {
        reads.merge(&r.reads);
        writes.merge(&r.writes);
        sent += r.sent;
        ok_reads += r.ok_reads;
        ok_writes += r.ok_writes;
        errors += r.errors;
    }
    for h in handles {
        let _ = h.join();
    }
    let elapsed = wall.elapsed().as_secs_f64().max(0.001);
    let achieved = sent as f64 / elapsed;
    let ratio = achieved / a.rate as f64;
    let verdict = if ratio < 0.9 {
        "HARNESS_BOUND"
    } else {
        "DUT_ATTRIBUTABLE"
    };

    let report = JsonObj::new()
        .str("schema", "lattice-loadgen/1")
        .str("protocol", a.proto.label())
        .str("host", &a.host)
        .num("offered_rate", a.rate)
        .num("achieved_rate", format!("{achieved:.1}"))
        .num("conns", a.conns)
        .num("read_ratio_pct", a.read_ratio)
        .num("value_size", a.value_size)
        .num("keyspace", a.keyspace)
        .num("sent", sent)
        .num("ok_reads", ok_reads)
        .num("ok_writes", ok_writes)
        .num("committed_writes", ok_writes)
        .num("errors", errors)
        .raw("read_tail", tail(&reads))
        .raw("write_tail", tail(&writes))
        .str("headroom_verdict", verdict)
        .render();
    println!("{report}");
    eprintln!(
        "[loadgen] sent={sent} ok_r={ok_reads} ok_w={ok_writes} err={errors} achieved={achieved:.0}/s verdict={verdict}"
    );
}
