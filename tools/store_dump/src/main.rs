//! `lattice-store-dump <dir> [--keys] [--prefix P]` — print a lattice disk
//! store's live keys and values.
//!
//! `<dir>` is the store's directory (a worker's `store_dir`), or the
//! working directory a disk worker ran in (its store is `<dir>/kv/`). The store is copied to a temporary directory and opened
//! there exactly as `kv_state_worker` opens it: the runs recovered, the
//! command log's tail replayed on top (the clock seeded at its start), so
//! what prints is what the store would serve — and the original is never
//! touched. Then every key of the flat protocol keyspace is printed, one
//! per line: `key<TAB>value` (a large value whole), or the key alone with
//! `--keys`. Keys sort.
//!
//! The inspection side of lattice as fluxor's store: where a test read a
//! host store's `store.log`, it reads this.

#[path = "../../../modules/common/kv_store.rs"]
mod kv_store;

#[path = "../../../modules/common/command_log.rs"]
mod command_log;

#[path = "../../../modules/common/types.rs"]
mod types;

#[path = "../../../modules/common/kv_large.rs"]
mod kv_large;

use command_log::{CommandLog, Replay, PAYLOAD_MAX};
use kv_large::{Expander, Need};
use kv_store::disk_store::run_storage::{FileKind, RunStorage, StorageError};
use kv_store::disk_store::{DiskState, DiskStore};
use kv_store::{apply_mat_ctx, DiskMaterializer, Materializer as _, ReadPolicy};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// `RunStorage` over a directory, named as the worker's FS layout names
/// its files on linux (`fs_run_storage.rs`): `K<id:7 hex>.KRN|KMN|KLG`.
struct DirStorage {
    dir: PathBuf,
}

impl DirStorage {
    fn path(&self, kind: FileKind, id: u32) -> PathBuf {
        let ext = match kind {
            FileKind::Run => "KRN",
            FileKind::Manifest => "KMN",
            FileKind::Log => "KLG",
        };
        self.dir.join(format!("K{id:07x}.{ext}"))
    }
}

fn io<T>(r: std::io::Result<T>) -> Result<T, StorageError> {
    r.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            StorageError::NotFound
        } else {
            StorageError::IoFault
        }
    })
}

impl RunStorage for DirStorage {
    fn create(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path(kind, id))
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(StorageError::AlreadyExists)
            }
            Err(_) => Err(StorageError::IoFault),
        }
    }

    fn append(&mut self, kind: FileKind, id: u32, data: &[u8]) -> Result<(), StorageError> {
        let mut f = io(fs::OpenOptions::new()
            .append(true)
            .open(self.path(kind, id)))?;
        io(f.write_all(data))
    }

    fn fsync(&mut self, _kind: FileKind, _id: u32) -> Result<(), StorageError> {
        // A scratch copy: nothing here needs to survive.
        Ok(())
    }

    fn write_at(
        &mut self,
        kind: FileKind,
        id: u32,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        use std::io::{Seek, SeekFrom};
        let mut f = io(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.path(kind, id)))?;
        io(f.seek(SeekFrom::Start(offset)))?;
        io(f.write_all(data))
    }

    fn read_at(
        &self,
        kind: FileKind,
        id: u32,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, StorageError> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = io(fs::File::open(self.path(kind, id)))?;
        io(f.seek(SeekFrom::Start(offset)))?;
        io(f.read(out))
    }

    fn delete(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        io(fs::remove_file(self.path(kind, id)))
    }

    fn list(&self, kind: FileKind, out: &mut [u32]) -> Result<usize, StorageError> {
        let ext = match kind {
            FileKind::Run => ".KRN",
            FileKind::Manifest => ".KMN",
            FileKind::Log => ".KLG",
        };
        let mut n = 0usize;
        for entry in io(fs::read_dir(&self.dir))? {
            let name = io(entry)?.file_name().to_string_lossy().to_uppercase();
            let Some(hex) = name.strip_prefix('K').and_then(|r| r.strip_suffix(ext)) else {
                continue;
            };
            let Ok(id) = u32::from_str_radix(hex, 16) else {
                continue;
            };
            let slot = out.get_mut(n).ok_or(StorageError::Overflow)?;
            *slot = id;
            n += 1;
        }
        Ok(n)
    }
}

/// The store, opened as the worker opens it.
struct Store {
    state: Box<DiskState>,
    storage: DirStorage,
    revision: u64,
    floor: u64,
}

impl Store {
    fn open(dir: PathBuf) -> Result<Store, String> {
        let layout = std::alloc::Layout::new::<DiskState>();
        // SAFETY: DiskState is repr(C) all-POD; zeroed is a valid empty
        // state, which `init` then sets up.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) }.cast::<DiskState>();
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        // SAFETY: allocated for exactly this layout, uniquely owned.
        let mut state: Box<DiskState> = unsafe { Box::from_raw(ptr) };
        state.init();
        let mut s = Store {
            state,
            storage: DirStorage { dir },
            revision: 0,
            floor: 0,
        };
        {
            let mut ds = DiskStore::new(&mut s.state, &mut s.storage);
            while !ds.prepared() {
                ds.prepare_step().map_err(|e| format!("prepare: {e:?}"))?;
            }
            ds.recover().map_err(|e| format!("recover: {e:?}"))?;
        }
        s.floor = s.state.highest_revision;
        s.revision = s.floor;
        // The command log's tail, replayed as the worker replays it.
        let mut log = CommandLog::new();
        match log.recover(&mut s.storage, s.floor) {
            Ok(None) => {}
            Ok(Some(start)) => {
                s.revision = start;
                let mut rp = Replay::new();
                let mut buf = vec![0u8; PAYLOAD_MAX];
                let mut out = vec![0u8; 65536];
                while let Some(r) = rp
                    .next(&log, &s.storage, &mut buf)
                    .map_err(|e| format!("command log: {e:?}"))?
                {
                    let room = (r.room as usize).min(out.len());
                    let policy = ReadPolicy {
                        consistency: r.consistency,
                        read_timestamp: 0,
                    };
                    let (op, body, now, ts, id) =
                        (r.op, r.body.to_vec(), r.now_ms, r.commit_ts, r.identity);
                    s.apply_as(op, &body, &mut out[..room], now, ts, id, policy);
                }
            }
            Err(e) => return Err(format!("command log: {e:?}")),
        }
        Ok(s)
    }

    fn apply_as(
        &mut self,
        op: u8,
        body: &[u8],
        out: &mut [u8],
        now_ms: u64,
        commit_ts: u64,
        (t, d, k): (u32, u32, u32),
        policy: ReadPolicy,
    ) -> (u8, usize) {
        let store = DiskStore::new(&mut self.state, &mut self.storage);
        let mut mat = DiskMaterializer::new(store, self.revision, self.floor);
        mat.set_key_identity(t, d, k);
        mat.set_commit_ts(commit_ts);
        let r = apply_mat_ctx(&mut mat, op, body, out, now_ms, policy);
        self.revision = mat.revision;
        r
    }

    /// A read in the flat keyspace, large values expanded.
    fn read(&mut self, op: u8, body: &[u8], out: &mut [u8]) -> (u8, usize) {
        let mut raw = vec![0u8; 1 << 20];
        let (r, n) = self.apply_as(op, body, &mut raw, 0, 0, (0, 0, 0), ReadPolicy::LATEST);
        if !Expander::wants(op, r) {
            out[..n].copy_from_slice(&raw[..n]);
            return (r, n);
        }
        let mut ex = Expander::new();
        ex.start(op, body, r, &raw[..n], out).expect("expand");
        let mut chunk = vec![0u8; 4096];
        loop {
            match ex.step().expect("expand") {
                Need::Chunk(h) => {
                    let (cr, cn) = self.apply_as(
                        kv_large::FETCH_OP,
                        &kv_large::chunk_request(&h),
                        &mut chunk,
                        0,
                        0,
                        (0, 0, 0),
                        ReadPolicy::LATEST,
                    );
                    assert_eq!(
                        cr,
                        types::KV_RESULT_OK,
                        "a chunk the store names is missing"
                    );
                    ex.take(&chunk[..cn], out).expect("expand");
                }
                Need::Done(result, len) => return (result, len),
            }
        }
    }

    /// Every live key under `prefix` and its value, in key order.
    fn entries(&mut self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut end = prefix.to_vec();
        while let Some(last) = end.pop() {
            if last != 0xFF {
                end.push(last + 1);
                break;
            }
        }
        if end.is_empty() {
            end.push(0);
        }
        let mut from = prefix.to_vec();
        let mut all = Vec::new();
        let mut out = vec![0u8; 16 << 20];
        loop {
            let mut b = Vec::new();
            b.extend((from.len() as u16).to_le_bytes());
            b.extend(&from);
            b.extend((end.len() as u16).to_le_bytes());
            b.extend(&end);
            b.extend(0u64.to_le_bytes());
            b.extend(256u32.to_le_bytes());
            b.extend([types::RANGE_META_FLAG_MANIFESTS, 0, 0]);
            b.extend([0u8; 32]);
            let (r, n) = self.read(types::KV_OP_RANGE_META, &b, &mut out);
            assert_eq!(r, types::KV_RESULT_KVS, "the store refused a read: {r:#x}");
            let page = &out[..n];
            let more = page[8] != 0;
            let count = u16::from_le_bytes([page[9], page[10]]) as usize;
            let mut p = 11usize;
            for _ in 0..count {
                let kl = u16::from_le_bytes([page[p], page[p + 1]]) as usize;
                let key = page[p + 2..p + 2 + kl].to_vec();
                let v = p + 2 + kl;
                let vl = u32::from_le_bytes(page[v..v + 4].try_into().unwrap()) as usize;
                let val = page[v + 4..v + 4 + vl].to_vec();
                p = v + 4 + vl + 32;
                from = key.clone();
                from.push(0);
                all.push((key, val));
            }
            if !more || count == 0 {
                return all;
            }
        }
    }
}

/// Copy `from` (a store directory) into a fresh temporary one.
fn scratch_copy(from: &Path) -> PathBuf {
    let to = std::env::temp_dir().join(format!(
        "lattice-store-dump-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    fs::create_dir_all(&to).expect("scratch directory");
    for entry in fs::read_dir(from).expect("the store directory") {
        let entry = entry.expect("a directory entry");
        if entry.file_type().is_ok_and(|t| t.is_file()) {
            fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
        }
    }
    to
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut dir = None;
    let mut keys_only = false;
    let mut prefix = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--keys" => keys_only = true,
            "--prefix" => prefix = args.next().expect("--prefix P").into_bytes(),
            _ => dir = Some(PathBuf::from(a)),
        }
    }
    let Some(dir) = dir else {
        eprintln!("usage: lattice-store-dump <store dir | working dir> [--keys] [--prefix P]");
        std::process::exit(2);
    };
    let kv = if dir.join("kv").is_dir() {
        dir.join("kv")
    } else {
        dir
    };
    if !kv.is_dir() {
        eprintln!("no store: {} is not a directory", kv.display());
        std::process::exit(1);
    }
    let copy = scratch_copy(&kv);
    let result = Store::open(copy.clone());
    let mut store = match result {
        Ok(s) => s,
        Err(e) => {
            let _ = fs::remove_dir_all(&copy);
            eprintln!("cannot open the store: {e}");
            std::process::exit(1);
        }
    };
    let entries = store.entries(&prefix);
    let _ = fs::remove_dir_all(&copy);
    let stdout = std::io::stdout();
    let mut w = std::io::BufWriter::new(stdout.lock());
    for (k, v) in entries {
        let _ = w.write_all(&k);
        if !keys_only {
            let _ = w.write_all(b"\t");
            let _ = w.write_all(&v);
        }
        let _ = w.write_all(b"\n");
    }
}
