//! `lattice-scrape` — bracketed `/metrics` scrape → JSON baseline record.
//!
//! Scrapes the binary `/metrics` export (Clustor telemetry format, fed by
//! lattice's adapter_metrics → operations fan-in) at window start and end,
//! computes counter deltas + final gauges, and writes a JSON record with
//! provenance: git SHA, config hash, target, run id, workload. Dependency-free
//! so it is reproducible and cross-project comparable. Ported from
//! clustor-bench/src/bin/scrape.rs.
//!
//! Usage:
//!   lattice-scrape --host 192.168.1.9:19090 --window 12 \
//!       --config configs/bare-metal-pi5.yaml --target pi5 \
//!       --label l1-redis --workload "offered=2000,op=set,val=64" \
//!       --out perf/results/l1-redis-$(date +%Y%m%d).json
//!
//! `--out -` writes JSON to stdout.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lattice_bench::{fnv1a64, http_get, json_str, parse_export, JsonObj, Snapshot, KIND_COUNTER};

struct Args {
    host: String,
    path: String,
    window_secs: u64,
    config: Option<String>,
    target: String,
    label: String,
    workload: String,
    out: String,
}

fn usage() -> ! {
    eprintln!(
        "lattice-scrape --host <addr:port> [--window N] [--path /metrics]\n\
         \x20  [--config <yaml>] [--target <name>] [--label <name>]\n\
         \x20  [--workload <k=v,...>] [--out <file|->]"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut a = Args {
        host: String::new(),
        path: "/metrics".to_string(),
        window_secs: 10,
        config: None,
        target: "linux".to_string(),
        label: "run".to_string(),
        workload: String::new(),
        out: "-".to_string(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = || it.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--host" => a.host = next(),
            "--path" => a.path = next(),
            "--window" => a.window_secs = next().parse().unwrap_or_else(|_| usage()),
            "--config" => a.config = Some(next()),
            "--target" => a.target = next(),
            "--label" => a.label = next(),
            "--workload" => a.workload = next(),
            "--out" => a.out = next(),
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown flag: {other}");
                usage();
            }
        }
    }
    if a.host.is_empty() {
        usage();
    }
    a
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn scrape(host: &str, path: &str) -> Snapshot {
    match http_get(host, path, Duration::from_secs(5)) {
        Ok(body) => match parse_export(&body) {
            Ok(samples) => Snapshot::from_samples(&samples),
            Err(e) => {
                eprintln!("[scrape] parse error: {e}");
                Snapshot::default()
            }
        },
        Err(e) => {
            eprintln!("[scrape] http error: {e}");
            Snapshot::default()
        }
    }
}

fn main() {
    let a = parse_args();

    let config_hash = a
        .config
        .as_ref()
        .and_then(|p| std::fs::read(p).ok())
        .map(|b| format!("{:016x}", fnv1a64(&b)))
        .unwrap_or_else(|| "none".to_string());

    eprintln!("[scrape] start window: {} ({}s)", a.host, a.window_secs);
    let start = scrape(&a.host, &a.path);
    std::thread::sleep(Duration::from_secs(a.window_secs));
    let end = scrape(&a.host, &a.path);
    eprintln!(
        "[scrape] end window: {} start-records, {} end-records",
        start.by_key.len(),
        end.by_key.len()
    );

    let mut metric_objs: Vec<String> = Vec::new();
    let mut keys: Vec<_> = end.by_key.keys().copied().collect();
    keys.sort_unstable();
    for k in keys {
        let s = end.by_key[&k];
        let (mid, pid, met) = k;
        let delta = if s.kind == KIND_COUNTER {
            end.counter_delta(&start, k)
        } else {
            s.value
        };
        metric_objs.push(
            JsonObj::new()
                .num("module_id", mid)
                .num("partition_id", pid)
                .num("metric_id", met)
                .num("kind", s.kind)
                .num("start", start.get(k).unwrap_or(0))
                .num("end", s.value)
                .num("delta_or_value", delta)
                .render(),
        );
    }
    let metrics_arr = format!("[{}]", metric_objs.join(","));

    let record = JsonObj::new()
        .str("schema", "lattice-bench/1")
        .str("label", &a.label)
        .str("target", &a.target)
        .str("host", &a.host)
        .str("build_git_sha", &git_sha())
        .str("config", a.config.as_deref().unwrap_or(""))
        .str("config_hash", &config_hash)
        .num("run_id_unix", now_unix())
        .num("window_secs", a.window_secs)
        .str("workload", &a.workload)
        .num("start_record_count", start.by_key.len())
        .num("end_record_count", end.by_key.len())
        .raw("metrics", metrics_arr)
        .render();

    if a.out == "-" {
        println!("{record}");
    } else {
        match std::fs::write(&a.out, format!("{record}\n")) {
            Ok(()) => eprintln!("[scrape] wrote {}", a.out),
            Err(e) => {
                eprintln!("[scrape] write {}: {e}", json_str(&a.out));
                std::process::exit(1);
            }
        }
    }
}
