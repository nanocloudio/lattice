//! Semantic lint for the committed-clock path in a Lattice graph.
//!
//! `fluxor build` proves CONNECTIVITY: a port marked `required` has an
//! edge landing on it. That is not the property the TTL design depends
//! on. `ttl_scheduler.tick_in` marked required only says SOMETHING
//! produces ticks — wire any emitter to it and the check passes, while
//! the state machine's clock is whatever that emitter says rather than
//! whatever the log agreed on.
//!
//! The actual invariant is a SHAPE: a tick this scheduler proposed must
//! come back to it through the same ordering the commands it will be
//! compared against went through, and reach the state worker through
//! that same ordering. Only two shapes satisfy it.
//!
//! **Non-replicated.** The command channel IS the order:
//!
//! ```text
//! ttl_scheduler.propose_out ──▶ kv_state_worker.commands
//!                          └──▶ ttl_scheduler.tick_in
//! ```
//!
//! **Replicated.** Consensus is the order, so the tick is proposed as
//! a client request and comes back committed, demuxed by the bridge:
//!
//! ```text
//! ttl_scheduler.propose_out ──▶ gateway.client_requests ──▶ … raft …
//! lattice_apply_bridge.records_out ──▶ ttl_scheduler.tick_in
//! lattice_apply_bridge.kv_out      ──▶ kv_state_worker.commands
//! ```
//!
//! Anything else — a tick source that is not this scheduler's own
//! proposal, a proposal that reaches the worker without reaching the
//! scheduler, a replicated graph proposing bare ticks that bypass the
//! log — is refused here, with the reason, rather than shipped as a
//! graph that validates and then disagrees with itself about what time
//! it is.

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "clock_lint")]
#[command(about = "Check the committed-clock path in Lattice graph configs")]
struct Args {
    /// A single config to check.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Directory of configs; every `*.yaml` in it is checked. This is
    /// what CI uses (all of `configs/`).
    #[arg(long)]
    config_dir: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Graph {
    #[serde(default)]
    modules: Vec<ModuleDecl>,
    #[serde(default)]
    wiring: Vec<Edge>,
}

#[derive(Deserialize)]
struct ModuleDecl {
    name: String,
    #[serde(default)]
    params: serde_yaml::Value,
}

#[derive(Deserialize)]
struct Edge {
    from: String,
    to: String,
}

/// A graph's clock shape, decided by what it contains rather than by
/// what it claims.
enum Shape {
    /// No consensus in this graph: the command channel is the order.
    Direct,
    /// `lattice_apply_bridge` is present: the log is the order.
    Replicated,
}

struct Findings {
    config: String,
    problems: Vec<String>,
}

impl Graph {
    fn has_module(&self, name: &str) -> bool {
        self.modules.iter().any(|m| m.name == name)
    }

    fn param_u64(&self, module: &str, key: &str) -> Option<u64> {
        let m = self.modules.iter().find(|m| m.name == module)?;
        m.params.get(key)?.as_u64()
    }

    fn has_edge(&self, from: &str, to: &str) -> bool {
        self.wiring.iter().any(|e| e.from == from && e.to == to)
    }

    fn producers_of(&self, to: &str) -> Vec<&str> {
        self.wiring
            .iter()
            .filter(|e| e.to == to)
            .map(|e| e.from.as_str())
            .collect()
    }
}

/// Every rule this tool enforces, stated against one parsed graph.
fn check(graph: &Graph) -> Vec<String> {
    let mut p = Vec::new();

    // Only a graph with a state machine has a clock to get wrong.
    if !graph.has_module("kv_state_worker") {
        return p;
    }
    if !graph.has_module("ttl_scheduler") {
        p.push(
            "a `kv_state_worker` with no `ttl_scheduler`: the state machine's clock never \
             starts, so no TTL ever expires and every record with a deadline outlives it"
                .to_string(),
        );
        return p;
    }

    let shape = if graph.has_module("lattice_apply_bridge") {
        Shape::Replicated
    } else {
        Shape::Direct
    };
    let wrap = graph
        .param_u64("ttl_scheduler", "propose_wrap")
        .unwrap_or(0);

    // ── The tick's round trip ────────────────────────────────────────
    //
    // `tick_in` fed by anything other than this scheduler's own
    // proposal, through the graph's ordering, is the case connectivity
    // validation cannot see.
    let tick_sources = graph.producers_of("ttl_scheduler.tick_in");
    match shape {
        Shape::Direct => {
            if wrap != 0 {
                p.push(
                    "`propose_wrap: 1` in a graph with no `lattice_apply_bridge`: the tick is \
                     wrapped as a consensus proposal that nothing in this graph commits, so the \
                     clock never advances"
                        .to_string(),
                );
            }
            if !graph.has_edge("ttl_scheduler.propose_out", "ttl_scheduler.tick_in") {
                p.push(
                    "no `ttl_scheduler.propose_out -> ttl_scheduler.tick_in`: the scheduler \
                     never learns the time it proposed, so its own expiry queue runs on a clock \
                     the worker does not share"
                        .to_string(),
                );
            }
            if !graph.has_edge("ttl_scheduler.propose_out", "kv_state_worker.commands") {
                p.push(
                    "no `ttl_scheduler.propose_out -> kv_state_worker.commands`: the tick must \
                     arrive IN BAND with the commands it orders against, or a deadline depends \
                     on when a module happened to be scheduled"
                        .to_string(),
                );
            }
            for src in &tick_sources {
                if *src != "ttl_scheduler.propose_out" {
                    p.push(format!(
                        "`{src}` feeds `ttl_scheduler.tick_in`: in a graph with no consensus the \
                         only admissible tick source is the scheduler's own `propose_out`, which \
                         also goes to `kv_state_worker.commands`. A separate producer gives the \
                         two consumers different clocks"
                    ));
                }
            }
        }
        Shape::Replicated => {
            if wrap != 1 {
                p.push(
                    "`propose_wrap` is not 1 in a replicated graph: the tick would be emitted as \
                     a bare envelope that never enters the log, so replicas would apply the same \
                     commands against different clocks"
                        .to_string(),
                );
            }
            if !graph.has_edge("lattice_apply_bridge.records_out", "ttl_scheduler.tick_in") {
                p.push(
                    "no `lattice_apply_bridge.records_out -> ttl_scheduler.tick_in`: the \
                     scheduler must take its time from COMMITTED ticks, not from its own \
                     proposals"
                        .to_string(),
                );
            }
            if !graph.has_edge("lattice_apply_bridge.kv_out", "kv_state_worker.commands") {
                p.push(
                    "no `lattice_apply_bridge.kv_out -> kv_state_worker.commands`: the committed \
                     tick reaches the state machine on this edge, in band with the commands it \
                     orders against"
                        .to_string(),
                );
            }
            let proposes = graph.wiring.iter().any(|e| {
                e.from == "ttl_scheduler.propose_out" && e.to.ends_with(".client_requests")
            });
            if !proposes {
                p.push(
                    "`ttl_scheduler.propose_out` does not reach a `.client_requests` ingress: \
                     nothing proposes the tick, so the log never establishes a time"
                        .to_string(),
                );
            }
            for src in &tick_sources {
                if *src != "lattice_apply_bridge.records_out" {
                    p.push(format!(
                        "`{src}` feeds `ttl_scheduler.tick_in`: in a replicated graph the only \
                         admissible tick source is the commit stream, which is what makes the \
                         value identical on every replica"
                    ));
                }
            }
        }
    }

    // ── The expiry loop ──────────────────────────────────────────────
    if !graph.has_edge("kv_state_worker.expiry_out", "ttl_scheduler.kv_expiry") {
        p.push(
            "no `kv_state_worker.expiry_out -> ttl_scheduler.kv_expiry`: nothing schedules \
             reclamation, so an expired record holds its slot in a bounded table until a \
             command happens to touch it"
                .to_string(),
        );
    }
    if !graph.has_edge("ttl_scheduler.expire_out", "kv_state_worker.expire") {
        p.push(
            "no `ttl_scheduler.expire_out -> kv_state_worker.expire`: the expiry events the \
             queue raises reach nobody, so nothing is ever reclaimed"
                .to_string(),
        );
    }

    // ── The lease clock ──────────────────────────────────────────────
    if graph.has_module("lease_manager")
        && !graph.has_edge("ttl_scheduler.tick_out", "lease_manager.tick")
    {
        p.push(
            "no `ttl_scheduler.tick_out -> lease_manager.tick`: the lease deadline scan runs on \
             that tick and on nothing else, so every lease TTL in this graph is never reached"
                .to_string(),
        );
    }

    p
}

fn check_file(path: &Path) -> Result<Findings> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let graph: Graph =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(Findings {
        config: path.display().to_string(),
        problems: check(&graph),
    })
}

#[expect(
    clippy::disallowed_macros,
    reason = "findings are reported on stdout, the CLI's output contract"
)]
fn report(findings: &[Findings]) -> Result<()> {
    let mut failed = 0usize;
    for f in findings {
        if f.problems.is_empty() {
            continue;
        }
        failed += 1;
        println!("clock_lint: {}", f.config);
        for problem in &f.problems {
            println!("  - {problem}");
        }
    }
    if failed > 0 {
        bail!(
            "clock_lint: {failed} of {} config(s) do not compose an authoritative clock",
            findings.len()
        );
    }
    println!(
        "clock_lint: {} config(s) compose an authoritative clock",
        findings.len()
    );
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    match (args.config, args.config_dir) {
        (Some(file), None) => report(&[check_file(&file)?]),
        (None, Some(dir)) => {
            // Deterministic order so CI output is stable.
            let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
                .with_context(|| format!("reading dir {}", dir.display()))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|ext| ext == "yaml"))
                .collect();
            files.sort();
            if files.is_empty() {
                bail!("no *.yaml configs found in {}", dir.display());
            }
            let findings: Vec<Findings> = files
                .iter()
                .map(|f| check_file(f))
                .collect::<Result<Vec<_>>>()?;
            report(&findings)
        }
        (Some(_), Some(_)) => bail!("pass either --config <file> or --config-dir <dir>, not both"),
        (None, None) => bail!("pass --config <file> or --config-dir <dir>"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(text: &str) -> Graph {
        serde_yaml::from_str(text).expect("parse")
    }

    const DIRECT: &str = r"
modules:
  - name: kv_state_worker
  - name: ttl_scheduler
    params:
      propose_wrap: 0
wiring:
  - from: ttl_scheduler.propose_out
    to: kv_state_worker.commands
  - from: ttl_scheduler.propose_out
    to: ttl_scheduler.tick_in
  - from: kv_state_worker.expiry_out
    to: ttl_scheduler.kv_expiry
  - from: ttl_scheduler.expire_out
    to: kv_state_worker.expire
";

    const REPLICATED: &str = r"
modules:
  - name: kv_state_worker
  - name: lattice_apply_bridge
  - name: gateway
  - name: ttl_scheduler
    params:
      propose_wrap: 1
wiring:
  - from: ttl_scheduler.propose_out
    to: gateway.client_requests
  - from: lattice_apply_bridge.records_out
    to: ttl_scheduler.tick_in
  - from: lattice_apply_bridge.kv_out
    to: kv_state_worker.commands
  - from: kv_state_worker.expiry_out
    to: ttl_scheduler.kv_expiry
  - from: ttl_scheduler.expire_out
    to: kv_state_worker.expire
";

    #[test]
    fn both_supported_shapes_pass() {
        assert!(check(&graph(DIRECT)).is_empty());
        assert!(check(&graph(REPLICATED)).is_empty());
    }

    #[test]
    fn a_graph_with_no_clock_at_all_is_refused() {
        let g = graph(
            r"
modules:
  - name: kv_state_worker
wiring: []
",
        );
        assert_eq!(check(&g).len(), 1);
    }

    #[test]
    fn an_arbitrary_tick_producer_does_not_satisfy_the_invariant() {
        // The case connectivity validation cannot see: `tick_in` has an
        // edge, so `required` is satisfied, but the time on it is not
        // the time the worker applies against.
        let g = graph(&DIRECT.replace(
            "  - from: ttl_scheduler.propose_out\n    to: ttl_scheduler.tick_in",
            "  - from: some_module.out\n    to: ttl_scheduler.tick_in",
        ));
        let problems = check(&g);
        assert!(problems.iter().any(|p| p.contains("some_module.out")));
    }

    #[test]
    fn a_replicated_graph_must_not_propose_bare_ticks() {
        let g = graph(&REPLICATED.replace("propose_wrap: 1", "propose_wrap: 0"));
        assert!(check(&g).iter().any(|p| p.contains("propose_wrap")));
    }

    #[test]
    fn a_lease_manager_needs_the_tick() {
        let g = graph(&DIRECT.replace(
            "  - name: kv_state_worker",
            "  - name: kv_state_worker\n  - name: lease_manager",
        ));
        assert!(check(&g).iter().any(|p| p.contains("lease_manager.tick")));
        let wired = graph(
            &format!("{DIRECT}  - from: ttl_scheduler.tick_out\n    to: lease_manager.tick\n")
                .replace(
                    "  - name: kv_state_worker",
                    "  - name: kv_state_worker\n  - name: lease_manager",
                ),
        );
        assert!(check(&wired).is_empty());
    }

    #[test]
    fn the_reclamation_loop_must_be_closed() {
        let g = graph(&DIRECT.replace(
            "  - from: ttl_scheduler.expire_out\n    to: kv_state_worker.expire",
            "",
        ));
        assert!(check(&g).iter().any(|p| p.contains("expire_out")));
    }

    #[test]
    fn a_graph_with_no_state_worker_has_no_clock_to_check() {
        let g = graph(
            r"
modules:
  - name: watch_registry
wiring: []
",
        );
        assert!(check(&g).is_empty());
    }
}
