# Tools

Host-side helpers that live alongside the PIC module tree but never run on
`fluxor`. One directory per role: a file is named for what it is, its directory
for what it is *for* — the same convention `../wave/tools/` uses.

The language split is not arbitrary. Shell orchestrates a gate (check a
condition, fail loudly). Rust holds the tools that parse structured input — the
wire and telemetry catalogs, the load generator's codecs — where a real parser
earns its keep.

| Directory | Role |
| --- | --- |
| `ci/` | Gates the `fluxor ci` / `make lint` phases run — the checks that must pass |
| `load/` | Throughput: the off-DUT open-loop load generator |

## `ci/`

| Tool | Gate |
| --- | --- |
| `shadow_guard.sh` | Hard-fails when the shadow checkout is missing, so CI cannot run zero tests and report green (`../standards/test-tracking.md` §7). Wired as the `[ci.test]` script in `fluxor.toml`. |
| `wire_lint/` | Compares the local wire catalog (`wire/`) against Clustor artefacts so deviations from the substrate's envelope vocabulary are caught at lint time, not at runtime. Runs in `make lint` (`cargo run -p wire_lint -- --wire-dir wire/`). |
| `telemetry_guard/` | Sanity-checks the telemetry catalog JSON for shape. A spec-lint gate. |

Each Rust gate is a workspace member (see `[workspace] members`) and carries a
relaxed `[lints]` posture recorded as a `[[ci.lints.exemption]]` in `fluxor.toml`
— they print to stdout and unwrap on I/O, so they opt out of the lib-grade
workspace lints rather than pretend to meet them.

## `load/`

`lattice-bench/` is the off-DUT benchmark harness (`lattice-loadgen` +
`lattice-scrape`). It runs on the driver host, never on `fluxor`, and is
dependency-free by design so it builds on an offline Pi — networking is raw
`std::net`, and the JSON, RESP client, and latency histogram are hand-rolled.
Like `wave`'s `load/wave-bench`, it shares **no code** with the database it
drives: a load test built from the DUT's own parser cannot fail on a shared
codec defect, because the bug cancels out on both sides and the run goes green.

The rate ladders that drive it (`run_l1.sh`, `run_l2_3node.sh`,
`run_l4_xmachine.sh`) currently live under `../perf/`; consolidating them here as
a `load/suite.sh` — the shape `wave/tools/load/` has — is a natural follow-on.
