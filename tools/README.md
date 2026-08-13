# Tools

Host-side helpers that live alongside the PIC module tree but never run on
`fluxor`. One directory per role: a file is named for what it is, its directory
for what it is *for* — the same convention `../wave/tools/` uses.

The language split is deliberate. Shell orchestrates a gate or a scenario (check
a condition, fail loudly). Rust holds the tools that parse structured input —
the wire and telemetry catalogs, the load generator's codecs — where a real
parser earns its keep. Python drives the end-to-end conformance scenarios that
speak a wire format directly (MQTT, RESP), where a short script with `struct`
is the whole tool.

| Directory | Role |
| --- | --- |
| `ci/` | Gates `fluxor ci` runs — the checks that must pass |
| `e2e/` | End-to-end drivers that exercise a running node and assert the effect |
| `load/` | Throughput: the off-DUT open-loop load generator |

## `ci/`

| Tool | Gate |
| --- | --- |
| `shadow_guard.sh` | Hard-fails when the shadow checkout is missing, so CI cannot run zero tests and report green (`../standards/test-tracking.md` §7). One of the two `[ci.test]` scripts in `fluxor.toml`. |
| `host_crates.sh` | fmt-checks, clippies, and builds the three standalone host crates, then runs the `wire_lint` and `telemetry_guard` gates. The other `[ci.test]` script: with no root workspace, nothing else builds or lints these crates. |
| `wire_lint/` | Compares the local wire catalog (`wire/`) against Clustor artefacts so deviations from the substrate's envelope vocabulary are caught at lint time, not at runtime. Run by `host_crates.sh`. |
| `telemetry_guard/` | Sanity-checks the telemetry catalog (`telemetry/catalog.json`) for shape. Run by `host_crates.sh`. |

Lattice has no root cargo manifest, so each Rust gate is a **standalone crate**
with its own `[workspace]`. They print to stdout and unwrap on I/O, so each
carries its own relaxed `[lints]` rather than opting into lib-grade ones —
`host_crates.sh` is what compiles and lints them, since without a workspace
nothing else would.

## `e2e/`

Drivers that stand up or attach to a running node and assert an observable
effect end to end. They take a target address (a DUT on the rig or a local
node), so they are ordinary primary-repo tools, not shadow-tracked test source.

| Tool | What it proves |
| --- | --- |
| `cdc_rig_validate.py` | The CDC egress path end to end: subscribes to the CDC topic on the quantum broker (MQTT 3.1.1, QoS 1), drives committed writes into the model RESP surface, and asserts the received envelopes — identity fields, monotone commit timestamps in apply order, the delete event, and resolved watermarks. The envelope layout mirrors `modules/common/cdc_wire.rs`. |
| `live_clients.sh` | Each database client works standalone against a real server: it starts the backend, runs the client graph, and asserts the observed effect, per protocol (`redis`, `pg`, …). |

## `load/`

`lattice-bench/` is the off-DUT benchmark harness (`lattice-loadgen` +
`lattice-scrape`). It runs on the driver host, never on `fluxor`, and is
dependency-free by design so it builds on an offline Pi — networking is raw
`std::net`, and the JSON, RESP client, and latency histogram are hand-rolled.
Like `wave`'s `load/wave-bench`, it shares **no code** with the database it
drives: a load test built from the DUT's own parser cannot fail on a shared
codec defect, because the bug cancels out on both sides and the run goes green.

The rate ladders that drive it (`run_l1.sh`, `run_l2_3node.sh`,
`run_l4_xmachine.sh`) live under the shadow-tracked `../perf/`; consolidating
them here as a `load/suite.sh` — the shape `wave/tools/load/` has — is a natural
follow-on.
