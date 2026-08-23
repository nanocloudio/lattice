#!/usr/bin/env bash
# The host crates' fmt/clippy/build + the wire/ and telemetry/ catalog gates,
# AS A CI GATE.
#
# Lattice has no root cargo workspace (retired along with crates/lattice-common;
# the cores live in modules/common and are #[path]-mounted, not linked). That
# changes what `fluxor ci` runs:
#   * phases 1.1/1.2 see no root Cargo.toml and fmt-check + clippy the PIC
#     module sources directly — a gain, since modules/** was linted by neither.
#   * phase 4's built-in cargo-test (harness) covers tests/harness (via
#     [ci.cargo] host_tools_crate), so the suites run — but the standalone
#     tools/* crates would be built and linted by nothing, and the wire_lint /
#     telemetry_guard / clock_lint checks would never run as gates (they used to
#     run ONLY from the old hand-written Makefile lint recipe).
# This closes both holes. Modelled on wave/spectra tools/ci/host_crates.sh.
#
# tests/harness is intentionally NOT clippy'd here: clippy on a crate that
# #[path]-mounts 70+ modules/common sources would lint every mounted file, which
# is a separate triage. Phase 4 compiles and runs the harness suites; a
# follow-up can add a scoped clippy once mounted-source warnings are addressed
# at their own sites (the way wave/spectra silence theirs at the mount).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

declare -A PKG=(
  ["tools/ci/wire_lint"]="wire_lint"
  ["tools/ci/telemetry_guard"]="telemetry_guard"
  ["tools/ci/clock_lint"]="clock_lint"
  ["tools/load/lattice-bench"]="lattice-bench"
)

fail=0
for c in "${!PKG[@]}"; do
  dir="$ROOT/$c"
  if [ ! -f "$dir/Cargo.toml" ]; then
    echo "host_crates: MISSING $c/Cargo.toml"; fail=1; continue
  fi
  ( cd "$dir" && cargo fmt -p "${PKG[$c]}" -- --check ) \
    || { echo "host_crates: fmt FAILED in $c"; fail=1; }
  ( cd "$dir" && cargo clippy --all-targets --all-features -- -D warnings ) \
    || { echo "host_crates: clippy FAILED in $c"; fail=1; }
  ( cd "$dir" && cargo build --all-targets ) >/dev/null 2>&1 \
    || { echo "host_crates: build FAILED in $c"; fail=1; }
  # Unit tests here (not a cargo `[ci.cargo]` Phase 2): these crates are
  # artefact-free, so their tests run before the module build without issue —
  # unlike tests/harness, which is why Phase 2 is omitted (see fluxor.toml).
  ( cd "$dir" && cargo test --all-targets --all-features ) >/dev/null 2>&1 \
    || { echo "host_crates: tests FAILED in $c"; fail=1; }
done

# The wire/ and telemetry/ catalog gates — previously only reachable via the
# hand-written Makefile, now real `fluxor ci` gates.
( cd "$ROOT/tools/ci/wire_lint" && cargo run -q -- --wire-dir "$ROOT/wire" ) \
  || { echo "host_crates: wire_lint gate FAILED"; fail=1; }
( cd "$ROOT/tools/ci/telemetry_guard" && cargo run -q -- --catalog "$ROOT/telemetry/catalog.json" ) \
  || { echo "host_crates: telemetry_guard gate FAILED"; fail=1; }
# The committed-clock shape gate. `fluxor build` proves each required port has
# an edge; this proves the edges COMPOSE an authoritative clock — see the tool's
# module doc for the two admissible shapes and why connectivity is not enough.
( cd "$ROOT/tools/ci/clock_lint" && cargo run -q -- --config-dir "$ROOT/configs" ) \
  || { echo "host_crates: clock_lint gate FAILED"; fail=1; }

if [ "$fail" -ne 0 ]; then
  echo "host_crates: FAILED"
  exit 1
fi
echo "host_crates: 4 crate(s) fmt/clippy/built/tested, wire_lint + telemetry_guard + clock_lint gates passed"
