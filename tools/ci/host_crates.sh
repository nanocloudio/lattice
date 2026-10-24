#!/usr/bin/env bash
# The host crates' fmt/clippy/build/test + the wire/ and telemetry/ catalog
# gates and the committed-clock shape gate, AS A CI GATE.
#
# Lattice has no root cargo workspace: the cores live in modules/common and are
# #[path]-mounted, not linked. That shapes what `fluxor ci` covers by itself:
#   * the fmt-check (modules) and clippy (modules) phases see no root
#     Cargo.toml and check the PIC module sources directly.
#   * the `cargo-test (harness)` integration phase compiles and runs
#     tests/harness after the module build. fluxor.toml deliberately sets no
#     `[ci.cargo] host_tools_crate` — that would run the artefact-dependent
#     harness a second time as the unit phase, before any .fmod exists.
#   * nothing builds or lints the standalone tools/* crates, and nothing runs
#     the wire_lint / telemetry_guard / clock_lint checks.
# This script covers that last gap; fluxor.toml runs it from `[ci.test]`.
# Modelled on wave/spectra tools/ci/host_crates.sh.
#
# tests/harness is intentionally NOT clippy'd here: clippy on a crate that
# #[path]-mounts 70+ modules/common sources would lint every mounted file, and
# those warnings belong at their own sites (the way wave/spectra silence theirs
# at the mount), not in a harness-wide gate.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

declare -A PKG=(
  ["tools/ci/wire_lint"]="wire_lint"
  ["tools/ci/telemetry_guard"]="telemetry_guard"
  ["tools/ci/clock_lint"]="clock_lint"
  ["tools/load/lattice-bench"]="lattice-bench"
  ["tools/store_dump"]="lattice-store-dump"
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
  # Unit tests run here rather than in fluxor's unit phase: these crates are
  # artefact-free, so their tests need no module build — unlike tests/harness,
  # which is why fluxor.toml names no `[ci.cargo] host_tools_crate`.
  ( cd "$dir" && cargo test --all-targets --all-features ) >/dev/null 2>&1 \
    || { echo "host_crates: tests FAILED in $c"; fail=1; }
done

# The wire/ and telemetry/ catalog gates.
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
echo "host_crates: ${#PKG[@]} crate(s) fmt/clippy/built/tested, wire_lint + telemetry_guard + clock_lint gates passed"
