#!/usr/bin/env bash
# Shadow-checkout guard (standards/test-tracking.md §7): tests/, benches/
# and examples/ are shadow-tracked (.git-shadow/), so a runner holding
# only the primary repo has zero files there and `cargo test --tests`
# (plus the bench/example builds) would pass vacuously. Hard-fail instead
# of reporting a green gate that ran nothing. Wired as `[ci.test] scripts`
# in fluxor.toml (CI phase 3.5).
set -euo pipefail
# From tools/ci/ the repo root is two levels up (matches host_crates.sh).
cd "$(dirname "$0")/../.."
if [ -z "$(ls -A tests 2>/dev/null)" ]; then
  echo "ci-shadow-guard: tests/ is empty or absent — the shadow-tracked tree" >&2
  echo "is not materialised on this machine (standards/test-tracking.md §7)." >&2
  exit 1
fi
