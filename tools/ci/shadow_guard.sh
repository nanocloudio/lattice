#!/usr/bin/env bash
# Shadow-checkout guard (standards/test-tracking.md): tests/, benches/
# and examples/ are shadow-tracked (.git-shadow/), so a runner holding
# only the primary repo has zero files there and the test phases would
# pass vacuously. Hard-fail instead of reporting a green gate that ran
# nothing. The probe is tests/ alone: it holds the harness every test
# phase runs, and the shadow tree materialises as a whole. Wired as the
# first `[ci.test] scripts` entry in fluxor.toml (the project-e2e phase).
set -euo pipefail
# From tools/ci/ the repo root is two levels up (matches host_crates.sh).
cd "$(dirname "$0")/../.."
if [ -z "$(ls -A tests 2>/dev/null)" ]; then
  echo "ci-shadow-guard: tests/ is empty or absent — the shadow-tracked tree" >&2
  echo "is not materialised on this machine (standards/test-tracking.md)." >&2
  exit 1
fi
