#!/usr/bin/env bash
# Test-only fault injection never ships: `fault_read_eagain_every`,
# `fault_busy_reaches_store` and `fault_log_hold_steps`
# (kv_state_worker_store, docs/architecture/limit_register.md "Test-only
# fault injection") make the store answer reads EAGAIN, or hold its
# writes' log records, on purpose. A graph
# config outside tests/ that sets it — copied from a test, or left in after
# a debugging session — fails CI here rather than slowing a deployment's
# every read. Test graphs (tests/**) and generated E2E configs (written to
# scratch, never tracked) are where it belongs. Wired as `[ci.test] scripts`
# in fluxor.toml (CI phase 3.5).
set -euo pipefail
cd "$(dirname "$0")/../.."
hits=$(find . \( -path ./tests -o -name target -o -name .git -o -name .git-shadow \) -prune -o \
  -type f \( -name '*.yaml' -o -name '*.yml' -o -name '*.json' \) -print0 |
  xargs -0 -r grep -lE 'fault_(read_eagain_every|busy_reaches_store|log_hold_steps)' || true)
if [ -n "$hits" ]; then
  echo "ci-no-fault-injection: a graph outside tests/ sets a test-only fault knob (fault_read_eagain_every, fault_busy_reaches_store, fault_log_hold_steps):" >&2
  echo "$hits" >&2
  exit 1
fi
echo "ci-no-fault-injection: no graph outside tests/ injects faults"
