#!/usr/bin/env bash
# Test-only fault injection never ships: `fault_read_eagain_every`,
# `fault_busy_reaches_store`, `fault_log_hold_steps` (kv_state_worker_store)
# and `fault_slow_write_ms`/`fault_slow_write_every` (every state worker;
# docs/architecture/limit_register.md "Test-only fault injection") make the
# store answer reads EAGAIN, hold its writes' log records, or keep its
# writes waiting as a slow device, on purpose. A graph config outside tests/
# that sets any of them — copied from a test, or left in after a debugging
# session — fails CI here rather than slowing a deployment's reads and
# writes. Test graphs (tests/**) and generated E2E configs (written to
# scratch, never tracked) are where they belong. Scans every YAML and JSON
# file outside tests/, target/ and the git directories. Wired as a
# `[ci.test] scripts` gate in fluxor.toml (fluxor ci's project-e2e phase).
set -euo pipefail
cd "$(dirname "$0")/../.."
hits=$(find . \( -path ./tests -o -name target -o -name .git -o -name .git-shadow \) -prune -o \
  -type f \( -name '*.yaml' -o -name '*.yml' -o -name '*.json' \) -print0 |
  xargs -0 -r grep -lE 'fault_(read_eagain_every|busy_reaches_store|log_hold_steps|slow_write_ms|slow_write_every)' || true)
if [ -n "$hits" ]; then
  echo "ci-no-fault-injection: a graph outside tests/ sets a test-only fault knob (fault_read_eagain_every, fault_busy_reaches_store, fault_log_hold_steps, fault_slow_write_ms/every):" >&2
  echo "$hits" >&2
  exit 1
fi
echo "ci-no-fault-injection: no graph outside tests/ injects faults"
