#!/usr/bin/env bash
# Test-only fault injection never ships: `fault_read_eagain_every`
# (kv_state_worker_store, docs/architecture/limit_register.md "Test-only
# fault injection") makes the store answer reads EAGAIN on purpose. A graph
# config outside tests/ that sets it — copied from a test, or left in after
# a debugging session — fails CI here rather than slowing a deployment's
# every read. Test graphs (tests/**) and generated E2E configs (written to
# scratch, never tracked) are where it belongs. Wired as `[ci.test] scripts`
# in fluxor.toml (CI phase 3.5).
set -euo pipefail
cd "$(dirname "$0")/../.."
hits=$(find . \( -path ./tests -o -name target -o -name .git -o -name .git-shadow \) -prune -o \
  -type f \( -name '*.yaml' -o -name '*.yml' -o -name '*.json' \) -print0 |
  xargs -0 -r grep -lE 'fault_read_eagain_every' || true)
if [ -n "$hits" ]; then
  echo "ci-no-fault-injection: a graph outside tests/ sets the test-only fault_read_eagain_every:" >&2
  echo "$hits" >&2
  exit 1
fi
echo "ci-no-fault-injection: no graph outside tests/ injects faults"
