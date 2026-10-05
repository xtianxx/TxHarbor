#!/usr/bin/env bash
set -euo pipefail
# Observation-window lane race build (validation-only): builds the -race binary
# on the host and records the DIRECT measured hashes of the NEW observation
# file at build time plus ALL closed predecessors (catalog-window, prelaunch,
# session, binding, driver, successor, bridge, DDL test). No caller-supplied
# hash is accepted and no old log is retrofitted.
ROOT=/home/dream/product_env/TxHarbor
BIN=/tmp/opencode/015-e504462/borrowed-replacement-owner-commit-ambiguity-fix3-race.test
LOG=/tmp/opencode/015-e504462/borrowed-replacement-owner-commit-ambiguity-fix3-race-build.log
if [[ -e "$LOG" ]]; then
  echo "refusing to overwrite existing build log: $LOG" >&2
  exit 3
fi
cd "$ROOT"
{
  echo "# owner commit-ambiguity fix race binary provenance build (direct seam/companion/journal measurement) $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# command: go test -c -tags drill -race -o $BIN ./internal/recovery/"
  echo "# go version: $(go version)"
  echo "# commit-ambiguity: $(sha256sum internal/recovery/borrowed-replacement-owner-commit-ambiguity_linux_test.go)"
  echo "# commit-ambiguity-companion: $(sha256sum internal/recovery/targetlock_commitambiguity_hook_linux_test.go)"
  echo "# targetlock-seam: $(sha256sum internal/recovery/targetlock.go)"
  echo "# owner-journal-window: $(sha256sum internal/recovery/borrowed-replacement-owner-journal-window_linux_test.go)"
  echo "# guard-row-window: $(sha256sum internal/recovery/borrowed-replacement-guard-row-window_linux_test.go)"
  echo "# owner-journal-window: $(sha256sum internal/recovery/borrowed-replacement-owner-journal-window_linux_test.go)"
  echo "# owner-tail: $(sha256sum internal/recovery/borrowed-replacement-owner-tail-feasibility_linux_test.go)"
  echo "# autonomous-observation: $(sha256sum internal/recovery/borrowed-replacement-autonomous-observation_linux_test.go)"
  echo "# observation-window: $(sha256sum internal/recovery/borrowed-replacement-observation-window_linux_test.go)"
  echo "# catalog-window: $(sha256sum internal/recovery/borrowed-replacement-catalog-window_linux_test.go)"
  echo "# prelaunch: $(sha256sum internal/recovery/borrowed-replacement-prelaunch-feasibility_linux_test.go)"
  echo "# session: $(sha256sum internal/recovery/borrowed-replacement-session-registration_linux_test.go)"
  echo "# binding: $(sha256sum internal/recovery/borrowed-replacement-binding-feasibility_linux_test.go)"
  echo "# driver: $(sha256sum internal/recovery/borrowed-receipt-auth-drain-driver_linux_test.go)"
  echo "# successor: $(sha256sum internal/recovery/borrowed-successor-probe-registration_linux_test.go)"
  echo "# bridge: $(sha256sum internal/recovery/targetwriter_controlanchor_drillbridge_linux_test.go)"
  echo "# ddl-test: $(sha256sum internal/recovery/borrowed-owner-ddl-feasibility_linux_test.go)"
} > "$LOG"
CGO_ENABLED=1 go test -c -tags drill -race -o "$BIN" ./internal/recovery/ 2>&1 | tee -a "$LOG"
STATUS=${PIPESTATUS[0]}
echo "exit=$STATUS" | tee -a "$LOG"
if [[ "$STATUS" -eq 0 ]]; then
  sha256sum "$BIN" | tee -a "$LOG"
fi
exit "$STATUS"
