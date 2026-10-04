#!/usr/bin/env bash
set -euo pipefail
# Observation-window lane DIRECT source bracket (validation-only): hashes the
# frozen deps + plan + ALL closed predecessors (driver, successor, bridge, DDL
# test, binding, session, prelaunch, catalog-window) + the NEW observation file
# directly. No caller-supplied expected hash is accepted. $3 overrides the
# observation-file path (empty skips it for the before bracket, when it did not
# exist yet).
ROOT=/home/dream/product_env/TxHarbor
cd "$ROOT"
OUT="$1"
LABEL="${2:-}"
NEW_PATH="${3-internal/recovery/borrowed-replacement-owner-commit-ambiguity_linux_test.go}"
FILES=(
  internal/recovery/borrowed-receipt-auth-entry_linux_test.go
  internal/recovery/borrowed-auth-handoff-fixture_linux_test.go
  internal/recovery/borrowed-receipt-auth-entry-probe_linux_testhelper.go
  internal/recovery/borrowed-receipt-auth-entry-probe-fd_linux_test.go
  internal/recovery/borrowed-auth-fixture-mini_linux_test.go
  internal/recovery/borrowed-gate-prefix_linux_test.go
  internal/recovery/targetwriter_receipthandoff_drillbridge_linux_test.go
  internal/recovery/receipt-handoff-mini_linux_test.go
  internal/recovery/receipt-handoff-ambiguity_linux_test.go
  internal/recovery/borrowed-health-adapter_linux_test.go
  internal/recovery/gate.go
  /tmp/opencode/015-e504462/borrowed-replacement-owner-commit-ambiguity-targetlock-before.go
  internal/recovery/targetprocess_linux.go
  internal/recovery/targetwriter_linux.go
  internal/recovery/borrowed-identity-strict-client_linux_testhelper.go
  internal/recovery/borrowed_fixture_identity_linux_test.go
  internal/recovery/origin_gate_proxy_linux_test.go
  internal/recovery/borrowed-auth-staging-helper_linux_test.go
  internal/recovery/borrowed-auth-staging-harness-lifecycle_linux_test.go
)
{
  echo "# owner commit-ambiguity BEFORE direct bracket $LABEL $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for f in "${FILES[@]}"; do sha256sum "$f"; done
  sha256sum internal/recovery/targetwriter_controlanchor_drillbridge_linux_test.go
  sha256sum internal/recovery/borrowed-receipt-auth-drain-driver_linux_test.go
  sha256sum internal/recovery/borrowed-successor-probe-registration_linux_test.go
  sha256sum internal/recovery/borrowed-owner-ddl-feasibility_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-binding-feasibility_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-session-registration_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-prelaunch-feasibility_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-guard-row-window_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-owner-tail-feasibility_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-catalog-window_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-observation-window_linux_test.go
  sha256sum internal/recovery/borrowed-replacement-autonomous-observation_linux_test.go
  if [[ -n "$NEW_PATH" ]]; then
    sha256sum "$NEW_PATH"
  fi
  sha256sum .slim/deepwork/015-owner-rotation-commit-plan.md
} > "$OUT"
echo "wrote $OUT"
