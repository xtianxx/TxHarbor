#!/usr/bin/env bash
# TxHarbor Quick Start cold-start smoke test.
#
# Verifies the documented Quick Start flow end to end in a fresh, isolated
# environment:
#   1. docker compose up        -> brand-new PostgreSQL + Anvil (wait for healthy)
#   2. txharbor migrate up      -> apply all migrations
#   3. txharbor migrate status  -> current_version / pending
#   4. txharbor serve           -> /livez, /readyz, /metrics
#   5. negative case            -> serve refuses to start without
#                                  TXHARBOR_REORG_MAX_DEPTH (fail-closed)
#   6. shutdown                 -> graceful stop (exit 0) and full teardown
#
# Isolation guarantees (the caller's environment and other runs are never touched):
#   - a per-run Compose project (txharbor-smoke-<timestamp>-<pid>) with its own
#     project-scoped volume; teardown only ever touches this run's project and
#     only after this run has started Compose;
#   - dedicated host ports: PG 55432, Anvil 58545, HTTP 18080
#     (override with SMOKE_PG_PORT / SMOKE_ANVIL_PORT / SMOKE_HTTP_PORT — the
#     Compose override consumes the same variables);
#   - all inherited TXHARBOR_* variables are dropped before loading
#     .env.example, so a running development stack or a local .env cannot
#     leak into the run;
#   - the binary is built into the evidence directory (no repo pollution).
#
# Usage: bash scripts/quickstart-smoke/run_smoke.sh [evidence-dir]
# Exit:  0 = PASS, non-zero = first failing step; see SUMMARY.txt in the
#        evidence directory (also holds logs, probes and compose output).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
project="txharbor-smoke-${run_id}"
pg_port="${SMOKE_PG_PORT:-55432}"
anvil_port="${SMOKE_ANVIL_PORT:-58545}"
http_port="${SMOKE_HTTP_PORT:-18080}"

if [ -n "${1:-}" ]; then
  evidence_dir="$1"
else
  evidence_dir="${SMOKE_EVIDENCE_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/txharbor-smoke.XXXXXX")}"
fi
mkdir -p "$evidence_dir"
evidence_dir="$(cd "$evidence_dir" && pwd)"

compose=(docker compose -p "$project" -f "$repo_root/compose.yaml" -f "$repo_root/scripts/quickstart-smoke/compose.smoke.yaml")
summary="$evidence_dir/SUMMARY.txt"
serve_pid=""
stack_started=0
fail_reason=""

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$summary"; }
fail() { fail_reason="$*"; log "FAIL: $*"; exit 1; }

cleanup() {
  rc=$?
  if [ -n "$serve_pid" ] && kill -0 "$serve_pid" 2>/dev/null; then
    kill -TERM "$serve_pid" 2>/dev/null || true
    wait "$serve_pid" 2>/dev/null || true
  fi
  if [ "$stack_started" -eq 1 ]; then
    "${compose[@]}" logs --no-color >"$evidence_dir/compose-logs.txt" 2>&1 || true
    if ! "${compose[@]}" down -v --remove-orphans >"$evidence_dir/compose-down.log" 2>&1; then
      log "FAIL: compose teardown failed (see compose-down.log); project $project may need manual cleanup"
      if [ "$rc" -eq 0 ]; then rc=1; fi
    fi
  fi
  if [ "$rc" -eq 0 ]; then
    log "RESULT: PASS"
  else
    log "RESULT: FAIL (exit $rc${fail_reason:+; $fail_reason})"
  fi
  # The EXIT trap's own exit status is otherwise discarded; propagate explicitly
  # so a teardown failure makes the script (and `make smoke-quickstart`) fail.
  exit "$rc"
}
trap cleanup EXIT

cd "$repo_root"

# The Compose override consumes these same variables for its port mappings.
export SMOKE_PG_PORT="$pg_port" SMOKE_ANVIL_PORT="$anvil_port"

log "repo=$repo_root project=$project evidence=$evidence_dir"
log "isolation: per-run project=$project ports pg=$pg_port anvil=$anvil_port http=$http_port"

# 1) Preflight: tools present, isolation ports free.
command -v docker >/dev/null || fail "docker not found"
docker compose version >"$evidence_dir/compose-version.txt" 2>&1 || fail "docker compose unavailable"
port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for p in "$pg_port" "$anvil_port" "$http_port"; do
  if port_busy "$p"; then fail "port $p already in use; set SMOKE_PG_PORT/SMOKE_ANVIL_PORT/SMOKE_HTTP_PORT"; fi
done
log "preflight ok: ports $pg_port/$anvil_port/$http_port free"

# 2) Environment: .env.example only, caller TXHARBOR_* dropped, ports overridden.
while IFS= read -r key; do unset "$key"; done < <(env | sed -n 's/^\(TXHARBOR_[A-Z0-9_]*\)=.*/\1/p')
cp "$repo_root/.env.example" "$evidence_dir/quickstart.env"
set -a; . "$evidence_dir/quickstart.env"; set +a
export TXHARBOR_PG_DSN="postgres://txharbor:txharbor@127.0.0.1:${pg_port}/txharbor?sslmode=disable"
export TXHARBOR_RPC_URL="http://127.0.0.1:${anvil_port}"
export TXHARBOR_HTTP_ADDR="127.0.0.1:${http_port}"
log "environment prepared from .env.example (template copy at evidence/quickstart.env)"

# 3) Build into the evidence dir.
go build -o "$evidence_dir/txharbor" ./cmd/txharbor >"$evidence_dir/build.log" 2>&1 || fail "go build failed"
log "build ok"

# 4) Start the isolated stack and prove the project name is scoped.
"${compose[@]}" config >"$evidence_dir/compose-config.yaml"
name="$("${compose[@]}" config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin).get("name",""))')"
[ "$name" = "$project" ] || fail "compose project resolved to '$name', expected '$project'"
stack_started=1
"${compose[@]}" up -d --wait --wait-timeout 180 >"$evidence_dir/compose-up.log" 2>&1 || fail "compose up failed"
"${compose[@]}" ps >"$evidence_dir/compose-ps.txt" 2>&1 || true
log "compose up ok: project=$name (containers: $(docker ps --filter "label=com.docker.compose.project=$project" --format '{{.Names}}' | tr '\n' ' '))"

# 5) Negative case: without the required reorg depth, serve must refuse to start.
set +e
env -u TXHARBOR_REORG_MAX_DEPTH "$evidence_dir/txharbor" serve >"$evidence_dir/serve-missing-reorg-depth.log" 2>&1
missing_rc=$?
set -e
[ "$missing_rc" -ne 0 ] || fail "serve started although TXHARBOR_REORG_MAX_DEPTH was unset"
grep -q 'TXHARBOR_REORG_MAX_DEPTH' "$evidence_dir/serve-missing-reorg-depth.log" \
  || fail "refusal did not name TXHARBOR_REORG_MAX_DEPTH"
log "negative case ok: serve refused without TXHARBOR_REORG_MAX_DEPTH (exit $missing_rc)"

# 6) Migrations.
"$evidence_dir/txharbor" migrate up >"$evidence_dir/migrate-up.log" 2>&1 || fail "migrate up failed"
"$evidence_dir/txharbor" migrate status >"$evidence_dir/migrate-status.log" 2>&1 || fail "migrate status failed"
grep -Eq 'current_version=[0-9]+' "$evidence_dir/migrate-status.log" || fail "migrate status lacks current_version"
grep -q 'pending=none' "$evidence_dir/migrate-status.log" || fail "migrate status reports pending migrations"
log "migrations ok: $(tr '\n' ' ' <"$evidence_dir/migrate-status.log")"

# 7) Serve and probe the documented endpoints.
"$evidence_dir/txharbor" serve >"$evidence_dir/serve.log" 2>&1 &
serve_pid=$!
ready=0
for _ in $(seq 1 60); do
  if curl -fsS --max-time 5 "http://127.0.0.1:${http_port}/readyz" >"$evidence_dir/readyz.json" 2>/dev/null; then ready=1; break; fi
  kill -0 "$serve_pid" 2>/dev/null || break
  sleep 1
done
[ "$ready" -eq 1 ] || fail "serve did not become ready (see serve.log)"
curl -fsS --max-time 5 "http://127.0.0.1:${http_port}/livez" >"$evidence_dir/livez.json" 2>/dev/null || fail "/livez probe failed"
curl -fsS --max-time 5 "http://127.0.0.1:${http_port}/metrics" >"$evidence_dir/metrics.txt" 2>/dev/null || fail "/metrics probe failed"
grep -q '"status":"alive"' "$evidence_dir/livez.json" || fail "/livez did not report alive"
grep -q '"status":"ready"' "$evidence_dir/readyz.json" || fail "/readyz did not report ready"
metrics_count="$(grep -c '^txharbor_' "$evidence_dir/metrics.txt" || true)"
[ "$metrics_count" -gt 0 ] || fail "/metrics exposed no txharbor_* series"
log "serve ok: /livez alive, /readyz ready, metrics series=$metrics_count"

# 8) Graceful shutdown: a non-zero exit is a smoke failure.
kill -TERM "$serve_pid"
set +e
wait "$serve_pid"
serve_rc=$?
set -e
serve_pid=""
tail -n 3 "$evidence_dir/serve.log" >>"$summary" 2>/dev/null || true
[ "$serve_rc" -eq 0 ] || fail "serve exited with $serve_rc on SIGTERM (expected 0)"
log "serve stopped gracefully (exit $serve_rc)"
