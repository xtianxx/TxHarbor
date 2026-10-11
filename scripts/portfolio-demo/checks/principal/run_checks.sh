#!/usr/bin/env bash
# Principal-fix regression suite (pure shell mocks; NO daemon, NO CLI, NO
# PostgreSQL, NO network).
#
# What it proves, without running the demo:
#   1. the principal guard refuses the old misconfiguration (API/grant caller
#      7001 vs signer principal 8001) with zero side effects;
#   2. with the fixed wiring the guard passes AND the REAL credentials block
#      extracted from run_demo.sh issues/sets-can-sign caller 7001 (asserted
#      from the argv the stub CLI actually received), with the issuer 9001
#      kept separate;
#   3. the API key and the signer credential stay two separate 0600 files with
#      distinct dummy values, and only the signer credential is handed to the
#      worker;
#   4. the grant-owner read-back fails closed on a missing or foreign caller id
#      and is a read-only check (no INSERT/UPDATE/DELETE, no second write).
#
# The suite executes code extracted from the real script (the credentials block
# between the BEGIN/END markers) instead of grepping for variable names.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
DEMO_DIR=$(cd "$HERE/../.." && pwd)
RUN_DEMO="$DEMO_DIR/run_demo.sh"
LIB="$DEMO_DIR/lib.sh"

[[ -f "$RUN_DEMO" ]] || { echo "FAIL: missing $RUN_DEMO" >&2; exit 1; }
[[ -f "$LIB" ]] || { echo "FAIL: missing $LIB" >&2; exit 1; }

# shellcheck source=../../lib.sh
. "$LIB"

WORK=$(mktemp -d /tmp/principal-checks.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
REC="$WORK/cli.argv"
DOCKER_REC="$WORK/docker.calls"

pass_count=0
fail_count=0
ok()   { pass_count=$((pass_count + 1)); printf 'ok: %s\n' "$1"; }
bad()  { fail_count=$((fail_count + 1)); printf 'FAIL: %s\n' "$1" >&2; }

# --- stubs: any daemon/CLI contact is recorded and refused ------------------
docker() { printf 'docker %s\n' "$*" >> "$DOCKER_REC"; return 99; }
cli_secret() {
  local label=$1
  shift
  [[ "${1:-}" == "${BIN:-}" ]] && shift
  printf '%s\n' "$*" >> "$REC"
  printf '%s\n' "DUMMY-KEY-$label-NOT-A-REAL-SECRET"
}
cli_run() {
  local label=$1
  shift
  [[ "${1:-}" == "${BIN:-}" ]] && shift
  printf '%s\n' "$*" >> "$REC"
  printf '%s\n' "op-0123456789abcdef0123456789abcdef"
}

# --- extraction from the real script ---------------------------------------
# The identity constants (plus AUTH_ID, which the grant read-back snippet
# references), evaluated in declaration order so that SIGNER_CALLER_ID="$CALLER_ID"
# resolves exactly as the script declares it.
extract_constants() {
  awk -F= '/^(CALLER_ID|ISSUER_CALLER_ID|SIGNER_CALLER_ID|AUTH_ID)=/ {print}' "$RUN_DEMO"
}

# The credentials block, verbatim, between the documented markers.
extract_credentials_block() {
  awk '/^  # BEGIN demo-credentials-block$/ {inblock=1; next}
       /^  # END demo-credentials-block$/   {inblock=0}
       inblock' "$RUN_DEMO"
}

# The grant read-back snippet (read-only verification after the supply call).
extract_grant_check() {
  awk '/^  GRANT_CALLER=\$\(psql_ro/ {found=1}
       found {print}
       found && /^  log "ok: grant owner/ {exit}' "$RUN_DEMO"
}

# The real worker-start stanza, verbatim: its credential argument is what the
# worker authenticates with, so the suite executes it (not just greps it).
extract_worker_stanza() {
  awk '/^  start_detached "\$ENV_WORKER"/ {found=1}
       found {print}
       found && /cannot start withdrawal-worker/ {exit}' "$RUN_DEMO"
}

# ===========================================================================
# Case 1 — the old misconfiguration is refused with zero side effects.
# ===========================================================================
rm -f "$REC" "$DOCKER_REC"
if require_signer_principal 7001 8001 9001 2>/dev/null; then
  bad "case1: guard accepted the old 7001/8001 mismatch"
else
  ok "case1: guard refused the old signer principal 8001"
fi
[[ ! -f "$REC" ]] || bad "case1: a CLI was invoked before/around the refusal"
[[ ! -f "$DOCKER_REC" ]] || bad "case1: docker was invoked around the refusal"
[[ ! -f "$WORK/evidence" ]] || bad "case1: an evidence path was created"
[[ $fail_count -eq 0 ]] && ok "case1: zero CLI/docker/evidence side effects"

# ===========================================================================
# Case 2 — the fixed wiring passes and the REAL block issues caller 7001.
# ===========================================================================
constants=$(extract_constants)
[[ "$(printf '%s\n' "$constants" | wc -l)" -eq 4 ]] \
  || bad "case2: expected 4 constants (3 identities + AUTH_ID), got [$(printf '%s\n' "$constants" | tr '\n' ' ')]"
eval "$constants"
if require_signer_principal "$CALLER_ID" "$SIGNER_CALLER_ID" "$ISSUER_CALLER_ID"; then
  ok "case2: guard passes with caller=$CALLER_ID signer=$SIGNER_CALLER_ID issuer=$ISSUER_CALLER_ID"
else
  bad "case2: guard refused the fixed wiring"
fi
[[ "$SIGNER_CALLER_ID" == "$CALLER_ID" ]] || bad "case2: SIGNER_CALLER_ID != CALLER_ID"
[[ "$ISSUER_CALLER_ID" != "$CALLER_ID" ]] || bad "case2: issuer must stay separate"

# Execute the extracted block for real, with stub CLI + real file writes.
mkdir -p "$WORK/keys" "$WORK/logs"
KEY_CALLER="$WORK/keys/caller.key"
KEY_ISSUER="$WORK/keys/issuer.key"
KEY_SIGNER_CRED="$WORK/keys/signer.cred"
BIN="/nonexistent/stub-txharbor"
rm -f "$REC"
eval "$(extract_credentials_block)"
grep -qx -- 'signer-auth issue --caller-id 7001 --label demo-signer --operator op --reason demo' "$REC" \
  && ok "case2: real block issued the signer credential for caller 7001" \
  || bad "case2: signer-auth issue argv not 7001: [$(grep signer-auth "$REC" | tr '\n' ' ')]"
grep -qx -- 'signer-auth set-can-sign --caller-id 7001 --can-sign true --operator op --reason demo' "$REC" \
  && ok "case2: real block set can-sign for caller 7001" \
  || bad "case2: signer-auth set-can-sign argv not 7001"
grep -qx -- 'apikey-auth issue --caller-id 7001 --label demo-caller --operator op --reason demo' "$REC" \
  && ok "case2: API caller key issued for 7001" \
  || bad "case2: apikey-auth issue caller argv not 7001"
grep -qx -- 'apikey-auth issue --caller-id 9001 --label demo-issuer --operator op --reason demo' "$REC" \
  && ok "case2: issuer key issued for the separate identity 9001" \
  || bad "case2: issuer argv not 9001"
grep -q -- '--caller-id 8001' "$REC" && bad "case2: a CLI call still used the retired principal 8001" \
  || ok "case2: no CLI call uses 8001"

# ===========================================================================
# Case 3 — two separate secrets, two 0600 files, worker gets only the signer.
# ===========================================================================
[[ "$KEY_CALLER" != "$KEY_ISSUER" && "$KEY_SIGNER_CRED" != "$KEY_CALLER" && "$KEY_SIGNER_CRED" != "$KEY_ISSUER" ]] \
  && ok "case3: caller/issuer/signer credentials live in three distinct files" \
  || bad "case3: credential files are not distinct"
for f in "$KEY_CALLER" "$KEY_ISSUER" "$KEY_SIGNER_CRED"; do
  [[ -f "$f" ]] || bad "case3: missing credential file $f"
  [[ "$(stat -c '%a' "$f")" == "600" ]] || bad "case3: $f is not 0600"
done
ok "case3: all three credential files exist with mode 0600"
[[ "$(cat "$KEY_CALLER")" != "$(cat "$KEY_SIGNER_CRED")" && "$(cat "$KEY_ISSUER")" != "$(cat "$KEY_SIGNER_CRED")" ]] \
  && ok "case3: dummy credential values are distinct (no secret reuse)" \
  || bad "case3: credential values are identical"
# The real worker-start stanza is EXECUTED against a start_detached stub: the
# credential handed to the worker must be the signer credential file itself,
# never an API key (substituting KEY_ISSUER here must fail the suite).
START_REC="$WORK/worker.start"
ENV_WORKER="$WORK/env/worker.env"
PRIVATE_DIR="$WORK/logs"
mkdir -p "$WORK/env" "$PRIVATE_DIR"
start_detached() { printf '%s\n' "$*" >> "$START_REC"; STARTED_PID=4242; }
worker_stanza=$(extract_worker_stanza)
[[ -n "$worker_stanza" ]] || bad "case3: worker-start stanza not found in run_demo.sh"
rm -f "$START_REC"
eval "$worker_stanza"
if [[ -f "$START_REC" ]] \
   && grep -qF -- "TXHARBOR_TX_SIGNER_CREDENTIAL $KEY_SIGNER_CRED -- $BIN withdrawal-worker" "$START_REC"; then
  ok "case3: real worker start injects the signer credential file + withdrawal-worker"
else
  bad "case3: worker start argv is not [TXHARBOR_TX_SIGNER_CREDENTIAL $KEY_SIGNER_CRED -- $BIN withdrawal-worker]: [$(cat "$START_REC" 2>/dev/null | tr '\n' ' ')]"
fi
if grep -qF -- "$KEY_CALLER" "$START_REC" || grep -qF -- "$KEY_ISSUER" "$START_REC"; then
  bad "case3: an API key file is passed to the worker start"
else
  ok "case3: no API key file (caller/issuer) is passed to the worker start"
fi
grep -qF -- "$ENV_WORKER" "$START_REC" \
  && ok "case3: the worker starts from its own 0600 env file" \
  || bad "case3: the worker start does not use the worker env file"

# ===========================================================================
# Case 4 — the grant read-back fails closed and stays read-only.
# ===========================================================================
require_grant_principal "" 7001 2>/dev/null && bad "case4: empty grant caller accepted" \
  || ok "case4: missing grant caller refused (fail closed)"
require_grant_principal 8001 7001 2>/dev/null && bad "case4: foreign grant caller accepted" \
  || ok "case4: foreign grant caller refused (fail closed)"
require_grant_principal 7001 7001 2>/dev/null && ok "case4: matching grant caller accepted" \
  || bad "case4: matching grant caller refused"

grant_check=$(extract_grant_check)
[[ -n "$grant_check" ]] || bad "case4: grant read-back snippet not found in run_demo.sh"
grep -q 'SELECT caller_id FROM withdrawal_authorizations' <<<"$grant_check" \
  && ok "case4: the check reads the durable grant owner" \
  || bad "case4: grant read-back does not read withdrawal_authorizations"
grep -qiE 'INSERT|UPDATE|DELETE|TRUNCATE|DROP' <<<"$grant_check" \
  && bad "case4: grant read-back is not read-only" \
  || ok "case4: grant read-back contains no write statement"

# Execute the REAL read-back snippet in isolated subshells against a psql_ro
# stub: only the matching owner may reach the success log; a missing owner, a
# foreign owner and a failed read must all fail closed.
PSQL_REC="$WORK/psql.queries"
psql_ro() { printf '%s\n' "$1" >> "$PSQL_REC"; printf '%s\n' "$PSQL_OUT"; return "$PSQL_RC"; }
run_grant_check() {
  PSQL_OUT="$1" PSQL_RC="$2"
  ( eval "$grant_check" ) > "$WORK/grant.out" 2> "$WORK/grant.err"
}
check_grant_case() {  # <label> <out> <rc> <want_ok>
  local label=$1 out=$2 rc=$3 want_ok=$4 got=0
  rm -f "$WORK/grant.out" "$WORK/grant.err"
  run_grant_check "$out" "$rc" && got=0 || got=1
  if [[ "$want_ok" == "1" && "$got" == "0" ]]; then
    grep -q 'ok: grant owner == signer principal' "$WORK/grant.out" \
      && ok "case4: real read-back succeeds for $label" \
      || bad "case4: read-back returned success for $label without the success log"
  elif [[ "$want_ok" == "0" && "$got" == "1" ]]; then
    grep -q 'ok: grant owner == signer principal' "$WORK/grant.out" \
      && bad "case4: read-back for $label reached the success log" \
      || ok "case4: real read-back fails closed for $label"
  else
    bad "case4: real read-back rc for $label = $got, want success=$want_ok (stderr: $(tr '\n' ' ' < "$WORK/grant.err"))"
  fi
}
rm -f "$PSQL_REC"
check_grant_case "matching owner 7001" 7001 0 1
check_grant_case "missing owner" "" 0 0
check_grant_case "foreign owner 8001" 8001 0 0
check_grant_case "unreadable grant (psql rc=1)" 7001 1 0
if [[ -f "$PSQL_REC" ]] && grep -q 'SELECT caller_id FROM withdrawal_authorizations' "$PSQL_REC" \
   && ! grep -qiE 'INSERT|UPDATE|DELETE|TRUNCATE|DROP' "$PSQL_REC"; then
  ok "case4: every real read-back query was a read-only SELECT"
else
  bad "case4: read-back queries are not SELECT-only: [$(cat "$PSQL_REC" 2>/dev/null | tr '\n' ' ')]"
fi

# ===========================================================================
[[ ! -f "$DOCKER_REC" ]] && ok "suite: no docker call in the whole suite" \
  || bad "suite: docker was invoked: $(cat "$DOCKER_REC" | tr '\n' ' ')"
printf '\nprincipal checks: %d passed, %d failed\n' "$pass_count" "$fail_count"
[[ "$fail_count" -eq 0 ]] || exit 1
