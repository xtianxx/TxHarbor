#!/usr/bin/env bash
# RESUME-03-FIX mock regression suite for the TxHarbor portfolio demo.
#
# Runs entirely against scripts/portfolio-demo/checks/mockbin (mock docker +
# mock curl): no daemon, no RPC, no listener, no network. It exercises the
# demo's preflight guards, the sanitized compose environment, the http_post /
# start_detached / stop_owned_containers / scan_public_for_secrets /
# graceful_stop helpers and democtl's pure solc-check.
#
# Safety: every invocation gets a fresh run id (RID); everything this suite
# creates lives under .evidence/portfolio-demo-checks/03-fix and is never
# deleted. `go build ./scripts/portfolio-demo/democtl` into that checks root is
# the only build; nothing else is compiled, started or connected to.
#
# Usage: bash scripts/portfolio-demo/checks/run_checks.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/../.." && pwd)"
CHECK_BIN="$SCRIPT_DIR/mockbin"
CHECKS_ROOT="$REPO_ROOT/.evidence/portfolio-demo-checks/03-fix"
RID="chk$(date -u +%Y%m%d%H%M%S)-$$"
RUNS="$CHECKS_ROOT/runs/$RID"
PATH="$CHECK_BIN:$PATH"
export PATH
cd "$REPO_ROOT"
mkdir -p "$CHECKS_ROOT" "$RUNS"

FAILED=0
TOTAL=0
CASE_LOG=""

# --- helpers ---------------------------------------------------------------
rc_ok() { # rc_ok <expected|nonzero> <actual>
  case "$1" in
    nonzero) [[ "$2" -ne 0 ]] ;;
    *) [[ "$2" == "$1" ]] ;;
  esac
}

stamp_hashes() { # stamp_hashes <case> — provenance of the sources under test
  {
    printf '== %s %s rid=%s\n' "$1" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RID"
    printf 'head %s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
    (cd "$DEMO_DIR" && sha256sum run_demo.sh lib.sh compile_token.sh)
  } >>"$CHECKS_ROOT/source-hashes.txt"
}

case_dir() { # case_dir <case> — per-run scratch dir; kept, never deleted
  local d="$RUNS/$1"
  mkdir -p "$d"
  printf '%s' "$d"
}

case_run() { # case_run <name> <expected_rc|nonzero> <cmd...>
  local name=$1 want=$2
  shift 2
  local log="$CHECKS_ROOT/03-fix-$name.log"
  local rc=0
  {
    printf '== case %s %s rid=%s\n' "$name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RID"
    printf '== cmd :'
    printf ' %q' "$@"
    printf '\n'
  } >"$log"
  "$@" >>"$log" 2>&1
  rc=$?
  printf '== rc  : %s (expected %s)\n' "$rc" "$want" >>"$log"
  TOTAL=$((TOTAL + 1))
  if rc_ok "$want" "$rc"; then
    printf 'PASS %s\n' "$name"
  else
    printf 'FAIL %s (rc=%s want %s; log %s)\n' "$name" "$rc" "$want" "$log"
    FAILED=$((FAILED + 1))
  fi
  CASE_LOG="$log"
  stamp_hashes "$name"
}

check() { # check <case> <description> <cmd...> — assertion on top of the rc
  local name=$1 desc=$2
  shift 2
  TOTAL=$((TOTAL + 1))
  if "$@" >>"$CASE_LOG" 2>&1; then
    printf 'PASS %s: %s\n' "$name" "$desc"
  else
    printf 'FAIL %s: %s\n' "$name" "$desc"
    FAILED=$((FAILED + 1))
  fi
}

has() { grep -qF -- "$2" "$1"; }   # has <file> <literal>
hasnt() { ! grep -qF -- "$2" "$1"; }
hasre() { grep -qE -- "$2" "$1"; }
hasntre() { ! grep -qE -- "$2" "$1"; }
no_calls() { [[ ! -s "$1" ]]; }    # missing or empty => the mock never ran
mode_is() { [[ "$(stat -c '%a' -- "$1" 2>/dev/null)" == "$2" ]]; }
dir_empty() { [[ -z "$(find "$1" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; }
no_compose_stop() { ! grep -qE 'compose +stop' "$1"; }

compose_env() { # compose_env <envlog> — env recorded for the compose config call
  awk '/^# ARGV / { hdr = $0; next }
       hdr ~ /compose/ && hdr ~ /config/ { print }' "$1"
}
env_has() { compose_env "$1" | grep -qE -- "$2"; }
env_lacks() { ! compose_env "$1" | grep -qE -- "$2"; }

# --- guards: preflight, run id, evidence root, tools dir, docker env -------
DEMO="$DEMO_DIR/run_demo.sh"

# 1) preflight passes against the mocks and exports the fixed demo ports.
d="$(case_dir preflight-pass)"
: >"$d/docker.log"
: >"$d/docker-env.log"
case_run preflight-pass 0 env \
  MOCK_LOG="$d/docker.log" MOCK_ENV_LOG="$d/docker-env.log" \
  PORTFOLIO_RUN_ID="$RID-preflight-pass" \
  "$DEMO" --preflight-only
check preflight-pass 'mock docker served the compose render' \
  has "$d/docker.log" 'config --format json'
check preflight-pass 'preflight reports success' \
  has "$CASE_LOG" 'PREFLIGHT PASS (no resources)'
check preflight-pass 'compose call has PORTFOLIO_PG_PORT=55436' \
  env_has "$d/docker-env.log" '^PORTFOLIO_PG_PORT=55436$'
check preflight-pass 'compose call has no COMPOSE_REMOVE_ORPHANS' \
  env_lacks "$d/docker-env.log" '^COMPOSE_REMOVE_ORPHANS='

# 2) caller env cannot inject compose knobs: the sanitized compose call still
#    sees the demo's own port and never sees COMPOSE_REMOVE_ORPHANS.
d="$(case_dir env-injection)"
: >"$d/docker.log"
: >"$d/docker-env.log"
case_run env-injection 0 env \
  MOCK_LOG="$d/docker.log" MOCK_ENV_LOG="$d/docker-env.log" \
  PORTFOLIO_RUN_ID="$RID-env-injection" \
  COMPOSE_REMOVE_ORPHANS=1 PORTFOLIO_PG_PORT=15436 \
  "$DEMO" --preflight-only
check env-injection 'preflight still reports success' \
  has "$CASE_LOG" 'PREFLIGHT PASS (no resources)'
check env-injection 'caller COMPOSE_REMOVE_ORPHANS is dropped' \
  env_lacks "$d/docker-env.log" '^COMPOSE_REMOVE_ORPHANS='
check env-injection 'caller PORTFOLIO_PG_PORT is overridden' \
  env_lacks "$d/docker-env.log" '^PORTFOLIO_PG_PORT=15436$'
check env-injection 'compose call has PORTFOLIO_PG_PORT=55436' \
  env_has "$d/docker-env.log" '^PORTFOLIO_PG_PORT=55436$'

# 3) run id path traversal is refused before any docker call.
d="$(case_dir runid-traversal)"
: >"$d/docker.log"
case_run runid-traversal nonzero env \
  MOCK_LOG="$d/docker.log" PORTFOLIO_RUN_ID='../evil' "$DEMO" --preflight-only
check runid-traversal 'no docker call in the guard path' no_calls "$d/docker.log"

# 4) run id with characters outside [a-z0-9-] is refused the same way.
d="$(case_dir runid-bad-chars)"
: >"$d/docker.log"
case_run runid-bad-chars nonzero env \
  MOCK_LOG="$d/docker.log" PORTFOLIO_RUN_ID='Bad_ID' "$DEMO" --preflight-only
check runid-bad-chars 'no docker call in the guard path' no_calls "$d/docker.log"

# 5) a pre-existing evidence root is refused and stays untouched.
d="$(case_dir root-exists)"
root="$REPO_ROOT/.evidence/portfolio-demo/$RID-root-exists"
mkdir -p "$root"
case_run root-exists nonzero env \
  MOCK_LOG="$d/docker.log" PORTFOLIO_RUN_ID="$RID-root-exists" "$DEMO" --preflight-only
check root-exists 'refusal is reported' has "$CASE_LOG" 'refused'
check root-exists 'pre-existing root left untouched' dir_empty "$root"

# 6) a symlinked evidence root is refused (not followed) and left in place.
d="$(case_dir root-symlink)"
target="$d/root-target"
mkdir -p "$target" "$REPO_ROOT/.evidence/portfolio-demo"
link="$REPO_ROOT/.evidence/portfolio-demo/$RID-root-symlink"
ln -s "$target" "$link"
case_run root-symlink nonzero env \
  MOCK_LOG="$d/docker.log" PORTFOLIO_RUN_ID="$RID-root-symlink" "$DEMO" --preflight-only
check root-symlink 'refusal is reported' has "$CASE_LOG" 'refused'
check root-symlink 'symlink not replaced' test -L "$link"
check root-symlink 'nothing written through the symlink' dir_empty "$target"

# 7) a fresh sandbox (no .evidence at all) still preflights: the evidence base
#    and the 0700 run root are created from the script's own location.
d="$(case_dir missing-base)"
sandbox="$d/sandbox"
mkdir -p "$sandbox/scripts/portfolio-demo"
cp "$DEMO" "$DEMO_DIR/lib.sh" "$sandbox/scripts/portfolio-demo/"
cp "$DEMO_DIR/compose.portfolio-demo.yaml" "$sandbox/scripts/portfolio-demo/"
cp "$REPO_ROOT/compose.yaml" "$sandbox/compose.yaml"
chmod +x "$sandbox/scripts/portfolio-demo/run_demo.sh"
: >"$d/docker.log"
: >"$d/docker-env.log"
rid="$RID-missing-base"
case_run missing-base 0 env \
  MOCK_LOG="$d/docker.log" MOCK_ENV_LOG="$d/docker-env.log" \
  PORTFOLIO_RUN_ID="$rid" \
  "$sandbox/scripts/portfolio-demo/run_demo.sh" --preflight-only
check missing-base 'preflight reports success' \
  has "$CASE_LOG" 'PREFLIGHT PASS (no resources)'
check missing-base 'run root created in the fresh sandbox' \
  test -d "$sandbox/.evidence/portfolio-demo/$rid"
check missing-base 'run root mode 700' \
  mode_is "$sandbox/.evidence/portfolio-demo/$rid" 700
check missing-base 'public dir mode 700' \
  mode_is "$sandbox/.evidence/portfolio-demo/$rid/public" 700

# 8) TXHARBOR_DEMO_TOOLS_DIR must be the approved path itself: a sibling path
#    and a symlink pointing at the approved path are refused.
d="$(case_dir tools-dir-refused)"
: >"$d/docker.log"
case_run tools-dir-refused nonzero env \
  MOCK_LOG="$d/docker.log" PORTFOLIO_RUN_ID="$RID-tools-dir-refused" \
  TXHARBOR_DEMO_TOOLS_DIR=/tmp/opencode/not-approved \
  "$DEMO" --preflight-only
check tools-dir-refused 'refusal is reported' hasre "$CASE_LOG" '^FAIL'

ln -s /tmp/opencode/txharbor-demo-tools "$d/approved-link"
: >"$d/docker2.log"
case_run tools-dir-symlink nonzero env \
  MOCK_LOG="$d/docker2.log" PORTFOLIO_RUN_ID="$RID-tools-dir-symlink" \
  TXHARBOR_DEMO_TOOLS_DIR="$d/approved-link" \
  "$DEMO" --preflight-only
check tools-dir-symlink 'refusal is reported' hasre "$CASE_LOG" '^FAIL'

# 9) an RPC override input is refused before any docker call or connection.
d="$(case_dir rpc-refused)"
: >"$d/docker.log"
case_run rpc-refused nonzero env \
  MOCK_LOG="$d/docker.log" PORTFOLIO_RUN_ID="$RID-rpc-refused" \
  TXHARBOR_RPC_URL=http://127.0.0.1:8545 "$DEMO" --preflight-only
check rpc-refused 'no docker call before the RPC guard' no_calls "$d/docker.log"
check rpc-refused 'refusal is reported' hasre "$CASE_LOG" '^FAIL'

# 10) remote docker inputs are refused before any docker call: the caller
#     DOCKER_* vars, and a context whose inspected endpoint is not a unix socket.
d="$(case_dir docker-guards)"
: >"$d/docker-host.log"
case_run docker-host nonzero env \
  MOCK_LOG="$d/docker-host.log" PORTFOLIO_RUN_ID="$RID-docker-host" \
  DOCKER_HOST=tcp://127.0.0.1:2375 "$DEMO" --preflight-only
check docker-host 'no docker call before the DOCKER_HOST guard' \
  no_calls "$d/docker-host.log"

case_run docker-tls-verify nonzero env \
  MOCK_LOG="$d/docker-tls.log" PORTFOLIO_RUN_ID="$RID-docker-tls" \
  DOCKER_TLS_VERIFY=1 "$DEMO" --preflight-only
check docker-tls-verify 'no docker call before the TLS guard' \
  no_calls "$d/docker-tls.log"

case_run docker-cert-path nonzero env \
  MOCK_LOG="$d/docker-cert.log" PORTFOLIO_RUN_ID="$RID-docker-cert" \
  DOCKER_CERT_PATH=/tmp/opencode/mock-docker-certs "$DEMO" --preflight-only
check docker-cert-path 'no docker call before the cert-path guard' \
  no_calls "$d/docker-cert.log"

case_run docker-config nonzero env \
  MOCK_LOG="$d/docker-config.log" PORTFOLIO_RUN_ID="$RID-docker-config" \
  DOCKER_CONFIG=/tmp/opencode/mock-docker-config "$DEMO" --preflight-only
check docker-config 'no docker call before the DOCKER_CONFIG guard' \
  no_calls "$d/docker-config.log"

case_run docker-context-endpoint nonzero env \
  MOCK_LOG="$d/docker-context.log" MOCK_BAD_ENDPOINT=1 \
  PORTFOLIO_RUN_ID="$RID-context-endpoint" "$DEMO" --preflight-only
check docker-context-endpoint 'the remote endpoint was seen' \
  has "$CASE_LOG" 'tcp://127.0.0.1:2375'
check docker-context-endpoint 'the context was inspected first' \
  has "$d/docker-context.log" 'context inspect'

# --- helpers under test ----------------------------------------------------
probe_http_post() { # probe_http_post <dir> — http_post under the mock curl
  local d=$1
  (
    export MOCK_LOG="$d/curl.log"
    : >"$d/curl.log"
    set -uo pipefail
    # shellcheck source=../lib.sh
    . "$DEMO_DIR/lib.sh"
    if ! declare -f http_post >/dev/null 2>&1; then
      eval "$(sed -n '/^http_post() {/,/^}/p' "$DEMO_DIR/run_demo.sh")"
    fi
    if ! declare -f http_post >/dev/null 2>&1; then
      printf 'http_post is defined in neither lib.sh nor run_demo.sh\n' >&2
      exit 9
    fi
    http_post "$d/curl.conf" "$d/body.json" "$d/out.txt"
  )
}

# 11) http_post must harden the curl invocation: no caller config, no proxy,
#     bounded connect and total time.
d="$(case_dir http-post-flags)"
printf 'url = "http://127.0.0.1:18081/orders"\n' >"$d/curl.conf"
printf '{}\n' >"$d/body.json"
case_run http-post-flags 0 probe_http_post "$d"
check http-post-flags 'curl argv has --disable' \
  has "$d/curl.log" --disable
check http-post-flags "curl argv has --noproxy '*'" \
  hasre "$d/curl.log" '--noproxy[ =]\*'
check http-post-flags 'curl argv has --connect-timeout 5' \
  hasre "$d/curl.log" '--connect-timeout 5'
check http-post-flags 'curl argv has --max-time 30' \
  hasre "$d/curl.log" '--max-time 30'
check http-post-flags 'the status printed by curl is surfaced' \
  has "$CASE_LOG" '000'

probe_start_detached() { # probe_start_detached <dir> — varfile exported to child
  local d=$1
  (
    set -uo pipefail
    # shellcheck source=../lib.sh
    . "$DEMO_DIR/lib.sh"
    if ! declare -f start_detached >/dev/null 2>&1; then
      printf 'start_detached is not defined in lib.sh\n' >&2
      exit 9
    fi
    printf 'A=1\n' >"$d/env"
    printf 's3cret\n' >"$d/cred"
    : >"$d/child.log"
    # run_demo.sh's own shape: the child is detached and self-terminating, so
    # the echoed PID is recorded as evidence only (never used to signal a PID
    # that a wait-free parent no longer owns).
    start_detached "$d/env" "$d/child.log" TXHARBOR_TX_SIGNER_CREDENTIAL "$d/cred" -- \
      bash -c '[ -n "$TXHARBOR_TX_SIGNER_CREDENTIAL" ] && echo HAS_SECRET || echo NOSECRET' \
      >"$d/started-pid"
    local i=0
    while [[ $i -lt 100 ]]; do
      if grep -qE 'HAS_SECRET|NOSECRET' "$d/child.log" 2>/dev/null; then
        break
      fi
      sleep 0.1
      i=$((i + 1))
    done
    exit 0
  )
}

probe_old_pattern() { # control: the pre-fix inline start, credential not exported
  local d=$1
  (
    set -a
    . "$d/env"
    set +a
    TXHARBOR_TX_SIGNER_CREDENTIAL="$(cat "$d/cred")"
    exec bash -c '[ -n "$TXHARBOR_TX_SIGNER_CREDENTIAL" ] && echo HAS_SECRET || echo NOSECRET'
  ) >"$d/control.log" 2>&1
  return 0
}

# 12) start_detached exports the varfile content into the child; the secret is
#     never printed and the pre-fix pattern (no export) cannot see it at all.
d="$(case_dir start-detached)"
case_run start-detached-export 0 probe_start_detached "$d"
check start-detached-export 'child sees the exported credential' \
  has "$d/child.log" HAS_SECRET
check start-detached-export 'a numeric PID is echoed on stdout' \
  hasre "$d/started-pid" '^[0-9]+$'
check start-detached-export 'the secret value never reaches the log' \
  hasnt "$d/child.log" s3cret
case_run old-pattern-control 0 probe_old_pattern "$d"
check old-pattern-control 'control (credential not exported) sees nothing' \
  has "$d/control.log" NOSECRET
check old-pattern-control 'control log carries no secret' \
  hasnt "$d/control.log" s3cret

probe_stop_owned() { # probe_stop_owned <dir> <owned-ids> — ownership intersection
  local d=$1 owned=$2
  (
    set -uo pipefail
    export MOCK_LOG="$d/docker.log" MOCK_ENV_LOG="$d/docker-env.log"
    export MOCK_OWNED_IDS="$owned"
    : >"$d/docker.log"
    : >"$d/docker-env.log"
    # shellcheck source=../lib.sh
    . "$DEMO_DIR/lib.sh"
    if ! declare -f stop_owned_containers >/dev/null 2>&1; then
      printf 'stop_owned_containers is not defined in lib.sh\n' >&2
      exit 9
    fi
    DOCKER=(docker --context default)
    PROJECT="txharbor-demo-$RID"
    RUN_ID="$RID"
    RUN_TOKEN="$RID"
    PORTFOLIO_RUN_TOKEN="$RID"
    declare -A CONTAINER_IDS=([postgres]=id1 [anvil]=id2)
    stop_owned_containers
  )
}

# 13) stop_owned_containers stops only this run's containers (intersection of
#     the ownership query with CONTAINER_IDS); never `compose stop`; a run that
#     owns nothing stops nothing.
d="$(case_dir stop-owned)"
case_run stop-owned-intersection 0 probe_stop_owned "$d" "id1 id2 id3"
check stop-owned-intersection 'stops exactly the owned intersection' \
  hasre "$d/docker.log" '^STOPPED (id1 id2|id2 id1)$'
check stop-owned-intersection 'never stops an id this run does not own' \
  hasntre "$d/docker.log" '^STOPPED .*id3'
check stop-owned-intersection 'queries the run_token label' \
  has "$d/docker.log" 'run_token'
check stop-owned-intersection 'never stops via compose' \
  no_compose_stop "$d/docker.log"
e="$(case_dir stop-owned-empty)"
case_run stop-owned-empty 0 probe_stop_owned "$e" ""
check stop-owned-empty 'queries the run_token label' \
  has "$e/docker.log" 'run_token'
check stop-owned-empty 'no STOPPED line for an empty intersection' \
  hasnt "$e/docker.log" 'STOPPED'
check stop-owned-empty 'never stops via compose' \
  no_compose_stop "$e/docker.log"

probe_scan() { # probe_scan <dir> <patterns-file> — public-evidence secret scan
  local dir=$1 pat=$2
  (
    set -uo pipefail
    # shellcheck source=../lib.sh
    . "$DEMO_DIR/lib.sh"
    if ! declare -f scan_public_for_secrets >/dev/null 2>&1; then
      printf 'scan_public_for_secrets is not defined in lib.sh\n' >&2
      exit 9
    fi
    scan_public_for_secrets "$dir" "$pat"
  )
}

# 14) scan_public_for_secrets: 0 on a clean dir, non-zero with a fail message
#     when a pattern value is present, non-zero when the patterns file is gone.
d="$(case_dir scan-public)"
mkdir -p "$d/clean" "$d/dirty"
printf 'nothing to see here\n' >"$d/clean/pub.txt"
printf 'token=s3cret-value\n' >"$d/dirty/pub.txt"
printf 's3cret-value\n' >"$d/patterns"
case_run scan-public-clean 0 probe_scan "$d/clean" "$d/patterns"
check scan-public-clean 'clean dir reports no failure' hasnt "$CASE_LOG" 'FAIL'
case_run scan-public-leak nonzero probe_scan "$d/dirty" "$d/patterns"
check scan-public-leak 'leak is reported as a failure' hasre "$CASE_LOG" '^FAIL'
case_run scan-public-missing-patterns nonzero probe_scan "$d/clean" "$d/nope.patterns"

probe_graceful_stop() { # probe_graceful_stop <dir> — TERM only, bounded, no KILL
  local d=$1
  (
    set -uo pipefail
    # shellcheck source=../lib.sh
    . "$DEMO_DIR/lib.sh"
    if ! declare -f graceful_stop >/dev/null 2>&1; then
      printf 'graceful_stop is not defined in lib.sh\n' >&2
      exit 9
    fi
    # TERM-ignoring child (exec keeps it a single process): the helper must
    # time out instead of escalating to KILL.
    bash -c 'trap "" TERM; exec sleep 60' &
    local pid=$! rc=0 alive=no
    graceful_stop "$pid" probe 1 || rc=$?
    if kill -0 "$pid" 2>/dev/null; then
      alive=yes
    fi
    printf 'rc=%s alive=%s pid=%s\n' "$rc" "$alive" "$pid" >"$d/probe.out"
    if [[ "$alive" == yes ]]; then
      kill -TERM "$pid" 2>/dev/null || true # ignored by the child, by design
      sleep 0.2
      kill -KILL "$pid" 2>/dev/null || true # this suite's own child, never leaked
      wait "$pid" 2>/dev/null || true
    fi
    exit 0
  )
}

# 15) graceful_stop is TERM-only: a process that outlives the bound yields
#     rc 1, is left alive and is never KILLed by the helper.
d="$(case_dir graceful-stop)"
case_run graceful-stop-timeout 0 probe_graceful_stop "$d"
check graceful-stop-timeout 'timeout returns rc 1' hasre "$d/probe.out" '^rc=1 '
check graceful-stop-timeout 'the process is still alive (no KILL)' \
  hasre "$d/probe.out" ' alive=yes '

# 16) democtl solc-check (pure helper): a clean render with a warning passes,
#     severity "error", invalid JSON and missing contract output all fail.
d="$(case_dir democtl-solc-check)"
democtl="$d/democtl"
case_run democtl-solc-build 0 go build -o "$democtl" ./scripts/portfolio-demo/democtl
check democtl-solc-build 'democtl built into the checks root' test -x "$democtl"

solc_ok='"contracts":{"MinimalERC20":{"MinimalERC20":{"abi":[{"type":"function","name":"transfer"}],"evm":{"bytecode":{"object":"6080"},"deployedBytecode":{"object":"6080"}}}}}'
printf '{"errors":[{"severity":"warning","message":"warned"}],%s}\n' "$solc_ok" >"$d/warn.json"
printf '{"errors":[{"severity":"error","message":"boom"}],%s}\n' "$solc_ok" >"$d/error.json"
printf '{"contracts":\n' >"$d/invalid.json"
printf '{"errors":[],"contracts":{"Other":{"Other":{"abi":[{"type":"function","name":"transfer"}],"evm":{"bytecode":{"object":"6080"},"deployedBytecode":{"object":"6080"}}}}}}\n' >"$d/missing.json"

probe_solc_check() { # probe_solc_check <democtl> <json-file>
  "$1" solc-check <"$2"
}

case_run solc-check-clean 0 probe_solc_check "$democtl" "$d/warn.json"
check solc-check-clean 'clean render reports only the warning' \
  has "$CASE_LOG" 'solc clean: 1 warning(s)'
case_run solc-check-error nonzero probe_solc_check "$democtl" "$d/error.json"
check solc-check-error 'the error message is surfaced' has "$CASE_LOG" 'boom'
case_run solc-check-invalid-json nonzero probe_solc_check "$democtl" "$d/invalid.json"
check solc-check-invalid-json 'invalid JSON is rejected' hasre "$CASE_LOG" '^democtl:'
case_run solc-check-missing-contract nonzero probe_solc_check "$democtl" "$d/missing.json"

# --- summary ---------------------------------------------------------------
printf '\n== summary: %s/%s checks failed (rid %s)\n' "$FAILED" "$TOTAL" "$RID"
printf '== checks root: %s\n' "$CHECKS_ROOT"
printf '== mock bin:    %s\n' "$CHECK_BIN"
if ((FAILED > 0)); then
  printf 'RESULT: FAIL %s check(s) failed\n' "$FAILED"
  exit 1
fi
printf 'RESULT: PASS all %s checks passed\n' "$TOTAL"
exit 0
