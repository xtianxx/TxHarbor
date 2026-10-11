#!/usr/bin/env bash
# TxHarbor portfolio demo: single-run orchestration (setup -> A -> B -> verify).
#
# One isolated run = one compose project (txharbor-demo-<run-id>), one fresh
# Postgres, one fresh Anvil (chain-id 31337, block 0), one evidence root.
# Nothing here touches the default `txharbor` stack, existing volumes or other
# runs; the script never deletes containers, volumes or networks.
#
# Modes: run_all (default) | --preflight-only | --help
#
# Safety shape: preflight refuses an RPC override input, non-unix/remote docker
# contexts and pre-existing run resources BEFORE any side effect; every chain
# call goes through the demo's own anvil container and every DB call through the
# demo's own postgres container; secrets live only in 0600 private/ files and
# never in argv or public/ evidence. Amounts are compared with `democtl uint256`
# -- never with bash arithmetic.
set -euo pipefail
umask 077  # every file created here is 0600 unless an explicit mode is given
# set -x is deliberately never enabled: it would echo argv into the run log.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=scripts/portfolio-demo/lib.sh
. "$SCRIPT_DIR/lib.sh"

# --- identity of this run -------------------------------------------------
RUN_ID="${PORTFOLIO_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)-$$}"
PROJECT="txharbor-demo-$RUN_ID"
# FIXED absolute evidence base (no caller override); preflight prepares it.
EVIDENCE_BASE="$REPO_ROOT/.evidence/portfolio-demo"
EVIDENCE_ROOT="$EVIDENCE_BASE/$RUN_ID"

# --- fixed demo port / policy constants -----------------------------------
PG_PORT=55436
ANVIL_PORT=58547
HTTP_PORT=18081
SIGNER_PORT=18091
CONFIRM_DEPTH=3
CALLER_ID=7001
ISSUER_CALLER_ID=9001
# The 009 signer credential must authenticate as the SAME logical caller that
# owns the withdrawal authorization: the signer scopes every grant read to the
# authenticated caller_id and refuses a foreign-owned grant as
# authorization_invalid (403), which would otherwise surface only as an
# executing intent with zero mines. One logical owner is not one secret: the
# API key and the signer credential stay two separate CLI-issued secrets in two
# separate 0600 files, and the issuer stays its own identity.
SIGNER_CALLER_ID="$CALLER_ID"
IDEM="portfolio-demo-1"
AUTH_ID="auth-portfolio-1"
INTENT_ID="intent-portfolio-1"
TOOLS_DIR="${TXHARBOR_DEMO_TOOLS_DIR:-/tmp/opencode/txharbor-demo-tools}"

# --- amounts (canonical decimal strings; never touched by bash math) -------
SUPPLY="1000000000000000000000000"
DEPOSIT="12345000000000000000000"
FUND="5000000000000000000000"
WITHDRAW="1000000000000000000000"
DEPLOYER_FINAL="982655000000000000000000"
SIGNER_FINAL="4000000000000000000000"
CONFLICT_AMOUNT="1000000000000000000001"   # WITHDRAW + 1, literal
# == MAX_GAS_LIMIT * MAX_FEE_PER_GAS (the literal product 2000000000000000):
# the scope fee ceiling may never be looser than the signer's own policy
# limits, so it IS that product (stage B asserts the relation in shell).
FEE_TOTAL="2000000000000000"
FEE_PER_GAS="10000000000"
FEE_PRIORITY="1000000000"
MAX_GAS_LIMIT="200000"
MAX_FEE_PER_GAS="10000000000"
MAX_PRIORITY_FEE_PER_GAS="1000000000"
MAX_GAS_PRICE="10000000000"
# Signer policy amount cap. The plan text's literal 1000000000000000000 (1e18)
# is smaller than this run's withdrawal (WITHDRAW = 1e21 = 1000 tokens) and
# `signer-serve` refuses any amount above its cap (internal/signer/policy.go:
# "policy_amount: amount exceeds cap"); 1e21 == WITHDRAW keeps the cap
# meaningful, signable and >= 1000 tokens as the plan requires.
MAX_AMOUNT="1000000000000000000000"

# keccak256("Transfer(address,address,uint256)"): the fixture emits exactly
# this event, so every receipt/log assertion compares against this literal.
TRANSFER_SIG="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# --- run state ------------------------------------------------------------
STACK_STARTED=0
RUN_OK=0
RUN_TOKEN=""
STARTED_PID=""
OWNED_PIDS=()
SERVE_PID=""
SIGNER_PID=""
WORKER_PID=""
BIN=""
DEMOCTL=""
RUN_BIN=""
TOKEN=""
SIGNER_ADDR=""
DEPLOYER=""
WATCH=""
RECIPIENT=""
REQUEST_ID=""
SIGNING_TX_HASH=""
DEPLOY_TX=""
DEPOSIT_TX=""
DEPOSIT_BLOCK_NUMBER=""
DEPOSIT_BLOCK_HASH=""
NATIVE_FUND_TX=""
SIGNER_NATIVE_BALANCE=""
CALLER_KEY=""
ISSUER_KEY=""
SIGNER_CRED=""
ENV_SERVE=""
ENV_SIGNER=""
ENV_WORKER=""
KEY_CALLER=""
KEY_ISSUER=""
KEY_SIGNER=""
KEY_SIGNER_CRED=""
PUBLIC_DIR=""
PRIVATE_DIR=""
declare -a DOCKER=() COMPOSE=() CAST=()
declare -A CONTAINER_IDS=() CONTAINER_NAMES=()

# --- runtime helpers ------------------------------------------------------
# cast_rpc <subcommand> [args...] -- every chain call goes through the demo's
# own anvil container (image-pinned cast; no host cast fallback) and is
# bounded by `timeout` (30s).
cast_rpc() {
  local sub=$1
  shift
  with_timeout 30 "${CAST[@]}" "$sub" --rpc-url "http://127.0.0.1:8545" "$@"
}

# cast_logs [args...] -- eth_getLogs through the same container (30s bound).
cast_logs() {
  with_timeout 30 "${CAST[@]}" logs --json --rpc-url "http://127.0.0.1:8545" "$@"
}

# cast_uint <token> <signature> [args...] -- decoded uint256 return; only the
# first whitespace-delimited token is kept (cast may append a human hint).
cast_uint() {
  local target=$1 sig=$2
  shift 2
  with_timeout 30 "${CAST[@]}" call "$target" "$sig" "$@" \
    --rpc-url "http://127.0.0.1:8545" | awk '{print $1}'
}

# cast_balance <address> -- native balance in wei (first token only: cast may
# append a human hint).
cast_balance() {
  cast_rpc balance "$1" | awk '{print $1}'
}

# psql_ro <sql> -- one statement through the demo's own postgres container
# (local connection inside the container; no password ever enters argv),
# bounded by `timeout` (20s).
psql_ro() {
  with_timeout 20 "${COMPOSE[@]}" exec -T postgres psql -w -U txharbor \
    -d txharbor -v ON_ERROR_STOP=1 -qAt -c "$1"
}

# psql_script -- multi-statement SQL on stdin through the same container.
psql_script() {
  with_timeout 20 "${COMPOSE[@]}" exec -T postgres psql -w -U txharbor \
    -d txharbor -v ON_ERROR_STOP=1 -qAt
}

# http_post <curl-conf> <body-file|-> <out-file> -- prints the HTTP status
# code. The bearer lives in the 0600 curl config, never in argv; only the body
# file path (never the body) is passed on the command line. `--disable` (no
# .curlrc), `--noproxy '*'` (no proxy hook) and the fixed timeouts pin the
# request to the demo's own loopback endpoint.
http_post() {
  local conf=$1 body=$2 out=$3
  local -a args=(--config "$conf" --silent --show-error --output "$out" --write-out '%{http_code}')
  if [[ "$body" != "-" ]]; then
    args+=(--data-binary "@$body")
  fi
  # --disable MUST be the first argument (curl reads .curlrc before parsing any
  # later flag), followed by the explicit direct-connection and timeout pins,
  # then the config/output arguments.
  curl --disable --noproxy '*' --connect-timeout 5 --max-time 30 "${args[@]}"
}

# require_services_alive -- EVERY poll calls this: a started service that died
# is reported at the point it happens, naming its private log (private logs
# are never echoed to stderr). A zombie counts as dead.
require_services_alive() {
  local name pid
  for name in serve signer worker; do
    case "$name" in
      serve)  pid=$SERVE_PID ;;
      signer) pid=$SIGNER_PID ;;
      worker) pid=$WORKER_PID ;;
    esac
    if [[ -n "$pid" ]] && ! pid_alive "$pid"; then
      fail "$name (pid $pid) is no longer running; private log: $PRIVATE_DIR/logs/$name.log"
    fi
  done
}

# public_sql <name> <sql> -- export one read-only query to public/sql/<name>.out
# (public evidence carries counts, statuses and hashes only).
public_sql() {
  mkdir -m 700 -p "$PUBLIC_DIR/sql" || fail "cannot create $PUBLIC_DIR/sql"
  psql_ro "$2" > "$PUBLIC_DIR/sql/$1.out" || fail "public sql export $1 failed"
}

# compose_service_block <rendered-json> <service> -- the rendered object of one
# service, from its key line up to the next sibling or the services map's
# closing brace (indentation-agnostic: the service key's own indent defines the
# block). Per-service checks then cannot match a sibling's value.
compose_service_block() {
  printf '%s\n' "$1" | awk -v svc="$2" '
    {
      if (inblock) {
        match($0, /^[ ]*/)
        if (RLENGTH <= padlen) exit
        print
        next
      }
      if ($0 ~ ("^[ ]*\"" svc "\":[ ]*[{]$")) {
        match($0, /^[ ]*/)
        padlen = RLENGTH
        inblock = 1
        print
      }
    }
  '
}

# check_transfer_receipt <label> <receipt-json> <sender> <recipient> <amount>
# The full ERC-20 transfer shape for BOTH real chain effects (stage A deposit
# and stage B signing tx): the transaction was sent by <sender> to the token
# CONTRACT (a transfer call's tx-level `to` is the contract, not the payee),
# and the Transfer log carries exactly three topics -- the event signature plus
# the zero-padded sender/recipient -- with the uint256 amount as data.
check_transfer_receipt() {
  local label=$1 rcpt=$2 sender=$3 recipient=$4 amount=$5 want_tx=$6
  local want_block_hash="${7:-}" want_block_number="${8:-}"
  expect_eq "$label receipt status" "0x1" "$(printf '%s\n' "$rcpt" | json_get status)"
  expect_eq "$label receipt transactionHash == expected" "$(lower_hex "$want_tx")" \
    "$(lower_hex "$(printf '%s\n' "$rcpt" | json_get transactionHash)")"
  expect_eq "$label receipt from == sender" "$sender" \
    "$(lower_hex "$(printf '%s\n' "$rcpt" | json_get from)")"
  expect_eq "$label receipt to == token (contract call)" "$TOKEN" \
    "$(lower_hex "$(printf '%s\n' "$rcpt" | json_get to)")"
  require_nonempty "$label receipt blockNumber" "$(printf '%s\n' "$rcpt" | json_get blockNumber)"
  if [[ -n "$want_block_number" ]]; then
    expect_uint_eq "$label receipt blockNumber == DB block_number" \
      "$(printf '%s\n' "$rcpt" | json_get blockNumber)" "$want_block_number"
  fi
  if [[ -n "$want_block_hash" ]]; then
    expect_eq "$label receipt blockHash == DB block_hash" "$(lower_hex "$want_block_hash")" \
      "$(lower_hex "$(printf '%s\n' "$rcpt" | json_get blockHash)")"
  fi
  expect_eq "$label log count" "1" "$(printf '%s\n' "$rcpt" | json_count logs)"
  expect_eq "$label log address == token" "$TOKEN" \
    "$(lower_hex "$(printf '%s\n' "$rcpt" | json_get 'logs[0].address')")"
  expect_eq "$label topic count" "3" \
    "$(printf '%s\n' "$rcpt" | json_get 'logs[0].topics' | json_count .)"
  expect_eq "$label topic0 == Transfer signature" "$TRANSFER_SIG" \
    "$(printf '%s\n' "$rcpt" | json_get 'logs[0].topics[0]')"
  expect_eq "$label topic1 == transfer from" "0x000000000000000000000000${sender#0x}" \
    "$(printf '%s\n' "$rcpt" | json_get 'logs[0].topics[1]')"
  expect_eq "$label topic2 == transfer to" "0x000000000000000000000000${recipient#0x}" \
    "$(printf '%s\n' "$rcpt" | json_get 'logs[0].topics[2]')"
  expect_eq "$label data == amount" "$("$DEMOCTL" uint256 hex "$amount")" \
    "$(printf '%s\n' "$rcpt" | json_get 'logs[0].data')"
  require_nonempty "$label logIndex" "$(printf '%s\n' "$rcpt" | json_get 'logs[0].logIndex')"
}

# check_transfer_log_uniqueness <observed_head> <signing_receipt> <signing_tx>
# Proves from eth_getLogs that across the whole chain (block 0..observed_head)
# there is exactly one Transfer(signer -> recipient) and that it is the signing
# transaction's log, field by field.
#
# The filter is passed as three RAW 32-byte topics, never as an event
# signature: foundry v1.8.1 `cast logs` derives topics only from indexed event
# inputs (crates/cast/src/cmd/logs.rs: build_filter -> build_filter_event_sig
# zips event.inputs filtered by `indexed` with the positional args), so
# "Transfer(address,address,uint256)" — whose inputs carry no `indexed` marker —
# yields topic0 only and leaves topic1/topic2 as wildcards. Raw topics are
# parsed by build_filter_topics (rawtopic0/1/2).
#
# Evidence order: the filter metadata and the raw stdout/stderr are persisted to
# this run's PRIVATE 0600 files before any assertion, so a failure or a
# malformed response stays reproducible; only after every structural and
# semantic check passes are they copied into public/ (which the final secrets
# scan still governs).
check_transfer_log_uniqueness() {
  local observed_head=$1 signing_receipt=$2 signing_tx=$3
  local topic_from topic_to filter_file raw_file err_file
  topic_from=$(lower_hex "0x000000000000000000000000${SIGNER_ADDR#0x}")
  topic_to=$(lower_hex "0x000000000000000000000000${RECIPIENT#0x}")
  filter_file="$PRIVATE_DIR/logs/transfer-log-query.filter.txt"
  raw_file="$PRIVATE_DIR/logs/transfer-log-query.json"
  err_file="$PRIVATE_DIR/logs/transfer-log-query.err"
  {
    printf 'from_block=0\nto_block=%s\naddress=%s\n' "$observed_head" "$TOKEN"
    printf 'topic0=%s\ntopic1=%s\ntopic2=%s\n' "$TRANSFER_SIG" "$topic_from" "$topic_to"
  } > "$filter_file"
  local rc=0
  # Redirect stdout straight into the private evidence file: a shell variable
  # would drop trailing newlines and NUL bytes, i.e. it would alter the very
  # bytes this evidence exists to preserve.
  cast_logs --address "$TOKEN" --from-block 0 --to-block "$observed_head" \
    "$TRANSFER_SIG" "$topic_from" "$topic_to" > "$raw_file" 2> "$err_file" || rc=$?
  chmod 600 "$filter_file" "$raw_file" "$err_file"
  if (( rc != 0 )); then
    fail "cast logs (eth_getLogs) failed rc=$rc; raw output and stderr kept private"
  fi
  local logs_n
  logs_n=$(json_count . < "$raw_file") \
    || fail "getLogs response is not a JSON array (raw kept private)"
  expect_eq "getLogs Transfer(signer->recipient) count at head $observed_head" "1" "$logs_n"
  # Type-exact and direct-member exact: json_get renders the JSON string
  # "false" and the boolean false identically, and a raw grep can be fooled by a
  # nested decoy member, so the PARSED object's direct member is checked on its
  # pretty-printed form (two-space indent; nested members sit deeper).
  "$DEMOCTL" json get --pretty '[0]' < "$raw_file" \
    | grep -Eq '^  "removed": false,?$' \
    || fail "getLogs log removed is not the direct JSON boolean false (raw kept private)"
  expect_eq "getLogs log transactionHash == signing tx" \
    "$(lower_hex "$signing_tx")" \
    "$(lower_hex "$(json_get '[0].transactionHash' < "$raw_file")")"
  expect_eq "getLogs log address == token" "$TOKEN" \
    "$(lower_hex "$(json_get '[0].address' < "$raw_file")")"
  expect_eq "getLogs log topic count" "3" \
    "$(json_count '[0].topics' < "$raw_file")"
  expect_eq "getLogs log topic0 == Transfer signature" "$TRANSFER_SIG" \
    "$(json_get '[0].topics[0]' < "$raw_file")"
  expect_eq "getLogs log topic1 == transfer from (signer)" "$topic_from" \
    "$(lower_hex "$(json_get '[0].topics[1]' < "$raw_file")")"
  expect_eq "getLogs log topic2 == transfer to (recipient)" "$topic_to" \
    "$(lower_hex "$(json_get '[0].topics[2]' < "$raw_file")")"
  expect_eq "getLogs log data == WITHDRAW amount" "$("$DEMOCTL" uint256 hex "$WITHDRAW")" \
    "$(json_get '[0].data' < "$raw_file")"
  expect_eq "getLogs log removed == false" "false" \
    "$(json_get '[0].removed' < "$raw_file")"
  # The unique log must be the same log the signing receipt already proved.
  expect_eq "getLogs log blockHash == signing receipt blockHash" \
    "$(lower_hex "$(printf '%s\n' "$signing_receipt" | json_get blockHash)")" \
    "$(lower_hex "$(json_get '[0].blockHash' < "$raw_file")")"
  expect_uint_eq "getLogs log blockNumber == signing receipt blockNumber" \
    "$(printf '%s\n' "$signing_receipt" | json_get blockNumber)" \
    "$(json_get '[0].blockNumber' < "$raw_file")"
  expect_uint_eq "getLogs log logIndex == signing receipt logIndex" \
    "$(printf '%s\n' "$signing_receipt" | json_get 'logs[0].logIndex')" \
    "$(json_get '[0].logIndex' < "$raw_file")"
  # Every structural and semantic check passed: publish the exact filter and the
  # raw JSON (still subject to the final secrets scan).
  cp "$filter_file" "$PUBLIC_DIR/chain/transfer-log-query.filter.txt"
  cp "$raw_file" "$PUBLIC_DIR/chain/transfer-log-query.json"
  log "ok: getLogs filter/raw JSON and the final chain snapshot persisted under public/chain"
}

# cli_secret <label> <cmd...> -- run an admin CLI command under the serve env
# and print ONLY the minted secret (the raw line, with the secret redacted,
# goes to the private CLI log; the plaintext never reaches stdout/logs).
cli_secret() {
  local label=$1 raw
  shift
  raw=$(with_env "$ENV_SERVE" "$@") || fail "$label failed"
  printf '%s\n' "$raw" | sed 's/ key=[^ ]*/ key=<redacted>/' >> "$PRIVATE_DIR/logs/cli.log"
  printf '%s\n' "$raw" | sed -n 's/.* key=\([^ ]*\).*/\1/p' | head -n 1
}

# cli_run <label> <cmd...> -- run an admin CLI command under the serve env and
# print its first output line (operator paper trail; never a secret).
cli_run() {
  local label=$1 raw
  shift
  raw=$(with_env "$ENV_SERVE" "$@") || fail "$label failed"
  printf '%s\n' "$raw" >> "$PRIVATE_DIR/logs/cli.log"
  printf '%s\n' "$raw" | head -n 1
}

# --- exit path ------------------------------------------------------------
# stop_stack: stop this run's own containers only, and only the ones this run
# can prove it owns (project + run_token intersection). Never `compose
# stop/down`, never -v, never rm, never prune: volumes, containers and
# networks survive the run.
stop_stack() {
  if [[ "$STACK_STARTED" != "1" ]]; then
    return 0
  fi
  stop_owned_containers
}

# stop_owned: TERM every still-tracked own PID, bounded wait, then require
# exit 0 from each. A PID that ignores TERM is left running (retained) and the
# cleanup is reported as failed -- no KILL is ever sent.
stop_owned() {
  local rc=0 pid st grace="${STOP_OWNED_GRACE_SECONDS:-10}" retained=()
  for pid in "${OWNED_PIDS[@]:-}"; do
    if [[ -z "$pid" ]]; then continue; fi
    if pid_alive "$pid"; then
      # Bounded per-PID stop: TERM + wait. A PID that ignores TERM is left
      # running (retained), is never waited on again (no hang) and never killed.
      if graceful_stop "$pid" "cleanup pid" "$grace"; then st=0; else st=$?; fi
    elif wait "$pid" 2>/dev/null; then
      st=0
    else
      st=$?
    fi
    if (( st != 0 )); then
      rc=1
      if pid_alive "$pid"; then
        retained+=("$pid")
        warn "cleanup: pid $pid did not exit within ${grace}s; left running (retained, not killed)"
      else
        warn "cleanup: pid $pid exited with status $st"
      fi
    fi
  done
  OWNED_PIDS=()
  if (( ${#retained[@]} > 0 )); then
    warn "cleanup: retained running PIDs: ${retained[*]} -- left running by policy; review manually"
  fi
  return "$rc"
}

# on_exit preserves the script's own exit code; a cleanup failure downgrades a
# 0 into 1 but never masks an existing non-zero code.
on_exit() {
  local rc=$?
  trap - EXIT
  local cleanup_rc=0
  if ! stop_owned; then cleanup_rc=1; fi
  if ! stop_stack; then cleanup_rc=1; fi
  if (( cleanup_rc != 0 )); then
    warn "cleanup did not complete cleanly"
    if (( rc == 0 )); then rc=1; fi
  fi
  # public/run-exit.txt: this run's own outcome (written on every exit path
  # once the evidence root exists; carries no secret material).
  if [[ -n "$PUBLIC_DIR" && -d "$PUBLIC_DIR" ]]; then
    {
      printf 'exit_code=%s\n' "$rc"
      printf 'cleanup_rc=%s\n' "$cleanup_rc"
      printf 'run_id=%s project=%s\n' "$RUN_ID" "$PROJECT"
      printf 'finished_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "$PUBLIC_DIR/run-exit.txt"
  fi
  if (( rc == 0 )); then
    # PASS is printed only after the whole run, cleanup included, succeeded.
    if (( ${RUN_OK:-0} == 1 )); then printf 'RESULT: PASS\n'; fi
  else
    printf 'RESULT: FAIL exit=%s\n' "$rc" >&2
  fi
  exit "$rc"
}
trap on_exit EXIT

# --- preflight ------------------------------------------------------------
# Mandatory order: identity check (step 0) -> refuse-only guards (RPC, docker)
# -> sanitize -> tool dir/evidence root/run lock -> resource/port/render
# checks. No docker daemon write, no chain call and no database call happens
# before every guard passed; a refused RPC/docker input leaves no file side
# effect at all (the evidence root is created only after the guards).
preflight() {
  # 0) Identity first: the run id must be a safe path/project segment; the
  #    evidence root is only created after the refuse-only guards below.
  #    EVIDENCE_BASE is the FIXED repo-ignored base (no caller override).
  require_safe_run_id "$RUN_ID"
  # 0a) Principal guard: pure (no daemon, no CLI, no evidence). A signer
  #     principal that is not the authorization caller would be refused by 009
  #     as authorization_invalid only after the intent reached the signer, so
  #     the mismatch is refused here instead.
  require_signer_principal "$CALLER_ID" "$SIGNER_CALLER_ID" "$ISSUER_CALLER_ID" \
    || fail "refused: signer principal must equal the authorization caller $CALLER_ID (009 ownership); no side effect was performed"
  EVIDENCE_BASE="$REPO_ROOT/.evidence/portfolio-demo"
  EVIDENCE_ROOT="$EVIDENCE_BASE/$RUN_ID"
  # Per-run ownership token: exported (the compose overlay labels every
  # container with it) and recorded in the public summary/resources.
  RUN_TOKEN=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
  if [[ ! "$RUN_TOKEN" =~ ^[0-9a-f]{32}$ ]]; then
    fail "cannot generate a run token"
  fi
  export PORTFOLIO_RUN_TOKEN="$RUN_TOKEN"
  command -v timeout >/dev/null 2>&1 \
    || fail "refused: GNU coreutils 'timeout' is required on the host"
  log "preflight: run_id=$RUN_ID project=$PROJECT evidence=$EVIDENCE_ROOT"
  # 1) No RPC override input: refuse before any connection (N3).
  if [[ -n "${TXHARBOR_RPC_URL:-}" ]]; then
    fail "refused: TXHARBOR_RPC_URL is set; the demo takes no RPC override input and made no connection"
  fi
  # 2) Docker guards: inspect only; never `docker context use`; never a write
  #    to a caller-supplied remote daemon (N4). DOCKER_CONFIG is refused as
  #    well: a caller-supplied client config could inject a plugin/credential
  #    helper into the very CLI that drives the stack.
  local v
  for v in DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH DOCKER_CONFIG; do
    if [[ -n "${!v:-}" ]]; then
      fail "refused: $v is set; the demo runs only against a local unix-socket docker context"
    fi
  done
  local ctx endpoint
  ctx="${DOCKER_CONTEXT:-}"
  if [[ -z "$ctx" ]]; then
    ctx=$(with_timeout 15 docker context show) || fail "docker context show failed"
  fi
  endpoint=$(with_timeout 15 docker context inspect "$ctx" \
    --format '{{.Endpoints.docker.Host}}') || fail "docker context inspect $ctx failed"
  case "$endpoint" in
    unix://*) ;;
    *) fail "refused: docker context $ctx endpoint [$endpoint] is not a local unix socket" ;;
  esac
  # The inspected context is pinned into the argv below: step 3 unsets every
  # caller DOCKER_*/COMPOSE_* variable, so nothing can re-point the run at
  # another daemon, project or file set after this point.
  DOCKER=(docker --context "$ctx")
  COMPOSE=("${DOCKER[@]}" compose -p "$PROJECT" --env-file /dev/null
           -f "$REPO_ROOT/compose.yaml"
           -f "$REPO_ROOT/scripts/portfolio-demo/compose.portfolio-demo.yaml")
  CAST=("${COMPOSE[@]}" exec -T anvil cast)

  # 3) Sanitize the caller environment AFTER the guards (names only are
  #    logged). The repo `.env` is never sourced. Everything the run derives
  #    from the caller is removed here -- TXHARBOR_* inputs, every
  #    COMPOSE_*/DOCKER_* override (the compose argv above already pins the
  #    inspected context) plus every proxy/curl hook. PORTFOLIO_* is then set
  #    by this script only.
  local name names=""
  for name in $(compgen -v | grep -E '^(TXHARBOR_|COMPOSE_|DOCKER_)' | sort -u); do
    names="$names $name"
  done
  for name in http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY \
              NO_PROXY no_proxy CURL_HOME CURL_CA_BUNDLE CURL_SSL_BACKEND; do
    if [[ -n "${!name:-}" ]]; then
      names="$names $name"
    fi
  done
  if [[ -n "$names" ]]; then
    log "sanitize: unsetting caller vars:$names"
    for name in $names; do
      unset "$name"
    done
  fi
  # Ports are re-exported from this script's own constants, so the overlay's
  # PORTFOLIO_* knobs cannot be redirected away from the ports that are
  # probed and asserted below.
  export PORTFOLIO_PG_PORT="$PG_PORT" PORTFOLIO_ANVIL_PORT="$ANVIL_PORT"
  log "sanitize: PORTFOLIO_PG_PORT=$PORTFOLIO_PG_PORT PORTFOLIO_ANVIL_PORT=$PORTFOLIO_ANVIL_PORT pinned"

  # 4) Tool dir + evidence root + run lock are created only now, after every
  #    refuse-only guard above passed: a refused N3/N4 run leaves no file side
  #    effect at all. The base is fixed and repo-ignored; the root is created
  #    atomically (plain mkdir, refusing any existing path); the lock refuses a
  #    concurrent run with the same id (TOCTOU guard).
  validate_tools_dir
  prepare_evidence_root
  mkdir -m 700 -p "$EVIDENCE_BASE/.locks" || fail "cannot create $EVIDENCE_BASE/.locks"
  if ! mkdir -m 700 "$EVIDENCE_BASE/.locks/$PROJECT"; then
    fail "refused: run lock already held: $EVIDENCE_BASE/.locks/$PROJECT (another run with this id is active)"
  fi

  # 5) Refuse pre-existing project resources: by label AND by actual name, so
  #    a leftover collision is caught even when it carries no demo label.
  local found
  found=$(with_timeout 15 "${DOCKER[@]}" ps -aq \
    --filter "label=com.docker.compose.project=$PROJECT")
  if [[ -n "$found" ]]; then
    fail "refused: project $PROJECT already has containers: $found"
  fi
  found=$(with_timeout 15 "${DOCKER[@]}" ps -a --format '{{.Names}}' \
    | grep -E "^${PROJECT}-" || true)
  if [[ -n "$found" ]]; then
    fail "refused: container name ${PROJECT}-* already exists: $found"
  fi
  found=$(with_timeout 15 "${DOCKER[@]}" network ls -q \
    --filter "name=^${PROJECT}_default$")
  if [[ -n "$found" ]]; then
    fail "refused: network ${PROJECT}_default already exists"
  fi
  found=$(with_timeout 15 "${DOCKER[@]}" volume ls -q --filter "name=^${PROJECT}_")
  if [[ -n "$found" ]]; then
    fail "refused: volume(s) named ${PROJECT}_* already exist: $found"
  fi

  # 6) Ports must be free (bash /dev/tcp probe only).
  local port
  for port in "$PG_PORT" "$ANVIL_PORT" "$HTTP_PORT" "$SIGNER_PORT"; do
    if ! port_free "$port"; then
      fail "refused: 127.0.0.1:$port is already in use"
    fi
  done

  # 7) The rendered config is checked per service against this run's own
  #    project name, image pins, published host ports, anvil command and the
  #    pgdata volume name -- a redirect here is caught before `up`.
  local rendered rendered_name rendered_ports anvil_block pg_block want
  rendered=$(with_timeout 30 "${COMPOSE[@]}" config --format json) \
    || fail "compose config render failed"
  rendered_name=$(printf '%s\n' "$rendered" \
    | sed -n 's/^[[:space:]]*"name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
  expect_eq "compose project name" "$PROJECT" "$rendered_name"
  # The exact published host-port set: only the two baseline services are
  # rendered (profile-gated redis/kafka are not active in this render), so the
  # set must be exactly this run's PG/Anvil ports -- a caller redirect is
  # caught here before `up`.
  rendered_ports=$(printf '%s\n' "$rendered" \
    | grep -o '"published"[[:space:]]*:[[:space:]]*"\?[0-9]\{1,5\}"\?' \
    | grep -o '[0-9]\{1,5\}' | sort -u || true)
  expect_eq "compose published host ports" \
    "$(printf '%s\n' 55436 58547 | sort -u)" "$rendered_ports"
  anvil_block=$(compose_service_block "$rendered" anvil)
  pg_block=$(compose_service_block "$rendered" postgres)
  if [[ -z "$anvil_block" || -z "$pg_block" ]]; then
    fail "compose config render has no anvil/postgres service object"
  fi
  if ! printf '%s\n' "$pg_block" | grep -Fq '"image": "postgres:18.6-trixie"'; then
    fail "compose config postgres image is not postgres:18.6-trixie"
  fi
  if ! printf '%s\n' "$anvil_block" \
    | grep -Fq '"image": "ghcr.io/foundry-rs/foundry:v1.8.1"'; then
    fail "compose config anvil image is not ghcr.io/foundry-rs/foundry:v1.8.1"
  fi
  for want in '"--chain-id"' '"31337"'; do
    if ! printf '%s\n' "$anvil_block" | grep -Fq -- "$want"; then
      fail "compose config anvil command does not carry $want"
    fi
  done
  if ! printf '%s\n' "$rendered" | grep -Fq "\"${PROJECT}_pgdata\""; then
    fail "compose config does not carry the scoped volume name ${PROJECT}_pgdata"
  fi
  log "preflight: guards passed; no run resource created yet"
}

# --- setup ----------------------------------------------------------------
setup() {
  # Tools are built per run: $TOOLS_DIR/runs/$RUN_ID/bin. The shared
  # $TOOLS_DIR/bin is never used or reused, so a stale binary from an earlier
  # run can never execute, and a symlinked path is refused outright.
  RUN_BIN="$TOOLS_DIR/runs/$RUN_ID/bin"
  log "== setup: build demo tools into $RUN_BIN"
  if [[ -L "$TOOLS_DIR/runs" || -L "$TOOLS_DIR/runs/$RUN_ID" || -L "$RUN_BIN" ]]; then
    fail "refused: per-run tool path is a symlink: $RUN_BIN"
  fi
  mkdir -m 700 -p "$RUN_BIN" || fail "cannot create $RUN_BIN"
  BIN="$RUN_BIN/txharbor"
  DEMOCTL="$RUN_BIN/democtl"
  ( cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/txharbor ) \
    || fail "go build ./cmd/txharbor failed"
  ( cd "$REPO_ROOT" && go build -o "$DEMOCTL" ./scripts/portfolio-demo/democtl ) \
    || fail "go build ./scripts/portfolio-demo/democtl failed"

  log "== setup: compose up postgres + anvil (project $PROJECT)"
  STACK_STARTED=1
  with_timeout 300 "${COMPOSE[@]}" up -d --wait --wait-timeout 180 postgres anvil \
    || fail "compose up failed"

  # Ownership: exactly one container per service, selected by project AND by
  # this run's token label, and the label is then read back from the container
  # itself -- the ID is only captured once ownership is proven.
  local svc id_list id n token
  for svc in postgres anvil; do
    id_list=$(with_timeout 15 "${DOCKER[@]}" ps -q \
      --filter "label=com.docker.compose.project=$PROJECT" \
      --filter "label=txharbor.demo.run_token=$RUN_TOKEN" \
      --filter "label=com.docker.compose.service=$svc")
    local -a ids=()
    mapfile -t ids <<<"$id_list"
    n=0
    for id in "${ids[@]}"; do
      if [[ -n "$id" ]]; then n=$((n + 1)); fi
    done
    expect_eq "$svc container count (project+run_token)" "1" "$n"
    CONTAINER_IDS[$svc]="${ids[0]}"
    token=$(with_timeout 15 "${DOCKER[@]}" inspect \
      --format '{{index .Config.Labels "txharbor.demo.run_token"}}' \
      --type container "${ids[0]}")
    expect_eq "$svc container run_token label" "$RUN_TOKEN" "$token"
    CONTAINER_NAMES[$svc]=$(with_timeout 15 "${DOCKER[@]}" inspect \
      --format '{{.Name}}' --type container "${ids[0]}" | sed 's|^/||')
  done
  expect_eq "postgres host port" "127.0.0.1:$PG_PORT" \
    "$(with_timeout 15 "${COMPOSE[@]}" port postgres 5432)"
  expect_eq "anvil host port" "127.0.0.1:$ANVIL_PORT" \
    "$(with_timeout 15 "${COMPOSE[@]}" port anvil 8545)"

  # Freshness: empty demo DB, block 0, chain 31337, unlocked demo accounts.
  expect_eq "fresh DB user tables" "0" \
    "$(psql_ro "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')")"
  expect_eq "fresh chain block number" "0" "$(cast_rpc block-number)"
  expect_eq "chain id" "31337" "$(cast_rpc chain-id)"
  local accounts
  accounts=$(cast_rpc rpc eth_accounts)
  DEPLOYER=$(lower_hex "$(printf '%s\n' "$accounts" | json_get '[0]')")
  WATCH=$(lower_hex "$(printf '%s\n' "$accounts" | json_get '[1]')")
  RECIPIENT=$(lower_hex "$(printf '%s\n' "$accounts" | json_get '[2]')")
  require_addr "deployer" "$DEPLOYER"
  require_addr "watch" "$WATCH"
  require_addr "recipient" "$RECIPIENT"

  # Freshness gates passed -> first real writes (token deploy, then migrate).
  # The token is deployed before `migrate up` because serve.env carries the
  # deployed token/watch addresses and every CLI command loads the full serve
  # env (config.Load); both steps still happen only after the gates above.
  log "== setup: verify the token fixture before deploying it"
  # compile_token.sh --verify: fixture hashes + embedded source copy + settings.
  # It points at THIS run's tool dir (freshly built democtl), so the run can
  # never verify with a stale parser from the shared tools dir.
  TXHARBOR_DEMO_TOOLS_DIR="$TOOLS_DIR/runs/$RUN_ID" \
    bash "$SCRIPT_DIR/compile_token.sh" --verify \
    || fail "compile_token.sh --verify failed (fixture hash, embedded source or settings)"
  require_file "token creation bytecode" \
    "scripts/portfolio-demo/fixtures/MinimalERC20.creation.bin"

  log "== setup: deploy the MinimalERC20 fixture on the fresh chain"
  local creation out
  creation=$(tr -d '[:space:]' < "scripts/portfolio-demo/fixtures/MinimalERC20.creation.bin")
  creation="${creation#0x}"
  if [[ ! "$creation" =~ ^[0-9a-fA-F]+$ ]]; then
    fail "creation bytecode is not hex; run compile_token.sh --compile first"
  fi
  out=$(cast_rpc send --unlocked --from "$DEPLOYER" --create "0x$creation" --json) \
    || fail "token deployment failed"
  DEPLOY_TX=$(printf '%s\n' "$out" | json_get transactionHash)
  expect_eq "deploy tx status" "0x1" "$(printf '%s\n' "$out" | json_get status)"
  TOKEN=$(lower_hex "$(printf '%s\n' "$out" | json_get contractAddress)")
  require_addr "token" "$TOKEN"
  expect_uint_eq "totalSupply == SUPPLY" "$SUPPLY" \
    "$(cast_uint "$TOKEN" "totalSupply()(uint256)")"
  expect_uint_eq "deployer balance == SUPPLY" "$SUPPLY" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$DEPLOYER")"
  {
    printf 'token=%s\n' "$TOKEN"
    printf 'deploy_tx=%s\n' "$DEPLOY_TX"
    printf 'deployer=%s watch=%s recipient=%s\n' "$DEPLOYER" "$WATCH" "$RECIPIENT"
  } > "$PRIVATE_DIR/logs/token-deploy.txt"

  log "== setup: write 0600 env files (private/env)"
  cat > "$ENV_SERVE" <<EOF
TXHARBOR_PG_DSN='postgres://txharbor:txharbor@127.0.0.1:$PG_PORT/txharbor?sslmode=disable'
TXHARBOR_RPC_URL='http://127.0.0.1:$ANVIL_PORT'
TXHARBOR_CHAIN_ID=31337
TXHARBOR_START_HEIGHT=0
TXHARBOR_LOG_START_HEIGHT=0
TXHARBOR_DEPOSIT_START_HEIGHT=0
TXHARBOR_LOG_CONTRACTS='$TOKEN'
TXHARBOR_DEPOSIT_CONTRACTS='$TOKEN:0'
TXHARBOR_DEPOSIT_WATCH_ADDRESSES='$WATCH:0'
TXHARBOR_CONFIRMATION_DEPTH=$CONFIRM_DEPTH
TXHARBOR_REORG_MAX_DEPTH=64
TXHARBOR_HTTP_ADDR='127.0.0.1:$HTTP_PORT'
TXHARBOR_AUTHZ_ISSUER_CALLERS=$ISSUER_CALLER_ID
EOF
  chmod 600 "$ENV_SERVE"

  log "== setup: migrate up + status"
  with_env "$ENV_SERVE" "$BIN" migrate up > "$PRIVATE_DIR/logs/migrate-up.log" 2>&1 \
    || fail "migrate up failed (see private/logs/migrate-up.log)"
  with_env "$ENV_SERVE" "$BIN" migrate status > "$PRIVATE_DIR/logs/migrate-status.log" 2>&1 \
    || fail "migrate status failed (see private/logs/migrate-status.log)"
  if ! grep -qx 'pending=none' "$PRIVATE_DIR/logs/migrate-status.log"; then
    fail "migrate status does not report 'pending=none' (see private/logs/migrate-status.log)"
  fi

  log "== setup: one-time signer key (cast wallet new, anvil image)"
  # Capture BOTH streams into private 0600 files (the human stderr lines also
  # carry the key) and parse only the machine pair from stdout; nothing from
  # the command reaches this script's stdout/stderr.
  wallet_new_capture "$PRIVATE_DIR/logs/wallet-new.out" "$PRIVATE_DIR/logs/wallet-new.err" \
    || fail "cast wallet new failed (private capture: $PRIVATE_DIR/logs/wallet-new.out|.err)"
  wallet_parse_pair "$PRIVATE_DIR/logs/wallet-new.out" \
    || fail "cast wallet new stdout was not the expected address<TAB>key pair (private capture: $PRIVATE_DIR/logs/wallet-new.out)"
  SIGNER_ADDR="$WALLET_ADDR"
  ( umask 077; printf '%s\n' "$WALLET_KEY" > "$KEY_SIGNER" )
  chmod 600 "$KEY_SIGNER"
  unset WALLET_KEY

  log "== setup: fund the signer with FUND from the deployer"
  local fund_out
  fund_out=$(cast_rpc send --unlocked --from "$DEPLOYER" "$TOKEN" \
    "transfer(address,uint256)" "$SIGNER_ADDR" "$FUND" --json) \
    || fail "funding the signer failed"
  expect_eq "fund tx status" "0x1" "$(printf '%s\n' "$fund_out" | json_get status)"
  expect_uint_eq "signer balance == FUND" "$FUND" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$SIGNER_ADDR")"

  # The signer broadcasts its own transaction, so it needs NATIVE gas money on
  # top of the ERC-20 funding: 1e18 wei from the unlocked deployer, and the
  # balance must then cover one full-fee transfer, i.e. the literal product
  # MAX_GAS_LIMIT * MAX_FEE_PER_GAS = 2000000000000000 wei. No token-balance
  # assertion depends on this step.
  log "== setup: fund the signer with native ETH from the deployer"
  local native_out
  native_out=$(cast_rpc send --unlocked --from "$DEPLOYER" "$SIGNER_ADDR" \
    --value 1000000000000000000 --json) \
    || fail "native ETH funding of the signer failed"
  expect_eq "native fund tx status" "0x1" \
    "$(printf '%s\n' "$native_out" | json_get status)"
  NATIVE_FUND_TX=$(printf '%s\n' "$native_out" | json_get transactionHash)
  require_nonempty "native fund tx hash" "$NATIVE_FUND_TX"
  SIGNER_NATIVE_BALANCE=$(cast_balance "$SIGNER_ADDR")
  expect_uint_ge "signer native balance >= MAX_GAS_LIMIT*MAX_FEE_PER_GAS" \
    "$SIGNER_NATIVE_BALANCE" "2000000000000000"
  {
    printf 'signer_native_fund_tx=%s\n' "$NATIVE_FUND_TX"
    printf 'signer_native_funded_value=%s\n' "1000000000000000000"
    printf 'signer_native_balance_after=%s\n' "$SIGNER_NATIVE_BALANCE"
  } >> "$PRIVATE_DIR/logs/token-deploy.txt"

  # signer-serve loads the SAME config.Load as serve/migrate, so its env file is
  # the serve key set plus the 009 signer policy keys (a bare TXHARBOR_SIGNER_*
  # file would be refused as a configuration error).
  {
    cat "$ENV_SERVE"
    cat <<EOF
TXHARBOR_SIGNER_MODE=development
TXHARBOR_SIGNER_KEY_FILE='$KEY_SIGNER'
TXHARBOR_SIGNER_CHAINS=31337
TXHARBOR_SIGNER_SENDERS='$SIGNER_ADDR'
TXHARBOR_SIGNER_ASSETS='$TOKEN'
TXHARBOR_SIGNER_RECIPIENTS='$RECIPIENT'
TXHARBOR_SIGNER_MAX_AMOUNT=$MAX_AMOUNT
TXHARBOR_SIGNER_MAX_GAS_LIMIT=$MAX_GAS_LIMIT
TXHARBOR_SIGNER_MAX_FEE_PER_GAS=$MAX_FEE_PER_GAS
TXHARBOR_SIGNER_MAX_PRIORITY_FEE_PER_GAS=$MAX_PRIORITY_FEE_PER_GAS
TXHARBOR_SIGNER_MAX_GAS_PRICE=$MAX_GAS_PRICE
TXHARBOR_SIGNER_HTTP_ADDR='127.0.0.1:$SIGNER_PORT'
EOF
  } > "$ENV_SIGNER"
  chmod 600 "$ENV_SIGNER"
  # Worker env = the serve keys plus the signer endpoint and the scan cadence.
  # The signer credential is deliberately NOT in this file: it is injected at
  # process start from the 0600 credential file.
  {
    cat "$ENV_SERVE"
    printf "TXHARBOR_TX_SIGNER_URL='http://127.0.0.1:%s'\n" "$SIGNER_PORT"
    printf 'TXHARBOR_WORKER_SCAN_INTERVAL_MS=1000\n'
  } > "$ENV_WORKER"
  chmod 600 "$ENV_WORKER"

  # BEGIN demo-credentials-block
  # Extracted verbatim by scripts/portfolio-demo/checks/principal/run_checks.sh
  # and executed against a stub CLI: the mock regression asserts the REAL argv
  # (--caller-id 7001 for both the API key and the signer credential).
  log "== setup: issue caller/issuer API keys (private/keys)"
  CALLER_KEY=$(cli_secret "apikey-auth issue caller $CALLER_ID" \
    "$BIN" apikey-auth issue --caller-id "$CALLER_ID" --label demo-caller \
    --operator op --reason demo)
  require_nonempty "caller key" "$CALLER_KEY"
  printf '%s\n' "$CALLER_KEY" > "$KEY_CALLER"
  chmod 600 "$KEY_CALLER"
  ISSUER_KEY=$(cli_secret "apikey-auth issue issuer $ISSUER_CALLER_ID" \
    "$BIN" apikey-auth issue --caller-id "$ISSUER_CALLER_ID" --label demo-issuer \
    --operator op --reason demo)
  require_nonempty "issuer key" "$ISSUER_KEY"
  printf '%s\n' "$ISSUER_KEY" > "$KEY_ISSUER"
  chmod 600 "$KEY_ISSUER"

  cli_run "withdrawal-exec permission-set $CALLER_ID op-perm-1" \
    "$BIN" withdrawal-exec permission-set --caller-id "$CALLER_ID" \
    --can-execute=true --operation-id op-perm-1 --operator op --reason demo > /dev/null
  SIGNER_CRED=$(cli_secret "signer-auth issue $SIGNER_CALLER_ID" \
    "$BIN" signer-auth issue --caller-id "$SIGNER_CALLER_ID" --label demo-signer \
    --operator op --reason demo)
  require_nonempty "signer credential" "$SIGNER_CRED"
  printf '%s\n' "$SIGNER_CRED" > "$KEY_SIGNER_CRED"
  chmod 600 "$KEY_SIGNER_CRED"
  cli_run "signer-auth set-can-sign $SIGNER_CALLER_ID" \
    "$BIN" signer-auth set-can-sign --caller-id "$SIGNER_CALLER_ID" \
    --can-sign true --operator op --reason demo > /dev/null
  # END demo-credentials-block

  log "== setup: nonce registry + upstream grant (mint/register/supply)"
  NONCE_OP=$(cli_run "nonce-admin mint" "$BIN" nonce-admin mint)
  if [[ ! "$NONCE_OP" =~ ^[0-9a-f]{32}$ ]]; then
    fail "nonce-admin mint did not print a 32-hex operation id: [$NONCE_OP]"
  fi
  cli_run "nonce-admin register $SIGNER_ADDR" \
    "$BIN" nonce-admin register --operation-id "$NONCE_OP" --chain-id 31337 \
    --sender "$SIGNER_ADDR" --operator op --reason demo > /dev/null
  AUTHZ_OP=$(cli_run "withdrawal-authz mint" "$BIN" withdrawal-authz mint)
  if [[ ! "$AUTHZ_OP" =~ ^[0-9a-f]{32}$ ]]; then
    fail "withdrawal-authz mint did not print a 32-hex operation id: [$AUTHZ_OP]"
  fi
  cli_run "withdrawal-authz supply $AUTH_ID" \
    "$BIN" withdrawal-authz supply --operation-id "$AUTHZ_OP" \
    --authorization-id "$AUTH_ID" --caller-id "$CALLER_ID" --chain-id 31337 \
    --asset "$TOKEN" --recipient "$RECIPIENT" --amount "$WITHDRAW" \
    --api-key-file "$KEY_ISSUER" --operator op --reason demo > /dev/null

  # Read back the durable grant owner (read-only psql; never a second
  # configuration write): the signer will authenticate as SIGNER_CALLER_ID and
  # 009 refuses a grant owned by another caller as authorization_invalid.
  GRANT_CALLER=$(psql_ro "SELECT caller_id FROM withdrawal_authorizations WHERE authorization_id='$AUTH_ID'") \
    || fail "cannot read the grant owner from withdrawal_authorizations"
  require_grant_principal "$GRANT_CALLER" "$SIGNER_CALLER_ID" \
    || fail "grant principal mismatch: observed caller_id [$GRANT_CALLER], signer principal [$SIGNER_CALLER_ID]"
  log "ok: grant owner == signer principal == $SIGNER_CALLER_ID"

  log "== setup: start serve + signer via start_detached (logs -> private/logs)"
  # stdout (the echoed PID) is discarded because the PID is taken from
  # STARTED_PID: only a PID owned by THIS shell can be `wait`ed for later.
  start_detached "$ENV_SERVE" "$PRIVATE_DIR/logs/serve.log" -- "$BIN" serve > /dev/null \
    || fail "cannot start serve"
  SERVE_PID=$STARTED_PID
  OWNED_PIDS+=("$SERVE_PID")
  start_detached "$ENV_SIGNER" "$PRIVATE_DIR/logs/signer.log" -- "$BIN" signer-serve > /dev/null \
    || fail "cannot start signer-serve"
  SIGNER_PID=$STARTED_PID
  OWNED_PIDS+=("$SIGNER_PID")
  log "ok: serve pid=$SERVE_PID signer pid=$SIGNER_PID"

  # serve readiness: /readyz answers 200. Every poll also re-checks that both
  # started services are still alive (a death is reported with its private log
  # path -- private logs are never echoed to the terminal).
  local deadline=$((SECONDS + 60)) code
  while :; do
    require_services_alive
    code=$(probe_http_code "http://127.0.0.1:$HTTP_PORT/readyz")
    if [[ "$code" == "200" ]]; then break; fi
    if (( SECONDS >= deadline )); then
      fail "serve did not become ready within 60s; private log: $PRIVATE_DIR/logs/serve.log"
    fi
    sleep 1
  done
  log "ok: serve ready on 127.0.0.1:$HTTP_PORT"

  # signer readiness: TCP port open (signer-serve exposes no HTTP probe).
  deadline=$((SECONDS + 60))
  while :; do
    require_services_alive
    if ! port_free "$SIGNER_PORT"; then break; fi
    if (( SECONDS >= deadline )); then
      fail "signer did not open 127.0.0.1:$SIGNER_PORT within 60s; private log: $PRIVATE_DIR/logs/signer.log"
    fi
    sleep 1
  done
  log "ok: signer listening on 127.0.0.1:$SIGNER_PORT"
}

# --- stage A: real deposit -> explicit confirmation depth -------------------
stage_a() {
  log "== stage A: real ERC-20 deposit, pending -> N=$CONFIRM_DEPTH confirmed"
  local out tx n deadline
  out=$(cast_rpc send --unlocked --from "$DEPLOYER" "$TOKEN" \
    "transfer(address,uint256)" "$WATCH" "$DEPOSIT" --json) \
    || fail "deposit transfer failed"
  expect_eq "deposit tx status" "0x1" "$(printf '%s\n' "$out" | json_get status)"
  DEPOSIT_TX=$(printf '%s\n' "$out" | json_get transactionHash)
  tx="$DEPOSIT_TX"

  deadline=$((SECONDS + 120))
  while :; do
    require_services_alive
    n=$(psql_ro "SELECT count(*) FROM deposit_observations WHERE tx_hash='$tx' AND contract='$TOKEN' AND status='pending'")
    if [[ "$n" == "1" ]]; then break; fi
    if (( SECONDS >= deadline )); then
      fail "no pending deposit_observations row for $tx within 120s (count=$n)"
    fi
    sleep 2
  done
  log "ok: deposit observed as pending (tx $tx)"
  public_sql deposit-pending \
    "SELECT status, block_number, confirm_tip_number, confirm_tip_hash, confirmations, confirm_threshold, confirm_policy_seq, confirmed_at FROM deposit_observations WHERE tx_hash='$tx'"

  expect_uint_eq "watch balance == DEPOSIT" "$DEPOSIT" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$WATCH")"
  expect_uint_eq "deployer balance == DEPLOYER_FINAL" "$DEPLOYER_FINAL" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$DEPLOYER")"

  # Depth N-1 (one block short of the threshold): still pending.
  cast_rpc rpc anvil_mine 0x1 > /dev/null
  sleep 2
  expect_eq "status at depth N-1" "pending" \
    "$(psql_ro "SELECT status FROM deposit_observations WHERE tx_hash='$tx'")"
  public_sql deposit-depth-n1 \
    "SELECT status, block_number, confirm_tip_number, confirm_tip_hash, confirmations, confirm_threshold, confirm_policy_seq, confirmed_at FROM deposit_observations WHERE tx_hash='$tx'"

  # Depth N: the confirmation commit lands.
  cast_rpc rpc anvil_mine 0x1 > /dev/null
  deadline=$((SECONDS + 120))
  while :; do
    require_services_alive
    n=$(psql_ro "SELECT count(*) FROM deposit_observations WHERE tx_hash='$tx' AND status='confirmed'")
    if [[ "$n" == "1" ]]; then break; fi
    if (( SECONDS >= deadline )); then
      fail "deposit not confirmed within 120s after depth N (count=$n)"
    fi
    sleep 2
  done
  expect_eq "confirm_threshold" "$CONFIRM_DEPTH" \
    "$(psql_ro "SELECT confirm_threshold FROM deposit_observations WHERE tx_hash='$tx'")"
  expect_uint_ge "confirmations >= CONFIRM_DEPTH" \
    "$(psql_ro "SELECT confirmations FROM deposit_observations WHERE tx_hash='$tx'")" \
    "$CONFIRM_DEPTH"
  expect_eq "confirm_policy_seq" "1" \
    "$(psql_ro "SELECT confirm_policy_seq FROM deposit_observations WHERE tx_hash='$tx'")"
  public_sql deposit-confirmed \
    "SELECT status, block_number, confirm_tip_number, confirm_tip_hash, confirmations, confirm_threshold, confirm_policy_seq, confirmed_at FROM deposit_observations WHERE tx_hash='$tx'"
  DEPOSIT_BLOCK_NUMBER=$(psql_ro "SELECT block_number FROM deposit_observations WHERE tx_hash='$tx'")
  DEPOSIT_BLOCK_HASH=$(lower_hex "$(psql_ro "SELECT block_hash FROM deposit_observations WHERE tx_hash='$tx'")")
  require_nonempty "deposit DB block_number" "$DEPOSIT_BLOCK_NUMBER"
  require_nonempty "deposit DB block_hash" "$DEPOSIT_BLOCK_HASH"
  printf 'deposit_tx=%s\n' "$tx" > "$PRIVATE_DIR/logs/stage-a.txt"
  log "ok: deposit confirmed at depth N=$CONFIRM_DEPTH"
}

# --- stage B: create -> admission -> real execution -> replays -------------
stage_b() {
  log "== stage B: create -> admission -> real signer/worker execution -> replay"
  local conf_create="$PRIVATE_DIR/curl/create.conf"
  local conf_admit="$PRIVATE_DIR/curl/admit.conf"
  local conf_nobearer="$PRIVATE_DIR/curl/admit-nobearer.conf"
  local body_create="$PRIVATE_DIR/curl/create.json"
  local body_conflict="$PRIVATE_DIR/curl/create-conflict.json"
  # HTTP responses are public evidence (status-code evidence only; the scan at
  # the end of verify re-checks them for secret material).
  local http_dir="$PUBLIC_DIR/http"
  local code
  mkdir -m 700 -p "$http_dir" || fail "cannot create $http_dir"

  cat > "$body_create" <<EOF
{"idempotency_key":"$IDEM","chain_id":31337,"asset":"$TOKEN","recipient":"$RECIPIENT","amount":"$WITHDRAW","authorization_id":"$AUTH_ID"}
EOF
  cat > "$body_conflict" <<EOF
{"idempotency_key":"$IDEM","chain_id":31337,"asset":"$TOKEN","recipient":"$RECIPIENT","amount":"$CONFLICT_AMOUNT","authorization_id":"$AUTH_ID"}
EOF
  cat > "$conf_create" <<EOF
url = "http://127.0.0.1:$HTTP_PORT/withdrawals"
request = "POST"
header = "Authorization: Bearer $CALLER_KEY"
EOF
  chmod 600 "$body_create" "$body_conflict" "$conf_create"

  log "-- B1: POST /withdrawals (bearer from the 0600 curl config)"
  code=$(http_post "$conf_create" "$body_create" "$http_dir/create-201.json")
  expect_eq "create HTTP status" "201" "$code"
  REQUEST_ID=$(json_get request_id < "$http_dir/create-201.json")
  require_nonempty "request_id" "$REQUEST_ID"
  expect_eq "withdrawal_requests rows after create" "1" \
    "$(psql_ro 'SELECT count(*) FROM withdrawal_requests')"

  cat > "$conf_admit" <<EOF
url = "http://127.0.0.1:$HTTP_PORT/withdrawals/$REQUEST_ID/execution"
request = "POST"
header = "Authorization: Bearer $CALLER_KEY"
EOF
  cat > "$conf_nobearer" <<EOF
url = "http://127.0.0.1:$HTTP_PORT/withdrawals/$REQUEST_ID/execution"
request = "POST"
EOF
  chmod 600 "$conf_admit" "$conf_nobearer"

  log "-- N5a: admission without bearer -> 401, zero intents"
  code=$(http_post "$conf_nobearer" "-" "$http_dir/admit-401.json")
  expect_eq "admission without bearer" "401" "$code"
  expect_eq "intents after 401" "0" \
    "$(psql_ro "SELECT count(*) FROM payment_intents WHERE request_id='$REQUEST_ID'")"

  log "-- N5c: same key, different amount -> 409, original request untouched"
  code=$(http_post "$conf_create" "$body_conflict" "$http_dir/create-409.json")
  expect_eq "conflicting create" "409" "$code"
  expect_eq "requests(caller,key) after 409" "1" \
    "$(psql_ro "SELECT count(*) FROM withdrawal_requests WHERE caller_id=$CALLER_ID AND idempotency_key='$IDEM'")"

  log "-- N5b: can_execute=false -> 403, zero intents; restore with the CLI"
  cli_run "withdrawal-exec permission-set $CALLER_ID op-perm-2 (false)" \
    "$BIN" withdrawal-exec permission-set --caller-id "$CALLER_ID" \
    --can-execute=false --operation-id op-perm-2 --operator op --reason demo > /dev/null
  code=$(http_post "$conf_admit" "-" "$http_dir/admit-403.json")
  expect_eq "admission with can_execute=false" "403" "$code"
  expect_eq "intents after 403" "0" \
    "$(psql_ro "SELECT count(*) FROM payment_intents WHERE request_id='$REQUEST_ID'")"
  cli_run "withdrawal-exec permission-set $CALLER_ID op-perm-3 (true)" \
    "$BIN" withdrawal-exec permission-set --caller-id "$CALLER_ID" \
    --can-execute=true --operation-id op-perm-3 --operator op --reason demo > /dev/null

  # The ONLY SQL write of this run. One transaction: a DO block asserts every
  # fresh-first-supply precondition under FOR SHARE row locks on the grant and
  # the request, then a plain INSERT with the LITERAL authorization_version 1
  # (never max+1) writes exactly one scope row and returns it. Any RAISE --
  # or a missing RETURNING row -- fails the run; ON_ERROR_STOP=1 aborts the
  # whole transaction, so a refused precondition leaves no row behind.
  log "-- B3: fee ceilings are within the signer's own policy limits"
  # Both directions are exact: the scope caps may not exceed the signer's
  # limits, and MAX_GAS_LIMIT * MAX_FEE_PER_GAS is the literal product.
  expect_uint_ge "FEE_PER_GAS <= MAX_FEE_PER_GAS" "$MAX_FEE_PER_GAS" "$FEE_PER_GAS"
  expect_uint_ge "FEE_PRIORITY <= MAX_PRIORITY_FEE_PER_GAS" \
    "$MAX_PRIORITY_FEE_PER_GAS" "$FEE_PRIORITY"
  expect_uint_ge "FEE_TOTAL <= MAX_GAS_LIMIT*MAX_FEE_PER_GAS" \
    "2000000000000000" "$FEE_TOTAL"

  log "-- B3: one controlled scope row (withdrawal_authorization_scopes)"
  local sql_out rows extra
  sql_out=$(psql_script <<SQL
BEGIN;
DO \$\$
DECLARE
  n BIGINT;
BEGIN
  SELECT 1 INTO n
    FROM withdrawal_authorizations g
    JOIN withdrawal_requests r ON r.authorization_id = g.authorization_id
   WHERE g.authorization_id = '$AUTH_ID'
     AND r.request_id = '$REQUEST_ID'
   FOR SHARE OF g, r;
  IF n IS NULL THEN
    RAISE EXCEPTION 'scope precondition: no locked grant+request pair for %/%', '$AUTH_ID', '$REQUEST_ID';
  END IF;
  SELECT count(*) INTO n FROM withdrawal_authorizations g
   WHERE g.authorization_id = '$AUTH_ID'
     AND g.state = 'active'
     AND (g.expires_at IS NULL OR g.expires_at > now())
     AND g.caller_id = $CALLER_ID
     AND g.chain_id = 31337
     AND g.asset = '$TOKEN'
     AND g.recipient = '$RECIPIENT'
     AND g.amount = $WITHDRAW;
  IF n <> 1 THEN
    RAISE EXCEPTION 'scope precondition: grant is not exactly one active, unexpired, matching row (n=%)', n;
  END IF;
  SELECT count(*) INTO n
    FROM withdrawal_requests r
    JOIN withdrawal_authorizations g ON g.authorization_id = r.authorization_id
   WHERE r.request_id = '$REQUEST_ID'
     AND r.authorization_id = '$AUTH_ID'
     AND r.status = 'accepted'
     AND r.caller_id = g.caller_id
     AND r.chain_id = g.chain_id
     AND r.asset = g.asset
     AND r.recipient = g.recipient
     AND r.amount = g.amount;
  IF n <> 1 THEN
    RAISE EXCEPTION 'scope precondition: request is not exactly one accepted row equal to its grant (n=%)', n;
  END IF;
  SELECT count(*) INTO n FROM withdrawal_authorization_scopes s
   WHERE s.authorization_id = '$AUTH_ID';
  IF n <> 0 THEN
    RAISE EXCEPTION 'scope precondition: a scope row already exists for % (n=%)', '$AUTH_ID', n;
  END IF;
  SELECT count(*) INTO n FROM withdrawal_grant_audit a
   WHERE a.authorization_id = '$AUTH_ID' AND a.action = 'supplied';
  IF n <> 1 THEN
    RAISE EXCEPTION 'scope precondition: expected exactly one supplied grant audit row (n=%)', n;
  END IF;
  SELECT count(*) INTO n FROM withdrawal_grant_audit a
   WHERE a.authorization_id = '$AUTH_ID' AND a.action IN ('resupplied', 'revoked');
  IF n <> 0 THEN
    RAISE EXCEPTION 'scope precondition: grant carries a resupply/revocation audit row (n=%)', n;
  END IF;
END
\$\$;
INSERT INTO withdrawal_authorization_scopes
  (authorization_id, intent_id, request_id, sender,
   fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement,
   authorization_version, attested_by)
VALUES ('$AUTH_ID', '$INTENT_ID', '$REQUEST_ID', '$SIGNER_ADDR',
        $FEE_TOTAL, $FEE_PER_GAS, $FEE_PRIORITY, TRUE, 1, 'demo-issuer')
RETURNING authorization_id;
COMMIT;
SQL
) || fail "scope insert failed"
  rows=$(printf '%s\n' "$sql_out" | grep -c -x "$AUTH_ID" || true)
  extra=$(printf '%s\n' "$sql_out" \
    | grep -Ev -x -e '' -e 'BEGIN' -e 'DO' -e 'COMMIT' -e 'INSERT 0 1' -e "$AUTH_ID" \
    | grep -c . || true)
  expect_eq "scope insert RETURNING rows" "1" "$rows"
  expect_eq "scope insert unexpected output lines" "0" "$extra"

  log "-- B2: admission POST -> 201 intent_id == $INTENT_ID"
  code=$(http_post "$conf_admit" "-" "$http_dir/admit-201.json")
  expect_eq "admission HTTP status" "201" "$code"
  expect_eq "admission intent_id" "$INTENT_ID" \
    "$(json_get intent_id < "$http_dir/admit-201.json")"
  expect_eq "payment_intents rows after admission" "1" \
    "$(psql_ro 'SELECT count(*) FROM payment_intents')"

  log "-- B2: immediate replay of the (unfinished) admission -> 200, same intent"
  code=$(http_post "$conf_admit" "-" "$http_dir/admit-200-unfinished.json")
  expect_eq "unfinished admission replay" "200" "$code"
  expect_eq "unfinished replay intent_id" "$INTENT_ID" \
    "$(json_get intent_id < "$http_dir/admit-200-unfinished.json")"

  log "-- B3: start withdrawal-worker (credential exported from the 0600 file; never argv, never logs)"
  require_file "signer credential" "$KEY_SIGNER_CRED"
  start_detached "$ENV_WORKER" "$PRIVATE_DIR/logs/worker.log" \
    TXHARBOR_TX_SIGNER_CREDENTIAL "$KEY_SIGNER_CRED" -- "$BIN" withdrawal-worker > /dev/null \
    || fail "cannot start withdrawal-worker"
  WORKER_PID=$STARTED_PID
  OWNED_PIDS+=("$WORKER_PID")
  log "ok: withdrawal-worker pid=$WORKER_PID"

  log "-- B3: poll payment_intents.state == completed (mining only while a signing exists)"
  local deadline=$((SECONDS + 240)) state mines=0 has_signing
  while :; do
    state=$(psql_ro "SELECT state FROM payment_intents WHERE intent_id='$INTENT_ID'")
    if [[ "$state" == "completed" ]]; then break; fi
    require_services_alive
    has_signing=$(psql_ro "SELECT count(*) FROM tx_attempt_signings s JOIN tx_attempts a ON a.attempt_id = s.attempt_id WHERE a.intent_id='$INTENT_ID'")
    if [[ "$has_signing" != "0" ]] && (( mines < 60 )); then
      if cast_rpc rpc anvil_mine 0x1 > /dev/null; then mines=$((mines + 1)); fi
    fi
    if (( SECONDS >= deadline )); then
      fail "intent did not reach completed within 240s (state=$state, mines=$mines); private log: $PRIVATE_DIR/logs/worker.log"
    fi
    sleep 2
  done
  log "ok: payment intent completed (extra blocks mined=$mines)"

  log "-- B3(b): export the B counts before the replay calls"
  public_sql b-counts-before-replay "SELECT (SELECT count(*) FROM withdrawal_requests) AS requests, (SELECT count(*) FROM payment_intents) AS intents, (SELECT count(*) FROM nonce_bindings) AS bindings, (SELECT count(*) FROM outbox_events) AS outbox_rows"

  log "-- B4: create replay (same key) -> 200, same request_id"
  code=$(http_post "$conf_create" "$body_create" "$http_dir/create-200-replay.json")
  expect_eq "create replay HTTP status" "200" "$code"
  expect_eq "create replay request_id" "$REQUEST_ID" \
    "$(json_get request_id < "$http_dir/create-200-replay.json")"
  log "-- B4: admission replay (completed) -> 200, same intent_id"
  code=$(http_post "$conf_admit" "-" "$http_dir/admit-200-replay.json")
  expect_eq "completed admission replay" "200" "$code"
  expect_eq "completed replay intent_id" "$INTENT_ID" \
    "$(json_get intent_id < "$http_dir/admit-200-replay.json")"
  log "-- B4(b): export the B counts after the replay calls (unchanged)"
  public_sql b-counts-after-replay "SELECT (SELECT count(*) FROM withdrawal_requests) AS requests, (SELECT count(*) FROM payment_intents) AS intents, (SELECT count(*) FROM nonce_bindings) AS bindings, (SELECT count(*) FROM outbox_events) AS outbox_rows"

  log "-- B5: unique request/intent/binding/outbox and a single chain effect"
  expect_eq "withdrawal_requests" "1" "$(psql_ro 'SELECT count(*) FROM withdrawal_requests')"
  expect_eq "payment_intents" "1" "$(psql_ro 'SELECT count(*) FROM payment_intents')"
  expect_eq "nonce_bindings" "1" "$(psql_ro 'SELECT count(*) FROM nonce_bindings')"
  expect_eq "outbox withdrawal.request.received" "1" \
    "$(psql_ro "SELECT count(*) FROM outbox_events WHERE event_type='withdrawal.request.received' AND aggregate_id='$REQUEST_ID'")"
  local state_changed agg_version filtered_attempts total_attempts
  state_changed=$(psql_ro "SELECT count(*) FROM execution_events WHERE intent_id='$INTENT_ID' AND kind='state_changed'")
  agg_version=$(psql_ro "SELECT COALESCE(max(aggregate_version),0) FROM outbox_events WHERE aggregate_type='withdrawal_intent' AND aggregate_id='$INTENT_ID'")
  expect_eq "state_changed count == max aggregate_version" "$agg_version" "$state_changed"
  if (( state_changed < 3 )); then
    fail "execution state_changed count $state_changed < 3"
  fi
  filtered_attempts=$(psql_ro "SELECT count(*) FROM tx_attempts WHERE intent_id='$INTENT_ID' AND nonce=0 AND authorization_version=1")
  if (( filtered_attempts < 1 )); then
    fail "no tx_attempt with nonce 0 / authorization_version 1 (count=$filtered_attempts)"
  fi
  total_attempts=$(psql_ro "SELECT count(*) FROM tx_attempts WHERE intent_id='$INTENT_ID'")
  log "ok: tx_attempts=$total_attempts (nonce 0 / authorization_version 1: $filtered_attempts)"
  SIGNING_TX_HASH=$(psql_ro "SELECT s.tx_hash FROM tx_attempt_signings s JOIN tx_attempts a ON a.attempt_id = s.attempt_id WHERE a.intent_id='$INTENT_ID' AND a.state NOT IN ('replaced','orphaned','ineffective') ORDER BY a.created_at DESC LIMIT 1")
  require_nonempty "signing tx hash" "$SIGNING_TX_HASH"

  expect_uint_eq "recipient balance == WITHDRAW" "$WITHDRAW" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$RECIPIENT")"
  expect_eq "signer nonce" "1" "$(cast_rpc nonce "$SIGNER_ADDR")"

  # One composite snapshot of the durable facts that must stay put: request/
  # intent/binding/outbox/state_changed counts plus the two chain facts.
  # tx_attempt counts are deliberately NOT part of it -- a retry is legitimate
  # retry diagnostics, so it must not be able to fail the stability check.
  # The attempt counts are printed next to the snapshots instead.
  snapshot() {
    printf '%s|%s|%s|%s|%s|%s|%s\n' \
      "$(psql_ro 'SELECT count(*) FROM withdrawal_requests')" \
      "$(psql_ro 'SELECT count(*) FROM payment_intents')" \
      "$(psql_ro 'SELECT count(*) FROM nonce_bindings')" \
      "$(psql_ro "SELECT count(*) FROM outbox_events WHERE event_type='withdrawal.request.received'")" \
      "$(psql_ro "SELECT count(*) FROM execution_events WHERE intent_id='$INTENT_ID' AND kind='state_changed'")" \
      "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$RECIPIENT")" \
      "$(cast_rpc nonce "$SIGNER_ADDR")"
  }
  local snap1 snap2 snap3 st attempts_now
  log "diag: tx_attempts for the intent=$total_attempts (nonce 0/authorization_version 1: $filtered_attempts) -- retry diagnostics, not part of the snapshot"
  snap1=$(snapshot)
  expect_eq "snapshot before the window" \
    "1|1|1|1|$state_changed|$WITHDRAW|1" "$snap1"
  public_sql b-outbox-aggregate-versions "SELECT aggregate_version, event_type FROM outbox_events WHERE aggregate_type='withdrawal_intent' AND aggregate_id='$INTENT_ID' ORDER BY aggregate_version"

  log "-- B6: bounded 30s observation with the worker still running"
  sleep 30
  if ! pid_alive "$WORKER_PID"; then
    fail "withdrawal-worker died during the observation window; private log: $PRIVATE_DIR/logs/worker.log"
  fi
  snap2=$(snapshot)
  expect_eq "snapshot after the 30s window" "$snap1" "$snap2"
  attempts_now=$(psql_ro "SELECT count(*) FROM tx_attempts WHERE intent_id='$INTENT_ID'")
  log "diag: tx_attempts after the 30s window=$attempts_now -- diagnostics only, not compared"

  log "-- B6: stop the worker gracefully and re-check the stable state"
  if graceful_stop "$WORKER_PID" "withdrawal-worker" 30; then st=0; else st=$?; fi
  untrack_pid "$WORKER_PID"
  expect_eq "worker graceful exit status" "0" "$st"
  snap3=$(snapshot)
  expect_eq "snapshot after the worker stopped" "$snap1" "$snap3"
  mkdir -m 700 -p "$PUBLIC_DIR/sql" || fail "cannot create $PUBLIC_DIR/sql"
  printf '%s\n' "$snap1" > "$PUBLIC_DIR/sql/b-window-before.out"
  printf '%s\n' "$snap2" > "$PUBLIC_DIR/sql/b-window-after.out"
  printf '%s\n' "$snap3" > "$PUBLIC_DIR/sql/b-window-after-stop.out"
  attempts_now=$(psql_ro "SELECT count(*) FROM tx_attempts WHERE intent_id='$INTENT_ID'")
  log "diag: tx_attempts after the worker stopped=$attempts_now -- diagnostics only, not compared"
  {
    printf 'request_id=%s\n' "$REQUEST_ID"
    printf 'signing_tx=%s\n' "$SIGNING_TX_HASH"
    printf 'snapshot=%s\n' "$snap1"
    printf 'tx_attempts=%s (diagnostics only)\n' "$attempts_now"
  } > "$PRIVATE_DIR/logs/stage-b.txt"
  log "ok: stage B replay/observation window complete (the 30s window bounds what was observed, it is not a future guarantee)"
}

# --- verify ---------------------------------------------------------------
verify() {
  log "== verify: chain effects, receipt, outbox state, evidence partition"
  expect_uint_eq "watch balance == DEPOSIT" "$DEPOSIT" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$WATCH")"
  expect_uint_eq "signer balance == SIGNER_FINAL" "$SIGNER_FINAL" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$SIGNER_ADDR")"
  expect_uint_eq "recipient balance == WITHDRAW" "$WITHDRAW" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$RECIPIENT")"
  expect_uint_eq "deployer balance == DEPLOYER_FINAL" "$DEPLOYER_FINAL" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$DEPLOYER")"
  expect_uint_eq "totalSupply == SUPPLY" "$SUPPLY" \
    "$(cast_uint "$TOKEN" "totalSupply()(uint256)")"

  require_nonempty "deposit tx hash" "$DEPOSIT_TX"
  require_nonempty "signing tx hash" "$SIGNING_TX_HASH"
  mkdir -m 700 -p "$PUBLIC_DIR/chain" || fail "cannot create $PUBLIC_DIR/chain"
  local deposit_receipt signing_receipt observed_head logs_out logs_n logs_hash
  deposit_receipt=$(cast_rpc receipt "$DEPOSIT_TX" --json) \
    || fail "cast receipt (deposit, stage A) failed"
  printf '%s\n' "$deposit_receipt" > "$PUBLIC_DIR/chain/deposit-receipt.json"
  signing_receipt=$(cast_rpc receipt "$SIGNING_TX_HASH" --json) \
    || fail "cast receipt (signing, stage B) failed"
  printf '%s\n' "$signing_receipt" > "$PUBLIC_DIR/chain/signing-receipt.json"
  # Both real chain effects are checked with the same shape: status, from/to,
  # blockNumber, one log on the token, exactly three topics (Transfer signature
  # + zero-padded from/to), the uint256 amount as data, and a logIndex.
  check_transfer_receipt "stage A deposit receipt" "$deposit_receipt" \
    "$DEPLOYER" "$WATCH" "$DEPOSIT" "$DEPOSIT_TX" "$DEPOSIT_BLOCK_HASH" "$DEPOSIT_BLOCK_NUMBER"
  check_transfer_receipt "stage B signing receipt" "$signing_receipt" \
    "$SIGNER_ADDR" "$RECIPIENT" "$WITHDRAW" "$SIGNING_TX_HASH"

  # eth_getLogs uniqueness at a fixed observed head: across the whole chain
  # there is exactly one Transfer(signer -> recipient), and it is the signing tx.
  # The filter uses raw 32-byte topics (see check_transfer_log_uniqueness).
  observed_head=$(cast_rpc block-number)
  require_nonempty "observed head" "$observed_head"
  check_transfer_log_uniqueness "$observed_head" "$signing_receipt" "$SIGNING_TX_HASH"
  # Final chain snapshot: the actual call results at this point (not summary
  # constants), so a reviewer can re-check the balances/nonce/supply directly.
  {
    printf 'observed_head=%s\n' "$observed_head"
    printf 'total_supply=%s\n' "$(cast_uint "$TOKEN" "totalSupply()(uint256)")"
    printf 'balance_watch=%s\n' "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$WATCH")"
    printf 'balance_signer=%s\n' "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$SIGNER_ADDR")"
    printf 'balance_recipient=%s\n' "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$RECIPIENT")"
    printf 'balance_deployer=%s\n' "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$DEPLOYER")"
    printf 'signer_nonce=%s\n' "$(cast_rpc nonce "$SIGNER_ADDR")"
  } > "$PUBLIC_DIR/chain/final-state.txt"
  log "ok: getLogs filter/raw JSON and the final chain snapshot persisted under public/chain"

  # Transactional outbox write (NOT event delivery): no events pipeline runs
  # here, so every outbox row must still be publish_state='pending'.
  expect_eq "outbox rows not pending" "0" \
    "$(psql_ro "SELECT count(*) FROM outbox_events WHERE publish_state <> 'pending'")"
  local outbox_total
  outbox_total=$(psql_ro "SELECT count(*) FROM outbox_events")
  if (( outbox_total < 1 )); then
    fail "outbox is empty; the transactional outbox write is missing"
  fi
  log "ok: outbox rows=$outbox_total, all publish_state=pending (write only, no delivery)"

  # Public run metadata: exactly what produced this evidence (git state, script
  # and fixture hashes, ports, tool dir). No secrets. The secrets scan runs
  # AFTER this file, at the very end of verify.
  log "-- verify: public/run-metadata.txt (git + script/fixture hashes)"
  local git_head dirty_count meta
  git_head=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf 'unavailable')
  dirty_count=$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null | grep -c . || true)
  {
    printf 'run_id=%s project=%s\n' "$RUN_ID" "$PROJECT"
    printf 'run_token=%s\n' "$RUN_TOKEN"
    printf 'token=%s\n' "$TOKEN"
    printf 'ports: pg=%s anvil=%s http=%s signer=%s\n' \
      "$PG_PORT" "$ANVIL_PORT" "$HTTP_PORT" "$SIGNER_PORT"
    printf 'tools_dir=%s run_bin=%s\n' "$TOOLS_DIR" "$RUN_BIN"
    printf 'metadata_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'git_head=%s\ngit_dirty_files=%s\n' "$git_head" "$dirty_count"
    for meta in "$SCRIPT_DIR/run_demo.sh" "$SCRIPT_DIR/lib.sh" \
                "$SCRIPT_DIR/compile_token.sh" "$SCRIPT_DIR"/fixtures/*; do
      printf 'sha256 %s = %s\n' "${meta#"$REPO_ROOT"/}" \
        "$(sha256sum "$meta" | cut -d' ' -f1)"
    done
  } > "$PUBLIC_DIR/run-metadata.txt"
  log "ok: public/run-metadata.txt written (git_head=$git_head dirty_files=$dirty_count)"

  local v_reqs v_intents v_bindings v_attempts v_sc v_agg
  v_reqs=$(psql_ro 'SELECT count(*) FROM withdrawal_requests')
  v_intents=$(psql_ro 'SELECT count(*) FROM payment_intents')
  v_bindings=$(psql_ro 'SELECT count(*) FROM nonce_bindings')
  v_attempts=$(psql_ro "SELECT count(*) FROM tx_attempts WHERE intent_id='$INTENT_ID'")
  v_sc=$(psql_ro "SELECT count(*) FROM execution_events WHERE intent_id='$INTENT_ID' AND kind='state_changed'")
  v_agg=$(psql_ro "SELECT COALESCE(max(aggregate_version),0) FROM outbox_events WHERE aggregate_type='withdrawal_intent' AND aggregate_id='$INTENT_ID'")
  expect_eq "final intent state" "completed" \
    "$(psql_ro "SELECT state FROM payment_intents WHERE intent_id='$INTENT_ID'")"

  cat > "$PUBLIC_DIR/summary.txt" <<EOF
TxHarbor portfolio demo - run summary (public evidence, no secrets)
run_id=$RUN_ID
project=$PROJECT
run_token=$RUN_TOKEN
evidence_root=$EVIDENCE_ROOT
token=$TOKEN chain_id=31337 (fresh anvil; block 0 at start of the run)
deploy_tx=$DEPLOY_TX
deposit_tx=$DEPOSIT_TX
signing_tx=$SIGNING_TX_HASH
signer_native: fund_tx=$NATIVE_FUND_TX funded=1000000000000000000 wei balance_after=$SIGNER_NATIVE_BALANCE wei
               (>= 2000000000000000 wei = MAX_GAS_LIMIT * MAX_FEE_PER_GAS, so the signer pays its own gas)
request_id=$REQUEST_ID intent_id=$INTENT_ID authorization_id=$AUTH_ID
amounts: supply=$SUPPLY deposit=$DEPOSIT fund=$FUND withdraw=$WITHDRAW
stage A: deposit observed pending -> depth N-1 still pending -> depth N=$CONFIRM_DEPTH confirmed
         (confirm_threshold=$CONFIRM_DEPTH, confirmations>=$CONFIRM_DEPTH, confirm_policy_seq=1)
stage B: create 201 -> admission 201 (intent_id=$INTENT_ID) -> real signer/worker execution
         -> create replay 200 (same request_id) -> admission replay 200 (same intent_id)
negative: admission without bearer 401 (0 intents); same key + different amount 409 (1 request row);
          can_execute=false -> 403 (0 intents), then restored to true via the admin CLI
execution: final state=completed; requests=$v_reqs intents=$v_intents nonce_bindings=$v_bindings
           tx_attempts=$v_attempts for the intent (retry diagnostics only, excluded from the
           stability snapshot) (>= 1 with nonce 0, authorization_version 1)
           execution state_changed events=$v_sc == max outbox aggregate_version=$v_agg (>= 3)
observation: the 30s stage-B window is WINDOW-LIMITED: it bounds what was observed while the
             worker ran and after it was stopped. It is not a guarantee about future behaviour.
outbox: all rows publish_state=pending -- this is the transactional outbox WRITE, not event delivery
chain: watch=$WATCH ($DEPOSIT) signer=$SIGNER_ADDR ($SIGNER_FINAL) recipient=$RECIPIENT ($WITHDRAW)
       deployer=$DEPLOYER ($DEPLOYER_FINAL) totalSupply=$SUPPLY
note: local-only demo; not for production. "About 5 minutes" describes the talk after preparation,
      never a cold-start promise. Containers, volume and network are kept (see resources.txt).
EOF
  cat > "$PUBLIC_DIR/resources.txt" <<EOF
TxHarbor portfolio demo - run resources (kept; nothing was deleted)
run_id=$RUN_ID
project=$PROJECT
run_token=$RUN_TOKEN
container postgres: name=${CONTAINER_NAMES[postgres]} id=${CONTAINER_IDS[postgres]}
container anvil: name=${CONTAINER_NAMES[anvil]} id=${CONTAINER_IDS[anvil]}
volume=${PROJECT}_pgdata
network=${PROJECT}_default
evidence_root=$EVIDENCE_ROOT
statement: no volumes, containers or networks were deleted by this run; cleanup requires a
separate explicit authorization (the script only stopped the containers it could prove it
owns: compose project AND the run_token label above; never \`compose down\`, -v, rm or prune).
EOF
  log "ok: public/summary.txt and public/resources.txt written"

  # The secrets scan runs LAST, after every public file is on disk: the pattern
  # file holds the exact secret values of this run (caller/issuer API keys, the
  # signer credential, the signer private key, the full DSN) and the match is
  # never printed. public/sql, public/http, public/chain, run-metadata.txt and
  # both summaries are therefore all covered.
  log "-- verify: secrets scan over public/ (0600 pattern file, no output)"
  printf '%s\n' "$CALLER_KEY" "$ISSUER_KEY" "$SIGNER_CRED" \
    "$(cat "$KEY_SIGNER")" \
    "postgres://txharbor:txharbor@127.0.0.1:$PG_PORT/txharbor?sslmode=disable" \
    > "$PRIVATE_DIR/scan-patterns.txt"
  chmod 600 "$PRIVATE_DIR/scan-patterns.txt"
  scan_public_for_secrets "$PUBLIC_DIR" "$PRIVATE_DIR/scan-patterns.txt"
  local marker_rc=0
  grep -RFq 'signed_tx_bytes' "$PUBLIC_DIR" || marker_rc=$?
  case "$marker_rc" in
    0) fail "'signed_tx_bytes' marker found in public/ evidence" ;;
    1) log "ok: no 'signed_tx_bytes' marker in public/ evidence" ;;
    *) fail "signed_tx_bytes scan failed (grep rc=$marker_rc)" ;;
  esac
  log "ok: public/ carries no secret value and no raw signed-material marker"
}

# --- entry points ---------------------------------------------------------
usage() {
  cat <<'EOF'
usage: bash scripts/portfolio-demo/run_demo.sh [mode]

Modes:
  run_all           (default) preflight -> setup -> stage A -> stage B -> verify.
  --preflight-only  run the guards only, print "PREFLIGHT PASS (no resources)"
                    and exit 0: no container, no stack, no chain/DB write.
  --help            this text.

Environment inputs (optional):
  PORTFOLIO_RUN_ID          run id / compose project suffix (default UTC ts + pid);
                            must match ^[a-z0-9][a-z0-9-]{0,63}$
  TXHARBOR_DEMO_TOOLS_DIR   tool build dir; only /tmp/opencode/txharbor-demo-tools
                            or <repo>/.evidence/portfolio-demo-tools are accepted
                            (tools are always built per run, never reused)

The host ports are fixed by run_demo.sh (postgres 55436, anvil 58547, HTTP 18081,
signer 18091); PORTFOLIO_PG_PORT/PORTFOLIO_ANVIL_PORT are re-exported from those
constants, so the overlay cannot be redirected away from them.

The demo refuses TXHARBOR_RPC_URL, DOCKER_HOST/DOCKER_TLS_VERIFY/DOCKER_CERT_PATH/
DOCKER_CONFIG and non-unix docker contexts, then unsets every COMPOSE_*/DOCKER_*/
TXHARBOR_* variable plus the proxy/CURL_* hooks before it renders the config. It
never sources the repo `.env`; it stops only containers it can prove it owns
(compose project AND the run's txharbor.demo.run_token label) and never runs
`compose down`, -v, rm or prune.
EOF
}

run_all() {
  preflight
  setup
  stage_a
  stage_b
  verify
  RUN_OK=1
  printf 'EVIDENCE: %s\n' "$EVIDENCE_ROOT"
}

case "${1:-run_all}" in
  run_all)
    run_all
    ;;
  --preflight-only)
    preflight
    printf 'PREFLIGHT PASS (no resources)\n'
    ;;
  --help|-h)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
