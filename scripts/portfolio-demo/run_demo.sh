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
EVIDENCE_BASE=".evidence/portfolio-demo"
EVIDENCE_ROOT="$EVIDENCE_BASE/$RUN_ID"

# --- fixed demo port / policy constants -----------------------------------
PG_PORT=55436
ANVIL_PORT=58547
HTTP_PORT=18081
SIGNER_PORT=18091
CONFIRM_DEPTH=3
CALLER_ID=7001
ISSUER_CALLER_ID=9001
SIGNER_CALLER_ID=8001
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
FEE_TOTAL="10000000000000000"
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

# --- run state ------------------------------------------------------------
STACK_STARTED=0
RUN_OK=0
OWNED_PIDS=()
SERVE_PID=""
SIGNER_PID=""
WORKER_PID=""
BIN=""
DEMOCTL=""
TOKEN=""
SIGNER_ADDR=""
DEPLOYER=""
WATCH=""
RECIPIENT=""
REQUEST_ID=""
SIGNING_TX_HASH=""
DEPLOY_TX=""
DEPOSIT_TX=""
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
# own anvil container (image-pinned cast; no host cast fallback).
cast_rpc() {
  local sub=$1
  shift
  "${CAST[@]}" "$sub" --rpc-url "http://127.0.0.1:8545" "$@"
}

# cast_uint <token> <signature> [args...] -- decoded uint256 return; only the
# first whitespace-delimited token is kept (cast may append a human hint).
cast_uint() {
  local target=$1 sig=$2
  shift 2
  "${CAST[@]}" call "$target" "$sig" "$@" --rpc-url "http://127.0.0.1:8545" \
    | awk '{print $1}'
}

# psql_ro <sql> -- one statement through the demo's own postgres container
# (local connection inside the container; no password ever enters argv).
psql_ro() {
  "${COMPOSE[@]}" exec -T postgres psql -w -U txharbor -d txharbor \
    -v ON_ERROR_STOP=1 -qAt -c "$1"
}

# psql_script -- multi-statement SQL on stdin through the same container.
psql_script() {
  "${COMPOSE[@]}" exec -T postgres psql -w -U txharbor -d txharbor \
    -v ON_ERROR_STOP=1 -qAt
}

# http_post <curl-conf> <body-file|-> <out-file> -- prints the HTTP status
# code. The bearer lives in the 0600 curl config, never in argv; only the body
# file path (never the body) is passed on the command line.
http_post() {
  local conf=$1 body=$2 out=$3
  local -a args=(--config "$conf" --silent --show-error --output "$out" --write-out '%{http_code}')
  if [[ "$body" != "-" ]]; then
    args+=(--data-binary "@$body")
  fi
  curl "${args[@]}"
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
# stop_stack: stop this run's containers only. Never down, never -v, never rm,
# never prune: volumes, containers and networks survive the run.
stop_stack() {
  if [[ "$STACK_STARTED" != "1" ]]; then
    return 0
  fi
  log "cleanup: compose stop (containers, volume and network are kept)"
  if "${COMPOSE[@]}" stop; then
    return 0
  fi
  return 1
}

# stop_owned: TERM every still-tracked own PID, bounded wait, KILL escalation
# only for own PIDs (which is itself a failure), then require exit 0 from each.
stop_owned() {
  local rc=0 pid st alive
  for pid in "${OWNED_PIDS[@]:-}"; do
    if [[ -z "$pid" ]]; then continue; fi
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    alive=0
    for pid in "${OWNED_PIDS[@]:-}"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then alive=1; fi
    done
    if (( alive == 0 )); then break; fi
    sleep 0.2
  done
  for pid in "${OWNED_PIDS[@]:-}"; do
    if [[ -z "$pid" ]]; then continue; fi
    if kill -0 "$pid" 2>/dev/null; then
      warn "cleanup: pid $pid ignored TERM; escalating to KILL"
      kill -KILL "$pid" 2>/dev/null || true
      rc=1
    fi
  done
  for pid in "${OWNED_PIDS[@]:-}"; do
    if [[ -z "$pid" ]]; then continue; fi
    if wait "$pid" 2>/dev/null; then st=0; else st=$?; fi
    if (( st != 0 )); then
      warn "cleanup: pid $pid exited with status $st"
      rc=1
    fi
  done
  OWNED_PIDS=()
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
# Mandatory order; no run resource exists before every guard passed.
preflight() {
  # 1) No RPC override input: refuse before any connection (N3).
  if [[ -n "${TXHARBOR_RPC_URL:-}" ]]; then
    fail "refused: TXHARBOR_RPC_URL is set; the demo takes no RPC override input and made no connection"
  fi
  # 2) Docker guards: inspect only; never `docker context use`; never a write
  #    to a caller-supplied remote daemon (N4).
  local v
  for v in DOCKER_HOST DOCKER_TLS_VERIFY DOCKER_CERT_PATH; do
    if [[ -n "${!v:-}" ]]; then
      fail "refused: $v is set; the demo runs only against a local unix-socket docker context"
    fi
  done
  local ctx endpoint
  ctx="${DOCKER_CONTEXT:-}"
  if [[ -z "$ctx" ]]; then
    ctx=$(docker context show) || fail "docker context show failed"
  fi
  endpoint=$(docker context inspect "$ctx" --format '{{.Endpoints.docker.Host}}') \
    || fail "docker context inspect $ctx failed"
  case "$endpoint" in
    unix://*) ;;
    *) fail "refused: docker context $ctx endpoint [$endpoint] is not a local unix socket" ;;
  esac
  export DOCKER_CONTEXT="$ctx"
  DOCKER=(docker --context "$DOCKER_CONTEXT")
  COMPOSE=("${DOCKER[@]}" compose -p "$PROJECT" --env-file /dev/null
           -f "$REPO_ROOT/compose.yaml"
           -f "$REPO_ROOT/scripts/portfolio-demo/compose.portfolio-demo.yaml")
  CAST=("${COMPOSE[@]}" exec -T anvil cast)

  # 3) Sanitize the caller environment AFTER the guards (names only are
  #    logged). The repo `.env` is never sourced.
  local name tx_names=""
  for name in $(env | sed -n 's/^\(TXHARBOR_[A-Za-z0-9_]*\)=.*/\1/p' | sort); do
    tx_names="$tx_names $name"
  done
  if [[ -n "$tx_names" ]]; then
    log "sanitize: unsetting caller vars:$tx_names"
    for name in $tx_names; do
      unset "$name"
    done
  fi
  unset COMPOSE_PROFILES COMPOSE_FILE COMPOSE_PROJECT_NAME

  # 4) Exclusive evidence root: refuse anything already at that path.
  if [[ -e "$EVIDENCE_ROOT" || -L "$EVIDENCE_ROOT" ]]; then
    fail "refused: evidence root already exists: $EVIDENCE_ROOT"
  fi
  mkdir -m 700 "$EVIDENCE_ROOT" || fail "cannot create evidence root $EVIDENCE_ROOT"
  mkdir -m 700 "$EVIDENCE_ROOT/public" "$EVIDENCE_ROOT/private" \
    "$EVIDENCE_ROOT/private/keys" "$EVIDENCE_ROOT/private/env" \
    "$EVIDENCE_ROOT/private/curl" "$EVIDENCE_ROOT/private/logs" \
    || fail "cannot create evidence subdirectories"
  PUBLIC_DIR="$EVIDENCE_ROOT/public"
  PRIVATE_DIR="$EVIDENCE_ROOT/private"
  ENV_SERVE="$PRIVATE_DIR/env/serve.env"
  ENV_SIGNER="$PRIVATE_DIR/env/signer.env"
  ENV_WORKER="$PRIVATE_DIR/env/worker.env"
  KEY_CALLER="$PRIVATE_DIR/keys/caller.key"
  KEY_ISSUER="$PRIVATE_DIR/keys/issuer.key"
  KEY_SIGNER="$PRIVATE_DIR/keys/signer.key"
  KEY_SIGNER_CRED="$PRIVATE_DIR/keys/signer.cred"

  # 5) Refuse pre-existing project resources (containers, network, volume).
  local found
  found=$("${DOCKER[@]}" ps -aq --filter "label=com.docker.compose.project=$PROJECT")
  if [[ -n "$found" ]]; then
    fail "refused: project $PROJECT already has containers: $found"
  fi
  found=$("${DOCKER[@]}" network ls -q --filter "name=^${PROJECT}_default$")
  if [[ -n "$found" ]]; then
    fail "refused: network ${PROJECT}_default already exists"
  fi
  found=$("${DOCKER[@]}" volume ls -q --filter "name=^${PROJECT}_pgdata$")
  if [[ -n "$found" ]]; then
    fail "refused: volume ${PROJECT}_pgdata already exists"
  fi

  # 6) Ports must be free (bash /dev/tcp probe only).
  local port
  for port in "$PG_PORT" "$ANVIL_PORT" "$HTTP_PORT" "$SIGNER_PORT"; do
    if ! port_free "$port"; then
      fail "refused: 127.0.0.1:$port is already in use"
    fi
  done

  # 7) Rendered config must carry this run's project name and overlay ports.
  local rendered rendered_name
  rendered=$("${COMPOSE[@]}" config --format json) || fail "compose config render failed"
  rendered_name=$(printf '%s\n' "$rendered" \
    | sed -n 's/^[[:space:]]*"name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
  expect_eq "compose project name" "$PROJECT" "$rendered_name"
  for port in "$PG_PORT" "$ANVIL_PORT"; do
    if ! printf '%s\n' "$rendered" \
      | grep -Eq "\"published\"[[:space:]]*:[[:space:]]*\"?${port}\"?"; then
      fail "compose config does not publish host port $port; overlay not applied?"
    fi
  done
  log "preflight: guards passed; no run resource created yet"
}

# --- setup ----------------------------------------------------------------
setup() {
  log "== setup: build demo tools into $TOOLS_DIR/bin"
  mkdir -p "$TOOLS_DIR/bin" || fail "cannot create $TOOLS_DIR/bin"
  BIN="$TOOLS_DIR/bin/txharbor"
  DEMOCTL="$TOOLS_DIR/bin/democtl"
  ( cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/txharbor ) \
    || fail "go build ./cmd/txharbor failed"
  ( cd "$REPO_ROOT" && go build -o "$DEMOCTL" ./scripts/portfolio-demo/democtl ) \
    || fail "go build ./scripts/portfolio-demo/democtl failed"
  require_file "token creation bytecode" \
    "scripts/portfolio-demo/fixtures/MinimalERC20.creation.bin"

  log "== setup: compose up postgres + anvil (project $PROJECT)"
  STACK_STARTED=1
  "${COMPOSE[@]}" up -d --wait --wait-timeout 180 postgres anvil \
    || fail "compose up failed"

  # Ownership: exactly one container per service, and it is this run's.
  local svc id_list id n
  for svc in postgres anvil; do
    id_list=$("${DOCKER[@]}" ps -q \
      --filter "label=com.docker.compose.project=$PROJECT" \
      --filter "label=com.docker.compose.service=$svc")
    local -a ids=()
    mapfile -t ids <<<"$id_list"
    n=0
    for id in "${ids[@]}"; do
      if [[ -n "$id" ]]; then n=$((n + 1)); fi
    done
    expect_eq "$svc container count" "1" "$n"
    CONTAINER_IDS[$svc]="${ids[0]}"
    CONTAINER_NAMES[$svc]=$("${DOCKER[@]}" inspect --format '{{.Name}}' \
      --type container "${ids[0]}" | sed 's|^/||')
  done
  expect_eq "postgres host port" "127.0.0.1:$PG_PORT" \
    "$("${COMPOSE[@]}" port postgres 5432)"
  expect_eq "anvil host port" "127.0.0.1:$ANVIL_PORT" \
    "$("${COMPOSE[@]}" port anvil 8545)"

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
  local key_out addr priv
  key_out=$("${CAST[@]}" wallet new) || fail "cast wallet new failed"
  addr=$(printf '%s\n' "$key_out" | sed -n 's/^Address:[[:space:]]*//p' | head -n 1)
  priv=$(printf '%s\n' "$key_out" | sed -n 's/^Private key:[[:space:]]*//p' | head -n 1)
  if [[ ! "$addr" =~ ^0x[0-9a-fA-F]{40}$ ]]; then
    fail "cannot parse the signer address out of cast wallet new"
  fi
  if [[ ! "$priv" =~ ^0x[0-9a-fA-F]{64}$ ]]; then
    fail "cannot parse the signer private key out of cast wallet new"
  fi
  SIGNER_ADDR=$(lower_hex "$addr")
  ( umask 077; printf '%s\n' "$priv" > "$KEY_SIGNER" )
  chmod 600 "$KEY_SIGNER"

  log "== setup: fund the signer with FUND from the deployer"
  local fund_out
  fund_out=$(cast_rpc send --unlocked --from "$DEPLOYER" "$TOKEN" \
    "transfer(address,uint256)" "$SIGNER_ADDR" "$FUND" --json) \
    || fail "funding the signer failed"
  expect_eq "fund tx status" "0x1" "$(printf '%s\n' "$fund_out" | json_get status)"
  expect_uint_eq "signer balance == FUND" "$FUND" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$SIGNER_ADDR")"

  # signer-serve loads the SAME config.Load as serve/migrate, so its env file is
  # the serve key set plus the 009 signer policy keys (a bare TXHARBOR_SIGNER_*
  # file would be refused as a configuration error).
  {
    cat "$ENV_SERVE"
    cat <<EOF
TXHARBOR_SIGNER_MODE=development
TXHARBOR_SIGNER_KEY_FILE='$REPO_ROOT/$KEY_SIGNER'
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

  log "== setup: start serve + signer (logs -> private/logs)"
  ( set -a; . "$ENV_SERVE"; set +a; exec "$BIN" serve ) \
    >> "$PRIVATE_DIR/logs/serve.log" 2>&1 &
  SERVE_PID=$!
  OWNED_PIDS+=("$SERVE_PID")
  ( set -a; . "$ENV_SIGNER"; set +a; exec "$BIN" signer-serve ) \
    >> "$PRIVATE_DIR/logs/signer.log" 2>&1 &
  SIGNER_PID=$!
  OWNED_PIDS+=("$SIGNER_PID")

  # serve readiness: /readyz answers 200.
  local deadline=$((SECONDS + 60)) code
  while :; do
    if ! kill -0 "$SERVE_PID" 2>/dev/null; then
      tail -n 40 "$PRIVATE_DIR/logs/serve.log" >&2 || true
      fail "serve exited during startup"
    fi
    code=$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:$HTTP_PORT/readyz" || true)
    if [[ "$code" == "200" ]]; then break; fi
    if (( SECONDS >= deadline )); then
      tail -n 40 "$PRIVATE_DIR/logs/serve.log" >&2 || true
      fail "serve did not become ready within 60s"
    fi
    sleep 1
  done
  log "ok: serve ready on 127.0.0.1:$HTTP_PORT"

  # signer readiness: TCP port open (signer-serve exposes no HTTP probe).
  deadline=$((SECONDS + 60))
  while :; do
    if ! kill -0 "$SIGNER_PID" 2>/dev/null; then
      tail -n 40 "$PRIVATE_DIR/logs/signer.log" >&2 || true
      fail "signer exited during startup"
    fi
    if ! port_free "$SIGNER_PORT"; then break; fi
    if (( SECONDS >= deadline )); then
      tail -n 40 "$PRIVATE_DIR/logs/signer.log" >&2 || true
      fail "signer did not open 127.0.0.1:$SIGNER_PORT within 60s"
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
    n=$(psql_ro "SELECT count(*) FROM deposit_observations WHERE tx_hash='$tx' AND contract='$TOKEN' AND status='pending'")
    if [[ "$n" == "1" ]]; then break; fi
    if (( SECONDS >= deadline )); then
      fail "no pending deposit_observations row for $tx within 120s (count=$n)"
    fi
    sleep 2
  done
  log "ok: deposit observed as pending (tx $tx)"

  expect_uint_eq "watch balance == DEPOSIT" "$DEPOSIT" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$WATCH")"
  expect_uint_eq "deployer balance == DEPLOYER_FINAL" "$DEPLOYER_FINAL" \
    "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$DEPLOYER")"

  # Depth N-1 (one block short of the threshold): still pending.
  cast_rpc rpc anvil_mine 0x1 > /dev/null
  sleep 2
  expect_eq "status at depth N-1" "pending" \
    "$(psql_ro "SELECT status FROM deposit_observations WHERE tx_hash='$tx'")"

  # Depth N: the confirmation commit lands.
  cast_rpc rpc anvil_mine 0x1 > /dev/null
  deadline=$((SECONDS + 120))
  while :; do
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
  local http_dir="$PRIVATE_DIR/logs"
  local code

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
  code=$(http_post "$conf_create" "$body_create" "$http_dir/http-create-201.json")
  expect_eq "create HTTP status" "201" "$code"
  REQUEST_ID=$(json_get request_id < "$http_dir/http-create-201.json")
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
  code=$(http_post "$conf_nobearer" "-" "$http_dir/http-admit-401.json")
  expect_eq "admission without bearer" "401" "$code"
  expect_eq "intents after 401" "0" \
    "$(psql_ro "SELECT count(*) FROM payment_intents WHERE request_id='$REQUEST_ID'")"

  log "-- N5c: same key, different amount -> 409, original request untouched"
  code=$(http_post "$conf_create" "$body_conflict" "$http_dir/http-create-409.json")
  expect_eq "conflicting create" "409" "$code"
  expect_eq "requests(caller,key) after 409" "1" \
    "$(psql_ro "SELECT count(*) FROM withdrawal_requests WHERE caller_id=$CALLER_ID AND idempotency_key='$IDEM'")"

  log "-- N5b: can_execute=false -> 403, zero intents; restore with the CLI"
  cli_run "withdrawal-exec permission-set $CALLER_ID op-perm-2 (false)" \
    "$BIN" withdrawal-exec permission-set --caller-id "$CALLER_ID" \
    --can-execute=false --operation-id op-perm-2 --operator op --reason demo > /dev/null
  code=$(http_post "$conf_admit" "-" "$http_dir/http-admit-403.json")
  expect_eq "admission with can_execute=false" "403" "$code"
  expect_eq "intents after 403" "0" \
    "$(psql_ro "SELECT count(*) FROM payment_intents WHERE request_id='$REQUEST_ID'")"
  cli_run "withdrawal-exec permission-set $CALLER_ID op-perm-3 (true)" \
    "$BIN" withdrawal-exec permission-set --caller-id "$CALLER_ID" \
    --can-execute=true --operation-id op-perm-3 --operator op --reason demo > /dev/null

  # The ONLY SQL write of this run: one scope/config row, single statement in
  # a transaction, exactly one RETURNING row (0 rows fails, no retry, no
  # ON CONFLICT, no UPDATE). It records the authorization/fee ceilings only --
  # request, intent, binding, signing, broadcast and chain effect below are all
  # produced by the real production paths.
  log "-- B3: one controlled scope row (withdrawal_authorization_scopes)"
  local sql_out rows extra
  sql_out=$(psql_script <<SQL
BEGIN;
INSERT INTO withdrawal_authorization_scopes
  (authorization_id, intent_id, request_id, sender,
   fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement,
   authorization_version, attested_by)
SELECT g.authorization_id, '$INTENT_ID', r.request_id, '$SIGNER_ADDR',
       $FEE_TOTAL, $FEE_PER_GAS, $FEE_PRIORITY, TRUE,
       COALESCE((SELECT max(s.authorization_version)
                   FROM withdrawal_authorization_scopes s
                  WHERE s.authorization_id = g.authorization_id), 0) + 1,
       'demo-issuer'
  FROM withdrawal_authorizations g
  JOIN withdrawal_requests r ON r.authorization_id = g.authorization_id
 WHERE g.authorization_id = '$AUTH_ID'
   AND g.state = 'active'
   AND r.request_id = '$REQUEST_ID'
   AND r.caller_id = g.caller_id
   AND r.chain_id = g.chain_id
   AND r.asset = g.asset
   AND r.recipient = g.recipient
   AND r.amount = g.amount
   AND NOT EXISTS (SELECT 1 FROM withdrawal_authorization_scopes s2
                    WHERE s2.authorization_id = g.authorization_id)
RETURNING authorization_id;
COMMIT;
SQL
) || fail "scope insert failed"
  rows=$(printf '%s\n' "$sql_out" | grep -c -x "$AUTH_ID" || true)
  extra=$(printf '%s\n' "$sql_out" \
    | grep -Ev -x -e '' -e 'BEGIN' -e 'COMMIT' -e 'INSERT 0 1' -e "$AUTH_ID" \
    | grep -c . || true)
  expect_eq "scope insert RETURNING rows" "1" "$rows"
  expect_eq "scope insert unexpected output lines" "0" "$extra"

  log "-- B2: admission POST -> 201 intent_id == $INTENT_ID"
  code=$(http_post "$conf_admit" "-" "$http_dir/http-admit-201.json")
  expect_eq "admission HTTP status" "201" "$code"
  expect_eq "admission intent_id" "$INTENT_ID" \
    "$(json_get intent_id < "$http_dir/http-admit-201.json")"
  expect_eq "payment_intents rows after admission" "1" \
    "$(psql_ro 'SELECT count(*) FROM payment_intents')"

  log "-- B2: immediate replay of the (unfinished) admission -> 200, same intent"
  code=$(http_post "$conf_admit" "-" "$http_dir/http-admit-200-unfinished.json")
  expect_eq "unfinished admission replay" "200" "$code"
  expect_eq "unfinished replay intent_id" "$INTENT_ID" \
    "$(json_get intent_id < "$http_dir/http-admit-200-unfinished.json")"

  log "-- B3: start withdrawal-worker (credential from the 0600 file, not argv)"
  require_file "signer credential" "$KEY_SIGNER_CRED"
  ( set -a; . "$ENV_WORKER"; set +a
    TXHARBOR_TX_SIGNER_CREDENTIAL="$(cat "$KEY_SIGNER_CRED")"
    exec "$BIN" withdrawal-worker ) >> "$PRIVATE_DIR/logs/worker.log" 2>&1 &
  WORKER_PID=$!
  OWNED_PIDS+=("$WORKER_PID")

  log "-- B3: poll payment_intents.state == completed (mining only while a signing exists)"
  local deadline=$((SECONDS + 240)) state mines=0 has_signing
  while :; do
    state=$(psql_ro "SELECT state FROM payment_intents WHERE intent_id='$INTENT_ID'")
    if [[ "$state" == "completed" ]]; then break; fi
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      tail -n 40 "$PRIVATE_DIR/logs/worker.log" >&2 || true
      fail "withdrawal-worker exited before completion (state=$state)"
    fi
    has_signing=$(psql_ro "SELECT count(*) FROM tx_attempt_signings s JOIN tx_attempts a ON a.attempt_id = s.attempt_id WHERE a.intent_id='$INTENT_ID'")
    if [[ "$has_signing" != "0" ]] && (( mines < 60 )); then
      if cast_rpc rpc anvil_mine 0x1 > /dev/null; then mines=$((mines + 1)); fi
    fi
    if (( SECONDS >= deadline )); then
      tail -n 40 "$PRIVATE_DIR/logs/worker.log" >&2 || true
      fail "intent did not reach completed within 240s (state=$state, mines=$mines)"
    fi
    sleep 2
  done
  log "ok: payment intent completed (extra blocks mined=$mines)"

  log "-- B4: create replay (same key) -> 200, same request_id"
  code=$(http_post "$conf_create" "$body_create" "$http_dir/http-create-200-replay.json")
  expect_eq "create replay HTTP status" "200" "$code"
  expect_eq "create replay request_id" "$REQUEST_ID" \
    "$(json_get request_id < "$http_dir/http-create-200-replay.json")"
  log "-- B4: admission replay (completed) -> 200, same intent_id"
  code=$(http_post "$conf_admit" "-" "$http_dir/http-admit-200-replay.json")
  expect_eq "completed admission replay" "200" "$code"
  expect_eq "completed replay intent_id" "$INTENT_ID" \
    "$(json_get intent_id < "$http_dir/http-admit-200-replay.json")"

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

  # One composite snapshot: request/intent/binding/outbox/event/attempt counts
  # plus the two chain facts that must stay put.
  snapshot() {
    printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
      "$(psql_ro 'SELECT count(*) FROM withdrawal_requests')" \
      "$(psql_ro 'SELECT count(*) FROM payment_intents')" \
      "$(psql_ro 'SELECT count(*) FROM nonce_bindings')" \
      "$(psql_ro "SELECT count(*) FROM outbox_events WHERE event_type='withdrawal.request.received'")" \
      "$(psql_ro "SELECT count(*) FROM execution_events WHERE intent_id='$INTENT_ID' AND kind='state_changed'")" \
      "$(psql_ro "SELECT count(*) FROM tx_attempts WHERE intent_id='$INTENT_ID'")" \
      "$(cast_uint "$TOKEN" "balanceOf(address)(uint256)" "$RECIPIENT")" \
      "$(cast_rpc nonce "$SIGNER_ADDR")"
  }
  local snap1 snap2 snap3 st
  snap1=$(snapshot)
  expect_eq "snapshot before the window" \
    "1|1|1|1|$state_changed|$total_attempts|$WITHDRAW|1" "$snap1"

  log "-- B6: bounded 30s observation with the worker still running"
  sleep 30
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    fail "withdrawal-worker died during the observation window"
  fi
  snap2=$(snapshot)
  expect_eq "snapshot after the 30s window" "$snap1" "$snap2"

  log "-- B6: stop the worker gracefully and re-check the stable state"
  if graceful_stop "$WORKER_PID" "withdrawal-worker" 30; then st=0; else st=$?; fi
  untrack_pid "$WORKER_PID"
  expect_eq "worker graceful exit status" "0" "$st"
  snap3=$(snapshot)
  expect_eq "snapshot after the worker stopped" "$snap1" "$snap3"
  {
    printf 'request_id=%s\n' "$REQUEST_ID"
    printf 'signing_tx=%s\n' "$SIGNING_TX_HASH"
    printf 'snapshot=%s\n' "$snap1"
  } > "$PRIVATE_DIR/logs/stage-b.txt"
  log "ok: stage B replay/observation window complete"
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

  require_nonempty "signing tx hash" "$SIGNING_TX_HASH"
  local receipt
  receipt=$(cast_rpc receipt "$SIGNING_TX_HASH" --json) || fail "cast receipt failed"
  mkdir -m 700 -p "$PUBLIC_DIR/chain"
  printf '%s\n' "$receipt" > "$PUBLIC_DIR/chain/receipt.json"
  expect_eq "signing receipt status" "0x1" "$(printf '%s\n' "$receipt" | json_get status)"
  expect_eq "signing receipt log count" "1" "$(printf '%s\n' "$receipt" | json_count logs)"
  expect_eq "transfer topic0 (Transfer event)" \
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" \
    "$(printf '%s\n' "$receipt" | json_get 'logs[0].topics[0]')"
  expect_eq "transfer topic1 (signer)" "0x000000000000000000000000${SIGNER_ADDR#0x}" \
    "$(printf '%s\n' "$receipt" | json_get 'logs[0].topics[1]')"
  expect_eq "transfer topic2 (recipient)" "0x000000000000000000000000${RECIPIENT#0x}" \
    "$(printf '%s\n' "$receipt" | json_get 'logs[0].topics[2]')"
  expect_eq "transfer data == WITHDRAW" "$("$DEMOCTL" uint256 hex "$WITHDRAW")" \
    "$(printf '%s\n' "$receipt" | json_get 'logs[0].data')"

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

  # Secrets scan over public/ only; the matched content is never printed.
  log "-- verify: secrets scan over public/ (0600 pattern file, no output)"
  printf '%s\n' "$CALLER_KEY" "$ISSUER_KEY" "$SIGNER_CRED" \
    "$(cat "$KEY_SIGNER")" \
    "postgres://txharbor:txharbor@127.0.0.1:$PG_PORT/txharbor?sslmode=disable" \
    > "$PRIVATE_DIR/scan-patterns.txt"
  chmod 600 "$PRIVATE_DIR/scan-patterns.txt"
  if grep -RFq -f "$PRIVATE_DIR/scan-patterns.txt" "$PUBLIC_DIR"; then
    fail "secret material found in public/ evidence"
  fi
  if grep -RFq 'signed_tx_bytes' "$PUBLIC_DIR"; then
    fail "'signed_tx_bytes' marker found in public/ evidence"
  fi
  log "ok: public/ carries no secret value and no raw signed-material marker"

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
evidence_root=$EVIDENCE_ROOT
token=$TOKEN chain_id=31337 (fresh anvil; block 0 at start of the run)
deploy_tx=$DEPLOY_TX
deposit_tx=$DEPOSIT_TX
signing_tx=$SIGNING_TX_HASH
request_id=$REQUEST_ID intent_id=$INTENT_ID authorization_id=$AUTH_ID
amounts: supply=$SUPPLY deposit=$DEPOSIT fund=$FUND withdraw=$WITHDRAW
stage A: deposit observed pending -> depth N-1 still pending -> depth N=$CONFIRM_DEPTH confirmed
         (confirm_threshold=$CONFIRM_DEPTH, confirmations>=$CONFIRM_DEPTH, confirm_policy_seq=1)
stage B: create 201 -> admission 201 (intent_id=$INTENT_ID) -> real signer/worker execution
         -> create replay 200 (same request_id) -> admission replay 200 (same intent_id)
negative: admission without bearer 401 (0 intents); same key + different amount 409 (1 request row);
          can_execute=false -> 403 (0 intents), then restored to true via the admin CLI
execution: final state=completed; requests=$v_reqs intents=$v_intents nonce_bindings=$v_bindings
           tx_attempts=$v_attempts for the intent (>= 1 with nonce 0, authorization_version 1)
           execution state_changed events=$v_sc == max outbox aggregate_version=$v_agg (>= 3)
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
container postgres: name=${CONTAINER_NAMES[postgres]} id=${CONTAINER_IDS[postgres]}
container anvil: name=${CONTAINER_NAMES[anvil]} id=${CONTAINER_IDS[anvil]}
volume=${PROJECT}_pgdata
network=${PROJECT}_default
evidence_root=$EVIDENCE_ROOT
statement: no volumes, containers or networks were deleted by this run; cleanup requires a
separate explicit authorization (the script only ran \`compose stop\` for this run's project).
EOF
  log "ok: public/summary.txt and public/resources.txt written"
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
  PORTFOLIO_RUN_ID          run id / compose project suffix (default UTC ts + pid)
  TXHARBOR_DEMO_TOOLS_DIR   tool build dir (default /tmp/opencode/txharbor-demo-tools)

The host ports are fixed by run_demo.sh (postgres 55436, anvil 58547, HTTP 18081,
signer 18091); the compose overlay's PORTFOLIO_PG_PORT/PORTFOLIO_ANVIL_PORT knobs
must stay at those defaults for run_demo.sh runs.

The demo refuses TXHARBOR_RPC_URL, DOCKER_HOST/DOCKER_TLS_VERIFY/DOCKER_CERT_PATH
and non-unix docker contexts; it never sources the repo `.env`; it only ever
stops its own containers (`compose stop`) and never down/-v/rm/prune.
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
