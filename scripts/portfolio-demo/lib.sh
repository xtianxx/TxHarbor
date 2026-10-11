#!/usr/bin/env bash
# TxHarbor portfolio demo: shared shell helpers.
#
# Sourced by run_demo.sh. Defines functions only -- no top-level side effects,
# no external dependencies beyond the demo toolset (democtl, docker, curl,
# psql/cast inside the demo containers) plus the host prerequisites `timeout`
# (bounds every psql/cast/docker call; checked in preflight) and `stat`/`id`
# plus /proc (evidence-root ownership and liveness checks). Amounts are never
# compared with bash arithmetic: everything uint256 goes through `democtl
# uint256`.
set -euo pipefail

log()  { printf '%s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
die()  { fail "$@"; }

# expect_eq <label> <want> <got> -- exact string equality (counts, status
# codes, hex hashes, states -- never uint256 amounts).
expect_eq() {
  if [[ "$2" != "$3" ]]; then
    fail "$1: want [$2] got [$3]"
  fi
  log "ok: $1 == $3"
}

# expect_uint_eq <label> <a> <b> -- uint256 equality (decimal or 0x-hex).
expect_uint_eq() {
  local out
  if ! out=$("$DEMOCTL" uint256 eq "$2" "$3" 2>&1); then
    fail "$1: uint256 not equal: [$2] vs [$3] ($out)"
  fi
  log "ok: $1 == $3"
}

# expect_uint_ge <label> <a> <b> -- uint256 a >= b.
expect_uint_ge() {
  local out
  if ! out=$("$DEMOCTL" uint256 ge "$2" "$3" 2>&1); then
    fail "$1: uint256 [$2] < [$3] ($out)"
  fi
  log "ok: $1 >= $3"
}

# json_get / json_require / json_count -- JSON on stdin, via democtl.
json_get()     { "$DEMOCTL" json get "$1"; }
json_require() { "$DEMOCTL" json require "$1" "$2"; }
json_count()   { "$DEMOCTL" json count "$1"; }

# lower_hex <string> -- lowercase A-F (EVM addresses are compared/stored
# lowercase; every DB CHECK in the migrations requires it).
lower_hex() { printf '%s\n' "$1" | tr 'A-F' 'a-f'; }

# require_addr <label> <value> -- must be lowercase 0x + 40 hex.
require_addr() {
  if [[ ! "$2" =~ ^0x[0-9a-f]{40}$ ]]; then
    fail "$1: not a lowercase 0x/40-hex address: [$2]"
  fi
}

# require_nonempty <label> <value>
require_nonempty() {
  if [[ -z "$2" ]]; then
    fail "$1: empty value"
  fi
}

# require_file <label> <path>
require_file() {
  if [[ ! -f "$2" ]]; then
    fail "$1: missing file: $2"
  fi
}

# require_safe_run_id <id> -- the run id becomes a compose project suffix, an
# evidence directory name and part of every container name, so it must be one
# safe path segment: lowercase alnum + dash, starting alnum, <= 64 chars (no
# slash, dot, uppercase, leading dash or newline).
require_safe_run_id() {
  if [[ ! "$1" =~ ^[a-z0-9][a-z0-9-]{0,63}$ ]]; then
    fail "refused: unsafe run id [$1] (allowed: lowercase a-z/0-9/dash, 1-64 chars, starting alnum)"
  fi
  log "ok: run id [$1] is a safe project/path segment"
}

# validate_tools_dir -- TOOLS_DIR is caller-influenced, so only two absolute
# paths are accepted: the shared /tmp/opencode tools dir or the in-repo
# evidence tools dir. Refuses symlinks on the directory and on its parent (a
# symlinked /tmp/opencode resp. .evidence would let a build land anywhere) and
# creates the approved directory (0700) when missing.
validate_tools_dir() {
  local tools=$TOOLS_DIR parent="$REPO_ROOT/.evidence"
  case "$tools" in
    /tmp/opencode/txharbor-demo-tools) ;;
    "$REPO_ROOT/.evidence/portfolio-demo-tools") ;;
    *) fail "refused: tool dir must be /tmp/opencode/txharbor-demo-tools or $REPO_ROOT/.evidence/portfolio-demo-tools, got [$tools]" ;;
  esac
  case "$tools" in
    /tmp/*) parent=/tmp/opencode ;;
  esac
  if [[ -L "$tools" || -L "$parent" ]]; then
    fail "refused: tool dir path is a symlink: $tools"
  fi
  if [[ -e "$tools" && ! -d "$tools" ]]; then
    fail "refused: tool dir $tools exists and is not a directory"
  fi
  if [[ ! -d "$tools" ]]; then
    mkdir -m 700 -p "$tools" || fail "cannot create tool dir $tools"
  fi
  log "ok: tool dir $tools (parent $parent)"
}

# prepare_evidence_root -- EVIDENCE_BASE is FIXED (no caller override):
# $REPO_ROOT/.evidence/portfolio-demo. Refuses a symlinked or foreign-owned
# base (when it exists), creates the base only when missing (it is
# repo-ignored), then creates THIS run's root atomically -- a plain mkdir that
# must fail if the root already exists (no -p, no reuse) -- and lays out
# public/ + private/{keys,env,curl,logs}. Sets PUBLIC_DIR/PRIVATE_DIR/ENV_* and
# the KEY_* paths for the caller.
prepare_evidence_root() {
  local base="$REPO_ROOT/.evidence/portfolio-demo" parent="$REPO_ROOT/.evidence" sub
  EVIDENCE_BASE="$base"
  EVIDENCE_ROOT="$base/$RUN_ID"
  for sub in "$parent" "$base"; do
    if [[ -L "$sub" ]]; then
      fail "refused: $sub is a symlink"
    fi
    if [[ -e "$sub" ]]; then
      if [[ ! -d "$sub" ]]; then
        fail "refused: $sub exists and is not a directory"
      fi
      if [[ "$(stat -c %u "$sub")" != "$(id -u)" ]]; then
        fail "refused: $sub is not owned by uid $(id -u)"
      fi
    else
      mkdir -m 700 -p "$sub" || fail "cannot create $sub"
    fi
  done
  if [[ -L "$EVIDENCE_ROOT" || -e "$EVIDENCE_ROOT" ]]; then
    fail "refused: evidence root already exists: $EVIDENCE_ROOT"
  fi
  mkdir -m 700 "$EVIDENCE_ROOT" || fail "cannot create evidence root $EVIDENCE_ROOT"
  for sub in public private private/keys private/env private/curl private/logs; do
    mkdir -m 700 "$EVIDENCE_ROOT/$sub" || fail "cannot create $EVIDENCE_ROOT/$sub"
  done
  PUBLIC_DIR="$EVIDENCE_ROOT/public"
  PRIVATE_DIR="$EVIDENCE_ROOT/private"
  ENV_SERVE="$PRIVATE_DIR/env/serve.env"
  ENV_SIGNER="$PRIVATE_DIR/env/signer.env"
  ENV_WORKER="$PRIVATE_DIR/env/worker.env"
  KEY_CALLER="$PRIVATE_DIR/keys/caller.key"
  KEY_ISSUER="$PRIVATE_DIR/keys/issuer.key"
  KEY_SIGNER="$PRIVATE_DIR/keys/signer.key"
  KEY_SIGNER_CRED="$PRIVATE_DIR/keys/signer.cred"
}

# port_free <port> -- 0 when nothing accepts a TCP connect on 127.0.0.1:port.
# Pure bash /dev/tcp probe; never talks to docker.
port_free() {
  if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then
    return 1
  fi
  return 0
}

# pid_alive <pid> -- 0 while the process still runs. A zombie has already
# exited (it only waits for its parent to reap it), so it must never count as
# running: the state letter in /proc/<pid>/stat is the authoritative check.
pid_alive() {
  local pid=$1 stat
  kill -0 "$pid" 2>/dev/null || return 1
  stat=$(cat /proc/"$pid"/stat 2>/dev/null) || return 1
  stat=${stat##*\) }   # strip "pid (comm) ": comm may contain spaces/parens
  [[ "${stat%% *}" != "Z" ]]
}

# with_timeout <secs> <cmd...> -- run cmd under the host `timeout` binary
# (declared prerequisite, checked in preflight): every psql/cast/docker call
# is bounded so a hung daemon or socket cannot hang the run. A timeout fails
# the run with the command name (124, or 137 when TERM had to be escalated by
# `timeout` itself); any other status is passed through unchanged.
with_timeout() {
  local secs=$1 rc=0
  shift
  timeout --kill-after=5 "$secs" "$@" || rc=$?
  if (( rc == 124 || rc == 137 )); then
    fail "timeout: [$1] did not finish within ${secs}s (rc=$rc)"
  fi
  return "$rc"
}

# start_detached <env-file> <log-file> [<var-name> <var-file>] -- <cmd...>
# Start one long-lived service: a subshell sources the env file with `set -a`
# (so every key reaches the child), optionally exports ONE secret read from a
# 0600 file (the export is the point -- a bare assignment would not reach the
# exec'd child), then `exec`s the command with stdout+stderr appended to the
# log. The secret never appears in argv or in the log. Prints the child PID
# and stores it in STARTED_PID.
#
# A caller that must later `wait` for the child (cleanup, graceful shutdown)
# reads STARTED_PID instead of capturing stdout with $(...): a PID captured in
# a command substitution belongs to a subshell, so `wait` could never report
# that child's exit status from this shell.
start_detached() {
  local env_file=$1 log_file=$2
  shift 2
  local var_name="" var_file=""
  if [[ "${1:-}" != "--" ]]; then
    var_name=${1:-}
    var_file=${2:-}
    shift 2
  fi
  if [[ "${1:-}" != "--" ]]; then
    fail "start_detached: expected -- before the command"
  fi
  shift
  (
    set -a
    . "$env_file"
    set +a
    if [[ -n "$var_name" ]]; then
      export "$var_name"="$(cat "$var_file")"
    fi
    exec "$@"
  ) >> "$log_file" 2>&1 &
  STARTED_PID=$!
  printf '%s\n' "$STARTED_PID"
}

# with_env <env-file> <cmd...> -- run cmd with the file sourced (set -a).
# The env file holds no secret that the process must not see here; the worker
# credential is injected separately, outside any file.
with_env() {
  local env_file=$1
  shift
  ( set -a; . "$env_file"; set +a; exec "$@" )
}

# graceful_stop <pid> <name> [seconds] -- TERM only, bounded wait, NO KILL
# escalation ever: a process that ignores TERM is left running (retained) and
# reported, returning 1. When the process does exit within the bound, its exit
# status is returned (a process that dies from the TERM reports 143).
graceful_stop() {
  local pid=$1 name=$2 secs=${3:-30} st=0
  local deadline=$((SECONDS + secs))
  if pid_alive "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
  fi
  while pid_alive "$pid"; do
    if (( SECONDS >= deadline )); then
      warn "$name (pid $pid) did not exit within ${secs}s; left running (retained), no further signal sent"
      return 1
    fi
    sleep 0.2
  done
  if wait "$pid" 2>/dev/null; then
    :
  else
    st=$?
  fi
  return "$st"
}

# stop_owned_containers -- stop ONLY containers this run can prove it owns:
# the docker query selects by compose project AND by the per-run label
# txharbor.demo.run_token, and the result is intersected with the IDs captured
# at start. Never `compose stop/down`, never a project-wide stop. An empty
# intersection (nothing provably owned) stops nothing and returns 0; a failed
# query or a failed stop returns 1.
stop_owned_containers() {
  local -a owned=()
  local id listed
  if ! listed=$(with_timeout 15 "${DOCKER[@]}" ps -aq \
      --filter "label=com.docker.compose.project=$PROJECT" \
      --filter "label=txharbor.demo.run_token=$RUN_TOKEN" 2>&1); then
    warn "cleanup: cannot list this run's containers (project+run_token): $listed"
    return 1
  fi
  for id in "${CONTAINER_IDS[@]}"; do
    if [[ -n "$id" ]] && printf '%s\n' "$listed" | grep -qx "$id"; then
      owned+=("$id")
    fi
  done
  if (( ${#owned[@]} == 0 )); then
    log "cleanup: no container carries both project $PROJECT and run_token $RUN_TOKEN; nothing is provably owned, so nothing was stopped (retained)"
    return 0
  fi
  log "cleanup: stopping own containers: ${owned[*]}"
  # The subshell keeps a hard failure (e.g. with_timeout's own `fail`, which
  # exits) from aborting the EXIT path: this must return, so the caller can
  # finish writing the run record.
  if ! ( with_timeout 60 "${DOCKER[@]}" stop "${owned[@]}" ); then
    warn "cleanup: docker stop failed for the owned containers"
    return 1
  fi
  return 0
}

# probe_http_code <url> -- bounded readiness probe. `--disable` is the FIRST
# argument (curl must see it before any .curlrc could be read), then the
# explicit direct-connection pin and timeouts; never prints a body.
probe_http_code() {
  curl --disable --noproxy '*' --connect-timeout 2 --max-time 5 \
    --silent --output /dev/null --write-out '%{http_code}' "$1" || true
}

# scan_public_for_secrets <public-dir> <patterns-file> -- grep -RFq over the
# public evidence with the exact secret values (one per line). rc 0 means a
# secret is present -> fail (the match itself is never printed); rc 1 is the
# clean result; any other rc is a scan error and also fails.
scan_public_for_secrets() {
  local public=$1 patterns=$2 rc=0
  grep -RFq -f "$patterns" "$public" || rc=$?
  case "$rc" in
    0) fail "secret material found in public evidence ($public); content not printed" ;;
    1) log "ok: public evidence carries no secret value" ;;
    *) fail "secrets scan failed (grep rc=$rc over $public)" ;;
  esac
}

# --- one-time signer key: capture + strict parse (pinned foundry v1.8.1) ----
# Verified on the pinned image (foundry v1.8.1): `cast wallet new` writes the
# machine pair "0x<addr>\t0x<key>" as its ONLY stdout line, while the human
# lines (which also carry the key) go to stderr. Both streams are captured
# into the given private 0600 files; nothing is echoed to the caller's
# stdout/stderr and neither stream is ever merged into a public log.
wallet_new_capture() { # wallet_new_capture <stdout-file> <stderr-file>
  local out=$1 err=$2
  ( umask 077; : >"$out" ) && chmod 600 "$out"
  ( umask 077; : >"$err" ) && chmod 600 "$err"
  with_timeout 30 "${CAST[@]}" wallet new >"$out" 2>"$err"
}

# wallet_parse_pair <stdout-file> -- strict parse of the single stdout line:
# exactly two TAB-separated columns (0x+40hex address, 0x+64hex key). Rejects
# empty input, multiple lines, extra columns, spaces/colons (the human format)
# and malformed hex. Sets WALLET_ADDR (lowercased) and WALLET_KEY; prints
# nothing and never embeds the raw text in an error message.
wallet_parse_pair() {
  local out=$1 line addr key extra
  local -a lines=()
  WALLET_ADDR=""
  WALLET_KEY=""
  [[ -f "$out" ]] || { warn "wallet stdout capture missing: $out"; return 1; }
  mapfile -t lines < "$out"
  if (( ${#lines[@]} != 1 )); then
    warn "wallet stdout must be exactly one line (got ${#lines[@]}); see $out"
    return 1
  fi
  line=${lines[0]}
  if [[ "$line" == *" "* || "$line" == *":"* ]]; then
    warn "wallet stdout is not the machine pair (space/colon present); see $out"
    return 1
  fi
  IFS=$'\t' read -r addr key extra <<<"$line"
  if [[ -n "${extra:-}" || -z "${addr:-}" || -z "${key:-}" ]]; then
    warn "wallet stdout must have exactly two TAB-separated columns; see $out"
    return 1
  fi
  if [[ ! "$addr" =~ ^0x[0-9a-fA-F]{40}$ ]]; then
    warn "wallet stdout column 1 is not 0x+40hex; see $out"
    return 1
  fi
  if [[ ! "$key" =~ ^0x[0-9a-fA-F]{64}$ ]]; then
    warn "wallet stdout column 2 is not 0x+64hex; see $out"
    return 1
  fi
  WALLET_ADDR=$(lower_hex "$addr")
  WALLET_KEY="$key"
  return 0
}

# untrack_pid <pid> -- drop a reaped PID from the OWNED_PIDS array so the EXIT
# trap never waits on it a second time.
untrack_pid() {
  local pid=$1 p
  local -a out=()
  for p in "${OWNED_PIDS[@]:-}"; do
    if [[ -n "$p" && "$p" != "$pid" ]]; then
      out+=("$p")
    fi
  done
  OWNED_PIDS=("${out[@]:-}")
}
