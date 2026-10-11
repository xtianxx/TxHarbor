#!/usr/bin/env bash
# TxHarbor portfolio demo: shared shell helpers.
#
# Sourced by run_demo.sh. Defines functions only -- no top-level side effects,
# no external dependencies beyond the demo toolset (democtl, docker, curl,
# psql/cast inside the demo containers). Amounts are never compared with bash
# arithmetic: everything uint256 goes through `democtl uint256`.
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

# port_free <port> -- 0 when nothing accepts a TCP connect on 127.0.0.1:port.
# Pure bash /dev/tcp probe; never talks to docker.
port_free() {
  if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then
    return 1
  fi
  return 0
}

# with_env <env-file> <cmd...> -- run cmd with the file sourced (set -a).
# The env file holds no secret that the process must not see here; the worker
# credential is injected separately, outside any file.
with_env() {
  local env_file=$1
  shift
  ( set -a; . "$env_file"; set +a; exec "$@" )
}

# graceful_stop <pid> <name> [seconds] -- TERM, bounded wait, KILL escalation
# only for this PID. Returns the exit status; a KILL escalation returns 137 so
# the caller must treat it as a failure.
graceful_stop() {
  local pid=$1 name=$2 secs=${3:-30} st=0
  local deadline=$((SECONDS + secs))
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
  fi
  while kill -0 "$pid" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      warn "$name (pid $pid) did not exit within ${secs}s; escalating to KILL"
      kill -KILL "$pid" 2>/dev/null || true
      st=137
      break
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
