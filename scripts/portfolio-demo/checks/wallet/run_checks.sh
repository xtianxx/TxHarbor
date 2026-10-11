#!/usr/bin/env bash
# Wallet capture/parse regression (pure mock; no docker/cast/network/RPC).
# Exercises the real lib.sh functions `wallet_new_capture` / `wallet_parse_pair`
# against the verified pinned-image (foundry v1.8.1) stream shape: the machine
# pair on stdout, the human lines (which also name the key) on stderr.
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
ROOT="$REPO_ROOT/.evidence/portfolio-demo-checks/walletfix/$(date -u +%Y%m%d%H%M%S)-$$"
mkdir -m 700 -p "$ROOT/scratch/private/logs" "$ROOT/scratch/private/keys"
LOG="$ROOT/wallet-checks.log"
# shellcheck source=../../lib.sh
. "$REPO_ROOT/scripts/portfolio-demo/lib.sh"
CAST=("$SCRIPT_DIR/mockbin/cast-wallet")
PRIVATE_DIR="$ROOT/scratch/private"
DUMMY_ADDR="0x1111111111111111111111111111111111111111"
DUMMY_KEY="0x2222222222222222222222222222222222222222222222222222222222222222"
RC=0
pass() { printf 'PASS %s\n' "$1" >>"$LOG"; printf 'PASS %s\n' "$1"; }
failc() { printf 'FAIL %s\n' "$1" >>"$LOG"; printf 'FAIL %s\n' "$1"; RC=1; }

happy_flow() {
  local out="$PRIVATE_DIR/logs/w1.out" err="$PRIVATE_DIR/logs/w1.err"
  MOCK_MODE=happy wallet_new_capture "$out" "$err" || return 1
  wallet_parse_pair "$out" || return 1
  [[ "$WALLET_ADDR" == "$DUMMY_ADDR" ]] || return 1
  [[ "$WALLET_KEY" =~ ^0x[0-9a-f]{64}$ ]] || return 1
  [[ "$(stat -c %a "$out")" == "600" && "$(stat -c %a "$err")" == "600" ]] || return 1
  grep -qF "$DUMMY_KEY" "$err" || return 1
  local keyfile="$PRIVATE_DIR/keys/w1.key"
  ( umask 077; printf '%s\n' "$WALLET_KEY" >"$keyfile" )
  chmod 600 "$keyfile"
  [[ "$(stat -c %a "$keyfile")" == "600" ]] || return 1
  grep -qxF "$DUMMY_KEY" "$keyfile" || return 1
  return 0
}

# 1) happy path: real functions produce the address and a 0600 key file
if happy_flow; then pass "happy: addr parsed, key file 0600, streams split"; else failc "happy path"; fi

# 2) no leak: the functions' own stdout/stderr carry no key material
outer="$ROOT/scratch/outer.log"
if happy_flow >"$outer" 2>&1; then
  if grep -qF "$DUMMY_KEY" "$outer"; then failc "public capture leaked the key"; else pass "no key on the functions' stdout/stderr"; fi
else
  failc "happy flow under outer capture"
fi

# 3) non-zero exit: capture fails, parse of the empty capture fails, no key
out="$PRIVATE_DIR/logs/w2.out"; err="$PRIVATE_DIR/logs/w2.err"
if MOCK_MODE=exit7 wallet_new_capture "$out" "$err"; then failc "exit7 capture unexpectedly succeeded"; else pass "exit7: capture rc != 0"; fi
if wallet_parse_pair "$out"; then failc "exit7 parse unexpectedly succeeded"; else pass "exit7: parse rejected"; fi
[[ -z "$WALLET_KEY" ]] && pass "exit7: no usable key" || failc "exit7: key set"

# 4) bad schemas: strict parse rejects every non-pair shape
for mode in multiline extracol badaddr badkey empty human; do
  out="$PRIVATE_DIR/logs/w3-$mode.out"; err="$PRIVATE_DIR/logs/w3-$mode.err"
  MOCK_MODE="$mode" wallet_new_capture "$out" "$err" || true
  if wallet_parse_pair "$out"; then
    failc "$mode: parse unexpectedly accepted"
  else
    if [[ -z "$WALLET_KEY" ]]; then pass "$mode: rejected, no usable key"; else failc "$mode: key set"; fi
  fi
done

# 5) the case log itself never carries the dummy key
if grep -qF "$DUMMY_KEY" "$LOG"; then failc "case log leaked the dummy key"; else pass "case log has no dummy key"; fi

printf '== summary: walletfix checks rc=%s\n' "$RC"
printf '== evidence root: %s\n' "$ROOT"
exit "$RC"
