#!/usr/bin/env bash
# Log-filter regression suite (pure mocks: NO cast, NO chain, NO docker, NO PG,
# NO network, no secrets).
#
# It EXECUTES the real `check_transfer_log_uniqueness` extracted from
# run_demo.sh (never a copy) against a SYNTHETIC `cast_logs` stub that filters a
# fixture array by the topics it actually receives. The stub only mirrors topic
# filter semantics; it is not a real Foundry run.
#
# What it proves:
#   C1 the real checker passes on a synthetic 4-Transfer chain (mint, FUND,
#      DEPOSIT, withdrawal) with the real argv = token + whole-chain range +
#      three RAW 32-byte topics, and publishes only after every check passed;
#   C2 the pre-fix signature-form query (extracted from the pre-fix commit
#      069a431) reproduces the historical count 4 on the same stub and fails;
#   C3 every mutated/malformed response fails closed with the raw JSON, the
#      filter and the stderr persisted privately and nothing published;
#   C4 negative mutations of the production section (wrong count target, wrong
#      raw topic) are caught — no false green.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
DEMO_DIR=$(cd "$HERE/../.." && pwd)
REPO=$(cd "$DEMO_DIR/../.." && pwd)
RUN_DEMO="${RUN_DEMO_UNDER_TEST:-$DEMO_DIR/run_demo.sh}"
LIB="$DEMO_DIR/lib.sh"
FIX="$HERE/fixtures"
PRE_FIX_SHA="069a431b6d4028e7f27d0af8d5042c930c73ed31"

[[ -f "$RUN_DEMO" ]] || { echo "FAIL: missing $RUN_DEMO" >&2; exit 1; }
[[ -f "$LIB" ]] || { echo "FAIL: missing $LIB" >&2; exit 1; }
# shellcheck source=../../lib.sh
. "$LIB"

WORK=$(mktemp -d /tmp/log-filter-checks.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

# --- synthetic constants: obviously fake, no real address/hash/secret -------
TOKEN="0x1111111111111111111111111111111111111111"
SIGNER_ADDR="0x2222222222222222222222222222222222222222"
RECIPIENT="0x3333333333333333333333333333333333333333"
WITHDRAW="1000000000000000000000"
TRANSFER_SIG="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
HEAD="9"
SIGNING_TX="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOPIC_FROM="0x000000000000000000000000${SIGNER_ADDR#0x}"
TOPIC_TO="0x000000000000000000000000${RECIPIENT#0x}"
SIGNING_RECEIPT=$(cat "$FIX/signing-receipt-synthetic.json")
FIXTURE="$FIX/chain-logs-synthetic.json"

# --- stubs ------------------------------------------------------------------
DEMOCTL=mocktool
mocktool() { python3 "$HERE/mocktool.py" "$@"; }
docker() { printf 'docker %s\n' "$*" >> "$WORK/docker.calls"; return 99; }
cast()   { printf 'cast %s\n' "$*" >> "$WORK/cast.calls"; return 99; }

ARGV_REC=""
MOCK_RESPONSE=""
MOCK_RC=0
cast_logs() {
  printf '%s\n' "$*" >> "$ARGV_REC"
  if (( MOCK_RC != 0 )); then
    printf 'stub cast logs failure\n' >&2
    return "$MOCK_RC"
  fi
  if [[ -n "$MOCK_RESPONSE" ]]; then
    cat "$MOCK_RESPONSE"
    return 0
  fi
  mocktool castlogs "$FIXTURE" "$@"
}

pass_count=0
fail_count=0
ok()  { pass_count=$((pass_count + 1)); printf 'ok: %s\n' "$1"; }
bad() { fail_count=$((fail_count + 1)); printf 'FAIL: %s\n' "$1" >&2; }

# extract_log_check <script> -- the real checker, verbatim.
extract_log_check() {
  awk '/^check_transfer_log_uniqueness\(\) \{/ {f=1}
       f {print}
       f && /^\}/ {exit}' "$1"
}

# run_case <fn-text> <workdir> [preset-response] [stub-rc] -- runs the extracted
# checker in a subshell against the stub; returns the checker's status.
run_case() {
  local fn=$1 wd=$2 resp=${3:-} rc=${4:-0}
  rm -rf "$wd"
  mkdir -p "$wd/private/logs" "$wd/public/chain"
  (
    ARGV_REC="$wd/argv" FIXTURE="$FIXTURE" MOCK_RESPONSE="$resp" MOCK_RC="$rc"
    PRIVATE_DIR="$wd/private" PUBLIC_DIR="$wd/public" DEMOCTL=mocktool
    TOKEN="$TOKEN" SIGNER_ADDR="$SIGNER_ADDR" RECIPIENT="$RECIPIENT"
    WITHDRAW="$WITHDRAW" TRANSFER_SIG="$TRANSFER_SIG"
    eval "$fn"
    check_transfer_log_uniqueness "$HEAD" "$SIGNING_RECEIPT" "$SIGNING_TX"
  ) > "$wd/stdout" 2> "$wd/stderr"
}

# mutate_response <base-fixture> <field[:idx]> <value> <out> -- one payment log.
mutate_response() {
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
logs = doc["logs"] if isinstance(doc, dict) else doc
pay = logs[3]
field, value = sys.argv[2], sys.argv[3]
if value in ("true", "false"):
    value = value == "true"
if ":" in field:
    name, idx = field.split(":")
    pay[name][int(idx)] = value
else:
    pay[field] = value
json.dump([pay], open(sys.argv[4], "w"), separators=(",", ":"))
PY
}

# preset <name> -- write a preset response file for the count/JSON cases.
preset() {
  local name=$1 out="$WORK/preset-$1.json"
  case "$name" in
    duplicates) python3 - "$FIXTURE" "$out" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
logs = doc["logs"] if isinstance(doc, dict) else doc
json.dump([logs[3], logs[3]], open(sys.argv[2], "w"), separators=(",", ":"))
PY
      ;;
    empty) printf '[]\n' > "$out" ;;
    all-four) python3 - "$FIXTURE" "$out" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
logs = doc["logs"] if isinstance(doc, dict) else doc
json.dump(logs, open(sys.argv[2], "w"), separators=(",", ":"))
PY
      ;;
    bad-json) printf 'this is not json\n' > "$out" ;;
    bad-json-trailing) printf 'not json\n\n\n' > "$out" ;;
    bad-json-nul) printf 'bad\0json\n' > "$out" ;;
    string-false) python3 - "$FIXTURE" "$out" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
logs = doc["logs"] if isinstance(doc, dict) else doc
pay = dict(logs[3])
pay["removed"] = "false"          # the JSON STRING, not the boolean
json.dump([pay], open(sys.argv[2], "w"), separators=(",", ":"))
PY
      ;;
    string-false-decoy) python3 - "$FIXTURE" "$out" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
logs = doc["logs"] if isinstance(doc, dict) else doc
pay = dict(logs[3])
pay["removed"] = "false"          # direct member: the JSON STRING
pay["extra"] = {"removed": False}  # nested decoy: a real boolean elsewhere
json.dump([pay], open(sys.argv[2], "w"), separators=(",", ":"))
PY
      ;;
    *) echo "unknown preset $name" >&2; return 1 ;;
  esac
  printf '%s\n' "$out"
}

# ===========================================================================
# C1 — the real checker passes with raw 32-byte topics and publishes late.
# ===========================================================================
FN_REAL=$(extract_log_check "$RUN_DEMO")
[[ -n "$FN_REAL" ]] || bad "C1: check_transfer_log_uniqueness not found in $RUN_DEMO"
WD="$WORK/c1"
if run_case "$FN_REAL" "$WD"; then
  ok "C1: real checker passes on the synthetic 4-Transfer chain (exactly one payment)"
else
  bad "C1: real checker failed: $(tail -2 "$WD/stderr" | tr '\n' ' ')"
fi
argv=$(cat "$WD/argv" 2>/dev/null || true)
expected="--address $TOKEN --from-block 0 --to-block $HEAD $TRANSFER_SIG $TOPIC_FROM $TOPIC_TO"
[[ "$argv" == "$expected" ]] \
  && ok "C1: real argv = token + whole-chain range (0..head) + three raw topics" \
  || bad "C1: argv mismatch: [$argv]"
[[ "$argv" != *"Transfer(address,address,uint256)"* ]] \
  && ok "C1: no event-signature form in the real argv" \
  || bad "C1: the signature form is still passed to cast logs"
for t in "$TRANSFER_SIG" "$TOPIC_FROM" "$TOPIC_TO"; do
  [[ "$t" =~ ^0x[0-9a-f]{64}$ ]] || bad "C1: topic is not 32-byte lowercase hex: [$t]"
done
ok "C1: all three topics are 32-byte lowercase hex"
for f in transfer-log-query.filter.txt transfer-log-query.json transfer-log-query.err; do
  [[ -f "$WD/private/logs/$f" ]] || bad "C1: missing private evidence $f"
done
ok "C1: filter, raw JSON and stderr persisted privately (0600 dir)"
[[ -f "$WD/public/chain/transfer-log-query.json" && -f "$WD/public/chain/transfer-log-query.filter.txt" ]] \
  && ok "C1: the positive case published the filter and raw JSON" \
  || bad "C1: the positive case did not publish"
grep -qx "topic1=$TOPIC_FROM" "$WD/public/chain/transfer-log-query.filter.txt" \
  && grep -qx "topic2=$TOPIC_TO" "$WD/public/chain/transfer-log-query.filter.txt" \
  && ok "C1: the published filter carries the same raw topic1/topic2 variables" \
  || bad "C1: published filter does not match the queried topics"

# ===========================================================================
# C2 — the pre-fix signature-form query reproduces the historical count 4.
# ===========================================================================
old_snip=$(git -C "$REPO" show "$PRE_FIX_SHA:scripts/portfolio-demo/run_demo.sh" \
  | awk '/^  logs_out=\$\(cast_logs --address/ {f=1} f {print} f && /^  expect_eq "getLogs Transfer/ {exit}')
[[ -n "$old_snip" ]] || bad "C2: cannot extract the pre-fix query from $PRE_FIX_SHA"
WD="$WORK/c2"
rm -rf "$WD"; mkdir -p "$WD/private/logs" "$WD/public/chain"
old_rc=0
(
  ARGV_REC="$WD/argv" FIXTURE="$FIXTURE" MOCK_RESPONSE="" MOCK_RC=0
  PRIVATE_DIR="$WD/private" PUBLIC_DIR="$WD/public" DEMOCTL=mocktool
  TOKEN="$TOKEN" SIGNER_ADDR="$SIGNER_ADDR" RECIPIENT="$RECIPIENT"
  observed_head="$HEAD"
  eval "$old_snip"
) > "$WD/stdout" 2> "$WD/stderr" || old_rc=$?
[[ "$old_rc" -ne 0 ]] \
  && ok "C2: pre-fix form fails the count target on the same stub" \
  || bad "C2: pre-fix form unexpectedly passed"
grep -q 'want \[1\] got \[4\]' "$WD/stderr" \
  && ok "C2: pre-fix form reproduced the historical count 4 (mint+FUND+DEPOSIT+withdrawal)" \
  || bad "C2: pre-fix count was not 4: [$(tr '\n' ' ' < "$WD/stderr")]"
grep -q 'Transfer(address,address,uint256)' "$WD/argv" \
  && ok "C2: pre-fix argv used the signature form (the defect this batch fixes)" \
  || bad "C2: pre-fix argv did not use the signature form"

# ===========================================================================
# C3 — mutated/malformed responses fail closed with private evidence kept.
# ===========================================================================
neg_case() { # <label> <field[:idx]> <value>
  local label=$1 field=$2 value=$3 wd="$WORK/neg-$1" resp="$WORK/neg-$1.json"
  mutate_response "$FIXTURE" "$field" "$value" "$resp"
  if run_case "$FN_REAL" "$wd" "$resp"; then
    bad "C3($label): checker PASSED a mutated response"
    return
  fi
  if [[ -f "$wd/private/logs/transfer-log-query.json" && -f "$wd/private/logs/transfer-log-query.filter.txt" ]]; then
    ok "C3($label): refused and kept the raw JSON + filter privately"
  else
    bad "C3($label): refused but the private evidence is missing"
  fi
  assert_no_public "C3($label)" "$wd"
}
preset_case() { # <label> <preset-name>
  local label=$1 name=$2 resp wd="$WORK/neg-$1"
  resp=$(preset "$name")
  if run_case "$FN_REAL" "$wd" "$resp"; then
    bad "C3($label): checker PASSED the [$name] response"
    return
  fi
  if [[ -f "$wd/private/logs/transfer-log-query.json" ]]; then
    ok "C3($label): refused and kept the raw response privately"
  else
    bad "C3($label): refused but the raw response was not kept"
  fi
  assert_no_public "C3($label)" "$wd"
}

# assert_no_public <label> <workdir> -- a rejected response must publish NOTHING:
# both the raw JSON and the filter metadata stay private.
assert_no_public() {
  local label=$1 wd=$2 found=0 f
  for f in transfer-log-query.json transfer-log-query.filter.txt; do
    if [[ -e "$wd/public/chain/$f" ]]; then
      found=1
      bad "$label: a failing case published public/$f"
    fi
  done
  if (( found == 0 )); then
    ok "$label: nothing was published (raw JSON and filter both absent)"
  fi
}

# retention_case <label> <preset-name> -- the private raw file must equal the
# response byte for byte (trailing newlines and NUL bytes included).
retention_case() {
  local label=$1 name=$2 resp wd="$WORK/ret-$1" got
  resp=$(preset "$name")
  run_case "$FN_REAL" "$wd" "$resp" >/dev/null 2>&1 || true
  got="$wd/private/logs/transfer-log-query.json"
  if [[ -f "$got" ]] && cmp -s "$resp" "$got"; then
    ok "C3($label): private raw file retains the response byte-for-byte ($(wc -c < "$resp") bytes)"
  else
    bad "C3($label): raw retention differs (want $(wc -c < "$resp") bytes, got $(wc -c < "$got" 2>/dev/null || echo 0))"
  fi
  assert_no_public "C3($label)" "$wd"
}

neg_case "wrong-token"          "address"     "0x9999999999999999999999999999999999999999"
neg_case "wrong-topic0"         "topics:0"    "0x0000000000000000000000000000000000000000000000000000000000000001"
neg_case "wrong-topic1"         "topics:1"    "0x0000000000000000000000008888888888888888888888888888888888888888"
neg_case "wrong-topic2"         "topics:2"    "0x0000000000000000000000007777777777777777777777777777777777777777"
neg_case "wrong-data"           "data"        "0x0000000000000000000000000000000000000000000000000000000000000001"
neg_case "wrong-txhash"         "transactionHash" "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
neg_case "wrong-blockhash"      "blockHash"   "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
neg_case "wrong-blocknumber"    "blockNumber" "0x8"
neg_case "wrong-logindex"       "logIndex"    "0x1"
neg_case "removed-true"         "removed"     "true"
preset_case "duplicate-payments" duplicates
preset_case "zero-matches"       empty
preset_case "filter-not-narrowed" all-four
preset_case "bad-json"           bad-json
preset_case "string-false-removed" string-false
preset_case "string-false-nested-decoy" string-false-decoy
# Control: the superseded raw-byte grep accepted the nested decoy, which is why
# the direct member is now checked on the PARSED object's pretty form.
if grep -Eq '"removed"[[:space:]]*:[[:space:]]*false([[:space:]]*[,}]|[[:space:]]*$)' \
     "$(preset string-false-decoy)"; then
  ok "C3(raw-grep control): the superseded grep is defeated by the nested decoy (parsed check required)"
else
  bad "C3(raw-grep control): the superseded grep already rejected the decoy; the control is not meaningful"
fi
retention_case "trailing-newlines" bad-json-trailing
retention_case "nul-bytes"         bad-json-nul

WD="$WORK/neg-cast-rc"
if run_case "$FN_REAL" "$WD" "" 1; then
  bad "C3(cast-logs-rc1): checker PASSED a failed cast logs"
else
  if [[ -f "$WD/private/logs/transfer-log-query.json" && -f "$WD/private/logs/transfer-log-query.err" ]]; then
    ok "C3(cast-logs-rc1): refused, raw output and stderr kept privately"
  else
    bad "C3(cast-logs-rc1): failure evidence missing"
  fi
  if [[ ! -s "$WD/private/logs/transfer-log-query.json" ]]; then
    ok "C3(cast-logs-rc1): empty stdout is retained as an empty file (no injected newline)"
  else
    bad "C3(cast-logs-rc1): empty stdout was altered ($(wc -c < "$WD/private/logs/transfer-log-query.json") bytes)"
  fi
  assert_no_public "C3(cast-logs-rc1)" "$WD"
fi

# ===========================================================================
# C4 — negative mutations of the production section must not pass.
# ===========================================================================
mutate_script() { # <src> <dst> <old> <new>
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import sys
src, dst, old, new = sys.argv[1:5]
text = open(src).read()
if old not in text:
    sys.exit(3)
open(dst, "w").write(text.replace(old, new, 1))
PY
}
mutant_control() { # <label> <old> <new>
  local label=$1 old=$2 new=$3 path="$WORK/mutant-$1.sh" fn rc=0
  if ! mutate_script "$RUN_DEMO" "$path" "$old" "$new"; then
    bad "C4($label): mutation anchor not found"
    return
  fi
  fn=$(extract_log_check "$path")
  run_case "$fn" "$WORK/mutant-$1-wd" >/dev/null 2>&1 || rc=$?
  [[ "$rc" -ne 0 ]] \
    && ok "C4($label): mutated production section fails the positive case" \
    || bad "C4($label): mutated production section still passed (false green)"
}
mutant_control "count-target-4" \
  'expect_eq "getLogs Transfer(signer->recipient) count at head $observed_head" "1" "$logs_n"' \
  'expect_eq "getLogs Transfer(signer->recipient) count at head $observed_head" "4" "$logs_n"'
mutant_control "wrong-raw-topic1" \
  'topic_from=$(lower_hex "0x000000000000000000000000${SIGNER_ADDR#0x}")' \
  'topic_from=$(lower_hex "0x000000000000000000000000${RECIPIENT#0x}")'

# ===========================================================================
[[ ! -f "$WORK/docker.calls" ]] && ok "suite: no docker call" || bad "suite: docker called"
[[ ! -f "$WORK/cast.calls" ]] && ok "suite: no real cast call (the stub replaced it)" || bad "suite: real cast called"
printf '\nlog-filter checks: %d passed, %d failed\n' "$pass_count" "$fail_count"
[[ "$fail_count" -eq 0 ]] || exit 1
