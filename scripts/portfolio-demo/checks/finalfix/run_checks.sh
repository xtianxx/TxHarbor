#!/usr/bin/env bash
# RESUME-03-FINALFIX mock regression suite for the TxHarbor portfolio demo.
#
# Mocks only: this suite runs against checks/finalfix/mockbin (a STRICT mock
# curl that fails when --disable is not the first argument). No docker, no
# solc run, no psql/cast, no listener, no network, no service is started; the
# only build performed is `go build ./scripts/portfolio-demo/democtl`.
#
# Cases (H1..H5 of the RESUME-03-FINALFIX ticket):
#   H1  1 http-post-first-arg, 2 http-post-old-order-control, 3 probe-http-code
#   H2  4 signer-key-path (sandbox prepare_evidence_root + signer-env heredoc)
#   H3  5 tools-dir-not-approved, 6 tools-dir-root-symlink,
#       7 tools-dir-build-symlink-sentinel, 8 verify-run-scoped
#   H4  9 stop-owned-retain, 10 stop-owned-normal, 11 stop-owned-failed-child
#   H5  12 receipt-binding-good, 13 receipt-binding-wrong-hash
# (case 0 build-democtl is the suite's single `go build`; its log is a case
# log like any other, so a build failure fails the suite.)
#
# Fixture addressing -- fixtures/receipt-good.json encodes exactly this fixed
# dummy transfer (all lowercase, documented here so the H5 cases set the
# receipt expectations to match):
#   TOKEN_FIXTURE      token contract (tx `to`, log address)
#                      = 0x5fbdb2315678afecb367f032d93f642f64180aa3
#   SENDER_FIXTURE     tx `from` / Transfer topic1
#                      = 0x70997970c51812dc3a010c7d01b50e0d17dc79c8
#   RECIPIENT_FIXTURE  Transfer topic2
#                      = 0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc
#   AMOUNT_FIXTURE=42  log data = 0x000...02a (uint256 hex, 64 digits)
#   TXHASH_FIXTURE     = 0x1f4a5d6e7f8091a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6
#   BLOCKHASH_FIXTURE  = 0x2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a
#   blockNumber        = "0x10"
#   TXHASH_MISMATCH_FIXTURE (case 13 control) = 0xdeadbeef repeated 8x
#
# Evidence: every case writes .evidence/portfolio-demo-checks/03-finalfix/
# 03-finalfix-<name>.log and appends its provenance to .../source-hashes.txt.
# Each invocation gets a unique run id; all scratch lives under .../runs/<rid>/
# and is kept -- nothing in this tree is ever deleted.
#
# Usage: bash scripts/portfolio-demo/checks/finalfix/run_checks.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/../.." && pwd)"
CHECK_BIN="$SCRIPT_DIR/mockbin"
FIXTURES="$SCRIPT_DIR/fixtures"
CHECKS_ROOT="$REPO_ROOT/.evidence/portfolio-demo-checks/03-finalfix"
RID="ff$(date -u +%Y%m%d%H%M%S)-$$"
RUNS="$CHECKS_ROOT/runs/$RID"
DEMOCTL="$RUNS/bin/democtl"
PATH="$CHECK_BIN:$PATH"
export PATH
cd "$REPO_ROOT"
mkdir -p "$CHECKS_ROOT" "$RUNS/bin"

# fixed fixture values (see the header): the H5 cases and the fixture agree
TOKEN_FIXTURE="0x5fbdb2315678afecb367f032d93f642f64180aa3"
SENDER_FIXTURE="0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
RECIPIENT_FIXTURE="0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"
TRANSFER_SIG_FIXTURE="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
AMOUNT_FIXTURE="42"
TXHASH_FIXTURE="0x1f4a5d6e7f8091a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6"
BLOCKHASH_FIXTURE="0x2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a"
TXHASH_MISMATCH_FIXTURE="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
# dummy signer key for the H2 sandbox (never printed; h2-check only writes it)
DUMMY_SIGNER_KEY="0x$(printf '11%.0s' {1..32})"

TOTAL=0
FAILED=0
CASE_LOG=""
CASE_NAME=""
CASE_PASS=1

log() { printf '%s\n' "$*"; }
rc_ok() {
  case "$1" in
    nonzero) [[ "$2" -ne 0 ]] ;;
    *) [[ "$2" == "$1" ]] ;;
  esac
}

stamp_hashes() { # stamp_hashes <case> -- provenance of the sources under test
  {
    printf '== %s %s rid=%s\n' "$1" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RID"
    printf 'head %s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
    (cd "$DEMO_DIR" && sha256sum run_demo.sh lib.sh compile_token.sh)
  } >>"$CHECKS_ROOT/source-hashes.txt"
}

case_dir() { # case_dir <case> -- per-run scratch dir; kept, never deleted
  local d="$RUNS/$1"
  mkdir -p "$d"
  printf '%s' "$d"
}

run_case() { # run_case <name> <expected_rc|nonzero> <cmd...>
  local name=$1 want=$2 rc=0
  shift 2
  local l="$CHECKS_ROOT/03-finalfix-$name.log"
  CASE_NAME=$name
  CASE_LOG=$l
  CASE_PASS=1
  {
    printf '== case %s %s rid=%s\n' "$name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$RID"
    printf '== cmd :'
    printf ' %q' "$@"
    printf '\n'
  } >"$l"
  "$@" >>"$l" 2>&1 || rc=$?
  printf '== rc  : %s (expected %s)\n' "$rc" "$want" >>"$l"
  TOTAL=$((TOTAL + 1))
  if rc_ok "$want" "$rc"; then
    log "PASS $name"
  else
    CASE_PASS=0
    FAILED=$((FAILED + 1))
    log "FAIL $name (rc=$rc want $want; log $l)"
  fi
  stamp_hashes "$name"
}

case_check() { # case_check <desc> <ere> -- extra assertion on the case log
  local desc=$1 pat=$2
  if grep -qE -- "$pat" "$CASE_LOG"; then
    printf 'check ok: %s\n' "$desc" >>"$CASE_LOG"
    if (( CASE_PASS == 1 )); then
      log "PASS $CASE_NAME (check: $desc)"
    else
      log "note: $CASE_NAME check ok ($desc) but the case already failed"
    fi
    return 0
  fi
  printf 'check FAIL: %s: no match for [%s]\n' "$desc" "$pat" >>"$CASE_LOG"
  log "FAIL $CASE_NAME (check: $desc)"
  if (( CASE_PASS == 1 )); then
    CASE_PASS=0
    FAILED=$((FAILED + 1))
  fi
}

# --- assertion helpers for probes ------------------------------------------
# Each ck_* must run inside a case probe subshell: on failure it prints a
# CHECK-FAIL line into the case log and exits that subshell non-zero (which
# run_case records as the case failure).
ck_eq() { # ck_eq <desc> <want> <got>
  [[ "$2" == "$3" ]] || { printf 'CHECK-FAIL: %s: want [%s] got [%s]\n' "$1" "$2" "$3"; exit 1; }
  printf 'ok: %s == %s\n' "$1" "$3"
}
ck_ne0() { # ck_ne0 <desc> <rc>
  [[ "$2" -ne 0 ]] || { printf 'CHECK-FAIL: %s: rc %s is zero\n' "$1" "$2"; exit 1; }
  printf 'ok: %s (rc=%s != 0)\n' "$1" "$2"
}
ck_has() { # ck_has <desc> <file> <ere>
  grep -qE -- "$3" "$2" || { printf 'CHECK-FAIL: %s: %s has no match for [%s]\n' "$1" "$2" "$3"; exit 1; }
  printf 'ok: %s\n' "$1"
}
ck_hasf() { # ck_hasf <desc> <file> <literal>
  grep -qF -- "$3" "$2" || { printf 'CHECK-FAIL: %s: %s has no match for [%s]\n' "$1" "$2" "$3"; exit 1; }
  printf 'ok: %s\n' "$1"
}
ck_lack() { # ck_lack <desc> <file> <ere>
  if grep -qE -- "$3" "$2"; then printf 'CHECK-FAIL: %s: %s unexpectedly matches [%s]\n' "$1" "$2" "$3"; exit 1; fi
  printf 'ok: %s\n' "$1"
}

# --- H1 -------------------------------------------------------------------
probe_http_post_first_arg() { # case 1: --disable is the FIRST curl argument
  (
    set -euo pipefail
    local d=$1
    local fn="$d/http_post.fn"
    export MOCK_LOG="$d/mock.log"
    : >"$MOCK_LOG"
    sed -n '/^http_post() {/,/^}/p' "$DEMO_DIR/run_demo.sh" >"$fn"
    printf 'extracted %s lines of http_post from run_demo.sh\n' "$(wc -l <"$fn")"
    ck_has 'extraction starts at the http_post definition' "$fn" '^http_post\(\) \{'
    ck_eq 'extraction ends at the closing brace' '}' "$(tail -n 1 "$fn")"
    bash -n "$fn" || { printf 'CHECK-FAIL: extracted http_post does not parse\n'; exit 1; }
    ck_hasf 'http_post pins --disable before the args' "$fn" 'curl --disable --noproxy'
    . "$fn"
    local conf="$d/api.conf" body="$d/body.json" out="$d/resp.json"
    printf 'header = "Authorization: Bearer FINALFIX-DUMMY-BEARER"\n' >"$conf"
    chmod 600 "$conf"
    printf '{"request_id":"finalfix-case1","amount":"42"}\n' >"$body"
    local rc=0 status=""
    status=$(http_post "$conf" "$body" "$out") || rc=$?
    printf 'http_post rc=%s status=%q out=%s\n' "$rc" "$status" "$out"
    ck_eq 'http_post rc' '0' "$rc"
    ck_eq 'http_post prints the HTTP status' '000' "$status"
    ck_has 'mock curl logged OK' "$MOCK_LOG" '^OK '
    ck_lack 'mock curl logged no VIOLATION' "$MOCK_LOG" '^VIOLATION '
    ck_has 'first curl argument is --disable' "$MOCK_LOG" '^argv: \[--disable\] \['
    ck_hasf '--noproxy * pinned' "$MOCK_LOG" '[--noproxy] [*]'
    ck_hasf '--connect-timeout pinned' "$MOCK_LOG" '[--connect-timeout]'
    ck_hasf '--max-time pinned' "$MOCK_LOG" '[--max-time]'
    ck_hasf 'conf passed via --config' "$MOCK_LOG" "[--config] [$conf]"
    ck_hasf 'body file passed as @path' "$MOCK_LOG" "[--data-binary] [@$body]"
    ck_lack 'bearer never reached curl argv' "$MOCK_LOG" 'FINALFIX-DUMMY-BEARER'
    printf 'ok: --disable first, pins present, conf-only bearer, status 000\n'
  )
}

probe_http_post_old_order_control() { # case 2: the strict mock catches the old bug
  (
    set -uo pipefail
    local d=$1
    export MOCK_LOG="$d/mock.log"
    : >"$MOCK_LOG"
    local conf="$d/api.conf" body="$d/body.json" out="$d/resp.json"
    printf 'header = "Authorization: Bearer FINALFIX-DUMMY-BEARER"\n' >"$conf"
    printf '{"request_id":"finalfix-case2"}\n' >"$body"
    local -a args=(--config "$conf" --silent --show-error --output "$out"
      --write-out '%{http_code}' --data-binary "@$body")
    local rc=0 got=""
    # the pre-fix order (run_demo.sh@a3451e8): the args came BEFORE --disable
    got=$(curl "${args[@]}" --disable --noproxy '*' --connect-timeout 5 --max-time 30) || rc=$?
    printf 'old-order curl rc=%s out=%q\n' "$rc" "$got"
    [[ "$rc" -ne 0 ]] || { printf 'CHECK-FAIL: strict mock accepted the old order\n'; exit 1; }
    [[ -z "$got" ]] || { printf 'CHECK-FAIL: old order still printed a status [%s]\n' "$got"; exit 1; }
    grep -qE '^VIOLATION ' "$MOCK_LOG" || { printf 'CHECK-FAIL: no VIOLATION line logged\n'; exit 1; }
    grep -qE '^argv: \[--config\] \[' "$MOCK_LOG" || { printf 'CHECK-FAIL: first arg was not --config\n'; exit 1; }
    printf 'ok: strict mock rejects the old order (VIOLATION, rc=%s) -- case 1 is meaningful\n' "$rc"
  )
}

probe_probe_http_code() { # case 3: lib.sh probe_http_code pins --disable first
  (
    set -euo pipefail
    # shellcheck source=../../lib.sh
    . "$DEMO_DIR/lib.sh"
    local d=$1
    export MOCK_LOG="$d/mock.log"
    : >"$MOCK_LOG"
    local rc=0 out=""
    out=$(probe_http_code "http://127.0.0.1:1/readyz") || rc=$?
    printf 'probe_http_code rc=%s out=%q\n' "$rc" "$out"
    ck_eq 'probe_http_code rc' '0' "$rc"
    ck_eq 'probe_http_code prints the status' '000' "$out"
    ck_has 'first curl argument is --disable' "$MOCK_LOG" '^argv: \[--disable\] \['
    ck_hasf '--noproxy * pinned' "$MOCK_LOG" '[--noproxy] [*]'
    ck_hasf 'bounded by --connect-timeout' "$MOCK_LOG" '[--connect-timeout]'
    ck_hasf 'bounded by --max-time' "$MOCK_LOG" '[--max-time]'
    ck_hasf 'probe URL passed last' "$MOCK_LOG" '[http://127.0.0.1:1/readyz]'
    printf 'ok: probe_http_code keeps --disable first and is bounded\n'
  )
}

# --- H2 -------------------------------------------------------------------
probe_signer_key_path() { # case 4: signer env binds the ABSOLUTE KEY_SIGNER
  (
    set -uo pipefail
    local d=$1
    local sandbox="$d/sandbox-h2" body="$d/signer-env.body"
    mkdir -p "$sandbox/scripts/portfolio-demo"
    cp "$DEMO_DIR/run_demo.sh" "$DEMO_DIR/lib.sh" "$sandbox/scripts/portfolio-demo/"
    cmp -s "$DEMO_DIR/run_demo.sh" "$sandbox/scripts/portfolio-demo/run_demo.sh" \
      || { printf 'CHECK-FAIL: sandbox run_demo.sh copy differs from the source\n'; exit 1; }
    cmp -s "$DEMO_DIR/lib.sh" "$sandbox/scripts/portfolio-demo/lib.sh" \
      || { printf 'CHECK-FAIL: sandbox lib.sh copy differs from the source\n'; exit 1; }
    # extract the signer-env heredoc BODY exactly as run_demo.sh writes it
    sed -n '/^TXHARBOR_SIGNER_MODE=development$/,/^TXHARBOR_SIGNER_HTTP_ADDR=/p' \
      "$DEMO_DIR/run_demo.sh" >"$body"
    printf 'extracted %s signer-env heredoc lines into %s\n' "$(wc -l <"$body")" "$body"
    sed -n '1,3p;$p' "$body"
    [[ "$(wc -l <"$body")" -ge 10 ]] || { printf 'CHECK-FAIL: signer-env extraction too short\n'; exit 1; }
    [[ "$(head -n 1 "$body")" == 'TXHARBOR_SIGNER_MODE=development' ]] \
      || { printf 'CHECK-FAIL: extraction does not start at the signer mode line\n'; exit 1; }
    grep -qE '^TXHARBOR_SIGNER_HTTP_ADDR=' "$body" || { printf 'CHECK-FAIL: extraction missing the addr line\n'; exit 1; }
    # the fix: the key path is the absolute KEY_SIGNER itself (the old code
    # wrote "$REPO_ROOT/$KEY_SIGNER", doubling an already-absolute path)
    grep -qF "TXHARBOR_SIGNER_KEY_FILE='\$KEY_SIGNER'" "$body" \
      || { printf 'CHECK-FAIL: signer env does not reference the absolute KEY_SIGNER\n'; exit 1; }
    printf 'ok: heredoc body references the absolute KEY_SIGNER\n'
    # (1) sandbox prepare_evidence_root: KEY_SIGNER absolute, key 0600
    (
      set -uo pipefail
      REPO_ROOT="$sandbox"
      RUN_ID=h2check
      EVIDENCE_BASE="$sandbox/.evidence/portfolio-demo"
      EVIDENCE_ROOT="$sandbox/.evidence/portfolio-demo/h2check"
      export REPO_ROOT RUN_ID EVIDENCE_BASE EVIDENCE_ROOT
      # shellcheck source=/dev/null
      . "$sandbox/scripts/portfolio-demo/lib.sh"
      prepare_evidence_root
      case "$KEY_SIGNER" in
        /*) ;;
        *) printf 'CHECK-FAIL: KEY_SIGNER is not absolute: %s\n' "$KEY_SIGNER"; exit 1 ;;
      esac
      [[ "$(stat -c %a "$(dirname "$KEY_SIGNER")")" == 700 ]] \
        || { printf 'CHECK-FAIL: private/keys is not 0700\n'; exit 1; }
      # run_demo.sh writes the key in setup (umask 077 + chmod 600); mirror it
      ( umask 077; printf '%s\n' "$DUMMY_SIGNER_KEY" >"$KEY_SIGNER" )
      chmod 600 "$KEY_SIGNER"
      [[ -f "$KEY_SIGNER" ]] || { printf 'CHECK-FAIL: KEY_SIGNER does not exist\n'; exit 1; }
      [[ "$(stat -c %a "$KEY_SIGNER")" == 600 ]] \
        || { printf 'CHECK-FAIL: KEY_SIGNER mode %s != 600\n' "$(stat -c %a "$KEY_SIGNER")"; exit 1; }
      printf 'KEY_SIGNER=%s\nENV_SIGNER=%s\n' "$KEY_SIGNER" "$ENV_SIGNER" >"$sandbox/paths.env"
      printf 'ok: KEY_SIGNER absolute (%s), exists, 0600; private/keys 0700\n' "$KEY_SIGNER"
    ) || exit 1
    # (2) emulate run_demo's unquoted heredoc: eval the extracted body so
    #     $KEY_SIGNER & friends expand exactly as run_demo writes the file
    #     (the single quotes in the body stay literal in the generated file)
    # shellcheck source=/dev/null
    . "$sandbox/paths.env"
    local gen="$sandbox/gen-signer-env.sh"
    {
      printf 'cat > %q <<%s\n' "$ENV_SIGNER" 'TXH_SIGNER_ENV_EOF'
      cat "$body"
      printf '%s\n' 'TXH_SIGNER_ENV_EOF'
    } >"$gen"
    ck_lack 'no heredoc delimiter collision in the body' "$body" '^TXH_SIGNER_ENV_EOF$'
    # dummy signer-policy values for the sandbox (no secret material here)
    SIGNER_ADDR="$SENDER_FIXTURE"
    TOKEN="$TOKEN_FIXTURE"
    RECIPIENT="$RECIPIENT_FIXTURE"
    MAX_AMOUNT=1000000000000000000000
    MAX_GAS_LIMIT=200000
    MAX_FEE_PER_GAS=10000000000
    MAX_PRIORITY_FEE_PER_GAS=1000000000
    MAX_GAS_PRICE=10000000000
    SIGNER_PORT=18750
    eval "$(cat "$gen")" || { printf 'CHECK-FAIL: generating the signer env failed\n'; exit 1; }
    chmod 600 "$ENV_SIGNER"
    printf 'generated %s (%s lines)\n' "$ENV_SIGNER" "$(wc -l <"$ENV_SIGNER")"
    local key_from_env
    key_from_env=$(sed -n 's/^TXHARBOR_SIGNER_KEY_FILE=//p' "$ENV_SIGNER" | tr -d "'")
    # only the PATH is echoed below; the key file content is never printed
    printf 'parsed TXHARBOR_SIGNER_KEY_FILE=%s\n' "$key_from_env"
    ck_eq 'env key path == prepare_evidence_root KEY_SIGNER' "$KEY_SIGNER" "$key_from_env"
    ck_eq 'key file exists at the parsed env path' 'yes' \
      "$([[ -f "$key_from_env" ]] && printf yes || printf no)"
    ck_eq 'key file mode at the parsed env path' '600' \
      "$(stat -c %a "$key_from_env" 2>/dev/null || printf missing)"
    ck_hasf 'env line quotes the absolute key path' "$ENV_SIGNER" "TXHARBOR_SIGNER_KEY_FILE='$KEY_SIGNER'"
    ck_hasf 'senders policy expanded from the sandbox values' "$ENV_SIGNER" "TXHARBOR_SIGNER_SENDERS='$SENDER_FIXTURE'"
    ck_hasf 'http addr expanded to the loopback endpoint' "$ENV_SIGNER" "TXHARBOR_SIGNER_HTTP_ADDR='127.0.0.1:18750'"
    ck_lack 'no doubled /$REPO_ROOT/ path in the generated env' "$ENV_SIGNER" "$sandbox/$sandbox"
    printf 'ok: generated signer env binds the absolute key path (0600)\n'
  )
}

# --- H3 -------------------------------------------------------------------
probe_tools_dir_not_approved() { # case 5: only the approved roots are usable
  (
    set -uo pipefail
    local dir=/tmp/opencode/not-approved rc=0 out="" before=no after=no
    [[ -e "$dir" ]] && before=yes
    out=$(TXHARBOR_DEMO_TOOLS_DIR="$dir" bash "$DEMO_DIR/compile_token.sh" --verify 2>&1) || rc=$?
    printf '%s\n' "$out"
    printf 'rc=%s dir=%s existed_before=%s\n' "$rc" "$dir" "$before"
    [[ -e "$dir" ]] && after=yes
    ck_ne0 'unapproved tools dir is refused' "$rc"
    ck_eq 'refusal message' 'yes' "$(printf '%s\n' "$out" | grep -qF 'refusing unapproved tools dir' && printf yes || printf no)"
    ck_eq 'unapproved dir still absent' "$before" "$after"
    printf 'ok: unapproved tools dir refused without side effects\n'
  )
}

probe_tools_dir_root_symlink() { # case 6: an approved root as a symlink is refused
  (
    set -uo pipefail
    local d=$1
    local sandbox="$d/sandbox-h3a" target="$d/link-target" rc=0 out=""
    mkdir -p "$sandbox/scripts/portfolio-demo" "$target"
    cp "$DEMO_DIR/compile_token.sh" "$sandbox/scripts/portfolio-demo/compile_token.sh"
    mkdir -p "$sandbox/.evidence"
    local root="$sandbox/.evidence/portfolio-demo-tools"
    ln -s "$target" "$root"
    out=$(TXHARBOR_DEMO_TOOLS_DIR="$root" bash "$sandbox/scripts/portfolio-demo/compile_token.sh" --verify 2>&1) || rc=$?
    printf '%s\n' "$out"
    printf 'rc=%s root=%s -> %s\n' "$rc" "$root" "$(readlink "$root")"
    ck_ne0 'symlinked approved root is refused' "$rc"
    ck_eq 'refusal message' 'yes' \
      "$(printf '%s\n' "$out" | grep -qF 'refusing symlinked tools-dir component' && printf yes || printf no)"
    ck_eq 'symlink left in place' 'yes' "$([[ -L "$root" ]] && printf yes || printf no)"
    ck_eq 'symlink target untouched' '' "$(ls -A "$target")"
    printf 'ok: symlinked tools root refused, symlink kept, target untouched\n'
  )
}

probe_tools_dir_build_symlink_sentinel() { # case 7: symlinked build/ refused, nothing written
  (
    set -uo pipefail
    local d=$1
    local sandbox="$d/sandbox-h3b" sentinel="$d/sentinel-dir" rc=0 out=""
    mkdir -p "$sandbox/scripts/portfolio-demo" "$sentinel"
    cp "$DEMO_DIR/compile_token.sh" "$sandbox/scripts/portfolio-demo/compile_token.sh"
    local root="$sandbox/.evidence/portfolio-demo-tools"
    mkdir -p "$root"
    printf 'finalfix-build-sentinel-unchanged\n' >"$sentinel/sentinel.txt"
    ln -s "$sentinel" "$root/build"
    out=$(TXHARBOR_DEMO_TOOLS_DIR="$root" bash "$sandbox/scripts/portfolio-demo/compile_token.sh" --verify 2>&1) || rc=$?
    printf '%s\n' "$out"
    printf 'rc=%s root=%s build->%s\n' "$rc" "$root" "$(readlink "$root/build")"
    ck_ne0 'symlinked build dir is refused' "$rc"
    ck_eq 'refusal message' 'yes' \
      "$(printf '%s\n' "$out" | grep -qF 'refusing symlinked build dir' && printf yes || printf no)"
    ck_eq 'build symlink left in place' 'yes' "$([[ -L "$root/build" ]] && printf yes || printf no)"
    ck_eq 'sentinel content unchanged' 'finalfix-build-sentinel-unchanged' \
      "$(cat "$sentinel/sentinel.txt")"
    ck_eq 'sentinel dir has exactly the sentinel file' '1' "$(ls -A "$sentinel" | wc -l)"
    printf 'ok: symlinked build refused; sentinel untouched\n'
  )
}

probe_verify_run_scoped() { # case 8: one runs/<run-id> child under an approved root
  (
    set -uo pipefail
    local root=/tmp/opencode/txharbor-demo-tools
    local run_id="fixchk-$RID"
    local dir="$root/runs/$run_id"
    local solc="$root/solc-0.8.30" sum_before="" sum_after=""
    [[ -f "$solc" ]] && sum_before="$(sha256sum "$solc")"
    local rc=0 out=""
    out=$(TXHARBOR_DEMO_TOOLS_DIR="$dir" bash "$DEMO_DIR/compile_token.sh" --verify 2>&1) || rc=$?
    printf '%s\n' "$out"
    printf 'rc=%s tools_dir=%s\n' "$rc" "$dir"
    ck_eq 'run-scoped --verify rc' '0' "$rc"
    ck_eq 'success marker' 'yes' \
      "$(printf '%s\n' "$out" | grep -qF 'artifact verification ok' && printf yes || printf no)"
    ck_eq 'run-scoped dir is under runs/' 'yes' \
      "$([[ -d "$dir" && "$dir" == *"/runs/$run_id" ]] && printf yes || printf no)"
    local built=0 f
    for f in "$dir"/build/helper.*/democtl; do
      [[ -x "$f" ]] && built=1
    done
    ck_eq 'helper democtl built under the run-scoped dir' '1' "$built"
    if [[ -n "$sum_before" ]]; then
      sum_after="$(sha256sum "$solc")"
      ck_eq 'pinned solc untouched (sha256)' "$sum_before" "$sum_after"
      printf 'note: pinned solc %s stays in the approved root; --verify used the runs/ child\n' "${sum_after%% *}"
    else
      printf 'note: no pinned solc at %s to compare\n' "$solc"
    fi
    printf 'ok: run-scoped --verify passed, helper built, pinned root untouched\n'
  )
}

# --- H4 -------------------------------------------------------------------
extract_stop_owned() { # extract_stop_owned <dir> -- extract + guard the helper
  local d=$1
  local fn="$d/stop_owned.fn"
  sed -n '/^stop_owned() {/,/^}/p' "$DEMO_DIR/run_demo.sh" >"$fn"
  printf 'extracted %s lines of stop_owned from run_demo.sh into %s\n' "$(wc -l <"$fn")" "$fn"
  ck_has 'extraction starts at the stop_owned definition' "$fn" '^stop_owned\(\) \{'
  ck_eq 'extraction ends at the closing brace' '}' "$(tail -n 1 "$fn")"
  bash -n "$fn" || { printf 'CHECK-FAIL: extracted stop_owned does not parse\n'; exit 1; }
  # the fixed body: one bounded graceful_stop per PID plus retained reporting
  ck_hasf 'stop_owned uses the bounded graceful_stop' "$fn" 'graceful_stop "$pid"'
  ck_hasf 'stop_owned reports retained PIDs' "$fn" 'retained'
  printf 'ok: extracted the fixed stop_owned (bounded per-PID stop, retained)\n'
}

probe_stop_owned_retain() { # case 9: a TERM-ignoring child is retained, no hang
  (
    set -uo pipefail
    local d=$1
    extract_stop_owned "$d"
    cat >"$d/stop-owned-retain.sh" <<'INNER'
#!/usr/bin/env bash
# case 9 inner: a child that IGNORES TERM and exits on its own after ~3s.
# stop_owned must give up after the 1s grace (rc != 0), leave the child
# running (retained, never KILLed) and must not hang.
set -uo pipefail
fn=$1 lib=$2
# shellcheck source=/dev/null
. "$lib"
# shellcheck source=/dev/null
. "$fn"
bad=0
bash -c 'trap "" TERM; sleep 3' &
child=$!
OWNED_PIDS=("$child")
STOP_OWNED_GRACE_SECONDS=1
rc=0
start=$SECONDS
stop_owned || rc=$?
elapsed=$((SECONDS - start))
alive=no
if pid_alive "$child"; then alive=yes; fi
printf 'stop_owned rc=%s elapsed=%ss child_alive_after_stop=%s pid=%s\n' \
  "$rc" "$elapsed" "$alive" "$child"
[[ "$rc" -ne 0 ]] || { printf 'CHECK-FAIL: rc=0 for a TERM-ignoring child\n'; bad=1; }
[[ "$elapsed" -lt 5 ]] || { printf 'CHECK-FAIL: took %ss (grace 1s, outer bound 15s)\n' "$elapsed"; bad=1; }
[[ "$alive" == yes ]] || { printf 'CHECK-FAIL: child was not retained\n'; bad=1; }
# the retained child is allowed to finish on its own; reap it (never KILL it)
if [[ "$alive" == yes ]]; then
  st=0
  wait "$child" || st=$?
  printf 'reaped pid %s after its natural exit (status %s)\n' "$child" "$st"
fi
exit "$bad"
INNER
    printf 'run: timeout 15 bash %q %q %q\n' \
      "$d/stop-owned-retain.sh" "$d/stop_owned.fn" "$DEMO_DIR/lib.sh"
    local rc=0
    timeout 15 bash "$d/stop-owned-retain.sh" "$d/stop_owned.fn" "$DEMO_DIR/lib.sh" || rc=$?
    ck_eq 'outer timeout 15 wrapper rc (124 would mean stop_owned hung)' '0' "$rc"
    printf 'ok: stop_owned gave up at the grace bound and retained the child\n'
  )
}

probe_stop_owned_normal() { # case 10: normal children -> rc 0, no stall
  (
    set -uo pipefail
    local d=$1
    extract_stop_owned "$d"
    # Two normal shapes: (a) the ticket's `sleep 0.3` child, which exits on its
    # own (rc 0, reaped promptly -- the old body stalled the whole 10s grace on
    # a zombie); (b) a child that honors TERM and exits 0, exactly like the
    # demo services (run_demo.sh asserts their "graceful exit status" == 0).
    # A plain `sleep` that is TERMed dies with status 143, which stop_owned
    # reports as a failure by design (see lib.sh graceful_stop's contract), so
    # it is deliberately not used as the "normal" case.
    cat >"$d/stop-owned-normal.sh" <<'INNER'
#!/usr/bin/env bash
# case 10 inner: normal shutdowns must end with stop_owned rc 0.
set -uo pipefail
fn=$1 lib=$2
# shellcheck source=/dev/null
. "$lib"
# shellcheck source=/dev/null
. "$fn"
bad=0
sleep 0.3 &
child=$!
sleep 0.5
OWNED_PIDS=("$child")
rc=0
start=$SECONDS
stop_owned || rc=$?
elapsed=$((SECONDS - start))
printf 'natural-exit child (sleep 0.3): stop_owned rc=%s elapsed=%ss pid=%s\n' \
  "$rc" "$elapsed" "$child"
[[ "$rc" -eq 0 ]] || { printf 'CHECK-FAIL: natural-exit child rc=%s != 0\n' "$rc"; bad=1; }
[[ "$elapsed" -lt 5 ]] || { printf 'CHECK-FAIL: natural-exit reap took %ss\n' "$elapsed"; bad=1; }
bash -c 'trap "exit 0" TERM; sleep 0.3' &
child2=$!
OWNED_PIDS=("$child2")
rc2=0
start2=$SECONDS
stop_owned || rc2=$?
elapsed2=$((SECONDS - start2))
alive2=no
if pid_alive "$child2"; then alive2=yes; fi
printf 'TERM-honoring child: stop_owned rc=%s elapsed=%ss alive_after=%s pid=%s\n' \
  "$rc2" "$elapsed2" "$alive2" "$child2"
[[ "$rc2" -eq 0 ]] || { printf 'CHECK-FAIL: TERM-honoring child rc=%s != 0\n' "$rc2"; bad=1; }
[[ "$alive2" == no ]] || { printf 'CHECK-FAIL: TERM-honoring child still alive\n'; bad=1; }
[[ "$elapsed2" -lt 5 ]] || { printf 'CHECK-FAIL: TERM-honoring child took %ss\n' "$elapsed2"; bad=1; }
exit "$bad"
INNER
    printf 'run: timeout 15 bash %q %q %q\n' \
      "$d/stop-owned-normal.sh" "$d/stop_owned.fn" "$DEMO_DIR/lib.sh"
    local rc=0
    timeout 15 bash "$d/stop-owned-normal.sh" "$d/stop_owned.fn" "$DEMO_DIR/lib.sh" || rc=$?
    ck_eq 'normal children -> stop_owned rc 0' '0' "$rc"
    printf 'ok: normal children report rc 0 without stalling\n'
  )
}

probe_stop_owned_failed_child() { # case 11: a child that exited 7 is reported
  (
    set -uo pipefail
    local d=$1
    extract_stop_owned "$d"
    cat >"$d/stop-owned-failed-child.sh" <<'INNER'
#!/usr/bin/env bash
# case 11 inner: a child that already exited 7 must be reported (rc != 0)
# without any hang or stall (a zombie is reaped, not waited on blindly).
set -uo pipefail
fn=$1 lib=$2
# shellcheck source=/dev/null
. "$lib"
# shellcheck source=/dev/null
. "$fn"
bad=0
bash -c 'exit 7' &
child=$!
sleep 0.2
OWNED_PIDS=("$child")
rc=0
start=$SECONDS
stop_owned || rc=$?
elapsed=$((SECONDS - start))
printf 'failed child (exit 7): stop_owned rc=%s elapsed=%ss pid=%s\n' \
  "$rc" "$elapsed" "$child"
[[ "$rc" -ne 0 ]] || { printf 'CHECK-FAIL: rc=0 although the child exited 7\n'; bad=1; }
[[ "$elapsed" -lt 5 ]] || { printf 'CHECK-FAIL: took %ss (hang?)\n' "$elapsed"; bad=1; }
exit "$bad"
INNER
    printf 'run: timeout 15 bash %q %q %q\n' \
      "$d/stop-owned-failed-child.sh" "$d/stop_owned.fn" "$DEMO_DIR/lib.sh"
    local rc=0
    timeout 15 bash "$d/stop-owned-failed-child.sh" "$d/stop_owned.fn" "$DEMO_DIR/lib.sh" || rc=$?
    ck_eq 'failed child -> stop_owned rc != 0 without hanging' '0' "$rc"
    printf 'ok: the failed child status is reported, no hang\n'
  )
}

# --- H5 -------------------------------------------------------------------
extract_check_transfer_receipt() { # extract + guard the receipt helper
  local d=$1
  local fn="$d/check_transfer_receipt.fn"
  sed -n '/^check_transfer_receipt() {/,/^}/p' "$DEMO_DIR/run_demo.sh" >"$fn"
  printf 'extracted %s lines of check_transfer_receipt from run_demo.sh into %s\n' \
    "$(wc -l <"$fn")" "$fn"
  ck_has 'extraction starts at the check_transfer_receipt definition' "$fn" '^check_transfer_receipt\(\) \{'
  ck_eq 'extraction ends at the closing brace' '}' "$(tail -n 1 "$fn")"
  bash -n "$fn" || { printf 'CHECK-FAIL: extracted check_transfer_receipt does not parse\n'; exit 1; }
  ck_hasf 'receipt binding checks the expected tx hash' "$fn" 'receipt transactionHash == expected'
  printf 'ok: extracted the fixed check_transfer_receipt\n'
}

probe_receipt_binding() { # cases 12/13: <dir> <expected-tx-hash> [block-hash] [block-number]
  (
    set -euo pipefail
    # shellcheck source=../../lib.sh
    . "$DEMO_DIR/lib.sh"
    local d=$1 want_tx=$2 want_block_hash="${3:-}" want_block_number="${4:-}"
    [[ -x "$DEMOCTL" ]] || { printf 'CHECK-FAIL: democtl was not built (%s)\n' "$DEMOCTL"; exit 1; }
    extract_check_transfer_receipt "$d"
    TOKEN="$TOKEN_FIXTURE"
    TRANSFER_SIG="$TRANSFER_SIG_FIXTURE"
    # fixture self-check: the log data must be democtl's uint256 hex of AMOUNT
    ck_eq 'fixture data == democtl uint256 hex(amount)' \
      "$(sed -n 's/.*"data": "\(0x[0-9a-f]*\)".*/\1/p' "$FIXTURES/receipt-good.json")" \
      "$("$DEMOCTL" uint256 hex "$AMOUNT_FIXTURE")"
    # shellcheck source=/dev/null
    . "$d/check_transfer_receipt.fn"
    local rcpt
    rcpt="$(cat "$FIXTURES/receipt-good.json")"
    printf 'fixture: token=%s sender=%s recipient=%s amount=%s want_tx=%s want_block_hash=%s want_block_number=%s\n' \
      "$TOKEN_FIXTURE" "$SENDER_FIXTURE" "$RECIPIENT_FIXTURE" "$AMOUNT_FIXTURE" \
      "$want_tx" "${want_block_hash:--}" "${want_block_number:--}"
    check_transfer_receipt "finalfix" "$rcpt" "$SENDER_FIXTURE" "$RECIPIENT_FIXTURE" \
      "$AMOUNT_FIXTURE" "$want_tx" "$want_block_hash" "$want_block_number"
    printf 'ok: check_transfer_receipt accepted the fixture (tx %s)\n' "$want_tx"
  )
}

# --- main -------------------------------------------------------------------
log "== RESUME-03-FINALFIX rid=$RID"
log "== checks root $CHECKS_ROOT (scratch $RUNS)"
log "== strict mock curl: $CHECK_BIN/curl (--disable must be the first argument)"

# the only build this suite performs; its log is a case log like any other
d="$(case_dir build-democtl)"
run_case build-democtl 0 go build -o "$DEMOCTL" ./scripts/portfolio-demo/democtl

# H1: hardened curl call sites (strict mock)
d="$(case_dir http-post-first-arg)"
run_case http-post-first-arg 0 probe_http_post_first_arg "$d"
d="$(case_dir http-post-old-order-control)"
run_case http-post-old-order-control 0 probe_http_post_old_order_control "$d"
d="$(case_dir probe-http-code)"
run_case probe-http-code 0 probe_probe_http_code "$d"

# H2: the signer env must bind the absolute key path
d="$(case_dir signer-key-path)"
run_case signer-key-path 0 probe_signer_key_path "$d"
case_check 'signer env binds the absolute key path' 'binds the absolute key path'

# H3: approved tools-dir roots, symlink refusals, run-scoped verify
d="$(case_dir tools-dir-not-approved)"
run_case tools-dir-not-approved 0 probe_tools_dir_not_approved "$d"
d="$(case_dir tools-dir-root-symlink)"
run_case tools-dir-root-symlink 0 probe_tools_dir_root_symlink "$d"
d="$(case_dir tools-dir-build-symlink-sentinel)"
run_case tools-dir-build-symlink-sentinel 0 probe_tools_dir_build_symlink_sentinel "$d"
d="$(case_dir verify-run-scoped)"
run_case verify-run-scoped 0 probe_verify_run_scoped "$d"

# H4: cleanup of owned PIDs (retain, normal, failed child)
d="$(case_dir stop-owned-retain)"
run_case stop-owned-retain 0 probe_stop_owned_retain "$d"
case_check 'the retained child is reported as retained' '^WARN: .*retained'
d="$(case_dir stop-owned-normal)"
run_case stop-owned-normal 0 probe_stop_owned_normal "$d"
d="$(case_dir stop-owned-failed-child)"
run_case stop-owned-failed-child 0 probe_stop_owned_failed_child "$d"
case_check 'the non-zero child status is recorded' 'exited with status 7'

# H5: receipt binding against the fixed fixture
d="$(case_dir receipt-binding-good)"
run_case receipt-binding-good 0 probe_receipt_binding "$d" "$TXHASH_FIXTURE" "$BLOCKHASH_FIXTURE" "0x10"
case_check 'the good receipt passes the status binding' 'receipt status == 0x1'
case_check 'the good receipt binds the DB block hash' 'receipt blockHash == DB block_hash'
case_check 'the good receipt binds the DB block number' 'receipt blockNumber == DB block_number'
d="$(case_dir receipt-binding-wrong-hash)"
run_case receipt-binding-wrong-hash nonzero probe_receipt_binding "$d" "$TXHASH_MISMATCH_FIXTURE"
case_check 'the wrong tx hash fails on transactionHash' 'receipt transactionHash == expected'

log "== summary: cases=$TOTAL failed=$FAILED rid=$RID"
log "== case logs: $CHECKS_ROOT/03-finalfix-*.log (scratch kept under $RUNS)"
if (( FAILED > 0 )); then
  log "RESULT: FAIL ($FAILED of $TOTAL cases failed)"
  exit 1
fi
log "RESULT: PASS ($TOTAL cases)"
exit 0
