#!/usr/bin/env bash
# raw_preservation_test.sh is the isolated regression fixture for the raw-evidence
# preservation audit (docs/evidence/013-supplement/consumer-seg/checks/
# raw_preservation.py, 013 review closure P2): the audit must fail (exit 1) on
# ANY addition, removal or modification inside the protected raw set (runs/,
# logs/, environment.txt, source_fingerprint.txt, smoke/; a rename counts as a
# removal plus an addition), and must still succeed when only non-raw files
# (corrections, checks/, manifests) are added.
#
# Usage: raw_preservation_test.sh [log-dir]
#
#   [log-dir]  directory for the fixture log (default: a fresh mktemp dir). The
#              fixture writes <log-dir>/raw_preservation_regression.log and
#              prints the log path plus its own exit code on the caller's
#              stderr; the caller records that exit status (0 = every case
#              matched its expected exit code, 1 = at least one mismatch).
#
# Isolation: everything runs in a mktemp sandbox holding a temporary git
# repository; the real evidence tree is never touched. The pre-fix script is
# extracted read-only from the parent commit with `git show` to demonstrate the
# missed cases (marked "pre-fix" in the log); the post-fix phase calls the real
# script in the work tree.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd -P)"
SCRIPT_REL="docs/evidence/013-supplement/consumer-seg/checks/raw_preservation.py"
SCRIPT="$REPO_ROOT/$SCRIPT_REL"
PRE_FIX_REV="c4bee0c445ef191a4b66901c101443856a6a5a7a"

if [[ $# -gt 1 ]]; then
	echo "usage: $0 [log-dir]" >&2
	exit 2
fi
LOG_DIR="${1:-$(mktemp -d "${TMPDIR:-/tmp}/raw-preservation-log.XXXXXX")}"
mkdir -p "$LOG_DIR" || exit 1
LOG_FILE="$LOG_DIR/raw_preservation_regression.log"

exec 3>&2 # the caller's stderr survives the redirect below
exec >"$LOG_FILE" 2>&1

printf 'raw-preservation audit regression fixture (013 review closure P2)\n'
printf 'repo: %s\n' "$REPO_ROOT"
if [[ ! -f "$SCRIPT" ]]; then
	printf 'fixture: script under test not found: %s\n' "$SCRIPT" >&2
	printf 'fixture: log=%s rc=2\n' "$LOG_FILE" >&3
	exit 2
fi
printf 'script under test: %s\n  sha256: %s\n' "$SCRIPT" "$(sha256sum "$SCRIPT" | cut -d' ' -f1)"

SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/raw-preservation-sandbox.XXXXXX")"
cleanup() {
	local rc=$?
	if [[ -n "${SANDBOX:-}" && -d "$SANDBOX" ]]; then
		rm -rf -- "$SANDBOX"
	fi
	return "$rc"
}
trap cleanup EXIT

# --- fixture repository: a minimal raw set plus one non-raw file ------------
FIX="$SANDBOX/fixture"
mkdir -p "$FIX/runs/r01_x" "$FIX/logs" "$FIX/smoke/runs/r01"
printf 'meta\n' >"$FIX/runs/r01_x/meta.json"
printf 'anchors\n' >"$FIX/runs/r01_x/anchors.json"
printf '0\n' >"$FIX/logs/r01_x.rc"
printf 'log-bytes\n' >"$FIX/logs/r01_x.log.gz"
printf 'env\n' >"$FIX/environment.txt"
printf 'fp\n' >"$FIX/source_fingerprint.txt"
printf 'smoke\n' >"$FIX/smoke/runs/r01/meta.json"
printf 'corrections\n' >"$FIX/CORRECTIONS.md"
git -C "$FIX" init -q
git -C "$FIX" add -A
git -C "$FIX" -c user.email=fixture@example.invalid -c user.name=fixture commit -qm baseline
REV="$(git -C "$FIX" rev-parse HEAD)"
printf 'fixture repo: %s at %s\n' "$FIX" "$REV"

reset_tree() { git -C "$FIX" checkout -q -- . && git -C "$FIX" clean -qfd; }

pass=0
fail=0
skip=0
say() { printf '%s\n' "$*"; }
case_result() { # case_result <name> <expected-rc> <actual-rc>
	if [[ "$2" == "$3" ]]; then
		say "PASS: $1 (rc=$3)"
		pass=$((pass + 1))
	else
		say "FAIL: $1 (rc=$3, want $2)"
		fail=$((fail + 1))
	fi
}
verdict_of() { grep -o 'VERDICT: .*' "$1" 2>/dev/null || printf 'VERDICT: <none>\n'; }

run_script() { # run_script <script> <outfile>
	RAW_PRESERVATION_REV="$REV" python3 "$1" "$FIX" >"$2" 2>&1
	RC=$?
}

# --- phase 1: the pre-fix script misses raw additions/removals --------------
PRE="$SANDBOX/raw_preservation_prefix.py"
if git -C "$REPO_ROOT" show "$PRE_FIX_REV:$SCRIPT_REL" >"$SANDBOX/prefix_original.py" 2>"$SANDBOX/git-show.err" &&
	[[ -s "$SANDBOX/prefix_original.py" ]]; then
	# The pre-fix script has no baseline-revision knob, so the sandbox copy is
	# pointed at the fixture baseline by rewriting exactly that one constant.
	# The defect under demonstration (the failure condition) is untouched: the
	# adaptation diff is logged and asserted to be one changed line only.
	sed -E "s|^BASELINE_REV = .*|BASELINE_REV = \"$REV\"|" \
		"$SANDBOX/prefix_original.py" >"$PRE"
	say ''
	say 'pre-fix copy adaptation (only the baseline-revision constant is rewritten):'
	diff -u "$SANDBOX/prefix_original.py" "$PRE" | sed 's/^/  /'
	if [[ "$(diff "$SANDBOX/prefix_original.py" "$PRE" | grep -c '^[<>]')" == 2 ]]; then
		say 'PASS: pre-fix copy differs from the extracted original in exactly one line'
		pass=$((pass + 1))
	else
		say 'FAIL: pre-fix copy adaptation touched more than one line'
		fail=$((fail + 1))
	fi
	say ''
	say '=== phase 1: pre-fix script (defect reproduction; rc=0 expected on raw add/remove/rename) ==='
	for spec in delete add rename; do
		reset_tree
		case "$spec" in
		delete) rm "$FIX/logs/r01_x.rc" ;;
		add) printf 'extra\n' >"$FIX/runs/r02_y.json" ;;
		rename) mv "$FIX/runs/r01_x/meta.json" "$FIX/runs/r01_x/meta2.json" ;;
		esac
		run_script "$PRE" "$SANDBOX/pre_$spec.out"
		say "--- pre-fix $spec: rc=$RC; $(verdict_of "$SANDBOX/pre_$spec.out")"
		case_result "pre-fix $spec is missed" 0 "$RC"
	done
	reset_tree
	printf 'meta2\n' >"$FIX/runs/r01_x/meta.json"
	run_script "$PRE" "$SANDBOX/pre_modify.out"
	say "--- pre-fix modify: rc=$RC; $(verdict_of "$SANDBOX/pre_modify.out")"
	case_result "pre-fix modification is still detected" 1 "$RC"
else
	say ''
	say "SKIP: pre-fix script unavailable at $PRE_FIX_REV: $(cat "$SANDBOX/git-show.err" 2>/dev/null)"
	skip=$((skip + 1))
fi

# --- phase 2: the fixed script (real work-tree script) ----------------------
say ''
say '=== phase 2: fixed script (rc expectations) ==='

reset_tree
run_script "$SCRIPT" "$SANDBOX/fixed_ok.out"
say "--- unchanged: rc=$RC; $(verdict_of "$SANDBOX/fixed_ok.out")"
case_result 'unchanged raw set succeeds' 0 "$RC"
if grep -q 'raw evidence byte-identical' "$SANDBOX/fixed_ok.out"; then
	say 'PASS: unchanged output says byte-identical'
	pass=$((pass + 1))
else
	say 'FAIL: unchanged output lacks byte-identical'
	fail=$((fail + 1))
fi

reset_tree
printf 'meta2\n' >"$FIX/runs/r01_x/meta.json"
run_script "$SCRIPT" "$SANDBOX/fixed_modify.out"
say "--- modify runs/r01_x/meta.json: rc=$RC; $(verdict_of "$SANDBOX/fixed_modify.out")"
case_result 'modification fails' 1 "$RC"
grep -q 'changed: ./runs/r01_x/meta.json' "$SANDBOX/fixed_modify.out" &&
	{ say 'PASS: modification listed under raw changed'; pass=$((pass + 1)); } ||
	{ say 'FAIL: modification not listed under raw changed'; fail=$((fail + 1)); }

reset_tree
rm "$FIX/logs/r01_x.rc"
run_script "$SCRIPT" "$SANDBOX/fixed_delete.out"
say "--- delete logs/r01_x.rc: rc=$RC; $(verdict_of "$SANDBOX/fixed_delete.out")"
case_result 'deletion fails' 1 "$RC"
grep -q 'removed: ./logs/r01_x.rc' "$SANDBOX/fixed_delete.out" &&
	{ say 'PASS: deletion listed under raw removed'; pass=$((pass + 1)); } ||
	{ say 'FAIL: deletion not listed under raw removed'; fail=$((fail + 1)); }

reset_tree
printf 'extra\n' >"$FIX/runs/r02_y.json"
run_script "$SCRIPT" "$SANDBOX/fixed_add.out"
say "--- add runs/r02_y.json: rc=$RC; $(verdict_of "$SANDBOX/fixed_add.out")"
case_result 'raw addition fails' 1 "$RC"
grep -q 'added:   ./runs/r02_y.json' "$SANDBOX/fixed_add.out" &&
	{ say 'PASS: addition listed under raw added'; pass=$((pass + 1)); } ||
	{ say 'FAIL: addition not listed under raw added'; fail=$((fail + 1)); }

reset_tree
mv "$FIX/runs/r01_x/meta.json" "$FIX/runs/r01_x/meta2.json"
run_script "$SCRIPT" "$SANDBOX/fixed_rename.out"
say "--- rename runs/r01_x/meta.json -> meta2.json: rc=$RC; $(verdict_of "$SANDBOX/fixed_rename.out")"
case_result 'rename fails (removal + addition)' 1 "$RC"

reset_tree
printf 'smoke2\n' >"$FIX/smoke/runs/r01/meta.json"
run_script "$SCRIPT" "$SANDBOX/fixed_smoke.out"
say "--- modify smoke/runs/r01/meta.json: rc=$RC; $(verdict_of "$SANDBOX/fixed_smoke.out")"
case_result 'smoke modification fails' 1 "$RC"

reset_tree
mkdir -p "$FIX/checks"
printf 'note\n' >"$FIX/checks/note.log"
run_script "$SCRIPT" "$SANDBOX/fixed_nonraw.out"
say "--- add non-raw checks/note.log: rc=$RC; $(verdict_of "$SANDBOX/fixed_nonraw.out")"
case_result 'non-raw addition succeeds' 0 "$RC"
grep -q 'raw evidence byte-identical' "$SANDBOX/fixed_nonraw.out" &&
	{ say 'PASS: non-raw addition keeps byte-identical verdict'; pass=$((pass + 1)); } ||
	{ say 'FAIL: non-raw addition broke the verdict'; fail=$((fail + 1)); }

# --- result -----------------------------------------------------------------
say ''
printf '===== raw-preservation fixture result: %d passed, %d failed, %d skipped =====\n' "$pass" "$fail" "$skip"
rc=0
if [[ "$fail" -gt 0 ]]; then
	rc=1
fi
printf 'fixture: log=%s rc=%d\n' "$LOG_FILE" "$rc" >&3
exit "$rc"
