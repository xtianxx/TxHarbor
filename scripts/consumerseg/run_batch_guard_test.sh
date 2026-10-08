#!/usr/bin/env bash
# run_batch_guard_test.sh is the repeatable, isolated regression fixture for
# the batch-root guard in run_batch.sh (review item #1 of the 013
# consumer-segmentation measurement): the batch root must be claimed
# atomically before any file is written, an existing path — directory (empty
# or populated), plain file or symlink (dangling included) — must be refused
# with exit 2 and left byte-identical, and concurrent invocations of one fresh
# root must have exactly one winner.
#
# Usage: scripts/consumerseg/run_batch_guard_test.sh [log-dir]
#
#   [log-dir]  directory for the fixture log; default: a fresh mktemp dir. The
#              fixture redirects its own stdout/stderr to
#              <log-dir>/fixture_root_guard.log, and prints the log path plus
#              its own exit code on the caller's stderr when it finishes; the
#              caller records that exit status (0 = every case passed or was
#              explicitly skipped, 1 = at least one FAIL).
#
# Isolation: every case runs inside a throwaway mktemp sandbox that is also
# used as TMPDIR and removed on exit. A controlled `go` stand-in is prepended
# to PATH for the runner-under-test calls only, so no real `go test`, no
# 10k-load drill, no broker and no container is ever started. Nothing outside
# the sandbox and the log directory is written; the pre-fix runner is extracted
# read-only from the baseline revision with `git show` and is never re-created
# in the work tree.
#
# Cases (each prints PASS/FAIL plus the observed exit codes; an explicit SKIP
# does not fail the run):
#   1  the pre-fix runner rewrites environment.txt / source_fingerprint.txt of
#      an existing batch dir before refusing (defect reproduction; SKIP if the
#      baseline revision is unavailable)
#   2  existing empty directory (and existing plain file) refused with rc=2
#   3  existing populated batch directory refused with rc=2, tree byte-identical
#   4  symlink to a directory and dangling symlink refused with rc=2
#   5  existing directory refused with rc=2 even for a different plan
#   6  fresh root runs the short plan to completion, artifacts asserted
#   7  four concurrent invocations of one fresh root: exactly one winner
#   8  extra: an invalid plan leaves no root behind; a missing parent reports
#      "cannot create" instead of "already exists"
set -uo pipefail # deliberately no -e: one failing case must not abort the rest

BASELINE_REV="27cc51d0801862f85c1ae04fd1fc74a4d300e80a"
RUNNER_REL="scripts/consumerseg/run_batch.sh"
PLAN_SHORT="on off"
PLAN_REPRO="pristine on off"

if [[ $# -gt 1 ]]; then
	echo "usage: $0 [log-dir]" >&2
	exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RUNNER="$REPO_ROOT/$RUNNER_REL"

LOG_DIR="${1:-$(mktemp -d "${TMPDIR:-/tmp}/consumerseg-guard-log.XXXXXX")}"
mkdir -p "$LOG_DIR" || exit 1
LOG_FILE="$LOG_DIR/fixture_root_guard.log"

exec 3>&2 # the caller's stderr survives the redirect below
exec >"$LOG_FILE" 2>&1

printf 'consumer seg guard fixture (013 review item 1: batch-root protection)\n'
printf 'repo: %s\n' "$REPO_ROOT"
if [[ ! -f "$RUNNER" ]]; then
	printf 'guard fixture: runner under test not found: %s\n' "$RUNNER" >&2
	printf 'guard fixture: log=%s rc=2\n' "$LOG_FILE" >&3
	exit 2
fi
printf 'runner under test: %s\n  sha256: %s\n' "$RUNNER" "$(sha256sum "$RUNNER" | cut -d' ' -f1)"

SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/consumerseg-guard-sandbox.XXXXXX")"
export TMPDIR="$SANDBOX/tmp"
mkdir -p "$TMPDIR"
printf 'sandbox: %s (also used as TMPDIR; removed on exit)\n' "$SANDBOX"

FAKE_BIN="$SANDBOX/bin"
mkdir -p "$FAKE_BIN"
cat >"$FAKE_BIN/go" <<'GO_STANDIN'
#!/usr/bin/env bash
# Controlled `go` stand-in: it never builds or runs anything, so this fixture
# cannot start the real drill, a broker or a container. `go version` answers
# with a recognisable fake so the runner's environment/fingerprint witnesses
# still record a toolchain line.
if [[ "${1:-}" == "version" ]]; then
	echo "go version go9.9.9 consumerseg-guard-fixture/amd64"
	exit 0
fi
printf 'consumerseg-guard-fixture go stand-in: %s\n' "$*"
exit 0
GO_STANDIN
chmod +x "$FAKE_BIN/go"
printf 'go stand-in: %s\n' "$FAKE_BIN/go"

cleanup() {
	local rc=$?
	if [[ -n "${SANDBOX:-}" && -d "$SANDBOX" ]]; then
		rm -rf -- "$SANDBOX"
	fi
	return "$rc"
}
trap cleanup EXIT

# --- helpers ----------------------------------------------------------------
pass=0
fail=0
skip=0
CASE_FAILURES=0

case_begin() { CASE_FAILURES=0; printf '\n--- case %s\n' "$1"; }
case_end() {
	if [[ "$CASE_FAILURES" -eq 0 ]]; then
		printf 'PASS: %s\n' "$1"
		pass=$((pass + 1))
	else
		printf 'FAIL: %s\n' "$1"
		fail=$((fail + 1))
	fi
}
case_skip() {
	printf '\n--- case %s\nSKIP: %s\n' "$1" "$2"
	skip=$((skip + 1))
}

seg_ok() { printf '    ok: %s\n' "$1"; }
seg_bad() {
	printf '    NOT OK: %s\n' "$1"
	CASE_FAILURES=$((CASE_FAILURES + 1))
}
seg_eq() { # seg_eq <what> <expected> <actual>
	if [[ "$2" == "$3" ]]; then
		seg_ok "$1: $3"
	else
		seg_bad "$1: got '$3', expected '$2'"
	fi
}
seg_file() { # seg_file <what> <path>
	if [[ -f "$2" ]]; then seg_ok "$1: $2"; else seg_bad "$1: $2 missing"; fi
}
seg_absent() { # seg_absent <what> <path>
	if [[ ! -e "$2" && ! -L "$2" ]]; then seg_ok "$1: $2 absent"; else seg_bad "$1: $2 still present"; fi
}
seg_contains() { # seg_contains <what> <file> <needle>
	if grep -qF -- "$3" "$2" 2>/dev/null; then
		seg_ok "$1: $2 contains '$3'"
	else
		seg_bad "$1: $2 lacks '$3'"
	fi
}
seg_tree_same() { # seg_tree_same <what> <dir> <expected-fingerprint>
	local what="$1" dir="$2" expected="$3" actual
	actual="$(tree_fingerprint "$dir")"
	if [[ "$expected" == "$actual" ]]; then
		seg_ok "$what: tree byte-identical"
	else
		seg_bad "$what: tree changed"
		diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") | sed 's/^/        /'
	fi
}

file_lines() { # file_lines <path> -> line count, or "missing"
	if [[ -f "$1" ]]; then wc -l <"$1"; else printf 'missing\n'; fi
}
file_content() { # file_content <path> -> content with newlines stripped, or "missing"
	if [[ -f "$1" ]]; then tr -d '\n' <"$1"; else printf 'missing\n'; fi
}

# Canonical, content-addressed listing of a tree: one line per entry, so an
# equal fingerprint means no added/removed entry and no changed bytes, target
# or link text.
tree_fingerprint() {
	local dir="$1" rel
	(
		cd "$dir" || exit 1
		find . -mindepth 1 | LC_ALL=C sort | while IFS= read -r rel; do
			if [[ -L "$rel" ]]; then
				printf 'L %s -> %s\n' "$rel" "$(readlink "$rel")"
			elif [[ -d "$rel" ]]; then
				printf 'D %s\n' "$rel"
			elif [[ -f "$rel" ]]; then
				printf 'F %s %s\n' "$rel" "$(sha256sum "$rel" | cut -d' ' -f1)"
			else
				printf 'O %s\n' "$rel"
			fi
		done
	)
}

LAST_RC=0
LAST_STDOUT=""
LAST_STDERR=""
invoke() { # invoke <script> <args...>; the stand-in is on PATH for this call only
	local runner="$1"
	shift
	PATH="$FAKE_BIN:$PATH" "$runner" "$@" >"$SANDBOX/last.stdout" 2>"$SANDBOX/last.stderr"
	LAST_RC=$?
	LAST_STDOUT="$(cat "$SANDBOX/last.stdout")"
	LAST_STDERR="$(cat "$SANDBOX/last.stderr")"
}

# --- case 1: the old defect is reproducible ---------------------------------
case_begin "1: pre-fix runner rewrites the witnesses of an existing batch dir (defect reproduction)"
old_repo="$SANDBOX/oldrepo"
old_script="$old_repo/$RUNNER_REL"
mkdir -p "$(dirname "$old_script")"
if git -C "$REPO_ROOT" show "$BASELINE_REV:$RUNNER_REL" >"$old_script" 2>"$SANDBOX/git-show.err" &&
	[[ -s "$old_script" ]]; then
	chmod +x "$old_script"
	# Minimal repo skeleton so the extracted runner's fingerprint step hashes
	# real files instead of erroring on missing ones.
	for rel in go.mod internal/events/consumer.go internal/events/perf_seam_none.go \
		internal/events/perf_seam_perf.go internal/events/backlog_drain_integration_test.go; do
		mkdir -p "$old_repo/$(dirname "$rel")"
		cp "$REPO_ROOT/$rel" "$old_repo/$rel"
	done
	printf '    extracted %s at %s into the sandbox (work tree untouched)\n' "$RUNNER_REL" "$BASELINE_REV"

	sentinel_root="$SANDBOX/oldbatch"
	mkdir -p "$sentinel_root/runs/r01_pristine" "$sentinel_root/logs"
	printf 'SENTINEL environment.txt: must not be rewritten\n' >"$sentinel_root/environment.txt"
	printf 'SENTINEL source_fingerprint.txt: must not be rewritten\n' >"$sentinel_root/source_fingerprint.txt"
	printf 'label\tmode\tn\trc\tstarted\tended\n' >"$sentinel_root/logs/summary.tsv"
	printf '{"sentinel":true}\n' >"$sentinel_root/runs/r01_pristine/report.json"
	env_before="$(sha256sum "$sentinel_root/environment.txt" | cut -d' ' -f1)"
	fp_before="$(sha256sum "$sentinel_root/source_fingerprint.txt" | cut -d' ' -f1)"
	summary_before="$(sha256sum "$sentinel_root/logs/summary.tsv" | cut -d' ' -f1)"

	invoke "$old_script" "$sentinel_root" "$PLAN_REPRO"
	env_after="$(sha256sum "$sentinel_root/environment.txt" | cut -d' ' -f1)"
	fp_after="$(sha256sum "$sentinel_root/source_fingerprint.txt" | cut -d' ' -f1)"
	summary_after="$(sha256sum "$sentinel_root/logs/summary.tsv" | cut -d' ' -f1)"
	printf '    pre-fix rc=%s\n    pre-fix stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
	printf '    environment.txt        %s -> %s\n' "$env_before" "$env_after"
	printf '    source_fingerprint.txt %s -> %s\n' "$fp_before" "$fp_after"

	seg_eq 'pre-fix runner exit code' 2 "$LAST_RC"
	seg_contains 'pre-fix refusal happens only in the per-round loop' "$SANDBOX/last.stderr" 'refusing to overwrite existing'
	seg_eq 'environment.txt rewritten before the refusal (defect)' changed \
		"$([[ "$env_before" != "$env_after" ]] && echo changed || echo unchanged)"
	seg_eq 'source_fingerprint.txt rewritten before the refusal (defect)' changed \
		"$([[ "$fp_before" != "$fp_after" ]] && echo changed || echo unchanged)"
	seg_eq 'summary.tsv untouched by the pre-fix run' unchanged \
		"$([[ "$summary_before" == "$summary_after" ]] && echo unchanged || echo changed)"
	seg_absent 'no round log for the refused round' "$sentinel_root/logs/r01_pristine.log"
	case_end "1: pre-fix runner truncates both witnesses then refuses (rc=2) — defect reproduced"
else
	case_skip "1: pre-fix defect reproduction" \
		"baseline revision $BASELINE_REV or its $RUNNER_REL is unavailable: $(cat "$SANDBOX/git-show.err" 2>/dev/null)"
fi

# --- case 2: existing empty directory ---------------------------------------
case_begin "2: an existing empty directory and an existing plain file are refused"
empty_root="$SANDBOX/work/empty/root"
mkdir -p "$empty_root"
invoke "$RUNNER" "$empty_root" "$PLAN_SHORT"
printf '    empty dir rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'empty dir exit code' 2 "$LAST_RC"
seg_eq 'entries left under the refused dir' 0 "$(find "$empty_root" -mindepth 1 | wc -l)"
seg_contains 'message says the target exists' "$SANDBOX/last.stderr" 'already exists'
seg_contains 'message points at a fresh evidence dir' "$SANDBOX/last.stderr" 'fresh evidence dir'

file_root="$SANDBOX/work/plainfile/root"
mkdir -p "$SANDBOX/work/plainfile"
printf 'SENTINEL plain file: must not be overwritten\n' >"$file_root"
file_before="$(sha256sum "$file_root" | cut -d' ' -f1)"
invoke "$RUNNER" "$file_root" "$PLAN_SHORT"
printf '    plain file rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'plain file exit code' 2 "$LAST_RC"
seg_eq 'plain file hash unchanged' "$file_before" "$(sha256sum "$file_root" | cut -d' ' -f1)"
case_end "2: existing empty directory and plain file refused (rc=2), both untouched"

# --- case 3: existing populated batch directory -----------------------------
case_begin "3: existing populated batch directory is refused, tree byte-identical"
data_root="$SANDBOX/work/data/root"
mkdir -p "$data_root/runs/r01_on" "$data_root/logs"
printf 'SENTINEL environment.txt\n' >"$data_root/environment.txt"
printf 'SENTINEL source_fingerprint.txt\n' >"$data_root/source_fingerprint.txt"
printf 'label\tmode\tn\trc\tstarted\tended\nr01_on\ton\t10000\t0\tt0\tt1\n' >"$data_root/logs/summary.tsv"
printf '7\n' >"$data_root/runs/r01_on/exit_code"
printf '{"prior":true}\n' >"$data_root/runs/r01_on/report.json"
printf 'prior log\n' >"$data_root/logs/r01_on.log"
data_before="$(tree_fingerprint "$data_root")"
invoke "$RUNNER" "$data_root" "$PLAN_SHORT"
printf '    rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'exit code' 2 "$LAST_RC"
seg_tree_same 'populated batch dir' "$data_root" "$data_before"
case_end "3: populated batch directory refused (rc=2), bytes unchanged"

# --- case 4: symlinks (dir target and dangling) -----------------------------
case_begin "4: symlink targets are refused (directory symlink and dangling symlink)"
sym_root="$SANDBOX/work/symlink"
mkdir -p "$sym_root/target"
printf 'keep me\n' >"$sym_root/target/keep.txt"
target_before="$(tree_fingerprint "$sym_root/target")"
ln -s "$sym_root/target" "$sym_root/link_to_dir"
invoke "$RUNNER" "$sym_root/link_to_dir" "$PLAN_SHORT"
printf '    directory symlink rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'directory symlink exit code' 2 "$LAST_RC"
seg_contains 'directory symlink refusal message' "$SANDBOX/last.stderr" 'already exists'
seg_eq 'link text preserved' "$sym_root/target" "$(readlink "$sym_root/link_to_dir")"
seg_tree_same 'symlink target directory' "$sym_root/target" "$target_before"

ln -s "$sym_root/nowhere" "$sym_root/dangling"
invoke "$RUNNER" "$sym_root/dangling" "$PLAN_SHORT"
printf '    dangling symlink rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'dangling symlink exit code' 2 "$LAST_RC"
seg_contains 'dangling symlink refusal message' "$SANDBOX/last.stderr" 'already exists'
seg_eq 'dangling link text preserved' "$sym_root/nowhere" "$(readlink "$sym_root/dangling")"
seg_absent 'dangling link target not created' "$sym_root/nowhere"
case_end "4: directory symlink and dangling symlink refused (rc=2), targets untouched"

# --- case 5: a different plan does not get in -------------------------------
case_begin "5: existing directory is refused even for a different plan"
plan_root="$SANDBOX/work/plan/root"
mkdir -p "$plan_root/runs/r01_pristine" "$plan_root/logs"
printf 'SENTINEL environment.txt\n' >"$plan_root/environment.txt"
printf 'SENTINEL source_fingerprint.txt\n' >"$plan_root/source_fingerprint.txt"
printf 'label\tmode\tn\trc\tstarted\tended\n' >"$plan_root/logs/summary.tsv"
plan_before="$(tree_fingerprint "$plan_root")"
invoke "$RUNNER" "$plan_root" "on off"
printf '    requested plan "%s" on an existing default-plan batch rc=%s\n    stderr: %s\n' \
	"$PLAN_SHORT" "$LAST_RC" "$LAST_STDERR"
seg_eq 'exit code for a different plan' 2 "$LAST_RC"
seg_tree_same 'pre-existing batch dir' "$plan_root" "$plan_before"
seg_absent 'no new run directory for the other plan' "$plan_root/runs/r01_on"
case_end "5: different plan on an existing root refused (rc=2), tree unchanged"

# --- case 6: a fresh root runs the short plan to completion -----------------
case_begin "6: fresh root runs the short plan to completion"
happy_root="$SANDBOX/work/happy/root" # parent exists, root does not
mkdir -p "$SANDBOX/work/happy"
invoke "$RUNNER" "$happy_root" "$PLAN_SHORT"
printf '    rc=%s\n    stdout tail: %s\n' "$LAST_RC" "$(printf '%s\n' "$LAST_STDOUT" | tail -n 1)"
happy_summary="$happy_root/logs/summary.tsv"
seg_eq 'exit code' 0 "$LAST_RC"
seg_file 'summary' "$happy_summary"
seg_eq 'summary line count (header + 2 rows)' 3 "$(file_lines "$happy_summary")"
seg_eq 'summary rows (label/mode/n/rc)' \
	$'r01_on\ton\t10000\t0\nr02_off\toff\t10000\t0' \
	"$(tail -n +2 "$happy_summary" 2>/dev/null | cut -f1-4)"
seg_eq 'runs/r01_on/exit_code' 0 "$(file_content "$happy_root/runs/r01_on/exit_code")"
seg_eq 'runs/r02_off/exit_code' 0 "$(file_content "$happy_root/runs/r02_off/exit_code")"
seg_file 'round 1 log' "$happy_root/logs/r01_on.log.gz"
seg_file 'round 2 log' "$happy_root/logs/r02_off.log.gz"
seg_absent 'round 1 log left uncompressed' "$happy_root/logs/r01_on.log"
for gz in "$happy_root/logs/r01_on.log.gz" "$happy_root/logs/r02_off.log.gz"; do
	if gzip -t "$gz" 2>/dev/null; then seg_ok "valid gzip: $gz"; else seg_bad "invalid gzip: $gz"; fi
done
if zcat "$happy_root/logs/r01_on.log.gz" 2>/dev/null | grep -qF 'consumerseg-guard-fixture'; then
	seg_ok 'round 1 log came from the go stand-in (no real drill was started)'
else
	seg_bad 'round 1 log lacks the go stand-in marker'
fi
seg_contains 'environment.txt generated' "$happy_root/environment.txt" 'generated_at:'
seg_contains 'source_fingerprint.txt generated' "$happy_root/source_fingerprint.txt" 'sha256sum'
seg_contains 'runner reported the batch complete' "$SANDBOX/last.stdout" 'all 2 invocations ok'
case_end "6: fresh root ran the short plan end to end (rc=0), artifacts complete"

# --- case 7: concurrent claim of one fresh root -----------------------------
case_begin "7: four concurrent invocations of one fresh root: exactly one winner"
for round in 1 2; do
	race_parent="$SANDBOX/work/race$round"
	race_root="$race_parent/root"
	caps="$race_parent/cap"
	mkdir -p "$caps"
	pids=()
	for i in 1 2 3 4; do
		(
			PATH="$FAKE_BIN:$PATH" "$RUNNER" "$race_root" "$PLAN_SHORT" >"$caps/out.$i" 2>&1
			printf '%s\n' "$?" >"$caps/rc.$i"
		) &
		pids+=("$!")
	done
	for pid in "${pids[@]}"; do wait "$pid"; done

	winner=0
	refused=0
	other=0
	rcs=""
	for i in 1 2 3 4; do
		rc_i="$(cat "$caps/rc.$i")"
		rcs="${rcs}${rcs:+ }$rc_i"
		case "$rc_i" in
		0) winner=$((winner + 1)) ;;
		2) refused=$((refused + 1)) ;;
		*) other=$((other + 1)) ;;
		esac
	done
	printf '    round %s: exit codes %s\n' "$round" "$rcs"
	seg_eq "round $round winners (rc=0)" 1 "$winner"
	seg_eq "round $round refusals (rc=2)" 3 "$refused"
	seg_eq "round $round other exit codes" 0 "$other"

	race_summary="$race_root/logs/summary.tsv"
	seg_eq "round $round summary line count" 3 "$(file_lines "$race_summary")"
	seg_eq "round $round summary rows (label/mode/n/rc)" \
		$'r01_on\ton\t10000\t0\nr02_off\toff\t10000\t0' \
		"$(tail -n +2 "$race_summary" 2>/dev/null | cut -f1-4)"
	seg_eq "round $round duplicate labels" "" \
		"$(tail -n +2 "$race_summary" 2>/dev/null | cut -f1 | sort | uniq -d | tr '\n' ' ')"
	seg_eq "round $round environment.txt files" 1 "$(find "$race_root" -name environment.txt | wc -l)"
	seg_eq "round $round source_fingerprint.txt files" 1 "$(find "$race_root" -name source_fingerprint.txt | wc -l)"
	seg_contains "round $round environment.txt complete" "$race_root/environment.txt" 'generated_at:'
	seg_contains "round $round source_fingerprint.txt complete" "$race_root/source_fingerprint.txt" 'sha256sum'
	seg_eq "round $round run dirs" "r01_on r02_off" \
		"$(find "$race_root/runs" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null | LC_ALL=C sort | paste -sd' ' -)"
	seg_eq "round $round logs dir contents" \
		"r01_on.env r01_on.log.gz r01_on.rc r02_off.env r02_off.log.gz r02_off.rc summary.tsv" \
		"$(find "$race_root/logs" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null | LC_ALL=C sort | paste -sd' ' -)"
	for gz in "$race_root/logs"/*.log.gz; do
		if gzip -t "$gz" 2>/dev/null; then seg_ok "round $round valid gzip: $(basename "$gz")"; else seg_bad "round $round invalid gzip: $gz"; fi
	done
done
case_end "7: concurrent race — exactly one winner per round, artifacts self-consistent"

# --- case 8 (extra): argument errors and missing parents --------------------
case_begin "8 (extra): an invalid plan leaves no root; a missing parent says so"
args_root="$SANDBOX/work/args/root"
mkdir -p "$SANDBOX/work/args"
invoke "$RUNNER" "$args_root" "pristine bogus"
printf '    unknown mode rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'unknown mode exit code' 2 "$LAST_RC"
seg_absent 'no root left behind by the invalid plan' "$args_root"

missing_root="$SANDBOX/work/missing/deep/root" # neither parent exists
invoke "$RUNNER" "$missing_root" "$PLAN_SHORT"
printf '    missing parent rc=%s\n    stderr: %s\n' "$LAST_RC" "$LAST_STDERR"
seg_eq 'missing parent exit code' 2 "$LAST_RC"
seg_contains 'missing parent message reports a creation failure' "$SANDBOX/last.stderr" 'cannot create'
seg_absent 'nothing created below the missing parent' "$SANDBOX/work/missing"
case_end "8 (extra): invalid plan and missing parent both refused (rc=2), nothing written"

# --- result -----------------------------------------------------------------
printf '\n===== guard fixture result: %d passed, %d failed, %d skipped =====\n' "$pass" "$fail" "$skip"
if [[ "$fail" -gt 0 ]]; then
	printf 'guard fixture: FAIL\n'
	rc=1
else
	printf 'guard fixture: OK\n'
	rc=0
fi
printf 'guard fixture: log=%s rc=%d\n' "$LOG_FILE" "$rc" >&3
exit "$rc"
