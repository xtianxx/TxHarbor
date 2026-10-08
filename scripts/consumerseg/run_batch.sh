#!/usr/bin/env bash
# run_batch.sh is the 013 supplement's consumer-segmentation batch runner: it
# runs one serial batch of `go test` invocations — the pristine drill as the
# untouched control plus the instrumented drill in its ON (collection) and OFF
# (instrumented but not collecting) arms — and records one log, one exit code
# and one evidence directory per invocation.
#
# Usage: scripts/consumerseg/run_batch.sh <evidence-dir> [plan]
#
#   <evidence-dir>  batch root; must not exist — this invocation claims it
#                   atomically (single mkdir, no parents) before writing
#                   anything, and never reuses or cleans up an existing path
#   [plan]          space-separated modes (pristine|on|off), in run order.
#                   Default: pristine on off off on on off off on pristine
#                   (a pristine bookend, then four on/off pairs, then the
#                   pristine bookend again).
#
# Layout:
#   <dir>/runs/r<NN>_<mode>/     one run directory (report.json, exit_code,
#                                meta.json + anchors.json + *.csv for the
#                                instrumented arms; csV files are gzipped)
#   <dir>/logs/r<NN>_<mode>.log  full stdout+stderr of one invocation
#   <dir>/logs/r<NN>_<mode>.env  host/container state captured right before
#                                that invocation started
#   <dir>/logs/r<NN>_<mode>.rc   exit code of that invocation
#   <dir>/logs/summary.tsv       label, mode, n, rc, started, ended
#   <dir>/environment.txt        host, toolchain, Docker and pinned-image state
#   <dir>/source_fingerprint.txt branch/HEAD/status + sha256 of every source in
#                                play (the harness, the seam and go.mod)
#
# Discipline: the batch root is claimed with a single `mkdir` before any file
# is written — an existing directory (empty or not), a plain file or a symlink
# (dangling or not) is refused with exit 2 and left untouched, so a batch is
# never resumed into, mixed with or appended to an earlier one. One bounded
# invocation at a time (timeout 2700s inside a 40m test timeout), the first
# failure aborts the batch (the remaining modes are not started) and the
# summary is printed before exiting non-zero. Only the very first invocation
# of a batch may pull/build the test images; every later mode reuses them.
# Exit codes: 2 = usage error or refused target, 1 = a failed invocation
# mid-batch.
set -euo pipefail

DEFAULT_PLAN="pristine on off off on on off off on pristine"
LOAD_N=10000
INVOCATION_TIMEOUT_SECONDS=2700
KAFKA_IMAGE="confluentinc/confluent-local:7.9.10"
POSTGRES_IMAGE="postgres:18.6-trixie"

if [[ $# -lt 1 || $# -gt 2 ]]; then
	echo "usage: $0 <evidence-dir> [plan]" >&2
	echo "  <evidence-dir>: must not exist; created atomically (no parents)" >&2
	echo "  plan: space-separated modes (pristine|on|off); default: \"$DEFAULT_PLAN\"" >&2
	exit 2
fi

# The plan is pure string work, so it is validated before anything is created:
# a bad or empty plan must never leave an empty batch root behind.
PLAN="${2:-$DEFAULT_PLAN}"
read -r -a MODES <<<"$PLAN"
if [[ "${#MODES[@]}" -eq 0 ]]; then
	echo "empty plan" >&2
	exit 2
fi
for mode in "${MODES[@]}"; do
	case "$mode" in
	pristine | on | off) ;;
	*)
		echo "unknown mode in plan: $mode (want pristine|on|off)" >&2
		exit 2
		;;
	esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

# The batch root is claimed atomically before any file is written: `mkdir`
# without -p fails on an existing directory (empty or not), plain file or
# symlink (dangling or not), and only the invocation that created the directory
# goes on. Nothing under <evidence-dir> is ever appended to or cleaned up.
if ! mkdir_error="$(mkdir "$1" 2>&1)"; then
	if [[ -e "$1" || -L "$1" ]]; then
		printf 'consumer seg batch: %s already exists; refusing to reuse it (use a fresh evidence dir)\n' "$1" >&2
	else
		printf 'consumer seg batch: cannot create %s: %s\n' "$1" "$mkdir_error" >&2
	fi
	exit 2
fi
EVIDENCE_DIR="$(cd "$1" && pwd -P)"
LOGS_DIR="$EVIDENCE_DIR/logs"
RUNS_DIR="$EVIDENCE_DIR/runs"
mkdir -p "$LOGS_DIR" "$RUNS_DIR"
SUMMARY="$LOGS_DIR/summary.tsv"
cd "$REPO_ROOT"

printf 'consumer seg batch: evidence=%s repo=%s head=%s\n' \
	"$EVIDENCE_DIR" "$REPO_ROOT" "$(git rev-parse HEAD 2>/dev/null || echo unknown)"
printf 'consumer seg batch: plan=%s\n' "${MODES[*]}"

# --- batch-level environment and source fingerprint -------------------------
{
	echo "generated_at: $(date -u +%FT%T.%NZ)"
	date -u
	uname -a
	go version
	echo "nproc: $(nproc)"
	free -m
	df -h "$REPO_ROOT"
	echo "--- docker ps ---"
	docker ps 2>&1 || echo "docker ps failed"
	echo "--- docker image inspect (kafka, postgres) ---"
	for image in "$KAFKA_IMAGE" "$POSTGRES_IMAGE"; do
		printf '%s: ' "$image"
		docker image inspect --format '{{.Id}} created={{.Created}}' "$image" 2>&1 || true
	done
	echo "--- go.mod pins ---"
	grep -E 'franz-go|jackc/pgx|testcontainers' go.mod || true
} >"$EVIDENCE_DIR/environment.txt" 2>&1

FINGERPRINT_FILES=(
	internal/events/consumer.go
	internal/events/perf_seam_none.go
	internal/events/perf_seam_perf.go
	internal/events/backlog_drain_integration_test.go
	scripts/consumerseg/run_batch.sh
	go.mod
)
while IFS= read -r file; do
	FINGERPRINT_FILES+=("$file")
done < <(find internal/events -maxdepth 1 -name 'backlog_drain_seg_*' -print | sort)
while IFS= read -r file; do
	FINGERPRINT_FILES+=("$file")
done < <(find scripts/consumerseg/analyze -name '*.go' -print 2>/dev/null | sort)

{
	echo "generated_at: $(date -u +%FT%T.%NZ)"
	echo "branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
	echo "head: $(git rev-parse HEAD 2>/dev/null || echo unknown)"
	echo "head_subject: $(git log -1 --format='%s' 2>/dev/null || echo unknown)"
	echo "go: $(go version)"
	echo "kernel: $(uname -srm)"
	echo "--- git status --porcelain (snapshot) ---"
	git status --porcelain 2>/dev/null || true
	echo "--- sha256sum ---"
	for file in "${FINGERPRINT_FILES[@]}"; do
		printf '%s  %s\n' "$(sha256sum "$file" | cut -d' ' -f1)" "$file"
	done
} >"$EVIDENCE_DIR/source_fingerprint.txt" 2>&1

# --- one invocation per planned mode ---------------------------------------
# The batch root was claimed exclusively above, so this summary and every round
# label are written exactly once per batch: a resumed batch can never mix runs
# measured with different source revisions into an existing directory.
printf 'label\tmode\tn\trc\tstarted\tended\n' >"$SUMMARY"

failed=0
total="${#MODES[@]}"
for i in "${!MODES[@]}"; do
	mode="${MODES[$i]}"
	index=$((i + 1))
	label="$(printf 'r%02d_%s' "$index" "$mode")"
	run_dir="$RUNS_DIR/$label"
	log="$LOGS_DIR/$label.log"
	mkdir -p "$run_dir"
	started="$(date -u +%FT%T.%NZ)"

	printf '==> [%d/%d] %s (mode=%s) started %s\n' "$index" "$total" "$label" "$mode" "$started"

	# Environment witness of this round, written before the invocation starts
	# so an anomalous run can be correlated with the host state around it. A
	# failed probe is recorded, never fatal.
	{
		echo "date: $(date -u +%FT%T.%NZ)"
		uptime
		free -m | head -2
		echo "--- docker stats ---"
		docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' 2>&1 ||
			echo "docker stats failed"
	} >"$LOGS_DIR/$label.env" 2>&1

	set +e
	if [[ "$mode" == "pristine" ]]; then
		TXHARBOR_BACKLOG_DRAIN_N="$LOAD_N" \
			TXHARBOR_BACKLOG_REPORT="$run_dir/report.json" \
			timeout "$INVOCATION_TIMEOUT_SECONDS" go test -tags integration_backlog -count=1 -timeout 40m \
			-run '^TestBacklogDrain$' -v ./internal/events/ >"$log" 2>&1
	else
		seg=0
		if [[ "$mode" == "on" ]]; then
			seg=1
		fi
		TXHARBOR_BACKLOG_DRAIN_N="$LOAD_N" \
			TXHARBOR_BACKLOG_SEG="$seg" \
			TXHARBOR_BACKLOG_SEG_DIR="$run_dir" \
			TXHARBOR_BACKLOG_REPORT="$run_dir/report.json" \
			timeout "$INVOCATION_TIMEOUT_SECONDS" go test -tags "integration_backlog perf" -count=1 -timeout 40m \
			-run '^TestBacklogDrainSeg$' -v ./internal/events/ >"$log" 2>&1
	fi
	rc=$?
	set -e

	printf '%s\n' "$rc" >"$LOGS_DIR/$label.rc"
	printf '%s\n' "$rc" >"$run_dir/exit_code"
	ended="$(date -u +%FT%T.%NZ)"
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$mode" "$LOAD_N" "$rc" "$started" "$ended" >>"$SUMMARY"

	# Compress the bulk evidence of this round (the exit code, the report and
	# the anchors stay plaintext). -n keeps gzip byte-reproducible; -f keeps
	# the runner non-interactive.
	shopt -s nullglob
	csv_files=("$run_dir"/*.csv)
	shopt -u nullglob
	if [[ "${#csv_files[@]}" -gt 0 ]]; then
		gzip -n -f "${csv_files[@]}"
	fi
	if [[ -f "$log" ]]; then
		gzip -n -f "$log"
	fi

	if [[ "$rc" -ne 0 ]]; then
		failed=1
		printf 'consumer seg batch: %s FAILED (rc=%s); log=%s.gz\n' "$label" "$rc" "$log" >&2
		break
	fi
	printf 'consumer seg batch: %s ok (log=%s.gz)\n' "$label" "$log"
done

printf '\nconsumer seg batch summary (%s):\n' "$SUMMARY"
cat "$SUMMARY"
if [[ "$failed" -ne 0 ]]; then
	printf 'consumer seg batch: aborted after a failed invocation; evidence so far is under %s\n' "$EVIDENCE_DIR" >&2
	exit 1
fi
printf 'consumer seg batch: all %d invocations ok; evidence under %s\n' "$total" "$EVIDENCE_DIR"
