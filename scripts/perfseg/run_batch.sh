#!/usr/bin/env bash
# run_batch.sh is the controlled four-condition segment batch runner
# (perf/seg-normal; R = query-class limiter on/off, G = background runtimes
# on/off). It runs five fixed `go test` invocations serially — an overhead
# bookend without collection, then three repeats covering every arm — and
# records one log and one exit code per invocation.
#
# Usage: scripts/perfseg/run_batch.sh <evidence-dir>
#
# Layout:
#   <dir>/logs/<label>.log    full stdout+stderr of one invocation
#   <dir>/logs/<label>.rc     exit code of that invocation
#   <dir>/logs/summary.tsv    label, arms, repeat, rc for every invocation
#   <dir>/repeat<N>/<arm>/    arm evidence (written by TestSegNormalBatch)
#
# The first failing invocation stops the batch (the remaining calls are not
# started) and the summary is printed before exiting non-zero. No compression,
# no report rendering: the orchestration side post-processes the arm dirs.
set -euo pipefail

if [[ $# -ne 1 ]]; then
	echo "usage: $0 <evidence-dir>" >&2
	exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
mkdir -p "$1"
EVIDENCE_DIR="$(cd "$1" && pwd -P)"
LOGS_DIR="$EVIDENCE_DIR/logs"
mkdir -p "$LOGS_DIR"
SUMMARY="$LOGS_DIR/summary.tsv"

# Fixed order: overhead bookend, then the three repeats, then the overhead
# bookend again. Overhead runs use their own repeat ids (10/11) so the two
# bookends never overwrite each other; collect=false.
LABELS=(overhead1 repeat1 repeat2 repeat3 overhead2)
ARMS=(
	'R1G1:nocoll'
	'R0G0,R1G0,R0G1,R1G1'
	'R1G0,R0G1,R1G1,R0G0'
	'R0G1,R1G1,R0G0,R1G0'
	'R1G1:nocoll'
)
REPEATS=(10 1 2 3 11)

printf 'label\tarms\trepeat\trc\n' > "$SUMMARY"
cd "$REPO_ROOT"

failed=0
printf 'perf seg batch: evidence=%s repo=%s commit=%s\n' \
	"$EVIDENCE_DIR" "$REPO_ROOT" "$(git rev-parse HEAD 2>/dev/null || echo unknown)"

for i in "${!LABELS[@]}"; do
	label="${LABELS[$i]}"
	arms="${ARMS[$i]}"
	repeat="${REPEATS[$i]}"
	log="$LOGS_DIR/$label.log"
	rc_file="$LOGS_DIR/$label.rc"
	printf '==> [%d/%d] %s: arms=%s repeat=%s\n' \
		"$((i + 1))" "${#LABELS[@]}" "$label" "$arms" "$repeat"

	set +e
	TXHARBOR_PERF_EVIDENCE_DIR="$EVIDENCE_DIR" \
		TXHARBOR_SEG_EVIDENCE_DIR="$EVIDENCE_DIR" \
		TXHARBOR_SEG_ARMS="$arms" \
		TXHARBOR_SEG_REPEAT="$repeat" \
		timeout 2400 go test -tags perf -count=1 -timeout 35m \
		-run '^TestSegNormalBatch$' -v ./internal/perf >"$log" 2>&1
	rc=$?
	set -e
	printf '%s\n' "$rc" > "$rc_file"
	printf '%s\t%s\t%s\t%s\n' "$label" "$arms" "$repeat" "$rc" >> "$SUMMARY"
	if [[ "$rc" -ne 0 ]]; then
		failed=1
		printf 'perf seg batch: %s FAILED (rc=%s); log=%s\n' "$label" "$rc" "$log" >&2
		break
	fi
	printf 'perf seg batch: %s ok (log=%s)\n' "$label" "$log"
done

printf '\nperf seg batch summary (%s):\n' "$SUMMARY"
cat "$SUMMARY"
if [[ "$failed" -ne 0 ]]; then
	printf 'perf seg batch: aborted after a failed invocation; arm evidence so far is under %s\n' "$EVIDENCE_DIR" >&2
	exit 1
fi
printf 'perf seg batch: all %d invocations ok; arm evidence under %s\n' "${#LABELS[@]}" "$EVIDENCE_DIR"
