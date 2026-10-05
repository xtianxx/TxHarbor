# WPA-A: RST hypothesis controlled verification — results

Branch: 015-followup-deposit-unknown-commit @ 2fca0b05 (no branch switch, no repo edits)
CI under investigation: main run 37274084298, job 111647171009, test
`TestDepositCommitUnknownOutcomeRereadsDB` (internal/indexer/deposit_integration_test.go:1124).

CI failure text (verbatim):

```
--- FAIL: TestDepositCommitUnknownOutcomeRereadsDB (0.14s)
    deposit_integration_test.go:1124: uncertain commit = commit deposit unit [10,20]
    (outcome unknown, progress unchanged): conn closed, want nil after the durable re-read
```

## Hypothesis under test (as handed over)

1. injector trigger frame = `Q\x00\x00\x00\x0bcommit\x00` (simple-protocol Query, pgx v5, text "commit");
2. `logscanCommitDropConn.Write` forwards COMMIT then immediately `Close()`; on Linux close with
   *unread data in the receive buffer* sends RST instead of FIN → server-side socket abort →
   in-flight COMMIT rolled back → commit invisible → immediate re-read sees unchanged progress;
3. the test comment "the forwarded COMMIT still lands" is violated by these Close semantics;
   local PASS is the race where the server finished the commit before the RST;
4. fix direction: read-side `logscanOpenCompletionDropPool` (observe completion pair, then close).

## Method

Temporary test file `internal/indexer/zzwpa_rst_probe_test.go` (same package, `//go:build integration`),
run against the package's shared testcontainers PostgreSQL 18.6 (Docker). All wire semantics replicated
exactly: raw write, `bytes.Contains(b, "commit")` trigger, arm CAS, close client side. 3 full runs
(run.log / run2.log / run3.log); the primary artifacts below are from run3 which also added the
delayed-read kernel scenarios. No production/test file was modified; the temp file is deleted.

Commands: `go test -tags integration -count=1 -timeout 25m -run 'TestZZWpa' -v ./internal/indexer/`

### Phase 1 — kernel close semantics (in-process TCP, no PostgreSQL), 3 reps each

| scenario | unread data at close | server first-read class | server classify-read | post-close server write |
|---|---|---|---|---|
| plain-no-unread (default injector condition) | no | data(4) | **FIN** | ok |
| plain-unread (reply already queued) | 7 B | data(4) | **RST** | error |
| linger0-no-unread (forced RST) | no | data(4) | **RST** | error |
| linger0-unread | 7 B | data(4) | **RST** | error |
| lingerPositive-no-unread | no | data(4) | **FIN** | ok |
| lingerPositive-unread | 7 B | data(4) | **RST** | error |
| **linger0-delayed-read** (RST sent, server sleeps 300 ms before reading) | no | **data(4)** | RST | error |
| plain-delayed-read | no | data(4) | FIN | ok |

Findings: (a) plain close with an empty receive queue → FIN (the exact default-injector condition);
(b) close with unread data → RST even with positive linger; (c) **already-queued bytes survive a
received RST**: the peer still reads the data first and only then gets ECONNRESET.

### Phase 2 — raw pgx tx (BEGIN; INSERT; COMMIT) through the drop wrapper, 4 groups x 60, run3

Trigger frame in 240/240 iterations: `510000000b636f6d6d697400` = `Q`, len 11, `commit\0`.
pgx error text in 240/240 iterations: `conn closed` (pgconn.ErrConnClosed) — byte-identical to the CI
failure's suffix. `dropped` = exactly 1 per armed commit. FIONREAD at the commit write/close: **0** in
all 180 default/linger0/lingerPositive iterations (the reply cannot and does not arrive before close).

| group | n | immediate re-read visible | committed visible (settle ≤2 s) | never visible | recvq at close |
|---|---|---|---|---|---|
| **default** (production close logic) | 60 | 41 | **60** | 0 | 0 B |
| **linger0** (SetLinger(0), forced RST) | 60 | 47 | **60** | 0 | 0 B |
| **lingerPositive** (SetLinger(2), FIN) | 60 | 60 | **60** | 0 | 0 B |
| linger0AfterReply (reply queued, then RST) | 60 | 60 | **60** | 0 | 18 B |

Replicates (default / linger0 settled-visible, immediate-visible):
run1 60/60 & 60/60 (immediate 40, 44); run2 60/60 & 60/60 (immediate 45, 45); run3 60/60 & 60/60
(immediate 41, 47). Pooled: default-path commits 180/180; forced-RST commits 180/180; no late-visible
rows (final re-check); no never-visible rows.

Race timing in the default group (the CI signature): 19/60 immediate misses. In every miss the
immediate query completed 0.65–0.97 ms after the COMMIT write, while the commit became visible
6.56–7.65 ms after it; 41/60 hits completed at ~0.78 ms (commit fast path ~0.8 ms). Same distribution
in the linger0 misses. `lingerPositive` close blocks until the connection closes (server finished),
so the re-read always follows completion (immediate 60/60).

### Phase 3 — production path (`commitDepositUnit`, first unit 10→20) with the ORIGINAL
`logscanCommitDrop` type (recording conn underneath), 60 iterations

- prodErr `nil` 60/60 (production `commitResultVisible` saw the advance every time);
- committed visible 60/60; trigger frame `Q...commit` 60/60; dropped = 1 each; recvq at commit write 0.

### Independent raw-socket confirmation (throwaway container, `/tmp/wpa-rst/raw_rst_fin_probe.txt`)

Protocol-level client sends `Q commit` then either SO_LINGER=0 RST or plain FIN: both rows were
committed and visible from a fresh connection 1.5 s later. PostgreSQL 18.6 logged no connection-loss
message for either close. (Container logs in the main runs likewise contain no reset/EOF/broken-pipe
backend lines; the only per-iteration server LOG lines are `PID N in cancel request did not match any
process`, which are artifacts of pgx's own `asyncClose` cancel request after the injected close —
they are evidence that pgx observed the close, not of a server-side abort. Log channels were therefore
not discriminating; commit visibility is.)

## Verdict: hypothesis (2)/(3) REFUTED as the CI mechanism; race CONFIRMED

- The default injector never sends RST: FIONREAD at close was 0 in all 240+60 measured iterations
  (structurally: the reply can only exist after the server sent CommandComplete, i.e. after commit
  completion), and empty-queue close sends FIN (phase 1).
- Even a forced RST immediately after the COMMIT write does not roll the commit back: 180/180
  committed across three replicates (and the kernel delivers already-queued COMMIT bytes before the
  reset error, phase-1 delayed-read).
- The only close that can RST (unread data) RSTs *after* the reply — i.e. after the commit was
  already durable; that exact scenario keeps the commit (180/180).
- The reproduced CI signature is the fsync/commit-completion vs immediate-re-read race: re-read at
  ~0.7 ms vs commit completion at ~7 ms in 25–33 % of local iterations, commit always landing; on a
  loaded CI runner the slow-tail dominates. This matches the repo's existing note ("fsync-vs-reread
  race diagnosed on the T028 CI failure" in logscan_integration_test.go).
- Fix direction supported: `logscanOpenCompletionDropPool` withholds the reply until the
  CommandComplete(COMMIT)+ReadyForQuery('I') pair is observed, so any client error implies the commit
  is already durable and the owning re-read is deterministic. The write-side injector's advertised
  promise ("the forwarded COMMIT still lands") is empirically true (all 360 default-path commits
  landed) — but it is not *timely*, which is what the test asserts.

## Evidence inventory (/tmp/wpa-rst)

- phase1_close_semantics.json — kernel table (24 observations)
- phase2_iterations.jsonl, phase2_summary.json — 240 wire-level iterations (run3)
- phase3_production.jsonl, phase3_summary.json — 60 production-path iterations (original injector)
- container_log_deltas_phase2.json / _phase3.json, container_logs_tail_phase{1,2,3}.txt
- run.log, run2.log, run3.log — three full replicate runs (phase2 log lines carry per-iteration data)
- raw_rst_fin_probe.txt — independent raw-socket RST/FIN confirmation
- ci_job.log — full CI job log of run 37274084298 (job 111647171009); failure block at line ~1568
