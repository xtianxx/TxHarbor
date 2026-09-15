# 007 closeout evidence (T030–T033 + review-fix batch)

Source rule: tallies below were witnessed by the orchestrator running the
commands in-session. Agent-reported numbers were re-run before merge; nothing
here is copy-pasted from an unverified report. Indexer shards ran as three
`go test -tags integration ./internal/indexer/ -run` selections because one
600s tool call cannot cover the package; shard regexes partition all 271
top-level `Test*` names (`^Test[A-C]` 82 / `^Test[D-P]` 111 / `^Test[Q-Z]` 78,
union 271, overlap 0, missed 0 — computed, not summed).

## Final gates (HEAD b54ec24 → review-fix HEAD, pre-commit)

| Entry | Result |
|---|---|
| `go build ./...` | clean |
| `gofmt -l internal/ cmd/` | clean |
| `go vet ./...` + `go vet -tags integration ./...` | clean |
| `go test ./... -count=1` | 737 passed / 11 pkgs |
| `go test -race ./... -count=1` | 737 passed / 11 pkgs |
| `-tags integration ./internal/withdrawal/` | 319 passed |
| `-tags integration ./internal/app/` | 153 passed |
| `-tags integration ./internal/db/` | 88 passed |
| `-tags integration ./internal/health/` | 5 passed |
| `-tags integration ./internal/indexer/` shards | 173 + 316 + 91 passed |

New tests in the review-fix batch (all green above): nil-pool unit +
nil-pool handler path; canonical-echo create + different-case replay;
revoke-miss caller-0; 404 wire equality; either-recovery-read unknown ×2;
metrics 8-series exactly-once; retention permanence; status-pending 23514.

## Honest boundaries (unchanged)

- Trigger-raised 40P01 exercises server-error classification/rollback, not a
  true deadlock cycle (native probe = class-existence only).
- SIGKILL evidence = real child + real SIGKILL at two deterministic points;
  caller-loss tests disclaim process-death equivalence in-file.
- Metrics exactly-once = per-attempt counter bump, not global exactly-once
  under crash/network retry.
- Retention = bounded-window observation + zero DELETE/TTL paths (grep over
  `*.go`/`*.sql` finds no `DELETE FROM`/`TRUNCATE` on the three tables; only
  000007 Down `DROP TABLE` schema rollback), not infinite-time proof.
- Revoke-miss `caller_id=0`: `RevokeGrant`/CLI carry no caller input by
  design; `IssueKey` rejects `callerID<=0`, so 0 is an unambiguous
  unattributable marker required for O-dedup (never operator-as-caller).
- T030 db-test fix is test-only (chain-relative counts + 007 removal/re-apply
  asserts); 001–006 schema untouched. Downgrade `[7 6 5]` hardcodes the
  current head — 008 will need the same maintenance.
