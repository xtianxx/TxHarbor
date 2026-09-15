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
## Provenance appendix (#8 closure, 2026-09-15, docs-only)

Literal commands (entries per Makefile/ci.yml four jobs; sharding only where a
single 600s tool call cannot cover a package):

- `go build ./...`
- `gofmt -l internal/ cmd/`
- `go vet ./...` + `go vet -tags integration ./...`
- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go test -tags integration ./internal/withdrawal/ ./internal/app/ ./internal/db/ ./internal/health/ -count=1` (db/health ran
  together; withdrawal/app each alone)
- indexer, three selections (union covers all top-level tests, see below):
  `go test -tags integration ./internal/indexer/ -count=1 -run '^Test[A-C]'`
  `go test -tags integration ./internal/indexer/ -count=1 -run '^Test[D-P]'`
  `go test -tags integration ./internal/indexer/ -count=1 -run '^Test[Q-Z]'`

Shard-union computation (recomputed read-only 2026-09-15 at 513825d,
identical result):

- command: `grep -h '^func Test' internal/indexer/*_test.go`, partition by
  `^Test[A-C]` / `^Test[D-P]` / `^Test[Q-Z]`
- output: `total top-level: 271`, `A-C: 82`, `D-P: 111`, `Q-Z: 78`,
  `union: 271`, `overlap: 0`, `MISSED: none`
- caliber note: 271 = top-level `Test*` function names (selection units);
  580 = executed instances including subtests (173+316+91 shard tallies).
  The two numbers measure different units and are not expected to be equal.

Toolchain (current check 2026-09-15, NOT backdated to run time):

- `go version go1.26.5 linux/amd64` (queried now; the Go version active during
  the historical runs was never recorded → unrecoverable, stated as such)
- PostgreSQL image `postgres:18.6-trixie` (test-declared in
  `*_integration_test.go` container specs, verified by grep now; same image
  named by every integration run)

Per-tally provenance:

- witnessed (orchestrator ran the command in-session and saw the tool output;
  no log files were persisted, so "witnessed" means observed-live, not
  attached): unit 733→737, race 733→737, withdrawal 309→313→319, app
  139→145→147→153, db 88 (post-fix), health 5, indexer shards 173/316/91,
  union computation (twice, identical).
- re-verified: every agent-reported number above was re-run by the
  orchestrator on the exact staged content before merge (merge runs ==
  HEAD content; commits contain no further edits).
- reused (not re-run): db 88/health 5/indexer shards from the T030 turn for
  the review-fix merge (unaffected packages; code diff of the review-fix
  batch touches none of their non-test files — verified via staged file
  list), plus the T032-time quickstart §5 snapshot below.
- original logs vs summary: no raw `go test` logs exist on branch or in
 Ticket artifacts; all tallies are human summaries of observed tool output.
  Nothing here reconstructs or invents a missing log.
