# 015 / R2 bounded technical validation evidence (2026-10-04, sanitized)

Scope: four-identity R2 permission-plan verification only; zero production
connection, zero personal-database touch. Disposable postgres:18.6-trixie
(docker digest sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722),
one-shot container r2lab-pg, network 172.17.0.x only. Plaintext SQL text and
TOC are record-level artifacts; no DSNs, no passwords are logged here.

Cases (see r2_grant_matrix.tsv for disposition):
- case1/case2/case3/case4/case5/case6/case7/case8/case9/case10 report why
  "pre-grant object ownership to recovery role" does not enable the archive's
  ALTER ... OWNER TO original writer under the pinned vector.
- case11/pos2/pos3 are three independent SUCCESS runs of the pinned command
  vector with --no-owner --no-privileges (restricted role), each from a clean
  target: same outcome, 7 relations restored as owner=recovery role, probe row
  reloaded, sequence advance identical.
- case12 (--role=writer_owner) succeeds as well in this standalone lane; not
  the production planned vector, kept for completeness.
- neg_nocreate / neg_crossdb / neg_crossdb2 / neg_wrongrole are the four
  refusal classes: missing schema CREATE; schema-authorized but unauthorized
  other database; connection-level CONNECT denial; non-recovery identity with
  equivalent grants.

PG18 semantics cross-check: ALTER_TABLE_OWNER_NOTES.md. R2 recommendation: R2P
verifies via both (a)R and (a-1) actual verified mechanism — objects restored
natively under R (+ optional deployment-lane ownership handover to W, with
audited ALTER ... OWNER TO executed as W-or-admin, never through a session
owned by R). --role based SET ROLE W is explicitly NOT a production approval
path here; it remains a secdiscussable but implementation-open vector (see
ADR-004 §2.1 R2 pending ruling).

## R5-consumption and stale-attempt clarification (unchanged from 2026-10-04 ruling; re-affirmed)

A consumed one-shot handle refuses further launches only. Continuing
evidence-taking and acceptance run inside the same coordinator run via the
Observation/one-use receipt path and existing prelaunch/acceptance
transactions (ADR-004 §2.1 R5 post-consumption paragraph). An unclosed
instance does not extend a voided attempt; new executions must re-satisfy
admission per R1, and controlled reconciliation only clears blocking records
(fresh exclusion + original-attempt proof), never reviving old handles.

## Bounded-validation disclaimers

- The runs in evidence/ are the disposable isolated lane r2lab-pg only; they
  never touched any production or personal database.
- These runs do NOT check any of the six OPEN tasks (T019/T020/T057/T058/
  T066/T068) and do not justify marking any of them complete; the drain T065/
  T068 tasks remain managed by the parent.
- The full-repo Go build failure over the archived evidence-tree .go snapshots
  is recorded as an existing issue and stands untouched (go build ./internal/...
  ./cmd/... exits 0; the failure is in docs/evidence snapshot files only).
- When a command was intended as a run — not as a code-executed command —
  it is stated plainly in these notes; no command was silently skipped.
