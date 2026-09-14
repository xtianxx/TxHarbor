# Data Model: 006-reorg-recovery

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14 | **Migration**: `migrations/000006_reorg_recovery.sql`
(goose, new file; 000002–000005 untouched in meaning. `embed.go` auto-includes `*.sql`.)

006 is a recovery writer on top of 002–005 truths: it never redefines upstream semantics, only
invalidates, rolls back, replays, revives, and releases under its own version. All transactions
reuse `indexer_lease` as the single coordination row (research R1); decisions in research.md.

## Table 1 — `reorg_recovery` (active recovery instance, one row per chain)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK, `CHECK (> 0)` | single active recovery per chain |
| recovery_id | TEXT | `NOT NULL UNIQUE`, never reused | instance identity (`reorg-<old_tip>-<detected_at>` shape, uniqueness by DB) |
| phase | TEXT | `NOT NULL CHECK IN ('detected','ancestor_confirmed','invalidated','replaying','complete_pending','reconcile_required')` | resume point (see §Phases) |
| policy_seq | BIGINT | `NOT NULL CHECK (> 0)`, FK → `reorg_policy_history (chain_id, policy_seq)` | bound depth-config version (Q1: no retro widening) |
| max_depth | BIGINT | `NOT NULL CHECK (> 0)` | bound value snapshot (audit convenience; authority is the policy row) |
| bound_old_number | BIGINT | `NOT NULL CHECK (>= 0)` | bound old canonical tip height (Q1 anchor) |
| bound_old_hash | TEXT | `NOT NULL`, hash format | bound old canonical tip hash |
| ancestor_number | BIGINT | NULL, `CHECK (>= 0)` | NULL until ancestor_confirmed |
| ancestor_hash | TEXT | NULL, hash format or NULL | NULL until ancestor_confirmed |
| new_tip_number | BIGINT | NULL | latest observed new-chain tip (tracking only, never a completion input) |
| new_tip_hash | TEXT | NULL | ditto |
| block_frontier | BIGINT | NULL | replay frontier per stream (NULL = not started) |
| log_frontier | BIGINT | NULL | ditto |
| deposit_frontier | BIGINT | NULL | ditto |
| detected_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | fork-evidence time |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | diagnosis |
| recovery_seq | BIGINT | `NOT NULL CHECK (> 0 AND recovery_seq < 9223372036854775807)` | fencing version; rule below |

- One row = one active recovery; repeat triggers converge (INSERT ... ON CONFLICT DO NOTHING, loser
  re-reads and joins). Terminal release = row DELETE (only by `complete_reverify` or `auth_release`
  transactions) + terminal event in Table 2. History lives in Table 2, never in this table.
- `recovery_seq` generation, persistence, exhaustion (authoritative rule): new value =
  `COALESCE(MAX(recovery_seq), 0) + 1` over `reorg_recovery_events` for the chain,
  read under the lease lock inside `establish`; assert result `< 9223372036854775807` (MaxInt64),
  else refuse establish (never wrap, never reset, never reuse). Rationale: Table 2 is append-only
  and survives row DELETEs, so the sequence is monotonic across rounds and releases — round-1
  executors can never collide with round-2 values, and deletes cannot resurrect old versions.
  (A SEQUENCE object is deliberately not used: rollback of a failed establish must not burn fencing
  versions outside the audited event stream; per-chain event volume is tiny so MAX is cheap.)
- Exact boundary (no "saturated" hand-waving): the last usable version is MaxInt64−1 =
  9223372036854775806. With events MAX at that value the next `establish` computes MaxInt64, fails
  the `< 9223372036854775807` assert (and would independently fail the column
  `CHECK (recovery_seq < 9223372036854775807)`), and refuses with zero writes — no recovery row, no
  event row, no frontier movement, no wrap, no reuse. Unreachable in practice (tens of rows per
  recovery); the exactness is what T019 pins, not a capacity claim. Supported range is unchanged:
  1 … MaxInt64−1, never 0, never MaxInt64, never reset.
- `CHECK (ancestor_number IS NULL) = (ancestor_hash IS NULL)`; when set,
  `CHECK (bound_old_number - ancestor_number >= 0 AND bound_old_number - ancestor_number <= max_depth)`.

## Table 2 — `reorg_recovery_events` (append-only audit)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| recovery_id | TEXT | `NOT NULL` | instance (no FK: survives row DELETE) |
| recovery_seq | BIGINT | `NOT NULL CHECK (> 0)` | fencing version of the causing instance (copied from Table 1 at write time) |
| event_seq | BIGINT | `NOT NULL CHECK (> 0)` | per-instance order |
| event | TEXT | `NOT NULL CHECK IN ('established','ancestor_confirmed','blocks_invalidated','observations_invalidated','checkpoints_rolled_back','replay_progress','observation_revived','auto_completed','repair_authorized','released','reconcile_signaled')` | phase/transition record |
| detail | TEXT | `NOT NULL DEFAULT ''` | evidence refs (heights, hashes, ranges, versions); no secrets, no raw RPC dumps |
| at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

- `UNIQUE (chain_id, recovery_id, event_seq)`; rows never UPDATE/DELETE (implementation asserts
  append-only). Per-(recovery_id) full lifecycle traceable after the active row is gone.

## Table 3 — `reorg_policy_history` (depth-config versions, mirrors 005 policy table)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK first col, `CHECK (> 0)` | |
| policy_seq | BIGINT | PK, `CHECK (> 0)`, in-chain increment (max+1 in txn, first = 1) | version identity |
| max_depth | BIGINT | `NOT NULL CHECK (> 0)` | Q1: required, no default, no business cap |
| prev_seq | BIGINT | NULL (first) | FK self-reference same chain |
| operator | TEXT | `NOT NULL DEFAULT ''` | authorizer (`bootstrap` for first row, written by first establish txn) |
| reason | TEXT | `NOT NULL DEFAULT ''` | |
| request_id | TEXT | NULL (first row only); non-null unique per chain + bootstrap partial-unique | idempotency identity (004/005 rule shape) |
| expected_old_seq | BIGINT | `NOT NULL DEFAULT 0` | caller's expected current version |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

- Effective policy = `MAX(policy_seq)` row. Change txn = authorized privileged txn (DB operator,
  same shape as confirmauth/depositauth: expected_old_seq gate, new≠old, single INSERT, request_id
  idempotency, atomic-or-nothing). Startup compares env max_depth to effective row: mismatch → loud
  refuse (005 drift precedent). In-flight recoveries bind captured `policy_seq`.

## Table 4 — `deposit_observation_transitions` (status transition log, repeated cycles)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| block_hash | TEXT | `NOT NULL`, hash format | source identity |
| tx_hash | TEXT | `NOT NULL`, hash format | source identity |
| log_index | BIGINT | `NOT NULL CHECK (>= 0)` | source identity |
| from_status | TEXT | `NOT NULL CHECK IN ('pending','confirmed','orphaned')` | |
| to_status | TEXT | `NOT NULL CHECK IN ('pending','confirmed','orphaned')` | |
| recovery_id | TEXT | `NOT NULL` | causing recovery (no FK: survives release) |
| basis_snapshot | TEXT | `NOT NULL DEFAULT ''` | confirm basis / orphan evidence / re-verify refs at transition time |
| at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

- `UNIQUE (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id)` —
  repeat execution converges (second write conflicts → already recorded). No FK to observations
  (rows outlive any state). `Confirmed→Orphaned→Pending→Confirmed` across recoveries = 3+ rows here,
  observation row itself only ever shows current status.

## Altered tables (000006, additive + predicate rewrites, no meaning change to 005 data)

**`chain_blocks`** (research R2): PK becomes `(chain_id, number, hash)`; retain
`UNIQUE (chain_id, number, hash)` (FK target for `indexer_checkpoint`) and `UNIQUE (chain_id, hash)`;
add partial `UNIQUE (chain_id, number) WHERE canonical` (I1 preserved at storage layer).
Existing rows (all canonical today) stay valid untouched.

**`deposit_observations`**: widen `deposit_observations_status_check` to
`status IN ('pending','confirmed','orphaned')`; rewrite
`deposit_observations_confirmation_consistency` as 3-state:
`((status='pending') = (confirmed_at IS NULL AND orphaned_at IS NULL))`
`AND (status='pending' OR (status='confirmed' AND <existing six-NOT-NULL>)`
`OR (status='orphaned' AND orphaned_at IS NOT NULL AND orphan_recovery_id IS NOT NULL))`;
basis columns never cleared on orphan (retained as history evidence).
Add `orphaned_at TIMESTAMPTZ NULL`, `orphan_recovery_id TEXT NULL`, `orphan_reason TEXT NULL`.

## Phases (resume points; crash restarts from persisted phase + frontiers)

`detected` → `ancestor_confirmed` → `invalidated` → `replaying` → `complete_pending` → (row DELETE,
terminal event) · any phase → `reconcile_required` (on unrecoverable evidence; terminal until
`auth_release`). Repeat triggers never move phase backward. Frontiers advance monotonically inside
`replaying`; a re-fork during recovery re-validates the ancestor (mismatch → back to
`ancestor_confirmed` search with new evidence, old conclusions re-verified before reuse).

## Transaction catalog (each: lease lock first, post-lock independent rechecks, writes + rowcounts)

Notation: `L` = lease ownership recheck, `P` = pause/ownership recheck, `V` = recovery-seq recheck,
`G` = exact progress/guard recheck. All run `SET LOCAL statement_timeout='5s'`, zero RPC inside.

| Transaction | Post-lock rechecks | Write set | Post-commit invariant |
|---|---|---|---|
| `establish` | L + policy bind (env max_depth == effective row, else refuse) + no active recovery row (else converge) + record pre-existing stream pauses as precondition (never modify them) | INSERT recovery row (phase=detected, bound tip, seq=events-MAX+1 asserted < MaxInt64) + established event row, SAME txn (atomic; rollback burns no seq). Writes NO pause table (research R9) | recovery row exists; ordinary commits refused via recovery gate; stream diagnoses intact; exactly one active recovery |
| `confirm_ancestor` | L + V + ancestor candidate re-verified (local+chain equality, continuity) + depth recompute ≤ bound | UPDATE phase→ancestor_confirmed + ancestor cols + event row | ancestor satisfies closed-bound formula |
| `invalidate_blocks` | L + V + phase | UPDATE `chain_blocks SET canonical=false` over `[ancestor+1, sweep_tip]` where canonical (rowcount recorded) + event row | old fork rows retained non-canonical; single canonical per height preserved |
| `invalidate_observations` | L + V + phase | UPDATE observations in swept range to `orphaned` + evidence cols + transition-log rows | affected Pending/Confirmed → Orphaned; ancestor-side rows untouched |
| `rollback_block/log/deposit_checkpoint` | L + V + phase + exact current-position match | guarded next_block/floor move (`$to = max(ancestor+1, start)`) | floors respected; no forward jump of unprocessed positions |
| `replay_range` (per stream) | L + V + phase + frontier match + coverage/canon proof over batch | new rows (new identities only) + frontier advance, same batch | idempotent; empty ranges advance legitimately |
| `revive_observation` | L + V + phase + row still orphaned + block/log binding + history-semantics recheck | status→pending + transition-log row (Q4 six rules) | no duplicate observation; old basis retained |
| `complete_reverify` (auto) | L + V + phase + ALL FR-19判据 re-read (ancestor, invalidation, conversions, rollback, replay coverage, reconfirmation) | DELETE recovery row + terminal event. Terminal `detail` MUST carry: bound tip, policy_seq, ancestor, swept ranges, disposition list, surviving stream pauses | only this recovery's cause released; independent pauses intact and still stopping ordinary work |
| `auth_enter_repair` (manual) | privilege (operator session) + Q2b evidence present + active row match | phase→repair-authorized marker + event row (NOT a release) | does not release confirm/sign/broadcast pauses |
| `auth_release` (manual) | privilege + Q2b minimum evidence + disposition list all terminal + re-verify pass | DELETE recovery row + terminal event with the same mandatory content as above + audit. NEVER deletes stream pause rows | atomic-or-nothing; reboot re-verifies; independent pauses intact |

Programmatic reconcile writes (evidence/progress/disposition during recovery) run as short txns under
the same lock with V + permission rechecks; each audited; none can trigger a paused external action
or skip the completion gate (Q3).

## Existing-interface changes (006 must touch; enumerated, no silent bypass left)

1. `scanner.go` insert/compare path: height-sibling aware (same height + different hash outside active
   recovery = fork evidence → existing hash_mismatch pause path; inside recovery = 006-owned rows).
   Upsert target retargets to `(chain_id, number, hash)` with the migration (research R2 linkage).
2. Four commit paths (`scanner.commitBlock`, `logscanner.commitLogRange`, `depositcommit.commitDepositUnit`,
   `confirmcommit.ConfirmDepositUnit`): add post-lock triple recheck — (a) no active non-terminal
   recovery row, (b) captured == current version (single-statement read: active seq else events MAX
   else 0), (c) existing verdicts/guards — beside existing verdicts. Loop level: never start a batch
   while an active row exists; capture version BEFORE reading batch inputs (binds inputs to the
   version). (a)/(b) mismatch refuses the whole batch with zero writes and zero progress, even on
   full content coincidence; recomputation starts a new batch, never re-labels old results.
   006's own transactions gate on captured seq+phase (enumerated paths only).
   Startup/loop gating readers (`scanner.loadProgress`/loop, `logscanner.loadLogState`/loop, serve
   health) read the recovery row the same way they read pause rows today.
3. 004 re-read conflict rule (`storedStatus != 'pending'`): 006's own re-reads exempt rows under the
   captured recovery version; ordinary 004 path unchanged (stopped during recovery anyway).
4. 005 candidate re-read (`neither pending nor confirmed`): orphaned rows route to ChainViewError-stop
   (behavior unchanged, now reachable — listed explicitly).
5. `deposit_pause.kind` / `deposit_pause_audit.action` CHECKs: unchanged (006 writes no stream pauses,
   no new audit actions — recovery audit lives in `reorg_recovery_events`).

## Re-canonicalization order (same old block canonical again)

Single `revive_observation`-adjacent block txn (lease lock first, V + phase rechecks): flip the
new-fork row `canonical=false` FIRST, then the old row `canonical=true`; each statement
individually satisfies the partial unique (no deferral needed); the transient zero-canonical state
is invisible inside the txn. Only after the block flip may `revive_observation` run for that height
(same or later txn — the revive recheck reads the flip). Downstream validity across the flip:
in-txn invisible + annotated queries outside (R11).

## Observation field semantics (which columns mean what, per status)

| status | confirm_* basis cols | orphaned_at / orphan_recovery_id / orphan_reason | Reader rule |
|--------|---------------------|--------------------------------------------------|-------------|
| pending (never confirmed) | all NULL | all NULL | candidate for 005 |
| confirmed | full basis (effective) | all NULL | effective confirmation; traceable |
| orphaned (was pending) | all NULL (never had basis) | all set (evidence) | NOT a candidate; history visible |
| orphaned (was confirmed) | retained STALE basis (history only) | all set (evidence) | status governs: confirm_* MUST NOT be read as effective |
| pending (revived) | retained OLD basis (history only) until reconfirmed | retained (history of the orphan episode) | candidate for 005; on reconfirm 005 overwrites confirm_* with the NEW basis (old basis survives only in transition log) |

"005 数据含义不动" is not an excuse here: the rule is explicit — row confirm_* columns are
*current-or-last* basis, historical truth lives in `deposit_observation_transitions.basis_snapshot`;
readers MUST key effectiveness off `status`, never off column non-NULLness.

## Worked example: Confirmed → Orphaned → Pending → Confirmed → Orphaned

Identity `(c, bh_old, tx, li)`, recovery R1 (fork F1), recovery R2 (fork F2):

1. `status=confirmed`, basis=B1, orphan cols NULL. No transition rows.
2. R1 `invalidate_observations`: `status=orphaned`, basis=B1 retained, orphaned_at=t2,
   orphan_recovery_id=R1. Transition row `(confirmed→orphaned, R1, snapshot=B1)`.
3. Old block re-canonicalized; R1 `revive_observation`: `status=pending`, basis=B1 retained
   (stale), orphan cols retained (episode history). Transition row `(orphaned→pending, R1,
   snapshot=re-verify refs)`.
4. 005 reconfirms (UNCHANGED path): `status=confirmed`, basis=B2 **overwrites** B1 in-row;
   B1 survives in step-2 transition row + step-3 snapshot. No 005 code change.
5. R2 fork hits the same height with `bh_new2`: new identity `(c, bh_new2, tx, li2?)` → separate row
   (different source, FR-08 new-observation rule); old row `(c, bh_old, ...)` → `status=orphaned`
   again (basis=B2 retained, orphan_recovery_id=R2). Transition row `(confirmed→orphaned, R2,
   snapshot=B2)`. Both rows coexist; default views never merge them (FR-18).

Repeat execution at any step: transition-log UNIQUE makes the second attempt a conflict → already
recorded, zero new state (idempotent by storage, not by memory).

## Invariants ↔ constraints

- Single canonical per height: partial UNIQUE (chain_id, number) WHERE canonical (replaces PK shape).
- Checkpoint always points at a persisted row: unchanged FK + exact guards + new rollback guards.
- One valid observation per source identity: PK + transition-log UNIQUE + no-duplicate revival rule.
- No observation without full batch atomicity: replay/revive txns keep write+frontier atomic.
- Pause/version mismatch ⇒ zero ordinary commits: stream pause rows + recovery-state gate (tested).
- First confirmation fact immutable: basis columns never cleared; transitions audited, never rewritten.

## Observability (OQ2 carriers — named, not deferred)

- State: active `reorg_recovery` row readable via existing status/diagnostic surface (phase, frontiers,
  bound tip, ancestor, policy_seq); events table for history.
- Metrics (extend existing `log_*`/indexer metric families, same naming shape): recovery active flag,
  current depth vs bound, frontier lag per stream, orphaned count, revival count, reconcile-required flag,
  evidence-insufficient wait counters. No new metric system.
- Diagnostics: detail/evidence columns carry heights, hashes, ranges, versions (no secrets, no raw RPC
  dumps — same redaction boundary as 002/003/004).
- Queries: validity annotation derived at read time from the active row (R11); contracts/observability.md
  lists the exact annotated fields.

## Indexes (non-speculative: each serves a named access path above)

- `reorg_recovery_events (chain_id, recovery_id, event_seq)` UNIQUE (lifecycle trace).
- `deposit_observation_transitions` UNIQUE (identity + from + to + recovery_id) (idempotent append).
- `chain_blocks (chain_id, number) WHERE canonical` partial UNIQUE (I1).
- No other new indexes: checkpoint/pause single-row point reads are PK-covered; frontier scans reuse
  existing `(chain_id, block_number)` indexes; policy max-seq is a PK-ordered aggregate over tiny rows.
