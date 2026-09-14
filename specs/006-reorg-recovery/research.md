# Research: 006 Reorg Recovery

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14 | **Spec**: [spec.md](spec.md) (7ace1fe, Q1–Q4 integrated, zero markers)

Phase 0 output. Every technical unknown from the plan's Technical Context is resolved below
except deliberate plan-level parameters (retry/backoff values, batch sizes) which are bounded
but not fixed here. OQ1–OQ3 and FR-24 are resolved, not renamed.

Evidence base (read-only, no code modified):
- Upstream designs: `specs/002-chain-indexer/data-model.md`, `specs/003-event-indexing/data-model.md`,
  `specs/004-deposit-detection/data-model.md` (393 lines incl. auth/merge protocols),
  `specs/005-confirmation-tracking/data-model.md` (228 lines incl. submit/switch protocols).
- Code mechanics (two parallel explore agents, verbatim SQL collected):
  `internal/indexer/lease.go`, `scanner.go` (commitBlock/commitPause + shared write consts),
  `logscanner.go` (commitLogRange/commitLogPause + canonicalBlockHashSQL),
  `coordinator.go` (RunPair/Trio/Quatro + checkLost), `depositcommit.go` (commitDepositUnit),
  `depositscanner.go` (pause insert/merge/delete + audit), `depositauth.go` (auth switch tx),
  `confirmcommit.go` (ConfirmDepositUnit triple-version recheck), `confirmscan.go` (candidate SQL),
  `confirmauth.go` (policy switch), `migrations/000002`–`000005`.

## R1 — Coordination: reuse `indexer_lease`, add recovery-seq fencing (resolves OQ1, OQ3, FR-24-lock)

- **Decision**: All 006 transactions take `indexer_lease` row lock first (`lockCoordSQL ... FOR UPDATE`),
  exactly like 002/003/004/005. No new lock order, no second lock, no deadlock ring.
  Fencing uses a new monotonic `reorg_recovery.recovery_seq` per chain, carried explicitly:
  006 transactions capture `(recovery_id, recovery_seq, phase)` outside the lock and recheck
  equality plus phase-allowance after the lock; ordinary 002/003/004/005 commit paths gain one
  additional post-lock recheck — "no active non-terminal recovery row" — next to their existing
  `leaseVerdictSQL` + stream-pause checks. This ordinary-path check is the primary stop (the recovery
  row is the sole recovery authority, research R9); the stream pause rows keep working unchanged as
  the independent second signal.
- **Rationale**: The existing single-lock discipline is proven (002 §concurrency argument, 003/004/005
  isomorphic protocols). A second coordination lock would create a lock-order ring with the four
  existing writers. The recovery-seq gives FR-14's "persistent version constraint" a concrete carrier.
  Ordinary paths need the check as the **primary stop** (the recovery row is the sole recovery
  authority per R9): it covers them even if stream pause rows are tampered with (manual SQL delete
  bypassing all writers), and on restart-after-tamper the recovery row — never manually cleared,
  only 006 release transactions delete it — still refuses. Stream pause rows remain the independent
  second signal with their owners and merge semantics untouched.
- **Alternatives considered**:
  - *Pause-rows only, no ordinary-path change*: rejected — leaves the tamper bypass open, and the
    task instruction explicitly forbids新增恢复入口却遗漏原写路径可绕过 ("no new entry while missing
    bypassable old paths").
  - *Rotate `fencing_token` at establish (forced failover)*: rejected — token rotation is the
    instance-failover mechanism; overloading it conflates "instance lost" with "chain forked",
    orphans legitimate heartbeats, and still leaves the same single lock-holder residual (see R7).
  - *New separate coordination table/lock*: rejected — lock-order ring risk with four existing writers.
- **Atomicity proof (why check-then-act is closed, not "one more SELECT")**: the recovery-state
  recheck lives **inside** each commit transaction **after** `lockCoordSQL ... FOR UPDATE` — the same
  lease row `establish` must acquire. Check and write are therefore ordered by the lock, not by time:
  - *I-A, ordinary first*: O holds lock → post-lock checks see no recovery row → writes → COMMIT →
    releases; E then acquires, inserts recovery row. O's result is a legal pre-pause commit
    (pause-effective point = establish COMMIT; nothing committed before it is revoked, only swept
    for affectedness under full audit if inside the fork range).
  - *I-B, establish first*: E commits recovery row; O acquires lock → post-lock recheck sees the
    active row → refuses, zero writes.
  - *Suspended-after-check worker*: it holds the lock while suspended, so establish blocks; on resume
    it commits pre-pause (case I-A, legal) — it can never commit post-pause because any commit after
    establish's COMMIT implies it acquired the lock after, hence saw the row.
  - *The "row absent at check, establish inserts before my write" gap*: impossible — between O's
    post-lock check and O's write there is no lock release; establish cannot interleave.
  - *Lease fields' division of labor*: `owner_id`+`fencing_token` reject the previous holder's **next**
    transaction after a takeover (in-transaction holder unaffected — same residual as above, covered
    by sweep); `expires_at` bounds staleness of a holder that stops heartbeating (DB `now()` clock only).
    Recovery-seq rejects post-establish stale **views**; lease rejects post-takeover stale **holders**.
    Neither alone suffices; together with the shared lock they close the window.
- **Current-version read, capture point, commit triple (authoritative)**: current version = active
  row's seq if present, else events-stream MAX for the chain, else 0 — one statement, read committed.
  Ordinary loops MUST NOT start a batch while an active row exists; otherwise capture version FIRST,
  then read batch inputs. Commit rechecks post-lock: (a) no active non-terminal row, (b) captured ==
  current, (c) all existing guards. (a)/(b) mismatch refuses the whole batch with zero writes and zero
  progress; recomputation starts a new batch. Establish writes row+event atomically in one txn
  (rollback burns nothing); only 006 transactions may append events; events are never deleted or
  archived (per-chain volume is tens of rows per recovery), so cleanup cannot undermine version
  authority.

## R2 — `chain_blocks` must hold two rows per height: PK rework (resolves FR-24-storage)

- **Decision**: Migration 000006 changes `chain_blocks` PK from `(chain_id, number)` to
  `(chain_id, number, hash)`; keeps `UNIQUE (chain_id, number, hash)` (now implied, retained for the
  FK target), keeps `UNIQUE (chain_id, hash)`; adds partial `UNIQUE (chain_id, number) WHERE canonical`
  enforcing at most one canonical row per height; `indexer_checkpoint`'s triple FK keeps working
  (target uniqueness preserved). 002's scanner insert path is an **affected existing interface**:
  `INSERT ... ON CONFLICT (chain_id, number) DO NOTHING` + post-insert hash compare must become
  height-sibling-aware (same height + different hash + not part of an active 006 recovery = fork
  evidence → pause path; during active recovery the 006 transactions own those rows).
- **Rationale**: FR-05 requires old fork rows retained (audit) while new fork blocks are stored at the
  same heights — physically impossible under PK `(chain_id, number)`. The partial-unique preserves
  invariant I1 (at most one canonical per height) at the storage layer, same guarantee shape as today.
- **Alternatives considered**:
  - *UPDATE old row's hash in place*: rejected — destroys fork audit evidence, violates FR-05/FR-07/FR-21.
  - *Separate fork-archive table, keep PK*: rejected — splits the canonical-read path (every
    `canonicalBlockHashSQL` consumer would need UNION logic), doubles the identity model, and breaks
    the checkpoint FK story; more code, more risk, same guarantees.
  - *Do nothing, store only new fork*: rejected — same evidence destruction as in-place update.
- **Full linkage (every touchpoint of the PK change)**:
  - *FK*: `indexer_checkpoint (chain_id, height, block_hash)` → `UNIQUE (chain_id, number, hash)` —
    retained verbatim, still unique, still enforced. No FK rewrite.
  - *Upsert target*: `scanner.go:1025 insertBlockSQL ON CONFLICT (chain_id, number)` → retarget to
    `(chain_id, number, hash)`: same number+hash = idempotent rescan (DO NOTHING, correct);
    same number + different hash = new sibling row (no conflict) → sibling-detection logic decides
    fork-evidence pause (ordinary path) vs 006-owned row (recovery path). This is the one 002 code
    change the migration forces.
  - *Canonical readers* (unchanged SQL, new determinism proof): `logscanner.go:1386
    canonicalBlockHashSQL`, 005 `readConfirmationTipSQL` (canonical ORDER BY number DESC LIMIT 1),
    005 candidate reference check, 004 `readjudicateCanonicalRange` — all filter `canonical` (+ height).
    Uniqueness: partial UNIQUE guarantees ≤1 canonical row per height; existence: only asserted after
    sweep+replay completion (release gate), never assumed mid-recovery. Height+hash binding is now
    **stronger** than before (PK binds them; previously only the UNIQUE did).
  - *Re-canonicalization order* (same old block canonical again): single txn —
    flip new-fork row `canonical=false` FIRST, then old row `canonical=true`; each statement
    individually satisfies the partial unique (no deferral needed); the transient zero-canonical state
    is invisible inside the txn. Downstream validity across the flip: in-txn invisible + annotated
    queries outside (R11), so no consumer observes the intermediate state.
  - *Intermediate states generally*: old-canonical-revoked / new-canonical-effective / downstream-
    effective are three separate commits (invalidate → replay → release); between them the recovery
    row + annotated queries prevent any consumer from treating the mixture as complete.

## R3 — Depth formula, integer domain, boundary, safe arithmetic (resolves FR-24-math, Q1 detail)

- **Decision**: `depth = bound_old_tip - ancestor` as `int64`, with a pre-assertion
  `ancestor <= bound_old_tip` (violation = internal error, refuse). Both inputs are `BIGINT CHECK (>= 0)`
  columns, so depth range is `[0, MaxInt64]`; subtraction cannot underflow given the assertion and
  cannot overflow (result ≤ bound_old_tip ≤ MaxInt64). Boundary is closed: `depth <= max_depth`
  allows auto-recovery. `max_depth` is `BIGINT CHECK (> 0)`, required, no default; missing/zero/
  negative/non-integer/out-of-representation refuses startup (same shape as 005 threshold Q1 rule).
  No business cap. **Saturation/clamping arithmetic MUST NOT be used as the audit basis** — the
  compared value is always the exact difference; any defensive guard that triggers is an internal
  error that refuses, never a number written to audit (mirrors 005's NUMERIC exactness stance).
- **Rationale**: Exact, checkable, no new concepts; reuses the proven "refuse, don't coerce" config
  precedent. The closed-boundary reading is the Q1 decision.
- **Alternatives considered**:
  - *Saturating subtraction with clamped audit value*: rejected per task instruction — a clamped value
    is not the true depth and would certify a false bound.
  - *uint64 domain*: rejected — heights are int64 columns; conversion adds edge cases for zero benefit.

## R4 — Depth config versioning and change protocol (resolves Q1-change, FR-24-config)

- **Decision**: New `reorg_policy_history(chain_id, policy_seq, max_depth, prev_seq, operator, reason,
  request_id, expected_old_seq, created_at)` mirroring `confirmation_policy_history` exactly
  (PK, self-FK chain, UNIQUE request_id, bootstrap partial-unique, max-seq = effective).
  Startup compares env max_depth against current max row: mismatch (incl. no row + env set) follows the
  005 drift rule — loud refuse/stop, never silent follow. Change path = authorized privileged
  transaction (DB operator, same shape as `confirmauth.go`/`depositauth.go` auth txns: expected_old_seq
  gate, new≠old, single-row INSERT, request_id idempotency, failure atomic). In-flight recoveries bind
  the captured `policy_seq`; a change never widens an in-flight recovery's authority (guard compares
  bound seq). Raising the limit never releases a manual-reconciliation pause (release only via Q2b path).
- **Rationale**: Reuses the reviewed 004/005 authorization precedent instead of inventing a third config
  mechanism; satisfies "受控授权、原子生效并审计" with existing shapes (operator/reason/request_id audit).
- **Alternatives considered**:
  - *Env-only, restart-to-change*: rejected — violates the Q1 "controlled authorized change" decision
    (no audit, no atomicity, no in-flight binding).
  - *Reuse `confirmation_policy_history` rows*: rejected — orthogonal version domains (005 data-model
    §version-trichotomy forbids mixing policy seq with other versions).

## R5 — Ancestor search: read-only suffix walk (resolves FR-24-algorithm, OQ1-drive)

- **Decision**: Search is **read-only** (no transaction, no locks): from `bound_old_tip` walk down while
  `depth <= max_depth`: at each height compare local canonical `(number, hash)` against chain RPC
  `(number, hash)`; first height with equality + locally continuous parent linkage on both sides is the
  ancestor candidate; then verify the new chain's parent continuity over the walked suffix
  (each new block's parent_hash == previous new block's hash, terminating at ancestor hash).
  Evidence classes reuse the 003 failure taxonomy: transport/timeout/rate-limit/parse = retryable
  (bounded backoff, parameters left to implementation within stated bounds); explicit
  contradictory/insufficient responses = evidence-insufficient → wait, never invalidate.
  Local history exhausted (no local row) or below scan-start or beyond max_depth = unrecoverable →
  persist reconcile signal, stay paused. The bound tip is fixed at establish; the live tip is tracked
  separately and keeps growing — completion is judged against the bound tip + swept range, never
  "catch the live head" (FR-19).
- **Rationale**: Read-only search cannot corrupt state and needs no lock (long RPC-bound transactions
  must never hold the coordination lock — the 002 "zero RPC in txn" rule). Suffix walk is the only
  algorithm consistent with the Q1 depth definition.
- **Alternatives considered**:
  - *Binary search on hash equality*: rejected — hash equality is not monotonic over a fork (a later
    height can match while an earlier doesn't only in bizarre multi-fork cases, but the guarantee
    needed is a continuous suffix; linear walk also yields the full evidence trail for audit).
  - *Search inside the establish transaction*: rejected — holds the global coordination lock across
    bounded-but-many RPC calls, stalling all writers; violates the short-txn discipline.

## R6 — Transaction list and per-txn shape (resolves FR-24-txn, OQ3-serialization)

- **Decision**: Every 006 transaction follows the 5-step shape (BEGIN → ensure lease → FOR UPDATE →
  independent post-lock rechecks → writes with `RowsAffected` checks → COMMIT; `SET LOCAL
  statement_timeout='5s'`). Full per-transaction read/write/lock/version tables go in data-model.md;
  the list: `establish` (recovery row + indexer_pause row + policy bind), `invalidate_blocks`
  (canonical=false flip over swept range), `invalidate_observations` (→orphaned + evidence),
  `rollback_block_checkpoint`, `rollback_log_checkpoint`, `rollback_deposit_checkpoint`,
  `replay_range` (per-stream, idempotent), `revive_observation` (orphaned→pending, Q4 rules),
  `complete_reverify` (auto path), `auth_enter_repair` + `auth_release` (manual Q2b path),
  programmatic reconcile writes (evidence/progress/disposition under seq+permission).
  No single mega-transaction: phase rows make each batch's commit the resume point; intermediate
  states (active recovery row + pause rows) refuse ordinary commits and force queries into
  annotated mode, so no misjudgment window exists.
- **Rationale**: Same serialization point + same recheck idiom as four predecessors → one mental model,
  no lock-order analysis needed beyond "lease first, always".
- **Alternatives considered**:
  - *One transaction for the whole recovery*: rejected per task instruction and practically — replay
    spans unbounded ranges and RPC reads; a mega-txn would hold the global lock indefinitely and
    exceed statement timeouts.
  - *Separate 006 coordination lock*: rejected (R1).

## R7 — Residual classes, sweep isolation, late writes (resolves FR-14/FR-20 honestly)

- **Decision**: Three classes, three treatments — stated exactly so "residual" can never be read as
  "post-pause submits are possible":
  - *Class 1 — legal pre-pause commits (including the single lock-holder across establish)*: committed
    before pause-effective point (= establish COMMIT). Valid when written. Handled by **controlled
    cleanup**: invalidation sweeps orphan affected rows with full audit. This is the ONLY residual,
    and it is cleanup, not a violation.
  - *Class 2 — unfinished recovery cleanup*: rows swept but replay incomplete. Handled by design:
    frontiers + phase gate completion; queries annotate provisional (R11).
  - *Class 3 — post-pause submits by old workers*: **impossible by construction**, not "compensated by
    sweep". Any commit after establish's COMMIT acquired the lease after it, hence passed through the
    post-lock recovery recheck and was refused. Recording it as a "risk" would be false; it is a
    refused path, tested in V6/V11.
  Coverage rule: every sweep computes its range from **live tip at sweep execution**
  (`[ancestor+1, max(persisted_tip_at_sweep)]`), never from establish-cached values. Progress =
  phase + per-stream frontiers persisted per batch; termination = swept coverage complete AND FR-19
  re-verification passes — unprocessed ⇒ no release, by gate, not by convention. Late writes:
  ordinary writes after sweep start are refused (Class 3 proof above); 006-owned writes advance
  frontiers atomically in the same txn, so they are never "late". Sweeps never skip locked records:
  they are range UPDATEs executed **under the lease lock**, not `SKIP LOCKED` scans — no record can
  be concurrently written while a sweep runs.
- **Rationale**: Lease serialization makes "at most one Class-1 straggler" provable; live-range sweeps
  make it covered; the completion gate makes "unfinished ⇒ held" structural. Claiming zero pre-pause
  stragglers would be false; claiming post-pause submits are possible would also be false.
- **Post-release version isolation (row deleted — version gate persists; content match is NOT
  exculpatory)**: deletion removes the row, never the version. Current version is always readable in
  one statement: active row's seq if a row exists, else events-stream MAX for the chain (0 when no
  history). Ordinary batches MUST capture this version BEFORE reading batch inputs (loop level: never
  start a batch while an active row exists — this order binds inputs to the version), and the commit
  transaction MUST recheck post-lock all three: (a) no active non-terminal recovery row,
  (b) captured == current version, (c) existing checkpoint/config/canonical/policy guards. Version
  mismatch refuses the WHOLE old batch — zero progress, zero business writes — even if hashes,
  contents, and checkpoints coincide: Q4 re-canonicalization makes content coincidence the expected
  case, and content re-verification, though still required, cannot substitute version isolation.
  Recomputation MUST start a NEW batch (re-capture, re-read); re-labeling old results with a new
  version is forbidden.
  - *Demanded interleaving*: W captures v=5 (no active row) → establish R6 writes row seq=6 (+event,
    same txn) → R6 completes, row deleted (events MAX now 6) → W returns and commits with fully
    matching content: post-lock recheck reads current=6 ≠ 5 → REFUSED, zero writes. The verdict never
    consults content.
  - *Preserved legal order*: O holds the lease, passes all three checks (no row, v==v, guards),
    commits, releases; establish then creates v+1. Pre-pause legal commits are never misjudged —
    the gate fires only on actual staleness (a completed round between capture and commit).
  - *Per-path second layer (still required, no longer sufficient alone)*: 002 parent-hash exact guard,
    003/004 canonical re-adjudication + coverage re-proof, 005 tip/policy rechecks. They now decide
    only same-version commits: the capture-during-recovery residual (loop gating forbids starting it;
    if it happens anyway, content guards judge against true current canonical state — match means
    genuinely still-valid derivation, mismatch refuses) and I-A legal commits.
  - *Round-1 executor after round-2 establish*: captured R1 seq ≠ current (events MAX ≥ R2 seq) →
    refused. Instance versions cannot collide: monotonic, never reset, deletes cannot resurrect them
    (Table 1 generation rule).
- **Alternatives considered**:
  - *Abort/kill in-flight ordinary txns at establish*: rejected — PostgreSQL has no safe "cancel the
    other guy's txn and be sure" primitive from app code without superuser `pg_cancel_backend`, which
    the service role must not hold; and cancellation is itself racy.

## R8 — Orphaned data model and revival vs existing predicates (resolves FR-24-model)

- **Decision**: Widen `deposit_observations_status_check` to `('pending','confirmed','orphaned')`;
  rewrite `deposit_observations_confirmation_consistency` as a 3-state predicate (pending ⟺ no basis
  and no orphan evidence; confirmed ⟹ full basis; orphaned ⟹ orphan evidence NOT NULL, basis retained
  untouched). Add columns `orphaned_at TIMESTAMPTZ NULL`, `orphan_recovery_id TEXT NULL`,
  `orphan_reason TEXT NULL` (NULL unless status='orphaned'). Add append-only
  `deposit_observation_transitions(chain_id, block_hash, tx_hash, log_index, from_status, to_status,
  recovery_id, basis_snapshot, at)` carrying every Orphaned→* and *→Orphaned step for repeated cycles.
  Revival (`revive_observation`) sets status back to `'pending'` — this automatically re-includes the row
  in `deposit_observations_pending_height_idx` and `readConfirmationCandidatesSQL` with zero index
  changes. Required existing-predicate updates (guarded, enumerated): 004's re-read
  `storedStatus != 'pending'` conflict rule must exempt rows under an active 006 recovery version
  (ordinary 004 path is stopped then anyway; the exemption lives in 006's own re-read, ordinary path
  keeps refusing); 005's candidate re-read `neither pending nor confirmed` must route orphaned rows
  to ChainViewError-stop (unchanged behavior, now reachable — explicitly listed, not silently kept).
- **Rationale**: Basis columns are never cleared (005's "never rewrite first confirmation fact" extends
  naturally); the transition log (not row mutation) carries repeated-cycle history, mirroring
  `deposit_pause_audit` precedent. Field-effectiveness rule (exact, no reader guesswork): for
  `status='confirmed'`, in-row confirm_* IS the current effective basis and history lives in the
  transition log; for `status='orphaned'` there is NO current effective confirmation — retained
  confirm_* are history evidence only; for revived `status='pending'` there is likewise no current
  basis and old values MUST NOT enter the new confirmation decision (005 recomputes from tip);
  on re-confirmation 005 overwrites confirm_* with the NEW basis while the old survives exclusively
  in transition rows. (The earlier summary "non-pending ⇒ history" was imprecise for Confirmed and
  is superseded by this paragraph; Q4 unchanged.)
- **Non-blocking proof (bulk Orphaned history vs live 005 confirmation)**: three cases —
  (a) *Selection*: `readConfirmationCandidatesSQL` filters `status='pending'` (`confirmscan.go`),
  so retained historical Orphaned rows are never selected — no per-tick collision possible.
  (b) *Selected-then-orphaned*: a Pending row selected pre-recovery and orphaned before its commit
  fails the commit-time re-read (`neither pending nor confirmed` → ChainViewError) and halts that
  tick once, by spec (stop, not skip); next tick the row is excluded by the `status='pending'`
  filter, so the stall is exactly one tick and never permanent.
  (c) *Genuine chain-view anomaly*: same halt path as (b); unblocked by recovery completion or
  reconciliation, never by skipping (no spec change — skipping anomalies is forbidden).
  `countConfirmationPendingSQL` likewise counts pendings only. Therefore post-release 005 processes
  new Pendings indefinitely alongside arbitrary Orphaned history with zero structural stall. Revival-to-pending reuses all existing downstream readers unchanged.
- **Alternatives considered**:
  - *Null the basis columns on orphan*: rejected — destroys the "旧确认依据保留为历史证据" requirement
    and the 006→005 re-verification input.
  - *Delete orphaned rows, re-insert on revival*: rejected — breaks I1 audit continuity and the
    "不隐藏 Orphaned 历史" query rule.

## R9 — Pause carrier: recovery row is the sole recovery authority; 006 writes NO pause table (CORRECTED)

- **Decision**: `establish` writes **only** the `reorg_recovery` row (+ terminal-bound event row). It does
  NOT write `indexer_pause`, `log_pause`, or `deposit_pause`. Ordinary commit paths stop via the
  recovery-state recheck (the R1 check, now the gate rather than a "backstop"): active non-terminal
  recovery row → refuse, alongside their unchanged stream-pause checks. 006's own transactions gate
  on captured `(recovery_id, recovery_seq)` + phase allowance — there is **no pause to bypass and no
  bypass flag any caller can pass**; only the enumerated 006 transaction functions carry a captured
  seq, so ordinary callers cannot obtain "006 mode". `complete_reverify` / `auth_release` DELETE only
  the recovery row (+ terminal event); they never touch stream pause rows. A recovery completing while
  a stream pause exists releases the recovery cause only — ordinary processing stays stopped on the
  stream pause, and the surviving pause is recorded in the terminal event (evidence, not a block).
- **Rationale**: `indexer_pause` is a single row with first-wins `ON CONFLICT DO NOTHING`
  (`scanner.go:1045` sole writer; `log_pause`/`deposit_pause` same shape). It cannot hold two
  concurrent causes: a 006 write would either lose to (or destroy evidence of) the stream's own pause,
  and a 006 release (`DELETE WHERE chain_id`) could delete a pause the stream still needs — exactly
  the overwrite/clear/ignore hazards named in the review. Separate tables per cause coexist without
  collision; the recovery row carries strictly more state (phase, frontiers, bound tip, policy) than a
  pause row could. Stream pauses keep their owners, merge semantics (`deposit_pause` revision merge),
  and release paths untouched.
- **Alternatives considered**:
  - *006 writes `indexer_pause` with ownership bypass (previous revision)*: REJECTED on re-review —
    fails cause coexistence (second cause lost to first-wins), fails safe release (DELETE by chain_id
    cannot distinguish owners without fragile detail-matching), fails "independent causes survive".
  - *Multi-row pause table (add cause column, change PK)*: rejected — rewrites three streams' pause
    contracts (owners, merges, audits, startup readers in scanner/logscanner/serve) for zero gain over
    the recovery row, which is needed anyway for phase/frontiers.
  - *New `reorg_pause` table checked by all writers*: rejected (as before) — the recovery row already
    is that table; a second one doubles reads per commit for nothing.

## R10 — Checkpoint rollback statements (resolves FR-24-progress)

- **Decision**: New guarded statements per stream, e.g.
  `UPDATE deposit_checkpoint SET next_block=$to WHERE chain_id=$1 AND start_block=$S AND config_hash=$H
  AND next_block=$from` (+ captured recovery-seq match in the 006 txn's recheck block),
  with `$to = max(ancestor+1, $S)` (never below start — respects `CHECK (next_block >= start_block)`,
  no constraint change). Same shape for `log_checkpoint` and `indexer_checkpoint` (the latter's triple
  FK still resolves: ancestor row exists and stays canonical). Rollback floor when ancestor predates
  scan start = the stream's own start (spec US3-2). Empty-log ranges replay as legitimately-empty
  intervals (progress advances, zero observations) — no gap markers.
- **Rationale**: Exact-guard rollback is the mirror image of the exact-guard advance; floors preserve
  every existing CHECK without DDL.
- **Alternatives considered**:
  - *Delete + re-insert checkpoint rows*: rejected — breaks the both-or-neither integrity rule with
    history tables and loses the audit trail of where progress was.

## R11 — Query validity expression (resolves FR-24-query)

- **Decision**: Readers (status views, deposit/confirmation queries) join the active `reorg_recovery` row:
  no active row → current answers as today; active row → every affected-range answer carries
  `(recovery_state, validity)` where validity ∈ {valid-unaffected, provisional-replaying,
  unknown-paused}; mixed views are never labeled complete; with no trusted data the reader returns
  explicit unavailable/unknown (Q3 decision), never "always available". Orphaned history is never
  hidden; default views never merge distinct source identities by tx_hash (FR-18).
- **Rationale**: Validity is derived from durable state at read time, not from a cache flag — same
  "re-read, don't trust memory" discipline as the write paths.
- **Alternatives considered**:
  - *Block all queries during recovery*: rejected — contradicts the Q3 decision and would blind operators
    exactly when they need visibility.

## R12 — Downstream handoff contract shape (resolves FR-24-downstream)

- **Decision**: `contracts/downstream.md` specifies check obligations, not behavior: before any
  broadcast/sign/nonce-allocate/confirm, the downstream stage must observe (a) no `indexer_pause` row,
  (b) no active `reorg_recovery` row (or active row in released-terminal phase), (c) its own stage
  gates. In-flight unknowns (requests emitted before pause): retain unknown, keep querying, record
  evidence; never re-pay on presumed failure; never claim a DB check recalls an emitted RPC.
  007–011 create no tables/services in this feature.
- **Rationale**: Turns the spec's handoff prose into checkable preconditions implementable entirely
  inside 007–011 plans.

## R13 — Migration compatibility, privilege, failure (resolves FR-24-migrate)

- **Decision**: Single goose migration `000006_reorg_recovery.sql`, fully transactional (PostgreSQL DDL
  is transactional): pre-asserts (zero non-pending/confirmed observations — extends 005's guard idiom;
  single canonical per height — true by PK today, asserted anyway); creates new tables + widens CHECKs +
  reworks `chain_blocks` PK (using `USING` index rebuild, no data loss; existing rows stay canonical).
  Privilege: same migrate role as 000002–000005 (no new role). Failure: statement failure rolls back
  the whole migration (goose runs one txn); no partial state; re-run safe. 005 rows/columns untouched
  in meaning (only the two CHECK rewrites + additive nullable columns + new tables/indexes).
- **Rationale**: Same migration discipline as the five predecessors; additive + assertion-guarded.
- **Order, locks, outage boundary (complete)**:
  - *Preconditions asserted first* (same txn, before DDL): single canonical per height (true by old PK,
    asserted anyway — extends the 000005 guard idiom); zero rows outside `{pending,confirmed}` in
    `deposit_observations` (000005 already guaranteed; re-asserted).
  - *DDL order*: CREATE new unique index on `(chain_id, number, hash)` → DROP old PK → ADD new PK USING
    the index → ADD partial unique `(chain_id, number) WHERE canonical` → CHECK rewrites (validated,
    not `NOT VALID`, so existing rows are proven compliant) → new tables/indexes. One goose txn:
    any failure rolls back everything; re-run safe.
  - *Locks/downtime*: PK rebuild takes `ACCESS EXCLUSIVE` on `chain_blocks` — plan a maintenance
    window; ordinary writers must be stopped (they are: recovery-style pause or deploy freeze).
  - *Old binaries*: NOT runnable post-migration (`ON CONFLICT (chain_id, number)` target no longer
    exists; insert path needs the sibling-aware rewrite) — explicit outage boundary: deploy 006 code
    with the migration, no mixed-version fleet. Rollback = code + DB restore point together; live
    downgrade of the DB alone is unsupported and stated as such.

## R14 — Verification strategy levels (resolves FR-24-verify; design only, nothing executed)

- **Decision**: Three levels, mapped per scenario in quickstart.md: unit (depth math incl. MaxInt64
  boundary, closed-boundary, invalid configs; txn guard unit tests with fault-injected snapshots);
  integration with real PostgreSQL (each 006 transaction's recheck matrix, lease-loss, pause-tamper
  backstop, checkpoint floors, transition-log append, repeated Confirmed→Orphaned→Pending cycles);
  Anvil end-to-end (shallow/deep/unrecoverable forks, re-canonicalization, re-fork mid-recovery,
  tip growth during recovery, multi-executor races, old-worker delay, policy-switch races,
  independent-pause retention). quickstart.md documents environment, fault injection, assertions,
  and resume steps — none recorded as passed.
- **Rationale**: Mirrors the 002–005 testing tiers (constitution X/XI) and the mandated scenario list.

## Phase 0 unknowns — disposition

- Technical Context unknowns: none remain as NEEDS CLARIFICATION (spec has zero markers; Q1–Q4 decided).
  Bounded-but-unfixed parameters (backoff ‖ batch sizes ‖ poll intervals ‖ statement timeouts beyond the
  inherited 5s writeGuard): left to implementation with stated bounds (bounded, jittered, never tight-loop),
  reusing INDEX backoff parameters where the failure classes match (003/004 precedent).
- OQ1 → R1+R5 (lease reuse; read-only search needs no drive mechanism beyond existing loops).
- OQ2 → data-model.md observability section + contracts/observability.md (carriers named there).
- OQ3 → R1+R6 (lease-first serialization shared with all writers; recheck-after-lock closes the window).
- FR-24 → R1–R14 above (no "decide later" remainder).
