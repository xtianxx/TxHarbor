# Contract: 008 Reconciliation Pause Observation & Operator Release

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md)
(FR-08/FR-10/FR-11/FR-12/FR-14/FR-15; OC-6/OC-7 Round 2) | **Design**: research R4/R6/R7/R9/R11,
data-model Tables 2/5/6/7.

This contract fixes how 008-owned reconciliation pauses are **established, observed, scoped, and
released**, and how they coexist with 006 without ever touching 006 state. It creates no future
business tables and no 009–011 behavior.

## 1. Pause model

- A 008 pause is a durable **hold row** (`nonce_scope_holds`) scoped to exactly one
  `(chain_id, sender)` — never chain-wide, never global (OC-6: 限定具体 `(chain_id, sender)`).
- A scope is admission-held iff ≥1 `active` hold exists. Multiple holds (different causes or
  instances) coexist; each is independent.
- Holds block **new admissions only** (allocations in the held scope). They never rewrite existing
  bindings, never recycle/reassign nonces, and never "undo" an already-committed admission
  (in-flight-approved delivery is uncancellable; a hold after admission affects later actions and
  reconcile processing, not the durable fact).
- 006's recovery pause is **not** a 008 hold and is never read-as-writable: 008 only observes it
  and refuses admission while it is active. A 008 hold can exist while 006 is active; releasing
  one never affects the other.

## 2. Causes and establishment (automatic, evidence-linked)

| Cause | Established when (classification, data-model matrix) | Blocks |
|---|---|---|
| `unattributed_consumption` | chain consumed above the local frontier with mined evidence (`P > M+1`, `L > M+1`) | new admissions in scope |
| `unexplained_gap` | chain consumed above the local frontier, pending-only evidence (`P > M+1`, `L <= M+1`) | new admissions in scope |
| `chain_view_divergence` | contradictory or regressing view (`L > P`, or `pending` below the previous observation) | new admissions in scope |

Establishment is automatic from a persisted observation (`evidence_observation_id` NOT NULL) and
never deletes or mutates anything else. Establishment does not require operator action; **release
does** (Section 3). A repeated detection of the same cause while a hold is active appends
evidence (new observation) without duplicating the hold row; a released cause that is detected
again creates a **new** hold (a new instance with its own evidence), never a silent re-open.

Healthy bootstrap is explicitly **not** a hold: a first-time scope with pre-existing chain history
records `bootstrap_external_consumed` evidence for `[0, P)` and admits at `P` (R2). It is durable,
auditable, and never a silent adoption.

### 2.1 Observation basis, validity window, and commit-time re-verification

- **Basis and applicability**: a 008 observation is exactly the two reads
  `eth_getTransactionCount(sender, "latest")` / `eth_getTransactionCount(sender, "pending")`
  plus the head identity, taken for one `(chain_id, sender)` scope outside any DB transaction
  (R4). It is evidence for that scope only; it is not a chain lock, never applies to another
  scope, and by itself never mutates domain state.
- **Invalidation during the window (no stale reuse)**: the pre-tx observation is only valid for
  the tx it fronts. If, after it was taken, another allocation commits, 006 recovery establishes
  or pauses, or the registry changes, the observation is stale for that decision: the
  coordination-row `FOR UPDATE` lock (R1) plus the in-tx re-reads below make the outcome
  fail-closed rather than silently taken from the stale view (candidate selection is always
  recomputed from durable `nonce_bindings` / `nonce_scope_state` under the lock, R2; a concurrent
  same-scope allocation is absorbed by the recomputed `M` and, as a backstop, by
  `nonce_bindings_scope_nonce_uniq`). A stale observation MUST NEVER lower `reconciled_floor`,
  release a hold, reset/recycle/reassign an existing binding, or re-derive a candidate.
- **Commit-time re-verify checklist (row lock + re-read items)**: inside the locked tx every 008
  write re-reads (a) the three 006 pause rows (`indexer_pause`/`log_pause`/`deposit_pause`) and
  the active `reorg_recovery` row; (b) the registry row (`state`, `registry_seq`); (c)
  `nonce_bindings` by `intent_id` and the scope max `M`; (d) `nonce_scope_state`
  (`reconciled_floor`, `last_*`); (e) the scope's active `nonce_scope_holds`. Any item that
  differs from the pre-tx basis, and any 006/registry/hold gate hit, refuses with the cause
  recorded and zero writes (data-model transaction catalog).
- **Non-atomicity with the chain (stated limit)**: the DB lock serializes 008 writers (and, because
  006 shares the coordination row, 006 establish/pause/release) only. It does **not** and cannot
  prevent an external transaction from being mined or queued during the observation window; no
  DB lock claim is made against chain state. Such an external consumption is surfaced by the next
  observation as `unattributed_consumption` / `unexplained_gap` / `divergence` → refusal + hold +
  reconcile (Section 2), and is never silently merged or used to free the consumed nonce.

## 3. Release path (operator-only; 006 Q2b-style existing carrier)

Carrier: `txharbor nonce-admin <hold-release|binding-release|register|disable> …`, the same
controlled operator-subcommand family as `confirm-auth` / `withdrawal-authz` / `apikey-auth`
(flag parsing → `config.Load` → operator-connection tx; `--operation-id` **required** and minted
first via `nonce-admin mint`; exit codes 0/1/2; no new role, no admin UI, no HTTP write).

### 3.1 `hold-release`

```
txharbor nonce-admin hold-release \
  --operation-id O --hold-id H --chain-id N --sender 0x… \
  --observation-id OB --evidence "finding text" --operator OP --reason R
```

Authorization scope: `--chain-id` MUST equal the deployment chain; `--sender` MUST equal the
hold's scope; `--hold-id` names exactly one hold. No role is created; the trust root is DSN
possession (same as every privileged carrier).

Evidence standard (OC-6; FR-08; US5-7) — release succeeds only when **all** hold:

1. the cause has been accounted for: the operator supplies a finding plus an observation
   reference; the service re-reads that observation and requires it to belong to the scope;
2. re-verification passes **inside the locked tx** with a fresh pre-tx observation: the scope
   classifies `consistent` under the current matrix (no consumption still above the frontier, no
   divergence);
3. no unresolved conflicts remain in the scope: every binding is attributable (has an intent and
   a recorded state) and non-terminal items are explicitly left in place with their safe
   follow-up recorded (在途项 MAY 保留但 MUST 明确归属与安全后续);
4. the release clears **only the named hold row** (`active → released`, operator/reason/evidence/
   operation id recorded) and, for consumption/gap causes, advances the durable scope floor
   `reconciled_floor = GREATEST(reconciled_floor, observed pending)` so subsequent admissions
   resume above the reconciled consumption (explicit, audited — never a max-value shortcut);
5. an audit row (`outcome=applied`) commits in the same tx.

**Evidence version and re-verify point**: a release is valid only for the exact evidence version
re-verified at the in-tx lock point. That version set is read under the same coordination-row lock
and recorded on the hold: the named `hold_id` still `active`; the operator-supplied
`--observation-id` (which MUST belong to the scope) plus the fresh pre-tx observation recorded as
`release_observation_id`; the current `registry_seq` (config-change version) and registry `state`;
the current active-hold set for the scope; and the 006 recovery state observed at release time
(recorded as evidence only — 008 never writes it). If any of these differs from the version the
operator's evidence asserts, the release MUST be refused (`outcome=refused`, zero hold/floor
change) — it is never applied against the newer version. The 006 recovery-completion marker is a
read-only version input, not a release trigger: a change to it after the release is a separate
cause handled by its own gate and never retroactively rewrites or re-opens the applied release. An
already-applied release is never reinterpreted against later evidence — its recorded
`release_observation_id` / `evidence_observation_id` are immutable, and a re-detected cause
creates a **new** hold instance (Section 2), never a silent re-open. Operator instruction versions
follow the same rule: one `operation_id` = one audit row; a replay with the same op-input reports
the recorded `applied`/`refused`/`nop` outcome unchanged; a differing op-input is
`operation_conflict` with zero writes; a new attempt requires a newly minted operation id (never
re-minted for the same attempt).

Refusal is a first-class committed outcome: evidence missing, observation not found, re-read
failure, re-verification failure, fresh observation still classifying the cause, or insufficient
attribution → audit row `outcome=refused`, zero hold/floor change, exit 1. **修复完成 MUST NOT
自动等于解除** — no automatic path, no timer, no "looks consistent" auto-clear exists.

### 3.2 `binding-release`

Same carrier and evidence discipline per binding (`--binding-id`): only a non-terminal binding
may be disposed; the no-external-side-effect determination MUST rest on explicit evidence
(chain re-observation showing the nonce never mined, no pending transaction, plus the operator
finding) — a timeout or connection error alone is never evidence (US3-3). Result: terminal
`released` + event + audit; the nonce is never reused. In-flight items may also simply remain
in-flight (release is not forced to resolve them).

### 3.3 Registry operations (same carrier)

`register` (insert or re-enable with a bumped `registry_seq`) and `disable` (state flip with a
bumped `registry_seq`); both audit. Change effect: registry state gates **admission only** — an
existing intent's binding sender/nonce is a durable fact and is never changed by a later config
change (R9). Disabling a sender blocks new admissions for it and preserves all existing records.

### 3.4 Attempt semantics (all operator actions)

One attempt = one `operation_id` = at most one audit row (named UNIQUE). Same operation retried
concurrently → 23505 → rollback → re-read by operation id → equal op-input: report the recorded
outcome (including `refused`/`nop` — never upgraded); differ: `operation_conflict`, zero writes.
Uncertain COMMIT → retry with the **same** operation id and same op-input; an operation id is
never re-minted for the same attempt (mint-first rule, 007 carrier protocol).

## 4. Multi-cause coexistence and 006 isolation

- Releasing one hold MUST NOT clear any other active hold; the scope stays held while any
  remains; each release is scoped and audited independently.
- 008 never inserts/updates/deletes `reorg_recovery`, `reorg_recovery_events`, or pause rows; the
  release audit records the 006 state observed at release time as evidence but changes nothing
  there.
- 006 recovery completion does not clear a 008 hold; a 008 release does not clear the 006 pause.
  Both gates are independent and are re-checked at every admission (FR-14).
- The 006 downstream preconditions are re-read inside every admission tx: no `indexer_pause`,
  `log_pause`, or `deposit_pause` row for the chain, and no active `reorg_recovery` row
  (`contracts/downstream.md` 006). Any hit → refuse with the cause recorded; no queue-for-later.

## 5. Observability (evidence exposed, not invented)

- Structured logs through `logx` (redacted): `chain_id`, `sender`, `nonce`, `binding_id`,
  `intent_id`, `hold_id`, `cause`, observation classification, RPC endpoint class, retry class,
  `registry_seq`; never credentials or key material.
- Metrics (existing registry; names fixed here for the future implementation):
  `txharbor_nonce_allocations_total{result}`, `txharbor_nonce_replays_total`,
  `txharbor_nonce_holds_active{chain_id,sender}` (gauge), `txharbor_nonce_hold_events_total{cause,action}`,
  `txharbor_nonce_observations_total{classification}`, `txharbor_nonce_read_total{outcome}`,
  `txharbor_nonce_admin_total{action,outcome}`.
- The allocator does not affect service readiness (mirrors 006's observer). Rebuild-gate state is
  logged and counted; allocation refusals always carry a machine reason.
- Query surfaces: active holds and scope state are readable through the operator carrier
  (`nonce-admin status` style read-only action is permitted on the operator path — never a write,
  never on the read API), so an operator can list active holds, their causes, evidence, and
  scope floors without touching the database directly.

## 6. What this contract does NOT do

- No automatic release, no auto-recycle, no auto-reassign, no timeout-based judgment.
- No new role, approval, or admin UI; no HTTP writes.
- No modification of 006/007 semantics or rows; no redefinition of 007's authorization rules.
- No tasks/implementation here — this is design for the future Phase 2+.
