# Contracts: Gate Reads & Claim Verification (both directions) — 011 Withdrawal Execution Worker

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R5/R6/R10 | **Joint contract**: J2/J3/J4 (`docs/workflow-010-011-parallel.md:111-123`)

011 observes upstream gates **read-only** (006 pause/recovery, 007 request/grant/scope, 008 registry/binding)
and exposes its own claim row for 010 to verify. It never writes, clears, releases, or queues upstream state
(FR-11; 006 OC-6 "只读观察、不得清除或代为解除"). Refusal taxonomy lives in [persistence.md](persistence.md);
the 010 advance boundary in [lifecycle.md](lifecycle.md).

## 0. Common rule — read order, locks, clock, statement guards

Every **send-enabling** gate read happens inside the transaction whose decision it gates, in this fixed
order (J3; the shape follows 009's `internal/signer/gates.go:25-64,120-171`):

1. `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events IN SHARE MODE`
   (relation-level, so future `INSERT`s and manual-DBA releases are covered; conflicts with the 006 writers'
   `ROW EXCLUSIVE`).
2. claim row: `SELECT … FOR UPDATE` for claim/renew/takeover/step issue/converge (the 011 row is the
   coordination row); read-only verification paths use the same statement shape but do not write.
3. 006 pause/recovery snapshot: **one statement**, `(indexer_paused, log_paused, deposit_paused,
   recovery_id, recovery_phase, recovery_seq, events_max)` — no two statements can straddle a committing
   pause/recovery write.
4. 007 grant row `FOR SHARE`, then PB scope row `FOR SHARE` (same sequence; a revoke/re-supply committing
   before the share lock is observed, one racing it waits for this transaction).
5. 008 registry row `FOR SHARE` (admission) and/or binding/hold observation (advance).
6. own rows (steps/state/projection) last; external calls (010) are **outside** every transaction.

Clock: all validity/expiry comparisons use PostgreSQL `now()`, never the application clock. Every statement
in these transactions runs under a fixed statement-timeout guard (`SET LOCAL statement_timeout`, the 009
`renewLockGuard` shape). Any lock wait/timeout/deadlock is `gate_read_failed` → fail closed, no send.
Fact-only paths (projection consumption, reconcile observation, revision application) do not take gate-table
locks and make no send decision.

## 1. 006 recovery gate (pause rows + active recovery + version)

**Sources (read-only)**: `indexer_pause`, `log_pause`, `deposit_pause` (row existence = paused),
`reorg_recovery` (active instance; any persisted phase blocks), `reorg_recovery_events` (version when no
active row exists).

**Version semantics**: `current_version = active recovery_seq, else events MAX, else 0` (monotonic). The
observed version is recorded with every step issue (`execution_steps.recovery_version`) and consumed by 010's
own send gate; 011 does **not** re-derive 010's content basis and does not persist a "build version".

**Refusals** (each records the basis in the step/event evidence):

| Observation | Class | Effect |
|---|---|---|
| any of the three pause rows present | `recovery_paused` | do not issue/advance; unknown stays reconcile; no queue-for-later |
| active `reorg_recovery` row (any phase) | `recovery_active` | do not issue/advance |
| version changed between 011's observation and 010's build/send | 010 class `refused_basis` | step recorded refused; re-observe and retry under current gates (never a grace period) |
| statement error / indeterminate | `gate_read_failed` | fail closed; retry bounded |

**Never**: 011 issues only `SELECT` and the gate-table `SHARE` lock against 006 objects.

## 2. 007 request / grant / scope reads (admission + every advance)

**Request row (admission only)**: `SELECT caller_id, chain_id, asset, recipient, amount::text, status FROM
withdrawal_requests WHERE request_id = $1` — `status` must be `accepted`; ownership enforced by the caller
comparison (no existence leak).

**Grant row** (admission and every send-enabling step):

```sql
SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
FROM withdrawal_authorizations WHERE authorization_id = $1 FOR SHARE
```

Required: exists, `state='active'`, `expires_at IS NULL OR expires_at > now()` (DB clock), and field
equality with the request row's `caller_id/chain_id/asset/recipient/amount` (amount compared as integer
decimal, never float). Mismatch ⇒ `authorization_invalid`; revoked ⇒ `authorization_revoked`; expired ⇒
`authorization_expired`; absent ⇒ `authorization_invalid`; read failure ⇒ `gate_read_failed`.

**Scope row** (same `FOR SHARE` sequence):

```sql
SELECT authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
       fee_max_priority, allows_fee_replacement, authorization_version
FROM withdrawal_authorization_scopes WHERE authorization_id = $1 FOR SHARE
```

Required: present (else `authorization_unverifiable` — PB Q-B fail-closed, **not** a silent fresh-grant
reinterpretation), `intent_id` = the intent's identity, `request_id` = the path request, `sender` = the
intent's fixed sender. `authorization_version` must equal the value persisted on the intent at admission
(R10); a change means a re-supply occurred after admission ⇒ refuse and require the caller/operator to
re-establish basis (`authorization_changed`). Fee caps and `allows_fee_replacement` are **read but not
adjudicated** by 011 for replacement reuse: the fee-triple test needs the candidate transaction content,
which 010 constructs; 011 only enforces that `replace` is never advanced when the scope does not explicitly
permit that purpose (R10). Absent permission ⇒ `refused_basis` from 011, no call to 010.

**Lock semantics**: 007's revoke/re-supply takes the grant row `FOR UPDATE`; the share lock either observes
the committed change or delays it until this transaction commits (009 gates.md §2 precedent). Expiry is a
time boundary re-evaluated per step, never a persisted permit.

## 3. 008 registry / binding observation (five classes)

**Interface (011-side sketch; concrete adapter lands when 010/008 wiring exists)**:

```go
type BindingResult int
const (
    BindingMatches BindingResult = iota // exists, no holds, registry active
    BindingAbsent
    BindingConflict
    BindingPaused     // paused/hold-annotated/registry disabled
    BindingTerminal   // consumed/released; audit-only
    BindingReadFailed
)
type BindingReader interface { ReadBinding(ctx context.Context, intentID string) (BindingResult, error) }
```

**Admission**: the registry row must be `active` for `(chain_id, sender)`, read `FOR SHARE` in the admission
transaction (`ScopeShareSQL` shape from `internal/signer/gates.go:46`). A disabled/unregistered sender ⇒
`sender_unregistered`, zero intents.

**Advance**: only `BindingMatches` permits issuing a send-class step; `BindingPaused` ⇒ wait/reconcile (no
send); `BindingTerminal` ⇒ no send (audit-only); `BindingAbsent`/`BindingConflict`/`BindingReadFailed` ⇒
fail closed (`binding_absent`/`binding_conflict`/`binding_read_failed`). 011 never allocates, consumes,
modifies, or releases a binding (OC-3; 008 owns binding lifecycle); it records the observed class as evidence.
The adapter may reuse 008's exported `ReadProvider` exactly as 009's live adapter does
(`internal/signer/binding_live.go`), keeping the business import graph free of 008's RPC-bearing read package.

## 4. 011 → 010: the claim-row verification contract (joint; J2/J4)

**Object**: `execution_claims` row by `intent_id` — one row per intent, monotonic `lease_version`
([data-model.md](../data-model.md) Table 2).

**010's read (send gate, inside its decision transaction)**:

```sql
SELECT owner_id, lease_version, state, expires_at, last_progress_at
FROM execution_claims WHERE intent_id = $1
-- plus row-share semantics so 011's qualification writes are ordered against this read (see below)
```

**Required conditions for 010 to accept a presented qualification** `(owner_id, lease_version)`:
row exists; `state='active'`; `expires_at > now()` (DB clock); `lease_version` **equals** the caller-presented
version; `owner_id` equals the caller-presented owner. Any mismatch ⇒ refuse, record the class
(`claim_lost`/`claim_not_current`), no send. Revocation and natural expiry are distinguished in audit
evidence but have the same refusal effect — **no grace period, no TTL extension, no "was valid when the
decision started" exception** (J4; 010 Q2/Q3).

**Ordering obligation (joint, recorded not waived)**: for "失效先行则拒绝 / 门禁读先行则失效等待" (J4) to
hold under concurrency, 010's gate transaction MUST read the claim row with **row-share semantics** that
conflict with 011's qualification writes (`UPDATE execution_claims`), not a bare MVCC snapshot read: a
revocation committed before the gate read is visible and refuses; a revocation racing the gate read waits
until the gate transaction commits (the decision stands; a send that has entered the non-cancellable stage
is in-flight/unknown on response loss, never "nothing happened"). 011's side of the bargain: every
qualification change is a **single-row exact-version update** (`WHERE intent_id=$1 AND owner_id=$2 AND
lease_version=$3 …`), never a batch or a lock-free claim; 011 accepts whatever row-lock wait 010's gate
imposes. If 010's plan cannot provide the row-share read, the resulting gap MUST be reported for ruling
(FR-15/Q3) — 011 MUST NOT widen "legitimate in-flight" or add a grace window to compensate.

**011's own reads of its claim** (for its writes) are `SELECT … FOR UPDATE` in the same transaction as the
guarded update; there is no cached qualification, no in-memory permit, and no "renewed recently" shortcut.

**继承的 010 两项有限例外（register J4 原文；逐字承接，不重新裁决）**：

- G-010-1（自然到期残差）：有限例外（2026-09-17 新批准，仅自然到期）：最终检查后自然到期的发送残差允许记录并对账，不描述为合法在途，不覆盖其他残差，不批准可配置宽限期；排队/退避/重连/重试后 MUST 重估门禁；结果明确保留真实结果，仅不确定记 unknown。
- G-010-2 class (c)（失锁未感知窗口）：有限例外之二（2026-09-17 新批准，仅失锁未感知窗口）：探针 MUST 用持有保护锁的同一会话/同一事务，仅为缓解；检出失效 MUST 阻止尚可取消的发送；仅限当次发送，禁用绕过重估的透明重试；进入延迟为优化目标，不宣称窗口极短/极罕见；对账比较记录版本与变更证据，无法判定保序时保留不确定性；确认保护丢失且存在门禁失效后发送证据（或无法排除）时冻结该意图后续发送并转人工复核（链观察/查询/对账照常；正常接管非违规证据）；人工复核仅解除本残差的独立冻结原因（受控权限+证据+审计），不覆盖任何门禁；恢复发送前重验全部门禁，对账永不直接许可重发。此批准不覆盖其他残差，不代表实际验证通过。

## 5. Gate read matrix (what is read when)

| Phase | 006 gate tables | 007 grant `FOR SHARE` | 007 scope `FOR SHARE` | 008 registry | 008 binding/holds | claim fence | Locks held |
|---|---|---|---|---|---|---|---|
| Admission (T-admit) | yes (one statement) | yes | yes | yes (row `FOR SHARE`) | no (binding not yet created) | n/a | gate tables + registry/grant/scope rows |
| Claim / takeover (T-claim) | yes | no | no | no | no | `FOR UPDATE` on claim | gate tables + claim row |
| Renew (T-renew) | no | no | no | no | no | `FOR UPDATE` on claim | claim row only |
| Step issue (T-step-issue) | yes | yes | yes | no | yes | `FOR UPDATE` on claim | gate tables + claim row + grant/scope + 008 read |
| Step converge (T-step-converge) | no | no | no | no | no | `FOR UPDATE` on claim | claim row only |
| Reconcile / projection / revision (fact-only) | no | no | no | no | via 010 reader | no | own rows only |
| Execution view (display) | no | no | no | no | no | no | none (projection + own rows) |

Rationale: gates that can revoke authority (006/007/008) are re-verified on **every send-enabling step**, not
only at admission; fact-only paths never re-adjudicate authority (M2: chain-fact observation, state/projection
revision, and reconciliation bookkeeping are not new asset execution) and never send.

## 6. Invariants and verification hooks

- `internal/execution` contains **no** non-`SELECT` statement against 006/007/008 tables and imports no
  006/007/008 writer package (reviewable + quickstart V12 assertion).
- No gate observation is cached across steps; every step row records the observed basis
  (`recovery_version`, `outcome_class`, `evidence`).
- A refusal never claims "definitely not sent" when the outcome is unknown; unknown uses the
  `unknown`/`reconcile` vocabulary (persistence.md §4).
- The claim row is the only object 010 reads from 011, and it is 010-read-only (J2). No 010 write path to 011
  tables exists by construction.
