# Contracts: 011 → 010 Lifecycle Boundary — 011 Withdrawal Execution Worker

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R8/R9 | **Joint contract**: J1/J4/J5 (`docs/workflow-010-011-parallel.md:105-110,120-128`)

This contract defines the **consumer side** of the 011→010 boundary only. 010 owns transaction construction,
attempt/signing-request identity, signed-byte persistence, broadcast/replay/replacement, receipts, confirmation
tracking, the revision chain, and the unknown-recovery read. 011 supplies identity, fencing, actions, and
consumes classified outcomes. The concrete Go types land at wiring time (the 009 `BindingReader`/`binding_live.go`
pattern); the shapes below are the minimum 011 requires of 010's plan. If 010's plan cannot provide a requirement
here, the gap is reported for ruling — 011 does not widen its own authority to compensate.

## 1. Ownership split (J1, restated)

| Object | Owner | 011's relationship |
|---|---|---|
| `withdrawal_requests` (007) | 007 | read-only; `status='accepted'` is receipt, not authority |
| `payment_intents` | **011** | creates/owns; 010 references it, never creates |
| `nonce_bindings` (008) | 008 | consumed via 008's read contract; 011 never allocates/releases |
| `tx_attempts.attempt_id` + `signing_request_id` | **010** | referenced only; 011 never authors attempt content/identity |
| `withdrawal_authorizations` + scopes | 007-extension | read-only; 011 binds to intent, never supplies/revokes |
| `execution_claims` / steps / events / projection | **011** | 010 read-only verifies the claim row (gates.md §4) |

Direction: 011 drives (one claim holder at a time), 010 executes; 010 never polls 011's work queue and never
creates intents (010 spec Non-Goals; OC-1/OC-4).

## 2. Consumer-side interfaces (sketch; 010 plan owns the producer side)

```go
type AdvanceAction string
const (
    ActionFirstBroadcast AdvanceAction = "first_broadcast"
    ActionReplay         AdvanceAction = "replay"
    ActionReplace        AdvanceAction = "replace"
)

type AdvanceRequest struct {
    IntentID         string        // 011-owned identity (declared by PB scope)
    RequestID        string        // 007 request id (linkage evidence)
    CallerID         int64         // 007 caller (ownership evidence)
    OwnerID          string        // 011 worker instance identity (fencing)
    LeaseVersion     int64         // 011 claim version the caller holds (fencing)
    RecoveryVersion  int64         // 006 version observed by the issuing transaction
    StepID           string        // 011-preallocated, stable across retries
    Action           AdvanceAction
    AnchorAttemptID  string        // replay/replace: attempt being replayed/replaced
    ExpectedTxHash   string        // replay: hash the caller expects for the anchor
}

type AdvanceOutcome struct {
    Class           string // sent | refused_gate | refused_basis | pending_unknown | reconcile_required | unavailable
    AttemptID       string // 010-owned attempt identity (new or converged)
    TxHash          string
    RevisionVersion int64  // authority revision version after this call
    Basis           string // recorded evidence (no secrets)
}

type LifecycleFacts struct {
    Attempts         []AttemptRef // identity + state references, no fact copies
    CurrentAttemptID string
    RevisionVersion  int64
    Unknown          *UnknownRef  // {AttemptID, TxHash, RecoveryCondition}
    Basis            string
}

type LifecycleAdvancer interface { Advance(ctx context.Context, req AdvanceRequest) (AdvanceOutcome, error) }
type LifecycleReader  interface { Read(ctx context.Context, intentID string) (LifecycleFacts, error) }
```

**011 passes only identity, fencing, actions** — never nonce, fees, calldata, signatures, or raw bytes
(construction belongs to 010, signing to 009). **010 returns only classified outcomes and identity
references** — 011 never persists 010's content facts (FR-13 "引用尝试身份而不复制其事实").

## 3. Advance semantics

- `first_broadcast`: 010 builds the first attempt under the intent, persists identity/content before calling
  009 (OC-4), signs via 009, persists signed bytes before any external send, then sends under its own gates
  (including the claim-row verification of gates.md §4). 011 issues at most one open step per intent.
- `replay`: same signed bytes as the anchor attempt; 010 MUST NOT create a new attempt or signing identity
  (010 FR-04); 011 asserts `AnchorAttemptID` + `ExpectedTxHash` so a wrong anchor is refused rather than
  silently replayed.
- `replace`: new attempt + new signing-request identity under the same intent/binding (OC-4; 010 FR-05);
  011 only advances `replace` when the current scope's `allows_fee_replacement=TRUE` (R10); the PB conditional
  fee-triple reuse test is 010's, because 010 constructs the candidate content.
- 011 never advances any action without a current valid claim, current authorization, no pause, valid
  recovery basis, and current 008 observation (gates.md §5).

## 4. `StepID` idempotency (required of 010)

010 MUST converge on `(intent_id, step_id)`:

- A retry with the same `step_id` (011 crash, response loss, timeout) MUST NOT create a second attempt, a
  second signing request, or a second replacement; it returns the recorded outcome for that step.
- A new logical step MUST use a new `step_id`; 011 never reuses a `step_id` for a different action or anchor.
- Consequence used by 011's recovery: if 010 reports no attempt exists for a given `step_id`, no signed bytes
  or send can exist for it (010 persists identity/content before the first 009 call — OC-4), so 011 may
  re-issue that same step under current gates. If existence cannot be determined, the step stays `unknown`
  and 011 reconciles first (never re-issues blindly — Q3).

## 5. Unknown recovery (J5)

- 010 classifies RPC timeout, response loss, missing receipt, DB/connection failure with network progress,
  partial writes, and crashes as **unknown**, never as success/failure/not-paid.
- `LifecycleFacts.Unknown` returns `(attempt_id, tx_hash, recovery condition)` plus the persisted facts and
  the conditions under which recovery can proceed; 011 consumes it, records a `reconcile_observed` event,
  and transitions the intent to `reconciling`.
- 011 MUST NOT mark `failed`, MUST NOT create a new intent/binding, MUST NOT treat unknown as "not sent", and
  MUST NOT "compensate" by rebuilding payment (FR-08/FR-10, C9).
- Resolution: 010's facts decide — `sent`/confirmed ⇒ `completed` (or `revised` later); recoverable-not-sent
  ⇒ a new step may be issued under current gates; still unknown ⇒ remain `reconciling` and keep observing.

**Three facts stay distinct (mandatory)**: (i) confirmed-not-sent this attempt (010 returned no send
result; business effect stays `unknown` pending reconcile) — record it as no-send, never as "nothing
happened"; (ii) a previously-`unknown` business effect stays persisted pending reconcile; (iii) known
RPC/on-chain results (`sent`/`refused_gate`/`refused_basis`, verified receipts) are preserved and never
rewritten. A failed COMMIT MUST NOT mechanically rewrite everything to `unknown`. Recovery bases are
persistable evidence only — never inference.

**继承的 010 两项有限例外（register J4 原文；逐字承接，不重新裁决）**：

- G-010-1（自然到期残差）：有限例外（2026-09-17 新批准，仅自然到期）：最终检查后自然到期的发送残差允许记录并对账，不描述为合法在途，不覆盖其他残差，不批准可配置宽限期；排队/退避/重连/重试后 MUST 重估门禁；结果明确保留真实结果，仅不确定记 unknown。
- G-010-2 class (c)（失锁未感知窗口）：有限例外之二（2026-09-17 新批准，仅失锁未感知窗口）：探针 MUST 用持有保护锁的同一会话/同一事务，仅为缓解；检出失效 MUST 阻止尚可取消的发送；仅限当次发送，禁用绕过重估的透明重试；进入延迟为优化目标，不宣称窗口极短/极罕见；对账比较记录版本与变更证据，无法判定保序时保留不确定性；确认保护丢失且存在门禁失效后发送证据（或无法排除）时冻结该意图后续发送并转人工复核（链观察/查询/对账照常；正常接管非违规证据）；人工复核仅解除本残差的独立冻结原因（受控权限+证据+审计），不覆盖任何门禁；恢复发送前重验全部门禁，对账永不直接许可重发。此批准不覆盖其他残差，不代表实际验证通过。

## 6. Revision consumption (J5)

- 010 writes the revision chain rows; 011's projection consumer reads them **in version order** and applies
  only when `revision_version > stored` ([data-model.md](../data-model.md) Table 5). A failed read marks the
  projection `possibly_stale`; it never rewrites a stored version downward.
- Revision application is an authority-driven fact transition (`completed→revised`, etc.) that consumes **no**
  authorization and needs **no** claim (M2: chain-fact observation, state/projection revision, and
  reconciliation bookkeeping are not new asset execution). Any subsequent send (replay/replace/first
  broadcast) re-runs the full current gate set under Q3 + PB conditional reuse.
- 011 never rebuilds an intent to "compensate" for a revision (FR-10, SC-07).

## 7. Failure classification & bounded retries (FR-08/FR-13, constitution IX)

| 010 class | 011 step terminal state | 011 action |
|---|---|---|
| `sent` | `converged` | progress; expect receipt/confirmation tracking (010) |
| `refused_gate` | `converged` (with refusal basis) | record; do not retry blindly; re-observe gates |
| `refused_basis` | `converged` | record; may require fresh authorization/scope per PB rules (010's instruction) |
| `pending_unknown` | `unknown` | reconcile first; state `reconciling`; never failure |
| `reconcile_required` | `unknown` | same as above |
| `unavailable` / transport error | stays `issued` | bounded retry with backoff; on exhaustion, reconcile on the next cycle (never a failure claim) |

Retries are bounded (research R14 proposal: ≤3 per cycle, exponential + jitter); retryable vs non-retryable
classes are distinguished and never collapsed (constitution RPC standards). No unbounded loop, no backoff
that hides a persistent correctness problem. The three-fact separation in §5 governs every recorded
outcome: a no-send-result abort is recorded as no-send with the effect left `unknown`, a prior `unknown`
stays pending reconcile, and known results are never rewritten. The inherited limited exceptions G-010-1
(natural expiry) and G-010-2 class (c) (unperceived lock-loss) are stated verbatim in §5 and MUST NOT be
widened here.

## 8. What 011 MUST NOT do (boundary assertions)

- No import of go-ethereum RPC/dial packages in `internal/execution`; no chain client (`internal/eth`) use.
- No transaction construction, no nonce arithmetic, no signing call, no broadcast/replay/replace RPC.
- No writes to 010/009/008/007 tables; no attempt/signing-request identity creation.
- No second intent, no compensation intent, no binding creation/release.
- No "decision first" claim of legitimacy: only 010's gate transaction decides the send boundary (Q3).

## 9. Joint acceptance touchpoints (design only; A-13 stays OPEN)

This boundary is where 011's independent acceptance stops and joint acceptance begins. Joint (real
HTTP/PG/Anvil, after 010 merges and upstream sync, 010→011 merge order) MUST cover: intent→attempt ordering,
claim verification inside 010's send gate (including the revoke/expiry race), step-idempotent retries after
response loss, unknown reconciliation, replay/replace identity rules, revision propagation and projection
freshness, and no-second-intent/no-fact-copy assertions. Contract-shape doubles used in 011-independent tests
MUST NOT be presented as joint verification (C10; FR-14).
