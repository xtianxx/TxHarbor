# Contracts: Send API (011-facing) — 010 Transaction Lifecycle Management

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) | **Data model**: [data-model.md](../data-model.md)

010 exposes one in-process Go surface (`internal/txlifecycle`) consumed by 011's executor and by 010's own
observe-only reconcile loop. There is no HTTP admin surface and no new process (R-010-01). The operations
below are the only legitimate ways to construct, sign, send, reconcile or read an attempt; each carries its
own refusal taxonomy and retry semantics.

## 1. Identity and ownership

| Fact | Owner | 010 behavior |
|---|---|---|
| `request_id` / `intent_id` | 007 / 011 | referenced, never created (D3/OC-1; J1) |
| `binding_ref` (`nonce_bindings.binding_id`) | 008 | read-only consumption; never allocate/consume/release (OC-3) |
| `attempt_id`, `signing_request_id` | caller (011) pre-allocates | persisted before the first 009 call (OC-4/D6; FR-01) |
| signed bytes + `tx_hash` | 010 | persisted before any send; `tx_hash` UNIQUE |
| send/receipt/revision facts | 010 | authoritative; 011 stores only a display projection (Q1/FR-13) |
| `execution_claims` | 011 | read-only `FOR SHARE` fence; never written by 010 (J2/C6) |

## 2. Operations

### 2.1 `PrepareAttempt(ctx, PrepareRequest) (AttemptRef, error)`

Constructs and persists an attempt (T1) before any 009 call. Idempotent by `attempt_id`: same identity +
same envelope re-converges; same identity + different envelope refuses `attempt_conflict` with zero writes.

```go
type PrepareRequest struct {
    AttemptID            string   // caller-allocated, printable
    SigningRequestID     string   // caller-allocated; 1:1 with the attempt
    ReplacementOf        string   // "" for a fresh attempt; anchor attempt id for a fee replacement
    IntentID             string
    BindingRef           string   // 008 binding id
    AuthorizationID      string   // PB grant (same grant for conditional reuse, fresh grant otherwise)
    AuthorizationVersion int64    // observed PB scope version (>= 1)
    RecoveryVersion      uint64   // 006 version the content was built under
    ChainID              uint64
    Sender               string
    Nonce                string   // decimal string
    TxType               uint8    // 0 or 2 (closed shapes)
    GasLimit             string   // decimal string, > 0
    GasPrice             string   // tx_type 0 only
    MaxFeePerGas         string   // tx_type 2 only
    MaxPriorityFeePerGas string   // tx_type 2 only
    Asset                string   // ERC-20 contract == To
    Recipient            string
    Amount               string   // decimal string >= 1
}
```

010 derives `to = asset`, `value = 0`, `data = transfer(recipient, amount)` (R-010-08 calldata builder) and
rejects any request that would violate the v1 shape. Storage guards (Table 1) enforce the same shapes at the
database level. A replacement request is refused (`replacement_mismatch`) unless `ReplacementOf` names an
existing attempt with the same `intent_id`, `binding_ref`, `chain_id`, `sender`, `nonce`, asset, recipient and
amount (FR-05); fee dimensions must differ from the anchor's, otherwise the replacement is pointless and is
refused as `replacement_no_fee_change`.

### 2.2 `Send(ctx, SendRequest) (SendResult, error)`

Runs the full send region (T2 if unsigned → T3). `Kind` is the caller's declared intent and is validated
against durable state; a mismatch refuses rather than guessing (`send_mode_mismatch`, `already_accepted`).

```go
type SendRequest struct {
    AttemptID    string
    Kind         SendKind // SendInitial | SendReplay
    Claim        ClaimRef // intent claim the caller currently holds
    ExpectedRevision *int64 // optional optimistic guard; stale → send_stale, zero dispatch
}
type ClaimRef struct {
    IntentID     string
    WorkerID     string
    LeaseVersion int64
}
type SendResult struct {
    Outcome      string   // accepted | rejected | unknown
    RPCClass     string   // evidence class when rejected/unknown
    TxHash       string
    SendSeq      int
    RefusalClass string   // set when zero-dispatch refusal (Outcome == "blocked")
}
```

- `SendInitial`: allowed only from `signed` (after a recorded reconcile observation, R-010-06); refused
  `already_accepted` when an `accepted` dispatch already exists, `attempt_not_sendable` for non-sendable
  states.
- `SendReplay`: allowed from `sent` directly, and from `unknown`/`signed` after a recorded reconcile
  observation; always requires current gates (FR-04/FR-15).
- The dispatch result is classified fail-safe (R-010-05). `rejected` and `unknown` both leave the business
  effect undetermined; the return value never claims success or failure of the payment.

### 2.3 `Reconcile(ctx, attemptID, txHash) (ReconcileResult, error)`

Claim-free, observe-only (T4): probes the chain by `tx_hash`, records one `tx_reconciliations` row, and on
inclusion verifies the receipt + expected Transfer, canonicality, confirmation basis and revision chain.
Never sends, never consumes a claim, never writes upstream tables. Idempotent under repetition. This is the
**unknown recovery interface** of J5 (facts + recovery conditions) when the attempt is `unknown`.

### 2.4 `Status(ctx, attemptID) (AttemptStatus, error)`

Authoritative read for operator diagnosis and for 011's display projection (R-010-13/FR-13). Returns
identity, state, `revision_seq`, `updated_at` (DB clock), `tx_hash` when persisted, latest dispatch outcome,
latest reconcile class, latest receipt effect/confirmation basis, and current recovery/authorization
references. It **never** triggers a send, gate bypass or delivery; using it as a permission is forbidden
(FR-13/Q1).

### 2.5 Operation matrix

| Operation | Writes | Needs claim | External effect | Idempotent | Typical caller |
|---|---|---|---|---|---|
| `PrepareAttempt` | T1 | no | none | yes (identity) | 011 executor |
| `Send` | T2/T3 | yes (current) | chain dispatch | yes (same bytes) | 011 executor |
| `Reconcile` | T4 | no | chain reads | yes | 011 loop / 010 loop |
| `Status` | none | no | none | yes | operator / 011 projection |

## 3. Refusal taxonomy (Send)

Fail-closed; every class is recorded with its observed basis and produces zero dispatch. Retryable classes
are safe to retry with the same identity after the state changes.

| Class | Meaning | Retryable |
|---|---|---|
| `claim_absent` / `claim_version_mismatch` / `claim_expired` / `claim_revoked` | execution qualification not current at initiation (FR-14; natural expiry and explicit revocation produce the same refusal, distinguished in the recorded basis) | yes (after takeover/re-verification) |
| `pause_present` / `recovery_active` | 006 gate active (cause recorded; multi-cause visible) | yes (after release) |
| `recovery_version_changed` | content built under a stale chain view | new identity required (009 gate precedent) |
| `gate_read_failed` | gate/binding/coordination read failed or indeterminate | yes |
| `binding_absent` / `binding_conflict` / `binding_paused` / `binding_terminal` / `binding_read_failed` | 008 consumption classes (gates.md §3 mapping) | yes except `binding_terminal` (reconcile with 008/011) |
| `authorization_missing` / `authorization_inactive` / `authorization_revoked` / `authorization_expired` / `authorization_mismatch` / `authorization_unverifiable` | 007/PB grant facts fail current checks | no (fresh authorization / new identity) |
| `scope_reuse_forbidden` / `fee_scope_exceeded` | replacement reuse not permitted by the scope or a fee dimension out of range | no (fresh authorization + new identity) |
| `attempt_conflict` / `hash_conflict` / `replacement_mismatch` / `replacement_no_fee_change` | identity/content invariant violation | no (correct the caller input) |
| `already_accepted` / `send_mode_mismatch` | declared kind contradicts durable state | yes (reissue with the correct kind) |
| `attempt_not_sendable` / `attempt_not_found` / `send_stale` | state/revision contradiction | depends (read status, then decide) |
| `signature_refused` / `signature_mismatch` | 009 refusal passed through / local hash-sender cross-check failed | `signature_mismatch`: no (investigate); 009 refusals: as 009 documents |
| `coordination_unavailable` | storage/coordination failure before dispatch | yes |

Refusals from steps 2–9 of the region are committed as events with the gate snapshot; the dispatch is never
entered (zero-send evidence, SC-01/SC-06).

## 4. Claim qualification mapping (J2, frozen contract shape)

The frozen contract fixes semantics; 011 owns the concrete column names. The 010 adapter absorbs naming and
must require, in one `FOR SHARE` read keyed by `intent_id`:

| Required semantic | Meaning in 010's gate |
|---|---|
| intent uniqueness | exactly one claim row per `intent_id`; absent → `claim_absent` |
| worker identity | equals the `WorkerID` presented by the caller; mismatch → `claim_version_mismatch` |
| monotonic `lease_version` | equals the presented `LeaseVersion`; old version → `claim_version_mismatch` (never revive an old version) |
| expiry | `expires_at > now()` on the DB clock; otherwise `claim_expired` |
| active/revocation state | no revocation marker and an active status; otherwise `claim_revoked` |

Natural expiry and explicit revocation are recorded with distinct basis strings and identical refusal effect
(no grace, no TTL). On the 010 branch this table does not exist; the adapter fails closed and 010-independent
tests use a contract-shaped fixture labeled test-only (R-010-11/G-010-4).

## 5. Guarantees and non-guarantees

- `Send` returning `accepted` means the node accepted the bytes for propagation — **not** inclusion and
  **not** payment (FR-08 keeps the verdict with the receipt + Transfer check).
- `not_found_yet` from `Reconcile` is never a failure verdict (G-010-5).
- `Status` is data, not permission (FR-13).
- 010 never creates an intent, never allocates a nonce, never releases a binding, never clears a pause or
  recovery, and never touches key material (FR-10, OC-3, OC-6).
- A send that entered dispatch under a valid gate set is legally in-flight; the invalidation that lost the
  lock race governs later replays/replacements (R-010-04; G-010-1 covers the time-based residue).
- G-010-2 class (c) is closed as a second explicit limited Q3 exception (unperceived lock-loss window only;
  adjudicated 2026-09-17; same-session probe mitigation; freeze-plus-manual-review on proof; reconcile never
  permits resend); classes (a)/(b) stand as specified (plan.md Adjudication addendum 2; send-gate.md §4(d)(c);
  research.md R-010-14; quickstart.md J5). This exception covers this residual only — not other residuals and
  not actual verification.
