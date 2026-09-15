# Contracts: Persistence, Retry & Failure Semantics — 009 Signer Service

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R3/R4/R6/R7

This contract fixes the durable ordering rules that make retries, response loss, and crashes
deterministic (FR-13–FR-15, FR-23), and the refusal taxonomy every failure class maps to. Table
definitions are in [data-model.md](../data-model.md); gate classes in [gates.md](gates.md);
response shapes in [api.md](api.md).

## 1. Sign+persist ordering (submit path)

One transaction owns the decision. Fixed order (research R4):

```text
BEGIN
  statement-timeout guard (SET LOCAL statement_timeout = '5s')
  plain INSERT signing_requests (state='received')            -- identity + envelope bound first
    └─ 23505 (caller_id, signing_request_id) → ROLLBACK → replay path (§3)
  SELECT … FOR UPDATE own row                                 -- serialize same-identity attempts
  006 gate read (one statement, gates.md §1)
  008 binding read (five classes, gates.md §3)
  007 grant read FOR SHARE + validity/equality (gates.md §2)
  policy validation (chain/sender/asset/recipient/amount/fees)
  KeyProvider.SignTx(...)                                     -- no external side effects
  INSERT signature_results                                    -- THE durable result
  UPDATE signing_requests SET state='signed' (or 'rejected')
  INSERT signing_request_audit
COMMIT
  └─ only now: delivery assessment (§2) → response bytes
```

Invariants this ordering establishes:

1. **Persist before return** (FR-13): no response containing signature material is produced before
   the result COMMIT succeeds. The delivery assessment is a separate, later, recorded step.
2. **No partial success**: a failure after signing but before COMMIT rolls the whole transaction
   back — no result row, no state change, response is a failure class; `signature_results` can
   never exist without the request row being `signed`, and vice versa (same transaction).
3. **No second observable signature**: `signature_results_pkey` (one result per request identity)
   + `signature_results_tx_hash_uniq` (one persisted result per signed object) decide at storage.
   The row lock serializes concurrent same-identity attempts; the loser converges on the winner's
   persisted outcome (§3).
4. **Evidence co-commits with the decision**: gate/authorization observations, policy version, and
   the audit row are written in the same transaction as the state transition they justify.

## 2. Delivery admission ordering

Delivery is never a pure read of Table 4; it is a recorded assessment (research R6, OC-6/OC-7):

1. `BEGIN` → short read transaction: 006 gate re-read (one statement) + version equality; 008
   binding re-read; 007 grant re-read `FOR SHARE` + fingerprint equality.
2. Persist one `delivery_admissions` row: verdict `admitted` **before** any response byte, or
   `blocked` with the observed basis (`pause_basis`, `recovery_basis`, `reason`). `COMMIT`.
3. Write the response. On `admitted`, a bounded best-effort `UPDATE … SET verdict='delivered',
   delivered_at=now()` follows; its loss leaves `admitted`, which means "cleared for delivery,
   write outcome unknown" (in-flight approved, not reclaimable).

Consequences:

- A gate failure at delivery produces a status-only response: `state`, `content_hash`, refusal
  class, reason, timestamps — **no signature bytes**; `tx_hash` only when a prior admission row is
  `delivered`/`admitted` (api.md §3). No "still-200 replay" semantics (OC-6).
- Re-delivery of the same persisted bytes after a passed gate is allowed (same bytes are
  idempotent); re-signing is never involved.
- `unknown_reconcile` (§4) is used when the service cannot determine whether a delivery happened.

## 3. Retry determinism (same identity, same envelope)

| Durable state on retry | Response path |
|---|---|
| No row (first attempt never committed) | full submit path runs; may sign once and persist |
| Row, no result yet (in-flight / crashed pre-commit / failed state) | submit path re-enters gates+sign for the same identity; `FOR UPDATE`/unique index makes concurrent duplicates wait; at most one result persists |
| Row + result, `signed` (crash after COMMIT, response lost) | T-deliver only; returns the persisted result through the current gates, or status-only if blocked; never re-signs |
| Row `rejected` (terminal refusal) | returns the same refusal class; no signature; a different content needs a new identity |
| Row `failed` (bounded transient, no result) | same identity may re-drive the sign path (gates re-run); `failed` never becomes `signed` without a persisted result |
| Duplicate arrives with same identity + **different** envelope | `409 request_conflict`; original row/result untouched |

Hard rules:

- Same identity + same content MUST converge to the same durable result (N≥2 concurrent/sequential
  submissions, restart included) — SC-03.
- A client MUST retry with the **same** `signing_request_id` + same content after any
  unavailable/unknown response; rotating the identity to "try again" is forbidden by contract and
  is how duplicate signing is avoided.
- The service MUST NOT claim "definitely not signed" for an unknown outcome: it reports the
  outcome as unknown/not-yet-visible and the caller re-drives the same identity.
- On retry, mutable **policy** is not re-evaluated (deterministic outcome), but revocable
  **gates** (006/007/008) are re-evaluated at delivery (§2).

## 4. Refusal taxonomy (machine classes)

One vocabulary, used by audit rows, logs, and the HTTP contract (api.md §2 maps codes to status).
Retryability is a property of the class, not of the caller's patience.

| Class | Meaning | Retry with same identity? |
|---|---|---|
| `unauthenticated` | credential missing/invalid/revoked | no (fix credential; different credential = same caller or different caller) |
| `signing_not_permitted` | authenticated, `can_sign=false` | no |
| `malformed_request` | body unparseable / unknown fields / wrong JSON types | no |
| `arbitrary_digest_rejected` | no complete transaction content (digest/hash/message-only) | no |
| `validation_failed` | field-shape/semantics (incl. `amount`, `address`, `fee_shape`) | no |
| `policy_refused` | chain/sender/asset/recipient/amount-cap/fee-cap policy (`policy_*`) | no (policy change required) |
| `request_conflict` | same identity, different envelope | no (new content needs a new identity) |
| `binding_absent` / `binding_conflict` | 008 binding read classes | no (upstream must fix reservation) |
| `binding_paused` | 008 paused/reconciling (hold annotation, recovery != none, or registry disabled) | after release (same identity) |
| `binding_terminal` | 008 binding exists but terminal (nonce consumed/released, audit-only) | no by content; only via a new identity + binding after reconcile — terminal never becomes signable |
| `binding_read_failed` | 008 read failed/indeterminate | yes (same identity) |
| `authorization_invalid` / `authorization_expired` / `authorization_revoked` | 007 grant absent/inactive/expired/revoked/mismatch/changed | no (fresh authorization required) |
| `recovery_paused` / `recovery_active` | 006 pause row / active recovery | after release (same identity; never queued) |
| `recovery_version_changed` | content built on a stale recovery version | no (reconstruct content on the new view, new identity) |
| `gate_read_failed` | gate statement error/indeterminate | yes (same identity) |
| `key_provider_unavailable` / `key_provider_timeout` | signing backend bounded failure | yes (same identity, bounded) |
| `storage_unavailable` | pool/tx/commit unavailable | yes (same identity) |
| `outcome_not_yet_visible` | identity row exists, result not yet durably visible (in-flight/unknown) | yes (same identity; poll/retry) |
| `signature_withheld` | delivery gate failed after a result exists (authorization expired/revoked/changed, 006/008 pause, 008 binding non-match incl. terminal/absent/conflict, version change) | yes, after the state changes (status-only until then); already-`delivered`/`admitted` rows stay delivered, never re-gated |
| `outcome_unknown` | cannot determine whether delivery happened (reconcile semantics) | status/reconcile only; never re-sign |

Rules: a refusal class MUST NOT be upgraded to success by any later step; `failed` MUST NOT be
recorded or reported as `signed`; delivery withholding MUST NOT delete or alter the persisted
result/binding/audit history; a withheld retry MUST NOT silently rebind the authorization (OC-6).

## 5. Bounded failure handling

- **Statement bound**: every transaction sets the repo's 5s `statement_timeout` guard; no
  unbounded query on any path.
- **Key provider bound**: a context deadline from `TXHARBOR_SIGNER_KEY_TIMEOUT` (default 5s)
  wraps `KeyProvider.SignTx`; timeout is `key_provider_timeout` (retryable), never a partial
  success and never recorded as signed.
- **No internal retry loops**: one 23505 classification per request; commit-unknown resolution is
  a bounded re-read (row by identity, admission by attempt_seq) with no loop; the retry budget
  belongs to the caller, which MUST use the same identity. Infinite retries: 0 (SC-08).
- **Crash safety**: crash before COMMIT → nothing observable (retry converges); crash after COMMIT
  → result/admission durable (retry reads it); crash during response write after `admitted` →
  in-flight approved (never re-signed, never claimed undelivered).
- **Storage unavailability**: no success response is ever synthesized without a durable result;
  when storage returns, the persisted result remains retrievable via the delivery path.
- **Idempotent retry instruction**: every unavailable/unknown response tells the caller to retry
  with the **same** `signing_request_id` and content (never to rotate keys or identities).

## 6. Unknown-outcome reconciliation

Unknown outcomes are first-class: the response says the outcome is unknown (status-only), the
durable rows say what was decided, and reconciliation uses the audit + admission history —
`signing_request_audit` (per-decision) and `delivery_admissions` (per-delivery snapshot/verdict).
An unknown outcome MUST NOT be resolved by creating a new identity or a new payment intent
(011 owns intent rework; 006 forbids "compensate by re-paying"). Reconciliation verification is
listed in quickstart V7; a `signature_withheld` state is the standing-safe answer until the gate
basis changes.

## 7. Logging, metrics, secrecy (FR-22/FR-24)

- Structured log fields allowed: `caller_id`, `signing_request_id`, `attempt_id`, `intent_id`,
  `chain_id`, `sender`, `nonce`, `content_hash`, `state`, `refusal_class`, `policy_version`,
  `retry_count`, stage, latency, key-provider class. Credentials, secrets, key material,
  signature bytes, `tx_hash` on refusal paths, and raw signed-transaction bytes MUST NOT appear
  in logs/errors/metrics (FR-22; constitution XII). `logx.Redact` covers credential-bearing
  strings in the startup echo and errors.
- Metrics (existing Prometheus registry): signing requests total, refusals by class, signing
  failures by class, key-provider latency/health, persistence commit results, gate-refusal
  counts (006/007/008), delivery admissions by verdict. No per-request high-cardinality labels.
- Audit `detail` strings are assembled from the allowlist above; they are redacted evidence, not
  dumps.

