# Contracts: API & Operator Surface — 011 Withdrawal Execution Worker

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R1/R2/R9/R14

011 exposes exactly two HTTP operations on the existing `serve` listener (no new listener, R1) plus one
operator subcommand. Gate reads are in [gates.md](gates.md); ordering/refusal taxonomy in
[persistence.md](persistence.md); the 010 boundary in [lifecycle.md](lifecycle.md). Error codes reuse
007's taxonomy (`internal/withdrawal/errors.go`); amounts are canonical decimal strings (never floats);
responses never carry credentials, keys, or raw signed bytes.

Route registration is method+pattern on the existing mux (`internal/app/serve.go:317-326`); the two
new patterns are more specific than the `/withdrawals/` subtree and therefore win without touching 007's
handler. A missing/invalid credential is 401 without a DB read; a foreign or nonexistent request renders
identically as 404 (007 precedent, `internal/withdrawal/query.go:76-96`).

## 1. Execution admission — `POST /withdrawals/{request_id}/execution`

**Purpose (FR-01/FR-02/FR-03/FR-07)**: admit one `Accepted` 007 request for execution and persist its
stable payment intent, **before** any 008 reservation. This is the only entry that creates intents.

**Authentication & fixed permission**: `Authorization: Bearer txh_…` (authenticated by
`withdrawal.Authenticate`, single indexed read, no cache — `internal/withdrawal/auth.go:104-167`), then
the fixed interface permission `execution_caller_permission.can_execute = TRUE` for the resolved
`caller_id`. Absent/FALSE ⇒ 403 `forbidden` (fail-closed, R2). No new role, no new credential family.

**Path/body**: `request_id` from the path; empty body. The intent identity is **not** client-supplied:
it is the PB scope's declared `intent_id` read in the admission transaction (R3), so there is nothing to
forge or mismatch.

**Admission gates (all must hold; any failure ⇒ zero intent rows)**:
1. The request row exists, belongs to the authenticated caller, and `status='accepted'`
   (`migrations/000007_withdrawal_creation.sql:86-112`: status is fixed at `accepted` by a CHECK — Accepted
   is receipt only, never execution authority).
2. The 007 grant row (`withdrawal_authorizations`) is `active`, unexpired on the DB clock, and equal to the
   request's `caller_id/chain_id/asset/recipient/amount` (read `FOR SHARE`).
3. The PB scope row exists and covers this request: `intent_id` present and well-shaped, `request_id` equals
   the path request, `sender` equals the request's operator-declared sender, `authorization_version ≥ 1`
   (read in the same `FOR SHARE` sequence). Missing scope ⇒ `authorization_unverifiable` (PB Q-B, R3/R10).
4. `sender` is an `active` row in `nonce_wallet_registry` for the request's chain (OC-2: registration proves
   usability, not authority; read `FOR SHARE`).
5. 006 gates: no pause row of the three, no active `reorg_recovery` row (single-statement snapshot under the
   gate-table `SHARE` lock — [gates.md](gates.md) §1). The observed recovery version is recorded as evidence.

**Transaction (T-admit, [data-model.md](../data-model.md))**: gate-table `SHARE` → request row read →
registry row `FOR SHARE` → grant `FOR SHARE` → scope `FOR SHARE` → `INSERT payment_intents` →
`INSERT execution_events('admitted')` → `INSERT request_status_projection`. On `23505` of
`payment_intents_request_uniq` or `payment_intents_authorization_uniq`: roll back, re-read the existing
intent, and classify by identity basis — same request/grant/scope/intent basis ⇒ **recorded** outcome;
different basis ⇒ 409 conflict with zero writes (insert-first protocol, 009 R3 precedent).

**Responses**:

| Status | Condition | Body |
|---|---|---|
| 201 | intent created now | `{request_id, intent_id, state:"admitted", sender, chain_id, authorization_id, authorization_version}` |
| 200 | replay/concurrent call, same basis (recorded) | same shape as 201 (recorded read-back) |
| 400 | malformed path/body | `{code:"validation_failed",…}` (no DB) |
| 401 | missing/malformed/revoked credential | 007-shaped `unauthenticated` |
| 403 | `can_execute=FALSE` (or row absent) | 007-shaped `forbidden` |
| 404 | request missing or owned by another caller | identical 404 (no existence leak) |
| 409 | `request_id`/`authorization_id` already bound with a **different** identity basis | conflict, zero writes |
| 422 | request not `accepted` / grant not active-valid / scope missing or mismatched / sender unregistered | 007-shaped refusal with the recorded class |
| 503 | storage unavailable | 007-shaped `temporarily_unavailable` (never a false refusal) |

**Idempotency & concurrency**: concurrent/repeated admission converges on one intent
(`UNIQUE(request_id)` + insert-first); a retry never creates a second intent; a refused admission writes
no rows except (best-effort) audit evidence. **Accepted ≠ admitted ≠ authorized** (FR-01).

**Explicit non-goals**: this endpoint does not reserve nonces, does not claim, does not sign, does not
broadcast, does not create compensation intents, and never mutates 007 rows (011 writes only its own
tables).

## 2. Execution view — `GET /withdrawals/{request_id}/execution`

**Purpose (FR-09, display only)**: authenticated caller reads the execution status of their own request.

**Semantics**:
- Ownership-enforced exactly as 007's query path: the response returns the same 404 for missing and
  foreign requests.
- `execution` block is read from 011's **authoritative** rows (`payment_intents`, `execution_claims`,
  `execution_steps`) — authoritative for 011's own domain.
- `lifecycle` block is read from `request_status_projection` and **must** include
  `{attempt_id, lifecycle_version, observed_at, freshness}` where `freshness ∈ {confirmed, possibly_stale}`.
  When `possibly_stale`, the payload states the reference may be out of date and never presents the stale
  terminal state as a verified current result (Q1/FR-09).
- This endpoint is **never** an admission/qualification/send/reconcile permit. No caller may treat its
  output as authorization (the same rule the 006/007 query surfaces state:

Response shape (illustrative, not a schema commitment):

```json
{
  "request_id": "…", "intent_id": "…",
  "execution": {"state": "executing", "state_version": 7, "owner": "…", "lease_version": 3,
                 "expires_at": "…", "steps": [{"step_id":"…","action":"first_broadcast","state":"converged","attempt_id":"…"}]},
  "lifecycle": {"attempt_id": "…", "revision_version": 12, "observed_at": "…", "freshness": "confirmed"}
}
```

**No secrets**: no keys, credentials, raw signed bytes, or transaction raw bytes; amounts/fees (if ever
included) are integer decimal strings.

## 3. Operator surface — `withdrawal-exec` subcommand

Privileged local operator paths (no new role, no new credential family; shape mirrors
`apikey-auth`/`withdrawal-authz`/`nonce-admin`). Every mutating operation carries a caller-supplied
`operation_id` and is deduplicated by `execution_ops_audit.operation_id` (23505 ⇒ rollback ⇒ read back ⇒
same input reports the recorded outcome / different input is `operation_conflict` with zero writes — 008
R7 protocol). Operator identity and reason are audit fields, always required for mutations.

| Operation | Arguments | Effect | Refusals |
|---|---|---|---|
| `permission-set` | `--caller-id`, `--can-execute=true/false`, `--operation-id`, `--operator`, `--reason` | upsert `execution_caller_permission` | caller missing (FK) |
| `permission-revoke` | `--caller-id`, `--operation-id`, `--operator`, `--reason` | set `can_execute=FALSE` | already false ⇒ `nop` recorded |
| `claim-revoke` | `--intent-id`, `--expected-lease-version`, `--operation-id`, `--operator`, `--reason`, `--evidence` | conditional single-row update: `state='revoked'`, `ended_at/end_kind='revoked'`; `execution_events('revoked')`; audit | evidence missing ⇒ refused; version mismatch ⇒ refused with zero writes (never chase the version); intent/claim missing ⇒ 404-equivalent |
| `projection-refresh` | `--request-id`, `--operation-id`, `--operator` | read 010 authority, apply version-guarded projection update; `projection_refreshed` event | authority unavailable ⇒ `refused` recorded, projection marked possibly_stale |
| `claim-show` / `step-list` / `event-list` | `--intent-id`/`--request-id` | read-only inspection (no audit row) | missing ⇒ not found |

**Ruling constraints honored**: no operation is a required gate for automatic takeover (M3); `claim-revoke`
is an evidence-required escape hatch, never a prerequisite; `projection-refresh` exists so that unbounded
display staleness is never the only option, while no numeric display SLA is promised (C11).

## 4. Error taxonomy & response hygiene

- Refusal/error classes reuse the 007 vocabulary; new classes are additive and closed:
  `execution_permission_denied`, `authorization_unverifiable`, `scope_mismatch`, `sender_unregistered`,
  `claim_lost`, `claim_not_current`, `step_open_unreconciled`, `lifecycle_unavailable`, `operation_conflict`.
- Every refusal records the observed basis (class + identity + versions); a failed read is a refusal class
  and never a default-open value; unknown outcomes use the `unknown`/`reconcile` vocabulary, never
  "failed"/"not paid".
- Server logs never contain credentials, keys, or signed/raw transaction bytes; structured fields are listed
  in [research.md](../research.md) R13.

## 5. Surface summary

| Surface | Transport | Auth | Writes | Decision authority |
|---|---|---|---|---|
| Admission | `serve` HTTP `POST /withdrawals/{id}/execution` | Bearer + `can_execute` | intents/events/projection rows | creates the intent; never sends |
| Execution view | `serve` HTTP `GET /withdrawals/{id}/execution` | Bearer + ownership | none | none (display only) |
| Operator ops | `withdrawal-exec` CLI | DB operator privileges + audit | permission/claim/audit/projection | disqualification/refusal only; no send, no signing |
| Worker loop | `withdrawal-worker` process | DB + 010 boundary | claims/steps/events/projection | scheduling + advance calls; never constructs/signs/broadcasts |
