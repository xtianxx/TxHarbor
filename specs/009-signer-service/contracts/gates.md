# Contracts: Gate Reads (consumer side) — 009 Signer Service

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R5/R7/R8

009 observes upstream gates **read-only**. It never writes, clears, releases, or queues any
upstream state (FR-17/FR-18). The three contracts below define what is read, when, how results
are classified, and what refusal each class produces. Refusal taxonomy lives in
[persistence.md](persistence.md); response shaping in [api.md](api.md).

Common rule: every gate read happens **inside the transaction whose decision it gates** (sign
transaction or delivery assessment transaction), so the recorded basis and the decision come from
the same read sequence — never from a cached or pre-transaction observation. Read failure is a
refusal class, never a default-open value. Every such transaction first acquires the shared
coordination lock `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery,
reorg_recovery_events IN SHARE MODE` (research R6), so the 006 observation is linearly ordered
against pause/recovery writers even when no pause row exists yet.

## 1. 006 recovery gate (pause rows + active recovery + version)

**Sources** (all 006-owned, read-only): `indexer_pause`, `log_pause`, `deposit_pause` (row
existence = paused), `reorg_recovery` (active instance; any persisted phase blocks ordinary
batches), `reorg_recovery_events` (version when no active row exists).

**Read shape — one statement, one snapshot**: the transaction first takes the gate-table `SHARE`
lock (common rule), then a single SQL statement with scalar subqueries/CTE returns
`(indexer_paused, log_paused, deposit_paused, recovery_id, recovery_phase, recovery_seq,
events_max)` for the deployment chain. This is stronger than a statement *sequence* (no two
statements can straddle a committing pause/recovery write) and stays a pure `SELECT`. It
re-implements the `captureRecoveryVersion` shape from `internal/indexer/reorgcommit.go:108-129`
as a consumer; 006's writer functions are never called.

**Version semantics**: `current_version = active recovery_seq, else events MAX, else 0`
(monotonic: establish/release both append events, so a completed recovery changes the version).
The request persists `recovery_version` — the version the caller's content was built under. The
gate requires **equality** at signing admission and at every delivery attempt. A change after
content construction means the content is based on a stale chain view → refuse
(`recovery_version_changed`); the caller MUST reconstruct content on the new view with a new
request identity (FR-17 "恢复版本变化后旧链视图内容 MUST NOT 签名").

**Refusal conditions** (each records the observed basis in the audit/admission row):
| Observation | Class | Effect |
|---|---|---|
| any pause row present (record which of the three) | `recovery_paused` | refuse; no signing; no queue-for-later |
| active `reorg_recovery` row in any phase | `recovery_active` | refuse; no signing; no queue-for-later |
| `current_version != request.recovery_version` | `recovery_version_changed` | refuse; caller must rebuild content |
| statement error / indeterminate | `gate_read_failed` | refuse (fail closed); retryable with same identity |

**Never**: 009 issues only `SELECT` and the gate-table `LOCK … IN SHARE MODE` (lock acquisition,
no data modification) against these tables; it MUST NOT release/merge pauses, delete recovery
rows, write events, or present any refusal as "approved, will execute after recovery" (006
`contracts/downstream.md`; OC-6 裁决 "只读观察、不得清除或代为解除").

**Ordering (closed in-plan; was EXPOSED GAP G-1)**: 006 establishes pauses via `INSERT` and
recovery rows via 006-owned transactions; those writes take `ROW EXCLUSIVE` on their tables. The
gate-table `LOCK … IN SHARE MODE` conflicts with `ROW EXCLUSIVE`, so:
- a pause/recovery write that committed before 009's lock is visible to 009's in-tx snapshot;
- a pause/recovery write racing 009 waits until 009's transaction commits, i.e. it is ordered
  *after* the admission;
- a **future** `INSERT` (no pause row currently) and the manual-DBA release of
  `indexer_pause`/`log_pause` (which takes no application lock) are both covered, because the
  lock is on the relation, not a row.
A `LOCK TABLE` wait or deadlock is `gate_read_failed` (fail-closed, retryable, same identity).
009 records the admitted snapshot; only bytes already written are in-flight approved and not
recallable, while any delayed send MUST re-admit under current gates (research R6). No 006 change
is required and no residual window is deferred.

## 2. 007 authorization read (`withdrawal_authorizations`, `FOR SHARE`)

**Read shape** (inside the sign transaction, before signing; re-read inside every delivery
transaction):

```sql
SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at
FROM withdrawal_authorizations
WHERE authorization_id = $1
FOR SHARE
```

**Required conditions** (all must hold; otherwise refuse `authorization_invalid` /
`authorization_expired` / `authorization_revoked` with the observed state recorded):
1. Row exists (`absent` → refuse; `read_failed_or_unknown` → refuse, retryable).
2. `state = 'active'` and (`expires_at IS NULL OR expires_at > now()`) on the **database clock**
   (no application clock; mirrors 007's validity predicate).
3. Field equality against the request's declared/bound values: `caller_id` = authenticated
   caller; `chain_id`, `asset`, `recipient`, `amount` equal the request's bound fields. Any
   mismatch → refuse.
4. `authorization_id` is not already bound to a *different anchor* request identity and, when
   reused by a replacement, OC-5's permission is satisfied. The partial anchor index
   `signing_requests_authorization_anchor_uniq` (`UNIQUE (authorization_id) WHERE replacement_of
   IS NULL`) allows at most one non-replacement request per grant (loser → `403
   authorization_invalid`, fresh-authorization instruction); a replacement (`replacement_of` set)
   may share the anchor's grant only if the authorization **explicitly permits the
   fee-replacement purpose** and the fee is in scope, else it MUST use a fresh grant (FR-05,
   research R7/R11). A persisted row's `authorization_id` is never updated — an old request can
   never be rebound ("旧请求换绑禁止").

**Fingerprint & version**: the read persists an `authz:v1` fingerprint over the observed fields
(`caller_id, chain_id, asset, recipient, amount, state, expires_at`) and the observed state; at
delivery the fingerprint MUST match the stored one or delivery is blocked
(`authorization_changed`). The fingerprint is a read-time **surrogate** and is never presented as
an authorization version; OC-5's required `authorization_version` is carried by the R11 upstream
extension and re-checked at delivery when present.

**Lock semantics (why `FOR SHARE` closes what is closable)**: 007's revoke path takes
`FOR UPDATE`/UPDATE on the same row (007 R7). A revoke either commits before 009's share lock is
granted (009 reads `revoked` → blocked/refused) or waits until 009's transaction commits (the
decision stands; the revoke governs later attempts). Share locks do not block concurrent
reads/other deliveries, so there is exactly one lock object on 009's paths. Expiry is a time
boundary, not a lock boundary: it is evaluated in the same read sequence and re-evaluated by the
next attempt (no persistent permit).

**Carrier gaps (explicit; extension owned by 007/011, research R7/R11)**: the carrier has
(a) no `intent_id`/`request_id` column → intent linkage cannot be verified from the row; 009
persists the caller-declared `intent_id` and verifies content/binding consistency + field
equality only; (b) no version column → the fingerprint is a read-time surrogate, never presented
as an authorization version; (c) no fee-scope/purpose column → "该授权显式允许费用替换用途"
cannot be verified from the row, so OC-5's conditional rule operates on the **fresh-authorization
branch** until the R11 `withdrawal_authorization_scopes` carrier lands (this is a recorded gap,
not a reinterpretation of the rule as fresh-only); (d) no `revoked_at` → revocation ordering is
observed as `state='revoked'` only. 009 MUST NOT add columns, must not invent a parallel
authorization table, and MUST NOT silently reinterpret these gaps as satisfied; a grant that
cannot be verified fails closed (`authorization_unverifiable`, research R11).

**Reuse rule**: retries of the same request identity (`signing_request_id`) reuse the same
authorization implicitly (the request row already binds it; no re-consumption, no extension —
FR-05). A fee-replacement identity MAY reuse the anchor's grant only under the OC-5 conditional
rule (condition 4); the partial anchor index keeps exactly one non-replacement request per grant.

## 3. 008 nonce binding read (consumption classes & provider adapter mapping)

**Interface** (009-side; the concrete adapter lands when 008's exact contract exists — D3):

```go
type BindingResult int
const (
    BindingMatches BindingResult = iota // exists, no holds, recovery none
    BindingAbsent
    BindingConflict
    BindingPaused     // paused, reconciling, hold-annotated, or registry disabled
    BindingTerminal   // exists but terminal (consumed/released); audit-only, never signable
    BindingReadFailed // read failed / indeterminate
)
type BindingReader interface {
    ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error)
}
```

**Adapter mapping (008 provider contract outcomes → 009 classes; the only mapping allowed)**:

| 008 provider outcome | 009 `BindingResult` |
|---|---|
| `bound` + no gate-hold annotations + recovery annotations `none` (registry enabled) | `BindingMatches` |
| `bound` with any hold annotation **or** recovery != none **or** registry disabled | `BindingPaused` |
| `terminal` (consumed/released; audit-only) | `BindingTerminal` |
| `not_bound` | `BindingAbsent` |
| `mismatch` | `BindingConflict` |
| `unavailable` | `BindingReadFailed` |

**Admission rule**: only `BindingMatches` admits signing. `BindingAbsent`, `BindingConflict`,
`BindingPaused`, `BindingTerminal`, `BindingReadFailed` → refuse (`binding_absent` /
`binding_conflict` / `binding_paused` / `binding_terminal` / `binding_read_failed`), record the
class and the read basis; no signature.

**`BindingTerminal` semantics** (audit-only; never signable): a terminal binding means 008 has
**consumed or released** that nonce. At submit the request is refused (`binding_terminal`,
recorded with the observed outcome), never signed, and the caller MUST reconcile via 008/011;
**no new identity may reuse the terminal nonce** — terminal never becomes signable, and 009 never
attempts to "re-open" or replace it. `BindingTerminal` is non-retryable by content: retry is only
possible on a *different* binding after reconciliation (new identity), never by resubmitting the
same request/nonce.

**Consumption rules**: 009 MUST NOT modify, consume, or write the binding; MUST NOT trust
caller-declared reservation claims; MUST record the binding reference it read (`binding_ref`) in
the request content set (FR-18/OC-3). A binding's existence is not authorization (OC-5).

**Serialization obligation (008-side, required by R6)**: the 008 adapter MUST implement
`ReadBinding` so the read participates in 008's own pause/registry-writer serialization — a pause
or registry disable that commits before the read MUST be visible to it, and one racing the read
MUST block it (or the read MUST be re-evaluated on the same guarantee). Only then is the
"008 pause since the last attempt" ordering in research R6 sound; 009 records the observed class
and never substitutes its own release evidence (OC-6/OC-7). **Bilateral acceptance**: provided by
008 `contracts/read-api.md` §4 (scope-row `FOR SHARE` during the snapshot; 008-owned writers take
the same row `FOR UPDATE`); 006 ordering for this read stays best-effort here and is covered
009-side by the R6 gate-table lock, not by this read.

**Delivery re-check**: every delivery attempt re-reads the binding and requires
`BindingMatches`. Any non-matching class at delivery — `BindingPaused`, `BindingReadFailed`,
`BindingTerminal`, `BindingAbsent`, `BindingConflict` — blocks delivery (status-only), consistent
with OC-6 "暂停或对账中 MUST NOT 产生签名" extended to the delivery gate; the blocked attempt is
recorded in `delivery_admissions`. Exception: rows whose admission is already
`delivered`/`admitted` stay delivered — an already-cleared delivery is never re-gated (a later
non-matching read does not un-deliver it; future attempts are still gated). 009 never clears 008
pauses and never substitutes its own release evidence (008 owns release authority; OC-6/OC-7).

## 4. Gate read matrix (what is read when)

| Phase | 006 pause+recovery+version | 007 grant `FOR SHARE` | 008 binding | `can_sign` | Policy |
|---|---|---|---|---|---|
| Submit (first) | yes (one statement) | yes | yes | auth-time | yes |
| Submit (same-identity replay, result exists) | via delivery | via delivery | via delivery | via delivery | not re-run (deterministic) |
| Delivery (first response + every retry) | yes (one statement) | yes (fingerprint/version equality) | yes | yes (`FOR SHARE`) | no |
| Status read | no (status reports the last recorded basis) | no | no | no | no |

Rationale: replay determinism (same identity + same content → same outcome) forbids re-running
mutable policy decisions on replay; gates that can revoke authority (006/007/008) are re-verified
on every delivery instead (OC-6).

## 5. Invariants and verification hooks

- `internal/signer` contains **no** non-`SELECT` statement against 006/007 tables; no import of
  006 writer functions or 007 grant writers (reviewable + quickstart V6 assertion).
- No gate observation is cached across requests/attempts; each decision row records the basis
  (audit `detail` for refusals; `delivery_admissions` snapshot for deliveries).
- Refusal never claims "definitely not signed" when the outcome is unknown; unknown outcomes use
  the `unknown_reconcile` vocabulary (persistence.md §4).
