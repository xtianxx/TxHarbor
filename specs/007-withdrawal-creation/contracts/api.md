# Contracts: 007 Withdrawal Creation & Query

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15 | **Spec**: [spec.md](spec.md) (Q1–Q5 integrated, zero markers)

## 1. HTTP contract

Base: existing probe listener (`TXHARBOR_HTTP_ADDR`, default `127.0.0.1:8080`); 007 handlers mount
under the same `http.Server` (plan wires; no new listener). All bodies JSON. Auth: `Authorization:
Bearer <txh_…>` (FR-03). `caller_id` comes from the key row, never from the body.

### POST /withdrawals

Request (all required):
```json
{"idempotency_key": "opaque 1-128 ASCII 0x21-0x7E",
 "chain_id": 31337,
 "asset": "0x1111111111111111111111111111111111111111",
 "recipient": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
 "amount": "1230000000000000000",
 "authorization_id": "up-9f2c…"}
```
- `amount`: string, `[1-9][0-9]*`, ≤ 2²⁵⁶−1 (FR-06). `asset`/`recipient`: FR-07 shape
  (mixed-case MUST pass EIP-55; stored lowercase). `authorization_id`: FR-03b grant.

Responses (Q5/FR-14, stable machine code + human message + request trace id; no secrets):
| Case | Code | Body essence |
|---|---|---|
| first persist | 201 | full request (see §3) |
| same key + FR-10 equality | 200 | original request, same `request_id` |
| same key + any business-param/auth-id differ | 409 | `idempotency_conflict`; original untouched |
| malformed JSON | 400 | `malformed_request` |
| schema/param invalid | 422 | `validation_failed` (+ field) |
| missing/invalid/revoked key | 401 | `unauthenticated` |
| no interface permission / grant missing/inactive/mismatched | 403 | `unauthorized` / `authorization_invalid` |
| storage unavailable | 503 | `temporarily_unavailable`; MUST NOT claim "definitely not created"; MUST tell caller to retry with the **same** key + params, never to rotate keys |
| uncertain submit outcome | 503-shape | same retry-with-same-key instruction; never "Accepted" |

Retry-after-accept with later grant expiry/revocation: still 200 (Q5) — the replay checks current
auth-interface permission but MUST NOT re-fail on the original grant's later state.

### GET /withdrawals/{id}

- Auth required (401 if not). Ownership-enforced read: other callers' or nonexistent ids return
  **identical 404** (`not_found`, same code/message/shape; FR-15/Q5). 404 normalization reduces
  existence leakage; it does not claim to stop all enumeration.
- Recovery-active behavior (§4): request facts always servable; execution state never claimed.

## 2. Validation order (绑定顺序; §三.3)

Per request, in order: (1) auth (401) → (2) interface permission + grant check (403) →
(3) input shape/semantics incl. whitelist (400/422) → (4) existing-key lookup (200/409 fast path) →
(5) grant re-validation in-tx (403) → (6) atomic persist. The in-tx grant re-check is
authoritative; the pre-tx lookup is an optimization only. Permanent-replay invariant: only steps
(1)–(2) are re-evaluated on replay; a later grant expiry/revocation MUST NOT turn a replay into
a failure, and no other mutable check may be added later that breaks permanent replay (plan
documents the closed check set; FR-11).

## 3. Query shape (Q8落盘: 请求事实; 字段名沿仓库 snake_case)

```json
{"request_id": "wr-…", "caller_id": 7, "chain_id": 31337,
 "asset": "0x1111…", "recipient": "0xaaaa…", "amount": "1230000000000000000",
 "status": "accepted", "created_at": "2026-09-15T…Z",
 "recovery": {"state": "none", "execution": "not_started"}}
```
- `status: accepted` = received + persisted; MUST NOT read as executed/paid/deducted (FR-08).
- `recovery.execution` is **always** `not_started` in 007: "尚未执行" is a standing fact, never
  "执行结果未知" (Q8 correction — unknown-outcome vocabulary belongs to 006 in-flight *external*
  requests, FR-17, not to 007 rows).
- `recovery.state`: 006 state at read time (`none`/`recovering`/`paused_reconcile`/`released`,
  006 FR-18 subset); request facts stay servable in every state. Reader contract (§五修正):
  every GET performs `LoadRecoveryState` + `RecoveryReleased` (`reorgquery.go:62-83` — nil-able
  point reads, no side effects, derived from durable state at read time, never a cache flag).
  The two reads are NOT one atomic snapshot: a release-then-re-establish landing between them
  can combine a stale row with a newer terminal event (or vice versa). Combination rule
  (locked here): if EITHER read fails → `state: unknown`; if the row is present → report its
  phase-mapped state (`recovering`/`paused_reconcile`) regardless of the terminal-event read
  (a live row is never contradicted by history — the row is the sole recovery authority per
  006 R9); if no row AND a terminal release event exists → `released`; if no row AND no event
  → `none`. A failed read MUST NOT map to none-or-released. `state` and the request row are two
  independent reads, not an atomic global view — the contract promises per-read freshness plus
  the precedence rule above, never cross-read atomicity.
- 007 rows carry NO chain height: `AnnotateRecoveryHeight(row, released, height)` is NOT called
  with a fabricated height (the previous "ancestor-side facts" wording is withdrawn — 007 must
  not invent an observation height to reuse a height-indexed function). Height-indexed validity
  (`valid_unaffected`/`provisional_replaying`/`unknown_paused`) does not apply to 007 rows at all;
  the only recovery signal on a 007 response is `state` (+ constant `execution: not_started`).
  Request facts (Table 3 row) and recovery signal (006 rows) are expressed side-by-side with no
  claim that they were read atomically.
- POST + recovery-read interplay: recovery state is never read on the POST path (creates do not
  depend on it — intake observes the 006 downstream preconditions subset per FR-16, which is a
  gate on follow-on action, of which 007 has none). A POST that committed but whose response was
  lost replays by key (200, §1); recovery-read failure on a later GET changes only the `state`
  field (`unknown`), never the replay outcome or the `request_id`.

## 4. Recovery-period behavior (006承接, FR-16/FR-17)

- Compliant creates during active recovery: allowed, idempotent persist, receive-only; the intake
  path performs zero execution side effects by construction (no nonce/signer/broadcast code exists
  in 007) and observes the 006 downstream preconditions subset (pause rows + active recovery row;
  observation failure → refuse follow-on action, which is none — and record the cause).
- Queries never present mixed/stale chain views as complete-valid; 007 rows carry no chain-derived
  execution data at all, so the boundary is structural, not annotative.
- 006-owned governance (pauses, de-authorization, isolation rules) is read-only to 007.
- Downstream (008–011, no specs yet): `request_id` + grant identity + immutable business params
  (`chain_id`, `asset`, `recipient`, `amount`) + one-request↔one-payment-intent association
  requirement; re-authorization/cancellation before execution is downstream's design surface
  (FR-03b recorded). No future tables/modules created here; no bilateral conformance claimed.
