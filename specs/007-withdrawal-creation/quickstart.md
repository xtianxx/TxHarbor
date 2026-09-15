# Quickstart: 007 Withdrawal Creation & Query (validation guide, design only)

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15

Design-only validation guide. Nothing here is executed in this step; scenarios become
integration/E2E tests in tasks/implementation. Environment: Anvil (chain-31337) + real
PostgreSQL via `make test-integration` (Docker-provisioned; compose stack is a manual-debug
aid only, per 006 plan precedent).

Prerequisites: migration `000007` applied; one `caller` row + one active `api_key`
(operator tooling, research R5); one `withdrawal_authorizations` grant row bound to the
same params; deployment `chain_id` = Anvil chain.

## V1 — happy path + self query
POST valid body → expect 201 + `request_id`, `status: accepted`; GET with same key →
same body, `recovery: {state: none, execution: not_started}`.

## V2 — auth matrix
No header / wrong key / revoked key → 401; key without `can_create` → 403; body with forged
`caller_id` differing from key identity → ignored (ownership from key), 201 under key identity.

## V3 — grant matrix
Missing/inactive/revoked/mismatched grant → 403, zero rows in `withdrawal_requests`;
grant reuse across a second key → 403-path (T-auth-bound), first row untouched.
Supply entry: `withdrawal-authz supply` (idempotent re-supply on same full params;
same id + differ → refused, row untouched); `withdrawal-authz revoke` (active → revoked;
already-revoked → idempotent success; bound requests keep rows).
Revocation interleaved with first receipt: revoke-committed-before-snapshot → 403 zero rows;
revoke-committed-after-snapshot → receipt stands ("snapshot时刻有效"), subsequent replays 200.

## V4 — param matrix
Wrong chain / non-whitelist asset / bad address shape / mixed-case failing EIP-55 /
`"0"`, `"00123"`, `"-5"`, `"1.5"`, non-digits, > uint256 → 400/422 per contract, zero rows.
Mixed-case passing EIP-55 → accepted, stored lowercase; re-POST same key → 200 same row.

## V5 — idempotency matrix
Same key + equal params → 200 same `request_id`, row count unchanged; same key + any param or
grant differ → 409, original unchanged; different caller + same key string → independent 201.

## V6 — concurrency & crash
N-way parallel same-key POSTs → exactly 1 row, all callers see the same `request_id`;
parallel same-grant different-key POSTs → exactly 1 row, losers 403-path;
dual-constraint race (same key + bound-elsewhere grant in one INSERT) → response follows
fixed-order classify (key-hit equality → 200; key-hit inequality → 409), never report order;
kill -9 between COMMIT and response → retry same key → 200 original;
restart → retry → 200 original; storage down → 503, zero Accepted;
uncertain COMMIT → re-classify (hit → 200/409/403 per order; miss → retryable, never "not created").

## V7 — rotation & revocation
Rotate key (same `caller_id`) → old key 401 (or dual-accept inside grace), new key works,
old rows still queryable, idempotency scope unchanged; revoke grant → new creates 403,
existing Accepted rows unaffected, replays still 200.

## V8 — recovery period
With an active 006 recovery row: compliant POST → persisted, zero nonce/sign/broadcast
artefacts (assert via absence: no new tables/rows outside 007 scope, no RPC broadcast);
GET → facts servable with live `recovery.state` (fresh LoadRecoveryState + RecoveryReleased
read per request; kill the read path in test → `state: unknown`, same body, still 200);
direct unit assertion that no recovery governance rows were written or deleted by 007 paths.
POST-then-lost-response during recovery → same-key retry → 200 with identical `request_id`;
recovery read failure never alters replay outcome.

## V9 — cross-caller privacy
Caller B GET caller A's `request_id` → 404 byte-identical shape to random-id 404.
