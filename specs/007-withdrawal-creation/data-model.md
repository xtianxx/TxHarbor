# Data Model: 007-withdrawal-creation

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15 | **Migration**: `migrations/000007_withdrawal_creation.sql`
(goose, new file; 000001–000006 untouched in meaning. `embed.go` auto-includes `*.sql`.)

007 is a receive-only intake writer on top of 002–006 truths: it never redefines upstream
semantics, never allocates nonces, never signs, never broadcasts. All decisions in research.md
(R1–R8). Conventions follow 004/005/006: lowercase `0x`-prefixed hex for addresses
(`~ '^0x[0-9a-f]{40}$'`), `NUMERIC` integer amounts, named UNIQUE constraints (pgx-mappable),
append-only audit, `TIMESTAMPTZ DEFAULT now()`.

Canonical input normalization (applied once, before compare AND before persist, so stored rows
are already canonical): amount must already match `[1-9][0-9]*` (FR-06 rejects anything else —
no normalization step exists); addresses lowercased after EIP-55 validation (FR-07); key used
verbatim (FR-09: no trim/convert). Stored rows therefore compare by equality.

## Table 1 — `caller` (stable business identity; credentials come and go, this row persists)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| caller_id | BIGINT | PK, `CHECK (> 0)` | stable identity; never derived from a key; survives rotation |
| label | TEXT | `NOT NULL DEFAULT ''` | operator display name; no secrecy |
| can_create | BOOLEAN | `NOT NULL DEFAULT TRUE` | fixed interface permission (FR-03b); FALSE = 403 on create, query unaffected |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | permission changes bump this; audited via Table 4 |

No `chain_id`: single deployment single chain (FR-04); identity is deployment-wide.
`withdrawals.caller_id` FK → this table (R3). Rows are never deleted (FR-11 spirit).

## Table 2 — `api_key` (credential; many rows per caller over time)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| key_id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | credential identity; never business identity |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | rotation keeps this constant (R3) |
| key_hash | CHAR(64) | `NOT NULL CHECK ~ '^[0-9a-f]{64}$'`, `UNIQUE` full (R1) | `sha256(key)` hex; full unique ⇒ revoked hash never re-registered |
| key_prefix | TEXT | `NOT NULL` | display/audit only (e.g. `txh_…abcd`); never auth input (R2) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| revoked_at | TIMESTAMPTZ | NULL = active | immediate revoke = `now()`; rotation grace = future ts (R4) |

Auth lookup (single statement, R4):
`SELECT key_id, caller_id FROM api_key WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())`.
Key format: `txh_` + base64url(32 CSPRNG bytes); presented via `Authorization: Bearer` (FR-03).

## Table 3 — `withdrawal_requests` (the Accepted request; receive-only)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | internal surrogate; never exposed as the idempotency signal |
| request_id | TEXT | `NOT NULL UNIQUE` | opaque public id returned to caller (201/200) |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | from auth (FR-03), never client-claimed |
| idempotency_key | TEXT | `NOT NULL CHECK (length BETWEEN 1 AND 128 AND key ~ '^[\x21-\x7e]+$')` | opaque, case-sensitive, verbatim (FR-09) |
| authorization_id | TEXT | `NOT NULL` | upstream per-attempt grant id (FR-03b) |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | must equal deployment chain (FR-04) |
| asset | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | canonical lowercase; whitelist-checked pre-insert (FR-05) |
| recipient | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | canonical lowercase (FR-07) |
| amount | NUMERIC(78,0) | `NOT NULL CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)` (literal = 2²⁵⁶−1; §四 DB upper bound) | uint256 integer; transport form `[1-9][0-9]*` (FR-06); never float |
| status | TEXT | `NOT NULL DEFAULT 'accepted' CHECK (= 'accepted')` | 007 writes no other status; Accepted = received, not executed (FR-08) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| CONSTRAINT `withdrawal_requests_caller_key_uniq` | `UNIQUE (caller_id, idempotency_key)` | replay-or-409 carrier (R6) | |
| CONSTRAINT `withdrawal_requests_authorization_uniq` | `UNIQUE (authorization_id)` | one-auth→one-request carrier (R7) | |

`NUMERIC(78,0)` holds the full uint256 range (78 decimal digits) as an integer; CHECK `> 0`
rejects zero at the storage layer as well. Layered amount enforcement (§四, all three — capacity alone is not validation):
(1) transport-shape reject in `validate.go` — must match `[1-9][0-9]*`, which already
excludes zero, leading zeros, signs, decimals, exponents, blanks (FR-06); decimals therefore
die at parse, never as values: `NUMERIC(78,0)` is NOT relied on to reject fraction input,
because driver conversion could round before the CHECK ever sees it — the shape regex is the
decimal barrier; (2) semantic range check in Go — value ≤ 2²⁵⁶−1 via `math/big` before INSERT
(FR-06; over-length digit strings die here, never reaching the DB); (3) storage CHECK
`amount >= 1 AND amount <= 2²⁵⁶−1` (literal in the Table 3 row above) as depth defense against
any future second writer that bypasses `validate.go`. Positive verification both directions
(future tests): max-uint256 accepted end-to-end; max-uint256+1 rejected at layer (2) with zero
rows; `1.5`/`1e3` rejected at layer (1) with zero rows. No nonce/signature/broadcast columns exist by design
(FR-19); 008+ add their own tables keyed off `request_id` (Downstream Handoff, no FK from 007 side).

## Table 4 — `withdrawal_authorizations` (upstream grant supply; 007 reads, never approves)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| authorization_id | TEXT | PK | upstream-issued grant id (FR-03b) |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | grant bound to one caller |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| asset | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | |
| recipient | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | |
| amount | NUMERIC(78,0) | `NOT NULL CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)` (literal = 2²⁵⁶−1; same §四 bound) | |
| state | TEXT | `NOT NULL CHECK IN ('active','revoked','expired')` | revocation/expiry live here |
| expires_at | TIMESTAMPTZ | NULL = no expiry | |
| supplied_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | audit: when upstream supplied it |
| supplied_by | TEXT | `NOT NULL DEFAULT ''` | audit: controlled-supply identity (R5-adjacent) |

Supply path (locked 2026-09-15定向收尾 — R9, see research.md): the `txharbor withdrawal-authz`
command (same controlled-script carrier as T024 `depositauth.go:1-32`: repository-owned,
version-controlled parameterized SQL in one explicit BEGIN..COMMIT over the DB operator's
connection; NOT a stored function; migrations stay pure DDL). Intake locks the grant row
`FOR SHARE` and validates `state='active' AND (expires_at IS NULL OR expires_at > now())`
plus full param equality in-tx (R7 FINAL, sole lock on the intake path); mismatch/inactive
→ 403, zero persistence. Revoke takes `FOR UPDATE`/UPDATE on the same row (three-case
interleave proof in research R7).

## Table 5 — `withdrawal_request_audit` (append-only receipt log)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| audit_id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | |
| request_id | TEXT | `NOT NULL` (no FK — audit outlives nothing but constrains nothing) | mirrors 004 pause-audit precedent |
| caller_id | BIGINT | `NOT NULL` | |
| action | TEXT | `NOT NULL CHECK IN ('created','replayed','conflict','rejected','auth_failed','unavailable')` | receipt vocabulary |
| detail | TEXT | `NOT NULL DEFAULT ''` | redacted key-value fragments (no secrets; `logx.Redact` funnel) |
| recorded_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

Written in the same tx as the request insert (R8); replay/conflict/reject paths append their
own rows. Never updated or deleted (FR-11).

## Table 6 — `withdrawal_grant_audit` (append-only grant-supply log; R9 carrier)

Shape mirrors `deposit_pause_audit` (`migrations/000004_deposit_detection.sql:109-122`):
chain-agnostic here (single deployment), keyed by grant id + action.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| audit_id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | |
| authorization_id | TEXT | `NOT NULL` (no FK — audit must survive and must key unbound grants; a grant row may never exist for a refused supply) | §三: unbound-grant audit keys here, not Table 5 |
| caller_id | BIGINT | `NOT NULL` | grant's caller (or attempted caller on refused supply) |
| action | TEXT | `NOT NULL CHECK IN ('supplied','resupplied','supply_refused','revoked','revoke_nop')` | supply vocabulary |
| operator | TEXT | `NOT NULL DEFAULT ''` | declared supply identity (audit claim; trust root is DSN possession, R9) |
| reason | TEXT | `NOT NULL DEFAULT ''` | |
| detail | TEXT | `NOT NULL DEFAULT ''` | redacted params snapshot (no secrets) |
| recorded_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| CONSTRAINT `withdrawal_grant_audit_uniq` | `UNIQUE (authorization_id, action, grant_state_seq)` — see attempt semantics below; `recorded_at` is deliberately NOT in the key (timestamps never converge) | idempotent actions converge; distinct attempts stay distinct | |
| grant_state_seq | BIGINT | `NOT NULL DEFAULT 0` | stable operation identity: supply-family sequence for this grant (0 = first supply; increments only on param-changing re-supply, which is refused — so in practice 0 for supplied/resupplied/supply_refused); revoke-family sequence (increments per revoke attempt on an already-revoked grant → distinct `revoke_nop` rows) — derivation rule in attempt semantics; NOT wall-clock |

Attempt semantics (locked 三轮定点 — an audit row records one *attempt*, not one state change;
no timestamp-based dedup is claimed):
- `supplied` (seq 0): first successful supply. Same-tx: grant INSERT + audit INSERT; both
  `RowsAffected()==1` (grant row is new, so the check holds).
- `resupplied` (seq 0): same grant id + same full params re-supplied. Same-tx: grant row
  verified equal (zero mutation — NO `RowsAffected()==1` on the grant row; assert `0`) + audit
  INSERT (`==1`). The UNIQUE key `(G,'resupplied',0)` converges concurrent duplicate re-supplies:
  one commits, losers get 23505 on the audit constraint → they re-read the grant row (still equal)
  and return the same success WITHOUT a second audit row. This is the only dedup the UNIQUE provides.
- `supply_refused` (seq 0): same id + any param differ. Same-tx: zero grant mutation + audit
  INSERT. Refused attempts MUST NOT roll back with the business refusal — the refusal and its
  audit commit together (the tx commits a no-mutation + audit row; "refused" is the committed
  outcome, not a rollback). No illegal grant data is ever written: the audit `detail` carries the
  *attempted* params snapshot, the grant table carries nothing.
- `revoked` (seq = revoke generation of the grant row, read under lock): active → revoked +
  audit in one tx, both `==1`.
- `revoke_nop` (seq = per-attempt counter: `SELECT count(*) FROM audit WHERE id=G AND action IN
  ('revoked','revoke_nop')` + 1, computed in-tx): every repeat revoke attempt records its own row —
  attempts stay distinct BY DESIGN (no convergence claimed). Concurrent duplicate revokes each
  record their own `revoke_nop`; both report idempotent success.
- Uncertain COMMIT on supply/revoke: re-read the grant row (durable state decides the report);
  the audit row's presence/absence is reconciled by re-issuing the same action — `supplied` and
  `revoked` converge via their UNIQUE keys on retry; `revoke_nop` appends (harmless duplicate
  evidence, never a state change).

Verifiable outcomes: successful supply → row + `supplied`; idempotent re-supply (same full
params) → `resupplied` (grant untouched, `RowsAffected()==0` asserted); same-id异参 →
`supply_refused` committed with zero grant mutation; revoke → `revoked`; repeat revoke →
distinct `revoke_nop` rows, idempotent success; uncertain COMMIT → re-read + same-action retry.

## Transaction catalog (behavioral; SQL in contracts/)

- **T-accept** (first receipt): pre-tx classify (fast path) → BEGIN → writeGuard
  (`SET LOCAL statement_timeout='5s'`, per-statement timeout only — NOT a recovery gate;
  007 persists during active recovery per FR-16) → `FOR SHARE` grant row + validate
  (DB-`now()` expiry evaluated post-lock; 403 + ROLLBACK on fail) → plain INSERT request →
  INSERT audit → COMMIT; 23505 → rollback → fixed-order classify → 200/409/403;
  commit-error → re-classify.
- **T-replay**: classify hit + FR-10 equality → return original + `replayed` audit (no new request).
- **T-conflict**: classify hit + any business-param/auth-id inequality → 409 + `conflict` audit,
  original untouched.
- **T-auth-bound**: 23505 on `authorization_uniq` → fixed-order classify (R8 FINAL: key first,
  then auth) → miss-on-key + hit-on-auth → 403 + `auth_failed` audit, zero new rows.
- **T-reject**: auth/param failure pre- or in-tx → typed 400/401/403/422 + `rejected` audit,
  zero persistence.
- **T-unavailable**: storage failure → 503 + `unavailable` audit attempt (best-effort; MUST NOT
  return Accepted).
- **T-dual-race**: both UNIQUEs violated at once → single reported `ConstraintName` is NOT
  semantic; fixed-order classify decides (key-hit equality → 200; key-hit inequality → 409;
  else auth-hit → 403; else retryable). Never derive the response from report order.

## Concurrency argument (why two writers cannot both win; R7 FINAL)

Both uniqueness carriers are single index inserts — atomic and serializing by construction.
Concurrent same-key writers: one INSERT commits, losers get 23505 on `caller_key_uniq` and
classify to the winner (R6). Concurrent same-auth different-key writers: one commits, losers
get 23505 on `authorization_uniq` → fixed-order classify → 403 (R7). Auth-validity itself is
proven by the `FOR SHARE` grant-row lock + validity SELECT in the receipt tx (three-case
interleave proof in research R7 — the earlier "shared snapshot" wording is withdrawn);
per-request key/permission checks prove startpoint freshness (no whole-request immediacy
claim); post-rollback classify proves loss/uncertain recovery. One lock object total on the
intake path. Restart safety follows: all guarantees are
durable rows; a retry after crash/response-loss replays against the same constraints (FR-13).
