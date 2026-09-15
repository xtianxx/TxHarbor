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
| amount | NUMERIC(78,0) | `NOT NULL CHECK (> 0)` | uint256 integer; transport form `[1-9][0-9]*` (FR-06); never float |
| status | TEXT | `NOT NULL DEFAULT 'accepted' CHECK (= 'accepted')` | 007 writes no other status; Accepted = received, not executed (FR-08) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| CONSTRAINT `withdrawal_requests_caller_key_uniq` | `UNIQUE (caller_id, idempotency_key)` | replay-or-409 carrier (R6) | |
| CONSTRAINT `withdrawal_requests_authorization_uniq` | `UNIQUE (authorization_id)` | one-auth→one-request carrier (R7) | |

`NUMERIC(78,0)` holds the full uint256 range (78 decimal digits) as an integer; CHECK `> 0`
rejects zero at the storage layer as well. No nonce/signature/broadcast columns exist by design
(FR-19); 008+ add their own tables keyed off `request_id` (Downstream Handoff, no FK from 007 side).

## Table 4 — `withdrawal_authorizations` (upstream grant supply; 007 reads, never approves)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| authorization_id | TEXT | PK | upstream-issued grant id (FR-03b) |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | grant bound to one caller |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| asset | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | |
| recipient | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | |
| amount | NUMERIC(78,0) | `NOT NULL CHECK (> 0)` | |
| state | TEXT | `NOT NULL CHECK IN ('active','revoked','expired')` | revocation/expiry live here |
| expires_at | TIMESTAMPTZ | NULL = no expiry | |
| supplied_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | audit: when upstream supplied it |
| supplied_by | TEXT | `NOT NULL DEFAULT ''` | audit: controlled-supply identity (R5-adjacent) |

Supply path: controlled, auditable operator/upstream entry (FR-03b — exact carrier in plan;
ordinary API callers MUST NOT write here; enforced by privilege separation, documented in plan).
Intake validates `state='active' AND (expires_at IS NULL OR expires_at > now())` plus full
param equality in-tx; mismatch/inactive → 403, zero persistence. `FOR SHARE` on this row only
if the plan locks the R7 strictness choice (default: plain read + constraint carrier).

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

## Transaction catalog (behavioral; SQL in contracts/)

- **T-accept** (first receipt): pre-tx classify (fast path) → BEGIN → writeGuard →
  validate auth in-tx → plain INSERT request → INSERT audit → COMMIT;
  23505 → rollback → classify by `ConstraintName` → 200/409/403; commit-error → re-classify.
- **T-replay**: classify hit + FR-10 equality → return original + `replayed` audit (no new request).
- **T-conflict**: classify hit + any business-param/auth-id inequality → 409 + `conflict` audit,
  original untouched.
- **T-auth-bound**: 23505 on `authorization_uniq` → classify by `authorization_id` → 403
  (FR-14 mapping; plan confirms) + `auth_failed` audit, zero new rows.
- **T-reject**: auth/param failure pre- or in-tx → typed 400/401/403/422 + `rejected` audit,
  zero persistence.
- **T-unavailable**: storage failure → 503 + `unavailable` audit attempt (best-effort; MUST NOT
  return Accepted).
- **Coordinator-lock variant** (only if plan locks R7 strictness): `ensureLeaseSQL` +
  `lockCoordSQL` between writeGuard and authValidation, fixed order, coordinator row first
  (shared consts, `scanner.go:1083-1098`).

## Concurrency argument (why two writers cannot both win)

Both uniqueness carriers are single index inserts — atomic and serializing by construction.
Concurrent same-key writers: one INSERT commits, losers get 23505 on `caller_key_uniq` and
classify to the winner (R6). Concurrent same-auth different-key writers: one commits, losers
get 23505 on `authorization_uniq` (R7). No app memory, no second lock object in the default
(constraints-only) variant. Restart safety follows: all guarantees are durable rows; a retry
after crash/response-loss replays against the same constraints (FR-13).
