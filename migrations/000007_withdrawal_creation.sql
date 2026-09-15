-- +goose Up
-- TxHarbor 007-withdrawal-creation: receive-only withdrawal intake storage
-- (specs/007-withdrawal-creation/data-model.md, Tables 1-6; research R1-R9).
--
-- 007 is a receive-only intake writer on top of 002-006 truths: it never
-- redefines upstream semantics, never allocates nonces, never signs, never
-- broadcasts. This migration is PURE DDL (R9): grant-supply logic lives in the
-- app's parameterized SQL (the `withdrawal-authz` command), never in stored
-- functions.
--
-- Invariants enforced here by storage:
--   Table 1 `caller` (stable business identity): caller_id is operator-
--       assigned BIGINT (NO identity) and survives credential rotation; rows
--       are never deleted. No chain_id: single deployment, single chain.
--   Table 2 `api_key` (credential, many per caller over time): key_hash is
--       sha256(key) hex, FULLY unique so a revoked hash can never be
--       re-registered; revoked_at NULL = active, future ts = rotation grace.
--   Table 3 `withdrawal_requests` (the Accepted request): replay-or-409 is
--       carried by CONSTRAINT withdrawal_requests_caller_key_uniq; one-auth-
--       to-one-request by CONSTRAINT withdrawal_requests_authorization_uniq;
--       status is fixed at 'accepted' (007 writes no other); amount is a
--       NUMERIC(78,0) uint256 bounded >= 1 AND <= 2^256-1.
--   Table 4 `withdrawal_authorizations` (upstream grant supply; 007 reads,
--       never approves): PK is explicitly named
--       CONSTRAINT withdrawal_authorizations_pkey (never PG auto-naming;
--       T006 probes the exact ConstraintName); state lives here.
--   Table 5 `withdrawal_request_audit` (append-only receipt log): request_id
--       carries NO FK (audit constrains nothing and outlives nothing).
--   Table 6 `withdrawal_grant_audit` (append-only grant-supply log, R9
--       carrier; mirrors deposit_pause_audit): attempt dedup is carried ONLY
--       by the named CONSTRAINT withdrawal_grant_audit_operation_id_uniq;
--       authorization_id carries NO FK (a grant row may never exist for a
--       refused supply, and audit keys unbound grants).
--
-- Naming rule (F3/F4): every constraint consumed by ConstraintName
-- classification carries an explicit `CONSTRAINT <name>` below — never rely
-- on PostgreSQL auto-naming. Short forms (`caller_key_uniq`,
-- `authorization_uniq`, `operation_id_uniq`) are PROSE SHORTHANDS ONLY and
-- never appear as SQL identifiers. The four exact names T006 matches:
--   withdrawal_requests_caller_key_uniq
--   withdrawal_requests_authorization_uniq
--   withdrawal_grant_audit_operation_id_uniq
--   withdrawal_authorizations_pkey
--
-- Conventions follow 004/005/006: lowercase 0x-prefixed hex addresses,
-- NUMERIC integer amounts, named UNIQUE constraints (pgx-mappable),
-- append-only audit, TIMESTAMPTZ DEFAULT now(). Timeout/transaction behavior
-- is app-owned (T-accept etc.); migrations stay pure DDL.

-- Table 1 — caller (stable business identity; credentials come and go).
-- caller_id is operator-assigned: NO identity, CHECK (> 0). Rows are never
-- deleted (FR-11 spirit), so the three FKs below can never dangle.
CREATE TABLE caller (
    caller_id  BIGINT      NOT NULL,
    label      TEXT        NOT NULL DEFAULT '',
    can_create BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT caller_pkey PRIMARY KEY (caller_id),
    CONSTRAINT caller_caller_id_check CHECK (caller_id > 0)
);

-- Table 2 — api_key (credential; many rows per caller over time). key_hash is
-- the auth lookup key (R4): full UNIQUE means a revoked hash can never be
-- re-registered. key_prefix is display/audit only, never auth input.
CREATE TABLE api_key (
    key_id     BIGINT      GENERATED ALWAYS AS IDENTITY,
    caller_id  BIGINT      NOT NULL,
    key_hash   CHAR(64)    NOT NULL,
    key_prefix TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    CONSTRAINT api_key_pkey PRIMARY KEY (key_id),
    CONSTRAINT api_key_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES caller (caller_id),
    CONSTRAINT api_key_key_hash_uniq UNIQUE (key_hash),
    CONSTRAINT api_key_key_hash_check CHECK (key_hash ~ '^[0-9a-f]{64}$')
);

-- Table 3 — withdrawal_requests (the Accepted request; receive-only).
-- Both uniqueness carriers are single-index inserts (atomic, serializing):
-- the (caller_id, idempotency_key) UNIQUE is the replay-or-409 carrier (R6);
-- the authorization_id UNIQUE is the one-auth-to-one-request carrier (R7).
-- status is fixed 'accepted'; amount is uint256-bounded at storage as depth
-- defense behind validate.go + the Go semantic range check.
CREATE TABLE withdrawal_requests (
    id               BIGINT        GENERATED ALWAYS AS IDENTITY,
    request_id       TEXT          NOT NULL,
    caller_id        BIGINT        NOT NULL,
    idempotency_key  TEXT          NOT NULL,
    authorization_id TEXT          NOT NULL,
    chain_id         BIGINT        NOT NULL,
    asset            TEXT          NOT NULL,
    recipient        TEXT          NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    status           TEXT          NOT NULL DEFAULT 'accepted',
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT withdrawal_requests_pkey PRIMARY KEY (id),
    CONSTRAINT withdrawal_requests_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES caller (caller_id),
    CONSTRAINT withdrawal_requests_request_id_uniq UNIQUE (request_id),
    CONSTRAINT withdrawal_requests_caller_key_uniq UNIQUE (caller_id, idempotency_key),
    CONSTRAINT withdrawal_requests_authorization_uniq UNIQUE (authorization_id),
    CONSTRAINT withdrawal_requests_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT withdrawal_requests_status_check CHECK (status = 'accepted'),
    CONSTRAINT withdrawal_requests_asset_check CHECK (asset ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT withdrawal_requests_recipient_check CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT withdrawal_requests_amount_check
        CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT withdrawal_requests_idempotency_key_check
        CHECK (length(idempotency_key) BETWEEN 1 AND 128 AND idempotency_key ~ '^[\x21-\x7e]+$')
);

-- Table 4 — withdrawal_authorizations (upstream grant supply; 007 reads,
-- never approves). PK is explicitly named per F3/F4. Invariance/validity
-- (state='active' AND (expires_at IS NULL OR expires_at > t_check)) is proven
-- by the app's FOR SHARE lock + validity SELECT; revocation/expiry live here.
CREATE TABLE withdrawal_authorizations (
    authorization_id TEXT          NOT NULL,
    caller_id        BIGINT        NOT NULL,
    chain_id         BIGINT        NOT NULL,
    asset            TEXT          NOT NULL,
    recipient        TEXT          NOT NULL,
    amount           NUMERIC(78,0) NOT NULL,
    state            TEXT          NOT NULL,
    expires_at       TIMESTAMPTZ,
    supplied_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    supplied_by      TEXT          NOT NULL DEFAULT '',
    CONSTRAINT withdrawal_authorizations_pkey PRIMARY KEY (authorization_id),
    CONSTRAINT withdrawal_authorizations_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES caller (caller_id),
    CONSTRAINT withdrawal_authorizations_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT withdrawal_authorizations_state_check
        CHECK (state IN ('active', 'revoked', 'expired')),
    CONSTRAINT withdrawal_authorizations_asset_check CHECK (asset ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT withdrawal_authorizations_recipient_check CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT withdrawal_authorizations_amount_check
        CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)
);

-- Table 5 — withdrawal_request_audit (append-only receipt log; rows are
-- never updated or deleted, FR-11). request_id carries NO FK: audit
-- constrains nothing and must attribute never-created rejects (a rej-...
-- marker) with no withdrawal_requests identity. Written in the same tx as
-- the request insert (success path) or best-effort single-statement (pre-tx
-- rejects with a known caller_id).
CREATE TABLE withdrawal_request_audit (
    audit_id    BIGINT      GENERATED ALWAYS AS IDENTITY,
    request_id  TEXT        NOT NULL,
    caller_id   BIGINT      NOT NULL,
    action      TEXT        NOT NULL,
    detail      TEXT        NOT NULL DEFAULT '',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT withdrawal_request_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT withdrawal_request_audit_action_check
        CHECK (action IN ('created', 'replayed', 'conflict', 'rejected', 'auth_failed', 'unavailable'))
);

-- Table 6 — withdrawal_grant_audit (append-only grant-supply log; R9 carrier,
-- shape mirrors deposit_pause_audit). Uniqueness is carried ONLY by the named
-- CONSTRAINT withdrawal_grant_audit_operation_id_uniq (never inline):
-- operation_id is the single dedup key, caller-supplied per attempt.
-- authorization_id carries NO FK — a grant row may never exist for a refused
-- supply, so unbound-grant audit keys here, not Table 5. operator/reason are
-- retry metadata, not bound op-input.
CREATE TABLE withdrawal_grant_audit (
    audit_id         BIGINT      GENERATED ALWAYS AS IDENTITY,
    operation_id     TEXT        NOT NULL,
    authorization_id TEXT        NOT NULL,
    caller_id        BIGINT      NOT NULL,
    action           TEXT        NOT NULL,
    operator         TEXT        NOT NULL DEFAULT '',
    reason           TEXT        NOT NULL DEFAULT '',
    detail           TEXT        NOT NULL DEFAULT '',
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT withdrawal_grant_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT withdrawal_grant_audit_operation_id_uniq UNIQUE (operation_id),
    CONSTRAINT withdrawal_grant_audit_action_check
        CHECK (action IN ('supplied', 'resupplied', 'supply_refused', 'revoked', 'revoke_nop'))
);

-- +goose Down
-- Drop only 007 objects, reverse FK-dependency order: caller is referenced by
-- api_key / withdrawal_requests / withdrawal_authorizations, so it goes last.
-- Identity sequences are table-owned and drop with their tables; no explicit
-- DROP SEQUENCE is needed. Authoring/scratch hygiene only (cf. 000004/000006
-- Down); 007 rows are append-only audit evidence.
DROP TABLE IF EXISTS withdrawal_grant_audit;
DROP TABLE IF EXISTS withdrawal_request_audit;
DROP TABLE IF EXISTS withdrawal_authorizations;
DROP TABLE IF EXISTS withdrawal_requests;
DROP TABLE IF EXISTS api_key;
DROP TABLE IF EXISTS caller;
