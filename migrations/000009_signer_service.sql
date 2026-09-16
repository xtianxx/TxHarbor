-- +goose Up
-- TxHarbor 009-signer-service: isolated signer storage
-- (specs/009-signer-service/data-model.md, Tables 1-6; research R1-R10).
--
-- 009 owns six tables and nothing else: it NEVER writes, copies, or extends a
-- 006/007 table. Gates (006 indexer_pause/log_pause/deposit_pause,
-- reorg_recovery; 007 withdrawal_authorizations) are read read-only through
-- contracts/gates.md, holding LOCK ... IN SHARE MODE coordination locks (R6) —
-- a lock acquisition, not a data write.
--
-- This migration is PURE DDL: no stored functions, no seed rows. Credential
-- supply lives in the app's parameterized SQL (`signer-auth` operator
-- subcommand, R9), never here.
--
-- Invariants enforced here by storage:
--   Table 1 `signer_caller` (stable operator-assigned identity, NO identity
--       column; never deleted so audit FKs stay valid): can_sign = FALSE means
--       the caller authenticates but MUST be refused — identity is not
--       permission (FR-05; mirror of 007's can_create).
--   Table 2 `signer_credential` (many per caller over time): secret_hash is
--       sha256(secret) lowercase hex, FULLY unique so a revoked hash can never
--       be re-registered; revoked_at NULL = active. Plaintext is shown once at
--       issuance and never stored.
--   Table 3 `signing_requests` (request identity <-> full content binding):
--       (caller_id, signing_request_id) is the request identity (OC-4);
--       attempt_id is UNIQUE (one 010 attempt = one request identity);
--       replacement_of self-FK marks fee replacements; the partial unique
--       index signing_requests_authorization_anchor_uniq enforces one anchor
--       (non-replacement) request per authorization (OC-5/R7); the fee-shape
--       CHECK pins tx_type to {0, 2} closed shapes.
--   Table 4 `signature_results` (the only signing artifact ever persisted:
--       signature value + hash; NEVER raw signed-tx bytes): PK is the request
--       row (one result per identity) with an explicitly named
--       signature_results_pkey; UNIQUE tx_hash is the storage-level "no second
--       observable signature" guard (FR-14).
--   Table 5 `signing_request_audit` (append-only decision/refusal log):
--       signing_request_id carries NO FK — audit must attribute refusals that
--       never produced a request row and outlives nothing (007 Table 5
--       rationale). Rows are never updated or deleted.
--   Table 6 `delivery_admissions` (append-only per-delivery gate snapshot +
--       verdict): one row per attempt_seq (> 0) per request. Admission and
--       handoff share ONE protected region — no TTL, no grace (R6): the
--       superseded TTL revision (2026-09-16) is WITHDRAWN and MUST NOT return
--       as an authorizing mechanism; no such column exists below by design.
--
-- Naming rule (F3/F4, 007 convention): every constraint consumed by
-- ConstraintName classification carries an explicit `CONSTRAINT <name>` below —
-- never rely on PostgreSQL auto-naming. The partial unique anchor index is a
-- named CREATE UNIQUE INDEX (partial uniqueness cannot be a table constraint).
--
-- Conventions follow 004-007: named constraints, lowercase 0x-prefixed hex
-- addresses, NUMERIC(78,0) integer money, TIMESTAMPTZ DEFAULT now(),
-- append-only audit. Timeout/transaction behavior is app-owned.

-- Table 1 — signer_caller (stable caller identity; FR-04/FR-05).
-- caller_id is operator-assigned: NO identity, CHECK (> 0). Rows are never
-- deleted, so every FK below can never dangle.
CREATE TABLE signer_caller (
    caller_id  BIGINT      NOT NULL,
    label      TEXT        NOT NULL DEFAULT '',
    can_sign   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT signer_caller_pkey PRIMARY KEY (caller_id),
    CONSTRAINT signer_caller_caller_id_check CHECK (caller_id > 0)
);

-- Table 2 — signer_credential (bearer credential; many rows per caller over
-- time). secret_hash is the auth lookup key: full UNIQUE means a revoked hash
-- can never be re-registered. secret_prefix is display/audit only, never auth
-- input. Rotation = insert successor, set predecessor revoked_at.
CREATE TABLE signer_credential (
    credential_id BIGINT      GENERATED ALWAYS AS IDENTITY,
    caller_id     BIGINT      NOT NULL,
    secret_hash   CHAR(64)    NOT NULL,
    secret_prefix TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at    TIMESTAMPTZ,
    CONSTRAINT signer_credential_pkey PRIMARY KEY (credential_id),
    CONSTRAINT signer_credential_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES signer_caller (caller_id),
    CONSTRAINT signer_credential_secret_hash_uniq UNIQUE (secret_hash),
    CONSTRAINT signer_credential_secret_hash_check CHECK (secret_hash ~ '^[0-9a-f]{64}$')
);

-- Table 3 — signing_requests (request identity <-> full content binding;
-- FR-13/FR-15). Every classified conflict carries a named constraint below.
-- The fee-shape CHECK is the only tx_type constraint and admits exactly the
-- legacy (0) and EIP-1559 (2) closed shapes; access_list is storage-visible
-- as [] only (v1 policy refuses non-empty lists; widening is a policy +
-- migration change, never silent acceptance).
CREATE TABLE signing_requests (
    id                          BIGINT        GENERATED ALWAYS AS IDENTITY,
    caller_id                   BIGINT        NOT NULL,
    signing_request_id          TEXT          NOT NULL,
    attempt_id                  TEXT          NOT NULL,
    replacement_of              BIGINT,
    intent_id                   TEXT          NOT NULL,
    binding_ref                 TEXT          NOT NULL,
    recovery_version            BIGINT        NOT NULL DEFAULT 0,
    chain_id                    BIGINT        NOT NULL,
    sender                      TEXT          NOT NULL,
    nonce                       NUMERIC(78,0) NOT NULL,
    tx_type                     INTEGER       NOT NULL,
    to_addr                     TEXT          NOT NULL,
    value                       NUMERIC(78,0) NOT NULL,
    data                        BYTEA         NOT NULL,
    gas_limit                   NUMERIC(78,0) NOT NULL,
    gas_price                   NUMERIC(78,0),
    max_fee_per_gas             NUMERIC(78,0),
    max_priority_fee_per_gas    NUMERIC(78,0),
    access_list                 JSONB         NOT NULL DEFAULT '[]'::jsonb,
    asset                       TEXT          NOT NULL,
    recipient                   TEXT          NOT NULL,
    amount                      NUMERIC(78,0) NOT NULL,
    canonical_envelope          TEXT          NOT NULL,
    content_hash                TEXT          NOT NULL,
    authorization_id            TEXT          NOT NULL,
    authorization_fingerprint   TEXT          NOT NULL,
    authorization_state         TEXT          NOT NULL,
    policy_version              TEXT          NOT NULL,
    state                       TEXT          NOT NULL DEFAULT 'received',
    refusal_class               TEXT          NOT NULL DEFAULT '',
    created_at                  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT signing_requests_pkey PRIMARY KEY (id),
    CONSTRAINT signing_requests_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES signer_caller (caller_id),
    CONSTRAINT signing_requests_caller_request_uniq UNIQUE (caller_id, signing_request_id),
    CONSTRAINT signing_requests_attempt_uniq UNIQUE (attempt_id),
    CONSTRAINT signing_requests_replacement_fkey FOREIGN KEY (replacement_of)
        REFERENCES signing_requests (id),
    CONSTRAINT signing_requests_state_check
        CHECK (state IN ('received', 'validated', 'signed', 'rejected', 'failed')),
    CONSTRAINT signing_requests_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT signing_requests_recovery_version_check CHECK (recovery_version >= 0),
    CONSTRAINT signing_requests_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_to_check CHECK (to_addr ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_asset_check CHECK (asset ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_recipient_check CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_nonce_check
        CHECK (nonce >= 0 AND nonce <= 18446744073709551615),
    CONSTRAINT signing_requests_value_check
        CHECK (value >= 0 AND value <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT signing_requests_amount_check
        CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT signing_requests_gas_limit_check CHECK (gas_limit > 0),
    CONSTRAINT signing_requests_fee_shape_check CHECK (
        (tx_type = 0 AND gas_price IS NOT NULL
                     AND max_fee_per_gas IS NULL AND max_priority_fee_per_gas IS NULL)
        OR
        (tx_type = 2 AND gas_price IS NULL
                     AND max_fee_per_gas IS NOT NULL AND max_priority_fee_per_gas IS NOT NULL
                     AND max_priority_fee_per_gas <= max_fee_per_gas)
    ),
    CONSTRAINT signing_requests_access_list_check CHECK (jsonb_array_length(access_list) = 0),
    CONSTRAINT signing_requests_content_hash_check CHECK (content_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT signing_requests_authorization_fingerprint_check
        CHECK (authorization_fingerprint ~ '^[0-9a-f]{64}$')
);

-- OC-5 conditional grant reuse (research R7): one anchor (non-replacement)
-- request per authorization; replacement rows may share the grant. A persisted
-- row's authorization_id is never updated, so an old request can never be
-- rebound to another grant. Partial uniqueness = named unique INDEX (a WHERE
-- clause cannot be a table constraint).
CREATE UNIQUE INDEX signing_requests_authorization_anchor_uniq
    ON signing_requests (authorization_id) WHERE replacement_of IS NULL;

-- Table 4 — signature_results (the persisted signing result; FR-13/FR-14/FR-16).
-- Written INSIDE the signing transaction, before any response byte (FR-13).
-- Raw signed-transaction bytes are never persisted and never logged; the
-- signature value is the only broadcast-reconstructable material here.
CREATE TABLE signature_results (
    signing_request_row BIGINT      NOT NULL,
    signature           TEXT        NOT NULL,
    tx_hash             TEXT        NOT NULL,
    signed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT signature_results_pkey PRIMARY KEY (signing_request_row),
    CONSTRAINT signature_results_request_fkey FOREIGN KEY (signing_request_row)
        REFERENCES signing_requests (id),
    CONSTRAINT signature_results_tx_hash_uniq UNIQUE (tx_hash),
    CONSTRAINT signature_results_signature_check CHECK (signature ~ '^0x[0-9a-f]{130}$'),
    CONSTRAINT signature_results_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$')
);

-- Table 5 — signing_request_audit (append-only decision/refusal log;
-- FR-12/FR-22/FR-24). signing_request_id carries NO FK: audit attributes
-- refusals that never produced a request row and outlives nothing. Rows are
-- never updated or deleted.
CREATE TABLE signing_request_audit (
    audit_id          BIGINT      GENERATED ALWAYS AS IDENTITY,
    signing_request_id TEXT       NOT NULL,
    caller_id         BIGINT      NOT NULL,
    action            TEXT        NOT NULL,
    reason_class      TEXT        NOT NULL DEFAULT '',
    detail            TEXT        NOT NULL DEFAULT '',
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT signing_request_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT signing_request_audit_action_check CHECK (action IN (
        'received', 'validated', 'signed', 'rejected', 'failed', 'conflict', 'replayed',
        'gate_refused', 'binding_refused', 'authorization_refused',
        'delivery_admitted', 'delivery_blocked', 'delivery_unknown'))
);

-- Table 6 — delivery_admissions (per-delivery gate snapshot + verdict;
-- FR-17/FR-23, OC-6/OC-7). Append-only; one row per delivery attempt.
-- NO TTL column: admission and handoff share one protected region under the
-- held 006/007/008 locks (R6); the withdrawn TTL revision MUST NOT return as
-- an authorizing mechanism.
CREATE TABLE delivery_admissions (
    admission_id              BIGINT      GENERATED ALWAYS AS IDENTITY,
    signing_request_row       BIGINT      NOT NULL,
    attempt_seq               INTEGER     NOT NULL,
    verdict                   TEXT        NOT NULL,
    authorization_id          TEXT        NOT NULL,
    authorization_fingerprint TEXT        NOT NULL,
    authorization_state       TEXT        NOT NULL,
    binding_class             TEXT        NOT NULL DEFAULT '',
    can_sign                  BOOLEAN     NOT NULL DEFAULT TRUE,
    recovery_version          BIGINT      NOT NULL,
    pause_basis               TEXT        NOT NULL DEFAULT 'none',
    recovery_basis            TEXT        NOT NULL DEFAULT 'none',
    reason                    TEXT        NOT NULL DEFAULT '',
    decided_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at              TIMESTAMPTZ,
    CONSTRAINT delivery_admissions_pkey PRIMARY KEY (admission_id),
    CONSTRAINT delivery_admissions_request_fkey FOREIGN KEY (signing_request_row)
        REFERENCES signing_requests (id),
    CONSTRAINT delivery_admissions_request_attempt_uniq UNIQUE (signing_request_row, attempt_seq),
    CONSTRAINT delivery_admissions_attempt_seq_check CHECK (attempt_seq > 0),
    CONSTRAINT delivery_admissions_binding_class_check CHECK (binding_class IN (
        '', 'matches', 'absent', 'conflict', 'paused', 'terminal', 'read_failed')),
    CONSTRAINT delivery_admissions_verdict_check CHECK (verdict IN (
        'admitted', 'delivered', 'blocked', 'unknown_reconcile'))
);

-- +goose Down
-- Drop only 009 objects, reverse FK-dependency order: signer_caller is
-- referenced by signer_credential / signing_requests, and signing_requests by
-- signature_results / delivery_admissions, so those go first and signer_caller
-- last. The partial anchor index drops with signing_requests. Identity
-- sequences are table-owned and drop with their tables; no explicit DROP
-- SEQUENCE is needed. Authoring/scratch hygiene only (cf. 000004/000007 Down);
-- 009 rows are append-only audit evidence.
DROP TABLE IF EXISTS delivery_admissions;
DROP TABLE IF EXISTS signing_request_audit;
DROP TABLE IF EXISTS signature_results;
DROP TABLE IF EXISTS signing_requests;
DROP TABLE IF EXISTS signer_credential;
DROP TABLE IF EXISTS signer_caller;
