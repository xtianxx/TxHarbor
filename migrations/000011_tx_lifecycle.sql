-- +goose Up
-- TxHarbor 010-transaction-lifecycle: attempt lifecycle storage
-- (specs/010-transaction-lifecycle/data-model.md, Tables 1-6; research
-- R-010-01..14; contracts/send-api.md, send-gate.md, persistence.md).
--
-- 010 owns six tables and nothing else: it NEVER writes, copies, or extends a
-- 006/007/008/009 table. Gates (006 pause/recovery, 007 grant/scope, 008
-- binding/scope/holds/registry) are read read-only through contracts/
-- send-gate.md, holding LOCK ... IN SHARE MODE / SELECT ... FOR SHARE
-- coordination locks - a lock acquisition, not a data write.
--
-- This migration is PURE DDL: no stored functions, no seed rows. Identity/
-- content columns on tx_attempts are immutable after insert (the app has no
-- UPDATE path for them); only state/revision_seq/updated_at and the state-fact
-- timestamps change, always through version-guarded updates.
--
-- Provisional number: `000011` = 010, `000012` = 011, `000010` = PB (occupied).
-- Re-verified at merge (PLAN-1); applied migrations are never rewritten.
-- No FK to 011-owned tables (payment_intents/execution_claims do not exist at
-- apply time; G-010-3); FKs are declared only to existing upstream PKs
-- `nonce_bindings(binding_id)` and `withdrawal_authorizations(authorization_id)`.
--
-- Naming rule (repo convention): every constraint consumed by ConstraintName
-- classification carries an explicit `CONSTRAINT <name>` - never rely on
-- PostgreSQL auto-naming.
--
-- Tables (dependency order):
--   Table 1 tx_attempts        (identity + immutable content + current state)
--   Table 2 tx_attempt_signings (signed bytes + hash; pre-send durable fact)
--   Table 3 tx_send_attempts   (one row per dispatch action + gate snapshot)
--   Table 4 tx_reconciliations (append-only chain observations)
--   Table 5 tx_receipts        (receipt + Transfer verdict + confirmation basis)
--   Table 6 tx_attempt_events  (append-only decision/revision log)

-- Table 1 - tx_attempts. One row per transaction attempt; identity + content
-- are immutable after insert.
CREATE TABLE tx_attempts (
    attempt_id             TEXT          NOT NULL,
    signing_request_id     TEXT          NOT NULL,
    replacement_of         TEXT,
    intent_id              TEXT          NOT NULL,
    binding_ref            TEXT          NOT NULL,
    authorization_id       TEXT          NOT NULL,
    authorization_version  BIGINT        NOT NULL,
    recovery_version       BIGINT        NOT NULL,
    chain_id               BIGINT        NOT NULL,
    sender                 TEXT          NOT NULL,
    nonce                  NUMERIC(78,0) NOT NULL,
    tx_type                INTEGER       NOT NULL,
    to_addr                TEXT          NOT NULL,
    value                  NUMERIC(78,0) NOT NULL,
    data                   BYTEA         NOT NULL,
    gas_limit              NUMERIC(78,0) NOT NULL,
    gas_price              NUMERIC(78,0),
    max_fee_per_gas        NUMERIC(78,0),
    max_priority_fee_per_gas NUMERIC(78,0),
    asset                  TEXT          NOT NULL,
    recipient              TEXT          NOT NULL,
    amount                 NUMERIC(78,0) NOT NULL,
    canonical_envelope     TEXT          NOT NULL,
    content_hash           TEXT          NOT NULL,
    state                  TEXT          NOT NULL DEFAULT 'prepared',
    revision_seq           BIGINT        NOT NULL DEFAULT 1,
    effective_at           TIMESTAMPTZ,
    confirmed_at           TIMESTAMPTZ,
    orphaned_at            TIMESTAMPTZ,
    replaced_at            TIMESTAMPTZ,
    created_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT tx_attempts_pkey PRIMARY KEY (attempt_id),
    CONSTRAINT tx_attempts_signing_request_uniq UNIQUE (signing_request_id),
    CONSTRAINT tx_attempts_replacement_fkey FOREIGN KEY (replacement_of)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_attempts_binding_fkey FOREIGN KEY (binding_ref)
        REFERENCES nonce_bindings (binding_id),
    CONSTRAINT tx_attempts_authorization_fkey FOREIGN KEY (authorization_id)
        REFERENCES withdrawal_authorizations (authorization_id),
    CONSTRAINT tx_attempts_state_check CHECK (state IN (
        'prepared', 'signed', 'sent', 'unknown',
        'effective', 'ineffective', 'confirmed', 'orphaned', 'replaced')),
    CONSTRAINT tx_attempts_attempt_shape
        CHECK (length(attempt_id) BETWEEN 1 AND 128 AND attempt_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT tx_attempts_signing_request_shape
        CHECK (length(signing_request_id) BETWEEN 1 AND 128 AND signing_request_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT tx_attempts_intent_shape
        CHECK (length(intent_id) BETWEEN 1 AND 128 AND intent_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT tx_attempts_replacement_not_self
        CHECK (replacement_of IS NULL OR replacement_of <> attempt_id),
    CONSTRAINT tx_attempts_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT tx_attempts_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT tx_attempts_to_check CHECK (to_addr ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT tx_attempts_asset_check CHECK (asset = to_addr),
    CONSTRAINT tx_attempts_recipient_check CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT tx_attempts_value_check
        CHECK (value >= 0 AND value <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT tx_attempts_amount_check
        CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT tx_attempts_nonce_check CHECK (nonce >= 0 AND nonce <= 18446744073709551615),
    CONSTRAINT tx_attempts_recovery_version_check CHECK (recovery_version >= 0),
    CONSTRAINT tx_attempts_authorization_version_check CHECK (authorization_version >= 1),
    CONSTRAINT tx_attempts_gas_limit_check CHECK (gas_limit > 0),
    CONSTRAINT tx_attempts_fee_shape_check CHECK (
        (tx_type = 0 AND gas_price IS NOT NULL
                     AND max_fee_per_gas IS NULL AND max_priority_fee_per_gas IS NULL)
        OR
        (tx_type = 2 AND gas_price IS NULL
                     AND max_fee_per_gas IS NOT NULL AND max_priority_fee_per_gas IS NOT NULL
                     AND max_priority_fee_per_gas <= max_fee_per_gas)),
    CONSTRAINT tx_attempts_content_hash_check CHECK (content_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_attempts_revision_seq_check CHECK (revision_seq > 0),
    CONSTRAINT tx_attempts_state_facts_check CHECK (
        (state <> 'confirmed' OR confirmed_at IS NOT NULL)
        AND (state <> 'orphaned'  OR orphaned_at IS NOT NULL)
        AND (state <> 'replaced'  OR replaced_at IS NOT NULL)
        AND (state NOT IN ('effective', 'confirmed') OR effective_at IS NOT NULL))
);
CREATE INDEX tx_attempts_scan_idx ON tx_attempts (state, updated_at);
CREATE INDEX tx_attempts_intent_idx ON tx_attempts (intent_id);

-- Table 2 - tx_attempt_signings. The durable authorized-to-send fact: written
-- in T2 before any dispatch; every dispatch reads signed_tx_bytes from here.
CREATE TABLE tx_attempt_signings (
    attempt_id      TEXT        NOT NULL,
    signature       TEXT        NOT NULL,
    signed_tx_bytes BYTEA       NOT NULL,
    tx_hash         TEXT        NOT NULL,
    signed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_attempt_signings_pkey PRIMARY KEY (attempt_id),
    CONSTRAINT tx_attempt_signings_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_attempt_signings_tx_hash_uniq UNIQUE (tx_hash),
    CONSTRAINT tx_attempt_signings_signature_check CHECK (signature ~ '^0x[0-9a-f]{130}$'),
    CONSTRAINT tx_attempt_signings_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_attempt_signings_bytes_check CHECK (octet_length(signed_tx_bytes) > 0)
);

-- Table 3 - tx_send_attempts. One row per dispatch action; append-only (rows
-- are never updated). outcome is send-level evidence, never a payment verdict.
CREATE TABLE tx_send_attempts (
    send_id                  BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id               TEXT        NOT NULL,
    send_seq                 INTEGER     NOT NULL,
    kind                     TEXT        NOT NULL,
    outcome                  TEXT        NOT NULL,
    rpc_class                TEXT        NOT NULL DEFAULT '',
    observed_recovery_version BIGINT     NOT NULL,
    observed_pause           TEXT        NOT NULL DEFAULT 'none',
    observed_claim_version   BIGINT,
    observed_claim_expiry    TIMESTAMPTZ,
    observed_authorization_id TEXT,
    observed_authorization_version BIGINT,
    observed_authorization_state TEXT   NOT NULL DEFAULT '',
    observed_expires_at      TIMESTAMPTZ,
    observed_now             TIMESTAMPTZ NOT NULL,
    observed_binding_state   TEXT        NOT NULL DEFAULT '',
    dispatched_at            TIMESTAMPTZ,
    recorded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_send_attempts_pkey PRIMARY KEY (send_id),
    CONSTRAINT tx_send_attempts_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_send_attempts_attempt_seq_uniq UNIQUE (attempt_id, send_seq),
    CONSTRAINT tx_send_attempts_seq_check CHECK (send_seq > 0),
    CONSTRAINT tx_send_attempts_kind_check CHECK (kind IN ('initial', 'replay')),
    CONSTRAINT tx_send_attempts_outcome_check CHECK (outcome IN ('accepted', 'rejected', 'unknown')),
    CONSTRAINT tx_send_attempts_dispatched_check
        CHECK (outcome = 'unknown' OR dispatched_at IS NOT NULL)
);
CREATE INDEX tx_send_attempts_attempt_idx ON tx_send_attempts (attempt_id, send_seq);

-- Table 4 - tx_reconciliations. Append-only chain observations; never edited.
CREATE TABLE tx_reconciliations (
    reconcile_id   BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id     TEXT        NOT NULL,
    tx_hash        TEXT        NOT NULL,
    classification TEXT        NOT NULL,
    block_number   BIGINT,
    block_hash     TEXT,
    rpc_class      TEXT        NOT NULL DEFAULT '',
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_reconciliations_pkey PRIMARY KEY (reconcile_id),
    CONSTRAINT tx_reconciliations_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_reconciliations_classification_check CHECK (classification IN (
        'found_pending', 'included', 'not_found_yet', 'unavailable')),
    CONSTRAINT tx_reconciliations_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_reconciliations_block_hash_check
        CHECK (block_hash IS NULL OR block_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_reconciliations_block_pair_check
        CHECK ((block_number IS NULL) = (block_hash IS NULL)),
    CONSTRAINT tx_reconciliations_included_block_check
        CHECK (classification <> 'included' OR block_number IS NOT NULL)
);
CREATE INDEX tx_reconciliations_attempt_idx ON tx_reconciliations (attempt_id, observed_at);

-- Table 5 - tx_receipts. Receipt + expected-Transfer verdict + confirmation
-- basis; one row per (tx_hash, block_hash), repeat observation converges.
CREATE TABLE tx_receipts (
    receipt_id        BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id        TEXT        NOT NULL,
    tx_hash           TEXT        NOT NULL,
    status            INTEGER     NOT NULL,
    block_number      BIGINT      NOT NULL,
    block_hash        TEXT        NOT NULL,
    effect            TEXT        NOT NULL,
    transfer_detail   TEXT        NOT NULL DEFAULT '',
    canonicality      TEXT        NOT NULL DEFAULT 'unverified',
    confirmations     NUMERIC(78,0) NOT NULL DEFAULT 0,
    confirm_threshold BIGINT      NOT NULL,
    confirm_policy_seq BIGINT     NOT NULL,
    confirm_tip_number BIGINT,
    confirm_tip_hash   TEXT,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at      TIMESTAMPTZ,
    orphaned_at       TIMESTAMPTZ,
    CONSTRAINT tx_receipts_pkey PRIMARY KEY (receipt_id),
    CONSTRAINT tx_receipts_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_receipts_tx_block_uniq UNIQUE (tx_hash, block_hash),
    CONSTRAINT tx_receipts_status_check CHECK (status IN (0, 1)),
    CONSTRAINT tx_receipts_effect_check CHECK (effect IN (
        'effective', 'ineffective_status',
        'ineffective_transfer_missing', 'ineffective_transfer_mismatch')),
    CONSTRAINT tx_receipts_effect_status_check
        CHECK ((effect = 'ineffective_status') = (status = 0)
               AND (effect <> 'effective' OR status = 1)),
    CONSTRAINT tx_receipts_canonicality_check CHECK (canonicality IN (
        'unverified', 'canonical', 'orphaned')),
    CONSTRAINT tx_receipts_orphan_facts_check CHECK (
        (canonicality <> 'orphaned' OR orphaned_at IS NOT NULL)
        AND (canonicality <> 'canonical' OR orphaned_at IS NULL)),
    CONSTRAINT tx_receipts_block_number_check CHECK (block_number >= 0),
    CONSTRAINT tx_receipts_block_hash_check CHECK (block_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_receipts_confirmations_check
        CHECK (confirmations >= 0 AND confirmations = floor(confirmations)),
    CONSTRAINT tx_receipts_threshold_check CHECK (confirm_threshold > 0),
    CONSTRAINT tx_receipts_policy_seq_check CHECK (confirm_policy_seq > 0),
    CONSTRAINT tx_receipts_confirmed_at_check
        CHECK (confirmed_at IS NULL OR canonicality <> 'unverified')
);
CREATE INDEX tx_receipts_attempt_idx ON tx_receipts (attempt_id, observed_at);
CREATE INDEX tx_receipts_open_idx ON tx_receipts (canonicality, block_number)
    WHERE canonicality <> 'orphaned';

-- Table 6 - tx_attempt_events. Append-only decision/revision log; attempt_id
-- carries NO FK (007/009 audit precedent) so a refusal that never produced a
-- row stays attributable. Rows are never updated or deleted.
CREATE TABLE tx_attempt_events (
    event_id       BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id     TEXT        NOT NULL,
    event_seq      BIGINT      NOT NULL,
    event          TEXT        NOT NULL,
    reason_class   TEXT        NOT NULL DEFAULT '',
    recovery_version BIGINT,
    detail         TEXT        NOT NULL DEFAULT '',
    at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_attempt_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT tx_attempt_events_attempt_seq_uniq UNIQUE (attempt_id, event_seq),
    CONSTRAINT tx_attempt_events_seq_check CHECK (event_seq > 0),
    CONSTRAINT tx_attempt_events_event_check CHECK (event IN (
        'created', 'replayed', 'attempt_conflict',
        'signature_persisted', 'signature_refused', 'signature_mismatch',
        'gate_refused', 'send_rejected', 'send_unknown',
        'reconcile_observed', 'receipt_verified', 'receipt_ineffective',
        'confirmed', 'orphaned', 'reconfirmed', 'replaced', 'unknown_cleared'))
);
CREATE INDEX tx_attempt_events_attempt_idx ON tx_attempt_events (attempt_id, event_seq);

-- +goose Down
-- Drop only 010 objects, in exact reverse FK-dependency order: every other
-- table references tx_attempts, so tx_attempts goes last. Indexes drop with
-- their tables; the identity sequences are table-owned. Authoring/scratch
-- hygiene only (cf. 000009 Down); 010 rows are audit evidence in production.
DROP TABLE IF EXISTS tx_attempt_events;
DROP TABLE IF EXISTS tx_receipts;
DROP TABLE IF EXISTS tx_reconciliations;
DROP TABLE IF EXISTS tx_send_attempts;
DROP TABLE IF EXISTS tx_attempt_signings;
DROP TABLE IF EXISTS tx_attempts;
