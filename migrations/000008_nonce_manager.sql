-- +goose Up
-- TxHarbor 008-nonce-manager: nonce allocation storage
-- (specs/008-nonce-manager/data-model.md, Tables 1-7; research R1-R13).
--
-- 008 allocates EVM account nonces against an explicit, durable state
-- machine. This migration is PURE DDL (FR-20/23): classification, admission,
-- holds and operator behavior live in the app's parameterized SQL; there are
-- NO stored functions.
--
-- Tables (dependency order):
--   Table 1 nonce_wallet_registry  (sender authority per chain; OC-2)
--   Table 2 nonce_scope_state      (durable per-scope frontier / reconcile floor)
--   Table 3 nonce_bindings         (the durable binding; core invariant carrier)
--   Table 4 nonce_binding_events   (append-only state-transition log)
--   Table 5 nonce_observations     (chain-view evidence; one row per attempt)
--   Table 6 nonce_scope_holds      (008-owned reconciliation pauses)
--   Table 7 nonce_ops_audit        (operator-operation audit + attempt dedup)
--
-- Naming rule: every constraint consumed by ConstraintName classification
-- carries an explicit `CONSTRAINT <name>` below -- never rely on PostgreSQL
-- auto-naming. The exact names 008 classification matches:
--   nonce_bindings_pkey, nonce_bindings_intent_uniq,
--   nonce_bindings_scope_nonce_uniq, nonce_bindings_registry_fkey,
--   nonce_bindings_state_check, nonce_bindings_terminal_consistency,
--   nonce_bindings_nonce_range, nonce_bindings_intent_shape,
--   nonce_wallet_registry_pkey, nonce_scope_state_pkey,
--   nonce_scope_state_registry_fkey, nonce_binding_events_binding_to_uniq,
--   nonce_scope_holds_status_consistency, nonce_scope_holds_registry_fkey,
--   nonce_ops_audit_pkey, nonce_ops_audit_operation_id_uniq.
--
-- Numeric convention: NUMERIC(78,0) for every uint64-domain count, bounded
-- 0..2^64-1 (R13). Addresses are lowercase 0x+40 hex; hashes 0x+64 hex.
-- Timeout/transaction behavior is app-owned; migrations stay pure DDL.

-- Table 1 -- nonce_wallet_registry (sender authority per chain; OC-2).
-- Admission gate only: `disabled` is the off state, rows are never deleted
-- (no delete path), so the FKs from Tables 2/3/6 can never dangle.
CREATE TABLE nonce_wallet_registry (
    chain_id     BIGINT      NOT NULL,
    sender       TEXT        NOT NULL,
    state        TEXT        NOT NULL,
    registry_seq BIGINT      NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nonce_wallet_registry_pkey PRIMARY KEY (chain_id, sender),
    CONSTRAINT nonce_wallet_registry_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT nonce_wallet_registry_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT nonce_wallet_registry_state_check CHECK (state IN ('active', 'disabled')),
    CONSTRAINT nonce_wallet_registry_registry_seq_check CHECK (registry_seq > 0)
);

-- Table 2 -- nonce_scope_state (durable per-scope frontier / reconcile floor).
-- A durable cache of evidence-linked facts, never an authority over bindings
-- (M is always recomputed from nonce_bindings under the lock).
CREATE TABLE nonce_scope_state (
    chain_id            BIGINT      NOT NULL,
    sender              TEXT        NOT NULL,
    reconciled_floor    NUMERIC(78,0),
    last_latest         NUMERIC(78,0),
    last_pending        NUMERIC(78,0),
    last_observation_id TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nonce_scope_state_pkey PRIMARY KEY (chain_id, sender),
    CONSTRAINT nonce_scope_state_registry_fkey FOREIGN KEY (chain_id, sender)
        REFERENCES nonce_wallet_registry (chain_id, sender),
    CONSTRAINT nonce_scope_state_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT nonce_scope_state_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT nonce_scope_state_reconciled_floor_check
        CHECK (reconciled_floor >= 0 AND reconciled_floor <= 18446744073709551615),
    CONSTRAINT nonce_scope_state_last_latest_check
        CHECK (last_latest >= 0 AND last_latest <= 18446744073709551615),
    CONSTRAINT nonce_scope_state_last_pending_check
        CHECK (last_pending >= 0 AND last_pending <= 18446744073709551615)
);

-- Table 3 -- nonce_bindings (the durable binding; core invariant carrier).
-- One binding per intent forever; one binding per (chain_id, sender, nonce)
-- forever; only a registered sender can receive bindings. The binding is
-- immutable except for state (+ terminal columns): there is no sender/nonce
-- update path, and nonces are never reassigned after release (OC-3 is
-- structural).
CREATE TABLE nonce_bindings (
    binding_id                TEXT          NOT NULL,
    intent_id                 TEXT          NOT NULL,
    chain_id                  BIGINT        NOT NULL,
    sender                    TEXT          NOT NULL,
    nonce                     NUMERIC(78,0) NOT NULL,
    state                     TEXT          NOT NULL,
    authorization_id          TEXT          NOT NULL,
    authorization_version     CHAR(64)      NOT NULL,
    registry_seq              BIGINT        NOT NULL,
    allocation_observation_id TEXT          NOT NULL,
    created_at                TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ   NOT NULL DEFAULT now(),
    consumed_at               TIMESTAMPTZ,
    released_at               TIMESTAMPTZ,
    release_operation_id      TEXT,
    CONSTRAINT nonce_bindings_pkey PRIMARY KEY (binding_id),
    CONSTRAINT nonce_bindings_intent_uniq UNIQUE (intent_id),
    CONSTRAINT nonce_bindings_scope_nonce_uniq UNIQUE (chain_id, sender, nonce),
    CONSTRAINT nonce_bindings_registry_fkey FOREIGN KEY (chain_id, sender)
        REFERENCES nonce_wallet_registry (chain_id, sender),
    CONSTRAINT nonce_bindings_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT nonce_bindings_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT nonce_bindings_state_check
        CHECK (state IN ('allocated', 'in_flight', 'consumed', 'released')),
    -- Pending states carry no terminal facts, and terminal states carry the
    -- matching fact + operation id; consumed/released are mutually exclusive.
    CONSTRAINT nonce_bindings_terminal_consistency CHECK (
        (state = 'consumed') = (consumed_at IS NOT NULL)
        AND (state = 'released') = (released_at IS NOT NULL AND release_operation_id IS NOT NULL)
    ),
    CONSTRAINT nonce_bindings_nonce_range
        CHECK (nonce >= 0 AND nonce <= 18446744073709551615),
    CONSTRAINT nonce_bindings_intent_shape
        CHECK (length(intent_id) BETWEEN 1 AND 128 AND intent_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT nonce_bindings_authorization_version_check
        CHECK (authorization_version ~ '^[0-9a-f]{64}$'),
    CONSTRAINT nonce_bindings_registry_seq_check CHECK (registry_seq > 0)
);

-- Table 4 -- nonce_binding_events (append-only state-transition log; mirrors
-- the 006 event-log shape). binding_id carries NO FK -- audit constrains
-- nothing. The named UNIQUE makes a repeat transition converge on the
-- constraint instead of duplicating history.
CREATE TABLE nonce_binding_events (
    event_id       BIGINT      GENERATED ALWAYS AS IDENTITY,
    binding_id     TEXT        NOT NULL,
    from_state     TEXT,
    to_state       TEXT        NOT NULL,
    observation_id TEXT,
    operation_id   TEXT,
    detail         TEXT        NOT NULL DEFAULT '',
    at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nonce_binding_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT nonce_binding_events_binding_to_uniq UNIQUE (binding_id, to_state),
    -- NULL from_state is the creation event; NULL passes the domain CHECK.
    CONSTRAINT nonce_binding_events_from_state_check
        CHECK (from_state IS NULL OR from_state IN ('allocated', 'in_flight', 'consumed', 'released')),
    CONSTRAINT nonce_binding_events_to_state_check
        CHECK (to_state IN ('allocated', 'in_flight', 'consumed', 'released'))
);

-- Table 5 -- nonce_observations (chain-view evidence; one row per attempt,
-- never updated, never deleted). Carries NO registry FK: evidence must be
-- recordable even for a scope refused at admission.
CREATE TABLE nonce_observations (
    observation_id TEXT          NOT NULL,
    chain_id       BIGINT        NOT NULL,
    sender         TEXT          NOT NULL,
    kind           TEXT          NOT NULL,
    classification TEXT          NOT NULL,
    latest_count   NUMERIC(78,0),
    pending_count  NUMERIC(78,0),
    head_number    NUMERIC(78,0),
    head_hash      TEXT,
    error_class    TEXT          NOT NULL DEFAULT '',
    rpc_ref        TEXT          NOT NULL DEFAULT '',
    observed_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT nonce_observations_pkey PRIMARY KEY (observation_id),
    CONSTRAINT nonce_observations_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT nonce_observations_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT nonce_observations_kind_check CHECK (kind IN ('allocation', 'reconcile')),
    CONSTRAINT nonce_observations_classification_check CHECK (classification IN (
        'consistent', 'bootstrap_external_consumed', 'unattributed_consumption',
        'unexplained_gap', 'divergence', 'unavailable')),
    CONSTRAINT nonce_observations_latest_count_check
        CHECK (latest_count >= 0 AND latest_count <= 18446744073709551615),
    CONSTRAINT nonce_observations_pending_count_check
        CHECK (pending_count >= 0 AND pending_count <= 18446744073709551615),
    CONSTRAINT nonce_observations_head_hash_check CHECK (head_hash ~ '^0x[0-9a-f]{64}$')
);

-- Access pattern: per-scope evidence history.
CREATE INDEX nonce_observations_scope_time_idx
    ON nonce_observations (chain_id, sender, observed_at);

-- Table 6 -- nonce_scope_holds (008-owned reconciliation pauses; one row per
-- cause instance). Multi-cause coexistence is by construction: several active
-- rows may exist per scope; no scope-level boolean exists.
CREATE TABLE nonce_scope_holds (
    hold_id                 TEXT        NOT NULL,
    chain_id                BIGINT      NOT NULL,
    sender                  TEXT        NOT NULL,
    cause                   TEXT        NOT NULL,
    status                  TEXT        NOT NULL,
    established_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    evidence_observation_id TEXT        NOT NULL,
    evidence_detail         TEXT        NOT NULL DEFAULT '',
    released_at             TIMESTAMPTZ,
    released_by             TEXT,
    release_operation_id    TEXT,
    release_evidence        TEXT,
    release_observation_id  TEXT,
    CONSTRAINT nonce_scope_holds_pkey PRIMARY KEY (hold_id),
    CONSTRAINT nonce_scope_holds_status_consistency
        CHECK ((status = 'active') = (released_at IS NULL)),
    CONSTRAINT nonce_scope_holds_registry_fkey FOREIGN KEY (chain_id, sender)
        REFERENCES nonce_wallet_registry (chain_id, sender),
    CONSTRAINT nonce_scope_holds_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT nonce_scope_holds_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT nonce_scope_holds_cause_check
        CHECK (cause IN ('unattributed_consumption', 'unexplained_gap', 'chain_view_divergence')),
    CONSTRAINT nonce_scope_holds_status_check CHECK (status IN ('active', 'released'))
);

-- Table 7 -- nonce_ops_audit (operator-operation audit + attempt dedup; 007
-- grant-audit protocol). Uniqueness is carried ONLY by the named
-- operation_id UNIQUE constraint (never inline): 23505 -> rollback -> re-read
-- by operation id -> equal op-input: report recorded outcome; differ:
-- operation_conflict, zero writes (R7).
CREATE TABLE nonce_ops_audit (
    audit_id     BIGINT      GENERATED ALWAYS AS IDENTITY,
    operation_id TEXT        NOT NULL,
    action       TEXT        NOT NULL,
    chain_id     BIGINT      NOT NULL,
    sender       TEXT        NOT NULL,
    subject_id   TEXT        NOT NULL DEFAULT '',
    outcome      TEXT        NOT NULL,
    operator     TEXT        NOT NULL DEFAULT '',
    reason       TEXT        NOT NULL DEFAULT '',
    evidence     TEXT        NOT NULL DEFAULT '',
    detail       TEXT        NOT NULL DEFAULT '',
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nonce_ops_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT nonce_ops_audit_operation_id_uniq UNIQUE (operation_id),
    CONSTRAINT nonce_ops_audit_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT nonce_ops_audit_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT nonce_ops_audit_action_check
        CHECK (action IN ('registry_register', 'registry_disable', 'hold_release', 'binding_release')),
    CONSTRAINT nonce_ops_audit_outcome_check
        CHECK (outcome IN ('applied', 'nop', 'refused'))
);

-- +goose Down
-- Drop only 008 objects, reverse FK-dependency order: nonce_wallet_registry is
-- referenced by nonce_scope_state / nonce_bindings / nonce_scope_holds, so it
-- goes last. Identity sequences are table-owned and drop with their tables; no
-- explicit DROP SEQUENCE is needed. Indexes drop with their tables.
DROP TABLE IF EXISTS nonce_ops_audit;
DROP TABLE IF EXISTS nonce_scope_holds;
DROP TABLE IF EXISTS nonce_observations;
DROP TABLE IF EXISTS nonce_binding_events;
DROP TABLE IF EXISTS nonce_bindings;
DROP TABLE IF EXISTS nonce_scope_state;
DROP TABLE IF EXISTS nonce_wallet_registry;
