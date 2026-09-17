-- +goose Up
-- TxHarbor 011 withdrawal-executor: durable execution carriers.
-- (specs/011-withdrawal-executor/data-model.md Tables 1-7; contracts/persistence.md,
-- contracts/lifecycle.md; research R3/R6/R8/R10.)
--
-- PROVISIONAL migration number per C12/J6 (011 = 000012; 010 = 000011; PB
-- occupies 000010). Re-verify the actual set at merge; never renumber or
-- rewrite an applied migration.
--
-- Additive-only, PURE DDL: no triggers, no stored functions, no changes to any
-- 006/007/008/009/010 object. Every classified constraint carries an explicit
-- CONSTRAINT name (or an explicitly named index) so PostgreSQL never
-- auto-names and callers classify by ConstraintName only. Amounts/fees are
-- integer-only (BIGINT); timestamps are TIMESTAMPTZ DEFAULT now(); validity is
-- judged on the DB clock, never the application clock.
--
-- 1:1 with the data model:
--   Table 1 payment_intents           -- stable payment intent (FR-02/03/07/12)
--   Table 2 execution_claims          -- claim/lease/qualification (FR-04/05/06/15)
--   Table 3 execution_steps           -- execution position (FR-08/13/14)
--   Table 4 execution_events          -- append-only execution log (FR-10/12/13/15)
--   Table 5 request_status_projection -- display-only projection (FR-09)
--   Table 6 execution_caller_permission -- fixed interface permission (FR-01)
--   Table 7 execution_ops_audit       -- operator audit + operation dedup (FR-15/R14)

-- Table 1 -- payment_intents. One request -> at most one intent; one
-- authorization binds one intent; sender is fixed at admission. Row fields are
-- immutable except state/state_version/updated_at.
CREATE TABLE payment_intents (
    intent_id                  TEXT        NOT NULL,
    request_id                 TEXT        NOT NULL,
    chain_id                   BIGINT      NOT NULL,
    sender                     TEXT        NOT NULL,
    authorization_id           TEXT        NOT NULL,
    authorization_version      BIGINT      NOT NULL,
    state                      TEXT        NOT NULL,
    state_version              BIGINT      NOT NULL DEFAULT 1,
    admitted_recovery_version  BIGINT      NOT NULL,
    admitted_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT payment_intents_pkey PRIMARY KEY (intent_id),
    CONSTRAINT payment_intents_request_uniq UNIQUE (request_id),
    CONSTRAINT payment_intents_authorization_uniq UNIQUE (authorization_id),
    CONSTRAINT payment_intents_request_fkey FOREIGN KEY (request_id)
        REFERENCES withdrawal_requests (request_id),
    CONSTRAINT payment_intents_intent_shape CHECK (
        length(intent_id) BETWEEN 1 AND 128 AND intent_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT payment_intents_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT payment_intents_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT payment_intents_authorization_version_check CHECK (authorization_version >= 1),
    CONSTRAINT payment_intents_state_check CHECK (
        state IN ('admitted','claimed','executing','completed','failed','reconciling','revised')),
    CONSTRAINT payment_intents_state_version_check CHECK (state_version >= 1)
);

-- Table 2 -- execution_claims. One row per intent (PK): monotonic
-- lease_version, never revived; expiry disqualifies. 010 reads this row only.
CREATE TABLE execution_claims (
    intent_id          TEXT        NOT NULL,
    owner_id           TEXT        NOT NULL,
    lease_version      BIGINT      NOT NULL,
    state              TEXT        NOT NULL,
    acquired_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at         TIMESTAMPTZ NOT NULL,
    last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_progress_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    stall_flagged_at   TIMESTAMPTZ,
    ended_at           TIMESTAMPTZ,
    end_kind           TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT execution_claims_pkey PRIMARY KEY (intent_id),
    CONSTRAINT execution_claims_intent_fkey FOREIGN KEY (intent_id)
        REFERENCES payment_intents (intent_id),
    CONSTRAINT execution_claims_owner_check CHECK (length(owner_id) BETWEEN 1 AND 128),
    CONSTRAINT execution_claims_lease_version_check CHECK (lease_version >= 1),
    CONSTRAINT execution_claims_state_check CHECK (state IN ('active','released','revoked')),
    CONSTRAINT execution_claims_state_consistency CHECK (
        (state = 'active') = (ended_at IS NULL AND end_kind IS NULL)),
    CONSTRAINT execution_claims_end_kind_check CHECK (
        end_kind IS NULL OR end_kind IN ('released','revoked')),
    CONSTRAINT execution_claims_expiry_check CHECK (expires_at > acquired_at)
);

-- Table 3 -- execution_steps. Insert-first execution position: one open
-- (issued) step per intent is structural via execution_steps_open_uniq.
CREATE TABLE execution_steps (
    step_id            TEXT        NOT NULL,
    intent_id          TEXT        NOT NULL,
    action             TEXT        NOT NULL,
    state              TEXT        NOT NULL,
    owner_id           TEXT        NOT NULL,
    lease_version      BIGINT      NOT NULL,
    attempt_id         TEXT,
    anchor_attempt_id  TEXT,
    tx_hash            TEXT,
    outcome_class      TEXT,
    recovery_version   BIGINT,
    revision_version   BIGINT,
    evidence           TEXT        NOT NULL DEFAULT '',
    issued_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT execution_steps_pkey PRIMARY KEY (step_id),
    CONSTRAINT execution_steps_intent_fkey FOREIGN KEY (intent_id)
        REFERENCES payment_intents (intent_id),
    CONSTRAINT execution_steps_action_check CHECK (action IN ('first_broadcast','replay','replace')),
    CONSTRAINT execution_steps_state_check CHECK (state IN ('issued','converged','refused','unknown')),
    CONSTRAINT execution_steps_outcome_class_check CHECK (
        outcome_class IS NULL OR outcome_class IN
        ('sent','refused_gate','refused_basis','pending_unknown','reconcile_required','unavailable')),
    CONSTRAINT execution_steps_lease_version_check CHECK (lease_version >= 1),
    CONSTRAINT execution_steps_tx_hash_check CHECK (
        tx_hash IS NULL OR tx_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT execution_steps_attempt_shape CHECK (
        attempt_id IS NULL OR (length(attempt_id) BETWEEN 1 AND 128 AND attempt_id ~ '^[\x21-\x7e]+$')),
    CONSTRAINT execution_steps_owner_check CHECK (length(owner_id) BETWEEN 1 AND 128)
);

-- One open send step per intent: reconcile-before-decision is impossible to
-- skip. The 23505 is classified by this index name.
CREATE UNIQUE INDEX execution_steps_open_uniq ON execution_steps (intent_id) WHERE state = 'issued';

-- Table 4 -- execution_events. Append-only; intent_id carries no FK (refusals
-- before admission must be recordable; 000007 Table 5 precedent). Rows are
-- never updated or deleted.
CREATE TABLE execution_events (
    event_id          BIGINT      GENERATED ALWAYS AS IDENTITY,
    intent_id         TEXT        NOT NULL,
    kind              TEXT        NOT NULL,
    from_state        TEXT,
    to_state          TEXT,
    lease_version     BIGINT,
    step_id           TEXT,
    attempt_id        TEXT,
    revision_version  BIGINT,
    detail            TEXT        NOT NULL DEFAULT '',
    at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT execution_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT execution_events_kind_check CHECK (kind IN (
        'admitted','admission_refused','claimed','released','taken_over','revoked',
        'stall_flagged','state_changed','step_issued','step_converged','step_refused',
        'step_unknown','reconcile_observed','revision_applied','projection_stale',
        'projection_refreshed'))
);

CREATE INDEX execution_events_intent_time_idx ON execution_events (intent_id, at);

-- Table 5 -- request_status_projection. Display-only; references identity,
-- never copies facts; two version-monotonic input streams.
CREATE TABLE request_status_projection (
    request_id             TEXT        NOT NULL,
    intent_id              TEXT        NOT NULL,
    execution_state        TEXT        NOT NULL,
    state_version          BIGINT      NOT NULL,
    lifecycle_attempt_id   TEXT,
    lifecycle_version      BIGINT      NOT NULL DEFAULT 0,
    lifecycle_observed_at  TIMESTAMPTZ,
    freshness              TEXT        NOT NULL,
    stale_since            TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT request_status_projection_pkey PRIMARY KEY (request_id),
    CONSTRAINT request_status_projection_intent_uniq UNIQUE (intent_id),
    CONSTRAINT request_status_projection_intent_fkey FOREIGN KEY (intent_id)
        REFERENCES payment_intents (intent_id),
    CONSTRAINT request_status_projection_state_check CHECK (execution_state IN
        ('admitted','claimed','executing','completed','failed','reconciling','revised')),
    CONSTRAINT request_status_projection_freshness_check CHECK (freshness IN ('confirmed','possibly_stale')),
    CONSTRAINT request_status_projection_stale_consistency CHECK (
        (freshness = 'possibly_stale') = (stale_since IS NOT NULL))
);

-- Table 6 -- execution_caller_permission. Fixed interface permission,
-- independent of 007's caller.can_create; absent/FALSE is fail-closed.
CREATE TABLE execution_caller_permission (
    caller_id    BIGINT      NOT NULL,
    can_execute  BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by   TEXT        NOT NULL DEFAULT '',
    CONSTRAINT execution_caller_permission_pkey PRIMARY KEY (caller_id),
    CONSTRAINT execution_caller_permission_caller_fkey FOREIGN KEY (caller_id)
        REFERENCES caller (caller_id)
);

-- Table 7 -- execution_ops_audit. Append-only operator audit; operation_id is
-- the only dedup key (23505 -> rollback -> read back).
CREATE TABLE execution_ops_audit (
    audit_id         BIGINT      GENERATED ALWAYS AS IDENTITY,
    operation_id     TEXT        NOT NULL,
    action           TEXT        NOT NULL,
    intent_id        TEXT        NOT NULL DEFAULT '',
    caller_id        BIGINT,
    subject_version  BIGINT,
    outcome          TEXT        NOT NULL,
    operator         TEXT        NOT NULL,
    reason           TEXT        NOT NULL DEFAULT '',
    evidence         TEXT        NOT NULL DEFAULT '',
    detail           TEXT        NOT NULL DEFAULT '',
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT execution_ops_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT execution_ops_audit_operation_id_uniq UNIQUE (operation_id),
    CONSTRAINT execution_ops_audit_action_check CHECK (action IN
        ('permission_set','permission_revoke','claim_revoke','projection_refresh')),
    CONSTRAINT execution_ops_audit_outcome_check CHECK (outcome IN ('applied','nop','refused'))
);

-- +goose Down
-- Drop 011 objects only, in exact reverse creation order; no upstream object
-- is touched (authoring/scratch hygiene, same as 000007/000010 Down).
DROP TABLE IF EXISTS execution_ops_audit;
DROP TABLE IF EXISTS execution_caller_permission;
DROP TABLE IF EXISTS request_status_projection;
DROP INDEX IF EXISTS execution_events_intent_time_idx;
DROP TABLE IF EXISTS execution_events;
DROP INDEX IF EXISTS execution_steps_open_uniq;
DROP TABLE IF EXISTS execution_steps;
DROP TABLE IF EXISTS execution_claims;
DROP TABLE IF EXISTS payment_intents;
