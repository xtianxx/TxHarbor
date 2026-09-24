-- +goose Up
-- TxHarbor 013 reliable event infrastructure: transactional outbox, consumer
-- state, quarantine, operator audit and cutover state (T008; data-model.md
-- §2/§3, FR-07/08/09/13/14/15/20).
--
-- PURE ADDITIVE DDL after 000014: no existing table, column, index or
-- constraint is altered, renamed or dropped; applied migrations 000001-000014
-- are never rewritten. Every classified constraint/index carries an explicit
-- name so callers classify PostgreSQL 23505/23514 errors by ConstraintName
-- only (data-model §10). No triggers, no stored functions, no CDC (R1).
--
-- Identity keys are partial UNIQUE indexes: EVM logs by
-- (chain_id, block_hash, tx_hash, log_index) and business objects by
-- (aggregate_type, aggregate_id, aggregate_version); block height is never an
-- identity component by itself (contracts/events.md §2).
--
-- Rollback procedure (T009 evidence; data-model §7): `down` is only used
-- inside a controlled window — stop event-publisher/consumer/reference
-- consumer, inventory and export pending|blocked rows (or drain first), then
-- apply this down (drops the seven new tables), then remove the wiring.
-- "Stop emitting while the business continues" is a violation of FR-07 and is
-- NOT a rollback.
--
-- 1:1 with data-model §2:
--   Table 1 outbox_events       -- event record + publish state + capacity view
--   Table 2 consumer_progress   -- persistent consumer offset (high water mark)
--   Table 3 consumer_inbox      -- event-level durable dedup (exactly-once effect)
--   Table 4 consumer_versions   -- object-level version guard and gap detection
--   Table 5 consumer_quarantine -- poison-event / unknown-version quarantine
--   Table 6 event_ops_audit     -- audited operator replay/unblock/prune (PD-4)
--   Table 7 event_system_state  -- cutover marker and catalog version (single row)

-- Table 1 -- outbox_events. Written in the same transaction as the business
-- transition (T1); publish_state machine: pending -> published (claim + ack)
-- or pending -> blocked (permanent class) -> pending (audited unblock).
CREATE TABLE outbox_events (
    id                BIGINT GENERATED ALWAYS AS IDENTITY,
    event_id          UUID        NOT NULL,
    identity_kind     TEXT        NOT NULL,
    event_type        TEXT        NOT NULL,
    schema_version    INTEGER     NOT NULL,
    aggregate_type    TEXT        NOT NULL,
    aggregate_id      TEXT        NOT NULL,
    aggregate_version BIGINT      NOT NULL,
    payload           JSONB       NOT NULL,
    payload_hash      TEXT        NOT NULL,
    occurred_at       TIMESTAMPTZ NOT NULL,
    chain_id          BIGINT,
    block_number      BIGINT,
    block_hash        TEXT,
    tx_hash           TEXT,
    log_index         INTEGER,
    recovery_version  BIGINT,
    revises_event_id  UUID,
    source_kind       TEXT,
    source_id         TEXT,
    source_version    BIGINT,
    publish_state     TEXT        NOT NULL DEFAULT 'pending',
    attempt_count     INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_owner       TEXT,
    claim_expires_at  TIMESTAMPTZ,
    last_error_class  TEXT,
    published_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT outbox_events_pkey PRIMARY KEY (id),
    CONSTRAINT outbox_events_event_id_uniq UNIQUE (event_id),
    CONSTRAINT outbox_events_identity_kind_check CHECK (identity_kind IN ('evm_log', 'business_object')),
    CONSTRAINT outbox_events_publish_state_check CHECK (publish_state IN ('pending', 'published', 'blocked')),
    CONSTRAINT outbox_events_schema_version_check CHECK (schema_version > 0),
    CONSTRAINT outbox_events_aggregate_version_check CHECK (aggregate_version > 0),
    CONSTRAINT outbox_events_attempt_count_check CHECK (attempt_count >= 0),
    CONSTRAINT outbox_events_log_identity_shape CHECK (
        identity_kind <> 'evm_log' OR (
            chain_id IS NOT NULL AND block_number IS NOT NULL AND
            block_hash IS NOT NULL AND tx_hash IS NOT NULL AND log_index IS NOT NULL
        )
    ),
    CONSTRAINT outbox_events_state_consistency CHECK (
        (
            (publish_state = 'published' AND published_at IS NOT NULL) OR
            (publish_state IN ('pending', 'blocked') AND published_at IS NULL)
        )
        AND (publish_state <> 'blocked' OR last_error_class IS NOT NULL)
    ),
    CONSTRAINT outbox_events_revision_shape CHECK (
        revises_event_id IS NULL OR recovery_version IS NOT NULL
    )
);

-- Identity: EVM log triple plus block hash (height alone is not identity).
CREATE UNIQUE INDEX outbox_events_log_identity_uniq
    ON outbox_events (chain_id, block_hash, tx_hash, log_index)
    WHERE identity_kind = 'evm_log';

-- Identity: business object version (emission-stream version, from 1).
CREATE UNIQUE INDEX outbox_events_object_identity_uniq
    ON outbox_events (aggregate_type, aggregate_id, aggregate_version)
    WHERE identity_kind = 'business_object';

-- Publisher claim queue (FIFO approximation).
CREATE INDEX outbox_events_pending_queue_idx
    ON outbox_events (next_attempt_at, id)
    WHERE publish_state = 'pending';

-- Capacity observation (count / oldest wait; Redis never participates).
CREATE INDEX outbox_events_pending_capacity_idx
    ON outbox_events (id)
    WHERE publish_state = 'pending';

-- Retention pruning of published rows only.
CREATE INDEX outbox_events_published_retention_idx
    ON outbox_events (published_at)
    WHERE publish_state = 'published';

-- Reconciliation audit by source watermark.
CREATE INDEX outbox_events_source_audit_idx
    ON outbox_events (source_kind, source_id, source_version);

-- Table 2 -- consumer_progress. next_offset is the next offset to process;
-- advanced inside the effect transaction (T4) and authoritative across
-- restart/rebalance.
CREATE TABLE consumer_progress (
    consumer_name TEXT        NOT NULL,
    topic         TEXT        NOT NULL,
    partition     INTEGER     NOT NULL,
    next_offset   BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT consumer_progress_pkey PRIMARY KEY (consumer_name, topic, partition),
    CONSTRAINT consumer_progress_next_offset_check CHECK (next_offset >= 0)
);

-- Table 3 -- consumer_inbox. Insert success = first effect application;
-- 23505 on the PK means already applied, skip (FR-13). `offset` is a reserved
-- word and is always quoted in SQL (column name kept 1:1 with data-model §2).
CREATE TABLE consumer_inbox (
    consumer_name     TEXT        NOT NULL,
    event_id          UUID        NOT NULL,
    aggregate_type    TEXT        NOT NULL,
    aggregate_id      TEXT        NOT NULL,
    aggregate_version BIGINT      NOT NULL,
    topic             TEXT        NOT NULL,
    partition         INTEGER     NOT NULL,
    "offset"          BIGINT      NOT NULL,
    applied_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT consumer_inbox_pkey PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX consumer_inbox_aggregate_idx
    ON consumer_inbox (consumer_name, aggregate_type, aggregate_id);

-- Table 4 -- consumer_versions. Object-level version guard: apply only when
-- event.aggregate_version > max_version; a gap is version_gap quarantine.
CREATE TABLE consumer_versions (
    consumer_name     TEXT        NOT NULL,
    aggregate_type    TEXT        NOT NULL,
    aggregate_id      TEXT        NOT NULL,
    max_version       BIGINT      NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT consumer_versions_pkey PRIMARY KEY (consumer_name, aggregate_type, aggregate_id),
    CONSTRAINT consumer_versions_max_version_check CHECK (max_version > 0)
);

-- Table 5 -- consumer_quarantine. Poison events / unknown versions are
-- isolated with a full snapshot; quarantine never deletes events and never
-- blocks partition progress (FR-14/SC-05).
CREATE TABLE consumer_quarantine (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY,
    consumer_name       TEXT        NOT NULL,
    event_id            UUID        NOT NULL,
    event_snapshot      JSONB       NOT NULL,
    failure_class       TEXT        NOT NULL,
    reason              TEXT        NOT NULL,
    attempt_count       INTEGER     NOT NULL,
    source_topic        TEXT,
    source_partition    INTEGER,
    source_offset       BIGINT,
    first_seen_at       TIMESTAMPTZ NOT NULL,
    last_seen_at        TIMESTAMPTZ NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'open',
    replayed_at         TIMESTAMPTZ,
    replay_operation_id TEXT,
    CONSTRAINT consumer_quarantine_pkey PRIMARY KEY (id),
    CONSTRAINT consumer_quarantine_failure_class_check CHECK (
        failure_class IN ('retry_exhausted', 'non_retryable', 'version_gap', 'schema_unsupported', 'identity_mismatch')
    ),
    CONSTRAINT consumer_quarantine_status_check CHECK (status IN ('open', 'replayed', 'superseded'))
);

-- At most one open quarantine entry per (consumer, event).
CREATE UNIQUE INDEX consumer_quarantine_open_uniq
    ON consumer_quarantine (consumer_name, event_id)
    WHERE status = 'open';

-- Table 6 -- event_ops_audit. PD-4 operator audit: operator, scope, reason,
-- result; operation_id is the CLI-retry dedup key.
CREATE TABLE event_ops_audit (
    id           BIGINT GENERATED ALWAYS AS IDENTITY,
    operation_id TEXT        NOT NULL,
    op_kind      TEXT        NOT NULL,
    operator     TEXT        NOT NULL,
    scope        JSONB       NOT NULL,
    reason       TEXT        NOT NULL,
    result       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_ops_audit_pkey PRIMARY KEY (id),
    CONSTRAINT event_ops_audit_operation_id_key UNIQUE (operation_id),
    CONSTRAINT event_ops_audit_op_kind_check CHECK (op_kind IN ('replay', 'unblock', 'retention_prune'))
);

-- Table 7 -- event_system_state. Single row: cutover marker and catalog
-- version (initial 1). Seeded here; never rewritten by later migrations.
CREATE TABLE event_system_state (
    id              SMALLINT    NOT NULL,
    cutover_at      TIMESTAMPTZ NOT NULL,
    catalog_version INTEGER     NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_system_state_pkey PRIMARY KEY (id),
    CONSTRAINT event_system_state_id_check CHECK (id = 1)
);

INSERT INTO event_system_state (id, cutover_at, catalog_version)
VALUES (1, now(), 1);

-- +goose Down
-- Controlled-window rollback only (see the Up header): drops the seven new
-- tables and their data (consumer_* state is acceptable rebuild cost). No
-- existing object of 000001-000014 is touched.
DROP TABLE IF EXISTS event_system_state;
DROP TABLE IF EXISTS event_ops_audit;
DROP TABLE IF EXISTS consumer_quarantine;
DROP TABLE IF EXISTS consumer_versions;
DROP TABLE IF EXISTS consumer_inbox;
DROP TABLE IF EXISTS consumer_progress;
DROP TABLE IF EXISTS outbox_events;
