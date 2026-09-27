-- +goose Up
-- TxHarbor 014 T040 expected-event discriminator: the durable producer-written
-- expectation carrier (event_obligation). One row means: the producer transition
-- committed a catalog v1 event of expected_event_type for
-- (aggregate_type, aggregate_id, aggregate_version) inside the same database
-- transaction, at obligated_at. The table is the R1/R3 evidence source of the
-- 014 read path (specs/014-reconciliation-exception-handling/
-- expected-event-discriminator.md §2-§3): a marker proves the transition had an
-- event obligation, so an absent event delivery is a real loss (missing); no
-- marker never proves the opposite (old producers, unmarked history) and the
-- discriminator stays conservative.
--
-- PURE ADDITIVE DDL after 000016: no existing table, column, index or
-- constraint is altered, renamed or dropped; applied migrations 000001-000016
-- are never rewritten. No triggers, no stored functions, no CDC, no seed rows.
-- No historical backfill is performed here or anywhere: pre-carrier rows simply
-- have no marker, which is "unknown", never "not applicable" and never
-- "missing" (Q-cutover option B, 2026-09-27; no time-based approximation).
--
-- Append-only: rows are inserted by the producer transaction and never updated
-- or deleted by any 013/014 code path. Events retention pruning (events-admin
-- retention-prune) deletes outbox_events rows only; it never touches this
-- table, which is exactly why the marker survives a legal trim and lets the
-- discriminator separate "possibly legally trimmed" (pending) from a genuine
-- loss inside the retention horizon (missing). There is no retention path for
-- event_obligation in this migration.
--
-- CAPABILITY BOUNDARY (recorded, not resolved): a marker is written by
-- internal/events.Append when the producer transaction emits the event. It can
-- therefore never prove that a producer code path invoked Append at all: a
-- transition whose code never calls Append produces neither event nor marker
-- and stays beyond this carrier's detection range. The marker is an
-- expectation/evidence record, not an independent completeness proof of all
-- producer paths; producer-side atomicity remains carried by the same
-- transaction and the existing 013 tests (see docs/evidence/014/
-- t040_discriminator_evidence.md §2). The read path MUST NOT upgrade "no
-- marker" into "no obligation".
--
-- Rollback procedure (controlled window only): stop reconcile-admin scans and
-- event producers that would write markers, then apply `down`; the table is
-- dropped and 001-016 objects are untouched. Down is never a substitute for the
-- FR-014 "stop new work, bounded in-flight settle" path.
CREATE TABLE event_obligation (
    obligation_id       BIGINT      GENERATED ALWAYS AS IDENTITY,
    aggregate_type      TEXT        NOT NULL,
    aggregate_id        TEXT        NOT NULL,
    aggregate_version   BIGINT      NOT NULL,
    expected_event_type TEXT        NOT NULL,
    obligated_at        TIMESTAMPTZ NOT NULL,
    -- Producer source watermark, exactly the outbox_events source triple; it
    -- may be empty for a producer that carries no source watermark (the
    -- outbox schema allows it too) and is bounded when present.
    source_kind         TEXT        NOT NULL DEFAULT '',
    source_id           TEXT        NOT NULL DEFAULT '',
    source_version      BIGINT,
    CONSTRAINT event_obligation_pkey PRIMARY KEY (obligation_id),
    -- Identity: one expectation per aggregate version and event type. A repeat
    -- Append of the same committed transition collides here and is a no-op
    -- (ON CONFLICT DO NOTHING), never a second row and never a rewrite.
    CONSTRAINT event_obligation_identity_uniq UNIQUE (
        aggregate_type, aggregate_id, aggregate_version, expected_event_type),
    CONSTRAINT event_obligation_aggregate_version_check CHECK (aggregate_version > 0),
    CONSTRAINT event_obligation_source_version_check CHECK (
        source_version IS NULL OR source_version > 0),
    CONSTRAINT event_obligation_aggregate_id_shape CHECK (
        length(aggregate_id) BETWEEN 1 AND 512),
    CONSTRAINT event_obligation_source_kind_shape CHECK (
        length(source_kind) <= 128),
    CONSTRAINT event_obligation_source_id_shape CHECK (
        length(source_id) <= 512),
    -- Closed (aggregate_type, expected_event_type) mapping: the catalog v1
    -- aggregate of each type accepts exactly the event types that aggregate
    -- can emit. Unknown pairs are rejected by name; widening the mapping is a
    -- catalog change (new migration), never an implicit one.
    CONSTRAINT event_obligation_mapping_check CHECK (
        (aggregate_type = 'deposit_observation' AND expected_event_type IN (
            'deposit.observation.created',
            'deposit.observation.status_changed',
            'deposit.observation.reinstated',
            'deposit.confirmation.confirmed',
            'deposit.revision.applied'))
        OR (aggregate_type = 'withdrawal_request'
            AND expected_event_type = 'withdrawal.request.received')
        OR (aggregate_type = 'withdrawal_intent' AND expected_event_type IN (
            'withdrawal.execution.state_changed',
            'withdrawal.execution.revised'))
    )
);
-- The discriminator's decisive read: the expectations of one aggregate.
CREATE INDEX event_obligation_aggregate_idx
    ON event_obligation (aggregate_type, aggregate_id);
-- Retention-horizon reasoning: when the event row is absent, the discriminator
-- compares obligated_at against the audited retention-prune cutoffs.
CREATE INDEX event_obligation_obligated_at_idx
    ON event_obligation (obligated_at);

-- +goose Down
-- Controlled-window rollback only (see the Up header): drops the expectation
-- carrier after producers that write it were stopped. No 001-016 object is
-- touched; the down is never a substitute for the bounded pause/cancel path.
DROP TABLE IF EXISTS event_obligation;
