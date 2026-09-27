-- +goose Up
-- TxHarbor 014 reconciliation and exception handling: task/checkpoint/gap,
-- discrepancy lifecycle, claim-execute-commit attempts, permission registry
-- and append-only audit (T002 draft → T004 full; data-model.md §1–§2/§5–§6;
-- FR-001–FR-024, Q1–Q5).
--
-- PURE ADDITIVE DDL after 000015: no existing table, column, index or
-- constraint is altered, renamed or dropped; applied migrations 000001-000015
-- are never rewritten. Ten new 014-owned tables. No balance ledger is built
-- (FR-021) and no monetary column is introduced: every amount stays
-- NUMERIC(78,0) in its 003-012 owning domain, while every count, quota,
-- height and range here is an integer type (BIGINT/INTEGER) and no
-- REAL/DOUBLE PRECISION appears anywhere.
--
-- ENUM semantics are realized as TEXT + named CHECK constraints (repo
-- convention, 000011/000015): callers classify PostgreSQL 23514 by
-- ConstraintName only, so every constraint/index carries an explicit name.
-- No triggers, no stored functions, no CDC, no seed rows.
--
-- Append-only: recon_audit, discrepancy_occurrence and reverify are insert-
-- only observation/audit trails (no updated_at, no 014 UPDATE/DELETE path);
-- refusals are recorded as rows, never silently dropped. 014 never writes a
-- 001-015 table.
--
-- Atomicity contract carried by the schema (data-model §5/§5.1): results are
-- persisted before the checkpoint pointer moves; the checkpoint
-- result_persisted_through MUST NOT pass its covered_through; a task has at
-- most one `claimed` recon_scan_attempt row at a time (partial UNIQUE), and
-- lease state distinguishes live/expired claims without holding a DB
-- transaction across slow RPC.
--
-- Rollback procedure (controlled window only): stop all reconcile-admin
-- invocations, drain or export pending 014 work, then apply `down`; the ten
-- 014 tables are dropped and 001-015 objects are untouched. Down is never a
-- substitute for the FR-014 "stop new work, bounded in-flight settle" path.
--
-- 1:1 with data-model §1:
--   recon_task             §1.1  scope + upstream receipt source + budget
--   recon_checkpoint       §1.2  monotonic persisted waterline
--   recon_gap              §1.3  uncovered ranges (explicit, never a pass)
--   discrepancy            §1.4  stable-identity ticket and lifecycle state
--   discrepancy_occurrence §1.5  reappearance evidence (append-only)
--   disposition            §1.6  ack/reuse/dry-run dispositions (idempotent)
--   reverify               §1.7  read-only revalidation verdicts (append-only)
--   recon_audit            §1.8  append-only operator/system audit trail
--   recon_scan_attempt     §1.9  claim-execute-commit attempt carrier
--   recon_permission       §1.10 default-deny permission evaluation source

-- Table 1 -- recon_task (§1.1). Scope, resource budget, policy snapshot and
-- per-business-type upstream receipt source. state machine (app-enforced,
-- contract task-lifecycle.md): created -> running <-> paused |
-- suspended_budget -> done | cancelled. upstream_receipt_source shape is
-- {business_type: {source, connected}}; connected=false means the upstream
-- is not connected and classifiers may only conclude `incomplete`, never
-- `consistent`. Scope ranges are typed pairs: height uses
-- scope_start/scope_end, time uses scope_start_at/scope_end_at; exactly the
-- pair matching scope_kind is valid.
CREATE TABLE recon_task (
    task_id                 UUID        NOT NULL,
    scope_chain_id          TEXT        NOT NULL,
    scope_kind              TEXT        NOT NULL,
    scope_start             BIGINT,
    scope_end               BIGINT,
    scope_start_at          TIMESTAMPTZ,
    scope_end_at            TIMESTAMPTZ,
    business_types          TEXT[]      NOT NULL,
    upstream_receipt_source JSONB       NOT NULL DEFAULT '{}'::jsonb,
    policy_refs             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    state                   TEXT        NOT NULL DEFAULT 'created',
    pause_reason            TEXT,
    budget                  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    history_sweep_through   JSONB,
    created_by              TEXT        NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_task_pkey PRIMARY KEY (task_id),
    CONSTRAINT recon_task_scope_kind_check CHECK (scope_kind IN ('height', 'time')),
    CONSTRAINT recon_task_state_check CHECK (state IN (
        'created', 'running', 'paused', 'suspended_budget', 'done', 'cancelled')),
    CONSTRAINT recon_task_scope_shape CHECK (
        (scope_kind = 'height'
            AND scope_start IS NOT NULL AND scope_end IS NOT NULL
            AND scope_start_at IS NULL AND scope_end_at IS NULL
            AND scope_start >= 0 AND scope_start <= scope_end)
        OR
        (scope_kind = 'time'
            AND scope_start IS NULL AND scope_end IS NULL
            AND scope_start_at IS NOT NULL AND scope_end_at IS NOT NULL
            AND scope_start_at <= scope_end_at)),
    -- Closed set of business types; unknown types are rejected. A new
    -- business type is a catalog change (new migration), never an implicit
    -- widening. NULL array elements are rejected explicitly because the
    -- containment operator alone would yield NULL (not FALSE).
    CONSTRAINT recon_task_business_types_check CHECK (
        cardinality(business_types) >= 1
        AND array_position(business_types, NULL) IS NULL
        AND business_types <@ ARRAY['withdrawal', 'deposit', 'event-delivery']::TEXT[]),
    CONSTRAINT recon_task_receipt_source_shape CHECK (
        jsonb_typeof(upstream_receipt_source) = 'object'),
    CONSTRAINT recon_task_policy_refs_shape CHECK (jsonb_typeof(policy_refs) = 'object'),
    CONSTRAINT recon_task_budget_shape CHECK (jsonb_typeof(budget) = 'object'),
    CONSTRAINT recon_task_sweep_shape CHECK (
        history_sweep_through IS NULL OR jsonb_typeof(history_sweep_through) = 'object'),
    CONSTRAINT recon_task_chain_shape CHECK (length(scope_chain_id) BETWEEN 1 AND 128),
    CONSTRAINT recon_task_creator_shape CHECK (length(created_by) BETWEEN 1 AND 128),
    CONSTRAINT recon_task_pause_reason_shape CHECK (
        pause_reason IS NULL OR length(pause_reason) BETWEEN 1 AND 1024)
);
CREATE INDEX recon_task_state_idx ON recon_task (state, updated_at);
CREATE INDEX recon_task_chain_idx ON recon_task (scope_chain_id, created_at);

-- Table 2 -- recon_checkpoint (§1.2). Append-only persisted waterline per
-- task; the current pointer is MAX(seq). result_persisted_through is the
-- result-persisted prefix and MUST NOT pass covered_through (results are
-- persisted before the pointer moves, same DB transaction). Cancel/timeout/
-- failure never advance past the uncovered interval; a resume may re-scan
-- covered ranges idempotently. Height/time pairs mirror the task scope kind.
CREATE TABLE recon_checkpoint (
    task_id                     UUID        NOT NULL,
    seq                         BIGINT      NOT NULL,
    covered_through             BIGINT,
    covered_through_at          TIMESTAMPTZ,
    result_persisted_through    BIGINT,
    result_persisted_through_at TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_checkpoint_pkey PRIMARY KEY (task_id, seq),
    CONSTRAINT recon_checkpoint_task_fkey FOREIGN KEY (task_id)
        REFERENCES recon_task (task_id),
    CONSTRAINT recon_checkpoint_seq_check CHECK (seq > 0),
    CONSTRAINT recon_checkpoint_covered_shape CHECK (
        (covered_through IS NULL) <> (covered_through_at IS NULL)),
    CONSTRAINT recon_checkpoint_persisted_shape CHECK (
        (result_persisted_through IS NULL) <> (result_persisted_through_at IS NULL)),
    CONSTRAINT recon_checkpoint_height_prefix CHECK (
        result_persisted_through IS NULL OR covered_through IS NULL
        OR result_persisted_through <= covered_through),
    CONSTRAINT recon_checkpoint_time_prefix CHECK (
        result_persisted_through_at IS NULL OR covered_through_at IS NULL
        OR result_persisted_through_at <= covered_through_at)
);

-- Table 3 -- recon_gap (§1.3). Every uncovered / not-verified range is an
-- explicit row: paused, budget-exhausted, interrupted, freshness-hold,
-- upstream-unconnected and query-failed states must stay visible, and a
-- "fully consistent" conclusion requires zero open gaps. Rows are removed
-- only when the range is successfully covered, never silently.
CREATE TABLE recon_gap (
    gap_id         BIGINT      GENERATED ALWAYS AS IDENTITY,
    task_id        UUID        NOT NULL,
    range_start    BIGINT,
    range_end      BIGINT,
    range_start_at TIMESTAMPTZ,
    range_end_at   TIMESTAMPTZ,
    reason         TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_gap_pkey PRIMARY KEY (gap_id),
    CONSTRAINT recon_gap_task_fkey FOREIGN KEY (task_id) REFERENCES recon_task (task_id),
    CONSTRAINT recon_gap_reason_check CHECK (reason IN (
        'not_started', 'interrupted', 'budget_exhausted', 'paused',
        'freshness_hold', 'upstream_unconnected', 'query_failed')),
    CONSTRAINT recon_gap_range_shape CHECK (
        (range_start IS NOT NULL AND range_end IS NOT NULL
            AND range_start_at IS NULL AND range_end_at IS NULL
            AND range_start >= 0 AND range_start <= range_end)
        OR
        (range_start IS NULL AND range_end IS NULL
            AND range_start_at IS NOT NULL AND range_end_at IS NOT NULL
            AND range_start_at <= range_end_at))
);
CREATE INDEX recon_gap_task_idx ON recon_gap (task_id, created_at);

-- Table 4 -- discrepancy (§1.4). Stable-identity ticket (data-model §2):
-- identity = (range, category, business_key, content_hash,
-- evidence_version_domain); discrepancy_id is the identity handle derived by
-- 014 code. Same identity with no divergence is absorbed without a ticket
-- (Q4); same identity reappearing appends discrepancy_occurrence and reopens
-- this row (reopen_count+1); a different identity creates a linked ticket
-- (linked_to). Lifecycle (contracts/discrepancy-lifecycle.md):
-- open_claimable -> claimed -> disposing -> pending_verify -> closed, plus
-- reopened -> pending_verify; illegal transitions are refused by 014 code.
-- claim is CAS-protected in code; the schema enforces that claimed/disposing
-- rows carry an owner and that `closed` carries a close_basis snapshot.
CREATE TABLE discrepancy (
    discrepancy_id          UUID        NOT NULL,
    category                TEXT        NOT NULL,
    business_key            TEXT        NOT NULL,
    content_hash            BYTEA       NOT NULL,
    evidence_version_domain JSONB       NOT NULL,
    state                   TEXT        NOT NULL DEFAULT 'open_claimable',
    claim_owner             TEXT,
    claimed_at              TIMESTAMPTZ,
    close_basis             JSONB,
    reopen_count            INTEGER     NOT NULL DEFAULT 0,
    linked_to               UUID,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT discrepancy_pkey PRIMARY KEY (discrepancy_id),
    CONSTRAINT discrepancy_linked_to_fkey FOREIGN KEY (linked_to)
        REFERENCES discrepancy (discrepancy_id),
    CONSTRAINT discrepancy_category_check CHECK (category IN (
        'missing', 'duplicate_divergent', 'state_mismatch', 'unknown', 'incomplete')),
    CONSTRAINT discrepancy_state_check CHECK (state IN (
        'open_claimable', 'claimed', 'disposing', 'pending_verify', 'closed', 'reopened')),
    CONSTRAINT discrepancy_business_key_shape CHECK (
        length(business_key) BETWEEN 1 AND 512),
    CONSTRAINT discrepancy_content_hash_shape CHECK (octet_length(content_hash) > 0),
    CONSTRAINT discrepancy_version_domain_shape CHECK (
        jsonb_typeof(evidence_version_domain) = 'object'),
    CONSTRAINT discrepancy_close_basis_shape CHECK (
        close_basis IS NULL OR jsonb_typeof(close_basis) = 'object'),
    CONSTRAINT discrepancy_reopen_count_check CHECK (reopen_count >= 0),
    CONSTRAINT discrepancy_claim_owner_shape CHECK (
        claim_owner IS NULL OR length(claim_owner) BETWEEN 1 AND 128),
    CONSTRAINT discrepancy_claimed_at_check CHECK (
        claimed_at IS NULL OR claim_owner IS NOT NULL),
    CONSTRAINT discrepancy_claim_states_check CHECK (
        state NOT IN ('claimed', 'disposing')
        OR (claim_owner IS NOT NULL AND claimed_at IS NOT NULL)),
    CONSTRAINT discrepancy_closed_basis_check CHECK (
        state <> 'closed' OR close_basis IS NOT NULL),
    CONSTRAINT discrepancy_linked_not_self CHECK (
        linked_to IS NULL OR linked_to <> discrepancy_id)
);
CREATE INDEX discrepancy_state_idx ON discrepancy (state, updated_at);
CREATE INDEX discrepancy_business_idx ON discrepancy (category, business_key);
CREATE INDEX discrepancy_claim_queue_idx ON discrepancy (updated_at)
    WHERE state = 'open_claimable';

-- Table 5 -- discrepancy_occurrence (§1.5). Append-only reappearance
-- evidence: a repeat detection appends a row instead of creating a new
-- ticket; scan_task_id attributes the observing scan (FK to the 014 task).
CREATE TABLE discrepancy_occurrence (
    occurrence_id  BIGINT      GENERATED ALWAYS AS IDENTITY,
    discrepancy_id UUID        NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL,
    evidence_ref   TEXT        NOT NULL,
    scan_task_id   UUID        NOT NULL,
    CONSTRAINT discrepancy_occurrence_pkey PRIMARY KEY (occurrence_id),
    CONSTRAINT discrepancy_occurrence_discrepancy_fkey FOREIGN KEY (discrepancy_id)
        REFERENCES discrepancy (discrepancy_id),
    CONSTRAINT discrepancy_occurrence_scan_task_fkey FOREIGN KEY (scan_task_id)
        REFERENCES recon_task (task_id),
    CONSTRAINT discrepancy_occurrence_evidence_shape CHECK (
        length(evidence_ref) BETWEEN 1 AND 512)
);
CREATE INDEX discrepancy_occurrence_discrepancy_idx
    ON discrepancy_occurrence (discrepancy_id, observed_at);
CREATE INDEX discrepancy_occurrence_task_idx
    ON discrepancy_occurrence (scan_task_id);

-- Table 6 -- disposition (§1.6). One row per dispose action:
-- ack_only (alert acknowledgement), reuse_recovery (references an existing
-- recovery/replay/unblock entry; 014 never auto-executes it and never
-- substitutes its gates) and new_fix_rule (this phase dry_run only, FR-023).
-- idempotency_key is UNIQUE: a repeated dispose converges by read-back
-- (23505 classification), producing no new state change or side effect.
CREATE TABLE disposition (
    disposition_id  UUID        NOT NULL,
    discrepancy_id  UUID        NOT NULL,
    kind            TEXT        NOT NULL,
    action_ref      TEXT        NOT NULL DEFAULT '',
    operator        TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    evidence_ref    TEXT        NOT NULL DEFAULT '',
    result          TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT disposition_pkey PRIMARY KEY (disposition_id),
    CONSTRAINT disposition_discrepancy_fkey FOREIGN KEY (discrepancy_id)
        REFERENCES discrepancy (discrepancy_id),
    CONSTRAINT disposition_idempotency_key_uniq UNIQUE (idempotency_key),
    CONSTRAINT disposition_kind_check CHECK (kind IN (
        'ack_only', 'reuse_recovery', 'new_fix_rule')),
    CONSTRAINT disposition_result_check CHECK (result IN (
        'done', 'refused', 'failed', 'dry_run')),
    CONSTRAINT disposition_new_fix_rule_dry_run_check CHECK (
        kind <> 'new_fix_rule' OR result = 'dry_run'),
    CONSTRAINT disposition_action_ref_shape CHECK (
        kind = 'ack_only' OR length(action_ref) BETWEEN 1 AND 512),
    CONSTRAINT disposition_operator_shape CHECK (length(operator) BETWEEN 1 AND 128),
    CONSTRAINT disposition_reason_shape CHECK (length(reason) BETWEEN 1 AND 1024),
    CONSTRAINT disposition_evidence_ref_shape CHECK (length(evidence_ref) <= 512),
    CONSTRAINT disposition_idempotency_shape CHECK (
        length(idempotency_key) BETWEEN 1 AND 256)
);
CREATE INDEX disposition_discrepancy_idx ON disposition (discrepancy_id, created_at);

-- Table 7 -- reverify (§1.7). Append-only read-only revalidation verdicts.
-- `consistent` requires complete fresh evidence; timeout/incomplete/
-- unavailable/insufficient evidence MUST NOT be written as consistent
-- (Q5-4). Automatic reverify writes only this table plus 014-owned records.
CREATE TABLE reverify (
    reverify_id    BIGINT      GENERATED ALWAYS AS IDENTITY,
    discrepancy_id UUID        NOT NULL,
    verdict        TEXT        NOT NULL,
    evidence_ref   TEXT        NOT NULL DEFAULT '',
    freshness_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT reverify_pkey PRIMARY KEY (reverify_id),
    CONSTRAINT reverify_discrepancy_fkey FOREIGN KEY (discrepancy_id)
        REFERENCES discrepancy (discrepancy_id),
    CONSTRAINT reverify_verdict_check CHECK (verdict IN (
        'consistent', 'divergent', 'unknown', 'stale')),
    CONSTRAINT reverify_consistent_evidence_check CHECK (
        verdict <> 'consistent'
        OR (length(evidence_ref) BETWEEN 1 AND 512 AND freshness_at IS NOT NULL))
);
CREATE INDEX reverify_discrepancy_idx ON reverify (discrepancy_id, created_at);

-- Table 8 -- recon_audit (§1.8). Append-only: rows are inserted, never
-- updated or deleted by 014 (refusals are rows too, including the ownership
-- hint). actor is the authenticated principal; free-text reason/evidence are
-- audit annotations only and never an authorization source.
CREATE TABLE recon_audit (
    audit_id   BIGINT      GENERATED ALWAYS AS IDENTITY,
    actor      TEXT        NOT NULL,
    action     TEXT        NOT NULL,
    target     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    reason     TEXT        NOT NULL DEFAULT '',
    evidence   TEXT        NOT NULL DEFAULT '',
    result     TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT recon_audit_action_check CHECK (action IN (
        'query', 'start', 'pause', 'resume', 'claim', 'dispose',
        'reverify', 'close', 'reopen', 'refuse')),
    CONSTRAINT recon_audit_actor_shape CHECK (length(actor) BETWEEN 1 AND 128),
    CONSTRAINT recon_audit_target_shape CHECK (jsonb_typeof(target) = 'object')
);
CREATE INDEX recon_audit_actor_idx ON recon_audit (actor, created_at);
CREATE INDEX recon_audit_action_idx ON recon_audit (action, created_at);

-- Table 9 -- recon_scan_attempt (§1.9, F3). Claim-execute-commit carrier:
-- the claim short transaction (SELECT recon_task ... FOR UPDATE) validates
-- state=running, computes the range from the pointer + budget, inserts this
-- row and commits -- no RPC inside the lock. The partial UNIQUE index admits
-- at most ONE claimed attempt per task, so two concurrent invocations cannot
-- both claim the same task (the loser retries the next interval); a late
-- submitter whose attempt is abandoned/superseded is discarded and audited
-- without moving the pointer. lease_expires_at is an optional heartbeat;
-- expired claims are abandoned and leave a gap row.
CREATE TABLE recon_scan_attempt (
    attempt_id       UUID        NOT NULL,
    task_id          UUID        NOT NULL,
    range_start      BIGINT,
    range_end        BIGINT,
    range_start_at   TIMESTAMPTZ,
    range_end_at     TIMESTAMPTZ,
    state            TEXT        NOT NULL DEFAULT 'claimed',
    owner            TEXT        NOT NULL,
    lease_expires_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_scan_attempt_pkey PRIMARY KEY (attempt_id),
    CONSTRAINT recon_scan_attempt_task_fkey FOREIGN KEY (task_id)
        REFERENCES recon_task (task_id),
    CONSTRAINT recon_scan_attempt_task_attempt_uniq UNIQUE (task_id, attempt_id),
    CONSTRAINT recon_scan_attempt_state_check CHECK (state IN (
        'claimed', 'done', 'abandoned', 'superseded')),
    CONSTRAINT recon_scan_attempt_range_shape CHECK (
        (range_start IS NOT NULL AND range_end IS NOT NULL
            AND range_start_at IS NULL AND range_end_at IS NULL
            AND range_start >= 0 AND range_start <= range_end)
        OR
        (range_start IS NULL AND range_end IS NULL
            AND range_start_at IS NOT NULL AND range_end_at IS NOT NULL
            AND range_start_at <= range_end_at)),
    CONSTRAINT recon_scan_attempt_owner_shape CHECK (length(owner) BETWEEN 1 AND 128)
);
CREATE UNIQUE INDEX recon_scan_attempt_claimed_uniq
    ON recon_scan_attempt (task_id) WHERE state = 'claimed';
CREATE INDEX recon_scan_attempt_lease_idx
    ON recon_scan_attempt (lease_expires_at) WHERE state = 'claimed';
CREATE INDEX recon_scan_attempt_task_idx
    ON recon_scan_attempt (task_id, created_at);

-- Table 10 -- recon_permission (§1.10, F5). Default-deny evaluation source:
-- no row means no permission; unknown actions and out-of-scope access are
-- denied and audited. PK(principal, action, scope_hash) is exact
-- (principal, action, normalized-scope) matching. NO grants are seeded --
-- grants/revokes are deployment-time local-privileged operations that bind
-- the principal from the authenticated caller (contracts/auth-matrix.md
-- Management); existing 011/009/012 permissions are never expanded and no
-- admin role is created.
CREATE TABLE recon_permission (
    principal  TEXT        NOT NULL,
    action     TEXT        NOT NULL,
    scope      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    scope_hash TEXT        NOT NULL,
    granted_by TEXT        NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_permission_pkey PRIMARY KEY (principal, action, scope_hash),
    CONSTRAINT recon_permission_action_check CHECK (action IN (
        'scan_manage', 'exception_handle', 'dispose_ack', 'dispose_reuse', 'close')),
    CONSTRAINT recon_permission_scope_shape CHECK (jsonb_typeof(scope) = 'object'),
    CONSTRAINT recon_permission_principal_shape CHECK (
        length(principal) BETWEEN 1 AND 128),
    CONSTRAINT recon_permission_scope_hash_shape CHECK (
        length(scope_hash) BETWEEN 1 AND 128),
    CONSTRAINT recon_permission_granted_by_shape CHECK (
        length(granted_by) BETWEEN 1 AND 128)
);

-- +goose Down
-- Controlled-window rollback only (see the Up header): drops the ten 014
-- tables and their records after all reconcile-admin invocations stopped and
-- pending work was drained/exported. No 001-015 object is touched; the down
-- is never a substitute for the bounded pause/cancel path.
DROP TABLE IF EXISTS recon_permission;
DROP TABLE IF EXISTS recon_audit;
DROP TABLE IF EXISTS reverify;
DROP TABLE IF EXISTS disposition;
DROP TABLE IF EXISTS discrepancy_occurrence;
DROP TABLE IF EXISTS discrepancy;
DROP TABLE IF EXISTS recon_scan_attempt;
DROP TABLE IF EXISTS recon_gap;
DROP TABLE IF EXISTS recon_checkpoint;
DROP TABLE IF EXISTS recon_task;
