-- +goose Up
-- TxHarbor 015 B1 (T006): recovery control-store initial schema.
--
-- Creates the 11 recovery_* entities of
-- specs/015-backup-recovery-safe-resumption/data-model.md §1 with the
-- constraints preserved from that model and from tasks.md T006.
--
-- Scope: this embedded goose sequence applies ONLY to the independent control
-- database (TXHARBOR_RECOVERY_CONTROL_DSN) through `recovery-admin migrate`.
-- The data DB migrations/ package and its goose_db_version table are never
-- touched: 015 makes zero data-DB schema changes (ADR-001, plan.md).
--
-- Invariants encoded at the storage layer and their enforcement boundary:
--   * NO preset rows: the migration inserts nothing. A fresh control store has
--     no participants, identity mappings, approvals, releases or instances —
--     absent rows mean "not released" and the gate refuses (fail-closed).
--   * NO amount/money column exists anywhere in this model (data-model storage
--     section): the control store carries governance facts only (instances,
--     people, evidence, verification items, gaps, isolation checks, approvals,
--     releases, audit, drill metrics), never money.
--   * Closed sets are TEXT + CHECK, never PG enum types: widening a set is an
--     explicit, reviewable migration, never an implicit value.
--   * Append-only tables (recovery_verification_item, recovery_approval,
--     recovery_release, recovery_audit) have no UPDATE/DELETE path in any 015
--     command. Release validity is deliberately NOT a column: recovery_release
--     stores append-only release/revoke rows and the gate re-derives validity
--     on every evaluation (INV-2), so a direct row write that lacks the
--     conditions never produces a release.
--   * recovery_instance.evidence_generation is the evidence-generation token
--     (data-model §5): it starts at 0 and advances by at most +1 per accepted
--     write, in the same transaction as the result row; the store (T007)
--     re-reads it under the instance row lock and discards mismatched commits
--     (audit result='discarded'). instance_id is immutable and never reused:
--     instance rows are inserted, never re-pointed or deleted.
--   * recovery_isolation_check.verified_by MUST differ from this instance's
--     executor (recovery_instance.opened_by / participant role='executor',
--     including the same person under another principal): checklist-verify
--     (T029) refuses self-verification, and the gate additionally requires the
--     whole isolation dependency set to be verified.
--   * recovery_release.approval_refs is written in deterministic ascending,
--     de-duplicated order so the recorded approval basis is reproducible; it
--     is the recorded basis, never a validity flag.
--   * Control-store schema version unknown/incompatible is refused by the
--     migrate and store read paths (T069) — never read best-effort.
--
-- Ordering/validity is NEVER decided by created_at (data-model §5/§6): the
-- commit order under the instance row lock is authoritative; created_at is
-- display/audit metadata only.

-- ---------------------------------------------------------------------------
-- 1. recovery_instance — recovery instances; at most one open globally.
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_instance (
    instance_id            UUID        NOT NULL,
    kind                   TEXT        NOT NULL,
    state                  TEXT        NOT NULL,
    supersedes_instance_id UUID,
    restore_point          JSONB,
    data_target            JSONB,
    evidence_generation    BIGINT      NOT NULL DEFAULT 0,
    evidence_hash          TEXT        NOT NULL,
    opened_by              TEXT        NOT NULL,
    opened_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_by              TEXT,
    closed_at              TIMESTAMPTZ,
    reason                 TEXT        NOT NULL DEFAULT '',
    CONSTRAINT recovery_instance_pkey PRIMARY KEY (instance_id),
    CONSTRAINT recovery_instance_kind_check CHECK (kind IN ('recovery', 'baseline')),
    CONSTRAINT recovery_instance_state_check CHECK (state IN ('open', 'closed')),
    CONSTRAINT recovery_instance_evidence_generation_check CHECK (evidence_generation >= 0),
    -- Closure is all-or-nothing: an open instance carries no closure facts, a
    -- closed one records who closed it and when (data-model §4.1).
    CONSTRAINT recovery_instance_closure_check CHECK (
        (state = 'closed') = (closed_by IS NOT NULL AND closed_at IS NOT NULL)
    )
);

-- INV-1: at most one open instance globally (partial unique index); closed
-- rows do not collide, so instance history accumulates and ids are never
-- reused. `baseline` is documentation-only deployment state; the gate arms
-- `recovery` instances.
CREATE UNIQUE INDEX recovery_instance_one_open_uniq
    ON recovery_instance (state) WHERE state = 'open';

-- ---------------------------------------------------------------------------
-- 2. recovery_identity — person<->principal mapping (deployment-controlled).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_identity (
    principal   TEXT        NOT NULL,
    person_id   TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    recorded_by TEXT        NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Revocation keeps the row (active=FALSE) so an old mapping can never be
    -- silently reused as proof. A principal without an active mapping is
    -- "cannot prove a distinct person" and approvals refuse (approval-matrix
    -- §2/F19). This table is maintained only by the deployment-controlled
    -- management path and never from the rolled-back data DB.
    active      BOOLEAN     NOT NULL,
    CONSTRAINT recovery_identity_pkey PRIMARY KEY (principal),
    CONSTRAINT recovery_identity_principal_check CHECK (principal ~ '^[a-z][a-z0-9_-]*:[^[:space:]]+$'),
    CONSTRAINT recovery_identity_person_id_check CHECK (person_id <> '')
);

CREATE INDEX recovery_identity_person_idx ON recovery_identity (person_id);

-- ---------------------------------------------------------------------------
-- 3. recovery_participant — per-instance executor/verifier/approver bindings.
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_participant (
    instance_id    UUID        NOT NULL,
    principal      TEXT        NOT NULL,
    person_id      TEXT        NOT NULL,
    role           TEXT        NOT NULL,
    bound_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    binding_source TEXT        NOT NULL,
    proof_ref      TEXT        NOT NULL DEFAULT '',
    CONSTRAINT recovery_participant_pkey PRIMARY KEY (instance_id, principal, role),
    CONSTRAINT recovery_participant_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    CONSTRAINT recovery_participant_principal_check CHECK (principal ~ '^[a-z][a-z0-9_-]*:[^[:space:]]+$'),
    CONSTRAINT recovery_participant_role_check CHECK (role IN ('executor', 'verifier', 'approver')),
    CONSTRAINT recovery_participant_binding_source_check CHECK (binding_source IN ('deploy_config', 'auth_principal')),
    -- Registration requires a resolved identity mapping: person_id is NOT NULL
    -- and non-empty, so a missing mapping refuses registration instead of
    -- storing an unprovable person (data-model §1.2).
    CONSTRAINT recovery_participant_person_id_check CHECK (person_id <> '')
);

-- ---------------------------------------------------------------------------
-- 4. recovery_evidence — evidence snapshots/batches.
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_evidence (
    evidence_id   UUID        NOT NULL,
    -- Nullable by design (DG-2/F7): verify-backup may record a manifest-level
    -- conclusion before any instance is open; the row is explicitly bound to
    -- the open instance later by a controlled command + audit. A bound row is
    -- never re-pointed — copying the manifest, rebuilding the target or
    -- changing the target fingerprint requires a new probe/evidence row.
    instance_id   UUID,
    generation    BIGINT      NOT NULL,
    kind          TEXT        NOT NULL,
    scope         JSONB       NOT NULL,
    artifact_hash TEXT        NOT NULL,
    artifact_ref  TEXT        NOT NULL,
    observed_at   TIMESTAMPTZ NOT NULL,
    collected_by  TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_evidence_pkey PRIMARY KEY (evidence_id),
    CONSTRAINT recovery_evidence_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    -- Closed set: the four kinds named by data-model §1.4; widening it is a
    -- spec change (new migration), never free text.
    CONSTRAINT recovery_evidence_kind_check CHECK (kind IN (
        'backup_manifest', 'restore_probe', 'verification_batch', 'isolation_check')),
    CONSTRAINT recovery_evidence_generation_check CHECK (generation >= 0)
);

CREATE INDEX recovery_evidence_instance_kind_idx
    ON recovery_evidence (instance_id, kind);

-- ---------------------------------------------------------------------------
-- 5. recovery_verification_item — V1-V9 conclusions (append-only).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_verification_item (
    item_id       UUID        NOT NULL,
    instance_id   UUID        NOT NULL,
    generation    BIGINT      NOT NULL,
    category      TEXT        NOT NULL,
    object_key    TEXT        NOT NULL,
    scope         JSONB       NOT NULL,
    sources       JSONB       NOT NULL,
    conclusion    TEXT        NOT NULL,
    reason        TEXT,
    evidence_refs TEXT[]      NOT NULL DEFAULT '{}',
    observed_at   TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_verification_item_pkey PRIMARY KEY (item_id),
    CONSTRAINT recovery_verification_item_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    CONSTRAINT recovery_verification_item_category_check CHECK (category IN (
        'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9')),
    CONSTRAINT recovery_verification_item_conclusion_check CHECK (conclusion IN (
        'consistent', 'divergent', 'unknown', 'stale')),
    CONSTRAINT recovery_verification_item_generation_check CHECK (generation >= 0),
    -- unknown/stale must say why and must carry the sources consulted; an
    -- empty sources payload is not evidence (FR-016/FR-018). Re-verification
    -- appends a new row (append-only); latest-row-wins only after the
    -- generation token check of data-model §5.
    CONSTRAINT recovery_verification_item_unknown_evidence_check CHECK (
        conclusion NOT IN ('unknown', 'stale')
        OR (reason IS NOT NULL
            AND btrim(reason) <> ''
            AND sources <> '{}'::JSONB
            AND sources <> '[]'::JSONB
            AND sources <> 'null'::JSONB)
    )
);

CREATE INDEX recovery_verification_item_instance_category_idx
    ON recovery_verification_item (instance_id, category);

-- ---------------------------------------------------------------------------
-- 6. recovery_gap — evidence gaps blocking capabilities (FR-016/019).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_gap (
    gap_id                UUID        NOT NULL,
    instance_id           UUID        NOT NULL,
    object_key            TEXT        NOT NULL,
    scope                 JSONB       NOT NULL,
    timeline              JSONB       NOT NULL,
    existing_evidence     JSONB       NOT NULL,
    required_evidence     JSONB       NOT NULL,
    affected_capabilities TEXT[]      NOT NULL,
    dependency_proof      JSONB,
    state                 TEXT        NOT NULL,
    closed_by             TEXT,
    closed_at             TIMESTAMPTZ,
    closure_evidence      JSONB,
    owner                 TEXT        NOT NULL DEFAULT '',
    escalation_ref        TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_gap_pkey PRIMARY KEY (gap_id),
    CONSTRAINT recovery_gap_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    -- timeout/attempts_exhausted/acknowledged are audit reasons, NOT states:
    -- they never close or escalate a gap by themselves (FR-019).
    CONSTRAINT recovery_gap_state_check CHECK (state IN ('open', 'closed', 'escalated')),
    -- A gap blocks at least its directly related capability; the array is a
    -- subset of the closed 7-capability set. dependency_proof may add
    -- conservative dependents, never narrow the capability vocabulary.
    CONSTRAINT recovery_gap_affected_capabilities_check CHECK (
        cardinality(affected_capabilities) > 0
        AND affected_capabilities <@ ARRAY[
            'query', 'chain_scan', 'deposit_confirmation',
            'existing_withdrawal_recovery', 'new_withdrawal_creation',
            'event_publishing', 'event_consuming']::TEXT[]
    ),
    -- Closure only by new evidence: a closed gap names the closer, the time
    -- and the closure evidence.
    CONSTRAINT recovery_gap_closure_check CHECK (
        state <> 'closed'
        OR (closed_by IS NOT NULL AND closed_at IS NOT NULL AND closure_evidence IS NOT NULL)
    )
);

CREATE INDEX recovery_gap_instance_state_idx ON recovery_gap (instance_id, state);

-- ---------------------------------------------------------------------------
-- 7. recovery_isolation_check — isolation checklist (FR-010/011/012).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_isolation_check (
    check_id           UUID        NOT NULL,
    instance_id        UUID        NOT NULL,
    item_key           TEXT        NOT NULL,
    state              TEXT        NOT NULL,
    evidence_ref       TEXT,
    checkpoint_summary JSONB,
    checked_by         TEXT,
    checked_at         TIMESTAMPTZ,
    verified_by        TEXT,
    verified_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_isolation_check_pkey PRIMARY KEY (check_id),
    CONSTRAINT recovery_isolation_check_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    -- One row per item per instance: the state machine (pending -> evidenced ->
    -- verified, or rejected for re-collection) transitions in place, and the
    -- gate reads exactly this row.
    CONSTRAINT recovery_isolation_check_instance_item_uniq UNIQUE (instance_id, item_key),
    CONSTRAINT recovery_isolation_check_item_key_check CHECK (item_key IN (
        'old_writers_stopped', 'writer_fencing_observed', 'network_isolation',
        'version_compatible', 'no_pre_release_effects', 'authorization_recheck')),
    CONSTRAINT recovery_isolation_check_state_check CHECK (state IN (
        'pending', 'evidenced', 'verified', 'rejected')),
    -- An evidenced/verified item must carry its external evidence reference:
    -- a state field alone is never proof (contracts/resumption-gate.md §3).
    CONSTRAINT recovery_isolation_check_evidence_check CHECK (
        state NOT IN ('evidenced', 'verified')
        OR (evidence_ref IS NOT NULL AND btrim(evidence_ref) <> '')
    ),
    CONSTRAINT recovery_isolation_check_checked_pair_check CHECK (
        (checked_by IS NULL) = (checked_at IS NULL)
    ),
    CONSTRAINT recovery_isolation_check_verified_pair_check CHECK (
        (verified_by IS NULL) = (verified_at IS NULL)
    ),
    -- Only verified/rejected carry a verifier, and a verified item must name
    -- one. The verifier MUST NOT be this instance's executor (including the
    -- same person under another principal): enforced by checklist-verify
    -- (T029) and re-checked by the gate; the schema cannot express the
    -- cross-row comparison.
    CONSTRAINT recovery_isolation_check_verifier_check CHECK (
        state IN ('verified', 'rejected')
        OR (verified_by IS NULL AND verified_at IS NULL)
    ),
    CONSTRAINT recovery_isolation_check_verified_by_check CHECK (
        state <> 'verified' OR verified_by IS NOT NULL
    )
);

-- ---------------------------------------------------------------------------
-- 8. recovery_approval — approval decisions (append-only, FR-023/024).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_approval (
    approval_id             UUID        NOT NULL,
    instance_id             UUID        NOT NULL,
    capability              TEXT        NOT NULL,
    scope_hash              TEXT        NOT NULL,
    decision                TEXT        NOT NULL,
    approval_class_snapshot TEXT        NOT NULL,
    principal               TEXT        NOT NULL,
    person_id               TEXT        NOT NULL,
    reason                  TEXT        NOT NULL DEFAULT '',
    evidence_generation     BIGINT      NOT NULL,
    evidence_hash           TEXT        NOT NULL,
    operation_id            TEXT        NOT NULL,
    supersedes_approval_id  UUID,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_approval_pkey PRIMARY KEY (approval_id),
    CONSTRAINT recovery_approval_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    -- operation_id is the idempotency key: same input reads back, a different
    -- input conflicts with zero writes (data-model §6).
    CONSTRAINT recovery_approval_operation_id_uniq UNIQUE (operation_id),
    CONSTRAINT recovery_approval_capability_check CHECK (capability IN (
        'query', 'chain_scan', 'deposit_confirmation',
        'existing_withdrawal_recovery', 'new_withdrawal_creation',
        'event_publishing', 'event_consuming')),
    CONSTRAINT recovery_approval_decision_check CHECK (decision IN ('approve', 'revoke')),
    CONSTRAINT recovery_approval_class_check CHECK (approval_class_snapshot IN (
        'single_non_executor', 'dual_non_executor')),
    CONSTRAINT recovery_approval_evidence_generation_check CHECK (evidence_generation >= 0)
);

CREATE INDEX recovery_approval_instance_capability_idx
    ON recovery_approval (instance_id, capability);

-- ---------------------------------------------------------------------------
-- 9. recovery_release — release/revoke decisions (append-only, FR-021/022).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_release (
    release_id            UUID        NOT NULL,
    instance_id           UUID        NOT NULL,
    capability            TEXT        NOT NULL,
    scope_hash            TEXT        NOT NULL,
    decision              TEXT        NOT NULL,
    evidence_generation   BIGINT      NOT NULL,
    evidence_hash         TEXT        NOT NULL,
    approval_refs         UUID[]      NOT NULL DEFAULT '{}',
    operation_id          TEXT        NOT NULL,
    supersedes_release_id UUID,
    reason                TEXT        NOT NULL DEFAULT '',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_release_pkey PRIMARY KEY (release_id),
    CONSTRAINT recovery_release_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    CONSTRAINT recovery_release_operation_id_uniq UNIQUE (operation_id),
    CONSTRAINT recovery_release_capability_check CHECK (capability IN (
        'query', 'chain_scan', 'deposit_confirmation',
        'existing_withdrawal_recovery', 'new_withdrawal_creation',
        'event_publishing', 'event_consuming')),
    CONSTRAINT recovery_release_decision_check CHECK (decision IN ('release', 'revoke')),
    CONSTRAINT recovery_release_evidence_generation_check CHECK (evidence_generation >= 0)
);

-- No `released` boolean exists on purpose: current validity = the latest
-- release row for (instance, capability, scope) not covered by a later
-- explicit revoke AND every gate condition re-derived at evaluation time
-- (INV-2). approval_refs are written sorted ascending and de-duplicated.
CREATE INDEX recovery_release_instance_capability_idx
    ON recovery_release (instance_id, capability);

-- ---------------------------------------------------------------------------
-- 10. recovery_audit — append-only audit (refusals and discards included).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_audit (
    audit_id            BIGSERIAL,
    instance_id         UUID,
    actor               TEXT        NOT NULL,
    action              TEXT        NOT NULL,
    target              JSONB,
    detail              JSONB,
    result              TEXT        NOT NULL,
    refusal_class       TEXT,
    evidence_generation BIGINT,
    operation_id        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT recovery_audit_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    CONSTRAINT recovery_audit_result_check CHECK (result IN ('ok', 'refused', 'discarded', 'failed')),
    -- Closed refusal set of the gate (data-model §3.3); NULL is allowed for
    -- outcomes that are not gate refusals (ok/discarded/failed). Gate
    -- refusals always carry a class from this set.
    CONSTRAINT recovery_audit_refusal_class_check CHECK (refusal_class IS NULL OR refusal_class IN (
        'no_instance', 'instance_mismatch', 'no_release', 'release_invalidated_generation',
        'release_revoked', 'capability_dependency_closed', 'isolation_unproven', 'gap_open',
        'approval_missing', 'approval_identity_unverified', 'approval_executor_excluded',
        'approval_stale', 'hard_gate_active', 'control_store_unavailable', 'scope_mismatch')),
    CONSTRAINT recovery_audit_evidence_generation_check CHECK (
        evidence_generation IS NULL OR evidence_generation >= 0
    )
);

CREATE INDEX recovery_audit_instance_created_idx ON recovery_audit (instance_id, created_at);
CREATE INDEX recovery_audit_operation_idx ON recovery_audit (operation_id);

-- ---------------------------------------------------------------------------
-- 11. recovery_drill_run — drill metrics (FR-030/031/036).
-- ---------------------------------------------------------------------------
CREATE TABLE recovery_drill_run (
    drill_id                   UUID        NOT NULL,
    instance_id                UUID        NOT NULL,
    scenario                   TEXT        NOT NULL,
    recovery_point             JSONB       NOT NULL,
    db_restore_seconds         NUMERIC,
    verification_seconds       NUMERIC,
    capability_release_seconds JSONB,
    backup_lag                 JSONB       NOT NULL,
    uncovered_interval         JSONB       NOT NULL,
    constraints_configured     BOOLEAN     NOT NULL,
    test_inputs                JSONB       NOT NULL,
    gap_counts                 JSONB,
    result                     TEXT        NOT NULL,
    log_ref                    TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recovery_drill_run_pkey PRIMARY KEY (drill_id),
    CONSTRAINT recovery_drill_run_instance_fkey FOREIGN KEY (instance_id)
        REFERENCES recovery_instance (instance_id),
    CONSTRAINT recovery_drill_run_result_check CHECK (result IN ('ok', 'refused_safe', 'failed_injected')),
    -- Separate timing scopes: DB restore and verification are separate
    -- columns; neither alone may claim RTO.
    -- backup_lag/uncovered_interval may be unknown but
    -- are never omitted (NOT NULL, encode "unknown" explicitly).
    CONSTRAINT recovery_drill_run_db_restore_seconds_check CHECK (
        db_restore_seconds IS NULL OR db_restore_seconds >= 0
    ),
    CONSTRAINT recovery_drill_run_verification_seconds_check CHECK (
        verification_seconds IS NULL OR verification_seconds >= 0
    )
);

CREATE INDEX recovery_drill_run_instance_idx ON recovery_drill_run (instance_id);

-- +goose Down
-- Reverse dependency order. Downgrade drops only the 015 control-store
-- objects: the data DB migrations/ package and its goose_db_version table are
-- never touched. No pre-existing object is altered.
DROP TABLE IF EXISTS recovery_drill_run;
DROP TABLE IF EXISTS recovery_audit;
DROP TABLE IF EXISTS recovery_release;
DROP TABLE IF EXISTS recovery_approval;
DROP TABLE IF EXISTS recovery_isolation_check;
DROP TABLE IF EXISTS recovery_gap;
DROP TABLE IF EXISTS recovery_verification_item;
DROP TABLE IF EXISTS recovery_evidence;
DROP TABLE IF EXISTS recovery_participant;
DROP TABLE IF EXISTS recovery_identity;
DROP TABLE IF EXISTS recovery_instance;
