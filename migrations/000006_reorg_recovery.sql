-- +goose Up
-- TxHarbor 006-reorg-recovery: fork-tolerant storage for chain reorganization
-- recovery (specs/006-reorg-recovery/data-model.md, Tables 1-4; research R2/R13).
--
-- Invariants enforced here by storage:
--   I1  at most one canonical row per (chain_id, number): the new partial
--       UNIQUE (replaces the old PK shape; same guarantee as before).
--   I2  checkpoint always points at a stored block: the pre-existing
--       UNIQUE (chain_id, number, hash) is RETAINED untouched below, so the
--       indexer_checkpoint composite FK never loses its target.
--   Orphaned lineage: every Orphaned conversion lands an append-only row in
--       deposit_observation_transitions (repeat execution converges on its
--       UNIQUE instead of duplicating state).
--   Policy monotonicity: reorg_policy_history is append-only by design (rows
--       are never updated or deleted); prev_seq points back at an existing
--       same-chain version, so the chain can never dangle (mirrors 004
--       deposit_config_history / 005 confirmation_policy_history). The
--       effective policy is MAX(policy_seq).
--   Requests are bound by UNIQUE (chain_id, request_id); the bootstrap row
--       (request_id IS NULL, operator='bootstrap') is the single NULL row per
--       chain via a partial unique index (NULLs do not collide under UNIQUE).
--   005 rows/columns are untouched in meaning: only the two CHECK rewrites
--       below plus additive nullable columns plus new tables/indexes.
--
-- Outage boundary (research R13): the PK rebuild takes ACCESS EXCLUSIVE on
--   chain_blocks — plan a maintenance window with ordinary writers stopped.
--   Old binaries are NOT runnable post-migration (ON CONFLICT (chain_id,
--   number) no longer exists; the insert path needs the sibling-aware
--   rewrite in internal/indexer/scanner.go). Deploy 006 code with this
--   migration, no mixed-version fleet. Rollback = code + DB restore point
--   together; live downgrade of the DB alone is unsupported (see T007).

-- Preconditions asserted first, same txn, before DDL (extends the 000005
-- guard idiom): single canonical per height (true by the old PK, asserted
-- anyway); zero rows outside {pending,confirmed} in deposit_observations
-- (000005 already guaranteed; re-asserted).
-- +goose StatementBegin
DO $$
BEGIN
    IF (SELECT count(*) FROM (
        SELECT chain_id, number FROM chain_blocks
        WHERE canonical GROUP BY chain_id, number HAVING count(*) > 1
    ) dup) <> 0 THEN
        RAISE EXCEPTION '000006 upgrade refused: chain_blocks holds multiple canonical rows per height';
    END IF;
    IF (SELECT count(*) FROM deposit_observations WHERE status NOT IN ('pending', 'confirmed')) <> 0 THEN
        RAISE EXCEPTION '000006 upgrade refused: deposit_observations holds rows outside {pending,confirmed}';
    END IF;
END
$$;
-- +goose StatementEnd

-- R2 PK rework (DDL order per R13): CREATE the new unique index -> DROP the
-- old PK -> ADD the new PK USING the index. The old UNIQUE (chain_id,
-- number, hash) constraint is deliberately kept: it stays the
-- indexer_checkpoint FK target, so no FK rewrite is needed.
CREATE UNIQUE INDEX chain_blocks_number_hash_uniq
    ON chain_blocks (chain_id, number, hash);
ALTER TABLE chain_blocks DROP CONSTRAINT chain_blocks_pkey;
ALTER TABLE chain_blocks ADD CONSTRAINT chain_blocks_pkey
    PRIMARY KEY USING INDEX chain_blocks_number_hash_uniq;

-- I1 preserved at the storage layer: at most one canonical row per height.
-- A plain unique index (not a constraint) suffices: no FK references it.
CREATE UNIQUE INDEX chain_blocks_canonical_uniq
    ON chain_blocks (chain_id, number) WHERE canonical;

-- Orphan-evidence columns first: the 3-state CHECK rewrite below references
-- them, so they must exist before the rewrite (validated, not NOT VALID, so
-- existing rows are proven compliant).
ALTER TABLE deposit_observations
    ADD COLUMN orphaned_at TIMESTAMPTZ,
    ADD COLUMN orphan_recovery_id TEXT,
    ADD COLUMN orphan_reason TEXT;

-- Widen the status set for Pending/Confirmed -> Orphaned conversion. The 005
-- inline CHECK is auto-named deposit_observations_status_check; keep the name
-- so a later migration can find and widen it again.
ALTER TABLE deposit_observations
    DROP CONSTRAINT deposit_observations_status_check;
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_status_check
    CHECK (status IN ('pending', 'confirmed', 'orphaned'));

-- Rewrite the 005 2-state predicate as 3-state (data-model §Altered
-- tables): pending rows carry no conversion facts; confirmed rows carry the
-- full six-column basis; orphaned rows carry orphan evidence NOT NULL while
-- the basis columns are retained untouched as history evidence (never
-- cleared on orphan; readers key effectiveness off status, never off column
-- non-NULLness).
ALTER TABLE deposit_observations
    DROP CONSTRAINT deposit_observations_confirmation_consistency;
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_confirmation_consistency
    CHECK (
        (status = 'pending') = (confirmed_at IS NULL AND orphaned_at IS NULL)
        AND (status = 'pending' OR (
            status = 'confirmed'
            AND confirmed_at IS NOT NULL
            AND confirm_tip_number IS NOT NULL
            AND confirm_tip_hash IS NOT NULL
            AND confirm_threshold IS NOT NULL
            AND confirmations IS NOT NULL
            AND confirm_policy_seq IS NOT NULL
        ) OR (
            status = 'orphaned'
            AND orphaned_at IS NOT NULL
            AND orphan_recovery_id IS NOT NULL
        ))
    );

-- Table 3 — reorg_policy_history (depth-config versions, mirrors
-- confirmation_policy_history exactly: PK, self-FK chain, UNIQUE
-- request_id, bootstrap partial-unique, max-seq = effective).
CREATE TABLE reorg_policy_history (
    chain_id         BIGINT      NOT NULL CHECK (chain_id > 0),
    policy_seq       BIGINT      NOT NULL CHECK (policy_seq > 0),
    max_depth        BIGINT      NOT NULL CHECK (max_depth > 0),
    prev_seq         BIGINT,
    operator         TEXT        NOT NULL DEFAULT '',
    reason           TEXT        NOT NULL DEFAULT '',
    request_id       TEXT,
    expected_old_seq BIGINT      NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, policy_seq),
    UNIQUE (chain_id, request_id),
    FOREIGN KEY (chain_id, prev_seq)
        REFERENCES reorg_policy_history (chain_id, policy_seq)
);

-- The first policy row is not an authorization request: request_id IS NULL,
-- operator='bootstrap', written by the first establish transaction. It is the
-- only such row per chain. No secondary index on this table: the policy
-- chain is read by PK (chain_id, policy_seq) and MAX(policy_seq).
CREATE UNIQUE INDEX reorg_policy_history_bootstrap_uniq
    ON reorg_policy_history (chain_id)
    WHERE request_id IS NULL;

-- Table 1 — reorg_recovery (active recovery instance, one row per chain).
-- Repeat triggers converge (INSERT ... ON CONFLICT DO NOTHING, loser
-- re-reads and joins). Terminal release = row DELETE (only by
-- complete_reverify or auth_release) + terminal event in Table 2. History
-- lives in Table 2, never in this table.
CREATE TABLE reorg_recovery (
    chain_id         BIGINT      NOT NULL CHECK (chain_id > 0),
    recovery_id      TEXT        NOT NULL UNIQUE,
    phase            TEXT        NOT NULL CHECK (phase IN ('detected', 'ancestor_confirmed', 'invalidated', 'replaying', 'complete_pending', 'reconcile_required')),
    policy_seq       BIGINT      NOT NULL CHECK (policy_seq > 0),
    max_depth        BIGINT      NOT NULL CHECK (max_depth > 0),
    bound_old_number BIGINT      NOT NULL CHECK (bound_old_number >= 0),
    bound_old_hash   TEXT        NOT NULL CHECK (bound_old_hash ~ '^0x[0-9a-f]{64}$'),
    ancestor_number  BIGINT      CHECK (ancestor_number >= 0),
    ancestor_hash    TEXT        CHECK (ancestor_hash IS NULL OR ancestor_hash ~ '^0x[0-9a-f]{64}$'),
    new_tip_number   BIGINT,
    new_tip_hash     TEXT,
    block_frontier   BIGINT,
    log_frontier     BIGINT,
    deposit_frontier BIGINT,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    recovery_seq     BIGINT      NOT NULL CHECK (recovery_seq > 0 AND recovery_seq < 9223372036854775807),
    PRIMARY KEY (chain_id),
    FOREIGN KEY (chain_id, policy_seq)
        REFERENCES reorg_policy_history (chain_id, policy_seq),
    CHECK ((ancestor_number IS NULL) = (ancestor_hash IS NULL)),
    CHECK (ancestor_number IS NULL OR (bound_old_number - ancestor_number >= 0 AND bound_old_number - ancestor_number <= max_depth))
);

-- Table 2 — reorg_recovery_events (append-only audit; rows never
-- UPDATE/DELETE; per-recovery_id lifecycle traceable after the active row is
-- gone; recovery_id carries no FK so rows survive the release DELETE).
CREATE TABLE reorg_recovery_events (
    chain_id     BIGINT      NOT NULL CHECK (chain_id > 0),
    recovery_id  TEXT        NOT NULL,
    recovery_seq BIGINT      NOT NULL CHECK (recovery_seq > 0),
    event_seq    BIGINT      NOT NULL CHECK (event_seq > 0),
    event        TEXT        NOT NULL CHECK (event IN ('established', 'ancestor_confirmed', 'blocks_invalidated', 'observations_invalidated', 'checkpoints_rolled_back', 'replay_progress', 'observation_revived', 'auto_completed', 'repair_authorized', 'released', 'reconcile_signaled')),
    detail       TEXT        NOT NULL DEFAULT '',
    at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, recovery_id, event_seq)
);

-- Table 4 — deposit_observation_transitions (status transition log for
-- repeated cycles; repeat execution converges on the UNIQUE: the second
-- write conflicts and is already recorded). No FK to observations (rows
-- outlive any state).
CREATE TABLE deposit_observation_transitions (
    chain_id      BIGINT      NOT NULL CHECK (chain_id > 0),
    block_hash    TEXT        NOT NULL CHECK (block_hash ~ '^0x[0-9a-f]{64}$'),
    tx_hash       TEXT        NOT NULL CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    log_index     BIGINT      NOT NULL CHECK (log_index >= 0),
    from_status   TEXT        NOT NULL CHECK (from_status IN ('pending', 'confirmed', 'orphaned')),
    to_status     TEXT        NOT NULL CHECK (to_status IN ('pending', 'confirmed', 'orphaned')),
    recovery_id   TEXT        NOT NULL,
    basis_snapshot TEXT       NOT NULL DEFAULT '',
    at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id)
);

-- +goose Down
-- Reverse dependency order (cf. 000005 Down) as authoring hygiene only — its
-- bounds are T007's, not a production rollback path: exercised ONLY on
-- scratch DBs without fork history during authoring. Downgrade after
-- fork-history rows exist is unsupported (it would destroy audit evidence)
-- and MUST NOT be described as lossless; production rollback = code + DB
-- restore point per R13.
DROP TABLE IF EXISTS deposit_observation_transitions;
DROP TABLE IF EXISTS reorg_recovery_events;
DROP TABLE IF EXISTS reorg_recovery;
DROP INDEX IF EXISTS reorg_policy_history_bootstrap_uniq;
DROP TABLE IF EXISTS reorg_policy_history;
ALTER TABLE deposit_observations DROP CONSTRAINT IF EXISTS deposit_observations_confirmation_consistency;
ALTER TABLE deposit_observations DROP CONSTRAINT IF EXISTS deposit_observations_status_check;
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_status_check CHECK (status IN ('pending', 'confirmed'));
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_confirmation_consistency
    CHECK (
        (status = 'pending') = (confirmed_at IS NULL)
        AND (status = 'pending' OR (
            confirmed_at IS NOT NULL
            AND confirm_tip_number IS NOT NULL
            AND confirm_tip_hash IS NOT NULL
            AND confirm_threshold IS NOT NULL
            AND confirmations IS NOT NULL
            AND confirm_policy_seq IS NOT NULL
        ))
    );
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS orphan_reason;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS orphan_recovery_id;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS orphaned_at;
DROP INDEX IF EXISTS chain_blocks_canonical_uniq;
ALTER TABLE chain_blocks DROP CONSTRAINT IF EXISTS chain_blocks_pkey;
DROP INDEX IF EXISTS chain_blocks_number_hash_uniq;
ALTER TABLE chain_blocks ADD CONSTRAINT chain_blocks_pkey PRIMARY KEY (chain_id, number);
