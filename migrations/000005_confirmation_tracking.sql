-- +goose Up
-- TxHarbor 005-confirmation-tracking: confirmation policy versions + observation confirmation basis
-- (specs/005-confirmation-tracking/data-model.md, Tables 1-2).
--
-- Invariants enforced here by storage:
--   I1  one confirmation conversion per source log identity (PK, unchanged).
--   I2  first-seen time and basis immutable: confirmed rows carry all six
--       basis columns NOT NULL; the application never UPDATEs confirmed rows.
--       Like the 004 append-only precedent, this is enforced by the
--       application plus tests, with no triggers: a trigger would be
--       demolition work a later status-widening migration must undo first.
--   Policy monotonicity: confirmation_policy_history is append-only by
--   design (rows are never updated or deleted); prev_seq points back at an
--   existing same-chain version, so the chain can never dangle (mirrors 004
--   deposit_config_history). The effective policy is MAX(policy_seq).
--   Requests are bound by UNIQUE (chain_id, request_id); the bootstrap row
--   (request_id IS NULL, operator='bootstrap') is the single NULL row per
--   chain via a partial unique index (NULLs do not collide under UNIQUE).
-- OI-1 (2026-09-14): confirmations is NUMERIC, exact decimal, integer-only
--   via CHECK (confirmations = floor(confirmations)) plus CHECK (>= 0),
--   mirroring the 004 amount NUMERIC precedent. Go-side decimal-string
--   conversion and the 2^63 exact-write proof belong to a later task.
-- The status set holds exactly ('pending','confirmed') here; widening it
-- further is a later migration's own DDL, not a placeholder here.

CREATE TABLE confirmation_policy_history (
    chain_id         BIGINT      NOT NULL CHECK (chain_id > 0),
    policy_seq       BIGINT      NOT NULL CHECK (policy_seq > 0),
    threshold        BIGINT      NOT NULL CHECK (threshold > 0),
    prev_seq         BIGINT,
    operator         TEXT        NOT NULL DEFAULT '',
    reason           TEXT        NOT NULL DEFAULT '',
    request_id       TEXT,
    expected_old_seq BIGINT      NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, policy_seq),
    UNIQUE (chain_id, request_id),
    FOREIGN KEY (chain_id, prev_seq)
        REFERENCES confirmation_policy_history (chain_id, policy_seq)
);

-- The first policy row is not an authorization request: request_id IS NULL,
-- operator='bootstrap', and it is the only such row per chain. NULLs do not
-- collide under UNIQUE (chain_id, request_id), so the bootstrap row needs its
-- own partial unique constraint. No secondary index on this table: the
-- policy chain is read by PK (chain_id, policy_seq) and MAX(policy_seq).
CREATE UNIQUE INDEX confirmation_policy_history_bootstrap_uniq
    ON confirmation_policy_history (chain_id)
    WHERE request_id IS NULL;

-- 005 is additive on deposit_observations: 004 column definitions are
-- untouched; only new basis columns are added.
ALTER TABLE deposit_observations
    ADD COLUMN confirmed_at TIMESTAMPTZ,
    ADD COLUMN confirm_tip_number BIGINT CHECK (confirm_tip_number >= 0),
    ADD COLUMN confirm_tip_hash TEXT CHECK (confirm_tip_hash ~ '^0x[0-9a-f]{64}$'),
    ADD COLUMN confirm_threshold BIGINT CHECK (confirm_threshold > 0),
    ADD COLUMN confirmations NUMERIC CHECK (confirmations >= 0 AND confirmations = floor(confirmations)),
    ADD COLUMN confirm_policy_seq BIGINT CHECK (confirm_policy_seq > 0);

-- The new policy version reference is composite: a version only exists as a
-- (chain_id, policy_seq) pair in confirmation_policy_history.
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_confirm_policy_seq_fkey
    FOREIGN KEY (chain_id, confirm_policy_seq)
    REFERENCES confirmation_policy_history (chain_id, policy_seq);

-- The 004 status CHECK already guarantees every existing row is pending, so
-- no history can be fabricated and no backfill is needed: assert it.
-- +goose StatementBegin
DO $$
BEGIN
    IF (SELECT count(*) FROM deposit_observations WHERE status <> 'pending') <> 0 THEN
        RAISE EXCEPTION '000005 upgrade refused: deposit_observations holds non-pending rows';
    END IF;
END
$$;
-- +goose StatementEnd

-- Widen the status set for Pending -> Confirmed conversion. The 004 inline
-- CHECK is auto-named deposit_observations_status_check; keep the name so a
-- later migration can find and widen it again.
ALTER TABLE deposit_observations
    DROP CONSTRAINT deposit_observations_status_check;
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_status_check
    CHECK (status IN ('pending', 'confirmed'));

-- Pending rows carry no conversion facts; confirmed rows carry all six.
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

-- Candidate scan: pending rows in chain/height order; confirmed rows never
-- enter this index, so it does not grow with confirmations.
CREATE INDEX deposit_observations_pending_height_idx
    ON deposit_observations (chain_id, block_number)
    WHERE status = 'pending';

-- +goose Down
-- Reverse dependency order: the pending-scan index and the confirmation
-- constraints go first, then the widened status set is narrowed back to the
-- 004 shape, then the basis columns (dropping the policy FK with them), then
-- the policy history table they referenced.
DROP INDEX IF EXISTS deposit_observations_pending_height_idx;
ALTER TABLE deposit_observations DROP CONSTRAINT IF EXISTS deposit_observations_confirmation_consistency;
ALTER TABLE deposit_observations DROP CONSTRAINT IF EXISTS deposit_observations_confirm_policy_seq_fkey;
ALTER TABLE deposit_observations DROP CONSTRAINT IF EXISTS deposit_observations_status_check;
ALTER TABLE deposit_observations
    ADD CONSTRAINT deposit_observations_status_check CHECK (status = 'pending');
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS confirm_policy_seq;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS confirmations;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS confirm_threshold;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS confirm_tip_hash;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS confirm_tip_number;
ALTER TABLE deposit_observations DROP COLUMN IF EXISTS confirmed_at;
DROP TABLE IF EXISTS confirmation_policy_history;
