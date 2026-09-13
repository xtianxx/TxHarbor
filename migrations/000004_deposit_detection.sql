-- +goose Up
-- TxHarbor 004-deposit-detection: deposit observation storage
-- (specs/004-deposit-detection/data-model.md, Tables 1-5).
--
-- Invariants enforced here by storage:
--   I1  one observation per source log identity
--       (chain_id, block_hash, tx_hash, log_index): PK. Re-processing
--       converges on the same row via ON CONFLICT DO NOTHING.
--   I1b version attribution: observations carry an FK onto
--       deposit_config_history (chain_id, version_seq), so a row can never
--       reference a version that never existed.
--   FR-05/FR-06 identity shape: config_hash is 64 lowercase hex.
-- The version ledger is append-only by design: rows are never updated or
-- deleted (the application asserts row-count monotonicity). prev_seq points
-- back at an existing same-chain version, so the chain can never dangle.
--
-- Deliberately no foreign key: deposit_checkpoint.next_block routinely points
-- beyond persisted chain_blocks (head chasing, same reasoning as 003), and
-- audit rows must outlive the deposit_pause row they describe (no FK between
-- deposit_pause and deposit_pause_audit).
--
-- All addresses/hashes are stored lowercase 0x-prefixed hex, matching the
-- chain_blocks/erc20_transfer_logs format checks.

CREATE TABLE deposit_config_history (
    chain_id                BIGINT      NOT NULL CHECK (chain_id > 0),
    version_seq             BIGINT      NOT NULL CHECK (version_seq > 0),
    config_hash             CHAR(64)    NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$'),
    prev_seq                BIGINT,
    start_block             BIGINT      NOT NULL CHECK (start_block >= 0),
    assets                  TEXT        NOT NULL,
    watches                 TEXT        NOT NULL,
    replay_from             BIGINT      NOT NULL CHECK (replay_from >= 0),
    operator                TEXT        NOT NULL DEFAULT '',
    reason                  TEXT        NOT NULL DEFAULT '',
    request_id              TEXT,
    expected_pause_id       BIGINT,
    expected_pause_revision BIGINT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, version_seq),
    UNIQUE (chain_id, request_id),
    CHECK ((expected_pause_id IS NULL) = (expected_pause_revision IS NULL)),
    FOREIGN KEY (chain_id, prev_seq)
        REFERENCES deposit_config_history (chain_id, version_seq)
);

-- The first version row is not an authorization request: request_id IS NULL,
-- operator='bootstrap', and it is the only such row per chain. NULLs do not
-- collide under UNIQUE (chain_id, request_id), so the bootstrap row needs its
-- own partial unique constraint. No secondary index on this table: the
-- version chain is read by PK (chain_id, version_seq) and prev_seq walk.
CREATE UNIQUE INDEX deposit_config_history_bootstrap_uniq
    ON deposit_config_history (chain_id)
    WHERE request_id IS NULL;

CREATE TABLE deposit_observations (
    chain_id     BIGINT      NOT NULL CHECK (chain_id > 0),
    block_hash   TEXT        NOT NULL CHECK (block_hash ~ '^0x[0-9a-f]{64}$'),
    tx_hash      TEXT        NOT NULL CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    log_index    BIGINT      NOT NULL CHECK (log_index >= 0),
    block_number BIGINT      NOT NULL CHECK (block_number >= 0),
    contract     TEXT        NOT NULL CHECK (contract ~ '^0x[0-9a-f]{40}$'),
    sender       TEXT        NOT NULL CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    recipient    TEXT        NOT NULL CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    amount       NUMERIC     NOT NULL CHECK (amount > 0),
    status       TEXT        NOT NULL DEFAULT 'pending' CHECK (status = 'pending'),
    version_seq  BIGINT      NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash, tx_hash, log_index),
    FOREIGN KEY (chain_id, version_seq)
        REFERENCES deposit_config_history (chain_id, version_seq)
);

-- 005 consumes observations by height; the recipient index backs the
-- operational "deposits for address X" query (see data-model.md §索引).
CREATE INDEX deposit_observations_height_idx
    ON deposit_observations (chain_id, block_number);
CREATE INDEX deposit_observations_recipient_height_idx
    ON deposit_observations (chain_id, recipient, block_number);

CREATE TABLE deposit_checkpoint (
    chain_id    BIGINT      PRIMARY KEY CHECK (chain_id > 0),
    start_block BIGINT      NOT NULL CHECK (start_block >= 0),
    config_hash CHAR(64)    NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$'),
    next_block  BIGINT      NOT NULL CHECK (next_block >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (next_block >= start_block)
);

-- pause_id is allocated by a sequence that is never recycled: deleting the
-- pause row (release) does not make a future pause reuse its identity, so
-- audit rows stay unambiguous across the pause instance lifecycle.
CREATE SEQUENCE deposit_pause_id_seq AS BIGINT NO CYCLE;

-- Persistent deposit-stream pause: row existence means "paused"; first pause
-- wins. Revision increments on in-place merge updates for the same instance.
CREATE TABLE deposit_pause (
    chain_id   BIGINT      PRIMARY KEY,
    pause_id   BIGINT      NOT NULL DEFAULT nextval('deposit_pause_id_seq') UNIQUE,
    revision   BIGINT      NOT NULL DEFAULT 1,
    height     BIGINT      NOT NULL,
    kind       TEXT        NOT NULL CHECK (kind IN ('upstream_gap', 'chain_view_changed', 'validation_failed')),
    detail     TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only pause instance audit: release/merge events with a full reason
-- snapshot, so the pause is explainable after the active row is gone.
CREATE TABLE deposit_pause_audit (
    chain_id    BIGINT      NOT NULL CHECK (chain_id > 0),
    pause_id    BIGINT      NOT NULL,
    revision    BIGINT      NOT NULL CHECK (revision > 0),
    action      TEXT        NOT NULL CHECK (action IN ('release', 'merge')),
    operator    TEXT        NOT NULL DEFAULT '',
    reason      TEXT        NOT NULL DEFAULT '',
    version_seq BIGINT      NOT NULL,
    kind        TEXT        NOT NULL,
    height      BIGINT      NOT NULL,
    detail      TEXT        NOT NULL DEFAULT '',
    at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, pause_id, revision, action)
);

-- +goose Down
-- Drop in dependency order: deposit_observations references
-- deposit_config_history. The pause_id sequence is not table-owned, so it
-- must be dropped explicitly after the tables that default from it.
DROP TABLE IF EXISTS deposit_pause_audit;
DROP TABLE IF EXISTS deposit_pause;
DROP TABLE IF EXISTS deposit_checkpoint;
DROP TABLE IF EXISTS deposit_observations;
DROP TABLE IF EXISTS deposit_config_history;
DROP SEQUENCE IF EXISTS deposit_pause_id_seq;
