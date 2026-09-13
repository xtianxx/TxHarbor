-- +goose Up
-- TxHarbor 003-event-indexing: log storage (specs/003-event-indexing/data-model.md).
--
-- Invariants enforced here by storage:
--   I1  one row per (chain_id, block_hash, tx_hash, log_index): PK.
--   I1b no two different logs share a log index within one block:
--       UNIQUE (chain_id, block_hash, log_index).
--   FR-05 config identity shape: config_hash is 64 lowercase hex.
-- I2/I3/I4/I6 are application-level by design (see data-model.md):
-- log_checkpoint deliberately has NO foreign key into chain_blocks because
-- next_block routinely points beyond persisted blocks (head chasing would
-- otherwise never commit).
--
-- All addresses/hashes are stored lowercase 0x-prefixed hex, matching the
-- chain_blocks CHECK constraint format.

CREATE TABLE erc20_transfer_logs (
    chain_id     BIGINT      NOT NULL CHECK (chain_id > 0),
    block_number BIGINT      NOT NULL CHECK (block_number >= 0),
    block_hash   TEXT        NOT NULL CHECK (block_hash ~ '^0x[0-9a-f]{64}$'),
    tx_hash      TEXT        NOT NULL CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    log_index    BIGINT      NOT NULL CHECK (log_index >= 0),
    contract     TEXT        NOT NULL CHECK (contract ~ '^0x[0-9a-f]{40}$'),
    topic0       TEXT        NOT NULL CHECK (topic0 ~ '^0x[0-9a-f]{64}$'),
    topic1       TEXT        NOT NULL CHECK (topic1 ~ '^0x[0-9a-f]{64}$'),
    topic2       TEXT        NOT NULL CHECK (topic2 ~ '^0x[0-9a-f]{64}$'),
    data         TEXT        NOT NULL CHECK (data ~ '^0x[0-9a-f]{64}$'),
    indexed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_hash, tx_hash, log_index),
    UNIQUE (chain_id, block_hash, log_index)
);

-- Primary consumption path for 004 (range reads by height): covered index,
-- not speculative (see data-model.md).
CREATE INDEX erc20_transfer_logs_height_idx
    ON erc20_transfer_logs (chain_id, block_number);

CREATE TABLE log_checkpoint (
    chain_id     BIGINT      PRIMARY KEY CHECK (chain_id > 0),
    start_block  BIGINT      NOT NULL CHECK (start_block >= 0),
    config_hash  CHAR(64)    NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$'),
    next_block   BIGINT      NOT NULL CHECK (next_block >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (next_block >= start_block)
);

-- Persistent log-stream pause: row existence means "paused"; first pause wins.
CREATE TABLE log_pause (
    chain_id   BIGINT      PRIMARY KEY,
    height     BIGINT      NOT NULL,
    kind       TEXT        NOT NULL CHECK (kind IN ('chain_view_changed', 'validation_failed', 'range_incomplete')),
    detail     TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
-- Drop in dependency order (no FKs point out of these tables, but keep the
-- convention explicit).
DROP TABLE IF EXISTS log_pause;
DROP TABLE IF EXISTS log_checkpoint;
DROP TABLE IF EXISTS erc20_transfer_logs;
DROP INDEX IF EXISTS erc20_transfer_logs_height_idx;
