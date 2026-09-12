-- +goose Up
-- TxHarbor 002-chain-indexer: indexer storage (specs/002-chain-indexer/data-model.md).
--
-- Invariants enforced here by storage:
--   I1  one row per (chain_id, number): PK.
--   I2  checkpoint always points at a stored block: composite FK onto
--       chain_blocks (chain_id, number, hash), which is backed by the
--       UNIQUE (chain_id, number, hash) constraint (FK target).
--   I4  monotonicity / never pointing past stored data: I2 plus the
--       application's exact guard; no cross-row FK is expressible for I3.
-- I3/I5/I6 are application-level by design (see data-model.md).
--
-- All hashes are stored lowercase 0x-prefixed 32-byte hex. The all-zero
-- genesis parent hash satisfies the same format check (0-9a-f includes 0).

CREATE TABLE chain_blocks (
    chain_id    BIGINT      NOT NULL CHECK (chain_id > 0),
    number      BIGINT      NOT NULL CHECK (number >= 0),
    hash        TEXT        NOT NULL CHECK (hash ~ '^0x[0-9a-f]{64}$'),
    parent_hash TEXT        NOT NULL CHECK (parent_hash ~ '^0x[0-9a-f]{64}$'),
    canonical   BOOLEAN     NOT NULL DEFAULT TRUE,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, number),
    UNIQUE (chain_id, number, hash),
    UNIQUE (chain_id, hash)
);

CREATE TABLE indexer_checkpoint (
    chain_id     BIGINT      PRIMARY KEY CHECK (chain_id > 0),
    height       BIGINT      NOT NULL CHECK (height >= 0),
    block_hash   TEXT        NOT NULL CHECK (block_hash ~ '^0x[0-9a-f]{64}$'),
    start_height BIGINT      NOT NULL CHECK (start_height >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (chain_id, height, block_hash)
        REFERENCES chain_blocks (chain_id, number, hash)
);

-- Coordination row: one per chain; FOR UPDATE on this row serializes all
-- indexer write transactions (never checkpoint/pause rows).
CREATE TABLE indexer_lease (
    chain_id      BIGINT      PRIMARY KEY,
    owner_id      TEXT        NOT NULL,
    fencing_token BIGINT      NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    expires_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Persistent pause: row existence means "paused"; first pause wins.
CREATE TABLE indexer_pause (
    chain_id      BIGINT      PRIMARY KEY,
    height        BIGINT      NOT NULL,
    expected_hash TEXT        NOT NULL,
    actual_hash   TEXT        NOT NULL,
    kind          TEXT        NOT NULL CHECK (kind IN ('hash_mismatch', 'parent_mismatch', 'checkpoint_changed')),
    detail        TEXT        NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
-- Reverse FK order: indexer_checkpoint references chain_blocks.
DROP TABLE IF EXISTS indexer_pause;
DROP TABLE IF EXISTS indexer_checkpoint;
DROP TABLE IF EXISTS indexer_lease;
DROP TABLE IF EXISTS chain_blocks;
