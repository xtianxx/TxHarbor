-- +goose Up
-- TxHarbor PB (007-extension): authorization scope carrier
-- (specs/012-007-authorization-carrier/data-model.md; research R-PB3/R-PB4;
-- contracts/supply-scope.md).
--
-- Additive-only: 007's columns, rows and read shape are untouched; a grant
-- without a scope row stays valid and queryable (pre-extension stock, PB-FR-06).
-- This migration is PURE DDL — supply/revoke/scope-sync logic lives in the
-- app's parameterized SQL (internal/withdrawal/grant.go), never in stored
-- functions. No signature column (Q-A, 2026-09-16).
--
-- 1:1 with withdrawal_authorizations: authorization_id is both PK and FK, so
-- no orphan scope can exist and no grant can carry two scopes. There is no FK
-- into this table from the audit rows: audit stays append-only and
-- constraint-free (Table 6 precedent in 000007).
--
-- Fee scope (PB-C2, one BIGINT per dimension, native-coin最小单位): single-tx
-- max total network fee, per-gas-unit cap, EIP-1559 priority cap, with the
-- frozen cross-check fee_max_priority <= fee_max_per_gas. BIGINT CHECK >= 0 is
-- the amount precedent (R-PB3: fee values fit uint64).
--
-- Naming rule: every constraint carries an explicit `CONSTRAINT <name>` below —
-- never rely on PostgreSQL auto-naming.

CREATE TABLE withdrawal_authorization_scopes (
    authorization_id        TEXT    NOT NULL,
    intent_id               TEXT    NOT NULL,
    request_id              TEXT    NOT NULL,
    sender                  TEXT    NOT NULL,
    fee_max_total           BIGINT  NOT NULL,
    fee_max_per_gas         BIGINT  NOT NULL,
    fee_max_priority        BIGINT  NOT NULL,
    allows_fee_replacement  BOOLEAN NOT NULL DEFAULT FALSE,
    authorization_version   BIGINT  NOT NULL,
    attested_by             TEXT    NOT NULL,
    CONSTRAINT withdrawal_authorization_scopes_pkey PRIMARY KEY (authorization_id),
    CONSTRAINT withdrawal_authorization_scopes_authorization_fkey FOREIGN KEY (authorization_id)
        REFERENCES withdrawal_authorizations (authorization_id),
    CONSTRAINT withdrawal_authorization_scopes_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT withdrawal_authorization_scopes_fee_max_total_check CHECK (fee_max_total >= 0),
    CONSTRAINT withdrawal_authorization_scopes_fee_max_per_gas_check CHECK (fee_max_per_gas >= 0),
    CONSTRAINT withdrawal_authorization_scopes_fee_max_priority_check CHECK (fee_max_priority >= 0),
    CONSTRAINT withdrawal_authorization_scopes_fee_priority_within_max_check CHECK (fee_max_priority <= fee_max_per_gas),
    CONSTRAINT withdrawal_authorization_scopes_authorization_version_check CHECK (authorization_version >= 1)
);

-- +goose Down
-- Drop only the carrier this migration added; 007 grant rows and the rest of
-- the chain are untouched (authoring/scratch hygiene, same as 000007 Down).
DROP TABLE IF EXISTS withdrawal_authorization_scopes;
