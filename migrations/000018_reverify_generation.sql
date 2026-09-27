-- +goose Up
-- TxHarbor 014 T026/T027 write-write ordering protocol: per-ticket
-- revalidation generation (data-model.md §3 "复核有效性令牌";
-- contracts/discrepancy-lifecycle.md "Revalidation Token Protocol").
--
-- PURE ADDITIVE DDL after 000017: no existing table, column, index or
-- constraint is altered, renamed or dropped; applied migrations 000001-000017
-- are never rewritten.
--
-- One non-negative counter per discrepancy row. It is the persisted validity
-- token of every in-flight evidence re-read: captured before the evidence is
-- gathered and re-validated under the discrepancy row lock at persistence
-- time. It is advanced (a) by every accepted reverify verdict write (ticket
-- entry and history sweep, inside the same transaction that inserts the
-- reverify row) and (b) by every ticket-row mutation through
-- updateDiscrepancySQL / invalidateTxAggregateSQL (claim, dispose, close,
-- invalidation, reopen, aggregate evidence replacement). A stale in-flight
-- re-read therefore can never overwrite a newer committed conclusion, delete
-- its gap, or make a close accept it. Default 0 keeps every pre-existing row
-- readable; the protocol applies from the first write after this migration.
ALTER TABLE discrepancy
    ADD COLUMN reverify_generation BIGINT NOT NULL DEFAULT 0;

-- +goose Down
-- Controlled-window rollback only: drops the additive token column after all
-- reconcile-admin invocations stopped. No 000001-000017 object is touched.
ALTER TABLE discrepancy DROP COLUMN IF EXISTS reverify_generation;
