-- +goose Up
-- TxHarbor 010 transaction-lifecycle: intent-FK follow-up (T044; G-010-3;
-- PLAN-1; specs/010-transaction-lifecycle/tasks.md:192).
--
-- 010's 000011 deliberately declared NO foreign key from tx_attempts.intent_id
-- to the 011-owned payment_intents(intent_id): that table did not exist at
-- apply time (G-010-3). 011 lands `000012_withdrawal_execution.sql` first in
-- the 010→011 merge order; this additive follow-up then closes the gap.
--
-- Lane guard (010-lane keep-lane resolution, 2026-09-17): the 010 delivery
-- branch is independently migratable without 011's tables (010 contract:
-- "010 does not need 011's tables to migrate, start or pass independent
-- acceptance"). On that lane `payment_intents` is absent, so this migration
-- records a no-op instead of failing the whole goose run; the intent FK is
-- applied exactly as ruled whenever 011's 000012 is present (the joint
-- integration workspace). The FK's presence on the merged mainline is the
-- recorded follow-through item in docs/workflow-010-011-parallel.md.
--
-- Additive only: no column, row, index or constraint of any 006/007/008/009/
-- 010/011 object is altered, renamed or dropped; applied migrations are never
-- renumbered or rewritten. When applied, the named constraint is validated by
-- PostgreSQL (NOT NOT VALID), so pre-existing tx_attempts rows must already
-- reference a real payment_intents row (the joint fixture seeds them in FK
-- order).
--
-- The check is a normal 23503 probe: inserting a tx_attempts row whose
-- intent_id is absent from payment_intents must fail with the exact
-- ConstraintName `tx_attempts_intent_fkey`.
-- +goose StatementBegin
DO $$
BEGIN
    IF to_regclass('public.payment_intents') IS NOT NULL THEN
        ALTER TABLE tx_attempts
            ADD CONSTRAINT tx_attempts_intent_fkey
            FOREIGN KEY (intent_id) REFERENCES payment_intents (intent_id);
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- Drop only the added constraint; no other object is touched.
ALTER TABLE tx_attempts DROP CONSTRAINT IF EXISTS tx_attempts_intent_fkey;
