-- +goose Up
-- TxHarbor 010 transaction-lifecycle: intent-FK REPAIR (T044; G-010-3;
-- PLAN-1; specs/010-transaction-lifecycle/tasks.md:192).
--
-- Defect this migration closes (defect premise, reproduced by
-- internal/db/intent_fk_repair_integration_test.go):
--   guarded 000013 self-disables when public.payment_intents is absent, which
--   is exactly the state of a database that had goose record 000013 while
--   011's 000012 was not yet present (010 standalone lane / pre-merge order).
--   goose never re-runs an applied version, so 000013 stays a recorded no-op
--   even after 000012 later creates payment_intents: the tx_attempts ->
--   payment_intents FK is never built and 23503 protection is silently
--   missing. Fresh installs (000012 before 000013) are unaffected: 000013
--   adds the FK itself and this repair is a no-op there.
--
-- Contract (never silent, never a data fix):
--   * runs after 000012 by version order and asserts the referenced object
--     EXISTS (table and column); if 000012 did not land, it RAISEs a clear
--     exception and the goose run FAILS. It never records a no-op instead of
--     building the FK.
--   * adds the named constraint only when pg_constraint has no
--     tx_attempts_intent_fkey for tx_attempts, so a manual re-run is safe.
--   * never deletes or rewrites rows and never fabricates a payment_intents
--     row: orphan tx_attempts rows make the validated ADD CONSTRAINT fail
--     with 23503, and that failure is allowed to surface (the goose txn
--     rolls back, the orphan rows stay for the operator to resolve).
--
-- Additive only: no column, row, index or constraint of any 006/007/008/009/
-- 010/011 object is altered, renamed or dropped; applied migrations 000011/
-- 000012/000013 are never renumbered or rewritten; the constraint is
-- validated by PostgreSQL (NOT NOT VALID), so pre-existing tx_attempts rows
-- must already reference a real payment_intents row.
-- +goose StatementBegin
DO $$
DECLARE
    fk_exists boolean;
BEGIN
    IF to_regclass('public.payment_intents') IS NULL THEN
        RAISE EXCEPTION '000014 intent-FK repair requires 011 table payment_intents (migration 000012); apply 000012 before 000014';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'payment_intents'
           AND column_name = 'intent_id'
    ) THEN
        RAISE EXCEPTION '000014 intent-FK repair requires payment_intents(intent_id) (migration 000012); referenced column absent';
    END IF;

    SELECT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'tx_attempts_intent_fkey'
           AND conrelid = 'public.tx_attempts'::regclass
           AND contype = 'f'
    ) INTO fk_exists;

    IF NOT fk_exists THEN
        ALTER TABLE tx_attempts
            ADD CONSTRAINT tx_attempts_intent_fkey
            FOREIGN KEY (intent_id) REFERENCES payment_intents (intent_id);
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- Drop only the repaired constraint; no other object is touched.
ALTER TABLE tx_attempts DROP CONSTRAINT IF EXISTS tx_attempts_intent_fkey;
