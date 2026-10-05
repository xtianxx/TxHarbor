-- +goose Up
-- T027: bind every instance to the complete deployment-controlled entry chain
-- inventory at creation. NULL deliberately identifies pre-migration instances;
-- it is never inferred or backfilled from decision rows.
ALTER TABLE recovery_instance
    ADD COLUMN entry_chain_inventory JSONB,
    ADD COLUMN entry_chain_inventory_version INTEGER;

ALTER TABLE recovery_instance
    ADD CONSTRAINT recovery_instance_entry_inventory_pair_check CHECK (
        (entry_chain_inventory IS NULL) = (entry_chain_inventory_version IS NULL)
    ),
    ADD CONSTRAINT recovery_instance_entry_inventory_version_check CHECK (
        entry_chain_inventory_version IS NULL OR entry_chain_inventory_version = 1
    ),
    ADD CONSTRAINT recovery_instance_entry_inventory_array_check CHECK (
        entry_chain_inventory IS NULL OR
        (jsonb_typeof(entry_chain_inventory) = 'array' AND jsonb_array_length(entry_chain_inventory) > 0)
    );

-- The inventory is a binding made exactly once when an instance is opened.
-- Legacy rows remain NULL, and ordinary lifecycle UPDATEs may change other
-- columns, but no caller (including a direct SQL caller) may replace, fill in,
-- or clear the binding after the row exists.
-- +goose StatementBegin
CREATE FUNCTION recovery_instance_entry_inventory_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.entry_chain_inventory IS DISTINCT FROM OLD.entry_chain_inventory
       OR NEW.entry_chain_inventory_version IS DISTINCT FROM OLD.entry_chain_inventory_version THEN
        RAISE EXCEPTION 'recovery instance entry-chain inventory is immutable after insert'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER recovery_instance_entry_inventory_immutable_trg
BEFORE UPDATE OF entry_chain_inventory, entry_chain_inventory_version ON recovery_instance
FOR EACH ROW EXECUTE FUNCTION recovery_instance_entry_inventory_immutable();

-- +goose Down
DROP TRIGGER recovery_instance_entry_inventory_immutable_trg ON recovery_instance;
DROP FUNCTION recovery_instance_entry_inventory_immutable();
ALTER TABLE recovery_instance
    DROP CONSTRAINT recovery_instance_entry_inventory_array_check,
    DROP CONSTRAINT recovery_instance_entry_inventory_version_check,
    DROP CONSTRAINT recovery_instance_entry_inventory_pair_check,
    DROP COLUMN entry_chain_inventory_version,
    DROP COLUMN entry_chain_inventory;
