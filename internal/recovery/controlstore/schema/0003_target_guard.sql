-- +goose Up
-- Durable target inventory. Absence is unknown; new targets are explicitly
-- inserted with unknown disposition and require audited baseline/rebuild.
CREATE TABLE recovery_target_guard (
    target_guard_key       TEXT        NOT NULL PRIMARY KEY,
    disposition            TEXT        NOT NULL DEFAULT 'unknown',
    active_writer          BOOLEAN     NOT NULL DEFAULT FALSE,
    launch_intent          BOOLEAN     NOT NULL DEFAULT FALSE,
    attempt_app_name       TEXT,
    operation_id           TEXT        NOT NULL,
    prepared_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    launch_intent_at       TIMESTAMPTZ,
    launched_at            TIMESTAMPTZ,
    rebuild_required_at    TIMESTAMPTZ,
    clean_at               TIMESTAMPTZ,
    rebuild_evidence       JSONB,
    CONSTRAINT recovery_target_guard_key_check CHECK (target_guard_key ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT recovery_target_guard_disposition_check CHECK (disposition IN ('unknown', 'clean', 'rebuild_required')),
    CONSTRAINT recovery_target_guard_app_name_check CHECK (attempt_app_name IS NULL OR length(attempt_app_name) BETWEEN 1 AND 128),
    CONSTRAINT recovery_target_guard_intent_pair_check CHECK (
        launch_intent = (attempt_app_name IS NOT NULL AND launch_intent_at IS NOT NULL)
    ),
    CONSTRAINT recovery_target_guard_state_check CHECK (
        (disposition = 'unknown' AND clean_at IS NULL AND rebuild_evidence IS NULL)
        OR (disposition = 'rebuild_required' AND rebuild_required_at IS NOT NULL AND clean_at IS NULL AND rebuild_evidence IS NULL)
        OR (disposition = 'clean' AND clean_at IS NOT NULL AND rebuild_evidence IS NOT NULL AND NOT active_writer)
    ),
    CONSTRAINT recovery_target_guard_writer_check CHECK (NOT active_writer OR (disposition = 'unknown' AND launch_intent))
);

ALTER TABLE recovery_instance
    ADD COLUMN target_guard_key TEXT,
    ADD COLUMN target_role_fingerprint TEXT;

CREATE UNIQUE INDEX recovery_target_guard_attempt_app_name_uniq
    ON recovery_target_guard (attempt_app_name) WHERE attempt_app_name IS NOT NULL;
CREATE UNIQUE INDEX recovery_target_guard_operation_uniq
    ON recovery_target_guard (operation_id);

-- +goose StatementBegin
CREATE FUNCTION recovery_instance_target_guard_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.target_guard_key IS DISTINCT FROM OLD.target_guard_key OR
       NEW.target_role_fingerprint IS DISTINCT FROM OLD.target_role_fingerprint THEN
        RAISE EXCEPTION 'recovery instance target guard binding is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER recovery_instance_target_guard_immutable_trg
    BEFORE UPDATE OF target_guard_key, target_role_fingerprint ON recovery_instance
    FOR EACH ROW EXECUTE FUNCTION recovery_instance_target_guard_immutable();

ALTER TABLE recovery_instance
    ADD CONSTRAINT recovery_instance_target_guard_fkey FOREIGN KEY (target_guard_key)
        REFERENCES recovery_target_guard (target_guard_key),
    ADD CONSTRAINT recovery_instance_target_guard_pair_check CHECK (
        (target_guard_key IS NULL) = (target_role_fingerprint IS NULL)
    ),
    ADD CONSTRAINT recovery_instance_target_guard_key_check CHECK (
        target_guard_key IS NULL OR target_guard_key ~ '^sha256:[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT recovery_instance_target_role_fingerprint_check CHECK (
        target_role_fingerprint IS NULL OR target_role_fingerprint ~ '^sha256:[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT recovery_instance_target_guard_binding_uniq UNIQUE (instance_id, target_guard_key);

-- +goose Down
DROP TRIGGER recovery_instance_target_guard_immutable_trg ON recovery_instance;
DROP FUNCTION recovery_instance_target_guard_immutable();
ALTER TABLE recovery_instance
    DROP CONSTRAINT recovery_instance_target_guard_binding_uniq,
    DROP CONSTRAINT recovery_instance_target_role_fingerprint_check,
    DROP CONSTRAINT recovery_instance_target_guard_key_check,
    DROP CONSTRAINT recovery_instance_target_guard_pair_check,
    DROP CONSTRAINT recovery_instance_target_guard_fkey,
    DROP COLUMN target_role_fingerprint,
    DROP COLUMN target_guard_key;
DROP TABLE recovery_target_guard;
