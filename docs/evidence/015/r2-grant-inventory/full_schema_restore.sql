--
-- PostgreSQL database dump
--

\restrict DFn9rSv7xSuOduhuk1Qvu12JzxxHgfaYxxTSrg6r9cyrQcNN409vFgH7WlDIq1k

-- Dumped from database version 18.6 (Debian 18.6-1.pgdg13+2)
-- Dumped by pg_dump version 18.6 (Debian 18.6-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: api_key; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.api_key (
    key_id bigint NOT NULL,
    caller_id bigint NOT NULL,
    key_hash character(64) NOT NULL,
    key_prefix text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT api_key_key_hash_check CHECK ((key_hash ~ '^[0-9a-f]{64}$'::text))
);


ALTER TABLE public.api_key OWNER TO writer_owner;

--
-- Name: api_key_key_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.api_key ALTER COLUMN key_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.api_key_key_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: caller; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.caller (
    caller_id bigint NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    can_create boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT caller_caller_id_check CHECK ((caller_id > 0))
);


ALTER TABLE public.caller OWNER TO writer_owner;

--
-- Name: chain_blocks; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.chain_blocks (
    chain_id bigint NOT NULL,
    number bigint NOT NULL,
    hash text NOT NULL,
    parent_hash text NOT NULL,
    canonical boolean DEFAULT true NOT NULL,
    indexed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chain_blocks_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT chain_blocks_hash_check CHECK ((hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT chain_blocks_number_check CHECK ((number >= 0)),
    CONSTRAINT chain_blocks_parent_hash_check CHECK ((parent_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.chain_blocks OWNER TO writer_owner;

--
-- Name: confirmation_policy_history; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.confirmation_policy_history (
    chain_id bigint NOT NULL,
    policy_seq bigint NOT NULL,
    threshold bigint NOT NULL,
    prev_seq bigint,
    operator text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    request_id text,
    expected_old_seq bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT confirmation_policy_history_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT confirmation_policy_history_policy_seq_check CHECK ((policy_seq > 0)),
    CONSTRAINT confirmation_policy_history_threshold_check CHECK ((threshold > 0))
);


ALTER TABLE public.confirmation_policy_history OWNER TO writer_owner;

--
-- Name: consumer_inbox; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.consumer_inbox (
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version bigint NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    "offset" bigint NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.consumer_inbox OWNER TO writer_owner;

--
-- Name: consumer_progress; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.consumer_progress (
    consumer_name text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    next_offset bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT consumer_progress_next_offset_check CHECK ((next_offset >= 0))
);


ALTER TABLE public.consumer_progress OWNER TO writer_owner;

--
-- Name: consumer_quarantine; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.consumer_quarantine (
    id bigint NOT NULL,
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    event_snapshot jsonb NOT NULL,
    failure_class text NOT NULL,
    reason text NOT NULL,
    attempt_count integer NOT NULL,
    source_topic text,
    source_partition integer,
    source_offset bigint,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    replayed_at timestamp with time zone,
    replay_operation_id text,
    CONSTRAINT consumer_quarantine_failure_class_check CHECK ((failure_class = ANY (ARRAY['retry_exhausted'::text, 'non_retryable'::text, 'version_gap'::text, 'schema_unsupported'::text, 'identity_mismatch'::text]))),
    CONSTRAINT consumer_quarantine_status_check CHECK ((status = ANY (ARRAY['open'::text, 'replayed'::text, 'superseded'::text])))
);


ALTER TABLE public.consumer_quarantine OWNER TO writer_owner;

--
-- Name: consumer_quarantine_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.consumer_quarantine ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.consumer_quarantine_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: consumer_versions; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.consumer_versions (
    consumer_name text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    max_version bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT consumer_versions_max_version_check CHECK ((max_version > 0))
);


ALTER TABLE public.consumer_versions OWNER TO writer_owner;

--
-- Name: delivery_admissions; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.delivery_admissions (
    admission_id bigint NOT NULL,
    signing_request_row bigint NOT NULL,
    attempt_seq integer NOT NULL,
    verdict text NOT NULL,
    authorization_id text NOT NULL,
    authorization_fingerprint text NOT NULL,
    authorization_state text NOT NULL,
    binding_class text DEFAULT ''::text NOT NULL,
    can_sign boolean DEFAULT true NOT NULL,
    recovery_version bigint NOT NULL,
    pause_basis text DEFAULT 'none'::text NOT NULL,
    recovery_basis text DEFAULT 'none'::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    decided_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_at timestamp with time zone,
    CONSTRAINT delivery_admissions_attempt_seq_check CHECK ((attempt_seq > 0)),
    CONSTRAINT delivery_admissions_binding_class_check CHECK ((binding_class = ANY (ARRAY[''::text, 'matches'::text, 'absent'::text, 'conflict'::text, 'paused'::text, 'terminal'::text, 'read_failed'::text]))),
    CONSTRAINT delivery_admissions_verdict_check CHECK ((verdict = ANY (ARRAY['admitted'::text, 'delivered'::text, 'blocked'::text, 'unknown_reconcile'::text])))
);


ALTER TABLE public.delivery_admissions OWNER TO writer_owner;

--
-- Name: delivery_admissions_admission_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.delivery_admissions ALTER COLUMN admission_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.delivery_admissions_admission_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: deposit_checkpoint; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.deposit_checkpoint (
    chain_id bigint NOT NULL,
    start_block bigint NOT NULL,
    config_hash character(64) NOT NULL,
    next_block bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deposit_checkpoint_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT deposit_checkpoint_check CHECK ((next_block >= start_block)),
    CONSTRAINT deposit_checkpoint_config_hash_check CHECK ((config_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT deposit_checkpoint_next_block_check CHECK ((next_block >= 0)),
    CONSTRAINT deposit_checkpoint_start_block_check CHECK ((start_block >= 0))
);


ALTER TABLE public.deposit_checkpoint OWNER TO writer_owner;

--
-- Name: deposit_config_history; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.deposit_config_history (
    chain_id bigint NOT NULL,
    version_seq bigint NOT NULL,
    config_hash character(64) NOT NULL,
    prev_seq bigint,
    start_block bigint NOT NULL,
    assets text NOT NULL,
    watches text NOT NULL,
    replay_from bigint NOT NULL,
    operator text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    request_id text,
    expected_pause_id bigint,
    expected_pause_revision bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deposit_config_history_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT deposit_config_history_check CHECK (((expected_pause_id IS NULL) = (expected_pause_revision IS NULL))),
    CONSTRAINT deposit_config_history_config_hash_check CHECK ((config_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT deposit_config_history_replay_from_check CHECK ((replay_from >= 0)),
    CONSTRAINT deposit_config_history_start_block_check CHECK ((start_block >= 0)),
    CONSTRAINT deposit_config_history_version_seq_check CHECK ((version_seq > 0))
);


ALTER TABLE public.deposit_config_history OWNER TO writer_owner;

--
-- Name: deposit_observation_transitions; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.deposit_observation_transitions (
    chain_id bigint NOT NULL,
    block_hash text NOT NULL,
    tx_hash text NOT NULL,
    log_index bigint NOT NULL,
    from_status text NOT NULL,
    to_status text NOT NULL,
    recovery_id text NOT NULL,
    basis_snapshot text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deposit_observation_transitions_block_hash_check CHECK ((block_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT deposit_observation_transitions_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT deposit_observation_transitions_from_status_check CHECK ((from_status = ANY (ARRAY['pending'::text, 'confirmed'::text, 'orphaned'::text]))),
    CONSTRAINT deposit_observation_transitions_log_index_check CHECK ((log_index >= 0)),
    CONSTRAINT deposit_observation_transitions_to_status_check CHECK ((to_status = ANY (ARRAY['pending'::text, 'confirmed'::text, 'orphaned'::text]))),
    CONSTRAINT deposit_observation_transitions_tx_hash_check CHECK ((tx_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.deposit_observation_transitions OWNER TO writer_owner;

--
-- Name: deposit_observations; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.deposit_observations (
    chain_id bigint NOT NULL,
    block_hash text NOT NULL,
    tx_hash text NOT NULL,
    log_index bigint NOT NULL,
    block_number bigint NOT NULL,
    contract text NOT NULL,
    sender text NOT NULL,
    recipient text NOT NULL,
    amount numeric NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    version_seq bigint NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    confirmed_at timestamp with time zone,
    confirm_tip_number bigint,
    confirm_tip_hash text,
    confirm_threshold bigint,
    confirmations numeric,
    confirm_policy_seq bigint,
    orphaned_at timestamp with time zone,
    orphan_recovery_id text,
    orphan_reason text,
    CONSTRAINT deposit_observations_amount_check CHECK ((amount > (0)::numeric)),
    CONSTRAINT deposit_observations_block_hash_check CHECK ((block_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT deposit_observations_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT deposit_observations_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT deposit_observations_confirm_policy_seq_check CHECK ((confirm_policy_seq > 0)),
    CONSTRAINT deposit_observations_confirm_threshold_check CHECK ((confirm_threshold > 0)),
    CONSTRAINT deposit_observations_confirm_tip_hash_check CHECK ((confirm_tip_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT deposit_observations_confirm_tip_number_check CHECK ((confirm_tip_number >= 0)),
    CONSTRAINT deposit_observations_confirmation_consistency CHECK ((((status = 'pending'::text) = ((confirmed_at IS NULL) AND (orphaned_at IS NULL))) AND ((status = 'pending'::text) OR ((status = 'confirmed'::text) AND (confirmed_at IS NOT NULL) AND (confirm_tip_number IS NOT NULL) AND (confirm_tip_hash IS NOT NULL) AND (confirm_threshold IS NOT NULL) AND (confirmations IS NOT NULL) AND (confirm_policy_seq IS NOT NULL)) OR ((status = 'orphaned'::text) AND (orphaned_at IS NOT NULL) AND (orphan_recovery_id IS NOT NULL))))),
    CONSTRAINT deposit_observations_confirmations_check CHECK (((confirmations >= (0)::numeric) AND (confirmations = floor(confirmations)))),
    CONSTRAINT deposit_observations_contract_check CHECK ((contract ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT deposit_observations_log_index_check CHECK ((log_index >= 0)),
    CONSTRAINT deposit_observations_recipient_check CHECK ((recipient ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT deposit_observations_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT deposit_observations_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'confirmed'::text, 'orphaned'::text]))),
    CONSTRAINT deposit_observations_tx_hash_check CHECK ((tx_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.deposit_observations OWNER TO writer_owner;

--
-- Name: deposit_pause_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

CREATE SEQUENCE public.deposit_pause_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.deposit_pause_id_seq OWNER TO writer_owner;

--
-- Name: deposit_pause; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.deposit_pause (
    chain_id bigint NOT NULL,
    pause_id bigint DEFAULT nextval('public.deposit_pause_id_seq'::regclass) NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    height bigint NOT NULL,
    kind text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deposit_pause_kind_check CHECK ((kind = ANY (ARRAY['upstream_gap'::text, 'chain_view_changed'::text, 'validation_failed'::text])))
);


ALTER TABLE public.deposit_pause OWNER TO writer_owner;

--
-- Name: deposit_pause_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.deposit_pause_audit (
    chain_id bigint NOT NULL,
    pause_id bigint NOT NULL,
    revision bigint NOT NULL,
    action text NOT NULL,
    operator text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    version_seq bigint NOT NULL,
    kind text NOT NULL,
    height bigint NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deposit_pause_audit_action_check CHECK ((action = ANY (ARRAY['release'::text, 'merge'::text]))),
    CONSTRAINT deposit_pause_audit_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT deposit_pause_audit_revision_check CHECK ((revision > 0))
);


ALTER TABLE public.deposit_pause_audit OWNER TO writer_owner;

--
-- Name: discrepancy; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.discrepancy (
    discrepancy_id uuid NOT NULL,
    category text NOT NULL,
    business_key text NOT NULL,
    content_hash bytea NOT NULL,
    evidence_version_domain jsonb NOT NULL,
    state text DEFAULT 'open_claimable'::text NOT NULL,
    claim_owner text,
    claimed_at timestamp with time zone,
    close_basis jsonb,
    reopen_count integer DEFAULT 0 NOT NULL,
    linked_to uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    reverify_generation bigint DEFAULT 0 NOT NULL,
    CONSTRAINT discrepancy_business_key_shape CHECK (((length(business_key) >= 1) AND (length(business_key) <= 512))),
    CONSTRAINT discrepancy_category_check CHECK ((category = ANY (ARRAY['missing'::text, 'duplicate_divergent'::text, 'state_mismatch'::text, 'unknown'::text, 'incomplete'::text]))),
    CONSTRAINT discrepancy_claim_owner_shape CHECK (((claim_owner IS NULL) OR ((length(claim_owner) >= 1) AND (length(claim_owner) <= 128)))),
    CONSTRAINT discrepancy_claim_states_check CHECK (((state <> ALL (ARRAY['claimed'::text, 'disposing'::text])) OR ((claim_owner IS NOT NULL) AND (claimed_at IS NOT NULL)))),
    CONSTRAINT discrepancy_claimed_at_check CHECK (((claimed_at IS NULL) OR (claim_owner IS NOT NULL))),
    CONSTRAINT discrepancy_close_basis_shape CHECK (((close_basis IS NULL) OR (jsonb_typeof(close_basis) = 'object'::text))),
    CONSTRAINT discrepancy_closed_basis_check CHECK (((state <> 'closed'::text) OR (close_basis IS NOT NULL))),
    CONSTRAINT discrepancy_content_hash_shape CHECK ((octet_length(content_hash) > 0)),
    CONSTRAINT discrepancy_linked_not_self CHECK (((linked_to IS NULL) OR (linked_to <> discrepancy_id))),
    CONSTRAINT discrepancy_reopen_count_check CHECK ((reopen_count >= 0)),
    CONSTRAINT discrepancy_state_check CHECK ((state = ANY (ARRAY['open_claimable'::text, 'claimed'::text, 'disposing'::text, 'pending_verify'::text, 'closed'::text, 'reopened'::text]))),
    CONSTRAINT discrepancy_version_domain_shape CHECK ((jsonb_typeof(evidence_version_domain) = 'object'::text))
);


ALTER TABLE public.discrepancy OWNER TO writer_owner;

--
-- Name: discrepancy_occurrence; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.discrepancy_occurrence (
    occurrence_id bigint NOT NULL,
    discrepancy_id uuid NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    evidence_ref text NOT NULL,
    scan_task_id uuid NOT NULL,
    CONSTRAINT discrepancy_occurrence_evidence_shape CHECK (((length(evidence_ref) >= 1) AND (length(evidence_ref) <= 512)))
);


ALTER TABLE public.discrepancy_occurrence OWNER TO writer_owner;

--
-- Name: discrepancy_occurrence_occurrence_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.discrepancy_occurrence ALTER COLUMN occurrence_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.discrepancy_occurrence_occurrence_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: disposition; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.disposition (
    disposition_id uuid NOT NULL,
    discrepancy_id uuid NOT NULL,
    kind text NOT NULL,
    action_ref text DEFAULT ''::text NOT NULL,
    operator text NOT NULL,
    reason text NOT NULL,
    evidence_ref text DEFAULT ''::text NOT NULL,
    result text NOT NULL,
    idempotency_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT disposition_action_ref_shape CHECK (((kind = 'ack_only'::text) OR ((length(action_ref) >= 1) AND (length(action_ref) <= 512)))),
    CONSTRAINT disposition_evidence_ref_shape CHECK ((length(evidence_ref) <= 512)),
    CONSTRAINT disposition_idempotency_shape CHECK (((length(idempotency_key) >= 1) AND (length(idempotency_key) <= 256))),
    CONSTRAINT disposition_kind_check CHECK ((kind = ANY (ARRAY['ack_only'::text, 'reuse_recovery'::text, 'new_fix_rule'::text]))),
    CONSTRAINT disposition_new_fix_rule_dry_run_check CHECK (((kind <> 'new_fix_rule'::text) OR (result = 'dry_run'::text))),
    CONSTRAINT disposition_operator_shape CHECK (((length(operator) >= 1) AND (length(operator) <= 128))),
    CONSTRAINT disposition_reason_shape CHECK (((length(reason) >= 1) AND (length(reason) <= 1024))),
    CONSTRAINT disposition_result_check CHECK ((result = ANY (ARRAY['done'::text, 'refused'::text, 'failed'::text, 'dry_run'::text])))
);


ALTER TABLE public.disposition OWNER TO writer_owner;

--
-- Name: erc20_transfer_logs; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.erc20_transfer_logs (
    chain_id bigint NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    tx_hash text NOT NULL,
    log_index bigint NOT NULL,
    contract text NOT NULL,
    topic0 text NOT NULL,
    topic1 text NOT NULL,
    topic2 text NOT NULL,
    data text NOT NULL,
    indexed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT erc20_transfer_logs_block_hash_check CHECK ((block_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT erc20_transfer_logs_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT erc20_transfer_logs_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT erc20_transfer_logs_contract_check CHECK ((contract ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT erc20_transfer_logs_data_check CHECK ((data ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT erc20_transfer_logs_log_index_check CHECK ((log_index >= 0)),
    CONSTRAINT erc20_transfer_logs_topic0_check CHECK ((topic0 ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT erc20_transfer_logs_topic1_check CHECK ((topic1 ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT erc20_transfer_logs_topic2_check CHECK ((topic2 ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT erc20_transfer_logs_tx_hash_check CHECK ((tx_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.erc20_transfer_logs OWNER TO writer_owner;

--
-- Name: event_obligation; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.event_obligation (
    obligation_id bigint NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version bigint NOT NULL,
    expected_event_type text NOT NULL,
    obligated_at timestamp with time zone NOT NULL,
    source_kind text DEFAULT ''::text NOT NULL,
    source_id text DEFAULT ''::text NOT NULL,
    source_version bigint,
    CONSTRAINT event_obligation_aggregate_id_shape CHECK (((length(aggregate_id) >= 1) AND (length(aggregate_id) <= 512))),
    CONSTRAINT event_obligation_aggregate_version_check CHECK ((aggregate_version > 0)),
    CONSTRAINT event_obligation_mapping_check CHECK ((((aggregate_type = 'deposit_observation'::text) AND (expected_event_type = ANY (ARRAY['deposit.observation.created'::text, 'deposit.observation.status_changed'::text, 'deposit.observation.reinstated'::text, 'deposit.confirmation.confirmed'::text, 'deposit.revision.applied'::text]))) OR ((aggregate_type = 'withdrawal_request'::text) AND (expected_event_type = 'withdrawal.request.received'::text)) OR ((aggregate_type = 'withdrawal_intent'::text) AND (expected_event_type = ANY (ARRAY['withdrawal.execution.state_changed'::text, 'withdrawal.execution.revised'::text]))))),
    CONSTRAINT event_obligation_source_id_shape CHECK ((length(source_id) <= 512)),
    CONSTRAINT event_obligation_source_kind_shape CHECK ((length(source_kind) <= 128)),
    CONSTRAINT event_obligation_source_version_check CHECK (((source_version IS NULL) OR (source_version > 0)))
);


ALTER TABLE public.event_obligation OWNER TO writer_owner;

--
-- Name: event_obligation_obligation_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.event_obligation ALTER COLUMN obligation_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.event_obligation_obligation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: event_ops_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.event_ops_audit (
    id bigint NOT NULL,
    operation_id text NOT NULL,
    op_kind text NOT NULL,
    operator text NOT NULL,
    scope jsonb NOT NULL,
    reason text NOT NULL,
    result text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT event_ops_audit_op_kind_check CHECK ((op_kind = ANY (ARRAY['replay'::text, 'unblock'::text, 'retention_prune'::text])))
);


ALTER TABLE public.event_ops_audit OWNER TO writer_owner;

--
-- Name: event_ops_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.event_ops_audit ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.event_ops_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: event_system_state; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.event_system_state (
    id smallint NOT NULL,
    cutover_at timestamp with time zone NOT NULL,
    catalog_version integer NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT event_system_state_id_check CHECK ((id = 1))
);


ALTER TABLE public.event_system_state OWNER TO writer_owner;

--
-- Name: execution_caller_permission; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.execution_caller_permission (
    caller_id bigint NOT NULL,
    can_execute boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by text DEFAULT ''::text NOT NULL
);


ALTER TABLE public.execution_caller_permission OWNER TO writer_owner;

--
-- Name: execution_claims; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.execution_claims (
    intent_id text NOT NULL,
    owner_id text NOT NULL,
    lease_version bigint NOT NULL,
    state text NOT NULL,
    acquired_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    last_progress_at timestamp with time zone DEFAULT now() NOT NULL,
    stall_flagged_at timestamp with time zone,
    ended_at timestamp with time zone,
    end_kind text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_claims_end_kind_check CHECK (((end_kind IS NULL) OR (end_kind = ANY (ARRAY['released'::text, 'revoked'::text])))),
    CONSTRAINT execution_claims_expiry_check CHECK ((expires_at > acquired_at)),
    CONSTRAINT execution_claims_lease_version_check CHECK ((lease_version >= 1)),
    CONSTRAINT execution_claims_owner_check CHECK (((length(owner_id) >= 1) AND (length(owner_id) <= 128))),
    CONSTRAINT execution_claims_state_check CHECK ((state = ANY (ARRAY['active'::text, 'released'::text, 'revoked'::text]))),
    CONSTRAINT execution_claims_state_consistency CHECK (((state = 'active'::text) = ((ended_at IS NULL) AND (end_kind IS NULL))))
);


ALTER TABLE public.execution_claims OWNER TO writer_owner;

--
-- Name: execution_events; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.execution_events (
    event_id bigint NOT NULL,
    intent_id text NOT NULL,
    kind text NOT NULL,
    from_state text,
    to_state text,
    lease_version bigint,
    step_id text,
    attempt_id text,
    revision_version bigint,
    detail text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_events_kind_check CHECK ((kind = ANY (ARRAY['admitted'::text, 'admission_refused'::text, 'claimed'::text, 'released'::text, 'taken_over'::text, 'revoked'::text, 'stall_flagged'::text, 'state_changed'::text, 'step_issued'::text, 'step_converged'::text, 'step_refused'::text, 'step_unknown'::text, 'reconcile_observed'::text, 'revision_applied'::text, 'projection_stale'::text, 'projection_refreshed'::text])))
);


ALTER TABLE public.execution_events OWNER TO writer_owner;

--
-- Name: execution_events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.execution_events ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.execution_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: execution_ops_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.execution_ops_audit (
    audit_id bigint NOT NULL,
    operation_id text NOT NULL,
    action text NOT NULL,
    intent_id text DEFAULT ''::text NOT NULL,
    caller_id bigint,
    subject_version bigint,
    outcome text NOT NULL,
    operator text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    evidence text DEFAULT ''::text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_ops_audit_action_check CHECK ((action = ANY (ARRAY['permission_set'::text, 'permission_revoke'::text, 'claim_revoke'::text, 'projection_refresh'::text]))),
    CONSTRAINT execution_ops_audit_outcome_check CHECK ((outcome = ANY (ARRAY['applied'::text, 'nop'::text, 'refused'::text])))
);


ALTER TABLE public.execution_ops_audit OWNER TO writer_owner;

--
-- Name: execution_ops_audit_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.execution_ops_audit ALTER COLUMN audit_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.execution_ops_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: execution_steps; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.execution_steps (
    step_id text NOT NULL,
    intent_id text NOT NULL,
    action text NOT NULL,
    state text NOT NULL,
    owner_id text NOT NULL,
    lease_version bigint NOT NULL,
    attempt_id text,
    anchor_attempt_id text,
    tx_hash text,
    outcome_class text,
    recovery_version bigint,
    revision_version bigint,
    evidence text DEFAULT ''::text NOT NULL,
    issued_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_steps_action_check CHECK ((action = ANY (ARRAY['first_broadcast'::text, 'replay'::text, 'replace'::text]))),
    CONSTRAINT execution_steps_attempt_shape CHECK (((attempt_id IS NULL) OR (((length(attempt_id) >= 1) AND (length(attempt_id) <= 128)) AND (attempt_id ~ '^[\x21-\x7e]+$'::text)))),
    CONSTRAINT execution_steps_lease_version_check CHECK ((lease_version >= 1)),
    CONSTRAINT execution_steps_outcome_class_check CHECK (((outcome_class IS NULL) OR (outcome_class = ANY (ARRAY['sent'::text, 'refused_gate'::text, 'refused_basis'::text, 'pending_unknown'::text, 'reconcile_required'::text, 'unavailable'::text])))),
    CONSTRAINT execution_steps_owner_check CHECK (((length(owner_id) >= 1) AND (length(owner_id) <= 128))),
    CONSTRAINT execution_steps_state_check CHECK ((state = ANY (ARRAY['issued'::text, 'converged'::text, 'refused'::text, 'unknown'::text]))),
    CONSTRAINT execution_steps_tx_hash_check CHECK (((tx_hash IS NULL) OR (tx_hash ~ '^0x[0-9a-f]{64}$'::text)))
);


ALTER TABLE public.execution_steps OWNER TO writer_owner;

--
-- Name: indexer_checkpoint; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.indexer_checkpoint (
    chain_id bigint NOT NULL,
    height bigint NOT NULL,
    block_hash text NOT NULL,
    start_height bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT indexer_checkpoint_block_hash_check CHECK ((block_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT indexer_checkpoint_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT indexer_checkpoint_height_check CHECK ((height >= 0)),
    CONSTRAINT indexer_checkpoint_start_height_check CHECK ((start_height >= 0))
);


ALTER TABLE public.indexer_checkpoint OWNER TO writer_owner;

--
-- Name: indexer_lease; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.indexer_lease (
    chain_id bigint NOT NULL,
    owner_id text NOT NULL,
    fencing_token bigint DEFAULT 0 NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT indexer_lease_fencing_token_check CHECK ((fencing_token >= 0))
);


ALTER TABLE public.indexer_lease OWNER TO writer_owner;

--
-- Name: indexer_pause; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.indexer_pause (
    chain_id bigint NOT NULL,
    height bigint NOT NULL,
    expected_hash text NOT NULL,
    actual_hash text NOT NULL,
    kind text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT indexer_pause_kind_check CHECK ((kind = ANY (ARRAY['hash_mismatch'::text, 'parent_mismatch'::text, 'checkpoint_changed'::text])))
);


ALTER TABLE public.indexer_pause OWNER TO writer_owner;

--
-- Name: log_checkpoint; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.log_checkpoint (
    chain_id bigint NOT NULL,
    start_block bigint NOT NULL,
    config_hash character(64) NOT NULL,
    next_block bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT log_checkpoint_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT log_checkpoint_check CHECK ((next_block >= start_block)),
    CONSTRAINT log_checkpoint_config_hash_check CHECK ((config_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT log_checkpoint_next_block_check CHECK ((next_block >= 0)),
    CONSTRAINT log_checkpoint_start_block_check CHECK ((start_block >= 0))
);


ALTER TABLE public.log_checkpoint OWNER TO writer_owner;

--
-- Name: log_pause; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.log_pause (
    chain_id bigint NOT NULL,
    height bigint NOT NULL,
    kind text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT log_pause_kind_check CHECK ((kind = ANY (ARRAY['chain_view_changed'::text, 'validation_failed'::text, 'range_incomplete'::text])))
);


ALTER TABLE public.log_pause OWNER TO writer_owner;

--
-- Name: nonce_binding_events; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_binding_events (
    event_id bigint NOT NULL,
    binding_id text NOT NULL,
    from_state text,
    to_state text NOT NULL,
    observation_id text,
    operation_id text,
    detail text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nonce_binding_events_from_state_check CHECK (((from_state IS NULL) OR (from_state = ANY (ARRAY['allocated'::text, 'in_flight'::text, 'consumed'::text, 'released'::text])))),
    CONSTRAINT nonce_binding_events_to_state_check CHECK ((to_state = ANY (ARRAY['allocated'::text, 'in_flight'::text, 'consumed'::text, 'released'::text])))
);


ALTER TABLE public.nonce_binding_events OWNER TO writer_owner;

--
-- Name: nonce_binding_events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.nonce_binding_events ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.nonce_binding_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: nonce_bindings; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_bindings (
    binding_id text NOT NULL,
    intent_id text NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    nonce numeric(78,0) NOT NULL,
    state text NOT NULL,
    authorization_id text NOT NULL,
    authorization_version character(64) NOT NULL,
    registry_seq bigint NOT NULL,
    allocation_observation_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    consumed_at timestamp with time zone,
    released_at timestamp with time zone,
    release_operation_id text,
    CONSTRAINT nonce_bindings_authorization_version_check CHECK ((authorization_version ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT nonce_bindings_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT nonce_bindings_intent_shape CHECK ((((length(intent_id) >= 1) AND (length(intent_id) <= 128)) AND (intent_id ~ '^[\x21-\x7e]+$'::text))),
    CONSTRAINT nonce_bindings_nonce_range CHECK (((nonce >= (0)::numeric) AND (nonce <= '18446744073709551615'::numeric))),
    CONSTRAINT nonce_bindings_registry_seq_check CHECK ((registry_seq > 0)),
    CONSTRAINT nonce_bindings_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT nonce_bindings_state_check CHECK ((state = ANY (ARRAY['allocated'::text, 'in_flight'::text, 'consumed'::text, 'released'::text]))),
    CONSTRAINT nonce_bindings_terminal_consistency CHECK ((((state = 'consumed'::text) = (consumed_at IS NOT NULL)) AND ((state = 'released'::text) = ((released_at IS NOT NULL) AND (release_operation_id IS NOT NULL)))))
);


ALTER TABLE public.nonce_bindings OWNER TO writer_owner;

--
-- Name: nonce_observations; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_observations (
    observation_id text NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    kind text NOT NULL,
    classification text NOT NULL,
    latest_count numeric(78,0),
    pending_count numeric(78,0),
    head_number numeric(78,0),
    head_hash text,
    error_class text DEFAULT ''::text NOT NULL,
    rpc_ref text DEFAULT ''::text NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nonce_observations_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT nonce_observations_classification_check CHECK ((classification = ANY (ARRAY['consistent'::text, 'bootstrap_external_consumed'::text, 'unattributed_consumption'::text, 'unexplained_gap'::text, 'divergence'::text, 'unavailable'::text]))),
    CONSTRAINT nonce_observations_head_hash_check CHECK ((head_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT nonce_observations_kind_check CHECK ((kind = ANY (ARRAY['allocation'::text, 'reconcile'::text]))),
    CONSTRAINT nonce_observations_latest_count_check CHECK (((latest_count >= (0)::numeric) AND (latest_count <= '18446744073709551615'::numeric))),
    CONSTRAINT nonce_observations_pending_count_check CHECK (((pending_count >= (0)::numeric) AND (pending_count <= '18446744073709551615'::numeric))),
    CONSTRAINT nonce_observations_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text))
);


ALTER TABLE public.nonce_observations OWNER TO writer_owner;

--
-- Name: nonce_ops_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_ops_audit (
    audit_id bigint NOT NULL,
    operation_id text NOT NULL,
    action text NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    subject_id text DEFAULT ''::text NOT NULL,
    outcome text NOT NULL,
    operator text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    evidence text DEFAULT ''::text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nonce_ops_audit_action_check CHECK ((action = ANY (ARRAY['registry_register'::text, 'registry_disable'::text, 'hold_release'::text, 'binding_release'::text]))),
    CONSTRAINT nonce_ops_audit_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT nonce_ops_audit_outcome_check CHECK ((outcome = ANY (ARRAY['applied'::text, 'nop'::text, 'refused'::text]))),
    CONSTRAINT nonce_ops_audit_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text))
);


ALTER TABLE public.nonce_ops_audit OWNER TO writer_owner;

--
-- Name: nonce_ops_audit_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.nonce_ops_audit ALTER COLUMN audit_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.nonce_ops_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: nonce_scope_holds; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_scope_holds (
    hold_id text NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    cause text NOT NULL,
    status text NOT NULL,
    established_at timestamp with time zone DEFAULT now() NOT NULL,
    evidence_observation_id text NOT NULL,
    evidence_detail text DEFAULT ''::text NOT NULL,
    released_at timestamp with time zone,
    released_by text,
    release_operation_id text,
    release_evidence text,
    release_observation_id text,
    CONSTRAINT nonce_scope_holds_cause_check CHECK ((cause = ANY (ARRAY['unattributed_consumption'::text, 'unexplained_gap'::text, 'chain_view_divergence'::text]))),
    CONSTRAINT nonce_scope_holds_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT nonce_scope_holds_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT nonce_scope_holds_status_check CHECK ((status = ANY (ARRAY['active'::text, 'released'::text]))),
    CONSTRAINT nonce_scope_holds_status_consistency CHECK (((status = 'active'::text) = (released_at IS NULL)))
);


ALTER TABLE public.nonce_scope_holds OWNER TO writer_owner;

--
-- Name: nonce_scope_state; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_scope_state (
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    reconciled_floor numeric(78,0),
    last_latest numeric(78,0),
    last_pending numeric(78,0),
    last_observation_id text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nonce_scope_state_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT nonce_scope_state_last_latest_check CHECK (((last_latest >= (0)::numeric) AND (last_latest <= '18446744073709551615'::numeric))),
    CONSTRAINT nonce_scope_state_last_pending_check CHECK (((last_pending >= (0)::numeric) AND (last_pending <= '18446744073709551615'::numeric))),
    CONSTRAINT nonce_scope_state_reconciled_floor_check CHECK (((reconciled_floor >= (0)::numeric) AND (reconciled_floor <= '18446744073709551615'::numeric))),
    CONSTRAINT nonce_scope_state_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text))
);


ALTER TABLE public.nonce_scope_state OWNER TO writer_owner;

--
-- Name: nonce_wallet_registry; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.nonce_wallet_registry (
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    state text NOT NULL,
    registry_seq bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT nonce_wallet_registry_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT nonce_wallet_registry_registry_seq_check CHECK ((registry_seq > 0)),
    CONSTRAINT nonce_wallet_registry_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT nonce_wallet_registry_state_check CHECK ((state = ANY (ARRAY['active'::text, 'disabled'::text])))
);


ALTER TABLE public.nonce_wallet_registry OWNER TO writer_owner;

--
-- Name: outbox_events; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.outbox_events (
    id bigint NOT NULL,
    event_id uuid NOT NULL,
    identity_kind text NOT NULL,
    event_type text NOT NULL,
    schema_version integer NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version bigint NOT NULL,
    payload jsonb NOT NULL,
    payload_hash text NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    chain_id bigint,
    block_number bigint,
    block_hash text,
    tx_hash text,
    log_index integer,
    recovery_version bigint,
    revises_event_id uuid,
    source_kind text,
    source_id text,
    source_version bigint,
    publish_state text DEFAULT 'pending'::text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    claim_owner text,
    claim_expires_at timestamp with time zone,
    last_error_class text,
    published_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_events_aggregate_version_check CHECK ((aggregate_version > 0)),
    CONSTRAINT outbox_events_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT outbox_events_identity_kind_check CHECK ((identity_kind = ANY (ARRAY['evm_log'::text, 'business_object'::text]))),
    CONSTRAINT outbox_events_log_identity_shape CHECK (((identity_kind <> 'evm_log'::text) OR ((chain_id IS NOT NULL) AND (block_number IS NOT NULL) AND (block_hash IS NOT NULL) AND (tx_hash IS NOT NULL) AND (log_index IS NOT NULL)))),
    CONSTRAINT outbox_events_publish_state_check CHECK ((publish_state = ANY (ARRAY['pending'::text, 'published'::text, 'blocked'::text]))),
    CONSTRAINT outbox_events_revision_shape CHECK (((revises_event_id IS NULL) OR (recovery_version IS NOT NULL))),
    CONSTRAINT outbox_events_schema_version_check CHECK ((schema_version > 0)),
    CONSTRAINT outbox_events_state_consistency CHECK (((((publish_state = 'published'::text) AND (published_at IS NOT NULL)) OR ((publish_state = ANY (ARRAY['pending'::text, 'blocked'::text])) AND (published_at IS NULL))) AND ((publish_state <> 'blocked'::text) OR (last_error_class IS NOT NULL))))
);


ALTER TABLE public.outbox_events OWNER TO writer_owner;

--
-- Name: outbox_events_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.outbox_events ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.outbox_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: payment_intents; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.payment_intents (
    intent_id text NOT NULL,
    request_id text NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    authorization_id text NOT NULL,
    authorization_version bigint NOT NULL,
    state text NOT NULL,
    state_version bigint DEFAULT 1 NOT NULL,
    admitted_recovery_version bigint NOT NULL,
    admitted_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT payment_intents_authorization_version_check CHECK ((authorization_version >= 1)),
    CONSTRAINT payment_intents_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT payment_intents_intent_shape CHECK ((((length(intent_id) >= 1) AND (length(intent_id) <= 128)) AND (intent_id ~ '^[\x21-\x7e]+$'::text))),
    CONSTRAINT payment_intents_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT payment_intents_state_check CHECK ((state = ANY (ARRAY['admitted'::text, 'claimed'::text, 'executing'::text, 'completed'::text, 'failed'::text, 'reconciling'::text, 'revised'::text]))),
    CONSTRAINT payment_intents_state_version_check CHECK ((state_version >= 1))
);


ALTER TABLE public.payment_intents OWNER TO writer_owner;

--
-- Name: recon_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.recon_audit (
    audit_id bigint NOT NULL,
    actor text NOT NULL,
    action text NOT NULL,
    target jsonb DEFAULT '{}'::jsonb NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    evidence text DEFAULT ''::text NOT NULL,
    result text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recon_audit_action_check CHECK ((action = ANY (ARRAY['query'::text, 'start'::text, 'pause'::text, 'resume'::text, 'claim'::text, 'dispose'::text, 'reverify'::text, 'close'::text, 'reopen'::text, 'refuse'::text]))),
    CONSTRAINT recon_audit_actor_shape CHECK (((length(actor) >= 1) AND (length(actor) <= 128))),
    CONSTRAINT recon_audit_target_shape CHECK ((jsonb_typeof(target) = 'object'::text))
);


ALTER TABLE public.recon_audit OWNER TO writer_owner;

--
-- Name: recon_audit_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.recon_audit ALTER COLUMN audit_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.recon_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: recon_checkpoint; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.recon_checkpoint (
    task_id uuid NOT NULL,
    seq bigint NOT NULL,
    covered_through bigint,
    covered_through_at timestamp with time zone,
    result_persisted_through bigint,
    result_persisted_through_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recon_checkpoint_covered_shape CHECK (((covered_through IS NULL) <> (covered_through_at IS NULL))),
    CONSTRAINT recon_checkpoint_height_prefix CHECK (((result_persisted_through IS NULL) OR (covered_through IS NULL) OR (result_persisted_through <= covered_through))),
    CONSTRAINT recon_checkpoint_persisted_shape CHECK (((result_persisted_through IS NULL) <> (result_persisted_through_at IS NULL))),
    CONSTRAINT recon_checkpoint_seq_check CHECK ((seq > 0)),
    CONSTRAINT recon_checkpoint_time_prefix CHECK (((result_persisted_through_at IS NULL) OR (covered_through_at IS NULL) OR (result_persisted_through_at <= covered_through_at)))
);


ALTER TABLE public.recon_checkpoint OWNER TO writer_owner;

--
-- Name: recon_gap; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.recon_gap (
    gap_id bigint NOT NULL,
    task_id uuid NOT NULL,
    range_start bigint,
    range_end bigint,
    range_start_at timestamp with time zone,
    range_end_at timestamp with time zone,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recon_gap_range_shape CHECK ((((range_start IS NOT NULL) AND (range_end IS NOT NULL) AND (range_start_at IS NULL) AND (range_end_at IS NULL) AND (range_start >= 0) AND (range_start <= range_end)) OR ((range_start IS NULL) AND (range_end IS NULL) AND (range_start_at IS NOT NULL) AND (range_end_at IS NOT NULL) AND (range_start_at <= range_end_at)))),
    CONSTRAINT recon_gap_reason_check CHECK ((reason = ANY (ARRAY['not_started'::text, 'interrupted'::text, 'budget_exhausted'::text, 'paused'::text, 'freshness_hold'::text, 'upstream_unconnected'::text, 'query_failed'::text])))
);


ALTER TABLE public.recon_gap OWNER TO writer_owner;

--
-- Name: recon_gap_gap_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.recon_gap ALTER COLUMN gap_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.recon_gap_gap_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: recon_permission; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.recon_permission (
    principal text NOT NULL,
    action text NOT NULL,
    scope jsonb DEFAULT '{}'::jsonb NOT NULL,
    scope_hash text NOT NULL,
    granted_by text NOT NULL,
    granted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recon_permission_action_check CHECK ((action = ANY (ARRAY['scan_manage'::text, 'exception_handle'::text, 'dispose_ack'::text, 'dispose_reuse'::text, 'close'::text]))),
    CONSTRAINT recon_permission_granted_by_shape CHECK (((length(granted_by) >= 1) AND (length(granted_by) <= 128))),
    CONSTRAINT recon_permission_principal_shape CHECK (((length(principal) >= 1) AND (length(principal) <= 128))),
    CONSTRAINT recon_permission_scope_hash_shape CHECK (((length(scope_hash) >= 1) AND (length(scope_hash) <= 128))),
    CONSTRAINT recon_permission_scope_shape CHECK ((jsonb_typeof(scope) = 'object'::text))
);


ALTER TABLE public.recon_permission OWNER TO writer_owner;

--
-- Name: recon_scan_attempt; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.recon_scan_attempt (
    attempt_id uuid NOT NULL,
    task_id uuid NOT NULL,
    range_start bigint,
    range_end bigint,
    range_start_at timestamp with time zone,
    range_end_at timestamp with time zone,
    state text DEFAULT 'claimed'::text NOT NULL,
    owner text NOT NULL,
    lease_expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recon_scan_attempt_owner_shape CHECK (((length(owner) >= 1) AND (length(owner) <= 128))),
    CONSTRAINT recon_scan_attempt_range_shape CHECK ((((range_start IS NOT NULL) AND (range_end IS NOT NULL) AND (range_start_at IS NULL) AND (range_end_at IS NULL) AND (range_start >= 0) AND (range_start <= range_end)) OR ((range_start IS NULL) AND (range_end IS NULL) AND (range_start_at IS NOT NULL) AND (range_end_at IS NOT NULL) AND (range_start_at <= range_end_at)))),
    CONSTRAINT recon_scan_attempt_state_check CHECK ((state = ANY (ARRAY['claimed'::text, 'done'::text, 'abandoned'::text, 'superseded'::text])))
);


ALTER TABLE public.recon_scan_attempt OWNER TO writer_owner;

--
-- Name: recon_task; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.recon_task (
    task_id uuid NOT NULL,
    scope_chain_id text NOT NULL,
    scope_kind text NOT NULL,
    scope_start bigint,
    scope_end bigint,
    scope_start_at timestamp with time zone,
    scope_end_at timestamp with time zone,
    business_types text[] NOT NULL,
    upstream_receipt_source jsonb DEFAULT '{}'::jsonb NOT NULL,
    policy_refs jsonb DEFAULT '{}'::jsonb NOT NULL,
    state text DEFAULT 'created'::text NOT NULL,
    pause_reason text,
    budget jsonb DEFAULT '{}'::jsonb NOT NULL,
    history_sweep_through jsonb,
    created_by text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recon_task_budget_shape CHECK ((jsonb_typeof(budget) = 'object'::text)),
    CONSTRAINT recon_task_business_types_check CHECK (((cardinality(business_types) >= 1) AND (array_position(business_types, NULL::text) IS NULL) AND (business_types <@ ARRAY['withdrawal'::text, 'deposit'::text, 'event-delivery'::text]))),
    CONSTRAINT recon_task_chain_shape CHECK (((length(scope_chain_id) >= 1) AND (length(scope_chain_id) <= 128))),
    CONSTRAINT recon_task_creator_shape CHECK (((length(created_by) >= 1) AND (length(created_by) <= 128))),
    CONSTRAINT recon_task_pause_reason_shape CHECK (((pause_reason IS NULL) OR ((length(pause_reason) >= 1) AND (length(pause_reason) <= 1024)))),
    CONSTRAINT recon_task_policy_refs_shape CHECK ((jsonb_typeof(policy_refs) = 'object'::text)),
    CONSTRAINT recon_task_receipt_source_shape CHECK ((jsonb_typeof(upstream_receipt_source) = 'object'::text)),
    CONSTRAINT recon_task_scope_kind_check CHECK ((scope_kind = ANY (ARRAY['height'::text, 'time'::text]))),
    CONSTRAINT recon_task_scope_shape CHECK ((((scope_kind = 'height'::text) AND (scope_start IS NOT NULL) AND (scope_end IS NOT NULL) AND (scope_start_at IS NULL) AND (scope_end_at IS NULL) AND (scope_start >= 0) AND (scope_start <= scope_end)) OR ((scope_kind = 'time'::text) AND (scope_start IS NULL) AND (scope_end IS NULL) AND (scope_start_at IS NOT NULL) AND (scope_end_at IS NOT NULL) AND (scope_start_at <= scope_end_at)))),
    CONSTRAINT recon_task_state_check CHECK ((state = ANY (ARRAY['created'::text, 'running'::text, 'paused'::text, 'suspended_budget'::text, 'done'::text, 'cancelled'::text]))),
    CONSTRAINT recon_task_sweep_shape CHECK (((history_sweep_through IS NULL) OR (jsonb_typeof(history_sweep_through) = 'object'::text)))
);


ALTER TABLE public.recon_task OWNER TO writer_owner;

--
-- Name: reorg_policy_history; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.reorg_policy_history (
    chain_id bigint NOT NULL,
    policy_seq bigint NOT NULL,
    max_depth bigint NOT NULL,
    prev_seq bigint,
    operator text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    request_id text,
    expected_old_seq bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reorg_policy_history_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT reorg_policy_history_max_depth_check CHECK ((max_depth > 0)),
    CONSTRAINT reorg_policy_history_policy_seq_check CHECK ((policy_seq > 0))
);


ALTER TABLE public.reorg_policy_history OWNER TO writer_owner;

--
-- Name: reorg_recovery; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.reorg_recovery (
    chain_id bigint NOT NULL,
    recovery_id text NOT NULL,
    phase text NOT NULL,
    policy_seq bigint NOT NULL,
    max_depth bigint NOT NULL,
    bound_old_number bigint NOT NULL,
    bound_old_hash text NOT NULL,
    ancestor_number bigint,
    ancestor_hash text,
    new_tip_number bigint,
    new_tip_hash text,
    block_frontier bigint,
    log_frontier bigint,
    deposit_frontier bigint,
    detected_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    recovery_seq bigint NOT NULL,
    CONSTRAINT reorg_recovery_ancestor_hash_check CHECK (((ancestor_hash IS NULL) OR (ancestor_hash ~ '^0x[0-9a-f]{64}$'::text))),
    CONSTRAINT reorg_recovery_ancestor_number_check CHECK ((ancestor_number >= 0)),
    CONSTRAINT reorg_recovery_bound_old_hash_check CHECK ((bound_old_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT reorg_recovery_bound_old_number_check CHECK ((bound_old_number >= 0)),
    CONSTRAINT reorg_recovery_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT reorg_recovery_check CHECK (((ancestor_number IS NULL) = (ancestor_hash IS NULL))),
    CONSTRAINT reorg_recovery_check1 CHECK (((ancestor_number IS NULL) OR (((bound_old_number - ancestor_number) >= 0) AND ((bound_old_number - ancestor_number) <= max_depth)))),
    CONSTRAINT reorg_recovery_max_depth_check CHECK ((max_depth > 0)),
    CONSTRAINT reorg_recovery_phase_check CHECK ((phase = ANY (ARRAY['detected'::text, 'ancestor_confirmed'::text, 'invalidated'::text, 'replaying'::text, 'complete_pending'::text, 'reconcile_required'::text]))),
    CONSTRAINT reorg_recovery_policy_seq_check CHECK ((policy_seq > 0)),
    CONSTRAINT reorg_recovery_recovery_seq_check CHECK (((recovery_seq > 0) AND (recovery_seq < '9223372036854775807'::bigint)))
);


ALTER TABLE public.reorg_recovery OWNER TO writer_owner;

--
-- Name: reorg_recovery_events; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.reorg_recovery_events (
    chain_id bigint NOT NULL,
    recovery_id text NOT NULL,
    recovery_seq bigint NOT NULL,
    event_seq bigint NOT NULL,
    event text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reorg_recovery_events_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT reorg_recovery_events_event_check CHECK ((event = ANY (ARRAY['established'::text, 'ancestor_confirmed'::text, 'blocks_invalidated'::text, 'observations_invalidated'::text, 'checkpoints_rolled_back'::text, 'replay_progress'::text, 'observation_revived'::text, 'auto_completed'::text, 'repair_authorized'::text, 'released'::text, 'reconcile_signaled'::text]))),
    CONSTRAINT reorg_recovery_events_event_seq_check CHECK ((event_seq > 0)),
    CONSTRAINT reorg_recovery_events_recovery_seq_check CHECK ((recovery_seq > 0))
);


ALTER TABLE public.reorg_recovery_events OWNER TO writer_owner;

--
-- Name: request_status_projection; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.request_status_projection (
    request_id text NOT NULL,
    intent_id text NOT NULL,
    execution_state text NOT NULL,
    state_version bigint NOT NULL,
    lifecycle_attempt_id text,
    lifecycle_version bigint DEFAULT 0 NOT NULL,
    lifecycle_observed_at timestamp with time zone,
    freshness text NOT NULL,
    stale_since timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT request_status_projection_freshness_check CHECK ((freshness = ANY (ARRAY['confirmed'::text, 'possibly_stale'::text]))),
    CONSTRAINT request_status_projection_stale_consistency CHECK (((freshness = 'possibly_stale'::text) = (stale_since IS NOT NULL))),
    CONSTRAINT request_status_projection_state_check CHECK ((execution_state = ANY (ARRAY['admitted'::text, 'claimed'::text, 'executing'::text, 'completed'::text, 'failed'::text, 'reconciling'::text, 'revised'::text])))
);


ALTER TABLE public.request_status_projection OWNER TO writer_owner;

--
-- Name: reverify; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.reverify (
    reverify_id bigint NOT NULL,
    discrepancy_id uuid NOT NULL,
    verdict text NOT NULL,
    evidence_ref text DEFAULT ''::text NOT NULL,
    freshness_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reverify_consistent_evidence_check CHECK (((verdict <> 'consistent'::text) OR (((length(evidence_ref) >= 1) AND (length(evidence_ref) <= 512)) AND (freshness_at IS NOT NULL)))),
    CONSTRAINT reverify_verdict_check CHECK ((verdict = ANY (ARRAY['consistent'::text, 'divergent'::text, 'unknown'::text, 'stale'::text])))
);


ALTER TABLE public.reverify OWNER TO writer_owner;

--
-- Name: reverify_reverify_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.reverify ALTER COLUMN reverify_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.reverify_reverify_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: signature_results; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.signature_results (
    signing_request_row bigint NOT NULL,
    signature text NOT NULL,
    tx_hash text NOT NULL,
    signed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT signature_results_signature_check CHECK ((signature ~ '^0x[0-9a-f]{130}$'::text)),
    CONSTRAINT signature_results_tx_hash_check CHECK ((tx_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.signature_results OWNER TO writer_owner;

--
-- Name: signer_caller; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.signer_caller (
    caller_id bigint NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    can_sign boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT signer_caller_caller_id_check CHECK ((caller_id > 0))
);


ALTER TABLE public.signer_caller OWNER TO writer_owner;

--
-- Name: signer_credential; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.signer_credential (
    credential_id bigint NOT NULL,
    caller_id bigint NOT NULL,
    secret_hash character(64) NOT NULL,
    secret_prefix text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT signer_credential_secret_hash_check CHECK ((secret_hash ~ '^[0-9a-f]{64}$'::text))
);


ALTER TABLE public.signer_credential OWNER TO writer_owner;

--
-- Name: signer_credential_credential_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.signer_credential ALTER COLUMN credential_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.signer_credential_credential_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: signing_request_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.signing_request_audit (
    audit_id bigint NOT NULL,
    signing_request_id text NOT NULL,
    caller_id bigint NOT NULL,
    action text NOT NULL,
    reason_class text DEFAULT ''::text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT signing_request_audit_action_check CHECK ((action = ANY (ARRAY['received'::text, 'validated'::text, 'signed'::text, 'rejected'::text, 'failed'::text, 'conflict'::text, 'replayed'::text, 'gate_refused'::text, 'binding_refused'::text, 'authorization_refused'::text, 'delivery_admitted'::text, 'delivery_blocked'::text, 'delivery_unknown'::text])))
);


ALTER TABLE public.signing_request_audit OWNER TO writer_owner;

--
-- Name: signing_request_audit_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.signing_request_audit ALTER COLUMN audit_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.signing_request_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: signing_requests; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.signing_requests (
    id bigint NOT NULL,
    caller_id bigint NOT NULL,
    signing_request_id text NOT NULL,
    attempt_id text NOT NULL,
    replacement_of bigint,
    intent_id text NOT NULL,
    binding_ref text NOT NULL,
    recovery_version bigint DEFAULT 0 NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    nonce numeric(78,0) NOT NULL,
    tx_type integer NOT NULL,
    to_addr text NOT NULL,
    value numeric(78,0) NOT NULL,
    data bytea NOT NULL,
    gas_limit numeric(78,0) NOT NULL,
    gas_price numeric(78,0),
    max_fee_per_gas numeric(78,0),
    max_priority_fee_per_gas numeric(78,0),
    access_list jsonb DEFAULT '[]'::jsonb NOT NULL,
    asset text NOT NULL,
    recipient text NOT NULL,
    amount numeric(78,0) NOT NULL,
    canonical_envelope text NOT NULL,
    content_hash text NOT NULL,
    authorization_id text NOT NULL,
    authorization_fingerprint text NOT NULL,
    authorization_state text NOT NULL,
    authorization_version bigint,
    policy_version text NOT NULL,
    state text DEFAULT 'received'::text NOT NULL,
    refusal_class text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT signing_requests_access_list_check CHECK ((jsonb_array_length(access_list) = 0)),
    CONSTRAINT signing_requests_amount_check CHECK (((amount >= (1)::numeric) AND (amount <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::numeric))),
    CONSTRAINT signing_requests_asset_check CHECK ((asset ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT signing_requests_authorization_fingerprint_check CHECK ((authorization_fingerprint ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT signing_requests_authorization_version_check CHECK (((authorization_version IS NULL) OR (authorization_version >= 1))),
    CONSTRAINT signing_requests_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT signing_requests_content_hash_check CHECK ((content_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT signing_requests_fee_shape_check CHECK ((((tx_type = 0) AND (gas_price IS NOT NULL) AND (max_fee_per_gas IS NULL) AND (max_priority_fee_per_gas IS NULL)) OR ((tx_type = 2) AND (gas_price IS NULL) AND (max_fee_per_gas IS NOT NULL) AND (max_priority_fee_per_gas IS NOT NULL) AND (max_priority_fee_per_gas <= max_fee_per_gas)))),
    CONSTRAINT signing_requests_gas_limit_check CHECK ((gas_limit > (0)::numeric)),
    CONSTRAINT signing_requests_nonce_check CHECK (((nonce >= (0)::numeric) AND (nonce <= '18446744073709551615'::numeric))),
    CONSTRAINT signing_requests_recipient_check CHECK ((recipient ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT signing_requests_recovery_version_check CHECK ((recovery_version >= 0)),
    CONSTRAINT signing_requests_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT signing_requests_state_check CHECK ((state = ANY (ARRAY['received'::text, 'validated'::text, 'signed'::text, 'rejected'::text, 'failed'::text]))),
    CONSTRAINT signing_requests_to_check CHECK ((to_addr ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT signing_requests_value_check CHECK (((value >= (0)::numeric) AND (value <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::numeric)))
);


ALTER TABLE public.signing_requests OWNER TO writer_owner;

--
-- Name: signing_requests_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.signing_requests ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.signing_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tx_attempt_events; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_attempt_events (
    event_id bigint NOT NULL,
    attempt_id text NOT NULL,
    event_seq bigint NOT NULL,
    event text NOT NULL,
    reason_class text DEFAULT ''::text NOT NULL,
    recovery_version bigint,
    detail text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tx_attempt_events_event_check CHECK ((event = ANY (ARRAY['created'::text, 'replayed'::text, 'attempt_conflict'::text, 'signature_persisted'::text, 'signature_refused'::text, 'signature_mismatch'::text, 'gate_refused'::text, 'send_rejected'::text, 'send_unknown'::text, 'reconcile_observed'::text, 'receipt_verified'::text, 'receipt_ineffective'::text, 'confirmed'::text, 'orphaned'::text, 'reconfirmed'::text, 'replaced'::text, 'unknown_cleared'::text, 'post_final_check_expiry'::text, 'region_aborted_no_dispatch'::text, 'frozen'::text, 'released'::text]))),
    CONSTRAINT tx_attempt_events_seq_check CHECK ((event_seq > 0))
);


ALTER TABLE public.tx_attempt_events OWNER TO writer_owner;

--
-- Name: tx_attempt_events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.tx_attempt_events ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tx_attempt_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tx_attempt_signings; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_attempt_signings (
    attempt_id text NOT NULL,
    signature text NOT NULL,
    signed_tx_bytes bytea NOT NULL,
    tx_hash text NOT NULL,
    signed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tx_attempt_signings_bytes_check CHECK ((octet_length(signed_tx_bytes) > 0)),
    CONSTRAINT tx_attempt_signings_signature_check CHECK ((signature ~ '^0x[0-9a-f]{130}$'::text)),
    CONSTRAINT tx_attempt_signings_tx_hash_check CHECK ((tx_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.tx_attempt_signings OWNER TO writer_owner;

--
-- Name: tx_attempts; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_attempts (
    attempt_id text NOT NULL,
    signing_request_id text NOT NULL,
    replacement_of text,
    intent_id text NOT NULL,
    binding_ref text NOT NULL,
    authorization_id text NOT NULL,
    authorization_version bigint NOT NULL,
    recovery_version bigint NOT NULL,
    chain_id bigint NOT NULL,
    sender text NOT NULL,
    nonce numeric(78,0) NOT NULL,
    tx_type integer NOT NULL,
    to_addr text NOT NULL,
    value numeric(78,0) NOT NULL,
    data bytea NOT NULL,
    gas_limit numeric(78,0) NOT NULL,
    gas_price numeric(78,0),
    max_fee_per_gas numeric(78,0),
    max_priority_fee_per_gas numeric(78,0),
    asset text NOT NULL,
    recipient text NOT NULL,
    amount numeric(78,0) NOT NULL,
    canonical_envelope text NOT NULL,
    content_hash text NOT NULL,
    state text DEFAULT 'prepared'::text NOT NULL,
    revision_seq bigint DEFAULT 1 NOT NULL,
    effective_at timestamp with time zone,
    confirmed_at timestamp with time zone,
    orphaned_at timestamp with time zone,
    replaced_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tx_attempts_amount_check CHECK (((amount >= (1)::numeric) AND (amount <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::numeric))),
    CONSTRAINT tx_attempts_asset_check CHECK ((asset = to_addr)),
    CONSTRAINT tx_attempts_attempt_shape CHECK ((((length(attempt_id) >= 1) AND (length(attempt_id) <= 128)) AND (attempt_id ~ '^[\x21-\x7e]+$'::text))),
    CONSTRAINT tx_attempts_authorization_version_check CHECK ((authorization_version >= 1)),
    CONSTRAINT tx_attempts_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT tx_attempts_content_hash_check CHECK ((content_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT tx_attempts_fee_shape_check CHECK ((((tx_type = 0) AND (gas_price IS NOT NULL) AND (max_fee_per_gas IS NULL) AND (max_priority_fee_per_gas IS NULL)) OR ((tx_type = 2) AND (gas_price IS NULL) AND (max_fee_per_gas IS NOT NULL) AND (max_priority_fee_per_gas IS NOT NULL) AND (max_priority_fee_per_gas <= max_fee_per_gas)))),
    CONSTRAINT tx_attempts_gas_limit_check CHECK ((gas_limit > (0)::numeric)),
    CONSTRAINT tx_attempts_intent_shape CHECK ((((length(intent_id) >= 1) AND (length(intent_id) <= 128)) AND (intent_id ~ '^[\x21-\x7e]+$'::text))),
    CONSTRAINT tx_attempts_nonce_check CHECK (((nonce >= (0)::numeric) AND (nonce <= '18446744073709551615'::numeric))),
    CONSTRAINT tx_attempts_recipient_check CHECK ((recipient ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT tx_attempts_recovery_version_check CHECK ((recovery_version >= 0)),
    CONSTRAINT tx_attempts_replacement_not_self CHECK (((replacement_of IS NULL) OR (replacement_of <> attempt_id))),
    CONSTRAINT tx_attempts_revision_seq_check CHECK ((revision_seq > 0)),
    CONSTRAINT tx_attempts_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT tx_attempts_signing_request_shape CHECK ((((length(signing_request_id) >= 1) AND (length(signing_request_id) <= 128)) AND (signing_request_id ~ '^[\x21-\x7e]+$'::text))),
    CONSTRAINT tx_attempts_state_check CHECK ((state = ANY (ARRAY['prepared'::text, 'signed'::text, 'sent'::text, 'unknown'::text, 'effective'::text, 'ineffective'::text, 'confirmed'::text, 'orphaned'::text, 'replaced'::text]))),
    CONSTRAINT tx_attempts_state_facts_check CHECK ((((state <> 'confirmed'::text) OR (confirmed_at IS NOT NULL)) AND ((state <> 'orphaned'::text) OR (orphaned_at IS NOT NULL)) AND ((state <> 'replaced'::text) OR (replaced_at IS NOT NULL)) AND ((state <> ALL (ARRAY['effective'::text, 'confirmed'::text])) OR (effective_at IS NOT NULL)))),
    CONSTRAINT tx_attempts_to_check CHECK ((to_addr ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT tx_attempts_value_check CHECK (((value >= (0)::numeric) AND (value <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::numeric)))
);


ALTER TABLE public.tx_attempts OWNER TO writer_owner;

--
-- Name: tx_intent_freezes; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_intent_freezes (
    intent_id text NOT NULL,
    cause text NOT NULL,
    evidence text DEFAULT ''::text NOT NULL,
    frozen_at timestamp with time zone DEFAULT now() NOT NULL,
    released_at timestamp with time zone,
    released_by text,
    release_basis text,
    CONSTRAINT tx_intent_freezes_cause_check CHECK ((cause = 'protection_loss_residual'::text)),
    CONSTRAINT tx_intent_freezes_release_consistency CHECK (((released_at IS NULL) = (released_by IS NULL)))
);


ALTER TABLE public.tx_intent_freezes OWNER TO writer_owner;

--
-- Name: tx_receipts; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_receipts (
    receipt_id bigint NOT NULL,
    attempt_id text NOT NULL,
    tx_hash text NOT NULL,
    status integer NOT NULL,
    block_number bigint NOT NULL,
    block_hash text NOT NULL,
    effect text NOT NULL,
    transfer_detail text DEFAULT ''::text NOT NULL,
    canonicality text DEFAULT 'unverified'::text NOT NULL,
    confirmations numeric(78,0) DEFAULT 0 NOT NULL,
    confirm_threshold bigint NOT NULL,
    confirm_policy_seq bigint NOT NULL,
    confirm_tip_number bigint,
    confirm_tip_hash text,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    confirmed_at timestamp with time zone,
    orphaned_at timestamp with time zone,
    CONSTRAINT tx_receipts_block_hash_check CHECK ((block_hash ~ '^0x[0-9a-f]{64}$'::text)),
    CONSTRAINT tx_receipts_block_number_check CHECK ((block_number >= 0)),
    CONSTRAINT tx_receipts_canonicality_check CHECK ((canonicality = ANY (ARRAY['unverified'::text, 'canonical'::text, 'orphaned'::text]))),
    CONSTRAINT tx_receipts_confirmations_check CHECK (((confirmations >= (0)::numeric) AND (confirmations = floor(confirmations)))),
    CONSTRAINT tx_receipts_confirmed_at_check CHECK (((confirmed_at IS NULL) OR (canonicality <> 'unverified'::text))),
    CONSTRAINT tx_receipts_effect_check CHECK ((effect = ANY (ARRAY['effective'::text, 'ineffective_status'::text, 'ineffective_transfer_missing'::text, 'ineffective_transfer_mismatch'::text]))),
    CONSTRAINT tx_receipts_effect_status_check CHECK ((((effect = 'ineffective_status'::text) = (status = 0)) AND ((effect <> 'effective'::text) OR (status = 1)))),
    CONSTRAINT tx_receipts_orphan_facts_check CHECK ((((canonicality <> 'orphaned'::text) OR (orphaned_at IS NOT NULL)) AND ((canonicality <> 'canonical'::text) OR (orphaned_at IS NULL)))),
    CONSTRAINT tx_receipts_policy_seq_check CHECK ((confirm_policy_seq > 0)),
    CONSTRAINT tx_receipts_status_check CHECK ((status = ANY (ARRAY[0, 1]))),
    CONSTRAINT tx_receipts_threshold_check CHECK ((confirm_threshold > 0))
);


ALTER TABLE public.tx_receipts OWNER TO writer_owner;

--
-- Name: tx_receipts_receipt_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.tx_receipts ALTER COLUMN receipt_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tx_receipts_receipt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tx_reconciliations; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_reconciliations (
    reconcile_id bigint NOT NULL,
    attempt_id text NOT NULL,
    tx_hash text NOT NULL,
    classification text NOT NULL,
    block_number bigint,
    block_hash text,
    rpc_class text DEFAULT ''::text NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tx_reconciliations_block_hash_check CHECK (((block_hash IS NULL) OR (block_hash ~ '^0x[0-9a-f]{64}$'::text))),
    CONSTRAINT tx_reconciliations_block_pair_check CHECK (((block_number IS NULL) = (block_hash IS NULL))),
    CONSTRAINT tx_reconciliations_classification_check CHECK ((classification = ANY (ARRAY['found_pending'::text, 'included'::text, 'not_found_yet'::text, 'unavailable'::text]))),
    CONSTRAINT tx_reconciliations_included_block_check CHECK (((classification <> 'included'::text) OR (block_number IS NOT NULL))),
    CONSTRAINT tx_reconciliations_tx_hash_check CHECK ((tx_hash ~ '^0x[0-9a-f]{64}$'::text))
);


ALTER TABLE public.tx_reconciliations OWNER TO writer_owner;

--
-- Name: tx_reconciliations_reconcile_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.tx_reconciliations ALTER COLUMN reconcile_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tx_reconciliations_reconcile_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tx_send_attempts; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.tx_send_attempts (
    send_id bigint NOT NULL,
    attempt_id text NOT NULL,
    send_seq integer NOT NULL,
    kind text NOT NULL,
    outcome text NOT NULL,
    rpc_class text DEFAULT ''::text NOT NULL,
    observed_recovery_version bigint NOT NULL,
    observed_pause text DEFAULT 'none'::text NOT NULL,
    observed_claim_version bigint,
    observed_claim_expiry timestamp with time zone,
    observed_authorization_id text,
    observed_authorization_version bigint,
    observed_authorization_state text DEFAULT ''::text NOT NULL,
    observed_expires_at timestamp with time zone,
    observed_now timestamp with time zone NOT NULL,
    observed_binding_state text DEFAULT ''::text NOT NULL,
    dispatched_at timestamp with time zone,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tx_send_attempts_dispatched_check CHECK (((outcome = 'unknown'::text) OR (dispatched_at IS NOT NULL))),
    CONSTRAINT tx_send_attempts_kind_check CHECK ((kind = ANY (ARRAY['initial'::text, 'replay'::text]))),
    CONSTRAINT tx_send_attempts_outcome_check CHECK ((outcome = ANY (ARRAY['accepted'::text, 'rejected'::text, 'unknown'::text]))),
    CONSTRAINT tx_send_attempts_seq_check CHECK ((send_seq > 0))
);


ALTER TABLE public.tx_send_attempts OWNER TO writer_owner;

--
-- Name: tx_send_attempts_send_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.tx_send_attempts ALTER COLUMN send_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tx_send_attempts_send_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: withdrawal_authorization_scopes; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.withdrawal_authorization_scopes (
    authorization_id text NOT NULL,
    intent_id text NOT NULL,
    request_id text NOT NULL,
    sender text NOT NULL,
    fee_max_total bigint NOT NULL,
    fee_max_per_gas bigint NOT NULL,
    fee_max_priority bigint NOT NULL,
    allows_fee_replacement boolean DEFAULT false NOT NULL,
    authorization_version bigint NOT NULL,
    attested_by text NOT NULL,
    CONSTRAINT withdrawal_authorization_scopes_authorization_version_check CHECK ((authorization_version >= 1)),
    CONSTRAINT withdrawal_authorization_scopes_fee_max_per_gas_check CHECK ((fee_max_per_gas >= 0)),
    CONSTRAINT withdrawal_authorization_scopes_fee_max_priority_check CHECK ((fee_max_priority >= 0)),
    CONSTRAINT withdrawal_authorization_scopes_fee_max_total_check CHECK ((fee_max_total >= 0)),
    CONSTRAINT withdrawal_authorization_scopes_fee_priority_within_max_check CHECK ((fee_max_priority <= fee_max_per_gas)),
    CONSTRAINT withdrawal_authorization_scopes_sender_check CHECK ((sender ~ '^0x[0-9a-f]{40}$'::text))
);


ALTER TABLE public.withdrawal_authorization_scopes OWNER TO writer_owner;

--
-- Name: withdrawal_authorizations; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.withdrawal_authorizations (
    authorization_id text NOT NULL,
    caller_id bigint NOT NULL,
    chain_id bigint NOT NULL,
    asset text NOT NULL,
    recipient text NOT NULL,
    amount numeric(78,0) NOT NULL,
    state text NOT NULL,
    expires_at timestamp with time zone,
    supplied_at timestamp with time zone DEFAULT now() NOT NULL,
    supplied_by text DEFAULT ''::text NOT NULL,
    CONSTRAINT withdrawal_authorizations_amount_check CHECK (((amount >= (1)::numeric) AND (amount <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::numeric))),
    CONSTRAINT withdrawal_authorizations_asset_check CHECK ((asset ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT withdrawal_authorizations_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT withdrawal_authorizations_recipient_check CHECK ((recipient ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT withdrawal_authorizations_state_check CHECK ((state = ANY (ARRAY['active'::text, 'revoked'::text, 'expired'::text])))
);


ALTER TABLE public.withdrawal_authorizations OWNER TO writer_owner;

--
-- Name: withdrawal_grant_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.withdrawal_grant_audit (
    audit_id bigint NOT NULL,
    operation_id text NOT NULL,
    authorization_id text NOT NULL,
    caller_id bigint NOT NULL,
    action text NOT NULL,
    operator text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT withdrawal_grant_audit_action_check CHECK ((action = ANY (ARRAY['supplied'::text, 'resupplied'::text, 'supply_refused'::text, 'revoked'::text, 'revoke_nop'::text])))
);


ALTER TABLE public.withdrawal_grant_audit OWNER TO writer_owner;

--
-- Name: withdrawal_grant_audit_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.withdrawal_grant_audit ALTER COLUMN audit_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.withdrawal_grant_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: withdrawal_request_audit; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.withdrawal_request_audit (
    audit_id bigint NOT NULL,
    request_id text NOT NULL,
    caller_id bigint NOT NULL,
    action text NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT withdrawal_request_audit_action_check CHECK ((action = ANY (ARRAY['created'::text, 'replayed'::text, 'conflict'::text, 'rejected'::text, 'auth_failed'::text, 'unavailable'::text])))
);


ALTER TABLE public.withdrawal_request_audit OWNER TO writer_owner;

--
-- Name: withdrawal_request_audit_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.withdrawal_request_audit ALTER COLUMN audit_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.withdrawal_request_audit_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: withdrawal_requests; Type: TABLE; Schema: public; Owner: writer_owner
--

CREATE TABLE public.withdrawal_requests (
    id bigint NOT NULL,
    request_id text NOT NULL,
    caller_id bigint NOT NULL,
    idempotency_key text NOT NULL,
    authorization_id text NOT NULL,
    chain_id bigint NOT NULL,
    asset text NOT NULL,
    recipient text NOT NULL,
    amount numeric(78,0) NOT NULL,
    status text DEFAULT 'accepted'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT withdrawal_requests_amount_check CHECK (((amount >= (1)::numeric) AND (amount <= '115792089237316195423570985008687907853269984665640564039457584007913129639935'::numeric))),
    CONSTRAINT withdrawal_requests_asset_check CHECK ((asset ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT withdrawal_requests_chain_id_check CHECK ((chain_id > 0)),
    CONSTRAINT withdrawal_requests_idempotency_key_check CHECK ((((length(idempotency_key) >= 1) AND (length(idempotency_key) <= 128)) AND (idempotency_key ~ '^[\x21-\x7e]+$'::text))),
    CONSTRAINT withdrawal_requests_recipient_check CHECK ((recipient ~ '^0x[0-9a-f]{40}$'::text)),
    CONSTRAINT withdrawal_requests_status_check CHECK ((status = 'accepted'::text))
);


ALTER TABLE public.withdrawal_requests OWNER TO writer_owner;

--
-- Name: withdrawal_requests_id_seq; Type: SEQUENCE; Schema: public; Owner: writer_owner
--

ALTER TABLE public.withdrawal_requests ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.withdrawal_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Data for Name: api_key; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.api_key (key_id, caller_id, key_hash, key_prefix, created_at, revoked_at) FROM stdin;
\.


--
-- Data for Name: caller; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.caller (caller_id, label, can_create, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: chain_blocks; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.chain_blocks (chain_id, number, hash, parent_hash, canonical, indexed_at) FROM stdin;
\.


--
-- Data for Name: confirmation_policy_history; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.confirmation_policy_history (chain_id, policy_seq, threshold, prev_seq, operator, reason, request_id, expected_old_seq, created_at) FROM stdin;
31337	1	12	\N			\N	0	2026-10-04 03:54:19.872534+00
\.


--
-- Data for Name: consumer_inbox; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.consumer_inbox (consumer_name, event_id, aggregate_type, aggregate_id, aggregate_version, topic, partition, "offset", applied_at) FROM stdin;
\.


--
-- Data for Name: consumer_progress; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.consumer_progress (consumer_name, topic, partition, next_offset, updated_at) FROM stdin;
\.


--
-- Data for Name: consumer_quarantine; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.consumer_quarantine (id, consumer_name, event_id, event_snapshot, failure_class, reason, attempt_count, source_topic, source_partition, source_offset, first_seen_at, last_seen_at, status, replayed_at, replay_operation_id) FROM stdin;
\.


--
-- Data for Name: consumer_versions; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.consumer_versions (consumer_name, aggregate_type, aggregate_id, max_version, updated_at) FROM stdin;
\.


--
-- Data for Name: delivery_admissions; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.delivery_admissions (admission_id, signing_request_row, attempt_seq, verdict, authorization_id, authorization_fingerprint, authorization_state, binding_class, can_sign, recovery_version, pause_basis, recovery_basis, reason, decided_at, delivered_at) FROM stdin;
\.


--
-- Data for Name: deposit_checkpoint; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.deposit_checkpoint (chain_id, start_block, config_hash, next_block, updated_at) FROM stdin;
\.


--
-- Data for Name: deposit_config_history; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.deposit_config_history (chain_id, version_seq, config_hash, prev_seq, start_block, assets, watches, replay_from, operator, reason, request_id, expected_pause_id, expected_pause_revision, created_at) FROM stdin;
\.


--
-- Data for Name: deposit_observation_transitions; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.deposit_observation_transitions (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id, basis_snapshot, at) FROM stdin;
\.


--
-- Data for Name: deposit_observations; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.deposit_observations (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, status, version_seq, observed_at, confirmed_at, confirm_tip_number, confirm_tip_hash, confirm_threshold, confirmations, confirm_policy_seq, orphaned_at, orphan_recovery_id, orphan_reason) FROM stdin;
\.


--
-- Data for Name: deposit_pause; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.deposit_pause (chain_id, pause_id, revision, height, kind, detail, created_at) FROM stdin;
\.


--
-- Data for Name: deposit_pause_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.deposit_pause_audit (chain_id, pause_id, revision, action, operator, reason, version_seq, kind, height, detail, at) FROM stdin;
\.


--
-- Data for Name: discrepancy; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.discrepancy (discrepancy_id, category, business_key, content_hash, evidence_version_domain, state, claim_owner, claimed_at, close_basis, reopen_count, linked_to, created_at, updated_at, reverify_generation) FROM stdin;
\.


--
-- Data for Name: discrepancy_occurrence; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.discrepancy_occurrence (occurrence_id, discrepancy_id, observed_at, evidence_ref, scan_task_id) FROM stdin;
\.


--
-- Data for Name: disposition; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.disposition (disposition_id, discrepancy_id, kind, action_ref, operator, reason, evidence_ref, result, idempotency_key, created_at) FROM stdin;
\.


--
-- Data for Name: erc20_transfer_logs; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.erc20_transfer_logs (chain_id, block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data, indexed_at) FROM stdin;
\.


--
-- Data for Name: event_obligation; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.event_obligation (obligation_id, aggregate_type, aggregate_id, aggregate_version, expected_event_type, obligated_at, source_kind, source_id, source_version) FROM stdin;
\.


--
-- Data for Name: event_ops_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.event_ops_audit (id, operation_id, op_kind, operator, scope, reason, result, created_at) FROM stdin;
\.


--
-- Data for Name: event_system_state; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.event_system_state (id, cutover_at, catalog_version, updated_at) FROM stdin;
1	2026-10-04 03:51:40.684139+00	1	2026-10-04 03:51:40.684139+00
\.


--
-- Data for Name: execution_caller_permission; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.execution_caller_permission (caller_id, can_execute, updated_at, updated_by) FROM stdin;
\.


--
-- Data for Name: execution_claims; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.execution_claims (intent_id, owner_id, lease_version, state, acquired_at, expires_at, last_heartbeat_at, last_progress_at, stall_flagged_at, ended_at, end_kind, updated_at) FROM stdin;
\.


--
-- Data for Name: execution_events; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.execution_events (event_id, intent_id, kind, from_state, to_state, lease_version, step_id, attempt_id, revision_version, detail, at) FROM stdin;
\.


--
-- Data for Name: execution_ops_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.execution_ops_audit (audit_id, operation_id, action, intent_id, caller_id, subject_version, outcome, operator, reason, evidence, detail, recorded_at) FROM stdin;
\.


--
-- Data for Name: execution_steps; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.execution_steps (step_id, intent_id, action, state, owner_id, lease_version, attempt_id, anchor_attempt_id, tx_hash, outcome_class, recovery_version, revision_version, evidence, issued_at, updated_at) FROM stdin;
\.


--
-- Data for Name: indexer_checkpoint; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.indexer_checkpoint (chain_id, height, block_hash, start_height, updated_at) FROM stdin;
\.


--
-- Data for Name: indexer_lease; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.indexer_lease (chain_id, owner_id, fencing_token, expires_at, updated_at) FROM stdin;
\.


--
-- Data for Name: indexer_pause; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.indexer_pause (chain_id, height, expected_hash, actual_hash, kind, detail, created_at) FROM stdin;
\.


--
-- Data for Name: log_checkpoint; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.log_checkpoint (chain_id, start_block, config_hash, next_block, updated_at) FROM stdin;
\.


--
-- Data for Name: log_pause; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.log_pause (chain_id, height, kind, detail, created_at) FROM stdin;
\.


--
-- Data for Name: nonce_binding_events; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_binding_events (event_id, binding_id, from_state, to_state, observation_id, operation_id, detail, at) FROM stdin;
\.


--
-- Data for Name: nonce_bindings; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_bindings (binding_id, intent_id, chain_id, sender, nonce, state, authorization_id, authorization_version, registry_seq, allocation_observation_id, created_at, updated_at, consumed_at, released_at, release_operation_id) FROM stdin;
\.


--
-- Data for Name: nonce_observations; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_observations (observation_id, chain_id, sender, kind, classification, latest_count, pending_count, head_number, head_hash, error_class, rpc_ref, observed_at) FROM stdin;
\.


--
-- Data for Name: nonce_ops_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_ops_audit (audit_id, operation_id, action, chain_id, sender, subject_id, outcome, operator, reason, evidence, detail, recorded_at) FROM stdin;
\.


--
-- Data for Name: nonce_scope_holds; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_scope_holds (hold_id, chain_id, sender, cause, status, established_at, evidence_observation_id, evidence_detail, released_at, released_by, release_operation_id, release_evidence, release_observation_id) FROM stdin;
\.


--
-- Data for Name: nonce_scope_state; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_scope_state (chain_id, sender, reconciled_floor, last_latest, last_pending, last_observation_id, updated_at) FROM stdin;
\.


--
-- Data for Name: nonce_wallet_registry; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.nonce_wallet_registry (chain_id, sender, state, registry_seq, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: outbox_events; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.outbox_events (id, event_id, identity_kind, event_type, schema_version, aggregate_type, aggregate_id, aggregate_version, payload, payload_hash, occurred_at, chain_id, block_number, block_hash, tx_hash, log_index, recovery_version, revises_event_id, source_kind, source_id, source_version, publish_state, attempt_count, next_attempt_at, claim_owner, claim_expires_at, last_error_class, published_at, created_at) FROM stdin;
\.


--
-- Data for Name: payment_intents; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.payment_intents (intent_id, request_id, chain_id, sender, authorization_id, authorization_version, state, state_version, admitted_recovery_version, admitted_at, updated_at) FROM stdin;
\.


--
-- Data for Name: recon_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.recon_audit (audit_id, actor, action, target, reason, evidence, result, created_at) FROM stdin;
\.


--
-- Data for Name: recon_checkpoint; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.recon_checkpoint (task_id, seq, covered_through, covered_through_at, result_persisted_through, result_persisted_through_at, created_at) FROM stdin;
\.


--
-- Data for Name: recon_gap; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.recon_gap (gap_id, task_id, range_start, range_end, range_start_at, range_end_at, reason, created_at) FROM stdin;
\.


--
-- Data for Name: recon_permission; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.recon_permission (principal, action, scope, scope_hash, granted_by, granted_at) FROM stdin;
\.


--
-- Data for Name: recon_scan_attempt; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.recon_scan_attempt (attempt_id, task_id, range_start, range_end, range_start_at, range_end_at, state, owner, lease_expires_at, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: recon_task; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.recon_task (task_id, scope_chain_id, scope_kind, scope_start, scope_end, scope_start_at, scope_end_at, business_types, upstream_receipt_source, policy_refs, state, pause_reason, budget, history_sweep_through, created_by, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: reorg_policy_history; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.reorg_policy_history (chain_id, policy_seq, max_depth, prev_seq, operator, reason, request_id, expected_old_seq, created_at) FROM stdin;
\.


--
-- Data for Name: reorg_recovery; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.reorg_recovery (chain_id, recovery_id, phase, policy_seq, max_depth, bound_old_number, bound_old_hash, ancestor_number, ancestor_hash, new_tip_number, new_tip_hash, block_frontier, log_frontier, deposit_frontier, detected_at, updated_at, recovery_seq) FROM stdin;
\.


--
-- Data for Name: reorg_recovery_events; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.reorg_recovery_events (chain_id, recovery_id, recovery_seq, event_seq, event, detail, at) FROM stdin;
\.


--
-- Data for Name: request_status_projection; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.request_status_projection (request_id, intent_id, execution_state, state_version, lifecycle_attempt_id, lifecycle_version, lifecycle_observed_at, freshness, stale_since, updated_at) FROM stdin;
\.


--
-- Data for Name: reverify; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.reverify (reverify_id, discrepancy_id, verdict, evidence_ref, freshness_at, created_at) FROM stdin;
\.


--
-- Data for Name: signature_results; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.signature_results (signing_request_row, signature, tx_hash, signed_at) FROM stdin;
\.


--
-- Data for Name: signer_caller; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.signer_caller (caller_id, label, can_sign, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: signer_credential; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.signer_credential (credential_id, caller_id, secret_hash, secret_prefix, created_at, revoked_at) FROM stdin;
\.


--
-- Data for Name: signing_request_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.signing_request_audit (audit_id, signing_request_id, caller_id, action, reason_class, detail, recorded_at) FROM stdin;
\.


--
-- Data for Name: signing_requests; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.signing_requests (id, caller_id, signing_request_id, attempt_id, replacement_of, intent_id, binding_ref, recovery_version, chain_id, sender, nonce, tx_type, to_addr, value, data, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas, access_list, asset, recipient, amount, canonical_envelope, content_hash, authorization_id, authorization_fingerprint, authorization_state, authorization_version, policy_version, state, refusal_class, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: tx_attempt_events; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_attempt_events (event_id, attempt_id, event_seq, event, reason_class, recovery_version, detail, at) FROM stdin;
\.


--
-- Data for Name: tx_attempt_signings; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_attempt_signings (attempt_id, signature, signed_tx_bytes, tx_hash, signed_at) FROM stdin;
\.


--
-- Data for Name: tx_attempts; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_attempts (attempt_id, signing_request_id, replacement_of, intent_id, binding_ref, authorization_id, authorization_version, recovery_version, chain_id, sender, nonce, tx_type, to_addr, value, data, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas, asset, recipient, amount, canonical_envelope, content_hash, state, revision_seq, effective_at, confirmed_at, orphaned_at, replaced_at, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: tx_intent_freezes; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_intent_freezes (intent_id, cause, evidence, frozen_at, released_at, released_by, release_basis) FROM stdin;
\.


--
-- Data for Name: tx_receipts; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_receipts (receipt_id, attempt_id, tx_hash, status, block_number, block_hash, effect, transfer_detail, canonicality, confirmations, confirm_threshold, confirm_policy_seq, confirm_tip_number, confirm_tip_hash, observed_at, updated_at, confirmed_at, orphaned_at) FROM stdin;
\.


--
-- Data for Name: tx_reconciliations; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_reconciliations (reconcile_id, attempt_id, tx_hash, classification, block_number, block_hash, rpc_class, observed_at) FROM stdin;
\.


--
-- Data for Name: tx_send_attempts; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.tx_send_attempts (send_id, attempt_id, send_seq, kind, outcome, rpc_class, observed_recovery_version, observed_pause, observed_claim_version, observed_claim_expiry, observed_authorization_id, observed_authorization_version, observed_authorization_state, observed_expires_at, observed_now, observed_binding_state, dispatched_at, recorded_at) FROM stdin;
\.


--
-- Data for Name: withdrawal_authorization_scopes; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.withdrawal_authorization_scopes (authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement, authorization_version, attested_by) FROM stdin;
\.


--
-- Data for Name: withdrawal_authorizations; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.withdrawal_authorizations (authorization_id, caller_id, chain_id, asset, recipient, amount, state, expires_at, supplied_at, supplied_by) FROM stdin;
\.


--
-- Data for Name: withdrawal_grant_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.withdrawal_grant_audit (audit_id, operation_id, authorization_id, caller_id, action, operator, reason, detail, recorded_at) FROM stdin;
\.


--
-- Data for Name: withdrawal_request_audit; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.withdrawal_request_audit (audit_id, request_id, caller_id, action, detail, recorded_at) FROM stdin;
\.


--
-- Data for Name: withdrawal_requests; Type: TABLE DATA; Schema: public; Owner: writer_owner
--

COPY public.withdrawal_requests (id, request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount, status, created_at) FROM stdin;
\.


--
-- Name: api_key_key_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.api_key_key_id_seq', 1, false);


--
-- Name: consumer_quarantine_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.consumer_quarantine_id_seq', 1, false);


--
-- Name: delivery_admissions_admission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.delivery_admissions_admission_id_seq', 1, false);


--
-- Name: deposit_pause_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.deposit_pause_id_seq', 1, false);


--
-- Name: discrepancy_occurrence_occurrence_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.discrepancy_occurrence_occurrence_id_seq', 1, false);


--
-- Name: event_obligation_obligation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.event_obligation_obligation_id_seq', 1, false);


--
-- Name: event_ops_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.event_ops_audit_id_seq', 1, false);


--
-- Name: execution_events_event_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.execution_events_event_id_seq', 1, false);


--
-- Name: execution_ops_audit_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.execution_ops_audit_audit_id_seq', 1, false);


--
-- Name: nonce_binding_events_event_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.nonce_binding_events_event_id_seq', 1, false);


--
-- Name: nonce_ops_audit_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.nonce_ops_audit_audit_id_seq', 1, false);


--
-- Name: outbox_events_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.outbox_events_id_seq', 1, false);


--
-- Name: recon_audit_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.recon_audit_audit_id_seq', 1, false);


--
-- Name: recon_gap_gap_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.recon_gap_gap_id_seq', 1, false);


--
-- Name: reverify_reverify_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.reverify_reverify_id_seq', 1, false);


--
-- Name: signer_credential_credential_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.signer_credential_credential_id_seq', 1, false);


--
-- Name: signing_request_audit_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.signing_request_audit_audit_id_seq', 1, false);


--
-- Name: signing_requests_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.signing_requests_id_seq', 1, false);


--
-- Name: tx_attempt_events_event_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.tx_attempt_events_event_id_seq', 1, false);


--
-- Name: tx_receipts_receipt_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.tx_receipts_receipt_id_seq', 1, false);


--
-- Name: tx_reconciliations_reconcile_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.tx_reconciliations_reconcile_id_seq', 1, false);


--
-- Name: tx_send_attempts_send_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.tx_send_attempts_send_id_seq', 1, false);


--
-- Name: withdrawal_grant_audit_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.withdrawal_grant_audit_audit_id_seq', 1, false);


--
-- Name: withdrawal_request_audit_audit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.withdrawal_request_audit_audit_id_seq', 1, false);


--
-- Name: withdrawal_requests_id_seq; Type: SEQUENCE SET; Schema: public; Owner: writer_owner
--

SELECT pg_catalog.setval('public.withdrawal_requests_id_seq', 1, false);


--
-- Name: api_key api_key_key_hash_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.api_key
    ADD CONSTRAINT api_key_key_hash_uniq UNIQUE (key_hash);


--
-- Name: api_key api_key_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.api_key
    ADD CONSTRAINT api_key_pkey PRIMARY KEY (key_id);


--
-- Name: caller caller_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.caller
    ADD CONSTRAINT caller_pkey PRIMARY KEY (caller_id);


--
-- Name: chain_blocks chain_blocks_chain_id_hash_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.chain_blocks
    ADD CONSTRAINT chain_blocks_chain_id_hash_key UNIQUE (chain_id, hash);


--
-- Name: chain_blocks chain_blocks_chain_id_number_hash_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.chain_blocks
    ADD CONSTRAINT chain_blocks_chain_id_number_hash_key UNIQUE (chain_id, number, hash);


--
-- Name: chain_blocks chain_blocks_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.chain_blocks
    ADD CONSTRAINT chain_blocks_pkey PRIMARY KEY (chain_id, number, hash);


--
-- Name: confirmation_policy_history confirmation_policy_history_chain_id_request_id_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.confirmation_policy_history
    ADD CONSTRAINT confirmation_policy_history_chain_id_request_id_key UNIQUE (chain_id, request_id);


--
-- Name: confirmation_policy_history confirmation_policy_history_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.confirmation_policy_history
    ADD CONSTRAINT confirmation_policy_history_pkey PRIMARY KEY (chain_id, policy_seq);


--
-- Name: consumer_inbox consumer_inbox_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.consumer_inbox
    ADD CONSTRAINT consumer_inbox_pkey PRIMARY KEY (consumer_name, event_id);


--
-- Name: consumer_progress consumer_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.consumer_progress
    ADD CONSTRAINT consumer_progress_pkey PRIMARY KEY (consumer_name, topic, partition);


--
-- Name: consumer_quarantine consumer_quarantine_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.consumer_quarantine
    ADD CONSTRAINT consumer_quarantine_pkey PRIMARY KEY (id);


--
-- Name: consumer_versions consumer_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.consumer_versions
    ADD CONSTRAINT consumer_versions_pkey PRIMARY KEY (consumer_name, aggregate_type, aggregate_id);


--
-- Name: delivery_admissions delivery_admissions_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.delivery_admissions
    ADD CONSTRAINT delivery_admissions_pkey PRIMARY KEY (admission_id);


--
-- Name: delivery_admissions delivery_admissions_request_attempt_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.delivery_admissions
    ADD CONSTRAINT delivery_admissions_request_attempt_uniq UNIQUE (signing_request_row, attempt_seq);


--
-- Name: deposit_checkpoint deposit_checkpoint_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_checkpoint
    ADD CONSTRAINT deposit_checkpoint_pkey PRIMARY KEY (chain_id);


--
-- Name: deposit_config_history deposit_config_history_chain_id_request_id_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_config_history
    ADD CONSTRAINT deposit_config_history_chain_id_request_id_key UNIQUE (chain_id, request_id);


--
-- Name: deposit_config_history deposit_config_history_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_config_history
    ADD CONSTRAINT deposit_config_history_pkey PRIMARY KEY (chain_id, version_seq);


--
-- Name: deposit_observation_transitions deposit_observation_transitio_chain_id_block_hash_tx_hash_l_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_observation_transitions
    ADD CONSTRAINT deposit_observation_transitio_chain_id_block_hash_tx_hash_l_key UNIQUE (chain_id, block_hash, tx_hash, log_index, from_status, to_status, recovery_id);


--
-- Name: deposit_observations deposit_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_observations
    ADD CONSTRAINT deposit_observations_pkey PRIMARY KEY (chain_id, block_hash, tx_hash, log_index);


--
-- Name: deposit_pause_audit deposit_pause_audit_chain_id_pause_id_revision_action_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_pause_audit
    ADD CONSTRAINT deposit_pause_audit_chain_id_pause_id_revision_action_key UNIQUE (chain_id, pause_id, revision, action);


--
-- Name: deposit_pause deposit_pause_pause_id_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_pause
    ADD CONSTRAINT deposit_pause_pause_id_key UNIQUE (pause_id);


--
-- Name: deposit_pause deposit_pause_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_pause
    ADD CONSTRAINT deposit_pause_pkey PRIMARY KEY (chain_id);


--
-- Name: discrepancy_occurrence discrepancy_occurrence_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.discrepancy_occurrence
    ADD CONSTRAINT discrepancy_occurrence_pkey PRIMARY KEY (occurrence_id);


--
-- Name: discrepancy discrepancy_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.discrepancy
    ADD CONSTRAINT discrepancy_pkey PRIMARY KEY (discrepancy_id);


--
-- Name: disposition disposition_idempotency_key_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.disposition
    ADD CONSTRAINT disposition_idempotency_key_uniq UNIQUE (idempotency_key);


--
-- Name: disposition disposition_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.disposition
    ADD CONSTRAINT disposition_pkey PRIMARY KEY (disposition_id);


--
-- Name: erc20_transfer_logs erc20_transfer_logs_chain_id_block_hash_log_index_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.erc20_transfer_logs
    ADD CONSTRAINT erc20_transfer_logs_chain_id_block_hash_log_index_key UNIQUE (chain_id, block_hash, log_index);


--
-- Name: erc20_transfer_logs erc20_transfer_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.erc20_transfer_logs
    ADD CONSTRAINT erc20_transfer_logs_pkey PRIMARY KEY (chain_id, block_hash, tx_hash, log_index);


--
-- Name: event_obligation event_obligation_identity_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.event_obligation
    ADD CONSTRAINT event_obligation_identity_uniq UNIQUE (aggregate_type, aggregate_id, aggregate_version, expected_event_type);


--
-- Name: event_obligation event_obligation_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.event_obligation
    ADD CONSTRAINT event_obligation_pkey PRIMARY KEY (obligation_id);


--
-- Name: event_ops_audit event_ops_audit_operation_id_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.event_ops_audit
    ADD CONSTRAINT event_ops_audit_operation_id_key UNIQUE (operation_id);


--
-- Name: event_ops_audit event_ops_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.event_ops_audit
    ADD CONSTRAINT event_ops_audit_pkey PRIMARY KEY (id);


--
-- Name: event_system_state event_system_state_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.event_system_state
    ADD CONSTRAINT event_system_state_pkey PRIMARY KEY (id);


--
-- Name: execution_caller_permission execution_caller_permission_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_caller_permission
    ADD CONSTRAINT execution_caller_permission_pkey PRIMARY KEY (caller_id);


--
-- Name: execution_claims execution_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_claims
    ADD CONSTRAINT execution_claims_pkey PRIMARY KEY (intent_id);


--
-- Name: execution_events execution_events_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_events
    ADD CONSTRAINT execution_events_pkey PRIMARY KEY (event_id);


--
-- Name: execution_ops_audit execution_ops_audit_operation_id_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_ops_audit
    ADD CONSTRAINT execution_ops_audit_operation_id_uniq UNIQUE (operation_id);


--
-- Name: execution_ops_audit execution_ops_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_ops_audit
    ADD CONSTRAINT execution_ops_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: execution_steps execution_steps_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_steps
    ADD CONSTRAINT execution_steps_pkey PRIMARY KEY (step_id);


--
-- Name: indexer_checkpoint indexer_checkpoint_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.indexer_checkpoint
    ADD CONSTRAINT indexer_checkpoint_pkey PRIMARY KEY (chain_id);


--
-- Name: indexer_lease indexer_lease_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.indexer_lease
    ADD CONSTRAINT indexer_lease_pkey PRIMARY KEY (chain_id);


--
-- Name: indexer_pause indexer_pause_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.indexer_pause
    ADD CONSTRAINT indexer_pause_pkey PRIMARY KEY (chain_id);


--
-- Name: log_checkpoint log_checkpoint_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.log_checkpoint
    ADD CONSTRAINT log_checkpoint_pkey PRIMARY KEY (chain_id);


--
-- Name: log_pause log_pause_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.log_pause
    ADD CONSTRAINT log_pause_pkey PRIMARY KEY (chain_id);


--
-- Name: nonce_binding_events nonce_binding_events_binding_to_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_binding_events
    ADD CONSTRAINT nonce_binding_events_binding_to_uniq UNIQUE (binding_id, to_state);


--
-- Name: nonce_binding_events nonce_binding_events_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_binding_events
    ADD CONSTRAINT nonce_binding_events_pkey PRIMARY KEY (event_id);


--
-- Name: nonce_bindings nonce_bindings_intent_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_bindings
    ADD CONSTRAINT nonce_bindings_intent_uniq UNIQUE (intent_id);


--
-- Name: nonce_bindings nonce_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_bindings
    ADD CONSTRAINT nonce_bindings_pkey PRIMARY KEY (binding_id);


--
-- Name: nonce_bindings nonce_bindings_scope_nonce_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_bindings
    ADD CONSTRAINT nonce_bindings_scope_nonce_uniq UNIQUE (chain_id, sender, nonce);


--
-- Name: nonce_observations nonce_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_observations
    ADD CONSTRAINT nonce_observations_pkey PRIMARY KEY (observation_id);


--
-- Name: nonce_ops_audit nonce_ops_audit_operation_id_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_ops_audit
    ADD CONSTRAINT nonce_ops_audit_operation_id_uniq UNIQUE (operation_id);


--
-- Name: nonce_ops_audit nonce_ops_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_ops_audit
    ADD CONSTRAINT nonce_ops_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: nonce_scope_holds nonce_scope_holds_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_scope_holds
    ADD CONSTRAINT nonce_scope_holds_pkey PRIMARY KEY (hold_id);


--
-- Name: nonce_scope_state nonce_scope_state_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_scope_state
    ADD CONSTRAINT nonce_scope_state_pkey PRIMARY KEY (chain_id, sender);


--
-- Name: nonce_wallet_registry nonce_wallet_registry_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_wallet_registry
    ADD CONSTRAINT nonce_wallet_registry_pkey PRIMARY KEY (chain_id, sender);


--
-- Name: outbox_events outbox_events_event_id_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_event_id_uniq UNIQUE (event_id);


--
-- Name: outbox_events outbox_events_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (id);


--
-- Name: payment_intents payment_intents_authorization_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.payment_intents
    ADD CONSTRAINT payment_intents_authorization_uniq UNIQUE (authorization_id);


--
-- Name: payment_intents payment_intents_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.payment_intents
    ADD CONSTRAINT payment_intents_pkey PRIMARY KEY (intent_id);


--
-- Name: payment_intents payment_intents_request_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.payment_intents
    ADD CONSTRAINT payment_intents_request_uniq UNIQUE (request_id);


--
-- Name: recon_audit recon_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_audit
    ADD CONSTRAINT recon_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: recon_checkpoint recon_checkpoint_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_checkpoint
    ADD CONSTRAINT recon_checkpoint_pkey PRIMARY KEY (task_id, seq);


--
-- Name: recon_gap recon_gap_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_gap
    ADD CONSTRAINT recon_gap_pkey PRIMARY KEY (gap_id);


--
-- Name: recon_permission recon_permission_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_permission
    ADD CONSTRAINT recon_permission_pkey PRIMARY KEY (principal, action, scope_hash);


--
-- Name: recon_scan_attempt recon_scan_attempt_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_scan_attempt
    ADD CONSTRAINT recon_scan_attempt_pkey PRIMARY KEY (attempt_id);


--
-- Name: recon_scan_attempt recon_scan_attempt_task_attempt_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_scan_attempt
    ADD CONSTRAINT recon_scan_attempt_task_attempt_uniq UNIQUE (task_id, attempt_id);


--
-- Name: recon_task recon_task_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_task
    ADD CONSTRAINT recon_task_pkey PRIMARY KEY (task_id);


--
-- Name: reorg_policy_history reorg_policy_history_chain_id_request_id_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_policy_history
    ADD CONSTRAINT reorg_policy_history_chain_id_request_id_key UNIQUE (chain_id, request_id);


--
-- Name: reorg_policy_history reorg_policy_history_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_policy_history
    ADD CONSTRAINT reorg_policy_history_pkey PRIMARY KEY (chain_id, policy_seq);


--
-- Name: reorg_recovery_events reorg_recovery_events_chain_id_recovery_id_event_seq_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_recovery_events
    ADD CONSTRAINT reorg_recovery_events_chain_id_recovery_id_event_seq_key UNIQUE (chain_id, recovery_id, event_seq);


--
-- Name: reorg_recovery reorg_recovery_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_recovery
    ADD CONSTRAINT reorg_recovery_pkey PRIMARY KEY (chain_id);


--
-- Name: reorg_recovery reorg_recovery_recovery_id_key; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_recovery
    ADD CONSTRAINT reorg_recovery_recovery_id_key UNIQUE (recovery_id);


--
-- Name: request_status_projection request_status_projection_intent_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.request_status_projection
    ADD CONSTRAINT request_status_projection_intent_uniq UNIQUE (intent_id);


--
-- Name: request_status_projection request_status_projection_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.request_status_projection
    ADD CONSTRAINT request_status_projection_pkey PRIMARY KEY (request_id);


--
-- Name: reverify reverify_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reverify
    ADD CONSTRAINT reverify_pkey PRIMARY KEY (reverify_id);


--
-- Name: signature_results signature_results_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signature_results
    ADD CONSTRAINT signature_results_pkey PRIMARY KEY (signing_request_row);


--
-- Name: signature_results signature_results_tx_hash_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signature_results
    ADD CONSTRAINT signature_results_tx_hash_uniq UNIQUE (tx_hash);


--
-- Name: signer_caller signer_caller_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signer_caller
    ADD CONSTRAINT signer_caller_pkey PRIMARY KEY (caller_id);


--
-- Name: signer_credential signer_credential_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signer_credential
    ADD CONSTRAINT signer_credential_pkey PRIMARY KEY (credential_id);


--
-- Name: signer_credential signer_credential_secret_hash_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signer_credential
    ADD CONSTRAINT signer_credential_secret_hash_uniq UNIQUE (secret_hash);


--
-- Name: signing_request_audit signing_request_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signing_request_audit
    ADD CONSTRAINT signing_request_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: signing_requests signing_requests_attempt_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signing_requests
    ADD CONSTRAINT signing_requests_attempt_uniq UNIQUE (attempt_id);


--
-- Name: signing_requests signing_requests_caller_request_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signing_requests
    ADD CONSTRAINT signing_requests_caller_request_uniq UNIQUE (caller_id, signing_request_id);


--
-- Name: signing_requests signing_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signing_requests
    ADD CONSTRAINT signing_requests_pkey PRIMARY KEY (id);


--
-- Name: tx_attempt_events tx_attempt_events_attempt_seq_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempt_events
    ADD CONSTRAINT tx_attempt_events_attempt_seq_uniq UNIQUE (attempt_id, event_seq);


--
-- Name: tx_attempt_events tx_attempt_events_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempt_events
    ADD CONSTRAINT tx_attempt_events_pkey PRIMARY KEY (event_id);


--
-- Name: tx_attempt_signings tx_attempt_signings_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempt_signings
    ADD CONSTRAINT tx_attempt_signings_pkey PRIMARY KEY (attempt_id);


--
-- Name: tx_attempt_signings tx_attempt_signings_tx_hash_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempt_signings
    ADD CONSTRAINT tx_attempt_signings_tx_hash_uniq UNIQUE (tx_hash);


--
-- Name: tx_attempts tx_attempts_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempts
    ADD CONSTRAINT tx_attempts_pkey PRIMARY KEY (attempt_id);


--
-- Name: tx_attempts tx_attempts_signing_request_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempts
    ADD CONSTRAINT tx_attempts_signing_request_uniq UNIQUE (signing_request_id);


--
-- Name: tx_intent_freezes tx_intent_freezes_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_intent_freezes
    ADD CONSTRAINT tx_intent_freezes_pkey PRIMARY KEY (intent_id);


--
-- Name: tx_receipts tx_receipts_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_receipts
    ADD CONSTRAINT tx_receipts_pkey PRIMARY KEY (receipt_id);


--
-- Name: tx_receipts tx_receipts_tx_block_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_receipts
    ADD CONSTRAINT tx_receipts_tx_block_uniq UNIQUE (tx_hash, block_hash);


--
-- Name: tx_reconciliations tx_reconciliations_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_reconciliations
    ADD CONSTRAINT tx_reconciliations_pkey PRIMARY KEY (reconcile_id);


--
-- Name: tx_send_attempts tx_send_attempts_attempt_seq_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_send_attempts
    ADD CONSTRAINT tx_send_attempts_attempt_seq_uniq UNIQUE (attempt_id, send_seq);


--
-- Name: tx_send_attempts tx_send_attempts_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_send_attempts
    ADD CONSTRAINT tx_send_attempts_pkey PRIMARY KEY (send_id);


--
-- Name: withdrawal_authorization_scopes withdrawal_authorization_scopes_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_authorization_scopes
    ADD CONSTRAINT withdrawal_authorization_scopes_pkey PRIMARY KEY (authorization_id);


--
-- Name: withdrawal_authorizations withdrawal_authorizations_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_authorizations
    ADD CONSTRAINT withdrawal_authorizations_pkey PRIMARY KEY (authorization_id);


--
-- Name: withdrawal_grant_audit withdrawal_grant_audit_operation_id_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_grant_audit
    ADD CONSTRAINT withdrawal_grant_audit_operation_id_uniq UNIQUE (operation_id);


--
-- Name: withdrawal_grant_audit withdrawal_grant_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_grant_audit
    ADD CONSTRAINT withdrawal_grant_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: withdrawal_request_audit withdrawal_request_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_request_audit
    ADD CONSTRAINT withdrawal_request_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: withdrawal_requests withdrawal_requests_authorization_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_requests
    ADD CONSTRAINT withdrawal_requests_authorization_uniq UNIQUE (authorization_id);


--
-- Name: withdrawal_requests withdrawal_requests_caller_key_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_requests
    ADD CONSTRAINT withdrawal_requests_caller_key_uniq UNIQUE (caller_id, idempotency_key);


--
-- Name: withdrawal_requests withdrawal_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_requests
    ADD CONSTRAINT withdrawal_requests_pkey PRIMARY KEY (id);


--
-- Name: withdrawal_requests withdrawal_requests_request_id_uniq; Type: CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_requests
    ADD CONSTRAINT withdrawal_requests_request_id_uniq UNIQUE (request_id);


--
-- Name: chain_blocks_canonical_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX chain_blocks_canonical_uniq ON public.chain_blocks USING btree (chain_id, number) WHERE canonical;


--
-- Name: confirmation_policy_history_bootstrap_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX confirmation_policy_history_bootstrap_uniq ON public.confirmation_policy_history USING btree (chain_id) WHERE (request_id IS NULL);


--
-- Name: consumer_inbox_aggregate_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX consumer_inbox_aggregate_idx ON public.consumer_inbox USING btree (consumer_name, aggregate_type, aggregate_id);


--
-- Name: consumer_quarantine_open_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX consumer_quarantine_open_uniq ON public.consumer_quarantine USING btree (consumer_name, event_id) WHERE (status = 'open'::text);


--
-- Name: deposit_config_history_bootstrap_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX deposit_config_history_bootstrap_uniq ON public.deposit_config_history USING btree (chain_id) WHERE (request_id IS NULL);


--
-- Name: deposit_observations_height_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX deposit_observations_height_idx ON public.deposit_observations USING btree (chain_id, block_number);


--
-- Name: deposit_observations_pending_height_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX deposit_observations_pending_height_idx ON public.deposit_observations USING btree (chain_id, block_number) WHERE (status = 'pending'::text);


--
-- Name: deposit_observations_recipient_height_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX deposit_observations_recipient_height_idx ON public.deposit_observations USING btree (chain_id, recipient, block_number);


--
-- Name: discrepancy_business_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX discrepancy_business_idx ON public.discrepancy USING btree (category, business_key);


--
-- Name: discrepancy_claim_queue_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX discrepancy_claim_queue_idx ON public.discrepancy USING btree (updated_at) WHERE (state = 'open_claimable'::text);


--
-- Name: discrepancy_occurrence_discrepancy_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX discrepancy_occurrence_discrepancy_idx ON public.discrepancy_occurrence USING btree (discrepancy_id, observed_at);


--
-- Name: discrepancy_occurrence_task_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX discrepancy_occurrence_task_idx ON public.discrepancy_occurrence USING btree (scan_task_id);


--
-- Name: discrepancy_state_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX discrepancy_state_idx ON public.discrepancy USING btree (state, updated_at);


--
-- Name: disposition_discrepancy_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX disposition_discrepancy_idx ON public.disposition USING btree (discrepancy_id, created_at);


--
-- Name: erc20_transfer_logs_height_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX erc20_transfer_logs_height_idx ON public.erc20_transfer_logs USING btree (chain_id, block_number);


--
-- Name: event_obligation_aggregate_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX event_obligation_aggregate_idx ON public.event_obligation USING btree (aggregate_type, aggregate_id);


--
-- Name: event_obligation_obligated_at_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX event_obligation_obligated_at_idx ON public.event_obligation USING btree (obligated_at);


--
-- Name: execution_events_intent_time_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX execution_events_intent_time_idx ON public.execution_events USING btree (intent_id, at);


--
-- Name: execution_steps_open_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX execution_steps_open_uniq ON public.execution_steps USING btree (intent_id) WHERE (state = 'issued'::text);


--
-- Name: nonce_observations_scope_time_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX nonce_observations_scope_time_idx ON public.nonce_observations USING btree (chain_id, sender, observed_at);


--
-- Name: outbox_events_log_identity_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX outbox_events_log_identity_uniq ON public.outbox_events USING btree (chain_id, block_hash, tx_hash, log_index) WHERE (identity_kind = 'evm_log'::text);


--
-- Name: outbox_events_object_identity_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX outbox_events_object_identity_uniq ON public.outbox_events USING btree (aggregate_type, aggregate_id, aggregate_version) WHERE (identity_kind = 'business_object'::text);


--
-- Name: outbox_events_pending_capacity_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX outbox_events_pending_capacity_idx ON public.outbox_events USING btree (id) WHERE (publish_state = 'pending'::text);


--
-- Name: outbox_events_pending_queue_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX outbox_events_pending_queue_idx ON public.outbox_events USING btree (next_attempt_at, id) WHERE (publish_state = 'pending'::text);


--
-- Name: outbox_events_published_retention_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX outbox_events_published_retention_idx ON public.outbox_events USING btree (published_at) WHERE (publish_state = 'published'::text);


--
-- Name: outbox_events_source_audit_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX outbox_events_source_audit_idx ON public.outbox_events USING btree (source_kind, source_id, source_version);


--
-- Name: recon_audit_action_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_audit_action_idx ON public.recon_audit USING btree (action, created_at);


--
-- Name: recon_audit_actor_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_audit_actor_idx ON public.recon_audit USING btree (actor, created_at);


--
-- Name: recon_gap_task_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_gap_task_idx ON public.recon_gap USING btree (task_id, created_at);


--
-- Name: recon_scan_attempt_claimed_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX recon_scan_attempt_claimed_uniq ON public.recon_scan_attempt USING btree (task_id) WHERE (state = 'claimed'::text);


--
-- Name: recon_scan_attempt_lease_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_scan_attempt_lease_idx ON public.recon_scan_attempt USING btree (lease_expires_at) WHERE (state = 'claimed'::text);


--
-- Name: recon_scan_attempt_task_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_scan_attempt_task_idx ON public.recon_scan_attempt USING btree (task_id, created_at);


--
-- Name: recon_task_chain_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_task_chain_idx ON public.recon_task USING btree (scope_chain_id, created_at);


--
-- Name: recon_task_state_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX recon_task_state_idx ON public.recon_task USING btree (state, updated_at);


--
-- Name: reorg_policy_history_bootstrap_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX reorg_policy_history_bootstrap_uniq ON public.reorg_policy_history USING btree (chain_id) WHERE (request_id IS NULL);


--
-- Name: reverify_discrepancy_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX reverify_discrepancy_idx ON public.reverify USING btree (discrepancy_id, created_at);


--
-- Name: signing_requests_authorization_anchor_uniq; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE UNIQUE INDEX signing_requests_authorization_anchor_uniq ON public.signing_requests USING btree (authorization_id) WHERE (replacement_of IS NULL);


--
-- Name: tx_attempt_events_attempt_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_attempt_events_attempt_idx ON public.tx_attempt_events USING btree (attempt_id, event_seq);


--
-- Name: tx_attempts_intent_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_attempts_intent_idx ON public.tx_attempts USING btree (intent_id);


--
-- Name: tx_attempts_scan_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_attempts_scan_idx ON public.tx_attempts USING btree (state, updated_at);


--
-- Name: tx_receipts_attempt_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_receipts_attempt_idx ON public.tx_receipts USING btree (attempt_id, observed_at);


--
-- Name: tx_receipts_open_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_receipts_open_idx ON public.tx_receipts USING btree (canonicality, block_number) WHERE (canonicality <> 'orphaned'::text);


--
-- Name: tx_reconciliations_attempt_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_reconciliations_attempt_idx ON public.tx_reconciliations USING btree (attempt_id, observed_at);


--
-- Name: tx_send_attempts_attempt_idx; Type: INDEX; Schema: public; Owner: writer_owner
--

CREATE INDEX tx_send_attempts_attempt_idx ON public.tx_send_attempts USING btree (attempt_id, send_seq);


--
-- Name: api_key api_key_caller_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.api_key
    ADD CONSTRAINT api_key_caller_id_fkey FOREIGN KEY (caller_id) REFERENCES public.caller(caller_id);


--
-- Name: confirmation_policy_history confirmation_policy_history_chain_id_prev_seq_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.confirmation_policy_history
    ADD CONSTRAINT confirmation_policy_history_chain_id_prev_seq_fkey FOREIGN KEY (chain_id, prev_seq) REFERENCES public.confirmation_policy_history(chain_id, policy_seq);


--
-- Name: delivery_admissions delivery_admissions_request_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.delivery_admissions
    ADD CONSTRAINT delivery_admissions_request_fkey FOREIGN KEY (signing_request_row) REFERENCES public.signing_requests(id);


--
-- Name: deposit_config_history deposit_config_history_chain_id_prev_seq_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_config_history
    ADD CONSTRAINT deposit_config_history_chain_id_prev_seq_fkey FOREIGN KEY (chain_id, prev_seq) REFERENCES public.deposit_config_history(chain_id, version_seq);


--
-- Name: deposit_observations deposit_observations_chain_id_version_seq_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_observations
    ADD CONSTRAINT deposit_observations_chain_id_version_seq_fkey FOREIGN KEY (chain_id, version_seq) REFERENCES public.deposit_config_history(chain_id, version_seq);


--
-- Name: deposit_observations deposit_observations_confirm_policy_seq_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.deposit_observations
    ADD CONSTRAINT deposit_observations_confirm_policy_seq_fkey FOREIGN KEY (chain_id, confirm_policy_seq) REFERENCES public.confirmation_policy_history(chain_id, policy_seq);


--
-- Name: discrepancy discrepancy_linked_to_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.discrepancy
    ADD CONSTRAINT discrepancy_linked_to_fkey FOREIGN KEY (linked_to) REFERENCES public.discrepancy(discrepancy_id);


--
-- Name: discrepancy_occurrence discrepancy_occurrence_discrepancy_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.discrepancy_occurrence
    ADD CONSTRAINT discrepancy_occurrence_discrepancy_fkey FOREIGN KEY (discrepancy_id) REFERENCES public.discrepancy(discrepancy_id);


--
-- Name: discrepancy_occurrence discrepancy_occurrence_scan_task_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.discrepancy_occurrence
    ADD CONSTRAINT discrepancy_occurrence_scan_task_fkey FOREIGN KEY (scan_task_id) REFERENCES public.recon_task(task_id);


--
-- Name: disposition disposition_discrepancy_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.disposition
    ADD CONSTRAINT disposition_discrepancy_fkey FOREIGN KEY (discrepancy_id) REFERENCES public.discrepancy(discrepancy_id);


--
-- Name: execution_caller_permission execution_caller_permission_caller_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_caller_permission
    ADD CONSTRAINT execution_caller_permission_caller_fkey FOREIGN KEY (caller_id) REFERENCES public.caller(caller_id);


--
-- Name: execution_claims execution_claims_intent_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_claims
    ADD CONSTRAINT execution_claims_intent_fkey FOREIGN KEY (intent_id) REFERENCES public.payment_intents(intent_id);


--
-- Name: execution_steps execution_steps_intent_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.execution_steps
    ADD CONSTRAINT execution_steps_intent_fkey FOREIGN KEY (intent_id) REFERENCES public.payment_intents(intent_id);


--
-- Name: indexer_checkpoint indexer_checkpoint_chain_id_height_block_hash_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.indexer_checkpoint
    ADD CONSTRAINT indexer_checkpoint_chain_id_height_block_hash_fkey FOREIGN KEY (chain_id, height, block_hash) REFERENCES public.chain_blocks(chain_id, number, hash);


--
-- Name: nonce_bindings nonce_bindings_registry_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_bindings
    ADD CONSTRAINT nonce_bindings_registry_fkey FOREIGN KEY (chain_id, sender) REFERENCES public.nonce_wallet_registry(chain_id, sender);


--
-- Name: nonce_scope_holds nonce_scope_holds_registry_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_scope_holds
    ADD CONSTRAINT nonce_scope_holds_registry_fkey FOREIGN KEY (chain_id, sender) REFERENCES public.nonce_wallet_registry(chain_id, sender);


--
-- Name: nonce_scope_state nonce_scope_state_registry_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.nonce_scope_state
    ADD CONSTRAINT nonce_scope_state_registry_fkey FOREIGN KEY (chain_id, sender) REFERENCES public.nonce_wallet_registry(chain_id, sender);


--
-- Name: payment_intents payment_intents_request_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.payment_intents
    ADD CONSTRAINT payment_intents_request_fkey FOREIGN KEY (request_id) REFERENCES public.withdrawal_requests(request_id);


--
-- Name: recon_checkpoint recon_checkpoint_task_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_checkpoint
    ADD CONSTRAINT recon_checkpoint_task_fkey FOREIGN KEY (task_id) REFERENCES public.recon_task(task_id);


--
-- Name: recon_gap recon_gap_task_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_gap
    ADD CONSTRAINT recon_gap_task_fkey FOREIGN KEY (task_id) REFERENCES public.recon_task(task_id);


--
-- Name: recon_scan_attempt recon_scan_attempt_task_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.recon_scan_attempt
    ADD CONSTRAINT recon_scan_attempt_task_fkey FOREIGN KEY (task_id) REFERENCES public.recon_task(task_id);


--
-- Name: reorg_policy_history reorg_policy_history_chain_id_prev_seq_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_policy_history
    ADD CONSTRAINT reorg_policy_history_chain_id_prev_seq_fkey FOREIGN KEY (chain_id, prev_seq) REFERENCES public.reorg_policy_history(chain_id, policy_seq);


--
-- Name: reorg_recovery reorg_recovery_chain_id_policy_seq_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reorg_recovery
    ADD CONSTRAINT reorg_recovery_chain_id_policy_seq_fkey FOREIGN KEY (chain_id, policy_seq) REFERENCES public.reorg_policy_history(chain_id, policy_seq);


--
-- Name: request_status_projection request_status_projection_intent_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.request_status_projection
    ADD CONSTRAINT request_status_projection_intent_fkey FOREIGN KEY (intent_id) REFERENCES public.payment_intents(intent_id);


--
-- Name: reverify reverify_discrepancy_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.reverify
    ADD CONSTRAINT reverify_discrepancy_fkey FOREIGN KEY (discrepancy_id) REFERENCES public.discrepancy(discrepancy_id);


--
-- Name: signature_results signature_results_request_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signature_results
    ADD CONSTRAINT signature_results_request_fkey FOREIGN KEY (signing_request_row) REFERENCES public.signing_requests(id);


--
-- Name: signer_credential signer_credential_caller_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signer_credential
    ADD CONSTRAINT signer_credential_caller_id_fkey FOREIGN KEY (caller_id) REFERENCES public.signer_caller(caller_id);


--
-- Name: signing_requests signing_requests_caller_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signing_requests
    ADD CONSTRAINT signing_requests_caller_id_fkey FOREIGN KEY (caller_id) REFERENCES public.signer_caller(caller_id);


--
-- Name: signing_requests signing_requests_replacement_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.signing_requests
    ADD CONSTRAINT signing_requests_replacement_fkey FOREIGN KEY (replacement_of) REFERENCES public.signing_requests(id);


--
-- Name: tx_attempt_signings tx_attempt_signings_attempt_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempt_signings
    ADD CONSTRAINT tx_attempt_signings_attempt_fkey FOREIGN KEY (attempt_id) REFERENCES public.tx_attempts(attempt_id);


--
-- Name: tx_attempts tx_attempts_authorization_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempts
    ADD CONSTRAINT tx_attempts_authorization_fkey FOREIGN KEY (authorization_id) REFERENCES public.withdrawal_authorizations(authorization_id);


--
-- Name: tx_attempts tx_attempts_binding_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempts
    ADD CONSTRAINT tx_attempts_binding_fkey FOREIGN KEY (binding_ref) REFERENCES public.nonce_bindings(binding_id);


--
-- Name: tx_attempts tx_attempts_intent_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempts
    ADD CONSTRAINT tx_attempts_intent_fkey FOREIGN KEY (intent_id) REFERENCES public.payment_intents(intent_id);


--
-- Name: tx_attempts tx_attempts_replacement_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_attempts
    ADD CONSTRAINT tx_attempts_replacement_fkey FOREIGN KEY (replacement_of) REFERENCES public.tx_attempts(attempt_id);


--
-- Name: tx_receipts tx_receipts_attempt_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_receipts
    ADD CONSTRAINT tx_receipts_attempt_fkey FOREIGN KEY (attempt_id) REFERENCES public.tx_attempts(attempt_id);


--
-- Name: tx_reconciliations tx_reconciliations_attempt_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_reconciliations
    ADD CONSTRAINT tx_reconciliations_attempt_fkey FOREIGN KEY (attempt_id) REFERENCES public.tx_attempts(attempt_id);


--
-- Name: tx_send_attempts tx_send_attempts_attempt_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.tx_send_attempts
    ADD CONSTRAINT tx_send_attempts_attempt_fkey FOREIGN KEY (attempt_id) REFERENCES public.tx_attempts(attempt_id);


--
-- Name: withdrawal_authorization_scopes withdrawal_authorization_scopes_authorization_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_authorization_scopes
    ADD CONSTRAINT withdrawal_authorization_scopes_authorization_fkey FOREIGN KEY (authorization_id) REFERENCES public.withdrawal_authorizations(authorization_id);


--
-- Name: withdrawal_authorizations withdrawal_authorizations_caller_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_authorizations
    ADD CONSTRAINT withdrawal_authorizations_caller_id_fkey FOREIGN KEY (caller_id) REFERENCES public.caller(caller_id);


--
-- Name: withdrawal_requests withdrawal_requests_caller_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: writer_owner
--

ALTER TABLE ONLY public.withdrawal_requests
    ADD CONSTRAINT withdrawal_requests_caller_id_fkey FOREIGN KEY (caller_id) REFERENCES public.caller(caller_id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO recovery_r;


--
-- PostgreSQL database dump complete
--

\unrestrict DFn9rSv7xSuOduhuk1Qvu12JzxxHgfaYxxTSrg6r9cyrQcNN409vFgH7WlDIq1k

