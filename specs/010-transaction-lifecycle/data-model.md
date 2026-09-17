# Data Model: 010 Transaction Lifecycle Management

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) | **Research**: [research.md](research.md) R-010-01..14

**Migration**: `migrations/000011_tx_lifecycle.sql` (provisional number, re-verified at merge — PLAN-1; pure DDL, no stored functions, additive-only; `000010` is PB-occupied). Tables 1–6 below are 010-owned; every constraint consumed by `ConstraintName` classification carries an explicit `CONSTRAINT <name>`.

**Conventions** (follow 004–009): lowercase `0x` hex addresses/hashes, `NUMERIC(78,0)` for uint256-domain values, `BIGINT` for chains/counters/versions, `TIMESTAMPTZ DEFAULT now()`, append-only audit/event tables, app-owned timeout/transaction behavior, no floats anywhere (constitution I). No cross-lane FK to 011-owned tables (they do not exist at `000011` apply time; merge order 010 → 011 — G-010-3); FKs are declared to existing upstream PKs `nonce_bindings(binding_id)` and `withdrawal_authorizations(authorization_id)`.

## Table 1 — `tx_attempts` (identity + immutable content + current state)

One row per transaction attempt. Content is immutable after insert (no UPDATE path for identity/content columns); only `state`, `revision_seq`, `updated_at` (and state-fact timestamps) change, always through version-guarded updates.

```sql
CREATE TABLE tx_attempts (
    attempt_id             TEXT          NOT NULL,
    signing_request_id     TEXT          NOT NULL,
    replacement_of         TEXT,
    intent_id              TEXT          NOT NULL,
    binding_ref            TEXT          NOT NULL,
    authorization_id       TEXT          NOT NULL,
    authorization_version  BIGINT        NOT NULL,
    recovery_version       BIGINT        NOT NULL,
    chain_id               BIGINT        NOT NULL,
    sender                 TEXT          NOT NULL,
    nonce                  NUMERIC(78,0) NOT NULL,
    tx_type                INTEGER       NOT NULL,
    to_addr                TEXT          NOT NULL,
    value                  NUMERIC(78,0) NOT NULL,
    data                   BYTEA         NOT NULL,
    gas_limit              NUMERIC(78,0) NOT NULL,
    gas_price              NUMERIC(78,0),
    max_fee_per_gas        NUMERIC(78,0),
    max_priority_fee_per_gas NUMERIC(78,0),
    asset                  TEXT          NOT NULL,
    recipient              TEXT          NOT NULL,
    amount                 NUMERIC(78,0) NOT NULL,
    canonical_envelope     TEXT          NOT NULL,
    content_hash           TEXT          NOT NULL,
    state                  TEXT          NOT NULL DEFAULT 'prepared',
    revision_seq           BIGINT        NOT NULL DEFAULT 1,
    effective_at           TIMESTAMPTZ,
    confirmed_at           TIMESTAMPTZ,
    orphaned_at            TIMESTAMPTZ,
    replaced_at            TIMESTAMPTZ,
    created_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT tx_attempts_pkey PRIMARY KEY (attempt_id),
    CONSTRAINT tx_attempts_signing_request_uniq UNIQUE (signing_request_id),
    CONSTRAINT tx_attempts_replacement_fkey FOREIGN KEY (replacement_of)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_attempts_binding_fkey FOREIGN KEY (binding_ref)
        REFERENCES nonce_bindings (binding_id),
    CONSTRAINT tx_attempts_authorization_fkey FOREIGN KEY (authorization_id)
        REFERENCES withdrawal_authorizations (authorization_id),
    CONSTRAINT tx_attempts_state_check CHECK (state IN (
        'prepared', 'signed', 'sent', 'unknown',
        'effective', 'ineffective', 'confirmed', 'orphaned', 'replaced')),
    CONSTRAINT tx_attempts_attempt_shape
        CHECK (length(attempt_id) BETWEEN 1 AND 128 AND attempt_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT tx_attempts_signing_request_shape
        CHECK (length(signing_request_id) BETWEEN 1 AND 128 AND signing_request_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT tx_attempts_intent_shape
        CHECK (length(intent_id) BETWEEN 1 AND 128 AND intent_id ~ '^[\x21-\x7e]+$'),
    CONSTRAINT tx_attempts_replacement_not_self
        CHECK (replacement_of IS NULL OR replacement_of <> attempt_id),
    CONSTRAINT tx_attempts_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT tx_attempts_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT tx_attempts_to_check CHECK (to_addr ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT tx_attempts_asset_check CHECK (asset = to_addr),
    CONSTRAINT tx_attempts_recipient_check CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT tx_attempts_value_check
        CHECK (value >= 0 AND value <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT tx_attempts_amount_check
        CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT tx_attempts_nonce_check CHECK (nonce >= 0 AND nonce <= 18446744073709551615),
    CONSTRAINT tx_attempts_recovery_version_check CHECK (recovery_version >= 0),
    CONSTRAINT tx_attempts_authorization_version_check CHECK (authorization_version >= 1),
    CONSTRAINT tx_attempts_gas_limit_check CHECK (gas_limit > 0),
    CONSTRAINT tx_attempts_fee_shape_check CHECK (
        (tx_type = 0 AND gas_price IS NOT NULL
                     AND max_fee_per_gas IS NULL AND max_priority_fee_per_gas IS NULL)
        OR
        (tx_type = 2 AND gas_price IS NULL
                     AND max_fee_per_gas IS NOT NULL AND max_priority_fee_per_gas IS NOT NULL
                     AND max_priority_fee_per_gas <= max_fee_per_gas)),
    CONSTRAINT tx_attempts_content_hash_check CHECK (content_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_attempts_revision_seq_check CHECK (revision_seq > 0),
    CONSTRAINT tx_attempts_state_facts_check CHECK (
        (state <> 'confirmed' OR confirmed_at IS NOT NULL)
        AND (state <> 'orphaned'  OR orphaned_at IS NOT NULL)
        AND (state <> 'replaced'  OR replaced_at IS NOT NULL)
        AND (state NOT IN ('effective', 'confirmed') OR effective_at IS NOT NULL))
);
CREATE INDEX tx_attempts_scan_idx ON tx_attempts (state, updated_at);
CREATE INDEX tx_attempts_intent_idx ON tx_attempts (intent_id);
```

Notes:
- `tx_attempts_state_facts_check` is an implication set, not a biconditional: a revision (confirmed → orphaned) keeps the earlier facts as history evidence, exactly like 006's orphaned deposits (`migrations/000006_reorg_recovery.sql:91-110`).
- `asset = to_addr` encodes v1's ERC-20-only shape (asset contract = `to`) at storage level behind the Go validator (009 precedent `signing_requests_asset_check`).
- `content_hash` is **not** UNIQUE by design (R-010-02): a recovery-version rebuild may legitimately reuse the economics under a new identity; duplicate *bytes* are blocked by `tx_attempt_signings.tx_hash` UNIQUE.
- `tx_attempts_state_facts_check` deliberately does not include `ineffective`/`sent`/`unknown` facts; those live in Tables 3–5.

## Table 2 — `tx_attempt_signings` (signed bytes + hash; the pre-send durable fact)

```sql
CREATE TABLE tx_attempt_signings (
    attempt_id      TEXT        NOT NULL,
    signature       TEXT        NOT NULL,
    signed_tx_bytes BYTEA       NOT NULL,
    tx_hash         TEXT        NOT NULL,
    signed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_attempt_signings_pkey PRIMARY KEY (attempt_id),
    CONSTRAINT tx_attempt_signings_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_attempt_signings_tx_hash_uniq UNIQUE (tx_hash),
    CONSTRAINT tx_attempt_signings_signature_check CHECK (signature ~ '^0x[0-9a-f]{130}$'),
    CONSTRAINT tx_attempt_signings_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_attempt_signings_bytes_check CHECK (octet_length(signed_tx_bytes) > 0)
);
```

The row is written in **T2** and is the durable authorization to send: every dispatch reads `signed_tx_bytes` from here (never re-derives from content at send time), and replay is byte-identical by construction. App-level invariant asserted before insert: `keccak256(signed_tx_bytes) == tx_hash == 009.tx_hash` and the recovered sender equals `tx_attempts.sender`; a mismatch refuses (`signature_mismatch`) and writes nothing.

## Table 3 — `tx_send_attempts` (one row per dispatch action; gate snapshot + outcome)

```sql
CREATE TABLE tx_send_attempts (
    send_id                  BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id               TEXT        NOT NULL,
    send_seq                 INTEGER     NOT NULL,
    kind                     TEXT        NOT NULL,
    outcome                  TEXT        NOT NULL,
    rpc_class                TEXT        NOT NULL DEFAULT '',
    observed_recovery_version BIGINT     NOT NULL,
    observed_pause           TEXT        NOT NULL DEFAULT 'none',
    observed_claim_version   BIGINT,
    observed_claim_expiry    TIMESTAMPTZ,
    observed_authorization_id TEXT,
    observed_authorization_version BIGINT,
    observed_authorization_state TEXT   NOT NULL DEFAULT '',
    observed_expires_at      TIMESTAMPTZ,
    observed_now             TIMESTAMPTZ NOT NULL,
    observed_binding_state   TEXT        NOT NULL DEFAULT '',
    dispatched_at            TIMESTAMPTZ,
    recorded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_send_attempts_pkey PRIMARY KEY (send_id),
    CONSTRAINT tx_send_attempts_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_send_attempts_attempt_seq_uniq UNIQUE (attempt_id, send_seq),
    CONSTRAINT tx_send_attempts_seq_check CHECK (send_seq > 0),
    CONSTRAINT tx_send_attempts_kind_check CHECK (kind IN ('initial', 'replay')),
    CONSTRAINT tx_send_attempts_outcome_check CHECK (outcome IN ('accepted', 'rejected', 'unknown')),
    CONSTRAINT tx_send_attempts_dispatched_check
        CHECK (outcome = 'unknown' OR dispatched_at IS NOT NULL)
);
CREATE INDEX tx_send_attempts_attempt_idx ON tx_send_attempts (attempt_id, send_seq);
```

- `outcome` is the **send-level evidence** (R-010-05): `accepted` = node returned the same hash (or `already known` for identical bytes); `rejected` = allowlisted deterministic non-acceptance; `unknown` = timeout/transport/rate-limit/invalid response/hash mismatch/unrecognized error/region write failure after dispatch began.
- `rpc_class` vocabulary (open text, documented, never a business verdict): `already_known`, `nonce_too_low`, `replacement_underpriced`, `insufficient_funds`, `intrinsic_gas_too_low`, `timeout`, `transport`, `rate_limited`, `invalid_response`, `hash_mismatch`, `storage_region_failure`, `unrecognized`.
- `send_seq` is allocated under the attempt row `FOR UPDATE` in T3; `tx_send_attempts_dispatched_check` allows `unknown` rows without `dispatched_at` only for the region-write-failure case (the row itself only exists if the region committed; if it did not commit, no row exists at all — G-010-2).
- Gate-snapshot columns are evidence of the basis under which the region decided; they are never read back as authorization.

## Table 4 — `tx_reconciliations` (append-only chain observations; unknown resolution evidence)

```sql
CREATE TABLE tx_reconciliations (
    reconcile_id   BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id     TEXT        NOT NULL,
    tx_hash        TEXT        NOT NULL,
    classification TEXT        NOT NULL,
    block_number   BIGINT,
    block_hash     TEXT,
    rpc_class      TEXT        NOT NULL DEFAULT '',
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_reconciliations_pkey PRIMARY KEY (reconcile_id),
    CONSTRAINT tx_reconciliations_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_reconciliations_classification_check CHECK (classification IN (
        'found_pending', 'included', 'not_found_yet', 'unavailable')),
    CONSTRAINT tx_reconciliations_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_reconciliations_block_hash_check
        CHECK (block_hash IS NULL OR block_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_reconciliations_block_pair_check
        CHECK ((block_number IS NULL) = (block_hash IS NULL)),
    CONSTRAINT tx_reconciliations_included_block_check
        CHECK (classification <> 'included' OR block_number IS NOT NULL)
);
CREATE INDEX tx_reconciliations_attempt_idx ON tx_reconciliations (attempt_id, observed_at);
```

Rows are appended, never edited. `not_found_yet` is the honest "no evidence yet" class and is never promoted to a failure verdict (G-010-5). A reconcile observation is the mandatory prerequisite for any dispatch from `signed` (crash-ambiguous) or `unknown` states (R-010-06).

## Table 5 — `tx_receipts` (receipt + expected-Transfer verdict + confirmation basis)

```sql
CREATE TABLE tx_receipts (
    receipt_id        BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id        TEXT        NOT NULL,
    tx_hash           TEXT        NOT NULL,
    status            INTEGER     NOT NULL,
    block_number      BIGINT      NOT NULL,
    block_hash        TEXT        NOT NULL,
    effect            TEXT        NOT NULL,
    transfer_detail   TEXT        NOT NULL DEFAULT '',
    canonicality      TEXT        NOT NULL DEFAULT 'unverified',
    confirmations     NUMERIC(78,0) NOT NULL DEFAULT 0,
    confirm_threshold BIGINT      NOT NULL,
    confirm_policy_seq BIGINT     NOT NULL,
    confirm_tip_number BIGINT,
    confirm_tip_hash   TEXT,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at      TIMESTAMPTZ,
    orphaned_at       TIMESTAMPTZ,
    CONSTRAINT tx_receipts_pkey PRIMARY KEY (receipt_id),
    CONSTRAINT tx_receipts_attempt_fkey FOREIGN KEY (attempt_id)
        REFERENCES tx_attempts (attempt_id),
    CONSTRAINT tx_receipts_tx_block_uniq UNIQUE (tx_hash, block_hash),
    CONSTRAINT tx_receipts_status_check CHECK (status IN (0, 1)),
    CONSTRAINT tx_receipts_effect_check CHECK (effect IN (
        'effective', 'ineffective_status',
        'ineffective_transfer_missing', 'ineffective_transfer_mismatch')),
    CONSTRAINT tx_receipts_effect_status_check
        CHECK ((effect = 'ineffective_status') = (status = 0)
               AND (effect <> 'effective' OR status = 1)),
    CONSTRAINT tx_receipts_canonicality_check CHECK (canonicality IN (
        'unverified', 'canonical', 'orphaned')),
    CONSTRAINT tx_receipts_orphan_facts_check CHECK (
        (canonicality <> 'orphaned' OR orphaned_at IS NOT NULL)
        AND (canonicality <> 'canonical' OR orphaned_at IS NULL)),
    CONSTRAINT tx_receipts_block_number_check CHECK (block_number >= 0),
    CONSTRAINT tx_receipts_block_hash_check CHECK (block_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT tx_receipts_confirmations_check
        CHECK (confirmations >= 0 AND confirmations = floor(confirmations)),
    CONSTRAINT tx_receipts_threshold_check CHECK (confirm_threshold > 0),
    CONSTRAINT tx_receipts_policy_seq_check CHECK (confirm_policy_seq > 0),
    CONSTRAINT tx_receipts_confirmed_at_check
        CHECK (confirmed_at IS NULL OR canonicality <> 'unverified')
);
CREATE INDEX tx_receipts_attempt_idx ON tx_receipts (attempt_id, observed_at);
CREATE INDEX tx_receipts_open_idx ON tx_receipts (canonicality, block_number)
    WHERE canonicality <> 'orphaned';
```

- `effect` is FR-08's verdict: `effective` requires `status = 1` **and** a matching expected Transfer; every other case is one of the `ineffective_*` classes with the observed difference in `transfer_detail`.
- `canonicality` starts `unverified` and moves to `canonical` only when `(block_number, block_hash)` matches the indexer's canonical `chain_blocks` view; `orphaned` is terminal for that row (re-inclusion creates a new row with the new block hash).
- Confirmation basis columns mirror 005's explicit/configurable policy (read-only `MAX(confirmation_policy_history.policy_seq)`); `confirm_tip_*` records the canonical tip used to compute `confirmations`.

## Table 6 — `tx_attempt_events` (append-only decision/revision log)

```sql
CREATE TABLE tx_attempt_events (
    event_id       BIGINT      GENERATED ALWAYS AS IDENTITY,
    attempt_id     TEXT        NOT NULL,
    event_seq      BIGINT      NOT NULL,
    event          TEXT        NOT NULL,
    reason_class   TEXT        NOT NULL DEFAULT '',
    recovery_version BIGINT,
    detail         TEXT        NOT NULL DEFAULT '',
    at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tx_attempt_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT tx_attempt_events_attempt_seq_uniq UNIQUE (attempt_id, event_seq),
    CONSTRAINT tx_attempt_events_seq_check CHECK (event_seq > 0),
    CONSTRAINT tx_attempt_events_event_check CHECK (event IN (
        'created', 'replayed', 'attempt_conflict',
        'signature_persisted', 'signature_refused', 'signature_mismatch',
        'gate_refused', 'send_rejected', 'send_unknown',
        'reconcile_observed', 'receipt_verified', 'receipt_ineffective',
        'confirmed', 'orphaned', 'reconfirmed', 'replaced', 'unknown_cleared'))
);
CREATE INDEX tx_attempt_events_attempt_idx ON tx_attempt_events (attempt_id, event_seq);
```

`attempt_id` carries **no FK** (007/009 audit precedent): a refusal that never produced a row must still be attributable, and rows outlive nothing. `event_seq` is allocated under the attempt row lock in the same transaction as any state change; `recovery_version`, when present, is the 006 version observed at revision time (FR-09's "按当前恢复版本修订"). Refusals store the observed basis in `detail` (pause cause, claim version mismatch, grant state, fee-cap difference, …). The log is never updated or deleted.

---

## State machines

### Attempt state (`tx_attempts.state`)

| From | To | Trigger | Transaction |
|---|---|---|---|
| — | `prepared` | identity + content persisted | T1 |
| `prepared` | `signed` | 009 result received, bytes + hash landed | T2 |
| `prepared` | `prepared` | 009 refusal (`signature_withheld` / gate / binding / authorization) | event only |
| `signed` | `sent` | dispatch `accepted` | T3 |
| `signed` | `unknown` | dispatch `rejected`/`unknown` | T3 |
| `signed` | `effective`/`ineffective` | reconcile finds an included verified receipt (crash-ambiguous dispatch) | T4 |
| `sent` | `effective`/`ineffective` | receipt verified (status + Transfer) and canonical | T4 |
| `sent` | `unknown` | reconcile observation: no receipt yet (`not_found_yet`/`found_pending`) — FR-03 missing-receipt = unknown | T4 |
| `unknown` | `sent` | reconcile: `found_pending` (present in mempool again) | T4 |
| `unknown` | `effective`/`ineffective` | reconcile: `included` + receipt verified | T4 |
| `effective` | `confirmed` | confirmations ≥ threshold | T4 |
| `effective`/`confirmed` | `orphaned` | canonicality check fails on reorg (revision event carries recovery version) | T4 |
| `orphaned` | `effective`/`confirmed` | receipt re-included in canonical chain and verified | T4 |
| `orphaned` | `unknown` | no canonical receipt and tx may be re-included or dropped | T4 |
| any non-terminal | `replaced` | sibling attempt on the same binding reached `effective`/`confirmed` | T4 |
| `ineffective`/`replaced` | — | terminal (no send path) | — |

**Sendability matrix** (every send runs the full send region regardless):

| State | Allowed send action | Precondition |
|---|---|---|
| `prepared` | none (first sign) | T2 must run first |
| `signed` | `initial` broadcast | reconcile observation recorded first (crash-ambiguous region, R-010-06) |
| `sent` | `replay` | current gates; existing durable `accepted` evidence |
| `unknown` | `replay` (or a replacement attempt) | reconcile observation recorded first |
| `effective`/`ineffective`/`confirmed`/`orphaned`/`replaced` | none | non-sendable; `orphaned` returns to `unknown` via revision when re-send is legitimate |

### Receipt canonicality (`tx_receipts.canonicality`)

`unverified` → `canonical` (indexer canonical view agrees) → `orphaned` (view disagrees / block gone). `orphaned` is terminal for the row; re-inclusion is a new row. Confirmation progress is only updated while `canonical`.

### Send outcome (`tx_send_attempts.outcome`)

`accepted` / `rejected` / `unknown` — set once at region commit, never updated. A later region adds a new row (`send_seq + 1`), preserving both the known result and the history (Q3: known results are never rewritten).

---

## SQL transaction catalog

| Tx | Name | Steps (in order) |
|---|---|---|
| T1 | attempt persist | BEGIN → `statement_timeout='5s'` → validate shape (Go) → INSERT `tx_attempts` → 23505 → replay/conflict classify → COMMIT |
| T2 | signing persist | [009 HTTP call, same identity retry-safe] → reconstruct bytes + verify hash/sender (Go) → BEGIN → `statement_timeout='5s'` → INSERT `tx_attempt_signings` → guarded UPDATE attempt `prepared→signed` + event (event_seq under attempt lock) → COMMIT |
| T3 | send region | BEGIN → `statement_timeout='5s'` → `lock_timeout='5s'` → claim `FOR SHARE` → coordination row `FOR UPDATE` → gate tables `LOCK … IN SHARE MODE` → 006 one-statement gate read (pause/recovery/version equality) → 008 scope row `FOR SHARE` → 008 binding/holds/registry read → 007 grant + PB scope `FOR SHARE` (equality, expiry on DB clock, replacement reuse/fee bounds) → attempt `FOR UPDATE` + sendability + `send_seq` → refusal: event + COMMIT (zero dispatch) **or** dispatch (`eth_sendRawTransaction`, bounded) → INSERT `tx_send_attempts` + guarded attempt state UPDATE + event → COMMIT |
| T4 | reconcile/receipt/revision | [chain probes by `tx_hash`, receipt read] → BEGIN → `statement_timeout='5s'` → attempt `FOR UPDATE` → INSERT `tx_reconciliations` (always) → on included+canonical: INSERT `tx_receipts` (or guarded UPDATE confirmation progress) + guarded attempt UPDATE + event → COMMIT |

No external call inside T1/T4; T3 contains exactly one bounded dispatch (R-010-04). Every guarded UPDATE carries `WHERE attempt_id = $1 AND revision_seq = $2` (or an expected state) and treats `RowsAffected() <> 1` as a stale-conflict rollback.

### Crash-point matrix (T2/T3 boundaries)

| Crash point | Durable after restart | Recovery action |
|---|---|---|
| before T1 COMMIT | nothing | retry creates the attempt |
| after T1, before/around 009 | `prepared` attempt | retry re-calls 009 with the same identity; 009 has no chain side effect (D9) |
| after 009 result, before T2 COMMIT | `prepared` attempt; 009 may hold a persisted result | retry re-calls 009 → identical signature/hash; persist bytes |
| after T2, before send region | `signed` attempt + bytes | reconcile probe first; `not_found_yet` → dispatch under gates |
| inside region, before dispatch | rollback (no evidence) | indistinguishable from the next row; probe-first on the next operation |
| after dispatch began, before COMMIT | rollback (G-010-2) | probe-first; never infer "not sent" |
| after COMMIT, before response | full durable state | caller retry converges (replay under gates or state read) |

---

## Idempotency carriers and 23505 classification

| Carrier | Named constraint | Meaning on 23505 |
|---|---|---|
| `tx_attempts` PK | `tx_attempts_pkey` | same attempt identity → replay path (envelope equality → converge; different → `attempt_conflict`) |
| attempt ↔ signing identity | `tx_attempts_signing_request_uniq` | another attempt claims this signing request identity → `attempt_conflict` |
| signed bytes | `tx_attempt_signings_tx_hash_uniq` | identical signed bytes already exist → `hash_conflict` refusal (never re-sign/re-send via a second attempt) |
| send order | `tx_send_attempts_attempt_seq_uniq` | concurrent region lost the attempt row lock → retry reads current `send_seq` |
| event order | `tx_attempt_events_attempt_seq_uniq` | concurrent event append lost the lock → retry with the current `event_seq` |
| receipt identity | `tx_receipts_tx_block_uniq` | repeat observation of the same receipt converges (update progress, not duplicate) |

Classification is by `pgErr.ConstraintName` only (repo convention). All other writes are append-only or version-guarded.

---

## Read-only consumption map (010 writes none of these)

| Source | Read | Mode |
|---|---|---|
| `indexer_pause`/`log_pause`/`deposit_pause`/`reorg_recovery`/`reorg_recovery_events` | gate flags + current version (shape of `captureRecoveryVersion`) | gate-table `SHARE` + one SELECT |
| `chain_blocks` | canonical head, receipt canonicality | plain reads |
| `withdrawal_authorizations` | grant state/expiry/fields | `FOR SHARE` |
| `withdrawal_authorization_scopes` | intent/sender/fee triple/purpose/version | `FOR SHARE` |
| `nonce_bindings` | binding facts (intent/chain/sender/nonce/state) | plain read under coordination lock |
| `nonce_scope_state` | 008 pause/release serialization | `FOR SHARE` |
| `nonce_scope_holds` | active holds (any → refuse) | plain read |
| `nonce_wallet_registry` | registry state | plain read |
| `execution_claims` (011, frozen J2 shape) | claim active/version/expiry/revocation | `FOR SHARE`; absent table/row → fail closed |
| `confirmation_policy_history` | threshold + `policy_seq` (MAX) | plain read |
| 009 | submit + status HTTP only | no DB access |

---

## Migration notes

- `000011_tx_lifecycle.sql` (provisional): `+goose Up` creates Tables 1–6 in FK-dependency order (`tx_attempts` first; `tx_attempt_signings`/`tx_send_attempts`/`tx_reconciliations`/`tx_receipts`/`tx_attempt_events` after), with named indexes; `+goose Down` drops in reverse order. Pure DDL — no stored functions, no seeds, no data mutation of upstream tables.
- Migration number and the absence/presence of 011 artifacts are re-verified at merge (PLAN-1); already-applied migrations are never rewritten.
- No FK to `payment_intents`/`execution_claims` on this branch (G-010-3); intended closure at the joint merge, absorbing 011's real column names in the claim adapter (J2 mapping).

## Projection revision semantics (Q1)

`tx_attempts.revision_seq` increments by exactly 1 on every guarded mutation; `tx_attempt_events.event_seq` is the ordered evidence stream. 011's display projection stores `(attempt_id, source_revision, updated_at)` and applies an incoming state only when `incoming_revision > stored_revision`; when freshness cannot be confirmed it must mark the projection possibly stale. The projection is never a permission: every execution-class decision reads and verifies the current authoritative facts through the send region and the unknown recovery interface (FR-13).

