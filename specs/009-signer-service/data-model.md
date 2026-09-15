# Data Model: 009 Signer Service（签名隔离服务）

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md) (OC-1–OC-7 resolved) | **Research**: [research.md](research.md) (R1–R10)

Storage additions live in one migration, `migrations/000009_signer_service.sql` (pure DDL,
numbered goose sequence; conventions follow 004–007: named constraints, lowercase 0x hex,
`NUMERIC(78,0)` integer money, `TIMESTAMPTZ DEFAULT now()`, append-only audit).

009 owns six tables (Tables 1–6 below). It does **not** own, copy, or write 006/007 tables; it
reads them read-only through the contracts in [contracts/gates.md](contracts/gates.md) (006
`indexer_pause`/`log_pause`/`deposit_pause`, 006 `reorg_recovery` + `reorg_recovery_events`,
007 `withdrawal_authorizations`), acquiring the gate-table `LOCK … IN SHARE MODE` coordination
lock described in research R6 (a lock acquisition, not a data write). Reading is consumer-side; this model adds no column to any
upstream table and MUST NOT be interpreted as extending them.

Cross-cutting rules: no floating-point anywhere (all monetary/fee/nonce quantities are
`NUMERIC(78,0)` or BIGINT bounded by CHECK; Go side is `big.Int`/`uint256`/`uint64`, per
constitution I); no key material, credentials, or raw signed-transaction bytes in any table or
log line (FR-22; the only signing artifact ever persisted is the signature value + its hash in
Table 4, which is storage, not logging); append-only tables (Tables 5–6) are never updated or
deleted by any code path.

## Table 1 — `signer_caller` (stable caller identity; FR-04/FR-05)

```sql
CREATE TABLE signer_caller (
    caller_id  BIGINT      NOT NULL,
    label      TEXT        NOT NULL DEFAULT '',
    can_sign   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT signer_caller_pkey PRIMARY KEY (caller_id),
    CONSTRAINT signer_caller_caller_id_check CHECK (caller_id > 0)
);
```

- Operator-assigned identity (no identity column); survives credential rotation; rows are never
  deleted (audit FKs stay valid). `can_sign = FALSE` means the caller authenticates but MUST be
  refused (`403 signing_not_permitted`) — identity is not permission (FR-05; mirror of 007's
  `can_create`).
- Created via the `signer-auth` operator subcommand only (research R9); no HTTP write surface.

## Table 2 — `signer_credential` (bearer credential; many per caller over time)

```sql
CREATE TABLE signer_credential (
    credential_id BIGINT      GENERATED ALWAYS AS IDENTITY,
    caller_id     BIGINT      NOT NULL,
    secret_hash   CHAR(64)    NOT NULL,
    secret_prefix TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at    TIMESTAMPTZ,
    CONSTRAINT signer_credential_pkey PRIMARY KEY (credential_id),
    CONSTRAINT signer_credential_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES signer_caller (caller_id),
    CONSTRAINT signer_credential_secret_hash_uniq UNIQUE (secret_hash),
    CONSTRAINT signer_credential_secret_hash_check CHECK (secret_hash ~ '^[0-9a-f]{64}$')
);
```

- `secret_hash = sha256(presented secret)` lowercase hex; full UNIQUE so a revoked hash can never
  be re-registered. Auth lookup: one indexed row by hash + constant-time compare; `revoked_at IS
  NULL` for active. `secret_prefix` is display/audit only, never auth input. Plaintext is shown
  once at issuance and never stored (R9/007 R1–R5 precedent).
- Rotation = insert successor, set predecessor `revoked_at`; revocation = set `revoked_at`.
  A revoked credential fails the next request's startpoint check (no cache).

## Table 3 — `signing_requests` (request identity ↔ full content binding; FR-13/FR-15)

```sql
CREATE TABLE signing_requests (
    id                          BIGINT        GENERATED ALWAYS AS IDENTITY,
    caller_id                   BIGINT        NOT NULL,
    signing_request_id          TEXT          NOT NULL,
    attempt_id                  TEXT          NOT NULL,
    replacement_of              BIGINT,
    intent_id                   TEXT          NOT NULL,
    binding_ref                 TEXT          NOT NULL,
    recovery_version            BIGINT        NOT NULL DEFAULT 0,
    chain_id                    BIGINT        NOT NULL,
    sender                      TEXT          NOT NULL,
    nonce                       NUMERIC(78,0) NOT NULL,
    tx_type                     INTEGER       NOT NULL,
    to_addr                     TEXT          NOT NULL,
    value                       NUMERIC(78,0) NOT NULL,
    data                        BYTEA         NOT NULL,
    gas_limit                   NUMERIC(78,0) NOT NULL,
    gas_price                   NUMERIC(78,0),
    max_fee_per_gas             NUMERIC(78,0),
    max_priority_fee_per_gas    NUMERIC(78,0),
    access_list                 JSONB         NOT NULL DEFAULT '[]'::jsonb,
    asset                       TEXT          NOT NULL,
    recipient                   TEXT          NOT NULL,
    amount                      NUMERIC(78,0) NOT NULL,
    canonical_envelope          TEXT          NOT NULL,
    content_hash                TEXT          NOT NULL,
    authorization_id            TEXT          NOT NULL,
    authorization_fingerprint   TEXT          NOT NULL,
    authorization_state         TEXT          NOT NULL,
    policy_version              TEXT          NOT NULL,
    state                       TEXT          NOT NULL DEFAULT 'received',
    refusal_class               TEXT          NOT NULL DEFAULT '',
    created_at                  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT signing_requests_pkey PRIMARY KEY (id),
    CONSTRAINT signing_requests_caller_id_fkey FOREIGN KEY (caller_id)
        REFERENCES signer_caller (caller_id),
    CONSTRAINT signing_requests_caller_request_uniq UNIQUE (caller_id, signing_request_id),
    CONSTRAINT signing_requests_attempt_uniq UNIQUE (attempt_id),
    CONSTRAINT signing_requests_replacement_fkey FOREIGN KEY (replacement_of)
        REFERENCES signing_requests (id),
    CONSTRAINT signing_requests_state_check
        CHECK (state IN ('received', 'validated', 'signed', 'rejected', 'failed')),
    CONSTRAINT signing_requests_chain_id_check CHECK (chain_id > 0),
    CONSTRAINT signing_requests_recovery_version_check CHECK (recovery_version >= 0),
    CONSTRAINT signing_requests_sender_check CHECK (sender ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_to_check CHECK (to_addr ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_asset_check CHECK (asset ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_recipient_check CHECK (recipient ~ '^0x[0-9a-f]{40}$'),
    CONSTRAINT signing_requests_nonce_check
        CHECK (nonce >= 0 AND nonce <= 18446744073709551615),
    CONSTRAINT signing_requests_value_check
        CHECK (value >= 0 AND value <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT signing_requests_amount_check
        CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
    CONSTRAINT signing_requests_gas_limit_check CHECK (gas_limit > 0),
    CONSTRAINT signing_requests_fee_shape_check CHECK (
        (tx_type = 0 AND gas_price IS NOT NULL
                     AND max_fee_per_gas IS NULL AND max_priority_fee_per_gas IS NULL)
        OR
        (tx_type = 2 AND gas_price IS NULL
                     AND max_fee_per_gas IS NOT NULL AND max_priority_fee_per_gas IS NOT NULL
                     AND max_priority_fee_per_gas <= max_fee_per_gas)
    ),
    CONSTRAINT signing_requests_access_list_check CHECK (jsonb_array_length(access_list) = 0),
    CONSTRAINT signing_requests_content_hash_check CHECK (content_hash ~ '^0x[0-9a-f]{64}$'),
    CONSTRAINT signing_requests_authorization_fingerprint_check
        CHECK (authorization_fingerprint ~ '^[0-9a-f]{64}$')
);

-- OC-5 conditional grant reuse (research R7): one anchor (non-replacement) request per
-- authorization; replacement rows may share the grant. A persisted row's authorization_id is
-- never updated, so an old request can never be rebound to another grant.
CREATE UNIQUE INDEX signing_requests_authorization_anchor_uniq
    ON signing_requests (authorization_id) WHERE replacement_of IS NULL;

Field notes:

- **Identity columns**: `(caller_id, signing_request_id)` is the request identity (OC-4);
  `attempt_id` is the 010 attempt identity (UNIQUE — one attempt = one request identity).
  `replacement_of` is NULL for a non-replacement anchor and points at the predecessor for a
  fee-replacement request (OC-4/OC-5; new identity, same intent/binding). A persisted row's
  `authorization_id` is never updated, so no old request is ever rebound (research R7).
  `intent_id` / `binding_ref` are upstream references (OC-1 / OC-3): the service verifies binding
  consistency through its read contract (research R8) and records what it verified.
- **Fee shape**: one of two closed shapes enforced by CHECK (legacy `gas_price`, or EIP-1559
  `max_fee_per_gas` + `max_priority_fee_per_gas` with tip ≤ max fee). `tx_type` is constrained by
  the fee-shape CHECK to `{0, 2}` only.
- **`access_list`**: present so the content set is complete; v1 policy refuses non-empty access
  lists, and the CHECK makes that storage-visible (`[]` only). Widening it is a policy + migration
  change, never a silent acceptance.
- **Declared transfer triple** (`asset`, `recipient`, `amount`): the request's explicit
  declaration of the ERC-20 transfer semantics; validation requires `asset = to_addr`, empty
  native `value` in v1, and calldata `transfer(address,uint256)` args equal to
  `recipient`/`amount` (FR-08/FR-09/FR-10) — the same triple the 007 authorization binds
  (Table 4 of 007), enabling field equality.
- **Canonical envelope** (`canonical_envelope`) and `content_hash = keccak256(envelope)`: the
  storage-side binding identity of research R3. The signing digest is always produced by
  go-ethereum `types.SignTx` from the reconstructed transaction; the envelope is the exact byte
  artifact compared on retry/conflict (never re-derived from columns).
- **Authorization binding**: `authorization_id` + `authorization_fingerprint`
  (`authz:v1` over observed grant fields) + `authorization_state` (observed `state` at read
  time). Reuse of one grant across requests is **conditional** (OC-5, research R7): a
  replacement may share the anchor's grant only when the authorization explicitly permits the
  fee-replacement purpose and the fee is in scope (branch currently unreachable because the 007
  carrier cannot express it — research R11); otherwise a fresh `authorization_id` is required.
  The fingerprint is a read-time **surrogate** for the authorization version the 007 carrier
  cannot express and MUST NOT be presented as a version (research R7/R11, gap D-2).
- **Policy version**: `policy_version` = versioned SHA-256 of the effective signing policy
  (allowed chains, sender registry, asset/function allowlist, recipient rules, amount cap, fee
  caps, mode) — every request/audit row carries the version that decided it (FR-12).
- **Refusal classes**: `refusal_class` holds the machine class (e.g. `recovery_paused`,
  `recovery_version_changed`, `authorization_expired`, `binding_conflict`, `policy_feecap`,
  `request_conflict`) when `state = 'rejected'`; empty otherwise.

State machine (FR-15, guarded transitions only; effects in the transactions below):

```text
received ──▶ validated ──▶ signed        (terminal success; Table 4 result exists)
   │              │
   │              ├──▶ rejected           (terminal refusal; replay returns the refusal)
   │              └──▶ failed             (bounded transient failure; no result persisted)
   └──────────────▶ rejected              (input-shape refusal recorded without signing path)

retry of failed / in-flight received-or-validated with the same identity and envelope:
    re-enters gate+sign path; `signed` is never re-entered — the persisted result is returned.
```

## Table 4 — `signature_results` (the persisted signing result; FR-13/FR-14/FR-16)

```sql
CREATE TABLE signature_results (
    signing_request_row BIGINT      NOT NULL,
    signature           TEXT        NOT NULL,
    tx_hash             TEXT        NOT NULL,
    signed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT signature_results_pkey PRIMARY KEY (signing_request_row),
    CONSTRAINT signature_results_request_fkey FOREIGN KEY (signing_request_row)
        REFERENCES signing_requests (id),
    CONSTRAINT signature_results_tx_hash_uniq UNIQUE (tx_hash),
    CONSTRAINT signature_results_signature_check CHECK (signature ~ '^0x[0-9a-f]{130}$'),
    CONSTRAINT signature_results_tx_hash_check CHECK (tx_hash ~ '^0x[0-9a-f]{64}$')
);
```

- One result per request identity (PK = request row). **Persisted inside the signing transaction,
  before any byte of the response is written** (FR-13); a crash before `COMMIT` leaves no result
  and no observable signature (research R4).
- `UNIQUE (tx_hash)` is the storage-level "no second observable signature" guard: the same signed
  object can never appear as two results, independent of application memory.
- The raw signed-transaction bytes are **never persisted and never logged**; the signature value
  is the only broadcast-reconstructable material in storage, readable only through the delivery
  admission path (Table 6 / contracts/api.md). Status-only responses omit it (OC-6).

## Table 5 — `signing_request_audit` (append-only decision/refusal log; FR-12/FR-22/FR-24)

```sql
CREATE TABLE signing_request_audit (
    audit_id          BIGINT      GENERATED ALWAYS AS IDENTITY,
    signing_request_id TEXT       NOT NULL,
    caller_id         BIGINT      NOT NULL,
    action            TEXT        NOT NULL,
    reason_class      TEXT        NOT NULL DEFAULT '',
    detail            TEXT        NOT NULL DEFAULT '',
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT signing_request_audit_pkey PRIMARY KEY (audit_id),
    CONSTRAINT signing_request_audit_action_check CHECK (action IN (
        'received', 'validated', 'signed', 'rejected', 'failed', 'conflict', 'replayed',
        'gate_refused', 'binding_refused', 'authorization_refused',
        'delivery_admitted', 'delivery_blocked', 'delivery_unknown'))
);
```

- `signing_request_id` carries **no FK**: audit must attribute refusals that never produced a
  request row (pre-persist auth/shape refusals use the caller-presented id, or a `rej-…` marker
  when unknown) and must outlive nothing (007 Table 5 rationale).
- `detail` is a redacted, operator-readable snapshot (observed pause basis, recovery phase/seq,
  authorization id + observed state, policy version, binding class, retry attempt) — **never**
  credentials, keys, signature bytes, or raw signed-transaction bytes.
- Written in the same transaction as the state transition it records on the signing path; for
  pre-transaction refusals a best-effort single-statement append is allowed (mirrors 007).

## Table 6 — `delivery_admissions` (per-delivery gate snapshot + verdict; FR-17/FR-23, OC-6/OC-7)

```sql
CREATE TABLE delivery_admissions (
    admission_id              BIGINT      GENERATED ALWAYS AS IDENTITY,
    signing_request_row       BIGINT      NOT NULL,
    attempt_seq               INTEGER     NOT NULL,
    verdict                   TEXT        NOT NULL,
    authorization_id          TEXT        NOT NULL,
    authorization_fingerprint TEXT        NOT NULL,
    authorization_state       TEXT        NOT NULL,
    binding_class             TEXT        NOT NULL DEFAULT '',
    can_sign                  BOOLEAN     NOT NULL DEFAULT TRUE,
    recovery_version          BIGINT      NOT NULL,
    pause_basis               TEXT        NOT NULL DEFAULT 'none',
    recovery_basis            TEXT        NOT NULL DEFAULT 'none',
    reason                    TEXT        NOT NULL DEFAULT '',
    decided_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at              TIMESTAMPTZ,
    CONSTRAINT delivery_admissions_pkey PRIMARY KEY (admission_id),
    CONSTRAINT delivery_admissions_request_fkey FOREIGN KEY (signing_request_row)
        REFERENCES signing_requests (id),
    CONSTRAINT delivery_admissions_request_attempt_uniq UNIQUE (signing_request_row, attempt_seq),
    CONSTRAINT delivery_admissions_attempt_seq_check CHECK (attempt_seq > 0),
    CONSTRAINT delivery_admissions_binding_class_check CHECK (binding_class IN (
        '', 'matches', 'absent', 'conflict', 'paused', 'terminal', 'read_failed')),
    CONSTRAINT delivery_admissions_verdict_check CHECK (verdict IN (
        'admitted', 'delivered', 'blocked', 'unknown_reconcile'))
);
```

- One row per delivery attempt (attempt_seq 1 = first response, >1 = same-identity retries). The
  transaction first takes the gate-table `SHARE` lock (research R6), so the snapshot and verdict
  are linearly ordered against 006/007 writers; the `admitted`/`blocked` verdict commits
  **before** response bytes are written, and the post-write marker flips `admitted → delivered`
  (best-effort). `binding_class` and `can_sign` record the observed 008 binding class and caller
  permission at decision time.
- **Bounded admission validity (research R6)**: an `admitted` row authorizes only the immediate
  response write of the same attempt — it is not a durable permit. A delayed send (retry,
  restart, scheduling gap) MUST re-run T-deliver and record a new `attempt_seq`; a row left at
  `admitted` means "cleared at that snapshot, write outcome unknown", not a standing permission
  (OC-7). Only bytes already written are in-flight approved and not reclaimable.
- `blocked` records the refusal and its basis (`pause_basis` names the observed pause table(s),
  `recovery_basis` the recovery phase/seq, `reason` the class). No signature material is
  delivered and none appears in the response.
- `unknown_reconcile` is recorded when the service cannot determine whether a prior delivery
  occurred (e.g. an admission COMMIT whose outcome was unknown and no row is visible on retry):
  the attempt does not deliver, is reported status-only with reconcile semantics, and MUST NOT be
  re-signed or silently retried as a new identity.
- Multi-pause semantics: every observed pause is recorded; release of one pause never bypasses
  others because each attempt re-reads all sources (no cached verdict).

## Transaction catalog (behavioral; SQL shapes live in contracts/)

| ID | Transaction | Statements (fixed order) | Owner file (planned) |
|----|-------------|--------------------------|----------------------|
| T-submit-first | first receipt of an identity | BEGIN → statement-timeout guard → gate-table `LOCK … IN SHARE MODE` (R6) → plain INSERT request row (`received`) → `SELECT … FOR UPDATE` own row → 006 gate read (one read sequence) → 008 binding read → 007 grant `FOR SHARE` + validity/equality → policy validation → `KeyProvider.SignTx` → INSERT `signature_results` → UPDATE state `signed` (or `rejected`; transient refusal keeps non-terminal state) → audit INSERT → COMMIT | `internal/signer/submit.go` |
| T-submit-replay | duplicate identity arrives | INSERT → catch 23505 exact `signing_requests_caller_request_uniq` → ROLLBACK → read row → envelope equal → delivery path (T-deliver) with persisted result; envelope differ → 409 `request_conflict` (+ audit best-effort); row exists, no result yet → `503`-shape "outcome not yet visible; retry same identity" | `internal/signer/submit.go` |
| T-deliver | first response or same-identity retry | BEGIN → statement-timeout guard → gate-table `LOCK … IN SHARE MODE` (R6) → 006 gate re-read (one statement) + version equality → 008 binding re-read → 007 grant `FOR SHARE` + fingerprint/version equality → `signer_caller.can_sign` `FOR SHARE` → INSERT `delivery_admissions` (`admitted`/`blocked`; records `binding_class`/`can_sign`) → COMMIT → write response → best-effort UPDATE to `delivered` | `internal/signer/delivery.go` |
| T-status | authenticated status read | single read: request row by `(caller_id, signing_request_id)` + latest admission row; other callers/nonexistent → identical 404 | `internal/signer/status.go` |
| T-auth | per-request authentication | read `signer_credential` by `secret_hash` (+ caller row); constant-time compare; revoked/absent → generic 401 | `internal/signer/auth.go` |

Discipline: T-submit-* and T-deliver are the only 009 transactions that touch upstream tables;
against them 009 issues only `SELECT` and the gate-table `LOCK … IN SHARE MODE` (lock acquisition
with no data mutation — research R6), never `INSERT`/`UPDATE`/`DELETE`. No RPC, no external calls
inside any transaction. `writeGuard`-style per-statement timeout (5s literal, same as the repo's
existing guard) bounds every transaction and the `LOCK TABLE` wait.

## Concurrency & crash argument (why no second observable signature)

- **Concurrent same identity**: the first `INSERT` of the identity wins the unique index; the
  loser's INSERT blocks on the index entry until the winner commits or rolls back. Winner commits
  → loser gets 23505 → converges on the persisted row/result. Winner rolls back → loser's INSERT
  succeeds and proceeds. Exactly one row, at most one result.
- **Concurrent different anchors, same 007 grant**: the partial anchor index
  `signing_requests_authorization_anchor_uniq` serializes them; the loser maps to
  `403 authorization_invalid`. A replacement row may share the anchor's grant only under OC-5's
  conditional rule and its matching `replacement_of`/`intent_id` (research R7).
- **Crash before COMMIT**: no row state change, no result; retry with the same identity re-runs
  the path; deterministic RFC-6979 bytes mean the eventual single persisted result matches what a
  hypothetical pre-crash sign produced.
- **Crash after COMMIT before response**: the result is durable; same-identity retry goes through
  T-deliver and either re-delivers the same bytes (gate pass) or returns status-only (gate fail).
  The caller's uncertainty is answered by the persisted row — never by a fresh signature.
- **Storage unavailable**: no COMMIT → no success response; a persisted result remains readable
  when storage returns (FR-23, SC-08).
- **Key provider unavailable/timeout**: bounded failure before any persistence of a result;
  recorded as `failed` with a retryable class; MUST NOT be recorded or reported as signed.

## Upstream read contracts (consumer side)

| Upstream | Read | Contract |
|----------|------|----------|
| 006 | `indexer_pause` / `log_pause` / `deposit_pause` existence; `reorg_recovery` active row (any phase) + version (active `recovery_seq`, else events MAX, else 0) | [contracts/gates.md](contracts/gates.md) §1 — gate-table `LOCK … IN SHARE MODE` (R6) + one read sequence, read-only, never cleared/released |
| 007 | `withdrawal_authorizations` by `authorization_id` `FOR SHARE` + state/expiry/field equality + fingerprint/version | [contracts/gates.md](contracts/gates.md) §2 — grant-axis lock in the R6 lock order; consumption never writes |
| 008 | nonce binding for `intent_id`/`attempt_id` with five-class result | [contracts/gates.md](contracts/gates.md) §3 — interface-level contract; adapter waits for 008 (D3) |

## Retention & secrecy notes

- Tables 3–6 are append-only evidence: `signing_requests` rows are never deleted, statuses only
  transition per the state machine; audit/admission rows are never updated or deleted (the one
  allowed post-insert update is the `admitted → delivered` marker + `delivered_at`).
- `signature_results.signature` is the only sensitive stored value; it is excluded from logs,
  metrics, errors, and status responses (only gated delivery returns it or, after a recorded
  delivery, its `tx_hash` — contracts/api.md §3).
- Credentials: only sha256 hashes are stored; plaintext exists only at issuance output. Key
  material never enters any table in this model (it lives only inside the `KeyProvider`).

