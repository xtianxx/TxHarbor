# Data Model: 007-withdrawal-creation

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15 | **Migration**: `migrations/000007_withdrawal_creation.sql`
(goose, new file; 000001–000006 untouched in meaning. `embed.go` auto-includes `*.sql`.)

007 is a receive-only intake writer on top of 002–006 truths: it never redefines upstream
semantics, never allocates nonces, never signs, never broadcasts. All decisions in research.md
(R1–R9: R1–R5 key lifecycle, R6–R8 intake, R9 grant supply). Conventions follow 004/005/006: lowercase `0x`-prefixed hex for addresses
(`~ '^0x[0-9a-f]{40}$'`), `NUMERIC` integer amounts, named UNIQUE constraints (pgx-mappable),
append-only audit, `TIMESTAMPTZ DEFAULT now()`.

Canonical input normalization (applied once, before compare AND before persist, so stored rows
are already canonical): amount must already match `[1-9][0-9]*` (FR-06 rejects anything else —
no normalization step exists); addresses lowercased after EIP-55 validation (FR-07); key used
verbatim (FR-09: no trim/convert). Stored rows therefore compare by equality.

## Table 1 — `caller` (stable business identity; credentials come and go, this row persists)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| caller_id | BIGINT | PK, `CHECK (> 0)` | stable identity; never derived from a key; survives rotation |
| label | TEXT | `NOT NULL DEFAULT ''` | operator display name; no secrecy |
| can_create | BOOLEAN | `NOT NULL DEFAULT TRUE` | fixed interface permission (FR-03b); FALSE = 403 on create, query unaffected |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | permission changes bump this; audited via Table 4 |

No `chain_id`: single deployment single chain (FR-04); identity is deployment-wide.
`withdrawals.caller_id` FK → this table (R3). Rows are never deleted (FR-11 spirit).

## Table 2 — `api_key` (credential; many rows per caller over time)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| key_id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | credential identity; never business identity |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | rotation keeps this constant (R3) |
| key_hash | CHAR(64) | `NOT NULL CHECK ~ '^[0-9a-f]{64}$'`, `UNIQUE` full (R1) | `sha256(key)` hex; full unique ⇒ revoked hash never re-registered |
| key_prefix | TEXT | `NOT NULL` | display/audit only (e.g. `txh_…abcd`); never auth input (R2) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| revoked_at | TIMESTAMPTZ | NULL = active | immediate revoke = `now()`; rotation grace = future ts (R4) |

Auth lookup (single statement, R4):
`SELECT key_id, caller_id FROM api_key WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())`.
Key format: `txh_` + base64url(32 CSPRNG bytes); presented via `Authorization: Bearer` (FR-03).

## Table 3 — `withdrawal_requests` (the Accepted request; receive-only)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | internal surrogate; never exposed as the idempotency signal |
| request_id | TEXT | `NOT NULL UNIQUE` | opaque public id returned to caller (201/200) |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | from auth (FR-03), never client-claimed |
| idempotency_key | TEXT | `NOT NULL CHECK (length BETWEEN 1 AND 128 AND key ~ '^[\x21-\x7e]+$')` | opaque, case-sensitive, verbatim (FR-09) |
| authorization_id | TEXT | `NOT NULL` | upstream per-attempt grant id (FR-03b) |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | must equal deployment chain (FR-04) |
| asset | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | canonical lowercase; whitelist-checked pre-insert (FR-05) |
| recipient | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | canonical lowercase (FR-07) |
| amount | NUMERIC(78,0) | `NOT NULL CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)` (literal = 2²⁵⁶−1; §四 DB upper bound) | uint256 integer; transport form `[1-9][0-9]*` (FR-06); never float |
| status | TEXT | `NOT NULL DEFAULT 'accepted' CHECK (= 'accepted')` | 007 writes no other status; Accepted = received, not executed (FR-08) |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |
| CONSTRAINT `withdrawal_requests_caller_key_uniq` | `UNIQUE (caller_id, idempotency_key)` | replay-or-409 carrier (R6) | |
| CONSTRAINT `withdrawal_requests_authorization_uniq` | `UNIQUE (authorization_id)` | one-auth→one-request carrier (R7) | |

Naming rule (F3/F4): every constraint consumed by `ConstraintName` classification MUST carry an
explicit `CONSTRAINT <name>` in `migrations/000007_withdrawal_creation.sql` — never rely on
PostgreSQL auto-naming. Short forms (`caller_key_uniq`, `authorization_uniq`, `operation_id_uniq`)
are prose shorthands only; the protocol in research R8 and the supply pseudocode below use exact
names. T006 verifies the real `ConstraintName` values against these declarations.

`NUMERIC(78,0)` holds the full uint256 range (78 decimal digits) as an integer; CHECK `> 0`
rejects zero at the storage layer as well. Layered amount enforcement (§四, all three — capacity alone is not validation):
(1) transport-shape reject in `validate.go` — must match `[1-9][0-9]*`, which already
excludes zero, leading zeros, signs, decimals, exponents, blanks (FR-06); decimals therefore
die at parse, never as values: `NUMERIC(78,0)` is NOT relied on to reject fraction input,
because driver conversion could round before the CHECK ever sees it — the shape regex is the
decimal barrier; (2) semantic range check in Go — value ≤ 2²⁵⁶−1 via `math/big` before INSERT
(FR-06; over-length digit strings die here, never reaching the DB); (3) storage CHECK
`amount >= 1 AND amount <= 2²⁵⁶−1` (literal in the Table 3 row above) as depth defense against
any future second writer that bypasses `validate.go`. Positive verification both directions
(future tests): max-uint256 accepted end-to-end; max-uint256+1 rejected at layer (2) with zero
rows; `1.5`/`1e3` rejected at layer (1) with zero rows. No nonce/signature/broadcast columns exist by design
(FR-19); 008+ add their own tables keyed off `request_id` (Downstream Handoff, no FK from 007 side).

## Table 4 — `withdrawal_authorizations` (upstream grant supply; 007 reads, never approves)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| authorization_id | TEXT | PK | upstream-issued grant id (FR-03b) |
| caller_id | BIGINT | `NOT NULL`, FK → `caller(caller_id)` | grant bound to one caller |
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | |
| asset | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | |
| recipient | TEXT | `NOT NULL CHECK ~ '^0x[0-9a-f]{40}$'` | |
| amount | NUMERIC(78,0) | `NOT NULL CHECK (amount >= 1 AND amount <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)` (literal = 2²⁵⁶−1; same §四 bound) | |
| state | TEXT | `NOT NULL CHECK IN ('active','revoked','expired')` | revocation/expiry live here |
| expires_at | TIMESTAMPTZ | NULL = no expiry | |
| supplied_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | audit: when upstream supplied it |
| supplied_by | TEXT | `NOT NULL DEFAULT ''` | audit: controlled-supply identity (R5-adjacent) |

Supply path (locked 2026-09-15定向收尾 — R9, see research.md): the `txharbor withdrawal-authz`
command (same controlled-script carrier as T024 `depositauth.go:1-32`: repository-owned,
version-controlled parameterized SQL in one explicit BEGIN..COMMIT over the DB operator's
connection; NOT a stored function; migrations stay pure DDL). Intake locks the grant row
`FOR SHARE` and validates in three statements (research R8): lock → `SELECT clock_timestamp()`
→ validity (`state='active' AND (expires_at IS NULL OR expires_at > t_check)`, strict `>`)
plus full param equality; mismatch/inactive/expired → 403, zero persistence. Revoke takes
`FOR UPDATE`/UPDATE on the same row (three-case interleave proof in research R7).

## Table 5 — `withdrawal_request_audit` (append-only receipt log)

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| audit_id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | |
| request_id | TEXT | `NOT NULL` (no FK — audit outlives nothing but constrains nothing) | mirrors 004 pause-audit precedent |
| caller_id | BIGINT | `NOT NULL` | |
| action | TEXT | `NOT NULL CHECK IN ('created','replayed','conflict','rejected','auth_failed','unavailable')` | receipt vocabulary |
| detail | TEXT | `NOT NULL DEFAULT ''` | redacted key-value fragments (no secrets; `logx.Redact` funnel) |
| recorded_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | |

Written in the same tx as the request insert (R8); replay/conflict/reject paths append their
own rows. Never updated or deleted (FR-11).

Pre-tx reject audit writer (C3 — no fictitious identity): rows are written ONLY when a
`caller_id` is known. Authenticated pre-tx rejections (422 validation, 403 permission/grant
fast-path, 400 malformed with valid key, 409 fast-path conflict) → the HTTP handler issues ONE
best-effort single-statement `INSERT` into this table in its own tx AFTER sending the response
decision (never blocks the response; failure → structured log + metric, never 503). The
`request_id` column carries the returned-or-would-be `request_id` (`wr-…` for replays/conflicts
of an existing row; a `rej-…` opaque marker for never-created rejects — never a fabricated
`withdrawal_requests` identity). Unauthenticated rejections (401, no verifiable identity) write
NO Table 5 row — there is no caller to attribute; they are covered by structured logs
(`logx.Redact`, no key material) + `unauthenticated_total` metric. In-tx rejections (validity
fails after BEGIN, T-auth-bound, T-unavailable-attempt) ride the receipt tx or its rollback
branch per the catalog. T010 owns the writer; T026 asserts the 401-no-row rule.

## Table 6 — `withdrawal_grant_audit` (append-only grant-supply log; R9 carrier)

Shape mirrors `deposit_pause_audit` (`migrations/000004_deposit_detection.sql:109-122`):
chain-agnostic here (single deployment), keyed by grant id + action.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| audit_id | BIGINT | PK (`GENERATED ALWAYS AS IDENTITY`) | sole identity; every attempt gets its own row |
| operation_id | TEXT | `NOT NULL` (UNIQUE via `CONSTRAINT withdrawal_grant_audit_operation_id_uniq` row below) | stable operation identity, caller-supplied per attempt (see below); the ONLY dedup key |
| authorization_id | TEXT | `NOT NULL` (no FK — audit must survive and must key unbound grants; a grant row may never exist for a refused supply) | §三: unbound-grant audit keys here, not Table 5 |
| CONSTRAINT `withdrawal_grant_audit_operation_id_uniq` | `UNIQUE (operation_id)` | attempt-dedup carrier (Table 6 23505 classify key) | |
| caller_id | BIGINT | `NOT NULL` | grant's caller (or attempted caller on refused supply) |
| action | TEXT | `NOT NULL CHECK IN ('supplied','resupplied','supply_refused','revoked','revoke_nop')` | supply vocabulary |
| operator | TEXT | `NOT NULL DEFAULT ''` | declared supply identity (audit claim; trust root is DSN possession, R9) |
| reason | TEXT | `NOT NULL DEFAULT ''` | |
| detail | TEXT | `NOT NULL DEFAULT ''` | redacted params snapshot (no secrets) |
| recorded_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | wall-clock evidence only; never a dedup input |

Attempt semantics (locked 六轮定点 — one attempt = one row; `operation_id` binds the FULL
normalized op-input; the 三轮 `grant_state_seq` key and its narrative stay withdrawn):
- `operation_id` binds the op-input (action, authorization_id, caller_id, chain_id, asset,
  recipient, amount, expires_at): the audit row persists all eight (action/authorization_id/
  caller_id columns + `detail` canonical params snapshot). NOTE on naming: prior revisions called
  this "七元组" while listing eight fields — corrected here to **op-input (8 fields)**; no semantic
  change, only the miscount is fixed. Any read by O MUST compare the presented op-input against
  the stored op-input BEFORE reporting: all equal → return the recorded outcome (whatever the
  action, INCLUDING `supply_refused` — a refusal is a recorded outcome, never re-executed, never
  upgraded to success); ANY differ → `operation_conflict`, zero business writes, original audit
  row untouched, original outcome NOT impersonated. O alone never proves success.
- operator/reason classification (locked 六轮定点): `operator` + `reason` are RETRY METADATA, not
  bound op-input. Same-O retry with different operator/reason text MUST NOT create a new row and
  MUST NOT silently rewrite the recorded row — the retry returns the recorded outcome; if the
  caller needs the new text preserved it MUST mint a new O (a new attempt by definition).
- O source (locked 六轮定点 — explicit-only): `supply`/`revoke` REQUIRE `--operation-id`;
  no in-command auto-mint, no echo-fallback path (both DELETED — print ≠ saved, and echo-loss
  left an unrecoverable O, contradicting "attempt never existed"). Minting is a SEPARATE step:
  `withdrawal-authz mint` prints one opaque id (e.g. UUID) and the caller durably captures it
  BEFORE invoking supply/revoke. Rule: no reusable O ⇒ no DB side effects, full stop. A crash
  before capture means the operator never had an O, so no attempt exists and nothing needs
  recovery; a crash after capture ⇒ retry with the captured O (same op-input).
- Business idempotency and audit attempts are stated separately: grant-table convergence
  (`supplied` once; re-supply-equal returns success) is a property of the Table 4 row, NOT of
  audit rows. Two different异参 supplies A then B record TWO `supply_refused` rows (different
  `operation_id`, different `detail`) — the counter-example is closed by construction.
- `supplied` (new `operation_id`): grant INSERT + audit INSERT in one tx, both `RowsAffected()==1`.
- `resupplied` (new `operation_id` per attempt): grant verified equal (`RowsAffected()==0` asserted)
  + audit INSERT. Concurrent duplicate re-supplies record one row each — attempts, not changes.
- `supply_refused` (new `operation_id` per attempt): zero grant mutation + audit INSERT commit
  together (refusal is the committed outcome, not a rollback). Attempted params live in `detail`;
  the grant table carries nothing. 23505 on `withdrawal_grant_audit_operation_id_uniq` (same attempt retried concurrently) →
  rollback → re-read by O → compare op-input → equal: report recorded outcome WITHOUT a second row
  (the only audit convergence, and it converges to the *same attempt*); differ: `operation_conflict`.
- `revoked` (new `operation_id`): active → revoked + audit, both `==1`.
- `revoke_nop` (new `operation_id` per attempt): each repeat records its own row; idempotent success.
- 23505 handling (audit tx aborted — MUST rollback before any further statement, same R8 rule):
  on `withdrawal_grant_audit_operation_id_uniq` conflict → rollback → re-read by O → compare-then-report per above; on any
  other error (incl. deadlock `40P01`, lock timeout) → rollback → typed retryable, never partial.
  Grant-row state alone NEVER proves an audit row committed and NEVER proves THIS attempt
  succeeded — especially: grant pre-exists + this attempt异参-refused ⇒ report refusal, never
  business success. Grant state is reported separately as current-state info, never conflated
  with this attempt's outcome.
- Uncertain COMMIT (unified recovery — "双缺→新O" rule DELETED): ALWAYS retry with the SAME O
  and the SAME op-input. O-miss does NOT mean rolled back or finished (the tx may still be open) —
  re-issuing the same O lets the tx + UNIQUE converge. Read failure or still-indeterminate ⇒
  report unknown/retryable WITH the same O for the next retry; NEVER mint a new O for the same
  operation. Outcome basis: the audit row matching O + op-input; grant state is current-state info only.

Verifiable outcomes: supply → `supplied` + grant row; equal re-supply → `resupplied`,
grant `RowsAffected()==0`; A-then-B异参 → TWO `supply_refused` rows with distinct details;
same-O异参 → `operation_conflict`, zero writes, original row/outcome intact (incl. refusal
stays refusal); revoke → `revoked`; repeat revoke → distinct `revoke_nop` rows; uncertain
COMMIT → same-O retry only (O-miss ⇒ unknown/retryable with same O, never new O).

## Transaction catalog (behavioral; SQL in contracts/)

- **T-accept** (first receipt): pre-tx classify (fast path) → BEGIN → writeGuard
  (`SET LOCAL statement_timeout='5s'`, per-statement timeout only — NOT a recovery gate,
  NOT a tx total; 007 persists during active recovery per FR-16) → `FOR SHARE` grant row →
  `clock_timestamp()` → validate (403 + ROLLBACK on fail) → plain INSERT request →
  INSERT audit → COMMIT; 23505 → rollback → fixed-order classify → 200/409/403;
  commit-error → re-classify.
- **T-replay**: classify hit + FR-10 equality → return original + `replayed` audit (no new request).
- **T-conflict**: classify hit + any business-param/auth-id inequality → 409 + `conflict` audit,
  original untouched.
- **T-auth-bound**: 23505 on `withdrawal_requests_authorization_uniq` → fixed-order classify (R8 FINAL: key first,
  then auth) → miss-on-key + hit-on-auth → 403 + `auth_failed` audit, zero new rows.
- **T-reject**: auth/param failure pre- or in-tx → typed 400/401/403/422 + `rejected` audit,
  zero persistence.
- **T-unavailable**: storage failure → 503 + `unavailable` audit attempt (best-effort; MUST NOT
  return Accepted).
- **T-dual-race**: both UNIQUEs violated at once → single reported `ConstraintName` is NOT
  semantic; fixed-order classify decides (key-hit equality → 200; key-hit inequality → 409;
  else auth-hit → 403; else retryable). Never derive the response from report order.

## Supply pseudocode (locked 六轮定点 — entry → commit → 23505 classify → uncertain recovery)

```
O = require(--operation-id)                 # explicit-only (mint step precedes; R9 rule);
                                            # no O ⇒ no DB side effects, full stop
OPIN = (action, G, caller, chain, asset, recipient, amount, expires_at)  # op-input, 8 fields
# Constraint-name protocol: match EXACT pg names (never prose shorthands):
#   withdrawal_requests_caller_key_uniq / withdrawal_requests_authorization_uniq /
#   withdrawal_grant_audit_operation_id_uniq / withdrawal_authorizations_pkey.
BEGIN
SET LOCAL statement_timeout = '5s'
grant = SELECT * FROM withdrawal_authorizations WHERE authorization_id = $G FOR UPDATE
        # MISS ⇒ no row locked: concurrent first-supplies serialize ONLY on the PK below
if grant.missing and action = supply:
    INSERT INTO withdrawal_authorizations (G, ...OPIN...)      # first supply
    INSERT INTO withdrawal_grant_audit (O, G, ...OPIN, 'supplied', ...)
elif grant.present and equal(OPIN, grant_OPIN) and action = supply:
    assert RowsAffected(grant_check) == 0
    INSERT INTO withdrawal_grant_audit (O, G, ...OPIN, 'resupplied', ...)
elif action = revoke and grant.state = 'active':
    UPDATE withdrawal_authorizations SET state='revoked' WHERE authorization_id=$G
    INSERT INTO withdrawal_grant_audit (O, G, ...OPIN, 'revoked', ...)
elif action = revoke and grant.state != 'active':
    INSERT INTO withdrawal_grant_audit (O, G, ...OPIN, 'revoke_nop', ...)
else:  # grant missing (non-supply), or OPIN != grant_OPIN
    INSERT INTO withdrawal_grant_audit (O, G, ...OPIN-attempted, 'supply_refused', ...)
COMMIT
on 23505(constraint = withdrawal_grant_audit_operation_id_uniq):
    ROLLBACK                                 # aborted tx: NO further statement in it
    row = SELECT * FROM withdrawal_grant_audit WHERE operation_id = $O
    if row.missing: return retryable(same O, same OPIN)   # tx may still be open; never new O
    if equal(OPIN, row.opin): return row.outcome          # incl. refusal — never upgraded
    else: return operation_conflict, zero writes
on 23505(constraint = withdrawal_authorizations_pkey):   # FIRST-SUPPLY RACE branch (六轮定点):
    ROLLBACK                                 # concurrent first-supply won; loser restarts BOUNDED:
    row0 = SELECT * FROM withdrawal_grant_audit WHERE operation_id = $O   # N1: O first —
    if row0.present and equal(OPIN, row0.opin): return row0.outcome       # already recorded
    if row0.present and differ: return operation_conflict, zero writes    # (e.g. twin recorded)
    grant2 = SELECT * FROM withdrawal_authorizations WHERE authorization_id = $G  # no lock, read-only
    if equal(OPIN, grant2_OPIN) and action = supply:
        # winner's params == mine: my attempt becomes a resupply — SAME O, SAME OPIN, one retry
        INSERT INTO withdrawal_grant_audit (O, G, ...OPIN, 'resupplied', ...)  # own single-statement tx
        # 23505(withdrawal_grant_audit_operation_id_uniq) here ⇒ my twin already recorded ⇒ re-read by O ⇒ report it
    else:
        INSERT INTO withdrawal_grant_audit (O, G, ...OPIN-attempted, 'supply_refused', ...)
    # retry budget: at most ONE re-execution per call (bounded — never a loop); a second 23505 or
    # error ⇒ retryable(same O, same OPIN) for the OUTER caller, which retries the whole call.
    # Budget exhaustion reports retryable — it does NOT promise immediate final success per call.
    # NEVER map the grant-PK conflict itself to operation_conflict (not an O-binding mismatch).
    # Contention-known-outcome is not storage-unavailable; genuine storage failure during recovery
    # still reports per the established 503/retryable semantics (never swallowed).
on 23505(other) / 40P01 / timeout / conn-error:
    ROLLBACK; return retryable(same O, same OPIN)
on COMMIT-unknown:
    row = SELECT * FROM withdrawal_grant_audit WHERE operation_id = $O
    if row.present and equal(OPIN, row.opin): return row.outcome
    if row.present and differ: return operation_conflict
    # row missing: grant state is CURRENT-STATE INFO ONLY — never this attempt's proof
    return unknown_retryable(same O, same OPIN)            # "双缺→新O" DELETED
```

Fixed verification scenarios (§四): (1) same-O same-OPIN concurrent ⇒ one audit row, all
report its outcome; (2) same-O different-G-or-params ⇒ `operation_conflict`, zero writes,
original intact; (3) original tx still open + double-miss ⇒ same-O retry (converges or stays
unknown, never new O); (4) grant pre-exists + this-attempt-refused ⇒ refusal (never business
success, even though a grant row exists); (5) crash after O-capture before COMMIT ⇒ recovery
reuses the SAME O (durable capture per R9); crash before capture ⇒ the attempt never existed,
a fresh mint is a NEW attempt by definition.
First-supply concurrency (§二): (i) same-O same-OPIN twins ⇒ ONE grant row + ONE `supplied`
audit row (loser restarts into resupply with the SAME O); (ii) different-O same-OPIN ⇒ ONE grant
row + one audit row PER O (`supplied` + `resupplied`); (iii) different-O different-OPIN ⇒ ONE
grant row (winner's params) + winner `supplied` + loser `supply_refused` (loser's params in
`detail`, grant untouched).

## Concurrency argument (why two writers cannot both win; R7 FINAL)

Both uniqueness carriers are single index inserts — atomic and serializing by construction.
Concurrent same-key writers: one INSERT commits, losers get 23505 on `withdrawal_requests_caller_key_uniq` and
classify to the winner (R6). Concurrent same-auth different-key writers: one commits, losers
get 23505 on `withdrawal_requests_authorization_uniq` → fixed-order classify → 403 (R7). Auth-validity itself is
proven by the `FOR SHARE` grant-row lock + validity SELECT in the receipt tx (three-case
interleave proof in research R7 — the earlier "shared snapshot" wording is withdrawn);
per-request key/permission checks prove startpoint freshness (no whole-request immediacy
claim); post-rollback classify proves loss/uncertain recovery. One lock object total on the
intake path. Restart safety follows: all guarantees are
durable rows; a retry after crash/response-loss replays against the same constraints (FR-13).
