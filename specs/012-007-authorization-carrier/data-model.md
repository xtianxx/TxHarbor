# PB Data Model: Authorization Scope Carrier

**Feature**: `012-007-authorization-carrier` | **Date**: 2026-09-16
Rules frozen here; DDL text to implement phase.

## Table: `withdrawal_authorization_scopes` (new, additive)

1:1 with `withdrawal_authorizations`; PK `authorization_id`;
FK → `withdrawal_authorizations(authorization_id)` (enforces no orphan scope;
grant may exist without scope = pre-extension stock).

| Column | Domain (rule) |
|---|---|
| `authorization_id` | TEXT PK/FK, equals grant id |
| `intent_id` | TEXT, operator-declared intent identity (011 binds it later) |
| `request_id` | TEXT, originating 007 request |
| `sender` | TEXT, lowercase 0x + 40 hex (EIP-55 pass, stored lowercase) |
| `fee_max_total` | BIGINT ≥ 0, single-tx max total network fee, native最小单位 |
| `fee_max_per_gas` | BIGINT ≥ 0, per-gas-unit cap |
| `fee_max_priority` | BIGINT ≥ 0, EIP-1559 priority cap; MUST satisfy `≤ fee_max_per_gas` (CHECK) |
| `allows_fee_replacement` | BOOLEAN, explicit purpose token (default false) |
| `authorization_version` | BIGINT ≥ 1, monotonic per grant (bumped on revoke-then-re-supply cycles that keep scope; re-issuance mints a NEW grant id, never rewrites) |
| `attested_by` | TEXT, issuing principal (joins issuance audit); server-resolved, joins op-input equality (different principal + same operation id → `operation_conflict`) |

No signature column (Q-A). No FK from audit tables into scope (audit stays
append-only and FK-free, matching Table 6 precedent).

## Transaction catalog

- **T-supply+scope**: `BEGIN → statement guard → api_key FOR SHARE +
  caller FOR SHARE + PermitIssue re-evaluation → grant FOR UPDATE →
  grant/scope/audit writes → COMMIT` (authority re-checks precede all writes;
  R-PB10). Refusal (`supply_refused` + named reason) writes nothing.
- **T-revoke-sync**: existing revoke tx + scope state/version sync in the same
  tx (scope follows grant state; version bump rule per table above).
- **T-reissue**: NEW grant id + NEW scope row in one supply tx, with an audit
  detail link to the old grant/request ids; MUST NOT UPDATE old rows, MUST NOT
  create intent/nonce (enforced by simply not touching those tables), MUST NOT
  rebind old signing requests (009-side invariant, out of scope here).
- **Commit-unknown**: existing `resolveByOperationID` pattern extended to
  include the scope row in the re-read (same operation id, same convergence).

## Lock order (extends grant.go: api_key/caller shares first, then the existing order)

`BEGIN → statement guard → api_key FOR SHARE → caller FOR SHARE →
grant FOR UPDATE → grant write → scope write → audit append → COMMIT`.
A key/caller UPDATE writer either lands before our SHARE (observed) or waits
for our COMMIT (R-PB10). Readers: 009 grant `FOR SHARE` + scope read in the
same sequence (009 lane). Intake `FOR SHARE` on grant unaffected. No
grant↔scope↔api_key lock cycle possible (single writer order; readers
share-lock; key/caller rows are never written by supply).

## Concurrency argument

- First supply: PK-insert convergence (existing `resolveGrantPKRace` pattern;
  scope insert rides the same winner tx).
- Equal resupply: guarded `IS DISTINCT FROM` extended to scope fields; any
  difference → `operation_conflict`, zero writes.
- Audit `operation_id` UNIQUE stays the commit-unknown convergence key.

## Write/read + lock matrix (PB scope only; 009 rows are 009-lane duties)

| Path | Reads | Writes | Locks held |
|---|---|---|---|
| T-supply+scope | grant row; issuance mapping (config, no lock) | grant INSERT/UPDATE; scope INSERT; audit INSERT | grant `FOR UPDATE` (existing #1); fresh scope insert (no lock); audit uniq index only |
| T-revoke-sync | grant row | grant state UPDATE; scope state/version sync; audit INSERT | same as above |
| T-reissue | old grant/request (read-only, for detail link) | NEW grant + NEW scope + audit (never UPDATE old) | new-row locks only |
| 009 submit (later lane) | grant + scope `FOR SHARE` in one sequence; version persisted | 009-owned request rows only | existing 009 order; scope read shares the sequence, no new lock object |
| 009 delivery (later lane) | grant + scope re-read `FOR SHARE`; version equality vs persisted | 009-owned admission rows only | same; version mismatch blocks |

- Shared coordination basis: writer and 009 readers meet on the **same grant
  row** (`FOR UPDATE` vs `FOR SHARE`) plus the **persisted
  `authorization_version`** that delivery re-checks — a scope/version change
  between submit and delivery is observed, never silently adopted. No advisory,
  lease, or new lock object is introduced (constraint, not just intent: the tx
  catalog above lists every lock; anything else is a plan violation).
- 009 follow-up duties (not this batch): extend read shape, implement scope
  checks, persist + re-check version, thread `replacement_of` (see R-PB6
  plug-in points).
