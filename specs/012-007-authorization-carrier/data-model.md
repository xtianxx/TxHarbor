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
| `attested_by` | TEXT, issuing principal (joins issuance audit) |

No signature column (Q-A). No FK from audit tables into scope (audit stays
append-only and FK-free, matching Table 6 precedent).

## Transaction catalog

- **T-supply+scope**: `BEGIN → SET LOCAL statement_timeout='5s' → grant row
  FOR UPDATE → INSERT/UPDATE grant → INSERT scope row (same tx)
  → INSERT grant audit → COMMIT`. One tx, existing lock #1 first; scope row
  adds no new lock object (fresh insert). Refusal paths write nothing.
- **T-revoke-sync**: existing revoke tx + scope state/version sync in the same
  tx (scope follows grant state; version bump rule per table above).
- **T-reissue**: NEW grant id + NEW scope row in one supply tx, with an audit
  detail link to the old grant/request ids; MUST NOT UPDATE old rows, MUST NOT
  create intent/nonce (enforced by simply not touching those tables), MUST NOT
  rebind old signing requests (009-side invariant, out of scope here).
- **Commit-unknown**: existing `resolveByOperationID` pattern extended to
  include the scope row in the re-read (same operation id, same convergence).

## Lock order (extends grant.go, no reordering)

`BEGIN → statement guard → grant FOR UPDATE → grant write → scope write →
audit append → COMMIT`. Readers: 009 grant `FOR SHARE` + scope read in the
same sequence (009 lane). Intake `FOR SHARE` on grant unaffected. No
grant↔scope lock cycle possible (single writer order; readers share-lock).

## Concurrency argument

- First supply: PK-insert convergence (existing `resolveGrantPKRace` pattern;
  scope insert rides the same winner tx).
- Equal resupply: guarded `IS DISTINCT FROM` extended to scope fields; any
  difference → `operation_conflict`, zero writes.
- Audit `operation_id` UNIQUE stays the commit-unknown convergence key.

## 009 read contract (for the 009 lane; stated here so both sides match)

009 reads grant + scope in one `FOR SHARE` sequence, requires
`authorization_id` equality, checks sender / fee triple / purpose /
intent+request linkage against the request, persists `authorization_version`,
re-checks version at delivery. Scopeless grant → `authorization_unverifiable`
(009-side, already built).
