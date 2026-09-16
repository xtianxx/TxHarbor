# PB Research: 007 Authorization Carrier Supplement

**Feature**: `012-007-authorization-carrier` | **Date**: 2026-09-16
**Sources**: PB spec + PB-C1/PB-C2 (same dir); 009 worktree @ `8f75450`
(R11, `contracts/gates.md`, tasks PB-01–PB-05); this worktree code
(`internal/withdrawal/grant.go`, `internal/app/withdrawalauthz.go`,
`internal/db/migrate.go`, `migrations/`); 008 merged @ `02641fb` (read-only).

## R-PB1 — Supply entry as-built (evidence, not redesign)

- `SupplyGrant`/`RevokeGrant` own their BEGIN..COMMIT
  (`internal/withdrawal/grant.go:374-424`, `:427-471`); sole lock = grant row
  `FOR UPDATE`; audit append-only; `SET LOCAL statement_timeout='5s'` per
  statement. Recovery via audit-row re-read by `operation_id`
  (`resolveByOperationID`, `:492-507`).
- Trust root today = DB DSN possession (`internal/app/withdrawalauthz.go:29-32`);
  `--operator` is a verbatim audit claim, optional, never compared
  (`withdrawalauthz.go:110,170`; `grant.go:168-183`). No `Authenticate` /
  permission check on supply/revoke; those exist only on HTTP intake
  (`internal/withdrawal/auth.go:127`, `intake.go:212-219`).
- **Decision**: PB-C1 is implemented as a *pre-pool validation gate* in the
  supply path (same phase as `validateSupplyOpInput`, which is already
  nil-pool-safe and unit-tested): resolve the caller's authenticated identity,
  check it against the issuance role mapping, refuse before any DB access.
  Reuse the existing `Authenticate` mechanism shape from intake; do NOT invent
  a parallel credential system. `--operator` stays audit-only (PB-C1).

## R-PB2 — Issuer role mapping (PB-C1 mechanization)

- Business decision fixed: single operations role, no dual control, no
  per-grant cryptography (Q-A). What plan fixes: *where* the mapping lives
  (deployment config, not code constants), *where* the check runs (CLI carrier
  before `SupplyGrant`, so the library stays transport-free like today), and
  the verifiable evidence (negative test: unauthenticated/unmapped principal →
  refusal with zero rows; positive test: mapped principal → supply green with
  audit agreement grant↔audit↔principal).
- Minimum-control rule: if the existing deployment cannot express the mapping,
  add the smallest config + check; never treat "can reach the DB" as permission.

## R-PB3 — Carrier table shape

- `withdrawal_authorization_scopes`, 1:1, PK `authorization_id`,
  FK → `withdrawal_authorizations(authorization_id)` (matches 009 R11 §carrier).
- Columns: `intent_id`, `request_id` (007), `sender` (lowercase 0x),
  fee triple per PB-C2, `allows_fee_replacement` (explicit purpose boolean),
  `authorization_version` (BIGINT, monotonic per grant, start 1),
  `attested_by` (issuing principal, audit join). No signature column (Q-A).
- Amount precedent: 007 `amount NUMERIC(78,0)`; fee values fit uint64, so
  `BIGINT CHECK >= 0` suffices — exact DDL to implement phase, rule fixed here.

## R-PB4 — Fee dimensions (PB-C2 mechanization)

- Enforced triple per authorization: (1) single-tx max total network fee;
  (2) per-gas-unit cap; (3) EIP-1559 priority-fee cap, with
  `priority ≤ max_fee` cross-check. Totals computed as
  `gas_limit × max_fee_per_gas` (EIP-1559) or `gas_limit × gas_price`
  (legacy); no new transaction types. Native-coin最小单位 integers;
  missing/illegal/over-limit → refuse. The service's stricter policy still
  applies conjunctively (009 policy.go stays authoritative for its own caps).
- Representation (one row with three BIGINT columns vs normalized range
  table) is implement-phase DDL detail; the *rule text above* is frozen.

## R-PB5 — Migration mechanics & numbering

- Runner: goose v3 Provider + Postgres session locker
  (`internal/db/migrate.go:260-278`), embed FS (`migrations/embed.go`),
  `NNNNNN_name.sql` discovery (`migrate.go:65-93`), `CheckCompatibility`
  auto-advances to highest embedded version. History freeze test guards
  000001–000006 (verify exact coverage in implement; 007/008 landed via
  normal merges).
- Current highest: `000008`. 009 lane owns `000009`. PB planning value
  `000010` with **renumber-at-merge** (`max(merged)+1`); only unapplied
  numbers may move; applied numbers immutable (009 PB-01 rule, adopted).
- Upgrade sequences required (PB-05): empty→full chain, 007-era→carrier,
  down→re-up. All on scratch DBs, never production.

## R-PB6 — 009 consumption gap & ownership (read-only, 009 @ `8f75450`)

- 009 reads exactly one row (`caller_id, chain_id, asset, recipient, amount,
  state, expires_at`) `FOR SHARE` (`internal/signer/gates.go:51-52` @009).
  Verified: identity/caller/chain/asset/recipient/amount/expiry/revocation.
  Missing: sender scope, fee scope, purpose, intent linkage, version
  (fingerprint surrogate only). Fail-closed chokepoint is hardcoded
  `GrantScope{Present:false}` (009 `submit.go:211`); delivery never re-checks
  scope; `replacement_of` is DDL-only (dead in prod).
- Plug-in (009 lane, NOT this batch): extend read shape, implement scope
  checks, persist `authorization_version`, re-check at delivery.
  **This batch owns carrier + supply only.**

## R-PB7 — Adjacent lanes (no interaction by design)

- 008 merged (`02641fb`): read-api scope-row order and reconcile are 008-owned;
  PB touches neither. Shared surfaces (Serve listener, metrics registry) follow
  the proven additive pattern; no 008 change in this plan.
- 011 is a downstream consumer of the carrier (intent linkage); no 011 work here.
