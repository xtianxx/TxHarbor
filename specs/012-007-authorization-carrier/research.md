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

## R-PB8 — Merge/deploy order and gap-fill (verified against runner code)

- Determination (not deferred): **PB merges first as `000010`** (gap at 9
  reserved for the 009 lane); **009 merges later as `000009` unchanged** —
  no renumbering is needed on either side, and incomplete 009 is never merged
  early for numbering reasons. Feature numbers (012) and migration numbers
  (000010) are unrelated namespaces.
- Gap-fill proof (`internal/db/migrate.go` + goose v3.28.0 `provider.go`):
  `MigrationFiles` requires only `NNNNNN_name.sql` format/positivity/
  uniqueness — **no contiguity requirement** (`migrate.go:65-93`); `Pending`
  is computed per version (`!applied[f.Version]`, `migrate.go:145-149`);
  goose `Provider.Up` "applies all pending migrations" (`provider.go:244`),
  so a DB at {1..8,10} later runs plain `migrate up` and applies exactly 9 —
  **no special option**. `CheckCompatibility` refuses serve while any version
  is pending (`migrate.go:164-194`), so the gap can never serve unfilled.
  Rollback descends applied versions (`Down`, `provider.go:308`).
- PB-05 grows sequence (d): DB at {1..8,10} + files {1..10} → `migrate up`
  applies exactly 9, serve gate green after. Sequences (a)–(c) unchanged.
- PB independence: carrier FK → `withdrawal_authorizations` (007) only;
  grant.go/supply reference no 009 table or package. PB merges and serves
  with 009 absent; 009 needs PB, never the reverse — dependency is acyclic.

## R-PB9 — Issuance permission control points (concrete, PB-C1 mechanization)

- Identity source: operator presents an API key (new required supply flag);
  resolved by the existing `Authenticate` (`internal/withdrawal/auth.go:127`:
  single indexed read on `api_key`, constant-time compare, no cache, revocation
  observed immediately) to `(key_id, caller_id)`.
- Role mapping: deployment config allowlist of issuance `caller_id`s (exact
  env name to implement; mapping lives in config, never in code constants).
- Independent gate: new pure `PermitIssue(callerID)` in `internal/withdrawal`
  (nil-pool-safe, unit-tested like `validateSupplyOpInput`); the CLI carrier
  calls Authenticate → PermitIssue **pre-pool**, before any DB access.
  Failure → exit 1, zero rows.
- Deny-by-default: ordinary callers, 011 executors, and bare `--operator`
  strings are simply absent from the mapping → refused. `--operator` is never
  an input to PermitIssue.
- Direct library calls: `grant.go` stays transport-free (intake `can_create`
  precedent — permission lives at the carrier layer, not the library).
  Library docs state `SupplyGrant` assumes an authorized caller; tests assert
  the refusal at the carrier. DB roles unchanged: DSN remains the operator
  tooling trust root (same as `migrate`); permission is application-level at
  the single CLI entry — stated, not silently extended.
- Audit binding: `attested_by` = server-resolved `(key_id, caller_id)`, never
  a caller-supplied string; it joins op-input equality, so the same operation
  id retried by a *different* principal yields `operation_conflict` instead of
  converging (same principal still converges — existing semantics kept).

## R-PB10 — Supply-time authority protocol (closes F1/F2; no leniency)

Authoritative sources and their change entries (verified in-tree):
- Key validity: `api_key` row (`revoked_at`); writers are `RotateKey`
  (`internal/withdrawal/auth.go:214-249`, grace) and direct revoke
  (`auth.go:267`, single-statement `UPDATE`) — both commit independently of
  any supply tx.
- Caller state: `caller` row (`can_create`); the tree has exactly one writer,
  `INSERT` at `auth.go:190` — no UPDATE path exists, so in-flight change is
  possible only via direct operator DB writes (still re-read, defense in depth).
- Business allowlist: deployment config `TXHARBOR_AUTHZ_ISSUER_CALLERS`
  (comma-separated decimal caller_ids; whitespace trimmed, empty entries
  ignored; missing/empty → deny-all; illegal → startup config error, exit 2).
- Controlled switchover (replaces the earlier attribution-closure note; no key
  revocation by default, no hot-reload required). Model correction: supply runs
  as short-lived per-invocation CLI processes, each loading config at start —
  there is no long-lived daemon holding a stale mapping, so the problem
  reduces to in-flight invocations, which are bounded (seconds) and
  confirmable:
  1. Editing the config file is NOT effective. The operations role (PB-C1)
     first halts new supply invocations (entry control: stop schedulers and
     manual invocation; concurrent CLI invocations are all covered, not just
     one daemon).
  2. Drain: let running invocations exit, or terminate them and confirm via
     process table + `pg_stat_activity` that no supply tx remains in `BEGIN`
     (idle-in-transaction from a killed CLI rolls back on disconnect).
  3. Atomically swap the config (rename, not in-place edit) and record its
     checksum + swap timestamp as the verifiable effective point.
  4. Re-enable the entry only after (2)–(3) are confirmed; any new invocation
     then loads the new mapping by construction.
  5. Failure at any step → entry stays closed (no new invocations) until
     confirmed; never declare the switchover complete while an old executor
     is unaccounted for.
  Precondition: the config file (and its replicas, if multi-host) is swapped
  atomically on every host that can invoke supply; a host that cannot meet
  (1)–(5) is reported as a deployment gap, not waved through.
  Standalone key revocation/rotation keeps following the DB ordering protocol
  above; it is never implied by a mapping change.

Fixed order inside T-supply+scope (after the existing grant `FOR UPDATE`
is NOT enough — authority re-checks come first):
`BEGIN → statement guard → api_key row FOR SHARE (by key_hash; miss →
refuse) → caller row FOR SHARE + re-evaluate PermitIssue against the loaded
mapping → grant FOR UPDATE → grant/scope/audit writes → COMMIT`.
A revoke/rotate/caller UPDATE either commits before our SHARE (observed →
`supply_refused` with the failed check named in `reason`) or blocks until our
COMMIT (governs the next supply) — the same share-vs-update ordering 009
uses on the grant row and 006 uses at pre-commit. No advisory/lease objects.
Failure behavior uses the existing action vocabulary (`supply_refused` +
reason); no new action, no new business semantic, no revocation grace.

## R-PB7 — Adjacent lanes (no interaction by design)

- 008 merged (`02641fb`): read-api scope-row order and reconcile are 008-owned;
  PB touches neither. Shared surfaces (Serve listener, metrics registry) follow
  the proven additive pattern; no 008 change in this plan.
- 011 is a downstream consumer of the carrier (intent linkage); no 011 work here.
