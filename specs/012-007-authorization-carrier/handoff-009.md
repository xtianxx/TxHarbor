# T051 — Handoff Requirements to the 009 Lane

**Type**: lane-local note. **Nothing here is implemented in this lane.**
Every item below is a REQUIREMENT ON THE 009 LANE, executed after this PB
lane (carrier + supply) merges. This note changes no code and no other spec.

- PB lane = `012-007-authorization-carrier` @ `caf071e` (worktree
  `.slim/worktrees/prep-pb-auth`). Owns carrier + supply only (research R-PB6).
- 009 reference = `prep-009-signer` @ `8f75450` (matches research.md source pin).
  All 009 `file:line` citations below are @ `8f75450`. 009 files are read-only
  for this lane and MUST NOT be modified here (tasks.md L12).
- **Ownership after PB merge: `V-PB3-009` + `H1–H5` are owned by the 009 lane**
  (tasks.md L101-110, L137; quickstart V-PB3-009 stays joint-open, not closed
  by T050).

## PB-side shapes each requirement binds to (actual code, not speculation)

Carrier row `withdrawal_authorization_scopes` — read columns
(`internal/withdrawal/grant.go:133-137`):
`authorization_id, intent_id, request_id, sender, fee_max_total,
fee_max_per_gas, fee_max_priority, allows_fee_replacement,
authorization_version, attested_by`.

Supply op-input scope fields (`grant.go:192-212`): `IntentID, RequestID,
Sender, FeeMaxTotal, FeeMaxPerGas, FeeMaxPriority, AllowsFeeReplacement,
AttestedBy` (all zero/empty = stock/OPEN path, `scoped()` `grant.go:217-221`).

Audit `detail` keys (`opInputDetail`, `grant.go:266-289`) — semicolon-joined,
`key="value"` (`strconv.Quote`) and bare ints: `action, authorization_id,
caller_id, chain_id, asset, recipient, amount, expires_at, intent_id,
request_id, sender, fee_max_total, fee_max_per_gas, fee_max_priority,
allows_fee_replacement, attested_by`.

`authorization_version` semantics: starts at 1 on first supply
(`scopeInsertSQL`/`insertScopeTx(...,1)`, `grant.go:141-146,744,891`);
monotonic, and bumped ONLY by `scopeBumpVersionSQL`
(`UPDATE ... SET authorization_version = authorization_version + 1 WHERE
authorization_id=$1`, `grant.go:154-157`) when a non-active grant is
re-supplied (revoke-then-re-supply cycle, `grant.go:774-778`). Applied scope
CONTENT is immutable; re-issue mints a NEW grant id (new version 1), never
rewrites (`ReissueInput`, `grant.go:497-551`).

Fee rules the scope encodes (`validateFeeScope`, `grant.go:448-481`):
native-coin smallest-unit integers ≥ 0; `fee_max_priority <= fee_max_per_gas`;
any fee dimension present ⇒ `fee_max_total` and `fee_max_per_gas` required
(a missing cap is refused, never "unlimited"); all-zero triple = no fee
constraint.

---

## H1 — Carrier read shape + scope checks (009 `gates.go`)

**Requirement on 009.** Extend the grant read to actually load and evaluate the
scope row.

- Today 009 reads exactly 7 columns `FOR SHARE` (`gates.go:51-52`
  `GrantReadSQL`); `GrantScope` carries only `Present bool`
  (`gates.go:226-232`); `EvaluateGrantScope` returns
  `ClassAuthorizationUnverifiable` when `!Present` (`gates.go:239-243`).
- 009 MUST read the `withdrawal_authorization_scopes` row for the same
  `authorization_id` in the SAME `FOR SHARE` sequence as the grant read
  (same tx, same order — data-model.md L48-51, L68). Scope read introduces NO
  new lock object; it shares the existing grant-row sequence.
- 009 MUST add the consumption fields to `GrantScope` and enforce, per request:
  `authorization_id` equality; `sender` vs the request sender;
  `intent_id` / `request_id` linkage; `allows_fee_replacement` purpose token;
  the fee triple from the request (total / per-gas / priority) within
  `fee_max_total` / `fee_max_per_gas` / `fee_max_priority`, honoring
  `priority <= per_gas` and the "missing applicable cap = refuse" rule.
- `attested_by` is issuance-side identity carried in the scope row; 009 MUST
  NOT re-refuse a grant solely for an `attested_by` it cannot compare — its
  binding is to the request identity (009 has no issuance principal).
- The 009 service policy caps stay authoritative and apply conjunctively
  (research R-PB4): scope caps are an additional ceiling, never a relaxation.

## H2 — `authorization_version`: persist at submit, re-check at delivery

**Requirement on 009.** The fingerprint is a surrogate, not a version.

- Today 009 persists `authorization_fingerprint` (`^[0-9a-f]{64}$`,
  migration `000009_signer_service.sql:119,158-159`) + `authorization_state`
  (`:120`), filled at submit by `submitFingerprintSQL` (`submit.go:57-60,203`)
  from `AuthzGrant.Fingerprint()` (the `authz:v1` Keccak surrogate,
  `gates.go:170-191`), and compared at delivery in `deliveryGrantClass`
  (`delivery.go:389-399`). There is NO `authorization_version` column today.
- 009 MUST persist the scope's `authorization_version` at submit alongside the
  fingerprint. This is a 009-owned DDL change in its own (still-unapplied)
  `000009` — the PB lane does not add the column (renumber/apply rule:
  unapplied numbers may still move).
- 009 MUST re-check version equality at delivery against the persisted value:
  a scope/version change between submit and delivery MUST block, never be
  silently adopted (data-model.md L68-75). Keep the existing fingerprint
  equality check; the version check is additive.
- Rationale binding: a revoke-then-resupply cycle bumps
  `authorization_version` (bare revoke bumps it too — D1 contract; observed
  via version AND grant state); 009 MUST re-check grant current
  state/validity at delivery, never version-equality alone. (Assumes the
  sibling D1 revoke-bump change has landed; `runRevokeTx` carries it.)

## H3 — `replacement_of` threading

**Requirement on 009.** `replacement_of` is DDL-only today (dead in prod,
research R-PB6 L85) and MUST be threaded.

- Migration `000009`: `signing_requests.replacement_of BIGINT` self-FK
  (`:97,131-132`) plus partial unique index
  `signing_requests_authorization_anchor_uniq ON signing_requests
  (authorization_id) WHERE replacement_of IS NULL` (`:167-168`): exactly one
  anchor (non-replacement) per authorization; replacement rows may share the
  grant.
- 009 MUST write `replacement_of` = the anchor row id on a fee-replacement
  request, so the anchor partial-unique is preserved and the persisted
  anchor's `authorization_id` is never rebound.
- 009 MUST gate the replacement path on the scope's `allows_fee_replacement`
  (H1): fee-replacement reuse under the same grant is admitted ONLY when the
  scope explicitly permits it and the fee stays in range; otherwise a new
  authorization + PB re-issue is required (PB-FR-05, spec.md).

## H4 — Replace the hardcoded scopeless refusal

**Requirement on 009.** Remove the literal, keep the fail-closed default.

- `submit.go:211` is hardcoded: `EvaluateGrantScope(GrantScope{Present:
  false})`, with the comment at `:207-213` stating the carrier is absent so
  every grant fails closed. Dependent 009 fixtures also assume this:
  `restart_integration_test.go:16`, `binding_integration_test.go:24`,
  `gates_integration_test.go:443-447`.
- 009 MUST replace the literal with the real scope-read result (H1). A
  SCOPELESS stock grant MUST still refuse `authorization_unverifiable`
  per request (PB-FR-04, spec.md L63); only a present-and-verifiable scope
  passes. `V-PB3-009` is the per-grant refusal case and stays 009-owned.
- 009 MUST NOT write upstream state while doing so: 009 stays SELECT-only on
  `withdrawal_authorizations` / `withdrawal_authorization_scopes` (contracts
  `supply-scope.md` L44-52; `gates.go:7-8`).

## H5 — 009 full legal-path acceptance (post-merge, joint)

**Requirement on 009.** The joint closing case.

- After PB (`000010`) and 009 (`000009`) both merge, run the full legal path:
  a PB-supplied scoped grant passes 009 submit → delivery end-to-end with zero
  `authorization_unverifiable`, verifying scope content, version, sender, fee
  within caps, and purpose/intent/request linkage.
- Intent note: 009 has no independent intent table; intent / signing-request
  binding is re-checked by 009 under H5 (`grant.go:504-508` explicitly defers
  this to the 009 lane; re-issue reuses the same business intent). PB invents
  no intent table (`ReissueInput`, `grant.go:509-513`).
- `V-PB3-009` + H1–H5 acceptance is owned by the 009 lane; the PB lane only
  records the obligation (tasks.md L101-110, L114, L118, L137).

## Delivery re-check point (009 `delivery.go`)

**Requirement on 009.** The H2 re-check lands here.

- Delivery re-reads the grant `FOR SHARE` at `delivery.go:232`
  (`readGrantForShare`) inside the fixed lock order
  `008 scope-row FOR SHARE → 006 gate tables → 007 grant FOR SHARE → own rows
  FOR UPDATE` (`delivery.go:212-218,223-240`); the grant classification is
  `deliveryGrantClass` (`delivery.go:389-399`).
- Extend this same read to load the scope row and compare persisted
  `authorization_version` for equality (H2). A mismatch MUST block delivery
  (version equality vs persisted, data-model.md L69). No new lock object;
  scope read rides the existing sequence.
- 009 writes only its own admission/audit rows; zero writes to
  grant/scope/audit tables at delivery (contracts `supply-scope.md` L44-52).

---

## Scope boundary (do NOT do these here)

- PB lane implements carrier + supply ONLY (research R-PB6 L88). No 009 file,
  no 009 read-shape, no version column, no `replacement_of`, no delivery change
  is made in this lane. This note is the only artifact.
- 009 files MUST NOT be modified in this lane (tasks.md L12); migration numbers
  and apply rules follow R-PB5/R-PB8 (PB = `000010` first, 009 = `000009`
  later; gap at 9 is filled by `migrate up` with the allow-missing provider
  (see migrate.go:newProvider)).
