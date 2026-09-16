# PB Quickstart & Acceptance Design (OPEN-3 included)

**Feature**: `012-007-authorization-carrier` | **Date**: 2026-09-16
This file *designs* the acceptance (tasks phase assigns execution; implement
runs it). No test doubles substitute for the legal path.

## Re-issuance dry-run procedure (OPEN-3 design)

1. Pick a scopeless stock grant (queryable, never executable).
2. Authorized issuer re-verifies the underlying business facts off-system.
3. Explicit re-issuance via extended supply: NEW grant id + scope, audit
   detail links old grant/request ids.
4. Assert: old rows byte-identical; no new intent/nonce tables touched;
   old signing requests unbound (009 invariant, verified 009-side later);
   new grant passes 009 independent verification.

## Validation matrix (all real PostgreSQL; Anvil where requests execute)

| # | Case | Pass |
|---|---|---|
| V-PB1 | Legal supply + 009-style read-back (all fields, version 1) | green |
| V-PB2 | Unauthenticated / unmapped principal supply | refusal, zero rows |
| V-PB3-PB | Scopeless stock grant PB part: queryable/auditable, scope row absent | PB executes |
| V-PB3-009 | Scopeless per-grant refuse on the 009 lane | joint — 009 lane post-merge, NOT closed by PB |
| V-PB4 | Fee boundaries: total/per-gas/priority each at cap, cap+1, missing, illegal; priority > max_fee | refuse exactly the over/illegal ones |
| V-PB5 | Fee replacement: allowed+in-range reuses; otherwise fresh grant+identity required | green |
| V-PB6 | Revoke vs supply race: loser observes winner state; scope consistent | green |
| V-PB7 | Grant+scope+audit atomicity (kill -9 at phase points) | converge by operation id, zero partial |
| V-PB8 | Commit-unknown retry with same operation id | converges, never second grant |
| V-PB9 | Explicit re-issuance dry-run (procedure above) | trace complete, zero new intent/nonce |
| V-PB10 | Migration chain: empty→full, 007-era→carrier, down→re-up, **gap-fill ({1..8,10} + files {1..10} → up applies exactly 9, gate green)** | green; applied numbers untouched |

## Acceptance tiers (do not conflate)

- **PB self-acceptance**: V-PB1–V-PB10 above (this lane).
- **009 real integration**: 009 lane re-runs its legal path against PB-supplied
  grants (009-owned, after PB merges).
- **010/011 chain-level**: out of scope for both lanes (A-13 stays OPEN).
