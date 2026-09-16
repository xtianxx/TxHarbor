# Contract: Extended Supply Entry (`withdrawal-authz supply`)

Applies to the existing `txharbor withdrawal-authz supply` carrier
(`internal/app/withdrawalauthz.go`); 009 never calls it.

## Extended op-input (added to current flags)

Current: `--operation-id` (required), `--authorization-id`, `--caller`,
`--chain-id`, `--asset`, `--recipient`, `--amount`, `--expires-at`,
`--operator` (audit-only), `--reason`. Added: `--intent-id`, `--request-id`,
`--sender`, `--fee-max-total`, `--fee-max-per-gas`, `--fee-max-priority`,
`--allows-fee-replacement`, `--attested-by` (defaults to the authenticated
principal when omitted; never to `--operator`).

## Preconditions (new, before pool use)

1. Caller presents credentials resolving to an authenticated principal.
2. Principal is mapped to the issuance role (PB-C1; mapping source is
   deployment config, fixed in implement).
3. `--operator` (if given) is recorded verbatim; it grants nothing.
Failure of (1)/(2) → exit 1, zero rows, audit row optional-but-identifiable
(no grant/scope mutation either way).

## Transactional guarantees (unchanged shape, extended payload)

Same tx writes grant + scope + audit (data-model.md T-supply+scope).
Equal-input retry converges (same operation id); differing input →
`operation_conflict`, zero writes. Commit-unknown converges by operation id.

## Exit codes (unchanged)

0 committed (supplied/resupplied), 1 refusal/failure, 2 usage.
`--operation-id` missing still precedes config (existing behavior kept).

# Contract: Scope Read (009 consumption boundary)

009 (later lane) reads `withdrawal_authorization_scopes` read-only in its
existing `FOR SHARE` sequence: `authorization_id` equality;
sender/fee-triple/purpose/intent/request checks; persist + re-check
`authorization_version`. No 009 writes to grant/scope/audit tables — ever
(this is a PB-side guarantee the 009 lane may assert in tests).
Scopeless grant → 009 `authorization_unverifiable`; this contract defines
nothing else about 009 behavior.
