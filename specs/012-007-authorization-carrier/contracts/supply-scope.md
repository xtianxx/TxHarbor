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

1. Operator presents an API key (required supply flag) resolving via existing
   `Authenticate` to `(key_id, caller_id)` — single indexed read, revocation
   observed immediately, no cache (`internal/withdrawal/auth.go:127`).
2. `PermitIssue(caller_id)` passes against `TXHARBOR_AUTHZ_ISSUER_CALLERS`
   (comma-separated caller_ids; missing/empty → deny-all; illegal → startup
   error exit 2). Ordinary callers, 011 executors, and bare `--operator`
   strings are absent from the mapping → deny-by-default.
3. Inside T-supply+scope, before any write: api_key row `FOR SHARE` +
   caller row `FOR SHARE` + `PermitIssue` re-evaluation (R-PB10 order).
   Failure at any point → `supply_refused` naming the failed check, zero writes.
4. `--operator` (if given) is recorded verbatim in audit; it grants nothing
   and is never an input to (1)–(3).
Failure of (1)/(2) → exit 1, zero rows. `attested_by` is always the
server-resolved principal (never caller-supplied) and joins op-input equality:
same operation id from a different principal → `operation_conflict`.

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
