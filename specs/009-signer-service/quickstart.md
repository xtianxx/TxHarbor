# Quickstart Validation — 009 Signer Service

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md) | **Contracts**: [contracts/api.md](contracts/api.md), [contracts/gates.md](contracts/gates.md), [contracts/persistence.md](contracts/persistence.md)

Design-only validation guide (Phase 1 output). Scenarios below are the acceptance matrix the
future implementation/tasks MUST execute; **this step starts no service and writes no code**.
Commands are illustrative sketches of the validation flow, not implementation artifacts.

## Isolation scheme (mandatory for every V-scenario)

008 and 009 run in parallel worktrees over one machine; shared test resources are not isolation
(`docs/workflow-008-009-parallel.md` R4; research R10):

- **Workdir-local database**: run 009 validation against a dedicated PostgreSQL database name
  (e.g. `TXHARBOR_PG_DSN=postgres://…/txharbor_009`), never the shared `txharbor` database.
  Integration tests use per-test testcontainers (container-local DB, random published port) as
  the repo already does — no fixed host port.
- **Non-default ports**: `TXHARBOR_SIGNER_HTTP_ADDR=127.0.0.1:8091` (never 8080, never 008's
  listeners). If a shared compose PostgreSQL is used for a manual walkthrough, map a non-default
  host port (e.g. `5433`) via a local override; never touch the shared `pgdata` volume.
- **No Anvil / no RPC**: 009 has no RPC client; none of these scenarios needs a chain. This is a
  structural property to assert (V8), not an environment choice.

## V1 — Structured signing happy path (US1/FR-01–FR-03, FR-16; SC-01)

1. Seed a caller + credential (`txharbor signer-auth issue …`), a 006-clean database (no pause
   rows, no active recovery), an 008 binding fixture (`matches`, no 008 contract yet → interface
   double per D3), and an active 007 grant whose fields equal the planned request.
2. Submit a complete ERC-20 transfer request (`POST /signer/v1/signing-requests`, api.md §2).
3. Assert: `200 signed` with `signature` + `tx_hash`; `delivery: delivered`; an admission row
   `delivered`; state `signed`.
4. **Independently verify**: reconstruct `types.Transaction` from the request fields, recover the
   sender from `signature` + `types.LatestSignerForChainID(chain_id)`, assert recovered address =
   `sender` and recomputed `tx.Hash()` = returned `tx_hash`.
5. Assert zero broadcast/RPC activity (009 has no RPC path; the test harness installs no chain).

## V2 — Arbitrary digest / incomplete content refusal (US1/FR-02; SC-01)

1. Submit a body with `digest`, `message`, or `hash` fields (or with `data` only and no full
   transaction fields) → assert `422 arbitrary_digest_rejected`, no signature, no result row.
2. Submit bodies missing one required field each (`chain_id`, `sender`, `nonce`, `to`, `data`,
   `gas_limit`, fee shape, `asset`/`recipient`/`amount`) → assert `422 validation_failed` with the
   field named; no result rows created.
3. Assert no endpoint or field accepts a raw digest anywhere (route list review + negative probe).

## V3 — Authentication, permission, ownership (US2/FR-04–FR-05; SC-02)

1. Missing credential → `401`; invalid credential → identical `401`; revoked credential → `401`.
2. Valid credential with `can_sign=false` → `403 signing_not_permitted`, zero signatures.
3. Body claims another `sender`/caller → service uses the credential-derived caller; the
   request's `sender` is validated against policy/registry, never trusted as identity.
4. Caller A queries caller B's `signing_request_id` → **identical** `404` to a nonexistent id
   (byte-equal code/message); no ownership or existence leak.
5. Scan responses/errors/logs for credential material → 0 occurrences.

## V4 — Identity ↔ content binding, conflict, determinism (US3/FR-13–FR-15; SC-03)

1. Submit identity K/content C twice (sequentially) → second returns the **same** `signature` and
   `tx_hash`; exactly one `signature_results` row; audit shows `replayed`.
2. Submit K again with one bound field changed (chain, sender, nonce, `to`, `data`, `value`,
   `asset`, `recipient`, `amount`, fee fields, `intent_id`, `binding_ref`, `attempt_id`,
   `authorization_id`, `recovery_version`) → `409 request_conflict`; original result unchanged;
   zero new signatures.
3. Concurrency: N (N≥2, plan defines N=8) parallel identical submissions → one result row, all
   successful responses carry the same bytes; losers observe either the result or
   `outcome_not_yet_visible` (same-identity retry converges).
4. Restart the signer process between attempts → same result from persistence (no memory
   dependence).
5. Fee replacement = new `attempt_id` + new `signing_request_id` + same intent/binding, per OC-5
   **conditional** rule: reuse the original grant only if it explicitly permits fee replacement
   and the new fee is in scope (structurally possible via `replacement_of` + the partial anchor
   index), else a **new** `authorization_id` with the new identity → accepted path; a reuse that
   cannot be verified, or an anchor-index collision → `403 authorization_invalid`. Never rebind an
   existing request row to another grant.

## V5 — Validation matrix (US4/FR-06–FR-12; SC-04)

One negative case each (all: refused before signing, signature count 0, class recorded):
`chain_id` not the deployment chain; sender absent/disabled from the registry; sender–key
mismatch; `asset != to`; asset/contract not allowlisted; calldata selector ≠
`transfer(address,uint256)`; calldata recipient/amount ≠ declared/authorized values;
recipient outside policy; zero/negative/decimal/non-integer/over-uint256 `amount`; native
`value != 0`; `gas_limit`/fee values outside policy caps; `max_priority_fee_per_gas >
max_fee_per_gas`; non-empty `access_list`. Positive controls pass. No float arithmetic appears in
any code path (static check + integer-typed validators).

## V6 — Gate consumption, read-only (US6/FR-17–FR-19; SC-06)

1. Insert an 006 `indexer_pause` (then `log_pause`, `deposit_pause`) row → submit → refused
   `recovery_paused`, basis names the observed table(s); no signature; no queue-for-later.
2. Insert an active `reorg_recovery` row (any phase) → refused `recovery_active`.
3. Recovery version changes between content construction and submit → refused
   `recovery_version_changed`; re-submitting the same content also refused at delivery.
4. 008 binding classes: `matches` signs; `absent`/`conflict`/`paused`/`read_failed` each refuse
   with its class, audited; no call to any 008 writer.
5. 007 grant: absent/inactive/expired/revoked/field-mismatch → refused with the class; a
   concurrent revoke during submit serializes on the grant row (share vs update lock); one grant
   never backs two request identities.
6. Read-only assertion: run the whole scenario set, then diff upstream tables (006 pause/recovery,
   007 grants) — byte-identical except the test-fixture rows the harness itself inserted.
7. 007 Accepted does not trigger anything: creating a 007 withdrawal request produces zero 009
   activity (no signing by receive status).

## V7 — Delivery admission, withholding, unknown (US6/FR-17/FR-23; SC-06/SC-08)

1. Sign successfully, then revoke the 007 grant (or let it expire) → same-identity retry returns
   `409 signature_withheld`, status-only (no signature, no `tx_hash`); audit records
   authorization id + observed state; result/binding/history retained.
2. Enter an 006 pause (or 008 binding pause) after signing → retry withheld with the pause basis;
   multiple independent pauses: releasing one does not bypass the other; 009 clears none.
3. Gate passes and admission commits → response carries the signature; simulate a crash between
   admission COMMIT and response write → retry re-runs gates, re-delivers the same bytes after a
   pass, or withholds after a failure; admission rows tell the story; never re-signed. Assert the
   **bounded admission validity**: a delayed send cannot reuse the stored `admitted` row — it must
   re-run T-deliver and record a new `attempt_seq`, and is `blocked` if a pause/revoke/`can_sign`
   off became visible in between; the two R6 timelines (pause/revoke committed before admission →
   status-only; admission committed before pause/revoke → immediate write, delayed send re-admits)
   and the `can_sign` disable-between-sign-and-delivery case are both exercised.
4. Force an indeterminate delivery outcome → response is `outcome_unknown` status-only; the
   reconcile path (persistence.md §6) uses audit + admission rows; no new identity, no new intent.
5. Injection points: after gate check, during signing, after result commit, at admission, at
   response — each asserts no ungated delivery and a recorded basis.

## V8 — Failure paths, secrecy, isolation (FR-20–FR-25; SC-05/SC-07/SC-08)

1. Key provider unavailable/timeout → bounded failure, class recorded, **never** `signed`; retry
   with the same identity succeeds once the provider returns; no partial result rows.
2. Storage unavailable after signing path starts → no success response; when storage returns, the
   persisted result (if any) is retrievable; no data loss.
3. Restart/crash at each point in persistence.md §2/§5 → no second observable signature; retries
   converge; infinite-retry occurrences: 0.
4. Test/deployment key isolation: `production` mode without a production provider refuses to
   start; `production` mode never loads the test key; `development` mode refuses when the key
   file is missing (no fallback).
5. Secrecy scan: logs, errors, responses, metrics, and repository for key material, credentials,
   signature bytes on refusal paths, raw signed-transaction bytes → 0 occurrences (incl. startup
   echo). Import boundary: business `serve` wires no `KeyProvider` (test asserts the dependency
   direction).
6. Never broadcasts: static check that `internal/signer` imports no RPC/dial package; runtime
   harness has no chain endpoint.
7. Isolation: run the full suite with `txharbor_009` + non-default ports while a dummy 008-like
   listener occupies other ports; assert no cross-talk and no writes to the shared database.

## Exit criteria (design mapping)

SC-01→V1/V2, SC-02→V3, SC-03→V4, SC-04→V5, SC-05→V3/V8, SC-06→V6/V7, SC-07→V3/V8, SC-08→V7/V8.
T000-P remains open; local Anvil-free green does not imply production readiness.
