# Research: 009 Signer Service（签名隔离服务）

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md) (clarify 两轮, zero markers; OC-1–OC-7 resolved 2026-09-16)

Phase 0 output. All technical unknowns resolved below; no NEEDS CLARIFICATION remains (the
business decisions were closed in the two 2026-09-16 clarify sessions and are inputs here, not
unknowns). OC-1–OC-7 rulings are consumed as fixed; this file MUST NOT reopen them. No code
modified by this step.

Evidence base (read-only):
- Upstream contracts: `specs/006-reorg-recovery/contracts/downstream.md` (FR-26 operation matrix);
  `specs/007-withdrawal-creation/contracts/api.md` §3/§4 + spec FR-08/FR-16/FR-19 (receive-only,
  Accepted ≠ intent/authorization); `specs/007-withdrawal-creation/research.md` R1–R9 (credential
  lifecycle, insert-first, one-lock discipline); `docs/workflow-008-009-parallel.md` R1–R7.
- Code mechanics: `internal/indexer/reorgcommit.go` (`readStreamPausesTx`,
  `captureRecoveryVersion`, `recheckRecoveryGate`, `RecoveryCapture`),
  `internal/indexer/reorgquery.go` (`RecoveryReleased`), `internal/withdrawal/grant.go` (007
  carrier read shape + `FOR SHARE` receipt precedent), `internal/config/config.go` (env-only,
  versioned config identity), `internal/app/serve.go` (single-binary subcommand wiring),
  `internal/db` (goose migrations), `internal/logx` (`Redact`), `internal/metrics` (existing
  registry), migrations `000002`/`000003`/`000004` (pause tables) and `000006` (`reorg_recovery`
  + events), `go.mod`, `Makefile`, `compose.yaml`.
- 008 nonce-manager artifacts are NOT read (R4/R6 parallel rule; not present in this worktree).
  Its consumption side is defined at the interface level only (R8), never as a carrier decision.

Repo pins that bind every decision: **postgres:18.6-trixie** (no `ON CONFLICT DO SELECT`);
**Go 1.26.5**; **go-ethereum v1.17.5 already vendored** (`types`/`crypto`/`common` — no new
dependency, no custom cryptography); **pgx v5.11.0** (`pgconn.PgError`); **no new infrastructure**
(constitution XIII; spec Non-Goals); 009 MUST have **no RPC client and no broadcast code path** —
"永不广播" is structural, not a runtime check. 009 reads 006 pause/recovery tables and the 007
grant carrier read-only; it owns its own credential, request, result, admission, and audit tables
(dedicated migration `000009`, dedicated database name in local validation — R10).

## R1 — Signing implementation: go-ethereum `types` + `crypto`; structured tx only

- **Decision**: build the signable object as `types.Transaction` (`types.LegacyTx` for tx_type 0,
  `types.DynamicFeeTx` for tx_type 2) from the validated request fields, sign with
  `types.SignTx(tx, types.LatestSignerForChainID(chainID), key)` behind `KeyProvider` (R2), and
  return `tx.Hash()` + the raw signature (v/r/s extracted via `tx.RawSignatureValues()`).
  Independent verification is defined as: rebuild `types.Transaction` from the same content
  fields, recover the sender from the signature + `types.LatestSignerForChainID(chainID)`, compare
  addresses and `Hash()`. Signature determinism (same key + same tx → same bytes) comes from
  go-ethereum's RFC-6979 deterministic nonce; the API never depends on it (it returns the
  persisted result, never a re-sign).
- **Rationale**: constitution Security Rules and FR-20: "既成密码学库，MUST NOT 自造密码学".
  go-ethereum is already the repo's EVM type/address authority (go.mod; 007 address checksum);
  `types.SignTx` performs EIP-155 replay protection and EIP-2718/1559 encoding, so there is no
  hand-rolled RLP or digest path. The signing surface is *only* whole transactions: the digest is
  derived inside the library from structured fields, which makes arbitrary-digest signing (FR-02)
  unreachable rather than merely rejected.
- **Alternatives considered**:
  - raw `crypto.Sign` over a caller-supplied hash — rejected: that *is* arbitrary-digest signing.
  - `personal_sign`/EIP-191/EIP-712 message signing — rejected: not a transaction request; out of
    scope and the primary abuse path the boundary exists to block.
  - custom RLP/canonical encoding for the signed object — rejected: custom crypto/encoding next to
    a battle-tested library is exactly what constitution Security Rules forbid.
- **Note on the canonical envelope (R3)**: it is a *storage-side binding identity*, never the
  signing digest. The signature is always produced by `types.SignTx` from the reconstructed
  transaction; the envelope/hash exist for same-identity conflict detection and audit.

## R2 — `KeyProvider` shape: structured-tx surface, sender-bound, mode-isolated

- **Decision**: one narrow interface, whole-transaction in / signed-transaction out:

  ```go
  type KeyProvider interface {
      // SignTx signs tx with the key registered for sender (EIP-155 via chainID).
      // It MUST refuse when no key is bound to sender or the key address != sender.
      SignTx(ctx context.Context, sender common.Address, chainID *big.Int, tx *types.Transaction) (*types.Transaction, error)
  }
  ```

  v1 ships one provider: a local, disposable **test-key** provider loaded only in `development`
  mode (`TXHARBOR_SIGNER_MODE=development`, key from an operator-supplied file with no default and
  no implicit fallback). `production` mode MUST fail closed at startup unless a non-local provider
  is configured — and none ships in 009 (T000-P stays open). There is no
  `SignDigest`/`SignHash`/`SignMessage` method anywhere in `internal/signer`; a digest-signing
  surface MUST NOT be added later (that would defeat the boundary, not extend it).
- **Rationale**: constitution VIII (KeyProvider isolates implementation, business logic unchanged
  when migrating to KMS/HSM) + FR-20/FR-21. Sender→key binding inside the provider is what makes
  the "签名密钥与 sender 匹配" check (OC-2) structural: the provider cannot sign as a different
  sender. Mode separation is enforced by configuration validation, not convention (FR-21: mixing
  MUST refuse startup or refuse signing).
- **Alternatives considered**:
  - `KeyProvider` returning a `crypto.Signer`/`ecdsa.PrivateKey` — rejected: hands digest-signing
    power (and raw key material) to business code; violates the isolation the interface exists for.
  - `KeyProvider.SignRequest(json)` taking the API request — rejected: couples the provider to
    transport/parsing; validation must happen before the provider sees anything.
  - KMS/HSM-backed provider in v1 — deferred: production selection is T000-P (open); the interface
    is shaped so the swap requires no business-logic change.
- **Key hygiene**: key bytes never enter logs, errors, metrics, or the request/result tables;
  `internal/logx.Redact` covers credential material in the startup echo; the provider is wired
  only by `signer-serve`/`signer-auth` paths — the business `serve` path MUST NOT import the
  provider constructor (import-boundary test, quickstart V8).

## R3 — Identity & content binding: caller-preallocated id + canonical envelope + content hash

- **Decision**: request identity = authenticated `caller_id` + caller-preallocated
  `signing_request_id` (OC-4), persisted UNIQUE `(caller_id, signing_request_id)`. The bound
  content set is the full normalized transaction plus its upstream references:
  `chain_id, sender, nonce, tx_type, to, value, data, gas_limit` and exactly one fee shape
  (`gas_price` for type 0; `max_fee_per_gas`+`max_priority_fee_per_gas` for type 2), the declared
  transfer triple (`asset, recipient, amount`), `intent_id`, `binding_ref` (008 binding identity,
  opaque), `attempt_id`, `authorization_id` + authorization fingerprint, and `recovery_version`
  (the recovery version the content was built under, R5). Deterministic **canonical envelope** =
  versioned text serialization (`signer-request:v1\n` + `field=<strconv.Quote(value)>` lines,
  mirroring `opInputDetail`), stored verbatim; `content_hash = keccak256(envelope)` as
  `0x`+64 hex, stored for audit/indexing. Same `(caller_id, signing_request_id)` + identical
  envelope → converge to the original request/result; any bound field differs → conflict, no
  signature (FR-13). A new attempt MUST use a new `attempt_id` + new `signing_request_id`
  (`attempt_id` is also UNIQUE — one attempt = one request identity).
- **Rationale**: FR-13/OC-4; constitution II. Storing the envelope byte-for-byte makes the
  equality decision exact (no re-encoding drift, no float, no JSON number ambiguity) and gives
  audit a single comparable artifact. `content_hash` is the short identity used in logs/status.
- **Alternatives considered**:
  - re-derive equality from parsed columns — rejected: two encodings of the same value
    (e.g. uppercase address, `0x0` vs `0x00`) must still be *the same content*; canonicalization
    happens before persistence, and the envelope records its result.
  - hash-only storage (no envelope) — rejected: collision handling and operator-readable audit
    both suffer; the envelope is small.
  - client-supplied content hash — rejected: trust boundary (FR-04/FR-13); the service computes it.
- **In-flight duplicate**: two concurrent submits with the same identity: insert-first → loser
  observes the unique conflict (R4); same envelope → the loser converges on the persisted
  result/outcome; different envelope → conflict. Never a second signed object.

## R4 — Sign+persist in one row-locked transaction; no second observable signature

- **Decision**: submit path owns one transaction (insert-first, mirroring the repo's reviewed
  intake shape):
  1. `BEGIN` → statement timeout guard → plain `INSERT` of the request row (`state='received'`)
     carrying identity + envelope + content hash + first-class refs.
  2. On `23505` for `signing_requests_caller_request_uniq`: roll back, re-read the existing row,
     compare envelopes → **equal**: if a signature result exists, continue to the delivery path
     (R6) with the persisted result; if not (first attempt in flight/unknown), return
     "outcome not yet visible, retry with the same identity" (`503`-shape) — never re-sign as a
     new identity. → **different**: `409 request_conflict`, original untouched.
  3. First winner: `SELECT … FOR UPDATE` its own row before gate reads, so a concurrent
     same-identity submit can never interleave between gate-read and signature persistence.
  4. Gate reads + policy validation (R5) → on refusal, persist state/reason (audit) and return
     the refusal; the request row keeps a non-terminal state where the refusal is transient
     (recovery/pause/read-failure) so a same-identity retry re-drives it, or `rejected` where the
     refusal is terminal (policy/authorization/conflict) so retries converge on the same refusal.
  5. Sign via `KeyProvider` → **insert `signature_results` and transition state to `signed` in
     the same transaction** → `COMMIT` → only then shape the response (first delivery goes through
     the delivery admission, R6).
  `signature_results` is keyed by the request row and `UNIQUE (tx_hash)`: storage itself makes a
  second persisted signature for the same signed object impossible; the ROW LOCK makes concurrent
  double-signing wait rather than race. A crash anywhere before `COMMIT` leaves no observable
  signature; because the same key + same content produce the same bytes, a retry re-signs
  deterministically and persists — exactly one observable result (FR-14, SC-03).
- **Rationale**: FR-13 ("签名结果 MUST 在成功返回前可靠持久化"), FR-14, constitution II/V/VI;
  the insert-first + 23505-classify protocol is the repo's established pattern (007 R6;
  `resolveAuthRace` precedent). "No second observable signature" is enforced by persistence
  (blocking result + row lock), never by an in-memory check.
- **Alternatives considered**:
  - read-then-write ("SELECT first") — rejected: does not remove the race and still needs 23505
    handling; wasted round trip.
  - `ON CONFLICT` variants — rejected on the PG18.6 pin; untargeted `DO NOTHING` would swallow
    unrelated uniqueness violations (007 R6 trap).
  - signing outside the transaction and persisting after — rejected: crash window would allow an
    observable signature with no durable result (violates FR-13 and the "未知不得视为未签名"
    edge case).
- **Bounded failure handling**: commit-unknown → do not claim signed; report `503`-shape with the
  same-identity retry instruction; recovery is a bounded re-read of the persisted row (no loops,
  no auto re-mint of identity).

## R5 — Read-only gate observation: 006 pause/recovery in one read sequence + 007 grant `FOR SHARE`

- **Decision**: every signing admission performs, inside the sign transaction and **before**
  signing:
  (a) **006 gate, one read sequence**: existence of `indexer_pause`, `log_pause`, `deposit_pause`
  rows for the deployment chain (row existence = paused; 006 owns them) **and** the active
  `reorg_recovery` row (any persisted phase blocks ordinary batches) **and** the current recovery
  version (active row's `recovery_seq`, else the events-stream MAX, else 0 — the
  `captureRecoveryVersion` shape, re-implemented as a read-only consumer; never 006's writer
  functions). The request's `recovery_version` MUST equal the current version, otherwise refuse
  (`recovery_version_changed`) — this is the FR-17 "旧链视图内容不得提交签名" boundary.
  (b) **007 authorization read**: `SELECT … FROM withdrawal_authorizations WHERE
  authorization_id = $1 FOR SHARE` plus `state='active'` and (`expires_at IS NULL OR
  expires_at > now()`) evaluated on the DB clock; the bound grant fields
  (`caller_id, chain_id, asset, recipient, amount`) plus the observed `state`/`expires_at` MUST
  match the corresponding request/declared fields. The transaction first acquires the shared gate
  lock (`LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events
  IN SHARE MODE`, R6) so the 006 observation is ordered against pause/recovery writers; the 007
  grant `FOR SHARE` is the grant-axis lock and matches the 007 receipt-path discipline (007 R7:
  revoke takes `FOR UPDATE`/UPDATE on that row; share locks do not block each other). `can_sign`
  is re-read at delivery under `FOR SHARE` (R6).
- **Rationale**: FR-17/FR-18, 006 `contracts/downstream.md` preconditions (observe in one read
  sequence; observation failure → refuse, record cause, no queue-for-later), 007 R7 lock precedent.
  009 MUST NOT write, clear, or release any 006 pause/recovery row or any 007 grant — all reads
  are read-only, and `internal/signer` contains no non-`SELECT` statement against those tables
  (reviewable + quickstart assertion).
- **Alternatives considered**:
  - queue/lease "wait until recovery ends" — rejected: 006 states refusal, not queued approval.
  - cache gate state — rejected: pause rows are the authority; a cache turns a durable gate into a
    staleness window with no upside at this volume.
  - read the 006 gate outside the sign tx — rejected: the gate basis must belong to the same
    transaction that later persists the signature, otherwise the recorded evidence and the
    decision can diverge (006 FR-26: "以读取序列为准").
- **Five-class gate vocabulary** (used for 006/007/008 reads alike): `ok` / `absent` /
  `conflict` / `paused_or_reconciling` / `read_failed_or_unknown`; only `ok` admits. Refusal
  classes are recorded in the audit row with the observed basis (pause table(s) present,
  recovery phase/seq, grant state/fingerprint).

## R6 — Delivery admission ordering: shared table-level gate lock closes the pause/revoke window

- **Decision — common coordination basis (table-level `SHARE` lock)**: every signing admission
  (T-submit) and every delivery assessment (T-deliver) acquires, as the first locked statement
  after the timeout guard and before any gate read:
  `LOCK TABLE indexer_pause, log_pause, deposit_pause, reorg_recovery, reorg_recovery_events
  IN SHARE MODE`, held to `COMMIT`. PostgreSQL `SHARE` conflicts with the `ROW EXCLUSIVE` every
  `INSERT`/`UPDATE`/`DELETE` takes, so a 006 writer **cannot commit a pause/recovery change
  concurrently with 009's decision**:
  - a pause/recovery write that committed before 009's lock is visible to 009's in-tx read;
  - a pause/recovery write racing 009 waits on the table lock until 009 commits and is therefore
    ordered *after* the admission;
  - an **empty** table ("当前无暂停行") is covered because the lock is on the relation, not on a
    row — this is the explicit handling of the future-`INSERT` ordering the row-lock design
    could not provide (locking the 007 grant never covered a new pause in another table);
  - the manual-DBA release path of `indexer_pause`/`log_pause` (no application writer exists) is
    covered too, because a direct `DELETE` takes `ROW EXCLUSIVE` on the table.
  This needs **no 006 code or migration change** and does not depend on 006's internal writer
  lock being present. The read-only invariant is refined (gates.md §5): 009 issues `SELECT` and
  `LOCK TABLE … IN SHARE MODE` — a lock acquisition with no data modification — against 006
  tables, never `INSERT`/`UPDATE`/`DELETE`. `LOCK TABLE` wait and the gate reads are bounded by
  the 5s `statement_timeout`; a wait/deadlock is `gate_read_failed`, fail-closed, retryable with
  the same identity.
- **Decision — 007 auto gate unchanged**: `SELECT … FOR SHARE` on the grant row; 007's revoke and
  re-supply take `FOR UPDATE`/`UPDATE` on that row, so revoke/replacement orders against delivery
  at the grant-row lock (R5; 007 R7/R8 precedent).
- **Decision — wallet/permission gates**: delivery re-reads `signer_caller.can_sign` with
  `SELECT … FOR SHARE`; 009's operator disable path (`signer-auth`) is required to take
  `FOR UPDATE` on the same row. The 008-side pause/registry-disabled gate is consumed through
  `BindingReader` (serialized per gates.md §3) AND through 009's direct scope-row `FOR SHARE`
  (below), so a paused/reconciling/disabled binding can never be admitted as `BindingMatches`.
- **Fixed lock order (one ring-free order)**: (0) 008 scope row `FOR SHARE` (joint coordination,
  same DB) → (1) 006 gate tables `SHARE` (single statement) →
  (2) 007 grant   `FOR SHARE` → (3) `signer_caller` `FOR SHARE` → (4) own `signing_requests` row
  `FOR UPDATE` (submit and delivery alike: same-identity deliveries serialize the send region) → (5) `delivery_admissions` `INSERT`. No 006/007
  writer ever acquires a 009 table, 008 writers take the scope row `FOR UPDATE` first and only
  read afterwards, and no 009 path acquires two of its objects out of this order,
  so no lock-order cycle can form.
- **Decision — admission ordering (evidence order kept)**: T-deliver takes the locks above,
  re-reads 006/007/008 + `can_sign` in one read sequence, `INSERT`s one `delivery_admissions` row
  (`admitted`/`blocked`) recording the snapshot and `COMMIT`s; **only then** are response bytes
  written; the post-write `admitted → delivered` marker is best-effort. Gate failure at delivery
  → no signature material in any field: desensitized status only (`state`, `content_hash`,
  refusal class/reason, timestamps; no signature bytes, no `tx_hash` unless a prior admission is
  `delivered`/`admitted`). Explicit inversion of the 007 still-200 replay (OC-6 MUST NOT 类比).
- **Decision — admission and handoff share one protected region (no grace window)**:
  T-deliver performs gate re-reads, the `admitted` INSERT, the response-bytes write, and the
  `delivered` marker inside ONE transaction holding all locks (order §"Fixed lock order").
  There is no "admit now, write later" split: bytes are written only while the locks are held,
  so a pause/revoke/permission change either commits before the region (observed → `blocked`)
  or waits for the region's `COMMIT` (ordered after → in-flight approved). No TTL, no
  post-`COMMIT` grace.
- **Superseded 2026-09-16 (retracted, history kept)**: the prior `valid_until` TTL revision
  claimed the window closed. It did not: `t=0` admission commits (5s TTL) → `t=1` revocation
  commits → `t=2` send still allowed = post-revocation delivery of unsent bytes, i.e. bounded
  grace semantics, not "uncancellable in-flight". A clock check additionally cannot survive
  preemption between the check and the write. The TTL column/mechanism is withdrawn (kept in
  git history only);   `valid_until` MUST NOT reappear as an authorizing mechanism.
- **Decision — bounded send region mechanics (locks span the write, bounded both sides)**:
  the earlier "locks never span network I/O" constraint is lifted for exactly this region, with
  both sides bounded. Transport = the existing `txharbor serve` HTTP response path (009 has no
  broadcast; plan Summary): "handoff" = the response-write syscall returning success (bytes
  accepted by the OS); "cancelled" = write error / client disconnect (plan-level requirement:
  server write timeout, e.g. ≤2s, strictly inside the 5s `statement_timeout`, both deployment
  config). Write failure or timeout → `ROLLBACK`: no admission row survives, no bytes counted
  as delivered; retry re-gates fresh. DB connection loss mid-region → transaction aborts →
  `unknown_reconcile` on retry (bytes may have gone out: honest unknown, identical-bytes
  re-delivery only). Process suspend mid-region → locks stay held AND the server-side
  `statement_timeout` still fires → region aborts on resume (or writers were merely delayed);
  suspension can delay pause/revoke writers but can never let an ungated byte out, because no
  byte is written outside the region. Writer-side impact (stated, bounded): 006 pause INSERTs,
  007 revokes, and 008 writers wait on 009's `SHARE` locks for at most the region budget
  (send timeout + `statement_timeout`); they are ordered after, never starved, never failed —
  delay, not denial. Same-identity concurrent deliveries serialize on the request row
  `FOR UPDATE` (catalog order below); a duplicated region would only ever emit byte-identical
  content.
  - Shared-serve impact (stated): `WriteTimeout` is connection-wide on the existing `serve`
    listener shared with 007 endpoints — setting it bounds stalled writes for all handlers
    (safety-positive; no 007 semantic change, only long-stalled writes now fail instead of
    hanging). Recorded here so the tasks/implement diff on shared `serve.go` is expected, not
    incidental.
- **Guarantee-scope revision (user ruling 2026-09-16, fault-model limitation accepted)**:
  protection-effective ⇒ ordering holds (pause/revoke before region entry blocks, racers wait);
  protection-lost (DB session death and kin) ⇒ NO absolute zero-byte promise. On detecting the
  loss the path MUST best-effort stop (pre-write session/tx liveness check in T-deliver; any
  detected death → `ROLLBACK`, zero bytes — stated as best-effort, TOCTOU-limited). The
  residual covers BOTH "bytes out, marker lost" AND "protection lost, sender unaware, sending
  continues" — undeterminable outcomes are `unknown`, never presented as success or as
  safe-failure. Same-bytes redelivery is NOT unconditional: every redelivery re-passes current
  authentication, authorization (incl. expiry/revocation), pause, and admission gates — a
  revoked/expired grant or an active pause blocks even byte-identical redelivery. Audit records
  only confirmed facts; unconfirmed outcomes stay `unknown`, never backfilled as confirmed.
  This residual is not claimed to be the only possible fault, and accepting it replaces no
  remaining design check or fault acceptance. Retracted stronger claims (history kept):
  "write failure ⇒ nothing delivered", "locks never span network", the TTL permission window.
- **Feasibility verification 2026-09-16 (probes + code + official docs; PG 18.6, Go 1.26.5)**:
  - `statement_timeout` bounds ONE statement's server execution, not the transaction and not
    network waits. Probed on stock PG 18: `pg_sleep(5)` under 2s `SET LOCAL` aborts the
    statement while the session continues; a 6s statement-free idle gap inside the tx runs
    untouched; a `FOR UPDATE` lock wait aborts at ~2s (`while locking tuple`). Hence the
    "5s guard" never bounded the send region — the network bound MUST come from the transport
    (`http.Server.WriteTimeout`, currently unset in `serve.go`; only `ReadHeaderTimeout`
    exists), strictly inside the statement budget, plus the serve path actually setting it
    (tasks/implement acceptance).
  - A small `Write`+`Flush` returns success with the peer provably receiving nothing (probed:
    23 bytes, nil error, RST-without-read); `Flusher.Flush` returns no error at all. Write
    success = handed to the OS; write failure = MAY already be partially out. The contract
    therefore claims neither receipt-on-success nor zero-delivery-on-error — both map to
    `unknown` with same-bytes-only recovery.
  - DB-session death releases the region's locks server-side (locks live to transaction end;
    the backend aborts the open tx). The sender learns of the death only on its next
    interaction (`pgx`: `ErrConnClosed`/op error; pool reuse detects via `ResetSession`);
    a `SELECT 1` health check before the write narrows but cannot close the race (TOCTOU).
    Prevention of bytes under a dissolved protection is impossible across independent
    channels — containment (`unknown` + overlap accounting + same-bytes-only + never re-sign)
    is the mechanism, and it is stated as such, not as prevention.
  - Suspended process: server-side `statement_timeout` fires only for an executing statement;
    nothing runs Go timers while suspended. On resume past the write deadline the write fails
    and the path re-gates (a revoke committed meanwhile → `blocked`); locks held during the
    suspension delay writers but release on abort. Suspension can delay, never smuggle.
- **Guaranteed vs residual (exact boundary)**: guaranteed — no byte is written unless gates
  passed at the last re-check under held locks; any pause/revoke/permission change committed
  before region entry blocks; any writer racing the region is ordered after its `COMMIT`;
  ambiguity always resolves to `unknown` with byte-identical-or-withhold recovery and no
  re-sign. Residual (not licensed, not silent): bytes emitted while a gate change commits
  concurrently with the handoff under a dissolved protection (dead session) — recorded by the
  post-handoff overlap re-read, never recallable, never repeatable with different bytes. This
  residual is a property of the fault model (independent DB/network failure, no DB↔net
  atomicity), not a permission window: unlike the withdrawn TTL, no committed revocation is
  ever knowingly overridden.
- **Decision — two concurrency timelines**:
  - (a) **Pause/revoke first → no delivery**: 006 pause tx `BEGIN → INSERT pause → COMMIT`; 009
    T-deliver then takes the gate-table `SHARE` lock (waiting up to the 5s guard if that tx is
    still open) and its in-tx read observes the pause → admission `blocked`, `COMMIT`, response
    status-only, zero signature bytes. If the pause tx outlives the guard, `LOCK TABLE` times out
    → `gate_read_failed`, fail-closed, retry same identity, still zero bytes. 007 revoke is the
    same shape through the grant `FOR SHARE` (revoke commits first → `revoked` read → blocked).
  - (b) **Admission first → pause/revoke after**: 009 T-deliver holds the scope-row + gate-table
    `SHARE` locks through the bytes write and `COMMIT`s after it; a concurrent pause/revoke tx's
    write waits for that commit, so it is ordered after the handoff. If the bounded write
    already returned, the bytes are in-flight approved and MUST NOT be claimed recallable;
    the `delivered` marker commits in the same transaction, so "approved" always has a durable
    record. A crash after the write but before `COMMIT` leaves no `delivered` marker → on retry
    the outcome is `unknown_reconcile`/`outcome_unknown` (bytes may be out: honest unknown),
    never a permit for a later ungated send, and the result is never re-signed.
- **Decision — unknown/commit-unknown recovery**: commit-unknown on submit/admission → do not
  claim signed/delivered; same-identity retry re-reads durable rows (bounded, no loop). A retry
  never inherits a prior admission's authority — it re-gates and re-admits.
- **Rationale**: OC-6/OC-7 rulings; FR-17/FR-23. The table lock is the common coordination basis
  the prior revision lacked; the admission row remains the verifiable artifact that makes
  "delivery admission ordered before/after pause/revocation" auditable.
- **Alternatives considered**:
  - `FOR SHARE` on 006's internal writer lease row — rejected: depends on that row existing and
    does **not** cover the manual-DBA release of `indexer_pause`/`log_pause` (no application lock).
  - "check then deliver in one long transaction" — rejected: the response write cannot be inside
    a DB transaction and the material must be persisted before return (FR-13).
  - "deliver then check" — rejected: violates OC-6 outright.
  - a 009-owned advisory lock that 006 does not take — rejected: zero cross-writer guarantee
    (false confidence is worse than an explicit protocol).

## R7 — Fee replacement: OC-5 conditional rule; carrier closure in R11

- **Decision — rule (OC-5 semantics, not reinterpreted)**: a fee-replacement signing request is a
  **new attempt**: new `attempt_id` + new `signing_request_id` (OC-4), same `intent_id` +
  `binding_ref`. Authorization use is **conditional**, exactly as OC-5 rules:
  1. if the original authorization **explicitly permits the fee-replacement purpose** and the
     replacement's fee is **within the authorized fee scope** → the same `authorization_id` MAY
     be reused (still a new request identity; the original request row is never modified/rebound);
  2. otherwise → the caller MUST first obtain a **new authorization** and use it with the new
     identity (a new `authorization_id`).
  The rule MUST NOT be flattened to "always fresh"; "always fresh" is only the operative path
  while the carrier cannot express the permission (gap D-3 / R11).
- **Decision — carrier (physical reuse must be possible)**: replace the over-tight
  `UNIQUE (authorization_id)` on `signing_requests` (which makes same-grant reuse physically
  impossible) with an anchor-plus-replacement shape:
  - `replacement_of BIGINT` nullable self-FK → `signing_requests(id)`, marking a replacement row;
  - partial unique index `UNIQUE (authorization_id) WHERE replacement_of IS NULL`, so a grant has
    at most one **anchor** (non-replacement) request, while replacement rows may name the same
    grant;
  - a replacement reusing the anchor's grant MUST declare the anchor's `intent_id` (and the same
    `binding_ref`); a fresh-grant replacement is its own anchor.
  **"旧请求换绑禁止" is preserved**: a persisted request row's `authorization_id` is never
  updated (retries reuse the same row); a replacement is always a new row/identity, never a
  rebind of the old row. Concurrent first uses of one grant serialize at the anchor index (loser
  → `23505` → `403 authorization_invalid`).
- **Decision — honest operative default**: the real 007 carrier has no purpose/fee-scope column,
  so "explicitly permits" is not verifiable from the row → branch (1) is currently **unreachable**
  and branch (2) (fresh authorization + new identity) is the operative path. This is a **recorded
  carrier gap** (R11/D-3), **not** acceptance of fresh-only as the OC-5 rule. Rejection of a reuse
  that cannot be verified: `403 authorization_invalid` with the fresh-authorization instruction.
- **Decision — version**: the carrier has no version column; the `authz:v1` fingerprint is a
  read-time **surrogate** and MUST NOT be presented as a version. OC-5 requires the request to
  persist authorization identity **and version**; the version carrier is part of the R11
  extension. Until it exists, 009 records the fingerprint as a surrogate and reports the gap; a
  request that cannot bind a version is not silently upgraded to "versioned".
- **Carrier gaps (explicit, MUST NOT be silently rewritten)**: 007 `withdrawal_authorizations`
  cannot express (a) intent linkage, (b) version, (c) fee-scope/purpose, (d) `revoked_at`; plus
  (e) sender scope and (f) cryptographic issuance authenticity. The per-attribute closure,
  concrete new carrier, owner, controlled write entry, consistency protocol, migration
  compatibility, and the two genuine business questions are all in **R11**. 009 MUST NOT add
  columns to 007's table, MUST NOT build a second authorization table, and MUST NOT present the
  fingerprint as a version it is not.
- **Rationale**: FR-05/OC-5 (conditional fee replacement; reuse of 007's approved carrier; gaps
  explicit), OC-4 (new attempt = new request identity).
- **Alternatives considered**:
  - unconditional reuse of the original grant — rejected: violates OC-5's explicit-permission
    requirement and lets one grant fund distinct signable objects without proof.
  - flatten to always-fresh — rejected: that is not OC-5; it silently drops the reuse branch and
    over-reads the carrier gap as a rule change.
  - add a purpose column to 007's table from 009 — rejected (forbidden to 009); the extension is
    007/011-owned (R11).

## R8 — 008 binding read: five-class consumption; only "exists and matches" signs

- **Decision**: 009 consumes 008's nonce binding exclusively through a read-only interface it
  defines on its side, `BindingReader`:

  ```go
  type BindingResult int
  const (
      BindingMatches BindingResult = iota // exists and matches intent/chain/sender/nonce
      BindingAbsent
      BindingConflict
      BindingPaused     // paused or reconciling
      BindingReadFailed // read failed / indeterminate
  )
  type BindingReader interface {
      ReadBinding(ctx context.Context, intentID, attemptID string) (BindingResult, error)
  }
  ```

  Only `BindingMatches` admits signing; every other class refuses and is audited with the class.
  009 MUST NOT trust caller-declared reservation claims, MUST NOT write/consume/modify the binding,
  and MUST persist the binding reference (`binding_ref`) it read as part of the request content
  set. Existence of a binding is **not** authorization (OC-5).
- **Rationale**: FR-18/OC-3 (008 owns the binding; explicit read-only contract with five classes),
  OC-6/OC-7 (paused/reconciling or read-failed MUST NOT produce a signature). Defining the
  interface (not the carrier) is the maximum 009 may decide while 008 designs its own storage
  (R4/R6: 008 artifacts are not visible/readable here).
- **Alternatives considered**:
  - 009 reading 008's table directly by guessed schema — rejected: would be 009 deciding 008's
    carrier and violates the parallel-work boundary.
  - a boolean interface (`IsValid`) — rejected: cannot distinguish absent vs conflict vs paused vs
    read-failure, which FR-18 explicitly requires to be distinct (only one class signs).
- **Integration boundary**: the concrete adapter waits for 008's contract (D3); contract-shape
  tests and a test caller that conforms to the OC-4 input contract keep 009 independently
  developable (R5). Test doubles do not substitute the final integration acceptance
  (constitution XI).

## R9 — Caller authentication & service interface: own credential family, internal HTTP, operator lifecycle

- **Decision**: 009 is a separate process (`txharbor signer-serve`) with its own listener
  (`TXHARBOR_SIGNER_HTTP_ADDR`, default non-default port `127.0.0.1:8091`), plain JSON over HTTP
  (no new protocol/framework). Authentication = repo-reviewed bearer-credential pattern,
  self-contained in 009's schema: `signer_caller` (stable identity, `can_sign` permission) +
  `signer_credential` (sha256(key) hex, full UNIQUE, `revoked_at`). Caller identity is derived
  from the credential row, never from the body; 401 is generic; credentials never appear in
  logs/errors (`logx.Redact` + allowlist). Lifecycle = `txharbor signer-auth
  <issue|rotate|revoke>` operator subcommand, mirroring the shipped `apikey-auth`/`confirm-auth`
  shape (flag parse → config.Load → one explicit operator transaction; operator + reason
  recorded). No HTTP admin surface; no new service or infrastructure.
- **Rationale**: FR-04/FR-05 (server-side identity, permission, revocation), constitution XIII
  (no new infra), repo precedent (007 R1–R5). 009 does not reuse 007's `caller`/`api_key` tables:
  its callers are the transaction-construction/broadcast side, not withdrawal API clients; sharing
  the namespace would couple two independently deployable boundaries and give 007's credential
  holder signing semantics it never had.
- **Own-requests-only surface**: submit returns the caller's own result; status is scoped to
  `(caller_id, signing_request_id)`; another caller's or nonexistent identity returns an identical
  `404` (007 privacy precedent), never a distinguishing error.
- **Alternatives considered**:
  - static env-carried tokens — rejected: no per-caller revocation without a process restart;
    unacceptable for a signing boundary.
  - reusing 007's credential tables — rejected above.
  - mTLS between business service and signer — deferred: adds PKI/rotation machinery without
    removing any threat this trust topology exhibits (both processes share the operator
    environment); the bearer pattern is revocable and testable locally. Revisit if deployment
    topology changes.
- **Transport**: status codes/field vocabulary are the plan-owned contract in `contracts/api.md`;
  this research only fixes the carrier family and the isolation boundary.

## R10 — Test-resource isolation: workdir-local PG database names + non-default ports

- **Decision**: 008 and 009 run in parallel worktrees over one machine (R4 warning:
  `compose.yaml` PostgreSQL/Anvil ports and the single `pgdata` volume are shared). 009 MUST NOT
  depend on shared mutable test resources:
  - **Database names are workdir-local**: any 009 run against a shared PostgreSQL server uses a
    dedicated database (e.g. `txharbor_009`), never the shared `txharbor` database; the 009
    migration (`000009`) is applied there. Integration tests keep the repo's per-test
    testcontainers shape (`postgres.Run` with a container-local database; random host port, no
    fixed published port), which is already collision-free.
  - **Ports are non-default**: the signer listener uses `TXHARBOR_SIGNER_HTTP_ADDR` with a
    non-default port (`127.0.0.1:8091` in examples) so it cannot collide with the probe listener
    (`127.0.0.1:8080`) or with 008's processes; quickstart examples use non-default ports
    explicitly. 009 has no RPC and needs **no Anvil** at all, removing the second shared resource
    from 009's test surface.
  - **No cross-worktree writes**: 009 tests never write to 006/007 tables beyond what the test
    fixtures own; fixtures for 006 pause/recovery rows and 007 grants are inserted in the
    workdir-local database only. Goose migration runs are serialized per database (goose's lock
    is per-DB), so a dedicated database also prevents migration interleaving with 008/other
    worktrees.
- **Rationale**: R4 (`docs/workflow-008-009-parallel.md`: "并行工作目录不等于测试资源隔离");
  constitution X (deterministic local testing) and XI (real-PG integration tests, no mocks for
  concurrency/recovery). Isolation is a prerequisite for trustworthy gate/race scenarios
  (V4/V6/V7) while 008 may be running its own integration suite.
- **Alternatives considered**:
  - reusing the shared `txharbor` database — rejected: concurrent migrations and fixture clashes
    would make gate results non-reproducible.
  - fixed published container ports — rejected: collides with any parallel worktree.
  - a 009-only compose stack — rejected: new infra surface (constitution XIII); a dedicated
    database + non-default port + testcontainers achieves isolation with zero new infra.

## R11 — Authorization carrier closure (OC-5 against the real 007 carrier)

Ground truth (verified in code, not assumed): `withdrawal_authorizations`
(`migrations/000007_withdrawal_creation.sql:118-139`) carries exactly
`authorization_id PK, caller_id FK→caller, chain_id, asset, recipient, amount,
state ∈ {active,revoked,expired}, expires_at, supplied_at, supplied_by` — **no fee column, no
request/intent direct link, no version, no `revoked_at`, no purpose/scope**. Supply is the
controlled operator entry `txharbor withdrawal-authz supply` (`internal/app/withdrawalauthz.go`;
identity = DSN trust + a declared `--operator` string recorded in `supplied_by` and
`withdrawal_grant_audit` — an *audit claim, not a cryptographic proof*). Revocation is
`RevokeGrant` (`FOR UPDATE` + `state='revoked'`). Validity is a Go predicate
(`state='active' AND (expires_at IS NULL OR expires_at > clock_timestamp())`) over a `FOR SHARE`
read (`internal/withdrawal/intake.go`). Request linkage is `withdrawal_requests.authorization_id
UNIQUE` (007 side). `caller.can_create` is the only interface permission, enforced on 007's POST
only.

Per-attribute closure (OC-5 attribute → carrier → 009 verification):

| OC-5 attribute | Carrier providing it | 009 verification |
|---|---|---|
| stable identity | `authorization_id` PK (real, 007) | row exists by id |
| issuing authority / authenticity | controlled `withdrawal-authz supply` entry + `supplied_by`/`withdrawal_grant_audit` (real, 007) | provenance = row + audit; **declared identity, not cryptographic** |
| single caller | `caller_id` FK (real, 007) | `= authenticated caller` |
| chain / asset / recipient / amount scope | columns (real, 007) | equality vs request bound fields |
| expiry | `expires_at` (real, 007) | `active` + `expires_at > clock_timestamp()` under `FOR SHARE` |
| revocation | `state` flip + grant-row lock (real, 007) | observed state; ordered by `FOR SHARE` (R6) |
| 007 request linkage | `withdrawal_requests.authorization_id UNIQUE` (real, 007) | 007-side bind; 009 cannot read intent from it |
| sender scope | **absent** in the grant; OC-2 registry/config + future 011 intent bind | registry policy + provider key match — not from the grant |
| fee scope | **absent** | not verifiable from the grant (policy caps only) |
| fee-replacement purpose | **absent** | not verifiable (R7 branch 1 unreachable) |
| intent / attempt / binding linkage | **absent** in the grant | 009 persists declarations; no carrier equality |
| authorization version | **absent** | fingerprint surrogate only; version binding unmet |

- **Applicability — first signatures need the carrier too (not only fee replacement)**: the
  table above is path-independent. A FIRST signature verifies the same attribute set
  (issuer/permission, request/intent/sender/content linkage, fee scope, expiry, revocation,
  version); the 007 row alone supplies only identity/caller/chain/asset/recipient/amount/state/
  expiry. Until the scopes carrier lands, first signatures operate in the same reduced mode as
  replacements: 007-columns-only verification + policy caps + fail-closed (`authorization_
  unverifiable`) on anything unverifiable (Q-B resolution). The carrier therefore unlocks
  COMPLETE delivery for both paths — the earlier "only unlocks the R7 reuse branch" phrasing is
  corrected here and in plan.md (Merge order). Independent development/merge of 008/009 with
  fail-closed behavior is allowed; complete delivery and integration acceptance are gated on the
  carrier batch below.

- **Concrete closure — new carrier, upstream-owned (009 does not build it)**:
  a 007/011-owned additive carrier `withdrawal_authorization_scopes` (1:1, PK
  `authorization_id`, FK → `withdrawal_authorizations`), written by the **same
  `withdrawal-authz supply` transaction** that supplies the grant (never by an ordinary caller,
  never by 009), carrying: `intent_id`, `request_id` (007), `sender`, `fee_scope` (max fee/tip or
  a range), `allows_fee_replacement` (explicit purpose token per OC-5), `authorization_version`
  (monotonic per grant), `attested_by`, and — only if Q-A rules so — an authorizer signature over
  `(authorization_id, authorization_version, fields)`. **Owner**: 007/011 (extension), never 009.
  **Controlled write entry**: the existing operator supply transaction, extended op-input;
  `RevokeGrant` extended to keep the scope row's state/version consistent. **Consistency
  protocol**: 009 reads the scope row read-only inside the same `FOR SHARE` read sequence (R6
  lock order), requires `authorization_id` equality and checks `sender`/`fee_scope`/
  `allows_fee_replacement`/`intent_id`/`request_id` against the request, persists
  `authorization_version` on the request, and re-checks version/fingerprint at delivery.
  **Minimal change scope / migration compatibility**: one additive migration (007/011); zero
  change to 007's existing columns, read shapes, or intake semantics; existing grants remain valid
  at the storage level (scope row optional), while 009 **fails closed** on a grant with no scope
  row/version (`authorization_unverifiable`) instead of silently accepting — a backfill is an
  operational step, not a semantic rewrite.
- **Genuine new business questions (listed, not self-decided)**:
  - **Q-A**: is the approved controlled-supply provenance (`--operator` declared, no
    cryptography) sufficient for OC-5 "可验证真实性", or must v1 require a cryptographic
    authorizer attestation in the scope carrier?
  - **Q-B**: for pre-extension grants without a scope row/version, does 009 fail closed
    permanently (`authorization_unverifiable`) or must a one-time backfill complete before 009
    accepts any grant?
- **Explicitly NOT substitutes**: the `authz:v1` fingerprint, the `caller_id` FK, and ordinary
  caller/API authentication are **not** authorization capabilities; none may be presented as
  satisfying the missing attributes above.

- **R11 resolutions (user ruling, 2026-09-16)**:
  - **Q-A → trusted-issuance control, no mandatory per-grant cryptography**: v1 does not
    require an authorizer signature on every grant. `--operator` stays a declaration/audit
    field only — never identity or authority by itself. Supply MUST be executed by an
    authenticated principal with issuance permission (reuse the real OS/deployment/DB
    permission system, with explicit control points and trust boundary); grant content and
    issuance audit MUST agree and be traceable, writable only through the controlled supply
    entry; 009 verifies provenance/content/validity against the trusted record, never against
    caller-supplied references or the operator string alone. If the existing supply path
    already satisfies this, tasks/implement cites the evidence and reuses it; otherwise the
    minimum permission control is added there. This does not retroactively negate 007's
    approved receiving semantics. The scope carrier ships WITHOUT a signature field unless a
    later ruling requires it.
  - **Q-B → per-grant refusal, no bulk backfill**: a grant without a trusted scope row/version
    is refused per request (`authorization_unverifiable`); scope is never inferred from history,
    config, or caller claims. New fully-conforming grants verify normally — no bulk backfill
    gates the service. No automatic backfill this phase; continuing historic business requires
    re-verification and explicit re-issuance by an authorized issuer, preserving traceability
    to the old grant/request. Re-issuance MUST NOT create a second intent or a second nonce,
    MUST reconcile existing intent/binding/attempt/signature state first, and MUST NOT
    silently rebind old requests or backfill history to fabricate original facts. Scopeless
    old grants stay queryable/auditable but never executable. Bulk-backfill as a precondition
    is cancelled; the legal path, controlled entry, refusal path, and recovery acceptance for
    new grants are still required and do not self-certify merge/deploy readiness.

## Open / deferred research items (carried into plan.md)

- **T000-P** stays open: production provider (KMS/HSM) selection is out of scope; R2 only promises
  interface shape.
- **D3 / R8**: 008's concrete binding read contract is not available in this worktree; the
  `BindingReader` interface is the 009-side consumption contract, and final integration
  verification waits for 008 (constitution XI; R5 of the parallel workflow). The R6 lock protocol
  adds one 008-side obligation: `ReadBinding` MUST be serialized against 008's pause transitions.
- **G-1 / R6 — CLOSED**: the 006 pause-establishment vs delivery window is closed in-plan by the
  gate-table `SHARE` lock (no 006 change); it is no longer an exposed gap or a deferred item.
- **D-1/D-2/D-4 / R7, R11**: intent linkage, authorization version, and `revoked_at` remain
  carrier gaps; the concrete closure carrier (`withdrawal_authorization_scopes`), owner 007/011,
  write entry, and consistency protocol are specified in R11.
- **D-3 / R7, R11**: fee-scope/purpose gap recorded; OC-5's conditional fee-replacement rule is
  restored, with the fresh-authorization branch operating until the R11 carrier lands.
- **Q-A / Q-B (R11)**: genuine business questions on authenticity strength and pre-extension
  fail-closed/backfill behavior — **decided by user 2026-09-16** (see R11 resolutions below).
