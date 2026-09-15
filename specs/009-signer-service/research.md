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
  match the corresponding request/declared fields. `FOR SHARE` is the *only* lock object on 009's
  signing path and it matches the 007 receipt-path discipline (007 R7: revoke takes
  `FOR UPDATE`/UPDATE on that row; share locks do not block each other).
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

## R6 — Delivery admission ordering + EXPOSED GAP (006 pause establishment has no shared lock)

- **Decision — ordering**: signature material is delivered only through a recorded **delivery
  admission**, on the first response and on every same-identity retry alike (OC-6). One attempt:
  1. Open a short transaction; in one read sequence re-run the 006 gate (pause + recovery +
     **current version equality vs the stored `recovery_version`**) and re-read the 007 grant
     `FOR SHARE` (state/expiry/fingerprint equality);
  2. Insert one `delivery_admissions` row recording the observation snapshot and verdict
     (`admitted` before any bytes are written; `blocked` on any gate refusal) and `COMMIT`;
  3. Only then write the response bytes; after the write, a bounded best-effort marker updates the
     row to `delivered`. A crash between (2) and (3) leaves `admitted` — the material was cleared
     for delivery and is treated as in-flight approved (`in_flight_approved`), never as "not
     delivered"; a same-identity retry re-runs the full gate (it may re-deliver the *same bytes*
     after passing, or return blocked status-only).
  Gate failure at delivery → **no signature material** in any field of the response: desensitized
  status only (`state`, `content_hash`, refusal class/reason, timestamps; no signature bytes, no
  `tx_hash` unless a prior admission is recorded as `delivered`/`admitted`). This is the explicit
  inversion of the 007 still-200 replay (OC-6: MUST NOT 类比 still-200).
- **Decision — races that ARE closable**:
  - 007 revocation: the delivery path holds `FOR SHARE` on the grant row (step 1) while the revoke
    path takes `FOR UPDATE`/UPDATE on the same row; a revoke either commits before the lock is
    granted (delivery sees `revoked` → blocked) or blocks until the admission commits (delivery
    stands; revoke applies to later attempts). Linearization point: the grant-row lock.
  - expiry: evaluated on the DB clock inside the same read sequence; a grant expiring after the
    grant-row read is a time boundary, not a lock boundary — the admission records the observed
    `expires_at`, and the next delivery attempt re-evaluates from scratch (no persistent permit).
- **Decision — EXPOSED GAP (recorded, not silently patched)**: 006 **establishes** a pause by
  `INSERT` into the pause table (and recovery rows by 006-owned transactions) and takes no lock
  that 009 can share; 009's one-read-sequence check and its response write are not atomic with
  that INSERT. A pause committing after 009's last read but before the response bytes leave the
  process is therefore not observable by 009, and **no 009-side mechanism can close it**:
  - `FOR SHARE` on the 007 grant row does not help — pause establishment never touches grant rows
    (that lock closes the *revocation* race, not the pause race);
  - adding a 009-side lock (advisory/table) only helps if 006's INSERT takes the same lock, which
    is a 006 contract change (out of 009's scope; 009 MUST NOT modify 006 governance).
  Consequence and ownership: 009 records the admitted snapshot and its verdict (the "明确且可验证
  的先后顺序" required by FR-23 is the *admission-commit → response-write* order plus the recorded
  observation), treats an admitted delivery as in-flight approved and not reclaimable (OC-7), and
  refuses all *future* attempts once the pause is visible. The residual window is **closable only
  by a 006-side contract change** (e.g. pause establishment taking a lock that downstream gate
  readers also take); that extension is deferred to a future 006 amendment or covered downstream
  by the 010 broadcast gate, which re-verifies recovery itself before broadcasting. This item is
  reported as an EXPOSED GAP in plan.md (deferred item G-1), not as a solved problem.
- **Rationale**: OC-6/OC-7 rulings; FR-17/FR-23. The admission row is the verifiable artifact
  that makes "delivery admission decided before pause/revocation" auditable after the fact.
- **Alternatives considered**:
  - "check then deliver in one long transaction" — rejected: the response write cannot be inside a
    DB transaction, and the material would not be persisted before return (FR-13).
  - "deliver then check" — rejected: violates OC-6 outright.
  - blocking on a 009-owned advisory lock that 006 does not take — rejected: it would look like a
    fix while providing zero cross-writer guarantee (false confidence is worse than a recorded gap).

## R7 — Fee replacement: fresh-authorization rule; 007 carrier gaps recorded, extension owned by 007/011

- **Decision**: a fee-replacement signing request is a **new attempt**: new `attempt_id` +
  new `signing_request_id`, same `intent_id`/`binding_ref`, and it MUST carry a **different
  `authorization_id`** than the attempt it replaces (fresh authorization + fresh identity per
  FR-05). Structural carrier: `signing_requests` carries `UNIQUE (authorization_id)` — one grant
  backs at most one signing-request identity, so a replacement cannot silently reuse the consumed
  grant; retries reuse the *same* request identity and therefore the same row (no new insert, no
  consumption, no extension of the authorization — FR-05 "重试 MUST NOT 消耗或延长授权").
  Rejection of a replacement that names an already-bound grant: `403 authorization_invalid` with
  the fresh-authorization instruction.
- **Authorization fingerprint** (the version surrogate): the grant row read under `FOR SHARE`
  stores, on the request row, `authorization_id` + a deterministic fingerprint of the observed
  fields (`caller_id, chain_id, asset, recipient, amount, state, expires_at`), versioned
  (`authz:v1`). The carrier has **no version column** and no `revoked_at` (revocation is a
  `state` flip), so the fingerprint + observed state at read time is the strongest bind 009 can
  honestly claim; delivery re-reads and MUST match the fingerprint to deliver.
- **Carrier gaps (explicit, MUST NOT be silently rewritten)** — 007 `withdrawal_authorizations`
  cannot express:
  1. **intent linkage**: no `intent_id`/`request_id` column → 009 cannot verify
     request→intent→binding→attempt→request association *from the carrier*; 009 persists the
     caller-declared `intent_id` and verifies content/binding consistency plus grant field
     equality. Full intent-linkage verification is owned by 011/007 extension.
  2. **version**: no version/sequence column → no "authorization version" can be compared; the
     fingerprint is the surrogate (must not be presented as a version).
  3. **fee-replacement purpose/scope**: no purpose/action/fee-scope column → "该授权显式允许费用
     替换用途" cannot be verified from the row. 009's rule therefore treats a *fresh
     authorization* as the only acceptable basis for replacement; whether that fresh grant
     "explicitly allows" replacement remains unverifiable until 007/011 extend the carrier.
  4. **revocation timestamp**: no `revoked_at` → revocation ordering is observed only as
     `state='revoked'`; the observed state is bound in the fingerprint.
  These gaps are recorded here and in plan.md (deferred items D-1..D-4) with owner 007/011; 009
  MUST NOT add columns to 007's table, MUST NOT invent a second authorization table, and MUST NOT
  present the fingerprint as a version it is not.
- **Rationale**: FR-05 (fresh authorization for fee replacement; reuse of 007's approved carrier;
  gaps explicit), OC-4 (new attempt = new request identity).
- **Alternatives considered**:
  - reuse the original grant for a replacement — rejected: violates the explicit-purpose rule and
    makes one grant fund two distinct signable objects.
  - add a purpose column to 007's table — rejected: forbidden (must not silently rewrite 007);
    extension is 007/011-owned.
  - treat every same-intent request as a replacement and refuse all — rejected: replacement is a
    legitimate path; the fresh-grant rule makes it expressible without new columns.

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

## Open / deferred research items (carried into plan.md)

- **T000-P** stays open: production provider (KMS/HSM) selection is out of scope; R2 only promises
  interface shape.
- **D3 / R8**: 008's concrete binding read contract is not available in this worktree; the
  `BindingReader` interface is the 009-side consumption contract, and final integration
  verification waits for 008 (constitution XI; R5 of the parallel workflow).
- **G-1 / R6**: the 006 pause-establishment vs delivery window is an EXPOSED GAP; closing it is a
  006-side contract change, not a 009 decision.
- **D-1..D-4 / R7**: 007 authorization carrier gaps (intent linkage, version, fee purpose,
  revoked_at) are recorded; extension is owned by 007/011.
