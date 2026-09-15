# Research: 007 Withdrawal Creation & Query

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15 | **Spec**: [spec.md](spec.md) (clarify 5/5, zero markers)

Phase 0 output. All technical unknowns resolved below; no NEEDS CLARIFICATION remains
(the five business decisions were closed in clarify Session 2026-09-15 and are inputs here,
not unknowns). Evidence base is read-only; two parallel research agents (API-key auth,
idempotency transactions) plus in-repo precedent inspection. No code modified.

Evidence base (read-only):
- Upstream designs: `specs/002-chain-indexer/data-model.md`, `specs/003-event-indexing/data-model.md`,
  `specs/004-deposit-detection/data-model.md` (incl. depositauth protocols),
  `specs/005-confirmation-tracking/data-model.md` (228 lines incl. submit/switch protocols),
  `specs/006-reorg-recovery/` research/data-model/contracts (downstream.md, observability.md).
- Code mechanics: `internal/app/serve.go` (serve lifecycle), `internal/indexer/scanner.go`
  (writeGuard/ensureLeaseSQL/lockCoordSQL), `depositauth.go` (classify/resolveAuthRace/audit),
  `reorgpolicy.go` (full tx protocol), `logscanner.go` (constraint-name mapping),
  `internal/logx/redact.go` (bearer redaction), `internal/metrics/metrics.go` (prometheus registry),
  `internal/config/config.go` (env-only), `migrations/000001`–`000006`, `compose.yaml`
  (postgres:18.6-trixie, Anvil chain-31337), `go.mod` (Go 1.26.5, pgx v5, goose).
- External: PostgreSQL 18 docs, UPSERT wiki, PG19 DO SELECT (Neon), Brandur/Stripe idempotency,
  coder/coder + Gitea + Grafana API-key implementations, pgsql-general advisory-lock guidance.

Repo pins that bind every decision: **postgres:18.6-trixie** (so PG19 `ON CONFLICT DO SELECT`
is unavailable — recorded as future simplification only); **pgx v5.11.0**
(`pgconn.PgError.Code`/`ConstraintName`); **no new infra** (007 Non-Goals; constitution
progression Go+PostgreSQL+Anvil); 007 intake uses exactly one lock object (grant-row
`FOR SHARE`, R7 FINAL) and never touches the `indexer_lease` discipline — no new lock order.

## R1 — Credential storage: deterministic SHA-256, UNIQUE-indexed (resolves auth storage)

- **Decision**: API keys are server-issued 32-byte CSPRNG secrets (`txh_` typed prefix, base64url).
  Store only `sha256(key)` as 64-char lowercase hex (`TEXT CHECK ~ '^[0-9a-f]{64}$'`,
  matching the repo's hash-column convention) behind `UNIQUE (key_hash)`. Lookup is
  `WHERE key_hash = sha256(presented)` → single-row read, then `subtle.ConstantTimeCompare`
  before accepting. Zero new Go deps (`crypto/sha256`, `crypto/subtle`, `crypto/rand` are stdlib).
- **Rationale**: Keys are high-entropy, so slow KDFs (bcrypt/argon2, ~65–100 ms each) buy nothing
  and are unindexable (per-row salt ⇒ compute-then-`WHERE` is impossible; must scan). A salted hash
  (Gitea PBKDF2 ×10000) forces prefix-narrow + candidate-scan (`O(candidates)`). A deterministic
  digest gives `O(1)` with no plaintext at rest. This matches coder/coder
  (`coderd/apikey/apikey.go`: `HashSecret` = sha256, `ValidateHash` = constant-time compare)
  and the dominant Go production shape (`WHERE key_hash = $1`).
- **Alternatives considered**:
  - bcrypt/argon2: rejected — hot-path ceiling + unindexable; wrong tool for high-entropy keys.
  - PBKDF2+salt: rejected — needs candidate scan; Gitea tolerates it only at tiny token counts.
  - HMAC-SHA256 + pepper (`TXHARBOR_API_KEY_PEPPER`): deferred — adds a required env secret whose
    rotation invalidates every stored hash (needs `pepper_version` + dual-verify). Documented upgrade
    path only; 007 ships plain SHA-256.
- **Full (not partial) UNIQUE**: with FR-11 permanent retention, revoked rows stay; full uniqueness
  guarantees a revoked/compromised key hash can never be re-registered (a
  `WHERE revoked_at IS NULL` partial unique would allow resurrection).

## R2 — Lookup flow: digest is the lookup key; prefix is display-only (resolves auth lookup)

- **Decision**: Hash the whole presented key; hit the unique index. Store a short `key_prefix`
  for operator audit/identification only — never for authentication (no prefix-narrow scan).
- **Rationale**: Deterministic digest ⇒ no lookup-time plaintext, no table scan. The prefix exists
  because the secret MUST NOT enter logs/repo/errors (FR-20); audit rows carry `key_id` + prefix.
- **Alternatives considered**: Gitea `token_last_eight` narrow-then-scan (rejected — price of a
  non-deterministic hash; unnecessary here).

## R3 — key_id vs caller_id: two tables; rotation inserts, identity never mutates (resolves FR-03/FR-10)

- **Decision**: `caller` (stable, non-secret business identity; `withdrawals.caller_id` FK → it)
  plus `api_key` (`key_id`, `caller_id` FK, `key_hash`, prefix, timestamps). Rotation = INSERT new
  `api_key` with the same `caller_id`, then revoke the old row. `caller_id` is never encoded in,
  derived from, or changed by the key. Idempotency scope `(caller_id, idempotency_key)` uses
  `caller_id` — never `key_id` (FR-10 forbids key material in comparison).
- **Rationale**: Mirrors coder (`ID` credential vs `UserID` identity; token `keyID-secret`) and Gitea
  (`ID` vs `UID`; `RegenerateAccessToken` keeps `ID`). Rotation/revocation leave historical query
  authorization (FR-03) and idempotency scope (FR-10) untouched, satisfying "轮换前后同一 caller_id".
- **Alternatives considered**: key_id-as-identity (rejected — rotation would change dedup scope);
  single table (rejected — identity would die with credential revocation).

## R4 — Revocation: same-query predicate, no cache/Redis/external call (resolves FR-03)

- **Decision**: `revoked_at` predicate inside the single auth lookup:
  `WHERE key_hash = $1 AND (revoked_at IS NULL OR revoked_at > now())`.
  Immediate revocation writes `now()`; rotation grace writes a future timestamp (dual-accept window).
  No cache, no Redis, no revocation service.
- **Rationale**: A per-request read of the local durable store is not an external call. The precise
  boundary (二轮定向修正 — "immediate" withdrawn as a whole-request claim): the predicate is
  evaluated at request start (auth step (1) of contracts/api.md §2); a revocation committing
  *before* that evaluation is observed (401/403, zero effect); a revocation committing *after*
  that evaluation does not abort the in-flight request — the request completes under the
  startpoint verdict. Zero cache narrows the staleness window to the request's own lifetime; it
  does not make revocation "instant for the whole request". Replay path re-evaluates steps (1)–(2)
  per attempt, so a revoked key fails the *next* attempt's startpoint check. No whole-request lock
  is added (Q1/Q5 approved no such mechanism); the boundary above is stated, not strengthened.
  Every cached-verification design trades revocation latency for throughput (Gitea re-reads the row
  on LRU hit precisely to catch deletion; Ory Talos documents TTL-window acceptance on other
  instances) — at operator-issued key cardinality a cache is a liability, not an optimization.
- **Alternatives considered**: in-process LRU with TTL (rejected — weakens FR-03 unless every hit
  re-reads, which removes the benefit); Redis denylist (rejected — new infra, forbidden).

## R5 — Ops entry point & secret hygiene (resolves FR-03 operability, no admin UI)

- **Decision**: Issuance/rotation/revocation = operator tooling, not HTTP. Reuse the T024 precedent
  (`depositauth.go`: migrations stay pure DDL; controlled, versioned operator path). Key generation
  in Go (CSPRNG + encoding are test-locked Go concerns); show plaintext once; persist digest only.
  `internal/logx.Redact` already covers `Authorization: Bearer …` (`redact.go` + bearer tests);
  the middleware MUST log only `caller_id` + `key_id` + prefix, and 401s stay generic.
  `.env.example` carries no real/seed key.
- **Key entropy premise (locked 2026-09-15定向收尾)**: 32 bytes from `crypto/rand`, base64url,
  `txh_` typed prefix. The SHA-256-only decision (R1) is valid *only* under this premise —
  server-issued high-entropy secrets. Tooling MUST generate (never accept caller-supplied key
  material), MUST show plaintext once, MUST persist digest only. Key rotation/revocation ride the
  same operator-subcommand family as R9 (`internal/app` shape: flag parsing → config.Load →
  operator-connection tx; operator + reason recorded; exit codes 0/1/2 mirroring `confirm-auth`).
- **Alternatives considered**: HTTP admin endpoint (rejected — new attack surface, unneeded);
  pgcrypto-side generation (rejected — crypto/encoding belong in Go per repo layout).

## R9 — Authorization supply entry: `withdrawal-authz` operator subcommand (resolves FR-03b operability)

**FINAL (locked 2026-09-15定向收尾).** Carrier: `txharbor withdrawal-authz <supply|revoke> …`
operator subcommand, mirroring the shipped `confirm-auth` shape
(`internal/app/confirmauth.go:16-35`): `flag` parsing → `config.Load` for the DSN (full serve-env
validation, run with the serve env file) → `pgxpool` connect over the DB operator's connection →
one explicit BEGIN..COMMIT of repository-owned parameterized SQL. NOT a stored function (T024
rationale, `depositauth.go:1-32`: failure-injection needs statement control; no second-language
identity logic; migrations stay pure DDL; reviewed artifact = executed artifact). No new service,
endpoint, or infrastructure; upstream is NOT required to hold a DB connection as a default premise
— the DB operator runs this binary with DSN access (same trust as `migrate` and `confirm-auth`).

- **Who calls / trusted identity**: the DB operator (human or upstream-driven runbook step) invoking
  the binary with DSN access. `--operator` is recorded verbatim as the declared supply identity in
  `supplied_by` + audit rows; it is an audit claim, not a cryptographic proof — the trust root is
  DSN possession, identical to every existing privileged path.
- **Who can create/revoke; why API callers cannot**: only this subcommand issues the supply
  statements — by *operational rule backed by credential separation*, stated precisely:
  (a) ordinary withdrawal API callers hold no DB credential at all (HTTP only), so they
  *technically cannot* reach the database through any application path; (b) no PostgreSQL
  role-level restriction is introduced in 007 (single DSN role, same as all existing paths —
  recorded here so no later step miscites a role barrier that does not exist); (c) anyone who
  *does* hold the DSN (DB operator, or an attacker who stole it) *can* technically issue raw SQL,
  which is why the "bare `psql` INSERT is forbidden" rule is an operational/review discipline
  (same as `confirm-auth`), not a technical impossibility — guards live inside the reviewed
  function, and this subcommand is the only *reviewed* binary path that executes them.
  `--operator` is a declared audit identity, not a verified identity; the trust root is DSN
  possession, identical to every existing privileged path.
- **Actions** (full per-action tx in data-model Table 6 attempt semantics + supply pseudocode):
  `withdrawal-authz mint` (standalone; prints one opaque id; caller durably captures BEFORE any
  supply/revoke) then `supply --operation-id O --authorization-id G --caller-id C --chain-id N
  --asset 0x… --recipient 0x… --amount D [--expires-at T] --operator OP --reason R` and
  `revoke --operation-id O --authorization-id G --operator OP --reason R`.
  `--operation-id` is REQUIRED on supply/revoke (no auto-mint, no echo-fallback — both DELETED
  六轮定点: print ≠ saved, and echo-loss left an unrecoverable O).
  First-supply miss-branch: `SELECT … FOR UPDATE` on a missing row locks nothing, so concurrent
  first-supplies serialize on the grant PK; the loser gets 23505 on `withdrawal_authorizations_pkey`
  → rollback → read-only re-read → equal ⇒ resupply-audit with SAME O (one bounded re-execution),
  differ ⇒ `supply_refused`-audit. Grant-PK conflict is NEVER `operation_conflict` and NEVER
  503-unavailable. Same attempt retried (same O + same op-input) converges via
  `UNIQUE (operation_id)`; same O + any differ ⇒ `operation_conflict`; a NEW attempt MUST mint
  a new O — grant state is never reverse-derived into identity. Already-bound requests
  keep their rows (revocation affects only not-yet-accepted receipts per FR-03b).
- **Field/param validation**: identical validators as intake (`validate.go` shared): FR-06 amount,
  FR-07 addresses, FR-04 chain bind, caller existence; `expires_at` must be future if given.
- **Atomicity**: supply/revoke + its audit row commit in one tx; per-action `RowsAffected`
  expectations in Table 6 (grant-INSERT `==1` on first supply, `==0`-assert on equal re-supply;
  audit-INSERT `==1` per NEW attempt). Uncertain COMMIT ⇒ retry with the SAME O and SAME op-input
  (O-miss ⇒ unknown/retryable with same O; "missing both ⇒ new O" DELETED — a missing audit row
  never proves rollback). Outcome basis is the audit row matching O + op-input; grant state is
  current-state info only (covers: grant pre-exists + this-attempt-refused ⇒ refusal, never
  business success).
- **Ops/acceptance**: runbook lines in quickstart V3/V7; integration tests drive the subcommand
  function in-process (like `confirmauth_test.go`) against real PostgreSQL — no shell-out.
- **"007 只读验授权" boundary clarified**: request-handling paths (`intake.go`, `query.go`) never
  write Table 4; the supply/revoke entry above plus its audit is owned by `grant.go` + this
  subcommand. Upstream keeps business-approval and balance-reservation responsibility (FR-18);
  this entry is the controlled, auditable handoff — not an approval engine.

## R6 — Insert-first, no ON CONFLICT for the request path (resolves FR-10/FR-12/FR-14)

- **Decision**: Plain `INSERT` (no `ON CONFLICT`) → catch `23505` → ROLLBACK → classify SELECT →
  compare FR-10 param set → 200 replay / 409 conflict / 403-or-409 auth-bound.
  This is the only correct shape on PG18 for "replay returns original / mismatch errors".
- **Rationale**:
  - `SELECT`-first never removes the race (the UNIQUE constraint stays the decider); it is a wasted
    round trip that must still handle 23505.
  - `ON CONFLICT DO NOTHING RETURNING` returns **nothing** for conflicting rows (PG docs; PG wiki
    UPSERT); `DO UPDATE … WHERE false` likewise returns nothing while taking locks. Neither yields
    the original for free; the classify SELECT is needed either way.
  - `DO UPDATE … RETURNING` always writes (dead tuples, no insert-vs-existing signal).
  - `ON CONFLICT DO SELECT` (PG19) would solve it in one statement — unavailable on the 18.6 pin.
  - **Trap recorded**: untargeted `ON CONFLICT DO NOTHING` absorbs *all* usable unique violations
    (PG docs), so an authorization-binding violation would be silently misreported as a key replay.
    If ON CONFLICT is ever used it MUST name the arbiter `(caller_id, idempotency_key)`.
  - The repo already runs this exact protocol (`resolveAuthRace`, `depositauth.go:1109-1136`:
    23505 → rollback → `classifyAuthRequest` → equal/differ/miss).
- **Alternatives considered**: see table above; insert-first is also what Stripe/Brandur prescribe
  (pre-read is an optimization, the constraint is the authority).

## R7 — One authorization → one request via constraint, not lock (resolves FR-03b)

**FINAL (locked 2026-09-15定向收尾; corrected 二轮定向): constraints + one shared row lock.
No coordinator lock, no advisory lock, no `FOR UPDATE` anywhere on the intake path.** The single
coordination primitive on this path is `FOR SHARE` on the one grant row being consumed (see
invariant table). Tasks/implement MUST NOT re-decide, add a global lock, or drop the share lock.

- **Carrier**: `UNIQUE (authorization_id)`, `NOT NULL` (FR-03b requires an authorization per create;
  named so pgx can map it). Concurrent different-key binds serialize at the index: one wins, the
  loser gets 23505 on this constraint → 403 (FR-14 "不满足逐笔授权要求"; locked here as 403,
  not 409 — a bound grant means the caller lacks a *usable* grant for a new request).
- **Rationale**: Uniqueness needs no lock ordering (both UNIQUEs), but grant *validity* does
  need one row-level coordination — a plain in-tx re-read is insufficient: under READ COMMITTED
  each statement takes its own snapshot, so a revoke committing after the validity SELECT but
  before the INSERT is visible to the INSERT, and the "same snapshot" claim in the previous
  revision was wrong (corrected here; it is NOT retained). The minimal provable carrier is
  `SELECT … FOR SHARE` on the single grant row: shared locks do not block each other, so
  concurrent receipts against *different* grants (and re-reads of the same grant) proceed in
  parallel; only a grant-state writer (`revoke`, which takes `FOR UPDATE`/UPDATE on that row)
  serializes against holders. There is exactly one lock object on this path, acquired at one
  fixed point (step 3 of Ordering) — with no second lock anywhere, no lock-order ring can form.
  Advisory locks remain rejected (session-scoped second primitive). `FOR UPDATE` on the grant row
  remains rejected (would serialize concurrent receipts against the same grant for no reason).
  The coordinator `indexer_lease` lock remains rejected for intake (serializes API creates against
  indexer loops for zero additional guarantee).
- **Invariant coverage (what proves what)**:
  | Invariant | Carrier | Why sufficient |
  |---|---|---|
  | 同 caller 同键至多一请求 | `UNIQUE (caller_id, idempotency_key)` index insert | atomic + serializing by construction |
  | 同一授权跨不同键至多一请求 | `UNIQUE (authorization_id)` index insert | same; loser maps to 403 |
  | 授权在接收时有效 (state/params/expiry/caller-bind) | `FOR SHARE` on the grant row + validity SELECT + param equality, all before the INSERT in one tx | share lock held to COMMIT ⇒ a concurrent revoke (`FOR UPDATE`/UPDATE on that row) either commits *before* the lock is granted (read sees revoked → 403, zero rows) or blocks *until* this tx commits (receipt stands, revoke applies to later receipts only). No third outcome. Linearization point: the grant-row lock acquisition ordered against the revoke's row write |
  | 调用方权限/密钥吊销 | per-request auth lookup (`api_key` predicate, R4) evaluated at request start (startpoint semantics: pre-startpoint revokes observed; post-startpoint revokes affect next attempt, not the in-flight one) | every attempt re-reads; no cache |
  | 响应丢失/提交未知后不重建 | post-rollback classify against durable rows (R8) | winner visibility decides; miss → retryable |
- **Ordering (locked)**: pre-tx classify (fast path only) → BEGIN → writeGuard →
  `SELECT grant row … FOR SHARE` + validity check (403 + ROLLBACK on fail) → plain INSERT →
  audit INSERT → COMMIT. No `ensureLeaseSQL`/`lockCoordSQL` on this path. 23505 handling per R8
  (rollback-then-classify; dual-constraint race below).
- **Full interleave proof (receive vs revoke, Q2 rule: "撤销对尚未接收请求生效、已 Accepted 不隐式取消")**:
  Let R = receipt tx (lock grant → validate → INSERT request → COMMIT), V = revoke tx
  (lock grant `FOR UPDATE` → set `revoked` → COMMIT). "接收" completes at R-COMMIT.
  Case A (V-COMMIT before R-lock): R reads `revoked` → 403, zero rows. Revocation effective. ✓
  Case B (R-COMMIT before V-lock): V blocks until R commits, then revokes; the Accepted row
  stands, never implicitly cancelled; later replays return 200 (Q5). Revocation affects only
  later receipts. ✓
  Case C (interleaved: R holds share, V waits): V cannot commit mid-R; R's INSERT + COMMIT decide
  first; then V commits. Identical outcome to Case B, deterministically — the "快照后撤销"
  hole of the previous revision is closed by the lock, not by snapshot wishful thinking. ✓
  No outcome creates a second request for one grant (`authorization_uniq`), and no outcome
  cancels an Accepted row. QED against the Q2 rule with no rule relaxation.
- **Alternatives closed**: advisory lock (rejected); auth-row `FOR UPDATE` (rejected);
  coordinator-lock intake (rejected — cost without coverage); conditional
  `INSERT … SELECT … WHERE NOT EXISTS` (rejected — still needs the constraint; zero-row
  result is ambiguous and needs classification anyway).

## R8 — pgx 23505 mapping without TOCTOU; atomic tx shape (resolves FR-12/FR-13/FR-14)

- **Decision**: Map by `pgconn.PgError.ConstraintName` (`caller_key_uniq` → replay-or-409;
  `authorization_uniq` → 403-path), matching the repo's `isUniqueViolation` + `logscanner.go`
  constraint-name precedent. A 23505 **aborts the tx** — ROLLBACK (or SAVEPOINT), then classify on
  the pool; that post-commit-visibility read is authoritative, not TOCTOU. Explicitly handle
  classify-miss (winner not yet visible → retryable, never fabricated) and uncertain COMMIT
  (re-classify → replay/conflict/retryable), mirroring `reorgpolicy.go:353-366`.
- **Tx shape (locked)**: pre-tx opportunistic classify (fast path only) → `BEGIN` → `SET LOCAL
  statement_timeout = '5s'` (shared `writeGuard`, `scanner.go:1084-1085` — a per-statement timeout
  ONLY; it is not a recovery-pause gate and never refuses commits: 007 persists compliant receipts
  during active recovery per FR-16, and writeGuard applies identically inside and outside recovery) →
  `SELECT grant row … FOR SHARE` + validity check (R7 FINAL, sole lock on this path) → plain
  `INSERT` request → `INSERT` audit (same tx; repo precedent: DELETE + audit in one tx,
  `depositauth.go:1142-1159`) → `COMMIT` with `RowsAffected()==1` checks, 23505/commit-error
  handling per above.
- **Grant expiry clock and evaluation point (locked §三; corrected 四轮定点)**: DB time stays the
  only clock for validity, but `now()`/`transaction_timestamp()` is FIXED at transaction start
  (PG docs) — it MUST NOT back the post-lock check. Protocol: (1) `SELECT grant row … FOR SHARE`
  (blocks until granted; the returned row proves state but NOT current time); (2) a SEPARATE
  statement `SELECT clock_timestamp()` — evaluated when executed, i.e. after the lock wait —
  binds `t_check`; (3) validity decided as `state='active' AND (expires_at IS NULL OR
  expires_at > t_check)` in a third statement (or client-side on the two result sets — same
  values, no earlier snapshot reused). `clock_timestamp()` MUST NOT be folded into the locking
  SELECT's target list as proof of post-lock time: expressions may evaluate before the lock wait.
  Boundary: `expires_at = t_check` is expired (`>` strict, locked here). Counter-example closed:
  tx-start-valid → lock wait → expiry passes mid-wait → post-lock `clock_timestamp()` reads past
  `expires_at` → 403, zero request rows. Q2 ("到期…对尚未接收请求生效") is decided at this
  post-lock check — the last check before the INSERT.
- **Timeout scope correction (四轮定点)**: `statement_timeout='5s'` bounds EACH statement only; it
  does NOT bound total tx time or inter-statement stalls. The previous "microseconds-to-ms under
  writeGuard" promise is withdrawn. No request/tx-level deadline mechanism exists in the repo
  today (no `idle_in_transaction_session_timeout`, no statement-count budget — recorded so no
  later step invents one); post-check pre-COMMIT expiry therefore has no time bound CLAIMED — it
  is accepted as residual (bounded in practice to a handful of indexed writes, unbounded in
  theory). Lock-wait itself IS bounded per-statement (the waiting SELECT dies at 5s → 503
  retryable).
- **Waiting and tx-error handling (§二后半; narrowed 四轮定点)**: the receipt path has TWO
  characterized wait edges — (a) `FOR SHARE` queuing behind a revoke's row write, (b) UNIQUE
  index arbitration between concurrent inserters. This enumerates *this path's* waits; it is NOT
  a whole-DB claim. Failure mapping (locked): per-statement timeout (`statement_timeout`, incl.
  lock-wait expiry) → ROLLBACK → 503 retryable; deadlock `40P01` → ROLLBACK → 503 retryable
  (deadlock is possible in principle wherever two txns touch the same rows in overlapping order —
  receipt takes grant-share-then-request-insert while revoke takes grant-update; the window is
  narrow but NOT proven absent, so the handler is specified rather than the impossibility);
  `23505` → ROLLBACK → fixed-order classify (R8; the ONLY non-rollback diagnostic read allowed —
  any other post-error statement in the aborted tx is forbidden); COMMIT error → dual re-read
  (R8/Table 6). No wait failure is ever mapped to 23505 semantics, and no 23505 is ever mapped
  to a wait/timeout. Full-DB lock auditing is explicitly out of scope.
- **Dual-constraint race (locked semantics)**: one INSERT can violate both UNIQUEs at once, but
  PostgreSQL reports exactly one `ConstraintName`. Order of report is NOT a semantic signal and
  MUST NOT decide the response. Rule: after any 23505, rollback, then classify **deterministically
  in fixed order** — (1) read by `(caller_id, idempotency_key)`: hit + FR-10 equality → 200;
  hit + inequality → 409 (authoritative for this key regardless of what else fired); (2) only on
  key-miss, read by `authorization_id`: hit (bound to another request) → 403; (3) miss both →
  retryable. A 409 for this key is never downgraded to 403 by a co-fired auth violation, and a
  same-key replay is never upgraded to 409/403.
- **Replay immunity to later grant state (locked)**: the replay path (classify hit + FR-10
  equality) checks current key-auth + interface permission only; it MUST NOT re-validate the
  original grant row. Grant expiry/revocation after accept changes nothing for replays (Q5).
