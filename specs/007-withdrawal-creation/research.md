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
progression Go+PostgreSQL+Anvil); **single `indexer_lease` FOR UPDATE discipline**,
no second lock object, no new lock order.

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
- **Rationale**: A per-request read of the local durable store is not an external call; the predicate
  makes revocation immediate with no invalidation problem. FR-11 and constitution forbid new infra.
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
- **Alternatives considered**: HTTP admin endpoint (rejected — new attack surface, unneeded);
  pgcrypto-side generation (rejected — crypto/encoding belong in Go per repo layout).

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

- **Decision**: `UNIQUE (authorization_id)`, `NOT NULL` (FR-03b requires an authorization per create;
  named so pgx can map it). Concurrent different-key binds serialize at the index: one wins, the
  loser gets 23505 on this constraint → 403 (FR-14 "不满足逐笔授权要求"; plan confirms 403 vs 409).
- **Rationale**: A constraint needs no lock ordering. Advisory locks are a second, session-scoped
  coordination primitive outside the repo's single-lock discipline (pgsql-general consensus: row
  locks/constraints over advisory locks for per-record races). `FOR UPDATE` on the auth row would
  add a second lock object and a lock-order-ring risk (explicitly forbidden).
- **Revocation-isolation option (only if strictness demands)**: `FOR SHARE` the auth row *only*
  (shared; acquired before any coordinator lock, never reversed), or route upstream auth
  supply/revocation writes through the same single `indexer_lease` FOR UPDATE lock for one total
  order with zero new locks. Default is constraints-only; plan locks this choice.
- **Alternatives considered**: advisory lock (rejected); auth-row `FOR UPDATE` (rejected as default);
  conditional `INSERT … SELECT … WHERE NOT EXISTS` (rejected — still needs the constraint; zero-row
  result is ambiguous and needs classification anyway).

## R8 — pgx 23505 mapping without TOCTOU; atomic tx shape (resolves FR-12/FR-13/FR-14)

- **Decision**: Map by `pgconn.PgError.ConstraintName` (`caller_key_uniq` → replay-or-409;
  `authorization_uniq` → 403-path), matching the repo's `isUniqueViolation` + `logscanner.go`
  constraint-name precedent. A 23505 **aborts the tx** — ROLLBACK (or SAVEPOINT), then classify on
  the pool; that post-commit-visibility read is authoritative, not TOCTOU. Explicitly handle
  classify-miss (winner not yet visible → retryable, never fabricated) and uncertain COMMIT
  (re-classify → replay/conflict/retryable), mirroring `reorgpolicy.go:353-366`.
- **Tx shape**: pre-tx opportunistic classify (fast path only) → `BEGIN` → `SET LOCAL
  statement_timeout` (shared `writeGuard`) → validate auth (same tx; `FOR SHARE` only if R7
  strictness chosen) → plain `INSERT` request → `INSERT` audit (same tx; repo precedent:
  DELETE + audit in one tx, `depositauth.go:1142-1159`) → `COMMIT` with
  `RowsAffected()==1` checks, 23505/commit-error handling per above.
- **Coordinator-lock choice** (locked in plan, §三.2): constraints-only is sufficient for FR-12/FR-13;
  add `ensureLeaseSQL` + `lockCoordSQL` (fixed order, coordinator row first) only to isolate the
  auth-validity read from concurrent revocation. The pre-tx read is never the guarantee.
