# Contract: 008 → 009 Nonce Binding Read API (provider side)

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md)
(OC-3/OC-4/OC-5/OC-6/OC-7; Downstream Handoff 009) | **Design**: research R8/R10, data-model
Tables 3/5/6.

This is the **provider side** of the read-only contract 009 consumes: 008 exposes durable binding
facts, never keys, never execution rights. It creates no 009 behavior and no bilateral conformance
claim; real 009 integration acceptance is deferred to post-dependency (see `downstream.md`).

## 1. Carrier and permissions

- **Transport**: authenticated HTTP/JSON served by the existing `txharbor serve` HTTP server
  (`TXHARBOR_HTTP_ADDR`), mounted alongside the 007 endpoints (no new listener, no new
  infrastructure).
- **Auth**: `Authorization: Bearer <TXHARBOR_NONCE_READ_TOKEN>` (deployment-env service
  credential; constant-time compare; never logged, never in the repo; rotation = env change +
  restart). Missing/invalid → **401** `unauthenticated`; the token grants **read only**.
- **Permission boundary**: this credential carries no allocation, no release, no registry, and no
  write of any kind. Release/registry operations live exclusively on the operator DSN carrier
  (`txharbor nonce-admin`, see `observation.md`). The read path is pure: it never creates,
  changes, releases, or reassigns a binding, hold, floor, observation, or upstream (006/007) row
  — OC-6 "查询 MUST NOT 新增、变更、释放或重分配绑定".
- **Encoding**: JSON, snake_case; addresses lowercase `0x` + 40 hex; `nonce` and all counts are
  decimal strings (uint64 range, never floats); timestamps RFC3339 UTC; unknown fields ignored.

## 2. Endpoints and the five outcomes

| Endpoint | Lookup key |
|---|---|
| `GET /nonce/bindings/{binding_id}` | stable binding identity |
| `GET /nonce/bindings/by-intent/{intent_id}?chain_id=&sender=` | stable intent identity; `chain_id`/`sender` are optional expected-scope cross-checks |

Every response is exactly one of **five outcomes**:

| Outcome | HTTP | Meaning | Consumer obligation (stated, not enforced here) |
|---|---|---|---|
| `bound` | 200 | one non-terminal binding returned as facts, with gate/recovery annotations | read facts; a usable-for-new-signature decision requires the consumer's own re-verification (authorization + pause gates) before each use (OC-5/OC-6); `bound` is never permission |
| `terminal` | 200 | binding exists but is in a terminal state (`consumed`/`released`) | do not use; facts serve audit/trace |
| `not_bound` | 404 | no binding for this identity | fail closed; do not allocate on 009's side, do not invent identity |
| `mismatch` | 409 | supplied expected scope contradicts the durable binding (chain/sender differ) | fail closed; the durable binding is authoritative |
| `unavailable` | 503 | 008 cannot produce a trustworthy answer (DB read failure, rebuild gate not open) | fail closed; retry later with the same identity; never treat as absent |

`not_bound` and `mismatch` use identical error-body shape (machine code + request trace id, no
internal detail); `unavailable` is explicitly retryable. Reads are side-effect free and retry-safe.

## 3. Response bodies

### 3.1 `bound`

```json
{
  "outcome": "bound",
  "binding": {
    "binding_id": "nb-…32hex",
    "intent_id": "…",
    "chain_id": 31337,
    "sender": "0x…40hex",
    "nonce": "7",
    "state": "allocated",
    "created_at": "2026-09-16T10:00:00Z",
    "authorization": {"id": "wa-…", "version": "64-hex-sha256"},
    "registry_seq": 3
  },
  "annotations": {
    "gate": {"state": "open", "causes": []},
    "recovery": {"state": "none"},
    "registry_state": "active"
  },
  "notice": "binding facts only; this response is not a signing or broadcast authorization"
}
```

- `state` ∈ `allocated` | `in_flight` (non-terminal; `in_flight` is the persistent unknown-outcome
  state — never auto-resolved, never reassigned).
- `nonce` is the durable binding fact; 008 never returns a different nonce for the same intent
  (replay returns the original; conflicts are refused at admission).
- `authorization` is the OC-5 binding recorded at admission: identity + version digest. 008 does
  not re-validate authorization on read; the consumer validates current authorization against the
  authority (OC-5) and MUST fail closed on missing/expired/revoked/mismatched/unverifiable.
- `annotations.gate.state` ∈ `open` | `held`; `held` lists every **active** 008 hold with
  `hold_id`, `cause`, `established_at` (OC-6 multi-cause visibility). `open` means only "no
  008-owned hold at this snapshot" — it is **not** an execution authorization.
- `annotations.recovery.state` ∈ `none` | `recovering` | `paused_reconcile` | `released` |
  `unknown` — read-only 006 state at the same snapshot (007 annotation subset); `unknown` is
  emitted when the 006 read fails and MUST be treated as not-clear.
- `annotations.registry_state` ∈ `active` | `disabled` — current registry state; a disabled
  sender keeps existing bindings valid as facts and is not retroactive (R9).

### 3.2 `terminal`

Same shape as `bound` with `state` ∈ `consumed` | `released`, plus `terminal_at` and, for
`released`, `release_operation_id`. `annotations` are still served. A terminal binding is a fact
for audit; it is not usable and its nonce is never reused.

### 3.3 `not_bound` / `mismatch` / `unavailable`

```json
{"outcome": "not_bound",
 "error": {"code": "not_bound", "request_trace_id": "…"},
 "notice": "…"}
```

`mismatch` additionally echoes the durable scope (`binding_id`, `chain_id`, `sender`) — the
caller's expected scope was wrong, not the binding. `unavailable` carries a retry instruction and
never claims absence.

## 4. Consistency

- Every request opens one `REPEATABLE READ` transaction in read-write mode (NOT `READ ONLY`:
  the read acquires a row lock below, which `READ ONLY` rejects) and performs lock acquisition +
  `SELECT` only — zero data modification; the transaction commits without writing. Order inside:
  `SELECT … FOR SHARE` on the scope's `nonce_scope_state` row (when the scope row exists) first,
  then the snapshot reads below (binding row, scope active holds + scope state, registry row, 006
  recovery signal). Same snapshot ⇒ no cross-version composite (e.g., a release-then-re-establish
  cannot be straddled).
- 008-owned read failure → `unavailable` (all-or-nothing; no partial fact set). 006 read failure
  → `recovery.state = "unknown"` on an otherwise successful `bound`/`terminal` response — never
  mapped to `none`/`released` (007 rule).
- Reads are linearly preceded by whatever admission committed; a concurrent allocation may not be
  visible to an older snapshot — consumers retry after `not_bound` if they expect a fresh
  admission (no cross-request ordering claim).
- Serialization against 008 writers (bilateral with 009 R6): the read takes `SELECT … FOR SHARE`
  on the scope's `nonce_scope_state` row (when the scope row exists) before opening the snapshot;
  every 008 writer takes the same row `FOR UPDATE` (data-model T-allocate/T-observe/T-hold-release/
  T-binding-release), so a pause/registry/floor/binding write that commits before the read's lock
  is visible in the snapshot, and one racing the read blocks until the snapshot is taken (ordered
  after). Absent scope row ⇒ `not_bound` under the retry rule above. 006 state in this snapshot
  stays best-effort (007 single-snapshot protocol); cross-process 006 ordering is the consumer's
  own gate re-read (009 R6 gate-table lock), not this read.
- The scope-row `FOR SHARE` ordering above applies to `bound`/`terminal` only: `not_bound` returns
  before any durable scope is known, and `mismatch` returns after reading the immutable binding
  row under the same snapshot and performs no annotation reads — neither takes the durable-scope
  lock (there is nothing to order), so no cross-version composite is possible for either.
- Consumer lock participation (bilateral): a consumer (009) MAY take `SELECT … FOR SHARE` on the
  same scope row directly in its own admission transaction (same database) BEFORE its gate reads
  and hold it to its own `COMMIT`, with the fixed cross-transaction order — 008 scope row, then
  006 gate tables, then 007 grant row, then own rows. 008 writers never take locks in reverse
  order (scope row `FOR UPDATE` first, reads only after), so no lock cycle is introduced; a
  racing 008 writer blocks until the consumer commits (ordered after the admission). The
  consumer MUST NOT write 008 rows; the `SHARE` lock is ordering-only. This is how 009's
  admission stays isolated from 008 pause/registry/release writes that commit after 009's own
  `ReadBinding` call returned — the returned read alone is a point fact, never the admission's
  isolation basis.
- No mutation, no delivery: 008's delivery point is the allocation-commit admission
  (OC-7 mapping); a read never advances state.

## 5. Deferred integration points (no bilateral claim)

- 009's client (auth header handling, retry/backoff, gate composition with its own signing policy)
  belongs to 009's own plan; this contract only fixes the provider's observable behavior.
- The `authorization.version` recomputation rule and the intent↔request↔authorization linkage
  cross-checks belong to the real 011 integration (deferred; `downstream.md` §4).
- Any change to this contract after the parallel window MUST go through the orchestrator's
  cross-check between the 008 and 009 branches (workflow R3).
