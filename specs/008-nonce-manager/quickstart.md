# Quickstart: 008 Nonce Manager (validation guide, design only)

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16

Design-only validation guide. **Nothing in this step starts a service, applies a migration, or
runs a test** — these scenarios become integration/E2E tests in tasks/implementation. Contract
details are not duplicated here: responses/outcomes live in `contracts/read-api.md`, holds and
release in `contracts/observation.md`, schema in `data-model.md`.

## Environment and test-resource isolation (mandatory)

- **U** (unit, no Docker): `make test` — pure logic (classification matrix, numeric bounds,
  operation-input equality, outcome mapping) with fakes.
- **I/E** (integration/E2E): `make test-integration` — tests self-provision **PostgreSQL** and
  **Anvil** via testcontainers (repo precedent; foundry `v1.8.1`, chain 31337); Anvil is the
  chain truth. Fault injection (RPC transport/timeout/rate-limit/divergent views, kill -9,
  concurrent executors) rides the same entry.
- **Workdir-local resources only** (workflow R4): if any manual debugging stack is ever used in
  this workdir, it MUST be namespaced away from the shared compose stack: database/schema
  `txharbor_008`, PostgreSQL port `127.0.0.1:55432`, Anvil port `127.0.0.1:58545`, distinct
  compose project (`txharbor008`) and volume name. **No shared `pgdata` assumption, no shared
  ports, no shared database names** with the sibling 009 workdir or the `txharbor` compose
  project (`compose.yaml` = 5432/8545 + single `pgdata` volume — never consumed by 008 tests).
- **Test doubles**: fake RPC/scripted counts are allowed for early development only; final
  concurrency/restart/recovery acceptance MUST run against real PostgreSQL + real Anvil
  (原则 XI; workflow R5). Real 009/010/011 integration acceptance is deferred (`contracts/
  downstream.md` §4).
- **Pass definition**: every assertion in a scenario green; the spec's 0-count/100 % criteria
  (SC-01–SC-09) are pass/fail, not notes. Any deviation is a failure.

## Scenario matrix

| # | Scenario | Level | Drives (US/FR) | Expected assertions |
|---|---|---|---|---|
| V1 | Concurrent allocation, one scope: N≥2 parallel requests, different intents same `(chain_id, sender)`; plus a different sender in parallel | E | US1-1/3, FR-01/02, SC-01 | each intent ≤1 binding; no duplicate nonce among active bindings; other sender unaffected; DB UNIQUE carriers present and named |
| V2 | Same-intent replay: sequential retry, concurrent duplicate, retry after restart, same intent + differing `chain_id`/`sender`/`authorization_id` | E | US1-2/4, US2-1, FR-05, SC-02 | equal input → original `binding_id`/nonce, zero new rows; differing input → conflict, original untouched, zero second binding |
| V3 | Crash/restart: kill -9 after admission commit, before any downstream effect; restart; retry same intent; rebuild gate | E+I | US2, FR-06/FR-13, SC-03 | retry returns original binding; no double allocation; allocation refused with `rebuild_incomplete` until verification completes; durable rows are the only basis (no memory) |
| V4 | Unknown outcome retention: chain shows the bound nonce pending (`nonce ∈ [L,P)`) | E | US3-1/2, FR-07/FR-09, SC-04 | binding → `in_flight` with observation evidence; still on the original intent; never auto-failed/recycled/reassigned; evidence 100 % retained; replacement attempts reference the same binding |
| V5 | Classification/holds: external mined consumption above frontier (`L>M+1`); pending-only above frontier (`P>M+1`, `L<=M+1`); RPC outage; divergent view (`L>P` / pending regression) | E+I | US4-1/2/3/4, FR-10/11/12, SC-06 | each case: classification + hold + refused admission; **zero silent reuse/adoption**; outage → `chain_view_unavailable`, no state change; not silently merged into the sequence |
| V6 | Bootstrap evidence: first-ever scope with pre-existing chain history (`P>0`) | E | FR-12 (no silent merge), R2 | `bootstrap_external_consumed` observation persists `[0,P)`; admission at `P`; no hold; evidence queryable |
| V7 | Reconcile release: insufficient evidence; cause still present; valid evidence; multi-cause; 006 coexistence | I+E | US5-6/7, FR-08/FR-14/FR-15, SC-07 | refusals recorded with zero hold/floor change; valid release clears **only** the named hold and advances floor to observed pending; other holds survive; release allowed under active 006 recovery but allocation stays blocked by the 006 gate; 006 rows byte-identical (snapshot) |
| V8 | 006 pause precedence: establish recovery before/after admission; recovery complete with independent 008 hold | I+E | US5-1/2, FR-14/FR-15, SC-07 | pause committed first → admission refused with reason; admission committed first → binding stands; recovery completion does not clear the 008 hold; no 006 write by 008 |
| V9 | Read contract five outcomes + permissions + immutability | I | FR-14 (OC-6), SC-06; contracts/read-api.md | `bound` (gate open/held with causes), `terminal`, `not_bound`, `mismatch`, `unavailable` each match the contract; 401 without/with wrong token; `notice` present; read requests leave all table snapshots unchanged; a release/establish racing a held-open read snapshot is never straddled; 006 read failure → `recovery.state=unknown`, never `none`/`released` |
| V10 | Registry lifecycle: register → disable → re-register; change-effect | I | FR-01/OC-2, SC-05 | disabled sender: new admission refused, existing binding facts unchanged, `registry_seq` history + audit rows complete; existing intent's sender/nonce never changes; operation-id replay/conflict semantics hold |
| V11 | Authorization binding and fail-closed | I | FR-17/OC-5, SC-05 | missing/inactive/expired/mismatched/unreadable authorization → no binding (zero rows); valid → binding stores id + version digest; 008 writes zero 007 rows (snapshot); retry does not consume/extend the authorization |
| V12 | Operator attempt semantics | I | FR-08, R7 | same operation id + same op-input → one audit row, recorded outcome; differ → `operation_conflict`, zero writes; refusals recorded as committed `refused` outcomes; uncertain COMMIT → same-id retry only |
| V13 | Numeric/evidence/log hygiene | U+I | FR-21, SC-09, R13 | nonce `0` and `2⁶⁴−1` handled without uniqueness break; counts/nonces are decimal strings (no floats anywhere); logs contain redacted fields, zero token/key material; metrics series for allocations/holds/observations/reads/admin exist; every refusal carries a machine reason |

## Failure-path checklist (first-class, not optional)

- RPC down / timing out / rate-limited / returning conflicting views during admission and during
  reconcile (V5, V9).
- Process kill -9 at: after observation, after binding commit, mid-reconcile, after release
  commit (V3, V7) — all resolved by durable re-read, none by memory.
- Concurrent: same-intent duplicates, same-scope different intents, release vs observer,
  release vs admission, two operators on one hold (V1, V2, V7, V12).
- Upstream races: 006 establish/release racing admission; 007 authorization expiry/revocation
  racing admission (V8, V11).
- Log/resource hygiene: zero secrets in logs; zero cross-workdir resource collision (V13 +
  Environment rules).

## Deferred (explicit, not silently absent)

- Real intent table/linkage (011), attempt lifecycle (010), and the 009 client are not exercised
  end-to-end; interface contracts exist (`contracts/`) and their real integration acceptance is
  deferred to post-dependency.
- Production provider selection (T000-P) remains open; Anvil-only validation does not claim
  production readiness.
