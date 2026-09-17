# Implementation Plan: 011 Withdrawal Execution Worker（提款执行）

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) (clarify 2026-09-17: M1n/M3 已裁决、M2 以既定契约消除；`[NEEDS CLARIFICATION]` = 0)

**Input**: Feature specification `specs/011-withdrawal-executor/spec.md`; frozen joint design contract v1 and C1–C12 registry (`docs/workflow-010-011-parallel.md`, read-only, merged at `a3ac87e` + C11 correction + v1 section); counterpart 010 spec `bd59754` (read-only, **not merged into this branch**); Charter Constitution 1.1.0 (`.specify/memory/constitution.md`); prior art `specs/009-signer-service/{plan,research,data-model,contracts,quickstart}.md`; delivered upstream code 006/007/008/009 and migrations `000001`–`000010`.

**Note**: This plan designs the **011 side only**. It consumes 010 exclusively through the frozen joint contract v1 (J1–J6) and does not redesign 010. Business semantics are inherited verbatim from approved rulings (OC-1–OC-7, PB-C1/C2, 010 Q1–Q3, 011 M1n/M2/M3); mechanism choices are plan-level and marked as proposals where a user ruling is required (C11/C12, thresholds).

## Summary

011 is the execution chain's last stage: it admits `Accepted` 007 requests (authenticated caller + fixed
interface permission + current per-request authorization + current gates), persists the stable payment
intent **before** any 008 reservation, and runs a single-writer-per-intent execution worker. Exclusivity is
a durable claim row (`execution_claims`, one per intent) with a monotonic `lease_version`; validity is
decided on the PostgreSQL clock; renewal is lock-first; disqualification (expiry, evidence-required operator
revocation, or explicit stall-based invalidation) atomically bumps the version so that old workers' writes
are fenced by exact-version guards and old send attempts are refused by 010's claim-row verification
(J2/J4). 011 calls 010 through a narrow consumer interface (`LifecycleAdvancer` / `LifecycleReader`) with a
pre-allocated `step_id` for idempotent convergence; 010 owns construction/signing-persistence/broadcast and
returns closed-set outcome classes. Request status is exposed only through a version-monotonic display
projection that references attempt identity, marks `possibly_stale` when authority cannot be confirmed, and
is never read by any decision path (Q1/J5). Revision after reorg is an authority-driven, version-guarded
fact transition that needs no authorization and no claim (M2). Lease TTL (30s) / heartbeat (10s ±10% jitter)
/ stall window (300s, independent of TTL) are the approved initial configuration (2026-09-17) with a
repo-grounded evidence basis; backoff/cadence remain technical values. The plan reports (rather than papers
over) the one real cross-lane gap it found: the migration-number/FK ordering coupling with 010 (C12).

## Technical Context

**Language/Version**: Go 1.26.5 (`go.mod`); monetary/nonce/fee values are integers only — `NUMERIC(78,0)` ↔
`math/big`/uint256, BIGINT for versions/counters, `big.Int` in Go; zero floating point at any layer
(Charter I). Addresses lowercase `0x`+40 hex; hashes `0x`+64 hex.

**Primary Dependencies**: pgx v5.11.0 (pool, tx, `pgconn.PgError` constraint classification); goose v3.28.0
(embedded migrations, `WithAllowOutofOrder(true)`, serve-time `CheckCompatibility`); existing
`internal/{config,db,health,logx,metrics}`; read-only consumption of `internal/withdrawal` (auth + error
taxonomy) and 008's read surface via an adapter (009 `binding_live.go` precedent). **No new module, no new
infrastructure** (no Redis/Kafka/broker; Charter XIII). The 010 boundary is a narrow interface **defined by
011 and implemented by 010 at wiring time** (research R8); concrete types land with 010's plan.

**Storage**: PostgreSQL only. New objects in one provisional migration `000012_withdrawal_execution.sql`
(pure additive DDL): `payment_intents`, `execution_claims`, `execution_steps`, `execution_events`,
`request_status_projection`, `execution_caller_permission`, `execution_ops_audit`
([data-model.md](data-model.md) Tables 1–7). Read-only upstreams: 006 `indexer_pause`/`log_pause`/
`deposit_pause`/`reorg_recovery`/`reorg_recovery_events`; 007 `withdrawal_requests`/
`withdrawal_authorizations`/`withdrawal_authorization_scopes`; 008 `nonce_wallet_registry` + binding read
contract; 010 attempts/revisions/unknown (via `LifecycleReader`). No cache; PostgreSQL is the only durable
truth (Charter III).

**Testing**: `go test ./...` (unit: admission gate matrix, claim CAS predicates, stall predicate, state
machine guards, projection version monotonicity, refusal taxonomy, 010 class mapping) + `make
test-integration` on real PostgreSQL (claim races, renewal/expiry, takeover timelines, crash points, gate
races, projection staleness) + race detector on the claim/step paths; real HTTP for admission/view routes.
011 has **no chain code path** (structural assertion: no RPC/dial import in `internal/execution`), so Anvil
belongs to the joint acceptance only. Quickstart V1–V13 is the scenario matrix (design only here).

**Target Platform**: Linux server; single deployment, single chain (v1 scope沿 006/007).

**Project Type**: Backend wallet/transaction infrastructure monorepo. New `internal/execution` domain
package; two subcommands (`withdrawal-worker`, `withdrawal-exec`) on the existing binary; two new routes on
the existing `serve` listener (no new listener); one new migration.

**Performance Goals**: No throughput target. Each step is a bounded sequence of point statements under a
fixed statement-timeout guard plus one 010 call outside any transaction; no benchmarks claimed. Claim
contention is per-intent; fairness is bounded backoff + jitter (proposal).

**Constraints**: Fail-closed configuration (invalid/missing knob combinations refuse startup); DB `now()` is
the only clock for expiry/validity; **no external call while holding DB locks or inside a transaction**;
bounded retries with defined retryable classes; no secrets/keys/raw signed bytes in logs; test resources
workdir-isolated (dedicated DB name + non-default ports, compose coordinated by the main orchestrator);
migrations additive only, never rewriting applied ones; **lease TTL 30s / heartbeat 10s (±10% jitter) /
stall window 300s are the approved initial configuration (2026-09-17)** — `stall_window` is an independent
value, never auto-derived from the TTL; backoff/cadence remain technical values. These are initial config,
not blanket business limits, and a config change/restart alone MUST NOT extend an existing qualification.

**Scale/Scope**: Single chain; one claim row per intent; operator-issued permission cardinality (tens); no
UI; no admin HTTP surface (operator subcommand only); no new infrastructure.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Pre-Phase-0 (2026-09-17, against Constitution 1.1.0)**:

- **I (Financial correctness)**: PASS — one intent per request enforced by `UNIQUE(request_id)`; exclusivity
  by a monotonic per-intent `lease_version` + exact-version guards; integer-only amounts/fees/versions; no
  path can create a second payment or claim a send without a current qualification.
- **II (Idempotency)**: PASS — insert-first + 23505 classification for admission; insert-first `step_id`
  convergence for advances; CAS claim acquisition; projection version monotonicity; no in-memory authority.
- **III (PostgreSQL truth)**: PASS — intents/claims/steps/events/projection/audit all durable in PG; worker
  and CLI hold no authoritative in-memory state; restart rebuilds execution position from PG + 010 facts.
- **IV (Reorg-aware)**: PASS — 010 owns receipt/confirmation authority; 011 revises state/projection by
  version-ordered authority consumption, tracks the original intent, creates no compensation payment; 006
  recovery version observed read-only per send-enabling step.
- **V (Explicit state machine)**: PASS — intent/claim/step state machines with named CHECKs and guarded
  transitions ([data-model.md](data-model.md)); send-enabling vs authority-fact transitions are physically
  separated; illegal transitions refused/recorded.
- **VI (Atomic boundaries)**: PASS — state transition + evidence + projection in one transaction;
  gate reads + step issue in one transaction; external calls outside transactions; no dual writes.
- **VII (Nonce concurrency)**: PASS (boundary) — 011 never allocates/consumes/releases nonces; it binds the
  intent identity and consumes 008 facts read-only; reservation ordering (intent before reservation) is a
  recorded contract obligation.
- **VIII (Key isolation)**: PASS — 011 holds no keys, never signs, never imports a signer/provider; no RPC
  client; signing only through 010→009.
- **IX (Failure paths)**: PASS — closed refusal taxonomy; unknown/reconcile is first-class and never a
  failure claim; bounded retries; crash points enumerated with recovery; gate read failure fails closed.
- **X (Deterministic local testing)**: PASS — real PostgreSQL + real HTTP + Anvil available locally; no
  public testnet; workdir-isolated resources.
- **XI (Invariant tests)**: PASS — concurrency/lease/takeover/crash/gate-race scenarios use real PG and real
  concurrency; contract-shape doubles only for the 010 boundary in 011-independent runs and never cited as
  joint acceptance.
- **XII (Observability)**: PASS — structured logs + metrics with identity/version/class fields on every
  admission/claim/step/revision path; secrets and raw bytes excluded; integer rendering only.
- **XIII (Simplicity)**: PASS — no new infra/service/protocol; one package + one migration + two subcommands
  + two routes; reuses existing config/log/metrics/health patterns and 006 supervision shapes.
- **XIV (Spec-driven)**: PASS — this plan; scope fenced by spec Non-Goals; upstream rulings consumed, not
  reopened; no implementation in this step.

**Post-Phase-1 re-check (2026-09-17)** — no new violations introduced. Carriers beyond a bare minimum are
justified in Complexity Tracking: (a) the second event-ish table (`execution_steps`) is required to hold
"execution position" so crash recovery cannot re-issue a send; (b) `execution_caller_permission` is the
FR-01 fixed-interface-permission carrier that cannot live on 007's `caller` without coupling two
independently owned interfaces; (c) `execution_ops_audit` is the operation_id dedup carrier required for
evidence-required operator paths. Lock discipline stays ring-free: gate-table `SHARE` → 011 claim row →
006 snapshot → 007 grant/scope `FOR SHARE` → 008 read → own rows; no upstream writer takes an 011 row, and
010's only 011 interaction is the read-only claim-row verification (gates.md §4).

Verification-round addendum (2026-09-17): no new violations introduced by verification. Adjudication addendum (2026-09-17): `lease_ttl`=30s / heartbeat 10s approved as initial config, `stall_window`=300s approved as initial threshold (independent value; conditions in research R14); approval covers these items only. Status split: plan documents delivered; design closed except C10/C12 execution and plan-level mechanisms; joint acceptance not executed; no auto-entry into tasks.

## Inherited rulings (verbatim) and joint contract v1 mapping

Business semantics are consumed, not reopened. Each ruling below is quoted verbatim from the approved
sources and mapped to its 011 design carrier.

| ID | Ruling (verbatim) | Source | 011 carrier |
|---|---|---|---|
| OC-1 | "付款意图身份（OC-1）：由 011 创建（本规格 FR-02）；008 只绑定既有 intent。" | 011 spec D3 | `payment_intents` (Table 1); T-admit; `UNIQUE(request_id)` |
| OC-2 | "sender 权威（OC-2）：钱包注册表/部署配置；调用方声明不具权威性。" | 011 spec D4 | sender fixed at admission from PB scope + `nonce_wallet_registry` cross-check |
| OC-3 | "nonce 绑定（OC-3）：008 自有；011 与 010 只消费其事实。" | 011 spec D5 | no allocation/binding writes; five-class read observation |
| OC-4 | "尝试与签名请求身份（OC-4）：010 预持久化；一次尝试一身份；011 消费身份链，不另建冲突身份。" | 011 spec D6 | `LifecycleAdvancer` + `step_id` convergence; attempt refs only |
| OC-5 + PB-C1/C2 | "执行授权（OC-5 + PB-C1/C2）：逐笔授权载体、条件式费用替换复用、重签发追溯；011 消费授权用于准入，调度不授予签发权。" | 011 spec D7; C4 | admission binds grant to one intent; per-step re-verify; replacement-purpose token checked, fee-triple judged by 010 |
| OC-6 / OC-7 | "暂停/撤销后行为与竞争定界（OC-6/OC-7）：009 逐次重验只约束签名侧；010 广播安全矩阵（Q3）约束发送侧；011 遵守同一矩阵。" | 011 spec D8 | 006 read-only gate; same send matrix; no release/clear |
| 010 Q1 | "选择 C：011 可以保存用于查询展示的状态投影，但 010 持久事实是交易尝试状态的权威来源。投影 MUST NOT 作为执行、重播、费用替换或结束恢复追踪的许可依据；相关决策 MUST 读取并验证当前权威事实。…无法确认新鲜度时 MUST 明确标记状态可能过期…" | 010 spec Clarifications | `request_status_projection` (Table 5); freshness; zero-projection-read decision paths |
| 010 Q2 | "选择 A 并修正表述：仅拒绝过期 worker 写库不够。010 MUST 阻止失去执行资格的 worker 凭旧租约、旧执行版本或旧准入结论发起首次广播、同字节重播及费用替换；已持有签名字节不构成继续发送的许可。…MUST NOT 为接管创建第二付款意图。…MUST 保留发送事实或结果未知状态，继续追踪和对账。" | 010 spec Clarifications | exact-version self-fencing; claim row 010-read contract; no grace/TTL |
| 010 Q3 | "选择 A 并收紧"已开始"定义：每次发送（首次广播、同字节重播、费用替换）均 MUST 满足当前有效授权、无阻断性暂停、恢复版本有效及调用者执行资格有效；…无重播例外或 TTL 宽限。…"发送决策先行"不足以构成合法在途…" | 010 spec Clarifications | per-step gate re-verification; refusal-before-send; unknown on non-cancellable stage |
| 011 M1n | "选择 A：原执行者失格后可与其他 worker 按同等条件重新竞争同一请求，不增加进程身份禁令。安全性由新资格和旧版本隔离保证…重领 MUST 取得新的、可与旧资格区分的执行版本并重验全部适用门禁；MUST NOT 恢复、续用或复活已失效的旧租约和旧版本；新资格 MUST NOT 使该进程中尚存的旧任务自动获得新许可…" | 011 Clarifications 2026-09-17 | same-CAS re-competition; `lease_version+1`; all-gates re-verification; old-version fencing |
| 011 M2 | "无需提问，既定契约覆盖：链事实观察、状态/投影修订与对账记账不是新的资产执行，不需要新授权；任何后续签名、广播、重播与费用替换适用 Q3…"发生重组" MUST NOT 自动等同于"必须重新签发授权"，历史授权 MUST NOT 当作当前发送许可。" | 011 Clarifications 2026-09-17 | authority-driven fact transitions without claim/authorization; physical separation from send-enabling transitions |
| 011 M3 | "选择 A：采用自动接管政策，不因单纯租约失效或暂时无进展一律增加人工审批关卡。但区分"无进展"与"已失格"：无进展 MUST NOT 直接证明租约失效，也 MUST NOT 让两个 worker 同时持有有效执行资格；租约已到期或执行资格已被有效撤销后，请求可自动开放重新领取；旧资格仍有效时 MUST 先依照明确的停滞判定与资格失效规则使旧资格失效，再允许新版本接管…" | 011 Clarifications 2026-09-17 | progress watermark + stall predicate under row lock + atomic invalidate/bump/assign (R6) |

**Joint contract v1 (J1–J6) → 011 carriers**:

| J | Contract content (frozen) | 011 carrier |
|---|---|---|
| J1 | identity chain `request_id → payment_intents.intent_id → nonce_bindings → tx_attempts.attempt_id + signing_request_id → authorization_id + scope version`; intent rows owned by 011, attempt rows by 010, FK not fact copy | `payment_intents` + `LifecycleAdvancer`/`LifecycleReader`; attempted FK ordering residual recorded (C12, see Limitations) |
| J2 | `execution_claims` `(intent_id UNIQUE)`, worker identity, monotonic `lease_version`, expiry, state; 010 reads the claim row in its send gate (read-only); re-claim = new version, old never revives | [data-model.md](data-model.md) Table 2 + [gates.md](contracts/gates.md) §4 |
| J3 | single-transaction read path; gate-table shared lock first; order claim → pause → recovery version → authorization/scope → binding → attempt; statement-timeout guard; DB clock; exact-version guarded updates | gates.md §0/§5; T-step-issue/T-claim catalogs |
| J4 | 010 re-verifies binding/authorization/pause/version/claim equality in one read sequence; ordering by lock order (decision-first ⇒ in-flight unknown; invalidation-first ⇒ refuse); DB/network independent failure ⇒ unknown; no grace/TTL; no 009 residual reuse | gates.md §4 ordering obligation + persistence.md §3/§6 |
| J5 | authority = 010 attempt rows; projection carries (source version, time), applies only if newer; unknown freshness marked; revision chain consumed in order; compensation refresh re-reads authority; unknown recovery interface `(attempt_id, tx_hash)` consumed, no rebuild | projection (Table 5) + revision consumption (R9) + lifecycle.md §5/§6 |
| J6 | provisional migrations 010 `000011_*`, 011 `000012_*`, pure additive, verified at merge; supply/revoke entry owned by 007-extension (read-only consumers); single-chain config; shared test resources coordinated; acceptance order 010 independent → 011 independent → real joint (HTTP/PG/Anvil); merge order 010→011; upstream sync before dependent acceptance | migration section below; acceptance split below |

## Design spine

### D1 — Admission & stable intent creation (FR-01/FR-02/FR-03)

Caller-facing, authenticated entry on the existing `serve` listener (`POST /withdrawals/{id}/execution`,
[api.md](contracts/api.md) §1): Bearer credential (007's `Authenticate`, read-only reuse) + fixed interface
permission `execution_caller_permission.can_execute` + request ownership + `status='accepted'` + active
valid grant equal to the request + PB scope present and covering the request (declared `intent_id`,
`request_id`, `sender`) + `sender` registered/active in `nonce_wallet_registry` + 006 gates clear. Intent
creation is **insert-first** (`UNIQUE(request_id)`), in one transaction with its evidence and projection
row, before any reservation is possible (OC-1/C1). `intent_id` is the PB scope's declared identity value —
011 persists it, does not mint a parallel identity (research R3). Refusals create zero intents; Accepted is
never treated as admission or authorization. `caller_id`, `chain_id`, `asset`, `recipient`, `amount` stay
single-sourced from the 007 request row; the intent stores only identity + immutables
(`intent_id, request_id, chain_id, sender, authorization_id, authorization_version`) so there is no
divergence path.

### D2 — Claim, lease, execution qualification, takeover (FR-04/FR-05/FR-06/FR-15)

One row per intent; `lease_version` monotonic; validity on the DB clock. **Acquisition** is one atomic CAS
(`INSERT … ON CONFLICT (intent_id) DO UPDATE … WHERE <claimable> RETURNING lease_version`); claimable =
released/revoked, or expired, or stalled past `stall_window` (R6). **Renewal** is a lock-first short
transaction (verify owner+version+active+unexpired, extend on DB clock); any mismatch ⇒ `ErrClaimLost`, the
holder stops writing immediately. **Self-fencing**: every execution write is an exact-version conditional
update — zero rows means disqualified, roll back and re-observe. **Cross-process fencing**: 011 presents
`(owner_id, lease_version)` with every `Advance`; 010's send gate verifies the claim row (active, unexpired,
version equal) in its own decision transaction and refuses otherwise (J2/J4); the joint ordering obligation
on 010's claim read is recorded in [gates.md](contracts/gates.md) §4 and, if unmet by 010's plan, is
reported (see Limitations) rather than compensated with grace windows. **Stall/disqualification/takeover
(M3)**: progress is a durable watermark advanced by step facts, authority-revision consumption,
reconcile observations, and state transitions — never by renewals or re-claims. A candidate takes over in
one transaction under the claim row `FOR UPDATE`, re-evaluating the stall predicate under the lock; progress
first ⇒ no takeover; takeover first ⇒ the old holder's progress write affects 0 rows and is fenced. The
sweep also marks (`stall_flagged_at` + event + metric) without takeover so FR-15 evidence always exists,
without resetting business state. Operator `claim-revoke` (evidence-required, expected-version-guarded) is
an escape hatch, not a required gate (M3). **Equal re-competition (M1n)**: re-claim goes through the same
CAS, receives a new version, re-verifies every current gate, and never resurrects the old qualification;
old tasks stay fenced.

### D3 — Calling 010: advance, retry, crash, unknown (FR-08/FR-11)

011 drives one **send-class step** at a time: persist `execution_steps(issued)` (with `step_id`, `action`,
`lease_version`, `owner_id`, observed `recovery_version`) in a gate-verified transaction, call
`LifecycleAdvancer.Advance` **outside** any transaction, then converge in a fenced transaction. 011 passes
only identity, fencing, action, `step_id`, and anchor references — never nonce/fees/calldata/signatures
(010 owns construction; 009 owns signing). Outcome classes are a closed set
(`sent|refused_gate|refused_basis|pending_unknown|reconcile_required|unavailable`) mapping to step terminal
states and to `reconciling` — unknown is never failure or "not paid". An open step blocks new steps
structurally (`execution_steps_open_uniq`), so "reconcile first" cannot be skipped; a retry uses the same
`step_id` and converges on the same 010 attempt (010 idempotency requirement). Crash points are enumerated
in [persistence.md](contracts/persistence.md) §8. Every send-enabling step re-verifies the full matrix —
current authorization, no blocking pause, valid recovery basis, current qualification (Q3) — with the
authority of the send decision resting in 010's gate transaction, never in a prior check.

### D4 — Scheduling vs projection, freshness, revision (FR-09/FR-10)

Two separate planes:

- **Scheduling/decisions** read only authoritative sources: 011's own intent/claim/step rows, 010's
  `LifecycleReader` facts, and the current gates. The projection table is never consulted.
- **Display** reads `request_status_projection`, whose two input streams are version-monotonic
  (`state_version` from 011, `lifecycle_version` from 010); older inputs affect zero rows; the row
  references attempt identity instead of copying transaction facts; an unconfirmed authority read sets
  `freshness='possibly_stale'` with a timestamp that display must surface, and a successful read at a
  version ≥ stored clears it. Revision after a reorg is applied as an **authority-driven fact transition**
  (no authorization consumed, no claim required — M2), version-guarded, recorded as an event, and it never
  creates a new intent. Compensation refresh (periodic consumption, startup catch-up, audited operator
  force-refresh) guarantees staleness is always visible and recoverable; **no numeric display SLA is
  promised** (C11), and the absence of an SLA does not permit unbounded staleness because the mechanism and
  the operator path always exist.

  **M2 scope fence (verbatim inheritance)**: the M2 bookkeeping exemption is strictly limited to chain-fact
  observation, request state, and projection revision. 011 introduces **no balance ledger** (spec Non-Goals)
  and the exemption grants **no relief from normal authentication or operator permissions**: admission,
  claiming, send-enabling transitions, and every operator mutation keep exactly the gates defined in this
  plan, and fact transitions grant no send authority.

### D5 — Lifecycle state machine (FR-12)

Intent states: `admitted → claimed → executing → completed | failed | reconciling | revised` with the
guarded transition set in [data-model.md](data-model.md). Send-enabling transitions (`→claimed`,
`→executing`) are claim-fenced and gate-verified; fact transitions (`→reconciling`, `→completed`,
`→failed`, `→revised`) are authority-driven and never consume authorization or require a claim. Refusals
are recorded evidence, not transitions. Illegal transitions are refused with zero writes.

### D6 — Operator paths and threshold proposals (FR-15/C11/C12; research R14)

`withdrawal-exec` provides evidence-required, `operation_id`-deduplicated operations (permission set/revoke,
claim revoke, projection refresh, read-only inspection). Lease TTL (30s), heartbeat (10s ±10% jitter) and
stall window (300s, independent of TTL) are the **approved initial configuration** (2026-09-17; research R14)
exposed as `TXHARBOR_WORKER_*` knobs with fail-closed validation; backoff and scan cadence remain technical
values. They are initial config, not approved business limits, and MUST NOT be written into acceptance as
blanket limits; a config change/restart alone MUST NOT extend an existing qualification. C11 (display-latency
business limit) stays unspecified; the plan provides the mechanism, not a number.

## Project Structure

### Documentation (this feature)

```text
specs/011-withdrawal-executor/
├── plan.md              # This file (/speckit.plan output)
├── research.md          # Phase 0 output (R1–R14 decisions + threshold proposals)
├── data-model.md        # Phase 1 output (Tables 1–7, state machines, txn catalog, concurrency)
├── quickstart.md        # Phase 1 output (V1–V13 design-only + independent/joint split)
├── contracts/           # Phase 1 output
│   ├── api.md           # admission/view interfaces + operator surface + error hygiene
│   ├── gates.md         # consumer-side reads + 010→011 claim-row verification + read matrix
│   ├── lifecycle.md     # 011→010 advance/read boundary, step idempotency, unknown, revision
│   └── persistence.md   # ordering, crash/retry, refusal taxonomy, secrecy
├── checklists/
│   └── requirements.md  # specify/clarify artifact (consumed, not rewritten)
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created here)
```

### Source Code (repository root)

```text
internal/execution/
├── admit.go        # admission core: auth/permission/grant/scope/registry/gates → intent insert-first
├── intent.go       # intent read + state machine CAS transitions (send-enabling vs fact variants)
├── claim.go        # claim CAS acquire / lock-first renew / release / operator revoke / stall takeover
├── steps.go        # execution_steps insert-first issue + fenced converge; open-step invariant
├── advance.go      # worker step driver: gate read → issue → 010 call → converge → progress/state
├── gates.go        # 006 snapshot + 007 grant/scope FOR SHARE + 008 observation + claim fence reads
├── lifecycle.go    # 011-side consumer interfaces (LifecycleAdvancer/LifecycleReader) + class mapping
├── projection.go   # version-monotonic apply, stale marking, forced refresh
├── revision.go     # version-ordered revision consumption + authority fact transitions
├── errors.go       # refusal taxonomy classes (closed set)
└── *_test.go       # unit: predicates, state guards, taxonomy, monotonicity (no DB where possible)

internal/app/
├── withdrawalworker.go   # `withdrawal-worker` loop: scan → claim → advance → reconcile → projection
├── withdrawalexec.go     # `withdrawal-exec` operator subcommand (permission/claim/projection ops)
├── withdrawalexecution.go# serve wiring: POST/GET /withdrawals/{id}/execution handlers
└── *_test.go

cmd/txharbor/main.go        # EXTEND: dispatch withdrawal-worker / withdrawal-exec
internal/config/config.go   # EXTEND: TXHARBOR_WORKER_* knobs with fail-closed validation
internal/metrics/           # EXTEND: execution counters/gauges on the existing registry
internal/logx/ health/      # reuse (no change expected)

migrations/
└── 000012_withdrawal_execution.sql   # Tables 1–7 (provisional number; pure additive DDL)

tests (per V-matrix; design only, not written here):
├── unit:              go test (admission matrix, claim predicates, stall predicate, state guards, taxonomy)
├── integration (PG):  claim races, renewal/expiry, takeover timelines, crash points, gate races, projection
├── http:              real serve admission/view routes (auth/permission/ownership/refusal mapping)
└── no chain:          011 has no RPC path (structural assertion); chain behavior is joint-only
```

**Structure Decision**: follow the existing `internal/indexer` writer layout (one concern per file,
repository-owned parameterized SQL) and the 009 subcommand/wiring pattern. `internal/execution` reads 007
auth/errors and observes 006/007/008 tables, but MUST NOT import 006/007/008 writer packages nor any
RPC/dial package (import-boundary tests); 010/009 interaction happens only through the narrow consumer
interfaces. No new service, listener, or infrastructure.

## FR/SC/Scenario → design/verification mapping

| FR | Design carrier | Verification (future) |
|---|---|---|
| FR-01 (admission gates: auth/fixed permission/grant/gates; Accepted ≠ authorization) | api.md §1; admit.go; T-admit | V1 |
| FR-02 (stable intent, one per request, before 008 reservation, reuse on retry/crash/unknown, never a second/compensation intent) | `payment_intents` + `UNIQUE(request_id)`; insert-first; api.md §1 | V1, V4 (no second intent), V7, V10 |
| FR-03 (sender from registry, fixed at admission, bound `intent_id+chain_id`; caller claims not authoritative) | scope `sender` + `nonce_wallet_registry FOR SHARE`; intent `chain_id+sender` | V1 |
| FR-04 (exclusive claim, lease expiry = disqualification, versioned qualification, M1n equal re-competition, new version + all gates re-verified, old leases/versions never revived) | `execution_claims` + CAS + exact-version guards; R6/R7 | V2, V4 |
| FR-05 (disqualified writes refused; 011 supplies verifiable current qualification + takeover semantics to 010; three send classes fenced; sent facts kept) | exact-version self-fencing + claim-row contract (gates.md §4) | V3 |
| FR-06 (legitimate takeover: re-verify all gates, same intent/nonce/history, no second intent, full history) | T-claim + T-reconcile; intent continuity | V4 |
| FR-07 (per-request authorization consumed; scheduling ≠ issuance; bound to one intent; fail-closed; PB conditional reuse for replacement) | api.md §1 gates; gates.md §2; R10 (fee-triple judged by 010) | V1, V6 |
| FR-08 (call 010 to advance; retry/crash/unknown reuse identity; never new payment; no conflicting identity) | `LifecycleAdvancer` + `step_id`; lifecycle.md §§2–5 | V7 |
| FR-09 (display-only projection; authority = 010; source version + time; no old-over-new; stale marked; decisions read authority) | `request_status_projection`; D4; R9 | V9 |
| FR-10 (reorg revision without new authorization, keep revision chain, track original intent, no compensation) | authority-driven fact transitions; revision.go; M2 | V10 |
| FR-11 (same broadcast matrix; per-send current gates; no replay exception/TTL; multi-pause; read-only observation) | gates.md §§0–2; T-step-issue | V6, V8 |
| FR-12 (explicit state machine; illegal transitions refused; durable transitions in transactions) | data-model state machines + guards | V11 |
| FR-13 (structured logs/observability; integer amounts; no secrets) | persistence.md §9; metrics/logx reuse | V12 |
| FR-14 (real joint acceptance requirements; real HTTP/PG/Anvil; no mock substitution; 010→011 order) | quickstart V13; acceptance split below | V13 (design) |
| FR-15 (long-stall marking with full evidence, no silent discard, automatic takeover after valid disqualification, no artificial human gates, thresholds evidenced) | stall predicate + takeover CAS + sweep marking; R6/R14 | V5 |

| SC | Verified by |
|---|---|
| SC-01 (admission intent-exactly-one 100%; zero on missing authorization/pause) | V1 |
| SC-02 (one winner under concurrency; zero loser side effects) | V2 |
| SC-03 (zero writes by disqualified worker; 100% of its sends refused by 010) | V3, V6 |
| SC-04 (takeover: intent continuity 100%, zero second intents, history complete) | V4, V5 |
| SC-05 (retry/crash/unknown reuse 100%; zero unknown-as-failure/not-paid) | V7, V8 |
| SC-06 (zero old-over-new; zero unmarked staleness; zero decision reads of projection) | V9 |
| SC-07 (revision tracks original intent 100%; zero compensation intents) | V10 |

## Parallelization & real dependencies (for tasks)

**Parallelizable inside 011** (no cross-lane dependency): migration `000012` DDL + negative probes;
`internal/execution` unit-layer predicates (claim CAS predicate, stall predicate, state guards, taxonomy,
class mapping); claim/renew/takeover core against real PG; projection version/staleness logic; operator
subcommand; serve route wiring + HTTP tests; docs-derived assertions (import boundaries).

**Hard-sequenced within 011**: admission core → serve route → V1; claim core → step driver → V2–V5;
lifecycle consumer interfaces → step driver → V7; projection consumer → V9.

**Real cross-lane dependencies**: the 010 `LifecycleAdvancer`/`LifecycleReader` concrete implementation is
needed for V7/V10/V13 integration and any joint run; 010's claim-row read ordering (gates.md §4) is needed
for V3's real-race assertions; 010→011 merge order and upstream sync precede joint acceptance (C10/J6).
011-independent tests may use contract-shape doubles for the 010 boundary but MUST label them as such and
MUST NOT cite them as joint verification.

**Migration ordering dependency**: see Limitations (C12) — 010's attempt→intent FK requires either an
ordering resolution at merge or a deferred FK migration.

## Acceptance split: 011-independent vs joint

- **011-independent (executable without 010/009/008/Anvil)**: real HTTP (`serve` admission/view), real
  PostgreSQL, real migration `000012`, contract-shape doubles only for the 010 boundary; scenarios V1–V12's
  011-side assertions, including fencing of 011 writes, claim races, takeover timelines, projection
  monotonicity/staleness, state guards, taxonomy, boundary/import and secrecy assertions.
- **Joint (real HTTP/PG/Anvil; after 010 merges + upstream sync; 010→011 merge order)**: V13 (and the joint
  half of V1/V3/V7/V8/V10) with real 010/009/008: intent→attempt ordering, claim verification inside 010's
  send gate under both lock orders, step-idempotent retries after response loss, unknown reconciliation,
  replay/replacement identity + PB conditional reuse, revision/projection propagation, no-second-intent.
- **Non-substitution rule**: no double, fake, or old-mechanism test may be reported as joint acceptance;
  010's independent acceptance is not joint acceptance (C10; FR-14). The joint run is deferred (A-13 OPEN).

## Reported limitations & open questions (no grace periods, no scope widening)

1. **J4 ordering needs 010's row-share claim read** — recorded as an obligation in
   [gates.md](contracts/gates.md) §4. If 010's plan establishes only a bare snapshot read, the
   "invalidation-first ⇒ refuse" guarantee does not hold under a concurrent revocation; the minimal open
   question is "what row-lock clause does 010's gate read take?" — reported for ruling, not compensated.
2. **C12 migration FK ordering** — resolved by register J1 (`docs/workflow-010-011-parallel.md`
   J1/G-010-3 closure, executable): `000011` excludes the attempt→intent FK and is independently
   applicable; after 011 lands, 010 adds `tx_attempts_intent_fkey` in a follow-up migration (same owner,
   merges into the integration workspace, no renumbering, no cross-owner writes). Encoded by T043 and
   the 010 counterpart `010:T044` (sibling worktree); the FK MUST NOT be claimed to exist before that
   closes. No merge-time re-adjudication of the numbering options; applied migrations are never rewritten.
3. **Display latency business limit (C11)** — unspecified. Mechanism provided (version monotonicity,
   staleness marking, forced refresh); no numeric SLA is approved or implied.
4. **Thresholds (TTL/heartbeat/stall/backoff/cadence)** — lease TTL 30s / heartbeat 10s (±10% jitter) /
   stall window 300s approved as the initial configuration (2026-09-17; `stall_window` independent of TTL,
   not TTL-derived); backoff/cadence remain technical values. Initial config, not blanket business limits;
   a config change/restart alone MUST NOT extend an existing qualification (research R14).
5. **010 concrete interface types** — the consumer-side shapes in lifecycle.md are minimal requirements;
   exact types land with 010's plan and are adapted at wiring (009 D3 precedent).

## Complexity Tracking

No Constitution Check violations; no principle is waived. Carriers beyond a bare minimum, justified:

| Non-standard choice | Why needed | Simpler alternative rejected because |
|---|---|---|
| `execution_steps` table (in addition to events) | "Execution position" must survive crash so a response-lost send is never re-issued blindly; the partial unique index makes "one open send step per intent" structural (FR-08/US4) | A pure event log cannot enforce the open-step invariant without mutable rows; in-memory position violates Charter III |
| `execution_caller_permission` (own table, not `caller.can_create`) | FR-01 requires a fixed interface permission independent from 007's create permission; owning it keeps the two interfaces independently revocable and avoids coupling to 007-extension migrations | Reusing `can_create` conflates interfaces; adding a column to 007's `caller` spans two owners (J6) |
| `execution_ops_audit` with `operation_id` UNIQUE | Operator mutations (permission/revocation/refresh) need evidence and exactly-once semantics under CLI retry (008 R7 protocol) | Log-only audit has no dedup and is not durable state (Charter III/VI) |
| Stall predicate + atomic invalidate/bump/assign | M3 requires automatic takeover after explicit disqualification while forbidding "no-progress ⇒ expired" inference and forbidding two simultaneous valid qualifications | Treating no-progress as expiry violates M3; heartbeat-only lets a wedged worker hold forever; human approval is rejected by M3 |
| 011-owned claim row read by 010 | J2 fixes the carrier; gives 010 a verifiable, versioned qualification without 009-style exchange | A separate credential/handshake would duplicate authority and add a second protocol |

## Verification plan (this step writes the plan; execution belongs to tasks/implement)

- **Migration (`000012`, scratch DB only)**: `up`/`down` reproducibility; negative probes for
  `payment_intents_request_uniq`, `payment_intents_authorization_uniq`, `execution_claims_pkey`,
  `execution_claims_state_consistency`, `execution_steps_open_uniq`,
  `execution_ops_audit_operation_id_uniq`, `request_status_projection_stale_consistency`,
  `execution_events_kind_check` → expect 23505/23514 with exact names; classification by `ConstraintName`
  only; additive-only schema diff.
- **Unit**: admission gate matrix; claim CAS predicate truth table (fresh/expired/released/revoked/stalled);
  stall predicate under lock; state-machine guard matrix (send-enabling vs fact); projection monotonicity
  and staleness; refusal taxonomy; 010 class → step state mapping; no-float and no-RPC import assertions.
- **Integration (real PostgreSQL, workdir-local DB)**: N-way claim races (one winner, losers no side
  effects); renewal vs takeover timelines (progress-first / takeover-first); expiry fencing of every write
  path; operator revoke evidence/version refusals + operation dedup; admission concurrency (one intent,
  23505 classification); open-step blocking; crash-point matrix (persistence.md §8) via injected
  faults/restarts; projection stale/refresh; revision ordering.
- **HTTP**: real `serve` routes — 201/200/401/403/404/409/422/503 mapping, ownership 404 identity,
  recorded-replay body, permission fail-closed, no rows on refusals.
- **Race detector**: `make test-race` on claim/step paths.
- **Boundary/secrecy**: import test (`internal/execution` imports no RPC/dial and no upstream writer
  package); SQL-statement scan (no non-`SELECT` against upstream tables); log/error/metric scan for
  credentials/keys/raw signed bytes; integer-only rendering.
- **CI**: existing `ci.yml` jobs (unit+race, integration-Docker, build, lint) cover 011 without workflow
  changes; no new CI infrastructure.
- **Joint (deferred)**: V13 per the acceptance split.

## Evidence separation

- Prior-step evidence used as baseline only: 006 merge `8e1a440`, 007 merge `19fa11e`, 008/PB merges
  per 009's record, 009 merge `295c49d` baseline, workflow rules `a3ac87e`, 010 clarify `bd59754`. Nothing
  here re-verifies upstream or claims its verification.
- This step produced **documentation only** (plan/research/data-model/contracts/quickstart); no code,
  migration, test, container, or service was written or executed; V1–V13 are design-only and MUST be
  executed in tasks/implement.
- 010 is consumed only through the frozen contract v1 and its read-only spec; no 010 artifact was created
  or modified; no 010 design decision is made here.
- 011-independent results (future) never imply joint acceptance; joint acceptance (A-13) and production
  provider selection (T000-P) remain OPEN.

## Deferred items (explicit, with owner)

| ID | Item | Owner |
|---|---|---|
| C11 | Display latency business limit: unspecified; mechanism provided, no number promised | user ruling if needed |
| C12 | Migration number verification + attempt→intent FK ordering resolution | 010/011 merge-time coordination |
| J4 | 010 claim-row read ordering clause (row-share semantics) confirmation | 010 plan |
| Thresholds | TTL/heartbeat/stall approved as initial config (2026-09-17); backoff/cadence technical (research R14) | approved (initial config; later parameter changes need validation) |
| 010 interface | Concrete `LifecycleAdvancer`/`LifecycleReader` types + idempotency proof | 010 plan, wiring |
| A-13 | Joint full-chain E2E execution (requirements defined here) | joint acceptance, later increment |
| T000-P | Production KMS/HSM provider selection | later production track |
| 008 adapter | Binding/hold observation adapter over 008's read surface (009 `binding_live.go` precedent) | 011 tasks (V6) |
