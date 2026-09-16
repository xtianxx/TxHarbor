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

## §操作 runbook（T041；合成数据演示，非真实凭据）

本节每一步均有实现/测试证据，不引入新语义。退出码全命令统一：0 成功（本次尝试已提交
`applied`/`nop`，或同参重试收敛到已记录结果）、1 拒绝/失败（含 `operation_conflict` 与
`temporarily_unavailable`，stderr 已脱敏）、2 用法错误（含缺 `--operation-id`，在任何配置或
数据库访问之前判定）。用 serve 同一份 env 文件运行（`config.Load` 全量校验；命令实际只用
`TXHARBOR_PG_DSN` + `TXHARBOR_RPC_URL` + `TXHARBOR_CHAIN_ID`）。

信任边界（沿用 005 F-R2 / 007 T032，如实声明，非新增机制）：当前模型以写 DSN 的访问权作为
数据库操作信任边界；serve 与 `nonce-admin` 未通过独立数据库角色隔离。持写 DSN 的主体技术上
能直写数据库、绕过应用事务守卫；受控 CLI 与"禁裸 SQL"约定是操作纪律，不是数据库强制限制。
`--operator` 为调用方声明的审计标签，不是已认证身份；`operation_id` 用于关联与幂等定性，
不能单独证明实际操作者身份。审计追溯靠 `nonce_ops_audit` 的 operator/reason/operation id 列。

operation-id 捕获规则（mint-first）：`txharbor nonce-admin mint` 打印一行 32 位小写十六进制
不透明 id，是纯熵（无 `config.Load`、无连接、不落库；mint 在任何数据库访问之前）。调用方
MUST 先持久化捕获该 id，再用它执行任何变更动作。每个变更动作 REQUIRED `--operation-id`
（无自动 mint、无回退 echo）；id 绝不为同一尝试重铸。

0. 启动与闸门：`migrate up` 到 `000008`；serve 绑定部署链（FR-04，链不符在打开任何连接前
   拒绝）。启动重建闸门（R5）完成前所有分配拒 `rebuild_incomplete`；闸门失败保持关闭并结构化
   报错，无静默修复、无内存猜测（证据 T017/T023）。

1. `mint`（纯熵，证据 T015）：`txharbor nonce-admin mint` → stdout 一行 id，exit 0。
   不碰配置、不碰库。

2. 注册/禁用（注册表，证据 T008/T014/T015/T036）：
   `txharbor nonce-admin register --operation-id O --chain-id 31337 --sender 0x… --operator op --reason r`
   → `ok action=register subject_id=… outcome=applied`（插入或重启用并 `registry_seq + 1`）。
   `txharbor nonce-admin disable --operation-id P --chain-id 31337 --sender 0x… --operator op --reason r`
   → 状态翻转并 `registry_seq + 1`。注册表状态只门禁新准入；既有 intent 的 sender/nonce 是不可变
   事实，后续配置变更永不改写。等参同 id 重放返已记录结果；同 id 异参 → `operation_conflict`、
   零写入、exit 1。

3. 解除 hold（`hold-release`，证据 T014/T015/T031/T032）：
   `txharbor nonce-admin hold-release --operation-id O --hold-id H --chain-id 31337 --sender 0x… --observation-id OB --evidence "finding" --operator op --reason r`
   证据标准（observation.md §3.1，全部满足才 applied）：在锁内以新鲜预观察重核作用域分类为
   `consistent`、无未决冲突、`--observation-id` 属于该作用域；成功只清被点名的那一行 hold
   （`active → released`）并把 `reconciled_floor` 推进到观察到的 pending；同事务写
   `outcome=applied` 审计行。证据缺失/观察不存在/重核失败/仍分类为该原因/归属不足 →
   `outcome=refused`、零 hold/floor 变更、exit 1。修复完成不自动等于解除：无自动路径、无定时器。

4. 处置 binding（`binding-release`，证据 T014/T015/T025）：同 carrier，`--binding-id B`；
   仅非终态 binding 可处置；无外部副作用的判定 MUST 有显式证据（链上重观察显示 nonce 从未被挖、
   无 pending 交易，外加运维结论），单凭超时/连接错误从不是证据（US3-3）。结果：终态 `released`
   加事件加审计；该 nonce 永不复用。在途项也可保持 in-flight（解除非强制）。

5. `status`（只读查询面，observation.md §5，证据 T015）：
   `txharbor nonce-admin status --chain-id 31337 [--sender 0x…]`
   → 打印作用域状态（`reconciled_floor`/`last_latest`/`last_pending`/`last_observation_id`，缺省
   `scope=none`）与逐 active hold（`hold_id`/`sender`/`cause`/`established_at`/`evidence_observation_id`），
   末行 `status ok active_holds=N`。SELECT-only，永不写入，exit 0。

6. 未知 COMMIT 同-op-id 重试规则（observation.md §3.4，证据 T038/T045）：COMMIT 结果不确定时
   MUST 用同一个 `--operation-id` 与同一组参数重试；等参 → 收敛到已记录的
   `applied`/`refused`/`nop`（永不升级），异参 → `operation_conflict`、零写入。id 不为同一尝试
   重铸（mint-first）。

7. V1–V13 检查清单与执行记录（T041 时点，HEAD `6a3ce1d9e28fa89b49aa956895ffaf7a20695a10`）。
   本表是 T041 时点的**场景到测试映射**；V1–V13 的完整矩阵执行与 FR/SC 签署由 T042 负责，本
   工作区 T039/T040/T042 均未勾选，故本记录**不宣称** V 矩阵 green，只登记 test 文件与任务归属
   （`tasks.md` 中 T018–T038、T044/T045 记为 green）。T000-P 独立 open。

   | V | 场景 | 测试文件（任务） |
   |---|---|---|
   | V1 | 并发分配、一作用域 | `allocate_concurrency_integration_test.go`（T018） |
   | V2 | 同意图重放/冲突/收敛 | `allocate_replay_integration_test.go`（T019）、`converge_integration_test.go`（T022）、`authz_commit_unknown_integration_test.go`（T045） |
   | V3 | 崩溃/重启/重建闸门 | `restart_integration_test.go`（T021）、`rebuild_integration_test.go`（T023） |
   | V4 | 未知结果保留/替换 | `unknown_outcome_integration_test.go`（T024）、`binding_release_integration_test.go`（T025）、`reconcile_integration_test.go`（T026） |
   | V5 | 分类/hold/故障注入 | `classify_e2e_integration_test.go`（T027）、`rpc_fault_integration_test.go`（T030） |
   | V6 | bootstrap 外部消耗证据 | `bootstrap_integration_test.go`（T028） |
   | V7 | 解除证据/版本漂移 | `hold_release_integration_test.go`（T031）、`release_version_integration_test.go`（T032）、`recovery_coexistence_integration_test.go`（T033） |
   | V8 | 006 pause 优先/独立共存 | `recovery_coexistence_integration_test.go`（T033） |
   | V9 | 读契约五结果/单快照 | `readapi_contract_integration_test.go`（T034）、`readapi_lockorder_integration_test.go`（T035） |
   | V10 | 注册表生命周期 | `registry_lifecycle_integration_test.go`（T036） |
   | V11 | 授权绑定/fail-closed/撤销竞态 | `authz_integration_test.go`（T037）、`authz_revoke_race_integration_test.go`（T044）、`authz_commit_unknown_integration_test.go`（T045） |
   | V12 | 运维尝试语义 | `admin_attempts_integration_test.go`（T038）、`authz_commit_unknown_integration_test.go`（T045） |
   | V13 | 数值/证据/日志卫生 | `migration_integration_test.go`（T005）、`observe_redact_test.go`（T020）、`rpc_fault_integration_test.go`（T030）、`readapi_contract_integration_test.go`（T034） |

## §资源隔离记录（T041）

仅 workdir-local 资源（workflow R4）。若本 workdir 使用任何手动调试栈，MUST 命名空间隔离于
共享 compose 栈：

- database/schema `txharbor_008`；
- PostgreSQL `127.0.0.1:55432`；
- Anvil `127.0.0.1:58545`；
- 独立 compose project 与 volume 名 `txharbor008`。

**NEVER** 共享 `compose.yaml` 的 `pgdata` 卷 / 5432 / 8545。008 测试永不消费共享栈，故 sibling
009 workdir 不会与本 workdir 碰撞。集成/E2E 由 testcontainers 自供给 PostgreSQL 加 Anvil
（foundry `v1.8.1`，chain 31337），Anvil 为链上真相；故障注入（RPC transport/timeout/rate-limit/
divergent、kill -9、并发执行器）走同一入口（`make test-integration`）。测试双（fake RPC/脚本
计数）仅限早期开发；最终并发/重启/恢复验收 MUST 跑真 PostgreSQL 加真 Anvil（原则 XI；workflow
R5）。通过定义：每个场景断言全绿，SC-01–SC-09 为 pass/fail 而非备注，任何偏离即失败。

## §延后验收记录（T043）

明确延后，不做双边宣称（contracts/downstream.md §4）：

1. **011 intent 存在性/linkage**：011 的 intent 表不存在；008 只把 `intent_id` 当作不透明稳定
   身份，任何 FK/存在性交叉检查以及意图与请求与授权之间的 linkage 校验都延后到真实 011 集成。
2. **010 attempt-level 精化**：010 的 attempt 生命周期不存在；008 的 binding 状态只由链证据推导
   （`allocated`/`in_flight`/`consumed`），不读不写 010 表。
3. **009 客户端与 gate 组合**：读客户端、重试策略，以及与 009 自身 signing/recovery gate 的组合。
4. **端到端验收**："API 请求 → queue → nonce 分配 → signing → broadcast → confirmation" 在全链
   存在前无法验收。

FR-18 延后半（attempt 可追溯性，contracts/downstream.md §2）是**记录在案的缺口**（known gap），
**不是被模拟的行为**：测试 fixture 使用 test-authored 的不透明 `intent_id`，upstream 无 011 表，
延后的存在性检查作为已知缺口登记，绝不模拟为现实（contracts/downstream.md §5）。

008 自身验收范围限于 **admission/reconcile/read-provider** 行为（本文件 §Scenario matrix 与
§Environment）：V1–V13 覆盖的就是这三类 008 provider-side 行为，含 006 恢复暂停承接与 007 授权
只读校验边界。T000-P（生产 provider 选择）保持 open；Anvil-only 验证不宣称生产就绪。

本记录不新增代码、表或状态机，也不创建 009/010/011 规范。
