# Data Model: 011 Withdrawal Execution Worker（提款执行）

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) | **Research**: [research.md](research.md) R1–R14

载体：单个新迁移 `migrations/000012_withdrawal_execution.sql`（**暂定编号**，纯 DDL、纯加法；见 [research.md](research.md) R11 与 [plan.md](plan.md) 的 C12 残差）。所有约束显式命名（供 `ConstraintName` 分类，沿 000007/000008/000009 约定）；金额/费用一律整数最小单位（`NUMERIC(78,0)`/BIGINT/`big.Int`，零浮点，章程 I）；地址小写 `0x`+40 hex；时间列 `TIMESTAMPTZ DEFAULT now()`；有效期判定只用 DB `now()`。

命名规则：凡被分类消费的约束都带显式 `CONSTRAINT <name>`；部分唯一索引以显式索引名建（`CREATE UNIQUE INDEX`），其 23505 仍按索引名分类。

---

## Table 1 — `payment_intents`（稳定付款意图；FR-02/FR-03/FR-07/FR-12）

**归属**：011（联合合同 J1）。**语义**：007 请求的执行形态；`intent_id` 取自 PB scope 的运营方声明值（R3）；一 request 至多一 intent；一授权绑唯一 intent；sender 在准入时固定并绑定 `intent_id + chain_id`（OC-2）；行内字段**不可变**（除 `state`/`state_version`/`updated_at`），因此无需跨表一致性校验，也没有换绑路径。

```text
intent_id                  TEXT        NOT NULL  -- PB scope 声明的身份值；shape 1..128 可打印
request_id                 TEXT        NOT NULL  -- FK → withdrawal_requests.request_id
chain_id                   BIGINT      NOT NULL
sender                     TEXT        NOT NULL  -- 小写 0x+40；准入时与注册表 active 行交叉验证
authorization_id           TEXT        NOT NULL  -- 007 授权身份；1:1 绑定
authorization_version      BIGINT      NOT NULL  -- 准入时观测到的 PB scope 版本（≥1）；每次推进重验相等
state                      TEXT        NOT NULL  -- 显式状态机（见下）
state_version              BIGINT      NOT NULL  DEFAULT 1   -- 单调；所有状态转移的 CAS 版本
admitted_recovery_version  BIGINT      NOT NULL  -- 准入时观测的 006 恢复版本（证据，不是前向约束）
admitted_at                TIMESTAMPTZ NOT NULL  DEFAULT now()
updated_at                 TIMESTAMPTZ NOT NULL  DEFAULT now()
```

**Named constraints**：`payment_intents_pkey PK(intent_id)`、`payment_intents_request_uniq UNIQUE(request_id)`、`payment_intents_authorization_uniq UNIQUE(authorization_id)`、`payment_intents_request_fkey FK→withdrawal_requests(request_id)`、`payment_intents_intent_shape`（1..128 可打印，沿 `nonce_bindings_intent_shape`）、`payment_intents_sender_check`（`^0x[0-9a-f]{40}$`）、`payment_intents_chain_id_check (> 0)`、`payment_intents_authorization_version_check (>= 1)`、`payment_intents_state_check`、`payment_intents_state_version_check (>= 1)`。

- `UNIQUE(request_id)` 是「一 request 至多一 intent」的物理载体（并发/重试/重启收敛，R3）。
- `UNIQUE(authorization_id)` 是「授权绑定唯一 intent」的物理载体（与 007 `withdrawal_requests_authorization_uniq` 同键，防止两意图争一授权）。
- 不存在 `caller_id` 列：调用方归属从 `withdrawal_requests.caller_id` 读（单一真相，避免冗余副本）。
- 不存在删除/失效路径：意图是身份根，永久保留（FR-02「MUST NEVER 为同一请求创建第二意图」）。

---

## Table 2 — `execution_claims`（领取/租约/执行资格；FR-04/FR-05/FR-06/FR-15，联合合同 J2）

**归属**：011。**语义**：每 intent 一行（`UNIQUE` 由 PK 承担）；`lease_version` 单调，永不回退；到期即失格（`state='active' AND expires_at <= now()`）；010 只读核验（[contracts/gates.md](contracts/gates.md) §4）。行只更新，不删除。

```text
intent_id          TEXT        NOT NULL  -- PK；FK → payment_intents
owner_id           TEXT        NOT NULL  -- 启动唯一实例身份（16B hex，沿 indexer NewOwnerID）
lease_version      BIGINT      NOT NULL  -- ≥1，每 intent 单调；接管 = +1，旧版本永不复活
state              TEXT        NOT NULL  -- active | released | revoked
acquired_at        TIMESTAMPTZ NOT NULL  DEFAULT now()
expires_at         TIMESTAMPTZ NOT NULL  -- now()+ttl；续租/接管刷新
last_heartbeat_at  TIMESTAMPTZ NOT NULL  DEFAULT now()  -- 续租证据（不推进进度）
last_progress_at   TIMESTAMPTZ NOT NULL  DEFAULT now()  -- 进度水位；接管停滞谓词的唯一输入
stall_flagged_at   TIMESTAMPTZ           -- 周期清扫的幂等标记（FR-15 证据）
ended_at           TIMESTAMPTZ           -- released/revoked 时落盘
end_kind           TEXT                  -- released | revoked
updated_at         TIMESTAMPTZ NOT NULL  DEFAULT now()
```

**Named constraints**：`execution_claims_pkey PK(intent_id)`、`execution_claims_intent_fkey FK→payment_intents(intent_id)`、`execution_claims_owner_check`（1..128）、`execution_claims_lease_version_check (>= 1)`、`execution_claims_state_check`、`execution_claims_state_consistency CHECK ((state = 'active') = (ended_at IS NULL AND end_kind IS NULL))`、`execution_claims_end_kind_check`、`execution_claims_expiry_check (expires_at > acquired_at)`。

**可领取条件**（CAS 谓词，单事务内）：`state <> 'active'` （已 released/revoked）**或** `expires_at < now()`（自然失效）**或** `now() - last_progress_at >= stall_window`（停滞失效，R6）。四条路径都导向同一次换代：`lease_version+1`、换 owner、`state='active'`、`expires_at=now()+ttl`、`last_progress_at=now()`、清 `stall_flagged_at`、清 `ended_at/end_kind`。

**续租**（锁先行短事务）：`SELECT owner_id, lease_version, state, expires_at FROM execution_claims WHERE intent_id=$1 FOR UPDATE` → `owner_id=$me AND lease_version=$v AND state='active' AND expires_at > now()` → `expires_at=now()+ttl, last_heartbeat_at=now()`；任一步不满足 → `ErrClaimLost`，调用方立即停写并重新观察（R5）。

**进度推进**（claim-fenced 精确更新）：`UPDATE execution_claims SET last_progress_at=now(), updated_at=now() WHERE intent_id=$1 AND owner_id=$me AND lease_version=$v AND state='active' AND expires_at > now()`；影响 0 行即失格。进度来源见 R6（步骤落盘、权威修订消费、对账观察、终态转移），心跳与重领不算。

---

## Table 3 — `execution_steps`（执行步骤；FR-08/FR-13/FR-14，崩溃恢复的「执行位置」）

**归属**：011。**语义**：每个 **发送类** 逻辑步骤（first_broadcast / replay / replace）一行；insert-first（先以 `issued` 落盘，再调用 010，再收敛）；`step_id` 由 011 预分配并在重试中不变（R8）；同一 intent 至多一个 `issued` 步骤（部分唯一索引），保证「先对账后决策」不可能被跳过。对账/观察不占步骤行（记 `execution_events`），见 Table 4。

```text
step_id            TEXT        NOT NULL  -- PK；011 预分配（16B hex）
intent_id          TEXT        NOT NULL  -- FK → payment_intents
action             TEXT        NOT NULL  -- first_broadcast | replay | replace
state              TEXT        NOT NULL  -- issued | converged | refused | unknown
owner_id           TEXT        NOT NULL  -- 发出时的实例身份（证据）
lease_version      BIGINT      NOT NULL  -- 发出时的资格版本（围栏依据）
attempt_id         TEXT                  -- 010 拥有的尝试身份引用（不复制事实）
anchor_attempt_id  TEXT                  -- replay/replace 的锚点尝试身份
tx_hash            TEXT                  -- 010 返回的交易哈希引用
outcome_class      TEXT                  -- 010 分类：sent|refused_gate|refused_basis|pending_unknown|reconcile_required|unavailable
recovery_version   BIGINT                -- 发出前观测的 006 恢复版本（证据）
revision_version   BIGINT                -- 收敛时消费到的权威修订版本
evidence           TEXT        NOT NULL DEFAULT ''   -- 记录依据（不含密钥/原始签名字节）
issued_at          TIMESTAMPTZ NOT NULL  DEFAULT now()
updated_at         TIMESTAMPTZ NOT NULL  DEFAULT now()
```

**Named constraints**：`execution_steps_pkey PK(step_id)`、`execution_steps_intent_fkey FK→payment_intents`、`execution_steps_action_check`、`execution_steps_state_check`、`execution_steps_outcome_class_check`（闭集或 NULL）、`execution_steps_lease_version_check (>= 1)`、`execution_steps_tx_hash_check`（NULL 或 `^0x[0-9a-f]{64}$`）、`execution_steps_attempt_shape`（NULL 或 1..128 可打印）；部分唯一索引 `CREATE UNIQUE INDEX execution_steps_open_uniq ON execution_steps (intent_id) WHERE state = 'issued'`。

**终态语义**：`converged`（010 返回分类结果，含 sent/refused_*）、`refused`（可确定未发生任何外部副作用，例如 010 报「无该 step 的尝试且门禁拒绝」——重试须重新过门禁）、`unknown`（结果未知，先对账）。`issued` 越久不改写，是一条待对账证据。

---

## Table 4 — `execution_events`（append-only 执行事件日志；FR-10/FR-12/FR-13/FR-15）

**归属**：011。**语义**：意图生命周期与调度生命周期的追加日志（对标 `nonce_binding_events`/`withdrawal_request_audit` 的 append-only 形态）；`intent_id` 不设 FK（审计约束不了任何东西，且必须能记录未被准入的拒绝，沿 000007 Table 5 先例）；行永不更新/删除。

```text
event_id          BIGINT      GENERATED ALWAYS AS IDENTITY
intent_id         TEXT        NOT NULL
kind              TEXT        NOT NULL
from_state        TEXT                  -- state_changed 时
to_state          TEXT                  -- state_changed 时
lease_version     BIGINT                -- 发生时资格版本（证据）
step_id           TEXT                  -- 步骤相关事件
attempt_id        TEXT
revision_version  BIGINT                -- 修订消费/对账观察的来源版本
detail            TEXT        NOT NULL DEFAULT ''
at                TIMESTAMPTZ NOT NULL  DEFAULT now()
```

`execution_events_kind_check` 闭集：`admitted, admission_refused, claimed, released, taken_over, revoked, stall_flagged, state_changed, step_issued, step_converged, step_refused, step_unknown, reconcile_observed, revision_applied, projection_stale, projection_refreshed`。
索引：`CREATE INDEX execution_events_intent_time_idx ON execution_events (intent_id, at)`。

---

## Table 5 — `request_status_projection`（仅显示的状态投影；FR-09，010 Q1/J5）

**归属**：011。**语义**：每 request 一行；**引用身份，不复制事实**（不存哈希/回执/费用）；两路输入各自版本单调（`state_version` 来自 Table 1，`lifecycle_version` 来自 010 权威修订版本）；`freshness='possibly_stale'` 必须能表达且有时间戳；投影可由权威重建（派生数据）。

```text
request_id            TEXT        NOT NULL  -- PK
intent_id             TEXT        NOT NULL  -- UNIQUE；FK → payment_intents
execution_state       TEXT        NOT NULL  -- Table 1 状态的显示副本
state_version         BIGINT      NOT NULL  -- 来源版本（011 侧）
lifecycle_attempt_id  TEXT                  -- 010 当前尝试身份引用（不复制事实）
lifecycle_version     BIGINT      NOT NULL DEFAULT 0  -- 已消费的权威修订版本
lifecycle_observed_at TIMESTAMPTZ           -- 最近一次成功读到权威的时间
freshness             TEXT        NOT NULL  -- confirmed | possibly_stale
stale_since           TIMESTAMPTZ           -- 非空 ⇔ possibly_stale
updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
```

**应用规则（SQL 级）**：011 侧输入 `... SET execution_state=$s, state_version=$v, updated_at=now() WHERE request_id=$r AND state_version < $v`；010 侧输入 `... SET lifecycle_attempt_id=$a, lifecycle_version=$lv, lifecycle_observed_at=now(), freshness='confirmed', stale_since=NULL WHERE request_id=$r AND lifecycle_version < $lv`；读失败/未确认 `... SET freshness='possibly_stale', stale_since=coalesce(stale_since, now()) WHERE request_id=$r AND freshness='confirmed'`。旧版到达影响 0 行且不报错（不覆盖新版，Q1）。

**约束**：`request_status_projection_pkey PK(request_id)`、`request_status_projection_intent_uniq UNIQUE(intent_id)`、`request_status_projection_intent_fkey`、`request_status_projection_state_check`、`request_status_projection_freshness_check`、`request_status_projection_stale_consistency CHECK ((freshness = 'possibly_stale') = (stale_since IS NOT NULL))`。

**决策禁令**：该表只在显示/查询路径被读；领取、续租、接管、推进、对账、结束恢复追踪零读该表（R9；quickstart V9 断言）。

---

## Table 6 — `execution_caller_permission`（固定接口权限；FR-01）

**归属**：011（R2）。**语义**：准入接口的调用方固定权限；默认不存在/FALSE 即 fail-closed；只由 `withdrawal-exec permission-set/revoke` 写；不复用 007 `can_create`。

```text
caller_id    BIGINT      NOT NULL  -- PK；FK → caller(caller_id)
can_execute  BOOLEAN     NOT NULL DEFAULT FALSE
updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
updated_by   TEXT        NOT NULL DEFAULT ''
```

**约束**：`execution_caller_permission_pkey PK(caller_id)`、`execution_caller_permission_caller_fkey FK→caller`。

---

## Table 7 — `execution_ops_audit`（操作员操作审计 + 尝试去重；FR-15/R14）

**归属**：011。**语义**：受控操作员路径的追加审计；唯一去重键只有 `operation_id`（23505 → 回滚 → 按 operation_id 回读 → 同输入报已记录结果，异输入 `operation_conflict` 且零写入，沿 008 R7 协议）。

```text
audit_id        BIGINT      GENERATED ALWAYS AS IDENTITY
operation_id    TEXT        NOT NULL
action          TEXT        NOT NULL  -- permission_set | permission_revoke | claim_revoke | projection_refresh
intent_id       TEXT        NOT NULL DEFAULT ''
caller_id       BIGINT
subject_version BIGINT                -- claim_revoke 的 expected_lease_version
outcome         TEXT        NOT NULL  -- applied | nop | refused
operator        TEXT        NOT NULL
reason          TEXT        NOT NULL DEFAULT ''
evidence        TEXT        NOT NULL DEFAULT ''
detail          TEXT        NOT NULL DEFAULT ''
recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
```

**约束**：`execution_ops_audit_pkey PK(audit_id)`、`execution_ops_audit_operation_id_uniq UNIQUE(operation_id)`、`execution_ops_audit_action_check`、`execution_ops_audit_outcome_check`。

---

## 状态机（FR-12）

### A. 执行请求状态（Table 1 `state`）

```text
admitted ──claim CAS──▶ claimed ──首个发送步骤 issued──▶ executing
executing ──步骤收敛为 unknown/pending_unknown/reconcile_required──▶ reconciling
reconciling ──对账判定「未发送」且重新过门禁后发新步骤──▶ executing
executing | reconciling ──权威事实：生效且确认──▶ completed
executing | reconciling ──权威事实：回执失败/Transfer 不符──▶ failed
completed | failed ──权威修订：回执失效/确认回退──▶ revised
revised ──重新过门禁后发新步骤──▶ executing
```

**两类转移的守卫严格分离**（M2 机制，R10/R9）：

| 转移类 | 例子 | 守卫 |
|---|---|---|
| **发送使能类（claim-fenced）** | `admitted→claimed`、`claimed→executing`、`reconciling→executing`、`revised→executing` | CAS `(state, state_version)` **且** `EXISTS(执行者持有的有效 claim: owner+lease_version+active+未过期)`；同时在同一读序内通过全部门禁（授权/暂停/恢复版本/资格） |
| **事实类（authority-driven）** | `→reconciling`（未知落盘）、`→completed`、`→failed`、`→revised` | 只 CAS `(state, state_version)` + 版本单调；**不**消费授权、**不**需要 claim（链事实观察/修订/对账记账不是新的资产执行，M2） |

- 非法转移（状态不符、版本不符、无 claim 的发送使能、无来源版本的修订）→ 拒绝并记 `state_changed` 之外的拒绝事件/错误，零写入（章程 V/VI）。
- **refusal 不是转移**：推进被门禁拒绝时意图状态不变，只记事件与指标；「准入后未发生任何外部发送」不等于失败。
- 终态性：`completed` 可被权威修订（→`revised`）；`failed` 对自动执行是终止态（不自动重付），仅权威修订可再改。

### B. 领取状态（Table 2 `state`）

```text
(无行) ──CAS 首次领取──▶ active ──自然过期 / 停滞 / 撤销 / 释放──▶ {released, revoked} 或（过期停在 active）
active ──续租──▶ active（同版本）
任意非 active 或过期或停滞 ──CAS 换代（lease_version+1）──▶ active（新 owner）
```

`released` = 持有者优雅退出；`revoked` = 操作员受控撤销（R14，需证据）或停滞接管时对旧版本的失效；`expires_at <= now()` 的 `active` 行不是终态但已失格（无需物化）。

### C. 步骤状态（Table 3 `state`）

```text
issued ──010 返回 sent/refused_*──▶ converged
issued ──可确定无尝试且无副作用──▶ refused（重试须重新过门禁并取新 step）
issued ──010 返回 pending_unknown/reconcile_required 或响应丢失且不可判定──▶ unknown（先对账）
```

`issued` → 终态必须经收敛事务（claim-fenced；若资格已换代，旧持有者不能收敛——由新持有者的对账观察接管）。

---

## Transaction catalog（行为；SQL 形状在 contracts/）

| # | 事务 | 内容 | 边界与守卫 |
|---|---|---|---|
| T-admit | 准入并创建意图（`serve` 路由） | 读 007 请求行 → 认证与固定权限（事务外已定）→ 门禁表 `SHARE` → 注册表行 `FOR SHARE` → 授权行 `FOR SHARE` → scope 行 `FOR SHARE` → `INSERT payment_intents` → `INSERT execution_events('admitted')` → `INSERT request_status_projection` | 一个事务；23505（`payment_intents_request_uniq`/`payment_intents_authorization_uniq`）→ 回滚 → 回读 → 同 basis 报已记录、异 basis 报 `conflict`；零意图创建是拒绝路径 |
| T-claim | 领取/接管 | 门禁表 `SHARE` → `SELECT … FOR UPDATE`（或无行）→ 在锁下重估可领取谓词与全部门禁 → `INSERT … ON CONFLICT DO UPDATE … WHERE <可领取> RETURNING lease_version` → `state_changed` 事件 → 状态转 `claimed` | CAS 单行；失败者零副作用；停滞接管在同一事务内失效旧版本并接班（R6） |
| T-renew | 续租 | `SELECT … FOR UPDATE` → owner+版本+active+未过期 → `UPDATE expires_at, last_heartbeat_at` | 短事务、语句守卫；0 行/失配 → `ErrClaimLost` |
| T-step-issue | 步骤落盘（发送前） | 门禁表 `SHARE` → claim 行 `FOR UPDATE`（核验）→ 暂停/恢复版本 → 授权/scope `FOR SHARE` → 注册表/绑定观察 → `INSERT execution_steps(issued)` → 进度推进 + `step_issued` 事件 →（`claimed→executing` 若需要） | 一个事务；**外部调用在事务外**；无有效资格/门禁不过 → 不落步骤 |
| （外部调用） | `LifecycleAdvancer.Advance`（010） | 只带身份、围栏（owner/lease_version）、动作与 `step_id`；010 内部构造、签名、落盘、发送 | 不得在持有事务锁时调用；超时/错误分类 |
| T-step-converge | 收敛 | claim 行 `FOR UPDATE` 核验 → `UPDATE execution_steps SET state(终态), attempt_id, tx_hash, outcome_class, revision_version` → 进度推进 → 事件 → 需要时状态转 `executing/reconciling` → 更新投影 | 事务边界=状态转移+证据落盘（章程 VI）；资格换代则收敛失败，由新持有者走 T-reconcile |
| T-reconcile | 对账 | 读 010 权威（`LifecycleReader`）→ 按 `step_id` 收敛开放步骤（converged/refused/unknown）→ 进度推进 + `reconcile_observed` 事件 → 需要时状态转 `reconciling/executing/completed/failed` | 无门禁表锁（非发送决策）；版本单调；读失败不改状态只标可能过期 |
| T-revision | 修订消费 | 读 010 修订（按版本升序）→ 若 `revision_version > 已存` → 权威驱动转移（`completed→revised` 等）+ `revision_applied` 事件 + 投影更新 | 不消费授权、不需要 claim（M2）；版本单调，旧版忽略 |
| T-projection | 投影应用/标脏 | 011 侧输入与 010 侧输入各自版本守卫更新；失败置 `possibly_stale` | 只写 Table 5；决策路径不读它 |
| T-ops | 操作员受控操作 | `permission_set/revoke`、`claim_revoke`、`projection_refresh`：精确条件更新 + 事件 + `execution_ops_audit`（operation_id 去重） | 证据不足拒绝；版本失配拒绝；0 行不追改 |

---

## Concurrency & crash argument（为什么不会双执行、双意图、未知变失败）

1. **双执行不可能**：有效资格是单行上的单值元组 `(owner_id, lease_version, state='active', expires_at > now())`；所有执行类写入都以该元组做精确条件更新（0 行即失格），010 的发送门禁再以「调用方呈递版本 = 行内活跃版本」核验（R5）。换代是单行原子更新（`lease_version+1`），所以任一时刻至多一个版本可写、至多一个版本可发送；旧版本的任务因版本失配被拒。
2. **双意图不可能**：`UNIQUE(request_id)` + insert-first：并发/重试/重启在约束上收敛，异 basis 则冲突拒绝（R3）。
3. **未知不变失败**：010 的未知分类映射到 `unknown`/`reconciling`；开放步骤存在时不允许发新步骤（部分唯一索引结构保证）；重发只在「可确定未发生副作用」或「权威要求重试」时发生（Q3「未知先对账」）。
4. **崩溃点全覆盖**（US4）：步骤落盘前崩溃 → 无副作用（未调用）；落盘后调用前 → 开放步骤待对账/重发（同一 `step_id`，010 幂等）；调用中/响应丢失 → 对账判定；收敛前崩溃 → 开放步骤待对账；收敛事务内崩溃 → 事务原子回滚或提交；投影失败 → 标可能过期，不影响决策。
5. **停滞与接管竞态**：见 R6 两条时间线；进度写与接管写互斥于 claim 行，谓词在锁下重估，无假接管。
6. **修订竞态**：投影与状态都按来源版本单调；旧版到达影响 0 行（010 Q1）。
7. **重启重建**：worker 启动先做一次全量对账扫描（开放步骤 + 非终态意图 + 投影追赶），再进入循环；内存中无任何执行真相（章程 III）。

---

## Upstream read contracts（消费侧只读清单）

| 来源 | 读取对象 | 形态 | 用途 |
|---|---|---|---|
| 006 | `indexer_pause`/`log_pause`/`deposit_pause` 行存在、`reorg_recovery` 活跃行、`reorg_recovery_events` 版本 | 门禁表 `LOCK … IN SHARE MODE` + 单语句快照（沿 `internal/signer/gates.go:25,33-40`） | 每次发送使能前；只读，不解除 |
| 007 | `withdrawal_requests` 行（状态 `accepted`、身份/金额字段）、`withdrawal_authorizations` `FOR SHARE`、`withdrawal_authorization_scopes` `FOR SHARE` | 与 009 同序（grant → scope） | 准入与每次推进 |
| 008 | `nonce_wallet_registry` 行 `FOR SHARE`（注册/可用性）；绑定与 hold 观察（`ReadProvider` 适配，沿 `internal/signer/binding_live.go`） | 五类消费语义（matches/absent/conflict/paused/terminal/read_failed） | 准入（注册表）与推进（hold/绑定观察） |
| 010 | 尝试身份、权威事实、修订版本、未知恢复接口 | `LifecycleReader`（[contracts/lifecycle.md](contracts/lifecycle.md)） | 对账、投影消费、崩溃重建；不复制事实 |

011 对这些表只有 `SELECT`/门禁锁与非 011 表的零写入；导入边界禁止调用 006/007/008 的写函数（plan 的验证计划固定）。

---

## Retention & secrecy notes

- `payment_intents`/`execution_claims`/`execution_steps`/`execution_events`/`execution_ops_audit` 永不删除；`request_status_projection` 是派生显示行，可由权威重建（重建不等于创建新意图）。
- 所有表不含密钥、凭据、原始签名字节、交易原始字节；`evidence`/`detail` 只写身份、版本、分类与链上引用。
- 无浮点列；无数组/JSON 承载业务语义；无触发器和存储过程（纯 DDL + 应用参数化 SQL，沿 000007/000008/000010 约定）。
