# Research: 011 Withdrawal Execution Worker（提款执行）

**Branch**: `011-withdrawal-executor` | **Date**: 2026-09-17 | **Spec**: [spec.md](spec.md) (clarify 2026-09-17; M1n/M3 已裁决，M2 以既定契约消除；zero markers)

**Inputs (read-only)**: `docs/workflow-010-011-parallel.md` 联合设计合同 v1 J1–J6（本分支已合入 `a3ac87e` + C11 修正 + v1 节）；010 spec `bd59754`（Q1–Q3 澄清，只读，不合入本分支）；charter `.specify/memory/constitution.md` v1.1.0；已交付上游代码（007 `/internal/withdrawal`、008 `/internal/nonce`、009 `/internal/signer`、006 `/internal/indexer`、migrations 000007/000008/000009/000010）。

**性质**: 本文件只做 011 侧设计决策，机制选择均为 plan 待验证项；业务语义全部继承已批准契约（OC-1–OC-7、PB-C1/C2、010 Q1–Q3、011 M1n/M2/M3），不新增业务裁决，不预批准阈值。

---

## R1 — 执行 worker 进程形态与准入入口：`serve` 上的调用方准入路由 + 独立 `withdrawal-worker` 子命令

**Decision**: 011 拆成两个运行面：

- **准入面**（写 `payment_intents`）：挂在既有 `serve` HTTP 监听器上的新路由（`POST /withdrawals/{id}/execution`，Bearer 认证与 007 同形），因为它复用 007 已认证的调用方通道与错误面，不需要新角色、新监听器（章程 XIII；`internal/app/serve.go:317-326` 已是路由汇合点，`internal/app/withdrawalhttp.go:276-280` 提供 Bearer 解析形态）。
- **执行面**（领取/续租/接管/调用 010/投影）：独立的 `withdrawal-worker` 子命令（长驻循环），无自有监听器；`withdrawal-exec` 操作员子命令承载受控操作路径（R14）。形态沿 009 的「单二进制 + 子命令」（`signer-serve`/`signer-auth`）与 008 的 `nonce-admin`。

**Rationale**: 011 不持有密钥材料，没有 009 那种「进程隔离即签名边界」的安全要求，因此不需要独立监听器；准入是调用方接口，天然属于 `serve`。worker 独立成进程使「领取/续租/接管」可被单独启停与观察，且多实例竞争正是 FR-04 要验证的行为。

**Alternatives considered**:
- *worker 内部轮询自动准入所有 Accepted 请求*：与 FR-01 的「调用方认证 + 固定接口权限」矛盾（没有调用方就没有这两个门禁），且把「请求进入执行」的显式意图变成隐式副作用。
- *011 自己的监听器（009 形）*：多一个监听面与配置面，无安全理由；章程 XIII 拒绝无理由的新监听器。
- *准入也放进 worker*：调用方认证属 HTTP 语义，放进 worker 就要在 worker 里重建 HTTP 服务。

**Evidence**: `internal/app/serve.go:307-326`（子 mux 与路由注册、无新监听器）；`internal/app/withdrawalhttp.go:132-160,217-246,276-280`（Bearer `txh_…` 解析、401/404 形态）；`internal/app/signerserve.go`（009 子命令先例，用于对比）。

---

## R2 — 调用方身份与固定接口权限载体：只读复用 007 `caller`/`api_key` + 011 自有的 `execution_caller_permission`

**Decision**: 准入的调用方认证**只读复用** 007 的 `Authenticate`（`api_key` → `caller`），固定接口权限由 011 自有表 `execution_caller_permission(caller_id PK/FK, can_execute, updated_at, updated_by)` 承载，只由 011 操作员子命令写入（R14），默认 FALSE（fail-closed）。准入要求：认证通过 + `can_execute=TRUE` + 请求 `caller_id` 与认证调用方一致。

**Rationale**: FR-01 把「调用方认证」与「固定接口权限」列为两个独立门禁，与 007 FR-03 的结构一致（`Caller.CanCreate` 在 007 中被注释为 "fixed interface permission (FR-03b)"，`internal/withdrawal/auth.go:46-50`）。011 不复用 `can_create`：那是创建接口的权限，复用会把两个接口的授权边界合并，且无法独立撤销。`execution_caller_permission` 是加法表（不改写 007 的列与行），与 009 自建 `signer_caller` 的先例同构（009 plan 说明：上游表不能承载本边界的调用方命名空间而不破坏上游契约）。

**Alternatives considered**:
- *复用 `caller.can_create`*：语义合并（创建 ≠ 执行），且撤销创建权限会连带关闭执行，或反之；FR-01 的「固定接口权限」失去独立载体。
- *007-extension 给 `caller` 加列 `can_execute`*：改动 007 已应用表的行/列语义，供给入口归 007-extension 所有（联合合同 J6），011 不能单方面改；且会让 011 的权限生命周期依赖上游迁移。
- *部署配置里的 caller 白名单*：不可按调用方独立撤销、无审计、重启才生效；与服务端凭证模型不一致。

**Evidence**: `internal/withdrawal/auth.go:46-50,127-167`（认证与 Caller.CanCreate 注释）；`migrations/000007_withdrawal_creation.sql:53-78`（caller/api_key 行从未删除，FK 可长期成立）；009 plan「Authorization carrier closure」节（自建调用方命名空间的先例与理由）。

---

## R3 — 稳定付款意图：`intent_id` 取自 PB scope 的运营方声明值，011 在准入事务内 insert-first 持久化

**Decision**: 准入通过后，011 在一个事务内 `INSERT INTO payment_intents`（`intent_id` = PB scope 行声明的 `intent_id`）并写 `execution_events`；并发/重试/重启由 `UNIQUE (request_id)` 与既有行读取收敛到同一意图（insert-first → 23505 → 回读 → 身份一致则复用）。intent 行先于任何 008 预留存在（008 预留只能由 010 在 intent 存在后发起）。

**Rationale**: `intent_id` 的**值**不是 011 现场生成的随机量：PB 载体 `withdrawal_authorization_scopes.intent_id` 是「operator-declared intent identity (011 binds it later)」（`specs/012-007-authorization-carrier/data-model.md:15`），007 supply 入口已接受 `--intent-id`（`specs/012-007-authorization-carrier/contracts/supply-scope.md:10`），009 又以该值做一致性比对（`internal/signer/gates.go:287-294`）。若 011 另铸新值，则 scope、009 请求声明、010 尝试、008 绑定将无法用同一 `intent_id` 串起来（联合合同 J1 的身份链）。一 request 至多一 intent 由 `UNIQUE (request_id)` 物理保证；授权绑唯一 intent 由 `UNIQUE (authorization_id)` 物理保证（007 已用同一约束保证一授权一请求：`migrations/000007_withdrawal_creation.sql:103`）。

**Alternatives considered**:
- *011 生成 UUID 作为 intent_id，另建映射表到 scope 声明值*：多一张表与一次映射，且破坏 J1「同一身份串全链」的可核验性；009/010 的比对字段将被分裂成两个身份。
- *把 intent 创建推迟到 008 预留时*：违反 OC-1/C1「intent 先于 008 预留持久化」；008 只绑定既有 intent，不能触发创建。
- *允许无 scope 行时创建 intent*：违反 PB Q-B（逐笔 fail-closed、不静默接受不可验证授权）；011 FR-07 同样要求不可验证即拒绝。

**Evidence**: `migrations/000010_withdrawal_authorization_scopes.sql:25-45`（scope 声明 intent_id/request_id/sender/版本/用途）；`specs/012-007-authorization-carrier/data-model.md:15`；`internal/signer/gates.go:62-64,283-297`（scope 读取与比对先例）；`migrations/000007_withdrawal_creation.sql:86-112`（`withdrawal_requests_caller_key_uniq` / `withdrawal_requests_authorization_uniq` 的单索引收敛先例）。

---

## R4 — 领取与租约载体：`execution_claims`（`UNIQUE(intent_id)` + 单调 `lease_version` + DB 时钟到期），CAS 领取、锁先行续租、抖动心跳

**Decision**: 联合合同 J2 已指定载体：011 新建 `execution_claims`，`intent_id UNIQUE`、worker 身份、单调 `lease_version`、`expires_at`、状态。领取用一条**原子 CAS**（`INSERT … ON CONFLICT (intent_id) DO UPDATE … WHERE <可领取条件> RETURNING lease_version`），续租用**锁先行短事务**（`SELECT … FOR UPDATE` → 校验 owner+版本+active → 延期 → COMMIT），心跳带 ±10% 抖动；有效期判定只用 PostgreSQL `now()`，应用时钟只决定「何时再试」。

**Rationale**: `internal/indexer/lease.go` 是仓库内已验证的同构实现：CAS 的失败者在行上重估 WHERE 后拿不到行（`ON CONFLICT … WHERE indexer_lease.expires_at < now()`，`internal/indexer/lease.go:55-64`），续租先取行锁再验 owner（`:172-209`），丢失时返回 `ErrLeaseLost` 并要求立即停写（`:44-47`），心跳 3:1 于 TTL 且带抖动（`:27-28,36,233-239`）。011 的增量只有两处：`lease_version` 是**每 intent 单调**（J2 语义，取代全局 fencing token），以及可领取条件包含「过期 / 已撤销 / 已释放 / 停滞」四种（R6）。到期不物化为状态：`state='active' AND expires_at <= now()` 即失格，任何写路径与 010 的读路径都按同一谓词判。

**Alternatives considered**:
- *PG advisory lock 作为执行权*：锁随会话消亡，跨崩溃不可审计、无版本号、无法被 010 只读核验（J2 要求 010 读行核验）。
- *只锁 intent 行（`SELECT … FOR UPDATE`）*：不能表达「有期」，且 010 无法只读核验「当前资格」；会退化成进程身份禁令。
- *独立协调行 + claim 行两表*：J2 已定 `execution_claims` 单行载体；两行会引入新的锁对象与不一致窗口。
- *应用时钟比较*：重启/时钟漂移下不可复现；仓库所有有效期判定都用 DB 时钟（009 `evaluate` 用 DB now；indexer 租约同）。

**Evidence**: `internal/indexer/lease.go:24-64,142-239`；`migrations/000008_nonce_manager.sql:82-120`（同库同风格的单调版本/状态约束写法）；联合合同 J2（`docs/workflow-010-011-parallel.md:111-114`）。

---

## R5 — 自我围栏与 010 侧资格核验：011 写路径版本守卫 + 010 发送门禁按行锁顺序读 claim 行

**Decision**: 资格 = `(claim.state='active' AND expires_at > now() AND owner_id=执行者 AND lease_version=执行者所持版本)`。两层使用：

1. **011 自我围栏**：所有执行类写入（领取、续租、步骤落盘、状态转移、进度水位）都是精确条件更新（`WHERE intent_id=$1 AND owner_id=$2 AND lease_version=$3 AND state='active' AND expires_at > now()`），影响行数 ≠1 即判定失格，回滚并停止动作（indexer 同构：影响 0 行即 `ErrLeaseLost`）。
2. **010 侧围栏**：`AdvanceRequest` 携带 `(intent_id, owner_id, lease_version)`；010 的发送门禁事务在同一读取序列内按 `intent_id` 读 claim 行并核验「活跃版本相等 + 未过期 + 无撤销」，失配即拒绝（联合合同 J2/J4）。已发出者无法撤回：010 决策先行且已进入不可取消的发送阶段时记在途/未知并对账。

**Ordering obligation (recorded, joint)**: J4 的「失效先行则拒绝」需要 010 的门禁读与 011 的资格变更写**在 claim 行上互斥**：仅靠 MVCC 快照读（裸 `SELECT`）时，011 的撤销提交可能落在 010 读快照之后而不可见，保证不成立。因此 011 的资格变更写（`UPDATE execution_claims …`）与 010 的门禁读必须落在同一行锁语义上：010 读 claim 行时取行共享级锁（`FOR SHARE`/`FOR KEY SHARE` 形态，009 对 007 grant 行即用 `FOR SHARE`：`internal/signer/gates.go:51-52`），使「撤销先行 → 010 读到新版本/撤销态并拒绝；门禁读先行 → 011 的撤销等待该门禁事务提交」。具体 SQL 子句由 010 的 plan 选定；011 的义务是把 claim 行的变更做成**单行精确版本更新**（不批量、不自造乐观路径），并接受任何 010 侧的行锁等待。若 010 的 plan 无法提供该行锁序，按 FR-15 要求报告缺口供裁决，**不自行扩大合法在途范围、不加宽限期**。

**Rationale**: 009 已经用同一模式解决过「上游写 → 本侧决策」的先后（grant 行 `FOR SHARE` + 006 门禁表 `SHARE` 锁，`specs/009-signer-service/contracts/gates.md:105-111,177-189`）。J4 明确「发送决策先行且已不可取消记在途未知并对账，失效先行则拒绝」——该二分只有在存在行级互斥时可判定；011 侧把变更做成精确单行更新是让该互斥可能的必要条件。claim 行 010 只读（J2），因此不存在 010 写 011 表的方向。

**Alternatives considered**:
- *010 只裸读 claim 行*：见上，J4 保证在撤销并发下不成立；这是必须报告的缺口而不是可接受的宽限。
- *011 在调用 010 前把 010 的发送决策锁在自己事务里*：跨进程外部调用不得在持有事务锁期间发生（章程 VI/IX；009 R6 同规则），会把 DB 事务暴露在签名/RPC 延迟下。
- *把围栏做成 011 端「最后检查」*：Q3 已明确「检查通过不算在途」；011 的检查只做排序与早拒，最终裁决在 010 门禁。

**Evidence**: `internal/indexer/lease.go:44-47,196-209`（影响 0 行即失锁）；`internal/signer/gates.go:25,51-52`（`LOCK … IN SHARE MODE` + 行 `FOR SHARE` 的锁序先例）；`specs/009-signer-service/contracts/gates.md:105-123`；联合合同 J2/J4（`docs/workflow-010-011-parallel.md:111-123`）。

---

## R6 — 停滞判定、资格失效与自动接管（M3）：进度水位 + 行锁下重估的停滞谓词 + 原子换代

**Decision**: 三件互相独立的事，语义严格分开：

1. **进度（progress）**：进度不等于心跳。`execution_claims.last_progress_at` 只在以下耐久事实发生时推进（DB `now()`）：(a) 一个执行步骤发出/收敛（`execution_steps` 行创建或进入终态）；(b) 消费到 010 权威修订且版本前进（R9）；(c) 未知对账观察落盘（`reconcile_observed` 事件，含权威来源版本）；(d) 状态转移落盘。续租**不**推进进度；持续重领**不**是进度（M3 原文）；一次失败的读取（源头不可达）不推进。
2. **停滞判定与失效**：停滞判据 = `state='active' AND now() - last_progress_at >= stall_window`。接管候选在**同一事务**内 `SELECT … FOR UPDATE` 该 claim 行 → 在锁下重估停滞谓词与全部当前门禁 → 成立则把 `lease_version+1`、换 owner、`expires_at=now()+ttl`、`last_progress_at=now()`、清 `stall_flagged_at`，并写 `taken_over` 事件（含前 owner、前版本、前 `last_progress_at`、窗口值作为证据）。这一事务同时完成「先使旧资格失效」与「新版本接管」，旧版本永不复活（联合合同 J2）。
3. **标记义务**：worker 的周期清扫对「停滞但尚无接管者」的 claim 做幂等标记（`stall_flagged_at` + `stall_flagged` 事件 + 指标），保留完整证据（FR-15），不静默丢弃、不重置业务状态。

**防误判（两条竞争时间线）**：接管与进度写在 claim 行上互斥——(a) 进度先行：接管事务在锁下看到新 `last_progress_at`，谓词为假，不接管（旧资格继续有效，两个执行者不并存）；(b) 接管先行：旧持有者的进度写影响 0 行（版本失配）→ 按失格处理并停止动作，其后续任何写与 010 发送均被围栏拒绝。停滞后若原持有者恢复动作，它必须先重新领取（M1n 同等竞争），取得新版本并重验全部门禁。

**Rationale**: M3 明确「无进展 MUST NOT 直接证明租约失效」且「不得让两个 worker 同时持有有效执行资格」；把失效做成**带证据的精确版本更新**而不是推断，正好满足两句：旧租约到期仍是自然失效路径（持有者死亡/停机），而活跃但卡死的持有者由停滞规则显式失效。自动接管不需要人工关卡（M3 选 A），操作员撤销路径（R14）只作补充，不作为默认流程。把进度与心跳分离，避免「持续重领/心跳即进展」的假进展。

**Alternatives considered**:
- *无进展即视为租约过期*：M3 明文禁止（无进展不证明失效）；也会让两个 worker 都自认为有效。
- *只靠心跳/租约到期*：活跃但卡死的 worker 心跳不停，请求永久占位，自动接管政策落空。
- *停滞后要求人工批准才可接管*：M3 明确拒绝「因单纯无进展一律增加人工审批关卡」。
- *心跳即进度*：把持续重领当成业务进展，M3 明文禁止。

**Evidence**: M3 全文（`specs/011-withdrawal-executor/spec.md:28`）；联合合同 J2/J4；`internal/indexer/lease.go:150-164`（CAS 原子换代先例）；`internal/nonce/hold.go`/`admin.go`（既有「有证据才允许状态变更」的操作员路径风格，仅作形态参考）。

---

## R7 — 同等重领、公平竞争与退避（M1n）：同一 CAS、无身份禁令、新版本 + 全门禁重验

**Decision**: 重领与首领会走**同一条 CAS**，不设进程身份禁令、不设优先级队列、不记忆前持有者。重领必然拿到 `lease_version+1`（可与旧资格区分），必须重验全部适用门禁（R5 R10：授权/暂停/恢复版本/执行资格），旧任务因版本失配而继续受围栏约束（R5）。竞争失败者按有界退避 + ±25% 抖动重试；退避只控制重试节奏，不构成任何门禁宽限（联合合同 J4「无宽限、无 TTL」）。

**Rationale**: M1n 的原文语义是「安全性由新资格和旧版本隔离保证，强制换 worker 身份不能替代这些保证」；同等竞争要求实现上不能有身份维度的准入差异。抖动抑制延续 indexer 心跳的 ±10% 思路，避免多实例同频争抢同一行（`internal/indexer/lease.go:36,233-239`）；退避参数为提案（见末节），不是已批准阈值。

**Alternatives considered**:
- *失格者冷却期/身份拉黑*：M1n 明文不增加进程身份禁令。
- *前持有者优先（sticky）*：会让失格判定与接管依赖身份记忆，且与自动接管政策冲突；也会让「持续重领算进展」的假象有激励。
- *无退避的紧密轮询*：在 PG 行锁上制造无谓争抢与日志噪声，章程 IX 要求有界重试。

**Evidence**: M1n 全文（`specs/011-withdrawal-executor/spec.md:26`）；联合合同 J2；`internal/indexer/lease.go:36,233-239`（抖动先例）。

---

## R8 — 011→010 边界：窄消费者接口（推进 + 权威读），步骤身份由 011 预分配、010 幂等收敛；011 不构造内容、不碰 nonce/签名

**Decision**: 011 只通过两个窄接口消费 010（联合合同 J1/C3）：

- `LifecycleAdvancer.Advance(ctx, AdvanceRequest{IntentID, RequestID, CallerID, OwnerID, LeaseVersion, RecoveryVersion, StepID, Action, AnchorAttemptID, ExpectedTxHash}) → AdvanceOutcome{Class, AttemptID, TxHash, RevisionVersion, Basis}`；`Action ∈ {first_broadcast, replay, replace}`。011 只传身份、围栏与动作；**不传** nonce、费用、签名、原始交易内容（010 拥有构造、009 调用、落盘与广播）。
- `LifecycleReader.Read(ctx, IntentID) → LifecycleFacts{Attempts[], CurrentAttemptID, RevisionVersion, Unknown{AttemptID, TxHash, RecoveryCondition}, Basis}`：供未知对账、投影消费与崩溃恢复重建执行位置；只读、不发。

`StepID` 由 011 预分配并在重试中保持不变；010 MUST 按 `(intent_id, step_id)` 幂等收敛到同一尝试身份（响应丢失重试不得产生第二尝试/第二替换）。`Class` 为闭集：`sent / refused_gate / refused_basis / pending_unknown / reconcile_required / unavailable`；`pending_unknown|reconcile_required` 映射到 011 的「未知对账中」，**永不**映射为失败或未付款（FR-08/Q3）。

**Rationale**: 联合合同 J1 明确归属：intent 行归 011、attempt 行归 010、attempt 经 FK 关联 intent 与绑定、不复制事实；J5 明确权威读与未知恢复接口由 010 提供、011 消费。011 预分配步骤身份与 OC-4「可信调用方预分配身份」同构（009 的 `signing_request_id` 由调用方声明：`specs/009-signer-service/spec.md` 上游传递，`internal/signer/gates.go` 消费）；`StepID` 解决的是「011 重试同一步」的收敛问题，`attempt_id` 解决的是 010 内部身份问题，两者不混淆。

**Alternatives considered**:
- *011 构造交易内容并传给 010*：010 的 Non-Goals 明确交易构造归 010（010 spec FR-01/FR-05）；011 构造会引入第二份内容真相与授权/费用判定的越权。
- *010 轮询 011 的任务队列*：把推送语义反转，011 的领取/租约语义失去消费者，且 010 会耦合调度状态；J1 的方向是 011 调 010。
- *用 attempt_id 做步骤幂等键*：attempt 身份归 010 预分配（OC-4），011 不能在调用前知道；用它做收敛键会让响应丢失后无法判定是「同一步重试」还是「新一步」。

**Evidence**: 联合合同 J1/J5（`docs/workflow-010-011-parallel.md:105-128`）；010 spec D6/D9/FR-01/FR-05（只读引用）；`specs/009-signer-service/plan.md:402-404`（消费方定义窄接口、具体类型在接线时落地的先例）。

---

## R9 — 投影与修订消费：版本单调应用、新鲜度标记、决策路径零读投影、补偿刷新不设秒级 SLA

**Decision**: 011 自有 `request_status_projection`（每 request 一行）只做显示：

- 输入分两路、各自**版本单调**：011 执行态（来源 `payment_intents.state_version`）与 010 生命周期引用（来源 `LifecycleFacts.RevisionVersion`）。应用规则统一为「输入版本 > 已存版本才写入」，旧版到达一律忽略（0 行），永不覆盖新版（J5）。
- 投影**引用身份不复制事实**：存 `lifecycle_attempt_id` + `lifecycle_version` + `lifecycle_observed_at`，不存交易哈希/回执/费用明细（010 FR-13 语义）。
- 新鲜度：成功读到权威即置 `freshness='confirmed'` 并记 `lifecycle_observed_at`；读取失败/未确认时置 `freshness='possibly_stale'`（保留旧数据与 `stale_since`，不降级成未核验的当前结论）。显示端必须呈现该标记。
- **决策零读投影**：领取、续租、接管、推进、对账、结束恢复追踪一律读 011 权威行（intent/claim/step）+ 010 权威事实 + 当前门禁；投影不是许可依据。此约束用静态边界测试 + 运行断言双重固定（quickstart V9）。
- 补偿刷新：worker 周期消费 + 启动追赶（按版本顺序）+ 受控操作员强制刷新（R14，带证据与操作去重）。**不设秒级 SLA、不写业务时限**（C11：未指定；若确需另行提出有依据建议）；但没有 SLA 不等于允许无限陈旧：机制上始终存在「周期消费 + 失败即标可能过期 + 强制刷新」三条收敛路径。

**Rationale**: 010 Q1（选 C）与 011 FR-09 的原文要求「保留来源版本与更新时间、旧版不覆盖新版、无法确认新鲜度必须标记、决策读验当前权威事实」；J5 要求「011 投影更新器按版本顺序消费；补偿刷新在过期信号下重读权威（不设秒级 SLA）」。把「引用身份」与「事实」分开，避免投影变成第二权威。

**Alternatives considered**:
- *查询时直读 010 权威（无缓存）*：显示会耦合 010 可用性，且 011 会依赖 010 的读接口在线；FR-09 允许并预期投影存在（「011 可以保存用于查询展示的状态投影」，010 Q1）。
- *带 TTL 的缓存*：TTL 到期即丢数据或接受陈旧，仍需过期标记；不如版本+新鲜度显式。
- *事件推送/通知（LISTEN/NOTIFY、外部总线）*：章程 XIII 拒绝新基础设施；J5 已选版本消费。
- *投影里存交易事实副本*：与「引用尝试身份不复制事实」相悖，且制造第二真相源（章程 III）。

**Evidence**: 010 Q1 全文（同仓库 sibling 工作树 `.slim/worktrees/prep-010-tx/specs/010-transaction-lifecycle/spec.md:28`；010 规格不在本分支）；011 FR-09；联合合同 J5（`docs/workflow-010-011-parallel.md:124-128`）；`internal/withdrawal/query.go:113-136`（同库「一个只读快照读多源、读不到即 unknown」的显示路径先例）。

---

## R10 — 授权消费与费用替换边界：准入绑定唯一 intent、每次推进重验、PB 条件式复用的费用维度归 010

**Decision**:

- 准入时读 007 `withdrawal_authorizations`（`FOR SHARE`）与 PB `withdrawal_authorization_scopes`（同一 `FOR SHARE` 序列，锁形态沿 `internal/signer/gates.go:51-64`），要求：`state='active'`、DB 时钟未过期、`caller_id/chain_id/asset/recipient/amount` 与 007 请求行相等、scope 存在且 `intent_id`（= 本次意图值）/`request_id`（= 本次请求）/`sender` 与注册表一致；`sender` 必须是 `nonce_wallet_registry` 中 `state='active'` 的已注册钱包（OC-2：注册证明可用性，不构成执行授权）。任一不满足 → 零意图创建（fail-closed，PB Q-B）。
- 每次发送类推进前重验同一组门禁（含授权当前有效性与 scope 版本、暂停、恢复版本、执行资格）——不依赖准入结论、不依赖历史成功（FR-11/Q3）。
- **费用替换的 PB 条件式复用由 010 在构造侧完成**（用途允许 + 三维费用全满足方可复用原授权，否则新授权）：011 无法评估费用维度，因为交易内容与费用由 010 构造；011 的义务限于 (a) 承认 `allows_fee_replacement=TRUE` 的 scope 是替换用途的前提，(b) 在 scope 不承认替换用途时拒绝以 `replace` 动作推进，(c) 不把「调度动作」当签发权、不自行签发或换绑授权。010 若判定不可复用，返回 `refused_basis`（需新授权），011 记录并暴露，不静默换绑、不重试成新授权。

**Rationale**: FR-07 把「调度 ≠ 签发」与「授权绑定唯一 intent」定为 011 义务，同时要求费用替换沿 PB 条件式复用；PB-C2 的三维费用判定需要候选交易的费用三元组，而该内容由 010 构造（010 FR-01/FR-05）。把判定放在能看到内容的一侧、把身份/用途/有效性判定放在两侧都做，是唯一不越权也不遗漏的分工。009 的先例：授权有效性在签名侧独立重验，但其 scope 读取与消费也在本侧完成（`internal/signer/gates.go:277-313`）。

**Alternatives considered**:
- *011 传费用三元组给 010 并自行判定复用*：011 不知道 010 将要构造的 gas 上限/费用（那正是 010 的构造职责），预传值会制造两份内容真相。
- *复用不检查用途、只查身份/有效性*：违反 PB-C1/C2 与 FR-07，等价于静默换绑。
- *准入即消费/标记授权为已用*：007 无「已消费」态且 011 不得写 007 表；绑定语义由 intent 的 `UNIQUE(authorization_id)` + 逐次重验表达。

**Evidence**: `migrations/000007_withdrawal_creation.sql:114-139`；`migrations/000010_withdrawal_authorization_scopes.sql:25-45`；`internal/signer/gates.go:48-64,277-342`；`migrations/000008_nonce_manager.sql:38-50`（注册表状态）；联合合同 C2/C4/J3。

---

## R11 — 迁移归属与编号：011 暂定 `000012_*`，纯加法；`000010` 已确认 PB 占用、`000011` 暂定 010；跨 lane FK 顺序缺口显式记录

**Decision**: 011 的新迁移为 `migrations/000012_withdrawal_execution.sql`（**暂定号**，J6）：
- 纯 DDL、纯加法：只新建 011 表，不改 000001–000010 的任何对象、不改写已应用迁移（规范与联合合同 J6；`internal/db/migrate.go:274-286` 已支持缺号补洞与乱序应用，`CheckCompatibility` 仍拒绝未应用/未知版本）。
- **编号为待核验项**：合并时按当时实际迁移集合核验；若 010 的 `000011` 先落地，011 的表引用不冲突；若顺序/号段被占用，按实际集合调整**未经应用的**新迁移编号（不改写已应用的）。
- **跨 lane FK 顺序缺口（记录，供裁决）**：J1 要求 010 的 attempt 行「经 FK 关联 intent 与绑定」。若 010 在其 `000011` 迁移里直接建 `tx_attempts.intent_id → payment_intents(intent_id)` 的 FK，而 `payment_intents` 由 011 的 `000012` 创建，则 goose 按版本升序应用时 `000011` 会先于 `000012`，FK 目标不存在 → 迁移失败。可接受的解法（由 010/011 合并顺序裁决，不在本 plan 自选）：(a) 010 的 intent FK 由 010 在其 `000012` 之后的迁移中补建；(b) 011 迁移号排在 010 之前；(c) FK 语义降级为不带约束的引用（需显式裁决与理由，章程 Database 要求「任何省略 MUST 有显式理由」）。本 plan 只记录该残差，**不预设改号策略**（C12）。
- 0011 只是暂定：`docs/workflow-010-011-parallel.md:129-130` 明确「合并时核验」。

**Rationale**: 章程 III/Database 与联合合同 J6 都要求迁移可复现、归属单一写者、已应用迁移不改写；而 J1 的 FK 要求与 J6 的暂定编号之间存在真实顺序耦合，必须显式报告而不是在合并时踩雷。

**Alternatives considered**:
- *011 先建 payment_intents 再让 010 改号*：改号本身属 010 的 plan 决策与合并期核验事项。
- *把 FK 拆到 011 侧反向引用 tx_attempts*：方向错误，attempt 归 010，且会让 011 依赖 010 的表。
- *两个 lane 合并成一条迁移*：违反「同一文件同一步骤只由一个阶段写入」（P4）。

**Evidence**: `internal/db/migrate.go:65-93,164-194,260-287`；`migrations/` 实际集合（000001–000010）；联合合同 J1/J6（`docs/workflow-010-011-parallel.md:105-110,129-132`）；011 spec 前置核验（`spec.md:20`）。

---

## R12 — 测试资源隔离与验收分层：workdir 本地库/端口；011 独立验收 ≠ 联合验收

**Decision**:

- **隔离**：011 的集成测试使用工作区本地数据库名与非默认端口（009 R10 同构：`compose.009.yaml` 先例 + 每 worktree 独立 DB 名/端口），共享的 `compose.yaml` PostgreSQL/Anvil 由主编排协调（workflow R4/P4）；测试不得并行写同一默认库。
- **011 独立验收**：真实 HTTP（`serve` 准入路由）+ 真实 PostgreSQL + 真实迁移；010/009/008 边界用**契约形替身**（只保证形状与分类，不号称真实接线）；011 无链上代码路径，Anvil 不属 011 独立范围（结构断言：`internal/execution` 不 import RPC/dial 包）。
- **联合验收（C10，只定义需求）**：真实 HTTP/PG/Anvil + 真实 010（经 009/008），覆盖 FR-14 场景清单；010→011 合并顺序保留、必要上游同步先于联合验收；**替身或旧机制测试 MUST NOT 被呈现为联合验证**；010 独立验收通过 ≠ 联合通过；010 自身正确性义务不整体推迟给 011。

**Rationale**: 章程 X/XI 要求真实本地环境与「mock 不能替代集成」；联合合同 C10 与 011 FR-14 明确联合验收的真实环境要求与「独立 ≠ 联合」。把 011 独立范围限定为「有真实 011 代码路径的部分」并在链上交叉处显式标为联合，避免替身冒充。

**Alternatives considered**:
- *默认库上跑*：并行 worktree 互相污染；009 R10 已确立隔离方案。
- *用 mock 010 跑完整执行链并称为联合*：FR-14 与 C10 明文禁止。
- *等 010 合并后再开始 011 的任何验证*：与 P1/P2 并行准入矛盾；契约形替身可支撑 011 独立范围。

**Evidence**: `compose.yaml`、`compose.009.yaml`；`specs/009-signer-service/quickstart.md:9-38`（隔离方案与断言先例）；联合合同 C6/C10/J6、P2/P3；011 FR-14。

---

## R13 — 可观察与保密：结构化字段、既有指标注册表、无密钥/凭据/原始签名字节

**Decision**: 011 的每次准入、领取、续租失败、接管、停滞标记、步骤发出/收敛/未知、状态转移、修订应用、投影标脏都发结构化日志与指标；字段含 `request_id, intent_id, caller_id, sender, lease_version, owner_id, step_id, action, attempt_id, tx_hash, recovery_version, authorization_id, scope_version, revision_version, outcome_class`（FR-13）；金额/费用一律整数（`NUMERIC(78,0)`/BIGINT/`big.Int`），零浮点（章程 I/XII）。日志与指标不得含凭据、密钥、原始签名字节或交易原始字节；复用既有 `internal/logx`、`internal/metrics`、`internal/health`。

**Rationale**: 章程 XII 要求关键操作从引入起可观察；FR-13 给出字段清单；009 的 secrecy 断言（`internal/signer/secrecy_test.go`）是同一仓库的固定手段（日志/错误/状态路径扫密钥与签名字节）。

**Alternatives considered**: 新指标库/新日志框架：章程 XIII 拒绝；既有注册表足够。

**Evidence**: `internal/signer/secrecy_test.go`、`internal/signer/status.go`（脱敏先例）；`internal/config/config.go:60-77`（既有 knob 命名先例）；011 FR-13。

---

## R14 — 操作员受控路径与阈值提案：`withdrawal-exec` 子命令；所有数值为提案待裁决

**Decision**:

- **操作员路径**（`withdrawal-exec`，形态沿 `withdrawal-authz`/`nonce-admin`，不新增角色）：
  - `permission-set/revoke`（caller_id, can_execute）→ 写 `execution_caller_permission` + `execution_ops_audit`（`operation_id` 唯一，23505 → 回滚 → 按 operation_id 回读 → 同输入报已记录结果，异输入拒绝；008 R7 协议）。
  - `claim-revoke`（intent_id, expected_lease_version, operation_id, operator, reason, evidence）→ 精确版本条件更新（版本失配即拒绝，不追改），写 `revoked` 事件 + 审计；**证据缺失拒绝**（沿 008 hold release「解除证据不足拒绝解除」）。
  - `projection-refresh`（request_id, operation_id, operator）→ 读权威、版本守卫应用（R9）。
  - 只读 `claim-show` / `step-list` / `event-list`（无写、无审计）。
- **阈值（2026-09-17 批准：`lease_ttl` 30s / `heartbeat` 10s ±10% / `stall_window` 300s 为初始配置；backoff/cadence 为技术参数；其余未批准数值仍为提案，不得被当作业务承诺）**：

| 参数 | 提案值 | 依据 | 状态 |
|---|---|---|---|
| `lease_ttl` | 30s | indexer 租约 15s TTL / 5s 心跳的 3:1 比例（`internal/indexer/lease.go:27-28`）；011 单步含一次跨包 010 调用（其内部语句受 5s statement guard 约束，`internal/signer/plan` 同款守卫 `renewLockGuard`）加签名与 RPC 往返 | 2026-09-17 批准为初始配置（正常调度/DB 响应下留一次错失心跳余量，不承诺任意延迟下容错） |
| `heartbeat` | 10s（TTL/3，±10% 抖动） | 同 indexer 比例与抖动（`:36,233-239`） | 随上批准（派生比例） |
| `stall_window` | 300s（独立取值，非 TTL 联动） | 章程 IX 有界调用；健康 worker 每退避周期至少落一条进度或权威观察；300s ≥ 10× 最长有界单步 | 2026-09-17 批准为初始阈值（心跳/续租/退避/重领/无新事实轮询不刷新进展；诊断与进展记录分开） |
| `claim_backoff` | base 1s、cap 30s、±25% 抖动 | PG 点语句毫秒级；claim 行按 intent 分布、争抢低；抖动沿 indexer `jitter`（`internal/indexer/scanner.go:1012-1014`） | 技术参数，plan 决定 |
| `scan_interval` | 1s | 单机 PG 点查询量级；仅为 worker 自身节奏，不是业务时限 | 技术参数，plan 决定 |
| `step_retry` | 每轮 ≤3 次、仅对可重试/不可用类 | 章程 IX「重试必须有界」；未知/拒绝不重试（先对账） | 技术参数，plan 决定 |
| 投影刷新节奏 | 每轮消费 + 30s 全量追赶 + 失败即标可能过期 | C11：显示时限未指定；机制存在，**不写秒级 SLA** | 技术参数，plan 决定 |

- 批准附带条件（2026-09-17）：租约有效期以 DB 权威时间和成功提交的续租事实为准；心跳已发起/进程存活/续租结果未知均不延长资格；过期资格 MUST NOT 由迟到续租复活，须按新版本重领；plan 覆盖迟到续租、提交未知与接管竞争。批准值不定义停滞外其他语义；后续参数变更须明确校验及对在途租约生效规则，配置修改/重启 alone 不得延长既有资格。本次批准不代表其余缺口闭合或自动进入 tasks。其余未批准数值仍为提案：实现为配置默认值（`TXHARBOR_WORKER_*`），启动时 fail-closed 校验（正数、heartbeat<TTL、stall_window>TTL）。

**Rationale**: 规范与 M3 明确要求「判定依据、竞争保护、防误判机制留 plan；业务超时阈值须提有依据的建议供确认，不自行设定」；把机制与数值分离，机制可以设计，数值只提案。

**Alternatives considered**: 无界重试（章程 IX 禁止）；应用时钟阈值（不可复现，DB 时钟是仓库统一口径）；把提案值写进验收标准（等于自批准）。

**Evidence**: `internal/indexer/lease.go:27-28,32,36,41,233-239`；`internal/nonce/admin.go`（操作员路径与 operation_id 去重先例，009 plan D 系列引用）；011 spec M3 与 FR-13/FR-15。

---

## Open / deferred research items（carried into plan.md）

- **C11 显示延迟业务时限**：保持未指定；若用户要求，按 R9 机制提出有依据建议。MUST NOT 在 plan/任务/验收中写成已批准时限。
- **C12 迁移编号与跨 lane FK 顺序缺口**（R11）：合并期核验；缺口报告供裁决。
- **010 侧行锁序义务**（R5）：若 010 plan 不能提供 claim 行的行共享读序，按 FR-15 报告缺口，不自行加宽限。
- **阈值裁决**（R14 表）：全部待用户确认。
- **联合验收执行**（A-13）：保持 OPEN，本步骤只定义需求。
- **生产 provider 选型**（T000-P）：保持 OPEN。
- **010 具体接口签名**（R8）：消费者侧最小形状已记录；具体类型由 010 plan 落地，011 在接线时适配（009 D3 先例）。
