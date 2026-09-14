# Research: 005-confirmation-tracking (Phase 0)

**Branch**: `005-confirmation-tracking` | **Date**: 2026-09-14
**Spec**: `specs/005-confirmation-tracking/spec.md`（Q1/Q2 已闭合，无 NEEDS CLARIFICATION 剩余）

本文件记录 plan 全部技术选择的 Decision / Rationale / Alternatives。所有复用位置均指仓库当前 `005-confirmation-tracking`
分支（基于 `origin/main` `fd45e8b`）的真实文件与行号特征；缺口与新增能力逐项列出。

## R1 — 确认数安全计算（uint64 全程无符号；等价比较消溢出）

- **Decision**：阈值 N、链头高度 tip、充值高度 h 全程 Go `uint64`；达标判定使用与规格公式数学等价、
  但无溢出点的形式：`tip >= h && tip - h >= N - 1`（N ≥ 1 已由配置校验保证，故 `N - 1` 安全；
  前件成立故减法安全）。精确确认数（展示/审计用）仍按规格公式 `tip - h + 1` 计算，
  末端保留饱和 guard（`d == MaxUint64` 时饱和）仅作纵深防御。
- **表示对齐（Go ↔ DB，无业务上限）**：N 经 `ParseUint` 解析后必须能存入 `BIGINT`
  （即 `N ≤ MaxInt64`），超出按"超出系统支持整数范围"拒绝——这是存储类型决定的系统范围，
  不是业务上限（Q1/Q2 禁止的业务上限仍不存在）。tip/h 来源列（`chain_blocks.number`、
  `deposit_observations.block_number`）均为 `BIGINT CHECK (>= 0)`，故可达域为 `[0, MaxInt64]`；
  域内 `tip - h + 1 ≤ 2^63`，uint64 与 BIGINT 均精确，无截断。饱和输入（tip == MaxUint64）
  在可达域外不可达（约束证据：上述两列的 `BIGINT` 类型 + `CHECK (>= 0)` + 行只源自 RPC 高度；
  见 `migrations/000002_chain_indexer.sql`、`000004_deposit_detection.sql`）。
- **等价 vs 精确的分工**：门禁比较只用等价式（任意输入无溢出，含假设性 MaxUint64）；
  审计列存精确值（可达域内恒精确）；饱和值永不作为精确确认数展示或审计——若 guard 被触发，
  按内部错误拒绝提交（不可达断言，单元测覆盖存在性，集成不模拟）。
- **Rationale**：`tip - h` 在 tip ≥ h 时永不下溢；等价式彻底消除 `+1` 溢出点，比"饱和后比较"更干净。
  N=1 且 tip == h 时 `tip - h(0) >= 0` 成立，符合规格。无符号全程避免 int64/uint64 互转的符号陷阱；
  与既有 `DepositConfig.StartBlock uint64` / `next_block uint64` 口径一致
  （`internal/indexer/depositscanner.go:36-57`）。候选上界沿用 `block_number <= maxEligible` 索引查询
  （`maxEligible = tip + 1 - N`，`tip + 1 < N` 即无候选；tip == MaxUint64 时饱和，同属不可达防御）。
- **Alternatives considered**：
  - int64 高度：与现有 uint64 进度口径冲突，且负值无意义，拒绝。
  - `big.Int`：金额需要，高度不需要；可达域 `[0, MaxInt64]` 内 uint64 精确且零分配，拒绝。
  - SQL 内联 `tip - block_number + 1`：pg bigint 是 int64，语义与 Go 侧两套，易分叉；查询只做 `<=` 比较，拒绝。
  - 饱和值参与比较/审计：把防御值当精确值使用，违反审计精确性，拒绝（本决策的反例）。

## R2 — 复用地图（真实位置；缺口单列）

| 能力 | 复用位置 | 本阶段新增 |
|------|----------|------------|
| 必填配置解析/拒绝 | `internal/config/config.go:414-424`（`require`/`invalid`）、`:475-484`（`parseChainID` 正整数形态） | `TXHARBOR_CONFIRMATION_DEPTH` 读取 + `parseConfirmationDepth`（`ParseUint` + `==0` 拒绝；超 uint64 范围由 `ParseUint` 报错覆盖"超出系统支持整数范围"） |
| 短事务形状 | `internal/indexer/scanner.go:985-1020`（`writeGuard`/`ensureLeaseSQL`/`lockCoordSQL`/`leaseVerdictSQL`/`pauseExistsSQL`） | 确认提交/暂停判定复用全部常量，不复制 SQL 文本 |
| 协调锁与失权 | `internal/indexer/lease.go:87-233`（`Lease`、`Acquire`/`Renew`/`Heartbeat`、token 单调） | 复用；确认/切换事务同样先锁协调行（锁顺序唯一，无死锁环） |
| 提交裁决形态 | `internal/indexer/depositcommit.go:186-265`（`commitDepositUnit` 步骤 1–4：BEGIN→guard→ensure→FOR UPDATE→独立语句重裁决） | `confirmcommit.go` 同构新裁决（暂停三行皆无 + lease 归属 + 策略版本守卫 + 链头重读 + 候选重读 + 条件写 + 行数核对） |
| 授权请求幂等 | `internal/indexer/depositauth.go`（`request_id` UNIQUE、同参返原/异参拒绝、未记录失败不绑定）+ `migrations/000004_deposit_detection.sql:25-54`（history 表形态） | `confirmauth.go` 同构缩小版（仅旧 seq 预期 + 新阈值 + 审计行；无 replay/暂停处置列——缺口见 R3） |
| 调度循环形态 | `internal/app/serve.go:217-277`（scanner 构造→observer→`RunTrio` 第三循环；授权保持 loop 外特权操作） | 第四确认循环 + `confirmationObserver`（`coordinator.go:23-57` 的 `ServeFunc` 形状已有四路扩展点：`RunTrio` 后新增 `RunQuatro` 或泛化——缺口，见 R4） |
| 重试/轮询节流 | `DepositConfig.PollInterval/RetryInitial/RetryMax`（`depositscanner.go:44-49`，复用 `TXHARBOR_INDEX_*`） | 确认循环复用同一三旋钮，不新增 env |
| 可观测注册 | `internal/metrics/metrics.go:69-151`（`New` 内 CounterVec/GaugeVec 模式）+ `contracts/observability.md`（004 组） | `confirmation_*` 组（见 contracts）；002/003/004 语义冻结 |

## R3 — 策略权威来源：单表版本链（不复用 deposit_config_history）

- **Decision**：新建单表 `confirmation_policy_history(chain_id, policy_seq)` append-only（见 data-model）；
  有效策略 = 同链最大 `policy_seq` 行。无独立"当前行"表。首次由首个确认提交事务内原子 bootstrap（`policy_seq=1`，
  并发第二写 PK 冲突→重读收敛/漂移拒绝，镜像 004 首单元语义）。授权切换 = 单行 INSERT（新 seq + 预期旧 seq 守卫 +
  `request_id` UNIQUE + 操作者/原因审计列），单语句原子，无部分生效可能。
- **Rationale**：阈值是与 004 资产/地址配置正交的版本化域；混入 `deposit_config_history` 会污染其
  `(assets, watches, replay_from)` 不变量与回放语义（004 FR-06/Q5/Q6），且 004 的暂停处置/回放列对阈值无意义。
  单表无"当前行 + 历史行"双写，不存在双侧不一致的损坏态（对比 004 Table 2 完整性前检的复杂性）。
  同参同阈值切换按 004 空授权规则拒绝（H′ 必须不同），故 seq 推进恒蕴含阈值变化，提交守卫可用"seq 相等"一次性覆盖。
- **Alternatives considered**：
  - 复用 `deposit_config_history` 加阈值列：污染 004 域，拒绝（上）。
  - 当前行 + 历史行双表（镜像 004 checkpoint/history）：双写一致性成本无收益（阈值切换是单值替换，无回放位置），拒绝。
  - 内存/env 即真相（无 durable 策略行）：重启/多 worker 无法裁决"有效版本"，违反 Q2 单有效版本，拒绝。

## R4 — 调度：无游标有序扫描（降阈值不跳过）

- **Decision**：确认循环无独立进度游标。每 tick 读取可信 tip → 计算 `maxEligible` → 按
  `(chain_id, block_number)` 有序批量取 `status='pending'` 候选（partial index，见 data-model）→ 逐个评估提交。
  降阈值后此前不合格的 Pending 自动落入候选窗；升阈值后不合格者自然排除；Confirmed 行永不回读。
- **Rationale**：资格是 (tip, N) 的纯函数，游标只会引入"降阈值后游标已越过"的跳过风险；有序 + 批量保证不饥饿、
  最终覆盖（SC-04）。读多写少：评估是只读查询 + 仅达标行进入写事务。
- **Alternatives considered**：
  - 独立确认游标（镜像 deposit next_block）：降阈值需回退游标→与"不跳过"证明负担大，且游标与 tip/N 三方一致性需新不变量，拒绝。
  - tip 推进事件触发 + 轮询混合：触发源（DB NOTIFY/内存事件）新增跨组件耦合；轮询复用 PollInterval 已满足"追赶后不遗漏"（SC-04），拒绝。
- **缺口**：`coordinator.go` 现有 `RunPair`/`RunTrio` 需扩展第四确认循环（新增 `RunQuatro` 或泛化为 streams 切片——
  `runStreams/serveStreams` 内部已是切片实现，见 `coordinator.go:57-109`，扩展面小）。

## R5 — 不新增确认暂停表（暂停域归属）

- **Decision**：005 不建 `confirmation_pause` 表。瞬态停止（tip 缺失/滞后/DB 失败/lease 失权）= 循环内等待 + 退避，
  无 durable 行（镜像 004 state=1/2/4 的无行等待）；结构性停止复用既有三行（`deposit_pause`/`indexer_pause`/`log_pause`），
  提交门禁要求三行皆无；单行不可信候选（引用块非 canonical）= 跳过该行（留 Pending）+ `skipped` 计数，不 halt 全循环。
- **Rationale**：004 Table 3 的首胜/累积合并/修订语义（Q8）是充值流专用的，005 写入会破坏其不变量；
  005 无游标故无需 durable 停止位即可保证"不越过"（无位可越）；读多写少使跳过重评估廉价（只读索引查询，无写放大）。
  006 未来可通过既有暂停行 halt 确认提交（门禁已含），交接无需新表。
  行级/循环级分类、无饿死论证见 data-model §候选分类；`skipped` 原因二分
  （`below_depth` 正常竞态 / `noncanonical` 防御告警）见 contracts。
- **Alternatives considered**：
  - 新建 `confirmation_pause(+audit)`：+2 表 + 跨组件语义，与"最小迁移"冲突；且 005 的停止条件均可由既有行 + 循环等待表达，拒绝。
  - 复用 `deposit_pause` 加 kind：破坏 004 累积合并与审计归属（detail 版本段按 deposit seq 打标），拒绝。

## R6 — 状态存放：拓宽 observations.status + 依据列（非 side-table）

- **Decision**：迁移拓宽 `status` CHECK 至 `('pending','confirmed')`，新增 `confirmed_at` + 依据五列
 （`confirm_tip_number/hash`、`confirm_threshold`、`confirmations`、`confirm_policy_seq`，后者 FK 到策略历史），
  CHECK 约束保证 pending 行依据全空、confirmed 行依据全非空且不可变（仅 `status='pending'` 行可写，见 data-model）。
  006 未来以自有迁移加入 `'orphaned'`（与 004 留给 005 的扩展位同构）。
  不可改写执行机制：全写语句恒带 `status = 'pending'` 谓词 + 改写尝试零行断言 + 完整性抽查 SQL，
  无触发器（触发器是 006 的障碍物，应用谓词对其零约束）；006 保留契约见 data-model §Table 1。
- **Rationale**：观察行已是来源身份的唯一载体（PK），转换是同行状态推进；side-table 需第二套身份 + 一致性证明，
  无收益。依据列即审计（单语句原子写，无审计表仍满足"转换与审计原子化"）。
- **Alternatives considered**：
  - 独立 `deposit_confirmations` 表：双身份一致性 + join 追溯，不及单行conditional UPDATE 简单，拒绝。
  - 依据存 JSONB：字段可查性（诊断 SQL、006 重验）不如显式列，拒绝。

## R7 — 旧配置 worker 舰队程序（切换后不永久停摆）

- **Decision**：切换授权成功后，持旧 env N 的 worker 在下一次提交守卫（policy_seq 失配→重读→阈值漂移）处以配置漂移错误
   loud 停止（非零退出，镜像 004 serve.go:212-235 的拒绝形态），绝不提交旧结果、不静默跟随新策略。
  部署程序在授权成功后以新 env N 重启 worker（标准发版流程）；新 worker 启动比较通过即恢复。
  "不永久停摆"由"旧 worker 大声停 + 新配置重启即恢复"保证，而非旧 worker 自适应（自适应=绕过授权，违反 Q2）。
  退出范围与重启步骤见 data-model §切换后恢复程序：漂移错误经既有扇出语义（`coordinator.go:106-174`）
  使整进程非零退出（非暂停行，故他循环不被"阻止"，只随进程退出而停，各自进度 durable 重启即续）；
  runbook 为授权成功→全舰队更新 env N→逐实例重启。
- **Rationale**：Q2 单有效版本 + 拒绝未授权漂移的直接推论；004 对 env 漂移同样拒绝而非跟随（`depositConfigMismatchError`，
  `depositcommit.go:69-71,206-210`）。
- **Alternatives considered**：worker 自动采纳库内新阈值：等价于无授权运行时变更，违反 Q2，拒绝。

## R8 — 可观测增量（冻结上游组）

- **Decision**：仅新增 `confirmation_*` 组（gauges： pending 估计/滞后/state/policy_seq；counters：
  confirmed_total{ok}、skipped_total{reason}、transition_total{ok|stale|rejected}、policy_transition_total{ok|rejected}），
  结构化日志字段与诊断 SQL 见 contracts；002/003/004 指标名与语义冻结；`/readyz` 不翻转（镜像 004 R8）；
  全部经 `logx.Redact`，金额十进制。
- **Rationale**：FR-11 只要求进度/积压/滞后/暂停可见；skipped 计数使"跳过未确认"可观测而不 halt；transition 计数使
  stale/rejected 提交可审计。

## R9 — 006 交接契约（005 落定、006 消费）

- **Decision**：005 落定三项可被 006 复用的契约：(1) 提交门禁包含三暂停行 → 006 以暂停行 halt 确认；
  (2) 锁顺序唯一（`indexer_lease` 行首锁）→ 006 恢复事务遵守同一顺序即无死锁；
  (3) 确认依据列（tip 身份、阈值、策略 seq、所得确认数、时间）→ 006 重验/Orphaned 判定/再确认的输入。
  005 不写 006 的恢复行，不定义 Orphaned 语义，不批准重上链恢复策略。
- **Rationale**：任务指令要求 005 形成"未来恢复与确认提交可遵守同一协议"的契约；上述三项是 005 侧的全部必要输出。
