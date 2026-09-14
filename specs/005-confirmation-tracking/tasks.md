# Tasks: 005-confirmation-tracking

**Input**: Design documents from `/specs/005-confirmation-tracking/` (spec.md + 澄清 Q1–Q2，plan.md, research.md R1–R9, data-model.md（含定向复核补齐），contracts/observability.md, quickstart.md D1–D5, review.md)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ 均已就绪（plan 定向复核提交 512bfa6）

**Tests**: 本规格以行为正确性为核心（章程 X/XI），集成测试为必选；单元测试覆盖纯逻辑（解析、等价式、分类）。
并发一致性必须用真实 PostgreSQL + 双 worker 断言，`-race` 仅为补充，不得替代。
已知偶发本地失败（原因未知，004 acceptance 遗留）不得豁免任何门禁：时序敏感测试按固定批次执行
（每批次恰好 5 次，记录全部结果；任一次失败则该批次不通过；重跑另记批次），证据齐全方可关闭；
未解释失败持续为未解决事项（后续成功不自动关闭），无新证据停跑并报告待诊断问题。

**门禁说明**：T000-L 关闭 → 可开始本地范围实现（Anvil + DB 播种）；
T000-P 关闭 → 方可谈生产接入就绪。本地验收通过不等于生产就绪。
生产就绪额外依赖上游 003 E1（provider 附录）；005 自身零 RPC，但链头语义受 002 约束。
T000-P 与偶发失败为历史遗留事项（见 review.md），与本阶段新增任务无关，不宣称已消除。

- [ ] T000-L 本地实现前置检查（Anvil + DB 播种范围；满足即可关闭）
  - 需求：plan 前置。依赖：无。
  - 内容：`Coordinator` 第四循环注册点形状确认（`coordinator.go` 现 `RunTrio` 三 serve，`runStreams/serveStreams` 已切片实现）；
    `serve.go` 接线位置确认（deposit 接线 `serve.go:217-277` 旁）；goose `000005` 在 `000004` 之后顺序与 `embed.go` 收录确认；
    pgx → `BIGINT` 的 uint64 非负映射写法确认；DB 播种清单完备（canonical 块 + Pending 观察行、前置暂停行、伪造哈希行、策略历史行）。
    这些结果仅证明本地行为，不证明生产行为。
  - 完成条件：上项逐项可勾选；关闭后 T001–T032 可在本地范围按依赖执行。
- [ ] T000-P 生产门禁（保持 open；约束生产接入与部署，不阻塞本地实现）
  - 需求：生产就绪。依赖：上游 003 T000-P（E1）关闭。
  - 完成条件：在此之前 005 不得标为生产可用；本地验收通过不得自动关闭本任务。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件、无依赖）
- **[Story]**: 归属用户故事（US1–US5，对应 spec.md 五个故事）
- 编号规则：T000 门禁；T001–T004 地基；T01x=US1、T014–T017=US2、T018–T020=US3、T021–T023=US4、
  T024–T028=US5、T029–T032 收尾。编号按阶段分组预留间隙，执行顺序以 Dependencies 为准。
- 每个任务含：需求、验收场景、依赖、完成条件、涉及文件

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 门禁状态显式记录（无新脚手架；沿用 001–004 工程底座）

T000-L / T000-P 见上（本阶段即二者建档）。**Checkpoint**: 门禁状态显式记录；本地工作可继续。

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 迁移、配置解析、安全数学、可观测注册——所有用户故事的前置

**⚠️ CRITICAL**: 本阶段完成前不得开始用户故事实现

- [ ] T001 [P] 新增迁移 `migrations/000005_confirmation_tracking.sql`（策略历史表 + 观察行加列 + CHECK 拓宽 + 索引）及迁移测试
  - 需求：FR-05/FR-08，data-model Table 1–2。验收场景：quickstart D5（升级部分）。依赖：T000-L（本地范围）。
  - 内容（约束逐字落实）：`confirmation_policy_history` 主键 `(chain_id, policy_seq)`、`threshold CHECK (> 0)`、
    `UNIQUE (chain_id, request_id)`、首版本 NULL 单行 partial unique、`prev_seq` 自引 FK；
    `deposit_observations` 加 `confirmed_at` + 依据五列（`confirm_tip_number/check(>=0)`、
    `confirm_tip_hash hex`、`confirm_threshold/check(>0)`、`confirmations NUMERIC/check(>=0 且 =floor(自身))`、
    `confirm_policy_seq` FK→历史表），`status` CHECK 拓宽为 `IN ('pending','confirmed')`，
    一致性 CHECK `(status='pending')=(confirmed_at IS NULL)` + confirmed 行六列全非空；
    partial 索引 `deposit_observations_pending_height_idx ... WHERE status='pending'`；
    升级 DO 断言零非 pending 行；Down 按依赖逆序。
  - 完成条件：空库迁移、从 004 库升级、重复迁移三条件绿；上列约束逐项有断言；002/003/004 表定义零改动（diff 为证）。
  - 涉及文件：`migrations/000005_confirmation_tracking.sql`（新建），迁移测试落 `internal/db/` 既有迁移测试旁。
- [ ] T002 [P] 配置 `TXHARBOR_CONFIRMATION_DEPTH` 读取与非法拒绝（`internal/config/config.go`、`internal/config/config_test.go`、`.env.example`）
  - 需求：FR-03（Q1）。验收场景：US2-非法阈值、quickstart D1（非法矩阵）。依赖：T000-L。
  - 内容：`require()` 缺失拒绝 + `parseConfirmationDepth`（`ParseUint` + `== 0` 拒绝 + `> MaxInt64` 按超系统范围拒绝，
    复用 `invalid()` 形态）；无默认值；`Summary()` 脱敏沿用；`.env.example` 增 Required 行（仿 `TXHARBOR_DEPOSIT_START_HEIGHT` 注释形态）。
  - 完成条件：缺失/空串/非整数/`0`/负数/超 uint64/超 int64 七类输入矩阵全拒绝且错误明确；合法值全集通过；`gofmt` 干净。
- [ ] T003 [P] 安全数学与确认配置基座（`internal/indexer/confirm.go` 新建、`internal/indexer/confirm_test.go` 新建）
  - 需求：FR-01/FR-03，research R1。验收场景：US2-1/2/3、quickstart D1（单元部分）。依赖：T000-L。
  - 内容：`ConfirmationConfig{ChainID int64, ThresholdN uint64, PollInterval/RetryInitial/RetryMax}` + 构造期校验；
    等价比较 `tip >= h && tip - h >= N - 1` 与规格公式逐值对照测试（含 N=1、N=MaxInt64、`tip < h` 两边皆假）；
    精确值饱和 guard 存在性测试（不可达防御）；`maxEligible` 上界函数。
  - 完成条件：对照矩阵全绿；`go test -race` 通过；无 int64/uint64 互转（审查门）。
- [ ] T004 [P] 可观测 `confirmation_*` 组注册（`internal/metrics/metrics.go`、`internal/metrics/metrics_test.go`）
  - 需求：FR-11，contracts/observability.md。验收场景：US5-3（追溯计数侧）。依赖：T000-L。
  - 内容：`New` 内新增 gauges（pending/lag/state/policy_seq）+ counters（confirmed_total/skipped_total{below_depth}/
    transition_total{ok|stale|rejected}/policy_transition_total{ok|rejected}）；002/003/004 组零改名（diff 为证）。
  - 完成条件：注册断言 + 标签值断言绿；`/readyz` 语义不动。

**Checkpoint**: Foundation ready - user story implementation can now begin（故事间按依赖顺序，文件不冲突者可并行）

---

## Phase 3: User Story 1 - 达阈值推进为 Confirmed (Priority: P1) 🎯 MVP

**Goal**: Pending 充值达阈值后恰好一次转为 Confirmed，依据齐备（US1-1/1-2，FR-01/02/05，SC-01）

**Independent Test**: Anvil 推进 canonical tip 越过阈值，每条达标 Pending 恰好一次转换，依据列可追溯（quickstart D1 主路径）

- [ ] T010 [US1] 确认提交事务与策略 bootstrap（`internal/indexer/confirmcommit.go` 新建）
  - 需求：FR-05/FR-07/FR-08，data-model §提交协议/§首确认协议。验收场景：US1-1/1-2、US4-1（收敛侧）。依赖：T001、T002、T003。
  - 内容：BEGIN→`writeGuard`→ensure lease→`FOR UPDATE`→独立重读（三暂停皆无 + lease 归属 + 策略(S,N) + tip(T,TH) +
    候选 pending + 哈希一致 + 等价式重算）→条件 UPDATE（`status='pending'` 谓词）+ 行数核对→COMMIT；
    首确认同事务 bootstrap `(chain_id,1)`（PK 冲突收敛）；提交未知按 PK 重读定性。
  - 完成条件：步骤与 data-model 逐项对应；失配路径全回滚零写入（单测或轻量 pg 断言）。
- [ ] T011 [US1] 候选扫描与确认循环（`internal/indexer/confirmscan.go` 新建）
  - 需求：FR-02/FR-06，research R4。验收场景：US3-3（等待侧）。依赖：T010（提交函数签名）。
  - 内容：`NewConfirmationScanner`（启动比较：阈值漂移拒绝，镜像 004）；每 tick 读 tip→`maxEligible`→partial 索引有序批量；
    tip 缺失/不可信→等待；退避复用 INDEX 三旋钮；`ConfirmationState()`/`ConfirmationProgress()` 原子快照（镜像 deposit 观测形态）。
    空状态断言（F3）：零候选时无策略行、无业务写入、可观测 state 为等待/运行空闲（非停止），首个达标转换时 bootstrap。
  - 完成条件：无 tip 零提交；正常滞后等待不记异常；快照与循环一致；空状态零行零写断言。
- [ ] T012 [US1] 第四循环接线（`internal/indexer/coordinator.go`、`internal/app/serve.go`）
  - 需求：FR-02，research R2/R4。验收场景：US1 独立测试前置。依赖：T011。
  - 内容：`RunTrio` 扩展第四确认循环（`runStreams` 切片复用，落点 `coordinator.go:52-57` 旁）；
    serve 构造确认 scanner→observer→循环注册（落点 `serve.go:217-277` 旁，header/log/deposit 行为不变）；
    授权切换保持 loop 外（注释明示）。
  - 完成条件：四循环并存启动；任一循环停止错误扇出行为不变（既有 coordinator 测试绿）。
- [ ] T013 [US1] 集成：达阈值恰好一次转换（`internal/indexer/confirmation_integration_test.go` 新建）
  - 需求：FR-01/05，SC-01。验收场景：US1-1/1-2、quickstart D1（主路径）。依赖：T012。
  - 内容：Anvil 预置 Pending→推进 tip 越过阈值→断言每条恰好一次 Confirmed + 六依据列精确 + `confirmed_total{ok}` +1；
    含旧 `version_seq`（004 收缩保留版本）观察：照常按 canonical + 阈值确认，不重审 004 版本语义，
    且仍须通过暂停/链视图/策略全部门禁（版本无关≠绕过门禁）。
  - 完成条件：5 次固定批次全绿（偶发失败规则见文件头 Tests）；失败留痕不豁免；旧版本行确认断言在列。

**Checkpoint**: MVP（US1）独立可测：无异常注入下的确认主路径端到端成立

---

## Phase 4: User Story 2 - 边界精确与非法配置 (Priority: P1)

**Goal**: N-1/N/N+1、N=1、整数边界精确；非法阈值拒绝（US2-1/2/3/4，FR-01/03，SC-01/02）

**Independent Test**: 边界矩阵 + 非法矩阵全绿；MaxInt64+1 表示决议落地（quickstart D1 全量）

- [ ] T014 [P] [US2] 等价式边界单元测试（`internal/indexer/confirm_test.go` 增补）
  - 需求：FR-01，research R1（复核后）。验收场景：US2-1/2/3。依赖：T003（不同文件？同文件增补→仅依赖 T003；与 T015/T016 文件不同可并行）。
  - 内容：N-1/N/N+1 判定矩阵、N=1 且 tip==h、N=MaxInt64、`tip<h` 全假。
  - 完成条件：矩阵全绿（含 `-race`）。
- [ ] T015 [P] [US2] 非法配置启动拒绝矩阵（`internal/config/config_test.go` 增补 + `internal/app/serve_config_test.go` 增补）
  - 需求：FR-03（Q1）。验收场景：US2-非法阈值边角、US5-5（漂移侧）。依赖：T002。
  - 内容：七类非法输入启动失败断言（错误明确、无默认、无静默修正）；重启异 N 漂移拒绝断言。
  - 完成条件：矩阵全绿；进程非零退出有断言。
- [ ] T016 [US2] Anvil 边界矩阵（`internal/indexer/confirmation_integration_test.go` 增补）
  - 需求：FR-01，SC-01/02。验收场景：US2-1/2/3、quickstart D1。依赖：T013（同文件顺序追加）。
  - 内容：确认数 9/10/11（N=10）分别保持/转换/转换；N=1 tip==h 转换；US2-4 切换重判移交 T025（本任务只断言切换前行为）。
  - 完成条件：5 次固定批次全绿。
- [ ] T017 [US2] 2^63 精确审计路径验证（OI-1 已决议：NUMERIC；`migrations/000005_confirmation_tracking.sql` 落定 + 审计断言）
  - 需求：FR-01/05，data-model Table 1/§确认数计算。验收场景：quickstart D1（整数边界）。依赖：T001（同文件顺序修订）。
  - 内容（决议，无二选一）：`confirmations` 列为 `NUMERIC` 精确整数（OI-1 关闭）；
    Go↔SQL 经十进制字符串（uint64→decimal→NUMERIC，镜像 `depositNumericAmount`；禁 int64/float64 中转）；
    tip=MaxInt64、h=0 时 2^63 精确写入/读取/审计（追溯 SQL 返回精确值）；饱和 guard 触发即拒提交。
  - 完成条件：极值审计端到端精确；T001 约束断言含整数性与非负；OI-1 关闭记录。

**Checkpoint**: 边界与配置语义锁定；OI-1 有明确决议记录

---

## Phase 5: User Story 3 - 异常分类与追赶 (Priority: P1)

**Goal**: 行级等待 vs 循环级停止正确分类；追赶不遗漏；异常按规格停止（US3-1/2/3/4，FR-02/04/06，SC-03/04）

**Independent Test**: 异常矩阵 + 滞留 starvation 集成全绿（quickstart D5）

- [ ] T018 [US3] 等待与停止映射（`internal/indexer/confirmscan.go` 增补）
  - 需求：FR-04/06，data-model §候选分类（规格原文：FR-06/US3-2/Edge-170）。验收场景：US3-1/2/3。依赖：T011（同文件顺序增补）。
  - 内容：`below_depth` 行级等待（留 Pending，继续同批，计数；唯一非停止分支）；
    引用缺失/哈希不一致/链头缺失/tip 不可信/暂停/漂移→循环停止（state=3，链头缺失 state=1；零提交，不建暂停行）。
  - 完成条件：分类与 data-model 表述逐项对应；异常行触发整批终止（已提交属合法先后）。
- [ ] T019 [US3] 集成：异常停止、追赶与小批量覆盖（`internal/indexer/confirmation_integration_test.go` 增补）
  - 需求：FR-02/06，SC-03/04。验收场景：US3-1/2/3/4、quickstart D5。依赖：T018（同文件顺序追加）。
  - 内容：SQL 直插伪造引用行（`deposit_observations` 无指向 `chain_blocks` 的 FK，直插可行；须附 history 行满足版本 FK；
    scanner 路径 pre-006 产不出此类行）→ 循环停止、本 tick 及后续零提交（state=3，`reference_unverifiable` 日志）；
    暂停行存在零提交；tip 缺失停止；滞后消除后符合 Pending 全处理；
    良性小批量：LIMIT 小于合格集 → 多 tick 后排全覆盖（单调性；异常阻塞不在此列，按规格保持停止）。
  - 完成条件：5 次固定批次全绿。
- [ ] T020 [US3] 等待/停止纯逻辑单元测试（`internal/indexer/confirm_test.go` 增补）
  - 需求：FR-06。验收场景：US3-1/3。依赖：T014（同文件顺序增补，不可并行）。
  - 内容：分类谓词矩阵（`below_depth` 行级等待 vs 引用缺失/哈希不一致/链头缺失/暂停/漂移停止条件）。
  - 完成条件：全绿（含 `-race`）。

**Checkpoint**: US3 独立可测：异常不停错、不漏、 converged

---

## Phase 6: User Story 4 - 幂等、并发与崩溃 (Priority: P1)

**Goal**: 重复/并发/崩溃/重启收敛，首次事实不可改写（US4-1/2/3，FR-05，SC-05/06/08）

**Independent Test**: 双 worker 竞态 + kill 恢复集成全绿（quickstart D2）

- [ ] T021 [US4] 双 worker 并发与重复检查收敛（`internal/indexer/confirmation_integration_test.go` 增补）
  - 需求：FR-05/07，SC-05/06，data-model §并发时序情形 2。验收场景：US4-1/2。依赖：T013（同文件顺序追加）。
  - 内容：双真实连接并发确认同一 Pending（恰好一方成功，败者 0 行收敛）；重复检查 ≥2 次转换恒 1；
    `-race` 并行跑。
  - 完成条件：5 次固定批次全绿；确认时间与依据为胜者值断言。
- [ ] T022 [US4] 提交临界崩溃与重启恢复（`internal/indexer/confirmation_integration_test.go` 增补）
  - 需求：FR-05，SC-08。验收场景：US4-3、quickstart D2。依赖：T021（同文件顺序追加）。
  - 内容：提交前 kill、响应丢弃后按 PK 重读定性；重启从 durable 状态继续；无遗漏/重复/部分行（完整性抽查 SQL=0）。
  - 完成条件：5 次固定批次全绿。
- [ ] T023 [US4] 不可改写回归（全部写路径覆盖；`internal/indexer/confirmation_integration_test.go` 增补 + 代码审查门）
  - 需求：FR-05/I2，data-model §Table 1 执行机制。验收场景：US4-1（不重写侧）、SC-08。依赖：T022（同文件顺序追加）。
  - 内容：改写 confirmed 行尝试影响 0 行；全仓库 005 写路径仅提交事务一条 UPDATE（含 `status='pending'` 谓词，
    grep 门禁记入完成证据）；完整性抽查 SQL 断言。
  - 完成条件：回归绿；写路径清单与代码一致（审查签字记入提交信息或 review 增补）。

- [ ] T033 [US4] 异配置首次竞争合法性（`internal/indexer/confirmation_race_integration_test.go` 增补）
  - 需求：FR-03/FR-08（Q2 单有效策略），data-model §首确认协议（分歧双首启）。验收场景：US4 并发类扩展、US5-5（漂移侧）。
    依赖：T026（同文件顺序追加，不可并行）。
  - 内容：双实例异 env N、零策略行同时启动→断言恰好一行 bootstrap（胜者 N 落定）；败者明确漂移错误停止、
    败者零确认转换；胜者后续转换绑定现行有效策略（策略 seq 一致断言）；胜负不决正确性（运维以授权切换纠正路径见 T029）。
  - 完成条件：5 次固定批次全绿；败者错误类型与零写双断言。

**Checkpoint**: US4 独立可测：任何重复/并发/崩溃下首次事实不变

---

## Phase 7: User Story 5 - 提交门禁、切换与审计 (Priority: P2)

**Goal**: 暂停/版本竞争零旧提交；受控切换全周期；审计可追溯（US5-1/2/3/4/5，FR-03/07/08/09/11，SC-03/07/09/10）

**Independent Test**: 竞争注入 + 切换全周期集成全绿（quickstart D3/D4）

- [ ] T024 [US5] 授权切换事务（`internal/indexer/confirmauth.go` 新建）
  - 需求：FR-03（Q2），data-model §授权切换协议。验收场景：US5-4/5（入口侧）。依赖：T001、T010（策略读/守卫形态）。
  - 内容：DB 操作员直连 SQL 入口（无端点/服务/角色新增；载体沿用 004 T024 决议"受控 SQL 脚本"，
    见 `specs/004-deposit-detection/tasks.md:91`，本任务不重做选型）；锁内重验（max seq==expected_old_seq、新值合法且不同、有源版本）；
    单行 INSERT；request_id 同参返原/异参拒绝/未绑定重试；丢失响应重读定性；切换零暂停行写入（断言）。
  - 完成条件：守卫逐项有测试（单测或轻量 pg）；旁路不存在（入口唯一性审查）。
- [ ] T025 [US5] 切换全周期集成 D4（`internal/indexer/confirmation_auth_integration_test.go` 新建）
  - 需求：FR-03，SC-10。验收场景：US2-4、US5-4、quickstart D4。依赖：T024。
  - 内容：降低重判全纳入（含切换点前 Pending，降低不批量直确；切换后以**小批量 LIMIT 多 tick 复核**既有 Pending 全纳入，
    无游标跳过）；提高不改写 Confirmed 且可追溯当时阈值；
    未授权漂移拒绝零破坏；切换失败无新行；未知结果 request_id 定性。
  - 完成条件：5 次固定批次全绿；小批量复核覆盖有断言。
- [ ] T026 [P] [US5] 竞争注入：暂停/链视图/切换 mid-flight（`internal/indexer/confirmation_race_integration_test.go` 新建）
  - 需求：FR-06/07/08，SC-03/07。验收场景：US5-1、quickstart D3。依赖：T010、T024（与 T025 文件不同可并行）。
  - 内容：经**独立第二数据库连接**、以锁等待为同步点（禁固定 sleep 定时）注入三竞争，
    覆盖两种合法线性化顺序：(a) 竞争方先提交（暂停行/新 tip/新策略行落地）→确认方后获锁→重读失配回滚；
    (b) 确认方先持锁→竞争方阻塞→确认方按旧快照合法提交→竞争方继续。三竞争下旧结果提交成功率均为 0
    （`transition_total{stale}` +1，零状态变化）；时序覆盖 data-model 三情形。
  - 完成条件：5 次固定批次全绿；两种顺序各有断言；sleep 定时零使用（审查门）。
- [ ] T027 [US5] 漂移退出与重启恢复（`internal/indexer/confirmation_integration_test.go` 增补）
  - 需求：FR-03/08（R7），SC-07。验收场景：US5-5、quickstart D4（恢复侧）。依赖：T024（同文件接 T023 顺序追加）。
  - 内容：旧 N 进程切换后漂移非零退出；断言切换未创建/删除/清除任何暂停行；新 N 重启恢复确认。
  - 完成条件：退出码与暂停行不变双断言绿。
- [ ] T028 [US5] 审计追溯断言（`internal/indexer/confirmation_auth_integration_test.go` 增补）
  - 需求：FR-09/11，SC-09。验收场景：US5-2/3。依赖：T025（同文件顺序追加）。
  - 内容：追溯 SQL 定位充值/区块/依据/时间；错误输出零凭据 + 零无限制转储（日志采样断言）；
    `policy_transition_total` 计数对照。
  - 完成条件：全绿。

**Checkpoint**: US5 独立可测：门禁、切换、审计闭环

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: 运行手册、CI 接入、全量验证、复核记录

- [ ] T029 [P] 切换运维手册（`specs/005-confirmation-tracking/quickstart.md` 增补 §切换 runbook）
  - 需求：FR-03（Q2 runbook）。验收场景：US5-5。依赖：T025（写实）。
  - 内容：授权→全舰队更新 env→逐实例重启步骤；旧进程退出预期；回滚（再次授权切换）路径；与 004 授权手册的职责边界。
  - 完成条件：步骤与 T025 验证行为一致；无新业务语义。
- [ ] T030 [P] CI 接入核验（`.github/workflows/ci.yml`、`Makefile`；收窄：不新建 job）
  - 需求：章程 X/XI。验收场景：全部分层。依赖：无（读现有写法后核验；与各任务文件不同可并行，但生效需测试存在）。
  - 内容：核验现有 CI 确实包含新增测试：`make test-integration`（testcontainers）自动覆盖全部 `-tags integration` 新增用例，
    无需新 job；仅在缺口处最小增补；`gofmt`/`vet` 门禁沿用；单元（含 `-race`）常驻门禁不变。
  - 完成条件：核验结论有记录（需新建 job 则说明理由，否则零新增）；本地 `make test test-race` 绿（含新增单元）。
- [ ] T031 全量 quickstart 验证（D1–D5 + 偶发批次纪律）
  - 需求：SC-01–SC-10。验收场景：全部 18 项。依赖：T013–T028（全部故事完成）。
  - 内容：按 quickstart D1–D5 执行；时序敏感项 5 次固定批次；任一失败留痕（不豁免、不自动关闭）。
  - 完成条件：全绿或失败清单（含证据）移交；T000-P 保持 open 声明。
- [ ] T032 复核记录与 tasks 自检证据（`specs/005-confirmation-tracking/review.md` 增补）
  - 需求：本 tasks 步骤自检。依赖：T031。
  - 内容：任务格式/依赖环/覆盖/文件冲突检查结论；OI-1 与残留事项状态；是否具备 analyze 条件（本步仅声明，不执行 analyze）。
  - 完成条件：自检项逐项有结论；无依据新增任务为零（或逐项说明来源）。
    自检已执行项（生成时）：T020 去 [P]（与 T014 同文件冲突）；编号分组间隙已文档化；18 场景/12 FR/10 SC 全映射；
    同文件链 7 条无环；T030/T032 来源已声明。

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: T000-L/T000-P 建档，无依赖。
- **Foundational (Phase 2)**: T001–T004 依赖 T000-L（本地范围）；BLOCKS 全部用户故事。
- **User Stories**: US1 → US2/US3/US4（US2-4 切换重判由 US5 交付，T016 显式移交）；US5 依赖 US1 提交形态（T010）；
  同文件任务严格顺序，不同文件且仅依赖已完成任务者可并行（[P] 见各任务标注；T020 已去标记）。
- **Polish (Phase 8)**: T029/T030 在 T025 后可并行；T031 依赖全部故事；T032 最后。

### Task Dependency Chains（同文件顺序链）

- `confirmation_integration_test.go`: T013 → T016 → T019 → T021 → T022 → T023 → T027（严格顺序，禁并行）。
- `confirmation_auth_integration_test.go`: T025 → T028（严格顺序）。
- `confirm_test.go`: T003 → T014 → T020（顺序增补；对外 [P] 仅相对其他文件）。
- `confirmscan.go`: T011 → T018（顺序增补）。
- `config_test.go` / `serve_config_test.go`: T002 → T015。
- `000005` 迁移文件：T001 → T017（决议修订）。
- 独立文件天然并行：T026（race 新文件）与 T025；T033 接 T026 同文件顺序（禁并行）；T029/T030（文档/CI）。

### Parallel Example

```bash
# Foundational batch (different files, only T000-L dep):
Task: "T001 migration 000005 in migrations/000005_confirmation_tracking.sql"
Task: "T002 config threshold in internal/config/config.go"
Task: "T003 safe math in internal/indexer/confirm.go"
Task: "T004 metrics group in internal/metrics/metrics.go"
# US2 boundary batch (after T003/T002/T013):
Task: "T014 unit matrix in internal/indexer/confirm_test.go"
Task: "T015 illegal config matrix in internal/config/config_test.go"
```

---

## Implementation Strategy & Batches（仅制定，不执行）

### MVP First (Batch A)

T000-L → T001–T004 → T010–T013；**STOP and VALIDATE**（D1 主路径独立测试）→ 适用检查 + 完整差异检查 →
仅本地提交相关内容 → 报告并停止。

### Incremental Batches

- **Batch B（边界与配置）**: T014–T017（OI-1 已决议，T017 落地验证）→ 验证 → 提交 → 停止。
- **Batch C（异常与追赶）**: T018–T020 → 验证 → 提交 → 停止。
- **Batch D（幂等并发崩溃）**: T021–T023、T033 → 验证 → 提交 → 停止。
- **Batch E（门禁切换审计）**: T024–T028 → 验证 → 提交 → 停止。
- **Batch F（收尾）**: T029–T032 → 验证 → 提交 → 停止。

### Batch Protocol（每批统一约定）

完成相关任务 → `go test` 适用分层 + `gofmt` → 完整差异与文件冲突检查 →
仅本地提交相关内容（排除无关修改）→ 报告并停止。不推送、不建 PR、不合并。

---

## Coverage

### FR → Tasks

| FR | Tasks |
|----|-------|
| FR-01 公式/含块/下界 | T003, T010, T013, T014, T016, T017 |
| FR-02 本地链头/缺失停/滞后等 | T011, T012, T013, T019 |
| FR-03 阈值必填/拒绝/受控变更全语义 | T002, T015, T017, T024, T025, T027, T029, T033 |
| FR-04 高度+哈希归属 | T010, T018, T019 |
| FR-05 条件更新/依据/不可变 | T001, T010, T013, T021, T022, T023 |
| FR-06 异常停止/等待/暴露 | T011, T018, T019 |
| FR-07 持锁重读/拒旧提交 | T010, T021, T026 |
| FR-08 三暂停/版本条件/审计 | T010, T024, T026, T028, T033 |
| FR-09 Confirmed 语义/006 边界 | T025, T028 |
| FR-10 无余额/无新平台 | T012（结构无相关模块，评审门禁） |
| FR-11 可观察/脱敏 | T004, T012, T028 |
| FR-12 不锁实现/交接清单 | T032（评审门禁） |

### SC → Tasks

| SC | Tasks |
|----|-------|
| SC-01 N-1/N/N+1 | T013, T016 |
| SC-02 N=1 | T014, T016 |
| SC-03 异常零提交 | T019, T026 |
| SC-04 追赶不遗漏 | T019 |
| SC-05 重复恒 1 | T021 |
| SC-06 并发一方成功 | T021 |
| SC-07 读后变化零旧提交 | T026 |
| SC-08 不重写/崩溃恢复 | T022, T023 |
| SC-09 可追溯/零泄露 | T028 |
| SC-10 切换全周期 | T025, T027 |

### 验收场景 → Tasks（18 项）

US1-1/1-2→T010/T013；US2-1/2/3→T014/T016；US2-4→T025；US3-1→T018/T019/T020；
US3-2→T019；US3-3→T011/T019；US3-4→T019；US4-1→T021/T023；US4-2→T021；US4-3→T022；
异配置首启→T033；US5-1→T026；US5-2/3→T028；US5-4→T024/T025；US5-5→T015/T027/T029/T033。

## Verification Tiers（分层归属）

- 单元（PR CI：`make test` / `make test-race`）：T002（矩阵）、T003/T014/T020（数学与分类）、T004（注册）。
- 真实 PostgreSQL 并发/故障注入（`make test-integration`，testcontainers）：T001（迁移三条件）、
  T010（守卫回滚）、T021/T022/T023/T033（竞态/kill/不可改写/异配置首启）、T024（守卫）、T025/T026/T027（切换/竞争/恢复）。
- Anvil 集成（`make test-integration`，chain-id 31337）：T013/T016（边界推进）、T019（追赶/滞留）。
- CI 接入：T030（配置 diff 最小）。
- 非自动化项：无关键项缺自动化；T031 为指南式全量执行（含人工判读），时序项强制 5 次批次；T029 runbook 步骤由 T025/T027 自动化覆盖。

## Open Issues & Carryovers（非阻塞 tasks 生成；阻塞项下有标注）

- **OI-1（已决议关闭，2026-09-14 remediation）**：`confirmations` 定为 `NUMERIC` 精确整数
  （整数性 CHECK + 非负 CHECK；与 004 `amount NUMERIC` 先例同形），tip=MaxInt64、h=0 的 2^63 精确可存可审；
  Go↔SQL 经十进制字符串，禁 int64/float64 中转。原矛盾点（`BIGINT` 存不下 2^63）消除；
  决议位置：data-model §确认数计算/Table 1、research R1；落地：T001（列定义）+ T017（转换与审计验证）。
  历史记录保留：analyze 报告 F1（HIGH）→ 本决议关闭，未改规格公式，未收窄输入范围。
- **T000-P open**：生产就绪门禁，与本阶段任务无关，不宣称消除；生产部署不是默认批次（Batch A–F 均为本地范围）。
- **偶发本地失败未知**：004 遗留；本 tasks 文件头 Tests 纪律已继承（5 次批次 + 留痕），与新增任务的关系为共同约束。
- **上游 003 E1 open**：004 acceptance 遗留；005 零 RPC 但链头语义受 002 约束，T019 tip 相关场景若遇上游行为变更需回查。

## Notes

- [P] 仅标不同文件、无未完成前置依赖者；同文件增补链（上列）一律顺序，禁并行。
- 任务保持未完成（全 `- [ ]`）；设计完成不代替实现或验证结果。
- 无依据新增任务自查：T030（CI，章程 X/XI 门禁要求）、T032（本步自检，tasks 命令 Done When 要求）来源明确；
  其余任务均有 FR/SC/场景/D 场景映射（见 Coverage）。
- 停止边界：本文件交付后停止，不进入 analyze/implement；下一命令建议 `/speckit.analyze`（正式一致性分析）。
