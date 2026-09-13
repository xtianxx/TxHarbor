# Tasks: 003-event-indexing

**Input**: Design documents from `/specs/003-event-indexing/` (spec.md + 3 澄清， plan.md, research.md R1–R8, data-model.md, contracts/observability.md, quickstart.md)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ 均已就绪

**Tests**: 本规格以行为正确性为核心（章程 X/XI），集成测试为必选；单元测试覆盖纯逻辑（校验、编码、分类）。
并发一致性必须用真实 PostgreSQL + 双 worker 断言，`-race` 仅为补充，不得替代。

**E1 门禁说明**：T000 为实现及生产接入的前置门禁，不阻塞任务拆解与本地验证；
生产 enablement 在 T000 关闭前不得标记完成。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件、无依赖）
- **[Story]**: 归属用户故事（US1–US6，对应 spec.md 六个故事）
- 每个任务含：需求、验收场景、依赖、完成条件、涉及文件

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 分支与基线确认（无新脚手架；沿用 001/002 工程底座）

- [ ] T000 [E1-GATE] 确认 provider 文档、限制、截断与分页语义，填写 `quickstart.md` E1 附录
  - 需求：FR-12/FR-13，E1。验收场景：#4/#5 的生产等价覆盖。依赖：无（可与本地任务并行）。
  - 完成条件：附录表逐项填完（上限/区间/标志/分页/错误码原文）+ 来源链接可打开 + 上限经实测复核记录；
    在此之前生产 provider 不得标为可用，`LOG_BATCH_BLOCKS` 生产值不得宣称已复核。
    本任务 open 不阻塞 T001–T019 的本地（Anvil/假 RPC）工作。

**Checkpoint**: E1 状态显式为 open 并记录；本地工作可继续

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 迁移、配置身份、RPC 封装、可观测契约——所有用户故事的前置

**⚠️ CRITICAL**: 本阶段完成前不得开始用户故事实现

- [ ] T001 [P] 新增迁移 `migrations/000003_event_indexing.sql`（3 表 + 约束 + 索引）及迁移测试
  - 需求：FR-10/FR-11，data-model Table 1–3。验收场景：#11。依赖：T000 无（本地范围可先行）。
  - 完成条件：空库迁移、从 002 库升级、重复迁移三条件绿；PK/UNIQUE/CHECK 约束逐项有断言；
    原数据与 002 进度零破坏；`DOWN` 先删新表。
- [ ] T002 [P] 配置：`TXHARBOR_LOG_START_HEIGHT` / `TXHARBOR_LOG_CONTRACTS` / `TXHARBOR_LOG_BATCH_BLOCKS` 解析、校验与 `config_hash`（`internal/config/`）
  - 需求：FR-04/FR-05，research R3/R6。验收场景：#10。依赖：无。
  - 完成条件：单元测试绿——小写归一化/排序去重/空白拒绝；`config_hash` 与澄清 A1 向量一致
    （`"erc20-transfer:v1\n"` 前缀、无 BOM、无尾换行）；大小写/顺序/重复差异收敛同一身份；
    批次/超时参数不在身份内；非法值拒绝启动且 `Summary` 脱敏。
- [ ] T003 [P] `eth.FilterLogs` 封装 + `KindIncomplete` 错误分类（`internal/eth/client.go`）
  - 需求：FR-12/FR-13，research R4。验收场景：#4。依赖：无。
  - 完成条件：单元测试绿——超限/截断/分页余量/解析失败/超时/限流/未知错误逐类断言；
    未知错误一律按失败；不 hardcode 任何 provider 数值；计数达上限判定默认禁用。
- [ ] T004 [P] `log_*` 指标组与观测契约测试（`internal/metrics/metrics.go`，`contracts/observability.md`）
  - 需求：FR-17，research R7。验收场景：SC-11。依赖：无。
  - 完成条件：五指标名/标签/语义按契约断言；空进度删序列；`readyz` 语义冻结（日志暂停不翻转）；
    日志字段清单与脱敏要求有测试钩子。

**Checkpoint**: Foundation ready —— 迁移、配置身份、RPC 分类、可观测契约全部落定

---

## Phase 3: User Story 1 — 连续扫描与独立起点补齐（Priority: P1）🎯 MVP

**Goal**: 从独立起点按连续闭区间补扫并推进，覆盖正常/空区间/零金额事件

**Independent Test**: Anvil 预置混合区间，真库启动，原始完整且进度连续；002 先同步更高后日志仍从独立起点补齐

- [ ] T005 [US1] 区间上界与覆盖验证：`min(请求末端, 002 checkpoint)` + `[a,b]` 连续 canonical 验证（`internal/indexer/` 日志扫描文件）
  - 需求：FR-02/FR-03。验收场景：#1/#9。依赖：T001–T004。
  - 完成条件：上界越界/缺失/非 canonical/链暂停时拒绝越区推进；起点高于覆盖时等待不推进；单测 + 集成断言绿。
- [ ] T006 [US1] 扫描循环 + 区间原子提交（写入 + `next_block=b+1` 同事务，精确守卫）
  - 需求：FR-02/FR-11，data-model 写事务协议。验收场景：#1。依赖：T005。
  - 完成条件：成功区间推进恰为 `b+1`（含合法空区间）；任何失败无部分提交；首区间原子建行。
- [ ] T007 [P] [US1] Anvil 集成测试：混合区间 + 002 已更高后补扫（`internal/indexer/logscan_integration_test.go`）
  - 需求：FR-02/FR-08。验收场景：#1/#9（SC-01/SC-08）。依赖：T006。
  - 完成条件：topics/data 原始保留（含零金额），进度连续，无重复有效记录；真库。

**Checkpoint**: US1 独立可跑：连续补扫 + 原子推进成立

---

## Phase 4: User Story 2 — 重复与乱序下幂等（Priority: P1）

**Goal**: 重复投递/乱序/完全重复收敛；相同身份不同内容报告冲突

**Independent Test**: 同区间重放两次 + 打乱顺序 + 注入冲突，分别收敛与整批失败

- [ ] T008 [US2] 幂等重放 + 冲突内容比对 + 行数核对实现与测试
  - 需求：FR-09/FR-10。验收场景：#2/#6（SC-02/SC-05 冲突部分）。依赖：T006。
  - 完成条件：`ON CONFLICT DO NOTHING` + 逐字节比对 + 行数核对三件套；完全重复收敛零新增；
    任一字节不同整批失败且 checkpoint 不变；永不无条件忽略冲突。真库断言。

**Checkpoint**: US1+US2：重复安全，冲突显式失败

---

## Phase 5: User Story 3 — 故障、崩溃与 RPC 限制下不漏扫（Priority: P1）

**Goal**: 崩溃恢复无漏扫无部分提交；RPC 异常退避/缩批；单块超限停推暴露

**Independent Test**: 查询后 kill、事务中断、响应丢失后重启；假 RPC 逐项注入故障

- [ ] T009 [US3] 崩溃与不确定提交恢复测试（查询后退出 / 事务中失败 / 提交响应丢失后重启）
  - 需求：FR-11/FR-16。验收场景：#3（SC-03）。依赖：T006。
  - 完成条件：重启后从持久化进度继续，无漏扫无部分提交；未知结果先重读 DB 再决策（有专用断言）。
- [ ] T010 [US3] 退避/缩批/单块停推实现与测试（`KindIncomplete` 路径 + 有界退避）
  - 需求：FR-12/FR-13。验收场景：#4/#5（SC-04）。依赖：T003，T006。
  - 完成条件：超时/限流/解析失败有界退避不推进；超限丢弃自原高度缩批；单块仍不可确认 → 停推 + `range_incomplete` 行；
    失败区间永不跳过。假 RPC 逐项绿。

**Checkpoint**: 故障行为全部可恢复、可解释

---

## Phase 6: User Story 4 — 非法日志与非法配置整批拒绝（Priority: P1）

**Goal**: 6 类非法 + removed + 冲突整批失败；配置变化/空白名单明确拒绝且零破坏

**Independent Test**: 逐项注入非法日志与非法配置，核对失败语义与数据完好

- [ ] T011 [US4] 8 项逐条校验实现 + 6 类非法日志测试（`detail.class` 七分类）
  - 需求：FR-07/FR-09。验收场景：#6（SC-05）。依赖：T006。
  - 完成条件：错地址/错 topic/畸形 data/缺字段/removed/越界/冲突逐项整批失败，checkpoint 不变；
    确定性重现 → `validation_failed` 行且 `detail.class` 正确；零金额通过。
- [ ] T012 [US4] 重启配置比较 + 空白名单拒绝测试
  - 需求：FR-04/FR-05。验收场景：#10（SC-09）。依赖：T002，T006。
  - 完成条件：改起点/增删地址重启即拒绝退出（码非零），数据进度零破坏；比较对象是行内 `start_block`；
    空白名单拒绝且绝无 RPC 外发（有断言）。

**Checkpoint**: 非法输入与非法配置全部显式失败

---

## Phase 7: User Story 5 — 链视图异常暂停与并发安全（Priority: P1）

**Goal**: 链视图异常暂停；双 worker 真库竞争单次有效；旧 worker/过期暂停被拒

**Independent Test**: 缺失/哈希漂移/取数中分叉；双真 worker 同进度推进；延迟提交与过期暂停尝试

- [ ] T013 [US5] 链视图复核 + `chain_view_changed` 暂停实现与测试
  - 需求：FR-14/FR-15。验收场景：#7（SC-06）。依赖：T006。
  - 完成条件：缺失/哈希不一致/查询期分叉 → 不提交不推进 + 暂停行；不回退游标不删历史；
    暂停重启后仍有效。真库断言。
- [ ] T014 [US5] 双 worker 竞争 + 旧 worker 延迟提交：真实 PostgreSQL 双实例一致性测试
  - 需求：FR-16，research R1/R2。验收场景：#8（SC-07）。依赖：T006。
  - 完成条件：两个独立 pool + 独立 lease 句柄的真 worker 同抢同进度，有效推进恒为 1；
    旧 token/过期进度提交 0 行；同进程双 scanner 共享一 Lease 句柄（T017 接线）；
    **显式要求：本条必须跨 worker 数据库断言，`-race` 仅补充进程内竞争，不可替代本条**。
- [ ] T015 [US5] 暂停并发与原子条件测试（首暂停获胜 + 证据过期放弃 + 失权禁写过期暂停）
  - 需求：FR-16，data-model Table 3 原子条件。验收场景：#7/#8。依赖：T013，T014。
  - 完成条件：并发暂停恰一行；进度已变/证据消失/lease 失权 → 放弃且零写入；批回滚永不直写暂停行。

**Checkpoint**: 并发与暂停的正确性由数据库保证，而非进程内存

---

## Phase 8: User Story 6 — 迁移与可观察（Priority: P2）

**Goal**: 三条件迁移完好；进度/滞后/重试/暂停可查；错误可定位且零凭据泄露

**Independent Test**: 空库/002 升级/重复迁移；状态查询与日志脱敏审计

- [ ] T016 [US6] 迁移三条件 + 约束完好测试（并入 T001 实施，本条为验收复核位可跳过——保留编号防漏项）
  - 需求：FR-10/FR-11。验收场景：#11（SC-10）。依赖：T001。
  - 完成条件：同 T001 三条件绿；本条在 T001 完成时同步勾选。
- [ ] T017 [US6] `serve` 接线：双 scanner 共享 Lease/心跳 + 状态镜像 + 安全退出
  - 需求：FR-16/FR-17，research R1/R7。验收场景：#3（退出部分）/SC-11。依赖：T004，T006，T014。
  - 完成条件：单 `NewLease` + 单心跳供两 scanner；`log_*` 指标随 `metricTicker` 镜像；
    终止信号下未提交零残留；配置拒绝/失权/不可重试错误按类别退出或停写。
- [ ] T018 [US6] 可观察端到端：滞后/状态/暂停查询 + 脱敏审计
  - 需求：FR-17。验收场景：SC-11。依赖：T004，T013。
  - 完成条件：运行/重试/暂停各态下进度、滞后、重试、暂停原因可查（指标 + 诊断 SQL）；
    全量日志凭据零出现；无无限制原始响应转储（有审计测试）。

**Checkpoint**: 运维可区分"追赶/等待/重试/暂停"，迁移无损

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: 全量验证、门禁复核、一致性检查

- [ ] T019 全量验证：`make build` + `make lint` + `go test ./...` + `go test -tags integration ./...` 全绿
  - 依赖：T007–T015，T017–T018。完成条件：四命令一次全绿；失败按回归处理，不弱化断言。
- [ ] T020 规格/计划/任务一致性复核 + 实现门禁报告
  - 依赖：T019。完成条件：逐项核对 FR-01–19、SC-01–11、11 验收场景在 tasks 的覆盖（见下表）；
    输出未解决项（E1）与实现门禁（T000 关闭 + 本复核通过）；发现语义冲突先澄清，不私改规格。

---

## Dependencies & Execution Order

- **Setup (T000)**: E1 门禁任务；open 状态下 T001–T019 本地工作可并行推进，生产实现不可开始。
- **Foundational (T001–T004)**: 全部完成 → 阻塞所有 US。T001/T002/T003/T004 相互独立，可并行。
- **US1 (T005–T007)**: 依赖 Foundational；T007 依赖 T006。
- **US2 (T008)**: 依赖 T006；与 US3–US6 可并行。
- **US3 (T009–T010)**: 依赖 T006（T010 另依赖 T003，已在 Foundation 内）。
- **US4 (T011–T012)**: 依赖 T006（T012 另依赖 T002）。
- **US5 (T013–T015)**: 依赖 T006；T015 依赖 T013/T014。
- **US6 (T016–T018)**: T016 随 T001；T017 依赖 T004/T006/T014；T018 依赖 T004/T013。
- **Polish (T019–T020)**: 依赖所有实现任务。

## 覆盖矩阵（复核用）

| FR/SC | 任务 | 验收场景 |
|-------|------|----------|
| FR-01/02/03 | T005，T006 | #1，#9 |
| FR-04/05 | T002，T012 | #10 |
| FR-06 | T002（参数排除），T010（调参语义） | #4 |
| FR-07/09 | T011 | #6 |
| FR-08 | T007 | #1 |
| FR-10 | T001，T008 | #2，#11 |
| FR-11 | T001，T006，T009 | #1，#3，#11 |
| FR-12/13 | T003，T010，T000（生产） | #4，#5 |
| FR-14/15 | T013 | #7 |
| FR-16 | T009，T014，T015，T017 | #3，#8 |
| FR-17 | T004，T017，T018 | SC-11 |
| FR-18/19 | T020（范围门禁） | — |
| SC-01–11 | T007–T015，T018 | #1–#11 |

## Notes

- `[P]` 仅标记不同文件无依赖可并行；同文件（`internal/indexer/` 内）任务按顺序执行防冲突。
- 真库测试一律 `//go:build integration` + testcontainers，复用 002 helper；Anvil 产 Transfer（含零金额）。
- T014 必须双真 worker；`-race` 为附加项。
- E1 未关闭前，任何"生产可用"表述不得出现；残余风险如实声明。
