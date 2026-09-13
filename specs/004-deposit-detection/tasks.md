# Tasks: 004-deposit-detection

**Input**: Design documents from `/specs/004-deposit-detection/` (spec.md + 6 澄清 Q1–Q6， plan.md, research.md R1–R11, data-model.md, contracts/observability.md, quickstart.md)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ 均已就绪

**Tests**: 本规格以行为正确性为核心（章程 X/XI），集成测试为必选；单元测试覆盖纯逻辑（解析、编码、分类）。
并发一致性必须用真实 PostgreSQL + 双 worker 断言，`-race` 仅为补充，不得替代。
已知偶发本地失败（原因未知）不得豁免任何门禁：时序敏感测试按固定批次执行（每批次恰好 5 次，记录全部结果；任一次失败则该批次不通过；重跑另记批次），证据齐全方可关闭；未解释失败持续为未解决事项（后续成功不自动关闭），无新证据停跑并报告待诊断问题。

**门禁说明**：T000-L 关闭 → 可开始本地范围实现（Anvil + DB 播种）；
T000-P 关闭 + T022（本地验收）通过 → 方可谈生产接入就绪。本地验收通过不等于生产就绪。
生产就绪额外依赖上游 003 E1（provider 附录）；004 自身零 RPC，但消费的上游覆盖语义受其约束。

- [x] T000-L 本地实现前置检查（Anvil + DB 播种范围；满足即可关闭）
  - 需求：R10 待核验项。依赖：无。
  - 内容：`Coordinator` 第三循环注册点形状确认（`coordinator.go:124-125` 现双循环）；
    `serve.go` 接线位置确认；goose `000004` 在 `000003` 之后顺序与 `embed.go` 收录确认；
    pgx → `NUMERIC` 的 `big.Int` 十进制映射写法确认（常规用法，单测锁定）；
    `deposit:v1` 标准向量 `31822b65…cf6600f0` 复现（`printf | sha256sum`）；
    Anvil 测试 token 部署方式确认；DB 播种清单完备（canonical 块 + 日志行、前置暂停行、冲突行）。
    授权事务载体二选一清单确认（SQL 事务函数 vs 受控 SQL 脚本，语义见 R11；选型由 T024 交付）。
    这些结果仅证明本地行为，不证明生产行为。
  - 完成条件：上项逐项可勾选；关闭后 T001–T020、T022、T024–T028 可在本地范围按依赖执行。
  - 状态（2026-09-13）：CLOSED。证据：1)Coordinator形状 coordinator.go:43 RunPair双serve、:123 chan2、:124-125双goroutine、:131 join2，第三循环落点同处；2)serve.go:238 RunPair调用点唯一；3)goose机制 migrate.go:65-93 glob+排序+最高版本target、embed.go:10-13 *.sql自动收录，000004待T001新建；4)pgx v5.11.0标准Numeric{Int,Exp,Valid}用法成立、仓库零现例、T002/T004单测锁定；5)printf无尾换行复现 deposit:v1向量 31822b65…cf6600f0 MATCH；6)Anvil方法=setCode+emitCode无forge先例，T007须显式采用；7)DB播种三类清单分散存在无汇总seed代码符合实现前；8)授权载体二选一封闭（事务函数vs受控脚本，R11），选型归T024。
- [ ] T000-P 生产门禁（保持 open；约束生产接入与部署，不阻塞本地实现）
  - 需求：生产就绪。依赖：上游 003 T000-P（E1）关闭 + T022 通过。
  - 完成条件：在此之前 004 不得标为生产可用；本地验收通过不得自动关闭本任务。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件、无依赖）
- **[Story]**: 归属用户故事（US1–US5，对应 spec.md 五个故事）
- 每个任务含：需求、验收场景、依赖、完成条件、涉及文件

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 门禁状态显式记录（无新脚手架；沿用 001/002/003 工程底座）

T000-L / T000-P 见上（本阶段即二者建档）。**Checkpoint**: 门禁状态显式记录；本地工作可继续。

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 迁移、配置身份、纯解析逻辑、可观测契约——所有用户故事的前置

**⚠️ CRITICAL**: 本阶段完成前不得开始用户故事实现

- [x] T001 [P] 新增迁移 `migrations/000004_deposit_detection.sql`（5 表 + 序列 + 约束 + 索引）及迁移测试
  - 需求：FR-06/FR-09/FR-12/I1/I2，data-model Table 1–5。验收场景：D10。依赖：T000-L（本地范围）。
  - 完成条件：空库迁移、从 003 库升级、重复迁移三条件绿；PK（来源身份）/ `amount > 0` /
    `status = 'pending'` / `config_hash` 全小写 hex / `next_block >= start_block` / `kind` 枚举约束逐项有断言；
    history 表 PK（来源链 + 版本 seq，本链递增）、`prev_seq` 链不断、行只增不改（无 UPDATE/DELETE 路径断言）；
    `request_id` 非空本链唯一 + 首版本 NULL 单行（partial unique）；
    history 预期目标列（expected_pause_id/revision，双空或双非空 CHECK）；
    观察行 `version_seq` 外键指向 history（chain_id, version_seq）；
    暂停表 `pause_id`（SEQUENCE 永不复用）+ `revision` 列 + 审计表 Table 5（含 action 枚举与实例事件唯一约束）；
    两处二级索引存在（history/审计表无二级索引）；原数据与 002/003 进度零破坏；`DOWN` 先删新表（含 sequence）。
  - 状态（2026-09-13）：CLOSED。证据：空库/003升级/重复迁移绿（集成37子项），约束负例+正例，pause序列不复用，DOWN回滚后重升；prev_seq自引用FK为存储层落实；NUMERIC真库往返（deposit_numeric_roundtrip集成实跑）：1wei与2^256-1精确，Exp归一探针过，0/-1触CHECK(23514)拒绝。
- [x] T002 [P] 配置：`TXHARBOR_DEPOSIT_START_HEIGHT` / `DEPOSIT_CONTRACTS` / `DEPOSIT_WATCH_ADDRESSES`
  （`addr[:effective]` 语法）/ `DEPOSIT_BATCH_BLOCKS` 解析、校验与配置身份（`internal/config/`）
  - 需求：FR-04/FR-05/FR-06/I5，research R3/R7。验收场景：D5（SC-07 部分）。依赖：无。
  - 完成条件：单元测试绿——20 字节校验/小写归一化/排序去重（同条目不同高度不合并）/缺省高度=全局起点/
    任一集合空白拒绝启动；配置身份与 R3 向量一致（`"deposit:v1\n"` 前缀、无 BOM、无尾换行；
    标准向量 `31822b65…cf6600f0` 实现必须复现）；大小写/顺序/重复差异收敛同一身份；
    批次/超时参数不在身份内；非法值拒绝启动且 `Summary` 脱敏；
    history 快照编码（`contract:effective` / `address:effective` 行，字典序，无尾换行）有单测锁定，供 T025 写入。
  - 状态（2026-09-13）：CLOSED。证据：config单元90通过（含向量复现/归一/拒绝/收敛/脱敏），R3向量独立复算一致；回归夹具env已补（db并发+app三处）；.env.example补4变量（3必填+1可选默认500），示例serve黑盒加载过（deposit_config_hash复现R3向量，后续失败为迁移未应用依赖态）。
- [x] T003 [P] `deposit_*` 指标组与观测契约测试（`internal/metrics/metrics.go`，`contracts/observability.md`）
  - 需求：FR-15，research R8。验收场景：SC-09（D1–D9 状态断言共用）。依赖：无。
  - 完成条件：六指标名/标签/语义按契约断言（含 `state=4` 结构停止、`result` 四分类计数器、
    `transition_total{result}` 授权审计计数）；
    空进度删序列；`readyz` 语义冻结（充值暂停/停止不翻转）；日志字段清单与脱敏要求有测试钩子。
  - 状态（2026-09-13）：CLOSED。证据：metrics 8/8通过（含-race），六指标/标签/语义按契约，readyz不翻转，日志字段与脱敏钩子锁定；logx顺序缺陷已修（bearer先于kvSecret，合成凭据回归，logx/metrics复绿）；残余authorization=Basic无正则留后续。
- [x] T004 [P] Transfer 解析与匹配纯逻辑 + 单元测试（`internal/indexer/depositparse.go` 新文件）
  - 需求：FR-01/FR-02/FR-03/FR-04/FR-05。验收场景：D1/D2 的逻辑部分。依赖：无（纯函数，不碰 DB）。
  - 完成条件：单元测试绿——`topic0` 以 `Keccak256("Transfer(address,address,uint256)")` 断言
    （禁无断言魔法字符串）；地址 topic 高 12 字节零检查；sender/recipient 低 20 字节提取；
    `data` 32 字节 big-endian → 十进制字符串（零、1 wei、uint256 最大值 `2^256-1` 边界全覆盖，
    精确无浮点）；三元组生效高度闭区间规则；白名单/监控集合精确匹配；任一非法整批失败语义。
    本文件只做纯逻辑，零 DB、零 RPC。
  - 状态（2026-09-13）：CLOSED。证据：10顶层/26子测试全过（签名派生/归一/零值·1wei·2^256-1往返/闭区间/精确匹配/整批失败），全仓单元绿，count=5与-race附加通过；DB级NUMERIC往返归T002/T006/T001。
- [x] T024 [P] 授权事务载体选型与模块骨架（`internal/indexer/depositauth.go` 新文件）
  - 需求：FR-06/FR-12，research R11。验收场景：D11（选型部分）。依赖：T000-L。
  - 完成条件：SQL 事务函数 vs 受控 SQL 脚本二选一并书面记录理由（注释 + 任务完成记录）；
    骨架声明授权事务边界（重验门、原子提交项、幂等/过期规则），无业务逻辑也可先合入；
    依赖本选型的 T025/T027/T028 在此之前不得假定任一载体。
  - 状态（2026-09-13）：CLOSED。选型：受控SQL脚本（参数化语句序列+显式BEGIN..COMMIT，非PL/pgSQL函数）。依据见depositauth.go头注释：语句级故障注入、协议步骤1为Go工作、零存储过程先例、被评审即被执行产物、入口权限不变；骨架含重验门/原子提交项/幂等过期规则，入口stub待T025实现。

**Checkpoint**: Foundation ready —— 迁移、配置身份、解析逻辑、可观测契约、授权载体全部落定

---

## Phase 3: User Story 1 — 合法匹配生成 Pending 充值观察（Priority: P1）🎯 MVP

**Goal**: 匹配日志恰好一条 Pending 观察；上游对齐与覆盖证明成立；单元原子提交

**Independent Test**: Anvil 部署测试 token 转入监控地址，真库启动，每条匹配恰好一条 Pending 且字段一致

- [x] T005 [US1] 上游对齐与覆盖证明：`H'` 重算比对 + `(S_u,H_u,N_u)` 读取 + `N_u > b` 证明 +
  canonical 重核（`internal/indexer/depositscanner.go` 新文件）
  - 需求：FR-07/FR-11/I4，research R2/R5。验收场景：D1/D2（SC-01 部分）。依赖：T001–T004。
  - 完成条件：env 白名单重算身份与上游行 `config_hash` 不一致即拒绝启动（`upstream_drift`）；
    覆盖证明除 `N_u > b` 外同时核验链身份一致、上游起点、暂停三行皆无、引用块逐块 canonical；
     证明逻辑有单测 + 集成断言；先读 checkpoint 后读行的语句序在注释中说明理由（R2）。
  - 状态（2026-09-13）：CLOSED。证据：depositscanner.go新建（零写入零RPC）；单测7顶层/15子全过；集成7顶层/14子全过（drift拒绝/逐块canonical/三缺口/三暂停阻断/空区间/完整性五分支）；H'重算、N_u>b+链身份+上游起点+暂停皆无+逐行绑定齐全。
- [x] T006 [US1] 扫描循环 + 单元原子提交（观察写入 + `next_block=b+1` 同事务，精确守卫）（`internal/indexer/depositcommit.go` 新文件；循环骨架落入 `depositscanner.go`，与 T005 同文件按序扩展）
  - 需求：FR-08/FR-09/I1/I2/I3，data-model 写事务协议。验收场景：D1/D4（SC-01/SC-04 部分）。依赖：T005。
  - 完成条件：成功单元推进恰为 `b+1`；首单元原子建行（含首版本 history 行，version_seq=1，request_id=NULL，operator=`bootstrap`，与 checkpoint 首行同事务）；
    读取时捕获当时版本 seq，提交时在锁内按 version_seq 核对仍一致（不一致即放弃重读；回环下哈希相同亦须放弃）；
    新观察写入捕获版本（禁取提交时刻最新 seq 冒充）；冲突内容比对排除 `version_seq`（回放保留原值）；
    精确守卫 `next_block=a` + 配置一致；单侧缺失（checkpoint/history 恰其一有行）即损坏态报错，禁补建；
     行数核对（插入 + 已存在一致 + 合法零生成 = 去重身份数）；未知提交结果重读 DB 幂等继续；
     任何失败无部分提交。真库断言。
  - 状态（2026-09-13）：CLOSED。证据：depositcommit.go新建（513行）+scanner循环骨架按序扩展；全仓单元274过；集成TestDeposit 56过（含T005回归）+race附加过；首单元原子/bootstrap同事务/版本锁内重验/冲突排除version_seq/精确守卫/单侧损坏禁补建/行数核对/未知提交重读齐全。未验证：Anvil全栈归T007；durable暂停行与恢复归T014/T015；ServeLoop的*Lease注册包装归T018。记录：批次边界已修正为b=min(a+BatchBlocks-1,N_u-1)（上限语义，依据research R167；溢出/下溢有守卫），仅N_u<=a等待；V1–V6真库验证全过（orchestrator独立重跑三项新集成14过）；commitResultVisible经协议复核无误判窗口（lease串行+精确守卫+行数核对，见lane报告），不重构。
- [x] T007 [US1] Anvil 全栈集成测试：匹配生成 Pending（`internal/indexer/deposit_integration_test.go`）
  - 需求：FR-01/FR-02/FR-11。验收场景：D1（SC-01）。依赖：T006。
  - 完成条件：测试 token 转入监控地址 → 恰好一条 Pending，sender/recipient/amount/来源与区块身份一致；
    多条匹配各一条；真库。
  - 状态（2026-09-13）：CLOSED。证据：TestDepositAnvilFullStackPending真Anvil(v1.8.1隔离链)+真PG(18.6)实跑过；2笔真实Transfer经002/003索引后各恰一条Pending（sender/recipient/1与2wei/来源与区块身份/版本与配置哈希逐项对链真值）；非匹配零生成但推进；BatchBlocks=500下完整尾部一次消费（next==N_u）；上游身份重算一致。lane实跑5.44s通过，orchestrator独立重跑通过。测试入口直连组件，不等于生产serve接入（T018开放）。未验证：T008+、完整验收。

**Checkpoint**: US1 独立可跑：匹配生成 + 覆盖证明 + 原子推进成立

---

## Phase 4: User Story 2 — 非匹配与零值不生成但正确推进（Priority: P1）

**Goal**: 非监控/非白名单/零值/低于生效高度零生成且进度连续

**Independent Test**: 预置混合区间，零观察生成，进度单调推进

- [x] T008 [US2] 非匹配与零值推进测试（`internal/indexer/deposit_integration_test.go`，与 T007 同文件，按序执行）
  - 需求：FR-01/FR-03/FR-05。验收场景：D2（SC-01）。依赖：T006。
  - 完成条件：非监控地址/非白名单资产/零值/低于任一生效高度逐项零生成且进度越过；
    合法空区间零生成并推进到 `b+1`；`observations_total{result}` 四分类计数正确。真库断言。
  - 状态（2026-09-13）：CLOSED。证据：TestDepositMixedIntervalZeroGeneration真库实跑过（DB播种上游行，方法已声明；Anvil路径归T007）；低于生效高度/非白名单/非监控/零值逐项零生成且进度越过，空尾区间推进到b+1；来源行字节一致无新增；四分类 matched=0/nomatch=3/zero=1/invalid=0；最小修正：depositscanner结果观察钩子（nil-safe，T018前默认nil）。lane单跑+5/5批次+TestDeposit回归全过，orchestrator独立重跑通过。未验证：T009+、完整验收。

**Checkpoint**: US1+US2：生成精确，不生成同样精确

---

## Phase 5: User Story 3 — 重复处理与崩溃恢复不重复生成（Priority: P1）

**Goal**: 重放收敛、冲突显式失败；崩溃/未知提交恢复无漏判无部分提交

**Independent Test**: 同单元重放两次；处理中途 kill、事务中断、提交响应丢弃后重启

- [x] T009 [US3] 幂等重放 + 冲突内容比对测试（`internal/indexer/deposit_integration_test.go`，按序执行）
  - 需求：FR-09/FR-10/I1。验收场景：D3（SC-02）、D2 冲突部分（SC-05）。依赖：T006。
  - 完成条件：`ON CONFLICT DO NOTHING` + 逐字段比对 + 行数核对；完全重复收敛零新增；
    任一字段不同整批失败且进度不变；永不无条件忽略冲突。真库断言。
  - 状态（2026-09-13）：CLOSED。证据：完全重复重放整行含版本关联逐字段相等零新增；5可构造字段逐一整批失败、进度不变、先插新身份回滚、已存行字节不变、零暂停行（status异值受CHECK约束不可播种已声明）。orchestrator独立重跑通过。未验证：T011+、完整验收。
- [x] T010 [US3] 崩溃与不确定提交恢复 + 事务回滚测试（conn-wrapper 故障注入；`internal/indexer/deposit_integration_test.go`，按序执行；本条按固定批次执行：每批次恰好 5 次并记录全部结果，任一次失败则该批次不通过，不追加运行凑成功；修复或明确诊断后的重跑另记批次并保留原证据）
  - 需求：FR-09/I2，research R9。验收场景：D3（SC-03）、D4（SC-04）。依赖：T006。
  - 完成条件：查询后退出/事务中失败/提交响应丢失后重启，从持久化进度继续，无漏判无重复无部分提交；
    未知结果先重读 DB 再决策（有专用断言）；DB 瞬时错误有界退避零推进。
  - 状态（2026-09-13）：CLOSED。证据：conn-wrapper确定性注入（转发后断连/转发前断连/闸门同步，无睡眠碰撞）；查询后退出/事务中失败/提交响应丢失/瞬时错误四场景重启无漏判无重复无部分提交；已提交vs未提交专用判定对（连接错误不当回滚）；固定批次恰5次全过（9.5–13.0s），修复前失败日志保留另记批次；最小修正：ServeLoop取消守卫（+7行，取消属关停非裁决）。lane回归TestDeposit 96过+race净，orchestrator独立重跑15过。回滚/未知恢复不报告未经证实成功（发射仅在提交单元与拒收批次）。未验证：T011+、完整验收。

**Checkpoint**: 重试与崩溃行为全部可恢复、可解释

---

## Phase 6: User Story 4 — 配置、生效高度与历史补扫（Priority: P1）

**Goal**: 配置变化拒绝零破坏；暂时缺口等待恢复；结构缺口报错停止并可恢复

**Independent Test**: 改配置后重启；上游滞后追赶；所需历史超出上游范围时的停止与修复恢复

- [x] T011 [US4] 重启配置比较 + 空白名单拒绝 + 上游漂移拒绝测试（`internal/indexer/deposit_integration_test.go`，按序执行）
  - 需求：FR-06/I5。验收场景：D5（SC-07）。依赖：T002，T006。
  - 完成条件：改资产/地址/高度/起点重启即拒绝退出（码非零），观察与进度零破坏；
    比较对象是行内 `start_block` + `config_hash`；空白名单拒绝且绝无上游外读；
    env 白名单与上游行身份漂移即拒绝（`upstream_drift`）；改回原配置后重启，
    精确守卫通过并从原进度继续，观察零变化（真库断言）；
    启动前断言两侧同有同无 + 行内与最新 history 行关联一致，单侧缺失即损坏态报错（禁补建/改回/授权修复）；
    本条仅覆盖意外漂移改回路径；有意授权转换见 T025–T028，两条路径不得混同。
  - 状态（2026-09-13）：CLOSED。证据：TestDepositRestartConfigComparison真库8子项全过（资产/监控地址/起点/生效高度四变体各拒*depositConfigMismatchError且检查点next=15、观察快照、history=1零破坏；起点单变体证比较对象为行内start+hash非hash alone；三空白集构造器同步拒绝零表行；上游ff持久身份拒upstream_drift且无检查点行；单侧checkpoint-only/history-only/行内与最新seq2dd分歧三态皆*depositCorruptStateError，loop层复验checkpoint-only；改配置拒后改回原配置精确守卫通过15→21，新观察amount=1/version=1，孤儿行字节一致）。回归：go build净+全仓单元283过。未验证：T012+、完整验收。
- [ ] T012 [US4] 缺口分类与恢复：暂时等待自动续 + 结构报错停止 + 修复重验后原缺口幂等恢复（`internal/indexer/deposit_integration_test.go`，按序执行）
  - 需求：FR-07/I3/I6，research R5。验收场景：D6/D7（SC-06 部分、SC-08）。依赖：T006。
  - 完成条件：`p >= N_u`（范围内）→ 等待零推进，补齐后自动从缺口继续；
    `E < S_u` / 资产不在覆盖 → 报错停止 + `upstream_gap(structural)` 行（范围/原因/配置版本齐全）；
    禁超时判定（慢速上游不得误判，有专用断言）；人工修复补扫后重验完整，从原缺口幂等恢复；
    结构缺口的身份采纳走授权转换（T025），本条不断言身份更新本身，只断言缺口分类与等待/停止行为；
    全程不跳位、不记无充值、不静默调起点/高度。真库断言。
- [ ] T013 [US4] 覆盖证明专项验证：checkpoint 高度永不单独作为完整证明（`internal/indexer/deposit_integration_test.go`，按序执行）
  - 需求：FR-07/FR-11/I3。验收场景：D2/D5/D6/D8 交叉。依赖：T005，T006。
  - 完成条件：空区间（零行但 `N_u > b`）推进 vs 上游缺失（`N_u <= b`）停留的对照断言；
    新增资产（覆盖内外两种）、配置不一致、提交期间链状态变化（canonical 翻转）四场景下，
    证明五要素（高度水位、链身份、上游起点、配置指纹+白名单、三暂停行、canonical、lease）缺一即停推。
    真库断言。
- [ ] T025 [US4] 授权转换实现：身份更新/位置回放/暂停处置/审计原子提交 + 前置重验（`internal/indexer/depositauth.go`，与 T024 同文件按序扩展）
  - 需求：FR-06/FR-09/FR-12/I2，research R11，data-model 授权协议。验收场景：D7/D11。依赖：T024，T006。
  - 完成条件：同一 lease 锁下重验（身份仍旧 + 覆盖重证明 + canonical + 暂停处置条件），任一失败全回滚；
    history 行以 version_seq 为身份（持锁取 max+1），审计字段（request_id、expected_old_seq、操作者/时间/旧新哈希/位置变化/原因）齐全，缺任一即回滚；
    暂停处置按实例 + 修订匹配（含调用方 expected_pause，非空必须一致，否则按目标替换拒绝）并同事务写审计行；
    双空目标按保留／必须处置分支执行：证据在 scope 外可保留时提交身份／位置／history 且暂停行原样保留
    （消费仍停）；证据在 scope 内已证解决而无目标时拒绝（调用方须以新 request_id 重新明确授权）；
    锁内暂停状态与依据不一致 → 回滚并报告状态变化，不扩大授权；
    授权入口完整性前检（两侧同有＋行内与最新 history 关联一致），损坏态拒绝（只读返回不受影响）；
    授权路径禁 UPDATE／合并暂停行（仅保留或条件 DELETE＋同事务审计）；
    请求幂等按 request_id 先查 history：同 ID 同参返回已记录结果（即使已进入更晚版本），同 ID 异参明确拒绝，
    异 ID 即使同参亦独立校验；expected_old_seq 等于当前最新 seq 否则过期拒绝；过期拒绝报告预期与当前版本；
    提交成功即生效；切换前已提交有效；跳过 owner/token 检查但操作员身份入审计；空授权（H′==H）拒绝。真库断言。
- [ ] T026 [US4] 历史回放与收缩边界实现：最小值规则/收缩向前/混合双规则/history 快照读写（`internal/indexer/depositscanner.go`/`depositcommit.go` 按序扩展 + `depositauth.go` 回放计算）
  - 需求：FR-05/FR-06/FR-07/I1/I3，research R5/R11。验收场景：D6/D7/D11。依赖：T025。
  - 完成条件：组合有效起点 max 规则 + replay min 规则 + 上游起点仅检查（禁抬高裁剪）；
    纯收缩 replay_from = 当前 next；收缩边界 = 切换前 next，禁追溯；回放幂等不新增不重置；
    history 快照读写；混合增减双规则并存。真库断言。
- [ ] T027 [US4] D11 授权子集测试（1）：失败回滚/重复/过期/未知/旧在途隔离/越权解除（`internal/indexer/deposit_integration_test.go`，按序执行；本条按固定批次执行：每批次恰好 5 次并记录全部结果，任一次失败则该批次不通过，不追加运行凑成功；修复或明确诊断后的重跑另记批次并保留原证据）
  - 需求：FR-06/FR-09/FR-12。验收场景：D11。依赖：T025。
  - 完成条件：前置任一失败全回滚原状；旧版本在途提交 0 行；请求四则断言——同 ID 同参跨版本重试返原结果不重执行、
    同 ID 异参拒绝无状态变化（含同 ID 更改目标）、异 ID 同参独立校验、过期拒绝报告预期与当前版本；
    显式目标被替换时授权全回滚（身份／位置／history／暂停均不变），不得改处置新暂停；
    NULL 目标遇到已有或并发新增暂停时，已有暂停原样保留、必须处置则拒绝并要求重新明确授权；
    保留分支成功时提交身份／位置／history、暂停原样保留、消费仍停止（暂停行仍在、零推进），
    保留暂停后续可经针对该实例的人工解除＋现版本重验恢复（由 T015 锁定）；
    裁决期间暂停变化（新增／替换／修订变化／消失）→ 回滚并报告状态变化，不处置；
    缺目标拒绝后沿用旧 request_id 加目标重试 → 按同 ID 异参拒绝，须新 request_id 重新授权；
    H1→H2→H1 下旧授权按过期拒绝（可定位原记录，不虚构成功）；
    提交未知以 DB 为准；暂停不可越权解除（无授权的删行/改行/清行即失败）；
    陈旧实例条件删除新暂停实例影响 0 行且审计无该实例释放记录；
    相同内容复现与同时间戳下观察版本引用可区分（seq 外键断言）；真库断言。
- [ ] T028 [US4] D11 授权子集测试（2）：历史回放/收缩边界/混合变更/多次切换嵌套（`internal/indexer/deposit_integration_test.go`，按序执行；本条按固定批次执行：每批次恰好 5 次并记录全部结果，任一次失败则该批次不通过，不追加运行凑成功；修复或明确诊断后的重跑另记批次并保留原证据）
  - 需求：FR-05/FR-06/FR-07/I1/I3。验收场景：D6/D7/D11。依赖：T025，T026，T012。
  - 完成条件：回放幂等无新增（已有观察保留原版本引用）；收缩历史保留、边界向前、无跳位；
    混合双规则并存；回放未完成时二次转换重算收敛；嵌套回放中边界之后尚无观察的来源适用后续收缩；
    H1→H2→H1 下旧消费提交被版本隔离守卫放弃并重读（禁打最新 seq 冒充）；
    结构缺口判定（禁裁剪）；真库断言。

**Checkpoint**: 配置与历史的正确性由证明保证，而非运气；授权转换与回放见 T024–T028

---

## Phase 7: User Story 5 — 链暂停、来源失效与可观察（Priority: P2）

**Goal**: 失效输入零生成；分层恢复可验收；并发单次有效；运维可观测

**Independent Test**: 构造暂停与失效；解除上游暂停观察自动续；人工解除走重验门；双真 worker 竞争

- [ ] T014 [US5] 链视图复核 + `chain_view_changed` / `validation_failed` 暂停实现与测试（实现落入 `depositscanner.go`/`depositcommit.go`（T005/T006 已建，按序扩展）；测试落入 `internal/indexer/deposit_integration_test.go`）
  - 需求：FR-12/I4。验收场景：D8（SC-06）。依赖：T006。
  - 完成条件：引用块缺失/非 canonical/来源失效 → 不提交不推进 + 暂停行；
    新建暂停行携带 `pause_id`（SEQUENCE 分配）+ `revision=1` + `version=<seq>` 标签；
    `detail.class` 七分类正确；不回退进度不删历史；暂停重启后仍有效。真库断言。
- [ ] T015 [US5] 分层恢复验证：上游解除自动续 + 自身暂停按实例条件解除与重验门 + 需 006 时保持暂停（`internal/indexer/deposit_integration_test.go`，按序执行；本条按固定批次执行：每批次恰好 5 次并记录全部结果，任一次失败则该批次不通过，不追加运行凑成功；修复或明确诊断后的重跑另记批次并保留原证据）
  - 需求：FR-12/I6，research R6。验收场景：D8（SC-06）。依赖：T014。
  - 完成条件：上游行消失 + 链视图一致 + 覆盖完整 + 位置有效 → 自动从原位置继续，
    且 004 从未清除上游行（有断言）；自身暂停按实例条件解除（`pause_id` + `revision` 匹配）并同事务写审计行，
    审计失败则解除回滚；解除后重验任一失败 → 重新建行（新实例身份）保持暂停；
    陈旧条件删除新实例影响 0 行且新行仍在（P1→P2 替换场景必测）；
    原目标已解除但当前已有 P2 时再解除：查审计返原结果（原结果 + 原操作者），不重写、不触碰 P2、不归功本次调用者；
    无独立解除请求身份时只返回该事实，不声称识别为同一次请求（操作者/原因/实例/修订相同亦不证明同一请求）；
    保留暂停的跨版本解除：暂停打标版本与当前 seq 不一致时，仍可按实例＋修订条件解除＋同事务审计，
    解除后按当前版本重验（版本标签不作为解除条件）；
    解除动作本身零写入消费状态（解除≠授权）；旧分叉失效/需回退而 006 不可用 → 保持暂停且 `needs_006=true` 可查。
    授权转换路径的暂停处置见 T025/T027，本条仅覆盖人工删除路径。
- [ ] T016 [US5] 暂停并发与原子条件测试（首暂停获胜 + 证据过期放弃 + 失权禁写过期暂停）（`internal/indexer/deposit_integration_test.go`，按序执行；本条按固定批次执行：每批次恰好 5 次并记录全部结果，任一次失败则该批次不通过，不追加运行凑成功；修复或明确诊断后的重跑另记批次并保留原证据）
  - 需求：FR-09/FR-12，data-model Table 3 原子条件。验收场景：D8。依赖：T014。
  - 完成条件：并发暂停恰一行；进度已变/证据消失/lease 失权 → 放弃且零写入；批回滚永不直写暂停行。
- [ ] T017 [US5] 双真 worker 竞争 + 旧 worker 延迟提交：真实 PostgreSQL 双实例一致性测试（含偶发有界重复）
  - 需求：FR-08/FR-09，research R1/R2。验收场景：D9（SC-02/SC-03 并发部分）。依赖：T006。
  - 完成条件：两个独立 pool + 独立 lease 句柄的真 worker 同抢同进度，有效推进恒为 1（`internal/indexer/deposit_integration_test.go`，按序执行）；
    旧 token/过期进度提交 0 行；**显式要求：本条必须跨 worker 数据库断言，
    `-race` 仅补充进程内竞争，不可替代本条**；因偶发失败原因未知，本条按固定批次执行：
    每批次恰好 5 次并记录全部结果，任一次失败则该批次不通过，不追加运行凑成功；
    修复或明确诊断后的重跑另记批次并保留原证据，**不允许用"重跑通过"掩盖失败**。
- [ ] T018 [US5] `coordinator.go` + `serve.go` 第三循环接线：三 serveLoop 并发 + 回归 + 退出 + 故障传播
  - 需求：FR-08/FR-12，research R1。验收场景：D3 退出部分。依赖：T006，T016。
  - 完成条件：`Coordinator` 唯一 Acquire 循环 + 唯一 Heartbeat 下并发跑三 loop，
    header/log 行为不变（002/003 回归测试全绿）；应用 serve 路径启停 deposit 循环的集成断言
    （沿用 `internal/app/serve_integration_test.go` 模式；仅断言启停与行为不变，不扩展其职责）；
    任一失权/心跳丢失即全停重取（专项集成测试绿）；
    终止信号下未提交零残留；失权/配置拒绝/不可重试错误按类别退出或停写。
    本任务是唯一的既有文件行为触碰点，范围不得扩大；授权转换走特权 SQL 路径，不经过 serve 循环，不改变本条接线。
- [ ] T019 [US5] 可观察端到端：进度/滞后/状态/暂停缺口查询 + 脱敏审计（指标断言落入 `internal/metrics/metrics_test.go`（按序扩展）；端到端落入 `internal/indexer/deposit_integration_test.go`）
  - 需求：FR-15。验收场景：SC-09（D1–D9 状态断言共用）、D11 转换审计。依赖：T003，T014，T025。
  - 完成条件：运行/等待/重试/暂停/结构停止各态下进度、滞后、重试、暂停或缺口原因可查
    （指标 + 诊断 SQL）；`transition_total` 计数与 history 版本链审计 SQL 可查（含 seq 排序、request_id 查询与观察↔版本关联查询）；
    暂停实例审计 SQL 可查（释放/合并事件、实例全生命周期）；
    解除结果判定查询可查（条件解除影响 0 行后按实例查审计：命中返原结果，未命中按陈旧处理）；
    全量日志凭据零出现；金额仅十进制；无无限制原始数据转储（有审计测试）。

**Checkpoint**: 暂停可解释、恢复可验收、并发由数据库保证、运维可观测

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: 全量验证、门禁复核、实现准入

- [ ] T020 全量验证：`make build` + `make lint` + `go test ./...` + `go test -tags integration ./...` 全绿
  - 依赖：T005–T019，T024–T028。完成条件：四命令一次全绿；T010/T015/T016/T017/T027/T028 批次证据齐全；
    未解释失败持续为未解决事项（后续成功批次不自动关闭；无新证据停跑并报告待诊断）；
    D11 全部断言证据齐全（承接任务见 T015/T018/T025/T027/T028，不在本条复述）；失败按回归处理，不弱化断言；
    否定性断言：观察表外无余额写路径、deposit 包外无 Pending 之外状态推进（grep 断言，承接 FR-13/14）。
- [x] T021 实现前规划复核（静态一致性，不依赖任何实现与测试结果）
  - 依赖：无（仅依赖 spec/plan/research/data-model/contracts/quickstart/tasks 文档）。
    显式不依赖 T020——本任务在实现开始前即可关闭；关闭是本地实现准入条件之一（另一条件为 T000-L）。
  - 完成条件：逐项核对 FR-01–16、I1–I6、SC-01–09、D1–D11（11 个验证场景）在 tasks 的覆盖（见下表）；
    确认无循环依赖、无超出规格的设计、无需上游变更的假设落空；输出实现准入结论（通过 / 附条件通过）。
    本任务只放行"开始本地实现"，不等同实现验收，更不等同生产就绪。
  - 状态（2026-09-13）：CLOSED，通过（只放行开始本地实现）。证据：FR-01~16/I1~I6/SC-01~09/D1~D11/R1~R11 静态全覆盖、无环依赖（T021零依赖、T022→T020、T023→T022）、无超规格设计、无需上游变更假设落空；003 T020b内联CLOSED记录存在（003 tasks.md:215-222）按任务原文充分、缺acceptance.md不构成本轮门禁；后续测试未执行属后续任务未倒置；T000-P保持open。
- [ ] T022 本地范围验收（实现后；名称固定，不得与生产就绪混称）
  - 依赖：T020（需全部本地测试证据）。完成条件：凭 Anvil/DB 播种测试证据逐项验收 D1–D11（11 个验证场景）与 SC；
    输出本地验收结论 + 未解决项（T000-P open、上游 003 E1 open）；发现语义冲突先澄清，不私改规格。
    通过仅表示"004 本地范围验收通过"，"生产接入就绪"需 T000-P 关闭后另行判定。
- [ ] T023 提交 PR + 远端 CI 通过 + 合并（仓库流程门禁，由实现仓库执行，不在规划步骤执行）
  - 依赖：T022（需本地范围验收结论）。完成条件：PR 发起 + 远端 CI 全绿 + 合并；
    本任务只记录仓库执行动作，不复述本地验收，不将远端 CI 结果混入 T022；
    生产就绪仍需 T000-P 关闭后另行判定。

---

## Dependencies & Execution Order

- **Setup (T000-L/T000-P)**: T000-L 关闭后 T001–T020、T022、T024–T028 本地工作可推进；T000-P open 约束生产接入与部署，不阻塞本地实现。
- **Foundational (T001–T004，T024)**: 全部完成 → 阻塞所有 US。T001/T002/T003/T004/T024 相互独立（不同文件；T024 仅依赖 T000-L），可并行。
- **US1 (T005–T007)**: 依赖 Foundational；T006 依赖 T005；T007 依赖 T006。
- **US2 (T008)**: 依赖 T006；与 US3–US5 可按文件分工并行（默认按优先级顺序执行）。
- **US3 (T009–T010)**: 依赖 T006；T009/T010 同测试文件，按序执行。
- **US4 (T011–T013，T025–T028)**: 依赖 T006（T011 另依赖 T002）；T013 依赖 T005；
  T025 依赖 T024（选型结论）+ T006，依赖 T025 的 T026/T027/T028 在 T024 关闭前不得假定任一载体；
  T026 依赖 T025；T027 依赖 T025；T028 依赖 T025/T026/T012。
- **US5 (T014–T019)**: 依赖 T006；T015/T016 依赖 T014；T018 依赖 T006/T016；T019 依赖 T003/T014/T025。
- **Polish (T020–T023)**: T020 依赖所有实现任务（含 T024–T028）；T021 独立于实现（可先行关闭）；T022 依赖 T020；T023 依赖 T022。
- 依赖图无环：T021 零依赖；T022 仅 →T020；T023 仅 →T022；T024 仅 →T000-L；T025–T028 均不指向 Foundation/US1–US3；无反向边。

## 覆盖矩阵（复核用）

| FR/SC/不变量 | 任务 | 验收场景 |
|-------|------|----------|
| FR-01/02/03 | T004，T006，T007 | D1，D2 |
| FR-04/05 | T002，T004，T008，T026 | D1，D2，D5，D6，D7 |
| FR-06 | T002，T011，T024，T025，T027 | D5，D7，D11 |
| FR-07 | T005，T012，T013，T026，T028 | D2，D5，D6，D7，D11 |
| FR-08 | T006，T017，T018 | D1，D3，D9 |
| FR-09 | T001，T006，T009，T010，T016，T017，T025，T027 | D3，D4，D8，D11 |
| FR-10 | T006，T008，T009 | D2，D3 |
| FR-11 | T001，T005，T006，T014 | D1，D8 |
| FR-12 | T005，T014，T015，T016，T018，T025，T027 | D8，D11 |
| FR-13/14 | T021（实现前范围门禁）+ T022（实现后验收） | — |
| FR-15 | T003，T019，T025 | SC-09，D11 |
| FR-16 | T021 | — |
| I1 | T001，T006，T009，T026，T028 | D1，D3，D6，D7，D11 |
| I2 | T001，T006，T010，T025 | D1，D3，D4，D11 |
| I3 | T006，T012，T013，T026，T028 | D4，D6，D7，D11 |
| I4 | T005，T014 | D1，D8 |
| I5 | T002，T011 | D5 |
| I6 | T006，T012，T015，T027 | D6，D7，D8，D11 |
| SC-01–05 | T007–T010，T014 | D1–D4，D8 |
| SC-06 | T012，T014，T015，T027 | D6，D8，D11 |
| SC-07 | T011，T027，T028 | D5，D11 |
| SC-08 | T012，T028 | D6，D7，D11 |
| SC-09 | T019 | 全场景状态断言 |
| D11 授权转换套件 | T024（选型），T025–T028，T018（接线无影响），T019（转换审计） | D11 |
| 发布门禁（仓库流程） | T023 | — |

## Notes

- `[P]` 仅标记不同文件无依赖可并行；同文件（`internal/indexer/` 内）任务按顺序执行防冲突；
  同测试文件任务按序执行。新文件落点：`depositauth.go`（T024 选型 + T025 实现 + T026 回放计算，
  同文件按序）；`depositscanner.go`/`depositcommit.go`（T005/T006 已建，后续任务按序扩展）。
- 载体选型门禁：T025/T027/T028 显式依赖 T024，在 T024 关闭前不得假定 SQL 事务函数或受控脚本任一选项；
  T024 的符合性标准是 R11 全部语义条。
- 版本与幂等铁律：版本身份 =（chain_id, version_seq），内容哈希禁作版本身份；
  观察↔版本显式外键（回放保留原值），禁时间戳推导；消费捕获版本、提交时锁内验版本，
  失配放弃重读（回环亦然），禁打最新 seq 冒充；
  请求幂等按 request_id 先查 history：同 ID 同参返原结果（跨版本亦然），同 ID 异参拒绝，
  异 ID 独立校验；过期拒绝报告预期与当前版本。
- 暂停实例铁律：实例身份 =（chain_id, pause_id）且 pause_id 永不复用，同实例修订号单调递增；
  解除必须匹配实例 + 修订并同事务写审计行，否则影响 0 行；审计行携带原因快照，活动行删除后仍可解释；
  人工解除与授权处置遵守同一规则。
- 授权目标铁律：expected_pause 双列同时提供或同时为空，同时为空表示不授权处置任何已有暂停（非通配），
  不等于一律拒绝——证据在 scope 外可保留时提交身份／位置／history 且暂停原样保留（消费仍停），
  证据在 scope 内已证解决而无目标时拒绝并要求重新明确授权；
  提供目标时锁内必须匹配否则拒绝；无目标而必须处置已有暂停才能完成时拒绝并要求重新明确授权；
  处置结果仍按锁内重验证出（派生），不得超出授权范围。
- 解除结果铁律：条件解除影响 0 行后查审计定性——命中原释放记录即返回"该目标已解除"及原审计结果
  （原结果、原操作者、原时间），不重写、不触碰新暂停、不归功本次调用者；
  无独立解除请求身份时只返回该事实，不声称识别为同一次请求；
  未命中即陈旧或目标不存在；人工路径无 request_id，"只返事实"即其全部语义。
- 真库测试一律 `//go:build integration` + testcontainers，复用 002/003 helper；
  Anvil 部署测试 token 产 Transfer；DB 播种只布置前置条件，不 mock 被测逻辑。
- T017 必须双真 worker + 有界重复证据；`-race` 为附加项。
- T000-P 未关闭前，任何"生产可用/生产就绪"表述不得出现；上游 003 E1 open 连带约束 004 生产就绪；
  残余风险如实声明；禁用计数判定不等于完整性已解决（003 侧约束，004 不重复判定）。
- 验收命名固定："004 本地范围验收" vs "生产接入就绪"，不得混称。
