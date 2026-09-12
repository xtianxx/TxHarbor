# Tasks: 002-chain-indexer

**Input**: Design documents from `/specs/002-chain-indexer/` (spec.md + 5 clarifications, plan.md,
research.md, data-model.md, contracts/observability.md, quickstart.md)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ — all present.

**Tests**: 集成测试强制（Constitution X/XI：真 PG + Anvil/可控假 RPC，`//go:build integration`，
并发项 `-race`）；单元测试覆盖纯逻辑（配置校验、错误分类）。所有测试任务保持未完成状态，
实现阶段先写断言并确认失败后再实现。

**图例**: `[P]` = 可并行（不同文件、无依赖）；`[USn]` = 所属用户故事；每项注明目标文件、
关联需求/验收、依赖、完成标准。

---

## Phase 1: 基础构件（无相互依赖，可并行）

- [ ] T001 [P] 迁移 `migrations/000002_chain_indexer.sql`：`chain_blocks`（PK/FK 目标 UNIQUE/
  身份 UNIQUE/格式 CHECK）/`indexer_checkpoint`（三元组外键/`start_height`）/`indexer_lease`/
  `indexer_pause`（kind CHECK）——关联 FR-04/06/12、data-model.md；完成标准：`migrate up`
  从空库一次成功，`migrate status` 无 pending，非法行（坏哈希/孤 checkpoint）被约束拒绝。
- [X] T002 [P] 配置扩展 `internal/config/config.go`（+ `.env.example`）：`TXHARBOR_START_HEIGHT`
  （必需，`>=0`）、`INDEX_RPC_TIMEOUT`（默认 5s）、`INDEX_POLL_INTERVAL`（默认 1s）、
  `INDEX_RETRY_INITIAL/MAX`（默认 200ms/30s）；`Summary` 中 RPC URL 走 `logx.Redact`
  ——关联 FR-02/15、R6；完成标准：缺失/非法逐项拒绝启动并点名变量，日志无凭据明文
  （单元测试，验收 US5-AC3/场景 13 脱敏抽查前置）。
- [X] T003 [P] 取块与错误分类 `internal/eth/client.go`：`HeaderByNumber` 封装、
  `KindNotFound`（等待极）/`KindRateLimited`（可重试）、429 与 `ethereum.NotFound` 映射
  ——关联 FR-09/10/11、R1/R4；完成标准：httptest 覆盖超时/429/空结果/非法响应/chain_id 错误
  的分类断言（单元测试）。

---

## Phase 2: 协调与扫描核心（依赖 Phase 1）

- [ ] T004 lease 与协调协议 `internal/indexer/lease.go`（+ 集成测试）：CAS 获取、心跳续约
  （锁优先短事务：取锁→复核 owner→延租约）、过期接管（token+1），失权（续约 0 行/复核失败）
  立即停写 ——关联 FR-08/14、R2；依赖 T001；完成标准：双实例下恰一持有者，
  杀持有者连接后租约过期被接管（生产默认 TTL 15s/心跳 5s，DB 时间口径；测试经内部参数
  注入短值）。接管测试必须验证真实过期条件：以带超时上限的条件等待确认 lease owner
  变更（超时仅作测试失败），禁用裸 sleep 猜测完成时间。
- [ ] T005 扫描主循环 `internal/indexer/scanner.go`（+ 集成测试）：启动 `CheckChainID` 门禁、
  S 高于链头零写入等待、首块边界（免父校验/断言高度 S/同事务建 checkpoint）、精确守卫推进
  （`height=$n-1 AND block_hash=$parent`）、三态核验、持久化暂停、不确定提交恢复
  （重连 SELECT 核对）、安全退出（未提交零残留）——关联 FR-01/02/03/05/06/10/11/12/13、
  澄清 #1–#5；依赖 T001–T004；完成标准：全部写事务走统一写事务协议
  （确保行→FOR UPDATE→独立语句裁决→写→提交），事务内零 RPC。
- [ ] T006 接线与可观测 `internal/app/serve.go`、`internal/metrics/metrics.go`：
  scanner 协程启停（沿用 runCtx/ShutdownTimeout）、4 个新指标、结构化日志字段、
  readyz 语义冻结 ——关联 FR-15、contracts/observability.md；依赖 T005；
  完成标准：`/metrics` 含新指标且命名与契约一致，暂停不翻转 readyz。

**Checkpoint**: 基础就绪——用户故事测试可并行展开。

---

## Phase 3: US1/US2 连续同步与故障（P1）

- [ ] T007 [US1] 集成测试：首次扫描顺序/重启恢复/起始高度变更拒绝/创世扫描
  （验收场景 1、5；FR-02/03/05）——依赖 T005；
  完成标准：空库首行即 S、无跳块；SIGTERM 重启后从 N+1 继续，SC-01 通过；
  已有 checkpoint 时改 START_HEIGHT 重启，断言拒绝启动且 blocks/checkpoint/`start_height`
  全不变；S=0 创世变体：首块 parent 全零保存、checkpoint 建行、S+1 父子衔接正常。
- [ ] T008 [US2] 集成测试：S 高于链头/追头等待/RPC 中断/DB 写入失败（场景 2、3、6、7）
  ——依赖 T005；完成标准：等待期零写入零 checkpoint、故障期 checkpoint 推进 0、
  恢复后自动继续，SC-03/04/08 通过。
- [ ] T009 [US2] 集成测试：不确定提交 + 安全退出（场景 8、13）——依赖 T005；
  完成标准：COMMIT 回包掐断后重启以 DB 为准继续；临界 SIGTERM 无部分进度且限时退出。

## Phase 4: US3 竞争与重复（P1）

- [ ] T010 [US3] 集成测试：重复扫描幂等（场景 4）——依赖 T005；完成标准：重投 3 次有效记录恒 1。
- [ ] T011 [US3] 集成测试：双实例竞争 60s（场景 9）——依赖 T004、T005；完成标准：同高度
  canonical 记录恒 1，checkpoint 单调连续，SC-05 通过。

## Phase 5: US4 分叉暂停（P2）

- [ ] T012 [US4] 集成测试：父哈希断裂/停机变哈希/chain_id 错误（场景 10、11、12）——依赖 T005；
  完成标准：三类暂停行 kind 正确、历史零改写、chain_id 不符零写入；重启后暂停仍有效，
  解除后重核验（SC-06/07 通过）。

## Phase 6: US5 可观察（P2）

- [ ] T013 [US5] 契约测试：指标/日志/暂停行查询（contracts/observability.md）——依赖 T006；
  完成标准：四态 `indexer_state` 可达、暂停诊断 SQL 返回完整字段、全量日志凭据零出现。

## Phase 7: T1–T4 确定性并发测试（真 PG，同步点构造，禁 sleep 碰运气）

- [ ] T014 [P] T1 暂停-推进互斥：测试事务持协调锁写 pause 未提交时，另一会话推进阻塞于锁
  ——以数据库锁等待状态（`pg_locks`/`pg_stat_activity`）或等价可靠同步机制确认阻塞中，
  所有等待设超时上限，失败输出锁等待诊断，禁用裸 sleep；提交后推进继续但被裁决拒绝，
  checkpoint 逐字段不变 ——依赖 T004、T005。
- [ ] T015 [P] T2 旧 token 拒绝：接管 bump 后，旧 token 写事务（含暂停事务）在步骤 4 被拒，
  零写入 ——依赖 T004、T005。
- [ ] T016 [P] T3 双首写互斥：起跑屏障并发首块协议，终态一行 checkpoint + 一行区块 S，
  败者转 S+1（同哈希）或走暂停（异哈希）——依赖 T004、T005。
- [ ] T017 [P] T4 暂停后零推进：pause 提交前后 checkpoint 精确比对，多实例 N 次尝试后
  完全一致 ——依赖 T004、T005、T012。
  注：T014–T017 仅可并行编写，运行需 `-race` 且共享测试库时串行执行。

## Phase 8: 收尾门禁

- [ ] T018 全量校验：`gofmt`/`go vet`（双 tag）、`make test`、`make test-integration`、
  quickstart 13 场景×硬断言逐项勾选、spec→plan→tasks 回溯无静默偏离 ——依赖 T007–T017；
  完成标准：SC-01–SC-10 可验证通过（测试本身保持未完成勾选，由实现阶段执行）。

---

## 验收覆盖矩阵

| 场景 | 任务 | 场景 | 任务 |
|------|------|------|------|
| 1 首次扫描 | T007 | 8 不确定提交 | T009 |
| 2 S 高于链头 | T008 | 9 双实例竞争 | T011 |
| 3 追上链头 | T008 | 10 父哈希不一致 | T012 |
| 4 重复扫描 | T010 | 11 checkpoint 变化 | T012 |
| 5 重启恢复 | T007 | 12 chain_id 错误 | T012 |
| 6 RPC 中断恢复 | T008 | 13 安全退出 | T009 |
| 7 DB 写入失败 | T008 | T1–T4 并发 | T014–T017 |

## 依赖与执行顺序

- Phase 1（T001–T003）无依赖，可并行启动。
- T004 依赖 T001；T005 依赖 T001–T004；T006 依赖 T005。
- T007–T013 依赖 T005（T011/T013 另需 T004/T006，见标注）；T014–T017 依赖 T004+T005（+T012 见标注）。
- T018 依赖全部。仅标注 [P] 的任务可并行；测试任务运行阶段按库串行。
