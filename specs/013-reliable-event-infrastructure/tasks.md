# Tasks: 013 Reliable Event Infrastructure（可靠事件基础设施）

**Input**: Design documents from `specs/013-reliable-event-infrastructure/` — [spec.md](spec.md)（FR-01–FR-28、SC-01–SC-12、五态×七类故障矩阵、PD-1–PD-4 已裁决）、[plan.md](plan.md)（D1–D10、FR/SC 映射、Complexity Tracking、Blockers=无、Deferred）、[research.md](research.md)（R1–R18）、[data-model.md](data-model.md)（Tables 1–7、§3–§10）、contracts/（[events.md](contracts/events.md)、[outbox-publisher.md](contracts/outbox-publisher.md)、[consumer.md](contracts/consumer.md)、[redis.md](contracts/redis.md)、[capacity.md](contracts/capacity.md)）、[quickstart.md](quickstart.md)（Q0–Q11）、[adr.md](adr.md)（ADR-013-01/02、§3 基准设计、§5 Debezium 排除）、[verification.md](verification.md)（§1 指标、§2 V-*、§3 分层、§4 PR 触发、§5 边界纪律）、checklists/requirements.md（16/16）；`.specify/memory/constitution.md` v1.1.0。

**Prerequisites**: 分支 `013-reliable-event-infrastructure`、HEAD `1fd79e1`、工作区干净、`main 99545bf` 无漂移；`.specify/scripts/bash/setup-tasks.sh --json` 已运行并产出 FEATURE_DIR/TASKS_TEMPLATE；plan.md + spec.md 已确认存在；research/data-model/contracts/quickstart/ADR/verification 齐备。`before_tasks`/`after_tasks` 的 `speckit.git.commit` 为 optional 且工作区干净，按指令跳过；本步骤仅生成 tasks.md 并本地提交。

**Tests**: FR-28（2026-09-24 用户明确）要求分层测试并各层独立运行——Unit（无外部中间件）、Integration 按 PostgreSQL/Redis/Kafka 分组件、Contract（格式/版本/消费者兼容）、E2E（核心充提）、Fault Injection 与 Performance 独立运行且不阻塞普通 PR。因此测试任务是本 feature 的规格交付物（非可选 TDD 推断）；完整 Fault/Performance 的触发、预算与 CI 接入任务独立成组（B10/B11），不进普通 PR。**Debezium/CDC/Kubernetes 不是运行或测试依赖**（FR-28、ADR §5）。

**Organization**: Phase 1 Setup → Phase 2 Foundational（阻塞全部故事）→ Phase 3–9 每用户故事一阶段（spec 顺序 US1→US7）→ Phase 10 Polish。**文档阶段顺序按 spec 故事分组；真实执行顺序以批次 B1–B11 与依赖图为准**：US1/US7 属闭包故事（其验证任务进入条件为其他批次完成），故其执行批次分别为 B9/B10。所有任务本轮保持未勾选、未执行。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 仅当任务可并行（不同文件、且不依赖未完成任务）时标注
- **[Story]**: 仅用户故事阶段任务标注 `[US1]`–`[US7]`；Setup / Foundational / Polish 不带故事标签
- 每个任务给出确切文件路径、可核验完成条件、FR/SC 或设计依据（plan D / research R / 契约节）、适用测试层与证据
- 资金关键路径任务禁止「实现并测试」式概括：必须写明具体断言对象（0 重复意图/0 新 nonce/0 签名/0 广播/0 陈旧权威等）
- 所有实现任务本轮为 `- [ ]` 未勾选

## Inputs & constraints（INPUTS — 不得重开、不得弱化）

- **裁决边界（PD-1–PD-4，已裁决，MUST NOT 变更）**：PD-1 限流失效 → 新提款创建拒绝 + 明确可重试错误，存量资金流程继续，RPC 保持有界控制、无法维持则安全暂停续跑，不批准替代限流；PD-2 积压接近容量边界 → 停新保在途、预留恢复容量、链上已发生充值不得拒绝（无法安全持久化则从可靠进度暂停、恢复后补扫）、绝不静默丢弃；PD-3 Kafka 保持范围内、ADR 写明理由/PG-only 替代/成本并设计可比基准、调整范围另行提交用户决定；PD-4 人工重放由授权操作员执行、记录范围与操作审计、不要求第二人审批、受持久幂等与门禁约束、不得重执行链上付款或创建新提款意图、与自动重试区分。
- **保证表述（全局强制）**：投递 = **至少一次**；处理 = **幂等**；同一事件有效应用次数恰好一次（以消费者 PG 效果计）。**MUST NOT** 把 Kafka/发布器/消费者设置写成「跨系统恰好一次」（PG ↔ Kafka 无分布式事务；contracts/consumer.md §1）。
- **数值纪律**：容量 soft/hard/reserve、限流速率、退避/超时、追赶窗口、告警阈值一律待测/配置；plan/契约中的技术初始值必须标注「初始值，测量后校准」；**不得编造业务阈值**（FR-20/24/25、spec Assumptions）。
- **范围纪律**：不重定义 002–011/PB 语义；不引入 Debezium/Kubernetes/多链；不维护用户余额账本；T000-P 保持 OPEN，不宣称生产就绪；验证归属 orchestrator，本步骤不进入 analyze/implement。
- **前置复核（本轮已完成）**：分支/HEAD/工作区/main 无漂移已核验；013 目录文档齐备；tasks.md 此前不存在。

## Blocker review（设计矛盾复核）

**结论：无 Blocker。** 本轮对已批准规格与技术计划做了交叉复核：spec FR-04 矩阵 vs plan D8 映射、D1 身份/版本 vs contracts/events.md §2 与 data-model §3、D2/D3 保证表述 vs FR-08/13（无跨系统恰好一次）、D7/PD-2 vs contracts/capacity.md §2 不变量、PD-4 vs contracts/consumer.md §6、FR-28 vs verification.md §3–4——未发现需修改 spec 或推翻裁决的矛盾；plan.md「Blockers」亦为「无」。若实现批次发现任一已裁决边界不可满足，按 Blockers 节流程回报 orchestrator，不得以任务文本改变资金语义。残余（非阻塞）事项见「Deferred」节。

## Batch overview（B1–B11；独立可验证批次 + 进入/退出准则）

| 批次 | 交付链 | 任务范围（文档 ID） | 进入准则 | 退出准则（证据） | 本地提交节点（orchestrator 执行） |
|---|---|---|---|---|---|
| B1 环境与迁移基础 | 1, 8 | T001–T007 + T008–T009 | 设计文档批准；分支干净 | 配置 fail-closed 单测、依赖构建、compose profile 启停、Makefile 各层 target、迁移 up/down/up + 命名约束探针 | `feat(events): add event infrastructure scaffolding and 000015 schema` |
| B2 Outbox 核心与契约合流 | 1, 2 | T010–T016 | B1 退出 | Append 原子性/身份/版本 Integration 绿；Contract 信封/兼容矩阵绿；**合流检查**（迁移 T009 × 契约 T014 × Append T015 × import 边界 T016 同时绿）后方可进入 B3+ | `feat(events): add transactional outbox core and event contract` |
| B3 生产者集成与 cutover | 1, 2 | T026–T032 + T038–T040 | B2 合流通过 | 各域原子性探针（T038–T040）绿；010 attempt 级引用完整性取证（T032/T040：未知结果尝试的 `state_changed` 载荷 attempt 引用 100% 存在）；cutover/回退证据（T031）；5 类目录一致性（T032，8 类由 T058 收口） | `feat(events): emit outbox events from deposit and withdrawal transactions` |
| B4 发布器 | 3 | T033–T037 | B2/B3 | V-PUBLISHER（PG+Kafka）绿；崩溃点矩阵；blocked 可见可审计；表述检查（无跨系统恰好一次） | `feat(events): add outbox publisher with lease claim and crash recovery` |
| B5 消费者、隔离与重放 + 核心 E2E | 3, 4 | T041–T051 + T078–T079 | B4 退出 | V-IDEMPOTENCY/V-PROGRESS/V-RETRY-QUARANTINE 绿；PD-4 反例断言 0 新意图/nonce/签名/广播；E2E 充值/提现核心流绿 | `feat(events): add idempotent consumer with quarantine and audited replay` |
| B6 修订 | 2 | T052–T058 | B3（生产者）+ B5（消费者） | V-REVISION（生产者×2 + 消费者）绿；8/8 目录矩阵收口；修订 0 新付款 | `feat(events): emit and converge reorg revision events` |
| B7 Redis 缓存与限流 | 5 | T059–T068（消费 T017/T018） | B1/B2；可与 B3–B6 并行 | V-CACHE/V-RATELIMIT（组件+HTTP）绿；PD-1 拒绝新创建与存量继续分别验收；单独故障组件证据 | `feat(cache): add non-authoritative cache and fail-closed rate limiting` |
| B8 容量保护与追赶 | 6 | T069–T074 + T018 | B4 + B7（错误通道） | V-CAPACITY/V-CATCHUP 绿；停新保在途、补扫、在途完成、再故障 0 丢失/0 重复；`internal/faultdrill` 基建可构建（T073：`go build ./...`） | `feat(events): add outbox capacity guard and catch-up rescan` |
| B9 五态故障矩阵与最终验收 | 1, 7 | T019–T025 + T080 | B3–B8 全部合流；故障环境就绪（faultdrill 基建来自 B8 T073） | 矩阵一致率 100%、门禁绕过 0、五项 0 不变量、证据包含 FR-16 边界声明 | `test(fault): add five-state failure matrix drills` |
| B10 观测与基准 | 7 | T075–T077 + T081 | B9 | 指标/告警齐备且有来源标注；V-BENCH 报告 100% 产出（数值待测不编造）；账本证据含边界声明 | `perf(events): add PG-only baseline and full-stack benchmark` |
| B11 CI 接入与收口 | 8 + 全部 | T082–T088 | B10 | PR 必需检查映射、独立 Fault/Perf 工作流不阻塞 PR、耗时预算实测、分层/DeDo 审计、quickstart 证据索引、文档同步、覆盖审计 | `ci(events): add layered checks and closeout records` |

**跨批次成员说明**：T017/T018 为故障态与降级面载体，B7/B8 消费；T073 于 B8 创建 `internal/faultdrill` 基建（`doc.go`＋`harness.go` 骨架，保证 B8 完成后 `go build ./...` 通过）并执行追赶测试；T019 于 B9 扩展 harness 编排（B9 首任务，T020–T025/T080 依赖）；T078/T079 于 B5 执行（消费者就绪后），T080 于 B9 执行；T075–T077/T081 于 B10 执行。批次完成后由 orchestrator 执行批次验证并本地提交；**不推送/PR/合并/部署**。

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 环境、依赖、指标、编排、测试基建与子命令骨架；全部为互不相交文件，可并行。

- [x] T001 [P] 扩展 `internal/config/config.go` 并新建 `internal/config/config_013_test.go`：新增 013 fail-closed 配置——events（`TXHARBOR_EVENTS_ENABLED`、发布器/消费者批次·轮询·租约·退避）、Redis（地址/超时/缓存 TTL/epoch）、Kafka（brokers/topic/消费组前缀）、按接口类限流速率与突发、容量 `soft_limit/hard_limit/reserve/retention/max_shutdown_window/drain_target_window`；启用容量时启动校验 `0 < reserve < soft_limit < hard_limit`，缺失/非法组合拒绝启动，无「无界」缺省语义；阈值一律「初始值，测量后校准」或必填待测，不编造业务阈值。完成条件：表驱动单测覆盖合法/非法组合与拒绝启动；既有配置语义不变。（FR-01/18/20/24/27；plan Constraints；contracts/capacity.md §1；contracts/redis.md §3；层：Unit；证据：配置矩阵测试）
- [x] T002 [P] 扩展 `go.mod`/`go.sum`：加入并固定新依赖 `github.com/twmb/franz-go`（纯 Go、幂等 producer）、`github.com/redis/go-redis/v9`、`testcontainers-go/modules/redis` 与 Kafka 模块（或经评估的 Redpanda 轻量候选，R17）；逐一验证版本可用性并记录理由；**MUST NOT** 引入 Debezium/CDC、Kubernetes 或其它新基础设施。完成条件：`go build ./...` 与 `go vet ./...` 通过；依赖清单仅含本 feature 范围。（FR-03/28；plan Primary Dependencies；R17；adr.md §5；层：build/lint；证据：go.mod diff + 版本记录）
- [x] T003 [P] 新建 `internal/metrics/events.go` 与 `internal/metrics/events_test.go`：按 verification.md §1 注册 `outbox_pending_count`、`outbox_pending_oldest_age_seconds`、`outbox_publish_failures_total{error_class}`、`outbox_published_total`/`outbox_attempts_total`、`consumer_lag_seconds`/`consumer_lag_messages`、`consumer_applied_total`/`consumer_retry_total`/`consumer_quarantine_total`、`consumer_catchup_seconds`、`cache_hits_total`/`cache_misses_total`/`cache_fallback_total`/`cache_epoch_rotations_total`、`ratelimit_denied_total{class}`/`ratelimit_unavailable`/`ratelimit_recovery_total`、`rpc_budget_paused_total{class}`、`capacity_soft_breaches_total`/`capacity_refusals_total{op_class}`、`redis_available`/`kafka_available`、`events_identity_conflict_total{event_type}`、`outbox_blocked_count`；标签仅含接口类/事件族/错误分类，**MUST NOT** 含凭据。完成条件：注册表齐全且既有指标不改名。（FR-24；verification.md §1；D9；层：Unit；证据：注册表清单测试）
- [x] T004 [P] 扩展 `compose.yaml`：新增 `redis` 与 Kafka（KRaft 单节点、显式创建 `txharbor.events.v1`、关闭 auto-create）并置于 profile（如 `--profile events`）；默认 `docker compose up -d` 仍为 PG+Anvil（PG-only 基线可运行）；镜像 tag 先验证可用后固定并记录；健康检查与 127.0.0.1 绑定沿用现状风格。完成条件：profile 启停冒烟通过；默认基线不带动 Redis/Kafka。（D10；R17；quickstart §1；层：本地编排；证据：启停记录 + compose diff）
- [x] T005 [P] 扩展 `Makefile`：新增 `test-contract`（tag `contract`；Contract 层独立入口，无中间件、无 Docker）、`test-integration-redis`、`test-integration-kafka`（新 build tags，如 `integration_redis`/`integration_kafka`）、`test-e2e`（tag `e2e`）、`test-fault`（tag `fault`）、`test-perf`（tag `perf`）；各层可独立运行、Unit 不启动中间件；既有 `test`/`test-race`/`test-integration` 语义不变。完成条件：target 清单可被本地与 CI 独立调用（Contract 用例 T014/T051/T058 经 `make test-contract` 独立执行）；无默认串跑全部中间件。（FR-28；verification.md §3；plan Testing；层：构建/CI；证据：target 清单与冒烟记录）
- [x] T006 [P] 新建 `internal/testutil/redis.go` 与 `internal/testutil/kafka.go`（test-support，供带 tag 的测试复用）：Redis/Kafka testcontainers 启动/清理、broker 地址、显式 topic 创建（不允许 auto-create）、故障注入钩子（停容器/断连）与就绪等待；资源命名随机化防冲突。完成条件：骨架测试能起停两类容器并创建 topic；`make test`（无 tag）不受影响。（FR-28；R16/R17；verification.md §3；层：Integration 基建；证据：容器启停日志）
- [x] T007 [P] 新建 `internal/app/eventpublisher.go`、`internal/app/eventconsumer.go`、`internal/app/eventsadmin.go`（仅参数解析、usage、fail-closed 配置加载、空循环入口，无业务逻辑），并在 `cmd/txharbor/main.go` 增加 `event-publisher`/`event-consumer`/`events-admin` 分发与 usage；**`cmd/txharbor/main.go` 为 013 单写者——本任务后其它批次只扩展 `internal/app` 对应文件，不再编辑它**。完成条件：`go build ./...`；三个子命令 `--help` 可用且配置缺失时 fail-closed；既有子命令不受影响。（plan Summary/Project Structure；quickstart §1；层：Unit/CLI 冒烟；证据：构建 + help 输出）

**Checkpoint**: 环境可运行、依赖/指标/编排/测试基建/子命令骨架就绪；无故事行为。

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 迁移、Outbox 核心、事件目录/身份/版本契约——所有用户故事的阻塞前置。

**⚠️ CRITICAL**: 本阶段（含合流检查）完成前，任何用户故事批次不得进入。

- [x] T008 新建 `migrations/000015_event_infrastructure.sql`（goose；纯增量，编号紧随 `000014`；**本文件 013 唯一 owner 任务，合流检查为 T009**）：按 data-model §2 建 7 表——`outbox_events`（`identity_kind` CHECK、部分 UNIQUE `outbox_events_log_identity_uniq`（`chain_id, block_hash, tx_hash, log_index` WHERE `identity_kind='evm_log'`）与 `outbox_events_object_identity_uniq`（`aggregate_type, aggregate_id, aggregate_version` WHERE `business_object`）、CHECK `outbox_events_log_identity_shape`/`state_consistency`/`revision_shape`、pending 队列/容量/保留/来源审计索引）、`consumer_progress`、`consumer_inbox`、`consumer_versions`、`consumer_quarantine`（含 `(consumer_name,event_id) WHERE status='open'` 部分唯一）、`event_ops_audit`（`operation_id UNIQUE`、`op_kind` CHECK）、`event_system_state`（单行 CHECK + `cutover_at` 种子 + `catalog_version=1`）；命名前缀 `outbox_`/`consumer_`/`event_` 支持 23505/23514 精确分类；**不改动** `000001`–`000014` 任何对象。完成条件：DDL 可应用；列/约束/索引与 data-model 逐项一致。（FR-07/08/09/13/14/15/20；data-model §2/§8；D1；层：Integration-PG；证据：schema 快照）
- [x] T009 迁移验证 + **合流检查（迁移）**：在 scratch DB 执行 `up`→`down`→`up`；对每个命名约束做负向探针（重复 `evm_log`/`business_object` 身份、`published` 无 `published_at`、`blocked` 无 `last_error_class`、修订缺 `recovery_version`、负 offset、重复开放隔离、重复 `operation_id`、`event_system_state.id≠1`）→ 23505/23514 且仅按 `ConstraintName` 分类；对 `000001`–`000014` 做 additive-only diff（无上游对象变化）。**合流门禁**：T009 × T014（契约）× T015（Append 原子性）× T016（import 边界）同时绿后，B3+ 方可进入。**回退**：down 仅在受控窗口使用；「仅停发射而业务继续」记录为违规，不得作为回退方案。完成条件：up/down/up 干净、探针命中预期约束名。（data-model §2/§7/§8/§10；R14；FR-27；层：Integration-PG；证据：迁移日志 + 约束探针 + 回退步骤记录）
- [x] T010 [P] 新建 `internal/events/catalog.go` 与 `internal/events/catalog_test.go`：定义目录 v1 全部 8 类（`deposit.observation.created`/`status_changed`/`reinstated`、`deposit.confirmation.confirmed`、`deposit.revision.applied`、`withdrawal.request.received`、`withdrawal.execution.state_changed`/`revised`）、`identity_kind ∈ {evm_log,business_object}`、信封字段、`event_id` 的 UUIDv5 确定性派生（`evm_log|chain|block_hash|tx_hash|log_index`；`business_object|type|id|version`）、payload 规范化 JSON + sha256、`schema_version` 从 1 起与兼容常量；载荷 **MUST NOT** 含密钥/凭据/原始签名字节。完成条件：8 类与派生规则单测通过（同事实同身份、载荷字节稳定）。（FR-09/10/12；contracts/events.md §1–3；data-model §3/§9；R2/R3/R4；层：Unit + Contract（T014）；证据：单测）
- [x] T011 新建 `internal/events/append.go` 与 `internal/events/append_test.go`：`Append(ctx, tx, Event)` 只接受调用方已在进行的 PG 事务；同事务内计算 `aggregate_version = coalesce(max,0)+1`（依赖调用方对源行的锁，UNIQUE 兜底；冲突不得靠重试掩盖）；写入用 `INSERT ... ON CONFLICT (<自然键>) DO UPDATE ... WHERE payload_hash = EXCLUDED.payload_hash`——同身份同内容 = 幂等 no-op（返回既有行、不告警）；同身份异内容 = 拒绝事务 + `events_identity_conflict_total` 告警；构造上不存在「提交后补写事件」的调用形态；**不得**使用触发器/CDC。完成条件：四类分支单测通过；与 T014/T015 构成 Outbox 核心合流门禁。（FR-07/09；D1；data-model §2 Table1/§3/§10；R1/R2/R3；层：Unit + Integration（T015）；证据：冲突/幂等分支断言）
- [x] T012 [P] 新建 `internal/events/outbox.go` 与 `internal/events/outbox_test.go`：发布状态机读取与守卫（`pending`/`published`/`blocked` 合法转换；`blocked` 仅经审计解阻回 `pending`；非法转换 0 行即拒绝）；发布器领取查询（`pending AND next_attempt_at<=now() ORDER BY id FOR UPDATE SKIP LOCKED`）、带 `claim_owner` 条件的 ack 标记、退避调度；容量观测查询（`pending` 计数/最老等待按事件族，PG 部分索引，**Redis 不参与**）；保留期裁剪查询（仅 `published`、记录审计水位，**绝不**删除/覆盖 `pending|blocked`）。完成条件：状态守卫与查询构造单测通过；SQL 内无网络调用。（FR-08/20/21；contracts/outbox-publisher.md §0/§4/§6；data-model §6/§8；R6/R13；层：Unit；证据：状态转换/查询断言）
- [x] T013 [P] 新建 `internal/events/errors.go` 与 `internal/events/errors_test.go`：封闭错误分类——outbox 身份冲突/契约错误（不可重试）、发布瞬时类（broker 不可达/超时/限流/leader 切换）与永久类（序列化/契约校验/无效 topic）、消费隔离原因 `retry_exhausted|non_retryable|version_gap|schema_unsupported|identity_mismatch`；23505/23514 仅按 `ConstraintName` 分类；未知错误默认不可重试且可观测。完成条件：分类矩阵单测逐项通过。（data-model §10；contracts/outbox-publisher.md §4；contracts/consumer.md §4/§7；FR-08/14；层：Unit；证据：分类矩阵测试）
- [x] T014 新建 `internal/events/events_contract_test.go`（Contract 层，`contract` tag；经 `make test-contract` 独立执行）+ **合流检查（事件契约）**：校验信封必填字段与两类身份自然键、`schema_version` 兼容规则（新增可选字段同版本、消费者忽略未知字段、破坏性变更必须新版本 + 兼容窗口）、`event_type+schema_version` 解析路由、载荷最小化与禁含字段扫描、目录 v1 基础声明；**合流门禁**：与 T009/T015/T016 同绿后方可进入 B3+；后续任何目录/信封语义改动必须走新版本（FR-12），不得原地变更。完成条件：兼容矩阵用例全部通过；目录与 contracts/events.md §3 逐项一致。（FR-09/10/12；contracts/events.md §1–§4；R4；层：Contract；证据：契约用例输出）
- [x] T015 新建 `internal/events/append_integration_test.go`（Integration-PG，真实迁移 `000015`）：对每个集成点形态构造「事务提交/事务回滚」两路径（提交 → 业务行与 outbox 行同现且同对象版本连续；回滚 → 两者皆无、0 残留）；同身份同内容重复 `Append` → no-op 行数不变；同身份异内容 → 事务拒绝 + 冲突计数；并发同对象版本竞争 → UNIQUE 兜底、0 静默覆盖。完成条件：全部断言通过。（FR-07/09；quickstart Q1；V-ATOMICITY；SC-03；层：Integration-PG；证据：行数对照 + 冲突分类）
- [x] T016 [P] 新建 `internal/events/boundary_test.go`：静态/运行时断言 `internal/events` 不 import 上游 writer 包与 RPC/dial/signer 包；无触发器/CDC 路径；事务内无网络发布调用；载荷/日志扫描不含密钥、凭据、原始签名字节。**合流门禁成员**：与 T009/T014/T015 同绿后方可进入 B3+（import 边界为资金安全前提）。完成条件：断言通过，违反即失败。（FR-02/03；R18；constitution VIII；plan Structure Decision；层：Unit/结构扫描；证据：扫描结果）

**Checkpoint**: 迁移/契约/Append/import 边界合流通过——用户故事批次可进入。

---

## Phase 3: User Story 1 — 基础设施故障下的资金业务连续性与门禁不被绕过（Priority: P1）

**Goal**: 五态故障下七类操作按矩阵继续/降级/拒绝；PG 权威与既有门禁不被绕过；恢复后事件补齐、进度恢复、0 重复付款、0 权威丢失。

**Independent Test**: 在本地环境注入仅 Redis 故障、仅 Kafka 故障、双故障（关 Redis+Kafka，保 PG+本地链），按矩阵逐项验证七类操作与安全前提；恢复后验证事件补齐、进度恢复、0 重复提款意图与链上付款、0 权威状态丢失。（执行批次 B9；其载体 T017/T018 于 B7/B8 就绪。）

- [ ] T017 [P] [US1] 新建 `internal/health/dependencies.go` 与 `internal/health/dependencies_test.go`：Redis/Kafka 可用性探针（停/起可判定），暴露为非权威健康信号（`redis_available`/`kafka_available`，复用 T003 指标）；**MUST NOT** 被任何资金门禁、授权、幂等、对账判定读取（静态断言）。完成条件：探针单测 + 无门禁引用断言；serve 冒烟可用。（FR-04/24；D9；contracts/redis.md §1；层：Unit + Integration 冒烟；证据：探针状态输出）
- [ ] T018 [P] [US1] 扩展 `internal/app/serve.go`（单写者链 T018→T063）：非关键功能降级开关（由依赖健康驱动、可配置）与查询/事件投递状态降级标注（积压未清时表达为降级信息，不伪装实时）；关键路径（充值/确认/修订/在途提款/已接受提款）不受非关键降级影响；降级状态可观测。完成条件：serve 集成断言降级不阻塞关键 handler、标注字段存在、0 门禁读取健康信号。（FR-04；D8；spec 矩阵「查询/非关键功能」行；层：Integration-PG；证据：降级开关测试）
- [ ] T019 [US1] 扩展 `internal/faultdrill/harness.go`（单写者链 T073→T019；**B9 首任务**）：在 B8 骨架（T073）上补齐五态（正常/仅 Redis/仅 Kafka/双故障/恢复追赶）故障注入与场景编排、逐态指标快照采集、证据落盘（日志/导出/环境规格 + commit）；场景可重复、失败不吞错；**不进入生产构建**。完成条件：`make test-fault` 可运行五态骨架场景；T020–T025/T080 依赖本任务（同包 harness 与故障环境）。
- [ ] T020 [US1] 新建 `internal/faultdrill/matrix_contract_test.go`（`fault` tag；依赖 T019 harness）：结构断言——资金决策路径不 import `internal/cache`/`internal/ratelimit`；门禁不读 Redis/Kafka/健康信号；事件/投递状态从不被解读为授权或许可；重复投递路径不存在新提款意图/nonce/签名/广播调用。完成条件：全部断言通过。（FR-02/05/06；contracts/consumer.md §9；层：Fault/结构；证据：扫描报告）
- [ ] T021 [US1] 新建 `internal/faultdrill/state_normal_redis_test.go`（`fault` tag；进入条件 B7 完成 + T019 harness）：正常态与仅 Redis 故障态逐项验证矩阵七类操作——充值处理继续（缓存旁路、RPC 有界降级）、确认与重组恢复继续（读 PG）、提款创建仅 Redis 故障时 100% 拒绝且错误明确可重试（0 无限制放行）、已有提款执行继续（资格/绑定读 PG）、查询降级直读 PG 且 0 陈旧财务权威、事件订阅继续、非关键功能降级；安全前提 0 门禁绕过。（FR-04/18/26；PD-1；quickstart Q6/Q8；V-DRILL；SC-01/08；层：Fault；证据：逐项断言 + 指标快照）
- [ ] T022 [US1] 新建 `internal/faultdrill/state_kafka_test.go`（`fault` tag；进入条件 B4/B5 + T019 harness）：仅 Kafka 故障态——业务按 PG 继续且事件入 Outbox 积压（0 丢失、0 覆盖）、提款创建接收语义不变（接近容量边界按 PD-2 先拒可控新写入）、已有提款执行在途继续、事件订阅停止投递但积压有界可观测、非关键功能降级；「订阅端未收到」不被读作业务未发生。（FR-04/20/21；PD-2；quickstart Q7/Q8；层：Fault；证据：积压指标 + 断言）
- [ ] T023 [US1] 新建 `internal/faultdrill/state_dual_test.go`（`fault` tag；进入条件 B7/B8 + T019 harness）：双故障（关 Redis+Kafka，保 PG+本地链）——七类操作按矩阵；提款创建拒绝（PD-1）与 Outbox 积压（PD-2）分别验收；恢复后事件补齐、进度恢复；0 重复提款意图、0 重复链上付款、0 孤儿永久入账、0 权威状态丢失、0 门禁绕过、0 静默丢失。完成条件：断言通过；证据含模拟消费者入账幂等与 FR-16 边界声明。（FR-04/05/06/26；SC-01/02/12；quickstart Q8；层：Fault；证据：演练报告）
- [ ] T024 [US1] 新建 `internal/faultdrill/state_catchup_refailure_test.go`（`fault` tag；进入条件 B8 + T019 harness）：恢复追赶态七类行为（继续 + 事件补齐；积压状态可见、不伪装实时）；追赶期间再次故障 → 安全重暂停、0 丢失、0 重复财务效果、进度可续；缓存/限流恢复为惰性/受控重建，不瞬间无界。完成条件：断言通过；追赶时间可观测（数值待测，不编造）。（FR-22/23；SC-10；quickstart Q9；V-CATCHUP；层：Fault；证据：时间线与指标）
- [ ] T025 [US1] 新建 `internal/faultdrill/matrix_invariants_test.go`（`fault` tag；依赖 T019 harness）：汇总断言五态 × 七类矩阵一致率 100%（与 spec 矩阵逐格比对表）、门禁绕过 0、重复提款意图 0、重复链上付款 0、孤儿永久入账 0、权威状态丢失 0、静默事件丢失 0；产出证据包（演练日志、指标导出、审计摘录、环境规格 + commit）与 FR-16 边界声明。完成条件：证据包完整、与 verification.md §2 对齐。（FR-05/06/26；SC-01/02/12；D8；层：Fault；证据：`docs/evidence/013/` 证据包）

**Checkpoint**: 用户故事 1 的机制载体由 B3–B8 提供，矩阵演练于 B9 独立执行并通过。

---

## Phase 4: User Story 2 — 业务状态与事件原子提交、至少一次发布、事件身份与版本顺序（Priority: P1）🎯 MVP

**Goal**: 每次业务状态变化与其事件同事务提交；发布至少一次且不丢失；事件携带稳定身份与版本；重复/乱序可被幂等吸收；同身份异内容不静默覆盖。

**Independent Test**: 注入提交前后崩溃、发布前后崩溃、重复发布、乱序投递与同身份异内容，观察原子性、不丢失、幂等收敛与冲突告警。（Q1/Q2；V-ATOMICITY/V-PUBLISHER。）

- [x] T026 [P] [US2] 在 `internal/indexer/depositcommit.go` 的观察插入事务（`commitDepositUnit`）内接入 `Append`，发射 `deposit.observation.created`（`evm_log` 身份三元组；对象版本 1；载荷：观察 id、链身份、来源日志三元组、初始状态）；同事务提交、无提交后补写；004 语义只读消费。完成条件：原子性由 T038 验证；代码路径审查无事务外发射。（FR-07/09；data-model §4；004 只读；层：Integration-PG（T038）；证据：T038）
- [x] T027 [P] [US2] 在 `internal/indexer/confirmcommit.go` 的确认提交事务内接入 `Append`，发射 `deposit.confirmation.confirmed`（对象版本 +1；载荷：`policy_version`、确认高度/哈希、链身份）；确认语义保持 005 门禁，事件不表达上游入账。完成条件：原子性由 T038 验证。（FR-07；data-model §4；005 只读；层：Integration-PG（T038）；证据：T038）
- [x] T028 [P] [US2] 在 `internal/indexer/reorgcommit.go`（恢复提交事务）与 `internal/indexer/depositscanner.go` 相关转换提交内接入 `Append`，发射 `deposit.observation.status_changed`（含 Orphaned 失效表达；载荷：`from_state`/`to_state`/原因/链身份）；本任务只处理状态转换；`reinstated`/`revision.applied` 属 US4（T052；单写者链 T028→T052）。完成条件：原子性由 T038 验证；Orphaned 表达为失效而非新事实。（FR-07/11；data-model §4；006 只读；层：Integration-PG（T038）；证据：T038）
- [x] T029 [P] [US2] 在 `internal/withdrawal/intake.go` 的 `SubmitWithdrawal` 事务内接入 `Append`，发射 `withdrawal.request.received`（对象版本 +1；载荷：request_id、caller、状态 Accepted）；**不改变** 007 receive-only 语义、认证/授权/幂等契约；事件送达不属于接收语义。完成条件：原子性由 T039 验证；既有 007 测试保持绿。（FR-05/07；data-model §4；007 只读；层：Integration-PG（T039）；证据：T039）
- [x] T030 [P] [US2] 在 `internal/execution/intent.go`（`TransitionIntent` 事务）与 `internal/execution/advance.go`（`converge` 事务）内接入 `Append`，发射 `withdrawal.execution.state_changed`（011 关键转换含终态；载荷：`from_state`/`to_state`、intent_id、attempt 引用、冻结/暂停上下文）；事件不是发送许可，重复投递不得触发新发送。完成条件：原子性由 T040 验证；011 门禁语义不变。（FR-05/07；data-model §4；011 只读；层：Integration-PG（T040）；证据：T040）
- [x] T031 [US2] 扩展 `internal/app/eventsadmin.go`（单写者链 T031→T045）：`bootstrap-export` 只读快照导出（供下游初始化历史状态；**不写业务表、不发射伪造历史事件**）与 cutover 操作路径（`cutover_at` 由迁移种子；上线后目录转换开始发射；首见版本即消费者基线）；配套受控回退程序：停发布器/消费者 → 盘点并导出 `pending|blocked`（或先排空）→ down 迁移 → 移除接线，明确「仅停发射而业务继续」为违规。完成条件：导出前后业务表计数不变、事件表无 cutover 前补造行；回退步骤记录与 T009 衔接。（FR-07；data-model §7；R14；quickstart Q0；层：Integration-PG；证据：导出对照 + 回退记录）
- [x] T032 [US2] 新建 `internal/events/catalog_conformance_integration_test.go`：对已实现的 5 类非修订事件（T026–T030）逐类断言 `event_type`/`schema_version=1`/身份类/版本单调/链身份字段/载荷最小化；同对象版本从 1 连续；同事实重复写入身份唯一。修订 3 类由 T058 收口（US4）。完成条件：5 类逐项通过并输出一致性矩阵；010 attempt 级引用完整性取证——未知结果尝试上 `withdrawal.execution.state_changed` 载荷的 attempt 引用 100% 存在；若目录 v1 无法表达已批准状态事实，按 Blockers 节流程回报 orchestrator 契约缺口（不新增业务事件、不自行扩目录）。（FR-07/09/10；contracts/events.md §3；层：Integration-PG；证据：一致性矩阵）
- [x] T033 [US2] 新建 `internal/events/publisher.go` 与 `internal/events/publisher_test.go`：领取（单事务 `FOR UPDATE SKIP LOCKED` → 写 claim lease → 提交）、**发布在事务外**（`ProduceSync`；`acks=all`、幂等 producer、`max.in.flight≤5`、delivery/request timeout 有界）、ack 后独立事务以 `id=ANY($ids) AND claim_owner=$owner` 标记 `published`；租约过期可重领（重复发布可接受）；瞬时失败释放领取 + 指数退避（初始值 base 1s/cap 60s/±20% 抖动，测量后校准）；永久类置 `blocked` + 告警（绝不丢弃）；**准确表述：至少一次投递 + 幂等处理，不宣称跨系统恰好一次**。完成条件：状态机单测通过；发布无持锁网络调用。（FR-08/21；contracts/outbox-publisher.md §1–§5；R6；层：Unit + Integration（T036/T037）；证据：状态机断言）
- [x] T034 [US2] 扩展 `internal/app/eventpublisher.go`：`event-publisher` 长驻循环——有界批量/并发/轮询、优雅停机（停止领取、完成在途、释放未确认）、多实例天然分片（SKIP LOCKED + 租约；不依赖 Redis 锁）、周期执行对账审计入口（T035）；发布速率有界，不无界冲击 broker/PG。完成条件：单实例与双实例冒烟；停机无「未提交行被发布」；配置 fail-closed。（FR-08/21；D2；contracts/outbox-publisher.md §5；层：Integration-PG/Kafka（T036/T037）；证据：双实例运行记录）
- [x] T035 [US2] 新建 `internal/events/audit.go` 与 `internal/events/audit_integration_test.go`：周期按 `source_kind/source_id/source_version` 比对来源表最近水位与 outbox 行；缺口 → 指标 + 告警（检测兜底），**不做静默补写**，修复走 `events-admin` 审计路径；只读、不持锁跨外部调用；不触碰 `pending|blocked`。完成条件：注入缺口可检出并告警、无自动写回。（plan D1 对账审计；data-model §4；FR-07/24；层：Integration-PG；证据：缺口检出记录）
- [x] T036 [P] [US2] 新建 `internal/events/publisher_integration_test.go`（Integration-PG）：领取互斥（同一行同一时刻仅一个 owner）；租约过期重领；ack 标记被 `claim_owner` 守卫（错 owner 0 行）；崩溃点矩阵（领取前/后、ack 前后）恢复后 0 丢失、重复可发生；`blocked` 可视可审计；未提交行永不发布。完成条件：全部断言通过。（FR-08/21；SC-03；quickstart Q2；V-PUBLISHER；层：Integration-PG；证据：崩溃点矩阵输出）
- [x] T037 [P] [US2] 新建 `internal/events/publisher_kafka_integration_test.go`（Integration-Kafka）：真实 broker 下投递确认、重复发布可发生且可被吸收（消费者侧由 T046/T050 验证）、多实例并发领取、停机→恢复排空（批量/并发有界）、topic 显式创建（无 auto-create）；测试与注释禁止任何「跨系统恰好一次」表述。完成条件：通过；排空窗口数值待测、不编造。（FR-08/21；SC-03；quickstart Q2；层：Integration-Kafka；证据：投递/重复记录）
- [x] T038 [P] [US2] 新建 `internal/indexer/outbox_atomicity_integration_test.go`（Integration-PG）：覆盖 T026–T028 三个集成点提交/回滚两路径——提交必有事件行且同对象版本连续；回滚两者皆无；重复事实同身份同内容 no-op；同身份异内容拒绝 + 告警；`deposit.observation.created` 身份为三元组、高度不作身份。完成条件：逐集成点断言通过。（FR-07/09；SC-03；V-ATOMICITY；quickstart Q1；层：Integration-PG；证据：探针输出）
- [x] T039 [P] [US2] 新建 `internal/withdrawal/outbox_intake_atomicity_integration_test.go`（Integration-PG）：覆盖 T029 集成点提交/回滚；`caller_id + idempotency_key` 重放不产生第二请求也不产生第二事件（同身份 no-op）；Accepted 语义与既有门禁不变；提交失败 0 事件可见。完成条件：断言通过。（FR-05/07；SC-03；V-ATOMICITY；层：Integration-PG；证据：探针输出）
- [x] T040 [P] [US2] 新建 `internal/execution/outbox_execution_atomicity_integration_test.go`（Integration-PG）：覆盖 T030 集成点提交/回滚；关键转换与事件同事务；重复投递不触发任何新 intent/nonce/签名/广播（调用计数断言）；非法转换不产生事件。完成条件：断言通过；010 未知结果尝试上 `state_changed` 载荷 attempt 引用 100% 存在（attempt 级引用完整性取证）；目录无法表达已批准状态事实时按 Blockers 节流程回报，不新增业务事件。（FR-05/07；SC-03；V-ATOMICITY；层：Integration-PG；证据：探针输出）

**Checkpoint**: MVP 核心——同事务 Outbox、5 类事件发射、至少一次发布、身份/版本收敛；US2 可独立验证（Q1/Q2）。

---

## Phase 5: User Story 3 — 消费者持久幂等、有界重试、隔离、重放与进度恢复（Priority: P1）

**Goal**: 消费者在重复投递、失败重试、重启、rebalance 与重放后收敛到同一持久状态；重试有界；毒事件隔离且可重放；进度持久可观察；未知版本 fail-closed。

**Independent Test**: 重复投递同一事件、注入可重试/不可重试失败、kill 进程后重启、从隔离区与指定位置重放，观察有效应用次数恒为 1、隔离可审计、进度续传与 lag 可观测。（Q3/Q4。）

- [x] T041 [US3] 新建 `internal/events/consumer.go` 与 `internal/events/consumer_test.go`：T4 单事务——`consumer_inbox` UNIQUE 去重（0 行 = 已应用 → 跳过）→ `consumer_versions` 版本守卫（`>` 应用、`=` 跳过、`>max+1` 缺口按配置等待窗口初始 10s 后仍缺 → 隔离 `version_gap` + 告警 + 分区继续，**不静默跳过、不无限阻塞**）→ 效果写入 → `consumer_progress.next_offset` 推进；崩溃语义按 data-model §5 T4（提交前后重投递均收敛「效果一次」）；链身份校验失败隔离 `identity_mismatch`；未知版本 fail-closed 隔离 `schema_unsupported`；可重试类有界退避（初始 base 500ms/cap 30s/上限 8 次，测量后校准），不可重试/超限 → 隔离；进度/去重**不得**存 Redis 单点。完成条件：守卫/缺口/重试/隔离分支单测通过。（FR-10/12/13/14/15；contracts/consumer.md §1–§5/§7；R7；层：Unit + Integration（T046–T048）；证据：分支断言）
- [x] T042 [P] [US3] 新建 `internal/events/quarantine.go` 与 `internal/events/quarantine_test.go`：持久隔离区（完整事件快照 + `failure_class` + 原因 + 尝试数 + 定位 + 状态 `open|replayed|superseded`）；部分唯一防重复开放项；隔离不删除事件、不阻塞分区（跳过并继续）；修复后可重放且重放幂等。完成条件：隔离/去重/状态转换单测通过。（FR-12/14；data-model Table5/§8；contracts/consumer.md §4；层：Unit + Integration（T048）；证据：状态断言）
- [x] T043 [US3] 新建 `internal/events/refconsumer.go`：参考消费者（验收证据用）——以 inbox + 版本表证明「同一事件有效应用次数 = 1、模拟账本效果不重复」；独立 `consumer_name`、状态可重建；代码与文档声明**非生产账本、非权威、不持用户余额**，证据边界限定于本项目事件身份/版本/投递语义 + 消费者幂等契约，不含外部真实账本保证。完成条件：骨架可消费；真实 Kafka 集成由 T050 验证。（FR-13/16；contracts/consumer.md §8；SC-12；层：Unit + Integration；证据：边界声明 + 效果计数）
- [x] T044 [US3] 扩展 `internal/app/eventconsumer.go`：`event-consumer` 长驻循环——逐分区有序处理；启动/再均衡 position = max(PG `next_offset`, Kafka committed offset 回退)；效果提交后异步提交 Kafka offset；重启/再均衡从 PG 进度续传（不依赖内存）；lag 双口径与追赶状态可观测；优雅停机。完成条件：重启续传与 rebalance 冒烟；offset 提交失败重投递由 inbox 吸收。（FR-15/22；contracts/consumer.md §5；R8；层：Integration-Kafka（T050）；证据：续传/再均衡记录）
- [x] T045 [US3] 扩展 `internal/app/eventsadmin.go`（单写者链 T031→T045）：`replay --consumer ... --scope <event-ids|aggregate|time-range|offset-range> --reason ... --operator ...`、`unblock`（发布器 `blocked`）、`retention_prune`——写 `event_ops_audit`（`operation_id` UNIQUE 去重；操作者/范围/理由/结果；PD-4：本地阶段授权操作员执行、**不要求第二人审批**）；重放必经 inbox/版本守卫，**MUST NOT** 绕过持久幂等/版本守卫/既有门禁；**MUST NOT** 重新执行链上付款或创建新提款意图；重放与自动重试在审计与指标上分列。完成条件：CLI 冒烟 + 审计行完整；解阻/裁剪均留审计。（FR-14/15；PD-4；contracts/consumer.md §6；R9；层：Integration-PG（T048/T049 联动）；证据：审计表摘录）
- [x] T046 [P] [US3] 新建 `internal/events/consumer_integration_test.go`（Integration-PG）：同一事件重复投递（含跨重启与 rebalance 模拟）→ inbox 中 `(consumer_name,event_id)` 唯一、有效应用次数恒为 1；乱序——旧版本晚到 0 覆盖新版本；缺口 → 有界等待后隔离 `version_gap` 且不阻塞分区；0 静默跳过。完成条件：计数断言通过。（FR-10/13；SC-04；quickstart Q3；V-IDEMPOTENCY；层：Integration-PG；证据：计数断言）
- [x] T047 [P] [US3] 新建 `internal/events/consumer_progress_integration_test.go`（Integration-PG）：offset 提交失败/丢失后重启 → 100% 从 PG `consumer_progress` 续传；重投递 0 重复财务效果；lag 与进度可读。完成条件：前后对照通过。（FR-15；SC-06；quickstart Q3/Q9；V-PROGRESS；层：Integration-PG；证据：重启前后进度对照）
- [x] T048 [P] [US3] 新建 `internal/events/quarantine_integration_test.go`（Integration-PG）：可重试失败按有界退避重试；超限/不可重试/未知版本/链身份不匹配 → 持久隔离且审计告警 100%，不静默丢弃、不无限阻塞其它事件；修复后重放幂等（效果计数不增）；无界重试次数 0。完成条件：断言通过。（FR-12/14；SC-05；quickstart Q4；V-RETRY-QUARANTINE；层：Integration-PG；证据：隔离/重放记录）
- [x] T049 [P] [US3] 新建 `internal/events/replay_boundary_integration_test.go`（Integration-PG）：重放/解阻**反例验证**——重放 `withdrawal.request.received`、`withdrawal.execution.state_changed`、修订类事件后，断言 0 新建提款意图、0 新 nonce 分配、0 新签名、0 新广播（对 007/008/009/010/011 相关表与调用计数取证）；审计行含操作者/范围/理由/结果且 `operation_id` 重复提交返回既有结果；自动重试与人工重放指标分列。完成条件：计数不变断言全绿。（FR-05/14；PD-4；SC-02/05；quickstart Q4；层：Integration-PG；证据：计数不变 + 审计摘录）
- [x] T050 [P] [US3] 新建 `internal/events/consumer_kafka_integration_test.go`（Integration-Kafka）：真实 Kafka 下重复/乱序投递、消费者组 rebalance、lag 可观测；以参考消费者 PG 效果计「同一事件有效应用次数 = 1」；「消费者已处理」≠「具备发送资格」；测试与文档禁止「跨系统恰好一次」表述。完成条件：通过。（FR-13/15/22；SC-04/06；quickstart Q2/Q3；层：Integration-Kafka；证据：rebalance/重复运行记录）
- [x] T051 [P] [US3] 新建 `internal/events/consumer_contract_test.go`（Contract 层，`contract` tag；经 `make test-contract` 独立执行）：模拟上游消费者兼容矩阵——未知可选字段被忽略、同版本向后兼容可用；破坏性变更需新版本 + 兼容窗口；未知/不支持版本 fail-closed 隔离并告警；路由解析正确；边界声明覆盖 FR-16（不保证外部真实账本）。完成条件：兼容矩阵通过。（FR-12/16；contracts/events.md §4、consumer.md §7–8；SC-05/12；层：Contract；证据：兼容矩阵输出）

**Checkpoint**: 消费幂等/进度/隔离/重放独立可验证（Q3/Q4）；PD-4 反例断言成立。

---

## Phase 6: User Story 4 — 重组修订事件与链身份语义（Priority: P1）

**Goal**: 重组使旧分叉数据失效时，修订事件表达旧身份、新 canonical 状态/Orphaned 处置、原因与恢复版本；消费者收敛；修订不触发新付款；Orphaned 不再作为有效事实。

**Independent Test**: 构造浅重组影响 Pending/Confirmed 充值、重复/乱序修订投递、同一旧 `block_hash` 再次 canonical、链身份缺失事件，观察收敛与 0 新付款意图。（Q11。）

- [x] T052 [US4] 扩展 `internal/indexer/reorgcommit.go`（单写者链 T028→T052）：发射 `deposit.observation.reinstated`（006 FR-08 同旧 `block_hash` 再 canonical 的复活，复用原观察语义，与 `created` 新来源观察严格区分、不重复转换）与 `deposit.revision.applied`（`superseded_identity`（旧事件/旧区块身份）、新 canonical 状态或 Orphaned 处置、原因、`recovery_version`；写 `revises_event_id`）；事件幂等、非新充值/新确认。完成条件：由 T055 验证；不在本文件追加其它语义。（FR-11；contracts/events.md §5；data-model §3.4/§4；006 只读；层：Integration-PG（T055）；证据：T055）
- [x] T053 [P] [US4] 扩展 `internal/execution/revision.go`（修订事实事务）接入 `Append`，发射 `withdrawal.execution.revised`（`superseded_identity`、修订后状态、原因、`recovery_version`）；修订**不得**触发新意图/nonce/签名/广播；不重建意图、不补偿付款。完成条件：由 T056 验证；011 修订语义只读。（FR-11；contracts/events.md §3/§5；data-model §4；层：Integration-PG（T056）；证据：T056）
- [x] T054 [US4] 扩展 `internal/events/consumer.go`（单写者链 T041→T054）：修订语义——`reinstated` 与 `created` 不混淆；修订按版本守卫幂等收敛，重复/乱序 0 覆盖；收敛后不再把 Orphaned 作为有效 canonical 事实；修订不被解读为新充值/新确认/新付款指令，不触发新意图/nonce/签名/广播；修订字段校验（`revises_event_id` + `recovery_version` 必填；缺失/链身份不匹配 → 隔离）。完成条件：由 T057 验证；单测覆盖字段校验分支。（FR-11/12；contracts/events.md §5/§6；SC-07；层：Unit + Integration（T057）；证据：T057）
- [x] T055 [P] [US4] 新建 `internal/indexer/revision_events_integration_test.go`（Integration-PG）：浅重组影响 Pending/Confirmed 充值——修订事件携带旧身份/新状态或 Orphaned/原因/恢复版本；`revises_event_id` 指向旧事件；同旧 `block_hash` 再 canonical 时 `reinstated` 与新观察区分；与业务转换同事务原子；重复/乱序修订投递幂等。完成条件：通过。（FR-11；SC-07；quickstart Q11；V-REVISION；层：Integration-PG；证据：修订行对照）
- [x] T056 [P] [US4] 新建 `internal/execution/execution_revision_events_integration_test.go`（Integration-PG）：链上事实修订发射 `withdrawal.execution.revised` 且修订不触发新发送；重复/乱序不重复转换；修订后的后续发送仍须重验全部门禁（不存在「修订即许可」）。完成条件：发送计数不变。（FR-05/11；SC-07；quickstart Q11；层：Integration-PG；证据：计数断言）
- [x] T057 [P] [US4] 新建 `internal/events/revision_consumer_integration_test.go`（Integration-PG）：消费者应用修订后收敛到修订后状态；重复/乱序修订幂等率 100%；0 次把 Orphaned 当有效 canonical；0 新付款意图/发送；修订先于原事件到达时不误用（版本守卫 + 隔离策略）。完成条件：收敛计数通过。（FR-11；SC-07；quickstart Q11；层：Integration-PG；证据：收敛计数）
- [x] T058 [P] [US4] 新建 `internal/events/revision_contract_test.go`（Contract 层，`contract` tag；经 `make test-contract` 独立执行）：修订事件载荷必填字段（`superseded_identity`、新 canonical 或 Orphaned、原因、`recovery_version`、`revises_event_id`）逐项断言；完成目录 v1 全部 8 类声明/发射一致性收口（与 T032 合并输出 8/8 矩阵）；修订类型语义不得原地变更（FR-12）。完成条件：8/8 矩阵产出。（FR-11/12；contracts/events.md §3/§5；层：Contract；证据：8/8 目录矩阵）

**Checkpoint**: US4 独立可验证（Q11）；修订不改变任何付款语义。

---

## Phase 7: User Story 5 — 缓存失效查询行为与限流失效处置（Priority: P2）

**Goal**: 权威状态变化后查询不返回陈旧财务权威；缓存故障/miss 直读权威；限流失效时新提款创建拒绝并返回明确可重试错误，存量资金流程继续（PD-1）；RPC 保持有界。

**Independent Test**: 权威状态变化后立即查询、注入缓存故障与限流失效，观察陈旧读取 0、无限制放行 0、存量按原门禁继续、RPC 有界且分类/完整性校验不被跳过。（Q5/Q6。）

- [ ] T059 [P] [US5] 新建 `internal/cache/cache.go` 与 `internal/cache/cache_test.go`：cache-aside 客户端——键 `txharbor:<epoch>:<family>:<id>`、值 `(value, source_version, cached_at)`、TTL 兜底（初始 30s 展示类，测量后校准）、新鲜度标注 `fresh|possibly_stale`；Redis 不可用/超时/不可信 → 直读 PG，回源有界（singleflight + 并发信号量 + 超时），**MUST NOT** 无界并发压垮 PG；epoch 轮换使旧值不可达；决策路径永不读缓存。完成条件：键/新鲜度/回源有界/epoch 单测通过。（FR-02/17；contracts/redis.md §2；R10；层：Unit + Integration-Redis（T065/T068）；证据：T065）
- [ ] T060 [US5] 新建 `internal/cache/invalidator.go` 与 `internal/cache/invalidator_test.go`：权威转换的 outbox 事件驱动失效（复用消费者运行时；事件即失效信号，**不新增双写**）——删除受影响 family 键范围；TTL 兜底；恢复后惰性重建/受控预热、梯度恢复，**MUST NOT** 瞬间放开为无界；**MUST NOT** 提供已失效陈旧值。完成条件：失效范围与恢复梯度单测通过。（FR-17/23；contracts/redis.md §2；R10；层：Unit + Integration-Redis（T068）；证据：T068）
- [ ] T061 [P] [US5] 新建 `internal/ratelimit/limiter.go` 与 `internal/ratelimit/limiter_test.go`：Redis 原子脚本（令牌桶/滑动窗口）按接口类限流（新提款创建、一般写、查询、操作员、RPC 预算）；速率/突发待测（测量后校准）；**限流不参与认证/授权/幂等判定**（结构断言）；Redis 连接失败/超时/脚本错误/结果不可信 → 「限流不可用」状态（可用性指标 + 降级状态暴露）。完成条件：脚本行为与不可用判定单测通过。（FR-02/18；contracts/redis.md §3；R11；层：Unit + Integration-Redis（T066）；证据：T066）
- [ ] T062 [US5] 新建 `internal/ratelimit/policy.go` 与 `internal/ratelimit/policy_test.go`（PD-1 落地，不可变更）：限流不可用时——新提款创建 `POST /withdrawals` **拒绝**并返回明确可重试错误（429/503 + `Retry-After`，分类沿用 007 taxonomy）；充值观察、确认、重组恢复、已由 PG 接受的提款继续遵守原有授权/幂等/状态/暂停/nonce/广播/对账门禁（不因 Redis 故障放宽或收紧）；查询可回源 PG；**MUST NOT** 批准/引入资金写入的替代限流机制；非关键功能可降级、不阻塞关键路径；恢复平滑（梯度放开 + 预热），**MUST NOT** 瞬间全放开。完成条件：策略矩阵单测逐项通过（含存量流程继续的 0 门禁绕过断言）。（FR-18；PD-1；contracts/redis.md §3；quickstart Q6；层：Unit + Integration（T066/T067）；证据：策略矩阵）
- [ ] T063 [US5] 扩展 `internal/app/serve.go`（单写者链 T018→T063）：以中间件接入缓存与按接口类限流（`POST /withdrawals` 走 fail-closed 拒绝路径；查询走缓存旁路/降级标注）；暴露限流不可用/降级状态与拒绝计数；关键路径与既有门禁顺序不变。完成条件：HTTP 集成断言 429/503 + `Retry-After`、0 无限制放行、存量 handler 不受影响。（FR-18/23；PD-1；contracts/redis.md §3；层：Integration（T067）；证据：T067）
- [ ] T064 [US5] 扩展 `internal/eth/client.go` 并新建 `internal/ratelimit/rpcbudget.go`（`internal/eth/client.go` 为 013 单写者，仅本任务编辑）：出站 RPC 既有每进程有界控制（超时/重试上限/并发上限/错误分类）为基线、不依赖 Redis；分布式 RPC 预算为叠加治理；Redis 故障时可维持基线有界的调用类以降级并发继续，仅靠分布式预算才能有界的调用类**安全暂停**并恢复后续跑（不新建付款意图、不重执行链上动作）；**MUST NOT** 借故障跳过错误分类、链身份校验、完整性检查。完成条件：`rpc_budget_paused_total{class}` 可观测；分类/完整性断言通过。（FR-19；PD-1；contracts/redis.md §4；R12；层：Unit + Integration（T067）；证据：T067）
- [ ] T065 [P] [US5] 新建 `internal/cache/cache_integration_test.go`（Integration-Redis）：建立缓存 → 权威状态变化 → 立即查询受影响键范围 → 0 陈旧财务权威；关/清 Redis → 直读 PG 且回源并发有界（信号量/单飞生效）；恢复 → 0 已失效值（epoch/失效生效）；新鲜度标注正确、不可确认时明确标注。完成条件：通过。（FR-17/23；SC-08；quickstart Q5；V-CACHE；层：Integration-Redis；证据：陈旧计数 0 + 回源上限记录）
- [ ] T066 [P] [US5] 新建 `internal/ratelimit/ratelimit_integration_test.go`（Integration-Redis）：停 Redis（或使脚本失败）→ 新提款创建 100% 拒绝且明确可重试、0 无限制放行；**分别**验证「拒绝新创建」与「存量资金流程继续」两组行为互不冲突；恢复后梯度放开（无瞬间无界）；限流不参与授权判定。完成条件：通过。（FR-18；SC-08；quickstart Q6；V-RATELIMIT；层：Integration-Redis；证据：策略矩阵执行记录）
- [ ] T067 [P] [US5] 新建 `internal/app/ratelimit_failure_integration_test.go`（E2E）：真实 `serve` 路由下 POST /withdrawals 返回可重试错误（`Retry-After`）、存量充值/确认/已接受提款继续、查询回源可用、RPC 暂停类可观测；0 门禁绕过（认证/授权/幂等/暂停/版本门禁计数断言）。完成条件：通过。（FR-18/19；PD-1；SC-08；quickstart Q6；层：E2E；证据：HTTP 断言 + 门禁计数）
- [ ] T068 [P] [US5] 新建 `internal/cache/invalidator_integration_test.go`（Integration-Redis）：权威转换事件驱动失效在真实 Redis 下删除受影响键；TTL 兜底；Redis 重启/清空 → epoch 轮换、旧键不可达、恢复后 0 陈旧读取；重建与降级状态可观测。完成条件：通过。（FR-17/23；SC-08；quickstart Q5；V-CACHE；层：Integration-Redis；证据：epoch/失效记录）

**Checkpoint**: US5 独立可验证（Q5/Q6）；PD-1 两条验收线（拒绝新创建 / 存量继续）分别成立。

---

## Phase 8: User Story 6 — 事件通道停机积压、容量保护、发布器恢复与消费者追赶（Priority: P2）

**Goal**: Kafka 长时间停机期间已提交事件保留在 Outbox 且积压有界可观测；恢复后发布器从持久状态排空、消费者从持久进度追赶；追赶期间再故障安全重暂停。

**Independent Test**: 停机期间观察积压与最老等待指标；恢复后观察排空、追赶与再故障；验证 0 丢失、0 重复财务效果、进度可续。（Q7/Q9。）

- [ ] T069 [P] [US6] 新建 `internal/events/capacity.go` 与 `internal/events/capacity_test.go`：容量观测（`outbox_pending_count`/`outbox_pending_oldest_age_seconds` 按事件族，PG 部分索引，**Redis 不参与**）；软/硬边界门禁在**接纳新可控制工作之前**判定；`soft` 起拒绝可控新资金写入（与 T062 同款可重试错误通道）；`hard` 时：不拒绝链上已发生事实；持久化仍安全时不可拒绝事实继续入 Outbox（`pending` 可超 `hard`），无法安全持久化时从可靠进度暂停、恢复后补扫；**准确表述**：`hard_limit` 为准入闸＋暂停触发器，非物理容量上限（已接纳写入永不因容量拒绝；不得暗示物理容量不会耗尽）；`pending|blocked` 红线（不静默丢弃、不覆盖未发布、不删除）；配置 fail-closed 公式校验（`0 < reserve < soft_limit < hard_limit`；阈值按 capacity.md §5 方法测量）；接纳后事件必然可写（I-CAP）。完成条件：门禁判定与不变量单测通过；hard_limit 语义表述检查通过（准入闸＋暂停触发器，非物理容量上限）。（FR-20；PD-2；contracts/capacity.md §1–§3；data-model §6；R13；层：Unit + Integration（T072）；证据：T072）
- [ ] T070 [US6] 扩展 `internal/withdrawal/intake.go`（单写者链 T029→T070）：可控新资金写入接入容量软门禁——`soft ≤ pending < hard` 时拒绝（明确可重试错误，与 T062 同通道）+ `capacity_refusals_total{op_class}`；已由 PG 接受的请求与重放不受影响；**不得**让容量判定依赖 Redis；**不得**因容量原因放宽任何既有门禁。完成条件：软边界拒绝 100%、存量继续、0 门禁绕过（T072）。（FR-20；PD-2；contracts/capacity.md §3；层：Integration-PG（T072）；证据：T072）
- [ ] T071 [P] [US6] 新建 `internal/indexer/capacitypause.go`（不改上游门禁语义）：容量硬边界下若无法安全持久化——按 003/004 可靠进度暂停链上处理并记录可恢复证据，容量恢复后**补扫**（不跳过观察、不拒绝链上已发生事实）；在途提款按既有暂停/对账协议收尾（结果未知先对账），**不新建付款意图**；暂停/补扫状态可观测。完成条件：单测 + 场景记录；补扫连续性断言（T072/T073）。（FR-20；PD-2；contracts/capacity.md §3；层：Integration-PG（T072）/Fault（T073）；证据：暂停与补扫记录）
- [ ] T072 [P] [US6] 新建 `internal/events/capacity_integration_test.go`（Integration-PG；先以小配置阈值注入）：停机累积至 `pending ≥ soft` → 可控新写入 100% 拒绝且可重试、链上观察/确认/修订继续或按可靠进度暂停（不得拒绝事实）、在途提款完成且事件齐备、`pending` 无「业务提交无事件」缺口、0 静默丢弃/0 覆盖；恢复排空；`pending_count`/`oldest_age` 可观测率 100%。完成条件：通过。（FR-20；SC-09；quickstart Q7；V-CAPACITY；层：Integration-PG；证据：边界行为记录 + 行数对照）
- [ ] T073 [P] [US6] 新建 `internal/faultdrill/doc.go`（无 tag，保证包可构建）、`internal/faultdrill/harness.go`（`//go:build fault`，test-support：故障注入原语（停/恢复 Redis、Kafka；双停）、场景编排、指标快照采集、证据落盘（日志/导出/环境规格 + commit）骨架；单写者链 T073→T019）与 `internal/faultdrill/catchup_test.go`（`fault` tag）：恢复排空与消费者追赶；追赶期间再次停 Kafka/Redis → 安全重暂停，0 丢失、0 重复财务效果、进度可续；追赶时间可观测（数值待测，不编造）；发布器恢复排空有界、消费者 lag 下降可观测。完成条件：`go build ./...` 通过（B8 退出前提）；`make test-fault` 可运行追赶骨架场景并通过。（FR-21/22；SC-09/10；quickstart Q9；V-CATCHUP；层：Fault；证据：时间线与计数）
- [ ] T074 [P] [US6] 新建 `internal/events/publisher_catchup_integration_test.go`（Integration-Kafka）：Kafka 恢复后发布器从持久 `pending` 排空（批量/并发/退避有界，不无界冲击 broker/PG）；已 ack 项保持 `published`、未确认项回退避；再次故障 0 丢失；排空过程 `outbox_pending_count`/`oldest_age` 可观测。完成条件：通过。（FR-21；SC-09；quickstart Q2/Q9；层：Integration-Kafka；证据：排空曲线记录）

**Checkpoint**: US6 独立可验证（Q7/Q9）；PD-2 停新保在途、补扫、在途完成三者不矛盾。

---

## Phase 9: User Story 7 — 故障演练、观测与对照基准（Priority: P2）

**Goal**: 双故障演练验证关键资金业务与恢复补齐；观测指标与 PG-only vs 全栈对照基准可产出；无实测数值标待测；「不重复入账」以模拟消费者验证并写明边界。

**Independent Test**: 在本地确定性环境执行双故障演练并留证；产出观测指标与对照报告；检查验证边界声明与数值标注。（Q8/Q10；V-DRILL/V-BENCH。）

- [ ] T075 [P] [US7] 新建 `internal/metrics/alerts.go` 与 `internal/metrics/alerts_test.go`（config.go 单写者链 T001→T075 仅新增告警配置键）：按 verification.md §1 接线告警——≥ soft 持续超 `drain_target_window` 告警、到达 hard 立即 P1、永久阻塞与身份冲突即告警、隔离新增即告警；阈值全部来自配置且有来源标注（测量/裁决），**MUST NOT** 硬编码或编造；结构化日志含事件/链/业务身份与尝试次数、**MUST NOT** 含密钥/凭据。完成条件：告警条件单测 + 脱敏扫描通过。（FR-24；verification.md §1；D9；层：Unit；证据：告警清单 + 来源标注）
- [ ] T076 [P] [US7] 新建 `internal/perf/doc.go`（无 tag）与 `internal/perf/harness.go`（`//go:build perf`，test-support）：同主机/同 PG 规格/同 Anvil/同数据集与初始状态/同负载生成器（速率阶梯与突发固定）/同故障时间线；路径 A（PG-only：013 接线关闭，仅 PG+Anvil）与路径 B（全栈）；采集 API/RPC p95/p99、吞吐、错误率、资源、Outbox 最老等待与待发量、发布失败、消费积压与追赶时间；记录环境规格与 commit；**不进入生产构建**。完成条件：harness 可运行骨架场景。（FR-25；adr.md §3；verification.md §4；PD-3；层：Performance；证据：环境规格记录）
- [ ] T077 [US7] 新建 `internal/perf/bench_test.go`（`perf` tag）与 `docs/evidence/013/benchmark_report_template.md`：执行 A/B 同负载同故障对照并产出报告（中位数/分位数/方差、故障期降级对比、追赶时间、资源占用、结论与置信限制）；未测数值 0 次表述为「已达标」，一律标「待测/待裁决」；Kafka 价值结论只在此报告产出后成立；若结果支持调整范围，MUST 另行提交用户决定（PD-3，不自动删减）。完成条件：报告 100% 产出且字段齐全、绑定 commit 与环境规格。（FR-25；SC-11；adr.md §3/§4；层：Performance；证据：`docs/evidence/013/` 报告）
- [x] T078 [P] [US7] 新建 `internal/app/e2e_deposit_test.go`（`e2e` tag，全栈 + Anvil；执行批次 B5）：核心充值流 `链上交易 → 索引 → 观测 → 确认 → 事件发射/投递/消费` 全链通过；与 PG-only 基线同核心语义；事件与业务状态一致（同事务）；无 mock 替代验收证据。完成条件：全链通过。（FR-26/28；constitution X/XI；verification.md §3；SC-03；层：E2E；证据：全链运行记录）
- [x] T079 [P] [US7] 新建 `internal/app/e2e_withdrawal_test.go`（`e2e` tag，全栈 + Anvil；执行批次 B5）：核心提现流 `API 请求 → 持久化接收 → … → 执行 → 事件` 通过；重复请求不产生第二请求/事件；`withdrawal.request.received` 不改变 Accepted 语义。完成条件：全链通过。（FR-05/26；verification.md §3；层：E2E；证据：全链运行记录）
- [ ] T080 [US7] 新建 `internal/faultdrill/drill_test.go`（`fault` tag；执行批次 B9；依赖 T019 harness）：双故障演练主场景——正常 → 仅 Redis → 仅 Kafka → 双故障（关 Redis+Kafka，保 PG+本地链）→ 恢复追赶；七类操作逐项核对 + 恢复后事件补齐/进度恢复/0 重复提款意图/0 重复链上付款/0 孤儿永久入账/0 权威状态丢失；证据包覆盖矩阵、模拟消费者入账幂等证据与 FR-16 边界声明；门禁绕过 0。完成条件：通过。（FR-04/05/06/26；SC-01/02/12；quickstart Q8；V-DRILL；层：Fault；证据：演练报告 + 审计摘录）
- [ ] T081 [P] [US7] 新建 `internal/faultdrill/ledger_evidence_test.go`（`fault` tag）：「不重复入账」证据——重复投递下模拟上游消费者账本恰好一次入账；证据明确声明只覆盖本项目事件身份/版本/投递语义 + 消费者幂等契约 + 参考消费者，**不保证外部真实账本**；0 次对外部账本的保证声明。完成条件：入账计数与边界声明齐备。（FR-16；SC-12；verification.md §5；层：Fault/证据审查；证据：入账计数 + 边界声明）

**Checkpoint**: US7 独立可验证（Q8/Q10）；对照报告与演练证据齐备且无未测宣称。

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: CI 接入、耗时核验、分层/DeDo 审计、quickstart 证据汇总、文档同步与覆盖收口。

- [ ] T082 [P] 扩展 `.github/workflows/ci.yml`：按 verification.md §3–§4 接入普通 PR 必需检查——`internal/events/**`/迁移 `000015` → Unit + Contract（`make test-contract`）+ Integration-PG + Race；事件契约语义变更（`contracts/events.md`、`internal/events/catalog.go`、信封/schema 版本）→ Contract（`make test-contract`）+ 参考消费者兼容回归（T051/T058），破坏性变更必须新版本 + 兼容窗口（FR-12），不得原地变更；`internal/cache/**`/`internal/ratelimit/**` → Unit + Integration-Redis（缺 Docker 时 Unit 照跑 + Integration-Redis 记『待运行』：PR 打 `ci:integration-pending` 标签并阻止合并，后续必需 run 补跑转绿后解除）；上游集成点文件 → Unit + Integration-PG + 受影响资金安全回归（幂等/门禁/重放断言）+ E2E 核心；发布器/消费者运行时 → Unit + Integration-PG + Integration-Kafka；复用既有 setup-go 缓存、`docker info` 校验与凭据 masking 模式；**MUST NOT** 在普通 PR 启动完整 Fault/Perf。完成条件：workflow diff + PR 运行记录；Docker 不可用时的『待运行』记录机制、补跑责任与关闭条件（label＋必需后续 run／阻塞合并）写入 workflow 注释并与 verification.md §4 一致；未运行检查不得标通过、不得静默跳过。（FR-28；verification.md §3–§4；层：CI；证据：workflow diff + PR 运行记录）
- [ ] T083 [P] 新建 `.github/workflows/fault-perf.yml`：Fault Injection 与 Performance 独立运行（定时/手动/发布前门禁），失败不阻塞普通 PR、但阻塞对应发布声明（无证据不得宣称 FR-25/26 达标）；复用现有 Docker/testcontainers 自供与镜像固定策略。完成条件：手动触发可跑通；与普通 PR 触发矩阵分离可验证。（FR-28；verification.md §4；层：CI；证据：独立运行记录）
- [ ] T084 [P] 实测并记录 CI 耗时预算：在目标 runner 上先测各层基线（PG-only/全栈）→ 预算 = max(基线 × 显式余量, 固定下限) 且不超过层 CI timeout 硬上限；余量系数与 timeout 来源写入 `docs/evidence/013/ci_budget.md` 并同步 CI 配置；**MUST NOT** 编造分钟数（未实测一律标待测）。完成条件：基线与预算记录可复核。（FR-28；verification.md §4；层：CI/测量；证据：基线测量记录 + 预算配置）
- [ ] T085 [P] 新建 `internal/app/layering_audit_test.go`（Unit）：分层独立性审计——Unit 不依赖外部中间件；各 Integration 层按组件可独立运行；Fault/Perf 不在普通 PR 触发；`go.mod`/`compose.yaml`/CI 中 **0 处** Debezium/CDC/Kubernetes 运行或测试依赖（ADR §5/FR-28）；`internal/events` import 边界（不 import RPC/signer/上游 writer）。完成条件：扫描全部通过，违反即失败。（FR-03/28；adr.md §5；constitution XIII；层：Unit/结构；证据：扫描输出）
- [ ] T086 [P] 按 `quickstart.md` 逐场景（Q0–Q11）执行并汇总 `docs/evidence/013/quickstart_evidence_index.md`：每场景标注执行批次、命令、结果与证据路径；未执行场景显式标注（不得伪装通过）；所有「通过」必须来自真实中间件与真实迁移（mock/double 不得作为验收证据）。完成条件：索引完整。（FR-26；quickstart §2/§4；verification.md §2/§5；层：证据汇总；证据：索引文件）
- [ ] T087 [P] 实现与证据产出后同步文档（不提前声明）：`README.md`/`CHANGELOG.md`/`docs/project-context.md`（013 状态行、迁移号、T000-P 保持 OPEN）、`adr.md`（ADR-013-01/02 实施记录/状态更新）、`verification.md` 阈值校准结果（如已实测）；**MUST NOT** 在证据前宣称性能/可靠性达标；范围调整走 PD-3 另行裁决。完成条件：文档 diff 与证据一致。（FR-24/25；verification.md §5.4；PD-3；层：文档；证据：文档 diff）
- [ ] T088 [P] 对照本文件覆盖附录执行收口审计并记录 `docs/evidence/013/coverage_audit.md`：FR-01–FR-28、SC-01–SC-12、五态×七类矩阵行为、ADR-013-01/02、PD-1–PD-4 逐项对应任务与证据；缺口/延期项显式列出（归属与理由），**MUST NOT** 静默扩缩范围；发现设计矛盾或裁决边界不可满足时按 Blockers 节流程回报 orchestrator。完成条件：覆盖审计记录产出。（FR-27/28；plan Blockers/Deferred；层：审计；证据：覆盖审计记录）

---

## Merge responsibility & single-writer map（共享文件：单一 owner 任务 + 合流检查）

| 共享载体 | 单一 owner 任务链（顺序编辑，禁止并行） | 合流检查 | 下游门禁 |
|---|---|---|---|
| `migrations/000015_event_infrastructure.sql` | T008 | T009（up/down/up + 命名约束探针 + additive-only diff） | B3+ 全部生产者/消费者批次 |
| 事件契约（`internal/events/catalog.go`、`append.go`、信封/兼容测试） | T010 → T011；`events_contract_test.go` T014 | T014 × T009 × T015 × T016 四方同绿 | B3+；任何语义变更须新版本 + 兼容窗口 |
| 资金状态文件（生产者集成） | `internal/indexer/reorgcommit.go`：T028 → T052（US4 修订）；`internal/indexer/depositcommit.go`：T026；`internal/indexer/confirmcommit.go`：T027；`internal/withdrawal/intake.go`：T029 → T070；`internal/execution/intent.go`+`advance.go`：T030；`internal/execution/revision.go`：T053 | T038/T039/T040（域原子性探针）+ T085（import 边界） | 每域探针绿后方可合并到主线批次 |
| 事件运行时 | `internal/events/publisher.go` T033；`internal/events/consumer.go` T041 → T054；`internal/events/quarantine.go` T042 → T045；`internal/events/audit.go` T035；`internal/events/capacity.go` T069 | T036/T046/T048/T072 对应层验证 | 对应故事 checkpoint |
| 故障演练基建 | `internal/faultdrill/doc.go`、`harness.go`：T073（B8 创建骨架）→ T019（B9 扩展编排） | T073 完成条件：`go build ./...` 通过、`make test-fault` 骨架可运行 | B9 矩阵任务 T020–T025/T080 |
| 应用接线 | `cmd/txharbor/main.go`：T007（唯一 owner）；`internal/app/eventpublisher.go` T034；`internal/app/eventconsumer.go` T044；`internal/app/eventsadmin.go` T031 → T045；`internal/app/serve.go` T018 → T063 | 各 batch 退出证据 | 后续批次只扩展 `internal/app`，不回改 main.go |
| 环境/构建 | `compose.yaml` T004；`Makefile` T005；`go.mod` T002；`internal/config/config.go` T001 → T075（仅新增告警键）；`internal/metrics/*` T003 → T075 | T085 分层审计 | CI 批次 B11 |

**规则**：任一共享文件的编辑必须按上表链顺序进行；前序任务未完成时，后续任务不得与其并行编辑同一文件。合流检查未通过时，批次不得推进（任务保持未勾选）。

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: 无依赖，全部 [P]（文件互不相交）。
- **Foundational (Phase 2)**: 依赖 Setup；**阻塞全部用户故事**；T008→T009 顺序，T011 依赖 T010，T014/T015 依赖 T010/T011。
- **用户故事（Phase 3–9）**: 均依赖 Foundational 合流（T009×T014×T015×T016）。故事内部依赖如下；同一优先级内按真实前置排序执行：
  - **US2（P1，MVP）**: B2 合流后进入；T026–T030 可并行，T032–T034 依赖 T011/T012，T035 依赖 T012，验证任务依赖对应实现。
  - **US3（P1）**: 依赖 US2 的发布器（需要可消费事件）；T041 先行，T042–T044 紧随，T045 依赖 T042，验证任务依赖 T041/T044。
  - **US4（P1）**: 依赖 US2 的目录/Append 与 US3 的消费者版本守卫；T052 依赖 T028（单写者链），T054 依赖 T041。
  - **US1（P1，闭包）**: T017/T018 可与 B2–B7 并行开发；B9 中 T019 先行（补齐 T073 于 B8 创建的 faultdrill harness），T020–T025/T080 依赖 T019（同包 harness 与故障环境，顺序执行，不标 [P]）；T021–T025 进入条件为 B3–B8 全部合流（矩阵载体来自各故事）。
  - **US5（P2）**: 依赖 Foundational；与 B3–B6 无强依赖，可并行（仅失效器消费事件时依赖 US2 事件流）。
  - **US6（P2）**: 依赖 US2（Outbox 观测/发布器）与 US5（错误通道）；T070 依赖 T029（单写者链）。
  - **US7（P2）**: T078/T079 于 B5 执行（消费者就绪）；T080 于 B9 执行（依赖 US1）；T075–T077/T081 于 B10 执行。
- **Polish (Phase 10)**: 依赖全部故事与证据产出（B11）。

### Task-level dependency graph（执行批次顺序）

```text
B1: T001..T007 ─┬─▶ T008 ─▶ T009 ──────────────┐
                ├─▶ T010 ─▶ T011 ─▶ T014/T015/T016 ┤─▶ 合流（迁移×契约×Append×import 边界）
                └─▶ T012/T013 ─────────────────┘
B3: T026..T030 ─▶ T038/T039/T040（域原子性）；T031（cutover/回退）；T032（5 类一致性）
B4: T033 ─▶ T034 ─▶ T036/T037；T035（对账审计）
B5: T041 ─▶ T044 ─▶ T046..T051；T042 ─▶ T045；T043；T078/T079（E2E）
B6: T052/T053/T054 ─▶ T055/T056/T057/T058（8/8 目录收口）
B7: T059/T061 ─▶ T060/T062 ─▶ T063/T064 ─▶ T065..T068；消费 T017/T018
B8: T069 ─▶ T070/T071 ─▶ T072; T073（faultdrill 基建 + 追赶）/T074；消费 T018
B9: T019（harness 补齐，先行）─▶ T020..T025（五态矩阵）+ T080（V-DRILL 终验）；依赖 B8 T073
B10: T075/T076 ─▶ T077；T081（账本边界证据）
B11: T082/T083/T084（CI）/ T085/T086/T087/T088（收口）
```

### 可并发项（[P] 汇总）

- Setup：T001–T007 全部可并行。
- Foundational：T010/T012/T013 可并行；T014/T015 在 T010/T011 后就绪。
- US2：T026–T030 可并行；T036/T037/T038/T039/T040 验证文件互不相交可并行（依赖各自实现）。
- US3：T042/T046–T051 可并行；T045 与 T044 顺序。
- US4：T053/T055–T058 可并行；T052/T054 为单写者链。
- US5：T059/T061 可并行；T065–T068 可并行（T064 依赖 T061/T062）。
- US6：T069/T071 可并行；T072–T074 可并行（依赖 T069/T070）。
- US1：T017/T018 可并行；B9 中 T019 先行，T020–T025/T080 依赖 T019 的 harness（同包同故障环境，顺序执行，不标 [P]）。
- US7/Polish：各验证与审计文件互不相交，可并行。

---

## Parallel Execution Examples

```bash
# B1 Setup（互不相交文件）：
Task: "T001 config knobs in internal/config/config.go"
Task: "T002 deps in go.mod/go.sum"
Task: "T003 metrics families in internal/metrics/events.go"
Task: "T004 compose redis/kafka profile in compose.yaml"
Task: "T005 make targets in Makefile"
Task: "T006 testcontainer helpers in internal/testutil/"
Task: "T007 subcommand skeletons + cmd/txharbor/main.go"

# B2 Foundational（T008/T009 顺序后）：
Task: "T010 catalog in internal/events/catalog.go"
Task: "T012 outbox state/read queries in internal/events/outbox.go"
Task: "T013 error taxonomy in internal/events/errors.go"

# B3 生产者集成（合流通过后，互不相交）：
Task: "T026 deposit created in internal/indexer/depositcommit.go"
Task: "T027 confirmed in internal/indexer/confirmcommit.go"
Task: "T028 status_changed in internal/indexer/reorgcommit.go"
Task: "T029 request received in internal/withdrawal/intake.go"
Task: "T030 execution state_changed in internal/execution/"

# B5 消费者验证波（不同文件）：
Task: "T046 V-IDEMPOTENCY in internal/events/consumer_integration_test.go"
Task: "T047 V-PROGRESS in internal/events/consumer_progress_integration_test.go"
Task: "T048 V-RETRY-QUARANTINE in internal/events/quarantine_integration_test.go"
Task: "T049 PD-4 counterexamples in internal/events/replay_boundary_integration_test.go"

# B7 Redis 验证波（不同文件）：
Task: "T065 V-CACHE in internal/cache/cache_integration_test.go"
Task: "T066 V-RATELIMIT in internal/ratelimit/ratelimit_integration_test.go"
Task: "T067 HTTP failure in internal/app/ratelimit_failure_integration_test.go"
Task: "T068 invalidation in internal/cache/invalidator_integration_test.go"
```

**禁止**：并行编辑单写者表中的同一文件；并行运行共享同一测试文件或同一故障环境的任务；在合流检查未通过时进入下游批次。

---

## Implementation Strategy

### MVP first

1. Phase 1 Setup → Phase 2 Foundational（CRITICAL，含迁移×契约×Append×import 边界合流检查）。
2. Phase 4 US2 → B3 生产者集成 → B4 发布器：最小可演示增量（业务状态与事件同事务、至少一次发布、身份/版本收敛）。
3. Phase 5 US3：消费幂等/进度/隔离/重放 + 核心 E2E（B5）。
4. 三个 P1（US2/US3/US4）与 Redis（US5）验证通过后再推进 B8–B9。
5. 任意 checkpoint 可独立验证：失败即停止，任务保持未勾选，不进入下游批次。

### Incremental delivery

Setup + Foundational → US2（MVP）→ US3 → US4 → US5（可与 US2–US4 并行）→ US6 → US1（矩阵闭包）→ US7（观测/基准）→ Polish。每个批次产出独立证据；Fault 与 Performance 独立运行、不阻塞普通 PR（FR-28）。

### 批次验证与本地提交节点（orchestrator 执行；无 push/PR/merge/deploy）

| 节点 | 验证命令（示例） | 提交信息（Conventional Commits，无 attribution trailer） |
|---|---|---|
| B1 | `make build && make test && make test-integration`（迁移探针） | `feat(events): add event infrastructure scaffolding and 000015 schema` |
| B2 | `make test && make test-contract && make test-integration`（events 包） | `feat(events): add transactional outbox core and event contract` |
| B3 | `make test-integration`（indexer/withdrawal/execution 探针） | `feat(events): emit outbox events from deposit and withdrawal transactions` |
| B4 | `make test && make test-integration-kafka` | `feat(events): add outbox publisher with lease claim and crash recovery` |
| B5 | `make test-contract && make test-integration && make test-e2e` | `feat(events): add idempotent consumer with quarantine and audited replay` |
| B6 | `make test-contract && make test-integration` | `feat(events): emit and converge reorg revision events` |
| B7 | `make test-integration-redis && make test` | `feat(cache): add non-authoritative cache and fail-closed rate limiting` |
| B8 | `make build && make test-integration && make test-fault` | `feat(events): add outbox capacity guard and catch-up rescan` |
| B9 | `make test-fault` | `test(fault): add five-state failure matrix drills` |
| B10 | `make test-perf`（独立，不阻塞普通 PR） | `perf(events): add PG-only baseline and full-stack benchmark` |
| B11 | CI 触发矩阵核验 + 审计测试 | `ci(events): add layered checks and closeout records` |

**本轮（任务拆解）**：不实现任何代码/迁移/CI；不进入 analyze/implement；所有实现任务保持 `- [ ]`；唯一本地提交为 tasks.md 本身。

### Gates and stop lines

- `/speckit.analyze` 在实现前独立执行；发现的问题优先处理。
- 本步骤授权范围：仅新建 `specs/013-reliable-event-infrastructure/tasks.md` 与本地分支提交；禁止推送/PR/合并/部署、生产 RPC、推进 T000-P。
- 验证归属 orchestrator；批次退出证据未齐不得勾选任务。

---

## Coverage appendix

### FR-01–FR-28 → tasks

| FR | 任务 | 验收/证据 |
|---|---|---|
| FR-01 PG 唯一权威 | T009, T014, T016, T020, T059, T068, T085 | V-BASE、决策路径不读缓存断言 |
| FR-02 Redis/Kafka 用途限定 | T003, T020, T059–T064, T085 | import 边界 + 无权威写入断言 |
| FR-03 不引入其他基础设施/不重定义 | T002, T020, T085 | 依赖/编排审计 + Debezium 排除 |
| FR-04 故障矩阵 | T017–T025, T080 | 五态×七类逐格断言 |
| FR-05 门禁继承/重复投递不触发新效果 | T030, T041, T049, T054, T080 | V-IDEMPOTENCY/V-DRILL/E2E |
| FR-06 故障不变量 | T025, T080 | 五项 0 不变量断言组 |
| FR-07 同事务 Outbox + 目录 | T008, T011, T026–T032, T038–T040 | 原子性探针 + 目录一致性 |
| FR-08 至少一次发布/崩溃恢复/有界尝试 | T033–T037 | V-PUBLISHER 崩溃点矩阵 |
| FR-09 事件身份 UNIQUE/冲突告警 | T010, T011, T014, T015, T016 | 23505/冲突探针 |
| FR-10 版本顺序与链身份 | T010, T041, T046, T051 | 乱序/缺口/旧不覆盖新 |
| FR-11 重组修订 | T028, T052–T058 | V-REVISION |
| FR-12 schema 版本兼容/fail-closed | T014, T042, T051, T058 | Contract 兼容矩阵 |
| FR-13 消费者持久幂等 | T041, T046, T048, T050 | inbox 基数断言 |
| FR-14 有界重试/隔离/重放 PD-4 | T042, T045, T048, T049 | V-RETRY-QUARANTINE + 反例断言 |
| FR-15 进度持久/续传/lag | T041, T044, T047, T050 | V-PROGRESS |
| FR-16 入账边界声明 | T043, T081 | 参考消费者 + 边界声明检查 |
| FR-17 缓存查询行为 | T059, T060, T063, T065, T068 | V-CACHE |
| FR-18 PD-1 限流失效 | T062, T063, T066, T067 | V-RATELIMIT 双验收线 |
| FR-19 RPC 降级不破坏契约 | T064, T067 | 分类/完整性不跳过断言 |
| FR-20 PD-2 容量保护 | T069–T074 | V-CAPACITY + I-CAP 不变量 |
| FR-21 发布器恢复排空 | T033, T034, T074 | V-CATCHUP 发布侧 |
| FR-22 消费者追赶/再故障 | T073, T080 | V-CATCHUP |
| FR-23 追赶期缓存/限流重建 | T060, T063, T068 | V-RECOVERY/V-CACHE |
| FR-24 观测 | T003, T075, T085 | 指标/日志/告警来源与脱敏 |
| FR-25 对照基准与 ADR | T076, T077, T087 | V-BENCH + 表述纪律 |
| FR-26 故障演练验收 | T019–T025, T080 | V-DRILL 证据包 |
| FR-27 范围留白纪律 | T001, T002, T085, T088 | 审计 + 数值纪律 |
| FR-28 测试分层与 CI 成本 | T005, T006, T082–T085 | 分层可独立运行 + PR 触发矩阵 |

### SC-01–SC-12 → tasks

| SC | 证据/断言 | 任务 |
|---|---|---|
| SC-01 矩阵一致率 100%、门禁绕过 0 | V-FAULT-MATRIX | T021–T025, T080 |
| SC-02 0 重复意图/付款/孤儿入账/权威丢失 | V-DRILL + 反例 | T025, T049, T080 |
| SC-03 状态↔事件 100%/0、重复投递财务重复 0 | V-ATOMICITY + 参考消费者 | T014, T015, T038–T040, T046 |
| SC-04 有效应用 1、旧不覆盖新、缺口不静默 | V-IDEMPOTENCY | T046, T050 |
| SC-05 毒事件隔离 100%、无界重试 0、静默丢弃 0 | V-RETRY-QUARANTINE | T042, T048, T049, T051 |
| SC-06 进度 100% 恢复、重放重复效果 0 | V-PROGRESS | T047, T050 |
| SC-07 修订幂等、Orphaned 误用 0、新付款 0 | V-REVISION | T055–T058 |
| SC-08 陈旧权威 0、无限制放行 0、恢复后陈旧 0 | V-CACHE + V-RATELIMIT | T065–T068 |
| SC-09 停机 0 丢失、可观测 100%、PD-2、排空有界 | V-CAPACITY + V-CATCHUP | T069–T074 |
| SC-10 再故障 0 丢失/0 重复、进度可续 | V-CATCHUP | T073, T080 |
| SC-11 对照报告 100%、未测不宣称 | V-BENCH | T076, T077 |
| SC-12 证据覆盖矩阵、边界声明、外部保证 0 | V-DRILL/V-BENCH 证据审查 | T080, T081, T086 |

### 五态 × 七类矩阵行为 → tasks

| 操作 | 正常 | 仅 Redis 故障 | 仅 Kafka 故障 | 双故障 | 恢复追赶 |
|---|---|---|---|---|---|
| 充值处理 | T035, T038 | T021（缓存旁路/RPC 有界） | T022（事件积压） | T023 | T024, T073 |
| 确认与重组恢复 | T027, T028, T055 | T021 | T022（修订积压） | T023 | T024, T057 |
| 提款创建（接收） | T029, T070 | T021, T067（拒绝新创建 + PD-1） | T022, T072（容量边界先拒） | T023 | T024 |
| 已有提款执行 | T030, T056 | T021, T067 | T022 | T023 | T024 |
| 查询 | T018, T063 | T021, T065（直读 PG/降级标注） | T022 | T023 | T024 |
| 事件订阅 | T033–T037 | T021（继续） | T022, T034（停止投递、积压有界） | T023 | T024, T073, T074 |
| 非关键功能 | T018 | T021（降级） | T022（降级） | T023（降级/暂禁） | T024（恢复） |

**安全前提行**：0 门禁绕过（T020/T025/T080）；不跳过链身份/完整性校验（T064/T067）；链上已发生充值不拒绝、无法持久化则暂停补扫（T071/T072）；在途提款不新建意图（T049/T071）；不静默丢弃/不覆盖未发布（T069/T072）。

### ADR / PD → tasks

| 决策 | 任务 |
|---|---|
| ADR-013-01 Redis 非权威缓存 + 分布式限流（含收益假设/成本/Review triggers） | T002, T059–T068, T077, T087 |
| ADR-013-02 Kafka + 事务性 Outbox（含 PG-only 替代与不宣称跨系统恰好一次） | T008, T011, T033–T050, T076, T077 |
| ADR §3 同负载对照基准设计 | T076, T077 |
| ADR §5 Debezium 明确不引入 | T002, T085 |
| PD-1 限流失效处置 | T062, T063, T066, T067, T021, T023 |
| PD-2 容量保护 | T069–T074, T022, T023 |
| PD-3 Kafka 价值证明与范围保持 | T076, T077, T087 |
| PD-4 人工重放边界（审计/幂等/不得重付） | T045, T048, T049, T050 |

---

## Deferred（非阻塞；承接 plan.md Deferred，含归属）

| 事项 | 状态/归属 |
|---|---|
| 实测阈值（soft/hard/reserve/retention、限流速率、退避/超时、追赶窗口、告警阈值） | 性能/实现批次测量后校准（T077/T075/T084）；不得提前写死 |
| Kafka 价值证明结论 | V-BENCH 报告（T077）产出后；范围调整须另行提交用户决定（PD-3），不自动删减 |
| 客户端/镜像可用性（franz-go、go-redis、testcontainers 模块、Kafka/Redis 镜像 tag） | T002/T004 验证后固定 |
| `event_system_state` 是否可裁剪为配置 | T031 实现批次确认 |
| T000-P 生产 provider 选型 | 保持 OPEN；本 feature 不关闭、不宣称生产就绪 |
| A-13（011 全链 E2E） | 已 CLOSED（沿用上游记录，不重开、不重新核验） |

---

## Notes

- `[P]` = 不同文件、不依赖未完成任务；单写者表中的文件一律顺序编辑（见 Merge responsibility）。
- 所有任务带确切文件路径、完成条件、FR/SC 或设计依据（D/R/契约节）、测试层与证据；资金关键路径写明具体断言对象。
- 所有任务本轮保持未勾选；本文件不声称任何执行、演练、基准或 analyze 已完成。
- 全局表述纪律：至少一次投递 + 幂等处理；不宣称跨系统恰好一次；无实测不宣称收益/达标；无裁决不引入替代限流。
- 002–011/PB 只读消费，不重定义；Debezium/CDC/Kubernetes 不是运行或测试依赖；验证归属 orchestrator。
