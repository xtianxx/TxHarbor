# Verification & Observability Design: 013 Reliable Event Infrastructure

**Feature**: 013-reliable-event-infrastructure | **Date**: 2026-09-24 | **Plan**: [plan.md](plan.md) D9/D10 | **Quickstart**: [quickstart.md](quickstart.md) | **Spec**: FR-24/25/26/28、SC-01–SC-12

**性质**：本文件是观测口径、验收设计与测试分层的**设计**；不执行、不产出实测数值。百分比/时间预算只给方法与边界，不编造精确值（spec 纪律）。

---

## 1. 指标与采集口径（FR-24）

**百分位方法**：服务侧用 Prometheus Histogram（按接口类分桶；p95/p99 由 `histogram_quantile` 计算，分桶边界实现批次按实测负载选定并记录）；基准侧同时用负载生成器逐请求采样（原始样本存证），两者差异 > 报告阈值时以生成器样本为准并记录原因。吞吐 = 完成请求数/测量窗口（排除预热窗口）；预热与样本量在实现批次固定并写入脚本。

| 指标 | 定义 | 来源 | 标签/维度 | 备注 |
|---|---|---|---|---|
| `api_request_duration_seconds`（histogram） | API 延迟分布 → p95/p99 | `serve` 中间件 | `interface_class`（查询/提款创建/操作员等）、`outcome` | 按接口类对照（SC-11） |
| `api_requests_total` / `api_errors_total` | 吞吐与错误率 | 同上 | 同上、`error_class` | 错误率分母/分子口径固定 |
| `rpc_request_duration_seconds` / `rpc_requests_total` | RPC 延迟/吞吐（按调用类） | RPC 客户端包装 | `class`（查块/取日志/发送等） | 不掩盖既有错误分类 |
| `outbox_pending_count`（gauge） | 待发事件数 | PG 查询（`publish_state='pending'`） | `event_family` | 容量保护的输入 |
| `outbox_pending_oldest_age_seconds`（gauge） | 最老待发等待（now − min(created_at) of pending） | PG 查询 | `event_family` | 停机影响核心观测 |
| `outbox_publish_failures_total` | 发布失败计数 | 发布器 | `error_class`（瞬时/永久/契约） | 永久类必须告警 |
| `outbox_published_total` / `outbox_attempts_total` | 发布完成/尝试 | 发布器 | — | 重复发布可见 |
| `consumer_lag_seconds` / `consumer_lag_messages`（gauge） | 消费积压（Kafka 高水位 − position） | 消费者/管理面 | `consumer_name`、`partition` | 追赶判据 |
| `consumer_catchup_seconds` | 追赶时间：从恢复时刻到 lag 低于阈值（阈值配置） | 演练脚本/消费者 | `consumer_name` | 数值待测 |
| `consumer_applied_total` / `consumer_retry_total` / `consumer_quarantine_total` | 应用/重试/隔离计数 | 消费者 | `failure_class` | SC-05 证据 |
| `consumer_replay_clock`、`event_replay_ops_total` | 人工重放计数（与自动重试分列） | `events-admin`/审计 | `op_kind`、`consumer_name` | PD-4 证据 |
| `cache_hits_total` / `cache_misses_total` / `cache_fallback_total` | 缓存命中/回退 | `internal/cache` | `family` | 回退=直读 PG |
| `cache_epoch_rotations_total` | epoch 轮换 | 同上 | — | 恢复不提供陈旧值的证据 |
| `ratelimit_denied_total` / `ratelimit_unavailable` / `ratelimit_recovery_total` | 限流拒绝/不可用/恢复 | `internal/ratelimit` | `interface_class` | SC-08/PD-1 证据 |
| `rpc_budget_paused_total` | RPC 有界控制暂停 | RPC 包装 | `class` | PD-1 RPC 路径证据 |
| `capacity_soft_breaches_total` / `capacity_refusals_total` | 容量软边界/拒绝 | 容量门禁 | `op_class` | SC-09 证据 |
| `redis_available` / `kafka_available`（gauge） | 依赖可用性（非权威健康信号） | health 探针 | — | 降级状态展示 |
| `events_identity_conflict_total` | 同身份异内容冲突 | `Append` | `event_type` | 必须告警 |
| `outbox_blocked_count`（gauge） | 永久阻塞事件数 | PG 查询 | — | 非静默丢弃证据 |

**结构化日志**：事件身份、业务身份、链身份、版本、尝试次数、错误分类；MUST NOT 含密钥/凭据/原始签名材料；脱敏沿用 `internal/logx`。

**告警阈值**：全部待测/配置；给方法——软边界持续超 `drain_target_window` 告警、到达硬边界即 P1、永久阻塞与身份冲突即告警、隔离新增即告警。数值实现批次按基准与实测校准后写入配置（FR-24 要求「有依据」，依据 = 测量/plan/裁决）。

## 2. 可重复验收设计（FR-26/SC-01–12）

**环境**：本地确定性（Anvil + PG + Redis + Kafka + Compose），不依赖公网测试网；PG-only 基线 = 仅 PG + Anvil。

| ID | 验收 | 方法要点 | 通过判定 | 覆盖 |
|---|---|---|---|---|
| V-BASE | PG-only 基线 | 013 接线关闭；核心充值/提现流跑通 | 门禁与既有语义不变；0 依赖 Redis/Kafka | FR-01/03 |
| V-ATOMICITY | 事务原子性 | 每集成点提交/回滚两路径；事件行与业务行对照 | 提交必有、回滚必无；版本连续；冲突拒绝+告警 | FR-07/09、SC-03 |
| V-PUBLISHER | 发布器 | 停机→恢复；发布前后 kill；双实例并发领取 | 0 丢失；重复可被吸收；同刻单 owner；blocked 可见可审计 | FR-08/21、SC-03 |
| V-IDEMPOTENCY | 幂等/版本 | 重复投递（含重启/rebalance）、乱序、缺口 | 效果 = 1；旧不覆盖新；缺口有界等待后隔离；0 静默跳过 | FR-10/13、SC-04 |
| V-PROGRESS | 进度恢复 | offset 提交失败/丢失后重启 | 100% 从 PG 进度续传；0 重复效果 | FR-15、SC-06 |
| V-RETRY-QUARANTINE | 重试/隔离/重放 | 注入可重试/不可重试/未知版本；修复后重放 | 隔离 100% 可审计可重放；重放幂等；无界重试 0；重放 0 付款动作 | FR-12/14、SC-05 |
| V-REVISION | 修订 | 浅重组影响 Pending/Confirmed；重复/乱序修订；block_hash 复活 | Orphaned 误用 0；修订幂等；0 新意图/发送；复活不混淆 | FR-11、SC-07 |
| V-CACHE | 缓存 | 状态变化后立即查询；关/清 Redis；恢复后再查 | 陈旧财务权威 0；回源有界；恢复后陈旧值 0；标注正确 | FR-17/23、SC-08 |
| V-RATELIMIT | 限流 | 停 Redis；调新提款创建/存量流程/查询 | 新创建 100% 拒绝+可重试；存量门禁不变；恢复平滑 | FR-18、SC-08 |
| V-CAPACITY | 容量 | 停 Kafka 累积至软/硬边界（配置小值注入） | 停新保在途；链上不拒绝/暂停补扫；0 静默丢弃；在途完成 | FR-20、SC-09 |
| V-CATCHUP | 追赶/再故障 | 恢复排空与摄入追赶；追赶中再停 | 0 丢失/0 重复；进度可续；追赶时间可观测 | FR-21/22、SC-09/10 |
| V-DRILL | 双故障演练 | 五态全链路：正常→仅 Redis→仅 Kafka→双故障（关 Redis+Kafka，保 PG+链）→恢复追赶；七类操作逐项 | 矩阵一致 100%；门禁绕过 0；事件补齐/进度恢复；0 重复意图/付款；0 孤儿入账；模拟消费者证据 + 边界声明 | FR-04/05/06/26、SC-01/02/12 |
| V-BENCH | 对照基准 | 路径 A/B 同负载同故障（[adr.md](adr.md) §3） | 报告 100% 产出；未测 0 次宣称达标 | FR-25、SC-11 |

**顺序**：每项可独立运行；V-DRILL 依赖 V-ATOMICITY/V-PUBLISHER/V-IDEMPOTENCY 的机制存在（设计层前置关系）。任何「通过」必须来自真实中间件与真实迁移；double/mock 不得作为验收证据（011 纪律）。

**证据留存**：演练日志、指标截图/导出、报告（含环境规格与 commit）、审计表摘录；「不重复入账」结论必须附 FR-16 边界声明。

## 3. 测试分层（FR-28）

| 层 | 内容 | 依赖 | 命令（设计；实现批次添加） | 独立性 |
|---|---|---|---|---|
| Unit | 状态机守卫、退避计算、身份/版本派生、容量公式校验、错误分类、脱敏 | 无中间件 | `make test`（现状可用） | 常跑 |
| Integration-PG | Outbox 原子性、冲突、领取/租约、inbox/版本/进度、容量查询、重放审计 | PostgreSQL（testcontainers，现状 `-tags integration`） | `make test-integration`（现状可用） | 独立 |
| Integration-Redis | 缓存失效/epoch/回源有界、限流脚本/失效处置 | Redis 容器 | `make test-integration-redis`（新增） | 独立 |
| Integration-Kafka | 投递确认、重复/乱序、消费者组/rebalance、lag | Kafka 容器 | `make test-integration-kafka`（新增） | 独立 |
| Contract | 信封/目录/schema 版本/兼容矩阵/消费者兼容（含未知版本 fail-closed）、参考消费者 | 无中间件（纯 Go，直接投喂 fixture；无 Docker） | `make test-contract`（新增；tag `contract`，T014/T051/T058 归属该 tag） | 独立 |
| E2E | 充值流（链→索引→识别→确认→事件）与提现流（API→…→执行→事件）核心路径 | 全栈 + Anvil | `make test-e2e`（新增） | 独立 |
| Fault Injection | V-PUBLISHER/V-RATELIMIT/V-CAPACITY/V-CATCHUP/V-DRILL（含关 Redis+Kafka） | 全栈 + 故障注入 | `make test-fault`（新增） | 独立，不进普通 PR |
| Performance | V-BENCH 对照报告（p95/p99/吞吐/资源/追赶） | 全栈 | `make test-perf`（新增） | 独立，不进普通 PR |

分层纪律：Unit 不依赖外部中间件；Contract 无中间件、经 `make test-contract` 独立运行（tag `contract`）；各 Integration 层按组件独立可运行；普通 Go 改动不默认启动全部中间件/本地链/全演练（FR-28）。Debezium 不在任何层。

## 4. PR 回归触发与预算（方法，不编造分钟数）

**触发映射**（实现批次写为 CI 规则）：

| 变更路径 | 必需检查（普通 PR） | 独立检查（定时/手动/发布前） |
|---|---|---|
| `internal/events/**`、迁移 `000015` | Unit + Contract（`make test-contract`）+ Integration-PG + Race | Fault、Perf、V-DRILL |
| 事件契约语义（`contracts/events.md`、`internal/events/catalog.go`、信封/schema 版本） | Contract（`make test-contract`）+ 参考消费者兼容回归（T051/T058）；破坏性变更须新版本 + 兼容窗口（FR-12） | Fault、Perf、V-DRILL |
| `internal/cache/**`、`internal/ratelimit/**` | Unit + Integration-Redis；缺 Docker 时 Unit 照跑、Integration-Redis 记『待运行』（`ci:integration-pending` 标签 + 阻止合并，后续必需 run 补跑转绿后解除；不得标通过/静默跳过） | Fault、Perf、V-DRILL |
| 上游集成点文件（indexer/withdrawal/execution 等） | Unit + Integration-PG + 受影响资金安全回归（幂等/门禁/重放断言）+ E2E 核心 | Fault、Perf |
| 发布器/消费者运行时 | Unit + Integration-PG + Integration-Kafka | Fault、Perf |
| 文档/规格目录 | 现有 docs 检查 | — |

**受影响资金安全回归**（MUST 仍运行）：幂等（重复投递/重复请求）、门禁不被绕过、重放不产生新付款动作、原子性断言——即便变更「看起来」不相关，只要触及集成点或事件运行时即触发。

**『待运行』纪律（Docker/中间件不可用）**：任何因环境缺失未运行的检查一律记『待运行』并阻断合并（label + 必需后续 run），不得呈现为通过、不得静默跳过；补跑必须使用真实中间件与真实迁移（mock/double 不得作为验收证据）；『待运行』记录、补跑责任与关闭条件由 T082 在 workflow 中固化并与本表一致。

**预算方法**：在目标 runner 上先测基线耗时（各层 × PG-only/全栈），预算 = max(基线 × 显式余量, 固定下限) 且不超过层 CI timeout 硬上限；余量系数与 timeout 在实现批次按测量写入 CI 配置并记录来源。本文件不写具体分钟数（未实测）。

**独立检查节奏**：Fault 与 Perf 由定时任务/手动触发/发布前门禁运行，失败不阻塞普通 PR，但阻塞对应发布声明（无证据不得宣称 FR-25/26 达标）。

## 5. 证据边界与表述纪律（FR-16/SC-12）

1. 「不重复入账」证据只覆盖：本项目事件身份/版本/投递语义 + 消费者幂等契约 + 参考消费者演示；MUST 明确声明不保证外部真实账本（0 次对外保证声明）。
2. 无实测数据的数值目标 MUST NOT 表述为「已达标」；未测一律标「待测/待裁决」。
3. Kafka 价值结论只在 V-BENCH 报告产出后成立；范围调整按 PD-3 另行提交用户裁决。
4. 所有验收证据绑定 commit 与环境规格；文档升级（README/CHANGELOG）须等证据产出后另行批次（本步不修改）。

## 6. 实施与校准状态（B11 收口记录，2026-09-24）

- **实现状态**：013 的 B1–B11 全部任务勾选（90/90）；本地验收证据索引见
  [quickstart_evidence_index.md](../../docs/evidence/013/quickstart_evidence_index.md)，
  覆盖审计见 [coverage_audit.md](../../docs/evidence/013/coverage_audit.md)。
- **阈值校准**：容量 soft/hard/reserve/retention、限流速率、退避/超时、追赶窗口、
  告警阈值**尚未校准**（无生产测量）；一律维持「待测/待裁决」，0 次表述为已达标。
- **CI 耗时预算**：本地各层基线已实测并记录于
  [ci_budget.md](../../docs/evidence/013/ci_budget.md)；目标 runner 基线与预算
  **待核验**（未推送、未触发远程 CI）。
- **远程 CI**：`.github/workflows/ci.yml`（T082）与 `.github/workflows/fault-perf.yml`
  （T083）已完成本地静态校验（YAML/`bash -n`/触发矩阵断言），GitHub runner 实际运行
  证据待核验；未运行项不得标通过。
- **状态边界**：分支 `013-reliable-event-infrastructure` 未合并/未部署；T000-P 保持
  OPEN；不宣称生产就绪。三态口径（本地验收 / 远程 CI / 生产就绪）不得混同。
