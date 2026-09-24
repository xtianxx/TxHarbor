# Changelog

本文件记录 TxHarbor 已合并到 main 的变更。条目附合并提交或规格路径；
未合并、未部署的内容不在此列。分支上已完成但尚未合并的变更记录在
`Unreleased` 节，并显式标注「未合并/未部署」与验收状态。

## [Unreleased]

### Added

- 013 Reliable Event Infrastructure（分支 `013-reliable-event-infrastructure`，
  **未合并/未部署**；B1–B10 提交 `23561da`…`65b2f07`，B11 收口提交见分支）：
  - 迁移 `migrations/000015_event_infrastructure.sql`：outbox_events（身份 UNIQUE/
    状态一致性/修订约束）、consumer_progress/inbox/versions/quarantine、
    event_ops_audit、event_system_state（catalog_version=1、cutover 种子）。
  - 事务性 Outbox（`internal/events`）：业务状态与事件同事务 Append、事件身份
    UUIDv5 派生、至少一次发布（SKIP LOCKED + 租约 + acks=all）、blocked 可见可审计；
    **不宣称跨系统恰好一次**。
  - 幂等消费者：inbox 去重、版本守卫、持久进度、有界重试、持久隔离与人工重放
    （PD-4：审计/幂等/不得重付；重放 0 新意图/nonce/签名/广播）。
  - 重组修订事件（`deposit.revision.applied`/`reinstated`、
    `withdrawal.execution.revised`）：旧身份/新状态或 Orphaned/原因/恢复版本。
  - Redis 非权威缓存与 fail-closed 分布式限流（PD-1：限流不可用时新提款创建
    拒绝且可重试，存量资金流程继续）。
  - 容量保护与恢复追赶（PD-2：停新保在途、链上事实不拒绝、无法持久化则按可靠
    进度暂停补扫、绝不静默丢弃）；`internal/faultdrill` 五态故障矩阵演练
    （独立 Fault 层）与 `internal/perf` PG-only vs 全栈对照基准（独立 Performance 层）。
  - CI 分层接入（`ci.yml`：受影响 Unit/Contract/Integration-PG/Redis/Kafka/E2E +
    `ci-required` 门禁；`fault-perf.yml`：定时/手动/可调用，不阻塞普通 PR）与
    分层审计 `internal/app/layering_audit_test.go`；Docker 不可用记 NOT RUN +
    `ci:integration-pending` 阻断合并纪律。
- 验收状态（截至 B11）：90/90 任务勾选；**本地验收**（真实中间件/迁移）通过；
  **远程 CI 运行待核验**；**不宣称生产就绪**（T000-P 保持 OPEN）。
  证据索引：`docs/evidence/013/quickstart_evidence_index.md`、
  `docs/evidence/013/coverage_audit.md`、`docs/evidence/013/benchmark_report.md`、
  `docs/evidence/013/ci_budget.md`。

### Known limitations（013，分支）

- 容量/限流/告警/追赶窗口等阈值一律「待测/待裁决」，未校准前不得宣称达标。
- 对照基准为本地单主机、短窗口、合成负载观测，不构成生产容量结论；Kafka 范围按
  PD-3 保持，调整须另行提交用户决定。
- 远程 GitHub runner 上的 CI/fault-perf 实际运行尚未发生（未推送），运行证据待核验。

## [v1.0.0] - 2026-09-23

### Added

- 001–008 工程底座与链上能力：配置/健康/迁移门禁、区块头索引、Transfer 日志索引、
  充值识别、确认跟踪、重组恢复、提现接收（receive-only）、nonce 管理。
  规格：`specs/001-project-foundation` … `specs/008-nonce-manager`；
  迁移：`migrations/000001_baseline.sql` … `000008_nonce_manager.sql`。
- 012 PB（007 授权载体）：grant+scope 受控供给、scope 身份/内容/版本/费用/用途与
  撤销一致性。规格：`specs/012-007-authorization-carrier`；
  迁移：`migrations/000010_withdrawal_authorization_scopes.sql`。
- 009 签名隔离服务：独立监听器、本地 development 密钥 provider、
  `production` 模式 fail closed。PR #12，merge `295c49d`；
  规格：`specs/009-signer-service`；迁移：`migrations/000009_signer_service.sql`。
- 010 交易生命周期 + 011 提款执行 worker：构造/签名/广播/同字节重播/费用替换/
  回执验证/重组后追踪，以及执行准入、领取续租、授权消费与状态投影。
  PR #15，merge `3ea6eb1`；迁移：`migrations/000011_tx_lifecycle.sql`、
  `000012_withdrawal_execution.sql`、`000013_tx_lifecycle_intent_fk.sql`、
  `000014_intent_fk_repair.sql`。
- 011 生产进程入口 `withdrawal-worker`（`jointwire.Worker` 联合装配）与 B1/B2
  （receipt canonicality、authority revision → 011 projection）。PR #16，
  merge `df5a280`。
- A-13 全链联合验收缺口关闭：V13-2b（撤销/到期 × 发送门禁双锁序交错）、
  V13-4b（同 grant 条件复用）。PR #17，merge `f51cde5`。
- 测试加固（PR #21，merge `ecffbed`）：执行重试/调用方隔离/过期签名
  （`2ac70c3`）、替换与对账不变量（`8ba420e`）、confirmauth 竞争/签名隔离/
  intake unknown-commit（`dd51d12`）、确认边界/充值提交竞争/释放审计（`4aefc11`）、
  重组策略授权不变量（`8176a60`）。
- 健康 readyz 同链 RPC 故障注入测试（PR #19，`0e22901`）；读侧 COMMIT 应答拦截
  故障注入（PR #14，`37509c5`/`09d7481`）。
- CI 工作流 `.github/workflows/ci.yml`：lint（gofmt + vet）、build、单元测试
  （+ race）、集成测试（Docker）四作业；PR #22（`5a49306`）增加
  `workflow_dispatch` 与 PR 并发控制。

### Changed

- 010/011 修复（PR #16，merge `df5a280`）：receipt canonicality 以链上事实为准；
  authority revision 正确到达 011 投影；011 在 completed 后保持追踪。
- `serve` 在持久化业务暂停下保持存活（不再退出）；被取消的在途读不再记录
  retry state 2。PR #20，merge `67979fd`（`4dc0c23`/`e508eed`）。
- 测试基础设施：indexer 集成测试共享一个 PostgreSQL 容器，并修正
  fault-injector 启动分类。PR #24，merge `6a3c132`。
- readyz 翻转测试在 PG Stop 前增加静默屏障，消除时序抖动。
  PR #23，merge `d297635`。
- 011 A-13 状态登记：A-13 CLOSED 于 main `f51cde5`，T000-P 保持 OPEN
  （`09f572b`；`specs/011-withdrawal-executor/integration-readiness.md`）。

### Security

- 签名密钥只存在于 009 进程；业务进程经 `KeyProvider` 边界调用，无 digest 签名面；
  凭据不进入日志/错误/指标，启动回显经 `internal/logx.Redact` 脱敏
  （`specs/009-signer-service`，merge `295c49d`）。
- 007/008/009 三套 Bearer 凭据独立；008 读端点在未配置 token 时拒绝一切请求；
  授权撤销为带外 CLI（`withdrawal-authz revoke`），不暴露 HTTP 写入口
  （`specs/007-withdrawal-creation`、`specs/008-nonce-manager`、`specs/012-007-authorization-carrier`）。
- 撤销/到期与发送门禁的交错窗口经双锁序测试关闭（PR #17，merge `f51cde5`）。

### Known limitations

- **T000-P 保持 OPEN**：未部署，不宣称生产就绪（`docs/project-context.md:13`、
  `specs/003-event-indexing/tasks.md:24`、`specs/005-confirmation-tracking/tasks.md:25`、
  `specs/004-deposit-detection/acceptance.md:56-57`）。A-13 已 CLOSED
  （`specs/011-withdrawal-executor/integration-readiness.md:348`），T000-P 独立 OPEN（同文件 `:350`）。
- 无 KMS/HSM provider：v1 仅本地 development 密钥 provider，生产模式拒绝启动
  （`internal/signer/provider.go:73-76`；`specs/009-signer-service/research.md:76,89`、
  `plan.md:365`）。
- 无 TLS（`serve` 明文监听，`internal/app/serve.go:367`）、无 Dockerfile；
  Redis / Kafka 未实现（`go.mod` 无相关依赖，`agent.md:3` 仅为目标愿景）。
- `.env.example` 仅覆盖 001–005 变量；007–011 变量需按规格另行配置
  （`internal/config/config.go` 共 48 个变量）。
