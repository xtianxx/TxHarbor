# TxHarbor 协作指南

TxHarbor 是开发与验证阶段的 EVM / ERC-20 充值观察与提现异步执行基础设施（Go + PostgreSQL）；
不是钱包界面，也不维护用户余额账本。总览与快速开始见 [README.md](README.md)，文档入口见 [docs/README.md](docs/README.md)。

（旧根指南 `agent.md` 已并入本文件；兼容入口保留在 [agent.md](agent.md)。）

## 核心原则

- 可靠性优先于功能速度。
- PostgreSQL 是唯一权威；Redis / Kafka 不承载资金事实。
- 所有资金操作必须幂等（以数据库 UNIQUE 约束为准，不依赖应用层判断）。
- 状态迁移必须显式。
- 不在 HTTP 处理器内执行不可逆资金动作——交给 worker 异步执行。
- 私钥只存在于 Signer 进程，绝不暴露给业务服务。
- 链数据必须容忍重试与重复投递；处理链重组时必须保持 canonical 正确性。
- 并发敏感路径必须有测试；失败路径是一等公民。

## Go

- I/O 边界都接收 `context.Context`。
- 返回错误，业务代码不 panic。
- 小接口、显式依赖、无全局状态。
- 数据库使用 `pgx`。
- 金额 / 代币值不用 `float64`；EVM 数量用 `big.Int`，地址 / 哈希用 go-ethereum 类型。

## 数据库

- 状态变更在 PostgreSQL 事务内完成。
- 游标推进与索引数据写入必须原子。
- 幂等依赖数据库级 UNIQUE 约束。

## 链索引

必须支持：可续跑同步、重复处理、RPC 重试、链重组、确认跟踪。不要假设 websocket 订阅不丢事件。

## 测试

- 按影响验收：纯文档 / 注释改动不需要 `go test`；Go 代码改动至少 `make test`、`make test-race`、
  `make lint`、`make build`，并按影响面追加相关集成层（命令以 [Makefile](Makefile) 为准）。
- 关键行为与失败路径必须有测试；不得用 mock 替代真实中间件 / 链的验收。
- 未运行的层如实说明，不伪造全层通过。

## Git

提交信息沿用 Conventional Commits 风格，例如：

- `feat(indexer): add resumable block scanning`
- `fix(indexer): handle duplicate logs`
- `test(reorg): cover two-block chain reorganization`

## 开发流程与 Spec Kit

- 默认直接实现：短方案 / 范围确认后进入实现，并按影响面测试与审查；已定位缺陷、局部重构、补测试、
  文档 / 配置 / 仓库卫生 / 提示词维护默认走此路径。业务代码、改动文件数或工作量本身，都不构成强制
  Spec Kit 的理由。
- 已有有效规格覆盖的增量变更走复用路径：复用既有产物（`.specify/` 与 `.opencode/commands/`），
  只按需维护受影响的 spec / plan / tasks，不整体重新生成、不机械执行所有命令；直接实现同样不得让
  文档契约与实现悄悄失配。
- Spec Kit 按需选用（opt-in）：仅当明确要求，或新增跨模块功能、重大需求歧义、多阶段长期规格维护能
  明显受益时才选用，并简述理由与最小所需阶段；存在 `.specify/` 或相关命令，或项目未初始化 / 缺扩展，
  都不自动触发 SDD，也不阻塞其他路径。
- 高风险不等于必须完整 Spec Kit：以强化设计、测试、审查与必要审批处理；资金 / 安全不变量与既有测试
  要求始终保留。
- 不静默扩大改动范围；涉及架构或安全不变量的取舍先提出再实施。
