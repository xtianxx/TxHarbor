# TxHarbor 项目背景（2026-09-17 快照）

生产级 EVM 钱包与交易基础设施，monorepo：API / Indexer / Worker / Signer /
PostgreSQL / Redis / Kafka / EVM RPC。详见 `agent.md` 与
`.specify/memory/constitution.md`（v1.1.0，2026-09-12 批准）。

## 阶段状态

| 阶段 | 状态 |
| --- | --- |
| 001–009 及 PB（012-007-authorization-carrier） | 已合并至 main（009 PR#12 `295c49d`，main CI `35192040030` 四项全绿） |
| 010 transaction-lifecycle | specify（`7d2022b`）与 clarify（`bd59754`，三项业务澄清已闭合）已完成；plan 尚未执行；分支 `010-transaction-lifecycle` |
| 011 | 尚未启动；无规格、无分支 |
| 010/011 并行 | 限定例外见 `docs/workflow-010-011-parallel.md`（2026-09-17 用户决定）；008/009 例外保持不变 |

## 实际协作文件索引

- 根指南：`agent.md`（根目录无 `AGENTS.md`）
- 章程：`.specify/memory/constitution.md`
- 单一活动 feature 指针：`.specify/feature.json`（当前 `specs/007-withdrawal-creation`；
  每次 specify 会覆盖；下游命令无显式目录时以它定位）
- 工作流配置：`.specify/workflows/workflow-registry.json`、
  `.specify/extensions.yml`（`before_specify` 自动建分支，强制启用）、
  `.opencode/commands/speckit.*.md`、`.specify/init-options.json`（sequential 编号）
- 工作目录机制先例：`.slim/worktrees.json`（worktree lane）
- 本地测试资源：`compose.yaml`（PostgreSQL `127.0.0.1:5432`、Anvil `127.0.0.1:8545`、
  单 `pgdata` 卷；并行工作目录共享这些资源，须另行协调）

## 并行开发规则

008/009 并行开发的例外、约束与待定事项见
`docs/workflow-008-009-parallel.md`（历史记录，保持不变）。
010/011 限定并行规则与共同契约登记见
`docs/workflow-010-011-parallel.md`。本文件只记录状态，不裁决业务问题。
