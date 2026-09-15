# TxHarbor 项目背景（2026-09-15 快照）

生产级 EVM 钱包与交易基础设施，monorepo：API / Indexer / Worker / Signer /
PostgreSQL / Redis / Kafka / EVM RPC。详见 `agent.md` 与
`.specify/memory/constitution.md`（v1.1.0，2026-09-12 批准）。

## 阶段状态

| 阶段 | 状态 |
| --- | --- |
| 001–006 | 已合并至 main |
| 007 withdrawal-creation | 已合并（`19fa11e`，main CI 四项全绿）；`requirements` 21/22 真实状态；T000-P 独立 open |
| 008 nonce-manager | 待 specify；本地分支 `008-nonce-management` 存在但无新提交；无 `specs/008*` |
| 009 signer-service | 待 specify；无分支；无 `specs/009*` |
| 010、011 | 不在本次并行例外内，仍严格串行，等待后续指令 |

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
`docs/workflow-008-009-parallel.md`。本文件只记录状态，不裁决业务问题。
