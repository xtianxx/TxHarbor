# 贡献指南

欢迎文档改进、缺陷报告与代码贡献。仓库处于开发与验证阶段，未生产部署（见 [README.md](README.md) 的项目状态说明）。

## 开发环境

- 依赖：Go 1.26.5、Docker + Docker Compose、bash 或 zsh；健康检查用 `curl`。
- 启动步骤见 [README.md](README.md) 的「快速开始」；全部环境变量与默认值见
  [docs/configuration.md](docs/configuration.md)。
- 本地依赖服务（PostgreSQL / Anvil，可选 Redis / Kafka）由 [compose.yaml](compose.yaml) 提供。

## 最小贡献流程

1. 从 `main` 切出主题分支（不要直接向 `main` 推送）。
2. 改动尽量聚焦单一主题；提交信息沿用仓库既有风格
   （`docs(...)` / `fix(...)` / `feat(...)` 等 Conventional Commits 形式）。
3. 推送分支并打开 PR，写清动机、影响范围与实际执行过的验证命令。

## 按影响选择验证命令

验证命令以仓库根目录 [Makefile](Makefile) 为准：

| 改动类型 | 建议命令 |
| --- | --- |
| 文档 / 注释 | 无需构建；检查相对链接与格式，`git diff --check` |
| Go 代码 | `make test`、`make test-race`、`make lint`、`make build` |
| 契约类改动（事件信封 / schema 版本等） | 追加 `make test-contract` |
| 需要真实中间件的行为 | 相关集成层：`make test-integration`（及 Redis / Kafka 变体） |
| 充提全链行为 | `make test-e2e`（需要 Docker 与 Anvil 栈） |

未运行的层请如实说明，不要声称全层通过。

## 数据库迁移纪律

- 迁移位于 `migrations/`（goose，embed 进二进制）。
- **只能追加新迁移，不得修改已应用的历史迁移文件**；编号递增。
- `serve` 不会自动迁移：运行 `go run ./cmd/txharbor migrate up` 前先确认目标数据库。
- 破坏性命令谨慎使用：`make db-reset` 会删除数据卷且不可恢复。

## PR 与安全报告

- PR 由维护者按可用性审查；请配合审查意见迭代。
- 安全问题不要开公开 issue：请走 [SECURITY.md](SECURITY.md) 的私密报告入口。

## 文档与工具

- 文档入口见 [docs/README.md](docs/README.md)；各层测试怎么跑、产物写到哪里见
  [docs/testing.md](docs/testing.md)。
- `.specify/` 与 `.opencode/commands/` 是维护者的 Spec Kit 规格驱动工作流，**不是运行时依赖**；
  普通贡献（包括文档修改）无需安装或运行该工作流。
