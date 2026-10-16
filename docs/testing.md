# 测试指南

本文档说明如何在本仓库运行各层测试；命令以根目录 [Makefile](../Makefile) 为准。
历史规格中提及的运行报告与验收记录不随源码分发，也不是当前使用入口——本文只讲怎么运行。

## 前置

- Go 1.26.5（见 [go.mod](../go.mod)）。
- 容器层命令要求可用的 Docker daemon：PostgreSQL / Redis / Kafka 集成层、E2E、fault、perf、
  drill 均由测试自身（testcontainers）创建所需依赖，**不需要**手工启动服务、`events` profile
  或额外的 `TXHARBOR_*` 配置。
- Compose 栈只在手动运行服务或本地演示时需要：`docker compose up -d` 启动 PostgreSQL / Anvil；
  事件通道用 `docker compose --profile events up -d` 并设置对应 `TXHARBOR_*` 键。端口与键见
  [配置参考](configuration.md) 与根 [README.md](../README.md)。

## 常规命令

| 命令 | 用途 | 依赖 |
| --- | --- | --- |
| `make test` | 单元测试（无构建标签） | Go |
| `make test-race` | 单元测试 + race 检测 | Go |
| `make test-contract` | 契约层：事件信封 / 目录 / schema 版本 / 消费者兼容 | Go |
| `make test-integration` | PostgreSQL 集成层（真实迁移，testcontainers） | Docker |
| `make test-integration-redis` | Redis 集成层 | Docker |
| `make test-integration-kafka` | Kafka 集成层 | Docker |
| `make test-e2e` | 充提全链（全栈 + Anvil） | Docker |
| `make test-fault` | 故障注入矩阵与不变量 | Docker |
| `make test-perf` | 性能对照测量（PG-only vs 全栈） | Docker |
| `make test-drill` | 灾备演练（S1–S12 + F1–F7） | Docker（真实 PG / Anvil 由测试创建；事件场景含 Kafka）+ 宿主 `pg_dump` / `pg_restore` |
| `make smoke-quickstart` | 冷启动冒烟（脚本自建隔离 Compose 项目 / 端口 / 卷） | bash + Docker Compose + Go + `curl` |
| `make lint` | gofmt + go vet（含集成构建标签） | Go |
| `make build` | 构建全部包 | Go |

- fault / perf / drill 属独立层（调度、手动或发布门禁），不阻塞普通 PR；drill 与
  smoke-quickstart 走独立通道。
- 分层入口在缺少对应构建标签测试时报告 **NOT RUN** 并非零退出（不空跑通过）。
- 日常最小集：`make test`、`make test-race`、`make lint`、`make build`；按改动影响追加相应层
  （见 [CONTRIBUTING.md](../CONTRIBUTING.md)）。

## 运行产物位置

各命令未显式配置时的真实默认行为：

| 命令 / 工具 | 默认产物位置（未配置时） | 覆盖方式 |
| --- | --- | --- |
| `make test-drill` | 系统临时目录（`mktemp -d`），命令结束后清理，不留存 | 设置 `TXHARBOR_DRILL_EVIDENCE_DIR` 后在该目录下按 run 留存 |
| 故障注入（fault 层 harness） | 系统临时目录 | `TXHARBOR_FAULT_EVIDENCE_DIR` |
| 性能测量（perf 层 harness） | 系统临时目录 | `TXHARBOR_PERF_EVIDENCE_DIR` |
| `txharbor recovery-admin drill` | 仓库内 `docs/evidence/015/drill`（已被根 [`.gitignore`](../.gitignore) 忽略） | `--out DIR` |
| 本地演示 `scripts/portfolio-demo/run_demo.sh` | 仓库内 `.evidence/portfolio-demo/<run-id>`（已忽略） | `PORTFOLIO_RUN_ID` 决定 run 目录名 |

- 上表中 `recovery-admin drill` 与本地演示的默认输出位于仓库内，均已忽略；
  `make test-drill` 会清理其默认临时目录，不要据此假定 fault / perf 的临时产物也会自动清理。
- 运行产物可能含日志、快照与本地输入，请勿提交；敏感内容处理见 [SECURITY.md](../SECURITY.md)。

## 注意

- **`make db-reset` 会删除数据**（等价 `docker compose down -v`，移除 PostgreSQL 数据卷且不可
  恢复）——只在你确认可以丢弃数据的本地栈上使用。
- 未运行的层请如实说明，不要声称通过。
