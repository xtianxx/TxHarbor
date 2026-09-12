# Implementation Plan: Project Foundation

**Branch**: `feat/001-project-foundation` | **Date**: 2026-09-12 | **Spec**: `specs/001-project-foundation/spec.md`

**Input**: Feature specification from `specs/001-project-foundation/spec.md`（含 Session 2026-09-12 五条澄清）

**Note**: 本次只生成技术设计，不编写实现代码（tasks/实现另行安排）。

## Summary

构建 TxHarbor 可本地启动的工程底座：env-only 配置校验 → goose 独立迁移命令（DB 层互斥串行）→ `serve` 启动链（PG Ping → 版本兼容 → RPC chain-id）→ 分离的 `/livez` `/readyz` + 最小 `/metrics` → 信号驱动的优雅退出。技术路线见 `research.md`：goose v3.28.0、pgx v5.11.0、go-ethereum v1.17.5、client_golang v1.24.1、Compose 起 PG 18.6 + Anvil v1.8.1、宿主机跑 Go 单二进制（`serve`/`migrate` 双子命令）、标准库 HTTP、无框架。

## Technical Context

**Language/Version**: Go 1.26.5（与 go.mod 一致，本地工具链已验证）

**Primary Dependencies**: `pressly/goose v3.28.0`（迁移，显式 SessionLocker）、`jackc/pgx/v5 v5.11.0`（pgxpool 数据访问）、`ethereum/go-ethereum v1.17.5`（ethclient ChainID 校验）、`prometheus/client_golang v1.24.1`（/metrics 最小集）；测试仅 `testcontainers-go v0.44.0`（pin，v0.x）。其余标准库：`net/http`（探针服务）、`os`（env 加载）、`database/sql` 仅作 goose 桥接（`pgx/v5/stdlib`）。

**Storage**: PostgreSQL 18（镜像 `postgres:18.6-trixie`，命名卷 `/var/lib/postgresql`）；唯一底座表为 goose 自建 `goose_db_version`；无业务表。

**Testing**: `go test ./...`（unit，默认 `-short`，无 Docker；并发敏感包加 `-race`）→ `go test -tags integration ./...`（testcontainers：PG 官方模块 + Anvil GenericContainer；CI 门禁，需 Docker）→ e2e smoke（opt-in 环境变量，不进默认 CI）。覆盖矩阵见 quickstart §3/§4（七类验收 + /metrics + 并发迁移）。

**Target Platform**: Linux 宿主机跑 Go 服务；`docker compose` v5 起 PG + Anvil（端口仅绑 `127.0.0.1`）。

**Project Type**: 后端服务，单二进制 CLI（`serve` + `migrate up|status`）。

**Performance Goals**: 验收界限（可配置，默认即验收值；计时不含镜像/依赖准备）：依赖可用时启动后 30s 内就绪、启动持续失败 30s 预算非零退出、依赖中断 10s 内转未就绪、恢复 10s 内重回就绪、终止后 15s 内退出（超时强制结束记错）。换算：探针 interval 2s + 单次 5s → 最坏感知约 7s < 10s。

**Constraints**: 配置载体仅环境变量；迁移经独立命令、启动永不自动迁移；日志凭据脱敏（`[REDACTED]`，保留非敏感结构信息）；`/metrics` 仅基础进程指标/就绪 gauge/探测计数，无业务指标、不部署 Prometheus/Grafana；禁止 `NO TRANSACTION` 迁移。

**Scale/Scope**: 本地单实例；并发仅迁移命令需互斥（DB advisory lock + 有限等待 + 超时非零退出）。001 不含区块扫描/事件/充值/提款/nonce/签名，不预建业务表与空壳接口。

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| 原则 | 001 适用性 | 结论 |
|---|---|---|
| I 金融正确性 | 无资金操作；迁移原子性（单文件单事务）+ 失败不记版本 | 通过 |
| II 幂等设计 | goose 版本表保证重复执行收敛；并发串行不重复应用 | 通过 |
| III PG 为真相源 | 版本记录在 PG；无 Redis/Kafka；内存健康态丢失可自探针重建 | 通过 |
| IV Reorg 感知 | 001 无索引逻辑，明确 out-of-scope | 通过（范围外） |
| V 显式状态机 | Lifecycle 五态 + 转换条件（data-model §4） | 通过 |
| VI 事务边界 | 迁移版本插入与 DDL 同事务；游标类问题不存在（无索引） | 通过 |
| VII Nonce | 001 无 outbound 交易 | 通过（范围外） |
| VIII 私钥隔离 | 无签名；Anvil 仅本地一次性测试链 | 通过 |
| IX 失败路径一等 | 超时/重试有界、错误分类、探针翻转自愈、退出预算 | 通过 |
| X 本地确定性测试 | Compose + testcontainers，不依赖公网测试网 | 通过 |
| XI 测不变量 | 并发迁移/失败重试/探针翻转进 integration（真依赖，非 mock） | 通过 |
| XII 可观测即正确 | 结构化日志 + 脱敏 + /livez//readyz//metrics 最小集 | 通过 |
| XIII 简单优先分布 | 标准库优先、单体单二进制、无 Redis/Kafka、无 K8s | 通过 |
| XIV 小步规格驱动 | 001 独立可测；范围锁死，无业务预建 | 通过 |
| Eng/Go | context 全 I/O 边界、有意义 error、优雅退出、无 float 金额（无金额） | 通过 |
| Eng/DB | 版本化迁移、破坏性变更需论证（001 无） | 通过 |
| Eng/RPC | 超时明确、错误分类（transport/timeout/chain-mismatch/invalid-response）、不信任违 local invariant 的响应 | 通过 |
| Quality Gates | 测试分层已定；`go test ./...` 门禁；迁移可重现；无密钥入库 | 通过 |
| Security | 无生产密钥、无密钥入库、日志脱敏、输入全校验 | 通过 |

**Post-design re-check**: 设计产物（research/data-model/contracts/quickstart）未引入新依赖面（最大新增面为测试-only 的 testcontainers），无违反。Gate 通过，无需例外。

## Project Structure

### Documentation (this feature)

```text
specs/001-project-foundation/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── cli.md           # serve/migrate 语义、退出码、迁移文件契约
│   └── http.md          # /livez /readyz /metrics 契约
├── checklists/
│   └── requirements.md  # Spec quality checklist (done, 16/16)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
cmd/txharbor/            # main：serve / migrate 子命令分发
├── main.go
internal/
├── config/              # env 加载 + 校验（stdlib os.LookupEnv）
├── db/                  # pgxpool 装配 + goose Provider（SessionLocker）
├── eth/                 # ethclient 封装：DialContext + ChainID 校验
├── health/              # Prober（db/rpc）+ 聚合 + /livez /readyz handler
├── logx/                # 凭据脱敏（[REDACTED]，保留非敏感结构信息）
├── metrics/             # /metrics 注册（ready GaugeFunc + probe CounterVec）
└── app/                 # 生命周期：启动链、探针循环、信号退出
migrations/              # goose SQL（000001_*.sql…，单事务，禁 NO TRANSACTION）
compose.yaml             # postgres:18.6-trixie + foundry:v1.8.1（healthcheck，127.0.0.1 绑定）
.env.example             # TXHARBOR_* 示例（凭据位占位符）
```

**Structure Decision**: 单体单二进制（constitution XIII），标准库 HTTP 无框架；`migrate` 为 thin main 调 goose Provider；集成测试与源码同包（`//go:build integration` 后缀文件），无需独立 tests/ 树。

## Complexity Tracking

> 无违反，无需填写。唯一接近门槛的决策（testcontainers-go v0.x 测试依赖）为测试-only 且已 pin 版本，不触及生产复杂度。
