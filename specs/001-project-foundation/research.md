# Research: 001-project-foundation

**Date**: 2026-09-12 | **Spec**: `specs/001-project-foundation/spec.md`
**Method**: 两条并行外部研究（Go 依赖 / 本地镜像与编排，版本号经官方源实时核对）+ 本地工具链实测（go 1.26.5，compose v5.3.1）。

## 版本总览

| 依赖 | 选定版本 | 依据 |
|---|---|---|
| Go toolchain | 1.26.5（与 go.mod 一致，本地已验证） | 仓库既定 |
| pressly/goose | v3.28.0 | 见 §1 |
| jackc/pgx (v5) | v5.11.0 | 见 §2 |
| ethereum/go-ethereum | v1.17.5 | 见 §3 |
| prometheus/client_golang | v1.24.1 | 见 §4 |
| testcontainers-go（仅测试） | v0.44.0（必须 pin，v0.x API 未稳定） | 见 §5 |
| postgres 镜像 | `postgres:18.6-trixie` | 见 §6 |
| foundry/Anvil 镜像 | `ghcr.io/foundry-rs/foundry:v1.8.1` | 见 §7 |

## 1. 迁移工具

- **Decision**: `pressly/goose v3.28.0`，library 嵌入（`goose.NewProvider` + `goose.WithMigrations(fs)`），显式开启 `WithSessionLocker`（Postgres session advisory lock；长迁移可选 v3.28 新增表租约锁）。
- **Rationale**: 失败语义契合 FR-007：goose 每个 migration 单事务，版本插入与 DDL 同事务，失败回滚后版本表无该版本行，直接重跑。并发：显式 session locker 后串行互斥，崩溃时连接断开自动释锁，满足 FR-006。
- **Alternatives considered**: `golang-migrate v4.20.1` —— 生态广、含 pgx5 原生驱动，但失败先单独提交 `dirty=true` 版本行，之后所有迁移返回 `ErrDirty` 需人工 `force`，违反“失败可重试”，拒绝。手写 runner —— 重造锁+版本表，拒绝。goose v4 —— 仅 dev 伪版本，不用。
- **约束**: 禁止 `NO TRANSACTION` 迁移（失原子性，失败留半成品且不记版本）。

## 2. 数据库访问

- **Decision**: `jackc/pgx/v5 v5.11.0` + `pgxpool`；`ParseConfig` 后 `NewWithConfig`，创建后立即 `Ping` 验连通；每次调用独立 `context.WithTimeout`。
- **Rationale**: constitution/ agent.md 指定 pgx；pool 默认 `MaxConns=max(4,NumCPU)`，本地单实例显式设 8–16，`MinConns` 1–2，`MaxConnLifetime` 1h+jitter，`HealthCheckPeriod` 1m；`pgxpool.New` 不建连，必须 Ping。goose 等 `database/sql` 消费者经 `pgx/v5/stdlib` 另开连接。
- **Alternatives considered**: lib/pq —— 事实停更，拒绝。纯 `database/sql` —— 应用层直接 pgxpool 更好，仅 goose 侧用 stdlib 桥。

## 3. EVM RPC 客户端

- **Decision**: `go-ethereum v1.17.5`，`ethclient.DialContext` + 自带超时的 `ChainID(ctx)` 启动校验（`id.Cmp(expected)`），运行期每次调用独立 ctx 超时，退出时 `Client.Close()`。
- **Rationale**: `DialContext` 的 ctx 只覆盖握手，后续调用必须各自包超时；`ChainID` 即 `eth_chainId`， mismatch 即拒绝就绪（FR-010）。启动探针可附带 `BlockNumber`。
- **Alternatives considered**: `simulated` 后端 —— 仅单元测试用，不验证真实 RPC 行为。`httptest` 假 JSON-RPC —— 测超时/错误分支，零容器。
- **许可证（已按上游仓库核实，非按项目名推断）**: 001 实际引用的 `ethclient`、`rpc` 包均位于上游 `cmd/` 目录之外，适用 `COPYING.LESSER`（LGPL-3.0）；GPL-3.0（`COPYING`）仅覆盖 `cmd/` 内二进制。依据：上游 README/仓库根 `COPYING` 与 `COPYING.LESSER` 双文件。实现阶段由 T040 按所用版本复核并记录分发策略。

## 4. /metrics 最小实现

- **Decision**: `client_golang v1.24.1` 最小接法：默认 registry（自带 process/go collector）+ 3 个自定义指标：就绪 `GaugeFunc`、DB/RPC 探测 `CounterVec{dep,result}`；`/readyz` 另返回 JSON（200/503）供人/LB 使用。
- **Rationale**: 一行 `promhttp.Handler()` 即有进程指标； exposition 格式/转义/gzip 由库维护；无业务指标故无基数风险。
- **Alternatives considered**: 标准库手写纯文本端 —— 零依赖但自理格式与 `/proc` 解析（Linux-only），未来接 Prometheus 重写，不划算，拒绝。expvar —— 非 Prometheus 生态，拒绝。

## 5. 测试策略与依赖

- **Decision**: 分三层 —— unit（默认 `go test -short`，无 Docker：配置校验、迁移文件embed校验、就绪聚合fake Prober、ChainID比对经httptest、退出逻辑）→ integration（`//go:build integration` + testcontainers-go v0.44.0：PG官方模块 + Anvil经GenericContainer固定tag、`wait.ForLog("Listening on")`，覆盖真迁移/并发锁/失败重试/探针翻转）→ e2e smoke（opt-in环境变量指向既有环境，不进默认CI）。
- **Rationale**: 并发迁移互斥、优雅退出等行为必须跨进程/真依赖验证；testcontainers 自带生命周期、端口随机、状态隔离，CI 确定；外部 compose 常驻模式并行性差、易 flaky。
- **Alternatives considered**: 外部 compose 就绪模式 —— 仅作本地热迭代 fallback。dockertest —— 维护弱于 testcontainers。Anvil 官方 testcontainers 模块 —— 不存在，用 GenericContainer（有生产先例）。
- **命令**: `go test ./...`（unit，含 `-race` 对并发敏感包）、`go test -tags integration ./...`（CI 门禁，需 Docker）。

## 6. PostgreSQL 镜像与 Compose

- **Decision**: `postgres:18.6-trixie` 精确 pin；命名卷挂 `/var/lib/postgresql`（PG18 新路径）；Compose 内 healthcheck `pg_isready -U $U -d $DB -h 127.0.0.1`（TCP，防 init 期 socket 误报），`interval 10s/timeout 5s/retries 5/start_period 30s`；端口仅绑 `127.0.0.1:5432`；日常 `up/down` 保留数据，清库只用独立 `down -v`（或 Czy `make db-reset`）。
- **Rationale**: 18.6 为当前稳定 minor（19 仍 beta）；禁止 `latest`（19 GA 会跳大版本）；PG18 `PGDATA` 路径变更，旧挂载路径失效。
- **Alternatives considered**: `18-trixie` 浮动吃补丁 —— 可接受折中，默认仍精确 pin。alpine 变体 —— 扩展生态弱。external 卷 —— 最保险但多一步初始化。

## 7. Anvil 镜像与 Compose

- **Decision**: `ghcr.io/foundry-rs/foundry:v1.8.1`；`anvil --host 0.0.0.0 --port 8545 --chain-id 31337`，保持默认 automine；healthcheck 用镜像自带 cast：`["CMD","cast","chain-id","--rpc-url","http://localhost:8545"]`（interval 2s/timeout 5s/retries 10）；端口仅绑 `127.0.0.1:8545`。
- **Rationale**: 官方镜像与文档推荐；`--host 0.0.0.0` 必需（默认只绑容器内 loopback）；默认 chain-id 31337 与 Go 侧期望断言一致；automine 下交易即时出块，测试确定性最好；Anvil 纯内存，容器重启即全新链，与 PG 保留策略互补。
- **Alternatives considered**: `:stable` 浮动 —— 重启可能换版本。`--block-time 1` —— 需 Go 侧轮询 receipt，本阶段不需要。

## 8. 结构性决策（无需外部研究）

- **单二进制双子命令**：`txharbor serve` / `txharbor migrate [up|status]`，同一模块，迁移命令为 thin main 调 goose Provider。
- **配置加载**：标准库 `os.LookupEnv` + 小校验函数，不引入 viper/envconfig（env-only，项少， ladder  rung-3）。
- **HTTP 服务**：标准库 `net/http` ServeMux，不引入框架（仅三个端点）。
- **超时间隔换算**（满足 10s 感知/恢复界限）：探针 interval 2s + 单次超时 5s → 最坏感知约 7s < 10s；恢复同理。启动预算 30s 内完成：顺序链为配置→PG Ping→版本校验→RPC ChainID，每步 5s 超时，总和远小于预算。
- **日志脱敏**：连接串/口令/令牌字段在输出前替换为 `[REDACTED]`，保留主机/端口/库名等非敏感结构信息；脱敏单测覆盖。

skipped: Redis/Kafka/Prometheus 部署、业务表、签名/随机数 —— 001 范围外，出现需求再议。
