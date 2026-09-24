# TxHarbor

生产取向的 EVM 钱包与交易基础设施（monorepo，单一 Go 二进制多子命令）：
充值侧从链上索引到确认跟踪与重组恢复，提现侧从受认证接收、逐笔授权、nonce 预留、
隔离签名到交易生命周期与执行 worker。PostgreSQL 是唯一事实来源；私钥只存在于
Signer；不可逆的资金动作不在 HTTP 处理器内执行。

## 定位与价值

- **充值链**：持续索引区块头与白名单 ERC-20 Transfer 日志，识别充值观察记录，
  按确认深度转 Confirmed，链重组时转 Orphaned 并恢复重算。
- **提现链**：接收（receive-only）→ 逐笔授权校验与绑定 → nonce 预留/绑定 →
  隔离签名 → 广播/替换/回执验证 → 执行状态投影与恢复追踪。
- **可靠性优先**：状态变更全部显式化；幂等由数据库 UNIQUE 约束保证；
  崩溃与重复投递是一等公民（崩溃点测试、租约/心跳、持久化暂停）。
- **可观测**：`/livez`、`/readyz`、`/metrics`（Prometheus）随进程暴露。

## 001～013 核心能力表（含 012 PB）

| 阶段 | 核心能力 | 关键落点 |
| --- | --- | --- |
| 001 Project Foundation | 环境变量唯一配置载体、serve/migrate 子命令、健康与指标、迁移版本兼容门禁 | `internal/config`、`internal/app/serve.go`、`internal/db/migrate.go` |
| 002 Chain Indexer | 区块头按高度连续同步、可恢复、chain_id 校验 | `migrations/000002_chain_indexer.sql` |
| 003 Event Indexing | 白名单 ERC-20 Transfer 原始日志索引（冻结配置身份） | `migrations/000003_event_indexing.sql` |
| 004 Deposit Detection | 充值观察识别与状态机（Pending/Confirmed/Orphaned） | `migrations/000004_deposit_detection.sql` |
| 005 Confirmation Tracking | 确认深度跟踪（`max(0, canonical_tip - block_number + 1) ≥ N`），阈值版本与重判 | `migrations/000005_confirmation_tracking.sql` |
| 006 Reorg Recovery | 分叉检测、共同祖先查找、旧分叉失效、Orphaned 后重索引、崩溃续跑 | `migrations/000006_reorg_recovery.sql` |
| 007 Withdrawal Creation & Query（Receive-Only） | 受认证、授权的幂等提现接收与查询；`POST /withdrawals`、`GET /withdrawals/{id}`；接收≠执行 | `migrations/000007_withdrawal_creation.sql`、`internal/withdrawal` |
| 008 Nonce Manager | 并发安全的 nonce 预留、持久绑定与恢复对账；只读绑定查询 `/nonce/bindings/` | `migrations/000008_nonce_manager.sql`、`internal/nonce/readapi.go` |
| 009 Signer Service | 签名隔离服务（独立监听器）；业务进程不接触私钥 | `migrations/000009_signer_service.sql`、`internal/signer` |
| 010 Transaction Lifecycle | 交易构造、调用 009 签名、广播、同字节重播、费用替换、回执与预期 Transfer 验证、重组后追踪 | `migrations/000011_tx_lifecycle.sql`、`migrations/000013`/`000014`（intent-FK） |
| 011 Withdrawal Executor | 执行准入、稳定付款意图、领取/续租、消费受控授权、状态投影、恢复追踪；生产进程入口 `withdrawal-worker` | `migrations/000012_withdrawal_execution.sql`、`internal/jointwire` |
| 012 PB（007 Authorization Carrier Supplement） | 授权 scope 载体：grant+scope 受控供给、scope 身份/内容/版本/费用/用途与撤销一致性，供 009 只读消费 | `migrations/000010_withdrawal_authorization_scopes.sql`、`specs/012-007-authorization-carrier` |
| 013 Reliable Event Infrastructure | 事务性 Outbox（至少一次投递 + 消费幂等，不宣称跨系统恰好一次）、Redis 非权威缓存与 fail-closed 限流（PD-1）、容量保护与恢复追赶（PD-2）、重组修订事件、五态故障矩阵演练与 PG-only vs 全栈对照基准 | `migrations/000015_event_infrastructure.sql`、`internal/events`、`internal/cache`、`internal/ratelimit`、`internal/faultdrill`、`internal/perf` |

迁移序号与阶段对应：000001–000009 依次对应 001–009；000010 对应 012 PB；
000011 对应 010；000012 对应 011；000013/000014 为 010 的 intent-FK 跟进与修复；
000015 对应 013 事件基础设施（分支 `013-reliable-event-infrastructure`，未合并/未部署）。

## 架构与选型

**单二进制，多进程角色**（`cmd/txharbor/main.go`）：

| 进程 | 子命令 | 职责 |
| --- | --- | --- |
| 业务进程 | `serve` | 单 HTTP 监听器；五条流（header/log/deposit/confirm/recovery）在同一个租约循环与心跳下运行（`indexer.RunQuatroPlusRecovery`）；挂载 007/008/011 路由与健康/指标 |
| 签名进程 | `signer-serve` | 009 独立监听器（默认 `127.0.0.1:8091`）；仅 `development` 模式的本地密钥 provider，`production` 模式启动即拒绝 |
| 执行进程 | `withdrawal-worker` | 011 worker；由 `jointwire.Worker` 装配 010 Store + 008 绑定观察/分配器 + 009 签名客户端；任一必需件缺失即拒绝启动 |
| 事件进程（013，分支） | `event-publisher` / `event-consumer` | 事务性 Outbox 发布器（租约领取、至少一次投递、blocked 可见）与幂等消费者（inbox/版本守卫/持久进度/隔离与重放）；PG-only 基线下不启动 |

`serve` 的路由（同一监听器）：

- `/withdrawals`、`/withdrawals/` —— 007 创建与查询（Bearer 认证）
- `POST /withdrawals/{request_id}/execution`、`GET .../execution` —— 011 执行
- `/nonce/bindings/` —— 008 只读绑定查询（Bearer，未配置 token 时一律拒绝）
- `GET /livez`、`GET /readyz`、`GET /metrics` —— 健康与指标

**启动门禁（fail closed）**：数据库版本不兼容（存在未应用迁移或未知/更新版本）时
`serve` 拒绝启动且**从不自动迁移**（`db.CheckCompatibility`，需先运行
`txharbor migrate up`）；chain_id 不匹配、冻结配置身份与持久行不一致、008 重建
校验门未开等，均拒绝启动或拒绝读写。

## 技术栈（以 go.mod 为准）

| 组件 | 版本/说明 |
| --- | --- |
| Go | 1.26.5 |
| go-ethereum | v1.17.5（EVM 类型、RPC、签名工具） |
| pgx | v5.11.0（PostgreSQL 驱动与连接池） |
| goose | v3.28.0（迁移；embed 到二进制） |
| prometheus/client_golang | v1.24.1（指标） |
| testcontainers-go（+ postgres 模块） | v0.44.0（集成测试） |
| moby/moby/api | v1.55.0（testcontainers 相关） |
| franz-go（+ kadm） | v1.22.0 / v1.19.0（013：纯 Go Kafka 客户端、幂等 producer；测试显式建 topic） |
| go-redis/v9 | v9.22.0（013：非权威缓存与分布式限流载体） |
| testcontainers-go redis/kafka 模块 | v0.44.0（013：Integration-Redis/Kafka 层） |

> **Redis / Kafka（013，分支实现）**：013 在分支 `013-reliable-event-infrastructure`
> 引入 Redis（非权威缓存 + fail-closed 限流）与 Kafka（事务性 Outbox 投递），
> 依赖与镜像 tag 已固定并记录（`go.mod`、`compose.yaml`、`docs/evidence/013/`）。
> 该分支**未合并/未部署**；PG 仍是唯一权威，Redis/Kafka 故障不改变门禁与资金状态。
> 生产阈值（容量/限流/告警/追赶窗口）仍为「待测/待裁决」。

## 充值链与提现链

**充值链（索引 → 确认）**

1. 002 区块头按高度连续同步，校验 `chain_id`。
2. 003 白名单 ERC-20 Transfer 日志索引；启动时比对冻结配置身份（起始高度 + 白名单哈希）。
3. 004 从已持久化日志识别转入受监控地址的充值，形成观察记录。
4. 005 达到确认深度 N 后由 Pending 转 Confirmed；阈值切换触发重判，非 canonical 拒绝确认。
5. 006 检测重组：在配置最大深度内找共同祖先，旧分叉失效、受影响充值转 Orphaned，
   重新索引/识别/计算确认；恢复过程可崩溃续跑、重复执行幂等。

**提现链（接收 → 执行）**

1. 007 接收：Bearer 认证 + 固定权限 + 逐笔授权校验，幂等持久化（caller_id + idempotency_key）；
   仅接收，不分配 nonce、不签名、不广播。
2. 012 PB：上游通过受控入口供给 grant+scope（`txharbor withdrawal-authz supply`），
   记录 scope 身份/内容/版本/费用/用途；撤销为带外 CLI（`withdrawal-authz revoke`），
   不暴露 HTTP 写入口。
3. 008 为付款意图预留并持久绑定 nonce；结果未知/缺口由对账处置。
4. 009 在隔离进程内完成签名；业务进程只持有签名结果，不接触私钥。
5. 010 构造/广播交易，处理同字节重播、费用替换、结果未知对账、回执与预期 Transfer 验证。
6. 011 worker 领取（租约+续租）、消费受控授权、调用 010 执行、投影请求状态并持续追踪
   （completed 之后仍保持追踪），重组后修订与恢复。

## 本地配置与快速启动

配置为 **env-only**（无配置文件、无第三方配置库）。`.env.example` 覆盖 001–005
的变量；`internal/config/config.go` 共 81 个变量（含 013 的 events/Redis/Kafka/
容量/限流键；阈值一律「初始值，测量后校准」），006–013 的变量需按对应 spec 自行补齐。

```bash
cp .env.example .env
# 编辑 .env 后加载（占位符示例）：
#   TXHARBOR_PG_DSN=postgres://txharbor:txharbor@127.0.0.1:5432/txharbor?sslmode=disable
#   TXHARBOR_RPC_URL=http://127.0.0.1:8545
#   TXHARBOR_CHAIN_ID=31337
set -a; source .env; set +a

docker compose up -d            # 启动 PostgreSQL(5432) 与 Anvil(8545)，仅绑定 127.0.0.1
go run ./cmd/txharbor migrate up   # 应用迁移；serve 不会自动迁移
```

三个进程示例（各自终端；端口与 DSN 为占位符，按需替换）：

```bash
# 终端 A：业务进程（001–008 读路径 + 011 执行路由）
# .env.example 覆盖 001–005；006 起需自行补齐，例如
# TXHARBOR_REORG_MAX_DEPTH（006，必填无默认）、
# TXHARBOR_NONCE_READ_TOKEN（008，缺省则读端点拒绝一切请求）
go run ./cmd/txharbor serve

# 终端 B：009 签名服务（独立监听器，默认 127.0.0.1:8091）
TXHARBOR_SIGNER_MODE=development \
TXHARBOR_SIGNER_KEY_FILE=/path/to/dev-key.hex \
go run ./cmd/txharbor signer-serve

# 终端 C：011 执行 worker（010/008 联合装配；缺件即拒绝启动）
TXHARBOR_TX_SIGNER_URL=http://127.0.0.1:8091 \
TXHARBOR_TX_SIGNER_CREDENTIAL=<signer-credential> \
go run ./cmd/txharbor withdrawal-worker
```

013 事件通道（可选，分支）：`docker compose --profile events up -d` 启动 Redis/Kafka
（显式创建 `txharbor.events.v1`，关闭 auto-create）；配置 `TXHARBOR_EVENTS_ENABLED=true`
与对应批次/租约/退避键后运行 `go run ./cmd/txharbor event-publisher` 与
`go run ./cmd/txharbor event-consumer`。默认 PG-only 基线（仅 PG+Anvil）不启动
发布器/消费者，业务按 PG 权威继续。

其他子命令：`migrate status`、`confirm-auth`、`withdrawal-authz`、`apikey-auth`、
`signer-auth`、`nonce-admin`、`withdrawal-exec`、`events-admin`（013：bootstrap-export/
replay/unblock/retention_prune，PD-4 审计）、`help`。

## PG / Anvil / Signer 运行关系

- **PostgreSQL**：`compose.yaml`（project `txharbor`）`postgres:18.6-trixie`，
  `127.0.0.1:5432`，数据在 `pgdata` 卷；Go 进程跑在宿主。
- **Anvil**：`ghcr.io/foundry-rs/foundry:v1.8.1`，`127.0.0.1:8545`，`--chain-id 31337`；
  仅供本地链交互与测试。
- **Signer**：不在 compose 中，是 Go 进程 `signer-serve`（默认 `127.0.0.1:8091`）；
  仅 `development` 模式可加载本地密钥文件，`production` 模式 fail closed（无 KMS/HSM provider）。
- **009 隔离叠加**（独立库/端口/卷，不触碰共享 `pgdata`）：

  ```bash
  docker compose -f compose.yaml -f compose.009.yaml up -d postgres
  # project txharbor-009；库 txharbor_009；宿主端口 127.0.0.1:5433
  # 注意：compose 叠加合并会同时保留 compose.yaml 的 127.0.0.1:5432 映射；
  # 执行前请先停掉默认项目，连接请使用 5433
  ```

## 测试入口

`Makefile` 原文（节选）：

```make
test:                   go test -count=1 -timeout 5m ./...
test-race:              go test -race -count=1 -timeout 10m ./...
test-integration:       go test -tags integration -count=1 -timeout 20m ./...
test-integration-redis: go test -tags integration_redis -count=1 -timeout 20m ./...
test-integration-kafka: go test -tags integration_kafka -count=1 -timeout 30m ./...
test-contract:          go test -tags contract -count=1 -timeout 10m ./...
test-e2e:               go test -tags e2e -count=1 -timeout 30m ./...
test-fault:             go test -tags fault -count=1 -timeout 60m ./...
test-perf:              go test -tags perf -count=1 -timeout 60m ./...
```

`lint`（`gofmt -l` 检查 + `go vet`，覆盖 unit 与 integration 两个标签）、
`build`（`go build ./...`）、`db-reset`（`docker compose down -v`）见 `Makefile`。

- `make test` / `make test-race`：单元测试，无需 Docker。
- `make test-integration`（PostgreSQL）、`test-integration-redis`、`test-integration-kafka`：
  testcontainers 集成测试，**需要 Docker daemon**；各层按组件独立运行。
- `make test-contract`：事件信封/目录/schema 版本/消费者兼容矩阵，纯 Go、无 Docker。
- `make test-e2e`：核心充提全链（全栈 + Anvil）。
- `make test-fault` / `make test-perf`：五态故障矩阵与对照基准，**独立运行、不进普通 PR**
  （定时/手动/发布前门禁；触发矩阵见 `.github/workflows/` 与
  `specs/013-reliable-event-infrastructure/verification.md` §3–§4）。
- 013 分层入口自带 `require_tagged_tests` 守卫：无 tag 测试时该层报 NOT RUN 并非零退出，
  不允许空层冒充通过。
- 关键失败路径测试包括：`internal/txlifecycle/crash_integration_test.go`（8 个
  崩溃边界硬杀子进程）、`internal/signer/restart_integration_test.go`（签名服务重启）、
  健康 readyz 翻转与同链 RPC 故障注入等。

## 故障恢复与安全设计

- **事实来源**：PostgreSQL 是唯一事实来源；不存在 Redis/Kafka 财务状态。
- **幂等**：金融操作靠数据库 UNIQUE 约束去重，不依赖应用层检查。
- **显式状态**：所有状态迁移显式；不可逆资金动作不在 HTTP 处理器内执行。
- **重组正确性**：以 canonical 链为准；恢复状态持久化，重复执行幂等，旧 worker 由持久化版本隔离。
- **崩溃恢复**：单一租约循环 + 心跳覆盖五条流；崩溃点与重启场景有集成测试。
- **签名隔离**：私钥只存在于 Signer 进程；`KeyProvider` 接口不暴露 digest 签名面；
  `production` 模式无 provider 即拒绝启动。
- **凭据**：007/008/009 三套 Bearer 凭据独立；凭据不进入日志/错误/指标，
  启动回显经 `internal/logx.Redact` 脱敏；008 未配置 token 时拒绝一切读取。
- **撤销带外**：授权撤销是受控 CLI 操作（`withdrawal-authz revoke`），不是 HTTP 端点。
  注：Serve/Signer/Worker 及管理 CLI 共用同一 `TXHARBOR_PG_DSN`，无数据库角色隔离；
  Revoke 隔离指可信管理 CLI 入口与业务 HTTP 调用路径分离；DSN 泄漏影响不限于未接收授权。
- **可观测**：`/metrics`（Prometheus）暴露执行、确认、恢复等关键计数。

## 限制与 T000-P 状态

- **T000-P 保持 OPEN**（未部署，不宣称生产就绪）：
  `docs/project-context.md:13`、`specs/003-event-indexing/tasks.md:24`、
  `specs/005-confirmation-tracking/tasks.md:25`、
  `specs/004-deposit-detection/acceptance.md:56-57`。
  A-13（全链联合验收门）已 CLOSED（`specs/011-withdrawal-executor/integration-readiness.md:348`），
  但 **T000-P 独立保持 OPEN**（同文件 `:350`）。
- **本地验收 ≠ 生产就绪**：各阶段验收均限定本地范围；生产接入需先关闭 T000-P
  与上游生产 provider 附录（003 E1），`LOG_BATCH_BLOCKS` 等生产值不得宣称已复核。
- **无 KMS/HSM**：v1 只有本地 development 密钥 provider；
  `internal/signer/provider.go:73-76` 生产模式拒绝启动，选型见
  `specs/009-signer-service/research.md:76,89`、`plan.md:365`（延后项）。
- **无 TLS**：`serve` 使用 `net.Listen` 明文监听（`internal/app/serve.go:367`），
  生产需自行前置 TLS 终止/HTTPS。
- **无 Dockerfile**：Go 进程当前在宿主运行，compose 只提供 PostgreSQL、Anvil 与
  （013 `events` profile）Redis/Kafka 依赖。
- **013（分支）状态**：实现与本地验收在分支 `013-reliable-event-infrastructure`
  完成（90/90 任务勾选；证据索引 `docs/evidence/013/quickstart_evidence_index.md`）；
  **未合并/未部署，远程 CI 运行待核验**；投递语义为「至少一次 + 消费幂等」，
  **不宣称跨系统恰好一次**；容量/限流/告警/追赶阈值待测/待裁决
  （`docs/evidence/013/benchmark_report.md` §9）；对照基准为本地单主机观测，
  不构成生产容量结论；Kafka 范围按 PD-3 保持、不自动调整。
