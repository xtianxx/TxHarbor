# TxHarbor

面向 EVM / ERC-20 充值与提现流程的开发演练项目（Go monorepo，单一二进制多子命令）：
把资金正确性相关的失败场景当作一等公民来设计和验证。PostgreSQL 是唯一权威数据源；
私钥只存在于独立 Signer 进程；不可逆资金动作不在 HTTP 处理器内执行。**未生产上线**（见 §8）。

> 简历/展示材料（30 秒介绍、5 分钟演示讲稿、面试问答与证据索引）：[docs/portfolio.md](docs/portfolio.md)

## 1. 项目介绍

TxHarbor 是一个面向 EVM / ERC-20 充值与提现流程的基础设施演练项目，用一套可本地运行、
可复现的证据链展示「资金正确性」问题如何被设计、实现与验证：

- 覆盖 EVM / ERC-20 充值与提现流程：链上索引与确认、重组恢复、受认证接收、授权、
  nonce 绑定、隔离签名、交易生命周期与执行 worker。
- 重点解决**幂等、链重组、交易结果未知、崩溃恢复与可靠事件投递**——这些失败场景
  全部有一等公民级的处理与测试入口。
- 属于**开发演练项目**：未生产上线、未部署（T000-P 保持 OPEN），不提供生产 SLO。

## 2. 核心能力与技术亮点

- **充值索引与确认**（002–005）：区块头按高度连续同步并校验 `chain_id`；白名单 ERC-20
  `Transfer` 日志索引，启动时比对冻结配置身份（起始高度 + 白名单哈希）；确认深度按
  `max(0, canonical_tip − block_number + 1) ≥ N` 判定，阈值版本化、切换触发重判。
- **链重组恢复**（006）：分叉检测 → 配置深度内共同祖先 → 旧分叉失效 → 受影响充值转
  `Orphaned` → 重索引与重算确认；恢复状态持久化、可崩溃续跑、重复执行幂等；
  重组产生修订事件，由消费者幂等吸收。
- **提现接收与执行**（007–012）：Bearer 认证 + 固定权限 + 逐笔授权（grant+scope 受控供给，
  撤销为带外 CLI）；幂等由 `UNIQUE(caller_id, idempotency_key)` 保证，接收 ≠ 执行；
  执行侧领取/续租、消费受控授权、构造/广播/同字节重播/费用替换/回执与预期 `Transfer`
  验证、状态投影与 completed 后持续追踪。
- **nonce 预留与持久绑定**（008）：并发安全的预留与持久绑定、结果未知/缺口对账；
  只读绑定查询端点（未配置 token 时拒绝一切读取）。
- **隔离签名**（009）：私钥只存在于 Signer 进程，业务进程只持有签名结果；
  `production` 模式无 KMS/HSM provider 即拒绝启动。
- **结果未知与对账**（014）：交易结果未知不重付、不重广播；对账任务（启动/扫描/暂停/
  恢复/取消）、差异项认领与处置、复核票据、权限授予（`reconcile-admin`）。
- **可靠事件投递**（013）：事务性 Outbox 与业务状态同事务写入；发布器以 `SKIP LOCKED` +
  租约 + `acks=all` 至少一次投递；消费者 inbox 去重、版本守卫、持久进度、有界重试、
  持久隔离与人工重放（重放不产生新意图/nonce/签名/广播）；**不宣称跨系统恰好一次**。
- **备份恢复与故障演练**（015）：备份 → 隔离恢复验证 → 实例核验 → 按能力分级放行
  （双人批准、缺口阻塞、旧实例隔离证据、放行前 `POST /withdrawals` 必须 503）；
  五态故障矩阵（Fault 层）与灾备演练 S1–S12 + F1–F7（drill 层，逐场景 PASS 判定）。
- **性能诊断**：`perf` 构建标签接缝（普通构建 no-op）+ ON/OFF 同接缝配对 + 串行批次与
  原始归档；只做**测量与定位**（例如尾窗覆盖 99.93%、`process` 占 tail 99.09–99.28%），
  不宣称性能提升。

## 3. 架构

```mermaid
%% 权威数据只在 PostgreSQL；Redis 不在 Outbox 投递链上（Kafka 才是事件通道）
flowchart LR
  chain["EVM RPC / Anvil"]
  pg[("PostgreSQL：唯一权威")]
  redis[("Redis：非权威缓存/限流（可选）")]
  kafka[("Kafka：事件通道（可选）")]
  serve["serve：索引 002-006 + 007/008 接口 + 011 执行准入"]
  signer["signer-serve：009 私钥隔离"]
  worker["withdrawal-worker：011 执行"]
  pub["event-publisher：Outbox 发布"]
  cons["event-consumer：幂等消费"]

  chain --> serve
  serve --> pg
  pg --> worker
  worker --> signer
  worker --> chain
  pg --> pub
  pub --> kafka
  kafka --> cons
  cons --> pg
  serve -. "配置 TXHARBOR_REDIS_ADDR 时" .-> redis
```

**进程职责**（`cmd/txharbor/main.go`，同一二进制的不同子命令）：

| 进程 | 子命令 | 职责 |
| --- | --- | --- |
| 业务进程 | `serve` | 单 HTTP 监听器；索引五条流（header/log/deposit/confirm/recovery）在同一租约循环与心跳下运行；挂载 007/008 接口、011 执行准入与健康/指标 |
| 签名进程 | `signer-serve` | 009 独立监听器（默认 `127.0.0.1:8091`）；`development` 模式加载本地密钥文件，`production` 模式无 provider 即拒绝启动 |
| 执行进程 | `withdrawal-worker` | 011 worker：领取/续租、调用 010 生命周期与 009 签名、状态投影与恢复追踪；装配对必需件逐项拒绝缺失 |
| 事件发布 | `event-publisher` | 事务性 Outbox 发布器：租约领取、至少一次投递、blocked 可见可审计 |
| 事件消费 | `event-consumer` | 幂等消费者：inbox 去重、版本守卫、持久进度、隔离与人工重放 |
| 管理 CLI | `migrate` / `confirm-auth` / `apikey-auth` / `withdrawal-authz` / `nonce-admin` / `signer-auth` / `withdrawal-exec` / `events-admin` / `reconcile-admin` / `recovery-admin` | 迁移与受控运维操作（授权供给/撤销、重放、恢复核验与放行等） |

- **PostgreSQL 是唯一权威数据源**；Outbox 经 `event-publisher` 投递到 Kafka，再由
  `event-consumer` 处理；**Redis 不在 Outbox 投递链上**。
- Redis 缓存模块存在，但**默认未启用**：仅当配置 `TXHARBOR_REDIS_ADDR` 时 `serve` 才装配
  缓存与限流（与 `TXHARBOR_EVENTS_ENABLED` 开关独立）；`TXHARBOR_EVENTS_ENABLED` 默认
  `false`，发布器/消费者不启动。本 README 不声称查询缓存已启用。
- **限流失败策略**（PD-1，见 `internal/ratelimit/policy.go`）：令牌耗尽 → `429`
  （`temporarily_unavailable`，带 `Retry-After`，提示同 key 重试）；限流**不可用**
  （Redis 故障/超时/脚本错误）→ 仅新提现创建 `POST /withdrawals` 返回 `503` 明确可重试拒绝，
  查询回退 PostgreSQL 继续，执行等其它路径继续按原门禁运行——限流从不是资金门禁，
  其故障既不放松也不收紧原门禁；类未配置 → fail-closed 报错，绝不静默放行。
- **启动门禁**：数据库版本不兼容（存在未应用或未知迁移）时拒绝启动且**从不自动迁移**；
  `chain_id` 不匹配、冻结配置身份与持久行不一致等同样拒绝启动。

## 4. 技术栈（版本以 `go.mod` / `compose.yaml` 为准）

| 组件 | 版本 | 用途 |
| --- | --- | --- |
| Go | 1.26.5 | 单一二进制、多子命令 |
| PostgreSQL | `postgres:18.6-trixie` | 唯一权威数据源；迁移经 goose |
| pgx | v5.11.0 | PostgreSQL 驱动与连接池 |
| goose | v3.28.0 | 迁移（embed 进二进制） |
| go-ethereum | v1.17.5 | EVM 类型、RPC、签名工具 |
| franz-go（+ kadm） | v1.22.0 / v1.19.0 | Kafka 客户端与显式建 topic（broker auto-create 关闭） |
| go-redis | v9.22.0 | 非权威缓存与分布式限流载体 |
| prometheus/client_golang | v1.24.1 | `/metrics` 指标 |
| testcontainers-go（+ postgres/redis/kafka 模块） | v0.44.0 | 集成测试中间件 |
| Anvil（foundry） | `ghcr.io/foundry-rs/foundry:v1.8.1` | 本地链（`127.0.0.1:8545`，chain-id 31337） |
| Redis | `redis:8.2.10-alpine` | 仅 `events` profile；非权威 |
| Kafka | `apache/kafka:4.1.0` | 仅 `events` profile；单节点 KRaft |

无 KMS/HSM、无 TLS、无 Dockerfile：Go 进程在宿主运行，compose 只提供依赖中间件。

## 5. 快速开始

**依赖与 shell**：Go 1.26.5、Docker + Docker Compose；bash/zsh（Linux、macOS 或 WSL2）。

**配置**（env-only，无配置文件；`.env.example` 为模板）：

```bash
cp .env.example .env
# 按需修改后加载：
set -a; source .env; set +a
```

> ⚠️ `.env.example` **不是完整启动清单**：它覆盖 001–005 的必填键，并登记 013 键（默认
> 全部注释）。`serve` 的**最小必填集** = `.env.example` 标注 `Required` 的 10 个键
> （`TXHARBOR_PG_DSN`、`TXHARBOR_RPC_URL`、`TXHARBOR_CHAIN_ID`、`TXHARBOR_START_HEIGHT`、
> `TXHARBOR_LOG_START_HEIGHT`、`TXHARBOR_LOG_CONTRACTS`、`TXHARBOR_DEPOSIT_START_HEIGHT`、
> `TXHARBOR_DEPOSIT_CONTRACTS`、`TXHARBOR_DEPOSIT_WATCH_ADDRESSES`、
> `TXHARBOR_CONFIRMATION_DEPTH`）**加上 006 的 `TXHARBOR_REORG_MAX_DEPTH`**（必填无默认，
> 缺失/为 0/非法均拒绝启动）。007 起的键按对应接口/CLI 的规格补齐：具体 CLI 缺必需配置时
> 拒绝启动并报出键名；接口凭据未配置时相关接口拒绝访问（例如 008 只读端点默认拒绝一切读取），
> 不代表 `serve` 必须拒绝启动。

**启动依赖、迁移、服务**：

```bash
docker compose up -d                 # PostgreSQL(127.0.0.1:5432) + Anvil(127.0.0.1:8545)，仅绑定 127.0.0.1
go run ./cmd/txharbor migrate up     # 应用迁移（000001–000018）；serve 不会自动迁移
go run ./cmd/txharbor serve          # 业务进程
```

**健康检查**（默认 `TXHARBOR_HTTP_ADDR=127.0.0.1:8080`）：

```bash
curl -fsS http://127.0.0.1:8080/livez     # 存活
curl -fsS http://127.0.0.1:8080/readyz    # 就绪
curl -fsS http://127.0.0.1:8080/metrics   # Prometheus 指标
```

**Signer 与 worker**（两者对缺失配置均 fail-closed；下列值为本地测试占位符，需替换）：

```bash
# 009：development 模式 + 本地密钥文件；策略键（chains/senders/assets/recipients/
# 金额与 gas/费用上限）全部必填，缺一即拒绝启动（config.SignerPolicyConfig）
TXHARBOR_SIGNER_MODE=development \
TXHARBOR_SIGNER_KEY_FILE=/path/to/dev-key.hex \
TXHARBOR_SIGNER_CHAINS=31337 \
TXHARBOR_SIGNER_SENDERS=<anvil-test-sender-address> \
TXHARBOR_SIGNER_ASSETS=<erc20-contract-address> \
TXHARBOR_SIGNER_RECIPIENTS=<anvil-test-recipient-address> \
TXHARBOR_SIGNER_MAX_AMOUNT=<positive-int> \
TXHARBOR_SIGNER_MAX_GAS_LIMIT=<positive-int> \
TXHARBOR_SIGNER_MAX_FEE_PER_GAS=<positive-int> \
TXHARBOR_SIGNER_MAX_PRIORITY_FEE_PER_GAS=<positive-int> \
TXHARBOR_SIGNER_MAX_GAS_PRICE=<positive-int> \
go run ./cmd/txharbor signer-serve

# 011：缺 RPC_URL / TX_SIGNER_URL / TX_SIGNER_CREDENTIAL / DSN 任一即拒绝启动
TXHARBOR_TX_SIGNER_URL=http://127.0.0.1:8091 \
TXHARBOR_TX_SIGNER_CREDENTIAL=<signer-auth 签发的凭据> \
go run ./cmd/txharbor withdrawal-worker
```

**接口演示**（与「启动服务」分开）：007 接收需要 Bearer caller key（`apikey-auth` 签发）
与逐笔授权（`withdrawal-authz supply`）；008 只读端点需要 `TXHARBOR_NONCE_READ_TOKEN`
（未配置则拒绝一切读取）。请求形状与验收步骤见 `specs/007-withdrawal-creation/`、
`specs/008-nonce-manager/`、`specs/011-withdrawal-executor/` 的 quickstart。

**013 事件进程（可选）**：

```bash
docker compose --profile events up -d     # Redis + Kafka（显式创建 txharbor.events.v1，关闭 auto-create）
# 启用时必填：TXHARBOR_REDIS_ADDR、TXHARBOR_KAFKA_BROKERS、五类 TXHARBOR_RATELIMIT_*
# （rate/burst，无默认）、容量 soft/hard/reserve 与时长键；缺失即拒绝启动
TXHARBOR_EVENTS_ENABLED=true go run ./cmd/txharbor event-publisher
TXHARBOR_EVENTS_ENABLED=true go run ./cmd/txharbor event-consumer
```

默认 PG-only 基线（仅 PG + Anvil）不启动发布器/消费者，业务按 PG 权威继续。

**015 恢复 CLI（可选）**：`recovery-admin` 需要独立控制库与主体配置
（`TXHARBOR_RECOVERY_CONTROL_DSN`、`TXHARBOR_RECOVERY_PRINCIPAL`、
`TXHARBOR_RECOVERY_ARTIFACT_DIR` 等），入口与场景见 `specs/015-backup-recovery-safe-resumption/quickstart.md`。

**009 隔离叠加（可选）**：`docker compose -f compose.yaml -f compose.009.yaml up -d postgres`
提供独立项目/库/端口（project `txharbor-009`、库 `txharbor_009`、`127.0.0.1:5433`，独立数据卷）；
compose 合并会保留默认 5432 映射，执行前先停默认项目并使用 5433 连接。

## 6. 测试与验证

| 入口 | 用途 | 需要 Docker |
| --- | --- | --- |
| `make test` / `make test-race` | 单元测试 / race 检测 | 否 |
| `make test-integration` | PostgreSQL 层集成（testcontainers） | 是 |
| `make test-integration-redis` / `make test-integration-kafka` | Redis / Kafka 层集成 | 是 |
| `make test-contract` | 事件信封、目录、schema 版本、消费者兼容与 014/015 契约 | 否 |
| `make test-e2e` | 核心充提全链（全栈 + Anvil） | 是 |
| `make test-fault` | 五态故障矩阵（独立层，不进普通 PR） | 是 |
| `make test-perf` | PG-only vs 全栈对照基准（独立层） | 是 |
| `make test-drill` | 灾备演练 S1–S12 + F1–F7（独立通道，见下；需真实 PG/Anvil，事件场景需 Kafka，并需宿主 `pg_dump`/`pg_restore` 等恢复工具） | 是 |
| `make lint` / `make build` / `make db-reset` | gofmt+vet（双标签）/ 构建 / 删除数据卷 | 否 / 否 / 是 |

- 013 起的分层入口带 `require_tagged_tests` 守卫：无对应 tag 测试时该层报 **NOT RUN**
  并非零退出，空层不得冒充通过。
- `make test-drill` 由 JSON 检查器逐场景要求 **PASS**：缺失/跳过的必需场景（S1–S12、
  F1–F7、效果与重入场景）即失败；`TXHARBOR_DRILL_EVIDENCE_DIR=<dir>` 可指定归档目录
  （每次运行独立子目录，未设置时写临时目录）。演练命令与场景定义见
  `specs/015-backup-recovery-safe-resumption/quickstart.md` §1–§3 与
  `docs/ops/recovery-runbook.md`。
- 独立层通道：`.github/workflows/fault-perf.yml` 与 `drill.yml`（schedule/dispatch，
  不阻塞普通 PR）；普通 PR 由 `ci.yml` 覆盖（lint/build/单元+race/集成，`ci-required` 门禁）。

## 7. 证据与文档导航

- **简历/展示材料**：[docs/portfolio.md](docs/portfolio.md)（30 秒介绍、5 分钟演示讲稿、
  简历要点、面试问答、证据索引）。
- **变更记录与依赖**：[CHANGELOG.md](CHANGELOG.md)、[THIRD_PARTY.md](THIRD_PARTY.md)。
- **规格**：`specs/001-project-foundation/` … `specs/015-backup-recovery-safe-resumption/`
  （每阶段 spec / plan / tasks / quickstart）。
- **运行手册**：`docs/ops/recovery-runbook.md`；演练检查器 `scripts/drillcoverage/check.go`。
- **证据归档**：`docs/evidence/013/`（事件基础设施）、
  `docs/evidence/013-supplement/consumer-seg/`（消费者追赶分段测量：README 口径与结论、
  CORRECTIONS、analysis/summary、checks 核验日志）、`docs/evidence/014/`、
  `docs/evidence/015/`、`docs/evidence/019-normal-query-seg/`（正常查询分段对照）。
- **性能口径**：只写测量与定位，不写提速。批次条件（N=10000、同机串行、`perf` 标签接缝、
  pristine/ON/OFF 配对）与数字来源见 `docs/evidence/013-supplement/consumer-seg/README.md`
  与 `docs/evidence/013-supplement/consumer-seg/analysis/summary.md`；结论不构成 SLO。
- **CI**：[Actions](https://github.com/xtianxx/TxHarbor/actions)。两个具体运行：
  PR #44 [run 37745833563](https://github.com/xtianxx/TxHarbor/actions/runs/37745833563)（success）；
  main 推送 [run 37748375307](https://github.com/xtianxx/TxHarbor/actions/runs/37748375307)
  （attempt 1 failure → 同 SHA 单次重试 attempt 2 success，**原因尚未确认**，不声称根因已修复）。
  两次运行的原始日志与元数据留存于本地 git-ignored 审计目录 `.evidence/ci-runs/verify/013-consumer-seg-pr44/`。

## 8. 项目边界

- **不维护用户余额账本**：资金事实来自链上观察与请求/执行状态，不维护内部余额账。
- **不宣称跨系统恰好一次**：013 投递语义为「至少一次 + 消费幂等」。
- **未生产上线**：013/014/015 已合入 main（013 PR #25；014 PR #27；015 PR #38/#39），
  但**未部署**；T000-P 保持 OPEN（`docs/project-context.md`、
  `specs/011-withdrawal-executor/integration-readiness.md`），不宣称生产就绪。
- **无 KMS/HSM**：v1 只有本地 `development` 密钥 provider，`production` 模式拒绝启动。
- **无 TLS**：`serve` 明文监听，生产需自行前置 TLS 终止。
- **无 Dockerfile**：Go 进程在宿主运行；compose 只提供 PostgreSQL、Anvil 与
  （013 `events` profile）Redis/Kafka。
- **阈值待裁决**：容量/限流/告警/追赶窗口与 015 的 RPO/RTO/频率/保留均为本地测试输入，
  一律「待测/待裁决」，不得当作生产阈值。
- **性能与 CI 表述纪律**：性能内容只做测量与定位，不宣称已实现的提速；
  CI 首次失败（`TestRestoreInterruptionNotRestoredAndRerunIdempotent`）原因未确认，
  同 SHA 重试通过不构成根因修复结论。
