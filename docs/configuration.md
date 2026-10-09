# TxHarbor 配置参考（env-only）

> **事实边界**：本文件描述当前实现的配置加载行为（`internal/config/config.go`）与模板
> （`.env.example`）。项目未部署、T000-P 保持 OPEN。所有阈值类取值一律「待测/待裁决」，
> 模板中出现的数值是测量示例或部署输入，**不是生产阈值**。
> **验证状态**：`serve` 的冷启动路径（001–006 必填键组合）已由 `make smoke-quickstart`
> （`scripts/quickstart-smoke/`，隔离环境）实际启动验证；其余键组合与 CLI 仍为源码常量、
> 加载分支与模板注释的静态对照（**NOT VERIFIED**）。

## 1. 加载规则

- **唯一配置载体是环境变量**：无配置文件、无第三方配置库；`config.Load` 一次性读取并校验。
- **缺失必填 → 拒绝启动并报出键名**（`missing required environment variable <KEY>`）；
  格式错误同样在加载期报错。
- **fail-closed 的条件必填**：某些键只在特定模式/开关下必填（见下各节），缺失即拒绝启动或拒绝执行。
- **凭据脱敏**：启动回显与日志经 `internal/logx.Redact` 处理；凭据不进入日志/错误/指标。
- **模板覆盖范围**：`.env.example` 覆盖 001–006 的必填键，`serve` 可直接用模板启动；007–015 的键
  以登记形式提供（signer / worker / 事件 / 管理 CLI，**默认注释**，按需启用）。

## 2. 所有进程共享的必填键（`serve` / `migrate`）

| 键 | 说明 | 缺失行为 |
| --- | --- | --- |
| `TXHARBOR_PG_DSN` | PostgreSQL DSN（pgx 接受的形式） | 拒绝启动 |
| `TXHARBOR_RPC_URL` | 外部 EVM JSON-RPC 端点 | 拒绝启动 |
| `TXHARBOR_CHAIN_ID` | 期望 chain id；不匹配拒绝就绪 | 拒绝启动 |
| `TXHARBOR_START_HEIGHT` | 区块头索引起始高度（0 合法，无默认） | 拒绝启动 |
| `TXHARBOR_LOG_START_HEIGHT` | Transfer 日志扫描起始高度（0 合法，无默认） | 拒绝启动 |
| `TXHARBOR_LOG_CONTRACTS` | ERC-20 合约白名单（逗号分隔；空列表拒绝，不退化为全链查询） | 拒绝启动 |
| `TXHARBOR_DEPOSIT_START_HEIGHT` | 充值扫描起始高度（0 合法，无默认） | 拒绝启动 |
| `TXHARBOR_DEPOSIT_CONTRACTS` | 充值合约白名单（`address[:effective]`；空列表拒绝） | 拒绝启动 |
| `TXHARBOR_DEPOSIT_WATCH_ADDRESSES` | 监控收款地址（`address[:effective]`；空列表拒绝） | 拒绝启动 |
| `TXHARBOR_CONFIRMATION_DEPTH` | 确认深度 N（正整数，无默认） | 拒绝启动 |
| `TXHARBOR_REORG_MAX_DEPTH` | 重组最大深度（006，正整数，无默认；模板已含） | `serve` 拒绝启动 |

常用可选键（含默认值）：`TXHARBOR_HTTP_ADDR`（`127.0.0.1:8080`）、`TXHARBOR_STARTUP_TIMEOUT`（30s）、
`TXHARBOR_PROBE_INTERVAL`（2s）、`TXHARBOR_PROBE_TIMEOUT`（5s）、`TXHARBOR_SHUTDOWN_TIMEOUT`（15s）、
`TXHARBOR_MIGRATE_LOCK_TIMEOUT`（30s）、`TXHARBOR_INDEX_RPC_TIMEOUT`（5s）、`TXHARBOR_INDEX_POLL_INTERVAL`（1s）、
`TXHARBOR_INDEX_RETRY_INITIAL`（200ms）、`TXHARBOR_INDEX_RETRY_MAX`（30s）、`TXHARBOR_LOG_BATCH_BLOCKS`（500）、
`TXHARBOR_DEPOSIT_BATCH_BLOCKS`（500）、`TXHARBOR_REORG_REPLAY_BATCH`（500）。

## 3. `signer-serve`（009）

| 键 | 必填性 | 说明 |
| --- | --- | --- |
| `TXHARBOR_SIGNER_MODE` | 默认 `production` | 仅 `development`/`production` 两个取值；`production` 无 KMS/HSM provider 即拒绝启动 |
| `TXHARBOR_SIGNER_KEY_FILE` | `development` 模式必填 | 本地密钥文件路径 |
| `TXHARBOR_SIGNER_CHAINS` | 必填 | 允许的 chain id 列表 |
| `TXHARBOR_SIGNER_SENDERS` | 必填 | 允许的 sender 列表 |
| `TXHARBOR_SIGNER_ASSETS` | 必填 | 允许的资产（合约）列表 |
| `TXHARBOR_SIGNER_RECIPIENTS` | 必填 | 允许的收款方列表 |
| `TXHARBOR_SIGNER_MAX_AMOUNT` | 必填 | 单笔金额上限（正整数） |
| `TXHARBOR_SIGNER_MAX_GAS_LIMIT` | 必填 | gas limit 上限（正整数） |
| `TXHARBOR_SIGNER_MAX_FEE_PER_GAS` | 必填 | fee per gas 上限 |
| `TXHARBOR_SIGNER_MAX_PRIORITY_FEE_PER_GAS` | 必填 | priority fee 上限 |
| `TXHARBOR_SIGNER_MAX_GAS_PRICE` | 必填 | gas price 上限 |

可选：`TXHARBOR_SIGNER_HTTP_ADDR`（默认 `127.0.0.1:8091`）、`TXHARBOR_SIGNER_KEY_TIMEOUT`。
签名凭据由 `signer-auth` 签发/轮换/吊销（持久化在 PG，不经 env 传递）。

## 4. `withdrawal-worker`（011）

在共享必填键（§2）之外：

| 键 | 必填性 | 说明 |
| --- | --- | --- |
| `TXHARBOR_TX_SIGNER_URL` | 必填 | 009 签名服务地址（如 `http://127.0.0.1:8091`） |
| `TXHARBOR_TX_SIGNER_CREDENTIAL` | 必填 | 009 凭据（`signer-auth` 签发） |

以上任一缺失即拒绝启动（`internal/jointwire/assembly.go` 逐项报出键名）。
节奏类可选键（有初始默认）：`TXHARBOR_WORKER_TTL_SECONDS`、`TXHARBOR_WORKER_HEARTBEAT_SECONDS`、
`TXHARBOR_WORKER_STALL_SECONDS`、`TXHARBOR_WORKER_BACKOFF_BASE_MS`、`TXHARBOR_WORKER_BACKOFF_MAX_MS`、
`TXHARBOR_WORKER_SCAN_INTERVAL_MS`、`TXHARBOR_WORKER_LABEL`。

## 5. 接口与凭据（007/008/012）

- **007 调用方凭据**：经 `apikey-auth`（issue/rotate/revoke）签发并持久化，不是 env 键；
  请求用 Bearer 携带。
- **008 只读端点**：`TXHARBOR_NONCE_READ_TOKEN`（未配置时拒绝一切读取；配置后按 Bearer 校验）。
- **012 授权供给**：经 `withdrawal-authz supply/revoke` 受控 CLI；`TXHARBOR_AUTHZ_ISSUER_CALLERS`
  为 issuer 允许列表（逗号分隔 caller_id；空 = deny-all；读取见 `internal/withdrawal/allowlist.go`）。

## 6. 013 事件基础设施（可选）

主开关 `TXHARBOR_EVENTS_ENABLED`（默认 `false`）。**启用时必填**（缺失即拒绝启动）：

| 键 | 说明 |
| --- | --- |
| `TXHARBOR_REDIS_ADDR` | Redis 地址 `host:port`（缓存/限流载体） |
| `TXHARBOR_KAFKA_BROKERS` | Kafka bootstrap brokers（逗号分隔） |
| `TXHARBOR_RATELIMIT_NEW_WITHDRAWAL` / `_WRITE` / `_QUERY` / `_OPERATOR` / `_RPC` | 五类接口的令牌桶 `rate/burst`（无默认，测量输入） |
| `TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT` / `_HARD_LIMIT` / `_RESERVE` | 容量保护阈值（须满足 `0 < reserve < soft < hard`） |
| `TXHARBOR_EVENTS_CAPACITY_RETENTION` / `_MAX_SHUTDOWN_WINDOW` / `_DRAIN_TARGET_WINDOW` | 正时长（排空目标窗口同时作为告警持续窗口的默认值） |

常用可选键（有默认值）：`TXHARBOR_REDIS_TIMEOUT`（1s）、`TXHARBOR_REDIS_CACHE_TTL`（30s）、
`TXHARBOR_REDIS_CACHE_EPOCH`（1）、`TXHARBOR_KAFKA_TOPIC`（`txharbor.events.v1`）、
`TXHARBOR_KAFKA_CONSUMER_GROUP_PREFIX`（`txharbor`）、`TXHARBOR_RATELIMIT_BUDGET`（默认继承 Redis 超时）、
`TXHARBOR_EVENTS_PUBLISHER_*`（batch/poll/lease/backoff）、`TXHARBOR_EVENTS_CONSUMER_*`（batch/poll/backoff/retry/gap）、
`TXHARBOR_EVENTS_ALERT_ENABLED`（默认 true）、`TXHARBOR_EVENTS_ALERT_SOFT_SUSTAINED_WINDOW`。

注意：**仅设置 `TXHARBOR_REDIS_ADDR`（即使 `TXHARBOR_EVENTS_ENABLED=false`）就会装配缓存与限流**；
Redis 不可用期间新提现创建按 PD-1 返回可重试拒绝（见 `docs/architecture.md` §1）。

## 7. 014/015 管理 CLI（按命令取子集）

- **014**：`reconcile-admin` 经 PG 与受控权限表工作（权限 grant/revoke/show），无独立 env 开关。
- **015**：`recovery-admin` 使用独立控制库与主体：`TXHARBOR_RECOVERY_CONTROL_DSN`、
  `TXHARBOR_RECOVERY_PRINCIPAL`、`TXHARBOR_RECOVERY_ARTIFACT_DIR`（产物命令未给 `--out` 时要求）；
  按命令条件必需或可选：`TXHARBOR_RECOVERY_ISOLATED_TARGET_DSN`、`TXHARBOR_RECOVERY_OBSERVER_DSN`、
  `TXHARBOR_RECOVERY_GATE_DSN`、`TXHARBOR_RECOVERY_DEPLOYMENT_ADMIN_DSN`、`TXHARBOR_RECOVERY_INSTANCE`、
  `TXHARBOR_RECOVERY_GATE_TTL`、`TXHARBOR_RECOVERY_ENTRY_CHAINS`、`TXHARBOR_RECOVERY_EFFECT_CLASS_RULING`、
  `TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_*`、`TXHARBOR_RECOVERY_RPO_TARGET`、`TXHARBOR_RECOVERY_RTO_TARGET`、
  `TXHARBOR_RECOVERY_BACKUP_FREQUENCY`、`TXHARBOR_RECOVERY_RETENTION`、`TXHARBOR_RECOVERY_STATUS_*`。
  缺失必需约束时只能报告「未配置」，不得宣称符合生产恢复目标。

## 8. 本地依赖端口（compose）

| 服务 | 镜像 | 宿主端口 | 备注 |
| --- | --- | --- | --- |
| PostgreSQL | `postgres:18.6-trixie` | `127.0.0.1:5432` | 默认基线；数据卷 `pgdata` |
| Anvil | `ghcr.io/foundry-rs/foundry:v1.8.1` | `127.0.0.1:8545` | chain-id 31337 |
| Redis | `redis:8.2.10-alpine` | `127.0.0.1:6379` | 仅 `--profile events` |
| Kafka | `apache/kafka:4.1.0` | `127.0.0.1:9092` | 仅 `--profile events`；单节点 KRaft；显式建 topic，auto-create 关闭 |

009 隔离叠加：`compose.009.yaml` 提供 project `txharbor-009`、库 `txharbor_009`、`127.0.0.1:5433`
与独立数据卷；叠加合并会保留默认 5432 映射，执行前先停默认项目并使用 5433 连接。
