# Data Model: 001-project-foundation

**Spec**: `spec.md`（Key Entities + FR-001…019）| **说明**: 001 仅含底座自身实体，**无任何业务表**（FR-017/018）。

## 1. ApplicationConfig（环境变量，唯一载体）

| 变量 | 必需 | 默认 | 校验规则 |
|---|---|---|---|
| `TXHARBOR_PG_DSN` | 是 | 无 | 可解析的 Postgres 连接串；缺失/非法→启动失败并点名 |
| `TXHARBOR_RPC_URL` | 是 | 无 | 可解析的 http(s) URL；缺失/非法→启动失败并点名 |
| `TXHARBOR_CHAIN_ID` | 是 | 无 | 十进制正整数，与 `eth_chainId` 比对；不一致→拒绝就绪 |
| `TXHARBOR_HTTP_ADDR` | 否 | `127.0.0.1:8080` | `host:port` 格式 |
| `TXHARBOR_STARTUP_TIMEOUT` | 否 | `30s` | >0 的 duration |
| `TXHARBOR_PROBE_INTERVAL` | 否 | `2s` | >0，须满足 感知界限（interval+单次超时 < 10s） |
| `TXHARBOR_PROBE_TIMEOUT` | 否 | `5s` | >0，单次 DB/RPC 探测超时 |
| `TXHARBOR_SHUTDOWN_TIMEOUT` | 否 | `15s` | >0，超时强制结束并记错 |
| `TXHARBOR_MIGRATE_LOCK_TIMEOUT` | 否 | `30s` | >0，后启动者等待上限 |

校验顺序：缺失检查 → 格式/范围校验 → 输出脱敏后的有效配置摘要（凭据位为 `[REDACTED]`）。任一步失败：`serve` 与 `migrate` 均以非零退出，不对外服务。

## 2. SchemaVersion（`goose_db_version`，PG 为权威）

goose v3 管理，迁移文件 `migrations/NNNNNN_name.sql`（`-- +goose Up/Down`，单文件单事务，禁 `NO TRANSACTION`）：

| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | integer | PK，IDENTITY | goose 内部序号 |
| `version_id` | bigint | NOT NULL | 迁移文件编号 |
| `is_applied` | boolean | NOT NULL | 是否已应用 |
| `tstamp` | timestamp | NOT NULL DEFAULT now() | 记录时间 |

不变量：失败版本**无行记录**（同事务回滚），可直接重跑；成功版本重复执行跳过；并发经 session advisory lock 串行（显式 `WithSessionLocker`）。

## 3. DependencyHealth（内存态，非权威）

| 字段 | 说明 |
|---|---|
| `dep` | `db` \| `rpc` |
| `last_result` | 最近一次探测 成功/失败 + 失败原因分类（transport/timeout/chain-mismatch/invalid-response） |
| `consecutive_failures` | 连续失败计数（抖动抑制：≥1 即翻转，本阶段不设阈值，靠 interval+timeout 换算满足 10s 界限） |

聚合规则：`ready = db_ok && rpc_ok && version_ok && chain_ok`；任一失败→就绪失败，存活不受影响。

## 4. ServiceLifecycle（可观测状态机）

`starting → ready ⇄ not-ready → shutting-down → exited`

| 转换 | 条件 |
|---|---|
| starting → ready | 配置合法 + PG Ping ok + 版本兼容（无 pending）+ RPC 可达 + chain-id 一致，且在启动预算内 |
| starting → exited(非零) | 任一前置失败或预算耗尽 |
| ready ⇄ not-ready | 探针聚合结果翻转；恢复自动回切，无需重启 |
| any → shutting-down | 收到 SIGINT/SIGTERM 即停收新工作 |
| shutting-down → exited | 资源释放完成；超时则强制结束并记错后退出 |

## 5. ProbeMetrics（`/metrics`，仅底座）

| 指标 | 类型 | 说明 |
|---|---|---|
| `txharbor_ready` | GaugeFunc | 抓取时求值：1=就绪，0=未就绪 |
| `txharbor_probe_total{dep,result}` | CounterVec | DB/RPC 探测成功/失败计数（`result=success\|failure`） |
| `process_*` / `go_*` | 默认 collector | client_golang 自带 |

禁止：业务指标、延迟直方图、backlog 预留（FR-019）。

## 关系

`ApplicationConfig` → 驱动 `migrate`（SchemaVersion 推进）与 `serve`（Lifecycle 转换）→ `DependencyHealth` 聚合 → `ready` 状态同时暴露于 `/readyz` 与 `ProbeMetrics`。PG 是 SchemaVersion 唯一权威；内存态丢失后重启自探针重建。
