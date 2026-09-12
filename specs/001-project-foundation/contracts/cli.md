# Contracts: CLI

二进制：`txharbor`（单二进制，双子命令）。配置载体：仅环境变量（见 data-model §1）。退出码：`0`=成功，`≠0`=失败并输出可诊断错误。

## `txharbor migrate`

独立迁移命令；服务启动永不自动迁移。

| 用法 | 行为 |
|---|---|
| `txharbor migrate up` | 串行应用全部 pending 迁移；已应用跳过；失败不记版本（可重跑）；并发经 DB 层互斥，后者等待至 `TXHARBOR_MIGRATE_LOCK_TIMEOUT`，超时非零退出 |
| `txharbor migrate status` | 输出当前版本与 pending 列表（供诊断与 CI 门禁） |

输出：`applied=N skipped=M pending=K` + 失败时 `failed_version=N reason=...`。

## `txharbor serve`

启动顺序（预算 `TXHARBOR_STARTUP_TIMEOUT=30s`，任一步超时/失败→非零退出）：

1. 加载并校验环境变量（缺失/非法点名报错）
2. PG `Ping`（`TXHARBOR_PROBE_TIMEOUT`）
3. 版本兼容校验（无 pending，否则拒绝就绪并提示先跑 `migrate up`）
4. RPC `eth_chainId` 校验（与 `TXHARBOR_CHAIN_ID` 比对，不一致拒绝就绪并同时报告期望/实际）
5. 监听 `TXHARBOR_HTTP_ADDR`，进入探针循环（`not-ready` 起步，首次全过→`ready`）

信号：SIGINT/SIGTERM → 停收新工作 → `ethclient.Close()` + `pool.Close()` → `TXHARBOR_SHUTDOWN_TIMEOUT=15s` 内退出，超时强制结束并记错。

## 迁移文件契约（`migrations/`）

- 文件名 `NNNNNN_snake_name.sql`，`-- +goose Up` / `-- +goose Down` 分段，单文件单事务
- 禁止 `NO TRANSACTION`（失原子性）
- 001 仅含底座自身对象（如 goose 版本表由工具自建；无业务表）
