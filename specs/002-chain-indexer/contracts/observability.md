# Contract: Indexer Observability (002-chain-indexer)

**Branch**: `002-chain-indexer` | **Date**: 2026-09-12

本阶段不新增 HTTP 端点、不新增业务 API。此文件是扫描状态的**可观测契约**：
指标名、日志字段、暂停行语义一经确定即为验收依据，后续阶段不得静默改名。

## Metrics（既有 `/metrics` 注册表扩展）

| Name | Type | Labels | Semantics |
|------|------|--------|-----------|
| txharbor_indexer_checkpoint_height | Gauge | chain | checkpoint 高度；空进度时不暴露该序列 |
| txharbor_indexer_state | Gauge | chain | 0 运行 / 1 等待（追头或 S 未到）/ 2 重试 / 3 暂停 |
| txharbor_indexer_rpc_total | Counter | kind(`transport\|timeout\|invalid-response\|rate-limited\|not-found\|chain-mismatch`), result(`ok\|error`) | 按类打点；`not-found/ok` 即等待 |
| txharbor_indexer_pause_total | Counter | chain | 暂停次数（与 pause 行数无关，只增） |

既有 `txharbor_ready` / `txharbor_probe_total{dep,result}` 不变。
关系：`/readyz` 只反映 DB/RPC 依赖健康；`state=3`（暂停）**不得**翻转 readyz（见 research R7）。

## Logs（结构化，字段固定）

- 推进：`chain_id, height, hash, attempt`（info）。
- 等待：`chain_id, height, reason(wait_head|wait_start|verify_retry)`（debug/info，低频）。
- 暂停：`chain_id, height, expected_hash, actual_hash, kind`（error）。
- 脱敏：所有日志经 `logx.Redact`；RPC URL 打印前必须脱敏（含 `config.Summary` 的附带修复）。

## Pause row（诊断查询，SQL 即接口）

```sql
SELECT chain_id, height, expected_hash, actual_hash, kind, detail, created_at
FROM indexer_pause WHERE chain_id = $1;
```

行存在 = 该链暂停有效；行不存在 = 未暂停。进度查询：

```sql
SELECT height, block_hash, updated_at FROM indexer_checkpoint WHERE chain_id = $1;
-- 无行 = 空进度
```
