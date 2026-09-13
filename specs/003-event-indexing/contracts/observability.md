# Contract: Log Observability (003-event-indexing)

**Branch**: `003-event-indexing` | **Date**: 2026-09-13

本阶段不新增 HTTP 端点、不新增业务 API。此文件是日志扫描状态的**可观测契约**：
指标名、日志字段、诊断 SQL 一经确定即为验收依据，后续阶段不得静默改名。
002 的 `indexer_*` 指标与语义冻结，本文件只新增 `log_*` 组。

## Metrics（既有 `/metrics` 注册表扩展）

| Name | Type | Labels | Semantics |
|------|------|--------|-----------|
| txharbor_log_checkpoint_next | Gauge | chain | 日志进度 `next_block`；空进度时不暴露该序列 |
| txharbor_log_lag_blocks | Gauge | chain | 相对滞后 = 002 checkpoint 高度 − (`next_block` − 1)；空进度或 002 空进度时不暴露 |
| txharbor_log_state | Gauge | chain | 0 运行 / 1 等待（上界未覆盖/追头）/ 2 重试 / 3 暂停 |
| txharbor_log_rpc_total | Counter | kind(`transport\|timeout\|invalid-response\|rate-limited\|not-found\|incomplete\|chain-mismatch`), result(`ok\|error`) | 按类打点；`incomplete` 即完整性可疑 |
| txharbor_log_pause_total | Counter | chain | 日志暂停次数（只增，与 pause 行数无关） |

关系：`/readyz` 只反映 DB/RPC 依赖健康；`log_state=3`（暂停）**不得**翻转 readyz（与 002 R7 同理）。
`indexer_state=3`（链级暂停）与 `log_state` 相互独立，允许"头停/日志停"分别表达。

## Logs（结构化，字段固定）

- 推进：`chain_id, from_block, to_block, log_count, attempt`（info）。
- 等待：`chain_id, next_block, reason(wait_coverage|wait_head)`（debug/info，低频）。
- 重试/缩批：`chain_id, from_block, to_block, kind, attempt, retry_in, shrink_to`（warn，`shrink_to` 仅缩批时）。
- 暂停：`chain_id, height, kind(chain_view_changed|validation_failed|range_incomplete), detail`（error；`detail` 不含凭据与原始响应；`validation_failed` 的 `detail.class` 取值 `bad_address|bad_topics|bad_data|removed|out_of_range|missing_field|identity_conflict`）。
- 配置拒绝：`chain_id, reason(blank_whitelist|config_changed), detail`（error；启动期）。
- 脱敏：所有日志经 `logx.Redact`；RPC URL 打印前必须脱敏；禁止转储无限制原始响应（SC-11）。

## Diagnostic SQL（SQL 即接口）

```sql
-- 日志进度（无行 = 空进度）
SELECT start_block, config_hash, next_block, updated_at
FROM log_checkpoint WHERE chain_id = $1;

-- 日志暂停（行存在 = 暂停有效）
SELECT height, kind, detail, created_at
FROM log_pause WHERE chain_id = $1;

-- 相对滞后（两流对照）
SELECT c.height AS block_checkpoint, l.next_block AS log_next
FROM indexer_checkpoint c FULL JOIN log_checkpoint l USING (chain_id)
WHERE chain_id = $1;

-- 区间完整性抽查（应 0 行缺口；仅诊断，不替代提交裁决）
SELECT s.n AS missing_height FROM generate_series($1, $2) s(n)
LEFT JOIN erc20_transfer_logs t
  ON t.chain_id = $3 AND t.block_number = s.n
GROUP BY s.n HAVING count(*) = 0 AND EXISTS
  (SELECT 1 FROM chain_blocks b WHERE b.chain_id = $3 AND b.number = s.n AND b.canonical);
-- 注意：合法空区间本就零日志，此查询只用于"有日志区间是否缺失"的辅助排查，
-- 以提交事务内的行数核对与精确守卫为准。
```
