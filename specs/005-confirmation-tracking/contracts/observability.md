# Contract: Confirmation Observability (005-confirmation-tracking)

**Branch**: `005-confirmation-tracking` | **Date**: 2026-09-14

本阶段不新增 HTTP 端点、不新增业务 API。此文件是确认跟踪状态的**可观测契约**：
指标名、日志字段、诊断 SQL 一经确定即为验收依据，后续阶段不得静默改名。
002 的 `indexer_*`、003 的 `log_*`、004 的 `deposit_*` 指标语义冻结，本文件只新增 `confirmation_*` 组。

## Metrics（既有 `/metrics` 注册表扩展，`internal/metrics/metrics.go:New` 内新增）

| Name | Type | Labels | Semantics |
|------|------|--------|-----------|
| txharbor_confirmation_pending | Gauge | chain | 未确认 Pending 估计数（索引计数；空即 0 暴露） |
| txharbor_confirmation_lag_blocks | Gauge | chain | 相对滞后 = canonical tip − 最高已确认高度；tip 缺失或零确认时不暴露 |
| txharbor_confirmation_state | Gauge | chain | 0 运行 / 1 等待可信 tip / 2 重试 / 3 停止（暂停/漂移/链视图不可信） |
| txharbor_confirmation_policy_seq | Gauge | chain | 当前有效策略 `policy_seq`；策略表空时不暴露 |
| txharbor_confirmation_confirmed_total | Counter | chain | 成功转换计数（明细在观察行依据列） |
| txharbor_confirmation_skipped_total | Counter | chain, reason(`noncanonical\|tip_unverifiable`) | 跳过未确认计数（行留 Pending，不 halt） |
| txharbor_confirmation_transition_total | Counter | chain, result(`ok\|stale\|rejected`) | 提交裁决审计计数（`stale`=锁内失配回滚，`rejected`=漂移/暂停拒绝） |
| txharbor_confirmation_policy_transition_total | Counter | chain, result(`ok\|rejected`) | 策略切换审计计数（明细在 DB history 行） |

关系：`/readyz` 只反映 DB 依赖健康；`confirmation_state=3` **不得**翻转 readyz
（与 002/003/004 同理）。`indexer_state`/`log_state`/`deposit_state`/`confirmation_state`
相互独立，允许四流分别表达。

## Logs（结构化，字段固定，经 `logx.Redact`）

- 确认转换：`chain_id, block_number, block_hash, tx_hash, log_index, tip, threshold, confirmations, policy_seq, attempt`（info）。
- 跳过：`chain_id, block_number, block_hash, reason(noncanonical|tip_unverifiable)`（debug，低频）。
- 重试：`chain_id, kind, attempt, retry_in`（warn；有界退避，复用 INDEX 参数）。
- 停止：`chain_id, reason(pause_present|tip_missing|tip_untrusted|policy_drift|lease_lost)`（error=需介入 / warn=等待类）。
- 配置拒绝：`chain_id, reason(blank|non_integer|non_positive|out_of_range|policy_drift), detail`（error；启动期与切换期同形）。
- 策略切换：`chain_id, request_id, operator, old_seq, old_threshold, new_threshold, result(ok|rejected), reason`（info=ok / warn=rejected；审计明细以 DB history 行为准）。
- 脱敏：禁止转储无限制原始数据；阈值与高度为普通数值字段，可打点。

## Diagnostic SQL（SQL 即接口）

```sql
-- 待确认候选（有序；降阈值后自动纳入，无游标）
SELECT block_number, block_hash, tx_hash, log_index, amount
FROM deposit_observations
WHERE chain_id = $1 AND status = 'pending' AND block_number <= $2
ORDER BY block_number, log_index LIMIT $3;

-- 确认依据追溯（任意 Confirmed 行：充值→区块→依据→时间）
SELECT block_number, block_hash, tx_hash, log_index,
       confirm_tip_number, confirm_tip_hash, confirm_threshold,
       confirmations, confirm_policy_seq, confirmed_at
FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed' ORDER BY confirmed_at LIMIT $2;

-- 策略版本链审计（有效策略 = 最大 seq；prev 链不断即线性无分叉）
SELECT policy_seq, threshold, prev_seq, operator, reason, request_id, expected_old_seq, created_at
FROM confirmation_policy_history WHERE chain_id = $1 ORDER BY policy_seq;

-- 按请求身份查切换结果（同 ID 同参返原结果；异参/过期可定位原记录，不虚构成功）
SELECT policy_seq, threshold, operator, reason, created_at
FROM confirmation_policy_history WHERE chain_id = $1 AND request_id = $2;

-- 四流对照（tip / 日志 / 充值 / 确认；任一空即该流未就绪）
SELECT c.height AS tip, c.block_hash AS tip_hash,
       l.next_block AS log_next, d.next_block AS deposit_next,
       (SELECT COUNT(*) FROM deposit_observations o
         WHERE o.chain_id = $1 AND o.status = 'pending') AS pending
FROM indexer_checkpoint c
FULL JOIN log_checkpoint l USING (chain_id)
FULL JOIN deposit_checkpoint d USING (chain_id)
WHERE chain_id = $1;

-- 转换完整性抽查（应返回 0：confirmed 行依据缺失）
SELECT COUNT(*) FROM deposit_observations
WHERE chain_id = $1 AND status = 'confirmed'
  AND (confirmed_at IS NULL OR confirm_tip_number IS NULL OR confirm_tip_hash IS NULL
       OR confirm_threshold IS NULL OR confirmations IS NULL OR confirm_policy_seq IS NULL);
```
