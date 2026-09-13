# Contract: Deposit Observability (004-deposit-detection)

**Branch**: `004-deposit-detection` | **Date**: 2026-09-13

本阶段不新增 HTTP 端点、不新增业务 API。此文件是充值识别状态的**可观测契约**：
指标名、日志字段、诊断 SQL 一经确定即为验收依据，后续阶段不得静默改名。
002 的 `indexer_*` 与 003 的 `log_*` 指标语义冻结，本文件只新增 `deposit_*` 组。

## Metrics（既有 `/metrics` 注册表扩展）

| Name | Type | Labels | Semantics |
|------|------|--------|-----------|
| txharbor_deposit_next | Gauge | chain | 充值进度 `next_block`；空进度时不暴露该序列 |
| txharbor_deposit_lag_blocks | Gauge | chain | 相对滞后 = 003 `log_checkpoint.next_block` − `deposit next_block`；任一空进度时不暴露 |
| txharbor_deposit_state | Gauge | chain | 0 运行 / 1 等待上游覆盖 / 2 重试 / 3 暂停 / 4 结构性缺口停止 |
| txharbor_deposit_observations_total | Counter | chain, result(`matched\|nomatch\|zero\|invalid`) | 按处理结果打点；`matched` 即生成观察 |
| txharbor_deposit_pause_total | Counter | chain | 充值暂停次数（只增，与 pause 行数无关） |
| txharbor_deposit_transition_total | Counter | chain, result(`ok\|error\|rejected`) | 授权转换审计计数（明细在 DB history 行；`rejected` 含过期/空授权拒绝） |

关系：`/readyz` 只反映 DB 依赖健康；`deposit_state=3/4` **不得**翻转 readyz
（与 002 R7、003 R7 同理）。`indexer_state`（链级）/`log_state`（日志流）/`deposit_state`
相互独立，允许"头停/日志停/充值停"分别表达。

## Logs（结构化，字段固定）

- 推进：`chain_id, from_block, to_block, matched, nomatch, zero, attempt`（info）。
- 等待：`chain_id, next_block, reason(wait_coverage)`（debug/info，低频）。
- 重试：`chain_id, from_block, to_block, kind, attempt, retry_in`（warn）。
- 缺口：`chain_id, gap_from, gap_to, class(transient|structural), cause, config`（warn=暂时等待 / error=结构停止；`cause` 取值 `below_upstream_start|asset_not_indexed|behind_head`）。
- 暂停：`chain_id, pause_id, revision, height, kind(upstream_gap|chain_view_changed|validation_failed), detail`（error；`detail` 含 `version=<seq>` 明确归属，不含凭据与原始数据；`validation_failed` 的 `detail.class` 取值 `bad_address|bad_topics|bad_amount|out_of_range|missing_field|identity_conflict`；需 006 时含 `needs_006=true`）。
- 暂停解除（审计补充日志；效力以 DB 审计行为准）：`chain_id, pause_id, revision, operator, reason, result(ok|stale)`（info；`stale` 指条件失配影响 0 行）。
- 配置拒绝：`chain_id, reason(blank_list|config_changed|upstream_drift), detail`（error；启动期；`upstream_drift` 指 env 重算的 003 身份与上游行不一致）。
- 授权转换：`chain_id, request_id, operator, old_config, new_config, replay_from, result(ok|error|rejected), reason`（info=ok / error=error / warn=rejected；审计明细以 DB history 行为准，日志只做索引；过期拒绝须带预期版本与当前版本）。
- 脱敏：所有日志经 `logx.Redact`；禁止转储无限制原始数据；金额只打十进制字符串（SC-09）。

## Diagnostic SQL（SQL 即接口）

```sql
-- 充值进度（无行 = 空进度）
SELECT start_block, config_hash, next_block, updated_at
FROM deposit_checkpoint WHERE chain_id = $1;

-- 充值暂停/停止（行存在 = 暂停有效）
SELECT height, kind, detail, created_at
FROM deposit_pause WHERE chain_id = $1;

-- 相对滞后（三流对照）
SELECT c.height AS block_checkpoint, l.next_block AS log_next, d.next_block AS deposit_next
FROM indexer_checkpoint c
FULL JOIN log_checkpoint l USING (chain_id)
FULL JOIN deposit_checkpoint d USING (chain_id)
WHERE chain_id = $1;

-- 覆盖证明复核（应返回真；仅诊断，不替代提交裁决）
SELECT (SELECT next_block FROM log_checkpoint WHERE chain_id = $1) > $2 AS covered;

-- 某地址充值查询（运维）
SELECT block_number, block_hash, tx_hash, log_index, contract, sender, amount, status
FROM deposit_observations
WHERE chain_id = $1 AND recipient = $2 ORDER BY block_number, log_index;

-- 配置版本链审计（按 seq 排序；prev 链不断即线性无分叉；相同内容复现有各自 seq 行）
SELECT version_seq, config_hash, prev_seq, start_block, replay_from, operator, reason, request_id, expected_pause_id, expected_pause_revision, created_at
FROM deposit_config_history WHERE chain_id = $1 ORDER BY version_seq;

-- 按请求身份查授权结果（同 ID 同参返原结果；异参/过期均可定位原记录，不虚构成功）
SELECT version_seq, config_hash, replay_from, operator, reason, created_at
FROM deposit_config_history WHERE chain_id = $1 AND request_id = $2;

-- 暂停实例审计（释放/合并事件；活动行删除后仍可解释）
SELECT pause_id, revision, action, operator, reason, version_seq, kind, height, detail, at
FROM deposit_pause_audit WHERE chain_id = $1 ORDER BY at;

-- 解除结果判定（条件解除影响 0 行后执行：命中返原结果，不重写不触碰新暂停；未命中按陈旧处理）
SELECT action, operator, reason, version_seq, kind, height, at
FROM deposit_pause_audit WHERE chain_id = $1 AND pause_id = $2 AND revision = $3 AND action = 'release';

-- 观察↔版本关联（显式引用，非时间戳推导；回放保留原值）
SELECT o.block_number, o.tx_hash, o.log_index, o.amount, o.version_seq, h.config_hash
FROM deposit_observations o JOIN deposit_config_history h
  ON h.chain_id = o.chain_id AND h.version_seq = o.version_seq
WHERE o.chain_id = $1 AND o.recipient = $2 ORDER BY o.block_number, o.log_index;
```
