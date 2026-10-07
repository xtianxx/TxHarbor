# 分段正常查询批次 离线统计（summary.md）

- 根目录：`/home/dream/product_env/TxHarbor/.evidence/seg-smoke`
- 生成时间：2026-10-07T12:33:34.935298575Z
- schema_version：1
- 单元数：1

## 0. 口径与纪律（必读）

- 选样偏差控制：全部客户端样本（含 429/503/500 等非 2xx 与 transport 错误）均计入状态计数与分位数分母；不做选择性剔除，错误响应不计为缺失。
- p95 不可相加：分位数不可加；跨段/跨项比较只允许同一 ID 集合上的差值（差值对比（禁止相加/占比证明））。
- 分段占比≠贡献证明：某段占总时长的比例大，不构成该段是收益来源的证明。
- 单机合成负载非生产 SLO：单机、in-process 合成负载的绝对值不代表生产 SLO，仅用于同一环境内的受控对比。
- 历史数值：89ef787 的 47.5/72.7ms 为历史测量，本轮不得标为当前 main 的实测数值。

限制：
- 相位分桶 [0s,4s)/[4s,8s)/[8s,12s) 相对 meta.window_start；固定 burst 在 8s 触发，第三档 [8s,12s) 与 burst 重叠，不可视为纯稳态。
- 预热样本只做单独小节，不并入稳态统计。
- pgq 语句级记录仅在 TXHARBOR_PERF_SEG_TRACE=1 时存在；pgq 缺失不等于没有 PG 活动。
- perf_id 为服务端逐请求铸造；缺少 perf_id 的客户端样本无法配对，单独计数。
- 计时嵌套：server_total ⊇ {admit ⊕ below_admit ⊕ 包装开销}，below_admit ⊇ 查询等，pgq 语句 ⊆ below_admit；禁止把各段相加当作总时长。
- 因子效应为差值（R 效应 @G0/@G1、G 效应 @R0/@R1、交互），由臂间差值定义；不得对分位数做加和或占比证明。
- 时钟步进：wall clock 回拨（按 server_total 的 id 序检测，阈值 <−50ms）按 |Δ| 对边界之后的标签做校正，并披露原始 wall 相位（client_query_phases_wall）；前跳（>+500ms）与停顿/GC 不可区分（ambiguous），只披露不校正；跨机绝对时间不可比；时长类指标（duration_ms / dur_ns）取自单调时钟，不受回拨影响。
- 校正精度受相邻请求间隔限制：Σ|Δ| 含一次正常间隔，校正后标签可能仍偏早约一个间隔（≈0.1s）；相位归类可用，精确时刻不可复原。
- 适用范围：只有 first_affected_id > first_steady_id（稳态 client perf_id 最小值）的步进才作用于窗口；预热期回拨不校正，window_start 永不校正。
- 若批次把预热流量也写进 server_segments.jsonl，其段记录落在窗口之外并被相位分桶排除，逐段计数见 segment_outside_window。

顶层字段（summary.json）：`schema_version`, `generated_at`, `root`, `factor_definition`, `factor_disclaimer`, `arms`, `repeat_p95_spread`, `clock_steps`, `factor_effects`, `overhead_compare`, `candidate_rules`, `pairing_totals`, `invariants`, `completeness`, `discipline`, `limitations`, `warnings`

因子定义：R = limiter_on（限流开关）；G = background_on（后台负载开关）；R1G1:nocoll = collect=false 的开销对照臂（不进因子对比）。

因子对比口径：差值对比（禁止相加/占比证明）：全部因子效应都是同一指标、同一 ID 集合上的臂间差值；分位数不可加，占比不构成因果或收益证明。

## 1. 覆盖率与完整性

- 重复：[99]；单元：1；行解析失败：0
- repeat99 臂：R1G1
- repeat99 缺失稳态臂：R0G0, R1G0, R0G1

相位口径：相位分桶基于**校正后标签**（详见 §11 时钟步进与校正）；phase_method=wall 表示未检测到回拨。

## 2. 每臂×重复：客户端与相位

| repeat | arm | query n | p50ms | p95ms | p99ms | maxms | 状态计数（全部样本，含非 2xx/transport） |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | 390 | 8.754 | 114.283 | 133.765 | 137.941 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |

相位分桶（相对 meta.window_start；[8s,12s) 与固定 burst 重叠，非纯稳态）：

| repeat | arm | 相位 | n | p50ms | p95ms | 状态计数 |
| --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | [0s,4s) | 42 | 9.364 | 12.106 | total=42 200×42 2xx=42/非2xx=0/transport=0/no_status=0/err=0 |
| 99 | `R1G1` | [4s,8s) | 152 | 8.757 | 127.629 | total=152 200×152 2xx=152/非2xx=0/transport=0/no_status=0/err=0 |
| 99 | `R1G1` | [8s,12s) | 196 | 8.677 | 13.297 | total=196 200×196 2xx=196/非2xx=0/transport=0/no_status=0/err=0 |

## 3. 配对段（同一 perf_id 的 server_total ∩ below_admit）

| repeat | arm | 配对数 | server_total/admit n | server_total p95 | admit p95 | below_admit p95 |
| --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | 510 | 510/510 | 86.287 | 6.711 | 79.576 |

原始段分位数（未配对，全 ID）：

| repeat | arm | server_total n/p95/max | admit n/p95/max | admit_skipped n/p95/max | below_admit n/p95/max | 每请求 pg 合计 p95 |
| --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | 510/86.287/134.565 | 510/6.711/25.503 | 0/—/— | 510/79.576/115.925 | 15.107 |

窗口外段记录（未进相位分桶；检查时钟偏差或窗口原点）：

| repeat | arm | 窗口外条数 |
| --- | --- | --- |
| 99 | `R1G1` | map[admit:96 below_admit:96 server_total:96] |

## 4. PG 语句（pgq）

| repeat | arm | pgq 条数 | SQL 前缀数 | 前≤8 前缀（条数/p95） | 每请求语句数（以客户端 perf_id 为基数） | 分布 |
| --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.941)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=2.186)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.851)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.912)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.953)<br>`begin isolation level repeatable…`×470(p95=1.654)<br>`rollback`×470(p95=1.666)<br>`INSERT INTO event_obligation (…`×40(p95=2.195) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |

## 5. 预热样本（单独小节，不并入稳态）

| repeat | arm | 预热样本 | query n | query p95 | query max | query 状态计数 |
| --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | 104 | 80 | 15.387 | 26.730 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |

## 6. 跨重复 p95 的逐重复值 / 中位数 / 极差

| arm | 指标 | 逐重复值；中位数；极差(min–max) |
| --- | --- | --- |
| `R1G1` | client_query_p95_ms | r99=+114.283；中位数=114.283；极差=[114.283, 114.283] |
| `R1G1` | server_total_p95_ms | r99=+86.287；中位数=86.287；极差=[86.287, 86.287] |
| `R1G1` | admit_p95_ms | r99=+6.711；中位数=6.711；极差=[6.711, 6.711] |
| `R1G1` | below_admit_p95_ms | r99=+79.576；中位数=79.576；极差=[79.576, 79.576] |
| `R1G1` | pg_total_p95_ms | r99=+15.107；中位数=15.107；极差=[15.107, 15.107] |

## 7. 配对完整性与不变量（违规逐条可见）

| repeat | arm | 客户端样本 | 无 perf_id | client id | server_total id | 双向未配对 | below_admit 未配对 | 重复 id | 缺失/多出段计数 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |

不变量检查（checked=受检 ID/记录数，violations=违规数）：

| 不变量 | checked | violations | 备注 |
| --- | --- | --- | --- |
| server_total_ge_admit_plus_below_admit(±0.05ms) | 510 | 0 | 已评估 |
| sum(pgq)_le_below_admit_plus_1ms | 510 | 0 | 已评估 |
| admit_skipped_only_when_limiter_off | 0 | 0 | 已评估 |

无违规记录。

## 8. 因子对比（差值对比（禁止相加/占比证明））

### client_query_p95_ms

稳态客户端 query 类 p95（含全部状态码与错误响应）

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | 无可用重复（缺失 [99]） |
| 水平 R1G0 | 无可用重复（缺失 [99]） |
| 水平 R0G1 | 无可用重复（缺失 [99]） |
| 水平 R1G1 | r99=+114.283；中位数=114.283；极差=[114.283, 114.283] |
| 效应 R_at_G0 | 无可用重复（缺失 [99]） |
| 效应 R_at_G1 | 无可用重复（缺失 [99]） |
| 效应 G_at_R0 | 无可用重复（缺失 [99]） |
| 效应 G_at_R1 | 无可用重复（缺失 [99]） |
| 效应 interaction | 无可用重复（缺失 [99]） |
| 效应 total_diff_R0G0_to_R1G1 | 无可用重复（缺失 [99]） |

### server_total_p95_ms

server_total 段 p95（全部 ID）

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | 无可用重复（缺失 [99]） |
| 水平 R1G0 | 无可用重复（缺失 [99]） |
| 水平 R0G1 | 无可用重复（缺失 [99]） |
| 水平 R1G1 | r99=+86.287；中位数=86.287；极差=[86.287, 86.287] |
| 效应 R_at_G0 | 无可用重复（缺失 [99]） |
| 效应 R_at_G1 | 无可用重复（缺失 [99]） |
| 效应 G_at_R0 | 无可用重复（缺失 [99]） |
| 效应 G_at_R1 | 无可用重复（缺失 [99]） |
| 效应 interaction | 无可用重复（缺失 [99]） |
| 效应 total_diff_R0G0_to_R1G1 | 无可用重复（缺失 [99]） |

### admit_p95_ms

admit 段 p95

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | 无可用重复（缺失 [99]） |
| 水平 R1G0 | 无可用重复（缺失 [99]） |
| 水平 R0G1 | 无可用重复（缺失 [99]） |
| 水平 R1G1 | r99=+6.711；中位数=6.711；极差=[6.711, 6.711] |
| 效应 R_at_G0 | 无可用重复（缺失 [99]） |
| 效应 R_at_G1 | 无可用重复（缺失 [99]） |
| 效应 G_at_R0 | 无可用重复（缺失 [99]） |
| 效应 G_at_R1 | 无可用重复（缺失 [99]） |
| 效应 interaction | 无可用重复（缺失 [99]） |
| 效应 total_diff_R0G0_to_R1G1 | 无可用重复（缺失 [99]） |

### below_admit_p95_ms

below_admit 段 p95

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | 无可用重复（缺失 [99]） |
| 水平 R1G0 | 无可用重复（缺失 [99]） |
| 水平 R0G1 | 无可用重复（缺失 [99]） |
| 水平 R1G1 | r99=+79.576；中位数=79.576；极差=[79.576, 79.576] |
| 效应 R_at_G0 | 无可用重复（缺失 [99]） |
| 效应 R_at_G1 | 无可用重复（缺失 [99]） |
| 效应 G_at_R0 | 无可用重复（缺失 [99]） |
| 效应 G_at_R1 | 无可用重复（缺失 [99]） |
| 效应 interaction | 无可用重复（缺失 [99]） |
| 效应 total_diff_R0G0_to_R1G1 | 无可用重复（缺失 [99]） |

### pg_total_p95_ms

每请求 PG 语句耗时合计的 p95（仅含 ≥1 条 pgq 的请求）

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | 无可用重复（缺失 [99]） |
| 水平 R1G0 | 无可用重复（缺失 [99]） |
| 水平 R0G1 | 无可用重复（缺失 [99]） |
| 水平 R1G1 | r99=+15.107；中位数=15.107；极差=[15.107, 15.107] |
| 效应 R_at_G0 | 无可用重复（缺失 [99]） |
| 效应 R_at_G1 | 无可用重复（缺失 [99]） |
| 效应 G_at_R0 | 无可用重复（缺失 [99]） |
| 效应 G_at_R1 | 无可用重复（缺失 [99]） |
| 效应 interaction | 无可用重复（缺失 [99]） |
| 效应 total_diff_R0G0_to_R1G1 | 无可用重复（缺失 [99]） |

## 9. 开销对照：R1G1 vs R1G1:nocoll（客户端 query）

未发现 nocoll 臂，未产出对照。

## 10. 候选规则检查（候选规则，非裁决）

| 候选规则（非裁决） | 状态 | 观测 | 口径注记 |
| --- | --- | --- | --- |
| 候选规则，非裁决：正常态（窗口 [0s,8s)，排除 burst 档）admit p95 ≤ 2ms | 证据不足 | 未找到 limiter_on=true/background_on=false 的臂 | 正常态口径取相位分桶 [0s,4s)+[4s,8s)；[8s,12s) 与固定 burst 重叠，故排除。评估臂：limiter_on=true/background_on=false。 |
| 候选规则，非裁决：客户端 query p95 的 |R 效应| ≥ 10ms（R@G0 与 R@G1） | 证据不足 | R@G0/R@G1 均无可用重复（臂或分位数缺失） | R 效应是同一指标、同一重复内的臂间差值（差值对比（禁止相加/占比证明））；两个水平都可用的重复才计入。 |
| 候选规则，非裁决：客户端 query p95 的 R 效应 @G1 占总差（R1G1−R0G0）≥50% | 证据不足 | R@G1 或总差的重复级数值缺失 | 占比仅作候选规则标注，不构成因果或收益证明；总差 ≤0.05ms 的重复不计入。 |

## 11. 时钟步进与校正（wall clock 回拨）

- 检测口径：每臂 server_total 记录按 **id 升序**取相邻标签差 Δ；Δ<−50ms 记为回拨步进并校正；Δ>+500ms 记为「可疑前跳/停顿」（ambiguous=true），只披露不校正。
- 适用范围：**仅当** first_affected_id > first_steady_id（稳态 client perf_id 最小值；预热样本不参与该判定）时，步进才视为作用于窗口；预热期回拨不校正（window_start 与稳态标签同处位移后的时基），window_start 永不校正。
- 校正口径：以适用步进的边界 id 为序，对 id > boundary_id 的记录标签加 Σ|回拨|；segments 与带 perf_id 的客户端样本统一校正，无 id 的样本不参与相位。
- 相位：相位分桶一律使用**校正后标签**与 window_start；原始 wall 相位计数保留为 `client_query_phases_wall` 供审计。
- 窗口：window_seconds 为原始 wall 时长；window_seconds_corrected = 原始 + Σ(作用于窗口的回拨 |Δ|)（无适用步进时即为原始值）；作用于窗口的可疑前跳则标注不可判定原因。
- 限制：前跳不可判（停顿/GC/事件循环阻塞与时钟前跳不可区分）；跨机绝对时间不可比；时长类指标（duration_ms / dur_ns）取自单调时钟，不受回拨影响；Σ|Δ| 含一次正常请求间隔，校正后标签可能仍偏早约一个间隔（≈0.1s），相位归类可用、精确时刻不可复原。

| repeat | arm | kind | boundary_id | first_affected_id | delta_ms | correction_ms | ambiguous | applies_to_window | reason | 注记 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | backward | 111 | 112 | -2438.759 | 2438.759 | false | true |  |  |

| repeat | arm | phase_method | 回拨步数 | 作用于窗口的回拨 | 可疑前跳 | window_seconds（raw） | window_seconds_corrected | 注记 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 99 | `R1G1` | step_corrected | 1 | 1 | 0 | 9.521 | 11.960 | 检测到 1 次回拨中 1 次作用于窗口（合计 2438.759ms）：边界之后的标签已校正，本次移原始 wall 相位保留在 client_query_phases_wall |

