# 分段正常查询批次 离线统计（summary.md）

- 根目录：`/home/dream/product_env/TxHarbor/docs/evidence/019-normal-query-seg/raw`
- 生成时间：2026-10-07T14:01:08.048287787Z
- schema_version：1
- 单元数：15

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

- 重复：[0 1 2 3 10 11]；单元：15；行解析失败：0
- repeat0 臂：R1G1:nocoll
- repeat1 臂：R0G0, R0G1, R1G0, R1G1
- repeat10 臂：R1G1:nocoll
- repeat11 臂：R1G1:nocoll
- repeat2 臂：R0G0, R0G1, R1G0, R1G1
- repeat3 臂：R0G0, R0G1, R1G0, R1G1
- 开销对照重复（仅 R1G1/R1G1:nocoll，不计为稳态缺口）：0, 10, 11

警告：
- 目录 logs 不匹配 repeat<N>，已忽略

相位口径：相位分桶基于**校正后标签**（详见 §11 时钟步进与校正）；phase_method=wall 表示本窗口未施加时钟校正（可能检出回拨但均发生在稳态窗口之前）。

## 2. 每臂×重复：客户端与相位

| repeat | arm | query n | p50ms | p95ms | p99ms | maxms | 状态计数（全部样本，含非 2xx/transport） |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | 390 | 8.281 | 97.548 | 108.006 | 111.778 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G0` | 390 | 7.154 | 83.237 | 95.434 | 102.262 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G1` | 390 | 7.413 | 91.381 | 105.812 | 110.436 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G0` | 390 | 8.506 | 80.387 | 92.944 | 95.277 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G1` | 390 | 8.081 | 76.174 | 87.943 | 91.356 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G0` | 390 | 7.646 | 61.895 | 72.225 | 78.116 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G1` | 390 | 7.375 | 108.303 | 142.465 | 149.622 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G0` | 390 | 8.738 | 76.572 | 87.720 | 91.407 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G1` | 390 | 9.398 | 94.356 | 111.857 | 117.837 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G0` | 390 | 7.872 | 70.201 | 79.016 | 88.516 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G1` | 390 | 7.395 | 95.903 | 113.207 | 114.418 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G0` | 390 | 8.318 | 81.541 | 95.710 | 99.429 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G1` | 390 | 8.422 | 167.572 | 187.736 | 195.214 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 10 | `R1G1:nocoll` | 390 | 9.938 | 105.451 | 122.426 | 127.534 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |
| 11 | `R1G1:nocoll` | 390 | 8.075 | 97.956 | 109.779 | 112.587 | total=390 200×390 2xx=390/非2xx=0/transport=0/no_status=0/err=0 |

相位分桶（相对 meta.window_start；[8s,12s) 与固定 burst 重叠，非纯稳态）：

| repeat | arm | 相位 | n | p50ms | p95ms | 状态计数 |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | [0s,4s) | 39 | 8.082 | 10.299 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 0 | `R1G1:nocoll` | [4s,8s) | 100 | 7.749 | 9.650 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 0 | `R1G1:nocoll` | [8s,12s) | 250 | 8.703 | 102.064 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G0` | [0s,4s) | 42 | 6.766 | 9.826 | total=42 200×42 2xx=42/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G0` | [4s,8s) | 152 | 7.705 | 90.238 | total=152 200×152 2xx=152/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G0` | [8s,12s) | 196 | 6.968 | 9.587 | total=196 200×196 2xx=196/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G1` | [0s,4s) | 39 | 6.988 | 8.825 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G1` | [4s,8s) | 100 | 7.005 | 8.877 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G1` | [8s,12s) | 250 | 7.832 | 97.509 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G0` | [0s,4s) | 39 | 7.572 | 9.321 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G0` | [4s,8s) | 100 | 8.420 | 11.833 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G0` | [8s,12s) | 250 | 8.727 | 85.570 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G1` | [0s,4s) | 39 | 7.612 | 10.095 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G1` | [4s,8s) | 100 | 8.188 | 10.220 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G1` | [8s,12s) | 250 | 8.100 | 81.028 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G0` | [0s,4s) | 42 | 7.257 | 8.868 | total=42 200×42 2xx=42/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G0` | [4s,8s) | 152 | 7.541 | 70.244 | total=152 200×152 2xx=152/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G0` | [8s,12s) | 196 | 7.838 | 10.860 | total=196 200×196 2xx=196/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G1` | [0s,4s) | 42 | 7.452 | 10.071 | total=42 200×42 2xx=42/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G1` | [4s,8s) | 152 | 7.728 | 132.452 | total=152 200×152 2xx=152/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G1` | [8s,12s) | 196 | 7.236 | 9.873 | total=196 200×196 2xx=196/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G0` | [0s,4s) | 39 | 8.293 | 10.346 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G0` | [4s,8s) | 100 | 7.703 | 9.250 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G0` | [8s,12s) | 250 | 9.329 | 81.032 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G1` | [0s,4s) | 39 | 7.761 | 9.137 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G1` | [4s,8s) | 100 | 9.252 | 11.830 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G1` | [8s,12s) | 250 | 9.791 | 101.303 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G0` | [0s,4s) | 39 | 6.890 | 8.159 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G0` | [4s,8s) | 100 | 7.712 | 11.307 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G0` | [8s,12s) | 251 | 8.210 | 72.951 | total=251 200×251 2xx=251/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G1` | [0s,4s) | 39 | 7.379 | 10.349 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G1` | [4s,8s) | 100 | 7.022 | 8.197 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G1` | [8s,12s) | 250 | 7.749 | 106.351 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G0` | [0s,4s) | 39 | 8.813 | 12.035 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G0` | [4s,8s) | 100 | 8.000 | 10.404 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G0` | [8s,12s) | 250 | 8.414 | 86.894 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G1` | [0s,4s) | 39 | 8.234 | 11.997 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G1` | [4s,8s) | 100 | 7.977 | 10.085 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G1` | [8s,12s) | 250 | 8.745 | 175.117 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 10 | `R1G1:nocoll` | [0s,4s) | 39 | 8.677 | 11.748 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 10 | `R1G1:nocoll` | [4s,8s) | 100 | 10.124 | 15.572 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 10 | `R1G1:nocoll` | [8s,12s) | 250 | 10.159 | 111.244 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |
| 11 | `R1G1:nocoll` | [0s,4s) | 39 | 7.853 | 11.427 | total=39 200×39 2xx=39/非2xx=0/transport=0/no_status=0/err=0 |
| 11 | `R1G1:nocoll` | [4s,8s) | 100 | 7.553 | 10.450 | total=100 200×100 2xx=100/非2xx=0/transport=0/no_status=0/err=0 |
| 11 | `R1G1:nocoll` | [8s,12s) | 250 | 8.492 | 103.427 | total=250 200×250 2xx=250/非2xx=0/transport=0/no_status=0/err=0 |

## 3. 配对段（同一 perf_id 的 server_total ∩ below_admit）

| repeat | arm | 配对数 | server_total/admit n | server_total p95 | admit p95 | below_admit p95 |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | 0 | 0/0 | — | — | — |
| 1 | `R0G0` | 510 | 510/40 | 60.539 | 1.816 | 60.533 |
| 1 | `R0G1` | 510 | 510/40 | 80.275 | 1.612 | 80.263 |
| 1 | `R1G0` | 510 | 510/510 | 69.316 | 8.612 | 57.900 |
| 1 | `R1G1` | 510 | 510/510 | 65.877 | 10.340 | 51.834 |
| 2 | `R0G0` | 510 | 510/40 | 56.576 | 1.509 | 56.571 |
| 2 | `R0G1` | 510 | 510/40 | 94.515 | 1.906 | 94.507 |
| 2 | `R1G0` | 510 | 510/510 | 55.652 | 6.067 | 51.863 |
| 2 | `R1G1` | 510 | 510/510 | 85.593 | 16.085 | 70.270 |
| 3 | `R0G0` | 510 | 510/40 | 58.799 | 2.014 | 58.793 |
| 3 | `R0G1` | 510 | 510/40 | 57.470 | 1.417 | 57.463 |
| 3 | `R1G0` | 510 | 510/510 | 67.741 | 12.008 | 56.215 |
| 3 | `R1G1` | 510 | 510/510 | 154.406 | 19.970 | 133.237 |
| 10 | `R1G1:nocoll` | 0 | 0/0 | — | — | — |
| 11 | `R1G1:nocoll` | 0 | 0/0 | — | — | — |

原始段分位数（未配对，全 ID）：

| repeat | arm | server_total n/p95/max | admit n/p95/max | admit_skipped n/p95/max | below_admit n/p95/max | 每请求 pg 合计 p95 |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | 0/—/— | 0/—/— | 0/—/— | 0/—/— | — |
| 1 | `R0G0` | 510/60.539/98.602 | 40/1.816/2.121 | 470/0.000/0.000 | 510/60.533/96.602 | 13.505 |
| 1 | `R0G1` | 510/80.275/139.537 | 40/1.612/2.245 | 470/0.000/0.000 | 510/80.263/137.284 | 14.505 |
| 1 | `R1G0` | 510/69.316/194.203 | 510/8.612/15.788 | 0/—/— | 510/57.900/192.395 | 13.909 |
| 1 | `R1G1` | 510/65.877/109.582 | 510/10.340/20.722 | 0/—/— | 510/51.834/93.003 | 13.194 |
| 2 | `R0G0` | 510/56.576/96.278 | 40/1.509/4.022 | 470/0.000/0.000 | 510/56.571/92.244 | 13.170 |
| 2 | `R0G1` | 510/94.515/175.586 | 40/1.906/2.633 | 470/0.000/0.000 | 510/94.507/173.511 | 21.378 |
| 2 | `R1G0` | 510/55.652/83.639 | 510/6.067/25.531 | 0/—/— | 510/51.863/75.269 | 13.176 |
| 2 | `R1G1` | 510/85.593/130.690 | 510/16.085/19.770 | 0/—/— | 510/70.270/112.499 | 16.340 |
| 3 | `R0G0` | 510/58.799/105.482 | 40/2.014/9.504 | 470/0.000/0.000 | 510/58.793/95.961 | 15.971 |
| 3 | `R0G1` | 510/57.470/102.427 | 40/1.417/22.394 | 470/0.000/0.000 | 510/57.463/80.020 | 13.613 |
| 3 | `R1G0` | 510/67.741/103.800 | 510/12.008/18.610 | 0/—/— | 510/56.215/94.997 | 14.354 |
| 3 | `R1G1` | 510/154.406/178.826 | 510/19.970/27.568 | 0/—/— | 510/133.237/158.932 | 18.563 |
| 10 | `R1G1:nocoll` | 0/—/— | 0/—/— | 0/—/— | 0/—/— | — |
| 11 | `R1G1:nocoll` | 0/—/— | 0/—/— | 0/—/— | 0/—/— | — |

窗口外段记录（未进相位分桶；检查时钟偏差或窗口原点）：

| repeat | arm | 窗口外条数 |
| --- | --- | --- |
| 1 | `R0G0` | map[admit:16 admit_skipped:80 below_admit:96 server_total:96] |
| 1 | `R0G1` | map[admit:17 admit_skipped:81 below_admit:98 server_total:98] |
| 1 | `R1G0` | map[admit:98 below_admit:98 server_total:98] |
| 1 | `R1G1` | map[admit:98 below_admit:98 server_total:98] |
| 2 | `R0G0` | map[admit:16 admit_skipped:80 below_admit:96 server_total:96] |
| 2 | `R0G1` | map[admit:16 admit_skipped:80 below_admit:96 server_total:96] |
| 2 | `R1G0` | map[admit:98 below_admit:98 server_total:98] |
| 2 | `R1G1` | map[admit:98 below_admit:98 server_total:98] |
| 3 | `R0G0` | map[admit:16 admit_skipped:80 below_admit:96 server_total:96] |
| 3 | `R0G1` | map[admit:17 admit_skipped:81 below_admit:98 server_total:98] |
| 3 | `R1G0` | map[admit:98 below_admit:98 server_total:98] |
| 3 | `R1G1` | map[admit:98 below_admit:98 server_total:98] |

## 4. PG 语句（pgq）

| repeat | arm | pgq 条数 | SQL 前缀数 | 前≤8 前缀（条数/p95） | 每请求语句数（以客户端 perf_id 为基数） | 分布 |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | 0（err=0） | 0 |  | n=0 无 pgq=0 mean=— p50=— p95=— max=0 |  |
| 1 | `R0G0` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.594)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.639)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.580)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.552)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.753)<br>`begin isolation level repeatable…`×470(p95=1.440)<br>`rollback`×470(p95=1.488)<br>`INSERT INTO event_obligation (…`×40(p95=1.482) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 1 | `R0G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.687)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.841)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.915)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.692)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.822)<br>`begin isolation level repeatable…`×470(p95=1.649)<br>`rollback`×470(p95=1.702)<br>`INSERT INTO event_obligation (…`×40(p95=2.177) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 1 | `R1G0` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.677)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.815)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.650)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.671)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.685)<br>`begin isolation level repeatable…`×470(p95=1.540)<br>`rollback`×470(p95=1.644)<br>`INSERT INTO event_obligation (…`×40(p95=1.528) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 1 | `R1G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.423)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.559)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.471)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.497)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.625)<br>`begin isolation level repeatable…`×470(p95=1.420)<br>`rollback`×470(p95=1.441)<br>`INSERT INTO event_obligation (…`×40(p95=1.966) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 2 | `R0G0` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.552)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.608)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.608)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.571)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.650)<br>`begin isolation level repeatable…`×470(p95=1.516)<br>`rollback`×470(p95=1.435)<br>`INSERT INTO event_obligation (…`×40(p95=2.321) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 2 | `R0G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=2.178)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=2.395)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=2.940)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=2.461)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=2.775)<br>`begin isolation level repeatable…`×470(p95=2.108)<br>`rollback`×470(p95=2.318)<br>`INSERT INTO event_obligation (…`×40(p95=2.229) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 2 | `R1G0` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.619)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.688)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.441)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.373)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.638)<br>`begin isolation level repeatable…`×470(p95=1.297)<br>`rollback`×470(p95=1.387)<br>`INSERT INTO event_obligation (…`×40(p95=2.423) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 2 | `R1G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.714)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.996)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.765)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.874)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.973)<br>`begin isolation level repeatable…`×470(p95=1.836)<br>`rollback`×470(p95=1.809)<br>`INSERT INTO event_obligation (…`×40(p95=2.607) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 3 | `R0G0` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.730)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.879)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.682)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.645)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.878)<br>`begin isolation level repeatable…`×470(p95=1.636)<br>`rollback`×470(p95=1.637)<br>`INSERT INTO event_obligation (…`×40(p95=2.825) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 3 | `R0G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.674)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.778)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.655)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.636)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.812)<br>`begin isolation level repeatable…`×470(p95=1.476)<br>`rollback`×470(p95=1.563)<br>`INSERT INTO event_obligation (…`×40(p95=1.565) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 3 | `R1G0` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=1.491)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.613)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=1.756)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=1.713)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=1.506)<br>`begin isolation level repeatable…`×470(p95=1.531)<br>`rollback`×470(p95=1.642)<br>`INSERT INTO event_obligation (…`×40(p95=1.491) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 3 | `R1G1` | 3890（err=0） | 20 | `SELECT caller_id, label, can_create…`×510(p95=2.035)<br>`SELECT key_id, caller_id, key_hash…`×510(p95=1.911)<br>`SELECT 1 FROM reorg_recovery_events…`×470(p95=2.481)<br>`SELECT recovery_id, phase, policy_seq,…`×470(p95=2.027)<br>`SELECT request_id, caller_id, chain_id,…`×470(p95=2.087)<br>`begin isolation level repeatable…`×470(p95=1.901)<br>`rollback`×470(p95=1.825)<br>`INSERT INTO event_obligation (…`×40(p95=1.289) | n=414 无 pgq=0 mean=7.464 p50=7.000 p95=15.000 max=15 | 15条×24, 7条×390 |
| 10 | `R1G1:nocoll` | 0（err=0） | 0 |  | n=0 无 pgq=0 mean=— p50=— p95=— max=0 |  |
| 11 | `R1G1:nocoll` | 0（err=0） | 0 |  | n=0 无 pgq=0 mean=— p50=— p95=— max=0 |  |

## 5. 预热样本（单独小节，不并入稳态）

| repeat | arm | 预热样本 | query n | query p95 | query max | query 状态计数 |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | 104 | 80 | 10.338 | 15.821 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G0` | 104 | 80 | 10.828 | 13.822 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R0G1` | 104 | 80 | 11.345 | 18.099 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G0` | 104 | 80 | 9.907 | 11.182 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 1 | `R1G1` | 104 | 80 | 13.714 | 15.951 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G0` | 104 | 80 | 10.076 | 13.318 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R0G1` | 104 | 80 | 10.788 | 17.789 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G0` | 104 | 80 | 10.315 | 11.959 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 2 | `R1G1` | 104 | 80 | 14.154 | 19.278 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G0` | 104 | 80 | 11.508 | 37.198 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R0G1` | 104 | 80 | 9.649 | 13.426 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G0` | 104 | 80 | 12.254 | 13.781 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 3 | `R1G1` | 104 | 80 | 11.195 | 13.728 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 10 | `R1G1:nocoll` | 104 | 80 | 42.352 | 240.297 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |
| 11 | `R1G1:nocoll` | 104 | 80 | 12.383 | 21.222 | total=80 200×80 2xx=80/非2xx=0/transport=0/no_status=0/err=0 |

## 6. 跨重复 p95 的逐重复值 / 中位数 / 极差

| arm | 指标 | 逐重复值；中位数；极差(min–max) |
| --- | --- | --- |
| `R0G0` | client_query_p95_ms | r1=+83.237 r2=+61.895 r3=+70.201；中位数=70.201；极差=[61.895, 83.237] |
| `R0G0` | server_total_p95_ms | r1=+60.539 r2=+56.576 r3=+58.799；中位数=58.799；极差=[56.576, 60.539] |
| `R0G0` | admit_p95_ms | r1=+1.816 r2=+1.509 r3=+2.014；中位数=1.816；极差=[1.509, 2.014] |
| `R0G0` | below_admit_p95_ms | r1=+60.533 r2=+56.571 r3=+58.793；中位数=58.793；极差=[56.571, 60.533] |
| `R0G0` | pg_total_p95_ms | r1=+13.505 r2=+13.170 r3=+15.971；中位数=13.505；极差=[13.170, 15.971] |
| `R0G1` | client_query_p95_ms | r1=+91.381 r2=+108.303 r3=+95.903；中位数=95.903；极差=[91.381, 108.303] |
| `R0G1` | server_total_p95_ms | r1=+80.275 r2=+94.515 r3=+57.470；中位数=80.275；极差=[57.470, 94.515] |
| `R0G1` | admit_p95_ms | r1=+1.612 r2=+1.906 r3=+1.417；中位数=1.612；极差=[1.417, 1.906] |
| `R0G1` | below_admit_p95_ms | r1=+80.263 r2=+94.507 r3=+57.463；中位数=80.263；极差=[57.463, 94.507] |
| `R0G1` | pg_total_p95_ms | r1=+14.505 r2=+21.378 r3=+13.613；中位数=14.505；极差=[13.613, 21.378] |
| `R1G0` | client_query_p95_ms | r1=+80.387 r2=+76.572 r3=+81.541；中位数=80.387；极差=[76.572, 81.541] |
| `R1G0` | server_total_p95_ms | r1=+69.316 r2=+55.652 r3=+67.741；中位数=67.741；极差=[55.652, 69.316] |
| `R1G0` | admit_p95_ms | r1=+8.612 r2=+6.067 r3=+12.008；中位数=8.612；极差=[6.067, 12.008] |
| `R1G0` | below_admit_p95_ms | r1=+57.900 r2=+51.863 r3=+56.215；中位数=56.215；极差=[51.863, 57.900] |
| `R1G0` | pg_total_p95_ms | r1=+13.909 r2=+13.176 r3=+14.354；中位数=13.909；极差=[13.176, 14.354] |
| `R1G1` | client_query_p95_ms | r1=+76.174 r2=+94.356 r3=+167.572；中位数=94.356；极差=[76.174, 167.572] |
| `R1G1` | server_total_p95_ms | r1=+65.877 r2=+85.593 r3=+154.406；中位数=85.593；极差=[65.877, 154.406] |
| `R1G1` | admit_p95_ms | r1=+10.340 r2=+16.085 r3=+19.970；中位数=16.085；极差=[10.340, 19.970] |
| `R1G1` | below_admit_p95_ms | r1=+51.834 r2=+70.270 r3=+133.237；中位数=70.270；极差=[51.834, 133.237] |
| `R1G1` | pg_total_p95_ms | r1=+13.194 r2=+16.340 r3=+18.563；中位数=16.340；极差=[13.194, 18.563] |
| `R1G1:nocoll` | client_query_p95_ms | r0=+97.548 r10=+105.451 r11=+97.956；中位数=97.956；极差=[97.548, 105.451] |

## 7. 配对完整性与不变量（违规逐条可见）

| repeat | arm | 客户端样本 | 无 perf_id | client id | server_total id | 双向未配对 | below_admit 未配对 | 重复 id | 缺失/多出段计数 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | 426 | 426 | 0 | 0 | client↛server_total=0; server_total↛client=0 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=0 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=0 |
| 1 | `R0G0` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=470 admit_skipped=40 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 1 | `R0G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=470 admit_skipped=40 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 1 | `R1G0` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 1 | `R1G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 2 | `R0G0` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=470 admit_skipped=40 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 2 | `R0G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=470 admit_skipped=40 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 2 | `R1G0` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 2 | `R1G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 3 | `R0G0` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=470 admit_skipped=40 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 3 | `R0G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=470 admit_skipped=40 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 3 | `R1G0` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 3 | `R1G1` | 426 | 12 | 414 | 510 | client↛server_total=0; server_total↛client=96 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=510 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=96 |
| 10 | `R1G1:nocoll` | 426 | 426 | 0 | 0 | client↛server_total=0; server_total↛client=0 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=0 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=0 |
| 11 | `R1G1:nocoll` | 426 | 426 | 0 | 0 | client↛server_total=0; server_total↛client=0 | server_total↛below=0; below↛server_total=0 | dup: admit=0 admit_skipped=0 below_admit=0 client=0 pgq=0 server_total=0 | missing: admit=0 admit_skipped=0 below_admit=0 server_total=0 / extra: admit=0 admit_skipped=0 below_admit=0 server_total=0 |

不变量检查（checked=受检 ID/记录数，violations=违规数）：

| 不变量 | checked | violations | 备注 |
| --- | --- | --- | --- |
| server_total_ge_admit_plus_below_admit(±0.05ms) | 6120 | 0 | 已评估 |
| sum(pgq)_le_below_admit_plus_1ms | 6120 | 0 | 已评估 |
| admit_skipped_only_when_limiter_off | 2820 | 0 | 已评估 |

无违规记录。

## 8. 因子对比（差值对比（禁止相加/占比证明））

### client_query_p95_ms

稳态客户端 query 类 p95（含全部状态码与错误响应）

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | r1=+83.237 r2=+61.895 r3=+70.201；中位数=70.201；极差=[61.895, 83.237] |
| 水平 R1G0 | r1=+80.387 r2=+76.572 r3=+81.541；中位数=80.387；极差=[76.572, 81.541] |
| 水平 R0G1 | r1=+91.381 r2=+108.303 r3=+95.903；中位数=95.903；极差=[91.381, 108.303] |
| 水平 R1G1 | r1=+76.174 r2=+94.356 r3=+167.572；中位数=94.356；极差=[76.174, 167.572] |
| 效应 R_at_G0 | r1=-2.850 r2=+14.677 r3=+11.340；中位数=11.340；极差=[-2.850, 14.677] |
| 效应 R_at_G1 | r1=-15.207 r2=-13.946 r3=+71.670；中位数=-13.946；极差=[-15.207, 71.670] |
| 效应 G_at_R0 | r1=+8.144 r2=+46.408 r3=+25.702；中位数=25.702；极差=[8.144, 46.408] |
| 效应 G_at_R1 | r1=-4.213 r2=+17.784 r3=+86.031；中位数=17.784；极差=[-4.213, 86.031] |
| 效应 interaction | r1=-12.357 r2=-28.624 r3=+60.329；中位数=-12.357；极差=[-28.624, 60.330] |
| 效应 total_diff_R0G0_to_R1G1 | r1=-7.063 r2=+32.461 r3=+97.372；中位数=32.461；极差=[-7.063, 97.372] |

### server_total_p95_ms

server_total 段 p95（全部 ID）

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | r1=+60.539 r2=+56.576 r3=+58.799；中位数=58.799；极差=[56.576, 60.539] |
| 水平 R1G0 | r1=+69.316 r2=+55.652 r3=+67.741；中位数=67.741；极差=[55.652, 69.316] |
| 水平 R0G1 | r1=+80.275 r2=+94.515 r3=+57.470；中位数=80.275；极差=[57.470, 94.515] |
| 水平 R1G1 | r1=+65.877 r2=+85.593 r3=+154.406；中位数=85.593；极差=[65.877, 154.406] |
| 效应 R_at_G0 | r1=+8.777 r2=-0.925 r3=+8.942；中位数=8.777；极差=[-0.925, 8.942] |
| 效应 R_at_G1 | r1=-14.398 r2=-8.922 r3=+96.935；中位数=-8.922；极差=[-14.398, 96.935] |
| 效应 G_at_R0 | r1=+19.736 r2=+37.939 r3=-1.329；中位数=19.736；极差=[-1.329, 37.939] |
| 效应 G_at_R1 | r1=-3.439 r2=+29.941 r3=+86.665；中位数=29.941；极差=[-3.439, 86.665] |
| 效应 interaction | r1=-23.175 r2=-7.998 r3=+87.993；中位数=-7.998；极差=[-23.175, 87.993] |
| 效应 total_diff_R0G0_to_R1G1 | r1=+5.338 r2=+29.016 r3=+95.606；中位数=29.016；极差=[5.338, 95.606] |

### admit_p95_ms

admit 段 p95

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | r1=+1.816 r2=+1.509 r3=+2.014；中位数=1.816；极差=[1.509, 2.014] |
| 水平 R1G0 | r1=+8.612 r2=+6.067 r3=+12.008；中位数=8.612；极差=[6.067, 12.008] |
| 水平 R0G1 | r1=+1.612 r2=+1.906 r3=+1.417；中位数=1.612；极差=[1.417, 1.906] |
| 水平 R1G1 | r1=+10.340 r2=+16.085 r3=+19.970；中位数=16.085；极差=[10.340, 19.970] |
| 效应 R_at_G0 | r1=+6.796 r2=+4.559 r3=+9.994；中位数=6.796；极差=[4.559, 9.994] |
| 效应 R_at_G1 | r1=+8.728 r2=+14.178 r3=+18.553；中位数=14.178；极差=[8.728, 18.553] |
| 效应 G_at_R0 | r1=-0.204 r2=+0.397 r3=-0.597；中位数=-0.204；极差=[-0.597, 0.397] |
| 效应 G_at_R1 | r1=+1.729 r2=+10.017 r3=+7.962；中位数=7.962；极差=[1.729, 10.017] |
| 效应 interaction | r1=+1.932 r2=+9.620 r3=+8.559；中位数=8.559；极差=[1.932, 9.620] |
| 效应 total_diff_R0G0_to_R1G1 | r1=+8.524 r2=+14.576 r3=+17.956；中位数=14.576；极差=[8.524, 17.956] |

### below_admit_p95_ms

below_admit 段 p95

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | r1=+60.533 r2=+56.571 r3=+58.793；中位数=58.793；极差=[56.571, 60.533] |
| 水平 R1G0 | r1=+57.900 r2=+51.863 r3=+56.215；中位数=56.215；极差=[51.863, 57.900] |
| 水平 R0G1 | r1=+80.263 r2=+94.507 r3=+57.463；中位数=80.263；极差=[57.463, 94.507] |
| 水平 R1G1 | r1=+51.834 r2=+70.270 r3=+133.237；中位数=70.270；极差=[51.834, 133.237] |
| 效应 R_at_G0 | r1=-2.633 r2=-4.709 r3=-2.578；中位数=-2.633；极差=[-4.709, -2.578] |
| 效应 R_at_G1 | r1=-28.429 r2=-24.238 r3=+75.774；中位数=-24.238；极差=[-28.429, 75.774] |
| 效应 G_at_R0 | r1=+19.730 r2=+37.936 r3=-1.330；中位数=19.730；极差=[-1.330, 37.936] |
| 效应 G_at_R1 | r1=-6.066 r2=+18.407 r3=+77.022；中位数=18.407；极差=[-6.066, 77.022] |
| 效应 interaction | r1=-25.796 r2=-19.529 r3=+78.352；中位数=-19.529；极差=[-25.796, 78.352] |
| 效应 total_diff_R0G0_to_R1G1 | r1=-8.699 r2=+13.699 r3=+74.444；中位数=13.699；极差=[-8.699, 74.444] |

### pg_total_p95_ms

每请求 PG 语句耗时合计的 p95（仅含 ≥1 条 pgq 的请求）

| 项 | 逐重复值；中位数；极差(min–max) |
| --- | --- |
| 水平 R0G0 | r1=+13.505 r2=+13.170 r3=+15.971；中位数=13.505；极差=[13.170, 15.971] |
| 水平 R1G0 | r1=+13.909 r2=+13.176 r3=+14.354；中位数=13.909；极差=[13.176, 14.354] |
| 水平 R0G1 | r1=+14.505 r2=+21.378 r3=+13.613；中位数=14.505；极差=[13.613, 21.378] |
| 水平 R1G1 | r1=+13.194 r2=+16.340 r3=+18.563；中位数=16.340；极差=[13.194, 18.563] |
| 效应 R_at_G0 | r1=+0.403 r2=+0.006 r3=-1.617；中位数=0.006；极差=[-1.617, 0.403] |
| 效应 R_at_G1 | r1=-1.311 r2=-5.038 r3=+4.949；中位数=-1.311；极差=[-5.038, 4.949] |
| 效应 G_at_R0 | r1=+0.999 r2=+8.208 r3=-2.358；中位数=0.999；极差=[-2.358, 8.208] |
| 效应 G_at_R1 | r1=-0.715 r2=+3.164 r3=+4.208；中位数=3.164；极差=[-0.715, 4.208] |
| 效应 interaction | r1=-1.714 r2=-5.044 r3=+6.566；中位数=-1.714；极差=[-5.044, 6.566] |
| 效应 total_diff_R0G0_to_R1G1 | r1=-0.312 r2=+3.170 r3=+2.591；中位数=2.591；极差=[-0.312, 3.170] |

## 9. 开销对照：R1G1 vs R1G1:nocoll（客户端 query）

配对重复：[]

| 侧 | 样本量 | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| `R1G1` | 0 | — | — | — |
| `R1G1:nocoll` | 1170 | r0=+8.281 r10=+9.938 r11=+8.075；中位数=8.281；极差=[8.075, 9.938] | r0=+97.548 r10=+105.451 r11=+97.956；中位数=97.956；极差=[97.548, 105.451] | r0=+108.006 r10=+122.426 r11=+109.779；中位数=109.779；极差=[108.006, 122.426] |
| 差值（nocoll − steady） | — | — | — | — |

参考（未配对，仅背景信息）：稳态 `R1G1` 样本量=1170，p50=r1=+8.081 r2=+9.398 r3=+8.422；中位数=8.422；极差=[8.081, 9.398] p95=r1=+76.174 r2=+94.356 r3=+167.572；中位数=94.356；极差=[76.174, 167.572] p99=r1=+87.943 r2=+111.857 r3=+187.736；中位数=111.857；极差=[87.943, 187.736]

差值对比（禁止相加/占比证明）：同一重复内 nocoll 与 steady 的客户端 query 分位数差值；无配对重复则不产出差值。

## 10. 候选规则检查（候选规则，非裁决）

| 候选规则（非裁决） | 状态 | 观测 | 口径注记 |
| --- | --- | --- | --- |
| 候选规则，非裁决：正常态（窗口 [0s,8s)，排除 burst 档）admit p95 ≤ 2ms | 满足 | 跨重复中位数 p95=1.423ms（逐重复最大值 1.522ms @repeat1，n=3 个重复） | 正常态口径取相位分桶 [0s,4s)+[4s,8s)；[8s,12s) 与固定 burst 重叠，故排除。评估臂：limiter_on=true/background_on=false。 |
| 候选规则，非裁决：客户端 query p95 的 |R 效应| ≥ 10ms（R@G0 与 R@G1） | 满足 | R_at_G0 中位数=+11.340ms；R_at_G1 中位数=-13.946ms | R 效应是同一指标、同一重复内的臂间差值（差值对比（禁止相加/占比证明））；两个水平都可用的重复才计入。 |
| 候选规则，非裁决：客户端 query p95 的 R 效应 @G1 占总差（R1G1−R0G0）≥50% | 不满足 | 跨重复中位数占比=15.3%（逐重复 n=2） | 占比仅作候选规则标注，不构成因果或收益证明；总差（R1G1−R0G0）≤0.05ms（含非正值，此时占比无定义）的重复不计入。 |

## 11. 时钟步进与校正（wall clock 回拨）

- 检测口径：每臂 server_total 记录按 **id 升序**取相邻标签差 Δ；Δ<−50ms 记为回拨步进并校正；Δ>+500ms 记为「可疑前跳/停顿」（ambiguous=true），只披露不校正。
- 适用范围：**仅当** first_affected_id > first_steady_id（稳态 client perf_id 最小值；预热样本不参与该判定）时，步进才视为作用于窗口；预热期回拨不校正（window_start 与稳态标签同处位移后的时基），window_start 永不校正。
- 校正口径：以适用步进的边界 id 为序，对 id > boundary_id 的记录标签加 Σ|回拨|；相位仅覆盖 query 类样本；无 perf_id 的 query 样本（本批为 3 个 nocoll 单元）以原始 wall 标签入桶、不参与 ID 校正；segments 与带 perf_id 的客户端样本统一校正。
- 相位：相位分桶一律使用**校正后标签**与 window_start；原始 wall 相位计数保留为 `client_query_phases_wall` 供审计。
- 窗口：window_seconds 为原始 wall 时长；window_seconds_corrected = 原始 + Σ(作用于窗口的回拨 |Δ|)（无适用步进时即为原始值）；作用于窗口的可疑前跳则标注不可判定原因。
- 限制：前跳不可判（停顿/GC/事件循环阻塞与时钟前跳不可区分）；跨机绝对时间不可比；时长类指标（duration_ms / dur_ns）取自单调时钟，不受回拨影响；Σ|Δ| 含一次正常请求间隔，校正后标签可能仍偏早约一个间隔（≈0.1s），相位归类可用、精确时刻不可复原。

| repeat | arm | kind | boundary_id | first_affected_id | delta_ms | correction_ms | ambiguous | applies_to_window | reason | 注记 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `R0G0` | backward | 106 | 107 | -2503.900 | 2503.900 | false | true |  |  |
| 1 | `R0G1` | backward | 1088 | 1089 | -2325.749 | 2325.749 | false | false | pre_window |  |
| 2 | `R0G0` | backward | 1663 | 1664 | -3030.721 | 3030.721 | false | true |  |  |
| 2 | `R0G1` | backward | 652 | 653 | -2062.185 | 2062.185 | false | true |  |  |
| 3 | `R0G0` | backward | 1393 | 1394 | -2545.145 | 2545.145 | false | true |  |  |
| 3 | `R1G1` | backward | 543 | 544 | -2211.588 | 2211.588 | false | false | pre_window |  |

| repeat | arm | phase_method | 回拨步数 | 作用于窗口的回拨 | 可疑前跳 | window_seconds（raw） | window_seconds_corrected | 注记 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | `R1G1:nocoll` | wall | 0 | 0 | 0 | 12.058 | 12.058 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 1 | `R0G0` | step_corrected | 1 | 1 | 0 | 9.452 | 11.956 | 检测到 1 次回拨中 1 次作用于窗口（合计 2503.900ms）：边界之后的标签已校正，本次移原始 wall 相位保留在 client_query_phases_wall |
| 1 | `R0G1` | wall | 1 | 0 | 0 | 12.056 | 12.056 | 检测到 1 次 wall clock 回拨，但均发生在稳态窗口之前（first_steady_id=1117）：window_start 与稳态标签同处位移后的时基，不校正 |
| 1 | `R1G0` | wall | 0 | 0 | 0 | 12.057 | 12.057 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 1 | `R1G1` | wall | 0 | 0 | 0 | 12.057 | 12.057 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 2 | `R0G0` | step_corrected | 1 | 1 | 0 | 8.927 | 11.958 | 检测到 1 次回拨中 1 次作用于窗口（合计 3030.721ms）：边界之后的标签已校正，本次移原始 wall 相位保留在 client_query_phases_wall |
| 2 | `R0G1` | step_corrected | 1 | 1 | 0 | 9.894 | 11.956 | 检测到 1 次回拨中 1 次作用于窗口（合计 2062.185ms）：边界之后的标签已校正，本次移原始 wall 相位保留在 client_query_phases_wall |
| 2 | `R1G0` | wall | 0 | 0 | 0 | 12.058 | 12.058 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 2 | `R1G1` | wall | 0 | 0 | 0 | 12.056 | 12.056 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 3 | `R0G0` | step_corrected | 1 | 1 | 0 | 9.492 | 12.037 | 检测到 1 次回拨中 1 次作用于窗口（合计 2545.145ms）：边界之后的标签已校正，本次移原始 wall 相位保留在 client_query_phases_wall |
| 3 | `R0G1` | wall | 0 | 0 | 0 | 12.058 | 12.058 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 3 | `R1G0` | wall | 0 | 0 | 0 | 12.057 | 12.057 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 3 | `R1G1` | wall | 1 | 0 | 0 | 12.058 | 12.058 | 检测到 1 次 wall clock 回拨，但均发生在稳态窗口之前（first_steady_id=607）：window_start 与稳态标签同处位移后的时基，不校正 |
| 10 | `R1G1:nocoll` | wall | 0 | 0 | 0 | 12.057 | 12.057 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |
| 11 | `R1G1:nocoll` | wall | 0 | 0 | 0 | 12.057 | 12.057 | 未检测到时钟步进（阈值：回拨 <−50ms，前跳 >+500ms） |

