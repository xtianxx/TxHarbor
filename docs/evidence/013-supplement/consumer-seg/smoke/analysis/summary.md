# 013-supplement 消费者追赶分段测量分析

- 证据根：`docs/evidence/013-supplement/consumer-seg/smoke`
- 运行：2 个（on=1, off=1, pristine=0, unknown=0）
- 生成时间：2026-10-08T10:53:19+08:00（唯一非确定性字段；其余输出对同一证据树可复现）
- 输出：本文件与 `summary_all.json`；每轮 `runs/<label>/summary.json`

## 0. 口径（冻结 v1）

- 所有百分比仅诊断参考，非 SLO/阈值。
- 组内/组间统计为描述性汇总：n 小、方差未测；不得因一次小差值声称『无扰动』或其他因果结论。
- 分位数：线性插值，rank = p·(n−1)。
- tx_total 口径：commit 行 (t_us+dur_us) − begin 行 t_us；无 begin/commit 的事件跳过并计数。
- 嵌套容差：Σsql ≤ process_dur + 行数×1µs；tx_total ≤ process_dur + 2µs（µs 量化裕度）。
- tail 裁剪：窗口 [publish_done_us, poll_confirm_us]；partial 仅计窗内交叠；residual = tail − Σ窗内覆盖。
- report↔anchors 对账：秒值容差 1e-6（=1µs，锚点分辨率）；速率容差 = 1e-6 + |rate|·1µs/时长（µs 量化传播）；report 的 *_seconds 扩展字段是相对 drain_start 的偏移。
- 检测延迟为锚点差值（µs）；applied_cb_latency 是回调与提交排序之差，可为负（回调在事件处理期间触发）。
- 锚点时间字段的 0 是「未采集」哨兵（如 OFF 臂无 tracer/回调行）：对应延迟渲染为 -（not collected），report 对应字段须同为 0/缺失，绝不做 0 的偏移换算。
- 顶层时长为 s，检测延迟为 µs，分段/采样时长为 µs（除标注外）。

## 1. 逐轮概览

| label | group | collection | n | rc | publish_s | tail_s | total_s | e2e_rate | 观察延迟(µs) | 完成延迟(µs) | 确认延迟(µs) | cb延迟(µs) | timing源 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r01_on | on | seg | 50 | 0 | 0.366185 | 0.302342 | 0.668527 | 74.791295 | 988 | 993 | 30347 | 12 | anchors |
| r02_off | off | seg | 50 | 0 | 0.317958 | 0.402936 | 0.720894 | 69.358325 | 1907 | 1913 | - | - | anchors |

`-` 表示未采集/不可得；`e2e_rate` = n / total_s（events/s）。

## 2. 检查（checks）

| label | anchors_complete | counts_ok | drops_ok | overlap_ok | nesting_ok | report_vs_anchors_ok | files_missing |
| --- | --- | --- | --- | --- | --- | --- | --- |
| r01_on | OK | OK | OK | OK | OK | OK |  |
| r02_off | OK | OK | OK | OK | OK | OK |  |

**r02_off** details：
- note: overlap_ok: loop.csv 未采集（未评估）
- note: nesting_ok: loop.csv/sql.csv 未采集（未评估）
- note: report_vs_anchors_ok: last_commit_end_seconds, last_applied_cb_seconds = 0 哨兵（该观测未采集，如 OFF 臂无 tracer/回调行）；report 对应值同为 0，按一致处理，不构成偏移比较

## 3. 分段统计（loop/sql 种类，µs）

### r01_on（loop 行=156, sql 行=351, 解析失败 loop=0 sql=0）

| kind | n | mean | p50 | p95 | p99 | max | min |
| --- | --- | --- | --- | --- | --- | --- | --- |
| loop.poll | 2 | 197846.500 | 197846.500 | 357824.650 | 372044.930 | 375600.000 | 20093.000 |
| loop.process | 50 | 5236.580 | 4935.000 | 6315.200 | 10376.350 | 11202.000 | 4407.000 |
| loop.mark | 50 | 6.860 | 7.000 | 9.000 | 11.040 | 13.000 | 4.000 |
| loop.rebalance | 2 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| loop.lag | 2 | 10239.500 | 10239.500 | 19399.250 | 20213.450 | 20417.000 | 62.000 |
| sql.begin | 50 | 600.720 | 584.500 | 796.900 | 892.010 | 917.000 | 441.000 |
| sql.commit | 50 | 647.460 | 636.500 | 764.650 | 781.730 | 793.000 | 524.000 |
| sql.rollback | - | - | - | - | - | - | - |
| sql.inbox_insert | 50 | 792.600 | 732.500 | 1068.600 | 2145.380 | 2262.000 | 585.000 |
| sql.version_read | 50 | 703.620 | 635.500 | 939.350 | 1864.260 | 2171.000 | 547.000 |
| sql.version_probe | - | - | - | - | - | - | - |
| sql.effect_insert | 50 | 702.060 | 653.500 | 859.950 | 1509.450 | 1654.000 | 561.000 |
| sql.version_upsert | 50 | 700.140 | 640.000 | 820.650 | 1503.290 | 1542.000 | 589.000 |
| sql.progress_update | 50 | 708.080 | 663.000 | 923.800 | 1522.360 | 1540.000 | 574.000 |
| sql.quarantine_insert | - | - | - | - | - | - | - |
| sql.other | 1 | 1763.000 | 1763.000 | 1763.000 | 1763.000 | 1763.000 | 1763.000 |
| tx_total | 50 | 5062.700 | 4778.000 | 6108.150 | 10088.440 | 10796.000 | 4254.000 |

tx_events=50, tx_skipped=0（无 begin/commit）
poll: 次数=2, 空 poll=1（records=0）, records min/mean/max=0/25.000/50, Σrecords=50；直方图 0:1 32-63:1
sql_unattributed（seq=-1 行）=1；无 process span 的 sql 行=0；未知 loop kind 行=0

## 4. tail 裁剪（窗口 [publish_done_us, poll_confirm_us]）

### r01_on

| kind | spans | full | partial | outside | 窗内覆盖(µs) | raw full(µs) |
| --- | --- | --- | --- | --- | --- | --- |
| lag | 2 | 1 | 0 | 1 | 20417 | 20417 |
| mark | 50 | 50 | 0 | 0 | 343 | 343 |
| poll | 2 | 0 | 2 | 0 | 19207 | 0 |
| process | 50 | 50 | 0 | 0 | 261829 | 261829 |
| rebalance | 2 | 1 | 0 | 1 | 0 | 0 |

window=[10621407, 10923749]µs, tail=302342µs, 覆盖=301796µs, residual=546µs（0.181%）, 交叠对=0, 负时长=0, 零时长=2（含 rebalance 点事件等无时长 span）, epoch 前=0

> raw full spans (完整批次) 的总时长不得与 tail 直接比较：本表已按窗口 [publish_done_us, poll_confirm_us] 裁剪 —— full 全额计入、partial 仅计窗内交叠、outside 不计入；residual = tail − Σ窗内覆盖。raw_full_spans_us 仅供核对，不作结论。

## 5. 计数与丢失

| label | process | applied | duplicate | version_skip | quarantined | errors | sql_spans | sql_unattributed | applied_cb | loop 计数 | drops | identity | applied==n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r01_on | 50 | 50 | 0 | 0 | 0 | 0 | 351 | 1 | 50 | process=50 cb=50 err=0 sql=351 seq=-1:1 | loop_unpaired=0 samples_dropped=0 (meta) | OK | OK |
| r02_off | 50 | 50 | 0 | 0 | 0 | 0 | 0 | 0 | 50 | - | loop_unpaired=0 samples_dropped=0 (meta) | OK | OK |

## 6. 曲线十分位（达到各比例的时间，相对 drain_start，ms）

| label/系列 | 10% | 20% | 30% | 40% | 50% | 60% | 70% | 80% | 90% | 100% |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r01_on/applied | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 |
| r01_on/published | 400.344 | 400.344 | 400.344 | 400.344 | 400.344 | 400.344 | 400.344 | 400.344 | 400.344 | 400.344 |
| r01_on/progress | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 | 600.367 |
| label/系列 | 10% | 20% | 30% | 40% | 50% | 60% | 70% | 80% | 90% | 100% |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r02_off/applied | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 |
| r02_off/published | 401.005 | 401.005 | 401.005 | 401.005 | 401.005 | 401.005 | 401.005 | 401.005 | 401.005 | 401.005 |
| r02_off/progress | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 | 600.490 |

`-` = 未达到（系列最大值为 0，或 anchors 缺失导致相对基准不可得）。

## 7. 分区/offset 与 lag_final

### r01_on

| partition | process span 数 | 最大 offset | 错误 outcome（s1 空） |
| --- | --- | --- | --- |
| 0 | 9 | 8 | 0 |
| 1 | 10 | 9 | 0 |
| 2 | 8 | 7 | 0 |
| 3 | 5 | 4 | 0 |
| 4 | 12 | 11 | 0 |
| 5 | 6 | 5 | 0 |

process outcome 分布：applied=50（含错误合计 0）

lag_final（anchors）：{0=0 1=0 2=0 3=0 4=0 5=5}

### r02_off


lag_final（anchors）：{0=0 1=0 2=0 3=0 4=2 5=6}

## 8. 发布周期（publish.csv）

| label | rows | 生产周期 | 边界行 | Σclaimed | Σacked | Σreleased | Σblocked | 末轮 pending_seen | 末轮终止形态 | 周期不变量违例 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r01_on | 2 | 1 | 1 | 50 | 50 | 0 | 0 | 0 | OK | 0 |

`边界行` = 冻结的末轮观测行（pending_seen=0 且无发布结果）；`末轮终止形态` = 该行形态成立。

## 9. 跨轮对照（描述性）

### publish_s（单位 s）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 0.366185 | 0.366185 | 0.366185 |
| off | 1 | 0.317958 | 0.317958 | 0.317958 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 0.048227，相对 15.168%

逐轮值：r01_on(on)=0.366185, r02_off(off)=0.317958

### tail_s（单位 s）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 0.302342 | 0.302342 | 0.302342 |
| off | 1 | 0.402936 | 0.402936 | 0.402936 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 -0.100594，相对 -24.965%

逐轮值：r01_on(on)=0.302342, r02_off(off)=0.402936

### total_s（单位 s）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 0.668527 | 0.668527 | 0.668527 |
| off | 1 | 0.720894 | 0.720894 | 0.720894 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 -0.052367，相对 -7.264%

逐轮值：r01_on(on)=0.668527, r02_off(off)=0.720894

### e2e_rate（单位 events/s）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 74.791295 | 74.791295 | 74.791295 |
| off | 1 | 69.358325 | 69.358325 | 69.358325 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 5.432970，相对 7.833%

逐轮值：r01_on(on)=74.791295, r02_off(off)=69.358325

### applied_at_publish_done（单位 events）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 0.000000 | 0.000000 | 0.000000 |
| off | 1 | 0.000000 | 0.000000 | 0.000000 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 0.000000，相对 -%

逐轮值：r01_on(on)=0.000000, r02_off(off)=0.000000

### publish_observe_latency_us（单位 us）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 988.000000 | 988.000000 | 988.000000 |
| off | 1 | 1907.000000 | 1907.000000 | 1907.000000 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 -919.000000，相对 -48.191%

逐轮值：r01_on(on)=988.000000, r02_off(off)=1907.000000

### publish_done_latency_us（单位 us）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 993.000000 | 993.000000 | 993.000000 |
| off | 1 | 1913.000000 | 1913.000000 | 1913.000000 |
| pristine | 0 | - | - | - |

组间差 on−off：绝对 -920.000000，相对 -48.092%

逐轮值：r01_on(on)=993.000000, r02_off(off)=1913.000000

### consume_confirm_latency_us（单位 us）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 30347.000000 | 30347.000000 | 30347.000000 |
| off | 0 | - | - | - |
| pristine | 0 | - | - | - |

组间差 on−off：不可得（缺 on 或 off 组值）

逐轮值：r01_on(on)=30347.000000

### applied_cb_latency_us（单位 us）

| 组 | n | median | min | max |
| --- | --- | --- | --- | --- |
| on | 1 | 12.000000 | 12.000000 | 12.000000 |
| off | 0 | - | - | - |
| pristine | 0 | - | - | - |

组间差 on−off：不可得（缺 on 或 off 组值）

逐轮值：r01_on(on)=12.000000

> 描述性汇总：n 小、方差未测；组基准为组内中位数；组间差 = (on 中位数 − off 中位数)，相对% 以 off 中位数为分母。不得因一次小差值声称『无扰动』或其他因果结论；所有百分比仅诊断参考，非 SLO/阈值。

## 10. 异常与不可解释项

未发现可验证异常（checks 全部通过且无解析/交叠/嵌套/计数问题）。

## 11. 未采集清单

- r02_off（seg）：未采集: loop.csv（loop 分段）；未采集: sql.csv（SQL 分段）；未采集: publish.csv（发布周期）

