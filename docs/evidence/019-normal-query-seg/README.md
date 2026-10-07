# 019 正常查询路径四条件分段测量批次（R/G 分离；测试侧接缝）

- 目的：对**正常态查询路径**做一次受控分段测量（四条件、3 重复、串行、平衡顺序），不实施任何性能调优；不改变生产默认配置或业务行为。
- 基线：`7587116eb895003fcf7a2fd91fcbd2a0939d6f05`（main）；实验分支 `perf/seg-normal-query-batch`（批次运行时 HEAD=基线 + 本目录所述测试侧改动，未提交；源码指纹见 `source_fingerprint.txt`）。
- 依赖：Go go1.26.5；镜像 postgres:18.6-trixie / ghcr.io/foundry-rs/foundry:v1.8.1 / redis:8.2.10-alpine / confluentinc/confluent-local:7.9.10；单机 WSL2（内核 6.18.33.2-microsoft-standard-WSL2）。各单元 EnvSpec 见 `raw/repeat*/<arm>/meta.json`。

## 1. 设计（条件、隔离程度、接线）

**四条件（R×G）**：

| 臂 | R=查询类限流跳 | G=发布/消费后台 runtimes |
|---|---|---|
| R0G0 | 跳过（不发 EVAL） | 不启动 |
| R0G1 | 跳过 | 启动（event-publisher + event-consumer） |
| R1G0 | 生产行为（走 admit→Redis EVAL） | 不启动 |
| R1G1 | 生产行为 | 启动 |

**隔离程度（实测证据）**：

- R 开关只作用于 `ClassQuery` 的准入跳：R0 臂每臂 `admit_skipped=470`（=80 预热 +390 稳态 query 请求）、`admit=40`（creates 未被跳过）；R1 臂 `admit_skipped=0`、`admit=510`。Redis 客户端/缓存/探针装配在四个条件下完全一致（未关闭 Redis）。因此 R0 语义是「查询类限流跳跳过」，不是「Redis 移除」。
- G 开关只决定 harness 是否启动 publisher/consumer：G0 单元 `runtime_progress={-1,-1}`；G1 单元 Kafka 高水位/消费位点实测（约 92–98）。
- 环境映射除每臂容器端点（HTTP_ADDR/KAFKA_BROKERS/REDIS_ADDR/RPC_URL）外完全一致；认证、归属、PG 权威读取与恢复读取路径在四条件下不变。
- 计时接缝（`-tags perf` 专用，普通构建为 no-op）：`/withdrawals` 请求铸造 id（响应头 `X-TXHarbor-Perf-Id` 回显并写入请求 ctx）；段定义与嵌套：`server_total ⊇ admit ⊕ below_admit ⊕ 包装开销`，`below_admit ⊇ auth+查询+响应`，`pgq` 语句级 ⊆ `below_admit`。**禁止跨段相加**；PG acquire 只以 `Pool.Stat` 窗口累计（非单请求等待）；语句级记录仅在 `TXHARBOR_PERF_SEG_TRACE=1` 时挂载。
- 开销对照：`:nocoll` 臂不启用采集（B/B′ 对照），仅用于估计采集开销，不作为 R/G 分离的对照。

## 2. 运行与复现

```
# 批次（固定顺序：开销 10 → repeat1 → repeat2 → repeat3 → 开销 11；每次调用串行）
scripts/perfseg/run_batch.sh docs/evidence/019-normal-query-seg/raw
# 每次调用：
#   TXHARBOR_SEG_ARMS=... TXHARBOR_SEG_REPEAT=... \
#   TXHARBOR_PERF_EVIDENCE_DIR=<dir> TXHARBOR_SEG_EVIDENCE_DIR=<dir> \
#   go test -tags perf -count=1 -timeout 35m -run '^TestSegNormalBatch$' -v ./internal/perf
# 离线统计（无 Docker）：
TXHARBOR_SEG_ANALYZE_DIR=$PWD/docs/evidence/019-normal-query-seg/raw \
  go test -tags perf -count=1 -run '^TestSegAnalyze$' ./internal/perf
```

- 顺序：repeat1 `R0G0,R1G0,R0G1,R1G1`；repeat2 `R1G0,R0G1,R1G1,R0G0`；repeat3 `R0G1,R1G1,R0G0,R1G0`（轮转平衡）。退出码见 `raw/logs/summary.tsv`（全部 0）。
- 负载（固定 profile，bench 输入非 SLO）：预热 8s（首档 10rps、无 burst，样本单列于 `warmup_samples.jsonl`，不并入稳态）；稳态 12s：查询阶梯 10→25→50 rps（0/4/8s）+ 50 请求 burst@8s；create 2rps；deposit 1rps。每臂每重复稳态 n=390 query；全量状态码/错误均记录。
- 注意：批次首次执行时 `run_batch.sh` 将两次开销运行写入同一目录（后者覆盖前者，遗留 `raw/repeat0/R1G1:nocoll`）。脚本已修正（开销 repeat=10/11），并按修正后的脚本补跑两次开销（repeat10/11）；`raw/repeat0/R1G1:nocoll` 作为遗留单元保留（分析器将其计入 nocoll 侧）。

## 3. 结果（详见 `raw/summary.md`、`raw/summary.json`）

**客户端 query p95（ms，逐重复）与中位数**

| 臂 | repeat1 | repeat2 | repeat3 | 中位数 |
|---|---|---|---|---|
| R0G0 | 83.2 | 61.9 | 70.2 | 70.2 |
| R1G0 | 80.4 | 76.6 | 81.5 | 80.4 |
| R0G1 | 91.4 | 108.3 | 95.9 | 95.9 |
| R1G1 | 76.2 | 94.4 | 167.6 | 94.4 |

**因子对比（同重复内差值；禁止相加/占比证明）**

| 指标 | R@G0 中位（逐重复） | R@G1 中位（逐重复） | G@R0 中位（逐重复） | G@R1 中位（逐重复） |
|---|---|---|---|---|
| 客户端 query p95 | +11.3（−2.9/+14.7/+11.3） | −13.9（−15.2/−13.9/+71.7） | +25.7（+8.1/+46.4/+25.7） | +17.8（−4.2/+17.8/+86.0） |
| below_admit p95 | −2.6（−2.6/−4.7/−2.6） | −24.2（−28.4/−24.2/+75.8） | +19.7（+19.7/+37.9/−1.3） | +18.4（−6.1/+18.4/+77.0） |

- `admit` 全窗 p95：R1 臂 6.1–19.2ms（含 burst 排队）；**规则口径（R1G0、[0s,8s) 相位、排除 burst）p95 中位 1.423ms（逐重复最大 1.522ms）** —— 健康态单次决策等待为毫秒级。
- `pg_total`（每请求语句耗时和）p95 中位 13.2–16.3ms；各因子对其影响 ≤~8ms（区间宽）。
- 开销对照（nocoll）：p95 = 97.5 / 105.5 / 98.0（中位 98.0）vs 稳态 R1G1 76.2/94.4/167.6 —— 无可见系统性采集开销（n 小，仅方向性）。
- 不变量（分析器）：`server_total ≥ admit+below_admit(±0.05ms)` 6120 项、`sum(pgq) ≤ below_admit+1ms` 6120 项、`admit_skipped 仅在 limiter_off` 2820 项，**违规 0**；配对完整（稳态 client id 全部有 server_total）；无 >200ms 的离群样本（r3 R1G1 的 burst 簇 185–195ms 除外）。
- 候选规则（**非裁决**，见 summary.candidate_rules）：规则1「健康态 admit p95 ≤2ms」满足（1.423ms）；规则2「|R 效应| ≥10ms」满足但方向不一致（@G0 +11.3 / @G1 −13.9）；规则3「R@G1 占总差 ≥50%」标注满足（73.6%），**占比不构成因果或收益证明**。

## 4. 时钟步进与校正（环境发现）

- WSL2 wall clock 在运行中发生**回拨**：批次 12 个采集单元（另有 3 个 nocoll）中检出 6 次（2.06–3.03s），其中 4 次作用于窗口（`phase_method=step_corrected`，raw 8.93–9.89s → corrected 11.96–12.04s）、2 次发生在预热期（`applies_to_window=false`，相位保持 wall，窗口本就正确）；批次前冒烟样本 `smoke/` 检出 −2.4388s（id 111→112）。
- 所有**时长**指标（客户端 DurationMS、段 dur_ns、pgq dur_ns）基于单调时钟，不受回拨影响；受影响的是 wall 时间戳派生的**相位分桶**与窗口跨度。
- 分析器（`seg_stats.go`）按 id 升序检测步进；仅对 `first_affected_id > first_steady_id`（窗口内）的步进做单步校正（`summary.clock_steps`、`window_seconds_corrected`）；预热期步进不施加窗口校正（样本与 window_start 同位移，差值不变）。校正残差 ≤~0.1s。
- 4 个 step_corrected 单元中 3 个（r1 R0G0、r2 R0G0、r2 R0G1）因 burst 落于 8s 边界前后呈 `[42,152,196]`（burst 被计入 [4s,8s)）；其余 9 个稳态单元为 `[39–42,100,250–251]`。规则1 所用 R1G0 三个重复均为 wall/干净分布。
- 无 ~1s 级停顿痕迹（无 >200ms 样本）；回拨来源为宿主/虚拟化时钟同步，非本仓库代码。

## 5. 限制与缺口

- 单机、合成固定 profile、3 重复；不构成生产容量/SLO 结论；旧 47.5/72.7ms（89ef787）仍为历史测量，本批次未复现其路径定义。
- R0 仅关闭查询类准入跳；Redis 其余装配与 creates 的准入仍在，且 R0 下 limiter 可用性状态不再由查询流量驱动。
- 相位统计受 wall 时钟步进残差影响（逐单元披露）；PG acquire 无单请求归因；nocoll 与稳态无配对重复（离散对照）。
- 未测：故障矩阵、长时间窗口、跨主机、>3 重复的方差、生产阈值裁决。

## 6. 目录

- `raw/repeat{0,1,2,3,10,11}/<arm>/`：`client_samples.jsonl.gz`（稳态全量样本）、`warmup_samples.jsonl.gz`、`server_segments.jsonl.gz`（seg+pgq，含预热与稳态）、`meta.json`（臂标志、快照、EnvSpec、计数）、`serve_stderr.log.gz`。
- `raw/summary.json|md`：离线统计（含 clock_steps、不变量、因子对比、候选规则）。
- `raw/logs/`：五次调用日志与退出码（`summary.tsv`）。
- `smoke/`：批次前冒烟单元（R1G1，时钟步进夹具）。
- `source_fingerprint.txt`、`SHA256SUMS`。
