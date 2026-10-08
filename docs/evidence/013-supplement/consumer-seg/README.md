# 013 验证补充：消费者追赶分段测量批次（consumer-seg）

- 特征/范围：`013-reliable-event-infrastructure` 的补充验证批次（V-CATCHUP 分段）。目标是**观察**消费追赶尾部的构成，不实施任何优化。
- 分支：`perf/consumer-catchup-seg`；基线 `origin/main@1e09c61e91ac8d98384cd5a994152c69834a7cfa`。
- 批次以「基线 + 未提交的测量改动」运行（与 019 批次同样的纪律）；测量源码的权威身份是 `source_fingerprint.txt`（逐文件 sha256）。
- 纪律：
  - 测点为 observation-only 接缝（`perf` 构建标签；普通构建 no-op），不改变业务调用顺序、事务边界、去重、版本守卫、恢复或位点语义；
  - 原始 pristine 装置 `internal/events/backlog_drain_integration_test.go` 未改动（sha256 `4c542c19b12707fa47ab70ae3a82845f4e7da321234e5b828924f572e9dd3554`，1180 行）；
  - 所有数值仅为同机单主机/容器化实测与描述性统计；**不是生产 SLO、不是容量结论、不是阈值**；百分比仅诊断参考。
  - 不宣称跨系统「恰好一次」：投递为 at-least-once，幂等由持久 inbox 承担（与既有证据边界一致）。

## 1. 装置与测点

真实中间件、真实代码路径不变，只加观察：

| 组件 | 说明 |
|---|---|
| 载体 | `internal/events/backlog_drain_integration_test.go`（`integration_backlog`，pristine 对照）与其分段孪生 `internal/events/backlog_drain_seg_integration_test.go`（`integration_backlog && perf`） |
| 接缝 | `internal/events/perf_seam_none.go`（`!perf`，no-op 冻结 API）+ `perf_seam_perf.go`（`perf`，装 sink 前惰性）；调用点仅 `KafkaConsumer.Run`（+9/−1 行） |
| recoder/tracer | `backlog_drain_seg_recorder_test.go`：span 配对、批记录数回填（由 process span 窗口推导）、pgx QueryTracer（仅 marked ctx 记录；未标记查询零分配转发） |
| 臂语义 | `on`=记录 span/SQL/CSV + tracer；`off`=相同接缝调用与计数、不存 span 不挂 tracer（匹配对照）；`pristine`=无接缝二进制（书挡参照） |
| 离线分析 | `scripts/consumerseg/analyze/`（stdlib；`-dir <evidence>` 写 `analysis/summary.md|json` 与每轮 `runs/<label>/summary.json`） |
| 批次 runner | `scripts/consumerseg/run_batch.sh`（串行、逐轮 log/.rc/exit_code、env 快照、batch 级 environment/fingerprint；首次失败即中止；目标根须不存在，由脚本原子独占创建，已存在即退出 2） |
| 边界审计 | 接缝文件不含 broker 词表（T016 扫描）/不 import franz-go（T085）；`go test ./internal/events/` 与 `TestT085EventsImportBoundary` 全绿 |

测点覆盖（格式 v1，monotonic µs relative to epoch；wall 仅环境记录）：
`loop.csv`（poll/process/mark/rebalance/lag/applied_cb）、`sql.csv`（begin/commit/rollback/7 类业务语句/other，seq=事件序号）、`publish.csv`（逐周期 count/publish/claimed/acked/released/blocked + 末轮 pending=0 边界行）、`samples.csv`（200ms：pending/published/applied/ledger/progress + pgxpool 累计计数）、`anchors.json`（t0、发布完成及其观察、第 N 次 commit 结束、applied 回调、轮询确认、consumer 停止、lag_final）、`meta.json`（配置/版本/丢弃计数）。

口径要点（冻结，详见 `analysis/summary.md` §0）：
- 发布完成判据 = DB 观察 `pending=0`（Broker ack 经 ProduceSync 后 mark 落库，已被包含）；观察延迟=末轮发布结束→观察到 0 的差值（逐轮 663–4400µs）。
- 消费完成判据 = `applied=N`（效果事务 COMMIT；含 inbox/versions/effect/progress 同事务）；**Kafka 自动提交不是效果完成边界**（`lag_final` 记录了读出时刻的位点落后，0–33 条）。
- `39.60−1.59` 式减法仅得到「发布排空之后的追赶尾巴」；重叠期已消费量（`applied_at_publish_done` 219–256）说明 39.60s 内消费全程与发布并行。
- 等待时间不得预设为零：`poll` 等待被实测，但两个窗口**分列且不得混用**——**尾窗** `[publish_done, poll_confirm]` 内 `poll` 覆盖 20497/80829/36809/61020µs（20–81ms；r02/r05/r06/r09），**总窗** `[drain_start, poll_confirm]` 内覆盖 414068/447783/503617/521530µs；两窗差额来自发布重叠期的长 fetch 等待（逐轮 ~0.37–0.47s 的首次 poll，只计入总窗、不属于 tail）。

## 2. 固定实验条件

N=10000；全部原参数保持（测试硬编码值，均记录在每轮 `meta.json`）：

| 组件 | 参数 |
|---|---|
| Publisher | batch=250、lease_ttl=60s、backoff 1s/30s、jitter=0、**直驱节奏**（测试逐周期调用 PublishOnce，无 ticker） |
| KafkaSink | delivery_timeout=10s、request_timeout=3s、max_inflight=5、max_buffered=2048 |
| Consumer | gap_wait=1s、backoff 50ms/1s、retry_limit=5、chain_id=31337 |
| KafkaConsumer | poll_batch=500、commit_interval=200ms、group_prefix=`txharbor` |
| 池/主题/镜像 | pgxpool max=8/min=1/idle=5m；topic `txharbor.events.v1` 6 分区；Kafka `confluentinc/confluent-local:7.9.10`、PG `postgres:18.6-trixie`（testcontainers v0.44.0） |

运行顺序（counterbalanced，pristine 书挡）：`pristine on off off | on on off off | on pristine`。

| label | mode | rc | started (UTC) | 结束 (UTC) |
|---|---|---|---|---|
| r01_pristine | pristine | 0 | 2026-10-08T02:53:59Z | 02:55:29Z |
| r02_on | on | 0 | 02:55:29Z | 02:57:02Z |
| r03_off | off | 0 | 02:57:02Z | 02:58:35Z |
| r04_off | off | 0 | 02:58:35Z | 03:00:10Z |
| r05_on | on | 0 | 03:00:10Z | 03:01:43Z |
| r06_on | on | 0 | 03:01:43Z | 03:03:17Z |
| r07_off | off | 0 | 03:03:18Z | 03:04:52Z |
| r08_off | off | 0 | 03:04:52Z | 03:06:25Z |
| r09_on | on | 0 | 03:06:25Z | 03:08:00Z |
| r10_pristine | pristine | 0 | 03:08:00Z | 03:09:32Z |

环境：go1.26.5 / WSL2 / 16 CPU；同机后台恒有用户 compose 栈（redis/kafka/postgres/anvil，kafka 容器常驻较高 CPU）——逐轮快照见 `logs/<label>.env` 与 `environment.txt`。

## 3. 结果

### 3.1 顶层（全部 10 轮）

| label | group | publish_s | tail_s | total_s | e2e_rate | 发布观察延迟(µs) | 消费确认延迟(µs) |
|---|---|---|---|---|---|---|---|
| r01_pristine | pristine | 1.7508 | 39.5225 | 41.2734 | 242.29 | - | - |
| r02_on | on | 1.6139 | 42.6412 | 44.2551 | 225.96 | 820 | 27621 |
| r03_off | off | 1.7621 | 42.6436 | 44.4057 | 225.20 | 663 | - |
| r04_off | off | 1.7365 | 42.6324 | 44.3690 | 225.38 | 4399 | - |
| r05_on | on | 1.7299 | 42.9424 | 44.6723 | 223.85 | 785 | 89337 |
| r06_on | on | 1.6198 | 42.9376 | 44.5574 | 224.43 | 974 | 43422 |
| r07_off | off | 1.7949 | 42.6376 | 44.4326 | 225.06 | 2514 | - |
| r08_off | off | 1.6196 | 42.7381 | 44.3576 | 225.44 | 759 | - |
| r09_on | on | 1.7604 | 42.0347 | 43.7951 | 228.34 | 804 | 67758 |
| r10_pristine | pristine | 1.8203 | 41.6380 | 43.4582 | 230.11 | - | - |

组内中位数（n 见括号）：

| metric | on (4) | off (4) | pristine (2) | on−off |
|---|---|---|---|---|
| publish_s | 1.6748 | 1.7493 | 1.7855 | −0.0745 (−4.26%) |
| tail_s | 42.7894 | 42.6406 | 40.5802 [39.5225, 41.6380] | +0.1488 (+0.35%) |
| total_s | 44.4062 | 44.3874 | 42.3658 | +0.0189 (+0.04%) |
| e2e_rate | 225.20 | 225.29 | 236.20 | −0.09 (−0.04%) |
| applied_at_publish_done | 248 | 240.5 | - | +7.5 (+3.1%) |

读数：ON 与 OFF（同接缝、不同采集）逐指标几乎相同（+0.04%～+0.35%）；pristine（无接缝二进制）更低 2.0–2.3s（~4.8%），但其两次书挡自身相差 2.12s（39.52/41.64）→ 该差值只能列为「可能包含接缝调度/记录开销与环境漂移，本批不可分解」，不作结论。

**配对差值（on−off，秒）**——配对规则：plan `pristine on off off | on on off off | on pristine`（runner 头注释声明的四对相邻 on/off）；算法：逐对差值 = on − off，四值中位 = 排序后两中间值的平均（与分析器 `rank = p·(n−1)` 线性插值一致），组中位差 = median(on) − median(off)。下表由 anchors.json 的整数 µs 读数计算（同源 report.json 浮点秒在 10⁻⁷s 量级有 ≤1µs 量化差）：

| 对 | publish_s Δ | tail_s Δ | total_s Δ |
|---|---|---|---|
| r02−r03 | −0.148237 | −0.002428 | −0.150665 |
| r05−r04 | −0.006612 | +0.309987 | +0.303375 |
| r06−r07 | −0.175190 | +0.300019 | +0.124829 |
| r09−r08 | +0.140835 | −0.703380 | −0.562545 |
| 配对差中位 | −0.0774245 | +0.1487955 | −0.0129180 |
| 组中位差 | −0.0745020 | +0.1487955 | +0.0188755 |

- total 的 **+0.0189s（≈+0.019s）是组中位差**，而配对差中位为 **−0.0129180s**（符号相反）——两个统计量必须分开引用；tail 两种算法恰好都 ≈+0.1488s（巧合，不代表统计量相同）。
- 不得把任一个统称「配对开销」：ON/OFF 只衡量**细分采集的增量**（OFF 臂仍在同一接缝上记账，只是不落 span、不挂 tracer）；pristine 两次书挡（n=2、自差 2.12s）不足以分解测点开销。

### 3.2 ON 分段（µs；完整表见 `analysis/summary.md` §3）

| run | process mean/p50/p95 | tx_total mean/p50/p95 | commit mean/p95 | begin mean | poll 次数/最大 | lag(观察) mean |
|---|---|---|---|---|---|---|
| r02_on | 4355/4221/5496 | 4304/4175/5427 | 603/778 | 552 | 22/393571 | 10482 |
| r05_on | 4389/4237/5501 | 4338/4191/5432 | 606/774 | 558 | 23/366906 | 12177 |
| r06_on | 4376/4238/5505 | 4323/4191.5/5431 | 606/773 | 554 | 22/466808 | 10890 |
| r09_on | 4299/4165/5386 | 4248/4118/5315 | 593/756 | 546 | 22/460510 | 10165 |

- 7 条业务语句（inbox_insert/version_read/effect_insert/version_upsert/progress_update + begin/commit）均值逐轮 ≈4.25ms，占 `tx_total` ≈98.8%、占 `process` ≈97.6%（r02：4254 vs 4304 vs 4355µs）。
- `mark` 均值 2.6–2.7µs；`rebalance` 0；批大小：records/批 ≤500（均值 ≈455；22 批覆盖 10000；含 1 次 0 记录 poll）。
- 连接获取：`acquire_count=10944–11008`（N 个事件事务 + 944–1008 次观察/断言查询各取一次）、`empty_acquire=2`、`canceled=0`（全部 ON/OFF 轮；见每轮 `report.json` `pool_final`）。`pool_final.acquire_duration_seconds` 是 pgx v5.11.0 `Stat().AcquireDuration()` 的**全部成功 acquire 累计耗时**（含建池/seed/monitor/断言查询，全轮快照；ON 轮 16.007887/17.700651/20.600273/18.440113ms，OFF 最大 r07 23.248503ms）——它不是排队专用等待：排队指标 `EmptyAcquireWaitTime()` **未采集**，池排队份额未测。池上限（8）不是并发约束（单消费者 goroutine）；逐事件获取耗时未单独隔离（见缺测）。

### 3.3 tail 裁剪分解（ON；窗口=[publish_done, poll_confirm]）

| run | tail (µs) | process | poll | mark | lag | Σ覆盖 | residual |
|---|---|---|---|---|---|---|---|
| r02_on | 42641170 | 42333632 | 20497 | 25667 | 230579 | 42610375 | 30795 (0.072%) |
| r05_on | 42942427 | 42550540 | 80829 | 25658 | 254450 | 42911477 | 30950 (0.072%) |
| r06_on | 42937633 | 42603704 | 36809 | 26210 | 239559 | 42906282 | 31351 (0.073%) |
| r09_on | 42034687 | 41694509 | 61020 | 26296 | 223586 | 42005411 | 29276 (0.070%) |

读数：裁剪后顶层互斥 span 覆盖 tail 的 99.93%；其中 `process` 贡献 99.09–99.28%（窗内覆盖/tail：42333632/42641170、42550540/42942427、42603704/42937633、41694509/42034687；每事件一个 span），`lag`(refreshLag) ~0.5–0.6%，poll 等待 ≤0.19%，mark 0.060–0.063%（25667/25658/26210/26296µs ÷ 各自 tail），residual ≤0.073%（span 边界的 µs 量化与循环外缝隙）。**§3.2 的 process 分位来自全程 10000 个 span，与本节「窗内覆盖/tail」不同口径，不得混用。**

### 3.4 计数、共享观测与发布周期

- 全部 ON/OFF 轮：`observed_process=10000`、`applied=10000`、`applied_cb=10000`、duplicate/version_skip/quarantined/errors=0、drops=0；ON 轮 `sql_spans=70076/70128`（7×N + other，other 为 refreshLag 的 `SELECT updated_at FROM consumer_progress`，即观察路径查询，seq=-1 正确归入 `sql_unattributed`）。
- 发布：40 个生产周期 ×250，Σclaimed=Σacked=10000，released/blocked=0，末轮 pending_seen=0 边界行形态 OK，周期不变量 0 违例。
- 业务断言与 pristine 装置一致（0 丢失、效果恰好一次、版本/进度单调、broker records=N、quarantine=0）——全部 10 轮通过（`logs/<label>.rc`=0，日志含 `--- PASS`）。

## 4. 归因支持度（只到实测刻度；不作 SLO/根因宣判）

**被实测支持的**（全部来自子段实测，非聚合外推）：
- tail ≈ Σ(`process`)：99.09–99.28%（裁剪后），残差 ≤0.073%；
- `process` 内部 ≈ 单事件事务：`tx_total` 占 process 98.8%，其中 7 条语句合计占 tx_total 98.8%（begin/commit/6 业务语句均为实测 p50/p95/p99）；
- poll 等待、refreshLag、mark 合计占 tail 逐轮 0.649/0.841/0.705/0.740%（r02/r05/r06/r09：276743/42641170、360937/42942427、302578/42937633、310902/42034687）——等待被实测而非假设为零；auto-commit 位点落后在读出时刻 0–33 条（观察值）。

**未被支持的**：
- 不采用「批处理占 tail ≥99% ⇒ 事务根因」式推理；上述为逐层实测子段的并置，不构成跨层因果证明；
- 未逐事件隔离连接获取（仅池累计统计）与 Go 调度/网络栈份额；服务端提交瞬时不可测（只测到客户端 commit 往返）；
- pristine vs 接缝臂的 ~2s 差值不可归因（pristine n=2、书挡自漂移 2.1s、环境含常驻 compose 栈与 16:00 前后批次漂移）；
- 本批配置为测试有界值，与生产默认（batch 100/1s tick、gap 10s、commit 1s）不同，绝对值不可外推。

## 5. 缺测

- 逐事件连接获取耗时与池排队份额（`EmptyAcquireWaitTime()` 未采集；池仅提供逐 tick 累计计数与全轮累计耗时）；
- 服务端提交瞬时与 PG 内部执行剖析（pg_stat_statements 等未接）；
- 跨机/多实例/生产负载回放；长稳（小时级）；
- pristine 臂无分段（按协议只有 report.json）；OFF 臂无 loop/sql/publish（协议）；
- 方差：每组 n≤4，仅描述性统计。

## 6. 原始件清单与复现入口

布局（全部在库内，压缩用 `gzip -n`，无 `.out`）：

```
docs/evidence/013-supplement/consumer-seg/
  runs/<label>/{meta.json,anchors.json,report.json,exit_code,samples.csv.gz[,loop.csv.gz,sql.csv.gz,publish.csv.gz]}
  logs/<label>.log.gz, logs/<label>.rc, logs/<label>.env, logs/summary.tsv, logs/runner.log
  environment.txt, source_fingerprint.txt, source_fingerprint_current.txt, SHA256SUMS
  CORRECTIONS.md, checks/
  analysis/{summary.md,summary_all.json}
  smoke/{runs/r01_on,runs/r02_off}/…, smoke/logs/…, smoke/analysis/…
```

复现（在仓库根；需要 Docker 与已缓存镜像）：

```sh
# 1) 冒烟（N=50，-race；可选）
TXHARBOR_BACKLOG_DRAIN_N=50 TXHARBOR_BACKLOG_SEG=1 \
TXHARBOR_BACKLOG_SEG_DIR=<dir>/smoke/runs/r01_on TXHARBOR_BACKLOG_REPORT=<dir>/smoke/runs/r01_on/report.json \
  go test -race -tags "integration_backlog perf" -count=1 -timeout 40m -run '^TestBacklogDrainSeg$' -v ./internal/events/

# 2) 批次（串行；默认 plan=pristine on off off on on off off on pristine）
#    目标根 <dir> 必须不存在：脚本以单次 mkdir 原子独占创建（不建父目录）；已存在（含空目录/符号链接）即拒绝并以退出码 2 结束，
#    不覆盖、不清理；plan 校验先于创建，更换 plan 也不得复用旧根。
scripts/consumerseg/run_batch.sh <dir>

# 3) 离线分析（可重算统计）
go run ./scripts/consumerseg/analyze -dir <dir>

# 4) 校验与解压
(cd <dir> && sha256sum -c SHA256SUMS)   # 相对证据根
zcat <dir>/runs/r02_on/loop.csv.gz | head
```

- 源码身份：`source_fingerprint.txt` 记录批次时刻的 HEAD/脏树状态与全部相关文件 sha256；`source_fingerprint_current.txt` 记录本轮（源码修正后）的对应指纹；本目录提交后与工作树一致（核验见下）。
- 独立核验：本轮可定位依据为同目录 `CORRECTIONS.md` 与 `checks/`（只读复算/核查的脚本与输出）；**原批次的独立复算为会话内记录、未归档**。

证据披露：
- 旧 smoke 批（`smoke/`，2026-10-08 02:44–02:45Z，N=50、`-race`）：只有逐轮 `.env`/`.log.gz`/`.rc`，**没有批次级 `environment.txt`/`source_fingerprint.txt`**——其源码身份绑定弱于主批（主批有批次级环境快照与逐文件 sha256 指纹），其数值只作冒烟参考。
- 旧 recorder、分析器、审计（T016/T085）及 race 声明的执行**没有独立归档日志**（仅在会话内记录，未归档）：本轮不补造日志，也不用当前（源码已变更的）运行冒充历史执行。
- 曲线（`analysis/summary.md` §6 的 samples 十分位）的分母是**各采样序列自身的最大值**：「100%」= 该序列样本内的最大值，不是 N 完成——applied 采样序列最大仅 9994/9984/9972/9965（r02/r04/r06/r08），smoke 为 42/40（N=50）。业务终态 N 由 counts/anchors 独立展示（ON/OFF 轮 applied=N=10000），不得以曲线补终点采样。

## 7. 声明

- 本目录与批次不触碰生产参数、CI、部署；不构成优化实施。
- 一切百分比/分解仅诊断参考，不登记为生产 SLO 或批准阈值。
- 原始件字节不变：`runs/`（meta/anchors/report/*.csv）、`logs/`（.env/.rc/.log.gz）、`environment.txt`、`source_fingerprint.txt` 与采集时指纹一致；本轮变更仅源码（runner/分析器）与文档/派生说明（本 README、`CORRECTIONS.md`、`checks/`、`source_fingerprint_current.txt`）。
- 「历史值」对照：2026-09-24 基线 `d2bf559` 的 1.59s/39.60s 为旧装置旧环境数值，本批 pristine 实测 1.75–1.82s/39.52–41.64s（同口径仅顶层），**新旧不合并统计**。
