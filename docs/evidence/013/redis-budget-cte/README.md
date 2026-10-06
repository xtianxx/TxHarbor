# 013 限流决策总预算：定向机制验证（2026-10-06）

- 性质：**定向机制验证 + 可控反例**，仅测试侧。生产代码、配置默认、对外错误
  映射、CI 均未改动；`ContextTimeoutEnabled` 与候选预算只出现在测试客户端
  构造（`internal/ratelimit/redisbudget_cte_test.go`）。
- 绑定：分支 `013-redis-latency-budget-design`，基线 `a59dcd1`（实验与复核
  提交保留未动）；go-redis v9.22.0（GOMODCACHE）；go1.26.5 linux/amd64。
- 复用夹具：`internal/testutil/redisgate.go` 本轮新增 `GateDrop` 态（转发
  执行 + 扣留 EVAL 应答 + 可选预算前关客户端 socket）与五项事件计数器；
  既有三态/计时语义未改（gate 自检与既有矩阵测试在 -race 下仍绿）。
  夹具两处修复记录见文末「第四轮修正记录」。
- **权威运行**：`go test -race -tags integration_redis -run 'TestRedisBudget'
  -count=1 -v ./internal/ratelimit`（`race_v_run.log`：两测试全 PASS、无
  DATA RACE）。旁证第二轮 `-v`（非 race）：`cte_boundary_run_second.log` /
  `response_loss_run_second.log`（同夹具、独立容器轮，用于跨轮一致性）。
- 候选参数（验证输入，**非生产默认、非 SLO**）：L=200ms、ε=80ms（仅调度
  抖动口径；v9.22.0 socket deadline 取自 `time.Now()`，conn.go:29-40，
  **无「≤50ms 缓存时钟」项**——历史注释中的 50ms 缓存已被移除，方向即使
  存在也只会让 deadline 更早）、取消时刻 60ms、close-after 30ms（< L 构造
  上即「预算前关闭」）、限流桶 rate=1/s burst=10（使扣减可读）。
  生产缺省 `TXHARBOR_REDIS_TIMEOUT=1s` 不动；本实验不引入任何配置键。

## 1. CTE 与总预算边界（`cte_boundary_records.json`，9 条记录，权威 -race 轮）

| 场景 | 调用者返回 | 错误类（JSON error_class） | 断言结论 |
|---|---|---|---|
| hot_poolwait primary（热连接读阻塞） | 200.6ms | context_deadline_exceeded | ≤ L+ε ✅；PoolStats.TotalConns=0（坏连接已 Remove）✅ |
| hot_poolwait secondary（池等待→hold） | 202.2ms | context_deadline_exceeded | ≤ L+ε ✅（池等待被 ctx 截断） |
| cancel_inflight（60ms 处主动取消，读在途） | **203.0ms**（非即时） | other（error_text 含 `context canceled`） | **非即时**：≥ cancel+100ms 且 ≤ L+ε ✅——socket deadline 在读开始时即算定，取消只在后续 ctx 检查（retry Sleep）时上报 |
| cancel_poolwait（池等待中取消） | 61.5ms | other（error_text 含 `context canceled`） | ≤ cancel+ε ✅（select 在 ctx.Done 即时返回）+ 错误文本断言 ✅ |
| cold_init（冷连接 HELLO 被扣） | 200.9ms | context_deadline_exceeded | ≤ L+ε ✅；init 失败连接已 Remove ✅ |
| parent_deadline（父预算 80ms < L） | 81.6ms | context_deadline_exceeded | ≤ parent+ε 且 < L ✅（min(parent,L) 生效） |
| cte_off_hot_control（CTE=false 对照） | **1001.3ms** | context_deadline_exceeded | ≥800ms ✅——**反向对照，不计入 L+ε 统计**：不开启时 socket 读跑满 ReadTimeout(1s)，证明 L 界以 CTE 为必要条件 |
| normal | 6.7ms | ok | < L ✅ + 背景增量 0 ✅（断言，非仅记录） |
| recovery（③→④ 翻转后首条可信 Allow） | 5.6ms | — | ≤30s 显式断言 ✅ + `Recovering()` 梯度窗仍开 ✅（恢复契约未变）；背景窗口记录不断言（恢复循环内合法重连） |

- **背景活动边界**：9 场景**全部采样**（修复前 normal/recovery 未采样），
  返回后 2s 窗口内 gate accepts 增量均为 0。这仅是**「返回后 2s 内未观察
  到新连接活动」的观测**，不是「后台协程已全部退出」的证明——后者在本
  夹具不可观测（dial 后台协程最长 ≈5.4s、tryDial 探测均不可见）。
- **连接清理**：热/冷故障后 `PoolStats.TotalConns=0`（bad conn 经
  `p.Remove`，release 在 Allow 返回前完成）。
- **撤回的绝对上界**（已写入设计文档）：①「主动取消即时中断在途 I/O」
  不成立（203.0ms vs 取消于 60ms）；②「调用者返回 ⇒ 后台已全部退出」
  不可证；③ CTE=false 时 `≤L` 不成立（1001.3ms 实测）；④ cancel-only
  （无 deadline）ctx 分支本轮未覆盖（limiter 路径恒带 L deadline）。
- **ε 口径**：L 类场景权威轮最大超出 3.0ms（203.0−200）；跨轮观测最大
  8.4ms（`cte_boundary_run_second.log` hot=208.4ms）——远小于 80ms 测试
  容差；**ε 是统计口径，不是硬常数**。
- **排序依赖说明**：PoolSize=1 的 primary/secondary 时序用 30ms 起跑差
  制造（非握手同步）；若次序颠倒，poolwait 断言会显式失败而非静默通过。

## 2. 已执行但响应丢失（`response_loss_records.json`，3 组，权威 -race 轮）

三计数口径（分列记录，绝不互相推定）：应用调用数=1（唯一 Allow）；
服务端发送数=gate 侧 EVAL 标记帧计数；**已确认服务端执行数=Redis
`INFO commandstats` cmdstat_eval:calls 增量**（权威），辅以 bucket
`tokens` 字段差值（rate=1/s 使扣减可读）。`responses_withheld`（应答
观测）为**次级口径**，见下方已知缺口。

| 组 | sends | confirmed execs | withheld | tokens 9→ | 扣减 | 返回 |
|---|---|---|---|---|---|---|
| close_before_expiry（默认重试，30ms 关连接） | **3** | **3** | 3 | 6.008 | **≈3.0** | 200.8ms（容差内） |
| hold_to_expiry（扣留至预算到期，对照，drop_close=-1ns） | 1 | 1 | 1 | 8.007 | ≈1.0 | 201.6ms |
| isolated_noretry（候选策略：MaxRetries=-1） | 1 | 1 | 1 | 8.008 | ≈1.0 | **35.5ms** |

**结论（反例证实，断言不放宽）**：
1. **CTE 不阻止预算内重发**：连接在预算到期前被关 → EOF 判可重试 →
   ctx 未到期 → 默认重试最多 4 次尝试（MaxRetries=3）⇒ 单次 Allow
   **3–4 次发送、3–4 次服务端执行、3–4 次 token 扣减**（权威 -race 轮
   3 次；第二轮旁证日志 4 次——重发次数随退避时序在 3–4 间波动，
   **sends==execs==扣减 跨全部轮次自洽**，「≥2 次重发+双扣」跨轮一致）。
   设计文档「CTE 后结构上不会自动重发」的不变量**已被证伪**（仅
   「ctx 到期 ⇒ 不重发」结构性成立：hold 组实测 1/1）。
2. **候选策略有效（测试侧证明）**：隔离客户端 `MaxRetries=-1` 下 1/1/1，
   且失败返回从 ~201ms 降到 35.5ms。**不全局关闭共享客户端重试**、
   **不新增脚本幂等协议**（均未触碰）。
3. **TTL 耦合实测**：候选 L=200ms 时 bucket TTL=2×L=400ms——hold 组
   settle 读取时 key 已过期（`tokens_settled=""`），空闲 400ms 即全额
   重置桶；isolated 组 35.5ms 返回则 key 尚存。印证设计文档 §5 的 TTL
   独立常量解耦要求。
4. 超时/未收到响应**从未**被用来推定「服务端未执行」：执行数只来自
   INFO 与 tokens。
5. **已知缺口（如实列出，未解释则不解释）**：`responses_withheld`
   （gate 应答观测）在 6 轮中出现 1 例漏计（留存的第二轮旁证日志即
   4/4/3 样本，`response_loss_run_second.log`），机制未定位——该计数
   不参与跨组一致性断言（仅 hold 组有 `>=1` 的单组下界断言）；三个
   权威口径（sends/execs/tokens）跨轮全部自洽。

## 3. 与设计文档的关系

本轮结果与本轮修正回写 `docs/evidence/013/redis-latency-budget-design.md`
（其「修订记录（第二轮定向验证）」+「第四轮修正记录」节）：边界条件收紧、
响应丢失不变量修正、共享接缝条件化结论、TTL 解耦实证、默认值/兼容方案的
确定建议、独立复核缺陷修复。

## 4. 边界

- 单机、testcontainers Redis、L=200ms 候选值单点（未做 L 多点扫描）；
- 取消场景恒经 limiter（必然带 L deadline），**未覆盖**「cancel-only 无
  deadline ctx + CTE」分支——该分支按源码预期 socket 段仍受 ReadTimeout
  约束（conn.go:1215-1233），列为待补验证；
- GateDrop 的 EVAL 标记识别为 RESP 字节串匹配（含跨读窗口去重，见第四轮
  修正），不解析 RESP 结构；`error_class` 分类器不含独立的 canceled 桶
  （canceled 落 `other`，以 error_text 为准——不把分类器未建类写成已建）；
- 未跑全量故障矩阵/全量 013 流程（本轮范围外）。

## 第四轮修正记录（2026-10-06，独立复核后修复）

独立复核（未参与修改者）发现并已修复：
1. **EVAL 标记跨读重复计数窗口**（gate）：128B tail 窗会把上一片已计数
   的完整标记再计一次 → 改为「只计延伸进新字节的出现」（跨读切片仍
   覆盖）；复跑 sends==execs 一致。
2. **Drop 连接对不自回收**（gate）：定时关客户端后对端 goroutine 永久
   阻塞 → 两侧 defer 互关对端；测试间/关闸时无残留 pair。
3. **断言补齐**：normal `<L`+背景增量 0、recovery `≤30s`、cancel_poolwait
   错误文本三处从「仅记录」升级为断言。
4. **后台采样补齐**：normal/recovery/cte_off 此前未采样却记 0 → 9 场景
   全部采样（见 §1）。
5. **字段截断**：hold 组 `drop_close_after_ms` 由 `Microseconds()/1000`
   把 -1ns 截成 0 → 改 ns→ms 比例换算（现记 -1e-06）。
6. **表述修正**：staleness 50ms 项从头注释/ε 注释删除（v9.22.0 无缓存
   时钟）；`200.8ms` 由「预算内」改「容差内」；「走满 MaxRetries=3」改
   「最多 4 次尝试（归档轮 3 次被预算截断）」；error_class 列与 JSON
   对齐（canceled 落 other）；跨轮 4 次重发的旁证日志保留为
   `response_loss_run_second.log`（第二轮 4/4/3）。

## 留存物与哈希

`SHA256SUMS`（归档后生成，覆盖本目录全部证据文件）；运行日志为 `-v`
输出原样（脱敏：仅含耗时/错误串/计数，无凭据）。权威数值以
`cte_boundary_records.json` / `response_loss_records.json` 为准，
两个 `*_second.log` 为跨轮旁证。
