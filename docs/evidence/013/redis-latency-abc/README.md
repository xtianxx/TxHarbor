# 013 Redis 故障延迟 A/B/C 隔离对照实验（2026-10-06）

- 目的：验证既有分析表中候选优化 R1「仅缩短 go-redis DialTimeout」对 Redis 故障期间
  `limiter.Allow` 停止-到-观测时延是否有可测收益。**这是有界隔离实验，不是生产
  优化、不是 SLO 校准、不是全量基准复测。**
- 绑定：main `be272cb`（实验时 HEAD，工作区干净）；go1.26.5 linux/amd64；
  WSL2 内核 6.18.33.2；docker 29.6.1；Redis 镜像 `redis:8.2.10-alpine`
  （与 `internal/testutil.RedisImage` 一致）；go-redis v9.22.0（默认重试/退避未覆盖）。
- 历史 1005ms 观测（`benchmark_report.md` §4，绑定 commit 89ef787）保持不变引用：
  本实验不重测全栈 serve 路径，其等价性声明为「未建立」。

## 变体（仅改客户端 DialTimeout；未新增生产配置键，未改默认值）

| 变体 | DialTimeout | 其余 Options（与生产 serve 侧一致部分） |
|---|---|---|
| A | 1s（= cfg.Redis.Timeout，serve.go:195-197 的现值） | Read/Write=1s；MaxRetries/DialerRetries/pool/退避 = go-redis 默认 |
| B | 200ms | 同上 |
| C | 300ms | 同上 |

实验主路径 = `internal/ratelimit`（真实 013 limiter + token-bucket script +
`Limiter.Allow` 的 `WithTimeout(ctx,1s)` 调用方预算）；观测调用
`limiter.Allow(ctx, ClassQuery)` 本体（不经过 policy —— PD-1 的 policy 对查询类
失败返回 nil「放行继续」，那不代表决策成功；省略 policy 不改变 EVAL 预算结构）。

## 故障形态（真实触发，不互相冒充）

| 形态 | 注入 | 触点 |
|---|---|---|
| ① refused | userspace `RedisGate` `GateDown`（关监听）→ ECONNREFUSED | 拨号即拒 |
| ② dial_blackhole | 测试侧自定义 `Dialer` 阻塞到 min(DialTimeout, attempt 剩余) 后返回 dial 形 timeout —— 拨号层阻塞由 Dialer 语义驱动（userspace 监听器无法造 SYN 黑洞；说明见 `internal/testutil/redisgate.go` 头注）。**语义边界：Dialer 不开 socket、不发 SYN——验证的是 go-redis 拨号等待+双层重试的合成语义，不是内核 connect/SYN 黑洞实测**；期间不会进入 HELLO/EVAL 写段 | 每次拨号尝试挂满 |
| ③(b) conn_hold_cold | `RedisGate` `GateHold`：TCP 建连成功（对端 accept 并吞入请求字节），但 HELLO/首命令无响应 —— **语义边界：是「已建连+无响应」，不是「Redis HELLO 握手完成」**（评审口径原文采纳） | 已建连+读无响应 |
| ③(a) conn_hold_warm | Pass 态下每 caller 预热一条池连接（poolSize==callers，评审 P1 修复），再翻 Hold —— **转换覆盖已建连**（`connPair.toHold`：关 backend 侧，网关侧转读吞）→ 每 caller 的 EVAL 都 ride 一条已初始化但静默的 socket | 已建连+命令无响应 |
| ④ normal | `GatePass`，无故障 | 对照组 |

独立验证：`internal/testutil/redisgate_test.go`
`TestRedisGatePassForwardsAndHoldConverses` 明确断言「Hold 翻转后 PASS 已建连的
PING 不再秒答，也不许被立即 reset」（350ms 下界护栏）——即 warm 转换真实成立。

## 参数（实验输入，**不是生产 SLO**；未裁决）

- 固定并发 8 callers / 组；窗口 1.2s / 组；轮次 = 每变体×形态 2 轮（round1 =
  首次故障，round2 = 持续故障语义由形态前置序列承载：warm 形态每轮先预热再挂）。
- 观测 = caller 侧 stop→return（含 limiter 内部 WithTimeout(1s)）；错误分类来自
  观测错误串（`classifyRedisError`：refused / context_deadline_exceeded /
  blackhole_dialer / dial_io_timeout / read_io_timeout / pool_timeout / other）。
- 不可观测项 = 未知：go-redis 内部 attempt 数、socket 级发送/拨号次数**未记录**
  （API 不暴露；本夹具不推算）。

## 结果（p50 / p95 / p99，单位 ms；轮次为组内第 1/2 轮；每变体×形态×轮 n=16 故障 / ~6.1k–6.8k for normal）

最终归档 = 修复 warm 池上限（reviewer P1：poolSize==callers，每 caller 一条
预热连接）并在 -race 下复跑的结果（证据哈希见下）。

下表第三列为 p99（线性插值，口径与 perf harness 一致；max 见 stats.json）。
normal 行为两轮均值。所有分位已从 samples.json 重算校验一致
（校验记录见文末「口径修正记录」第 5 条）。

| 组（组内轮次） | A | B (200ms) | C (300ms) |
|---|---|---|---|
| refused r1 | 1000.5 / 1000.8 / 1000.8 | 1000.8 / 1001.4 / 1001.4 | 1001.1 / 1001.3 / 1001.4 |
| refused r2 | 1000.5 / 1000.8 / 1000.8 | 1000.7 / 1000.9 / 1000.9 | 1000.7 / 1000.9 / 1000.9 |
| dial_blackhole r1 | 1001.7 / 1002.7 / 1002.7 | 1000.9 / 1001.5 / 1001.5 | 1000.8 / 1001.3 / 1001.4 |
| dial_blackhole r2 | 1000.8 / 1001.0 / 1001.0 | 1001.0 / 1001.3 / 1001.4 | 1000.9 / 1001.1 / 1001.2 |
| conn_hold_cold r1 | 1002.1 / 1002.6 / 1003.0 | **1013.8 / 1017.9 / 1018.0** | 1002.7 / 1003.9 / 1004.0 |
| conn_hold_cold r2 | 1002.5 / 1002.8 / 1002.9 | 1001.9 / 1002.6 / 1003.0 | 1006.6 / 1009.3 / 1009.3 |
| conn_hold_warm r1 | 1002.3 / 1004.3 / 1004.4 | 1001.2 / 1001.9 / 1001.9 | 1001.1 / 1001.9 / 1001.9 |
| conn_hold_warm r2 | 1001.5 / 1002.3 / 1002.7 | 1001.4 / 1002.0 / 1002.0 | 1003.9 / 1006.6 / 1006.7 |
| normal（两轮均值） | p50=1.4 p95=1.8 | p50=1.44 p95=1.94 | p50=1.45 p95=1.95 |

- 故障组错误率 100%（全部 `context_deadline_exceeded`，`ratelimit: unavailable:
  ratelimit eval: context deadline exceeded`）；正常组错误率 0%。
- 单组内偶发尾部（cold r11 p95=1017.9，比 A cold 首轮高约 1.5%（p95 口径）；
  cold r20 1009.3、warm r22 1006.6）为 <2% 量级抖动，不构成 A/B/C 的变异
  模式。跨变体均值差异的**可复现口径**（每变体 2 轮均值→相邻变体比较）：
  refused 0.02%/0.04%；blackhole 0.03%/0.04%；cold 0.55%/0.23%（cold 含
  r11 离群，剔除后 0.04%）；warm 0.06%/0.06%。
- normal 组：A/B/C 的 p50/p95 在 ~1.4–2.0ms 内同量级（绝对差 <1ms，属本机
  噪声量级）——不能声称「数值完全一致」，只声称「无定向收益/无回归」。
- 恢复组（C 变体，③→④ 翻转，-race 复跑）：首个可信 Allow 成功耗时
  **4.9ms**，`Recovering()` 梯度重开窗口仍然开启（契约未变）。**证据来源
  说明：recovery 无独立分位组**——该数值来自测试日志（`-v` 输出，未入库，
  见「复现入口」），stats.json 中无对应条目，不能作为分位统计引用。

## 结论（按形态，全部 [实测]）

1. **① refused：A/B/C 无差异**（全部 ≈1000ms）。拨号立即失败，但故障期间
   每次 `Allow` 仍等待满 1s caller 预算——本实验中观测到的等待由「预算+
   重试+I/O 组合」决定（caller 预算为主导项），不是由 DialTimeout 支配。
2. **② dial_blackhole：A/B/C 无差异**（全部 ≈1000–1003ms）。**语义边界**：
   本形态由测试侧自定义 Dialer 驱动，验证的是「拨号阶段阻塞 + 拨号层重试
   （DialerRetries=5、每尝试 min(DialTimeout, attempt-ctx)、100ms 退避）+
   命令层重试」在 go-redis 内的真实交互；Dialer 本身不打开 socket、不发 SYN，
   因此不是内核 connect/SYN 黑洞的实测（该层真实性需要 root/内核级注入，
   属后续实验）。在此合成形态下拨号预算减小未产生可测差异。
3. **③ cold/warm hold：A/B/C 无差异**（全部 ≈1001–1018ms，含 δ 越界）。读
   超时由 ReadTimeout=1s 支配（与 DialTimeout 无关）；warm 组在 poolSize
   修复后每 caller 一条已初始化连接。
4. **④ normal：无定向收益/无回归**（p50/p95 均在 ~1.4–2.0ms 同量级）。
   **DialTimeout 只约束尚未建立的连接**（每次拨号 attempt 预算 =
   min(DialTimeout, attempt-ctx 剩余)；连接建立成功后即复用，其后的命令
   读写由 Read/WriteTimeout 支配、与 DialTimeout 无关）。
5. **恢复：契约不变**（梯度重开窗口仍在），恢复首条调用 ≈5ms。

**R1 判定：撤回**。四种形态下「仅缩短 DialTimeout」均未产生低于 1s caller
预算量级的改善。「每一次 Allow 的总耗时 = caller 预算」的准确表述是：
- 等待点（拿连接、池排队、重试退避）都被 limiter 的
  `WithTimeout(ctx, l.timeout)`（`limiter.go:222`，=1s）截断；
- 但**最后一次成功连上的 socket 读写** deadline 不随 caller ctx（
  `ContextTimeoutEnabled=false` ⇒ socket deadline=now+Read/WriteTimeout），
  读可在调用方 deadline 之后继续越界达 ≤1s（实测 δ≈1–5ms，cold 形态最大
  ≈18ms——1005ms 历史观测的构成与该机制一致）；
- dial 每尝试预算由 Background 派生的 attempt ctx 限定
  （min(DialTimeout, attempt-ctx)，不继承 caller 剩余）。

**证据支持的后续候选（仍未实施、未批准）**：
收紧 `limiter.Allow` 的调用方预算（`internal/ratelimit/limiter.go:222` 的
`l.timeout`），或在不可用状态下对 EVAL 做「快速短路 + 主动重连探测」重启
判定。两者都要改生产行为语义/契约位置（前者改动每决策等待上限，后者改动
`Unavailable()` 的探测时点），均属 PD-1 边界外（非资金判定）但需要单独评审
与验收（承接 V-RATELIMIT/V-DRILL）。此为候选，不等同已批准 SLO。

**边界声明（保留）**：n=16/组的小样本（分位数对尾部敏感）；② 为合成 Dialer
（无 SYN，不实测内核 connect 路径）；无「Redis 慢但活着（往返落 (L,1s)）」
的中间态样本；全部形态未证明统计等价（小样本下只支持「未见差异」）。
**CI 状态**：main 必需 CI / fault-perf / Recovery Drill 三态分列见文末
「第二轮修正记录」第 2 条。

## 留存物（最终哈希；-race 复跑后更新）

- `docs/evidence/013/redis-latency-abc/samples.json`（gate 形态全样本）sha256
  `0ab680e7021683b73e798ef22f9cb68a53201e9a7d36c80593929e2bc6107e8f`
- `docs/evidence/013/redis-latency-abc/stats.json` sha256
  `7422cda934eff793d36ef218f879772d8403cf55ea5ec51efd22b21a751db808`
- `docs/evidence/013/redis-latency-abc/dial_blackhole_samples.json` sha256
  `fa80b9f694277707ace66d9fb4d47aad593350c2271e61b8be9cd2a0836d517f`
- `docs/evidence/013/redis-latency-abc/dial_blackhole_stats.json` sha256
  `1e46bbc48620a837ecdaaa8476d614a7f46a03e62c288996dfcca5b10586a72a`
- 复现入口：`go test -tags integration_redis -count=1 -run 'TestRedisLatency'
  -timeout 15m ./internal/ratelimit`（首次跑需 docker daemon；单测试重复运行
  会轮换证据目录 `/tmp/txharbor-test-evidence-*`，原目录哈希见表）
- 运行日志存副本 `/tmp/redislat/run_full2.log`（未入库；测试输出即证据的
  summary 面向读者部分，如需完整 stdout 应以再跑一次为准）
- 边界声明：本实验无生产部署、无缓存启用、无熔断、无 PD-1 调整；所有阈值是
  实验环境的输入，不是生产容量/延迟结论。

## 第二轮修正记录（原诊断报告的三处表述修正）

1. **超时预算表述修正**（原报告「总等待预算 1s 封顶」过于粗糙）：
   - caller 预算 1s 来自 `Limiter.Allow` 内 `WithTimeout(ctx, l.timeout)`
     （`internal/ratelimit/limiter.go:222`），只约束「拿连接/排队/重试退避」；
   - socket 读写 deadline 不随 caller ctx：`ContextTimeoutEnabled=false`（默认）时
     `c.context(ctx)` 返回 Background（go-redis `redis.go:1468-1472`），
     `cn.deadline()` = `now + timeout`（`conn.go:1207-1235`）⇒ 读可在调用方
     deadline 之后继续，越界上限 = R（=1s），实际越量 = readStart − Allow入口
     δ（warm 实测 ≈1–2ms；③(b) cold 实测 ≈1–2ms 量级，上限结构 [1s+δ, 2s]）；
   - dial 层：每次尝试 `min(DialTimeout, attempt-ctx 剩余)`，attempt ctx 由
     **Background 派生**（`pool.go:1059`）⇒ 拨号预算不继承 caller 剩余；
     caller 靠 `queuedNewConn` 的 `select`（`pool.go:1112-1116`）在 L 处返回。
   - 三层计数修正：应用层 Eval 调用恒 1；go-redis command 尝试 ≤4 轮
     （MaxRetries=3 默认；dial 错误重试（error.go:97-100 dial 判定先于 ctx）、
     读超时按 retryTimeout 标志重试、ctx 错误不重试）；socket 实际发送 =
     到达写段的尝试数（冷连接先发 1 次 HELLO，`redis.go:793`，在 EVAL 之前）。
   - 「响应丢失 ≠ 未执行」对 token-bucket 是实质风险：script 无幂等键
     （HMSET 覆写 + PEXPIRE），已执行但响应丢失时 token 已扣；正确性上无害
     （扣减只是暂时性丢弃一次决策机会，非资金动作），但实验与运维日志不得
     把「EVAL 返回错误」解读为「服务端未执行」。
2. **人工验收/CI 状态表述修正**（原报告「当前 main CI 未闭合，文档未见记录」
   已过时，本轮只读核验起到的更正）：
   - main `be272cb` 的 push CI **run `37298301943` conclusion=success**
     （2026-10-05T10:42:40Z，12m2s；job：lint/build/unit+race/contract/e2e/
     integration-PG 全绿，integration-Redis/Kafka 按路径分层为 skip，未运行
     不构成失败）——当前 main CI 在 013/限流触发面所属层的普通 PR 通道已绿；
   - fault-perf 定时层 run `37297533285`（main `2fca0b0`）success：
     fault(V-DRILL) 14m13s + perf(V-BENCH) 4m1s 两 job 绿（该轮 headSha=
     `2fca0b0`，先于 be272cb 的测试夹具修复，基准 harness 代码路径无差异）；
   - Recovery Drill 定时层 run `37298994567`（be272cb）FAIL，失败原因为
     runner 环境性（`mkdir /host-gomodcache/...: read-only file system`，
     装配步骤未过、测试未开跑），与代码无关；记录保留为待修项；
   - 历史 main push 失败 run `37257158182`（883f1c0）/`37274084298`（2fca0b0）
     均已被后续 PR/夹具修复路线闭合（见 `docs/evidence/015-fix-session-census/`
     与 `docs/evidence/015-followup-deposit-unknown/`），旧失败记录保留不改写。
   - 「人工验收」证据面维持原结论：013 B1–B11/Q0–Q11 本地闭合、阈值一律待测/
     待裁决；本实验不重排验收，仅新增本对照实验证据面。
3. **015 准入影响说明的精确化**（原报告表述过强）：
   - `TXHARBOR_RECOVERY_CONTROL_DSN` 未配置（默认部署）时
     `assembleServeRecovery` 返回 nil（`serve.go:853-860`），
     `withdrawalRecoveryGate(nil, next)` = 纯 passthrough
     （`withdrawalhttp.go:315-317`）——每请求 **0 次额外 I/O**，
     「015 门禁改变了入口时序」只适用于**配置了控制 DSN** 的部署：每动作
     请求新增 2 次控制库查询（`openInstance` + `targetGuard`，`gate.go:421/:428`），
     bound 态另增事务/锁/审计写（≥4 次 +1 INSERT）。
   - 本实验不运行 serve 进程，上述差异不进入本轮测量结论。
4. **复核纪律补充（评审口径采纳）**：
   - `serve.go` 自 013 起就创建 cacheClient 并赋给 `degradation.cache` ——
     生产仓库从未有过「无缓存接线」状态；本实验的边界声明应读作「本轮
     **未新增/未改动**缓存接线」，不是「仓库从未接线」。
   - 复核者实测重算核查：p50/p95/p99 线性插值口径与 perf harness 完全一致
     （rank=(n-1)*p/100、floor/ceil 插值、样本标准差同式）。
   - 归档的错误分类与真实错误串兼容（`context_deadline_exceeded` 分类在
     `ratelimit` 包装前缀下命中）；dial i/o timeout 的 HasPrefix 判断已在
     夹具中改为 Contains（limiter 前缀存在时同样可分类）——该修复在
     最终归档复跑之后才落码，因此**最终归档不含该分类路径的实测样本**
     （dial_blackhole 归档样本全部落在 `context_deadline_exceeded` 分类，
     与测试日志一致；dial_io_timeout 分类保持为「形态② statement 的备用
     桶」，本轮无样本）。

## 第三轮口径修正记录（2026-10-06，本轮：只改文档不改数据）

5. **表格重算校验（C 热连接等）**：
   - 从 `samples.json` 以同一插值公式重算 `C/conn_hold_warm/r21`（n=16：
     p50=1001.141 / p95=1001.930 / p99=1001.942）、`r22`（p50=1003.946 /
     p95=1006.621 / p99=1006.651）、`B/conn_hold_cold/r11`（p50=1013.786 /
     p95=1017.851 / p99=1017.970）——与 `stats.json` 逐位一致（ALL-MATCH）。
   - **「C 热连接 p50 大于 p95」未在归档数据中出现**：stats.json 全 24 组 +
     blackhole 6 组均满足 p50<p95<p99≤max。该提法源自第一轮会话正文的草案表
     （未落盘）；归档 README 的 C warm 行始终与 stats.json 一致。此项列为
     **已澄清缺口**：不存在需要校正的归档表格行。
   - 表内既有的真实口径问题已修（见上文结果节）：表头原标「p50/p95/p99/max」
     但每格 3 值、第三列 4 处取 max、2 处取 p99 → 统一为 p99 真值并改表头；
     表内 r1/r2 明确为组内轮次；normal 行明确为两轮均值；「约 1.3%」改为
     「约 1.5%（p95 口径）」；评审均值差异改为可复现口径（每变体 2 轮均值
     →相邻比较；cold 含 r11 离群 0.55%、剔除后 0.04%）。
   - 原始 samples/stats JSON 未改写（哈希不变）。
6. **「DialTimeout 只作用成功建连」类表述**：仓库内不存在该字面句；结论 4 已
   改写为「DialTimeout 只约束尚未建立的连接（每次拨号 attempt 预算 =
   min(DialTimeout, attempt-ctx 剩余)）；连接建立后命令读写由 Read/Write
   Timeout 支配」。
7. **「等待由 caller 预算直接决定」限定**：结论 1 已改为「本实验中观测到的
   等待由『预算+重试+I/O 组合』决定（caller 预算为主导项）」；该限定仅适用
   于本实验的预算/重试/I-O 组合与形态覆盖，不外推为普适上界证明。
8. **小样本/合成 Dialer/统计等价边界**：见结论段「边界声明」，保留不改。
9. **CI 三态分列**：已在第 2 条分列（main 必需 CI `37298301943` passed；
   旧提交 `2fca0b0` fault-perf `37297533285` passed；`be272cb` Recovery Drill
   `37298994567` 独立失败=runner 环境性）。本轮不顺带修该 runner，保留待修。
