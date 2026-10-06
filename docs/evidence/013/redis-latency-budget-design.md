# 013 补充设计：Redis 限流决策总等待预算（2026-10-06）

- 性质：**技术补充设计（可审阅），非实施、非生产参数、非 SLO**。基线 =
  main `a59dcd1`（实验与独立复核成果），分支
  `013-redis-latency-budget-design`。复用 `docs/evidence/013/redis-latency-abc/`
  的实验与复核结论，不重跑 A/B/C、不重跑 013 全量流程。
- 推荐方向：**缩短并真正落实「限流决策」的总等待预算**。**不采用**基于
  不可用状态的熔断、不采用探测驱动短路（用户本轮裁决；以下机制评估据此
  排除对应选项）。
- 委派合流来源：BudgetAudit/MechanismAudit（预算执行机制逐层核对，
  `history://MechanismAudit`）、ContractAudit（共享客户端影响与契约核对，
  `history://ContractAudit`）。所有 file:line 由两代理独立读取源码产出。
- **第二轮（定向验证）合流**：CTEBoundary / ResponseLossDesign /
  SharedImpactTTL（并发只读核对）+ 测试侧实测
  `docs/evidence/013/redis-budget-cte/`（CTE 边界 9 场景 + 响应丢失
  3 组反例，-race 归档）。本轮修订：修正 4 处源码级表述、收敛 8 条
  撤回项、响应丢失不变量修正、共享接缝结论条件化、推荐方案更新为
  **限流专用隔离客户端**（理由见 §2/§3）。

---

## 1. 总预算的起止点与逐段约束（闭合 1）

**起点**：`limiter.Allow(ctx, class)` 入口，其中 ctx =
HTTP handler 的 `r.Context()`（`ratelimit_middleware.go:100`）∩
`context.WithTimeout(ctx, L)`（`limiter.go:222`，L 现值 =
`TXHARBOR_REDIS_TIMEOUT` = 1s，`ratelimit_middleware.go:68`）。
有效截止 `D = min(parent_deadline, now+L)`。

**逐段清单**（「受截断」= 该段被 D 截断；源码行号来自 go-redis v9.22.0
GOMODCACHE 与本仓）：

| # | 段 | 预算来源 | 受 D 截断？ |
|---|---|---|---|
| 1 | 池信号量等待 waitTurn→FastSemaphore.Acquire | min(D, PoolTimeout=2s)（options.go:534-540；semaphore.go:78-105） | ✅（semaphore.go:101-102） |
| 2 | dials 许可 select | D（pool.go:1050-1056） | ✅ |
| 3 | 拨号等待（caller 侧 select） | D（pool.go:1112-1119） | ✅（到期返 ctx.Err；**撤回原「后台拨号由 w.cancel 中止」表述**——`w.cancel()` 只 close(result) 不调 cancelCtx（want_conn.go:40-58），后台拨号继续最长 ≈5×DialTimeout+4×100ms≈5.4s，期间占住 turn+dials 许可（pool.go:1092-1107）） |
| 4 | dialConn 每尝试 | min(DialTimeout, attempt-ctx)；attempt ctx 由 **Background 派生**（pool.go:1059,716-725）——不继承 caller 剩余 | ❌（仅被 DT 封顶；5 次尝试+100ms 退避 ≈5.4s 后台预算） |
| 5 | dialErrorsNum 快路 | 0 耗时（pool.go:692-694） | — |
| 6 | 连接初始化等待（另一 goroutine 在 init） | min(D, DialTimeout)（redis.go:668-710） | ✅ |
| 7 | HELLO 握手 socket 写/读 | now±W/R（`c.context(ctx)`=Background，redis.go:1468-1473 + :793 + conn.go:1207-1235） | ❌ |
| 8 | 命令层重试退避 internal.Sleep | D（redis.go:1324-1328 → util.go:37-46） | ✅ |
| 9 | push 预处理 peek | min(D, 1ms)（redis.go:2546-2552） | ✅（≤1ms） |
| 10 | EVAL 命令 socket 写 | now+W（redis.go:1348；Background） | ❌ |
| 11 | EVAL 命令 socket 读 | now+R（redis.go:1375；EVAL 无 per-cmd timeout，redis.go:1454-1466） | ❌ |

**总耗时的严格上界**（MechanismAudit 结论；CTEBoundary 修正后口径）：

```
CTE 关闭（现状）：T_allow ≈ D + n × (W + R) + ε
  n = 1（热连接：仅 EVAL 一段）
  n = ≥2（冷连接：HELLO 段 + identity 往返段（ClientSetInfo×2，redis.go:994-1008，
     设计文档原 n=2 漏计）+ EVAL 段；init pipeline 在本 wiring 下 0 条命令 ⇒ 0 I/O，
     redis.go:836-861）
  ⇒ 原文「冷 = L+4s 是结构上界」撤回为「按实际可达阶段计数、阶段链在首个
     超时阶段终止」；现值 W=R=1s 下热 ≈L+2s、冷 ≥L+4s。
CTE 开启（推荐）：T_allow ≤ min(parent, L) + ε（socket 段逐段入口取
  min(now+t, D)，阶段链不可累加；实测 9 场景见 §7 验证）
```

**因此：仅缩短 `limiter.WithTimeout`（L）不构成总耗时有界**。要真正有界，
必须让「最后一次开始的 socket 段」也受 D 约束 ⇒ 见 §2 推荐机制。

**父请求更早截止 / 取消 / 清理**（闭合 1 的另一半；实测见
`docs/evidence/013/redis-budget-cte/`）：
- 立即返回点：池信号量（semaphore.go:101-102）、dials select（pool.go:1053-1055）、
  拨号等待 select（pool.go:1113-1115）、重试退避 Sleep（util.go:42-44）、
  等初始化（conn_state.go:268）。ctx 错误经 shouldRetry=false 直接终止
  （error.go:103-105；redis.go:1242-1252）。**实测**：池等待中 60ms 处
  取消 → 61.5ms 返回（即时）。
- **不会**中断点：在途 socket 写/读（#10/#11）。CTE 开启后在途读的
  deadline = min(readStart+R, D)，**取消本身不改已装载的 deadline**
  （conn.go:1045/1084 阶段入口一次性设定，无 ctx 监听者）：
  ① 回复先到 ⇒ **成功返回**（redis.go:1446-1448 后无 ctx 复查）——即使
  ctx 已取消；② 无回复 ⇒ 读跑到 socket deadline（CTE 下=min(entry+R,D)）
  以 i/o timeout 结束 ⇒ retryTimeout=1 ⇒ 重试 ⇒ 下一轮 Sleep 立即返
  ctx.Err。**实测**：60ms 处取消（读在途）→ **203.0ms 返回**
  （=Allow 入口 + L，非取消时刻）——「主动取消即时中断在途 I/O」**撤回**。
- **cancel-only（无 Deadline）ctx 分支：本轮未覆盖且上界撤回**——
  `conn.deadline()` 只读 `ctx.Deadline()`（conn.go:1215-1229），无
  Deadline 的 ctx 落到 `now+t`（conn.go:1231-1233）⇒ 无 D 时取消后在途
  读仍等满 R，CTE 开/关在该分支**行为相同**；limiter 路径恒有 D
  （limiter.go:222）故 `≤D+ε` 不受影响，但起算点是 Allow 入口而非取消
  时刻——凡「取消后 ≤ε 返回」的表述一律撤回。
- 连接清理：读写超时与 ctx 错误均命中 `isBadConn(err,false,addr)=true`
  （error.go:193-196、:217-222）⇒ `p.Remove`（redis.go:1049-1051）而非归池；
  仅成功回复归池（redis.go:1070）；初始化失败 Remove（redis.go:536-538，
  **CTE 截断 HELLO ⇒ Remove 必然发生**：HELLO i/o timeout 非 RedisError
  ⇒ 直接 Closed+return（redis.go:796-805），不走 AUTH 兜底）；
  caller 在拨号等待中返回且 cn 已生成 ⇒ 归 idle 并还 turn
  （pool.go:1069-1071）——无泄漏。**实测**：热/冷故障后
  `PoolStats.TotalConns=0`。
- **「调用者返回 ⇒ 后台全部退出」撤回**：后台拨号最长 ≈5.4s（Background
  派生，见表 row3/4）；`tryDial` 每 1s 探测直至成功/关池（pool.go:806-833）。
  本轮可观测边界 = 返回后 2s 窗口 gate accepts 增量（9 场景全部 0，
  `background_add=0`）——这是观测，不是协程退出证明。
- **设计必须显式声明的取消语义副作用**（MechanismAudit 发现）：limiter.go:231
  用 `%w: %v` 包装，底层 `context.Canceled` 无法 `errors.Is` 追溯 ⇒ 客户端
  断连（caller 取消）也被记成 `ErrUnavailable` 并 `markUnavailable()`；
  同理 RPC 预算的 send 类会被置 paused（rpcbudget.go:121-138）直到下次成功
  决策。**确定建议（内部分辨，分两步，不改对外映射/PD-1）**：
  (i) 改双 `%w`（`fmt.Errorf("%w: %w", ErrUnavailable, err)`，Go1.20+
  支持）——`errors.Is(ErrUnavailable)` 与 policy 判定、HTTP 503/429 形状
  全部不变，内部可 `errors.Is(err, context.Canceled)` 区分「调用方取消」
  与「依赖故障」；(ii) 豁免副作用：`limiter.go:229-232` 对
  `errors.Is(err, context.Canceled)` **不调用 `markUnavailable`**（仍返回
  ErrUnavailable 包装错误 ⇒ 本请求的 policy 判定/503 形状不变）、
  `rpcbudget.go:136-138` 对 canceled 不 `setPaused`——否则仅凭双 `%w`
  无法阻止「客户端断连被记成 limiter 中断 + send paused」的状态面放大
  （这是独立复核指出的 P1：识别≠豁免）。单凭 (i) 只解决可追溯性；
  (i)+(ii) 才兑现「取消与依赖故障在内部真正分开」。
  **(iii) 识别口径 = 错误身份优先 + ctx 仅用于超时歧义消解（四分类，
  本轮定向修正）**（独立复核 P1：推荐配置下 `MaxRetries=-1` 使在途取消的
  go-redis 终态可能是 socket `i/o timeout`——无重试路径把 Sleep 转成
  `context.Canceled`——`errors.Is(err, context.Canceled)` 不命中）。确定
  判别契约（Eval 失败后按顺序；`parent` = 调用者 ctx，`budgetCtx` =
  limiter `WithTimeout(L)`）：
  1. 错误身份优先：`errors.Is(err, context.Canceled)` ⇒ **父取消**；
     `errors.Is(err, context.DeadlineExceeded)` ⇒ 父 deadline 已触发则
     **父deadline**，否则 **预算到期**；
  2. 仅 timeout 形状（`net.Error.Timeout()`）消解歧义：`parent.Err()`
     Canceled ⇒ 父取消；DeadlineExceeded ⇒ 父deadline；`budgetCtx` 到期
     ⇒ 预算到期；无 ctx 到期 ⇒ 传输故障；
  3. **其余一律传输故障，无论 ctx 状态**（EOF/refused/reset/池超时/脚本
     错误）——**真实故障不得被共时的取消掩盖**（「不能仅凭返回时
     ctx.Err() 分类」的硬约束）。
  豁免面（唯一）：**父取消**不 `markUnavailable`/`setPaused`；父deadline、
  预算到期、传输故障保持现行污染面（无法证明依赖健康 ⇒ 保守）。错误以
  `errors.Join(ErrUnavailable, 原err[, ctx cause])` 返回（原 err 已含该
  cause 时不重复；三者均可 `errors.Is` 回溯；传输路径保持 `%w: %w`
  原文本形状）。**边界硬约束**：取消仍返回 `ErrUnavailable` 包装 ⇒
  PD-1 503/查询放行等对外映射不变；**不放行任何请求、不清除既有
  Unavailable/Paused、不提前进入恢复**。
  三步 (i)(ii)(iii) 均列入实施范围（§7），非用户决策；§7 增补
  「禁重试客户端在途取消的身份断言」与「传输故障 × 取消竞争不得被
  掩盖」验收。

## 2. 执行机制评估与最小推荐（闭合 2）

| 选项 | 改动面 | 真实上界 | 共享接缝副作用 | 判定 |
|---|---|---|---|---|
| a) 共享客户端 `ContextTimeoutEnabled: true` | 1 字段（serve.go:193-198） | **L+ε**（socket deadline = min(now+W/R, D)；无 deadline ctx 回落 now+t，与现状一致）；**实测已证**（8 个 CTE-on 场景 + 1 反向对照，§7） | 见下方「共享接缝条件化结论」——正常态近似不变但**定义已变**、故障态差异可达秒级；**且不满足不变量 3**（预算内断连仍默认重发，实测 3–4 次双扣；共享客户端禁用重试被本轮边界禁止） | ❌ 不满足边界（不变量 3） |
| b) EVAL 局部 SetRead/WriteTimeout | v9.22.0 **API 不可用**（setReadTimeout/readTimeout 未导出，command.go:557-559/:232；Eval 不设置，scripting_commands.go:30-53）；fork 强设则 cmdTimeout=t+10s 反而放宽（redis.go:1454-1462） | fork+CTE=false：D+(W+t+10s)（更差） | 需维护 fork；失去读超时重试（retryTimeout→0） | ❌ |
| c) **限流专用隔离客户端**（独立 Dial/Read/Write/Pool + CTE=true + **MaxRetries=-1**） | 第二个客户端 + serve.go:269 换绑 + 生命周期（4 处） | **L+ε**（同 a，实测已证）+ 单次尝试（不重发，实测 1/1/1）+ 快速失败 35.5ms | 共享客户端（探测/缓存）**完全不动** ⇒ 三接缝零行为变化；连接数 +1 池须显式设小；限流面状态沿用既有 `ratelimit_unavailable` gauge（无新探测缺口） | ✅ **本轮推荐**（唯一同时满足界+不变量 3） |
| d) 只收紧 L、保留共享客户端 | 0 | L + n×(W+R)（热 ≥L+2s/冷 ≥L+4s）——**不得界**（CTE=false 对照实测 1001ms） | 故障期共享池 churn 上升（更多 Remove） | ❌（单独不充分） |
| e) 熔断/探测驱动短路 | — | — | — | **本轮明确排除**（用户裁决；且会触及 contracts/redis.md §3.2 需增列错误类） |
| f) 每命令 NoRetry（限流命令局部禁重试） | go-redis Cmder 的 `NoRetry()` 恒 false 且无导出 setter（command.go:245-248、:570-572；仅两处库内覆写 :1016/:1111，均非 Eval） | — | 需 fork | ❌（API 不可达；库方对不可重传命令的官方建议即 `MaxRetries:-1`，autopipeline.go:527-528） |

**最小推荐方案 = c**：新增限流专用 `redis.NewClient`（仅 `ratelimit.NewRedisScriptStore`
换绑 serve.go:269），Options = `ContextTimeoutEnabled: true` +
`MaxRetries: -1` + 显式小 `PoolSize`；`DialTimeout/Read/WriteTimeout` 保持
= `cfg.Redis.Timeout`（不收紧既有旋钮）。理由：
1. 实测证明 CTE 收敛上界（8 个 CTE-on 场景 ≤L+ε + 1 个 CTE-off 反向
   对照 1001.3ms；权威 -race 轮 ε 最大 3.0ms、跨轮最大 8.4ms，测试容差
   80ms——统计口径非常数）；
2. 实测证明 `MaxRetries=-1` 满足不变量 3（1/1/1、无双扣、快失败 35.5ms），
   且**不全局关闭共享重试、不改脚本协议**——这是 a 方案拿不到的边界；
3. 共享客户端零改动 ⇒ 探测/缓存/RPC 三接缝的条件化影响（下表）全部
   不实现化——风险面不扩散；
4. RPC 预算（rpcbudget→同一 limiter）自动随隔离客户端受益：EVAL 段
   ≤min(RPC ctx, L)，send pause/取放槽时序提前且 fail-closed 形状不变。

**共享接缝条件化结论**（原「零影响」修正，供将来若评估 a 时参考；
本轮 c 方案下不生效）：
- **健康 Ping**：ctx=`WithTimeout(runCtx, 5s)`（dependencies.go:124）；
  当 池+dial+init < 4s（=ProbeTimeout−ReadTimeout）时 socket deadline 数值
  不变；否则提前（改善）；**关停在途读不因 CTE 更早中断**（两模式相同，
  cancel 不改已装载 deadline）；重试轮数上界不变（≤4 轮，应用层 0 次，
  恢复 = 下一 tick ≤2s）。
- **缓存**：deadline 定义从 `read_start+1s` 变 `call_start+1s`
  （cache.go 各 WithTimeout(1s) 与 ReadTimeout 同值）；正常态差值≈0ms
  （**定义变了但行为近似**），故障态差值=池+dial（可达秒级）⇒ 单次调用
  上界 ≈2–3s → 1s+ε；重试轮数上界不变（慢读=1、快失败≤4——退避 Sleep
  本就用原始 ctx，`redis.go:1326`，**CTE 不增减轮数门控**）。
- **serve 侧 RPC 预算**：出站 ctx read=5s/send=15s ≫ L ⇒ EVAL 恒被 L
  封顶；实际影响 = 归还被越界占用的 0–4s 出站预算 + send pause/read 取放槽
  时序提前；若出现 RPC ctx < L 的调用点（现未观测）结论改写。

**副作用（写入设计的验收范围）**：
① 越界错误形态变化：先 `i/o timeout` 再经退避 Sleep 变成
`context.DeadlineExceeded/Canceled`——错误分类观测面变化，§3.2 四分类
不变（超时仍归失效）；
② 被截断的读 ⇒ isBadConn ⇒ Remove——与现状同性质的 churn，但**限流池
隔离**后不波及缓存/探测；
③ **单次尝试的可用性取舍**（MaxRetries=-1 的代价）：瞬时网络抖动不再被
重试吸收，直接一次失败 ⇒ 按 PD-1 频繁 503/gauge 翻转的概率上升——
该取舍与「禁止静默双扣」的硬约束并列，其误拒绝率并入 §8 待决策 ② 一并
测量（正常态无故障时的单次失败率）。

## 3. EVAL 已执行但响应丢失的分类与重试策略（闭合 3）

- **判定纪律**：任何超时/取消/断连都**不得**解读为「服务端未执行」。
  token-bucket script 非幂等（HMSET 覆写 + PEXPIRE，limiter.go:111-131；
  无幂等键）：EVAL 已执行但响应丢失 ⇒ token 已扣；重发同参数 EVAL ⇒
  再扣一次（now 相同 ⇒ refill=0，扣减再跑一次）。
- **不变量修正（本轮核心更正）**：上一轮「开启 CTE 后结构上不会自动
  重发」**已被可控反例证伪**（`redis-budget-cte/README.md` §2）：
  - 连接在**预算到期前**被断开（EOF 可重试 error.go:90-92；ctx 未到期
    ⇒ Sleep 不被拦）⇒ go-redis 默认重试最多 4 次尝试（MaxRetries=3）
    ⇒ 实测
    **单次 Allow 3–4 次发送 / 3–4 次服务端执行 / 3–4 次 token 扣减**
    （权威 -race 轮 3 次、扣减≈3.0——JSON 精确差 2.992；第二轮旁证 4 次发送/4 次执行/3 次扣留观测、扣减≈4.0；跨轮
    sends==execs==扣减 自洽）。
  - **结构性不重发的只有「ctx 已到期」路径**：响应扣留至预算到期组
    实测 1 发送/1 执行（读超时撞过期 ctx 的 Sleep ⇒ 终止）。
  - 修正后不变量（写入实施验收）：**① ctx 到期 ⇒ 不重发（结构性，
    已实证）；② 预算内断连的防重发必须由限流专用客户端
    `MaxRetries=-1` 保证（已实证 1/1/1），不得依赖 CTE、不得全局关闭
    共享客户端重试、不得新增脚本幂等协议。**
- **分类**：超时/断连 ⇒ `ErrUnavailable`（§3.2 失效，不是拒绝）——不变；
  PD-1 的 503 触发与形状不受影响。
- **重复扣除的影响与运维口径**：单键重复扣 N 个 token 是暂时性丢弃 N 次
  决策机会（非资金动作），但违反「不悄然双扣」的本轮硬约束 ⇒ 由
  MaxRetries=-1 消除；EVAL 错误日志/计数**不得**统计为「服务端未执行
  次数」（执行数以 INFO commandstats / bucket 状态为准，测试同口径）。
- **三计数口径**（测试与运维一致）：应用调用数（Allow 次数）、发送数
  （服务端绑定的 EVAL 帧）、已确认服务端执行数（INFO cmdstat_eval /
  bucket tokens 差值）——三者分列，绝不互相推定；未收到响应 ≠ 未执行。
- **可观测面**：`ratelimit_unavailable` gauge / `ratelimit_recovery_total`
  （redis.md §6）保持；错误分类计数沿用 `classifyRedisError` 语义
  （测试侧），生产指标面不新增。

## 4. PD-1 与误拒绝/恢复梯度影响（闭合 4）

- **PD-1 形状不变**：`ErrUnavailable` ∧ `ClassNewWithdrawal` ⇒ 503
  RetryableError（policy.go:97-105）——L 收紧与 CTE 均不改变触发条件、
  错误分类、Retry-After；「0 次无限制放行」方向不变。
- **查询不变**：policy 对非创建类返回 nil（policy.go:107-110）⇒ 查询继续
  经过认证、归属校验、PG 权威读取（withdrawalhttp.go/intake.go/query.go
  既有链路）；缓存回源超时不受本设计影响（**不**复用 EnvRedisTimeout，见 §5）。
- **误拒绝风险（显式声明）**：L 收紧把「Redis 慢但活着、EVAL 往返落 (L,1s)」
  的决策从「慢但成功」变为「失效 ⇒ NewWithdrawal 503」。已测正常态
  p50/p95/p99 ≈ 1.4/1.8/2.2ms、max ≈ 11ms（stats.json normal 六组）⇒
  L=200–300ms 有 20–70× 余量；但 (L,1s) 中间态**无实验样本**（证据空白，
  明示）。Write/Operator 在失效时 0 误拒绝（直落原门禁，代价是拥塞期
  限流事实失效 + gauge 抖动）；RPC send 类「慢于 L 即按故障处置（安全
  暂停）」——redis.md §4.2 的「Redis 故障时」在实现上等价于「慢于 L 时」，
  语义面扩大须在裁决时显式确认（不改条款文字）。
- **恢复判定与梯度**：恢复探测仍由每请求 EVAL 承担（markAvailable 只在
  Allow 内翻转，limiter.go:238/:290-298）；单次探测成本从 ~1s 降到 L
  （若后续采用显式更小预算值）。Recovering 半速率 + 10s 窗与 L 无耦合
  （ratelimit_middleware.go:42）。**实测**：③→④ 翻转后首条可信 Allow
  5.6ms、`Recovering()` 梯度窗仍开（`redis-budget-cte` 归档）——恢复
  契约不变。间接效应：失效更频 ⇒ markUnavailable 清零 recoveringUntil
  （limiter.go:280）⇒ 半速窗启停更频繁、gauge 翻转更密——梯度本身不变，
  观测密度上升（告警阈值待测口径不变）。
- **取消 vs 依赖故障的内部分辨**：见 §1 末条确定建议（双 `%w` 包装，
  对外 503/429 形状与 policy 判定不变，内部可区分 `context.Canceled`）——
  这是内部观测修正，不改 PD-1、不改错误映射。

## 5. 配置兼容方案（闭合 5，确定建议）

- **独立预算键**：现 `TXHARBOR_REDIS_TIMEOUT` 有三个消费面（socket 三值
  serve.go:195-197、缓存回源 serve.go:260、限流预算
  ratelimit_middleware.go:68）；复用会连带收紧 §2.5 回源超时（契约条件 2）。
  **确定：新增 `TXHARBOR_RATELIMIT_BUDGET`**（语义落 RateLimitConfig，
  config.go:626-632）。
- **默认值与旧配置兼容（确定）**：**缺省保持旧预算数值**——新键未设置
  时沿用 `TXHARBOR_REDIS_TIMEOUT` 现值（1s）⇒ 旧部署的**预算数值**零
  迁移；**200/300ms 仅作为本实验的显式验证配置**（测试/文档输入），不是
  生产缺省、不是 SLO——生产取值由 §8 待决策 ① 经测量后裁定。
  **如实声明（独立复核指出）**：隔离客户端 + CTE + `MaxRetries=-1` 是
  **机制变更**——即便缺省预算仍为 1s，故障期行为（不重发、单次尝试、
  快速失败）也与今天不同；本设计不宣称「整体零行为变化」，只宣称
  「预算数值缺省继承」。正常态（Redis 健康）路径无机制差异（实测
  normal 6.7ms、0 背景增量）。
- **合法范围（确定）**：duration() 天然 must-be-positive（config.go:1043-1049）；
  新增关系校验「显式 budget ≤ Redis.Timeout」（先例 config.go:848-852/
  :1312-1315/:1436-1439）；limiter 构造期 fail-closed 双保险已有
  （limiter.go:153-158）。
- **适用类别（确定，不分面）**：limiter.Config.Timeout 是单一注入点 ⇒
  五类统一（NewWithdrawal/Write/Query/Operator/RPC）。分面需要新增
  per-class 时长结构与校验、且当前无任何按类差异的测量证据 ⇒ **不分面**；
  若将来出现「查询可更长、创建更短」的实测证据，再作为独立提案。
- **改动点清单（8 项，第三轮实施）**：① config.go 常量区键名
  （:118-129 旁）；② 默认值（继承语义，:311-313 旁）；③ 承载结构字段；
  ④ duration 装配 + 关系校验（:1371 旁）；⑤ buildLimiter 改读新字段
  （ratelimit_middleware.go:68）；⑥ config_013_test.go 三处同步（默认断言
  :57-59、解析 :93/:115、FailClosed 表 :174-225）；⑦ 夹具同步三处
  （ratelimit_failure_integration_test.go:60、faultdrill/scene.go:219、
  perf/harness.go:543）；⑧ 隔离客户端装配 4 处（serve.go:269 换绑、
  启动 fail 路径、关停 Close、测试夹具同构）。
- **配置文档登记（确定）**：沿用 013 现状（013 既有键**未**登记在
  .env.example/README，唯一命中 benchmark_report.md:81-82）——**实施
  批次把 013 既有键与新键一次性成组补登记**（不孤立新增、不制造
  「部分登记」的新不一致）；本批已执行（.env.example 全 013 键成组登记）。
- **TTL 解耦（本轮定向修正：独立参数 × 原算法）**：limiter.go:227 的
  `2*l.timeout` 同时是令牌桶键 TTL——L 收紧会把 TTL 从 2s 压到 400–600ms，
  空闲过期 ⇒ `tokens=burst` 全额重置（limiter.go:111-113），等价于用预算
  值改写限流算法。**实测印证**：候选 L=200ms 下 bucket TTL=400ms，hold 组
  settle 时 key 已过期（`redis-budget-cte` 归档 `tokens_settled=""`）。
  **第二轮曾定「独立常量 2s」，本轮按用户定向修正为**：TTL 作为 limiter
  的独立构造参数（`Config.BucketTTL`），装配处**仍按原算法**计算 =
  `2 × 有效 Redis.Timeout`（旧值来源）——`2×` 算法不变、输入换成显式旧值，
  与预算 L 完全无关；**不固定归一为 2s、不新增 TTL 迁移**。
  等价性：对任意旧部署（含非默认 `TXHARBOR_REDIS_TIMEOUT`）TTL 数值逐位
  不变（旧值就是输入）⇒ 补给（Δt×rate，与 TTL 无关）、到期时机、续期
  节奏（每次调用无条件 PEXPIRE）、过期→全量重置四条语义全部保持；新预算
  200/300ms 不再触碰桶生命周期（新验收：PTTL 恒 2×Redis.Timeout）。
  实现 = limiter.go 构造字段 + 装配处 `2 * cfg.Redis.Timeout` 一行 +
  「TTL 不随 L 变化」专项断言（默认与非默认配置对照）。

## 6. 变更分级判断（真实契约对照）

**结论：轻量补充设计级**。理由：只动「限流调用方预算（新键）+ 限流专用
隔离客户端装配（CTE/MaxRetries/PoolSize，均在 serve 装配处新增，不动共享
客户端）+ TTL 独立参数（原算法）+ 内部错误包装（双 `%w`）」，PD-1（redis.md:23-27
§3.3）、失效四分类（:22 §3.2）、恢复梯度（:29 §3.5）、RPC 基线（:33-36 §4）
全部保持既有条款文字与语义；契约文本不含毫秒数值（:21/:22/:24）⇒ 不违反
条款。**前提条件两项**（不满足则升级为契约变更，须另行裁决）：
1. 令牌桶键 TTL 与预算解耦为独立参数、按原算法以旧有效 Redis.Timeout
   计算（否则 §3.1 被预算值间接改写）——本轮定向修正为**全配置逐位等价、
   零迁移**（§5）；
2. 使用独立预算键且缺省继承旧预算（否则 §2.5 回源超时被连带改动、或旧
   部署行为漂移）——本轮已确定（§5）。
**显式声明（不改条款文字、语义面扩大）**：§4.2 的故障判定阈值实际 = L
（「慢于 L 即按故障处置」）；§3.3 的 503 在 (L,1s) 拥塞区间成为误拒绝
（须按既有要求单独评审 + 验收上限项）。
**不改**：contracts 文件、PD-1、恢复梯度、失效分类、对外错误形状、
共享客户端（探测/缓存）行为。

## 7. 实施范围与验收条件（第三轮实施批次——本轮执行）

**实施范围**（最小集）：
1. **限流专用隔离客户端**（serve.go:269 换绑；`ContextTimeoutEnabled: true`
   + `MaxRetries: -1` + 显式小 `PoolSize`；Dial/Read/WriteTimeout 维持
   = `cfg.Redis.Timeout`；启动 fail 路径与关停序列补 Close；探测/缓存
   继续用共享客户端不动）；
2. 新增 `TXHARBOR_RATELIMIT_BUDGET`（§5 清单 8 项；**缺省继承旧预算**）；
3. TTL 解耦为 limiter 独立参数 `Config.BucketTTL`，装配处按原算法计算
   `2 × cfg.Redis.Timeout`（limiter.go 构造字段 + ratelimit_middleware.go
   一行；**不固定 2s、不新增迁移**）；
4. `buildLimiter` 改读新键；内部错误包装与取消身份按 §1 末条三步落地：
   (i) `errors.Join`/双 `%w`（可 Is 追溯）、(ii) `context.Canceled` 豁免
   `markUnavailable`/`setPaused`、(iii) 识别口径用 limiter 侧 `ctx.Err()`
   三方判别（禁重试配置下 go-redis 错误串可能是 i/o timeout，不能当
   取消身份）；
5. 共享客户端零改动。

**机制验证基线（第二轮归档；实施批次已复跑确认，结果见实施记录）**：
`docs/evidence/013/redis-budget-cte/`（权威 = `-race -v` 单轮归档，
`race_v_run.log`；两测试全 PASS、无 DATA RACE）：
- CTE 边界 9 记录：热读阻塞 200.6/202.2ms、池等待取消 61.5ms（即时）、
  读在途取消 203.0ms（非即时）、冷初始化 200.9ms、父预算 80ms→81.6ms、
  CTE 关闭对照 1001.3ms（**反向对照，不计入 L+ε 统计**）、normal 6.7ms、
  recovery 5.6ms+梯度窗开；8 个 CTE-on 场景 ≤L+ε（权威轮 ε 最大 3.0ms、
  跨轮最大 8.4ms，测试容差 80ms——**ε 是统计口径非常数**，原「ε≤50ms
  （cached-time）」表述撤回：v9.22.0 `getCachedTimeNs=time.Now()` 无
  staleness（conn.go:29-40），50ms 只是历史注释）；
  9 场景返回后 2s 后台 accepts 增量全 0（观测，非协程退出证明）。
- 响应丢失 3 组：close-before-expiry 3/3/3 + 扣减≈3.0（双扣反例；
  第二轮旁证日志 4/4/3（其 withheld=3 即上述已知缺口样本），重发
  3–4 次随退避波动）；hold-to-expiry 1/1；
  isolated_noretry 1/1 + 35.5ms；bucket TTL=400ms 过期实证；
  `responses_withheld` 次级计数 6 轮 1 例漏计（已知缺口，不参与断言）。

**实施批次验收条件**（已执行；结果与证据见文末「实施记录（第三轮）」）：
（integration_redis/e2e 现有层，复用 a59dcd1+本轮夹具；**不默认全量故障矩阵**）:
- **禁重试客户端的取消身份断言（新增，独立复核 P1）**：在隔离客户端
  （MaxRetries=-1）+ CTE 下于在途读取消 ⇒ `errors.Is(err,
  context.Canceled)` 为真、limiter 不 `markUnavailable`、rpcBudget 不
  `setPaused`；依赖超时路径仍 `markUnavailable`（两路对照断言）。
- **整决策耗时**（Allow 入口→返回，非子阶段）：四形态（拒连/合成拨号阻塞/
  冷连接初始化无响应/热连接命令无响应）+ 正常 + 恢复 + 父请求取消，
  断言 `T_allow ≤ min(parent, L) + ε`（ε 用实测统计口径并在证据中记录
  实际最大值；**取消用例的正确断言是 ≤Allow 入口+L+ε**，不是
  「取消时刻+ε」——后者已撤回）；逐形态 n≥16、两轮，父 deadline 早于 L
  用例保留（min(parent,L) 生效）。
- **响应丢失回归（本轮反例转正向断言）**：三组计数（应用调用/发送/已确认
  执行）1/1/1 在隔离客户端+MaxRetries=-1 下保持；断言无双扣
  （tokens 扣减 <1.5）；INFO commandstats 作为执行铁证。
- **HTTP 策略回归（e2e 层）**：`ratelimit_failure_integration_test.go` 全绿
  （PD-1 503 形状/0 行/幂等重放/查询 200+bypass/预算 paused-degraded/
  恢复窗 30s）。
- **资金不变量回归**：`go test ./internal/withdrawal/...`（intake/auth/query
  + import 解析器守卫）与 007/011 既有门禁全绿（限流不可信时新提款仍
  503、0 行落库；查询经认证、归属校验、PG 权威读取不变）。
- **配置回归**：config_013_test 新键三处断言 + 关系校验 case +
  「缺省继承旧预算」的兼容断言；**TTL 专项断言：键 TTL 恒 = 2×有效 Redis.Timeout（缺省 2s）且不随 L 变**。
- **观测面检查**：`ratelimit_unavailable` 翻转密度变化被预期并记录；
  双 `%w` 后 `errors.Is(ctx.Canceled)` 可分取消与依赖故障（新增断言）。
- **误拒绝测量（方法先行，数值待测）**：V-RATELIMIT 增列
  「(L,1s) 中间态 NewWithdrawal 误拒绝率」与「无故障态单次失败率
  （MaxRetries=-1 的抖动代价）」测量方法；阈值待生产测量后登记
  verification.md（只给方法不编数值的纪律不变）。

## 8. 待决策项（仅真正需要业务裁决或测量阈值的事项）

1. **生产 L 取值**：缺省已确定为继承旧预算（1s）；是否下调到某个
   验证值（200/300ms 仅本轮验证配置）——取决于 (L,1s) 中间态误拒绝率
   与生产 RTT 分布测量，**测量后裁决，不自行批准**。
2. **误拒绝率上限阈值**：(L,1s) 中间态 503 误拒绝率 + 无故障态单次失败率
   （MaxRetries=-1 放弃瞬时重试的可用性取舍）——两者都需要测量数值后
   在 verification.md 登记上限（该文件只给方法不编数值），**不自行批准**。

（已收敛为确定技术建议、不再推回用户的项：预算键默认=继承旧值；不分面；
取消/依赖故障内部分辨=双 `%w`（对外映射不变）；配置登记=013 键成组补登记；
限流面状态=沿用既有 `ratelimit_unavailable` gauge（c 方案无新探测缺口）；
TTL=独立参数、原算法 2×有效 Redis.Timeout（零迁移）。）

## 9. 证据边界

- 机制结论来自三路源码推导（MechanismAudit/ContractAudit/
  CTEBoundary+ResponseLossDesign+SharedImpactTTL，go-redis v9.22.0 +
  本仓 @ a59dcd1）与两轮实测（`redis-latency-abc/`、`redis-budget-cte/`）。
- 已测：L=200ms 单点的 CTE 边界、响应丢失三组、TTL=2×L 过期；
  **未测**：(L,1s) 中间态、生产拥塞分布、L 多点扫描、cancel-only 无
  deadline ctx 分支（本轮取消场景恒经 limiter 带 D）、隔离客户端在
  serve 全进程装配后的端到端分布。
- 200/300ms 不是生产 SLO；ε（调度误差预算，测试容差 80ms、观测最大
  3.0ms（权威轮）/8.4ms（跨轮））为统计口径，非硬常数。
- CI 三态（沿用已核证据，本轮未新查）：main 必需 CI run `37298301943`
  passed（be272cb）；旧提交 fault-perf run `37297533285` passed（2fca0b0）；
  Recovery Drill run `37298994567` **独立失败**（runner 环境性
  read-only gomodcache，**保持单列，本轮不修**）。

---

## 修订记录（第二轮定向验证，2026-10-06）

**源码级修正（CTEBoundary 指出，全部采纳）**：
1. §1 row3「后台拨号由 w.cancel 中止」撤回——`w.cancel()` 不调 cancelCtx
   （want_conn.go:40-58），后台拨号最长 ≈5.4s 且占住 turn+dials 许可；
2. 「ε≤50ms（cached-time staleness）」撤回——v9.22.0
   `getCachedTimeNs=time.Now()`（conn.go:29-40），ε 是统计口径（观测最大
   3.0ms（权威轮）/8.4ms（跨轮）），50ms 只是历史注释残留（方向即使存在
   也只会更早）；
3. 冷路径 n=2 漏计 identity 往返（redis.go:994-1008 ClientSetInfo×2），
   「冷 = L+4s 是结构上界」改为「按实际可达阶段计数」；
4. HELLO 非 RedisError 失败直接 Closed+return（redis.go:796-805），
   不走 AUTH 兜底 ⇒ CTE 截断 HELLO ⇒ Remove 必然发生。

**结论级修正**：
5. 推荐方案 a（共享客户端 + CTE）→ **c（限流专用隔离客户端）**：因
   响应丢失反例证明共享方案不满足不变量 3（预算内断连默认重发双扣，
   且共享客户端禁重试被本轮边界禁止）；c 由实验直接验证（界 + 1/1/1）；
6. 不变量「CTE 后结构上不会自动重发」证伪并改写（§3）；
7. 共享三接缝「零影响」→ 条件化结论（§2），且在 c 方案下不实现化；
8. 撤回项集中：主动取消不即时中断在途 I/O（实测 203.0ms vs 60ms 取消）；
   调用者返回 ≠ 后台全部退出（仅 2s accepts 观测）；cancel-only ctx
   分支未覆盖；「取消时刻起算」改为「Allow 入口起算」。

**新增实测**：`docs/evidence/013/redis-budget-cte/`（测试侧、-race 归档；
CTE 边界 9 场景 + 响应丢失 3 组 + TTL=2×L 过期实证 + 恢复 5.6ms 梯度窗）。

**确定建议沉淀**（不再推回用户）：见 §5/§8 尾括注。

## 第四轮修正记录（2026-10-06，独立复核后修复，同文档轮）

独立复核（未参与修改者 RoundReview）判定并已修复/如实标注：

**P1 两项**
1. **取消/依赖故障内部分辨的兑现路径补全**（§1 末条）：双 `%w` 只解决
   可追溯性，不能单独阻止「客户端断连被记成 limiter 中断 + send paused」；
   确定建议扩为两步 (i) 双 `%w` + (ii) 对 `context.Canceled` 豁免
   `markUnavailable`/`setPaused`（本请求判定与 503 形状不变）——识别与
   豁免同批实施（§7）。
2. **TTL 独立常量的等价性边界**（§5）：常量 2s 仅对缺省（1s）部署逐位
   等价；非默认 `TXHARBOR_REDIS_TIMEOUT` 部署的桶生命周期会变，实施
   批次须在配置登记/迁移说明中显式记录该差异（仓库内无非默认部署证据）。

**P0 口径类（数据/断言/表述，按复核的特别定义）**
3. 夹具修复 ×2（`internal/testutil/redisgate.go`，详见实验 README 第四轮）：
   EVAL 标记跨读重复计数窗口（只计延伸进新字节的出现）；Drop 连接对
   互关对端（原定时关客户端后对端 goroutine 永久阻塞）。
4. 断言补齐 ×3（normal `<L`+背景增量、recovery `≤30s`、cancel_poolwait
   错误文本由「仅记录」升级为断言）。
5. 后台采样补齐：normal/recovery/cte_off 未采样却记 0 → 9 场景全采样。
6. 字段截断：`drop_close_after_ms` 的 -1ns→0 截断改为比例换算（现记
   -1e-06，hold 组约定可辨）。
7. 「9 场景全部 ≤L+ε」改「8 个 CTE-on 场景 ≤L+ε + 1 个 CTE-off 反向
   对照」（对照组 1001.3ms 本身即反向证据）。
8. 「旧部署零行为变化」改「预算数值缺省继承；机制变更如实声明」（§5）。
9. 「走满 MaxRetries=3」改「最多 4 次尝试」（权威轮 3 次被预算截断）。
10. staleness 表述同步：实验 README/测试头注释删除「≤50ms 缓存时钟」项
    （与 §9 撤回一致）。
11. `error_class` 列与 JSON 对齐：canceled 落 `other`（分类器未建独立
    桶，不把未建写成已建）；`200.8ms`「预算内」改「容差内」。
12. 数值统一为权威 `-race -v` 单轮（`race_v_run.log`）；跨轮 4 次重发
    旁证保留为 `response_loss_run_second.log`；`responses_withheld`
    6 轮 1 例漏计列为已知缺口（次级口径，不参与断言）。

**红线复核结论（RoundReview）**：git 范围恰为 4 项（设计文档、
redisgate.go 纯新增 209 行 0 删除、新测试、证据目录）；生产代码/配置
默认/CI 零 diff；既有已提交测试文件零改动；`ContextTimeoutEnabled`、
`MaxRetries:` 在非测试文件零命中。

**第四轮二次验证（同轮闭环）**：复核者对 12 项修复再验——夹具 4 项、
断言/采样/字段/口径 5 项通过；剩余 5 项（P1：禁重试在途取消的身份识别
未闭合；P2：权威/旁证数值引用未统一、withheld「不参与断言」声明失实、
§6 未同步默认配置边界、扣减 3.00 精度）已再次修复：
1. §1 确定建议增补 (iii) `ctx.Err()` 三方判别 + §7 新增「禁重试客户端
   取消身份断言」验收（识别口径不再依赖 go-redis 错误串）；
2. §6 前提条件 1 同步 §5 的默认配置等价边界；
3. 数值统一为权威轮：61.5/203.0/35.5/5.6ms、扣减≈3.0（JSON 精确
   2.992）、第二轮旁证 4/4/3（含 withheld 缺口样本）；
4. withheld 声明改为「不参与跨组一致性断言（仅 hold 组 ≥1 单组下界）」；
5. 实验 README 同步（第二轮 4/4/3、缺口样本定位到留存旁证日志）。

---

## 实施记录（第三轮，2026-10-06，本批实施完成）

**落地范围**（验收证据 [redis-budget-implement/](redis-budget-implement/README.md)，
全部 PASS、无 DATA RACE）：

1. **限流专用隔离客户端**：`newLimiterRedisClient`（`ContextTimeoutEnabled=true`、
   `MaxRetries=-1`、`PoolSize=16`；Dial/Read/Write=`cfg.Redis.Timeout`）接入
   serve.go 限流换绑点；启动失败路径与关停序列补 `Close`；探测/缓存共享
   客户端零改动（复核见 git diff）。
2. **`TXHARBOR_RATELIMIT_BUDGET`**：缺省继承有效 `TXHARBOR_REDIS_TIMEOUT`
   （缺省 1s 保持；非默认旧值同样保持）；显式值必须 ≤ 之，否则启动拒绝
   （config_013_test 兼容/边界/非法值三面断言）；Summary 增
   `ratelimit_budget`；`.env.example` 013 键（含全部既有键）成组登记。
3. **TTL 解耦（本轮定向修正）**：`Config.BucketTTL` 独立参数，装配处
   `2 × cfg.Redis.Timeout`（原算法）——实测 PTTL 1998/1999/5999/5999ms：
   L=200ms 不触碰桶生命周期；任意旧配置逐位等价、零迁移。
4. **错误四分类**：错误身份优先（传输故障不查 ctx，不被共时取消掩盖）；
   仅父取消豁免 `markUnavailable`/`setPaused`，且不清除既有
   Unavailable/Paused（单测 + RPC 姿态测试）；仍返回 `ErrUnavailable`
   包装（PD-1 503/查询放行等对外映射不变；取消不放行任何请求）。
5. **机制复验与代价测量**（修复后权威轮，-race）：
   - 专用装配丢答（已执行+预算前断连）1 发送/1 执行/扣减 0.991/36.1ms；
     旧对照 3/3/2.993/201.3ms（失败反例保留；跨轮 3–4 次随退避波动）；
   - **取消身份装配级验收**（设计 §7 增补项，真实 socket）：在途取消
     200.5ms 返回且 `errors.Is(err, context.Canceled)` 为真（终态为
     socket `i/o timeout`，身份来自 join）、limiter 未标记、observer 0 事件、
     RPC send 未 paused；RPC 预算取用取消同样未 paused；对照（无取消超时）
     216.1ms、DeadlineExceeded、标记 +1、send paused——「仅父取消豁免」成立；
   - 并发池竞争 32 goroutine max 205.2ms、返回后 TotalConns=3（≤16 界内）；
   - CTE 边界 9 记录复跑全 ≤L+ε（cancel 非即时事实保持）；
   - HTTP 矩阵：正常态误拒绝 0%（四单元 p50 11.9–12.6ms）；(200/300ms,1s)
     中间拥塞（注入 400ms）误拒绝 100%（p50 201.9/302.1ms；L 的核心代价项，
     **阈值待裁决**）；拒绝键 0 行（无新付款意图）、接受键恰 1 行（幂等）。

**独立复核与修复（同批闭环）**：未参与修改者定向复核提出 5 项（.env.example
可选值默认生效、下游启动失败未关专用客户端、缺真实无重试取消验收、
GateDelay 定时窗口 pair 滞留、摘要数值与归档不一致），全部修复并重跑：
- `.env.example` 013 块全部注释化（作为未启用部署的既有行为零变化）；
- serve.go 增 deferred 启动清理（关停时序接管前，任何下游启动失败都关闭
  专用客户端）；
- 新增装配级取消身份测试（上条验收）与取消观测证据；
- `runDelay` 任一转发方向结束即回收 pair（客户端超时不再滞留后端连接；
  gate 自测以 `connected_clients` 回归基线证明，Hold 转换语义保持）；
- 证据摘要全部改引修复后归档值。
复核第二轮再验补充修复：设计记录 PTTL 元组同步为归档值
（1998/1999/5999/5999）；`runDelay` 在 Hold 翻转时吞掉「已读取、延迟中」
的应答（转换后不再交付排队应答）——新增回归断言并留存失败前证据
（临时回退修复必红：`eval during the delay window succeeded after the
Hold flip`，恢复后绿）。
6. **可用性取舍（如实）**：`MaxRetries=-1` 放弃库内瞬时重试——单次传输
   抖动即按不可用处置（换取「无双扣」与 L 硬上界；丢答路径实测 35ms
   快速失败）。无故障态单次失败率与中间拥塞误拒绝率的生产测量仍为
   §8 待决策②。
7. **边界**：`max_retries` 记录字段 -1=构造禁用（读回 0；未设会读回 3）；
   GateDelay 逐 backend→client 块延迟（实测单块；方向结束即回收 pair）；矩阵
   `old_assembly` 仅复刻旧客户端配置；ε/容差为测试口径、非 SLO；
   分面/熔断/查询缓存/Recovery Drill runner 仍不在本轮。
