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
| 3 | 拨号等待（caller 侧 select） | D（pool.go:1112-1119） | ✅（到期返 ctx.Err，后台拨号由 w.cancel 中止，want_conn.go:40） |
| 4 | dialConn 每尝试 | min(DialTimeout, attempt-ctx)；attempt ctx 由 **Background 派生**（pool.go:1059,716-725）——不继承 caller 剩余 | ❌（仅被 DT 封顶；5 次尝试+100ms 退避 ≈5.4s 后台预算） |
| 5 | dialErrorsNum 快路 | 0 耗时（pool.go:692-694） | — |
| 6 | 连接初始化等待（另一 goroutine 在 init） | min(D, DialTimeout)（redis.go:668-710） | ✅ |
| 7 | HELLO 握手 socket 写/读 | now±W/R（`c.context(ctx)`=Background，redis.go:1468-1473 + :793 + conn.go:1207-1235） | ❌ |
| 8 | 命令层重试退避 internal.Sleep | D（redis.go:1324-1328 → util.go:37-46） | ✅ |
| 9 | push 预处理 peek | min(D, 1ms)（redis.go:2546-2552） | ✅（≤1ms） |
| 10 | EVAL 命令 socket 写 | now+W（redis.go:1348；Background） | ❌ |
| 11 | EVAL 命令 socket 读 | now+R（redis.go:1375；EVAL 无 per-cmd timeout，redis.go:1454-1466） | ❌ |

**总耗时的严格上界**（MechanismAudit 结论，本轮采纳为设计基准）：

```
T_allow ≈ D + n × (W + R) + ε
  n = 1（热连接：仅 EVAL 一段）
  n = 2（冷连接：HELLO 段 + EVAL 段；init pipeline 在本 wiring 下 0 条命令 ⇒ 0 I/O）
  今天 W=R=1s ⇒ 热 = L+2s，冷 = L+4s（把 L 收到 200ms 只缩短前半段）
```

**因此：仅缩短 `limiter.WithTimeout`（L）不构成总耗时有界**。要真正有界，
必须让「最后一次开始的 socket 段」也受 D 约束 ⇒ 见 §2 推荐机制。

**父请求更早截止 / 取消 / 清理**（闭合 1 的另一半）：
- 立即返回点：池信号量（semaphore.go:101-102）、dials select（pool.go:1053-1055）、
  拨号等待 select（pool.go:1113-1115）、重试退避 Sleep（util.go:42-44）、
  等初始化（conn_state.go:268）。ctx 错误经 shouldRetry=false 直接终止
  （error.go:103-105；redis.go:1242-1252）。
- **不会**中断点：在途 socket 写/读（#10/#11，Background deadline）、HELLO
  段、已进入 dialConn 的后台协程。取消落在读在途时的两种结局：
  ① 回复先到 ⇒ **成功返回**（redis.go:1446-1448 后无 ctx 复查）——即使
  ctx 已取消；② 无回复 ⇒ 读跑到 now+R 以 i/o timeout 结束 ⇒ retryTimeout=1
  ⇒ 重试 ⇒ 下一轮退避 Sleep 立即返 ctx.Err ⇒ 观测到取消最多滞后 R=1s
  （冷连接再加 HELLO 段越界）。
- 连接清理：读写超时与 ctx 错误均命中 `isBadConn(err,false,addr)=true`
  （error.go:193-196、:217-222）⇒ `p.Remove`（redis.go:1049-1051）而非归池；
  仅成功回复归池（redis.go:1070）；初始化失败 Remove（redis.go:536-538）；
  caller 在拨号等待中返回且 cn 已生成 ⇒ 归 idle 并还 turn
  （pool.go:1069-1071）——无泄漏。
- **设计必须显式声明的取消语义副作用**（MechanismAudit 发现）：limiter.go:231
  用 `%w: %v` 包装，底层 `context.Canceled` 无法 `errors.Is` 追溯 ⇒ 客户端
  断连（caller 取消）也被记成 `ErrUnavailable` 并 `markUnavailable()`；
  同理 RPC 预算的 send 类会被置 paused（rpcbudget.go:121-138）直到下次成功
  决策。这不是本设计引入的，但「缩短预算」会放大其出现频次 ⇒ 列入 §4 待决项。

## 2. 执行机制评估与最小推荐（闭合 2）

| 选项 | 改动面 | 真实上界 | 共享接缝副作用 | 判定 |
|---|---|---|---|---|
| a) 共享客户端 `ContextTimeoutEnabled: true` | 1 字段（serve.go:193-198） | **L+ε**（socket deadline = min(now+W/R, D)；无 deadline ctx 回落 now+t，与现状一致） | 探测（5s ctx>1s socket）：无变化；缓存（每操作自 WithTimeout(1s)）：把其声明语义做实（cache.go:105-106）；限流：L 成为真旋钮；RPC 预算：EVAL 段被 min(RPC ctx, L) 封顶，RPC 自身预算不动 | **推荐** |
| b) EVAL 局部 SetRead/WriteTimeout | v9.22.0 **API 不可用**（setReadTimeout/readTimeout 未导出，command.go:557-559/:232；Eval 不设置，scripting_commands.go:30-53）；fork 强设则 cmdTimeout=t+10s 反而放宽（redis.go:1454-1462） | fork+CTE=false：D+(W+t+10s)（更差） | 需维护 fork；失去读超时重试（retryTimeout→0） | ❌ |
| c) 限流专用隔离客户端（独立 Dial/Read/Write/Pool + 自开 CTE） | 第二个客户端 + serve.go:269 换绑 + 生命周期 | L+ε（同 a，且可叠 MaxRetries=-1/DialerRetries=1 把尝试压成单次） | 连接池翻倍须显式设小；**探测面缺口**：redis_available 仍只探共享客户端，限流面需改用 `limiter.Unavailable()` 或补探测；缓存/限流 churn 隔离（收益） | 备选（要求零外溢时） |
| d) 只收紧 L、保留共享客户端 | 0 | L + n×(W+R)（今天热 L+2s/冷 L+4s）——**不得界** | 故障期共享池 churn 上升（更多 Remove） | ❌（单独不充分） |
| e) 熔断/探测驱动短路 | — | — | — | **本轮明确排除**（用户裁决；且会触及 contracts/redis.md §3.2 需增列错误类） |

**最小推荐方案 = a**：`serve.go:193-198` 的 `redis.Options` 增加
`ContextTimeoutEnabled: true`（1 字段）。理由：
1. 单点改动让 Allow 的每个阻塞段（池、拨号等待、退避、等初始化、HELLO、
   EVAL 写/读）都被 `min(parent, L)` 封顶 ⇒ 上界从 `L+n×(W+R)` 收敛为 `L+ε`；
2. 对探测零影响（probe ctx 5s > socket 1s，socket deadline 不变）；对缓存只是
   把其注释声明的语义做实；不默认全局收紧任何时长旋钮；
3. 与「不默认全局收紧」完全兼容：没有任何既有超时值被改动，只是 socket
   开始遵守调用方**已有**的预算；
4. b 不可用、d 不得界、c 得到同一个界但改动面与探测缺口更大。

**副作用（写入设计的验收范围）**：① 越界错误形态变化：先 `i/o timeout`
再经退避 Sleep 变成 `context.DeadlineExceeded/Canceled`（redis.go:1326、
error.go:103-105）——错误分类观测面变化，§3.2 四分类不变（超时仍归失效）；
② 被截断的读 ⇒ isBadConn ⇒ Remove——与现状 1s 读超时同性质的连接 churn，
故障期共享池 churn 增加（缓存/探测共用池的排队可能变差——若实测不可接受，
升级为 c）。

## 3. EVAL 已执行但响应丢失的分类与重试策略（闭合 3）

- **判定纪律**：任何超时/取消都**不得**解读为「服务端未执行」。token-bucket
  script 非幂等（HMSET 覆写 + PEXPIRE，limiter.go:111-131；无幂等键）：
  EVAL 已执行但响应丢失 ⇒ token 已扣；重发同参数 EVAL ⇒ 再扣一次
  （now 相同 ⇒ refill=0，扣减再跑一次）。
- **分类**：超时 ⇒ `ErrUnavailable`（§3.2 失效，不是拒绝）——不变。本设计
  **不引入任何自动重发**：现状（a59dcd1 实验+MechanismAudit 复核）下
  ctx 截断使热连接实际单次发送；开启 CTE 后读被 D 截断 ⇒ isBadConn ⇒
  Remove ⇒ 同样**不会**自动重发（退避 Sleep 撞 ctx 立即终止）。将「不得
  自动重发」写成实现的显式不变量（依赖现有 ctx 截断结构，无需新代码）。
- **重复扣除的量化影响**：单键重复扣 1 个 token 是暂时性丢弃一次决策机会，
  非资金动作、不影响 PD-1 语义；运维口径：EVAL 错误日志不得统计为
  「服务端未执行次数」。
- **可观测面**：`ratelimit_unavailable` gauge / `ratelimit_recovery_total`
  （redis.md §6）保持；错误分类计数沿用本轮 `classifyRedisError` 语义
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
  Allow 内翻转，limiter.go:238/:290-298）；单次探测成本从 ~1s 降到 L；
  Recovering 半速率 + 10s 窗与 L 无耦合（ratelimit_middleware.go:42）。
  间接效应：失效更频 ⇒ markUnavailable 清零 recoveringUntil（limiter.go:280）
  ⇒ 半速窗启停更频繁、gauge 翻转更密——梯度本身不变，观测密度上升
  （告警阈值待测口径不变）。

## 5. 配置兼容方案（闭合 5）

- **需要独立预算键**：现 `TXHARBOR_REDIS_TIMEOUT` 有三个消费面
  （socket 三值 serve.go:195-197、缓存回源 serve.go:260、限流预算
  ratelimit_middleware.go:68）；复用会连带收紧 §2.5 回源超时（契约条件 2）。
  新键建议 `TXHARBOR_RATELIMIT_BUDGET`（语义落在 RateLimitConfig，
  config.go:626-632——比 RedisConfig 更准）。
- **默认值**：沿用「有注释初值、测量后校准、禁止编造生产阈值」纪律
  （config.go:145-148）。候选验证值 **200ms/300ms 仅作实验与评审输入**，
  默认值待测量后裁决；本设计**不指定**默认数。
- **合法范围**：duration() 天然 must-be-positive（config.go:1043-1049）；
  建议新增关系校验「budget ≤ Redis.Timeout」（先例 config.go:848-852/
  :1312-1315/:1436-1439）；limiter 构造期 fail-closed 双保险已有
  （limiter.go:153-158）。
- **适用类别**：limiter.Config.Timeout 是单一注入点 ⇒ 五类统一
  （NewWithdrawal/Write/Query/Operator/RPC），不分面；分面须新增结构，
  超出本轮范围（列为待决项）。
- **改动点清单（实施期，8 项，本轮不实施）**：① config.go 常量区键名
  （:118-129 旁）；② 默认值（:311-313 旁）；③ 承载结构字段；④ duration
  装配 + 关系校验（:1371 旁）；⑤ buildLimiter 改读新字段
  （ratelimit_middleware.go:68）；⑥ config_013_test.go 三处同步（默认断言
  :57-59、解析 :93/:115、FailClosed 表 :174-225）；⑦ 夹具同步三处
  （ratelimit_failure_integration_test.go:60、faultdrill/scene.go:219、
  perf/harness.go:543）；⑧ 文档登记——注意 013 既有键**未登记**在
  .env.example/README（唯一命中 benchmark_report.md:81-82）：要么沿用
  现状不登记，要么连同既有 013 键一起补登记，不得只登记新键。
- **隐藏耦合（必须解耦，否则实质触及 §3.1）**：limiter.go:227 的
  `2*l.timeout` 同时是令牌桶键 TTL——L 收紧会把 TTL 从 2s 压到 400–600ms，
  空闲过期 ⇒ `tokens=burst` 全额重置（limiter.go:111-113），等价于用预算值
  改写限流算法补给行为。设计要求：TTL 与预算解耦（独立常量或按 rate/burst
  推导），实施时一并落地。

## 6. 变更分级判断（真实契约对照）

**结论：轻量补充设计级**。理由：只动「限流调用方预算 + 共享客户端的
ContextTimeoutEnabled 字段（+ 新独立配置键）」，PD-1（redis.md:23-27 §3.3）、
失效四分类（:22 §3.2）、恢复梯度（:29 §3.5）、RPC 基线（:33-36 §4）全部
保持既有条款文字与语义；契约文本不含毫秒数值（:21/:22/:24）⇒ 不违反条款。
**前提条件两项**（不满足则升级为契约变更，须另行裁决）：
1. 令牌桶键 TTL 与预算解耦（否则 §3.1 被预算值间接改写）；
2. 使用独立预算键（否则 §2.5 回源超时被连带改动）。
**显式声明（不改条款文字、语义面扩大）**：§4.2 的故障判定阈值实际 = L
（「慢于 L 即按故障处置」）；§3.3 的 503 在 (L,1s) 拥塞区间成为误拒绝
（须按既有要求单独评审 + 验收上限项）。
**不改**：contracts 文件、PD-1、恢复梯度、失效分类、对外错误形状。

## 7. 实施范围与验收条件（后续实施批次，本轮不实施）

**实施范围**（最小集）：
1. serve.go:193-198 增 `ContextTimeoutEnabled: true`（机制 a）；
2. 新增 `TXHARBOR_RATELIMIT_BUDGET`（§5 清单 8 项）+ TTL 解耦（limiter.go:227）；
3. `buildLimiter` 改读新键；serve.go:195-197 维持 = Redis.Timeout 不变。

**验收条件**（全部在 integration_redis/e2e 现有层内，复用 a59dcd1 夹具；
**不默认全量故障矩阵**）：
- **Redis 集成（复用 `internal/ratelimit` 现有测试 + redislatency 夹具）**：
  对四形态（拒连 / 合成拨号阻塞 / 冷连接初始化无响应 / 热连接命令无响应）
  + 正常 + 恢复，断言**整个决策耗时**（Allow 入口到返回，含调度误差预算
  ε≤50ms——即 `T_allow ≤ min(parent, L) + ε`；冷形态上限 =
  `min(parent, L) + ε`，HELLO 段也被 CTE 截断）——**不是只测某个子阶段**；
  逐形态样本 n≥16、两轮（复用现夹具参数），并新增**父请求取消**用例：
  caller ctx 在 EVAL 在途取消 ⇒ 观测总耗时 ≤ min(parent 已过点)+ε（CTE 后
  socket 段受截断）且错误为 ctx 错误族。
- **HTTP 策略回归（e2e 层）**：`ratelimit_failure_integration_test.go` 全绿
  （PD-1 503 形状/0 行/幂等重放/查询 200+bypass/预算 paused-degraded/恢复
  窗）——L 收紧与 CTE 后形状不变。
- **资金不变量回归**：`go test ./internal/withdrawal/...`（intake/auth/query
  + import 解析器守卫）与 007/011 既有门禁测试全绿（限流不可信时新提款
  仍 503、0 行落库；查询 PG 权威读取不受影响）。
- **配置回归**：config_013_test 新键三处断言 + 关系校验 case；TTL 解耦后
  令牌桶补给行为有专项断言（键 TTL 不随 L 变化）。
- **误拒绝上限（新增验收项，数值待测/待裁决）**：V-RATELIMIT 增列
  「正常拥塞区间 (L,1s) 的 NewWithdrawal 误拒绝率」测量方法（ContractAudit
  建议采纳），阈值待生产测量后登记 verification.md（该文件只给方法不编
  数值的纪律不变）。
- **观测面检查**：`ratelimit_unavailable` 翻转密度变化被预期并记录；
  取消语义副作用（caller 断连记为 limiter 中断 + send 类 paused）作为已知
  行为记录在实施说明。

## 8. 待决策项（不自行批准）

1. **L 的取值与默认**：200/300ms 仅候选验证值；默认值待 (L,1s) 中间态
   误拒绝率测量后裁决。
2. **限流面探测缺口**（若升级选 c）：redis_available 是否增加限流客户端
   探测或改以 limiter.Unavailable() 为限流面信号。
3. **caller 取消与 Redis 故障的错误区分**：limiter.go:231 的 `%v` 包装使
   ctx 错误不可追溯（断连被记为 limiter 中断 + send paused）——是否改为
   `%w` 或预分类，属错误分类面变更，需单独评审。
4. **预算是否分面**（NewWithdrawal vs Query 等不同 L）：现单一注入点，
   分面须新增结构。
5. **(L,1s) 中间态误拒绝率**的测量方法与阈值登记进 verification.md。
6. **013 配置键文档登记策略**（沿用不登记 vs 连同既有键一起补登记）。

## 9. 证据边界

- 本设计全部机制结论来自源码推导（MechanismAudit/ContractAudit，
  go-redis v9.22.0 + 本仓 @ a59dcd1）与 a59dcd1 实验数据；无新运行数据。
- (L,1s) 中间态、生产拥塞分布、CTE 开启后的端到端时延分布 = 未测。
- 200/300ms 不是生产 SLO；L、ε（调度误差预算 50ms）均为候选验证参数。
