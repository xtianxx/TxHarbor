# Contract: Redis Cache & Distributed Rate Limiting (Failure Policy)

**Feature**: 013-reliable-event-infrastructure | **Plan**: [../plan.md](../plan.md) D5/D6 | **Research**: R10–R12 | **Spec**: FR-02/17/18/19；PD-1

## §1 允许用途（FR-02/章程 III）

Redis MAY 用于：缓存、限流、临时协调、非权威健康状态、性能优化。
Redis MUST NOT 作为：资金状态、幂等去重凭据、门禁判定、对账依据、消费者进度的**唯一**载体。缓存/限流丢失或不可用 MUST NOT 使权威状态不可恢复或门禁被绕过。

## §2 缓存契约（FR-17/FR-23）

1. **对象**：仅非权威读模型（查询展示、聚合、列表）。财务权威判定（充值/确认/提款/执行/对账/修订）MUST 直读 PostgreSQL；决策路径 MUST NOT 读缓存（继承 011 投影纪律）。
2. **键**：`txharbor:<epoch>:<family>:<id>`；`epoch` 为命名空间版本，Redis 清空/重建/检测到不一致时轮换；恢复后旧 epoch 键不可达（MUST NOT 提供已失效陈旧值）。
3. **值**：`(value, source_version, cached_at)`；响应携带新鲜度标注（`fresh` / `possibly_stale` + 时间戳）；新鲜度不可确认时 MUST 明确标注，MUST NOT 伪装权威。
4. **失效**：权威转换的 outbox 事件驱动删除受影响键范围（失效器复用消费者运行时，事件即失效信号）；TTL 兜底（初始 30s 展示类，测量后校准）。权威状态变化后受影响键范围 MUST NOT 返回陈旧财务权威（SC-08）。
5. **故障**：Redis 不可用/超时/不可信 → 直读 PostgreSQL；回源有界保护（singleflight + 并发信号量 + 超时），MUST NOT 以无界并发压垮 PG；故障期间查询可降级但不得错误声称新鲜。
6. **恢复**：惰性重建或受控预热；梯度恢复，MUST NOT 瞬间放开为无界；重建进度与降级状态可观测（FR-23）。

## §3 分布式限流与 PD-1 失效处置

1. **算法与作用面**：Redis 原子脚本（令牌桶/滑动窗口）按接口类限流（新提款创建、一般写、查询、操作员、RPC 预算）；速率/突发数值待测（测量方法 [../verification.md](../verification.md) §1/§4）；限流 MUST NOT 参与认证/授权/幂等判定。
2. **失效判定**：Redis 连接失败、超时、脚本错误、结果不可信 → 进入「限流不可用」状态（可用性指标 + 降级状态暴露）。
3. **资金写入处置（PD-1，不可变更）**：
   - 新提款创建（`POST /withdrawals`）**拒绝**，返回明确可重试错误（429/503 + `Retry-After`，错误分类沿用 007 taxonomy）；0 次无限制放行。
   - 充值观察、确认、重组恢复、已由 PostgreSQL 接受的提款：继续遵守原有授权、幂等、状态、暂停、nonce、广播及对账门禁（不因 Redis 故障放宽或收紧）。
   - 查询：可绕过缓存回源 PostgreSQL。
   - MUST NOT 批准/引入资金写入的替代限流机制（PD-1）；如需受限替代须另行裁决。
4. **非关键功能**：可降级/暂禁；MUST NOT 阻塞关键路径、MUST NOT 伪装权威。
5. **恢复**：平滑重建（梯度放开 + 预热），MUST NOT 瞬间全放开；拒绝计数与降级状态可观测（FR-18/FR-23）。

## §4 RPC 有界控制（PD-1/FR-19）

1. 既有每进程有界控制（连接/请求超时、重试上限、并发上限、错误分类）为基线，不依赖 Redis；分布式 RPC 预算为叠加治理。
2. Redis 故障时：可维持基线有界的调用类以降级并发继续；仅靠分布式预算才能有界的调用类**安全暂停**并在恢复后续跑（不新建付款意图、不重执行链上动作）。
3. MUST NOT 借故障跳过错误分类、链身份校验、完整性检查或把不完整结果当完整（FR-19）。
4. 暂停/续跑可观测（状态指标 + 日志）。

## §5 锁与非权威纪律

若使用 Redis 锁（如缓存防击穿 singleflight 跨实例），仅允许用于效率：MUST NOT 作为资金状态、幂等、nonce、发送资格或对账的唯一依据；PG 约束与门禁始终是正确性边界。

## §6 错误与可观测口径

`cache_hits_total`/`cache_misses_total`/`cache_fallback_total`/`cache_epoch_rotations_total`；`ratelimit_denied_total{class}`/`ratelimit_unavailable`（gauge）/`ratelimit_recovery_total`；`rpc_budget_paused_total{class}`。标签不含凭据；日志沿用 `internal/logx` 脱敏。告警阈值待测（[../verification.md](../verification.md) §1）。
