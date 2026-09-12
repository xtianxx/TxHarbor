# Implementation Plan: 002-chain-indexer

**Branch**: `002-chain-indexer` | **Date**: 2026-09-12 | **Spec**: `specs/002-chain-indexer/spec.md`

**Input**: Feature specification from `/specs/002-chain-indexer/spec.md`（含 5 条已采纳澄清）

## Summary

连续同步单链区块头：从配置起始高度（或 checkpoint 下一块）按序取头，
逐高度短事务原子落块+推进 checkpoint；lease 租约+fencing 实现多实例单主，
三态核验与持久化暂停覆盖分叉可疑；可观测经指标/日志/暂停行，不碰 readyz 语义。

## Technical Context

**Language/Version**: Go 1.26.5（`context` 全 I/O 边界，错误上浮，金融量无浮点——本阶段无金额）

**Primary Dependencies**: go-ethereum v1.17.5（`HeaderByNumber` 新增封装）, pgx/v5（pool 沿用 8/1/1h/30m/1m）,
goose v3（`000002_chain_indexer.sql` 新迁移）, testcontainers（集成测）

**Storage**: PostgreSQL 18（4 张新表：`chain_blocks` / `indexer_checkpoint` / `indexer_lease` /
`indexer_pause`，详见 `data-model.md`）

**Testing**: `go test ./...` + `go test -tags integration ./...`（真库+Anvil/假RPC，race 覆盖并发项）

**Target Platform**: Linux 单部署单链；本地 Anvil（chain-id 31337）为权威验证链

**Project Type**: 后端常驻同步服务（既有 `app.Serve` 生命周期内新增 scanner 协程）

**Performance Goals**: 追尾延迟 < poll 间隔量级（默认 1s）；单高度事务毫秒级（本地库），无吞吐目标
（正确性优先，Constitution I）

**Constraints**: 事务内零 RPC；租约 ttl 15s/心跳 5s；退避 200ms→30s 封顶±25% 抖动；
退出沿用 ShutdownTimeout（15s）；不引入 Redis/Kafka/K8s，不新增 HTTP 端点

**Scale/Scope**: 单链、逐块同步；Anvil 级块量；暂停行单链单行

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- I 金融正确优先：逐高度原子事务 + 外键 + PK + 单调守卫 → 通过。
- II 幂等：`DO NOTHING` + 约束收敛，内存只做短路 → 通过。
- III PG 唯一真相：lease/ checkpoint/ pause 全 durable；无 Redis/Kafka → 通过。
- IV 重组感知：存 hash/parent_hash、断裂即持久化暂停、不自动恢复 → 通过。
- V 显式状态机：运行/等待/重试/暂停四态（`indexer_state` 指标即状态载体，转换条件见下）→ 通过。
- VI 事务边界：取块在 BEGIN 前，游标与数据同提交 → 通过。
- VII nonce：不涉及（无提款）→ N/A。
- VIII 签名隔离：不涉及 → N/A。
- IX 失败路径一等：三态核验、分类重试（无限尾随有封顶退避+可中断+退出分类，见 R5）、
  不确定提交以 DB 为准 → 通过。
- X/XI 本地确定性测试 + 测不变量：13 场景全映射集成测，mock 仅用于 RPC 故障形变 → 通过。
- XII 可观测：指标+结构化日志+暂停行，凭据脱敏（含 Summary 修复）→ 通过。
- XIII 简单优先：4 表中 lease 为唯一新增协调件（R2 已否决更重的锁方案；约束收敛保留为防线，
  非重复建设）；无新服务/队列/端点 → 通过。
- XIV 小步规范驱动：本 plan 只覆盖头索引 → 通过。
- Go/DB/RPC 工程标准：ctx 边界、有界超时、错误分类不坍缩、迁移版本化 → 通过。

*Post-design re-check：设计未引入新例外，以上结论维持。*

## Project Structure

### Documentation (this feature)

```text
specs/002-chain-indexer/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── observability.md # 指标/日志/暂停行契约（无新增业务 API）
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
internal/
├── config/config.go        # 新增 START_HEIGHT/INDEX_* 变量+校验；Summary 脱敏 RPCURL
├── eth/client.go           # 新增 HeaderByNumber 封装 + KindNotFound/KindRateLimited
├── indexer/                # 新增：scanner 循环、lease 心跳、verify、store（短事务）
├── health/*                # 只读复用（语义冻结）
├── metrics/metrics.go      # 新增 4 指标
└── app/serve.go            # 接线 scanner 启停（沿用 runCtx/ShutdownTimeout）
migrations/000002_chain_indexer.sql  # 新增 4 表
```

**Structure Decision**: 单体单包增量（`internal/indexer` 为唯一新包，行为内聚；eth/config/metrics
均为原位扩展，不搭新抽象层）。

## 关键流程（状态转换总览）

```
启动 → CheckChainID 门禁 → 取 lease（获胜/旁观）→ checkpoint 核验（三态）→ 循环：
  取头(RPC, 超时) → 高度/父哈希预检 → 短事务(INSERT块 + 同高度哈希重读比对 + 精确守卫推进
  checkpoint：height=$n-1 AND block_hash=$parent AND 无暂停 AND fencing 通过) → commit
  追头/空结果 → state=1 等待轮询 ｜ 可重试错 → state=2 退避 ｜ 哈希分歧 → 持久化暂停 state=3
  失权/不可重试错/shutdown → 停写（续约0行即停；暂停行存在即停）
```

## 与 spec 的一致性声明

- 5 条澄清逐条落实：Q1→FR-10/R3 等待零写入；Q2→FR-11 三态 + FR-09 不可重试停止；
  Q3→FR-12 持久化暂停 + 重启有效 + 无管理接口；Q4→FR-04 canonical 定义 + 恒 TRUE；
  Q5→FR-05 首块边界 + 后续 `checkpoint+1` 规则 + FR-06 首块同事务。
- 未发现 spec 冲突；无静默修改。如 tasks/实现阶段发现冲突，将显式报告。

## Complexity Tracking

> 无 Constitution 违反，无需豁免。最重的设计件（lease 租约 vs 纯约束收敛）的取舍见
> `research.md` R2；4 表选择的最小性论证见 `data-model.md`（无投机索引、无未来列）。
