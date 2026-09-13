# Implementation Plan: 003-event-indexing

**Branch**: `003-event-indexing` | **Date**: 2026-09-13 | **Spec**: `specs/003-event-indexing/spec.md`

**Input**: Feature specification from `/specs/003-event-indexing/spec.md`（含 3 条已采纳澄清：小写归一化 + 版本化 `config_hash`；独立日志进度/暂停并服从链级暂停；截断信号分类与缩批语义）

## Summary

按连续闭区间查询白名单 ERC-20 的 Transfer 原始日志：区间上界以 002 已持久化连续范围为限，
`FilterLogs` 取数后逐条 8 项严格校验，整区间日志写入与 `next_block` 推进在同一短事务内原子提交；
复用 `indexer_lease` 协调行串行化所有写事务，持锁后独立语句重裁决双暂停行、lease 归属与链视图覆盖，
使旧 worker 与配置变更天然被拒；`KindIncomplete` 区分缩批路径，有界退避覆盖其余瞬时错误；
进度/滞后/暂停经 `log_*` 指标、结构化日志与 SQL 诊断暴露；生产 provider 未定，E1 保持 open，
计数达上限判定默认禁用。

## Technical Context

**Language/Version**: Go 1.26.5（`context` 全 I/O 边界，错误上浮；本阶段金额仅为 32 字节原始 `data`，不解析）

**Primary Dependencies**: go-ethereum v1.17.5（`FilterLogs` + `FilterQuery` 新增封装，`KindIncomplete` 分类扩展）,
pgx/v5（pool 沿用）, goose v3（`000003_event_indexing.sql` 新迁移）, testcontainers（集成测）

**Storage**: PostgreSQL 18（3 张新表：`erc20_transfer_logs` / `log_checkpoint` / `log_pause`，详见 `data-model.md`；
复用 `indexer_lease` 协调行与 `chain_blocks` 链视图真相；002 四表不动）

**Testing**: `go test ./...` + `go test -tags integration ./...`（真库 + Anvil/假 RPC，race 覆盖并发项）

**Target Platform**: Linux 单部署单链；本地 Anvil（chain-id 31337）为权威验证链

**Project Type**: 后端常驻同步服务（既有 `app.Serve` 生命周期内新增 log-scanner 协程，与 header scanner 并存）

**Performance Goals**: 追尾延迟 < poll 间隔量级（默认 1s 复用）；单区间事务毫秒级（本地库）；默认批 500 块；
无吞吐目标（正确性优先，Constitution I）

**Constraints**: 事务内零 RPC；租约 ttl 15s/心跳 5s 复用；退避 200ms→30s 封顶 ±25% 抖动复用；
退出沿用 ShutdownTimeout（15s）；不引入 Redis/Kafka/K8s，不新增 HTTP 端点

**Scale/Scope**: 单链、单日志流；Anvil 级块量；暂停行单链单行；白名单规模按常规 ERC-20 部署（地址数十量级，逐条校验无压力）

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- I 金融正确优先：区间原子事务 + PK/UNIQUE + 精确守卫 + 冲突比对 → 通过。
- II 幂等：身份 PK + `DO NOTHING` + 内容比对收敛，内存只做短路 → 通过。
- III PG 唯一真相：日志/进度/暂停全 durable；无 Redis/Kafka → 通过。
- IV 重组感知：引用块逐块重裁决、分歧即持久化信号、不自动恢复 → 通过。
- V 显式状态机：运行/等待/重试/暂停四态（`log_state` 即载体）→ 通过。
- VI 事务边界：取数在 BEGIN 前，日志与游标同提交 → 通过。
- VII nonce：不涉及 → N/A。
- VIII 签名隔离：不涉及 → N/A。
- IX 失败路径一等：分类恢复（退避 vs 缩批 vs 暂停）、未知按失败、E1 诚实 open → 通过。
- X/XI 本地确定性测试 + 测不变量：11 场景全映射集成测，mock 仅用于 RPC 故障形变 → 通过。
- XII 可观测：`log_*` 指标 + 结构化日志 + 暂停行，凭据脱敏、禁原始响应转储 → 通过。
- XIII 简单优先：复用 lease 行/退避参数/readyz 语义；新增仅 3 表 + 3 配置项（1 复用论证见 R6），无新服务/队列/端点 → 通过。
- XIV 小步规范驱动：本 plan 只覆盖日志索引 → 通过。
- Go/DB/RPC 工程标准：ctx 边界、有界超时、错误分类不坍缩（新增 kind 正交）、迁移版本化 → 通过。

*Post-design re-check：设计未引入新例外；E1 open 是规格要求的诚实状态，非违反。以上结论维持。*

## Project Structure

### Documentation (this feature)

```text
specs/003-event-indexing/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── observability.md # log_* 指标/日志/诊断 SQL 契约（无新增业务 API）
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
internal/
├── config/config.go        # 新增 LOG_START_HEIGHT/LOG_CONTRACTS/LOG_BATCH_BLOCKS + 校验；Summary 脱敏沿用
├── eth/client.go           # 新增 FilterLogs 封装 + KindIncomplete 分类
├── indexer/                # 新增：logscanner 循环、whitelist/config_hash、validate、commit（短事务，复用 lease 协议）
├── metrics/metrics.go      # 新增 5 个 log_* 指标
└── app/serve.go            # 接线 log-scanner 启停：与 header scanner 共享同一 Lease 句柄 + 同一心跳（沿用 runCtx/ShutdownTimeout，两流并存）
migrations/000003_event_indexing.sql  # 新增 3 表
```

**Structure Decision**: 单体单包增量（`internal/indexer` 内新增日志扫描文件，行为内聚；
eth/config/metrics 均为原位扩展，不搭新抽象层）。

## 关键流程（状态转换总览）

```
启动 → CheckChainID 门禁 → 解析白名单/计算 config_hash → 取 lease（获胜/旁观）→
配置比较（行存在且 start/config 任一不同即拒绝退出）→ 循环：
  定上界(min(请求末端, 002 checkpoint) + 连续 canonical 验证) →
  FilterLogs(RPC, 超时) → 逐条 8 项校验 → 复核末端块身份 →
  短事务[确保 lease 行 → FOR UPDATE 取协调锁 →
  独立语句重读裁决(双暂停行皆无 + owner/token/有效期 + 精确守卫 next=a 且 start/config 一致 +
  [a,b] 逐块 canonical 重裁决) →
  INSERT 日志 + 冲突内容比对 + 行数核对 + 推进 next=b+1] → commit
  上界未覆盖 → state=1 等待 ｜ 可重试错 → state=2 退避 ｜ incomplete → 缩批自 a 重查 ｜
  单块仍不可确认 → range_incomplete 暂停 ｜ 确定性校验失败（含身份冲突）→ validation_failed 暂停（detail.class 分类）｜
  链视图变 → chain_view_changed 暂停 ｜ 失权/配置拒绝 → 停写停服
```

## 与 spec 的一致性声明

- 3 条澄清逐条落实：A1→FR-05/R3（编码、比较、原子初始化）；A2→FR-16/R1（独立行 + 双暂停裁决 + 服从链级暂停）；
  A3→FR-12/R4（`KindIncomplete`、丢弃缩批、单块停推、未知按失败）。
- FR-01–FR-19 全部映射到 data-model/research 对应节；11 验收场景全部映射到 quickstart 验证表（见下节覆盖矩阵）。
- 未发现 spec 冲突；**本次 plan 核对未改变任何已确定的业务语义**（暂停 kind 由 `log_conflict` 并入 `validation_failed` + `detail.class` 属设计层命名，不在规格锁定范围内；`log_checkpoint` 无外键、lease 共享句柄、暂停原子条件均为规格已锁行为的落实），无需规格修订。如 tasks/实现阶段发现冲突，将显式报告。
- E1 按规格要求保持 open：它是实现及生产接入的门禁，不阻塞任务拆解；计数达上限判定默认禁用（禁用 ≠ 已解决，残余风险如实声明）；不虚构 provider 映射，不将生产 provider 标为可用。

## 需求与验收覆盖（FR/SC → 设计 → 验证）

| FR | 设计位置 | 验证（quickstart #） |
|----|----------|----------------------|
| FR-01 独立进度 | data-model Table 2；research R1 | #9（独立起点补扫） |
| FR-02 起点补扫/next 语义 | data-model Table 2；R2 | #1，#9 |
| FR-03 连续闭区间 ≤002 范围 | data-model 写事务协议步骤 1/4 | #1，#7 |
| FR-04 白名单 + 空白拒绝 | research R3/R6；data-model Table 1 | #10 |
| FR-05 单流 + 配置身份 | research R3；data-model Table 2 | #10 |
| FR-06 调参不改语义 | research R6 | #4（改批重试语义不变） |
| FR-07 逐条 8 项校验 | research R4；data-model Table 1（见下附校验清单） | #6 |
| FR-08 原始保留、无投影 | data-model Table 1（无业务列） | #1 |
| FR-09 顺序无关/冲突整批败 | data-model 协议步骤 5；失败分类表 | #2，#6 |
| FR-10 去重身份 | data-model Table 1（PK + 块内 UNIQUE） | #2 |
| FR-11 区间原子提交 | data-model 写事务协议 | #3 |
| FR-12 不完整≠空结果/缩批/单块停推 | research R4/R5；失败分类表 | #4，#5 |
| FR-13 有界退避 + 信任边界 | research R4/R5；E1 附录 | #4 |
| FR-14 链视图复核 + 暂停 | data-model 协议步骤 1/4/暂停事务 | #7 |
| FR-15 不回退/不删历史 | research R1（002 表不动）；Table 3 | #7，#11 |
| FR-16 并发单次有效 + 旧 worker 隔离 | research R2；data-model 协议步骤 3–5 | #8 |
| FR-17 可观察 + 脱敏 | research R7；contracts/observability.md | #3–#8 状态断言 + SC-11 |
| FR-18 范围外不做 | 本 plan 结构（无相关模块/表） | 评审门禁 |
| FR-19 规格不限实现 | 本 plan 即锁定处 | — |

逐条校验清单（FR-07，事务外执行，任一失败整批丢弃）：①合约地址属本次白名单（小写比较）；
②高度 ∈ `[a, b]`；③ `block_hash` == `chain_blocks` 同高度 canonical 哈希；④区块/交易/索引身份字段完整合法；
⑤ `removed == false`；⑥地址 20 字节、哈希 32 字节；⑦ `len(topics)==3` 且各 32 字节、topic0 为 Transfer 签名、
地址 topic 高 12 字节为零；⑧ `len(data)==32` 字节。

| SC | 验证 |
|----|------|
| SC-01 连续扫描完整 | quickstart #1（Anvil 混合区间） |
| SC-02 重复乱序收敛 | #2 |
| SC-03 崩溃恢复 | #3 |
| SC-04 RPC 故障/单块停推 | #4，#5 |
| SC-05 非法日志整批败 | #6 |
| SC-06 链视图暂停 | #7 |
| SC-07 并发单次推进 | #8（`-race` 重复运行） |
| SC-08 独立起点补齐 | #9 |
| SC-09 配置拒绝零破坏 | #10 |
| SC-10 迁移约束与保留 | #11 |
| SC-11 状态可查 + 零凭据泄露 | contracts 指标/SQL + 全量日志脱敏断言 |

## Complexity Tracking

> 无 Constitution 违反，无需豁免。最重的设计件（复用 lease 行串行化两类写事务 vs 日志自建锁）的取舍见
> `research.md` R1；3 表选择的最小性论证见 `data-model.md`（二级索引仅 `(chain_id, block_number)` 一处，
> 由 004 消费路径直接需要，非投机）。
