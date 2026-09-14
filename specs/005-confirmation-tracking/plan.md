# Implementation Plan: 005-confirmation-tracking

**Branch**: `005-confirmation-tracking` | **Date**: 2026-09-14 | **Spec**: `specs/005-confirmation-tracking/spec.md`

**Input**: Feature specification from `/specs/005-confirmation-tracking/spec.md`
（含 2 条已采纳澄清 Q1/Q2：非法阈值必填拒绝；受控授权变更单有效版本）

## Summary

消费 004 已持久化的 Pending 充值观察，按 env 必填阈值 N 与本地 canonical 链头计算
`max(0, tip - block_number + 1)`（uint64 安全数学 research R1），达标行经复用 `indexer_lease`
协调行的同一短事务条件转换为 Confirmed（持锁后独立重读三暂停行、lease 归属、策略版本、
链头身份、候选 pending 与哈希一致，重算 ≥ N；data-model 提交协议）。
阈值策略以单表 `confirmation_policy_history` 为权威（有效 = max seq；首确认事务内原子 bootstrap；
切换为特权单行 INSERT，request_id 幂等，镜像 004 授权形态的缩小版）。
调度为无游标有序扫描（降阈值自动纳入，无跳过）；瞬态停止只等待，结构停止复用既有三暂停行，
不新增暂停表；可观测只增 `confirmation_*` 组。006 交接：暂停门禁、锁顺序、依据列三项契约落定，
不实现 006 算法。生产就绪 T000-P 保持 open。

## Technical Context

**Language/Version**: Go 1.26.5（`context` 全 I/O 边界，错误上浮；高度/阈值全程 `uint64`，无符号运算；无 float）

**Primary Dependencies**: pgx/v5（pool 沿用，`BIGINT` 映射 `uint64` 经应用层转换断言非负）,
goose v3（`000005_confirmation_tracking.sql` 新迁移）, testcontainers（集成测）, prometheus client（`confirmation_*` 组）,
go-ethereum（无新增使用——005 零 RPC，所有链视图来自库内 `chain_blocks`）

**Storage**: PostgreSQL 18（1 张新表 `confirmation_policy_history` + `deposit_observations` 加 6 列 + 1 partial 索引，
详见 `data-model.md`；复用 `indexer_lease` 协调行、`chain_blocks` 链视图、`deposit_observations` 观察行、
三暂停行；002/003/004 表结构除 `status` CHECK 拓宽外不动）

**Testing**: `go test ./...` + `go test -tags integration ./...`（真库 + Anvil 边界/并发/注入场景，race 覆盖并发项）

**Target Platform**: Linux 单部署单链；本地 Anvil（chain-id 31337）为权威验证链 + DB 播种

**Project Type**: 后端常驻同步服务（既有 `Coordinator` 下新增确认 serveLoop，与 header/log/deposit 三循环并存，
授权切换保持 loop 外特权操作）

**Performance Goals**: 追尾延迟 < poll 间隔量级（复用 1s）；单提交事务毫秒级（本地库）；默认候选批与 deposit 同量级；
无吞吐目标（正确性优先，Constitution I）

**Constraints**: 事务内零外部调用（005 本就零 RPC）；租约 ttl 15s/心跳 5s 复用；
退避沿用 INDEX 参数（200ms→30s 封顶抖动）；退出沿用 ShutdownTimeout（15s）；
不引入 Redis/Kafka/K8s，不新增 HTTP 端点，不设业务阈值上限，不设默认值

**Scale/Scope**: 单链、单确认流；Anvil 级块量；策略表行数极少（每次切换一行）；Pending 扫描经 partial 索引有序批量

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- I 金融正确优先：条件 UPDATE + 三版本持锁重裁决 + 行数核对 + 首次不可变 → 通过。
- II 幂等：PK 身份 + 条件写 + 重读收敛（胜者值保留），内存只做短路 → 通过。
- III PG 唯一真相：策略版本链 + 确认依据列全 durable；零 RPC；无 Redis/Kafka → 通过。
- IV 重组感知：引用块逐行重裁决 canonical + 哈希；分歧跳过/停止，不自动恢复；Confirmed 回退归 006 → 通过。
- V 显式状态机：Pending → Confirmed 单转换 + CHECK 显式集合；策略版本链显式生命周期；运行/等待/重试/停止经 `confirmation_state` 表达 → 通过。
- VI 事务边界：候选读取在 BEGIN 前，转换与依据同提交；bootstrap 与首确认同事务；切换单行 INSERT 即原子 → 通过。
- VII nonce：不涉及 → N/A。
- VIII 签名隔离：不涉及；授权切换执行者沿用既有 DB 操作员角色（与 004 特权路径同形，无新服务/端点/信任根）→ 通过，无例外。
- IX 失败路径一等：等待 vs 停止二分、漂移大声停、未知按失败重读、超时永不做确认 verdict、T000-P 诚实 open → 通过。
- X/XI 本地确定性测试 + 测不变量：D1–D5 全映射验证场景（quickstart），播种只布置前置条件；`-race` + 双真实连接覆盖并发 → 通过。
- XII 可观测：`confirmation_*` 指标 + 结构化日志 + 诊断 SQL，凭据脱敏、禁原始数据转储 → 通过。
- XIII 简单优先：复用 lease 行/退避参数/Coordinator 模式/004 授权形态；新增 1 表 + 6 列 + 1 partial 索引
  （复用论证见 research R2–R6），无新服务/队列/端点/RPC/暂停表 → 通过。
- XIV 小步规范驱动：本 plan 只覆盖确认跟踪 → 通过。
- Go/DB/RPC 工程标准：ctx 边界、有界超时、无 float、迁移版本化、零 RPC 故无 RPC 错误分类 → 通过。

*Post-design re-check：Phase 1 未引入新例外——单表策略（R3）比双表更简单（XIII 加强）；
无游标扫描（R4）删除一类不变量而非增加；无新暂停表（R5）删除两表；side-table 否决（R6）删除一套身份。
Complexity 仍为空表。T000-P open 与偶发失败未知是规格要求的诚实状态，非违反。以上结论维持。*

## Project Structure

### Documentation (this feature)

```text
specs/005-confirmation-tracking/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── observability.md # confirmation_* 指标/日志/诊断 SQL 契约（无新增业务 API）
├── checklists/
│   └── requirements.md  # specify/clarify 质量清单（16/16，Notes 已同步澄清后事实）
├── spec.md
└── review.md            # specify 审查 + clarify 进展记录
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
internal/
├── config/config.go        # 新增 TXHARBOR_CONFIRMATION_DEPTH 读取 + parseConfirmationDepth（require/invalid/ParseUint 正整数形态复用）；Summary 脱敏沿用
├── indexer/                # 新增：confirmserve 循环（ConfirmationScanner + ServeLoop）、确认提交事务 confirmcommit（复用 lease 协议常量）、授权切换 confirmauth（request_id 幂等的缩小版）；coordinator.go 扩展第四循环（RunTrio→四路，runStreams 已是切片实现）；serve.go 接线（header/log/deposit 行为不变）
├── metrics/metrics.go      # 新增 confirmation_* 指标组（gauges + counters，见 contracts）
migrations/000005_confirmation_tracking.sql  # 新建：策略历史表 + observations 加列 + CHECK 拓宽 + partial 索引 + 升级断言
```

**Structure Decision**: 单体单包增量（`internal/indexer` 内新增确认文件，行为内聚；
config/metrics 均为原位扩展，不搭新抽象层；eth 包零改动——005 无 RPC；
coordinator 扩展面限于循环注册）。

## 关键流程（状态转换总览）

```
启动 → CheckChainID 门禁 → 解析 N（必填正整数，非法即拒绝退出）→
读策略 max 行：无行→待首确认 bootstrap；有行且阈值≠N→漂移拒绝退出；相等→进入循环 →
Coordinator 取 lease（获胜/旁观）→ 胜出期间四 serveLoop 并存（单心跳保活）：
  每 tick 读可信 tip（无 tip→等待；indexer_pause 在→停止）→ 算 maxEligible →
  有序批量取 Pending 候选（partial 索引；降阈值自动纳入）→ 逐个评估：
    引用不可信→跳过计数（留 Pending）｜ 达标→短事务[确保 lease 行→FOR UPDATE 取协调锁→
    独立语句重读裁决（三暂停皆无 + lease 归属 + 策略(S,N) + tip(T,TH) + 候选 pending + 哈希一致 + 重算≥N）→
    条件 UPDATE + 行数核对]→commit；失配→回滚（stale 计数），重读再决策
  可重试错→退避｜漂移→大声停｜失权→停写
  授权切换（特权 SQL，DB 操作员）：request_id 定性→验旧 seq→验新值合法且不同→
  同 lease 锁下重验→单行 INSERT 新策略行→部署以新 N 重启 worker；旧 worker 在守卫处漂移停止
```

## 与 spec 的一致性声明

- Q1/Q2 逐条落实：Q1→FR-03 前半/R1（必填拒绝/无默认/无上限/表示与运算留 plan 已在本 plan 落定为 uint64）；
  Q2→FR-03 后半/R3/R7（单有效版本/重启同属变更/向前适用/Confirmed 永不改写/失败原子/机制留 plan 已落定）。
- FR-01–FR-12 全部映射到 data-model/research 对应节；验收场景 15 项 + SC-01–SC-10 全部映射到 quickstart
  D1–D5（见下节覆盖矩阵）。
- 未发现 spec 冲突；**本次 plan 核对未改变任何已确定的业务语义**（uint64 表示、单表策略、无游标、
  无新暂停表、side-table 否决属设计层落实，不在规格锁定范围内；`status` CHECK 拓宽、partial 索引、
  state 计数值均为规格已锁行为的落实），无需规格修订。如 tasks/实现阶段发现冲突，将显式报告。
- 门禁诚实声明：T000-P open；偶发本地测试失败原因未知（D2/D3 类测试须重复运行并报告）；
  验收命名区分"本地范围验收"与"生产接入就绪"。

## 需求与验收覆盖（FR/SC → 设计 → 验证）

| FR | 设计位置 | 验证（quickstart） |
|----|----------|---------------------|
| FR-01 公式含所在块/下界 0 | research R1；data-model §版本三分法 | D1 |
| FR-02 本地 canonical 链头/缺失停/滞后等/禁 RPC 高度 | data-model 提交协议步骤 3；R4/R5 | D1，D5 |
| FR-03 阈值必填拒绝/受控变更全语义 | research R1/R3/R7；data-model Table 2 + 切换协议 | D1，D4 |
| FR-04 高度+哈希归属/缺失不确认 | data-model Table 1 + 候选重裁决 | D1，D3 |
| FR-05 条件更新/时间+依据/首次不可变 | data-model Table 1 CHECK + 提交协议步骤 4 | D2 |
| FR-06 异常停止/滞后等待/暴露原因 | research R5；contracts state=3/skipped | D3，D5 |
| FR-07 持锁重读/失配拒提交/重读再决策 | data-model 提交协议 + 并发时序论证 | D3 |
| FR-08 三暂停门禁/版本条件/审计 | data-model 步骤 3 + Table 2/审计列 | D3，D4 |
| FR-09 Confirmed 语义/不重写/006 边界 | research R9；data-model I5 | D4 |
| FR-10 无余额/无新平台/单链边界 | 本 plan 结构（无相关模块/表/端点） | 评审门禁 |
| FR-11 进度/积压/滞后/暂停可观察 + 脱敏 | research R8；contracts/observability.md | D5 + SC-09 |
| FR-12 不锁实现/006 交接清单/不批新恢复策略 | research R9；data-model 006 预留 | 评审门禁 |

| SC | 验证 |
|----|------|
| SC-01 N-1 保持 / N、N+1 转换 | D1 |
| SC-02 N=1 所在块计 1 | D1 |
| SC-03 非 canonical/失配/缺失/异常零提交 | D3 |
| SC-04 追赶后不遗漏 | D5 |
| SC-05 重复检查恒 1 转换 | D2 |
| SC-06 并发恰好一方成功 | D2 |
| SC-07 读后变化零旧提交 | D3 |
| SC-08 已确认不重写/崩溃恢复 | D2 |
| SC-09 可追溯 + 零敏感泄露 | D5 |
| SC-10 切换重判/Confirmed 不变/漂移拒绝/失败原子 | D4 |

## 006 交接契约（本 plan 落定，006 消费）

- 暂停门禁：确认提交要求 `deposit_pause`/`indexer_pause`/`log_pause` 皆无 → 006 以暂停行 halt 确认，无需新表。
- 锁顺序：一切写事务首锁 `indexer_lease` 行 → 006 恢复事务遵守同一顺序即无死锁环。
- 审计输入：观察行依据六列 + 策略版本链 → 006 重验/Orphaned 判定/再确认的输入；005 不写恢复行。
- 006 自有迁移拓宽 `status` 加入 `'orphaned'`；不得修改 005 列语义。

## Complexity Tracking

> 无 Constitution 违反，无需豁免。最重的设计件（单表策略 vs 双表/复用 004 历史）的取舍见 `research.md` R3；
> 无游标扫描（R4）、无新暂停表（R5）、side-table 否决（R6）均为删减而非增加复杂度。
