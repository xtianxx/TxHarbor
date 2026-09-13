# Implementation Plan: 004-deposit-detection

**Branch**: `004-deposit-detection` | **Date**: 2026-09-13 | **Spec**: `specs/004-deposit-detection/spec.md`

**Input**: Feature specification from `/specs/004-deposit-detection/spec.md`（含 6 条已采纳澄清 Q1–Q6：
暂时 vs 结构性缺口；分层恢复；授权跃迁原则、生效时机、回放位置、收缩语义）

## Summary

消费 003 已持久化的 canonical Transfer 日志，按块粒度连续单元识别转入受监控地址的充值：
env 声明资产/地址集合与生效高度并编码版本化配置身份；上游覆盖以 003 `log_checkpoint.next_block`
单调水位证明（`N_u > b`），暂时缺口等待、结构缺口报错停止；观察写入与 `next_block` 推进在复用
`indexer_lease` 协调行的同一短事务内原子提交，持锁后重裁决三暂停行、lease 归属与链视图；
004 零 RPC 调用；暂停按归属分层恢复（上游 auto-resume、自身 manual-release + 重验门，
需回退的一律保持暂停待 006）；有意配置变更走授权转换事务（身份、回放位置、暂停处置、审计同事务原子提交，
历史版本链存 `deposit_config_history`，回放按受影响范围最小值规则，收缩仅向前生效）；
进度/滞后/暂停经 `deposit_*` 指标、结构化日志与 SQL 诊断暴露；
生产就绪依赖上游 003 E1，T000-P 保持 open。

## Technical Context

**Language/Version**: Go 1.26.5（`context` 全 I/O 边界，错误上浮；金额全程 `big.Int`，无 float 通路）

**Primary Dependencies**: go-ethereum v1.17.5（仅 `Keccak256` 断言 Transfer 签名与地址类型，无 RPC 调用）,
pgx/v5（pool 沿用，`NUMERIC` 十进制映射）, goose v3（`000004_deposit_detection.sql` 新迁移）,
testcontainers（集成测）, prometheus client（`deposit_*` 指标组）

**Storage**: PostgreSQL 18（5 张新表：`deposit_observations` / `deposit_checkpoint` / `deposit_pause` / `deposit_config_history`（版本链 + 授权审计）/ `deposit_pause_audit`（暂停实例事件审计），
详见 `data-model.md`；复用 `indexer_lease` 协调行、`chain_blocks` 链视图真相、
`erc20_transfer_logs` 日志真相；002/003 七表不动）

**Testing**: `go test ./...` + `go test -tags integration ./...`
（真库 + Anvil 全栈 happy-path + DB 播种故障/并发场景，race 覆盖并发项）

**Target Platform**: Linux 单部署单链；本地 Anvil（chain-id 31337）为权威验证链 + DB 播种

**Project Type**: 后端常驻同步服务（既有 `Coordinator` 下新增 deposit serveLoop，与 header/log 双循环并存）

**Performance Goals**: 追尾延迟 < poll 间隔量级（复用 1s）；单单元事务毫秒级（本地库）；默认批 500 块；
无吞吐目标（正确性优先，Constitution I）

**Constraints**: 事务内零外部调用（004 本就零 RPC）；租约 ttl 15s/心跳 5s 复用；
退避沿用 INDEX 参数（200ms→30s 封顶 ±25% 抖动的 `newBackoff`）；退出沿用 ShutdownTimeout（15s）；
不引入 Redis/Kafka/K8s，不新增 HTTP 端点

**Scale/Scope**: 单链、单充值流；Anvil 级块量；暂停行单链单行；资产与监控地址数十量级（内存集合无压力）

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- I 金融正确优先：单元原子事务 + PK/UNIQUE + 精确守卫 + 冲突比对 + 覆盖证明 → 通过。
- II 幂等：来源身份 PK + `DO NOTHING` + 内容比对收敛，内存只做短路 → 通过。
- III PG 唯一真相：观察/进度/暂停全 durable；零 RPC 使"真相"完全在库内；无 Redis/Kafka → 通过。
- IV 重组感知：引用块逐块重裁决、分歧即持久化信号、不自动恢复、需回退时保持暂停待 006 → 通过。
- V 显式状态机：Pending 单值 CHECK 显式初始态；运行/等待/重试/暂停/结构停止经 `deposit_state` 表达 → 通过。
- VI 事务边界：上游读取在 BEGIN 前，观察与游标同提交 → 通过。
- VII nonce：不涉及 → N/A。
- VIII 签名隔离：不涉及 → N/A。
- IX 失败路径一等：缺口二分（等待 vs 报错停止）、分层恢复、未知按失败、超时永不做结构 verdict、
  T000-P/E1 与偶发失败残余风险诚实 open → 通过。
- X/XI 本地确定性测试 + 测不变量：D1–D10 全映射集成测，播种只布置前置条件；
  `-race` + 双真实连接覆盖并发 → 通过。
- XII 可观测：`deposit_*` 指标 + 结构化日志 + 暂停行，凭据脱敏、禁原始数据转储、金额十进制 → 通过。
- XIII 简单优先：复用 lease 行/退避参数/readyz 语义/Coordinator 模式；新增 5 表 + 4 配置项
  （复用论证见 R1/R6/R7/R8），无新服务/队列/端点/RPC → 通过。
- XIV 小步规范驱动：本 plan 只覆盖充值识别 → 通过。
- Go/DB/RPC 工程标准：ctx 边界、有界超时、无 float 金额、迁移版本化、零 RPC 故无 RPC 错误分类 → 通过。

*Post-design re-check（Q3–Q6 同步后）：新增特权授权事务与 history 表不引入新例外——执行者沿用既有 DB 操作员角色
（无新服务/端点/信任根，VIII 不适用），审计与身份更新同事务持久化（III/XII），程序显式分层（V），
重验门与互斥完整（VI/IX）；多次切换无需新增业务限制（R11 已论证），故 Complexity 仍为空表。
T000-P open 与偶发失败未知是规格要求的诚实状态，非违反。以上结论维持。*

## Project Structure

### Documentation (this feature)

```text
specs/004-deposit-detection/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── observability.md # deposit_* 指标/日志/诊断 SQL 契约（无新增业务 API）
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
internal/
├── config/config.go        # 新增 DEPOSIT_START_HEIGHT/DEPOSIT_CONTRACTS/DEPOSIT_WATCH_ADDRESSES/DEPOSIT_BATCH_BLOCKS + 校验；Summary 脱敏沿用
├── indexer/                # 新增：depositserve 循环、deposit config_hash、transfer 解析匹配、deposit commit（短事务，复用 lease 协议）、授权转换事务 + history 写入（落入 depositcommit 或其拆分文件，按 tasks C1 落点规则固定）；coordinator.go 机械注册第三循环；serve.go 接线（header/log 行为不变）
├── metrics/metrics.go      # 新增 6 个 deposit_* 指标（含 transition 审计计数）
migrations/000004_deposit_detection.sql  # 新增 5 表（含 sequence 与审计表）
```

**Structure Decision**: 单体单包增量（`internal/indexer` 内新增充值识别文件，行为内聚；
config/metrics 均为原位扩展，不搭新抽象层；eth 包零改动——004 无 RPC）。

## 关键流程（状态转换总览）

```
启动 → CheckChainID 门禁 → 解析资产/地址/生效高度/计算 config_hash →
重算 003 身份对齐校验（env 白名单 → H' == H_u，否则拒绝退出）→
Coordinator 取 lease（获胜/旁观）→ 配置比较（行存在且 start/config 任一不同即拒绝退出）→
胜出期间并发运行三 serveLoop（单心跳保活）：
  定位置 a（deposit next 或起点）→ 读上游 (S_u,H_u,N_u) → 缺口分类：
    结构缺口 → 报错停止 + upstream_gap(structural) 暂停行 ｜
    暂时缺口 → state=1/4 等待 ｜
    覆盖完整 → 读 [a,b] 行（ORDER BY 高度,log_index）→ 逐条解析匹配 →
  短事务[确保 lease 行 → FOR UPDATE 取协调锁 →
  独立语句重读裁决(三暂停行皆无 + owner/token/有效期 + 精确守卫 next=a 且 start/config 一致 +
  log_checkpoint next>b 重证明 + [a,b] 逐块 canonical 重裁决) →
  INSERT 观察 + 冲突内容比对 + 行数核对 + 推进 next=b+1] → commit
  可重试错 → state=2 退避 ｜ 确定性解析失败（含身份冲突）→ validation_failed 暂停 ｜
  链视图变 → chain_view_changed 暂停 ｜ 失权/配置拒绝 → 停写停服 ｜
  授权转换（特权 SQL，DB 操作员）：按 request_id 查 history 定性（已记录请求的同参返原结果/异参拒绝/独立校验，未记录失败不绑定 ID；
  损坏态下已记录结果仍只读返回，新执行拒绝）→
  解析新配置 → 算 H′/replay_from → 缺口分类 → 同 lease 锁下重验（含完整性：两侧同有＋行内与最新 history
  关联一致；预期 version_seq 一致、身份仍旧 + 覆盖重证明 + canonical + 暂停处置条件；
  调用方 expected_pause 非空必须与锁内行一致，否则按目标替换拒绝；
   双空表示不授权处置任何已有暂停：证据在本 scope 内已证解决（必须处置）则缺目标拒绝，
   调用方读取实例后重新明确授权（2026-09-13 批准修订：拒绝无记录不绑定 ID，同 ID 补目标按独立候选完整重验）；证据在 scope 外（可保留）则提交身份/位置/history、
  暂停原样保留、消费仍停；锁内暂停状态与依据不一致→回滚报告状态变化，不扩大授权）→
  原子提交[history 行（新 seq + request_id + 审计） + 身份 H→H′ + next→replay_from + 暂停处置
  （匹配目标条件 DELETE，或原样保留；禁 UPDATE／合并）] → 按新身份继续；
  任一失败全回滚；回放未完成时可再次授权（重算收敛，无需新业务限制）｜
  消费提交核对捕获版本仍等于当前 seq（失配放弃重读，回环亦然）｜
  漂移改回：改回 env 与旧配置一致后重启，启动比较通过即恢复（非授权路径）
  上游暂停 → 服从等待（自动续）｜ 自身暂停 → 按实例条件解除（实例 + 修订匹配）+ 同事务审计 + 重验门 ｜ 需回退 → 保持暂停待 006
```

## 与 spec 的一致性声明

- 2 条首轮澄清逐条落实：Q1→FR-07/R5（暂时等待/结构报错/禁超时判定/重验恢复）；
  Q2→FR-12/R6（上游 auto-resume、自身 manual-release + 重验门、006 边界保持暂停）。
- Q3–Q6（授权跃迁/生效时机/回放位置/收缩语义）落实：FR-06/R11（授权事务 + 审计 + 双解除路径）、
  FR-07/R5（最小值回放规则 + 上游起点仅检查 + 混合双规则）、data-model Table 3–5 + 授权/回放协议、
  请求身份（意图参数比对、request_id 持久化）与暂停实例身份（不可复用 pause_id + 修订 + 同事务审计）已同步 research R11、contracts 审计查询、quickstart D8/D11、tasks（T001/T006/T015/T019/T025/T027/T028/Notes）。
  contracts 审计字段与 transition 计数、quickstart D5/D7/D11；tasks 首轮同步已完成（首轮当时E2保持开放待复核，保留；2026-09-13最终：E2经定向修正与静态复验闭合，见spec 244-245。）
  请求幂等与版本隔离强化轮：request_id 身份（已记录请求的同参返原/异参拒绝/异 ID 独立；未记录失败不绑定）与 seq 隔离
  （消费捕获+提交核验、授权验预期 seq、暂停锁内重估）已同步 research R11、data-model 协议、
  contracts 审计查询、quickstart D11、tasks（T001/T006/T019/T025/T027/T028/Notes）。
  目标绑定与解除结果判定轮：expected_pause 双空或双非空（非通配）、锁内目标匹配、解除查审计定性
  已同步 data-model Table 3–5、research R6/R11、contracts 审计查询、quickstart D8、tasks（T001/T015/T019/T025/T027/Notes）。
  保留分支与完整性前检轮：expected_pause 双空保留执行分支（必须处置定义、锁内一致不扩大授权、
  保留后消费仍停、保留暂停走实例人工解除无死路）、授权路径 DELETE-only（累积合并专属暂停写事务 T029/Q8，
  授权事务禁用）、
  消费提交与授权裁决的完整性前检（损坏态定义、bootstrap 与已记录结果只读返回保留）
  已同步 data-model 授权/写事务协议、research R11、plan 关键流程、tasks（T015/T025/T027/Notes）、
  quickstart D8/D11。
  请求幂等与版本隔离强化轮：request_id 身份（已记录请求的同参返原/异参拒绝/异 ID 独立；未记录失败不绑定）与 seq 隔离
  （消费捕获+提交核验、授权验预期 seq、暂停锁内重估）已同步 research R11、data-model 协议、
  contracts 审计查询、quickstart D11、tasks（T001/T006/T019/T025/T027/T028/Notes）。
- FR-01–FR-16 全部映射到 data-model/research 对应节；验收矩阵 10 项 + D11 全部映射到 quickstart 验证表
  （见下节覆盖矩阵）。
- 未发现 spec 冲突；**本次 plan 核对未改变任何已确定的业务语义**（`status` 单值 CHECK、
  二级索引取舍、state=4 的等待/停止区分属设计层落实，不在规格锁定范围内；`deposit_checkpoint`
  无外键、暂停原子条件均为规格已锁行为的落实），无需规格修订。如 tasks/实现阶段发现冲突，将显式报告。
- 门禁诚实声明：T000-P open（生产就绪依赖上游 003 E1）；偶发本地测试失败原因未知（D9 类测试须重复运行并报告）；
  R10 待核验项是实现前置条件。验收命名区分"本地范围验收"与"生产接入就绪"。

## 需求与验收覆盖（FR/SC → 设计 → 验证）

| FR | 设计位置 | 验证（quickstart） |
|----|----------|---------------------|
| FR-01 仅白名单+监控/恰好一条/Pending | data-model Table 1；research R4 | D1，D2 |
| FR-02 解析 sender/recipient/amount 无浮点 | research R4；data-model Table 1 | D1 |
| FR-03 零值保留无观察 | data-model Table 1（CHECK>0）；R4 | D2 |
| FR-04 规范化精确匹配 + 空白拒绝 | research R3/R7 | D1，D5（空名单） |
| FR-05 三元组生效高度闭区间 | research R3；data-model §编码 | D1，D5 |
| FR-06 配置身份/变更拒绝/授权跃迁/收缩向前 | research R3/R11；data-model Table 2/4 + 授权协议 | D5，D11 |
| FR-07 缺口二分/禁超时/回放最小值/混合双规则 | research R5/R11；data-model 回放规则 + 失败分类 | D6，D7，D11 |
| FR-08 独立进度/next 语义 | data-model Table 2；research R1 | D1，D9 |
| FR-09 原子提交/幂等/冲突整批败/授权原子 | data-model 写事务协议步骤 4–5 + 授权协议 | D3，D4，D9，D11 |
| FR-10 空区间推进/去重收敛/异常整批败 | data-model 协议步骤 5；失败分类 | D2，D3 |
| FR-11 来源+区块身份/canonical 绑定 | data-model Table 1 + 身份关联节 | D1，D8 |
| FR-12 越位禁推/暂停/分层恢复/不回退 | research R6/R11；data-model 协议步骤 4 + Table 3–5 | D8，D11 |
| FR-13 无余额变更 | 本 plan 结构（无余额模块/列） | 评审门禁 |
| FR-14 005/006/基建范围外不做 | 本 plan 结构（无相关模块/表/端点） | 评审门禁 |
| FR-15 可观察 + 脱敏 + 转换审计 | research R8/R11；contracts/observability.md | D1–D9 状态断言 + D11 + SC-09 |
| FR-16 规格不限实现 | 本 plan 即锁定处 | — |

| SC | 验证 |
|----|------|
| SC-01 匹配恰好一条/非匹配零生成/连续推进 | D1，D2 |
| SC-02 重复收敛 | D3 |
| SC-03 崩溃恢复 | D3 |
| SC-04 故障零推进/断点继续 | D3，D4 |
| SC-05 非法整批败 | D2（invalid 注入），D8 |
| SC-06 不越位/可诊断暂停与恢复/006 保持 | D6，D8 |
| SC-07 配置拒绝零破坏 | D5 |
| SC-08 缺口等待与结构停止/重验恢复 | D6，D7 |
| SC-09 状态可查 + 零敏感泄露 | contracts 指标/SQL + 全量日志脱敏断言 |

## Complexity Tracking

> 无 Constitution 违反，无需豁免。最重的设计件（复用 lease 行串行化三类写事务 vs 充值自建锁）
> 的取舍见 `research.md` R1；5 表 + 2 二级索引选择的最小性论证见 `data-model.md`
> （第 5 表为暂停审计所必需：无它则解除无痕；表数服从完整性，不维持旧数）。
> （第二索引由按地址运维查询直接需要，非投机）；state=4 的等待/停止区分见 R8。
