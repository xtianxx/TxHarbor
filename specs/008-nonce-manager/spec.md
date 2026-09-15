# Feature Specification: 008 Nonce Manager

**Feature Branch**: `008-nonce-manager`

**Created**: 2026-09-16

**Status**: Draft

**Input**: User description: "008 nonce-manager（nonce 管理与恢复对账）：为出站交易提供并发安全的 nonce 预留、持久绑定与恢复对账；本次仅 specify，不进入 clarify / plan / tasks / analyze / implement。"

**阅读对象**：本规格面向操作员（处置 nonce 缺口、外部消耗与结果未知对账）、审计者（追溯绑定与证据）与下游阶段设计者（009–011）。链上术语含义见 Key Entities（nonce 预留 = 为某付款意图保留某 sender 在某链上的一个可用 nonce；持久绑定 = 预留与其付款意图之间经受持久层保证的关联；结果未知 = 外部副作用可能已发出但结果无法确认；缺口 = 低 nonce 尚未终态而其上 nonce 已有记录）；技术载体（表结构、锁、算法、模块接口）一律留给 plan，上游技术引用仅出现在溯源说明中。

前置依赖核验结论（2026-09-16，基线 `d9096db` = `origin/main` tip `19fa11e`（007 合并点）+ 工作流文档 `d0dcd60` + R1 修复 `d9096db`；006 合并点 `8e1a440`）：

- **006 已交付并合并**：恢复期间操作矩阵（FR-26，Q3 B 已批准）——nonce 分配与新签名在恢复启动后一律暂停，恢复完成后仍须满足各自阶段门禁；恢复完成不得清除下游独立暂停（Downstream Handoff：008 必须保证异常缺口或不明消耗暂停对账的语义与恢复暂停可共存）；暂停前已发出的外部请求保留结果未知并继续查询取证，不得视为失败后重新付款；不得声称一次数据库检查能撤回已发出的 RPC，竞态隔离机制归属各自 plan（006 只设边界）。
- **007 已交付并合并（`19fa11e`）**：提款请求接收为**仅接收**——Accepted 仅表示"请求已被接收并持久化"，不表示已执行/已付款/已扣减；007 不携带 nonce 字段、不分配 nonce、不签名、不广播（FR-08/FR-19）；008 承接：nonce 分配时机、去重与恢复暂停的共存语义由 008 设计（007 Downstream Handoff）；"请求 ↔ 付款意图"关联要求与执行前重新授权/取消为下游设计面（007 contracts/api.md §4）。
- **T000-P 保持 open**：生产 provider 选型不在本规格中关闭。
- **共同契约事项**：OC-1–OC-5 已于 2026-09-16 clarify 裁决（见 Clarifications；原 FR-01/FR-03/FR-04 澄清标记与 FR-17/FR-18 的 OPEN 引用已改为裁决表述），OC-6/OC-7 保持 OPEN（见 Dependencies & Assumptions 的 OC 表）。
- 本规范按 `docs/workflow-008-009-parallel.md`（R1 限定例外、R2 单步骤、R3 共同契约）在 008/009 并行编制窗口内完成 specify（2026-09-16）与 clarify 写回（2026-09-16）；不进入 plan/tasks/analyze/implement。

## Background, Goals & Scope

### Background

TxHarbor 为交易所、托管平台和 Web3 钱包后台提供可恢复、可审计的链上资产处理能力。第一版保持单部署单链、白名单标准 ERC-20、原生币仅用于 gas，不维护用户余额账本，不承担 KYC、风控审批或冷热钱包调度，不引入 Redis、Kafka 或其他新增基础设施，本地使用 Anvil，Go + PostgreSQL + EVM JSON-RPC。项目章程为 Constitution 1.1.0（`.specify/memory/constitution.md`）。

本规范是 TxHarbor 第 008 号功能规范，目录固定为 `specs/008-nonce-manager`（本次新建；仓库此前不存在 008 规范文件，不复用、不改写编号）。仓库已存在 `specs/001-project-foundation` 至 `specs/007-withdrawal-creation`。

008 的职责：把出站交易所需的 nonce 视为共享可变财务基础设施状态（章程原则 VII），提供并发安全的预留、与付款意图的持久绑定，以及崩溃、重启、结果未知与链视图分歧下的恢复对账。008 不签名、不广播、不持有密钥，也不创建付款意图（付款意图创建与身份为 OC-1，已裁决 2026-09-16：由 011 创建并持久化，见 Clarifications）。

### Goals

- 为同一 `(chain_id, sender)` 的并发请求提供至多一个有效绑定的 nonce 预留；不同链、不同 sender 互不干扰。
- 预留与付款意图之间建立持久绑定；重复请求（重试、并发重复、重启后重复）复用原绑定，不重复分配。
- 崩溃与重启后可恢复：从持久记录重建状态，不依赖内存；不产生双分配、不丢失已持久绑定。
- 结果未知与不明确状态一律保留（hold + 对账信号），不自动回收、不重指派；同 nonce 替换保持原付款意图。
- 消化链上视图与本地记录的分歧（latest/pending、缺口、外部消耗），不静默复用、不静默跳过。
- 承接 006 恢复暂停与 007 仅接收边界；007 Accepted 不表示付款意图或执行授权存在。

### Non-Goals / Out of Scope

- 002–007 已锁定行为的重定义；恢复暂停范围与"仅接收"边界为上游契约。
- 具体数据库结构、锁机制、对账算法与模块接口（留给 plan）。
- 签名、密钥与签名策略（009）；交易构造、广播与费用替换机制（010）；执行 worker 与最终结果修订（011）。
- 付款意图的创建者与身份（OC-1）、执行授权机制（OC-5）、签名请求与交易尝试身份（OC-4）：已于 2026-09-16 裁决（见 Clarifications），相应机制实现归 011/授权方/010，008 只绑定与校验其契约。
- 余额账本、KYC、风控审批、冷热钱包调度；多链支持；Redis/Kafka 等新增基础设施。
- 生产 provider 选型（T000-P 保持 open）。

## Clarifications

### Session 2026-09-16

- Q: 付款意图由哪个阶段创建、以何稳定标识存在、与 007 请求如何关联？（OC-1 / FR-04） → A: 付款意图由 011 在执行准入时创建并持久化；其独立、稳定的 `intent_id` 与 007 的 `request_id` 建立持久关联。007 的 Accepted 不是付款意图；一个 `request_id` 至多映射一个 intent。并发、重试、重启与结果未知情形一律复用原 intent。intent MUST 在 008 预留之前持久化；008 只绑定已存在的 intent，MUST NEVER 隐式创建 intent。intent 的存在不蕴含持续执行许可（由 OC-5 治理）。
- Q: sender 从何处权威取得？不同来源决定隔离键的取得方式与既有数据兼容性。（OC-2 / FR-01） → A: sender 权威来源为机构控制的钱包注册表/部署配置；调用方声明 MUST NOT 具有权威性。011 在执行准入时固定 sender，并在首次预留前持久绑定 `intent_id` 与 `chain_id`；重试与替换复用原 `chain_id` 与 sender。008 MUST 按绑定的 `chain_id`+sender 隔离分配，并拒绝与绑定不匹配的输入。注册表未注册、禁用或不可读时 MUST 拒绝新预留并保留既有记录；配置变更 MUST 受控且可审计。注册仅证明 sender 可用性，MUST NOT 被解读为执行授权（OC-5）。
- Q: nonce 绑定由 008 自有持久记录承载，还是复用/扩展上游现有记录？绑定归属与跨阶段读取契约如何定？（OC-3 / FR-03） → A: nonce 持久绑定由 008 自有并负责创建、维护与对账；009 通过显式只读契约消费。绑定链接 `intent_id`、`chain_id`、sender 与 nonce，并具稳定、可引用的身份。重复请求返回原绑定；同一 intent MUST NEVER 获得第二绑定；同一 `(chain_id, sender, nonce)` MUST NEVER 绑定另一 intent。费用替换通过不同的 attempt/request 身份引用原绑定（OC-4）。绑定不等于授权（OC-5），也不等于始终可执行（OC-6/OC-7）。载体与一致性机制留给 plan；009 保持密钥隔离边界。
- Q: 签名请求与交易尝试身份由谁分配、绑定哪些字段、与 010 交易尝试如何关联？费用替换如何取得新身份？（OC-4 / FR-18） → A: 签名请求身份由可信调用方预分配（`signing_request_id`）。010 MUST 在首次签名调用前持久化 `attempt_id`、`signing_request_id` 与完整内容，并链接 `intent_id` 与 008 的持久绑定。重试 MUST 复用原身份与原内容；同一身份不同内容 MUST 拒绝。009 MUST 在成功前持久化其绑定与可恢复结果。一次尝试对应一个请求身份。费用替换 MUST 在原 intent 与原绑定下创建新的 `attempt_id` 与 `signing_request_id`。身份 MUST NOT 作为授权凭据（其绕过范围由 OC-5/OC-6/OC-7 治理）。
- Q: 签名前是否必须存在显式执行授权？由谁签发、凭何字段校验、与付款意图及签名请求身份如何关联、无效时如何处置？（OC-5 / FR-17） → A: 每笔交易 MUST 具备显式执行授权，由指定可信上游业务授权方签发。007 Accepted、授权成功本身、intent 存在或 nonce 绑定 MUST NOT 替代执行授权。授权 MUST 具稳定身份与可验证真实性，链接原 007 request 与调用方，并约束 `chain_id`、sender、资产、收款方、整数金额与费用范围，且具有效期与撤销能力。011 MUST 将授权绑定到唯一 intent；008 与 009 MUST 校验授权。缺失、过期、已撤销、不匹配或不可验证时 MUST fail-closed：008 不产生新预留，009 不产生新签名。授权仅限单一 intent；重试 MUST NOT 消耗或延长授权；每个签名请求 MUST 持久绑定其授权身份与版本，008 的预留同样 MUST 绑定授权。费用替换 MUST 具备明确允许该操作的授权，否则 MUST 使用新授权与新身份。撤销后重放已签名结果的治理属 OC-6/OC-7（非默认允许）。授权载体与语义 MUST 优先复用 007 已批准方案；缺口 MUST 显式记录，MUST NOT 静默改写 007。

## Dependencies & Assumptions

### Prerequisite dependencies

- **D1 — 006 恢复暂停与在途未知（已核验，只读消费）**：恢复启动后 nonce 分配一律暂停；恢复完成后的分配仍须满足 008 自有门禁；恢复完成不得清除独立暂停；在途未知保留并继续查询，不得视为失败重付；检查不能撤回已发出的外部请求。本规范引用而不重新规定 006 语义。
- **D2 — 007 仅接收边界（已核验，只读消费）**：Accepted 请求不携带 nonce、不触发 nonce/签名/广播；007 与 008 之间无执行绑定；请求↔付款意图关联与执行前授权/取消为下游设计面。本规范引用而不重新规定 007 语义。
- **D3 — 付款意图身份（OC-1 已裁决 2026-09-16）**：011 在执行准入时创建并持久化付款意图，以独立稳定 `intent_id` 持久关联 007 `request_id`；008 只绑定既有 intent，不隐式创建（见 Clarifications）。
- **D4 — sender 权威来源（OC-2 已裁决 2026-09-16）**：权威来源为机构控制的钱包注册表/部署配置，调用方声明不具权威性；008 按绑定的 `(chain_id, sender)` 隔离并拒绝不匹配输入（见 Clarifications）。
- **D5 — nonce 持久绑定的载体（OC-3 已裁决 2026-09-16）**：绑定记录归 008 自有并负责创建/维护/对账，009 经显式只读契约消费；表结构、锁、事务划分仍留给 plan（见 Clarifications）。
- **D6 — 签名请求/交易尝试身份（OC-4 已裁决 2026-09-16）**：可信调用方预分配 `signing_request_id`，010 在首次签名调用前持久化 `attempt_id`、`signing_request_id` 与完整内容并链接 `intent_id` 与 008 绑定（见 Clarifications）。
- **D7 — 执行授权（OC-5 已裁决 2026-09-16）**：每笔交易 MUST 具备指定可信上游业务授权方签发的显式执行授权；缺失/过期/撤销/不匹配/不可验证一律 fail-closed（见 Clarifications）。
- **D8 — 对账信号载体（缺失，本规范提需求、plan 定载体）**：分歧/缺口/外部消耗/结果未知的暴露方式留给 plan。
- **D9 — 可观测载体扩展（缺失，本规范提需求、plan 定载体）**：状态查询与日志字段组合留给 plan。

### Contract items（共同契约事项；OC-1–OC-5 已裁决 2026-09-16，OC-6/OC-7 保持 OPEN）

| ID | 待确定事项 | 状态 |
| --- | --- | --- |
| OC-1 | 付款意图创建与身份 | 已裁决 2026-09-16 |
| OC-2 | sender 权威来源 | 已裁决 2026-09-16 |
| OC-3 | nonce 持久绑定 | 已裁决 2026-09-16 |
| OC-4 | 签名请求与交易尝试身份 | 已裁决 2026-09-16 |
| OC-5 | 执行授权 | 已裁决 2026-09-16 |
| OC-6 | 006 恢复暂停 | OPEN |
| OC-7 | 检查到副作用之间的竞争 | OPEN |

OC-1–OC-5 已裁决（2026-09-16）并写入 Clarifications 与相关 FR/依赖；OC-6/OC-7 保持 OPEN 行记录，相关条款不将其写成已定机制。

### Explicit assumptions

- 单部署单链：同一时间只服务一条配置链；多链支持不属 v1（沿用上游）。
- 一个付款意图至多一个有效 nonce 绑定；一个 nonce 至多一个付款意图主张（本规范核心不变量；付款意图身份形式已裁决为 011 持久化的 `intent_id`，见 Clarifications）。
- nonce 数值的表示与安全运算留给 plan（不得使用浮点，原则 I）。
- 本地验证以 Anvil + PostgreSQL + Docker Compose 为权威环境；006/007 已声明并行工作目录共享本地测试资源须协调（workflow R4），本规范不例外。
- 006 已合并（`8e1a440`）、007 已合并（`19fa11e`）为输入；T000-P 保持 open；005 的实现/回归/验收证据状态沿用上游记录，不阻塞本规格。

## User Scenarios & Testing *(mandatory)*

### User Story 1 — 并发下的唯一预留与重复调用复用（Priority: P1）

多个执行者为同一 `(chain_id, sender)` 的不同付款意图并发请求 nonce；同一意图随后重试；系统至多产生一个有效绑定，重复调用返回原绑定。

**Why this priority**: 核心价值；非重复分配是原则 VII 的直接要求，也是下游签名与广播的前提。

**Independent Test**: 在本地环境以 N 个并发请求（同作用域、不同意图）与同意图重复请求（含并发重复与重启后重试）驱动，断言无重复 nonce、每意图有效绑定数 ≤1、重复调用不改变绑定。

**Acceptance Scenarios**:

1. **Given** 同一 `(chain_id, sender)` 与两个不同付款意图，**When** 并发请求预留，**Then** 各自获得不同 nonce（或一方安全失败后重读再决策），MUST NOT 出现同一 nonce 的两条有效绑定。
2. **Given** 意图 A 已绑定 nonce N，**When** A 的预留请求重试（含并发重复），**Then** 返回原绑定 N，不产生新分配、不改变绑定。
3. **Given** nonce N 已有有效绑定，**When** 另一意图请求，**Then** MUST NOT 获得 N；不同 sender 或不同 chain 的请求 MUST NOT 影响该作用域。
4. **Given** 进程重启，**When** A 再次请求，**Then** 仍返回原绑定（持久层判定，不依赖内存）。

---

### User Story 2 — 预留后崩溃与重启恢复（Priority: P1）

预留持久化后、外部副作用前后崩溃；重启后系统从持久记录重建，不双分配、不丢失绑定。

**Why this priority**: 崩溃与重启是生产常态；没有持久绑定与续跑，唯一性只是"检查时正确"。

**Independent Test**: 在预留持久化后与外部副作用已发起后分别注入崩溃并重启，断言绑定唯一、原意图复用原 nonce、重建完成前不分配。

**Acceptance Scenarios**:

1. **Given** 绑定已持久化且尚未发起任何外部副作用，**When** 崩溃重启，**Then** 原意图重复请求返回原绑定；MUST NOT 产生第二绑定。
2. **Given** 崩溃发生在外部副作用（签名/提交）已发起之后且结果未知，**When** 重启，**Then** 该 nonce 保持不可分配，继续查询取证，MUST NOT 自动回收或重指派。
3. **Given** 重启后持久状态重建尚未完成，**When** 新的分配请求到达，**Then** fail-closed（等待或拒绝并记录原因），MUST NOT 依赖内存猜测分配。

---

### User Story 3 — 结果未知、禁止回收与同 nonce 替换（Priority: P1）

结果未知的预留必须保留；同 nonce 的替换尝试保持原付款意图；已签名/已广播/结果未知的 nonce 绝不交给另一意图。

**Why this priority**: 未知结果被误判为失败会导致重复付款（006 已禁止）；重指派会破坏付款意图归属与审计。

**Independent Test**: 构造结果未知场景后发起新分配与同 nonce 替换尝试，断言 0 回收、0 重指派、替换仍归属原意图。

**Acceptance Scenarios**:

1. **Given** nonce N 的外部副作用结果未知，**When** 任何新意图请求 nonce，**Then** N MUST NOT 被分配；系统 MUST 继续查询并记录证据。
2. **Given** nonce N 已签名、已广播或结果未知，**When** 同 nonce 替换（含费用替换）或重播尝试发生，**Then** 该尝试 MUST 保持与 N 原付款意图的绑定，MUST NOT 建立第二意图主张。
3. **Given** 系统需要判定某预留"未产生外部副作用"，**Then** MUST 基于明确证据；超时或连接错误 MUST NOT 单独作为判据；证据不足时保持未知并记录。

---

### User Story 4 — 链视图分歧、缺口与外部消耗的对账（Priority: P1）

链上 latest/pending 视图与本地记录分歧、出现缺口或有外部消耗时，系统保持事实并进入对账，绝不静默复用。

**Why this priority**: 静默复用会与已消耗 nonce 冲突（交易被替换/拒绝）或破坏本地序列的可追溯性。

**Independent Test**: 构造"链上已消耗而本地未终态"、"本地缺口"、"RPC 落后/不可用"三类场景，断言静默复用 0 次、对账记录 100%。

**Acceptance Scenarios**:

1. **Given** 链上显示 nonce N 已被消耗而本地 N 未终态，**When** 系统观察或对账，**Then** 记录已消耗及其证据；MUST NOT 复用 N。
2. **Given** 本地出现缺口（低 nonce 未终态而其上 nonce 已有记录），**When** 有新分配请求，**Then** MUST NOT 假定缺口可复用；缺口 MUST 经受控对账定性后才可处置。
3. **Given** latest 与 pending 视图冲突或 RPC 失败/落后，**When** 需要判定，**Then** 按证据不足处理：保持现状并记录，MUST NOT 以不足证据解除 hold 或回收 nonce。
4. **Given** 发现外部来源（非本系统发出）消耗了某 nonce，**When** 对账，**Then** 记录证据与归属不确定性；MUST NOT 将其静默并入本地序列。

---

### User Story 5 — 承接 006 恢复暂停与 007 仅接收边界（Priority: P2）

恢复期间分配一律暂停；恢复完成后仅当自有门禁满足才可恢复；仅存在 Accepted 请求不构成分配/签名/广播依据。

**Why this priority**: 006/007 契约承接；违反即破坏重组恢复安全边界与"仅接收"承诺。

**Independent Test**: 模拟 006 恢复活跃状态下请求分配（断言 0 分配、记录原因）；恢复完成后存在独立 hold（断言不被清除）；仅以 Accepted 请求驱动（断言 0 隐式动作）。

**Acceptance Scenarios**:

1. **Given** 006 恢复活跃（恢复行存在且非释放终态），**When** 任何 nonce 分配请求到达，**Then** 一律拒绝或保持并记录原因；MUST NOT 分配、签名或触发广播。
2. **Given** 恢复完成，**When** 分配请求到达，**Then** 仅当 008 自有门禁（未终态对账 hold、证据充分、状态已重建）同时满足时恢复；恢复完成 MUST NOT 清除独立暂停。
3. **Given** 仅存在 007 Accepted 请求，**When** 任何路径执行，**Then** MUST NOT 推断付款意图存在、已获执行授权或允许 nonce 分配/签名/广播。
4. **Given** 暂停检查与外部副作用之间存在时序竞争（OC-7 OPEN），**When** 状态无法确认，**Then** fail-closed（不发起需要有效链视图授权的动作并记录原因）；MUST NOT 声称一次检查能撤回已发出的外部请求。

---

### Edge Cases

- nonce 0（首个 nonce）与最大值边界：不得因此破坏唯一性；数值表示与安全运算留给 plan。
- 持久层/数据库不可用：fail-closed，不分配、不猜测；恢复可用后先重建状态再继续。
- 006 恢复完成后的链视图重建：对 nonce 绑定有效性的影响 MUST 基于重验后的证据；恢复前的结果未知预留 MUST NOT 自动回收。
- 同一意图的重复预留以稳定身份判定（OC-1 已裁决：011 持久化的 `intent_id`）；MUST NOT 以不稳定运行时标识（内存地址、请求追踪 ID）作为唯一去重依据。
- 对账处置的授权与证据标准（何主体、何证据可释放绑定）未决：留给后续 clarify/plan；本规范只要求受控、可审计、有证据。
- 日志脱敏：错误输出保留 chain、sender、nonce、付款意图标识、状态、错误分类、重试次数等非敏感上下文；凭据与私钥材料 MUST NOT 输出。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-01**: nonce 预留 MUST 以 `(chain_id, sender)` 为隔离作用域：同一作用域内非重复分配；不同链或不同 sender MUST NOT 共享或相互影响 nonce 空间。sender 权威来源已裁决（2026-09-16，OC-2）：sender 权威取得自机构控制的钱包注册表/部署配置，调用方声明 MUST NOT 作为权威来源；011 MUST 在执行准入时固定 sender 并在首次预留前持久绑定 `intent_id` 与 `chain_id`，重试/替换 MUST 复用原 `chain_id` 与 sender；008 MUST 按绑定的 `(chain_id, sender)` 隔离分配，并拒绝与绑定不匹配的输入；注册表未注册/禁用/不可读时 MUST 拒绝新预留并保留既有记录；配置变更 MUST 受控且可审计；注册仅证明可用性，MUST NOT 被解读为执行授权（OC-5）。
- **FR-02**: 同一 `(chain_id, sender)` 下，一个 nonce MUST 至多关联一个有效付款意图绑定；并发正确性 MUST 由持久层保证（唯一约束/行锁/事务级串行化等，具体机制留给 plan），MUST NOT 仅依赖进程内锁；并发正确性 MUST 由真实并发集成测试覆盖（原则 VII/XI）。
- **FR-03**: 预留与付款意图的绑定 MUST 在发起任何签名请求或广播触发之前持久化；绑定语义 MUST 至少覆盖作用域、nonce、付款意图身份、时间与状态；重复执行 MUST 幂等（原则 II/VI）。绑定归属已裁决（2026-09-16，OC-3）：持久绑定由 008 自有并负责创建、维护与对账，009 经显式只读契约消费；绑定 MUST 链接 `intent_id`、`chain_id`、sender 与 nonce，并具稳定可引用身份；重复请求 MUST 返回原绑定；同一 intent MUST NEVER 获得第二绑定；同一 `(chain_id, sender, nonce)` MUST NEVER 绑定另一 intent；费用替换 MUST 以不同的 attempt/request 身份引用原绑定（OC-4）；绑定 MUST NOT 被等同于执行授权（OC-5）或始终可执行（OC-6/OC-7）；载体与一致性机制留给 plan（009 保持密钥隔离边界）。
- **FR-04**: 每个 nonce 绑定 MUST 关联一个稳定的付款意图身份。付款意图身份已裁决（2026-09-16，OC-1）：011 MUST 在执行准入时创建并持久化付款意图，以独立、稳定的 `intent_id` 持久关联 007 `request_id`；007 Accepted MUST NOT 被视为付款意图；一个 `request_id` 至多映射一个 intent；并发/重试/重启/结果未知情形 MUST 复用原 intent；intent MUST 在 008 预留之前持久化；008 MUST 只绑定既有 intent，MUST NEVER 隐式创建；intent 的存在 MUST NOT 被解读为持续执行许可（OC-5）；系统 MUST NOT 依据 Accepted 状态推断付款意图存在（守卫生效范围见 FR-16）。
- **FR-05**: 同一付款意图的重复预留请求（含重试、并发重复、重启后重复）MUST 复用原绑定与原 nonce，MUST NOT 产生第二绑定；重复判定 MUST 基于持久层记录，MUST NOT 依赖进程内状态（原则 II）。
- **FR-06**: 绑定持久化后进程崩溃，重启 MUST 从持久记录重建并复用原绑定；MUST NOT 双分配，MUST NOT 在保留旧绑定的同时为同一意图重新分配不同 nonce。
- **FR-07**: 已发起外部副作用（签名/提交/广播）但结果未知时，系统 MUST 保留结果未知语义：继续查询、记录证据，MUST NOT 视为失败后重新付款或重新分配；该 nonce 的唯一性约束在此期间 MUST 继续成立（006 在途未知条款）。
- **FR-08**: 对任何"不明确"的预留（结果未知、链上/本地不一致、未解释缺口），系统 MUST NOT 自动回收或自动释放 nonce；状态改变 MUST 仅经受控、可审计且附证据的对账处置（授权与证据标准留给后续阶段/plan）。
- **FR-09**: 已签名、已广播或结果未知的 nonce MUST NEVER 被分配给另一付款意图；同 nonce 的替换（费用替换、同字节重播）MUST 保持与原付款意图的绑定，MUST NOT 建立第二意图主张（与 006 下游承接同构）。
- **FR-10**: 当链上 latest/pending 视图与本地记录分歧时，系统 MUST 保持事实并记录对账信号（分歧类型、双方证据）；MUST NOT 静默复用、跳过或覆盖；RPC 失败/落后 MUST 按证据不足处理，MUST NOT 以不足证据解除 hold。
- **FR-11**: 本地出现 nonce 缺口（低 nonce 未终态而其上 nonce 已有记录）时，MUST NOT 假定缺口可复用、MUST NOT 静默跳过后继续分配；缺口 MUST 在受控对账中定性后才可处置；系统 MUST 可查询/审计每个 nonce 的当前绑定与状态。
- **FR-12**: 链上/外部已消耗的 nonce MUST 被记录为已消耗（无论是否本系统发出），相关本地预留 MUST NOT 被复用；无法归属的消耗 MUST 记录证据并对账，MUST NOT 静默并入本地序列。
- **FR-13**: 重启后系统 MUST 从持久记录重建 `(chain_id, sender)` 状态；MUST NOT 依赖内存状态；在重建完成前 MUST NOT 分配（fail-closed）。
- **FR-14**: 006 恢复启动后，nonce 分配 MUST 一律暂停（006 FR-26 继承）；恢复完成 MUST NOT 清除 008 独立暂停（异常缺口/不明消耗对账，006 Downstream Handoff）；恢复完成后的分配 MUST 同时满足 008 自有门禁与 006 完成判据。008 自有暂停的触发、解除与授权细节未决（OC-6，OPEN；本规范只锁定"可共存、不互相清除、解除须重验"三条边界，MUST NOT 写成已定机制）。
- **FR-15**: 分配提交前 MUST 在持久层保证下重验暂停/恢复门禁与自身状态，失配即拒绝/回滚（原则 VI；与 006 FR-20 提交门禁同构）；任何前置检查失败或状态不可确认时 MUST NOT 发起需要有效链视图授权的动作。跨阶段的"检查→副作用"隔离机制未决（OC-7，OPEN；006 已定：隔离机制属相应后续 plan，006 只设边界）；系统 MUST NOT 声称一次检查能撤回已发出的外部请求。
- **FR-16**: 仅存在 007 Accepted 请求 MUST NOT 被解释为：付款意图已存在、已获执行授权、或允许 nonce 分配/签名/广播；系统 MUST NOT 因 Accepted 请求的存在而自动分配 nonce 或推断付款意图（007 FR-08/FR-19、contracts/api.md §4；守卫条款，本规范已定）。
- **FR-17**: 执行授权已裁决（2026-09-16，OC-5）：每笔交易 MUST 具备显式执行授权，由指定可信上游业务授权方签发；007 Accepted、授权成功本身、付款意图存在或 nonce 绑定 MUST NOT 替代该授权。授权 MUST 具稳定身份与可验证真实性，链接原 007 request 与调用方，约束 `chain_id`、sender、资产、收款方、整数金额与费用范围，并具有效期与撤销能力；011 MUST 将授权绑定到唯一付款意图；008 与 009 MUST 校验授权。缺失、过期、已撤销、不匹配或不可验证时 MUST fail-closed：008 MUST NOT 产生新预留，009 MUST NOT 产生新签名；授权仅限单一付款意图，重试 MUST NOT 消耗或延长授权；每次签名请求 MUST 持久绑定其授权身份与版本，008 的预留同样 MUST 绑定授权；费用替换 MUST 具备明确允许该操作的授权，否则 MUST 使用新授权与新身份；撤销后重放已签名结果的治理属 OC-6/OC-7（非默认允许）；授权载体与语义 MUST 优先复用 007 已批准方案，缺口 MUST 显式记录，MUST NOT 静默改写 007。
- **FR-18**: 签名请求与交易尝试身份已裁决（2026-09-16，OC-4）：可信调用方 MUST 预分配 `signing_request_id`；010 MUST 在首次签名调用前持久化 `attempt_id`、`signing_request_id` 与完整内容，并链接 `intent_id` 与 008 的持久绑定；重试 MUST 复用原身份与原内容；同一身份不同内容 MUST 拒绝；009 MUST 在成功前持久化其绑定与可恢复结果；一次尝试对应一个请求身份；费用替换 MUST 在原付款意图与原绑定下创建新的 `attempt_id` 与 `signing_request_id`；身份 MUST NOT 作为授权凭据（其绕过范围由 OC-5/OC-6/OC-7 治理）。系统 MUST 保证：同一绑定上的多次尝试可追溯至同一付款意图；MUST NOT 产生允许同一 nonce 被两个意图主张的身份或关联。
- **FR-19**: 008 MUST NOT 持有或访问私钥、MUST NOT 执行签名或广播；签名/广播属 009/010 边界（原则 VIII）。008 的输出仅为预留与绑定事实。
- **FR-20**: 预留/绑定生命周期 MUST 为显式状态机（允许状态、允许转换、终态、失败态、重试行为），非法转换 MUST 拒绝或按错误处理；状态转换 MUST 在数据库事务内完成（原则 V）。
- **FR-21**: 关键操作 MUST 使用结构化日志与可观察信息（含 `chain_id`、sender、nonce、付款意图标识、绑定状态、恢复状态、对账 hold、重试/尝试、RPC 端点等字段）；MUST NOT 输出私钥、凭据或不受限原始数据（原则 XII、安全规则）。
- **FR-22**: 并发、崩溃、重启、结果未知、分歧/缺口/外部消耗 MUST 有真实并发/集成测试（Anvil + PostgreSQL）；mocks MUST NOT 替代（原则 X/XI）。
- **FR-23**: 本规范 MUST NOT 重定义 006/007 已锁定行为；恢复暂停范围、仅接收边界与在途未知语义为上游契约（原则 XIV）。本规范 MUST NOT 锁定表结构、锁机制、对账算法与模块接口（留给 plan）；影响正确性的未决问题 MUST 以 OPEN 行或澄清标记显式标注，MUST NOT 把未经确认的假设写成已确定事实。

### Key Entities *(include if feature involves data)*

- **Nonce 作用域**：`(chain_id, sender)` 二元组，nonce 空间的隔离单位；sender 权威来源为机构控制的钱包注册表/部署配置（OC-2 已裁决 2026-09-16）。
- **付款意图（Payment Intent）**：绑定目标身份；由 011 在执行准入时创建并持久化，稳定标识 `intent_id` 持久关联 007 `request_id`，一个 `request_id` 至多一个 intent（OC-1 已裁决 2026-09-16）；008 只绑定既有 intent，不创建。
- **Nonce 绑定（预留）**：作用域、nonce、付款意图身份、状态、时间与证据引用的持久关联。语义生命周期（命名与载体留给 plan）：`已预留` → `已发起外部副作用`（结果已知/未知分支）→ 终态 `已消耗` 或 `经对账释放`；"结果未知"为持续态，MUST NOT 自动转换为终态。
- **对账信号**：分歧（latest/pending vs 本地）、缺口、外部消耗、结果未知的持久说明（含证据引用）；解除只能经受控对账处置。
- **恢复状态（源自 006）**：只读消费；008 MUST NOT 修改其行、暂停或版本。
- **签名请求/交易尝试（OC-4 已裁决 2026-09-16）**：消费绑定的下游对象；调用方预分配 `signing_request_id`，010 持久化 `attempt_id` 与内容并链接意图与绑定，费用替换在原意图与原绑定下创建新身份；本规范仅约束其不得破坏绑定不变量。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-01**: 同一 `(chain_id, sender)` 下 N 路（N≥2）并发为不同意图请求预留，出现重复 nonce 生效分配的次数为 0；每个意图有效绑定数 ≤1。
- **SC-02**: 同一意图重复预留请求（重试/并发/重启后）M 次，新增分配次数为 0，返回原绑定比例 100%。
- **SC-03**: 在绑定持久化后与外部副作用已发起后分别注入崩溃并重启，双分配次数为 0，已持久绑定丢失次数为 0；重建完成前分配次数为 0。
- **SC-04**: 结果未知预留被自动回收、改判失败或重指派的次数为 0；证据与查询记录 100% 保留。
- **SC-05**: 同 nonce 替换场景下，原付款意图绑定保持不变的比例 100%；第二意图获得该 nonce 的次数为 0。
- **SC-06**: 分歧/缺口/外部消耗场景下，静默复用次数为 0；对账记录比例 100%；证据不足时解除 hold 的次数为 0。
- **SC-07**: 006 恢复活跃期间分配次数为 0；恢复完成后独立暂停被清除次数为 0。
- **SC-08**: 仅存在 Accepted 请求的场景下，隐式 nonce 分配/签名/广播次数为 0。
- **SC-09**: 验收运行中日志与响应出现私钥或凭据材料的次数为 0。

## Assumptions

- 需求状态区分：【上游已确认约束】见 Background 核验结论与 Upstream Traceability；【本阶段需求】为 FR-01–FR-23；【已裁决】OC-1–OC-5（2026-09-16，见 Clarifications，含原 FR-01/FR-03/FR-04 澄清标记与 FR-17/FR-18 的 OPEN 引用）；【记录为 OPEN、不在本规范裁决】为 OC-6/OC-7；【留给 plan】为表结构、锁机制、对账载体、状态命名与算法、数值表示。
- Deferred decisions（不设澄清标记，留待 clarify 或 plan）：
  - **D-01（留给 plan/后续阶段）**："明确未产生外部副作用"的证据标准与释放处置（不得以超时/连接错误单独判定）。
  - **D-02（留给 plan）**：对账信号的载体、状态命名、锁与事务划分、fail-closed 的具体实现形态。
  - **D-03（clarify 已裁决 2026-09-16）**：OC-1/OC-2/OC-3（原标记）与 OC-4/OC-5（原 OPEN 行）均已裁决（见 Clarifications）；OC-6/OC-7 保持 OPEN 行。
  - **D-04（下游联动）**：009 签名请求身份与恢复版本绑定、010 同 nonce 多尝试允许条件、011 结果修订与原意图追踪（见 Downstream Handoff）。
- 006 恢复暂停与 008 自有暂停（异常缺口/不明消耗对账）的共存只保留边界约束，具体机制不在本规范设计（FR-14）。
- 本规范不创建任何未来业务表与状态机实现；不宣称与 009–011 双向核对；本次交付为将 5 项 clarify 裁决写回需求规范并复验质量清单，不进入 plan、tasks、analyze、implement。

## Upstream Traceability（联动记录：上游已确认约束及其来源）

| 上游约束 | 来源 |
|----------|------|
| nonce 分配为共享可变财务基础设施状态；并发不得重复分配；需持久层并发控制；恢复须覆盖重启、挂起、替换、RPC 分歧与外部提交 | Constitution 1.1.0 原则 VII |
| 财务正确性、持久层幂等、显式状态机、事务边界、失败路径一等行为、可观测性、小步规格 | Constitution 1.1.0 原则 I/II/V/VI/IX/XII/XIV |
| 恢复启动后 nonce 分配与新签名一律暂停；恢复完成后仍须满足各自阶段门禁；同字节重播与费用替换广播暂停 | 006 spec FR-02/FR-26（Q3 B 已批准，2026-09-14） |
| 008 必须保证异常缺口/不明消耗暂停对账与恢复暂停可共存；恢复完成不得清除其独立暂停 | 006 spec Downstream Handoff |
| 在途未知保留并继续查询，不得视为失败重付；检查不能撤回已发出的 RPC；隔离机制归属各自 plan | 006 spec FR-26 操作矩阵、contracts/downstream.md |
| 旧 worker 版本隔离；提交前重验门禁（持锁后重读复核，失配回滚） | 006 spec FR-14/FR-20/FR-23 |
| Accepted 仅表示已接收；007 不携带 nonce、不分配/签名/广播；请求↔付款意图关联为下游设计面 | 007 spec FR-08/FR-19、contracts/api.md §4 |
| 008 承接：nonce 分配时机、去重与恢复暂停的共存语义由 008 设计 | 007 spec Downstream Handoff |
| 共同契约 OC-1–OC-5 已裁决 2026-09-16、OC-6/OC-7 保持 OPEN；008/009 并行规则；mocks 不替代集成验收 | docs/workflow-008-009-parallel.md R1/R3/R5；Clarifications Session 2026-09-16 |

## Downstream Handoff（下游待承接契约，不创建未来业务表与状态机）

- **009（签名隔离）**：签名请求身份已裁决（OC-4，2026-09-16）；签名重试必须与内容/绑定版本绑定，恢复版本变化后不得提交基于旧链视图的内容（006 FR-26）；008 经显式只读契约提供 `(intent_id, chain_id, sender, nonce, 绑定状态)` 事实与稳定绑定身份，不提供密钥材料，009 保持密钥隔离边界。
- **010（交易构造与广播）**：同 nonce 多尝试的允许条件由 010 设计（006 只锁定暂停边界）；交易尝试/签名请求身份已裁决（OC-4）：首次签名调用前持久化 `attempt_id`、`signing_request_id` 与完整内容并链接意图与绑定（见 Clarifications）；结果未知先对账；同 nonce 替换保持原付款意图（本规范 FR-09）。
- **011（提款执行）**：重组后修订结果并追踪原付款意图；不得重建付款"补偿"（含未知结果情形）；过期 worker 隔离与 006 版本约束同构。
- 本规格与 009–011 之间**未做双向核对**（对方规范不在本规范可见范围内），上述为单向待承接声明。
