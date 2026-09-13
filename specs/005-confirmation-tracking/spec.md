# Feature Specification: 005 Confirmation Tracking

**Feature Branch**: `005-confirmation-tracking`

**Created**: 2026-09-14

**Status**: Draft

**Input**: User description: "005 确认数跟踪：充值达到配置确认深度后，由 Pending 转为 Confirmed。确认数公式为 max(0, canonical_tip - block_number + 1)，使用本地连续校验过的 canonical 链头，阈值 N≥1，只有属于当前 canonical 链的充值达到阈值才可确认，区块身份须含 block_hash，状态变更采用条件更新并保留确认时间及审计依据，重复检查/并发/重启幂等，链视图异常时停止确认，确认提交受一致链视图约束，Confirmed 仅表示达到项目确认策略。保持单部署单链、白名单标准 ERC-20、Go + PostgreSQL + EVM JSON-RPC 边界，不引入余额账本、Redis、Kafka 或其他额外平台。"

## Background, Goals & Scope

### Background

TxHarbor 是生产导向的 EVM 钱包与链上交易基础设施后端。第一版为单部署、单链，不维护用户余额账本。

本规范是 TxHarbor 第 005 号功能规范，目录固定为 `specs/005-confirmation-tracking`（本次新建，不存在既有 005 目录需要复用）。项目章程为 Constitution 1.1.0（`.specify/memory/constitution.md`，版本变更 1.0.0 → 1.1.0）。仓库中已存在 `specs/001-project-foundation`（工程底座）、`specs/002-chain-indexer`（区块头同步）、`specs/003-event-indexing`（Transfer 原始日志索引）、`specs/004-deposit-detection`（充值识别）。

前置依赖核验结论（2026-09-14，基于仓库实际状态，分支 `005-confirmation-tracking` 基于 `origin/main` 合并提交 `fd45e8b`）：

- **004 已合并**：`origin/main` 包含合并提交 `fd45e8b`（PR #6 `004-deposit-detection`），其 `spec.md` / `data-model.md` / `contracts/observability.md` / `quickstart.md` / `acceptance.md` 为 005 的上游接口依据。005 只消费 004 已持久化的数据，不重新规定 004 的行为，不修改 004 已锁定的业务语义。
- **004 交付的可用输入**：初始状态为 Pending 的充值观察（`deposit_observations`，主键 `(chain_id, block_hash, tx_hash, log_index)`，含 `block_number`、`block_hash`、`status` 锁死为 `pending`、`version_seq` 来源版本关联）；独立于 003 的充值处理进度（`deposit_checkpoint`）；暂停与结构性缺口信号（`deposit_pause`，`kind ∈ upstream_gap | chain_view_changed | validation_failed`，`pause_id` + `revision` 实例语义 + `deposit_pause_audit` 审计）；只增不改的配置版本历史（`deposit_config_history`）。004 永不写入 `pending` 之外的值；005 以自有迁移扩展状态集合（见 FR-09）。
- **002/003 交付的链视图依据**：`chain_blocks` 以 `(chain_id, number)` 为主键保证同链同高度物理单行，`canonical` 仅表示"索引当前认可的链上记录"，不代表已达确认深度或终局不可逆（002 spec FR-04 / Q&A）；查询统一带 `canonical` 条件；`indexer_checkpoint(height, block_hash)` 以外键三列绑定同一已存块；`indexer_lease` 为全链唯一协调行；`indexer_pause`（`hash_mismatch | parent_mismatch | checkpoint_changed`）存在时推进写必须为 0。003 的 `erc20_transfer_logs` 主键与 004 充值观察同源，其 `block_hash` 与 `chain_blocks` 对应 canonical 块一致；`log_pause`（`chain_view_changed | validation_failed | range_incomplete`）与 `indexer_pause` 独立共存，任一存在即阻止下游提交（004 写事务协议步骤 4 要求三行皆无）。
- **T023 说明**：用户确认 PR #6 与 main CI 绿色，合并提交 `fd45e8b` 已在 `origin/main` 中核验存在；证据提交 `3a79dfa`（"record PR #6, remote CI and merge, close T023"）存在于分支 `004-deposit-detection` 但**不在** `origin/main` 祖先链中（`merge-base --is-ancestor` 核验为 NOT-IN-MAIN）。按任务指令，该待核验项不能据此否定已确认的合并结果，记为待核验实现（见 Assumptions），不阻塞规格编写。
- **003/004 的 T000-P 仍开放**：004 `acceptance.md`（已合并版本）记录"未解决项：T000-P open"，且"通过仅表示本地验收，不等于生产接入就绪"。005 同样不宣称生产就绪。
- 本规范不重复 001–004 的验收结论；凡 005 需要但前阶段未交付的能力，一律列为本规范的新需求，不得声称其已实现。

### Goals

- 对 004 生成的 Pending 充值观察，按配置确认阈值 N 与本地连续校验过的 canonical 链头计算确认数，达到阈值后以条件更新推进为 Confirmed，保留确认时间及可审计的转换依据，供后续流程使用。
- 在重复检查、并发 worker、提交前后崩溃、进程重启下保持幂等：同一充值最多一次有效转换，首次确认事实不可改写。
- 链视图异常、链头缺失、暂停有效或提交前链视图版本变化时停止确认；确认提交必须受一致链视图约束，旧结果不得提交。
- 定义确认提交所需的暂停、版本有效性及审计契约（行为语义层面；具体存储与事务机制留给 plan），并向 006 交接重组撤销所需的全部信息，不擅自批准新的状态恢复策略。

### Non-Goals / Out of Scope

- 用户账户余额变更。本阶段仅推进充值观察状态，不增加任何用户余额（延续 001/003/004 "不维护用户余额账本"）。
- 充值识别本身（004 已负责）。005 不生成充值观察，不重新裁决匹配资格。
- 分叉发现、共同祖先搜索、游标回退与重放算法、旧分叉审计保留（006 的后续职责）。005 发现链视图问题时只停止确认、不自动改写历史；已 Confirmed 行的重组撤销一律由 006 负责，005 不自行回退。
- 确认阈值之外的新增状态（如终局性/上游入账语义）。005 只定义 Pending → Confirmed 的一次转换。
- 阈值变更生效规则的最终裁决（未确定策略标为待澄清，见 FR-03 与 [NEEDS CLARIFICATION]；plan 不得静默选择）。
- 多链调度（第一版每条链一个确认跟踪流；当前部署按单链运行）。
- 生产 provider 选型（T000-P 保持 open）。
- 引入 Redis、Kafka、Kubernetes 或其他额外基础设施；不引入余额账本。

## Dependencies & Assumptions

### Prerequisite dependencies

- **D1 — 004 充值观察输入（已核验可用，只读消费 + 受控状态推进）**：`deposit_observations` 的稳定身份、Pending 初始语义、`version_seq` 版本关联、`block_number` + `block_hash` 区块身份、`deposit_checkpoint` 的独立进度语义、`deposit_pause` / `deposit_pause_audit` 的暂停与审计语义、`deposit_config_history` 的版本链为 005 的上游依据。本规范引用而不重新规定。
- **D2 — 002/003 链视图输入（已核验可用，只读消费）**：`chain_blocks` 的 `(chain_id, number)` 单行 + `canonical` 语义、`indexer_checkpoint` 的外键绑定真相、`indexer_pause` / `log_pause` 的暂停语义为 005 的链头与异常信号依据。本规范引用而不重新规定。
- **D3 — 确认状态持久化扩展（缺失，本规范提出需求、plan 阶段设计）**：`pending` 之外的状态值、确认时间与转换依据的 durable 存储、确认提交的条件更新与版本守卫机制尚未存在。表结构、迁移、锁机制、模块接口一律不在本规范中锁定，留给 plan 阶段。
- **D4 — 确认阈值配置载体（缺失，本规范提出需求）**：阈值 N 的声明位置、格式、校验入口留给 plan 阶段；语义（N≥1、链级单值 v1、变更不静默生效）由本规范锁定，未确定部分标为待澄清。
- **D5 — 可观测载体扩展（缺失，本规范提需求、plan 定载体）**：确认进度、待确认积压、链头滞后、暂停原因的暴露方式（指标 / 状态查询 / 日志字段组合）留给 plan 阶段。

### Explicit assumptions

- 单部署单链：同一时间只跟踪一条配置链；多 worker 指同一链的多个相同部署副本竞争，而非多链调度。
- 本地验证范围限定为 Anvil + 可控假 RPC；不代表生产就绪（T000-P 保持 open）。
- 004 实现验收要求保留为后续实现门禁；未取得的证据标为待核验，不阻塞规格编写。
- 此前本地测试偶发失败原因未知：保留记录，不标记已修复；本规范不得声称该问题已解决，实现阶段须如实复核。
- 重试退避的具体参数（初值、上限、次数、抖动）、批次大小、轮询间隔、请求超时由 plan 阶段确定并给出理由；本规范只锁定行为语义（可重试错误必须有界退避、不可跳位遗漏）。
- 确认检查的驱动方式（ tip 推进触发 / 周期轮询 / 混合）留给 plan 阶段；本规范只要求：正常追赶后符合条件的 Pending 可被处理、不遗漏（SC-04）。
- 阈值 N 为链级单值（v1）：与单部署单链一致，满足章程 IV"确认深度明确且按链或资产策略可配置"中的按链配置；按资产差异化阈值不在本阶段范围。

### Open questions (tracked, resolved in plan, not in spec)

- **OQ1**：确认检查的驱动与实例竞争协调机制（轮询 / 通知 / 游标跟随、与 004 消费的互斥关系）——留给 plan，但必须同时满足 FR-07、FR-08。
- **OQ2**：确认进度/积压/滞后/暂停原因的暴露载体——留给 plan，但 SC 与 FR-11 的可验证性必须保留。
- **OQ3**：确认提交事务与 004 消费提交事务对 `indexer_lease` 协调行的复用与串行化细节——留给 plan，但 FR-08 的"检查→提交无窗口"必须成立。

## User Scenarios & Testing

### User Story 1 — 达到阈值推进为 Confirmed（Priority: P1）

操作员配置确认阈值 N 并启动；凡处于 Pending、属于当前 canonical 链、确认数达到 N 的充值观察，系统将其推进为 Confirmed，记录确认时间及转换依据（链头高度与哈希、适用阈值、计算所得确认数）。

**Why this priority**: 核心价值；没有"达阈值即恰好一次确认转换"，确认跟踪无从谈起。

**Independent Test**: 在 Anvil 上预置 Pending 充值并推进 canonical 链头越过阈值，观察每条达标充值恰好一次转为 Confirmed，且确认时间与依据齐备。

**Acceptance Scenarios**:

1. **Given** 一条 Pending 充值位于高度 h、其 `block_hash` 与当前 canonical 链同高度块一致，canonical 链头为 tip 且 `max(0, tip - h + 1) >= N`，**When** 确认检查覆盖该充值，**Then** 其状态恰好一次变为 Confirmed，并记录确认时间及转换依据（tip 高度与哈希、阈值 N、所得确认数）。
2. **Given** 同一批含多条达标 Pending 充值，**When** 处理完成，**Then** 每条各转换一次，无合并、无遗漏。

---

### User Story 2 — 未达阈值保持 Pending，边界精确（Priority: P1）

确认数处于 N-1 时保持 Pending；处于 N、N+1 时转为 Confirmed；N=1 时充值所在区块计为一次确认（链头即所在高度时确认数为 1）。

**Why this priority**: 与主流程同等重要；边界差一即误确认或漏确认，误确认是资金风险。

**Independent Test**: 固定阈值 N，分别构造确认数为 N-1、N、N+1 的 Pending 充值，以及 N=1 且链头等于所在高度的充值，观察状态结果。

**Acceptance Scenarios**:

1. **Given** Pending 充值的确认数为 N-1，**When** 确认检查覆盖，**Then** 保持 Pending，不记录确认转换。
2. **Given** Pending 充值的确认数为 N 或 N+1，**When** 确认检查覆盖，**Then** 转为 Confirmed（恰好一次）。
3. **Given** 阈值 N=1 且链头高度恰为充值所在高度，**When** 确认检查覆盖，**Then** 确认数计为 1 并转为 Confirmed。

---

### User Story 3 — 非 canonical 与链视图异常不得确认，追赶后不遗漏（Priority: P1）

充值所属区块非 canonical、区块身份（高度 + 哈希）不匹配、链头缺失、链视图异常或暂停有效时，不得确认；属正常索引滞后（链头尚未追上）时仅等待；追赶完成后符合条件的 Pending 可被处理，不遗漏。

**Why this priority**: 基于不可信链视图的确认不可逆风险极高；区分"不可信"与"尚未追上"是确认正确性的前提。

**Independent Test**: 分别构造引用块非 canonical、哈希不一致、链头缺失、暂停有效、单纯滞后的场景，观察确认停止与恢复行为。

**Acceptance Scenarios**:

1. **Given** Pending 充值的 `block_hash` 与当前 canonical 链同高度块不一致（或该高度无 canonical 块），**When** 确认检查覆盖，**Then** 不得转为 Confirmed。
2. **Given** 链头缺失（如空进度无可用链头）、或链视图异常（暂停有效、引用区块无法核实），**When** 确认检查运行，**Then** 停止确认，不提交任何转换。
3. **Given** 仅属正常索引滞后（链头可信但高度尚未覆盖充值所在高度 + N - 1），**When** 确认检查覆盖，**Then** 保持 Pending 等待，不记为异常。
4. **Given** 滞后消除、链头追赶到覆盖位置且链视图一致，**When** 确认检查覆盖，**Then** 符合条件的 Pending 被处理，不遗漏。

---

### User Story 4 — 重复与并发下幂等，崩溃可恢复（Priority: P1）

同一充值被重复检查、多 worker 并发确认、提交前崩溃、提交成功但响应丢失、进程重启后，系统收敛到同一持久化状态：恰好一次 Pending → Confirmed 转换，确认时间与首次依据不可改写。

**Why this priority**: 确认链路正确性的底线；重试、并发与重放必然发生。

**Independent Test**: 同一充值检查两次、双 worker 并发确认、提交临界点 kill、提交响应丢弃后重启，分别观察收敛与首次事实不变。

**Acceptance Scenarios**:

1. **Given** 某充值已成功转为 Confirmed（含确认时间与依据），**When** 再次检查同一充值（任意次数、任意顺序），**Then** 无新增转换，确认时间与依据保持首次值。
2. **Given** 双 worker 并发确认同一 Pending 充值，**When** 双方提交，**Then** 恰好一方成功，另一方收敛为"已确认"而非重复转换；确认时间与依据为首次成功者写入的值。
3. **Given** 确认完成但在提交前退出，或写入中途失败，或提交成功但响应丢失，**When** 重启恢复，**Then** 从持久化状态继续，无遗漏、无重复转换；提交结果未知时先重读持久化状态再决策。

---

### User Story 5 — 暂停与版本变化阻止旧结果提交，语义可审计（Priority: P2）

读取候选后、提交前发生暂停或链视图版本变化时，旧确认结果不得提交成功；已有 Confirmed 不因普通重复检查重复转换或重写历史；确认记录可追溯对应充值、区块、确认依据及转换时间；Confirmed 仅表示达到项目确认策略，不代表不可撤销或上游入账。

**Why this priority**: 没有提交时刻的一致性门禁，"检查时正确"会在并发与暂停下变成"提交时错误"；语义不清则下游会把确认误作终局。

**Independent Test**: 构造读取后暂停生效、读取后链头回退/替换、重复检查已 Confirmed 行的场景，观察提交拒绝与历史不变；检查确认记录的可追溯字段。

**Acceptance Scenarios**:

1. **Given** 某确认候选已读取但尚未提交，**When** 提交前暂停生效或链视图版本变化（如链头哈希/高度 rebased、暂停实例新建或修订变化），**Then** 该次提交必须失败，不产生状态转换。
2. **Given** 某充值已为 Confirmed，**When** 普通重复检查覆盖（无重组），**Then** 不重复转换、不重写确认时间与依据；后续重组撤销一律由 006 负责，本阶段不自行回退。
3. **Given** 任意一条 Confirmed 记录，**When** 操作员或下游追溯，**Then** 可定位对应充值（来源事件身份）、所属区块（高度 + 哈希）、确认依据（链头高度 + 哈希、阈值、所得确认数）及转换时间。
4. **Given** 任意 Confirmed 充值，**When** 下游解读其语义，**Then** 仅表示"达到项目确认策略"，不代表不可撤销，不等于上游入账。

### Edge Cases

- 确认数公式下界：`tip < block_number` 时确认数为 0（`max(0, …)`），不得为负，不得确认。
- 高度恰为阈值边界：`tip - h + 1 == N` 包含（闭区间），必须确认。
- 链头恰为充值所在高度且 N=1：确认数为 1，必须确认。
- 非法阈值（N<1 或非正整数）：见 FR-03 与待澄清项；在裁决前按 [NEEDS CLARIFICATION] 处理，不得静默钳制或忽略。
- 阈值变更：是否允许、如何生效见 FR-03 与待澄清项；在裁决前不得静默沿用旧阈值确认，也不得静默切换新阈值。
- 充值区块身份缺哈希：不得确认（身份必须含 `block_hash`，不能仅按高度判定归属）。
- 同高度存在 canonical 行但哈希与充值引用不一致：不得确认，按链视图异常停止（006 接管前保持停止）。
- 链头缺失 vs 正常滞后：无可用链头（空进度）→ 停止确认；有可信链头但高度未覆盖 → 等待。二者不得混同，更不得把尚未观察到的高度用于确认。
- 读取候选后暂停/版本变化：提交必须失败（FR-08），调用方重读后重新决策。
- 终止信号到达提交临界点：以持久化实际提交结果为准，不假设成功或失败（先重读再决策）。
- 同一充实在 004 侧因收缩变更保留旧版本关联：确认只看 canonical 归属与阈值，不重审 004 的版本语义。
- 006 尚不可用且需回退或修订历史时：保持停止，不自行回退 Confirmed。
- 日志脱敏边界：错误输出保留链、高度、阈值、错误分类、重试次数等非敏感上下文，凭据与无限制原始数据不得输出。

## Requirements

### Functional Requirements

- **FR-01**：系统 MUST 按公式 `confirmations = max(0, canonical_tip - block_number + 1)` 计算每条 Pending 充值观察的确认数，其中 `canonical_tip` 为本地连续校验过的 canonical 链头高度，`block_number` 为充值所在区块高度。确认数包含交易所在区块。`canonical_tip < block_number` 时确认数 MUST 为 0，MUST NOT 为负。
- **FR-02**：`canonical_tip` MUST 取自本地连续校验过的 canonical 链头（002 `chain_blocks` 中 `canonical` 为真且经父子连续性校验的链），MUST NOT 直接使用 RPC 返回的最新高度作为确认依据。链头缺失（无可用链头）时 MUST 停止确认；正常索引滞后（链头可信但高度尚未覆盖）时 MUST 等待，MUST NOT 把尚未观察到的高度用于确认。
- **FR-03**：确认阈值 N MUST 满足 N≥1；只有属于当前 canonical 链的充值达到阈值才可确认。非法阈值（N<1 或非正整数）的处理，以及阈值变更是否允许、如何生效，现有文档未确定，标为待澄清：在裁决前系统 [NEEDS CLARIFICATION: 非法阈值应拒绝启动并报错，还是拒绝本次确认并暂停？]；阈值变更 [NEEDS CLARIFICATION: 是否允许运行时变更阈值？若允许，经何种受控程序生效（授权事务/版本化），生效边界是仅向前还是追溯重算？]。在澄清裁决前，plan 与实现 MUST NOT 静默选择其中任一行为。
- **FR-04**：充值区块身份 MUST 包含 `block_hash`，确认归属判定 MUST 同时比对高度与哈希（与 004 持久化的 `block_hash` 及 `chain_blocks` 同高度 canonical 行一致），MUST NOT 仅按高度判定归属。身份缺失或不匹配时 MUST NOT 确认。
- **FR-05**：Pending → Confirmed 转换 MUST 采用条件更新（仅当行仍为 Pending 且转换依据仍有效时提交），MUST 记录确认时间及可审计的转换依据（链头高度与哈希、适用阈值、所得确认数）。重复检查、并发执行和重启 MUST NOT 重复产生状态转换或改写首次确认事实（首次确认时间与依据不可变）。
- **FR-06**：链视图异常时 MUST 停止确认：引用区块缺失或非 canonical、区块身份不匹配、链头无法核实、适用暂停有效，任一成立即不得提交确认转换。属正常滞后时等待；属不可信链视图时停止并暴露原因。
- **FR-07**：确认提交 MUST 受一致链视图约束：读取候选后、提交前发生暂停或链视图版本变化（如链头高度/哈希变化、暂停实例新建或修订变化）时，旧确认结果 MUST NOT 提交成功。提交事务 MUST 在持锁后重读并复核转换依据（含链头身份、canonical 归属、暂停状态），任一不成立即回滚，调用方重读后重新决策。具体事务与锁机制留给 plan，但"检查→提交无窗口"的正确性要求 MUST 在本阶段锁定，MUST NOT 全部推迟到 006。
- **FR-08**：确认提交 MUST 满足暂停与版本有效性契约：提交前 `deposit_pause` 行 MUST 无（004 自身暂停有效即停止）；`indexer_pause` 行 MUST 无（链级暂停下链视图不可信即停止）；`log_pause` 中 `chain_view_changed` 有效即停止。提交 MUST 携带版本有效性条件（所依据的链头身份与暂停实例/修订在提交时刻仍有效），失配即拒绝。具体检查语句与审计表设计留给 plan。
- **FR-09**：Confirmed 语义 MUST 仅表示"达到项目确认策略"，MUST NOT 被解释为不可撤销，MUST NOT 等同上游入账。已有 Confirmed MUST NOT 因普通重复检查重复转换或重写历史；后续重组撤销一律由 006 负责，005 MUST NOT 自行回退 Confirmed。
- **FR-10**：系统 MUST NOT 在本阶段变更任何用户账户余额；MUST NOT 引入余额账本、Redis、Kafka 或其他额外平台；保持单部署单链、白名单标准 ERC-20、Go + PostgreSQL + EVM JSON-RPC 的现有边界。
- **FR-11**：系统 MUST 提供确认进度、待确认积压、链头滞后及暂停原因的可观察信息；错误信息 MUST 足以定位失败位置和错误类型，但 MUST NOT 泄露敏感凭据或输出无限制的原始数据。
- **FR-12**：本规范 MUST NOT 锁定确认状态的表结构、迁移内容、锁机制、模块接口；006 交接所需的保留信息 MUST 显式列出（见 Key Entities 与 006 交接），MUST NOT 把未经确认的假设写成已确定事实；MUST NOT 擅自批准新的状态恢复策略。

### Key Entities & Consistency Invariants

- **确认阈值策略**：链级单值 N（v1），N≥1。非法值与变更规则待澄清（FR-03），裁决前不得静默选择。
- **Canonical 链头视图**：本地连续校验过的链头（高度 + 哈希），源自 002 `chain_blocks` canonical 行；是确认数公式的唯一高度输入；缺失即停止，无效即停止，滞后即等待。
- **确认候选（评估视图，非持久化断言）**：Pending 观察 + 其区块身份（高度 + 哈希）+ 评估时刻链头身份 + 适用阈值 + 所得确认数。候选只在提交事务的复核通过后才转为转换，复核失败即丢弃重读。
- **确认转换记录**：充值来源身份（沿用 004 主键 `(chain_id, block_hash, tx_hash, log_index)`）+ 所属区块（高度 + 哈希）+ 确认依据（链头高度 + 哈希、阈值 N、所得确认数）+ 确认时间。首次写入后不可变。
- **暂停与版本信号（复用，不重建）**：`deposit_pause`（004 自身暂停）、`indexer_pause`（链级暂停）、`log_pause`（日志流链视图信号）、`indexer_lease`（协调与失权判定）。005 提交裁决要求相关暂停皆无，且所依据版本在提交时刻仍有效。
- **006 交接保留信息**：每条 Confirmed 记录 MUST 保留可供 006 消费的全部输入——充值来源身份、所属区块高度与哈希、确认依据（链头高度与哈希、阈值、所得确认数）、确认时间。005 MUST 保证这些信息可追溯、可重验；重组撤销、Orphaned 标记、游标回退与重放一律由 006 负责。
- **一致性不变量（必须始终成立）**：
  - I1：同一来源身份最多一次有效 Pending → Confirmed 转换（含重复检查、并发、恢复后）。
  - I2：确认时间与首次转换依据写入后不可变；重复检查 MUST NOT 改写。
  - I3：引用的 `block_hash` 必与提交时刻同高度 canonical 块一致，否则该提交 MUST NOT 成功。
  - I4：暂停有效或链视图版本失配期间确认提交次数为 0。
  - I5：Confirmed 行在 006 接管前永不由 005 回退或删除。

### 006 交接要求（本次定义契约，实现与算法留给 006）

- 005 本次 MUST 定义确认提交所需的暂停、版本有效性及审计契约（FR-07、FR-08、确认转换记录），具体实现方式留给 plan。
- 006 的后续职责（本次不实现、不完整设计）：检测分叉并持久化恢复状态，暂停该链确认及后续新提款广播；在最大恢复深度内寻找共同祖先，将旧区块和日志标记为非 canonical，使受影响的 Pending 和 Confirmed 充值转为 Orphaned，并回退相关游标；保留旧分叉审计记录，重新索引与计算确认，恢复过程可重启续跑；通过版本标记阻止旧 worker 写入；超深重组或无法确定祖先时暂停并要求对账。
- 对于重新上链的观察身份、关联和再确认历史：005 MUST 保留上述交接信息使 006 可判定；MUST NOT 擅自批准新的状态恢复策略；006 缺失时 005 保持停止。

## Success Criteria

### Measurable Outcomes

- **SC-01**：确认数处于 N-1 的 Pending 充值 100% 保持 Pending，无转换记录；处于 N、N+1 的 100% 各恰好一次转为 Confirmed。
- **SC-02**：N=1 且链头高度等于充值所在高度时，确认数计为 1 并 100% 转为 Confirmed。
- **SC-03**：非 canonical 引用、区块身份（高度 + 哈希）不匹配、链头缺失、链视图异常或暂停有效场景下，确认提交次数 100% 为 0。
- **SC-04**：正常追赶消除滞后后，符合条件的 Pending 100% 可被处理，无遗漏。
- **SC-05**：同一充值重复检查 2 次以上时，有效转换数恒为 1，确认时间与依据正确率 100%。
- **SC-06**：双 worker 并发确认同一 Pending 充值时，有效转换数恒为 1，确认时间与依据为首次成功者写入的值。
- **SC-07**：读取候选后、提交前发生暂停或链视图版本变化时，旧结果提交成功率 100% 为 0；调用方重读后可重新决策。
- **SC-08**：已 Confirmed 行经普通重复检查后，转换数不变、确认时间与依据不变（100%）；崩溃/重启后 100% 从持久化状态恢复，无遗漏、无重复转换。
- **SC-09**：任意 Confirmed 记录 100% 可追溯对应充值、所属区块、确认依据及转换时间；全部验收运行的错误输出中敏感凭据明文出现次数为 0，且无无限制原始数据转储。

## Assumptions (supplement)

- 阈值 N 为链级单值（v1），按链配置满足章程 IV；按资产差异化阈值不在本阶段范围（见 Explicit assumptions）。
- 确认检查驱动方式（tip 推进触发 / 周期轮询 / 混合）与实例竞争协调机制留给 plan（OQ1），但 SC-04 的不遗漏必须保留。
- 存储形状（是否复用 `deposit_observations.status` 扩展 CHECK 集合、确认时间与依据的列设计、是否需要独立确认游标）一律留给 plan（D3）；004 `status` 单值 CHECK 由 005 自有迁移扩展，004 语义不受影响。
- T023 证据提交 `3a79dfa` 是否进入 main 仍待核验（已核验：存在但不在 `origin/main` 祖先链），不能据此否定已确认的 PR #6 合并结果（`fd45e8b` 在 main 中已核验）；不阻塞规格完成。
- 004 acceptance 记录的未解决项（T000-P open、上游 003 E1 open、serve 端口占用者未知）按已合并文本引用，其当前解决状态未经本步骤核验，记为待核验实现，不阻塞规格完成。
- 003/004 的 T000-P 仍开放：005 不宣称生产就绪；生产就绪判定在 T000-P 关闭后另行进行。
