# Specification Quality Checklist: 007 Withdrawal Creation & Query

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-09-15
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — 表结构、锁、并发算法、模块接口、字段名与序列化形状明确留给 plan（FR-12/FR-13/FR-15）；金额/地址/键的内部表示只谈行为语义；上游记录名（pause 表、恢复行）仅作消费契约引用，与 002–006 前例一致。
- [x] Focused on user value and business needs — US1–US6 按 P1/P2 排列，核心为"受认证授权的幂等接收 + 本人可查"；恢复期约束（US6）来自已批准的 006 契约，非技术自嗨。
- [ ] Written for non-technical stakeholders — 用户场景与验收场景使用 Given/When/Then 业务语言，但调用方集成者仍需理解幂等键、uint256、EVM 地址概念。与 006 同属基础设施规格层级（operator/integrator-facing），属有意的规格层级选择，非疏漏。
- [x] All mandatory sections completed — User Scenarios & Testing、Requirements（Functional + Key Entities）、Success Criteria、Assumptions、Non-Goals、Upstream Traceability、Downstream Handoff 均已填写；模板节顺序与标题保留。

## Requirement Completeness

- [ ] No [NEEDS CLARIFICATION] markers remain — 计 16 处标记（FR-03/06/07/09/10/11/14/15/18 及 US/Edge 引用），均为无上游已批准依据的真实业务选择（认证机制、金额/地址规范化、键格式、比较参数集、保留期、响应编码、404/403 策略、上游供给方式）。按任务指令不执行 clarify、不自行假设消除，已如实保留，留待用户下一步决定。
- [x] Requirements are testable and unambiguous — FR-01–FR-21 每条均为 MUST/MUST NOT 可测断言；拒绝类、幂等行为类、恢复期零副作用类均可在 Anvil + PostgreSQL 本地环境验证（SC-01–SC-08 对应）。
- [x] Success criteria are measurable — SC-01–SC-08 均为 100%/0 次/有且仅 1 个的可计数断言，无"正确/安全/高效"裸词。
- [x] Success criteria are technology-agnostic (no implementation details) — SC 只谈请求行为结果（持久化、返回、拒绝、收敛计数、零副作用），未提语言、框架、表结构、锁机制。
- [x] All acceptance scenarios are defined — US1–US6 共 21 个验收场景，覆盖任务指令全部条目：正常创建及本人查询；未认证/无授权/越权；错链/非白名单/非法地址/零负小数超范围；同键同参/异参/跨调用方；并发/丢响应/重启/持久化失败；恢复期创建查询与零副作用。
- [x] Edge cases are identified — 覆盖保留期到期、金额字符串变体、地址大小写、处理中二次到达、非法标识、存储超时 vs 失败、恢复翻转竞态。
- [x] Scope is clearly bounded — Non-Goals 排除 nonce/签名/广播/确认跟踪/worker、余额账本/KYC/风控/冷热钱包、多链、新基础设施、表结构与算法（plan）、T000-P。
- [x] Dependencies and assumptions identified — Assumptions + Upstream Traceability（含来源文件与基线 SHA `8e1a440`、Constitution 1.1.0）；供给方式待澄清项已标记（FR-18），但"不自建账本"已定。

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria — FR-01–FR-21 均有 US/SC 对应；响应编码等确切映射待澄清，但行为语义已定、可测。
- [x] User scenarios cover primary flows — US1 主流程、US2–US5 安全/校验/幂等/故障（P1）、US6 恢复期契约（P2）。
- [x] Feature meets measurable outcomes defined in Success Criteria — SC-01–SC-08 与 US1–US6 对应，可由验收场景直接验证。
- [x] No implementation details leak into specification — 存储形状、事务语句、并发机制、载体选择均显式留给 plan；键/地址/金额的规范化决策标记为业务澄清而非机制发明。

## 006 Contract Conformance（本规格特有：下游承接核验）

- [x] 仅接收语义承接 — FR-08/FR-16 明确 Accepted 仅表示已接收；FR-19 禁止本阶段 nonce/签名/广播与执行模块（来源：006 FR-26、downstream.md）。
- [x] 恢复期零副作用 — SC-06 要求 0 次 nonce/签名/广播；FR-16 要求观察 006 前置检查适用子集（来源：006 downstream.md）。
- [x] 查询有效性边界 — FR-16/US6-2 要求区分请求持久化事实与未来链上执行状态，不包装混合视图（来源：006 FR-18、observability.md）。
- [x] 未知语义保留 — FR-17/SC-07 禁止重建付款意图补偿（来源：006 操作矩阵、在途未知条款）。
- [x] 不擅改恢复治理 — FR-16 明确不改变暂停/授权/隔离规则（来源：006 FR-23/FR-26）。
- [x] 下游单向声明 — Downstream Handoff 仅记录 008–011 待承接，未创建其规范，明确声明未做双向核对。

## Notes

- 本清单为 specify 步骤的质量清单。标记为 `[ ]` 的两项均为有意的、按指令保留的状态：非技术读者友好度（基础设施规格层级）与 NEEDS CLARIFICATION 标记（真实业务歧义，IN-007-Q1–Q8，见下）。DO NOT 为清单全绿擅自裁决。
- 待业务澄清清单（IN-007-Q1–Q8，建议选项与影响见报告正文，不冒充已批准要求）：
  1. IN-007-Q1（FR-03）：认证机制与身份来源。
  2. IN-007-Q2（FR-03/FR-18）：业务授权证明载体与上游供给方式。
  3. IN-007-Q3（FR-09）：幂等键格式。
  4. IN-007-Q4（FR-10/FR-06/FR-07）：幂等比较参数集与金额/地址规范化。
  5. IN-007-Q5（FR-11）：幂等保留期与到期语义。
  6. IN-007-Q6（FR-14）：响应编码映射。
  7. IN-007-Q7（FR-15）：未找到 vs 无权访问的可观察区分。
  8. IN-007-Q8（FR-15/US6-2）：查询返回确切字段与恢复期标注映射（部分留给 plan，部分需业务确认措辞）。
- 上游依据：006 spec FR-02/FR-18/FR-23/FR-26、006 contracts/downstream.md、006 contracts/observability.md（基线 `8e1a440`）；003/004/005 白名单与确认语义；Constitution 1.1.0。
- Validation iterations run: 1（specify 内自检：模板节完整性、FR 可测性、SC 可数性、006 契约逐项核对、标记计数 16）。
- Readiness：规格已就绪进入 clarify（8 个业务问题待用户决策）；plan 输入已分离（存储/并发/字段形状留给 plan，不阻塞 clarify）。
