# Specification Quality Checklist: 009 Signer Service（签名隔离服务）

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-09-16
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — FR-01–FR-26 全部为行为语义（MUST/MUST NOT），表结构、锁、密码学库、传输协议、模块接口、字段命名显式留给 plan（FR-26）；Background 中的 Go/PostgreSQL/Anvil 为仓库既有环境描述与上游溯源，非需求约束，与 006/007 前例一致。
- [x] Focused on user value and business needs — US1–US6 按 P1/P2 排列，核心为"结构化签名、不签任意摘要、身份-内容绑定、密钥隔离、恢复期不签名、永不广播"；恢复期约束来自已批准的 006 契约，非技术自嗨。
- [ ] Written for non-technical stakeholders — 用户场景与验收场景使用 Given/When/Then 业务语言，但调用方集成者与安全审计者仍需理解 EVM 交易字段、uint256、nonce 概念。与 006/007 同属基础设施规格层级（operator/integrator/security-auditor-facing），属有意的规格层级选择，非疏漏。
- [x] All mandatory sections completed — User Scenarios & Testing、Requirements（Functional + Key Entities）、Success Criteria、Assumptions 均已填写；模板节顺序与标题保留；Non-Goals、Upstream Traceability、Downstream Handoff、Acceptance Mapping 并按仓库前例补充。

## Requirement Completeness

- [ ] No [NEEDS CLARIFICATION] markers remain — **按本步骤设计保留 3 处**（FR-05 OC-5、FR-07 OC-2、FR-13 OC-4；`grep -o '\[NEEDS CLARIFICATION'` 计数 = 3，配额用满）。三项均为 R3 共同契约中影响签名边界最关键、无合理默认的开放项；其余 OC-1/OC-3/OC-6/OC-7 仅以 Assumptions 开放契约项表 OPEN 行记录，未写入正文标记。待 `/speckit.clarify` 裁决后消除。
- [x] Requirements are testable and unambiguous — FR-01–FR-26 均为 MUST/MUST NOT 可测断言；不确定性已隔离在 3 处标记内，其余 23 条无歧义（拒绝类、绑定类、零副作用类、密钥隔离类均可本地验证）。
- [x] Success criteria are measurable — SC-01–SC-08 均为 100%/0 次/恰好 1 个的可计数断言，无"正确/安全/高效"裸词。
- [x] Success criteria are technology-agnostic (no implementation details) — SC 只谈请求行为结果（签名产出计数、拒绝计数、收敛计数、零广播/零副作用、零泄露），未提语言、框架、表结构、密码学库。
- [x] All acceptance scenarios are defined — US1–US6 共 27 个 Given/When/Then 场景（4/4/4/7/4/4），覆盖指令全部条目：结构化签名与任意摘要拒绝、认证与最小权限、身份-内容绑定与同 ID 异参、校验矩阵（链/sender/资产/转账内容/收款方/金额/费用）、密钥隔离与日志保密、恢复暂停继承与永不广播。
- [x] Edge cases are identified — 12 条：处理中二次到达、响应丢失、签名前后崩溃、密钥提供者不可用、存储不可用、重复投递、未知身份、开放契约项不得预设、OC-7 竞争窗口、恢复翻转竞态、任意摘要伪装、日志脱敏边界。
- [x] Scope is clearly bounded — Non-Goals 排除 nonce 分配（008）、构造/广播（010）、意图/worker（011）、余额/KYC/风控/冷热钱包、多链、新基础设施、上游供给入口设计、表结构与算法（plan）、T000-P；Downstream Handoff 仅单向声明。
- [x] Dependencies and assumptions identified — D1–D5 前置依赖 + Explicit assumptions + Upstream Traceability（含来源与基线 `d9096db`、Constitution 1.1.0）；R3 开放契约项表逐行 OPEN。

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria — FR-01–FR-26 均有 US/SC 对应（见 Acceptance Mapping）；其中 FR-05/FR-07/FR-13 含标记项，行为边界已锁定（"不得预设"），待 clarify 补全。
- [x] User scenarios cover primary flows — US1 签名主流程、US2–US5 安全/绑定/校验/密钥隔离（P1）、US6 恢复期契约与永不广播（P2）；失败路径由 Edge Cases 与 SC-08 覆盖。
- [x] Feature meets measurable outcomes defined in Success Criteria — SC-01–SC-08 与 US1–US6 及失败路径一一对应，可由验收场景直接验证。
- [x] No implementation details leak into specification — 存储形状、事务语句、并发机制、密码学实现、传输协议均显式留给 plan；密钥/金额/地址只谈行为语义。

## 009 Contract Conformance（本规格特有：上游承接核验）

- [x] 私钥隔离与签名边界 — FR-01/FR-20/FR-21：仅签名边界可访问密钥，KeyProvider 抽象，测试/部署密钥隔离（来源：Constitution VIII 与 Security Rules）。
- [x] 结构化请求、禁止任意摘要 — FR-02/FR-03：无完整交易内容一律拒绝（来源：Constitution VIII"结构化签名请求"；任务指令 009 范围）。
- [x] 校验矩阵 — FR-06–FR-12：链、sender、资产/转账内容、收款方、金额、费用策略（来源：Constitution VIII 策略清单）。
- [x] 身份-内容持久绑定、同 ID 异参拒绝、重试/丢响应/重启确定 — FR-13/FR-14/FR-15（来源：Constitution II/V/VI；006 版本隔离同构）。
- [x] 仅返回签名与哈希、永不广播 — FR-16/SC-06（来源：Constitution VIII；006 FR-26 广播边界）。
- [x] 006 恢复暂停继承 — FR-17 继承已批准的暂停范围与 downstream 前置检查，OC-6 仅记录接线待裁；US6 断言 0 签名、不改 006 治理（来源：006 FR-26、contracts/downstream.md）。
- [x] nonce 不分配、不预设绑定 — FR-18：只消费上游给定 nonce；OC-3 OPEN 不实现为既定事实（来源：006 FR-26；R3）。
- [x] 007 Guard — FR-19：Accepted ≠ 付款意图/执行授权；签名不由接收状态推定（来源：007 spec FR-08/FR-19、contracts/api.md §3）。
- [x] 日志保密 — FR-22/SC-05/SC-07：零密钥材料、零凭据、零原始已签名交易（来源：Constitution Security Rules、原则 XII）。
- [x] R3 表逐行 OPEN、无一行被写成已决定需求；3 处标记限于 OC-2/OC-4/OC-5 — 核验通过（verbatim diff 通过；标记计数 = 3）（来源：docs/workflow-008-009-parallel.md R1/R3/R5/R6）。
- [x] 测试替身限制与并行边界 — Assumptions 记录"测试替身不可替代最终集成验收"、"不读取/不覆盖 008 产物"（来源：R4/R5/R6）。

## Notes

- 本清单为 specify 步骤的质量清单。2 项 `[ ]` 为有意保留的真实状态：(1) 非技术读者友好度——基础设施规格层级（006/007 同例）；(2) 3 处 [NEEDS CLARIFICATION] 标记——本步骤按任务指令保留，待 clarify 裁决。除这 2 项外无未修复失败项。
- 真实计数（grep 核验）：NEEDS CLARIFICATION = 3；FR = 26；US = 6；验收场景 = 27；SC = 8；Edge Cases = 12；OC 表 7 行全部 OPEN 且逐字一致。
- Validation iterations run: 1（写后逐项核对 + 计数/verbatim 校验）；无因可修复问题触发的重写。
- 本步骤按任务约束**未写入 `.specify/feature.json`**（该文件在允许编辑范围之外）：下游步骤 MUST 使用显式 `SPECIFY_FEATURE_DIRECTORY=specs/009-signer-service` 定位（R6）。
- 上游依据：Constitution 1.1.0；006 spec FR-26/Downstream Handoff、006 contracts/downstream.md；007 spec FR-08/FR-16/FR-19、contracts/api.md；docs/workflow-008-009-parallel.md（R1–R7）；基线 commit `d9096db`。
- Readiness：**不可直接进入 `/speckit.plan`**；需先 `/speckit.clarify` 裁决 3 处标记（OC-2/OC-4/OC-5）与确认 OC-1/OC-3/OC-6/OC-7 处置；R1 门禁同时要求 008/009 完成 clarify 并闭合 R3 后才可并行实现。
