# Specification Quality Checklist: 008 Nonce Manager

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-09-16
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — 表结构、锁机制、对账算法、模块接口、状态命名与字段均显式留给 plan（FR-23、D-02、Assumptions）；FR-02 的并发保证以"持久层"行为语义表述，具体机制未锁定；上游记录名（恢复行、暂停、Accepted）仅作消费契约引用，与 006/007 前例一致。
- [x] Focused on user value and business needs — US1–US5 按 P1/P2 排列，核心为"不重复分配、绑定可恢复、未知结果不误判、分歧不静默复用"；对操作员（对账）、审计者（证据追溯）与下游阶段（009–011）的失败代价明确，非技术自嗨。
- [ ] Written for non-technical stakeholders — 基础设施规格层级（operator/auditor/downstream-facing）；nonce、sender、结果未知、RPC 等领域术语不可避免。与 006 同属有意的规格层级选择（006 清单同项亦未通过），非疏漏。
- [x] All mandatory sections completed — User Scenarios & Testing、Requirements（Functional + Key Entities）、Success Criteria、Assumptions 均已填写；附加节（Background/Goals/Scope、Dependencies & Assumptions、Non-Goals、Upstream Traceability、Downstream Handoff）齐备；模板节顺序与标题保留。

## Requirement Completeness

- [ ] No [NEEDS CLARIFICATION] markers remain — 3 个标记按编排指令有意保留（FR-01=OC-2、FR-03=OC-3、FR-04=OC-1；总数上限 3 用满），供 `/speckit.clarify` 裁决；其余 4 项以 OPEN 表行记录。不得为清单全绿而擅自裁决业务问题（R3）。
- [x] Requirements are testable and unambiguous — FR-01–FR-23 每条为 MUST/MUST NOT 可测断言；不依赖 OPEN 项的部分均可由验收场景与 SC 直接验证；依赖 OPEN 项的部分只锁定边界（可测）而非机制。
- [x] Success criteria are measurable — SC-01–SC-09 均为 0 次 / 100% / ≤1 / 有且仅 1 的可计数断言，无"正确/安全/可靠"裸词。
- [x] Success criteria are technology-agnostic (no implementation details) — SC 只谈可观察行为结果（分配、复用、回收、重指派、对账、暂停），未提语言、框架、表结构、锁机制。
- [x] All acceptance scenarios are defined — US1–US5 共 18 个验收场景，覆盖任务指令全部条目：chain+sender 隔离、并发唯一、重复调用复用原绑定、预留后崩溃、结果未知、重启恢复、latest/pending 分歧、缺口、外部消耗、禁止自动回收、禁止重指派、同 nonce 替换保原意图、006 暂停、007 边界。
- [x] Edge cases are identified — nonce 0/最大值边界、持久层不可用、恢复后重建、不稳定标识去重、对账处置证据标准、日志脱敏。
- [x] Scope is clearly bounded — Non-Goals 排除上游重定义、表结构/算法（plan）、签名/广播/执行（009–011）、OC-1/OC-4/OC-5 裁决、账本/风控、多链、新基础设施、T000-P；OC 表逐行 OPEN。
- [x] Dependencies and assumptions identified — D1–D9 前置依赖、显式假设、OC 表（7 行 OPEN）、Assumptions 与 Deferred decisions；来源含基线 `d9096db`、006（`8e1a440`）/007（`19fa11e`）合并点与 Constitution 1.1.0。

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria — FR-01–FR-23 与 US/SC 对应（US1→FR-01/02/05；US2→FR-06/07/13；US3→FR-07/08/09；US4→FR-10/11/12；US5→FR-14/15/16/17；横切→FR-19–FR-23）；OPEN 相关条款以边界形式保留可测性。
- [x] User scenarios cover primary flows — US1 并发预留与复用、US2 崩溃/重启（P1）、US3 未知结果/替换、US4 分歧/缺口/外部消耗（P1）、US5 006/007 承接（P2）。
- [x] Feature meets measurable outcomes defined in Success Criteria — SC-01–SC-09 与 US/FR 一一对应，可由验收场景直接验证。
- [x] No implementation details leak into specification — 存储形状、锁语句、事务划分、对账载体与字段命名均显式留给 plan；未决事项以 OPEN 行/标记呈现，未以机制发明填充。

## 006/007 Contract Conformance（本规格特有：上游承接核验）

- [x] 006 恢复暂停承接 — FR-14：恢复启动后分配一律暂停；恢复完成后的分配须同时满足 008 自有门禁（来源：006 spec FR-02/FR-26、Downstream Handoff）。
- [x] 006 在途未知/不得重付承接 — FR-07：结果未知保留并继续查询，不得视为失败重付或重新分配（来源：006 FR-26 操作矩阵、contracts/downstream.md）。
- [x] 006 独立暂停不清除 — FR-14 + SC-07：恢复完成不得清除 008 独立暂停（来源：006 Downstream Handoff）。
- [x] 006 检查 vs 副作用边界 — FR-15：不得声称一次检查能撤回已发出的 RPC；隔离机制归属后续 plan，OC-7 只记边界（来源：006 contracts/downstream.md）。
- [x] 007 仅接收边界承接 — FR-16/FR-19：不因请求存在而分配/签名/广播；008 不持有密钥（来源：007 spec FR-08/FR-19）。
- [x] 007 Accepted ≠ 付款意图/执行授权 — FR-04/FR-16、US5-3、SC-08：显式守卫条款，已定（来源：007 spec Downstream Handoff、contracts/api.md §4；任务守卫要求）。
- [x] 未裁决事项保持 OPEN — OC 表 7 行逐行 OPEN；3 个标记仅对应 OC-1/OC-2/OC-3；OC-4–OC-7 仅作边界引用，未写成已定机制（R3）。

## Notes

- 未通过项与原因（真实计数）：
  1. "Written for non-technical stakeholders" — 基础设施规格层级的有意选择（沿 006 先例），不通过但不改写。
  2. "No [NEEDS CLARIFICATION] markers remain" — 3 个标记为编排指令明确要求（上限 3，供 clarify 裁决）；本步骤不得自行裁决业务问题。
- 清单计数：模板 16 项中 14 通过；上游承接核验 7 项全部通过；合计 **21/23**。
- 验证迭代：1（specify 自检逐项核对；无因非标记项失败而改写规格，原因同上）。
- 本步执行记录：before_specify hook 按编排指令尝试一次即失败（目标分支 `008-nonce-management` 已存在且被 `/tmp/opencode/txharbor-008-nonce-management` 工作目录占用，HEAD `19fa11e`；未使用 `--allow-existing-branch`，未切换/删除/重置任何分支或工作目录）；spec 直接编制于 `prep/008-nonce`（`d9096db`）后停止。详见编排报告。
- 本步未写 `.specify/feature.json`（任务护栏禁止编辑 `specs/008-nonce-manager/` 以外路径；R6 要求下游显式使用 `SPECIFY_FEATURE_DIRECTORY=specs/008-nonce-manager`，不得默认依赖该指针）。
- 补执行记录（主编排定向收尾，非首次正常执行）：首次 hook 因错误分支参数（传入 `GIT_BRANCH_NAME=008-nonce-management`，与授权值 `008-nonce-manager` 不符）在预期外失败（exit 1）；spec 与本清单已先行提交于 `32fe006`。本次在 `32fe006` 上以授权参数补执行 hook 成功：`SPECIFY_FEATURE_DIRECTORY=specs/008-nonce-manager GIT_BRANCH_NAME=008-nonce-manager bash .specify/extensions/git/scripts/bash/create-new-feature-branch.sh --json --short-name "nonce-manager" "008 nonce-manager: ..."`，返回 `{"BRANCH_NAME":"008-nonce-manager","FEATURE_NUM":"008"}`，exit 0；工作目录分支由 `prep/008-nonce` 切换至新建 `008-nonce-manager`（HEAD 仍为 `32fe006`，`32fe006` 为其祖先，产物完整保留，工作区干净）。未重生成 spec，未使用 allow-existing-branch，未伪造状态。
- Readiness：需先经 `/speckit.clarify` 裁决 OC-1/OC-2/OC-3（3 个标记），方可进入 `/speckit/plan`；OC-4–OC-7 可随 clarify/plan 继续。plan 输入：绑定载体（OC-3）、锁与事务划分、状态命名与算法、对账证据标准、fail-closed 形态、数值表示。
