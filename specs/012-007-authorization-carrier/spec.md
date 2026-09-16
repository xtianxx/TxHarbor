# Feature Specification: 007 Authorization Carrier Supplement (PB)

**Feature Branch**: `012-007-authorization-carrier`

**Created**: 2026-09-16

**Status**: Draft

**Input**: 007-extension batch: formal carrier spec for the authorization scope
records 009's complete legal path depends on (scope identity/content/sender/
purpose/fee-range/validity/version linkage; controlled supply; re-issuance
traceability; revocation consistency; 009 read-only consumption boundary).

**Basis (read-only sources, no duplication of ownership)**:
- 009-signer-service spec/research/contracts at
  `.slim/worktrees/prep-009-signer` @ `8f75450` — R11 carrier closure,
  Q-A/Q-B rulings (2026-09-16), `contracts/gates.md` consumption boundary,
  PB-01–PB-05 task definitions (009 `tasks.md` §§Phase 0/Checkpoint).
- 007-withdrawal-creation spec (`specs/007-withdrawal-creation` @ `origin/main`
  `02641fb`): FR-03b grant model (逐笔授权记录：唯一授权 ID、caller_id、
  chain_id、资产、收款地址、金额；受控可审计供给入口；普通调用方不得写入),
  receive-only boundary (Accepted ≠ 执行授权；FR-08/FR-19).
- 009 spec FR-05 (OC-5, 2026-09-16): explicit per-transaction execution
  authorization with stable identity, verifiable authenticity, fee scope, and
  conditional fee-replacement reuse.
- Constitution I/II/V/VII/XII/XIV (financial correctness, explicit state,
  shared-variable infrastructure, observability, no upstream redefinition).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 合法签发：带 scope 的授权可被 009 完整验证 (Priority: P1)

授权签发主体通过受控 `withdrawal-authz supply` 入口一次写入 grant + scope
行（含身份、内容、sender、用途、费用范围、有效性、版本关联、attested_by）；
009 在同一 `FOR SHARE` 读序列中只读消费并完成首签与费用替换的全部核验，
不再回退到 `authorization_unverifiable`。

**Why this priority**: 没有它，009 合法路径不可达（009 T014/T030 的 PB-gate
已记录）；它是 PB-01–PB-03 的交付核心。

**Independent test**: 新 grant 自签发即可通过 009 独立验证（字段相等、
sender/fee-scope/purpose/version 一致），无需历史数据。

**Acceptance**: supply+revoke 往返集成测试绿；grant 内容 ↔ 审计可追溯；
009 对该 grant 的首签核验绿。

### User Story 2 - 无权拒绝与撤销/变更一致性 (Priority: P2)

无签发权限主体的供给被拒绝（零写入）；`RevokeGrant`/变更同步保持 scope
行状态/版本一致；撤销后 009 观测到的新状态阻止后续放行（已裁决竞争语义
OC-6/OC-7 由 009 侧门禁执行，本规格只保证载体侧状态一致）。

**Why this priority**: 真实性（Q-A）与有效性（expiry/revocation）是 OC-5
可验证性的支柱；与签发同等重要但可独立验收。

**Independent test**: 无权供给 → 拒绝且零行；revoke 后 scope 状态一致且
009 读到 revoked。

**Acceptance**: 权限负例测试绿；revoke 往返一致性断言绿。

### User Story 3 - 存量缺 scope 与重签发追溯 (Priority: P3)

无 scope 行的存量授权逐笔拒绝（`authorization_unverifiable`，009 侧执行，
本批次不实现拒绝逻辑）；有权限主体在重核验后显式重签发，新行追溯旧
grant/request，不创建第二意图/nonce，不静默换绑，不回填历史；存量无 scope
行保持可查可审、永不可执行。

**Why this priority**: Q-B 已裁决（逐笔拒绝、不强制回填）；它是历史业务
连续的唯一合法通道，但依赖 P1/P2 先成立。

**Independent test**: 存量 scopeless grant 可查、调用即拒；一次重签发演练
（dry-run 记录）含完整追溯链且无第二意图/nonce。

**Acceptance**: 重签发程序文档 + 一次演练记录；scopeless 行可查不可执行断言。

### Edge Cases

- supply 与 revoke 并发：同一事务写 grant + scope，revoke 保持版本一致；
  009 侧 `FOR SHARE` 顺序裁决竞争（009 已有，不在本批次）。
- scope 行缺失 vs grant 行缺失：前者逐笔拒绝（009 侧），后者按 007 既有
  `authorization_invalid` 语义（007 侧，不变）。
- 费用替换复用：仅当 scope 显式许可用途且费用在范围内；否则必须新授权 +
  新请求身份（009 FR-05 条件式规则，不重述为 fresh-only）。
- 迁移升级三序列（空库全链 / 007-era 升级 / down 后重 up）见 PB-05，
  规划期只定序列要求，不锁迁移编号（renumber-at-merge）。
- `--operator` 字符串出现在供给输入中：仅审计字段，不参与身份或权限判定。

## Requirements *(mandatory)*

### Functional Requirements

- **PB-FR-01**: 新增 `withdrawal_authorization_scopes` 载体（1:1，主键
  `authorization_id`，FK → `withdrawal_authorizations`），字段至少：
  `intent_id`、`request_id`（007）、`sender`、`fee_scope`（最大费用/tip 或
  范围）、`allows_fee_replacement`（显式用途标记）、`authorization_version`
  （每 grant 单调）、`attested_by`。无签名密码学字段（Q-A 已裁决；后续裁决
  要求时才加）。
- **PB-FR-02**: 载体行 MUST 由同一 supply 事务与 grant 同写；供给入口为既有
  受控 `withdrawal-authz supply` 的扩展 op-input；普通调用方与 009 MUST NOT
  可写；`RevokeGrant` MUST 同步 scope 状态/版本。
- **PB-FR-03**: 供给 MUST 由经认证且具签发权限的主体执行（复用 OS/部署/DB
  实际权限体系，明确控制点与信任边界）；`--operator` 声明仅审计字段，
  MUST NOT 替代认证与签发权限（Q-A）。grant 内容 ↔ 签发审计 MUST 一致可追溯。
- **PB-FR-04**: 无 scope 行/版本的存量授权在 009 侧逐笔拒绝，本批次不强制
  全量回填；重签发 MUST 由有权限主体重核验后显式执行并保留对旧 grant/request
  的追溯，MUST NOT 创建第二意图/nonce、静默换绑或回填历史（Q-B）。
- **PB-FR-05**: 费用替换复用授权仅当 scope 显式许可且费用在范围内，否则
  MUST 用新授权 + 新请求身份（009 FR-05 条件式规则；本规格只保证载体表达
  该条件，不重定义规则）。
- **PB-FR-06**: 007 仅接收边界不变：Accepted 不变成执行授权（007 FR-08/FR-19）；
  本批次 MUST NOT 修改 007 现有列、读形态、intake 语义；MUST NOT 追溯否定
  007 历史验收；现有 grant 在存储层保持有效（scope 行可选）。
- **PB-FR-07**: 载体及受控写入口由 007-extension 负责，009 只读消费，011 为
  后续消费者；本批次 MUST NOT 实现 009 消费逻辑或 010/011 能力。
- **PB-FR-08**: 迁移为纯加法 DDL；编号留待合并时按 `max(merged)+1` 确定
  （规划值 `000010`，renumber-at-merge；仅未应用编号可重排，已应用编号
  MUST NOT 改写）。本规格不锁死迁移编号与技术方案。

### Key Entities

- **授权 scope 行**：`(authorization_id)` 1:1 挂靠 grant 的 OC-5 属性载体；
  身份、内容、sender、用途、费用范围、有效性、版本关联的权威来源；
  009 只读消费，011 后续消费。
- **签发审计**：受控供给事务的操作记录；与 grant/scope 内容一致可追溯；
  `--operator` 为其中声明字段。
- **重签发链**：新 grant → 旧 grant/request 的显式追溯；不产生新意图/nonce。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **PB-SC-01**: 新签发 grant 的 009 独立验证通过率 100%（首签与费用替换
  路径各至少一次），`authorization_unverifiable` 出现次数为 0。
- **PB-SC-02**: 无权供给尝试写入行数为 0；revoke 后 scope 状态不一致次数为 0。
- **PB-SC-03**: 存量 scopeless grant 调用拒绝率 100%（009 侧执行）；
  重签发演练 1 次，追溯完整且新增意图/nonce 数为 0。
- **PB-SC-04**: 迁移三序列（空库 / 007-era / down-up）全部绿；已应用迁移
  编号被改写次数为 0。

## Assumptions

- Q-A/Q-B 为 2026-09-16 用户已批裁决（009 research R11 resolutions），本规格
  直接引用，不重开 clarify。
- PB-01–PB-05（009 tasks.md）为本批次的任务输入；本规格是其规格前置，
  不冒充其已完成独立规划（plan/tasks/analyze/implement 均未启动）。
- 009 工作区（`8f75450`）产物为只读引用；009 未合并，其 T028/T035 与本批次无关。
- 011 intent 创建/绑定语义沿用 OC-1/OC-5 已裁决；本批次不创建意图。
- 迁移编号、技术方案（表结构细节除外载体字段清单）、锁机制为 plan 留白；
  仅 PB-FR-08 的编号规则为例外（009 已批准 renumber-at-merge）。
- **OPEN（未决，需后续裁决，不自行批准）**：
  - OPEN-1：供给权限在具体部署中的主体映射（哪个 OS/DB 角色）留待 plan 明确。
  - OPEN-2：`fee_scope` 的精确表示（单上限 vs 范围）留待 plan，规格只要求覆盖“费用范围”。
  - OPEN-3：重签发演练的具体业务用例选择留待 implement 阶段。
- A-13 全链 E2E 与 T000-P 分别保持 OPEN，不在本规格关闭或改变含义。
