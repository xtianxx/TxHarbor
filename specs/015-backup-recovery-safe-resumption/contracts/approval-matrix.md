# Contract: Approval Matrix — Roles, Identity Binding, Approvals (015)

**Spec**: [spec.md](../spec.md) (FR-023, FR-024; 2026-09-28 裁决) | **Design**: [data-model.md](../data-model.md) §1.2/§1.3/§1.8/§3.1 | **Reuse**: 014 `contracts/auth-matrix.md` Management 模式（本地特权路径 + 主体绑定 + `operation_id` + 审计）；011 `withdrawalexec.go` `execOperatorOp`

适用边界：**仅 015 灾备后复服**。不改变日常运行，不改变 014 已批权限，不新增紧急绕过/管理员强制复服入口。

## 1. 动作 × 权限 × 审批档（闭集）

| 动作 | 所需权限 | 审批档 | 备注 |
|---|---|---|---|
| `instance-open` / `restore` / `verify`（核验写入） | `recovery_execute` | 无独立批准（属执行） | 实例记录 executor；恢复与核验操作全审计 |
| 读取核验结果 | `recovery_verify_read`（限定授权范围） | 无 | executor 可读自己实例的核验结果 |
| 批准/撤销复服（查询、链扫描、充值确认、非真实下游范围的事件发布/消费） | `recovery_approve` | **single_non_executor**（一名非执行者 + 审计） | 只能开放所列能力；不得间接启动签名/广播/付款/真实下游副作用 |
| 批准/撤销复服（**新提款创建、既有提款恢复、向真实下游投递、可产生真实下游业务效果的消费恢复**） | `recovery_approve` ×2 | **dual_non_executor**（两名不同、且均非本实例执行者的已授权人员） | 两人必须 `person_id` 不同；同一人双账号不算两人 |
| 释放/撤销放行（release/revoke） | `recovery_execute`（写入） | 校验对应审批记录；无有效批准不得 release | release 为派生判定的载体，不是独立特权 |
| 实例关闭 | `recovery_execute` + 全部能力有效 release | 若本次实例发生过资金/投递能力 release，关闭需 dual 批准 | 任一能力未放行（含缺口）不得关闭（[data-model.md](../data-model.md) §4.1） |
| 参与者注册 / 身份映射维护 | `recovery_control_manage` | 部署期特权路径（单主体 + 审计；本阶段不强制第二人） | 首次信任根 = 受控部署配置；无配置默认拒绝；不预置任何主体；单特权维护**不能自证双人映射真实性**（F19）：变更审计+相关既有批准失效重批；保守 dual 不抵消错映射 |
| 读取状态/审计 | 认证 + 范围读取权限 | 无 | 拒绝亦审计 |

## 2. 身份绑定来源（禁止自由填写替代认证）

- 主体 = 认证调用者身份（`<kind>:<id>`，复用既有认证/部署配置模式）；CLI 侧来自部署受控配置（`TXHARBOR_RECOVERY_PRINCIPAL`），并必须命中 `recovery_participant` 注册。
- `--operator`/`--reason`/`--evidence` 等自由文本**仅为审计注记**；`operation_id` 仅解决幂等；两者都不构成授权或批准依据。
- 同人多账号：`recovery_identity`（person↔principal）为部署期受控维护；**principal 无映射 → 无法证明不同人 → 拒绝**（dual 与 single 的执行者排除同规则）。
- 执行恢复、读取核验、批准复服为独立权限；不要求三人互斥；**executor 不得批准自己的实例**（即使具有 `recovery_approve`）。
- **映射变更影响（F19）**：`recovery_identity` 变更（新增/改写/撤销）立即使以该映射为依据的既有批准失效、须重核重批；审批有效性按**当前映射**重算——批准行记录的 `person_id` 与当前映射不一致 → `approval_identity_unverified` 拒绝；映射维护为部署受控特权路径+审计（`source`/`recorded_by`/`recorded_at`），单特权维护无法由 015 证明双人真实性（程序/人的边界，不虚构自动检测），**保守 dual 不抵消错映射**（映射缺失/冲突→0 放行）。

## 3. 批准记录与生效条件（FR-023）

每条批准/撤销记录必须含：授权主体与权限、能力、范围（`scope_hash`：链/资产/业务类型/能力）、理由、依据证据的 **代次 + 证据哈希**、时间与结果；写入 `recovery_approval`（append-only，`operation_id` UNIQUE）。

生效（由门禁派生评估，非行上布尔）：

1. 实例 open；principal 已注册且具 `approver` role；
2. 执行者排除：`person_id ≠ executor.person_id`；
3. dual 档：两名不同 principal 且 `person_id` 互异；
4. 代次/哈希与实例当前值一致——**任何 `evidence_generation`/`evidence_hash` 变化立即失效**（由门禁求值器在控制库行锁内读取权威值发现；不等待 TTL，缓存不得复用旧代次结果）；控制库不可达 = fail-closed 拒绝（F5）；
5. 无该 principal 的更新 `revoke`；
6. 审批**不能**覆盖硬门禁：缺证、未隔离、未知付款结果、开放缺口、既有资金门禁激活等一律不放行；双人批准亦同。

## 4. 撤销、失效与重复请求

- 撤销 = 显式 `revoke` 行（有授权、有理由、审计）；撤销后门禁立即重算为拒绝。
- 证据变化/门禁失效：旧批准 100% 失效、停止后续执行、重核重批；重核重批本身幂等。
- 重复批准/重复 release/重复 close：按 `operation_id` 读回，不产生第二次副作用、不翻转状态。
- 中断重入：重复有界调用继续推进；无内存态、无"半批准"。
- 未准入 vs 已在途（F5）：未准入=拒绝、不产生动作；已在途=按原门禁处理，未知结果按 unknown 纪律（永不视为未执行），不追溯中止已提交工作。
- 术语（F16）：`release`（派生放行，每次求值重算）与 `approve`（批准记录，仅条件输入）不得混用；批准不构成放行，放行必须由 `release_valid` 派生为真。

## 5. 明确缺席（不在本阶段交付，不提供入口）

- 风险接受后强制复服、损失核销、人工补偿付款、自动补造付款意图：**无设计、无权限行、无契约、无任务**；若需要属业务阻塞，另行业务裁决。
- 紧急绕过、管理员强制复服、"双人批准覆盖缺证"：**禁止**（无实现路径）。
- 本契约不授权任何签名/广播/付款/既有恢复能力；调用仍须独立满足原有授权与门禁（009/010/011/012/014）。
