# Contract: Action × Permission × Scope Matrix (014)

**Spec**: [spec.md](../spec.md) (FR-011/012/020/024, Q2) | **Reuse**: 009 401/403、`signer/policy`；011 `can_execute` + `execution_ops_audit(operation_id)`；012 `--operator` 仅审计；013 `event_ops_audit(operation_id)` + PD-4 单人授权不自动扩大；005/006/011 operator+reason+evidence 先例。

| Action | Permission | Scope | Notes |
|---|---|---|---|
| query/view | 认证 + 范围读取权限，按权限脱敏 | 获授权范围 | 无匿名查询；拒绝亦审计 |
| start/pause/resume scan | 对账扫描管理权限 | 获授权范围内 014 任务 | 不得暂停上游资金流程 |
| claim | 对应范围异常处理权限 | 单差异 | 仅归属，不授予执行权 |
| dispose ack_only | 具体动作执行权限 + reason | 单差异 | 记录范围/理由/证据/结果 |
| dispose reuse_recovery | 同上 + 该既有能力原有授权与门禁 | 单差异 | 014 不代行授权；operator/理由/证据≠授权 |
| dispose new_fix_rule | 本阶段仅 dry_run | 单差异 | 真实修复未批准 |
| reverify (auto) | 系统（有界只读复核） | 单差异 | 仅 Q1 范围 + 014 自身记录 |
| verify-close | 闭合权限 + 最新 consistent 未过期 | 单差异 | 不完整/过期/未知/仍分歧不得强制关闭 |
| risk-accept/ignore | 未批准 | — | 不提供该功能；需业务阻塞另裁 |

认证与授权机制复用方式留实现，但越权拒绝 MUST 可测并审计；既有更严审批保留。

## Evaluation Source（F5；求值源裁决）

- 主体认证复用既有认证机制（API key 中间件模式）；权限求值源为 014 专属 `recon_permission` 表（见 `data-model.md` §1.10），按（principal， action， scope）精确匹配，默认拒绝。
- 不可复用依据：`execution_caller_permission`（`migrations/000012:171-179`，caller_id 主键）是 011 提款执行固定接口权限，009 签名授权系密钥材料域，012 `--operator` 仅审计——三者语义均为其域内，复用即自动扩大为对账权限，Q2 已禁止。
- 未配置行、未知动作、越界范围一律拒绝并审计。权限授予为部署期运维行为，本阶段不预置任何授予；若未来需要新增业务授权政策（如审批链、角色包），属业务裁决事项，本轮仅回报，不自行裁决。

## Management（R2；管理授权面）

- 复用依据：011 `withdrawalexec.go:1-136` 本地特权操作面（唯一可写 CLI、`execOperatorOp` 审计＋`operation_id` 去重、`permission-set/permission-revoke :41-43/220-336`）——014 权限授予/撤销/查询沿用同一模式：本地执行、认证主体绑定、`--operator/--reason/--operation-id` 必填、逐行审计。不新建权限管理平台。
- 主体绑定：`principal` 取自认证调用者身份（复用 API-key 认证中间件模式），禁止手工填写；`operator/reason/evidence` 自由文本仅为审计注记（012 先例），`operation_id` 仅解决幂等：三者均不构成管理授权依据。
- 管理权归属：授予/撤销 014 权限须经本地特权操作路径（与被管权限正交）；普通 014 权限持有者不得自行授予（含自己），首次授予亦须走该路径并审计。撤销在提交后即时生效（后续求值不命中即拒绝）；历史审计保留。
- 待业务裁决（具体问题，不自设管理员）：首次授予与高风险授予是否需要双人审批（第二人复核），或本地特权单人＋审计已足够？在裁决前，管理操作面保持“本地特权单人＋三字段审计”的既有先例语义，不扩大、不新增角色。
