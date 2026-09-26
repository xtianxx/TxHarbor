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
