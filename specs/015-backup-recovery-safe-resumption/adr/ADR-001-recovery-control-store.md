# ADR-001: Recovery Control Store and Resumption Carrier

**Date**: 2026-09-28 | **Spec**: [spec.md](../spec.md) (FR-009–FR-013, FR-022–FR-024) | **Status**: Accepted (plan scope) | **Related**: [research.md](../research.md) §3/§4, [data-model.md](../data-model.md) §1/§3

## Context

被恢复的数据 DB 正是回滚对象：其中的批准/撤销/授权行会随备份复活或丢失（旧批准重现、撤销丢失、旧授权复活），**不能作为恢复控制事实的自证来源**。同时：宪章 III 要求持久金融状态以 PG 为真源、禁止本地文件成为金融状态唯一真源；宪章 XIII 要求不无理由新增服务/平台；spec FR-009/SC-003 要求运行期拒绝（不是纯运维流程）；014 的 `recon_permission` 位于数据 DB、语义域也不对。

## Decision

1. **恢复控制事实存入独立 PostgreSQL 控制库** `txharbor_recovery_control`（独立 DSN，默认可与数据 DB 同实例的另一 database），不进入数据 DB 备份/恢复集，独立保留；schema 独立 goose 版本化；控制库 schema 版本未知/不兼容 → 拒绝（T069）。控制库丢失 = fail-closed 隔离（无批准即无放行），并在实例级灾难时按再引导流程重建控制事实后才可能放行。**控制库自身回退纪律（F6）**：禁盲恢复；受支持恢复 = 停机隔离＋显式重建/supersede＋审计（新实例 ID、执行者失去旧权限的确认、部署受控配置+恢复点证据+重新隔离清单、旧库 retired）；不得以回退库内自写 supersede 冒充可检；015 不提供自动检测，「旧批准不重生效」仅就数据 DB 回滚域成立（控制库回退域保持限定，见 DG-4）。
2. **复服闸门 `internal/recovery.Gate`**：无 open 恢复实例时按正常态放行（不改变日常运行）；存在 open 实例时，接线入口默认拒绝，能力放行为**派生评估**（实例+代次+隔离检查+缺口+审批+依赖+既有门禁），无可直写放行布尔；有界 TTL 缓存，控制库不可达即拒绝；缓存代次感知（代次/哈希变化立即失效，发现者=求值器）；signer 等独立进程以受支持装配＋进程内检查点（T070）为边界。
3. 载体形态：核心库 + 薄 `recovery-admin` CLI（本地特权路径、`operation_id` 审计幂等、主体绑定），**不新增常驻服务/监听/UI/调度器**；数据 DB 本阶段零 schema 变更。

## Rationale

- 回滚域独立是 015 的存在理由：控制事实不能随数据 DB 一起回滚；同实例另一 database 已覆盖主威胁模型（逻辑恢复/回退），实例级同失按 fail-closed 处理且明示不虚构独立性。
- PG 控制库保留事务/行锁/UNIQUE 能力，可复刻 014 §3.1 代次令牌协议；本地文件账本跨主机协调与持久性弱，仅保留为证据附件（哈希入控制库）。
- 自建控制服务/K8s/管理台违反 XIII；复用 014 Management 模式（本地特权 + 主体绑定 + 审计）已够用。

## Consequences

- 所有 7 类入口新增动作前门禁校验与状态暴露；控制库不可达时 gated 能力 fail-closed（正确性优先于可用性），运维需监控控制库。
- 部署需配置控制库 DSN 与身份映射；单人部署无法满足非执行者批准（FR-023），列为部署前人员配置要求（另见 plan.md 待裁决）；身份映射变更立即使相关既有批准失效重批（F19）。
- 程序边界（不得声称运行时全覆盖）：`reconcile-admin`/`events-admin`/外部调度不在运行期门禁接线内（程序边界：停服+审计+`no_pre_release_effects`）；signer 非受支持装配直调、控制库盲恢复/旧副本不自动检测（T025/T028/T064/T070）；同实例储层同失超出逻辑回滚域（F14/DG-3），备份落盘/异地待裁决。
- 若形态不满足验收，plan 回报调整，不得削弱要求；取舍记技术计划，必要时补 ADR。

## Alternatives

- 数据 DB 内控制表：rejected（回滚自证不可信，旧批准/撤销随备份回流）。
- 本地 append-only JSONL 账本作为唯一权威：rejected（违反 III 精神、跨主机协调弱）；降级为证据附件。
- 纯运维流程（不建运行期门禁）：rejected（FR-009/SC-003 要求请求级拒绝）。
- 常驻控制服务/管理台：rejected（XIII 无理由新服务边界）。
