# Data Model: 015 Backup Recovery and Safe Service Resumption (Phase 1)

**Branch**: `015-backup-recovery-safe-resumption` | **Date**: 2026-09-28 | **Spec**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Research**: [research.md](research.md) | **Contracts**: [contracts/](contracts/)

Design only; no implementation, no migration, no product code in this round.

**存储分层（关键设计决定）**:

- **数据 DB（被恢复对象）**：本阶段 **零 schema 变更**（不新增 `migrations/0000NN`）。015 不把任何控制事实写回数据 DB，避免"控制信息随备份回滚复活/丢失"（research §3）。
- **恢复控制库（新增，独立 DSN）**：`TXHARBOR_RECOVERY_CONTROL_DSN` 指向独立 PostgreSQL database（同名实例另一 database 即满足逻辑回滚域独立；实例级灾难同失 → fail-closed 重建）。schema 独立 goose 版本化（`recovery-admin migrate`），下表以 `recovery_` 前缀命名。**控制库不是金融真源**（宪法 III）：它只承载恢复治理事实（实例/人员/证据/批准/放行/审计/演练），丢失即默认隔离。控制库 schema 版本未知/不兼容 → 拒绝（fail-closed，T069）；控制库自身旧副本/盲恢复不提供自动检测——恢复纪律见 §4.4。
- 金额字段：本模型不承载任何金额；出现能力范围引用一律引用数据 DB 对象，不复制金额。

## 1. 实体与字段（控制库）

### 1.1 `recovery_instance`（恢复实例；FR-009/010/022/023）

- `instance_id` UUID PK；`kind` CHECK(`recovery`,`baseline`——`baseline` 仅用于文档化部署态，生产闸门只 arm `recovery`）；`state` CHECK(`open`,`closed`；关闭仅在全能力 released 后允许，见 §4.2）；`supersedes_instance_id` UUID nullable；`restore_point` JSONB（manifest 引用 + 快照元数据，见 §7）；`data_target` JSONB（目标 DSN 指纹：database/role 指纹，**不存明文凭据**；脱敏后入库）；`evidence_generation` BIGINT NOT NULL DEFAULT 0（§5）；`evidence_hash` TEXT NOT NULL（当前证据快照聚合哈希）；`opened_by/opened_at`、`closed_by/closed_at`、`reason` TEXT。
- 约束：`CREATE UNIQUE INDEX ... ON recovery_instance (state) WHERE state='open'`（全局至多一个 open 实例）；实例 ID 只增不改、禁止复用（无更新 instance_id 的路径）；`evidence_generation` 只允许单调 +1。
- `instance_id` 由控制库生成，**不来自数据 DB 内容**；旧实例 ID 在数据 DB/旧控制库里的残留对当前实例惰性（§3 条件）。

### 1.2 `recovery_participant`（实例参与者；FR-023 执行/核验/批准分离）

- `instance_id` FK；`principal` TEXT（认证调用者身份，格式 `<kind>:<id>`，禁用自由填写）；`person_id` TEXT（经身份映射解析；映射缺失时 NOT NULL 不成立 → 拒绝注册）；`role` CHECK(`executor`,`verifier`,`approver`；一人可多 role，但批准有效性另行排除 executor）；`bound_at`；`binding_source` TEXT（`deploy_config`/`auth_principal`）；`proof_ref` TEXT（审计/部署配置引用）。
- PK(`instance_id`,`principal`,`role`)；注册是部署期受控写入（`recovery_control_manage`，见 contracts/approval-matrix.md）。

### 1.3 `recovery_identity`（人员↔principal 身份映射；FR-023 同人多账号）

- `person_id` TEXT、`principal` TEXT，PK(`principal`)；`source` TEXT（部署受控配置/身份源）；`recorded_by/recorded_at`；`active` BOOL（撤销映射也留行）。
- 语义：双人批准要求两人 `person_id` 不同；**principal 缺映射 → 视为无法证明不同人 → 拒绝**。本表只由部署期特权路径维护，永不来自被回退数据库内容。

### 1.4 `recovery_evidence`（证据快照/批次；FR-006/014/031）

- `evidence_id` UUID PK；`instance_id` FK；`generation` BIGINT（接受时实例代次）；`kind` CHECK(`backup_manifest`,`restore_probe`,`verification_batch`,`isolation_check`，…闭集)；`scope` JSONB（链/资产/业务类型/能力/对象范围）；`artifact_hash` TEXT（证据文件 SHA-256）；`artifact_ref` TEXT（`docs/evidence`/`.evidence` 类路径引用或控制库内快照）；`observed_at`；`collected_by`；`created_at`。
- **实例绑定语义（DG-2 决议，F7）**：`instance_id` 必须绑定 open 实例；`verify-backup` 先于实例开启时先落 manifest 级验证结论，实例开启后经受控命令显式同步绑定并记录审计（**不得谎称实例绑定**）；`scope` 记录 `backup_id` 与 `target_fingerprint`；复制 manifest、重建目标实例或更换目标指纹后该证据不再适用——必须重跑 `verify-backup`/`restore_probe`，不得沿用旧 `verified`。
- 接受写入的同一事务推进 `recovery_instance.evidence_generation` 并刷新 `evidence_hash`（§5）。

### 1.5 `recovery_verification_item`（核验项；FR-014/015/018）

- `item_id` UUID PK；`instance_id` FK；`generation` BIGINT（写入时实例代次）；`category` CHECK(`V1`…`V9`，见 contracts/verification-items.md)；`object_key` TEXT（稳定对象身份：request_id/intent_id/tx_hash/block/consumer…）；`scope` JSONB；`sources` JSONB（每来源的观察值/位置/时点）；`conclusion` CHECK(`consistent`,`divergent`,`unknown`,`stale`)；`reason` TEXT；`evidence_refs` TEXT[]；`observed_at`；`created_at`。
- 只追加；重核写新行（latest-row-wins 仅在代次校验通过后成立，§5）。`unknown`/`stale` 必须带 `reason` 且不得带空 `sources`。

### 1.6 `recovery_gap`（证据缺口；FR-016/019）

- `gap_id` UUID PK；`instance_id` FK；`object_key`/`scope`；`timeline` JSONB；`existing_evidence` JSONB；`required_evidence` JSONB；`affected_capabilities` TEXT[]（闭集能力名；至少包含直接关联能力）；`dependency_proof` JSONB nullable（独立性论证：路径 + 逐边证据引用；无证明则所有依赖者保守纳入）；`state` CHECK(`open`,`closed`,`escalated`)；`closed_by/closed_at/closure_evidence`；`owner`；`escalation_ref`；`created_at`。
- 缺口闭合只能由新证据触发；`timeout`/`attempts_exhausted`/`acknowledged` 不是状态（作为审计理由记录，不改变 `state`）。
- 能力放行条件包含"无 `open` 缺口将本能力列入 `affected_capabilities`"。

### 1.7 `recovery_isolation_check`（隔离检查项；FR-010/011/012）

- `check_id` UUID PK；`instance_id` FK；`item_key` TEXT（闭集，见 contracts/resumption-gate.md §Isolation Checklist）；`state` CHECK(`pending`,`evidenced`,`verified`,`rejected`)；`evidence_ref` TEXT；`checkpoint_summary` JSONB（如租约 takeover/fencing token、进程/主机证据、网络隔离证据——**状态记录不能单独作为证明**，必须带外部证据）；`checked_by`/`checked_at`；`verified_by`/`verified_at`（`verified_by` 必须 ≠ 本实例 executor）；`created_at`。
- 能力放行要求其**依赖集**全部 isolation check = `verified`。

### 1.8 `recovery_approval`（批准决策；FR-023/024）

- `approval_id` UUID PK；`instance_id` FK；`capability` TEXT（闭集 7 能力）；`scope_hash` TEXT（规范化范围哈希）；`decision` CHECK(`approve`,`revoke`)；`approval_class_snapshot` CHECK(`single_non_executor`,`dual_non_executor`)；`principal`/`person_id`；`reason` TEXT；`evidence_generation` BIGINT；`evidence_hash` TEXT；`operation_id` TEXT UNIQUE（幂等）；`supersedes_approval_id` UUID nullable；`created_at`。
- append-only。批准有效性（派生，不在行上存布尔）：见 §3。`revoke` 是覆盖本 principal 先前 `approve` 的显式记录；其他人批准不受影响。

### 1.9 `recovery_release`（放行/撤销决策；FR-021/022/024）

- `release_id` UUID PK；`instance_id` FK；`capability`；`scope_hash`；`decision` CHECK(`release`,`revoke`)；`evidence_generation`/`evidence_hash`；`approval_refs` UUID[]（本放行所依据的批准行；确定性排序）；`operation_id` TEXT UNIQUE；`supersedes_release_id` UUID nullable；`reason`；`created_at`。
- append-only。当前有效放行 = 最新 `release`（未被同能力 `revoke` 覆盖）且 §3 条件全部成立；该条件在**每次门禁求值时重算**，不存在可直写的 `released=true` 布尔（防作弊，FR/SC 反作弊纪律）。

### 1.10 `recovery_audit`（审计；FR-023/026/032）

- `audit_id` BIGSERIAL PK；`instance_id` nullable；`actor`（认证 principal）；`action` TEXT；`target` JSONB；`detail` JSONB；`result` CHECK(`ok`,`refused`,`discarded`,`failed`)；`refusal_class` TEXT nullable（§3.4 枚举）；`evidence_generation`、`operation_id`；`created_at`。
- append-only；拒绝/丢弃亦记行；查询按实例+时间。

### 1.11 `recovery_drill_run`（演练度量；FR-030/031/036）

- `drill_id` UUID PK；`instance_id` FK；`scenario` TEXT（含 7 类失败注入标识）；`recovery_point` JSONB；`db_restore_seconds`、`verification_seconds` NUMERIC（分开记录）；`capability_release_seconds` JSONB（per capability）；`backup_lag`、`uncovered_interval` JSONB；`constraints_configured` BOOL（未配置必需约束时不得宣称生产目标）；`test_inputs` JSONB（本地演练值标注用途）；`gap_counts` JSONB；`result` CHECK(`ok`,`refused_safe`,`failed_injected`)；`log_ref`；`created_at`。
- 不写生产阈值承诺；时间口径分开，禁止用 `db_restore_seconds` 单独宣称 RTO 达标。

## 2. Backup Manifest（文件模型，非控制库表；FR-001–004/006）

- 位置：备份产物目录内，与 dump 同身份同目录；**manifest 是唯一选择依据**（禁止按文件名/mtime/人工记忆挑选）。
- 字段（JSON，稳定排序、规范化后哈希）：
  - `manifest_version`、`backup_id`（UUID，生成时分配，全局唯一）、`created_at`、`created_by`（principal）；
  - `carrier` = `{"kind":"pg_dump_custom","pg_server_version":"18.x","pg_dump_version":"18.x"}`；
  - `coverage`：**覆盖声明** = 数据 DB 权威对象清单（链游标/区块/事件/充值/确认/提款请求/付款意图/出站交易/签名广播记录/nonce/outbox/义务标记/消费者幂等与进度/审计与权限/014 记录）；**排除声明** = 非权威状态（Redis/Kafka 数据、缓存）与**密钥材料**（Signer 私钥/真实凭据永不入库备份；PG 内仅存凭据哈希）；
  - `recovery_point`：`{"snapshot":{"xmin":…,"xip":[…],"xmax":…},"wal_lsn":"…","wall_clock":"RFC3339","server":"…","database":"…"}`（research §2 唯一口径）；
  - `schema`：`goose_db_version` 精确应用集 + 缺失/未知版本处理；`program`：生成时二进制版本标识 + 最低兼容标识；
  - `artifacts`：`[{"path":…,"bytes":…,"sha256":…}]`（含 dump；可扩展分片）；
  - `verification`：`{"state":"unverified|verified|rejected","verified_at":…,"verifier":…,"target":"isolated","checks":{readable,structure_constraints,business_state_probes,verification_executable},"evidence_ref":…}`；
  - `retention_class`/`note`（部署配置；无生产数值）。
- 校验规则：字段缺失/哈希不符/覆盖声明缺失/兼容性未知 → 不可用；`verification.state != verified` → 不得用于恢复与复服（FR-006）。损坏/截断/部分写入检测 = 文件长度+哈希+`pg_restore -l` 列表可读。

## 3. 门禁派生评估（唯一放行判定；FR-021/022/023/025）

对（当前 open 实例 `I`，能力 `C`，范围 `S`）：

```text
release_valid(I, C, S) :=
    I.state = 'open'
 && C ∈ {query, chain_scan, deposit_confirmation, existing_withdrawal_recovery,
         new_withdrawal_creation, event_publishing, event_consuming}
 && requires_capabilities(C) 全部 release_valid(I, ·, ·)              -- 能力依赖（§3.3）
 && isolation_dependency_set(C) 全部 state='verified'                 -- §3.2 依赖集
 && NOT exists open gap G: C ∈ G.affected_capabilities                -- 缺口阻塞
 && exists latest release row R for (I,C,S): R.decision='release'
      && R.evidence_generation = I.evidence_generation
      && R.evidence_hash = I.evidence_hash
      && R 未被同 (I,C,S) 的更晚 'revoke' 覆盖
 && approvals_valid(I, C, S)                                          -- §3.1
 && existing_fund_gates(C) 在真实动作处仍全部通过                       -- FR-025，不被本门禁替代
```

**两阶段判权（F20）**：`existing_fund_gates(C)` 是动作处第②阶段的引用；本门禁为第①阶段（recovery allow），两阶段均须通过，禁止以本门禁替代/合并/短路原门禁。

### 3.1 `approvals_valid`

- 高影响能力（dual）：存在两条决策序最新的 `approve` 行（不同 principal），且：两人都有 `approver` role、**两人的 `person_id` 互不相同**、**两人的 `person_id` 均 ≠ executor.person_id**、两行 `scope_hash`/`evidence_generation`/`evidence_hash` 与当前一致、两行 `person_id` 与当前身份映射一致（映射变更→旧批准失效重批，F19）、均未被各自 `revoke` 覆盖。
- 其余能力（single）：存在一条满足同条件的 `approve` 行（非执行者、映射完整）。
- 映射缺失、principal 未注册、role 不符、执行者自批、代次不符、映射不一致、含 revoke → 不满足。**任何硬门禁不因审批通过而改变**。

### 3.2 能力依赖与隔离依赖集（闭集，写死在代码/契约，不靠配置放宽）

| 能力 | requires_capabilities | isolation 依赖项（全部 verified） |
|---|---|---|
| `query` | — | 旧实例停止/网络隔离/版本兼容 |
| `chain_scan` | — | 旧实例停止 + 写者 fencing 观察 + 版本兼容 |
| `deposit_confirmation` | `chain_scan` | 同 chain_scan（充值确认依赖已索引链事实） |
| `existing_withdrawal_recovery` | `chain_scan` | 旧执行者停止 + 执行/签名/广播路径隔离 + 授权面再核验 |
| `new_withdrawal_creation` | `existing_withdrawal_recovery` | 同 existing_withdrawal_recovery（不得只开入口） |
| `event_publishing` | `chain_scan` | 旧 publisher 停止 + outbox/义务核验 + broker 目标范围确认 |
| `event_consuming` | — | 旧 consumer 停止 + 幂等/进度核验 + effect class 范围确认 |

- `new_withdrawal_creation → existing_withdrawal_recovery` 是保守固有依赖（禁止"只开入口不备处置"）；依赖图变化属规格变更，不在配置面放开。

### 3.3 求值来源与失败分类

- 求值只读控制库（实例行 + 最新决策 + 检查项 + 缺口 + 参与人/映射）；**绝不读数据 DB 授权行作为放行依据**。
- 有界 TTL 缓存（部署配置）；**代次感知失效（F5）**：缓存键含实例+能力+scope+代次+哈希；任何代次/哈希变化立即失效（发现者=求值器，下一次真实动作前求值即拒绝，不得等 TTL）；缓存过期且控制库不可达 → 拒绝。
- **单动作准入协议（R3）**：缓存命中与未命中均在**动作准入前**于共同锁（实例行锁，与决策写入同一锁）内读取权威 `(state, evidence_generation, evidence_hash)` 并按代次协议校验——**键含代次≠最新性证明**；缓存只复用仍有效的计算结果，不得用旧 allow 跳过本次授权检查；控制库不可达即拒绝；顺序=判定点（锁内）→释锁→实际动作，**一次准入=当次调用单个明确动作**，不得跨请求/循环步进/批次/异步重试复用；撤销先于准入→拒绝，准入后撤销→在途＋unknown（不追溯中止已提交工作、不回滚）并阻止后续准入；**缓存命中不得被追认为在途**；不宣称跨系统原子；区分「未准入」（拒绝）与「已在途」（按原门禁处理、未知结果按 unknown 纪律）。
- 控制库 schema 版本未知/不兼容 → 拒绝（fail-closed；以 `control_store_unavailable` 表达并审计注记版本，T069）。
- 拒绝分类（`refusal_class` 闭集）：`no_instance`（正常态不拒绝，仅标记 normal）、`instance_mismatch`、`no_release`、`release_invalidated_generation`、`release_revoked`、`capability_dependency_closed`、`isolation_unproven`、`gap_open`、`approval_missing`、`approval_identity_unverified`、`approval_executor_excluded`、`approval_stale`、`hard_gate_active`、`control_store_unavailable`、`scope_mismatch`。全部 fail-closed，全部审计。

## 4. 状态机

### 4.1 恢复实例

```text
(created) --instance open--> open
open --close(要求: 全部 7 能力当前有效 release)--> closed
open --supersede(仅显式、双人批准、审计)--> closed + 新 open 实例（新实例 ID）
```

- `close` 不允许在任一能力为 blocked/缺口未闭合时发生（否则等于把未闭合能力带回日常态放行）。若某能力因缺口无法开放，实例保持 open、该能力保持关闭、升级人工处置——**不交付风险接受**（spec 澄清 3）。
- 关闭后回到日常运行（不 arm）；`closed` 实例记录不可复用。

### 4.2 能力三态（restored / verified / released；FR-022；术语统一 F16）

- `restored`：存在 `recovery_evidence(kind=restore_probe)` 且结构/约束/兼容性/关键可用性检查通过；**不蕴含** verified/released。
- `verified`：该能力证据范围（V1–V9 中适用项）已有代次=当前的核验结论；允许存在 `unknown`（此时不可 released）。
- `released`：`release_valid` 为真（派生放行，每次求值重算）；`approved` 不是状态——它仅指 `recovery_approval` 存在有效批准记录（§3.1），不单独构成放行；spec FR-022 的 `approved` 状态即本设计的 `released`。
- `status` 输出逐能力显示三态与阻塞原因；`restored` 不得显示为 `verified`，`verified` 不得显示为 `released`（FR-026）。

### 4.3 核验项/缺口/隔离项/批准/放行

- 核验项：append-only；`consistent|divergent|unknown|stale`；重核写新行。
- 缺口：`open → closed`（仅新证据）/`escalated`；不因超时/知悉变化。
- 隔离项：`pending → evidenced → verified`（或 `rejected`）；`rejected` 表示证据不足重新收集。
- 批准：`approve`/`revoke` append-only；有效性 §3.1 派生。
- 放行：`release`/`revoke` append-only；有效性 §3 派生。

### 4.4 控制库自身的恢复纪律（F6；不提供自动检测）

- **禁止盲恢复控制库**（按备份/快照直接回退控制库并继续使用）。
- 受支持的控制恢复 = 停机隔离（执行者执行＋**非执行者**核验隔离项）→ 显式重建/`supersede`＋审计：新实例 ID 只增；执行者失去旧权限的确认（旧 DSN/凭据吊销或新库新凭据，记录时间与主体）；建新实例的可信依据（部署受控配置＋恢复点证据＋重新隔离清单）；旧库标 retired。
- **不得以「在已回退库内写 supersede 行」作为可检/已重建证明**；015 不提供对盲恢复/旧副本的自动检测（程序边界，DG-4）。
- 「旧批准不自动重生效」仅就数据 DB 回滚域成立（控制域独立＋`instance_id` 绑定）；控制库回退域的普适声明保持限定，若要求普适保证须另立 ADR/裁决。

## 5. 证据代次与写写反序协议（FR-024；复用 014 §3.1 教训）

- 取证（事务外）：读取 `(I.state, I.evidence_generation, I.evidence_hash)` 作为令牌。
- 提交（事务内）：`SELECT ... FOR UPDATE` 锁实例行，重读令牌并逐项校验；不符 → 丢弃结果，仅写 `recovery_audit(result=discarded)`，不写结果行、不改缺口、不推进代次。
- 接受写入：同一事务内插入结果行 + `evidence_generation = evidence_generation + 1` + 刷新 `evidence_hash`；因此所有绑定旧代次的批准/放行立即失效（fail-closed）。
- 触发代次推进的写入：核验批次结果、缺口闭合/建立、隔离项 verified/rejected、restore_probe 接受、证据快照接受，以及 `restore_started` 预写失效标记（restore 在首次目标写入前提交，旧证据/批准立即不可再用于准入；失败/中断不回退该代次）。
- **不靠 `created_at`/进程时钟裁决**；同秒并发以提交序（自增序/锁内重读）为准。

## 6. 幂等与审计

- 所有命令携带 `operation_id`：`recovery_audit`/决策表以 UNIQUE + 读回实现"同输入同结果、异输入冲突零写"（同 011 `execOperatorOp`/013 `event_ops_audit` 先例）。
- 中断重入 = 重复有界步进：`backup`/`verify-backup`/`restore`/`verify`/`release`/`close` 均可重复调用收敛；`restore` 的中断残留按 §7 处置。
- 审计行包含 actor/action/target/detail/result/refusal_class/operation_id/时间；拒绝与丢弃必须留痕。
- 有界只读复核（F13）：可读范围（实例/授权 scope 内证据、核验、缺口、审计）＋次数/时间/资源预算（部署配置）；耗尽 → 拒绝后续复核＋审计、不改变任何状态；超时、耗尽、人工知悉 ≠ 缺口闭合或获准复服。

## 7. 恢复执行模型（restore；FR-005/008；不新增数据 DB schema）

- `restore` 前置：实例 open；manifest `verified` 且兼容；恢复依赖检查（目标 DSN 可达/权限、RPC 事实来源、broker/控制库可达、Signer 边界可达但**不获取私钥**）；任一缺失 → 明确阻塞状态，不宣称恢复完成。
- 目标：显式 `--target-dsn`（默认仅允许隔离目标；生产主库恢复必须显式声明并记录）；目标库为空或显式重建；`pg_restore --clean --if-exists` 于**新建/重建 database** 上执行。
- 中断：不得标记 restored；重试 = 重建目标 database 后重跑（幂等，避免同一权威状态叠加双份/混合）；残留状态可识别（目标 DB 指纹 + 审计）。
- 完成后探针（restore_probe）：`CheckCompatibility` 通过；关键表存在且约束完整；代表性只读查询可执行；014/013 元数据表可读；**`business_state_probes` 逐项对照 FR-002 九类权威对象**（链身份/游标、事件、充值确认、提款请求与付款意图、出站交易与签名/广播、nonce、Outbox/义务标记、消费者幂等/进度、审计/权限/014 差异）抽样并声明覆盖边界，不能证明的类别标 `unknown` 且不计入 `restored`；目标实例重建/目标指纹变化 → 重跑探针，不得沿用旧结论。通过才写 `restored` 证据；随后进入 V1–V9 核验。
- 私钥边界：恢复工具只检查 Signer 边界可达性，**不接触私钥/真实凭据**；manifest/日志/审计按既有 `logx.Redact` 脱敏，DSN 不落库不入日志。

## 8. 核验目录（V1–V9 摘要；明细见 contracts/verification-items.md）

| # | 对象 | 主要来源（只读） | 阻塞能力（直接关联） |
|---|---|---|---|
| V1 | 链事实 vs PG（区块/日志/确认/reorg） | RPC canonical + `chain_blocks/erc20_transfer_logs/*_checkpoint/reorg_recovery` | chain_scan、充值相关能力 |
| V2 | 提款请求/付款意图/授权 | `withdrawal_requests/payment_intents/withdrawal_authorizations` + 07 审计 | 新提款创建、既有提款恢复 |
| V3 | nonce 分配/占用 | 008 表 + `eth_getTransactionCount`；`txlifecycle` 只读 | 既有提款恢复（发射路径） |
| V4 | 签名/广播结果（含 unknown） | `signing_requests/signature_results/tx_attempt_signings/tx_send_attempts/tx_receipts/tx_reconciliations` + 链回执 | 既有提款恢复 |
| V5 | Outbox 与义务标记 | `outbox_events` + `event_obligation` + publisher 进度 | 事件发布 |
| V6 | 消费者幂等/进度/隔离 | `consumer_inbox/versions/progress/quarantine` + broker offsets（可读时） | 事件消费 |
| V7 | 014 差异/复核/处置/权限/审计 | 000016 表只读引用 | 与 V2/V5/V6 重叠部分 |
| V8 | 授权面漂移（回退后撤销/发放） | 控制库记录的外部真源证据 / 再核验/再施加结果 | 资金与投递能力 |
| V9 | 工具/依赖就绪 | 镜像/客户端/DSN/Signer 边界可达性 | 全部（恢复前置） |

- 结论规则与"缺失≠从未发生/unknown≠通过"见 contracts/verification-items.md。

## 9. 不变量与校验（验收锚点）

- INV-1：任一时刻全局至多一个 open 实例（DB 部分唯一索引）。
- INV-2：放行 = 派生评估为真；不存在可被直写的放行布尔；直写决策表而缺条件不产生放行（反作弊验收）。
- INV-3：批准/放行必须绑定当前代次与证据哈希；证据变化后旧批准 100% 失效。
- INV-4：高影响能力的批准不含执行者、不含同一 `person_id` 的两人。
- INV-5：证据/控制库不可读 → 门禁拒绝，不存在默认放行路径。
- INV-6：能力放行不得解锁/替代既有资金门禁（FR-025）；`existing_fund_gates` 在任何真实动作处独立校验。
- INV-7：核验/放行不触发付款、签名、广播、真实下游投递；未知结果永不视为未执行。
- INV-8：缺证/缺口未闭合的能力保持关闭；关闭实例前不存在未决能力（§4.1）。
- INV-9：恢复工具不携带/不导出私钥与真实凭据；证据与日志脱敏。
- INV-10：度量口径分开（恢复/核验/各能力放行），未配置必需约束不得宣称生产恢复目标。
- INV-11：控制库 schema 版本未知/不兼容 → 拒绝（migrate 与门禁读取均 fail-closed，T069）。
- INV-12：V1–V9 只读 accessor 白名单；核验/探针执行前后权威表零写（F4/F10）。
- INV-13：门禁缓存不跨代次复用（代次/哈希变化立即失效）；放行=两阶段判权（本门禁 AND 动作处原门禁），禁替代（F5/F20）。
