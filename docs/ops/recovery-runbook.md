# 015 Recovery Runbook（备份恢复与安全复服运维手册）

- Feature: `015-backup-recovery-safe-resumption` ｜ 设计: [plan.md](../../specs/015-backup-recovery-safe-resumption/plan.md) · [quickstart.md](../../specs/015-backup-recovery-safe-resumption/quickstart.md) · [contracts/](../../specs/015-backup-recovery-safe-resumption/contracts/) · [data-model.md](../../specs/015-backup-recovery-safe-resumption/data-model.md) · [ADR-001](../../specs/015-backup-recovery-safe-resumption/adr/ADR-001-recovery-control-store.md)
- 证据归口: [docs/evidence/015/README.md](../evidence/015/README.md)
- **Gate: T000-P 保持 OPEN**。本文档不是发布，不宣称生产就绪；文内所有示例数值均为**本地测试输入**，生产 RPO/RTO/备份频率/保留期未裁决（FR-036），不得据本地值宣称符合生产恢复目标。
- 本文档描述的是 015 受支持路径。风险接受后强制复服、损失核销、人工补偿付款、自动补造付款意图**不存在**（无命令、无权限行、无契约）；双人批准不得替代缺失证据（FR-019）。
- 适用边界：仅 015 灾备后复服；不改变日常运行，不改变 014 已批权限，不新增紧急绕过/管理员强制入口（FR-023）。

角色边界（2026-09-28 裁决）：执行恢复（`recovery_execute`）、读取核验（`recovery_verify_read`）、批准复服（`recovery_approve`）为独立权限；恢复执行者可读授权范围内核验结果，但**不得批准/核验自己执行的实例**；新提款创建、既有提款恢复、向真实下游投递及可产生真实下游业务效果的消费恢复需**两名不同且均非本实例执行者**的已授权人员共同批准；其余能力由一名非执行者批准并审计（`contracts/approval-matrix.md` §1）。

---

## 1. 配置键（示例中的值均为本地测试输入）

| 键 | 用途 | 必填性 | 本地测试示例（非生产值） |
|---|---|---|---|
| `TXHARBOR_RECOVERY_CONTROL_DSN` | 独立恢复控制库 DSN（恢复模式总开关）；与数据 DSN 相同 → 拒绝；明文 DSN 不落库/不入日志/不入证据 | 启用 015 时必填 | `postgres://txharbor:txharbor@127.0.0.1:5432/txharbor_recovery_control?sslmode=disable` |
| `TXHARBOR_RECOVERY_PRINCIPAL` | 部署受控调用主体 `<kind>:<id>`；必须命中有效身份映射与参与者注册；自由文本（`--operator/--reason`）仅审计，永不授权 | 每个命令必填（受控配置） | `deploy:recovery-exec-1` |
| `TXHARBOR_RECOVERY_ARTIFACT_DIR` | `backup` 默认产物目录（dump + manifest） | 可选（`--out` 可覆盖） | `/tmp/opencode/015-artifacts` |
| `TXHARBOR_RECOVERY_INSTANCE` | 把进程/命令绑定到一个实例 UUID；命令 `--instance` 与之不一致 → 拒绝 | 恢复期推荐 | `11111111-1111-1111-1111-111111111111` |
| `TXHARBOR_RECOVERY_GATE_TTL` | 门禁求值缓存 TTL；控制库配置后必填（**无默认**，非法/缺失按名拒绝） | 控制库配置后必填 | `5s` |
| `TXHARBOR_RECOVERY_EFFECT_CLASS_RULING` | 受控部署规则：effect-class token（= scope 的业务类型 `kind`）→ `real_downstream` / `no_real_downstream_effect`；未配置/未知一律**保守 dual**；调用者不得自声明降档 | 可选（不配则事件能力恒 dual） | `{"no-real-downstream":"no_real_downstream_effect"}` |
| `TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_<CATEGORY>` | 每类证据（V1…V9）新鲜度容忍；缺失/非正 → 该类别最多 `unknown`（不是通过） | 核验前部署裁决 | `TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_V4=10m` |
| `TXHARBOR_RECOVERY_VERIFICATION_BATCH_LIMIT` | 单次 `verify` 有界步进的条目上界；无默认，超界→拒绝且零写入 | `verify` 必填 | `200` |
| `TXHARBOR_RECOVERY_STATUS_TIMEOUT` | 一次 `status` 有界只读复核的墙钟预算；无默认 | `status` 必填 | `10s` |
| `TXHARBOR_RECOVERY_STATUS_MAX_READS` | 一次复核可发出的有界读取/求值次数 | `status` 必填 | `200` |
| `TXHARBOR_RECOVERY_STATUS_MAX_ROWS` | 单次有界读取可返回的行数 | `status` 必填 | `2000` |
| `TXHARBOR_RECOVERY_RPO_TARGET` / `_RTO_TARGET` / `_BACKUP_FREQUENCY` / `_RETENTION` | 可配置目标/频率/保留（FR-036）；**未配置 = 显式未配置状态**，不得宣称符合生产目标 | 可选 | `10m` / `30m` / `1h` / `168h`（仅测试输入） |
| `TXHARBOR_PG_DSN` / `TXHARBOR_RPC_URL` / `TXHARBOR_CHAIN_ID` | 数据 DB / 事实来源 / 链身份（核验与演练读取） | 核验/演练必填 | `postgres://…/txharbor` / `http://127.0.0.1:8545` / `31337` |

说明：必需值缺失或非法一律**按精确键名拒绝**，没有内置默认；`SDK/CLI` 帮助里的全部键名以 `txharbor recovery-admin <action> --help` 输出为准。

## 2. 真实命令路径（S1–S12）

前置（一次性）：`txharbor recovery-admin migrate up`（只作用于控制 DSN）；参与者注册 `txharbor recovery-admin control participant-register --instance ID --principal <kind>:<id> --role executor|verifier|approver --operator ... --reason ... --operation-id ...`；身份映射 `control identity-map-set --principal <kind>:<id> --person-id P --operator ... --reason ... --operation-id ...`（映射变更→相关旧批准失效、须重核重批）。

| # | 步骤 | 真实命令 | 关键期望/边界 |
|---|---|---|---|
| S1 | 生成备份 | `txharbor recovery-admin backup --chain-id 31337 --out DIR` | 产物 + manifest；`verification.state=unverified`；选择只按 manifest 身份 |
| S2 | 实际恢复验证 | `txharbor recovery-admin verify-backup --manifest M --target-dsn ISOLATED [--instance ID]` | 真实 `pg_restore` 到隔离库；四项检查通过才 `verified`；备份级验证 ≠ 实例级 `restore_probe`；复制/重建/换目标必须重跑 |
| S3 | 开启恢复实例 | `txharbor recovery-admin instance-open --kind recovery --reason R` | 新 `instance_id`、全局至多一个 open、executor=opened_by；所有能力默认关闭 |
| S4 | 恢复 | `txharbor recovery-admin restore --manifest M --target-dsn TARGET --instance ID [--declaration isolated\|production_main]` | 前置：实例 open + 控制库存在绑定 `backup_id` 的验证证据；预写失效标记推进代次；探针通过才 `restored`（不得显示 verified/released） |
| S5 | 隔离核验 | `checklist-set --instance ID --item ITEM --evidence-ref REF` + `checklist-verify --instance ID --item ITEM`（非执行者） | 六项：`old_writers_stopped/writer_fencing_observed/network_isolation/version_compatible/no_pre_release_effects/authorization_recheck`；仅状态/摘要不是证明；无法证明 → 相关能力保持关闭 |
| S6 | 事实核验 | `txharbor recovery-admin verify --instance ID --scope all` | 有界 V1–V9 步进；外部领先 → `divergent/unknown`；缺口生成（对象/范围/时间线/所需证据/受影响能力） |
| S7 | 缺口阻塞 | 对受影响能力执行 `release --instance ID --capability C --scope 'capability=C;chain=31337'` | 拒绝（`gap_open` 等闭集类）；证据包可导出；超时/知悉不改变状态 |
| S8 | 独立能力放行 | `approve --instance ID --capability query --scope 'capability=query;chain=31337'`（非执行者 single）→ `release` 同参 | 仅 `query` 放行；链扫描/付款/投递仍拒绝；`approve` 不是放行（F16） |
| S9 | 分级推进 | 按依赖 `chain_scan` → `deposit_confirmation` → `existing_withdrawal_recovery` 逐项 approve/release；高影响能力（新提款创建/既有提款恢复/默认档的事件发布与消费）需双人 | 每项独立条件与审计；每项能力（含每个 scope 流）分别放行 |
| S10 | 状态诚实 | `txharbor recovery-admin status [--instance ID] [--scope S]`；`GET /status/degradation`；`GET /readyz` | 逐能力显示 restored/verified/released 与阻塞原因；三态互不冒充；未核验/回退数据不显示为正常一致；健康探针不替代资金门禁 |
| S11 | 关闭实例 | `txharbor recovery-admin instance-close --instance ID` | 全 7 能力当前有效 release 才可关闭；任一能力含未闭合缺口 → 拒绝并保持 open、升级人工 |
| S12 | 度量记录 | `txharbor recovery-admin drill --manifest M --target-dsn TARGET --instance ID --chain-id 31337 [--scenario full_recovery]` | 恢复点/DB 恢复时间/核验时间/各能力放行时间/backup_lag/uncovered_interval 分开记录；未配置约束标未配置；本地值=测试输入 |

幂等：所有命令带 `operation-id` 时同输入同结果读回、异输入冲突零写；重复 restore/verify/approve/release/close ≥10 次零状态翻转。`--scope` 必须是规范化能力 scope（键排序；示例 `capability=query;chain=31337`）；旧 opaque/跨能力/非规范串一律 `scope_mismatch` 拒绝。

## 3. 隔离证明

1. **控制库独立性**：`TXHARBOR_RECOVERY_CONTROL_DSN` 指向独立 database（可与数据 DB 同 PG 实例的另一 database）；控制 DSN 与数据 DSN 相同 → `migrate`/命令拒绝；**控制库不在数据 DB 的备份/恢复集内**（备份只覆盖数据 DB 权威对象；控制事实不随数据回滚复活或丢失）。
2. **门禁 fail-closed**：控制库不可达、schema 版本未知/不兼容 → gated 能力拒绝（`control_store_unavailable`），无降级放行；进程绑定实例但未配置控制库 → 启动拒绝；无 open 实例 → 正常态直通（不改变日常运行）。
3. **隔离检查项**：`isolation_dependency_set(C)` 全部 `verified` 前，能力不得放行。执行者采集证据（`checklist-set --evidence-ref`），非执行者确认（`checklist-verify`）；状态记录/摘要/口头确认不是证明；`verified_by` 必须 ≠ 本实例 executor。
4. **程序边界（F3，不得声称运行时全覆盖）**：`reconcile-admin`（claim/dispose/reverify/scan/start/resume）、`events-admin`（replay/unblock/retention-prune）、外部定时调度与 `withdrawal-exec` 操作员路径**不在 015 运行时门禁接线内**。这些路径的隔离靠：停服/下线、访问与凭证移除（记录时间与主体）＋门禁审计＋`no_pre_release_effects`。**仅 checklist 签署不构成运行时隔离证明**；无法证明已停止/已失去权限 → 相关能力保持关闭，不得以超时/失联/人工知悉放行。若需运行时强制，另立 ADR/设计。
5. **`no_pre_release_effects` 的确认方式**：以门禁审计为准——实例 open 后 0 次外部可见动作（无 `ok` 的准入、outbox/consumer 未推进）；不得以状态字段自证。
6. **在途交错（OpenInstance 边界）**：*同一次调用*在 `instance-open` 前获准、在其后执行，不属于跨调用复用，也不产生新的准入审计。因此：隔离流程必须**等待/排空**这类在途动作（按各动作原门禁：nonce hold、发送/执行门禁、claim/lease、inbox/version 守卫，未知结果按 unknown 处理），把它记录进隔离证据包并在确认其已终结后才允许确认 `no_pre_release_effects`。**不得只以「下一次求值会拒绝」证明该交错已覆盖**；无法确认终结 → 相关能力保持关闭。
7. **Signer 边界**：受支持的 `signer-serve` 装配受门禁 + 进程内检查点覆盖；受支持装配之外的直调/蓄意伪造门禁不自动检测，不得宣称任意 in-process 调用全覆盖（T070 边界）。

## 4. 失败处置（F1–F7）与升级

| # | 注入 | 可观察结果 | 收敛动作 | 升级/归属 |
|---|---|---|---|---|
| F1 | 备份不可用/损坏/截断/未验证 | `verify-backup`/`restore` 拒绝；`verification.state≠verified` 不得用于恢复/复服 | 重跑 `verify-backup`（真实隔离恢复）；修复产物来源；禁止「尽力恢复」 | 备份责任 + 部署裁决（保留/频率） |
| F2 | 恢复中断/部分完成 | 不得标记 `restored`；预写失效标记使旧批准/放行不可再用 | 重建目标 database 后幂等重跑 `restore`；检查无双重/混合状态 | 执行者；重复失败→升级 |
| F3 | 版本/schema 不兼容 | 恢复/复服明确拒绝；0 次静默降级/自动改写 | 使用兼容备份或受控升级程序；禁止改写数据继续 | 部署/发布责任 |
| F4 | 外部事实领先恢复点 | V1–V9 `divergent/unknown`；缺口生成；受影响能力保持关闭；0 重付/重广播/错误事件效果 | 关闭缺口只能靠新证据；历史 signed bytes/广播/意图/重复投递重发一律拒绝 | 人工处置证据包 + 业务裁决 |
| F5 | 旧实例未隔离/无法证明 | 写入/发送/投递 100% 拒绝并列出缺失证据；程序边界路径需停服/权限移除证据 | 完成 §3 隔离项；无法证明 → 保持关闭 | 运维隔离 + 升级 |
| F6 | 核验发现无法证明的缺口 | 保持 `unknown/pending`；不当作「从未发生/可执行」；不新建意图/补写幂等记录 | 收集外部证据闭合；有界只读复核（`status`）耗尽/超时 ≠ 闭合 | 责任归属 + 升级记录 |
| F7 | 越权/证据不足/过期批准 | 100% 拒绝并审计；证据变化后旧批准失效 | 重核重批；身份映射变更→旧批准失效重批；保守 dual 不抵消错映射 | 审批人配置（部署前裁决） |

补充负例（本轮 analyze 增，不改 F1–F7 编号）：

- **控制库盲恢复**：禁止按备份/快照直接回退控制库继续使用（§6）；在回退库内自写 `supersede` 行**不构成**重建证明。
- **控制库 schema 版本未知/不兼容**：`migrate` 与 store 读取均拒绝（`control_store_unavailable` + 版本审计注记，T069）。
- **Opaque/非规范 scope**：`scope_mismatch` 拒绝、零写入；旧批准须在规范 scope 上重核重批。
- **Signer 非受支持装配残余**：见 §3.7，不声称全覆盖。

## 5. RTO 超限处理（FR-031/036；C1）

- 口径分开记录且不得混同：**恢复点、数据库恢复时间（`db_restore_seconds`）、核验时间（`verification_seconds`）、各能力安全复服时间（per capability）、backup_lag、uncovered_interval、缺口数量与处置**。**不得以「数据库可连接/恢复完成」单独宣称 RTO 达标**；RTO 只在全部 7 能力观测放行后按端到端口径判定（`drill` 记录）。
- 配置了 `TXHARBOR_RECOVERY_RTO_TARGET` 且超时：**100% 记录不达标 + 告警 + 升级**；**不单独成为永久禁止后续安全复服的条件**。
- 安全门禁与时间目标分离：证据缺失、未隔离、未知付款结果、未闭合缺口等**独立阻塞**受影响能力；RTO 超限不改变这些门禁，反之门禁阻塞也不因时间达标而解除。
- 未配置必需约束：`constraints_configured=false` 显式报告，不得宣称符合生产恢复目标；本地演练数值仅测试输入。

## 6. 控制库回退纪律（F6/DG-4）

- **禁盲恢复控制库**（按备份/快照直接回退并继续使用禁止）。
- 受支持的控制恢复 = **停机隔离**（执行者执行 + **非执行者**核验隔离项）→ **显式重建/supersede + 审计**：
  - 新实例 ID 只增（不复用旧 ID）；
  - 执行者失去旧权限的确认方式：旧 DSN/凭据吊销或新库新凭据，**记录时间与主体**；
  - 建新实例的可信依据：部署受控配置 + 恢复点证据 + 重新隔离清单；
  - 旧库标记 retired；旧实例绑定不匹配即拒绝。
- **不得以「在已回退库内写 supersede 行」冒充可检/已重建**。015 不提供对盲恢复/旧副本的自动检测；「旧批准不自动重生效」仅就**数据 DB 回滚域**成立（控制域独立 + `instance_id` 绑定）；控制库回退域的普适保证须另立 ADR/裁决。

## 7. 身份映射维护边界与变更审计（F19）

- `recovery_identity`（person ↔ principal）由部署受控特权路径维护（`control identity-map-set`），记录 `source/recorded_by/recorded_at`；**单特权维护无法由 015 证明双人真实性**（程序/人的边界，不虚构自动检测）。
- **映射变更（新增/改写/撤销）→ 以该映射为依据的既有批准立即失效、须重核重批**；审批有效性按当前映射重算，批准行 `person_id` 与当前映射不一致 → `approval_identity_unverified` 拒绝；**保守 dual 不抵消错映射**（映射缺失/冲突 → 0 放行）。
- 生产身份名单待部署配置（plan「部署前裁决」）；本地路径以 `control identity-map-show` 复核。

## 8. 有界只读复核 bounds（F13；T051/T063）

- **可读范围**：本实例及授权 scope 内的证据/核验/缺口/审计行；禁止无界全表扫描（每次读取带实例谓词 + LIMIT）。
- **次数/时间/资源预算**：`TXHARBOR_RECOVERY_STATUS_TIMEOUT` / `_MAX_READS` / `_MAX_ROWS`（部署配置，无默认；本地值仅测试输入）。
- **耗尽行为**：拒绝后续复核 + 追加一条 refused 审计；**不改变任何**缺口/实例/批准/放行状态；**超时、预算耗尽、人工知悉 ≠ 缺口闭合或获准复服**。
- serve 状态面（`GET /status/degradation`）只报告恢复姿态与显式非声明（无控制库读取、无逐能力判定、无写）；逐能力三态的权威复核在 `recovery-admin status`。
- 不得以状态查询/健康探针替代资金门禁判权。

## 9. 同实例储层同失（F14/DG-3）

同一 PG 实例失效时数据 DB 与控制库同失，超出逻辑回滚域：015 **不提供实例级独立性**、不虚构跨实例保护；两者同失时 fail-closed 重建控制事实后才可能放行。备份产物落盘/异地策略为部署前裁决（不阻塞实现）。

## 10. 非声明

T000-P 保持 OPEN；本手册不宣称生产就绪、不宣称外部账本一致、不承诺跨系统恰好一次；生产 RPO/RTO/频率/保留未裁决；本地示例数值不得当作生产阈值；风险接受/核销/补偿/自动补造意图不交付；既有资金门禁不可绕过（`indexer_pause`、日志/充值暂停、006 恢复行、nonce hold、授权范围暂停、容量红线）。
