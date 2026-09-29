# Contract: Backup Manifest and Recovery Point (015)

**Spec**: [spec.md](../spec.md) (FR-001–FR-008, FR-036) | **Design**: [data-model.md](../data-model.md) §2/§7 | **Plan**: [plan.md](../plan.md) | **Quickstart**: [quickstart.md](../quickstart.md) | **ADR**: [ADR-002](../adr/ADR-002-backup-carrier-and-recovery-point.md)

备份 = `pg_dump --format=custom` 产物 + 外部 manifest。**manifest 是唯一备份身份与选择依据**；文件名/目录/mtime/人工记忆不构成选择规则。

**Gate**: T000-P 保持 OPEN；生产 RPO/RTO/备份频率/保留期未裁决（§4 的 `retention_class`/约束为部署配置，本地值仅测试输入）；风险接受后强制复服/损失核销/人工补偿付款/自动补造意图不在本阶段交付；本契约不批准任何资金数据损失额度。

## 1. Manifest 字段（规范化 JSON，哈希落库）

| 字段 | 必填 | 语义与校验 |
|---|---|---|
| `manifest_version` | 是 | 契约版本；未知版本 → 拒绝 |
| `backup_id` | 是 | UUID，生成时分配，全局唯一；恢复/审计引用此 ID |
| `created_at` / `created_by` | 是 | 生成时间与 principal（非自由文本） |
| `carrier` | 是 | `pg_dump_custom` + server/pg_dump 版本；与目标 PG 主版本不符 → 拒绝 |
| `coverage` | 是 | 覆盖声明（数据 DB 权威对象清单）与排除声明（Redis/Kafka 非权威状态、**Signer 私钥/真实凭据永不入库**）；缺失声明 → 拒绝 |
| `recovery_point` | 是 | 快照元数据（`pg_current_snapshot` xmin/xip/xmax + export 时 `pg_current_wal_lsn` 上界 + wall clock + server/database 标识） |
| `schema` | 是 | 生成时 `goose_db_version` 精确应用集 |
| `program` | 是 | 生成时程序版本 + 最低兼容标识 |
| `artifacts[]` | 是 | 每产物 `path/bytes/sha256`（dump 可分片） |
| `verification` | 是 | `state`(`unverified`/`verified`/`rejected`) + 实际恢复验证记录（见 §3） |
| `retention_class`/`note` | 否 | 部署策略引用；生产数值不在本阶段裁决 |

## 2. 选择、完整性与兼容性规则

- **选择**：只接受 `verification.state=verified` 且 manifest 校验通过的备份；多个候选必须经 manifest 字段确定性比较（`created_at` 仅作展示，用 `recovery_point.wal_lsn`/`backup_id` 决胜），可复核。
- **完整性**：任一产物缺失/长度不符/`sha256` 不符/`pg_restore -l` 不可读 → `rejected`；完整性未知 = 不可用（FR-003）。
- **兼容性**：manifest `schema` 与当前程序目标版本不一致，或版本集含未知迁移 → 明确拒绝恢复/复服；`MigrateOptions.Inspect/CheckCompatibility` 语义复用，不静默降级、不自动改写（FR-004）。
- **依赖检查**：恢复前检查目标 DSN/权限、事实来源可达性、下游/中间件状态、Signer 边界可达性；缺失 → 阻塞状态 + 报告缺失项，不宣称恢复基本完成（FR-005）。

## 3. 恢复验证记录（备份成功 ≠ 恢复成功；FR-006）

`verification.state=verified` 必须来自**至少一次隔离环境实际恢复**（真实 `pg_restore`，不是仅"能读文件"、不是重新初始化空库）：

| 检查 | 通过标准 |
|---|---|
| `readable` | dump 可列出、可恢复，无截断/校验错误 |
| `structure_constraints` | 目标库 schema 版本精确等于 manifest；关键表/约束/FK 完整 |
| `business_state_probes` | 代表性只读查询可执行且结构可用（不得以"计数非零"作为业务正确性证明）；**逐项对照 FR-002 九类权威对象**（链身份/游标、事件、充值确认、提款请求与付款意图、出站交易与签名/广播、nonce、Outbox/义务标记、消费者幂等/进度、审计/权限/014 差异）抽样并声明覆盖边界（F9） |
| `verification_executable` | 015 核验工具可对目标执行 V1–V9 只读核验；不能证明的类别标 `unknown`，不计入通过（F9） |

- 验证记录含 `verified_at`/`verifier`/`target=isolated`/`evidence_ref`；写回 manifest 与恢复控制库。
- **验证生命周期（F7/DG-2）**：备份级验证（manifest `verified`，绑定 backup_id/carrier/schema）≠ 目标实例级验证（`restore_probe`，绑定实例 + `data_target` 指纹）；复制 manifest、重建实例或更换目标后必须重跑实际恢复验证/探针，不得沿用旧 `verified`；`restore` 前置必须校验控制库存在绑定该 `backup_id` 的可验证证据行（无可验证证据 → 拒绝或重跑 `verify-backup`），不得只信 manifest 文件内的旗标。
- `verify-backup` 先于实例开启时的证据绑定按 [data-model.md](../data-model.md) §1.4：先落 manifest 级结论，实例开启后经受控命令显式同步绑定并审计。
- 未验证备份仅可标记 `unverified`，不得作为恢复或复服依据。
- **禁止**：以备份频率、业务表 `MAX(created_at)`、备份文件 mtime 证明 RPO；以"备份命令退出 0"证明恢复成功。

## 4. 恢复点与度量口径

- `recovery_point` = §1 快照元组；`backup_lag` = 恢复点 → 最近可观察外部事实/最新本地写入；`uncovered_interval` = 恢复点 → 失败点未覆盖区间；二者在演练/恢复记录中显式填写（可为 unknown，但不得省略）。
- `db_restore_time`、`verification_time`、`per_capability_release_time` 分开记录，禁止以数据库可连接代表 RTO 达标（FR-031/036）。
- 未配置必需约束（目标/频率/保留/演练要求）→ `constraints_configured=false`，不得宣称符合生产恢复目标；本地演练数值标注 `test_inputs`。

## 5. 被拒绝行为（fail-closed 清单）

| 条件 | 结果 |
|---|---|
| manifest 缺失/字段缺失/哈希不符/未知版本 | 拒绝使用；不尝试"尽力恢复" |
| 产物损坏/截断/部分写入/完整性未知 | `rejected`；不得进入核验完成或复服 |
| schema/程序版本不兼容 | 拒绝恢复或拒绝复服；不静默降级 |
| 依赖/配置缺失 | 阻塞在明确状态并报告；不宣称恢复完成 |
| `verification.state != verified` | 不得用于恢复与复服 |
