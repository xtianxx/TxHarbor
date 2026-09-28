# Research: 015 Backup Recovery and Safe Service Resumption (Phase 0)

**Branch**: `015-backup-recovery-safe-resumption` | **Date**: 2026-09-28 | **Spec**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Design**: [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md), [ADR-001](adr/ADR-001-recovery-control-store.md), [ADR-002](adr/ADR-002-backup-carrier-and-recovery-point.md)

All spec clarifications are resolved (Session 2026-09-28: FR-036 scope, FR-023 security, FR-019 business; 0 NEEDS CLARIFICATION remain). This research resolves the mechanism questions the spec deliberately left to plan: backup/restore tooling, control-information storage, runtime enforcement, approval/identity model, verification catalog, concurrency protocol, CI layering and measurement semantics.

**Gate status**: **T000-P stays OPEN** — plan-only round; no release, no production-readiness claim; every number below is a mechanism/knob, never a production threshold. Risk-accept forced resumption, loss write-off, manual compensation payments and automatic intent re-creation are **explicitly absent**: not approved, not designed, no contract row, no task (see §10).

## 1. 既有能力复用（真实入口逐项核对；结论：只读复用或经各自授权入口调用，不重建）

- **链观察与索引（002/003/004/005/006）**：serve 启动版本门禁 `internal/app/serve.go:141`（`db.CheckCompatibility`）；单主租约与 fencing token `internal/indexer/lease.go:56-63`（`ensureLeaseSQL` 含 `fencing_token = indexer_lease.fencing_token + 1`）、`:67 lockLeaseSQL`；scanner `serve.go:319`（header）/`:331`（log）/`:350-353`（deposit scanner，配置身份冻结拒绝漂移）/`:380-398`（confirmation scanner+committer）/`:411 NewRecoveryLoopWithMetrics`（006 恢复循环）；`serve.go:615 runServiceStreams` 五流并发；暂停门 `internal/indexer/scanner.go:1108/1148`（`indexer_pause`）。015 只做"停止证明/继续证明"的观察与门禁，不改写这些状态机。
- **提款创建（007）**：`internal/app/withdrawalhttp.go:136 ServeHTTP` / `:148 ServePOST`（调用链含 `guardRoute` + 013 `CapacityGate`，serve 装配 `serve.go:466-467`）/ `:227 ServeGET`；路由挂载 `serve.go:473-474`（`ratelimit.ClassNewWithdrawal`）、执行路由 `:479-480`、nonce 读 `:488`；限流 fail-closed 语义与"中间件只拒绝不改写"纪律 `internal/app/ratelimit_middleware.go:1-10/136`。
- **提款执行与在途恢复（008/009/010/011/012）**：worker `internal/app/withdrawalworker.go:318 NewWithdrawalWorker` / `:349 Run` / `:367 cycle` / `:566 startupCatchUp` / `:649 WithdrawalWorkerCommand`；唯一可写操作员 CLI `internal/app/withdrawalexec.go:35` 与 `execOperatorOp`（`operation_id` 审计去重、23505 读回、冲突零写）；门禁 `internal/execution/gates.go:31 GateLockSQL……:85/:161/:202/:240/:307/:345` 被 `admit.go:101-118`/`advance.go:136-164` 真实调用；fact-only 对账 `internal/execution/reconcile.go:41 ReconcileIntent`；未知结果 `internal/txlifecycle/reconcile.go:62 Reconcile` / `:183 UnknownRecovery`（只读事实+恢复条件）；锁序 `internal/txlifecycle/gates.go:84`；nonce 门 `internal/nonce/coord.go:69/118/172`。015 核验复用这些只读事实，**从不自动触发恢复/重放/付款**。
- **事件（013）**：发布 `internal/app/eventpublisher.go:41` → `internal/events/publisher.go:177 NewPublisher`（`:161-162` 说明 `FOR UPDATE SKIP LOCKED` + owner/lease 分片，无 Redis 锁）；消费 `internal/app/eventconsumer.go:31` → `internal/events/consumer.go:351 NewConsumer`、`:639 Effect.Apply`；参考消费者 `internal/events/refconsumer.go` 明确"非账本、仅本项目证据"；真实非权威 Effect = 缓存失效 `internal/cache/invalidator.go:120 Apply`；隔离/重放 `internal/events/quarantine.go`；watermark 审计 `internal/events/audit.go:148-180`。015 只做回退检测、幂等吸收边界与投递门禁接线。
- **014（差异/权限/审计/代次）**：`migrations/000016_reconciliation_handling.sql`（`recon_task/recon_checkpoint/recon_gap/discrepancy/disposition/reverify/recon_audit/recon_scan_attempt/recon_permission`，默认拒绝、无预置授予，`:377-402`）；`migrations/000017_event_obligations.sql`（事件义务标记，只增不减、无保留裁剪）；`migrations/000018_reverify_generation.sql`（纯增列 `discrepancy.reverify_generation`；写写反序令牌协议见 014 `data-model.md §3.1`；升级/回滚禁混跑检查清单见 014 `quickstart.md`「000018 升级与回滚」——**本规格沿用的 000018 检查清单模式即此模板**）。015 核验引用 014 差异/审计；014 生命周期动作仍走 014 自己已授权的入口。
- **审计与证据模式**：append-only 审计族（`execution_ops_audit`、`event_ops_audit`、`recon_audit`、`signing_request_audit`、`withdrawal_request_audit`、`nonce_ops_audit`）；`docs/evidence/*` 与 `.evidence/*` 证据目录模式；`internal/faultdrill`（五态矩阵/真实 Anvil）与 `internal/perf` harness。015 复用同一模式，不新建第二套审计体系。
- **迁移与版本检查**：`internal/db/migrate.go:105 Inspect` / `:164 CheckCompatibility`（非精确版本拒绝服务，只读）/ `:201 MigrateUp`（advisory lock 串行）/ `:235 MigrateStatus`；版本表 `goose_db_version`。015 备份兼容性检查复用该只读语义。
- **认证与信任根（现状限制）**：`principal = <kind>:<id>` 是 caller_id 非自然人（`migrations/000007:53 caller`、`:66 api_key`；`000012:173 execution_caller_permission`；`000009:62 signer_caller`/`:76 signer_credential`）；`operator` 多为自由文本；撤销靠 `DELETE`/`revoked_at UPDATE`/行内 UPDATE——**旧备份会复活已撤授权与凭据、重现旧批准**。015 的身份映射与审批模型必须正面处理（§5），不得把两个 principal 字符串默认当作两个人。
- **Decision**: 上述全部作为只读原语或经其自身授权入口复用；015 新增仅"恢复控制面 + 复服闸门 + 备份/核验/演练模型"四层。**Rationale**: 探针、检查点、幂等键、审计、租约、门禁都已存在且被测试覆盖，重建会违反宪法 XIII。**Alternatives considered**: 重写扫描器/自建备份控制台/在数据 DB 内建控制表——rejected（隔离举证难或回滚自证不可信，见 §3）。

## 2. R1 备份载体、完整性、兼容性与恢复点

- **Decision**: 基线载体 = **逻辑备份 `pg_dump --format=custom`**（由仓库外部的 `recovery-admin backup` 命令在**单事务导出的快照**上执行），产物 = dump 文件 + **外部 manifest（JSON）**；备份身份、覆盖范围、恢复点、完整性、schema/程序版本、验证状态全部写入 manifest，**选择恢复点只允许按 manifest 身份，不按文件名/人工记忆**（FR-001）。恢复 = `pg_restore` 到**显式隔离目标**，随后执行结构/约束/兼容性/关键可用性检查（FR-006 的"实际恢复验证"），验证结论写回 manifest 与恢复控制存储。受控快照方法：`BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_wal_lsn(), pg_current_snapshot(), pg_export_snapshot(), now();` 保持事务打开期间以 `pg_dump --snapshot=<id>` 导出，manifest 记录该快照元数据作为恢复点。
- **Rationale**: (a) 无 Dockerfile、compose 仅具名卷 `pgdata`（compose.yaml:19/卷声明），逻辑转储是唯一不依赖存储层/编排的载体；(b) 单一可校验文件+manifest 使"备份身份/完整性/选择规则"可落为纯函数与契约测试；(c) `pg_dump/pg_restore` 与 pinned `postgres:18.6-trixie` 同镜像自带，不新增产品/厂商/运行时依赖；(d) 快照 LSN + `pg_current_snapshot()` 给出**事务一致性恢复点**，天然区分于"业务表最大时间戳/备份频率"。
- **Recovery point 定义（唯一允许口径）**: `(snapshot xmin/xip/xmax, wal_lsn(export 时，作为上界), wall clock, server/database identity)`；`backup_lag = 恢复点 → 最近可观察外部事实/最新本地写入`，`uncovered interval = 恢复点 → 失败点之间未被备份覆盖的时间区间`。**MUST NOT** 用业务表 `MAX(created_at)`、备份频率、备份文件 mtime 充当 RPO 证明（spec FR-036、验收 SC-005）。
- **Integrity**: 产物 SHA-256 + manifest 自描述校验；缺失/截断/校验失败/完整性未知一律"不可用"（fail-closed，FR-003）。
- **Compatibility**: manifest 记录生成时 `goose_db_version` 精确版本集与程序版本标识；恢复/复服前与当前二进制目标版本比对（复用 `CheckCompatibility` 只读语义）；不兼容 → 明确拒绝，禁止静默降级/自动改写（FR-004）。
- **Retention**: 保留策略 = 部署配置（数量/时长），**生产数值不在本阶段裁决**；未配置必需约束时不得宣称符合生产恢复目标（FR-036）。
- **Alternatives considered**: (a) `pg_basebackup` + WAL 归档/PITR——deferred：需要归档存储/编排与同主版本约束，当前单机 posture 无此依赖；manifest 身份模型对物理载体同样适用，未来可替换（ADR-002）；(b) 卷/文件系统快照——rejected：依赖宿主存储实现、跨环境可移植差、无法在仓库内确定性演练；(c) 外部托管备份产品——rejected：spec 明示不指定厂商、不引入仓库外信任依赖。

## 3. R2 恢复控制信息可信来源（拒绝被回退 DB 自证）

- **Decision**: 恢复控制事实（恢复实例身份、执行者/核验者/批准者绑定、权限、批准、隔离检查证据、核验结论、缺口、审计、演练度量）存入**独立 PostgreSQL 控制库** `txharbor_recovery_control`（独立 DSN `TXHARBOR_RECOVERY_CONTROL_DSN`；默认可以与被恢复的数据 DB 位于同一 PG 实例的**另一个 database**），它**永不进入数据 DB 的备份/恢复集**，保留域独立；控制库丢失 = **fail-closed 隔离**（无批准即无放行），不是业务数据丢失。schema 用仓库同款 goose 机制独立版本化（独立 embedded FS + `recovery-admin migrate`；数据 DB `migrations/` 编号不动，本阶段数据 DB 零 schema 变更）。
- **Rationale**: (a) 被恢复的数据 DB 是回滚对象，**不能作为恢复实例/批准/撤销的自证来源**（旧行复活、撤销丢失、旧批准重现正是 spec 要处理的问题）；(b) 宪法 III 要求持久金融状态以 PG 为真源、并禁止本地文件成为金融状态唯一真源——控制库仍是 PG、可事务化、可审计，优于 JSONL 文件账本；(c) 同一 PG 实例另一 database 即可满足"逻辑回滚域独立"的主威胁模型（数据 DB 的 dump 不含控制库）；实例级灾难时两者同失，规则是 fail-closed 重建控制事实后才可能放行，不虚构独立性；(d) 最小形态：一个 schema + 一个薄 CLI，无服务进程、无 UI、无调度器、无主备。
- **处理四类污染**:
  - **旧授权复活/撤销丢失**：释放评估**不信任**数据 DB 中的授权类行（`recon_permission`/`signer_credential`/`api_key`/`execution_caller_permission` 等）作为 015 放行依据；对依赖这些授权的资金能力，隔离检查清单要求"授权面再核验/再施加"条目（对已有外部真源者重新执行撤销/再核验；无可信外部真源者保持未知→能力关闭），证据写入控制库。
  - **旧批准重现**：批准只在控制库、且**绑定恢复实例 ID**；数据 DB 或旧控制库行中的旧实例批准对当前实例惰性（instance_id 不匹配即无效）；015 从不读取数据 DB 中的批准行。
  - **实例身份复用**：实例 ID 由控制库生成、全局唯一、只追加、禁止复用；同一时刻**至多一个 open 实例**（部分唯一索引）；进程/命令绑定实例不匹配即拒绝。
  - **可信输入缺失**：控制库不可达/无实例/无批准/身份映射缺失 → 拒绝（不提供自由填写替代认证；`--operator` 类自由文本仅审计注记）。
- **Alternatives considered**: (a) 数据 DB 内建控制表——rejected：回滚自证不可信（旧批准/旧撤销随备份回流）；(b) 本地 append-only JSONL 证据账本——rejected 作为唯一权威：违反 III 精神且跨主机协调/持久性弱；可继续作为**证据附件**（哈希入控制库）；(c) 纯运维流程无存储——rejected：无法满足 FR-009/SC-003 的运行期拒绝与可查询状态；(d) 新控制平台/服务/UI——rejected：XIII 明确禁止无理由新服务边界。

## 4. R3 复服闸门与运行期强制（隔离默认、逐项放行）

- **Decision**: 新增核心库 `internal/recovery.Gate`（纯逻辑 + 控制库读取），由全部可能产生外部效果/双写的真实入口在"动作前"调用；**无 open 恢复实例时按正常态放行（不改变日常运行，FR-023）**；一旦存在 open 实例，所有已接线入口的对应能力**默认拒绝**，仅当该能力在控制库中对"当前实例 + 当前证据代次 + 范围"满足全部条件（§data-model §3 派生评估）才放行。单实例全局至多一个 open；进程可显式绑定实例（`TXHARBOR_RECOVERY_INSTANCE`），绑定不匹配即拒绝。控制库读取带**有界 TTL 缓存**（部署配置；本地值仅测试输入），缓存过期且控制库不可达 → fail-closed 拒绝（正确性优先于可用性，宪法 I）。
- **接线检查点（能力放行不是进程开关，而是动作前校验）**：查询类 HTTP 处理器准入前；链扫描/充值确认的 loop 启动与循环步进前；既有提款恢复（worker 认领/推进与 `withdrawal-exec` 操作员写路径）动作前；新提款创建 POST 准入前；事件发布 claim 批次前；事件消费 Effect 前；签名交付（signer-serve 交付路径）作为"既有提款恢复"的下游依赖同步受门禁。**任何入口 MUST NOT 存在一个开关恢复全部**（FR-021）。
- **重启语义**: 放行/批准状态只存在于控制库，进程重启读同一控制库 → 重启自动回到与库一致的判权，**不会自动解除隔离**（FR-009）；实例关闭前一直受门禁。
- **残留 procedural 边界（明示）**: 若操作者绕过 `recovery-admin` 手工恢复并让进程在"无 open 实例"的正常态下运行，门禁按设计不启用。015 的保证以"恢复走受支持入口 + 000018 检查清单"为前提：`restore` 必须在 open 实例下执行、恢复后首个动作必须是实例绑定与隔离核验；该前提是部署纪律（与 000018 的禁混跑同级），不虚构成自动检测能力。检测手段：控制库审计 + checklist 证据 + 门禁在实例开启后强制。
- **Alternatives considered**: (a) 纯运维流程（不起进程即隔离）——rejected：无法满足 FR-009 运行期拒绝与 SC-003 请求级 100% 拒绝；(b) 数据 DB 内开关——rejected：回滚复活/擦除控制状态；(c) 每进程 env 开关（默认关）——rejected：重启/遗忘即自动放开，违反"重启不自动解除"；(d) 常驻控制服务——rejected：XIII。

## 5. R4 人员、身份绑定与审批模型

- **Decision**: 权限动作集 = `recovery_execute`（执行恢复/核验写入）、`recovery_verify_read`（读授权范围内核验结果）、`recovery_approve`（批准/撤销复服）、`recovery_control_manage`（控制库主体/身份映射管理，仅部署期特权路径）。执行者（executor）按恢复实例记录；**执行者可读核验结果、不得批准自己执行的实例**（FR-023）。
- **身份绑定来源**（禁止自由填写替代认证）：CLI 主体来自**部署受控配置** `TXHARBOR_RECOVERY_PRINCIPAL`（与 014 `TXHARBOR_RECON_PRINCIPAL` 同模式），并必须命中控制库参与者注册；HTTP 侧无 015 变更接口（全部操作走本地特权 CLI）；`--operator/--reason/--evidence` 自由文本仅审计注记；`operation_id` 仅幂等去重。首次信任根 = 部署期受控引导（控制库 DSN 权限 + 身份绑定 + 审计），无有效配置默认拒绝。
- **同人多账号识别**: 控制库维护 `person_id ↔ principal` **身份映射**（部署期导入/维护，非自由文本）。规则：双人批准 = 两个不同 principal **且**两个不同 `person_id`（同一人多账号只算一人）；单人批准的非执行者校验同样按 `person_id ≠ executor.person_id`；**映射缺失/未知 → 不能证明是不同人，一律拒绝**（fail-closed）。执行者排除按实例记录，不按"用户名不同"推断。
- **审批类别**: 高影响能力（新提款创建、既有提款恢复、向真实下游投递及可产生真实下游业务效果的消费恢复）必须两名不同人员（均具 `recovery_approve`、均为本恢复实例非执行者）；其余能力（查询、链扫描、充值确认、事件发布/消费的非真实下游范围）由一名非执行者批准 + 审计。两档都绑定（实例、能力、范围、证据代次+哈希），证据变化/门禁失效即失效（重核重批）；**硬门禁不可被任何批准覆盖**（缺证/未隔离/未知付款结果），且只适用于 015 灾后复服，不改变日常运行与 014 已批权限，不新增紧急绕过/管理员强制入口。
- **撤销与重入**: 批准/释放为 append-only 决策记录，撤销是显式新记录（后序覆盖前序，但**代次不匹配的历史批准永远无效**）；重复批准/重复释放按 `operation_id` 幂等读回，不产生第二次副作用；中断重入等价于继续推进（§7）。
- **Alternatives considered**: (a) 复用/扩大 `recon_permission`、`execution_caller_permission`、signer 凭据——rejected：语义域不同，复用即自动扩权（014 同类结论），且位于被回退 DB；(b) 仅 principal 字符串区分两人——rejected：违反 FR-023 同人双账号条款；(c) 增加第三人在场审批——spec 裁决明确不要求三人互斥，不引入。

## 6. R5 核验项与可证明边界

- **Decision**: 核验目录固定为 9 类，逐项记录对象/范围/证据来源/结论（一致/差异/未知/过期）/时间与缺口关联（FR-014）：V1 链上事实 vs PG（区块/日志/确认/reorg 行，只读复用 002-006 与 014 `reconciliation` 只读原语）；V2 提款请求与付款意图（`withdrawal_requests`/`payment_intents`/授权授权范围）；V3 nonce 分配与占用（008 表 vs `eth_getTransactionCount`）；V4 签名/广播结果（`signing_requests`/`signature_results`/`tx_attempt_signings`/`tx_send_attempts`/`tx_receipts`/`tx_reconciliations`，unknown 保留）；V5 Outbox 事件与义务标记（`outbox_events`+`event_obligation`，分段发布/未决策略）；V6 消费者幂等与进度（`consumer_inbox/versions/progress/quarantine` + broker committed offset 可读时的回退检测）；V7 014 差异/复核/处置/权限/审计（只读引用）；V8 授权面漂移（回退点后撤销/发放的外部证据再核验，见 §3）；V9 工具与依赖就绪（恢复依赖、密钥边界可达性、镜像/客户端版本）。
- **结论纪律**: `consistent` 仅当证据完整、新鲜、覆盖闭合且全部来源一致；外部领先 → `divergent`；不可证明/依赖缺失/来源不可达 → `unknown`；超时效 → `stale`。**缺失≠从未发生、DB 回退≠外部回退、unknown 不得当通过、不得默认值补证**（FR-015/016/018）；不得假设 014/扫描能重建备份后丢失的付款意图，核验永不触发付款/重放/重广播（FR-017/020）。
- **缺口与独立性**: 每个未闭合缺口产出证据包（对象/范围/时间线/现有证据/所需外部证据/受影响能力/风险/责任归属/升级记录，FR-019）；路径图 + 逐边证据证明独立性，无法证明即保守纳入暂停；超时/重试耗尽/人工知悉≠闭合。缺口闭合只能靠新证据；**风险接受/核销/补偿付款/自动补造意图不在本阶段**。
- **边界声明（FR-027-029）**: at-least-once 语义下重复事件由既有 inbox/version 守卫幂等吸收；offset/inbox 回退可检测（PG 进度 vs broker offset）但**不得据缺失自动触发有副作用的重处理**；不宣称跨系统恰好一次；未接入真实上游/下游回执时只声明本项目范围内结论。
- **Alternatives considered**: 自建第二套对账器——rejected（013/014/006 已有；XIII）；把核验做成自动修复——rejected（FR-020 禁止）。

## 7. R6 并发、代次与幂等协议（复用 014 教训，不只靠时间戳）

- **Decision**: 每个恢复实例有 `evidence_generation`（接受写入即推进）；批准/释放**绑定代次 + 证据哈希**；取证在事务外、提交在实例行锁内逐项校验令牌，不符即丢弃并审计（`discarded`），不写结果、不删缺口、不倒序覆盖——与 014 `data-model.md §3.1` 的写写反序协议同形。所有决策表 append-only，前向状态 = 按提交序取最新 + 显式撤销记录；**不以 `created_at` 排序裁决有效性**。命令幂等键 `operation_id` UNIQUE（同形 011/013/014 读回语义）；中断重入 = 有界步进重复调用，无内存态。
- **Rationale**: 014 已证明"仅时间戳/进程时钟/裁决类型不能防倒序覆盖"；同一场景在恢复期（长时间人工操作、跨主机取证）更突出。
- **Alternatives considered**: 时间戳/最新行获胜——rejected（014 T026/T027 教训）；长事务持锁取证——rejected（跨慢调用不可行，违背有界原则）。

## 8. R7 CI 分层与演练通道

- **Decision**: 普通 PR 通道 = unit（纯函数：manifest 校验、门禁派生评估、审批规则、代次校验、状态机）+ `contract`（备份/复服/审批契约）+ PG `integration`（控制库事务/并发/幂等；小规模真实 `pg_dump→pg_restore` 隔离库恢复）。完整灾备演练（真实备份→真实恢复→核验→分级复服 + 7 类失败注入）走**独立通道**（新 build tag `drill` + Makefile 目标 + 独立 workflow/job，接入既有 `fault-perf.yml` 的 schedule/dispatch 模式），**不进普通 PR 闸门**（FR-033/SC-008）。演练与集成中，PG/链/中间件按层级使用真实实例（testcontainers/pinned images/Anvil）；**替身只能用于纯逻辑单测，不得作为门禁/核验的验收证据**。
- **反作弊纪律（写进契约与 quickstart）**: 禁止直写批准/释放/缺口状态绕过命令；禁止注入核验替身使门禁放行；禁止测试专用"关门禁"开关；真实恢复场景必须跑 `pg_dump/pg_restore`，链上事实必须来自真实链（Anvil/测试 RPC），事件声明必须来自真实 broker；外部账本结论一律超出证据范围。
- **Alternatives considered**: 把演练塞进普通 PR——rejected（spec 明令禁止 + 长时容器依赖破坏 PR 稳定性）；纯 mock 演练——rejected（宪法 XI）。

## 9. R8 配置、度量与非声明

- **Decision**: 可配置约束（FR-036）: RPO/RTO 目标、备份频率、保留策略、新鲜度容忍、门禁缓存 TTL、演练重复数——全部部署配置；**生产数值留部署前业务裁决，不编造**。度量分开记录且可查询：`recovery point`、`db_restore_time`、`verification_time`、`per_capability_release_time`、`backup_lag`、`uncovered_interval`、缺口数量与处置状态；**不得以数据库可连接宣称 RTO 达标**；若配置了目标，超时 100% 记不达标 + 告警 + 升级，且**不单独永久禁止后续安全复服**。未配置必需约束时状态显式报告"未配置"，不得宣称符合生产恢复目标；本地演练数值标注"测试输入"。
- **Alternatives considered**: 内置默认生产阈值——rejected（spec 明确禁止编造）；只记录总时长——rejected（SC-005 要求分列口径）。

## 10. 迁移编号、待测参数与待裁决

- **数据 DB 迁移**: 本设计新增 `000019+` **无**——015 不修改数据 DB schema（控制事实不入回滚集；见 §3/ADR-001）。若实现期发现必需的数据 DB 表，按纪律另报并保持 fail-closed 默认。控制库 schema 独立版本化（`recovery-admin migrate`），编号自成体系。
- **待测参数（实现后测量填入，不编造，不阻塞设计）**: 门禁缓存 TTL、各证据类别新鲜度容忍、核验批次上界、控制库语句超时、演练时长与备份大小/耗时。生产阈值单独立项裁决。
- **部署前裁决（不阻塞本轮设计）**: 生产 RPO/RTO/备份频率/保留期（FR-036 已声明留裁决）；控制库拓扑（同实例独立 database vs 独立实例）与保留；身份映射（人员↔principal）内容与维护者；真实下游 effect class 清单（哪些 topic/scope 属"真实下游投递"）；单机/单人部署时非执行者批准人来源（FR-023 下单人无法自批）；备份产物落盘/异地策略。以上按 spec 纪律单列，本计划不批准、不阻塞无关设计。
- **阻塞项**: 无设计阻塞。风险接受后强制复服、损失核销、人工补偿付款、自动补造意图：**明确缺席**——无设计、无契约行、无迁移表、无任务；若未来需要属业务阻塞，另行业务裁决（双人批准亦不得替代缺失证据）。
- **发布门禁**: T000-P 保持 OPEN；本目录全部结论仅本地范围，不宣称生产就绪。
