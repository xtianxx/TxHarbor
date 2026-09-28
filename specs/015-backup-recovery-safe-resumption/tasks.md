# Tasks: 015 Backup Recovery and Safe Service Resumption

**Input**: Design documents from `/specs/015-backup-recovery-safe-resumption/` — spec.md（6 US：US1–US3 P1、US4–US6 P2；FR-001–036；SC-001–008；Clarifications 2026-09-28 三项裁决）, plan.md（Structure Decision、真实入口接线清单、FR/SC/澄清映射）, research.md（R1–R8）, data-model.md（控制库 11 实体、门禁公式、状态机、代次协议、V1–V9、INV-1–10）, contracts/（backup-manifest、resumption-gate、approval-matrix、verification-items）, adr/ADR-001、ADR-002, quickstart.md（S1–S12、F1–F7、分层）, checklists/requirements.md（16/16）, constitution v1.1.0

**Prerequisites**: plan.md（required）、spec.md（required for user stories）、research.md、data-model.md、contracts/、quickstart.md

**Tests**: 含测试任务——依据 spec 验收场景 + quickstart S1–S12/F1–F7 + 宪法 X/XI（确定性本地测试、测不变量：门禁/核验/放行的验收证据必须来自真实控制库与真实恢复/链/中间件；替身仅限纯逻辑单测，不得作为门禁/核验验收证据）。分层挂 CI：unit/contract 普通 PR；PG integration 按路径分类；drill 独立通道、不进普通 PR；Docker 缺位记 NOT RUN，不得记 pass。

**Gate/口径**: T000-P 保持 OPEN；本清单不是发布、不宣称生产就绪；所有任务初始 `[ ]` 未勾选、无工作已开始；本地数值仅测试输入，生产 RPO/RTO/频率/保留未裁决。风险接受后强制复服、损失核销、人工补偿付款、自动补造意图明确缺席（无设计、无契约行、无任务）。

**本轮 analyze 定向修正（2026-09-28，F1–F20）**: 仅改 `specs/015-backup-recovery-safe-resumption/` 规划文档，未运行任何测试/验证，不得据此宣称运行验证通过；修正不降低任何安全承诺——问题以「前置/完成条件/书面边界/追加任务」方式关闭，原阻塞与 fail-closed 口径全部保留。

**Organization**: 阶段按 user story（P1 → P2）；阶段内按批次（B#）组织。每批标注：前置 / 实际入口（文件:行）/ 完成条件 / 验证命令所属层次（unit `make test`·race `make test-race`·contract `make test-contract`·PG integration `make test-integration`·kafka/redis `make test-integration-kafka|redis`·drill `make test-drill` 独立通道）/ 提交节点（本地 commit，不 push）。7 类能力与 7 条交付链的归属见下节与尾部映射表。

## 七条交付链 → 批次归属（总览）

| # | 交付链 | 归属批次 | 锚点任务/证据 |
|---|---|---|---|
| 1 | 备份到恢复（快照生命周期、dump 实际用快照、manifest 产物绑定、失败不发成功备份、隔离库真实 restore+兼容检查；快照身份/可恢复范围/RPO 证明区分；restore_probe 九类权威对象枚举＋备份级/实例级验证生命周期 T019/T020） | B5、B6（+B17 联合演练） | T014–T022、T058；证据 `docs/evidence/015/` |
| 2 | 控制库与信任边界（独立初始化、schema 版本与版本校验 T069、最小权限、配置校验、CLI 构造；不误指数据恢复目标、不进数据备份集、独立 DSN≠隔离证明；不可达/丢失行为；旧副本回退纪律=禁盲恢复+停机隔离+显式重建/supersede+审计 T025/T064；旧批准不自动重生效仅限数据 DB 回滚域，普适声明保持限定） | B1、B2（含 T069）、B7（T025）、B8 | T006–T008、T025、T027、T069、T008/T022 DSN 校验 |
| 3 | 身份与审批（人员映射、授予/撤销、执行者记录、同人多账号拒绝、执行者不批己案、单/双人批准、幂等审计；本地可操作身份配置路径；映射变更→相关批准失效重批） | B3、B12、B13 | T009–T010、T043–T051 |
| 4 | 七类能力接线（逐项生产入口/Gate 调用点/范围依赖/原门禁保留/正反例；签名·广播·发布·消费效果与重试入口；signer 进程内检查点 T070；execution HTTP 写路径 T032；正常 vs 恢复模式；缺配置行为；reconcile-admin/events-admin/调度=程序边界声明 T028/T064） | B4（底座）、B7（负例）、B9（接线，含 T070）、B12·B13（放行正例） | T011–T012、T023、T030–T034、T045、T070 |
| 5 | 并发与失效（取证后变化、批准后撤销、检查后执行、旧实例续跑、迟到核验、重复复服、中断重入；TTL 代次感知失效——证据代次/哈希变化立即失效（发现者=求值器），未准入拒绝 vs 已在途原门禁；代次/隔离设计+受控交错；外部副作用不可回滚保持 unknown） | B4（代次底座）、B7（TTL/拒绝）、B12（T047）、B13（失效重批） | T013、T023、T047、T048–T049 |
| 6 | 核验与正向复服（V1–V9 真实适配器、完整性/新鲜度/预算/缺证行为；备份→外部推进→恢复旧数据→缺口拒绝→按审批恢复指定能力；禁补造意图/直写批准；不可重建=保持暂停） | B10、B11、B15、B12（放行正例）、B17 | T035–T042、T045、T052–T054、T058 |
| 7 | 指标运维 CI（三段计时+RTO 超时处理分开；配置示例/真实命令/隔离证明/失败处置/证据索引；普通 PR 分层、真实小规模 dump/restore 路径触发与失败传播、完整演练独立；NOT RUN 纪律） | B5（真实 dump/restore 路径）、B16、B17、B18、B19 | T015、T055–T061、T062–T068 |

## 本轮 analyze 定向修正 → 批次归属（不重排原批次；新增仅 T069 起）

> 依据本轮 analyze 发现（F1–F20）集中修正规划文档。不降低任何安全承诺：原阻塞与 fail-closed 口径保留；F5/F7/F4/F9 仍为对应首批完成条件前置（不得推迟收尾），F1/F2/F3/F12 归 B9 接线批（T070 并入 B9），F11/T069 并入 B2（与 T007/T008 同批，版本守卫），F13/F19 归后批/Polish 合并但先于其依赖路径生效。

| 发现 | 修正位置 | 批次归属/阻塞口径 | 残余边界（不夸大） |
|---|---|---|---|
| F1 signer 交付门禁只到 app 包装层（HIGH） | T032 标注交付核心路径＋**新增 T070**；resumption-gate §1 | B9（T070 并入 B9，于 T032 后串行）；不得称全覆盖 | 非受支持装配/蓄意伪造门禁不自动检测（DG-1 同类）；worker/exec 门禁＋V8 为纵深 |
| F2 withdrawalexecution.go 写路径无 owner（HIGH） | T032 增 owner＋正反例（归属 `existing_withdrawal_recovery`） | B9 | HTTP 直调 `execution.Admit` 与 CLI `execOperatorOp` 非同漏斗，须独立接线 |
| F3 reconcile-admin/events-admin/定时调度无运行时门禁（HIGH） | T028 证据覆盖＋T064 runbook；resumption-gate §1.2 | B9 书面边界＋B18 文档 | 程序边界（停服+审计+`no_pre_release_effects`）；仅 checklist 签署不构成运行时隔离证明 |
| F12 每轮步进门禁与包禁令冲突（MEDIUM） | T030 明确回调/注入形态；resumption-gate §1 | B9 | recovery 根包不得 import indexer（T001），装配在 `internal/app` |
| F4 V4「复用 Reconcile」表述含混（HIGH） | T036/T039 钉死只读方法白名单＋零写断言；verification-items §1 | B10/B11 完成条件前置 | 核验前后权威表指纹零写；方法名不构成只读证明 |
| F5 TTL 未钉死代次感知失效（HIGH） | T012/T047；approval-matrix §3；data-model §3.3 | B4 底座＋B12 交错验收 | 发现者=门禁求值器（行锁内权威代次/哈希）；控制库不可达 fail-closed；未准入拒绝 vs 已在途原门禁 |
| F6 控制库旧副本代内自洽无法自检（HIGH/DG-1） | T025 控制回滚负例；T064 控制恢复纪律；ADR-001/research §3 | B7 负例＋B18 纪律 | 不提供自动检测；普适「旧批准不重生效」声明保持限定/阻塞；仅受支持重建路径＋审计 |
| F14/DG-3 同实例储层同失（MEDIUM） | T064＋research §3/§10；待裁决单列 | 文档/待裁决 | 超出逻辑回滚域；备份落盘/异地待裁决，不阻塞实现 |
| F7 S2 先于开实例、restore 只认 manifest 旗（HIGH/DG-2） | T019/T020＋quickstart S2＋backup-manifest §3＋data-model §1.4 | B6 完成条件前置 | 备份级验证≠目标实例级验证；复制 manifest/重建/换目标指纹须重 probe |
| F8 plan 指代 signer/gates.go:123/162 不精确（HIGH 附带） | plan/contracts/tasks 改指交付路径＋行号核对注记 | 文档同步 | 行号按 HEAD 10e78f9 核对，实现期以实际文件为准 |
| F9 restore_probe 未枚举 FR-002 九类（MEDIUM） | T019/T020＋backup-manifest §3＋data-model §7 | B6 | 抽样声明边界；不能证明标 unknown，不计入 restored |
| F10 无历史重发显式负例（MEDIUM） | T036＋verification-items §1 | B10 | 0 重放/重广播/重投递/重建意图 |
| F11 控制库仅 0001 无升级校验（MEDIUM） | **新增 T069**＋T008 引用；data-model §3.3 | B2（T069 并入 B2，与 T007/T008 同批、T007/T008 后串行）；B4/B13 前置经 B2 获得版本守卫 | 未知/不兼容版本拒绝；当前仅 0001 初版 |
| F13 有界只读复核无 bounds（MEDIUM） | T051/T063＋verification-items §2＋data-model §6 | B14/B18（Polish） | 范围+预算+耗尽拒绝审计；超时/耗尽≠闭合 |
| F15 B11/B13 并行表述（LOW） | B11/B13 批次注记：串行现状保留、owner 确认前不并行、不加 [P] | 批次注记 | 不重排 |
| F16 approved/released 术语（LOW） | data-model §4.2 命名统一＋plan/contracts/quickstart 校正 | 文档同步 | `released`=派生放行；`approved`=批准记录（spec FR-022 的 approved 即 released） |
| F17 ci.yml:123 行号（LOW） | T062/B18 注记「按 HEAD 10e78f9 核对，实现期复核」 | 文档同步 | — |
| F18 US1「无跨 story 依赖」（LOW） | Dependencies 注记：S4 恢复 E2E 归 US2 | 文档同步 | US1 自验=S1/S2＋F1–F3 |
| F19 身份映射单特权维护可伪造双人（MEDIUM） | T010/T046/T064＋approval-matrix §2/§3 | B3＋后批验收/文档 | 映射变更即相关批准失效重批；保守 dual 不抵消错映射；生产名单待部署，本地路径可操作 |
| F20 门禁公式两阶段判权（LOW） | T012 注记＋data-model §3 | B4 | recovery allow AND 动作处原门禁，禁替代 |

---

## Phase 1: Setup（共享基础设施）

**Purpose**: 骨架、配置键、CLI 命令面、独立演练层——不实现行为。

### 批次 B0 · Setup

- **前置**: 无（可立即开始）。
- **实际入口**: `internal/recovery/`（新建）；`cmd/txharbor/main.go:56`（命令注册 switch）；`internal/app/recoveryadmin/recoveryadmin.go`（新建，参照 `internal/app/reconcileadmin/reconcileadmin.go:1-40`）；`Makefile`；`.github/workflows/drill.yml`（新建）。
- **完成条件**: `go build ./...` 通过；`txharbor recovery-admin --help` 可运行且列出全部子命令；配置键常量存在；`make test-drill` 在无 drill 测试时按 NOT RUN 退出（非 0）。
- **验证层次**: unit（`make test` 编译层）；无 Docker。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [x] T001 创建 `internal/recovery/doc.go` 核心库骨架与边界声明：package 文档写明 plan.md Structure Decision（核心库 manifest/gate/capabilities/verification/gaps/approvals/generation/controlstore + 薄 CLI；不新增常驻服务/监听/daemon/UI/调度器；数据 DB 本阶段零 schema 变更），并记录硬约束——被 `package app` import 的根包不得传递 import `internal/{txlifecycle,events,indexer,reconciliation,cache}`（这些包的内部集成测试 import `internal/app`，会形成测试构建环；先例 `internal/app/reconcileadmin/reconcileadmin.go:1-20`），V1–V9 重依赖适配器落后包 `internal/recovery/sources/` 或经接口注入，证明方式 `go vet -tags integration ./internal/txlifecycle ./internal/events` 可构建
- [x] T002 [P] 建立控制库独立 goose FS 骨架于 `internal/recovery/controlstore/schema/embed.go` + `internal/recovery/controlstore/schema/0001_control_init.sql`（占位）：与数据 DB `migrations/embed.go` 分离、独立版本序列；`recovery-admin migrate` 将来只作用于控制 DSN，数据 DB `migrations/` 与 `goose_db_version` 零变化
- [x] T003 [P] 在 `internal/config/config.go` 定义 `TXHARBOR_RECOVERY_*` 配置键与缺省拒绝语义：`TXHARBOR_RECOVERY_CONTROL_DSN`、`TXHARBOR_RECOVERY_PRINCIPAL`、`TXHARBOR_RECOVERY_ARTIFACT_DIR`、`TXHARBOR_RECOVERY_GATE_TTL`、`TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_*`、`TXHARBOR_RECOVERY_RPO_TARGET`、`TXHARBOR_RECOVERY_RTO_TARGET`、`TXHARBOR_RECOVERY_BACKUP_FREQUENCY`、`TXHARBOR_RECOVERY_RETENTION`；缺失必需项→按名拒绝并报告「未配置」、MUST NOT 内置默认值（生产阈值待裁决、本地值仅测试输入）；命名不得与既有测试键 `TXHARBOR_RECOVERY_KILL_CHILD/_DSN/_READY/_DISPATCH`（`internal/withdrawal/recovery_kill_integration_test.go:65-68`）碰撞、不得改名既有键
- [x] T004 [P] 注册 `recovery-admin` 命令与固定子命令面于 `cmd/txharbor/main.go` + `internal/app/recoveryadmin/recoveryadmin.go`（命令 owner：migrate/control/backup/verify-backup/restore/instance-open/instance-close/checklist-set/checklist-verify/verify/approve/release/status/drill 全部经此汇合）；Run/Deps 模式对齐 `internal/app/reconcileadmin/reconcileadmin.go`；stub 只做 help/参数解析、无行为、无非授权默认
- [x] T005 [P] 增加 drill 独立层骨架：`Makefile` 增 `test-drill` 目标 + `require_tagged_tests(drill,test-drill)` NOT RUN 守卫（无 `drill` 标签测试时非 0 退出）；`.github/workflows/drill.yml` 骨架（`schedule` + `workflow_dispatch`，**无 `pull_request` 触发**），对齐 `.github/workflows/fault-perf.yml:20-29` 独立通道模式与 NOT RUN/证据归档纪律

**Checkpoint（Phase 1）**: 骨架可构建、CLI help 可运行、drill 层空跑记 NOT RUN；无行为变更；可独立提交。

---

## Phase 2: Foundational（阻塞所有 story）

**Purpose**: 控制库 schema/store、身份与授权底座、门禁派生评估与代次协议——全部 story 的前置。

**⚠️ CRITICAL**: 本阶段完成前任何 user story 不得开始。合流责任：控制库 schema（T006）单一所有者、只合并一次；store（T007）为决策读写唯一入口；身份（T009/T010）与门禁（T011–T013）各自单一所有者，所有接线只调用这些接口，不得就地复制判定逻辑。

### 批次 B1 · 控制库 schema（链 2）

- **前置**: B0 完成。
- **实际入口**: `internal/recovery/controlstore/schema/0001_control_init.sql`；控制 DSN（`TXHARBOR_RECOVERY_CONTROL_DSN`）。
- **完成条件**: 在隔离 PostgreSQL 上迁移干净；11 实体与逐字约束全部落地；无预置主体/授予/放行；数据 DB `migrations/` 与 `goose_db_version` 无差异。
- **验证层次**: PG integration（`make test-integration`；Docker 缺位 → NOT RUN，不得记 pass）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [x] T006 实现控制库 schema `internal/recovery/controlstore/schema/0001_control_init.sql`，建 data-model.md §1 全部 11 实体并逐字保留约束：`recovery_instance`（`kind` CHECK(`recovery`,`baseline`)、`state` CHECK(`open`,`closed`)、部分唯一 `CREATE UNIQUE INDEX … ON recovery_instance (state) WHERE state='open'`（全局至多一个 open）、`evidence_generation` BIGINT NOT NULL DEFAULT 0 只允许单调 +1、`instance_id` 只增不改禁止复用）；`recovery_participant`（`role` CHECK(`executor`,`verifier`,`approver`)、PK(`instance_id`,`principal`,`role`)，映射缺失时 NOT NULL 不成立→拒绝注册）；`recovery_identity`（PK(`principal`)、`active` BOOL 撤销留行）；`recovery_evidence`（`kind` CHECK(`backup_manifest`,`restore_probe`,`verification_batch`,`isolation_check`,…闭集)）；`recovery_verification_item`（`category` CHECK(`V1`…`V9`)、`conclusion` CHECK(`consistent`,`divergent`,`unknown`,`stale`)，`unknown`/`stale` 必须带 `reason` 且不得带空 `sources`，只追加）；`recovery_gap`（`state` CHECK(`open`,`closed`,`escalated`)、`affected_capabilities` TEXT[]、`dependency_proof` JSONB nullable，timeout/attempts_exhausted/acknowledged 不是状态）；`recovery_isolation_check`（`state` CHECK(`pending`,`evidenced`,`verified`,`rejected`)、`verified_by` 必须 ≠ 本实例 executor）；`recovery_approval`（`decision` CHECK(`approve`,`revoke`)、`approval_class_snapshot` CHECK(`single_non_executor`,`dual_non_executor`)、`operation_id` TEXT UNIQUE、append-only）；`recovery_release`（`decision` CHECK(`release`,`revoke`)、`approval_refs` UUID[] 确定性排序、`operation_id` TEXT UNIQUE、append-only、无可直写放行布尔）；`recovery_audit`（BIGSERIAL PK、`result` CHECK(`ok`,`refused`,`discarded`,`failed`)、`refusal_class` 闭集、append-only、拒绝/丢弃亦记行）；`recovery_drill_run`（`db_restore_seconds`/`verification_seconds` NUMERIC 分开、`capability_release_seconds` JSONB、`constraints_configured` BOOL、`test_inputs` JSONB、`result` CHECK(`ok`,`refused_safe`,`failed_injected`)）；本模型不承载任何金额；无预置数据

### 批次 B2 · 控制库构造与信任边界（链 2）

- **前置**: B1 完成。
- **实际入口**: `internal/recovery/controlstore/store.go`；`internal/app/recoveryadmin/migrate.go`（命令汇合于 `cmd/txharbor/main.go:56`）；控制库 schema 版本清单（`internal/recovery/controlstore/schema/`）。
- **完成条件**: store 提供 append-only 决策读、行锁、`operation_id` 幂等读回、审计写入；`migrate up/status` 只作用控制 DSN；控制 DSN = 数据 DSN 被拒绝；DSN 明文不入日志/证据；最小权限说明落地；**版本守卫（T069）**：控制库 schema 版本兼容校验落地，未知/不兼容版本在 migrate 与 store 读路径均被明确拒绝（fail-closed），正反例可测；后续门禁/审批任务不得缺此检查即称完成。
- **验证层次**: unit + PG integration（`make test-integration`）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [x] T007 [P] 实现控制库 store 于 `internal/recovery/controlstore/store.go`：实例行锁（`SELECT … FOR UPDATE`）与部分唯一冲突拒绝；append-only 决策读取（前向状态=按提交序取最新+显式 revoke，**不以 `created_at` 排序裁决有效性**）；`operation_id` UNIQUE 同输入读回、异输入冲突零写；审计写入含 `result`/`refusal_class`/`evidence_generation`；不得新增可直写放行布尔（依赖 T006）
- [x] T008 [P] 实现控制库初始化与信任边界校验于 `internal/app/recoveryadmin/migrate.go`（`recovery-admin migrate up/status`）：只对 `TXHARBOR_RECOVERY_CONTROL_DSN` 执行；DSN 未配置/不可达/认证失败→拒绝；控制 DSN 与数据 DSN 相同→拒绝；`data_target` 只存 database/role 指纹、明文 DSN 不入库/不入日志/不入证据；最小权限与独立保留域说明；无有效配置默认拒绝；控制库 schema 版本未知/不兼容→拒绝（版本校验与正反例见同批 T069；本任务不得在未知版本上继续）（依赖 T006）
- [x] T069 控制库 schema 版本校验（F11；本批 T007/T008 后串行）：`recovery-admin migrate` 与 store 连接识别未知/不兼容控制库 schema 版本→明确拒绝（fail-closed；拒绝以 `control_store_unavailable` 表达并审计注记版本，不新增放行路径）；migrate 不得对未知版本静默降级/尽力读取；即使本阶段仅 0001 初版，校验路径与拒绝路径必须存在并可测（正例=已知 0001 通过；反例=未知/缺版本表/版本不兼容→拒绝）；B4/B13 的求值/放行/审批前置经本批获得版本守卫，T012 与 T048–T051 不得缺此检查即称完成（依赖 T007、T008）

### 批次 B3 · 身份与授权底座（链 3）

- **前置**: B2 完成。
- **实际入口**: `internal/recovery/controlstore/identity.go`；`internal/app/recoveryadmin/control.go`。
- **完成条件**: 参与者注册与身份映射可操作（本地验收必须有可操作身份配置与授权路径）；principal 缺映射→拒绝；撤销留行；身份数据永不来自被回退数据 DB。
- **验证层次**: unit + PG integration（`make test-integration`）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [x] T009 [P] 实现身份映射与参与者存储于 `internal/recovery/controlstore/identity.go`：写入 `recovery_participant`（`principal` 为认证调用者身份格式 `<kind>:<id>`，禁用自由填写；`person_id` 必须经映射解析，映射缺失→拒绝注册；PK(`instance_id`,`principal`,`role`)）与 `recovery_identity`（PK(`principal`)、`active` BOOL 撤销留行、`source`/`recorded_by`/`recorded_at`）；永不从被回退数据 DB 读取身份/授权行作为 015 依据（依赖 T007）
- [x] T010 实现控制面管理 CLI 于 `internal/app/recoveryadmin/control.go`：`participant-register`/`identity-map-set`/`identity-map-show`；`recovery_control_manage` 部署期特权路径（单主体+审计、本阶段不强制第二人、不预置任何主体、无有效配置默认拒绝）；主体来自 `TXHARBOR_RECOVERY_PRINCIPAL`，`--operator/--reason` 自由文本仅审计注记；`operation_id` 幂等审计（依赖 T009）；**身份映射变更纪律（F19）**：映射维护=部署受控配置+审计（`source`/`recorded_by`/`recorded_at`）；单特权维护无法自证双人真实性——映射变更立即使以该映射为依据的既有批准失效、须重核重批；保守 dual 不抵消错映射（映射缺失/不一致→拒绝）；本地真实身份配置与验收路径可操作（T046），生产名单待部署

### 批次 B4 · 门禁/能力/代次底座（链 4/5）

- **前置**: B2、B3 完成（B2 含 T069：控制库版本守卫已落地——未知/不兼容版本拒绝；B4 求值不得缺此守卫）。
- **实际入口**: `internal/recovery/capabilities.go`、`internal/recovery/gate.go`、`internal/recovery/generation.go`（接口将被 B9 的全部真实入口调用）。
- **完成条件**: 能力闭集与依赖矩阵写死；`release_valid` 派生评估公式完整；无 open 实例→正常态直通；有 open 实例→默认拒绝；TTL 有界且失效语义可测；控制库不可达→拒绝；**控制库版本未知/不兼容→拒绝（经 B2/T069，不得缺此检查）**；代次令牌协议可丢弃过期提交。
- **验证层次**: unit（`make test`，纯逻辑）+ contract 在 B7 补证；PG integration 由 B7 承接入库。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [x] T011 [P] 实现能力闭集与依赖矩阵于 `internal/recovery/capabilities.go`：7 能力闭集 `query`/`chain_scan`/`deposit_confirmation`/`existing_withdrawal_recovery`/`new_withdrawal_creation`/`event_publishing`/`event_consuming`；`requires_capabilities` 与 `isolation_dependency_set` 按 data-model §3.2 写死（`deposit_confirmation→chain_scan`、`existing_withdrawal_recovery→chain_scan`、`new_withdrawal_creation→existing_withdrawal_recovery`（保守固有依赖，禁止「只开入口不备处置」）、`event_publishing→chain_scan`）；依赖图变化属规格变更、不在配置面放开
- [x] T012 实现派生评估门禁于 `internal/recovery/gate.go`（唯一放行判定）：`release_valid(I,C,S) := I.state='open' && C∈闭集 && requires_capabilities(C) 全部放行 && isolation_dependency_set(C) 全部 state='verified' && 无 open gap 命中 affected_capabilities && 最新 release 行 decision='release' 且 evidence_generation=当前且 evidence_hash=当前且未被同 (I,C,S) 更晚 revoke 覆盖 && approvals_valid() && existing_fund_gates(P) 在真实动作处仍全部通过（本门禁不替代）`；无 open 实例→正常态直通（不改变日常运行；直通判定同样以控制库权威读取为准，缺必需配置按名拒绝）；有界 TTL 缓存（部署配置；非法/缺失按名拒绝、无默认）；**单动作准入协议（R3）**：①缓存命中与未命中均须在**本次动作准入前**读取控制库权威状态，并在共同锁（实例行锁，与决策写入同一锁）内按代次协议校验 `(state, evidence_generation, evidence_hash)`——缓存键含代次/哈希仅是可复用条件，**不构成最新性证明**；②缓存只复用仍有效的计算结果（当前代次/哈希、未撤销、门禁未失效），**不得复用旧 allow 跳过本次授权检查**；控制库不可达（含缓存过期）→拒绝；③顺序=判定点（锁内）→释锁点→实际动作：判定成立并释锁后再执行本次调用的唯一明确动作，**一次准入不得跨请求/循环步进/批次/异步重试复用**；④撤销先于准入→拒绝；准入后撤销→按在途＋unknown 规则处理（不追溯中止已提交工作、不回滚）并阻止后续准入；**缓存命中不得被追认为在途**；不宣称跨系统原子；⑤任何代次/哈希变化立即使相关缓存失效（发现者=本求值器，下一次真实动作前求值即拒绝，不得等 TTL）；区分「未准入」（拒绝、不产生动作）与「已在途」（按原门禁处理、未知结果按 unknown 纪律，不追溯中止已提交工作）；**控制库 schema 版本未知/不兼容→拒绝（经 B2/T069，不得缺此检查即称完成）**；拒绝分类闭集 `no_instance/instance_mismatch/no_release/release_invalidated_generation/release_revoked/capability_dependency_closed/isolation_unproven/gap_open/approval_missing/approval_identity_unverified/approval_executor_excluded/approval_stale/hard_gate_active/control_store_unavailable/scope_mismatch` 全部审计；无「关门禁」开关（依赖 T010、T011）；判权两阶段注记（F20）：本门禁=第①阶段（recovery allow），第②阶段=动作处既有原门禁独立评估；公式中 `existing_fund_gates(P)` 是对第②阶段的引用，禁止用本门禁替代/合并/短路原门禁（两阶段均须通过）
- [x] T013 [P] 实现证据代次与写写反序协议于 `internal/recovery/generation.go`：事务外取证捕获 `(state, evidence_generation, evidence_hash)` 令牌；提交在实例行锁内重读并逐项校验，不符→丢弃结果、仅写 `recovery_audit(result=discarded)`、不写结果行/不改缺口/不推进代次/不倒序覆盖；接受写入同事务 `evidence_generation = evidence_generation + 1` 并刷新 `evidence_hash`；触发代次推进的写入=核验批次结果/缺口闭合或建立/隔离项 verified 或 rejected/restore_probe 接受/证据快照接受；同秒并发以提交序为准、不以 `created_at` 裁决（复用 014 data-model §3.1 教训）（依赖 T007）

**Checkpoint（Phase 2）**: 控制库可独立初始化且版本守卫生效（未知/不兼容版本拒绝，T069）；身份注册与映射可操作；门禁在无实例时直通、有实例时默认拒绝且控制库不可达拒绝；代次令牌可丢弃过期提交。**Foundation ready — 所有 story 可开始**（合流 owner：T006 schema、T007 store＋T069 版本守卫、T009/T010 身份、T011–T013 门禁与代次分别合并后 story 才 rebase）。

---

## Phase 3: User Story 1 - 备份可恢复性：备份成功不等于恢复成功 (Priority: P1) 🎯 MVP

**Goal**: 每个「成功」备份必须经实际隔离恢复验证才可用；损坏/不完整/未验证/不兼容/缺依赖全部 fail-closed（FR-001–FR-008、FR-036；SC-001；US1 验收 1–3）

**Independent Test**: 受控备份样本（完整/损坏/截断/未验证/版本不兼容/缺依赖）验证只有完成实际恢复验证且兼容且依赖齐备的备份可被使用，其余全部 fail-closed；无需复服流程即可演示（quickstart S1/S2 + F1/F2/F3）。

### 批次 B5 · US1 测试先行（链 1/7）

- **前置**: B4 完成（T001–T003 提供骨架与配置键）。
- **实际入口**: 新建测试文件（见下）；真实 `pg_dump/pg_restore`（pinned `postgres:18.6-trixie` 镜像）。
- **完成条件**: 三个测试文件先落、在实现缺失时失败（TDD 先行），不得以替身代偿真实 dump/restore。
- **验证层次**: contract（`make test-contract`，普通 PR、无 Docker）+ PG integration（`make test-integration`，Docker 缺位 → NOT RUN）+ unit（secrecy，无 Docker）。
- **提交节点**: 测试先行批可单独本地 commit（不 push）。

- [x] T014 [P] [US1] 编写 manifest 契约测试 `internal/recovery/manifest_contract_test.go`（tags: contract；无 Docker）：字段缺失/哈希不符/未知 `manifest_version`→拒绝；`verification.state != verified` 不得用于恢复与复服；选择规则确定性（`created_at` 仅展示，`recovery_point.wal_lsn`/`backup_id` 决胜；禁止文件名/mtime/人工记忆）；禁止以备份频率、业务表 `MAX(created_at)`、backup 文件 mtime 证明 RPO；「快照元组或单时间戳≠完整恢复目标证明」——`backup_lag`/`uncovered_interval` 可 unknown 但不得省略；四项恢复验证检查（`readable`/`structure_constraints`/`business_state_probes`/`verification_executable`）缺一不得 `verified`；「仅文件存在/可列出」不得充当恢复验证
- [x] T015 [P] [US1] 编写备份/恢复 PG 集成测试 `internal/recovery/backup_integration_test.go`（tags: integration；真实 PG；Docker 缺位→NOT RUN 不得记 pass）：真实小规模 `pg_dump --format=custom --snapshot` → 隔离 `pg_restore`；证明 dump 实际使用导出快照（快照后并发写入不入 dump；manifest `recovery_point` 与 `pg_export_snapshot` 元组一致）；F1 损坏/截断/部分写入/哈希不符/未验证→拒绝且 0 次「尽力恢复」；F2 中断/部分完成→不标记 `restored`、重建目标 database 后幂等重跑、零双份/混合状态；F3 schema/程序版本不匹配→拒绝、0 静默降级/自动改写；`artifacts[]` bytes/sha256 不匹配→`rejected`；快照失效/导出失败→不得产出成功 manifest；restore 默认仅隔离目标、控制库 DSN 不可作目标
- [x] T016 [P] [US1] 编写保密性单测 `internal/recovery/secrecy_test.go`（无 Docker）：manifest/审计/日志不得含私钥、签名密钥、真实凭据或 DSN 明文；复用 `internal/logx/redact.go:34 Redact`；恢复工具只验证 Signer 边界可达性、不获取私钥（FR-007）

### 批次 B6 · US1 备份-恢复实现（链 1）

- **前置**: B5 测试已落并失败；B4 完成。
- **实际入口**: `internal/recovery/manifest.go`、`backup.go`、`restore.go`、`verifybackup.go`；CLI `internal/app/recoveryadmin/backup.go`、`restore.go`（汇合 `cmd/txharbor/main.go:56`）；兼容性复用 `internal/db/migrate.go:105 Inspect`、`:164 CheckCompatibility`。
- **完成条件**: quickstart S1/S2 正向走通；F1/F2/F3 收敛 fail-closed；「备份成功≠恢复成功」有证据；快照生命周期、dump 对快照实际使用、manifest 产物绑定、失败不发成功备份全部落地。
- **验证层次**: contract + PG integration（`make test-contract`、`make test-integration`）。
- **提交节点**: 本批退出时本地 commit（不 push）；US1 checkpoint 可验收。

- [x] T017 [US1] 实现 manifest 模型/校验/选择于 `internal/recovery/manifest.go`（data-model §2 + contracts/backup-manifest.md 逐字）：`manifest_version`（未知→拒绝）、`backup_id` UUID（生成时分配、全局唯一）、`created_at`/`created_by`、`carrier`=`{"kind":"pg_dump_custom","pg_server_version":"18.x","pg_dump_version":"18.x"}`（主版本不符→拒绝）、`coverage` 覆盖声明（数据 DB 权威对象清单）与排除声明（Redis/Kafka 非权威状态、Signer 私钥/真实凭据永不入库；缺一→拒绝）、`recovery_point` 快照元组（`xmin`/`xip`/`xmax` + export 时 `pg_current_wal_lsn` 上界 + `wall_clock` + `server`/`database`）、`schema`（`goose_db_version` 精确应用集）、`program`（生成时版本+最低兼容）、`artifacts[]`（`path`/`bytes`/`sha256`）、`verification`（`state`∈`unverified`/`verified`/`rejected`）；规范化 JSON 稳定排序后哈希；选择只接受 `verified` 且校验通过者
- [x] T018 [US1] 实现备份执行器于 `internal/recovery/backup.go`：`BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_current_wal_lsn(), pg_current_snapshot(), pg_export_snapshot(), now();` 保持事务打开，`pg_dump --snapshot=<id> --format=custom` 对导出快照执行（dump DSN 必须与快照导出 DSN 同一）；快照失效/过期/导出失败→显式失败且**不产出成功 manifest**（不得「失败发成功备份」）；产物 bytes+sha256 绑定 `artifacts[]`；失败不与既有产物混叠/覆盖；禁止发布事件、签名、广播、调用业务 RPC（FR-002/003/006/007；依赖 T017）
- [x] T019 [US1] 实现恢复执行器于 `internal/recovery/restore.go`：前置=open 实例 + manifest `verified` + 控制库存在绑定该 `backup_id` 的 `verified` 证据行（`recovery_evidence`，哈希/scope 与 manifest 一致；无可验证证据→拒绝或重跑 `verify-backup`；不得只信 manifest 文件内的 `verified` 旗标，F7/DG-2）+ 兼容性（`internal/db/migrate.go:105 Inspect`/`:164 CheckCompatibility`，精确版本语义）+ 依赖检查（目标 DSN 可达/权限、RPC 事实来源、broker/控制库可达、Signer 边界可达且不获取私钥）；任一缺失→明确阻塞状态+缺失项清单，不得宣称恢复完成；`--target-dsn` 默认仅隔离目标、生产主库必须显式声明并记录、不得等于控制库 DSN；`pg_restore --clean --if-exists` 于新建/重建 database；中断→不标记 `restored`，重试=重建目标 database 后重跑（幂等、零双份/混合）；restore_probe 四项（`readable`/`structure_constraints`/`business_state_probes`/`verification_executable`）通过才写 `restored` 证据；**验证生命周期（F7）**：备份级验证（manifest `verified`，绑定 backup_id/carrier/schema）≠ 目标实例级验证（`restore_probe` 绑定实例 + `data_target` 指纹）；复制 manifest、重建目标实例或更换目标（指纹变化）后必须重跑 probe，不得沿用旧 `verified`；**探针枚举（F9）**：`business_state_probes` 逐项对照 FR-002 九类权威对象——链身份/游标、事件、充值确认、提款请求与付款意图、出站交易与签名/广播记录、nonce 分配/占用、Outbox 与义务标记、消费者幂等与进度、审计与权限/014 差异；抽样须声明覆盖边界，不能证明的类别标 `unknown` 且不计入 `restored` 通过（FR-004/005/008；依赖 T018）
- [x] T020 [US1] 实现隔离恢复验证于 `internal/recovery/verifybackup.go`：`verify-backup` 在隔离目标执行真实 `pg_restore` + 四项检查（不是仅「能读文件」、不是重新初始化空库）；结论写回 manifest（`verified_at`/`verifier`/`target=isolated`/`evidence_ref`）与恢复控制库证据（`recovery_evidence`，kind=`backup_manifest`/`restore_probe`，按 data-model §1.4 实例绑定语义：绑定 open 实例；无 open 实例时先落 manifest 级验证结论，实例开启后经受控命令显式同步绑定并审计，不得谎称实例绑定——DG-2 已决议）；`business_state_probes` 按 T019 九类权威对象清单抽样并声明覆盖边界、不能证明标 `unknown`；回写 contracts/backup-manifest.md §3 的备份级/实例级验证生命周期（复制 manifest/重建实例/换目标指纹→重 probe，不得沿用旧 `verified`）；未验证仅可标记 `unverified`、不得作为恢复/复服依据；禁止以「备份命令退出 0」证明恢复成功（FR-006；依赖 T019）
- [x] T021 [US1] 实现 `recovery-admin backup/verify-backup` 接线于 `internal/app/recoveryadmin/backup.go`：主体绑定 `TXHARBOR_RECOVERY_PRINCIPAL`（必须命中参与者注册）；`TXHARBOR_RECOVERY_ARTIFACT_DIR` 必需、缺失按名拒绝；输出 manifest 路径与状态；`operation_id` 幂等；退出码明确（0 成功/1 配置或拒绝/2 用法）；不得以自由文本授权（依赖 T018、T020）
- [x] T022 [US1] 实现 `recovery-admin restore` 接线于 `internal/app/recoveryadmin/restore.go`：`--manifest M --target-dsn TARGET --instance ID`；前置检查失败→明确阻塞状态+缺失项，不宣称恢复完成；与 open 实例绑定执行（`TXHARBOR_RECOVERY_INSTANCE` 不匹配拒绝）；中断可安全重入（重建目标后重跑）；DSN 指纹脱敏记录、明文不入日志（FR-008；依赖 T019、T021）

**Checkpoint（Phase 3 · US1）**: quickstart S1/S2 通过、F1/F2/F3 fail-closed；受控样本恢复验证通过率与「备份成功≠恢复成功」证据就绪；contract+PG integration 层记录；**MVP 候选**；本地 commit（不 push）。S3–S12 依赖 US2–US6。

---

## Phase 4: User Story 2 - 隔离恢复与旧实例控制：默认不产生任何外部效果 (Priority: P1)

**Goal**: 恢复环境默认隔离；旧写入者/调度器/发布者/消费者停止与隔离可验证；未证明隔离不得开启任何写入/发送/投递；进程重启不自动解除（FR-009–FR-013、FR-032；SC-003；US2 验收 1–3）

**Independent Test**: 恢复环境未解除隔离、旧实例状态不明、隔离证据缺失三组条件下所有写入/发送/投递路径被拒绝；隔离证据齐备后仅指定能力可分级开放（quickstart S3/S4/S5 + F5）。

### 批次 B7 · US2 测试先行（链 2/4/5）

- **前置**: B4 完成；US1 的 restore 不阻塞本批测试编写（S4 的完整序列在 B9 后跑）。
- **实际入口**: 新建测试文件；受控控制库与恢复目标；真实入口（`serve`/`withdrawal-worker` 等）在 T026 中使用。
- **完成条件**: 四个测试先落并失败；隔离与拒绝的证据必须来自真实控制库/真实入口，禁用替身放行。
- **验证层次**: contract（无 Docker）+ PG integration + app 级 integration（Docker 缺位 → NOT RUN）。
- **提交节点**: 测试先行批可单独本地 commit（不 push）。

- [ ] T023 [P] [US2] 编写门禁契约测试 `internal/recovery/gate_contract_test.go`（tags: contract）：正常态直通（无 open 实例不改变日常运行）；open 实例下 7 能力默认拒绝；能力依赖闭包（`new_withdrawal_creation` 不得在 `existing_withdrawal_recovery` 未放行时放行）；拒绝分类闭集；TTL 非法/缺失按名拒绝、无默认；无「关门禁」开关/环境变量绕过；健康探针不得替代资金门禁判权；缺配置行为显式（禁漏配或普通启动绕隔离）
- [ ] T024 [P] [US2] 编写隔离检查集成测试 `internal/recovery/isolation_integration_test.go`（tags: integration）：checklist 状态机 `pending→evidenced→verified`（或 `rejected` 重采）；`verified_by` 必须 ≠ 本实例 executor；状态记录不能单独作为证明（必须带外部证据引用）；旧实例失联/状态不明/仅口头确认→无法 `verified`→放行拒绝；F5 负例：旧实例存活时写入/发送/投递 100% 拒绝并列出缺失隔离证据；不得以超时/失联推断已停止
- [ ] T025 [P] [US2] 编写控制库信任边界集成测试 `internal/recovery/controlstore/isolation_integration_test.go`（tags: integration）：控制 DSN = 数据 DSN 拒绝；数据 DB 备份产物（`pg_restore -l`）不含 `recovery_*` 对象；对数据 DB 恢复后控制库不受影响、旧实例批准对当前实例惰性（不自动重生效）；控制库不可达/丢失→门禁拒绝（fail-closed）；实例级灾难（同 PG 实例同失）按「重建控制事实后才可能放行」验证，不虚构独立性；证据范围限定逻辑回滚域（DG-3 边界）；**控制库回退负例（F6）**：盲恢复控制库不被支持；受支持控制恢复仅经停机隔离（执行者执行＋非执行者核验隔离项）＋显式重建/supersede＋审计；在已回退库内自写 supersede 行不得作为「可检/已重建」证明；断言旧实例批准对新实例惰性（instance_id 绑定）；测试不得据此宣称普适「旧批准不重生效」（该声明在控制库回退域保持限定/阻塞，见 DG-4）
- [ ] T026 [P] [US2] 编写真实入口默认拒绝集成测试 `internal/app/recovery_isolation_integration_test.go`（tags: integration）：open 实例下真实 `serve` 读路径/`POST /withdrawals`/`withdrawal-worker`/`event-publisher`/`event-consumer` 入口全部拒绝并暴露 `refusal_class`；原门禁保留（degradation/CapacityGate/ratelimit 次序不变）；进程重启不自动解除隔离（重读控制库）；`TXHARBOR_RECOVERY_INSTANCE` 绑定不匹配拒绝

### 批次 B8 · 恢复实例与隔离清单（链 2/4）

- **前置**: B4 完成（B7 测试可并行先行）。
- **实际入口**: `internal/recovery/instance.go`、`internal/recovery/checklist.go`；CLI `internal/app/recoveryadmin/instance.go`、`checklist.go`。
- **完成条件**: S3 开启实例、S5 checklist 可操作；close 守卫（全能力有效 release）与 supersede 语义落地；隔离依赖集判定可驱动门禁。
- **验证层次**: unit + PG integration。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T027 [P] [US2] 实现恢复实例生命周期于 `internal/recovery/instance.go` + `internal/app/recoveryadmin/instance.go`：`instance-open`（记录 executor、全局唯一 open、实例 ID 只增不改）；`instance-close`（仅在全 7 能力当前有效 release 后允许；本次实例发生过资金/投递能力 release 时需 dual 批准；任一能力被缺口阻塞→拒绝关闭、实例保持 open 并升级人工处置、不交付风险接受）；`supersede`（仅显式、双人批准、审计，新实例 ID）；`baseline` kind 仅文档化部署态、生产闸门只 arm `recovery`；进程/命令绑定 `TXHARBOR_RECOVERY_INSTANCE` 不匹配拒绝（data-model §4.1；依赖 T012）
- [ ] T028 [US2] 实现隔离检查清单于 `internal/recovery/checklist.go`：`item_key` 闭集 6 项（`old_writers_stopped`/`writer_fencing_observed`/`network_isolation`/`version_compatible`/`no_pre_release_effects`/`authorization_recheck`）；每项须 `evidenced`（执行者采集证据）+ `verified`（**非本实例 executor** 的参与者确认）；`checkpoint_summary` 不得单独作为证明、必须带外部证据；`rejected` 表示证据不足重新收集；能力放行要求其 `isolation_dependency_set` 全部 `verified`（000018 检查清单模式；FR-010/011/012；依赖 T027）
  （注：`no_pre_release_effects` 需以「实例 open 后 0 次外部可见动作」的门禁审计证据核验，不得以状态字段自证。`old_writers_stopped` 证据须点名覆盖 `reconcile-admin`（claim/dispose/reverify/scan/start/resume 等写路径）、`events-admin`（replay/unblock/retention-prune）、`withdrawal-exec` 操作员 CLI 与外部定时调度；上述路径无 015 运行时门禁接线，属程序边界（T064）：须附停服/下线/权限移除等可执行隔离手段证据＋门禁审计＋`no_pre_release_effects`；仅 checklist 签署不构成运行时隔离证明；无法证明→相关能力保持关闭。另记：在途交错边界——“同一次调用在 OpenInstance 前获准、在其后执行”不属于跨调用复用；后续（T028/T064 验收）须说明该在途动作如何被隔离流程等待、阻断或记录，以及何时才允许确认 `no_pre_release_effects`；不得仅以“下次求值拒绝”证明该交错已覆盖。）
- [ ] T029 [US2] 实现 checklist CLI 于 `internal/app/recoveryadmin/checklist.go`：`checklist-set`（采证）/`checklist-verify`（非执行者确认）；越权/自证/缺证据→拒绝并审计；主体绑定与 `operation_id` 幂等（依赖 T028）

### 批次 B9 · 七类能力接线（链 4）

- **前置**: B4、B8 完成（门禁与实例/清单可用）。
- **实际入口（逐项）**: ① query `internal/app/withdrawalhttp.go:227 ServeGET` + `internal/app/serve.go:473-474/479-480/488`；② chain_scan `internal/app/serve.go:319/331`、`:615 runServiceStreams`（lease/fencing `internal/indexer/lease.go:56-67`）；③ deposit_confirmation `internal/app/serve.go:350-353/380-398/411`；④ existing_withdrawal_recovery `internal/app/withdrawalworker.go:318/349/566`、`internal/app/withdrawalexec.go:35`（CLI `execOperatorOp`）、`internal/app/withdrawalexecution.go`（HTTP `POST /withdrawals/{request_id}/execution`，`serve.go:479`，直调 `execution.Admit`）、签名交付 `internal/app/signerserve.go:295 signer.Deliver` → `internal/signer/delivery.go:170 Deliver`/`:194 deliverGated`（`internal/signer/gates.go:123/162` 为 009 内部 gate 行，非交付接线点；行号按 HEAD 10e78f9 核对，实现期以实际为准）；⑤ new_withdrawal_creation `internal/app/withdrawalhttp.go:148`（`serve.go:466-467 guardRoute+CapacityGate`）；⑥ event_publishing `internal/app/eventpublisher.go:41` → `internal/events/publisher.go:177`；⑦ event_consuming `internal/app/eventconsumer.go:31` → `internal/events/consumer.go:351`（Effect `:639`；`internal/cache/invalidator.go:120`）。
- **完成条件**: 7 项逐项接线，调用点在「动作前」；正常模式直通、恢复模式默认拒绝；原门禁与状态机一律保留、不被门禁替代；缺配置行为显式；不得产生 Action 后置检查（先动后判）；受 T026 正反例覆盖；signer 交付以受支持装配为边界＋T070 进程内检查点（不得称全覆盖）；`reconcile-admin`/`events-admin`/定时调度为程序边界（T028/T064 书面边界，不声称运行时强制，F3）。
- **并行注记**: T070 在 T032 后串行（同改 `internal/app/signerserve.go` 装配与 `internal/signer` 核心，不带 [P]）——不重排批次与编号。
- **验证层次**: app 级 integration（`make test-integration`）+ unit 编译；drill 联合验收在 B17。
- **提交节点**: 本批退出时本地 commit（不 push）；US2 checkpoint 可验收。

- [ ] T030 [P] [US2] 接线 serve 链观察入口于 `internal/app/serve.go`：query 数据读路径（`:473-474`/`:479-480`/`:488`）在 HTTP 准入前、chain_scan（`:319/331`、`:615`）在循环启动检查＋包装步进回调（每步前调用注入门禁）、deposit_confirmation（`:350-353`/`:380-398`/`:411`）同上调用门禁；**回调/注入形态（F12）**：步进门禁以注入的 `gateCheck` 函数值/接口由 `internal/app` 装配传入 scanner，`internal/recovery` 根包不得传递 import `internal/indexer`（T001 包禁令），不得就地复制判定逻辑；保留 `indexer_pause`（`internal/indexer/scanner.go:1108/1148`）与 lease/fencing（`internal/indexer/lease.go:56-67`）等原门禁；区分正常 vs 恢复模式；禁止「一个开关恢复全部」（FR-009/012/021/025；依赖 T012）
- [ ] T031 [P] [US2] 接线提款写入口于 `internal/app/withdrawalhttp.go`：new_withdrawal_creation 在 `:148 ServePOST` 处理器准入前、且先于 `guardRoute`/`CapacityGate` 拒绝；query GET `:227`；缺配置/控制库不可达→拒绝（fail-closed）；原 007/013 门禁与限流次序不变、不得被 015 门禁绕过（FR-009/012/025；依赖 T012）
- [ ] T032 [P] [US2] 接线既有提款恢复与签名交付于 `internal/app/withdrawalworker.go` + `internal/app/withdrawalexec.go` + `internal/app/withdrawalexecution.go` + `internal/app/signerserve.go`：worker 认领/推进前（`:318/349/566`）、exec 每个操作前（`:35` + `execOperatorOp`）按 `existing_withdrawal_recovery` 及其 `chain_scan` 依赖判定；`internal/app/withdrawalexecution.go`（HTTP `POST /withdrawals/{request_id}/execution`，`serve.go:479`，`servePOST`→`execution.Admit`）在准入前独立接线（F2：该 HTTP 写路径与 CLI `withdrawal-exec` 的 `execOperatorOp` **非同漏斗**，不得以 CLI/worker 接线代表覆盖；正例=放行后仍走 011 原门禁 `can_execute`/execution gates；反例=拒绝时不产生 claim/不推进、审计 `refusal_class`）；签名交付前（受支持路径：`internal/app/signerserve.go:295 signer.Deliver` → 交付核心 `internal/signer/delivery.go:170 Deliver`/`:194 deliverGated`；`internal/signer/gates.go:123/162` 为 009 内部 gate 读取/评估行、非 015 交付接线点；行号按 HEAD 10e78f9 核对，实现期以实际为准）按 `existing_withdrawal_recovery` 及其 `chain_scan` 依赖判定；禁止重放历史签名/广播；nonce hold、发送/执行门禁、authorization/T-deliver 原门禁保留；既有重试/恢复入口在放行后按原门禁工作、在拒绝时不得推进进度或吞掉重试（FR-012/013/025）。**残余边界（F1，不得称全覆盖）**：app 包装层门禁只覆盖受支持的 `signer-serve` 装配；`internal/signer` 核心可被受支持装配之外的 in-process 调用直调——由 T070（进程内检查点）承接，仍以「受支持装配提供门禁依赖」为边界，非受支持装配/蓄意伪造门禁不自动检测（DG-1 同类）；worker/exec 门禁与 V8 授权面再核验为纵深。
- [ ] T033 [P] [US2] 接线事件发布入口于 `internal/app/eventpublisher.go`（→ `internal/events/publisher.go:177`）：claim 批次前与 settle 前调用门禁（`event_publishing` 依赖 `chain_scan`）；拒绝时不 claim/不 settle/不推进 outbox 进度；at-least-once 语义不变；不得发布真实下游事件（FR-009/012；依赖 T012）
- [ ] T034 [P] [US2] 接线事件消费入口于 `internal/app/eventconsumer.go`（→ `internal/events/consumer.go:351`，Effect `:639`；缓存失效 `internal/cache/invalidator.go:120`）：Effect 应用前与进度推进前调用门禁（`event_consuming`）；拒绝时不应用 Effect、不推进 offset/inbox；参考消费者 `refconsumer.go` 仅证据、非账本；effect class 范围在放行时校验（高影响档判定见 T050）（FR-009/012；依赖 T012）
- [ ] T070 接线 signer 进程内检查点（F1；本批 T032 后串行）：在 `internal/signer` 交付核心（`delivery.go` `Deliver`/`deliverGated`，交付字节离开前）以注入的门禁函数（`DeliveryDeps` 新增依赖字段；`submit.go` 签名路径酌情同构）执行 `existing_withdrawal_recovery`（含 `chain_scan` 依赖）动作前校验；门禁函数缺失/配置缺失按 fail-closed 拒绝，不得解释为直通；`signer-serve` 装配（`internal/app/signerserve.go`）提供同一 Gate 与控制库配置；不改变 009 门禁次序、unknown 语义与历史重放拒绝；残余边界：受支持装配之外直调 signer 核心/蓄意伪造门禁不自动检测（DG-1 同类），不得称全覆盖（依赖 T012、T032）

**Checkpoint（Phase 4 · US2）**: S3/S4/S5 正向走通（需 US1 restore）、F5 负例、T026 真实入口默认拒绝全绿；写入/发送/投递 0 次在隔离期发生；链 4 负例证据齐备；signer 进程内检查点（T070）与受支持装配边界落证；本地 commit（不 push）。

---

## Phase 5: User Story 3 - 恢复后事实核验：证据缺口保持未知 (Priority: P1)

**Goal**: 恢复后逐项核对本地与外部事实；DB 回退不被解释为外部效果回退；缺失不当作「从未发生」；无法证明保持 unknown 并产出人工处置证据包，受影响能力保持暂停（FR-014–FR-020；SC-002；US3 验收 1–5）

**Independent Test**: 构造「外部事实领先于恢复点」受控数据集（链上已确认付款、签名/广播已推进、下游已消费、恢复点缺记录），验证核验产出正确差异/未知清单、零重复付款、零错误事件效果，受影响能力在缺口闭合或经独立证明并按 FR-023 批准前保持关闭（quickstart S6/S7 + F4/F6）。

### 批次 B10 · US3 测试先行（链 6）

- **前置**: B4 完成；真实 PG/Anvil 可用（Docker 缺位 → NOT RUN）。
- **实际入口**: 新建测试文件；真实链（Anvil/本地 RPC）。
- **完成条件**: 三个测试先落；结论规则与缺口不变量被钉死；禁用替身代偿真实适配器。
- **验证层次**: contract（无 Docker）+ PG integration。
- **提交节点**: 测试先行批可单独本地 commit（不 push）。

- [ ] T035 [P] [US3] 编写核验结论契约测试 `internal/recovery/verification_contract_test.go`（tags: contract）：`consistent` 仅当证据完整、新鲜（配置容忍内）、覆盖闭合且全部来源一致；任一来源 unknown →整项至多 unknown；unknown/stale 不得当通过；缺失不构成「从未发生/从未付款/可重执行」；不得以默认值/旧状态/时间推断/「大概未发生」填补（FR-015/016/018）
- [ ] T036 [P] [US3] 编写 V1–V9 真实适配器集成测试 `internal/recovery/verification_integration_test.go`（tags: integration；真实 PG+Anvil）：外部领先→`divergent`/`unknown`+缺口；恢复点缺记录→`unknown`+gap；核验 0 次触发付款/签名/广播/重放/真实下游投递；批次/预算有界；V1–V9 全部经真实适配器（V5/V6 的 broker 不可读→unknown 由 T053 补事件层）；F4/F6；负例（F10）：历史 signed bytes 重放、历史广播重发、历史付款意图重执行、重复投递重发全部被拒绝（0 次重放/重广播/重投递/新建意图）；零写断言（F4，R1 细化）：①**隔离夹具（无并发业务写者）**下，核验批次执行前后对 V1–V9 读取链的数据 DB 表做**整表内容指纹 diff**（全行 `to_jsonb` 串接聚合的确定性摘要，不止行数；复用 `internal/txlifecycle/readonly_integration_test.go` 指纹模式），diff 必须为空；**静态清单**（表名以 `migrations/` 实际 schema 为准；实现期以 T039/T040 落地读取表同步增补，测试不得动态收集）：V1 `chain_blocks`/`erc20_transfer_logs`/`indexer_checkpoint`/`log_checkpoint`/`deposit_observations`/`deposit_observation_transitions`/`deposit_checkpoint`/`deposit_config_history`/`confirmation_policy_history`/`reorg_recovery`/`reorg_recovery_events`；V2 `withdrawal_requests`/`payment_intents`/`withdrawal_authorizations`/`withdrawal_authorization_scopes`/`request_status_projection`/`withdrawal_request_audit`/`withdrawal_grant_audit`；V3 `nonce_bindings`/`nonce_observations`/`nonce_scope_state`/`nonce_scope_holds`/`nonce_wallet_registry`/`nonce_binding_events`/`nonce_ops_audit`；V4 `signing_requests`/`signature_results`/`signing_request_audit`/`delivery_admissions`/`signer_caller`/`tx_attempts`/`tx_attempt_signings`/`tx_send_attempts`/`tx_receipts`/`tx_reconciliations`/`tx_attempt_events`/`tx_intent_freezes`/`execution_claims`/`execution_steps`/`execution_events`/`execution_ops_audit`/`execution_caller_permission`；V5 `outbox_events`/`event_obligation`/`event_system_state`/`event_ops_audit`；V6 `consumer_inbox`/`consumer_versions`/`consumer_progress`/`consumer_quarantine`；V7/V8 `recon_task`/`recon_checkpoint`/`recon_gap`/`discrepancy`/`discrepancy_occurrence`/`disposition`/`reverify`/`recon_audit`/`recon_scan_attempt`/`recon_permission`/`caller`/`api_key`；门禁/租约表（`indexer_pause`/`log_pause`/`deposit_pause`/`deposit_pause_audit`/`indexer_lease`）不属 V1–V9 读取链，另由既有门禁覆盖；②**控制库写入另列**：`recovery_verification_item`/`recovery_evidence`/`recovery_gap`/`recovery_audit` 与 `evidence_generation`/`evidence_hash` 推进是本核验的预期输出，按 append-only＋代次协议单独断言，**不混入数据 DB 零写**；③**生产/演练并发观察边界**：并发业务写者（serve/worker/publisher/consumer/execution 等）对数据 DB 的写入 ≠ 核验副作用——并发场景不得断言整表指纹为空，只断言「核验归因写入=0」（核验经只读角色/只读事务执行，并以数据库侧证据做归因），边界写入证据（FR-014–FR-017/020；SC-002）
- [ ] T037 [P] [US3] 编写缺口/独立性/升级集成测试 `internal/recovery/gaps_integration_test.go`（tags: integration）：证据包含对象/范围/时间线/现有证据/所需外部证据/受影响能力/风险/责任归属/升级记录；依赖无法证明独立→保守纳入暂停（不得仅凭「不同模块」判独立）；超时/重试耗尽/人工知悉不改变 `state`、不得视为闭合或获准复服；缺口闭合仅由新证据触发；不可重建场景=保持暂停并升级（FR-019；C3）

### 批次 B11 · V1–V9 核验与缺口实现（链 6）

- **前置**: B10 测试已落并失败；B4 完成。
- **实际入口**: `internal/recovery/verification.go`（编排+接口声明）、`internal/recovery/sources/chain.go`（V1–V4）、`internal/recovery/sources/ops.go`（V5–V9）、`internal/recovery/gaps.go`；CLI `internal/app/recoveryadmin/verify.go`。
- **完成条件**: S6 可重复有界步进产出 V1–V9 结论；S7 缺口阻塞放行；核验只读、永不触发付款/重放/重广播；适配器落在 `sources` 子包（测试构建环约束，见 T001）。
- **并行注记（F15）**: T039/T040 串行现状保留（同属 `internal/recovery/sources/` 适配器单一 owner，接口冻结/owner 确认前不并行、不加 [P]）——不重排批次与编号。
- **验证层次**: contract + PG integration（+ 事件层在 B15）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T038 [US3] 实现核验编排与持久化于 `internal/recovery/verification.go`：定义只读来源接口（适配器实现于 `internal/recovery/sources/`，防 `package app` 测试构建环）；`recovery_verification_item` 落库（`category` V1–V9、`object_key` 稳定身份、`sources` JSONB、`conclusion` 闭集、`reason`、`evidence_refs`、`observed_at`，只追加、重核写新行）；新鲜度容忍与核验批次上界（配置；缺失→保守 unknown，不得默认放行）；结论规则强制（unknown≠通过）；核验及其触发的动作 MUST NOT 直接触发付款（FR-014/018/020；依赖 T013）
- [ ] T039 [US3] 实现 V1–V4 只读适配器于 `internal/recovery/sources/chain.go`：V1 链事实 vs PG（RPC canonical + `chain_blocks`/`erc20_transfer_logs`/`indexer_checkpoint`/`log_checkpoint`/`deposit_*`/006 `reorg_recovery` 只读；孤块/确认未达/不可达→unknown）；V2 提款请求/付款意图/授权（`withdrawal_requests`/`payment_intents`/`withdrawal_authorizations` + 007 审计；缺失→unknown+gap，不得新建意图）；V3 nonce 分配/占用（008 表 + `eth_getTransactionCount`；只读观察、绝不自动重分配）；V4 签名/广播结果含 unknown（`signing_requests`/`signature_results`/`tx_attempt_signings`/`tx_send_attempts`/`tx_receipts`/`tx_reconciliations` + 链回执；**只读方法白名单（F4）**：`AttemptByID`（`store.go:264`）、`UnknownRecovery`（`reconcile.go:183`，内部只读事实 `sendFacts`/`reconcileFacts`）、`Status`/projection 读与回执只读查询；**MUST NOT 调 `Reconcile`**（`reconcile.go:62`：写 `tx_reconciliations` 观察行+状态转换）、`Release`、`Send`、`applyReceipt` 等写路径；方法名含查询/恢复不构成只读证明；核验前后权威表零写由 T036 断言）（FR-014/015/016/017；依赖 T038）
- [ ] T040 [US3] 实现 V5–V9 只读适配器于 `internal/recovery/sources/ops.go`：V5 Outbox 与义务标记（`outbox_events`+`event_obligation`（只增不减）+publisher 进度；broker 不可读/保留裁剪→unknown）；V6 消费者幂等/进度/隔离（`consumer_inbox/versions/progress/quarantine` + broker committed offset 可读时的回退检测）；V7 014 差异/复核/处置/权限/审计（000016 表只读引用）；V8 授权面漂移（控制库记录的外部真源证据 + 再核验/再施加结果；无外部真源→unknown→资金与投递能力保持关闭）；V9 工具/依赖就绪（镜像/客户端/DSN/Signer 边界可达性）（FR-014/020/034；依赖 T038）
- [ ] T041 [US3] 实现证据缺口与证据包于 `internal/recovery/gaps.go`：`recovery_gap` 逐字（`state` CHECK(`open`,`closed`,`escalated`)、`affected_capabilities` TEXT[] 至少含直接关联能力、`dependency_proof` JSONB nullable 路径+逐边证据、`closed_by`/`closure_evidence`、`owner`/`escalation_ref`）；缺口闭合仅由新证据；timeout/attempts_exhausted/acknowledged 作为审计理由记录、不改变 `state`；证据包可导出供人工处置；暂停范围含依赖/可能放大副作用的能力；可证明独立能力放行须记录范围、依赖核验、有效证据与批准结果，且不得间接启动被暂停的付款/签名/广播/真实下游副作用；不交付风险接受/核销/补偿付款/自动补造意图（FR-019；C3；依赖 T038）
- [ ] T042 [US3] 实现 `recovery-admin verify` 接线于 `internal/app/recoveryadmin/verify.go`：`--instance --scope`；重复有界步进收敛、无内存态；输出分列结论与缺口清单；操作幂等（`operation_id`）；越权/缺配置拒绝并审计；发现缺口时不得自动修复、不得触发付款（FR-014/020；依赖 T038）

**Checkpoint（Phase 5 · US3）**: S6/S7 通过、F4/F6 fail-closed；「外部事实领先」场景零重复付款/零错误事件效果；缺口保持 unknown+证据包；本地 commit（不 push）。

---

## Phase 6: User Story 4 - 分级复服：三类状态分开，逐项放行 (Priority: P2)

**Goal**: restored/verified/released 三态分离（spec FR-022 称 approved；`released`=派生放行，`approved` 仅指批准记录）；7 能力各自独立开放条件；审批规则按 2026-09-28 裁决（执行/核验/批准独立、高影响双人非执行者、批准绑定证据版本、硬门禁不可覆盖）；放行幂等可重入；无「一个开关恢复全部」（FR-021–FR-026；SC-004/007；US4 验收 1–5）

**Independent Test**: 数据库已恢复但核验未完成、核验完成但未获准、获准但范围受限三种状态下各能力开关独立性与拒绝/放行结果；批准规则（双人/单人、非执行者限制）、证据变化导致批准失效、中断重入幂等（quickstart S8/S9/S10/S11 + F7）。

### 批次 B12 · US4 测试先行（链 3/4/5）

- **前置**: B4 完成（T047 依赖 T013/T012；release 路径由 B13 落地，测试先失败）。
- **实际入口**: 新建测试文件；真实控制库；身份映射须显式注册（禁以任意 `person_id` 证双人独立）。
- **完成条件**: 五个测试先落并失败；F7 拒绝矩阵与链 5 交错场景钉死（含 R3 单动作准入受控交错：命中后撤销/证据与身份映射变化/控制库不可达/撤销先于准入/准入后撤销＋后续重求值）。
- **验证层次**: contract + PG integration + app 级 integration。
- **提交节点**: 测试先行批可单独本地 commit（不 push）。

- [ ] T043 [P] [US4] 编写审批有效性契约测试 `internal/recovery/approvals_contract_test.go`（tags: contract）：single/dual 两档；dual=两条不同 principal 的 approve 且 `person_id` 互异（同一人双账号不算两人）；executor 排除按实例记录（不按「用户名不同」推断）；role 不符/principal 未注册/映射缺失→拒绝（无法证明不同人）；代次/哈希不符→失效；revoke 覆盖本 principal 先前 approve；硬门禁（缺证/未隔离/未知付款结果/开放缺口/既有资金门禁激活）不得被批准覆盖；仅适用 015 灾备后复服、不新增紧急绕过/管理员强制入口
- [ ] T044 [P] [US4] 编写放行/撤销/三态/幂等 PG 集成测试 `internal/recovery/release_integration_test.go`（tags: integration）：S8/S9 未获授权拒绝并审计；新提款创建/既有提款恢复/真实下游投递及可产生真实下游业务效果的消费恢复缺第二人 0 次放行；执行者自批 0 次；证据变化后旧批准 100% 失效、须重核重批；重复 approve/release/close ≥10 次零状态翻转；撤销必须显式、有授权、审计；`restored` 不得显示为 `verified`、`verified` 不得显示为 `released`（F16 术语）；F7（FR-022/023/024；SC-004/007）
- [ ] T045 [P] [US4] 编写真实入口放行正例集成测试 `internal/app/recovery_release_integration_test.go`（tags: integration）：S8 前 `POST /withdrawals` 必须 503（`no_release` 类拒绝）；single 放行 `query` 后读路径可用而写路径仍拒绝；按依赖顺序 `chain_scan→deposit_confirmation→existing_withdrawal_recovery` 逐项放行；放行不得解锁/替代既有资金门禁（signer 交付、nonce hold、发送/执行门禁仍独立校验）；0 次直写放行（INV-2）
- [ ] T046 [P] [US4] 编写身份与授权路径验收测试 `internal/recovery/identity_path_integration_test.go`（tags: integration）：本地可操作身份配置——显式注册两个 person 的两个 principal 并完成双人批准；主体验证走部署受控配置（`TXHARBOR_RECOVERY_PRINCIPAL`）；缺映射/未注册 principal 拒绝；执行/核验/批准权限独立（一人可多 role，但 executor 不得批准自己执行的实例）；**禁止以任意 `person_id` 证明双人独立**（必须以显式映射+不同 person 证明）；部署名单内容待裁决、不阻塞本测试；**映射变更（F19）**：变更后旧批准立即失效（重核重批），变更审计可查（谁/何时/改了哪条映射），`person_id` 与当前映射不一致的批准按身份不可信拒绝（`approval_identity_unverified`）；保守 dual 不得抵消错映射（映射冲突时 0 放行）
- [ ] T047 [P] [US4] 编写代次与失效受控交错集成测试 `internal/recovery/generation_integration_test.go`（tags: integration；链 5）：写写反序——A 取证→B 取证并先提交→A 提交被丢弃且不写结果/不改缺口/不推进代次、仅 `result=discarded` 审计；批准后撤销→下一求值拒绝；检查后执行（隔离项被 rejected 后放行停止）；旧实例续跑/迟到核验→丢弃+审计；TTL 语义——有界、撤销后至迟于 TTL 到期/下一次求值拒绝，不得未经裁决把 TTL 变为「撤销后仍可执行」；TTL 到期+控制库不可达→拒绝；**单动作准入协议受控交错（F5/R3，逐项）**：①缓存命中后撤销——命中不得跳过本次准入的权威读取与授权校验，撤销后下一次准入拒绝；②证据/身份映射变化——代次/哈希/映射变化后下一次求值（未等 TTL）立即拒绝，缓存内旧代次放行不得用于新动作；③控制库不可达（含缓存过期）→拒绝；④撤销先于准入→拒绝；⑤准入后撤销→按在途＋unknown 纪律（不追溯中止已提交工作、不回滚），并阻止后续准入；后续重求值仍拒绝；缓存命中不得被追认为在途；⑥一次准入不得跨请求/循环步进/批次/异步重试复用；「未准入」（拒绝、无动作）与「已在途」（原门禁+unknown 纪律、不追溯中止）分别验收；外部副作用不可回滚：未知结果保持 unknown、不自动重付/重广播/重投递（FR-024；SC-007；INV-3/5/7）

### 批次 B13 · 审批与放行实现（链 3/4/5）

- **前置**: B12 测试已落并失败；B4 完成（控制库版本守卫经 B2/T069，不得在未知/不兼容版本上做审批或放行）。
- **实际入口**: `internal/recovery/approvals.go`、`release.go`、`scope.go`；决策行写 `recovery_approval`/`recovery_release`（append-only）。
- **完成条件**: approvals_valid 与 release 生效条件完整；高影响 effect class 保守分类；close/supersede 守卫；全部经派生评估、无行上布尔；控制库版本守卫经 B2/T069（T048–T051 不得缺此检查即称完成）。
- **并行注记（F15）**: T049/T050 串行现状保留（同属审批/放行单一 owner，接口冻结/owner 确认前不并行、不加 [P]）——不重排批次与编号。
- **验证层次**: unit + PG integration。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T048 [US4] 实现审批有效性于 `internal/recovery/approvals.go`（§3.1 逐字）：高影响档（dual）存在两条决策序最新 approve（不同 principal）且两人都有 `approver` role、两人 `person_id` 互不相同、两人 `person_id` 均 ≠ executor.person_id、两行 `scope_hash`/`evidence_generation`/`evidence_hash` 与当前一致、两行 `person_id` 与当前身份映射一致（映射变更→旧批准失效重批，F19）、均未被各自 revoke 覆盖；其余能力 single 档同条件一条；映射缺失/未注册/role 不符/执行者自批/代次不符/映射不一致/含 revoke→不满足；有效性派生、不在行上存布尔；append-only + `operation_id` 幂等（FR-023；依赖 T013；控制库版本守卫经 B2/T069）
- [ ] T049 [US4] 实现放行/撤销决策于 `internal/recovery/release.go`：`recovery_release` 逐字（`decision` CHECK(`release`,`revoke`)、`approval_refs` UUID[] 确定性排序、`evidence_generation`/`evidence_hash` 绑定、`operation_id` UNIQUE、append-only）；当前有效放行=最新 release 未被同 (I,C,S) 更晚 revoke 覆盖且 §3 条件全部成立、**每次门禁求值重算**；撤销显式且有授权审计；close 前置（全 7 能力有效 release；缺口未闭合不得 close）；supersede 仅显式双人批准；重复 release/revoke/close 按 `operation_id` 读回零翻转（FR-021/022/024；依赖 T048；版本守卫经 B2/T069）
- [ ] T050 [US4] 实现能力范围/effect class 保守分类于 `internal/recovery/scope.go`：规范化 `scope_hash`（链/资产/业务类型/能力）；高影响档判定（新提款创建、既有提款恢复、向真实下游投递、可产生真实下游业务效果的消费恢复→dual）；真实下游 effect class 清单属部署前裁决——未配置/未知一律保守按 dual、不得默认 single；范围匹配失败→`scope_mismatch` 拒绝（FR-021/023；依赖 T048；版本守卫经 B2/T069）

### 批次 B14 · 放行/状态 CLI（链 4）

- **前置**: B13 完成。
- **实际入口**: `internal/app/recoveryadmin/approve.go`、`internal/app/recoveryadmin/status.go`。
- **完成条件**: `approve/release/revoke` 与 `status` 可操作；status 逐能力显示三态+阻塞原因+`refusal_class`；诚实报告（未核验/回退数据不显示为正常一致；健康探针不替代资金门禁判权）；S10/S11 通过。
- **验证层次**: PG integration + app 级 integration。
- **提交节点**: 本批退出时本地 commit（不 push）；US4 checkpoint 可验收。

- [ ] T051 [US4] 实现 `recovery-admin approve/release/status` 接线于 `internal/app/recoveryadmin/approve.go` + `internal/app/recoveryadmin/status.go`：approve/revoke/release/revoke-release 校验对应审批记录（无有效批准不得 release）；主体绑定+自由文本仅审计+`operation_id` 幂等；`status` 只读、逐能力显示 `restored/verified/released` 三态与阻塞原因、拒绝分类；不得把未核验或回退后数据显示为正常一致；健康探针不得替代资金门禁判权；**有界只读复核 bounds（F13）**：定义可读范围（本实例及授权 scope 内的证据/核验/缺口/审计行；禁止无界全表扫描）、次数/时间/资源预算（部署配置，本地值仅测试输入）、耗尽行为（拒绝后续复核＋审计，不改变缺口/实例/批准/放行状态）；超时/预算耗尽/人工知悉≠缺口闭合/获准复服（FR-019）；术语（F16）：对外状态统一 `released`＝派生放行，`approved` 仅指有效批准记录、不构成放行（FR-022/026；依赖 T048–T050；版本守卫经 B2/T069，缺此检查不得称完成）

**Checkpoint（Phase 6 · US4）**: S8/S9/S10/S11 通过、F7 拒绝矩阵全绿；7 能力独立条件与审计记录齐备；证据失效重批可观察；本地 commit（不 push）。

---

## Phase 7: User Story 5 - 事件与下游边界：回退后果可检测，不宣称恰好一次 (Priority: P2)

**Goal**: 备份回退造成的事件重复、inbox/offset 回退、历史幂等记录缺失可检测、可保守处理；不宣称跨系统恰好一次；未接真实回执不宣称外部账本一致或已恢复（FR-027–FR-029；US5 验收 1–3）

**Independent Test**: 注入重复事件、offset 回退、幂等记录缺失三组场景，验证重复被吸收、回退被检测、缺失不被静默处理，且所有对外结论限定在本项目可验证范围（quickstart §5；F4 事件侧）。

### 批次 B15 · 事件与下游边界（链 6/7）

- **前置**: B11 完成（V5/V6 结论可用）。
- **实际入口**: `internal/recovery/boundary.go`；事件层真实 broker（Redis/Kafka，compose `events` profile）；`internal/events/recovery_boundary_integration_test.go`（tags: integration_kafka；Docker 缺位 → NOT RUN）。
- **完成条件**: 回退可检测、重复被幂等吸收、缺失不静默补写、外部重复效果可检测可报告；0 跨系统恰好一次声明。
- **验证层次**: unit + PG integration + Kafka 层（`make test-integration-kafka`）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T052 [P] [US5] 实现事件/下游边界报告于 `internal/recovery/boundary.go`：回退检测（PG 进度 vs broker committed offset，broker 不可读→unknown）；重复事件由既有 `consumer_inbox`/`consumer_versions` 幂等吸收、缺失的历史幂等记录不得静默补写为「已处理」、不得据缺失判定可重执行；下游去重稳定身份=`event_id`+source 三元组与检测/处置边界；可能的外部重复效果可检测可报告；MUST NOT 宣称跨系统恰好一次；未接入真实上游/下游回执时只声明本项目可验证范围（FR-027/028/029；依赖 T040）
- [ ] T053 [P] [US5] 编写事件层回退/重复集成测试 `internal/events/recovery_boundary_integration_test.go`（tags: integration_kafka）：真实 broker；offset/inbox 回退被检测；重复消费幂等吸收零错误效果；缺失记录不触发有副作用的重处理；重新处理只能经既有 quarantine replay 等授权路径；Docker 缺位 → NOT RUN（FR-027；SC-006 事件侧）
- [ ] T054 [US5] 编写应用层事件边界集成测试 `internal/app/recovery_eventboundary_integration_test.go`（tags: integration）：恢复后 `event-publisher`/`event-consumer` 路径门禁+回退检测可观察；重复不产生额外真实效果；未核验不显示正常；参考账本仅证据、非账本（FR-027/028/029；依赖 T052）

**Checkpoint（Phase 7 · US5）**: 重复/回退/缺失三组场景全部收敛保守；对外结论限定本项目范围；本地 commit（不 push）。

---

## Phase 8: User Story 6 - 演练、指标与失败路径：隔离环境可复现 (Priority: P2)

**Goal**: 隔离环境可复现演练「恢复→核验→拒绝不安全复服→条件满足后分级复服」；分列记录恢复/核验/各能力放行时间与备份滞后、未覆盖区间；7 类失败路径 fail-closed；演练独立通道（FR-030–FR-033、FR-036；SC-005/006/007/008；US6 验收 1–3）

**Independent Test**: 受控备份+注入失败路径，独立运行演练流程，检查时间口径/指标与每类失败路径的 fail-closed 结果；不依赖生产（quickstart S12 + F1–F7 + `make test-drill`）。

### 批次 B16 · 指标与演练模型（链 7）

- **前置**: B11、B14 完成（核验与放行路径可用）。
- **实际入口**: `internal/metrics/recovery.go`（复用 `internal/metrics` registry）；`internal/recovery/drill.go`；CLI `internal/app/recoveryadmin/drill.go`。
- **完成条件**: 三段计时分开、RTO 超时处理落地（记录不达标+告警+升级，不单独永久禁止安全复服）；drill 记录字段与约束完整；未配置必需约束标注 `constraints_configured=false`。
- **验证层次**: unit + PG integration。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T055 [P] [US6] 实现恢复指标于 `internal/metrics/recovery.go`：有界低基数（ENUM label：capability/refusal_class/result；无原始 ID/tx hash）；放行/拒绝/缺口/丢弃/重试计数；度量分开记录并分类——`recovery point`、`db_restore_time`、`verification_time`、`per_capability_release_time`、`backup_lag`、`uncovered_interval`、缺口数量与处置状态；**不得以数据库可连接宣称 RTO 达标**；若配置了时间目标，超时 100% 记不达标+告警+升级且不单独永久禁止后续安全复服；未配置必需约束→显式「未配置」状态（FR-031/036；C1）
- [ ] T056 [P] [US6] 实现演练记录模型于 `internal/recovery/drill.go`：`recovery_drill_run` 逐字（`scenario` 含 7 类失败注入标识、`recovery_point` JSONB、`db_restore_seconds`/`verification_seconds` NUMERIC 分开、`capability_release_seconds` JSONB per capability、`backup_lag`/`uncovered_interval` JSONB、`constraints_configured` BOOL、`test_inputs` JSONB、`gap_counts` JSONB、`result` CHECK(`ok`,`refused_safe`,`failed_injected`)、`log_ref`）；不写生产阈值承诺；时间口径分开、禁止用 `db_restore_seconds` 单独宣称 RTO 达标（FR-030/031/036）
- [ ] T057 [US6] 实现 `recovery-admin drill` 接线于 `internal/app/recoveryadmin/drill.go`：隔离环境真实恢复流程（不得以重新初始化空库冒充恢复）、S12 分列记录、可重复且结果可存档；未配置必需约束标注用途与未配置状态；演练产物引用 `docs/evidence/015/`（FR-030；依赖 T055、T056）

### 批次 B17 · 独立演练通道（链 1/6/7）

- **前置**: B16 完成（B5/B6/B9/B11/B14/B15 已交付）。
- **实际入口**: `internal/recovery/drill_e2e_test.go`、`drill_failure_test.go`、`drill_idempotence_test.go`（tags: drill）；真实 Anvil/PG（+事件层 Kafka）；`.github/workflows/drill.yml`；`Makefile`。
- **完成条件**: S1–S12 + F1–F7 在独立通道全绿；含「缺口无法补齐→保持暂停」场景（非所有演练必须全复服）；重复 ≥10 次零副作用；`make test-drill` 可独立并行、永不进普通 PR。
- **验证层次**: drill 独立通道（`make test-drill`）。
- **提交节点**: 本批退出时本地 commit（不 push）；US6 checkpoint 可验收。

- [ ] T058 [P] [US6] 编写端到端演练测试 `internal/recovery/drill_e2e_test.go`（tags: drill；独立通道）：真实流程「生成备份→外部推进（链上确认付款/签名广播/下游消费）→恢复旧数据→核验发现缺口并拒绝不安全复服→有证据按审批恢复指定能力」；分列记录恢复点/DB 恢复时间/核验时间/各能力放行时间/backup_lag/uncovered_interval；**含「缺口无法补齐→保持暂停」验收场景**（不得为过测全复服）；0 重复付款、0 错误事件效果；禁止直写批准/放行/缺口绕过命令（quickstart §4 反作弊纪律）
- [ ] T059 [P] [US6] 编写 7 类失败注入矩阵测试 `internal/recovery/drill_failure_test.go`（tags: drill）：F1 备份不可用/损坏/截断/未验证；F2 恢复中断/部分完成；F3 版本/schema 不兼容；F4 外部事实领先；F5 旧实例未隔离/无法证明；F6 核验发现无法证明的缺口；F7 越权/证据不足/过期批准——每类收敛到明确 fail-closed、可观察、可审计、可重入，0 错误开放、0 重复外部副作用；缺口类保持 unknown/pending 并记录证据包/责任归属/升级，超时/重试耗尽/人工知悉不得视为闭合（FR-032；SC-006；quickstart §2）
- [ ] T060 [P] [US6] 编写演练幂等测试 `internal/recovery/drill_idempotence_test.go`（tags: drill）：restore/verify/approve/release/close 重复执行（含中断后重入）≥10 次，外部副作用与状态翻转 0 次；撤销必须显式、有授权、审计；已开放与未开放项可分别观察（FR-024；SC-007）
- [ ] T061 [US6] 定稿 drill 通道与文档对照：`.github/workflows/drill.yml`（独立 schedule/dispatch、可并行、不阻塞普通 PR）+ `Makefile` `test-drill` + `specs/015-backup-recovery-safe-resumption/quickstart.md` §3 分层表核对；演练产物存档路径与 NOT RUN 纪律（Docker 缺位不得记 pass）写入 workflow 注释（FR-033；SC-008；依赖 T058–T060）

**Checkpoint（Phase 8 · US6）**: `make test-drill` 独立通道 S1–S12+F1–F7 全绿（含保持暂停场景）；普通 PR 通道不受影响；本地 commit（不 push）。

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: CI 分层、状态诚实性、运维文档与证据归口、声明收口（不含风险接受功能；不宣称生产阈值）。

### 批次 B18 · CI 分层与运维证据归口（链 7）

- **前置**: B17 完成（US1–US6 全部可验收）。
- **实际入口**: `.github/workflows/ci.yml:123`（pg 分类臂）；`Makefile`；`docs/ops/recovery-runbook.md`（新建）；`docs/evidence/015/README.md`（新建）。
- **完成条件**: `internal/recovery/*` 进入 pg 分类且分类遗漏有反向守卫；drill 不进 PR；运维文档含配置示例/真实命令/隔离证明/失败处置；证据索引建立。
- **验证层次**: CI 配置检查 + 文档核对（无 Docker / 无长测）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T062 [P] CI 分层补齐：`.github/workflows/ci.yml:123` pg 分类臂增列 `internal/recovery/*`（含 `internal/recovery/sources/*`），加分类遗漏反向守卫（未分类内部路径显性红）；确认 `drill` 标签测试不在普通 PR 任何 job；`Makefile` `test-contract` 路径增补 `./internal/recovery`（FR-033；SC-008）。（行号注记 F17：`:123` 为 HEAD 10e78f9 的 `is_integration_pg_path()` 路径集所在行；实现期 MUST 按实际文件复核/更新，不得机械沿用）
- [ ] T063 [P] 状态面诚实性审计 pass 于 `internal/app/serve.go` + `internal/app/degradation.go`：恢复期对外诚实报告（哪些能力可用/被隔离/暂停/核验未知）；查询与健康信号不得把未核验或回退后数据显示为正常一致；健康探针不替代资金门禁判权；`restored/verified/released` 三态不互相冒充（FR-022/026）；有界只读复核（F13）：状态/复核查询受范围+次数/时间/资源预算约束，预算耗尽→拒绝＋审计、不改变任何状态，超时/耗尽≠缺口闭合
- [ ] T064 [P] 运维文档 `docs/ops/recovery-runbook.md`：配置键示例（标注本地值仅测试输入、生产阈值未裁决）、真实命令调用路径（S1–S12 对应 `recovery-admin` 命令）、隔离证明（控制库独立 DSN/不属于数据备份集/门禁 fail-closed）、失败处置（F1–F7 收敛动作与升级路径）、RTO 超时处理（记录不达标/告警/升级，不永久禁后续安全复服）（FR-031/033/036；C1）。**本轮 analyze 增补**：①（F3）`reconcile-admin`（claim/dispose/reverify/scan 等写路径）、`events-admin`（replay/unblock/retention-prune）与外部定时调度为程序边界：015 无运行时门禁接线，隔离靠停服/下线/权限移除＋审计＋`no_pre_release_effects` 证据；仅 checklist 签署不构成运行时隔离证明；无法证明→相关能力保持关闭；②（F6）控制库回退纪律：禁盲恢复；仅经停机隔离（执行者执行＋非执行者核验隔离项）→显式重建/supersede＋审计；写明执行者失去旧权限的确认方式（旧 DSN/凭据吊销或新库新凭据，记录时间与主体）、建新实例的可信依据（部署受控配置＋恢复点证据＋重新隔离清单）与旧实例不可继续工作的条件（instance_id 只增、绑定不匹配、旧库标 retired）；不得在已回退库内写 supersede 即称可检；③（F14/DG-3）同实例储层同失超出逻辑回滚域：明示不提供实例级独立性，备份落盘/异地策略待裁决（不阻塞实现）；④（F19）身份映射维护边界与变更审计：映射变更→相关批准失效重批，保守 dual 不抵消错映射；生产名单待部署，本地路径见 T010/T046；⑤（F13）有界只读复核 bounds（T051/T063）写入 runbook；⑥（在途交错）“同一次调用在 OpenInstance 前获准、在其后执行”不属于跨调用复用：runbook 须说明该在途动作如何被隔离流程等待、阻断或记录，以及何时才允许确认 `no_pre_release_effects`；不得仅以“下次求值拒绝”证明该交错已覆盖
- [ ] T065 [P] 证据索引 `docs/evidence/015/README.md`：演练/核验/复服/隔离证据归口与命名、NOT RUN 记录要求、测试输入标注要求、反作弊纪律（禁直写批准/放行/缺口；禁替身放行；真实 dump/restore/链/中间件）；与 `docs/evidence/014/` 模式对齐

### 批次 B19 · 收口验证与声明（链 7 + 全链）

- **前置**: B18 完成。
- **实际入口**: `docs/evidence/015/quickstart_matrix_evidence.md`（新建）；`specs/015-backup-recovery-safe-resumption/`。
- **完成条件**: quickstart 矩阵分层执行并如实记录；文档交叉链接与声明收口；保密边界复核通过。
- **验证层次**: 综合（unit/contract/普通 PR integration + drill 分开记录；未运行项记 NOT RUN）。
- **提交节点**: 本批退出时本地 commit（不 push）。

- [ ] T066 执行 quickstart 验证矩阵并记录到 `docs/evidence/015/quickstart_matrix_evidence.md`：S1–S12 正向与 F1–F7 失败注入，按层记录（unit/contract 普通 PR；PG integration 按路径；drill 独立通道）；Docker 缺位 → NOT RUN 且不得记 pass；未执行项不得记通过；结果仅本地范围、非生产阈值（FR-033；SC-005/008）
- [ ] T067 [P] 文档收口：`specs/015-backup-recovery-safe-resumption/plan.md`、`research.md`、`data-model.md`、`contracts/*`、`adr/*` 交叉链接与口径复核；T000-P OPEN 复述；风险接受/核销/补偿付款/自动补造意图明确缺席；生产 RPO/RTO/频率/保留未裁决复述（FR-035）
- [ ] T068 [P] 私钥/凭据边界全量复核 `internal/recovery/**` + `internal/app/recoveryadmin/**`：备份/证据/日志/审计禁含私钥、签名密钥、真实凭据、DSN 明文；签名密钥继续留在 Signer 边界、恢复环境不为恢复获取可直接持有的私钥；复用 `internal/logx.Redact`；复核结论写入 `docs/evidence/015/README.md` 证据索引（FR-007；INV-9）

**Checkpoint（Phase 9）**: 分层验证记录齐全（NOT RUN 如实）、文档与证据索引收口、无生产就绪声明；可交付本地范围。

---

## 覆盖映射（任务与验证证据）

### FR-001–036 → 任务

| FR | 任务 | 主要验证证据（层次） |
|---|---|---|
| FR-001 备份身份/元数据/选择规则 | T001、T014、T017 | contract（manifest 校验/选择）；PG integration（dump 快照绑定） |
| FR-002 覆盖范围声明 | T014、T017 | contract（覆盖+排除声明缺一拒绝） |
| FR-003 完整性可验证 | T015、T017、T020 | PG integration（损坏/截断/哈希）；F1 |
| FR-004 schema/程序兼容 | T015、T019 | PG integration（F3；`Inspect`/`CheckCompatibility` 只读语义） |
| FR-005 恢复依赖检查 | T015、T019 | PG integration（缺失→阻塞状态+缺失项） |
| FR-006 实际恢复验证 | T015、T020 | PG integration（真实 `pg_restore`+四检查）；SC-001 |
| FR-007 私钥/凭据不入备份 | T016、T018、T020、T068 | unit（secrecy）；Polish 全量复核 |
| FR-008 中断/部分完成 | T015、T019 | PG integration（F2；重建目标幂等） |
| FR-009 默认隔离 | T024、T026、T030–T034、T070 | app 级 integration；7 入口默认拒绝；signer 受支持装配 |
| FR-010 旧实例停止可验证 | T024、T028、T029 | PG integration（checklist 双签） |
| FR-011 禁混跑（000018 模式） | T024、T028 | checklist 6 项 + 证据；F5 |
| FR-012 覆盖全部外部效果路径 | T024、T030–T034、T070 | app 级 integration（含恢复工具自身边界；程序边界见 T028/T064） |
| FR-013 既有约束/禁重放历史签名 | T016、T032、T036、T070 | app 级 integration（signer 交付/原门禁保留）；历史重发负例 |
| FR-014 核验类别与记录 | T035、T038、T039、T040 | contract + PG integration（V1–V9 落库） |
| FR-015 DB 回退≠外部回退 | T035、T036、T039 | PG integration（F4 divergent/unknown） |
| FR-016 缺失≠从未发生 | T035、T036、T041 | contract + integration |
| FR-017 不重建付款意图 | T035、T036、T041 | integration（0 次新建/重放意图） |
| FR-018 unknown 不当通过 | T035、T038 | contract（结论规则） |
| FR-019 缺口证据包/独立性/暂停 | T037、T041、T043、T051 | integration（证据包字段、保守暂停、超时≠闭合、有界只读复核）；C3 |
| FR-020 与既有能力衔接不绕门禁 | T035、T039、T040 | 只读复用证明 + 核验不触发付款 |
| FR-021 分级且无总开关 | T012、T044、T045 | contract + app 级 integration（单能力放行） |
| FR-022 restored/verified/released（spec 称 approved） | T044、T051、T063 | contract + CLI status + 状态面审计 |
| FR-023 审批规则（2026-09-28） | T010、T043、T046、T048、T049、T050、T051、T064 | contract + PG integration + app 级；F7；C2；映射变更失效 |
| FR-024 幂等/可重入/撤销显式 | T044、T047、T049、T051、T060 | integration + drill（重复 ≥10 次零翻转）；SC-007 |
| FR-025 既有资金门禁继续有效 | T030–T034、T045、T070 | app 级 integration（放行不解锁原门禁；两阶段判权） |
| FR-026 诚实报告状态 | T051、T063 | CLI status + serve 状态面审计 |
| FR-027 事件重复/offset 回退/幂等缺失 | T052、T053、T054 | Kafka 层 integration + app 级 |
| FR-028 不承诺跨系统恰好一次 | T052、T053 | boundary 报告 + 测试断言 |
| FR-029 未接回执不宣称外部一致 | T036、T051、T052 | status/核验输出限定范围 |
| FR-030 隔离环境可复现演练 | T057、T058 | drill（S1–S12） |
| FR-031 时间口径分开/指标 | T055、T056、T057、T058 | drill + drill_run 记录 |
| FR-032 失败路径 fail-closed | T015、T025、T026、T036、T037、T044、T059 | F1–F7 全矩阵＋控制库回退负例；SC-006 |
| FR-033 快速检查 vs 独立演练 | T005、T023、T035、T043、T061、T062 | contract 层进普通 PR；drill 独立通道 |
| FR-034 PG 唯一真源 | T008、T025、T040、T069 | 控制库隔离证明与版本校验；数据 DB 零 schema 变更 |
| FR-035 不含 K8s/主备/厂商；T000-P | T001、T004、T067 | 文档收口与非声明 |
| FR-036 可配置目标与测量口径 | T003、T055、T056、T058、T064 | drill 记录 + 未配置状态；C1 |

### SC-001–008 → 任务

| SC | 任务 | 证据 |
|---|---|---|
| SC-001 备份验证通过率 100%/0 误判 | T014、T015、T020 | 受控样本全量判定 + F1 |
| SC-002 外部领先：缺口识别/0 重复付款/独立放行留痕 | T036、T037、T044、T058 | F4 + S7 + S8；0 间接启动被暂停副作用 |
| SC-003 隔离未证明 100% 拒绝 | T024、T026、T070 | F5 + 真实入口拒绝矩阵（含 signer 交付） |
| SC-004 7 能力独立+审批规则+失效 | T043、T044、T045、T046 | F7 + S8/S9/S10 |
| SC-005 端到端演练+分列口径+RTO 处理 | T056、T058、T066 | S12 + 矩阵记录 |
| SC-006 7 类失败注入 0 错误开放 | T059 | drill F1–F7 |
| SC-007 重复执行 10 次幂等 | T044、T047、T060 | drill 幂等 |
| SC-008 快速检查进 PR/演练独立 | T005、T061、T062 | 分层表 + CI 分类守卫 |

### 澄清裁决（2026-09-28）→ 任务

| 裁决 | 任务 | 落地要点 |
|---|---|---|
| C1 FR-036：可配置目标+实测先行；RTO 超时不永久禁后续安全复服 | T003、T055、T056、T064、T067 | 分列计时；未配置=未配置；超时记录+告警+升级 |
| C2 FR-023：执行/核验/批准独立；高影响双人非执行者；批准绑定证据版本；硬门禁不可覆盖 | T043–T051 | 审批档闭集 + 执行者排除 + 代次/哈希绑定 + 无绕过入口 |
| C3 FR-019：缺口无法补齐保留 unknown/pending+证据包+升级；仅可证明独立能力放行 | T037、T041、T058 | 超时/知悉≠闭合；不可重建=保持暂停 |

### V1–V9 → 任务

| # | 对象 | 任务 | 验证 |
|---|---|---|---|
| V1 | 链事实 vs PG（区块/日志/确认/reorg） | T036、T039 | PG+Anvil integration |
| V2 | 提款请求/付款意图/授权 | T036、T039 | 同上（缺失→unknown+gap） |
| V3 | nonce 分配/占用 | T036、T039 | 只读观察、绝不重分配 |
| V4 | 签名/广播结果（含 unknown） | T036、T039 | 只读 accessor 白名单（`AttemptByID`/`UnknownRecovery`/`Status`/回执读）；禁 `Reconcile` 等写路径；核验前后权威表零写断言 |
| V5 | Outbox 与义务标记 | T040、T052 | broker 不可读→unknown |
| V6 | 消费者幂等/进度/隔离 | T040、T052、T053 | Kafka 层 integration |
| V7 | 014 差异/复核/处置/权限/审计 | T040 | 000016 只读引用 |
| V8 | 授权面漂移（撤销/发放再核验） | T040、T028 | source=控制库外部真源证据 |
| V9 | 工具/依赖就绪 | T019、T040 | 恢复前置依赖检查 |

### 七类能力 → 接线/依赖/验证

| # | 能力 | 接线任务（真实入口） | requires | isolation 依赖要点 | 验证 |
|---|---|---|---|---|---|
| 1 | query | T030（`serve.go:473-474/479-480/488`）、T031（`withdrawalhttp.go:227`） | — | 旧实例停止/网络隔离/版本兼容 | T023/T026/T045（single 档） |
| 2 | chain_scan | T030（`serve.go:319/331/615`；lease `indexer/lease.go:56-67`） | — | 旧实例停止+写者 fencing 观察+版本兼容 | T024/T026/T045 |
| 3 | deposit_confirmation | T030（`serve.go:350-353/380-398/411`） | chain_scan | 同 chain_scan | 同上 |
| 4 | existing_withdrawal_recovery | T032（`withdrawalworker.go:318/349/566`、`withdrawalexec.go:35`、`withdrawalexecution.go`（`serve.go:479`，直调 `execution.Admit`）、`signerserve.go:295`→`signer/delivery.go:170`/`:194`）、T070（signer 进程内检查点） | chain_scan | 旧执行者停止+执行/签名/广播隔离+授权面再核验 | T045（dual 档） |
| 5 | new_withdrawal_creation | T031（`withdrawalhttp.go:148`，先于 guardRoute/CapacityGate） | existing_withdrawal_recovery | 同 4（不得只开入口） | T045（dual 档） |
| 6 | event_publishing | T033（`eventpublisher.go:41`→`events/publisher.go:177`） | chain_scan | 旧 publisher 停止+outbox/义务核验+目标范围确认 | T053/T054（非真实下游=single；真实下游=dual） |
| 7 | event_consuming | T034（`eventconsumer.go:31`→`events/consumer.go:351/639`） | — | 旧 consumer 停止+幂等/进度核验+effect class 范围确认 | T053/T054/T050（effect class 判定） |

### quickstart S1–S12 / F1–F7 → 任务

| # | 任务 | # | 任务 |
|---|---|---|---|
| S1 生成备份 | T021 | S7 缺口阻塞 | T037、T041 |
| S2 实际恢复验证 | T019、T020 | S8 独立能力放行 | T045 |
| S3 开启恢复实例 | T027 | S9 分级推进 | T045 |
| S4 恢复 | T022 | S10 状态诚实 | T051、T063 |
| S5 隔离核验 | T029 | S11 关闭实例 | T027、T049 |
| S6 事实核验 | T042 | S12 度量记录 | T057、T058 |
| F1 备份不可用/损坏/未验证 | T015、T019 | F5 旧实例未隔离 | T024、T026 |
| F2 恢复中断/部分完成 | T015、T019 | F6 核验缺口 | T037、T058 |
| F3 版本/schema 不兼容 | T015、T019 | F7 越权/证据不足/过期批准 | T044、T046 |
| F4 外部事实领先 | T036、T053 | 复入与幂等 ≥10 次 | T060（+T044） |

补充负例（本轮 analyze 增，不改 F1–F7 编号）：控制库盲恢复/旧库自写 supersede≠重建（T025）；未知控制库 schema 版本→拒绝（T069）；历史 signed bytes/广播/付款意图/重复投递重发→拒绝（T036）；有界只读复核预算耗尽→拒绝+审计不改状态（T051/T063）；身份映射变更→旧批准失效重批（T044/T046）；signer 非受支持装配残余边界（T032/T070）。

---

## 待裁决 · 待测 · 缺口 · 业务未决（分列；不暗改规格）

### 部署前裁决（不批准、不阻塞无关设计；本清单不编造数值）

1. 生产 RPO/RTO、备份频率、保留期（FR-036 明示留裁决；未配置时任务只能报告「未配置」）。
2. 控制库拓扑（同 PG 实例独立 database vs 独立实例）与保留域。
3. 身份映射（人员↔principal）名单内容与维护者（T010/T046 提供可操作路径；名单内容待裁决）。
4. 真实下游 effect class 清单（哪些 topic/scope 属「真实下游投递」；未配置时 T050 保守按 dual）。
5. 单机/单人部署时非执行者批准人来源（FR-023 下单人无法自批）。
6. 备份产物落盘/异地策略与保留实现。
7. 控制库自身回退/旧副本的检测边界与普适「旧批准不重生效」范围（DG-4；要求普适自动检测须另立设计/裁决，不降调关闭）。
8. 控制库 schema 升级策略（本阶段仅 0001：T069 只交付未知/不兼容版本拒绝；升级路径留未来版本）。

### 实现期测量（实现后测量填入，不编造，不阻塞设计）

1. 门禁缓存 TTL（`TXHARBOR_RECOVERY_GATE_TTL`）与各证据类别新鲜度容忍。
2. 核验批次上界与单次有界步进预算。
3. 控制库语句超时与连接池参数。
4. 演练时长、备份体积/耗时（标注 `test_inputs`）。
5. 指标告警阈值（RTO 超时告警路由）。
6. 有界只读复核（T051/T063）的可读范围、次数/时间/资源预算（实现后测量填入；仍须拒绝无界扫描）。

### 设计缺口（标记受影响任务前置阻塞；不假装已有）

- **DG-1 误回退防护（绕过受支持入口的恢复/旁路手工恢复自动检测）**：无设计依据——research §4 明示为「部署纪律（恢复走受支持入口 + 000018 检查清单模式）+ 控制库审计 + 实例开启后强制」，015 不提供自动检测。**同族边界（F1/F6）**：signer 核心非受支持装配直调、控制库自身旧副本/盲恢复同样不提供自动检测。**受影响任务**：T030–T034（接线验收限定：不得宣称旁路恢复会被自动阻止/检测）、T026（测试只覆盖受支持入口与 `TXHARBOR_RECOVERY_INSTANCE` 绑定）、T032/T070（signer 受支持装配边界）、T025/T064（控制库回退纪律）。**阻塞的声明**：任何「旁路恢复也会被自动检测/阻止」「任意 in-process 调用全覆盖」「普适旧批准不重生效」的验收声明被阻塞（无任务承接）；若未来需要，先补 ADR/设计。上述受影响任务在设计范围内不被阻塞，但完成条件必须携带该边界。
- **DG-2（决议 2026-09-28 analyze）verify-backup 先于实例开启时的证据绑定口径**：已钉死——备份级验证（manifest `verified`，绑定 backup_id/carrier/schema）与目标实例级验证（`restore_probe`，绑定实例+`data_target` 指纹）分属不同生命周期；`verify-backup` 先于实例开启时先落 manifest 级结论，实例开启后经受控命令显式同步绑定并审计（data-model §1.4、contracts/backup-manifest.md §3、quickstart S2、T020）；复制 manifest、重建实例或更换目标指纹后必须重 probe，不得沿用旧 `verified`。**受影响任务**：T019、T020、T042（结论落库不得谎称实例绑定）；不再留实现期口径漂移。
- **DG-3 控制库与数据 DB 同 PG 实例时的实例级灾难（两者同失）**：research §3 明示 fail-closed、重建控制事实后才可能放行；不虚构跨主机独立性；**F14 补充**：同实例储层同失超出逻辑回滚域，备份落盘/异地策略待裁决（不阻塞实现），T064 写明。**受影响任务**：T025、T026、T064、T067（证据范围限定在逻辑回滚域，不得宣称实例级独立）。
- **DG-4 控制库自身回退/旧副本的可检测性边界（F6）**：受支持控制恢复=停机隔离＋显式重建/supersede＋审计（T025/T064）；015 不提供对控制库盲恢复/旧副本自写 supersede 的自动检测；「旧批准不自动重生效」仅就数据 DB 回滚域成立（instance_id 绑定+控制域独立），控制库回退域的普适声明保持限定/阻塞。**若要求普适保证，须另立 ADR/裁决；不得以缩范围装成关闭**。

### 业务未决（明确缺席；若未来需要属业务阻塞，另行业务裁决）

- 风险接受后强制复服、损失核销、人工补偿付款、自动补造付款意图：无设计、无权限行、无契约、无任务；双人批准不得替代缺失证据；本清单不得实现任何绕过入口。

### 实现期硬约束（横切，编入相关任务完成条件）

1. **测试构建环**：`package app` import `internal/recovery` 时，recovery 根包不得传递 import `internal/{txlifecycle,events,indexer,reconciliation,cache}`（其内部集成测试 import `internal/app`；先例 `internal/app/reconcileadmin/reconcileadmin.go:1-20`）。V1–V9 重依赖适配器落 `internal/recovery/sources/` 子包或接口注入；证明：`go vet -tags integration ./internal/txlifecycle ./internal/events` 可构建（T001/T038–T040）。
2. **环境变量命名**：新键不得与既有测试键 `TXHARBOR_RECOVERY_KILL_CHILD/_DSN/_READY/_DISPATCH`（`internal/withdrawal/recovery_kill_integration_test.go:65-68`）碰撞，不得改名既有键（T003）。
3. **反作弊纪律**（quickstart §4）：禁止直写控制库的批准/放行/缺口/隔离状态绕过命令；禁止注入核验替身使门禁放行；禁止测试专用「关门禁」开关；真实恢复必须跑真实 `pg_dump/pg_restore`；链证据来自真实链、Kafka 证据来自真实 broker；禁以「数据库可连接」宣称 RTO 达标；V4 不得以 `Reconcile` 写路径冒充只读核验（T039）；signer 交付不得绕过进程内检查点（T070）（T023/T044/T058/T059）。
4. **控制库不是金融真源**（宪法 III）：控制库只承载恢复治理事实，丢失/不可达 = fail-closed 隔离；schema 版本未知/不兼容→拒绝（T069）；金额不落控制模型（T006/T008/T025）。

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup（Phase 1）**: 无依赖，可立即开始。
- **Foundational（Phase 2）**: 依赖 Setup；**阻塞所有 user story**；合流责任：T006（schema 单一所有者，只合并一次）、T007＋T069（store 唯一决策读写入口与版本守卫，B2 内 T069 在 T007/T008 后串行）、T009/T010（身份与授权）、T011–T013（门禁与代次）各自所有者合并后，story 才 rebase。
- **User Stories（Phase 3+）**: 均依赖 Foundational 完成。
  - **US1（P1）**: 自身验收（S1/S2＋F1–F3）无跨 story 依赖；MVP。S4「恢复→核验」端到端序列归 US2（B9 checkpoint），不属 US1 验收（F18 注记）。
  - **US2（P1）**: 实现依赖 Foundational；S4/S5 的完整正向序列使用 US1 的 restore（本批负例/默认拒绝可先独立验收）。
  - **US3（P1）**: 实现依赖 Foundational；真实「外部领先」场景使用 US1 的备份/恢复数据构造；放行相关断言待 US4。
  - **US4（P2）**: 依赖 Foundational；放行正例（T045）依赖 US1 restore + US2 实例/清单；approval 独立性依赖 B3 身份路径。
  - **US5（P2）**: 依赖 US3 的 V5/V6 结论（T040→T052/T053）与 US2 事件入口接线（T033/T034 用于 T054）。
  - **US6（P2）**: 演练联合验收依赖 US1–US5 全部可运行；指标/记录模型可先落。
- **Polish（Phase 9）**: 依赖所有目标 story 完成。

### User Story Dependencies（真实依赖，非模板）

- US1 → 无（Foundational 后）；S4 恢复 E2E 归 US2（F18）。
- US2 → US1（仅 S4/S5/S9 序列的数据前置；默认拒绝与 checklist 本身独立）。
- US3 → US1（受控数据集构造）。
- US4 → US1 + US2（放行正例与实例/清单）；US4 的审批规则实现不依赖 US3。
- US5 → US3（V5/V6）+ US2（事件入口门禁）。
- US6 → US1–US5 全部（联合演练）。

### Shared-Artifact Confluence（合流责任）

- **控制库 schema** `internal/recovery/controlstore/schema/0001_control_init.sql`（T006）：单一所有者；所有 story 只读/写数，不并行改 schema；需变更回写 data-model 后再改。
- **store** `internal/recovery/controlstore/store.go`（T007）：决策读写唯一入口；其他任务不得绕过它直写决策表。
- **身份/授权** `identity.go`+`control.go`（T009/T010）：参与者与映射唯一写路径；测试与 CLI 只经此注册。
- **门禁** `gate.go`/`capabilities.go`（T011/T012）：派生评估唯一实现；T030–T034 只调用、不得就地复制判定；TTL/失效语义只在此处变更。
- **代次协议** `generation.go`（T013）：所有核验/缺口/隔离写入必须经此协议。
- **核验编排** `verification.go`（T038）：V1–V9 结论唯一落库路径；适配器（T039/T040）只提供只读来源（V4 只读 accessor 白名单与零写断言见 T039/T036）。
- **审批/放行** `approvals.go`/`release.go`/`scope.go`（T048–T050）：批准与放行有效性唯一判定；CLI（T051）只做接线与审计。
- **命令面** `cmd/txharbor/main.go` + `recoveryadmin/recoveryadmin.go`（T004）：子命令注册唯一所有者；后续 CLI 任务只新增子命令文件。
- **接线入口文件单一所有者**：`serve.go`→T030；`withdrawalhttp.go`→T031；`withdrawalworker.go`+`withdrawalexec.go`+`withdrawalexecution.go`+`signerserve.go`→T032；`internal/signer/**`（进程内检查点）→T070；`eventpublisher.go`→T033；`eventconsumer.go`→T034；`Makefile`→T005/T061/T062（按批次顺序）；`.github/workflows/ci.yml`→T062；`.github/workflows/drill.yml`→T005/T061。
- **文档与证据** `docs/evidence/015/`、`docs/ops/recovery-runbook.md`、`specs/015-…/quickstart.md`：T061/T064/T065/T066/T067 按批次顺序收口。

### Within Each User Story

- Tests（若含）先写并失败，再实现。
- Models → Services → Endpoints/CLI → Integration。
- 核心实现先于接线；接线先于演练。
- story 完成后再进入下一优先级；每批 Checkpoint 独立可测。

### Batch Order（依赖链）

B0 → B1 → B2（含 T069 版本守卫）→ B3 → B4（Foundational 完成）→ B5 → B6（US1）→ B7 → B8 → B9（US2，含 T070）→ B10 → B11（US3）→ B12 → B13 → B14（US4）→ B15（US5）→ B16 → B17（US6）→ B18 → B19（Polish）。**T069/T070 已并入 B2/B9，无独立收尾批**；B4/B13 前置经 B2 获得版本守卫。
可并行：B7 与 B8/B9 的测试编写；B5 测试与 B6 实现存在 TDD 先后但不阻塞其他文件；US3 与 US4 的测试文件可与相邻实现并行编写（依赖见各批前置）。**不并行**：T039/T040、T049/T050 串行现状保留（F15，owner 确认前不加 [P]）。

---

## Parallel Example: User Story 1

```bash
# B5 测试先行（三个文件，可同时落）：
Task: "manifest 契约测试 internal/recovery/manifest_contract_test.go"          # T014
Task: "备份/恢复 PG 集成测试 internal/recovery/backup_integration_test.go"      # T015
Task: "保密性单测 internal/recovery/secrecy_test.go"                            # T016

# B9 七类能力接线（五个入口文件，互不冲突）：
Task: "serve.go 链观察接线"                                                     # T030
Task: "withdrawalhttp.go 提款写入口接线"                                        # T031
Task: "worker/exec/signer 既有提款恢复接线"                                     # T032
Task: "signer 进程内检查点（T032 后串行，不带 [P]）"                              # T070
Task: "eventpublisher.go 接线"                                                  # T033
Task: "eventconsumer.go 接线"                                                   # T034

# B12 US4 测试先行（五个文件）：
Task: "approvals_contract_test.go" / "release_integration_test.go" /
      "recovery_release_integration_test.go" / "identity_path_integration_test.go" /
      "generation_integration_test.go"                                          # T043–T047
```

## Parallel Example: Setup / Foundational

```bash
# B0：T002–T005 并行（不同文件/目录）
# B2：T007 与 T008 并行（不同文件，均只依赖 T006）；T069 在 T007/T008 后串行（同改 store.go/migrate.go，不带 [P]）
# B4：T011 与 T013 并行；T012 在 T011 后串行
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. 完成 Phase 1 Setup。
2. 完成 Phase 2 Foundational（CRITICAL，阻塞所有 story）。
3. 完成 Phase 3 US1（B5 测试先行 → B6 实现）。
4. **STOP and VALIDATE**：S1/S2 + F1/F2/F3；contract + PG integration 层；「备份成功≠恢复成功」证据。
5. 本地 commit；可演示受控备份样本判定。

### Incremental Delivery

1. Setup + Foundational → 控制库/身份/门禁底座就绪（本地 commit）。
2. US1 → 独立验证 → commit（MVP）。
3. US2 → 默认隔离与 7 入口拒绝 → commit。
4. US3 → V1–V9 核验与缺口 → commit。
5. US4 → 分级复服与审批 → commit。
6. US5 → 事件边界 → commit。
7. US6 → drill 独立通道 → commit。
8. Polish → 分层矩阵与文档收口 → commit。
每步不破坏前序 story；drill 层始终独立于普通 PR。

### Parallel Team Strategy

1. 全队完成 Setup + Foundational（schema/store/身份/门禁 四 owner 先行合并）。
2. Foundational 后：
   - 开发 A：US1（链 1）+ US2 接线（链 4）；
   - 开发 B：US3 核验（链 6）；
   - 开发 C：US4 审批/放行（链 3/5）；
   - 开发 D：US5/US6 + CI（链 7）。
3. Story 经各批 Checkpoint 与合流 owner 独立集成；US6 演练为联合验收。

---

## Entry/Exit Criteria per Batch

- **Entry（通用）**：前序批次已合并；本批「前置」所列依赖完成；测试夹具/受控样本/身份映射可用；控制库可初始化（或本批不需要）。
- **Exit（通用）**：本批完成条件在指定层次全绿；未执行/未覆盖检查如实记录（NOT RUN 不得记为 pass）；测试先行批必须先失败后转绿；每批本地 commit（不 push）；Checkpoint 可独立复现。
- **层次适用**：`gofmt`/`go vet`/unit（`make test`）/race（`make test-race`）始终适用；contract（`make test-contract`）普通 PR；PG integration（`make test-integration`）按 `internal/recovery/*` 路径分类触发，Docker 缺位 → NOT RUN；Kafka/Redis 层按 `ci.yml` 分类；drill（`make test-drill`）仅独立通道，永不进普通 PR。
- **逐批 Exit 摘要**：
  - B0：构建/CLI help/drill NOT RUN 守卫；B1：schema 与零数据 DB 变更；B2：store/migrate/DSN 校验＋版本守卫（T069：未知/不兼容拒绝）；B3：身份注册路径可操作；B4：门禁直通/默认拒绝与代次丢弃可测；B5：测试先落且失败；B6：S1/S2+F1/F2/F3；B7：测试先落且失败；B8：S3/S5+close 守卫；B9：7 入口默认拒绝与原门禁保留＋signer 进程内检查点（T070）；B10：测试先落且失败；B11：S6/S7+F4/F6；B12：测试先落且失败；B13：审批/放行派生判定与保守 effect class；B14：S10/S11+F7；B15：回退/重复/缺失三场景；B16：分列计时与 drill 记录；B17：S1–S12+F1–F7（含保持暂停）+幂等；B18：CI 分类守卫/runbook/证据索引；B19：矩阵记录/文档收口/保密复核。

---

## Notes

- **T000-P 保持 OPEN**：勾选仅代表本地范围完成，不是发布、不宣称生产就绪；生产 RPO/RTO/频率/保留留部署前裁决；本地数值仅测试输入（`plan.md` Gate status、`research.md` §10、`quickstart.md` Gate）。
- **[P]** = 不同文件且不依赖同批未完成任务；测试任务按 TDD 先写并预期失败，不在 [P] 约束的依赖含义内。**不虚标并行**：同文件或有依赖边的任务一律不带 [P]。
- **共享产物合流责任**见 Dependencies 节；真实入口装配（T030–T034、T050/T051、T057）全部落在 story 批次内、不拖到「已完成」之后。
- **风险接受后强制复服/损失核销/人工补偿付款/自动补造意图**：NOT approved、无实现任务；若未来需要属业务阻塞，另行业务裁决；双人批准不得替代缺失证据。
- **不把完整性检查称为 analyze**：manifest 完整性 = 长度/哈希/`pg_restore -l` 可读/实际恢复验证；本清单及后续证据不使用「analyze」表述该检查。
- 设计缺口 DG-1–DG-3 的阻塞范围见上文；受影响任务完成条件必须携带边界，不得暗改规格来「消缺口」。
- 环境变量命名与测试构建环两条硬约束见「实现期硬约束」；实现期以 `go vet -tags integration ./internal/txlifecycle ./internal/events` 与 `go build ./...` 证明无环。
- 控制库不可达/丢失 = fail-closed 隔离；控制库不是金融真源；Redis/Kafka/缓存永不成为金融真相。
- 未接入真实上游回执时不宣称外部账本一致；不承诺跨系统恰好一次。
- 每任务完成后或逻辑分组后本地 commit；任何 Checkpoint 均可停下独立验证。
