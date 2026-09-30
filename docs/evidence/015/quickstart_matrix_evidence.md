# 015 Quickstart 验证矩阵证据（T066）

- Feature: `015-backup-recovery-safe-resumption` ｜ 分支: `015-backup-recovery-safe-resumption` ｜ main 基线: `8d4b9af`
- 树状态（以下 §1–§8）：历史快照 HEAD `65df393` + 当时未提交脏树（17 M + 9 未跟踪文件；含 B17 drill 独立通道、B18 CI/证据归档、B19 收口与已知失败修复）。原记录完整保留；全部历史结果仅适用于该树。
- **状态口径**：§1–§9 为历史记录；§9 是旧的 `e7ba8c0` precommit 快照（62/70），不代表当前 follow-up。当前基线 `64140fb` 之后的补记见 §10：仅 T017/T027 已标记满足，当前 64/70；T019/T020/T057/T058/T066/T068 与 T000-P 仍 OPEN。历史计数不回写，也不得由局部 PASS 推断未覆盖入口或生产行为通过。
- 日期: 2026-09-29 ｜ 执行者: 本轮（B17/B18 + fix-27/fix-28 合流验证；T066 记录） ｜ Docker/testcontainers: 可用且实际使用（真实 `postgres:18.6-trixie`、`ghcr.io/foundry-rs/foundry:v1.8.1`、`confluentinc/confluent-local:7.9.10`）；本轮运行无 Docker 缺位、无超时、无取消。
- 状态口径（三者分离，不得混同）：
  - **本地执行**：本轮实际运行的层（lint / build / unit / contract / 聚焦 race / CI 静态复验 / PG 具名入口 2/2 / drill 13/13）全部通过，运行于上述共享脏树。
  - **远程 CI**：未推送/未触发 → **待核验**；§4 仅为本地静态模拟。
  - **生产就绪**：未宣称；**T000-P 保持 OPEN**；drill/S12 与本地数值一律仅为测试输入。
- 未执行项一律 **NOT RUN**，不得读作通过（对齐 `docs/evidence/015/README.md` §2 与 `docs/evidence/014/quickstart_matrix_evidence.md` 模式）。
- 原始日志：本轮证据的原始日志在 `/tmp/opencode/`（未入库）；持久载体以测试文件 + 提交为准（见 §7）。

## 1. 分层执行结果（本轮实际执行 + NOT RUN）

| 层 | 命令 | 范围 | 结果 | 备注 |
|---|---|---|---|---|
| lint | `make lint` | 全仓（gofmt + `go vet` unit/integration tags） | **通过** | fix-27 复验（含 `internal/app`、`.github`、drill 测试树） |
| build | `make build` | 全仓 | **通过** | fix-27 复验 |
| unit | `make test` | 全仓（无 Docker） | **通过** | fix-27 复跑：全部包 ok（原始日志 `/tmp/opencode/make_test_final.log`）；含 T063 `internal/app` 4 个单测 |
| contract | `make test-contract` | `./internal/events ./internal/reconciliation ./internal/recovery` | **通过** | 含新载体 `verification_instant_contract_test.go`（T038）与 `internal/recovery` 既有 contract 用例 |
| race（聚焦） | `go test -race -count=1 -run 'TestRecoveryStatusPosture\|TestDegradationStatusRecovery\|TestRecoveryHealthAnnotation' ./internal/app` | 仅 T063 面 | **通过** | 非全量 race |
| integration-PG（具名入口 2/2） | `go test -tags integration -count=1 -run 'TestRecoveryAdminVerifyApproveReleaseStatusRealEntry\|TestRecoveryAdminS11PositiveCloseRealEntry' ./internal/app/recoveryadmin` | 2 个真实入口用例 | **通过 2/2** | 真实 PG `postgres:18.6-trixie` + 真实 Anvil `foundry:v1.8.1`；11.83s + 13.34s（合并复跑 24.407s）；载体 `internal/app/recoveryadmin/verify_approve_status_integration_test.go`（integration 标签）；原始日志 `/tmp/opencode/r1-neg-1.log`、`r1-s11.log`、`r1-combined-2.log`；**非全量**：其余 integration-PG 文件本轮未执行（不推断） |
| drill（独立通道） | `make test-drill`（`drill` tag） | S1–S12 + F1–F7（13 用例） | **通过 13/13** | 真实 PG 18.6（`postgres:18.6-trixie`）+ 真实 Anvil（`foundry:v1.8.1`）+ 真实 Kafka（`confluent-local:7.9.10`）；`internal/recovery` 包 123.7s（123.653s）；0 FAIL / 0 SKIP；无 Docker 缺位、无超时、无取消；原始日志 `/tmp/opencode/drill-full-2.log`、`drill-detail-final.log`；载体 4 个 drill 测试文件（§7） |
| integration-redis / -kafka（全量） | `make test-integration-redis` / `make test-integration-kafka` | cache/ratelimit/testutil、events/app/health/testutil | **NOT RUN** | 本轮未执行该两层全量；drill 内真实 Kafka 事件场景通过 ≠ 该层全量验收；恢复：具备 Docker 环境执行后按实记录 |
| e2e（全量） | `make test-e2e` | `./internal/app`（Anvil + Docker） | **NOT RUN** | 本轮未执行 |
| race（全量） | `make test-race` | 全仓 | **NOT RUN** | 本轮仅 T063 聚焦 race；全量由合流/远程 CI 决策 |
| CI 配置静态检查 | §4 命令 | `.github/workflows/ci.yml` + `.github/workflows/drill.yml` + 分类脚本模拟 | **通过**（本地静态，非 GitHub runner） | B18 复验：YAML 解析、分类正反例、四层反向守卫（pg 232 / redis 4 / kafka 7 / e2e 3 全命中）、drill 隔离守卫正反例 |

## 2. quickstart §1 S1–S12 覆盖索引

层标注：`unit`＝无 Docker；`contract`＝无 Docker；`PG`＝真实 PG integration（含 CLI 入口）；`drill`＝独立通道（真实 PG/Anvil/Kafka）。每行写明载体测试名与层；未运行层一律 **NOT RUN**（即使载体存在），不得由已通过层推断。

| # | 场景 | 主要载体（测试/命令） | 层 | 本轮结果 |
|---|---|---|---|---|
| S1 | 生成备份 | `TestManifestContract*`（contract）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`（drill：真实 `pg_dump`→manifest，`verification.state=unverified`） | contract＋drill | contract **通过**（manifest 契约面）；drill **通过**（真实 `pg_dump`）；`recovery-admin backup` CLI（PG）本轮未执行 → **NOT RUN** |
| S2 | 实际恢复验证 | `TestManifestContractFourChecksRequiredForVerified`、`TestManifestContractNotVerifiedNeverUsable`（contract）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`（drill：真实 `pg_restore` 到隔离库，四项检查 → `verified`） | contract＋drill | contract **通过**；drill **通过**（真实 `pg_restore` 隔离验证）；`TestBackupVerifyRestoreSnapshotBoundIsolated`、`TestVerifyBackupCLIOperationIDSemantics`（PG/CLI）本轮未执行 → **NOT RUN** |
| S3 | 开启恢复实例 | drill harness：13 个 drill 用例逐一真实调用 `recovery.OpenInstance`（真实 PG 控制库）；`TestGateContractOpenInstanceDeniesAllCapabilities`（contract） | drill＋contract | drill **通过**（全局唯一 open、能力默认关闭）；contract **通过**；`TestGateBaselineInstanceDoesNotArm`、`TestControlIntegrationIdentityPath`（PG）本轮未执行 → **NOT RUN** |
| S4 | 恢复 | `TestRestoreHelpStatesPreconditionsAndEvidenceTiming`（unit）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`（drill：真实 `pg_restore`＋`restore_probe`）；`TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent`、`TestT060DrillRestoreReentryConvergesWithSingleState`（drill） | unit＋drill | unit **通过**（help/前置声明）；drill **通过**（恢复流程、中断重入收敛）；`TestRestore*CLI*`（PG/CLI）本轮未执行 → **NOT RUN** |
| S5 | 隔离核验 | `TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`、`TestT058DrillGapCannotBeClosedStaysPaused`（drill：`checklist-set` 执行者采集 + `checklist-verify` 非执行者确认，真实入口）；`TestGateContractIsolationItemMustBeExactlyVerified`（contract） | drill＋contract | drill **通过**；contract **通过**；`TestIsolationChecklist*`（PG）本轮未执行 → **NOT RUN** |
| S6 | 事实核验 | `TestT035*`（contract）；`TestRecoveryAdminVerifyApproveReleaseStatusRealEntry`（PG：真实 `verify --scope all` 有界步进，V1–V9 经真实只读适配器）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`、`TestT058DrillEventLayerKafkaBrokerOffsetDivergence`（drill：真实链 + 真实 Kafka 的 V5/V6） | contract＋PG＋drill | contract **通过**；PG **通过**（真实 V1–V9）；drill **通过** |
| S7 | 缺口阻塞 | `TestT058DrillGapCannotBeClosedStaysPaused`、`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`（drill：`gap_open`/依赖链拒绝、空证据不闭合、超时/知悉不改变状态、证据包/责任/升级、有界 review）；`TestRecoveryAdminVerifyApproveReleaseStatusRealEntry`（PG：缺口存在时 release 仅作为决定记录，派生准入仍以 `refusal_class=gap_open` 拒绝（status/close 断言），放行决定 ≠ 复服） | drill＋PG | drill **通过**；PG **通过**；`TestT037*`（`internal/recovery` PG）本轮未执行 → **NOT RUN** |
| S8 | 独立能力放行 | `TestT043*`、`TestGateContract*`（contract）；`TestRecoveryAdminVerifyApproveReleaseStatusRealEntry`（PG：非执行者 single 批准 `query`/`chain_scan` → 放行；无有效批准的能力 `approval_missing` 拒绝且零 release 行）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`（drill：仅 `query` 放行，其余保持拒绝） | contract＋PG＋drill | contract **通过**；PG **通过**；drill **通过** |
| S9 | 分级推进 | `TestT043*`、`TestGateContractCapabilityDependencyClosure`（contract）；`TestRecoveryAdminVerifyApproveReleaseStatusRealEntry`（PG：真实双人批准与依赖路径）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`、`TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips`（drill：依赖顺序、高影响能力缺第二人批准 0 放行、重复零翻转） | contract＋PG＋drill | contract **通过**；PG **通过**；drill **通过**；其余 PG 载体（`TestGateDualApprovalRules`、`TestGateCapabilityDependencyClosed`、`TestT046ExplicitTwoPrincipalDualApprovalPath`、`TestT044HighImpactCapabilitiesNeedTwoDistinctPeople` 等）本轮未执行 → **NOT RUN** |
| S10 | 状态诚实 | `TestRecoveryStatusPostureModes`、`TestDegradationStatusRecoveryNormalMode`、`TestDegradationStatusRecoveryBoundModeNeverClaimsDerivedStates`、`TestRecoveryHealthAnnotationMarksModeWithoutClaimingState`（unit：serve 状态面/探针非声明）；`TestRecoveryAdminVerifyApproveReleaseStatusRealEntry`（PG：真实 `status` 入口，逐能力三态 + 拒绝类，撤销后 `released=false`）；drill 全程拒绝类/状态断言 | unit＋PG＋drill | unit **通过**（含聚焦 race）；PG **通过**（真实控制库 status）；drill **通过**；`TestT044RestoredVerifiedReleasedDoNotImpersonate`（PG）本轮未执行 → **NOT RUN** |
| S11 | 关闭实例 | `TestRecoveryAdminS11PositiveCloseRealEntry`（PG：真实 `instance-close`，未全放行/缺口未闭 → 拒绝，全放行 → 关闭）；`TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent`（drill） | PG＋drill | PG **通过**；drill **通过**；`TestT044RepeatedApproveReleaseCloseDoNotFlipState`（PG）本轮未执行 → **NOT RUN** |
| S12 | 度量记录 | `TestDrillScenarioSet`、`TestPrepareDrillRunSeparateTimingShape`、`TestSafeResumptionSecondsRequiresFullScope`、`TestDrillRunRefusalsWriteNothing`（unit）；`TestT058DrillE2EBackupAdvanceRestoreRefuseThenRelease`（drill：恢复点/DB 恢复/核验/各能力放行/backup_lag/uncovered_interval 分列 + `RecordDrillRun` 归档；局部能力集永不给出端到端时长） | unit＋drill | unit **通过**（场景/记录模型）；drill **通过**（真实记录 + 归档，`/tmp/opencode/drill-evidence/drill-t058-e2e.json`、`drill-t058-event-kafka.json`、`drill-t058-gap-paused.json`）；`TestDrillCLIRealRestoreRecordAndArchive`、`TestDrillCLILocalInputAnnotations`（PG/CLI）本轮未执行 → **NOT RUN** |

历史 drill 载体记录了两库内容指纹比较及特定拒绝/重放场景的结果；这些有限断言不证明普遍的零重复付款或零下游效果，相关全局性结论在此撤回。S7/S8/S9 的列明门禁场景只限当时列出的 drill/PG 具名入口。**serve/HTTP 入口级**断言（S8 前 `POST /withdrawals` 503、S9 前 worker/publisher/consumer 拒绝）当时未执行 → **NOT RUN**。以上历史结果不覆盖当前修复树。

PG/CLI 直连层范围：本轮 integration-PG 仅执行 §1 的 2 个具名入口用例；`internal/app/recoveryadmin`、`internal/recovery` 其余 integration 标签载体（如 `TestBackup*`、`TestRestore*CLI*`、`TestIsolationChecklist*`、`TestT036*`、`TestT037*`、`TestT044*`、`TestT046*`、`TestGate*` 集成版）本轮未执行 → 其层 **NOT RUN**，不得由 drill 或具名入口通过推断。

## 3. quickstart §2 F1–F7 覆盖索引

| # | 注入 | 主要载体 | 本轮结果 |
|---|---|---|---|
| F1 | 备份不可用/损坏/截断/未验证 | `TestManifestContractNotVerifiedNeverUsable`（contract）；`TestT059F1BackupUnusableCorruptTruncatedUnverified`（drill：未验证/损坏/截断/改写 manifest 全部拒绝，目标零表、零 best-effort、零 probe）；`TestBackupCorruptionRejectedAndRestoreRefused`、`TestBackupFailureProducesNoSuccessManifest`、`TestVerifyBackupRejectsMissingAuthoritativeObject`、`TestVerifyBackupCLINoOverEvidenceSuccessOnMissingObject`（PG/CLI） | contract **通过**；drill **通过**（真实拒绝矩阵）；PG/CLI **NOT RUN** |
| F2 | 恢复中断/部分完成 | `TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent`、`TestT060DrillRestoreReentryConvergesWithSingleState`（drill：10 次中断复入不达 restored、按已提交 start marker 前进、最终单状态收敛）；`TestRestoreInterruptionNotRestoredAndRerunIdempotent`、`TestRestoreInterruptionAfterFirstWriteKeepsOldPermissionStale`、`TestRestoreStartInterleavingDoesNotRewindAdmittedAction`（PG） | drill **通过**；PG **NOT RUN** |
| F3 | 版本/schema 不兼容 | `TestManifestContractValidateCompatibility`、`TestManifestContractUnknownVersionRefused`（contract）；`TestT059F3IncompatibleProgramRefusedNoSilentDowngrade`（drill：程序版本不兼容 → 拒绝且无静默降级）；`TestRestoreTargetGuardsAndIncompatibleManifestRefused`、`TestSchemaGuardUnknownVersionRefusesAndAudits`、`TestControlIntegrationRefusesUnknownControlVersion`（PG） | contract **通过**；drill **通过**；PG **NOT RUN** |
| F4 | 外部事实领先 | `TestT059F4ExternalLeadZeroReplayZeroAuthorityWrites`（drill：零重放、零权威写）；`TestT058DrillEventLayerKafkaBrokerOffsetDivergence`（drill：真实 Kafka broker committed=6 vs restored=5，divergence 记录、无重消费）；`TestBoundaryExactlyOnceNeverClaimed`、`TestBoundaryDedupIdentity`、`TestBoundaryAssessFailClosed`（unit）；`TestT036VerificationV1ToV9RealAdaptersExternalLead`、`TestT036VerificationHistoricalReplayNegativeZeroSideEffects`（PG） | unit **通过**（边界口径）；drill **通过**（真实链 + 真实 Kafka）；PG **NOT RUN** |
| F5 | 旧实例未隔离/无法证明 | `TestT059F5IsolationUnprovenRefusedAndNotInferred`（drill：未证明即全拒绝、不得推断）；`TestGateContractIsolationItemMustBeExactlyVerified`（contract）；`TestOldInstanceIsolationUnprovenRefusesAllEffectfulAdmissions`、`TestControlRollbackNegativeF6`（PG） | contract **通过**；drill **通过**；PG **NOT RUN** |
| F6 | 核验发现无法证明的缺口 | `TestT059F6UnprovableGapStaysUnknownWithPackageAndEscalation`、`TestT058DrillGapCannotBeClosedStaysPaused`（drill：保持 unknown/paused、证据包、责任、升级；超时/知悉/预算耗尽不闭合）；`TestGateContractOpenInstanceDeniesAllCapabilities`（contract）；`TestT037*`（PG） | contract **通过**；drill **通过**；PG **NOT RUN** |
| F7 | 越权/证据不足/过期批准 | `TestT059F7UnauthorizedInsufficientStaleApprovalsRefused`（drill：未授权/不足/过期批准全部拒绝且审计）；`TestT043*`（contract）；`TestT044ExecutorSelfApprovalIsZeroReleases`、`TestT046MappingChangeInvalidatesApprovalsAndIsAudited`、`TestT046ConservativeDualDoesNotOffsetWrongMapping`、`TestGateStaleApprovalRefused`（PG） | contract **通过**；drill **通过**；PG **NOT RUN** |

复入与幂等：`TestT060DrillRepeatedVerifyApproveReleaseCloseNoFlips`（drill：同一 operation id 重复 ≥10 次 + 真实撤销，单行单裁决零翻转）、`TestT060DrillRestoreReentryConvergesWithSingleState`、`TestT060DrillInstanceCloseOnlyWhenAllReleasedAndIdempotent`（drill）→ **通过**；`TestGateCacheGenerationAwareReuse`（unit）**通过**；`TestT044RepeatedApproveReleaseCloseDoNotFlipState`（PG）本轮未执行 → **NOT RUN**。

## 4. CI 配置检查与分类正反例（T062/B18；本地静态，非 runner）

| 检查 | 命令/方法 | 结果 |
|---|---|---|
| workflow YAML 语法 | `python3 -c 'yaml.safe_load(...)'`（PyYAML 可用；`yamllint`/`actionlint` 本机不可用） | **通过**（jobs: build/changes/ci-required/contract/e2e/integration-kafka/pg/redis/lint/unit；无 drill job） |
| 分类脚本端到端 | 抽取 `changes` job 的 `run` 脚本，`EVENT_NAME=workflow_dispatch` 执行（保守全层） | **通过**；4 层守卫全绿 + drill 隔离守卫输出「ci.yml 无 drill 调用；drill.yml 无 pull_request」 |
| 分类正例（路径驱动） | 伪造 changed 列表执行同一脚本：`internal/recovery/gate.go`、`internal/recovery/sources/chain.go`、`internal/events/recovery_boundary_integration_test.go`、`docs/evidence/015/README.md` | `pg=true`（recovery 命中）、`kafka=true`（recovery_boundary 命中，**修复既有分类缺口**）、docs-only 使 redis/e2e=false；**通过** |
| 反向守卫反例（未分类应显性红） | 在临时副本移除 `internal/recovery/*` 后执行 | **红**：列出 14 个未覆盖 `//go:build integration` 文件并以 `::error::` 退出 1 |
| 反向守卫正例 | 真实路径集 | 绿：pg 232、redis 4、kafka 7、e2e 3 个 tagged 测试文件全部命中（B18 复验同值）；空枚举→红（保留原语义） |
| drill 隔离守卫正例 | 真实 `drill.yml`（schedule/dispatch，无 `pull_request`） | **通过** |
| drill 隔离守卫反例 | 向 `drill.yml` 注入 `pull_request:` 触发 | **红**（`::error::` 退出 1） |
| drill 隔离守卫缺位情形 | 删除 `drill.yml`（临时副本） | 输出显式 notice（drill 通道缺位时如实声明），不冒充通过 |
| 自匹配回归 | 守卫在真实 `ci.yml` 上运行（grep 自身） | 无误报（正则用 `[-]tags`/`test[-]drill` 防自匹配） |
| Makefile 只读核对 | `make test-contract` 目标（Makefile:41-43）仍含 `./internal/recovery`，该行未改动 | **no-op**（见 §7 报告项） |
| drill 标签是否进普通 PR | 全仓 grep + job 列表 + `changes` 脚本守卫 | **确认不在**：`ci.yml` 无 drill 调用；drill 标签测试只在 `drill` 通道（`make test-drill`，schedule/dispatch）编译 |

## 5. 本轮 analyze 语义收口（scope / effect / 事件身份）

- **scope（入口真实可执行范围）**：入口构造器只表达 `chain=<id>;capability=<c>`（canonical，键排序）；`asset`/`kind` 维度保留给更细部署授权，**入口不消费更细 scope**——在更细 scope 上放行只构成更窄授权流，不解除入口能力；**省略维度 ≠ 通配**；依赖在 `DependencyScope` 显式映射（同链/资产/业务类型，仅替换能力）求值；旧 opaque/跨能力串 `scope_mismatch` 拒绝零写。载体：`scope_test.go`（`TestCapabilityScopeCanonicalAndFailClosed`、`TestParseCapabilityScopeEquivalenceAndRefusals`、`TestDependencyScopeMapsExplicitly`、`TestGateAdmitRequiresCanonicalCapabilityScope`）→ unit **通过**。文本：`contracts/resumption-gate.md` §2.1、`data-model.md` §3.2。
- **effect（降档只用可信配置）**：effect class 只来自受控部署 `TXHARBOR_RECOVERY_EFFECT_CLASS_RULING`，调用者/`--scope` 不得自声明降档；未配置/未知一律保守 **dual**；新提款创建/既有提款恢复恒 dual，ruling 不能收窄；事件发布/消费仅在 ruling 对该业务类型显式 `no_real_downstream_effect` 时收窄；批准行快照固定保守档。真实下游 effect class 清单属**部署前裁决**（plan/research 待裁决单列）。载体：`scope_test.go`（`TestParseEffectClassRulingCarriage`、`TestRequiredApprovalClassForScopeConservativeDefaults`）、`gate_unit_test.go`（`TestRequiredApprovalClassConservativeDefault`）→ unit **通过**。文本：`contracts/approval-matrix.md` §3、`contracts/resumption-gate.md` §2.1。
- **事件身份（沿 013 契约）**：稳定去重身份 = `event_id`（自然键确定性 UUIDv5，`specs/013.../contracts/events.md`）＋ source 三元组；消费幂等键 = `(consumer_name, event_id)`（`consumer_inbox`，013 `contracts/consumer.md`）；**broker offset 只是投递位置/消费进度**（用于回退检测，PG 进度 vs offset），不是业务身份；业务效果去重属下游独立判定；不承诺跨系统恰好一次；offset/inbox 回退不自动触发有副作用的重处理。载体：`boundary_unit_test.go`（`TestBoundaryDedupIdentity`、`TestBoundaryExactlyOnceNeverClaimed`、`TestBoundaryOffsetRelationClosedSet`）→ unit **通过**。文本：`contracts/verification-items.md` §3。

## 6. NOT RUN / 待核验（不得读作通过）

| 项 | 状态与原因 |
|---|---|
| GitHub runner 上的 `ci.yml` / `drill.yml` 实际运行 | **待核验**（未推送/未触发；§4 为本地静态模拟；drill.yml 仅 schedule/dispatch，本记录不构成远程通过） |
| integration-redis / integration-kafka 全量 | **NOT RUN**（本轮未执行；drill 内真实 Kafka 事件场景通过 ≠ 该层全量验收） |
| e2e 全量（`make test-e2e`，Anvil + 全栈） | **NOT RUN**（本轮未执行；serve/HTTP 级硬断言随之未执行） |
| `make test-race` 全量 | **NOT RUN**（本轮仅 T063 聚焦 race） |
| 生产 RPO/RTO/频率/保留与任何生产阈值 | 未测未裁决；drill/S12 数值为**本地测试输入**（`local_values_only`），不构成生产恢复目标 |

## 7. 持久载体（本轮全部新增/修改，未提交）

- `.github/workflows/ci.yml`（B18）：pg 分类臂增列 `internal/recovery/*`；kafka 分类缺口修复 `internal/events/recovery_boundary*.go`；四层反向覆盖守卫（integration→pg / integration_redis→redis / integration_kafka→kafka / e2e→e2e）；drill 隔离守卫；头部注释与 step summary 同步；本地静态复验 pg 232 / redis 4 / kafka 7 / e2e 3 全绿。
- `.github/workflows/drill.yml`（B17/B18）：drill 独立通道（schedule/dispatch，无 `pull_request`）；`make test-drill` 接线；NOT RUN 纪律（无测试/无 Docker/Kafka 不可用/中断均不冒充通过）；S12 记录归档 `$TXHARBOR_DRILL_EVIDENCE_DIR` → run artifact。
- `Makefile`：`test-drill` 目标保留并同步 NOT RUN 纪律与证据归档注释；`test-contract` 行未改动（仍含 `./internal/recovery`）。
- `internal/app/degradation.go`、`internal/app/serve.go` + `internal/app/degradation_recovery_test.go`：T063 恢复期诚实报告（`recovery` 姿态块、探针非声明头、`degradation.recovery` 装配）与 4 个单元证据。
- `internal/indexer/depositscanner_test.go`：写路径守卫扩展——仅允许 015 V1 只读适配器 `internal/recovery/sources/chain.go` 的两个固定只读形态提及 `deposit_observations`；白名单外文件与写语句保持显性红（含负例）。
- `internal/recovery/sources/chain.go`、`internal/recovery/sources/ops.go`、`internal/recovery/verification.go`：真实只读源适配器与核验即时性修复（评估即时晚于源读取，消解 V9 “future timestamp” 抖动）。
- `internal/recovery/drill_e2e_test.go`、`drill_failure_test.go`、`drill_idempotence_test.go`、`drill_harness_test.go`：drill 13 用例（S1–S12 + F1–F7）持久载体；`internal/recovery/verification_instant_contract_test.go`：T038 contract 回归。
- specs 8 文件（未提交）：`adr/ADR-001-recovery-control-store.md`、`adr/ADR-002-backup-carrier-and-recovery-point.md`、`contracts/approval-matrix.md`、`contracts/backup-manifest.md`、`contracts/resumption-gate.md`、`contracts/verification-items.md`、`data-model.md`、`quickstart.md`。
- docs 3 文件（未提交）：`docs/ops/recovery-runbook.md`（T064）、`docs/evidence/015/README.md`（T065/T068）、`docs/evidence/015/quickstart_matrix_evidence.md`（本文，T066）。
- 以上全部位于 HEAD `65df393` 之上的未提交树；原始日志在 `/tmp/opencode/`（未入库）；持久证据以测试文件 + 提交为准。

## 8. 历史快照结论

- 历史记录：当时树上列明的 lint / build / unit / contract / 聚焦 race / CI 静态复验、PG 具名入口 2/2、drill 13/13 报告为通过；drill 所用真实依赖与当时范围见 §1。此记录只陈述当时运行结果，不验证当前代码；有限指纹/场景断言不支持普遍的零重复付款或零下游效果声明。关于未直写控制状态等反作弊陈述也仅是当时执行者对该次记录的说明，不能据此推出实现不存在其他路径。
- 历史记录中的未执行项：远程 CI（未推送/未触发）、integration-redis/-kafka 全量、e2e 全量、test-race 全量；**NOT RUN 不得读作通过**，不得由已通过层推断。当前 precommit 树结果仅见 §9，不回写或覆盖此历史记录。
- 三口径分离不变：本地执行 ≠ 远程 CI ≠ 生产就绪。**T000-P 保持 OPEN**；仅本地范围；不宣称生产就绪；drill/S12 与本地数值一律仅为测试输入。

## 9. 历史 precommit 树快照（64140fb follow-up 旧口径；e7ba8c0；62/70；非当前验收）

- **树范围**：基线 HEAD `e7ba8c0` + 全部本地未提交实现；以下结果是该 precommit tree 的观察，不是单独 HEAD `e7ba8c0` 的结果。最终提交将标识归档。原始日志均在 `/tmp/opencode/`，非持久证据。
- 全仓门禁：`make lint && make build && go test -count=1 ./... && make test-contract && make test-race && git diff --check` — **全部通过**。
- 全仓集成：`make test-integration` — **最终全仓通过**；日志 `/tmp/opencode/015-integration-all-final-tree.log`（本地非持久）。
- e2e：`make test-e2e` — **通过**；日志 `/tmp/opencode/015-e2e-final-tree.log`。其 Anvil bound-worker 覆盖一笔真实 broadcast、拒绝期间不重复；不证明拒绝解除后的安全继续。
- 独立 drill：`go test -tags=drill -count=1 -timeout=20m -v ./...` — exit 0，跨所有包 877 PASS / 11 顶层 SKIP / 0 FAIL；日志 `/tmp/opencode/015-drill-final-tree.log`。本机直接 `pg_restore` ELF 缺失；多个真实 restore/verify/drill 正向场景因此 NOT RUN/SKIP，即使测试命令 exit 0 亦不得宣称覆盖。Kafka offset divergence 场景的 restore 同样 NOT RUN/SKIP。
- Kafka：`make test-integration-kafka` — **通过**；日志 `/tmp/opencode/015-kafka-final-tree.log`。
- Redis：`make test-integration-redis` 在最后一次仅修改 `gate.go` 前、相同实现上通过（redis/cache/ratelimit/testutil），日志 `/tmp/opencode/015-redis-final.log`。作为该未受 gate.go 影响层的复用证据记录；不是修改后的最终重跑。
- recovery PG 专项：`go test -tags=integration -count=1 -timeout=12m -v ./internal/recovery` 在最后一次仅修改 `gate.go` 前 190 PASS / 9 SKIP / 0 FAIL，日志 `/tmp/opencode/015-recovery-pg-current.log`；之后 `make test-integration` 最终全仓通过，但没有 verbose skip 计数。
- 失败历史：较早的失败运行由 fixture 变化修复，属于历史失败，不是当前最终结果；不得将它们说成当前失败，也不得将最终通过倒推为此前运行通过。
- 仍未执行/未证明：远程 CI、push、PR、merge、deploy 均 **NOT RUN**；T017/T019/T020/T027/T057/T058/T066/T068 仍 OPEN（62/70），T000-P OPEN。真实直接 `pg_restore` ELF 正向路径缺位；T068 有限证据仅包括 manifest 双排除项、credential-shaped reason 拒绝及子进程 argv/passfile 保护，父 CLI argv、SIGKILL 后 passfile 残留及 ELF 信任未解决/未证明。无生产就绪声明。

## 10. `64140fb` follow-up final drill supplement（2026-09-30；not overall acceptance）

- 本节和 [follow-up acceptance ledger](64140fb-followup-acceptance.md) 记录基线 `64140fb` 之后本地 dirty tree 的补充，不改写 §1–§9 历史矩阵或历史 **62/70、11 top-level SKIP**。T017/T027 功能验收由 owner 判定满足，当前 64/70；T019/T020/T057/T058/T066/T068 OPEN，T000-P OPEN。此文档 lane 不更改 tasks checkbox。
- Final drill `make test-drill`: required 22 = **19 PASS / 3 FAIL / 0 SKIP**; top-level = **913 PASS / 3 FAIL / 0 SKIP**; all test events = **2049 PASS / 3 FAIL**; exit 2. Failures: `TestT058DrillEventLayerKafkaBrokerOffsetDivergence` (Kafka broker offset target guard not clean), `TestT059F2RestoreInterruptionNotRestoredRebuildRerunIdempotent` and `TestT060DrillRestoreReentryConvergesWithSingleState` (original identity/process proof not established). No destructive acceptance.
- Independent owned-process commit-before-READY / same-role kill-reap / reconnect-rejection PASS, and two actual acceptance-barrier observer/login-loss negative cases PASS. These establish narrow cancellation boundaries, not full rebuild lifecycle. Query-only approval is tested; actual post-approval fund/consumer effects remain unproven. Do not claim zero repeat payments or erroneous effects globally.
- Host missing-client negative exits 2 and names 13 required top-level plus 3 nested F1/F3 NOT RUN; overlapping names are counted once. Missing tree fingerprint also causes refusal; real tool skips are independently decisive. The earlier 913 PASS/3 FAIL/0 SKIP attempt with malformed metadata is invalid structured-metadata evidence and is not replaced by a retry PASS.
- Tested-tree fingerprint `sha256:d761a4fa68d40e1eff40937ec466bdf2953664621b15d2aedabe8a942b6b552a`; source stable before/after. This fingerprints a dirty tree, not final commit. Valid metadata, tree validation and reduced terminal event artifacts are in `followup-final/`; raw logs remain under `/tmp/opencode/` and are not committed.
- Final full PG integration run `/tmp/opencode/015-resumed-final/pg-integration.jsonl`: exit 1; top-level **1755 PASS/8 FAIL/2 SKIP**, all test events **3765 PASS/8 FAIL/2 SKIP**. Failures: app `TestServeDurablePauseKeepsServiceAliveAndPaused`, health `TestReadyzFlipsAndRecoversWithRealDependencies`, nonce/signer/withdrawal `*MigrationHistoryUntouched` (environment-dependent; host rerun 5/5 PASS), recovery `TestVerifyBackupRejectsMissingAuthoritativeObject` (observed `unverified`, expected `rejected`), and two restore invalidation cases which were denied earlier by unknown/active isolation guard rather than reaching expected stale-state assertions. Full suite remains FAIL. Skips: txlifecycle `TestCrashHelper` (not helper) and withdrawal `TestWithdrawalIntakeStorageDown` (owned fault injection); neither is a required recovery/drill scenario.
- Package outcomes: full `internal/app/recoveryadmin` including real native CLI S11 close, verify/approve/release/status, interruption pair and record-only negative PASS; controlstore PASS. The original full `internal/recovery` run had three failures; the subsequent fix81 focused rerun passed all three affected tests. These focused passes do not change the full PG suite failure.
- `static-unit-contract-race.log` plus `validation-status.json`: static/unit/contract/race exit 0; full PG exit 1. Isolated host-only rerun `/tmp/opencode/015-resumed-final/host-environment-rerun.log` exit 0, 5 PASS/0 SKIP/0 FAIL, no source edits. Artifact summaries, terminal events (action/package/test only), raw-log SHA-256 values and exact failure list are indexed in the ledger and `followup-final/`.
- **S/F semantic result matrix (local evidence only; “partial” is not pass):**

| Scenarios | Status | Evidence boundary |
|---|---|---|
| S1 backup | PARTIAL | Main drill and available CLI evidence pass; not enough for entire S1 acceptance. |
| S2 verify backup | PARTIAL | Real artifact positive evidence exists; missing-authoritative-object PG case failed (`unverified`, expected `rejected`). |
| S3 open recovery instance | PASS (scoped) | Named control-store/drill opening and fail-closed contract assertions passed. |
| S4 restore | FAIL | F2/T060 drill required failures and two PG invalidation assertions failed before expected stale-state check. |
| S5 isolation | PARTIAL | Owned-process and observer negatives pass, but F2/T060 original identity/process proof was not established. |
| S6 fact verification | PARTIAL | Named CLI verification passes; Kafka offset target-guard drill fails. |
| S7 gap blocking | PASS (scoped) | Named gap-paused drill and refusal assertions pass; not a whole-system guarantee. |
| S8 independent capability release | PASS (query-only scope) | Named verify/approve/release/status path passes; no claim for post-approval funds/effects. |
| S9 graded progression | PARTIAL | Named approval/release tests pass; full external-effect progression not established. |
| S10 truthful status | PASS (scoped) | Named status and recoveryadmin real-entry evidence passes; does not offset suite failures. |
| S11 close instance | PASS (scoped) | Real native CLI positive close and lifecycle core pass; does not constitute full PG suite acceptance. |
| S12 metrics/archive | PARTIAL | Structured drill artifacts exist, but drill overall fails and destructive acceptance was not reached. |
| F1 unusable backup | PARTIAL | Corrupt/unverified matrix passes; missing authoritative object PG expectation fails. |
| F2 interruption/partial restore | FAIL | Required drill identity/process proof fails; PG stale-state cases are blocked by unresolved active/unknown isolation guard. |
| F3 incompatibility | PASS (scoped) | Named compatibility rejection cases pass; limited to listed inputs. |
| F4 external facts lead | FAIL | Kafka committed-offset target guard not clean; no global zero-replay/effect claim. |
| F5 old instance isolation | PARTIAL | Narrow process/reconnect/observer negatives pass; full original-identity witness is not established. |
| F6 unprovable gap | PASS (scoped) | Named gap-paused/unknown and audit assertions pass. |
| F7 unauthorized/stale approvals | PASS (scoped) | Named authorization/approval and control-store assertions pass. |

- Fix81 focused recovery integration: **3 PASS/0 FAIL/0 SKIP** on pinned PG18.6; details and sanitized summary in [the ledger](64140fb-followup-acceptance.md) and `followup-final/fix81-focused-pg-summary.json`. This narrow rerun does not alter the full-suite **1755 PASS/8 FAIL/2 SKIP, exit 1** result.
- Current owner task count **64/70** (only T017/T027 marked accepted; original task text unchanged); six follow-up tasks remain OPEN. Historical **62/70 + 11 top-level SKIP** are preserved. Final source fingerprint is `sha256:43e15d6a13224f600f15b786aa016ab995c3d90eda3f8ec0705be75ef6569514`; it differs from the pre-fix81 drill source fingerprint above only in the two integration-test files, with docs/tasks excluded. Runtime/drill inputs unchanged; affected unit and script-parser/bash-syntax checks PASS. Resolve commit identity via evidence-path `git log` or parent final report, not by equating it to a fingerprint. Parent confirmed this fingerprint and will repeat the exact-source check before commit. Remote CI/push/deploy NOT RUN; T000-P OPEN.
