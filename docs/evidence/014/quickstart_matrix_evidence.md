# 014 Quickstart 验证矩阵证据（T031）

- Feature: `014-reconciliation-exception-handling`
- 批次: T031（Polish）｜日期: 2026-09-27
- 树状态: HEAD `1f18d88` + 未提交工作树（T029 metrics、T030 honesty 测试、T031 单元证据钉与本文）；迁移 tip `000016_reconciliation_handling.sql`
- 任务边界: 本文件只记录**本地测试证据**，不宣称生产阈值、不宣称生产就绪；T000-P 保持 OPEN；未提交、未推送。

## 0. 状态口径（三者分离）

- **本地验证**：unit/contract 层无需 Docker；integration 层为真实 PostgreSQL（testcontainers `postgres:18.6-trixie`）+ 真实迁移 `000001..000016`；014 不依赖 Anvil（无 014 e2e 面），Anvil/e2e/Redis/Kafka 本轮未运行。
- **命令入口边界**：CLI 测试经 `internal/app/reconcileadmin` 的 `Run(ctx,args,Deps)`（`cmd/txharbor/main.go:75` 调用的同一函数）驱动；另做了 built-binary usage 冒烟（`/tmp/opencode/014-t031/txharbor reconcile-admin`，exit 2，打印全部子命令），未做带 DB 的 binary 全链路（由包内集成测试覆盖同一 Run 函数）。
- **远程 CI**：未推送/未触发 → 待核验（本文件只做静态路径核对 + 本地分层运行）。
- **生产就绪/阈值**：未宣称；本地数值仅为测试输入（Q3-6）。
- 原始日志目录：`/tmp/opencode/014-t031/`（机器本地、未入库）；持久载体 = 本文件 + 测试文件 + 未提交工作树。

## 1. tags × suites 矩阵执行结果

| 层 | 命令（实际执行） | 范围 | 结果 | 日志 |
|---|---|---|---|---|
| lint | `make lint`（gofmt + `go vet` unit/integration tags） | 全仓 | 通过 | `lint-build.log`、`final-fast-layers.log` |
| build | `make build` | 全仓 | 通过 | 同上 |
| unit | `make test` | 全仓（无 Docker） | 通过 | `final-fast-layers.log` |
| race 抽样 | `go test -race -count=1 ./internal/reconciliation ./internal/app/reconcileadmin ./internal/metrics ./internal/db` | **仅 014 影响面 4 包，非全量 `make test-race`** | 通过 | `race-sample.log` |
| contract | `make test-contract` | 全仓（无 Docker） | 通过 | `contract.log`、`final-fast-layers.log` |
| integration | `make test-integration` | 全仓 PG 层（真实 PG） | 首跑失败→修正后复跑**通过**（详见 §4） | `integration-pg.log`（首跑失败）、`integration-pg-rerun.log`（通过） |
| fault（014） | `go test -tags fault -count=1 ./internal/reconciliation` | 014 纯函数逻辑（T024） | 通过；**不称真实依赖故障演练** | `fault-014.log` |
| perf（014） | 无 014 perf 文件（`grep '^//go:build perf'` 于 014 路径为空） | — | **NOT RUN**（014 无 perf 载体；不重跑 013 历史 perf 长测） | `fault-014.log` 末尾探针 |
| e2e / redis / kafka | 未运行 | 014 不新增这些载体（quickstart L44） | NOT RUN（与 014 无关） | — |

关键复跑（收口证据）：

- `go test -tags integration -count=1 -run 'TestIntentFKRepair' ./internal/db` → ok（22.5s）
- `go test -tags integration -count=1 -run 'TestSignerMigration' ./internal/signer` → ok（20.1s）
- `go test -tags integration -count=1 -v -run 'TestIntegrationReconcileAdminUS3CallPathConvergence' ./internal/app/reconcileadmin` → PASS（4.4s）
- `go test -tags integration -count=1 -v -run 'TestIntegrationCloseReadsLatestReverifyRowFromDB|TestIntegrationCloseRequiresFreshConsistentUngappedEvidence|TestIntegrationEventOnlyAbsenceStillMintsFailClosedTicket|TestIntegrationPausedTaskNeverRendersFullyConsistent' ./internal/reconciliation` → PASS（13.8s）
- `go test -tags integration -count=1 -v -run 'TestIntegrationDisposeIdempotencyZeroSideEffects' ./internal/reconciliation` → PASS（10x 重放循环）
- `go test -tags integration -count=1 ./internal/reconciliation` → ok（最终树包级复跑，65.5s）

## 2. quickstart §1–11 覆盖索引（FR/SC/Q/任务 → 真实证据）

边界列：`纯函数`＝无 DB 无 Docker；`真实PG`＝testcontainers 真库真迁移；`命令入口`＝同 `Run` 函数/二进制 usage。

| § | FR/SC/Q・任务 | 主要证据（测试） | 边界 | 本轮结果 |
|---|---|---|---|---|
| §1 漏处理检出 | FR-002/007/008/015, SC-001, Q1；T011/T012/T016/T017/T033/T034 | `TestClassifyMissingTicketsStableIdentity`、`TestCompareScanIntervalMissingPGBusinessRecordStillTickets`；`TestIntegrationScanOnceChainFirstAcceptance`、`TestIntegrationReconcileAdminAttributionAcceptance`、`TestIntegrationReconcileAdminEntrySmoke` | 纯函数＋真实PG＋命令入口 | 通过（集成全绿） |
| §2 正常重复零建单 | FR-009/016, SC-001/004, Q4；T017/T021/T022/T035 | `TestClassifyAbsorbedDuplicateIsMetricsOnly`、`TestClassifyAbsorbedLegalOldVersion`、`TestClassifyDivergentDuplicatesTicket`、`TestEventStateAbsorbedDuplicateIsMetricsOnly`；`TestContractIdempotencyKeyReadBackClassification`；`TestIntegrationDisposeIdempotencyZeroSideEffects`、`TestIntegrationScanOnceTicketDedupAcrossScans` | 纯函数＋contract＋真实PG | 通过 |
| §3 证据不足 | FR-004/005/006/018, Edge, Q1；T016/T017/T030 | `TestClassifyEvidenceGates`、`TestClassifyUnconnectedNeverConsistent`、`TestClassifyConnectedFlagAloneDoesNotProveUpstream`、`TestCompareScanIntervalIncompleteCoverageNeverConsistent`；`TestIntegrationUpstreamUnconnectedNeverConsistent`、`TestIntegrationPausedTaskNeverRendersFullyConsistent`、`TestIntegrationEventOnlyAbsenceStillMintsFailClosedTicket` | 纯函数＋真实PG | 通过；T030 第三项 blocker 见 §6 |
| §4 重组保守 | FR-017, SC-003, US3-1, Q5；T024/T026/T035 | `TestFaultT024ReorgInvalidationIsConservative`（纯函数）、`TestTimeHeightResolverReorgInvalidated`、`TestContractQ5InvalidationGuards`；`TestIntegrationTxAggregateIdentityAcceptance` | 纯函数＋contract＋真实PG | 通过（逻辑层）；真实链重组演练 NOT RUN |
| §5 并发闭合 | FR-010, SC-003/005, US2-2/US3, Q2/Q5；T019/T020/T025/T026/T027 | `TestContractClaimIsSingleOwnerCAS`、`TestContractReverifyVerdictsAndCloseFreshness`；`TestIntegrationDiscrepancyClaimMutualExclusion`、`TestIntegrationCloseReadsLatestReverifyRowFromDB`、`TestIntegrationRecoveryCrashResumeAndOverlap`；`TestFaultT024ConcurrentUpdatesInvalidateClosedOnly`、`TestFaultT024StaleResultCloseNeverCloses` | contract＋真实PG＋纯函数 | 通过（并发反例 T025 落库） |
| §6 越权矩阵 | FR-011/012, SC-005, Q2；T009/T020/T023 | `TestIntegrationAuthorizationRefusalsAreAudited`、`TestIntegrationClaimOwnershipIsNotADisposalRight`、`TestIntegrationReconcileAdminClaimReplayAcceptance`、`TestIntegrationReconcileAdminUS3CallPathConvergence`（close 默认拒绝+审计）、`TestAuthzRangeCoveredDirectedCases` 等 | 真实PG＋纯函数 | 通过（拒绝 100% 审计，见测试断言） |
| §7 中断恢复 | FR-003, SC-003/006, US3-3, Q3；T006/T010/T025 | `TestIntegrationScopedScanCheckpointResumeAndBudget`、`TestIntegrationRecoveryCrashResumeAndOverlap`、`TestIntegrationScanOnceBudgetedCommitAndSuspend`；`TestFaultT024InterruptRestartConvergesConservatively` | 真实PG＋纯函数 | 通过 |
| §8 超预算 | FR-019, SC-006, Q3；T008/T016/T038 | `TestContractBudgetBoundsSuspendObservably`；`TestIntegrationScanOnceBudgetedCommitAndSuspend`、`TestIntegrationPausedTaskNeverRendersFullyConsistent` | contract＋真实PG | 通过 |
| §9 重复处置幂等 | FR-016, SC-004, US2-3；T019/T022/T023 | `TestContractIdempotencyKeyReadBackClassification`；`TestIntegrationDisposeIdempotencyZeroSideEffects`（**T031 补 10x 重放循环**：同一幂等键重放 10 次零新副作用）、`TestIntegrationReconcileAdminClaimReplayAcceptance` | contract＋真实PG | 通过 |
| §10 预算与隔离 | Q3/ADR-001；T028 | `TestContractBudgetBoundsSuspendObservably`；`TestIntegrationReconcileAdminEntrySmoke`；built-binary usage 冒烟；ADR-001（无 daemon、serve/worker 不自动启动） | contract＋真实PG＋命令入口 | 逻辑/接线通过；**真实故障注入＋并发资金负载 NOT RUN** |
| §11 历史复查 | FR-007/010, Q5；T027/T028/T035 | `TestIntegrationReconcileAdminUS3CallPathConvergence`（真实 CLI reverify：bounds 必填、单方读取=unknown、gap 可见、verified_complete=false、跨检查点失效、旧 consistent 不得再闭合）；`TestIntegrationTxAggregateIdentityAcceptance`；**T031 新增** `TestNormalizeReverifyFindingNeverTakesCallerConsistentOnFaith`、`TestReverifyInvalidationTriggerNeverWidensVocabulary` | 真实PG＋命令入口＋纯函数 | 通过 |
| 横切：metrics(T029) | FR-024/027 | `TestReconMetricsRegistryManifest`（`internal/metrics/reconciliation_test.go`） | 纯函数 | 通过（unit） |
| 横切：docs/policy(T037) | Q5 | `TestScanTaskConfirmThresholdN`（policy_refs 快照） | 纯函数 | 通过（unit） |

## 3. 特殊核实项（T031 要求 #4）

1. **reverify 通过实际来源复查，而非接受调用者声明**
   - CLI `reverify` 构造生产 `EventStateReverifyEvaluator`（真实 T015 event adapter + reference-consumer + 只读 quarantine + T036 window resolver），读真实 PG 事件面；集成测试证明单方读取只能得 `unknown`、不回写 consistent、gap 可见、`verified_complete=false`（`internal/app/reconcileadmin/us3admin_integration_test.go`，本轮 PASS）。
   - 调用者声明防线：`normalizeReverifyFinding` 把「consistent 但缺 evidence_ref/新鲜度」降级为 `unknown`；新增单元测试逐例钉死（`internal/reconciliation/reverify_finding_honesty_test.go`，本轮 PASS）。未知 verdict 词元为 wiring defect（error），不会静默变成可闭合判词。
2. **close-basis 不能替换数据库复核结果**
   - `Store.CloseDiscrepancy` 只经 `LoadDiscrepancyEvidence` 读取 DB 中最新 `reverify` 行（调用方无法注入更新证据）；
   - 证据：`TestIntegrationCloseReadsLatestReverifyRowFromDB`（旧 fresh consistent 输给新 unknown；stale/future 一律拒绝；缺 tolerance 不闭合；拒绝不改状态、不写 close_basis、写 refuse 审计；仅 fresh consistent 可闭合且记录 close_basis）＋`TestIntegrationCloseRequiresFreshConsistentUngappedEvidence`（未验证/过期/gap-limited/缺 basis 拒绝）——本轮均 PASS。
3. **reverify-tolerance 来源合法范围与授权**
   - 来源：显式 `--reverify-tolerance`（必须为正）或 `TXHARBOR_RECON_FRESHNESS_TOLERANCE`；缺失/非正值按名拒绝、无自造默认。新增单元测试 `TestParseReconcileCloseToleranceSourceAndRange`（含 nil/零配置拒绝并点名 env key）与 `TestParseReconcileCloseBasisRequiresObjectSnapshot`——本轮 PASS。
   - 授权：close = principal × `verify_close` × 工单记录范围，默认拒绝、拒绝亦审计（`TestIntegrationReconcileAdminUS3CallPathConvergence` 的 unprivileged close 子例，本轮 PASS）；reverify = scan_manage × 任务范围（auth-matrix 的 system-only 行未扩大，未新造权限）。

## 4. 失败修正记录（首跑红 → 修正 → 复跑绿）

首跑 `make test-integration` 在 `internal/db`、`internal/signer` 的迁移链断言上失败，根因均为 014 新迁移 `000016` 把联合链 tip 从 15 推到 16（历史硬编码未随新 lane 更新；非产品缺陷）。按既有 000015 lane 的同一模式重基线：

- `internal/db/intent_fk_repair_integration_test.go`
  - `TestIntentFKRepairIncrementalGuardedNoOpThenRepair`：era 夹具改为排除 16（`repairSetFS(t, 12, 14, 15, 16)`，避免历史夹具吸收新 lane），stage2 期望 `applied=4 skipped=12 (…000015, 000016)`。
  - `TestIntentFKRepairRerunIsNoOpSuccess`：全链复跑 `skipped=15` → `16`。
- `internal/signer/migration_integration_test.go`
  - chain head 15 → 16；lane extension 版本/名称表加 `16: 000016_reconciliation_handling.sql`；status 允许列表加新文件名；`TestSignerMigrationUpgradeDowngradeFrom007`（applied 8→9、DownTo 列表补 15、pending 8→9、re-up 8→9）、`TestSignerMigrationMergedChain`（16 / 8+8 / 15+15）同步。
- 结果：聚焦复跑与**整层 `make test-integration` 复跑全绿**（`integration-pg-rerun.log`, exit 0）。未改任何产品代码、迁移或 CI。
- 说明：其后 T031 仅在 `lifecycle_integration_test.go` 追加 10x 重放断言（§9）；该包已按包级整层复跑绿（`recon-integration-final.log`），其他包不受该文件影响。

## 5. CI 分层与路径核对（本任务未改任何 CI/Makefile；`git diff -- .github Makefile` 为空）

静态模拟 `.github/workflows/ci.yml` 分类臂（脚本 `ci-path-sim.sh`，样本路径 → 触发层）：

| 变更路径 | integration-pg | 说明 |
|---|---|---|
| `internal/reconciliation/**` | **false（缺口）** | 与 quickstart L45 备注一致：尚未列入 PG 路径集合；**仅报告，不自行改 CI/分支保护**（CI owner 增列） |
| `internal/app/reconcileadmin/**` | true | `internal/app/*` 命中，命令入口变更触发 PG 层 |
| `migrations/**`、`internal/db/**`、`internal/config/**`、`internal/metrics/**` | true | 014 迁移/配置/指标变更触发 PG 层 |
| `specs/**`、`docs/**` only | false（所有中间件层不触发） | lint/build/unit/contract 为无条件 job，014 契约测试仍在普通 PR 必跑 |
| `.github/workflows/**` | pg/redis/kafka/e2e 全 true | workflow 变更保守全触发 |

- 无条件 job（普通 PR 必跑）：`lint (gofmt + vet)`、`build`、`unit tests (+ race)`、`contract tests (no Docker)`；必需检查名稳定（另有 `ci-required` 汇总闸门，名称未变）。
- `fault-perf.yml` 仅 `schedule`/`workflow_dispatch`/`workflow_call`，无 `pull_request`；fault/perf 永不进普通 PR，失败不阻塞 PR 但阻塞对应发布声明。
- Makefile：`test`/`test-race`/`test-contract`/`test-integration`/`test-fault`/`test-perf` 目标与 tag 守卫未变；014 fault 有 tag 载体、014 perf 无载体（按守卫语义 NOT RUN）。

## 6. NOT RUN / 待核验（不得读作通过）

| 项 | 状态与原因 |
|---|---|
| GitHub runner 上的 ci.yml / fault-perf.yml 实际运行 | **待核验**（未推送、未触发） |
| 真实依赖故障演练（fault-perf.yml 的 Docker 故障注入） | **NOT RUN**；014 fault 层本轮只有纯函数逻辑测试（T024），不称真实演练 |
| perf 层 | **NOT RUN**（014 无 perf 文件；未重跑 013 历史 perf） |
| 全量 `make test-race` | 未跑（仅 014 影响面 4 包 race 抽样） |
| e2e / integration_redis / integration_kafka | 未跑（014 不为 Redis/Kafka 增加权威状态、不新增 e2e 流程） |
| T030 第三项（pre-cutover 无事件行 → pending 而非 missing 票） | **BLOCKED**：缺 cutover/预期事件判别器，保持 fail-closed 旧路径（`scan_evidence_honesty_test.go` 头注；tasks.md:83 ③） |
| US1 已记录剩余限制 | scope 末端右缝 `query_failed` gap、intent 键 `linked_row_missing` 只能 pending、pre-cutover 事件缺票、chain-first `tx_hash` 无 EventKey（tasks.md:79-83） |
| 生产阈值/容量/暂停响应 | 未测未裁决；本地数值≠生产阈值 |

## 7. 持久载体（本轮新增/更新，未提交）

- 新增 `internal/reconciliation/reverify_finding_honesty_test.go`（unit；调用者 consistent 声明必须带证据/新鲜度，否则降级 unknown；trigger 词表闭合）。
- 新增 `internal/app/reconcileadmin/us3admin_unit_test.go`（unit；close tolerance 来源/正性、close-basis 必为 JSON object）。
- 更新 `internal/db/intent_fk_repair_integration_test.go`、`internal/signer/migration_integration_test.go`（000016 joint tip 重基线，见 §4）。
- 更新 `internal/reconciliation/lifecycle_integration_test.go`（T031 在 SC-004 幂等测试内补 10x 重放循环）。
- 引用（T029/T030 已在工作树）：`internal/metrics/reconciliation.go`＋`reconciliation_test.go`、`scan_evidence_honesty_test.go`、`scan_evidence_honesty_integration_test.go`、`lifecycle_evidence_honesty_integration_test.go`。
- 原始日志：`/tmp/opencode/014-t031/`（`final-fast-layers.log`、`race-sample.log`、`contract.log`、`integration-pg.log`、`integration-pg-rerun.log`、`fault-014.log`、`special-us3.log`、`special-reconciliation.log`、`ci-static.log`、`binary-smoke.log`）。

## 8. 结论

- 普通 PR 相关层（gofmt/vet、unit、race 抽样、contract、integration-PG）在本地真实 PG 上全部通过；fault/perf 保持独立通道，普通 PR 未新增长测。
- 首跑暴露的两处迁移链断言已按既有 lane 模式修正并复跑全绿；`internal/reconciliation/**` 未列入 PG 路径集合属已知缺口，仅回报。
- 本文件与测试证据不构成发布或生产就绪声明；T000-P 保持 OPEN。
