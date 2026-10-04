# 015 证据索引与纪律（T065）

- Feature: `015-backup-recovery-safe-resumption` ｜ 分支: `015-backup-recovery-safe-resumption` ｜ main 基线: `8d4b9af`
- 设计: [spec.md](../../../specs/015-backup-recovery-safe-resumption/spec.md) · [plan.md](../../../specs/015-backup-recovery-safe-resumption/plan.md) · [quickstart.md](../../../specs/015-backup-recovery-safe-resumption/quickstart.md) · [contracts/](../../../specs/015-backup-recovery-safe-resumption/contracts/) · [docs/ops/recovery-runbook.md](../../ops/recovery-runbook.md)
- **Gate: T000-P 保持 OPEN**。本目录不是发布，不宣称生产就绪；所有本地数值仅为测试输入，生产 RPO/RTO/频率/保留未裁决（FR-036）。
- **快照口径**：历史 §5a/§5b 与 quickstart matrix §1–§9 保留，不代表当前验收。Follow-up ledger 记录本地证据：仅 T017/T027 由 owner 标记满足，当前 **64/70**；T019/T020/T057/T058/T066/T068 与 T000-P 保持 OPEN。历史 62/70 与 11 顶层 SKIP 原样保留。Drill 必需项 19/22 PASS、3 FAIL；全量 PG 集成 exit 1（1755 PASS/8 FAIL/2 SKIP）。后续 fix81 三项聚焦 PG 测试 PASS，不改变全量 PG 失败结论。Drill 树指纹 `sha256:d761a4fa68d40e1eff40937ec466bdf2953664621b15d2aedabe8a942b6b552a` 与当前源指纹 `sha256:43e15d6a13224f600f15b786aa016ab995c3d90eda3f8ec0705be75ef6569514` 的差异仅为两份 integration 测试文件（不含 docs/tasks）；指纹不是提交 ID。

## 1. 归口与命名

| 载体 | 内容 | 任务 |
|---|---|---|
| `docs/evidence/015/README.md` | 本索引：归口、纪律、测试输入标注、反作弊、T068 凭据边界复核结论 | T065、T068 |
| `docs/evidence/015/quickstart_matrix_evidence.md` | quickstart S1–S12 + F1–F7 的分层执行/未执行记录（unit/contract/PG/drill 分开） | T066 |
| `docs/evidence/015/drill/drill-<drill_id>.json` | `recovery-admin drill` 归档产物（S12 分列度量 + 非声明；`log_ref` 指向） | T056/T057（B16） |
| `internal/recovery/*_test.go`、`internal/app/recoveryadmin/*_test.go` | 可执行证据的持久载体（unit / contract / integration 标签分层） | 各实现批次 |
| `docs/evidence/015/quickstart_matrix_evidence.md` 引用 | 复用既有批次证据时必须写明批次与文件；原始日志若在 `/tmp/opencode/` 未入库，以测试文件 + 提交为准 | 全部 |

本目录沿用 `docs/evidence/014/` 的证据模式：本地/远程/生产三口径分离、NOT RUN 显式（不得伪装通过）、持久载体 = 测试文件 + 提交 + `docs/evidence/015/` 报告（原始日志可留在 `/tmp/opencode/` 不入库）。

命名约定：归档文件 `<layer>_<scenario>_<date>.json|md`（如 `drill-f1_backup_unusable_or_unverified-20260929.json`）；文件内容必须自带 `scenario`、`result`、`test_inputs`/`local_values_only` 字段或章节，禁止无上下文的裸日志。

## 2. NOT RUN 纪律（违反即无效证据）

- 未执行的层/场景一律记 **NOT RUN**，写明原因；**NOT RUN 不得读作通过**，不得用「其余都绿」推断。
- Docker 缺位（或本 lane 验证预算不含 Docker 层）→ 相关 PG/Redis/Kafka/e2e/drill 层记 NOT RUN，不得记 pass；恢复：在具备真实 Docker 的环境重跑对应层后才可改记。
- `drill` 独立通道：普通 PR 永不运行 `drill` 标签（CI 反向守卫 + drill 隔离守卫强制）；演练未跑时 S1–S12/F1–F7 不得记通过。
- 替身/桩只可用于纯逻辑单测；门禁、核验、恢复、链、中间件、broker 的验收证据必须来自真实控制库、真实 `pg_dump/pg_restore`、真实链（Anvil/本地 RPC）与真实中间件。
- 超时、重试耗尽、预算耗尽、人工知悉：**不是**缺口闭合、复服许可或 RTO 达标。

## 3. 测试输入标注（不得冒充生产阈值）

- 本地演练/测试使用的 RPO/RTO/频率/保留、门禁 TTL、新鲜度容忍、核验批次上界、`status` 预算、backup_lag/uncovered_interval 等一律标注为**测试输入**（`test_inputs` / `local_values_only=true` / 文档明写「本地测试输入」）。
- 未配置必需约束 → 显式「未配置」（`constraints_configured=false`），不得宣称符合生产恢复目标；不得以数据库可连接宣称 RTO 达标。
- 对外结论限定本项目可验证范围：未接入真实上游/下游回执时不宣称外部账本一致；不承诺跨系统恰好一次。

## 4. 反作弊纪律（quickstart §4 摘要，违反即无效）

- 禁止直写控制库的批准/放行/缺口/隔离状态绕过命令；放行必须由门禁派生评估得出（INV-2）。
- 禁止盲恢复控制库（旧副本）绕过控制事实；受支持控制恢复只能走停机隔离 + 显式重建/supersede + 审计（F6）；不得以回退库内自写 supersede 行冒充重建。
- 禁止注入核验替身使门禁放行；V4 只读 accessor 白名单外的方法（如 `Reconcile` 写路径）不得用于核验取证（F4）。
- 禁止测试专用「关门禁」开关/环境变量绕过；禁止以健康探针/状态查询替代资金门禁判权。
- 真实恢复必须真实 `pg_dump/pg_restore`；链上事实必须来自真实链；Kafka 声明必须来自真实 broker。
- 演练不得为了「全复服」而绕过缺口：「缺口无法补齐 → 保持暂停」是验收场景本身。

## 5. T068 私钥/凭据边界历史记录（未构成当前验收）

- 这段是旧记录对 HEAD `65df393` + 当时未提交脏树的描述；扫描及抽样不是穷尽的秘密检测，也未对当前修复后的代码复核。T068 仍 OPEN，以下不得引用为当前安全结论。
- 历史复核范围：`internal/recovery/**` + `internal/app/recoveryadmin/**`（生产代码，不含测试）；当时采用 import 检视、若干关键词/形态搜索、既有 `secrecy` 单测载体和关键路径抽样。搜索不能证明所有形式的秘密均被发现。
- 当时结论仅为有限范围内「这些检查未发现所检路径中的问题」，不等于不存在违反 FR-007/INV-9 的路径。具体历史观察：
  1. **签名密钥只在 Signer 边界**：恢复核心不 import 任何钱包/密钥库（无 `crypto/ecdsa`、keystore、mnemonic 相关引用）；恢复环境不为恢复获取可直接持有的私钥，`CheckSignerBoundary` 只探测可达性并回传脱敏后的 endpoint（`internal/recovery/restore.go`）。
  2. **DSN 与自由文本分开考量**：历史记录称 DSN 会以 `--dbname=<DSN>` 传给 `pg_dump`/`pg_restore`，因此进程存活期间 DSN 暴露于本机进程参数表；这是 argv 暴露面，不等同于日志/审计/证据落盘。`--reason` 等自由文本是另一个输入面，不能因 DSN 脱敏或结构化校验就推定其内容安全；需单独检查其进入日志、审计、CLI 输出及证据的路径。历史记录对 `RedactedDSN`、`logx.Redact` 和 `data_target` 的描述并非穷尽保证，须在 T068 重验。
  3. **检测边界**：历史记录描述 manifest 排除项检查、`ScanManifestSecrets` 和子进程 stderr 的脱敏行为；它们是特定路径/模式的检查，不构成对任意秘密、编码形式或 `--reason` 自由文本的全量检测保证。
  4. **复核载体**：`internal/recovery/secrecy_test.go`（5 个单测：Redact canary、RedactedDSN、manifest 秘密扫描、Signer 边界、审计载荷拒 DSN/凭据），随普通 unit 层（`make test`）执行。
- 残余边界（不构成本阶段缺陷，需随证据携带）：
  - 历史记录称 `coverage.excluded` 校验硬性要求 `signer_private_keys`，而外部手改 manifest 省略 `real_credentials` 时校验层不单独拒绝。不能据此宣称嵌入凭据必被所有路径拒绝；`ScanManifestSecrets`/`ValidateDataTarget` 只覆盖其实现的特定规则，当前行为待 T068 重验。
  - **历史说明及其边界**：当时记录声称 `pg_dump`/`pg_restore` 以 `--dbname=<DSN>` 接收连接串，因此 DSN 在进程存活期间出现在本机进程参数表；当时还将其描述为不属于备份集/日志/审计/证据、命令结束即消失，并称可考虑 `PGPASSWORD`/`PGSERVICEFILE`。这些是历史记录中的技术判断，不是用户的风险接受，也不证明 argv 暴露已被当前实现处置；须在 T068 当前代码复核中确认并记录边界，不得据此宣称 FR-007/INV-9 已验收。
  - `logx.Redact` 是模式化脱敏；未匹配模式的新型凭据串可能不被遮盖。自由文本（尤其 `--reason`）不得视为安全，需独立验证其用途和输出路径；不得据此声称秘密检测穷尽。
- 旧树中的 `internal/recovery/drill_harness_test.go` 使用标准 Anvil 公开测试助记词派生账号（仅 `drill` 标签测试、公开已知测试值）；此观察不能替代对当前树的复核。
- Signer 非受支持装配之外的直调/蓄意伪造门禁不自动检测（T070 边界，见 runbook §3.7）；不声称任意 in-process 调用全覆盖。

## 5a. T068 工作树范围记录（2026-09-29；历史范围快照，非完成声明）

- 本节是对当前工作树相关实现与测试载体的范围说明，不是 T068 完成验收；此处没有声称本次运行了这些测试，也不把源码/测试存在等同于执行通过。T000-P 仍 OPEN；T017/T019/T020/T027/T057/T058/T066/T068 八项仍 OPEN。不得据此宣称完整 FR-007/INV-9 已满足。
- **Manifest 声明**：`internal/recovery/manifest.go` 定义 `signer_private_keys` 与 `real_credentials` 排除项；`internal/recovery/manifest_contract_test.go` 的契约注释及 canonical fixture 要求 `coverage.excluded` 声明两项。声明本身不证明备份内容不含秘密；`internal/recovery/secrecy_test.go` 的 `TestSecrecyManifestScanRefusesEmbeddedSecrets` 是模式化扫描的测试载体，不是任意秘密穷尽检测。
- **Restore reason 预检**：`internal/app/recoveryadmin/restore.go` 在恢复副作用前拒绝 credential-shaped reason；`internal/app/recoveryadmin/restore_reason_integration_test.go` 的 `TestRestoreCLIRejectsCredentialShapedReasonWithoutDisclosure` 检查拒绝输出、marker/evidence/tool-call 无增量及持久文本不含 canary。CLI audit/output canary 路径另由 `internal/app/recoveryadmin/backup_restore_integration_test.go` 的 `TestBackupVerifyRestoreCLIRedactCredentialShapedFreeText` 覆盖，包含操作审计检查。以上是测试断言范围，不是本次执行结果。
- **子进程凭据路径**：`internal/recovery/pgconnection.go` / `internal/recovery/backup.go` 中 `LocalPGCommand` 将密码移入私有 passfile；`internal/recovery/pgconnection_test.go` 覆盖 `0600` passfile、`0700` 私有目录、child argv 不含密码、以及常规成功/失败返回后的清理。受监督的 Linux 子进程另由 `internal/recovery/targetprocess_linux.go`、`internal/recovery/targetwriter_linux.go` 与 `internal/recovery/targetwriter_linux_unit_test.go` 的 `TestSupervisedPGRestoreProtectsArgvAndEnvironment` 覆盖，测试检查 argv/env 和正常子进程完成后的 passfile 清理。
- **Native executable / Signer 范围**：`internal/recovery/targetwriter_linux.go` 对直接 `pg_restore` ELF 可执行文件作检查；`internal/recovery/targetwriter_executable_linux_test.go` 含 `TestTargetWriterRejectsPATHScriptBeforeGuardWork`。`internal/recovery/restore.go` 的 `CheckSignerBoundary` 仅探测 Signer endpoint 可达性；`internal/recovery/secrecy_test.go` 的 `TestSecrecySignerBoundaryReachableWithoutKeyMaterial` 覆盖无私钥参数/仅可达性边界，不证明任意 Signer 调用安全。
- **明确残余/未证明项**：
  - 父级 `recovery-admin` 的 `--target-dsn` / `--broker-dsn` 参数仍可能暴露 DSN；上述 child argv 保护不消除此父进程 argv 暴露面。
  - passfile 在正常清理路径会移除，但若进程遭 SIGKILL 可能残留；同 UID 进程也不在该文件权限隔离保证之外。
  - ELF 身份/格式检查不证明二进制行为可信或正确。
  - 模式/形态匹配的 redaction 与 credential 检查不覆盖任意秘密或未知编码形式。
  - 外部部署配置及其实际权限/启动方式未经此证据证明。
- 使用真实 `LocalPGCommand` 执行成功 `pg_restore` 的正向验证 **NOT RUN**；远程 CI **NOT RUN**。不得把假子进程单测或集成负向 canary 测试解读为真实恢复成功。

## 5b. 当前 precommit 树补记（2026-09-30；非完成声明）

- **范围**：基线 HEAD `e7ba8c0` + 完整本地未提交实现（precommit tree）；不是仅 HEAD 的测试结果。最终提交将作为本档案的持久版本标识。以下均为本地执行；原始日志在 `/tmp/opencode/`，未入库。
- **全仓最终门禁**：`make lint && make build && go test -count=1 ./... && make test-contract && make test-race && git diff --check` — **通过**。
- **集成/e2e/drill**：`make test-integration` — 最终全仓通过，日志 `/tmp/opencode/015-integration-all-final-tree.log`（非持久）；`make test-e2e` — 通过，日志 `/tmp/opencode/015-e2e-final-tree.log`；`go test -tags=drill -count=1 -timeout=20m -v ./...` — exit 0，跨所有包 877 PASS / 11 顶层 SKIP / 0 FAIL，日志 `/tmp/opencode/015-drill-final-tree.log`；`make test-integration-kafka` — 通过，日志 `/tmp/opencode/015-kafka-final-tree.log`。
- **Redis 与 PG 专项**：`make test-integration-redis` 曾在相同实现、最后仅修改 `gate.go` 之前通过（redis/cache/ratelimit/testutil 范围），日志 `/tmp/opencode/015-redis-final.log`；明确复用为受影响面未变的证据，**不是** gate.go 修改后的最终重跑。`go test -tags=integration -count=1 -timeout=12m -v ./internal/recovery` 在该 gate-only 修改前 190 PASS / 9 SKIP / 0 FAIL，日志 `/tmp/opencode/015-recovery-pg-current.log`；之后最终 `make test-integration` 全仓通过，但没有 verbose skip 计数。
- **解释边界**：本机缺少直接 `pg_restore` ELF；尽管相关命令可 exit 0，真实 restore/verify/drill 多个正向场景仍是 NOT RUN/SKIP。独立 Kafka offset divergence 场景中 restore 为 NOT RUN/SKIP。绑定 worker 的 Anvil e2e 只覆盖一次真实 broadcast 及拒绝期间不重复；不证明拒绝解除后的安全继续。之前因 fixture 变化失败的运行属于已修复的历史失败，不得覆盖上述最终结果，也不代表当前仍失败。
- **T068 当前有限证据**：包括 manifest 双排除项、credential-shaped reason 拒绝、子进程 argv/passfile 保护；这些有限断言不等于完整秘密检测或 T068 验收。父 CLI argv、SIGKILL 后 passfile 残留及 ELF 信任边界仍未解决/未证明。远程 CI、push、PR、merge、deploy 均 **NOT RUN**。不宣称生产就绪或外部账本一致。

## 6. 三态与术语口径（F16）

- `restored` = 存在已接受的 `restore_probe` 证据；`verified` = 当前代次的 V1–V9 适用结论；`released` = 门禁派生放行（每次求值重算）。
- 三态**互不冒充**：`restored` 不得显示为 `verified`，`verified` 不得显示为 `released`；`approved` 仅指有效批准记录，不构成放行。
- serve 状态面只报恢复姿态与显式非声明，不缓存/不替代 `recovery-admin status` 的有界复核（F13）。

## 7. 非声明

T000-P 保持 OPEN；本目录不宣称生产就绪、不宣称外部账本一致、不承诺跨系统恰好一次；不批准任何资金数据损失额度；不授权绕过既有资金门禁；风险接受后强制复服、损失核销、人工补偿付款、自动补造意图不在本阶段交付。

## 8. `64140fb` follow-up acceptance ledger（2026-09-30）

- [Follow-up ledger](64140fb-followup-acceptance.md) holds seven findings, eight original acceptance mappings, semantic S/F results, final drill and PG evidence; it is not a production declaration. T017/T027 are accepted functionally (64/70); six follow-up tasks remain OPEN. Historical 62/70 + 11 top-level SKIP are unchanged.
- Final drill: 必需用例 19 PASS/3 FAIL/0 SKIP；顶层 913 PASS/3 FAIL/0 SKIP。Kafka offset guard 与 F2/T060 原始身份/进程证明失败，未执行破坏性验收。全量 PG 集成为 **1755 PASS/8 FAIL/2 SKIP**、exit 1；5 项隔离环境复跑 PASS 不改变全量失败结论。fix81 后三项聚焦 PG 测试 PASS（0 FAIL/0 SKIP），不代表全量 PG 通过。
- 静态/unit/contract/race 检查 exit 0；PG 全量 exit 1。受影响 unit 与 script-parser/bash-syntax 检查 PASS。汇总和安全裁剪后的事件记录见本目录。最终源指纹不等于提交 ID；提交身份以证据文件的 `git log` 或归口方最终报告为准，归口方提交前复核源指纹。远程 CI 未运行。
- T019/T020/T057/T058/T066/T068 remain OPEN. T000-P OPEN; no production readiness claim.

## 9. ADR-004 R2 有界实现轮（2026-10-04/05）

ADR-004 在 §2.1 记录 2026-10-04/05 轮次有限裁定（R1=LAUNCH 一次准入、R2=no-owner/no-privileges 别名 + 部署收敛洞察 + 修复向量、R3=分层路由头、R4=令牌衔接流、

R5=见证升级路径）；实现轮在隔离环境中跑真 fixture。ADR 本身继续 Proposed；实现继续 BLOCKED（设计方向而非部署权限）。

- **提交链（本轮，正推）**：
  [8aadb96](/docs/evidence/015/README.md) 4 个恢复 build 归档补记 →
  [27e533d] R1/R3/R5 生产代码 + 3 项 fixed-vectors 同步（含 `errAdmissionInvalidated` 等 4 项拒绝）→
  [cdf01ec] R2 部署收敛步骤 + `RecoveryConfig.DeploymentAdminDSN` + CLI 以及 drill/PG fixture 接线 →
  [d16de83] 见证重建 `RebuildTargetWithRetainedProof`（低置信度快照 bin 数据 +`attempt_proof` 审计行 + T059F2/T060 fixture 切换） →
  [f98fb13] gap 闭合哨兵优先级 + 收敛门控确认绑定恢复 + childtransport 断言 →
  [678b8bd] 后提交 native-start P 测试 `${Try}+3` 更新（R5 语义一致）。
- **drill5（根 runner，全部 296 顶层测试）**：`/tmp/r2lab/run_drill5/focus.log` + `/tmp/r2lab/pgfull2.log`；
  **295 通过/0 失败/1 个跳过（fd-probe child helper 的 wrapper 自引用，历来如此）**；包含：
  `TestBorrowedReplacementBoundPostcommitNativeStart`（P+N1..N10）、`TestBorrowedReplacementBoundPostcommitNativeReady`（`F`+`R` 两向）、
  `TestBorrowedReplacementBoundPostcommitAdmission`/`AdmissionMismatch`、`TestBorrowedReplacementControlLedgerIntegrity`（T057 类）、
  `TestT058Drill*` 方向（T058 类）以及 witness-destroy negative（`TestBorrowedReplacementTargetWitnessDestroy*`）。
- **pgfull3b（git 修复 runner，全仓库标签 `integration`，HEAD `1a6779f`）**：`/tmp/r2lab/run_pgfull3b/pg-integration.jsonl`；
  **1825 顶层：1822 PASS / 0 FAIL / 3 SKIP**，`go_test_exit=0`。runner 镜像在 pinned `postgres@sha256:4ef4dbc9…` 之上补装
  `git` 并以 `safe.directory=/workspace` 运行（一次性镜像 `txharbor-dev/run-pg-git:pg18`，仅本地构建）；此前 pgfull2 的 3 个
  `Test*MigrationHistoryUntouched` 失败与 9 个 `error obtaining VCS status` 失败全部真实复跑至 PASS（迁移历史检查以真实仓库根
  `/workspace` 与真实 git 历史执行，未被跳过或改记）。3 个 SKIP 均非必验 helper：`TestCrashHelper`（kill-point 子进程体，
  父侧 `TestPublisherCrashPointMatrix` 本轮 PASS）、`TestWithdrawalIntakeStorageDown`（T026-owned 占位）、
  `TestEpochIONonRootHelperProcess`（子进程体，wrapper 本轮 PASS）。pgfull2 的 runner-env 口径作废。
- **.host-side suite 健康状况（最终树）**：`go build ./...` 退出码 0；`make lint`/`make test`/`make test-race`/`make test-contract`/`make test-integration-kafka/redis`/`make test-e2e` 均通过 (exit 0)。
- **快速启动（quickstart）场景对照锚点**：此轮 maintenance evidence 在 `quickstart_matrix_evidence.md` §A.4 中逐场景体现。
- **非声明**：不是生产就绪声明；未进行部署；仅执行了本地测试进程。
