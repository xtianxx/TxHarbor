# 015 证据索引与纪律（T065）

- Feature: `015-backup-recovery-safe-resumption` ｜ 分支: `015-backup-recovery-safe-resumption` ｜ main 基线: `8d4b9af`
- 设计: [spec.md](../../../specs/015-backup-recovery-safe-resumption/spec.md) · [plan.md](../../../specs/015-backup-recovery-safe-resumption/plan.md) · [quickstart.md](../../../specs/015-backup-recovery-safe-resumption/quickstart.md) · [contracts/](../../../specs/015-backup-recovery-safe-resumption/contracts/) · [docs/ops/recovery-runbook.md](../../ops/recovery-runbook.md)
- **Gate: T000-P 保持 OPEN**。本目录不是发布，不宣称生产就绪；所有本地数值仅为测试输入，生产 RPO/RTO/频率/保留未裁决（FR-036）。

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

## 5. T068 私钥/凭据边界全量复核结论

- 复核范围：`internal/recovery/**` + `internal/app/recoveryadmin/**`（生产代码，不含测试）；方法：import 边界逐项核对 + 全量模式扫描（`private_key`/`PRIVATE KEY`/`mnemonic`/`keystore`/`crypto/ecdsa`/wallet/DSN 形值/`password`/`token` 在日志、审计、证据、CLI 输出与文件写入中的出现）+ 既有 `secrecy` 单测载体 + 关键路径抽样（manifest 校验、DSN 解析/指纹、审计写入、日志与 CLI 输出、Signer 边界）。
- 结论：**未发现违反 FR-007/INV-9 的路径**。具体边界：
  1. **签名密钥只在 Signer 边界**：恢复核心不 import 任何钱包/密钥库（无 `crypto/ecdsa`、keystore、mnemonic 相关引用）；恢复环境不为恢复获取可直接持有的私钥，`CheckSignerBoundary` 只探测可达性并回传脱敏后的 endpoint（`internal/recovery/restore.go`）。
  2. **DSN 明文不入日志/审计/证据**：DSN 不进入日志/审计/证据（错误路径统一经 `logx.Redact` 后再输出）；`RedactedDSN` 是唯一允许的 DSN 呈现形态（复用 `logx.Redact`，保留结构、抹除凭据）；`data_target` 只存 database/role/target 指纹，`ValidateDataTarget` 对任意深度拒绝 DSN 样值与凭据键（`internal/recovery/controlstore/store.go`）；`data_target`/审计不使用明文 DSN。
  3. **备份/证据/日志禁含私钥与真实凭据**：manifest 覆盖声明必须包含 `signer_private_keys` 排除项（canonical 产物同时声明 `real_credentials`，见残余边界）；`ScanManifestSecrets` 拒绝自由文本中嵌入的 DSN/凭据值/PEM 私钥材料（声明「排除私钥」本身不是密钥材料，不得拒绝）；`pg_dump`/`pg_restore` stderr 在进入错误信息前经 `Redact`。
  4. **复核载体**：`internal/recovery/secrecy_test.go`（5 个单测：Redact canary、RedactedDSN、manifest 秘密扫描、Signer 边界、审计载荷拒 DSN/凭据），随普通 unit 层（`make test`）执行。
- 残余边界（不构成本阶段缺陷，需随证据携带）：
  - `coverage.excluded` 校验硬性要求 `signer_private_keys`；canonical 产物同时声明 `real_credentials`，但外部手改 manifest 省略 `real_credentials` 时校验层不单独拒绝（执行面窄于契约 §1 所列两族排除声明）——非泄漏路径：嵌入凭据仍被 `ScanManifestSecrets`/`ValidateDataTarget` 拒绝；是否加严属实现期收口项。
  - `pg_dump`/`pg_restore` 子进程以 `--dbname=<DSN>` 接收连接串：命令存活期内 DSN 出现在本机进程参数表（不属于备份集/日志/审计/证据，命令结束即消失；本地特权 CLI 前台有界执行、无常驻服务持有）；如需进一步收敛属部署加固（如 PGPASSWORD/PGSERVICEFILE 形态），不影响 FR-007/INV-9 的日志/审计/证据/备份边界结论。
  - `logx.Redact` 是模式化脱敏（URI userinfo、`password/token/private_key` 等键值、Bearer）；未匹配这些模式的新型凭据串可能不被遮盖——缓解：DSN 值不直接进入日志/审计（只经 `RedactedDSN` 与解析后的指纹），manifest 自由文本与 `data_target` 另有结构性拒绝。
  - `internal/recovery/drill_harness_test.go` 使用标准 Anvil 公开测试助记词派生账号（仅 `drill` 标签测试、公开已知测试值，非真实凭据）；生产代码不携带任何测试密钥。
  - Signer 非受支持装配之外的直调/蓄意伪造门禁不自动检测（T070 边界，见 runbook §3.7）；不声称任意 in-process 调用全覆盖。

## 6. 三态与术语口径（F16）

- `restored` = 存在已接受的 `restore_probe` 证据；`verified` = 当前代次的 V1–V9 适用结论；`released` = 门禁派生放行（每次求值重算）。
- 三态**互不冒充**：`restored` 不得显示为 `verified`，`verified` 不得显示为 `released`；`approved` 仅指有效批准记录，不构成放行。
- serve 状态面只报恢复姿态与显式非声明，不缓存/不替代 `recovery-admin status` 的有界复核（F13）。

## 7. 非声明

T000-P 保持 OPEN；本目录不宣称生产就绪、不宣称外部账本一致、不承诺跨系统恰好一次；不批准任何资金数据损失额度；不授权绕过既有资金门禁；风险接受后强制复服、损失核销、人工补偿付款、自动补造意图不在本阶段交付。
