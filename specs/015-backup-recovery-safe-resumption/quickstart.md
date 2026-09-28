# Quickstart: 015 Backup Recovery Validation Guide (Phase 1)

**Branch**: `015-backup-recovery-safe-resumption` | **Spec**: [spec.md](spec.md) | **Design**: [plan.md](plan.md), [data-model.md](data-model.md), [contracts/](contracts/) | **Research**: [research.md](research.md)

Backend-only validation. No product code exists yet; run the named suites/commands only after implementation. 命令行为 plan 级接口面（精确 flag 由实现期 tasks 定稿），但每条都指向已存在的真实入口（[contracts/resumption-gate.md](contracts/resumption-gate.md) §1）。

**Gate**: T000-P stays OPEN — this guide is not a release and claims no production readiness; all numbers here are local test inputs. Risk-accept forced resumption / loss write-off / compensation payments / automatic intent re-creation are explicitly absent (no scenario below).

## Prerequisites

- 本地栈：PG 18.6（pinned 镜像自带 `pg_dump/pg_restore`）+ Anvil；事件能力场景加 Redis/Kafka（compose `events` profile）；Docker 可用（真实恢复/演练）。
- 恢复控制库：独立 database（`TXHARBOR_RECOVERY_CONTROL_DSN`）+ `recovery-admin migrate`（独立 schema 版本）。
- 配置（部署/测试参数，非生产阈值）：`TXHARBOR_RECOVERY_PRINCIPAL`、`TXHARBOR_RECOVERY_ARTIFACT_DIR`、`TXHARBOR_RECOVERY_GATE_TTL`、`TXHARBOR_RECOVERY_*_TOLERANCE`、目标/频率/保留（可选）。缺失必需约束时只能报告"未配置"。
- 受控数据集：在备份后继续推进外部事实（Anvil 上确认一笔付款、写入签名/广播记录、下游消费），构造"外部事实领先恢复点"。

## §1 正向入口验收（恢复 → 核验 → 拒绝不安全复服 → 分级复服）

| # | 步骤 | 命令/真实入口 | 期望 |
|---|---|---|---|
| S1 | 生成备份 | `recovery-admin backup --chain-id CHAIN --out DIR` | 产物+manifest；`verification.state=unverified`；manifest 含覆盖/恢复点/校验/版本 |
| S2 | 实际恢复验证 | `recovery-admin verify-backup --manifest M --target-dsn ISOLATED` | 真实 `pg_restore` 到隔离库；四项检查通过 → `verified`；失败 → `rejected` 且原因明确 |
| S3 | 开启恢复实例 | `recovery-admin instance-open --kind recovery --reason R`（`recovery_execute`） | 新 `instance_id`；全局唯一 open；executor 记录；所有能力默认关闭 |
| S4 | 恢复 | `recovery-admin restore --manifest M --target-dsn TARGET --instance ID` | 版本/完整性/依赖前置检查；`pg_restore`；restore_probe 通过 → `restored`（不得显示 verified/approved） |
| S5 | 隔离核验 | `recovery-admin checklist-set/verify --instance ID --item …`（`old_writers_stopped/writer_fencing_observed/network_isolation/version_compatible/no_pre_release_effects/authorization_recheck`） | 旧实例存续时相关项无法 verified → 放行被拒；停止旧实例并采集证据 + 非执行者确认 → verified |
| S6 | 事实核验 | `recovery-admin verify --instance ID --scope all`（重复有界步进） | V1–V9 项结论落库；外部领先项 `divergent/unknown`；缺口生成（对象/范围/时间线/所需证据/受影响能力） |
| S7 | 缺口阻塞 | 尝试 `release` 受影响能力 | 拒绝（`gap_open`）；证据包可导出供人工处置；超时/知悉不改变状态 |
| S8 | 独立能力放行 | `recovery-admin approve … --capability query`（single，非执行者）→ `release …` | 仅 `query` 放行；`serve` 读路径恢复；链扫描/付款/投递仍拒绝 |
| S9 | 分级推进 | 按依赖顺序 approve/release：`chain_scan` → `deposit_confirmation` → `existing_withdrawal_recovery`；`new_withdrawal_creation`、`event_publishing`、`event_consuming` 高影响档 | 每项独立条件与审计；高影响能力缺第二人批准 0 次放行；双人批准出现时仅开所列能力 |
| S10 | 状态诚实 | `recovery-admin status`；`serve` 状态面 | 逐能力显示 restored/verified/released 与阻塞原因；未核验/回退数据不显示为正常一致；健康探针不代替资金门禁 |
| S11 | 关闭实例 | 全部能力 released 后 `recovery-admin instance-close --instance ID` | 回到日常态；任一能力未放行（含缺口）→ 拒绝关闭 |
| S12 | 度量记录 | `recovery-admin drill` / 恢复记录 | 恢复点、DB 恢复时间、核验时间、各能力放行时间、backup_lag、uncovered_interval 分开记录；未配置约束标注未配置 |

正向硬断言：S8 前 `POST /withdrawals` 必须 503（`no_release` 类拒绝）；S9 前 `withdrawal-worker`/`event-publisher`/`event-consumer` 必须拒绝执行；全程 0 重复付款、0 错误事件效果、0 直写放行。

## §2 失败注入（7 类；全部 fail-closed、可观察、可重入）

| # | 注入 | 入口 | 期望 |
|---|---|---|---|
| F1 | 备份不可用/损坏/截断/未验证 | `verify-backup`/`restore` | 拒绝使用；不进入核验完成/复服；"尽力恢复"0 次 |
| F2 | 恢复中断/部分完成 | 中断 `restore` 后重试 | 不标记 restored；重建目标后幂等重跑；无双份/混合状态 |
| F3 | 版本/schema 不兼容 | 版本错配 manifest 恢复 | 明确拒绝恢复/复服；0 次静默降级/自动改写 |
| F4 | 外部事实领先恢复点 | 备份后链上付款/签名广播/下游消费后恢复 | 差异/未知清单产生；受影响能力保持关闭；0 重付/重广播/错误事件效果 |
| F5 | 旧实例未隔离/无法证明 | 保留旧 `serve`/worker/publisher/consumer 存活 | 写入/发送/投递 100% 拒绝并列出缺失证据；不得以超时/失联推断已停止 |
| F6 | 核验发现无法证明的缺口 | 缺失付款意图/nonce/inbox 记录 | 保持 unknown/pending；不当作"从未发生/可执行"；不新建意图/补写幂等记录；证据包+升级 |
| F7 | 越权/证据不足/过期批准 | 执行者自批、同人双账号、缺批准、代次过期后 release | 100% 拒绝并审计；证据变化后旧批准失效；越权 0 放行 |

复入与幂等：重复执行 restore/verify/approve/release/close ≥10 次，外部副作用与状态翻转 0 次（SC-007）；撤销放行必须显式、有授权、审计。

## §3 分层与通道（FR-033/SC-008）

| 层 | 命令/标签 | 通道 | 覆盖 |
|---|---|---|---|
| 逻辑单测（无 Docker） | `make test` / `make test-race` | 普通 PR 必跑 | manifest 校验、门禁派生评估、审批规则（双人/执行者/映射）、代次令牌、状态机纯函数 |
| 契约 | `make test-contract` | 普通 PR 必跑（无 Docker） | 备份/放行/审批/核验契约；非法状态与越权拒绝形状 |
| PG 集成 | `make test-integration`（`integration` tag） | 普通 PR 路径分类（`internal/recovery/**` 等命中） | 控制库事务/并发/幂等；小规模真实 `pg_dump→pg_restore` 隔离恢复；门禁时序；Docker 缺位记 NOT RUN，不得记 pass |
| 完整灾备演练 | `make test-drill`（`drill` tag；新目标） | **独立通道**（schedule/dispatch，不阻塞普通 PR） | S1–S12 全流程 + F1–F7 + 真实 Anvil/PG/（事件层）Kafka；长测 |
| 事件层集成 | `integration_redis` / `integration_kafka` | 既有 `ci.yml` 分类 | 回退检测/幂等吸收的中间件层用例 |

- `drill` 通道可独立并行，永不进入普通 PR；演练产物（日志/度量/证据引用）按 `docs/evidence/015/` 或 `.evidence/` 存档。

## §4 反作弊纪律（违反即无效证据）

- 禁止直写控制库的批准/放行/缺口/隔离状态绕过命令；放行必须由门禁派生评估得出（INV-2）。
- 禁止注入核验替身使门禁放行；门禁/核验的验收证据必须来自真实控制库与真实恢复/链/中间件；替身只可用于纯逻辑单测。
- 禁止测试专用"关门禁"开关或环境变量绕过。
- 真实恢复场景必须执行真实 `pg_dump/pg_restore`；链上事实必须来自真实链（Anvil/本地 RPC）；声明 Kafka 证据必须来自真实 broker。
- 不得以"数据库可连接"宣称 RTO 达标；不得宣称外部账本一致/跨系统恰好一次（超出本项目证据范围）。
- 不得用本地演练数值冒充生产阈值；未配置必需约束时不得宣称符合生产恢复目标。

## §5 配置键（plan 级；精确命名实现期定稿）与非声明

`TXHARBOR_RECOVERY_CONTROL_DSN`、`TXHARBOR_RECOVERY_PRINCIPAL`、`TXHARBOR_RECOVERY_ARTIFACT_DIR`、`TXHARBOR_RECOVERY_GATE_TTL`、`TXHARBOR_RECOVERY_EVIDENCE_FRESHNESS_*`、`TXHARBOR_RECOVERY_RPO_TARGET`、`TXHARBOR_RECOVERY_RTO_TARGET`、`TXHARBOR_RECOVERY_BACKUP_FREQUENCY`、`TXHARBOR_RECOVERY_RETENTION`（可选项；缺失 → 未配置状态）。

非声明：T000-P OPEN；不宣称生产就绪；生产 RPO/RTO/频率/保留数值留部署前裁决；不批准任何资金数据损失额度；不授权绕过既有资金门禁；不承诺跨系统恰好一次；未接入真实上游回执不宣称外部账本一致。
