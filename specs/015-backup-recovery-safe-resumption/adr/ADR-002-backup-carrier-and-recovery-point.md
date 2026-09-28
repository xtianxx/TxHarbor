# ADR-002: Backup Carrier and Recovery Point

**Date**: 2026-09-28 | **Spec**: [spec.md](../spec.md) (FR-001–FR-008, FR-030/031/036) | **Status**: Accepted (plan scope) | **Related**: [contracts/backup-manifest.md](../contracts/backup-manifest.md), [research.md](../research.md) §2, [data-model.md](../data-model.md) §2/§7

## Context

仓库无备份服务/产品/Dockerfile；compose 基线仅 PG（`compose.yaml:19 postgres:18.6-trixie`）+ 具名卷 `pgdata`，本地仅另加 Anvil（events profile 加 Redis/Kafka）。spec 明示不指定备份产品/存储厂商/PITR 实现，留 plan。恢复目标必须"实际恢复验证"，且**不得以业务表最大时间戳或备份频率充当 RPO 证明**。生产保留/频率数值不在本阶段裁决。

## Decision

1. **载体 = 逻辑备份** `pg_dump --format=custom`（pinned `postgres:18.6-trixie` 镜像自带客户端，不新增依赖），产物 + 外部 manifest（[contracts/backup-manifest.md](../contracts/backup-manifest.md)）构成备份身份；选择只按 manifest。
2. **一致性快照**：`REPEATABLE READ` 事务中 `SELECT pg_current_wal_lsn(), pg_current_snapshot(), pg_export_snapshot(), now()`，以 `--snapshot` 导出；恢复点 = 快照元组（xmin/xip/wall clock/LSN 上界），非业务表时间戳。
3. **完整性/兼容性**：SHA-256 + `pg_restore -l` 可读；manifest 记录 `goose_db_version` 精确集与程序版本；复用 `internal/db` `Inspect/CheckCompatibility` 只读语义，拒绝不兼容。
4. **恢复验证**：`verify-backup` 在隔离目标真实 `pg_restore` + 结构/约束/兼容/可用性/核验可执行检查；未验证备份不得用于恢复/复服。
5. **调度/保留**：薄命令 + 外部调度（operator/cron），无守护进程；保留策略部署配置，**不编造生产数值**；未配置必需约束不得宣称生产恢复目标。

## Rationale

- 逻辑转储不依赖宿主存储实现，仓库内可确定性演练（testcontainers/pinned image）；manifest 化使身份/完整性/选择规则可测。
- 快照 LSN 口径把"恢复点"从模糊的"备份时间"变成可复核的一致性位置，直接满足 FR-001/FR-036 的禁用口径。
- 物理/PITR 需要 WAL 归档存储与编排，当前单机 posture 无此依赖，spec 亦不为本阶段引入。

## Consequences

- RPO 粒度 = 备份频率（部署决策），不是 PITR 级；本设计不宣称秒级/连续恢复能力。
- 大库 dump 时间/体积属部署关注点，保留策略与存储位置走配置；本地演练数值标注测试输入。
- 若未来需要 PITR/物理载体：manifest 身份模型与恢复点字段可扩展 `carrier`，控制面/门禁/核验模型不变（替换 ADR 即可）。

## Alternatives

- `pg_basebackup` + WAL 归档/PITR：deferred（需归档存储与编排、同主版本约束；非本阶段验收所需）。
- 卷/文件系统快照：rejected（依赖宿主存储、跨环境可移植差、无法仓库内确定性演练）。
- 外部托管备份产品：rejected（spec 不指定厂商、不引入仓库外信任依赖）。
- 仅 `pg_dump` 无 manifest / 按文件名挑选：rejected（FR-001 明确禁止文件名/人工记忆选择，完整性不可复核）。
