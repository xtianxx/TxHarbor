# Contract: Resumption Gate — Capabilities, Real Entry Points, Isolation (015)

**Spec**: [spec.md](../spec.md) (FR-009–FR-013, FR-021–FR-026, FR-032/033) | **Design**: [data-model.md](../data-model.md) §3/§4/§7 | **ADR-001**: [adr/ADR-001-recovery-control-store.md](../adr/ADR-001-recovery-control-store.md)

恢复环境默认隔离；能力逐项放行；**不存在"一个开关恢复全部"**。本契约为 7 类能力的真实接线与放行判定；不含实现，但每条都指向已存在的真实入口。

## 1. 能力 × 真实入口 × 放行检查点（接线清单）

| # | 能力 | 真实入口（进程/命令/挂载） | 依赖能力 | 恢复环境默认 | 放行检查点（动作前） |
|---|---|---|---|---|---|
| 1 | `query` | `txharbor serve` 读路径：`internal/app/withdrawalhttp.go:227 ServeGET` + `serve.go:473-474`；执行读 `serve.go:479-480`；nonce 读 `serve.go:488`；状态面 `serve.go` `/status/degradation` | — | 关闭（拒绝） | HTTP 处理器准入前 |
| 2 | `chain_scan` | `txharbor serve` indexer header/log scanner（`serve.go:319/331`，`:615 runServiceStreams`）；单主租约+fencing `internal/indexer/lease.go:56-67` | — | 关闭 | 扫描循环启动与每轮步进前 |
| 3 | `deposit_confirmation` | `txharbor serve` deposit scanner `serve.go:350-353`、confirmation scanner+committer `serve.go:380-398`、006 recovery loop `serve.go:411` | `chain_scan` | 关闭 | 上述循环启动与每轮步进前 |
| 4 | `existing_withdrawal_recovery` | `txharbor withdrawal-worker`（`internal/app/withdrawalworker.go:318/349/566`）；`txharbor withdrawal-exec` 写路径（`internal/app/withdrawalexec.go:35` + `execOperatorOp`）；其下游签名交付 = `txharbor signer-serve`（009 gates `internal/signer/gates.go:123/162`） | `chain_scan` | 关闭 | worker 认领/推进前；exec 每个操作前；signer 交付前（按依赖能力判定） |
| 5 | `new_withdrawal_creation` | `txharbor serve` `POST /withdrawals`（`internal/app/withdrawalhttp.go:148`；`guardRoute`+`CapacityGate`） | `existing_withdrawal_recovery` | 关闭 | 处理器准入前（在既有 guardRoute/capacity 之前拒绝） |
| 6 | `event_publishing` | `txharbor event-publisher`（`internal/app/eventpublisher.go:41` → `internal/events/publisher.go:177`，`:161-162 FOR UPDATE SKIP LOCKED`+lease） | `chain_scan` | 关闭 | claim 批次前与 settle 前 |
| 7 | `event_consuming` | `txharbor event-consumer`（`internal/app/eventconsumer.go:31` → `internal/events/consumer.go:351`；Effect=缓存失效 `internal/cache/invalidator.go:120`；参考账本 `refconsumer.go` 仅证据） | — | 关闭 | Effect 应用前与进度推进前 |

- 依赖集为保守固有依赖（[data-model.md](../data-model.md) §3.2）；`new_withdrawal_creation → existing_withdrawal_recovery` 防止"只开入口不备处置"。
- 恢复工具自身：`recovery-admin backup/verify-backup/restore/verify` **禁止**发布事件、签名、广播、调用业务 RPC 或产生真实下游投递；`restore` 只写显式目标 DSN（默认隔离目标）。
- 隔离与解除覆盖：提款创建与执行、签名、广播、事件发布与消费、下游投递、定时调度与自动任务、恢复/核验工具外部写（FR-012）。

## 2. 放行判定（唯一权威；禁止旁路）

- 判定公式与失败分类见 [data-model.md](../data-model.md) §3；求值**只读恢复控制库**；数据 DB 中的授权/批准行不作为放行依据。
- **无 open 恢复实例时按正常态放行（不改变日常运行，FR-023）**；一旦存在 open 实例，接线入口默认拒绝直至放行条件满足。
- 运行期：有界 TTL 缓存（部署配置；本地值仅测试输入）；缓存过期且控制库不可达 → 拒绝（fail-closed）。拒绝理由以闭集 `refusal_class` 暴露并审计。
- 进程重启读取同一控制库 → **不自动解除隔离**（FR-009）；放行状态不驻留进程内存。
- 状态暴露：`recovery-admin status` 与 serve 状态面逐能力显示 `restored/verified/released` 三态与阻塞原因；`restored` 不得显示为 `verified`，`verified` 不得显示为 `approved`；健康探针不得替代资金门禁判权（FR-026）。

## 3. Isolation Checklist（000018 模式的恢复版；FR-010/011）

放行任一能力前，其 `isolation_dependency_set` 全部 `verified`。每项必须 `evidenced`（执行者采集证据）+ `verified`（**非本实例 executor** 的参与者确认）；**状态记录不能单独作为证明**（租约到期/进程表快照/口头确认均不够）。

| item_key | 要求 | 可接受证据（示例） |
|---|---|---|
| `old_writers_stopped` | 旧 `serve`/`withdrawal-worker`/`event-publisher`/`event-consumer`/`signer-serve` 及外部调度器/CLI 调用者停止 | 服务管理器状态、进程/主机下线、访问移除记录（带时间与主体） |
| `writer_fencing_observed` | 新环境取得写权且旧写者无法再提交 | 新租约 takeover（`fencing_token` 递增）、publisher owner 变更且无旧 owner settle、执行 claim 归属证据 |
| `network_isolation` | 旧环境不可达 PG/broker/RPC（或已下线） | 网络控制/主机清单证据 |
| `version_compatible` | 全部在途二进制版本受控一致；schema 兼容 | `CheckCompatibility` 通过、版本核对记录（000018 检查清单模式） |
| `no_pre_release_effects` | 实例 open 后 0 次外部可见动作 | 门禁审计无 `ok` 的外部动作、outbox/consumer 未推进证据 |
| `authorization_recheck` | 回退点后的授权面撤销/发放已再核验/再施加（V8） | 撤销记录/外部真源证据、再施加结果与时间 |

## 4. 失效、在途与副作用边界

- 证据变化（新核验写入/缺口变化/隔离项 rejected）→ 代次推进 → 相关批准/放行失效；门禁在下一次求值拒绝；**停止后续执行**。
- 在途动作：不追溯中止已提交工作；按既有安全门禁处理（nonce hold、发送/执行门禁、claim/lease、inbox/version 守卫、reorg 版本捕获）；**未知结果永不视为未执行**，不得自动重付/重广播/重投递。
- **数据库回退不等于外部副作用回退**：已签名/已广播/已发布/已消费的效果保持外部事实；恢复工具与门禁不得假设外部效果随 DB 事务回滚。
- 撤销/回退某项复服必须显式、有授权并审计；重复 release/revoke/批准按 `operation_id` 幂等，零重复副作用（FR-024）。
- 复服不得解锁任何既有资金门禁（`indexer_pause`/日志/充值暂停/006 恢复行/nonce hold/授权范围暂停/容量红线）；既有门禁解除仍走其原有路径（FR-025）。
