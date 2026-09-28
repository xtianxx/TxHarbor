# Contract: Resumption Gate — Capabilities, Real Entry Points, Isolation (015)

**Spec**: [spec.md](../spec.md) (FR-009–FR-013, FR-021–FR-026, FR-032/033) | **Design**: [data-model.md](../data-model.md) §3/§4/§7 | **ADR-001**: [adr/ADR-001-recovery-control-store.md](../adr/ADR-001-recovery-control-store.md)

恢复环境默认隔离；能力逐项放行；**不存在"一个开关恢复全部"**。本契约为 7 类能力的真实接线与放行判定；不含实现，但每条都指向已存在的真实入口。

## 1. 能力 × 真实入口 × 放行检查点（接线清单）

| # | 能力 | 真实入口（进程/命令/挂载） | 依赖能力 | 恢复环境默认 | 放行检查点（动作前） |
|---|---|---|---|---|---|
| 1 | `query` | `txharbor serve` 读路径：`internal/app/withdrawalhttp.go:227 ServeGET` + `serve.go:473-474`；执行读 `serve.go:479-480`；nonce 读 `serve.go:488`；状态面 `serve.go` `/status/degradation` | — | 关闭（拒绝） | HTTP 处理器准入前 |
| 2 | `chain_scan` | `txharbor serve` indexer header/log scanner（`serve.go:319/331`，`:615 runServiceStreams`）；单主租约+fencing `internal/indexer/lease.go:56-67` | — | 关闭 | 扫描循环启动检查＋包装步进回调（注入 `gateCheck`，见 §1.1/F12） |
| 3 | `deposit_confirmation` | `txharbor serve` deposit scanner `serve.go:350-353`、confirmation scanner+committer `serve.go:380-398`、006 recovery loop `serve.go:411` | `chain_scan` | 关闭 | 同上（循环启动＋步进回调） |
| 4 | `existing_withdrawal_recovery` | `txharbor withdrawal-worker`（`internal/app/withdrawalworker.go:318/349/566`）；`txharbor withdrawal-exec` 写路径（`internal/app/withdrawalexec.go:35` + `execOperatorOp`）；**execution HTTP 写路径**（`internal/app/withdrawalexecution.go`，`POST /withdrawals/{request_id}/execution`，`serve.go:479`，直调 `execution.Admit`——与 CLI `execOperatorOp` 非同漏斗）；其下游签名交付 = `txharbor signer-serve`（`internal/app/signerserve.go:295 signer.Deliver` → 交付核心 `internal/signer/delivery.go:170 Deliver`/`:194 deliverGated`；`internal/signer/gates.go:123/162` 为 009 内部 gate 行、非 015 交付接线点） | `chain_scan` | 关闭 | worker 认领/推进前；exec 每个操作前；execution HTTP 准入前；signer 交付前（受支持装配；进程内检查点见 T070） |
| 5 | `new_withdrawal_creation` | `txharbor serve` `POST /withdrawals`（`internal/app/withdrawalhttp.go:148`；`guardRoute`+`CapacityGate`） | `existing_withdrawal_recovery` | 关闭 | 处理器准入前（在既有 guardRoute/capacity 之前拒绝） |
| 6 | `event_publishing` | `txharbor event-publisher`（`internal/app/eventpublisher.go:41` → `internal/events/publisher.go:177`，`:161-162 FOR UPDATE SKIP LOCKED`+lease） | `chain_scan` | 关闭 | claim 批次前与 settle 前 |
| 7 | `event_consuming` | `txharbor event-consumer`（`internal/app/eventconsumer.go:31` → `internal/events/consumer.go:351`；Effect=缓存失效 `internal/cache/invalidator.go:120`；参考账本 `refconsumer.go` 仅证据） | — | 关闭 | Effect 应用前与进度推进前 |

- 依赖集为保守固有依赖（[data-model.md](../data-model.md) §3.2）；`new_withdrawal_creation → existing_withdrawal_recovery` 防止"只开入口不备处置"。
- 恢复工具自身：`recovery-admin backup/verify-backup/restore/verify` **禁止**发布事件、签名、广播、调用业务 RPC 或产生真实下游投递；`restore` 只写显式目标 DSN（默认隔离目标）。
- 隔离与解除覆盖：提款创建与执行、签名、广播、事件发布与消费、下游投递、定时调度与自动任务、恢复/核验工具外部写（FR-012）。

### 1.1 入口 → 能力 → 检查点 → 原门禁 → 正反例（映射表，本轮 analyze 增补）

| 能力 | 检查点（动作前） | 保留的原门禁（不得被替代） | 反例（拒绝 + `refusal_class` 审计） | 正例（放行后仍走原门禁） |
|---|---|---|---|---|
| `query` | HTTP 准入前（读路径） | 认证/降级/限流（`guardRoute`、ratelimit ClassQuery） | 无 release/依赖未放行/控制库不可达→拒绝 | 读路径可用且状态面只报 restored/verified/released |
| `chain_scan` | 循环启动检查＋包装步进回调（注入 `gateCheck`，T030/F12） | `indexer_pause`、lease/fencing（`internal/indexer/lease.go:56-67`）、启动版本门禁 | 旧实例未隔离→循环不启动/每步拒绝；回调缺失→fail-closed | 扫描恢复且 lease/fencing 仍独立生效 |
| `deposit_confirmation` | 同上（scanner/committer） | 确认策略、006 recovery loop 原语义 | `chain_scan` 未放行→拒绝 | 充值确认恢复且 006 行仍独立生效 |
| `existing_withdrawal_recovery` | worker 认领/推进前；exec 操作前；execution HTTP 准入前；signer 交付前（受支持装配＋T070） | nonce hold、发送/执行门禁、authorization/T-deliver、`can_execute`、unknown 纪律 | 自批/缺批准/缺口/未隔离→拒绝；拒绝不得推进进度或吞掉重试 | 恢复执行且 008–011 原门禁独立通过；历史签名/广播不重放 |
| `new_withdrawal_creation` | POST 处理器准入前（先于 `guardRoute`/CapacityGate 判定） | `guardRoute`、CapacityGate、007 授权、限流次序 | `existing_withdrawal_recovery` 未放行（固有依赖）→拒绝 | 创建恢复且 007 门禁仍独立生效 |
| `event_publishing` | claim 批次前与 settle 前 | owner/lease 分片、outbox 幂等、义务标记 | 拒绝时不 claim/不 settle/不推进 outbox | 发布恢复且 at-least-once 语义不变 |
| `event_consuming` | Effect 应用前与进度推进前 | inbox/version 守卫、quarantine、offset 进度 | 拒绝时不应用 Effect、不推进 offset/inbox | 消费恢复；effect class 超范围→dual 档校验（T050） |

- 证据要求：拒绝必须暴露闭集 `refusal_class` 并审计；放行不得解锁任何既有资金门禁（FR-025）；**两阶段判权**——本门禁 allow **AND** 动作处原门禁独立评估，禁止替代/合并/短路（F20）。
- 门禁调用不得就地复制判定逻辑；`chain_scan`/`deposit_confirmation` 的步进门禁以回调/函数值注入装配在 `internal/app`（F12）。

### 1.2 程序边界（无运行时门禁接线；不得声称运行时强制，F3）

`reconcile-admin`（`claim`/`dispose`/`reverify`/`scan`/`start`/`resume` 等写路径）、`events-admin`（`replay`/`unblock`/`retention-prune`）、`withdrawal-exec` 操作员 CLI 与外部定时调度**不在 015 运行时门禁接线内**：

- 可执行隔离手段：停机/下线、访问与凭证移除、部署配置移除调度（记录时间与主体）。
- 验证证据：停服/权限移除记录＋门禁审计（实例 open 后 0 次外部可见动作）＋`no_pre_release_effects` 检查项；**仅 checklist 签署不构成运行时隔离证明**。
- 失败行为：无法证明已停止/已失去权限→相关能力保持关闭；不因超时、失联或人工知悉放行；若未来要求运行时强制，须补 ADR/设计（不得以文档措辞冒充）。
- signer 交付边界（F1）：受支持的 `signer-serve` 装配受门禁＋T070 进程内检查点覆盖；受支持装配之外直调 `internal/signer` 核心/蓄意伪造门禁不自动检测（DG-1 同类）；**不得声称任意 in-process 调用全覆盖**。

## 2. 放行判定（唯一权威；禁止旁路）

- 判定公式与失败分类见 [data-model.md](../data-model.md) §3；求值**只读恢复控制库**；数据 DB 中的授权/批准行不作为放行依据。
- **无 open 恢复实例时按正常态放行（不改变日常运行，FR-023）**；一旦存在 open 实例，接线入口默认拒绝直至放行条件满足。
- 运行期：有界 TTL 缓存（部署配置；本地值仅测试输入）；**代次感知失效（F5）**：任何代次/哈希变化立即使相关缓存失效（发现者=门禁求值器，下一次真实动作前求值即拒绝，不得等 TTL）；缓存过期且控制库不可达 → 拒绝（fail-closed）；区分「未准入」（拒绝、不产生动作）与「已在途」（按原门禁处理、未知结果按 unknown 纪律，不追溯中止已提交工作）。拒绝理由以闭集 `refusal_class` 暴露并审计。
- **单动作准入协议（R3）**：①缓存命中与未命中均须在**本次动作准入前**读取控制库权威 `(state, evidence_generation, evidence_hash)`，并在共同锁（实例行锁）内校验代次协议——缓存键含代次/哈希**不构成最新性证明**；②缓存只复用仍有效的计算结果，**不得复用旧 allow 跳过本次授权检查**，控制库不可达即拒绝；③顺序=判定点（锁内）→释锁点→实际动作：一次准入只覆盖当次调用的**单个明确动作**，不得跨请求/循环步进/批次/异步重试复用；④撤销先于准入→拒绝；准入后撤销→按在途＋unknown 处理（不追溯中止已提交工作、不回滚）并阻止后续准入；**缓存命中不得被追认为在途**；不宣称跨系统原子。
- **两阶段判权（F20）**：本门禁（recovery allow）AND 动作处既有原门禁独立评估；`release_valid` 公式中的 `existing_fund_gates` 是对第二阶段的引用，禁止替代/合并/短路。
- 进程重启读取同一控制库 → **不自动解除隔离**（FR-009）；放行状态不驻留进程内存。
- 状态暴露：`recovery-admin status` 与 serve 状态面逐能力显示 `restored/verified/released` 三态与阻塞原因；`restored` 不得显示为 `verified`，`verified` 不得显示为 `released`（术语 F16：`released`=派生放行；`approved` 仅指有效批准记录，不构成放行）；健康探针不得替代资金门禁判权（FR-026）。

## 3. Isolation Checklist（000018 模式的恢复版；FR-010/011）

放行任一能力前，其 `isolation_dependency_set` 全部 `verified`。每项必须 `evidenced`（执行者采集证据）+ `verified`（**非本实例 executor** 的参与者确认）；**状态记录不能单独作为证明**（租约到期/进程表快照/口头确认均不够）。

| item_key | 要求 | 可接受证据（示例） |
|---|---|---|
| `old_writers_stopped` | 旧 `serve`/`withdrawal-worker`/`event-publisher`/`event-consumer`/`signer-serve`、**`reconcile-admin`/`events-admin` 写路径与外部定时调度** 及 CLI 调用者停止 | 服务管理器状态、进程/主机下线、访问移除记录（带时间与主体）；程序边界证据要求见 §1.2 |
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
- 有界只读复核（F13）：可读范围+次数/时间/资源预算（部署配置）；耗尽→拒绝后续复核＋审计、不改变缺口/实例/批准/放行状态；超时/耗尽/人工知悉≠缺口闭合或获准复服。
- 控制库自身回退纪律（F6，详见 [adr/ADR-001-recovery-control-store.md](../adr/ADR-001-recovery-control-store.md) 与 tasks T025/T064）：禁盲恢复；仅经停机隔离＋显式重建/supersede＋审计；不提供对盲恢复/旧副本的自动检测，「旧批准不自动重生效」仅就数据 DB 回滚域成立。
- 复服不得解锁任何既有资金门禁（`indexer_pause`/日志/充值暂停/006 恢复行/nonce hold/授权范围暂停/容量红线）；既有门禁解除仍走其原有路径（FR-025）。
