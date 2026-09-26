# Research: 014 Reconciliation and Exception Handling (Phase 0)

**Branch**: `014-reconciliation-exception-handling` | **Date**: 2026-09-26 | **Spec**: [spec.md](spec.md)

All NEEDS CLARIFICATION from spec are resolved (Q1–Q5, 2026-09-26). No new technical unknowns remain that block design; test-budget numbers are intentionally left as待测参数 (see §7).

## 1. 既有能力复用（逐项核对真实入口；结论：全部只读复用，不自动触发）

- 链上观察：`indexer/scanner.go:105 NewScanner` / `:284 ServeLoop` / `:649 commitBlock`；`logscanner.go:131/314/681/880`；`reorg.go:106/166/231/402`；serve wiring `app/serve.go:319,334,398,411`；只读复用 `app/serve.go:835 indexerRPC` 与 `reorgmetrics.go:60 LoadRecoverySnapshot`。限制：lease+chainID 绑定。
- 确认与重组：`indexer/confirm.go:58/71/88`；`confirmscan.go:111/427`；`confirmcommit.go:135`（drift 拒绝/收敛）；`confirmauth.go:117` + CLI `app/confirmauth.go:36`（request_id 幂等）；`reorgcommit.go:301/422/477/531/674/852/1150/1213/1418/1537/1575`（版本捕获+门禁+锁+事件序）。014 只读观察 `readRecoveryRow/RecoverySnapshot`，不得自动触发（FR-023）。
- 未知交易结果：`txlifecycle/reconcile.go:62 Reconcile`（probe→classification→INSERT `tx_reconciliations`→revision 乐观→receipt 唯一键 `migrations/000011:219`）；`:183 UnknownRecovery`（只读 facts + `recoveryConditions:283`，014 未知证据直接复用入口）；`gates.go:54 runGates`（claim FOR SHARE→lease FOR UPDATE→LOCK TABLE pause/recovery→快照落 `tx_send_attempts`）。
- 执行门禁：`execution/gates.go:85/161/202/240/307/345`（被 `admit.go:101-118`/`advance.go:136-164` 真实调用）；`claim.go:236/243/391`（CAS/takeover/乐观 revoke+审计；行锁 :43/:217/:267）。
- Outbox/消费者：`events/outbox.go:23/37/129`；`publisher.go:177/224/325/423-498/613`（SKIP LOCKED 多实例分片，无 Redis）；`consumer.go:351/444/572(T4顺序)/553/708/762/887/914/1155-1198`；`quarantine.go:92/131/332-346/391/457/794`（open 唯一索引、Replay 强制 consumer 匹配+reason、Unblock operation_id 幂等）；`audit.go:133/158/200`（watermark 比对→`txharbor_outbox_audit_gaps_total`）；CLI `app/eventpublisher.go:41`、`app/eventconsumer.go`、`app/eventsadmin.go:49`（replay 仅绑定 `RefConsumerName :452-455`）。
- 人工恢复：`app/withdrawalexec.go:35`（唯一可写 CLI；operation_id 审计幂等 `:83/:176`）；`events-admin replay/unblock`（reason+operator 必填）；`app/withdrawalworker.go:649/566/696`；006 `AuthorizeRecoveryRepair/Release:1537/1575`。
- 授权：009 `signer/gates.go:123/162`、`policy.go:155`、CLI `app/signerauth.go:35`、HTTP `app/signerserve.go:172`；011 `app/apikeyauth.go:46`（key 存哈希）、`execution/claim.go`；012 `withdrawal/grant.go:1114/1122/1169/1182/312/328/448` + allowlist `:42/:68` + CLI `app/withdrawalauthz.go:69`。
- 指标：`metrics/events.go:10-23/266-317` + publisher `:522 RefreshGauges`；reorg `reorgmetrics.go:60` + serve `:566-567/:1054-1105`；`metrics/metrics.go:374/835-838` frontier lag；`execution/projection.go:22-30/113-119` freshness/staleness。
- Decision: 以上全部作为只读原语复用；014 新增仅任务编排/完整性/身份去重/生命周期四层。Rationale: 避免重复建设单点探针，且满足 Q1 不自动触发。Alternatives considered: 重写扫描器/自建水位——rejected（违反 XIII 简单优先，且 checkpoint/audit/progress 已有载体）。

## 2. 对账证据与覆盖（权威来源与比较规则）

- 链上事实：canonical block/log/receipt + `recovery_version` + confirm basis（`confirm_tip_*`, policy_seq）；孤块证据只能转待复核（FR-017）。
- PG 业务状态：007 request/grant（`request_id`/`caller+idempotency_key` 唯一）、010 `tx_attempts`/`tx_send_attempts`/`tx_reconciliations`/`tx_receipts`、011 intent/claim/step/projection（`owner/lease_version/state_version/freshness`）；未知一律保留未知（FR-018）。
- 事件侧：`outbox_events`（`source_kind/id/version` 唯一）+ `consumer_inbox`/`consumer_versions`/`consumer_progress`/`consumer_quarantine` + `event_ops_audit(operation_id 唯一)`；Q4 仅业务分歧建单。
- 范围身份：chain_id + 高度/时间区间 + 业务类型 + policy/cutover 版本；区块身份（number/hash）+ 业务版本（recovery/authorization/scope/state_version）+ 证据时点共同构成闭合有效域（Q5）。
- 新鲜度/完整性：三 checkpoint（000002/000003/000004）+ frontier lag + outbox watermark (`audit.go:148-180`) + consumer 双口径 lag；未闭合不判异常（FR-004），过期降级（FR-005），上游未接入只判未验证（FR-006），缺证据≠一致≠异常。
- 并发/保留/裁剪/失败：并发写入经版本比对触发失效重验证（Q5）；保留裁剪导致证据缺失按覆盖不完整处理；查询失败按未知/待核验，不记成功失败。

## 3. 身份、模型与状态机选项

- Decision: 稳定身份 =（范围， 差异类别， 业务主键， 内容哈希， 证据版本域）组合键；同一身份追加证据/重开原单，不同身份建关联单。Rationale: 满足 FR-007/SC-002 且避免重组重扫无限建单。Alternatives: 纯内容哈希——rejected（跨范围碰撞）；纯自增单号——rejected（重复检出无法归并）。
- Decision: 生命周期沿 FR-010 五态 + 驳回/转人工；复核一致为系统标记，验证闭合为操作员确认；失效/重开为显式转换并保留历史。Alternatives: 闭合即终局——rejected（违反 Q5）。
- Decision: 检查点与结果同库事务一致（见 data-model.md §5），崩溃恢复可重扫但不漏扫不重复副作用。

## 4. 授权与处置接线

- 复用 `operation_id` 审计幂等（011 execOperatorOp、013 Unblock/Replay、012 supply、005 confirmauth）与现有固定权限（009 401/403、011 `can_execute`、012 `--operator` 仅审计）。
- 014 动作×权限×范围矩阵见 `contracts/auth-matrix.md`；认领≠执行权；字段填写≠授权；既有恢复入口保留其门禁（010 锁序、011 claim 验证、013 inbox/version 守卫、006 版本捕获）。
- 请求重复/响应丢失/结果未知：operation_id 去重读回、丢失按未知观察、超时有界重试或保持待验证（Q5-4）。

## 5. 承载形态与隔离（取舍见 ADR-001）

- Decision: 核心库 `ScanOnce` + 薄 `reconcile-admin` 命令；本阶段不在 serve/worker 自动启动。Rationale: Q3 只锁验收（独立暂停/在途有界/故障隔离/预算/状态诚实），实际调用链显示 serve 已承载 scanner/logscanner/confirmation/recovery + nonce loop，叠加自动扫描会耦合资金节拍；独立命令最易满足暂停不影响资金流程。Alternatives: 复用 worker 节拍——rejected（暂停/预算与资金调度共享，难以证明隔离）；常驻独立进程——deferred（本阶段用 admin 命令+预算即可验证，无需新超进程）。
- 隔离验证：故障注入（扫描错误/依赖超时/重试风暴）+ 并发资金负载下资金流程 SLO 不受影响；方法与预算见 quickstart.md。

## 6. CI 分层与测试路径

- 普通 PR：lint/build/unit(+race)/contract/`ci-required` 必跑；PG/Redis/Kafka/e2e 按 `ci.yml` 路径分类；Docker 缺位 NOT RUN + `ci:integration-pending`。
- 重载独立：大扫描/故障注入/长基准走 `fault`/`perf`/独立 integration 标签经 `fault-perf.yml`（schedule/dispatch），不新增普通 PR 长测（FR 测试假设）。
- 上游未接入证据边界：本地夹具只断言本项目范围结论；外部入账正确性明确标记超出证据（FR-006/Q4-5）。

## 7. 迁移编号与待测参数

- 实际最大 `migrations/000015_event_infrastructure.sql`；`000014` 被 intent-FK 修复占用；014 新迁移用 **`000016_reconciliation_handling.sql`**（已核对目录，不预猜）。
- 待测参数（非阻塞，需实现后测量填入，不编造）：暂停响应时间、单次扫描范围/时长配额、并发上限、PG/RPC 配额、新鲜度容忍窗、证据保留期、重复率告警阈值；生产阈值单独立项裁决。
- 真正阻塞项：风险接受/忽略差异政策未批准——本阶段不建该功能；若后续方案依赖，列为业务阻塞（见 plan.md）。
