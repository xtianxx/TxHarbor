# 014 pending_verify 生产复核闭环证据（2026-09-27 补齐轮）

- Feature: `014-reconciliation-exception-handling`
- 范围: 补 `pending_verify` 票的有界、受授权生产复核入口（真实 chain/PG/event 全量三路比较 + T040 R1/R2/R3），使 `dispose → pending_verify → 事实收敛 → consistent → close` 闭环在**真实入口**上成立；不改业务范围、不改资金门禁、不新增 daemon、不放宽 close。
- 关联任务: T026/T027/T028（本文件是其重新勾选的验收证据）。
- 关联契约: `contracts/discrepancy-lifecycle.md`（Transitions/History）、`expected-event-discriminator.md` §2/§5、`quickstart.md` §call-path。

## 1. 根因（此前口径为何不成立）

1. 生产 `reverify` 只巡检 `closed` 票；内置 `EventStateReverifyEvaluator` 是单方（event）再读、结构上永不返回 `consistent`（`internal/reconciliation/reverify.go` 注释自认）。
2. 历史验收中写 `consistent` 的路径只有测试替身 `us3ITConsistentEvaluator`（`internal/app/reconcileadmin/us3admin_integration_test.go`），不具备生产能力；`close` 所需的证据从未由真实适配器产生。
3. `CloseDiscrepancy` 在取得行锁**之前**独立读取最新 `reverify` 行（`LoadDiscrepancyEvidence`），读取与 `TransitionDiscrepancy` 的 CAS 之间存在并发窗口：并发的复核写行可在窗口内提交，close 仍按旧行闭合——"stale results never close / latest-row-wins" 未成立。

## 2. 实现（实际路径）

### 2.1 新入口命令面

```text
txharbor reconcile-admin reverify-ticket --task-id UUID --discrepancy-id UUID \
  --max-pg-requests N --max-item-attempts N [--max-item-duration D] [--reason R]
```

- 选择独立子命令而非扩展 `reverify`：`reverify` 是带 `history_sweep_through` 游标的 `closed` 票批量巡检（`--max-items`/游标语义），`reverify-ticket` 是按票随机访问、不写游标；两者共享 reverify/gap/audit 写入器与失败语义，但边界、对象与输出不同，混用会让一个必填界（`--max-items`）对单票静默失效。
- 授权沿用任务 scope 的 scan-management 模式（`principal × ActionScanStart × task scope`）；auth-matrix `reverify` 行仍 system-only，未新增权限、未新增 env 键。
- 单票必须处于 `pending_verify` 且记录 scope 被任务 scope 包含；任务必须 `running`（pause/cancel 与 scan 同等停止 014 工作）；预算为任务总预算 + 本切片 PG 上限；失败按累计审计尝试有界封顶（与 sweep 同语义）。

### 2.2 真实全量比较（`internal/reconciliation/ticketverify.go`）

- 复用 `compareScanCandidate`：本轮把 scan compare loop 的逐候选三路比较抽为共享函数（行为不变），scan 与新入口走**同一** chain/PG/event + R1/R2/R3 判别 + T017 分类路径。
- 候选从记录身份重建：业务键（request_id/intent_id/tx_hash/event_id）、业务类型（新票持久化；旧票仅无歧义推导）、chain 锚（区块 + tx_hash 键补 TxHash）、event 聚合键（`EventAggregateBusinessKey`，request/intent 映射与 scan 枚举同源；tx_hash 不发明 join → R2）。
- 重读窗口：优先记录的 detection interval（新票持久化，≤ 单次 claim 跨度）；旧票回退区块锚 `[b,b]`；两者皆无 → unknown/gap。
- `consistent` 仅在以下全部成立时写：三路证据完整、新鲜、覆盖闭合，分类结论 `consistent`，且记录结论守卫通过——
  - tx_hash 聚合：记录成员集在当前链证据中完整（否则 divergent），deposit 聚合还要求每个记录成员都有 004 credit 行（部分记录 → divergent；旧票无成员集 → unknown，不猜）；
  - 记录区块身份未变（变 → divergent/reorg）；
  - 记录 recovery 版本仍可观察且未轮换（轮换 → divergent；不可观察 → unknown）。
- 其余一律 unknown/stale/divergent + gap：单方未变、未找到候选、双缺、查询失败、可能裁剪、未知覆盖、越界/非 `pending_verify`、旧票证据不足；`normalizeReverifyFinding` 继续拒绝无证据引用/新鲜度的 consistent。
- 写入：票行锁 + 状态 CAS 内写 `reverify`/`recon_gap`/`recon_audit`（`sweep` 判别为 `ticket`，含范围与版本审计）；读取期间票离开 `pending_verify` → 丢弃 + 审计，不写裁决行。不触发处置/恢复/重放/付款；只动 014 表。

### 2.3 检测元数据持久化（沿革兼容）

`discrepancy.evidence_version_domain`（JSONB，无迁移）新增：`business_type`、`detection`（claimed interval）、`tx_members`（tx 聚合成员集，扫描建单/失效替换时写入）。旧行缺省即保守：不能证明就不一致，不猜不补。

### 2.4 close 行锁修复（T026）

`CloseDiscrepancy` 改为：BEGIN → `SELECT … FOR UPDATE` 锁定票行 → **同一事务内**读最新 `reverify` 行 → 校验守卫 → CAS 更新 → COMMIT；拒绝路径在同一事务写 `refuse` 审计。`TransitionDiscrepancy` 与新 close 共享 `applyLockedDiscrepancyTransition`，行为单源。

## 3. 正反例测试与断言

### 3.1 真实入口闭环（built binary + 真实 PG + 隔离 Anvil）

`internal/app/reconcileadmin/ticketverify_integration_test.go`
`TestIntegrationReconcileAdminPendingVerifyAcceptance`

| 子测试 | 断言 |
|---|---|
| `positive_loop_missing_converges_then_verify_consistent_then_close` | 真实 `start→resume→scan` 铸 `missing` 票（tickets=1/pending=0/gaps=0）→ 真实 `claim→dispose` 到 `pending_verify` → 011/012 事实收敛后 `close` 先被拒（无复核行）→ 真实 `reverify-ticket` 输出 `verdict=consistent consistent=true`、DB 最新裁决 `consistent` 且 `freshness_at`/`evidence_ref(ticketverify/v1)` 非空、状态保持 `pending_verify` → 授权 `close` 成功（from=pending_verify/to=closed）；007–012 资金表全行摘要零移动；closed 巡检 `reverify` 仍可运行（生产单方 evaluator → unknown，状态保持 closed） |
| `unrepaired_missing_and_partial_aggregate_stay_divergent` | 未修复 missing → `verdict=divergent`、close 拒绝；deposit 聚合 2 成员仅 1 条 credit → `verdict=divergent` 且输出含 `partially recorded`、close 拒绝 |
| `unknown_gap_and_scope_refusals` | 未闭合事件覆盖 → `verdict=unknown pending=true gap=true`、close 拒绝；`open_claimable` 上复核 → 拒绝 + `refuse` 审计 1 行 + 0 复核行；任务 scope 不含票 scope → 拒绝 + `refuse` 审计 |
| `change_expiry_revocation_concurrency` | 重复复核两次均 consistent 且仅追加裁决行、状态不变、资金表零移动；并发复核 3 进程全部 0；过期容忍（`--reverify-tolerance 1ns`）close 拒绝；撤权（删除 close 授权）close 拒绝；并发 close 3 进程恰 1 成功（行锁 CAS）；consistent 后事实回退再复核 → divergent，随后 close 拒绝（旧 consistent 行被取代不可闭合） |

### 3.2 并发守卫（真实 PG + 受控交错）

`internal/reconciliation/ticketverify_store_integration_test.go`

- `TestIntegrationTicketVerifyDiscardsWhenStateChangesDuringRead`：证据读取开始后把票改为 `closed`，复核在持久化行锁处发现状态变化 → `Discarded=true`、0 条 reverify 行、1 条 `discarded` 审计。
- `TestIntegrationCloseReadsLatestReverifyUnderRowLock`：持锁事务内提交 divergent 行后释放锁，close 在同一行锁事务内读最新行 → `ErrReverifyRequired`、票保持 `pending_verify`、1 条 `refuse` 审计（修复前该场景会按锁前旧 consistent 行闭合）。

### 3.3 纯函数/单元

`internal/reconciliation/ticketverify_test.go`：检测元数据 JSON 往返与拒绝、业务类型无歧义推导、重读窗口选择、scope/区间包含、outcome 矩阵（divergent/stale/unknown/consistent、recovery 轮换、区块变化、聚合部分记录/成员缺失/旧票无成员集）。

## 4. 验证运行记录（2026-09-27，本机）

| 检查 | 命令 | 结果 |
|---|---|---|
| build | `go build ./...` | PASS |
| vet | `go vet ./...` | PASS |
| unit | `go test ./... -count=1` | PASS |
| contract | `go test -tags contract ./... -count=1` | PASS |
| unit + race 抽样 | `go test -race ./internal/reconciliation/ ./internal/app/reconcileadmin/ -count=1` | PASS |
| integration（reconciliation 全量） | `go test -tags integration -run TestIntegration ./internal/reconciliation/ -count=1` | PASS（74.4s，含既有 sweep/close/scan 与新增并发守卫） |
| integration（reconcileadmin 全量，含新二进制闭环） | `go test -tags integration -run TestIntegration ./internal/app/reconcileadmin/ -count=1` | PASS（30.6s） |
| integration + race（并发守卫） | `go test -race -tags integration -run 'TestIntegrationTicketVerify…|TestIntegrationClose…' ./internal/reconciliation/ -count=1` | PASS（9.0s） |
| NOT RUN | `fault` / `perf` 标签套件（独立通道，本轮未改 fault/perf 文件；Docker 存在但未跑重载层） | 未执行，不记 pass |

## 5. 剩余限制（保守、显式）

1. 旧票（本变更前创建、无 `business_type`/`detection`/`tx_members`）：仅能从无歧义形状推导时复核；tx_hash 且 scope 混合类型/无检测窗 → unknown/gap，不得闭合（不猜、不补写历史证据）。存量票经一次证据变化的重检（scan 失效路径）会带上新元数据。
2. 记录 recovery 版本在生产任务中通常为空；一旦记录非空且重读不可观察，结果为 unknown（保守）。
3. 未新增自动失效检测器：`close` 后的事实变化仍依赖 scan/巡检的差异发现（既有路径）或下次复核；close 的窗口由最新行 + tolerance 界定，不承诺跨链/PG 全局瞬时快照。
4. 阈值仍为部署/测试参数（无默认）：`--reverify-tolerance` 默认取 `TXHARBOR_RECON_FRESHNESS_TOLERANCE`；本文件数值不构成生产阈值声明。

## 6. 后续更正（2026-09-27 复核写写反序修复轮）

本文件 §2.4/§3.2 的并发守卫（票行锁 + 状态 CAS、close 行锁内 latest-row-wins）**不构成完整的写写时序保护**：已证实“A 先取证、B 后取证但先提交”时，A 恢复提交仍会用过期结果覆盖 B 的有效结论、删除 B 的 gap、推进验证进度并使 close 接受旧 consistent。修复轮为所有裁决写入引入复核有效性令牌协议（`reverify_generation` + state + 证据版本哈希，共同行锁内校验；丢弃只审计），受影响任务 T026/T027/T028 曾恢复未完成并重新验收。**本文件的旧结论“复核入口已无合入前缺陷”撤回**；完整根因、负对照与修后证据见 `docs/evidence/014/reverify_write_order_evidence.md`。其余证据（真实二进制闭环、R1/R2/R3、失败语义等）不撤销。
