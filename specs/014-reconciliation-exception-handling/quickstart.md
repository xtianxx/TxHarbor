# Quickstart: 014 Reconciliation Validation Guide (Phase 1)

**Branch**: `014-reconciliation-exception-handling` | **Spec**: [spec.md](spec.md) | **Design**: [plan.md](plan.md), [data-model.md](data-model.md), [contracts/](contracts/) | **Research**: [research.md](research.md)

Backend-only validation. No product code exists yet; run the named suites only after implementation. Upstream-unconnected assertions are explicitly out of evidence.

**Gate**: T000-P stays OPEN — this guide is not a release and claims no production readiness; the numbers here are local test inputs only. Risk-accept/ignore is explicitly absent (not approved, no contract row, no task), so there is no scenario for it below.

## Prerequisites

- Local PG/Anvil (+Redis/Kafka where the layer needs them); `make test`, `make test-integration`, `make test-e2e` runnable; fault/perf via `make test-fault` / `make test-perf` (independent channel).

## Scenarios (FR/SC/Q1–Q5 mapping)

1. 漏处理检出（FR-002/007/008/015, SC-001, US1-1, Q1）：受控数据集链上有确认交易而 PG 无记录 → 漏处理单、只告警、零付款。
2. 正常重复零建单（FR-009/016, SC-001/004, US1-2, Q4）：at-least-once 重投被幂等吸收 → 仅指标审计；业务分歧重复 → 建单去重。
3. 证据不足（FR-004/005/006/018, Edge, Q1）：扫描未闭合/过期/上游未接入/未知 → 待核验或覆盖不完整，不判一致不闭合。
4. 重组保守（FR-017, SC-003, US3-1, Q5）：孤块证据 → 撤销或转待复核；确认再现重开原单。
5. 并发闭合（FR-010, SC-003/005, US2-2/US3, Q2/Q5）：A 认领后 B 抢占被拒并审计；并发写入致证据过期 → 失效转待验证，不以过期结果闭合。
6. 越权矩阵（FR-011/012, SC-005, Q2）：匿名/越权 query/claim/dispose/close 100% 拒绝并审计；`contracts/auth-matrix.md` 全行覆盖。
7. 中断恢复（FR-003, SC-003/006, US3-3, Q3）：kill/重启 → 指针不越界，已持久不重报，未覆盖继续。
8. 超预算（FR-019, SC-006, Q3）：配额耗尽 → 可观察暂停 + gap 行；恢复无遗漏无重复副作用。
9. 重复处置幂等（FR-016, SC-004, US2-3）：已闭合处置重放 10 次 → 零新副作用，审计完整。
10. 预算与隔离（Q3/ADR-001）：故障注入 + 并发资金负载下资金流程不受影响；暂停仅作用 014 范围。
11. 历史复查（FR-007/010, Q5, `data-model.md` §6）：水位已前进、旧范围证据变化 → 无人工逐条触发即被发现并进入重验证；中断恢复与证据不足不得错误闭合；失败项记 gap 留痕有界重试，遍历游标不等于已验证完整，gap 相关差异不得闭合；复查消耗有界 slice，无无限全量扫描。

## Layering

- 普通 PR：unit/contract/小 integration（PG tags）；重载（大扫描/fault/perf/长基准）走独立标签与 `fault-perf.yml`，不新增普通 PR 长测。
- 本地值≠生产阈值：暂停响应/配额/容忍窗实测填入，不宣称生产结论（Q3-6）。
- 确认参数与旧任务（T037，Q5）：`--confirm-threshold-n` 为对账观察参数（来源=start 显式参数，快照=`task.policy_refs`），不写 `confirmation_policy_history`，不改变 005 确认语义；`policy_refs`/scope/budget 创建时确定、无修订事务，变更走 cancel＋重建；收口前已创建的 `policy_refs='{}'` 旧任务链证据恒 pending（fail-closed），须重建，禁止静默默认值。

### tags × suites 对照矩阵

| Go build tag | 套件 / 命令 | 触发通道 | 覆盖（014） | Docker 缺位纪律 |
|---|---|---|---|---|
| （无 tag） | unit `make test` / `make test-race` | 普通 PR 必跑 | 包骨架（T001）、identity/classify/状态机纯函数（T005/T007 等落地后） | 不需要 Docker |
| `contract` | `make test-contract` | 普通 PR 必跑（无 Docker） | 任务/差异生命周期契约、幂等键与非法跳转（T011/T019） | 不需要 Docker |
| `integration` | `make test-integration` | 普通 PR 路径分类命中（`migrations/**`、`internal/db/**` 已命中；`internal/reconciliation/**` 见下方备注） | PG 扫描/检查点/认领互斥/崩溃恢复/越权拒绝（T012/T020/T025） | 记 NOT RUN（配 `ci:integration-pending` 标签），不得记 pass；PG 由 testcontainers 真实提供 |
| `fault` | `make test-fault` | 独立 `fault-perf.yml`（schedule/dispatch/release gate），永不进普通 PR | 重组/未知/中断/并发失效（T024，quickstart §4/5/7/10） | 独立层同样 NOT RUN，不得以“本地无 Docker”推断通过 |
| `perf` | `make test-perf` | 独立 `fault-perf.yml`（同上） | 预算耗尽/暂停响应与并发资金负载隔离的测量（数值待测，不宣生产阈值） | 同左；014 本轮未新增 perf 文件时，该层按 Makefile 守卫如实报 NOT RUN |

- 014 不新增普通 PR 长测；`fault`/`perf` 失败不阻塞普通 PR，但阻塞对应发布声明（无绿色运行即无该项证据）。
- `integration_redis` / `integration_kafka` / `e2e` 与 014 无关（014 不为 Redis/Kafka 增加权威状态、不新增 e2e 流程），保持 `ci.yml` 既有分类，不引入新触发。
- CI 路径分类备注（非本任务改动）：`internal/reconciliation/**` 尚未列入 `.github/workflows/ci.yml` 的 integration-PG 路径集合；实现批次合并前由 CI owner 增列，不得默认“已被分类”。

## §call-path

T028 把“重复调用”定为 014 的正式运行方式：唯一的入口是 `txharbor reconcile-admin`（thin command），每次调用只做**一个有界的步进**并落库；收敛来自反复调用推进已持久化的指针/游标，而不是单次长跑。014 不新增 daemon，serve/worker 不自动启动任何 014 工作（ADR-001：core `ScanOnce` + thin admin command），调用者由 operator 或外部调度器充当。

### 精确调用路径（built binary；命令面见 `reconcileAdminUsage`）

```bash
# 0) 前置：PG/RPC、预算/新鲜度/窗口键、主体绑定（键名见下；值均为部署参数）
export TXHARBOR_RECON_PRINCIPAL=deploy:recon-ops

# 1) 建任务：scope/budget/policy_refs 创建时固定（无修订事务；变更=cancel+重建）→ created
txharbor reconcile-admin start --chain-id CHAIN --scope-kind height --from H0 --to H1 \
  --business-types withdrawal[,deposit,event-delivery] --confirm-threshold-n N \
  [--task-id UUID] [--upstream-receipts withdrawal=ledger:connected] [--reason R]

# 2) 激活 → running
txharbor reconcile-admin resume --task-id UUID [--reason R]

# 3) 重复 scan：每次领取下一个预算区间；同事务推进 result_persisted_through + checkpoint 行
txharbor reconcile-admin scan --task-id UUID

# 4) 重复 reverify：每次一个有界历史复查 slice；推进 history_sweep_through 游标
txharbor reconcile-admin reverify --task-id UUID \
  --max-items N --max-pg-requests N --max-item-attempts N [--max-item-duration D] [--reason R]

# 5) US2 处置闭环：claim（仅归属）→ dispose（ack_only/reuse_recovery/new_fix_rule）→ show
txharbor reconcile-admin claim --discrepancy-id UUID --operator NAME --reason R --operation-id OP
txharbor reconcile-admin dispose --discrepancy-id UUID --kind ack_only [--result done] \
  --operator NAME --reason R --operation-id OP
txharbor reconcile-admin show --discrepancy-id UUID

# 6) 证据门控闭合：最新 reverify 行只从 DB 读取，调用方不可替换证据
txharbor reconcile-admin close --discrepancy-id UUID \
  --close-basis '{"range":"H0..H1","block":H1,"version":"v1","observed_at":"RFC3339"}' \
  --reason R [--reverify-tolerance D]

# 7) 暂停/恢复/取消（仅 014 任务；不暂停上游资金流程）
txharbor reconcile-admin pause --task-id UUID --reason R
txharbor reconcile-admin resume --task-id UUID [--reason R]
txharbor reconcile-admin cancel --task-id UUID --reason R
```

### 重复调用语义（收敛点）

- `scan` 可重复：幂等重调在**同一 DB 事务**内推进 `result_persisted_through` + checkpoint 行；崩溃/中断后从指针恢复，已持久范围不重报、未覆盖范围继续（quickstart §7）；配额耗尽 → 可观察暂停 + gap 行（§8）。
- `reverify` 可重复：每个 slice 严格有界（`--max-items` / `--max-pg-requests` / `--max-item-attempts`；`--max-item-duration` 可选，零=由父 context 界定），切片消耗同时计入任务总预算；失败项记 `query_failed` gap 并按**失败次数升序 + 证据年龄降序**有界重试（不饿死任何项），成功即消除 gap。**遍历游标（`history_sweep_through`）≠ 已验证完整**：只有 `verified_complete=true`（水位到底 + 零开放 gap + 无重试耗尽/失败且未被配额截断）才可称为完整；`stop=slice_exhausted` 表示仍有剩余，下一次调用继续。命令只做有界只读复核与自身记录（reverify/gap/audit/游标），不自动处置、不自动闭合、不触发恢复/重放/付款。
- `close` 证据门控：授权 = principal × verify_close × 票据记录范围（默认拒绝，拒绝亦审计）；`Store.CloseDiscrepancy` 始终读取 DB 中**最新** reverify 行，过期/未知/分歧/gap 受限一律拒绝并审计（disposed ≠ reverified ≠ closed）；`close_basis` 记录范围/区块/版本/时点。
- 失效/重开（Q5）：结论相关变化（并发写入/重组/新证据/来源或版本轮换）把 `closed` 退回 `pending_verify`（仅触发已批准的重验证流程）；确认再现 → `reopened`（`reopen_count+1`，历史保留）；无关写入不触发。**旧 consistent 结果不能直接闭合**：重验证写入新的裁决行后，close 以最新行为准；被取代的旧结果再次 close 会被拒绝并审计。
- `pause`/`resume`/`restart`：pause/cancel 仅作用于 014 范围（在途 attempt 有界结清、留 gap，不暂停上游资金流程）；`resume` 从 `result_persisted_through` 指针与 `history_sweep_through` 游标继续，不重报已持久范围；进程 kill/restart 不丢状态——重启用的是同一 DB 指针/游标（无内存态依赖），重启后的重复调用等价于一次新的有界步进。
- 调度：operator/外部调度器重复调用同一二进制即可（频率/间隔是部署决策）；014 不新增 daemon、不在 serve/worker 内自动启动或自动恢复（ADR-001）；所有调用都是前台的、有界的、可暂停的。

### 配置键（T018 定义；T028 不重命名、不新增）

`TXHARBOR_RECON_PRINCIPAL`、`TXHARBOR_RECON_CONCURRENCY`、`TXHARBOR_RECON_MAX_SPAN_PER_CLAIM`、`TXHARBOR_RECON_MAX_DURATION`、`TXHARBOR_RECON_MAX_PG_REQUESTS`、`TXHARBOR_RECON_MAX_RPC_REQUESTS`、`TXHARBOR_RECON_LEASE_TTL`、`TXHARBOR_RECON_FRESHNESS_TOLERANCE`（亦为 `close` 的 `--reverify-tolerance` 默认来源）、`TXHARBOR_RECON_MAX_TIP_LAG`、`TXHARBOR_RECON_MAX_CANDIDATES`、`TXHARBOR_RECON_MAX_EVENT_ROWS`、`TXHARBOR_RECON_SETTLE_LIMIT`、`TXHARBOR_RECON_WINDOW_MAX_PROBES`、`TXHARBOR_RECON_MANAGEMENT_TRUST`（仅管理面信任根）。

所有数值均为部署/测试参数：缺失/非正值由命令**按名拒绝**（无默认）；本文件不宣称任何生产阈值（Q3-6）。
