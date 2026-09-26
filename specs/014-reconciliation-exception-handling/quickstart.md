# Quickstart: 014 Reconciliation Validation Guide (Phase 1)

**Branch**: `014-reconciliation-exception-handling` | **Spec**: [spec.md](spec.md) | **Design**: [plan.md](plan.md), [data-model.md](data-model.md), [contracts/](contracts/)

Backend-only validation. No product code exists yet; run the named suites only after implementation. Upstream-unconnected assertions are explicitly out of evidence.

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
