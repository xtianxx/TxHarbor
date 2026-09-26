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
11. 历史复查（FR-007/010, Q5, `data-model.md` §6）：水位已前进、旧范围证据变化 → 无人工逐条触发即被发现并进入重验证；中断恢复与证据不足不得错误闭合；复查消耗有界 slice，无无限全量扫描。

## Layering

- 普通 PR：unit/contract/小 integration（PG tags）；重载（大扫描/fault/perf/长基准）走独立标签与 `fault-perf.yml`，不新增普通 PR 长测。
- 本地值≠生产阈值：暂停响应/配额/容忍窗实测填入，不宣称生产结论（Q3-6）。
