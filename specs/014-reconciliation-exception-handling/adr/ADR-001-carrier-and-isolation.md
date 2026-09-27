# ADR-001: 014 Carrier Form and Isolation

**Date**: 2026-09-26 | **Spec**: [spec.md](spec.md) (FR-025, Q3) | **Status**: Accepted (plan scope)

## Context

Q3 只锁定验收要求，不锁独立进程/复用 worker。实际调用链：serve 已承载 scanner/logscanner/confirmation/recovery + nonce loop；worker 承载 claim/advance；eventpublisher/consumer 为独立命令。014 需独立暂停、不影响资金流程、在途有界、故障隔离、预算可测。

## Decision

核心库 `ScanOnce` + 薄 `reconcile-admin` 命令；本阶段不在 serve/worker 自动启动 014 扫描；暂停/预算/退避/取消在核心库强制，载体无法绕过。

## Rationale

- 独立命令最易证明暂停仅作用 014 范围；复用 worker 节拍会共享调度与预算，隔离难测。
- 常驻独立进程 deferred：admin 命令 + 预算已可验证全部验收，无需新增超进程（XIII 简单优先）。

## Consequences

- 若形态不满足验收，plan 回报调整，不得削弱要求；取舍记技术计划，必要时补 ADR。
- 测试参数与生产阈值分开记录。

## Alternatives

- 复用 worker 节拍：rejected（隔离举证难）。
- 常驻独立进程：deferred（当前不需要）。
