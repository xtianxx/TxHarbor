# Acceptance Record: 002-chain-indexer

**Branch**: `002-chain-indexer` | **Baseline**: `9e060bf` | **Date**: 2026-09-12

## 运行证据（本机可执行部分，全部通过）

- `gofmt -l internal/ migrations/` → 空输出
- `go vet ./...` + `go vet -tags integration ./...` → 干净（含新增集成测试文件的类型检查）
- `go test ./...` → 112 passed / 10 packages
- `go test -race -count=1 ./internal/indexer/ ./internal/eth/ ./internal/config/ ./internal/metrics/` → 71 passed / 4 packages
- 其中：config 37（T002）、eth 15（T003）、indexer 单元 15（lease 11 + scanner 4）

## 受阻未执行（环境缺 Docker daemon，无权限启动，如实记录）

- `make test-integration`（testcontainers 真 PG + Anvil）：全部 `//go:build integration`
  测试已编写并通过类型检查，但**一次未运行**，包括：
  - 迁移版本断言更新（`migrate_integration_test.go` 已同步到 version 2，未运行验证）
  - T004 lease 接管 / T005 行为 / T007–T013 13 场景 / T014–T017 确定性并发
- 因此 tasks.md 中仅 T002、T003 勾选（纯单元验证闭环）；其余任务保持未完成，
  不以静态检查替代运行证据。

## 13 场景 × 不变量核对状态

| 场景 | 测试位置 | 运行状态 |
|------|----------|----------|
| 1,5 首扫/重启 | scan_integration_test.go T007 组 | 未运行 |
| 2,3 追头等待 | 同上 T008 组 | 未运行 |
| 4 重复幂等 | T010 | 未运行 |
| 6,7 RPC/DB 故障 | T008 | 未运行 |
| 8 不确定提交 | T009 | 未运行 |
| 9 双实例 | T011 | 未运行 |
| 10,11,12 暂停/错链 | T012 | 未运行 |
| 13 安全退出 | T009 | 未运行 |
| T1–T4 并发 | coordination_integration_test.go | 未运行（编译通过） |
| 不变量硬断言 | quickstart 对应 SQL（gap/link/single/post-pause-zero） | 随集成测试，未运行 |

## 设计同步

- 实现中未发现与 spec 冲突；`migrate_integration_test.go` 版本断言随 002 迁移同步（1→2）。
- `Summary` RPC URL 脱敏修复已随 T002 落地并有单元测试覆盖。
