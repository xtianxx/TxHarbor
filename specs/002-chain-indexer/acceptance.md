# Acceptance Record: 002-chain-indexer

**Branch**: `002-chain-indexer` | **Baseline**: `9e060bf` | **Date**: 2026-09-12

**提交纠正**：`9e060bf..HEAD` 共 **4** 个新提交（此前误报为 5，`9e060bf` 为基线本身）：
`aa0c6f1`、`e7a4dc8`、`e9f3e50`、`96c7739`。

**任务数纠正**：tasks.md 任务总数始终为 **18**（T001–T018，基线与当前一致，无删除）；
“16”为未勾选数（16 未勾选 + 2 已勾选 T002/T003），此前误读为总数。

## 运行证据（本机可执行部分，全部通过）

- `gofmt -l internal/ migrations/` → 空输出
- `go vet ./...` + `go vet -tags integration ./...` → 干净（含全部集成测试文件的类型检查）
- `go test ./...` → 112 passed / 10 packages
- `go test -race -count=1 ./internal/indexer/ ./internal/eth/ ./internal/config/ ./internal/metrics/` → 71 passed / 4 packages
- 其中：config 37（T002）、eth 15（T003）、indexer 单元 15（lease 11 + scanner 4）

## 逐任务状态（T001–T018）

| 任务 | 实现 | 验证 | 证据/备注 |
|------|------|------|-----------|
| T001 迁移 | ✅ 文件落地 | ⚠️ 部分 | 命名/版本/embed 单元绿；**DB 上库应用未运行** |
| T002 配置+脱敏 | ✅ | ✅ 已勾选 | 37 单元绿 |
| T003 取块+分类 | ✅ | ✅ 已勾选 | 15 单元绿 |
| T004 lease | ✅ | ⚠️ 部分 | 11 单元 + race 绿；接管/心跳集成**未运行** |
| T005 scanner | ✅ | ⚠️ 部分 | 4 单元绿；行为**未运行** |
| T006 接线+指标 | ✅ | ⚠️ 部分 | 契约单元绿；运行时接线**未运行** |
| T007 场景1,5+S变更+创世 | ✅ 测试已写 | ❌ 未运行 | scan_integration_test.go，编译通过 |
| T008 场景2,3,6,7 | ✅ 测试已写 | ❌ 未运行 | 同上 |
| T009 场景8,13 | ✅ 测试已写 | ❌ 未运行 | 同上 |
| T010 场景4 | ✅ 测试已写 | ❌ 未运行 | 同上 |
| T011 场景9 | ✅ 测试已写 | ❌ 未运行 | 同上 |
| T012 场景10,11,12 | ✅ 测试已写 | ❌ 未运行 | 同上 |
| T013 契约 | ✅ 测试已写 | ❌ 未运行 | 同上 |
| T014–T017 T1–T4 | ✅ 测试已写 | ❌ **从未执行**，强依赖真实 PG | coordination_integration_test.go（4 函数），仅类型检查 |
| T018 总体验收 | ⚠️ 静态+单元完成 | ❌ 集成门禁受阻 | 本文件即记录 |

## 阻塞原因（精确，非“Docker 未安装”）

- Docker **已安装**（client 29.6.1），daemon **正在运行**（dockerd + containerd + 3 shim 存活）。
- 真实原因：`/var/run/docker.sock` 属 `root:docker`，当前用户（uid 1000）**不在 `docker` 组**，
  无 sudo 口令，无法提权。`docker info` 报 `permission denied ... unix:///var/run/docker.sock`。
- 本机**不能**安全恢复（任何恢复都需 root：加组/改 socket 权限/以 root 运行）。

## 解除阻塞后的步骤（接手命令与环境）

1. 有权限者执行：`sudo usermod -aG docker <user>` 后**重新登录**（或以 root 会话运行）。
2. 验证：`docker info` 成功；`make lint && make build && make test`。
3. 全量集成（单命令覆盖全部 7 个集成测试文件：db×2、health、app、indexer×3，
   含 001 回归 + 002 全部受阻场景 + T1–T4 + 版本断言）：
   `make test-integration`（`go test -tags integration -count=1 -timeout 20m ./...`）。
   首次运行需拉取 `postgres:18.6-trixie` 与 `ghcr.io/foundry-rs/foundry:v1.8.1`（需网络与磁盘）。
4. 仅当对应测试**实际变绿**后，才勾选 tasks.md 剩余复选框并提交；并发项建议先单独
   `go test -race -tags integration ./internal/indexer/` 串行验证。

## 设计同步

- 实现中未发现与 spec 冲突；`migrate_integration_test.go` 版本断言随 002 迁移同步（1→2）。
- `Summary` RPC URL 脱敏修复已随 T002 落地并有单元测试覆盖。
- **002 验收未完成**：集成证据缺失，在解除阻塞并全绿之前不得声称通过。
