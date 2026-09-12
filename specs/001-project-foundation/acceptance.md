# Acceptance Record: 001-project-foundation

**Feature**: `feat/001-project-foundation` | **Date**: 2026-09-12
**Status**: 实现已落地，集成验收已完成（tasks.md 40/40，逐项核对勾选）
**Scope**: 仅 001 工程底座；未进入 002。

## 1. 通过检查（证据）

- `gofmt -l` 为空；`go vet ./...` 与 `go vet -tags integration ./...` 通过。
- `make test`（=`go test ./...`）7 包全绿；`make lint` 通过。
- `make test-integration`（=`go test -tags integration ./...`）全绿：app / config / db（含并发迁移 T031）/ eth / health（含探针翻转 T028）/ logx / metrics。
- `go test -race ./internal/health/... ./internal/db/...` 通过。
- quickstart §3 八项手工演练：配置缺失/非法点名退出、重复迁移 `applied=0 skipped=1`、错链报 expected 1/actual 31337、未迁移拒服并提示 `migrate up`、PG 中断 3s 恢复、SIGTERM 5ms 退出码 0、日志凭据 0 命中、Anvil 中断恢复见 §2。

## 2. Anvil/RPC 中断与恢复证据（SC-003 / FR-012）

`TestReadyzFlipsAndRecoversWithRealDependencies`（真 Anvil + 真 PG，非 mock）连续 3 次通过（13.9s / 15.6s / 15.1s），断言：
- 停 Anvil 后 10s 内 `/readyz` → 503 且 `/livez` 保持 200；
- 重启 Anvil 后 10s 内 `/readyz` → 200，无需重启服务；
- PG 中断/恢复同矩阵同样断言通过；`/metrics` 同步翻转（`txharbor_ready 0/1`）。
结论：10s 感知/恢复界限满足（探针 2s + 单次 5s，最坏约 7s）。

## 3. 验收期修复事项

1. 迁移 `skipped` 计数与契约不符 → 改为本轮跳过数（`internal/db/migrate.go`）。
2. foundry 镜像 `ENTRYPOINT` 为 `/bin/sh -c`，`command` 参数被吞致 anvil 只绑容器内环回 → compose 与测试均显式覆写 entrypoint（`compose.yaml` + 2 个测试 helper）。
3. testcontainers Stop 后 Start 重分配宿主机端口 → 恢复类测试固定宿主机端口（测试脚手架约束，非产品缺陷）。
以上发现已回写 `research.md` §5/§7。

## 4. 表述修正（取代前序报告措辞）

前序报告“停止旧 Anvil 不丢失内存状态”表述不准确，修正如下：
Anvil 为纯内存链，kill 进程即丢失其链状态。被停止的是操作员此前手动启动的 arbitrum-sepolia fork 实例（`--fork-url publicnode`，chain-id 31337，已运行约 2 天）；其内存状态已丢失。影响评估：该实例为一次性本地 fork（状态本就随进程结束而消失，可用原命令重建，无持久数据损失），且其占用 8545 端口阻塞了 compose 依赖启动；001 自身的 compose Anvil 为独立容器，不受影响。

## 5. 剩余限制

- 集成验收依赖 Docker daemon + 外网镜像（本机经代理 + registry mirror 解决；CI 需同等出网条件）。
- testcontainers 恢复类测试依赖固定宿主机端口脚手架（见 §3.3），属测试约束。
- go-ethereum 使用与分发策略见 `THIRD_PARTY.md`（LGPL-3.0 库包，零 `cmd/` 包；分发二进制须附带声明，T040）。
- 后续阶段（002 起）另立规范；本分支不含任何业务表与业务逻辑。
