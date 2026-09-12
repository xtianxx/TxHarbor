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

## 6. CI 补齐验收记录（2026-09-12，`chore/add-ci`）

范围：仅检查入口（Makefile）、GitHub Actions 工作流、根 `.gitignore`、`THIRD_PARTY.md` 补录与本记录；不改业务行为、DB 语义、权限与签名边界。

### 6.1 共用检查入口（本地与 CI 同调，`.PHONY: test test-race test-integration db-reset lint build`）

| 目标 | 命令 | 说明 |
|---|---|---|
| `make lint` | `gofmt -l .` + `go vet ./...` + `go vet -tags integration ./...` | 新增 integration tag 的 vet 覆盖 |
| `make build` | `go build ./...` | 新增 |
| `make test` | `go test -count=1 -timeout 5m ./...` | 语义不变，仅固化 count/timeout |
| `make test-race` | `go test -race -count=1 -timeout 10m ./...` | 新增 race 固化 |
| `make test-integration` | `go test -tags integration -count=1 -timeout 20m ./...` | 语义不变，仅固化 count/timeout |

未引入 golangci-lint / staticcheck 等新依赖。

### 6.2 本地执行结果（本机 Go 1.26.5 / Docker 29.6.1，全部通过）

| 命令 | 结果 | 耗时 |
|---|---|---|
| `make lint` | 通过（gofmt 空；两种 tag vet 均过） | 1.07s |
| `make build` | 通过 | 4.39s |
| `make test` | 通过（7 个测试包全 ok） | 3.73s |
| `make test-race` | 通过（7 个测试包全 ok） | 12.09s |
| `sg docker -c 'make test-integration'` | 通过（app 9.19s / db 24.89s / health 14.79s，Go 包级并发，总 28.62s） | 28.62s |

集成细项（`-v` 复跑，91 PASS / 0 SKIP / 0 FAIL）：迁移首跑 `applied=1`、重复迁移 `applied=0 skipped=1`、并发迁移串行（10.86s）、锁等待超时非零退出、失败迁移回滚重试、未迁移拒服、错误配置点名退出（in-process 与真容器两类）、错链 `expected 1`/`actual 31337`、依赖故障 10s 内 503/200 翻转（14.87s）、SIGTERM 退出码 0（`TestRunShutdown*` 与 flip 用例尾部断言）。

Docker 访问说明：当前登录会话 `id` 未含 docker 组，但 `/etc/group` 中 dream 属 docker(gid 989)，故以 `sg docker -c` 执行，全部真容器执行、0 skip（未触发 `SkipIfProviderIsNotHealthy`）。CI runner 直连 Docker，无需 sg。

### 6.3 CI 工作流（`.github/workflows/ci.yml`）

- 触发：`pull_request` + `push` 到 `main`；无自动部署。
- jobs：`lint`、`build`、`unit`（均不依赖 Docker，逐 job 调用 Makefile 目标）与 `integration`（testcontainers 自管容器 + `docker info` 前置探测）。
- 固定：Go 1.26.5；镜像 `postgres:18.6-trixie` / `ghcr.io/foundry-rs/foundry:v1.8.1`（测试代码内已 pin）；job 与 step 均设 timeout。
- 凭据：仅使用虚构 DSN/RPC_URL/CHAIN_ID；integration job 以 `::add-mask::` 掩码虚构 DSN，业务日志复用 logx 脱敏，工作流不 echo 密钥。
- 失败语义：Make 目标任一失败即 job 失败；多行步骤 `set -euo pipefail`。
- 已用工作流同款虚构 env 复跑并发迁移用例（PASS），确认 workflow 级 env 不遮蔽测试注入的容器 DSN（Go 读取 `os.Environ()` 同名末值）。

### 6.4 安全小缺口与其他

- 根 `.gitignore` 新增：忽略 `.env`（`.env.example` 保持跟踪）；`git check-ignore .env` 已验证命中。
- `THIRD_PARTY.md` 直连依赖表补录 `github.com/moby/moby/api v1.55.0`（Apache-2.0，仅 integration tag 测试使用，固定端口脚手架类型）。
- 未改动业务代码、迁移、配置校验、健康/指标语义与脱敏实现。

### 6.5 剩余限制

- 本机经 `sg docker` 验证；CI runner 直连 Docker，不走 sg。
- integration job 需外网拉取两个 pinned 镜像（本机已缓存，CI 首次运行实拉）。
- “工作流存在≠门禁生效”：main 分支保护与 required status checks 需在 GitHub 配置后才具约束力（现状与待办见 PR 描述）。

### 6.6 CI 首跑结果（GitHub Actions，PR #1）

| Run | SHA | 结论 | wall | 备注 |
|---|---|---|---|---|
| [34700354205](https://github.com/xtianxx/TxHarbor/actions/runs/34700354205) | 3027360 | success（4/4 job） | 97s | unit 94s / integration 83s / build 52s / lint 65s |
| [34700504329](https://github.com/xtianxx/TxHarbor/actions/runs/34700504329) | 37f87e2 | success（4/4 job） | 82s | actions checkout@v7 + setup-go@v7（node24），无 annotation |

- lint / build / unit / integration 四个 job 均真实执行；integration 在 runner 上拉取 pin 镜像并运行 testcontainers，非 skip。
- Node.js 20 弃用告警（checkout@v4 / setup-go@v5）经升级到 v7 major 消除，升版后复跑仍全绿。
