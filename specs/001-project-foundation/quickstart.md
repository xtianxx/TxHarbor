# Quickstart: 001-project-foundation

前置：Go 1.26.5、`docker compose` v5（顶层不写 `version:`）。计时口径：以下时间界限**不含**镜像下载、依赖安装与本地依赖启动；界限可配置，验收用默认值。

**环境限制（C1）**: 带 `integration` tag 的测试（tasks T014/T022/T028/T031）要求 Docker daemon 可用（`docker info` 通过）。当前记录：本机仅有 compose CLI、无 daemon，集成层在此环境不可运行。执行前提：本地备好 Docker 或在有 Docker 的 CI/机器上跑集成层；`go test ./...`（unit）无需 Docker，可先行。规则：不得以 mock 替代真实依赖验收；`SkipIfProviderIsNotHealthy` 跳过计为未执行、不得视为通过。

## 1. 启动依赖

```bash
docker compose up -d --wait        # PG + Anvil，healthcheck 全过才返回
docker compose ps
```

期望：`postgres` / `anvil` 均为 `healthy`。Go 服务连 `localhost:5432` / `localhost:8545`。

## 2. 迁移 + 启动

```bash
cp .env.example .env && $EDITOR .env   # 填写 TXHARBOR_PG_DSN / TXHARBOR_RPC_URL / TXHARBOR_CHAIN_ID=31337
set -a; source .env; set +a
go run ./cmd/txharbor migrate up       # 空库初始化；重复执行跳过
go run ./cmd/txharbor migrate status
go run ./cmd/txharbor serve &
curl -s localhost:8080/readyz         # 依赖可用时启动后 30s 内 200 ready
curl -s localhost:8080/livez          # 200 alive
curl -s localhost:8080/metrics | grep txharbor
```

## 3. 故障演练（对应 SC-002…006）

| 演练 | 命令 | 期望 |
|---|---|---|
| 配置缺失 | `env -u TXHARBOR_PG_DSN txharbor serve` | 30s 内非零退出，错误点名缺失项 |
| 非法配置 | `TXHARBOR_CHAIN_ID=abc txharbor serve` | 点名期望格式并退出 |
| 重复迁移 | `migrate up` ×2 | 第二次全跳过，无副作用 |
| 错链 | `TXHARBOR_CHAIN_ID=1 txharbor serve` | 拒绝就绪，报告期望 1 / 实际 31337 |
| 未迁移启动 | 跳过 `migrate up` 直接 `serve` | 拒绝就绪，提示先跑迁移；不自动迁移 |
| 依赖中断 | `docker compose stop postgres` | 10s 内 `/readyz` 503、`/livez` 200；`docker compose start postgres` 后 10s 内回 200 |
| 正常退出 | `kill -TERM <pid>` | 15s 内退出；超时强制结束并记错 |
| 日志脱敏 | `grep -rE 'password|secret|token=[^R]' <logs>` | 凭据明文出现 0 次（仅 `[REDACTED]`） |

## 4. 测试

```bash
go test ./...                                   # unit（默认 -short，无 Docker）
go test -race ./internal/health/... ./internal/config/...
go test -tags integration ./...                 # 集成（testcontainers，需 Docker）
```

## 5. 停止与清库

```bash
docker compose stop        # 默认保留 pgdata
docker compose down        # 同上，保留数据
docker compose down -v     # 独立显式清库命令（删本工程命名卷）
```

详见 `contracts/cli.md`（命令语义）与 `contracts/http.md`（探针语义）。
