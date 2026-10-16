# TxHarbor

TxHarbor 是 Go + PostgreSQL 实现的 EVM / ERC-20 **充值观察与提现异步执行基础设施**：连续同步链上
区块与白名单 ERC-20 转账日志并判定充值确认（含链重组回退与重索引），接收提现请求后交给异步
worker 完成隔离签名、广播与结果对账。

它不是钱包界面，也不维护用户余额账本。服务通过 EVM JSON-RPC 跟踪配置范围内的代币与收款地址；
PostgreSQL 保存权威业务状态，Redis / Kafka 是可选的缓存、限流与事件通道。

<a id="限制与路线图"></a>

> **开发与验证阶段，未生产部署。** 目前仅提供 `development` 本地密钥 provider（`production` 模式
> 无 KMS/HSM provider 即拒绝启动）；应用内无 TLS，需前置终止；数据库角色未隔离，各进程共用同一
> 业务 DSN。

<a id="架构"></a>

## 文档

按读者任务选择入口：

- **架构与设计取舍** —— 组件、数据流与信任边界：[docs/architecture.md](docs/architecture.md)
- **配置参考** —— 环境变量、默认值与管理 CLI：[docs/configuration.md](docs/configuration.md)
- **测试与验证** —— 分层入口、已留存状态与证据索引：[docs/verification-matrix.md](docs/verification-matrix.md)
- **备份与恢复运维** —— 备份、隔离验证、恢复核验与按能力放行：
  [docs/ops/recovery-runbook.md](docs/ops/recovery-runbook.md)
- <a id="演示本地隔离"></a>**本地演示** —— 隔离 Anvil 上的真实充提与负例演练：
  [scripts/portfolio-demo/README.md](scripts/portfolio-demo/README.md)
- **变更记录** —— 已合并变更与版本历史：[CHANGELOG.md](CHANGELOG.md)
- **展示材料** —— 集中讲解提纲与面试要点：[docs/portfolio.md](docs/portfolio.md)
- **规格文档** —— 各阶段 spec / plan / data-model / contracts / ADR：[specs/](specs/)

<a id="功能特性"></a>

## 典型工作流

- **充值**：配置需要观察的代币与收款地址，`serve` 跟踪转账并按确认深度更新充值状态。
  遇到链重组时，撤销旧分叉的观察结果并重新索引；进程重启后可继续处理。
- **提现**：`POST /withdrawals` 校验身份、逐笔授权与幂等请求并持久化，不直接签名或广播。
  执行准入后，`withdrawal-worker` 预留 nonce，调用独立的 `signer-serve` 签名，再广播并检查回执。
  结果未知时先按链上事实对账，不盲目重新付款；完成后仍跟踪链上修订。
- **事件**：业务状态与 Outbox 同事务写入，再发布给下游；投递采用至少一次语义，消费端负责幂等，
  不承诺跨系统恰好一次。

## 适用场景

- 需要把「链上充值确认」和「提现异步执行」接进自有系统，并希望进程崩溃、链重组之后可以恢复的团队；
- 需要私钥进程隔离、nonce 与交易生命周期持久化、以及结果未知时按链上事实收敛而不是重发的场景；
- 需要本地、可复现地演示充提闭环与拒绝负例（见上方「本地演示」入口）的评审或教学场景。

## 快速开始

需要 Go 1.26.5、Docker + Docker Compose、bash 或 zsh，以及用于健康检查的 `curl`。
首次获取后进入仓库根目录执行；已有本地仓库可跳过前两行。

```bash
git clone https://github.com/xtianxx/TxHarbor.git   # 首次获取
cd TxHarbor
cp .env.example .env
set -a; source .env; set +a

docker compose up -d                 # PostgreSQL(127.0.0.1:5432) + Anvil(127.0.0.1:8545)
go run ./cmd/txharbor migrate up     # 应用迁移；serve 不会自动迁移
go run ./cmd/txharbor serve          # 长运行进程，默认监听 127.0.0.1:8080
```

另开一个终端检查健康状态：

```bash
curl -fsS http://127.0.0.1:8080/livez
curl -fsS http://127.0.0.1:8080/readyz
```

> `.env.example` 中的合约与观察地址是占位值。以上步骤只启动索引服务与健康检查：不会部署代币，
> 也不会自动完成充值或提现。完整的本地充提演示（隔离 Anvil、一次性测试代币与测试密钥，
> **不要用于真实资产**）见[演示说明](scripts/portfolio-demo/README.md)。

`signer-serve`（隔离签名）、`withdrawal-worker`（提现执行）与事件通道等其它进程的必填键见
[配置参考](docs/configuration.md)；停止时对前台运行的 `serve` 按 Ctrl-C 即可优雅退出。

## 开发与测试

```bash
make test        # 单元测试
make test-race   # 单元测试 + race 检测
make lint        # gofmt + go vet（含集成构建标签）
make build       # 构建
```

集成（PostgreSQL / Redis / Kafka）、E2E、故障注入、性能与灾备演练等分层入口需要 Docker；各层已
留存状态与证据索引见 [docs/verification-matrix.md](docs/verification-matrix.md)。

## 许可证

许可证待定：仓库暂无独立 LICENSE 文件；依赖与许可说明见 [THIRD_PARTY.md](THIRD_PARTY.md)。
