# 变更方案：简历演示包（本地隔离真实充提演示）

> **文档性质**：轻量补充流程方案（plan-only，含 2026-10-11 F1–F7 修正版）。按规划仓库
> `docs/change-and-ci-policy.md` 的"轻量补充流程"人工整理：变更说明 + 任务清单 + 验收条件
> （+ 后续 PR 检查证据留位），合并于本一份文档。**本方案未执行任何 Spec Kit 命令，未构建/
> 测试、未启动任何服务；本步仅编辑本文档并做只读源码核对。**
>
> **基线**：`main @ 2d170192a45a6c0751c6ac9a8cbeac4508ab8bad`；文档分支
> `docs/portfolio-demo`（本地，未推送）。`origin/main` 为本地缓存引用，不视为实时远端。
>
> **Spec Kit 指针纪律**：`.specify/feature.json` 当前指向
> `specs/015-backup-recovery-safe-resumption`；本方案**不**触碰该文件、不初始化/覆盖任何
> `.specify` 配置、不借同名斜杠命令把产物写进 015。若后续确需走工具流程，由 supervisor
> 单独裁决命令与目标。

## 1. 变更说明（范围）

**目标**：交付一个可实际复现的本地隔离演示：同一运行内完成
(a) 真实 ERC-20 充值：真实发起链上交易 → 观察 Pending → 达到显式 N>1 确认深度后 Confirmed；
(b) 合法提现：HTTP 创建 → 准入 → 真实 signer + worker 执行 → 链上真实付款效果；随后以
**同一 caller / 同一 idempotency_key** 重放，直接核对请求/意图/事件与链上效果不重复。

**边界表述（严格口径）**：
- 本演示**不是**"纯管理 CLI 端到端闭环"，也**不**声称"CLI 衔接已修"。授权环节存在
  **一行受控 SQL 配置例外**（`withdrawal_authorization_scopes`，第 8.4 节，含无 CLI 路径的
  完整论证）；除该行外，调用方/授权/策略/nonce 注册等配置全部经现有管理 CLI，业务结果
  （请求/意图/绑定/签名/广播/执行终态）**全部由真实生产路径产生**。
- "约 5 分钟"指**预备完成后的讲解/展示节奏**，不是冷启动总耗时承诺（首次拉取镜像、一次性
  编译工具获取、迁移等准备时间不计入）。
- 既有 e2e 夹具（日志发射器）**不得**被包装为完整 token，也不得被引为"真实 ERC-20 资产转移/
  余额正确性"证据。

**In scope（唯一新增物，全部为 demo 辅助/测试夹具，不改生产契约）**：

| 产物 | 路径（拟） | 性质 |
| --- | --- | --- |
| 演示编排脚本（setup/A/B/verify/exit） | `scripts/portfolio-demo/run_demo.sh` | demo 辅助，不入生产代码 |
| 端口隔离 overlay | `scripts/portfolio-demo/compose.portfolio-demo.yaml` | compose 覆盖（仅端口映射） |
| 真实 ERC-20 测试夹具（源码+ABI+creation/runtime+溯源） | `scripts/portfolio-demo/fixtures/` | 一次性本地 fixture |
| 本方案文档 | `docs/changes/portfolio-demo.md` | 本文件 |
| 运行证据（git-ignored） | `.evidence/portfolio-demo/<run-id>/` | 运行归档（public/private 分区，见 §10） |

**Out of scope / 明确不做**：
- 不修改生产 Go、数据库 schema/migration、认证/签名/状态机、`README.md`、`docs/portfolio.md`、
  既有脚本、`.specify/*`、`feature.json`；
- 不新增业务阶段、不重跑 001–015、不改业务契约；
- 首轮**不**启用 Kafka/Redis/events，**不**做恢复/重组/长测量/故障矩阵；
- 不执行 RESUME-01 中的"P1 多套测试组合"路线（已由 supervisor 裁决否决：非用户可见的真实充提演示）；
- 本轮**无** PR/CI：远程门禁与本地证据严格分离（见 §12）。

## 2. 用户授权与 supervisor 裁决记录

| # | 来源 | 裁决 |
| --- | --- | --- |
| D1 | **用户明确授权** | 允许**一次性获取固定版本官方 Solidity 编译器**；仅落入隔离工具目录，不做系统安装、不改生产依赖；来源与校验可追溯（§5.2） |
| D2 | **用户明确授权** | 允许在**新建隔离 demo DB** 中写入**一行**授权/费用 scope 配置；必须披露其**并非完整纯 CLI 授权闭环**（§1、§8.4）；业务结果仍全部由真实生产路径产生 |
| S1 | supervisor 裁决 | 不执行 RESUME-01 的 P1 测试组合；A/B 按"单隔离运行 + 同 request/intent"组织（§4） |
| S2 | supervisor 裁决 | token 必须真实（balanceOf/Transfer 可核）；不得用日志发射器包装（§5） |
| S3 | supervisor 裁决 | attempts **不恒为 1**；request/intent 与最终链上效果必须唯一（§4.2） |
| S4 | supervisor 裁决 | 破坏性操作禁止；只允许本轮自有进程正常退出与自有容器停止；卷/容器不删（§7 exit、§8） |
| D3 | supervisor 接受 | N=3 与默认端口（§6.2） |
| D5 | supervisor 裁决 | 链操作固定容器 cast（镜像 v1.8.1），**无宿主 cast 静默 fallback**（§5.3） |

## 3. 关键事实（RESUME-01 + 本步定向核验）

1. **A 场景**：单个隔离运行，真实链上交易 → Pending → 显式 N=3 深度 Confirmed。
2. **B 场景**：同一运行、同一 request、同一 intent；不得用"B1 恢复测试 + B2 不签名测试"跨运行推导。
3. **既有 e2e 夹具不是真实 token**：`e2eEmitTransferCode`（`internal/app/e2e_deposit_test.go:261-278`）
   与 `installRecoveryTransferEmitter`（`internal/app/e2e_recovery_worker_effect_test.go:690-706`）
   只是 `anvil_setCode` 安装 LOG3/STOP 日志发射器，无余额语义。
4. **仓库内无任何真实 token 源码/字节码**（全库 grep `pragma solidity`/`60806040`/`balanceOf`/`.sol`
   均 NOT FOUND）；宿主有 foundry `forge/cast/anvil` 1.7.1，**无 solc/solcjs、无 `~/.svm` 缓存、
   无宿主 psql**；`ghcr.io/foundry-rs/foundry:v1.8.1` 镜像本机已存在（本机缓存，非实时远端事实）。
5. **CLI 无法给真实 request 挂 execution scope**（源码核验）：
   - scope 只在**带 scope 参数的首次 supply** 随 grant 原子写入（`internal/withdrawal/grant.go:741-747`）；
   - 既有 grant 的 resupply 带 scope 且 scope 缺失/不等 → `operation_conflict` 零写入（`grant.go:763-779`）；
     scope 内容一经写入不可改写（`grant.go:139-153` 仅版本自增）；
   - `request_id` 服务端随机铸造（`internal/withdrawal/intake.go:419,662-670`），create 请求体无
     `request_id`（`internal/app/withdrawalhttp.go:95-104`；`specs/007-withdrawal-creation/contracts/api.md:13-23`）；
   - 011 准入要求 scope 行存在且 `scope.request_id == 路径 request_id`（`internal/execution/admit.go:139-163`）。
   - 结论：必须由一行配置类 SQL 补齐（§8.4 论证）。
6. **`withdrawal_authorizations` 无版本列**（`migrations/000007_withdrawal_creation.sql:118-134`）：
   列含 `authorization_id/caller_id/chain_id/asset/recipient/amount/state/expires_at/…`；scope 的
   `authorization_version` 是独立序列（`migrations/000010_withdrawal_authorization_scopes.sql:34,44`，
   CHECK ≥1），首个 scope 为 1、revoke/re-supply 周期自增（`grant.go` `scopeBumpVersionSQL`）。
   因此 §8.4 的版本必须**从真实 scope 状态派生**（max+1；首插=1），不得盲写。
7. **准入重放语义已核**（F6）：`AdmissionCreated=201 / AdmissionRecorded=200 / NotFound=404 /
   Conflict=409 / Refused=422`（`internal/execution/admit.go:16-22`）。执行重放**无 read-first 捷径**：
   走完整门禁（recovery→body→401→`CanExecute` 403→`Admit`）后 insert 触发唯一冲突 →
   `classifyAdmissionConflict` 同 basis 返回 **200**（`admit.go:257-289`；handler
   `internal/app/withdrawalexecution.go:101-110,166-176`），响应 `state` 回显既有 intent 当前状态；
   request 状态门要求 `accepted`（永久，`000007:105` CHECK）。完成态重放 200 的前提是
   grant 未撤销、scope 未改、registry 未禁用、无恢复/暂停——本演示不改这些；运行中仍须**实测**该 200。
8. **events 关闭时 outbox 仍被真实路径写入**：intake 的 `appendWithdrawalRequestReceivedEvent`
   无条件随请求事务写入（`intake.go:209-228,498`）；publisher/consumer/events-admin 在
   `TXHARBOR_EVENTS_ENABLED=false` 时拒绝启动（`internal/app/eventpublisher.go:66-70`、
   `eventconsumer.go:60-64`、`eventsadmin.go:65-69`）。故断言=**事务性 outbox 写入**，
   **非**事件交付演示（publish_state 恒 `pending`）。
9. **确认策略/充值配置惰性 bootstrap（无需 seed）**：首笔合格确认事务写
   `confirmation_policy_history`（`internal/indexer/confirmcommit.go:237-253,492-502`）；首个充值提交
   单元写 `deposit_config_history`（`internal/indexer/depositcommit.go:175-177,308-320,570-577`）。
   → B 的 007 白名单门（FR-05）要求 **A 先于 B**（同运行自然满足）。
10. **无 deposit HTTP 读接口**（`internal/app/serve.go:518-544`）→ 充值断言直查 PG。
11. **Docker 现场**（只读）：`docker context show` = `default`，endpoint = `unix:///var/run/docker.sock`；
    `DOCKER_HOST/DOCKER_CONTEXT/DOCKER_TLS_VERIFY` 均未设置。此为静态发现，运行期仍须按 §7 S0 守卫。

## 4. 需求与验收场景

### 4.1 验收场景 A（充值：观察 → 显式深度 Confirmed）
- A1 真实链上交易：对 allowlist 内**真实 ERC-20 合约**调用 `transfer(WATCH, X)`（真实余额变动）。
- A2 观察：003 日志行与 004 观察行（`deposit_observations.status='pending'`）由真实索引/识别产生（零 seed）。
- A3 深度 N−1（N=3）时仍 `pending`，确认列未写。
- A4 再挖 1 块（深度 N）后 `confirmed`：`confirm_threshold=3`、`confirmations>=3`、
  `confirm_policy_seq=1`；`confirmed_at` 等依据列非空。
- A5 链侧对账（不止单一余额）：`balanceOf(WATCH)` 增加 X、`balanceOf(SENDER_TOKEN)` 减少 X、
  `totalSupply()` 不变、交易回执成功且 `Transfer` 日志 (sender/recipient/amount) 匹配；
  `erc20_transfer_logs` 对应行存在。

### 4.2 验收场景 B（提现：真实付款 + 同身份重放不重复）
- B1 HTTP 创建：`POST /withdrawals`（Bearer）→ 201；`withdrawal_requests` 恰 1 行。
- B2 准入：`POST /withdrawals/{request_id}/execution` → 201；`payment_intents` 恰 1 行；`intent_id`
  等于 scope 声明值；**随后立即做一次未完成态重放**（同端点）→ 实测 200 且同 `intent_id`（见 §7 verify）。
- B3 真实执行：真实 `signer-serve`（dev）+ `withdrawal-worker` 完成 claim → nonce 绑定 → 签名 →
  广播 → 回执校验；`payment_intents.state='completed'`。
- B4 完成态重放：同 `idempotency_key` 同参数再 POST → 实测 200 且同 `request_id`；再 POST 准入 →
  实测 200 且同 `intent_id`（响应 `state` 回显当前状态）；重放后 `withdrawal_requests`/
  `payment_intents`/`nonce_bindings` 计数不变、`withdrawal.request.received` 不新增。
- B5 效果唯一性（链上直核，非仅单侧余额）：recipient 余额增加金额、sender 余额减少金额、
  `totalSupply()` 不变、恰一笔成功交易（唯一 tx_hash）且 Transfer topics/data 匹配、
  sender nonce 恰消耗一次；`tx_attempts` **不设为 1**（允许替换/重试），但每 attempt 至多一条
  send 行、同一 nonce、最终效果唯一。
- B6 有界观察：重放后保持本轮 worker 运行一个有界周期（≥3× worker 扫描间隔，默认 30s），
  期间断言无新增 attempt/binding/send/效果；随后停止**本轮自有** worker 并复检最终 DB/链稳定态。
  **不得把重放后即时快照当作"无重复"结论。**

### 4.3 负例（手段与成本）
| # | 负例 | 手段 | 期望 | 成本 |
| --- | --- | --- | --- | --- |
| N1 | 端口占用 | 预占 PG 端口后运行 | preflight 非零退出；未创建任何本 run 资源（按 project 标签查空） | 秒级 |
| N2 | 证据根/项目资源已存在 | (a) 预建同名证据根；(b) 预建同名 project 容器/网络/卷 | 在任何副作用前拒绝（两类都拒） | 秒级 |
| N3 | 危险 RPC | caller 注入 `TXHARBOR_RPC_URL`（任意值，含非回环）后运行 | **无连接即拒绝**（脚本不接受 RPC 覆盖输入；不做任何网络/daemon 调用） | 秒级 |
| N4 | 非默认/远程 Docker 上下文 | 注入 `DOCKER_HOST`/`DOCKER_TLS_VERIFY` 或使用非 unix-socket context 运行 | preflight 拒绝；不连接、不改全局配置 | 无 |
| N5 | 错误权限 | (a) 无 Bearer POST → 401（零审计、零请求行）；(b) `can_execute=false` 时准入 → 403 且 `payment_intents=0`；随后**用真实 CLI 恢复权限**（`withdrawal-exec permission-set --can-execute=true`）再走正常准入；(c) 同键异参 create → 409 原行不动、不新增 request 行（仅按表记 1 行 conflict 审计） | 状态码 + 直查 DB 计数（`withdrawal_requests` 保持 1） | 3 次 HTTP + 1 次 CLI |

## 5. 技术选择与依赖

### 5.1 运行栈
`PG(18.6-trixie) + Anvil(v1.8.1, chain-id 31337) + serve + signer-serve + withdrawal-worker`，
仅此五者；`TXHARBOR_EVENTS_ENABLED` 不设（默认 false）、`TXHARBOR_REDIS_ADDR` 不设 →
无 Kafka/Redis/限流/容量门（`docs/configuration.md:83-101`、`.env.example:154-155`）。

### 5.2 Token fixture（D1 已授权；F1 方案，含完整接口）
- **形态**：仓库内新写单文件最小标准 ERC-20（Solidity），接口 = `totalSupply()`、
  `balanceOf(address)`、`transfer(address,uint256)`、`allowance(address,address)`、
  `approve(address,uint256)`、`transferFrom(address,address,uint256)`；事件 =
  `Transfer(address,address,uint256)`、`Approval(address,address,uint256)`；
  constructor **一次性铸造固定供应量**给部署者；**无 mint 函数、无 owner/管理接口**；
  文件头与 PROVENANCE 注明 **demo fixture，禁止生产使用**。
- **编译器（已固定并实核）**：**solc 0.8.30** = `0.8.30+commit.73712a01`（linux-amd64 静态二进制），
  sha256 `f3e987dc…c428f7` 已与官方 `list.json` 条目（`releases["0.8.30"]` → 确切 path）核对一致；
  manifest 获取于 2026-10-11（`list.json` sha256 `ee1a2b48…97d6`），二进制 URL 与哈希记录在
  `fixtures/PROVENANCE.md`。manifest 自身真实性（仅 TLS/域名）未做独立交叉核对 → 保持"待核"，
  不编造官方 hash。
- **获取/校验路径（实现批次已落地）**：`compile_token.sh --fetch` 直连 `binaries.soliditylang.org`
  （HTTPS）下载到**隔离工具目录** `${TXHARBOR_DEMO_TOOLS_DIR:-/tmp/opencode/txharbor-demo-tools}`
  （仓库外、不进追踪文件），校验 sha256 后才允许执行；**不用 forge/svm 用户全局缓存、不系统安装、
  不改 PATH/全局配置、不成为产品依赖**；`run_demo.sh` **从不联网获取**。
- **固定编译设置**：`evm_version=cancun`、`optimizer=false`、`via_ir=false`、
  `metadata.bytecodeHash=none`、`appendCBOR=false`（固定输入 `fixtures/compile-input.json` 已提交）；
  编译产物 = `MinimalERC20.abi.json` + `creation.bin` + `runtime.bin` + `PROVENANCE.md`（全部哈希
  与编译命令），`--verify` 复算哈希并校验编译输入内嵌源码与 `.sol` 一致。
- **提交产物**：`MinimalERC20.sol`、`MinimalERC20.abi.json`、`MinimalERC20.creation.bin`、
  `MinimalERC20.runtime.bin`、`PROVENANCE.md`（含上述哈希与命令）。
- **部署**：用 **creation bytecode**（`cast send --create <creation_hex>`），不提交仅 runtime 却称可部署。
- 验证必须含 `balanceOf`/`totalSupply`/Transfer 对账（§4.1 A5、§4.2 B5）。

### 5.3 工具（D5）
链操作固定 `docker --context "$DOCKER_CONTEXT" compose exec -T anvil cast …`（镜像 v1.8.1）；
**不使用宿主 cast，无静默 fallback**（宿主 1.7.1 仅作静态发现记录）。PG 查询走
`… compose exec -T postgres psql …`（容器内本地连接，无需口令入 argv）。不引入 testcontainers、
Kafka/Redis 或任何生产代码改动。

## 6. 环境、配置与资源命名

### 6.1 全链必填配置（键名与来源）
- `serve` 共享必填（`.env.example:9-77`，`docs/configuration.md:20-34`）：`TXHARBOR_PG_DSN`、
  `TXHARBOR_RPC_URL`（**由 run 资源派生，不接受覆盖**，§7 S0）、`TXHARBOR_CHAIN_ID=31337`、
  `TXHARBOR_START_HEIGHT=0`、`TXHARBOR_LOG_START_HEIGHT=0`、`TXHARBOR_LOG_CONTRACTS=<TOKEN>`、
  `TXHARBOR_DEPOSIT_START_HEIGHT=0`、`TXHARBOR_DEPOSIT_CONTRACTS=<TOKEN>:0`、
  `TXHARBOR_DEPOSIT_WATCH_ADDRESSES=<WATCH>:0`、`TXHARBOR_CONFIRMATION_DEPTH=3`、
  `TXHARBOR_REORG_MAX_DEPTH=64`、`TXHARBOR_HTTP_ADDR=127.0.0.1:18081`。
- 发行方白名单：`TXHARBOR_AUTHZ_ISSUER_CALLERS=<ISSUER_CALLER_ID>`（`.env.example:101`；
  `internal/withdrawal/allowlist.go:17-22,31-60`；空=deny-all）。
- `signer-serve`（`.env.example:111-129`）：`TXHARBOR_SIGNER_MODE=development`、
  `TXHARBOR_SIGNER_KEY_FILE=<0600 key>`、`TXHARBOR_SIGNER_CHAINS=31337`、
  `TXHARBOR_SIGNER_SENDERS=<SENDER>`、`TXHARBOR_SIGNER_ASSETS=<TOKEN>`、
  `TXHARBOR_SIGNER_RECIPIENTS=<RECIPIENT>`；策略上限：`TXHARBOR_SIGNER_MAX_AMOUNT>=WITHDRAW 常量（本演示取 1e21，见 §13 偏差①）`、
  `MAX_GAS_LIMIT>=200000`、`MAX_FEE_PER_GAS/MAX_PRIORITY_FEE_PER_GAS/MAX_GAS_PRICE>=1e10`。
  Key 文件 = 32 字节 hex 文本（非 JSON，`internal/signer/provider.go:62-68,80-89`）。
- `withdrawal-worker`（`.env.example:133-134`，`docs/configuration.md:61-73`）：共享必填键之外
  `TXHARBOR_TX_SIGNER_URL=http://127.0.0.1:18091`、`TXHARBOR_TX_SIGNER_CREDENTIAL`
  （`signer-auth issue` 签发，前缀 `txs_`，`internal/signer/auth.go:25-27`；**不写入 env 文件**，
  进程启动时从 0600 文件读入变量，见 §8.2）。
- 不设置：`TXHARBOR_REDIS_ADDR`、`TXHARBOR_KAFKA_*`、`TXHARBOR_EVENTS_ENABLED`、
  `TXHARBOR_RECOVERY_CONTROL_DSN`。

### 6.2 资源命名与端口（D3 已接受；均可 env 覆盖）
- Compose project：`txharbor-demo-<run-id>`（run-id = UTC 时间戳 + PID）；卷
  `txharbor-demo-<run-id>_pgdata`；网络 `<project>_default`。**不复用**默认项目 `txharbor` 栈/`txharbor_pgdata`。
- 端口：PG `55436`、Anvil `58547`、HTTP `18081`、signer `18091`。
- 证据根：`.evidence/portfolio-demo/<run-id>/`（独占，见 §7 S0/§10）。

## 7. 阶段化命令设计（实现批次按此落地；本步未执行）

统一脚本骨架：`set -euo pipefail`（**禁止 `set -x`**）。

### S0 preflight（零副作用；顺序不可交换）
1. **捕获 caller 原始环境**（只记变量名与是否设置，不打印值）；`TXHARBOR_RPC_URL` 若由 caller
   设置 → **立即拒绝**（N3：无任何连接）。
2. **Docker 守卫（先检查、后净化；任何 daemon 操作之前）**：
   - `DOCKER_HOST`/`DOCKER_TLS_VERIFY`/`DOCKER_CERT_PATH` 任一非空 → 立即拒绝；
   - 解析 context 名（`DOCKER_CONTEXT` 若设置则用之，否则 `docker context show`——本地配置读取）；
   - `docker context inspect <name> --format '{{.Endpoints.docker.Host}}'` 必须为 `unix://…` 本地 socket；
     `tcp://`/`ssh://`/`npipe://` 等一律拒绝；**绝不 `docker context use`**；
   - 通过后 `export DOCKER_CONTEXT=<name>`；**此后每次调用显式 `docker --context "$DOCKER_CONTEXT" …`**
     （含 compose/ps/exec/port）；绝不向原 caller 远程 daemon 发任何写入。
3. **环境净化（守卫之后）**：`unset` 全部 `TXHARBOR_*`、`COMPOSE_PROFILES/COMPOSE_FILE/COMPOSE_PROJECT_NAME`；
   **不 source** 仓库 `.env`；compose 全程 `--env-file /dev/null`。
4. **资源预存在检查（拒绝在副作用前）**：`$PROJECT` 的容器（label `com.docker.compose.project`）、
   网络 `$PROJECT_default`、卷 `${PROJECT}_pgdata` 任一已存在 → 拒绝。
5. **证据根独占**：`mkdir -m 700`（失败即拒绝）；`private/` 0700。
6. **端口检查**：PG/Anvil/HTTP/Signer 四连检，占用即拒绝（N1）。
7. **compose config 渲染校验**：渲染 project 名 == `$PROJECT`。

### S1 setup（首个副作用起）
```bash
compose=(docker --context "$DOCKER_CONTEXT" compose -p "$PROJECT" --env-file /dev/null \
         -f compose.yaml -f scripts/portfolio-demo/compose.portfolio-demo.yaml)
"${compose[@]}" up -d --wait --wait-timeout 180 postgres anvil      # 只起这两个
# —— 所有权/身份核实（在 migrate / 链写入之前）——
# 容器：按 project+service 标签取唯一 ID（postgres/anvil 各 1）
# 端口：compose port postgres 5432 / anvil 8545 → 必须等于预期宿主端口
# 新 DB：information_schema 用户表计数 == 0（fresh）
# 新链：cast rpc eth_blockNumber == 0x0；chain-id == 31337；eth_accounts 非空
# 以上任一不满足 → 拒绝（不 migrate、不部署 token、不发交易）
$BIN migrate up && $BIN migrate status                 # $BIN=证据目录内构建的 txharbor
# Token 部署（F1 creation bytecode）→ 记录 contractAddress 与回执；cast call 验证 totalSupply/balanceOf
# 一次性 CLI 配置（全部现有管理 CLI）：
#   apikey-auth issue --caller-id <CALLER> …               (internal/app/apikeyauth.go:201-203)
#   apikey-auth issue --caller-id <ISSUER> …               (发行方；key 入 0600 文件)
#   withdrawal-exec permission-set --caller-id <CALLER> --can-execute=true …  (withdrawalexec.go:701-708)
#   signer-auth issue --caller-id <SIGNER_CALLER> …        (signerauth.go:193-197；凭据前缀 txs_)
#   signer-auth set-can-sign --caller-id <SIGNER_CALLER> --can-sign true …
#   nonce-admin mint；nonce-admin register --operation-id O --chain-id 31337 --sender <SENDER> …  (nonceadmin.go:76-83)
#   withdrawal-authz mint；withdrawal-authz supply --operation-id O --authorization-id <AUTH_ID> \
#     --caller-id <CALLER> --chain-id 31337 --asset <TOKEN> --recipient <RECIPIENT> --amount 1000 \
#     --api-key-file private/keys/issuer.key …             (withdrawalauthz.go:91-110)
# 启动三进程（各自 env 子集；日志→private/logs）：serve → signer-serve → withdrawal-worker
# 等待 serve /readyz；worker 无就绪面，以 DB 心跳/日志判活
```

### A 阶段（充值 → 显式深度）
```bash
CAST=(docker --context "$DOCKER_CONTEXT" compose -p "$PROJECT" --env-file /dev/null \
      -f compose.yaml -f scripts/portfolio-demo/compose.portfolio-demo.yaml exec -T anvil cast)
"${CAST[@]}" send --unlocked --from <DEPLOYER> <TOKEN> "transfer(address,uint256)" <WATCH> "$DEPOSIT" --rpc-url http://127.0.0.1:8545
# A 阶段不启动 worker、不发起其他链上写者（避免与深度判定竞态）
# 轮询（psql 只读）：erc20_transfer_logs 行 + deposit_observations(status='pending')
"${CAST[@]}" rpc anvil_mine 0x1   # 深度=2=N-1 → 断言仍 pending
"${CAST[@]}" rpc anvil_mine 0x1   # 深度=3=N   → 轮询 confirmed
# 链侧对账：balanceOf(WATCH) == $DEPOSIT；balanceOf(<DEPLOYER>) == $DEPLOYER_FINAL；totalSupply() 不变
# （$DEPOSIT/$FUND/$DEPLOYER_FINAL 等均为脚本内固定十进制常量，不做 bash 整数/浮点运算）
```
（门禁公式 `tip>=h && tip−h>=N−1`，`internal/indexer/confirm.go:89-95`；精确确认数 `max(0,tip−h+1)`，`:65-77`。）

### B 阶段（创建 → 准入 → 真实执行 → 重放）
```bash
curl --config private/curl/create.conf        # 0600：url/request/header(Bearer)/data；不含 argv 凭据
# → 201 {request_id: wr-…}；先断言 A 已写 deposit_config_history（FR-05 白名单门）
# [受控 SQL：§8.4 一行 scope 配置] → 然后：
curl --config private/curl/admit.conf         # POST /withdrawals/$REQUEST_ID/execution
# → 201 {intent_id: <SCOPE_INTENT_ID>, state: admitted, …}
curl --config private/curl/admit.conf         # 未完成态重放 → 实测 200 且同 intent_id（不得预设，见 §3.7）
# 此时才启动本轮 worker（配置/请求/scope/首次准入/一次同 intent 重放全部完成后；避免与重放竞态）
# 轮询 payment_intents.state='completed'（有超时；超时=失败）；若已出现 tx_hash 而状态未完成，
# 按有界次数显式 `cast rpc anvil_mine 0x1`（010 回执确认可能需要新块，不以首个 receipt 判 completed）
# 重放：create 同 body → 实测 200 同 request_id；admit 再发 → 实测 200 同 intent_id
# 有界观察（B6）：保持 worker ≥3×扫描间隔（默认 30s）→ 断言无新增；停自有 worker → 最终稳定态复检
```

### verify（直观测断言）
- 每条断言解析 psql/cast 的结构化输出并比较值；**日志/exit code 不作为 PASS 依据**。
- 固定只读查询清单（表/列来源）：`withdrawal_requests(request_id,caller_id,idempotency_key,status)`
  `migrations/000007_withdrawal_creation.sql:86-111`；`payment_intents(intent_id,request_id,state,state_version,authorization_id,authorization_version)`
  `migrations/000012_withdrawal_execution.sql:29-53`；`nonce_bindings(intent_id,nonce,state)`
  `migrations/000008_nonce_manager.sql:82-103`；`tx_attempts(state,nonce,attempt_id)`
  `migrations/000011_tx_lifecycle.sql:33-74`；`tx_attempt_signings(tx_hash)` `:122-137`；
  `tx_send_attempts(attempt_id,send_seq,outcome)` `:139-166`；`outbox_events(event_type,aggregate_id,aggregate_version,publish_state)`
  `migrations/000015_event_infrastructure.sql:36-75`；`deposit_observations(status,confirm_threshold,confirmations,confirm_policy_seq)`
  `migrations/000004_deposit_detection.sql:56-70` + `migrations/000005_confirmation_tracking.sql:43-58`。
  **清单不含 `signed_tx_bytes` 等原始签名材料。**
- 链侧：`cast receipt <tx_hash>` 成功、Transfer topics/data 匹配、`balanceOf` 双方、`totalSupply`、
  `cast nonce <SENDER>` 单次消耗。
- **失败纪律**：轮询全部带超时（超时=失败）；脚本聚合失败并以非零退出；**不吞关键失败后报 PASS**。

### exit（保留资源的安全退出）
```bash
kill -TERM $WORKER_PID $SERVE_PID $SIGNER_PID; wait   # 仅本轮自有进程正常退出
"${compose[@]}" stop                                  # 仅停止本轮容器；不 down、不 -v、不 prune、不 rm
# resources.txt（public/）：project、容器名/ID、卷名、网络名、证据根 + "未删除任何资源；清理需另授权"
```
重跑策略：新 run-id → 新 project/端口/卷/证据根；旧栈保持停止且不动。**任何删除/清理（含 `down -v`、
`make db-reset`、DROP/restore、prune、触碰默认栈或 7879 旧卷）另请授权**——本方案**不**调用、也不照抄
smoke 脚本的 `down -v --remove-orphans`（`scripts/quickstart-smoke/run_smoke.sh` 仅作隔离模式参考）。

## 8. 安全约束细则

### 8.1 环境与 Docker（F1/F2）
- 先检查后净化：原始 `DOCKER_*` 与 context endpoint 校验通过后才 unset；**绝不向原 caller 远程
  daemon 发写入**；不 `docker context use`；每次调用显式绑定已验证 context。
- RPC 不接受覆盖输入（N3 无连接即拒绝）；由 run 资源（容器 ID + port 映射）派生；
  chain-id 31337 不作为"新链"证明，另有 blockNumber==0 新鲜度校验（§7 S1）。
- 资源标签/容器 ID/新 DB 身份（空 schema）核实后才 migrate/seed/链写入；project/网络/卷预存在即拒绝。

### 8.2 凭据与脱敏（F3；替代旧 R2）
- 凭据策略：API key / signer credential（`txs_`）/ signer 私钥 一律 0600 文件、目录 0700；
  `--api-key-file` 优先（`withdrawalauthz.go:91-110`）；worker 凭据经 0600 文件→进程 env
  （产品契约为 env-only，`docs/configuration.md:61-73`）；HTTP 请求用 **0600 `curl --config` 文件**
  （url/header/data），避免 Bearer 进 argv，不引入新系统依赖。
- 证据分区：`public/` 仅放**过滤后的清单/统计/链 receipt/SQL 输出**（固定查询清单）；`private/` 放
  keys/env/curl 配置/完整日志。**env 文件含 DSN 开发口令，按敏感件处理，不得宣称"env 无凭据"**；
  不打包 keys/env/原始签名材料。
- 秘密扫描（**不打印命中内容**）：以本轮真实值（API key、`txs_` credential、私钥 hex、DSN 口令）
  对 `public/` 做 `grep -RFq`（逐值，只报文件名/计数）；另检 `public/` 不含 `signed_tx_bytes` 字样。
- 边界如实声明：同 UID 进程可读 `/proc/<pid>/environ` 与 0600 文件；该边界为本地演示**已接受**并
  记录，**不声称更强隔离**。

### 8.3 禁止行为
不 fetch/push/PR/merge/deploy；不删卷/容器/对象；不碰默认栈与 7879 旧卷；不 DROP/reset/restore；
不安装系统级工具；Anvil 仅新建本地 chain-id 31337，绝不接外部 RPC/旧钱包。

### 8.4 受控 SQL 配置（唯一写表例外；D2 已授权，须披露）
- **披露**：这是"CLI 管理配置 + 1 行受控 SQL 配置 + 其余全真实生产路径"的混合形态，
  **不是纯管理 CLI 端到端闭环**。
- **为何无 CLI**（§3.5）：scope 只能随"带 scope 参数的首次 supply"写入且事后不可改，而
  `request_id` 服务端随机铸造、create 请求体无该字段 → CLI 在任何时点都无法声明正确 `request_id`；
  resupply 补挂会被 `operation_conflict` 拒绝。
- **写表/条件（事务内、单语句、真实身份断言）**：
```sql
BEGIN;
INSERT INTO withdrawal_authorization_scopes
  (authorization_id, intent_id, request_id, sender,
   fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement,
   authorization_version, attested_by)
SELECT g.authorization_id, :intent_id, r.request_id, :sender,
       :fee_total, :fee_per_gas, :fee_priority, TRUE,
       COALESCE((SELECT max(s.authorization_version) FROM withdrawal_authorization_scopes s
                 WHERE s.authorization_id = g.authorization_id), 0) + 1,
       :attested_by
FROM withdrawal_authorizations g
JOIN withdrawal_requests r ON r.authorization_id = g.authorization_id
WHERE g.authorization_id = :auth_id
  AND g.state = 'active'
  AND r.request_id = :request_id
  AND r.caller_id = g.caller_id AND r.chain_id = g.chain_id
  AND r.asset = g.asset AND r.recipient = g.recipient AND r.amount = g.amount
  AND NOT EXISTS (SELECT 1 FROM withdrawal_authorization_scopes s2
                  WHERE s2.authorization_id = g.authorization_id)
RETURNING authorization_id;
COMMIT;
```
  - 断言：恰返回 1 行（0 行 → 失败；不重试为其他形态）；**无 `ON CONFLICT`、无 `UPDATE`**，
    既有 scope 存在时直接 0 行失败；
  - 版本：仅在"已验证 fresh run + 新 CLI 首次 supplied 且 grant `active` + 无既有 scope + 无
    revoke/reissue 变更"条件下首插为 1；`NOT EXISTS` 守卫使 `max+1` 实际恒为 1——**不得**把它
    宣称为"已验证的历史版本推导"；既有 scope 行存在则 0 行插入→失败，绝不 UPDATE；
  - 费用三元组满足 DB CHECK `fee_max_priority <= fee_max_per_gas`（`000010:43`）且 ≤ signer 策略上限；
  - 该行只是授权/费用上限配置，不构成资金事实；B1–B6 的请求/意图/绑定/签名/广播/终态与链上效果
    全部仍由真实路径产生并直观测。
- 实现批次须做"SQL 写语句清单"审查：除本行外全部 SELECT；`withdrawal-exec permission-set` 等权限
  配置仍走真实 CLI（N5 恢复即用 CLI）。

## 9. 任务清单（实现批次，最小）

| # | 任务 | 依赖 | 验收 |
| --- | --- | --- | --- |
| T1 | D1 落地：一次性获取 pinned solc 到隔离目录，记录版本/二进制 sha256（官方哈希交叉核对=待核） | 用户已授权 | 记录文件落盘 |
| T2 | 产出 `scripts/portfolio-demo/fixtures/`（源码/ABI/creation+runtime/PROVENANCE） | T1 | sha256 复算一致；可部署 |
| T3 | 写 `compose.portfolio-demo.yaml` + `run_demo.sh` S0/S1（F1/F2 守卫、身份/新鲜度、资源命名、退出） | — | N1–N4 负例通过；零副作用预检 |
| T4 | A 阶段（transfer→pending→N−1 仍 pending→N confirmed + A5 全对账） | T2,T3 | §4.1 全断言 |
| T5 | B 阶段（CLI 配置链、create、§8.4 一行、准入、worker 执行、重放、B6 有界观察） | T3,T4 | §4.2 全断言 |
| T6 | verify 层（固定查询清单 + cast 断言 + public/private 分区 + 秘密扫描） | T4,T5 | 断言可复算；public 无秘密 |
| T7 | 负例 N5 并入脚本（含 CLI 恢复权限；401/409 零新增 request 行断言） | T5 | §4.3 行 N5 |
| T8 | 文档同步（仅必要时在本文追加验收记录；不动 README/portfolio） | T4–T7 | 变更记录 |
| T9 | 完整端到端运行 + 证据归档 + resources.txt | T4–T7 | §10 路径完整、保留声明 |
| T10 | 实现批次差异审查与本地提交（等待 supervisor 授权） | T9 | 提交范围=demo 辅助文件 |

## 10. 验证证据路径（实现批次产出；public/private 分区）

```
.evidence/portfolio-demo/<run-id>/          (0700; umask 077)
  public/   (0700, files 0600)  summary.txt/json（过滤清单/统计）、manifest.txt（文件+sha256）、
                    sql/*.out（固定查询清单）、chain/*.out（receipt/balanceOf/nonce/totalSupply）、
                    compose-ps.txt、resources.txt
  private/  (0700)  keys/（0600）、env/（0600，含 DSN，敏感）、curl/（0600 config）、logs/
```
- 预检失败时允许留下该证据目录（仅诊断文件；**不创建任何 Docker/DB/链资源**）——如实表述为"无资源副作用"，不称"绝对零文件副作用"。
- 秘密扫描针对 `public/`（§8.2）；证据打包只含 public/（keys/env/curl/日志/原始签名材料不打包）。
- **PR/远程门禁口径**：本轮无 push/PR/CI，**远程 CI 状态 = NOT RUN**；本地证据**不替代**远程门禁，
  不以 diff 替代远程检查结果。后续如进入 PR，另按 `docs/change-and-ci-policy.md` 登记检查证据。

## 11. 剩余阻塞与下一实现批次检查预算

**剩余真正阻塞/待核**：
1. ~~solc 获取细节~~ **已完成**：0.8.30+commit.73712a01 已下载并对照官方 list.json 条目核对 sha256，
   隔离目录 `/tmp/opencode/txharbor-demo-tools`，编译产物 + PROVENANCE 已落盘（详见 §13）。
   仅"manifest 自身真实性（TLS/域名之外）"保持**待核**（不阻塞）。
2. **N1 端口占用工具**：宿主已静态确认存在 `python3`（可作占用进程）与 `curl`；负例脚本按此实现，
   若目标环境无 python3 再退化为最小监听器。
3. 其余无阻塞（D1/D2 已获用户授权；D3/D5 已裁决）。**实跑仍待 supervisor 审阅后授权（RESUME-04）**。

**检查预算（实现批次）**：
- 静态（本批已执行项见 §13）：`bash -n` 脚本、`compose config` 只读渲染（安全 context 先核、绝不 up）、
  fixture 全部 sha256 复算（`--verify`）、SQL 写语句清单审查、追踪文件秘密扫描。
- 运行（待授权，NOT RUN）：①1 次完整演示（A/B/两次重放/B6 有界观察，预计 ~15–25 分钟）；
  ②1 次负例运行（N1–N4 经 `--preflight-only`、N5 在完整 run 内，预计 ~5 分钟）；
  ③1 次新 run-id 重跑安全验证（预计 ~15–25 分钟）。
- 明确**不跑**：Go 测试套件、CI、fault/perf/drill、恢复/重组/长测量；不改产品代码（构建产物仅入
  ignored 工具/证据目录）。

## 12. Spec Kit 实际使用情况（如实声明）

- 仓库具备 Spec Kit 配置与命令定义（`.specify/init-options.json` = speckit 1.0.5；15 个
  `.opencode/commands/speckit.*.md`；模板见 `.specify/templates/`），但 **OMP 未调用任何同名斜杠
  工具**，也不声称其可用性由本步验证。
- 本方案 = **按 Spec Kit 轻量补充流程人工整理/修正**（变更说明 + 任务清单 + 验收条件），对应
  `docs/change-and-ci-policy.md`"补 CI、增加既有行为测试、调整构建工具且不改变业务契约"档；
  未新建业务阶段、未重跑 001–015、未写入 015 目录、未初始化/覆盖 `.specify` 配置。

## 13. 实现记录（RESUME-03；本地提交 SHA 见执行报告）

**交付文件**（`scripts/portfolio-demo/**`，全部为 demo 辅助/测试夹具，不改生产代码）：
- `run_demo.sh`（~1035 行）：preflight / setup / stage_a / stage_b / verify / run_all，
  `--preflight-only`、`--help`；PASS 仅在全部断言与清理（含 `compose stop`）成功后才输出。
- `lib.sh`（127 行）：断言、JSON/uint256 封装、端口探测、优雅停止等共享函数。
- `compose.portfolio-demo.yaml`（24 行）：仅端口覆盖（`!override`，55436/58547）。
- `democtl/`（Go 仅标准库）：`json get/count/require`、`uint256 eq/ge/hex`、`solc-manifest`；
  纯单元 10/10（含尾随 JSON、严格索引、base 规则、manifest 负例）。
- `fixtures/`：`MinimalERC20.sol`、`compile-input.json`（固定 standard-json 输入）、
  `MinimalERC20.abi.json`、`creation.bin`、`runtime.bin`、`PROVENANCE.md`。
- `compile_token.sh`：`--fetch`（一次性网络）/`--compile`/`--verify`。
- `README.md`（146 行）：入口、证据读法、N1–N4 负例命令、保留策略、5 分钟口径。

**编译 provenance（实核）**：solc `0.8.30+commit.73712a01`（sha256 `f3e987dc…c428f7`，对照官方
list.json `releases["0.8.30"]` 条目；manifest sha256 `ee1a2b48…97d6`）；settings = cancun /
optimizer off / viaIR off / bytecodeHash none / appendCBOR false；产物哈希见 `PROVENANCE.md`；
`compile_token.sh --verify` rc=0。

**本批静态检查**（证据：`.evidence/portfolio-demo-checks/03a/checks.log`、`03b/compile.log`、
`03b/static.log`）：democtl gofmt clean / vet rc=0 / 10 tests PASS；三脚本 `bash -n` rc=0；
compose 只读渲染 rc=0（name 正确、端口 55436/58547、无 5432/8545 残留、未创建任何资源）；
SQL 写面 = 仅 1 条 scope INSERT；危险模式扫描无实质命中；`--compile`/`--verify` rc=0。
**NOT RUN（待 RESUME-04 授权）**：完整 demo、N1–N4 负例、N5（在完整 run 内）、任何容器/服务/
集成/e2e 运行。

**实现偏差（已披露）**：①`TXHARBOR_SIGNER_MAX_AMOUNT` 取 1e21（=WITHDRAW），因 1e18 会低于
本演示提现金额而被 signer 策略拒绝（`internal/signer/policy.go`）；②token 部署先于 `migrate up`
（serve.env 需含 TOKEN/WATCH；两者仍严格在所有权/新鲜度门之后）；③`signer.env`/`worker.env`
复用 serve 必填键（`config.Load` 对 signer/worker 同样要求）。

## 14. 修复记录（RESUME-03-FIX：启动前独立验收 G1–G8）

在 `4efeea8` 之上追加提交（SHA 见执行报告）；未 amend、未重写历史。逐项：

- **G1**：①一次性 signer 增加 dev native ETH 资金（unlocked deployer 转账 1 ETH；断言 native
  balance ≥ MAX_GAS_LIMIT×MAX_FEE_PER_GAS=2e15；证据入 private/logs + public summary）；
  ②worker 凭据改由 `start_detached` 以 **export** 注入子进程（旧写法 `VAR=… exec` 不导出；
  mock 回归证明 export=HAS_SECRET、旧模式=NOSECRET，且日志不含秘密值）。
- **G2**：RUN_ID 限 `^[a-z0-9][a-z0-9-]{0,63}$`；evidence base 固定为 repo-ignored
  `$REPO_ROOT/.evidence/portfolio-demo`（父目录 symlink/归属校验、缺失时安全创建、run root 原子
  独占）；TOOLS_DIR 仅允许 `/tmp/opencode/txharbor-demo-tools` 或 repo 内固定目录（拒 symlink/
  任意外部路径）；构建入 per-run 目录；部署前先执行 `compile_token.sh --verify`（哈希+内嵌源码+
  固定 settings 校验）。
- **G3**：所有 curl `--disable --noproxy '*'` + 固定 connect/max-timeout；净化全部
  COMPOSE_*/DOCKER_*/proxy/CURL_*（DOCKER_CONFIG 拒绝）；PORTFOLIO_* 端口由脚本常量固定导出；
  render 校验升级为逐服务（name/端口集/镜像/命令/卷名）；existing 资源按标签+实际名双查。
- **G4**：不再 `compose stop`；仅停止 **project 标签 + `txharbor.demo.run_token` 标签**双匹配且
  在捕获 ID 集合内的容器（overlay 注入该标签）；无法证明归属即不停止并报告保留；进程只 TERM、
  超时不 KILL（保留并报失败）；所有 poll 检查进程存活；psql/cast/docker/curl 均有有限 timeout。
- **G5**：秘密扫描移至全部 public 文件落盘之后（rc=0 命中→FAIL / 1 干净 / 其他→扫描错误 FAIL）；
  不再向 stderr 输出 private 日志（只给路径）；public 导出 sql/http/chain（含 A 充值回执与 B
  signing 回执）+ run-metadata（HEAD/dirty/脚本与 fixture 哈希）+ run-exit；A/B 均校验
  status/from/to/block/log 地址/恰 3 topics/data/logIndex；B 另做固定观察 head 的 eth_getLogs
  唯一性（恰 1 条且 tx hash 匹配）；attempt 计数仅诊断，不再参与观测窗恒等；观测结论限定窗口。
- **G6**：scope INSERT 前置于同一事务的 `DO` 断言块（锁 grant+request；逐值核 caller/chain/
  asset/recipient/amount、有效期限、request accepted、无既有 scope、audit 恰一条 supplied 且无
  resupply/revoked）；version 使用 fresh 前置推导的字面 1（不再 max+1）；COMMIT 前恰 1 行，失败
  RAISE + ON_ERROR_STOP 回滚；费用上限与 signer 策略界在 shell 直接断言。
- **G7**：README 修正（标题=本地测试代币、非真实资金；输出顺序；Anvil 无状态持久化，仅保留
  容器/PG 卷/导出链证据；固定端口；NOT RUN 声明；新增 timeout 前置）。
- **G8**：compile_token 取消 `rm -rf` trap（临时目录保留于批准目录）；democtl 每次现构建；
  新增 `democtl solc-check` 完整解析 errors（任何 severity=error 或解析失败均拒绝，ABI/bin 存在
  也不放行）；`--verify` 增加固定 settings 与 compiler 字段核对；curl 同样加固。

**回归证据**：`scripts/portfolio-demo/checks/run_checks.sh`（mock Docker/curl/CLI，无 daemon/RPC）
83/83 PASS；日志 `.evidence/portfolio-demo-checks/03-fix/`。仍 **NOT RUN**：完整 demo 与真实负例。
实现偏差：FEE_TOTAL 由 1e16 收敛为 2e15（= MAX_GAS_LIMIT×MAX_FEE_PER_GAS，与 signer 费用界一致）；
render 端口集按真实行为固定为 {55436,58547}（profile 服务不参与渲染）。
