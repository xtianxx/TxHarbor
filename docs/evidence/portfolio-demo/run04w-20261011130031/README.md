# 简历演示 run04w 证据包（精选脱敏摘录；非原始记录）

- **性质**：本目录是**精选派生包**（derived excerpt）——从一次真实本地隔离运行的
  git-ignored 原始证据中，按白名单**逐字节复制** 22 个 public 文件，并新增本说明、
  `PROVENANCE.md` 与 `SHA256SUMS`。**不是**原始运行记录的替代品：完整原始树（含
  `private/`、全部日志与 4 个未复制的 public 文件）留在本地 ignored 位置
  `.evidence/portfolio-demo/run04w-20261011130031/`，见 `PROVENANCE.md` 的来源映射。
- **执行来源（run source）**：runID `run04w-20261011130031`，compose project
  `txharbor-demo-run04w-20261011130031`，run_token `ab9f084dc44409fbf7c2d19556b7af55`。
- **执行时 HEAD**：`11d698c75edf14e28e7075300ca082fe6dc9a308`（`docs/portfolio-demo`，
  `git_dirty_files=0`）。注意：该 SHA 是**运行时的源码版本**，**不是**收录本证据包的那次
  文档提交（文档提交只新增本目录与文字，不改任何被运行的字节）。
- **命令与时间**：`timeout 1200 env PORTFOLIO_RUN_ID=run04w-20261011130031 bash
  scripts/portfolio-demo/run_demo.sh`；UTC `2026-10-11T13:01:02Z → 13:03:14Z`（wall 132s）；
  `run_rc=0`、`exit_code=0`、`cleanup_rc=0`、`RESULT: PASS`。
- **被运行字节的指纹**（摘自 `run-metadata.txt`，均为仓库相对路径）：
  `run_demo.sh 29bfbfbb…d0a956`、`lib.sh 616423218…f96d6`、`compile_token.sh 5b35856d…361372`、
  `MinimalERC20.abi.json 5060bb41…34628c6`、`creation.bin 37f500d7…5c76041`、
  `runtime.bin 6c0a8d33…604c8bd`、`MinimalERC20.sol 136f4d82…9312e8f`、
  `PROVENANCE.md 4e6332a9…f737d72`、`compile-input.json 47d98f22…536a803`。

## 1. 结果（一句话）

同一次运行内：**A** 真实 ERC-20 充值 `pending` → 深度 `N-1` 仍 `pending` → `N=3 confirmed`；
**B** 真实提现（201 创建/准入 → 真实 signer 签名 + worker 广播上链完成 → 同 caller/同
idempotency_key 重放 200 且身份/计数/链效应不变 → 30s 有界窗口 + 停 worker 后快照稳定）；
**N5** 401/403/409 拒绝且不误增；**最终 verify** 全部字段级断言（含 raw-topic `eth_getLogs`
唯一付款与回执同一 log 一致）通过；public secret 扫描通过；退出/清理码 0。

## 2. 文件清单与读法

| 文件 | 字段/内容 | 用途 |
| --- | --- | --- |
| `chain/deposit-receipt.json` | 充值 tx 回执（blockHash/number/from/to/logs…） | A 阶段链上事实 |
| `chain/signing-receipt.json` | 签名付款 tx 回执 | B 阶段真实付款事实；与下方 getLogs 同一 log |
| `chain/transfer-log-query.filter.txt` | `from_block=0`、`to_block=9`（固定 observed head）、token、**topic0/topic1/topic2 为 32-byte raw topic** | 唯一付款查询的**实际 argv 口径** |
| `chain/transfer-log-query.json` | 该查询的原始 JSON 输出（恰 1 条） | 唯一 log 的 address/3 topics/data(`…3635c9adc5dea00000`=1000e18)/`removed`=JSON 布尔 false/txhash/blockhash/blocknumber/logindex |
| `chain/final-state.txt` | `observed_head/total_supply/balance_*`/`signer_nonce` | 运行末链上状态（余额与总供应守恒） |
| `http/create-201.json` / `create-200-replay.json` | 同 `request_id`（`wr-2182190d…`） | 创建幂等（同 caller+key 重放） |
| `http/admit-201.json` / `admit-200-unfinished.json` / `admit-200-replay.json` | 同 `intent_id`（`intent-portfolio-1`） | 准入/未完重放/完成态重放同一意图 |
| `http/admit-401.json` / `admit-403.json` / `create-409.json` | 拒绝响应体 | N5 负例（未认证/无执行权限/同 key 异额） |
| `sql/deposit-pending.out`、`sql/deposit-depth-n1.out` | `status\|block_number\|confirm_tip_number\|confirm_tip_hash\|confirmations\|threshold\|policy_seq\|confirmed_at`（后段空） | 深度 `N-1` 仍 `pending` |
| `sql/deposit-confirmed.out` | 同 8 字段（`confirmed\|4\|6\|0x6e671bb3…\|3\|3\|1\|…`） | `N=3` 确认；**第 4 字段是 `confirm_tip_hash`（确认尖端），不是充值 tx 的 block hash**——后者在 `deposit-receipt.json` 与 run 内 DB 断言中 |
| `sql/b-counts-before-replay.out` / `b-counts-after-replay.out` | `requests\|intents\|bindings\|outbox_rows` = `1\|1\|1\|6` | 重放前后计数不变 |
| `sql/b-window-before.out` / `b-window-after.out` / `b-window-after-stop.out` | `requests\|intents\|bindings\|outbox(request.received)\|state_changed events\|recipient balance\|signer nonce` = `1\|1\|1\|1\|3\|1000000000000000000000\|1` | 30s 窗口前/后与停 worker 后**逐字节相同** |
| `sql/b-outbox-aggregate-versions.out` | `aggregate_version\|event_type`（3 行） | Outbox 写入版本（**写入**，非投递） |

**重复哈希是事实而非打包错误**：`admit-201.json == admit-200-unfinished.json`、
`create-201.json == create-200-replay.json`、`b-counts-before == after`、
`b-window-{before,after,after-stop}`、`deposit-pending == deposit-depth-n1` —— 这正是
"重放/窗口/深度不变"的直接证据（逐字节相等）。

## 3. 未收录的内容（有意排除）

- `private/`（API keys、signer 凭据、开发私钥、DSN 密码、curl 配置、全部服务日志）；
- 运行 stderr、Anvil 日志；
- 4 个 public 文件**按精选白名单未复制**（本包只收 `chain/*.json`、`chain/*.txt`、`http/*.json`、
  `sql/*.out` 四类），只以 sha256 记录于 `PROVENANCE.md` 供本地追溯。各自的实际原因不同：
  `summary.txt` 与 `resources.txt` 含**证据根主机绝对路径**；`run-metadata.txt` 含宿主本地工具
  目录的绝对路径（`/tmp/…`）；`run-exit.txt` **不含任何绝对路径**，纯按白名单未收（其
  退出/清理码已摘入本 README 第 1 节）。

## 4. 完整性与校验

- `SHA256SUMS` 覆盖**本目录全部 22 个 raw 文件 + `README.md` + `PROVENANCE.md`**，并
  **排除自身**（避免自引用循环；`sha256sum -c SHA256SUMS` 逐行可复算）。
- 每个 raw 文件的 sha256 与源文件**逐一核对相等**（22/22）；新增说明文件为派生文本。
- 提交前已用该轮运行自己的 `private/scan-patterns.txt`（0600，程序内、只输出布尔/路径）
  对本包与待提交 diff 扫描：无命中；源 22 文件亦无 `/home` 路径或操作者标识。

## 5. 边界（不夸大）

- 本地隔离演示（fixture ERC-20、Anvil 开发密钥、仅 localhost 绑定、一次性 PG）；**非生产**，
  不代表生产 SLO/吞吐/上线状态。
- 主会话已对本次运行做独立验收（完整 A/B/verify、26 个 public 文件、metadata 源哈希、
  退出/清理码、资源对照）；本包是其**精选子集**。
- N1–N4 负例在本轮**未运行**（NOT RUN），不因本包出现而视为新 PASS。
- 历史失败**不抹**：`run04p`/`run04v` 等失败记录、`run04v` 最终 verify FAIL 及其 raw getLogs
  未留存、2 次 `--rm` 诊断、≥7 个 PG 隐式删除、R6 partial(`query_rc=3`) 见
  `docs/changes/portfolio-demo.md` §17–§20。
- `outbox_events` 全部 `publish_state=pending` = 事务性**写入**，不是事件投递；
  `Confirmed` 是索引确认状态，**不是记账/入账**；`tx_attempts=1` 仅本 run 诊断口径。
