# 015 后续轮：deposit unknown-COMMIT 测试夹具修复（main run 37274084298 follow-up）

## 范围与基线
- 基线：main@`2fca0b05b1f21944388ef32d1e1ec71ed98c203b`（PR #39 squash 合并提交）。
- 分支：`015-followup-deposit-unknown-commit`（独立补充分支）。
- 触发：main push run 37274084298（attempt 1）PG job 失败于 `TestDepositCommitUnknownOutcomeRereadsDB`
  （`internal/indexer/deposit_integration_test.go`）：`uncertain commit = commit deposit unit [10,20]
  (outcome unknown, progress unchanged): conn closed`。

## job 计数校准（更正此前报告）
run 37274084298 共 **10 job = 5 success / 3 skipped / 2 failure**（此前报告误写"6 success"）：
success=lint/build/unit(+race)/contract/changes；skipped=integration(Redis/Kafka)/e2e（路径层选择预期裁剪）；
failure=integration(PostgreSQL)/ci-required（受后者聚合）。

## 根因（受控实验定案，双代理并发 + 780 次武装提交）
- **触发帧实证**：注入在 simple-protocol COMMIT 帧 `Q\x00\x00\x00\x0bcommit\x00` 上触发（pgx v5 对无参
  语句走 simple protocol；事务内其余 SQL 均不含小写 "commit" 子串）——触发点正确，720/720。
- **RST 回滚假设被证伪**（WP-A，`phase1_close_semantics.json`/`phase2_*`）：默认路径 close 时客户端接收队列为空
  （FIONREAD=0）→ 发 FIN 而非 RST；`SetLinger(0)` 强制 RST 从未回滚任何提交（180/180 提交完成且可见）；唯一能 RST 的
  close-with-unread-data 场景（18 字节未读应答）发生在提交完成之后（60/60 提交完成且可见）。（本机受控数据；"可见"口径见下"证据边界"。）
- **本地已证根因 = COMMIT 完成与立即重读的竞态**：重读在 COMMIT 写出后 0.65–0.97ms 完成，而行可见在 6.56–7.65ms
  之后（本地 miss 率 25–33%，三组各 60 次：immediate miss 20/15/19；run1/run2 原始日志本轮未归档，仅 run3 逐次
  JSONL 在档 `phase2_iterations.jsonl`）。夹具注释承诺 "the forwarded COMMIT still lands" 在重读时序上不成立——
  **夹具缺陷，非产品缺陷**。
- **CI 归因限定（[INFERENCE]）**：CI run 37274084298 当次仅记录 pgx `conn closed` + `progress unchanged`（测试
  0.14s 内失败），**无当次时延/可见性测量**；"CI runner 较慢的 fsync 尾部放大该竞态"与"本地历史 PASS = 同一
  竞态被赢下"均为 **[INFERENCE]**（缺当次证据），不作为已证事实。
- **证据边界（fsync=off 容器）**：本目录集成数据取自 testcontainers PostgreSQL（模块默认 `postgres -c fsync=off`，
  仓库仅追加 `-c max_connections=…`）。"committed/可见"= 目标事务提交完成且可被其他连接观察，**不证明断电/
  介质耐久性**（非 durable/WAL 落盘断言）。
- **产品契约核对通过**（WP-B）：unknown 分支 `depositcommit.go:441-452`（never assume either way）+
  `commitResultVisible:479-509`（start/hash/next 三条件，写入者全枚举无"next==b+1 但单元未完成"路径）与
  specs/004 data-model FR-09「以数据库为准、幂等继续」一致。**生产代码零修改**。

## 修复（最小边界，仅测试侧）
- `internal/indexer/deposit_integration_test.go`：`TestDepositCommitUnknownOutcomeRereadsDB` 改用既有
  **read-side completion-drop 注入器** `logscanOpenCompletionDropPool`（转发完整 COMMIT，读侧扣住应答直到
  wire 上观察到 CommandComplete(COMMIT)+ReadyForQuery('I') 完成对才关连接）——"提交完成（fsync=off 容器，
  见证据边界）、应答丢失"成为 wire 证据保证的确定性前提，竞态被移除而非假设掉。计数断言改
  `dropped==1 && achieved==1`（achieved 只在完成对处 +1，证明丢失真实成立；bound 到期/未确认一律 achieved==0
  → 测试 Fatalf，fail-closed）。本测试 DB 名 `idx_t_testdepositcommitunknownoutcomerereadsdb_99` 含小写
  "commit"（startup-packet 误匹配面存在），该面同样由 achieved==0 fail-closed 兜底。
- 业务断言逐条保持：checkpoint next==21、observations==1（无部分提交）、stale replay → errDepositVersionMismatch。
- `logscan_integration_test.go:1620`（同一 write-side 注入器的另一调用方）**不修**：其断言为收敛性
  （**[引用修正]** 调用链在 **`logscanner.go`**：`commitLogRange` 的 COMMIT 错误 → `ServeLoop` 退避后重试 →
  `loadLogStateRetry` 重读持久状态＋配置检查，exact guard 幂等——首落则 errStaleState 重读、未落则重提交，
  两种结局都收敛），无 flake 面；超出本轮最小边界。
- 守卫边界保留、不可推广：上述收敛依赖协调锁、owner/fencing/expiry、身份/版本裁决等既有守卫；**不得**推广为
  未知付款的盲目重执行。

## 受控证据（覆盖层级标注）
- **已运行正例（修复后）**：注入触发且完成对在线上被观察（dropped==1 && achieved==1）→ 权威重读确定性收敛
  next==21、observations==1；重放拒绝语义不变（`TestDepositCommitUnknownOutcomeRereadsDB`，收录于
  `archive/indexer.jsonl.gz`）。
- **已运行产品负例**：`TestDepositCommitUnknownOutcomeVerdicts/commit_never_reached_db`——COMMIT 未达
  PostgreSQL 时 unknown 分支返回显式错误（回读证明 progress 未变、无部分提交），已运行并 pass
  （见 `archive/indexer.jsonl.gz`）。
- **已运行协议夹具覆盖**：注入器 not-achieved（ErrorResponse/EOF/bound 到期）→ 真实错误送达、achieved==0 的
  协议级判别由 `TestLogScanCompletionDropProtocolBoundary`（net.Pipe，已运行 pass）覆盖；它**不能替代**产品
  unknown 失败负例（两者层级不同）。
- **路径分析（非已运行反例，仅静态路径枚举）**：重读自身失败（`depositcommit.go:441-452`，rerr!=nil 返回错误）
  与保守假失败（`commitResultVisible` 身份/next 交错证据不足 → (false,…)）；`commitResultVisible` 对证据不足
  一律 (false,…) → 错误返回，conn closed 不作为成功。
- 旧失败机制受控复现：write-side 注入器 180 次默认组迭代复现 54 次 immediate miss（25-33%），与 CI 失败签名
  （pgx `conn closed` → progress unchanged）一致；本机 PASS 与 CI 单次 FAIL 的关联归因同上为 **[INFERENCE]**。
- 确定性验证：修复后本机 6 次连续运行全 PASS（含 -race）。

## 检查
- gofmt 干净；`go build ./...` exit 0；`go vet -tags integration ./internal/indexer/` 干净；
- 本机定向：`TestDepositCommitUnknownOutcomeRereadsDB` 6/6 PASS（含 race）；相邻契约
  `TestT028CrashDrillAndRefork`/`TestLogScanCrashAndRestartRecovery`（含 :1620 子测试）PASS；unit `-race` ok。
- pinned PG 回归两次运行器尝试（全 `internal/indexer` 包，CI=true、pinned 18.6 客户端），分记如下：
  - **pgfix3 = 运行器前置失败，不计测试成败**：容器内无 git 二进制（`git: not found`）→ 树头取证等元数据
    前置步骤失败，**测试未启动**；完整日志归档 `archive/pgfix3.log`。
  - **pgfix3b = 测试成功**：元数据命令遇 `dubious-ownership` → 以 `git -c safe.directory` 补取树头后测试正常
    执行：**go_test_exit=0，test 级 674 pass / 0 fail / 0 skip**（单次运行，不循环追绿）。口径：674 = **312
    顶层 + 362 子测试**（父/子分别计数，`Action∈{pass,fail,skip}`），独立重算见 `archive/indexer.jsonl.gz`
    与 `archive/ARCHIVE.md`（来源/哈希/重算清单）。完整日志归档 `archive/pgfix3b.log`（含 `go_test_exit=0`）。
- 运行树对应（**事后核对边界**）：运行当时工作树 = HEAD `2fca0b05` + 1 个已跟踪修改（本修复的工作树副本，
  只读挂载），但**运行当时未持久化精确树快照**；现存绑定证据均为事后——`safe.directory` 取证（输出未持久化
  留存）与事后 diff blob 匹配 `7194719..e19dbd8`——属事后核对，不可当作运行当时快照。

## Review P2 回应（逐项收敛，共 6 项）
- **P2-1（README 两次运行混写）**：已拆分为 pgfix3 = 容器内 `git: not found`、运行器前置失败、测试未启动、
  不计测试成败；pgfix3b = 元数据 `dubious-ownership` → `safe.directory` 补取 → 测试成功（`go_test_exit=0`）。
- **P2-2（pgfix3b 归档缺完成态 + 674 口径）**：完整日志已归档 `archive/pgfix3b.log`（含 `go_test_exit=0`）；
  674 = 312 顶层 + 362 子测试（父/子分别计数），重算清单见 `archive/ARCHIVE.md`、逐次 JSONL 见
  `archive/indexer.jsonl.gz`。
- **P2-3（CI 归因越界）**：受控数据只证明本地"提交完成 vs 立即重读"竞态（重读 0.65–0.97ms vs 行可见
  6.56–7.65ms，miss 25–33%）；CI run 37274084298 当次仅记录 `conn closed` + `progress unchanged`，无后续
  可见性/时延测量——"CI 慢 fsync 尾部放大""本地历史 PASS = 同一竞态被赢下"均降为 **[INFERENCE]**。
  phase3 = 原始注入器在生产路径 60 次**成功**（正例），非失败复现。
- **P2-4（耐久性表述限定）**：容器 fsync=off（testcontainers 默认；仓库仅追加 `max_connections`）——完成对只
  证明"目标事务提交完成且可被其他连接观察"，不证明断电/介质耐久；注释与本文 durable/WAL 表述均按此限定。
  本测试 DB 名 `idx_t_testdepositcommitunknownoutcomerereadsdb_99` 含小写 "commit"（startup 误匹配面存在），
  由 achieved==0 fail-closed 兜底。
- **引用修正**：`:1620` 收敛/重试调用链引 **`logscanner.go`**（`commitLogRange` COMMIT 错误 → `ServeLoop`
  退避 → `loadLogStateRetry` 重读＋配置检查），非 `scanner.go` 的 `loadProgressRetry`；保留守卫边界（协调锁、
  owner/fencing/expiry、身份/版本裁决）；不可推广为未知付款盲目重执行。
- **覆盖层级标注**：重读自身失败与 `commitResultVisible` 保守假失败 = **路径分析**（未运行反例）；注入器
  not-achieved→Fatalf = **协议夹具覆盖**（`TestLogScanCompletionDropProtocolBoundary`，已运行；不能替代产品
  unknown 失败负例）；产品负例 = `TestDepositCommitUnknownOutcomeVerdicts/commit_never_reached_db`（已运行）。

## 边界不变
- 不修 T026（其根因仍未知）；main run 37257158182 两次失败与 run 37274084298 本次失败记录保留；
- 无无条件吞错/盲目重执行/降证据/延超时/降并发/宽泛重试/skip；
- ADR-004 Proposed；生产部署 BLOCKED；T000-P OPEN。
