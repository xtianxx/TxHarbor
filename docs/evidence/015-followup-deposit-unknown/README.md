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
  （FIONREAD=0）→ 发 FIN 而非 RST；`SetLinger(0)` 强制 RST 从未回滚任何提交（180/180 落地）；唯一能 RST 的
  close-with-unread-data 场景（18 字节未读应答）发生在提交已完成之后（60/60 落地）。
- **真根因 = COMMIT 完成与立即重读的竞态**：重读在 COMMIT 写出后 0.65–0.97ms 完成，而行可见在 6.56–7.65ms
  之后（本地 miss 率 25–33%，三组各 60 次：immediate miss 20/15/19）；CI runner 较慢的 fsync 尾部把该竞态放大为
  一次性失败（测试 0.14s 内失败）。本地历史 PASS = 同一竞态被赢下。夹具注释承诺"the forwarded COMMIT still
  lands"在重读时序上不成立——**夹具缺陷，非产品缺陷**。
- **产品契约核对通过**（WP-B）：unknown 分支 `depositcommit.go:441-452`（never assume either way）+
  `commitResultVisible:479-509`（start/hash/next 三条件，写入者全枚举无"next==b+1 但单元未完成"路径）与
  specs/004 data-model FR-09「以数据库为准、幂等继续」一致。**生产代码零修改**。

## 修复（最小边界，仅测试侧）
- `internal/indexer/deposit_integration_test.go`：`TestDepositCommitUnknownOutcomeRereadsDB` 改用既有
  **read-side completion-drop 注入器** `logscanOpenCompletionDropPool`（转发完整 COMMIT，读侧扣住应答直到
  wire 上观察到 CommandComplete(COMMIT)+ReadyForQuery('I') 完成对才关连接）——"提交持久化、应答丢失"成为
  wire 证据保证的确定性前提，竞态被移除而非假设掉。计数断言改 `dropped==1 && achieved==1`（achieved 只在
  完成对处 +1，证明丢失真实成立；startup-packet 误匹配/bound 到期都会表现为 achieved==0 → fail-closed）。
- 业务断言逐条保持：checkpoint next==21、observations==1（无部分提交）、stale replay → errDepositVersionMismatch。
- `logscan_integration_test.go:1620`（同一 write-side 注入器的另一调用方）**不修**：其断言为收敛性
  （ServeLoop 对 commit 错误同高度重试，exact guard 幂等——首落则 errStaleState 重读、未落则重提交，
  两种结局都收敛），无 flake 面；超出本轮最小边界。

## 受控证据（正/反例与判别）
- 正例（修复后）：注入触发且完成对在线上被观察（dropped==1 && achieved==1）→ 权威重读确定性收敛 next==21、
  observations==1；重放拒绝语义不变。
- 反例面（fail-closed 保留）：注入器 not-achieved（ErrorResponse/EOF/bound 到期）→ 真实错误送达 →
  achieved==0 → 测试 Fatalf；`commitResultVisible` 对证据不足一律 (false,…) → 错误返回，conn closed 不作为成功。
- 旧失败机制受控复现：write-side 注入器 180 次默认组迭代复现 54 次 immediate miss（25-33%），与 CI 失败签名
  （pgx `conn closed` → progress unchanged）一致；本机 PASS 与 CI FAIL 均为同一竞态的不同结局。
- 确定性验证：修复后本机 6 次连续运行全 PASS（含 -race）。

## 检查
- gofmt 干净；`go build ./...` exit 0；`go vet -tags integration ./internal/indexer/` 干净；
- 本机定向：`TestDepositCommitUnknownOutcomeRereadsDB` 6/6 PASS（含 race）；相邻契约
  `TestT028CrashDrillAndRefork`/`TestLogScanCrashAndRestartRecovery`（含 :1620 子测试）PASS；unit `-race` ok。
- pinned+git 运行器一次适用 PG 回归（全 `internal/indexer` 包，CI=true、pinned 18.6 客户端、宿主 git 二进制
  挂载）：**go_test_exit=0，test 级 674 pass / 0 fail / 0 skip**（单次运行，不循环追绿）。运行树绑定：
  HEAD=`2fca0b05` + 1 个已跟踪修改（即本修复的工作树副本，只读挂载）；首次尝试（pgfix3）因容器内 git
  dubious-ownership 拒绝未能打印树头（测试未启动即退出，exit 1），改用 git -c safe.directory 取证确认
  树头（见 `pgfix3b.log` 与本说明）。

## 边界不变
- 不修 T026（其根因仍未知）；main run 37257158182 两次失败与 run 37274084298 本次失败记录保留；
- 无无条件吞错/盲目重执行/降证据/延超时/降并发/宽泛重试/skip；
- ADR-004 Proposed；生产部署 BLOCKED；T000-P OPEN。
