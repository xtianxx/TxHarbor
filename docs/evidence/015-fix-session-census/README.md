# 015 修复轮：F2 重建探针退出同步（run 37257158182 follow-up）

## 范围与基线
- 基线：main@`883f1c025bdeec8c4f8dfe6b9ee5745066b674fd`（PR #38 squash 合并提交）。
- 分支：`015-fix-rebuild-session-census`（独立补充分支，从核实后的 origin/main 创建）。
- 触发：main push CI run 37257158182 attempt 2，`TestRestoreInterruptionNotRestoredAndRerunIdempotent` 失败于
  `internal/recovery/backup_integration_test.go`（当时 :1429）：`recreated target is not empty of sessions: count=1 err=<nil>`。
- T026（indexer，attempt 1 失败）保持历史原因未知，不在本轮。

## 根因（机制证实 + 本地复现）
`pgx.Conn.Close` 是客户端本地关闭：发协议 Terminate → flush → 关 socket，不读 EOF、不等服务端确认
（pgx v5.11.0 `conn.go:302-309`、`pgconn/pgconn.go:736-765`）。PG 后端在自身 `proc_exit` 退出钩子
`pgstat_beshutdown_hook` 中才把 `st_procpid=0`（REL_18_STABLE `backend_status.c:246,500-515`），此后该行才从
`pg_stat_activity` 消失（`backend_status.c:811,884-886`）。因此 Close 返回 ≠ 后端已退出 ≠ 统计行已消失。

受控存在性实验（pinned `postgres:18.6-trixie`，300 次 Close→立即观察）：**16/300（5.33%）观察窗内 count=1**，
最大滞后 28.14ms（25ms 轮询上界），无异常、无超时。见 `zz_wpa_probe_exit_window.json/.tsv`、`wpa_probe_exit_run.log`。

会话身份清单（排除恢复连接/旧写者泄漏）：DROP 前普查已零会话 + DROP 无 FORCE 成功 ⇒ 中断尝试的后端已排空；
CREATE 与普查之间唯一连过 target 的连接是测试自身的空基线探针 `fresh`（pgx.Connect→pg_class 计数→Close）。
count=1 = 自家探针的退出可见窗口，非生产生命周期缺陷。

## 修复（最小边界，仅测试侧）
- `internal/recovery/backup_integration_test.go`：
  - 探针连接后捕获自身身份 `(pg_backend_pid(), backend_start)`；
  - Close 后新增 `bkpAwaitProbeExit`：有界（10s deadline / 25ms 间隔，ctx 派生 ticker）等待**该二元组**从
    `pg_stat_activity` 消失 —— 只等自家探针，绝不按角色/application_name 宽泛排除、绝不杀未知会话；
  - 最终对整个 target 的零会话普查**逐字节不变**；失败/超时诊断附 `bkpTargetSessionDump`
    （pid/backend_start/application_name/usename/state/wait_event/query）。
- 判别力测试 `internal/recovery/bkp_probe_exit_integration_test.go`：
  - 正例 `TestBkpProbeExitPositiveZeroCensus`：探针退出后等待返回、普查确零；
  - 负例 1 `TestBkpProbeExitForeignSessionRefusedByCensus`：空 application_name 的外来会话（旧写者形态）必须仍被普查拒绝，转储可识别；
  - 负例 2 `TestBkpProbeExitAttemptTaggedSessionStillRefused`：`txh015_<hex>` 形态标签会话必须仍被拒绝（无名字白/黑名单）；
  - 负例 3 `TestBkpProbeExitWaitIsBoundedAndDiagnoses`：故障注入永不退出的后端身份 ⇒ 有界超时 + 诊断命名二元组与会话清单。

## 验证
- `go build ./...` + `go vet ./internal/recovery/` + `go vet -tags integration ./internal/recovery/`：干净。
- 本机（Docker）：判别力 4 测试 PASS（16.5s）；`go test -race -count=1 ./internal/recovery/` ok。
- pinned 容器运行器（root、CI=true、pinned 18.6 客户端）：定向 `pgfix1` ——
  `TestRestoreInterruptionNotRestoredAndRerunIdempotent` PASS + 判别力 4 测试 PASS（`pgfix1_focus.log`）。
- 完整 PG 集成一次（全仓 `./...`，CI 同并发条件，pinned 运行器）：test-level pass=3901 fail=7 skip=3；
  **015 相关面全部 PASS**（`internal/recovery`、`internal/recovery/controlstore`、`internal/app/recoveryadmin`）。
  7 个失败全部在 015 范围外且均有历史同类（详见 `pgfull4_summary.txt`）：3×`*MigrationHistoryUntouched`
  为运行器容器 PATH 无 `git`（环境缺口，与 main attempt1 同类）；events `TestPublisherCrashPointMatrix/published_before_ack`
  为已知 Kafka crash-point 时序 flake；signer `TestSignerLegalPathH5/expired_grant...` 为孤立时序 flake
  （`error <nil> is not a *signer.RefusalError`，与本分支无变更交集）。按授权仅此一次完整运行，不循环重跑。

## 边界不变
- 不改生产代码（`attemptproof.go requireEmptyTargetCensus` 的调用时机在 tagged quiescence 之后，无此竞态；本轮未动）。
- 无 sleep/无界轮询/无断言删除/无按名排除/无杀会话/无超时延长/无并发下调。
- ADR-004 Proposed；生产部署 BLOCKED；T000-P OPEN。
