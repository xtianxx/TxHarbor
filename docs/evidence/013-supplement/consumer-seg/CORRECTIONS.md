# 013 消费者追赶分段测量：Review 闭合说明（#1–#8）

本文件记录 2026-10-08 只读 review 表 #1–#8 的闭合（闭合表行号即 review 表行号）。
边界：不改动任何原始记录（`runs/**`、`logs/**`、`environment.txt`、`source_fingerprint.txt` 字节不变，
核验见 `checks/raw_preservation.log`）；不重跑 10 轮/10k 批次与旧 smoke；不推送、不建 PR、
不改 CI/生产参数、不部署。本轮全部核验日志在 `checks/`（命令、退出码、真实输出；均为本轮新增，
不冒充历史执行日志）。

## 1. 闭合表

| # | 发现（review 摘要） | 本轮修正 | 证据 |
|---|---|---|---|
| 1 | runner 复用已有批次目录时先重写 `environment.txt`/`source_fingerprint.txt` 再拒绝；换 plan 可绕开逐轮 label 检查混入新轮 | `scripts/consumerseg/run_batch.sh`：在任何写入前以单次 `mkdir`（无 `-p`）**原子独占创建**批次根；已存在路径（空目录、数据目录、普通文件、符号链接含悬挂）一律拒绝、退出码 2、不写不清理；plan 校验前置；删除不可达的逐轮覆盖检查与 summary append 分支 | 新回归 `scripts/consumerseg/run_batch_guard_test.sh` 8/8 PASS（含旧脚本缺陷复现：两见证文件被重写后 rc=2）；`checks/fixture_root_guard.log`/`.rc` |
| 2 | 分析器 `PreEpochSpans` 分支未置 `OverlapOK=false`，结构化 checks 与 details 不一致 | `scripts/consumerseg/analyze/analyze.go` 一行修复；新增 `TestPreEpochSpanFailsOverlapCheck`（窗内零时长 span 仍正确归 full；真实零值与缺失哨兵处理不变） | `checks/analyzer_pre_epoch_before.log`（rc=1，修复前 FAIL）→ `checks/analyzer_pre_epoch_after.log`（rc=0）；`checks/analyzer_pkg*.log`（含 -race） |
| 3 | README「process 贡献 99.3–99.5%」与原始记录不符 | 改为 **99.09–99.28%**（窗内覆盖/tail 分数并列），并标注与 §3.2 全程 10000 span 分位口径不同、不得混用 | README §3.3/§4；`checks/regen_analysis.log` |
| 4 | README「poll+lag+mark 合计 ≤0.7%」不符 | 删除 ≤0.7%，改为逐轮 **0.649/0.841/0.705/0.740%**（r02/r05/r06/r09，含分数） | README §4 |
| 5 | README 把尾窗 poll 覆盖（20–81ms）标为「总窗内」 | 两窗分列：尾窗 `[publish_done,poll_confirm]` 20497/80829/36809/61020µs；总窗 `[drain_start,poll_confirm]` 414068/447783/503617/521530µs；差额=发布重叠期长 fetch（~0.37–0.47s 首次 poll，只计总窗） | README 口径要点；`checks/regen_analysis.log` |
| 6 | README 把 pgx `AcquireDuration` 称为「累计获取等待（≈tail 0.04%）」 | 改注为**全部成功 acquire 的累计耗时**（含建池/seed/monitor/断言，全轮快照；ON 16.007887/17.700651/20.600273/18.440113ms，OFF 最大 r07 23.248503ms）；排队指标 `EmptyAcquireWaitTime()` 未采集、池排队份额未测；删除 0.04% 结论 | README §3.2；缺测条目同步 |
| 7 | 缺四对 ON−OFF 差值表/配对规则/算法；total +0.019s 来源未区分 | 新增表与算法说明：配对=plan 相邻四对（r02↔r03、r05↔r04、r06↔r07、r09↔r08）；逐对差 = on − off，四值中位 = 两中间值平均（与分析器 `rank=p·(n−1)` 一致）；**total 组中位差 +0.0188755s vs 配对差中位 −0.0129180s（符号相反）**；tail 两算法恰好同为 +0.1487955s；ON/OFF 仅衡量细分采集增量，pristine n=2 不可分解测点开销 | README §3.1 |
| 8 | 证据披露缺口（smoke 无批次级 env/fingerprint；recorder/分析器/审计/race 无归档日志；曲线 100%≠N；「见交付报告」不可定位） | README §6/§7 增披露：smoke 源码身份绑定弱于主批；上述执行仅为会话内记录、不补造、不以本轮冒充历史；曲线分母=各采样序列自身最大值（applied max 9994/9984/9972/9965，smoke 42/40 vs N=50），终态 N 由 counts/anchors 独立展示；「独立核验」改为本目录 `CORRECTIONS.md` + `checks/`，并注明原批次独立复算未归档 | README §6/§7 |

## 2. 离线重生成与派生变化

用修复后的分析器对同一证据树离线重生成（`go run ./scripts/consumerseg/analyze -dir .`，rc=0，见 `checks/regen_analysis.log`）：

| 产物 | 变化 |
|---|---|
| `analysis/summary.md` | 仅「生成时间」一行（生成器自述：唯一非确定性字段） |
| `analysis/summary_all.json` | 仅 `generated_at` |
| `runs/*/summary.json`（10 轮） | **字节不变**（未进入 git diff） |
| `smoke/analysis/*`、`smoke/runs/*` | 未重生成、未改动 |

统计确认：原四轮 ON 的覆盖/residual/process 份额/分位/计数/配对等全部数值与修正前归档一致（差异仅时间戳），
即修复未意外改变任何正确统计。

## 3. 原始件保持（字节不变）

对照提交 `27cc51d` 树与基线哈希：`runs/**`（meta/anchors/report/*.csv.gz/exit_code/summary.json）、
`logs/**`（.env/.rc/.log.gz/runner.log/summary.tsv）、`environment.txt`、`source_fingerprint.txt` 全部未变。
核验：`checks/raw_preservation.log`。

## 4. 本轮检查日志（`checks/`）

- `analyzer_pre_epoch_before.log/.rc`（rc=1）、`analyzer_pre_epoch_after.log/.rc`（rc=0）——合成负例修前失败/修后通过
- `analyzer_pkg.log/.rc`、`analyzer_pkg_race.log/.rc`、`analyzer_gofmt.log/.rc`——分析器包级（含 -race）
- `fixture_root_guard.log/.rc`——runner 根保护夹具（8 用例：缺陷复现/空目录/数据目录/符号链接/换 plan/新根全流程/并发争用/参数错误）
- `regen_analysis.log/.rc`——离线重生成
- `build.log/.rc`、`build_perf.log/.rc`、`vet_analyze.log/.rc`、`gofmt.log/.rc`、`bash_n.log/.rc`
- `raw_preservation.py`（只读审计脚本，可重跑）+ `raw_preservation.log/.rc`；`manifest_check.log/.rc`
- `environment.txt`、`commands.tsv`（本轮命令 → 退出码 → 日志对照表）

## 5. 身份与指纹

- `source_fingerprint.txt`：**采集时**指纹（字节不变，对应提交 `27cc51d` 的工作树）。
- `source_fingerprint_current.txt`：本轮修正后（run_batch.sh / analyze.go / analyze_test.go 变更后）的对应指纹，
  与刷新后的 `SHA256SUMS` 配套。
- `SHA256SUMS`（本轮刷新）覆盖除自身与 `checks/manifest_check.{log,rc}`（manifest 生成后的核验产物，
  按构造不可自指）外的全部证据文件；核验见 `checks/manifest_check.log`（rc=0）。

## 6. 未决（不补造）

精确测试 argv/GOFLAGS、smoke `-race` 构建归属、recorder/分析器/审计的执行记录、远程权限/PR/CI、
物理提交瞬时、逐事件池等待、跨机/生产归因——本批从未测得，仅如实披露。
