# 013 限流决策预算 — 第四轮验收证据（发布前审查 F1–F5 闭环）

**批次**：发布前审查修复（分支 `013-redis-latency-budget-design`；基线链
`be272cb→a59dcd1→db21da2→476e987→9e0ae19→本批工作树`）。本轮只增测试与
证据、刷新文档/清单：**生产代码、配置默认、CI 零改动**；生产阈值
（L 取值、误拒绝率上限）仍为设计 §8 待决策。

**环境**：Go 1.26.5；go-redis v9.22.0；容器 redis:8.2.10-alpine
（testcontainers-go v0.44.0；docker 29.6.1；WSL2 linux 6.18.33.2）。
所有命令 `-race -count=1`；`TXHARBOR_FAULT_EVIDENCE_DIR` 指向本次运行的
临时目录（无生产凭据、无敏感值）。

**范围指认**：本目录 = 第四轮两次运行（F2 四形态装配验收；F5/F4 GateDelay
绿/红/恢复）的归档。第三轮与更早归档（[../redis-budget-implement/](../redis-budget-implement/README.md)、
[../redis-budget-cte/](../redis-budget-cte/README.md)、
[../redis-latency-abc/](../redis-latency-abc/README.md)）**不**被改标为
本轮运行。

## F1–F5 闭环对照

| 发现 | 闭环动作 | 证据 |
|---|---|---|
| F1 实施记录「CTE 边界 9 记录复跑全 ≤L+ε」表述有误（第 9 条实为 CTE-off 反向对照） | 设计文档实施记录改为**断言分列**：8 个 CTE-on 场景 ≤L+ε；1 条 CTE-off（`cte_off_hot_control` 1001.2ms）只断言 ≥0.8s、**不计入 L+ε 统计** | [../redis-latency-budget-design.md](../redis-latency-budget-design.md)（实施记录第三轮）；[../redis-latency-abc/SHA256SUMS](../redis-latency-abc/SHA256SUMS) 5/5 |
| F2 §7「整决策耗时四形态」验收未执行 | 新增装配级测试 `TestRatelimitBudgetFourShapeAcceptance`：4 形态 × L{200,300}ms × 2 轮 × 16 = **256/256 通过**（-race，无 DATA RACE） | `fourshape_run.log`、`fourshape_records.json`、`fourshape_meta.json` |
| F3 证据清单陈旧（abc README 条目失配） | abc manifest 刷新（5/5 OK）；implement manifest 随 README 指针更新重算（12/12 OK） | [../redis-latency-abc/SHA256SUMS](../redis-latency-abc/SHA256SUMS)、[../redis-budget-implement/SHA256SUMS](../redis-budget-implement/SHA256SUMS) |
| F4 GateDelay Hold 反转吞答的失败历史红日志未留存 | 诚实声明日志灭失 + **受控负例**：临时 worktree 回退吞答分支 ⇒ exit 1（行 230）；恢复 ⇒ exit 0 | `gate_red_negcontrol.log`、`negative_control_patch.diff`、`gate_green_negcontrol_restored.log`、`gate_meta.json` |
| F5 GateDelay 自测在延迟窗口内触发 Hold 翻转时序不稳（固定 `Sleep(100ms)`） | 改为**有界等待**：`DelayedReplies` 快照 + 2ms 轮询（5s 上限，超时 fatal 并列出两值） | `gate_green_main.log`、`gate_meta.json`（`redisgate_test.go` sha256 `3baea444…`） |

## 运行表（4 条命令）

| # | 命令（cwd） | exit | 结果 | 日志 |
|---|---|---|---|---|
| 1 | `TXHARBOR_FAULT_EVIDENCE_DIR=/tmp/txharbor-r6/fourshape go test -race -tags e2e -count=1 -timeout 30m -v ./internal/app -run TestRatelimitBudgetFourShapeAcceptance`（repo） | 0 | `--- PASS: TestRatelimitBudgetFourShapeAcceptance (75.74s)`；`ok internal/app 76.820s`；DATA RACE 0 行 | `fourshape_run.log` |
| 2 | `TXHARBOR_FAULT_EVIDENCE_DIR=/tmp/txharbor-r6/gate go test -race -tags integration_redis -count=1 -timeout 10m -v ./internal/testutil -run TestRedisGate`（repo、主树） | 0 | 3/3 PASS；`TestRedisGateDelayDelaysEvalReplies PASS (9.15s)` | `gate_green_main.log` |
| 3 | 同上命令（临时 worktree，仅回退 Hold 吞答分支） | 1（**预期红**） | `FAIL: TestRedisGateDelayDelaysEvalReplies (2.91s)`；断言见下 | `gate_red_negcontrol.log` |
| 4 | 同上命令（worktree 恢复修复后） | 0 | 3/3 PASS；`TestRedisGateDelayDelaysEvalReplies PASS (9.08s)` | `gate_green_negcontrol_restored.log` |

#3 的白盒补丁 = `negative_control_patch.diff`（sha256
`0dc6cc15b1c0195335daee18c15e68655276c14fa5973479caf5332675a867b5`；
只回退 `runDelay` backend→client 的 holdingNow 吞答分支；测试文件先
`git add` 以保证 diff 纯净）。

## 关键数值

**1. 四形态 × 预算 8 组（n=32/组，-race）**——p50/max ms；容差 150ms 为
装配层测试输入（非 SLO）：

| 形态 | L=200ms p50/max | L=300ms p50/max |
|---|---|---|
| refused（拒连） | 200.52 / 205.61 | 300.32 / 306.55 |
| dial_blackhole（合成拨号阻塞） | 200.96 / 201.92 | 300.73 / 307.39 |
| cold_init（冷初始化无响应） | 201.11 / 205.44 | 301.23 / 320.20 |
| hot_blocked（热命令无响应） | 201.35 / 208.18 | 301.50 / 310.81 |

256/256 样本全断言通过（err≠nil、`ErrUnavailable`、上下界、错误类、
gate accepts/eval_frames 计数）；8 组 `bound_pass=true`。

**2. refused 两相（go-redis v9.22.0 结构）**：phase1
`budget_expiry_dial_retrying`（class timeout）**18/17**——`queuedNewConn`
把拨号跑在 detached ctx（`internal/pool/pool.go:~1059`），预算只中止 caller
等待、拨号继续重试（DialerRetries 默认 5×100ms 退避 ≈0.4–0.5s > L）；
phase2 `dial_error_fast_path`（class refused）**14/15**——`dialErrorsNum`
饱和后短路到缓存的 refused 错误（`pool.go:~692`；簿记 `:~765`；后台
tryDial 探测仅成功才复位 `:~812-830`）。拨号尝试合计 **89/84**；最后
返回后 +2（91/86，300ms 窗口）为**观测，非协程退出证明**。两相均
`ErrUnavailable`，无时序界被放宽。

**3. 选项等价（Dialer 替换法）**：4/4 替换组（refused/dial_blackhole ×
200/300ms）`deep_equal=true`——对生产构造逐字段 `reflect.DeepEqual`
（全部导出字段；Dialer 与 PushNotificationProcessor 除外，并以两边置 nil
后整结构 DeepEqual）；`production_assert_pass` 8/8、MaxRetries sentinel
还原 4/4。**`MaxRetries=-1` 不触碰 `DialerRetries`**（生产默认保留；本批
生产代码零改动）。

**4. F4/F5 GateDelay**：受控负例红行（逐字）：
`redisgate_test.go:230: eval during the delay window succeeded after the Hold
flip: the pending reply was delivered (must be swallowed)`；补丁 sha256 见上；
修复后 `redisgate_test.go` sha256
`3baea444a9aa43ecf1296f947ca10dc77b4ce93bc93634b389499fdcd5531e94`。
**原始失败日志已灭失**（有界搜索 /tmp、docs、internal 仅命中源码断言
文本与设计文档引用）——本轮以受控负例替代，不冒称历史原运行。

## 边界（如实）

1. 200/300ms 是验证配置、不是生产 SLO/默认；150ms 容差是装配层测试
   输入、非 SLO；实测最大超界 +20.20ms（cold_init/300 max 320.196 vs
   300ms 基值），全部 ≤L+150ms 内。
2. F2 断言的是「单次 Allow 入口→返回」的界与错误类，不含生产分布；
   四形态均为实验注入姿态。
3. 后台拨号观测（refused 组 +2）仅是计数增量观测，不是协程退出证明。
4. F4 历史红运行不可引用（日志灭失）；受控负例只证明「当前测试能捕获
   该回退」，不冒称历史运行复现。
5. Recovery Drill run `37298994567` 仍为独立失败（runner 环境性），
   本轮不修、不在范围。
6. 更早轮次归档不重标：本目录文件来自 run 时
   `TXHARBOR_FAULT_EVIDENCE_DIR`，run 时 git 状态与参数见
   `fourshape_meta.json`、`gate_meta.json`。

## 可追溯指纹（run 时工作树）

| 文件 | sha256 |
|---|---|
| internal/app/serve.go | `9567cd75209e080df20edca70c9ef24bc4aab66bcc9c495b975221aba285cdb5` |
| internal/app/ratelimit_middleware.go | `79c815ac01fb566bc0831241313696cf6c170cd5034649e0a37f72bdcbcd4c5e` |
| internal/ratelimit/limiter.go | `ef259b8cbf412db6d0aa9889a21bce0e06662752a68b1467dee2094dea019da7` |
| internal/testutil/redisgate.go | `29612c6343961277abaa0eebffe9993f6eab24c82f4ab4a5721744292a5edc96` |
| internal/app/ratelimit_budget_fourshape_integration_test.go（新测试） | `228f80cde00217f42dc190637012b57277d0e00c248afb124a1f1c7457a03ae9` |
| internal/testutil/redisgate_test.go（F5 稳定化后） | `3baea444a9aa43ecf1296f947ca10dc77b4ce93bc93634b389499fdcd5531e94` |

以上指纹标识**本批工作树**（run 时 git 状态：
` M docs/evidence/013/redis-latency-abc/SHA256SUMS`、
` M docs/evidence/013/redis-latency-budget-design.md`、
` M internal/testutil/redisgate_test.go`、
`?? internal/app/ratelimit_budget_fourshape_integration_test.go`）；
更早轮次归档**不**按本轮标签重标；提交树可用这些哈希对照。

## 复现入口

```sh
TXHARBOR_FAULT_EVIDENCE_DIR=$(mktemp -d) \
  go test -race -tags e2e -count=1 -timeout 30m -v ./internal/app \
  -run TestRatelimitBudgetFourShapeAcceptance
TXHARBOR_FAULT_EVIDENCE_DIR=$(mktemp -d) \
  go test -race -tags integration_redis -count=1 -timeout 10m -v ./internal/testutil \
  -run TestRedisGate
```

清单口径：本目录与 `redis-budget-implement/`、`redis-budget-cte/` 的
SHA256SUMS 为**目录相对**（在目录内 `sha256sum -c SHA256SUMS`）；
`redis-latency-abc/` 为**仓库根相对**（在仓库根校验）。
