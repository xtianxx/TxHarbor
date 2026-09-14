# Quickstart: 005-confirmation-tracking 验证指南

**Branch**: `005-confirmation-tracking` | **Date**: 2026-09-14

本文件是验证运行指南，不是实现代码。前置：PostgreSQL 18 + Anvil（chain-id 31337），
002/003/004 按各自 quickstart 已就绪并产出 canonical 块、Transfer 日志与 Pending 充值观察。
命令占位符（tasks 阶段固定真实调用）：`go test ./...`（PR CI，常驻）、
`go test -tags integration ./...`（故障注入，需真库 + Anvil）。

## D1 — 边界与配置（FR-01/02/03/04，SC-01/02/09）

- 预置：N=10；Pending 充值位于 h，推进 canonical tip 使确认数分别为 9（N-1）、10（N）、11（N+1）。
- 期望：9 → 保持 Pending 无转换记录；10/11 → 各恰好一次 Confirmed，依据列（tip 高度 + 哈希、阈值 10、
  所得确认数）齐备且可追溯（contracts 追溯 SQL）。
- N=1：tip == h 的 Pending 确认数为 1 并转换。
- 整数边界（单元测，PR CI）：`tip < h` 两边皆假；`tip - h >= N - 1` 与规格公式逐值对照（含 N=1、N=MaxInt64）；
  饱和 guard 存在性测试（不可达防御，集成不模拟；可达性证据见 research R1）；
  N=MaxInt64 合法但永不达标（等待）；N=MaxInt64+1 解析期拒绝（系统表示范围，非业务上限）；
  tip=MaxInt64、h=0 的 2^63 经 `NUMERIC` 精确写入/读取/审计（OI-1 决议；十进制字符串转换，禁 int64/float64 中转）。
- 非法配置（启动期，PR CI + 集成）：缺失/空串/非整数/`0`/`-5`/超 uint64 范围一律启动失败并返回明确配置错误；
  无默认值、无静默修正（Q1）。断言：进程非零退出，零确认提交。

## D2 — 幂等与崩溃（FR-05，SC-05/08）

- 同一充值重复检查 ≥2 次：有效转换恒为 1，确认时间与依据不变。
- 双 worker 并发确认同一 Pending（`-race`，PR CI 可跑逻辑竞态项，真并发在集成）：
  恰好一方成功，另一方收敛；首次值不可改写。
- 提交前 kill、提交响应丢弃后重启：按 PK 重读定性后继续；无遗漏、无重复、无部分行
  （完整性抽查 SQL 返回 0）。

## D3 — 提交竞争：暂停 / 链视图变化 / 策略切换（FR-06/07/08，SC-03/07）

- 候选读取后、提交前分别注入：`indexer_pause` 行、`deposit_pause` 行、tip 推进（新 canonical 块）、
  策略切换（新 seq 行）。期望：四种注入下旧结果提交成功率均为 0（`transition_total{stale}` +1，
  零状态变化）；调用方重读后可重新决策。
- 双 worker 时序断言：败者条件 UPDATE 影响 0 行 → 回滚收敛（data-model §并发时序情形 2）。
- 旧配置 worker 在切换后提交：策略守卫失配 → 拒绝并漂移停止（R7），永不提交旧结果。

## D4 — 阈值变更全周期（FR-03，SC-10）

- 授权降低 N（如 10→3）：切换点前已存在的 Pending 全部按新阈值重新判定（有序扫描覆盖证明：
  候选 SQL 无下界游标）；仍须 canonical/链视图/暂停全部门禁（降低不批量直接确认）；
  已有 Confirmed 零改写且可追溯当时阈值。
- 授权提高 N（如 3→10）：已 Confirmed 保持；未达新阈值的 Pending 保持 Pending。
- 未授权漂移：重启携带不同 N → 启动拒绝（error 日志 `policy_drift`），已有观察与策略零破坏。
- 切换失败（预期旧 seq 过期/空授权/非法值）：`policy_transition_total{rejected}` +1，
  策略表无新行（无部分生效、无多版本）。
- 提交结果未知（授权 INSERT 后断连）：按 `request_id` 重读定性（返原结果 / 未绑定可重试），不虚构成功。

## D5 — 升级、恢复与审计（FR-09/10/11/12，SC-04/09/10）

- 004 存量库升级：`000005` 迁移在全 Pending 库上一次通过（DO 断言零非 pending 行）；
  升级后既有 Pending 按现行策略正常处理；启动失败（非法 N）后库状态不变，修正配置重启即恢复。
- 重启恢复：kill -9 确认循环后重启，从 durable 状态继续，无遗漏（SC-04：追赶后符合条件 100% 处理）。
- 审计完整性：全部 Confirmed 行通过追溯 SQL 可定位充值/区块/依据/时间；错误输出零凭据明文、零无限制转储。
- 正常滞后 vs 异常：tip 可信但高度未覆盖 → 等待（state=1）；tip 缺失（零提交，state=1 等待可信 tip）/
  暂停/不可信 tip/漂移 → 停止（state=3）；单行引用缺失或哈希不一致 → 按链视图异常停止确认、
  本 tick 及后续零提交（state=3，error 日志 `reference_unverifiable`；US3-2/Edge-170），
  同批未提交行不受污染但亦不提交（规格"不提交任何转换"）。
- 前排异常阻塞（规格要求行为）：前排引用不可信行使循环停止，后排合格行在异常消除前不被处理
  （006 接管前保持停止；data-model §候选分类）。种子可行性：`deposit_observations` 无指向
  `chain_blocks` 的 FK（000004 仅 `version_seq`→history 外键），故 SQL 直插伪造引用行可行
  （须附 history 行满足 FK；scanner 路径 pre-006 产不出此类行，只能 SQL 播种）。
  良性无饿死对照：全行可信 + 小批量 LIMIT → 多 tick 后后排全覆盖（单调性论证）。

## 验证分层（CI vs 故障注入）

- PR CI（`go test ./...`，含 `-race` 并发项）：R1 安全数学单元测、配置解析拒绝表（D1 非法值矩阵）、
  条件 UPDATE 收敛逻辑（mock 行级行为或轻量 pg）、守卫谓词测试。
- 集成故障注入（`go test -tags integration ./...`，真库 + Anvil）：D1 Anvil 推进场景、D2 双真实连接竞态 +
  kill 恢复、D3 暂停/重组/切换注入、D4 全周期、D5 升级与重启。Anvil 重组模拟以"新 canonical 块推进 +
  暂停行插入"表达；超深重组与祖先搜索属 006，不在本阶段验证。
- 本步不运行实现测试；以上为方案，真实命令与断言由 tasks 固定。本设计评审结论不是实现验证。

## §切换 runbook（T029，FR-03/Q2 操作步骤）

本节每一步均有实现/测试证据，不引入新语义。唯一操作入口是：

`txharbor confirm-auth --request-id ID --expected-old-seq S --new-threshold N --operator OP --reason R`

（T029 carrier；裸 psql INSERT 禁止——old-seq 检查、Q1 解析、request_id 定性
全部在 `AuthorizeConfirmationPolicy` 同一事务内执行，旁路即无守卫；
证据 `internal/indexer/confirmauth.go` T024 + `internal/app/confirmauth.go`）。
退出码：0 提交或同意图已记录（stdout
`ok policy_seq=<seq> threshold=<N> recorded=<bool>`）；1 拒绝/失败
（stderr 已脱敏）；2 用法错误。用 serve 同一份 env 文件运行
（`config.Load` 全量校验，命令实际只用 PGDSN + ChainID；`--operator`
记名义身份，执行者是持 DSN 运行该二进制的 DB operator，与 migrate 同信任）。

1. 启动与 N（required-N）：`TXHARBOR_CONFIRMATION_DEPTH` 必填正整数
   `[1, MaxInt64]`，无默认值；缺失/非法一律启动拒绝、零确认提交
   （D1 + T015 拒绝矩阵）。首次启动策略表为空是 pre-bootstrap（非错误）：
   首行只能由 first-confirm 事务建，空表上 confirm-auth 拒绝
   （T024 `TestConfirmAuthEmptyTableRefused`）；tip 缺失则等待
   （state=1，D5）。不要手动插首行。
2. 分歧首启对账（两实例带不同 N 同时首启）：恰好一方 bootstrap `(1, N)`
   落地，败方 drift 拒绝并 loud 停止、零转换（T033
   `TestConfirmationRaceDivergentFirstStart_N10Wins/N20Wins`）。对账：
   以策略 SQL 查赢家——
   `SELECT policy_seq, threshold, prev_seq, operator, reason, request_id,
   expected_old_seq, created_at FROM confirmation_policy_history
   WHERE chain_id = $1 ORDER BY policy_seq`（T028 version-chain SQL；
   有效策略 = max seq 行）。赢家 N 若非预期，用第 3 步授权切换纠正
   （T033 注释：wrong-N winner 由 T029 路径纠正），不要直接改库。
3. 受控切换（常规 N 变更）：(a) 读当前 max seq——
   `SELECT policy_seq, threshold FROM confirmation_policy_history
   WHERE chain_id = $1 ORDER BY policy_seq DESC LIMIT 1`；
   (b) 以该 seq 为 `--expected-old-seq`、全新 `request_id` 运行
   confirm-auth；(c) stdout 含 `ok` 后验行（第 2 步 SQL：新行
   seq=max+1、prev_seq=旧值、operator/reason/request_id/expected_old_seq
   齐备；T024 happy-path 断言形状）；(d) 全舰队更新
   `TXHARBOR_CONFIRMATION_DEPTH`=新 N；(e) 逐实例重启（rolling，一次一台）。
   拒绝（S 过期/空授权 H'==H/非法值）：`policy_transition_total{rejected}`+1、
   无新行、无部分生效（T025 phase d）；失败不绑定 ID，修正参数换新
   request_id 重试（T024 可重试断言）。
4. 未知结果规则（INSERT 后断连）：先按 `(chain_id, request_id)` 重读定性，
   永不换 ID 重试——COMMIT 已落地返原结果（`recorded=true`，不重执行），
   未落地则 ID 未绑定、原参数重试即为一次新切换；换不同意图复用 ID 直接
   拒绝、零状态变化（T025 phase e；分类 SQL：
   `SELECT policy_seq, prev_seq, threshold, operator, reason, expected_old_seq
   FROM confirmation_policy_history WHERE chain_id = $1 AND request_id = $2`）。
   回滚 = 以当前 max seq 为旧值再做一次授权切换（新 request_id）；没有别的
   回滚路径。
5. 旧配置退出与新配置重启：切换后旧 N 进程 `ServeLoop` 返回 drift 错误、
   state=3、零提交，经 `RunQuatro` 上浮由 serve 映射为非零退出
   （T027 `TestConfirmationDriftExitLoudStop` +
   `internal/app/serve.go:327-335`）；这是预期行为，不要"抢救"旧进程。
   切换本身不建/删/改任何暂停行（T027 pause-invariance：
   deposit_pause/indexer_pause/log_pause 三表不变）；新 N 重启后恢复确认
   （T027 owns drift-exit + pause-invariance + resume，见 tasks T027）。
6. 审计追溯（T028，逐字 contracts/observability.md 诊断 SQL）：Converted 行
   定位——deposit→block→依据→时间的 10 列 trace SQL；完整性 SQL
  （basis 六列 + confirmed_at 零缺失）；策略侧 version-chain SQL（第 2 步）。
   错误输出零凭据、零无限制转储；计数器对账：
   `policy_transition_total{ok}` == 切换次数、`{rejected}` == 失败尝试。
   与 004 授权手册的职责边界：004 管 deposit 配置授权，005 confirm-auth
   只管确认阈值；confirm 切换不写 deposit 观察行、不碰三流暂停行
   （T024 zero-write + zero-pause 断言）。
