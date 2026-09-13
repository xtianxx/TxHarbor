# Acceptance Record: 004-deposit-detection（本地范围验收，T022）

**Branch**: `004-deposit-detection` | **HEAD**: `83f8146` | **Date**: 2026-09-13

**本次相关提交**：`52506db`（全段处置修复＋成功断言＋探针诊断）、`103adea`（可观测限制＋端口定级＋T020 关闭）、
`83f8146`（本验收记录＋T022 关闭＋G12 注释修正；相对 103adea 仅文档与单行注释零语义，编译与 G7 定点已复验）。
完整链见 `git log`（004 自 `3eaf427` 起；T020 收口链见 tasks.md T020 证据）。

**任务**：T000-L/T000-P（门禁）＋ T001–T029（T021 先行关闭，T020/T029 已关闭，T022 为本记录）。

## 运行证据（复用有效结果，未无理由重跑全套件）

- T020 终态四命令一次全绿（build ✓、lint ✓、unit 全包 ok、integration EXIT=0 八包：app 19.8s/config/db 63s/eth/health 16.8s/indexer 357.6s/logx/metrics）：
  `/tmp/opencode/t020/full_integration_t020close.log`。
- 固定批次（留存）：t010 5×、t020 timing/manual/auth_subset、t029 merge 5×（35 子项）/pause_regress 5×/single、根 `auth_subset.log`（17 PASS，含 merged 处置/拒绝）、t027/batch1（修订前失败史）。
- 本批补跑：`TestDepositPauseMergeCumulative/merged_row_manual_release` 单跑通过（rtk 汇总 2 passed＝父＋子；wrapper 不保留逐案日志，此处如实记录）；
  `TestServeRejectsBadConfigBeforeListening` 单跑 13 过（F4 诊断分支未触发，符合预期）。
- 种类图例：**A** 真 Anvil 链路 / **B** 真库播种（含双真 worker/并发） / **F** 故障注入 / **S** 生产 serve 接线 / **U** 静态单元（含否定性 grep）。

## D1–D11 逐项结论（均为 PASS，下为证据）

- **D1** 合法匹配生成 Pending：`TestDepositAnvilFullStackPending`（A，逐字段对链真值）＋`TestDepositCommitFirstUnitAtomic`/`AdvanceExactGuard`（B）。`t007_run1.log`；T020 close。
- **D2** 非监控/零值不生成：`TestDepositMixedIntervalZeroGeneration`（B，四分类 0/3/1/0）＋空区间推进/缺失等待（B＋U shape）。`t008_run1.log`；T020 close。限制：`invalid` 非零路径无直接指标断言（G10），只记 `invalid=0`。
- **D3** 重复与崩溃恢复：重放全同收敛、字段冲突整批失败、崩溃注入 4 子、未知提交判定（B＋F conn-wrapper）。t010 5×；T020 close。种类声明：进程 kill 由连接死亡＋租约过期重启模拟，标 **F** 非字面 kill（G9）。
- **D4** 写与进度原子：首单元原子建行、mid-tx 失败、commit 未达库、精确守卫（B＋F）。t010 5×；T020 close。
- **D5** 配置变更拒绝/漂移改回：`TestDepositRestartConfigComparison` 8 子＋漂移拒绝＋mismatch＋单侧损坏（B，零破坏）＋`TestServeDepositConfigRefusalEndToEnd` 空白退出极性（S）。限制：serve 级仅空白名单退出有测试；运行时 mismatch 不导致 serve 退出（只停循环），故无 serve 级 mismatch 退出测试，此为设计语义非缺口（G8）。
- **D6** 暂时缺口等待恢复：瞬态等待 1s 零误判后补齐恢复、缺口分类（B）。tasks:188；T020 close。
- **D7** 结构缺口停止＋授权恢复：两类 structural 停止、无超时误判、新资产界内/界外、暂停行落行、授权释放 replay=10 折叠、回放收缩、缺目标重定目标（B）。auth_subset；T020 close。
- **D8** 链暂停/失效/解除（B＋F，详表略记测试名）：暂停行实例＋修订＋版本标签、链视图缺失、canonical 失败引理、commit 回滚中止、三流阻断证明、validation 入暂停、冲突版本锁、漂移零行、重启保持、上游解除自动续（004 不清上游行）、人工解除审计同事务（零消费写）、重验失败建新实例、陈旧 0 行、旧修订失配、两路径审计失败回滚、重解除返原、跨版本解除、needs_006 保持、并发首胜/失权/回滚不直写、合并 8 子（两因保留/不降级/幂等/旧修订栅栏/并发不丢＋旧基不合/审计回滚/已决不合并）、可观测审计可查。manual/timing/merge/pause_regress 各 5×；T020 close。
- **D9** 双 worker＋延迟提交：首单元恰一次推进跨 pool 断言、旧 token fencing、stale 依据隔离（B 双独立 pool＋lease）。t029 5×；T020 close。限制：原 T017 批次 20/20 日志未留存，以上为后继证据（G3）。
- **D10** 迁移：空库/003 升级/约束/序列审计/索引/降级仅去 004/NUMERIC 往返/文件形状（B＋U）。`ok db 62.9s`；T020 close。
- **D11** 授权套件（B＋F）：全拒回滚、旧在途 0 行、同 ID 同参返原/异参拒/异 ID 独立、并发同 ID、未知提交以 DB 为准、回放收缩三则、嵌套收敛、暂停 fencing（陈旧删/同实例旧修订/替换目标/无目标必处置/保留 needs_006/消费者停）、同内容复现区分、请求四则、绑定修订（修订前 5 失败留证 t027/batch1，修订后 PASS）、翻转期不变式、多段处置（全解决 ReplayFrom=10/次段未覆盖点名拒绝）。auth_subset 17 PASS；timing 5×；T020 close。

## SC-01–09 与 FR

- SC-01–09：代表测试见映射（全栈/零生成/原子/崩溃注入/backoff/冲突暂停/缺口恢复/配置回放/覆盖拒绝/可观测端到端＋readyz 不被翻转＋health 真依赖翻转恢复），全部 PASS，证据同上。
- FR-01–16：FR-01–15 均有测试落点（解析单测 U、提交/缺口/暂停/授权 B＋F、迁移 B＋U、否定性 FR-13/14 由 `TestDepositWritePathConfinement` U 锁定、可观测 FR-15 由端到端＋契约单测锁定）；FR-16 为规划门禁（T021 CLOSED），无测试是定义。

## 后续批准项（已验证）

P1 请求绑定、P2 start_block 同步 Q7（raise-start 断言锁定 checkpoint start）、P3 累积合并 Q8/T029、P4 全段恢复判定修复（D4 判别 ReplayFrom=10）、P5 成功断言、P6 多段未覆盖拒绝。证据见上。

## 已知限制与未解决事项

1. **serve 端口占用失败（A 类已定位，未解释未修复）**：`TestServeRejectsBadConfigBeforeListening/invalid_pg_dsn、invalid_rpc_url`，
   `address 127.0.0.1:41924 still in use after config failure: bind: address already in use`，
   日志 `~/.local/share/rtk/tee/1789304323_go_test.log:1069-1078`。占用者未知；ss 快照、39/39 重跑日志缺失。
   此后一切运行（F4 单跑 13 过、全单元、全集成）均未复现。F4 已补探针 stderr 诊断供未来取证。
2. **合并暂停可见面**：修订仅经诊断 SQL 可见；日志不新增行；`deposit_pause.height/kind` 为首原因（契约已注记）。
3. **可追溯性缺口**：T028 原始 flake 日志文件已不存在（分析结论保留在 tasks 证据；该项已解释＋修复＋新批次）；T017/T027 修订后批次、T025 轮次、140 过中间批次无留存日志，以 T020 终态全绿＋留存批次为后继证据。
4. **种类与粒度**：D3 崩溃为故障注入非字面 kill；D2 `invalid` 非零无直接指标断言；多项最终证据为包级 ok＋叙述级任务证据。
5. **注释债已清**：di:1483 “T018 未实现”旧注已修正（本批）。

## 结论

004 本地范围验收通过（T022）。未解决项：T000-P open、上游 003 E1 open、serve 端口占用者未知（上 1）。
通过仅表示本地验收，不等于生产接入就绪（仍需 T000-P 关闭后另行判定）。未推送、未合并，不进入 T023。
