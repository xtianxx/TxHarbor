# 规格审查记录：005 Confirmation Tracking（specify 步骤）

**日期**：2026-09-14 · **分支**：`005-confirmation-tracking`（基于 `origin/main` `fd45e8b`）· **命令**：`/speckit.specify`（仓库实际配置：Spec Kit 1.0.5，opencode 集成，增强工作流 Full SDD Cycle 1.0.1；`before_specify` 强制 hook `speckit.git.feature` 已执行，`after_specify` 为可选 hook，仅记录不自动进入后续步骤）

本记录为 specify 步骤的审查记录，不是已批准的业务规范，不修改 001–004 的业务语义，不重跑已完成阶段。

## 一、实际执行的命令与工作流

- Spec Kit 版本：`.specify/init-options.json` → `speckit_version 1.0.5`，`feature_numbering: sequential`，集成 `opencode`；工作流注册表 `.specify/workflows/workflow-registry.json` → `speckit 1.0.1`（specify → plan → tasks → implement，含评审门）。
- `before_specify` 强制 hook 已执行：`bash .specify/extensions/git/scripts/bash/create-new-feature-branch.sh --json --short-name "confirmation-tracking" "…"`（注：脚本须用 `bash` 显式调用，直接 `sh` 报 `Syntax error: "(" unexpected`）→ `{"BRANCH_NAME":"005-confirmation-tracking","FEATURE_NUM":"005"}`。
- 基线处理：本地 `main` 落后 `origin/main` 35 个提交且含无关 `.gitignore` 修改（oh-my-opencode-slim worktrees 样板）。无关修改已 `git stash` 暂存隔离；`main` 快进到 `origin/main`（`fd45e8b`）后创建分支；收尾时恢复 stash 且不纳入提交。
- 产物：`specs/005-confirmation-tracking/spec.md`、`checklists/requirements.md`、本文件；`.specify/feature.json` 指向 `specs/005-confirmation-tracking`。未生成 plan.md、tasks.md，未写实现代码，未运行实现测试，未推送、未创建 PR、未合并。

## 二、上游契约结论（引用仓库实际定义）

- **004 → 005**：充值观察身份 `(chain_id, block_hash, tx_hash, log_index)`、来源与区块引用（`block_number` + `block_hash`）、Pending 初始状态（`status` CHECK 锁死 `pending`，005 以自有迁移扩展）、canonical 判定来源（`chain_blocks` 同高度 canonical 行）、状态历史（`deposit_pause` + `deposit_pause_audit`，`pause_id`/`revision` 实例语义）与链视图异常信号（`deposit_pause` / `indexer_pause` / `log_pause` 三行独立共存、提交要求皆无）均已核对一致，无互不兼容的新字段或状态语义。
- **002/003 → 005**："本地连续校验过的链头"来源为 002 `chain_blocks` canonical 行（父子连续校验 + checkpoint 外键绑定真相）；有效条件为 canonical 为真且无 `indexer_pause`；链头缺失（空进度）→ 停止，链视图异常（暂停/哈希不一致/无法核实）→ 停止，正常索引滞后 → 等待。尚未观察到的高度不得用于确认（FR-02、US3 场景 3/4）。
- **005 → 006**：005 已定义确认提交的暂停契约（FR-08：三行暂停皆无）、版本有效性契约（FR-07/FR-08：持锁后重读复核，失配拒提交）与审计契约（确认转换记录：来源身份、区块高度 + 哈希、链头高度 + 哈希、阈值、所得确认数、确认时间）；实现机制留给 plan。006 职责（分叉发现、祖先搜索、游标回退、Orphaned 转换、版本标记、超深重组对账）本次不实现不设计；再确认历史所需的保留信息已列出，未批准新的状态恢复策略。

## 三、发现分类

### 已有约束（来自任务基线、章程 1.1.0 与 001–004 已合并规格）

1. 公式 `max(0, canonical_tip - block_number + 1)`，含所在区块；N=1 时所在区块计一次确认。
2. 链头取自本地连续校验过的 canonical 链，不用 RPC 高度；N≥1；仅 canonical 充值可确认；身份含 `block_hash`。
3. 条件更新 + 确认时间 + 审计依据；重复/并发/重启不重复转换、不改写首次事实。
4. 链视图异常停止确认；提交受一致链视图约束（检查→提交无窗口，暂停/版本失配拒提交）。
5. Confirmed 仅表示达到项目确认策略，不可撤销性/上游入账均不承诺。
6. 单部署单链、白名单标准 ERC-20、Go + PostgreSQL + EVM JSON-RPC；无余额账本、无 Redis/Kafka/新增平台。

### 待决策项（2026-09-14 clarify 已全部闭合；以下为决定摘要，详细语义见 spec）

1. 非法阈值处理（Q1，已闭合）：必填无默认值，缺失/非整数/<1/超整数范围一律拒绝启动并明确报错，不静默修正、截断或回退；不设业务上限，表示范围与安全运算留 plan；不预先授权运行时变更。
2. 阈值变更允许性与生效规则（Q2，已闭合）：选项 A——受控授权变更；重启漂移同属策略变更，未授权拒绝；单链任一时刻单一有效策略版本，切换后旧结果不得提交；新阈值从切换点起适用于全部未确认 Pending，降低不批量直接确认；提高/降低均不改写 Confirmed，保留当时阈值、策略版本与依据；006 职责不变；切换失败无部分生效；机制留 plan，不扩展通用配置平台。
3. 确认检查驱动与竞争协调、存储形状、可观测载体、lease 复用细节留给 plan（OQ1–OQ3、D3–D5），plan 不得违背已闭合决定。

### 待核验实现（不阻塞规格完成）

1. T023 证据提交 `3a79dfa` 在分支 `004-deposit-detection` 存在，但不在 `origin/main` 祖先链（已核验）；PR #6 合并 `fd45e8b` 在 main 中已核验，不据此否定合并结果。
2. 004 acceptance（已合并文本）未解决项：T000-P open、上游 003 E1 open、serve 端口占用者未知——当前解决状态未经本步骤核验。
3. 003/004 的 T000-P 仍开放，不宣称生产就绪；此前本地测试偶发失败原因未知，实现阶段须如实复核。

## 四、规格检查结果与阻塞

- 质量清单 `checklists/requirements.md`：Q1、Q2 闭合后复核，16/16 全通过（"无 NEEDS CLARIFICATION 标记"项由未勾选转为勾选；其余 15 项无回归）。
- 阻塞：无。clarify 已闭合，满足进入 `/speckit.plan` 的规格条件。
- hooks 建议：`after_clarify` 为可选提交 hook，本次以常规本地提交交付（见五）；工作流下一门为评审后 `/speckit.plan`——按任务停止边界，本次记录建议并停止，不进入。

## 五、澄清进展（2026-09-14 clarify）

- Q1（非法阈值处理）已裁决：选项 A（必填无默认值，拒绝不静默纠正；不设业务上限，表示范围与安全运算留 plan；不预先授权运行时变更，允许变更时非法变更拒绝、原策略保持）。已写入 spec `## Clarifications`、`FR-03` 与 Edge Cases；`checklists/requirements.md` 按 clarify 规则仅切换标记——"无 NEEDS CLARIFICATION 标记"项仍有 1 处剩余故保持未勾选，文件其余内容（含 Notes 中"2 处"字样）暂未动，待澄清全部闭合后再刷新。
- Q2（阈值变更允许性与生效规则）已裁决：选项 A + 补充语义（受控授权、重启漂移同属变更、单有效版本、旧结果不得提交、新阈值向前适用于全部未确认 Pending、降低不批量确认、Confirmed 永不改写且保留当时阈值/版本/依据、006 职责不变、失败无部分生效、机制留 plan）。已写入 spec `## Clarifications`、`FR-03`、US2 场景 4、US5 场景 5、Edge Cases、Non-Goals、SC-10；两项歧义闭合，标记清零。

## 六、计划进展（2026-09-14 plan）

- plan 产物：`plan.md`（技术上下文/章程门/结构/关键流程/覆盖矩阵/006 交接）、`research.md`（R1–R9）、
  `data-model.md`（迁移 `000005` + 三版本 + 提交/首确认/切换协议 + 时序论证）、`contracts/observability.md`
  （`confirmation_*` 组）、`quickstart.md`（D1–D5）。upstream 语义零改动；`requirements.md` Notes 与场景计数已同步澄清后事实。
- 章程门：初检与设计后复检均通过，无豁免（见 plan.md）。评审结论：设计评审通过，非实现验证。
- 阻塞：无；满足进入 `/speckit.tasks` 的计划条件（本次不进入）。

## 七、plan 定向复核（2026-09-14 plan 内复核，非完整重跑）

- Q1 公式与范围：疑点为饱和值审计精确性与 Go/DB 表示一致。证据：tip/h 来源列 `BIGINT CHECK (>=0)`
  （000002/000004 迁移）→可达域 `[0, MaxInt64]`；结论：门禁改用等价式 `tip>=h && tip-h>=N-1`
  （数学等价，任意输入无溢出），N 可达域 `[1, MaxInt64]`（BIGINT 即系统范围，非业务上限），
  饱和 guard 仅纵深防御、触发按内部错误拒绝。修正：research R1、data-model §确认数计算、quickstart D1。
  未改规格公式（等价式仅实现形式），Q1/Q2 未重开。
- Q2 策略初始化与切换：疑点为无充值确立、双首启 race、入口权限、丢失响应、旧 worker 恢复。
  证据：004 首单元/PK 串行化、`serve.go:212-235` 特权 loop 外授权、`coordinator.go:106-174` 任一错全停扇出、
  004 request_id 绑定规则。结论：机制完整，缺的是 005 落地文字。修正：data-model §首确认协议
  （无充值长期无行 + 分歧双首启胜者落定/败者大声停 + 胜负不决正确性）、§授权切换协议（入口=DB 操作员直连 SQL、
  守卫全在事务内无旁路 + 丢失响应按 request_id 重读定性）、§切换后恢复程序（整进程退出范围 + 零暂停行 +
  发版重启 runbook）。未扩展通用配置平台。
- Q3 跳过分类【本条已被 §九 F4 取代：行级跳过不可用于异常引用，缺失/不一致引用按规格属链视图异常→循环停止；
  以下为 plan 复核当时的历史记录，保留不删】：疑点为跳过与停止边界及饿死。证据：spec FR-06/US3（行级"不得提交" vs 循环级"停止确认"）、
  chain_blocks 不可变（pre-006）。结论：行级跳过（`below_depth` 正常 / `noncanonical` 防御，不建暂停行）
  与循环级停止（暂停/tip/漂移/失权）互不代替；批量选择、逐行独立事务；单调性无饿死证明。
  修正：data-model §候选分类、contracts skipped 原因二分、research R5 指针、quickstart D5 滞留用例。
- Q4 审计与 006 边界：疑点为不可改写执行体与 orphaned 含义。证据：004 append-only 应用断言先例。
  结论：应用谓词 + 零行断言 + 抽查 SQL，无触发器；依据列任何阶段不得改写删除；005 DDL 无 `'orphaned'`
  占位，"预留"纯属设计说明。修正：data-model §Table 1、§006 预留，research R6，plan 交接节。
- 本次复核未发现须改变已批准业务语义的问题；spec 未动。验证场景同步至 quickstart D1/D5。

## 八、任务拆解进展（2026-09-14 tasks；remediation 后为 30 项）

- 产物：`tasks.md`（T000-L/T000-P + T001–T004 + T010–T032 + remediation 新增 T033，共 30 项定义行，全未完成）。
  按模板分阶段：Setup（门禁建档）、Foundational（迁移/配置/数学/指标）、US1–US5（P1×4、P2×1）、Polish。
  （计数修正：此前记"31 项"有误，实际定义行 29，新增 T033 后 30。F8）
- 自检（本步内，非正式 analyze）：格式全合规；依赖 7 条同文件链无环；FR-01–12、SC-01–10、18 验收场景全映射；
  [P] 仅跨文件无依赖者（T020 同文件冲突已去标记）；T030/T032 来源已声明；业务代码零改动。
- OI-1（`confirmations BIGINT` vs 2^63 精确值）记入 tasks 待 T017 决议，未掩盖、未改规格。
  【§九 F1 已决议关闭为 NUMERIC，本条为 tasks 步骤当时的历史记录，保留不删】
- 阻塞：无新增阻塞；具备进入正式一致性分析的条件（本次不执行）。下一命令建议 `/speckit.analyze`。

## 九、remediation 处理（2026-09-14，直改规划文档；修正完成，待正式复分析）

本节记录 analyze F1–F10 的逐项处理与证据位置。状态为"修正完成，待正式复分析"——
**不自行宣称 analyze 已通过**，复分析另行安排。spec 未动（无一处需改变已批准业务语义）。

- F1（OI-1 关闭）：`confirmations` 改 `NUMERIC` 精确整数（整数性 + 非负 CHECK），2^63 精确可存可审；
  Go↔SQL 十进制字符串，禁 int64/float64 中转；"二选一"删除。证据：data-model §确认数计算/Table 1、
  research R1、tasks T001/T017/OI-1 条目、quickstart D1。
- F2：新增 T033（异配置首启 race：恰好一行 bootstrap、败者漂移错误零转换、胜者绑定现行策略）；
  覆盖/批次/依赖同步。证据：tasks Phase 6/依赖链/覆盖矩阵/Batch D。
- F3：T011 加空状态断言（零候选→无策略行、无写入、等待非停止）。证据：tasks T011。
- F4（先核对后修正，未直接采纳行级跳过）：核对 FR-06（"引用区块缺失……任一成立即不得提交"）、
  US3-2（"引用区块无法核实"→停止确认、不提交任何转换）、Edge-170（哈希不一致→按链视图异常停止），
  判定缺失/不一致引用属链视图异常→循环停止（state=3，链头缺失 state=1），`below_depth` 为唯一行级等待；
  前排异常阻塞后排是规格要求（006 接管前保持停止），无饿死论证仅限良性。未作新业务选择。
  证据：data-model §候选分类/§提交协议、research R5/R8、contracts（skipped 仅 `below_depth`、
  停止日志加 `reference_unverifiable`）、quickstart D5（含 SQL 播种可行性：无指向 chain_blocks 的 FK）、
  plan 关键流程/Constitution Check IV、tasks T018/T019/T020/T004。
- F5：T026 加独立第二连接、锁等待同步点、两种线性化顺序断言、禁 sleep。证据：tasks T026。
- F6：T019 明确小批量 LIMIT 多 tick；T025 加降阈值后小批量复核。证据：tasks T019/T025、quickstart D5。
- F7：T013 加旧 `version_seq` 观察照常确认断言（含全部门禁仍须通过，非绕过）。证据：tasks T013。
- F8：计数 29→30（新增 T033 后）同步本节；此前"31"为笔误。证据：任务定义行实测。
- F9：T024 补 004 T024（`specs/004-deposit-detection/tasks.md:91` 受控 SQL 脚本）引用，不重做选型。证据：tasks T024。
- F10：T030 收窄为核验现有 CI（`make test-integration` 自动覆盖，无需新 job）。证据：tasks T030、
  `.github/workflows/ci.yml:71-92`。
- F11：无需修改。
- 剩余问题：无新增阻塞；F4 判定链已完整引用规格原文，复分析可直接复核。

## 十、T032 tasks 自检结论（2026-09-14，Batch F docs-only；仅声明，不执行 analyze）

本节为 tasks.md T032 要求的自检证据（review.md 增补）。方法：只读核验
（tasks.md 全文复核 + grep 实测 + 工作树 diff 清单），未改生产代码与测试、
未勾选任何任务、未运行测试、未提交。以下逐项给结论。

- (a) 格式检查：结论——通过（有三处生成时即存在的形态例外，非回归）。
  实测：任务定义行 30 行（`^- [.] T` 实测 30 匹配，与 §八 F8 修后计数一致）；
  `需求：` 30 处、`依赖：` 30 处、`完成条件：` 30 处全覆盖；
  `验收场景：` 27 处，缺失 3 项为 T000-L/T000-P（门禁建档任务，验收即完成条件本身，
  用"内容"代替场景）与 T032（自检任务自身，验收即本节结论），三者生成时即此形态；
  文件归属除 T001 有显式"涉及文件："行外，其余均内嵌于任务标题括号
  （如"（`internal/indexer/confirmcommit.go` 新建）"），T031 为指南式全量执行无独立涉及文件
  （属任务性质，非遗漏）。ID 唯一：T000-L/T000-P/T001–T004/T010–T028/T029–T032/T033
  无重复、无缺号（T005–T009 等间隙为编号规则预留，见 tasks.md 格式节，已文档化）。
  [P] 标记 9 处（T001/T002/T003/T004/T014/T015/T026/T029/T030）：T001–T004/T014/T015/T026
  均为跨文件且前置依赖已完成（生成时 T020 已去 [P]，与 T014 同文件 `confirm_test.go` 冲突）；
  T029/T030 为文档/CI 独立文件。未发现同文件链成员带 [P]。
- (b) 依赖 DAG 无环：结论——通过，7 条同文件链与声明依赖一致（1 处需精确表述）。
  实测链：`confirm_test.go` T003→T014→T020（依赖行 T014→T003、T020→T014）；
  `confirmscan.go` T011→T018（T018→T011）；集成主文件
  T013→T016→T019→T021→T022→T023→T027（依赖行逐级 T016→T013、T019→T018、
  T021→T013、T022→T021、T023→T022 均同文件顺序）；
  唯一例外：T027 声明依赖为 T024（US5 切换语义前置），其"接 T023 顺序追加"为文件位置约束
  （tasks.md 任务链节原文"同文件接 T023 顺序追加"），位置链与依赖边共同无环；
  `confirmation_auth_integration_test.go` T025→T028（T028→T025）；
  `config_test.go`/`serve_config_test.go` T002→T015；迁移文件 T001→T017；
  race 文件 T026（dep T010+T024，与 T025 文件不同可并行）→T033（dep T026）。
  无反向边、无自环。
- (c) 覆盖复核：结论——通过，12 FR / 10 SC / 18 场景映射与生成时一致，无掉线。
  FR 表 12 行、SC 表 10 行逐行有任务（T033 已进入 FR-03/FR-08 行，F2 落地）；
  场景映射 15 条目共 18 项（US1-1/1-2=2、US2-1/2/3=3、US2-4=1、US3-1…US3-4=4、
  US4-1/2/3=3、异配置首启=1、US5-1=1、US5-2/3=2、US5-4=1、US5-5=1）逐项有任务；
  T033 暂缓→闭合记录完整（Batch D 暂缓注记 + Batch E 闭合注记，tasks.md T033 行内）；
  US2-4→T025 移交成立（T016"切换重判移交 T025" + T025 验收场景含 US2-4 + T025 含降低重判全纳入）。
  生成时预告的"T020-de-P 等"与终态一致（T020 终态无 [P]，§八自检注记吻合）。
- (d) 文件冲突检查：结论——通过，共享文件均有全序，无不兼容并写。
  共享文件与顺序：`confirmation_integration_test.go`（T013→T016→T019→T021→T022→T023→T027，
  另 T027(a) 与 serve 侧新测试分属不同包）；`confirmation_auth_integration_test.go`
  （T025→T028）；`confirm_test.go`（T003→T014→T020）；`confirmscan.go`（T011→T018）；
  `config_test.go` + `serve_config_test.go`（T002→T015）；
  `migrations/000005_confirmation_tracking.sql`（T001 建表→T017 NUMERIC 决议修订）；
  `confirmation_race_integration_test.go`（T026 新建→T033 同文件追加）；
  `confirmauth.go`（T024 新建；T025/T026/T028 仅经其公开守卫调用，无并写）。
  工作树未提交改动（`git status`：M 6 项 + 新文件 `internal/app/confirmauth*.go` 3 项）分属
  Batch F 范围（runbook/confirm-auth 子命令/serve 漂移测试/旧 harness 补 env），
  与已关闭批次文件无语义冲突（见 (j)）。
- (e) OI-1 关闭：结论——关闭成立，无复开依据。`confirmations NUMERIC`
  （`migrations/000005_confirmation_tracking.sql:19` 注释决议 + `:58` 列定义
  `NUMERIC CHECK (>=0 且 =floor(自身))`）；落地任务 T001（列定义）+ T017（极值审计）均已勾选；
  审计测试文件 `internal/indexer/confirmnumeric_integration_test.go` 存在。
- (f) 残留事项：T000-P 保持 open（生产门禁，tasks.md:25 未勾选）；
  上游 003 E1 保持 open（tasks.md Open Issues 节声明，005 不代关）；
  偶发本地失败（004 遗留）原因仍未知——本次 Batch A–E + T031 全量未观察到新偶发失败
  （声明：本步未运行测试，该"无新失败"转述自 orchestrator 全量记录，见 (j)；未知项不因成功而关闭，
  按文件头 Tests 纪律仍为未解决事项）；T023 已关闭（勾选 + Batch D 闭合记录，
  三路径 0 行断言 + 完整性 SQL + 5d43e98 门禁探针证据）；N1：仓库内无名为 N1 的跟踪项
  （tasks.md/review.md/quickstart.md/plan.md/data-model.md/research.md 全文 grep 仅命中
  quickstart:98 处 T033 测试名 `…_N10Wins/N20Wins`，非跟踪项），按 orchestrator 口径记"关闭"，
  无仓库证据可引——若 N1 另有所指，需 orchestrator 补编号来源后重核。
- (g) 任务计数对账：结论——终态 30 定义行中 25 勾选、5 未勾选，与分支现实一致。
  已勾选 25：T000-L + T001–T004（4）+ T010–T013（4）+ T014–T017（4）+ T018–T020（3）
  + T021–T023/T033（4）+ T024–T028（5）。未勾选 5（逐项列出，不静默丢弃）：
  T000-P（open，见 (f)）、T029/T030/T031（实现与证据已在工作树，见 (j)，勾选权属 orchestrator，
  本步按禁令不勾选）、T032（即本任务，本文档增补落地后仍待 orchestrator 关闭）。
  Batch A–E 无遗留未勾选项（T033 随 Batch E 闭合）；生成时"全 `- [ ]`"注记（tasks.md:407）
  为设计阶段历史陈述，终态以本条对账为准。
- (h) 006 交接完整：结论—— intact，无 006 代码。暂停门禁（三暂停行提交前重读复核，T010/T026）、
  锁序（lease/writeGuard/`FOR UPDATE`/独立重读，data-model §提交协议）、依据列
  （六依据列 + `confirmed_at`，§Table 1 保留契约"MUST NOT 被 UPDATE/DELETE（含 006）"）均未动；
  005 代码无 `'orphaned'` 语义实现（`internal/` 全文 grep 仅两处否定性断言：
  迁移测试 forbidden-list 与 `confirmscan_test.go:296` 非法状态拒绝用例，均为"不得出现"证明）；
  005 DDL 无 `'orphaned'` 占位（data-model §006 预留节：拓宽 CHECK 属 006 自有迁移职责）。
- (i) T027 边界：结论——两级证据各证其事，不互相代替。fan-out 点级：
  `TestConfirmationDriftExitLoudStop`（T027(a)，`confirmation_integration_test.go:1815`）
  证明旧 N 进程切换后 `ServeLoop` 返回 `*confirmationConfigMismatchError`（state=3、零提交），
  且该错误值到达 `RunQuatro` fan-out 点（注释明示：import cycle 使 indexer 包无法导入 app，
  `os.Exit` 无法进程内断言，故止于 fan-out 点并引用映射）；serve 级：
  新测试 `TestServeConfirmationDriftExitsNonZero`（`serve_integration_test.go:326`）
  证明真实 `Serve()` 在旧 N 下经 `serve.go:327-335` 的 indexerErr→exitCode=1 路径返回退出码 1，
  闭合 T027"退出码"断言的 serve 侧缺口。暂停行不变（pause-invariance）与新 N 重启恢复仍由 T027(a) 侧覆盖。
- (j) T031 全量记录：结论——工作树证据与 orchestrator 全量口径一致（本步未独立运行测试，
  以下转述 + 工作树实证，不冒充亲测）。`git log` 含 Batch A–E + T023 门禁 + F1–F10 remediation
  提交链（`2b1a65b`→`dcb7233`）；工作树未提交部分即 T029–T031 增量：
  quickstart §切换 runbook（T029，`quickstart.md:75-138`：唯一入口 `txharbor confirm-auth`、
  退出码 0/1/2、分歧首启对账、切换 5 步、未知结果规则、旧配置退出、新配置重启、审计追溯，
  每步引测试证据，无新语义）；confirm-auth 子命令（`internal/app/confirmauth.go` 新建 +
  `cmd/txharbor/main.go` 接线 + 单测/集成测试 3 个）；serve 漂移 exit-1 测试（见 (i)）。
  found-and-fixed：旧 harness 缺 `TXHARBOR_CONFIRMATION_DEPTH`（T002 新增必填项后，
  004 时代 harness 未跟进）——工作树 diff 实证 3 文件共 +7 行
  （`migrate_concurrent_integration_test.go:59` cliEnv、`flip_integration_test.go:63` serve env、
  `serve_integration_test.go` 5 处 env map）；
  全仓现 7 处 harness env 携带该变量（上 2 处 + `serve_integration_test.go` 5 处含新测试 2 处）。
  unit/race/integration/lint 全绿与 `make test-integration` 7:34 全绿为 orchestrator 全量口径，
  本步仅记录不复证；T031/T029/T030 勾选待 orchestrator。
- (k) 写路径清单 vs 门禁一致性：结论——一致，INSERT 载体为批准的算子入口。
  005 写路径：`deposit_observations` 唯一 UPDATE 在 `confirmcommit.go:463`
  （`status='pending'` 谓词 + 七项批准 SET，`TestDepositWritePathConfinement` 全仓 grep 半侧 pin）；
  `confirmation_policy_history` INSERT-only（载体 `confirmauth.go:411`，
  经 `AuthorizeConfirmationPolicy` 守卫事务；门禁禁其 UPDATE）；
  批准的算子入口即 `confirm-auth` 子命令（`internal/app/confirmauth.go` 头注释明示
  "A bare psql INSERT is forbidden … the only binary path"，runbook §引用为唯一入口），
  与 T024"无端点/服务/角色新增、DB 操作员直连"决议一致（子命令是该直连的二进制载体，非新服务）。
  门禁 pin UPDATE/DELETE、INSERT 载体文档化——与 data-model §Table 1 执行机制一致。

**analyze 条件声明（仅声明）**：进入 `/speckit.analyze` 的实质条件已齐
（设计文档冻结、F1–F10 已 remediation、30 任务映射/证据闭合、残留项显式化），
形式条件差 T032 自身关闭一事（即本节被接受 + orchestrator 勾选 T029–T032）。
本步不执行 analyze、不宣称 analyze 通过、不宣称生产就绪、不宣称远程 CI 状态。
