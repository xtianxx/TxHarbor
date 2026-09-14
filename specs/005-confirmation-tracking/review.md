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
- Q3 跳过分类：疑点为跳过与停止边界及饿死。证据：spec FR-06/US3（行级"不得提交" vs 循环级"停止确认"）、
  chain_blocks 不可变（pre-006）。结论：行级跳过（`below_depth` 正常 / `noncanonical` 防御，不建暂停行）
  与循环级停止（暂停/tip/漂移/失权）互不代替；批量选择、逐行独立事务；单调性无饿死证明。
  修正：data-model §候选分类、contracts skipped 原因二分、research R5 指针、quickstart D5 滞留用例。
- Q4 审计与 006 边界：疑点为不可改写执行体与 orphaned 含义。证据：004 append-only 应用断言先例。
  结论：应用谓词 + 零行断言 + 抽查 SQL，无触发器；依据列任何阶段不得改写删除；005 DDL 无 `'orphaned'`
  占位，"预留"纯属设计说明。修正：data-model §Table 1、§006 预留，research R6，plan 交接节。
- 本次复核未发现须改变已批准业务语义的问题；spec 未动。验证场景同步至 quickstart D1/D5。

## 八、任务拆解进展（2026-09-14 tasks）

- 产物：`tasks.md`（T000-L/T000-P + T001–T004 + T010–T032，共 31 项，全未完成）。
  按模板分阶段：Setup（门禁建档）、Foundational（迁移/配置/数学/指标）、US1–US5（P1×4、P2×1）、Polish。
- 自检（本步内，非正式 analyze）：格式全合规；依赖 7 条同文件链无环；FR-01–12、SC-01–10、18 验收场景全映射；
  [P] 仅跨文件无依赖者（T020 同文件冲突已去标记）；T030/T032 来源已声明；业务代码零改动。
- OI-1（`confirmations BIGINT` vs 2^63 精确值）记入 tasks 待 T017 决议，未掩盖、未改规格。
- 阻塞：无新增阻塞；具备进入正式一致性分析的条件（本次不执行）。下一命令建议 `/speckit.analyze`。
