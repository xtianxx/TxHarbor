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

### 待决策项（需 clarify 裁决；审查建议，非已批准规范）

1. 非法阈值（N<1 或非正整数）处理：拒绝启动并报错，还是拒绝本次确认并暂停？（spec FR-03 标记 1）
2. 阈值变更是否允许、如何生效：授权事务/版本化、向前生效还是追溯重算？（spec FR-03 标记 2）
3. 确认检查驱动与竞争协调、存储形状、可观测载体、lease 复用细节留给 plan（OQ1–OQ3、D3–D5），plan 不得静默选择待澄清项。

### 待核验实现（不阻塞规格完成）

1. T023 证据提交 `3a79dfa` 在分支 `004-deposit-detection` 存在，但不在 `origin/main` 祖先链（已核验）；PR #6 合并 `fd45e8b` 在 main 中已核验，不据此否定合并结果。
2. 004 acceptance（已合并文本）未解决项：T000-P open、上游 003 E1 open、serve 端口占用者未知——当前解决状态未经本步骤核验。
3. 003/004 的 T000-P 仍开放，不宣称生产就绪；此前本地测试偶发失败原因未知，实现阶段须如实复核。

## 四、规格检查结果与阻塞

- 质量清单 `checklists/requirements.md`：16 项中 15 项通过，未通过仅为"2 处 [NEEDS CLARIFICATION] 待澄清"（按任务指令保留）。
- 阻塞：FR-03 两处待澄清裁决阻塞 `/speckit.clarify` 之后的工作；不阻塞本 specify 步骤交付。
- hooks 建议：`after_specify` 为可选提交 hook；工作流下一门为评审后 `/speckit.clarify` 或 `/speckit.plan`——按任务停止边界，本次记录建议并停止，不进入。
