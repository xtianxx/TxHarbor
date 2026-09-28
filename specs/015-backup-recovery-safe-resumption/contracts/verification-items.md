# Contract: Verification Items, Evidence Gaps and External Boundaries (015)

**Spec**: [spec.md](../spec.md) (FR-014–FR-020, FR-027–FR-029, FR-034) | **Design**: [data-model.md](../data-model.md) §1.5/§1.6/§8 | **Reuse**: 006 恢复区间重放、013 隔离重放/进度、014 差异/证据/权限/审计（只读或经其自身授权入口）

核验只读、只记录、只阻塞；**核验不触发付款、签名、广播、重放或真实下游投递**（FR-020）。结论一律 `consistent / divergent / unknown / stale`；**unknown 不得当通过**。

## 1. 核验目录与结论规则

| # | 对象 | 证据来源 | 可证明 | 必然 unknown 的情形 | 直接阻塞能力 |
|---|---|---|---|---|---|
| V1 | 链事实 vs PG | RPC canonical（区块/回执/日志）+ `chain_blocks/erc20_transfer_logs/indexer_checkpoint/log_checkpoint/deposit_*` + 006 `reorg_recovery` | 恢复点后链上已发生但本地缺失/矛盾 | RPC 不可达、确认未达阈值、孤块未决 | `chain_scan`、充值相关 |
| V2 | 提款请求/付款意图/授权 | `withdrawal_requests/payment_intents/withdrawal_authorizations` + 007 审计 | 请求/意图的存在与状态；**不能**证明"从未发生" | 恢复点无记录且无外部证据 | `new_withdrawal_creation`、`existing_withdrawal_recovery` |
| V3 | nonce 分配/占用 | 008 `nonce_bindings/nonce_observations/nonce_scope_state…` + `eth_getTransactionCount` | 分配与链上消耗的一致性 | RPC 不可达、pending/unknown | `existing_withdrawal_recovery` |
| V4 | 签名/广播结果（含 unknown） | `signing_requests/signature_results/tx_attempt_signings/tx_send_attempts/tx_receipts/tx_reconciliations` + 链回执（**只读 accessor 白名单（F4）**：`AttemptByID`/`UnknownRecovery`〔内部只读事实 `sendFacts`/`reconcileFacts`〕/`Status`/回执只读查询；**禁用 `Reconcile`〔写 `tx_reconciliations` 观察行+状态转换〕、`Release`、`Send`、`applyReceipt` 等写路径**；方法名含查询/恢复不构成只读证明） | 已持久化事实与链上结果的对照；unknown 保持 unknown | 无记录且无法从链/签名边界取证 | `existing_withdrawal_recovery` |
| V5 | Outbox 与义务标记 | `outbox_events` + `event_obligation`（000017，只增不减）+ publisher 进度 | 义务存在但事件未推进/已发布未知 | broker 不可读、保留裁剪区段 | `event_publishing` |
| V6 | 消费者幂等/进度/隔离 | `consumer_inbox/versions/progress/quarantine` + broker committed offset（可读时） | 回退检测（PG 进度 vs offset）、重复吸收现状 | broker 不可读（offset 未知） | `event_consuming` |
| V7 | 014 差异/复核/处置/权限/审计 | 000016 表（只读引用） | 既有差异与处置历史可追溯 | 表缺失/未迁移 | 与 V2/V5/V6 重叠部分 |
| V8 | 授权面漂移 | 控制库记录的外部真源证据 + 再核验/再施加结果 | 回退点后撤销/发放是否被回收/重现 | 无外部真源记录 | 资金与投递能力 |
| V9 | 工具/依赖就绪 | 镜像/客户端/DSN/Signer 边界可达性 | 恢复与核验可执行 | 任一依赖不可达 | 全部（前置） |

规则：

- `consistent` 仅当证据完整、新鲜（配置容忍内）、覆盖闭合且全部来源一致；任何来源 `unknown` → 整项至多 `unknown`。
- **缺失不构成"从未发生/从未付款/可重执行"**（FR-016）；**不得假设 014/扫描能重建备份后丢失的付款意图**，不得新建/重放/替代产生意图（FR-017）。
- 时间推断、默认值、旧状态、"大概未发生"不得填补证据（FR-018）。
- DB 回退 ≠ 外部付款/消费效果回退；已签名/已广播/已发布/已消费保持外部事实，差异必须显式列入结论（FR-015）。
- 复用 014/006/013 能力不得绕过其授权与门禁；核验发现的修复动作只能引用其既有入口（FR-020）。
- **只读纪律（F4）与零写断言范围（R1）**：V1–V9 适配器只能经只读 accessor/只读查询取证；**MUST NOT 调用 `Reconcile` 等任何写路径冒充只读**。零写证明分两层：①**隔离夹具（无并发业务写者）**下，核验批次执行前后对 V1–V9 读取链全部数据 DB 表做**整表内容指纹 diff**（全行确定性摘要，不止行数），diff 为空；基线表清单（以 `migrations/` 实际 schema 为准，逐表完整列表见 tasks T036，新增读取表必须同步加入静态清单）：V1 链/日志/充值/确认/重组族（`chain_blocks`/`erc20_transfer_logs`/`indexer_checkpoint`/`log_checkpoint`/`deposit_observations`/`deposit_observation_transitions`/`deposit_checkpoint`/`deposit_config_history`/`confirmation_policy_history`/`reorg_recovery`/`reorg_recovery_events`）；V2 提款/意图/授权/执行族（`withdrawal_requests`/`payment_intents`/`withdrawal_authorizations`/`withdrawal_authorization_scopes`/`request_status_projection`/`withdrawal_request_audit`/`withdrawal_grant_audit`）；V3 nonce 族（`nonce_bindings`/`nonce_observations`/`nonce_scope_state`/`nonce_scope_holds`/`nonce_wallet_registry`/`nonce_binding_events`/`nonce_ops_audit`）；V4 签名/广播/tx 生命周期与执行链（`signing_requests`/`signature_results`/`signing_request_audit`/`delivery_admissions`/`signer_caller`/`tx_attempts`/`tx_attempt_signings`/`tx_send_attempts`/`tx_receipts`/`tx_reconciliations`/`tx_attempt_events`/`tx_intent_freezes`/`execution_claims`/`execution_steps`/`execution_events`/`execution_ops_audit`/`execution_caller_permission`）；V5 `outbox_events`/`event_obligation`/`event_system_state`/`event_ops_audit`；V6 `consumer_inbox`/`consumer_versions`/`consumer_progress`/`consumer_quarantine`；V7/V8 000016 差异/审计族与授权面证据（`recon_task`/`recon_checkpoint`/`recon_gap`/`discrepancy`/`discrepancy_occurrence`/`disposition`/`reverify`/`recon_audit`/`recon_scan_attempt`/`recon_permission`/`caller`/`api_key`）。②**控制库写入另列**：`recovery_evidence`/`recovery_verification_item`/`recovery_gap`/`recovery_audit` 与 `evidence_generation`/`evidence_hash` 推进是本核验的预期输出，按 append-only＋代次协议单独断言，**不混入数据 DB 零写**。③**并发观察边界**：生产/演练并发业务写入（serve/worker/publisher/consumer 等）≠ 核验副作用；并发下只断言「核验归因写入=0」（只读角色/只读事务＋数据库侧归因证据），**不得断言整表指纹为空**。
- **历史重发负例（F10）**：历史 signed bytes 重放、历史广播重发、历史付款意图重执行、重复投递重发一律拒绝（0 次重放/重广播/重投递/重建意图）；回退绝不构成重放许可。
- **探针枚举（F9）**：`restore_probe.business_state_probes` 必须逐项对照 FR-002 九类权威对象（链身份/游标、事件、充值确认、提款请求与付款意图、出站交易与签名/广播、nonce、Outbox/义务标记、消费者幂等/进度、审计/权限/014 差异）抽样并声明覆盖边界；不能证明的类别标 `unknown` 且不得计入 `restored` 通过。

## 2. 证据缺口与人工处置证据包（FR-019）

每个 `open` 缺口记录：对象与范围、时间线、现有证据、所需外部证据/人工确认、受影响能力（含依赖/可能放大副作用者）、独立性论证（路径 + 逐边证据）、责任归属、升级记录。

- 受影响能力保持暂停；**依赖无法证明独立 → 保守纳入暂停**；不得仅凭"不同模块"判独立。
- 可证明独立且证据充分的能力可按 [approval-matrix.md](approval-matrix.md) 放行；放行必须记录范围、依赖核验、证据与批准结果，且不得间接启动被暂停的付款/签名/广播/真实下游副作用。
- 缺口无法补齐：保留 `unknown/pending` + 证据包 + 责任归属 + 升级；允许有界只读复核；**超时、重试耗尽、人工知悉 ≠ 闭合或获准复服**。
- 本阶段不交付风险接受后强制复服、损失核销、人工补偿付款、自动补造意图；**双人批准不能替代缺失证据**。
- **有界只读复核（F13）**：可读范围=本实例及授权 scope 内的证据/核验/缺口/审计；次数/时间/资源预算（部署配置）；预算耗尽→拒绝后续复核＋审计、不改变缺口/实例/批准/放行状态；超时、预算耗尽、人工知悉 ≠ 缺口闭合或获准复服。

## 3. 事件与下游边界（FR-027–FR-029）

- 至少一次投递：重复事件由 `consumer_inbox`/`consumer_versions` 幂等吸收；offset/inbox 回退**可检测**（PG 进度 vs broker offset），但**不得因缺失自动触发有副作用的重处理**；可能的外部重复效果必须可检测、可报告。
- 缺失的历史幂等记录不得静默补写为"已处理"，也不得据此判定可重执行；重新处理只能经既有 quarantine replay 等授权路径。
- 下游去重身份 = 既有事件稳定身份（`event_id` + source 三元组）；本阶段**不承诺跨系统恰好一次**，只提供本项目范围内的检测/处置边界。
- 未接入真实上游/下游回执时：**不得宣称外部账本一致或"外部效果已恢复"**；结论限定在本项目可验证范围（状态、游标、inbox/version、offset 关系、链上事实）。
- 真实效果边界：`cache.Invalidator` = 非权威缓存删除（Redis 失效不构成资金事实）；参考消费者 `ref_consumer_ledger` = 证据材料、明确非账本；真实下游交付（如生产 Kafka 主题的对外消费者）属"真实下游投递"，其恢复走 dual 审批范围。
