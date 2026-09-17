# 010/011 限定并行规则（决策记录，2026-09-17）

范围：仅新增适用于 010 交易生命周期管理与 011 提款执行（worker）的限定并行规则，
并登记两阶段共同契约。不启动任何阶段的 specify / clarify / plan / tasks / analyze /
implement，不做业务实现，不创建 011 产物，不自行裁决新业务语义。

依据：用户 2026-09-17 决定按 010/011 并行方向推进；010 specify（`7d2022b`）与
clarify（`bd59754`）已完成并经问答闭合，011 尚未启动。

## 章程一致性说明（本次无章程修改）

核查结论：串行约束仅存在于文档层——`docs/workflow-008-009-parallel.md` R1
（"010、011 不纳入本次例外，仍严格串行"）与 `docs/project-context.md` 状态表。
章程 v1.1.0（2026-09-12 批准）的 Development Workflow 只规定单特性的 Spec-Kit
阶段顺序（`Constitution -> Specify -> Clarify -> Plan -> Tasks -> Analyze ->
Implement -> Test -> Review`）、聚焦分支与质量门禁，未规定阶段之间必须串行执行。
因此本次限定例外沿用 008/009 的文档层决策记录机制即可满足章程 Governance
（"例外须显式记录"），无需章程修正案与版本升级；Q1–Q3 已裁决语义不在此复议。

## P1 规划可交错，单步骤推进

010/011 允许交错或并行开展规划工作，不要求 010 合并后才创建 011 规格。
一次只推进一个 Spec-Kit 步骤；只有后续指令明确指定两个阶段的同类步骤时
（例如"010 plan + 011 specify"），才可同时执行，不能自动串联后续流程。
本条取代 `docs/workflow-008-009-parallel.md` R1 中"010、011 不纳入本次例外"
一句；008/009 例外本身保持不变，不自动扩展到其他阶段。

## P2 并行实现准入

共同契约闭合（见下登记 C1–C9 的闭合条件），且 010、011 各自的 spec、必要 clarify、
plan、tasks、analyze 满足实现准入后，允许独立模块并行实现。
某些真实接线与验收仍有依赖（C10），不得把允许并行解释为无依赖：
依赖项在各自 analyze 与联合验收中显式验证，不提前视为已满足。

## P3 合并顺序与验收记录

合并顺序采用 010 → 011。010 自身验收和 011 最终全链验收分别记录；
必要上游同步应先于真实联合验收，不能要求真实测试通过后才合入其依赖。
本条合并顺序不是当前推送、PR 或合并授权；推送/PR/合并另行安排。

## P4 独立工作区与主编排协调

- 010 / 011 分别使用独立工作区、独立分支和显式 feature 定位
 （`SPECIFY_FEATURE_DIRECTORY=specs/010-* / specs/011-*`），遵循已核实的强制 hook
  机制（`before_specify` 自动建分支须配合显式 `GIT_BRANCH_NAME`），不依赖共享默认
  `feature.json`（machine-local，不入库），不全局关闭 hook。
- 共享接口、迁移、公共入口与测试资源（`compose.yaml` 单套 PostgreSQL / Anvil
  端口与数据卷）由主编排协调归属和合流：同一文件在同一步骤内只由一个阶段写入；
  迁移编号以实际集合为准，已确认 `000010` 被 PB 占用，不得默认改号，不得改写
  已应用于持久库的迁移。
- 不固定子模型，不设任意任务数量上限；批次内独立任务尽量并发。

## P5 批次节奏与停止线

完成适用检查和完整差异审阅后及时本地提交，报告并停止。
不得自动推进下一批，不得推送、PR、合并或部署。
旧机制测试或 mock 不能代替真实联合验收。

## 共同契约登记

图例：【继】= 已批准、直接继承（不重复提问）；【010→011】= 010 已确定、011 待承接
（011 须在自身 spec/plan 中引用并承接，尚无 011 规格，不声称双向闭合）；
【验】= 仍需设计核验或业务裁决（含闭合条件）。

| ID | 契约项 | 状态 | 010 责任 | 011 承接要求 | 闭合条件 |
| --- | --- | --- | --- | --- | --- |
| C1 | 意图与 request 关联（一 request 至多一 intent；intent 先于 008 预留持久化；Accepted 不是意图） | 【继】OC-1（2026-09-16） | 只关联既有 intent，不创建 | 在执行准入时创建并持久化 `intent_id ↔ 007 request_id` | 011 spec 引用并承接 |
| C2 | sender 与 nonce 权威（钱包注册表；008 独占绑定；绑定≠许可） | 【继】OC-2/OC-3（2026-09-16） | 只读消费绑定事实并校验 | 准入时固定 sender 并绑定 `intent_id + chain_id` | 011 spec 引用并承接 |
| C3 | attempt / signing_request 身份（预分配；010 预持久化；一次尝试一身份；替换用新身份） | 【继】OC-4（2026-09-16） | 首次调用 009 前持久化并关联意图与绑定 | 消费身份链，不另建冲突身份 | 011 spec 引用并承接 |
| C4 | 授权版本及条件式复用（用途允许 + 三维费用全满足方可复用，否则新授权；不静默换绑） | 【继】OC-5 + PB-C1/C2（2026-09-16） | 每次发送独立校验并绑定授权身份与版本 | 将授权绑定到唯一 intent；调度权≠签发权 | 011 spec 引用并承接 |
| C5 | 状态投影权威（010 持久事实唯一权威；011 可保留查询展示投影；投影不得作为执行类许可依据；显示保留版本与时间，旧版不覆盖新版，不明标过期） | 【010→011】Q1（2026-09-17） | 拥有尝试持久事实；修订后更新，对账以 010 事实为准 | 投影引用身份不复制事实；决策读验当前权威事实 | 011 spec 承接 + 联合验收覆盖显示与决策分离 |
| C6 | 执行资格与发送围栏（仅拒写库不够；失格 worker 禁止三类发送；围栏针对发起时资格；接管者重验后可恢复，不建第二意图；已发出无法撤回） | 【010→011】Q2（2026-09-17） | 发送前重验执行资格并拒绝过期依据 | 过期写入被拒；接管流程重验全部当前门禁 | 011 spec 承接 + 联合验收覆盖接管与围栏 |
| C7 | 广播安全语义（每次发送须四门禁全过；失效后无重播例外/TTL；"决策先行"不算在途；可阻止则阻止，否则保留事实或未知对账；已知结果不改未知） | 【010→011】Q3（2026-09-17） | 按矩阵执行发送/拒绝/对账 | 遵守同一矩阵，不凭旧准入结论发送 | 011 spec 承接 + 联合验收覆盖三类发送与竞争时点 |
| C8 | 暂停/恢复版本（006 FR-26；多暂停共存；恢复版本变化后旧链视图不提交；只读观察不解除） | 【继】OC-6（2026-09-16） | 发送前观察当前门禁 | 同等继承，过期 worker 隔离与版本约束同构 | 011 spec 引用并承接 |
| C9 | 未知结果对账与重组修订（未知≠失败/未付；保留追踪；修订追踪原意图，不重建意图补偿） | 【继】OC-6/OC-7（2026-09-16）+ 010 FR-03/FR-09 | 拥有未知与修订历史并提供对账依据 | 修订结果并追踪原意图 | 联合验收覆盖未知对账与重组修订 |
| C10 | 真实接线与联合验收（010 独立验收 ≠ 联合验收；mock 不替代） | 【验】 | 010 独立范围验收（010 spec FR-16） | 011 spec/plan 设计联合验收 | 真实 010/011 联合验收通过 |
| C11 | 显示延迟业务时限（如确需） | 【验】 | 未指定，不预设 | 若确需业务时限，提出有依据的建议供用户裁决，无须默认另立项目 | 有依据的推荐值经确认，或保持未指定 |
| C12 | 迁移编号与保护机制残差（fencing/锁/事务载体、生效顺序、证据、DB 与网络独立失效残差） | 【验】 | 在 plan 中明确，不套用 009 残差例外 | 在 plan 中明确接管侧机制 | 各自 plan + analyze；缺口报告供裁决 |

完整继承 010 三项澄清（Q1–Q3，`bd59754`），不重新提问，不削弱广播边界。
009 逐次签名交付重验与既定独立故障残差只作为签名侧已批准契约，不推导 010 新例外。

## 后续两阶段采用规则的方式

后续 010/011 工作区引用本提交（分支 `docs/010-011-parallel-rules`，SHA 见提交报告）
的方式二选一：在各自工作区内以只读方式读取本文档，或将其合并（merge）进各自
分支。本轮不擅自合并其他分支；010 现有分支（`010-transaction-lifecycle@bd59754`）
保持不动，待后续指令再行同步。

## 本次实际工作流限制

- 不执行任何阶段的 specify / clarify / plan / tasks / analyze / implement。
- 不启动 011，不创建 011 产物。
- 不推送、不创建 PR、不合并、不部署。
- 变更文件：新增本文档；同步 `docs/project-context.md` 状态（008/009 历史例外原文保留）。
- A-13 全链 E2E 与 T000-P 分别保持 OPEN。

## 联合设计合同 v1（2026-09-17，010/011 plan 共用输入）

性质：设计输入，不是新业务裁决。业务语义全部继承已批准契约（OC-1–OC-7、
PB-C1/C2、010 Q1–Q3、011 M1n/M3）；载体、锁、顺序等机制为 plan 待验证的技术
选择。单写者：本轮由 010 工作区拥有；011 分支同步相同内容并记录版本
（`a3ac87e` + C11 修正 + 本节 v1）。

- **J1 身份链和所有权**：`withdrawal_requests.request_id`（007）→ 011 `payment_intents.intent_id`
  （新建，011 所有，暂定迁移 `000012`）→ `nonce_bindings`（008 所有，稳定绑定身份）→
  010 `tx_attempts.attempt_id + signing_request_id`（新建，010 所有，暂定迁移 `000011`）→
  `withdrawal_authorizations.authorization_id + withdrawal_authorization_scopes` 版本。
  归属：intent 行归 011，attempt 行归 010；attempt 经 FK 关联 intent 与绑定，不复制事实。
  暂定迁移号仅供规划（未应用，合并时按实际集合核验；已应用迁移不改写）。
  intent-FK 闭合方案（可执行，G-010-3/011-C12）：`000011` 不含 intent-FK（独立可应用）；
  011 落地后由 010 后续迁移追加该 FK（同一所有者，不改合并顺序、不跨 owner 写表、不重排号）。
  承接批次：010→011 联合集成阶段；在真实联合验收完成门之前闭合，在此之前不声称 FK 约束存在。
- **J2 领取/续租/失效/重领/接管载体及版本语义；010 核验 011 资格**：011 新建
  `execution_claims`（011 所有）：`(intent_id UNIQUE)`、worker 身份、单调 `lease_version`、
  到期时间、状态。010 在发送门禁读取中按 `intent_id` 核验 claim 行（版本有效 + 未过期 +
  无撤销标记），失配即拒绝；claim 行 010 只读。重领 = 新 `lease_version` 更新，旧版本永不复活。
- **J3 读取/变更路径、生效点、锁顺序和时钟依据**：读路径为单事务：先取门禁表级共享锁
 （沿用 009 gates 与 indexer 写协议形态），顺序为 claim/协调锁 → 暂停行 → 恢复版本 →
  授权/scope → 绑定 → 尝试；语句级超时守卫。时钟以 PostgreSQL `now()` 为准，不用应用时钟
  比较。变更经精确版本守卫更新（影响行数 ≠1 即 stale 回滚）。
- **J4 发送与失格竞争保护**：010 发送门禁事务在同一读取序列内重验绑定一致、授权有效、
  无暂停、恢复版本相等、claim 活跃版本相等；任一失配拒绝并记录依据。资格变更与发送的
  竞争靠锁顺序定界：发送决策先行且已不可取消记在途未知并对账，失效先行则拒绝。
  DB 事务/连接失效但网络继续、部分写出、崩溃、超时一律进未知并对账，不记成功或失败。
  自然到期与显式撤销在审计依据中区分表述，拒绝效果相同；无宽限、无 TTL；不套用 009 残差例外。
  有限例外（2026-09-17 新批准，仅自然到期）：最终检查后自然到期的发送残差允许记录并对账，
  不描述为合法在途，不覆盖其他残差，不批准可配置宽限期；排队/退避/重连/重试后 MUST 重估门禁；
  结果明确保留真实结果，仅不确定记 unknown。
  有限例外之二（2026-09-17 新批准，仅失锁未感知窗口）：探针 MUST 用持有保护锁的同一会话/同一事务，
  仅为缓解；检出失效 MUST 阻止尚可取消的发送；仅限当次发送，禁用绕过重估的透明重试；进入延迟为优化目标，
  不宣称窗口极短/极罕见；对账比较记录版本与变更证据，无法判定保序时保留不确定性；确认保护丢失且存在
  门禁失效后发送证据（或无法排除）时冻结该意图后续发送并转人工复核（链观察/查询/对账照常；正常接管非违规证据）；
  人工复核仅解除本残差的独立冻结原因（受控权限+证据+审计），不覆盖任何门禁；恢复发送前重验全部门禁，
  对账永不直接许可重发。此批准不覆盖其他残差，不代表实际验证通过。
- **J5 状态权威、投影版本、修订传播、补偿刷新及未知恢复接口**：权威为 010 attempt 行；
  011 投影行带（来源版本，更新时间），仅当输入版本大于已存才应用；新鲜度不明标可能过期。
  010 写修订链行，011 投影更新器按版本顺序消费；补偿刷新在过期信号下重读权威（机制留 plan，
  不设秒级 SLA）。未知恢复接口：010 按（attempt_id，tx_hash）返回未知 + 已落盘事实 +
  恢复条件；011 消费，不重建意图。
- **J6 迁移/共享入口/配置/测试资源归属、同步顺序和联合验收责任**：迁移暂定 010 `000011_*`、
  011 `000012_*`，纯加法，合并时核验；供给/撤销入口归 007-extension 所有，010/011 只读消费；
  配置沿单链与部署注册表；测试资源单套共享由主编排协调。验收顺序：010 独立 → 011 独立 →
  真实联合（HTTP/PG/Anvil）；010→011 合并顺序；上游同步先于依赖它的验收。
  合流依赖记录：010 先合并时 claim 表缺席，所有发送门禁以 `claim_absent` fail-closed，
  J1–J4 真实发送验收因此尚未执行（不算通过）；联合集成工作区先纳入 011 的真实实现与迁移
  （含 `000012` 建表），适配器读到真实 claim 行，真实联合验收方可执行；适用的联合门禁在
  011 合并到 main 之前完成；替身永不代替真实接线。无循环依赖：010 不需要 011 表即可
  迁移、启动与独立验收；011 不需要 010 表即可迁移与独立验收。

## 联合批次执行记录（2026-09-17，010 单写者）

工作区：`.slim/worktrees/joint-010-011`（分支 `joint-010-011-integration`）。
基线 `f03e825`（010 `d2c14b9` + 011 `beba5e9` 已合入）。
状态：**执行完成**（迁移/适配器接缝 T043–T045、held validation T021/T026/T038、真实联合验收 J1–J5 全部执行且全绿）。
唯一保持 OPEN 的是 A-13（全链 E2E）与 T000-P（生产 provider），按约束不闭合。

### 已执行（含证据命令与结果）

| 任务 | 门禁 | 命令 | 结果 |
|---|---|---|---|
| T043 | 迁移集合 + 合并顺序 + 无改写 | `go test -tags integration -run TestT043MigrationSetMergeOrder ./internal/txlifecycle` | PASS：注入 `migrations/000011/000012/000013`；`goose` applied {..11,12,13}，无 pending；`000011`/`000012` 的 sha256 与各自 lane 提交逐字节一致；`000013` 为本批次新增（`ff17ebba…`/`29613714…`） |
| T044 | intent-FK 加法迁移 | `go test -tags integration -run TestT044IntentFK ./internal/txlifecycle` | PASS：`tx_attempts_intent_fkey` 指向 `public.payment_intents(intent_id)`；`convalidated=true`（非 NOT VALID）；缺 intent 插入 → `23503` 且 `ConstraintName=tx_attempts_intent_fkey`；既有有效行不受影响 |
| T045 | claim 适配器切换真实 `execution_claims` | `go test -tags integration ./internal/txlifecycle` | PASS（92s 全套）：`claimColumns` 读 `owner_id/lease_version/expires_at/state`；`state != 'active'` 记为 `claim_revoked`，自然过期记 `claim_expired`，同一拒绝效果不同 basis；V4/V6/T041 全部通过 |
| T021 | V2 真实 009 signer-serve | `go test -tags integration -run TestT021V2RealSigner ./internal/txlifecycle` | PASS：真实 `app.SignerServe`（in-process，真实凭证 + dev key + `TXHARBOR_SIGNER_*`）；dispatch 钩子观测到 T2 先于首次派发；`keccak256(bytes)==持久 tx_hash==009.tx_hash` 且恢复 sender 相符；篡改签名 → `signature_mismatch`、零派发、记录 `signature_mismatch` 事件 |
| T026 | V4 faultproxy（010↔009 HTTP 代理） | `go test -tags integration -run TestT026 ./internal/txlifecycle` | PASS：timeout/drop/503 → delivery-unknown 同身份重试（3 次，不换身份）；429/invalid/401 → 单次 fail-closed；409 `request_conflict` → `attempt_conflict` 且无重复签名请求；成功体透传；凭据不入错误文本。链上派发分类矩阵仍由 `TestV4DispatchClassification`（lane 链替身，V 层合法）与 `TestV4KnownRowsImmutableAndRetry` 覆盖 |
| T038 | V9b 每 T-boundary 进程击杀矩阵 | `go test -tags integration -run TestV9bCrashMatrix ./internal/txlifecycle` | PASS（8/8）：子进程在每个边界 `os.Exit` 硬杀；before T1→无部分身份；after T1 / after 009 result→`prepared` 同身份重做 T2；after T2→`signed` reconcile-before-dispatch 后派发 accepted；mid-region→`region_aborted_no_dispatch` 零派发；after dispatch before COMMIT→`unknown` probe-first 永不 failed；after COMMIT→`sent` 调用方重试 `already_accepted` 无重复 attempt；after T4→`effective` |
| T046 (J1) | 真实 007 HTTP → 011 intent+claim → 010 消费真实 claim/grant/scope → Anvil → 验证 receipt | `go test -tags integration -run TestJointJ1IntentAuthorizationWiring ./internal/txlifecycle` | PASS：真实 testcontainer PG + 真实 Anvil（`anvil_setCode` ERC-20 Transfer 事件合约 + 充值 sender）+ 真实 in-process `app.SignerServe` + 真实 `WithdrawalHandler` HTTP 建单 + 真实 `WithdrawalExecutionHandler` HTTP 准入 + 真实 `execution.ClaimStore`；010 门禁读真实 `execution_claims`/grant/scope/binding → 真实 009 签名 → Anvil 发送 → receipt `effective/canonical` 且 confirmed；恰好 1 intent/1 attempt，无第二 intent |
| T047 (J2) | 过期/fenced worker 三种发送被拒 + 合法接管 | `go test -tags integration -run TestJointJ2ExpiredWorkerIsolationAndTakeover ./internal/txlifecycle` | PASS：真实 claim 过期后 initial/replay/replacement 三发送均在 010 真实 claim 门禁被拒（零派发、零新增 send 行）；真实 011 `ClaimStore` 接管单调递增 `lease_version`；接管者重放通过全部门禁并派发（Anvil `nonce_too_low` 为链上裁决，非拒绝），同意图/同 nonce/同 attempt 历史，恰好 1 intent |
| T048 (J3) | 真实派发响应丢失 → 联合 unknown 对账 | `go test -tags integration -run TestJointJ3UnknownReconciliation ./internal/txlifecycle` | PASS：节点已接收但 010 丢失响应 → `unknown` 且 bytes/hash 完整；011 `Reconciler` 经真实 010 authority reader 诚实持久化（不判失败），链上证据确定后消费 confirmed-sent 事实完成；attempts/intents/sends 均 1，无 repaying |
| T049 (J4) | 联合 reorg 修订 + 011 投影版本顺序 | `go test -tags integration -run TestJointJ4ReorgRevisionAndProjectionOrder ./internal/txlifecycle` | PASS：确认后 reorg → receipt `orphaned` + attempt `orphaned` + `orphaned` 事件；再入链 → `reconfirmed`；不重建支付（1 attempt/1 intent）。011 `ReadProjection`：新版本 100 生效、旧版本 50 永不覆盖、authority 读取失败标 `possibly_stale` |
| T050 (J5) | 失锁残差（可检测路径）冻结 + 受控解除 | `go test -tags integration -run TestJointJ5LockLossDetectablePath ./internal/txlifecycle` | PASS：真实 pause 的 `created_at` 早于记录 `pause=none` 的派发 → 010 冻结（`tx_intent_freezes` + `frozen` 事件），011 `IsFrozen` 观测到；冻结期一切发送零派发、观察/对账仍可用；受控解除仅解除本冻结原因（011 标记按设计粘滞），重发重验全部门禁（`pause_present`）；断言了非声称项 |

### 本批次由真实接缝暴露的 010 交付缺陷（需回填 010 交付分支）

1. `internal/txlifecycle/calldata.go` `TransferCalldata`：`amount.FillBytes(append(out, make([]byte,32)...))`
   会填充**整个** 68 字节缓冲，选择子与收款人字被清零（实测 `selector=00000000`）。V8 用合成
   receipt 从未触达该路径。已改为只填 amount 字。
2. `internal/txlifecycle/attempt.go` `CanonicalEnvelope`：`canonicalDecimalOptional("")` 返回 `"0"`，
   使 `omitempty` 失效，type-2 信封恒带 `gas_price:"0"`，真实 009 以 `validation_failed`
   拒绝（实测 field=gas_price）。已改为按 fee 形状只发一个维度。

两处均为 010 交付分支修复，需随 010 回填；不改变 Q1–Q3、G-010-1/2、PB-C1/C2、M1n/M2/M3、
阈值与三事实分离。

### 本批次由真实接缝暴露的 009 交付缺陷（需回填 009 交付分支）

- `internal/signer/submit.go`：fee-replacement（anchor 非 nil）先经 `EvaluateGrantReuse` 放行，
  但随后**无条件**执行 `EvaluateGrantScope`，其 `scope.request_id == req.SigningRequestID`
  等值检查会拒绝 anchor 作用域下的任何新签名身份 → 复用同一 PB scope 的费用替换永远无法签名。
  J2 的 replacement 腿因此改走 fresh grant（010 fresh 分支，另一 scope 的
  `request_id == 新 signing_request_id`）才能到达 010 claim 门禁。属 009 交付分支修复事项，
  未在本批次改动 009 代码。

### 联合验收门禁（gating statement）

- J1–J5（T046–T050）与 T038 的真实联合验收**已执行且全绿**（见上表；全套
  `go test -tags integration ./internal/txlifecycle` ok，153.8s）。
- **适用联合门禁已完成，011 可在评审后推进其合并**；010→011 合并顺序保持不变。
- A-13（全链 E2E）保持 OPEN；T000-P（生产 provider）保持 OPEN。本记录**不声称 A-13 闭合**：
  本批次由测试侧提供 010↔011 的 `LifecycleAdvancer/LifecycleReader` 适配器与 008 binding/
  Anvil 事件合约等联合测试资源，生产端的同款接线仍属 A-13 范围。
- 替身/夹具/010 独立通过均未用作联合证据；上表 J 相关场景全部使用真实 HTTP/PG/Anvil/009。

### 未执行项与原因

- 010 本批次无未执行项（T043–T051 全部执行）。
- 保持 OPEN（按约束不闭合）：A-13 全链 E2E、T000-P 生产 provider。生产端 010↔011
  lifecycle 适配器与 008 分配接线仍属 A-13 范围，本批次由测试侧适配器覆盖联合验收。

### out-of-lane 失败已闭合：000014 intent-FK 修复（joint 分支专属）

- `internal/db/withdrawal_execution_migration_integration_test.go` 的两处 stale 断言已在联合分支
  按 post-repair 集合 `{10 PB, 11 010, 12 011, 13 guarded, 14 repair}` 适配：
  `TestWithdrawalExecutionMigrationNumberIsProvisional`（3d8556e 记录的“000011 必须缺席”）
  与 `TestWithdrawalExecutionMigrationDownRemovesOnlyItself`（夹具改为精确停在 000012）。
  重复版本检测与精确集合断言保留，未删除任何保护。
- 联合分支 `000013` 字节对齐 010 lane 的 guarded 版本（sha256 `45e4b8a1…`）；新增
  `000014_intent_fk_repair.sql`：guard no-op 已 recorded 而 `payment_intents` 后到的库在
  000012 之后补齐 FK；`payment_intents` 缺席时 RAISE（绝不静默跳过），约束存在则不重复添加，
  Down 仅 `DROP CONSTRAINT IF EXISTS`。
- **该修复迁移为 joint 分支专属**：000012 落地前禁止 cherry-pick 回 010 独立候选
  （010 独立契约仍由 guarded `000013` 承担）。

### 011 侧输入需求（不修改 011 任务框，011 lane 拥有）

- 011:T041 工作区：已由 010 建立并纳入 011 真实实现 + `000012`（本分支 `f03e825`）。
- 011:T042 迁移：`000012_withdrawal_execution.sql` 与 lane 提交逐字节一致；`000013` 为 010 追加。
- 011:T043 intent-FK：由 010 的 `000013_tx_lifecycle_intent_fk.sql` 闭合；011 无需改表。
- 011:T044 claims：010 的 `claimColumns` 已对齐真实 `owner_id/state`；J2 语义不变。
- 011:T045 联合测试：J1–J5 已在联合工作区执行（真实 007 HTTP + 011 + 010 + 009 + PG + Anvil）；
  010↔011 的 `LifecycleAdvancer/LifecycleReader` 适配器当前为联合测试侧实现（见 `joint_*_integration_test.go`），
  生产端接线仍属 A-13。
- 011:T046 记录：联合门禁已完成，011 可在评审后推进合并；A-13/T000-P 保持 OPEN，不得据此声称全链 E2E 闭合。

## 最终联合验收记录（2026-09-18，clean tree）

工作区 `.slim/worktrees/joint-010-011`（`joint-010-011-integration`）；基线 `0946638`
（production wiring unit），测试提交 `ad1cd84`；本轮无推送/PR/合并/部署。日志目录
`/tmp/opencode/joint-final/`。

### 本轮验收口径（与上一批次记录的差异）

- J1–J5 **一律经生产 worker Driver 路径**执行：`app.NewJointWithdrawalWorker` →
  `worker.Driver.IssueAndAdvance` → `txlifecycle.NewLifecycleLive`（010 适配器）→
  真实 010 Store 门禁 → 真实 009 signer-serve → 真实 Anvil + 测试 ERC-20；reconcile 腿经
  `worker.Reconciler.ReconcileIntent`；binding 由真实 008 Allocator 分配。
- 原 `joint_j*_integration_test.go`（直接调 Store）保留为**补充性 shared-state 覆盖**，
  已在测试注释与 `t.Log` 中标注其不单独构成 worker-Driver 证据；其运行仍全绿，作为附加佐证。

### 逐项证据（全部 PASS；`-tags integration -count=1`）

| 项 | 命令 | 日志 | 结果 |
|---|---|---|---|
| J1–J5（worker Driver，T046–T050） | `-run TestJointDriverJ ./internal/txlifecycle/` | `j1-j5-driver.log` | 5/5 PASS |
| J1–J5（补充 shared-state） | `-run 'TestJointJ[1-5]' ./internal/txlifecycle/` | `t046-t050-supplementary.log` | 5/5 PASS |
| T038 V9b 进程击杀矩阵（V9b） | `-run TestV9bCrashMatrix ./internal/txlifecycle/` | `t038-crash.log` | 8/8 子边界 PASS |
| 010 T043/T044 | `-run 'TestT043MigrationSetMergeOrder\|TestT044IntentFK' ./internal/txlifecycle/` | `010-t043-t044.log` | PASS |
| 011 T043/T044 | `-run 'TestT043JointClaimsIntentFK\|TestT044JointClaimsColumnParity' ./internal/txlifecycle/` | `011-t043-t044.log` | PASS（含 4 个语义子探针） |
| 011 T045 readiness（wiring+J） | `-run 'TestJointWorkerProductionWiring\|TestJointDriverJ' ./internal/txlifecycle/` | `011-t045-readiness.log` | PASS |
| 回归 txlifecycle 全套 | `./internal/txlifecycle/` | `txlifecycle-full.log` | `ok` 232.5s |
| 回归 execution 全套 | `./internal/execution/` | `execution-full.log` | 仅预存 V12 失败（见下） |
| 回归 app 全套 | `./internal/app/` | `app-full.log` | `ok` 173.4s |
| 回归 db 全套 | `./internal/db/` | `db-full.log` | 9 项预存失败（见下） |
| 单元全套 | `go test -count=1 ./...` | `unit-all.log` | 仅预存 V12 失败 |
| lint/build | `gofmt -l .` / `go build ./...` / `go vet`（两 tags） | `lint-build.log` | 全部 exit 0 |

### 011 T043–T045 联合重验（先置 `[ ]`，通过后恢复 `[x]`）

- **T043**：此前 lane 记录只复用了 010 的 `tx_attempts_intent_fkey` 证据（并声明未重跑）；
  本轮新增 `TestT043JointClaimsIntentFK`，对 011 自有的 `execution_claims_intent_fkey` 做
  命名 + `convalidated` + 缺 intent 23503 探针，两条 FK 均闭合。
- **T044**：新增 `TestT044JointClaimsColumnParity`：真实列集逐序等于 frozen J2 形状
  （`specs/011-withdrawal-executor/data-model.md` Table 2），010 单一映射列
  （`intent_id/owner_id/lease_version/expires_at/state`）全部存在，intent-unique、
  `lease_version>=1`、expiry、revocation-marker 语义约束按名命中。
- **T045**：lane 记录中的三个阻塞前置（生产适配器、真实 008 分配、链上 transfer 合约）本轮均已满足
  （生产适配器 = `txlifecycle.LifecycleLive` + `app.NewJointWithdrawalWorker`；008 = 真实
  `nonce.Allocator`；链上 = Anvil + 测试 ERC-20 emitter）；J1–J5 全绿证明 011 participant 路径
  （准入/intent+claim supply、fencing/takeover、reconcile loop、projection updater、freeze consumer）
  在真实接线中运行。生产进程入口对适配器的装配（`WithdrawalWorkerCommand` 仍用 standalone
  构造器）仍属 A-13 范围，见门禁声明。

### 预存失败（非本轮回归，已取证）

1. `internal/execution TestV12NoSecretsIn011Paths`：`internal/config/config.go` 注释含
   `RawTransaction`/`eth_sendRawTransaction` 令牌（line 133 附近）。
   `git diff c991680 -- internal/config/config.go` 为空（本轮未触碰）；在
   `git archive c991680` 的 pristine 树上运行得到**逐字节相同**的失败输出
   （`v12-pristine-c991680.log`）。不修复、不隐藏，列为跨 lane 遗留发现。
2. `internal/db` 9 项：`TestT036SequenceA/B/C`、`TestT040OverlayGreenOnEmptySequence`、
   `TestT041GapFillSequenceD`、`TestT042RollbackRevertsTenBeforeNine`、
   `TestT042DownOfAppliedThenRenumberedNumberForbidden`、
   `TestAuthzScopeMigrationDownRemovesOnlyItself`、
   `TestWithdrawalExecutionMigrationIsAdditiveOnly` —— 均为 009/PB-era 硬编码 `{1..10}`
   序列或 lane-local 集合断言，在联合迁移集 `{…11,12,13,14}` 下失败。在
   `git archive 0946638`（本轮改动前的 tip）上运行得到**完全相同的 9 项失败与断言文本**
   （`db-pristine-0946638.log`；唯一差异为非确定性的 map 遍历顺序），证明先于本轮存在。

### 记录缺口（不修复，供编排裁决）

- `Advance(ActionReplace)` 返回 `refused_basis`（010 无费用构造策略）：J2 替换腿按
  blocked-with-reason 记录，未伪造 fee policy；010 对已备好 replacement attempt 的
  claim 围栏由补充性 direct-Store 测试覆盖。
- 011 issue 阶段对 frozen intent 的拒绝携带 `basis="frozen:<class>"` 而 `Refusal` 为空
  （零写入、零派发）：本轮如实断言 basis，未改 011 语义。

### 门禁声明

- 真实联合验收（真实 007 HTTP + 011 + 010 + 009 + PG + Anvil 与测试 ERC-20；替身/夹具/
  010 独立通过均未用作联合证据）已在联合工作区通过；**适用联合门禁已完成，011 可在评审后
  推进其合并**，010→011 合并顺序不变。
- A-13（全链 E2E）保持 OPEN（生产进程入口的适配器装配仍属其范围）；T000-P（生产 provider）
  保持 OPEN。本记录不声称 A-13 闭合。
- 010 任务框 `T038/T043–T045/T046–T051` 全部由本轮 clean-tree 证据复现通过（`T042` 保持
  `[ ]`）；011 任务框 `T043/T044/T045` 经重验恢复 `[x]`。
