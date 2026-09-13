# Research: 004-deposit-detection

**Branch**: `004-deposit-detection` | **Date**: 2026-09-13 | **Spec**: `specs/004-deposit-detection/spec.md`

All decisions below are behavior-locked by the spec + 2 adopted clarifications (Session 2026-09-13:
temporarily-vs-structurally-missing upstream coverage; layered pause recovery).
Open implementation detail lives only where the spec explicitly defers it (D3–D5, OQ1–OQ3).
Technical Context has no NEEDS CLARIFICATION: language, dependencies, storage, testing and
platform are all fixed by the repo (Go 1.26.5, go-ethereum v1.17.5, pgx/v5, goose v3, testcontainers,
Linux single deployment, Anvil + controllable fakes locally).

A central finding of this research: **004 needs no RPC calls at all** — it is a pure
database consumer of 003-persisted rows. There is no FilterLogs, no KindIncomplete, no provider
truncation risk inside 004. The only "completeness" question is upstream coverage, answered by
003's own checkpoint monotonicity (R2). A second finding: **no upstream (002/003) schema change
is required** — the whitelist-membership check needed for structural-gap classification is done
by recomputing 003's `config_hash` from the shared env (R5).

## R1 — 存储与协调：新表 + 复用 `indexer_lease` 协调行 + 第三 serveLoop（OQ1 已决）

- **Decision**: 新增五张表（见 `data-model.md`）：`deposit_observations`（充值观察，PK 即来源身份）、
  `deposit_checkpoint`（充值处理进度，按 `chain_id` 单行）、`deposit_pause`（暂停/结构性缺口信号，单链单行）、
  `deposit_config_history`（配置版本历史 + 授权审计，见 R11；当前哈希不能替代历史版本与生效边界）、
  `deposit_pause_audit`（暂停实例事件审计；无它则解除无痕，见 R6/解除结果判定）。
  复用 002/003 的接口与机制：
  - 复用 `indexer_lease` 行作为全链唯一协调行：充值写事务同样先 `INSERT … ON CONFLICT DO NOTHING`
    确保行存在，再 `SELECT … FOR UPDATE` 取锁，持锁后用后续独立语句裁决（Read Committed 语句级新快照）。
    锁顺序唯一且固定（永远只锁 lease 行），与 002/003 写事务互斥串行，不存在死锁环。
    持有所有权的进程在事务之外不占用协调行：行锁只在毫秒级短事务内存在。
  - 复用 `chain_blocks` 作为链视图真相、`erc20_transfer_logs` 作为唯一日志真相：004 不存第二份区块或日志真相，
    只存派生观察 + 进度 + 暂停。
  - 不复用 `log_checkpoint` / `log_pause` / `indexer_pause` 行：三个流各有独立进度与暂停记录；
    充值提交裁决同时要求三行缺席：`deposit_pause` 无行（本流未暂停）、`log_pause` 无行、
    `indexer_pause` 无行（任一上游暂停同样阻止充值提交，澄清 Q2 的"服从"语义）。
  - 运行形态：沿用 003 `Coordinator`（单获取循环 + 单心跳，`internal/indexer/coordinator.go`）。
    现状是该文件硬编码恰好两个并发循环（`headerServe` + `logServe`，`coordinator.go:124-125`），
    004 在实现阶段做机械扩展——增加第三个 `depositServe` 并发循环，header/log 两循环行为不变。
    任一循环返回失权错误或心跳失联 → 全部取消、重回获取（沿用既有语义）。
- **Rationale**: 与 003 R1 相同的两层机制论证（所有权 vs 短持行锁）；三类写事务在同一行锁下排队，
  暂停提交后发起的任何充值事务必见暂停行。不为 004 自建锁：两把锁引入锁顺序问题，且上游暂停与充值提交的
  互斥还需跨锁协议，得不偿失（章程 XIII）。
- **Alternatives considered**:
  - 004 自建 lease/锁行：否决——同 R1，跨锁互斥协议复杂且无收益。
  - PostgreSQL `LISTEN/NOTIFY` 或轮询上游内存状态：否决——直接轮询 003 的持久化 checkpoint 行已足够；
    内存通知不可靠（重启丢失），与"PG 唯一真相"相悖；引入通知 infra 违反简单优先。
  - 复用 `log_checkpoint` 加流标识：否决——规格 FR-08 明确禁止共享进度记录；且 004 进度语义
    （充值处理位置）与日志扫描位置是不同流的水位。
- **兼容影响**：`000004` 新迁移只建新表，不改 002/003 共七张表的定义；`indexer_lease` 行被三类写事务
  外加授权事务共享，锁竞争只在毫秒级短事务内，可接受；`coordinator.go` + `serve.go`
  的接线变更是机械增量（新增循环注册），header/log 行为不变，002/003 回归测试覆盖。
  第四表 `deposit_config_history` 的最小性：没有它，结构缺口分类所需的历史白名单、回放所需的历史生效边界、
  授权审计都无处可存；用"只存最新哈希 + 运维记忆"替代即把正确性外包给人，不符合章程 I/III。
  第五表 `deposit_pause_audit` 的最小性：没有它，暂停释放/合并无 durable 痕迹，
  条件解除的结果判定（返原 vs 陈旧）无处可查；仅靠外部日志不符合章程 III/XII。

## R2 — 事务边界、覆盖证明与旧 worker 隔离：整单元一事务 + 精确守卫 + `N_u > b` 证明

- **Decision**: 一个处理单元 `[a, b]`（连续闭区间，块粒度）一次短事务，事务内零外部调用
  （004 无 RPC，只有 SQL；`SET LOCAL statement_timeout = '5s'` 沿用 `writeGuard` 护栏）。
  协议镜像 003 五步法，关键差异是覆盖证明不依赖 RPC 而依赖 003 checkpoint 单调性：
  1. 事务外：确定待处理位置 `a`（读 `deposit_checkpoint.next_block` 或空进度 + 配置起点）；
     读 003 `log_checkpoint` 得 `(S_u, H_u, N_u)`；缺口分类（R5）；若可处理则读 `[a, b]` 日志行
     （`ORDER BY block_number, log_index`），逐条解析与匹配（R4 + 生效高度规则）。
  2. `BEGIN` → 语句超时护栏 → 确保 lease 行 → `FOR UPDATE` 取协调锁。
  3. 持锁后独立语句裁决（任一失败即 `ROLLBACK`）：三暂停行（`deposit_pause`、`log_pause`、
     `indexer_pause`）均无行；lease 的 owner/token/有效期成立；`deposit_checkpoint` 精确守卫
     （`next_block = a` 且 `start_block`/`config_hash` 与本次配置一致）；**覆盖重证明**：
     重读 `log_checkpoint` 仍有 `next_block > b`（捕获锁等待期间的上游变化——上游只会推进，不会回退，
     故重读只可能更完整；若上游行消失则视为链视图异常，拒绝）；`[a, b]` 引用区块逐块 canonical 重裁决。
  4. 写入：`INSERT` 充值观察行（`ON CONFLICT DO NOTHING`）→ 重读冲突行做内容比对
     （一致则收敛，不一致则回滚走冲突暂停）→ 行数核对（本单元去重后来源身份数 ==
     实际插入 + 已存在一致行数 + 合法零生成数）→ `UPDATE deposit_checkpoint SET next_block = b+1`
     （带 `WHERE next_block = a` 精确条件，`RowsAffected != 1` 即判过期）→ `COMMIT`。
  5. 提交结果未知时：不假设成功/失败，下一轮重读 `deposit_checkpoint` 并以精确守卫重新裁决，幂等继续。
- **Rationale（覆盖证明的可靠性）**: 003 只在整区间完整提交后才推进 `next_block`（其 I2/I3），
  故 `N_u > b` ⟺ `[a, b]` 的日志集合已完整持久化——"零日志"此时才是合法空区间，而非缺失。
  003 提交是原子的：使 `N_u > b` 成立的事务在 004 的 checkpoint 重读语句之前已经提交，
  因此 004 后续的行读取语句（Read Committed 新快照）必见完整行集。顺序是关键：
  先读 checkpoint 再读行；任何交错提交只会增加覆盖，不会减少。
- **Alternatives considered**: 逐块提交（否决：单元语义要求整批原子；且块粒度已是最小可用单元，
  更细的逐日志提交徒增事务数）；宽松守卫 `next_block <= a`（否决：允许跳块，违反 I3）；
  事务内读上游行做覆盖判断后不再重读（否决：检查与提交之间存在窗口，必须持锁后重裁决）。

## R3 — `config_hash` 编码与比较流程（D4 落实）

- **Decision**: 充值配置身份 `deposit_config_hash = SHA-256(UTF-8("deposit:v1\n" + "start:<S>\n" +
  `asset:<contract>:<effective>` 行（排序）+ `watch:<address>:<effective>` 行（排序）))`，
  无 BOM、末尾无换行，hex 64 位小写存 `CHAR(64)`。载体：`TXHARBOR_DEPOSIT_CONTRACTS` 与
  `TXHARBOR_DEPOSIT_WATCH_ADDRESSES` 逗号分隔，条目为 `0x…[ :effective]`（`:height` 后缀可选，
  缺省等于全局起点；`>=0` 整数）。流程：启动时逐项 `IsHexAddress` 校验 → 小写 → 排序去重
  （同地址/合约不同生效高度视为不同条目，不去重）→ 任一集合空即拒绝启动 → 计算身份。
  初始化：无 `deposit_checkpoint` 行时，在首个单元提交事务内原子写入
  `(start_block, config_hash, next_block=a)`，冲突时重读已有行并执行同样比较。
  重启：比较配置 `(start_block, config_hash)` 与行内值；任一不一致即拒绝启动（报错并退出码 1），不触碰数据。
- **标准测试向量**（`printf` 无尾换行实测，T 实现必须复现）：
  输入 `deposit:v1\nstart:0\nasset:0x1111…1111:0\nwatch:0xaaaa…aaaa:0` →
  `31822b65a6444c91bdaaa04a86582f4db25f35d5dd8ee02b7c2c12a4cf6600f0`。
- **Rationale**: 与 003 R3 同构（域分隔符 `deposit:v1` 避免跨流碰撞）；缺省生效高度 = 全局起点使
  最简配置（纯地址列表）合法，同时显式 `:height` 覆盖新增资产/地址场景；"比较行内起点而非 next"
  避免正常推进后误判。
- **Alternatives considered**: 独立 `EFFECTIVE_HEIGHTS` 映射变量（否决：地址与高度分离的两个变量易错配，
  `addr:height` 内聚更 khó 错）；把批次/超时参数纳入身份（否决：规格明确排除）。

## R4 — Transfer 解析：topics/data → 精确整数，无浮点（FR-02 落实）

- **Decision**: 对每条来源行：断言 `topic0` 为 Transfer 签名
  `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
  （003 已校验，此处为纵深复核；实现中以 `Keccak256("Transfer(address,address,uint256)")` 断言，
  不得手写魔法字符串无断言）；取 `topic1` 低 20 字节为 sender、`topic2` 低 20 字节为 recipient
  （高 12 字节必须为零，否则整批失败）；`data` 32 字节 big-endian 转 `big.Int` → 十进制字符串存
  `NUMERIC`（`CHECK (amount > 0)`，零值不建行）；金额字段在 Go 侧全程 `big.Int`，日志与错误输出只打
  十进制字符串，**任何路径不出现 float**。recipient 规范化后查监控集合、contract 查资产集合、
  高度查三元组（FR-05）——三者全命中且金额非零才建行。
- **Rationale**: 003 落库时已做 8 项校验，004 的复核是廉价的防腐败检查（DB 手工篡改、未来上游 bug 不会
  静默变成充值）；`NUMERIC` 精确容纳 uint256 全范围（`float64` 只能精确到 2^53，章程 I 禁止）。
- **Alternatives considered**: 存 32 字节 hex 原文另加解析列（否決：观察行需要可直接消费的金额，
  双列增加不一致面；来源 hex 仍在上游行可查）；`BIGINT` 金额（否决：uint256 远超 int64）。

## R5 — 缺口分类算法：暂时等待 vs 结构报错，无超时判定（澄清 Q1 落实）

- **Decision**: 对待处理位置 `p`（及所属单元 `[a, b]`），按以下顺序判定，全部使用已持久化数据：
  1. 读 003 `log_checkpoint` 得 `(S_u, H_u, N_u)`；用 `TXHARBOR_LOG_CONTRACTS`（同一部署共享 env）
     重算 003 `config_hash`，必须等于 `H_u`，否则拒绝启动（上游配置漂移，需人工对齐；不是等待）。
  2. 若所需资产 ∉ 上游白名单，或任一所需生效位置 `< S_u` → **结构性缺口**：报错并停止，
     记录缺口范围、原因（`below_upstream_start` / `asset_not_indexed`）、受影响配置版本；
     人工修复补扫后重验完整，从原缺口幂等恢复。
  3. 若 `p >= N_u` → **暂时性缺口**：标记等待（state=1/4，不建暂停行），补齐后自动继续。
  4. 否则覆盖完整 → 处理。覆盖完整性证明见 R2（`N_u > b`）。
  超时只驱动重试节奏，**永不作为结构性 verdict**。
- **Rationale**: 结构性判定全部来自配置比较（白名单成员、`S_u` 下界），与时间无关，故不存在
  "等多久算永久"的 Sorites 问题；暂时性判定来自单调水位 `N_u`，等待必有上界语义（上游推进即解除）。
  用共享 env 重算 003 身份是关键技巧：003 只存 hash 不存名单，但同一部署的 env 即名单真相，
  hash 相等即证明"眼前的名单就是上游用的名单"，无需上游 schema 变更（R10）。
- 跨版本分类：回放区间适用的历史白名单与生效高度取自 `deposit_config_history` 中对应版本的快照
  （每次授权转换时写入），不依赖运维记忆或上游 schema；当前哈希永不替代历史版本（R11）。
- **Alternatives considered**: 上游新增白名单持久化表（否决：可用现有数据回答，不动上游 schema；
  若未来 003 白名单支持历史版本，再议）；超时阈值判定结构缺口（否决：澄清明确禁止，慢速上游会被误判）。

## R6 — 分层恢复：上游 auto-resume + 自身 manual-release + 重验门（澄清 Q2 落实）

- **Decision**: 提交裁决要求三暂停行皆无。恢复路径：
  - 仅服从上游暂停（`log_pause` 或 `indexer_pause` 存在）：004 不建自身行；每轮重验
    （上游行消失 + 链视图一致 + 所需覆盖完整 + 本位置仍有效）→ 自动继续。
  - 004 自身暂停分两条解除路径（均不绕过重验，写入授权只来自持锁后重裁决；两条都按实例条件执行并写审计）：
    (a) 人工解除：`DELETE ... WHERE chain_id AND pause_id AND revision`（读取时所见值），
    同一事务写 `deposit_pause_audit` 行（操作者、原因、实例、修订、版本、快照），审计失败则回滚；
    影响 0 行 → 重读后重新决定（陈旧条件不得解除新实例），结果判定查审计表（见 data-model Table 3 释放规则）；
    下一轮完整裁决通过才继续，适用于无需身份变更的暂停（`chain_view_changed`、`validation_failed`）；
    影响 0 行后的结果判定查审计表（命中返原结果，未命中按陈旧处理，见 data-model）；
    (b) 授权转换事务（R11）：适用于配置变更，事务内含释放语义——同一事务完成重验 + 身份更新 +
    位置回放 + 暂停处置（含实例匹配与审计行），重验失败则整体回滚、暂停保持。
  - 需回退/修订历史（旧分叉失效、历史观察修订、进度回退）：004 无此能力；保持暂停，归 006。
    `deposit_pause.detail` 记录 `needs_006=true` 供识别。
  - 并发保护：解除与提交之间无窗口——提交事务在持锁后重读暂停行，失权 worker 与"解除后又出新问题"
    的竞态都被同一裁决覆盖；首暂停获胜（`ON CONFLICT DO NOTHING` + 重验），后者收敛。
- **Rationale**: 与 003 暂停语义同构（人工解除 + 重验门），运维心智一致；"解除≠授权"把安全条件
  放在原子提交路径内，而非前置检查，消除 TOCTOU。
- **Alternatives considered**: 自身暂停自动恢复（否决：原因消除的定义不可验收，且与 003 惯例冲突）；
  解除 API/HTTP 端点（否决：003 已定"人工 SQL 或后续阶段"，004 不新增端面；权限=DB 权限，审计=行 + 日志）。

## R7 — 配置项（env-only，沿用 001/002/003 约定；D4 落实）

- **Decision**: 新增三项，复用其余：
  - `TXHARBOR_DEPOSIT_START_HEIGHT`（必需，无默认值：资金语义下界必须显式；`>=0`）。
  - `TXHARBOR_DEPOSIT_CONTRACTS`（必需，逗号分隔 `address[:effective]`；空白/空集合拒绝启动）。
  - `TXHARBOR_DEPOSIT_WATCH_ADDRESSES`（必需，同上语法；空白/空集合拒绝启动，绝不退化全地址监控）。
  - `TXHARBOR_DEPOSIT_BATCH_BLOCKS`（可选，默认 500：与 003 同值的往返开销折中；调参不改变历史语义）。
  - 复用 `TXHARBOR_INDEX_POLL_INTERVAL / RETRY_INITIAL / RETRY_MAX`（轮询与退避语义与既有流一致，
    不新增旋钮）；`TXHARBOR_INDEX_RPC_TIMEOUT` 与 004 无关（零 RPC），不引用。
- **Rationale**: 必需项无声默认值会掩盖资金意图（003 R6 同理）；`:effective` 后缀把"地址—高度"绑定在
  同一条目内，避免双变量错配；沿用既有退避/轮询旋钮符合简单优先。
- **Alternatives considered**: 独立生效高度映射变量（否决：见 R3）；独立 RPC 超时（否决：零 RPC，YAGNI）。

## R8 — 可观测：`deposit_*` 指标组 + 结构化日志 + SQL 诊断（OQ3 已决，D5 落实）

- **Decision**: 新增 `deposit_` 前缀指标组（见 `contracts/observability.md`）：
  `txharbor_deposit_next{chain}`（空进度删序列）、`txharbor_deposit_lag_blocks{chain}`
  （`= 003 next − deposit next`；任一空进度不暴露）、`txharbor_deposit_state{chain}`
  （0 运行 / 1 等待上游覆盖 / 2 重试 / 3 暂停 / 4 结构性缺口停止）、
  `txharbor_deposit_observations_total{chain,result}`（`matched|nomatch|zero|invalid`）、
  `txharbor_deposit_pause_total{chain}`、`txharbor_deposit_transition_total{chain,result}`
  （`ok|error|rejected`，授权转换审计计数；明细在 DB history 行）。`/readyz` 语义冻结（暂停不得翻转 readyz，与 002/003 同理）。
  结构化日志字段固定并经 `logx.Redact`；`deposit_checkpoint`/`deposit_pause` 行可经 SQL 直查。
- **Rationale**: 与 002/003 契约同构；state=4 把"结构性停止"与"暂停"区分开，直接回答运维
  "卡住还是等上游"；计数器按结果分类使"误生成/漏判"在指标层面可断言。
- **Alternatives considered**: 新增 HTTP 状态端点（否决：指标+日志+SQL 已覆盖 SC-09）；
  复用 `log_state` 同名序列（否决：两流状态独立，混用无法区分"日志跑/充值停"）。

## R9 — 验证策略：真库 + Anvil（全栈 happy-path）+ DB 播种（故障/并发场景）

- **Decision**: 约束/事务/协调类断言一律真实 PostgreSQL（testcontainers，`//go:build integration`，
  复用既有 helper）；Anvil 部署测试 token 产出真实 Transfer 日志覆盖匹配主路径；
  缺口/暂停/冲突/并发场景用 DB 直接播种（`chain_blocks` canonical 行 + `erc20_transfer_logs` 行，
  即"上游已提交"状态）——播种即前置条件布置，不是 mock 被测逻辑。
  DB 瞬时故障（连接丢弃、提交未知态）复用 003 的 conn-wrapper 故障注入模式
  （`logscanCommitDropConn` 同类）；并发测试跑 `-race`，但 `-race` 只覆盖进程内数据竞争，
  跨 worker 一致性必须用两个真实 DB 连接（独立 pool + 独立 lease 句柄）做集成断言。
  验收矩阵 10 项全映射见 `quickstart.md`；硬断言（无跳位、无部分提交、暂停零推进、单次有效推进、
  冲突必败、结构缺口必停）为必过门禁。授权转换专属验证（`quickstart.md` D11）：
  授权失败全回滚、旧版本在途隔离、重复授权请求收敛、授权提交未知恢复、历史回放幂等、
  收缩边界 grandfathered、混合变更双规则、多次版本切换嵌套、结构缺口判定、暂停不可越权解除——
  逐项真库断言，细节见 R11。
- **Rationale**: 章程 X/XI：锁、事务、并发、恢复必须集成测；004 的全部正确性都在 DB 边界上，
  mock 不可替代。Anvil 负责"链确实产出标准 Transfer"的真实性，DB 播种负责故障确定性。
- **残余风险诚实声明**：此前本地测试偶发失败原因未知（保留记录）；实现阶段对 `-race` 双连接测试
  必须重复运行并如实报告次数与结果，不得以"本地偶发"为由豁免门禁。

## R10 — 003 接口核验：已存在 / 待核验 / 补充契约清单

- **已存在（可直接依赖）**：`erc20_transfer_logs`（PK + 块内 UNIQUE + `(chain_id, block_number)` 索引，
  覆盖 004 全部读取模式）；`log_checkpoint`（`start/next/config_hash` 齐全）；
  `log_pause` + `indexer_pause`（服从目标）；`chain_blocks`（canonical 真相）；
  `indexer_lease`（协调行 + `Acquire/Heartbeat/Token`）；`newBackoff`（±25% 抖动）；
  `writeGuard` 5s 语句超时；`logx.Redact`；`Coordinator` 单获取循环模式；
  `TXHARBOR_LOG_CONTRACTS` 共享 env（R5 重算用）。
- **待核验（实现前置条件，不阻塞规划）**：`Coordinator` 第三循环注册点的确切形状；
  `serve.go` 接线位置；goose 迁移顺序（`000004` 在 `000003` 之后自动收录经 `embed.go *.sql`）；
  pgx → `NUMERIC` 的 `big.Int` 十进制映射写法（常规用法，实现时单测锁定）；
  授权事务载体形状（SQL 事务函数 vs 受控 SQL 脚本二选一，语义见 R11，实现时锁定并给出选择理由）。
- **所需补充契约**：**Q3–Q6 复核后仍无需上游（002/003）代码或 schema 变更**。覆盖所需的全部信息
  （白名单成员经 env、上限经 `next_block`、canonical 经 `chain_blocks`）均已存在；
  历史版本所需的旧白名单/旧生效边界改存于 004 侧 `deposit_config_history` 快照（R11），
  不向 003 追索历史配置；
  唯一的既有文件触碰是 `coordinator.go` + `serve.go` 的循环注册接线（机械增量，header/log 行为不变，
  由 002/003 回归测试锁定）。**覆盖修复绝不等于改哈希**：授权事务必须对回放区间重做 `N_u` 覆盖证明，
  仅记录新哈希而不验证覆盖的实现视为违反 FR-07，发现即回归失败。若实现阶段发现本清单遗漏，显式报告，不静默改上游。

## R11 — 受控配置转换：授权事务 + 版本历史 + 多次切换（澄清 Q3–Q6 落实）

- **Decision**: 授权转换是唯一的合法身份更新路径（默认拒绝不变），语义如下：
  - 入口与权限：特权 SQL 操作，由 DB 操作员角色执行（沿用既有 DB 权限体系，不新增服务/端点/infra）。
    载体二选一（实现时锁定）：库内事务函数，或受控 SQL 脚本；符合性标准是 R11 全部语义条，而非载体名。
  - 原子内容：身份 H→H′ + 位置 next→replay_from + 暂停行处置 + history 行 + 审计字段
    （request_id、expected_old_seq、操作者、时间、旧新哈希、位置变化、原因），同一事务，任一失败全回滚；提交成功即生效，失败全回滚。
    缺审计字段即回滚——审计不是附带日志，是提交有效性的一部分。
    版本身份为 `(chain_id, version_seq)`（本链递增，授权事务内取 max+1），内容哈希禁作版本身份；
    相同内容复现即新行新 seq。
  - 前置重验（授权不绕过）：链视图一致、上游覆盖完整（对回放区间重做 `N_u` 证明，**禁以记新哈希冒充覆盖修复**）、
    位置有效、适用暂停解除条件；任一失败 → 回滚且保持原暂停/位置/身份。
  - 在途隔离：授权事务与消费提交/暂停变更走同一 `indexer_lease` 锁互斥；切换前已提交的行有效（历史保留）；
    旧身份在途事务在切换后提交时精确守卫（start,config,next）失配 → 0 行。
    版本隔离以 seq 为准，不以哈希为准：消费捕获处理依据的 version_seq，提交时在锁内按 version_seq 核对当前仍一致，
    不一致即放弃重读（H1 回环下哈希相同亦然）；授权请求校验预期 version_seq，哈希仅做内容一致性校验；
    禁把旧处理结果打上最新 seq 提交。暂停写入同样在锁内按当前版本重估证据并打标当前 seq（见 data-model）。
  - 请求身份：调用方提供同链稳定的 `request_id`；首版本行 `request_id` 为 NULL（partial unique 单行，
    不参与幂等判定）。未知/重复/过期一律按 request_id 先查 history 再定性（见 data-model 授权协议），
    禁仅比目标哈希：同 ID 同参返回已记录结果（即使已进入更晚版本），同 ID 异参明确拒绝
    （均指已记录请求；2026-09-13 批准修订：未记录的明确失败不绑定 ID，见 data-model 步骤 1 绑定规则），
    异 ID 即使同参亦独立校验（是否执行仍守当前版本/空变更门禁）。过期拒绝须报告预期版本与当前版本，
    不虚构成功记录。调用方意图含 expected_pause 二列（同时提供或同时为空；为空表示不授权处置任何已有暂停，
     非通配）；同 ID 更改目标即异参（已记录请求）。系统重算的 replay_from/处置结论只返回不参比。
  - 目标缺席操作规则（执行语义以 data-model 授权协议步骤 1／3／4 为准）：双空不等于一律拒绝。
    必须处置 ⟺ 行存在＋证据在本授权解决范围内已证解决（恢复消费的愿望不算理由），无目标即拒绝，
    调用方读取实例后重新明确授权（2026-09-13 批准修订：拒绝未留记录，ID 未绑定；
    同 ID 补目标按独立候选完整重验执行，不再强制新 request_id；成功绑定后同 ID 改目标仍拒绝）。
    可保留 ⟺ 行存在但证据在范围外 → 提交身份／位置／history（expected_pause 记 NULL），行原样保留，
    消费仍停；保留暂停后续走实例人工解除＋现版本重验，无死路。
    锁内暂停状态与依据不一致 → 回滚并报告状态变化，不自动切换分支，不扩大授权。
  - 授权路径暂停写限定：只允许原样保留（不动行）或对显式匹配目标的条件 DELETE（同事务审计行）；
    禁 UPDATE／合并。合并更新（revision+1＋merge 审计行）专属暂停写事务，
    须持 lease 锁并走 Table 3 原子条件。
  - 完整性前检：授权入口先核对两侧同有＋行内与最新 history 关联一致，否则损坏态（禁修复）；
    损坏不拦截已记录结果的只读返回（不开事务，不重执行），只拦截新授权执行。
  - 解除结果判定：条件解除影响 0 行后查审计表定性——命中原实例释放记录即返回"该目标已解除"
    及原审计结果（原结果、原操作者、原时间），不重写、不触碰新暂停、不归功本次调用者；
    无独立解除请求身份时只返回该事实，不声称识别为同一次请求
    （操作者/原因/实例/修订相同亦不证明同一请求）；未命中即陈旧或目标不存在，按新实例重走流程。
    人工路径无 request_id，上述"只返事实"即其全部语义。
  - 未知结果：授权提交未知 → 按 request_id 查 history，以 DB 为准：
    命中且全参数一致 → 同一次请求已提交，返回已记录结果（即使已进入更晚版本）；
    命中但参数不同 → 同 ID 异参，明确拒绝；未命中 → 重试授权（重算后）。
  - 重复请求（禁仅比目标哈希，一律先按 request_id 定性，见上；下仅为独立请求路径）：
    未命中且 expected_old_seq 等于当前最新 version_seq 且 H′ 与当前不同 → 走完整重验执行；
    expected_old_seq 不符 → 过期拒绝（报告预期与当前版本）；
    H′ 与当前相同 → 空授权拒绝。配置回环（如 H1→H2→H1）命中"未命中"分支（旧行参数必然不同），
    按全新授权建新行新 seq，禁合并。
  - 版本关联：观察行存 `version_seq` 外键（首次生成依据的版本）；回放遇到已有观察保留原值，不重写；
    不用时间戳推导版本归属（同时间戳与内容复现可区分）。新生成观察记录捕获版本（实际处理依据），
    禁取提交时刻最新 seq 冒充（见 data-model 写事务协议）。
  - 多次切换（回放未完成时再次请求）：**无需新增业务限制**。授权每次从当前持久化状态重算：
    replay_from_new = min（当前 next，各新增组合有效起点，未解决适用缺口起点）；
    暂停行按版本打标合并（detail 含版本；新结构缺口在旧暂停行未解除时，由暂停写事务原子更新其内容，
    不另建行——单行约束；授权事务禁用此写法）；
    历史链不断（每行记 old→new，可审计追踪；并发分叉以 exact-guard 拒绝保证线性）。
    未引入"回放完成前禁止新授权"之类的业务规则——重算律已保证嵌套安全；如实现证伪，显式报告，不静默加规则。
  - 漂移改回：非授权路径——改回 env 与旧配置一致后重启，启动比较通过即恢复；
    plan 与 quickstart D5 明确承接（tasks T011 不变）。
  - 首单元结构缺口可达性：首单元遇结构缺口时 checkpoint 为空，无需授权（无身份可更新），
    仅经上游修复 + 覆盖重验后由首单元提交建立首版本；与 H2"无源版本拒绝授权"无循环
    （拒绝仅针对凭空建版本，首单元路径不经过授权事务）。
- **Rationale**: "谁、何时、改什么、前提、审计"全部收进一个原子事务，TOCTOU 消除；
  多次切换无需业务限制，因为重算律 + exact-guard + 单行暂停合并已使嵌套收敛；
  审计行与身份更新同事务，杜绝"改了哈希没留痕"的绕过。
- **Alternatives considered**:
  - 独立授权服务/HTTP 端点：否决——003 暂停解除即人工 SQL；新端面违反简单优先，且引入签发权限新信任根。
  - 版本号乐观锁列：否决——(start,config,next) 精确守卫已是等效 fencing，不增列。
  - 回放完成前禁止新授权（业务限制）：否决——未经确认的业务规则；重算律已覆盖嵌套场景（结论见上）。
