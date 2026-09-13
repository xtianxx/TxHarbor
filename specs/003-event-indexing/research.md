# Research: 003-event-indexing

**Branch**: `003-event-indexing` | **Date**: 2026-09-13 | **Spec**: `specs/003-event-indexing/spec.md`

All decisions below are behavior-locked by the spec + 3 adopted clarifications (Session 2026-09-13).
Open implementation detail lives only where the spec explicitly defers it (D3–D6, OQ1–OQ3).
E1 (provider 限制与截断语义) remains an explicit pre-implementation condition — see R5.

## R1 — 存储方案：新表 + 复用 `indexer_lease` 协调行（OQ1 已决）

- **Decision**: 新增三张表（见 `data-model.md`）：`erc20_transfer_logs`（原始日志）、`log_checkpoint`（日志扫描进度，按 `chain_id` 单行）、`log_pause`（日志流暂停信号，单链单行）。
  复用 002 的接口与机制：
  - 复用 `indexer_lease` 行作为全链唯一协调行：日志写事务同样先 `INSERT … ON CONFLICT DO NOTHING` 确保行存在，再 `SELECT … FOR UPDATE` 取锁，持锁后用后续独立语句裁决（Read Committed 语句级新快照）。锁顺序唯一且固定（永远只锁 lease 行），与 002 写事务互斥串行，不存在死锁环。
  - 复用 `chain_blocks` 作为链视图真相：区间上界、逐块 `block_hash`/canonical 核对全部以该表为准；日志表不存第二份区块真相。
  - 不复用 `indexer_checkpoint` / `indexer_pause` 行：两个扫描流各有独立进度行与暂停行，互不覆盖暂停原因（澄清 A2）。日志提交裁决同时要求两行缺席：`log_pause` 无行（本流未暂停）且 `indexer_pause` 无行（链级暂停同样阻止日志提交）。
- **Decision（lease 共存，2026-09-13 修正）**: 同一进程内 header 与 log 双流共享同一 lease 行，但**只有一个获取循环**：
  新增包内 `Coordinator`（`internal/indexer/` 新文件）：唯一调用 `Acquire` 的循环 + 胜出后唯一的 `Heartbeat`；
  胜出期间并发运行 header `serveLoop` 与 log `serveLoop`；任一返回 `ErrLeaseLost`/心跳失联 → 全部取消、重回获取。
  002 的 `Scanner.Run`（自带获取循环）保留供既有测试，生产 `serve.go` 改走 `Coordinator`；
  `Scanner.serve` 仅做机械拆分（心跳移出，循环体原样保留为 `serveLoop`），外部行为不变，002 回归测试覆盖。
- **Rationale（两层机制，不存在"一方持续占用"）**: 须区分 **lease 所有权**（长持：CAS 获取 + 5s 心跳续约 + 15s TTL，决定"谁有权写"）与 **协调行锁**（短持：写事务内 `FOR UPDATE` 到 `COMMIT` 的毫秒级临界区，决定"写操作串行顺序"）。持有所有权的进程在事务之外**不占用**协调行：行锁只在短事务内存在，header 推进、日志提交、暂停写入三者在此排队，均为毫秒级，不存在一方饿死另一方。未获所有权的进程写事务在 lease 归属裁决即被拒绝，原地待命重试，不产生部分写入。
  否决"双获取循环共享同一句柄"：同 `owner_id` 在行未过期时 `Acquire` 永返回 `won=false`（`lease.go:150`），
  后启动的流将永久待命——此为本次核验发现并修正的原 R1 缺陷。
  002 的 `commitPause` 已走同一协议（`ensure → 加锁 → 复核 → 写入`，见 `scanner.go:743`），故链级暂停写入与日志提交天然互斥，不存在竞争窗口——"无窗口"的依据是行锁互斥 + 持锁后新快照裁决（与 002 data-model 并发论证相同的两条成立语义）。
- **Alternatives considered**:
  - 每个 scanner 独立 `Lease` 句柄（不同 `owner_id`）：否决——同进程内 header 与 log 将争夺同一 lease 行，恰好一个流长期饿死，且心跳翻倍毫无收益。
  - 日志自建 lease/锁行：否决——两把锁引入锁顺序问题，且链级暂停与日志提交的互斥还需跨锁协议，得不偿失。
  - 复用 `indexer_checkpoint` 加流标识：否决——澄清 A2 明确禁止共享同一条进度记录；且该行有指向 `chain_blocks` 的复合外键，`next_block`（尚未存在的高度）无法被外键表达。
- **兼容影响**：`000003` 新迁移只建新表，不改 002 四张表的定义；`indexer_lease` 行被两类写事务共享，锁竞争只在毫秒级短事务内，可接受。`DOWN` 迁移按 FK 依赖顺序先删新表。

## R2 — 事务边界与旧 worker 隔离：整区间一事务 + 精确守卫（OQ1/OQ2 已决）

- **Decision**: 一个扫描区间 `[a, b]` 一次短事务，事务内零 RPC。协议镜像 002 五步法：
  1. 事务外：确定上界（`min(请求末端, 002 checkpoint 高度)` 并验证 `[a, b]` 在 `chain_blocks` 中连续且 canonical）、RPC 取日志、逐条严格校验、复核末端区块身份。
  2. `BEGIN` → 语句超时护栏 → 确保 lease 行 → `FOR UPDATE` 取协调锁。
  3. 持锁后独立语句裁决（任一失败即 `ROLLBACK`）：`log_pause` 与 `indexer_pause` 均无行；lease 的 owner/token/有效期成立；`log_checkpoint` 精确守卫（`next_block = a` 且 `start_block`/`config_hash` 与本次配置一致——过期 worker 与配置变更在此被拒绝）；`[a, b]` 引用区块仍连续 canonical（重读裁决，捕获锁等待期间的链视图变化）。
  4. 写入：`INSERT` 日志行（`ON CONFLICT DO NOTHING`）→ 重读冲突行做内容比对（一致则收敛，不一致则回滚走冲突暂停）→ 整批行数核对 → `UPDATE log_checkpoint SET next_block = b+1`（带 `WHERE next_block = a` 精确条件，`RowsAffected != 1` 即判过期）→ `COMMIT`。
  5. 提交结果未知时：不假设成功/失败，下一轮重读 `log_checkpoint` 并以精确守卫重新裁决，幂等继续。
- **Rationale**: 精确守卫 `next_block = a` 使并发/延迟提交天然串行：胜者推进后，旧 worker 的 `UPDATE … WHERE next_block = a` 影响 0 行；配置变更导致守卫列不匹配，同样 0 行。暂停行检查在持锁后的新快照中进行，暂停提交后发起的任何事务必见暂停行。内容比对把"无条件忽略冲突"变为显式失败路径。
- **Alternatives considered**: 逐块提交（否决：区间语义要求整批原子，"部分日志+推进"正是 I2/I3 禁止的状态）；`UPDATE … WHERE next_block <= a` 宽松守卫（否决：允许跳块，违反 I3；002 已证明精确守卫的必要性）。

## R3 — `config_hash` 编码与比较流程（澄清 A1 落实，D5 部分）

- **Decision**: `crypto/sha256` 对 `UTF-8("erc20-transfer:v1\n" + strings.Join(sortedLower, "\n"))` 取摘要，hex 编码 64 位小写存 `CHAR(64)`（CHECK 全小写 hex）。流程：启动时解析 `TXHARBOR_LOG_CONTRACTS`（逗号分隔，去空格）→ 逐项 `common.IsHexAddress` 校验 → `strings.ToLower` → 排序去重 → 空集合即拒绝启动 → 计算 `config_hash`。初始化：无 `log_checkpoint` 行时，在首个区间提交事务内原子写入 `(start_block, config_hash, next_block=a)`，冲突时（`ON CONFLICT DO NOTHING` 后 0 行）重读已有行并执行同样比较。重启：比较配置 `(start_block, config_hash)` 与行内值；任一不一致即拒绝启动（报错并退出码 1），不触碰数据。
- **Rationale**: 编码规则由规格澄清逐字锁定，无自由度；排序+去重使大小写/顺序/重复差异自然收敛为同一身份；"比较配置起点与行内起点"而非与 `next_block` 比较，避免正常推进后被误判为配置变化。
- **Alternatives considered**: EIP-55 校验和存储（否决：澄清已定小写）；把批次/超时参数纳入身份（否决：澄清明确排除，调参不改变历史语义）。

## R4 — RPC 查询与错误分类：`FilterLogs` + 新增 `KindIncomplete`

- **Decision**: `ethclient.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: a, ToBlock: b, Addresses: whitelist, Topics: [[transferSig]]})`（topic0 下推，地址在客户端严格复核）。错误分类在现有 `eth.Kind` 上新增：
  - `KindIncomplete`（"incomplete"）：区间过大/结果过多/响应超限类错误、显式 truncated/partial 标记、未取完分页、返回数达到已确认上限。 polarity = 缩批重试（从原 `a` 重查更小区间），绝不视为空结果、不提交部分。
  - 其余沿用：`timeout/transport/rate-limited` → 有界退避重试；`invalid-response`（含解析失败）→ 停留报告；`not-found`/空结果 polarity 需谨慎：`eth_getLogs` 对空区间返回空数组是合法成功（可推进），只有传输层失败才属重试——这与头索引的 `NotFound` 等待语义不同，见 data-model §校验流程。
  - 未知错误一律按失败处理（`transport`），绝不按成功推进。
- **Rationale**: 澄清 A3 要求把"区间超限"与"限流/超时/解析失败"分为两类恢复路径；`FilterQuery` 的 topic0 下推减少传输，剩余 7 项校验（地址归属、高度范围、hash 一致、字段完整、removed、长度、topic 结构）全部在客户端逐条执行，不信任 RPC 过滤。
- **Alternatives considered**: 自行组装 `eth_getLogs` 原始调用（否决：go-ethereum 已提供类型化封装，自造增加解析故障面）；把 incomplete 并入 rate-limited（否决：恢复动作不同——缩批 vs 等待退避，验收需分别断言）。

## R5 — Provider 上限与 E1：未选定，双轨门禁（本地可放行 / 生产保持 open，不虚构映射）

- **Decision**: 生产 RPC provider 尚未选定（仓库唯一配置指向本地 Anvil `127.0.0.1:8545`）。E1 拆分为双轨：
  - **T000-L（本地轨，可关闭）**：Anvil 版本固定、`FilterLogs` 接口、`config_hash` 向量、本地故障注入清单
    （见 quickstart 本地前置清单）。关闭后 T001–T019、T020b 可在本地范围执行。
  - **T000-P（生产轨，保持 open）**：选型 + 附录 + 生产验证；门禁生产接入与部署，不阻塞本地实现。
    tasks 可先生成；生产 enablement 在 T000-P 关闭前不得标记完成。
  - 本 plan 只定义信号分类框架（R4）与"provider 附录"必填表（见 `quickstart.md` E1 附录模板：单次结果数量上限、区间上限、截断标志字段、分页语义、超限错误码/消息、来源文档链接）。
  - 不 hardcode 任何具体数值（如"2000 块""10000 条"）到实现或测试；计数达上限判定默认禁用，直到 provider 附录提供经文档确认的上限值。**禁用计数判定只是"不误判"，不等于完整性已解决**：未知上限下仍可能存在静默截断，实现与部署文档必须如实声明该残余风险。
  - 本地/CI 验证范围限定为 Anvil + 可控假 RPC，并明确记录未覆盖项：生产 provider 的真实上限、截断与分页行为不在本阶段验证内；生产接入前必须补附录并复核 `LOG_BATCH_BLOCKS` 默认值。
  - E1 在 plan 末尾列为未解决事项：tasks 中设专项确认任务（证据 = 附录表填完 + 来源链接可打开 + 上限经实测复核），该任务是生产实现的门禁。

- **Decision**: 生产 RPC provider 尚未选定（仓库唯一配置指向本地 Anvil `127.0.0.1:8545`）。因此：
  - 本地/CI 验证范围限定为 Anvil + 可控假 RPC，并明确记录未覆盖项：生产 provider 的真实上限、截断与分页行为不在本阶段验证内；生产接入前必须补附录并复核 `LOG_BATCH_BLOCKS` 默认值。Anvil 对测试区间返回完整结果是可观察事实（测试失败即暴露），不宣称为通用保证。
  - E1 在 plan 末尾列为未解决事项：tasks 中设专项确认任务（证据 = 附录表填完 + 来源链接可打开 + 上限经实测复核），该任务是生产实现与接入的门禁，不阻塞任务拆解与本地验证。
- **Rationale**: 规格 FR-13/E1 与澄清 A3 明确禁止"仅依赖通用错误码或模糊字符串猜测"以及"凭任意阈值判断截断"；在 provider 未定阶段虚构映射正是规格禁止的行为。诚实标记 E1 为 open 比编造数字更符合章程（I 金融正确优先）。
- **Alternatives considered**: 引用某公有 provider 文档数值作为默认值（否决：本项目未选定该 provider，引用即虚构适用性）；跳过 E1 直接实现（否决：违反 FR-13 信任边界）。

## R6 — 配置项（env-only，沿用 001/002 约定；D5 落实）

- **Decision**: 新增三项，复用其余：
  - `TXHARBOR_LOG_START_HEIGHT`（必需，无默认值：与 002 `START_HEIGHT` 同理，资金语义下界必须显式；`>=0`，创世合法）。
  - `TXHARBOR_LOG_CONTRACTS`（必需，逗号分隔 EVM 地址；空白/空集合拒绝启动，绝不退化全链查询）。
  - `TXHARBOR_LOG_BATCH_BLOCKS`（可选，默认 500：Anvil 本地验证灵敏与往返开销的折中；生产值由 provider 附录复核，调参不改变历史语义）。
  - 复用 `TXHARBOR_INDEX_RPC_TIMEOUT / POLL_INTERVAL / RETRY_INITIAL / RETRY_MAX`（>0 校验已存在）：日志扫描的退避与轮询语义与头索引一致，不新增旋钮（章程 XIII 简单优先）。
- **Rationale**: 必需项无声默认值会掩盖资金意图；批次大小是唯一新增的操作旋钮，且规格明确允许调参。500 的默认依据：Anvil 上 500 块过滤查询为毫秒级且 payload 可控；provider 附录若给出更小上限，部署配置覆盖即可，历史语义不受影响。
- **Alternatives considered**: 独立 `LOG_RPC_TIMEOUT`（否决：当前无证据表明日志查询需要不同超时口径；YAGNI，附录阶段若有证据再加）。

## R7 — 可观测：沿用 `/metrics` + 结构化日志 + SQL 诊断（D6 落实）

- **Decision**: 新增 `log_` 前缀指标组（`txharbor_log_checkpoint_next{chain}` Gauge 空进度时删序列、`txharbor_log_lag_blocks{chain}` Gauge、`txharbor_log_state{chain}` 0/1/2/3、`txharbor_log_rpc_total{kind,result}`、`txharbor_log_pause_total{chain}`），`txharbor_ready`/`readyz` 语义冻结（日志暂停不得翻转 readyz，与 002 R7 同理）。结构化日志字段固定（见 `contracts/observability.md`），全部经 `logx.Redact`；错误输出保留链/区间/分类/重试次数，禁止原始响应转储。`log_checkpoint`/`log_pause` 行可经 SQL 直查。
- **Rationale**: 与 002 契约同构，运维心智一致；滞后指标 `lag = 002 checkpoint 高度 - (log next - 1)` 直接回答"相对区块扫描的滞后"。
- **Alternatives considered**: 新增 HTTP 状态端点（否决：指标+日志+SQL 已覆盖 SC-11）；复用 `indexer_state` 同名序列（否决：两流状态独立，混用无法区分"头停/日志跑"）。

## R9 — 不依赖 provider 选择的文档核对（2026-09-13，无需澄清即可确认）

- **V1 — `FilterLogs` 接口存在**：已锁定依赖 `go-ethereum v1.17.5` 的 `ethclient.go:473`
  `func (ec *Client) FilterLogs(ctx, ethereum.FilterQuery) ([]types.Log, error)` 可用；
  `types.Log` 携带 `Address/Topics/Data/BlockNumber/TxHash/BlockHash/Removed` 全字段（FR-07 所需）。
- **V2 — `config_hash` 标准测试向量**（SHA-256，`printf` 无尾换行实测）：
  输入 `erc20-transfer:v1\n0x1111…1111\n0x2222…2222`（各 40 位）→
  `b4eeddb97cb6ab1ba66eb8bb97e43b3f11b466de859f10bf30497ebd2e9cef7f`。
  T002 实现必须复现该向量；大小写/顺序/重复变体收敛同一向量，增删地址则变化。
- **V3 — Transfer 事件签名**：`0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df2883e6f`
  为以太坊标准常量；T003/T011 必须在代码中断言（`Keccak256("Transfer(address,address,uint256)")`），不得手写魔法字符串无断言。
- **V4 — 仓库 provider 现状**：`compose.yaml` 固定 `ghcr.io/foundry-rs/foundry:v1.8.1` Anvil（chain-id 31337），
  `.env.example` 的 `TXHARBOR_RPC_URL` 指向本地 Anvil；全仓库无任何生产/托管 provider 选型记录。
  Anvil 的 `eth_getLogs` 上限与分页行为标记为**未知**：测试体量保持小且可控，未知错误一律按失败处理，
  不推断 Anvil 无限制。
- **结论**：T001–T019 的本地验证链完整；E1（生产 provider 附录）仍 open，不可关闭。

## R8 — 验证策略：真库 + Anvil + 可控假 RPC（11 场景全映射）

- **Decision**: 事务/约束/协调类断言一律真实 PostgreSQL（testcontainers，`//go:build integration`，复用现有 helper）；链与日志行为用 Anvil（正常流、空区间、零金额、追头、重启、双实例）与可控假 RPC（httptest：超时/429/解析失败/超限错误/truncated 标记/达上限计数/哈希突变/乱序/重复/冲突）。并发测试跑 `-race`，但 `-race` 只覆盖进程内数据竞争，**不能替代跨 worker 的数据库一致性测试**：双 worker 竞争、旧 worker 延迟提交、暂停互斥必须用两个真实 DB 连接（不同进程或独立 pool + 独立 lease 句柄）做集成断言。11 验收场景映射见 `quickstart.md`；硬断言（无跳块、无部分提交、暂停后零推进、单次有效推进、冲突必败）为必过门禁。
- **Rationale**: 章程 X/XI：锁、事务、并发、恢复必须集成测；日志内容比对与缩批逻辑涉及字节级正确性，mock 不可替代。Anvil 可真实产出 Transfer 日志（含零金额），是决定性验证手段。
