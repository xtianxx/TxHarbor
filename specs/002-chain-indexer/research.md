# Research: 002-chain-indexer

**Branch**: `002-chain-indexer` | **Date**: 2026-09-12 | **Spec**: `specs/002-chain-indexer/spec.md`

All decisions below are behavior-locked by the spec + 5 adopted clarifications.
Open implementation detail lives only where the spec explicitly defers it (table mechanics are now
decided here — the spec deferred them to plan; retry/interval *values* are decided here too).

## R1 — 按高度取区块头：`HeaderByNumber` + `ethereum.NotFound` 等待语义

- **Decision**: 使用 `ethclient.HeaderByNumber(ctx, big.NewInt(n))`；`nil` 返回伴随 `ethereum.NotFound`
  时视为"高度尚无块"，进入等待/轮询，不记为故障；其他错误沿用现有 `eth.Kind` 分类并扩展（见 R4）。
- **Rationale**: `HeaderByNumber` 对应 `eth_getBlockByNumber(fullTx=false)`，正是头索引所需；
  `BlockByNumber` 会拉取完整交易体，浪费且扩大故障面。`NotFound` 是追尾索引的常态而非异常。
- **Alternatives considered**: `BlockByNumber`（否决：载荷过大）；订阅新头（否决：Constitution 明确
  WebSocket 不得视为无损，本阶段只做区间轮询）。

## R2 — 多实例协调：lease 行 + 心跳 + fencing token；拒绝常持 session advisory lock

- **Decision**: 新增 `indexer_lease` 单行（按 chain_id），以原子 CAS 获取、
  短语句心跳续约；每一次写事务都在同一事务内用 `EXISTS (lease owner=me AND token=tok AND 未过期)`
  做 fencing；一致性约束（PK/单调 checkpoint）作为第二道防线。任何情况下都不在池连接上
  常持 `pg_advisory_lock`，不在事务内调 RPC。
- **Rationale**: session advisory lock 在连接丢失时静默失效（应用无感知）、与 pgxpool 的连接复用
  冲突（lock/unlock 必须同物理连接；`Release` 不重置会话状态导致锁泄漏回池）、`MaxConnLifetime`/
  健康检查可能在不知情下销毁持锁连接。lease+心跳把 leadership 变成显式 durable 状态，
  丢失信号明确（续约 0 行即失权），fencing token 让过期执行者的写事务在提交前被数据库拒绝。
- **Alternatives considered**:
  - 纯约束收敛（双实例都写、`DO NOTHING` + 单调 checkpoint 收敛）：正确性可成立，但 RPC 负载翻倍、
    且无法回答"过期执行者如何被阻止写入"——旧主看到的仍是旧 token，其写事务被 fencing 显式拒绝，
    可观测、可测试。否决为唯一机制，保留为第二道防线。
  - 常持 advisory lock + 独占连接：否决，理由见上（池污染、静默失权、无 fencing）。
  - 事务级 `pg_advisory_xact_lock`：仅在极短事务内串行化时有意义，本设计所有写已由行级约束
    串行化，无需引入。

## R3 — 原子提交：短事务，先 RPC 后 DB

- **Decision**: 每个高度一个短事务，事务内只做本地读写：
  `INSERT block … ON CONFLICT (chain_id, number) DO NOTHING` → 同事务重读同高度已存哈希比对
  （不同即回滚走暂停事务）→ 单条 checkpoint 推进
  `UPDATE … WHERE height = $n-1 AND block_hash = $parent AND NOT EXISTS (pause)
   AND <fencing EXISTS 谓词>` → commit。RPC 取头在 `BEGIN` 之前完成（带超时）；
  事务内永不调外部网络、永不 `pool.Acquire` 常持。内存持有的"我是主"标志仅为提示，
  写语句内的 fencing/暂停谓词才是权威裁决——校验与写入同属一条语句，不存在竞争窗口
  （Read Committed 下语句级快照，并发推进语句经行锁串行后逐条重估 WHERE）。
  辅以 `statement_timeout` / `idle_in_transaction_session_timeout` 作为护栏。
- **Rationale**: 精确守卫（`height = $n-1` + 父哈希等于旧 checkpoint 哈希）在一条语句内同时强制
  +1、连续性、暂停即停、失权即停；0 行即停推重读。`DO NOTHING` 使重放与并发插入幂等；
  崩溃时两者同滚，不存在半进度。不确定提交的恢复规则：重连后 `SELECT` checkpoint + block 行，
  以数据库为准、幂等继续（不假设成功/失败）。
- **Alternatives considered**: 先开事务再取块（否决：持锁等 RPC，无界事务）；应用层"先查后写"
  替代约束（否决：竞态下不可靠，违反 Constitution II/VI）。

## R4 — 错误分类扩展：`not-found` 与 `rate-limited`

- **Decision**: 在现有 `eth.Kind` 上新增 `not-found`（等待极，非错误：高度无块/空结果）与
  `rate-limited`（可重试：429/限流，需退避）；其余映射不变（超时/传输→可重试；
  非法响应→停留报告；配置/认证类不可重试→停止扫描并报告，不无限重试）。
  checkpoint 核验三态（空结果 / 可重试错 / 明确哈希不同）分别映射为等待重试 / 退避重试 / 持久化暂停。
- **Rationale**: 验收必须区分"等待"与"故障"（SC-08）与核验三态（澄清 #2）；指标按 kind 打点后，
  暂停/重试/等待的测试断言可直接读数。
- **Alternatives considered**: 限流并入 transport（否决：验收与可观测需要区分退避原因）；
  `not-found` 并入空结果重试（部分采纳：语义相同，但独立 kind 使"追头等待零误报"可断言）。

## R5 — 重试与轮询参数（带默认值，需校验）

- **Decision**:
  - 可重试错误：指数退避初值 200ms ×2，上限 30s，抖动 ±25%；运行期间不设总次数上限
    （ tailing 本质要求），但每次退避可被 shutdown/暂停/失权中断——此即 Constitution IX 要求的
    明确 bound + justification（上限退避 + 可中断 + 分类退出）。
  - 追头/核验轮询间隔：`TXHARBOR_INDEX_POLL_INTERVAL` 默认 1s（>0 必检）。
  - RPC 单次超时：`TXHARBOR_INDEX_RPC_TIMEOUT` 默认 5s（复用探测量级，>0 必检）。
  - 不可重试（配置/认证/chain_id 不一致）：停止扫描并报告，不重试。
- **Rationale**: 上限退避保证 Anvil/故障演练在秒级可观测恢复；1s 轮询在本地链上足够灵敏且不压测；
  默认值全部可在验收矩阵的等待阈值内验证，plan 后由测试固定。
- **Alternatives considered**: 固定间隔重试（否决：限流下加剧）；总次数上限后放弃（否决：违背"不漏扫"，
  放弃即丢块）。

## R6 — 配置项（env-only，沿用 001 约定）

- **Decision**: 新增 `TXHARBOR_START_HEIGHT`（必需，无默认值：无声默认 0 可能掩盖意图；必须为
  非负整数，创世 0 合法）、`TXHARBOR_INDEX_RPC_TIMEOUT`（默认 5s）、
  `TXHARBOR_INDEX_POLL_INTERVAL`（默认 1s）、`TXHARBOR_INDEX_RETRY_INITIAL/MAX`（默认 200ms/30s，
  可选）。全部 `>0` 校验（高度 `>=0`），非法即拒绝启动；`.env.example` 同步；`Summary` 回显脱敏。
- **Rationale**: 起始高度是资金语义的下界，必须显式；其余沿用 001 的 env-only + 启动期全量校验。
- **附带修复（必须，非静默）**: `config.Summary` 当前原文打印 `RPCURL`（explorer 确认），而 FR-15
  要求日志不得暴露 RPC 凭据——plan 要求实现时对该字段走 `logx.Redact`。此为 spec 合规所必需，
  非范围扩张。

## R7 — 可观测：指标为主，readyz 不变

- **Decision**: 扫描状态/checkpoint/暂停原因经 `/metrics` 新指标 + 结构化日志暴露；
  `/livez`、`/readyz` 语义冻结（readyz 只反映 DB/RPC 依赖健康，不反映扫描暂停——暂停不是依赖故障，
  不得与其混同）。暂停行可经 SQL 直查（语句见 quickstart）。
- 新增指标：`txharbor_indexer_checkpoint_height{chain}`（Gauge）、
  `txharbor_indexer_state{chain}`（0 运行/1 等待/2 重试/3 暂停）、
  `txharbor_indexer_rpc_total{kind,result}`（Counter）、`txharbor_indexer_pause_total{chain}`。
- **Rationale**: 最小载体、与现有 `txharbor_ready`/`txharbor_probe_total` 注册表一致；
  readyz 若被暂停污染，001 的"依赖中断⇄恢复"验收将被破坏。
- **Alternatives considered**: 新增 HTTP 状态端点（否决：本阶段无新增端点需求，指标+日志+SQL 已覆盖
  SC-10 的"状态查询"）；暂停翻转 readyz（否决：理由见上）。

## R8 — 验证策略：真库 + 真链/可控故障 RPC

- **Decision**: 事务/约束/协调类断言一律用真实 PostgreSQL（testcontainers，
  `//go:build integration`，沿用现有 helper）；链行为用 Anvil（正常流、追头、重启、双实例）与
  可控假 RPC（httptest：超时/429/空结果/非法响应/哈希突变/chain_id 错误）。并发测试跑 `-race`。
  13 场景映射见 quickstart；双实例/崩溃/不确定提交的四条硬断言（不倒退、不跳高、连续一致、
  无重复有效记录、暂停后零推进）为必过门禁。
- **Rationale**: Constitution X/XI：锁、事务、并发、恢复必须集成测，mock 不可替代。
