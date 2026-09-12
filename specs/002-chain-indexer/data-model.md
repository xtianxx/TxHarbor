# Data Model: 002-chain-indexer

**Branch**: `002-chain-indexer` | **Date**: 2026-09-12 | **Migration**: `migrations/000002_chain_indexer.sql`
(goose，新文件；001 基线为刻意空迁移，本文件是首个业务结构。`embed.go` 自动收录 `*.sql`。)

## Table 1 — `chain_blocks`（已索引区块头）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK 首列，`CHECK (> 0)` | 配置期望链 |
| number | BIGINT | PK 次列，`CHECK (>= 0)` | 高度 |
| hash | TEXT | `NOT NULL`，格式 `^0x[0-9a-f]{64}$`（小写入库） | 区块身份之一 |
| parent_hash | TEXT | `NOT NULL`，同上格式；创世允许全零 `0x00…00` | 连续性证据 |
| canonical | BOOLEAN | `NOT NULL DEFAULT TRUE` | 本阶段恒 TRUE；见状态说明 |
| indexed_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用，非排序依据 |

- `PRIMARY KEY (chain_id, number)` —— 同链同高度物理上只能有一行 → 不变量 I1 由存储保证。
- `UNIQUE (chain_id, number, hash)` —— 冗余但必需：作为 checkpoint 外键目标。
- `UNIQUE (chain_id, hash)` —— 区块身份索引，支持按哈希核验与诊断。
- 状态说明：本阶段不做重组恢复，canonical 恒为 TRUE（列保留供后续阶段使用，查询统一带
  `canonical` 条件以兼容未来）。

## Table 2 — `indexer_checkpoint`（扫描进度，高水位）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK，`CHECK (> 0)` | 单链单行 |
| height | BIGINT | `NOT NULL CHECK (>= 0)` | 最后已存高度 |
| block_hash | TEXT | `NOT NULL`，同哈希格式 | 与 height 二元组 |
| start_height | BIGINT | `NOT NULL CHECK (>= 0)` | 首块确立的起始高度，建行后冻结 |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用 |

- `FOREIGN KEY (chain_id, height, block_hash) REFERENCES chain_blocks (chain_id, number, hash)`
  —— 不变量 I2（checkpoint 必指已存块）由数据库强制；三列绑定到 `chain_blocks` 的**同一行**，
  不是三个独立存在断言；任何"先 checkpoint 后补块"的写法直接失败。
- 推进语句（唯一合法写，单条语句原子裁决）：
  ```sql
  UPDATE indexer_checkpoint
     SET height=$n, block_hash=$h, updated_at=now()
   WHERE chain_id=$c AND height = $n-1 AND block_hash = $parent
     AND NOT EXISTS (SELECT 1 FROM indexer_pause WHERE chain_id=$c)
     AND EXISTS (SELECT 1 FROM indexer_lease WHERE chain_id=$c
                  AND owner=$me AND fencing_token=$tok AND expires_at > now());
  ```
  其中 `$parent` 为新取区块的 parent_hash——本语句同时强制：恰好 +1（非 `< n`）、
  父哈希等于旧 checkpoint 哈希（连续性）、暂停存在即停推（0 行）、失权即停推（0 行）。
  影响 0 行 = 前提不成立，调用方必须停推并重读状态，不得按成功处理。
- 同高度哈希比对（FR-12 第一臂）：推进事务内 `INSERT … ON CONFLICT DO NOTHING` 后，
  同事务 `SELECT hash FROM chain_blocks WHERE chain_id=$c AND number=$n`；若已存哈希与本次
  取回哈希不同 → 回滚本事务，走暂停事务（kind=`hash_mismatch`，expected=已存，actual=取回）。
  相同则继续推进 UPDATE。内存中的"我刚取到什么"永不作为正确性依据，以行锁下的重读为准。
- 首块事务：仅当 checkpoint 无行时 `INSERT (chain_id, height=S, block_hash, start_height=S)`，
  且必须与首块 `INSERT` 同一事务；应用层先断言取回高度 = S（S 变化检测见下）。
- 起始高度变更检测（FR-03）：启动时若 checkpoint 行存在且配置 `S != start_height`，
  拒绝启动并报告（不得自动丢弃/覆盖进度）；相等才允许继续。
- 无 checkpoint 行 = 空进度（不得用 height=-1 之类哨兵值）。

## Table 3 — `indexer_lease`（实例协调，单主租约）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK | 单链单行 |
| owner_id | TEXT | `NOT NULL`（实例唯一标识，启动生成） | 现任持有者 |
| fencing_token | BIGINT | `NOT NULL DEFAULT 0 CHECK (>= 0)` | 单调 fencing |
| expires_at | TIMESTAMPTZ | `NOT NULL` | 租约到期（DB `now()` 口径） |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用 |

- 获取（原子 CAS）：`INSERT … ON CONFLICT (chain_id) DO UPDATE SET owner/expiry/token+1
  WHERE indexer_lease.expires_at < now()`；返回行即获胜，token+1 仅获胜者可见新值。
- 续约：`UPDATE … SET expires_at = now() + ttl WHERE chain_id=$c AND owner=$me`；
  影响 0 行 = 失权，立即停写（不得"再试一次写"）。
- TTL/心跳：ttl = 15s，心跳每 5s（≈1/3 ttl，DB 时间口径，防时钟偏斜）；连接断开后不续约，
  租约自然过期，他实例接管。过期前任的写事务被 fencing 谓词拒绝（见事务一节）。
- 与 goose 迁移锁共存：迁移锁是短事务级 session lock（001 已有），lease 是行级租约，
  两者正交，不共享连接。

## Table 4 — `indexer_pause`（持久化暂停）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK | 单链单行（当前暂停） |
| height | BIGINT | `NOT NULL` | 发现分歧的高度 |
| expected_hash | TEXT | `NOT NULL` | 已存/期望哈希 |
| actual_hash | TEXT | `NOT NULL` | 链上实际哈希（父断裂时为断裂点子块 parent_hash 侧） |
| kind | TEXT | `NOT NULL CHECK IN ('hash_mismatch','parent_mismatch','checkpoint_changed')` | 原因分类 |
| detail | TEXT | `NOT NULL DEFAULT ''` | 人读诊断（无凭据） |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 暂停时刻 |

- 写入协议（暂停事务，防推进竞窗）：
  1. `SELECT height, block_hash FROM indexer_checkpoint WHERE chain_id=$c FOR UPDATE`
     —— 先锁 checkpoint 行，使并发推进事务的 UPDATE 在行锁上排队。
  2. 同一事务内 `INSERT INTO indexer_pause … ON CONFLICT (chain_id) DO NOTHING`。
  3. COMMIT。排队的推进 UPDATE 在锁释放后按最新快照重估 `WHERE`，此时
     `NOT EXISTS (pause)` 已为假 → 0 行，无任何在暂停提交后**开始执行**的推进语句能生效。
     （暂停提交前已执行完 UPDATE 的在途语句可能提交——其效果发生于暂停可见之前，
     属合法排序；暂停可见后零推进由守卫保证，测试在静止后采样。）
- 写入：除上序外，`INSERT … ON CONFLICT (chain_id) DO NOTHING`（首暂停获胜；暂停期间扫描已停，
  不存在合法并发写）。行存在 = 暂停有效，重启/切主后依然有效，本阶段不自动删行。
- 解除：仅人工（SQL）或后续重组规范；解除后继续前必须重走 checkpoint 核验（FR-12 已锁定）。
- 不变量：暂停行存在时，任何实例的推进写必须为 0（测试硬断言）。

## 一致性不变量 ↔ 约束对照

- I1（同链同高度至多一个 canonical）：PK + 恒 TRUE。
- I2（checkpoint 必指已存块）：外键。
- I3（父子连续）：应用层按"parent_hash = 已存前块 hash"校验 + 断裂即暂停；DB 层不设跨行 FK
  （创世/边界无前块，跨行约束无法表达边界豁免）。
- I4（单调、不指未存）：`height = $n-1 AND block_hash = $parent` 精确守卫 + 外键。
- I5（chain_id 不符零写入）：启动/运行期 `CheckChainID` 门禁，无 DB 对象参与。
- I6（不跳块）：顺序循环 + 逐高度事务 + 单调守卫；"不跳高"即 checkpoint 只能落在连续已存序列末端，
  由"每高度一事务 + 外键"合成保证。

## 索引

- PK/UNIQUE 之外不建二级索引：访问模式 = 按 (chain_id, number) 点查 + checkpoint/lease/pause 单行读写，
  全部被 PK 覆盖。后续阶段若加日志扫描再议（禁止投机索引）。
