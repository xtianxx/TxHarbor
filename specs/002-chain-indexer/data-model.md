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
- 写事务协议（首块 / 推进 / 暂停统一。锁先行、持锁后独立语句裁决）：
  1. `BEGIN`（短事务；RPC 取头已在事务外完成，事务内零外部调用）。
  2. `INSERT INTO indexer_lease (chain_id, owner_id, fencing_token, expires_at) VALUES …`
     `ON CONFLICT (chain_id) DO NOTHING` —— 幂等确保协调行存在（首启双实例恰一行胜出，
     另一方 no-op；不存在"对不存在行加锁"）。
  3. `SELECT owner_id, fencing_token, expires_at FROM indexer_lease WHERE chain_id=$c FOR UPDATE`
     —— 获取全链唯一协调锁，保持到 COMMIT/ROLLBACK。所有写事务在此处串行化，无死锁环。
  4. 后续**独立语句**（Read Committed 每条语句取新快照，故其所见包含锁等待期间提交的一切事务）
     重读并裁决，任一失败即 ROLLBACK：
     - `SELECT 1 FROM indexer_pause WHERE chain_id=$c` 必须无行；
     - lease 行 `owner_id=$me AND fencing_token=$tok AND expires_at > now()` 必须成立；
     - 推进：`SELECT height, block_hash FROM indexer_checkpoint …` 必须行存在且
       `height=$n-1 AND block_hash=$parent`（恰好 +1 与连续性，非 `< n`）；
       首块：checkpoint 必须无行且 `$n=S`（应用层已断言取回高度，事务内复核无行）。
  5. 执行写入（INSERT 块 + 推进/首建 checkpoint，或 INSERT pause），COMMIT。
  影响 0 行 / 裁决失败 = 前提不成立，调用方必须停推并重读状态，不得按成功处理。
  正确性不依赖"子查询与写入同语句"：裁决读是持锁后发出的独立语句。
- 同高度哈希比对（FR-12 第一臂，协议步骤 5 内）：持锁裁决通过后，
  `INSERT block … ON CONFLICT (chain_id, number) DO NOTHING`，随后同事务
  `SELECT hash FROM chain_blocks WHERE chain_id=$c AND number=$n`；若已存哈希与本次
  取回哈希不同 → ROLLBACK，走暂停事务（kind=`hash_mismatch`，expected=已存，actual=取回）。
  相同则继续 checkpoint 写入并 COMMIT。内存中的"我刚取到什么"永不作为正确性依据，
  以持锁事务内的重读为准。
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
- 续约（同样遵守协调协议）：`BEGIN` → `SELECT … FOR UPDATE` 取协调锁（行缺失则视为无租约，
  走获取路径）→ 复核 `owner=$me`（不符即 ROLLBACK 并上报失权）→
  `UPDATE … SET expires_at = now() + ttl WHERE chain_id=$c` → `COMMIT`。
  影响 0 行或复核失败 = 失权，立即停写。心跳每 5s 一次短事务，与写事务串行化，
  临界区仅两条点语句。
- TTL/心跳：ttl = 15s，心跳每 5s（≈1/3 ttl，DB 时间口径，防时钟偏斜）；连接断开后不续约，
  租约自然过期，他实例接管。过期前任的写事务被 in-tx 重读裁决拒绝（见"写事务协议"）。
- **协调行角色**：lease 行即全链唯一的协调行。首块、推进、暂停三种写事务必须首先
  `SELECT … FROM indexer_lease WHERE chain_id=$c FOR UPDATE`（行不存在则同一事务内先
  `INSERT … ON CONFLICT DO NOTHING` 再加锁，见"写事务协议"），锁顺序唯一且固定，
  不存在死锁环。checkpoint 行与 pause 行永不作为协调锁目标。
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

- 写入协议（暂停事务，走统一写事务协议）：BEGIN → 确保 lease 行存在 → `SELECT … FOR UPDATE`
  锁定协调行 → 独立语句复核分歧证据（重读 checkpoint/blocks 行，确认分歧仍成立；
  若已被并发推进修复如哈希已一致，则放弃暂停、ROLLBACK）→ `INSERT INTO indexer_pause …
  ON CONFLICT (chain_id) DO NOTHING` → COMMIT。暂停事务同样先锁协调行，故与推进事务
  互斥，不存在"暂停与推进同时提交"的竞窗：任一在暂停提交后获取协调锁的推进事务，
  其持锁后裁决读（新快照）必见 pause 行 → 拒绝写入。
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
- I6（不跳块）：顺序循环 + 逐高度事务 + 精确守卫；"不跳高"即 checkpoint 只能落在连续已存序列末端，
  由"每高度一事务 + 外键 + `height=$n-1`"合成保证。

## 并发正确性论证（取代旧"单语句子查询"论证）

撤回声明：旧版称"UPDATE 被行锁阻塞后重估 WHERE 会刷新跨表子查询快照"——不成立。
Read Committed 下阻塞语句仅目标行走 EPQ 重检，`NOT EXISTS / EXISTS` 子查询沿用语句级旧快照。
以下论证仅依赖两条成立语义：(a) 同一行的 `FOR UPDATE` 锁互斥，持有至事务结束；
(b) 同一事务内持锁后发出的**后续独立语句**取新快照，可见锁等待期间提交的一切事务。

- **情形 1（暂停持锁，推进已启动等待）**：暂停事务先获协调锁；推进事务阻塞于步骤 3。
  暂停 COMMIT 后推进获锁，其步骤 4 裁决读（新快照）必见 pause 行 → ROLLBACK，零写入。
  暂停提交前已 COMMIT 的推进属暂停可见前的合法排序；暂停可见后任何推进事务必经步骤 4
  而被拒绝。不存在"测试静止后采样"弱化——断言为"pause 行创建时间点之后提交的推进事务数为 0"。
- **情形 2（新实例接管，旧 token 写入被拒）**：接管事务先获协调锁，`token+1 + 换 owner` 后提交。
  旧主在其之后发起的任何写事务，步骤 4 重读 lease 行必见 owner/token 不符 → ROLLBACK。
  旧主已持有锁的在途事务先提交（其接管尚在排队），属锁排序决定的合法先后；接管一旦提交，
  旧 token 再无任何写路径可走（含暂停事务：暂停同样先锁协调行并复核 owner/token）。
- **情形 3（无 checkpoint，双实例首次写入）**：双方步骤 2 幂等确保单行（恰一行 INSERT 胜出），
  步骤 3 串行化；胜者见 checkpoint 无行、写入首块+首 checkpoint 后提交；后者步骤 4 见行已存在，
  转入"推进 S"分支：其精确守卫要求 `height=S-1` 而行已为 S → 拒绝，重读后转向 S+1；
  若其取回 S 哈希与已存不同 → 走暂停事务。恰好一首块，无缺口（缺口检查见 quickstart）。
- **无死锁**：所有写事务只锁一行（lease 行）且恒为首动作；续约/只读查询不取协调锁，
  不存在锁顺序环。
- **锁持有时长**：事务内零 RPC、仅 3–5 条点查点写，持锁毫秒级；心跳/续约走独立短语句，
  不放大临界区。

## 索引

- PK/UNIQUE 之外不建二级索引：访问模式 = 按 (chain_id, number) 点查 + checkpoint/lease/pause 单行读写，
  全部被 PK 覆盖。后续阶段若加日志扫描再议（禁止投机索引）。
