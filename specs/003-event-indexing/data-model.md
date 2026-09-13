# Data Model: 003-event-indexing

**Branch**: `003-event-indexing` | **Date**: 2026-09-13 | **Migration**: `migrations/000003_event_indexing.sql`
(goose，新文件；002 迁移不动。`embed.go` 自动收录 `*.sql`。)

## Table 1 — `erc20_transfer_logs`（原始 Transfer 日志）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK 首列，`CHECK (> 0)` | 配置期望链 |
| block_number | BIGINT | `NOT NULL CHECK (>= 0)` | 请求区间内高度 |
| block_hash | TEXT | `NOT NULL`，`^0x[0-9a-f]{64}$` | 与 `chain_blocks` 对应 canonical 块一致 |
| tx_hash | TEXT | `NOT NULL`，`^0x[0-9a-f]{64}$` | 交易身份 |
| log_index | BIGINT | `NOT NULL CHECK (>= 0)` |  topic/data 之外的身份要素 |
| contract | TEXT | `NOT NULL`，`^0x[0-9a-f]{40}$` | 小写归一化地址，属白名单 |
| topic0/1/2 | TEXT | `NOT NULL`，各 `^0x[0-9a-f]{64}$` | topic0 为 Transfer 签名 |
| data | TEXT | `NOT NULL`，`^0x[0-9a-f]{64}$` | 32 字节原始值（含零金额） |
| indexed_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用，非排序依据 |

- `PRIMARY KEY (chain_id, block_hash, tx_hash, log_index)` —— 规格 FR-10 的去重身份由存储强制；重复投递收敛到同一行。
- `UNIQUE (chain_id, block_hash, log_index)` —— 同一链同一区块内日志索引不对应多条不同日志（FR-10 第二句；以太坊 `logIndex` 本为块内序号，此约束使其不可违）。
- 无 `removed` 列：removed 日志被校验拒绝（FR-07），永不落库；落库行隐含 `removed = false`。
- 无金额/业务投影列：本阶段只存原始字节（FR-08）。
- 状态说明：行一旦提交即为该 `(chain_id, block_hash, tx_hash, log_index)` 的唯一真相；内容冲突在写入事务内比对发现（见 §写事务协议步骤 5），冲突不覆盖。

## Table 2 — `log_checkpoint`（日志扫描进度，下一待扫高度）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK，`CHECK (> 0)` | 单链单行，与 002 进度行相互独立 |
| start_block | BIGINT | `NOT NULL CHECK (>= 0)` | 首区间确立的起点，建行后冻结 |
| config_hash | CHAR(64) | `NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$')` | 白名单配置身份（澄清 A1 编码） |
| next_block | BIGINT | `NOT NULL CHECK (>= 0)` | 下一待扫描高度 |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用 |

- `CHECK (next_block >= start_block)` —— 进度永不倒退到起点之前。
- 无行 = 空进度（沿用 002 约定，不用哨兵值）。
- **本表无外键（显式决定）**：`next_block` 的语义是"下一待扫描高度"，成功推进后的值 routinely 指向尚无对应 `chain_blocks` 行的高度（追头场景下 `b+1` 高度可能还未被 002 持久化，甚至尚未出块）。若要求 `next_block` 引用已存在的 `chain_blocks` 行，追头推进将永远无法提交。因此 I2（进度对应完整已处理区间）不用外键表达，而由三者合成保证：区间事务原子性（日志与进度同提交）+ 精确守卫（`next_block=$a`）+ 行数核对（本批去重身份数 == 插入+已存在一致行数）。`start_block` 同样不设外键：起点是配置语义的下界，不是"已存在块"的断言（起点高于链头时等待，不写入）。
- 起点/白名单变更检测（FR-05）：启动及每次提交裁决时，若行存在且配置 `(start_block, config_hash)` 任一不同，拒绝并报错；相等才允许继续。比较对象是行内 `start_block`，绝不是 `next_block`。

## Table 3 — `log_pause`（日志流暂停信号）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK | 单链单行（当前暂停） |
| height | BIGINT | `NOT NULL` | 发现问题的高度（区间首块或冲突块） |
| kind | TEXT | `NOT NULL CHECK IN ('chain_view_changed','validation_failed','range_incomplete')` | 原因分类（下表；机读细分进 `detail.class`） |
| detail | TEXT | `NOT NULL DEFAULT ''` | 人读 + 机读诊断：`class=<分类> identity=<日志身份> expected=<…> actual=<…>` 等键值片段；无凭据、无原始响应转储 |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 暂停时刻 |

- `chain_view_changed`：整批引用区块与 `chain_blocks` 不一致、或提交时链视图已变化（FR-14）。
- `validation_failed`：逐条校验失败的持久化信号。`detail.class` 取值覆盖规格全部非法类别，不必每种单建状态：
  `bad_address | bad_topics | bad_data | removed | out_of_range | missing_field | identity_conflict`。
  策略：校验失败当即整批失败、不推进（验收场景 6 的全部要求）；worker 按退避重查；同一区间在重查后仍确定性失败
  （canonical 数据不可变，重查同错即非瞬态）→ 持久化本行并停止，避免无休止热重试。瞬态恢复则自然收敛，不建行。
- `range_incomplete`：缩批至单块仍无法确认完整（FR-12）；停止推进并暴露原因。
- 行存在 = 日志流暂停有效，重启后依然有效；解除仅人工（SQL）或后续阶段规范；解除后继续前必须重走配置比较 + 区间覆盖核验。
- 与 `indexer_pause` 关系：两行独立共存，互不覆盖原因；日志提交裁决要求两行皆无（002 链级暂停同样阻止日志提交，澄清 A2）。
- **暂停写入的原子条件（过期 worker 不得写入过期暂停）**：暂停事务同样先锁协调行，持锁后独立语句必须同时成立才允许 `INSERT`：
  1. lease 归属裁决通过（owner/token/有效期）——失权 worker 在此被拒；
  2. `log_pause` 仍无行（首暂停获胜，后者收敛）；
  3. 证据仍成立：重读 `log_checkpoint` 仍为 `(start_block=$S, config_hash=$H, next_block=$a)`（进度已变化 → 证据过期，放弃），
     且分歧证据重读仍成立（`chain_view_changed`：引用块仍缺失/哈希仍不同；`validation_failed`：以"最近一次事务外重查仍失败"为准，暂停事务内不调 RPC）；
  任一不成立即 `ROLLBACK` 并放弃暂停（调用方仍停止推进，由新状态决定下一步）。批事务回滚本身永不直接写暂停行：
  回滚后由 worker 另起暂停事务走完整裁决。

## 写事务协议（区间提交 / 首区间 / 暂停统一；锁先行、持锁后独立语句裁决）

与 002 协议同构，协调行复用 `indexer_lease`（R1）。设本批区间 `[a, b]`（`b - a + 1 <= LOG_BATCH_BLOCKS`）：

1. `BEGIN`（短事务；RPC 取日志与逐条校验已在事务外完成，事务内零外部调用；`SET LOCAL statement_timeout = '5s'`）。
2. `INSERT INTO indexer_lease … ON CONFLICT (chain_id) DO NOTHING` —— 幂等确保协调行存在。
3. `SELECT owner_id, fencing_token, expires_at > now() FROM indexer_lease WHERE chain_id=$c FOR UPDATE` —— 获取全链唯一协调锁，保持到 `COMMIT/ROLLBACK`。所有写事务（002 推进/暂停 + 日志提交/暂停）在此处串行化。
4. 后续**独立语句**（Read Committed 每条语句取新快照，可见锁等待期间提交的一切事务）重读并裁决，任一失败即 `ROLLBACK`：
   - `SELECT 1 FROM log_pause WHERE chain_id=$c` 必须无行；
   - `SELECT 1 FROM indexer_pause WHERE chain_id=$c` 必须无行（链级暂停阻止日志提交）；
   - lease 行 `owner_id=$me AND fencing_token=$tok AND expires_at > now()` 必须成立（旧 worker 在此被拒绝）；
   - 进度精确守卫：首区间要求 `log_checkpoint` 无行（有行 → `errStaleState`，转重读比较流程）；推进要求 `SELECT 1 FROM log_checkpoint WHERE chain_id=$c AND start_block=$S AND config_hash=$H AND next_block=$a`（恰好连续 + 配置一致；行数 0 → 过期或配置变化，拒绝）；
   - 链视图重裁决：`[a, b]` 内每一高度在 `chain_blocks` 中存在、canonical 且哈希与本次请求一致（重读行数必须等于区间长度；缺一即回滚走 `chain_view_changed` 暂停）。此即"检查链状态→提交之间无窗口"的保证：裁决读发生在持锁之后。
5. 执行写入：
   - 逐行 `INSERT INTO erc20_transfer_logs … ON CONFLICT (chain_id, block_hash, tx_hash, log_index) DO NOTHING`；
   - 冲突内容比对：对本批每一身份重读已存行 `(contract, block_number, topic0/1/2, data)` 逐字节比较；任一不一致 → `ROLLBACK`，随后另起 `validation_failed(detail.class=identity_conflict)` 暂停事务（见 Table 3 原子条件）。内存中的"我刚取到什么"永不作为正确性依据；
   - 行数核对：本批去重后身份数必须等于实际插入+已存在一致行数（防止静默丢行）；
   - `UPDATE log_checkpoint SET next_block=$b+1, updated_at=now() WHERE chain_id=$c AND start_block=$S AND config_hash=$H AND next_block=$a`（首区间为 `INSERT (chain_id, start_block=$a, config_hash=$H, next_block=$b+1)`），`RowsAffected != 1` → `errStaleState`，拒绝；
   - `COMMIT`。影响 0 行 / 裁决失败 = 前提不成立，调用方必须停推并重读状态，不得按成功处理。
- 暂停事务（`chain_view_changed` / `validation_failed` / `range_incomplete`）：同样先锁协调行 → 按 Table 3 原子条件复核（lease 归属 + 暂停行仍无 + 进度仍为 `(S,H,a)` + 分歧证据仍成立；任一不成立即放弃、`ROLLBACK`）→ `INSERT INTO log_pause … ON CONFLICT (chain_id) DO NOTHING` → `COMMIT`。与推进事务互斥：暂停提交后获锁的推进事务必见暂停行而被拒绝。
- 不确定提交恢复：重连后 `SELECT log_checkpoint` + 重走步骤 4，以数据库为准、幂等继续（FR-16/OQ2）。

## 失败分类（恢复动作对照）

| 信号 | 分类 | 动作 |
|------|------|------|
| 超时 / 连接中断 / 限流 / 节点错误 / 解析失败 | 请求失败 | 有界退避重试（复用 INDEX 退避参数），不推进 |
| 区间过大 / 结果过多 / 响应超限错误；显式 truncated/partial/未取完分页；计数达到已确认上限 | 完整性可疑（`KindIncomplete`） | 丢弃本批，从 `a` 缩批重试；单块仍不可确认 → `range_incomplete` 暂停 |
| 未知错误 | 失败 | 按失败处理，不推进，不猜测 |
| 单条越界/畸形/缺字段/removed | 整批失败 | 不提交不推进；重查仍确定性失败 → `validation_failed` 暂停（`detail.class` 标记类别） |
| 相同身份不同内容 | 冲突 | 整批失败 + `validation_failed(detail.class=identity_conflict)` 暂停 |
| 链视图变化 | 暂停 | `chain_view_changed` 暂停，不提交 |

## 一致性不变量 ↔ 约束对照

- I1（身份唯一 + 块内序号唯一）：PK + `UNIQUE (chain_id, block_hash, log_index)`。
- I2（进度对应完整已处理区间）：区间事务原子性 + 行数核对 + 精确守卫（`next_block=$a`）；无外键但无"进度已进而日志缺失"状态。
- I3（连续无缺口、不跳块）：`next_block=$a` 精确守卫 + 上界覆盖验证；"不跳高"即进度只能落在连续已存序列末端。
- I4（引用块与 002 canonical 一致）：步骤 4 链视图重裁决逐块验证；`block_hash` 存文本但以 `chain_blocks` 重读为准。
- I5（空白名单零查询零写入）：启动期拒绝 + 查询构造断言（空地址集即错，不发 RPC）。
- I6（暂停有效期间推进为 0）：双暂停行裁决 + 持锁后新快照；测试硬断言。

## 日志 ↔ 区块身份关联与 canonical 读取（审计保留）

- 每行日志携带 `(block_number, block_hash)` 双字段：`block_number` 用于区间归属与 004 按高度消费，
  `block_hash` 是与链视图绑定的身份部分（PK 成员）。两者在写入时必须与 `chain_blocks` 同行一致，事后永不改写。
- canonical 读取范式（三处统一，均为 `canonical = TRUE` 点查，不信任 RPC 自述）：
  `SELECT hash FROM chain_blocks WHERE chain_id=$c AND number=$n AND canonical`。
  用途：①上界确定与区间覆盖验证（事务外）；②末端块前后复核（事务外）；③提交裁决重读（事务内持锁后）。
- 003 永不改写已存日志行的 `block_hash`/`block_number`，永不删除历史行：若后续发生重组，
  旧分叉日志仍以原哈希保留，作为 006 恢复的审计依据；新分叉日志以新 `block_hash` 落为不同 PK 行，
  由 006 规范决定取舍。本阶段只保证"引用块在提交瞬间为 canonical"，不追踪之后的变化。

## 索引

- PK/UNIQUE 之外：`CREATE INDEX ON erc20_transfer_logs (chain_id, block_number)` —— 区间回查与 004 消费（按高度消费日志）的主访问模式；非投机（消费路径已在规格目标中明确为 004 输入）。
- 其余访问（checkpoint/pause 单行点查、冲突比对点查）全部被 PK/UNIQUE 覆盖，不建二级索引。
