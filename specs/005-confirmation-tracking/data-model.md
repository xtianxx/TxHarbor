# Data Model: 005-confirmation-tracking

**Branch**: `005-confirmation-tracking` | **Date**: 2026-09-14 | **Migration**: `migrations/000005_confirmation_tracking.sql`
(goose，新文件；002/003/004 迁移不动。`embed.go` 自动收录 `*.sql`。)

005 是 004 充值观察的纯派生推进者：不复制区块或日志真相，不改写 004 的匹配语义，
只做 Pending → Confirmed 的条件转换、策略版本权威记录与确认依据留存。
全部读取经既有索引（`deposit_observations(chain_id, block_number)`、`chain_blocks(chain_id, number)`）。

## 版本三分法（提交携带的三组依据，互不混用）

- **链视图版本** =（tip 高度，tip 哈希）+ 暂停实例状态（`deposit_pause`/`indexer_pause`/`log_pause` 有无）。
  判定"当前 canonical 链归属"与公式输入。
- **策略版本** = `confirmation_policy_history` 的 `policy_seq`（+ 当时阈值 N）。判定"按哪条策略确认"。
- **充值状态版本** = `deposit_observations.status`（pending/confirmed）+ 依据列。判定"是否已转换"。
- 提交事务必须同时携带三组捕获值并在持锁后独立重读复核（见 §提交协议）；任一组失配即回滚。
  禁止用策略 seq 推导链视图有效性，禁止用 deposit `version_seq`（004 匹配版本）替代确认策略版本。

## 确认数计算（规格公式的等价安全形式）

- 门禁比较用等价式 `tip >= h && tip - h >= N - 1`（N ≥ 1；与 `max(0, tip - h + 1) >= N` 数学等价，
  证明：tip < h 时两边皆假；tip ≥ h 时 `tip - h + 1 >= N ⟺ tip - h >= N - 1`），任意输入无溢出。
- 精确确认数（`confirmations` 列与展示）按规格公式原样计算；可达域 `[0, MaxInt64]`
  （tip/h 来源列均为 `BIGINT CHECK (>= 0)`）内恒精确；饱和 guard 仅纵深防御，被触发按内部错误拒绝提交。
- 精度与转换设计（OI-1 决议，2026-09-14 remediation）：`confirmations` 列用 `NUMERIC`
  （精确十进制，整数性由 `CHECK (confirmations = floor(confirmations))` 保证，非负由 `CHECK (>= 0)` 保证；
  与 004 `amount NUMERIC` 精确整数先例同形）。tip=MaxInt64、h=0 时精确值 2^63 可精确保存、读取和审计。
  Go↔SQL 转换：写入为 uint64→十进制字符串→`NUMERIC`（全程无 int64/float64 中转，
  形态镜像 `depositNumericAmount`）；读取为十进制字符串→uint64，遇非整数/超范围按内部错误拒绝。
  `BIGINT` 列（高度、阈值）保持不变——其域 `[0, MaxInt64]`（N）/`[0, MaxInt64]`（高度）天然容纳，无需改动。
- N 的可达域为 `[1, MaxInt64]`：`BIGINT` 列即系统表示上限（解析期拒绝更大值，不属业务上限）；
  极大合法 N（如 MaxInt64）永不达标，行为是合法等待而非错误。详见 research R1。

## Table 1 — `deposit_observations`（005 扩展：状态拓宽 + 依据列）

004 建表（`000004`）基础上，本迁移只做加法（`ALTER TABLE`，不碰 004 列定义）：

| Column（新增） | Type | Constraints | Notes |
|--------|------|-------------|-------|
| confirmed_at | TIMESTAMPTZ | NULL | 转换时间；pending 行 MUST NULL |
| confirm_tip_number | BIGINT | NULL，`CHECK (>= 0)` | 转换依据链头高度；confirmed 行 MUST NOT NULL |
| confirm_tip_hash | TEXT | NULL，`^0x[0-9a-f]{64}$` | 转换依据链头哈希；confirmed 行 MUST NOT NULL |
| confirm_threshold | BIGINT | NULL，`CHECK (> 0)` | 转换当时阈值 N；confirmed 行 MUST NOT NULL |
| confirmations | NUMERIC | NULL，`CHECK (confirmations >= 0 AND confirmations = floor(confirmations))` | 转换当时所得确认数，精确整数（NUMERIC 精确十进制，无 float 通道；见 §确认数计算）；confirmed 行 MUST NOT NULL |
| confirm_policy_seq | BIGINT | NULL，`CHECK (> 0)`，FK → `confirmation_policy_history (chain_id, policy_seq)` | 转换当时策略版本（显式引用，非时间戳推导）；confirmed 行 MUST NOT NULL |

- `status` CHECK 由 `000004` 的 `status = 'pending'` 拓宽为 `status IN ('pending', 'confirmed')`
 （`ALTER TABLE … DROP CONSTRAINT … ADD CONSTRAINT`；006 未来以自有迁移加入 `'orphaned'`）。
- 一致性 CHECK：`(status = 'pending') = (confirmed_at IS NULL)`，且 confirmed 行六列全非空
  （`CHECK (status = 'pending' OR (confirmed_at IS NOT NULL AND confirm_tip_number IS NOT NULL
  AND confirm_tip_hash IS NOT NULL AND confirm_threshold IS NOT NULL AND confirmations IS NOT NULL
  AND confirm_policy_seq IS NOT NULL))`）。升级断言：000004 的 CHECK 已保证现存行全为 pending，
  故无历史依据可虚构、无需回填（迁移内以 `DO` 块断言 `COUNT(*) FILTER (WHERE status <> 'pending') = 0`）。
- 转换写只允许 `WHERE status = 'pending'` 的条件 `UPDATE`（应用层 + 行数核对双保险）；
  confirmed 行任何字段对 005 不可写（后续重组撤销是 006 自有迁移 + 自有事务的职责）。
- 不可改写执行机制（无触发器）：(a) 005 全部写语句恒带 `status = 'pending'` 谓词，无第二写路径
  （代码审查门：新增 UPDATE 必须出示谓词）；(b) 一致性测试尝试改写 confirmed 行，断言影响 0 行
  （quickstart D2/D5）；(c) 完整性抽查 SQL（contracts）断言依据非空。
  形态镜像 004 append-only"由实现断言行数单调"先例（`000004` 迁移头注释），不用 DB 触发器——
  触发器会成为 006 必须先拆除的障碍物，而应用谓词 + 测试对 006 零约束。
- 006 保留契约：依据六列 + `confirmed_at` 任何阶段 MUST NOT 被 UPDATE/DELETE（含 006）；
  006 的 Orphaned 标记只改 `status`（自有迁移拓宽 CHECK），保留全部依据列作为其重验/再确认输入；
  005 永不删除观察行。
- 新索引（候选扫描用， patrol 查询已被 004 两索引覆盖）：
  `CREATE INDEX deposit_observations_pending_height_idx ON deposit_observations (chain_id, block_number)
  WHERE status = 'pending'` —— 无游标有序扫描的唯一新增索引；Confirmed 行不进入索引，无膨胀。

## Table 2 — `confirmation_policy_history`（策略版本权威来源，只增不改）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK 首列，`CHECK (> 0)` | 单链版本链 |
| policy_seq | BIGINT | PK，`CHECK (> 0)`，本链递增（事务内取 max+1，首版本为 1） | 策略版本身份 |
| threshold | BIGINT | `NOT NULL CHECK (> 0)` | 本版本阈值 N；可达域 `[1, MaxInt64]`（`BIGINT` 即系统表示上限，解析期拒绝更大值；见 §确认数计算） |
| prev_seq | BIGINT | NULL（首版本） | 上一版本号；首行 NULL |
| operator | TEXT | `NOT NULL DEFAULT ''` | 授权操作者（首版本为 `bootstrap`，即首确认提交事务内原子建行） |
| reason | TEXT | `NOT NULL DEFAULT ''` | 授权原因 |
| request_id | TEXT | NULL（仅首版本行）；非空时本链唯一（`UNIQUE (chain_id, request_id)`；首版本单行另以 partial unique 约束） | 授权请求身份（语义镜像 004：同 ID 同参返原结果，同 ID 异参拒绝，未记录失败不绑定） |
| expected_old_seq | BIGINT | `NOT NULL DEFAULT 0` | 调用方预期的当前版本；必须等于锁内 max seq，否则过期拒绝 |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 授权时刻（审计序） |

- 有效策略 = `MAX(policy_seq)` 行（单表，无"当前行"双写；见 research R3）。
- 版本链不断：`prev_seq` FK 自引同链上一行（镜像 004）；相同阈值再次授权按空授权拒绝（H′ 必须不同），
  故 seq 推进恒蕴含阈值变化，提交守卫可用 seq 相等一次性覆盖。
- 与 `deposit_config_history` 无关联：两个正交版本域，禁止互相引用或复用 seq（见 §版本三分法）。
- 无二级索引：按 PK 点查 + `MAX(policy_seq)` 聚合（单链行数极少）。

## 提交协议（确认转换 / 首确认 / 暂停判定统一；锁先行、持锁后独立语句裁决）

与 002/003/004 协议同构，协调行复用 `indexer_lease`（research R2）。设候选观察为 P（身份、
`block_number=h`、`block_hash=bh`），捕获依据为（tip 高度 T + 哈希 TH，策略 seq S + 阈值 N）。
事务外已完成：tip 读取、候选查询、公式计算（R1）、逐项预检；事务内零外部调用，
`SET LOCAL statement_timeout = '5s'`（沿用 `writeGuard`）。

1. `BEGIN` → `INSERT INTO indexer_lease … ON CONFLICT DO NOTHING`（幂等确保行存在）。
2. `SELECT … FROM indexer_lease WHERE chain_id=$c FOR UPDATE`（全链唯一协调锁，保持到 COMMIT；
   确认、004 消费、授权切换、暂停写事务在此处串行化，锁顺序唯一，无死锁环）。
3. 持锁后**独立语句**重读并裁决（Read Committed 每条语句取新快照，可见锁等待期间提交的一切事务），
   任一失败即 `ROLLBACK`：
   - `SELECT 1 FROM deposit_pause / indexer_pause / log_pause WHERE chain_id=$c` 必须皆无行；
   - lease 行 `owner_id=$me AND fencing_token=$tok AND expires_at > now()` 必须成立（失权 worker 在此被拒）；
   - 策略守卫：`SELECT policy_seq, threshold FROM confirmation_policy_history WHERE chain_id=$c
     ORDER BY policy_seq DESC LIMIT 1` 必须等于捕获 (S, N)（首确认时允许无行，转 §首确认）；
   - 链头重裁决：`SELECT number, hash FROM chain_blocks WHERE chain_id=$c AND canonical
     ORDER BY number DESC LIMIT 1` 必须等于捕获 (T, TH)（tip 缺失→无行→拒绝；tip 变化→失配→拒绝）；
   - 候选重裁决：观察行仍 `status='pending'`，且 `chain_blocks` 中 `(h)` 行存在、canonical 且哈希 = bh
     （缺一即回滚；引用缺失/哈希不一致属链视图异常，按 §候选分类停止确认——US3-2/Edge-170，
     不建暂停行：暂停行归属 002/004 流）；
   - 用重读值重算确认数（R1），必须 `>= N`（tip 回退导致不足即回滚）。
4. 执行写入：`UPDATE deposit_observations SET status='confirmed', confirmed_at=now(), …依据列…
   WHERE chain_id=$c AND block_hash=$bh AND tx_hash=$tx AND log_index=$li AND status='pending'`；
   `RowsAffected != 1` → 并发已转换（收敛，非错误）或前提漂移 → 回滚并重读（重读见 confirmed 即收敛成功）。
5. `COMMIT`。提交结果未知（连接断开）时重连后按观察行 PK 重读定性，再决策（镜像 004 不确定提交恢复）。

### 为什么"检查通过后、提交前条件变化"不能产生旧结果提交（并发时序论证）

仅依赖两条成立语义（与 002 论证同源）：(a) 同一 `indexer_lease` 行的 `FOR UPDATE` 互斥至事务结束；
(b) 同一事务内持锁后发出的后续独立语句取新快照。

- **情形 1（暂停/策略切换/ tip 推进在候选读取后、确认提交前落地）**：后来者先获协调锁并提交
  （暂停行 INSERT / 策略新 seq 行 / 新 canonical 块）；确认事务随后获锁，其步骤 3 重读（新快照）必见
  暂停行存在 / seq 失配 / tip 失配 → `ROLLBACK`，零写入。后来者提交前已 COMMIT 的确认属锁排序决定的合法先后。
- **情形 2（双 worker 并发确认同一 Pending）**：胜者提交后，败者步骤 3 重读见 `status='confirmed'` →
  条件 UPDATE 影响 0 行 → 回滚并重读收敛；确认时间与依据为胜者值，不可改写（I2）。
- **情形 3（旧配置 worker 在策略切换后提交）**：其捕获 seq S 恒小于当前 max → 步骤 3 策略守卫失配 →
  拒绝；重读见 env N ≠ 现行阈值 → 漂移停止（research R7），永不提交旧结果。
- 条件 UPDATE 的 `status='pending'` 谓词是第二道门（防步骤 3 与写入之间的本事务内竞窗无意义——
  同一事务内无并发；真正防的是锁等待期间的排序变化，而那已被步骤 3 覆盖；双门即纵深）。

## 候选分类：行级等待 vs 循环级停止（规格原文决定，无新业务选择）

分类依据为已批准规格原文（remediation 逐条核对，非新决定）：
FR-06"引用区块缺失或非 canonical……任一成立即不得提交确认转换"；
US3-2"链视图异常（暂停有效、引用区块无法核实）→ 停止确认，不提交任何转换"；
Edge"同高度存在 canonical 行但哈希与充值引用不一致：不得确认，按链视图异常停止"。

- **行级等待**（该行留 Pending，继续同批，无异常，仅 `below_depth` 一类）：
  选中后 tip 推进/重算不足。纯竞态，下 tick 自动重估；`skipped_total{below_depth}` 只计常规重估。
- **循环级停止**（整循环 halt，本 tick 及后续零提交；对应 `confirmation_state=3`，
  链头缺失表现为 state=1 等待可信 tip——零提交语义与 FR-02"停止确认"一致，tip 出现即恢复）：
  引用行缺失（= 引用区块无法核实，US3-2）、哈希不一致（Edge-170 按链视图异常）、
  链头缺失、tip 不可信、任一暂停有效、策略漂移。停止前不建暂停行（暂停行归属 002/004 流；
  005 以循环停止 + error 日志 `reason=reference_unverifiable|…` 表达，006 接管前保持停止）。
- **后果（规格要求，非设计选择）**：前排异常行阻塞后排（US3-2"不提交任何转换"），
  后排合格行在异常消除前不被处理——006 接管前保持停止。无饿死论证仅适用于良性情形：
  tip、N 固定时资格对 h 单调，`below_depth` 前排不合格蕴含后排亦不合格，不存在良性饿死；
  异常阻塞的解除属 006/人工职责，不属 005。
- **同批独立性（良性范围内）**：`below_depth` 行的留 Pending 不影响同批其他行；
  任一异常行触发即整批终止（已提交者属锁排序合法先后，未提交者零写入，见 §并发时序情形 1）。

## 首确认协议（策略 bootstrap，与 004 首单元同构）

- 无 `confirmation_policy_history` 行时，首个确认提交事务在步骤 3 改为：确认仍无行 →
  同事务 `INSERT (chain_id, 1, N, NULL, 'bootstrap', …)`（`request_id` NULL，partial unique 保证单行）→
  继续步骤 4（策略守卫按新建行 (1, N) 成立）。并发双首写以 PK `(chain_id, 1)` 串行化：
  败者唯一冲突回滚 → 重读见行 → 走正常路径（阈值一致则继续，不一致则漂移拒绝）。
- 无充值时：策略表可以长期无行；循环有候选才进入提交，无候选即等待——策略在首个达标转换时确立，
  此前不存在"有效策略"，也不需要（无决策可做）。
- 分歧双首启（两实例 env N 不同、同时首次启动）：胜者 bootstrap 落定其 N 为初始有效策略；
  败者重读见阈值漂移 → 以配置漂移错误大声停止（非零退出，见 §切换后恢复），永不静默跟随。
  抢锁胜负只决定"谁先写"，不决定"谁正确"——若胜者 N 配错，运维以授权切换纠正（§授权切换协议），
  不存在胜者任意决定策略的路径。
- 启动比较（serve 构造期，无 I/O 出错即拒绝退出，镜像 004）：行存在且阈值 ≠ env N → 漂移拒绝；
  相等才允许进入循环。比较对象是现行阈值，绝不是候选高度。

## 授权切换协议（特权 SQL，DB 操作员；锁先行、重验门、单行原子提交）

入口与角色（无旁路）：切换入口是 DB 操作员直接执行的特权 SQL（语句本体落在 `confirmauth.go`，
经管理工具/psql 执行；与 004"授权保持 loop 外特权操作"同形，见 `serve.go:212-235` 注释）。
不设新端点、新服务、新角色。全部守卫（旧 seq 预期、新值合法且不同、锁内重验、request_id 定性）
都在同一事务内的 SQL 中执行——不存在绕过守卫的第二入口；调用方传参不参与正确性判定，
只参与请求身份识别。

1. 事务外按 `(chain_id, request_id)` 查 history 定性（镜像 004：命中同参返原结果 / 异参拒绝 /
   未命中继续；明确失败无记录时 ID 未绑定，同 ID 可重试）。
2. 解析新阈值（同 Q1 解析规则：必填正整数，无默认）→ 要求 `expected_old_seq` 等于当前 max seq
   （否则过期拒绝）→ 要求新阈值 ≠ 现行阈值（否则空授权拒绝）。
3. `BEGIN` → 确保 lease 行 → `FOR UPDATE` 取协调锁（跳过 owner/token 检查，特权路径；操作员记审计列）。
4. 持锁后重验：max seq 仍为 expected_old_seq；无行 ↔ 首确认未发生时拒绝"无源版本可转换"
   （首行只能由首确认事务建立）；通过 → 单行 `INSERT` 新策略行 → `COMMIT`。
   单语句即原子：失败无部分生效、无多版本（有效 = max seq 恒唯一）。
5. 提交结果未知（断连/超时）：按 `request_id` 重读定性——命中且同参 → 返回已记录结果（确立的
   policy_seq 与阈值），不重新执行；未命中 → ID 未绑定，按独立候选重试（允许同 ID 重用，
   镜像 004 绑定规则）；命中异参 → 拒绝，不产生状态变化。

## 切换后恢复程序（退出范围与重启步骤）

- 旧 worker 收敛：切换后持旧 env N 的 worker 在下一次提交守卫失配 → 重读确认漂移 →
  确认 ServeLoop 返回漂移错误。按既有扇出语义（`coordinator.go:106-174 serveStreams`：
  任一循环的停止错误取消全部循环），漂移错误使本进程四循环全部取消、进程非零退出。
  这是整进程的大声停止，不是暂停行——切换本身零暂停行写入，故 header/log/deposit 循环从不因策略切换被"阻止"；
  它们随进程退出而停，其各自进度均 durable（002/003/004 恢复协议），重启即续。
- 重启步骤（标准发版流程）：授权成功 → 全舰队更新 env N → 逐实例重启；新 N 进程启动比较通过即恢复确认。
  在旧进程退出、新进程就绪之间，确认短暂停顿（零旧提交）；deposit 识别在新进程继续，不存在"旧配置永久停摆"
  （停摆只发生在无人更新配置时，属运维未完成程序，而非协议死路）。
- 确认循环与其他循环的退出范围：确认漂移只应停确认，但既有协调器只支持"任一错全停"；
  本阶段复用该语义（不新增 per-loop 独立退出机制——那是新机制，违反最小性），runbook 以整进程重启收敛。

## 一致性不变量 ↔ 约束对照

- I1（同一来源最多一次有效转换）：PK + 条件 UPDATE + 行数核对。
- I2（首次时间与依据不可变）：005 对 confirmed 行零写 + CHECK 非空；重复检查收敛路径只读。
- I3（引用哈希与提交时刻 canonical 一致）：步骤 3 候选重裁决逐行比对；内存值永不作为依据。
- I4（暂停/版本失配期间零提交）：步骤 3 三暂停 + 双版本守卫；论证见 §并发时序。
- I5（005 永不回退/删除 Confirmed）：无相关语句；回退是 006 自有迁移 + 事务的职责。
- 策略单调：UNIQUE(request_id) + prev FK + max-seq 有效定义；损坏态（dangling prev）FK 即拒。

## 索引

- 新增唯一索引：`deposit_observations_pending_height_idx`（partial，候选扫描；§Table 1）。
- 其余访问（策略 max seq、tip 查询、观察 PK 点查、暂停单行读写）全被 PK 覆盖，不建二级索引。
- 006 预留：`status` 扩展位（`'orphaned'`）、依据列即 006 重验输入；006 自有迁移不得修改 005 列语义，
  只拓宽状态集合（交接契约见 research R9）。
  准确含义声明："orphaned 预留"纯属设计说明——005 迁移 DDL 中无 `'orphaned'` 字样、无占位约束、
  无预留列（grep 可证）；当前必要性仅两项：(a) 让 006 知道 `status` CHECK 非封闭，拓宽是预期演进；
  (b) 依据列即 006 的重验输入（保留契约见 §Table 1）。005 不定义 Orphaned 语义（spec Non-Goals），
  无必要 DDL 全部留给 006。

## 写事务与锁顺序总表（005 新增两类 + 复用三类）

| 事务 | 首锁 | 持锁后裁决 | 写入 |
|------|------|------------|------|
| 确认提交（含首确认 bootstrap） | `indexer_lease` 行 | 三暂停皆无 + lease 归属 + 策略 (S,N) + tip (T,TH) + 候选 pending + 哈希一致 + 重算 ≥ N | 条件 UPDATE 观察行（+ 首确认时同事务 INSERT 策略首行） |
| 授权切换（特权） | `indexer_lease` 行（跳过归属检查） | max seq == expected_old_seq + 新阈值合法且不同 + 有源版本 | 单行 INSERT 新策略行 |
| 004 消费/暂停/授权（既有） | 同左 | 既有（不动） | 既有（不动） |

三类写事务锁顺序恒为"协调行首锁"，不存在锁顺序环；暂停提交后获锁的确认事务必见暂停行而被拒
（与 002 §并发正确性论证同构）。
