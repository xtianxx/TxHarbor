# Data Model: 004-deposit-detection

**Branch**: `004-deposit-detection` | **Date**: 2026-09-13 | **Migration**: `migrations/000004_deposit_detection.sql`
(goose，新文件；002/003 迁移不动。`embed.go` 自动收录 `*.sql`。)

004 是 003 行的纯派生消费者：不复制区块或日志真相，只存充值观察、处理进度、暂停信号与配置版本历史。
全部读取经 `(chain_id, block_number)` 索引（003 已建，非投机）。

## Table 1 — `deposit_observations`（充值观察，初始 Pending）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK 首列，`CHECK (> 0)` | 配置期望链 |
| block_hash | TEXT | PK，`^0x[0-9a-f]{64}$` | 来源身份：与上游行一致 |
| tx_hash | TEXT | PK，`^0x[0-9a-f]{64}$` | 来源身份 |
| log_index | BIGINT | PK，`CHECK (>= 0)` | 来源身份（块内序号） |
| block_number | BIGINT | `NOT NULL CHECK (>= 0)` | 区间归属与消费排序 |
| contract | TEXT | `NOT NULL`，`^0x[0-9a-f]{40}$` | 小写归一化，属资产集合 |
| sender | TEXT | `NOT NULL`，`^0x[0-9a-f]{40}$` | topic1 低 20 字节 |
| recipient | TEXT | `NOT NULL`，`^0x[0-9a-f]{40}$` | topic2 低 20 字节，属监控集合 |
| amount | NUMERIC | `NOT NULL CHECK (amount > 0)` | 最小单位整数，全程无浮点 |
| status | TEXT | `NOT NULL DEFAULT 'pending' CHECK (status = 'pending')` | 显式初始态；005 以自有迁移扩展 |
| version_seq | BIGINT | `NOT NULL`，FK → `deposit_config_history (chain_id, version_seq)` | 首次生成本观察时依据的配置版本（seq 身份；回放保留原值，不重写） |
| observed_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用，非排序依据，非版本关联依据 |

- `PRIMARY KEY (chain_id, block_hash, tx_hash, log_index)` —— 与上游行同一身份，
  规格 I1（"每条来源日志最多一条有效观察"，含重处理、恢复与收缩回放后）由存储强制；重复处理收敛到同一行。
  版本关联显式存 `version_seq`（FK 指历史版本），不用时间戳推导：同时间戳、相同内容复现的版本可区分；
  回放遇到已有观察保留原版本引用，不重写为当前版本。
- `status` 单值 CHECK 是有意显式（章程 V）：行存在即 Pending 观察，005 以自有迁移放宽 CHECK 集合；
  004 永不写入除 `pending` 外的值。
- 零值来源不建行（FR-03）；非匹配来源不建行；冲突内容在写入事务内比对发现（见 §写事务协议步骤 5），冲突不覆盖。
- 状态说明：行一旦提交即为该来源身份的唯一观察；`amount` 为十进制精确整数（uint256 全范围），
  Go 侧 `big.Int` → 十进制字符串 → `NUMERIC`，无 float 通路。

## Table 2 — `deposit_checkpoint`（充值处理进度，下一待处理高度）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK，`CHECK (> 0)` | 单链单行，与 003 进度行相互独立 |
| start_block | BIGINT | `NOT NULL CHECK (>= 0)` | 当前已授权配置的全局起点，授权事务原子更新 |
| config_hash | CHAR(64) | `NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$')` | 充值配置身份（R3 编码） |
| next_block | BIGINT | `NOT NULL CHECK (>= 0)` | 下一待处理高度（块粒度） |
| updated_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 诊断用 |

- `CHECK (next_block >= start_block)` —— 进度永不倒退到起点之前。
- 无行 = 空进度（沿用 002/003 约定，不用哨兵值）。
- **本表无外键（显式决定，与 003 同理）**：`next_block` routinely 指向尚无对应 `chain_blocks` 行的
  高度（追头场景）。I2 不用外键表达，而由三者合成保证：单元事务原子性 + 精确守卫
  （`next_block=$a`）+ 行数核对。
- 配置变更检测（FR-06）：启动及每次提交裁决时，若行存在且配置 `(start_block, config_hash)` 任一不同，
  拒绝并报错；相等才允许继续。比较对象是行内 `start_block`，绝不是 `next_block`。
- 状态完整性：同链 checkpoint 行与 history 行必须同有或同无；单侧缺失为损坏态，
  明确报错，禁按首次启动补建、禁按漂移改回、禁授权自动修复；修复须人工核查后重验，保留诊断证据，不猜测正确状态。
  同有时核对 checkpoint 行内 `(start_block, config_hash)` 与最新 history 行一致（不一致即损坏态同上）。

## Table 3 — `deposit_pause`（暂停与结构性缺口信号）

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK | 单链单行（当前暂停/停止） |
| pause_id | BIGINT | `NOT NULL DEFAULT nextval('deposit_pause_id_seq')`，UNIQUE，永不复用 | 实例身份（同高同类同版本的新实例亦不同值；SEQUENCE 保证删除后不回收） |
| revision | BIGINT | `NOT NULL DEFAULT 1`，合并更新时 +1 | 同实例修订号；释放须同时匹配实例与修订 |
| height | BIGINT | `NOT NULL` | 发现问题的高度（单元首块或缺口起点） |
| kind | TEXT | `NOT NULL CHECK IN ('upstream_gap','chain_view_changed','validation_failed')` | 原因分类（下表；机读细分进 `detail`） |
| detail | TEXT | `NOT NULL DEFAULT ''` | 人读 + 机读诊断：`class=<分类> gap=<起-止> cause=<…> config=<版本>` 等键值片段；无凭据、无原始数据转储 |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 暂停时刻 |

- `upstream_gap`：暂时性缺口等待超出为"等待"时**不建行**（state=1/4 表达等待）；本行只记录
  **结构性缺口停止**（`detail.class=structural`，含缺口范围、原因 `below_upstream_start|asset_not_indexed`、
  受影响配置版本）与需 006 的情形（`detail` 含 `needs_006=true`）。
- `chain_view_changed`：引用块与 `chain_blocks` 不一致、或提交时链视图已变化。
- `validation_failed`：逐条解析/匹配失败的持久化信号。`detail.class` 取值覆盖规格非法类别：
  `bad_address | bad_topics | bad_amount | out_of_range | missing_field | identity_conflict`。
  策略：失败当即整批失败、不推进；同一单元重查仍确定性失败 → 持久化本行并停止，避免无休止热重试。
- 行存在 = 充值流暂停/停止有效，重启后依然有效；解除有两条路径（R11），两条都必须按实例条件执行并写审计：
  (a) 人工解除——`DELETE ... WHERE chain_id=$c AND pause_id=$p AND revision=$r`（$p/$r 为读取时所见），
  同一事务内 `INSERT INTO deposit_pause_audit …`（操作者、原因、实例身份、适用修订与版本、解除时间与结果），
  审计失败则解除回滚；影响 0 行 → 重读后重新决定，禁把旧条件用于新实例；
  解除后继续前仍须走完整重验（配置比较 + 覆盖核验等），重验失败即重建暂停；
   条件解除影响 0 行后的结果判定查审计表：命中 (pause_id, revision, release) 原记录
   → 返回"该目标已解除"及原审计结果（原结果、原操作者、原时间），不重复写入，
   不触碰当前新暂停，不把原解除归功于本次调用者；无独立解除请求身份时只返回该事实，
   不声称识别为同一次请求——操作者/原因/实例/修订相同亦不证明同一请求，
   参数差异不改变已提交的历史事实；未命中 → 报告目标不存在、已变更或需核查，
   不自动解除当前暂停；
  (b) 授权转换事务——暂停处置同样按实例 + 修订匹配（失配即证据过期，拒绝），处置结论与审计行同事务提交。
  两种路径都不直接授权写入（解除≠授权，授权只来自持锁后重裁决）。
- 暂停 `detail` 必须携带版本上下文（`version=<seq>` 明确归属，哈希短标签仅作辅助展示；结构缺口另含缺口范围与原因），使多次切换（含内容回环）下的暂停归属可审计；新结构缺口在旧暂停行未解除时，由暂停写事务原子更新其内容（revision +1 并写 merge 审计行；须走 Table 3 原子条件＋持 lease 锁；授权事务禁用此写法，只能保留或条件 DELETE），不另建行（单行约束下的合并语义）。
  （2026-09-13 Q8 累积式裁决：上句按累积语义执行——保留 `pause_id` 与首原因，新有效原因以 `+merged[rev=<R>] kind=<K> height=<H> version=<S> :: <原样证据>` 段追加、`revision` +1，
  各原因类型/范围/来源版本/证据全保留、MUST NOT 覆盖；同一有效原因幂等（零写零审计）；旧基证据不合入；
  段编码、等价谓词与审计格式见 research R6/R11 与暂停写事务实现节。）
- 累积合并后的读法：`height` / `kind` 列 MUST 只读作**首原因**（合并永不改写这两列）；`detail` 为多段累积文本
  （首段沿用 `<ev.detail> version=<S>`，其后每段 `\n+merged[rev=<R>] kind=<K> height=<H> version=<S> :: <ev.detail>`）。
  恢复判定、缺口评估与 needs_006 判定 MUST 读取全量 `detail` 多段，MUST NOT 只看首段（2026-09-13 Q8）。
- 历史保留：释放不删除原因——审计行携带 kind/height/detail/version 快照；活动行删除后仍可解释。
  审计行与暂停行无 FK（行删除后审计仍在），按 (chain_id, pause_id) 追踪实例全生命周期。
- 暂停写入版本回环防护：证据在锁外按某版本采集，锁内必须按持锁后当前版本重估（缺口分类用当前版本快照），行打标持锁后当前 seq；锁外旧版本依据不得直接落行。人工解除路径天然取当前版本重验，无回环问题。
- 与 `log_pause` / `indexer_pause` 关系：三行独立共存，互不覆盖原因；充值提交裁决要求三行皆无
  （任一上游暂停同样阻止充值提交，澄清 Q2）。
- **暂停写入的原子条件（过期 worker 不得写入过期暂停）**：暂停事务同样先锁协调行，持锁后独立语句
  必须同时成立才允许 `INSERT`：
  1. lease 归属裁决通过（owner/token/有效期）——失权 worker 在此被拒；
  2. `deposit_pause` 仍无行（首暂停获胜，后者收敛）；
  2b. （2026-09-13 Q8 累积式裁决）`deposit_pause` 已有行时不做首胜丢弃，而进入累积合并评估：
      持锁后 lease 归属（条件 1）、捕获基精确一致、新证据锁内重验三者缺一即放弃本次暂停写，
      活行 MUST NOT 改动、旧基证据 MUST NOT 合入；三者成立且同一有效原因（kind, height, core）已存在时
      幂等收敛（零写零审计）；否则保留 `pause_id` 与首原因，新原因段追加、`revision` +1，
      merge 审计行同事务提交，任一失败全回滚。授权事务禁用此写法（只保留或条件 DELETE）。
  3. 证据仍成立：重读 `deposit_checkpoint` 仍为 `(start_block=$S, config_hash=$H, next_block=$a)`
     （进度已变化 → 证据过期，放弃），且分歧/缺口证据重读仍成立
     （以"最近一次事务外重查仍失败/仍缺失"为准，暂停事务内零外部调用——004 暂停事务内只有 SQL）；
  任一不成立即 `ROLLBACK` 并放弃暂停（调用方仍停止推进，由新状态决定下一步）。批事务回滚本身永不直接写暂停行：
  回滚后由 worker 另起暂停事务走完整裁决。

## Table 4 — `deposit_config_history`（配置版本历史 + 授权审计）

当前 `config_hash` 不能替代历史版本及生效边界：结构缺口分类需要历史白名单，回放需要历史生效高度，
审计需要授权全记录。本表只增不改（行永不 `UPDATE/DELETE`，由实现断言行数单调）。

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | PK 首列，`CHECK (> 0)` | 单链版本链 |
| version_seq | BIGINT | PK，`CHECK (> 0)`，本链递增（授权事务内取 max+1，首版本为 1） | 版本身份（seq 唯一；相同内容哈希可重复出现，各占一行，禁作内容去重） |
| config_hash | CHAR(64) | `NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$')` | 本次授权确立的版本内容；非唯一，禁单独替代版本身份 |
| prev_seq | BIGINT | NULL（首版本） | 上一版本号；首版本 NULL |
| start_block | BIGINT | `NOT NULL CHECK (>= 0)` | 本版本全局起点 |
| assets | TEXT | `NOT NULL` | 本版本资产快照：`contract:effective` 行，字典序，无尾换行 |
| watches | TEXT | `NOT NULL` | 本版本监控地址快照：`address:effective` 行，字典序，无尾换行 |
| replay_from | BIGINT | `NOT NULL CHECK (>= 0)` | 本次切换设定的回放位置（Q5/Q6 规则计算值） |
| operator | TEXT | `NOT NULL DEFAULT ''` | 授权操作者（首版本为 `bootstrap`，即首单元原子建行） |
| reason | TEXT | `NOT NULL DEFAULT ''` | 授权原因；结构缺口另含缺口范围 |
| request_id | TEXT | NULL（仅首版本行）；非空时本链唯一（UNIQUE (chain_id, request_id)；首版本单行另以 partial unique 约束） | 授权请求身份（调用方提供，同链稳定；同 ID 同参认重试（已记录），同 ID 异参拒绝（已记录；未记录的明确失败不绑定 ID，见步骤 1 绑定规则），异 ID 独立校验） |
| expected_pause_id | BIGINT | NULL；与 expected_pause_revision 同时提供或同时为空（CHECK 双空或双非空） | 调用方授权的目标暂停实例；为空表示不授权解除或替换任何已有暂停（非通配） |
| expected_pause_revision | BIGINT | NULL；同上 CHECK | 目标实例在授权依据读取时的修订号 |
| created_at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 授权时刻（审计序） |

- 版本链不断：每行 `prev_seq` 指向同链上一行的 `version_seq`（首行 NULL）；并发分叉以授权事务的
  精确守卫拒绝保证线性，无分叉行。相同内容哈希再次出现（如配置回环）即新行新 seq，不与旧行合并。
- 首版本行不属于授权请求：`request_id` 为 NULL，`operator`=`bootstrap`，只允许单行
  （partial unique），不参与请求幂等判定。
- 请求身份规则（禁仅比目标哈希）：授权参数含预期旧版本 seq（`expected_old_seq`），仅 old_hash 不足。
  按 (chain_id, request_id) 查 history：命中且调用方意图参数（request_id、expected_old_seq、
  配置快照：资产/地址/高度/起点→H′、operator、reason）一致
  → 同一次请求已提交，返回已记录结果（确立的 version_seq、replay_from），不重新执行，
  即使当前已进入更晚版本；
   命中但意图参数任一不同 → 同 ID 异参（已记录请求的异参），明确拒绝，不产生状态变化；
   未命中 → 独立请求：要求 expected_old_seq 等于当前最新 version_seq（否则过期拒绝），
  H′ 与当前不同（否则空授权拒绝）；不同 request_id 即使意图相同也不合并。
  目标绑定：expected_pause 二列同时提供或同时为空，同时为空表示不授权解除或替换任何已有暂停
  （非通配目标）；提供目标时，锁内必须匹配该实例及修订（见步骤 3），否则拒绝转换，
   不得自动改为处置新暂停；同 ID 更改目标即同 ID 异参（已记录请求），明确拒绝。
  系统计算的处置结果仍为派生结果，但不得超出调用方授权范围；
   不得通过修改实例、修订或归属变相绕过解除授权
   （授权事务禁 UPDATE／合并暂停行：只允许原样保留或按实例＋修订条件 DELETE）。
  replay_from、新 seq、暂停处置结论是派生执行结果，只返回不参与同一性比较；
  禁止先按当前状态重算 replay_from 再将其纳入比对（进度变化会导致已完成请求的重算值漂移而误拒）。
- 历史快照使跨版本分类自给自足：回放区间适用的旧白名单/旧生效高度从本表读，不向 003 追索历史配置。
- 首版本行由首单元提交事务内原子写入（operator=`bootstrap`），与 checkpoint 首行同事务，
  语义等同"创世授权"；后续行只由授权转换事务写入。

## Table 5 — `deposit_pause_audit`（暂停实例事件审计，只增不改）

人工解除与授权处置都必须留痕：仅输出外部日志不足以保证，审计行与暂停变更同事务提交。
与暂停行无 FK（行删除/更新后审计仍在）。

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| chain_id | BIGINT | `NOT NULL CHECK (> 0)` | 暂停所属链 |
| pause_id | BIGINT | `NOT NULL` | 实例身份（被释放/合并的实例；不可复用） |
| revision | BIGINT | `NOT NULL CHECK (> 0)` | 事件发生时该实例的修订号 |
| action | TEXT | `NOT NULL CHECK IN ('release','merge')` | 解除 / 合并更新 |
| operator | TEXT | `NOT NULL DEFAULT ''` | 操作者（授权路径为授权 operator + request_id） |
| reason | TEXT | `NOT NULL DEFAULT ''` | 解除/合并原因 |
| version_seq | BIGINT | `NOT NULL` | 事件发生时的配置版本 |
| kind | TEXT | `NOT NULL` | 暂停原因分类快照 |
| height | BIGINT | `NOT NULL` | 暂停高度快照 |
| detail | TEXT | `NOT NULL DEFAULT ''` | 暂停 detail 快照（原因可解释） |
| at | TIMESTAMPTZ | `NOT NULL DEFAULT now()` | 事件时刻 |

- `UNIQUE (chain_id, pause_id, revision, action)` —— 同一实例同修订的同类事件只记一次；
  重复解除同一实例（行已不在）影响 0 行在先，不会建第二条。
- 按 (chain_id, pause_id) 可追踪实例全生命周期（创建见暂停行/日志，合并与解除见本表）。

## 写事务协议（单元提交 / 首单元 / 暂停统一；锁先行、持锁后独立语句裁决）

与 002/003 协议同构，协调行复用 `indexer_lease`（R1）。设本单元 `[a, b]`（块粒度连续闭区间）。
消费在读取上游与 checkpoint 时同步捕获当时版本 seq（记为 $V）；提交时核对版本身份（见步骤 4），
哈希仅做内容一致性校验，不作版本隔离依据。

1. `BEGIN`（短事务；上游读取、缺口分类、逐条解析匹配已在事务外完成，事务内零外部调用；
   `SET LOCAL statement_timeout = '5s'`，沿用 `writeGuard`）。
2. `INSERT INTO indexer_lease … ON CONFLICT (chain_id) DO NOTHING` —— 幂等确保协调行存在。
3. `SELECT owner_id, fencing_token, expires_at > now() FROM indexer_lease WHERE chain_id=$c FOR UPDATE`
   —— 获取全链唯一协调锁，保持到 `COMMIT/ROLLBACK`。所有写事务
   （002 推进/暂停 + 003 提交/暂停 + 004 提交/暂停/授权）在此处串行化。
   授权事务同样取此锁，但跳过 owner/token 归属检查（特权路径：执行者是 DB 操作员而非 lease 持有者；
   操作员身份记入 history 审计行，安全性来自持锁串行 + 精确守卫 + 重验门，而非 lease 所有权）。
4. 后续**独立语句**（Read Committed 每条语句取新快照，可见锁等待期间提交的一切事务）重读并裁决，
   任一失败即 `ROLLBACK`：
   - `SELECT 1 FROM deposit_pause WHERE chain_id=$c` 必须无行；
   - `SELECT 1 FROM log_pause WHERE chain_id=$c` 必须无行；
   - `SELECT 1 FROM indexer_pause WHERE chain_id=$c` 必须无行（任一上游暂停阻止充值提交）；
   - lease 行 `owner_id=$me AND fencing_token=$tok AND expires_at > now()` 必须成立
     （旧 worker 在此被拒绝）；
   - 进度精确守卫（含完整性前检）：首单元要求 `deposit_checkpoint` 与 `deposit_config_history`
     同链均无行（恰其一有行 → 损坏态拒绝，禁补建）；推进要求两表同链同有、
     checkpoint 行内 `(start_block, config_hash)` 与最新 history 行一致（否则损坏态拒绝，
     禁补建、禁改回、禁授权修复），且 `SELECT 1 FROM deposit_checkpoint
     WHERE chain_id=$c AND start_block=$S AND config_hash=$H AND next_block=$a`
     （恰好连续 + 配置一致；行数 0 → 过期或配置变化，拒绝）；
   - 版本隔离守卫：捕获版本 $V 必须仍等于当前最新 `version_seq`
     （`SELECT MAX(version_seq) FROM deposit_config_history WHERE chain_id=$c`）；
     不一致 → 旧版本在途处理，放弃本次结果并重读重算（H1 回环下哈希相同亦然；禁把旧结果打上最新 seq 提交）；
   - 覆盖重证明：重读 `log_checkpoint` 仍有 `next_block > $b`（R2；上游行消失即按链视图异常拒绝）；
   - 链视图重裁决：`[a, b]` 内每一高度在 `chain_blocks` 中存在、canonical 且哈希与来源行一致
     （重读行数必须等于区间长度；缺一即回滚走 `chain_view_changed` 暂停）。此即"检查→提交无窗口"的保证：
     裁决读发生在持锁之后。
5. 执行写入：
   - 对本单元每条匹配来源逐行
     `INSERT INTO deposit_observations … ON CONFLICT (chain_id, block_hash, tx_hash, log_index) DO NOTHING`，
     新行 `version_seq` 取捕获版本 $V（实际处理依据的版本；禁取提交时刻最新 seq 冒充）；
   - 冲突内容比对：对本单元每一来源身份重读已存行
     `(contract, block_number, sender, recipient, amount, status)` 逐字段比较（`version_seq` 不参与比对；
     回放保留已有观察原值，不重写）；任一不一致 →
     `ROLLBACK`，随后另起 `validation_failed(detail.class=identity_conflict)` 暂停事务。
     内存中的"我刚算出什么"永不作为正确性依据；
   - 行数核对：本单元去重后来源身份数必须等于
     实际插入 + 已存在一致行数 + 合法零生成数（零值/非匹配/低于生效高度），防止静默丢行；
   - `UPDATE deposit_checkpoint SET next_block=$b+1, updated_at=now()
     WHERE chain_id=$c AND start_block=$S AND config_hash=$H AND next_block=$a`
     （首单元为 `INSERT (chain_id, start_block=$a, config_hash=$H, next_block=$b+1)`），
     `RowsAffected != 1` → 过期，拒绝；
   - `COMMIT`。影响 0 行 / 裁决失败 = 前提不成立，调用方必须停推并重读状态，不得按成功处理。
- 暂停事务（`upstream_gap(structural)` / `chain_view_changed` / `validation_failed`）：
  同样先锁协调行 → 按 Table 3 原子条件复核 → `INSERT INTO deposit_pause … ON CONFLICT (chain_id) DO NOTHING`
  → `COMMIT`。与推进事务互斥：暂停提交后获锁的推进事务必见暂停行而被拒绝。
- 不确定提交恢复：重连后 `SELECT deposit_checkpoint` + 重走步骤 4，以数据库为准、幂等继续（FR-09）。

## 回放位置计算（Q5/Q6 规则，事务外计算、事务内复核）

设当前持久化状态为 `(start_block=$S, config_hash=$H, next_block=$a)`，新配置经解析规范化后：

1. 对每个需补充识别的资产—地址组合（新增资产/地址、生效高度提前、全局起点降低波及的组合）
   计算有效起点 = max（全局起点，资产生效高度，地址生效高度）。
2. replay_from = min（当前 next_block $a，各受影响组合有效起点，未解决且适用的缺口起点）。
3. 无补充识别需求（纯收缩：仅删除条目、推迟高度、提高起点）→ replay_from = $a，不前移不跳位。
4. 上游起点只做可行性检查：replay_from < 上游 `start_block`，或所需资产历史无覆盖
   → 判结构性缺口（授权事务拒绝，调用方走结构缺口路径）；**禁止**用上游起点抬高 replay_from 裁掉缺失历史。
5. 收缩边界 = 授权切换前的 next_block（=$a）：即使同次变更因新增条目回放更早，收缩规则不追溯适用；
   已有观察全部保留（I1：回放不新增、不重置），后续确认与链上失效走对应阶段规则。
   嵌套回放中边界之后尚无观察的来源可受后续收缩影响（补充识别义务随重算更新，非永久冻结），
   但已生成观察永不因此删除或重置。
6. 授权事务内复核 replay_from 的计算输入（当前 (S,H,a) 未变 + history 快照一致），任一变化即 `ROLLBACK`
   按过期授权处理（调用方基于最新状态重算后可重试）。

## 授权转换事务协议（锁先行、重验门、原子提交）

1. 事务外：先做完整性分流，再按 (chain_id, request_id) 查 history 定性（禁仅比目标哈希；禁先重算派生值再比对）：
   完整性：checkpoint 与 history 恰其一有行，或两表皆有但 checkpoint 行内 `(start_block, config_hash)`
   与最新 history 行不一致 → 损坏态，禁补建、禁改回、禁授权修复；但该损坏不拦截只读返回——下述
   "命中且意图一致"的已记录结果仍按原样返回（不开事务，不重新执行），新授权执行一律拒绝。
   命中且调用方意图参数（request_id、expected_old_seq、配置快照：资产／地址／高度／起点→H′、
   operator、reason、expected_pause 目标二列）一致
   → 同一次请求已提交，返回已记录结果（确立的 version_seq、replay_from），不重新执行，
   不开事务（即使当前已进入更晚版本，不因暂停变化重验目标）；
   命中但意图参数任一不同 → 同 ID 异参（已记录），明确拒绝，不产生状态变化，不开事务；
   未命中 → 独立候选请求继续下述流程（不同 request_id 即使意图相同也不合并；2026-09-13 批准修订：
   绑定仅由持久化记录确立——明确失败全回滚无记录时 ID 未绑定，同 ID 重用含补目标按独立候选完整重验，
   不再强制新 request_id；成功绑定后同 ID 改目标仍拒绝）。
   当前无版本（checkpoint 与 history 均无行）→ 直接拒绝"无源版本可转换"，不开事务；
   首行只能由首单元提交建立，不由授权操作创建。
   目标缺席规则：expected_pause 双空表示不授权处置任何已有暂停（非通配），但不等于一律拒绝：
   必须处置 ⟺ 暂停行存在，且其证据属于本次授权解决范围并经本次重验证明已解决
   （结构缺口已补齐／校验失败已修复；"希望恢复消费"本身不是处置理由）。
   此时无显式目标 → 拒绝并要求调用方读取实例后重新明确授权（2026-09-13 批准修订：
   拒绝未持久化任何记录，ID 未绑定；同 ID 补目标重用按独立候选请求完整重验执行，不再强制新 request_id；
   成功绑定后同 ID 改目标仍拒绝）。
   可保留 ⟺ 暂停行存在，但其证据不属于本次解决范围（无关种类／区间、needs_006；
   上游暂停行存在本就导致授权拒绝，不进入本分支），或根本无暂停行。
   此时允许继续：原子提交身份 H→H′、位置 next→replay_from 与 history 行
   （expected_pause 记 NULL），暂停实例、修订、内容及解除要求均不变；
   消费因暂停行仍在而保持停止。保留不改变暂停归属：后续解除走针对该实例的人工路径
   （Table 3a）＋当前版本重验门，与暂停打标版本无关，故保留不产生无法解除的死路。
   锁内一致性：步骤 3 须确认暂停状态（无行／有行实例＋修订）与本决策依据一致；
   不一致（新增、替换、修订变化、消失）→ 回滚并报告状态变化，调用方重读后重试
   （同 request_id 重试——同参或补目标——仍按"未命中"走独立候选请求，因前次未记录；2026-09-13 批准修订，与上绑定规则一致），
   不得自动改按处置或保留执行，不扩大调用方授权。
   独立请求继续：解析新配置并规范化 → 计算 H′ 与 replay_from（上节规则）→ 读当前 (S,H,a) 与当前最新 seq、
   暂停行、上游 (S_u,H_u,N_u) → 缺口分类 → 对回放区间做覆盖预检（N_u 证明）与 canonical 预检；
   要求 expected_old_seq 等于当前最新 seq（否则过期拒绝），H′ 与当前不同（否则空授权拒绝）。
2. `BEGIN`（短事务；`SET LOCAL statement_timeout = '5s'`）→ 确保 lease 行 → `FOR UPDATE` 取协调锁
   （跳过 owner/token 检查，见上；操作员身份来自会话 + history 审计行）。
3. 持锁后独立语句重验（任一失败即 `ROLLBACK`，保持原身份/位置/暂停）：
   - 完整性前检：checkpoint 与 history 同链同有（均无行已在步骤 1 判无源版本拒绝；
     恰其一有行 → 损坏态拒绝），且 checkpoint 行内 `(start_block, config_hash)` 与最新 history 行一致，
     否则同判损坏态；损坏态禁补建、禁改回、禁授权修复（只读返回不受影响，见步骤 1）；
   - 当前 `(start_block, config_hash, next_block)` 仍为授权依据的 (S,H,a)，且当前最新 version_seq
     仍等于 expected_old_seq（任一已变 → 过期，拒绝；哈希仅做内容一致性校验，版本隔离以 seq 为准）；
   - H′ 快照（assets/watches/高度/起点）与本次 env 解析一致；
   - 覆盖重证明：N_u ≥ 当前 next（上游未丢失覆盖）且回放区间 `[replay_from, a-1]`（非空时）
     逐块 canonical 与来源行一致（重读行数必须等于区间长度）；
   - 暂停处置：先按锁内最新状态重推步骤 1 的必须／保留结论，与依据不一致 → 回滚并报告状态变化
     （不自动切换分支，不扩大授权）；`deposit_pause` 无行 → 直接继续；有行 →
     其 (pause_id, revision) 必须与授权依据读取时一致（已变 → 状态变化，拒绝重估），
     且：调用方提供了 expected_pause，须与该行一致（否则按目标替换拒绝，不自动改处置新暂停）；
     调用方双空 ＋ 步骤 1 判必须处置 → 缺目标拒绝；调用方双空 ＋ 步骤 1 判可保留 →
     原样保留，不读写暂停行；证据属于本次解决范围并已证解决 ＋ 有显式匹配目标 →
     按实例 + 修订条件 DELETE（审计行同事务）。
     `log_pause` / `indexer_pause` 有行 → 拒绝（上游暂停未解除）；
     证据指向需 006 回退 → 拒绝并保持暂停。
     授权事务禁 UPDATE／合并暂停行：只允许原样保留（不动行）或按实例＋修订条件 DELETE 已解决行
     （审计行同事务），不得通过修改实例、修订或归属变相绕过解除授权。
   - H′ 与 H 相同 → 拒绝为空授权（身份无变化不得建 history 行）。
4. 执行写入（原子提交）：
   - 取新 seq = 同链 `MAX(version_seq)+1`（持锁后重读，首版本为 1）；
   - `INSERT INTO deposit_config_history (chain_id, version_seq, config_hash=H′, prev_seq, start_block,
     assets, watches, replay_from, operator, reason, request_id,
     expected_pause_id, expected_pause_revision, …)`（审计字段齐全，缺任一即 `ROLLBACK`；
     相同内容复现即新行新 seq，禁与旧行合并；
     并发重复请求在此以 `UNIQUE (chain_id, request_id)` 冲突失败 → 回滚后走步骤 1 重查，命中即返原结果）；
   - `UPDATE deposit_checkpoint SET start_block=S_new, config_hash=H′, next_block=replay_from, updated_at=now()
     WHERE chain_id=$c AND start_block=$S AND config_hash=$H AND next_block=$a`
     （S_new 为新配置全局起点，起点列随授权与身份原子更新；首版及历次起点由 Table 4 不可变保留；
     next_block 仍按回放 min/收缩规则，不跳过区间），
     `RowsAffected != 1` → 过期，拒绝；
   - 暂停处置：按步骤 3 结论执行——已解决行按实例 + 修订条件 `DELETE`（审计行同事务），
     或原样保留（不读写暂停行；history 行 expected_pause 记 NULL，
     保留事实由"新 history 行＋仍存在的暂停行"共同表达，不另建审计行）；
     未解决的保持不动（此时步骤 3 早已拒绝，本条不执行）；
     本路径禁 UPDATE／合并（合并更新专属暂停写事务，见 Table 3）；
   - `COMMIT`。提交成功即生效，之后一切裁决按 H′ 执行；切换前已提交的行有效（历史保留）。
5. 不确定授权恢复：重连后按 request_id 查 history（跨版本也命中原行）：
   命中且全参数一致 → 同一次请求已提交，返回已记录结果（确立的 version_seq、replay_from），不重新执行；
   命中但参数不同 → 同 ID 异参，明确拒绝；
   未命中 → 重试授权（重算后；当前若已非预期旧版本则按过期拒绝，并报告预期与当前版本）。
6. 多次切换嵌套：每次授权从当前持久化状态重算（本节步骤 1），replay 单调不变量为
   "只回放不跳过"（replay_from ≤ 当前 next 恒成立，因为 min() 含当前 next）；
   暂停行按版本打标合并（由暂停写事务执行，授权事务禁用此写法）；history 链线性可审计。无需"回放完成前禁止新授权"的业务限制。

## 失败分类（恢复动作对照）

| 信号 | 分类 | 动作 |
|------|------|------|
| DB 瞬时错误（连接中断/超时/序列化失败） | 请求失败 | 有界退避重试（复用 INDEX 退避参数），不推进 |
| 所需位置暂时无上游覆盖（`p >= N_u`，范围内） | 暂时性缺口 | 等待（state=1/4），不建暂停行，覆盖完整后自动继续 |
| 所需历史低于上游起点 / 资产不在上游覆盖 | 结构性缺口 | 报错停止 + `upstream_gap(structural)` 暂停行；人工修复后重验完整，从原缺口幂等恢复 |
| 上游暂停行存在 | 服从暂停 | 不提交不推进；上游解除且重验通过后自动继续 |
| 单条越界/畸形/缺字段/失效 | 整批失败 | 不提交不推进；重查仍确定性失败 → `validation_failed` 暂停 |
| 相同身份不同内容 | 冲突 | 整批失败 + `validation_failed(detail.class=identity_conflict)` 暂停 |
| 链视图变化 | 暂停 | `chain_view_changed` 暂停，不提交 |
| 授权事务前置失败 / 事务失败 / 并发 request_id 冲突 | 授权失败 | 全回滚，保持原身份/位置/暂停；冲突方回滚后走步骤 1 重查，命中即返原结果 |
| 授权身份/参数不符（过期、空授权、同ID异参） | 授权拒绝 | 不产生状态变化；过期拒绝报告预期与当前版本；同 ID 异参（已记录）明确拒绝 |
| 未知错误 | 失败 | 按失败处理，不推进，不猜测 |

## 一致性不变量 ↔ 约束对照

- I1（来源身份最多一条观察，含重处理、恢复与收缩回放后）：PK；回放不新增、不重置；版本关联显式存行内 seq。
- I2（进度对应完整已处理结果）：单元事务原子性 + 行数核对 + 精确守卫（`next_block=$a`）；跨版本时 history 链（prev_seq→version_seq 线性）为版本账本，授权事务原子衔接；无外键但无"进度已进而观察缺失"状态。
- I3（连续无缺口、不跳位）：`next_block=$a` 精确守卫 + 覆盖重证明；"不跳位"即进度只能落在连续已处理序列末端；回放只回放不跳过（replay_from ≤ next 恒成立），收缩不前移。
- I4（引用块与 003 canonical 一致）：步骤 4 链视图重裁决逐块验证；`block_hash` 存文本但以 `chain_blocks` 重读为准。
- I5（空白名单零识别零写入）：启动期拒绝 + 匹配构造断言（空集合即错，不读上游）。
- I6（暂停/缺口有效期间推进为 0；缺口补齐前无"无充值"结论）：三暂停行裁决 + 持锁后新快照 + 覆盖重证明；测试硬断言。

## 观察 ↔ 日志 ↔ 区块身份关联与 canonical 读取（审计保留）

- 每行观察携带 `(block_number, block_hash)` 双字段：`block_number` 用于区间归属与排序消费，
  `block_hash` 是与链视图绑定的身份部分（PK 成员）。两者在写入时必须与上游来源行及 `chain_blocks` 同行一致，
  事后永不改写。
- canonical 读取范式（与 003 统一，均为 `canonical = TRUE` 点查）：
  `SELECT hash FROM chain_blocks WHERE chain_id=$c AND number=$n AND canonical`。
  用途：①覆盖核验的逐块绑定（事务外）；②提交裁决重读（事务内持锁后）。
- 004 永不改写已存观察行的身份字段，永不删除历史行：若后续发生重组，旧分叉观察仍以原哈希保留，
  作为 006 恢复的审计依据；新分叉来源以新 `block_hash` 落为不同 PK 行，由 006 规范决定取舍。
  本阶段只保证"引用块在提交瞬间为 canonical"，不追踪之后的变化。

## 配置身份编码（R3 落实，含测试向量）

- 输入（UTF-8，无 BOM，末尾无换行）：`deposit:v1\n` + `start:<S>\n` +
  `asset:<contract>:<effective>\n`（字典序）+ `watch:<address>:<effective>\n`（字典序）。
- 标准向量：`deposit:v1\nstart:0\nasset:0x1111…1111:0\nwatch:0xaaaa…aaaa:0` →
  `31822b65a6444c91bdaaa04a86582f4db25f35d5dd8ee02b7c2c12a4cf6600f0`。
  实现必须复现该向量；大小写/顺序/重复变体收敛同一向量，增删条目或改高度则变化。

## 索引

- PK 之外：`CREATE INDEX ON deposit_observations (chain_id, block_number)` —— 区间回查与 005 按高度消费
  的主访问模式（005 消费已在规格目标中明确为后续输入，非投机）。
- `CREATE INDEX ON deposit_observations (chain_id, recipient, block_number)` —— 按监控地址的运维查询
  （"某地址的充值"），为规格可观测目标直接需要，非投机。
- 其余访问（checkpoint/pause 单行点查、冲突比对点查）全部被 PK 覆盖，不建二级索引。
- `deposit_config_history` 无二级索引：版本链按 `PK (chain_id, version_seq)` 排序扫描 + `prev_seq` 链式追踪，
  单链版本数极小，不建索引。
