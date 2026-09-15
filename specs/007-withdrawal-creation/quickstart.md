# Quickstart: 007 Withdrawal Creation & Query (validation guide, design only)

**Branch**: `007-withdrawal-creation` | **Date**: 2026-09-15

Design-only validation guide. Nothing here is executed in this step; scenarios become
integration/E2E tests in tasks/implementation. Environment: Anvil (chain-31337) + real
PostgreSQL via `make test-integration` (Docker-provisioned; compose stack is a manual-debug
aid only, per 006 plan precedent).

Prerequisites: migration `000007` applied; one `caller` row + one active `api_key`
(operator tooling, research R5); one `withdrawal_authorizations` grant row bound to the
same params; deployment `chain_id` = Anvil chain.

## V1 — happy path + self query
POST valid body → expect 201 + `request_id`, `status: accepted`; GET with same key →
same body, `recovery: {state: none, execution: not_started}`.

## V2 — auth matrix
No header / wrong key / revoked key → 401; key without `can_create` → 403; body with forged
`caller_id` differing from key identity → ignored (ownership from key), 201 under key identity.

## V3 — grant matrix
Missing/inactive/revoked/mismatched grant → 403, zero rows in `withdrawal_requests`;
grant reuse across a second key → 403-path (T-auth-bound), first row untouched.
Supply entry: `withdrawal-authz mint` (capture O first; no O ⇒ no DB effects), then
`withdrawal-authz supply --operation-id O …` (O REQUIRED, no auto-mint). Equal re-supply →
`resupplied`, grant `RowsAffected()==0`; same-O retry compares op-input first —
equal ⇒ recorded outcome (incl. refusal), differ ⇒ `operation_conflict`;
A-then-B异参 (new O each) → TWO `supply_refused` rows. Concurrent first-supplies:
same-O same-op-input ⇒ ONE grant + ONE `supplied` (loser restarts into resupply, same O);
different-O same-op-input ⇒ ONE grant + per-O audits; different-O异参 ⇒ winner `supplied`,
loser `supply_refused` (never `operation_conflict`, never 503).
`withdrawal-authz revoke --operation-id P …`
(active → `revoked`; repeat → distinct `revoke_nop` rows). Uncertain COMMIT → same-O retry only
(O-miss ⇒ unknown/retryable with same O; grant-present + attempt-refused ⇒ refusal, never success).
Revocation interleaved with first receipt: revoke-committed-before-grant-lock → 403 zero rows;
revoke-blocked-on-grant-lock (receipt first) → receipt stands, revoke applies after COMMIT,
subsequent replays 200. Assert the three-case table from research R7.

## V4 — param matrix
Wrong chain / non-whitelist asset / bad address shape / mixed-case failing EIP-55 /
`"0"`, `"00123"`, `"-5"`, `"1.5"`, non-digits, > uint256 → 400/422 per contract, zero rows.
Mixed-case passing EIP-55 → accepted, stored lowercase; re-POST same key → 200 same row.

## V5 — idempotency matrix
Same key + equal params → 200 same `request_id`, row count unchanged; same key + any param or
grant differ → 409, original unchanged; different caller + same key string → independent 201.

## V6 — concurrency & crash
N-way parallel same-key POSTs → exactly 1 row, all callers see the same `request_id`;
parallel same-grant different-key POSTs → exactly 1 row, losers 403-path;
dual-constraint race (same key + bound-elsewhere grant in one INSERT) → response follows
fixed-order classify (key-hit equality → 200; key-hit inequality → 409), never report order;
kill -9 between COMMIT and response → retry same key → 200 original;
restart → retry → 200 original; storage down → 503, zero Accepted;
uncertain COMMIT → re-classify (hit → 200/409/403 per order; miss → retryable, never "not created");
lock-wait timeout → 503 retryable; deadlock → 503 retryable (assert no silent mapping to 23505).

## V7 — rotation & revocation
Rotate key (same `caller_id`) → old key 401 (or dual-accept inside grace), new key works,
old rows still queryable, idempotency scope unchanged; revoke grant → new creates 403,
existing Accepted rows unaffected, replays still 200.

## V8 — recovery period
With an active 006 recovery row: compliant POST → persisted, zero nonce/sign/broadcast
artefacts (assert via absence: no new tables/rows outside 007 scope, no RPC broadcast);
GET → facts servable with `recovery.state` from one REPEATABLE READ snapshot (row + terminal
event read inside it; kill either statement → `state: unknown`, same body, still 200;
release-then-re-establish cannot land inside the snapshot — assert by concurrent establish
during a held-open read tx in test: response still reflects exactly one snapshot, never a
forged `released`/`none`);
direct unit assertion that no recovery governance rows were written or deleted by 007 paths.
POST-then-lost-response during recovery → same-key retry → 200 with identical `request_id`;
recovery read failure never alters replay outcome.

## V9 — cross-caller privacy
Caller B GET caller A's `request_id` → 404 byte-identical shape to random-id 404.

## §操作 runbook（T032；合成数据演示，非真实凭据）

本节每一步均有实现/测试证据，不引入新语义。退出码全命令统一：0 成功、
1 拒绝/失败（stderr 已脱敏）、2 用法错误。用 serve 同一份 env 文件运行
（`config.Load` 全量校验）。

信任边界（沿用 005 F-R2，如实声明，非新增机制）：当前模型以写 DSN 的访问权
作为数据库操作信任边界；`--operator` 为调用方声明的审计标签，不是已认证身份；
`request_id`/`operation_id` 用于关联与幂等定性，不能单独证明实际操作者身份。
密钥明文只在签发/轮换的 stdout 成功行出现一次，永不进日志、仓库或错误响应。

0. 启动与策略供给：`migrate up` 到 `000007`；serve 绑定部署链（FR-04）。
   FR-05 白名单读每链最新 `deposit_config_history` 行（004 快照 `assets` 列）。
   无策略行时：serve 照常启动；新键首次创建 503 可重试（同键等策略落地后重试）；
   已接收请求重放不受影响（永久重放不变量）。不要手插空策略或默认全链。

1. 调用方密钥（`apikey-auth`；证据 T007/T008/T018）：
   `txharbor apikey-auth issue --caller-id 7001 --label demo --operator op --reason r`
   → stdout `issued caller_id=7001 key_id=K prefix=… key=明文`（保存明文，仅此一次）。
   `… rotate --caller-id 7001 [--grace-seconds S] …` → 新行 `rotated … key=新明文`，
   同一 `caller_id` 保持，历史查询与幂等域不变；宽限内旧键双接受，显式吊销即刻终止
   （`revoke --key-id K …` → `revoked …`）。吊销后该键请求 401，零审计行。

2. 逐笔授权（`withdrawal-authz`；证据 T008/T023）：先 `mint` 取不透明 O
   （纯熵，未持久化；无 O 不碰库）， durable 保存 O 后再用：
   `supply --operation-id O --authorization-id G --caller-id 7001 --chain-id 31337
   --asset 0x1111… --recipient 0xaaaa… --amount 100 --operator op --reason r`
   → `ok authorization_id=G action=supplied|resupplied|supply_refused`。
   等参同 O 重放返记录结果；同 O 异参 → `operation_conflict`，原行不动；
   异 O 同参并发 → 一 grant + 逐 O 审计（败方 `supply_refused`，永不 503）。
   `revoke --operation-id P --authorization-id G …`（active→`revoked`；
   重复→互异 `revoke_nop` 行）。COMMIT 不确定时只许同 O 重试：O 未绑定→未知/
   可重试；grant 在 + 本次被拒→拒因，永不成功。

3. 创建/查询/重放（证据 T012–T029）：`POST /withdrawals`（Bearer 密钥）
   → 201 全请求体；同键同参 → 200 原 `request_id`（仅重估当前认证与接口权限；
   策略删除/缺失/不可读不阻断重放）；同键异参 → 409 原行不动；
   异 caller 同键串 → 各自 201。401 零审计行；422/403/409 按表写单行
   `rejected`/`conflict` 意图（响应先写后落库，response-first）；
   503（存储/策略源故障）→ 同键同参重试，永不换键，永不宣称"肯定没创建"。
   `GET /withdrawals/{id}` 鉴权归属读，他人/不存在 → 同形 404；
   `recovery: {state, execution: not_started}`，007 零执行副作用。

4. 恢复/错误/指标边界：恢复期 POST 照常持久化且无 nonce/签名/广播；
   GET 快照单点读，任一失败只置 `state: unknown`。指标
   `txharbor_withdrawal_{accepted,replayed,conflict,rejected,unavailable,unauthenticated}_total`
   每创建尝试 exactly-once 归一（证据 T015）。真 SIGKILL/40P01 注入证据见实现；
   原生 SQL 40P01 探针仅证错误类别存在。

5. V1–V9 执行记录（本机 `make test test-race test-integration` 全绿）：
   单元 733/11 包；race 全仓 733/11 包；集成 withdrawal 313、app 147（含重放×策略/
   认证丢失回归）、db 88、health 5、indexer 580。V1 T012/T015、V2 T016–T018、
   V3 T009、V4 T019/T020、V5 T021–T023、V6 T024–T026（含真 kill-9 双终止点、
   应用路径 40P01）、V7 T018/T025、V8 T027–T029、V9 T017。requirements 保持
   21/22 真实状态；T000-P 独立 open；008–011 只记边界，未实现。
