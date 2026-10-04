# ADR-004: Selective Recovery Admission While Original Writers Remain Excluded

**Status:** Proposed
**Baseline:** HEAD `e504462`
**Scope:** 015 backup-recovery chain; design prerequisite only. No implementation lane, permission expansion, or production-readiness claim is authorized.

## Context

**Sequencing remains BLOCKED because authenticated native admission and required writer exclusion do not currently coexist:**

- `internal/recovery/borrowed-replacement-bound-writer-isolation_linux_test.go:278–288` prepends original writer-role rejection across enumerated ingress scopes; `:686–713` requires genuine P1 rejection before and after `duringProof`; `:723–730` rechecks effective fence coverage. Missing condition: a server-enforced recovery admission path that preserves these refusals.
- `internal/recovery/borrowed-gate-prefix_linux_test.go:160–171,183–193` preserves the original role/database, attributes the authentic child, and starts its gate session. `internal/recovery/origin_gate_proxy_linux_test.go:1138–1153` connects that session upstream to the protected server. **Origin attribution is not an exemption from server-side rejection.**
- `internal/recovery/borrowed-replacement-bound-postcommit-native-start_linux_test.go:15–43` proves child start against a reader-only endpoint, with authentication withheld. Missing condition: an authenticated server backend executing authorized recovery while the original writer remains excluded.
- `internal/recovery/drill_target_witness_test.go:248–256` still refuses rebuilding without authentic original interrupted-writer identity/process proof. Missing condition: retained original-attempt proof, not a newly manufactured witness.

Closed-chain references: writer-isolation `91f4dd5a`; baseline-commit `6034416e` (`completed_restoration=false`); postcommit-admission `07781388`; postcommit-native-start `321a48d9`; targetlock seam `3bea2e24` and companion `b68e3a2e`; witness `8d0a26a1`; guard-row `264cbc81`; journal `ee880415`. These are bounded evidence references, not admission authority.

## Decision / Proposal

### 1. Admission object, identity, and the trusted sources of authorization — authenticated MUST NOT equal authorized-to-restore

Propose a **single-attempt, controller-owned recovery admission object**, distinct from authentication credentials, readiness facts, process receipts, and service-resumption approvals.

Its immutable binding includes:

- Authentic recovery instance, original operation, canonical target key, original writer-role fingerprint/OID, and retained control-owner incarnation.
- Captured cluster incarnation and independently verified replacement database OID.
- Recovery execution role/OID, specific recovery operation, scope, archive descriptor/digest, sealed tools, and exact gate endpoint.
- Current evidence generation/hash, acknowledged preparation commit, live exclusion interval, and subsequently observed child and backend incarnations.

The original writer identity remains unchanged. The recovery execution identity is recorded **separately**, never substituted into original capture or witness fields.

**Authorization source and minting:** the existing trusted recovery controller, acting for an authenticated, registered executor with existing recovery-execution permission, may mint the object only after applicable entry/prelaunch checks, authentic original-writer proof, registered-P1 retirement, lock/guard checks, and live server exclusion succeed. Readiness is consumed only as lineage, as established by `borrowed-replacement-bound-reentry-readiness_linux_test.go:20–29`; it cannot mint authority by itself.

The object is an opaque controller-owned handle, not a caller-supplied token. Copies share one-use consumption and permanent invalidation. Durable audit references describe its binding and lifecycle; reconstructing a handle from those references is forbidden.

**Proposed backend authentication boundary:** use a distinct, restricted recovery database role through a protected local peer-authenticated server connection owned by the gate. The server maps a real, deployment-controlled gate OS identity to that role. The native child names the separately bound recovery role; no fabricated identity or bearer token is introduced. This changes existing same-role admission and requires the separate rulings below.

Verification occurs before dispatch, before upstream connection, after real backend registration, before executable-frame release, and before evidence acceptance. Missing authorization, wrong identity, replay, lost owner/prefix, expired context, or unacknowledged commit refuses. A successful authentication or visible audit row MUST NOT substitute for those checks.

### 2. How recovery tooling obtains authorized connection, how old writers are terminated and blocked from reconnecting — ordinary CLI claims MUST NOT count as proof

Only the existing sealed tool factory/supervisor receives the admission handle and bound endpoint. It launches genuine native tooling in the supported dedicated Linux process group. The gate attributes the frontend using observed PID/start identity and owned socket evidence before opening upstream.

The proposed server path admits the recovery role only through peer authentication of the protected gate OS identity. Other OS identities cannot authenticate as that role; TCP ingress rejects it. The gate identity must be exclusive to the trusted controller boundary, not shared with ordinary CLI users or old writers. Socket access, peer mapping, role privileges, and alternative routes must be verified deployment facts. Loopback source address alone is insufficient.

After authentication, independently register the actual backend's PID/start, OS incarnation, socket association, role/database OIDs, and cluster incarnation. Retain the first executable frame until registration, watcher readiness, live authorization, and exclusion checks pass. `application_name`, SQL shape, CLI flags, and claimed PIDs provide no authority.

**Old-writer termination requires all three:**

1. The authentic original supervisor proves disappearance of the entire original process group, with retained process identities.
2. Its sole `Wait` actually completes and produces the authentic terminal observation/process receipt. Process exit or frontend EOF is insufficient.
3. Independent protected observation proves every retained backend incarnation gone, with complete writer census and no ambiguous OS/socket evidence.

Registered P1 receives its existing single-use/strict retirement treatment (`borrowed-replacement-session-registration_linux_test.go:3–19`). Original interrupted-attempt proof remains independently mandatory.

**Reconnect refusal is enforced by PostgreSQL:** retain the original writer-role HBA reject fence across every supported ingress. Genuine P0/P1 attempts must receive the intended server admission refusal, including around `duringProof`. Password rotation alone, a failed route, timeout, or client assertion does not prove exclusion. Existing sessions must be drained separately: HBA rejection does not terminate them.

No allow rule for the original writer role is proposed. Any route, role membership, credential, or administrative path that permits an excluded writer to reach recovery write authority blocks admission.

### 3. Protection timeline from first target write through probe to evidence acceptance — covering concurrent recovery attempts, lock loss, tooling interruption, and proof invalidation

Protection begins **before any recovery target mutation**, including destructive replacement DDL. A fence established after replacement creation cannot retroactively prove protection of those earlier writes.

| Phase | Guard | Durable record | Fail-closed behavior |
|---|---|---|---|
| Establish prerequisites | Authentic original capture/entry, executor authorization, same canonical advisory-lock domain, present guard row with matching provenance, original-process termination proof, server fence and complete census. | Existing guard and append-only audit reference the attempt and observed prerequisites. | Missing original proof, absent row, ambiguous identity or route coverage: no target mutation. |
| Controlled preparation/rebuild | Retained owner/anchor; exact guard-row transaction window; live server exclusion. Replacement must be independently captured and verified. | Acknowledged preparation/rebuild transaction and truthful audit. Preparation remains `completed_restoration=false`. | Missing/uncertain commit acknowledgment yields no admission. Preserve committed history; do not infer permission from independently visible rows. |
| Native preparation and launch | One-use admission, entry/prelaunch gates, live source rechecks, sealed archive/tools and endpoint, server fence still effective. | Genuine counted `restore_started` marker and generation invalidation, guard preparation, and launch intent commit before execution. | Marker/endpoint failure launches nothing. After launch attempt, ambiguous outcome blocks re-entry; no synthetic receipt or "restored" result. |
| Authenticate and execute | Attributed child; protected recovery-role route; actual backend registration; first-frame barrier; owner, prefix, census and exclusion monitoring. | Launch/process identity and credential-free backend registration references, linked to durable attempt intent. | Unregistered backend, unexpected connection, proof loss or cancellation latches admission, stops further frames, and triggers supported group termination/drain. |
| Drain and probe | Process-group disappearance, sole-Wait completion, registered backend incarnation gone, no unauthorized writer, target binding and exclusion still valid. Probe uses a separately registered, bound read-only observer. | Authentic terminal receipt and scoped probe results; guard remains unresolved pending acceptance. | Nonzero exit, pending Wait, incomplete census, failed probe or observer loss prevents acceptance. Quiescence alone never establishes clean disposition. |
| Evidence acceptance | Same target lock; exact guard/instance row checks; current generation/hash; live exclusion and shared-loss arbitration through transaction completion. | Existing atomic evidence/generation/guard-clean transaction, with truthful completion audit. | Any stale token, protection failure, transaction failure or acknowledgment uncertainty emits no positive acceptance. Existing committed rows are retained, not deleted or relabelled. |
| Finish | Recovery admission is consumed; all associated sessions are drained; excluded writers remain blocked. | Append-only terminal classification and verified cleanup references. | No automatic original-writer reopening or service release. Lost cleanup proof blocks positive continuation. |

Reuse the synchronous `duringProof` hook for bounded local composition: authentication, execution, drain, probes, and acceptance must remain inside the held protection interval. Returning from the hook and publishing historical isolation facts does not provide continuing exclusion.

The guard-row window protects the actual present row only while held (`borrowed-replacement-guard-row-window_linux_test.go:9–29`). Continuous serialization comes from the target advisory owner plus durable unresolved disposition—not from an earlier row-lock sample. Concurrent recovery attempts cannot mint or dispatch a second admission for the same target; missing rows never imply clean.

Lock loss, tooling interruption, observer failure, or shared-prefix invalidation permanently invalidates the attempt and all handle copies. Handled failures terminate the process group and await authentic completion. Parent SIGKILL leaves durable intent/guard blocking; successors cannot reconstruct authority or automatically clear it. Controlled reconciliation requires fresh exclusion and original-attempt proof.

Append-only journal rows preserve actual outcomes. Existing refusal rows remain non-authorizing (`borrowed-replacement-owner-journal-window_linux_test.go:12–30`). If the control store is unavailable, acceptance and further dispatch stop; inability to journal is not permission to continue.

### 4. What a local isolated deployment CAN prove vs what production MUST still provide

**A local isolated deployment can prove**, after separately authorized implementation and genuine execution:

- Authentic fixture/capture lineage, one-use admission, attributed native child and real registered backend.
- Genuine original-writer rejection over the fixture's enumerated routes while restricted recovery executes.
- Supported process-group drain, sole-Wait observation, backend-incarnation disappearance, concurrency refusal, and protection-loss rejection.
- Actual restore/probe completion and atomic evidence acceptance within that demonstrated interval.

It cannot turn reader-only child-start evidence, preparation commits, readiness, journal existence, raw-RPC replay, or reference-ledger observations into completed restoration.

**Production must independently provide:**

- Protected executor identity/permissions, control-store integrity and ADR-001 rollback discipline.
- Real gate OS identity, peer mapping, restricted recovery-role privileges, protected server ingress, and exclusive ownership of that route.
- Complete deployment writer/credential/role-membership inventory, alias/topology handling, and enforceable exclusion across schedulers, administrative tools, publishers, consumers and unsupported execution paths.
- HBA/configuration change control throughout the protected interval. Periodic checks do not establish atomic exclusion against arbitrary concurrent privileged mutation.
- Visibility-complete independent observation, supported termination authority, and isolation from signing, broadcasting and real downstream effects.
- Existing business approvals and service-resumption hard gates after restoration.

Privileged administrators and hosts remain explicit trust assumptions. Extending protection against their compromise requires another security decision. Local fixture evidence proves no production guarantee; T000-P remains OPEN.

### 5. Positive/negative acceptance examples: authorized recovery executes; old-writer reconnect refused; unauthorized connections cannot write; protection failure MUST NOT accept success evidence

**A — Authorized recovery executes.** Given authentic original-attempt retirement, a held owner/guard, acknowledged preparation, current executor permission and live exclusion, the controller mints one admission. Genuine native tooling reaches the protected server as the distinct recovery role; its actual backend is registered before frames are released. Restore and probes complete, drain is proven, and evidence/guard acceptance commits atomically. Preparation history remains unchanged and non-restoration evidence. No service capability is implicitly released.

**B — Old-writer reconnect refused.** While A executes and again through acceptance, authentic original writer P0/P1 connections over Unix, loopback and server-IP routes receive genuine server-side rejection. No retained old backend remains. A wrong password or unreachable endpoint does not satisfy this example.

**C — Unauthorized connections cannot write.** An authenticated ordinary CLI, copied handle, unregistered child, stale operation, wrong database/role, or another OS identity cannot obtain recovery admission. Original-role ingress rejects; recovery-role ingress denies identities outside the protected gate route. A concurrent same-target attempt cannot launch. Expected results are no executable-frame release and no unauthorized target write.

**D — Protection failure MUST NOT accept success evidence.** Inject lock loss, admitting original-writer ingress, partial/invalid HBA coverage, retained writer, observer loss, pending sole-Wait, process-incarnation ambiguity, shared-prefix loss, interruption, generation drift, or commit-acknowledgment loss—including after probes and at acceptance. No positive restore acceptance or continuation is emitted. Post-launch failure retains blocking disposition; uncertainty is not recast as success, and historical committed records are not repaired.

## Consequences

The proposal separates restore authorization from authentication without weakening original-writer rejection. Existing capture, readiness, retirement, guard, journal, registration, marker and receipt mechanisms remain prerequisites—not substitutes for authorization.

It requires an explicitly reviewed distinction between original writer identity and recovery execution identity, and a protected server admission route not established by the closed lanes. Until the following rulings and a separate implementation authorization, sequencing remains BLOCKED and `rebuildTargetWithWitness` remains refusing.

Ruling update (2026-10-04): R1/R3/R4/R5 received limited design-direction rulings (recorded verbatim in §2.1 Ruling record); R2 (exact database grant inventory) remains unruled, and no implementation authorization exists. Sequencing therefore remains BLOCKED as above — the BLOCKED exit additionally requires every item ruled plus §4 acceptance evidence per the Checklist.

## Separate Decision Items Requiring Explicit User Ruling

1. **Execution-identity change:** approve or reject a distinct recovery role and separately bound execution identity. This does not approve overwriting original fingerprints or weakening existing comparator checks.
2. **Database permissions:** approve the exact target-scoped restoration privileges and archive ownership/role semantics. No implicit superuser, role-management authority, original-writer membership, or privilege escalation.
3. **Server/OS admission boundary:** approve protected local peer admission and its exclusive gate OS identity, configuration ownership and ingress restrictions. Existing TCP forwarding is not this enforcement boundary.
4. **Protection-window change:** approve establishing exclusion before the first recovery mutation and retaining it through acceptance, rather than treating closed preparation/teardown intervals as continuous protection.
5. **Acceptance integration:** approve the explicit instance-bound authorization lifecycle and conditional reuse of existing atomic acceptance. This does not promote non-authorizing journal/readiness records or permit recovery from copied diagnostics.

No new business-policy exception is proposed. Forced resumption, missing-proof waivers, data-loss approval and completed-restoration claims from preparation remain forbidden.

### 2.1 Ruling material and proposed approval sentences (excerpt-level references into the decision body; added 2026-10-04 handoff, does not itself change Status)

**Ruling record (2026-10-04, user decision — design direction only, not implementation authorization):** R1/R3/R4/R5 approved in the limited design direction recorded below; **R2 remains NOT ruled** (its exact grant inventory below is the pending material). This record changes no deployment configuration, grants no production privileges, and does not open an implementation lane.

- **R1 (approved, limited):** the existing trusted controller issues one-shot admission to an authenticated executor holding existing recovery-execution permission; authentication, readiness facts, and audit records never substitute for authorization.
- **R3 (approved, limited):** an exclusive deployment-controlled OS identity connects to the recovery role through the protected local peer path; other identities are refused and the role's TCP ingress is refused.
- **R4 (approved, limited):** protection is established before the first target mutation and held through evidence acceptance; pre-existing sessions must be drained separately; lock loss, interruption, or proof invalidation voids the attempt and accepts no success evidence.
- **R5 (approved, limited):** binding covers instance, target, operation, and evidence generation; acceptance is atomic inside the shared lock; stale or uncertain results are never positively accepted; existing service-resumption approvals keep executing independently.
- **R2 (pending):** none of the grant inventory below is approved; the exact SQL object-level list stays 待定 until generated from the pinned archive with evidence.

The five items below cite, for each approval sentence, the exact ADR body section it is drawn from. Ruling on an item approves that sentence's content and nothing beyond it; local acceptance and production prerequisites keep the item's BLOCKED exit gated on §4 plus the Acceptance Checklist row.

**R1 — Execution identity (item 1, from §1 and §5-A).** Authorizing subject: the existing trusted recovery controller acting for an authenticated, registered executor ("acting for an authenticated, registered executor with existing recovery-execution permission", §1) holding existing recovery-execution permission; the trusted sources of identity are the immutable binding fields ("Authentic recovery instance, original operation, canonical target key, original writer-role fingerprint/OID, and retained control-owner incarnation… Captured cluster incarnation and independently verified replacement database OID… Recovery execution role/OID, specific recovery operation, scope, archive descriptor/digest, sealed tools, and exact gate endpoint; Current evidence generation/hash, acknowledged preparation commit, live exclusion interval, and subsequently observed child and backend incarnations", §1). Allowed action and window: minting one opaque, one-use admission handle per attempt ("single-attempt, controller-owned recovery admission object", §1), consumable from mint through evidence acceptance within the live exclusion interval; verification fixed at the five points of §1 ("before dispatch, before upstream connection, after real backend registration, before executable-frame release, and before evidence acceptance"). Refusal conditions: missing authorization, wrong identity, replay, lost owner/prefix, expired context, or unacknowledged commit ("Missing authorization, wrong identity, replay, lost owner/prefix, expired context, or unacknowledged commit refuses. A successful authentication or visible audit row MUST NOT substitute for those checks", §1). Failure behavior: permanent invalidation of the attempt and all handle copies ("Lock loss, tooling interruption, observer failure, or shared-prefix invalidation permanently invalidates the attempt and all handle copies", §3). Prohibition kept: no original-fingerprint overwrite and no comparator weakening ("This does not approve overwriting original fingerprints or weakening existing comparator checks", item 1).
Proposed approval sentence: "批准在上述 R1 范围内使用独立恢复执行身份：现有受信恢复控制器仅为通过认证且已注册、持有既有恢复执行权限的执行者铸造每次尝试至多一个的一次性不透明准入句柄，其不可变绑定的身份来源与应用条件、五个验证点、拒绝条件与失效即永久作废行为以 ADR-004 §1 与 §3 为准；不批准覆盖原 writer 指纹或削弱既有比较器检查。"

**R2 — Database permissions (item 2 — NOT YET RULED; material below is the precise grant inventory awaiting the user's ruling, updated per the 2026-10-04 handoff that R2 stays pending).** Four separated service identities; grants hold for the minted-attempt interval unless stated otherwise.

**(a) Restricted recovery database role (the native `pg_restore` child's role) — target data database.**
- Grant/revoking subject and timing: deployment-controlled privileged administrator provisions the role and target-side privileges **before minting** (deployment prerequisites, not runtime grants) and revokes/expires them at the §3 Finish phase (verified cleanup references). TxHarbor never performs or self-grants these privileges.
- Pinned command vector: `pg_restore '--clean' '--if-exists' '--dbname=<conninfo>'` (`internal/recovery/targetwriter_linux.go:293`; neither `--no-owner` nor `--no-privileges` is passed, so archive semantics apply as follows).
- Exact privileges, all scoped inside that single target database:
  - `CONNECT` on that database only.
  - `--clean` issues `DROP TABLE/VIEW/SEQUENCE/...` (guarded by `--if-exists`). Legitimate completion path: **object ownership must sit with the recovery role on every object the archive replaces**; where the archive records the original writer as owner, ownership is legally established by the archive's own `ALTER ... OWNER TO` statements executed under the restricted role only for objects already owned by it, and any pre-provisioned ownership handover of the replacement's objects to the recovery role is executed and audited in the deployment lane. Ownership-attribution failures fail the restore — no `--role=` rewriting, no `--no-owner` bypass, no superuser.
  - Payload `CREATE`/`ALTER TABLE ... ADD CONSTRAINT` exactly as re-created by the archive content; sequence/serial grants exactly as the dump content creates them, no extras.
  - `CREATEDB` is not granted (the vector cannot create databases); no cluster-wide `CREATE` on any other database.
  - Never granted: superuser, `CREATEROLE`/role management, `REPLICATION`, `BYPASSRLS`, membership in the original writer role or any other role, any credential of the original writer role.
- Object-level SQL list: 待定 — the schema-qualified enumeration is generated from the pinned archive content at implementation time (with evidence) and separately approved; the ADR text alone does not decide it.
- Refusal conditions: role-OID mismatch or unregistered child refuses; wrong database/role refuses (§5-C); a scope outside this enumeration → no executable-frame release and no unauthorized target write; an archive payload needing unenumerated privileges (e.g. database-level object re-creation) refuses.
- ADR anchor: item 2 "approve the exact target-scoped restoration privileges and archive ownership/role semantics. No implicit superuser, role-management authority, original-writer membership, or privilege escalation"; §1 "The native child names the separately bound recovery role; no fabricated identity or bearer token is introduced."

**(b) Control-store writer — independent control database (distinct from (a)).**
- Grant/revoking subject: deployment-controlled administrator; the control store is a separate database (ADR-001), never the data DB.
- Exact privileges at the closed enforcement points: `SELECT/INSERT` on `recovery_evidence` (restore.go `Prelaunch` inserts the `restore_started` marker; `Acceptance` inserts `restore_probe_accepted`); `SELECT` + conditional `UPDATE` of `evidence_generation`/`evidence_hash` on `recovery_instance` (generation.go:424-425); `INSERT/SELECT` on `recovery_audit` (store.go:1284); `SELECT/INSERT/UPDATE` on `recovery_target_guard` plus the `target_guard_key`/`target_role_fingerprint` columns of `recovery_instance` (targetguard.go Initialize/Prepare/Mark*/Accept/Resolve/Record; schema 0003), all under `SELECT ... FOR UPDATE` row locks; `recovery_verification_item`/`recovery_approval`/`recovery_release` read + insert only — no `UPDATE`/`DELETE` path in any 015 command (0001 schema header invariant).
- Scope: single control database; zero data-DB privileges; no cluster-wide rights.

**(c) Monitoring/quiescence observer — distinct from both (a) and (b).**
- Actual visibility: `pg_read_all_stats` is cluster-wide (PG18 monitoring-stats doc): it exposes other roles' `pg_stat_activity` rows — the exact mechanism `waitTargetQuiescentWithLock` (`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, targetlock.go:411) depends on; the observer must connect to the same PostgreSQL cluster as the restore target (targetwriter_linux.go:68).
- Grant: `GRANT pg_read_all_stats` + `CONNECT` on the target database; it does not read target tables, cannot write anything, is not a control-store writer, and holds no recovery-role credential.
- Source: ADR-003 "The observer must see full details for sessions owned by other roles. ... the configured observer role must have `pg_read_all_stats` (or be superuser); deployment should grant the least-privilege monitoring role capability needed for this check rather than require superuser."

**(d) Deployment-management actions vs recovery-process privileges.** Separate lanes, separate authorization:
- Database create/drop (replacement rebuild): the controlled replace path is `DROP DATABASE` without `FORCE`, then `CREATE DATABASE ... OWNER <original writer>` through the dedicated control connection (`borrowed-owner-ddl-feasibility_linux_test.go:11-13`), executed as a **separately authorized deployment-management action with its own audit** — never by the recovery role, never implied by R1/R3/R4/R5.
- Role management (`CREATE ROLE`, `ALTER ROLE`, object grants): deployment-lane only, audit-recorded; the recovery chain proceeds only when the recovery role holds exactly the enumerated set — under-grant refuses the restore, over-grant refuses admission.
- Anything not explicitly authorized stays refused; no implicit superuser and no inherited original-writer privilege exists in any lane.

Local-can-prove (§4): real restricted-role restore/probe/acceptance over the fixture target with exactly the (a)+(b)+(c) enumeration, with (d) executed as separately authorized deployment-management steps in the fixture's own lane. Production still provides (item 2 + §4): the same enumeration provisioned by the deployment-controlled administrator, role password/config security controls, the HBA inventory keeping the recovery role's ingress to the single protected route, (d)'s change-control record, and the constraint review of this privilege set at deployment.
Proposed approval sentence for R2 (awaiting ruling; this round does not approve it): "批准受信部署管理员为恢复链创建并持有四个分立身份：(a) 受限恢复 DB 角色仅要求目标库 CONNECT 与该库内 pinned `pg_restore --clean --if-exists` 载荷 DROP/CREATE/ALTER 所需对象级特权；对象所有权合法完成＝部署 lane 独立受控审计的预置所有权交接 + 归档自带 `ALTER ... OWNER TO` 仅对已属本角色的对象生效，所有权失败即拒绝，禁 `--no-owner`/`--role` 重写/超用户；(b) 控制库写入者仅在独立控制库内具备既有 enforcement point 的 SELECT/INSERT/条件 UPDATE（recovery_evidence、recovery_instance 代次与守卫列、recovery_audit、recovery_target_guard；append-only 表只插入），零目标库特权；(c) 观察者为目标库 CONNECT + `pg_read_all_stats`（cluster 级可见其他角色 pg_stat_activity 行，供精确 application_name 静默 census），无表读、无写、非恢复角色；(d) 数据库建删与角色管理为独立授权的部署管理 lane（DROP 不带 FORCE + CREATE ... OWNER 原 writer，经专用控制连接，独立审计），恢复角色永不执行且不继承原 writer 权限；未列明权限保持拒绝；对象级 SQL 清单实现期由 pinned 归档生成并附证据后单独批准（待定）。"

**R3 — Server/OS admission boundary (item 3, from §2 and §4).** Gate OS identity: a real, deployment-controlled OS account, mapped via peer authentication to the recovery role ("The server maps a real, deployment-controlled gate OS identity to that role", §1; "peer authentication of the protected gate OS identity", §2). Privileges granted to the gate OS identity: exclusive ownership of the protected gate peer-authentication route ("exclusive ownership of that route", §4) — its socket access, peer mapping, and role privileges must be verified deployment facts ("Socket access, peer mapping, role privileges, and alternative routes must be verified deployment facts", §2); it is exclusive to the trusted controller boundary and not shared with ordinary CLI users or old writers ("The gate identity must be exclusive to the trusted controller boundary, not shared with ordinary CLI users or old writers", §2). No other privileges are conferred by this item beyond what that verified-deployment-fact boundary needs; any route, role membership, credential, or administrative path permitting an excluded writer to reach recovery write authority blocks admission ("Any route, role membership, credential, or administrative path that permits an excluded writer to reach recovery write authority blocks admission", §2). Allowed action and window: the gate owns the peer-authenticated server connection and dispatches the single registered backend per minted admission, from mint to evidence acceptance within the live exclusion interval. Refusals (§2): "Other OS identities cannot authenticate as that role; TCP ingress rejects it"; "Loopback source address alone is insufficient". Failure behavior: any mismatch, unregistered child, bridge, or second concurrent same-target attempt → admission latches, no executable frames released (§5-C). Prohibition kept: "Existing TCP forwarding is not this enforcement boundary" (item 3).
Proposed approval sentence: "批准受保护的本地 peer 准入边界及其独占 gate OS 身份：gate = 单个部署受控 OS 账户，经 peer 认证唯一映射到受限恢复 role，且独占该受保护准入路由——不与普通 CLI 用户或旧 writer 共享；其余 OS 身份不得认证为该 role 且 TCP 拒绝该 role 入站；socket 访问、peer 映射、角色特权与替代路由作为部署事实核验；环回源地址不单独充分；既有 TCP 转发不是本执行边界；任何不匹配、未注册子进程或第二次同目标试验立即拒绝且不释放可执行帧。"

**R4 — Protection window (item 4, from the §3 table and its paragraphs).** Allowed action and window: establish exclusion before the first recovery-target mutation, i.e., before destructive replacement DDL ("Protection begins before any recovery target mutation, including destructive replacement DDL", §3), and retain it through evidence acceptance ("authentication, execution, drain, probes, and acceptance must remain inside the held protection interval", §3). Refusal behavior on window failure: permanently invalidates the attempt and all handle copies, terminates the process group, awaits authentic completion; lock loss / tooling interruption / observer failure / shared-prefix invalidation all route there (§3). Concurrent-attempt refusal: "Concurrent recovery attempts cannot mint or dispatch a second admission for the same target; missing rows never imply clean" (§3).Fail-closed guarantee kept (§5-D): "No positive restore acceptance or continuation is emitted" and "historical committed records are not repaired" under any protection-loss injection.
Proposed approval sentence: "批准建立并保持保护窗口：server 排他（HBA 拒绝 + 巡查）在恢复链的第一次目标变更（含破坏性替换 DDL）之前建立并保持到证据接受为止；census/排空/probes 必须整链都在保护窗内；锁丢失/工具中断/观察者失败/共享前缀失效永久作废本尝试并终止进程组；同目标并发恢复尝试不得铸造第二张准入；保护丢失绝不冒充成功或继续。"

**R5 — Acceptance integration (item 5, from the §3 table's Evidence-acceptance row, its fail-closed paragraph, §5-A/D and item 5).** Subject and lifecycle: admission authorization is bound to the instance row from mint to close; one attempt's consumption makes its handle invalid for all copies ("Copies share one-use consumption and permanent invalidation", §1); the acceptance transaction is conditional reuse of the existing atomic mechanism (the "existing atomic evidence/generation/guard-clean transaction" of the §3 table, reuse allowed only under the R1 identity/permission ruling and the §3 fail-closed arbitration). Guard/condition: "Same target lock; exact guard/instance row checks; current generation/hash; live exclusion and shared-loss arbitration through transaction completion" (§3 table). Refusal conditions: stale token, protection failure, transaction failure or acknowledgment uncertainty → no positive acceptance; committed rows retained, "not deleted or relabelled" (§3); non-authorizing journal/readiness records are not promoted (item 5); recovery from copied diagnostics forbidden (item 5); "If the control store is unavailable, acceptance and further dispatch stop; inability to journal is not permission to continue" (§3).

Post-consumption clarification (2026-10-04 ruling point 4): consuming the one-shot handle refuses further *launches* only. The minted attempt's evidentiary chain continues inside the same coordinator run: the durable attempt intent, `restore_started` marker, guard preparation, and launch-intent commit precede execution (§3 table); the child/backend can be observed and drained without a second handle (`DrillTargetProcessObservation` is the opaque observation handle and `DrillTargetProcessReceipt` is the frozen one-use receipt returned even on failure or cancellation — "It returns the frozen opaque receipt even on failure or cancellation; there is no second Wait and no clean-retirement proof", drillbridge; `ConsumeFacts` is itself one-use); the in-transaction Verify/Acceptance hooks run under the same target lock and the prelaunch/acceptance transactions, not via a re-minted handle. Therefore: a consumed handle cannot start again; an unclosed instance does not extend an invalidated attempt — a new attempt requires a fresh mint under the R1 conditions, and invalidated attempts stay blocked until the controlled reconciliation path (fresh exclusion + original-attempt proof, §3) re-opens admission legitimately.
Proposed approval sentence: "批准实例绑定授权生命周期与原子接受的条件性复用：准入授权从铸造绑定到 close 的实例行，一次性消耗并即对全部副本永久作废；一次性消耗仅拒绝再次启动，已绑定尝试的取证与接受经由同一 coordinator 运行续链完成（durable attempt intent + restore_started 标记 + 守卫准备 + launch-intent 先行提交、Observation/一次性 receipt 观察 child/backend 与排空、Verify/Acceptance 钩子在既有 prelaunch/接受事务内执行），不以重新铸造的句柄进行；未关闭实例不得延长已失效尝试，新尝试须按 R1 条件重新铸造，失效尝试保持阻塞直至受控对账（fresh exclusion + original-attempt proof）使其重新合法；接受沿用既有原子 evidence/generation/guard-clean 事务，须在同一目标锁、精确守卫/实例行检查、当前代次/哈希、活跃排他与共同丢失仲裁下完成；不接受 stale token/保护失败/事务失败/提交确认不确定，已提交行不删除不改标；非授权期刊/readiness 记录不得升级，不得从复制的诊断件恢复；控制库不可达即停止接受与进一步派遣；既有复服审批仍独立执行。"

## Acceptance Checklist

- [ ] **§5 A:** Real authorized native restore, registered backend, completed probes/drain, acknowledged atomic acceptance; no implicit service release.
- [ ] **§5 B:** Genuine original P0/P1 server refusals on every supported route throughout the protected interval; no retained writer.
- [ ] **§5 C:** Unauthorized, copied, stale, mismatched and concurrent attempts cannot write or launch outside admission.
- [ ] **§5 D:** Every protection-loss injection prevents positive acceptance/continuation and preserves truthful blocking state and append-only history.
- [ ] All separate decision items receive explicit rulings; unresolved items retain BLOCKED status.
- [ ] Evidence states its demonstrated interval and local scope; preparation remains `completed_restoration=false`, and T000-P remains OPEN.
