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

## Separate Decision Items Requiring Explicit User Ruling

1. **Execution-identity change:** approve or reject a distinct recovery role and separately bound execution identity. This does not approve overwriting original fingerprints or weakening existing comparator checks.
2. **Database permissions:** approve the exact target-scoped restoration privileges and archive ownership/role semantics. No implicit superuser, role-management authority, original-writer membership, or privilege escalation.
3. **Server/OS admission boundary:** approve protected local peer admission and its exclusive gate OS identity, configuration ownership and ingress restrictions. Existing TCP forwarding is not this enforcement boundary.
4. **Protection-window change:** approve establishing exclusion before the first recovery mutation and retaining it through acceptance, rather than treating closed preparation/teardown intervals as continuous protection.
5. **Acceptance integration:** approve the explicit instance-bound authorization lifecycle and conditional reuse of existing atomic acceptance. This does not promote non-authorizing journal/readiness records or permit recovery from copied diagnostics.

No new business-policy exception is proposed. Forced resumption, missing-proof waivers, data-loss approval and completed-restoration claims from preparation remain forbidden.

## Acceptance Checklist

- [ ] **§5 A:** Real authorized native restore, registered backend, completed probes/drain, acknowledged atomic acceptance; no implicit service release.
- [ ] **§5 B:** Genuine original P0/P1 server refusals on every supported route throughout the protected interval; no retained writer.
- [ ] **§5 C:** Unauthorized, copied, stale, mismatched and concurrent attempts cannot write or launch outside admission.
- [ ] **§5 D:** Every protection-loss injection prevents positive acceptance/continuation and preserves truthful blocking state and append-only history.
- [ ] All separate decision items receive explicit rulings; unresolved items retain BLOCKED status.
- [ ] Evidence states its demonstrated interval and local scope; preparation remains `completed_restoration=false`, and T000-P remains OPEN.
