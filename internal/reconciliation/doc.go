// Package reconciliation implements the 014 reconciliation and exception
// handling core: scoped, checkpointed, resumable three-way comparison of chain
// facts, PostgreSQL business state, and event delivery/consumer results, with
// stable discrepancy identity, bounded scan budgets, and default-deny
// authorization. See specs/014-reconciliation-exception-handling/.
//
// Planned file map (see plan.md Source Code layout):
//
//	scan.go        - ScanOnce core: scope → budgeted intervals → compare (T006, T016)
//	identity.go    - stable identity + evidence hash (T005, T021)
//	classify.go    - machine classification, business-divergence-only duplicates (T017)
//	lifecycle.go   - claim/dispose/reverify/close transitions + invalidation (T007, T022, T026)
//	budget.go      - concurrency/range/time quotas, backoff, cancel (T008)
//	authz.go       - action×permission×scope enforcement, default-deny (T009)
//	store.go       - atomic result+checkpoint commit, crash recovery (T010)
//	chainfacts.go / pgstate.go / eventstate.go - read-only evidence adapters (T013-T015)
//	obligation.go  - T040 expected-event discriminator (event_obligation read + R1/R2/R3)
//	reverify.go    - bounded reverify executor (T027)
//
// Hard boundaries (see spec.md FR-014/015/020/021/023 and Q1–Q5):
//
//   - Read-only reuse of chain observation, txlifecycle reconcile/unknown
//     recovery facts, execution gates, and outbox/consumer progress; never
//     auto-trigger recovery, replay/unblock, or payments.
//   - Discrepancies are never a re-payment permission; signing paths untouched.
//   - No user balance ledger; PostgreSQL remains the authoritative store.
//   - Permissions default-deny via recon_permission; operator/reason text and
//     operation_id are audit/idempotency only, never authorization.
//   - Audit and occurrence rows are append-only.
//
// Migration and table inventory (see data-model.md §1 and
// migrations/000016_reconciliation_handling.sql):
//
//	recon_task, recon_checkpoint, recon_gap, discrepancy,
//	discrepancy_occurrence, disposition, reverify, recon_audit,
//	recon_scan_attempt, recon_permission.
//
// Test layering (see quickstart.md; ordinary PR CI stays layered):
//
//	contract/integration tags for lifecycle and scan paths; fault tag for
//	reorg/unknown/interrupt/concurrent groups on the independent channel;
//	heavy scans and long benchmarks never gate ordinary PRs.
package reconciliation
