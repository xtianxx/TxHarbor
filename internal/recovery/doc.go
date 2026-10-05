// Package recovery is the 015 backup-recovery and safe-service-resumption core
// library.
//
// Structure Decision (specs/015-backup-recovery-safe-resumption/plan.md): the
// feature is a testable core library inside the existing monorepo — manifest
// (backup identity/selection), gate (derived release evaluation), capabilities
// (the closed 7-capability set and dependency matrix), verification (V1-V9
// read-only orchestration), gaps (evidence bundles and dependency proofs),
// approvals (single/dual validity), generation (evidence-generation write-write
// ordering) and controlstore (the independent control-store store and schema) —
// plus the thin `recovery-admin` operator command
// (internal/app/recoveryadmin). It adds no service boundary, listener, daemon,
// UI or scheduler, and this phase makes zero data-DB schema changes: the root
// migrations/ package and its goose_db_version sequence stay untouched, while
// recovery control facts live in an independent control store with its own
// embedded goose FS (internal/recovery/controlstore/schema) and its own
// `recovery-admin migrate` version sequence.
//
// Hard import boundary (T001): this root package is imported by package app
// (the capability gates are wired at the real entry points, T030-T034/T070), so
// it MUST NOT transitively import internal/txlifecycle, internal/events,
// internal/indexer, internal/reconciliation or internal/cache. Those packages'
// internal integration tests import internal/app, so a transitive import from
// package app would close a test-build cycle (the same constraint documented on
// internal/app/reconcileadmin). The heavy V1-V9 source adapters therefore live
// in internal/recovery/sources/ (B10) or are injected as interfaces assembled
// by internal/app; they are never imported by this root package. Proof that the
// boundary holds: `go vet -tags integration ./internal/txlifecycle
// ./internal/events` must build.
//
// Safety boundaries (contracts/resumption-gate.md, ADR-001): no open recovery
// instance means normal pass-through; while an instance is open every wired
// capability denies by default, and a capability is released only by the
// derived evaluation (instance + evidence generation/hash + isolation
// checklist + no open gap + valid single/dual approvals + capability
// dependencies + the unchanged existing fund gates), never by a writable
// boolean; control-store unavailability refuses (fail-closed); there is no
// "disable the gate" switch; and the recovery tools themselves never publish
// events, sign, broadcast, call business RPCs or produce real downstream
// deliveries.
package recovery
