# Pinned 009 migration — TEST FIXTURE ONLY

`000009_signer_service.sql` is a byte-exact read-only copy of 009's signer-service
migration, used only by PB's T040/T041/T042/T036 integration tests to prove the
carrier chain composes with 009's file. It is NOT a production migration.

Provenance (verified 2026-09-17):

- Source lane: 009-signer-service @ `8f7545023e531eb0bc95cb20f19eb39ec284256a`
  (worktree clean at copy time)
- Source path: `migrations/000009_signer_service.sql` in that lane
- SHA256: `53e6ca6fac0b03de86961175d5471612b54e7dad89dabf9cc45e1567013f04b9`
  (identical to the former `/tmp/pb-009ref` scratch pin; `cmp` clean)

Rules:

- Keep bytes identical to the pinned 009 version. If 009 advances, re-pin
  deliberately (copy + update `pb009FileSHA256` in
  `scratch_009_overlay_integration_test.go` + re-judge T040/T041 scope per T002).
  The tests assert the hash, so drift fails loudly.
- NEVER move this file into `migrations/` or any production embed. Production
  migration discovery (`migrations/embed.go`) must stay 009-free; guarded by
  `TestLaneMigrationsExclude009`.
- This file replaced the `/tmp/pb-009ref` scratch dependency that broke remote
  CI (PR #11, run 35169843720): a clean checkout now runs with no outside-tree
  fixture.
