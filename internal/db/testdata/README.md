# Pinned 009 migration — TEST FIXTURE ONLY

`000009_signer_service.sql` is a byte-exact read-only copy of 009's signer-service
migration, used only by PB's T040/T041/T042/T036 integration tests to prove the
carrier chain composes with 009's file. It is NOT a production migration.

Provenance (verified 2026-09-17):

- Source lane: 009-signer-service @ `9f029eed63a11177963e24180efa16a0c6e617a0`
  (T037 `authorization_version` revision; supersedes the `8f75450` pin)
- Source path: `migrations/000009_signer_service.sql` in that lane
- SHA256: `cd77bffd2b434f0dcd0bf8bf63e169b1e2db75bcf5beda8edc05f0901b1ae859`
  (re-pinned by 009-lane T041; prior pin `53e6ca6f…` = pre-T037 bytes)

Rules:

- Keep bytes identical to the pinned 009 version. If 009 advances, re-pin
  deliberately (copy + update `pb009FileSHA256` in
  `scratch_009_overlay_integration_test.go` + re-judge T040/T041 scope per T002).
  The tests assert the hash, so drift fails loudly.
- NEVER move this file into `migrations/` or any production embed. Production
  migration discovery (`migrations/embed.go`) must stay 009-free; guarded by
  `TestLaneMigrationsCarryReal009` (retargeted 2026-09-17 for the 009 lane; main-side exclusion restored at 009 merge).
- This file replaced the `/tmp/pb-009ref` scratch dependency that broke remote
  CI (PR #11, run 35169843720): a clean checkout now runs with no outside-tree
  fixture.
