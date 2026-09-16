# Implementation Plan: 007 Authorization Carrier Supplement (PB)

**Branch**: `012-007-authorization-carrier` | **Date**: 2026-09-16 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/012-007-authorization-carrier/spec.md`

## Summary

Give 009's complete legal path a trustworthy authorization carrier without
redefining 007: one additive `withdrawal_authorization_scopes` table written
atomically by the extended `withdrawal-authz supply` transaction (grant +
scope + audit), a pre-pool issuance-permission gate implementing PB-C1
(single operations role; `--operator` stays audit-only), the PB-C2 three-dimension
fee rule with cross-checks, revoke-synced scope state/version, an explicit
re-issuance procedure with old-grant traceability, and a renumber-at-merge
migration chain. 009 consumes read-only (later lane); 011 consumes later.

## Technical Context

**Language/Version**: Go 1.26.5 (repo toolchain)

**Primary Dependencies**: pgx v5.11.0, goose v3.28.0, go-ethereum v1.17.5 (unchanged; no new dependencies)

**Storage**: PostgreSQL (existing DB; one additive table + extended supply tx; pure-DDL migration)

**Testing**: `go test ./...` + `go test -tags integration ./...` (testcontainers: PostgreSQL 18.6-trixie; Anvil only where 009-side execution runs — not in this lane)

**Target Platform**: Linux server (single binary + subcommands, no new infra)

**Project Type**: cli + library (extended subcommand + `internal/withdrawal` library)

**Performance Goals**: supply/revoke remain single-row short transactions (existing 5s statement guard kept); no new lease, no new background loop

**Constraints**: zero change to 007 columns/read shapes/intake semantics; no 008 change; no 009/010/011 implementation; only unapplied migration numbers may move; applied history immutable

**Scale/Scope**: one table, one extended CLI entry, one permission gate, one migration, one dry-run procedure

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- I (financial correctness) / II (idempotency via UNIQUE carriers): supply+scope atomic in one tx; operation-id convergence preserved and extended to scope re-read. PASS.
- V (explicit state machine): scope follows grant state; version monotonic; re-issuance is new-identity, never silent rewrite. PASS.
- VI (fail-closed): unauthenticated/unmapped supply refused pre-pool; scopeless stock stays 009-refused; missing/illegal/over-limit fees refused. PASS.
- VII (shared-variable infrastructure): grant-row `FOR UPDATE` order kept; readers share-lock; no new lock objects. PASS.
- XII (observability): supply audit extended with principal + scope version; no key material (no keys exist on this path); `--operator` audit-only. PASS.
- XIV (no upstream redefinition): 007 columns/reads/intake untouched; Accepted semantics untouched; history not negated. PASS.
- Simplicity: no dual control, no per-grant cryptography (Q-A); no new infra, no new background work. PASS.

## Project Structure

### Documentation (this feature)

```text
specs/012-007-authorization-carrier/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── supply-scope.md  # extended supply entry + scope read boundary
├── checklists/
│   └── requirements.md  # spec quality (prior phase)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
migrations/
└── 000010_withdrawal_authorization_scopes.sql   # new (planning number; renumber-at-merge)

internal/withdrawal/
├── grant.go                 # extended OpInput + T-supply+scope + T-revoke-sync + convergence re-read
├── grant_integration_test.go # extended round-trips (existing file)
└── auth.go                  # issuance-permission check shape (existing Authenticate)

internal/app/
├── withdrawalauthz.go       # extended flags + pre-pool permission gate
└── withdrawalauthz_integration_test.go  # extended cycles (existing file)

internal/db/
└── migrate.go               # untouched (runner picks up the new file automatically)
```

**Structure Decision**: Single project (Go monorepo); additive migration + extended
existing files only — no new packages, no new binaries, no new infrastructure.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| *(none)* | — | — |

## Key design decisions (traceable to research)

1. Permission gate pre-pool, transport-free library kept (R-PB1/R-PB2).
2. Scope 1:1 FK, version monotonic, no signature column (R-PB3, Q-A).
3. Fee triple + cross-checks as frozen rule text; DDL representation to implement (R-PB4, PB-C2).
4. Renumber-at-merge `max(merged)+1`, planning `000010` (R-PB5).
5. 009 plug-in points enumerated read-only; 009 lane owns consumption (R-PB6).
6. OPEN-3 acceptance designed here (quickstart.md V-PB9 + matrix); tasks assign execution.
7. No new business questions raised; PB-C1/PB-C2/Q-A/Q-B not reopened.
8. Merge/deploy order determined (R-PB8): PB merges first as `000010`,
   009 later as `000009` unchanged; gap-fill is native goose `Up`
   (`provider.go:244`), serve gate enforces it — no merge-time decision left.
9. Authority protocol closed (R-PB10): in-tx re-verification (api_key +
   caller `FOR SHARE` + `PermitIssue` re-eval, `supply_refused` on failure);
   allowlist env `TXHARBOR_AUTHZ_ISSUER_CALLERS` fixed; controlled switchover
   procedure (halt → drain-confirm → atomic swap + checksum → re-enable;
   rehearsed by T043/V-PB11); V-PB3 split into PB-executed and joint-009 parts. No bounded-window option remains.
