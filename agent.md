# TxHarbor Agent Guide

Production-oriented EVM wallet + transaction infrastructure. Monorepo: API / Indexer / Worker / Signer / PostgreSQL / Redis / Kafka / EVM RPC.

总览与快速启动见 `README.md`；已合并变更见 `CHANGELOG.md`。

## Core Principles

- Reliability over feature velocity.
- PostgreSQL is the source of truth. Redis must never hold financial truth.
- All financial operations must be idempotent (DB UNIQUE constraints, not app-only checks).
- All state transitions must be explicit.
- Never do irreversible financial actions inside HTTP handlers — enqueue to Worker.
- Private keys only in Signer. Never expose to business services.
- Chain data must tolerate retries + duplicate delivery.
- Reorg handling must preserve canonical-chain correctness.
- Every concurrency-sensitive op needs tests. Failure paths are first-class.

## Go

- `context.Context` on all I/O boundaries.
- Return errors, never panic in app code.
- Small interfaces, explicit deps, no global state.
- `pgx` for PostgreSQL.
- Money/token values: never `float64`. EVM quantities: `big.Int`. Addresses/hashes: go-ethereum types.

## Database

- State changes inside PostgreSQL transactions.
- Cursor advancement + indexed data insert must be atomic.
- Idempotency via DB-level UNIQUE constraints.

## Blockchain / Indexer

Must support: resumable sync, duplicate processing, RPC retries, reorgs, confirmation tracking.
Do not assume websocket subscriptions are lossless.

## Testing

- Every feature: unit + integration + E2E.
- Critical behaviors: failure-path tests required.
- Always run `go test ./...` before completing a task.

## Git

Conventional Commits:

- `feat(indexer): add resumable block scanning`
- `fix(indexer): handle duplicate logs`
- `test(reorg): cover two-block chain reorganization`
