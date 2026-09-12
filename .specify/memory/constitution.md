<!-- Sync Impact Report
Version change: 1.0.0 → 1.1.0
Modified principles: strengthened mandatory tone (SHOULD/MAY/where-appropriate → MUST/MUST NOT)
across I, II, V, VI, VII, VIII, IX, X, XI, XII, XIV; Engineering Standards (Go, Database, RPC);
Security Rules rewritten as MUST NOT; Governance self-weakening prohibited (MUST NOT).
Added sections: none
Removed sections: none
Follow-up TODOs: none
-->

# TxHarbor Constitution

## Preamble

TxHarbor is a production-oriented EVM wallet and on-chain transaction
infrastructure project.

Its purpose is to reliably bridge off-chain application state with
EVM-compatible blockchains, including blockchain indexing, deposit detection,
withdrawal processing, transaction lifecycle management, nonce allocation,
signing isolation, RPC resilience, and operational observability.

TxHarbor is not primarily a wallet UI, DeFi protocol, block explorer, or
general-purpose blockchain framework.

The project demonstrates production-grade backend engineering under failure,
concurrency, duplication, restart, and blockchain reorganization scenarios.

Correctness of financial and blockchain state takes precedence over
development speed, architectural novelty, or benchmark numbers.

## Core Principles

### I. Financial Correctness First

Financial correctness is the highest priority of TxHarbor.

Any operation involving deposits, withdrawals, balances, transaction state,
token amounts, nonce allocation, confirmation state, or accounting events
MUST favor correctness and recoverability over throughput, latency, or
implementation convenience.

The system MUST NOT knowingly accept a design that can result in duplicate
withdrawals, duplicate credits, silently lost deposits, duplicated nonce
allocation, corrupted transaction state, or irreversible state divergence.

Token amounts, native asset amounts, gas quantities, and other monetary
values MUST NOT use floating-point representations. EVM monetary values MUST
use lossless integer representations such as `big.Int` and integer/NUMERIC
columns. Inexact binary floating-point types (float32/float64) MUST NOT be
used for any monetary value at any layer. Exact decimal is permitted ONLY for
display formatting converted from the integer source.

Database and application invariants MUST make invalid financial states
impossible to commit via CHECK, UNIQUE, FK, and state-transition guards.

### II. Idempotency by Design

Every operation that may be retried, replayed, duplicated, or redelivered
MUST be idempotent, including block indexing, transaction indexing, log
processing, deposit detection and confirmation, withdrawal creation,
asynchronous jobs, message consumers, transaction broadcasting, and Outbox
processing.

Idempotency MUST NOT rely solely on in-memory checks. Guarantees MUST be
enforced at the persistence layer using UNIQUE constraints, stable event
identifiers, idempotency keys, explicit state-transition validation, AND
transactional writes covering the state transition and cursor/key write
atomically.

For EVM logs, the tuple (chainID, blockHash or txHash, logIndex) MUST be
treated as the stable event identity and enforced with a UNIQUE constraint.
Repeated processing of the same valid input MUST converge to the same
durable state.

### III. PostgreSQL Is the Durable Source of Truth

PostgreSQL is the authoritative durable state store for chain sync cursors,
canonical and orphaned blocks, indexed chain events, deposits, withdrawals,
outbound transactions, nonce allocation state, Outbox events, and business
state-machine transitions.

Redis, Kafka, local memory, and caches MUST NOT become the sole source of
truth for financially significant state. Redis MAY be used for caching, rate
limiting, temporary coordination, non-authoritative health state, and
performance optimization. Kafka MAY be used for asynchronous event delivery
and service decoupling.

Loss, restart, or temporary unavailability of Redis or Kafka MUST NOT make
authoritative financial state unrecoverable. Durable financial decisions MUST
remain reconstructable from PostgreSQL and canonical blockchain data.

### IV. Blockchain State Must Be Reorg-Aware

TxHarbor MUST NOT assume an observed block is permanently canonical merely
because it was returned by an RPC node. Indexed block data MUST preserve
chain ID, block number, block hash, and parent hash at minimum.

The system MUST distinguish canonical from orphaned chain data when
reorganization handling is implemented. Domain state derived from blockchain
events MUST be traceable to its originating chain data.

A chain reorganization MUST NOT result in permanent credit for an orphaned
deposit, permanent reliance on orphaned event data, or silent corruption of
synchronization state. Confirmation depth MUST be explicit and configurable
per chain or asset policy.

WebSocket subscriptions MUST NOT be treated as lossless. Persistent
range-based synchronization or equivalent recovery MUST exist so missed
events can be recovered after disconnects or restarts.

### V. State Machines Must Be Explicit

Financial and transaction workflows MUST use explicit state machines.
Deposits, withdrawals, outbound transactions, and jobs MUST define allowed
states, allowed transitions, terminal states, failure states, and retry
behavior.

Example withdrawal lifecycle:
`CREATED -> VALIDATING -> QUEUED -> SIGNING -> BROADCASTED -> CONFIRMING -> CONFIRMED`.

Failure states MAY include `REJECTED`, `FAILED`, `CANCELLED`, `REPLACED`.
Code MUST NOT arbitrarily assign states from unrelated execution paths.
Invalid transitions MUST be rejected or treated as errors. State transitions
affecting durable financial behavior MUST occur inside database
transactions.

### VI. Transaction Boundaries Must Preserve Invariants

Operations that form one durable state transition MUST be
committed atomically. Blockchain indexing MUST persist block, events,
derived state, and sync cursor in one atomic boundary per indexing unit.

The system MUST avoid cursor advanced but block missing, deposit credited
but event missing, withdrawal marked broadcasted without a durable
transaction reference, or nonce allocated without a recoverable transaction
relationship.

Transaction boundaries MUST be designed around domain invariants, not
convenience. Distributed dual writes such as `database write -> Kafka
publish` MUST NOT be assumed atomic. Where durable event publication is
required, a transactional Outbox or equivalently safe mechanism MUST be
used. Direct database-write-then-publish without outbox MUST NOT be used.

### VII. Nonce Allocation Must Be Concurrency-Safe

Outbound EVM transactions MUST treat nonce allocation as shared mutable
financial infrastructure state. Concurrent workers MUST NOT independently
allocate the same nonce to multiple transactions for the same sender and
chain.

Nonce allocation MUST have a durable concurrency-control strategy such as
PostgreSQL row locking, transaction-level serialization, advisory locking,
or another design with equivalent guarantees. In-memory mutexes alone MUST
NOT be relied upon across restarts or multiple worker instances.

The nonce subsystem MUST define recovery for service restart, pending,
dropped, and replacement transactions, RPC disagreement, and externally
submitted transactions. Concurrency correctness MUST be
covered by integration tests.

### VIII. Private Keys Must Be Isolated

Business services MUST NOT directly own or expose private keys. API,
indexer, deposit processor, withdrawal processor, and orchestration
components MUST operate without direct access to signing secrets.

Signing MUST be performed behind an explicit signing boundary accepting
structured signing requests and validating policy. Policy MUST cover
permitted chain IDs, source wallets, destination policy, value limits,
contract/function allowlists, and audit logging. Additional fields MAY be added.

Private keys MUST NOT be committed to source control. Design MUST assume
migration from local development keys to KMS or HSM systems without
business-logic redesign. A `KeyProvider` or equivalent abstraction MUST
isolate signing implementation details. Business code MUST NOT import
signing-key material except through this boundary. Local development MUST use
disposable test keys only.

### IX. Failure Paths Are First-Class Behavior

Failure behavior is part of the feature. Specifications and plans for
critical functionality MUST consider RPC timeout, rate limiting, and node
failure, WebSocket disconnection, PostgreSQL / Redis / Kafka interruption,
duplicate delivery, process crash and restart, chain reorganization, nonce
contention, transaction replacement, dropped transactions, and signer
unavailability.

Retries MUST be bounded and intentional, defining retryable and
non-retryable errors, retry limit, timeout, and backoff strategy. Retries
MUST be bounded. Infinite retries MUST NOT be used unless explicitly
justified with retry limit, timeout, and backoff. A recoverable failure
MUST NOT silently become data loss.

### X. Deterministic Local Testing Is Mandatory

Core functionality MUST be testable without depending on public testnet
funds. The primary development and integration environment SHOULD use local
infrastructure such as Anvil, PostgreSQL, Redis, Kafka, and Docker Compose.

Public testnets are validation environments, not the primary automated
testing environment. Critical workflows MUST be reproducible locally and in
CI, with deterministic commands such as `make test`, `make test-integration`,
and `make test-e2e` where appropriate.

Local tests MUST simulate deposits, ERC-20 transfers, block production,
confirmation progression, chain reorganization, duplicate events, concurrent
withdrawals, nonce contention, service restart, and RPC outage. External
testnet availability MUST NOT determine whether the core suite passes.

### XI. Test the Invariant, Not Only the Happy Path

Testing MUST be proportional to risk. Pure business logic SHOULD have unit
tests. Behavior involving PostgreSQL locking, transactions, concurrency,
Anvil, RPC behavior, or process recovery MUST use integration tests.
Mocks MUST NOT substitute for these.

Critical E2E workflows MUST include deposit flow
`chain transaction -> indexer -> deposit detection -> confirmation -> final state`
and withdrawal flow
`API request -> queue -> nonce allocation -> signing -> broadcast -> confirmation`.

Critical invariants MUST have explicit tests: no double deposit credit, no
duplicate withdrawals from one idempotency key, no duplicate nonces under
concurrency, no duplicate chain records on indexer restart, no independent
cursor commit, no orphaned data treated as canonical, no duplicated
financial effects from duplicate delivery.

Tests for concurrency-sensitive code MUST exercise real concurrency. Go
race-sensitive components SHOULD be testable with the race detector where
practical.

### XII. Observability Is Part of Correctness

Background financial infrastructure MUST expose enough telemetry to
determine health and progress. Important operations MUST use structured
logging with fields such as chain ID, block number, block hash, transaction
hash, withdrawal ID, deposit ID, nonce, wallet address, retry attempt, and
RPC endpoint.

Secrets and private keys MUST never appear in logs. Components MUST expose
metrics for indexer tip, lag behind head, RPC health, pending withdrawals,
stuck transactions, RPC failure counts, reorganization occurrence and depth,
and processing backlog.

Logging and metrics MUST NOT be cosmetic final-stage additions. Critical
operations MUST be observable when introduced.

### XIII. Simplicity Before Distribution

TxHarbor MUST NOT adopt distributed-system complexity solely to appear
production-grade. The project begins as a modular monorepo. New
infrastructure or service boundaries require a concrete reason.

The default preference is: correct, simple, testable, observable,
performant, and distributed only when justified.

Kafka MUST NOT be introduced before a simpler durable workflow has shown
where async messaging provides value. Redis MUST NOT replace PostgreSQL
correctness guarantees. Microservices MUST NOT be created merely to separate
directories or domain names. Kubernetes is not required unless a later
specification justifies deployment requirements.

Abstractions MUST correspond to real boundaries or variation points.
Unnecessary layers, interfaces, factories, repositories, or service wrappers
SHOULD be rejected when they do not improve correctness, testing,
substitution, or maintainability.

### XIV. Small, Reviewable, Specification-Driven Changes

Significant functionality MUST be implemented through bounded Spec-Kit
specifications, each describing one independently testable capability such
as project foundation, resumable chain indexing, event indexing, deposit
detection, confirmation tracking, reorg recovery, withdrawal creation, nonce
management, signer isolation, RPC resilience, transactional Outbox, or
observability.

Specifications MUST separate in-scope behavior, out-of-scope behavior,
acceptance criteria, and relevant failure cases. Implementation MUST NOT
silently expand scope beyond the active specification.

Large architectural changes MUST be split into independently reviewable
increments; exceptions MUST be justified in the plan. Prefer vertical, working slices over large
amounts of incomplete scaffolding.

## Engineering Standards

### Go

Go is the primary backend language. Code MUST propagate meaningful errors
rather than silently swallowing them, use `context.Context` across relevant
I/O boundaries, avoid uncontrolled global mutable state, use explicit
dependency wiring, keep interfaces focused and justified, handle shutdown
gracefully for long-running services, and MUST NOT use floating-point
representations for EVM financial quantities.

Panics MUST NOT be used for expected runtime failures in application code.
Generated code is exempt from normal manual formatting and style rules where
appropriate.

### Database

Schema design MUST enforce correctness constraints at the database
level, including UNIQUE constraints, foreign keys, CHECK
constraints, NOT NULL constraints, and transactional locking. Any omission
MUST have an explicit justified exception in the plan.

Migrations MUST be version controlled. Destructive or irreversible schema
changes require explicit justification. Indexes SHOULD be added based on
access patterns, correctness requirements, or measured performance needs
rather than speculation.

### RPC and External Dependencies

All external network calls MUST have explicit timeout behavior. RPC code
MUST distinguish transport failure, timeout, rate limiting, invalid
response, chain mismatch, and transaction rejection; collapsing classes
MUST NOT hide retryability.

Failover and retries MUST NOT hide persistent correctness problems. An RPC
response MUST NOT automatically be considered trustworthy if it violates
known local invariants.

## Quality Gates

A feature is not complete merely because it compiles. Before a feature is
considered complete, applicable checks MUST pass:

1. Implementation satisfies the active specification.
2. Constitution requirements remain satisfied.
3. Relevant unit tests pass.
4. Relevant integration tests pass.
5. Critical failure paths have tests.
6. Formatting and static analysis pass.
7. `go test ./...` passes.
8. Concurrency-sensitive changes are reviewed for race conditions.
9. Database migrations are reproducible.
10. No secrets or private keys are committed.

For critical financial or concurrency functionality, passing only mocked
unit tests is insufficient. The implementation plan MUST explicitly identify
the appropriate testing level.

## Architecture Evolution

Architecture MUST evolve incrementally. Initial preferred progression is
`Go + PostgreSQL + Anvil`, then only when justified `Redis`, then
`Kafka / Outbox`, then `Prometheus / Grafana`, then `Admin UI`.

Multi-chain support SHOULD build on a proven single-chain implementation
rather than multiplying unfinished functionality. Ethereum-compatible chain
abstractions MUST avoid assuming identical operational behavior across every
chain.

Optimization MUST follow measurement. Benchmarks MUST report meaningful
latency, throughput, error-rate, and resource observations rather than
maximizing headline request counts without context.

## Documentation Requirements

Architecturally significant behavior MUST be documented, explaining reasoning
behind resumable indexing, reorg recovery, idempotency, nonce management,
transaction replacement, signer isolation, RPC failover, and transactional
Outbox.

README claims about reliability or performance MUST be supported by
implementation, tests, reproducible demonstrations, or benchmark results.
Diagrams and examples MUST reflect current implementation rather than
aspirational architecture.

## Security Rules

The following are NON-NEGOTIABLE:

- Production private keys MUST NOT exist outside the signer/KMS/HSM boundary.
- Secrets MUST NOT be committed to Git.
- Private keys MUST NOT appear in logs.
- User input MUST NOT be trusted without validation.
- Token accounting MUST NOT use floating-point.
- Withdrawal authorization MUST NOT depend solely on client-side validation.
- Signing policy MUST NOT be silently bypassed.
- Business services MUST NOT directly access signer key material.
- Security-critical TODO MUST NOT be accepted as completed behavior.

All cryptographic primitives MUST come from established libraries. Custom
cryptography MUST NOT be used unless explicitly required by a specification and
separately reviewed.

## Development Workflow

The preferred Spec-Kit workflow is:
`Constitution -> Specify -> Clarify when needed -> Plan -> Tasks -> Analyze -> Implement -> Test -> Review`.

The Constitution MUST be checked during planning. If a plan conflicts with
this Constitution, the plan MUST be changed unless the Constitution itself
is explicitly amended first.

Features SHOULD be developed on focused branches and merged only after
acceptance criteria and applicable quality gates pass. Conventional Commits
SHOULD be used, for example `feat(indexer): add resumable block
synchronization`, `fix(reorg): restore canonical deposit state`,
`feat(tx): add concurrency-safe nonce allocation`,
`test(withdrawal): cover duplicate idempotency keys`, and
`perf(indexer): benchmark block ingestion`.

## Governance

This Constitution is the highest project-level engineering authority for
TxHarbor. Feature specifications, implementation plans, tasks, code changes,
and architectural decisions MUST comply with it. When a conflict exists
between a feature implementation and this Constitution, the Constitution
takes precedence.

A principle MUST NOT be weakened merely to make an implementation easier.
Constitution amendments MUST be explicit and intentional, including the
reason for the change, affected principles, expected architectural impact,
compatibility implications, and updated version number.

Versioning follows semantic versioning: MAJOR for removal or fundamental
redefinition of an existing principle, MINOR for addition of a new principle
or materially expanded governance requirement, PATCH for clarification that
does not materially change project governance.

Every implementation plan MUST include a Constitution Check. Any justified
exception MUST be documented explicitly in the relevant plan rather than
silently ignored.

**Version**: 1.1.0 | **Ratified**: 2026-09-12 | **Last Amended**: 2026-09-12
