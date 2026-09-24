// Package perf is the 013 comparative benchmark harness (T076/T077; FR-25;
// SC-11; adr.md §3; verification.md §4; PD-3). It is test support only and
// never enters a production build:
//
//   - this file carries no build tag, so `go build ./...` (and ordinary unit
//     runs) compile the package with only its documentation;
//   - harness.go and bench_test.go carry the `perf` build tag, so the
//     performance layer is built and run only through `make test-perf` and
//     never in an ordinary PR (FR-28);
//   - the harness drives the real PostgreSQL/Redis/Kafka test dependencies
//     (internal/testutil), the real local Anvil chain and the real 013
//     runtimes through the real command entry points (internal/app). It never
//     substitutes a hand-called state function for a dependency and never
//     mocks a middleware;
//   - path A (PG-only) runs PostgreSQL + Anvil with the 013 wiring off; path B
//     (full stack) runs PostgreSQL + Anvil + Redis + Kafka with caching, rate
//     limiting, publisher and consumer on. Both paths run the identical fixed
//     load generator (rate ladder and burst) and the identical fault timeline
//     (normal → Redis-only → Kafka-only → dual → recovery), so the comparison
//     is controlled (adr.md §3);
//   - every measured value is recorded as measured; values that were not
//     measured are rendered as "待测/待裁决" and are never presented as
//     "已达标" (SC-11; verification.md §5).
//
// The benchmark writes its evidence (raw per-request samples, per-state
// summaries, metric expositions, environment spec and the rendered report)
// outside the repository by default; TXHARBOR_PERF_EVIDENCE_DIR overrides the
// directory. The committed report artifact lives under docs/evidence/013/.
package perf
