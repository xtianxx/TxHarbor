// Package faultdrill is the 013 fault-injection drill harness (T073; FR-28;
// verification.md §3–§4). It is test support only and never enters a
// production build:
//
//   - this file carries no build tag, so `go build ./...` (and ordinary unit
//     runs) compile the package with only its documentation — the B8 exit
//     prerequisite;
//   - harness.go and every drill test carry the `fault` build tag, so the
//     fault layer is built and run only through `make test-fault` and never in
//     a normal PR;
//   - the harness drives the real PostgreSQL/Redis/Kafka test dependencies
//     (internal/testutil) and the real 013 runtimes; it never substitutes a
//     hand-called state function for a dependency.
//
// Single-writer chain: T073 creates this skeleton (doc.go + harness.go);
// T019 (B9) extends harness.go with the five-state matrix orchestration that
// T020–T025/T080 depend on.
package faultdrill
