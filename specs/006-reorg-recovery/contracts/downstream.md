# Downstream Handoff Contract: 006 → 007–011

**Branch**: `006-reorg-recovery` | **Date**: 2026-09-14 | **Spec**: FR-26, Downstream Handoff (Q3-approved)

007–011 do not exist yet. This contract states **check obligations** downstream stages must
implement in their own plans. It creates no future business tables, services, or state machines,
and no bilateral conformance is claimed.

## Preconditions (check before each gated action)

Before any broadcast, signing, nonce allocation, or confirmation, the stage MUST observe,
in one read sequence:
1. No `indexer_pause` row for the chain (existing gate — reused, not new).
2. No active `reorg_recovery` row for the chain, or active row in a released-terminal phase only.
3. Its own stage gates (policy versions, independent pauses).

Failure of (1) or (2) → refuse the action, record the refusal cause, do not queue-for-later
as if approved. 007 request intake is the only exception: authenticated, authorized,
parameter-validated idempotent persistence (receive-only; MUST NOT trigger nonce/sign/broadcast).

## In-flight unknowns (emitted before pause took effect)

- Retain `unknown`: keep querying the upstream, record evidence, never re-pay on presumed failure.
- Never claim a database check recalls an emitted RPC — isolation mechanics belong to the
  respective 010/011 plans; 006 only sets the boundary.
- 011 MUST revise results tracking the original payment intent; creating a new payment to
  "compensate" a reorg effect is forbidden (including unknown-outcome cases).

## What 006 guarantees to downstream

- Pause rows + recovery row are durable and restart-persistent; release is atomic with re-verification.
- Ordinary-path backstop (R1) means downstream cannot observe a "recovered" chain while 006 still
  holds it, even if pause rows are tampered with.
- Query validity annotation (observability.md) lets downstream distinguish current-effective from
  historical-confirmed results.
