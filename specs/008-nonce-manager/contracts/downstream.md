# Contract: 008 → 010/011 Downstream Inputs (no future specs created)

**Branch**: `008-nonce-manager` | **Date**: 2026-09-16 | **Spec**: [spec.md](spec.md)
(OC-1/OC-2/OC-4, Downstream Handoff) | **Design**: research R2/R3/R10/R12.

008 states what it needs from, and guarantees to, its downstream callers 010/011. It creates **no**
010/011 specification, table, or state machine, and claims no bilateral conformance. Real
integration acceptance with 010/011 is explicitly deferred (Section 4).

## 1. Allocation input contract (caller: 011 execution admission, OC-1/OC-2/OC-5)

The caller submits one allocation request per already-persisted payment intent:

| Field | Form | Caller obligation | Provider behavior |
|---|---|---|---|
| `intent_id` | opaque 1–128 printable | persist the intent **before** requesting allocation (OC-1); one intent ⇒ at most one binding | replay on repeat; conflict on any differing input; never creates an intent |
| `chain_id` | decimal > 0 | fixed at admission; reuse on retry/replacement (OC-2) | must equal deployment chain; else fail-closed |
| `sender` | lowercase `0x`+40hex | fixed at admission from the authority (OC-2); a caller claim is never authoritative | independently verified against `nonce_wallet_registry` (active) |
| `authorization_id` | opaque 1–128 printable | supply the current per-transaction authorization (OC-5) | read-only validation against 007 `withdrawal_authorizations`: exists, `state='active'`, not expired by DB clock, chain matches; identity + version digest recorded on the binding |

Provider responses (machine outcomes; stable strings): `allocated` (new binding), `replayed`
(original binding, input equality), `allocation_conflict` (same intent, differing input),
`chain_view_unavailable`, `scope_held` (with hold causes), `sender_not_registered`,
`sender_disabled`, `authorization_invalid`, `rebuild_incomplete`, `recovery_active`
(006 pause precedent applied), `temporarily_unavailable`. All non-`allocated` outcomes are
fail-closed and record their cause; none creates or mutates a binding.

Retry rules mirror 007: retry with the **same** input set (same intent, same scope, same
authorization id); never re-mint an intent or rotate the authorization to force progress; a
retry MUST NOT consume or extend the authorization (008 never writes authorization rows).

## 2. Binding identity handed downstream (OC-4 linkage)

- The durable binding identity is `binding_id` (stable, opaque, never reused). 010's attempt rows
  (`attempt_id`, `signing_request_id`, full content) link to the intent and to this `binding_id`;
  one attempt = one signing request identity; retries reuse identity and content; same identity
  with different content is refused downstream.
- 008 does not read or write 010 tables; it never creates attempt rows. The read API
  (`read-api.md`) serves `(intent_id, chain_id, sender, nonce, state)` plus the authorization
  identity/version and gate annotations.
- Fee replacement: a new `attempt_id`/`signing_request_id` under the **same** intent and the
  **same** binding; if replacement requires an authorization that explicitly permits it, the
  caller supplies it (OC-5); 008 binds the authorization presented at admission and never
  silently rebinds a different authorization.
- Same nonce never maps to a second intent; replacement/replay attempts never create a second
  intent claim (structural: nonce immutability + `nonce_bindings_scope_nonce_uniq`).

## 3. For 009 (reference only)

009 consumes the read-only provider contract in `read-api.md`: five outcomes
(`bound`/`terminal`/`not_bound`/`mismatch`/`unavailable`), facts separated from execution
permission, single-snapshot consistency, bearer-permission boundary. 009's client obligations are
009's plan; this file creates none.

## 4. Deferred integration acceptance (explicit)

Deferred until the dependency exists (orchestrator instruction; workflow R5):

1. **Intent existence / linkage** — 011's intent table does not exist; 008 treats `intent_id` as
   an opaque stable identity and defers any FK/existence cross-check and the
   intent↔request↔authorization linkage validation to the real 011 integration.
2. **Attempt-level state refinement** — 010's attempt lifecycle does not exist; 008's binding
   states are derived from chain evidence only (`allocated`/`in_flight`/`consumed`); a future
   integration may read attempt linkage read-only to refine evidence, but no such dependency is
   assumed by this plan.
3. **009 client and gate composition** — the read client, retry policy, and composition with 009's
   own signing/recovery gates.
4. **End-to-end acceptance** — "API request → queue → nonce allocation → signing → broadcast →
   confirmation" cannot be accepted until the full chain exists; 008's own acceptance is scoped
   to its admission/reconcile behavior (quickstart.md).

## 5. Fixture scheme for now (tests may not wait for 010/011)

Fixtures build valid inputs from tables that already exist, plus test-authored identities; no
fixture invents upstream behavior:

- **Intent**: a test-authored opaque `intent_id` string; stored nowhere upstream (no 011 table),
  exercised against 008's UNIQUE/intent-replay carriers. The deferred existence check is recorded
  as a known gap, not simulated as reality.
- **Sender**: insert `nonce_wallet_registry` via the operator command (or an equivalent
  fixture-only SQL path in tests, marked non-production) with a known address; Anvil is funded at
  that address by the test harness.
- **Authorization**: rows in 007's existing carrier — `caller` + `withdrawal_authorizations`
  (`state='active'`, future `expires_at`, chain 31337) — optionally linked to a
  `withdrawal_requests` row via `authorization_id`; expiry/revocation fixtures use the same table.
- **Chain state**: Anvil transactions from the sender key create real `latest`/`pending` counts;
  fault injection uses an RPC proxy (transport/timeout/rate-limit/divergent responses), reusing
  the 006 integration harness pattern; testcontainers self-provision PostgreSQL and Anvil.
- **Isolation**: workdir-local names/ports only (quickstart.md §Environment); never the shared
  compose `pgdata`.

Test doubles (fake RPC, scripted counts) are allowed for early development only; final
concurrency/restart/recovery acceptance MUST run against real PostgreSQL + real Anvil (原则 XI;
workflow R5).
