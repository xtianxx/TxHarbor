# Contracts: Service Interface — 009 Signer Service

**Branch**: `009-signer-service` | **Date**: 2026-09-16 | **Spec**: [spec.md](../spec.md) | **Research**: [research.md](../research.md) R2/R3/R6/R9

009 exposes exactly two authenticated operations: submit a structured transaction signing
request, and query the status of one's own signing request. It never broadcasts, never calls any
chain/RPC interface, and never returns anything beyond the signing result facts. Transport is
plain JSON over HTTP on the signer's own listener (no new protocol/framework/facility).

## 1. Transport & authentication

- **Process/listener**: `txharbor signer-serve`, `TXHARBOR_SIGNER_HTTP_ADDR`
  (default `127.0.0.1:8091` — non-default port, research R10). Separate process from the business
  `serve`; business paths do not import the provider (research R2).
- **Auth**: `Authorization: Bearer <signer credential>`; caller identity comes from the
  `signer_credential`/`signer_caller` row, never from the body (FR-04). Missing/invalid/revoked →
  generic `401 unauthenticated`; authenticated without `can_sign` → `403 signing_not_permitted`.
  Credentials never appear in responses, logs, or errors.
- **Ownership**: every read/write is scoped to the authenticated `caller_id`. Another caller's or
  nonexistent request is an **identical** `404 not_found` (no existence/ownership leak).
- **Content type**: `application/json`; unknown fields are rejected (MUST NOT silently accept a
  field the signer would not bind).

## 2. `POST /signer/v1/signing-requests` — submit

Request (all fields required; no defaults, no lenient parsing):

```json
{
  "signing_request_id": "sr-7f3a…",
  "attempt_id": "at-9c02…",
  "intent_id": "pi-1b44…",
  "binding_ref": "nb-…",
  "recovery_version": 12,
  "chain_id": 31337,
  "sender": "0x1111111111111111111111111111111111111111",
  "nonce": "42",
  "tx_type": 2,
  "to": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "value": "0",
  "data": "0xa9059cbb000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000000000000000000000000000000000000000000000000000000000f4240",
  "gas_limit": "65000",
  "max_fee_per_gas": "1500000000",
  "max_priority_fee_per_gas": "1000000000",
  "asset": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "recipient": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "amount": "1000000",
  "authorization_id": "wa-…"
}
```

- Amounts/nonces/fees are **decimal strings** (no JSON numbers, no floats — constitution I);
  `data` is `0x`-hex; addresses are lowercase hex or mixed-case that passes EIP-55.
- Fee shape: exactly one of (`gas_price`) or (`max_fee_per_gas` + `max_priority_fee_per_gas`),
  consistent with `tx_type` (`0`/`2` respectively); `max_priority_fee_per_gas <= max_fee_per_gas`.
- `asset` MUST equal `to`; v1 policy requires an ERC-20 `transfer(address,uint256)` calldata whose
  arguments equal `recipient`/`amount`, and `value = "0"` (native currency is gas-only).
- `recovery_version` is the 006 recovery version the content was built under (gates.md §1).
- A body carrying only a digest/hash/message (no complete transaction content) is refused as
  `arbitrary_digest_rejected`; there is no digest/message endpoint and no field that can carry one.

Responses (stable machine code + human message + trace id; no secrets):

| Case | HTTP | Machine code | Body essence |
|---|---|---|---|
| first submit delivered | 200 | `signed` | `signature` + `tx_hash` + state facts + `delivery: delivered` |
| same-identity retry, gate passed | 200 | `signed` | same persisted `signature`/`tx_hash` (identical result) |
| same-identity retry, delivery withheld (expired/revoked auth, 006/008 pause, version change) | 409 | `signature_withheld` | status-only (§3); no signature; reason + observed basis |
| same identity, different envelope | 409 | `request_conflict` | original untouched; new content needs a new identity |
| authenticated, no signing permission | 403 | `signing_not_permitted` | — |
| authorization missing/inactive/expired/revoked/mismatch/changed | 403 | `authorization_invalid` / `authorization_expired` / `authorization_revoked` | fresh authorization required for replacements |
| malformed JSON / unknown field / wrong type | 400 | `malformed_request` | — |
| arbitrary digest / missing required content / field validation | 422 | `arbitrary_digest_rejected` / `validation_failed` (+ field) | — |
| policy refusal (chain/sender/asset/recipient/amount/fee) | 422 | `policy_refused` (`policy_*` detail class) | — |
| 006 gate refusal (pause/active recovery) | 409 | `recovery_paused` / `recovery_active` | not queued; retry after release with same identity |
| content built on stale recovery version | 409 | `recovery_version_changed` | reconstruct content on the new view (new identity) |
| 008 binding absent/conflict | 409 | `binding_absent` / `binding_conflict` | — |
| 008 binding paused/reconciling | 409 | `binding_paused` | retry after release (same identity) |
| missing/invalid credential | 401 | `unauthenticated` | generic |
| key provider unavailable/timeout | 503 | `key_provider_unavailable` / `key_provider_timeout` | retry **same identity** |
| storage/commit unavailable or unknown | 503 | `storage_unavailable` / `outcome_not_yet_visible` | retry **same identity**; never "signed" |
| delivery outcome indeterminate | 503 | `outcome_unknown` | status-only + reconcile instruction |
| gate read failed/indeterminate | 503 | `gate_read_failed` / `binding_read_failed` | retry **same identity** |

Delivery hygiene: the only success body that carries signing material is `signed`; it contains
the signature and transaction hash, **never** raw signed-transaction bytes or key material. The
service never broadcasts and has no RPC dependency (FR-16; SC-06).

## 3. `GET /signer/v1/signing-requests/{signing_request_id}` — status

- `401` unauthenticated; other callers' or nonexistent ids → **identical** `404 not_found`.
- **Desensitized status** (OC-6: usable after authorization expiry/revocation/pause, first-class
  on withheld submissions): always includes `signing_request_id`, `state`
  (`received|validated|signed|rejected|failed`), `content_hash`, `attempt_id`, `intent_id`,
  `policy_version`, `created_at`, `updated_at`, `delivery` (last admission verdict + timestamp),
  and the last recorded `reason`/`refusal_class` with observed basis (`pause_basis`,
  `recovery_basis`, authorization state) when blocked.
- Signature bytes are **never** returned by status. `tx_hash` is returned only when an admission
  row is recorded `delivered`/`admitted` (the material was already cleared for delivery); after
  revocation/expiry/pause the status is status-only, regardless of the persisted result — this is
  the explicit inversion of the 007 still-200 replay (OC-6).
- Status never triggers signing, gate bypass, or delivery; it is a read of recorded facts plus
  the latest admission row.

## 4. Request processing order (fixed)

1. Authentication (`401`) and permission (`403`).
2. Body shape: parse strict JSON, reject unknown fields (`400`); reject digest-only requests
   (`422 arbitrary_digest_rejected`); field semantics (`422 validation_failed`).
3. Identity lookup: same `(caller_id, signing_request_id)` → envelope equality → replay path
   (persisted result through delivery) or `409 request_conflict`. In-flight/unknown result →
   `503 outcome_not_yet_visible` (same-identity retry).
4. First receipt: submit transaction (gates → binding → authorization → policy → sign → persist,
   persistence.md §1). No gate is read before authentication and shape checks.
5. Delivery assessment (persistence.md §2) → response.
Replay does not re-run policy (deterministic), but delivery re-runs the revocable gates
(006/007/008) on every attempt.

## 5. Operator lifecycle surface (no HTTP)

Credential issuance/rotation/revocation is operator-only: `txharbor signer-auth
<issue|rotate|revoke>` (research R9; mirrors `apikey-auth`). No HTTP admin endpoint exists.
