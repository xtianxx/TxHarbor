# Contracts: 010 → 009 Signer Call — 010 Transaction Lifecycle Management

**Branch**: `010-transaction-lifecycle` | **Date**: 2026-09-17 | **Spec**: [spec.md](../spec.md) | **Consumed contract**: `specs/009-signer-service/contracts/api.md` §1–§4, `contracts/gates.md` §1–§3 (read-only; 009 is not redefined here)

010 is an authenticated 009 caller: it submits one structured signing request per attempt identity and
consumes the returned signing facts. 010 holds no keys, performs no signing and has no digest/signing-hash
path (FR-10). 009 never broadcasts (D9), so **no 009 failure can have a chain side effect** — the only
unknown at this boundary is whether the response was received.

## 1. Transport, identity and configuration

- `POST {TXHARBOR_TX_SIGNER_URL}/signer/v1/signing-requests` with
  `Authorization: Bearer {TXHARBOR_TX_SIGNER_CREDENTIAL}` (secret; never logged). 009 derives `caller_id`
  server-side; 010 never sends a caller identity.
- Same-identity retries send the **byte-identical** request body persisted as `tx_attempts.canonical_envelope`;
  009's replay/conflict semantics then apply unchanged (same ID + same content → same persisted result;
  different content → `request_conflict`, which 010 records as `attempt_conflict` and never auto-corrects).
- No other 009 endpoint is used for sending. The status endpoint may be used for operator diagnosis only; a
  status response is never treated as a send authorization (it is desensitized and status-only after
  revocation/expiry/pause, `api.md` §3).

## 2. Request body (exact mapping)

| 009 field | 010 source | Notes |
|---|---|---|
| `signing_request_id` | `tx_attempts.signing_request_id` | pre-allocated, 1:1 with the attempt |
| `attempt_id` | `tx_attempts.attempt_id` | 009 `signing_requests.attempt_id` is UNIQUE |
| `intent_id` | `tx_attempts.intent_id` | identity chain |
| `binding_ref` | `tx_attempts.binding_ref` | 008 binding |
| `recovery_version` | `tx_attempts.recovery_version` | 009 requires equality at delivery |
| `chain_id`, `sender`, `nonce` | attempt columns | decimal strings for nonce |
| `tx_type`, `gas_limit`, fees | attempt columns | closed fee shapes; exact one-of |
| `to`, `value`, `data` | derived: `to = asset`, `value = 0`, `data = transfer(recipient, amount)` | v1 ERC-20 shape |
| `asset`, `recipient`, `amount` | attempt columns | declared triple must equal calldata |
| `authorization_id` | `tx_attempts.authorization_id` | 009 re-reads the grant and applies OC-5 conditional reuse; `authorization_version` is recorded by 009 from the PB scope (H2) |

The body is serialized once, hashed into `tx_attempts.content_hash` (domain-tagged economic projection) and
stored verbatim as `canonical_envelope`; replay never re-serializes from scratch (R-010-02).

## 3. Response handling

| 009 response | Meaning | 010 action |
|---|---|---|
| `200 signed` (`signature` + `tx_hash`) | signature delivered for this attempt identity | reconstruct bytes + verify (R-010-03), persist in T2, then proceed to the send region |
| `409 signature_withheld` / `recovery_*` / `binding_*` / `authorization_*` / `422 policy_refused` / `403` grant classes | 009 refused to sign or to deliver | record `signature_refused` + observed basis; zero send; attempt stays `prepared`; retry only when the basis changes (fresh identity for stale-view content) |
| `409 request_conflict` | same identity, different envelope | record `attempt_conflict`; never overwrite; caller must fix the identity/content |
| `503 key_provider_*` / `storage_*` / `outcome_unknown` / `gate_read_failed` | delivery outcome indeterminate at 009 | **retry the same identity and the same envelope** with bounded backoff; 009 persists and re-delivers identically once the gate passes; no new attempt, no new identity, no chain effect |
| `401` / `403 signing_not_permitted` | credential/permission problem | fail closed; operator fix; never retry blindly |
| malformed/unknown-field errors (`400`) | 010/009 schema drift | fail closed, record; never "fix up" the body silently |

**Delivery-unknown at this boundary** (009 `outcome_unknown`): 010 must not infer that no signature exists.
Because 009 persists the signature result and re-delivers it byte-identically on same-identity retry, the
resolution is deterministic retry — no attempt-level `unknown` state is warranted (there is no chain side
effect). If retries exhaust, the attempt stays `prepared` with the evidence recorded; it is never marked
failed and never re-signed under a new identity for the same attempt.

## 4. Signed-bytes reconstruction (mandatory before any send)

1. Rebuild `types.Transaction` from the persisted attempt content (legacy tx_type 0 or dynamic-fee tx_type 2).
2. Attach the 65-byte signature with `types.LatestSignerForChainID(chain_id)`.
3. `signed_tx_bytes = tx.MarshalBinary()`; `local_hash = keccak256(signed_tx_bytes)`.
4. Require `local_hash == 009.tx_hash`; recover the sender and require it equals `attempts.sender`.
5. Persist `(attempt_id, signature, signed_tx_bytes, tx_hash)` in T2 — **before any dispatch**. Mismatch
   fails closed (`signature_mismatch`) with zero dispatch and a recorded event.

Reconstruction is deterministic and fixed by the persisted content + signature; a fixed-vector unit test
pins it (V2). The persisted bytes — not a re-derivation at send time — are what the send region dispatches
(`eth_sendRawTransaction`), so replay is byte-identical by construction (SC-03).

## 5. Prohibited at this boundary

- No raw-transaction or digest/message submission; no signing-hash field; 009 has no such surface and 010
  must never construct one.
- No key material, signature, or signed bytes in logs, errors or metrics.
- No new attempt/identity on any 009 error; no gate bypass based on a prior 009 success (009's delivery
  gate is not 010's send gate — FR-15's last paragraph).
- No use of the 009 status endpoint as a permit; only the persisted signature result delivered under the
  current 009 gate is consumed.
