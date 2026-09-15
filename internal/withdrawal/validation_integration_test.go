//go:build integration

// validation_integration_test.go owns T019 (US3): the V4 illegal-parameter
// matrix against a real PostgreSQL container. Every case drives the frozen
// SubmitWithdrawal surface with a valid key and an active grant seeded through
// the reviewed IssueKey / SupplyGrant paths, and asserts the step-3 contract
// outcome (422 validation_failed) with ZERO withdrawal_requests rows.
//
// Post-T014 the core returns a response-first AuditIntent for a 422 instead of
// writing a Table 5 row itself; these tests therefore assert the intent is
// present and that the DB stayed untouched (a nil-intent 422 is a regression).
//
// The matrix additionally proves data-model.md §四 layered amount enforcement
// in both directions: "1.5"/"1e3" die at the shape layer (layer 1), max+1
// (= 2²⁵⁶) dies at the Go big.Int range check (layer 2) with zero rows — i.e.
// it never reaches the DB CHECK (layer 3) — while max-uint256 persists 201. A
// legal EIP-55 mixed-case address is included as the positive counterpart so
// the matrix cannot pass by over-rejecting every address.
//
// It reuses the T006 container/migration helper (grantSetup), the T008 supply
// helper (intakeSupplyGrant) and the T010 request/key/count helpers
// (intakeKey, intakeReq, intakeRequestCount); only validation-prefixed helpers
// are added, so no name collides with the existing integration test files.
package withdrawal

import (
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// validationNonWhitelistedAsset is a shape-valid canonical address that is NOT
// in the seeded allowlist.
const validationNonWhitelistedAsset = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// validationWantRejected asserts the step-3 outcome: 422 validation_failed,
// no request id, and the response-first `rejected` audit intent the transport
// persists after the response (never a Table 5 row written by the core).
func validationWantRejected(t *testing.T, res *SubmitResult, callerID int64) {
	t.Helper()
	if res == nil {
		t.Fatal("SubmitWithdrawal returned a nil result")
	}
	if res.Status != 422 || res.Code != CodeValidationFailed {
		t.Fatalf("result = %+v, want 422/%s", res, CodeValidationFailed)
	}
	if res.RequestID != "" {
		t.Fatalf("RequestID = %q, want empty on a 422", res.RequestID)
	}
	if res.Audit == nil {
		t.Fatalf("result %+v has a nil Audit; a 422 must carry a rejected intent", res)
	}
	if res.Audit.CallerID != callerID || res.Audit.Action != auditActionRejected || res.Audit.RequestID != "" {
		t.Fatalf("Audit = %+v, want {caller %d request \"\" action %q}",
			res.Audit, callerID, auditActionRejected)
	}
}

// TestWithdrawalValidationMatrix is the V4 illegal matrix: every illegal shape
// returns 422 with a reject intent and persists zero request rows. Counts are
// compared as before/after deltas so the assertion is per-case, not just an
// end-of-test tally.
func TestWithdrawalValidationMatrix(t *testing.T) {
	ctx, pool := grantSetup(t)
	const callerID = int64(7201)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-validation", intakeAmount)

	// eip55Fail is the published EIP-55 vector with one letter re-cased: still
	// mixed-case (so the checksum branch runs) but no longer checksum-valid.
	const eip55Fail = "0x5Aaeb6053F3E94C9b9A09f33669435E7Ef1BeAed"

	cases := []struct {
		name   string
		mutate func(*SubmitRequest)
	}{
		{"chain_id zero", func(r *SubmitRequest) { r.ChainID = 0 }},
		{"chain_id negative", func(r *SubmitRequest) { r.ChainID = -1 }},
		{"chain_id mismatch", func(r *SubmitRequest) { r.ChainID = intakeChainID + 1 }},
		{"asset not whitelisted", func(r *SubmitRequest) { r.Asset = validationNonWhitelistedAsset }},
		{"asset empty allowlist", func(r *SubmitRequest) { r.Allowlist = nil }},
		{"asset short", func(r *SubmitRequest) { r.Asset = "0x1234" }},
		{"asset non-hex", func(r *SubmitRequest) { r.Asset = "0x" + strings.Repeat("zz", 20) }},
		{"recipient short", func(r *SubmitRequest) { r.Recipient = "0x1234" }},
		{"recipient non-hex", func(r *SubmitRequest) { r.Recipient = "0x" + strings.Repeat("zz", 20) }},
		{"recipient eip55 fail", func(r *SubmitRequest) { r.Recipient = eip55Fail }},
		{"amount zero", func(r *SubmitRequest) { r.Amount = "0" }},
		{"amount leading zeros", func(r *SubmitRequest) { r.Amount = "00123" }},
		{"amount plus sign", func(r *SubmitRequest) { r.Amount = "+123" }},
		{"amount negative", func(r *SubmitRequest) { r.Amount = "-5" }},
		{"amount decimal point", func(r *SubmitRequest) { r.Amount = "1.5" }},
		{"amount exponent", func(r *SubmitRequest) { r.Amount = "1e3" }},
		{"amount non-numeric", func(r *SubmitRequest) { r.Amount = "abc" }},
		{"amount empty", func(r *SubmitRequest) { r.Amount = "" }},
		{"amount whitespace", func(r *SubmitRequest) { r.Amount = " " }},
		{"idempotency empty", func(r *SubmitRequest) { r.IdempotencyKey = "" }},
		{"idempotency 129 bytes", func(r *SubmitRequest) { r.IdempotencyKey = strings.Repeat("a", 129) }},
		{"idempotency space", func(r *SubmitRequest) { r.IdempotencyKey = "has space" }},
		{"idempotency non-ascii", func(r *SubmitRequest) { r.IdempotencyKey = "café" }},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a valid active grant and one request mutated to an illegal shape.
			req := intakeReq(key, fmt.Sprintf("idem-validation-%d", i), "auth-validation")
			tc.mutate(&req)

			// When the illegal request is submitted.
			before := intakeRequestCount(t, ctx, pool)
			res, err := SubmitWithdrawal(ctx, pool, req)

			// Then it is a 422 reject intent and no row was persisted.
			if err != nil {
				t.Fatalf("SubmitWithdrawal(%s): unexpected error %v", tc.name, err)
			}
			validationWantRejected(t, res, callerID)
			if after := intakeRequestCount(t, ctx, pool); after != before {
				t.Fatalf("request rows changed %d -> %d, want 0 (no persistence)", before, after)
			}
		})
	}
}

// TestWithdrawalValidationDecimalBarrier pins §四 layer 1: the transport shapes
// "1.5" and "1e3" are rejected before any parse — they can never be rounded to
// an integer — and persist zero rows.
func TestWithdrawalValidationDecimalBarrier(t *testing.T) {
	ctx, pool := grantSetup(t)
	const callerID = int64(7202)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-decimal", intakeAmount)

	for i, raw := range []string{"1.5", "1e3"} {
		t.Run(raw, func(t *testing.T) {
			// Given a valid grant and a fractional/exponent transport shape.
			req := intakeReq(key, fmt.Sprintf("idem-decimal-%d", i), "auth-decimal")
			req.Amount = raw

			// When submitted.
			before := intakeRequestCount(t, ctx, pool)
			res, err := SubmitWithdrawal(ctx, pool, req)
			if err != nil {
				t.Fatalf("SubmitWithdrawal(%q): unexpected error %v", raw, err)
			}

			// Then it dies at the shape layer (field=amount), never as a value.
			validationWantRejected(t, res, callerID)
			if !strings.Contains(res.Audit.Detail, "field=amount") {
				t.Fatalf("Audit.Detail = %q, want the amount shape rejection", res.Audit.Detail)
			}
			if after := intakeRequestCount(t, ctx, pool); after != before {
				t.Fatalf("shape %q persisted a row (%d -> %d); decimals must die at parse", raw, before, after)
			}
		})
	}
}

// TestWithdrawalValidationAmountBoundaries proves §四 in both directions:
// max-uint256 is accepted end-to-end (layer 3 admits it), and max+1 (= 2²⁵⁶) is
// rejected at the Go big.Int range check (layer 2) with zero rows — proving it
// never reached the DB CHECK (layer 3).
func TestWithdrawalValidationAmountBoundaries(t *testing.T) {
	ctx, pool := grantSetup(t)

	// Direction 1: max-uint256 accepted and persisted byte-identically.
	const callerMax = int64(7203)
	keyMax := intakeKey(t, ctx, pool, callerMax)
	intakeSupplyGrant(t, ctx, pool, callerMax, "auth-max-ok", withdrawalMaxUint256)
	reqMax := intakeReq(keyMax, "idem-max-ok", "auth-max-ok")
	reqMax.Amount = withdrawalMaxUint256

	resMax, err := SubmitWithdrawal(ctx, pool, reqMax)
	if err != nil {
		t.Fatalf("max-uint256 SubmitWithdrawal: %v", err)
	}
	if resMax.Status != 201 || resMax.RequestID == "" {
		t.Fatalf("max-uint256 result = %+v, want 201 with a request id", resMax)
	}
	if got := intakeAmountOf(t, ctx, pool, resMax.RequestID); got != withdrawalMaxUint256 {
		t.Fatalf("persisted max amount = %q, want byte-identical %q", got, withdrawalMaxUint256)
	}

	// Direction 2: max+1 rejected at layer 2 with zero rows.
	const callerOver = int64(7204)
	keyOver := intakeKey(t, ctx, pool, callerOver)
	intakeSupplyGrant(t, ctx, pool, callerOver, "auth-max-over", intakeAmount)

	over := new(big.Int).Lsh(big.NewInt(1), 256).String() // 2²⁵⁶ = max+1
	reqOver := intakeReq(keyOver, "idem-max-over", "auth-max-over")
	reqOver.Amount = over

	before := intakeRequestCount(t, ctx, pool)
	resOver, err := SubmitWithdrawal(ctx, pool, reqOver)
	if err != nil {
		t.Fatalf("max+1 SubmitWithdrawal: %v", err)
	}
	validationWantRejected(t, resOver, callerOver)
	if !strings.Contains(resOver.Audit.Detail, "uint256 maximum") {
		t.Fatalf("Audit.Detail = %q, want the Go range-check rejection (layer 2), not a DB CHECK", resOver.Audit.Detail)
	}
	if after := intakeRequestCount(t, ctx, pool); after != before {
		t.Fatalf("max+1 persisted a row (%d -> %d); it must die in Go, never at the DB CHECK", before, after)
	}
}

// TestWithdrawalValidationEIP55MixedCaseAccepted is the positive counterpart to
// the address matrix: a genuinely mixed-case EIP-55 checksum-valid recipient is
// accepted and stored in canonical lowercase, so the matrix cannot pass merely
// by rejecting every address.
func TestWithdrawalValidationEIP55MixedCaseAccepted(t *testing.T) {
	ctx, pool := grantSetup(t)
	const callerID = int64(7205)
	key := intakeKey(t, ctx, pool, callerID)
	intakeSupplyGrant(t, ctx, pool, callerID, "auth-eip55", intakeAmount)

	// Given the EIP-55 checksummed form of the grant-bound recipient.
	mixed := common.HexToAddress(intakeRecipient).Hex()
	if mixed == strings.ToLower(mixed) || mixed == strings.ToUpper(mixed) {
		t.Fatalf("checksum form %q is not mixed-case; the EIP-55 branch is not exercised", mixed)
	}

	req := intakeReq(key, "idem-eip55", "auth-eip55")
	req.Recipient = mixed

	// When submitted.
	before := intakeRequestCount(t, ctx, pool)
	res, err := SubmitWithdrawal(ctx, pool, req)
	if err != nil {
		t.Fatalf("EIP-55 SubmitWithdrawal: %v", err)
	}

	// Then it is accepted, persisted once, and stored lowercase.
	if res.Status != 201 || res.RequestID == "" {
		t.Fatalf("EIP-55 result = %+v, want 201 with a request id", res)
	}
	if after := intakeRequestCount(t, ctx, pool); after != before+1 {
		t.Fatalf("request rows = %d, want %d after one accepted create", after, before+1)
	}
	var stored string
	if err := pool.QueryRow(ctx,
		`SELECT recipient FROM withdrawal_requests WHERE request_id = $1`, res.RequestID).Scan(&stored); err != nil {
		t.Fatalf("read persisted recipient: %v", err)
	}
	if stored != intakeRecipient {
		t.Fatalf("persisted recipient = %q, want canonical lowercase %q", stored, intakeRecipient)
	}
}
