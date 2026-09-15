// grant_test.go owns the unit-level T008 tests for the grant-supply core.
// No database is contacted: validation ordering (operation id required) is
// proven by passing a nil pool, so a function that touched the pool before
// validating would panic instead of returning CodeValidationFailed.
package withdrawal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestGrantMintOperationIDFormatAndUniqueness asserts the mint contract:
// 32 lowercase hex characters, opaque, and unique across many draws.
func TestGrantMintOperationIDFormatAndUniqueness(t *testing.T) {
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id, err := MintOperationID()
		if err != nil {
			t.Fatalf("MintOperationID() error = %v", err)
		}
		if len(id) != 32 {
			t.Fatalf("MintOperationID() length = %d, want 32 (%q)", len(id), id)
		}
		for _, c := range id {
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("MintOperationID() = %q contains non-hex character %q", id, c)
			}
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("MintOperationID() returned duplicate %q", id)
		}
		seen[id] = struct{}{}
	}
}

// grantValidOp is a minimal valid supply op-input; individual tests mutate it.
func grantValidOp() OpInput {
	return OpInput{
		OperationID:     "0123456789abcdef0123456789abcdef",
		Action:          grantSupplyAction,
		AuthorizationID: "auth-1",
		CallerID:        7001,
		ChainID:         31337,
		Asset:           "0x1111111111111111111111111111111111111111",
		Recipient:       "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Amount:          "100",
	}
}

// TestGrantSupplyValidatesBeforePoolUse proves every invalid supply op-input
// fails with CodeValidationFailed while the pool is nil (untouched).
func TestGrantSupplyValidatesBeforePoolUse(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	past := now.Add(-time.Minute)

	cases := []struct {
		name string
		op   OpInput
	}{
		{"empty operation id", func() OpInput { o := grantValidOp(); o.OperationID = ""; return o }()},
		{"wrong action", func() OpInput { o := grantValidOp(); o.Action = grantRevokeAction; return o }()},
		{"empty authorization id", func() OpInput { o := grantValidOp(); o.AuthorizationID = ""; return o }()},
		{"zero caller id", func() OpInput { o := grantValidOp(); o.CallerID = 0; return o }()},
		{"negative chain id", func() OpInput { o := grantValidOp(); o.ChainID = -1; return o }()},
		{"bad asset", func() OpInput { o := grantValidOp(); o.Asset = "nope"; return o }()},
		{"mixed-case asset checksum fail", func() OpInput {
			o := grantValidOp()
			o.Asset = badChecksumAddr
			return o
		}()},
		{"bad recipient", func() OpInput { o := grantValidOp(); o.Recipient = "0x1234"; return o }()},
		{"zero amount", func() OpInput { o := grantValidOp(); o.Amount = "0"; return o }()},
		{"decimal amount", func() OpInput { o := grantValidOp(); o.Amount = "1.5"; return o }()},
		{"over uint256 amount", func() OpInput {
			o := grantValidOp()
			o.Amount = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
			return o
		}()},
		{"past expires_at", func() OpInput { o := grantValidOp(); o.ExpiresAt = &past; return o }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := SupplyGrant(ctx, nil, tc.op, "op", "reason")
			if out != nil {
				t.Fatalf("SupplyGrant() outcome = %+v, want nil", out)
			}
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("SupplyGrant() error = %v (%T), want *Error", err, err)
			}
			if e.Code != CodeValidationFailed {
				t.Fatalf("SupplyGrant() code = %q, want %q", e.Code, CodeValidationFailed)
			}
		})
	}
}

// TestGrantSupplyAcceptsFutureExpiresAt proves a future expiry passes shape
// validation (the nil pool is reached only after validation, so this asserts
// the validator did not reject it and stopped short of the pool).
func TestGrantSupplyAcceptsFutureExpiresAt(t *testing.T) {
	op := grantValidOp()
	future := time.Now().Add(time.Hour)
	op.ExpiresAt = &future
	if _, err := validateSupplyOpInput(op, time.Now()); err != nil {
		t.Fatalf("validateSupplyOpInput() error = %v, want nil", err)
	}
}

// TestGrantRevokeValidatesBeforePoolUse proves an empty operation id (and an
// empty authorization id) fail with CodeValidationFailed while the pool is nil.
func TestGrantRevokeValidatesBeforePoolUse(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, opID, authID string }{
		{"empty operation id", "", "auth-1"},
		{"empty authorization id", "0123456789abcdef0123456789abcdef", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := RevokeGrant(ctx, nil, tc.opID, tc.authID, "op", "reason")
			if out != nil {
				t.Fatalf("RevokeGrant() outcome = %+v, want nil", out)
			}
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("RevokeGrant() error = %v (%T), want *Error", err, err)
			}
			if e.Code != CodeValidationFailed {
				t.Fatalf("RevokeGrant() code = %q, want %q", e.Code, CodeValidationFailed)
			}
		})
	}
}

// TestGrantOpInputCompareHelper covers the same-operation convergence rule:
// all eight op-input fields must match; operator/reason are retry metadata and
// never participate.
func TestGrantOpInputCompareHelper(t *testing.T) {
	base := grantValidOp()
	baseExpiry := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	base.ExpiresAt = &baseExpiry
	stored := attemptRow{action: grantOutcomeSupplied, authorizationID: base.AuthorizationID, detail: opInputDetail(base)}

	if !stored.opInputMatches(base) {
		t.Fatal("identical op-input must match")
	}

	// Each of the eight bound fields, when changed, breaks the match.
	differ := map[string]OpInput{
		"action":           func() OpInput { o := base; o.Action = grantRevokeAction; return o }(),
		"authorization_id": func() OpInput { o := base; o.AuthorizationID = "auth-2"; return o }(),
		"caller_id":        func() OpInput { o := base; o.CallerID = 2; return o }(),
		"chain_id":         func() OpInput { o := base; o.ChainID = 1; return o }(),
		"asset":            func() OpInput { o := base; o.Asset = "0x2222222222222222222222222222222222222222"; return o }(),
		"recipient":        func() OpInput { o := base; o.Recipient = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; return o }(),
		"amount":           func() OpInput { o := base; o.Amount = "101"; return o }(),
		"expires_at":       func() OpInput { o := base; later := baseExpiry.Add(time.Second); o.ExpiresAt = &later; return o }(),
	}
	for field, op := range differ {
		if stored.opInputMatches(op) {
			t.Errorf("changed %s must NOT match", field)
		}
	}
	// nil versus set expiry is a real difference.
	noExpiry := base
	noExpiry.ExpiresAt = nil
	if stored.opInputMatches(noExpiry) {
		t.Error("expires_at nil must NOT match a set expires_at")
	}

	// Same op-input, different operator/reason → still equal (metadata only).
	other := attemptRow{action: stored.action, authorizationID: stored.authorizationID, detail: stored.detail}
	if !other.opInputMatches(base) {
		t.Error("operator/reason variation must not affect op-input equality")
	}
	if strings.Contains(stored.detail, "operator") || strings.Contains(stored.detail, "reason") {
		t.Fatalf("detail snapshot must exclude retry metadata: %q", stored.detail)
	}
}
