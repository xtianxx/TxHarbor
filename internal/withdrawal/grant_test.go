// grant_test.go owns the unit-level T008 tests for the grant-supply core.
// No database is contacted: validation ordering (operation id required) is
// proven by passing a nil pool, so a function that touched the pool before
// validating would panic instead of returning CodeValidationFailed.
package withdrawal

import (
	"context"
	"errors"
	"math"
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
// It is deliberately scopeless: this is the stock/OPEN path, and the whole
// scope extension must leave it valid and unchanged.
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

// grantScopedOp is a fully-populated valid scoped supply op-input (PB-FR-01):
// every scope field plus the server-resolved attested_by is set.
func grantScopedOp() OpInput {
	o := grantValidOp()
	o.IntentID = "intent-1"
	o.RequestID = "request-1"
	o.Sender = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	o.FeeMaxTotal = 21000
	o.FeeMaxPerGas = 2
	o.FeeMaxPriority = 1
	o.AllowsFeeReplacement = true
	o.AttestedBy = "principal-1"
	return o
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

// TestGrantSupplyScopedValid proves a fully-populated scoped op-input passes
// shape validation and its sender is stored in canonical lowercase form.
func TestGrantSupplyScopedValid(t *testing.T) {
	op := grantScopedOp()
	norm, err := validateSupplyOpInput(op, time.Now())
	if err != nil {
		t.Fatalf("validateSupplyOpInput() error = %v, want nil", err)
	}
	if norm.Sender != strings.ToLower(op.Sender) {
		t.Fatalf("normalized sender = %q, want lowercase %q", norm.Sender, strings.ToLower(op.Sender))
	}
	if norm.AttestedBy != op.AttestedBy {
		t.Fatalf("attested_by = %q, want %q", norm.AttestedBy, op.AttestedBy)
	}
}

// TestGrantSupplyScopeInvalidBeforePoolUse proves each malformed scope field
// fails with CodeValidationFailed while the pool is nil (untouched).
func TestGrantSupplyScopeInvalidBeforePoolUse(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		op    OpInput
		field string
	}{
		{"bad sender shape", func() OpInput { o := grantScopedOp(); o.Sender = "0x1234"; return o }(), "sender"},
		{"mixed-case sender checksum fail", func() OpInput {
			o := grantScopedOp()
			o.Sender = badChecksumAddr
			return o
		}(), "sender"},
		{"negative fee max total", func() OpInput { o := grantScopedOp(); o.FeeMaxTotal = -1; return o }(), "fee_max_total"},
		{"negative fee max per gas", func() OpInput { o := grantScopedOp(); o.FeeMaxPerGas = -1; return o }(), "fee_max_per_gas"},
		{"negative fee max priority", func() OpInput { o := grantScopedOp(); o.FeeMaxPriority = -1; return o }(), "fee_max_priority"},
		{"missing attested_by", func() OpInput { o := grantScopedOp(); o.AttestedBy = ""; return o }(), "attested_by"},
		{"scope without sender", func() OpInput { o := grantScopedOp(); o.Sender = ""; return o }(), "sender"},
		{"intent without sender", func() OpInput {
			o := grantValidOp()
			o.IntentID = "intent-1"
			return o
		}(), "sender"},
		{"purpose without sender", func() OpInput {
			o := grantValidOp()
			o.AllowsFeeReplacement = true
			return o
		}(), "sender"},
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
			if e.Field != tc.field {
				t.Fatalf("SupplyGrant() field = %q, want %q", e.Field, tc.field)
			}
		})
	}
}

// TestGrantSupplyFeeTripleValidation is T016: the PB-C2 fee triple on a scoped
// supply. Amounts are native-coin smallest-unit integers, so a negative cap is
// illegal; a scope that carries any fee dimension must carry the applicable
// caps (fee_max_total and fee_max_per_gas present, never "unlimited"); the
// EIP-1559 priority cap MUST NOT exceed the per-gas max_fee cap, while
// fee_max_priority == 0 selects the legacy gas_price path; and an all-zero
// triple is a scope with no fee constraint (pre-T016 shape kept so a scoped
// supply that predates fee scoping stays valid). Each boundary is asserted on
// both sides: at-cap accepted, cap+1 refused.
func TestGrantSupplyFeeTripleValidation(t *testing.T) {
	const maxInt64 = int64(math.MaxInt64)
	now := time.Now()
	cases := []struct {
		name                    string
		total, perGas, priority int64
		wantField               string // "" means valid
	}{
		{"full triple", 21000, 2, 1, ""},
		{"priority at per-gas cap", 10, 10, 10, ""},
		{"priority below per-gas cap", 10, 10, 9, ""},
		{"legacy gas_price path (priority zero)", 10, 10, 0, ""},
		{"native integer upper bound", maxInt64, maxInt64, maxInt64, ""},
		{"all zero is not fee-bearing", 0, 0, 0, ""},
		{"priority one above per-gas cap refused", 10, 10, 11, "fee_max_priority"},
		{"priority above cap with larger total refused", 1000, 10, 11, "fee_max_priority"},
		{"missing total cap refused", 0, 10, 0, "fee_max_total"},
		{"missing per-gas cap refused", 100, 0, 0, "fee_max_per_gas"},
		{"priority present without per-gas cap refused", 100, 0, 1, "fee_max_per_gas"},
		{"negative total refused", -1, 10, 1, "fee_max_total"},
		{"negative per-gas refused", 10, -1, 0, "fee_max_per_gas"},
		{"negative priority refused", 10, 10, -1, "fee_max_priority"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := grantScopedOp()
			op.FeeMaxTotal, op.FeeMaxPerGas, op.FeeMaxPriority = tc.total, tc.perGas, tc.priority

			norm, err := validateSupplyOpInput(op, now)
			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("validateSupplyOpInput() error = %v, want nil", err)
				}
				if norm.FeeMaxTotal != tc.total || norm.FeeMaxPerGas != tc.perGas || norm.FeeMaxPriority != tc.priority {
					t.Fatalf("normalized fee triple = (%d, %d, %d), want (%d, %d, %d)",
						norm.FeeMaxTotal, norm.FeeMaxPerGas, norm.FeeMaxPriority,
						tc.total, tc.perGas, tc.priority)
				}
				return
			}
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("validateSupplyOpInput() error = %v (%T), want *Error", err, err)
			}
			if e.Code != CodeValidationFailed {
				t.Fatalf("error code = %q, want %q", e.Code, CodeValidationFailed)
			}
			if e.Field != tc.wantField {
				t.Fatalf("error field = %q, want %q", e.Field, tc.wantField)
			}
		})
	}
}

// TestGrantScopelessDetailUnchanged proves the stock/OPEN snapshot still
// carries the original eight fields, with the scope segment rendered empty.
func TestGrantScopelessDetailUnchanged(t *testing.T) {
	detail := opInputDetail(grantValidOp())
	for _, key := range []string{
		"action=", "authorization_id=", "caller_id=", "chain_id=",
		"asset=", "recipient=", "amount=", "expires_at=",
	} {
		if !strings.Contains(detail, key) {
			t.Fatalf("scopeless detail missing %q: %q", key, detail)
		}
	}
	if detail == opInputDetail(grantScopedOp()) {
		t.Fatal("scoped and scopeless snapshots must differ")
	}
}

// TestGrantOpInputCompareScopeFields proves every scope field joins the
// same-operation comparison, so a differing scope is operation_conflict.
func TestGrantOpInputCompareScopeFields(t *testing.T) {
	base := grantScopedOp()
	stored := attemptRow{action: grantOutcomeSupplied, authorizationID: base.AuthorizationID, detail: opInputDetail(base)}
	if !stored.opInputMatches(base) {
		t.Fatal("identical scoped op-input must match")
	}
	differ := map[string]OpInput{
		"intent_id":              func() OpInput { o := base; o.IntentID = "intent-2"; return o }(),
		"request_id":             func() OpInput { o := base; o.RequestID = "request-2"; return o }(),
		"sender":                 func() OpInput { o := base; o.Sender = "0xcccccccccccccccccccccccccccccccccccccccc"; return o }(),
		"fee_max_total":          func() OpInput { o := base; o.FeeMaxTotal = 21001; return o }(),
		"fee_max_per_gas":        func() OpInput { o := base; o.FeeMaxPerGas = 3; return o }(),
		"fee_max_priority":       func() OpInput { o := base; o.FeeMaxPriority = 2; return o }(),
		"allows_fee_replacement": func() OpInput { o := base; o.AllowsFeeReplacement = false; return o }(),
		"attested_by":            func() OpInput { o := base; o.AttestedBy = "principal-2"; return o }(),
	}
	for field, op := range differ {
		if stored.opInputMatches(op) {
			t.Errorf("changed %s must NOT match", field)
		}
	}
}
