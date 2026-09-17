package signer

import (
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestBindingRefusalMatrix(t *testing.T) {
	cases := []struct {
		res   BindingResult
		err   error
		class RefusalClass
	}{
		{BindingMatches, nil, ""},
		{BindingAbsent, nil, ClassBindingAbsent},
		{BindingConflict, nil, ClassBindingConflict},
		{BindingPaused, nil, ClassBindingPaused},
		{BindingTerminal, nil, ClassBindingTerminal},
		{BindingReadFailed, nil, ClassBindingReadFailed},
		{BindingMatches, errors.New("boom"), ClassBindingReadFailed},
		{BindingResult(99), nil, ClassBindingReadFailed},
	}
	for _, tc := range cases {
		if got := BindingRefusal(tc.res, tc.err); got != tc.class {
			t.Fatalf("BindingRefusal(%d, %v) = %q, want %q", tc.res, tc.err, got, tc.class)
		}
	}
}

func TestRecoveryGateEvaluate(t *testing.T) {
	open := &RecoveryGate{}
	if got := open.Evaluate(0); got != "" {
		t.Fatalf("open gate = %q, want pass", got)
	}
	paused := &RecoveryGate{IndexerPaused: true}
	if got := paused.Evaluate(0); got != ClassRecoveryPaused {
		t.Fatalf("paused gate = %q", got)
	}
	if bases := paused.PauseBases(); len(bases) != 1 || bases[0] != "indexer_pause" {
		t.Fatalf("bases = %v", bases)
	}
	active := &RecoveryGate{HasRecovery: true, RecoveryPhase: "replaying", RecoverySeq: 4}
	if got := active.Evaluate(4); got != ClassRecoveryActive {
		t.Fatalf("active recovery = %q", got)
	}
	stale := &RecoveryGate{HasEventsMax: true, EventsMax: 9}
	if got := stale.Evaluate(7); got != ClassRecoveryVersionChanged {
		t.Fatalf("stale version = %q", got)
	}
	if got := stale.Evaluate(9); got != "" {
		t.Fatalf("current version = %q, want pass", got)
	}
	if v := active.CurrentVersion(); v != 4 {
		t.Fatalf("active version = %d, want 4", v)
	}
}

func TestEvaluateGrant(t *testing.T) {
	req := mustDecode(t, validBody())
	future := time.Now().Add(time.Hour).UTC()
	grant := &AuthzGrant{
		CallerID: 7, ChainID: 31337,
		Asset:     "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Recipient: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Amount:    big.NewInt(1000000), State: "active", ExpiresAt: &future,
	}
	if got := EvaluateGrant(true, grant, 7, &req, time.Now().UTC()); got != "" {
		t.Fatalf("valid grant = %q, want pass", got)
	}
	if got := EvaluateGrant(false, grant, 7, &req, time.Now().UTC()); got != ClassAuthorizationInvalid {
		t.Fatalf("absent grant = %q", got)
	}
	revoked := *grant
	revoked.State = "revoked"
	if got := EvaluateGrant(true, &revoked, 7, &req, time.Now().UTC()); got != ClassAuthorizationRevoked {
		t.Fatalf("revoked grant = %q", got)
	}
	expired := *grant
	past := time.Now().Add(-time.Hour).UTC()
	expired.ExpiresAt = &past
	if got := EvaluateGrant(true, &expired, 7, &req, time.Now().UTC()); got != ClassAuthorizationExpired {
		t.Fatalf("expired grant = %q", got)
	}
	if got := EvaluateGrant(true, grant, 8, &req, time.Now().UTC()); got != ClassAuthorizationInvalid {
		t.Fatalf("caller mismatch = %q", got)
	}
	changed := *grant
	changed.Amount = big.NewInt(999)
	if got := EvaluateGrant(true, &changed, 7, &req, time.Now().UTC()); got != ClassAuthorizationInvalid {
		t.Fatalf("amount mismatch = %q", got)
	}
	fp1 := grant.Fingerprint()
	fp2 := grant.Fingerprint()
	if fp1 != fp2 || !strings.HasPrefix(fp1, "authz:v1:") {
		t.Fatalf("fingerprint not deterministic: %q", fp1)
	}
	if changed.Fingerprint() == fp1 {
		t.Fatalf("fingerprint insensitive to amount")
	}
}

func TestGateSQLShapes(t *testing.T) {
	for _, q := range []string{GateReadSQL, GrantReadSQL, GrantScopeReadSQL, ScopeShareSQL} {
		upper := strings.ToUpper(strings.TrimSpace(q))
		if !strings.HasPrefix(upper, "SELECT") {
			t.Fatalf("gate read is not SELECT-only: %.40q", q)
		}
	}
	for _, q := range []string{GrantReadSQL, GrantScopeReadSQL, ScopeShareSQL} {
		if !strings.Contains(strings.ToUpper(q), "FOR SHARE") {
			t.Fatalf("gate read lacks FOR SHARE: %.40q", q)
		}
	}
	if !strings.Contains(GateLockSQL, "IN SHARE MODE") {
		t.Fatalf("gate lock is not a SHARE lock")
	}
}

func TestEvaluateGrantScope(t *testing.T) {
	req := mustDecode(t, validBody())
	legacyBody := strings.Replace(validBody(), `"tx_type": 2`, `"tx_type": 0`, 1)
	legacyBody = strings.Replace(legacyBody, `"max_fee_per_gas": "1500000000",`, `"gas_price": "1000000000",`, 1)
	legacyBody = strings.Replace(legacyBody, `"max_priority_fee_per_gas": "1000000000",`, ``, 1)
	legacy := mustDecode(t, legacyBody)

	// validBody: 65000 gas × 1.5 gwei max fee cap (total 9.75e13), 1 gwei tip.
	scoped := GrantScope{
		Present:              true,
		AuthorizationID:      req.AuthorizationID,
		IntentID:             req.IntentID,
		RequestID:            req.SigningRequestID,
		Sender:               req.Sender,
		FeeMaxTotal:          97500000000000,
		FeeMaxPerGas:         1500000000,
		FeeMaxPriority:       1000000000,
		AllowsFeeReplacement: true,
	}
	with := func(mutate func(*GrantScope)) GrantScope {
		s := scoped
		mutate(&s)
		return s
	}

	cases := []struct {
		name  string
		scope GrantScope
		req   *Request
		want  RefusalClass
	}{
		{"absent carrier", GrantScope{}, &req, ClassAuthorizationUnverifiable},
		{"carrier covers the request at the caps", scoped, &req, ""},
		{"no fee constraint", with(func(s *GrantScope) { s.FeeMaxTotal, s.FeeMaxPerGas, s.FeeMaxPriority = 0, 0, 0 }), &req, ""},
		{"legacy request on the legacy path", with(func(s *GrantScope) {
			s.FeeMaxTotal, s.FeeMaxPerGas, s.FeeMaxPriority = 65000000000000, 1000000000, 0
		}), &legacy, ""},
		{"authorization_id mismatch", with(func(s *GrantScope) { s.AuthorizationID = "wa-2" }), &req, ClassAuthorizationInvalid},
		{"sender mismatch", with(func(s *GrantScope) { s.Sender = "0x2222222222222222222222222222222222222222" }), &req, ClassAuthorizationInvalid},
		{"intent mismatch", with(func(s *GrantScope) { s.IntentID = "pi-other" }), &req, ClassAuthorizationInvalid},
		{"request_id mismatch", with(func(s *GrantScope) { s.RequestID = "sr-other" }), &req, ClassAuthorizationInvalid},
		{"total one over the cap", with(func(s *GrantScope) { s.FeeMaxTotal = 97499999999999 }), &req, ClassAuthorizationInvalid},
		{"per-gas one over the cap", with(func(s *GrantScope) { s.FeeMaxPerGas = 1499999999 }), &req, ClassAuthorizationInvalid},
		{"priority one over the cap", with(func(s *GrantScope) { s.FeeMaxPriority = 999999999 }), &req, ClassAuthorizationInvalid},
		{"missing priority cap on a 1559 request", with(func(s *GrantScope) { s.FeeMaxPriority = 0 }), &req, ClassAuthorizationInvalid},
		{"missing total cap", with(func(s *GrantScope) { s.FeeMaxTotal = 0 }), &req, ClassAuthorizationInvalid},
		{"missing per-gas cap", with(func(s *GrantScope) { s.FeeMaxPerGas, s.FeeMaxPriority = 0, 0 }), &req, ClassAuthorizationInvalid},
		{"illegal priority cap above per-gas", with(func(s *GrantScope) { s.FeeMaxPriority = 2000000000 }), &req, ClassAuthorizationInvalid},
	}
	for _, tc := range cases {
		if got := EvaluateGrantScope(tc.scope, tc.req); got != tc.want {
			t.Fatalf("EvaluateGrantScope(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
