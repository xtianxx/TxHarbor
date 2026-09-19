package signer

import (
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
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

func TestEvaluateGrantScopeBoundIdentity(t *testing.T) {
	req := mustDecode(t, validBody())

	// The carrier binds the anchor's request identity. A fee replacement has a
	// new signing identity, so the carrier must be evaluated against the
	// anchor's identity (V13-4b) — and must still be rejected when evaluated
	// against any other identity.
	scoped := GrantScope{
		Present: true, AuthorizationID: req.AuthorizationID,
		IntentID: req.IntentID, RequestID: "sr-anchor",
		Sender:               req.Sender,
		FeeMaxTotal:          97500000000000,
		FeeMaxPerGas:         1500000000,
		FeeMaxPriority:       1000000000,
		AllowsFeeReplacement: true,
	}
	if got := EvaluateGrantScopeBoundIdentity(scoped, &req, "sr-anchor"); got != "" {
		t.Fatalf("anchor-bound carrier = %q, want pass", got)
	}
	if got := EvaluateGrantScopeBoundIdentity(scoped, &req, req.SigningRequestID); got != ClassAuthorizationInvalid {
		t.Fatalf("carrier evaluated against the replacement identity = %q, want %q", got, ClassAuthorizationInvalid)
	}
	wrongIntent := scoped
	wrongIntent.IntentID = "pi-other"
	if got := EvaluateGrantScopeBoundIdentity(wrongIntent, &req, "sr-anchor"); got != ClassAuthorizationInvalid {
		t.Fatalf("cross-intent carrier = %q, want %q", got, ClassAuthorizationInvalid)
	}
	wrongSender := scoped
	wrongSender.Sender = "0x2222222222222222222222222222222222222222"
	if got := EvaluateGrantScopeBoundIdentity(wrongSender, &req, "sr-anchor"); got != ClassAuthorizationInvalid {
		t.Fatalf("sender-mismatched carrier = %q, want %q", got, ClassAuthorizationInvalid)
	}
	if got := EvaluateGrantScopeBoundIdentity(GrantScope{}, &req, "sr-anchor"); got != ClassAuthorizationUnverifiable {
		t.Fatalf("absent carrier = %q, want %q", got, ClassAuthorizationUnverifiable)
	}
}

func TestEvaluateAnchorBinding(t *testing.T) {
	req := mustDecode(t, validBody())
	anchor := ReplacementAnchor{
		RowID: 7, SigningRequestID: "sr-anchor",
		ChainID: int64(req.ChainID), Sender: req.Sender, Nonce: req.Nonce,
		TxType: int(req.TxType), To: req.To, Value: req.Value,
		Data: common.FromHex(req.Data), GasLimit: req.GasLimit,
		Asset: req.Asset, Recipient: req.Recipient, Amount: req.Amount,
	}
	if got := EvaluateAnchorBinding(anchor, &req); got != "" {
		t.Fatalf("preserved anchor binding = %q, want pass", got)
	}

	// The fee dimensions are the replacement's one free axis.
	feeBump := req
	feeBump.MaxFeePerGas = "1800000000"
	feeBump.MaxPriorityFeePerGas = "1200000000"
	if got := EvaluateAnchorBinding(anchor, &feeBump); got != "" {
		t.Fatalf("fee-raised request = %q, want pass (fees are the replacement axis)", got)
	}
	// Decimal normalization is not a rebinding.
	padded := req
	padded.Nonce = "00042"
	if got := EvaluateAnchorBinding(anchor, &padded); got != "" {
		t.Fatalf("zero-padded equal nonce = %q, want pass", got)
	}

	cases := []struct {
		name   string
		mutate func(*ReplacementAnchor, *Request)
		field  string
	}{
		{"chain", func(a *ReplacementAnchor, r *Request) { r.ChainID = 1 }, "chain_id"},
		{"sender", func(a *ReplacementAnchor, r *Request) { r.Sender = "0x2222222222222222222222222222222222222222" }, "sender"},
		{"tx type", func(a *ReplacementAnchor, r *Request) { r.TxType = 0 }, "tx_type"},
		{"nonce", func(a *ReplacementAnchor, r *Request) { r.Nonce = "43" }, "nonce"},
		{"value", func(a *ReplacementAnchor, r *Request) { r.Value = "1" }, "value"},
		{"gas limit", func(a *ReplacementAnchor, r *Request) { r.GasLimit = "70000" }, "gas_limit"},
		{"amount", func(a *ReplacementAnchor, r *Request) { r.Amount = "999" }, "amount"},
		{"to", func(a *ReplacementAnchor, r *Request) { r.To = "0x2222222222222222222222222222222222222222" }, "to"},
		{"asset", func(a *ReplacementAnchor, r *Request) { r.Asset = "0x2222222222222222222222222222222222222222" }, "asset"},
		{"recipient", func(a *ReplacementAnchor, r *Request) { r.Recipient = "0x2222222222222222222222222222222222222222" }, "recipient"},
		{"data", func(a *ReplacementAnchor, r *Request) {
			r.Data = "0xa9059cbb" + strings.Repeat("0", 24) + strings.Repeat("cc", 20) +
				strings.Repeat("0", 31) + "1"
		}, "data"},
	}
	for _, tc := range cases {
		a, r := anchor, req
		tc.mutate(&a, &r)
		if got := EvaluateAnchorBinding(a, &r); got != ClassAuthorizationInvalid {
			t.Fatalf("anchor rebinding (%s) = %q, want %q", tc.name, got, ClassAuthorizationInvalid)
		}
		if field := anchorRebindingField(a, &r); field != tc.field {
			t.Fatalf("anchor rebinding (%s) named field %q, want %q", tc.name, field, tc.field)
		}
	}
	// An undecodable calldata string fails closed as a data rebinding.
	bad := req
	bad.Data = "0xnot-hex"
	if got := EvaluateAnchorBinding(anchor, &bad); got != ClassAuthorizationInvalid {
		t.Fatalf("undecodable data = %q, want %q", got, ClassAuthorizationInvalid)
	}
}

func TestEvaluateGrantReuse(t *testing.T) {
	req := mustDecode(t, validBody())
	// validBody: 65000 gas × 1.5 gwei max fee (total 9.75e13), 1 gwei tip.
	base := GrantScope{
		Present:              true,
		AllowsFeeReplacement: true,
		FeeMaxTotal:          97500000000000,
		FeeMaxPerGas:         1500000000,
		FeeMaxPriority:       1000000000,
	}
	with := func(mutate func(*GrantScope)) GrantScope {
		s := base
		mutate(&s)
		return s
	}
	cases := []struct {
		name  string
		scope GrantScope
		want  RefusalClass
	}{
		{"absent carrier", GrantScope{}, ClassAuthorizationInvalid},
		{"purpose token off", with(func(s *GrantScope) { s.AllowsFeeReplacement = false }), ClassAuthorizationInvalid},
		{"permitted at the caps", base, ""},
		{"permitted with no fee constraint", with(func(s *GrantScope) { s.FeeMaxTotal, s.FeeMaxPerGas, s.FeeMaxPriority = 0, 0, 0 }), ""},
		{"fee above the per-gas cap", with(func(s *GrantScope) { s.FeeMaxPerGas = 1499999999 }), ClassAuthorizationInvalid},
		{"fee above the total cap", with(func(s *GrantScope) { s.FeeMaxTotal = 97499999999999 }), ClassAuthorizationInvalid},
		{"fee above the priority cap", with(func(s *GrantScope) { s.FeeMaxPriority = 999999999 }), ClassAuthorizationInvalid},
		{"missing applicable cap", with(func(s *GrantScope) { s.FeeMaxPerGas = 0 }), ClassAuthorizationInvalid},
	}
	for _, tc := range cases {
		if got := EvaluateGrantReuse(tc.scope, &req); got != tc.want {
			t.Fatalf("EvaluateGrantReuse(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
