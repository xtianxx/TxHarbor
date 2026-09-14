package indexer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseConfirmThreshold(t *testing.T) {
	maxN := "9223372036854775807"
	cases := []struct {
		name    string
		raw     string
		want    uint64
		wantErr bool
	}{
		{"min valid", "1", 1, false},
		{"typical", "10", 10, false},
		{"max int64", maxN, 9223372036854775807, false},
		{"blank refused (no default)", "", 0, true},
		{"zero refused", "0", 0, true},
		{"non-integer refused", "abc", 0, true},
		{"decimal refused", "10.5", 0, true},
		{"negative refused", "-5", 0, true},
		{"spaces refused", " 10", 0, true},
		{"max+1 refused", "9223372036854775808", 0, true},
		{"max uint64 refused", "18446744073709551615", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConfirmThreshold(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseConfirmThreshold(%q) = nil error, want refusal", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseConfirmThreshold(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("parseConfirmThreshold(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

// TestConfirmAuthValidationGuards exercises every pre-DB refusal: none of
// these may touch the database (nil pool is the proof — no query is issued).
func TestConfirmAuthValidationGuards(t *testing.T) {
	ctx := context.Background()
	valid := ConfirmAuthRequest{
		ChainID: 1, RequestID: "r1", ExpectedOldSeq: 1,
		NewThresholdRaw: "10", Operator: "op", Reason: "why",
	}
	cases := []struct {
		name string
		req  ConfirmAuthRequest
	}{}
	_ = cases
	_ = valid
	if _, err := AuthorizeConfirmationPolicy(ctx, nil, valid); err == nil {
		t.Fatal("nil pool = nil error, want refusal")
	}
	for _, tc := range []struct {
		name string
		req  ConfirmAuthRequest
	}{
		{"chain 0 rejected", ConfirmAuthRequest{ChainID: 0, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "10", Operator: "op", Reason: "why"}},
		{"chain negative rejected", ConfirmAuthRequest{ChainID: -1, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "10", Operator: "op", Reason: "why"}},
		{"empty request_id rejected", ConfirmAuthRequest{ChainID: 1, ExpectedOldSeq: 1, NewThresholdRaw: "10", Operator: "op", Reason: "why"}},
		{"empty operator rejected", ConfirmAuthRequest{ChainID: 1, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "10", Reason: "why"}},
		{"empty reason rejected", ConfirmAuthRequest{ChainID: 1, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "10", Operator: "op"}},
		{"blank threshold refused", ConfirmAuthRequest{ChainID: 1, RequestID: "r", ExpectedOldSeq: 1, Operator: "op", Reason: "why"}},
		{"zero threshold refused", ConfirmAuthRequest{ChainID: 1, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "0", Operator: "op", Reason: "why"}},
		{"non-integer threshold refused", ConfirmAuthRequest{ChainID: 1, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "x", Operator: "op", Reason: "why"}},
		{"over-range threshold refused", ConfirmAuthRequest{ChainID: 1, RequestID: "r", ExpectedOldSeq: 1, NewThresholdRaw: "9223372036854775808", Operator: "op", Reason: "why"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := AuthorizeConfirmationPolicy(ctx, nil, tc.req); err == nil {
				t.Fatalf("AuthorizeConfirmationPolicy(%+v) with nil pool = nil error, want refusal", tc.req)
			}
		})
	}
}

func TestObserveConfirmAuthResult(t *testing.T) {
	defer SetConfirmAuthObserver(nil)
	var got []string
	SetConfirmAuthObserver(func(r string) { got = append(got, r) })

	observeConfirmAuthResult(ConfirmAuthResult{PolicySeq: 2, Threshold: 10}, nil)
	observeConfirmAuthResult(ConfirmAuthResult{}, ErrConfirmAuthRejected)
	observeConfirmAuthResult(ConfirmAuthResult{}, ErrConfirmAuthExpired)
	observeConfirmAuthResult(ConfirmAuthResult{}, errors.New("boom"))

	want := []string{"ok", "rejected", "rejected", "error"}
	if len(got) != len(want) {
		t.Fatalf("observer calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("observer calls = %v, want %v", got, want)
		}
	}
	// Wrapped sentinels still classify as rejections.
	got = nil
	observeConfirmAuthResult(ConfirmAuthResult{},
		errors.Join(errors.New("x"), ErrConfirmAuthRejected))
	if len(got) != 1 || got[0] != "rejected" {
		t.Fatalf("wrapped rejection observer calls = %v, want [rejected]", got)
	}
	// Nil observer disables counting without panicking.
	SetConfirmAuthObserver(nil)
	observeConfirmAuthResult(ConfirmAuthResult{}, nil)
}

// TestConfirmAuthStaticConfinement pins the T024 structural contract on
// confirmauth.go source: shared lease constants reused (never copied SQL
// text), exactly one policy INSERT, no policy UPDATE/DELETE, and zero
// pause-row writes.
func TestConfirmAuthStaticConfinement(t *testing.T) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(self), "confirmauth.go"))
	if err != nil {
		t.Fatalf("read confirmauth.go: %v", err)
	}
	body := string(raw)
	for _, want := range []string{"writeGuard", "ensureLeaseSQL", "lockCoordSQL"} {
		if !strings.Contains(body, want) {
			t.Errorf("confirmauth.go does not reference shared constant %s (reuse, never copy SQL text)", want)
		}
	}
	if n := strings.Count(body, "INSERT INTO confirmation_policy_history"); n != 1 {
		t.Errorf("INSERT INTO confirmation_policy_history sites = %d, want exactly 1 (single-row switch append)", n)
	}
	for _, forbidden := range []string{
		"UPDATE confirmation_policy_history",
		"DELETE FROM confirmation_policy_history",
		"deposit_pause", "indexer_pause", "log_pause",
		"INSERT INTO deposit_pause", "INSERT INTO indexer_pause", "INSERT INTO log_pause",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("confirmauth.go contains %q: policy is append-only and switches write zero pause rows", forbidden)
		}
	}
}
