package nonce

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testSender() string { return "0x" + strings.Repeat("ab", 20) }

// TestRegistryReadBySenderVersioned pins the read: keyed by
// (chain_id, sender), returning the monotonic registry_seq, and issuing zero
// writes. An absent sender is (nil, nil), never an error.
func TestRegistryReadBySenderVersioned(t *testing.T) {
	sender := testSender()
	now := time.Now().UTC()
	f := &fakeQuerier{handler: func(sql string, args []any) pgx.Row {
		if !strings.Contains(sql, "FROM nonce_wallet_registry WHERE chain_id = $1 AND sender = $2") {
			t.Errorf("unexpected registry SQL: %q", sql)
		}
		if len(args) != 2 || args[0] != int64(31337) || args[1] != sender {
			t.Errorf("registry args = %v", args)
		}
		return rows(int64(31337), sender, RegistryActive, int64(7), now, now)
	}}
	r, err := readRegistryBySenderTx(context.Background(), f, 31337, sender)
	if err != nil || r == nil {
		t.Fatalf("readRegistryBySenderTx = (%v, %v)", r, err)
	}
	if r.ChainID != 31337 || r.Sender != sender || r.State != RegistryActive ||
		r.RegistrySeq != 7 || !r.CreatedAt.Equal(now) || !r.UpdatedAt.Equal(now) {
		t.Fatalf("registry = %+v", r)
	}
	if got := f.execCount(); got != 0 {
		t.Fatalf("registry read issued %d writes, want 0", got)
	}

	absent, err := readRegistryBySenderTx(context.Background(), &fakeQuerier{}, 31337, sender)
	if err != nil || absent != nil {
		t.Fatalf("absent registry = (%v, %v), want (nil, nil)", absent, err)
	}
}

// TestRegistryAdmissionGates pins that active/disabled gate admission only,
// fail-closed: only a present, active row admits; absent refuses
// sender_not_registered; disabled (or any unexpected state) refuses
// sender_disabled.
func TestRegistryAdmissionGates(t *testing.T) {
	cases := []struct {
		name string
		row  *WalletRegistry
		want Outcome
	}{
		{"active admits", &WalletRegistry{State: RegistryActive}, ""},
		{"disabled refuses", &WalletRegistry{State: RegistryDisabled}, OutcomeSenderDisabled},
		{"unknown state refuses fail-closed", &WalletRegistry{State: "paused"}, OutcomeSenderDisabled},
		{"absent refuses", nil, OutcomeSenderNotRegistered},
	}
	for _, tc := range cases {
		if got := tc.row.AdmissionRefusal(); got != tc.want {
			t.Errorf("%s: AdmissionRefusal() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestRegistryLaterChangeNeverAltersBinding pins R9 non-retroactivity: the
// binding stores the registry_seq in force at admission (7); a later registry
// change (seq 8) leaves the binding untouched. Structurally, only the binding
// INSERT carries registry_seq — no UPDATE path writes it.
func TestRegistryLaterChangeNeverAltersBinding(t *testing.T) {
	sender := testSender()
	now := time.Now().UTC()
	f := &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
		if strings.Contains(sql, "nonce_wallet_registry") {
			return rows(int64(31337), sender, RegistryActive, int64(8), now, now)
		}
		return rows("b-1", "intent-1", int64(31337), sender, "5", StateAllocated,
			"auth-1", strings.Repeat("0", 64), int64(7), "obs-1", now, now, nil, nil, nil)
	}}

	b, err := readBindingByIDTx(context.Background(), f, "b-1")
	if err != nil || b == nil {
		t.Fatalf("readBindingByIDTx = (%v, %v)", b, err)
	}
	reg, err := readRegistryBySenderTx(context.Background(), f, 31337, sender)
	if err != nil || reg == nil {
		t.Fatalf("readRegistryBySenderTx = (%v, %v)", reg, err)
	}
	if reg.RegistrySeq != 8 {
		t.Fatalf("registry moved to seq %d, want 8", reg.RegistrySeq)
	}
	if b.RegistrySeq != 7 {
		t.Fatalf("binding registry_seq = %d, want pinned 7", b.RegistrySeq)
	}

	if !strings.Contains(insertBindingSQL, "registry_seq") {
		t.Fatal("binding INSERT does not store registry_seq")
	}
	if strings.Contains(advanceBindingStateSQL, "registry_seq") {
		t.Fatal("binding UPDATE path writes registry_seq; non-retroactivity broken")
	}
}
