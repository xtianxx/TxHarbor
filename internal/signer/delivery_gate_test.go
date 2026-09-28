package signer

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// testDeliveryGateAllow is the pass-through 015 delivery checkpoint (T070) for
// tests whose subject is not the recovery gate: the checkpoint stays wired
// (the core's fail-closed nil check is not under test there) and always
// admits. Gate-specific tests script their own function.
func testDeliveryGateAllow(context.Context, DeliveryGateRequest) error { return nil }

// gateNeverDB is a DB double that fails the test when any database method is
// reached: the nil-checkpoint refusal must happen before the first 009 read or
// write.
type gateNeverDB struct{ t *testing.T }

func (d gateNeverDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	d.t.Fatalf("database QueryRow(%q) ran before the recovery-gate nil check", sql)
	return nil
}

func (d gateNeverDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.t.Fatalf("database Exec(%q) ran before the recovery-gate nil check", sql)
	return pgconn.CommandTag{}, nil
}

func (d gateNeverDB) Begin(_ context.Context) (pgx.Tx, error) {
	d.t.Fatalf("database Begin ran before the recovery-gate nil check")
	return nil, nil
}

// gateNeverBinding is the BindingReader half of the same double.
type gateNeverBinding struct{ t *testing.T }

func (b gateNeverBinding) ReadBinding(context.Context, string, string) (BindingResult, error) {
	b.t.Fatal("binding read ran before the recovery-gate nil check")
	return BindingAbsent, nil
}

// gateNeverSink fails the test if any delivery byte is attempted.
type gateNeverSink struct{ t *testing.T }

func (s gateNeverSink) WriteDelivery(context.Context, []byte) error {
	s.t.Fatal("delivery bytes were attempted after the nil checkpoint")
	return nil
}

// TestDeliveryRefusesWithoutRecoveryGate pins T070's fail-closed direction
// without a database: DeliveryDeps.RecoveryGate is a required dependency and
// its absence refuses (signature_withheld, zero bytes, no 009 read). It is
// never interpreted as pass-through.
func TestDeliveryRefusesWithoutRecoveryGate(t *testing.T) {
	res, err := Deliver(context.Background(),
		DeliveryDeps{DB: gateNeverDB{t: t}, Binding: gateNeverBinding{t: t}},
		Caller{ID: 1, CanSign: true}, "sr-no-gate", gateNeverSink{t: t})
	if res != nil {
		t.Fatalf("result = %+v, want nil (refused before any delivery work)", res)
	}
	var re *RefusalError
	if !errors.As(err, &re) || re.Class != ClassSignatureWithheld {
		t.Fatalf("error = %v, want *RefusalError class %s", err, ClassSignatureWithheld)
	}
	if r := RetryabilityOf(re.Class); r != RetryAfterRelease {
		t.Fatalf("retryability = %v, want RetryAfterRelease (the same identity re-gates after release)", r)
	}
}
