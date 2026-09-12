package indexer

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The validation paths of NewLease touch no connection, so a zero-value pool
// is enough to exercise them.
var noPool = &pgxpool.Pool{}

func TestNewLeaseDefaults(t *testing.T) {
	l, err := NewLease(noPool, 31337, Params{OwnerID: "instance-a"})
	if err != nil {
		t.Fatalf("NewLease() error = %v", err)
	}
	if l.ttl != DefaultTTL {
		t.Fatalf("ttl = %s, want default %s", l.ttl, DefaultTTL)
	}
	if l.heartbeat != DefaultHeartbeat {
		t.Fatalf("heartbeat = %s, want default %s", l.heartbeat, DefaultHeartbeat)
	}
	if l.Token() != 0 {
		t.Fatalf("token before Acquire = %d, want 0", l.Token())
	}
}

func TestNewLeaseValidation(t *testing.T) {
	cases := []struct {
		name    string
		chainID int64
		params  Params
	}{
		{"zero chain id", 0, Params{OwnerID: "a"}},
		{"negative chain id", -1, Params{OwnerID: "a"}},
		{"empty owner", 1, Params{}},
		{"negative ttl", 1, Params{OwnerID: "a", TTL: -time.Second}},
		{"negative heartbeat", 1, Params{OwnerID: "a", TTL: 5 * time.Second, Heartbeat: -time.Second}},
		{"heartbeat equals ttl", 1, Params{OwnerID: "a", TTL: time.Second, Heartbeat: time.Second}},
		{"heartbeat exceeds ttl", 1, Params{OwnerID: "a", TTL: time.Second, Heartbeat: 2 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewLease(noPool, tc.chainID, tc.params); err == nil {
				t.Fatal("NewLease() = nil error, want validation error")
			}
		})
	}
	if _, err := NewLease(nil, 1, Params{OwnerID: "a"}); err == nil {
		t.Fatal("NewLease(nil pool) = nil error, want error")
	}
}

func TestNewOwnerIDUnique(t *testing.T) {
	a, err := NewOwnerID()
	if err != nil {
		t.Fatalf("NewOwnerID() error = %v", err)
	}
	b, err := NewOwnerID()
	if err != nil {
		t.Fatalf("NewOwnerID() error = %v", err)
	}
	if len(a) != 32 || strings.Trim(a, "0123456789abcdef") != "" {
		t.Fatalf("NewOwnerID() = %q, want 32 lowercase hex chars", a)
	}
	if a == b {
		t.Fatalf("NewOwnerID() returned the same id twice: %q", a)
	}
}

func TestNextDelayJitterBounds(t *testing.T) {
	l := &Lease{heartbeat: time.Second}
	span := time.Second / jitterDiv
	for i := 0; i < 500; i++ {
		d := l.nextDelay()
		if d < time.Second-span || d > time.Second+span {
			t.Fatalf("nextDelay() = %s, want within [%s, %s]", d, time.Second-span, time.Second+span)
		}
	}
	// A heartbeat shorter than the jitter divisor still yields a positive wait.
	tiny := &Lease{heartbeat: time.Nanosecond}
	if d := tiny.nextDelay(); d != time.Nanosecond {
		t.Fatalf("nextDelay() tiny heartbeat = %s, want %s", d, time.Nanosecond)
	}
}
