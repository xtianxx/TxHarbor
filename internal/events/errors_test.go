package events

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyPGErrorByConstraintName(t *testing.T) {
	identity := []string{
		"outbox_events_object_identity_uniq",
		"outbox_events_log_identity_uniq",
		"outbox_events_event_id_uniq",
	}
	for _, constraint := range identity {
		err := ClassifyPGError(&pgconn.PgError{Code: "23505", ConstraintName: constraint})
		if !errors.Is(err, ErrIdentityConflict) {
			t.Fatalf("23505 on %s = %v, want ErrIdentityConflict", constraint, err)
		}
	}
	otherUnique := ClassifyPGError(&pgconn.PgError{Code: "23505", ConstraintName: "consumer_inbox_pkey"})
	if !errors.Is(otherUnique, ErrContract) || errors.Is(otherUnique, ErrIdentityConflict) {
		t.Fatalf("23505 on consumer_inbox_pkey = %v, want ErrContract", otherUnique)
	}
	check := ClassifyPGError(&pgconn.PgError{Code: "23514", ConstraintName: "outbox_events_state_consistency"})
	if !errors.Is(check, ErrContract) {
		t.Fatalf("23514 = %v, want ErrContract", check)
	}
	if !errors.Is(ClassifyPGError(&pgconn.PgError{Code: "23514", ConstraintName: "outbox_events_revision_shape"}), ErrContract) {
		t.Fatal("23514 on revision shape must classify as ErrContract")
	}

	// Non-classified SQLSTATEs and non-PG errors pass through unchanged.
	serialization := &pgconn.PgError{Code: "40001", ConstraintName: ""}
	if got := ClassifyPGError(serialization); got != serialization {
		t.Fatalf("40001 was reclassified: %v", got)
	}
	plain := errors.New("connection reset")
	if got := ClassifyPGError(plain); got != plain {
		t.Fatalf("plain error was reclassified: %v", got)
	}
}

func TestClassifyPublishError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want FailureClass
	}{
		{"transient wrapper", Transient(errors.New("broker unreachable")), ClassTransient},
		{"transient wrapper, wrapped again", fmt.Errorf("publish: %w", Transient(errors.New("timeout"))), ClassTransient},
		{"permanent wrapper", Permanent(errors.New("serialize payload")), ClassPermanent},
		{"identity conflict", ErrIdentityConflict, ClassContract},
		{"contract error", ErrContract, ClassContract},
		{"unknown error", errors.New("unclassified"), ClassPermanent},
		{"nil error", nil, ClassPermanent},
	}
	for _, tc := range cases {
		if got := ClassifyPublishError(tc.err); got != tc.want {
			t.Fatalf("%s: ClassifyPublishError = %s, want %s", tc.name, got, tc.want)
		}
	}
	if !IsRetryable(Transient(errors.New("throttled"))) {
		t.Fatal("transient class must be retryable")
	}
	for _, err := range []error{Permanent(errors.New("bad topic")), ErrIdentityConflict, ErrContract, errors.New("unknown")} {
		if IsRetryable(err) {
			t.Fatalf("non-transient error %v reported retryable", err)
		}
	}
}

func TestQuarantineVocabulary(t *testing.T) {
	want := []QuarantineReason{
		"retry_exhausted", "non_retryable", "version_gap",
		"schema_unsupported", "identity_mismatch",
	}
	got := QuarantineReasons()
	if len(got) != len(want) {
		t.Fatalf("QuarantineReasons() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("QuarantineReasons()[%d] = %q, want %q", i, got[i], want[i])
		}
		if !got[i].Valid() {
			t.Fatalf("QuarantineReason(%q).Valid() = false", got[i])
		}
	}
	if QuarantineReason("dropped").Valid() {
		t.Fatal("unknown quarantine reason reported valid")
	}
}

func TestConsumerRetryable(t *testing.T) {
	if !ConsumerRetryable(Transient(errors.New("pg connection reset"))) {
		t.Fatal("transient consumer failure must be retryable")
	}
	for _, code := range []string{"40001", "40P01"} {
		if !ConsumerRetryable(&pgconn.PgError{Code: code}) {
			t.Fatalf("SQLSTATE %s must be retryable", code)
		}
	}
	nonRetryable := []error{
		ErrContract,
		ErrIdentityConflict,
		ErrSchemaUnsupported,
		errors.New("unknown consumer failure"),
		nil,
	}
	for _, err := range nonRetryable {
		if ConsumerRetryable(err) {
			t.Fatalf("error %v reported retryable", err)
		}
	}
}
