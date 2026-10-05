// generation_unit_test.go covers the pure logic of the T013 evidence-generation
// protocol: the closed set of generation-advancing writes, token capture and
// component-wise validation, the evidence hash chain and request
// canonicalization. Database behavior (instance lock, discard audit,
// concurrent stale commits) is covered by generation_integration_test.go
// against a real PostgreSQL.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

func TestEvidenceMutationKindClosedSet(t *testing.T) {
	want := []EvidenceMutationKind{
		MutationVerificationBatch,
		MutationGapOpened,
		MutationGapClosed,
		MutationIsolationVerified,
		MutationIsolationRejected,
		MutationRestoreStarted,
		MutationRestoreProbeAccepted,
		MutationEvidenceSnapshotAccepted,
	}
	got := KnownEvidenceMutationKinds()
	if !slices.Equal(got, want) {
		t.Fatalf("closed mutation set = %v, want %v", got, want)
	}

	// Frozen string values: they appear in audit targets and in the evidence
	// hash chain. The first seven mirror the data-model §5 trigger list
	// verbatim; `restore_started` is the restore pre-write invalidation marker
	// added by restore.go (duplicate-restore timing) and must keep its frozen
	// spelling too.
	wantStrings := []string{
		"verification_batch", "gap_opened", "gap_closed",
		"isolation_verified", "isolation_rejected",
		"restore_started", "restore_probe_accepted", "evidence_snapshot_accepted",
	}
	for i, k := range got {
		if string(k) != wantStrings[i] {
			t.Fatalf("mutation kind %d = %q, want %q", i, k, wantStrings[i])
		}
	}
	if !slices.Equal(KnownEvidenceMutationKinds(), want) {
		t.Fatal("KnownEvidenceMutationKinds must keep the canonical order")
	}

	// The caller receives a fresh slice.
	got[0] = "mutated"
	if KnownEvidenceMutationKinds()[0] != MutationVerificationBatch {
		t.Fatal("KnownEvidenceMutationKinds must not expose package state")
	}

	for _, k := range want {
		if !k.Known() {
			t.Fatalf("kind %q must be known", k)
		}
		parsed, err := ParseEvidenceMutationKind(string(k))
		if err != nil || parsed != k {
			t.Fatalf("parse %q: got %q err=%v", k, parsed, err)
		}
	}
	for _, raw := range []string{"", " ", "Verification_Batch", "verification_batch ", "gap", "restore-probe", "unknown"} {
		if _, err := ParseEvidenceMutationKind(raw); err == nil {
			t.Fatalf("mutation kind %q must be refused", raw)
		} else if !errors.Is(err, ErrUnknownEvidenceMutation) {
			t.Fatalf("refusal of %q must be ErrUnknownEvidenceMutation, got %v", raw, err)
		}
	}
}

func TestEvidenceTokenMatchesAndValidate(t *testing.T) {
	base := EvidenceToken{
		InstanceID: "11111111-1111-1111-1111-111111111111",
		State:      "open",
		Generation: 3,
		Hash:       "sha256:aaaa",
	}
	if !base.Matches(base) {
		t.Fatal("a token must match itself")
	}
	if base.Validate(base) != nil {
		t.Fatal("Validate must return nil for a matching token")
	}

	cases := []struct {
		name    string
		mutate  func(EvidenceToken) EvidenceToken
		reasons []string
	}{
		{
			name: "instance",
			mutate: func(tk EvidenceToken) EvidenceToken {
				tk.InstanceID = "22222222-2222-2222-2222-222222222222"
				return tk
			},
			reasons: []string{"instance changed"},
		},
		{
			name: "state",
			mutate: func(tk EvidenceToken) EvidenceToken {
				tk.State = "closed"
				return tk
			},
			reasons: []string{"state changed during the evidence read: open -> closed"},
		},
		{
			name: "generation",
			mutate: func(tk EvidenceToken) EvidenceToken {
				tk.Generation = 4
				return tk
			},
			reasons: []string{"generation changed during the evidence read: 3 -> 4"},
		},
		{
			name: "hash",
			mutate: func(tk EvidenceToken) EvidenceToken {
				tk.Hash = "sha256:bbbb"
				return tk
			},
			reasons: []string{"evidence hash changed during the evidence read"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captured := base
			observed := tc.mutate(base)
			mismatch := captured.Validate(observed)
			if mismatch == nil {
				t.Fatal("a changed component must fail validation")
			}
			if mismatch.Captured != captured || mismatch.Observed != observed {
				t.Fatalf("mismatch must carry both tokens, got %+v", mismatch)
			}
			rendered := strings.Join(mismatch.Reasons(), " | ")
			for _, want := range tc.reasons {
				if !strings.Contains(rendered, want) {
					t.Fatalf("mismatch reasons %q must contain %q", rendered, want)
				}
			}
			if !strings.Contains(mismatch.Error(), tc.reasons[0]) {
				t.Fatalf("mismatch error %q must name the changed component", mismatch.Error())
			}
			// Matches is the boolean form of the same comparison.
			if captured.Matches(observed) {
				t.Fatal("Matches must agree with Validate")
			}
		})
	}

	// One component is enough; two changed components produce two reasons.
	double := base
	double.Generation = 9
	double.Hash = "sha256:cccc"
	if reasons := base.Validate(double).Reasons(); len(reasons) != 2 {
		t.Fatalf("two changed components must produce two reasons, got %v", reasons)
	}

	// A direct construction that fails without a named component still yields
	// an explicit fallback reason (never an empty reason list).
	fallback := (&TokenMismatch{Captured: base, Observed: base}).Reasons()
	if len(fallback) != 1 || !strings.Contains(fallback[0], "no longer matches") {
		t.Fatalf("fallback mismatch reason must stay explicit, got %v", fallback)
	}
}

func TestEvidenceChainHashDeterministicAndSensitive(t *testing.T) {
	const prev = "sha256:prev"
	base := EvidenceChainHash(prev, 1, MutationVerificationBatch, "op-1", []byte("payload"))

	if !strings.HasPrefix(base, "sha256:") || len(base) != len("sha256:")+64 {
		t.Fatalf("chain hash %q must be sha256:<64 hex>", base)
	}
	if again := EvidenceChainHash(prev, 1, MutationVerificationBatch, "op-1", []byte("payload")); again != base {
		t.Fatalf("chain hash must be deterministic: %q != %q", again, base)
	}
	if base == prev {
		t.Fatal("an accepted write must change the aggregate hash")
	}
	if base == controlstore.EmptyEvidenceHash {
		t.Fatal("an accepted write must not keep the empty evidence hash")
	}

	variants := map[string]string{
		"previous hash": EvidenceChainHash("sha256:other", 1, MutationVerificationBatch, "op-1", []byte("payload")),
		"generation":    EvidenceChainHash(prev, 2, MutationVerificationBatch, "op-1", []byte("payload")),
		"kind":          EvidenceChainHash(prev, 1, MutationGapClosed, "op-1", []byte("payload")),
		"operation":     EvidenceChainHash(prev, 1, MutationVerificationBatch, "op-2", []byte("payload")),
		"digest":        EvidenceChainHash(prev, 1, MutationVerificationBatch, "op-1", []byte("other")),
		"nil digest":    EvidenceChainHash(prev, 1, MutationVerificationBatch, "op-1", nil),
	}
	for name, variant := range variants {
		if variant == base {
			t.Fatalf("changing the %s must change the chain hash", name)
		}
	}
	// The nil digest is a stable, reproducible input (digest of the empty
	// input), not a wildcard.
	if variants["nil digest"] != EvidenceChainHash(prev, 1, MutationVerificationBatch, "op-1", []byte{}) {
		t.Fatal("nil and empty result digests must hash identically")
	}
}

func TestEvidenceWriteRequestValidate(t *testing.T) {
	token := EvidenceToken{
		InstanceID: "11111111-1111-1111-1111-111111111111",
		State:      "open",
		Generation: 2,
		Hash:       "sha256:aaaa",
	}
	valid := EvidenceWriteRequest{
		InstanceID: token.InstanceID,
		Token:      token,
		Kind:       MutationVerificationBatch,
		Actor:      "deploy:verifier",
		Apply:      func(context.Context, pgx.Tx, EvidenceToken) error { return nil },
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid request must pass: %v", err)
	}

	// The instance id comparison is case-insensitive (CaptureEvidenceToken
	// returns the canonical lower-case form from the database).
	upper := valid
	upper.InstanceID = strings.ToUpper(token.InstanceID)
	if err := upper.validate(); err != nil {
		t.Fatalf("upper-case instance id must validate against the canonical token: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*EvidenceWriteRequest)
		substr string
	}{
		{"missing instance", func(r *EvidenceWriteRequest) { r.InstanceID = " " }, "instance_id"},
		{"missing token", func(r *EvidenceWriteRequest) { r.Token.InstanceID = "" }, "captured before"},
		{"token of another instance", func(r *EvidenceWriteRequest) { r.Token.InstanceID = "22222222-2222-2222-2222-222222222222" }, "belongs to instance"},
		{"token without state", func(r *EvidenceWriteRequest) { r.Token.State = "" }, "no state"},
		{"negative generation", func(r *EvidenceWriteRequest) { r.Token.Generation = -1 }, ">= 0"},
		{"token without hash", func(r *EvidenceWriteRequest) { r.Token.Hash = " " }, "no evidence_hash"},
		{"unknown kind", func(r *EvidenceWriteRequest) { r.Kind = "verify" }, "unknown evidence mutation"},
		{"missing actor", func(r *EvidenceWriteRequest) { r.Actor = " " }, "actor is required"},
		{"missing apply", func(r *EvidenceWriteRequest) { r.Apply = nil }, "apply function is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mutate(&req)
			err := req.validate()
			if err == nil {
				t.Fatal("malformed request must be refused before any database access")
			}
			if !strings.Contains(err.Error(), tc.substr) {
				t.Fatalf("refusal %q must mention %q", err, tc.substr)
			}
			if tc.name == "unknown kind" && !errors.Is(err, ErrUnknownEvidenceMutation) {
				t.Fatalf("unknown kind must wrap ErrUnknownEvidenceMutation, got %v", err)
			}
		})
	}
}

func TestCaptureEvidenceTokenReadsCanonicalToken(t *testing.T) {
	q := &fakeTokenQueryer{row: fakeTokenRow{
		instanceID: "11111111-1111-1111-1111-111111111111",
		state:      "open",
		generation: 7,
		hash:       "sha256:cccc",
	}}
	token, err := CaptureEvidenceToken(context.Background(), q, "  11111111-1111-1111-1111-111111111111  ")
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if token.InstanceID != "11111111-1111-1111-1111-111111111111" ||
		token.State != "open" || token.Generation != 7 || token.Hash != "sha256:cccc" {
		t.Fatalf("unexpected captured token: %+v", token)
	}
	if len(q.args) != 1 || q.args[0] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("capture must pass the trimmed instance id, got %v", q.args)
	}

	if _, err := CaptureEvidenceToken(context.Background(), nil, "11111111-1111-1111-1111-111111111111"); err == nil {
		t.Fatal("capture without a database handle must refuse")
	}
	if _, err := CaptureEvidenceToken(context.Background(), q, "  "); err == nil {
		t.Fatal("capture without an instance id must refuse")
	}

	missing := &fakeTokenQueryer{row: fakeTokenRow{err: pgx.ErrNoRows}}
	if _, err := CaptureEvidenceToken(context.Background(), missing, "11111111-1111-1111-1111-111111111111"); err == nil {
		t.Fatal("a missing instance must refuse")
	} else if !errors.Is(err, controlstore.ErrInstanceNotFound) {
		t.Fatalf("missing instance must refuse with ErrInstanceNotFound, got %v", err)
	}

	broken := &fakeTokenQueryer{row: fakeTokenRow{err: errors.New("connection reset")}}
	if _, err := CaptureEvidenceToken(context.Background(), broken, "11111111-1111-1111-1111-111111111111"); err == nil {
		t.Fatal("a database failure must refuse")
	} else if !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("database failure must be wrapped, got %v", err)
	}
}

func TestLockedEvidenceTokenAdaptsInstanceToken(t *testing.T) {
	locked := controlstore.InstanceToken{
		InstanceID:         "11111111-1111-1111-1111-111111111111",
		State:              "open",
		EvidenceGeneration: 4,
		EvidenceHash:       "sha256:dddd",
	}
	token := LockedEvidenceToken(locked)
	if token.InstanceID != locked.InstanceID || token.State != locked.State ||
		token.Generation != locked.EvidenceGeneration || token.Hash != locked.EvidenceHash {
		t.Fatalf("locked token adaptation lost a component: %+v", token)
	}
}

func TestBoundedEvidenceDetail(t *testing.T) {
	if got := boundedEvidenceDetail("  short reason  "); got != "short reason" {
		t.Fatalf("short detail must only be trimmed, got %q", got)
	}
	long := strings.Repeat("x", evidenceDetailMaxBytes+100)
	got := boundedEvidenceDetail(long)
	if len(got) != evidenceDetailMaxBytes {
		t.Fatalf("bounded detail must be exactly %d bytes, got %d", evidenceDetailMaxBytes, len(got))
	}
	if !strings.HasSuffix(got, " [bound]") {
		t.Fatalf("truncation must stay visible, got suffix %q", got[len(got)-8:])
	}
}

// fakeTokenQueryer is a TokenQueryer test double; only QueryRow is used.
type fakeTokenQueryer struct {
	row  fakeTokenRow
	args []any
}

func (f *fakeTokenQueryer) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	f.args = args
	return f.row
}

type fakeTokenRow struct {
	instanceID string
	state      string
	generation int64
	hash       string
	err        error
}

func (r fakeTokenRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 4 {
		return fmt.Errorf("fakeTokenRow.Scan: expected 4 destinations, got %d", len(dest))
	}
	*(dest[0].(*string)) = r.instanceID
	*(dest[1].(*string)) = r.state
	*(dest[2].(*int64)) = r.generation
	*(dest[3].(*string)) = r.hash
	return nil
}
