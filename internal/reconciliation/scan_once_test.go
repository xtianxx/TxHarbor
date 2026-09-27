// scan_once_test.go is the T016 unit layer for the ScanOnce compare loop. It
// pins the pure decision helpers with no database and no Docker: input
// validation fails closed, chain-fact matching distinguishes present /
// definitive-absence / orphaned / non-attributable evidence, event identity
// matching never guesses between versions, duplicate evidence merges
// conservatively (Q4), evidence instants stay fact-derived (cross-scan dedup)
// and pending classifications map onto the closed gap vocabulary.
package reconciliation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/events"
)

// scanOnceNoRowsDB is a StoreDB whose reads return no rows; it exists so
// ScanOnce's fail-closed input validation can be exercised without a live
// database (any validation gap surfaces as ErrTaskNotFound, never a panic).
type scanOnceNoRowsDB struct{}

func (scanOnceNoRowsDB) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, errors.New("begin must not be reached by validation-only tests")
}

func (scanOnceNoRowsDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return scanOnceNoRowsRow{}
}

type scanOnceNoRowsRow struct{}

func (scanOnceNoRowsRow) Scan(dest ...any) error { return pgx.ErrNoRows }

func TestScanOnceInputsFailClosed(t *testing.T) {
	ctx := context.Background()
	validLimits := BudgetLimits{
		MaxConcurrency: 1, MaxSpanPerClaim: 4, MaxDuration: time.Minute,
		MaxPGRequests: 8, MaxRPCRequests: 2,
	}

	var nilStore *Store
	if _, err := nilStore.ScanOnce(ctx, ScanOnceRequest{
		TaskID: "t", Owner: "op", LeaseTTL: time.Minute, Limits: validLimits, Candidates: scanZeroCandidates{},
	}); !errors.Is(err, ErrContract) {
		t.Fatalf("nil store err = %v, want ErrContract", err)
	}

	store := &Store{db: scanOnceNoRowsDB{}}
	base := ScanOnceRequest{TaskID: "t", Owner: "op", LeaseTTL: time.Minute, Limits: validLimits, Candidates: scanZeroCandidates{}}

	refusals := []struct {
		name   string
		mutate func(*ScanOnceRequest)
	}{
		{"missing task id", func(r *ScanOnceRequest) { r.TaskID = " " }},
		{"missing owner", func(r *ScanOnceRequest) { r.Owner = "\t" }},
		{"non-positive lease", func(r *ScanOnceRequest) { r.LeaseTTL = 0 }},
		{"zero limits", func(r *ScanOnceRequest) { r.Limits = BudgetLimits{} }},
		{"nil candidates", func(r *ScanOnceRequest) { r.Candidates = nil }},
		{"consumers without quarantine", func(r *ScanOnceRequest) {
			r.EventConsumers = []EventConsumerRegistration{{Name: "c", Progress: scanNoProgress{}}}
		}},
	}
	for _, tc := range refusals {
		req := base
		tc.mutate(&req)
		_, err := store.ScanOnce(ctx, req)
		if err == nil {
			t.Errorf("%s: ScanOnce accepted malformed input", tc.name)
			continue
		}
		if !errors.Is(err, ErrContract) && !errors.Is(err, ErrInvalidBudget) {
			t.Errorf("%s: err = %v, want ErrContract/ErrInvalidBudget", tc.name, err)
		}
	}

	// A non-running task is refused with ErrTaskNotRunning after the shape
	// checks (the fake database reports ErrTaskNotFound instead, which still
	// proves the read path was reached only after validation).
	_, err := store.ScanOnce(ctx, base)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("well-formed request err = %v, want ErrTaskNotFound from the fake database", err)
	}
}

type scanZeroCandidates struct{}

func (scanZeroCandidates) ScanCandidates(ctx context.Context, interval ScanInterval) ([]ScanCandidate, error) {
	return nil, nil
}

type scanNoProgress struct{}

func (scanNoProgress) ReadProgress(ctx context.Context) ([]events.Progress, error) { return nil, nil }

func (scanNoProgress) ResumeOffset(ctx context.Context, topic string, partition int) (int64, bool, error) {
	return 0, false, nil
}

func TestMatchChainCandidateFacts(t *testing.T) {
	logIndex := int64(0)
	otherLogIndex := int64(7)
	bundle := &ChainFactsBundle{
		ChainID: 7,
		From:    100,
		To:      101,
		Blocks: []ChainFactBlock{
			{Number: 100, Hash: "0xaa", Canonical: true, IndexedAt: time.Unix(1000, 0).UTC()},
			{Number: 101, Hash: "0xbb", Canonical: true, IndexedAt: time.Unix(1001, 0).UTC()},
		},
		Logs: []ChainFactLog{
			{BlockNumber: 100, BlockHash: "0xaa", TxHash: "0xt1", LogIndex: 0, Contract: "0xc", Topic0: "0xd", Data: "0x"},
		},
	}
	cases := []struct {
		name        string
		ref         ChainFactRef
		present     bool
		ambiguous   bool
		orphaned    bool
		wantBlock   int64
		wantLogsLen int
	}{
		{"block+hash match", ChainFactRef{BlockNumber: 100, BlockHash: "0xAA"}, true, false, false, 100, 0},
		{"block number only match", ChainFactRef{BlockNumber: 101}, true, false, false, 101, 0},
		{"block number with drifted hash is orphaned", ChainFactRef{BlockNumber: 100, BlockHash: "0xcc"}, false, false, true, 0, 0},
		{"missing block is a definitive absence", ChainFactRef{BlockNumber: 102}, false, false, false, 0, 0},
		{"log match", ChainFactRef{TxHash: "0xT1"}, true, false, false, 0, 1},
		{"log index filter", ChainFactRef{TxHash: "0xt1", LogIndex: &logIndex}, true, false, false, 0, 1},
		{"log index mismatch is not located", ChainFactRef{TxHash: "0xt1", LogIndex: &otherLogIndex}, false, true, false, 0, 0},
		{"tx-only miss is not provable absence", ChainFactRef{TxHash: "0xmissing"}, false, true, false, 0, 0},
		{"hash-only miss is not located", ChainFactRef{BlockHash: "0xdead"}, false, true, false, 0, 0},
		{"empty ref is not attributable", ChainFactRef{}, false, true, false, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			match := matchChainCandidateFacts(bundle, tc.ref)
			if match.present != tc.present || match.ambiguous != tc.ambiguous || match.orphaned != tc.orphaned {
				t.Fatalf("match = %+v, want present=%v ambiguous=%v orphaned=%v",
					match, tc.present, tc.ambiguous, tc.orphaned)
			}
			if match.block != nil && match.block.Number != tc.wantBlock {
				t.Fatalf("matched block = %d, want %d", match.block.Number, tc.wantBlock)
			}
			if len(match.logs) != tc.wantLogsLen {
				t.Fatalf("matched logs = %d, want %d", len(match.logs), tc.wantLogsLen)
			}
		})
	}

	if match := matchChainCandidateFacts(nil, ChainFactRef{BlockNumber: 100}); !match.ambiguous || match.present {
		t.Fatalf("nil bundle match = %+v, want ambiguous/unknown", match)
	}
}

func TestMatchCandidateEventObservation(t *testing.T) {
	e1, e2 := uuid.New(), uuid.New()
	evidence := &EventStateEvidence{
		Observations: []EventDeliveryObservation{
			{EventID: e1, AggregateType: "withdrawal", AggregateID: "req-1"},
			{EventID: e2, AggregateType: "withdrawal", AggregateID: "req-1"},
			{EventID: uuid.New(), AggregateType: "withdrawal", AggregateID: "req-2"},
		},
	}
	key := BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal/req-1"}
	if _, match := matchCandidateEventObservation(evidence, key); match != eventMatchAmbiguous {
		t.Fatalf("aggregate with two event versions = %v, want eventMatchAmbiguous", match)
	}
	if observation, match := matchCandidateEventObservation(evidence,
		BusinessKey{Kind: BusinessKeyEventID, Value: e1.String()}); match != eventMatchFound || observation.EventID != e1 {
		t.Fatalf("event id match = (%v, %v), want the e1 observation", observation, match)
	}
	if observation, match := matchCandidateEventObservation(evidence,
		BusinessKey{Kind: EventBusinessKeyAggregate, Value: "withdrawal/req-2"}); match != eventMatchFound || observation.AggregateID != "req-2" {
		t.Fatalf("unique aggregate match = (%v, %v), want the req-2 observation", observation, match)
	}
	if _, match := matchCandidateEventObservation(evidence,
		BusinessKey{Kind: BusinessKeyRequestID, Value: "unknown"}); match != eventMatchNone {
		t.Fatalf("missing key match = %v, want eventMatchNone", match)
	}
	if _, match := matchCandidateEventObservation(nil, key); match != eventMatchNone {
		t.Fatalf("nil evidence match = %v, want eventMatchNone", match)
	}
}

func TestMergeDuplicateEvidence(t *testing.T) {
	eventSide := DuplicateEvidence{
		Deliveries: 3, ContentChecked: true, VersionGuardIgnoredLegalOld: true,
		IdempotencyRecorded: true, EffectEvidencePresent: true, EffectCount: 1,
	}
	pgSide := DuplicateEvidence{
		Deliveries: 1, EffectCount: 2, RepeatedBusinessEffect: true,
	}
	merged := mergeDuplicateEvidence(eventSide, pgSide)
	if merged.Deliveries != 3 || merged.EffectCount != 2 {
		t.Fatalf("merged counts = (%d, %d), want max (3, 2)", merged.Deliveries, merged.EffectCount)
	}
	if !merged.RepeatedBusinessEffect || !merged.VersionGuardIgnoredLegalOld || !merged.ContentChecked {
		t.Fatalf("merged flags = %+v, want conservative OR of both sides", merged)
	}
	if !merged.Divergent() {
		t.Fatalf("merged evidence with a repeated business effect must be divergent")
	}
	if !merged.absorbed() {
		// absorbed() returns false when divergent: merging must not turn a
		// divergence into an absorption.
		if merged.absorbed() {
			t.Fatalf("merged divergent evidence reported absorbed")
		}
	}
}

func TestStableScanEvidenceInstantIsFactDerived(t *testing.T) {
	fallback := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	block := &ChainFactBlock{IndexedAt: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)}
	event := &EventDeliveryObservation{OccurredAt: time.Date(2026, 9, 26, 11, 0, 0, 0, time.UTC)}
	pg := &PGStateRecord{}
	pg.Freshness.NewestObservedAt = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	if got := stableScanEvidenceInstant(block, event, pg, fallback); !got.Equal(pg.Freshness.NewestObservedAt) {
		t.Fatalf("latest fact instant = %v, want %v", got, pg.Freshness.NewestObservedAt)
	}
	if got := stableScanEvidenceInstant(block, nil, nil, fallback); !got.Equal(block.IndexedAt) {
		t.Fatalf("block-only instant = %v, want %v", got, block.IndexedAt)
	}
	if got := stableScanEvidenceInstant(nil, event, nil, fallback); !got.Equal(event.OccurredAt) {
		t.Fatalf("event-only instant = %v, want %v", got, event.OccurredAt)
	}
	// The fallback is the scan clock and is used only when no fact carries a
	// time; identity dedup then cannot be proven stable.
	if got := stableScanEvidenceInstant(nil, nil, nil, fallback); !got.Equal(fallback) {
		t.Fatalf("empty-fact instant = %v, want the fallback %v", got, fallback)
	}
}

func TestScanTaskIdentityScope(t *testing.T) {
	heightTask := &Task{
		TaskID: "task-1", ScopeChainID: "1", ScopeKind: ScopeHeight,
		ScopeStart: HeightBound(100), ScopeEnd: HeightBound(103),
		BusinessTypes: []BusinessType{BusinessWithdrawal},
	}
	scope, err := scanTaskIdentityScope(heightTask)
	if err != nil {
		t.Fatalf("height scope: %v", err)
	}
	if scope.From != 100 || scope.To != 103 || scope.ChainID != "1" || len(scope.BusinessTypes) != 1 {
		t.Fatalf("height scope = %+v, want 100..103 on chain 1", scope)
	}

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	timeTask := &Task{
		TaskID: "task-2", ScopeChainID: "1", ScopeKind: ScopeTime,
		ScopeStart: TimeBound(base), ScopeEnd: TimeBound(base.Add(3 * time.Microsecond)),
		BusinessTypes: []BusinessType{BusinessDeposit},
	}
	scope, err = scanTaskIdentityScope(timeTask)
	if err != nil {
		t.Fatalf("time scope: %v", err)
	}
	if scope.From != base.UnixMicro() || scope.To != base.Add(3*time.Microsecond).UnixMicro() {
		t.Fatalf("time scope = %+v, want microsecond bounds", scope)
	}

	bad := &Task{TaskID: "task-3", ScopeChainID: "1", ScopeKind: ScopeKind("epoch"),
		ScopeStart: HeightBound(1), ScopeEnd: HeightBound(2), BusinessTypes: []BusinessType{BusinessWithdrawal}}
	if _, err := scanTaskIdentityScope(bad); !errors.Is(err, ErrContract) {
		t.Fatalf("unknown kind err = %v, want ErrContract", err)
	}
}

func TestScanTaskConfirmThresholdN(t *testing.T) {
	task := &Task{PolicyRefs: []byte(`{"confirm_threshold_n": 12, "cutover": "v2"}`)}
	if got := scanTaskConfirmThresholdN(task); got != 12 {
		t.Fatalf("confirm threshold = %d, want 12", got)
	}
	for _, raw := range [][]byte{nil, {}, []byte(`{}`), []byte(`not-json`), []byte(`{"confirm_threshold_n": "x"}`)} {
		if got := scanTaskConfirmThresholdN(&Task{PolicyRefs: raw}); got != 0 {
			t.Fatalf("policy_refs %q -> %d, want 0 (fail-closed)", raw, got)
		}
	}
}

func TestGapReasonForClassification(t *testing.T) {
	cases := []struct {
		reason   ClassificationReason
		upstream UpstreamReceiptSource
		want     GapReason
	}{
		{ReasonUpstreamUnconnected, UpstreamReceiptSource{Connected: false}, GapUpstreamUnconnected},
		{ReasonUpstreamUnavailable, UpstreamReceiptSource{Connected: true, Available: false}, GapUpstreamUnconnected},
		{ReasonFreshnessExpired, UpstreamReceiptSource{Connected: true, Available: true}, GapFreshnessHold},
		{ReasonFreshnessUnproven, UpstreamReceiptSource{Connected: true, Available: true}, GapFreshnessHold},
		{ReasonEvidenceTrimmed, UpstreamReceiptSource{Connected: true, Available: true}, GapFreshnessHold},
		{ReasonScanIncomplete, UpstreamReceiptSource{Connected: true, Available: true}, GapQueryFailed},
		{ReasonEvidenceOrphaned, UpstreamReceiptSource{Connected: true, Available: true}, GapQueryFailed},
		{ReasonUnknownResult, UpstreamReceiptSource{}, GapUpstreamUnconnected},
	}
	for _, tc := range cases {
		got := gapReasonForClassification(Classification{Reason: tc.reason}, tc.upstream)
		if got != tc.want || !got.Valid() {
			t.Errorf("reason %q upstream %+v -> %q, want %q", tc.reason, tc.upstream, got, tc.want)
		}
	}
}

func TestScanValidateCandidate(t *testing.T) {
	scope := IdentityScope{
		ChainID: "1", Kind: ScopeHeight, From: 1, To: 2,
		BusinessTypes: []BusinessType{BusinessWithdrawal},
	}
	valid := ScanCandidate{
		BusinessType: BusinessWithdrawal,
		BusinessKey:  BusinessKey{Kind: BusinessKeyRequestID, Value: "req-1"},
	}
	if err := scanValidateCandidate(&valid, scope); err != nil {
		t.Fatalf("valid candidate refused: %v", err)
	}
	outOfScope := valid
	outOfScope.BusinessType = BusinessDeposit
	if err := scanValidateCandidate(&outOfScope, scope); !errors.Is(err, ErrContract) {
		t.Fatalf("out-of-scope candidate err = %v, want ErrContract", err)
	}
	oversized := valid
	oversized.BusinessKey.Value = strings.Repeat("a", scanBusinessKeyMax)
	if err := scanValidateCandidate(&oversized, scope); !errors.Is(err, ErrContract) {
		t.Fatalf("oversized business key err = %v, want ErrContract", err)
	}
	if err := scanValidateCandidate(nil, scope); !errors.Is(err, ErrContract) {
		t.Fatalf("nil candidate err = %v, want ErrContract", err)
	}
}

func TestScanEvidenceRefIsBounded(t *testing.T) {
	attempt := uuid.NewString()
	classification := Classification{EvidenceRef: "events:v1 event_id=e1"}
	if got := scanEvidenceRef(&classification, attempt); got != "events:v1 event_id=e1" {
		t.Fatalf("evidence ref = %q, want the candidate ref", got)
	}
	classification.EvidenceRef = string(make([]byte, scanEvidenceRefMax+1))
	got := scanEvidenceRef(&classification, attempt)
	if len(got) == 0 || len(got) > scanEvidenceRefMax {
		t.Fatalf("oversized evidence ref -> %q (len %d), want a synthesized bounded ref", got, len(got))
	}
}
