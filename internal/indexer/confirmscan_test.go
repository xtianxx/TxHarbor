// confirmscan_test.go locks the T011 scan logic that can be exercised
// without a database: constructor validation, the outcome classification
// (below_depth row-level wait vs whole-loop halt), the F3 empty-state
// zero-write property and the atomic snapshot shape. Real-PostgreSQL
// assertions live in confirmation_integration_test.go (T013/T019).
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// confirmTestConfig is the minimal valid confirmation configuration used by
// the unit tests (timings shrunk so wait-bound tests finish in milliseconds).
func confirmTestConfig() ConfirmationConfig {
	return ConfirmationConfig{
		ChainID:      7,
		ThresholdN:   10,
		PollInterval: 5 * time.Millisecond,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	}
}

// confirmFakeCommitter records captured bases and serves a scripted outcome
// per call; it lets the unit tests prove batch driving without a database.
type confirmFakeCommitter struct {
	calls []ConfirmBasis
	fn    func(call int, basis ConfirmBasis) error
}

func (f *confirmFakeCommitter) ConfirmDepositUnit(_ context.Context, _ *Lease, basis ConfirmBasis, _ ...RecoveryCapture) error {
	f.calls = append(f.calls, basis)
	if f.fn == nil {
		return nil
	}
	return f.fn(len(f.calls)-1, basis)
}

// confirmFakeMetrics records every observation call for later assertion.
type confirmFakeMetrics struct {
	pending     []uint64
	lags        []uint64
	lagOK       []bool
	states      []int
	policySeq   []uint64
	policyOK    []bool
	confirmed   int
	skipped     map[string]int
	transitions map[string]int
}

func (m *confirmFakeMetrics) ObserveConfirmationPending(_ int64, pending uint64) {
	m.pending = append(m.pending, pending)
}

func (m *confirmFakeMetrics) ObserveConfirmationLag(_ int64, lag uint64, ok bool) {
	m.lags = append(m.lags, lag)
	m.lagOK = append(m.lagOK, ok)
}

func (m *confirmFakeMetrics) ObserveConfirmationState(_ int64, state int) {
	m.states = append(m.states, state)
}

func (m *confirmFakeMetrics) ObserveConfirmationPolicySeq(_ int64, seq uint64, ok bool) {
	m.policySeq = append(m.policySeq, seq)
	m.policyOK = append(m.policyOK, ok)
}

func (m *confirmFakeMetrics) ObserveConfirmationConfirmed(_ int64) { m.confirmed++ }

func (m *confirmFakeMetrics) ObserveConfirmationSkipped(_ int64, reason string) {
	if m.skipped == nil {
		m.skipped = map[string]int{}
	}
	m.skipped[reason]++
}

func (m *confirmFakeMetrics) ObserveConfirmationTransition(_ int64, result string) {
	if m.transitions == nil {
		m.transitions = map[string]int{}
	}
	m.transitions[result]++
}

// confirmFakeQuerier serves canned QueryRow scans and one canned candidate
// batch; unmapped statements read as absent (pgx.ErrNoRows).
type confirmFakeQuerier struct {
	calls    []string
	rows     map[string]func(dest ...any) error
	batch    [][]any
	batchErr error
}

func (f *confirmFakeQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	f.calls = append(f.calls, sql)
	if scan, ok := f.rows[sql]; ok {
		return confirmFakeRow(scan)
	}
	return confirmFakeRow(func(_ ...any) error { return pgx.ErrNoRows })
}

func (f *confirmFakeQuerier) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	f.calls = append(f.calls, sql)
	if sql != readConfirmationCandidatesSQL {
		return nil, errors.New("unexpected Query in unit test: " + sql)
	}
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	return &confirmFakeRows{rows: f.batch, i: -1}, nil
}

type confirmFakeRow func(dest ...any) error

func (r confirmFakeRow) Scan(dest ...any) error { return r(dest...) }

// confirmFakeRows is a scripted pgx.Rows over []any rows holding int64 and
// string values.
type confirmFakeRows struct {
	rows [][]any
	i    int
}

func (r *confirmFakeRows) Close() {}

func (r *confirmFakeRows) Err() error { return nil }

func (r *confirmFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *confirmFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *confirmFakeRows) Next() bool {
	r.i++
	return r.i < len(r.rows)
}

func (r *confirmFakeRows) Scan(dest ...any) error {
	if r.i < 0 || r.i >= len(r.rows) {
		return errors.New("scan without current row")
	}
	row := r.rows[r.i]
	if len(dest) != len(row) {
		return errors.New("scan arity mismatch")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *int64:
			v, ok := row[i].(int64)
			if !ok {
				return errors.New("scan type mismatch for *int64")
			}
			*p = v
		case *string:
			v, ok := row[i].(string)
			if !ok {
				return errors.New("scan type mismatch for *string")
			}
			*p = v
		case *int:
			v, ok := row[i].(int)
			if !ok {
				return errors.New("scan type mismatch for *int")
			}
			*p = v
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

func (r *confirmFakeRows) Values() ([]any, error) {
	if r.i < 0 || r.i >= len(r.rows) {
		return nil, errors.New("values without current row")
	}
	return r.rows[r.i], nil
}

func (r *confirmFakeRows) RawValues() [][]byte { return nil }

func (r *confirmFakeRows) Conn() *pgx.Conn { return nil }

func (r *confirmFakeRows) TypeMap() *pgtype.Map { return nil }

func (r *confirmFakeRows) NextResultSet() bool { return false }

// scanPolicy serves a present effective-policy row.
func scanPolicy(seq, threshold int64) func(...any) error {
	return func(dest ...any) error {
		*(dest[0].(*int64)) = seq
		*(dest[1].(*int64)) = threshold
		return nil
	}
}

// scanTip serves a present canonical tip row.
func scanTip(number int64, hash string) func(...any) error {
	return func(dest ...any) error {
		*(dest[0].(*int64)) = number
		*(dest[1].(*string)) = hash
		return nil
	}
}

// scanCount serves a COUNT(*) row.
func scanCount(n int64) func(...any) error {
	return func(dest ...any) error {
		*(dest[0].(*int64)) = n
		return nil
	}
}

// scanOne serves a present single-row existence check (e.g. a pause row:
// err==nil from the existence SELECT means the pause is effective).
func scanOne() func(...any) error {
	return func(dest ...any) error {
		*(dest[0].(*int)) = 1
		return nil
	}
}

// confirmCandidateRow builds one batch row: block_number, block_hash,
// tx_hash, log_index.
func confirmCandidateRow(h int64, bh, tx string, li int64) []any {
	return []any{h, bh, tx, li}
}

// newConfirmTestScanner builds a scanner through the real constructor
// against a dummy pool (never dialled: the tick reads use db) and swaps the
// read surface for the fake querier.
func newConfirmTestScanner(t *testing.T, q *confirmFakeQuerier, committer *confirmFakeCommitter, m *confirmFakeMetrics) *ConfirmationScanner {
	t.Helper()
	sc, err := NewConfirmationScanner(&pgxpool.Pool{}, confirmTestConfig(), committer, m)
	if err != nil {
		t.Fatalf("NewConfirmationScanner() error = %v", err)
	}
	sc.db = q
	return sc
}

func TestNewConfirmationScannerValidation(t *testing.T) {
	good := confirmTestConfig()
	committer := &confirmFakeCommitter{}
	pool := &pgxpool.Pool{}
	if _, err := NewConfirmationScanner(nil, good, committer, nil); err == nil {
		t.Fatal("nil pool must be an error")
	}
	if _, err := NewConfirmationScanner(pool, good, nil, nil); err == nil {
		t.Fatal("nil committer must be an error")
	}
	bad := good
	bad.ThresholdN = 0
	if _, err := NewConfirmationScanner(pool, bad, committer, nil); err == nil {
		t.Fatal("N=0 must be an error")
	}
	bad = good
	bad.ChainID = 0
	if _, err := NewConfirmationScanner(pool, bad, committer, nil); err == nil {
		t.Fatal("chain id 0 must be an error")
	}
	if _, err := NewConfirmationScanner(pool, good, committer, nil); err != nil {
		t.Fatalf("valid config with nil metrics must construct: %v", err)
	}
}

func TestClassifyConfirmationOutcome(t *testing.T) {
	chainErr := func(detail string) error { return &ConfirmationChainViewError{detail: detail} }
	tests := []struct {
		name       string
		err        error
		want       confirmOutcome
		wantReason string
	}{
		{"committed", nil, confirmCommitted, ""},
		{"stale_reticks", errStaleState, confirmRetryTick, ""},
		{"drift_halts", &ConfirmationDriftError{detail: "x"}, confirmHalt, "policy_drift"},
		{"pause_halts", &streamPauseError{stream: "log_pause", chainID: 7}, confirmHalt, "pause_present"},
		{"gate_fail_waits_row", chainErr("re-computed gate fails: tip=10 h=5 N=10"), confirmWaitRow, "below_depth"},
		{"tip_missing_halts", chainErr("canonical tip is missing under the lock"), confirmHalt, "tip_missing"},
		{"tip_moved_halts", chainErr("captured tip (10 a) differs from canonical tip (11 b)"), confirmHalt, "tip_untrusted"},
		{"reference_missing_halts", chainErr("reference block 5 is missing or non-canonical under the lock"), confirmHalt, "reference_unverifiable"},
		{"hash_mismatch_halts", chainErr("reference block 5 hash a diverges from candidate bh b"), confirmHalt, "reference_unverifiable"},
		{"candidate_missing_halts", chainErr("candidate a/b/0 is missing under the lock"), confirmHalt, "reference_unverifiable"},
		{"candidate_reread_mismatch_halts", chainErr("candidate re-read (status=pending block_number=5 block_hash=a) mismatches captured (h=5 bh=b)"), confirmHalt, "reference_unverifiable"},
		{"candidate_bad_status_halts", chainErr(`candidate status "orphaned" is neither pending nor confirmed`), confirmHalt, "reference_unverifiable"},
		{"negative_tip_halts", chainErr("canonical tip number -1 is negative"), confirmHalt, "reference_unverifiable"},
		{"wrapped_reference_missing_halts", fmt.Errorf("commit: %w", chainErr("reference block 5 is missing or non-canonical under the lock")), confirmHalt, "reference_unverifiable"},
		{"wrapped_drift_halts", fmt.Errorf("commit: %w", &ConfirmationDriftError{detail: "x"}), confirmHalt, "policy_drift"},
		{"wrapped_pause_halts", fmt.Errorf("commit: %w", &streamPauseError{stream: "deposit_pause", chainID: 7}), confirmHalt, "pause_present"},
		{"lease_lost_halts", errors.Join(ErrLeaseLost, errors.New(" verdict failed")), confirmHalt, "lease_lost"},
		{"startup_mismatch_halts", &confirmationConfigMismatchError{detail: "x"}, confirmHalt, "policy_drift"},
		{"transient_backs_off", errors.New("connection refused"), confirmBackoff, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := classifyConfirmationOutcome(tc.err)
			if got != tc.want || reason != tc.wantReason {
				t.Fatalf("classify(%v) = (%d, %q), want (%d, %q)",
					tc.err, got, reason, tc.want, tc.wantReason)
			}
		})
	}
}

func TestConfirmationScanEmptyState(t *testing.T) {
	tipHash := "0x" + strings.Repeat("ab", 32)
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationTipSQL:      scanTip(100, tipHash),
			countConfirmationPendingSQL: scanCount(0),
		},
	}
	committer := &confirmFakeCommitter{}
	m := &confirmFakeMetrics{}
	sc := newConfirmTestScanner(t, q, committer, m)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := sc.ServeLoop(ctx, &Lease{}, nil); err != nil {
		t.Fatalf("ServeLoop() on empty state = %v, want nil (ctx shutdown)", err)
	}
	if len(committer.calls) != 0 {
		t.Fatalf("empty state committed %d candidates, want 0", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got == 3 {
		t.Fatalf("ConfirmationState() = 3 on empty state, want running-idle/waiting (never stopped)")
	}
	if _, ok := sc.ConfirmationProgress(); ok {
		t.Fatal("ConfirmationProgress() ok = true on empty state, want false (no conversion yet)")
	}
	if len(m.pending) == 0 || m.pending[len(m.pending)-1] != 0 {
		t.Fatalf("pending gauge samples = %v, want trailing 0", m.pending)
	}
	for _, ok := range m.policyOK {
		if ok {
			t.Fatal("policy_seq exposed while no policy row exists, want absent")
		}
	}
	for _, ok := range m.lagOK {
		if ok {
			t.Fatal("lag exposed with zero confirmations, want absent")
		}
	}
	seen := false
	for _, c := range q.calls {
		if c == readConfirmationCandidatesSQL {
			seen = true
		}
	}
	if !seen {
		t.Fatal("empty-state tick never issued the ordered candidate fetch")
	}
}

func TestConfirmationScanBatchHalt(t *testing.T) {
	tipHash := "0x" + strings.Repeat("ab", 32)
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL:   scanPolicy(1, 10),
			readConfirmationTipSQL:      scanTip(100, tipHash),
			countConfirmationPendingSQL: scanCount(3),
		},
		batch: [][]any{
			confirmCandidateRow(90, "0x"+strings.Repeat("aa", 32), "0xtx1", 0),
			confirmCandidateRow(91, "0x"+strings.Repeat("bb", 32), "0xtx2", 1),
			confirmCandidateRow(92, "0x"+strings.Repeat("cc", 32), "0xtx3", 2),
		},
	}
	committer := &confirmFakeCommitter{
		fn: func(call int, _ ConfirmBasis) error {
			if call == 0 {
				return nil
			}
			return &ConfirmationChainViewError{detail: "reference block 91 is missing or non-canonical under the lock"}
		},
	}
	m := &confirmFakeMetrics{}
	sc := newConfirmTestScanner(t, q, committer, m)

	err := sc.ServeLoop(context.Background(), &Lease{}, nil)
	var chainView *ConfirmationChainViewError
	if !errors.As(err, &chainView) {
		t.Fatalf("ServeLoop() = %v (%T), want *ConfirmationChainViewError halt", err, err)
	}
	if len(committer.calls) != 2 {
		t.Fatalf("committer calls = %d, want 2 (third row never attempted after halt)", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	if m.confirmed != 1 || m.transitions["ok"] != 1 || m.transitions["stale"] != 1 {
		t.Fatalf("counters = confirmed %d transitions %v, want confirmed 1 ok 1 stale 1",
			m.confirmed, m.transitions)
	}
	if h, ok := sc.ConfirmationProgress(); !ok || h != 90 {
		t.Fatalf("ConfirmationProgress() = (%d, %v), want (90, true)", h, ok)
	}
	first := committer.calls[0]
	if first.TipNumber != 100 || first.TipHash != tipHash || first.PolicySeq != 1 || first.ThresholdN != 10 || first.Height != 90 {
		t.Fatalf("captured basis = %+v, want (T=100 S=1 N=10 h=90)", first)
	}
}

func TestConfirmationBelowDepthContinuesBatch(t *testing.T) {
	tipHash := "0x" + strings.Repeat("ab", 32)
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL:   scanPolicy(1, 10),
			readConfirmationTipSQL:      scanTip(100, tipHash),
			countConfirmationPendingSQL: scanCount(2),
		},
		batch: [][]any{
			confirmCandidateRow(90, "0x"+strings.Repeat("aa", 32), "0xtx1", 0),
			confirmCandidateRow(91, "0x"+strings.Repeat("bb", 32), "0xtx2", 1),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	committer := &confirmFakeCommitter{
		fn: func(call int, _ ConfirmBasis) error {
			if call == 0 {
				return &ConfirmationChainViewError{detail: "re-computed gate fails: tip=100 h=90 N=10"}
			}
			// The fake batch replays every tick; stop after the second
			// row proves the batch continued past the below_depth wait.
			cancel()
			return nil
		},
	}
	m := &confirmFakeMetrics{}
	sc := newConfirmTestScanner(t, q, committer, m)

	if err := sc.ServeLoop(ctx, &Lease{}, nil); err != nil {
		t.Fatalf("ServeLoop() with below_depth row = %v, want nil (ctx shutdown)", err)
	}
	if len(committer.calls) != 2 {
		t.Fatalf("committer calls = %d, want 2 (below_depth continues the batch)", len(committer.calls))
	}
	if m.skipped["below_depth"] != 1 {
		t.Fatalf("skipped = %v, want below_depth 1", m.skipped)
	}
	if m.confirmed != 1 {
		t.Fatalf("confirmed = %d, want 1", m.confirmed)
	}
	if got := sc.ConfirmationState(); got == 3 {
		t.Fatalf("ConfirmationState() = 3 after below_depth wait, want non-stopped")
	}
	if h, ok := sc.ConfirmationProgress(); !ok || h != 91 {
		t.Fatalf("ConfirmationProgress() = (%d, %v), want (91, true)", h, ok)
	}
}

func TestConfirmationStartupDriftStops(t *testing.T) {
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL: scanPolicy(2, 999),
		},
	}
	committer := &confirmFakeCommitter{}
	sc := newConfirmTestScanner(t, q, committer, &confirmFakeMetrics{})

	err := sc.ServeLoop(context.Background(), &Lease{}, nil)
	var mismatch *confirmationConfigMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("ServeLoop() = %v (%T), want *confirmationConfigMismatchError", err, err)
	}
	if len(committer.calls) != 0 {
		t.Fatalf("drifted startup committed %d candidates, want 0", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
}

func TestConfirmationTipMissingWaits(t *testing.T) {
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			countConfirmationPendingSQL: scanCount(0),
		},
	}
	committer := &confirmFakeCommitter{}
	sc := newConfirmTestScanner(t, q, committer, &confirmFakeMetrics{})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := sc.ServeLoop(ctx, &Lease{}, nil); err != nil {
		t.Fatalf("ServeLoop() with missing tip = %v, want nil (ctx shutdown)", err)
	}
	if len(committer.calls) != 0 {
		t.Fatalf("missing tip committed %d candidates, want 0", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got != 1 {
		t.Fatalf("ConfirmationState() = %d, want 1 (waiting on trusted tip)", got)
	}
}

func TestConfirmationTipUntrustedWaits(t *testing.T) {
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL:   scanPolicy(1, 10),
			readConfirmationTipSQL:      scanTip(100, ""),
			countConfirmationPendingSQL: scanCount(2),
		},
	}
	committer := &confirmFakeCommitter{}
	sc := newConfirmTestScanner(t, q, committer, &confirmFakeMetrics{})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := sc.ServeLoop(ctx, &Lease{}, nil); err != nil {
		t.Fatalf("ServeLoop() with untrusted tip = %v, want nil (ctx shutdown)", err)
	}
	if len(committer.calls) != 0 {
		t.Fatalf("untrusted tip committed %d candidates, want 0", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got != 1 {
		t.Fatalf("ConfirmationState() = %d, want 1 (waiting on trusted tip)", got)
	}
}

func TestConfirmationPausePrecheckStops(t *testing.T) {
	tipHash := "0x" + strings.Repeat("ab", 32)
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL:   scanPolicy(1, 10),
			readConfirmationTipSQL:      scanTip(100, tipHash),
			countConfirmationPendingSQL: scanCount(2),
			depositPauseExistsSQL:       scanOne(),
		},
	}
	committer := &confirmFakeCommitter{}
	m := &confirmFakeMetrics{}
	sc := newConfirmTestScanner(t, q, committer, m)

	err := sc.ServeLoop(context.Background(), &Lease{}, nil)
	var paused *streamPauseError
	if !errors.As(err, &paused) {
		t.Fatalf("ServeLoop() = %v (%T), want *streamPauseError halt", err, err)
	}
	if len(committer.calls) != 0 {
		t.Fatalf("pause-present tick committed %d candidates, want 0", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	if m.transitions["rejected"] != 1 {
		t.Fatalf("transitions = %v, want rejected 1", m.transitions)
	}
	for _, c := range q.calls {
		if upper := strings.ToUpper(c); strings.Contains(upper, "INSERT") ||
			strings.Contains(upper, "UPDATE") || strings.Contains(upper, "DELETE") {
			t.Fatalf("pause halt issued a write statement, want reads only:\n%s", c)
		}
	}
}

func TestConfirmationCommitDriftStopsBatch(t *testing.T) {
	tipHash := "0x" + strings.Repeat("ab", 32)
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL:   scanPolicy(1, 10),
			readConfirmationTipSQL:      scanTip(100, tipHash),
			countConfirmationPendingSQL: scanCount(3),
		},
		batch: [][]any{
			confirmCandidateRow(90, "0x"+strings.Repeat("aa", 32), "0xtx1", 0),
			confirmCandidateRow(91, "0x"+strings.Repeat("bb", 32), "0xtx2", 1),
			confirmCandidateRow(92, "0x"+strings.Repeat("cc", 32), "0xtx3", 2),
		},
	}
	committer := &confirmFakeCommitter{
		fn: func(call int, _ ConfirmBasis) error {
			if call == 0 {
				return nil
			}
			return &ConfirmationDriftError{detail: "captured policy (seq=1 N=10) differs from effective (seq=2 N=20)"}
		},
	}
	m := &confirmFakeMetrics{}
	sc := newConfirmTestScanner(t, q, committer, m)

	err := sc.ServeLoop(context.Background(), &Lease{}, nil)
	var drift *ConfirmationDriftError
	if !errors.As(err, &drift) {
		t.Fatalf("ServeLoop() = %v (%T), want *ConfirmationDriftError halt", err, err)
	}
	if len(committer.calls) != 2 {
		t.Fatalf("committer calls = %d, want 2 (third row never attempted after halt)", len(committer.calls))
	}
	if got := sc.ConfirmationState(); got != 3 {
		t.Fatalf("ConfirmationState() = %d, want 3 (stopped)", got)
	}
	if m.confirmed != 1 || m.transitions["ok"] != 1 || m.transitions["rejected"] != 1 {
		t.Fatalf("counters = confirmed %d transitions %v, want confirmed 1 ok 1 rejected 1",
			m.confirmed, m.transitions)
	}
}

func TestConfirmationCommitLeaseLostStopsBatch(t *testing.T) {
	tipHash := "0x" + strings.Repeat("ab", 32)
	q := &confirmFakeQuerier{
		rows: map[string]func(dest ...any) error{
			readConfirmationPolicySQL:   scanPolicy(1, 10),
			readConfirmationTipSQL:      scanTip(100, tipHash),
			countConfirmationPendingSQL: scanCount(3),
		},
		batch: [][]any{
			confirmCandidateRow(90, "0x"+strings.Repeat("aa", 32), "0xtx1", 0),
			confirmCandidateRow(91, "0x"+strings.Repeat("bb", 32), "0xtx2", 1),
			confirmCandidateRow(92, "0x"+strings.Repeat("cc", 32), "0xtx3", 2),
		},
	}
	committer := &confirmFakeCommitter{
		fn: func(call int, _ ConfirmBasis) error {
			if call == 0 {
				return nil
			}
			return errors.Join(ErrLeaseLost, errors.New("owner/fencing/expiry verdict failed"))
		},
	}
	m := &confirmFakeMetrics{}
	sc := newConfirmTestScanner(t, q, committer, m)

	err := sc.ServeLoop(context.Background(), &Lease{}, nil)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ServeLoop() = %v (%T), want ErrLeaseLost halt", err, err)
	}
	if len(committer.calls) != 2 {
		t.Fatalf("committer calls = %d, want 2 (third row never attempted after halt)", len(committer.calls))
	}
	if m.confirmed != 1 {
		t.Fatalf("confirmed = %d, want 1 (first row stays committed, rest zero writes)", m.confirmed)
	}
}

func TestConfirmationSnapshotsInitial(t *testing.T) {
	sc := newConfirmTestScanner(t, &confirmFakeQuerier{}, &confirmFakeCommitter{}, &confirmFakeMetrics{})
	if got := sc.ConfirmationState(); got != 0 {
		t.Fatalf("initial ConfirmationState() = %d, want 0", got)
	}
	if _, ok := sc.ConfirmationProgress(); ok {
		t.Fatal("initial ConfirmationProgress() ok = true, want false")
	}
}

func TestConfirmationCandidateSQLShape(t *testing.T) {
	for _, want := range []string{
		"status = 'pending'",
		"block_number <= $2",
		"ORDER BY block_number, log_index",
		"LIMIT $3",
		"chain_id = $1",
	} {
		if !strings.Contains(readConfirmationCandidatesSQL, want) {
			t.Fatalf("candidate SQL is missing %q:\n%s", want, readConfirmationCandidatesSQL)
		}
	}
}
