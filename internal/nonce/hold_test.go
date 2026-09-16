package nonce

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const holdTestSender = "0x1111111111111111111111111111111111111111"

// sp boxes a string for the fakes' nullable scan targets.
func sp(s string) *string { return &s }

// TestHoldEstablishmentMultiCauseCoexistence pins that each classification
// cause establishes its own row on the same scope, that every instance is
// active and independently identified, and that establishment never touches
// scope state or mutates existing rows (data-model Table 6, R7).
func TestHoldEstablishmentMultiCauseCoexistence(t *testing.T) {
	ctx := context.Background()
	f := &fakeQuerier{}
	f.handler = func(sql string, args []any) pgx.Row { return noRows() }
	var inserted []struct{ holdID, cause, obs string }
	f.execErr = func(sql string, args []any) error {
		if strings.Contains(sql, "INSERT INTO nonce_scope_holds") {
			inserted = append(inserted, struct{ holdID, cause, obs string }{
				args[0].(string), args[3].(string), args[4].(string)})
		}
		return nil
	}

	h1, err := establishHoldTx(ctx, f, HoldEstablishment{
		ChainID: 1, Sender: holdTestSender, Cause: CauseUnattributedConsumption,
		ObservationID: "no-a", Detail: "pending above frontier"})
	if err != nil {
		t.Fatalf("establish unattributed hold: %v", err)
	}
	h2, err := establishHoldTx(ctx, f, HoldEstablishment{
		ChainID: 1, Sender: holdTestSender, Cause: CauseUnexplainedGap,
		ObservationID: "no-b"})
	if err != nil {
		t.Fatalf("establish unexplained-gap hold: %v", err)
	}

	if h1.HoldID == h2.HoldID {
		t.Fatal("distinct causes share one hold instance")
	}
	if !strings.HasPrefix(h1.HoldID, "nh-") || len(h1.HoldID) != 35 {
		t.Fatalf("hold id %q is not nh- + 32 hex", h1.HoldID)
	}
	if h1.Status != HoldStatusActive || h2.Status != HoldStatusActive {
		t.Fatalf("established holds not active: %q, %q", h1.Status, h2.Status)
	}
	if h1.Cause != CauseUnattributedConsumption || h2.Cause != CauseUnexplainedGap {
		t.Fatalf("causes not carried: %q, %q", h1.Cause, h2.Cause)
	}
	if h1.EvidenceObservationID != "no-a" || h2.EvidenceObservationID != "no-b" {
		t.Fatalf("evidence links not carried: %q, %q", h1.EvidenceObservationID, h2.EvidenceObservationID)
	}
	if len(inserted) != 2 || inserted[0].holdID != h1.HoldID || inserted[1].holdID != h2.HoldID {
		t.Fatalf("expected exactly two append-only inserts, got %+v", inserted)
	}
	for _, sql := range f.recordedExecs() {
		if strings.Contains(sql, "nonce_scope_state") {
			t.Fatalf("hold establishment touched scope state: %q", sql)
		}
		if strings.Contains(sql, "UPDATE") {
			t.Fatalf("hold establishment mutated a row: %q", sql)
		}
	}

	f.handler = func(sql string, args []any) pgx.Row {
		if strings.Contains(sql, "SELECT EXISTS") {
			return rows(true)
		}
		return noRows()
	}
	held, err := scopeIsHeldTx(ctx, f, 1, holdTestSender)
	if err != nil {
		t.Fatalf("derived held state: %v", err)
	}
	if !held {
		t.Fatal("scope with two active causes reported not held")
	}
}

// TestScopeHeldDerivedFromActiveRowsOnly pins the no-scope-level-boolean rule:
// admission-held is computed from active hold rows on every read, filters
// released rows out, and consults nothing outside nonce_scope_holds.
func TestScopeHeldDerivedFromActiveRowsOnly(t *testing.T) {
	ctx := context.Background()
	f := &fakeQuerier{}
	f.handler = func(sql string, args []any) pgx.Row {
		if !strings.Contains(sql, "FROM nonce_scope_holds") {
			t.Fatalf("held derivation must read hold rows, got %q", sql)
		}
		if !strings.Contains(sql, "status = 'active'") {
			t.Fatalf("held derivation must filter active rows, got %q", sql)
		}
		if strings.Contains(sql, "nonce_scope_state") {
			t.Fatalf("held derivation consulted scope state: %q", sql)
		}
		return rows(false)
	}
	held, err := scopeIsHeldTx(ctx, f, 1, holdTestSender)
	if err != nil {
		t.Fatalf("no active rows: %v", err)
	}
	if held {
		t.Fatal("no active rows reported held")
	}

	f.handler = func(sql string, args []any) pgx.Row { return rows(true) }
	held, err = scopeIsHeldTx(ctx, f, 1, holdTestSender)
	if err != nil {
		t.Fatalf("one active row: %v", err)
	}
	if !held {
		t.Fatal("one active row reported not held")
	}
	if got := len(f.recordedQueries()); got != 2 {
		t.Fatalf("held state was cached: %d reads for 2 derivations", got)
	}
}

// TestReleasedCauseRedetectedCreatesNewInstance pins the append-only instance
// rule: a released hold no longer matches the active reader (the query filters
// status='active'), so re-detecting the same cause appends a NEW row with a
// fresh id and the new evidence link — it never reopens or updates the
// released row.
func TestReleasedCauseRedetectedCreatesNewInstance(t *testing.T) {
	ctx := context.Background()
	f := &fakeQuerier{}
	f.handler = func(sql string, args []any) pgx.Row {
		if !strings.Contains(sql, "status = 'active'") {
			t.Fatalf("cause lookup must exclude released instances, got %q", sql)
		}
		return noRows() // the released instance is history, not an active match
	}
	var holdIDs []string
	f.execErr = func(sql string, args []any) error {
		if strings.Contains(sql, "INSERT INTO nonce_scope_holds") {
			holdIDs = append(holdIDs, args[0].(string))
		}
		return nil
	}

	est := HoldEstablishment{ChainID: 1, Sender: holdTestSender,
		Cause: CauseChainViewDivergence, ObservationID: "no-1"}
	first, err := establishHoldTx(ctx, f, est)
	if err != nil {
		t.Fatalf("first establishment: %v", err)
	}
	est.ObservationID = "no-2"
	second, err := establishHoldTx(ctx, f, est)
	if err != nil {
		t.Fatalf("re-establishment after release: %v", err)
	}

	if first.HoldID == second.HoldID {
		t.Fatal("re-detected released cause reused the disposed instance")
	}
	if second.Status != HoldStatusActive || second.EvidenceObservationID != "no-2" {
		t.Fatalf("second instance = %+v, want a fresh active instance anchored to no-2", second)
	}
	if len(holdIDs) != 2 || holdIDs[0] == holdIDs[1] {
		t.Fatalf("expected two distinct appended instances, got %v", holdIDs)
	}
	for _, sql := range f.recordedExecs() {
		if strings.Contains(sql, "UPDATE") {
			t.Fatalf("re-detection mutated a row instead of appending: %q", sql)
		}
	}
}

// TestActiveCauseRedetectedConverges pins the other half of "one row per cause
// instance": while the instance is still active, re-detecting the cause
// converges on it untouched (zero writes, original evidence link).
func TestActiveCauseRedetectedConverges(t *testing.T) {
	ctx := context.Background()
	establishedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	f := &fakeQuerier{}
	f.handler = func(sql string, args []any) pgx.Row {
		return rows("nh-existing", int64(1), holdTestSender, CauseUnexplainedGap,
			HoldStatusActive, establishedAt, "no-old", "detail",
			nil, nil, nil, nil, nil)
	}
	got, err := establishHoldTx(ctx, f, HoldEstablishment{
		ChainID: 1, Sender: holdTestSender, Cause: CauseUnexplainedGap, ObservationID: "no-new"})
	if err != nil {
		t.Fatalf("convergence: %v", err)
	}
	if got.HoldID != "nh-existing" || got.EvidenceObservationID != "no-old" {
		t.Fatalf("active instance not converged untouched: %+v", got)
	}
	if f.execCount() != 0 {
		t.Fatalf("convergence issued %d writes, want 0", f.execCount())
	}
}

// TestHoldEstablishmentRefusals pins the input validation: unknown causes and
// missing evidence refusals issue zero statements.
func TestHoldEstablishmentRefusals(t *testing.T) {
	ctx := context.Background()
	f := &fakeQuerier{}
	if _, err := establishHoldTx(ctx, f, HoldEstablishment{
		ChainID: 1, Sender: holdTestSender, Cause: "recovery_active", ObservationID: "no-1"}); err == nil {
		t.Fatal("006/unknown cause accepted as a 008 hold cause")
	}
	if _, err := establishHoldTx(ctx, f, HoldEstablishment{
		ChainID: 1, Sender: holdTestSender, Cause: CauseUnexplainedGap, ObservationID: ""}); err == nil {
		t.Fatal("establishment without an evidence observation id accepted")
	}
	if f.execCount() != 0 || len(f.recordedQueries()) != 0 {
		t.Fatalf("refusals touched %d execs / %d queries, want 0/0", f.execCount(), len(f.recordedQueries()))
	}
}

// TestScopeStateReader pins the scope frontier reader: NUMERIC text roundtrip,
// NULL facts stay nil, a missing scope row yields (nil, nil).
func TestScopeStateReader(t *testing.T) {
	ctx := context.Background()
	updatedAt := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)

	f := &fakeQuerier{}
	f.handler = func(sql string, args []any) pgx.Row {
		if !strings.Contains(sql, "FROM nonce_scope_state") {
			t.Fatalf("unexpected query %q", sql)
		}
		return rows(sp("7"), sp("10"), sp("9"), sp("no-3"), updatedAt)
	}
	st, err := readScopeStateTx(ctx, f, 1, holdTestSender)
	if err != nil {
		t.Fatalf("read scope state: %v", err)
	}
	if bigText(st.ReconciledFloor) != "7" || bigText(st.LastLatest) != "10" || bigText(st.LastPending) != "9" {
		t.Fatalf("frontier parsed wrong: F=%s L=%s P=%s",
			bigText(st.ReconciledFloor), bigText(st.LastLatest), bigText(st.LastPending))
	}
	if st.LastObservationID == nil || *st.LastObservationID != "no-3" {
		t.Fatalf("last observation id = %v, want no-3", st.LastObservationID)
	}
	if !st.UpdatedAt.Equal(updatedAt) || st.ChainID != 1 || st.Sender != holdTestSender {
		t.Fatalf("scope identity/updated_at wrong: %+v", st)
	}

	f.handler = func(sql string, args []any) pgx.Row { return rows(nil, nil, nil, nil, updatedAt) }
	st, err = readScopeStateTx(ctx, f, 1, holdTestSender)
	if err != nil {
		t.Fatalf("read NULL scope state: %v", err)
	}
	if st.ReconciledFloor != nil || st.LastLatest != nil || st.LastPending != nil || st.LastObservationID != nil {
		t.Fatalf("NULL facts did not scan as nil: %+v", st)
	}

	f.handler = func(sql string, args []any) pgx.Row { return noRows() }
	st, err = readScopeStateTx(ctx, f, 1, holdTestSender)
	if err != nil || st != nil {
		t.Fatalf("missing scope row = (%+v, %v), want (nil, nil)", st, err)
	}
}

// TestScopeFrontierWriters pins the writer semantics: last_latest/
// last_pending/last_observation_id are recorded as integer-exact NUMERIC
// carriers, the frontier update never moves the floor, the floor statement
// carries the monotonic GREATEST guard, and out-of-range or evidence-less
// inputs refuse with zero writes.
func TestScopeFrontierWriters(t *testing.T) {
	ctx := context.Background()

	f := &fakeQuerier{}
	var frontierSQL string
	var frontierArgs []any
	f.execErr = func(sql string, args []any) error {
		frontierSQL, frontierArgs = sql, args
		return nil
	}
	if err := updateScopeFrontierTx(ctx, f, 1, holdTestSender,
		classifyTestBig(10), classifyTestBig(9), "no-4"); err != nil {
		t.Fatalf("update scope frontier: %v", err)
	}
	for _, want := range []string{"last_latest", "last_pending", "last_observation_id"} {
		if !strings.Contains(frontierSQL, want) {
			t.Fatalf("frontier update missing %s: %q", want, frontierSQL)
		}
	}
	if strings.Contains(frontierSQL, "reconciled_floor") {
		t.Fatalf("frontier update must not move the floor: %q", frontierSQL)
	}
	for i, want := range []int64{10, 9} {
		nv, ok := frontierArgs[2+i].(pgtype.Numeric)
		if !ok || !nv.Valid || nv.Exp != 0 || nv.Int.Cmp(classifyTestBig(want)) != 0 {
			t.Fatalf("frontier arg %d = %#v, want integer-exact %d", i, frontierArgs[2+i], want)
		}
	}
	if frontierArgs[4].(string) != "no-4" {
		t.Fatalf("observation id arg = %v, want no-4", frontierArgs[4])
	}

	bad := &fakeQuerier{}
	if err := updateScopeFrontierTx(ctx, bad, 1, holdTestSender, classifyTestBig(-1), classifyTestBig(0), "no"); err == nil {
		t.Fatal("negative latest accepted")
	}
	if err := updateScopeFrontierTx(ctx, bad, 1, holdTestSender, classifyTestBig(0), classifyTestBig(0), ""); err == nil {
		t.Fatal("frontier update without an observation id accepted")
	}
	if bad.execCount() != 0 {
		t.Fatalf("frontier refusals issued %d writes, want 0", bad.execCount())
	}

	floor := &fakeQuerier{}
	var floorSQL string
	var floorArgs []any
	floor.execErr = func(sql string, args []any) error {
		floorSQL, floorArgs = sql, args
		return nil
	}
	moved, err := advanceScopeFloorTx(ctx, floor, 1, holdTestSender, classifyTestBig(12))
	if err != nil || !moved {
		t.Fatalf("advance floor = (%v, %v), want (true, nil)", moved, err)
	}
	if !strings.Contains(floorSQL, "GREATEST(COALESCE(reconciled_floor, 0)") {
		t.Fatalf("floor statement lost the monotonic GREATEST guard: %q", floorSQL)
	}
	if !strings.Contains(floorSQL, "reconciled_floor < $3") {
		t.Fatalf("floor statement lost the advance guard: %q", floorSQL)
	}
	if nv, ok := floorArgs[2].(pgtype.Numeric); !ok || !nv.Valid || nv.Int.Cmp(classifyTestBig(12)) != 0 {
		t.Fatalf("floor arg = %#v, want 12", floorArgs[2])
	}

	over := &fakeQuerier{}
	tooBig := new(big.Int).Add(MaxNonceBig(), classifyTestBig(1))
	if moved, err := advanceScopeFloorTx(ctx, over, 1, holdTestSender, tooBig); err == nil || moved {
		t.Fatalf("out-of-range floor = (%v, %v), want refusal", moved, err)
	}
	if over.execCount() != 0 {
		t.Fatalf("out-of-range floor issued %d writes, want 0", over.execCount())
	}
}
