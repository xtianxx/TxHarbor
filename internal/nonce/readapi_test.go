// readapi_test.go is the T013 EARLY-VALIDATION suite for the read provider:
// scripted querier doubles only (no acceptance claim rests on them; real
// PostgreSQL integration belongs to T018/T019/T022). It pins the five-outcome
// mapping, the exact-bound body shape (snake_case, decimal nonce, lowercase
// scope, fixed notice), the gate/recovery annotations, the constant-time
// bearer check, and the all-or-nothing unavailable behavior.
package nonce

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	readTestChain   = int64(31337)
	readTestSender  = "0x4444444444444444444444444444444444444444"
	readTestBinding = "nb-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func readPtr(v int64) *int64 { return &v }

// readBindingValues renders an admin-style binding row in bindingColumnsSQL
// scan order.
func readBindingValues(state string) []any {
	now := time.Now().UTC()
	b := Binding{
		BindingID:               readTestBinding,
		IntentID:                "intent-read",
		ChainID:                 readTestChain,
		Sender:                  readTestSender,
		Nonce:                   big.NewInt(41),
		State:                   state,
		AuthorizationID:         "wa-read",
		AuthorizationVersion:    strings.Repeat("c", 64),
		RegistrySeq:             3,
		AllocationObservationID: "no-prev",
		CreatedAt:               now,
		UpdatedAt:               now,
	}
	switch state {
	case StateConsumed:
		b.ConsumedAt = &now
	case StateReleased:
		op := "op-old"
		b.ReleasedAt = &now
		b.ReleaseOperationID = &op
	}
	return allocBindingValues(b)
}

// readHoldValues renders one active nonce_scope_holds row in holdColumnsSQL
// scan order.
func readHoldValues(id, cause string) []any {
	return []any{
		id, readTestChain, readTestSender, cause, HoldStatusActive, time.Now().UTC(),
		"no-prev", "seed",
		nil, nil, nil, nil, nil,
	}
}

// readFakeTx is a txQuerier with a transaction surface, driving the multi-row
// readers through rowsFn (the shared fakes only cover QueryRow).
type readFakeTx struct {
	*fakeQuerier
	rowsFn                func(sql string, args []any) (pgx.Rows, error)
	commitErr             error
	committed, rolledBack bool
}

func (t *readFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.mu.Lock()
	t.queries = append(t.queries, sql)
	t.mu.Unlock()
	if t.rowsFn == nil {
		return allocRowSet(), nil
	}
	return t.rowsFn(sql, args)
}

func (t *readFakeTx) Commit(context.Context) error {
	t.committed = true
	return t.commitErr
}
func (t *readFakeTx) Rollback(context.Context) error { t.rolledBack = true; return nil }

// readProvider wires a provider over one scripted transaction.
func readProvider(tx *readFakeTx, token string) *ReadProvider {
	return &ReadProvider{
		token: token,
		begin: func(context.Context) (readTx, error) { return tx, nil },
	}
}

// readBaseHandler answers the identity lookup, the scope share lock, and the
// registry read; anything else is a miss (006 probe → none).
func readBaseHandler(holds ...[]any) (*fakeQuerier, *readFakeTx) {
	tx := &readFakeTx{
		fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "WHERE binding_id = $1"):
				return rows(readBindingValues(StateAllocated)...)
			case strings.Contains(sql, "WHERE intent_id = $1"):
				return rows(readBindingValues(StateAllocated)...)
			case strings.Contains(sql, "FOR SHARE"):
				return rows(readTestChain)
			case strings.Contains(sql, "nonce_wallet_registry"):
				return rows(allocRegistryValues(RegistryActive, 3)...)
			default:
				return noRows()
			}
		}},
		rowsFn: func(sql string, _ []any) (pgx.Rows, error) {
			if strings.Contains(sql, "nonce_scope_holds") {
				return allocRowSet(holds...), nil
			}
			return allocRowSet(), nil
		},
	}
	return tx.fakeQuerier, tx
}

func TestReadAPIBoundBodyAndAnnotations(t *testing.T) {
	_, tx := readBaseHandler(readHoldValues("nh-1", CauseUnexplainedGap))

	resp, err := readProvider(tx, "secret-token").ReadByBindingID(context.Background(), readTestBinding)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Outcome != ReadBound || resp.HTTPStatus() != 200 {
		t.Fatalf("outcome = %q/%d, want bound/200", resp.Outcome, resp.HTTPStatus())
	}
	if !tx.committed || tx.rolledBack {
		t.Fatalf("read tx = (committed=%v, rolledBack=%v), want commit only", tx.committed, tx.rolledBack)
	}
	if n := len(tx.recordedExecs()); n != 2 {
		t.Fatalf("read path executed %d statements, want exactly the two SET LOCAL guards", n)
	}
	for _, stmt := range tx.recordedExecs() {
		if !strings.HasPrefix(stmt, "SET LOCAL ") {
			t.Fatalf("read path executed a non-guard statement %q, want SET LOCAL guards only", stmt)
		}
	}
	if q := strings.Join(tx.recordedQueries(), "\n"); !strings.Contains(q, "FOR SHARE") {
		t.Fatalf("scope share lock missing from read queries:\n%s", q)
	} else if strings.Contains(q, "FOR UPDATE") {
		t.Fatalf("read path must never take a FOR UPDATE lock:\n%s", q)
	}

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		`"outcome":"bound"`,
		`"binding_id":"` + readTestBinding + `"`,
		`"nonce":"41"`,
		`"state":"allocated"`,
		`"sender":"` + readTestSender + `"`,
		`"authorization":{"id":"wa-read","version":"`,
		`"registry_seq":3`,
		`"gate":{"state":"held","causes":[{"hold_id":"nh-1","cause":"unexplained_gap","established_at":"`,
		`"recovery":{"state":"none"}`,
		`"registry_state":"active"`,
		`"notice":"` + ReadNoticeFacts + `"`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("bound body missing %q:\n%s", want, s)
		}
	}
}

func TestReadAPITerminalBody(t *testing.T) {
	tx := &readFakeTx{
		fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "WHERE binding_id = $1"):
				return rows(readBindingValues(StateReleased)...)
			case strings.Contains(sql, "FOR SHARE"):
				return rows(readTestChain)
			case strings.Contains(sql, "nonce_wallet_registry"):
				return rows(allocRegistryValues(RegistryDisabled, 4)...)
			default:
				return noRows()
			}
		}},
	}

	resp, err := readBindingOutcomeTx(context.Background(), tx, ReadRequest{BindingID: readTestBinding})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Outcome != ReadTerminal || resp.HTTPStatus() != 200 {
		t.Fatalf("outcome = %q/%d, want terminal/200", resp.Outcome, resp.HTTPStatus())
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		`"outcome":"terminal"`,
		`"state":"released"`,
		`"terminal_at":"`,
		`"release_operation_id":"op-old"`,
		`"gate":{"state":"open","causes":[]}`,
		`"registry_state":"disabled"`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("terminal body missing %q:\n%s", want, s)
		}
	}
}

func TestReadAPINotBoundAndMismatch(t *testing.T) {
	tx := &readFakeTx{fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
		if strings.Contains(sql, "WHERE binding_id = $1") {
			return noRows()
		}
		return noRows()
	}}}

	resp, err := readBindingOutcomeTx(context.Background(), tx, ReadRequest{BindingID: readTestBinding})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Outcome != ReadNotBound || resp.HTTPStatus() != 404 {
		t.Fatalf("outcome = %q/%d, want not_bound/404", resp.Outcome, resp.HTTPStatus())
	}
	if resp.Binding != nil {
		t.Fatalf("not_bound carried a binding: %+v", resp.Binding)
	}
	body, _ := json.Marshal(resp)
	if !strings.Contains(string(body), `"error":{"code":"not_bound"`) {
		t.Fatalf("not_bound body = %s", body)
	}
	if q := strings.Join(tx.recordedQueries(), "\n"); strings.Contains(q, "FOR SHARE") {
		t.Fatalf("not_bound must not lock a scope row:\n%s", q)
	}

	_, tx2 := readBaseHandler()
	mismatch, err := readBindingOutcomeTx(context.Background(), tx2,
		ReadRequest{IntentID: "intent-read", ExpectedChainID: readPtr(7), ExpectedSender: readTestSender})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mismatch.Outcome != ReadMismatch || mismatch.HTTPStatus() != 409 {
		t.Fatalf("outcome = %q/%d, want mismatch/409", mismatch.Outcome, mismatch.HTTPStatus())
	}
	if mismatch.DurableBindingID != readTestBinding || mismatch.DurableChainID == nil ||
		*mismatch.DurableChainID != readTestChain || mismatch.DurableSender != readTestSender {
		t.Fatalf("mismatch durable echo = (%q, %v, %q)", mismatch.DurableBindingID, mismatch.DurableChainID, mismatch.DurableSender)
	}
	body2, _ := json.Marshal(mismatch)
	s := string(body2)
	if !strings.Contains(s, `"error":{"code":"mismatch"`) || !strings.Contains(s, `"binding_id":"`+readTestBinding+`"`) {
		t.Fatalf("mismatch body = %s", s)
	}
}

func TestReadAPIUnknownRecoveryOn006ReadFailure(t *testing.T) {
	tx := &readFakeTx{
		fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "WHERE binding_id = $1"):
				return rows(readBindingValues(StateAllocated)...)
			case strings.Contains(sql, "FOR SHARE"):
				return rows(readTestChain)
			case strings.Contains(sql, "nonce_wallet_registry"):
				return rows(allocRegistryValues(RegistryActive, 3)...)
			case strings.Contains(sql, "reorg_recovery"):
				return fakeRow{err: errors.New("fake: 006 read exploded")}
			default:
				return noRows()
			}
		}},
	}

	resp, err := readBindingOutcomeTx(context.Background(), tx, ReadRequest{BindingID: readTestBinding})
	if err != nil {
		t.Fatalf("read must survive a 006 failure, got %v", err)
	}
	if resp.Outcome != ReadBound || resp.Annotations == nil {
		t.Fatalf("response = %+v, want a bound response with annotations", resp)
	}
	state := resp.Annotations.Recovery.State
	if state != RecoveryUnknown {
		t.Fatalf("recovery state = %q, want unknown", state)
	}
	if state == RecoveryNone || state == RecoveryReleased {
		t.Fatalf("006 failure was mapped to %q; unknown is the only safe value", state)
	}
	body, _ := json.Marshal(resp)
	if !strings.Contains(string(body), `"recovery":{"state":"unknown"}`) {
		t.Fatalf("body = %s", body)
	}
}

func TestReadAPIUnavailableAllOrNothing(t *testing.T) {
	tx := &readFakeTx{
		fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "WHERE binding_id = $1"):
				return rows(readBindingValues(StateAllocated)...)
			case strings.Contains(sql, "FOR SHARE"):
				return rows(readTestChain)
			case strings.Contains(sql, "nonce_wallet_registry"):
				return fakeRow{err: errors.New("fake: 008 read exploded")}
			default:
				return noRows()
			}
		}},
	}
	p := readProvider(tx, "secret-token")

	resp, err := p.ReadByBindingID(context.Background(), readTestBinding)
	if !IsOutcome(err, ReadUnavailable) {
		t.Fatalf("error = %v, want unavailable", err)
	}
	if resp.Outcome != ReadUnavailable || resp.HTTPStatus() != 503 {
		t.Fatalf("outcome = %q/%d, want unavailable/503", resp.Outcome, resp.HTTPStatus())
	}
	if resp.Binding != nil || resp.Annotations != nil {
		t.Fatalf("unavailable leaked a partial fact set: %+v", resp)
	}
	body, _ := json.Marshal(resp)
	s := string(body)
	if !strings.Contains(s, `"error":{"code":"unavailable"`) || !strings.Contains(s, `"notice":"`+ReadNoticeUnavailable+`"`) {
		t.Fatalf("unavailable body = %s", s)
	}
	if !tx.rolledBack || tx.committed {
		t.Fatalf("failed read tx = (committed=%v, rolledBack=%v), want rollback", tx.committed, tx.rolledBack)
	}
}

func TestReadAPIRequestValidate(t *testing.T) {
	if err := (ReadRequest{}).Validate(); err == nil {
		t.Fatal("empty read request accepted")
	}
	if err := (ReadRequest{BindingID: "nb-x", IntentID: "intent-x"}).Validate(); err == nil {
		t.Fatal("dual-key read request accepted")
	}
	if err := (ReadRequest{BindingID: "nb-x"}).Validate(); err != nil {
		t.Fatalf("valid binding-id request refused: %v", err)
	}
	if err := (ReadRequest{IntentID: "intent-x"}).Validate(); err != nil {
		t.Fatalf("valid intent request refused: %v", err)
	}
}

func TestReadAPIAuthAndStatusMapping(t *testing.T) {
	p := &ReadProvider{token: "secret-token"}
	if !p.Authenticate("secret-token") {
		t.Fatal("correct token refused")
	}
	if p.Authenticate("wrong-token") || p.Authenticate("") || p.Authenticate("secret-token ") {
		t.Fatal("invalid token admitted")
	}
	if (&ReadProvider{}).Authenticate("") {
		t.Fatal("unconfigured provider admitted a token")
	}

	unauth := UnauthenticatedResponse()
	if unauth.Outcome != ReadUnauthenticated || unauth.HTTPStatus() != 401 {
		t.Fatalf("unauthenticated = %q/%d, want 401", unauth.Outcome, unauth.HTTPStatus())
	}
	body, _ := json.Marshal(unauth)
	if !strings.Contains(string(body), `"outcome":"unauthenticated"`) ||
		!strings.Contains(string(body), `"code":"unauthenticated"`) {
		t.Fatalf("unauthenticated body = %s", body)
	}

	for _, tc := range []struct {
		outcome Outcome
		want    int
	}{
		{ReadBound, 200}, {ReadTerminal, 200}, {ReadNotBound, 404},
		{ReadMismatch, 409}, {ReadUnavailable, 503}, {ReadUnauthenticated, 401},
		{Outcome("future"), 503},
	} {
		if got := (ReadResponse{Outcome: tc.outcome}).HTTPStatus(); got != tc.want {
			t.Fatalf("HTTPStatus(%q) = %d, want %d", tc.outcome, got, tc.want)
		}
	}
}

// TestReadAPIGateClosedIsUnavailable pins read-api.md §2 "rebuild gate not
// open" → `unavailable` for both closed-gate states (startup zero value and a
// failed verification) and confirms an open gate proceeds to the read. A
// gate-closed read must never open a transaction (begin is nil on purpose).
func TestReadAPIGateClosedIsUnavailable(t *testing.T) {
	failed := NewRebuildGate()
	failed.KeepClosed("rebuild_incomplete: 3 of 49 named carriers present")
	for _, tc := range []struct {
		name string
		gate *RebuildGate
	}{
		{"startup_zero_value", NewRebuildGate()},
		{"verification_failed", failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &ReadProvider{token: "t", gate: tc.gate}
			resp, err := p.ReadByBindingID(context.Background(), readTestBinding)
			if !IsOutcome(err, ReadUnavailable) {
				t.Fatalf("error = %v, want unavailable", err)
			}
			if resp.Outcome != ReadUnavailable || resp.HTTPStatus() != 503 {
				t.Fatalf("outcome = %q/%d, want unavailable/503", resp.Outcome, resp.HTTPStatus())
			}
			if resp.Binding != nil || resp.Annotations != nil {
				t.Fatalf("gate-closed read leaked a partial fact set: %+v", resp)
			}
			body, _ := json.Marshal(resp)
			if !strings.Contains(string(body), `"error":{"code":"unavailable"`) ||
				!strings.Contains(string(body), `"notice":"`+ReadNoticeUnavailable+`"`) {
				t.Fatalf("gate-closed body = %s", body)
			}
		})
	}

	open := NewRebuildGate()
	open.Open()
	_, tx := readBaseHandler()
	p := readProvider(tx, "secret-token")
	p.gate = open
	resp, err := p.ReadByBindingID(context.Background(), readTestBinding)
	if err != nil || resp.Outcome != ReadBound {
		t.Fatalf("open-gate read = (%q, %v), want bound", resp.Outcome, err)
	}
	if !tx.committed {
		t.Fatal("open-gate read did not commit")
	}
}

// TestReadAPIReadGuardsAndTimeoutUnavailable pins the read transaction guards
// (read-api.md §4): they are the transaction's first two statements, reuse the
// shared 5s write bound, and any guard/lock failure maps to `unavailable` with
// the transaction released.
func TestReadAPIReadGuardsAndTimeoutUnavailable(t *testing.T) {
	_, tx := readBaseHandler()
	resp, err := readProvider(tx, "secret-token").ReadByBindingID(context.Background(), readTestBinding)
	if err != nil || resp.Outcome != ReadBound {
		t.Fatalf("read = (%q, %v), want bound", resp.Outcome, err)
	}
	execs := tx.recordedExecs()
	if len(execs) != 2 || execs[0] != localWriteGuard || execs[1] != readLockGuard {
		t.Fatalf("read guards = %#v, want [%q %q] as the first statements", execs, localWriteGuard, readLockGuard)
	}

	// A guard failure aborts the read as unavailable and rolls the tx back.
	guardFail := &readFakeTx{fakeQuerier: &fakeQuerier{execErr: func(sql string, _ []any) error {
		if sql == readLockGuard {
			return errors.New("fake: lock guard failed")
		}
		return nil
	}}}
	respGuard, errGuard := readProvider(guardFail, "secret-token").ReadByBindingID(context.Background(), readTestBinding)
	if !IsOutcome(errGuard, ReadUnavailable) || respGuard.Outcome != ReadUnavailable {
		t.Fatalf("guard failure = (%q, %v), want unavailable", respGuard.Outcome, errGuard)
	}
	if !guardFail.rolledBack || guardFail.committed {
		t.Fatalf("guard failure tx = (committed=%v, rolledBack=%v), want rollback only",
			guardFail.committed, guardFail.rolledBack)
	}

	// A real lock timeout (55P03) on the scope FOR SHARE is the same outcome.
	lockTimeout := &pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}
	timeoutTx := &readFakeTx{fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
		switch {
		case strings.Contains(sql, "WHERE binding_id = $1"):
			return rows(readBindingValues(StateAllocated)...)
		case strings.Contains(sql, "FOR SHARE"):
			return fakeRow{err: lockTimeout}
		default:
			return noRows()
		}
	}}}
	respTimeout, errTimeout := readProvider(timeoutTx, "secret-token").ReadByBindingID(context.Background(), readTestBinding)
	if !IsOutcome(errTimeout, ReadUnavailable) || respTimeout.Outcome != ReadUnavailable {
		t.Fatalf("lock timeout = (%q, %v), want unavailable", respTimeout.Outcome, errTimeout)
	}
	if !timeoutTx.rolledBack || timeoutTx.committed {
		t.Fatalf("timeout tx = (committed=%v, rolledBack=%v), want rollback only",
			timeoutTx.committed, timeoutTx.rolledBack)
	}
}

// TestReadAPICommitFailureBoundary pins the read-api.md §4 abort-then-serve
// boundary: a failed commit is served only for a fully assembled fact response
// already degraded to recovery `unknown`; every other commit failure collapses
// to `unavailable`. This is the boundary that keeps a masked commit failure from
// serving a rolled-back snapshot.
func TestReadAPICommitFailureBoundary(t *testing.T) {
	degraded := &readFakeTx{
		fakeQuerier: &fakeQuerier{handler: func(sql string, _ []any) pgx.Row {
			switch {
			case strings.Contains(sql, "WHERE binding_id = $1"):
				return rows(readBindingValues(StateAllocated)...)
			case strings.Contains(sql, "FOR SHARE"):
				return rows(readTestChain)
			case strings.Contains(sql, "nonce_wallet_registry"):
				return rows(allocRegistryValues(RegistryActive, 3)...)
			case strings.Contains(sql, "reorg_recovery"):
				return fakeRow{err: errors.New("fake: 006 probe aborted the tx")}
			default:
				return noRows()
			}
		}},
		commitErr: errors.New("fake: current transaction is aborted, commands ignored"),
	}
	resp, err := readProvider(degraded, "secret-token").ReadByBindingID(context.Background(), readTestBinding)
	if err != nil || resp.Outcome != ReadBound {
		t.Fatalf("degraded commit failure = (%q, %v), want the degraded bound response served", resp.Outcome, err)
	}
	if resp.Annotations == nil || resp.Annotations.Recovery.State != RecoveryUnknown {
		t.Fatalf("degraded response recovery = %+v, want unknown", resp.Annotations)
	}
	if !degraded.rolledBack {
		t.Fatal("degraded commit failure must still release the tx")
	}

	_, clean := readBaseHandler()
	clean.commitErr = errors.New("fake: commit failed")
	respClean, errClean := readProvider(clean, "secret-token").ReadByBindingID(context.Background(), readTestBinding)
	if !IsOutcome(errClean, ReadUnavailable) || respClean.Outcome != ReadUnavailable {
		t.Fatalf("non-degraded commit failure = (%q, %v), want unavailable", respClean.Outcome, errClean)
	}
	if !clean.rolledBack || clean.committed == false {
		t.Fatalf("clean commit failure tx = (committed=%v, rolledBack=%v), want an attempted commit then rollback",
			clean.committed, clean.rolledBack)
	}
}

// TestDegradedBy006FailurePredicate pins the narrow predicate that gates the
// abort-then-serve path: only a bound/terminal body whose recovery was already
// degraded to `unknown` qualifies; a 006 success (none/released) or any error
// outcome never does.
func TestDegradedBy006FailurePredicate(t *testing.T) {
	bound := func(recovery string) ReadResponse {
		return ReadResponse{Outcome: ReadBound, Annotations: &ReadAnnotations{Recovery: ReadRecovery{State: recovery}}}
	}
	for _, tc := range []struct {
		name string
		resp ReadResponse
		want bool
	}{
		{"bound_unknown", bound(RecoveryUnknown), true},
		{"bound_none", bound(RecoveryNone), false},
		{"bound_released", bound(RecoveryReleased), false},
		{"terminal_unknown", ReadResponse{Outcome: ReadTerminal, Annotations: &ReadAnnotations{Recovery: ReadRecovery{State: RecoveryUnknown}}}, true},
		{"not_bound", ReadResponse{Outcome: ReadNotBound}, false},
		{"mismatch", ReadResponse{Outcome: ReadMismatch}, false},
		{"unavailable", unavailableResponse(), false},
		{"nil_annotations", ReadResponse{Outcome: ReadBound}, false},
	} {
		if got := degradedBy006Failure(tc.resp); got != tc.want {
			t.Fatalf("%s: degradedBy006Failure = %v, want %v", tc.name, got, tc.want)
		}
	}
}
