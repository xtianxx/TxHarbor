// observe_test.go is the T011 EARLY-VALIDATION suite: a scripted fake RPC
// (never a real endpoint — real RPC is covered by T027/T030) plus a fake
// handle with no transaction support. No acceptance claim rests on these
// doubles (quickstart §Environment; constitution XI).
package nonce

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/xtianxx/txharbor/internal/eth"
)

const obsTestSender = "0x1111111111111111111111111111111111111111"

var obsTestHash = "0x" + strings.Repeat("ab", 32)

func obsStr(s string) *string { return &s }

func obsTestHead() *rpcHead {
	return &rpcHead{Number: obsStr("0x2a"), Hash: obsTestHash}
}

// obsTestObserver uses tiny retry delays so retry paths stay fast.
func obsTestObserver(rpc rpcCaller) *Observer {
	return NewObserver(rpc, ObserverConfig{
		RPCTimeout:   50 * time.Millisecond,
		RetryInitial: time.Millisecond,
		RetryMax:     2 * time.Millisecond,
	})
}

// obsRPCCall records one fake JSON-RPC call (method + exact args).
type obsRPCCall struct {
	Method string
	Args   []any
}

// obsFakeRPC is the scripted chain double: it answers the two observation
// quantity reads and the head read, and can inject per-method failures.
type obsFakeRPC struct {
	mu          sync.Mutex
	calls       []obsRPCCall
	latest      string
	pending     string
	block       *rpcHead
	failures    map[string]error
	failNext    int
	sawDeadline bool
}

func (f *obsFakeRPC) CallContext(ctx context.Context, result any, method string, args ...any) error {
	f.mu.Lock()
	f.calls = append(f.calls, obsRPCCall{Method: method, Args: append([]any(nil), args...)})
	if _, ok := ctx.Deadline(); ok {
		f.sawDeadline = true
	}
	if f.failNext > 0 {
		f.failNext--
		f.mu.Unlock()
		return errors.New("fake: injected transport failure")
	}
	injected := f.failures[method]
	f.mu.Unlock()
	if injected != nil {
		return injected
	}
	switch r := result.(type) {
	case *string:
		block, _ := args[1].(string)
		switch block {
		case "latest":
			*r = f.latest
		case "pending":
			*r = f.pending
		default:
			return fmt.Errorf("fake: unscripted block %q", block)
		}
		return nil
	case **rpcHead:
		*r = f.block
		return nil
	default:
		return fmt.Errorf("fake: unscripted result type %T", result)
	}
}

func (f *obsFakeRPC) recordedCalls() []obsRPCCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]obsRPCCall(nil), f.calls...)
}

// obsRPCError is a plain JSON-RPC server error (gethrpc.Error).
type obsRPCError struct{ code int }

func (e obsRPCError) Error() string  { return fmt.Sprintf("fake rpc error %d", e.code) }
func (e obsRPCError) ErrorCode() int { return e.code }

// obsFakeExec satisfies txQuerier WITHOUT any transaction surface: it has no
// Begin/Commit/Rollback, so the test proves the writer only needs an exec
// handle.
type obsFakeExec struct {
	execs []string
	args  [][]any
}

var _ txQuerier = (*obsFakeExec)(nil)

func (f *obsFakeExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.execs = append(f.execs, sql)
	f.args = append(f.args, append([]any(nil), args...))
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (f *obsFakeExec) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, errors.New("obsFakeExec: Query not scripted")
}

func (f *obsFakeExec) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return fakeRow{err: pgx.ErrNoRows}
}

func (f *obsFakeExec) assertNoTxSupport(t *testing.T) {
	t.Helper()
	for _, m := range []string{"Begin", "BeginTx", "Commit", "Rollback"} {
		if _, ok := reflect.TypeOf(f).MethodByName(m); ok {
			t.Fatalf("fake unexpectedly exposes tx method %s", m)
		}
	}
	for _, sql := range f.execs {
		if strings.Contains(strings.ToUpper(sql), "BEGIN") {
			t.Fatalf("writer emitted a transaction statement: %q", sql)
		}
	}
}

func TestObserveSamplesLatestPendingHeadAndPersists(t *testing.T) {
	rpc := &obsFakeRPC{latest: "0x10", pending: "0x11", block: obsTestHead()}
	ctx := context.Background()

	obs := obsTestObserver(rpc).Observe(ctx, 7, obsTestSender, ObservationKindAllocation)

	if obs.Classification != "" || obs.ErrorClass != "" {
		t.Fatalf("successful observation classified %q/%q, want unclassified", obs.Classification, obs.ErrorClass)
	}
	if obs.LatestCount == nil || obs.LatestCount.Cmp(big.NewInt(16)) != 0 {
		t.Fatalf("latest = %v, want 16", obs.LatestCount)
	}
	if obs.PendingCount == nil || obs.PendingCount.Cmp(big.NewInt(17)) != 0 {
		t.Fatalf("pending = %v, want 17", obs.PendingCount)
	}
	if obs.HeadNumber == nil || obs.HeadNumber.Cmp(big.NewInt(42)) != 0 || obs.HeadHash != obsTestHash {
		t.Fatalf("head = %v/%q, want 42/%s", obs.HeadNumber, obs.HeadHash, obsTestHash)
	}

	calls := rpc.recordedCalls()
	wantArgs := [][]any{
		{obsTestSender, "latest"},
		{obsTestSender, "pending"},
		{"latest", false},
	}
	wantMethods := []string{methodTransactionCount, methodTransactionCount, methodBlockByNumber}
	if len(calls) != len(wantMethods) {
		t.Fatalf("rpc calls = %d, want %d", len(calls), len(wantMethods))
	}
	for i := range calls {
		if calls[i].Method != wantMethods[i] || !reflect.DeepEqual(calls[i].Args, wantArgs[i]) {
			t.Fatalf("call %d = %s%v, want %s%v", i, calls[i].Method, calls[i].Args, wantMethods[i], wantArgs[i])
		}
	}
	if !rpc.sawDeadline {
		t.Fatal("RPC call ran without the RPCTimeout deadline")
	}

	// T012 simulates the under-lock classification step.
	obs.Classification = ClassificationConsistent
	exec := &obsFakeExec{}
	id, err := insertObservationTx(ctx, exec, obs)
	if err != nil {
		t.Fatalf("insertObservationTx: %v", err)
	}
	if !strings.HasPrefix(id, "no-") || len(id) != 35 {
		t.Fatalf("minted observation id = %q, want no- + 32 hex", id)
	}
	if len(exec.execs) != 1 || !strings.Contains(exec.execs[0], "INSERT INTO nonce_observations") {
		t.Fatalf("execs = %v, want one nonce_observations insert", exec.execs)
	}
	exec.assertNoTxSupport(t)

	args := exec.args[0]
	if args[0] != id || args[1] != int64(7) || args[2] != obsTestSender ||
		args[3] != ObservationKindAllocation || args[4] != ClassificationConsistent {
		t.Fatalf("insert args = %v, want id/chain/sender/kind/classification", args[:5])
	}
	if num, ok := args[5].(pgtype.Numeric); !ok || !num.Valid || num.Int.Cmp(big.NewInt(16)) != 0 {
		t.Fatalf("latest_count arg = %#v, want NUMERIC 16", args[5])
	}
	if args[8] != obsTestHash || args[9] != "" || args[10] != "" {
		t.Fatalf("insert args = %v, want head hash and empty error/rpc refs", args[8:])
	}
}

func TestObserveFailureClassesPersistUnavailable(t *testing.T) {
	cases := []struct {
		name      string
		failOn    string
		inject    error
		wantCalls int
		wantClass string
	}{
		{"transport", methodTransactionCount, errors.New("connection refused"), 3, "transport"},
		{"timeout", methodTransactionCount, context.DeadlineExceeded, 3, "timeout"},
		{"rate-limited", methodTransactionCount, gethrpc.HTTPError{StatusCode: http.StatusTooManyRequests}, 3, "rate_limited"},
		{"invalid-response", methodTransactionCount, obsRPCError{code: -32000}, 1, "invalid_response"},
		{"chain-mismatch", methodTransactionCount, &eth.Error{Kind: eth.KindChainMismatch, Op: "test", Err: errors.New("wrong chain")}, 1, "chain_mismatch"},
		{"not-found", methodTransactionCount, ethereum.NotFound, 1, "rpc_unavailable"},
		{"invalid-response-head", methodBlockByNumber, obsRPCError{code: -32000}, 3, "invalid_response"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &obsFakeRPC{
				latest:   "0x5",
				pending:  "0x5",
				block:    obsTestHead(),
				failures: map[string]error{tc.failOn: tc.inject},
			}
			obs := obsTestObserver(rpc).Observe(context.Background(), 7, obsTestSender, ObservationKindAllocation)

			if obs.Classification != ClassificationUnavailable {
				t.Fatalf("classification = %q, want %q", obs.Classification, ClassificationUnavailable)
			}
			if obs.ErrorClass != tc.wantClass {
				t.Fatalf("error class = %q, want %q", obs.ErrorClass, tc.wantClass)
			}
			if obs.LatestCount != nil || obs.PendingCount != nil || obs.HeadNumber != nil || obs.HeadHash != "" {
				t.Fatalf("failed observation carries facts: %+v", obs)
			}
			if got := len(rpc.recordedCalls()); got != tc.wantCalls {
				t.Fatalf("rpc calls = %d, want %d", got, tc.wantCalls)
			}

			exec := &obsFakeExec{}
			if _, err := insertObservationTx(context.Background(), exec, obs); err != nil {
				t.Fatalf("insertObservationTx: %v", err)
			}
			exec.assertNoTxSupport(t)
			if len(exec.args) != 1 || exec.args[0][4] != ClassificationUnavailable || exec.args[0][9] != tc.wantClass {
				t.Fatalf("persisted row = %v, want unavailable + %s", exec.args, tc.wantClass)
			}
		})
	}
}

func TestObserveRetriesTransientFailureThenSucceeds(t *testing.T) {
	rpc := &obsFakeRPC{latest: "0x9", pending: "0x9", block: obsTestHead(), failNext: 1}
	obs := obsTestObserver(rpc).Observe(context.Background(), 7, obsTestSender, ObservationKindReconcile)

	if obs.Classification != "" || obs.ErrorClass != "" || obs.LatestCount == nil {
		t.Fatalf("retry did not recover: %+v", obs)
	}
	if got := len(rpc.recordedCalls()); got != 4 {
		t.Fatalf("rpc calls = %d, want 4 (1 retried + 3 reads)", got)
	}
}

func TestObserveMalformedHeadIsInvalidResponse(t *testing.T) {
	rpc := &obsFakeRPC{
		latest:  "0x1",
		pending: "0x1",
		block:   &rpcHead{Number: obsStr("0x1"), Hash: "0xnothex"},
	}
	obs := obsTestObserver(rpc).Observe(context.Background(), 7, obsTestSender, ObservationKindAllocation)

	if obs.Classification != ClassificationUnavailable || obs.ErrorClass != "invalid_response" {
		t.Fatalf("malformed head = %q/%q, want unavailable/invalid_response", obs.Classification, obs.ErrorClass)
	}
}

func TestObserveNullHeadIsRPCEvidence(t *testing.T) {
	rpc := &obsFakeRPC{latest: "0x1", pending: "0x1", block: nil}
	obs := obsTestObserver(rpc).Observe(context.Background(), 7, obsTestSender, ObservationKindAllocation)

	if obs.Classification != ClassificationUnavailable || obs.ErrorClass != "rpc_unavailable" {
		t.Fatalf("null head = %q/%q, want unavailable/rpc_unavailable", obs.Classification, obs.ErrorClass)
	}
}

func TestObserveInsertObservationRefusesUnpersistableRows(t *testing.T) {
	valid := Observation{
		ChainID:        7,
		Sender:         obsTestSender,
		Kind:           ObservationKindAllocation,
		Classification: ClassificationUnavailable,
		ErrorClass:     "transport",
	}
	cases := []struct {
		name string
		mut  func(*Observation)
	}{
		{"unclassified", func(o *Observation) { o.Classification = "" }},
		{"uppercase-sender", func(o *Observation) { o.Sender = "0x111111111111111111111111111111111111111A" }},
		{"unknown-kind", func(o *Observation) { o.Kind = "mystery" }},
		{"out-of-range-count", func(o *Observation) { o.LatestCount = new(big.Int).Lsh(big.NewInt(1), 64) }},
		{"bad-hash", func(o *Observation) { o.HeadHash = "0xabc" }},
		{"zero-chain", func(o *Observation) { o.ChainID = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs := valid
			tc.mut(&obs)
			exec := &obsFakeExec{}
			if _, err := insertObservationTx(context.Background(), exec, obs); err == nil {
				t.Fatal("unpersistable row accepted")
			}
			if len(exec.execs) != 0 {
				t.Fatalf("refused row still wrote: %v", exec.execs)
			}
		})
	}
}
