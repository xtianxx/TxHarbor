// intake_capacity_test.go is the T070 unit layer for the PD-2 capacity gate:
// the refusal mapping (soft/hard backlog and an unreadable capacity state all
// refuse a first create with the retryable 503 channel; a normal backlog
// proceeds) and the structural constraints that make the gate safe:
//
//   - the capacity decision never touches Redis;
//   - the gate is consulted exactly once, after the replay fast path and
//     before the receipt transaction, so replays/accepted requests are never
//     refused and no existing gate is bypassed.
package withdrawal

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/events"
)

// fakeCapacityGate is a scripted CapacityAdmitter (no database).
type fakeCapacityGate struct {
	admission events.CapacityAdmission
	err       error
	calls     int
	lastOp    string
}

func (f *fakeCapacityGate) AdmitNewControllable(_ context.Context, opClass string) (events.CapacityAdmission, error) {
	f.calls++
	f.lastOp = opClass
	return f.admission, f.err
}

func TestCapacityAdmissionRefusalMapping(t *testing.T) {
	cases := []struct {
		name        string
		admission   events.CapacityAdmission
		err         error
		wantRefusal bool
		wantMessage string
	}{
		{
			name:        "normal_admits",
			admission:   events.CapacityAdmission{Level: events.CapacityNormal, PendingTotal: 1},
			wantRefusal: false,
		},
		{
			name:        "soft_refuses",
			admission:   events.CapacityAdmission{Level: events.CapacitySoft, PendingTotal: 10, Refused: true},
			wantRefusal: true,
			wantMessage: intakeCapacityRefusedMessage,
		},
		{
			name:        "hard_refuses",
			admission:   events.CapacityAdmission{Level: events.CapacityHard, PendingTotal: 20, Refused: true},
			wantRefusal: true,
			wantMessage: intakeCapacityRefusedMessage,
		},
		{
			name:        "unreadable_state_fails_closed",
			err:         errors.New("pg down"),
			wantRefusal: true,
			wantMessage: intakeCapacityUnavailableMessage,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := &fakeCapacityGate{admission: tc.admission, err: tc.err}
			res := capacityAdmissionRefusal(context.Background(), 7, gate)
			if tc.wantRefusal && res == nil {
				t.Fatal("new controllable write was admitted; want a refusal")
			}
			if !tc.wantRefusal && res != nil {
				t.Fatalf("res = %+v, want admission", res)
			}
			if res == nil {
				return
			}
			if res.Status != 503 || res.Code != CodeTemporarilyUnavailable {
				t.Fatalf("refusal = %d/%s, want 503/%s", res.Status, res.Code, CodeTemporarilyUnavailable)
			}
			if res.Message != tc.wantMessage {
				t.Fatalf("message = %q, want %q", res.Message, tc.wantMessage)
			}
			if !strings.Contains(res.Message, "retry with the same idempotency key") {
				t.Fatalf("message %q is not a same-key retry instruction", res.Message)
			}
			if res.Audit == nil || res.Audit.Action != auditActionUnavailable || res.Audit.CallerID != 7 {
				t.Fatalf("audit = %+v, want an unavailable intent for caller 7", res.Audit)
			}
			if res.RequestID != "" {
				t.Fatalf("refusal carries request id %q; a refused first create never fabricates one", res.RequestID)
			}
		})
	}
}

func TestCapacityAdmissionNilGateAdmits(t *testing.T) {
	if res := capacityAdmissionRefusal(context.Background(), 7, nil); res != nil {
		t.Fatalf("nil gate must leave the PG-only baseline unchanged, got %+v", res)
	}
}

func TestCapacityAdmissionUsesWithdrawalCreateOpClass(t *testing.T) {
	gate := &fakeCapacityGate{}
	_ = capacityAdmissionRefusal(context.Background(), 7, gate)
	if gate.calls != 1 {
		t.Fatalf("gate calls = %d, want exactly 1", gate.calls)
	}
	if gate.lastOp != events.CapacityOpWithdrawalCreate {
		t.Fatalf("op class = %q, want %q", gate.lastOp, events.CapacityOpWithdrawalCreate)
	}
}

// TestIntakeCapacityGateStructure pins the gate placement and the Redis-free
// constraint on the intake source: the capacity decision is consulted once,
// only on the first-create path (after the step-4 replay fast path and the
// allowlist gate, before the receipt transaction), and no Redis client is
// involved.
func TestIntakeCapacityGateStructure(t *testing.T) {
	body, err := os.ReadFile("intake.go")
	if err != nil {
		t.Fatalf("ReadFile intake.go: %v", err)
	}
	src := string(body)
	if !strings.Contains(src, "capacityAdmissionRefusal(ctx, callerID, req.CapacityGate)") {
		t.Fatal("intake.go does not consult the capacity gate before the receipt transaction")
	}
	if n := strings.Count(src, "capacityAdmissionRefusal(ctx, callerID, req.CapacityGate)"); n != 1 {
		t.Fatalf("capacity gate consulted %d times, want exactly 1 (first-create path only)", n)
	}
	gateCall := strings.Index(src, "capacityAdmissionRefusal(ctx, callerID, req.CapacityGate)")
	submitCall := strings.Index(src, "return submitInTx(ctx, pool, callerID, req, norm)")
	if gateCall < 0 || submitCall < 0 || gateCall > submitCall {
		t.Fatal("capacity gate is not positioned before the receipt transaction")
	}
	// The replay fast path must return before the gate: the found=true block
	// precedes it in source order.
	replayReturn := strings.Index(src, "withdrawal request already accepted")
	if replayReturn < 0 || replayReturn > gateCall {
		t.Fatal("capacity gate is not positioned after the replay fast path")
	}
	// The intake must not depend on Redis: no Redis client may be imported
	// here (the decision reads PostgreSQL only). The source comment may name
	// Redis, so the check is on the import set, not on prose.
	parsed, perr := parser.ParseFile(token.NewFileSet(), "intake.go", body, parser.ImportsOnly)
	if perr != nil {
		t.Fatalf("parse intake.go: %v", perr)
	}
	for _, spec := range parsed.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		if strings.Contains(path, "redis") {
			t.Fatalf("intake.go imports %q; the capacity decision must not depend on Redis", path)
		}
	}
}
