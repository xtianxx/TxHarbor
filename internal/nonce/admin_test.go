// admin_test.go is the T014 EARLY-VALIDATION suite for the operator
// transaction library: scripted querier doubles only (no acceptance claim
// rests on them; real-PostgreSQL integration belongs to T018/T019/T024/T025).
// It pins the refusal paths (evidence/version drift), the nop paths, the
// applied writes, and the operation-id dedup (equal op-input reports the
// recorded outcome; a differing op-input is operation_conflict with zero
// writes).
package nonce

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	adminChain  = int64(31337)
	adminSender = "0x5555555555555555555555555555555555555555"
)

// adminFakeTx is a txQuerier with a transaction surface and recorded Exec
// arguments (the shared fake's Query is extended to the multi-row readers).
type adminFakeTx struct {
	*fakeQuerier
	f                     *adminFixture
	execArgs              [][]any
	committed, rolledBack bool
}

func (t *adminFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.execArgs = append(t.execArgs, append([]any(nil), args...))
	return t.fakeQuerier.Exec(ctx, sql, args...)
}

func (t *adminFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.mu.Lock()
	t.queries = append(t.queries, sql)
	t.mu.Unlock()
	if t.f.rowsHandler == nil {
		return allocRowSet(), nil
	}
	return t.f.rowsHandler(sql, args)
}

func (t *adminFakeTx) Commit(context.Context) error   { t.committed = true; return nil }
func (t *adminFakeTx) Rollback(context.Context) error { t.rolledBack = true; return nil }

// adminFixture scripts one runner attempt over scripted durable facts.
type adminFixture struct {
	t   *testing.T
	rpc *obsFakeRPC
	txs []*adminFakeTx

	rowHandler  func(sql string, args []any) pgx.Row
	rowsHandler func(sql string, args []any) (pgx.Rows, error)
	// auditErr is injected on the nonce_ops_audit Exec to script the
	// operation-id 23505 race.
	auditErr error
}

func newAdminFixture(t *testing.T) *adminFixture {
	t.Helper()
	return &adminFixture{
		t:   t,
		rpc: &obsFakeRPC{latest: "0x5", pending: "0x5", block: obsTestHead()},
	}
}

func (f *adminFixture) begin(context.Context) (adminTx, error) {
	tx := &adminFakeTx{
		f: f,
		fakeQuerier: &fakeQuerier{
			handler: f.rowHandler,
			execErr: func(sql string, _ []any) error {
				if f.auditErr != nil && strings.Contains(sql, "nonce_ops_audit") {
					return f.auditErr
				}
				return nil
			},
		},
	}
	f.txs = append(f.txs, tx)
	return tx, nil
}

func (f *adminFixture) runner() *AdminRunner {
	return &AdminRunner{observer: obsTestObserver(f.rpc), begin: f.begin}
}

func (f *adminFixture) tx(i int) *adminFakeTx { return f.txs[i] }

func (f *adminFixture) execArgsFor(tx *adminFakeTx, substr string) []any {
	for i, sql := range tx.recordedExecs() {
		if strings.Contains(sql, substr) && i < len(tx.execArgs) {
			return tx.execArgs[i]
		}
	}
	return nil
}

func (f *adminFixture) execCount(tx *adminFakeTx, substr string) int {
	n := 0
	for _, sql := range tx.recordedExecs() {
		if strings.Contains(sql, substr) {
			n++
		}
	}
	return n
}

func adminReq(action string) AdminRequest {
	return AdminRequest{
		Action:        action,
		OperationID:   "op-admin-1",
		ChainID:       adminChain,
		Sender:        adminSender,
		HoldID:        "nh-target",
		BindingID:     "nb-target",
		ObservationID: "no-ref",
		Evidence:      "manual reconciliation finding",
		Operator:      "operator-A",
		Reason:        "reconcile complete",
	}
}

func adminHold(id, cause string) []any {
	return []any{
		id, adminChain, adminSender, cause, HoldStatusActive, time.Now().UTC(),
		"no-prev", "seed",
		nil, nil, nil, nil, nil,
	}
}

func adminReleasedHold(id, cause string) []any {
	now := time.Now().UTC()
	by, op, ev, ref := "operator-old", "op-old", "old evidence", "no-old"
	return []any{
		id, adminChain, adminSender, cause, HoldStatusReleased, now.Add(-time.Hour),
		"no-prev", "seed",
		&now, &by, &op, &ev, &ref,
	}
}

func adminBindingRow(nonce int64, state string) []any {
	now := time.Now().UTC()
	b := Binding{
		BindingID:               "nb-target",
		IntentID:                "intent-admin",
		ChainID:                 adminChain,
		Sender:                  adminSender,
		Nonce:                   big.NewInt(nonce),
		State:                   state,
		AuthorizationID:         "wa-admin",
		AuthorizationVersion:    strings.Repeat("c", 64),
		RegistrySeq:             1,
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

// adminHoldReleaseHandler answers the T-hold-release readers. nil hold /
// obsScope / registry script the miss that drives nop/refused paths.
func adminHoldReleaseHandler(hold, obsScope, registry []any) func(sql string, args []any) pgx.Row {
	return func(sql string, _ []any) pgx.Row {
		switch {
		case strings.Contains(sql, "indexer_lease"):
			return rows("nonce-008", int64(0), true)
		case strings.Contains(sql, "nonce_scope_state") && strings.Contains(sql, "FOR UPDATE"):
			return rows(adminChain)
		case strings.Contains(sql, "hold_id = $1 FOR UPDATE"):
			if hold == nil {
				return noRows()
			}
			return rows(hold...)
		case strings.Contains(sql, "nonce_observations"):
			if obsScope == nil {
				return noRows()
			}
			return rows(obsScope...)
		case strings.Contains(sql, "nonce_wallet_registry"):
			if registry == nil {
				return noRows()
			}
			return rows(registry...)
		case strings.Contains(sql, "reconciled_floor"):
			return rows(obsStr("4"), obsStr("5"), obsStr("5"), obsStr("no-prev"), time.Now().UTC())
		default:
			return noRows()
		}
	}
}

// adminScopeRows answers the scope binding/hold list readers.
func adminScopeRows(binding []any) func(sql string, args []any) (pgx.Rows, error) {
	return func(sql string, _ []any) (pgx.Rows, error) {
		switch {
		case strings.Contains(sql, "nonce_scope_holds"):
			return allocRowSet(adminHold("nh-target", CauseUnexplainedGap)), nil
		case strings.Contains(sql, "FROM nonce_bindings"):
			if binding == nil {
				return allocRowSet(), nil
			}
			return allocRowSet(binding), nil
		default:
			return allocRowSet(), nil
		}
	}
}

func adminBindingReleaseHandler(binding, obsScope []any) func(sql string, args []any) pgx.Row {
	return func(sql string, _ []any) pgx.Row {
		switch {
		case strings.Contains(sql, "indexer_lease"):
			return rows("nonce-008", int64(0), true)
		case strings.Contains(sql, "nonce_scope_state") && strings.Contains(sql, "FOR UPDATE"):
			return rows(adminChain)
		case strings.Contains(sql, "binding_id = $1 FOR UPDATE"):
			if binding == nil {
				return noRows()
			}
			return rows(binding...)
		case strings.Contains(sql, "nonce_observations"):
			if obsScope == nil {
				return noRows()
			}
			return rows(obsScope...)
		default:
			return noRows()
		}
	}
}

func TestAdminRequestValidate(t *testing.T) {
	if err := adminReq(AdminActionHoldRelease).Validate(); err != nil {
		t.Fatalf("valid hold-release refused: %v", err)
	}
	if err := adminReq(AdminActionRegistryRegister).Validate(); err != nil {
		t.Fatalf("valid register refused: %v", err)
	}
	bad := adminReq(AdminActionHoldRelease)
	bad.OperationID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("missing operation_id accepted")
	}
	bad = adminReq(AdminActionHoldRelease)
	bad.HoldID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("hold-release without hold_id accepted")
	}
	bad = adminReq(AdminActionBindingRelease)
	bad.BindingID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("binding-release without binding_id accepted")
	}
	bad = adminReq("nonsense")
	if err := bad.Validate(); err == nil {
		t.Fatal("unknown action accepted")
	}
	bad = adminReq(AdminActionRegistryDisable)
	bad.Sender = "0xUPPER"
	if err := bad.Validate(); err == nil {
		t.Fatal("malformed sender accepted")
	}
}

// TestAdminHoldReleaseApplied pins the T-hold-release applied path: the named
// hold is cleared, the floor advances to the observed pending, the fresh
// observation + audit row commit in one tx, and nothing else changes.
func TestAdminHoldReleaseApplied(t *testing.T) {
	f := newAdminFixture(t)
	f.rowHandler = adminHoldReleaseHandler(
		adminHold("nh-target", CauseUnexplainedGap),
		[]any{adminChain, adminSender},
		allocRegistryValues(RegistryActive, 7),
	)
	f.rowsHandler = adminScopeRows(adminBindingRow(4, StateAllocated))

	req := adminReq(AdminActionHoldRelease)
	res, err := f.runner().Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != AdminApplied {
		t.Fatalf("outcome = %q, want applied (detail %q)", res.Outcome, res.Detail)
	}
	if !strings.Contains(res.Detail, "registry_seq=7") || !strings.Contains(res.Detail, "006=clear") {
		t.Fatalf("version detail = %q, want registry + 006 evidence", res.Detail)
	}
	tx := f.tx(0)
	if !tx.committed || tx.rolledBack {
		t.Fatalf("tx = (committed=%v, rolledBack=%v), want commit only", tx.committed, tx.rolledBack)
	}

	holdArgs := f.execArgsFor(tx, "UPDATE nonce_scope_holds")
	if holdArgs == nil || holdArgs[2] != req.OperationID || holdArgs[1] != req.Operator {
		t.Fatalf("hold release args = %v, want operation id + operator", holdArgs)
	}
	freshID, _ := holdArgs[4].(string)
	if !strings.HasPrefix(freshID, "no-") {
		t.Fatalf("release_observation_id = %v, want the fresh persisted observation", holdArgs[4])
	}
	if !strings.Contains(holdArgs[3].(string), req.Evidence) {
		t.Fatalf("release evidence = %v, want the operator finding", holdArgs[3])
	}

	floorArgs := f.execArgsFor(tx, "reconciled_floor = GREATEST")
	if floorArgs == nil {
		t.Fatal("floor advance missing on the applied path")
	}
	floor, err := BigFromNumeric(floorArgs[2].(pgtype.Numeric))
	if err != nil || floor == nil || floor.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("advanced floor = %v (%v), want the observed pending 5", floorArgs[2], err)
	}

	if f.execCount(tx, "INSERT INTO nonce_observations") != 1 {
		t.Fatalf("fresh observation rows = %d, want 1", f.execCount(tx, "INSERT INTO nonce_observations"))
	}
	auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit")
	if auditArgs == nil || auditArgs[0] != req.OperationID || auditArgs[5] != string(AdminApplied) {
		t.Fatalf("audit args = %v, want the applied row", auditArgs)
	}
}

// TestAdminHoldReleaseRefusedOnFreshDrift pins the re-verify refusal: a fresh
// observable still diverging (L > P) refuses with a refused audit and zero
// hold/floor change.
func TestAdminHoldReleaseRefusedOnFreshDrift(t *testing.T) {
	f := newAdminFixture(t)
	f.rpc = &obsFakeRPC{latest: "0x6", pending: "0x5", block: obsTestHead()}
	f.rowHandler = adminHoldReleaseHandler(
		adminHold("nh-target", CauseUnexplainedGap),
		[]any{adminChain, adminSender},
		allocRegistryValues(RegistryActive, 7),
	)
	f.rowsHandler = adminScopeRows(adminBindingRow(4, StateAllocated))

	res, err := f.runner().Run(context.Background(), adminReq(AdminActionHoldRelease))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != AdminRefused {
		t.Fatalf("outcome = %q, want refused", res.Outcome)
	}
	tx := f.tx(0)
	if !tx.committed {
		t.Fatal("a refused attempt must commit its audit row")
	}
	if n := f.execCount(tx, "UPDATE nonce_scope_holds"); n != 0 {
		t.Fatalf("refused attempt wrote the hold %d times", n)
	}
	if n := f.execCount(tx, "reconciled_floor = GREATEST"); n != 0 {
		t.Fatalf("refused attempt advanced the floor %d times", n)
	}
	if n := f.execCount(tx, "INSERT INTO nonce_observations"); n != 0 {
		t.Fatalf("refused attempt persisted %d observations, want zero change", n)
	}
	auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit")
	if auditArgs == nil || auditArgs[5] != string(AdminRefused) {
		t.Fatalf("audit args = %v, want refused", auditArgs)
	}
}

// TestAdminHoldReleaseRefusedOnVersionDrift pins the evidence-version set: a
// missing registry row (a version input the operator's evidence cannot
// assert) refuses without touching the hold.
func TestAdminHoldReleaseRefusedOnVersionDrift(t *testing.T) {
	f := newAdminFixture(t)
	f.rowHandler = adminHoldReleaseHandler(
		adminHold("nh-target", CauseUnexplainedGap),
		[]any{adminChain, adminSender},
		nil, // registry row vanished: version drift vs the operator evidence
	)

	res, err := f.runner().Run(context.Background(), adminReq(AdminActionHoldRelease))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != AdminRefused {
		t.Fatalf("outcome = %q, want refused", res.Outcome)
	}
	tx := f.tx(0)
	if f.execCount(tx, "UPDATE nonce_scope_holds") != 0 {
		t.Fatal("version drift still wrote the hold")
	}
	if auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit"); auditArgs == nil || auditArgs[5] != string(AdminRefused) {
		t.Fatalf("audit args = %v, want refused", auditArgs)
	}
}

// TestAdminHoldReleaseRefusedOnForeignObservation pins the observation
// reference scope check: a reference outside the scope is refused.
func TestAdminHoldReleaseRefusedOnForeignObservation(t *testing.T) {
	f := newAdminFixture(t)
	f.rowHandler = adminHoldReleaseHandler(
		adminHold("nh-target", CauseUnexplainedGap),
		[]any{adminChain, "0x9999999999999999999999999999999999999999"},
		allocRegistryValues(RegistryActive, 7),
	)

	res, err := f.runner().Run(context.Background(), adminReq(AdminActionHoldRelease))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != AdminRefused {
		t.Fatalf("outcome = %q, want refused", res.Outcome)
	}
	if f.execCount(f.tx(0), "UPDATE nonce_scope_holds") != 0 {
		t.Fatal("foreign observation reference still released the hold")
	}
}

// TestAdminHoldReleaseNopWhenMissingOrReleased pins the nop outcome: a
// missing or already released hold audits `nop` with zero change (never
// upgraded, never an error).
func TestAdminHoldReleaseNopWhenMissingOrReleased(t *testing.T) {
	for name, hold := range map[string][]any{
		"missing":  nil,
		"released": adminReleasedHold("nh-target", CauseUnexplainedGap),
	} {
		t.Run(name, func(t *testing.T) {
			f := newAdminFixture(t)
			f.rowHandler = adminHoldReleaseHandler(hold, nil, nil)

			res, err := f.runner().Run(context.Background(), adminReq(AdminActionHoldRelease))
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if res.Outcome != AdminNop {
				t.Fatalf("outcome = %q, want nop", res.Outcome)
			}
			tx := f.tx(0)
			if !tx.committed {
				t.Fatal("a nop attempt must commit its audit row")
			}
			if f.execCount(tx, "UPDATE nonce_scope_holds") != 0 {
				t.Fatal("nop attempt wrote the hold")
			}
			if auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit"); auditArgs == nil || auditArgs[5] != string(AdminNop) {
				t.Fatalf("audit args = %v, want nop", auditArgs)
			}
		})
	}
}

// TestAdminBindingReleaseRefusedOnSideEffect pins the mandatory no-side-effect
// evidence: a pending observation (P > nonce) refuses with zero binding change.
func TestAdminBindingReleaseRefusedOnSideEffect(t *testing.T) {
	f := newAdminFixture(t)
	f.rpc = &obsFakeRPC{latest: "0x4", pending: "0x5", block: obsTestHead()}
	f.rowHandler = adminBindingReleaseHandler(adminBindingRow(4, StateAllocated), []any{adminChain, adminSender})

	res, err := f.runner().Run(context.Background(), adminReq(AdminActionBindingRelease))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != AdminRefused {
		t.Fatalf("outcome = %q, want refused", res.Outcome)
	}
	tx := f.tx(0)
	if f.execCount(tx, "UPDATE nonce_bindings") != 0 {
		t.Fatal("side-effect evidence still released the binding")
	}
	if auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit"); auditArgs == nil || auditArgs[5] != string(AdminRefused) {
		t.Fatalf("audit args = %v, want refused", auditArgs)
	}
}

// TestAdminBindingReleaseApplied pins the applied path: state released with
// the operation id, the append-only event, and the audit row.
func TestAdminBindingReleaseApplied(t *testing.T) {
	f := newAdminFixture(t)
	f.rpc = &obsFakeRPC{latest: "0x4", pending: "0x4", block: obsTestHead()}
	f.rowHandler = adminBindingReleaseHandler(adminBindingRow(4, StateAllocated), []any{adminChain, adminSender})

	req := adminReq(AdminActionBindingRelease)
	res, err := f.runner().Run(context.Background(), req)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Outcome != AdminApplied {
		t.Fatalf("outcome = %q, want applied", res.Outcome)
	}
	tx := f.tx(0)
	if !tx.committed || tx.rolledBack {
		t.Fatalf("tx = (committed=%v, rolledBack=%v), want commit only", tx.committed, tx.rolledBack)
	}
	bindArgs := f.execArgsFor(tx, "UPDATE nonce_bindings")
	if bindArgs == nil || bindArgs[1] != StateReleased || bindArgs[2] != req.OperationID || bindArgs[3] != StateAllocated {
		t.Fatalf("binding release args = %v, want allocated → released + operation id", bindArgs)
	}
	evArgs := f.execArgsFor(tx, "INSERT INTO nonce_binding_events")
	if evArgs == nil || evArgs[2] != StateReleased || evArgs[4] != req.OperationID {
		t.Fatalf("release event args = %v, want → released + operation id", evArgs)
	}
	if auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit"); auditArgs == nil || auditArgs[5] != string(AdminApplied) {
		t.Fatalf("audit args = %v, want applied", auditArgs)
	}
}

// TestAdminRegistryOperations pins T-registry: register materialises a first
// row, disable flips an active row, both with a bumped registry_seq and an
// audit row.
func TestAdminRegistryOperations(t *testing.T) {
	f := newAdminFixture(t)
	f.rowHandler = func(sql string, _ []any) pgx.Row {
		if strings.Contains(sql, "indexer_lease") {
			return rows("nonce-008", int64(0), true)
		}
		return noRows()
	}

	reg, err := f.runner().Run(context.Background(), adminReq(AdminActionRegistryRegister))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if reg.Outcome != AdminApplied {
		t.Fatalf("register outcome = %q, want applied", reg.Outcome)
	}
	tx := f.tx(0)
	regArgs := f.execArgsFor(tx, "INSERT INTO nonce_wallet_registry")
	if regArgs == nil || regArgs[0] != adminChain || regArgs[1] != adminSender {
		t.Fatalf("register args = %v, want chain + sender", regArgs)
	}
	if auditArgs := f.execArgsFor(tx, "INSERT INTO nonce_ops_audit"); auditArgs == nil ||
		auditArgs[1] != AdminActionRegistryRegister || auditArgs[5] != string(AdminApplied) {
		t.Fatalf("register audit args = %v, want registry_register/applied", auditArgs)
	}

	dis, err := f.runner().Run(context.Background(), adminReq(AdminActionRegistryDisable))
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if dis.Outcome != AdminApplied {
		t.Fatalf("disable outcome = %q, want applied", dis.Outcome)
	}
	tx2 := f.tx(1)
	if f.execArgsFor(tx2, "state = 'disabled'") == nil {
		t.Fatalf("disable statement missing:\n%s", strings.Join(tx2.recordedExecs(), "\n"))
	}
	if !strings.Contains(strings.Join(tx2.recordedExecs(), "\n"), "registry_seq = registry_seq + 1") {
		t.Fatal("disable must bump registry_seq")
	}
	if auditArgs := f.execArgsFor(tx2, "INSERT INTO nonce_ops_audit"); auditArgs == nil ||
		auditArgs[1] != AdminActionRegistryDisable || auditArgs[5] != string(AdminApplied) {
		t.Fatalf("disable audit args = %v, want registry_disable/applied", auditArgs)
	}
}

// TestAdminOperationReplayReturnsRecordedOutcome pins op-input equality on the
// operation-id race: 23505 on the named carrier → rollback → re-read → the
// recorded outcome is reported unchanged, never upgraded.
func TestAdminOperationReplayReturnsRecordedOutcome(t *testing.T) {
	f := newAdminFixture(t)
	req := adminReq(AdminActionHoldRelease)
	f.auditErr = &pgconn.PgError{Code: "23505", ConstraintName: "nonce_ops_audit_operation_id_uniq"}
	f.rowHandler = func(sql string, _ []any) pgx.Row {
		switch {
		case strings.Contains(sql, "nonce_ops_audit"):
			return rows(req.OperationID, req.Action, req.ChainID, req.Sender, req.subjectID(),
				string(AdminRefused), req.Operator, req.Reason, req.Evidence,
				auditInputPrefix+adminInputDigest(req)+"; refused earlier", time.Now().UTC())
		case strings.Contains(sql, "indexer_lease"):
			return rows("nonce-008", int64(0), true)
		case strings.Contains(sql, "nonce_scope_state") && strings.Contains(sql, "FOR UPDATE"):
			return rows(adminChain)
		case strings.Contains(sql, "hold_id = $1 FOR UPDATE"):
			return rows(adminHold("nh-target", CauseUnexplainedGap)...)
		case strings.Contains(sql, "nonce_observations"):
			return rows(adminChain, adminSender)
		case strings.Contains(sql, "nonce_wallet_registry"):
			return rows(allocRegistryValues(RegistryActive, 7)...)
		default:
			return noRows()
		}
	}
	f.rowsHandler = adminScopeRows(adminBindingRow(4, StateAllocated))

	res, err := f.runner().Run(context.Background(), req)
	if err != nil {
		t.Fatalf("replay must report the recorded outcome, got %v", err)
	}
	if res.Outcome != AdminRefused {
		t.Fatalf("replay outcome = %q, want the recorded refused (never upgraded)", res.Outcome)
	}
	if res.Detail != "refused earlier" {
		t.Fatalf("replay detail = %q, want the recorded detail", res.Detail)
	}
	if !f.tx(0).rolledBack || f.tx(0).committed {
		t.Fatal("the raced attempt must roll back before the re-read")
	}
}

// TestAdminOperationConflictOnDifferingInput pins the other half of §3.4: a
// recorded operation id with a differing op-input is operation_conflict with
// zero writes.
func TestAdminOperationConflictOnDifferingInput(t *testing.T) {
	f := newAdminFixture(t)
	req := adminReq(AdminActionHoldRelease)
	other := req
	other.Evidence = "a different operator finding"
	f.auditErr = &pgconn.PgError{Code: "23505", ConstraintName: "nonce_ops_audit_operation_id_uniq"}
	f.rowHandler = func(sql string, _ []any) pgx.Row {
		switch {
		case strings.Contains(sql, "nonce_ops_audit"):
			return rows(req.OperationID, req.Action, req.ChainID, req.Sender, req.subjectID(),
				string(AdminApplied), req.Operator, req.Reason, other.Evidence,
				auditInputPrefix+adminInputDigest(other)+"; applied", time.Now().UTC())
		case strings.Contains(sql, "indexer_lease"):
			return rows("nonce-008", int64(0), true)
		case strings.Contains(sql, "nonce_scope_state") && strings.Contains(sql, "FOR UPDATE"):
			return rows(adminChain)
		case strings.Contains(sql, "hold_id = $1 FOR UPDATE"):
			return rows(adminHold("nh-target", CauseUnexplainedGap)...)
		case strings.Contains(sql, "nonce_observations"):
			return rows(adminChain, adminSender)
		case strings.Contains(sql, "nonce_wallet_registry"):
			return rows(allocRegistryValues(RegistryActive, 7)...)
		default:
			return noRows()
		}
	}
	f.rowsHandler = adminScopeRows(adminBindingRow(4, StateAllocated))

	res, err := f.runner().Run(context.Background(), req)
	if !IsOutcome(err, OutcomeOperationConflict) {
		t.Fatalf("error = %v, want operation_conflict", err)
	}
	if res.Outcome != "" {
		t.Fatalf("conflict returned result %+v, want zero result", res)
	}
	if !f.tx(0).rolledBack || f.tx(0).committed {
		t.Fatal("the conflicting attempt must roll back with zero writes")
	}
}

// TestAdminInputDigestCoversEveryInputField keeps the op-input equality honest:
// any differing attempt input changes the digest.
func TestAdminInputDigestCoversEveryInputField(t *testing.T) {
	base := adminReq(AdminActionHoldRelease)
	variants := []AdminRequest{
		func() AdminRequest { r := base; r.Action = AdminActionBindingRelease; return r }(),
		func() AdminRequest { r := base; r.ChainID = base.ChainID + 1; return r }(),
		func() AdminRequest { r := base; r.Sender = "0x6666666666666666666666666666666666666666"; return r }(),
		func() AdminRequest { r := base; r.HoldID = "nh-other"; return r }(),
		func() AdminRequest { r := base; r.ObservationID = "no-other"; return r }(),
		func() AdminRequest { r := base; r.Operator = "operator-B"; return r }(),
		func() AdminRequest { r := base; r.Reason = "other reason"; return r }(),
		func() AdminRequest { r := base; r.Evidence = "other evidence"; return r }(),
	}
	baseDigest := adminInputDigest(base)
	for i, v := range variants {
		if adminInputDigest(v) == baseDigest {
			t.Fatalf("variant %d hashed to the base op-input digest", i)
		}
	}
	if adminInputDigest(base) != baseDigest {
		t.Fatal("digest is not deterministic")
	}
}
