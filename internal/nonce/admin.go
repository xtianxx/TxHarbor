// admin.go owns the nonce-admin operator transaction library: hold-release,
// binding-release, and registry register/disable, each as one serialized
// transaction under the T006 coordination lock with an audit row keyed by
// operation_id (contracts/observation.md §3/§4, data-model §Transaction
// catalog).
//
// Attempt semantics (§3.4): one attempt = one operation_id = at most one
// nonce_ops_audit row (nonce_ops_audit_operation_id_uniq). A concurrent
// duplicate raises 23505 on that exact carrier; the transaction rolls back and
// the recorded attempt is re-read: an equal op-input reports the recorded
// outcome unchanged (never upgraded), a differing op-input is
// operation_conflict with zero writes.
//
// Every write path re-runs the lock discipline (coordination row → scope row
// FOR UPDATE → 006 read-only gate reads → own rows), re-verifies the fresh
// pre-tx observation inside the lock, and writes only on `applied`: a
// `refused` outcome records the audit row and leaves the hold/floor/binding/
// registry untouched, and a `nop` outcome records the audit row with zero
// subject state change. 008 writes zero 006/007 rows.
package nonce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Admin actions are the nonce_ops_audit.action CHECK domain.
const (
	AdminActionHoldRelease      = "hold_release"
	AdminActionBindingRelease   = "binding_release"
	AdminActionRegistryRegister = "registry_register"
	AdminActionRegistryDisable  = "registry_disable"
)

// AdminRequest is one operator attempt. OperationID is minted first by the
// caller (T015's `nonce-admin mint`) and is the only dedup key; ChainID/Sender
// are the authorization scope every action binds before connecting.
type AdminRequest struct {
	Action        string
	OperationID   string
	ChainID       int64
	Sender        string
	HoldID        string // hold-release subject
	BindingID     string // binding-release subject
	ObservationID string // operator-supplied observation reference (in-scope)
	Evidence      string // operator finding (release evidence standard)
	Operator      string // declared operator identity (audit claim)
	Reason        string
}

// Validate applies the pre-tx caller-bug checks. Evidence/observation
// insufficiencies are NOT validated here: they are first-class refused
// outcomes recorded in the audit, per observation.md §3.1.
func (r AdminRequest) Validate() error {
	if r.OperationID == "" {
		return errors.New("operation_id is required (mint it first)")
	}
	if len(r.OperationID) > 128 {
		return fmt.Errorf("operation_id exceeds 128 bytes")
	}
	if r.ChainID <= 0 {
		return fmt.Errorf("admin chain id %d is not positive", r.ChainID)
	}
	if !obsSenderPattern.MatchString(r.Sender) {
		return fmt.Errorf("admin sender %q is not a lowercase 0x + 40 hex address", r.Sender)
	}
	switch r.Action {
	case AdminActionHoldRelease:
		if r.HoldID == "" {
			return errors.New("hold-release requires hold_id")
		}
	case AdminActionBindingRelease:
		if r.BindingID == "" {
			return errors.New("binding-release requires binding_id")
		}
	case AdminActionRegistryRegister, AdminActionRegistryDisable:
	default:
		return fmt.Errorf("unknown admin action %q", r.Action)
	}
	return nil
}

// subjectID is the nonce_ops_audit.subject_id for the action: hold_id /
// binding_id / sender per action (data-model Table 7).
func (r AdminRequest) subjectID() string {
	switch r.Action {
	case AdminActionHoldRelease:
		return r.HoldID
	case AdminActionBindingRelease:
		return r.BindingID
	default:
		return r.Sender
	}
}

// AdminResult is the committed attempt outcome returned to the carrier
// (T015 maps it to exit code 0/1/2 and stdout).
type AdminResult struct {
	Outcome   Outcome
	Action    string
	SubjectID string
	Detail    string
}

// AdminAudit is one nonce_ops_audit row as re-read for attempt dedup.
type AdminAudit struct {
	OperationID string
	Action      string
	ChainID     int64
	Sender      string
	SubjectID   string
	Outcome     Outcome
	Operator    string
	Reason      string
	Evidence    string
	Detail      string
	RecordedAt  time.Time
}

// inputEqual reports whether the recorded attempt carries the request's exact
// op-input. The canonical digest lives in the detail prefix (adminInputDigest).
func (a *AdminAudit) inputEqual(req AdminRequest) bool {
	return auditInputDigestOf(a.Detail) == adminInputDigest(req)
}

// adminTx is the transaction surface the runner drives; pgx.Tx satisfies it.
type adminTx interface {
	txQuerier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// AdminRunner is the operator transaction library. The observer samples the
// chain view OUTSIDE any transaction (hold-release/binding-release only; the
// registry actions take no RPC).
type AdminRunner struct {
	observer *Observer
	begin    func(ctx context.Context) (adminTx, error)
}

// NewAdminRunner wires the operator pool and the pre-tx observer (T015
// constructs it with the operator connection). A nil observer is allowed for
// registry-only use.
func NewAdminRunner(pool *pgxpool.Pool, observer *Observer) *AdminRunner {
	return &AdminRunner{
		observer: observer,
		begin:    func(ctx context.Context) (adminTx, error) { return pool.Begin(ctx) },
	}
}

// Run executes one operator attempt. A committed attempt returns its result
// (applied/refused/nop); an op-id race returns the recorded outcome or
// operation_conflict; a storage failure returns a wrapped error.
func (a *AdminRunner) Run(ctx context.Context, req AdminRequest) (AdminResult, error) {
	if err := req.Validate(); err != nil {
		return AdminResult{}, err
	}

	var obs Observation
	switch req.Action {
	case AdminActionHoldRelease, AdminActionBindingRelease:
		if a.observer == nil {
			return AdminResult{}, errors.New("admin runner has no chain observer")
		}
		obs = a.observer.Observe(ctx, req.ChainID, req.Sender, ObservationKindReconcile)
	case AdminActionRegistryRegister, AdminActionRegistryDisable:
	default:
		return AdminResult{}, fmt.Errorf("unknown admin action %q", req.Action)
	}

	tx, err := a.begin(ctx)
	if err != nil {
		return AdminResult{}, storageRefusal("open admin transaction", err)
	}
	out, err := adminInTx(ctx, tx, req, obs)
	if err != nil {
		_ = tx.Rollback(ctx)
		return AdminResult{}, storageRefusal("admin transaction failed", err)
	}
	if err := insertAuditTx(ctx, tx, req, out); err != nil {
		_ = tx.Rollback(ctx)
		if auditOperationIDConflict(err) {
			// The attempt raced an identical operation id: the recorded
			// attempt is authoritative (rollback already undid any write).
			return a.replayRecorded(ctx, req)
		}
		return AdminResult{}, storageRefusal("record admin audit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdminResult{}, storageRefusal("commit admin transaction", err)
	}
	return AdminResult{Outcome: out.Outcome, Action: req.Action, SubjectID: req.subjectID(), Detail: out.Detail}, nil
}

// adminTxOutcome is the in-tx decision: outcome plus the audit detail. Writes
// (if any) were already performed for `applied`; refused/nop performed none.
type adminTxOutcome struct {
	Outcome Outcome
	Detail  string
}

// adminInTx runs the action under the T006 coordination lock. The fixed lock
// order is preserved: coordination row → scope row FOR UPDATE → 006 gate
// reads → own rows.
func adminInTx(ctx context.Context, tx txQuerier, req AdminRequest, obs Observation) (adminTxOutcome, error) {
	if err := lockChain(ctx, tx, req.ChainID); err != nil {
		return adminTxOutcome{}, err
	}
	switch req.Action {
	case AdminActionHoldRelease:
		return holdReleaseInTx(ctx, tx, req, obs)
	case AdminActionBindingRelease:
		return bindingReleaseInTx(ctx, tx, req, obs)
	default:
		return registryInTx(ctx, tx, req)
	}
}

// holdReleaseInTx is T-hold-release (observation.md §3.1, data-model
// T-hold-release): FOR UPDATE the named hold, re-verify the evidence version
// set under the lock (fresh observation consistent, no unresolved scope
// conflicts, observation reference in scope, registry version, 006 state),
// then clear ONLY the named hold, advance the monotonic floor to the observed
// pending count, and audit `applied`. Any insufficiency or drift is a refused
// audit with zero hold/floor change.
func holdReleaseInTx(ctx context.Context, tx txQuerier, req AdminRequest, obs Observation) (adminTxOutcome, error) {
	if err := lockScopeRowTx(ctx, tx, req.ChainID, req.Sender); err != nil {
		return adminTxOutcome{}, err
	}

	hold, err := readHoldByIDForUpdateTx(ctx, tx, req.HoldID)
	if err != nil {
		return adminTxOutcome{}, err
	}
	if hold == nil || hold.Status != HoldStatusActive {
		return adminTxOutcome{Outcome: AdminNop, Detail: "hold is missing or already released; zero change"}, nil
	}
	if hold.ChainID != req.ChainID || hold.Sender != req.Sender {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "hold scope does not match the requested chain_id/sender"}, nil
	}
	if req.Evidence == "" {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "release requires an operator finding (evidence)"}, nil
	}
	if req.ObservationID == "" {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "release requires an observation reference"}, nil
	}
	refScope, err := readObservationScopeTx(ctx, tx, req.ObservationID)
	if err != nil {
		return adminTxOutcome{}, err
	}
	if refScope == nil || refScope.chainID != req.ChainID || refScope.sender != req.Sender {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "observation reference is missing or outside the scope"}, nil
	}
	gates, err := readGateStateTx(ctx, tx, req.ChainID)
	if err != nil {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "006 gate state is unreadable at the re-verify point"}, nil
	}
	registry, err := readRegistryBySenderTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return adminTxOutcome{}, err
	}
	if registry == nil {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "registry row is missing for the scope"}, nil
	}
	bindings, err := readBindingsByScopeTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return adminTxOutcome{}, err
	}
	scope, err := readScopeStateTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return adminTxOutcome{}, err
	}
	if !scopeAttributionOK(bindings) {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "scope has unattributable bindings; resolve them first"}, nil
	}

	decision := Classify(ClassificationInput{
		ReadFailed:      obs.Classification == ClassificationUnavailable,
		Latest:          obs.LatestCount,
		Pending:         obs.PendingCount,
		PendingPrev:     scopeLastPending(scope),
		MaxBindingNonce: MaxBoundNonce(bindings),
		HasBindings:     len(bindings) > 0,
		ReconciledFloor: scopeFloor(scope),
	})
	if decision.Classification != ClassificationConsistent {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "fresh observation still classifies " + decision.Classification}, nil
	}

	active, err := readActiveHoldsTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return adminTxOutcome{}, err
	}
	freshID, err := insertObservationTx(ctx, tx, Observation{
		ChainID:        req.ChainID,
		Sender:         req.Sender,
		Kind:           ObservationKindReconcile,
		Classification: decision.Classification,
		LatestCount:    obs.LatestCount,
		PendingCount:   obs.PendingCount,
		HeadNumber:     obs.HeadNumber,
		HeadHash:       obs.HeadHash,
		RPCRef:         obs.RPCRef,
	})
	if err != nil {
		return adminTxOutcome{}, err
	}

	version := fmt.Sprintf("registry_seq=%d registry_state=%s active_holds=%d 006=%s release_observation=%s",
		registry.RegistrySeq, registry.State, len(active), gateSummary(gates), freshID)
	if ok, err := releaseHoldTx(ctx, tx, req.HoldID, req, req.Evidence+" | "+version, freshID); err != nil {
		return adminTxOutcome{}, err
	} else if !ok {
		return adminTxOutcome{}, errors.New("hold release lost its active row")
	}
	if _, err := advanceScopeFloorTx(ctx, tx, req.ChainID, req.Sender, obs.PendingCount); err != nil {
		return adminTxOutcome{}, err
	}
	return adminTxOutcome{Outcome: AdminApplied, Detail: version}, nil
}

// bindingReleaseInTx is T-binding-release (observation.md §3.2): only a
// non-terminal binding is disposable, and the no-external-side-effect
// determination must rest on the fresh chain re-observation plus the operator
// finding — a timeout or connection error alone is never evidence.
func bindingReleaseInTx(ctx context.Context, tx txQuerier, req AdminRequest, obs Observation) (adminTxOutcome, error) {
	if err := lockScopeRowTx(ctx, tx, req.ChainID, req.Sender); err != nil {
		return adminTxOutcome{}, err
	}

	b, err := readBindingByIDForUpdateTx(ctx, tx, req.BindingID)
	if err != nil {
		return adminTxOutcome{}, err
	}
	if b == nil || b.State == StateReleased {
		return adminTxOutcome{Outcome: AdminNop, Detail: "binding is missing or already released; zero change"}, nil
	}
	if b.ChainID != req.ChainID || b.Sender != req.Sender {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "binding scope does not match the requested chain_id/sender"}, nil
	}
	if b.State == StateConsumed {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "binding is consumed; its nonce was used and is never released"}, nil
	}
	if req.Evidence == "" {
		return adminTxOutcome{Outcome: AdminRefused,
			Detail: "release requires explicit no-side-effect evidence; a timeout or connection error alone is never evidence"}, nil
	}
	if req.ObservationID != "" {
		refScope, err := readObservationScopeTx(ctx, tx, req.ObservationID)
		if err != nil {
			return adminTxOutcome{}, err
		}
		if refScope == nil || refScope.chainID != req.ChainID || refScope.sender != req.Sender {
			return adminTxOutcome{Outcome: AdminRefused, Detail: "observation reference is missing or outside the scope"}, nil
		}
	}
	if obs.Classification == ClassificationUnavailable || obs.LatestCount == nil || obs.PendingCount == nil {
		return adminTxOutcome{Outcome: AdminRefused,
			Detail: "fresh chain re-observation unavailable; no-side-effect evidence was not established"}, nil
	}
	if obs.LatestCount.Cmp(obs.PendingCount) > 0 {
		return adminTxOutcome{Outcome: AdminRefused, Detail: "fresh chain view diverges; the evidence is not trustworthy"}, nil
	}
	if obs.LatestCount.Cmp(b.Nonce) > 0 || obs.PendingCount.Cmp(b.Nonce) > 0 {
		return adminTxOutcome{Outcome: AdminRefused,
			Detail: "fresh observation shows the nonce mined or pending; the binding has an external side effect"}, nil
	}

	if ok, err := advanceBindingStateTx(ctx, tx, b.BindingID, StateReleased, req.OperationID, b.State); err != nil {
		return adminTxOutcome{}, err
	} else if !ok {
		return adminTxOutcome{}, errors.New("binding release lost its expected state")
	}
	detail := fmt.Sprintf("fresh latest=%s pending=%s nonce=%s",
		FormatDecimal(obs.LatestCount), FormatDecimal(obs.PendingCount), FormatDecimal(b.Nonce))
	if created, err := insertBindingEventTx(ctx, tx, BindingEvent{
		BindingID:   b.BindingID,
		FromState:   &b.State,
		ToState:     StateReleased,
		OperationID: req.OperationID,
		Detail:      detail + "; " + req.Evidence,
	}); err != nil {
		return adminTxOutcome{}, err
	} else if !created {
		return adminTxOutcome{}, errors.New("binding release event was not written")
	}
	return adminTxOutcome{Outcome: AdminApplied, Detail: detail}, nil
}

// registryInTx is T-registry (observation.md §3.3): register inserts or
// re-enables with a bumped registry_seq, disable flips the state with a
// bumped registry_seq; both are RowsAffected-guarded and audited. Registry
// state gates admission only — existing binding facts are never altered.
func registryInTx(ctx context.Context, tx txQuerier, req AdminRequest) (adminTxOutcome, error) {
	switch req.Action {
	case AdminActionRegistryRegister:
		tag, err := tx.Exec(ctx, ensureRegistryRowSQL, req.ChainID, req.Sender)
		if err != nil {
			return adminTxOutcome{}, fmt.Errorf("register wallet: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return adminTxOutcome{Outcome: AdminApplied, Detail: "sender registered"}, nil
		}
		tag, err = tx.Exec(ctx, bumpRegistryActiveSQL, req.ChainID, req.Sender)
		if err != nil {
			return adminTxOutcome{}, fmt.Errorf("re-enable wallet: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return adminTxOutcome{Outcome: AdminApplied, Detail: "sender re-enabled with a bumped registry_seq"}, nil
		}
		return adminTxOutcome{Outcome: AdminNop, Detail: "sender is already registered and active; zero change"}, nil
	default:
		tag, err := tx.Exec(ctx, disableRegistrySQL, req.ChainID, req.Sender)
		if err != nil {
			return adminTxOutcome{}, fmt.Errorf("disable wallet: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return adminTxOutcome{Outcome: AdminApplied, Detail: "sender disabled with a bumped registry_seq"}, nil
		}
		return adminTxOutcome{Outcome: AdminNop, Detail: "sender is unregistered or already disabled; zero change"}, nil
	}
}

// replayRecorded re-reads the recorded attempt after an operation-id 23505 and
// compares the op-input: equal reports the recorded outcome unchanged (never
// upgraded), differing is operation_conflict with zero writes (observation.md
// §3.4).
func (a *AdminRunner) replayRecorded(ctx context.Context, req AdminRequest) (AdminResult, error) {
	tx, err := a.begin(ctx)
	if err != nil {
		return AdminResult{}, storageRefusal("open replay classify transaction", err)
	}
	row, err := readAuditByOperationIDTx(ctx, tx, req.OperationID)
	_ = tx.Rollback(ctx) // read-only classify; never commits
	if err != nil {
		return AdminResult{}, storageRefusal("re-read audit by operation id", err)
	}
	if row == nil {
		return AdminResult{}, Refuse(OutcomeTemporarilyUnavailable,
			"operation-id race classify miss; retry with the same operation id")
	}
	if !row.inputEqual(req) {
		return AdminResult{}, Refuse(OutcomeOperationConflict,
			"operation_id was already recorded with a differing op-input")
	}
	return AdminResult{
		Outcome:   row.Outcome,
		Action:    row.Action,
		SubjectID: row.SubjectID,
		Detail:    auditDetailOf(row.Detail),
	}, nil
}

// scopeAttributionOK mirrors observation.md §3.1 item 3: every binding in the
// scope is attributable (has an intent and a recorded state) and
// constraint-consistent. Non-terminal items may stay in place — the release
// leaves them untouched — but an unattributable row blocks the release.
func scopeAttributionOK(bindings []Binding) bool {
	for i := range bindings {
		if bindings[i].IntentID == "" {
			return false
		}
		switch bindings[i].State {
		case StateAllocated, StateInFlight, StateConsumed, StateReleased:
		default:
			return false
		}
		if !bindings[i].TerminalConsistent() {
			return false
		}
	}
	return true
}

// gateSummary renders the read-only 006 state observed at the re-verify point
// (evidence only; 008 never writes it).
func gateSummary(g GateState) string {
	if causes := g.Causes(); len(causes) > 0 {
		return strings.Join(causes, ",")
	}
	return "clear"
}

// Hold-release SQL. The named hold is locked by its stable identity; only the
// active row is cleared, so a racing release converges to zero affected rows.
const readHoldByIDForUpdateSQL = holdColumnsSQL + ` WHERE hold_id = $1 FOR UPDATE`

const releaseHoldSQL = `
UPDATE nonce_scope_holds
SET status = 'released',
    released_at = now(),
    released_by = $2,
    release_operation_id = $3,
    release_evidence = $4,
    release_observation_id = $5
WHERE hold_id = $1 AND status = 'active'`

// readObservationScopeTx verifies the operator-supplied observation reference
// belongs to the scope; a missing row yields (nil, nil).
const readObservationScopeSQL = `
SELECT chain_id, sender FROM nonce_observations WHERE observation_id = $1`

// observationScope is the (chain_id, sender) ownership fact of one
// observation row.
type observationScope struct {
	chainID int64
	sender  string
}

// Binding-release SQL: the binding row is locked FOR UPDATE; the released
// transition itself goes through binding.go's guarded advance.
const readBindingByIDForUpdateSQL = bindingColumnsSQL + ` WHERE binding_id = $1 FOR UPDATE`

// Registry SQL. register materialises a first registration with registry_seq
// 1, or re-enables a disabled row with a bumped registry_seq; disable flips
// only an active row. Every guarded statement reports its real row count.
const (
	ensureRegistryRowSQL = `
INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
VALUES ($1, $2, 'active', 1)
ON CONFLICT (chain_id, sender) DO NOTHING`

	bumpRegistryActiveSQL = `
UPDATE nonce_wallet_registry
SET state = 'active', registry_seq = registry_seq + 1, updated_at = now()
WHERE chain_id = $1 AND sender = $2 AND state = 'disabled'`

	disableRegistrySQL = `
UPDATE nonce_wallet_registry
SET state = 'disabled', registry_seq = registry_seq + 1, updated_at = now()
WHERE chain_id = $1 AND sender = $2 AND state = 'active'`
)

// nonce_ops_audit SQL. The canonical op-input digest rides in the detail
// prefix so attempt dedup compares the exact input set without a schema
// change.
const (
	insertAuditSQL = `
INSERT INTO nonce_ops_audit (operation_id, action, chain_id, sender, subject_id, outcome, operator, reason, evidence, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	auditColumnsSQL = `
SELECT operation_id, action, chain_id, sender, subject_id, outcome,
       operator, reason, evidence, detail, recorded_at
FROM nonce_ops_audit`

	readAuditByOperationIDSQL = auditColumnsSQL + ` WHERE operation_id = $1`

	auditInputPrefix = "input="
)

// readHoldByIDForUpdateTx locks the named hold row (any status) and returns it;
// a missing row yields (nil, nil) — the caller records `nop`.
func readHoldByIDForUpdateTx(ctx context.Context, q txQuerier, holdID string) (*ScopeHold, error) {
	h, err := scanHold(q.QueryRow(ctx, readHoldByIDForUpdateSQL, holdID))
	if err != nil {
		return nil, fmt.Errorf("read hold FOR UPDATE: %w", err)
	}
	return h, nil
}

// readObservationScopeTx reads the (chain_id, sender) ownership of the
// operator's observation reference; a missing row yields (nil, nil).
func readObservationScopeTx(ctx context.Context, q txQuerier, observationID string) (*observationScope, error) {
	var s observationScope
	err := q.QueryRow(ctx, readObservationScopeSQL, observationID).Scan(&s.chainID, &s.sender)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read observation scope: %w", err)
	}
	return &s, nil
}

// releaseHoldTx clears exactly the named active hold and records the operator
// facts + the fresh observation used by the re-verify. Zero affected rows is a
// concurrent release, never a silent success.
func releaseHoldTx(ctx context.Context, q txQuerier, holdID string, req AdminRequest, evidence, freshObservationID string) (bool, error) {
	tag, err := q.Exec(ctx, releaseHoldSQL, holdID, req.Operator, req.OperationID, evidence, freshObservationID)
	if err != nil {
		return false, fmt.Errorf("release hold: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// readBindingByIDForUpdateTx locks the named binding row; a missing row yields
// (nil, nil) — the caller records `nop`.
func readBindingByIDForUpdateTx(ctx context.Context, q txQuerier, bindingID string) (*Binding, error) {
	b, err := scanBinding(q.QueryRow(ctx, readBindingByIDForUpdateSQL, bindingID))
	if err != nil {
		return nil, fmt.Errorf("read binding FOR UPDATE: %w", err)
	}
	return b, nil
}

// insertAuditTx records the committed attempt. Detail carries the canonical
// op-input digest followed by the outcome detail.
func insertAuditTx(ctx context.Context, q txQuerier, req AdminRequest, out adminTxOutcome) error {
	detail := auditInputPrefix + adminInputDigest(req) + "; " + out.Detail
	if _, err := q.Exec(ctx, insertAuditSQL, req.OperationID, req.Action, req.ChainID, req.Sender,
		req.subjectID(), string(out.Outcome), req.Operator, req.Reason, req.Evidence, detail); err != nil {
		return fmt.Errorf("insert nonce ops audit: %w", err)
	}
	return nil
}

// readAuditByOperationIDTx reads the recorded attempt keyed by operation id;
// a miss yields (nil, nil).
func readAuditByOperationIDTx(ctx context.Context, q txQuerier, operationID string) (*AdminAudit, error) {
	var a AdminAudit
	err := q.QueryRow(ctx, readAuditByOperationIDSQL, operationID).Scan(
		&a.OperationID, &a.Action, &a.ChainID, &a.Sender, &a.SubjectID, &a.Outcome,
		&a.Operator, &a.Reason, &a.Evidence, &a.Detail, &a.RecordedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read audit by operation id: %w", err)
	}
	return &a, nil
}

// auditOperationIDConflict reports whether err is the 23505 raised by the
// named nonce_ops_audit operation-id carrier. Exact ConstraintName match only.
func auditOperationIDConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr.Code == "23505" &&
		pgErr.ConstraintName == "nonce_ops_audit_operation_id_uniq"
}

// adminInputDigest is the canonical op-input digest: the exact attempt input
// set, so a replay with any differing input is operation_conflict.
func adminInputDigest(req AdminRequest) string {
	parts := []string{
		req.Action,
		strconv.FormatInt(req.ChainID, 10),
		req.Sender,
		req.subjectID(),
		req.ObservationID,
		req.Operator,
		req.Reason,
		req.Evidence,
	}
	h := sha256.New()
	for _, part := range parts {
		_, _ = io.WriteString(h, part)
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// auditInputDigestOf extracts the canonical digest from a recorded detail;
// "" when the row predates the digest protocol.
func auditInputDigestOf(detail string) string {
	if !strings.HasPrefix(detail, auditInputPrefix) {
		return ""
	}
	rest := detail[len(auditInputPrefix):]
	i := strings.IndexByte(rest, ';')
	if i < 0 {
		return ""
	}
	return rest[:i]
}

// auditDetailOf strips the digest prefix for presentation.
func auditDetailOf(detail string) string {
	if !strings.HasPrefix(detail, auditInputPrefix) {
		return detail
	}
	i := strings.Index(detail, "; ")
	if i < 0 {
		return ""
	}
	return detail[i+2:]
}
