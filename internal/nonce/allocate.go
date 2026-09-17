// allocate.go owns the admission core: the serialized allocate transaction
// that re-validates every gate, classifies the observed chain view, selects
// the candidate nonce from durable state, and commits at most one binding per
// intent (FR-01..FR-06, FR-15/FR-17, data-model T-allocate, R1/R2/R3/R5/R10).
//
// Transaction shape (one tx per admission): pre-tx input validation → pre-tx
// RPC observation (observe.go, OUTSIDE any tx; zero RPC inside a tx) → BEGIN
// → lockChain (T006 framing) → 006 gate recheck → registry recheck → scope
// row FOR UPDATE → active-hold recheck → 007 authorization row FOR SHARE →
// intent re-read (T-replay / T-conflict) → recompute M/F from nonce_bindings
// under the lock → Classify (T010) → insert observation (always) → insert
// binding + creation event, or commit the observation (+ hold when the matrix
// demands one) and refuse with no binding.
//
// Cross-module lock order is preserved: coordination row → scope row → 007
// authorization row → own rows. The 006 gate reads and the registry read
// between the coordination lock and the scope lock are non-locking SELECTs,
// so they occupy no position in the lock order; the registry read must
// precede scope-row creation because nonce_scope_state carries the registry
// FK (an unregistered sender refuses with zero writes instead of tripping
// the FK). Every refusal before classification is rolled back with zero
// domain writes; a classification refusal commits its observation (and hold)
// and no binding; T-replay rolls back and returns the original binding
// untouched; T-conflict rolls back with allocation_conflict and never a
// second binding.
package nonce

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/logx"
)

// AllocationRequest is the canonical allocation input (data-model §Canonical
// allocation input). The four fields are the full input-equality set for
// replay/conflict classification (R3); the candidate nonce is never an input.
type AllocationRequest struct {
	IntentID        string
	ChainID         int64
	Sender          string
	AuthorizationID string
}

// Validate applies the pre-tx input validation; all checks are fail-closed
// and run before any RPC or DB access. A malformed request is a caller bug,
// so it surfaces as a plain validation error (no wire outcome exists).
func (r AllocationRequest) Validate() error {
	if r.ChainID <= 0 {
		return fmt.Errorf("allocation chain id %d is not positive", r.ChainID)
	}
	if !obsSenderPattern.MatchString(r.Sender) {
		return fmt.Errorf("allocation sender %q is not a lowercase 0x + 40 hex address", r.Sender)
	}
	if err := validateOpaqueID("intent_id", r.IntentID, 0x21); err != nil {
		return err
	}
	return validateOpaqueID("authorization_id", r.AuthorizationID, 0x20)
}

// validateOpaqueID enforces the 1..128 printable-ASCII carrier shape;
// printableFrom is 0x21 (no spaces) for intent_id and 0x20 (printable) for
// authorization_id, exactly as data-model §Canonical allocation input states.
func validateOpaqueID(name, value string, printableFrom byte) error {
	if len(value) < 1 || len(value) > 128 {
		return fmt.Errorf("%s must be 1..128 bytes, got %d", name, len(value))
	}
	for i := 0; i < len(value); i++ {
		if value[i] < printableFrom || value[i] > 0x7e {
			return fmt.Errorf("%s contains a non-printable byte at offset %d", name, i)
		}
	}
	return nil
}

// allocTx is the transaction surface Allocate drives; pgx.Tx satisfies it.
type allocTx interface {
	txQuerier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Allocator is the admission core. The observer samples the chain view
// OUTSIDE any transaction; begin opens one pooled transaction per attempt.
type Allocator struct {
	observer *Observer
	begin    func(ctx context.Context) (allocTx, error)
	// gate is the fail-closed startup readiness gate (R5/FR-13): while it is
	// closed every admission refuses rebuild_incomplete with the recorded
	// cause, before any RPC or DB access. A nil gate is the unit-test seam
	// (ungated); production wires the gate the startup verification drives.
	gate *RebuildGate
	// logger and sink are the optional observability seam (T020, R11/FR-21).
	// A nil logger falls back to slog.Default; a nil sink skips counting.
	// Both are emission-only and never affect an admission outcome.
	logger *slog.Logger
	sink   AllocationSink
}

// AllocationSink is the metrics seam for admission observability (R11/FR-21).
// The *metrics.Metrics registry satisfies it structurally, so the nonce
// package never imports the registry and the app layer stays the only wiring
// point. Every argument is a fixed-vocabulary value: no sender, nonce, intent
// or hold value is ever passed (FR-21/SC-09).
type AllocationSink interface {
	ObserveNonceAllocation(result string)
	ObserveNonceReplay()
	ObserveNonceObservation(classification string)
}

// holdEventSink is the optional extension the concrete metrics registry
// implements for hold establishments. It is a separate interface so existing
// AllocationSink doubles stay valid (an optional capability, not a widened
// requirement). The count is label-free: the cause rides the redacted log
// line and the hold row, never a metric label.
type holdEventSink interface {
	ObserveNonceHoldEstablished()
}

// allocationObservation is the structured, pre-redaction payload of one
// admission for the T020 observability funnel. It carries no key material:
// emitAllocation runs every string field through logx.Redact before slog.
type allocationObservation struct {
	chainID        int64
	sender         string
	nonce          *big.Int
	bindingID      string
	intentID       string
	holdID         string
	cause          string
	classification string
	registrySeq    int64
	result         Outcome
}

// newAllocObservation seeds the request-derived fields.
func newAllocObservation(req AllocationRequest) allocationObservation {
	return allocationObservation{chainID: req.ChainID, sender: req.Sender, intentID: req.IntentID}
}

// mergeAllocObservation folds the in-tx attempt payload into dst, preserving
// whatever dst already carries.
func mergeAllocObservation(dst *allocationObservation, src allocationObservation) {
	if src.result != "" {
		dst.result = src.result
	}
	if src.cause != "" {
		dst.cause = src.cause
	}
	if src.classification != "" {
		dst.classification = src.classification
	}
	if src.holdID != "" {
		dst.holdID = src.holdID
	}
	if src.bindingID != "" {
		dst.bindingID = src.bindingID
	}
	if src.nonce != nil {
		dst.nonce = src.nonce
	}
	if src.registrySeq != 0 {
		dst.registrySeq = src.registrySeq
	}
}

// WithObservability installs the optional observability seam: logger is the
// structured-log destination (nil -> slog.Default) and sink the metrics seam
// (nil -> no counting). It returns the allocator so NewAllocator can be
// chained.
func (a *Allocator) WithObservability(logger *slog.Logger, sink AllocationSink) *Allocator {
	a.logger = logger
	a.sink = sink
	return a
}

// emitAllocation records one admission through the logx redaction funnel and
// the optional metrics sink. It is emission-only and never changes an outcome.
func (a *Allocator) emitAllocation(obs allocationObservation) {
	if obs.result == "" {
		return
	}
	if a.sink != nil {
		a.sink.ObserveNonceAllocation(string(obs.result))
		if obs.result == OutcomeReplayed {
			a.sink.ObserveNonceReplay()
		}
		if obs.classification != "" {
			a.sink.ObserveNonceObservation(obs.classification)
		}
		// A hold established by this attempt's classification refusal is
		// exactly the case holdID != "" with a classification set: the
		// pre-existing-hold refusal path sets holdID but never classifies,
		// so it is excluded here (its free-form "active holds: ..." cause
		// must never become a label). Rolled-back paths never set holdID.
		if obs.holdID != "" && obs.classification != "" {
			if hs, ok := a.sink.(holdEventSink); ok {
				hs.ObserveNonceHoldEstablished()
			}
		}
	}
	logger := a.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("nonce allocation",
		"chain_id", obs.chainID,
		"sender", logx.Redact(obs.sender),
		"nonce", logx.Redact(FormatDecimal(obs.nonce)),
		"binding_id", logx.Redact(obs.bindingID),
		"intent_id", logx.Redact(obs.intentID),
		"hold_id", logx.Redact(obs.holdID),
		"cause", logx.Redact(obs.cause),
		"classification", obs.classification,
		"registry_seq", obs.registrySeq)
}

// NewAllocator wires the pooled transaction opener, the pre-tx observer and
// the required startup rebuild gate (R5/FR-13): while it is closed every
// admission refuses rebuild_incomplete before any RPC or DB access. The gate
// is required (never variadic-optional) so no caller can silently lose the
// R5/FR-13 protection; tests pass an explicitly opened gate.
func NewAllocator(pool *pgxpool.Pool, observer *Observer, gate *RebuildGate) *Allocator {
	return &Allocator{
		observer: observer,
		gate:     gate,
		begin:    func(ctx context.Context) (allocTx, error) { return pool.Begin(ctx) },
	}
}

// Allocate is the T-allocate entry point: it returns the volatile binding
// with OutcomeAllocated, the untouched original with OutcomeReplayed, or a
// classified *Error refusal (outcome duplicated as the returned Outcome).
// Every admission attempt persists its observation when classification was
// reached; RPC runs strictly before BEGIN.
func (a *Allocator) Allocate(ctx context.Context, req AllocationRequest) (*Binding, Outcome, error) {
	obs := newAllocObservation(req)
	defer func() { a.emitAllocation(obs) }()

	if err := req.Validate(); err != nil {
		return nil, "", err
	}

	// R5/FR-13: the rebuild gate is consulted before any RPC or DB access, so
	// a closed gate fails closed with zero external effect and no memory-based
	// nonce derivation.
	if a.gate != nil && !a.gate.IsOpen() {
		_, cause := a.gate.State()
		if cause == "" {
			cause = "startup rebuild verification has not completed"
		}
		obs.result, obs.cause = OutcomeRebuildIncomplete, cause
		return nil, OutcomeRebuildIncomplete, Refuse(OutcomeRebuildIncomplete, cause)
	}

	// R4: the chain view is sampled once, before the transaction opens.
	observation := a.observer.Observe(ctx, req.ChainID, req.Sender, ObservationKindAllocation)

	tx, err := a.begin(ctx)
	if err != nil {
		obs.result, obs.cause = OutcomeTemporarilyUnavailable, "open allocation transaction"
		return nil, OutcomeTemporarilyUnavailable, storageRefusal("open allocation transaction", err)
	}

	res, err := allocateInTx(ctx, tx, req, observation)
	mergeAllocObservation(&obs, res.obs)
	switch {
	case err != nil:
		_ = tx.Rollback(ctx)
		if bindingCarrierConstraint(err) {
			binding, outcome, rerr := a.convergeAfterRace(ctx, req, attemptedNonce(res.attempted))
			obs.result = outcome
			if binding != nil {
				obs.nonce, obs.bindingID, obs.registrySeq = binding.Nonce, binding.BindingID, binding.RegistrySeq
			} else {
				obs.nonce = attemptedNonce(res.attempted)
			}
			if rerr != nil {
				obs.result = OutcomeTemporarilyUnavailable
			}
			return binding, outcome, rerr
		}
		obs.result, obs.cause = OutcomeTemporarilyUnavailable, "allocation transaction failed"
		return nil, OutcomeTemporarilyUnavailable, storageRefusal("allocation transaction failed", err)
	case res.refuse != nil && res.refuse.Outcome == OutcomeReplayed:
		// T-replay: the original binding was returned, no row was touched.
		_ = tx.Rollback(ctx)
		return res.binding, OutcomeReplayed, nil
	case res.refuse != nil && !res.persist:
		// Gate/registry/hold/authorization refusal, or T-conflict: zero
		// domain writes, so the transaction is abandoned.
		_ = tx.Rollback(ctx)
		return nil, res.refuse.Outcome, res.refuse
	case res.refuse != nil:
		// Classification refusal: the observation (+ hold) is the evidence and
		// must be committed.
		if cerr := tx.Commit(ctx); cerr != nil {
			obs.result, obs.cause = OutcomeTemporarilyUnavailable, "commit classification refusal"
			return nil, OutcomeTemporarilyUnavailable, storageRefusal("commit classification refusal", cerr)
		}
		return nil, res.refuse.Outcome, res.refuse
	default:
		if cerr := tx.Commit(ctx); cerr != nil {
			// Commit-unknown: retry with the same input set converges through
			// the UNIQUE carriers (T-converge).
			obs.result, obs.cause = OutcomeTemporarilyUnavailable, "commit allocation"
			return nil, OutcomeTemporarilyUnavailable, storageRefusal("commit allocation", cerr)
		}
		return res.binding, OutcomeAllocated, nil
	}
}

// allocationTxResult is the in-tx outcome carrier.
type allocationTxResult struct {
	// binding is the committed candidate (commit) or the untouched original
	// on T-replay (rollback); nil for a refusal.
	binding *Binding
	// attempted is the candidate binding whose insert raced, for T-converge
	// step 2; nil unless the binding insert failed.
	attempted *Binding
	// refuse is the classified refusal. OutcomeReplayed marks the untouched
	// original (successful replay); nil means a new binding is ready.
	refuse *Error
	// persist is true only for classification refusals, whose observation
	// (+ hold) must be committed; every other refusal path is rolled back.
	persist bool
	// obs is the emission-only observability payload of this attempt (T020);
	// the allocation logic never reads it.
	obs allocationObservation
}

// allocateInTx runs the T-allocate steps inside the caller's transaction.
func allocateInTx(ctx context.Context, tx txQuerier, req AllocationRequest, obs Observation) (allocationTxResult, error) {
	o := newAllocObservation(req)
	if err := lockChain(ctx, tx, req.ChainID); err != nil {
		return allocationTxResult{}, err
	}

	// 006 precedence (R1): the coordination lock is held, so pause/recovery
	// establishment cannot commit between these reads and ours.
	gates, err := readGateStateTx(ctx, tx, req.ChainID)
	if err != nil {
		return allocationTxResult{}, err
	}
	if gates.Any() {
		o.result, o.cause = OutcomeRecoveryActive, "006 gate active: "+strings.Join(gates.Causes(), ",")
		return allocationTxResult{
			refuse: Refuse(OutcomeRecoveryActive, o.cause),
			obs:    o,
		}, nil
	}

	// Registry recheck (OC-2): must precede scope-row creation (FK carrier).
	registry, err := readRegistryBySenderTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return allocationTxResult{}, err
	}
	if refusal := registry.AdmissionRefusal(); refusal != "" {
		// An absent row (sender_not_registered) carries no version: only a
		// present row has a registry_seq to record. Dereferencing unconditionally
		// faults on exactly the fail-closed path that must never fault.
		o.result, o.cause = Outcome(refusal), registryRefusalReason(registry)
		if registry != nil {
			o.registrySeq = registry.RegistrySeq
		}
		return allocationTxResult{
			refuse: Refuse(refusal, o.cause),
			obs:    o,
		}, nil
	}

	// Scope row FOR UPDATE: every later own-row read and write is serialized
	// per scope under it (M/F and the hold set are recomputed from row state).
	if err := ensureScopeRowTx(ctx, tx, req.ChainID, req.Sender); err != nil {
		return allocationTxResult{}, err
	}
	if err := lockScopeRowTx(ctx, tx, req.ChainID, req.Sender); err != nil {
		return allocationTxResult{}, err
	}

	holds, err := readActiveHoldsTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return allocationTxResult{}, err
	}
	if len(holds) > 0 {
		o.result = OutcomeScopeHeld
		o.cause = "active holds: " + strings.Join(holdCauseList(holds), ",")
		o.holdID = holds[0].HoldID
		return allocationTxResult{
			refuse: Refuse(OutcomeScopeHeld, o.cause),
			obs:    o,
		}, nil
	}

	// 007 authorization row FOR SHARE: after the scope row, before own rows
	// (cross-module order). A RevokeGrant/SupplyGrant that committed first is
	// observed here (the WHERE re-evaluates under the share lock) and refused;
	// one racing this admission blocks until commit. 008 writes zero 007 rows.
	auth, err := readAuthorizationForShareTx(ctx, tx, req.AuthorizationID, req.ChainID)
	if err != nil {
		return allocationTxResult{}, err
	}
	if auth == nil {
		o.result = OutcomeAuthorizationInvalid
		o.cause = "authorization is missing, inactive, expired, or chain-mismatched"
		return allocationTxResult{
			refuse: Refuse(OutcomeAuthorizationInvalid, o.cause),
			obs:    o,
		}, nil
	}

	// R3: the durable intent re-read decides replay/conflict; it runs after
	// the authorization check so a revoked/expired authorization never
	// re-delivers a persisted binding (FR-14/FR-17).
	existing, err := readBindingByIntentTx(ctx, tx, req.IntentID)
	if err != nil {
		return allocationTxResult{}, err
	}
	if existing != nil {
		if bindingMatchesInput(existing, req) {
			o.result, o.cause = OutcomeReplayed, "allocation input equality"
			o.bindingID, o.nonce, o.registrySeq = existing.BindingID, existing.Nonce, existing.RegistrySeq
			return allocationTxResult{
				binding: existing,
				refuse:  Refuse(OutcomeReplayed, o.cause),
				obs:     o,
			}, nil
		}
		o.result, o.cause = OutcomeAllocationConflict, "intent already bound to a differing input set"
		return allocationTxResult{
			refuse: Refuse(OutcomeAllocationConflict, o.cause),
			obs:    o,
		}, nil
	}

	// M is recomputed from nonce_bindings under the scope lock; F and the
	// previous pending count come from the durable scope row.
	bindings, err := readBindingsByScopeTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return allocationTxResult{}, err
	}
	scope, err := readScopeStateTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return allocationTxResult{}, err
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

	// One evidence row per attempt, on every outcome that reached
	// classification (including refusals and unavailable reads).
	obs.Classification = decision.Classification
	observationID, err := insertObservationTx(ctx, tx, obs)
	if err != nil {
		return allocationTxResult{}, err
	}

	// Persist the verified waterline in the same transaction. last_* are
	// "last observed" facts, not chain truth and never the allocated nonce:
	// only a successful read's counts advance them, so a failed/expired read
	// (nil counts) and a contradictory view (divergence) leave the previous
	// waterline intact for the next admission's PendingPrev comparison.
	if obs.LatestCount != nil && obs.PendingCount != nil &&
		obs.Classification != ClassificationUnavailable &&
		obs.Classification != ClassificationDivergence {
		if err := updateScopeFrontierTx(ctx, tx, req.ChainID, req.Sender,
			obs.LatestCount, obs.PendingCount, observationID); err != nil {
			return allocationTxResult{}, err
		}
	}

	if decision.Candidate == nil {
		o.result, o.cause, o.classification = decision.Refusal, decision.Classification, decision.Classification
		if decision.HoldCause != "" {
			hold, err := establishHoldTx(ctx, tx, HoldEstablishment{
				ChainID:       req.ChainID,
				Sender:        req.Sender,
				Cause:         decision.HoldCause,
				ObservationID: observationID,
				Detail:        decision.Classification,
			})
			if err != nil {
				return allocationTxResult{}, err
			}
			o.holdID = hold.HoldID
		}
		return allocationTxResult{persist: true,
			refuse: Refuse(decision.Refusal, decision.Classification),
			obs:    o}, nil
	}

	bindingID, err := newBindingID()
	if err != nil {
		return allocationTxResult{}, err
	}
	binding := Binding{
		BindingID:               bindingID,
		IntentID:                req.IntentID,
		ChainID:                 req.ChainID,
		Sender:                  req.Sender,
		Nonce:                   decision.Candidate,
		State:                   StateAllocated,
		AuthorizationID:         req.AuthorizationID,
		AuthorizationVersion:    authorizationVersionDigest(auth, req.AuthorizationID),
		RegistrySeq:             registry.RegistrySeq,
		AllocationObservationID: observationID,
	}
	if err := insertBindingTx(ctx, tx, binding); err != nil {
		return allocationTxResult{attempted: &binding}, err
	}
	created, err := insertBindingEventTx(ctx, tx, BindingEvent{
		BindingID:     bindingID,
		ToState:       StateAllocated,
		ObservationID: observationID,
		Detail:        "admission",
	})
	if err != nil {
		return allocationTxResult{attempted: &binding}, err
	}
	if !created {
		return allocationTxResult{attempted: &binding},
			errors.New("admission creation event was not written")
	}
	o.result, o.classification = OutcomeAllocated, decision.Classification
	o.bindingID, o.nonce, o.registrySeq = binding.BindingID, binding.Nonce, binding.RegistrySeq
	return allocationTxResult{binding: &binding, obs: o}, nil
}

// bindingMatchesInput is the full R3 input-equality set: intent identity is
// the lookup key, so equality compares chain, sender and authorization id.
// The derived authorization_version and the candidate nonce are not inputs.
func bindingMatchesInput(b *Binding, req AllocationRequest) bool {
	return b.ChainID == req.ChainID &&
		b.Sender == req.Sender &&
		b.AuthorizationID == req.AuthorizationID
}

// registryRefusalReason renders the admission refusal cause.
func registryRefusalReason(registry *WalletRegistry) string {
	if registry == nil {
		return "sender is not in nonce_wallet_registry"
	}
	return "sender registry state is " + registry.State
}

// holdCauseList renders the active hold causes in stable row order.
func holdCauseList(holds []ScopeHold) []string {
	causes := make([]string, 0, len(holds))
	for i := range holds {
		causes = append(causes, holds[i].Cause)
	}
	return causes
}

// scopeLastPending and scopeFloor read the durable frontier defensively: the
// scope row exists (ensureScopeRowTx) but a read miss must not panic.
func scopeLastPending(s *ScopeState) *big.Int {
	if s == nil {
		return nil
	}
	return s.LastPending
}

func scopeFloor(s *ScopeState) *big.Int {
	if s == nil {
		return nil
	}
	return s.ReconciledFloor
}

// newBindingID mints an opaque binding id ("nb-" + 32 hex); a mint failure
// refuses the admission rather than inventing a colliding identity.
func newBindingID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("mint binding id: %w", err)
	}
	return "nb-" + hex.EncodeToString(buf[:]), nil
}

// authorizationBindingRow is the read-only 007 row as validated under the
// share lock. State/chain/expiry are enforced by the FOR SHARE statement's
// WHERE clause; the remaining fields feed the version digest.
type authorizationBindingRow struct {
	ChainID   int64
	Asset     string
	Recipient string
	Amount    string // canonical decimal (amount::text)
	State     string // always 'active' for returned rows
	ExpiresAt *time.Time
}

// readAuthorizationForShareSQL takes the 007 row lock and re-evaluates
// validity under it (READ COMMITTED: a revoke that commits while we wait for
// the lock drops the row out of the predicate and yields ErrNoRows, i.e. a
// refusal). clock_timestamp() is the DB clock taken after the lock wait.
const readAuthorizationForShareSQL = `
SELECT chain_id, asset, recipient, amount::text, state, expires_at
FROM withdrawal_authorizations
WHERE authorization_id = $1
  AND chain_id = $2
  AND state = 'active'
  AND (expires_at IS NULL OR expires_at > clock_timestamp())
FOR SHARE`

// readAuthorizationForShareTx reads and locks the authorization row; a miss,
// inactive/expired row, or chain mismatch yields (nil, nil) — the caller
// refuses authorization_invalid with zero writes.
func readAuthorizationForShareTx(ctx context.Context, q txQuerier, authorizationID string, chainID int64) (*authorizationBindingRow, error) {
	var row authorizationBindingRow
	err := q.QueryRow(ctx, readAuthorizationForShareSQL, authorizationID, chainID).Scan(
		&row.ChainID, &row.Asset, &row.Recipient, &row.Amount, &row.State, &row.ExpiresAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read authorization FOR SHARE: %w", err)
	}
	return &row, nil
}

// authorizationVersionDigest is the authorization_version recorded on the
// binding (R10): SHA-256 over the authorization's binding-relevant fields in
// the documented order, newline-separated and domain-tagged v1. The output is
// 64 lowercase hex (the nonce_bindings_authorization_version_check domain).
func authorizationVersionDigest(row *authorizationBindingRow, authorizationID string) string {
	expires := ""
	if row.ExpiresAt != nil {
		expires = row.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	parts := []string{
		"txharbor:nonce:authorization:v1",
		authorizationID,
		strconv.FormatInt(row.ChainID, 10),
		row.Asset,
		row.Recipient,
		row.Amount,
		row.State,
		expires,
	}
	h := sha256.New()
	for _, part := range parts {
		_, _ = io.WriteString(h, part)
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// bindingCarrierConstraint reports whether err is a 23505 raised by one of
// the binding UNIQUE carriers. Exact ConstraintName match only: any other
// 23505 (or any other error) is never classified into replay/conflict.
func bindingCarrierConstraint(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	switch pgErr.ConstraintName {
	case "nonce_bindings_pkey",
		"nonce_bindings_intent_uniq",
		"nonce_bindings_scope_nonce_uniq":
		return true
	}
	return false
}

// convergeAfterRace runs the T-converge classify off the aborted transaction:
// a fresh read-only transaction (the pooled connection was rolled back and
// carries no state), never a scan of the failed tx.
func (a *Allocator) convergeAfterRace(ctx context.Context, req AllocationRequest, attempted *big.Int) (*Binding, Outcome, error) {
	tx, err := a.begin(ctx)
	if err != nil {
		return nil, OutcomeTemporarilyUnavailable, storageRefusal("open race classify transaction", err)
	}
	binding, refuse, err := resolveAdmissionRace(ctx, tx, req, attempted)
	_ = tx.Rollback(ctx) // read-only classify; never commits
	if err != nil {
		return nil, OutcomeTemporarilyUnavailable, storageRefusal("race classify read failed", err)
	}
	if refuse != nil {
		return nil, refuse.Outcome, refuse
	}
	return binding, OutcomeReplayed, nil
}

// resolveAdmissionRace is the only allowed post-23505 diagnostic: fixed order
// (1) by intent_id (hit + equality → the winner is the replay; hit + differ →
// conflict), (2) by (chain_id, sender, nonce) (hit → internal serialization
// failure; the winner belongs to another intent and is never adopted), (3)
// miss → retryable. A classify miss is never fabricated into a binding.
func resolveAdmissionRace(ctx context.Context, q txQuerier, req AllocationRequest, attempted *big.Int) (*Binding, *Error, error) {
	winner, err := readBindingByIntentTx(ctx, q, req.IntentID)
	if err != nil {
		return nil, nil, err
	}
	if winner != nil {
		if bindingMatchesInput(winner, req) {
			return winner, nil, nil
		}
		return nil, Refuse(OutcomeAllocationConflict,
			"intent already bound to a differing input set"), nil
	}
	if attempted == nil {
		return nil, Refuse(OutcomeTemporarilyUnavailable,
			"allocation race classify miss"), nil
	}
	other, err := readBindingByScopeNonceTx(ctx, q, req.ChainID, req.Sender, attempted)
	if err != nil {
		return nil, nil, err
	}
	if other != nil {
		return nil, Refuse(OutcomeTemporarilyUnavailable,
			"scope nonce carrier raced; retry with the same input set"), nil
	}
	return nil, Refuse(OutcomeTemporarilyUnavailable,
		"allocation race classify miss; retry with the same input set"), nil
}

// attemptedNonce exposes the raced candidate for T-converge step 2.
func attemptedNonce(b *Binding) *big.Int {
	if b == nil {
		return nil
	}
	return b.Nonce
}

// storageRefusal maps a storage/commit failure to the retryable outcome,
// carrying the cause for the caller's logs (never secrets).
func storageRefusal(reason string, cause error) *Error {
	return Refuse(OutcomeTemporarilyUnavailable, reason).Wrap(cause)
}
