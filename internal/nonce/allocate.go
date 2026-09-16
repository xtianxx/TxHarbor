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
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
}

// NewAllocator wires the pooled transaction opener and the pre-tx observer
// (T017 constructs it with the serve pool and a real RPC observer).
func NewAllocator(pool *pgxpool.Pool, observer *Observer) *Allocator {
	return &Allocator{
		observer: observer,
		begin:    func(ctx context.Context) (allocTx, error) { return pool.Begin(ctx) },
	}
}

// Allocate is the T-allocate entry point: it returns the volatile binding
// with OutcomeAllocated, the untouched original with OutcomeReplayed, or a
// classified *Error refusal (outcome duplicated as the returned Outcome).
// Every admission attempt persists its observation when classification was
// reached; RPC runs strictly before BEGIN.
func (a *Allocator) Allocate(ctx context.Context, req AllocationRequest) (*Binding, Outcome, error) {
	if err := req.Validate(); err != nil {
		return nil, "", err
	}

	// R4: the chain view is sampled once, before the transaction opens.
	observation := a.observer.Observe(ctx, req.ChainID, req.Sender, ObservationKindAllocation)

	tx, err := a.begin(ctx)
	if err != nil {
		return nil, OutcomeTemporarilyUnavailable, storageRefusal("open allocation transaction", err)
	}

	res, err := allocateInTx(ctx, tx, req, observation)
	switch {
	case err != nil:
		_ = tx.Rollback(ctx)
		if bindingCarrierConstraint(err) {
			return a.convergeAfterRace(ctx, req, attemptedNonce(res.attempted))
		}
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
			return nil, OutcomeTemporarilyUnavailable, storageRefusal("commit classification refusal", cerr)
		}
		return nil, res.refuse.Outcome, res.refuse
	default:
		if cerr := tx.Commit(ctx); cerr != nil {
			// Commit-unknown: retry with the same input set converges through
			// the UNIQUE carriers (T-converge).
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
}

// allocateInTx runs the T-allocate steps inside the caller's transaction.
func allocateInTx(ctx context.Context, tx txQuerier, req AllocationRequest, obs Observation) (allocationTxResult, error) {
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
		return allocationTxResult{refuse: Refuse(OutcomeRecoveryActive,
			"006 gate active: "+strings.Join(gates.Causes(), ","))}, nil
	}

	// Registry recheck (OC-2): must precede scope-row creation (FK carrier).
	registry, err := readRegistryBySenderTx(ctx, tx, req.ChainID, req.Sender)
	if err != nil {
		return allocationTxResult{}, err
	}
	if refusal := registry.AdmissionRefusal(); refusal != "" {
		return allocationTxResult{refuse: Refuse(refusal, registryRefusalReason(registry))}, nil
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
		return allocationTxResult{refuse: Refuse(OutcomeScopeHeld,
			"active holds: "+strings.Join(holdCauseList(holds), ","))}, nil
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
		return allocationTxResult{refuse: Refuse(OutcomeAuthorizationInvalid,
			"authorization is missing, inactive, expired, or chain-mismatched")}, nil
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
			return allocationTxResult{
				binding: existing,
				refuse:  Refuse(OutcomeReplayed, "allocation input equality"),
			}, nil
		}
		return allocationTxResult{refuse: Refuse(OutcomeAllocationConflict,
			"intent already bound to a differing input set")}, nil
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

	if decision.Candidate == nil {
		if decision.HoldCause != "" {
			if _, err := establishHoldTx(ctx, tx, HoldEstablishment{
				ChainID:       req.ChainID,
				Sender:        req.Sender,
				Cause:         decision.HoldCause,
				ObservationID: observationID,
				Detail:        decision.Classification,
			}); err != nil {
				return allocationTxResult{}, err
			}
		}
		return allocationTxResult{persist: true,
			refuse: Refuse(decision.Refusal, decision.Classification)}, nil
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
	return allocationTxResult{binding: &binding}, nil
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
