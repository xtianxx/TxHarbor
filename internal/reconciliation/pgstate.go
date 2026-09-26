// pgstate.go implements T014: the read-only PostgreSQL-state adapter for 014
// reconciliation (FR-001–FR-006, FR-018, FR-021; data-model.md §1/§4;
// research.md §1/§2).
//
// The adapter materializes the PG side of the three-way compare: the
// authoritative 007 request/grant, 010 authorization-scope carrier, 011
// attempt/signing/send/reconciliation/receipt facts, and 012
// intent/claim/step state — nothing else. It is strictly read-only:
//
//   - it never writes, never begins a transaction, and never holds a
//     connection across a slow call. Every query is a single statement (no
//     FOR UPDATE/FOR SHARE gate semantics, no LOCK TABLE): 014 never enables a
//     send, so there is no send gate to linearize against, and a reconciliation
//     scan must not add lock latency to funds flows (FR-025);
//   - it never calls txlifecycle.Reconcile (which appends observations and
//     applies state transitions) and never calls any recovery/replay/payment
//     entrypoint (FR-014/FR-015/FR-023).
//
// Read-only reuse:
//
//   - txlifecycle.UnknownRecovery (reconcile.go:183) supplies the current
//     attempt's persisted facts (signed-bytes presence, dispatch facts,
//     append-only observations, last gate basis, freeze state). Its recovery
//     conditions are recorded as evidence text only; they are never a
//     permission and trigger nothing.
//   - txlifecycle.Reconcile's classification vocabulary (reconcile.go:62/:166)
//     is preserved verbatim: found_pending / included / not_found_yet /
//     unavailable, and not_found_yet is never read as a failure verdict.
//   - execution.RecoverySnapshotSQL (gates.go) supplies the 006 recovery
//     version / events high-water for the version domain. 014 takes no gate
//     SHARE lock; the single statement is only a consistent observation.
//
// Hard rules encoded here:
//
//   - unknown stays unknown (FR-018): an attempt in the 011 'unknown' state, a
//     non-final pipeline state (prepared/signed/sent), or a last observation
//     'unavailable' is reported as PGStateUnknown — never success, never
//     failure, never a re-pay input.
//   - unknown/stale/unreachable/incomplete evidence never permits a
//     "consistent" conclusion: PGStateStatus.PermitsConsistency is true only
//     for a fully read, unaffected record; PGStateStatus.Pending marks the
//     conservative cases.
//   - source, scope, version, and freshness are recorded on every read
//     (PGStateRecord.Sources/ChainID/BusinessType/BusinessKey/Version/
//     Freshness). Freshness age is judged only against an explicitly
//     configured window; no threshold is invented here (research §7).
//   - money stays integer/decimal (NUMERIC(78,0) -> big.Int); no float ever
//     touches a monetary value.
//
// The canonical PG-party bytes for identity content hashing are
// PGStateRecord.CanonicalBytes: deterministic, business-state only (read
// metadata such as Status/Reason/ReadAt is deliberately excluded so the same
// state read twice yields the same hash and dedup keeps working).
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
)

// DefaultPGMaxAttempts bounds the attempt history returned per read when no
// explicit bound is configured (FR-019: reads are bounded; no unbounded work).
const DefaultPGMaxAttempts = 64

// PGStateQueryer is the read-only pgx surface the adapter needs; *pgxpool.Pool
// satisfies it. pgx.Tx also satisfies it structurally, but callers MUST NOT
// hand the adapter a transaction held across slow work: the adapter itself
// never opens a transaction and every statement is standalone.
type PGStateQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PGAttemptFactReader is the read-only 011 attempt-facts surface. It is
// satisfied by *txlifecycle.Store. 014 deliberately depends on the smallest
// read-only slice of that store: UnknownRecovery returns persisted facts plus
// recovery conditions, never a permission and never a rebuild.
type PGAttemptFactReader interface {
	UnknownRecovery(ctx context.Context, attemptID, txHash string) (*txlifecycle.UnknownRecovery, error)
}

// PGSource names one authoritative PostgreSQL source read as evidence. The
// values are stable table/query tokens (audit vocabulary).
type PGSource string

// The recognized sources.
const (
	PGSourceWithdrawalRequest       PGSource = "withdrawal_requests"
	PGSourceWithdrawalAuthorization PGSource = "withdrawal_authorizations"
	PGSourceWithdrawalAuthScope     PGSource = "withdrawal_authorization_scopes"
	PGSourcePaymentIntent           PGSource = "payment_intents"
	PGSourceExecutionClaim          PGSource = "execution_claims"
	PGSourceExecutionStep           PGSource = "execution_steps"
	PGSourceTxAttempt               PGSource = "tx_attempts"
	PGSourceTxSigning               PGSource = "tx_attempt_signings"
	PGSourceTxSendAttempt           PGSource = "tx_send_attempts"
	PGSourceTxReconciliation        PGSource = "tx_reconciliations"
	PGSourceTxReceipt               PGSource = "tx_receipts"
	PGSourceRecoveryGate            PGSource = "recovery_snapshot"
	// PGSourceTxHash is the chain-anchored lookup token of a tx-hash keyed
	// read: the hash resolves through tx_attempt_signings/tx_receipts to the
	// attempt graph, and "no row" is the definitive absence of a business
	// record for the chain fact (US1 missing detection).
	PGSourceTxHash PGSource = "tx_hash"
)

// PGStateStatus is the conservative availability verdict of one PG-state read.
// Only PGStateComplete can support a "consistent" conclusion; PGStateAbsent is
// a definitive absence (usable for a missing-record compare); unknown, stale,
// incomplete, and unreachable are pending/insufficient and MUST NOT be rendered
// as consistent (FR-004/FR-005/FR-018).
type PGStateStatus string

// The recognized statuses.
const (
	// PGStateComplete: the authoritative rows were read successfully and no
	// uncertainty or invalidation signal affects the compared object.
	PGStateComplete PGStateStatus = "complete"
	// PGStateAbsent: the authoritative answer is "no such business row". This
	// is a definitive read, not a consistency verdict.
	PGStateAbsent PGStateStatus = "absent"
	// PGStateUnknown: the authoritative state itself is unresolved (011
	// attempt 'unknown', non-final pipeline state, last observation
	// 'unavailable'); it is preserved as unknown, never success/failure.
	PGStateUnknown PGStateStatus = "unknown"
	// PGStateStale: observed evidence was invalidated (orphaned attempt or
	// receipt) or is older than the configured freshness window.
	PGStateStale PGStateStatus = "stale"
	// PGStateIncomplete: rows exist but the evidence package is partial
	// (missing linkage, malformed fields, unwired readers).
	PGStateIncomplete PGStateStatus = "incomplete"
	// PGStateUnreachable: the read failed; nothing is known.
	PGStateUnreachable PGStateStatus = "unreachable"
)

// Valid reports whether s is one of the closed statuses.
func (s PGStateStatus) Valid() bool {
	switch s {
	case PGStateComplete, PGStateAbsent, PGStateUnknown, PGStateStale,
		PGStateIncomplete, PGStateUnreachable:
		return true
	}
	return false
}

// Definitive reports whether the read itself is a definitive answer (complete
// state or authoritative absence) as opposed to pending/insufficient evidence.
func (s PGStateStatus) Definitive() bool {
	return s == PGStateComplete || s == PGStateAbsent
}

// PermitsConsistency reports whether this read may feed a "consistent"
// conclusion. Only a complete read qualifies; unknown/stale/unreachable/
// incomplete and even absent (which is a discrepancy input) do not.
func (s PGStateStatus) PermitsConsistency() bool { return s == PGStateComplete }

// Pending reports whether the status is a conservative non-conclusion that
// must be carried as pending/incomplete (FR-004/FR-005/FR-018).
func (s PGStateStatus) Pending() bool {
	switch s {
	case PGStateUnknown, PGStateStale, PGStateIncomplete, PGStateUnreachable:
		return true
	}
	return false
}

// pgStatusRank orders statuses for conservative downgrade: a read can only
// move toward the stronger non-conclusion, never back.
func pgStatusRank(s PGStateStatus) int {
	switch s {
	case PGStateUnreachable:
		return 6
	case PGStateIncomplete:
		return 5
	case PGStateStale:
		return 4
	case PGStateUnknown:
		return 3
	case PGStateAbsent:
		return 2
	case PGStateComplete:
		return 1
	}
	return 0
}

// PGStateReason is the stable machine reason of a non-complete status (empty
// for complete/absent). It is evidence vocabulary and MUST NOT be reworded.
type PGStateReason string

// The refusal/downgrade reasons.
const (
	PGReasonReadFailed              PGStateReason = "read_failed"
	PGReasonChainMismatch           PGStateReason = "chain_mismatch"
	PGReasonMalformedEvidence       PGStateReason = "malformed_evidence"
	PGReasonLinkedRowMissing        PGStateReason = "linked_row_missing"
	PGReasonScopeMissing            PGStateReason = "scope_missing"
	PGReasonEvidenceInconsistent    PGStateReason = "evidence_inconsistent"
	PGReasonGateContextUnavailable  PGStateReason = "gate_context_unavailable"
	PGReasonAttemptStateUnknown     PGStateReason = "attempt_state_unknown"
	PGReasonOutcomeNotFinal         PGStateReason = "outcome_not_final"
	PGReasonAttemptOrphaned         PGStateReason = "attempt_orphaned"
	PGReasonReceiptMissing          PGStateReason = "receipt_missing"
	PGReasonReceiptUnverified       PGStateReason = "receipt_unverified"
	PGReasonReceiptOrphaned         PGStateReason = "receipt_orphaned"
	PGReasonObservationUnavailable  PGStateReason = "observation_unavailable"
	PGReasonStateEffectInconsistent PGStateReason = "state_effect_inconsistent"
	PGReasonUnknownAttemptState     PGStateReason = "unknown_attempt_state"
	PGReasonFreshnessExceeded       PGStateReason = "freshness_exceeded"
)

// 011 tx_attempts.state vocabulary (migrations/000011_tx_lifecycle.sql). Read
// verbatim; values outside the set are refused conservatively.
const (
	pgAttemptStatePrepared    = "prepared"
	pgAttemptStateSigned      = "signed"
	pgAttemptStateSent        = "sent"
	pgAttemptStateUnknown     = "unknown"
	pgAttemptStateEffective   = "effective"
	pgAttemptStateIneffective = "ineffective"
	pgAttemptStateConfirmed   = "confirmed"
	pgAttemptStateOrphaned    = "orphaned"
	pgAttemptStateReplaced    = "replaced"
)

// tx_reconciliations.classification vocabulary (reconcile.go:145-162). The
// values are preserved verbatim; not_found_yet is never a failure verdict.
const (
	pgObservationFoundPending = "found_pending"
	pgObservationIncluded     = "included"
	pgObservationNotFoundYet  = "not_found_yet"
	pgObservationUnavailable  = "unavailable"
)

// tx_receipts.canonicality / effect vocabulary.
const (
	pgCanonicalityUnverified = "unverified"
	pgCanonicalityCanonical  = "canonical"
	pgCanonicalityOrphaned   = "orphaned"
	pgReceiptEffectEffective = "effective"
)

// PGReadRequest identifies one PG business object to materialize. ChainID is
// the scope chain and is always required (FR-001); every row read is checked
// against it and a mismatch downgrades to incomplete.
type PGReadRequest struct {
	ChainID      int64
	BusinessType BusinessType
	Key          BusinessKey
}

// validate refuses unknown shapes fail-closed.
func (r PGReadRequest) validate() error {
	if r.ChainID <= 0 {
		return contractErrorf("pg-state read requires a positive chain_id, got %d", r.ChainID)
	}
	if !r.BusinessType.Known() {
		return contractErrorf("pg-state read has unknown business type %q", r.BusinessType)
	}
	if err := r.Key.Validate(); err != nil {
		return err
	}
	switch r.Key.Kind {
	case BusinessKeyRequestID, BusinessKeyIntentID, BusinessKeyAttemptID:
		return nil
	case BusinessKeyTxHash:
		// The chain-anchored key resolves the tx hash to the authoritative
		// attempt rows; it never fabricates an attempt identity (a missing
		// row is reported as absence, not as an invented attempt id).
		return nil
	}
	return contractErrorf("business key kind %q has no PG-state source", r.Key.Kind)
}

// PGWithdrawalRequest is the authoritative 007 request row.
type PGWithdrawalRequest struct {
	RequestID       string
	CallerID        int64
	AuthorizationID string
	ChainID         int64
	Asset           string
	Recipient       string
	AmountText      string
	Amount          *big.Int
	Status          string
	CreatedAt       time.Time
}

// PGAuthorization is the authoritative 007 grant row. Unexpired is decided by
// the database clock in the read statement, never by the application clock.
type PGAuthorization struct {
	AuthorizationID string
	CallerID        int64
	ChainID         int64
	Asset           string
	Recipient       string
	AmountText      string
	Amount          *big.Int
	State           string
	ExpiresAt       *time.Time
	Unexpired       bool
	SuppliedAt      time.Time
}

// PGAuthScope is the 010 authorization-scope carrier row.
type PGAuthScope struct {
	AuthorizationID      string
	IntentID             string
	RequestID            string
	Sender               string
	FeeMaxTotal          int64
	FeeMaxPerGas         int64
	FeeMaxPriority       int64
	AllowsFeeReplacement bool
	AuthorizationVersion int64
}

// PGIntent is the authoritative 012 payment_intents row.
type PGIntent struct {
	IntentID                string
	RequestID               string
	ChainID                 int64
	Sender                  string
	AuthorizationID         string
	AuthorizationVersion    int64
	State                   string
	StateVersion            int64
	AdmittedRecoveryVersion int64
	AdmittedAt              time.Time
	UpdatedAt               time.Time
}

// PGExecutionClaim is the 012 execution_claims row.
type PGExecutionClaim struct {
	IntentID        string
	OwnerID         string
	LeaseVersion    int64
	State           string
	ExpiresAt       time.Time
	Active          bool
	LastHeartbeatAt time.Time
	LastProgressAt  time.Time
	UpdatedAt       time.Time
}

// PGExecutionStep is the open (issued) 012 execution step, if any.
type PGExecutionStep struct {
	StepID          string
	IntentID        string
	Action          string
	State           string
	OwnerID         string
	LeaseVersion    int64
	AttemptID       string
	TxHash          string
	OutcomeClass    string
	RecoveryVersion *int64
	IssuedAt        time.Time
	UpdatedAt       time.Time
}

// PGAttempt is one 011 tx_attempts row. The fields below the row set describe
// the current attempt only (rec.Attempt); attempts in rec.Attempts are row
// summaries without detail.
type PGAttempt struct {
	AttemptID            string
	IntentID             string
	AuthorizationID      string
	AuthorizationVersion int64
	RecoveryVersion      int64
	ChainID              int64
	Sender               string
	Asset                string
	Recipient            string
	AmountText           string
	Amount               *big.Int
	ContentHash          string
	State                string
	RevisionSeq          int64
	TxHash               string
	EffectiveAt          *time.Time
	ConfirmedAt          *time.Time
	OrphanedAt           *time.Time
	ReplacedAt           *time.Time
	UpdatedAt            time.Time

	// Detail of the current attempt (txlifecycle.UnknownRecovery facts plus the
	// latest receipt). RecoveryConditions is advisory evidence text; it is
	// never a permission and never triggers recovery.
	SignedBytesPresent bool
	Frozen             bool
	LastGateBasis      txlifecycle.GateSnapshot
	LastReceiptEffect  string
	RecoveryConditions []string
	Dispatches         []txlifecycle.SendFact
	Observations       []txlifecycle.ReconcileFact
	Receipt            *PGReceipt
}

// PGReceipt is one tx_receipts row (the latest per attempt).
type PGReceipt struct {
	TxHash            string
	Status            int
	BlockNumber       int64
	BlockHash         string
	Effect            string
	Canonicality      string
	ConfirmationsText string
	Confirmations     int64
	ConfirmThreshold  int64
	ConfirmPolicySeq  int64
	ConfirmTipNumber  *int64
	ConfirmTipHash    string
	ObservedAt        time.Time
	UpdatedAt         time.Time
	ConfirmedAt       *time.Time
	OrphanedAt        *time.Time
}

// PGGateContext is the read-only 006 gate snapshot observed with the read
// (execution.RecoverySnapshotSQL; no SHARE lock). It contributes the recovery
// version to the evidence version domain and pause context; it is never a
// permission to act.
type PGGateContext struct {
	Observed      bool
	IndexerPaused bool
	LogPaused     bool
	DepositPaused bool
	HasRecovery   bool
	RecoveryPhase string
	RecoverySeq   int64
	HasEventsMax  bool
	EventsMax     int64
}

// CurrentVersion mirrors execution.RecoveryGate.CurrentVersion: active
// recovery_seq, else events MAX, else 0.
func (g PGGateContext) CurrentVersion() int64 {
	if g.HasRecovery {
		return g.RecoverySeq
	}
	if g.HasEventsMax {
		return g.EventsMax
	}
	return 0
}

// PGVersion is the PG-side version contribution to the identity evidence
// version domain (identity.go VersionDomain). Zero means "not carried by this
// object"; it never means "verified equal".
type PGVersion struct {
	AuthorizationVersion int64
	RecoveryVersion      int64
	StateVersion         int64
	LeaseVersion         int64
	RevisionSeq          int64
	ContentHash          string
}

// PGFreshness records the freshness of one read. Stale is only ever set from
// an explicitly configured window (no invented threshold) or a native
// invalidation signal (which is carried by Status/Reason).
type PGFreshness struct {
	Source           PGSource
	ReadAt           time.Time
	NewestObservedAt time.Time
	Window           time.Duration
	Stale            bool
	Reason           PGStateReason
}

// PGStateRecord is one business object's PG-side evidence package: the source
// list, scope, rows, version contribution, and freshness verdict. It is a
// read-only observation; it carries no write and no authorization.
type PGStateRecord struct {
	BusinessType BusinessType
	BusinessKey  BusinessKey
	ChainID      int64

	Sources []PGSource
	Status  PGStateStatus
	Reason  PGStateReason

	Request       *PGWithdrawalRequest
	Authorization *PGAuthorization
	AuthScope     *PGAuthScope
	Intent        *PGIntent
	Claim         *PGExecutionClaim
	OpenStep      *PGExecutionStep

	// Attempts are all attempt rows of the intent (ascending), bounded by the
	// adapter's MaxAttempts. Attempt is the current payment fact with detail:
	// the latest attempt of the intent, or the anchor attempt when the caller
	// asked by attempt_id.
	Attempts          []PGAttempt
	AttemptsTruncated bool
	Attempt           *PGAttempt

	Gate    PGGateContext
	Version PGVersion

	Freshness PGFreshness
	ReadAt    time.Time

	// Notes carries human-readable, secret-free evidence detail (audit only;
	// deliberately excluded from CanonicalBytes so derived text never churns
	// identity hashes).
	Notes []string

	newestObservedAt time.Time
}

// PGStateOptions configures the adapter. Concrete freshness windows are
// deployment/test parameters (research §7); the adapter never invents one.
type PGStateOptions struct {
	// FreshnessWindow downgrades an otherwise-complete read when the newest
	// authoritative timestamp is older than this window. Zero leaves age-based
	// downgrade off (read success and native signals still apply).
	FreshnessWindow time.Duration
	// MaxAttempts bounds the attempt history per read; zero means
	// DefaultPGMaxAttempts. The latest attempt is always inside the bound.
	MaxAttempts int
}

// PGStateAdapter is the read-only PG-state adapter. Create it with
// NewPGStateAdapter; the zero value and a nil adapter refuse every read.
type PGStateAdapter struct {
	db          PGStateQueryer
	facts       PGAttemptFactReader
	window      time.Duration
	maxAttempts int
}

// NewPGStateAdapter wires the adapter over db and the txlifecycle attempt-facts
// reader (pass txlifecycle.NewStore(pool)). Both are mandatory and a malformed
// configuration is refused fail-closed.
func NewPGStateAdapter(db PGStateQueryer, facts PGAttemptFactReader, opts PGStateOptions) (*PGStateAdapter, error) {
	if db == nil {
		return nil, contractErrorf("pg-state adapter requires a database")
	}
	if facts == nil {
		return nil, contractErrorf("pg-state adapter requires the txlifecycle attempt-facts reader")
	}
	if opts.FreshnessWindow < 0 {
		return nil, contractErrorf("freshness window must not be negative")
	}
	if opts.MaxAttempts < 0 {
		return nil, contractErrorf("max attempts must not be negative")
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultPGMaxAttempts
	}
	return &PGStateAdapter{db: db, facts: facts, window: opts.FreshnessWindow, maxAttempts: maxAttempts}, nil
}

// Read materializes the PG state for one business key. The returned error is
// non-nil only for an unreachable read (Status PGStateUnreachable); a
// conservative downgrade (unknown/stale/incomplete) is reported through
// Status/Reason with a nil error so callers persist the evidence and mark
// pending, never re-classify it as a definitive anomaly.
func (a *PGStateAdapter) Read(ctx context.Context, req PGReadRequest) (PGStateRecord, error) {
	if a == nil || a.db == nil {
		return PGStateRecord{}, contractErrorf("pg-state adapter is not wired")
	}
	if err := req.validate(); err != nil {
		return PGStateRecord{}, err
	}

	rec := &PGStateRecord{
		BusinessType: req.BusinessType,
		BusinessKey:  req.Key,
		ChainID:      req.ChainID,
		Status:       PGStateComplete,
	}
	readAt, err := a.readClock(ctx)
	if err != nil {
		rec.Status = PGStateUnreachable
		rec.Reason = PGReasonReadFailed
		rec.Freshness = PGFreshness{Source: rec.anchorSource(), Window: a.window}
		return *rec, err
	}
	rec.ReadAt = readAt

	if err := a.readGraph(ctx, rec); err != nil {
		rec.Freshness = PGFreshness{
			Source:           rec.anchorSource(),
			ReadAt:           rec.ReadAt,
			NewestObservedAt: rec.newestObservedAt,
			Window:           a.window,
		}
		return *rec, err
	}
	rec.crossCheck()
	if rec.Status != PGStateAbsent {
		evaluatePGAttempt(rec)
	}
	a.finalizeRecord(rec)
	return *rec, nil
}

// readGraph reads the anchor row(s) and the linked business graph. It returns
// an error only for an unreachable read and has then already marked the record
// PGStateUnreachable.
func (a *PGStateAdapter) readGraph(ctx context.Context, rec *PGStateRecord) error {
	switch rec.BusinessKey.Kind {
	case BusinessKeyRequestID:
		request, found, err := a.readRequest(ctx, rec, rec.BusinessKey.Value)
		if err != nil {
			return markPGUnreachable(rec, PGSourceWithdrawalRequest, err)
		}
		if !found {
			rec.Status = PGStateAbsent
			return nil
		}
		rec.Request = request
		rec.checkChain(PGSourceWithdrawalRequest, request.ChainID)
		intent, found, err := a.readIntentByRequest(ctx, rec, request.RequestID)
		if err != nil {
			return markPGUnreachable(rec, PGSourcePaymentIntent, err)
		}
		if found {
			rec.Intent = intent
		}
	case BusinessKeyIntentID:
		intent, found, err := a.readIntentByID(ctx, rec, rec.BusinessKey.Value)
		if err != nil {
			return markPGUnreachable(rec, PGSourcePaymentIntent, err)
		}
		if !found {
			rec.Status = PGStateAbsent
			return nil
		}
		rec.Intent = intent
		rec.checkChain(PGSourcePaymentIntent, intent.ChainID)
		request, found, err := a.readRequest(ctx, rec, intent.RequestID)
		if err != nil {
			return markPGUnreachable(rec, PGSourceWithdrawalRequest, err)
		}
		if found {
			rec.Request = request
			rec.checkChain(PGSourceWithdrawalRequest, request.ChainID)
		} else {
			rec.downgrade(PGStateIncomplete, PGReasonLinkedRowMissing)
			rec.note("intent %s references missing request %s", intent.IntentID, intent.RequestID)
		}
	case BusinessKeyAttemptID:
		if err := a.readAttemptAnchor(ctx, rec, rec.BusinessKey.Value); err != nil {
			return err
		}
	case BusinessKeyTxHash:
		// Chain-anchored read: resolve the tx hash to the authoritative
		// attempt (tx_attempt_signings/tx_receipts, read-only). No row is the
		// definitive absence of a business record for the chain fact; an
		// ambiguous resolution downgrades conservatively inside the helper.
		attemptID, found, err := a.readAttemptIDByTxHash(ctx, rec, rec.BusinessKey.Value)
		if err != nil {
			return markPGUnreachable(rec, PGSourceTxHash, err)
		}
		if !found {
			rec.Status = PGStateAbsent
			return nil
		}
		if err := a.readAttemptAnchor(ctx, rec, attemptID); err != nil {
			return err
		}
	}

	if err := a.readCommonGraph(ctx, rec); err != nil {
		return err
	}
	return nil
}

// readAttemptAnchor materializes the attempt graph of one attempt id: the
// attempt row plus its intent/request/authorization linkage. A missing attempt
// is the definitive absence of the anchor row.
func (a *PGStateAdapter) readAttemptAnchor(ctx context.Context, rec *PGStateRecord, attemptID string) error {
	attempt, found, err := a.readAttemptByID(ctx, rec, attemptID)
	if err != nil {
		return markPGUnreachable(rec, PGSourceTxAttempt, err)
	}
	if !found {
		// A direct attempt-id read that finds nothing is the definitive
		// absence of the anchor row. A resolution that already downgraded
		// (ambiguous tx-hash resolution) must stay incomplete, never absent.
		if rec.Status == PGStateComplete {
			rec.Status = PGStateAbsent
		} else {
			rec.downgrade(PGStateIncomplete, PGReasonLinkedRowMissing)
			rec.note("resolved attempt anchor row is missing")
		}
		return nil
	}
	rec.Attempt = attempt
	rec.checkChain(PGSourceTxAttempt, attempt.ChainID)
	intent, found, err := a.readIntentByID(ctx, rec, attempt.IntentID)
	if err != nil {
		return markPGUnreachable(rec, PGSourcePaymentIntent, err)
	}
	if found {
		rec.Intent = intent
		rec.checkChain(PGSourcePaymentIntent, intent.ChainID)
	} else {
		rec.downgrade(PGStateIncomplete, PGReasonLinkedRowMissing)
		rec.note("attempt %s references missing intent %s", attempt.AttemptID, attempt.IntentID)
	}
	if rec.Intent != nil {
		request, found, err := a.readRequest(ctx, rec, rec.Intent.RequestID)
		if err != nil {
			return markPGUnreachable(rec, PGSourceWithdrawalRequest, err)
		}
		if found {
			rec.Request = request
			rec.checkChain(PGSourceWithdrawalRequest, request.ChainID)
		} else {
			rec.downgrade(PGStateIncomplete, PGReasonLinkedRowMissing)
			rec.note("intent %s references missing request %s", rec.Intent.IntentID, rec.Intent.RequestID)
		}
	}
	return nil
}

// readCommonGraph reads the rows reachable from the anchor: authorization,
// scope carrier, claim, open step, attempt history, current-attempt detail,
// and the 006 gate context, then derives the version contribution.
func (a *PGStateAdapter) readCommonGraph(ctx context.Context, rec *PGStateRecord) error {
	authID := rec.authorizationID()
	if authID == "" {
		rec.downgrade(PGStateIncomplete, PGReasonLinkedRowMissing)
		rec.note("no authorization link resolved from the anchor")
	} else {
		authorization, found, err := a.readAuthorization(ctx, rec, authID)
		if err != nil {
			return markPGUnreachable(rec, PGSourceWithdrawalAuthorization, err)
		}
		if found {
			rec.Authorization = authorization
			rec.checkChain(PGSourceWithdrawalAuthorization, authorization.ChainID)
		} else {
			rec.downgrade(PGStateIncomplete, PGReasonLinkedRowMissing)
			rec.note("authorization %s is missing", authID)
		}

		scope, found, err := a.readAuthScope(ctx, rec, authID)
		if err != nil {
			return markPGUnreachable(rec, PGSourceWithdrawalAuthScope, err)
		}
		if found {
			rec.AuthScope = scope
		} else {
			rec.downgrade(PGStateIncomplete, PGReasonScopeMissing)
			rec.note("authorization %s has no 010 scope carrier", authID)
		}
	}

	if rec.Intent != nil {
		claim, found, err := a.readClaim(ctx, rec, rec.Intent.IntentID)
		if err != nil {
			return markPGUnreachable(rec, PGSourceExecutionClaim, err)
		}
		if found {
			rec.Claim = claim
		}
		step, found, err := a.readOpenStep(ctx, rec, rec.Intent.IntentID)
		if err != nil {
			return markPGUnreachable(rec, PGSourceExecutionStep, err)
		}
		if found {
			rec.OpenStep = step
		}
		if err := a.readAttempts(ctx, rec, rec.Intent.IntentID); err != nil {
			return markPGUnreachable(rec, PGSourceTxAttempt, err)
		}
		if rec.Attempt == nil && len(rec.Attempts) > 0 {
			latest := rec.Attempts[len(rec.Attempts)-1]
			rec.Attempt = &latest
		}
	}

	if rec.Attempt != nil {
		if err := a.readAttemptDetail(ctx, rec); err != nil {
			return markPGUnreachable(rec, PGSourceTxReconciliation, err)
		}
	}

	a.readGateContext(ctx, rec)
	rec.deriveVersion()
	return nil
}

// authorizationID resolves the authorization linkage from the richest anchor
// row present.
func (rec *PGStateRecord) authorizationID() string {
	if rec.Request != nil && rec.Request.AuthorizationID != "" {
		return rec.Request.AuthorizationID
	}
	if rec.Intent != nil && rec.Intent.AuthorizationID != "" {
		return rec.Intent.AuthorizationID
	}
	if rec.Attempt != nil {
		return rec.Attempt.AuthorizationID
	}
	return ""
}

// readClock reads the authoritative database clock so every read of one
// business object shares one observation instant.
func (a *PGStateAdapter) readClock(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := a.db.QueryRow(ctx, pgNowSQL).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	return now.UTC(), nil
}

// markPGUnreachable attributes a failed read, marks the record unreachable and
// returns the wrapped error.
func markPGUnreachable(rec *PGStateRecord, source PGSource, err error) error {
	rec.noteSource(source)
	rec.Status = PGStateUnreachable
	rec.Reason = PGReasonReadFailed
	rec.note("%s read failed: %v", source, err)
	return fmt.Errorf("pg-state read %s: %w", source, err)
}

// finalizeRecord computes the freshness report and applies the configured
// age-based downgrade. A zero window never downgrades; a native invalidation
// (orphaned attempt/receipt) is already carried by Status/Reason.
func (a *PGStateAdapter) finalizeRecord(rec *PGStateRecord) {
	stale := a.window > 0 && !rec.newestObservedAt.IsZero() &&
		rec.ReadAt.Sub(rec.newestObservedAt) > a.window
	freshness := PGFreshness{
		Source:           rec.anchorSource(),
		ReadAt:           rec.ReadAt,
		NewestObservedAt: rec.newestObservedAt,
		Window:           a.window,
	}
	if stale {
		freshness.Stale = true
		freshness.Reason = PGReasonFreshnessExceeded
		if rec.Status == PGStateComplete {
			rec.Status = PGStateStale
			rec.Reason = PGReasonFreshnessExceeded
		}
		rec.note("newest evidence %s is older than the configured freshness window %s",
			rec.newestObservedAt.UTC().Format(time.RFC3339Nano), a.window)
	}
	rec.Freshness = freshness
}

// anchorSource names the table the read was keyed by.
func (rec *PGStateRecord) anchorSource() PGSource {
	switch rec.BusinessKey.Kind {
	case BusinessKeyRequestID:
		return PGSourceWithdrawalRequest
	case BusinessKeyIntentID:
		return PGSourcePaymentIntent
	case BusinessKeyAttemptID:
		return PGSourceTxAttempt
	case BusinessKeyTxHash:
		return PGSourceTxHash
	}
	return ""
}

// noteSource records a source exactly once, in first-read order.
func (rec *PGStateRecord) noteSource(source PGSource) {
	if source == "" {
		return
	}
	if slices.Contains(rec.Sources, source) {
		return
	}
	rec.Sources = append(rec.Sources, source)
}

// note appends secret-free evidence detail for audit.
func (rec *PGStateRecord) note(format string, args ...any) {
	rec.Notes = append(rec.Notes, fmt.Sprintf(format, args...))
}

// trackObserved advances the freshness anchor.
func (rec *PGStateRecord) trackObserved(at time.Time) {
	if at.IsZero() {
		return
	}
	at = at.UTC()
	if at.After(rec.newestObservedAt) {
		rec.newestObservedAt = at
	}
}

// downgrade moves the record to the stronger non-conclusion (never back).
func (rec *PGStateRecord) downgrade(status PGStateStatus, reason PGStateReason) {
	if pgStatusRank(status) > pgStatusRank(rec.Status) {
		rec.Status = status
		rec.Reason = reason
	}
}

// checkChain flags a row whose chain_id escapes the requested scope.
func (rec *PGStateRecord) checkChain(source PGSource, chainID int64) {
	if chainID != rec.ChainID {
		rec.downgrade(PGStateIncomplete, PGReasonChainMismatch)
		rec.note("%s chain_id %d does not match scope chain_id %d", source, chainID, rec.ChainID)
	}
}

// crossCheck flags structural drift between the linked authoritative rows.
// Drift means the evidence cannot be trusted as one coherent state.
func (rec *PGStateRecord) crossCheck() {
	inconsistent := func(format string, args ...any) {
		rec.downgrade(PGStateIncomplete, PGReasonEvidenceInconsistent)
		rec.note(format, args...)
	}

	if rec.Request != nil && rec.Authorization != nil {
		if rec.Request.AuthorizationID != rec.Authorization.AuthorizationID {
			inconsistent("request %s links authorization %s but read %s",
				rec.Request.RequestID, rec.Request.AuthorizationID, rec.Authorization.AuthorizationID)
		}
		if rec.Request.CallerID != rec.Authorization.CallerID {
			inconsistent("request caller %d != authorization caller %d", rec.Request.CallerID, rec.Authorization.CallerID)
		}
		if !strings.EqualFold(rec.Request.Asset, rec.Authorization.Asset) ||
			!strings.EqualFold(rec.Request.Recipient, rec.Authorization.Recipient) {
			inconsistent("request asset/recipient != authorization asset/recipient")
		}
		if a, b := rec.Request.Amount, rec.Authorization.Amount; a != nil && b != nil && a.Cmp(b) != 0 {
			inconsistent("request amount %s != authorization amount %s", a, b)
		}
	}
	if rec.Intent != nil {
		if rec.Request != nil {
			if rec.Intent.RequestID != rec.Request.RequestID {
				inconsistent("intent request %s != anchor request %s", rec.Intent.RequestID, rec.Request.RequestID)
			}
			if rec.Intent.AuthorizationID != rec.Request.AuthorizationID {
				inconsistent("intent authorization %s != request authorization %s",
					rec.Intent.AuthorizationID, rec.Request.AuthorizationID)
			}
		}
		if rec.AuthScope != nil {
			if rec.AuthScope.IntentID != rec.Intent.IntentID {
				inconsistent("scope intent %s != intent %s", rec.AuthScope.IntentID, rec.Intent.IntentID)
			}
			if rec.AuthScope.RequestID != rec.Intent.RequestID {
				inconsistent("scope request %s != intent request %s", rec.AuthScope.RequestID, rec.Intent.RequestID)
			}
			if rec.AuthScope.AuthorizationVersion != rec.Intent.AuthorizationVersion {
				inconsistent("scope authorization version %d != intent authorization version %d",
					rec.AuthScope.AuthorizationVersion, rec.Intent.AuthorizationVersion)
			}
		}
		if rec.Attempt != nil {
			if rec.Attempt.IntentID != rec.Intent.IntentID {
				inconsistent("attempt intent %s != intent %s", rec.Attempt.IntentID, rec.Intent.IntentID)
			}
			if rec.Attempt.AuthorizationID != rec.Intent.AuthorizationID {
				inconsistent("attempt authorization %s != intent authorization %s",
					rec.Attempt.AuthorizationID, rec.Intent.AuthorizationID)
			}
		}
	}
}

// evaluatePGAttempt applies the FR-018 conservative outcome rules for the
// current attempt. Unknown stays unknown; nothing here is a success/failure
// verdict and nothing triggers recovery or payment.
func evaluatePGAttempt(rec *PGStateRecord) {
	// Intent-level finality: only the 012 terminal states are conclusions;
	// in-flight states (and any unknown value) cannot support a consistent
	// verdict and never imply the payment failed.
	if rec.Intent != nil {
		switch {
		case !execution.ValidIntentState(rec.Intent.State):
			rec.downgrade(PGStateIncomplete, PGReasonMalformedEvidence)
			rec.note("intent %s has unknown state %q", rec.Intent.IntentID, rec.Intent.State)
		case rec.Intent.State != execution.IntentCompleted && rec.Intent.State != execution.IntentFailed:
			rec.downgrade(PGStateUnknown, PGReasonOutcomeNotFinal)
			rec.note("intent %s state %q is not final", rec.Intent.IntentID, rec.Intent.State)
		}
	}

	att := rec.Attempt
	if att == nil {
		// No payment attempt yet: a definitive PG fact for the request.
		return
	}

	// (1) Receipt invalidation/uncertainty first: a receipt row can exist
	// while the attempt state has not caught up.
	if att.Receipt != nil {
		switch att.Receipt.Canonicality {
		case pgCanonicalityOrphaned:
			rec.downgrade(PGStateStale, PGReasonReceiptOrphaned)
		case pgCanonicalityUnverified:
			rec.downgrade(PGStateUnknown, PGReasonReceiptUnverified)
		case pgCanonicalityCanonical:
		default:
			rec.downgrade(PGStateIncomplete, PGReasonMalformedEvidence)
			rec.note("receipt %s has unknown canonicality %q", att.Receipt.TxHash, att.Receipt.Canonicality)
		}
	}

	// (2) The 011 durable state.
	switch att.State {
	case pgAttemptStateUnknown:
		rec.downgrade(PGStateUnknown, PGReasonAttemptStateUnknown)
	case pgAttemptStatePrepared, pgAttemptStateSigned, pgAttemptStateSent:
		rec.downgrade(PGStateUnknown, PGReasonOutcomeNotFinal)
	case pgAttemptStateOrphaned:
		rec.downgrade(PGStateStale, PGReasonAttemptOrphaned)
	case pgAttemptStateReplaced:
		// A replaced attempt is a definitive "never included" fact; the
		// payment outcome belongs to its successor and no downgrade applies.
	case pgAttemptStateEffective, pgAttemptStateConfirmed, pgAttemptStateIneffective:
		if att.Receipt == nil {
			rec.downgrade(PGStateIncomplete, PGReasonReceiptMissing)
			rec.note("attempt %s state %s has no receipt row", att.AttemptID, att.State)
		} else if att.Receipt.Canonicality == pgCanonicalityCanonical {
			checkPGStateEffectConsistency(rec, att)
		}
	default:
		rec.downgrade(PGStateIncomplete, PGReasonUnknownAttemptState)
		rec.note("attempt %s has unknown state %q", att.AttemptID, att.State)
	}

	// (3) The last append-only observation is preserved verbatim
	// (reconcile.go:62/:166 vocabulary); not_found_yet is never a failure
	// verdict, and an unavailable chain view leaves the outcome unknown.
	if n := len(att.Observations); n > 0 {
		switch last := att.Observations[n-1].Classification; last {
		case pgObservationUnavailable:
			rec.downgrade(PGStateUnknown, PGReasonObservationUnavailable)
		case pgObservationNotFoundYet:
			rec.note("last recorded observation is not_found_yet; never a failure verdict")
		case pgObservationFoundPending:
			rec.note("last recorded observation is found_pending; outcome not final")
		case pgObservationIncluded:
			rec.note("last recorded observation is included")
		default:
			rec.note("last recorded observation has unknown classification %q", last)
		}
	}
}

// checkPGStateEffectConsistency flags drift between the durable attempt state
// and its canonical receipt.
func checkPGStateEffectConsistency(rec *PGStateRecord, att *PGAttempt) {
	effective := att.Receipt.Effect == pgReceiptEffectEffective
	switch att.State {
	case pgAttemptStateEffective, pgAttemptStateConfirmed:
		if !effective || att.Receipt.Status != 1 {
			rec.downgrade(PGStateIncomplete, PGReasonStateEffectInconsistent)
			rec.note("attempt %s state %s disagrees with receipt effect %s/status %d",
				att.AttemptID, att.State, att.Receipt.Effect, att.Receipt.Status)
		}
	case pgAttemptStateIneffective:
		if effective {
			rec.downgrade(PGStateIncomplete, PGReasonStateEffectInconsistent)
			rec.note("attempt %s state ineffective disagrees with effective receipt", att.AttemptID)
		}
	}
}

// deriveVersion folds the row versions into the PG-side version contribution.
// Object-anchored versions win over the live gate snapshot; the gate version is
// the fallback so a scope without any attempt still carries recovery evidence.
func (rec *PGStateRecord) deriveVersion() {
	version := PGVersion{}
	if rec.AuthScope != nil {
		version.AuthorizationVersion = rec.AuthScope.AuthorizationVersion
	}
	if version.AuthorizationVersion == 0 && rec.Intent != nil {
		version.AuthorizationVersion = rec.Intent.AuthorizationVersion
	}
	if version.AuthorizationVersion == 0 && rec.Attempt != nil {
		version.AuthorizationVersion = rec.Attempt.AuthorizationVersion
	}
	if rec.Intent != nil {
		version.StateVersion = rec.Intent.StateVersion
		version.RecoveryVersion = rec.Intent.AdmittedRecoveryVersion
	}
	if version.RecoveryVersion == 0 && rec.Attempt != nil {
		version.RecoveryVersion = rec.Attempt.RecoveryVersion
	}
	if version.RecoveryVersion == 0 {
		version.RecoveryVersion = rec.Gate.CurrentVersion()
	}
	if rec.Claim != nil {
		version.LeaseVersion = rec.Claim.LeaseVersion
	} else if rec.OpenStep != nil {
		version.LeaseVersion = rec.OpenStep.LeaseVersion
	}
	if rec.Attempt != nil {
		version.RevisionSeq = rec.Attempt.RevisionSeq
		version.ContentHash = rec.Attempt.ContentHash
	}
	rec.Version = version
}

// ---------------------------------------------------------------------------
// Row reads. Every statement is standalone (no transaction, no gate lock) and
// mirrors the authoritative columns of its 007/010/011/012 source; the reads
// that have an execution/gates.go counterpart are marked.

const pgNowSQL = `SELECT now()`

// pgRequestReadSQL mirrors execution.RequestReadSQL's authoritative columns
// plus the authorization link and creation timestamp that evidence needs; no
// gate lock is taken because 014 never enables a send.
const pgRequestReadSQL = `
SELECT caller_id, authorization_id, chain_id, asset, recipient, amount::text, status, created_at
FROM withdrawal_requests
WHERE request_id = $1`

// pgAuthorizationReadSQL mirrors execution.GrantReadSQL plus supplied_at,
// without FOR SHARE (the DB clock still decides unexpired).
const pgAuthorizationReadSQL = `
SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at,
       (expires_at IS NULL OR expires_at > now()) AS unexpired,
       supplied_at
FROM withdrawal_authorizations
WHERE authorization_id = $1`

// pgScopeReadSQL mirrors execution.ScopeReadSQL without FOR SHARE.
const pgScopeReadSQL = `
SELECT authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
       fee_max_priority, allows_fee_replacement, authorization_version
FROM withdrawal_authorization_scopes
WHERE authorization_id = $1`

const pgIntentByRequestSQL = `
SELECT intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
       state, state_version, admitted_recovery_version, admitted_at, updated_at
FROM payment_intents
WHERE request_id = $1`

const pgIntentByIDSQL = `
SELECT intent_id, request_id, chain_id, sender, authorization_id, authorization_version,
       state, state_version, admitted_recovery_version, admitted_at, updated_at
FROM payment_intents
WHERE intent_id = $1`

const pgClaimReadSQL = `
SELECT owner_id, lease_version, state, expires_at,
       (state = 'active' AND expires_at > now()) AS active,
       last_heartbeat_at, last_progress_at, updated_at
FROM execution_claims
WHERE intent_id = $1`

const pgOpenStepReadSQL = `
SELECT step_id, action, state, owner_id, lease_version,
       COALESCE(attempt_id, ''), COALESCE(tx_hash, ''), COALESCE(outcome_class, ''),
       recovery_version, issued_at, updated_at
FROM execution_steps
WHERE intent_id = $1 AND state = 'issued'
ORDER BY issued_at DESC, step_id
LIMIT 1`

const pgAttemptColumns = `
a.attempt_id, a.intent_id, a.authorization_id, a.authorization_version, a.recovery_version,
a.chain_id, a.sender, a.asset, a.recipient, a.amount::text, a.content_hash, a.state,
a.revision_seq, COALESCE(s.tx_hash, ''),
a.effective_at, a.confirmed_at, a.orphaned_at, a.replaced_at, a.updated_at`

const pgAttemptsForIntentSQL = `
SELECT ` + pgAttemptColumns + `
FROM tx_attempts a
LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
WHERE a.intent_id = $1
ORDER BY a.created_at DESC, a.attempt_id DESC
LIMIT $2`

const pgAttemptByIDSQL = `
SELECT ` + pgAttemptColumns + `
FROM tx_attempts a
LEFT JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
WHERE a.attempt_id = $1`

// pgAttemptIDByTxHashSQL is the chain-anchored reverse lookup: the durable
// signing row (unique tx hash) and the receipt rows both bind a transaction
// hash to its attempt. Read-only; no row means no business record exists for
// the chain fact.
const pgAttemptIDByTxHashSQL = `
SELECT resolved.attempt_id
FROM (
    SELECT attempt_id FROM tx_attempt_signings WHERE tx_hash = $1
    UNION
    SELECT attempt_id FROM tx_receipts WHERE tx_hash = $1
) resolved
ORDER BY resolved.attempt_id
LIMIT 2`

const pgLatestReceiptSQL = `
SELECT tx_hash, status, block_number, block_hash, effect, canonicality,
       confirmations::text, confirm_threshold, confirm_policy_seq,
       confirm_tip_number, COALESCE(confirm_tip_hash, ''),
       observed_at, updated_at, confirmed_at, orphaned_at
FROM tx_receipts
WHERE attempt_id = $1
ORDER BY receipt_id DESC
LIMIT 1`

// readRequest loads one 007 request row.
func (a *PGStateAdapter) readRequest(ctx context.Context, rec *PGStateRecord, requestID string) (*PGWithdrawalRequest, bool, error) {
	rec.noteSource(PGSourceWithdrawalRequest)
	row := &PGWithdrawalRequest{RequestID: requestID}
	err := a.db.QueryRow(ctx, pgRequestReadSQL, requestID).Scan(
		&row.CallerID, &row.AuthorizationID, &row.ChainID, &row.Asset, &row.Recipient,
		&row.AmountText, &row.Status, &row.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	row.Amount = parsePGAmount(rec, "withdrawal_requests", requestID, row.AmountText)
	rec.trackObserved(row.CreatedAt)
	return row, true, nil
}

// readAuthorization loads one 007 grant row.
func (a *PGStateAdapter) readAuthorization(ctx context.Context, rec *PGStateRecord, authorizationID string) (*PGAuthorization, bool, error) {
	rec.noteSource(PGSourceWithdrawalAuthorization)
	row := &PGAuthorization{AuthorizationID: authorizationID}
	err := a.db.QueryRow(ctx, pgAuthorizationReadSQL, authorizationID).Scan(
		&row.CallerID, &row.ChainID, &row.Asset, &row.Recipient, &row.AmountText,
		&row.State, &row.ExpiresAt, &row.Unexpired, &row.SuppliedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	row.Amount = parsePGAmount(rec, "withdrawal_authorizations", authorizationID, row.AmountText)
	rec.trackObserved(row.SuppliedAt)
	return row, true, nil
}

// readAuthScope loads one 010 scope carrier row.
func (a *PGStateAdapter) readAuthScope(ctx context.Context, rec *PGStateRecord, authorizationID string) (*PGAuthScope, bool, error) {
	rec.noteSource(PGSourceWithdrawalAuthScope)
	row := &PGAuthScope{}
	err := a.db.QueryRow(ctx, pgScopeReadSQL, authorizationID).Scan(
		&row.AuthorizationID, &row.IntentID, &row.RequestID, &row.Sender,
		&row.FeeMaxTotal, &row.FeeMaxPerGas, &row.FeeMaxPriority,
		&row.AllowsFeeReplacement, &row.AuthorizationVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return row, true, nil
}

// readIntentByRequest loads the 012 intent of a request, if admitted.
func (a *PGStateAdapter) readIntentByRequest(ctx context.Context, rec *PGStateRecord, requestID string) (*PGIntent, bool, error) {
	return a.readIntentRow(ctx, rec, pgIntentByRequestSQL, requestID)
}

// readIntentByID loads one 012 intent by its identity.
func (a *PGStateAdapter) readIntentByID(ctx context.Context, rec *PGStateRecord, intentID string) (*PGIntent, bool, error) {
	return a.readIntentRow(ctx, rec, pgIntentByIDSQL, intentID)
}

func (a *PGStateAdapter) readIntentRow(ctx context.Context, rec *PGStateRecord, sql, arg string) (*PGIntent, bool, error) {
	rec.noteSource(PGSourcePaymentIntent)
	row := &PGIntent{}
	err := a.db.QueryRow(ctx, sql, arg).Scan(
		&row.IntentID, &row.RequestID, &row.ChainID, &row.Sender,
		&row.AuthorizationID, &row.AuthorizationVersion, &row.State, &row.StateVersion,
		&row.AdmittedRecoveryVersion, &row.AdmittedAt, &row.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	rec.trackObserved(row.AdmittedAt)
	rec.trackObserved(row.UpdatedAt)
	return row, true, nil
}

// readClaim loads the intent's execution claim, if one exists (its absence is
// a definitive "not claimed yet" fact).
func (a *PGStateAdapter) readClaim(ctx context.Context, rec *PGStateRecord, intentID string) (*PGExecutionClaim, bool, error) {
	rec.noteSource(PGSourceExecutionClaim)
	row := &PGExecutionClaim{IntentID: intentID}
	err := a.db.QueryRow(ctx, pgClaimReadSQL, intentID).Scan(
		&row.OwnerID, &row.LeaseVersion, &row.State, &row.ExpiresAt, &row.Active,
		&row.LastHeartbeatAt, &row.LastProgressAt, &row.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	rec.trackObserved(row.UpdatedAt)
	return row, true, nil
}

// readOpenStep loads the intent's open (issued) step, if any.
func (a *PGStateAdapter) readOpenStep(ctx context.Context, rec *PGStateRecord, intentID string) (*PGExecutionStep, bool, error) {
	rec.noteSource(PGSourceExecutionStep)
	row := &PGExecutionStep{IntentID: intentID}
	err := a.db.QueryRow(ctx, pgOpenStepReadSQL, intentID).Scan(
		&row.StepID, &row.Action, &row.State, &row.OwnerID, &row.LeaseVersion,
		&row.AttemptID, &row.TxHash, &row.OutcomeClass, &row.RecoveryVersion,
		&row.IssuedAt, &row.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	rec.trackObserved(row.UpdatedAt)
	return row, true, nil
}

// readAttempts loads the bounded attempt history of one intent (newest first
// in SQL, stored ascending). Truncation is visible on the record and never
// hides the latest attempt.
func (a *PGStateAdapter) readAttempts(ctx context.Context, rec *PGStateRecord, intentID string) error {
	rec.noteSource(PGSourceTxAttempt)
	rec.noteSource(PGSourceTxSigning)
	rows, err := a.db.Query(ctx, pgAttemptsForIntentSQL, intentID, a.maxAttempts+1)
	if err != nil {
		return fmt.Errorf("read tx_attempts of intent %s: %w", intentID, err)
	}
	defer rows.Close()

	var out []PGAttempt
	for rows.Next() {
		var row PGAttempt
		if err := scanPGAttempt(rows, &row); err != nil {
			return fmt.Errorf("scan tx_attempts row: %w", err)
		}
		row.Amount = parsePGAmount(rec, "tx_attempts", row.AttemptID, row.AmountText)
		rec.trackObserved(row.UpdatedAt)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate tx_attempts: %w", err)
	}
	if len(out) > a.maxAttempts {
		rec.AttemptsTruncated = true
		out = out[:a.maxAttempts]
	}
	// Reverse into ascending order for stable evidence.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	rec.Attempts = out
	return nil
}

// readAttemptByID loads one attempt row.
func (a *PGStateAdapter) readAttemptByID(ctx context.Context, rec *PGStateRecord, attemptID string) (*PGAttempt, bool, error) {
	rec.noteSource(PGSourceTxAttempt)
	rec.noteSource(PGSourceTxSigning)
	row := &PGAttempt{}
	err := scanPGAttempt(a.db.QueryRow(ctx, pgAttemptByIDSQL, attemptID), row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	row.Amount = parsePGAmount(rec, "tx_attempts", attemptID, row.AmountText)
	rec.trackObserved(row.UpdatedAt)
	return row, true, nil
}

// readAttemptIDByTxHash resolves a chain transaction hash to its authoritative
// attempt id through the durable 010/011 rows (tx_attempt_signings first, then
// tx_receipts; both read-only). No row is a definitive "no business record for
// this chain fact" answer. More than one distinct attempt behind one hash can
// only be structural drift: it is downgraded conservatively (incomplete) so the
// read can never support a conclusion.
func (a *PGStateAdapter) readAttemptIDByTxHash(ctx context.Context, rec *PGStateRecord, txHash string) (string, bool, error) {
	rec.noteSource(PGSourceTxSigning)
	rec.noteSource(PGSourceTxReceipt)
	rows, err := a.db.Query(ctx, pgAttemptIDByTxHashSQL, txHash)
	if err != nil {
		return "", false, fmt.Errorf("read tx_attempt_signings/tx_receipts by tx hash: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", false, fmt.Errorf("scan tx-hash attempt resolution: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("iterate tx-hash attempt resolution: %w", err)
	}
	switch len(ids) {
	case 0:
		return "", false, nil
	case 1:
		return ids[0], true, nil
	default:
		rec.downgrade(PGStateIncomplete, PGReasonEvidenceInconsistent)
		rec.note("tx hash resolves to %d distinct attempts; evidence is structurally inconsistent", len(ids))
		return ids[0], true, nil
	}
}

// readAttemptDetail attaches the txlifecycle.UnknownRecovery facts and the
// latest receipt to the current attempt.
func (a *PGStateAdapter) readAttemptDetail(ctx context.Context, rec *PGStateRecord) error {
	att := rec.Attempt
	if a.facts == nil {
		return fmt.Errorf("txlifecycle attempt-facts reader is not wired")
	}
	rec.noteSource(PGSourceTxSendAttempt)
	rec.noteSource(PGSourceTxReconciliation)
	rec.noteSource(PGSourceTxReceipt)
	unknown, err := a.facts.UnknownRecovery(ctx, att.AttemptID, "")
	if err != nil {
		return fmt.Errorf("read attempt facts %s: %w", att.AttemptID, err)
	}
	if unknown == nil {
		return fmt.Errorf("read attempt facts %s: no evidence returned", att.AttemptID)
	}

	att.SignedBytesPresent = unknown.SignedBytesPresent
	att.Frozen = unknown.Frozen
	att.LastGateBasis = unknown.LastGateBasis
	att.LastReceiptEffect = unknown.LastReceiptEffect
	att.RecoveryConditions = append([]string(nil), unknown.RecoveryConditions...)
	att.Dispatches = append([]txlifecycle.SendFact(nil), unknown.Dispatches...)
	att.Observations = append([]txlifecycle.ReconcileFact(nil), unknown.Observations...)
	for i := range att.Dispatches {
		if att.Dispatches[i].DispatchedAt != nil {
			rec.trackObserved(*att.Dispatches[i].DispatchedAt)
		}
	}
	for i := range att.Observations {
		rec.trackObserved(att.Observations[i].ObservedAt)
	}

	receipt, found, err := a.readLatestReceipt(ctx, att.AttemptID)
	if err != nil {
		return err
	}
	if found {
		att.Receipt = receipt
		rec.trackObserved(receipt.ObservedAt)
		rec.trackObserved(receipt.UpdatedAt)
		if receipt.ConfirmedAt != nil {
			rec.trackObserved(*receipt.ConfirmedAt)
		}
		if receipt.OrphanedAt != nil {
			rec.trackObserved(*receipt.OrphanedAt)
		}
	}
	return nil
}

// readLatestReceipt loads the most recent tx_receipts row of one attempt.
func (a *PGStateAdapter) readLatestReceipt(ctx context.Context, attemptID string) (*PGReceipt, bool, error) {
	row := &PGReceipt{}
	err := a.db.QueryRow(ctx, pgLatestReceiptSQL, attemptID).Scan(
		&row.TxHash, &row.Status, &row.BlockNumber, &row.BlockHash, &row.Effect,
		&row.Canonicality, &row.ConfirmationsText, &row.ConfirmThreshold,
		&row.ConfirmPolicySeq, &row.ConfirmTipNumber, &row.ConfirmTipHash,
		&row.ObservedAt, &row.UpdatedAt, &row.ConfirmedAt, &row.OrphanedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	confirmations, parseErr := strconv.ParseInt(strings.TrimSpace(row.ConfirmationsText), 10, 64)
	if parseErr != nil {
		return nil, false, fmt.Errorf("tx_receipts.confirmations %q is not an integer", row.ConfirmationsText)
	}
	row.Confirmations = confirmations
	return row, true, nil
}

// readGateContext reads the 006 recovery snapshot (read-only reuse of
// execution.RecoverySnapshotSQL; no SHARE lock). A failed snapshot downgrades
// to incomplete instead of failing the whole read: business state is still
// readable, but the recovery version evidence is unavailable.
func (a *PGStateAdapter) readGateContext(ctx context.Context, rec *PGStateRecord) {
	rec.noteSource(PGSourceRecoveryGate)
	var (
		gate       PGGateContext
		recoveryID *string
		phase      *string
		seq        *int64
		eventsMax  *int64
	)
	err := a.db.QueryRow(ctx, execution.RecoverySnapshotSQL, rec.ChainID).Scan(
		&gate.IndexerPaused, &gate.LogPaused, &gate.DepositPaused,
		&recoveryID, &phase, &seq, &eventsMax)
	if err != nil {
		rec.downgrade(PGStateIncomplete, PGReasonGateContextUnavailable)
		rec.note("006 recovery snapshot unavailable: %v", err)
		return
	}
	gate.Observed = true
	if recoveryID != nil {
		gate.HasRecovery = true
	}
	if phase != nil {
		gate.RecoveryPhase = *phase
	}
	if seq != nil {
		gate.RecoverySeq = *seq
	}
	if eventsMax != nil {
		gate.HasEventsMax = true
		gate.EventsMax = *eventsMax
	}
	rec.Gate = gate
}

// scanPGAttempt scans one attempt row (pgx.Row and pgx.Rows both satisfy the
// Scan method set).
func scanPGAttempt(scanner interface{ Scan(dest ...any) error }, row *PGAttempt) error {
	return scanner.Scan(
		&row.AttemptID, &row.IntentID, &row.AuthorizationID, &row.AuthorizationVersion,
		&row.RecoveryVersion, &row.ChainID, &row.Sender, &row.Asset, &row.Recipient,
		&row.AmountText, &row.ContentHash, &row.State, &row.RevisionSeq, &row.TxHash,
		&row.EffectiveAt, &row.ConfirmedAt, &row.OrphanedAt, &row.ReplacedAt, &row.UpdatedAt)
}

// parsePGAmount parses an unsigned integer decimal; a malformed value is
// malformed evidence (incomplete), never a numeric guess.
func parsePGAmount(rec *PGStateRecord, source, id, raw string) *big.Int {
	value, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10)
	if !ok || value.Sign() < 0 {
		rec.downgrade(PGStateIncomplete, PGReasonMalformedEvidence)
		rec.note("%s %s amount %q is not an unsigned integer decimal", source, id, raw)
		return nil
	}
	return value
}

// ---------------------------------------------------------------------------
// Canonical PG-party bytes (Snapshot.PG). Business-state only: Status, Reason,
// freshness, and Notes are observation metadata and are deliberately excluded
// so the same state read twice hashes identically (dedup stability).

// pgStateCanonicalVersion freezes the PG-snapshot canonicalization domain.
// Changing it changes every content hash and is a dedup-breaking change.
const pgStateCanonicalVersion = "txharbor.reconciliation.pgstate.v1"

// CanonicalBytes returns the deterministic canonical bytes of the PG party.
func (r *PGStateRecord) CanonicalBytes() []byte {
	if r == nil {
		return nil
	}
	w := &identityCanonWriter{}
	w.bytesField("pgstate.version", []byte(pgStateCanonicalVersion))
	pgCanonString(w, "chain_id", strconv.FormatInt(r.ChainID, 10))
	pgCanonString(w, "business_type", string(r.BusinessType))
	pgCanonString(w, "business_key.kind", string(r.BusinessKey.Kind))
	pgCanonString(w, "business_key.value", r.BusinessKey.Value)

	pgCanonBool(w, "request.present", r.Request != nil)
	if r.Request != nil {
		pgCanonString(w, "request.request_id", r.Request.RequestID)
		pgCanonInt(w, "request.caller_id", r.Request.CallerID)
		pgCanonString(w, "request.authorization_id", r.Request.AuthorizationID)
		pgCanonInt(w, "request.chain_id", r.Request.ChainID)
		pgCanonString(w, "request.asset", r.Request.Asset)
		pgCanonString(w, "request.recipient", r.Request.Recipient)
		pgCanonMoney(w, "request.amount", r.Request.AmountText, r.Request.Amount)
		pgCanonString(w, "request.status", r.Request.Status)
		pgCanonTime(w, "request.created_at", r.Request.CreatedAt)
	}
	pgCanonBool(w, "authorization.present", r.Authorization != nil)
	if r.Authorization != nil {
		pgCanonString(w, "authorization.authorization_id", r.Authorization.AuthorizationID)
		pgCanonInt(w, "authorization.caller_id", r.Authorization.CallerID)
		pgCanonInt(w, "authorization.chain_id", r.Authorization.ChainID)
		pgCanonString(w, "authorization.asset", r.Authorization.Asset)
		pgCanonString(w, "authorization.recipient", r.Authorization.Recipient)
		pgCanonMoney(w, "authorization.amount", r.Authorization.AmountText, r.Authorization.Amount)
		pgCanonString(w, "authorization.state", r.Authorization.State)
		pgCanonTimePtr(w, "authorization.expires_at", r.Authorization.ExpiresAt)
		pgCanonTime(w, "authorization.supplied_at", r.Authorization.SuppliedAt)
	}
	pgCanonBool(w, "scope.present", r.AuthScope != nil)
	if r.AuthScope != nil {
		pgCanonString(w, "scope.authorization_id", r.AuthScope.AuthorizationID)
		pgCanonString(w, "scope.intent_id", r.AuthScope.IntentID)
		pgCanonString(w, "scope.request_id", r.AuthScope.RequestID)
		pgCanonString(w, "scope.sender", r.AuthScope.Sender)
		pgCanonInt(w, "scope.fee_max_total", r.AuthScope.FeeMaxTotal)
		pgCanonInt(w, "scope.fee_max_per_gas", r.AuthScope.FeeMaxPerGas)
		pgCanonInt(w, "scope.fee_max_priority", r.AuthScope.FeeMaxPriority)
		pgCanonBool(w, "scope.allows_fee_replacement", r.AuthScope.AllowsFeeReplacement)
		pgCanonInt(w, "scope.authorization_version", r.AuthScope.AuthorizationVersion)
	}
	pgCanonBool(w, "intent.present", r.Intent != nil)
	if r.Intent != nil {
		pgCanonString(w, "intent.intent_id", r.Intent.IntentID)
		pgCanonString(w, "intent.request_id", r.Intent.RequestID)
		pgCanonInt(w, "intent.chain_id", r.Intent.ChainID)
		pgCanonString(w, "intent.sender", r.Intent.Sender)
		pgCanonString(w, "intent.authorization_id", r.Intent.AuthorizationID)
		pgCanonInt(w, "intent.authorization_version", r.Intent.AuthorizationVersion)
		pgCanonString(w, "intent.state", r.Intent.State)
		pgCanonInt(w, "intent.state_version", r.Intent.StateVersion)
		pgCanonInt(w, "intent.admitted_recovery_version", r.Intent.AdmittedRecoveryVersion)
		pgCanonTime(w, "intent.admitted_at", r.Intent.AdmittedAt)
		pgCanonTime(w, "intent.updated_at", r.Intent.UpdatedAt)
	}
	pgCanonBool(w, "claim.present", r.Claim != nil)
	if r.Claim != nil {
		pgCanonInt(w, "claim.lease_version", r.Claim.LeaseVersion)
		pgCanonString(w, "claim.state", r.Claim.State)
		pgCanonTime(w, "claim.expires_at", r.Claim.ExpiresAt)
		pgCanonTime(w, "claim.updated_at", r.Claim.UpdatedAt)
	}
	pgCanonBool(w, "step.present", r.OpenStep != nil)
	if r.OpenStep != nil {
		pgCanonString(w, "step.step_id", r.OpenStep.StepID)
		pgCanonString(w, "step.action", r.OpenStep.Action)
		pgCanonString(w, "step.state", r.OpenStep.State)
		pgCanonInt(w, "step.lease_version", r.OpenStep.LeaseVersion)
		pgCanonString(w, "step.attempt_id", r.OpenStep.AttemptID)
		pgCanonString(w, "step.tx_hash", r.OpenStep.TxHash)
		pgCanonString(w, "step.outcome_class", r.OpenStep.OutcomeClass)
		pgCanonIntPtr(w, "step.recovery_version", r.OpenStep.RecoveryVersion)
		pgCanonTime(w, "step.updated_at", r.OpenStep.UpdatedAt)
	}

	pgCanonUint(w, "attempts.count", uint64(len(r.Attempts)))
	pgCanonBool(w, "attempts.truncated", r.AttemptsTruncated)
	for i := range r.Attempts {
		pgCanonAttempt(w, fmt.Sprintf("attempts.%d", i), &r.Attempts[i])
	}
	pgCanonBool(w, "attempt.present", r.Attempt != nil)
	if r.Attempt != nil {
		pgCanonAttempt(w, "attempt", r.Attempt)
		pgCanonAttemptDetail(w, "attempt", r.Attempt)
	}

	pgCanonBool(w, "gate.has_recovery", r.Gate.HasRecovery)
	pgCanonInt(w, "gate.recovery_seq", r.Gate.RecoverySeq)
	pgCanonInt(w, "gate.events_max", r.Gate.EventsMax)

	pgCanonInt(w, "version.authorization_version", r.Version.AuthorizationVersion)
	pgCanonInt(w, "version.recovery_version", r.Version.RecoveryVersion)
	pgCanonInt(w, "version.state_version", r.Version.StateVersion)
	pgCanonInt(w, "version.lease_version", r.Version.LeaseVersion)
	pgCanonInt(w, "version.revision_seq", r.Version.RevisionSeq)
	pgCanonString(w, "version.content_hash", r.Version.ContentHash)
	return w.buf.Bytes()
}

// pgCanonAttempt writes one attempt row summary.
func pgCanonAttempt(w *identityCanonWriter, name string, att *PGAttempt) {
	pgCanonString(w, name+".attempt_id", att.AttemptID)
	pgCanonString(w, name+".intent_id", att.IntentID)
	pgCanonString(w, name+".authorization_id", att.AuthorizationID)
	pgCanonInt(w, name+".authorization_version", att.AuthorizationVersion)
	pgCanonInt(w, name+".recovery_version", att.RecoveryVersion)
	pgCanonInt(w, name+".chain_id", att.ChainID)
	pgCanonString(w, name+".sender", att.Sender)
	pgCanonString(w, name+".asset", att.Asset)
	pgCanonString(w, name+".recipient", att.Recipient)
	pgCanonMoney(w, name+".amount", att.AmountText, att.Amount)
	pgCanonString(w, name+".content_hash", att.ContentHash)
	pgCanonString(w, name+".state", att.State)
	pgCanonInt(w, name+".revision_seq", att.RevisionSeq)
	pgCanonString(w, name+".tx_hash", att.TxHash)
	pgCanonTimePtr(w, name+".effective_at", att.EffectiveAt)
	pgCanonTimePtr(w, name+".confirmed_at", att.ConfirmedAt)
	pgCanonTimePtr(w, name+".orphaned_at", att.OrphanedAt)
	pgCanonTimePtr(w, name+".replaced_at", att.ReplacedAt)
	pgCanonTime(w, name+".updated_at", att.UpdatedAt)
}

// pgCanonAttemptDetail writes the current attempt's unknown-recovery facts and
// receipt. Advisory recovery-condition text is excluded (derived wording must
// not churn identity hashes).
func pgCanonAttemptDetail(w *identityCanonWriter, name string, att *PGAttempt) {
	pgCanonBool(w, name+".signed_bytes_present", att.SignedBytesPresent)
	pgCanonBool(w, name+".frozen", att.Frozen)
	pgCanonInt(w, name+".gate.observed_recovery_version", att.LastGateBasis.ObservedRecoveryVersion)
	pgCanonString(w, name+".gate.observed_pause", att.LastGateBasis.ObservedPause)
	pgCanonIntPtr(w, name+".gate.observed_claim_version", att.LastGateBasis.ObservedClaimVersion)
	pgCanonTimePtr(w, name+".gate.observed_claim_expiry", att.LastGateBasis.ObservedClaimExpiry)
	pgCanonString(w, name+".gate.observed_authorization_id", att.LastGateBasis.ObservedAuthorizationID)
	pgCanonIntPtr(w, name+".gate.observed_authorization_version", att.LastGateBasis.ObservedAuthorizationVersion)
	pgCanonString(w, name+".gate.observed_authorization_state", att.LastGateBasis.ObservedAuthorizationState)
	pgCanonTime(w, name+".gate.observed_now", att.LastGateBasis.ObservedNow)
	pgCanonString(w, name+".gate.observed_binding_state", att.LastGateBasis.ObservedBindingState)

	pgCanonUint(w, name+".dispatches.count", uint64(len(att.Dispatches)))
	for i := range att.Dispatches {
		prefix := fmt.Sprintf("%s.dispatches.%d", name, i)
		pgCanonInt(w, prefix+".send_seq", int64(att.Dispatches[i].SendSeq))
		pgCanonString(w, prefix+".kind", att.Dispatches[i].Kind)
		pgCanonString(w, prefix+".outcome", att.Dispatches[i].Outcome)
		pgCanonString(w, prefix+".rpc_class", att.Dispatches[i].RPCClass)
		pgCanonTimePtr(w, prefix+".dispatched_at", att.Dispatches[i].DispatchedAt)
	}
	pgCanonUint(w, name+".observations.count", uint64(len(att.Observations)))
	for i := range att.Observations {
		prefix := fmt.Sprintf("%s.observations.%d", name, i)
		pgCanonString(w, prefix+".classification", att.Observations[i].Classification)
		pgCanonIntPtr(w, prefix+".block_number", att.Observations[i].BlockNumber)
		pgCanonString(w, prefix+".block_hash", att.Observations[i].BlockHash)
		pgCanonString(w, prefix+".rpc_class", att.Observations[i].RPCClass)
		pgCanonTime(w, prefix+".observed_at", att.Observations[i].ObservedAt)
	}
	pgCanonBool(w, name+".receipt.present", att.Receipt != nil)
	if att.Receipt != nil {
		receipt := att.Receipt
		prefix := name + ".receipt"
		pgCanonString(w, prefix+".tx_hash", receipt.TxHash)
		pgCanonInt(w, prefix+".status", int64(receipt.Status))
		pgCanonInt(w, prefix+".block_number", receipt.BlockNumber)
		pgCanonString(w, prefix+".block_hash", receipt.BlockHash)
		pgCanonString(w, prefix+".effect", receipt.Effect)
		pgCanonString(w, prefix+".canonicality", receipt.Canonicality)
		pgCanonString(w, prefix+".confirmations", receipt.ConfirmationsText)
		pgCanonInt(w, prefix+".confirm_threshold", receipt.ConfirmThreshold)
		pgCanonInt(w, prefix+".confirm_policy_seq", receipt.ConfirmPolicySeq)
		pgCanonIntPtr(w, prefix+".confirm_tip_number", receipt.ConfirmTipNumber)
		pgCanonString(w, prefix+".confirm_tip_hash", receipt.ConfirmTipHash)
		pgCanonTime(w, prefix+".observed_at", receipt.ObservedAt)
		pgCanonTime(w, prefix+".updated_at", receipt.UpdatedAt)
		pgCanonTimePtr(w, prefix+".confirmed_at", receipt.ConfirmedAt)
		pgCanonTimePtr(w, prefix+".orphaned_at", receipt.OrphanedAt)
	}
}

func pgCanonString(w *identityCanonWriter, name, value string)      { w.stringField(name, value) }
func pgCanonInt(w *identityCanonWriter, name string, value int64)   { w.int64Field(name, value) }
func pgCanonUint(w *identityCanonWriter, name string, value uint64) { w.uint64Field(name, value) }

func pgCanonBool(w *identityCanonWriter, name string, value bool) {
	if value {
		w.stringField(name, "true")
		return
	}
	w.stringField(name, "false")
}

func pgCanonTime(w *identityCanonWriter, name string, value time.Time) {
	if value.IsZero() {
		w.stringField(name, "zero")
		return
	}
	w.int64Field(name, value.UTC().UnixNano())
}

func pgCanonTimePtr(w *identityCanonWriter, name string, value *time.Time) {
	if value == nil {
		w.stringField(name, "nil")
		return
	}
	pgCanonTime(w, name, *value)
}

func pgCanonIntPtr(w *identityCanonWriter, name string, value *int64) {
	if value == nil {
		w.stringField(name, "nil")
		return
	}
	w.int64Field(name, *value)
}

func pgCanonMoney(w *identityCanonWriter, name, raw string, value *big.Int) {
	if value == nil {
		w.stringField(name+".malformed", raw)
		return
	}
	w.stringField(name, value.String())
}
