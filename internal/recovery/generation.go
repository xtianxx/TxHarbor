// generation.go implements T013: the evidence-generation token and the
// write-write out-of-order protocol of data-model.md §5 (FR-024), in the same
// shape as the 014 §3.1 reverify token.
//
// Problem. Evidence is gathered outside any database transaction: a source
// reader gathers facts, forms a conclusion, and only then commits. If writer A
// captures its token first, writer B captures later but commits first, A's
// stale commit must not overwrite B's newer conclusion, close or open a gap on
// top of B's, advance the generation backwards, or let a stale result be read
// as the current evidence. Wall clocks and `created_at` are never part of the
// protection: data-model §5 decides same-second concurrency by commit order,
// because the instance row lock serializes every write.
//
// Protocol.
//
//  1. Capture, outside any transaction: CaptureEvidenceToken reads
//     (state, evidence_generation, evidence_hash) of the instance before the
//     sources are read. The captured token is the only validity claim an
//     in-flight result may carry.
//
//  2. Commit, in one short transaction: CommitEvidenceWrite takes the
//     instance row lock (controlstore.LockInstance, the same lock every
//     decision write takes), re-reads the token and validates all of its
//     components. On any mismatch the in-flight result is discarded: the
//     transaction writes exactly one recovery_audit row with
//     result='discarded' — no result row, no gap change, no generation
//     advance, no overwrite of anything a newer committed write produced.
//
//  3. Accept: a matching token calls the caller's Apply hook, which writes
//     the result rows inside the locked transaction using the accepted
//     token's new generation; the same transaction then advances
//     evidence_generation by exactly 1 and refreshes evidence_hash. Every
//     approval/release bound to the previous generation/hash is therefore
//     invalidated at once (fail-closed, INV-3).
//
// Generation-advancing writes (the closed set of data-model §5: verification
// batch results, gap establishment/closure, isolation item verified/rejected,
// restore-probe acceptance and evidence-snapshot acceptance, plus the restore
// pre-write invalidation marker of restore.go, `restore_started`, which the
// duplicate-restore timing needs so the old authorization basis is stale
// before the first target write — INV-3). Every one of those writes must go
// through CommitEvidenceWrite — this file is the only generation protocol in
// 015 (tasks.md Shared-Artifact Confluence).
//
// Reusable interface for T012 (gate.go) and the later write paths:
//
//   - T012 re-reads the instance inside the instance lock it already holds
//     (controlstore.LockInstance) and validates with
//     LockedEvidenceToken + EvidenceToken.Validate before deriving release
//     validity; a mismatch means "evidence changed after the decision was
//     captured" and the gate must refuse (the generation-aware cache
//     invalidation of data-model §3.3).
//   - The later write paths (verification batches, gaps, checklist, restore
//     probe, evidence snapshots) wrap their persistence in Apply and let
//     CommitEvidenceWrite own the lock, the token check, the discard audit and
//     the generation + hash advance.
package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// ActionEvidenceWrite is the recovery_audit action of the generation protocol:
// every CommitEvidenceWrite call writes exactly one row under this action —
// result='ok' for an accepted write, result='discarded' for a stale one. The
// row's target carries the transition kind and the captured/observed/accepted
// token components.
const ActionEvidenceWrite = "evidence_write"

// ErrUnknownEvidenceMutation marks a write kind outside the closed set of
// generation-advancing writes (data-model §5). Like the capability set, this
// is a contract error, never a gate refusal: callers wire kinds from this
// package's constants, so an unknown value means the caller bypassed the
// protocol. Refusing is the only safe handling.
var ErrUnknownEvidenceMutation = errors.New("unknown evidence mutation kind")

// EvidenceMutationKind is the closed set of writes that advance the instance
// evidence generation (data-model §5). The string values are frozen
// vocabulary: they appear in recovery_audit targets and in the evidence hash
// chain, so renaming one is a specification change, not a refactor.
type EvidenceMutationKind string

// The generation-advancing writes of data-model §5.
const (
	// MutationVerificationBatch: a verification-batch result (V1-V9 items)
	// was accepted.
	MutationVerificationBatch EvidenceMutationKind = "verification_batch"
	// MutationGapOpened: a new evidence gap was established.
	MutationGapOpened EvidenceMutationKind = "gap_opened"
	// MutationGapClosed: an existing gap was closed by new evidence.
	MutationGapClosed EvidenceMutationKind = "gap_closed"
	// MutationIsolationVerified: an isolation checklist item reached
	// state='verified'.
	MutationIsolationVerified EvidenceMutationKind = "isolation_verified"
	// MutationIsolationRejected: an isolation checklist item was rejected
	// (evidence insufficient, re-collection required).
	MutationIsolationRejected EvidenceMutationKind = "isolation_rejected"
	// MutationRestoreStarted: a restore executor started a real restore
	// against its declared target. This is the pre-write invalidation marker
	// of restore.go, committed before the first target write so that every
	// release/approval bound to the pre-restore generation stops being usable
	// for admission even if the restore fails or is interrupted (INV-3,
	// data-model §5; duplicate-restore invalidation timing). The accepted
	// restore_probe evidence is still written only after all four probes pass.
	MutationRestoreStarted EvidenceMutationKind = "restore_started"
	// MutationRestoreProbeAccepted: a restore probe was accepted (the
	// `restored` evidence of data-model §4.2/§7).
	MutationRestoreProbeAccepted EvidenceMutationKind = "restore_probe_accepted"
	// MutationEvidenceSnapshotAccepted: an evidence snapshot/batch was
	// accepted and bound to the instance.
	MutationEvidenceSnapshotAccepted EvidenceMutationKind = "evidence_snapshot_accepted"
)

// knownEvidenceMutationKinds is the canonical order of the closed set; the
// order is data-model §5's trigger list. It is display/validation-only.
var knownEvidenceMutationKinds = []EvidenceMutationKind{
	MutationVerificationBatch,
	MutationGapOpened,
	MutationGapClosed,
	MutationIsolationVerified,
	MutationIsolationRejected,
	MutationRestoreStarted,
	MutationRestoreProbeAccepted,
	MutationEvidenceSnapshotAccepted,
}

// Known reports whether k is one of the generation-advancing write kinds.
func (k EvidenceMutationKind) Known() bool {
	return slices.Contains(knownEvidenceMutationKinds, k)
}

// KnownEvidenceMutationKinds returns the closed set in canonical order. The
// caller receives a fresh slice and may mutate it freely.
func KnownEvidenceMutationKinds() []EvidenceMutationKind {
	return slices.Clone(knownEvidenceMutationKinds)
}

// ParseEvidenceMutationKind maps s onto the closed set. Matching is exact:
// unknown, empty, differently-cased or whitespace-padded input is refused with
// ErrUnknownEvidenceMutation. An unparsed kind must never become a default.
func ParseEvidenceMutationKind(s string) (EvidenceMutationKind, error) {
	k := EvidenceMutationKind(s)
	if !k.Known() {
		return "", fmt.Errorf("%w: %q (known: %v)", ErrUnknownEvidenceMutation, s, knownEvidenceMutationKinds)
	}
	return k, nil
}

// ---------------------------------------------------------------------------
// Evidence-generation token (capture / validate)
// ---------------------------------------------------------------------------

// EvidenceToken is the captured evidence-generation token of one instance: the
// (state, evidence_generation, evidence_hash) triple of data-model §5. It is
// captured transaction-externally by CaptureEvidenceToken and is the only
// validity claim an in-flight result may carry.
//
// The token is a freshness claim, never an authorization: a matching token
// only says "nothing conclusion-relevant committed since this capture". It
// does not say the write itself is allowed — that is decided by the caller
// path (checklist/verification rules, approvals, the T012 gate).
type EvidenceToken struct {
	// InstanceID is the canonical (lower-case) recovery instance id.
	InstanceID string
	// State is recovery_instance.state ('open' for any valid evidence write).
	State string
	// Generation is recovery_instance.evidence_generation at capture time.
	Generation int64
	// Hash is recovery_instance.evidence_hash at capture time: the aggregate
	// hash of the accepted evidence stream (data-model §1.1).
	Hash string
}

// TokenQueryer is the narrow database surface CaptureEvidenceToken needs.
// *pgxpool.Pool and pgx.Tx satisfy it, so the same helper serves the
// transaction-external capture and (for T012) the authoritative re-read inside
// the instance lock.
type TokenQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const captureEvidenceTokenSQL = `
SELECT instance_id::text, state, evidence_generation, evidence_hash
FROM recovery_instance
WHERE instance_id = $1`

// CaptureEvidenceToken reads the evidence-generation token of one instance.
//
// For the write-write protocol it is the transaction-external capture: call it
// before the sources are read, outside any transaction holding locks
// (data-model §5), and let CommitEvidenceWrite validate it under the instance
// row lock. It is also the authoritative re-read for T012's gate: called on a
// transaction that already holds controlstore.LockInstance, it returns the
// same (state, evidence_generation, evidence_hash) the lock protects.
//
// The instance id is returned by the database, so the token carries the
// canonical form regardless of the input's case. A missing instance refuses
// with controlstore.ErrInstanceNotFound; a database failure refuses with the
// wrapped error — never a zero-value "no instance, so pass" token.
func CaptureEvidenceToken(ctx context.Context, q TokenQueryer, instanceID string) (EvidenceToken, error) {
	if q == nil {
		return EvidenceToken{}, errors.New("evidence token capture requires a database handle")
	}
	id := strings.TrimSpace(instanceID)
	if id == "" {
		return EvidenceToken{}, errors.New("evidence token capture requires an instance_id")
	}
	var token EvidenceToken
	err := q.QueryRow(ctx, captureEvidenceTokenSQL, id).Scan(
		&token.InstanceID, &token.State, &token.Generation, &token.Hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return EvidenceToken{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotFound, id)
	}
	if err != nil {
		return EvidenceToken{}, fmt.Errorf("capture evidence token of instance %s: %w", id, err)
	}
	return token, nil
}

// LockedEvidenceToken adapts the authoritative in-lock instance snapshot
// (controlstore.InstanceToken) to a token. T012 uses this — or
// CaptureEvidenceToken on the locked transaction — to re-read the authoritative
// token inside the instance row lock before deriving release validity.
func LockedEvidenceToken(locked controlstore.InstanceToken) EvidenceToken {
	return EvidenceToken{
		InstanceID: locked.InstanceID,
		State:      locked.State,
		Generation: locked.EvidenceGeneration,
		Hash:       locked.EvidenceHash,
	}
}

// Matches reports whether both tokens describe the same instance state: all
// four components identical. It is the boolean form for T012's gate decision;
// use Validate when the mismatch must be explained (audit/discard reason).
func (t EvidenceToken) Matches(other EvidenceToken) bool {
	return t.InstanceID == other.InstanceID &&
		t.State == other.State &&
		t.Generation == other.Generation &&
		t.Hash == other.Hash
}

// TokenMismatch describes a captured token that no longer matches the
// persisted instance token: which components changed and to what. It carries
// no credential material.
type TokenMismatch struct {
	// Captured is the token captured before the sources were read.
	Captured EvidenceToken
	// Observed is the token re-read under the instance row lock.
	Observed EvidenceToken
}

// Error renders the mismatch with the changed components.
func (m *TokenMismatch) Error() string {
	if m == nil {
		return ""
	}
	return "evidence generation token no longer matches: " + strings.Join(m.Reasons(), "; ")
}

// Reasons names the changed token components, in a fixed order. It always
// returns at least one reason, so a discard audit never says only "mismatch"
// without naming what changed.
func (m *TokenMismatch) Reasons() []string {
	reasons := make([]string, 0, 4)
	if m.Captured.InstanceID != m.Observed.InstanceID {
		reasons = append(reasons, fmt.Sprintf("instance changed during the evidence read: %s -> %s",
			m.Captured.InstanceID, m.Observed.InstanceID))
	}
	if m.Captured.State != m.Observed.State {
		reasons = append(reasons, fmt.Sprintf("state changed during the evidence read: %s -> %s",
			m.Captured.State, m.Observed.State))
	}
	if m.Captured.Generation != m.Observed.Generation {
		reasons = append(reasons, fmt.Sprintf("generation changed during the evidence read: %d -> %d",
			m.Captured.Generation, m.Observed.Generation))
	}
	if m.Captured.Hash != m.Observed.Hash {
		reasons = append(reasons, "evidence hash changed during the evidence read")
	}
	if len(reasons) == 0 {
		// Matches() failed without a named component: impossible with the
		// current comparison, but the reason must stay explicit.
		reasons = append(reasons, "captured evidence token no longer matches the instance")
	}
	return reasons
}

// Validate compares the captured token against the token re-read under the
// instance row lock. All components must match; a nil return means the
// in-flight result may be committed. Any non-nil result discards the write:
// the caller writes only the discard audit row (CommitEvidenceWrite does this
// itself).
func (t EvidenceToken) Validate(locked EvidenceToken) *TokenMismatch {
	if t.Matches(locked) {
		return nil
	}
	return &TokenMismatch{Captured: t, Observed: locked}
}

// ---------------------------------------------------------------------------
// Evidence hash chain
// ---------------------------------------------------------------------------

// EvidenceChainHash derives the evidence_hash of one instance after an
// accepted transition. The instance hash is the aggregate hash of the accepted
// evidence stream (data-model §1.1): each accepted write chains the previous
// aggregate with the assigned generation and the canonical identity of the
// transition (kind, operation_id and an optional result digest, see
// EvidenceWriteRequest.ResultDigest), so:
//
//   - the hash changes on every accepted write (generation + hash advance
//     together, invalidating every older-generation approval/release),
//   - the same sequence of accepted transitions always reproduces the same
//     hash (auditable and testable),
//   - a stale write can never recompute the current hash because it does not
//     know the adoption order,
//   - two different contents accepted in the same position can never produce
//     the same hash (the result digest enters the chain).
//
// The returned value uses the same "sha256:<hex>" form as
// controlstore.EmptyEvidenceHash. generation is the generation being assigned
// (the previous generation + 1). This function is pure; callers use it to
// verify a recomputed chain.
func EvidenceChainHash(previousHash string, generation int64, kind EvidenceMutationKind, operationID string, resultDigest []byte) string {
	h := sha256.New()
	h.Write([]byte(evidenceHashDomain))
	h.Write([]byte{0})
	h.Write([]byte(previousHash))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(generation, 10)))
	h.Write([]byte{0})
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write([]byte(operationID))
	h.Write([]byte{0})
	digest := sha256.Sum256(resultDigest)
	h.Write([]byte(hex.EncodeToString(digest[:])))
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

const evidenceHashDomain = "txharbor-recovery-evidence-v1"

// ---------------------------------------------------------------------------
// Write-write ordering protocol (commit path)
// ---------------------------------------------------------------------------

// EvidenceWriteRequest is one generation-advancing write. The caller captures
// the token before deriving its result, then hands over the result-writing
// hook; this protocol owns the instance lock, the token check, the discard
// audit and the generation + hash advance.
type EvidenceWriteRequest struct {
	// InstanceID is the recovery instance the write belongs to.
	InstanceID string
	// Token is the token captured (via CaptureEvidenceToken) before the
	// result was derived.
	Token EvidenceToken
	// Kind is the generation-advancing write kind (closed set, data-model §5).
	Kind EvidenceMutationKind
	// Actor is the authenticated principal recorded on the protocol audit row.
	Actor string
	// Reason is an optional operator-facing note recorded in the audit detail.
	Reason string
	// OperationID is the command idempotency key (data-model §6). It is
	// recorded on the audit row and in the evidence hash chain; the result
	// tables keep their own UNIQUE operation_id semantics.
	OperationID string
	// ResultDigest is an optional canonical digest of the accepted result
	// content (for example the SHA-256 of an evidence artifact or of the
	// canonical verification batch payload). It enters the chained evidence
	// hash. A nil digest is allowed and hashed as the digest of the empty
	// input.
	ResultDigest []byte
	// Apply writes the result rows inside the locked transaction. It runs
	// only after the token validated; the transaction still holds the
	// instance row lock and no accepted write exists yet, so a failure rolls
	// everything back and leaves the instance untouched.
	//
	// Contract:
	//   - Write only result rows of this transition. Never touch
	//     recovery_instance directly (the protocol advances it) and never
	//     write a recovery_audit row with result='discarded' or action
	//     ActionEvidenceWrite (the protocol owns those).
	//   - Every result row MUST record the accepted token's Generation: the
	//     rows become the evidence of the NEW instance generation
	//     (recovery_evidence.generation, recovery_verification_item.generation
	//     etc.), which is what makes "generation = current" checks meaningful
	//     for the gate ("重核写新行" latest-row-wins only after this check).
	//   - It may write its own additional audit rows (different actions).
	Apply func(ctx context.Context, tx pgx.Tx, accepted EvidenceToken) error
}

// EvidenceWriteOutcome reports one protocol attempt. A discarded attempt is
// not an error: it is the protocol working as designed, recorded with
// result='discarded'.
type EvidenceWriteOutcome struct {
	// Discarded reports that the captured token no longer matched the
	// instance under the lock. The only write of this attempt is the discard
	// audit row: no result row, no gap change, no generation advance, no
	// overwrite.
	Discarded bool
	// Token is the accepted token (new generation/hash) on an accepted write,
	// or the observed instance token under the lock on a discarded write.
	Token EvidenceToken
	// Mismatch names the changed components (nil on an accepted write).
	Mismatch *TokenMismatch
	// DiscardReason is the bounded audit reason (empty on an accepted write).
	DiscardReason string
}

const advanceEvidenceGenerationSQL = `
UPDATE recovery_instance
SET evidence_generation = $2, evidence_hash = $3
WHERE instance_id = $1 AND evidence_generation = $4`

// CommitEvidenceWrite runs the write-write ordering protocol of data-model §5
// for one generation-advancing write:
//
//  1. Take the instance row lock (controlstore.LockInstance) — the same lock
//     every decision write takes, so commit order equals lock order.
//  2. Re-read the instance token under the lock and validate the captured
//     token. On mismatch: write exactly one recovery_audit row
//     (result='discarded', ActionEvidenceWrite, captured/observed components in
//     the target) and commit. Nothing else is written.
//  3. On match: call req.Apply with the accepted token
//     (Generation = observed+1, Hash = chained), then advance
//     evidence_generation to the accepted value and refresh evidence_hash in
//     the same transaction (guarded by the observed generation, so any
//     unexpected interleaving fails closed instead of overwriting), then write
//     the accepted audit row and commit.
//
// Ordering is decided by this lock/commit order alone, never by created_at or
// process clocks (data-model §5): two writers that captured the same token
// serialize here, the first advances the generation, and the second validates
// against the advanced token and is discarded.
func CommitEvidenceWrite(ctx context.Context, store *controlstore.Store, req EvidenceWriteRequest) (EvidenceWriteOutcome, error) {
	if store == nil || store.Pool() == nil {
		return EvidenceWriteOutcome{}, errors.New("evidence write requires a control-store handle")
	}
	if err := req.validate(); err != nil {
		return EvidenceWriteOutcome{}, err
	}

	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return EvidenceWriteOutcome{}, fmt.Errorf("begin evidence write: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := controlstore.LockInstance(ctx, tx, req.InstanceID); err != nil {
		return EvidenceWriteOutcome{}, fmt.Errorf("lock instance for evidence write: %w", err)
	}
	outcome, err := CommitEvidenceWriteTx(ctx, tx, req)
	if err != nil {
		return EvidenceWriteOutcome{}, err
	}
	if outcome.Discarded {
		// Preserve the standalone API's established behavior: even though the
		// result is discarded, its protocol audit is committed on its own.
		if err := tx.Commit(ctx); err != nil {
			return EvidenceWriteOutcome{}, fmt.Errorf("commit evidence discard audit: %w", err)
		}
		return outcome, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return EvidenceWriteOutcome{}, fmt.Errorf("commit evidence write: %w", err)
	}
	return outcome, nil
}

// CommitEvidenceWriteTx applies the evidence-generation protocol in tx without
// acquiring locks or committing. The caller MUST already hold the
// recovery_instance row lock. Target-writing callers must additionally hold
// the shared target-session lock, acquired before the instance row lock.
// This avoids nested instance-lock deadlocks in restore marker and post-probe
// acceptance workflows; ordinary evidence writers need no target lock.
//
// The outcome is Discarded when the captured token is stale. In that case this
// helper writes only the discarded evidence_write audit row: callers must not
// treat the write as guard-clean or persist result rows, and may commit the
// transaction only if they intentionally want to retain that discard audit.
// Accepted result rows, generation/hash advancement, and protocol audit are
// all part of the caller's transaction and therefore share its commit/rollback.
func CommitEvidenceWriteTx(ctx context.Context, tx pgx.Tx, req EvidenceWriteRequest) (EvidenceWriteOutcome, error) {
	if tx == nil {
		return EvidenceWriteOutcome{}, errors.New("transaction-scoped evidence write requires a transaction")
	}
	if err := req.validate(); err != nil {
		return EvidenceWriteOutcome{}, err
	}
	observed, err := CaptureEvidenceToken(ctx, tx, req.InstanceID)
	if err != nil {
		return EvidenceWriteOutcome{}, fmt.Errorf("read evidence token in evidence write transaction: %w", err)
	}
	if mismatch := req.Token.Validate(observed); mismatch != nil {
		reason := boundedEvidenceDetail("evidence write discarded: " + strings.Join(mismatch.Reasons(), "; "))
		if err := writeEvidenceTransitionAudit(ctx, tx, req, observed, EvidenceToken{}, controlstore.AuditDiscarded, reason); err != nil {
			return EvidenceWriteOutcome{}, err
		}
		return EvidenceWriteOutcome{
			Discarded:     true,
			Token:         observed,
			Mismatch:      mismatch,
			DiscardReason: reason,
		}, nil
	}
	if observed.State != "open" {
		return EvidenceWriteOutcome{}, fmt.Errorf("%w: %s", controlstore.ErrInstanceNotOpen, observed.InstanceID)
	}

	accepted := EvidenceToken{
		InstanceID: observed.InstanceID,
		State:      observed.State,
		Generation: observed.Generation + 1,
		Hash: EvidenceChainHash(observed.Hash, observed.Generation+1,
			req.Kind, req.OperationID, req.ResultDigest),
	}
	if err := req.Apply(ctx, tx, accepted); err != nil {
		return EvidenceWriteOutcome{}, fmt.Errorf("apply %s evidence write: %w", req.Kind, err)
	}
	tag, err := tx.Exec(ctx, advanceEvidenceGenerationSQL,
		accepted.InstanceID, accepted.Generation, accepted.Hash, observed.Generation)
	if err != nil {
		return EvidenceWriteOutcome{}, fmt.Errorf("advance evidence generation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return EvidenceWriteOutcome{}, fmt.Errorf(
			"advance evidence generation matched %d rows, expected exactly 1: instance=%s generation=%d",
			tag.RowsAffected(), accepted.InstanceID, observed.Generation)
	}
	if err := writeEvidenceTransitionAudit(ctx, tx, req, observed, accepted, controlstore.AuditOK, ""); err != nil {
		return EvidenceWriteOutcome{}, err
	}
	return EvidenceWriteOutcome{Token: accepted}, nil
}

// writeEvidenceTransitionAudit appends the single protocol audit row of one
// attempt (accepted or discarded) inside the caller's transaction. The row's
// evidence_generation is the CAPTURED generation — the token the attempt was
// evaluated against; the accepted/observed components live in the target.
func writeEvidenceTransitionAudit(ctx context.Context, tx pgx.Tx, req EvidenceWriteRequest,
	observed, accepted EvidenceToken, result, discardReason string) error {
	target := map[string]any{
		"kind":                string(req.Kind),
		"operation_id":        req.OperationID,
		"state":               observed.State,
		"captured_state":      req.Token.State,
		"captured_generation": req.Token.Generation,
		"observed_generation": observed.Generation,
		"captured_hash":       req.Token.Hash,
		"observed_hash":       observed.Hash,
	}
	detail := map[string]any{"reason": boundedEvidenceDetail(req.Reason)}
	if result == controlstore.AuditOK {
		target["accepted_generation"] = accepted.Generation
		target["accepted_hash"] = accepted.Hash
	} else {
		target["discard_reason"] = discardReason
		detail["discard_reason"] = discardReason
	}
	encodedTarget, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("encode evidence audit target: %w", err)
	}
	encodedDetail, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("encode evidence audit detail: %w", err)
	}
	generation := req.Token.Generation
	return controlstore.WriteAudit(ctx, tx, controlstore.AuditRecord{
		InstanceID:         observed.InstanceID,
		Actor:              req.Actor,
		Action:             ActionEvidenceWrite,
		Target:             encodedTarget,
		Detail:             encodedDetail,
		Result:             result,
		EvidenceGeneration: &generation,
		OperationID:        req.OperationID,
	})
}

// validate refuses a malformed request before any database access. The rule
// set is the protocol contract: a token captured on another instance, an
// unknown mutation kind, a missing actor and a missing Apply hook are
// programming errors, not concurrency events.
func (r EvidenceWriteRequest) validate() error {
	instanceID := strings.ToLower(strings.TrimSpace(r.InstanceID))
	if instanceID == "" {
		return errors.New("evidence write requires an instance_id")
	}
	if strings.TrimSpace(r.Token.InstanceID) == "" {
		return errors.New("evidence write requires a token captured before the result was derived")
	}
	if r.Token.InstanceID != instanceID {
		return fmt.Errorf("captured token belongs to instance %s, not %s", r.Token.InstanceID, instanceID)
	}
	if r.Token.State == "" {
		return errors.New("captured evidence token has no state")
	}
	if r.Token.Generation < 0 {
		return fmt.Errorf("captured evidence generation must be >= 0, got %d", r.Token.Generation)
	}
	if strings.TrimSpace(r.Token.Hash) == "" {
		return errors.New("captured evidence token has no evidence_hash")
	}
	if !r.Kind.Known() {
		return fmt.Errorf("%w: %q (known: %v)", ErrUnknownEvidenceMutation, r.Kind, knownEvidenceMutationKinds)
	}
	if strings.TrimSpace(r.Actor) == "" {
		return errors.New("evidence write actor is required")
	}
	if r.Apply == nil {
		return errors.New("evidence write apply function is required")
	}
	return nil
}

// boundedEvidenceDetail bounds a free-text audit annotation to the 512-byte
// bound (same bound as the 014 audit details), keeping the truncation visible.
func boundedEvidenceDetail(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= evidenceDetailMaxBytes {
		return text
	}
	return text[:evidenceDetailMaxBytes-8] + " [bound]"
}

const evidenceDetailMaxBytes = 512
