// hold.go owns the 008 hold lifecycle on top of the durable scope row: one
// nonce_scope_holds row per cause instance established from a persisted
// observation (evidence_observation_id), the derived admission-held state, and
// the scope frontier facts (monotonic floor via the guarded GREATEST update,
// plus last_latest/last_pending/last_observation_id).
//
// There is no scope-level boolean anywhere: a scope is admission-held iff at
// least one `active` hold row exists (data-model Table 6, R7/FR-11). A
// re-detected cause while its instance is still active converges on that
// instance; a released cause re-detected appends a NEW instance, because a
// released row is immutable history and is never reopened (release itself is
// operator-only, admin.go T014 — this file never releases).
//
// Callers run these helpers inside the T006 write transaction while holding
// the scope row FOR UPDATE, so the read-then-insert establishment is
// serialized per scope; none of these helpers opens a transaction, takes the
// coordination lock, or performs RPC.
package nonce

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
)

// Hold statuses (the nonce_scope_holds.status CHECK domain).
const (
	HoldStatusActive   = "active"
	HoldStatusReleased = "released"
)

// ScopeHold is one nonce_scope_holds row (data-model Table 6): a durable,
// evidence-linked pause for exactly one cause on one (chain_id, sender) scope.
type ScopeHold struct {
	HoldID                string
	ChainID               int64
	Sender                string
	Cause                 string
	Status                string
	EstablishedAt         time.Time
	EvidenceObservationID string
	EvidenceDetail        string
	ReleasedAt            *time.Time
	ReleasedBy            *string
	ReleaseOperationID    *string
	ReleaseEvidence       *string
	ReleaseObservationID  *string
}

// holdCauses is the nonce_scope_holds.cause CHECK domain: 008 causes only
// (006 causes are never represented here).
var holdCauses = map[string]bool{
	CauseUnattributedConsumption: true,
	CauseUnexplainedGap:          true,
	CauseChainViewDivergence:     true,
}

// Hold SQL. Established rows are inserted active and only ever flipped to
// released by the operator path; status_consistency is mirrored by the schema.
const holdColumnsSQL = `
SELECT hold_id, chain_id, sender, cause, status, established_at,
       evidence_observation_id, evidence_detail,
       released_at, released_by, release_operation_id, release_evidence, release_observation_id
FROM nonce_scope_holds`

const (
	readActiveHoldsSQL = holdColumnsSQL + `
WHERE chain_id = $1 AND sender = $2 AND status = 'active'
ORDER BY established_at, hold_id`

	readActiveHoldByCauseSQL = holdColumnsSQL + `
WHERE chain_id = $1 AND sender = $2 AND cause = $3 AND status = 'active'
ORDER BY established_at, hold_id
LIMIT 1`

	insertHoldSQL = `
INSERT INTO nonce_scope_holds (hold_id, chain_id, sender, cause, status,
       evidence_observation_id, evidence_detail)
VALUES ($1, $2, $3, $4, 'active', $5, $6)`

	// scopeIsHeldSQL derives the admission-held state from the row set only:
	// released rows (and the absence of any scope-level boolean) cannot hold.
	scopeIsHeldSQL = `
SELECT EXISTS (
    SELECT 1 FROM nonce_scope_holds
    WHERE chain_id = $1 AND sender = $2 AND status = 'active'
)`

	// updateScopeFrontierSQL records the fresh observation facts. It never
	// moves the floor: floor movement goes through advanceScopeFloorSQL, the
	// only statement carrying the monotonic GREATEST guard.
	updateScopeFrontierSQL = `
UPDATE nonce_scope_state
SET last_latest = $3::numeric,
    last_pending = $4::numeric,
    last_observation_id = $5,
    updated_at = now()
WHERE chain_id = $1 AND sender = $2`

	// advanceScopeFloorSQL advances F monotonically: GREATEST can only move
	// the value up, and the WHERE guard makes RowsAffected == 0 mean "already
	// at or above the proposed floor" (a no-op, never a regression).
	advanceScopeFloorSQL = `
UPDATE nonce_scope_state
SET reconciled_floor = GREATEST(COALESCE(reconciled_floor, 0), $3::numeric),
    updated_at = now()
WHERE chain_id = $1 AND sender = $2
  AND (reconciled_floor IS NULL OR reconciled_floor < $3::numeric)`
)

// scanHold reads one hold row; pgx.ErrNoRows yields (nil, nil).
func scanHold(row pgx.Row) (*ScopeHold, error) {
	var h ScopeHold
	err := row.Scan(&h.HoldID, &h.ChainID, &h.Sender, &h.Cause, &h.Status, &h.EstablishedAt,
		&h.EvidenceObservationID, &h.EvidenceDetail,
		&h.ReleasedAt, &h.ReleasedBy, &h.ReleaseOperationID, &h.ReleaseEvidence, &h.ReleaseObservationID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &h, nil
}

// readActiveHoldByCauseTx returns the scope's active instance for one cause,
// or nil when none is active (a released instance is history, not a match).
func readActiveHoldByCauseTx(ctx context.Context, q txQuerier, chainID int64, sender, cause string) (*ScopeHold, error) {
	h, err := scanHold(q.QueryRow(ctx, readActiveHoldByCauseSQL, chainID, sender, cause))
	if err != nil {
		return nil, fmt.Errorf("read active hold by cause: %w", err)
	}
	return h, nil
}

// readActiveHoldsTx lists every active hold of a scope — the evidence for the
// derived admitted/held state and for read-api annotations. Released rows are
// excluded by the query, so no caller can resurrect a disposed hold.
func readActiveHoldsTx(ctx context.Context, q txQuerier, chainID int64, sender string) ([]ScopeHold, error) {
	rowRows, err := q.Query(ctx, readActiveHoldsSQL, chainID, sender)
	if err != nil {
		return nil, fmt.Errorf("list active holds: %w", err)
	}
	defer rowRows.Close()
	var out []ScopeHold
	for rowRows.Next() {
		h, err := scanHold(rowRows)
		if err != nil {
			return nil, err
		}
		out = append(out, *h)
	}
	if err := rowRows.Err(); err != nil {
		return nil, fmt.Errorf("list active holds: %w", err)
	}
	return out, nil
}

// scopeIsHeldTx reports whether the scope is admission-held: true iff at
// least one active hold row exists. The state is derived per read; nothing
// outside the row set caches or stores it.
func scopeIsHeldTx(ctx context.Context, q txQuerier, chainID int64, sender string) (bool, error) {
	var held bool
	if err := q.QueryRow(ctx, scopeIsHeldSQL, chainID, sender).Scan(&held); err != nil {
		return false, fmt.Errorf("read scope held state: %w", err)
	}
	return held, nil
}

// HoldEstablishment is the establishment input: the scope, the cause the
// classification matrix derived, and the persisted observation that carries
// the evidence. Detail is the redacted classification summary.
type HoldEstablishment struct {
	ChainID       int64
	Sender        string
	Cause         string
	ObservationID string
	Detail        string
}

// establishHoldTx establishes one hold instance for the classification cause:
// an existing active instance for the same cause converges on it untouched; a
// released (or never-established) cause appends a new row with a fresh
// hold_id. Establishment is automatic from a persisted observation — the
// caller never fetches evidence here.
func establishHoldTx(ctx context.Context, q txQuerier, est HoldEstablishment) (*ScopeHold, error) {
	if !holdCauses[est.Cause] {
		return nil, fmt.Errorf("unknown hold cause %q", est.Cause)
	}
	if est.ObservationID == "" {
		return nil, errors.New("hold establishment requires a persisted evidence observation id")
	}
	existing, err := readActiveHoldByCauseTx(ctx, q, est.ChainID, est.Sender, est.Cause)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}
	holdID, err := newHoldID()
	if err != nil {
		return nil, err
	}
	if _, err := q.Exec(ctx, insertHoldSQL, holdID, est.ChainID, est.Sender, est.Cause,
		est.ObservationID, est.Detail); err != nil {
		return nil, fmt.Errorf("insert scope hold: %w", err)
	}
	return &ScopeHold{
		HoldID:                holdID,
		ChainID:               est.ChainID,
		Sender:                est.Sender,
		Cause:                 est.Cause,
		Status:                HoldStatusActive,
		EvidenceObservationID: est.ObservationID,
		EvidenceDetail:        est.Detail,
	}, nil
}

// newHoldID mints an opaque hold id ("nh-" + 32 hex, data-model Table 6) from
// crypto/rand; a mint failure refuses establishment rather than inventing a
// colliding identity.
func newHoldID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("mint hold id: %w", err)
	}
	return "nh-" + hex.EncodeToString(buf[:]), nil
}

// ScopeState is the durable scope frontier row (data-model Table 2) as
// consumed by reconciliation: F plus the last observation facts.
type ScopeState struct {
	ChainID           int64
	Sender            string
	ReconciledFloor   *big.Int
	LastLatest        *big.Int
	LastPending       *big.Int
	LastObservationID *string
	UpdatedAt         time.Time
}

// readScopeStateTx reads the scope row (coord.go readScopeStateSQL); a missing
// row yields (nil, nil) — the caller materialises it with ensureScopeRowTx.
func readScopeStateTx(ctx context.Context, q txQuerier, chainID int64, sender string) (*ScopeState, error) {
	var (
		floorText, latestText, pendingText *string
		st                                 ScopeState
	)
	err := q.QueryRow(ctx, readScopeStateSQL, chainID, sender).Scan(
		&floorText, &latestText, &pendingText, &st.LastObservationID, &st.UpdatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read scope state: %w", err)
	}
	st.ChainID, st.Sender = chainID, sender
	if st.ReconciledFloor, err = parseNullableDecimal("reconciled floor", floorText); err != nil {
		return nil, err
	}
	if st.LastLatest, err = parseNullableDecimal("last_latest", latestText); err != nil {
		return nil, err
	}
	if st.LastPending, err = parseNullableDecimal("last_pending", pendingText); err != nil {
		return nil, err
	}
	return &st, nil
}

// parseNullableDecimal converts a scanned ::text NUMERIC (NULL → nil) back to
// an integer-exact big.Int.
func parseNullableDecimal(field string, text *string) (*big.Int, error) {
	if text == nil {
		return nil, nil
	}
	n, err := ParseDecimal(*text)
	if err != nil {
		return nil, fmt.Errorf("scope %s: %w", field, err)
	}
	return n, nil
}

// updateScopeFrontierTx records the fresh observation facts on the scope row:
// last_latest, last_pending, last_observation_id. The floor is deliberately
// untouched here — only advanceScopeFloorTx moves it.
func updateScopeFrontierTx(ctx context.Context, q txQuerier, chainID int64, sender string, latest, pending *big.Int, observationID string) error {
	if err := ValidateNonceRange(latest); err != nil {
		return fmt.Errorf("scope last_latest: %w", err)
	}
	if err := ValidateNonceRange(pending); err != nil {
		return fmt.Errorf("scope last_pending: %w", err)
	}
	if observationID == "" {
		return errors.New("scope frontier update requires an observation id")
	}
	tag, err := q.Exec(ctx, updateScopeFrontierSQL, chainID, sender,
		NumericValue(latest), NumericValue(pending), observationID)
	if err != nil {
		return fmt.Errorf("update scope frontier: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("scope row missing for frontier update")
	}
	return nil
}

// advanceScopeFloorTx advances the durable reconciled floor monotonically
// (GREATEST guard): it reports whether the floor moved. Zero affected rows is
// a legal no-op — the floor already stood at or above the proposed value (a
// repeat release) — and is never a regression; the caller holds the scope row
// under lock, so it cannot also mean a missing row.
func advanceScopeFloorTx(ctx context.Context, q txQuerier, chainID int64, sender string, floor *big.Int) (bool, error) {
	if err := ValidateNonceRange(floor); err != nil {
		return false, fmt.Errorf("scope reconciled floor: %w", err)
	}
	tag, err := q.Exec(ctx, advanceScopeFloorSQL, chainID, sender, NumericValue(floor))
	if err != nil {
		return false, fmt.Errorf("advance scope floor: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
