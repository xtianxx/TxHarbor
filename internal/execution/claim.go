package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrClaimLost means this holder no longer has a valid qualification: the row
// is missing, owned by another instance, the version moved, or it is expired
// on the DB clock. The holder MUST stop writing immediately and re-observe.
var ErrClaimLost = errors.New("execution claim lost")

// acquireClaimSQL is the single atomic CAS (persistence.md §3.1): claimable =
// state <> 'active' OR expires_at <= now() OR now() - last_progress_at >=
// stall_window. Losers serialize on the row and re-evaluate the predicate
// against the winner's committed row, so at most one version is active.
const acquireClaimSQL = `INSERT INTO execution_claims
    (intent_id, owner_id, lease_version, state, acquired_at, expires_at, last_heartbeat_at, last_progress_at)
  VALUES ($1, $2, 1, 'active', now(), now() + make_interval(secs => $3), now(), now())
  ON CONFLICT (intent_id) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        lease_version = execution_claims.lease_version + 1,
        state = 'active',
        acquired_at = now(),
        expires_at = EXCLUDED.expires_at,
        last_heartbeat_at = now(),
        last_progress_at = now(),
        stall_flagged_at = NULL,
        ended_at = NULL,
        end_kind = NULL,
        updated_at = now()
    WHERE execution_claims.state <> 'active'
       OR execution_claims.expires_at <= now()
       OR now() - execution_claims.last_progress_at >= make_interval(secs => $4)
  RETURNING lease_version`

const lockClaimSQL = `SELECT owner_id, lease_version, state FROM execution_claims
  WHERE intent_id = $1 FOR UPDATE`

const renewClaimSQL = `UPDATE execution_claims
  SET expires_at = now() + make_interval(secs => $4), last_heartbeat_at = now(), updated_at = now()
  WHERE intent_id = $1 AND owner_id = $2 AND lease_version = $3
    AND state = 'active' AND expires_at > now()`

const advanceProgressSQL = `UPDATE execution_claims
  SET last_progress_at = now(), updated_at = now()
  WHERE intent_id = $1 AND owner_id = $2 AND lease_version = $3
    AND state = 'active' AND expires_at > now()`

const releaseClaimSQL = `UPDATE execution_claims
  SET state = 'released', ended_at = now(), end_kind = 'released', updated_at = now()
  WHERE intent_id = $1 AND owner_id = $2 AND lease_version = $3 AND state = 'active'`

const readClaimSQL = `SELECT intent_id, owner_id, lease_version, state, expires_at,
  last_heartbeat_at, last_progress_at, stall_flagged_at, ended_at, end_kind
  FROM execution_claims WHERE intent_id = $1`

const claimIsCurrentSQL = `SELECT EXISTS (SELECT 1 FROM execution_claims
  WHERE intent_id = $1 AND owner_id = $2 AND lease_version = $3
    AND state = 'active' AND expires_at > now())`

// Claim is one execution_claims row.
type Claim struct {
	IntentID        string
	OwnerID         string
	LeaseVersion    int64
	State           string
	ExpiresAt       time.Time
	LastHeartbeatAt time.Time
	LastProgressAt  time.Time
	StallFlaggedAt  *time.Time
	EndedAt         *time.Time
	EndKind         *string
}

// ClaimStore owns the claim/lease core for one worker's TTL and stall window.
// Every expiry/validity comparison happens on the DB clock.
type ClaimStore struct {
	pool        *pgxpool.Pool
	ttl         time.Duration
	stallWindow time.Duration
}

// NewClaimStore validates the qualification timings: TTL and the independent
// stall window are positive and the stall window strictly exceeds the TTL.
func NewClaimStore(pool *pgxpool.Pool, ttl, stallWindow time.Duration) (*ClaimStore, error) {
	if pool == nil {
		return nil, errors.New("claim store: nil pool")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("claim store: ttl %s must be > 0", ttl)
	}
	if stallWindow <= 0 {
		return nil, fmt.Errorf("claim store: stall window %s must be > 0", stallWindow)
	}
	if stallWindow <= ttl {
		return nil, fmt.Errorf("claim store: stall window %s must be > ttl %s (independent value)", stallWindow, ttl)
	}
	return &ClaimStore{pool: pool, ttl: ttl, stallWindow: stallWindow}, nil
}

// Acquire attempts the atomic claim CAS. It returns (0, false, nil) when
// another active, unexpired, non-stalled qualification holds the row.
func (s *ClaimStore) Acquire(ctx context.Context, intentID, ownerID string) (int64, bool, error) {
	var version int64
	err := s.pool.QueryRow(ctx, acquireClaimSQL, intentID, ownerID,
		s.ttl.Seconds(), s.stallWindow.Seconds()).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("acquire execution claim: %w", err)
	}
	return version, true, nil
}

// Renew extends the qualification with the lock-first short transaction. Any
// owner/version/state/expiry mismatch (on the DB clock) is ErrClaimLost.
func (s *ClaimStore) Renew(ctx context.Context, intentID, ownerID string, leaseVersion int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin claim renewal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, GateStatementTimeout); err != nil {
		return fmt.Errorf("claim renewal statement guard: %w", err)
	}
	var (
		owner   string
		version int64
		state   string
	)
	err = tx.QueryRow(ctx, lockClaimSQL, intentID).Scan(&owner, &version, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: claim row missing", ErrClaimLost)
	}
	if err != nil {
		return fmt.Errorf("lock claim for renewal: %w", err)
	}
	if owner != ownerID || version != leaseVersion || state != "active" {
		return fmt.Errorf("%w: owner %q version %d state %q", ErrClaimLost, owner, version, state)
	}
	tag, err := tx.Exec(ctx, renewClaimSQL, intentID, ownerID, leaseVersion, s.ttl.Seconds())
	if err != nil {
		return fmt.Errorf("renew execution claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: renewal matched no unexpired active row", ErrClaimLost)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit claim renewal: %w", err)
	}
	return nil
}

// AdvanceProgress moves the progress watermark with a claim-fenced exact-version
// update. Renewals, re-claims, backoff and legitimate waiting are NOT progress;
// only durable facts (step issue/converge, authority revision, reconcile
// observation, state transition) may call it.
func (s *ClaimStore) AdvanceProgress(ctx context.Context, intentID, ownerID string, leaseVersion int64) error {
	tag, err := s.pool.Exec(ctx, advanceProgressSQL, intentID, ownerID, leaseVersion)
	if err != nil {
		return fmt.Errorf("advance claim progress: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: progress write fenced", ErrClaimLost)
	}
	return nil
}

// Release marks the qualification released on graceful exit. A mismatch is
// ErrClaimLost; nothing is chased.
func (s *ClaimStore) Release(ctx context.Context, intentID, ownerID string, leaseVersion int64) error {
	tag, err := s.pool.Exec(ctx, releaseClaimSQL, intentID, ownerID, leaseVersion)
	if err != nil {
		return fmt.Errorf("release execution claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: release fenced", ErrClaimLost)
	}
	return nil
}

// ReadClaim reads one claim row; found=false when it does not exist.
func ReadClaim(ctx context.Context, q Queryer, intentID string) (Claim, bool, error) {
	var c Claim
	err := q.QueryRow(ctx, readClaimSQL, intentID).Scan(
		&c.IntentID, &c.OwnerID, &c.LeaseVersion, &c.State, &c.ExpiresAt,
		&c.LastHeartbeatAt, &c.LastProgressAt, &c.StallFlaggedAt, &c.EndedAt, &c.EndKind)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, fmt.Errorf("read execution claim: %w", err)
	}
	return c, true, nil
}

// ClaimIsCurrent is the read-only fencing predicate (owner + version + active +
// unexpired, DB clock). Send-enabling paths call it before applying a
// transition.
func ClaimIsCurrent(ctx context.Context, q Queryer, intentID, ownerID string, leaseVersion int64) (bool, error) {
	var ok bool
	if err := q.QueryRow(ctx, claimIsCurrentSQL, intentID, ownerID, leaseVersion).Scan(&ok); err != nil {
		return false, fmt.Errorf("verify execution claim: %w", err)
	}
	return ok, nil
}
