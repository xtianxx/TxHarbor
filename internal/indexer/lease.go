// Package indexer implements the chain indexer's coordination. This file owns
// the single-master lease (data-model.md Table 3, research.md R2): an
// expiry-based CAS acquisition with a monotonic fencing token, a lock-first
// short renewal transaction, and a jittered heartbeat loop.
//
// Lease validity is always decided by the database clock (now()), never by the
// application clock; the application only decides when to try next.
package indexer

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultTTL and DefaultHeartbeat are the production cadence: heartbeat
	// every ~1/3 TTL, no new environment variables (data-model.md Table 3).
	DefaultTTL       = 15 * time.Second
	DefaultHeartbeat = 5 * time.Second

	// dbTimeout bounds every lease statement and transaction so a stalled pool
	// cannot hang acquisition or renewal without a deadline.
	dbTimeout = 5 * time.Second

	// jitterDiv is the heartbeat jitter divisor: wait heartbeat ± heartbeat/10,
	// so instances that acquired together do not renew in lockstep.
	jitterDiv = 10

	// renewLockGuard bounds the renewal transaction's point statements. The
	// guard is fixed rather than TTL-derived so injected short test TTLs cannot
	// time out a healthy local statement.
	renewLockGuard = "SET LOCAL statement_timeout = '5s'"
)

// ErrLeaseLost means this instance no longer holds the lease: the row is
// missing, owned by another instance, or the renewal updated zero rows. Any
// caller seeing it must stop writing immediately and re-read state.
var ErrLeaseLost = errors.New("indexer lease lost")

// acquireSQL is the atomic compare-and-swap acquisition (data-model.md Table 3):
// insert when the chain has no coordination row, otherwise take over only when
// the existing lease expired by DB time, bumping the fencing token. Concurrent
// callers serialize on the row: the loser re-evaluates the WHERE against the
// winner's committed row and gets no row back. The winner reads the new token
// back from RETURNING, so the token+1 is visible only to it.
const acquireSQL = `
INSERT INTO indexer_lease (chain_id, owner_id, expires_at, updated_at)
VALUES ($1, $2, now() + make_interval(secs => $3), now())
ON CONFLICT (chain_id) DO UPDATE
    SET owner_id      = EXCLUDED.owner_id,
        expires_at    = EXCLUDED.expires_at,
        fencing_token = indexer_lease.fencing_token + 1,
        updated_at    = now()
    WHERE indexer_lease.expires_at < now()
RETURNING fencing_token`

// lockLeaseSQL takes the coordination row lock and re-reads the current owner.
const lockLeaseSQL = `
SELECT owner_id FROM indexer_lease WHERE chain_id = $1 FOR UPDATE`

// renewSQL extends the lease on the DB clock. It is only issued after the
// owner check above, inside the same transaction.
const renewSQL = `
UPDATE indexer_lease
SET expires_at = now() + make_interval(secs => $2), updated_at = now()
WHERE chain_id = $1`

// Params configures one lease instance. TTL and Heartbeat default to the
// production values when zero; tests inject short ones.
type Params struct {
	OwnerID   string
	TTL       time.Duration
	Heartbeat time.Duration
}

// Lease is one instance's handle on a chain's indexer_lease row. The zero
// value is not usable; construct with NewLease. It is safe for concurrent use.
type Lease struct {
	pool      *pgxpool.Pool
	chainID   int64
	ownerID   string
	ttl       time.Duration
	heartbeat time.Duration
	token     atomic.Int64
}

// NewLease validates the parameters and builds a lease handle. It performs no
// I/O; call Acquire to attempt to win the lease.
func NewLease(pool *pgxpool.Pool, chainID int64, p Params) (*Lease, error) {
	if pool == nil {
		return nil, errors.New("indexer lease: nil pool")
	}
	if chainID <= 0 {
		return nil, fmt.Errorf("indexer lease: chain id %d must be > 0", chainID)
	}
	if p.OwnerID == "" {
		return nil, errors.New("indexer lease: owner id must not be empty")
	}
	ttl, heartbeat := p.TTL, p.Heartbeat
	if ttl == 0 {
		ttl = DefaultTTL
	}
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeat
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("indexer lease: ttl %s must be > 0", ttl)
	}
	if heartbeat <= 0 {
		return nil, fmt.Errorf("indexer lease: heartbeat %s must be > 0", heartbeat)
	}
	if heartbeat >= ttl {
		return nil, fmt.Errorf("indexer lease: heartbeat %s must be < ttl %s", heartbeat, ttl)
	}
	return &Lease{
		pool:      pool,
		chainID:   chainID,
		ownerID:   p.OwnerID,
		ttl:       ttl,
		heartbeat: heartbeat,
	}, nil
}

// NewOwnerID generates a startup-unique instance identity.
func NewOwnerID() (string, error) {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate lease owner id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Token returns the fencing token from the last successful Acquire (0 before
// any win). Write transactions must present it for the owner/token check.
func (l *Lease) Token() int64 { return l.token.Load() }

// Acquire attempts to win the lease with one atomic CAS. Takeover uses this
// same path: it only succeeds when the current row has expired according to
// the database clock. It returns (false, 0, nil) when another instance holds
// an unexpired lease, and the new fencing token when it wins.
func (l *Lease) Acquire(ctx context.Context) (bool, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var token int64
	err := l.pool.QueryRow(ctx, acquireSQL, l.chainID, l.ownerID, l.ttl.Seconds()).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("acquire indexer lease: %w", err)
	}
	l.token.Store(token)
	return true, token, nil
}

// Renew extends the lease with the lock-first protocol (data-model.md Table 3):
// BEGIN, SELECT ... FOR UPDATE on the coordination row, verify owner == me,
// extend expiry, COMMIT. The transaction holds no RPC and only point
// statements. A missing row or an owner mismatch is ErrLeaseLost; the caller
// must stop writing. Any error means the lease is unconfirmed, so callers
// treat it the same way until a fresh Acquire succeeds.
func (l *Lease) Renew(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin lease renewal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT

	if _, err := tx.Exec(ctx, renewLockGuard); err != nil {
		return fmt.Errorf("lease renewal statement guard: %w", err)
	}

	var owner string
	err = tx.QueryRow(ctx, lockLeaseSQL, l.chainID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: lease row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock lease for renewal: %w", err)
	}
	if owner != l.ownerID {
		return fmt.Errorf("%w: owner is %q, want %q", ErrLeaseLost, owner, l.ownerID)
	}

	tag, err := tx.Exec(ctx, renewSQL, l.chainID, l.ttl.Seconds())
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: renewal updated 0 rows", ErrLeaseLost)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit lease renewal: %w", err)
	}
	return nil
}

// Heartbeat renews the lease until ctx is done or the lease is lost. It
// returns nil on cancellation (a stopped heartbeat simply lets the lease
// expire; validity is never judged on the app clock) and an error wrapping
// ErrLeaseLost on the first renewal that does not confirm ownership, so loss
// surfaces before the next write attempt.
func (l *Lease) Heartbeat(ctx context.Context) error {
	timer := time.NewTimer(l.nextDelay())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		if err := l.Renew(ctx); err != nil {
			return err
		}
		timer.Reset(l.nextDelay())
	}
}

// nextDelay is the heartbeat interval with ±1/jitterDiv jitter.
func (l *Lease) nextDelay() time.Duration {
	span := l.heartbeat / jitterDiv
	if span <= 0 {
		return l.heartbeat
	}
	return l.heartbeat - span + time.Duration(mrand.Int64N(int64(2*span)+1))
}
