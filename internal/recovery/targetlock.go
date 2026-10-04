package recovery

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
)

// TargetKey is a stable, credential-free identity for the supported endpoint
// spelling. It deliberately excludes role and operation information. It does
// not resolve DNS aliases.
type TargetKey [32]byte

// CanonicalTargetKey lowercases the host and hashes a versioned,
// length-prefixed host/port/database tuple. Invalid and unknown identities
// fail closed.
func CanonicalTargetKey(target controlstore.DSNTarget) (TargetKey, error) {
	var zero TargetKey
	encoded, err := controlstore.TargetGuardKey(target)
	if err != nil {
		return zero, err
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(encoded, "sha256:"))
	if err != nil || len(raw) != len(zero) {
		return zero, errors.New("invalid canonical target key")
	}
	copy(zero[:], raw)
	return zero, nil
}

// AdvisoryLockKey maps the target digest to PostgreSQL's two-int advisory
// lock namespace. Collisions only serialize unrelated targets.
func (k TargetKey) AdvisoryLockKey() (int32, int32) {
	return int32(binary.BigEndian.Uint32(k[0:4])), int32(binary.BigEndian.Uint32(k[4:8]))
}

// String is a safe credential-free target key representation.
func (k TargetKey) String() string { return "sha256:" + hex.EncodeToString(k[:]) }

// TargetLock is a session advisory lock held on one dedicated control-store
// connection. It is a live coordination primitive only; it is not a durable
// guard and does not establish target cleanliness.
type TargetLock struct {
	conn       advisoryConn
	key1       int32
	key2       int32
	controlKey TargetKey
	mu         sync.Mutex
	closed     bool
}

type advisoryConn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Ping(context.Context) error
	Close(context.Context) error
}

// AcquireTargetLock establishes a dedicated pgx session and repeatedly
// attempts the session-level lock until acquired or timeout/context expiry. A
// positive timeout is mandatory, and pollInterval <= 0 selects 50ms. No pool
// connection is used because the lock lifetime must equal the owning session
// lifetime.
func AcquireTargetLock(ctx context.Context, controlDSN string, key TargetKey, timeout, pollInterval time.Duration) (*TargetLock, error) {
	if ctx == nil || controlDSN == "" || key == (TargetKey{}) || timeout <= 0 {
		return nil, errors.New("context, control-store DSN, known target key, and positive acquisition timeout are required")
	}
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := connectTargetLockSession(bounded, controlDSN)
	if err != nil {
		return nil, fmt.Errorf("connect dedicated target-lock session: %w", err)
	}
	dsnTarget, err := controlstore.ParseDSNTarget(controlDSN)
	if err != nil {
		_ = conn.Close(context.Background())
		return nil, errors.New("target-lock control-store identity is invalid")
	}
	connectedTarget := controlstore.DSNTarget{
		Host: conn.Config().Host, Port: conn.Config().Port,
		Database: conn.Config().Database, Role: conn.Config().User,
	}
	dsnKey, err := CanonicalTargetKey(dsnTarget)
	if err != nil {
		_ = conn.Close(context.Background())
		return nil, errors.New("target-lock control-store identity is unknown")
	}
	connectedKey, err := CanonicalTargetKey(connectedTarget)
	if err != nil || connectedKey != dsnKey {
		_ = conn.Close(context.Background())
		return nil, errors.New("target-lock connected control-store identity does not match its DSN")
	}
	k1, k2 := key.AdvisoryLockKey()
	lock := &TargetLock{conn: conn, key1: k1, key2: k2, controlKey: connectedKey}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var acquired bool
		if err := conn.QueryRow(bounded, `SELECT pg_try_advisory_lock($1, $2)`, k1, k2).Scan(&acquired); err != nil {
			_ = conn.Close(context.Background())
			return nil, fmt.Errorf("attempt target advisory lock: %w", err)
		}
		if acquired {
			if err := lock.Health(bounded); err != nil {
				_ = conn.Close(context.Background())
				return nil, fmt.Errorf("verify acquired target advisory lock: %w", err)
			}
			return lock, nil
		}
		select {
		case <-bounded.Done():
			_ = conn.Close(context.Background())
			return nil, fmt.Errorf("bounded target advisory lock acquisition: %w", bounded.Err())
		case <-ticker.C:
		}
	}
}

// acquireHealthMutex serializes Health with WithTransaction and Release using
// the same lock mutex, but never blocks past the caller context: TryLock is
// polled on a bounded ticker and the wait is abandoned as soon as ctx ends. It
// starts no goroutine, so a caller deadline can never leave a waiter behind
// that would later steal serialization, and no SQL runs while waiting.
func (l *TargetLock) acquireHealthMutex(ctx context.Context) error {
	if ctx == nil {
		return errors.New("target lock health requires a context")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("target lock health context is already done: %w", err)
	}
	if l.mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("target lock health could not acquire serialization before context end: %w", ctx.Err())
		case <-ticker.C:
			if l.mu.TryLock() {
				return nil
			}
		}
	}
}

// Health verifies both that the dedicated control connection responds and
// that this exact session still owns the expected advisory lock. The mutex
// acquisition is context-aware: a nil context, an already-done context, or a
// context that ends while another operation holds serialization refuses with
// a fixed safe stage that wraps the context cause (errors.Is works) before any
// SQL call and before the holder is released. Without a deadline a caller may
// wait indefinitely, matching the conventional context contract; production
// callers cap the wait (the control anchor caps at 3s with a 1s health
// interval).
func (l *TargetLock) Health(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return errors.New("target lock is unknown")
	}
	if err := l.acquireHealthMutex(ctx); err != nil {
		return err
	}
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("target lock health context ended before the health check: %w", err)
	}
	closed := l.closed
	if closed {
		return errors.New("target lock session is closed")
	}
	if err := l.conn.Ping(ctx); err != nil {
		return fmt.Errorf("target lock control session health: %w", err)
	}
	var held bool
	err := l.conn.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted
    AND pid = pg_backend_pid() AND objsubid = 2
    AND classid = $1::oid AND objid = $2::oid
)`, uint32(l.key1), uint32(l.key2)).Scan(&held)
	if err != nil {
		return fmt.Errorf("check target advisory lock ownership: %w", err)
	}
	if !held {
		return errors.New("target advisory lock ownership was lost")
	}
	return nil
}

// WithTransaction runs callback in a transaction on the exact dedicated
// session holding this TargetLock's session advisory lock. Health and Release
// are serialized behind the callback and commit. Callback code must use only
// the supplied pgx.Tx (not another connection) and must not manipulate session
// advisory locks. A callback error is rolled back. A commit error is returned
// as-is wrapped with context: its outcome may be ambiguous and callers must
// never infer success from it. A successful return means the commit succeeded;
// releasing the lock is a separate operation and can fail independently.
func (l *TargetLock) WithTransaction(ctx context.Context, callback func(context.Context, pgx.Tx) error) error {
	if l == nil || l.conn == nil {
		return errors.New("target lock is unknown")
	}
	if ctx == nil || callback == nil {
		return errors.New("transaction context and callback are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("target lock session is closed")
	}
	beginner, ok := l.conn.(interface {
		BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	})
	if !ok {
		return errors.New("target lock session does not support transactions")
	}
	tx, err := beginner.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin target-lock acceptance transaction: %w", err)
	}
	rollback := func(cause error) error {
		// Independent bounded rollback context: an already cancelled caller
		// must not strand the owner transaction, and the cleanup never uses the
		// unbounded background close of Release.
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), targetLockRollbackBudget)
		rollbackErr := tx.Rollback(rollbackCtx)
		cancelRollback()
		if rollbackErr != nil {
			// pgx.ErrTxClosed is NOT an acknowledged rollback of THIS exact
			// transaction: a callback that finalized the transaction early (for
			// example committed it through the supplied tx) and still returned
			// an error lands here. Unless an acknowledged rollback of that exact
			// transaction was separately established (it is not here), the
			// uncertainty is conservatively reported and the owner is
			// permanently retired with a bounded disposal whose errors are
			// joined into the returned error.
			disposalErr := l.retireOwnerAfterUncertainRollbackLocked()
			return errors.Join(cause, fmt.Errorf("rollback target-lock acceptance transaction: %w", rollbackErr), disposalErr)
		}
		return cause
	}
	if err := l.checkHeldInTx(ctx, tx); err != nil {
		return rollback(err)
	}
	if err := callback(ctx, tx); err != nil {
		return rollback(fmt.Errorf("target-lock acceptance callback: %w", err))
	}
	// Session advisory locks survive transaction rollback. Check immediately
	// before commit so a callback that accidentally released the lock cannot
	// report an accepted transaction.
	if err := l.checkHeldInTx(ctx, tx); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit target-lock acceptance transaction (outcome may be ambiguous): %w", err)
	}
	return nil
}

func (l *TargetLock) checkHeldInTx(ctx context.Context, tx pgx.Tx) error {
	var held bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE locktype = 'advisory' AND granted
    AND pid = pg_backend_pid() AND objsubid = 2
    AND classid = $1::oid AND objid = $2::oid
)`, uint32(l.key1), uint32(l.key2)).Scan(&held)
	if err != nil {
		return fmt.Errorf("check target advisory lock ownership in transaction: %w", err)
	}
	if !held {
		return errors.New("target advisory lock ownership was lost")
	}
	return nil
}

// Release unlocks and closes the dedicated session. Any uncertainty is
// returned; connection closure itself causes PostgreSQL to release the lock.
func (l *TargetLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return errors.New("target lock is unknown")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var unlocked bool
	err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1, $2)`, l.key1, l.key2).Scan(&unlocked)
	closeErr := l.conn.Close(context.Background())
	if err != nil {
		return fmt.Errorf("release target advisory lock: %w", err)
	}
	if !unlocked {
		return errors.New("target advisory lock was not owned at release")
	}
	if closeErr != nil {
		return fmt.Errorf("close target-lock session: %w", closeErr)
	}
	return nil
}

const maxApplicationNameBytes = 63

// ValidateAttemptApplicationName rejects empty, control-containing, invalid
// UTF-8, and overlong values so PostgreSQL cannot silently truncate the tag.
func ValidateAttemptApplicationName(name string) error {
	if name == "" || len(name) > maxApplicationNameBytes || !utf8.ValidString(name) || strings.ContainsAny(name, "\x00\r\n") {
		return fmt.Errorf("application_name must be valid UTF-8, 1..%d bytes, and contain no NUL/CR/LF", maxApplicationNameBytes)
	}
	return nil
}

// NewAttemptApplicationName returns a cryptographically random, PostgreSQL
// bounded correlation tag. It is not an authenticated identity.
func NewAttemptApplicationName() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("create attempt application name: %w", err)
	}
	name := "txh015_" + hex.EncodeToString(nonce[:])
	return name, ValidateAttemptApplicationName(name)
}

// ConninfoWithAttemptApplicationName ensures the supplied child conninfo
// carries exactly the bounded attempt tag. Existing options are preserved.
func ConninfoWithAttemptApplicationName(dsn, name string) (string, error) {
	if err := ValidateAttemptApplicationName(name); err != nil {
		return "", err
	}
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return "", fmt.Errorf("parse child PostgreSQL conninfo: %w", err)
	}
	if strings.HasPrefix(strings.ToLower(dsn), "postgres://") || strings.HasPrefix(strings.ToLower(dsn), "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse child PostgreSQL URI: %w", err)
		}
		query := u.Query()
		query.Set("application_name", name)
		u.RawQuery = query.Encode()
		return u.String(), nil
	}
	options, err := parsePGKeywordDSN(dsn)
	if err != nil {
		return "", errors.New("parse child PostgreSQL keyword conninfo")
	}
	found := false
	for i := range options {
		if options[i].key != "application_name" {
			continue
		}
		if found {
			return "", errors.New("duplicate application_name in child PostgreSQL conninfo")
		}
		options[i].value = name
		found = true
	}
	if !found {
		options = append(options, pgConnOption{key: "application_name", value: name})
	}
	return formatPGKeywordDSN(options), nil
}

// CheckTargetQuiescent performs one fail-closed target observation. It first
// proves the observer is superuser or a member of pg_read_all_stats, then
// counts exact application_name matches in pg_stat_activity. A bounded caller
// should repeat this under its own deadline until true or timeout.
func CheckTargetQuiescent(ctx context.Context, targetDSN, appName string) (bool, error) {
	if ctx == nil {
		return false, errors.New("target observer context is unknown")
	}
	if err := ValidateAttemptApplicationName(appName); err != nil {
		return false, err
	}
	if strings.TrimSpace(targetDSN) == "" {
		return false, errors.New("target observer DSN is unknown")
	}
	conn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		return false, fmt.Errorf("connect target observer: %w", err)
	}
	defer conn.Close(context.Background())
	var visible bool
	if err := conn.QueryRow(ctx, `SELECT rolsuper OR pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') FROM pg_roles WHERE rolname = current_user`).Scan(&visible); err != nil {
		return false, fmt.Errorf("check target observer visibility: %w", err)
	}
	if !visible {
		return false, errors.New("target observer lacks pg_read_all_stats visibility")
	}
	var matches int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, appName).Scan(&matches); err != nil {
		return false, fmt.Errorf("observe target application sessions: %w", err)
	}
	return matches == 0, nil
}

// WaitTargetQuiescent polls the visibility-checked observer within timeout.
// Timeout, query/visibility errors, or an unknown attempt tag fail closed.
func WaitTargetQuiescent(ctx context.Context, targetDSN, appName string, timeout, interval time.Duration) error {
	if ctx == nil || timeout <= 0 {
		return errors.New("observer context and positive quiescence timeout are required")
	}
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		quiet, err := CheckTargetQuiescent(bounded, targetDSN, appName)
		if err != nil {
			return err
		}
		if quiet {
			return nil
		}
		select {
		case <-bounded.Done():
			return fmt.Errorf("target application sessions did not quiesce before deadline: %w", bounded.Err())
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Drill-only owner-session socket wrap seam
// ---------------------------------------------------------------------------

// targetLockSocketWrap is the unexported fixed wrapper signature of the
// drill-only seam. It is never exported and never accepts a caller-supplied
// socket callback: only the drill companion (same package, build-tagged test
// file) reserves it with its fixed fault controls.
type targetLockSocketWrap func(net.Conn) net.Conn

var (
	targetLockSocketWrapMu     sync.Mutex
	targetLockSocketWrapGen    uint64
	targetLockSocketWrapActive bool
	targetLockSocketWrapFn     targetLockSocketWrap
)

// reserveTargetLockSocketWrap atomically reserves the SINGLE drill-only socket
// wrap installation for the NEXT dedicated owner-session connection
// establishment, refusing an overlapping reservation. The returned reset is
// conditional on THIS installation identity, so a stale reset can never clear a
// newer installation.
func reserveTargetLockSocketWrap(wrap targetLockSocketWrap) (func(), error) {
	if wrap == nil {
		return nil, errors.New("target-lock socket wrap is required")
	}
	targetLockSocketWrapMu.Lock()
	defer targetLockSocketWrapMu.Unlock()
	if targetLockSocketWrapActive {
		return nil, errors.New("target-lock socket wrap is already reserved")
	}
	targetLockSocketWrapActive = true
	targetLockSocketWrapFn = wrap
	targetLockSocketWrapGen++
	generation := targetLockSocketWrapGen
	return func() {
		targetLockSocketWrapMu.Lock()
		defer targetLockSocketWrapMu.Unlock()
		if !targetLockSocketWrapActive || targetLockSocketWrapGen != generation {
			return
		}
		targetLockSocketWrapActive = false
		targetLockSocketWrapFn = nil
	}, nil
}

// consumeTargetLockSocketWrap atomically consumes the reserved installation for
// ONE connection establishment; every later acquisition is unaffected.
func consumeTargetLockSocketWrap() targetLockSocketWrap {
	targetLockSocketWrapMu.Lock()
	defer targetLockSocketWrapMu.Unlock()
	if !targetLockSocketWrapActive {
		return nil
	}
	wrap := targetLockSocketWrapFn
	targetLockSocketWrapActive = false
	targetLockSocketWrapFn = nil
	return wrap
}

// connectTargetLockSession establishes the dedicated owner session through the
// EXISTING pgx.Connect path unless the drill-only socket wrap seam is reserved.
// A consumed reservation wraps the REAL established socket of the unchanged
// control DSN exactly once; no DSN, key, owner, identity check or acquisition
// step is replaced.
func connectTargetLockSession(ctx context.Context, controlDSN string) (*pgx.Conn, error) {
	wrap := consumeTargetLockSocketWrap()
	if wrap == nil {
		return pgx.Connect(ctx, controlDSN)
	}
	config, err := pgx.ParseConfig(controlDSN)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{}
	config.DialFunc = func(dialCtx context.Context, network, addr string) (net.Conn, error) {
		rawConn, dialErr := dialer.DialContext(dialCtx, network, addr)
		if dialErr != nil {
			return nil, dialErr
		}
		wrapped := wrap(rawConn)
		if wrapped == nil {
			_ = rawConn.Close()
			return nil, errors.New("target-lock socket wrap returned no connection")
		}
		return wrapped, nil
	}
	return pgx.ConnectConfig(ctx, config)
}

// targetLockRollbackBudget is the independent bounded budget of the rollback
// cleanup. The caller context is never used for this cleanup: an already
// cancelled caller must not strand the owner transaction.
const targetLockRollbackBudget = 10 * time.Second

// targetLockDisposalBudget is the independent bounded budget of the
// uncertain-rollback connection disposal; it is SEPARATE from (never combined
// with) the rollback budget.
const targetLockDisposalBudget = 10 * time.Second

// retireOwnerAfterUncertainRollbackLocked permanently retires the owner handle
// after an uncertain rollback and boundedly disposes its dedicated connection,
// returning any disposal error so the caller can join it. A nil return means
// the bounded disposal was CONFIRMED, never merely attempted. It deliberately
// does not use the Release SQL path and never performs an unbounded close; the
// caller must hold l.mu.
func (l *TargetLock) retireOwnerAfterUncertainRollbackLocked() error {
	if l.closed {
		return nil
	}
	l.closed = true
	if l.conn == nil {
		return nil
	}
	disposeCtx, cancelDispose := context.WithTimeout(context.Background(), targetLockDisposalBudget)
	defer cancelDispose()
	if err := l.conn.Close(disposeCtx); err != nil {
		return fmt.Errorf("dispose retired target-lock session (bounded disposal attempt, closure not confirmed): %w", err)
	}
	return nil
}
