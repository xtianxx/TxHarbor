// logscanner.go implements the 003 event-log scanner: continuous closed-range
// eth_getLogs queries over the configured ERC-20 whitelist, 8-point per-log
// validation, incomplete-result shrinking, and an interval-atomic commit that
// advances log_checkpoint under the shared indexer_lease coordination row. It
// never touches the 002 header progress or pause rows. Behavior is locked by
// specs/003-event-indexing/ (FR-01..FR-17, data-model.md write-transaction
// protocol, research R1/R2/R4/R6).
//
// ServeLoop performs no lease acquisition or heartbeat: the 003 coordinator
// owns both (research R1) and drives the header and log loops concurrently.
package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/logx"
)

// Pause kinds, matching the log_pause.kind CHECK constraint (data-model Table 3).
const (
	pauseChainViewChanged = "chain_view_changed"
	pauseValidationFailed = "validation_failed"
	pauseRangeIncomplete  = "range_incomplete"
)

// Machine-readable validation classes carried in log_pause.detail (data-model
// Table 3, plan.md FR-07 checklist).
const (
	classBadAddress       = "bad_address"
	classBadTopics        = "bad_topics"
	classBadData          = "bad_data"
	classRemoved          = "removed"
	classOutOfRange       = "out_of_range"
	classMissingField     = "missing_field"
	classIdentityConflict = "identity_conflict"
)

// maxValidationAttempts is the number of deterministic validation failures on
// the same range start before the worker stops and persists validation_failed:
// canonical data is immutable, so an identical re-query failure is not
// transient (data-model Table 3).
const maxValidationAttempts = 3

// LogConfig carries the log scanner's runtime knobs and its immutable
// configuration identity (FR-05, research R6). ResultLimit 0 disables the
// count-based completeness verdict: no provider limit is confirmed yet, so no
// arbitrary threshold is applied (research R5, FR-12).
type LogConfig struct {
	StartBlock   uint64
	Contracts    []string
	ConfigHash   string
	BatchBlocks  uint64
	RPCTimeout   time.Duration
	PollInterval time.Duration
	RetryInitial time.Duration
	RetryMax     time.Duration
	ResultLimit  uint64
}

// LogsClient is the minimal log read surface the scanner needs; *eth.Client
// satisfies it. HeaderClient (scanner.go) is reused for the end-block checks.
type LogsClient interface {
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

// LogProgress is the read-only mirror of one log_checkpoint row.
type LogProgress struct {
	StartBlock uint64
	ConfigHash string
	NextBlock  uint64
}

// logPauseInfo describes one pause row to persist.
type logPauseInfo struct {
	from   uint64 // range start the evidence is anchored to
	first  bool   // no log_checkpoint row existed for this range
	height uint64 // reported height (problem block or range start)
	kind   string
	detail string
}

// LogScanner advances one chain's ERC-20 Transfer log index. ServeLoop is the
// only entry point; it may be called once per lease win. State, Checkpoint and
// LogProgress are safe for concurrent readers (metrics).
type LogScanner struct {
	pool   *pgxpool.Pool
	client HeaderClient
	logs   LogsClient
	lease  *Lease
	cfg    LogConfig
	logger *slog.Logger

	chainID   int64
	whitelist map[string]struct{} // normalized lowercase 0x addresses
	addresses []common.Address

	mu     sync.RWMutex
	state  State
	reason string
	prog   *LogProgress
}

// NewLogScanner validates the dependencies and builds a log scanner. It
// performs no I/O; ServeLoop does all connecting. An empty whitelist is a
// configuration error and never degrades into a full-chain query (FR-04, I5).
func NewLogScanner(pool *pgxpool.Pool, client HeaderClient, logs LogsClient, lease *Lease, cfg LogConfig, logger *slog.Logger) (*LogScanner, error) {
	if pool == nil {
		return nil, errors.New("log scanner: nil pool")
	}
	if client == nil {
		return nil, errors.New("log scanner: nil chain client")
	}
	if logs == nil {
		return nil, errors.New("log scanner: nil logs client")
	}
	if lease == nil {
		return nil, errors.New("log scanner: nil lease")
	}
	if len(cfg.Contracts) == 0 {
		return nil, errors.New("log scanner: empty contract whitelist; refusing to scan")
	}
	whitelist := make(map[string]struct{}, len(cfg.Contracts))
	addresses := make([]common.Address, 0, len(cfg.Contracts))
	for _, c := range cfg.Contracts {
		if !common.IsHexAddress(c) {
			return nil, fmt.Errorf("log scanner: contract %q is not a 20-byte EVM address", c)
		}
		addr := common.HexToAddress(c)
		norm := strings.ToLower(addr.Hex())
		if _, dup := whitelist[norm]; dup {
			continue
		}
		whitelist[norm] = struct{}{}
		addresses = append(addresses, addr)
	}
	if !validConfigHash(cfg.ConfigHash) {
		return nil, fmt.Errorf("log scanner: config hash %q is not 64 lowercase hex", cfg.ConfigHash)
	}
	if cfg.BatchBlocks == 0 {
		return nil, errors.New("log scanner: batch blocks must be > 0")
	}
	if cfg.RPCTimeout <= 0 {
		return nil, fmt.Errorf("log scanner: rpc timeout %s must be > 0", cfg.RPCTimeout)
	}
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("log scanner: poll interval %s must be > 0", cfg.PollInterval)
	}
	if cfg.RetryInitial <= 0 {
		return nil, fmt.Errorf("log scanner: retry initial %s must be > 0", cfg.RetryInitial)
	}
	if cfg.RetryMax < cfg.RetryInitial {
		return nil, fmt.Errorf("log scanner: retry max %s must be >= retry initial %s", cfg.RetryMax, cfg.RetryInitial)
	}
	if cfg.StartBlock > math.MaxInt64 {
		return nil, fmt.Errorf("log scanner: start block %d exceeds the bigint column range", cfg.StartBlock)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LogScanner{
		pool:      pool,
		client:    client,
		logs:      logs,
		lease:     lease,
		cfg:       cfg,
		logger:    logger,
		chainID:   lease.chainID,
		whitelist: whitelist,
		addresses: addresses,
	}, nil
}

// State returns the current scanner state.
func (s *LogScanner) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// Checkpoint returns the mirrored next_block (the next height to scan); ok is
// false while progress is empty. It mirrors Scanner.Checkpoint.
func (s *LogScanner) Checkpoint() (next uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.prog == nil {
		return 0, false
	}
	return s.prog.NextBlock, true
}

// LogProgress returns the full log_checkpoint mirror; ok is false while
// progress is empty.
func (s *LogScanner) LogProgress() (LogProgress, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.prog == nil {
		return LogProgress{}, false
	}
	return *s.prog, true
}

func (s *LogScanner) setState(state State, reason string) {
	s.mu.Lock()
	s.state = state
	s.reason = reason
	s.mu.Unlock()
}

func (s *LogScanner) setProgress(p *LogProgress) {
	s.mu.Lock()
	s.prog = p
	s.mu.Unlock()
}

// ServeLoop runs the log scan while the caller's coordinator holds the lease.
// It performs no acquisition and no heartbeat (research R1): checkLost reports
// a lost lease and aborts before any write. It returns nil on ctx
// cancellation, a stop error on a durable pause/configuration refusal, and
// ErrLeaseLost when the lease is gone.
func (s *LogScanner) ServeLoop(ctx context.Context, checkLost func() error) error {
	if checkLost == nil {
		checkLost = func() error { return nil }
	}

	// Startup: read durable state (no row = empty) and stop on any pause row;
	// log_pause and indexer_pause are independent (FR-16).
	cp, logPause, chainPause, err := s.loadLogStateRetry(ctx)
	if err != nil {
		return nil // ctx cancelled
	}
	if cp != nil {
		s.setProgress(cp)
	}
	if err := s.rejectConfigChange(cp); err != nil {
		return err
	}
	if logPause != nil {
		return s.reportPaused(logPause)
	}
	if chainPause != nil {
		return s.reportChainPaused(chainPause)
	}

	back := newBackoff(s.cfg.RetryInitial, s.cfg.RetryMax)
	attempt := 0
	span := s.cfg.BatchBlocks
	validationAttempts := 0
	var validationStart uint64

	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := checkLost(); err != nil {
			return err
		}

		a := s.cfg.StartBlock
		first := true
		if cp != nil {
			a = cp.NextBlock
			first = false
		}
		// The deterministic-failure counter is per range start: a commit
		// moves a forward, which resets it.
		if a != validationStart {
			validationAttempts = 0
			validationStart = a
		}

		// Upper bound: only within 002's persisted coverage (FR-03). Missing
		// coverage is a wait, not a fault (edge case: start above the head).
		height, ok, err := s.loadChainHeight(ctx)
		if err != nil {
			d := back.next()
			s.setState(StateRetrying, "load_coverage")
			s.logger.Warn("read block checkpoint failed; retrying",
				"chain_id", s.chainID, "next_block", a, "attempt", attempt,
				"retry_in", d, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, d) {
				return nil
			}
			continue
		}
		if !ok || height < a {
			s.setState(StateWaiting, "wait_coverage")
			s.logger.Debug("waiting for verified block coverage",
				"chain_id", s.chainID, "next_block", a, "reason", "wait_coverage")
			if !s.wait(ctx, s.cfg.PollInterval) {
				return nil
			}
			continue
		}
		b := capSpan(a, span, height)

		attempt++

		// Coverage verification: every height must exist as canonical in
		// chain_blocks before any RPC query (FR-03/I4). Point reads follow the
		// canonical-read pattern of data-model.md.
		coverage, err := s.verifyCoverage(ctx, a, b)
		if err != nil {
			var cv *chainViewError
			if errors.As(err, &cv) {
				return s.pauseChainView(ctx, a, first, cv)
			}
			d := back.next()
			s.setState(StateRetrying, "verify_coverage")
			s.logger.Warn("coverage verification failed; retrying",
				"chain_id", s.chainID, "from_block", a, "to_block", b,
				"attempt", attempt, "retry_in", d, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, d) {
				return nil
			}
			continue
		}

		// End-block identity check before the query (FR-14): the node's view
		// at b must match the stored canonical block.
		expectedB := coverage[b]
		actualB, err := s.headerHash(ctx, b)
		if err != nil {
			done, rerr := s.handleHeaderCheckError(ctx, err, a, b, attempt, back)
			if rerr != nil {
				return rerr
			}
			if done {
				continue
			}
			return fmt.Errorf("end block header %d: %w", b, err)
		}
		if actualB != expectedB {
			return s.pauseChainView(ctx, a, first, &chainViewError{height: b, expected: expectedB, actual: actualB})
		}

		logs, err := s.filterLogs(ctx, a, b)
		if err != nil {
			if eth.KindOf(err) == eth.KindIncomplete {
				next, ok := shrinkSpan(span)
				if !ok {
					return s.pauseRangeIncomplete(ctx, a, first)
				}
				s.setState(StateRetrying, "incomplete")
				s.logger.Warn("log query incomplete; shrinking range",
					"chain_id", s.chainID, "from_block", a, "to_block", b,
					"kind", string(eth.KindIncomplete), "attempt", attempt, "shrink_to", next)
				span = next
				continue
			}
			if isLogRetryable(err) {
				d := back.next()
				s.setState(StateRetrying, "filter_logs")
				s.logger.Warn("log query failed; retrying",
					"chain_id", s.chainID, "from_block", a, "to_block", b,
					"kind", string(eth.KindOf(err)), "attempt", attempt, "retry_in", d,
					"error", logx.Redact(err.Error()))
				if !s.wait(ctx, d) {
					return nil
				}
				continue
			}
			// Unknown errors are failures: never treated as an empty result
			// and never retried on a guess (FR-12/R4).
			return fmt.Errorf("filter logs [%d,%d]: %w", a, b, err)
		}
		// A count reaching the configured limit is a completeness suspicion,
		// not a result: discard and shrink from a (FR-12).
		if countIncomplete(s.cfg.ResultLimit, len(logs)) {
			next, ok := shrinkSpan(span)
			if !ok {
				return s.pauseRangeIncomplete(ctx, a, first)
			}
			s.setState(StateRetrying, "incomplete")
			s.logger.Warn("log result count reached the configured limit; shrinking range",
				"chain_id", s.chainID, "from_block", a, "to_block", b,
				"kind", string(eth.KindIncomplete), "attempt", attempt, "shrink_to", next)
			span = next
			continue
		}

		// End-block identity check after the query (FR-14): a view that moved
		// during the query invalidates the whole batch.
		actualB, err = s.headerHash(ctx, b)
		if err != nil {
			done, rerr := s.handleHeaderCheckError(ctx, err, a, b, attempt, back)
			if rerr != nil {
				return rerr
			}
			if done {
				continue
			}
			return fmt.Errorf("end block header %d: %w", b, err)
		}
		if actualB != expectedB {
			return s.pauseChainView(ctx, a, first, &chainViewError{height: b, expected: expectedB, actual: actualB})
		}

		// 8-point per-log validation, outside any transaction (FR-07).
		rows, err := s.validateLogs(a, b, coverage, logs)
		if err != nil {
			var cv *chainViewError
			if errors.As(err, &cv) {
				return s.pauseChainView(ctx, a, first, cv)
			}
			var lv *logValidationError
			if errors.As(err, &lv) {
				validationAttempts++
				s.setState(StateRetrying, "validate_logs")
				if validationAttempts >= maxValidationAttempts {
					return s.pauseValidation(ctx, a, b, first, lv)
				}
				d := back.next()
				s.logger.Warn("log validation failed; re-querying",
					"chain_id", s.chainID, "from_block", a, "to_block", b,
					"kind", lv.class, "attempt", validationAttempts, "retry_in", d,
					"detail", lv.detail)
				if !s.wait(ctx, d) {
					return nil
				}
				continue
			}
			return fmt.Errorf("validate logs [%d,%d]: %w", a, b, err)
		}

		err = s.commitLogRange(ctx, a, b, first, coverage, rows)
		switch {
		case err == nil:
			if first {
				cp = &LogProgress{StartBlock: a, ConfigHash: s.cfg.ConfigHash, NextBlock: b + 1}
			} else {
				ncp := *cp
				ncp.NextBlock = b + 1
				cp = &ncp
			}
			s.setProgress(cp)
			s.setState(StateRunning, "")
			s.logger.Info("log range committed",
				"chain_id", s.chainID, "from_block", a, "to_block", b,
				"log_count", len(rows), "attempt", attempt)
			back.reset()
			attempt = 0
			span = s.cfg.BatchBlocks
			validationAttempts = 0
		case errors.Is(err, errPaused):
			_, lp, ip, rerr := s.loadLogStateRetry(ctx)
			if rerr != nil {
				return nil
			}
			if lp != nil {
				return s.reportPaused(lp)
			}
			if ip != nil {
				return s.reportChainPaused(ip)
			}
			s.setState(StatePaused, "pause_row")
			return fmt.Errorf("%w: durable log pause row present", errPaused)
		case errors.Is(err, ErrLeaseLost):
			return err
		case errors.Is(err, errStaleState):
			// A concurrent writer (or an uncertain commit from the previous
			// iteration) moved the durable state. Re-read and continue
			// idempotently; the exact guard decides what actually committed.
			old := cp
			ncp, nlp, nip, rerr := s.loadLogStateRetry(ctx)
			if rerr != nil {
				return nil
			}
			if nlp != nil {
				return s.reportPaused(nlp)
			}
			if nip != nil {
				return s.reportChainPaused(nip)
			}
			if err := s.rejectConfigChange(ncp); err != nil {
				return err
			}
			cp = ncp
			if sameLogProgress(old, cp) {
				// Nothing moved: avoid a hot loop against the database.
				if !s.wait(ctx, back.next()) {
					return nil
				}
			}
		default:
			var lv *logValidationError
			if errors.As(err, &lv) {
				validationAttempts++
				s.setState(StateRetrying, "commit_conflict")
				if validationAttempts >= maxValidationAttempts {
					return s.pauseValidation(ctx, a, b, first, lv)
				}
				d := back.next()
				s.logger.Warn("log identity conflict; re-querying",
					"chain_id", s.chainID, "from_block", a, "to_block", b,
					"kind", lv.class, "attempt", validationAttempts, "retry_in", d,
					"detail", lv.detail)
				if !s.wait(ctx, d) {
					return nil
				}
				continue
			}
			var cv *chainViewError
			if errors.As(err, &cv) {
				return s.pauseChainView(ctx, a, first, cv)
			}
			// Database failure: stay on this range and retry (FR-09).
			d := back.next()
			s.setState(StateRetrying, "commit_logs")
			s.logger.Warn("log commit failed; retrying",
				"chain_id", s.chainID, "from_block", a, "to_block", b,
				"attempt", attempt, "retry_in", d, "error", logx.Redact(err.Error()))
			if !s.wait(ctx, d) {
				return nil
			}
		}
	}
}

// handleHeaderCheckError applies the wait/backoff policy for an end-block
// header read. done is true when the caller should continue the loop; a
// non-nil rerr means stop. done=false means the error is not retryable and
// the caller should stop with the raw error.
func (s *LogScanner) handleHeaderCheckError(ctx context.Context, err error, a, b uint64, attempt int, back *backoff) (done bool, rerr error) {
	switch {
	case isNotFound(err):
		s.setState(StateWaiting, "wait_head")
		s.logger.Debug("end block not visible on the node yet; waiting",
			"chain_id", s.chainID, "from_block", a, "to_block", b,
			"next_block", b, "reason", "wait_head")
		s.wait(ctx, s.cfg.PollInterval)
		return true, nil // the loop top turns ctx cancellation into nil
	case isLogRetryable(err):
		d := back.next()
		s.setState(StateRetrying, "verify_end_block")
		s.logger.Warn("end block header check failed; retrying",
			"chain_id", s.chainID, "from_block", a, "to_block", b,
			"attempt", attempt, "retry_in", d, "error", logx.Redact(err.Error()))
		s.wait(ctx, d)
		return true, nil // the loop top turns ctx cancellation into nil
	default:
		return false, nil
	}
}

// rejectConfigChange enforces FR-05: an existing checkpoint row must match the
// configured (start_block, config_hash); comparing against next_block is
// forbidden, and nothing is modified on refusal.
func (s *LogScanner) rejectConfigChange(cp *LogProgress) error {
	if cp == nil {
		return nil
	}
	if cp.StartBlock == s.cfg.StartBlock && cp.ConfigHash == s.cfg.ConfigHash {
		return nil
	}
	detail := fmt.Sprintf("checkpoint(start_block=%d config_hash=%s) configured(start_block=%d config_hash=%s)",
		cp.StartBlock, cp.ConfigHash, s.cfg.StartBlock, s.cfg.ConfigHash)
	s.logger.Error("log scanner configuration changed; refusing to continue",
		"chain_id", s.chainID, "reason", "config_changed", "detail", detail)
	return fmt.Errorf("log config changed: checkpoint start_block=%d config_hash=%s but configured start_block=%d config_hash=%s; refusing to scan (FR-05)",
		cp.StartBlock, cp.ConfigHash, s.cfg.StartBlock, s.cfg.ConfigHash)
}

// loadLogStateRetry reads the durable log state, retrying database errors with
// capped backoff until success or ctx cancellation.
func (s *LogScanner) loadLogStateRetry(ctx context.Context) (*LogProgress, *pauseRow, *pauseRow, error) {
	back := newBackoff(s.cfg.RetryInitial, s.cfg.RetryMax)
	for {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		cp, lp, ip, err := s.loadLogState(ctx)
		if err == nil {
			return cp, lp, ip, nil
		}
		d := back.next()
		s.setState(StateRetrying, "load_log_state")
		s.logger.Warn("read log scanner state failed; retrying",
			"chain_id", s.chainID, "attempt", back.attempt, "retry_in", d,
			"error", logx.Redact(err.Error()))
		if !s.wait(ctx, d) {
			return nil, nil, nil, ctx.Err()
		}
	}
}

// loadLogState reads log_checkpoint (nil = empty progress), log_pause (nil =
// not paused) and indexer_pause (nil = chain not paused).
func (s *LogScanner) loadLogState(ctx context.Context) (*LogProgress, *pauseRow, *pauseRow, error) {
	var (
		start, next int64
		hash        string
	)
	cp := (*LogProgress)(nil)
	err := s.pool.QueryRow(ctx, readLogCheckpointSQL, s.chainID).Scan(&start, &hash, &next)
	switch {
	case err == nil:
		cp = &LogProgress{StartBlock: uint64(start), ConfigHash: hash, NextBlock: uint64(next)}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, nil, fmt.Errorf("read log checkpoint: %w", err)
	}

	var (
		lpHeight int64
		lpKind   string
		lpDetail string
	)
	lp := (*pauseRow)(nil)
	err = s.pool.QueryRow(ctx, readLogPauseSQL, s.chainID).Scan(&lpHeight, &lpKind, &lpDetail)
	switch {
	case err == nil:
		lp = &pauseRow{height: uint64(lpHeight), kind: lpKind, detail: lpDetail}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, nil, fmt.Errorf("read log pause row: %w", err)
	}

	var (
		ipHeight             int64
		ipExpected, ipActual string
		ipKind, ipDetail     string
	)
	ip := (*pauseRow)(nil)
	err = s.pool.QueryRow(ctx, readPauseSQL, s.chainID).Scan(&ipHeight, &ipExpected, &ipActual, &ipKind, &ipDetail)
	switch {
	case err == nil:
		ip = &pauseRow{
			height: uint64(ipHeight), expected: ipExpected, actual: ipActual,
			kind: ipKind, detail: ipDetail,
		}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, nil, nil, fmt.Errorf("read chain pause row: %w", err)
	}
	return cp, lp, ip, nil
}

// loadChainHeight reads 002's persisted checkpoint height (the log scanner's
// coverage upper bound); ok=false means 002 has no checkpoint yet.
func (s *LogScanner) loadChainHeight(ctx context.Context) (height uint64, ok bool, err error) {
	var (
		h, start int64
		hash     string
	)
	err = s.pool.QueryRow(ctx, readCheckpointSQL, s.chainID).Scan(&h, &hash, &start)
	switch {
	case err == nil:
		return uint64(h), true, nil
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("read block checkpoint: %w", err)
	}
}

// verifyCoverage reads the canonical hash of every height in [a, b]; a missing
// or non-canonical block is a chain-view divergence.
func (s *LogScanner) verifyCoverage(ctx context.Context, a, b uint64) (map[uint64]string, error) {
	coverage := make(map[uint64]string, b-a+1)
	for n := a; ; n++ {
		hash, ok, err := s.canonicalHashAt(ctx, s.pool, n)
		if err != nil {
			return nil, fmt.Errorf("coverage read at height %d: %w", n, err)
		}
		if !ok {
			return nil, &chainViewError{height: n, absent: true}
		}
		coverage[n] = hash
		if n == b {
			return coverage, nil
		}
	}
}

// canonicalHashAt is the canonical=TRUE point read shared by coverage,
// end-block checks and the in-transaction re-adjudication.
func (s *LogScanner) canonicalHashAt(ctx context.Context, q rowQuerier, number uint64) (string, bool, error) {
	var hash string
	err := q.QueryRow(ctx, canonicalBlockHashSQL, s.chainID, number).Scan(&hash)
	switch {
	case err == nil:
		return hash, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		return "", false, nil
	default:
		return "", false, err
	}
}

// rowQuerier is satisfied by *pgxpool.Pool and pgx.Tx: the canonical point
// read runs both outside and inside write transactions.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// headerHash reads one header with the configured RPC timeout and validates
// its height and hash shapes.
func (s *LogScanner) headerHash(ctx context.Context, number uint64) (string, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RPCTimeout)
	defer cancel()
	h, err := s.client.HeaderByNumber(rctx, new(big.Int).SetUint64(number))
	if err != nil {
		return "", err
	}
	if err := checkHeader(h, number); err != nil {
		return "", err
	}
	return hashHex(h.Hash()), nil
}

// filterLogs issues one bounded eth_getLogs call for [a, b] with topic0 pushed
// down and no pagination.
func (s *LogScanner) filterLogs(ctx context.Context, a, b uint64) ([]types.Log, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RPCTimeout)
	defer cancel()
	return s.logs.FilterLogs(rctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(a),
		ToBlock:   new(big.Int).SetUint64(b),
		Addresses: s.addresses,
		Topics:    [][]common.Hash{{eth.TransferSig}},
	})
}

// capSpan returns min(a+span-1, height) with an overflow guard: a batch never
// runs past the verified coverage and never wraps uint64 (FR-03).
func capSpan(a, span, height uint64) uint64 {
	b := a + span - 1
	if b < a || b > height {
		return height
	}
	return b
}

// shrinkSpan halves the requested span after an incomplete result; ok=false
// means a single block was already incomplete, so the caller stops and
// persists range_incomplete (FR-12).
func shrinkSpan(span uint64) (next uint64, ok bool) {
	if span <= 1 {
		return 0, false
	}
	return span / 2, true
}

// countIncomplete reports the count-based completeness verdict. ResultLimit 0
// disables it: without a confirmed provider limit no arbitrary threshold may
// be applied (research R5, FR-12).
func countIncomplete(limit uint64, n int) bool {
	return limit > 0 && uint64(n) >= limit
}

// isLogRetryable reports the log stream's transient request failures. It
// extends the shared isRetryable with KindInvalidResponse: node errors and
// response parse failures are request failures per FR-12/data-model.md and
// back off instead of stopping the stream. Unknown errors are not retryable.
func isLogRetryable(err error) bool {
	return isRetryable(err) || eth.KindOf(err) == eth.KindInvalidResponse
}

// logValidationError is one deterministic per-log validation failure; class is
// persisted as detail.class (data-model Table 3).
type logValidationError struct {
	height uint64
	class  string
	detail string
}

func (e *logValidationError) Error() string { return e.detail }

// pgUniqueViolation is SQLSTATE 23505. insertLogSQL absorbs the PK conflict
// with ON CONFLICT DO NOTHING, so the only unique violation the batch INSERT
// can raise is the block-scoped UNIQUE (chain_id, block_hash, log_index).
const pgUniqueViolation = "23505"

// insertLogFailure classifies one batch INSERT error. A unique violation is
// the block-scoped constraint, i.e. the same block slot bound to another
// transaction: it is reported as an identity conflict so the caller rolls
// back and pauses with validation_failed(detail.class=identity_conflict),
// identical to the content-conflict path (FR-10; data-model Table 3).
func insertLogFailure(row logRow, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		key := logKey{blockHash: row.blockHash, txHash: row.txHash, logIndex: row.logIndex}
		detail := fmt.Sprintf("class=%s identity=%s actual=%s",
			classIdentityConflict, key, rowSummary(row))
		if pgErr.ConstraintName != "" {
			detail += fmt.Sprintf(" constraint=%s", pgErr.ConstraintName)
		}
		return &logValidationError{height: row.blockNumber, class: classIdentityConflict, detail: detail}
	}
	return fmt.Errorf("insert log %s: %w",
		logKey{blockHash: row.blockHash, txHash: row.txHash, logIndex: row.logIndex}, err)
}

// chainViewError is a divergence between the observed chain view and the
// stored canonical block; it maps to a chain_view_changed pause (FR-14).
type chainViewError struct {
	height   uint64
	expected string // expected canonical hash; empty when absent
	actual   string // observed hash (RPC header or log)
	absent   bool   // evidence is "no canonical row at height"
}

func (e *chainViewError) Error() string {
	if e.absent {
		return fmt.Sprintf("chain_blocks has no canonical block at height %d", e.height)
	}
	return fmt.Sprintf("canonical hash at height %d is %s, observed %s", e.height, e.expected, e.actual)
}

func (e *chainViewError) pauseDetail() string {
	if e.absent {
		return fmt.Sprintf("class=%s height=%d expected=canonical_block actual=missing", pauseChainViewChanged, e.height)
	}
	return fmt.Sprintf("class=%s height=%d expected=%s actual=%s", pauseChainViewChanged, e.height, e.expected, e.actual)
}

// logKey is the FR-10 dedup identity (chain_id is constant per scanner).
type logKey struct {
	blockHash string
	txHash    string
	logIndex  uint64
}

func (k logKey) String() string {
	return fmt.Sprintf("%s/%s/%d", k.blockHash, k.txHash, k.logIndex)
}

// logBlockKey is the block-scoped UNIQUE identity (chain_id is constant per
// scanner): one block hash + log_index maps to exactly one log, so a second
// transaction using the same slot is an identity conflict (data-model Table 1).
type logBlockKey struct {
	blockHash string
	logIndex  uint64
}

func (k logBlockKey) String() string {
	return fmt.Sprintf("%s/%d", k.blockHash, k.logIndex)
}

// logRow is one validated log ready to persist.
type logRow struct {
	blockNumber uint64
	blockHash   string
	txHash      string
	logIndex    uint64
	contract    string
	topic0      string
	topic1      string
	topic2      string
	data        string
}

func rowSummary(r logRow) string {
	return fmt.Sprintf("contract=%s block=%d topic0=%s topic1=%s topic2=%s data=%s",
		r.contract, r.blockNumber, r.topic0, r.topic1, r.topic2, r.data)
}

// validateLogs applies the FR-07 8-point checklist to every log and dedupes
// exact repeats; a repeated identity with different content is a conflict that
// fails the whole batch (FR-09/FR-10).
func (s *LogScanner) validateLogs(a, b uint64, coverage map[uint64]string, logs []types.Log) ([]logRow, error) {
	rows := make([]logRow, 0, len(logs))
	seen := make(map[logKey]logRow, len(logs))
	// blockSeen covers the second UNIQUE constraint: same block slot with a
	// different transaction is an identity conflict, exactly like the PK
	// repeat with different content (FR-10; data-model Table 1).
	blockSeen := make(map[logBlockKey]logKey, len(logs))
	for i := range logs {
		row, err := s.validateLog(a, b, coverage, logs[i])
		if err != nil {
			return nil, err
		}
		key := logKey{blockHash: row.blockHash, txHash: row.txHash, logIndex: row.logIndex}
		if prev, ok := seen[key]; ok {
			if prev == row {
				continue // exact duplicate converges (FR-09)
			}
			return nil, &logValidationError{
				height: row.blockNumber,
				class:  classIdentityConflict,
				detail: fmt.Sprintf("class=%s identity=%s expected=%s actual=%s",
					classIdentityConflict, key, rowSummary(prev), rowSummary(row)),
			}
		}
		bk := logBlockKey{blockHash: row.blockHash, logIndex: row.logIndex}
		if prevKey, ok := blockSeen[bk]; ok {
			// Same (block_hash, log_index), different tx_hash: the block slot
			// is already bound to another log. A same-PK repeat never reaches
			// here (handled above), so any hit is the second constraint.
			return nil, &logValidationError{
				height: row.blockNumber,
				class:  classIdentityConflict,
				detail: fmt.Sprintf("class=%s identity=%s expected=%s actual=%s",
					classIdentityConflict, bk, rowSummary(seen[prevKey]), rowSummary(row)),
			}
		}
		seen[key] = row
		blockSeen[bk] = key
		rows = append(rows, row)
	}
	return rows, nil
}

// validateLog checks one log against the checklist, returning the normalized
// lowercase-hex row on success.
func (s *LogScanner) validateLog(a, b uint64, coverage map[uint64]string, l types.Log) (logRow, error) {
	fail := func(class, format string, args ...any) (logRow, error) {
		return logRow{}, &logValidationError{
			height: l.BlockNumber,
			class:  class,
			detail: fmt.Sprintf("class=%s identity=%s %s", class, logIdentity(l), fmt.Sprintf(format, args...)),
		}
	}

	// (1) Whitelisted contract; (6) 20-byte address.
	contract := strings.ToLower(l.Address.Hex())
	if _, ok := s.whitelist[contract]; !ok {
		return fail(classBadAddress, "expected=whitelisted_contract actual=%s", contract)
	}
	// (2) Height inside the requested range.
	if l.BlockNumber < a || l.BlockNumber > b {
		return fail(classOutOfRange, "expected=block_in_[%d,%d] actual=%d", a, b, l.BlockNumber)
	}
	// (4) Identity fields complete and shaped.
	blockHash, txHash := hashHex(l.BlockHash), hashHex(l.TxHash)
	if !validHash(blockHash) || l.BlockHash == (common.Hash{}) {
		return fail(classMissingField, "expected=32_byte_block_hash actual=%s", blockHash)
	}
	if !validHash(txHash) || l.TxHash == (common.Hash{}) {
		return fail(classMissingField, "expected=32_byte_tx_hash actual=%s", txHash)
	}
	// (5) Not a removed log.
	if l.Removed {
		return fail(classRemoved, "expected=removed_false actual=true")
	}
	// (7) Exactly three 32-byte topics, Transfer topic0, left-padded address
	// topics.
	if len(l.Topics) != 3 {
		return fail(classBadTopics, "expected=3_topics actual=%d", len(l.Topics))
	}
	if l.Topics[0] != eth.TransferSig {
		return fail(classBadTopics, "expected=topic0_%s actual=%s", hashHex(eth.TransferSig), hashHex(l.Topics[0]))
	}
	for i := 1; i <= 2; i++ {
		if !zeroHigh12(l.Topics[i]) {
			return fail(classBadTopics, "expected=topic%d_address_padded actual=%s", i, hashHex(l.Topics[i]))
		}
	}
	// (8) Exactly 32 bytes of raw data.
	if len(l.Data) != 32 {
		return fail(classBadData, "expected=32_byte_data actual=%d_bytes", len(l.Data))
	}
	// (3) block_hash must equal the canonical 002 block at this height.
	expected, ok := coverage[l.BlockNumber]
	if !ok || expected != blockHash {
		return logRow{}, &chainViewError{height: l.BlockNumber, expected: expected, actual: blockHash}
	}

	return logRow{
		blockNumber: l.BlockNumber,
		blockHash:   blockHash,
		txHash:      txHash,
		logIndex:    uint64(l.Index),
		contract:    contract,
		topic0:      hashHex(l.Topics[0]),
		topic1:      hashHex(l.Topics[1]),
		topic2:      hashHex(l.Topics[2]),
		data:        hexutil.Encode(l.Data),
	}, nil
}

// logIdentity renders the FR-10 identity for detail strings.
func logIdentity(l types.Log) string {
	return fmt.Sprintf("%s/%s/%d", hashHex(l.BlockHash), hashHex(l.TxHash), l.Index)
}

// zeroHigh12 reports whether the top 12 bytes of an address topic are zero.
func zeroHigh12(h common.Hash) bool {
	for _, b := range h[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}

// validConfigHash matches the log_checkpoint.config_hash CHECK constraint.
func validConfigHash(s string) bool {
	if len(s) != 64 || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// commitLogRange persists one validated batch and advances (or creates) the
// log checkpoint in a single short transaction, following data-model.md's
// write protocol step by step. There is no special case for an uncertain
// COMMIT: the next iteration re-reads the durable state and the exact guard
// settles what actually committed (FR-16/OQ2).
func (s *LogScanner) commitLogRange(ctx context.Context, a, b uint64, first bool, coverage map[uint64]string, rows []logRow) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin log transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("log transaction statement guard: %w", err)
	}

	// Step 2: idempotently ensure the coordination row exists.
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.chainID, s.lease.ownerID, s.lease.Token(), s.lease.ttl.Seconds()); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	// Step 3: take the chain-wide coordination lock (held to COMMIT).
	var (
		owner string
		token int64
		valid bool
	)
	err = tx.QueryRow(ctx, lockCoordSQL, s.chainID).Scan(&owner, &token, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: coordination row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}

	// Step 4: independent statements re-read and adjudicate under the lock.
	var one int
	err = tx.QueryRow(ctx, logPauseExistsSQL, s.chainID).Scan(&one)
	if err == nil {
		return errPaused
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("log pause verdict: %w", err)
	}
	err = tx.QueryRow(ctx, pauseExistsSQL, s.chainID).Scan(&one)
	if err == nil {
		return errPaused
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("chain pause verdict: %w", err)
	}
	err = tx.QueryRow(ctx, leaseVerdictSQL, s.chainID, s.lease.ownerID, s.lease.Token()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lease verdict: %w", err)
	}
	if first {
		// First interval: no log_checkpoint row may exist at all (a concurrent
		// creator makes this stale, and the caller re-reads).
		err = tx.QueryRow(ctx, logCheckpointExistsSQL, s.chainID).Scan(&one)
		if err == nil {
			return errStaleState
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("first-interval verdict: %w", err)
		}
	} else {
		// Advance: the row must be exactly (S, H, a) — progress, start and
		// configuration all checked (FR-05, I2/I3).
		err = tx.QueryRow(ctx, logCheckpointGuardSQL, s.chainID, s.cfg.StartBlock, s.cfg.ConfigHash, a).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return errStaleState
		}
		if err != nil {
			return fmt.Errorf("log checkpoint guard: %w", err)
		}
	}

	// Chain-view re-adjudication: every height in [a, b] must still exist,
	// canonical and with the hash observed before the RPC (FR-14, I4). The
	// reads happen after the lock, so they include everything committed while
	// waiting (Read Committed statement snapshots).
	for n := a; ; n++ {
		stored, ok, err := s.canonicalHashAt(ctx, tx, n)
		if err != nil {
			return fmt.Errorf("chain view verdict at height %d: %w", n, err)
		}
		if !ok || stored != coverage[n] {
			return &chainViewError{height: n, expected: coverage[n], actual: stored, absent: !ok}
		}
		if n == b {
			break
		}
	}

	// Step 5: insert the batch; conflicts are ignored by the write, then every
	// identity is re-read and compared byte by byte. In-memory knowledge is
	// never the correctness basis (FR-09/FR-10).
	for _, row := range rows {
		if _, err := tx.Exec(ctx, insertLogSQL,
			s.chainID, row.blockNumber, row.blockHash, row.txHash, row.logIndex,
			row.contract, row.topic0, row.topic1, row.topic2, row.data); err != nil {
			return insertLogFailure(row, err)
		}
	}
	matched := 0
	for _, row := range rows {
		var (
			storedBlock                              int64
			storedContract                           string
			storedT0, storedT1, storedT2, storedData string
		)
		err := tx.QueryRow(ctx, readLogRowSQL, s.chainID, row.blockHash, row.txHash, row.logIndex).Scan(
			&storedContract, &storedBlock, &storedT0, &storedT1, &storedT2, &storedData)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("log row missing after insert: identity %s (silent loss)",
				logKey{blockHash: row.blockHash, txHash: row.txHash, logIndex: row.logIndex})
		}
		if err != nil {
			return fmt.Errorf("re-read log row: %w", err)
		}
		stored := logRow{
			blockNumber: uint64(storedBlock), blockHash: row.blockHash, txHash: row.txHash,
			logIndex: row.logIndex, contract: storedContract,
			topic0: storedT0, topic1: storedT1, topic2: storedT2, data: storedData,
		}
		if stored != row {
			return &logValidationError{
				height: row.blockNumber,
				class:  classIdentityConflict,
				detail: fmt.Sprintf("class=%s identity=%s expected=%s actual=%s",
					classIdentityConflict,
					logKey{blockHash: row.blockHash, txHash: row.txHash, logIndex: row.logIndex},
					rowSummary(stored), rowSummary(row)),
			}
		}
		matched++
	}
	// Row-count reconciliation (data-model step 5): the de-duplicated identity
	// count must equal the consistent stored rows; the loop above returned
	// early on any absence, this makes the invariant explicit.
	if matched != len(rows) {
		return fmt.Errorf("log batch reconciliation: %d identities, %d consistent rows", len(rows), matched)
	}

	if first {
		tag, err := tx.Exec(ctx, insertLogCheckpointSQL, s.chainID, a, s.cfg.ConfigHash, b+1)
		if err != nil {
			return fmt.Errorf("insert log checkpoint: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errStaleState
		}
	} else {
		tag, err := tx.Exec(ctx, advanceLogCheckpointSQL, s.chainID, b+1, a, s.cfg.StartBlock, s.cfg.ConfigHash)
		if err != nil {
			return fmt.Errorf("advance log checkpoint: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return errStaleState
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit log range [%d,%d]: %w", a, b, err)
	}
	return nil
}

// commitLogPause persists a log pause row through the unified write protocol:
// lock first, then re-verify Table 3's atomic conditions (lease ownership, no
// pause row, unchanged progress, evidence) with independent statements. Any
// condition failing abandons the pause with errStaleState; an existing pause
// row yields errPaused (first pause wins).
func (s *LogScanner) commitLogPause(ctx context.Context, p logPauseInfo, ev *chainViewError) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin log pause transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return fmt.Errorf("log pause statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.chainID, s.lease.ownerID, s.lease.Token(), s.lease.ttl.Seconds()); err != nil {
		return fmt.Errorf("ensure coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	err = tx.QueryRow(ctx, lockCoordSQL, s.chainID).Scan(&owner, &token, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: coordination row missing", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock coordination row: %w", err)
	}

	var one int
	err = tx.QueryRow(ctx, logPauseExistsSQL, s.chainID).Scan(&one)
	if err == nil {
		return errPaused // first pause wins
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pause verdict: %w", err)
	}
	err = tx.QueryRow(ctx, leaseVerdictSQL, s.chainID, s.lease.ownerID, s.lease.Token()).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: owner/fencing/expiry verdict failed", ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lease verdict: %w", err)
	}

	// The pause is anchored to the range start: progress must still be
	// exactly (S, H, a), otherwise the evidence is stale.
	if p.first {
		err = tx.QueryRow(ctx, logCheckpointExistsSQL, s.chainID).Scan(&one)
		if err == nil {
			return errStaleState
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("pause progress verdict: %w", err)
		}
	} else {
		err = tx.QueryRow(ctx, logCheckpointGuardSQL, s.chainID, s.cfg.StartBlock, s.cfg.ConfigHash, p.from).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return errStaleState
		}
		if err != nil {
			return fmt.Errorf("pause progress verdict: %w", err)
		}
	}

	// Evidence re-read for chain-view divergence (no RPC inside a
	// transaction): the evidence holds while the canonical row is absent or
	// still differs from the observed hash.
	if ev != nil {
		var stored string
		err := tx.QueryRow(ctx, canonicalBlockHashSQL, s.chainID, ev.height).Scan(&stored)
		switch {
		case err == nil:
			if ev.absent || stored == ev.actual {
				return errStaleState
			}
		case errors.Is(err, pgx.ErrNoRows):
			// Divergence unchanged: the canonical row is still missing.
		default:
			return fmt.Errorf("pause evidence re-read: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, insertLogPauseSQL, s.chainID, p.height, p.kind, p.detail); err != nil {
		return fmt.Errorf("insert log pause row: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit log pause: %w", err)
	}
	return nil
}

// pauseChainView persists a chain_view_changed pause and returns the stop
// error (stopping is unconditional; persistence is best effort).
func (s *LogScanner) pauseChainView(ctx context.Context, from uint64, first bool, ev *chainViewError) error {
	s.setState(StatePaused, pauseChainViewChanged)
	s.logger.Error("chain view changed; pausing log scan",
		"chain_id", s.chainID, "height", ev.height, "kind", pauseChainViewChanged,
		"expected_hash", ev.expected, "actual_hash", ev.actual)
	return s.persistLogPause(ctx, logPauseInfo{
		from: from, first: first, height: ev.height,
		kind: pauseChainViewChanged, detail: ev.pauseDetail(),
	}, ev)
}

// pauseValidation persists a validation_failed pause after the deterministic
// failure survived maxValidationAttempts re-queries.
func (s *LogScanner) pauseValidation(ctx context.Context, from, to uint64, first bool, lv *logValidationError) error {
	height := from
	if lv.height >= from && lv.height <= to {
		height = lv.height
	}
	s.setState(StatePaused, pauseValidationFailed)
	s.logger.Error("log validation failed repeatedly; pausing",
		"chain_id", s.chainID, "height", height, "kind", pauseValidationFailed, "detail", lv.detail)
	return s.persistLogPause(ctx, logPauseInfo{
		from: from, first: first, height: height,
		kind: pauseValidationFailed, detail: lv.detail,
	}, nil)
}

// pauseRangeIncomplete persists a range_incomplete pause: even a single-block
// query could not be confirmed complete (FR-12).
func (s *LogScanner) pauseRangeIncomplete(ctx context.Context, from uint64, first bool) error {
	s.setState(StatePaused, pauseRangeIncomplete)
	s.logger.Error("single-block log query still incomplete; pausing",
		"chain_id", s.chainID, "height", from, "kind", pauseRangeIncomplete)
	detail := fmt.Sprintf("class=%s height=%d expected=complete actual=incomplete", pauseRangeIncomplete, from)
	return s.persistLogPause(ctx, logPauseInfo{
		from: from, first: first, height: from,
		kind: pauseRangeIncomplete, detail: detail,
	}, nil)
}

// persistLogPause runs the pause transaction and always returns the stop error.
func (s *LogScanner) persistLogPause(ctx context.Context, p logPauseInfo, ev *chainViewError) error {
	switch err := s.commitLogPause(ctx, p, ev); {
	case err == nil:
	case errors.Is(err, ErrLeaseLost):
		return fmt.Errorf("%w (persisting log pause: %v)", ErrLeaseLost, err)
	case errors.Is(err, errPaused):
		// Another pause row already exists: the desired end state.
	case errors.Is(err, errStaleState):
		s.logger.Warn("log pause evidence changed before it was persisted; stopping anyway",
			"chain_id", s.chainID, "height", p.height, "kind", p.kind)
	default:
		s.logger.Error("persist log pause failed",
			"chain_id", s.chainID, "height", p.height, "kind", p.kind,
			"error", logx.Redact(err.Error()))
	}
	return fmt.Errorf("%w: log %s at height %d", errPaused, p.kind, p.height)
}

// reportPaused logs a durable log_pause row and returns the stop error.
func (s *LogScanner) reportPaused(p *pauseRow) error {
	s.setState(StatePaused, p.kind)
	s.logger.Error("log stream is paused; refusing to advance",
		"chain_id", s.chainID, "height", p.height, "kind", p.kind, "detail", p.detail)
	return fmt.Errorf("%w: log %s at height %d", errPaused, p.kind, p.height)
}

// reportChainPaused reports 002's chain-level pause, which blocks every log
// commit (FR-16). It never touches the 002 pause row.
func (s *LogScanner) reportChainPaused(p *pauseRow) error {
	s.setState(StatePaused, "chain_paused")
	s.logger.Error("chain-level pause blocks the log stream; refusing to advance",
		"chain_id", s.chainID, "height", p.height, "kind", p.kind)
	return fmt.Errorf("%w: chain-level %s at height %d blocks log scanning (FR-16)", errPaused, p.kind, p.height)
}

// sameLogProgress reports whether two mirror snapshots describe the same
// durable row.
func sameLogProgress(a, b *LogProgress) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// wait sleeps for d and reports false when ctx is done, so shutdown aborts
// every wait and backoff immediately.
func (s *LogScanner) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Log write-transaction SQL, following data-model.md. RPC is always done
// outside these transactions.

const (
	// canonicalBlockHashSQL is the canonical=TRUE point read (data-model
	// §canonical 读取范式): coverage, end-block checks and the in-transaction
	// re-adjudication all use this single statement.
	canonicalBlockHashSQL = `
SELECT hash FROM chain_blocks
WHERE chain_id = $1 AND number = $2 AND canonical`

	// readLogCheckpointSQL reads the log progress row (no row = empty).
	readLogCheckpointSQL = `
SELECT start_block, config_hash, next_block FROM log_checkpoint WHERE chain_id = $1`

	// readLogPauseSQL reads the log pause row (no row = not paused).
	readLogPauseSQL = `
SELECT height, kind, detail FROM log_pause WHERE chain_id = $1`

	// logPauseExistsSQL is write-protocol step 4: an existing log pause
	// rejects every advance.
	logPauseExistsSQL = `SELECT 1 FROM log_pause WHERE chain_id = $1`

	// logCheckpointExistsSQL is write-protocol step 4 for the first log
	// interval and the first-pause anchor: no log_checkpoint row may exist at
	// all (a concurrent creator makes the verdict stale, and the caller
	// re-reads). It must never be confused with 002's checkpointExistsSQL,
	// which guards indexer_checkpoint.
	logCheckpointExistsSQL = `SELECT 1 FROM log_checkpoint WHERE chain_id = $1`

	// logCheckpointGuardSQL is write-protocol step 4: exact progress guard
	// (next = a) plus frozen configuration identity (start, config_hash).
	logCheckpointGuardSQL = `
SELECT 1 FROM log_checkpoint
WHERE chain_id = $1 AND start_block = $2 AND config_hash = $3 AND next_block = $4`

	// insertLogCheckpointSQL atomically creates the progress row on the first
	// interval (R3: ON CONFLICT DO NOTHING, zero rows means stale).
	insertLogCheckpointSQL = `
INSERT INTO log_checkpoint (chain_id, start_block, config_hash, next_block)
VALUES ($1, $2, $3, $4)
ON CONFLICT (chain_id) DO NOTHING`

	// advanceLogCheckpointSQL advances only from exactly a (I2/I3) with the
	// frozen configuration identity, mirroring logCheckpointGuardSQL and 002's
	// advanceCheckpointSQL (fail-closed: any manual edit of S/H makes the
	// update match zero rows and the caller rejects it as stale).
	advanceLogCheckpointSQL = `
UPDATE log_checkpoint
SET next_block = $2, updated_at = now()
WHERE chain_id = $1 AND start_block = $4 AND config_hash = $5 AND next_block = $3`

	// insertLogSQL dedupes on the FR-10 identity; content is verified by the
	// subsequent re-read.
	insertLogSQL = `
INSERT INTO erc20_transfer_logs
    (chain_id, block_number, block_hash, tx_hash, log_index,
     contract, topic0, topic1, topic2, data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (chain_id, block_hash, tx_hash, log_index) DO NOTHING`

	// readLogRowSQL re-reads one identity's stored content for the byte-wise
	// conflict comparison (never trust in-memory knowledge).
	readLogRowSQL = `
SELECT contract, block_number, topic0, topic1, topic2, data
FROM erc20_transfer_logs
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = $4`

	insertLogPauseSQL = `
INSERT INTO log_pause (chain_id, height, kind, detail)
VALUES ($1, $2, $3, $4)
ON CONFLICT (chain_id) DO NOTHING`
)
