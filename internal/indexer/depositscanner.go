// depositscanner.go implements 004's upstream alignment and coverage proof
// (T005): the transaction-external half of the deposit write protocol, exactly
// as locked by specs/004-deposit-detection/ (FR-07/FR-11, research R2/R5,
// data-model.md 写事务协议步骤 1 and §canonical 读取范式).
//
// T005 scope: read and align the 003 upstream state, classify upstream gaps,
// prove the [a,b] unit covered (watermark, chain identity, upstream start,
// three pause rows absent, block-by-block canonical binding) and read the
// source rows in R2 order. It performs no writes and no RPC; the scan loop and
// the atomic commit are T006 (depositcommit.go), and pause/authorization
// persistence is T014/T025.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
)

// DepositConfig is the frozen input of one deposit scanner. Identity fields
// (StartBlock, Assets, Watches, ConfigHash) come from the T002 encoding;
// LogContracts/LogConfigHash/LogStartHeight are the shared 003 configuration
// used to recompute and align the upstream identity (research R5). BatchBlocks
// and the timing knobs are tuning parameters and never part of any identity
// (research R3/R7).
type DepositConfig struct {
	ChainID     int64
	StartBlock  uint64
	Assets      []config.DepositEntry
	Watches     []config.DepositEntry
	ConfigHash  string
	BatchBlocks uint64

	// PollInterval, RetryInitial and RetryMax are the shared
	// TXHARBOR_INDEX_* knobs (research R7); zero means the repository
	// defaults. They only pace waiting and retry, never coverage verdicts.
	PollInterval time.Duration
	RetryInitial time.Duration
	RetryMax     time.Duration

	// Shared upstream (003) configuration: the env whitelist is re-hashed here
	// and compared with the persisted log_checkpoint.config_hash; a mismatch
	// is an upstream drift refusal, never a wait (R5).
	LogContracts   []string
	LogConfigHash  string
	LogStartHeight uint64
}

// DepositScanner holds the immutable deposit configuration plus the derived
// upstream identity. T005 exposes the read/proof surface only; T006 adds the
// serial scan loop and the atomic commit on this same type.
type DepositScanner struct {
	pool *pgxpool.Pool
	cfg  DepositConfig
	// upstreamHash is recomputed from LogContracts at construction, never
	// accepted as an opaque value (R5: the shared env is the whitelist truth).
	upstreamHash string
	// resultObserver, when non-nil, receives one call per processed source row
	// result (contracts/observability.md: matched|nomatch|zero for a committed
	// unit, invalid for a batch refused by the structural re-check). It keeps
	// the scanner independent of the metrics registry; app wiring is T018.
	resultObserver func(result string)
	// pauseObserver, when non-nil, receives one call per newly persisted
	// pause row (contracts/observability.md txharbor_deposit_pause_total,
	// which counts pause events, not live rows). First-wins convergence
	// calls it only for the writer that inserted the row. App wiring is
	// T018; T019 locks the count.
	pauseObserver func()
	// depState/depNext/depHasProgress mirror the loop condition for the
	// deposit_state/next gauges (contracts/observability.md): 0 running, 1
	// waiting on upstream coverage, 2 backing off, 3 paused, 4 structural
	// stop. Atomics because serve samples them off-loop on a ticker.
	depState       atomic.Int32
	depNext        atomic.Uint64
	depHasProgress atomic.Bool
}

// NewDepositScanner validates the deposit configuration without any I/O. A
// blank collection is a configuration error and never degrades into
// all-address identification or all-asset recognition (FR-04, I5).
func NewDepositScanner(pool *pgxpool.Pool, cfg DepositConfig) (*DepositScanner, error) {
	if pool == nil {
		return nil, errors.New("deposit scanner: nil pool")
	}
	hash, err := validateDepositConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("deposit scanner: %w", err)
	}
	return &DepositScanner{pool: pool, cfg: cfg, upstreamHash: hash}, nil
}

// SetResultObserver wires the per-row result counter hook behind
// txharbor_deposit_observations_total (contracts/observability.md). The
// callback receives one of matched|nomatch|zero for every source row of a
// committed unit, or invalid once for a batch refused before commit. A nil
// observer (the default) disables counting; app wiring is T018.
func (s *DepositScanner) SetResultObserver(observe func(result string)) {
	s.resultObserver = observe
}

// SetPauseObserver wires the pause-event hook behind
// txharbor_deposit_pause_total (contracts/observability.md). It fires once
// per newly inserted pause row, never on first-wins convergence. A nil
// observer (the default) disables counting; app wiring is T018.
func (s *DepositScanner) SetPauseObserver(observe func()) {
	s.pauseObserver = observe
}

// DepositState reports the loop condition for the deposit_state gauge
// (contracts/observability.md): 0 running, 1 waiting on upstream coverage, 2
// backing off, 3 paused, 4 structural stop. It mirrors the last loop
// decision; terminal stops keep their verdict so the final sample explains
// the exit.
func (s *DepositScanner) DepositState() int {
	return int(s.depState.Load())
}

// DepositProgress reports the last committed next_block for the deposit_next
// gauge (contracts/observability.md). ok is false until the first unit
// commits, so the series stays absent on empty progress.
func (s *DepositScanner) DepositProgress() (uint64, bool) {
	return s.depNext.Load(), s.depHasProgress.Load()
}

// observeResult forwards one processed-row result to the observer, if any.
func (s *DepositScanner) observeResult(result string) {
	if s.resultObserver != nil {
		s.resultObserver(result)
	}
}

// validateDepositConfig checks the deposit configuration and returns the
// recomputed upstream identity. It is pure so the validation branches are
// unit-testable without a pool.
func validateDepositConfig(cfg DepositConfig) (string, error) {
	if cfg.ChainID <= 0 {
		return "", fmt.Errorf("chain id %d must be > 0", cfg.ChainID)
	}
	if len(cfg.Assets) == 0 {
		return "", errors.New("empty asset set; refusing to scan")
	}
	if len(cfg.Watches) == 0 {
		return "", errors.New("empty watch address set; refusing to scan")
	}
	if !validConfigHash(cfg.ConfigHash) {
		return "", fmt.Errorf("config hash %q is not 64 lowercase hex", cfg.ConfigHash)
	}
	if cfg.BatchBlocks == 0 {
		return "", errors.New("batch blocks must be > 0")
	}
	for _, e := range append(append([]config.DepositEntry(nil), cfg.Assets...), cfg.Watches...) {
		if !common.IsHexAddress(e.Address) {
			return "", fmt.Errorf("%q is not a 20-byte EVM address", e.Address)
		}
	}
	return recomputeUpstreamIdentity(cfg)
}

// recomputeUpstreamIdentity re-derives the 003 whitelist identity from the
// same env list the 003 scanner uses (research R5: the shared env is the
// whitelist truth; only the hash is persisted upstream). The configured
// LogConfigHash must equal the recomputation — a mismatch is a configuration
// error, never silently accepted.
func recomputeUpstreamIdentity(cfg DepositConfig) (string, error) {
	if len(cfg.LogContracts) == 0 {
		return "", errors.New("empty upstream contract whitelist")
	}
	_, hash, err := config.NormalizeWhitelist(strings.Join(cfg.LogContracts, ","))
	if err != nil {
		return "", fmt.Errorf("recompute upstream identity: %w", err)
	}
	if cfg.LogConfigHash != "" && hash != cfg.LogConfigHash {
		return "", fmt.Errorf("upstream whitelist identity mismatch: configured %s, recomputed %s",
			cfg.LogConfigHash, hash)
	}
	return hash, nil
}

// upstreamState is the read-only mirror of 003's log_checkpoint row. chainID is
// selected explicitly so the proof asserts chain identity instead of merely
// relying on the WHERE clause.
type upstreamState struct {
	chainID    int64
	startBlock uint64
	configHash string
	nextBlock  uint64
}

// depositProgress is the read-only mirror of deposit_checkpoint plus the
// latest deposit_config_history version it must agree with (data-model Table 2
// integrity: both sides exist or neither does, and their (start_block,
// config_hash) match).
type depositProgress struct {
	startBlock uint64
	configHash string
	nextBlock  uint64
	versionSeq int64
}

// depositUnit is the R2 step 1 preparation for one processing unit: the
// aligned upstream state, the canonical binding of every height in [a, b] and
// the source rows read after the upstream checkpoint.
type depositUnit struct {
	upstream  upstreamState
	canonical map[uint64]string
	rows      []depositSourceLog
}

// depositQuerier is satisfied by *pgxpool.Pool and pgx.Tx: the T005 reads run
// outside the transaction, T006's under-lock re-adjudication reuses the same
// statements on the lease transaction.
type depositQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// upstreamDriftError is the R5 refusal: the env whitelist recomputes to a
// different identity than the one 003 actually indexed with. This is not a
// gap and not transient — 004 must not consume a log stream whose whitelist it
// cannot name, and it never repairs the identity itself.
type upstreamDriftError struct {
	persisted string
	expected  string
}

func (e *upstreamDriftError) Error() string {
	return fmt.Sprintf("upstream config drift (upstream_drift): log_checkpoint config_hash=%s but the env whitelist recomputes %s; refusing to continue",
		e.persisted, e.expected)
}

// depositCorruptStateError is a persisted deposit-state inconsistency (for
// example checkpoint and history exist on only one side, or their
// (start_block, config_hash) disagree). It is never repaired automatically.
type depositCorruptStateError struct {
	detail string
}

func (e *depositCorruptStateError) Error() string {
	return "deposit state corrupted: " + e.detail
}

// streamPauseError reports that one of the three pause streams has a row. A
// deposit unit is only covered when deposit_pause, log_pause and
// indexer_pause are all absent (FR-12, data-model 暂停写入的原子条件; T014
// owns the persistent pause behavior).
type streamPauseError struct {
	stream  string
	chainID int64
}

func (e *streamPauseError) Error() string {
	return fmt.Sprintf("%s pause row exists for chain %d; a deposit unit is only covered with all three pause streams absent",
		e.stream, e.chainID)
}

// Gap classes and causes of the R5 classification (contracts/observability.md
// gap log line: class(transient|structural), cause values below).
const (
	depositGapTransient  = "transient"
	depositGapStructural = "structural"

	gapCauseBehindHead         = "behind_head"
	gapCauseBelowUpstreamStart = "below_upstream_start"
	gapCauseAssetNotIndexed    = "asset_not_indexed"
)

// depositGap is the R5 verdict for one requested unit. transient means
// "upstream coverage has not reached the unit yet — wait, never a timeout
// verdict"; structural means "waiting cannot help — stop and record the range,
// cause and config version". Neither carries any permission to trim the unit
// or to raise its start (research R5: 禁止裁剪/禁超时判定).
type depositGap struct {
	class      string
	cause      string
	from, to   uint64
	configHash string
}

func (g *depositGap) Error() string {
	return fmt.Sprintf("deposit upstream gap: class=%s gap=%d-%d cause=%s config=%s",
		g.class, g.from, g.to, g.cause, g.configHash)
}

// classifyUpstreamGap applies research R5 to one requested unit [a, b] without
// any I/O: it distinguishes a fully covered unit (nil) from a transient gap
// (the coverage watermark has not reached b) and a structural gap (required
// history below the upstream start, or a required asset the upstream whitelist
// never indexed). The decision uses persisted configuration/coverage only —
// elapsed time is never an input.
func classifyUpstreamGap(cfg DepositConfig, up *upstreamState, a, b uint64) *depositGap {
	// Required history below the upstream start is unrecoverable. The start is
	// never raised to S_u to "fit" (that would silently drop history).
	upstreamStart := cfg.LogStartHeight
	if up != nil {
		upstreamStart = up.startBlock
	}
	if a < upstreamStart {
		return &depositGap{
			class: depositGapStructural, cause: gapCauseBelowUpstreamStart,
			from: a, to: b, configHash: cfg.ConfigHash,
		}
	}
	// Assets that can generate an observation inside this unit (effective
	// height <= b) must be in the whitelist 003 actually indexed. An asset
	// missing there can never produce source rows: waiting has no solution.
	whitelist := make(map[string]struct{}, len(cfg.LogContracts))
	for _, c := range cfg.LogContracts {
		whitelist[strings.ToLower(c)] = struct{}{}
	}
	for _, e := range cfg.Assets {
		if e.Effective > b {
			continue // not required by this unit yet
		}
		if _, ok := whitelist[strings.ToLower(e.Address)]; !ok {
			return &depositGap{
				class: depositGapStructural, cause: gapCauseAssetNotIndexed,
				from: a, to: b, configHash: cfg.ConfigHash,
			}
		}
	}
	// The height watermark: 003 only advances after a complete interval
	// commit, so b < N_u proves [a,b] persisted; anything else waits.
	if up == nil || b >= up.nextBlock {
		return &depositGap{
			class: depositGapTransient, cause: gapCauseBehindHead,
			from: a, to: b, configHash: cfg.ConfigHash,
		}
	}
	return nil
}

// verifyUpstreamIdentity compares the persisted 003 identity with the one
// recomputed from the shared env whitelist (R5 step 1).
func verifyUpstreamIdentity(up *upstreamState, expected string) error {
	if up.configHash != expected {
		return &upstreamDriftError{persisted: up.configHash, expected: expected}
	}
	return nil
}

// readUpstream reads 003's log_checkpoint row and verifies chain identity and
// whitelist identity. A missing row means 003 has committed no coverage yet
// (ok=false); a present row whose identity does not match the env recomputation
// is an upstream_drift refusal, never a wait.
//
// Statement order: this is always the first read of a unit — see
// readCoveredUnit for the R2 reason.
func (s *DepositScanner) readUpstream(ctx context.Context, q depositQuerier) (*upstreamState, error) {
	var (
		chainID, start, next int64
		hash                 string
	)
	err := q.QueryRow(ctx, readUpstreamCheckpointSQL, s.cfg.ChainID).Scan(&chainID, &start, &hash, &next)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("read upstream log checkpoint: %w", err)
	}
	up := &upstreamState{
		chainID:    chainID,
		startBlock: uint64(start),
		configHash: hash,
		nextBlock:  uint64(next),
	}
	// Chain identity: the proof never consumes a row that is not this chain's,
	// even if a future query shape loses the WHERE filter.
	if up.chainID != s.cfg.ChainID {
		return nil, fmt.Errorf("upstream log checkpoint chain identity mismatch: row chain_id=%d, configured %d",
			up.chainID, s.cfg.ChainID)
	}
	if err := verifyUpstreamIdentity(up, s.upstreamHash); err != nil {
		return nil, err
	}
	return up, nil
}

// readProgress reads deposit_checkpoint together with the latest
// deposit_config_history version and enforces the data-model Table 2
// integrity: both sides exist or neither does, and their (start_block,
// config_hash) agree. A nil progress is the empty-progress state (first unit);
// any one-sided or inconsistent state is corruption, reported and never
// repaired.
func (s *DepositScanner) readProgress(ctx context.Context, q depositQuerier) (*depositProgress, error) {
	var (
		start, next int64
		hash        string
	)
	cp := (*depositProgress)(nil)
	err := q.QueryRow(ctx, readDepositCheckpointSQL, s.cfg.ChainID).Scan(&start, &hash, &next)
	switch {
	case err == nil:
		cp = &depositProgress{startBlock: uint64(start), configHash: hash, nextBlock: uint64(next)}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, fmt.Errorf("read deposit checkpoint: %w", err)
	}

	var (
		versionSeq, hStart int64
		hHash              string
	)
	haveHistory := false
	err = q.QueryRow(ctx, readLatestDepositHistorySQL, s.cfg.ChainID).Scan(&versionSeq, &hStart, &hHash)
	switch {
	case err == nil:
		haveHistory = true
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, fmt.Errorf("read deposit config history: %w", err)
	}

	switch {
	case cp == nil && !haveHistory:
		return nil, nil
	case cp == nil:
		return nil, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d has deposit_config_history version %d but no deposit_checkpoint row", s.cfg.ChainID, versionSeq)}
	case !haveHistory:
		return nil, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d has deposit_checkpoint (start_block=%d config_hash=%s next_block=%d) but no deposit_config_history row",
			s.cfg.ChainID, cp.startBlock, cp.configHash, cp.nextBlock)}
	case cp.startBlock != uint64(hStart) || cp.configHash != hHash:
		return nil, &depositCorruptStateError{detail: fmt.Sprintf(
			"chain %d checkpoint (start_block=%d config_hash=%s) disagrees with latest history version %d (start_block=%d config_hash=%s)",
			s.cfg.ChainID, cp.startBlock, cp.configHash, versionSeq, hStart, hHash)}
	}
	cp.versionSeq = versionSeq
	return cp, nil
}

// rejectStreamPauses requires all three pause rows to be absent (FR-12,
// data-model 暂停写入的原子条件 step 3). It is the pre-transaction check;
// T006 re-reads the same statements under the coordination lock before any
// commit.
func (s *DepositScanner) rejectStreamPauses(ctx context.Context, q depositQuerier) error {
	for _, stream := range []struct {
		name string
		sql  string
	}{
		{"deposit_pause", depositPauseExistsSQL},
		{"log_pause", logPauseExistsSQL},
		{"indexer_pause", pauseExistsSQL},
	} {
		var one int
		err := q.QueryRow(ctx, stream.sql, s.cfg.ChainID).Scan(&one)
		switch {
		case err == nil:
			return &streamPauseError{stream: stream.name, chainID: s.cfg.ChainID}
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("read %s: %w", stream.name, err)
		}
	}
	return nil
}

// readCoveredUnit performs R2 step 1 for one requested unit [a, b]:
//
//  1. read 003's log_checkpoint first and align the whitelist identity;
//  2. classify the gap (transient/structural) from persisted state alone;
//  3. require all three pause rows absent;
//  4. bind every height in [a, b] to its canonical chain_blocks hash;
//  5. read the source rows for [a, b] and verify every referenced block hash
//     equals that canonical hash.
//
// The statement order is the R2 correctness argument, not style: 003 advances
// next_block only after committing a complete interval atomically, so reading
// the checkpoint first and the rows second means any concurrent 003 commit
// between the two reads can only add coverage, never remove it — a row set
// observed after `N_u > b` is complete by construction, while rows read first
// could belong to an interval the checkpoint does not yet confirm. The
// function performs no write and no RPC; T006 re-runs the same adjudication
// under the coordination lock before committing.
func (s *DepositScanner) readCoveredUnit(ctx context.Context, q depositQuerier, a, b uint64) (*depositUnit, error) {
	if a > b {
		return nil, fmt.Errorf("deposit unit [%d,%d] is empty", a, b)
	}
	up, err := s.readUpstream(ctx, q)
	if err != nil {
		return nil, err
	}
	// classify returns nil only for a present row with b < N_u, so up is
	// non-nil on the covered path.
	if gap := classifyUpstreamGap(s.cfg, up, a, b); gap != nil {
		return nil, gap
	}
	if err := s.rejectStreamPauses(ctx, q); err != nil {
		return nil, err
	}
	canonical, err := s.readCanonicalRange(ctx, q, a, b)
	if err != nil {
		return nil, err
	}
	rows, err := s.readSourceRows(ctx, q, a, b)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if want := canonical[row.blockNumber]; row.blockHash != want {
			return nil, &chainViewError{height: row.blockNumber, expected: want, actual: row.blockHash}
		}
	}
	return &depositUnit{upstream: *up, canonical: canonical, rows: rows}, nil
}

// readCanonicalRange reads the canonical=TRUE hash of every height in [a, b]
// with the point statement shared with 003. A missing or non-canonical block
// is a chain-view divergence, never an empty interval (data-model
// §canonical 读取范式, I4).
func (s *DepositScanner) readCanonicalRange(ctx context.Context, q depositQuerier, a, b uint64) (map[uint64]string, error) {
	canonical := make(map[uint64]string, b-a+1)
	for n := a; ; n++ {
		var hash string
		err := q.QueryRow(ctx, canonicalBlockHashSQL, s.cfg.ChainID, int64(n)).Scan(&hash)
		switch {
		case err == nil:
			canonical[n] = hash
		case errors.Is(err, pgx.ErrNoRows):
			return nil, &chainViewError{height: n, absent: true}
		default:
			return nil, fmt.Errorf("canonical coverage read at height %d: %w", n, err)
		}
		if n == b {
			return canonical, nil
		}
	}
}

// readSourceRows reads the stored Transfer rows for [a, b] in consumption
// order (block_number, log_index). It runs only after the upstream checkpoint
// has been read and the unit proven covered (see readCoveredUnit).
func (s *DepositScanner) readSourceRows(ctx context.Context, q depositQuerier, a, b uint64) ([]depositSourceLog, error) {
	rows, err := q.Query(ctx, readDepositSourceRowsSQL, s.cfg.ChainID, int64(a), int64(b))
	if err != nil {
		return nil, fmt.Errorf("read deposit source rows: %w", err)
	}
	defer rows.Close()
	var out []depositSourceLog
	for rows.Next() {
		var (
			number, index int64
			row           depositSourceLog
		)
		if err := rows.Scan(&number, &row.blockHash, &row.txHash, &index, &row.contract,
			&row.topic0, &row.topic1, &row.topic2, &row.data); err != nil {
			return nil, fmt.Errorf("scan deposit source row: %w", err)
		}
		row.blockNumber = uint64(number)
		row.logIndex = uint64(index)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deposit source rows: %w", err)
	}
	return out, nil
}

// Deposit read statements. All are chain-scoped; the upstream checkpoint
// select also returns chain_id so the proof can assert chain identity.
const (
	readUpstreamCheckpointSQL = `
SELECT chain_id, start_block, config_hash, next_block FROM log_checkpoint WHERE chain_id = $1`

	// readDepositCheckpointSQL reads the deposit progress row (no row = empty
	// progress).
	readDepositCheckpointSQL = `
SELECT start_block, config_hash, next_block FROM deposit_checkpoint WHERE chain_id = $1`

	// readLatestDepositHistorySQL reads the latest version by seq identity, not
	// by timestamp (data-model Table 4).
	readLatestDepositHistorySQL = `
SELECT version_seq, start_block, config_hash FROM deposit_config_history
WHERE chain_id = $1 ORDER BY version_seq DESC LIMIT 1`

	// depositPauseExistsSQL is one third of the FR-12 pause gate.
	depositPauseExistsSQL = `SELECT 1 FROM deposit_pause WHERE chain_id = $1`

	// readDepositSourceRowsSQL reads the [a,b] source rows in consumption
	// order; the R2 checkpoint-before-rows order is enforced by
	// readCoveredUnit.
	readDepositSourceRowsSQL = `
SELECT block_number, block_hash, tx_hash, log_index, contract, topic0, topic1, topic2, data
FROM erc20_transfer_logs
WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3
ORDER BY block_number, log_index`

	// insertDepositPauseSQL writes one first-wins pause row (T014): the
	// sequence mints a never-reused instance identity, revision starts at 1.
	// ON CONFLICT converges a concurrent first-wins race to zero rows.
	insertDepositPauseSQL = `
INSERT INTO deposit_pause (chain_id, height, kind, detail)
VALUES ($1, $2, $3, $4)
ON CONFLICT (chain_id) DO NOTHING`

	// deleteDepositPauseSQL removes one pause instance by identity + revision
	// (Table 3a manual release and old-instance fencing alike) and returns
	// the row content for the same-transaction audit row.
	deleteDepositPauseSQL = `
DELETE FROM deposit_pause WHERE chain_id = $1 AND pause_id = $2 AND revision = $3
RETURNING kind, height, detail`

	// insertReleasePauseAuditSQL records a manual release with the operator,
	// reason, instance, applicable revision and locked current version.
	insertReleasePauseAuditSQL = `
INSERT INTO deposit_pause_audit
    (chain_id, pause_id, revision, action, operator, reason, version_seq, kind, height, detail)
VALUES ($1, $2, $3, 'release', $4, $5, $6, $7, $8, $9)`
)

// Deposit loop defaults, used when the matching DepositConfig knob is zero.
// They mirror config.DefaultIndex* (research R7: the INDEX_* knobs are reused,
// no new environment variable).
const (
	depositDefaultPollInterval = time.Second
	depositDefaultRetryInitial = 200 * time.Millisecond
	depositDefaultRetryMax     = 30 * time.Second
)

// loopTimings resolves the polling/backoff knobs with repository defaults.
func (s *DepositScanner) loopTimings() (poll, retryInitial, retryMax time.Duration) {
	poll = s.cfg.PollInterval
	if poll <= 0 {
		poll = depositDefaultPollInterval
	}
	retryInitial = s.cfg.RetryInitial
	if retryInitial <= 0 {
		retryInitial = depositDefaultRetryInitial
	}
	retryMax = s.cfg.RetryMax
	if retryMax <= 0 {
		retryMax = depositDefaultRetryMax
	}
	if retryMax < retryInitial {
		retryMax = retryInitial
	}
	return poll, retryInitial, retryMax
}

// matchConfig freezes the T004 parsing view from the immutable configuration.
// The keys are normalized lowercase so the stored 0x hex forms compare by
// exact string identity (FR-04).
func (s *DepositScanner) matchConfig() depositMatchConfig {
	match := depositMatchConfig{
		startBlock: s.cfg.StartBlock,
		assets:     make(map[string]uint64, len(s.cfg.Assets)),
		watches:    make(map[string]uint64, len(s.cfg.Watches)),
	}
	for _, e := range s.cfg.Assets {
		match.assets[strings.ToLower(e.Address)] = e.Effective
	}
	for _, e := range s.cfg.Watches {
		match.watches[strings.ToLower(e.Address)] = e.Effective
	}
	return match
}

// verifyProgressIdentity applies the FR-06 comparison to a durable progress
// row: once the row exists its (start_block, config_hash) is frozen and must
// equal this scanner's configuration. A mismatch is a configuration change,
// reported and never continued silently.
func (s *DepositScanner) verifyProgressIdentity(p *depositProgress) error {
	if p == nil {
		return nil
	}
	if p.startBlock != s.cfg.StartBlock || p.configHash != s.cfg.ConfigHash {
		return &depositConfigMismatchError{detail: fmt.Sprintf(
			"chain %d checkpoint (start_block=%d config_hash=%s) differs from configured (start_block=%d config_hash=%s)",
			s.cfg.ChainID, p.startBlock, p.configHash, s.cfg.StartBlock, s.cfg.ConfigHash)}
	}
	return nil
}

// wait sleeps for d and reports false when ctx is done, so shutdown aborts a
// waiting loop immediately.
func (s *DepositScanner) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// --- T014 durable pause persistence (data-model Table 3) ---

// pauseEvidence is one persisted stop: the Table 3 kind, the height the row
// points at (unit first block or gap start), and the detail without the
// version stamp (the writer appends version=<seq> under the lock from the
// locked current version).
type pauseEvidence struct {
	kind   string
	height uint64
	detail string
}

// pauseEvidenceForStop maps a ServeLoop stop error to pause evidence, or nil
// when the condition writes no pause row: upstream drift, corruption and
// config mismatch belong to the operator/config domain (refuse and stop, zero
// damage), and an already-present pause converges by first-wins.
func pauseEvidenceForStop(err error, a, b uint64) *pauseEvidence {
	var gap *depositGap
	if errors.As(err, &gap) && gap.class == depositGapStructural {
		return &pauseEvidence{kind: "upstream_gap", height: gap.from,
			detail: fmt.Sprintf("class=structural gap=%d-%d cause=%s config=%s",
				gap.from, gap.to, gap.cause, gap.configHash)}
	}
	var cv *chainViewError
	if errors.As(err, &cv) {
		return &pauseEvidence{kind: "chain_view_changed", height: cv.height,
			detail: fmt.Sprintf("class=chain_view_changed height=%d expected=%s actual=%s",
				cv.height, cv.expected, cv.actual)}
	}
	var pe *depositParseError
	if errors.As(err, &pe) {
		return &pauseEvidence{kind: "validation_failed", height: pe.height,
			detail: fmt.Sprintf("%s unit=%d-%d", pe.detail, a, b)}
	}
	var conflict *depositIdentityConflictError
	if errors.As(err, &conflict) {
		return &pauseEvidence{kind: "validation_failed", height: a,
			detail: fmt.Sprintf("class=identity_conflict identity=%s unit=%d-%d",
				conflict.identity, a, b)}
	}
	return nil
}

// ReleaseDepositPause removes one pause instance by identity + revision with
// its audit row in the same transaction (Table 3a manual path). It writes no
// consumer state — checkpoint, history and observations are untouched
// (release is not authorization) — and performs no evidence re-verification
// itself: callers re-verify after release and rebuild on failure (the loop
// does this through the T014 writer).
//
// Outcome: (true, nil) deleted exactly one row and audited it; (false, nil)
// zero rows — the instance is gone or revised, so callers定性 via the audit
// table (T019) and must not touch the live instance; (*, err) on lease loss,
// audit failure (rollback) or DB errors. Operator and reason are required
// audit fields. The audit version is the locked current latest seq, or 0 when
// no version exists yet (pre-bootstrap pause).
func (s *DepositScanner) ReleaseDepositPause(ctx context.Context, lease *Lease, pauseID, revision int64, operator, reason string) (bool, error) {
	if lease == nil {
		return false, errors.New("deposit release: nil lease")
	}
	if operator == "" || reason == "" {
		return false, errors.New("deposit release: operator and reason are required audit fields")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin deposit release transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return false, fmt.Errorf("deposit release transaction statement guard: %w", err)
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.cfg.ChainID, lease.ownerID, lease.Token(), lease.ttl.Seconds()); err != nil {
		return false, fmt.Errorf("ensure release coordination row: %w", err)
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, s.cfg.ChainID).Scan(&owner, &token, &valid); err != nil {
		return false, fmt.Errorf("lock release coordination row: %w", err)
	}
	var one int
	if err := tx.QueryRow(ctx, leaseVerdictSQL, s.cfg.ChainID, lease.ownerID, lease.Token()).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("%w: release owner/fencing/expiry verdict failed", ErrLeaseLost)
	} else if err != nil {
		return false, fmt.Errorf("release lease verdict: %w", err)
	}
	var (
		kind, detail string
		height       int64
	)
	err = tx.QueryRow(ctx, deleteDepositPauseSQL, s.cfg.ChainID, pauseID, revision).Scan(&kind, &height, &detail)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("delete deposit pause row: %w", err)
	}
	var seq int64
	if err := tx.QueryRow(ctx, readAuthLatestHistorySQL, s.cfg.ChainID).Scan(&seq, new(int64), new(string), new(string), new(string)); errors.Is(err, pgx.ErrNoRows) {
		seq = 0
	} else if err != nil {
		return false, fmt.Errorf("release read latest version: %w", err)
	}
	if _, err := tx.Exec(ctx, insertReleasePauseAuditSQL,
		s.cfg.ChainID, pauseID, revision, operator, reason, seq, kind, height, detail); err != nil {
		return false, fmt.Errorf("insert release pause audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit deposit release: %w", err)
	}
	return true, nil
}

// persistDepositPause writes the pause row for a durable stop in a dedicated
// transaction (it never rides the rolled-back unit transaction: a batch
// rollback itself never writes a pause row). Outcomes: written, converged
// (row exists / evidence expired / lease lost) or given up after bounded
// retries on transient DB errors. Every outcome leaves the caller's stop
// error intact — the loop still stops.
func (s *DepositScanner) persistDepositPause(ctx context.Context, lease *Lease, ev *pauseEvidence, progress *depositProgress) {
	if ev == nil || lease == nil {
		return
	}
	_, retryInitial, retryMax := s.loopTimings()
	back := newBackoff(retryInitial, retryMax)
	for i := 0; i < 3; i++ {
		if s.tryPersistPause(ctx, lease, ev, progress) {
			return
		}
		if !s.wait(ctx, back.next()) {
			return
		}
	}
}

// tryPersistPause runs one Table 3 pause transaction. It reports true when no
// further attempt is needed (written, converged, or terminally refused) and
// false only on a transient DB error worth retrying.
func (s *DepositScanner) tryPersistPause(ctx context.Context, lease *Lease, ev *pauseEvidence, progress *depositProgress) bool {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after COMMIT
	if _, err := tx.Exec(ctx, writeGuard); err != nil {
		return false
	}
	if _, err := tx.Exec(ctx, ensureLeaseSQL, s.cfg.ChainID, lease.ownerID, lease.Token(), lease.ttl.Seconds()); err != nil {
		return false
	}
	var (
		owner string
		token int64
		valid bool
	)
	if err := tx.QueryRow(ctx, lockCoordSQL, s.cfg.ChainID).Scan(&owner, &token, &valid); err != nil {
		return false
	}
	// Atomic condition 1: the lease verdict. A dispossessed worker writes
	// nothing here; the loop still stops on its original error.
	var one int
	if err := tx.QueryRow(ctx, leaseVerdictSQL, s.cfg.ChainID, lease.ownerID, lease.Token()).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
		return true
	} else if err != nil {
		return false
	}
	// Atomic condition 2: first pause wins. A row already present means
	// another writer (or an earlier attempt whose COMMIT was uncertain)
	// established the pause: converge without a second row.
	if err := tx.QueryRow(ctx, depositPauseExistsSQL, s.cfg.ChainID).Scan(&one); err == nil {
		return true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	// Atomic condition 3: the evidence still holds — the checkpoint is
	// exactly the captured basis (any advance means the stop evidence
	// expired with it). History is never touched by this transaction.
	current, _, err := readAuthProgress(ctx, tx, s.cfg.ChainID)
	if err != nil {
		// Corruption is a terminal stop, not a pause write.
		var corrupt *depositCorruptStateError
		if errors.As(err, &corrupt) {
			return true
		}
		return false
	}
	switch {
	case progress == nil && current == nil:
	case progress == nil || current == nil:
		return true
	case current.startBlock != progress.startBlock || current.configHash != progress.configHash ||
		current.nextBlock != progress.nextBlock || current.versionSeq != progress.versionSeq:
		return true
	}
	var seq int64
	if progress != nil {
		seq = progress.versionSeq
	}
	detail := fmt.Sprintf("%s version=%d", ev.detail, seq)
	tag, err := tx.Exec(ctx, insertDepositPauseSQL, s.cfg.ChainID, int64(ev.height), ev.kind, detail)
	if err != nil {
		return false
	}
	if tag.RowsAffected() != 1 {
		// Lost a concurrent first-wins race: converge.
		return true
	}
	if err := tx.Commit(ctx); err != nil {
		// Uncertain COMMIT: the next attempt converges on the row if it
		// landed, or retries the write if it did not.
		return false
	}
	// Exactly one writer converges here: this attempt inserted the row, so
	// the pause event counts once (first-wins convergence never re-fires).
	if s.pauseObserver != nil {
		s.pauseObserver()
	}
	return true
}

// ServeLoop is the T006 consumption loop skeleton: while the coordinator holds
// the lease it repeatedly reads the durable progress, sizes one covered unit
// [a,b] (b = min(a+BatchBlocks-1, N_u-1): the batch is an upper bound and the
// exclusive upstream watermark N_u is the real end), proves it covered via
// T005's readCoveredUnit, parses and matches the rows (T004) and commits the
// unit atomically (depositcommit.go). It performs no acquisition and no
// renewal (research R1): checkLost reports a lost lease and aborts before any
// write. It returns nil on ctx cancellation, ErrLeaseLost when the lease is
// gone, and a stop error for every condition that must not continue.
//
// T006 stops explicitly on the durable conditions whose pause persistence,
// resume rules and authorization transitions are T014/T015/T025: structural
// gaps, stream pauses, upstream drift, chain-view divergence, identity
// conflicts and corrupted state return the diagnostic error (wrapped with the
// class) instead of defaulting to success or creating pause rows here.
// A watermark that covers nothing at a (N_u <= a) waits without advancing;
// version-isolation and stale progress outcomes abandon the unit and re-read
// the durable state.
func (s *DepositScanner) ServeLoop(ctx context.Context, lease *Lease, checkLost func() error) error {
	if lease == nil {
		return errors.New("deposit scanner: nil lease")
	}
	if checkLost == nil {
		checkLost = func() error { return nil }
	}
	poll, retryInitial, retryMax := s.loopTimings()
	back := newBackoff(retryInitial, retryMax)
	match := s.matchConfig()

	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := checkLost(); err != nil {
			return err
		}

		progress, err := s.readProgress(ctx, s.pool)
		if err != nil {
			var corrupt *depositCorruptStateError
			if errors.As(err, &corrupt) {
				return err
			}
			// The durable state is unreadable: bounded retry with zero
			// advance. The exact guards re-adjudicate the unit on retry.
			s.depState.Store(2)
			if !s.wait(ctx, back.next()) {
				return nil
			}
			continue
		}
		if err := s.verifyProgressIdentity(progress); err != nil {
			return err
		}

		a := s.cfg.StartBlock
		if progress != nil {
			a = progress.nextBlock
		}

		// Size the unit before the R2 read. BatchBlocks is an upper bound
		// (research R7), so the unit is trimmed to N_u-1; only N_u <= a
		// (nothing processable at the position) waits, which keeps the wait
		// bounded by upstream progress (research R5). This sizing read is an
		// extra checkpoint point read; readCoveredUnit re-reads the checkpoint
		// as its first statement, so the R2 order still holds (coverage can
		// only grow between the two reads).
		up, err := s.readUpstream(ctx, s.pool)
		if err != nil {
			var drift *upstreamDriftError
			if errors.As(err, &drift) {
				return err
			}
			s.depState.Store(2)
			if !s.wait(ctx, back.next()) {
				return nil
			}
			continue
		}
		nextExclusive := uint64(0)
		if up != nil {
			nextExclusive = up.nextBlock
		}
		b := depositCoveredEnd(a, s.cfg.BatchBlocks, nextExclusive)

		unit, err := s.readCoveredUnit(ctx, s.pool, a, b)
		if err != nil {
			// A cancellation is a shutdown, never a coverage verdict: a
			// context error surfacing from an in-flight read must not be
			// reported as a durable stop (the loop returns nil on ctx
			// cancellation).
			if ctx.Err() != nil {
				return nil
			}
			var gap *depositGap
			if errors.As(err, &gap) && gap.class == depositGapTransient {
				// The freshly read watermark still covers nothing at a: wait
				// for upstream progress, never raise the start or drop
				// required history (R5).
				s.depState.Store(1)
				if !s.wait(ctx, poll) {
					return nil
				}
				continue
			}
			// Structural gaps and chain-view divergence persist a pause row
			// (T014); drift, present pauses and corruption stop bare with
			// zero damage. The terminal sample keeps the stop verdict:
			// structural stops read 4, any pause-governed stop reads 3.
			var gapStop *depositGap
			var pauseStop *streamPauseError
			switch {
			case errors.As(err, &gapStop) && gapStop.class == depositGapStructural:
				s.depState.Store(4)
			case pauseEvidenceForStop(err, a, b) != nil:
				s.depState.Store(3)
			case errors.As(err, &pauseStop):
				s.depState.Store(3)
			}
			s.persistDepositPause(ctx, lease, pauseEvidenceForStop(err, a, b), progress)
			return err
		}

		batch, err := parseDepositLogs(match, unit.rows)
		if err != nil {
			// A deterministic invalid row fails the whole batch; no unit is
			// committed and the loop stops with a validation_failed pause row.
			// The refused row is the one invalid result for the counter.
			s.observeResult("invalid")
			s.depState.Store(3)
			s.persistDepositPause(ctx, lease, pauseEvidenceForStop(err, a, b), progress)
			return err
		}

		err = s.commitDepositUnit(ctx, lease, unit, batch, progress, a, b)
		switch {
		case err == nil:
			// Only a committed unit is counted: rolled-back units are retried
			// and counted once, on the attempt that makes them durable.
			for i := 0; i < len(batch.matched); i++ {
				s.observeResult("matched")
			}
			for i := 0; i < batch.zero; i++ {
				s.observeResult("zero")
			}
			for i := 0; i < batch.nomatch; i++ {
				s.observeResult("nomatch")
			}
			s.depState.Store(0)
			s.depNext.Store(b + 1)
			s.depHasProgress.Store(true)
			back.reset()
		case errors.Is(err, errDepositVersionMismatch), errors.Is(err, errStaleState):
			// The captured basis moved under us (version isolation or a
			// concurrent writer): nothing committed, re-read and continue.
			continue
		case errors.Is(err, ErrLeaseLost):
			return fmt.Errorf("%w (deposit commit: %v)", ErrLeaseLost, err)
		default:
			var (
				corrupt  *depositCorruptStateError
				cv       *chainViewError
				conflict *depositIdentityConflictError
				mismatch *depositConfigMismatchError
			)
			if errors.As(err, &corrupt) || errors.As(err, &cv) ||
				errors.As(err, &conflict) || errors.As(err, &mismatch) ||
				errors.Is(err, errDepositCoverageLost) {
				// Durable stop conditions: the atomic commit rolled back.
				// Pause-governed stops (conflict, chain-view) sample 3;
				// corruption, config mismatch and coverage loss stop bare.
				if pauseEvidenceForStop(err, a, b) != nil {
					s.depState.Store(3)
				}
				s.persistDepositPause(ctx, lease, pauseEvidenceForStop(err, a, b), progress)
				return err
			}
			// Unknown failure (for example a transient DB error): bounded
			// backoff with zero advance; the unit commit is atomic, so
			// retrying the same unit is safe.
			if !s.wait(ctx, back.next()) {
				return nil
			}
		}
	}
}
