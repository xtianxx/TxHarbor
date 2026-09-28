// Package sources carries the heavy read-only V1-V9 verification adapters of
// the 015 recovery feature (tasks.md T039/T040).
//
// The adapters live in this subpackage, not in package recovery, because the
// root package is imported by internal/app and must stay free of the heavy
// internal/txlifecycle, internal/events, internal/indexer,
// internal/reconciliation and internal/cache dependencies (internal/recovery
// doc.go, T001). This package is a leaf: nothing in the 015 core imports it,
// and it imports nothing that closes a test-build cycle for those packages.
//
// Read-only discipline (contracts/verification-items.md §1, F4/R1/INV-12):
// every adapter in this file observes through SELECT queries and the frozen
// read-only accessor surface of internal/txlifecycle (AttemptByID, Status,
// UnknownRecovery). No effectful entry point is reachable: the verification
// source interface exposes only Category and Observe, this package issues only
// SELECT reads against the data DB, and it never dispatches, never signs and
// never creates an intent/binding. A verification batch may write only through
// the T038 orchestrator into the control store (append-only evidence items and
// evidence gaps).
//
// Conclusion discipline (all fail-closed):
//
//   - a record missing at the restore point is unknown plus an FR-019
//     evidence gap; absence is never "never happened", "never paid" or safe to
//     re-execute (FR-016), and an intent is never rebuilt (FR-017);
//   - an explicit local/chain contradiction is divergent and names the
//     difference (FR-015);
//   - an unreadable/unprovable source (RPC down, coverage open, evidence
//     incomplete) is unknown plus a gap, never a default pass;
//   - the adapters never reassign a nonce and never move a binding.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
)

// quantityMethod is the one raw JSON-RPC quantity read the frozen *eth.Client
// surface does not expose (eth_getTransactionCount). It is read through the
// optional QuantityCaller; a nil caller keeps the chain-count side unreadable
// and every affected nonce scope conservative (unknown), never a default.
const quantityMethod = "eth_getTransactionCount"

// QuantityCaller is the raw JSON-RPC surface for eth_getTransactionCount.
// *github.com/ethereum/go-ethereum/rpc.Client satisfies it. It is an optional
// ChainOptions field: the 010-frozen *eth.Client cannot serve this call, and a
// missing caller never fabricates a count.
type QuantityCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

// ChainOptions constructs the V1-V4 read-only chain-fact adapters. Data and
// RPC are required; the pool may be a SELECT-only role (that is the strongest
// database-side attribution of zero verification writes).
type ChainOptions struct {
	Data       *pgxpool.Pool // data DB; the pool may be a SELECT-only role
	RPC        *eth.Client   // canonical chain reads
	ChainID    uint64
	DataTarget string
	// Now is a test seam for observation timestamps (nil means time.Now).
	Now func() time.Time
	// Quantity optionally supplies the eth_getTransactionCount surface
	// (V3). nil keeps the count side unknown.
	Quantity QuantityCaller
}

// NewChainSources builds the four read-only V1-V4 adapters.
func NewChainSources(opts ChainOptions) ([]recovery.VerificationSource, error) {
	if opts.Data == nil {
		return nil, errors.New("chain sources require the data-DB pool")
	}
	if opts.RPC == nil {
		return nil, errors.New("chain sources require the canonical chain client")
	}
	if opts.ChainID == 0 {
		return nil, errors.New("chain sources require a non-zero chain id")
	}
	if opts.ChainID > 1<<62 {
		return nil, fmt.Errorf("chain id %d is outside the supported range", opts.ChainID)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	base := &chainSourceBase{
		data:       opts.Data,
		rpc:        opts.RPC,
		quantity:   opts.Quantity,
		chainID:    opts.ChainID,
		dataTarget: opts.DataTarget,
		now:        now,
		txl:        txlifecycle.NewStore(opts.Data),
	}
	return []recovery.VerificationSource{
		&chainFactsSource{base: base},
		&withdrawalFactsSource{base: base},
		&nonceFactsSource{base: base},
		&signingFactsSource{base: base},
	}, nil
}

// The capability sets a missing/unprovable V1-V4 object blocks. The set is the
// directly related capability set (contracts/verification-items.md §1); the
// T038 gap writer conservatively expands it along the dependency matrix.
var (
	chainScanCaps          = []recovery.Capability{recovery.CapabilityChainScan}
	chainScanDepositCaps   = []recovery.Capability{recovery.CapabilityChainScan, recovery.CapabilityDepositConfirmation}
	withdrawalCaps         = []recovery.Capability{recovery.CapabilityNewWithdrawalCreation, recovery.CapabilityExistingWithdrawalRecovery}
	withdrawalRecoveryCaps = []recovery.Capability{recovery.CapabilityExistingWithdrawalRecovery}
)

// chainSourceBase carries the read-only dependencies shared by V1-V4. It holds
// no mutable state.
type chainSourceBase struct {
	data       *pgxpool.Pool
	rpc        *eth.Client
	quantity   QuantityCaller
	chainID    uint64
	dataTarget string
	now        func() time.Time
	txl        *txlifecycle.Store
}

// observationInstant is the timestamp recorded on every observation. Whole-
// second precision is deliberate: the orchestrator captures its evaluation
// instant immediately before reading the sources (T038), so a live read
// timestamped with sub-second precision would postdate that instant by
// microseconds and trip the evaluator's strict "future timestamp" rule. Whole
// seconds keep a live read inside its own evaluation instant while a
// configured freshness tolerance still ages the evidence.
func (b *chainSourceBase) observationInstant() time.Time {
	return b.now().UTC().Truncate(time.Second)
}

// observationInput is one assembled adapter observation.
type observationInput struct {
	category     recovery.VerificationCategory
	objectKey    string
	conclusion   recovery.VerificationConclusion
	reason       string
	facts        map[string]any
	evidenceRefs []string
	// directCaps names the directly affected capabilities when the conclusion
	// is unknown: a missing/unprovable record establishes an FR-019 gap.
	directCaps []recovery.Capability
	// required describes the external evidence needed to close the gap.
	required map[string]any
}

// observationCollector accumulates observations deterministically.
type observationCollector struct {
	base *chainSourceBase
	out  []recovery.SourceObservation
}

func (c *observationCollector) add(in observationInput) error {
	obs, err := c.base.observe(in)
	if err != nil {
		return err
	}
	c.out = append(c.out, obs)
	return nil
}

// observe renders one SourceObservation. A gap bundle is attached only to an
// unknown conclusion (missing/unprovable evidence): T038 caps a gapped item at
// unknown by construction, so a gapped observation must not be divergent
// (an explicit contradiction is reported as divergent, without a gap).
func (b *chainSourceBase) observe(in observationInput) (recovery.SourceObservation, error) {
	facts := in.facts
	if facts == nil {
		facts = map[string]any{}
	}
	sourcesPayload, err := json.Marshal(facts)
	if err != nil {
		return recovery.SourceObservation{}, fmt.Errorf("encode %s/%s sources payload: %w", in.category, in.objectKey, err)
	}
	scope := map[string]any{
		"chain_id": b.chainID,
		"category": string(in.category),
		"object":   in.objectKey,
	}
	if b.dataTarget != "" {
		scope["data_target"] = b.dataTarget
	}
	scopePayload, err := json.Marshal(scope)
	if err != nil {
		return recovery.SourceObservation{}, fmt.Errorf("encode %s/%s scope payload: %w", in.category, in.objectKey, err)
	}
	observation := recovery.SourceObservation{
		ObjectKey:    in.objectKey,
		Scope:        scopePayload,
		Sources:      sourcesPayload,
		Conclusion:   in.conclusion,
		Reason:       in.reason,
		EvidenceRefs: in.evidenceRefs,
		ObservedAt:   b.observationInstant(),
	}
	if in.conclusion == recovery.ConclusionUnknown && len(in.directCaps) > 0 {
		gap, err := b.gapEvidence(in.objectKey, in.directCaps, facts, in.required)
		if err != nil {
			return recovery.SourceObservation{}, err
		}
		observation.GapEvidence = gap
	}
	return observation, nil
}

// gapEvidence renders the FR-019 evidence bundle of one unprovable object.
func (b *chainSourceBase) gapEvidence(objectKey string, direct []recovery.Capability, existing, required map[string]any) (*recovery.GapEvidence, error) {
	timeline, err := json.Marshal(map[string]any{
		"object":      objectKey,
		"chain_id":    b.chainID,
		"observed_at": b.observationInstant().Format(time.RFC3339),
	})
	if err != nil {
		return nil, fmt.Errorf("encode gap timeline for %s: %w", objectKey, err)
	}
	existingPayload, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("encode gap existing evidence for %s: %w", objectKey, err)
	}
	if required == nil {
		required = externalRequirement("manual reconciliation with trusted external evidence")
	}
	requiredPayload, err := json.Marshal(required)
	if err != nil {
		return nil, fmt.Errorf("encode gap required evidence for %s: %w", objectKey, err)
	}
	risk, err := json.Marshal(map[string]any{"unproven_external_effect": true, "object": objectKey})
	if err != nil {
		return nil, fmt.Errorf("encode gap risk for %s: %w", objectKey, err)
	}
	return &recovery.GapEvidence{
		Timeline:             timeline,
		ExistingEvidence:     existingPayload,
		RequiredEvidence:     requiredPayload,
		Risk:                 risk,
		AffectedCapabilities: direct,
	}, nil
}

// externalRequirement renders the required-evidence payload of a gap.
func externalRequirement(parts ...string) map[string]any {
	return map[string]any{"external": parts}
}

// lowerHash normalizes a 0x hex hash for comparison (stored values are
// lowercase; RPC responses are not guaranteed to be).
func lowerHash(hash string) string {
	return strings.ToLower(strings.TrimSpace(hash))
}

// sameDecimal compares two decimal integer strings exactly (NUMERIC columns
// are read as text; floats are never used for money or nonces).
func sameDecimal(a, b string) bool {
	left, ok := new(big.Int).SetString(strings.TrimSpace(a), 10)
	if !ok {
		return false
	}
	right, ok := new(big.Int).SetString(strings.TrimSpace(b), 10)
	if !ok {
		return false
	}
	return left.Cmp(right) == 0
}

// ---------------------------------------------------------------------------
// V1: chain facts vs PG (canonical RPC + indexer/log/deposit/reorg tables)
// ---------------------------------------------------------------------------

type chainFactsSource struct{ base *chainSourceBase }

// Category implements recovery.VerificationSource.
func (s *chainFactsSource) Category() recovery.VerificationCategory { return recovery.VerificationV1 }

// Observe reads the restored indexer frontier, the log and deposit coverage
// and any active 006 reorg recovery, and compares them with the canonical
// chain view. A chain that leads the restored frontier is never consistent
// (F4); an unreachable chain, an unconfirmed deposit or an unresolved reorg is
// unknown.
func (s *chainFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	b := s.base
	c := &observationCollector{base: b}

	tip, tipErr := b.rpc.BlockNumber(ctx)
	tipOK := tipErr == nil
	tipClass := ""
	if !tipOK {
		tipClass = string(eth.KindOf(tipErr))
	}

	// --- indexer_checkpoint ---
	if err := s.observeIndexerCheckpoint(ctx, c, tip, tipOK, tipClass); err != nil {
		return nil, err
	}
	// --- log_checkpoint (+ erc20_transfer_logs coverage) ---
	if err := s.observeLogCheckpoint(ctx, c, tip, tipOK, tipClass); err != nil {
		return nil, err
	}
	// --- deposit_checkpoint / deposit_observations / confirmation policy ---
	if err := s.observeDepositCoverage(ctx, c, tip, tipOK, tipClass); err != nil {
		return nil, err
	}
	// --- active 006 reorg recovery rows ---
	if err := s.observeReorgRecovery(ctx, c); err != nil {
		return nil, err
	}
	return c.out, nil
}

func (s *chainFactsSource) observeIndexerCheckpoint(ctx context.Context, c *observationCollector, tip uint64, tipOK bool, tipClass string) error {
	b := s.base
	objectKey := fmt.Sprintf("v1/indexer_checkpoint?chain_id=%d", b.chainID)
	refs := []string{
		fmt.Sprintf("data:indexer_checkpoint?chain_id=%d", b.chainID),
		fmt.Sprintf("data:chain_blocks?chain_id=%d", b.chainID),
		fmt.Sprintf("chain:eth_getBlockByNumber?chain_id=%d", b.chainID),
	}

	var (
		height      int64
		startHeight int64
		hash        string
		updatedAt   time.Time
	)
	err := b.data.QueryRow(ctx,
		`SELECT height, block_hash, start_height, updated_at
		   FROM indexer_checkpoint WHERE chain_id = $1`, b.chainID).
		Scan(&height, &hash, &startHeight, &updatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no indexer_checkpoint row exists at the restore point; the indexed chain frontier cannot be " +
				"established and a missing record is not evidence that no chain facts exist (FR-016)",
			facts:        map[string]any{"table": "indexer_checkpoint", "found": false},
			evidenceRefs: refs,
			directCaps:   chainScanDepositCaps,
			required: externalRequirement(
				"the indexer checkpoint row from a trusted copy of the data DB",
				"canonical chain evidence for the restored frontier",
			),
		})
	case err != nil:
		return fmt.Errorf("read indexer checkpoint: %w", err)
	}

	facts := map[string]any{
		"table":                 "indexer_checkpoint",
		"checkpoint_height":     height,
		"checkpoint_hash":       lowerHash(hash),
		"start_height":          startHeight,
		"checkpoint_updated_at": updatedAt.UTC().Format(time.RFC3339Nano),
	}
	if tipOK {
		facts["chain_head"] = tip
	} else {
		facts["chain_head_error"] = tipClass
	}

	// The checkpoint must point at a stored, canonical chain_blocks row.
	var canonical bool
	cerr := b.data.QueryRow(ctx,
		`SELECT canonical FROM chain_blocks WHERE chain_id = $1 AND number = $2 AND hash = $3`,
		b.chainID, height, lowerHash(hash)).Scan(&canonical)
	blockMissing := errors.Is(cerr, pgx.ErrNoRows)
	if cerr != nil && !blockMissing {
		return fmt.Errorf("read chain block %d: %w", height, cerr)
	}

	if blockMissing {
		facts["chain_blocks_row"] = "missing"
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the indexer checkpoint names block %d (%s) that is not stored in chain_blocks; "+
				"the restored chain frontier is unprovable", height, lowerHash(hash)),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanDepositCaps,
			required: externalRequirement(
				"the chain_blocks row for the checkpointed height from a trusted copy of the data DB",
				"canonical chain evidence for the restored frontier",
			),
		})
	}
	if !canonical {
		facts["chain_blocks_row"] = "non_canonical"
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the indexer checkpoint points at block %d (%s) that the restore point marks "+
				"non-canonical; the local frontier contradicts its own chain facts", height, lowerHash(hash)),
			facts:        facts,
			evidenceRefs: refs,
		})
	}
	facts["chain_blocks_row"] = "canonical"

	header, herr := b.rpc.HeaderByNumber(ctx, big.NewInt(height))
	switch {
	case herr != nil && eth.KindOf(herr) == eth.KindNotFound:
		facts["canonical_header"] = "not_found"
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the canonical chain does not reach the checkpointed height %d: confirmations are "+
				"not reached and the restored block identity is unverifiable", height),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanDepositCaps,
			required: externalRequirement(
				"a canonical chain view that reaches the checkpointed height",
			),
		})
	case herr != nil:
		facts["canonical_header_error"] = string(eth.KindOf(herr))
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the canonical chain read for height %d is unavailable (%s); the chain fact "+
				"cannot be compared", height, string(eth.KindOf(herr))),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanDepositCaps,
			required: externalRequirement(
				"a reachable canonical RPC endpoint",
			),
		})
	case lowerHash(header.Hash().Hex()) != lowerHash(hash):
		facts["canonical_hash"] = lowerHash(header.Hash().Hex())
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the canonical chain serves block %d with hash %s while the restore point records "+
				"%s; the indexed block identity differs from the chain", height,
				lowerHash(header.Hash().Hex()), lowerHash(hash)),
			facts:        facts,
			evidenceRefs: refs,
		})
	}
	facts["canonical_hash"] = lowerHash(header.Hash().Hex())

	switch {
	case !tipOK:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain head is unreadable (%s); the restored frontier cannot be compared with "+
				"the current chain", tipClass),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanDepositCaps,
			required: externalRequirement(
				"a reachable canonical RPC endpoint",
			),
		})
	case tip < uint64(height):
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the restored checkpoint height %d is beyond the current chain head %d; the local "+
				"frontier claims blocks the chain does not have", height, tip),
			facts:        facts,
			evidenceRefs: refs,
		})
	case tip > uint64(height):
		facts["chain_lead_blocks"] = tip - uint64(height)
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain head %d leads the restored indexed frontier %d by %d block(s); chain "+
				"facts after the restore point cannot be excluded (F4)", tip, height, tip-uint64(height)),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanDepositCaps,
			required: externalRequirement(
				"indexed chain facts covering the blocks after the restored frontier",
				"operator reconciliation of post-frontier chain activity",
			),
		})
	}
	return c.add(observationInput{
		category:     recovery.VerificationV1,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

func (s *chainFactsSource) observeLogCheckpoint(ctx context.Context, c *observationCollector, tip uint64, tipOK bool, tipClass string) error {
	b := s.base
	objectKey := fmt.Sprintf("v1/log_checkpoint?chain_id=%d", b.chainID)
	refs := []string{
		fmt.Sprintf("data:log_checkpoint?chain_id=%d", b.chainID),
		fmt.Sprintf("data:erc20_transfer_logs?chain_id=%d", b.chainID),
	}

	var (
		startBlock int64
		nextBlock  int64
		configHash string
		updatedAt  time.Time
	)
	err := b.data.QueryRow(ctx,
		`SELECT start_block, next_block, config_hash, updated_at
		   FROM log_checkpoint WHERE chain_id = $1`, b.chainID).
		Scan(&startBlock, &nextBlock, &configHash, &updatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no log_checkpoint row exists at the restore point; the indexed log coverage cannot be " +
				"established and a missing record is not evidence that no logs exist (FR-016)",
			facts:        map[string]any{"table": "log_checkpoint", "found": false},
			evidenceRefs: refs,
			directCaps:   chainScanCaps,
			required: externalRequirement(
				"the log checkpoint row from a trusted copy of the data DB",
				"canonical chain logs for the restored coverage",
			),
		})
	case err != nil:
		return fmt.Errorf("read log checkpoint: %w", err)
	}

	var (
		logCount    int64
		maxLogBlock int64
	)
	if err := b.data.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(block_number), -1) FROM erc20_transfer_logs WHERE chain_id = $1`,
		b.chainID).Scan(&logCount, &maxLogBlock); err != nil {
		return fmt.Errorf("read erc20 transfer logs: %w", err)
	}

	facts := map[string]any{
		"table":                 "log_checkpoint",
		"start_block":           startBlock,
		"next_block":            nextBlock,
		"config_hash":           strings.TrimSpace(configHash),
		"checkpoint_updated_at": updatedAt.UTC().Format(time.RFC3339Nano),
		"log_rows":              logCount,
		"max_log_block":         maxLogBlock,
	}
	if tipOK {
		facts["chain_head"] = tip
	} else {
		facts["chain_head_error"] = tipClass
	}

	switch {
	case !tipOK:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain head is unreadable (%s); the indexed log coverage cannot be compared "+
				"with the chain", tipClass),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanCaps,
			required:     externalRequirement("a reachable canonical RPC endpoint"),
		})
	case nextBlock > int64(tip)+1:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the log checkpoint claims coverage through block %d while the chain head is %d; "+
				"the local log frontier reaches beyond the chain", nextBlock-1, tip),
			facts:        facts,
			evidenceRefs: refs,
		})
	case nextBlock <= int64(tip):
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the indexed log coverage stops at block %d while the chain head is %d; chain logs "+
				"after the restored coverage are unindexed and cannot be excluded", nextBlock-1, tip),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   chainScanCaps,
			required: externalRequirement(
				"indexed logs covering the blocks after the restored coverage",
				"operator reconciliation of post-frontier logs",
			),
		})
	case maxLogBlock > int64(tip):
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("erc20_transfer_logs reaches block %d beyond the chain head %d; the stored log rows "+
				"contradict the chain", maxLogBlock, tip),
			facts:        facts,
			evidenceRefs: refs,
		})
	}
	return c.add(observationInput{
		category:     recovery.VerificationV1,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

func (s *chainFactsSource) observeDepositCoverage(ctx context.Context, c *observationCollector, tip uint64, tipOK bool, tipClass string) error {
	b := s.base
	objectKey := fmt.Sprintf("v1/deposit_checkpoint?chain_id=%d", b.chainID)
	refs := []string{
		fmt.Sprintf("data:deposit_checkpoint?chain_id=%d", b.chainID),
		fmt.Sprintf("data:deposit_observations?chain_id=%d", b.chainID),
		fmt.Sprintf("data:confirmation_policy_history?chain_id=%d", b.chainID),
	}

	var (
		startBlock int64
		nextBlock  int64
		configHash string
		updatedAt  time.Time
	)
	err := b.data.QueryRow(ctx,
		`SELECT start_block, next_block, config_hash, updated_at
		   FROM deposit_checkpoint WHERE chain_id = $1`, b.chainID).
		Scan(&startBlock, &nextBlock, &configHash, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no deposit_checkpoint row exists at the restore point; the scanned deposit coverage cannot be " +
				"established and a missing record is not evidence that no deposit happened (FR-016)",
			facts:        map[string]any{"table": "deposit_checkpoint", "found": false},
			evidenceRefs: refs,
			directCaps:   []recovery.Capability{recovery.CapabilityDepositConfirmation},
			required: externalRequirement(
				"the deposit checkpoint row from a trusted copy of the data DB",
				"canonical chain deposit evidence for the restored coverage",
			),
		})
	}
	if err != nil {
		return fmt.Errorf("read deposit checkpoint: %w", err)
	}

	var pendingDeposits int64
	if err := b.data.QueryRow(ctx,
		`SELECT count(*) FROM deposit_observations WHERE chain_id = $1 AND status = 'pending'`,
		b.chainID).Scan(&pendingDeposits); err != nil {
		return fmt.Errorf("count pending deposit observations: %w", err)
	}
	var (
		policySeq int64
		threshold int64
	)
	perr := b.data.QueryRow(ctx,
		`SELECT policy_seq, threshold FROM confirmation_policy_history
		  WHERE chain_id = $1 ORDER BY policy_seq DESC LIMIT 1`, b.chainID).
		Scan(&policySeq, &threshold)
	policyFound := perr == nil
	if perr != nil && !errors.Is(perr, pgx.ErrNoRows) {
		return fmt.Errorf("read confirmation policy: %w", perr)
	}

	facts := map[string]any{
		"table":                 "deposit_checkpoint",
		"start_block":           startBlock,
		"next_block":            nextBlock,
		"config_hash":           strings.TrimSpace(configHash),
		"checkpoint_updated_at": updatedAt.UTC().Format(time.RFC3339Nano),
		"pending_deposits":      pendingDeposits,
	}
	if policyFound {
		facts["confirmation_policy_seq"] = policySeq
		facts["confirmation_threshold"] = threshold
	} else {
		facts["confirmation_policy"] = "missing"
	}
	if tipOK {
		facts["chain_head"] = tip
	} else {
		facts["chain_head_error"] = tipClass
	}

	switch {
	case !tipOK:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain head is unreadable (%s); deposit confirmations cannot be evaluated",
				tipClass),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   []recovery.Capability{recovery.CapabilityDepositConfirmation},
			required:     externalRequirement("a reachable canonical RPC endpoint"),
		})
	case pendingDeposits > 0 && !policyFound:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("%d deposit observation(s) remain pending and no confirmation_policy_history row "+
				"exists at the restore point; the confirmation basis is unprovable", pendingDeposits),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   []recovery.Capability{recovery.CapabilityDepositConfirmation},
			required: externalRequirement(
				"the confirmation policy version in effect at the restore point",
				"canonical confirmation evidence for each pending deposit",
			),
		})
	case pendingDeposits > 0:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("%d deposit observation(s) remain pending (policy_seq=%d threshold=%d); their "+
				"confirmation/conversion state is not closed", pendingDeposits, policySeq, threshold),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   []recovery.Capability{recovery.CapabilityDepositConfirmation},
			required: externalRequirement(
				"canonical confirmation evidence for each pending deposit",
				"the confirmed/orphaned conversion decision for each pending deposit",
			),
		})
	case nextBlock > int64(tip)+1:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the deposit checkpoint claims coverage through block %d while the chain head is "+
				"%d; the local deposit frontier reaches beyond the chain", nextBlock-1, tip),
			facts:        facts,
			evidenceRefs: refs,
		})
	case nextBlock <= int64(tip):
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the scanned deposit coverage stops at block %d while the chain head is %d; deposits "+
				"after the restored coverage cannot be excluded", nextBlock-1, tip),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   []recovery.Capability{recovery.CapabilityDepositConfirmation},
			required: externalRequirement(
				"scanned deposit coverage for the blocks after the restored frontier",
				"operator reconciliation of post-frontier deposits",
			),
		})
	case !policyFound:
		return c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: "no confirmation_policy_history row exists at the restore point; the confirmation basis of the " +
				"restored deposit state is unprovable",
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   []recovery.Capability{recovery.CapabilityDepositConfirmation},
			required: externalRequirement(
				"the confirmation policy version in effect at the restore point",
			),
		})
	}
	return c.add(observationInput{
		category:     recovery.VerificationV1,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

func (s *chainFactsSource) observeReorgRecovery(ctx context.Context, c *observationCollector) error {
	b := s.base
	rows, err := b.data.Query(ctx,
		`SELECT recovery_id, phase, bound_old_number, bound_old_hash, COALESCE(new_tip_number, -1), updated_at
		   FROM reorg_recovery WHERE chain_id = $1 ORDER BY detected_at, recovery_id`, b.chainID)
	if err != nil {
		return fmt.Errorf("read reorg recovery: %w", err)
	}
	type reorgRow struct {
		recoveryID     string
		phase          string
		boundOldNumber int64
		boundOldHash   string
		newTipNumber   int64
		updatedAt      time.Time
	}
	var active []reorgRow
	for rows.Next() {
		var row reorgRow
		if err := rows.Scan(&row.recoveryID, &row.phase, &row.boundOldNumber, &row.boundOldHash,
			&row.newTipNumber, &row.updatedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan reorg recovery: %w", err)
		}
		active = append(active, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read reorg recovery: %w", err)
	}

	for _, row := range active {
		lastEvent := ""
		lastEventAt := time.Time{}
		eventErr := b.data.QueryRow(ctx,
			`SELECT event, at FROM reorg_recovery_events
			  WHERE chain_id = $1 AND recovery_id = $2 ORDER BY event_seq DESC LIMIT 1`,
			b.chainID, row.recoveryID).Scan(&lastEvent, &lastEventAt)
		if eventErr != nil && !errors.Is(eventErr, pgx.ErrNoRows) {
			return fmt.Errorf("read reorg recovery event %s: %w", row.recoveryID, eventErr)
		}
		facts := map[string]any{
			"table":               "reorg_recovery",
			"recovery_id":         row.recoveryID,
			"phase":               row.phase,
			"bound_old_number":    row.boundOldNumber,
			"bound_old_hash":      lowerHash(row.boundOldHash),
			"new_tip_number":      row.newTipNumber,
			"recovery_updated_at": row.updatedAt.UTC().Format(time.RFC3339Nano),
		}
		if lastEvent != "" {
			facts["last_event"] = lastEvent
			facts["last_event_at"] = lastEventAt.UTC().Format(time.RFC3339Nano)
		}
		objectKey := fmt.Sprintf("v1/reorg_recovery?chain_id=%d&recovery_id=%s", b.chainID, row.recoveryID)
		if err := c.add(observationInput{
			category:   recovery.VerificationV1,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("an active 006 reorg recovery (recovery_id=%s phase=%s) is open at the restore "+
				"point; chain facts before its completion are unresolved", row.recoveryID, row.phase),
			facts: facts,
			evidenceRefs: []string{
				fmt.Sprintf("data:reorg_recovery?chain_id=%d&recovery_id=%s", b.chainID, row.recoveryID),
				fmt.Sprintf("data:reorg_recovery_events?chain_id=%d&recovery_id=%s", b.chainID, row.recoveryID),
			},
			directCaps: chainScanCaps,
			required: externalRequirement(
				"the completed reorg recovery outcome (auto_completed/released) or operator reconciliation",
			),
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// V2: withdrawal requests / payment intents / authorizations
// ---------------------------------------------------------------------------

type withdrawalFactsSource struct{ base *chainSourceBase }

// Category implements recovery.VerificationSource.
func (s *withdrawalFactsSource) Category() recovery.VerificationCategory {
	return recovery.VerificationV2
}

// Observe reads the restored withdrawal request / payment intent /
// authorization triple per request. A missing intent or authorization is
// unknown plus a gap and the intent is never rebuilt (FR-016/FR-017); an
// explicit request/authorization contradiction is divergent; only a completed
// intent whose projection has converged is consistent.
func (s *withdrawalFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	b := s.base
	c := &observationCollector{base: b}

	rows, err := b.data.Query(ctx,
		`SELECT request_id, caller_id, authorization_id, chain_id, asset, recipient, amount::text, status, created_at
		   FROM withdrawal_requests ORDER BY request_id`)
	if err != nil {
		return nil, fmt.Errorf("read withdrawal requests: %w", err)
	}
	type requestRow struct {
		requestID       string
		callerID        int64
		authorizationID string
		chainID         int64
		asset           string
		recipient       string
		amount          string
		status          string
		createdAt       time.Time
	}
	var requests []requestRow
	for rows.Next() {
		var row requestRow
		if err := rows.Scan(&row.requestID, &row.callerID, &row.authorizationID, &row.chainID,
			&row.asset, &row.recipient, &row.amount, &row.status, &row.createdAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan withdrawal request: %w", err)
		}
		requests = append(requests, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read withdrawal requests: %w", err)
	}

	if len(requests) == 0 {
		return c.out, c.add(observationInput{
			category:   recovery.VerificationV2,
			objectKey:  "v2/withdrawal_requests?scope=all",
			conclusion: recovery.ConclusionUnknown,
			reason: "no withdrawal request rows exist at the restore point; absence is not evidence that no " +
				"request or payment intent ever existed (FR-016), and the verification never fabricates one (FR-017)",
			facts: map[string]any{"table": "withdrawal_requests", "found": false},
			evidenceRefs: []string{
				"data:withdrawal_requests?scope=all",
				"data:payment_intents?scope=all",
				"data:withdrawal_authorizations?scope=all",
			},
			directCaps: withdrawalCaps,
			required: externalRequirement(
				"the withdrawal request / payment intent records from a trusted copy of the data DB",
				"external payment evidence for the restored window",
			),
		})
	}

	for _, request := range requests {
		if err := s.observeRequest(ctx, c, request.requestID, request.callerID, request.authorizationID,
			request.chainID, request.asset, request.recipient, request.amount, request.status, request.createdAt); err != nil {
			return nil, err
		}
	}
	return c.out, nil
}

func (s *withdrawalFactsSource) observeRequest(
	ctx context.Context, c *observationCollector,
	requestID string, callerID int64, authorizationID string, chainID int64,
	asset, recipient, amount, status string, createdAt time.Time,
) error {
	b := s.base
	objectKey := "v2/withdrawal_request?" + requestID
	refs := []string{
		"data:withdrawal_requests?request_id=" + requestID,
		"data:payment_intents?request_id=" + requestID,
		"data:withdrawal_authorizations?authorization_id=" + authorizationID,
	}
	facts := map[string]any{
		"table":            "withdrawal_requests",
		"request_id":       requestID,
		"caller_id":        callerID,
		"authorization_id": authorizationID,
		"chain_id":         chainID,
		"asset":            lowerHash(asset),
		"recipient":        lowerHash(recipient),
		"amount":           strings.TrimSpace(amount),
		"status":           status,
		"created_at":       createdAt.UTC().Format(time.RFC3339Nano),
	}

	// --- withdrawal_authorizations ---
	var (
		authCallerID   int64
		authChainID    int64
		authAsset      string
		authRecipient  string
		authAmount     string
		authState      string
		authExpiresAt  *time.Time
		authSuppliedAt time.Time
	)
	authErr := b.data.QueryRow(ctx,
		`SELECT caller_id, chain_id, asset, recipient, amount::text, state, expires_at, supplied_at
		   FROM withdrawal_authorizations WHERE authorization_id = $1`, authorizationID).
		Scan(&authCallerID, &authChainID, &authAsset, &authRecipient, &authAmount, &authState,
			&authExpiresAt, &authSuppliedAt)
	authFound := authErr == nil
	if authErr != nil && !errors.Is(authErr, pgx.ErrNoRows) {
		return fmt.Errorf("read withdrawal authorization %s: %w", authorizationID, authErr)
	}
	if !authFound {
		facts["authorization"] = "missing"
		return c.add(observationInput{
			category:   recovery.VerificationV2,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("withdrawal request %s has no withdrawal_authorizations row at the restore point; "+
				"the authorization governing the request cannot be proven and a missing record is not permission "+
				"to re-create it (FR-016/FR-017)", requestID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalCaps,
			required: externalRequirement(
				"the withdrawal authorization row from a trusted copy of the data DB",
				"proof of whether the authorized payment was executed externally",
			),
		})
	}
	authorization := map[string]any{
		"state":       authState,
		"supplied_at": authSuppliedAt.UTC().Format(time.RFC3339Nano),
	}
	if authExpiresAt != nil {
		authorization["expires_at"] = authExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	facts["authorization"] = authorization

	if authCallerID != callerID || authChainID != chainID ||
		lowerHash(authAsset) != lowerHash(asset) || lowerHash(authRecipient) != lowerHash(recipient) ||
		!sameDecimal(authAmount, amount) {
		return c.add(observationInput{
			category:   recovery.VerificationV2,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("withdrawal request %s and its authorization %s disagree on caller/chain/asset/"+
				"recipient/amount; the restored records contradict each other", requestID, authorizationID),
			facts:        facts,
			evidenceRefs: refs,
		})
	}

	// --- withdrawal_authorization_scopes (010): a grant without a scope row
	// stays valid (pre-extension stock, PB-FR-06); a present scope must agree.
	var (
		scopeRequestID string
		scopeIntentID  string
		scopeAddress   string
	)
	scopeErr := b.data.QueryRow(ctx,
		`SELECT request_id, intent_id, sender FROM withdrawal_authorization_scopes
		  WHERE authorization_id = $1`, authorizationID).
		Scan(&scopeRequestID, &scopeIntentID, &scopeAddress)
	scopeFound := scopeErr == nil
	if scopeErr != nil && !errors.Is(scopeErr, pgx.ErrNoRows) {
		return fmt.Errorf("read authorization scope %s: %w", authorizationID, scopeErr)
	}

	// --- payment_intents ---
	var (
		intentID      string
		intentState   string
		stateVersion  int64
		intentUpdated time.Time
	)
	intentErr := b.data.QueryRow(ctx,
		`SELECT intent_id, state, state_version, updated_at FROM payment_intents WHERE request_id = $1`,
		requestID).Scan(&intentID, &intentState, &stateVersion, &intentUpdated)
	intentFound := intentErr == nil
	if intentErr != nil && !errors.Is(intentErr, pgx.ErrNoRows) {
		return fmt.Errorf("read payment intent for %s: %w", requestID, intentErr)
	}

	if scopeFound {
		if scopeRequestID != requestID || (intentFound && scopeIntentID != intentID) {
			facts["authorization_scope"] = map[string]any{
				"request_id": scopeRequestID, "intent_id": scopeIntentID, "sender": lowerHash(scopeAddress),
			}
			return c.add(observationInput{
				category:   recovery.VerificationV2,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionDivergent,
				reason: fmt.Sprintf("the authorization scope of %s names request %s / intent %s, which contradicts "+
					"the restored request/intent identity", authorizationID, scopeRequestID, scopeIntentID),
				facts:        facts,
				evidenceRefs: refs,
			})
		}
		facts["authorization_scope"] = map[string]any{
			"request_id": scopeRequestID, "intent_id": scopeIntentID, "sender": lowerHash(scopeAddress),
		}
	} else {
		facts["authorization_scope"] = "absent"
	}

	if !intentFound {
		facts["payment_intent"] = "missing"
		return c.add(observationInput{
			category:   recovery.VerificationV2,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("withdrawal request %s has no payment_intents row at the restore point; whether the "+
				"payment happened externally cannot be proven from absence, and the intent is never rebuilt "+
				"(FR-016/FR-017)", requestID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalCaps,
			required: externalRequirement(
				"the payment intent row from a trusted copy of the data DB",
				"proof of whether the authorized payment was executed externally",
			),
		})
	}
	facts["payment_intent"] = map[string]any{
		"intent_id":     intentID,
		"state":         intentState,
		"state_version": stateVersion,
		"updated_at":    intentUpdated.UTC().Format(time.RFC3339Nano),
	}

	if intentState != "completed" {
		return c.add(observationInput{
			category:   recovery.VerificationV2,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("payment intent %s of request %s is state=%s (not completed); the external payment "+
				"outcome is unresolved and never assumed", intentID, requestID, intentState),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalCaps,
			required: externalRequirement(
				"the terminal payment outcome for the intent (or its chain evidence)",
				"operator reconciliation before any capability may be released",
			),
		})
	}

	// A completed intent is only consistent when its projection has converged.
	var (
		executionState string
		freshness      string
		projectionAt   time.Time
	)
	projectionErr := b.data.QueryRow(ctx,
		`SELECT execution_state, freshness, updated_at FROM request_status_projection WHERE request_id = $1`,
		requestID).Scan(&executionState, &freshness, &projectionAt)
	projectionFound := projectionErr == nil
	if projectionErr != nil && !errors.Is(projectionErr, pgx.ErrNoRows) {
		return fmt.Errorf("read request status projection for %s: %w", requestID, projectionErr)
	}
	if projectionFound {
		facts["request_status_projection"] = map[string]any{
			"execution_state": executionState,
			"freshness":       freshness,
			"updated_at":      projectionAt.UTC().Format(time.RFC3339Nano),
		}
	} else {
		facts["request_status_projection"] = "missing"
	}
	if !projectionFound || executionState != intentState || freshness != "confirmed" {
		return c.add(observationInput{
			category:   recovery.VerificationV2,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("payment intent %s is completed but its request_status_projection has not converged "+
				"(found=%t state=%s freshness=%s); the completed chain of record is not closed",
				intentID, projectionFound, executionState, freshness),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalCaps,
			required: externalRequirement(
				"a current projection row for the completed intent (local re-derivation)",
			),
		})
	}

	var (
		lastAction string
		auditAt    time.Time
	)
	auditErr := b.data.QueryRow(ctx,
		`SELECT action, recorded_at FROM withdrawal_request_audit
		  WHERE request_id = $1 ORDER BY audit_id DESC LIMIT 1`, requestID).Scan(&lastAction, &auditAt)
	if auditErr != nil && !errors.Is(auditErr, pgx.ErrNoRows) {
		return fmt.Errorf("read withdrawal request audit for %s: %w", requestID, auditErr)
	}
	if auditErr == nil {
		facts["latest_request_audit"] = map[string]any{
			"action": lastAction, "recorded_at": auditAt.UTC().Format(time.RFC3339Nano),
		}
	}
	var grantAudits int64
	if err := b.data.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_grant_audit WHERE authorization_id = $1`, authorizationID).
		Scan(&grantAudits); err != nil {
		return fmt.Errorf("count withdrawal grant audit for %s: %w", authorizationID, err)
	}
	facts["grant_audit_rows"] = grantAudits

	return c.add(observationInput{
		category:     recovery.VerificationV2,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

// ---------------------------------------------------------------------------
// V3: nonce allocation and occupancy (008 tables + chain transaction count)
// ---------------------------------------------------------------------------

type nonceFactsSource struct{ base *chainSourceBase }

// Category implements recovery.VerificationSource.
func (s *nonceFactsSource) Category() recovery.VerificationCategory { return recovery.VerificationV3 }

// Observe reads each nonce scope's durable frontier and compares it with the
// chain's transaction count when a QuantityCaller is configured. A scope whose
// chain view is unreadable or whose frontier does not account for the chain
// view is unknown; the adapter only observes and never allocates or reassigns.
func (s *nonceFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	b := s.base
	c := &observationCollector{base: b}

	rows, err := b.data.Query(ctx, `
		SELECT chain_id, sender FROM nonce_wallet_registry
		UNION SELECT chain_id, sender FROM nonce_bindings
		UNION SELECT chain_id, sender FROM nonce_scope_state
		UNION SELECT chain_id, sender FROM nonce_scope_holds
		UNION SELECT chain_id, sender FROM nonce_observations
		ORDER BY chain_id, sender`)
	if err != nil {
		return nil, fmt.Errorf("read nonce scopes: %w", err)
	}
	type nonceScope struct {
		chainID int64
		sender  string
	}
	var scopes []nonceScope
	for rows.Next() {
		var scope nonceScope
		if err := rows.Scan(&scope.chainID, &scope.sender); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan nonce scope: %w", err)
		}
		scopes = append(scopes, scope)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read nonce scopes: %w", err)
	}

	if len(scopes) == 0 {
		return c.out, c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  fmt.Sprintf("v3/nonce_scopes?chain_id=%d", b.chainID),
			conclusion: recovery.ConclusionUnknown,
			reason: "no nonce registry/binding/observation rows exist at the restore point; absence is not " +
				"evidence that no nonce was ever allocated or consumed (FR-016)",
			facts: map[string]any{
				"table": "nonce_wallet_registry", "found": false, "chain_id": b.chainID,
			},
			evidenceRefs: []string{
				fmt.Sprintf("data:nonce_wallet_registry?chain_id=%d", b.chainID),
				fmt.Sprintf("data:nonce_bindings?chain_id=%d", b.chainID),
			},
			directCaps: withdrawalRecoveryCaps,
			required: externalRequirement(
				"the nonce scope rows from a trusted copy of the data DB",
				"the chain transaction count for every sender scope in the restored window",
			),
		})
	}

	for _, scope := range scopes {
		if err := s.observeScope(ctx, c, scope.chainID, scope.sender); err != nil {
			return nil, err
		}
	}
	return c.out, nil
}

// nonceBindingFact is one read-only nonce binding row.
type nonceBindingFact struct {
	bindingID  string
	intentID   string
	nonce      string
	state      string
	consumedAt *time.Time
	releasedAt *time.Time
}

func (s *nonceFactsSource) observeScope(ctx context.Context, c *observationCollector, chainID int64, sender string) error {
	b := s.base
	objectKey := fmt.Sprintf("v3/nonce_scope?chain_id=%d&sender=%s", chainID, lowerHash(sender))
	refs := []string{
		fmt.Sprintf("data:nonce_wallet_registry?chain_id=%d&sender=%s", chainID, lowerHash(sender)),
		fmt.Sprintf("data:nonce_bindings?chain_id=%d&sender=%s", chainID, lowerHash(sender)),
	}
	facts := map[string]any{
		"chain_id": chainID,
		"sender":   lowerHash(sender),
	}

	// Registry state.
	var (
		registryState string
		registrySeq   int64
	)
	registryErr := b.data.QueryRow(ctx,
		`SELECT state, registry_seq FROM nonce_wallet_registry WHERE chain_id = $1 AND sender = $2`,
		chainID, sender).Scan(&registryState, &registrySeq)
	registryFound := registryErr == nil
	if registryErr != nil && !errors.Is(registryErr, pgx.ErrNoRows) {
		return fmt.Errorf("read nonce registry %d/%s: %w", chainID, sender, registryErr)
	}
	if registryFound {
		facts["registry"] = map[string]any{"state": registryState, "registry_seq": registrySeq}
	} else {
		facts["registry"] = "missing"
	}

	// Bindings.
	rows, err := b.data.Query(ctx,
		`SELECT binding_id, intent_id, nonce::text, state, consumed_at, released_at
		   FROM nonce_bindings WHERE chain_id = $1 AND sender = $2 ORDER BY nonce`, chainID, sender)
	if err != nil {
		return fmt.Errorf("read nonce bindings %d/%s: %w", chainID, sender, err)
	}
	var bindings []nonceBindingFact
	for rows.Next() {
		var binding nonceBindingFact
		if err := rows.Scan(&binding.bindingID, &binding.intentID, &binding.nonce, &binding.state,
			&binding.consumedAt, &binding.releasedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan nonce binding: %w", err)
		}
		bindings = append(bindings, binding)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read nonce bindings %d/%s: %w", chainID, sender, err)
	}
	bindingFacts := make([]map[string]any, 0, len(bindings))
	for _, binding := range bindings {
		entry := map[string]any{
			"binding_id": binding.bindingID,
			"intent_id":  binding.intentID,
			"nonce":      strings.TrimSpace(binding.nonce),
			"state":      binding.state,
		}
		if binding.consumedAt != nil {
			entry["consumed_at"] = binding.consumedAt.UTC().Format(time.RFC3339Nano)
		}
		if binding.releasedAt != nil {
			entry["released_at"] = binding.releasedAt.UTC().Format(time.RFC3339Nano)
		}
		bindingFacts = append(bindingFacts, entry)
	}
	facts["bindings"] = bindingFacts

	var maxBindingNonce *big.Int
	for _, binding := range bindings {
		nonce, ok := new(big.Int).SetString(strings.TrimSpace(binding.nonce), 10)
		if !ok {
			return fmt.Errorf("nonce binding %s carries non-decimal nonce %q", binding.bindingID, binding.nonce)
		}
		if maxBindingNonce == nil || nonce.Cmp(maxBindingNonce) > 0 {
			maxBindingNonce = nonce
		}
	}

	// Active holds.
	holdRows, err := b.data.Query(ctx,
		`SELECT hold_id, cause, established_at FROM nonce_scope_holds
		  WHERE chain_id = $1 AND sender = $2 AND status = 'active' ORDER BY established_at, hold_id`,
		chainID, sender)
	if err != nil {
		return fmt.Errorf("read nonce holds %d/%s: %w", chainID, sender, err)
	}
	var holdFacts []map[string]any
	for holdRows.Next() {
		var (
			holdID        string
			cause         string
			establishedAt time.Time
		)
		if err := holdRows.Scan(&holdID, &cause, &establishedAt); err != nil {
			holdRows.Close()
			return fmt.Errorf("scan nonce hold: %w", err)
		}
		holdFacts = append(holdFacts, map[string]any{
			"hold_id": holdID, "cause": cause, "established_at": establishedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	holdRows.Close()
	if err := holdRows.Err(); err != nil {
		return fmt.Errorf("read nonce holds %d/%s: %w", chainID, sender, err)
	}
	facts["active_holds"] = holdFacts

	// Durable frontier: reconciled floor and the last recorded observation.
	var (
		reconciledFloor string
		lastLatest      string
		lastPending     string
		lastObsID       string
	)
	scopeErr := b.data.QueryRow(ctx,
		`SELECT COALESCE(reconciled_floor::text, ''), COALESCE(last_latest::text, ''),
		        COALESCE(last_pending::text, ''), COALESCE(last_observation_id, '')
		   FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2`, chainID, sender).
		Scan(&reconciledFloor, &lastLatest, &lastPending, &lastObsID)
	scopeFound := scopeErr == nil
	if scopeErr != nil && !errors.Is(scopeErr, pgx.ErrNoRows) {
		return fmt.Errorf("read nonce scope state %d/%s: %w", chainID, sender, scopeErr)
	}
	if scopeFound {
		facts["scope_state"] = map[string]any{
			"reconciled_floor": reconciledFloor, "last_latest": lastLatest,
			"last_pending": lastPending, "last_observation_id": lastObsID,
		}
	} else {
		facts["scope_state"] = "missing"
	}

	var (
		lastClassification string
		lastObservedAt     time.Time
	)
	obsErr := b.data.QueryRow(ctx,
		`SELECT classification, observed_at FROM nonce_observations
		  WHERE chain_id = $1 AND sender = $2 ORDER BY observed_at DESC LIMIT 1`, chainID, sender).
		Scan(&lastClassification, &lastObservedAt)
	observationFound := obsErr == nil
	if obsErr != nil && !errors.Is(obsErr, pgx.ErrNoRows) {
		return fmt.Errorf("read nonce observation %d/%s: %w", chainID, sender, obsErr)
	}
	if observationFound {
		facts["last_observation"] = map[string]any{
			"classification": lastClassification,
			"observed_at":    lastObservedAt.UTC().Format(time.RFC3339Nano),
		}
	} else {
		facts["last_observation"] = "missing"
	}

	// A scope outside the verification target chain cannot be probed here.
	if chainID != int64(b.chainID) {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("nonce scope %d/%s belongs to chain %d, outside the verification target chain %d; "+
				"its chain occupancy is not observable", chainID, lowerHash(sender), chainID, b.chainID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required:     externalRequirement("a canonical chain view for the scope's own chain"),
		})
	}

	if len(holdFacts) > 0 {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("nonce scope %d/%s carries %d active hold(s); the allocation/occupancy state is "+
				"unresolved", chainID, lowerHash(sender), len(holdFacts)),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"the hold resolution evidence for every active hold",
			),
		})
	}

	// Chain transaction count (optional raw quantity surface).
	latest, latestErr := b.transactionCount(ctx, sender, "latest")
	pending, pendingErr := b.transactionCount(ctx, sender, "pending")
	countErr := latestErr
	if countErr == nil {
		countErr = pendingErr
	}
	if countErr == nil {
		facts["chain_transaction_count"] = map[string]any{
			"latest": latest.String(), "pending": pending.String(),
		}
	} else {
		facts["chain_transaction_count"] = "unreadable"
		facts["chain_transaction_count_error"] = logx.Redact(countErr.Error())
		if latestErr == nil {
			facts["chain_transaction_count_latest"] = latest.String()
		}
		if pendingErr == nil {
			facts["chain_transaction_count_pending"] = pending.String()
		}
		if probe := s.probeHighestBinding(ctx, bindings); probe != nil {
			facts["chain_binding_probe"] = probe
		}
	}
	return s.concludeScope(c, objectKey, facts, refs, chainID, sender, bindings, maxBindingNonce,
		reconciledFloor, lastClassification, observationFound, countErr, latest, pending)
}

// probeHighestBinding documents the highest binding's transaction status
// through the frozen read-only chain surface (a single TransactionByHash
// probe; it proves nothing by itself and never moves the binding).
func (s *nonceFactsSource) probeHighestBinding(ctx context.Context, bindings []nonceBindingFact) map[string]any {
	if len(bindings) == 0 {
		return nil
	}
	highest := bindings[len(bindings)-1]
	var (
		txHash string
	)
	err := s.base.data.QueryRow(ctx,
		`SELECT s.tx_hash FROM tx_attempts a
		   JOIN tx_attempt_signings s ON s.attempt_id = a.attempt_id
		  WHERE a.binding_ref = $1 ORDER BY a.created_at DESC LIMIT 1`, highest.bindingID).Scan(&txHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return map[string]any{"binding_id": highest.bindingID, "tx_hash": "missing"}
		}
		return map[string]any{"binding_id": highest.bindingID, "probe": "read_failed"}
	}
	_, isPending, err := s.base.rpc.TransactionByHash(ctx, common.HexToHash(txHash))
	switch {
	case err != nil && eth.KindOf(err) == eth.KindNotFound:
		return map[string]any{"binding_id": highest.bindingID, "tx_hash": lowerHash(txHash), "chain": "not_found_yet"}
	case err != nil:
		return map[string]any{"binding_id": highest.bindingID, "tx_hash": lowerHash(txHash),
			"chain": "unavailable", "error_class": string(eth.KindOf(err))}
	case isPending:
		return map[string]any{"binding_id": highest.bindingID, "tx_hash": lowerHash(txHash), "chain": "found_pending"}
	default:
		return map[string]any{"binding_id": highest.bindingID, "tx_hash": lowerHash(txHash), "chain": "found"}
	}
}

// concludeScope derives the scope verdict from the durable frontier and the
// chain count. Divergence is checked before every unknown; the durable state
// is never adjusted and no nonce is ever reallocated.
func (s *nonceFactsSource) concludeScope(
	c *observationCollector, objectKey string, facts map[string]any, refs []string,
	chainID int64, sender string, bindings []nonceBindingFact, maxBindingNonce *big.Int,
	reconciledFloor, lastClassification string, observationFound bool,
	countErr error, latest, pending *big.Int,
) error {
	// A recorded divergence is an explicit contradiction.
	if observationFound && lastClassification == "divergence" {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the last recorded chain-view observation of scope %d/%s classified divergence; the "+
				"durable frontier contradicts the observed chain view", chainID, lowerHash(sender)),
			facts:        facts,
			evidenceRefs: refs,
		})
	}
	if countErr != nil {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain transaction count of scope %d/%s is unreadable (%s); the allocation and "+
				"occupancy of the scope cannot be proven", chainID, lowerHash(sender), logx.Redact(countErr.Error())),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"the eth_getTransactionCount(latest/pending) readings for the sender scope",
				"operator reconciliation of any nonce the durable state does not account for",
			),
		})
	}

	facts["chain_transaction_count"] = map[string]any{"latest": latest.String(), "pending": pending.String()}
	latestInt := new(big.Int).Set(latest)

	// latest > pending is a contradictory chain view.
	if latest.Cmp(pending) > 0 {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the chain reports latest=%s > pending=%s for scope %d/%s; the chain view is "+
				"contradictory", latest, pending, chainID, lowerHash(sender)),
			facts:        facts,
			evidenceRefs: refs,
		})
	}

	// Previous pending regression (008 matrix): divergence.
	if scopeState, ok := facts["scope_state"].(map[string]any); ok {
		if raw, ok := scopeState["last_pending"].(string); ok && strings.TrimSpace(raw) != "" {
			if previous, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10); ok && pending.Cmp(previous) < 0 {
				return c.add(observationInput{
					category:   recovery.VerificationV3,
					objectKey:  objectKey,
					conclusion: recovery.ConclusionDivergent,
					reason: fmt.Sprintf("the chain pending count %s regressed below the scope's last observed pending "+
						"count %s for %d/%s", pending, previous, chainID, lowerHash(sender)),
					facts:        facts,
					evidenceRefs: refs,
				})
			}
		}
	}

	// A consumed binding must be below the chain's latest count.
	for _, binding := range bindings {
		if binding.state != "consumed" {
			continue
		}
		nonce, ok := new(big.Int).SetString(strings.TrimSpace(binding.nonce), 10)
		if !ok {
			continue
		}
		if nonce.Cmp(latestInt) >= 0 {
			return c.add(observationInput{
				category:   recovery.VerificationV3,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionDivergent,
				reason: fmt.Sprintf("binding %s records nonce %s as consumed while the chain latest count is %s; "+
					"the durable consumption contradicts the chain", binding.bindingID, nonce, latest),
				facts:        facts,
				evidenceRefs: refs,
			})
		}
	}

	// Durable next-nonce base: M+1 with bindings, else the reconciled floor,
	// else zero (the 008 matrix base; never max(M+1, P)).
	base := big.NewInt(0)
	if maxBindingNonce != nil {
		base = new(big.Int).Add(maxBindingNonce, big.NewInt(1))
	} else if strings.TrimSpace(reconciledFloor) != "" {
		if floor, ok := new(big.Int).SetString(strings.TrimSpace(reconciledFloor), 10); ok {
			base = floor
		}
	}

	if latest.Cmp(base) > 0 {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain latest count %s exceeds the durable frontier %s for scope %d/%s; "+
				"nonce(s) were consumed outside the durable allocations and cannot be attributed",
				latest, base, chainID, lowerHash(sender)),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"attribution evidence for every chain-consumed nonce beyond the durable frontier",
				"operator reconciliation before the scope can be used",
			),
		})
	}
	if pending.Cmp(base) > 0 {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain pending count %s exceeds the durable frontier %s for scope %d/%s; "+
				"pending transaction(s) are not attributed by the durable state",
				pending, base, chainID, lowerHash(sender)),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"attribution evidence for every pending nonce beyond the durable frontier",
			),
		})
	}

	if observationFound && lastClassification != "consistent" {
		return c.add(observationInput{
			category:   recovery.VerificationV3,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the last recorded chain-view observation of scope %d/%s classified %s; the scope's "+
				"occupancy history is not closed", chainID, lowerHash(sender), lastClassification),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"evidence resolving the last recorded nonce observation classification",
			),
		})
	}
	if registryState, ok := facts["registry"].(map[string]any); ok {
		if registryState["state"] == "disabled" && len(bindings) > 0 {
			return c.add(observationInput{
				category:   recovery.VerificationV3,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: fmt.Sprintf("the nonce registry of scope %d/%s is disabled while %d binding(s) exist; the "+
					"scope's terminal state is unresolved", chainID, lowerHash(sender), len(bindings)),
				facts:        facts,
				evidenceRefs: refs,
				directCaps:   withdrawalRecoveryCaps,
				required: externalRequirement(
					"the registry disable/binding release evidence for the scope",
				),
			})
		}
	}

	return c.add(observationInput{
		category:     recovery.VerificationV3,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

// transactionCount reads one eth_getTransactionCount quantity through the
// optional raw JSON-RPC surface. A nil caller is an unreadable count, never a
// fabricated zero.
func (b *chainSourceBase) transactionCount(ctx context.Context, sender, block string) (*big.Int, error) {
	if b.quantity == nil {
		return nil, errors.New("no raw JSON-RPC quantity surface is configured")
	}
	var raw string
	if err := b.quantity.CallContext(ctx, &raw, quantityMethod, lowerHash(sender), block); err != nil {
		return nil, fmt.Errorf("%s read failed: %w", quantityMethod, err)
	}
	value, err := parseHexQuantity(raw)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// parseHexQuantity parses one 0x-prefixed JSON-RPC quantity exactly (big.Int,
// never a float or a uint64 truncation).
func parseHexQuantity(raw string) (*big.Int, error) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "0x") {
		return nil, fmt.Errorf("quantity %q is not 0x-prefixed", raw)
	}
	value, ok := new(big.Int).SetString(trimmed[2:], 16)
	if !ok {
		return nil, fmt.Errorf("quantity %q is not hexadecimal", raw)
	}
	return value, nil
}

// ---------------------------------------------------------------------------
// V4: signing / broadcast results (including unknown)
// ---------------------------------------------------------------------------

type signingFactsSource struct{ base *chainSourceBase }

// Category implements recovery.VerificationSource.
func (s *signingFactsSource) Category() recovery.VerificationCategory { return recovery.VerificationV4 }

// Observe reads every tx attempt through the read-only txlifecycle accessor
// surface (AttemptByID / Status / UnknownRecovery), compares the persisted
// receipt with the canonical chain receipt, and reads signing requests that
// never became attempts plus open execution steps. Unknown outcomes stay
// unknown; no signed history is ever replayed or re-broadcast.
func (s *signingFactsSource) Observe(ctx context.Context) ([]recovery.SourceObservation, error) {
	b := s.base
	c := &observationCollector{base: b}

	attemptIDs, err := b.attemptIDs(ctx)
	if err != nil {
		return nil, err
	}
	for _, attemptID := range attemptIDs {
		if err := s.observeAttempt(ctx, c, attemptID); err != nil {
			return nil, err
		}
	}
	if err := s.observeOrphanSigningRequests(ctx, c); err != nil {
		return nil, err
	}
	if err := s.observeOpenExecutionSteps(ctx, c); err != nil {
		return nil, err
	}
	if len(c.out) == 0 {
		return c.out, c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  fmt.Sprintf("v4/signing_activity?chain_id=%d", b.chainID),
			conclusion: recovery.ConclusionUnknown,
			reason: "no tx attempt / signing request / open execution step rows exist at the restore point; absence " +
				"is not evidence that no signed or broadcast transaction ever existed (FR-016)",
			facts: map[string]any{
				"tables": []string{"tx_attempts", "signing_requests", "execution_steps"}, "found": false,
			},
			evidenceRefs: []string{
				"data:tx_attempts?scope=all",
				"data:signing_requests?scope=all",
				fmt.Sprintf("chain:eth_getTransactionReceipt?chain_id=%d", b.chainID),
			},
			directCaps: withdrawalRecoveryCaps,
			required: externalRequirement(
				"the signing/execution rows from a trusted copy of the data DB",
				"chain receipts for any transaction the restored window may have broadcast",
			),
		})
	}
	return c.out, nil
}

func (b *chainSourceBase) attemptIDs(ctx context.Context) ([]string, error) {
	rows, err := b.data.Query(ctx, `SELECT attempt_id FROM tx_attempts ORDER BY attempt_id`)
	if err != nil {
		return nil, fmt.Errorf("read tx attempts: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan tx attempt: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read tx attempts: %w", err)
	}
	return ids, nil
}

// localReceiptFact is the latest local tx_receipts row of one attempt.
type localReceiptFact struct {
	found       bool
	txHash      string
	status      int
	blockNumber int64
	blockHash   string
	effect      string
	canonical   string
}

func (b *chainSourceBase) localReceipt(ctx context.Context, attemptID string) (localReceiptFact, error) {
	var fact localReceiptFact
	err := b.data.QueryRow(ctx,
		`SELECT tx_hash, status, block_number, block_hash, effect, canonicality
		   FROM tx_receipts WHERE attempt_id = $1 ORDER BY receipt_id DESC LIMIT 1`, attemptID).
		Scan(&fact.txHash, &fact.status, &fact.blockNumber, &fact.blockHash, &fact.effect, &fact.canonical)
	if errors.Is(err, pgx.ErrNoRows) {
		return localReceiptFact{}, nil
	}
	if err != nil {
		return localReceiptFact{}, fmt.Errorf("read tx receipt of %s: %w", attemptID, err)
	}
	fact.found = true
	return fact, nil
}

// latestChainObservation is the newest append-only chain observation class of
// one attempt (a read-only lookup; no observation row is added or changed by
// the verification).
func (b *chainSourceBase) latestChainObservation(ctx context.Context, attemptID string) (string, error) {
	var class string
	err := b.data.QueryRow(ctx,
		`SELECT classification FROM tx_reconciliations
		  WHERE attempt_id = $1 ORDER BY reconcile_id DESC LIMIT 1`, attemptID).Scan(&class)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read tx reconciliations of %s: %w", attemptID, err)
	}
	return class, nil
}

func (s *signingFactsSource) observeAttempt(ctx context.Context, c *observationCollector, attemptID string) error {
	b := s.base
	attempt, err := b.txl.AttemptByID(ctx, attemptID)
	if err != nil {
		return fmt.Errorf("read attempt %s: %w", attemptID, err)
	}
	status, err := b.txl.Status(ctx, attemptID)
	if err != nil {
		return fmt.Errorf("read attempt status %s: %w", attemptID, err)
	}
	receipt, err := b.localReceipt(ctx, attemptID)
	if err != nil {
		return err
	}
	observationClass, err := b.latestChainObservation(ctx, attemptID)
	if err != nil {
		return err
	}

	var recoveryConditions []string
	if attempt.State == "unknown" || attempt.State == "sent" {
		recovery, rerr := b.txl.UnknownRecovery(ctx, attemptID, attempt.TxHash)
		if rerr != nil {
			return fmt.Errorf("read attempt recovery facts %s: %w", attemptID, rerr)
		}
		recoveryConditions = recovery.RecoveryConditions
	}

	objectKey := "v4/tx_attempt?" + attemptID
	refs := []string{
		"data:tx_attempts?attempt_id=" + attemptID,
		"data:tx_attempt_signings?attempt_id=" + attemptID,
		"data:tx_receipts?attempt_id=" + attemptID,
		"data:tx_reconciliations?attempt_id=" + attemptID,
		fmt.Sprintf("chain:eth_getTransactionReceipt?chain_id=%d", b.chainID),
	}
	facts := map[string]any{
		"table":                    "tx_attempts",
		"attempt_id":               attempt.AttemptID,
		"intent_id":                attempt.IntentID,
		"chain_id":                 attempt.ChainID,
		"binding_ref":              attempt.BindingRef,
		"nonce":                    attempt.Nonce,
		"authorization_id":         attempt.AuthorizationID,
		"state":                    attempt.State,
		"revision_seq":             attempt.RevisionSeq,
		"tx_hash":                  lowerHash(attempt.TxHash),
		"status_state":             status.State,
		"latest_send_outcome":      status.LatestDispatchOutcome,
		"latest_chain_observation": observationClass,
		"latest_receipt_effect":    status.LatestReceiptEffect,
	}
	if len(recoveryConditions) > 0 {
		facts["recovery_conditions"] = recoveryConditions
	}
	if receipt.found {
		facts["local_receipt"] = map[string]any{
			"tx_hash": lowerHash(receipt.txHash), "status": receipt.status,
			"block_number": receipt.blockNumber, "block_hash": lowerHash(receipt.blockHash),
			"effect": receipt.effect, "canonicality": receipt.canonical,
		}
	} else {
		facts["local_receipt"] = "missing"
	}

	if deliveryVerdict, found, derr := b.latestDeliveryVerdict(ctx, attemptID); derr != nil {
		return derr
	} else if found {
		facts["latest_delivery_verdict"] = deliveryVerdict
	}

	if attempt.ChainID != int64(b.chainID) {
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("attempt %s belongs to chain %d, outside the verification target chain %d; its chain "+
				"outcome is not observable here", attemptID, attempt.ChainID, b.chainID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required:     externalRequirement("a canonical chain view for the attempt's own chain"),
		})
	}

	if attempt.TxHash == "" {
		facts["chain_receipt"] = "unprobeable"
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("attempt %s carries no persisted signed tx hash at the restore point; the chain "+
				"cannot be probed and absence of signed evidence is not proof that nothing was broadcast (FR-016)",
				attemptID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"the signed tx hash / signing record from a trusted copy of the data DB",
				"chain receipts for any transaction the attempt may have broadcast",
			),
		})
	}

	chainReceipt, rerr := b.rpc.TransactionReceipt(ctx, common.HexToHash(attempt.TxHash))
	switch {
	case rerr != nil && eth.KindOf(rerr) == eth.KindNotFound:
		probe := "not_found_yet"
		if _, isPending, perr := b.rpc.TransactionByHash(ctx, common.HexToHash(attempt.TxHash)); perr == nil {
			if isPending {
				probe = "found_pending"
			}
		} else if eth.KindOf(perr) != eth.KindNotFound {
			probe = "unavailable"
		}
		facts["chain_receipt"] = probe
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("attempt %s has no chain receipt (%s); the broadcast outcome is unresolved and is "+
				"never treated as not executed", attemptID, probe),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"a chain receipt for the persisted tx hash",
				"operator reconciliation of the broadcast outcome",
			),
		})
	case rerr != nil:
		facts["chain_receipt"] = map[string]any{"error_class": string(eth.KindOf(rerr))}
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain receipt read for attempt %s is unavailable (%s); the broadcast outcome "+
				"cannot be compared", attemptID, string(eth.KindOf(rerr))),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required:     externalRequirement("a reachable canonical RPC endpoint"),
		})
	}
	facts["chain_receipt"] = map[string]any{
		"status":       chainReceipt.Status,
		"block_number": chainReceipt.BlockNumber.Int64(),
		"block_hash":   lowerHash(chainReceipt.BlockHash.Hex()),
	}

	// Canonicality of the served receipt: the chain header at its height must
	// still name that block.
	header, herr := b.rpc.HeaderByNumber(ctx, chainReceipt.BlockNumber)
	switch {
	case herr != nil:
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the canonical header for receipt block %d of attempt %s is unavailable (%s); the "+
				"chain receipt canonicality cannot be proven", chainReceipt.BlockNumber.Int64(), attemptID,
				string(eth.KindOf(herr))),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required:     externalRequirement("a reachable canonical RPC endpoint"),
		})
	case lowerHash(header.Hash().Hex()) != lowerHash(chainReceipt.BlockHash.Hex()):
		facts["canonical_header_hash"] = lowerHash(header.Hash().Hex())
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("the chain serves a receipt for attempt %s at block %s while the canonical header at "+
				"that height is %s; the served receipt is not on the canonical chain", attemptID,
				lowerHash(chainReceipt.BlockHash.Hex()), lowerHash(header.Hash().Hex())),
			facts:        facts,
			evidenceRefs: refs,
		})
	}

	if !receipt.found {
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("the chain carries an included receipt for attempt %s but the restore point has no "+
				"tx_receipts row; the external effect is not recorded locally and absence is not proof it did not "+
				"happen (FR-015/FR-016)", attemptID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"the tx_receipts row from a trusted copy of the data DB",
				"operator reconciliation of the externally included transaction",
			),
		})
	}
	if lowerHash(receipt.blockHash) != lowerHash(chainReceipt.BlockHash.Hex()) {
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("attempt %s records receipt block hash %s while the chain serves %s; the local and "+
				"chain receipt identities differ", attemptID, lowerHash(receipt.blockHash),
				lowerHash(chainReceipt.BlockHash.Hex())),
			facts:        facts,
			evidenceRefs: refs,
		})
	}
	switch receipt.canonical {
	case "orphaned":
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("attempt %s records its receipt as orphaned while the chain serves it as a canonical "+
				"receipt", attemptID),
			facts:        facts,
			evidenceRefs: refs,
		})
	case "unverified":
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("attempt %s records its receipt with canonicality=unverified; the receipt's canonical "+
				"standing cannot be proven from the stored verdict", attemptID),
			facts:        facts,
			evidenceRefs: refs,
			directCaps:   withdrawalRecoveryCaps,
			required: externalRequirement(
				"the canonicality decision for the receipt (chain header comparison)",
			),
		})
	}
	if chainReceipt.Status == 0 && receipt.effect != "ineffective_status" {
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("attempt %s records effect=%s while the chain receipt status is 0; the recorded "+
				"payment verdict contradicts the chain", attemptID, receipt.effect),
			facts:        facts,
			evidenceRefs: refs,
		})
	}
	if chainReceipt.Status == 1 && receipt.effect == "ineffective_status" {
		return c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  objectKey,
			conclusion: recovery.ConclusionDivergent,
			reason: fmt.Sprintf("attempt %s records effect=ineffective_status while the chain receipt status is 1; "+
				"the recorded payment verdict contradicts the chain", attemptID),
			facts:        facts,
			evidenceRefs: refs,
		})
	}

	return c.add(observationInput{
		category:     recovery.VerificationV4,
		objectKey:    objectKey,
		conclusion:   recovery.ConclusionConsistent,
		facts:        facts,
		evidenceRefs: refs,
	})
}

// latestDeliveryVerdict reads the newest delivery admission verdict of one
// attempt. Absence is not an error: it is simply no admission record.
func (b *chainSourceBase) latestDeliveryVerdict(ctx context.Context, attemptID string) (string, bool, error) {
	var verdict string
	err := b.data.QueryRow(ctx,
		`SELECT da.verdict FROM delivery_admissions da
		   JOIN signing_requests sr ON sr.id = da.signing_request_row
		  WHERE sr.attempt_id = $1 ORDER BY da.admission_id DESC LIMIT 1`, attemptID).Scan(&verdict)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read delivery admission of %s: %w", attemptID, err)
	}
	return verdict, true, nil
}

// observeOrphanSigningRequests covers signing requests that never became a
// tx attempt: a persisted signature is an external fact the restore point must
// account for, and missing evidence stays unknown.
func (s *signingFactsSource) observeOrphanSigningRequests(ctx context.Context, c *observationCollector) error {
	b := s.base
	rows, err := b.data.Query(ctx, `
		SELECT s.signing_request_id, s.state, s.refusal_class
		  FROM signing_requests s
		  LEFT JOIN tx_attempts a ON a.attempt_id = s.attempt_id
		 WHERE a.attempt_id IS NULL
		 ORDER BY s.signing_request_id`)
	if err != nil {
		return fmt.Errorf("read orphan signing requests: %w", err)
	}
	type signingRow struct {
		signingRequestID string
		state            string
		refusalClass     string
	}
	var signingRows []signingRow
	for rows.Next() {
		var row signingRow
		if err := rows.Scan(&row.signingRequestID, &row.state, &row.refusalClass); err != nil {
			rows.Close()
			return fmt.Errorf("scan signing request: %w", err)
		}
		signingRows = append(signingRows, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read orphan signing requests: %w", err)
	}

	for _, row := range signingRows {
		objectKey := "v4/signing_request?" + row.signingRequestID
		refs := []string{"data:signing_requests?signing_request_id=" + row.signingRequestID}
		facts := map[string]any{
			"table":              "signing_requests",
			"signing_request_id": row.signingRequestID,
			"state":              row.state,
			"refusal_class":      row.refusalClass,
		}
		var txHash string
		sigErr := b.data.QueryRow(ctx,
			`SELECT sr.tx_hash FROM signature_results sr
			   JOIN signing_requests s ON s.id = sr.signing_request_row
			  WHERE s.signing_request_id = $1`, row.signingRequestID).Scan(&txHash)
		sigFound := sigErr == nil
		if sigErr != nil && !errors.Is(sigErr, pgx.ErrNoRows) {
			return fmt.Errorf("read signature result of %s: %w", row.signingRequestID, sigErr)
		}
		if !sigFound {
			facts["signature_result"] = "missing"
			if err := c.add(observationInput{
				category:   recovery.VerificationV4,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: fmt.Sprintf("signing request %s has no tx attempt and no signature_results row at the "+
					"restore point; whether the signing boundary produced a signature cannot be proven from "+
					"absence (FR-016)", row.signingRequestID),
				facts:        facts,
				evidenceRefs: refs,
				directCaps:   withdrawalRecoveryCaps,
				required: externalRequirement(
					"the signature result row from a trusted copy of the data DB",
					"the signer boundary's own audit for the request",
				),
			}); err != nil {
				return err
			}
			continue
		}
		facts["signature_result"] = map[string]any{"tx_hash": lowerHash(txHash)}
		_, rerr := b.rpc.TransactionReceipt(ctx, common.HexToHash(txHash))
		switch {
		case rerr == nil:
			facts["chain_receipt"] = "included"
			if err := c.add(observationInput{
				category:   recovery.VerificationV4,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionDivergent,
				reason: fmt.Sprintf("signing request %s produced a signature whose transaction is included on chain, "+
					"but the restore point has no tx_attempts row for it; the external effect exists locally only "+
					"as a signature", row.signingRequestID),
				facts:        facts,
				evidenceRefs: append(refs, "chain:eth_getTransactionReceipt"),
			}); err != nil {
				return err
			}
		default:
			class := string(eth.KindOf(rerr))
			facts["chain_receipt"] = map[string]any{"error_class": class}
			if err := c.add(observationInput{
				category:   recovery.VerificationV4,
				objectKey:  objectKey,
				conclusion: recovery.ConclusionUnknown,
				reason: fmt.Sprintf("signing request %s produced persisted signed bytes with no tx attempt; its "+
					"chain outcome is unproven (%s) and the signed history is never re-broadcast",
					row.signingRequestID, class),
				facts:        facts,
				evidenceRefs: append(refs, "chain:eth_getTransactionReceipt"),
				directCaps:   withdrawalRecoveryCaps,
				required: externalRequirement(
					"a chain receipt for the persisted signed tx hash",
					"operator reconciliation of the broadcast outcome",
				),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// observeOpenExecutionSteps covers open ('issued'/'unknown') execution steps:
// their outcome is unresolved and must stay unknown.
func (s *signingFactsSource) observeOpenExecutionSteps(ctx context.Context, c *observationCollector) error {
	b := s.base
	rows, err := b.data.Query(ctx,
		`SELECT step_id, intent_id, action, state, COALESCE(attempt_id, ''), COALESCE(outcome_class, '')
		   FROM execution_steps WHERE state IN ('issued', 'unknown') ORDER BY step_id`)
	if err != nil {
		return fmt.Errorf("read open execution steps: %w", err)
	}
	type stepRow struct {
		stepID       string
		intentID     string
		action       string
		state        string
		attemptID    string
		outcomeClass string
	}
	var steps []stepRow
	for rows.Next() {
		var row stepRow
		if err := rows.Scan(&row.stepID, &row.intentID, &row.action, &row.state, &row.attemptID,
			&row.outcomeClass); err != nil {
			rows.Close()
			return fmt.Errorf("scan execution step: %w", err)
		}
		steps = append(steps, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read open execution steps: %w", err)
	}

	for _, row := range steps {
		facts := map[string]any{
			"table":         "execution_steps",
			"step_id":       row.stepID,
			"intent_id":     row.intentID,
			"action":        row.action,
			"state":         row.state,
			"attempt_id":    row.attemptID,
			"outcome_class": row.outcomeClass,
		}
		var (
			claimOwner  string
			leaseSerial int64
			expiresAt   time.Time
		)
		claimErr := b.data.QueryRow(ctx,
			`SELECT owner_id, lease_version, expires_at FROM execution_claims
			  WHERE intent_id = $1 AND state = 'active'`, row.intentID).
			Scan(&claimOwner, &leaseSerial, &expiresAt)
		if claimErr == nil {
			facts["active_claim"] = map[string]any{
				"owner_id": claimOwner, "lease_version": leaseSerial,
				"expires_at": expiresAt.UTC().Format(time.RFC3339Nano),
			}
		} else if !errors.Is(claimErr, pgx.ErrNoRows) {
			return fmt.Errorf("read active execution claim of %s: %w", row.intentID, claimErr)
		}
		if err := c.add(observationInput{
			category:   recovery.VerificationV4,
			objectKey:  "v4/execution_step?" + row.stepID,
			conclusion: recovery.ConclusionUnknown,
			reason: fmt.Sprintf("execution step %s (intent %s action %s) is state=%s; the execution/broadcast "+
				"outcome is unresolved", row.stepID, row.intentID, row.action, row.state),
			facts: facts,
			evidenceRefs: []string{
				"data:execution_steps?step_id=" + row.stepID,
				"data:execution_claims?intent_id=" + row.intentID,
			},
			directCaps: withdrawalRecoveryCaps,
			required: externalRequirement(
				"the converged outcome of the execution step (chain evidence or reconciliation)",
			),
		}); err != nil {
			return err
		}
	}
	return nil
}

// The four adapters implement the single read-only verification surface.
var (
	_ recovery.VerificationSource = (*chainFactsSource)(nil)
	_ recovery.VerificationSource = (*withdrawalFactsSource)(nil)
	_ recovery.VerificationSource = (*nonceFactsSource)(nil)
	_ recovery.VerificationSource = (*signingFactsSource)(nil)
)
