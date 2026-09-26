// attribution.go implements the T033/T034 chain-first 方向/资产归属过滤 for the
// reconcile-admin enumeration: a chain fact without a PostgreSQL business row
// becomes a `missing` candidate only when it can be attributed to the project
// through the existing deposit/withdrawal business configuration. Everything
// here is read-only.
//
// Trusted attribution sources (existing configuration, never a parallel rule):
//
//   - asset allowlist: the newest 004 deposit_config_history.assets snapshot —
//     the same source the withdrawal business resolves FR-05 against
//     (internal/withdrawal `ResolveAssetAllowlist` reads the same table);
//   - deposit receive addresses: the newest deposit_config_history.watches
//     snapshot (the 004 configured watch-address set);
//   - withdrawal send addresses: the deployment's configured signer senders
//     (TXHARBOR_SIGNER_SENDERS, the 009 withdrawal sender allowlist).
//
// Conservative outcomes:
//
//   - attributed (the member log's direction matches the configured project
//     address set AND its contract is in the asset allowlist) → the chain fact
//     becomes a `missing` candidate;
//   - confirmed not attributable (all needed sources are known and the fact
//     matches no enabled direction) → metrics-only, never a ticket;
//   - attribution undecidable (a needed source is missing/blank/unreadable) →
//     no chain-first candidate is emitted and the interval records an explicit
//     `query_failed` gap: the discovery set is NOT claimed complete and an
//     unattributed chain fact is never read as project missing.
//
// Only 014-owned candidate state and the read-only business sources above are
// touched: no business table is written, no recovery/replay/payment path is
// invoked (FR-014/015/023).
package reconcileadmin

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/xtianxx/txharbor/internal/indexer"
	"github.com/xtianxx/txharbor/internal/reconciliation"
)

// reconcileAttributionSQL reads the newest 004 policy snapshot of one chain.
const reconcileAttributionSQL = `
SELECT assets, watches
FROM deposit_config_history
WHERE chain_id = $1
ORDER BY version_seq DESC
LIMIT 1`

// reconcileAttribution is the resolved attribution source set of one scan
// invocation. `Known` reports whether the corresponding source was readable and
// non-empty; an unknown source is never read as an empty (negative) set.
type reconcileAttribution struct {
	assets      map[string]struct{}
	deposits    map[string]struct{}
	withdrawals map[string]struct{}

	assetsKnown      bool
	depositsKnown    bool
	withdrawalsKnown bool
}

// reconcileChainLogKey is the on-chain log identity used to deduplicate
// metrics-only counting across the direction passes.
type reconcileChainLogKey struct {
	blockNumber int64
	blockHash   string
	txHash      string
	logIndex    int64
}

// reconcileChainFirstPass is one direction pass's result: the candidates, the
// log identities the pass decided (attributed or confirmed not attributable),
// and whether the pass could not decide because its attribution source is
// unknown.
type reconcileChainFirstPass struct {
	candidates   []reconciliation.ScanCandidate
	attributed   map[reconcileChainLogKey]struct{}
	unattributed map[reconcileChainLogKey]struct{}
	gap          bool
}

// newReconcileChainFirstPass builds an empty pass result.
func newReconcileChainFirstPass() reconcileChainFirstPass {
	return reconcileChainFirstPass{
		attributed:   make(map[reconcileChainLogKey]struct{}),
		unattributed: make(map[reconcileChainLogKey]struct{}),
	}
}

// reconcileChainLogKeyOf names one chain log fact.
func reconcileChainLogKeyOf(log reconciliation.ChainFactLog) reconcileChainLogKey {
	return reconcileChainLogKey{
		blockNumber: log.BlockNumber,
		blockHash:   strings.ToLower(strings.TrimSpace(log.BlockHash)),
		txHash:      strings.ToLower(strings.TrimSpace(log.TxHash)),
		logIndex:    log.LogIndex,
	}
}

// loadAttribution resolves the attribution sources once per invocation and
// caches them. The 004 policy read is charged to the scan budget as it
// executes (T038); the signer sender set comes from controlled deployment
// configuration (no query). A missing policy row leaves the table sources
// unknown; malformed snapshots are refused so no partial set silently widens
// or narrows attribution.
func (s *reconcileScanCandidates) loadAttribution(ctx context.Context, chainID int64, budget reconciliation.ScanQueryBudget) error {
	if s.attribution != nil {
		return nil
	}
	attr := &reconcileAttribution{}
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return err
	}
	var assets, watches string
	err := s.pool.QueryRow(ctx, reconcileAttributionSQL, chainID).Scan(&assets, &watches)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No 004 policy history: both table sources stay unknown.
	case err != nil:
		return fmt.Errorf("read attribution config for chain %d: %w", chainID, err)
	default:
		assetSet, assetKnown, assetErr := parseReconcileAddressSnapshot(assets)
		if assetErr != nil {
			return fmt.Errorf("attribution asset allowlist: %w", assetErr)
		}
		depositSet, depositKnown, depositErr := parseReconcileAddressSnapshot(watches)
		if depositErr != nil {
			return fmt.Errorf("attribution deposit addresses: %w", depositErr)
		}
		attr.assets, attr.assetsKnown = assetSet, assetKnown
		attr.deposits, attr.depositsKnown = depositSet, depositKnown
	}
	if senders, err := canonicalReconcileAddresses(s.signerSenders); err != nil {
		return fmt.Errorf("attribution withdrawal senders: %w", err)
	} else if len(senders) > 0 {
		attr.withdrawals, attr.withdrawalsKnown = senders, true
	}
	s.attribution = attr
	return nil
}

// parseReconcileAddressSnapshot parses the 004 `<address>:<effective>` snapshot
// format (config.DepositSnapshot) into a canonical lowercase address set. A
// malformed line is refused; a snapshot without any address is reported as
// unknown (known=false), never as a confirmed empty project set — an empty
// allowlist must not silently turn chain facts into metrics-only.
func parseReconcileAddressSnapshot(snapshot string) (map[string]struct{}, bool, error) {
	out := make(map[string]struct{})
	for _, line := range strings.Split(snapshot, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		address, _, ok := strings.Cut(trimmed, ":")
		if !ok || !common.IsHexAddress(address) {
			return nil, false, fmt.Errorf("entry %q is not an address:effective line", trimmed)
		}
		out[strings.ToLower(address)] = struct{}{}
	}
	return out, len(out) > 0, nil
}

// canonicalReconcileAddresses validates and canonicalizes a configured address
// list (lowercase, deduplicated, sorted for determinism). An empty list is an
// empty set; a malformed entry is refused, never ignored.
func canonicalReconcileAddresses(raw []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(raw))
	ordered := append([]string(nil), raw...)
	sort.Strings(ordered)
	for _, entry := range ordered {
		trimmed := strings.TrimSpace(entry)
		if trimmed == "" {
			continue
		}
		if !common.IsHexAddress(trimmed) {
			return nil, fmt.Errorf("configured address %q is not a 0x address", entry)
		}
		out[strings.ToLower(trimmed)] = struct{}{}
	}
	return out, nil
}

// observeChainFirst performs the bounded read-only chain-facts observation the
// chain-first passes consume. The adapter call is charged its proven internal
// statement/RPC cap before execution (T038: the cap is the charge basis; the
// adapter itself runs no unbounded read).
func (s *reconcileScanCandidates) observeChainFirst(ctx context.Context, chainID, from, to int64,
	budget reconciliation.ScanQueryBudget) (reconciliation.ChainFactsBundle, error) {
	cap := reconciliation.QueryStatementCap{
		PG:  reconciliation.ChainFactsObservePGStatementCap,
		RPC: reconciliation.ChainFactsObserveRPCCallCap,
	}
	if err := cap.Charge(ctx, budget); err != nil {
		return reconciliation.ChainFactsBundle{}, err
	}
	bundle, err := s.chain.Observe(ctx, reconciliation.ChainFactsQuery{
		ChainID:          chainID,
		From:             from,
		To:               to,
		NeedTransferLogs: true,
	})
	if err != nil {
		return bundle, fmt.Errorf("chain-first enumeration %d..%d: %w", from, to, err)
	}
	if bundle.Orphaned {
		return bundle, fmt.Errorf("chain-first enumeration %d..%d: block evidence is orphaned; refusing to derive permanent missing candidates", from, to)
	}
	if !bundle.Coverage.LogRangeCovered {
		return bundle, fmt.Errorf("chain-first enumeration %d..%d: the durable log stream does not cover the interval; refusing to claim a complete discovery set", from, to)
	}
	return bundle, nil
}

// reconcileTxMembers groups the attributed member logs of one transaction.
type reconcileTxMembers struct {
	txHash  string
	members []reconciliation.TxAggregateMember
}

// withdrawalChainFirstPass attributes transfer logs to project withdrawals: a
// member log counts when the project sender set contains its `from` address and
// its contract is in the asset allowlist. The reverse PG check then decides
// which transactions already have an authoritative withdrawal execution row.
func (s *reconcileScanCandidates) withdrawalChainFirstPass(ctx context.Context, chainID int64,
	logs []reconciliation.ChainFactLog, budget reconciliation.ScanQueryBudget) (reconcileChainFirstPass, error) {
	pass := newReconcileChainFirstPass()
	attr := s.attribution
	if attr == nil || !attr.assetsKnown || !attr.withdrawalsKnown {
		// Undecidable attribution: the pass can neither claim missing nor
		// claim completeness over the interval.
		pass.gap = len(logs) > 0
		return pass, nil
	}

	byTx := make(map[string]*reconcileTxMembers)
	txOrder := make([]string, 0)
	for i := range logs {
		log := logs[i]
		key := reconcileChainLogKeyOf(log)
		if _, _, ok := reconcileMemberDirection(log, attr.assets, attr.withdrawals, true); !ok {
			pass.unattributed[key] = struct{}{}
			continue
		}
		pass.attributed[key] = struct{}{}
		txHash := key.txHash
		if txHash == "" {
			continue
		}
		entry, ok := byTx[txHash]
		if !ok {
			entry = &reconcileTxMembers{txHash: txHash}
			byTx[txHash] = entry
			txOrder = append(txOrder, txHash)
		}
		entry.members = append(entry.members, reconciliation.TxAggregateMember{
			BlockNumber: log.BlockNumber,
			BlockHash:   log.BlockHash,
			TxHash:      log.TxHash,
			LogIndex:    log.LogIndex,
			Contract:    log.Contract,
			Topic0:      log.Topic0,
		})
	}
	if len(txOrder) == 0 {
		return pass, nil
	}

	covered, err := s.pgTxHashes(ctx, chainID, txOrder, budget)
	if err != nil {
		return pass, err
	}
	for _, txHash := range txOrder {
		if _, hasPG := covered[txHash]; hasPG {
			// The transaction already has an authoritative PG execution row;
			// the PG-anchored pass owns it.
			continue
		}
		entry := byTx[txHash]
		members, err := reconciliation.CanonicalizeTxAggregateMembers(entry.members)
		if err != nil {
			return pass, err
		}
		if len(members) == 0 {
			continue
		}
		first := members[0]
		pass.candidates = append(pass.candidates, reconciliation.ScanCandidate{
			BusinessType: reconciliation.BusinessWithdrawal,
			BusinessKey: reconciliation.BusinessKey{
				Kind:  reconciliation.BusinessKeyTxHash,
				Value: first.TxHash,
			},
			ChainFact: reconciliation.ChainFactRef{
				BlockNumber: first.BlockNumber,
				BlockHash:   first.BlockHash,
				TxHash:      first.TxHash,
			},
			Members:     members,
			EvidenceRef: fmt.Sprintf("scan:candidates:chain-withdrawal tx_hash=%s block=%d members=%d", first.TxHash, first.BlockNumber, len(members)),
		})
	}
	return pass, nil
}

// depositChainFirstPass attributes transfer logs to project deposits: a member
// log counts when the project deposit watch set contains its `to` address and
// its contract is in the asset allowlist. A member without a stored 004
// deposit-credit row is a missing credit; one candidate per transaction
// aggregates its missing member logs (T035).
func (s *reconcileScanCandidates) depositChainFirstPass(ctx context.Context, chainID int64,
	logs []reconciliation.ChainFactLog, budget reconciliation.ScanQueryBudget) (reconcileChainFirstPass, error) {
	pass := newReconcileChainFirstPass()
	attr := s.attribution
	if attr == nil || !attr.assetsKnown || !attr.depositsKnown {
		pass.gap = len(logs) > 0
		return pass, nil
	}

	byTx := make(map[string]*reconcileTxMembers)
	txOrder := make([]string, 0)
	for i := range logs {
		log := logs[i]
		key := reconcileChainLogKeyOf(log)
		if _, _, ok := reconcileMemberDirection(log, attr.assets, attr.deposits, false); !ok {
			pass.unattributed[key] = struct{}{}
			continue
		}
		pass.attributed[key] = struct{}{}
		txHash := key.txHash
		if txHash == "" {
			continue
		}
		entry, ok := byTx[txHash]
		if !ok {
			entry = &reconcileTxMembers{txHash: txHash}
			byTx[txHash] = entry
			txOrder = append(txOrder, txHash)
		}
		entry.members = append(entry.members, reconciliation.TxAggregateMember{
			BlockNumber: log.BlockNumber,
			BlockHash:   log.BlockHash,
			TxHash:      log.TxHash,
			LogIndex:    log.LogIndex,
			Contract:    log.Contract,
			Topic0:      log.Topic0,
		})
	}
	if len(txOrder) == 0 {
		return pass, nil
	}

	recorded, err := s.pgDepositLogIndexes(ctx, chainID, txOrder, budget)
	if err != nil {
		return pass, err
	}
	for _, txHash := range txOrder {
		entry := byTx[txHash]
		var missing []reconciliation.TxAggregateMember
		for _, member := range entry.members {
			if _, ok := recorded[reconcileDepositRowKey{txHash: strings.ToLower(member.TxHash), logIndex: member.LogIndex}]; ok {
				continue
			}
			missing = append(missing, member)
		}
		missing, err := reconciliation.CanonicalizeTxAggregateMembers(missing)
		if err != nil {
			return pass, err
		}
		if len(missing) == 0 {
			continue
		}
		first := missing[0]
		pass.candidates = append(pass.candidates, reconciliation.ScanCandidate{
			BusinessType: reconciliation.BusinessDeposit,
			BusinessKey: reconciliation.BusinessKey{
				Kind:  reconciliation.BusinessKeyTxHash,
				Value: first.TxHash,
			},
			ChainFact: reconciliation.ChainFactRef{
				BlockNumber: first.BlockNumber,
				BlockHash:   first.BlockHash,
				TxHash:      first.TxHash,
				LogIndex:    &first.LogIndex,
			},
			Members:     missing,
			EvidenceRef: fmt.Sprintf("scan:candidates:chain-deposit tx_hash=%s block=%d missing_logs=%d", first.TxHash, first.BlockNumber, len(missing)),
		})
	}
	return pass, nil
}

// reconcileMemberDirection checks one log against the project address set of
// one direction: outgoing (`from` for withdrawal) or incoming (`to` for
// deposit), plus the shared asset allowlist. It returns the observed addresses
// and whether the log is attributed.
func reconcileMemberDirection(log reconciliation.ChainFactLog, assets, projectAddresses map[string]struct{}, outgoing bool) (from, to string, attributed bool) {
	from = strings.ToLower(strings.TrimSpace(log.From))
	to = strings.ToLower(strings.TrimSpace(log.To))
	if _, ok := assets[strings.ToLower(strings.TrimSpace(log.Contract))]; !ok {
		return from, to, false
	}
	address := to
	if outgoing {
		address = from
	}
	if address == "" {
		return from, to, false
	}
	_, attributed = projectAddresses[address]
	return from, to, attributed
}

// reconcileDepositRowKey is one deposit observation identity: the 004 PK is
// (chain_id, block_hash, tx_hash, log_index); the log identity is tx+log index.
type reconcileDepositRowKey struct {
	txHash   string
	logIndex int64
}

// pgDepositLogIndexes returns the (tx_hash, log_index) deposit rows already
// recorded for the attributed member logs, through the indexer-owned read-only
// deposit-credit API (the deposit storage stays confined to internal/indexer).
// One charged query; the caller bounds the hash list by its candidate limit.
func (s *reconcileScanCandidates) pgDepositLogIndexes(ctx context.Context, chainID int64,
	txOrder []string, budget reconciliation.ScanQueryBudget) (map[reconcileDepositRowKey]struct{}, error) {
	hashes := make([]string, 0, len(txOrder))
	for _, txHash := range txOrder {
		hashes = append(hashes, txHash)
	}
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return nil, err
	}
	stored, err := indexer.ReadDepositStoredObservations(ctx, s.pool, chainID, hashes)
	if err != nil {
		return nil, fmt.Errorf("deposit presence lookup: %w", err)
	}
	out := make(map[reconcileDepositRowKey]struct{}, len(stored))
	for i := range stored {
		out[reconcileDepositRowKey{
			txHash:   strings.ToLower(strings.TrimSpace(stored[i].TxHash)),
			logIndex: stored[i].LogIndex,
		}] = struct{}{}
	}
	return out, nil
}

// pgTxHashes returns the subset of the given tx hashes that already has an
// authoritative withdrawal execution row on the scoped chain. Read-only, one
// charged query.
func (s *reconcileScanCandidates) pgTxHashes(ctx context.Context, chainID int64,
	txHashes []string, budget reconciliation.ScanQueryBudget) (map[string]struct{}, error) {
	if err := budget.ConsumePG(ctx, 1); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, reconcilePGTxHashLookupSQL, chainID, txHashes)
	if err != nil {
		return nil, fmt.Errorf("chain-first PG reverse lookup: %w", err)
	}
	defer rows.Close()
	out := make(map[string]struct{}, len(txHashes))
	for rows.Next() {
		var txHash string
		if err := rows.Scan(&txHash); err != nil {
			return nil, fmt.Errorf("chain-first PG reverse lookup: %w", err)
		}
		out[strings.ToLower(strings.TrimSpace(txHash))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chain-first PG reverse lookup: %w", err)
	}
	return out, nil
}

// mergeChainFirstPasses folds the direction passes into the enumeration: the
// candidates are appended, an undecidable pass contributes an explicit
// query_failed gap, and metrics-only counts every decided-but-unattributed log
// exactly once (a log attributed by any direction is never metrics-only).
func mergeChainFirstPasses(enum *reconciliation.ScanEnumeration, passes ...reconcileChainFirstPass) {
	attributed := make(map[reconcileChainLogKey]struct{})
	unattributed := make(map[reconcileChainLogKey]struct{})
	gap := false
	for i := range passes {
		pass := &passes[i]
		enum.Candidates = append(enum.Candidates, pass.candidates...)
		for key := range pass.attributed {
			attributed[key] = struct{}{}
		}
		for key := range pass.unattributed {
			unattributed[key] = struct{}{}
		}
		gap = gap || pass.gap
	}
	if gap {
		enum.GapReasons = append(enum.GapReasons, reconciliation.GapQueryFailed)
	}
	for key := range unattributed {
		if _, ok := attributed[key]; ok {
			continue
		}
		enum.MetricsOnly++
	}
}
