package withdrawal

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxUint256Dec is 2²⁵⁶−1, the largest value a NUMERIC(78,0) uint256 column can
// hold (data-model.md Table 3 amount CHECK). Money is integer-only: no float
// representation exists anywhere on this path (constitution §I).
const maxUint256Dec = "115792089237316195423570985008687907853269984665640564039457584007913129639935"

// maxUint256 is the same bound derived independently by arithmetic (1<<256)-1,
// so the literal constant and the computed bound cannot silently drift.
var maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

var (
	// amountShape is FR-06 transport form: decimal, first digit non-zero, no
	// sign/decimal-point/exponent/whitespace/empty. The shape regex is the
	// decimal barrier — "1.5"/"1e3" die at parse, never as rounded values.
	amountShape = regexp.MustCompile(`^[1-9][0-9]*$`)
	// addressShape is FR-07: 0x prefix + exactly 40 hex characters.
	addressShape = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
)

// ValidateChainID enforces FR-04: the request chain_id must be positive and
// equal to the chain this deployment is bound to. The deployment bind value
// itself (config.ChainID) is owned by the wiring, not here — this compares only.
func ValidateChainID(chainID int64, expected int64) error {
	if chainID <= 0 || chainID != expected {
		return New(CodeValidationFailed,
			fmt.Sprintf("chain_id %d does not match this deployment's chain", chainID)).
			WithField("chain_id")
	}
	return nil
}

// ValidateAssetWhitelisted enforces FR-05: a canonical (lowercase) asset must be
// a member of the caller-supplied allowlist. The allowlist source is owned by
// config (NormalizeWhitelist); this function only performs membership. An empty
// allowlist always rejects — it never degrades into a full-chain fallback.
func ValidateAssetWhitelisted(asset string, allowlist []string) error {
	for _, a := range allowlist {
		if a == asset {
			return nil
		}
	}
	return New(CodeValidationFailed, "asset is not in the configured whitelist").
		WithField("asset")
}

// resolveAssetAllowlistSQL reads the newest deposit policy version for a chain.
// deposit_config_history is the append-only 004 version ledger the deposit
// indexer authorizes against; its `assets` column is the canonical
// `<address>:<effective>` snapshot (004 data-model Table 4, research R11). The
// latest version by version_seq is the currently effective policy.
const resolveAssetAllowlistSQL = `
SELECT assets FROM deposit_config_history
WHERE chain_id = $1 ORDER BY version_seq DESC LIMIT 1`

// ResolveAssetAllowlist reads the live 003/004 policy source for FR-05: the
// asset set of the newest deposit_config_history row for chainID. It is T020's
// source replacement for T014's interim cfg.DepositContracts projection —
// callers thread the result into ValidateAssetWhitelisted (typically once per
// create attempt), so the asset list is never redefined here. The returned list
// is canonical (lowercase, sorted, deduplicated), mirroring
// config.NormalizeWhitelist. A missing or blank policy row is an error: the
// whitelist never degrades into an empty or full-chain fallback.
func ResolveAssetAllowlist(ctx context.Context, pool *pgxpool.Pool, chainID int64) ([]string, error) {
	var snapshot string
	err := pool.QueryRow(ctx, resolveAssetAllowlistSQL, chainID).Scan(&snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("no deposit config history for chain %d: whitelist source is undefined", chainID)
	}
	if err != nil {
		return nil, fmt.Errorf("read deposit config history for chain %d: %w", chainID, err)
	}
	return parseAssetAllowlist(snapshot)
}

// parseAssetAllowlist extracts the asset addresses from a canonical 004 policy
// snapshot (`<address>:<effective>` lines, as produced by
// config.DepositSnapshot) into the same canonical form NormalizeWhitelist
// yields: lowercase 0x hex, sorted, deduplicated. A blank snapshot, a blank
// entry, or a malformed address is an error — never a partial or empty list.
func parseAssetAllowlist(snapshot string) ([]string, error) {
	if strings.TrimSpace(snapshot) == "" {
		return nil, errors.New("deposit config asset snapshot is blank")
	}
	lines := strings.Split(snapshot, "\n")
	seen := make(map[string]struct{}, len(lines))
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		addr, _, ok := strings.Cut(ln, ":")
		if !ok || !common.IsHexAddress(addr) {
			return nil, fmt.Errorf("deposit config asset line %q is not address:effective", ln)
		}
		norm := strings.ToLower(addr)
		if _, dup := seen[norm]; dup {
			continue
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}
	sort.Strings(out)
	return out, nil
}

// ValidateAmount enforces FR-06's layered amount rule: the raw transport string
// must match [1-9][0-9]* (rejects zero, leading zeros, signs, decimals,
// exponents, blanks/whitespace and non-digits), then the parsed integer must be
// ≤ 2²⁵⁶−1. Over-length digit strings are parsed exactly with math/big and die
// here in the range check — they never reach the database. The returned value
// is the exact integer; no float is involved at any point.
func ValidateAmount(raw string) (*big.Int, error) {
	if !amountShape.MatchString(raw) {
		return nil, New(CodeValidationFailed,
			"amount must be a decimal string of the form [1-9][0-9]*").
			WithField("amount")
	}
	v, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		// Unreachable after the shape check; guarded so a nil big.Int can
		// never be dereferenced.
		return nil, New(CodeValidationFailed, "amount is not a base-10 integer").
			WithField("amount")
	}
	if v.Cmp(maxUint256) > 0 {
		return nil, New(CodeValidationFailed, "amount exceeds the uint256 maximum (2^256-1)").
			WithField("amount")
	}
	return v, nil
}

// CanonicalAddress enforces FR-07: the address must be 0x + 40 hex characters,
// and a mixed-case Keccak-256 EIP-55 encoded address must equal
// common.HexToAddress(raw).Hex() exactly. All-lowercase and all-uppercase forms
// are accepted without a checksum test. The canonical lowercase form is
// returned, so stored rows are already normalized and compare by equality.
//
// common.IsHexAddress checks shape only, never the checksum, so the mixed-case
// comparison is performed here explicitly.
func CanonicalAddress(raw string) (string, error) {
	if !addressShape.MatchString(raw) {
		return "", New(CodeValidationFailed,
			"address must be 0x followed by 40 hexadecimal characters").
			WithField("recipient")
	}
	hexPart := raw[2:]
	hasLower, hasUpper := false, false
	for i := 0; i < len(hexPart); i++ {
		switch c := hexPart[i]; {
		case c >= 'a' && c <= 'f':
			hasLower = true
		case c >= 'A' && c <= 'F':
			hasUpper = true
		}
	}
	if hasLower && hasUpper && raw != common.HexToAddress(raw).Hex() {
		return "", New(CodeValidationFailed,
			"mixed-case address fails the EIP-55 checksum").
			WithField("recipient")
	}
	return strings.ToLower(raw), nil
}

// ValidateIdempotencyKey enforces FR-09: an opaque caller key of length 1–128
// whose bytes are all ASCII 0x21–0x7E (visible, no space). The key is used
// verbatim: it is never trimmed, lowercased, or otherwise converted, so it
// remains case-sensitive and byte-exact.
func ValidateIdempotencyKey(raw string) error {
	if len(raw) < 1 || len(raw) > 128 {
		return New(CodeValidationFailed, "idempotency_key must be 1-128 bytes").
			WithField("idempotency_key")
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < 0x21 || raw[i] > 0x7e {
			return New(CodeValidationFailed,
				"idempotency_key must contain only visible ASCII (0x21-0x7E)").
				WithField("idempotency_key")
		}
	}
	return nil
}
