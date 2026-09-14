//go:build integration

// T017 2^63 exact-audit path on a real PostgreSQL (the real migration-000005
// schema via startIndexerPostgres): tip=MaxInt64, h=0 writes, reads and audits
// exactly 2^63 end to end with no int64/float64 transit. The T001
// integer/non-negative CHECK probes below are targeted refusals only; the full
// constraint matrix lives in
// internal/db/confirmation_migration_integration_test.go
// (TestConfirmationMigrationSchemaConstraints) and is cited, not duplicated.
// DDL is untouched: 000005 already landed confirmations as NUMERIC with the
// integer + non-negative CHECKs (OI-1 closed).
package indexer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// wantCheckViolation pins a PostgreSQL check-violation refusal (SQLSTATE
// 23514): the T001 integer/non-negative CHECKs on the NUMERIC column.
func wantCheckViolation(t *testing.T, err error, what string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("%s: err = %v, want SQLSTATE 23514 check violation", what, err)
	}
}

// TestConfirmNumericExact2To63EndToEnd is T017's deliverable: tip=MaxInt64,
// h=0 yields exactly 2^63 through the write path (uint64 -> decimal-string ->
// NUMERIC), the read path (decimal-string -> uint64) and a SQL-level audit
// recomputation. Only the two boundary chain_blocks rows are seeded (tip and
// h=0); no real chain at that height is required — the commit adjudicates the
// tip point-read and the h=0 reference row, never a full range.
func TestConfirmNumericExact2To63EndToEnd(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(81)
	const h = uint64(0)
	const tip = uint64(1<<63 - 1) // MaxInt64: highest BIGINT tip the schema can hold
	const n = uint64(1)
	const want = "9223372036854775808" // 2^63

	tipHash := depositBlockHash(tip)
	zeroParent := "0x" + strings.Repeat("00", 32)
	if _, err := pool.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, TRUE), ($5, $6, $7, $8, TRUE)`,
		chainID, int64(h), depositBlockHash(h), zeroParent,
		chainID, int64(tip), tipHash, depositBlockHash(tip-1)); err != nil {
		t.Fatalf("seed boundary chain_blocks: %v", err)
	}
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, h)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, int64(n), nil, "bootstrap", nil)

	exact, err := ExactConfirmations(tip, h)
	if err != nil {
		t.Fatalf("ExactConfirmations(MaxInt64,0): %v", err)
	}
	if exact != uint64(1)<<63 {
		t.Fatalf("ExactConfirmations(MaxInt64,0) = %d, want 2^63", exact)
	}

	c, lease := confirmCommitter(t, pool, chainID, n)
	basis := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: h,
		TipNumber: tip, TipHash: tipHash, PolicySeq: 1, ThresholdN: n}
	if err := c.ConfirmDepositUnit(ctx, lease, basis); err != nil {
		t.Fatalf("ConfirmDepositUnit(): %v", err)
	}

	status, _, _, _, _, _, conf := confirmReadBasis(t, ctx, pool, chainID, bh, txHash)
	if status != "confirmed" || conf != want {
		t.Fatalf("stored = status %s confirmations %q, want confirmed/%s", status, conf, want)
	}
	var raw string
	if err := pool.QueryRow(ctx, `SELECT confirmations::text FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`,
		chainID, bh, txHash).Scan(&raw); err != nil {
		t.Fatalf("raw SQL text read: %v", err)
	}
	if raw != want {
		t.Fatalf("raw confirmations::text = %q, want %q", raw, want)
	}
	if got, err := parseNumericConfirmations(raw); err != nil || got != uint64(1)<<63 {
		t.Fatalf("parseNumericConfirmations(%q) = (%d, %v), want (2^63, nil)", raw, got, err)
	}

	// Audit side: recompute tip-h+1 at the SQL level in exact NUMERIC
	// arithmetic and require it to equal both the stored value and 2^63.
	var recomputed, stored string
	if err := pool.QueryRow(ctx, `
SELECT (confirm_tip_number::NUMERIC - block_number::NUMERIC + 1)::TEXT, confirmations::TEXT
FROM deposit_observations
WHERE chain_id = $1 AND block_hash = $2 AND tx_hash = $3 AND log_index = 0`,
		chainID, bh, txHash).Scan(&recomputed, &stored); err != nil {
		t.Fatalf("audit recomputation: %v", err)
	}
	if recomputed != want || stored != want {
		t.Fatalf("audit = recomputed %q stored %q, want both %q", recomputed, stored, want)
	}
}

// TestConfirmNumericChecksRefuseDecimalAndNegative pins the T001 CHECKs
// (confirmations >= 0 AND confirmations = floor(confirmations)) against a
// decimal and a negative. Full-matrix coverage stays in the db package (see
// file header); these two probes are the T017 closeout assertions.
func TestConfirmNumericChecksRefuseDecimalAndNegative(t *testing.T) {
	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(82)
	depositSeedHistory(t, ctx, pool, chainID, 1, 10, strings.Repeat("aa", 32))
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 10, nil, "bootstrap", nil)
	goodTip := depositBlockHash(12)

	const insertConfirmed = `
INSERT INTO deposit_observations
    (chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount,
     status, version_seq, confirmed_at, confirm_tip_number, confirm_tip_hash,
     confirm_threshold, confirmations, confirm_policy_seq)
VALUES ($1, $2, $3, 0, 7, $4, $5, $6, '100',
    'confirmed', 1, now(), 12, $7, 10, $8, 1)`
	_, err := pool.Exec(ctx, insertConfirmed,
		chainID, depositBlockHash(101), depositTxHash(101, 0),
		testContractA, testContractB, depositWatchAddr, goodTip, "10.5")
	wantCheckViolation(t, err, "decimal confirmations 10.5")

	_, err = pool.Exec(ctx, insertConfirmed,
		chainID, depositBlockHash(102), depositTxHash(102, 0),
		testContractA, testContractB, depositWatchAddr, goodTip, "-1")
	wantCheckViolation(t, err, "negative confirmations -1")

	if n := depositCountRows(t, ctx, pool, "deposit_observations", chainID); n != 0 {
		t.Fatalf("deposit_observations rows = %d, want 0 (both refused inserts wrote nothing)", n)
	}
}

// TestConfirmNumericSaturationGuardRefusesCommit pins the unreachable-defense
// saturation guard: tip==MaxUint64 && h==0 has no exact value, so
// ExactConfirmations refuses with ErrConfirmationsSaturated and nothing is
// stored. The saturated tip is inexpressible in BIGINT storage, hence the
// commit-level wrap ("confirmation commit refused") is unreachable through a
// real DB; the commit half is proven by refusing a saturated-equivalent basis
// (claimed tip MaxUint64 against the stored MaxInt64 tip) with zero writes.
func TestConfirmNumericSaturationGuardRefusesCommit(t *testing.T) {
	maxU := ^uint64(0)
	if _, err := ExactConfirmations(maxU, 0); !errors.Is(err, ErrConfirmationsSaturated) {
		t.Fatalf("ExactConfirmations(MaxUint64,0) = %v, want ErrConfirmationsSaturated", err)
	}
	if _, err := parseNumericConfirmations("18446744073709551616"); err == nil {
		t.Fatal("parseNumericConfirmations(2^64) = nil error, want over-range refusal")
	}
	// Boundary just below saturation round-trips exactly through both halves.
	if got := confirmNumericConfirmations(maxU); got.Int == nil || got.Int.String() != "18446744073709551615" {
		t.Fatalf("confirmNumericConfirmations(MaxUint64) = %v, want exact decimal", got.Int)
	}
	if back, err := parseNumericConfirmations("18446744073709551615"); err != nil || back != maxU {
		t.Fatalf("parseNumericConfirmations(MaxUint64) = (%d, %v), want exact round-trip", back, err)
	}

	dsn := startIndexerPostgres(t)
	pool := openIndexerPool(t, dsn)
	defer pool.Close()
	ctx := context.Background()

	const chainID = int64(83)
	const tip = uint64(1<<63 - 1)
	tipHash := depositBlockHash(tip)
	zeroParent := "0x" + strings.Repeat("00", 32)
	if _, err := pool.Exec(ctx, `
INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
VALUES ($1, $2, $3, $4, TRUE), ($5, $6, $7, $8, TRUE)`,
		chainID, int64(0), depositBlockHash(0), zeroParent,
		chainID, int64(tip), tipHash, depositBlockHash(tip-1)); err != nil {
		t.Fatalf("seed boundary chain_blocks: %v", err)
	}
	bh, txHash := confirmSeedPending(t, ctx, pool, chainID, 0)
	confirmSeedPolicyRow(t, ctx, pool, chainID, 1, 1, nil, "bootstrap", nil)

	c, lease := confirmCommitter(t, pool, chainID, 1)
	saturated := ConfirmBasis{BlockHash: bh, TxHash: txHash, Height: 0,
		TipNumber: maxU, TipHash: tipHash, PolicySeq: 1, ThresholdN: 1}
	if err := c.ConfirmDepositUnit(ctx, lease, saturated); err == nil {
		t.Fatal("ConfirmDepositUnit(saturated tip basis) = nil error, want refusal")
	}
	confirmAssertZeroWrite(t, ctx, pool, chainID, bh, txHash, 1)
}
