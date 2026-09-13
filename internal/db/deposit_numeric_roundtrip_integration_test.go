//go:build integration

package db

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestDepositAmountNumericRoundTrip is the T001/T004 NUMERIC evidence against a
// real PostgreSQL: deposit_observations.amount
// (migrations/000004_deposit_detection.sql) must store the full uint256 range
// exactly through pgx parameter binding. All arithmetic stays in big.Int and
// decimal strings; no float64 is on the path.
//
// Pins:
//   - 1 wei round-trips as decimal "1";
//   - 2^256-1 round-trips as its exact decimal string;
//   - a display-scale value (1.00) comes back as the same value even though the
//     Int/Exp representation differs from what was bound;
//   - amount=0 and amount=-1 are rejected by CHECK (amount > 0), SQLSTATE 23514.
func TestDepositAmountNumericRoundTrip(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	migrateUpAll(t, dsn)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	// deposit_observations.version_seq is FK-bound to the version ledger.
	if _, err := conn.Exec(ctx, `INSERT INTO deposit_config_history
		(chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, operator)
		VALUES (1, 1, $1, 0, 'a', 'w', 0, 'bootstrap')`, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed config version: %v", err)
	}

	uint256Max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if uint256Max.BitLen() != 256 {
		t.Fatalf("uint256 fixture BitLen = %d, want 256", uint256Max.BitLen())
	}

	valid := []struct {
		name    string
		bind    pgtype.Numeric // exactly what pgx sends
		want    *big.Int       // exact value expected back
		wantTxt string         // PostgreSQL amount::text rendering of bind
	}{
		{"one_wei", pgtype.Numeric{Int: big.NewInt(1), Exp: 0, Valid: true}, big.NewInt(1), "1"},
		{"uint256_max", pgtype.Numeric{Int: uint256Max, Exp: 0, Valid: true}, uint256Max, uint256Max.String()},
		{"display_scale", pgtype.Numeric{Int: big.NewInt(100), Exp: -2, Valid: true}, big.NewInt(1), "1.00"},
	}
	for i, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			bh, th := hash64("11"), hash64("22")
			if _, err := conn.Exec(ctx, `INSERT INTO deposit_observations
				(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
				VALUES (1, $1, $2, $3, 1, $4, $5, $6, $7, 1)`,
				bh, th, i, addr40("33"), addr40("44"), addr40("55"), tc.bind); err != nil {
				t.Fatalf("insert amount %s: %v", tc.want, err)
			}

			var got pgtype.Numeric
			var gotText string
			if err := conn.QueryRow(ctx, `SELECT amount, amount::text FROM deposit_observations
				WHERE chain_id = 1 AND block_hash = $1 AND tx_hash = $2 AND log_index = $3`,
				bh, th, i).Scan(&got, &gotText); err != nil {
				t.Fatalf("select amount: %v", err)
			}

			if gotText != tc.wantTxt {
				t.Errorf("amount::text = %q, want %q", gotText, tc.wantTxt)
			}
			if gotInt := numericIntegerValue(t, got); gotInt.Cmp(tc.want) != 0 {
				t.Errorf("numeric value = %s, want %s (bound %s * 10^%d)", gotInt, tc.want, tc.bind.Int, tc.bind.Exp)
			}
		})
	}

	invalid := []struct {
		name string
		amt  *big.Int
	}{
		{"zero", big.NewInt(0)},
		{"negative", big.NewInt(-1)},
	}
	for i, tc := range invalid {
		t.Run(tc.name+"_rejected", func(t *testing.T) {
			_, err := conn.Exec(ctx, `INSERT INTO deposit_observations
				(chain_id, block_hash, tx_hash, log_index, block_number, contract, sender, recipient, amount, version_seq)
				VALUES (1, $1, $2, $3, 1, $4, $5, $6, $7, 1)`,
				hash64("66"), hash64("77"), i, addr40("33"), addr40("44"), addr40("55"),
				pgtype.Numeric{Int: tc.amt, Exp: 0, Valid: true})
			wantPgErrorCode(t, err, "23514")
		})
	}
}

// numericIntegerValue returns n's exact value, normalizing the Int/Exp
// representation (Exp is display scale, not part of the value) so the caller
// compares by value rather than memory layout.
func numericIntegerValue(t *testing.T, n pgtype.Numeric) *big.Int {
	t.Helper()
	if !n.Valid || n.NaN || n.InfinityModifier != pgtype.Finite || n.Int == nil {
		t.Fatalf("unexpected numeric value: %+v", n)
	}
	v := new(big.Int).Set(n.Int)
	for exp := int64(n.Exp); exp < 0; exp++ {
		q, r := new(big.Int).QuoRem(v, big.NewInt(10), new(big.Int))
		if r.Sign() != 0 {
			t.Fatalf("numeric %s * 10^%d is not an integer", v, n.Exp)
		}
		v = q
	}
	if n.Exp > 0 {
		v.Mul(v, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil))
	}
	return v
}
