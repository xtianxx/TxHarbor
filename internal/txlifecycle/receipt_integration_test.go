//go:build integration

package txlifecycle

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
)

// TestV8ReceiptVerdicts is quickstart V8: exact FR-08 verdicts over the
// expected Transfer, never "successful" on a mismatch, canonicality decided
// against chain_blocks with an unindexed height staying unverified.
func TestV8ReceiptVerdicts(t *testing.T) {
	e := newEnv(t)

	cases := []struct {
		name       string
		status     int
		logs       func(f *fixture) []*types.Log
		wantEffect string
		wantState  string
	}{
		{"effective", 1, func(f *fixture) []*types.Log { return []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)} }, "effective", "effective"},
		{"status_zero", 0, func(f *fixture) []*types.Log { return []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)} }, "ineffective_status", "ineffective"},
		{"missing_transfer", 1, func(f *fixture) []*types.Log { return nil }, "ineffective_transfer_missing", "ineffective"},
		{"wrong_recipient", 1, func(f *fixture) []*types.Log {
			return []*types.Log{xferLog(fxAsset, f.sender, "0x9999999999999999999999999999999999999999", 1000)}
		}, "ineffective_transfer_mismatch", "ineffective"},
		{"wrong_amount", 1, func(f *fixture) []*types.Log { return []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 999)} }, "ineffective_transfer_mismatch", "ineffective"},
		{"wrong_emitter", 1, func(f *fixture) []*types.Log {
			return []*types.Log{xferLog("0x8888888888888888888888888888888888888888", f.sender, fxRecipient, 1000)}
		}, "ineffective_transfer_mismatch", "ineffective"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := e.seed()
			receipt := receiptAt(100, blockHashHex(100), c.status, c.logs(f))
			res := e.reconcileIncluded(t, f, receipt)
			if res.ReceiptEffect != c.wantEffect {
				t.Fatalf("effect = %s, want %s", res.ReceiptEffect, c.wantEffect)
			}
			if res.AttemptState != c.wantState {
				t.Fatalf("state = %s, want %s", res.AttemptState, c.wantState)
			}
			var canonicality string
			if err := e.pool.QueryRow(context.Background(),
				`SELECT canonicality FROM tx_receipts WHERE attempt_id = $1`, f.attemptID).Scan(&canonicality); err != nil {
				t.Fatal(err)
			}
			if canonicality != "canonical" {
				t.Fatalf("canonicality = %s, want canonical", canonicality)
			}
		})
	}

	t.Run("extra_logs_stay_effective", func(t *testing.T) {
		f := e.seed()
		logs := []*types.Log{
			xferLog(fxAsset, "0x7777777777777777777777777777777777777777", "0x6666666666666666666666666666666666666666", 5),
			xferLog(fxAsset, f.sender, fxRecipient, 1000),
		}
		res := e.reconcileIncluded(t, f, receiptAt(100, blockHashHex(100), 1, logs))
		if res.ReceiptEffect != "effective" {
			t.Fatalf("extra logs effect = %s", res.ReceiptEffect)
		}
	})

	t.Run("unindexed_height_stays_unverified", func(t *testing.T) {
		f := e.seed()
		receipt := receiptAt(150, blockHashHex(150), 1, []*types.Log{xferLog(fxAsset, f.sender, fxRecipient, 1000)})
		res := e.reconcileIncluded(t, f, receipt)
		if res.ReceiptEffect != "effective" {
			t.Fatalf("unindexed effect = %s", res.ReceiptEffect)
		}
		if res.AttemptState == "effective" {
			t.Fatal("unindexed receipt was assumed canonical")
		}
		var canonicality string
		if err := e.pool.QueryRow(context.Background(),
			`SELECT canonicality FROM tx_receipts WHERE attempt_id = $1`, f.attemptID).Scan(&canonicality); err != nil {
			t.Fatal(err)
		}
		if canonicality != "unverified" {
			t.Fatalf("canonicality = %s, want unverified", canonicality)
		}
	})
}
