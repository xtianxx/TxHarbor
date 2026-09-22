//go:build integration

package txlifecycle

import (
	"context"
	"testing"
)

// TestV10ClaimFailClosed is quickstart V10's claim_absent acceptance (T053):
// with the claim carrier broken at three independent levels — the fixture
// table absent (SQLSTATE 42P01), the row absent, and a required column absent
// (SQLSTATE 42703) — EVERY send entry point refuses claim_absent, dispatches
// nothing and commits gate_refused evidence. The adapter fails closed and
// never degrades into an unqualified send (FR-14; G-010-4; R-010-11); the
// suite is executable pre-011 because the claim read is 010's own gate.
func TestV10ClaimFailClosed(t *testing.T) {
	cases := []struct {
		name  string
		smash func(f *fixture)
	}{
		{"claim_table_absent", func(f *fixture) {
			f.env.exec(`DROP TABLE execution_claims`)
		}},
		{"claim_row_absent", func(f *fixture) {
			f.env.exec(`DELETE FROM execution_claims WHERE intent_id = $1`, f.intentID)
		}},
		{"claim_column_absent", func(f *fixture) {
			f.env.exec(`ALTER TABLE execution_claims DROP COLUMN owner_id`)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Destructive lane (DROP TABLE / DROP COLUMN): dedicated container
			// per whitelist.
			e := newDedicatedEnv(t)
			f := e.seed()
			f.sign()
			c.smash(f)

			for _, kind := range []SendKind{SendInitial, SendReplay} {
				before := e.rpc.dispatchCount()
				res, err := e.send(f, kind, nil)
				assertBlocked(t, e, f.attemptID, res, err, ClassClaimAbsent, before)

				// Zero dispatch is durable, not just in-process: no
				// tx_send_attempts row may exist for the refused attempt.
				var sends int
				if err := e.pool.QueryRow(context.Background(),
					`SELECT count(*) FROM tx_send_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&sends); err != nil {
					t.Fatal(err)
				}
				if sends != 0 {
					t.Fatalf("%s: %d send rows after a claim_absent refusal, want 0", kind, sends)
				}
				// The refusal moved no revision: the attempt stays signed.
				var revision int64
				if err := e.pool.QueryRow(context.Background(),
					`SELECT revision_seq FROM tx_attempts WHERE attempt_id = $1`, f.attemptID).Scan(&revision); err != nil {
					t.Fatal(err)
				}
				if revision != 2 {
					t.Fatalf("%s: revision_seq = %d, want 2 (refusal is not a mutation)", kind, revision)
				}
			}
		})
	}
}
