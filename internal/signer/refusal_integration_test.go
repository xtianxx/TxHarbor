//go:build integration

// refusal_integration_test.go owns spec task T015 for 009-signer-service: the
// V2 arbitrary-digest / incomplete-content refusal vectors on a real, isolated
// PostgreSQL (quickstart V2; FR-02; api.md §4 step 2). A complete transaction
// content set is mandatory: bodies carrying only a digest/hash/message, bodies
// missing required fields, and bodies with unknown fields are refused in the
// 4xx pre-sign band with zero signatures and no request row. Authentication
// runs before the body is read, so every case presents a valid credential.
package signer

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestSignerSubmitV2Refusals covers the V2 matrix: digest-only, hash-only,
// message-only and incomplete bodies refuse 4xx with zero signatures, and the
// strict JSON schema rejects an unknown field as malformed_request.
func TestSignerSubmitV2Refusals(t *testing.T) {
	pool := signerAuthStartPG(t)
	ctx := context.Background()
	const callerID = int64(7001)

	cred, err := IssueCredential(ctx, pool, callerID, "submit-refusal")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	deps := submitTestDeps(t, pool)
	full := submitGrantBody()

	cases := []struct {
		name  string
		body  string
		class RefusalClass
	}{
		{"digest only", `{"digest": "0xdeadbeef"}`, ClassArbitraryDigestRejected},
		{"hash only", `{"hash": "0xdeadbeef"}`, ClassArbitraryDigestRejected},
		{"message only", `{"message": "0xdeadbeef"}`, ClassArbitraryDigestRejected},
		{"raw transaction only", `{"raw_transaction": "0x02f8"}`, ClassArbitraryDigestRejected},
		{"incomplete content", `{"signing_request_id": "sr-incomplete"}`, ClassValidationFailed},
		{"missing fee shape", strings.Replace(full, `"max_fee_per_gas": "1500000000",`, ``, 1), ClassValidationFailed},
		{"unknown field", strings.Replace(full,
			`"authorization_id": "`+gateAuthID+`"`, `"authorization_id": "`+gateAuthID+`", "caller_id": 9`, 1), ClassMalformedRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := Submit(ctx, deps, cred, []byte(tc.body))
			if resp != nil {
				t.Fatalf("refused body returned a signature response: %+v", resp)
			}
			re := signerAuthRefusal(t, err, tc.class)
			if status := signerPolicyHTTPStatus(re.Class); status < 400 || status > 499 {
				t.Fatalf("refusal %s maps to HTTP %d, want a 4xx pre-sign band", re.Class, status)
			}
			if got := signerAuthSignatureCount(t, pool); got != 0 {
				t.Fatalf("refusal produced %d signature result(s), want 0", got)
			}
			if got := refusalRequestRowCount(t, pool); got != 0 {
				t.Fatalf("refusal created %d signing_requests row(s), want 0 (refused before the transaction)", got)
			}
		})
	}
}

// refusalRequestRowCount counts every signing_requests row: shape refusals must
// never enter the insert-first path.
func refusalRequestRowCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM signing_requests`).Scan(&n); err != nil {
		t.Fatalf("count signing_requests: %v", err)
	}
	return n
}
