//go:build integration

package recovery

import (
	"strings"
	"testing"
)

func TestChecklistDirectCallsRejectCredentialCanariesWithoutWrites(t *testing.T) {
	f, checklist := isoChecklistFixture(t)
	const uriPassword = "checklist-uri-canary-4a71"
	const keywordToken = "checklist-token-canary-2b83"
	const hexKey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const privateKeyHex = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	cases := []struct {
		name   string
		call   func() error
		canary string
	}{
		{"evidence URI", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "postgres://operator:" + uriPassword + "@db.example.test/app", Actor: "deploy:executor"})
			return err
		}, uriPassword},
		{"nested summary token", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/clean", CheckpointSummary: []byte(`{"nested":{"token":"` + keywordToken + `"}}`), Actor: "deploy:executor"})
			return err
		}, keywordToken},
		{"private PEM summary", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/pem", CheckpointSummary: []byte(`{"detail":"-----BEGIN PRIVATE KEY-----\ncanary\n-----END PRIVATE KEY-----"}`), Actor: "deploy:executor"})
			return err
		}, "BEGIN PRIVATE KEY"},
		{"hex private key summary", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/hex", CheckpointSummary: []byte(`{"private_key":"` + hexKey + `"}`), Actor: "deploy:executor"})
			return err
		}, hexKey},
		{"nested authorization field", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/auth", CheckpointSummary: []byte(`{"checks":[{"authorization":"Basic direct-canary"}]}`), Actor: "deploy:executor"})
			return err
		}, "Basic direct-canary"},
		{"nested privatekey field", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/privatekey", CheckpointSummary: []byte(`{"checks":[{"privatekey":"` + privateKeyHex + `"}]}`), Actor: "deploy:executor"})
			return err
		}, privateKeyHex},
		{"nested client_secret field", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/client-secret", CheckpointSummary: []byte(`{"checks":[{"client_secret":"client-secret-canary"}]}`), Actor: "deploy:executor"})
			return err
		}, "client-secret-canary"},
		{"nested client-secret field", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/client-secret-hyphen", CheckpointSummary: []byte(`{"checks":[{"client-secret":"client-secret-canary"}]}`), Actor: "deploy:executor"})
			return err
		}, "client-secret-canary"},
		{"whitespace padded private_key key", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/padded-private-key", CheckpointSummary: []byte(`{"checks":[{" private_key ":"padded-private-key-canary"}]}`), Actor: "deploy:executor"})
			return err
		}, "padded-private-key-canary"},
		{"whitespace padded authorization key", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/padded-authorization", CheckpointSummary: []byte(`{"checks":[{" authorization ":"padded-authorization-canary"}]}`), Actor: "deploy:executor"})
			return err
		}, "padded-authorization-canary"},
		{"whitespace padded client_secret key", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/padded-client-secret", CheckpointSummary: []byte(`{"checks":[{" client_secret ":"padded-client-secret-canary"}]}`), Actor: "deploy:executor"})
			return err
		}, "padded-client-secret-canary"},
		{"private-key alias field", func() error {
			_, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced, EvidenceRef: "evidence://015/private-key", CheckpointSummary: []byte(`{"nested":{"private-key":"` + privateKeyHex + `"}}`), Actor: "deploy:executor"})
			return err
		}, privateKeyHex},
		{"verify rejection reason", func() error {
			_, err := checklist.Verify(f.ctx, ChecklistVerifyRequest{InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, Actor: "auth:verifier", Reject: true, Reason: "password=" + uriPassword})
			return err
		}, uriPassword},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var beforeAudit int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2`, f.instanceID, ActionIsolationCheck).Scan(&beforeAudit); err != nil {
				t.Fatal(err)
			}
			var beforeItems int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_isolation_check WHERE instance_id=$1`, f.instanceID).Scan(&beforeItems); err != nil {
				t.Fatal(err)
			}
			err := tc.call()
			if err == nil {
				t.Fatal("credential-shaped input accepted")
			}
			if strings.Contains(err.Error(), tc.canary) {
				t.Fatalf("error disclosed canary: %v", err)
			}
			var afterItems, afterAudit int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_isolation_check WHERE instance_id=$1`, f.instanceID).Scan(&afterItems); err != nil {
				t.Fatal(err)
			}
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM recovery_audit WHERE instance_id=$1 AND action=$2`, f.instanceID, ActionIsolationCheck).Scan(&afterAudit); err != nil {
				t.Fatal(err)
			}
			if afterItems != beforeItems || afterAudit != beforeAudit {
				t.Fatalf("rejected direct call wrote state: items %d→%d, checklist audits %d→%d", beforeItems, afterItems, beforeAudit, afterAudit)
			}
			var persisted string
			if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT COALESCE(string_agg(to_jsonb(a)::text, ' '), '') FROM recovery_audit a WHERE instance_id=$1) || ' ' || (SELECT COALESCE(string_agg(to_jsonb(c)::text, ' '), '') FROM recovery_isolation_check c WHERE instance_id=$1)`, f.instanceID).Scan(&persisted); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(persisted, tc.canary) {
				t.Fatalf("database contains canary %q", tc.canary)
			}
		})
	}
	if _, err := checklist.Set(f.ctx, ChecklistEvidenceRequest{
		InstanceID: f.instanceID, ItemKey: IsolationItemOldWritersStopped, State: ChecklistStateEvidenced,
		EvidenceRef: "evidence://015/padded-safe", CheckpointSummary: []byte(`{" status ":"healthy"," key_fingerprint ":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
		Actor: "deploy:executor",
	}); err != nil {
		t.Fatalf("ordinary padded status/fingerprint fields rejected: %v", err)
	}
}
