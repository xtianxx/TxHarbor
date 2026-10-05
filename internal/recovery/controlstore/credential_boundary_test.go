package controlstore

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type credentialAuditQueryer struct {
	Queryer
	called bool
}

func (q *credentialAuditQueryer) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	q.called = true
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func TestCredentialBoundaryRecognizesJWTAndAccessTokenWithoutRejectingReferences(t *testing.T) {
	for _, raw := range []string{
		`{"access_token":"opaque-canary"}`,
		`{"note":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature123456"}`,
	} {
		if err := ValidateCredentialJSON("audit detail", []byte(raw)); err == nil {
			t.Errorf("expected credential-bearing JSON to be rejected: %s", raw)
		} else if strings.Contains(err.Error(), "canary") || strings.Contains(err.Error(), "opaque") {
			t.Errorf("validation error echoed the secret: %v", err)
		}
	}
	if err := ValidateCredentialText("reason", "access_token=opaque-canary"); err == nil || strings.Contains(err.Error(), "opaque-canary") {
		t.Fatalf("access token annotation should be refused without echo: %v", err)
	}
	for _, value := range []string{"ticket-2026-0912", "authorization_recheck", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		if err := ValidateCredentialText("reference", value); err != nil {
			t.Errorf("public operational reference %q rejected: %v", value, err)
		}
	}
}

func TestWriteAuditCredentialFieldsFailBeforeQuery(t *testing.T) {
	const dsn = "postgres://operator:write-audit-canary@db/control"
	for _, record := range []AuditRecord{
		{Actor: dsn, Action: "probe", Result: AuditOK},
		{Actor: "deploy:operator", Action: "probe", OperationID: dsn, Result: AuditOK},
		{Actor: "deploy:operator", Action: "probe", Detail: []byte(`{"reason":"` + dsn + `"}`), Result: AuditOK},
	} {
		q := &credentialAuditQueryer{}
		err := WriteAudit(context.Background(), q, record)
		if err == nil || q.called || strings.Contains(err.Error(), "write-audit-canary") {
			t.Fatalf("credential audit record should fail before query without echo: called=%v err=%v", q.called, err)
		}
	}
}
