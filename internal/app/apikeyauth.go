package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// APIKeyAuth runs the privileged API-key lifecycle carrier (T034, research R5):
//
//	txharbor apikey-auth issue  --caller-id C --label L --operator OP --reason R
//	txharbor apikey-auth rotate --caller-id C [--grace-seconds S] --operator OP --reason R
//	txharbor apikey-auth revoke --key-id K --operator OP --reason R
//
// Carrier shape (mirrors ConfirmAuth): args[0] selects the action, then flag
// parsing, config.Load for the DSN (full serve-env validation, exactly like
// migrate does — run it with the serve env file), a pgxpool connect over the
// DB operator's connection, and one call into internal/withdrawal's credential
// core (IssueKey / RotateKey / RevokeKey). The library generates every secret
// from crypto/rand (tooling never accepts caller-supplied key material),
// stores only the SHA-256 digest, and returns the plaintext exactly once; this
// command prints that plaintext on the single stdout success line and never
// logs it, echoes it in an error, or writes it to stderr.
//
// Exit codes mirror ConfirmAuth: 0 the action committed, 1 the action failed
// (stderr carries a redacted reason), 2 flag usage errors. A bare psql INSERT
// is forbidden: the credential invariants (positive caller_id, digest-only
// persistence, predecessor stamping, revoke-wins) live inside the reviewed
// library functions, and this is the only binary path that executes them.
//
// The operator identity and reason are CLI-accepted for runbook parity with
// the other privileged carriers and are echoed on the stdout success line as
// the operator paper trail. Credential Tables 1-2 carry no operator column in
// 007: this command writes no audit rows and no operator/reason to the
// database — the runbook (T032) owns the operator-layer record. The trust root
// is DSN possession, identical to migrate/confirm-auth.
func APIKeyAuth(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		apiKeyAuthUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "issue":
		return apiKeyAuthIssue(ctx, args[1:], d)
	case "rotate":
		return apiKeyAuthRotate(ctx, args[1:], d)
	case "revoke":
		return apiKeyAuthRevoke(ctx, args[1:], d)
	default:
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor apikey-auth: unknown action %q\n", args[0])
		apiKeyAuthUsage(stderr)
		return 2
	}
}

// apiKeyAuthIssue creates (or reuses) the caller row and mints one credential,
// printing the plaintext exactly once. All four flags are required and C must
// be a positive integer.
func apiKeyAuthIssue(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("apikey-auth issue", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "stable caller identity to issue for (required, positive)")
	label := fs.String("label", "", "caller display label, set on first issue (required)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the issue (required)")
	if err := fs.Parse(args); err != nil {
		apiKeyAuthUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *callerID <= 0 || *label == "" || *operator == "" || *reason == "" {
		apiKeyAuthUsage(stderr)
		return 2
	}

	pool, code := apiKeyAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()

	plaintext, ref, err := withdrawal.IssueKey(ctx, pool, *callerID, *label)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor apikey-auth: issue failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor apikey-auth: issued caller_id=%d key_id=%d prefix=%s key=%s operator=%s reason=%s\n",
		ref.CallerID, ref.KeyID, ref.Prefix, plaintext, *operator, *reason)
	return 0
}

// apiKeyAuthRotate mints a successor credential for an existing caller and
// stamps the predecessor revoked_at = now() + graceSeconds, so a positive
// grace leaves a dual-accept window while 0 revokes immediately. Rotation
// never mutates caller_id (identity and idempotency scope survive).
func apiKeyAuthRotate(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("apikey-auth rotate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "stable caller identity to rotate (required, positive)")
	grace := fs.Int64("grace-seconds", 0, "dual-accept window for the predecessor (default 0 = immediate)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the rotation (required)")
	if err := fs.Parse(args); err != nil {
		apiKeyAuthUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *callerID <= 0 || *grace < 0 || *operator == "" || *reason == "" {
		apiKeyAuthUsage(stderr)
		return 2
	}

	pool, code := apiKeyAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()

	plaintext, ref, err := withdrawal.RotateKey(ctx, pool, *callerID, *grace)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor apikey-auth: rotate failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor apikey-auth: rotated caller_id=%d key_id=%d prefix=%s grace_seconds=%d key=%s operator=%s reason=%s\n",
		ref.CallerID, ref.KeyID, ref.Prefix, *grace, plaintext, *operator, *reason)
	return 0
}

// apiKeyAuthRevoke immediately terminates one still-effective credential. A
// missing or already-terminated key_id is a not-found failure (exit 1) that
// names the key_id and carries no secret material.
func apiKeyAuthRevoke(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("apikey-auth revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	keyID := fs.Int64("key-id", 0, "credential key_id to revoke (required, positive)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the revocation (required)")
	if err := fs.Parse(args); err != nil {
		apiKeyAuthUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *keyID <= 0 || *operator == "" || *reason == "" {
		apiKeyAuthUsage(stderr)
		return 2
	}

	pool, code := apiKeyAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()

	if err := withdrawal.RevokeKey(ctx, pool, *keyID); err != nil {
		var typed *withdrawal.Error
		if errors.As(err, &typed) && typed.Code == withdrawal.CodeNotFound {
			fmt.Fprintf(stderr, "txharbor apikey-auth: revoke failed: no such key_id=%d\n", *keyID)
			return 1
		}
		fmt.Fprintf(stderr, "txharbor apikey-auth: revoke failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor apikey-auth: revoked key_id=%d operator=%s reason=%s\n", *keyID, *operator, *reason)
	return 0
}

// apiKeyAuthOpenPool loads the serve config (full validation, redacted
// diagnostics) and opens the operator connection. A nil pool means a non-zero
// exit code was already reported on stderr.
func apiKeyAuthOpenPool(ctx context.Context, d Deps) (*pgxpool.Pool, int) {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor apikey-auth: configuration error: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor apikey-auth: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	return pool, 0
}

// apiKeyAuthUsage prints the three action forms. It is the only usage text, so
// every usage error points at the exact accepted shape.
func apiKeyAuthUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor apikey-auth issue --caller-id C --label L --operator OP --reason R")
	fmt.Fprintln(w, "       txharbor apikey-auth rotate --caller-id C [--grace-seconds S] --operator OP --reason R")
	fmt.Fprintln(w, "       txharbor apikey-auth revoke --key-id K --operator OP --reason R")
}
