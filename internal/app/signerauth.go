package app

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/signer"
)

// SignerAuth runs the signer credential lifecycle carrier:
//
//	txharbor signer-auth issue --caller-id C --label L --operator OP --reason R
//	txharbor signer-auth rotate --caller-id C --credential-id K --operator OP --reason R
//	txharbor signer-auth revoke --credential-id K --operator OP --reason R
//	txharbor signer-auth set-can-sign --caller-id C --can-sign true|false --operator OP --reason R
//
// Carrier shape mirrors apikey-auth: action dispatch, flag parsing,
// config.Load for the DSN, a pgxpool connect over the DB operator's
// connection, and one call into internal/signer's credential core. Secrets
// are generated from crypto/rand inside the library, stored as SHA-256 only,
// and printed exactly once on the stdout success line; errors are redacted.
// Exit codes mirror apikey-auth: 0 committed, 1 failed, 2 usage. Rotation is
// immediate (successor insert + predecessor revoke in one transaction); there
// is no grace window. The operator identity and reason are CLI-accepted for
// runbook parity and echoed on the stdout success line as the operator paper
// trail; no operator/reason reaches the database. Trust root is DSN
// possession, identical to migrate/apikey-auth.
func SignerAuth(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		signerAuthUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "issue":
		return signerAuthIssue(ctx, args[1:], d)
	case "rotate":
		return signerAuthRotate(ctx, args[1:], d)
	case "revoke":
		return signerAuthRevoke(ctx, args[1:], d)
	case "set-can-sign":
		return signerAuthSetCanSign(ctx, args[1:], d)
	default:
		fmt.Fprintf(d.stderr(), "txharbor signer-auth: unknown action %q\n", args[0])
		signerAuthUsage(d.stderr())
		return 2
	}
}

func signerAuthIssue(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("signer-auth issue", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "stable caller identity to issue for (required, positive)")
	label := fs.String("label", "", "caller display label, set on first issue (required)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the issue (required)")
	if err := fs.Parse(args); err != nil {
		signerAuthUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *callerID <= 0 || *label == "" || *operator == "" || *reason == "" {
		signerAuthUsage(stderr)
		return 2
	}
	pool, code := signerAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()
	plaintext, err := signer.IssueCredential(ctx, pool, *callerID, *label)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-auth: issue failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor signer-auth: issued caller_id=%d key=%s operator=%s reason=%s\n",
		*callerID, plaintext, *operator, *reason)
	return 0
}

func signerAuthRotate(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("signer-auth rotate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "stable caller identity to rotate (required, positive)")
	credentialID := fs.Int64("credential-id", 0, "predecessor credential id to revoke (required, positive)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the rotation (required)")
	if err := fs.Parse(args); err != nil {
		signerAuthUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *callerID <= 0 || *credentialID <= 0 || *operator == "" || *reason == "" {
		signerAuthUsage(stderr)
		return 2
	}
	pool, code := signerAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()
	plaintext, err := signer.RotateCredential(ctx, pool, *callerID, *credentialID)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-auth: rotate failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor signer-auth: rotated caller_id=%d key=%s operator=%s reason=%s\n",
		*callerID, plaintext, *operator, *reason)
	return 0
}

func signerAuthRevoke(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("signer-auth revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	credentialID := fs.Int64("credential-id", 0, "credential id to revoke (required, positive)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the revocation (required)")
	if err := fs.Parse(args); err != nil {
		signerAuthUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *credentialID <= 0 || *operator == "" || *reason == "" {
		signerAuthUsage(stderr)
		return 2
	}
	pool, code := signerAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()
	if err := signer.RevokeCredential(ctx, pool, *credentialID); err != nil {
		fmt.Fprintf(stderr, "txharbor signer-auth: revoke failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor signer-auth: revoked credential_id=%d operator=%s reason=%s\n",
		*credentialID, *operator, *reason)
	return 0
}

func signerAuthSetCanSign(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	fs := flag.NewFlagSet("signer-auth set-can-sign", flag.ContinueOnError)
	fs.SetOutput(stderr)
	callerID := fs.Int64("caller-id", 0, "stable caller identity (required, positive)")
	canSignRaw := fs.String("can-sign", "", "signing permission true|false (required)")
	operator := fs.String("operator", "", "declared operator identity for the paper trail (required)")
	reason := fs.String("reason", "", "operator reason for the change (required)")
	if err := fs.Parse(args); err != nil {
		signerAuthUsage(stderr)
		return 2
	}
	canSign, err := strconv.ParseBool(*canSignRaw)
	if err != nil || fs.NArg() > 0 || *callerID <= 0 || *operator == "" || *reason == "" {
		signerAuthUsage(stderr)
		return 2
	}
	pool, code := signerAuthOpenPool(ctx, d)
	if pool == nil {
		return code
	}
	defer pool.Close()
	if err := signer.SetCanSign(ctx, pool, *callerID, canSign); err != nil {
		fmt.Fprintf(stderr, "txharbor signer-auth: set-can-sign failed: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintf(stdout, "txharbor signer-auth: set-can-sign caller_id=%d can_sign=%t operator=%s reason=%s\n",
		*callerID, canSign, *operator, *reason)
	return 0
}

func signerAuthOpenPool(ctx context.Context, d Deps) (*pgxpool.Pool, int) {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-auth: configuration error: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-auth: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	return pool, 0
}

func signerAuthUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor signer-auth issue --caller-id C --label L --operator OP --reason R")
	fmt.Fprintln(w, "       txharbor signer-auth rotate --caller-id C --credential-id K --operator OP --reason R")
	fmt.Fprintln(w, "       txharbor signer-auth revoke --credential-id K --operator OP --reason R")
	fmt.Fprintln(w, "       txharbor signer-auth set-can-sign --caller-id C --can-sign true|false --operator OP --reason R")
}
