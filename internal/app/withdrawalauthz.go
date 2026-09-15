package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// WithdrawalAuthz runs the privileged withdrawal-authorization supply carrier
// (T009, R9): `txharbor withdrawal-authz <mint|supply|revoke> [flags]`, the
// operator entry point for withdrawal.MintOperationID / SupplyGrant /
// RevokeGrant.
//
// Carrier shape (mirrors ConfirmAuth): `flag` parsing, then config.Load for the
// DSN (full serve-env validation, run with the serve env file), then a pgxpool
// connect over the DB operator's connection, then the repository-owned
// transaction. The operator identity is the DB operator running this binary
// with DSN access (same trust as migrate/confirm-auth); --operator is recorded
// verbatim as the declared supply identity — an audit claim, not a
// cryptographic proof.
//
// Actions: `mint` prints one opaque operation id and is pure entropy — it
// validates no configuration and opens no connection, so nothing is persisted
// until supply/revoke and no DSN is needed to mint (R9: the caller MUST
// durably capture the id first). `supply`/`revoke` REQUIRE --operation-id (no
// auto-mint, no echo-fallback): a missing id is a usage error with zero DB side
// effects. `supply` additionally binds --chain-id to this deployment's chain
// (FR-04) before any connection is opened.
//
// Exit codes mirror ConfirmAuth: 0 the attempt committed (or an equal retry
// converged on the recorded outcome), 1 the attempt was refused or failed
// (stderr carries the redacted reason; operation_conflict,
// temporarily_unavailable, and validation_failed all exit 1 because the flags
// parsed fine), 2 flag usage errors — including a missing --operation-id, which
// is detected before any configuration or database access.
func WithdrawalAuthz(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()
	if len(args) == 0 {
		withdrawalAuthzUsage(stderr)
		return 2
	}
	switch args[0] {
	case "mint":
		return withdrawalAuthzMint(stdout, stderr, args[1:])
	case "supply":
		return withdrawalAuthzSupply(ctx, args[1:], d)
	case "revoke":
		return withdrawalAuthzRevoke(ctx, args[1:], d)
	default:
		fmt.Fprintf(stderr, "txharbor withdrawal-authz: unknown action %q\n", args[0])
		withdrawalAuthzUsage(stderr)
		return 2
	}
}

// withdrawalAuthzUsage documents all three actions; every usage error routes
// here so the operator sees the full carrier shape.
func withdrawalAuthzUsage(w io.Writer) {
	fmt.Fprint(w, `usage: txharbor withdrawal-authz mint
       txharbor withdrawal-authz supply --operation-id O --authorization-id G --caller-id C --chain-id N --asset 0x… --recipient 0x… --amount D [--expires-at RFC3339] --operator OP --reason R
       txharbor withdrawal-authz revoke --operation-id O --authorization-id G --operator OP --reason R

mint prints one opaque operation id; capture it durably before supply/revoke.
--operation-id is required on supply and revoke (no auto-mint, no echo-fallback).
`)
}

// withdrawalAuthzMint prints one fresh opaque operation id (32 lowercase hex).
// It is pure entropy: no config.Load, no pool, nothing persisted — the attempt
// does not exist until the caller supplies/revokes with this id.
func withdrawalAuthzMint(stdout, stderr io.Writer, args []string) int {
	if len(args) > 0 {
		withdrawalAuthzUsage(stderr)
		return 2
	}
	id, err := withdrawal.MintOperationID()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz mint: %s\n", logx.Redact(err.Error()))
		return 1
	}
	fmt.Fprintln(stdout, id)
	return 0
}

func withdrawalAuthzSupply(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("withdrawal-authz supply", flag.ContinueOnError)
	fs.SetOutput(stderr)
	operationID := fs.String("operation-id", "", "opaque caller-minted attempt id (required)")
	authorizationID := fs.String("authorization-id", "", "upstream-issued grant id")
	callerIDRaw := fs.String("caller-id", "", "grant's caller id (positive integer)")
	chainIDRaw := fs.String("chain-id", "", "grant's chain id; must equal this deployment's chain")
	asset := fs.String("asset", "", "0x-prefixed asset address")
	recipient := fs.String("recipient", "", "0x-prefixed recipient address")
	amount := fs.String("amount", "", "decimal amount of the form [1-9][0-9]*")
	expiresAtRaw := fs.String("expires-at", "", "optional RFC3339 expiry (must be in the future)")
	operator := fs.String("operator", "", "declared operator identity for the audit row")
	reason := fs.String("reason", "", "audit reason")
	if err := fs.Parse(args); err != nil {
		withdrawalAuthzUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *operationID == "" {
		withdrawalAuthzUsage(stderr)
		return 2
	}
	callerID, err := strconv.ParseInt(*callerIDRaw, 10, 64)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz supply: invalid --caller-id %q: not a decimal integer\n", *callerIDRaw)
		withdrawalAuthzUsage(stderr)
		return 2
	}
	chainID, err := strconv.ParseInt(*chainIDRaw, 10, 64)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz supply: invalid --chain-id %q: not a decimal integer\n", *chainIDRaw)
		withdrawalAuthzUsage(stderr)
		return 2
	}
	expiresAt, code := withdrawalAuthzExpiresAt(stderr, *expiresAtRaw)
	if code != 0 {
		withdrawalAuthzUsage(stderr)
		return code
	}

	pool, code := withdrawalAuthzConnect(ctx, d, &chainID)
	if code != 0 {
		return code
	}
	defer pool.Close()

	out, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     *operationID,
		Action:          "supply",
		AuthorizationID: *authorizationID,
		CallerID:        callerID,
		ChainID:         chainID,
		Asset:           *asset,
		Recipient:       *recipient,
		Amount:          *amount,
		ExpiresAt:       expiresAt,
	}, *operator, *reason)
	if err != nil {
		return withdrawalAuthzRefuse(d, *operationID, err)
	}
	fmt.Fprintf(stdout, "txharbor withdrawal-authz: ok authorization_id=%s action=%s\n",
		out.AuthorizationID, out.Action)
	return 0
}

func withdrawalAuthzRevoke(ctx context.Context, args []string, d Deps) int {
	stdout, stderr := d.stdout(), d.stderr()

	fs := flag.NewFlagSet("withdrawal-authz revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	operationID := fs.String("operation-id", "", "opaque caller-minted attempt id (required)")
	authorizationID := fs.String("authorization-id", "", "upstream-issued grant id")
	operator := fs.String("operator", "", "declared operator identity for the audit row")
	reason := fs.String("reason", "", "audit reason")
	if err := fs.Parse(args); err != nil {
		withdrawalAuthzUsage(stderr)
		return 2
	}
	if fs.NArg() > 0 || *operationID == "" {
		withdrawalAuthzUsage(stderr)
		return 2
	}

	// Revoke carries no chain claim (it names an existing grant by id), so it
	// skips the FR-04 deployment bind.
	pool, code := withdrawalAuthzConnect(ctx, d, nil)
	if code != 0 {
		return code
	}
	defer pool.Close()

	out, err := withdrawal.RevokeGrant(ctx, pool, *operationID, *authorizationID, *operator, *reason)
	if err != nil {
		return withdrawalAuthzRefuse(d, *operationID, err)
	}
	fmt.Fprintf(stdout, "txharbor withdrawal-authz: ok authorization_id=%s action=%s\n",
		out.AuthorizationID, out.Action)
	return 0
}

// withdrawalAuthzConnect runs the shared carrier gate: config.Load (full
// serve-env validation, run with the serve env file), the deployment chain bind
// when the action carries a --chain-id claim (FR-04), and the operator pool
// connect. It returns a non-zero exit code on any failure; the chain bind is
// checked before the pool opens so a foreign chain touches no database.
func withdrawalAuthzConnect(ctx context.Context, d Deps, claimChainID *int64) (*pgxpool.Pool, int) {
	stderr := d.stderr()
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz: configuration error: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	if cfg.ChainID > math.MaxInt64 {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz: configuration error: %s out of system range\n", config.EnvChainID)
		return nil, 1
	}
	if claimChainID != nil {
		if err := withdrawal.ValidateChainID(*claimChainID, int64(cfg.ChainID)); err != nil {
			fmt.Fprintf(stderr, "txharbor withdrawal-authz: %s\n", logx.Redact(err.Error()))
			return nil, 1
		}
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz: %s\n", logx.Redact(err.Error()))
		return nil, 1
	}
	return pool, 0
}

// withdrawalAuthzExpiresAt parses the optional --expires-at as RFC3339. Empty
// means no expiry; a malformed value is a usage error (exit 2), while a present
// value in the past is a business refusal the library raises (exit 1).
func withdrawalAuthzExpiresAt(stderr io.Writer, raw string) (*time.Time, int) {
	if raw == "" {
		return nil, 0
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor withdrawal-authz: invalid --expires-at %q: not RFC3339\n", raw)
		return nil, 2
	}
	return &t, 0
}

// withdrawalAuthzRefuse maps a supply/revoke error to exit code 1 and writes
// the redacted reason. A temporarily_unavailable outcome carries the same-O
// retry instruction (the id was minted once; never rotate it for one attempt);
// every other classified code — operation_conflict, validation_failed — is a
// business refusal, never a usage error.
func withdrawalAuthzRefuse(d Deps, operationID string, err error) int {
	stderr := d.stderr()
	var e *withdrawal.Error
	if errors.As(err, &e) && e.Code == withdrawal.CodeTemporarilyUnavailable {
		fmt.Fprintf(stderr,
			"txharbor withdrawal-authz: %s; retry with the same --operation-id %s and the same parameters\n",
			logx.Redact(e.Error()), operationID)
		return 1
	}
	fmt.Fprintf(stderr, "txharbor withdrawal-authz: %s\n", logx.Redact(err.Error()))
	return 1
}
